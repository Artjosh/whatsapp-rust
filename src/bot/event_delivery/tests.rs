use super::*;
use crate::test_utils::{create_test_client, poll_until};
use crate::types::events::{Connected, EventKind, PairingCode};
use std::time::Duration;

fn event(code: &str) -> Arc<Event> {
    Arc::new(Event::PairingCode(
        PairingCode::builder()
            .code(code.to_string())
            .timeout(Duration::ZERO)
            .build(),
    ))
}

#[tokio::test]
async fn worker_pool_bounds_real_callbacks_and_exact_overflow() {
    let client = create_test_client().await;
    let (started_tx, started_rx) = async_channel::unbounded();
    let (_release_tx, release_rx) = async_channel::bounded::<()>(1);
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::BoundedConcurrent {
            capacity: 2,
            max_concurrency: 3,
        },
        move |_, _client| {
            let started = started_tx.clone();
            let release = release_rx.clone();
            async move {
                started.send(()).await.unwrap();
                release.recv().await.unwrap();
            }
        },
    );
    let subscription = client.subscribe_handler(adapter.clone());
    for i in 0..3 {
        adapter.handle_event(event(&i.to_string()));
        started_rx.recv().await.unwrap();
    }
    for _ in 0..100 {
        adapter.handle_event(event("load"));
    }
    assert_eq!(
        adapter.workers.len(),
        3,
        "fixed pool, not spawn-per-job waiters"
    );
    assert_eq!(adapter.stats().callbacks_active, 3);
    assert_eq!(adapter.stats().callbacks_started, 3);
    assert_eq!(adapter.stats().accepted, 5);
    assert_eq!(adapter.stats().dropped_full, 98);
    assert_eq!(client.stats.events_dropped(), 98);
    let Delivery::Queued(tx) = &adapter.delivery else {
        panic!("queued mode")
    };
    assert_eq!(tx.len(), 2);
    adapter.cancel();
    poll_until("cancelled pool", || {
        adapter.stats().callbacks_active == 0 && adapter.stats().discarded == 2
    })
    .await;
    assert_eq!(adapter.stats().callbacks_cancelled, 3);
    assert_eq!(adapter.stats().callbacks_completed, 0);
    drop(subscription);
}

#[tokio::test]
async fn zero_capacity_and_concurrency_are_clamped_not_unbounded() {
    let client = create_test_client().await;
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::BoundedConcurrent {
            capacity: 0,
            max_concurrency: 0,
        },
        |_, _| async { std::future::pending::<()>().await },
    );
    assert_eq!(adapter.workers.len(), 1);
    adapter.handle_event(event("first"));
    adapter.handle_event(event("overflow"));
    assert_eq!(adapter.stats().dropped_full, 1);
    let Delivery::Queued(tx) = &adapter.delivery else {
        panic!("queued mode")
    };
    assert_eq!(tx.capacity(), Some(1));
    // No worker has been polled on this current-thread executor. Abort must
    // still release the queued payload and its accounting guard.
    adapter.cancel();
    poll_until("never-polled queue cancelled", || {
        adapter.stats().discarded == 1
    })
    .await;
    assert_eq!(adapter.stats().callbacks_started, 0);
}

#[tokio::test]
async fn shutdown_cancels_running_and_pending_without_retaining_client() {
    for policy in [
        EventDelivery::Concurrent,
        EventDelivery::Ordered { capacity: 2 },
        EventDelivery::default(),
    ] {
        let client = create_test_client().await;
        let weak_client = Arc::downgrade(&client);
        let (entered_tx, entered_rx) = async_channel::bounded(1);
        let adapter = CallbackEventHandler::from_callback(
            &client,
            EventInterest::ALL,
            policy,
            move |_, client| {
                let entered = entered_tx.clone();
                async move {
                    entered.send(()).await.unwrap();
                    // Really retain the Client in the suspended future.
                    std::future::pending::<()>().await;
                    drop(client);
                }
            },
        );
        let subscription = client.subscribe_handler(adapter.clone());
        adapter.handle_event(event("running"));
        entered_rx.recv().await.unwrap();
        let queued = event("pending");
        let weak_event = Arc::downgrade(&queued);
        adapter.handle_event(queued);
        client.shutdown().await;
        poll_until("terminal callback release", || {
            adapter.stats().callbacks_active == 0 && weak_event.upgrade().is_none()
        })
        .await;
        assert_eq!(adapter.stats().callbacks_cancelled, 1);
        adapter.handle_event(event("after shutdown"));
        assert_eq!(adapter.stats().closed, 1);
        assert_eq!(adapter.stats().dropped_full, 0);
        drop(subscription);
        drop(client);
        poll_until("client released", || weak_client.upgrade().is_none()).await;
    }
}

#[tokio::test]
async fn subscription_drop_cancels_unowned_adapter_and_future_deliveries() {
    let client = create_test_client().await;
    let (entered_tx, entered_rx) = async_channel::bounded(1);
    let (released_tx, released_rx) = async_channel::bounded(1);
    struct NotifyDrop(async_channel::Sender<()>);
    impl Drop for NotifyDrop {
        fn drop(&mut self) {
            let _ = self.0.try_send(());
        }
    }
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::Ordered { capacity: 1 },
        move |_, _| {
            let entered = entered_tx.clone();
            let guard = NotifyDrop(released_tx.clone());
            async move {
                let _guard = guard;
                entered.send(()).await.unwrap();
                std::future::pending::<()>().await;
            }
        },
    );
    let weak = Arc::downgrade(&adapter);
    let subscription = client.subscribe_handler(adapter);
    client
        .core
        .event_bus
        .dispatch(Event::Connected(Connected::builder().build()));
    entered_rx.recv().await.unwrap();
    drop(subscription);
    released_rx.recv().await.unwrap();
    assert!(weak.upgrade().is_none());
    client
        .core
        .event_bus
        .dispatch(Event::Connected(Connected::builder().build()));
    assert!(entered_rx.try_recv().is_err());
}

#[tokio::test]
async fn retained_adapter_may_finish_accepted_work_after_unsubscribe() {
    let client = create_test_client().await;
    let (tx, rx) = async_channel::unbounded();
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::Ordered { capacity: 2 },
        move |event, _| {
            let tx = tx.clone();
            async move {
                tx.send(event.kind()).await.unwrap();
            }
        },
    );
    let subscription = client.subscribe_handler(adapter.clone());
    client
        .core
        .event_bus
        .dispatch(Event::Connected(Connected::builder().build()));
    drop(subscription);
    client
        .core
        .event_bus
        .dispatch(Event::Connected(Connected::builder().build()));
    assert_eq!(rx.recv().await.unwrap(), EventKind::Connected);
    assert!(rx.try_recv().is_err());
    assert_eq!(adapter.stats().accepted, 1);
}

#[tokio::test]
async fn reentrant_shutdown_does_not_join_its_callback() {
    let client = create_test_client().await;
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::Ordered { capacity: 1 },
        |_, client| async move {
            client.shutdown().await;
        },
    );
    let _subscription = client.subscribe_handler(adapter.clone());
    adapter.handle_event(event("shutdown"));
    poll_until("reentrant terminal signal", || {
        client.shutdown_signal().is_fired()
    })
    .await;
    poll_until("reentrant callback terminates", || {
        adapter.stats().callbacks_active == 0
    })
    .await;
    assert_eq!(adapter.stats().callbacks_started, 1);
    assert_eq!(
        adapter.stats().callbacks_completed + adapter.stats().callbacks_cancelled,
        1
    );
}

#[tokio::test]
async fn interest_filter_skips_payload_and_callback_future_creation() {
    let client = create_test_client().await;
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::of(&[EventKind::Messages]),
        EventDelivery::default(),
        |_, _| -> std::future::Ready<()> { panic!("uninterested callback future constructed") },
    );
    let _subscription = client.subscribe_handler(adapter.clone());
    client
        .core
        .event_bus
        .dispatch_with(EventKind::Connected, || {
            panic!("uninterested payload built")
        });
    adapter.handle_event(event("wrong kind"));
    assert_eq!(adapter.stats(), EventDeliveryStats::default());
    let none = CallbackEventHandler::from_callback(
        &client,
        EventInterest::none(),
        EventDelivery::default(),
        |_, _| async {},
    );
    assert!(none.workers.is_empty());
}

#[tokio::test]
async fn concurrent_panics_are_isolated_and_counted() {
    let client = create_test_client().await;
    let adapter = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::Concurrent,
        |event, _| {
            if matches!(&*event, Event::PairingCode(code) if code.code == "creation") {
                panic!("creation panic");
            }
            async move {
                if matches!(&*event, Event::PairingCode(code) if code.code == "poll") {
                    panic!("poll panic");
                }
            }
        },
    );
    for code in ["creation", "poll", "ok"] {
        adapter.handle_event(event(code));
    }
    poll_until("isolated callback outcomes", || {
        adapter.stats().callbacks_completed + adapter.stats().callbacks_panicked == 3
    })
    .await;
    assert_eq!(adapter.stats().callbacks_panicked, 2);
    assert_eq!(adapter.stats().callbacks_active, 0);
    assert_eq!(adapter.stats().callbacks_cancelled, 0);
}
