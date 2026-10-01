//! Public consumer coverage: portable host assembly, real stanza dispatch and observer lifetime.
use std::sync::Arc;
use std::time::Duration;
use whatsapp_rust::handlers::chatstate::ChatstateHandler;
use whatsapp_rust::handlers::traits::StanzaHandler;
use whatsapp_rust::http::{HttpClient, HttpRequest, HttpResponse};
use whatsapp_rust::store::persistence_manager::PersistenceManager;
use whatsapp_rust::transport::{Transport, TransportEvent, TransportFactory};
use whatsapp_rust::types::events::{
    ChannelEventHandler, Event, EventHandler, EventInterest, EventKind,
};
use whatsapp_rust::wacore::iq::chatstate::ReceivedChatState;
use whatsapp_rust::wacore::store::in_memory::InMemoryBackend;
use whatsapp_rust::{
    CallbackEventHandler, Client, EventDelivery, NodeBuilder, TokioRuntime, anyhow, async_channel,
    wacore_binary,
};

struct OfflineHttp;
#[whatsapp_rust::async_trait]
impl HttpClient for OfflineHttp {
    async fn execute(&self, _: HttpRequest) -> anyhow::Result<HttpResponse> {
        anyhow::bail!("offline observer HTTP")
    }
}
struct OfflineTransport;
#[whatsapp_rust::async_trait]
impl TransportFactory for OfflineTransport {
    async fn create_transport(
        &self,
    ) -> anyhow::Result<(Arc<dyn Transport>, async_channel::Receiver<TransportEvent>)> {
        anyhow::bail!("offline observer transport")
    }
}
async fn client() -> Arc<Client> {
    let pm = Arc::new(
        PersistenceManager::new(Arc::new(InMemoryBackend::new()))
            .await
            .unwrap(),
    );
    let (client, receiver) = Client::builder()
        .with_persistence_manager(pm)
        .with_runtime(TokioRuntime)
        .with_http_client(OfflineHttp)
        .with_transport_factory(OfflineTransport)
        .with_version_override((2, 3000, 1))
        .build()
        .await
        .unwrap()
        .into_parts();
    assert_eq!(receiver.receiver_count(), 1);
    client
}
async fn dispatch(client: &Arc<Client>, state: &'static str, media: Option<&str>) {
    let mut child = NodeBuilder::new(state);
    if let Some(media) = media {
        child = child.attr("media", media);
    }
    let node = NodeBuilder::new("chatstate")
        .attr("from", "120363000001@g.us")
        .attr("participant", "15550001111@s.whatsapp.net")
        .children([child.build()])
        .build();
    let packed = wacore_binary::marshal::marshal(&node).unwrap();
    let bytes = wacore_binary::util::unpack(&packed).unwrap();
    let owned = Arc::new(whatsapp_rust::OwnedNodeRef::new(bytes.into_owned()).unwrap());
    assert!(
        ChatstateHandler
            .handle(client.clone(), owned, &mut false)
            .await
    );
}
async fn eventually(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn async_observer_is_filtered_and_subscription_is_raii() {
    let client = client().await;
    let (tx, rx) = async_channel::unbounded();
    let handler = CallbackEventHandler::from_callback(
        &client,
        EventInterest::of(&[EventKind::ChatPresence]),
        EventDelivery::Ordered { capacity: 4 },
        move |event, _| {
            let tx = tx.clone();
            async move {
                tx.send(event).await.unwrap();
            }
        },
    );
    let subscription = client.subscribe_handler(handler.clone());
    dispatch(&client, "composing", Some("audio")).await;
    let observed = rx.recv().await.unwrap();
    assert!(
        matches!(&*observed, Event::ChatPresence(update) if update.source.chat.to_string() == "120363000001@g.us" && update.source.sender.to_string() == "15550001111@s.whatsapp.net")
    );
    drop(subscription);
    dispatch(&client, "paused", None).await;
    assert_eq!(handler.stats().accepted, 1);
    assert!(rx.try_recv().is_err());
    handler.cancel();
    client.shutdown().await;
}

#[tokio::test]
async fn chatstate_compatibility_view_uses_the_same_bus_fact() {
    let client = client().await;
    let (tx, rx) = async_channel::unbounded();
    let subscription = client.subscribe_chatstate_handler(Arc::new(move |event| {
        tx.try_send(event).unwrap();
    }));
    for (state, media, expected) in [
        ("composing", None, ReceivedChatState::Typing),
        (
            "composing",
            Some("audio"),
            ReceivedChatState::RecordingAudio,
        ),
        ("paused", None, ReceivedChatState::Idle),
    ] {
        dispatch(&client, state, media).await;
        let observed = rx.recv().await.unwrap();
        assert_eq!(observed.chat.to_string(), "120363000001@g.us");
        assert_eq!(
            observed.participant.unwrap().to_string(),
            "15550001111@s.whatsapp.net"
        );
        assert_eq!(observed.state, expected);
        assert!(rx.try_recv().is_err());
    }
    drop(subscription);
    dispatch(&client, "composing", None).await;
    assert!(rx.try_recv().is_err());
    client.shutdown().await;
}

#[tokio::test]
async fn bounded_pool_drops_newest_without_stalling_protocol_handler() {
    let client = client().await;
    let (tx, rx) = async_channel::unbounded();
    let handler = CallbackEventHandler::from_callback(
        &client,
        EventInterest::of(&[EventKind::ChatPresence]),
        EventDelivery::BoundedConcurrent {
            capacity: 2,
            max_concurrency: 2,
        },
        move |_, client| {
            let tx = tx.clone();
            async move {
                tx.send(()).await.unwrap();
                std::future::pending::<()>().await;
                drop(client);
            }
        },
    );
    let _subscription = client.subscribe_handler(handler.clone());
    for _ in 0..2 {
        dispatch(&client, "composing", None).await;
        rx.recv().await.unwrap();
    }
    for _ in 0..12 {
        dispatch(&client, "composing", None).await;
    }
    assert_eq!(handler.stats().callbacks_active, 2);
    assert_eq!(handler.stats().dropped_full, 10);
    assert_eq!(client.stats().events_dropped, 10);
    client.shutdown().await;
    eventually(|| handler.stats().callbacks_active == 0 && handler.stats().discarded == 2).await;
    assert_eq!(handler.stats().callbacks_cancelled, 2);
}

#[tokio::test]
async fn defaults_zero_and_reentrant_terminal_cancellation_are_public() {
    assert!(matches!(
        EventDelivery::default(),
        EventDelivery::BoundedConcurrent {
            capacity: 256,
            max_concurrency: 16
        }
    ));
    let (_, bounded) = ChannelEventHandler::new();
    assert_eq!(bounded.capacity(), Some(256));
    let (_, unlimited) = ChannelEventHandler::unbounded();
    assert_eq!(unlimited.capacity(), None);
    let (_, zero) = ChannelEventHandler::with_capacity(0);
    assert_eq!(zero.capacity(), Some(1));
    let client = client().await;
    let weak = Arc::downgrade(&client);
    let handler = CallbackEventHandler::from_callback(
        &client,
        EventInterest::ALL,
        EventDelivery::Ordered { capacity: 0 },
        |_, client| async move {
            client.shutdown().await;
        },
    );
    let subscription = client.subscribe_handler(handler.clone());
    dispatch(&client, "composing", None).await;
    eventually(|| client.shutdown_signal().is_fired() && handler.stats().callbacks_active == 0)
        .await;
    assert_eq!(handler.stats().callbacks_started, 1);
    drop(subscription);
    drop(client);
    eventually(|| weak.upgrade().is_none()).await;
    // Terminal intake remains closed even when the adapter is kept by the host.
    let connected = Event::Connected(whatsapp_rust::types::events::Connected::builder().build());
    handler.handle_event(Arc::new(connected));
    assert_eq!(handler.stats().closed, 1);
}
