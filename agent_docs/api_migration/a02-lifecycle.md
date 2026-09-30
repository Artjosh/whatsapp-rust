# A02: observable execution and terminal shutdown

## Foreground

Before:

```rust,ignore
bot.run().await; // ()
let reason = client.run_with_reason().await;
```

Now (both preserve every `RunCompletionReason`, including `Stopped`,
`AlreadyRunning`, and `AutoReconnectDisabled` with its available causes):

```rust,no_run
# async fn example(bot: whatsapp_rust::bot::Bot) {
use whatsapp_rust::RunCompletionReason;
match bot.run().await {
    RunCompletionReason::ShutdownRequested => {},
    RunCompletionReason::AutoReconnectDisabled { connection, connect_error, protocol_error, .. } => {
        eprintln!("{connection:?} {connect_error:?} {protocol_error:?}");
    }
    other => eprintln!("{other:?}"),
}
# }
```

`Client::run` and `Bot::run` now return the reason instead of `()`.
Statements ending in `;` still work. Unit-returning closures / spawned futures
must explicitly discard it (`async move { bot.run().await; }`).
`run_with_reason` remains an unchanged, non-deprecated compatibility alias.
No second run loop was added. The Bot boxed cross-crate barrier remains.

## Background and ownership

Before: `bot.spawn().await` returned `()`, losing the run reason.

```rust,no_run
# async fn example(bot: whatsapp_rust::bot::Bot) {
use whatsapp_rust::BotRunOutcome;
let handle = bot.spawn();
let client = handle.client();
// Keep the handle, not merely the Client Arc, for as long as supervision runs.
match handle.await {
    BotRunOutcome::Completed(reason) => eprintln!("{reason:?}"),
    BotRunOutcome::AbortRequested => eprintln!("abort requested, no run verdict"),
    BotRunOutcome::Unobserved => eprintln!("result sender lost; cause unknown"),
    _ => {},
}
# drop(client);
# }
```

Drop still invokes the runtime's abort handle; retaining a `Client` Arc does
not keep the driver alive. `abort(&self)` still skips graceful cleanup. Its
observable result is **AbortRequested**, not proof the runtime acknowledged
cancellation. Awaiting an explicitly aborted handle does not wait for the
oneshot sender to disappear, even for a runtime whose abort is delayed/no-op.
An already-sent run reason wins over a later abort request. Losing the sender
without such a request is **Unobserved**: panic, executor shutdown, and external
cancellation cannot be distinguished by this runtime interface. Abort/Drop
neither logs out nor promises a graceful flush or a reusable client.

## Terminal shutdown and report

Before: `client.disconnect().await` was terminal but returned `()`.

```rust,no_run
# async fn example(client: std::sync::Arc<whatsapp_rust::Client>) {
let report = client.shutdown().await;
if let Err(error) = report.device { eprintln!("snapshot flush failed: {error}"); }
eprintln!("inbound {:?}, outbound {:?}, Signal {:?}",
    report.inbound, report.outbound, report.signal_settle);
eprintln!("secret write failures: {}", report.message_secrets.failed_batches);
# }
```

```rust,no_run
# async fn example(bot: whatsapp_rust::bot::Bot) {
let handle = bot.spawn();
let report = handle.shutdown().await;
// Independent observations: client cleanup and background driver outcome.
eprintln!("{:?} {:?}", report.shutdown, report.run);
# }
```

`disconnect` remains a **documentation-deprecated** compatibility wrapper:
it invokes `shutdown`, discards its report and preserves its old `()` return.
There is intentionally no compiler deprecation warning and no removal scheduled
in this PR; removal would require a later breaking release. Migrate when you
need to inspect cleanup. `BotHandle::shutdown` now returns `BotShutdownReport`.
New outputs and variants are non-exhaustive.

Shutdown is still sticky. Later `run` returns `ShutdownRequested`, `connect`
returns `ConnectError::Shutdown`, and `resume` cannot undo it. Construct a **new
Client over the same store** to reopen. Logout (server deregistration), reversible
pause/resume, reconnect with backoff, and immediate reconnect remain separate.

### What the report actually proves

Each field records **this shutdown invocation**, not every concurrent teardown:

- `inbound`: pre-close commit/Signal durability decision, bounded to 5 seconds.
  `Failed` means the existing durability gate returned false; `TimedOut` means
  the deadline won. `Completed` does not mean a durability hook succeeded: its
  durable replay rows can remain after hook failure, just as before.
- `outbound`: tracked outbound guards drained within 5 seconds. Guard release
  includes cancellation and tasks that logged a send failure; **not successful
  delivery, server receipt, or callback completion**.
- `device`: the actual `PersistenceManager::flush` result and original typed
  error/source. A success is the manager's snapshot contract, not a fence against
  later concurrent mutations nor a power-loss guarantee stronger than the store.
- `signal_settle`: the post-close permit-held inbound/Signal settle performed by
  this cleanup. Failed flushes remain `Failed`, timeout remains `TimedOut` even
  after cancellation cleanup. Detached cleanup sender loss is `Unobserved`.
  Late uncommitted entries dropped by this cleanup downgrade completion to failure.
- `message_secrets`: cumulative failed batches since buffer construction, with
  the first storage error/source. Includes earlier detached writes: an empty final
  drain cannot hide warn-and-drop failures. Retention/drop/order behavior is unchanged.

Deadline results are returned where timeout is decided, never reconstructed by
sampling pending counters after the race. The flush guard is still captured
before the runtime's first poll. Device/secret writes, cleanup locks, and existing
lifecycle waits are not a new global shutdown deadline; a hung backend can still
prevent shutdown returning. Cancelling shutdown itself yields **no report**, not
an invented successful/failed one. No `ShutdownReport` is attached to sends.

### Worker/callback boundary for A04

This PR does **not** introduce an event-worker drain. Ordinary Concurrent Bot
callbacks and the Ordered drainer remain detached; an accepted callback can
retain a strong Client and outlive shutdown. The report does not count, abort or
join these callbacks. A04 must establish its delivery cancellation/drain policy.

Cleanup retires the existing generation before clearing chat lanes; old lane
work stops by its existing generation/permit guards. Coalesced Signal workers
stand down on generation recheck under their existing gate (not joined). Secret
intake is sealed, its channel closes, and the final serialized drain observes
its writes; this is not a join of every producer. The device saver observes the
sticky terminal signal and attempts its own final save; it is not joined here.
The major-sync intake holds a Weak while parked and can remain parked until the
channel closes/client drops; accepted detached history jobs are not joined.

With `client-lifecycle`/`plugins`, existing scope cancellation, closure ordering,
callback deadlines/panic isolation, and configured plugin task-drain policies
remain unchanged. `shutdown_lifecycle` waits for the existing driver except in
its reentrant callback context. Its unit return does not certify hook success
or full cooperative-task drain; plugin diagnostics remain the source for those
failures. None of the report fields claim lifecycle callbacks succeeded.

## Readiness: pairing socket versus authenticated session

Before: `wait_for_socket` and the ambiguously named `wait_for_connected`.
Both remain non-deprecated compatibility aliases with their original distinctions.

```rust,no_run
# async fn example(client: &whatsapp_rust::Client) -> Result<(), whatsapp_rust::ConnectError> {
use std::time::Duration;
client.wait_for_socket_ready(Duration::from_secs(30)).await?; // pre-login pair code
// Authentication plus critical app-state sync settled under its existing policy:
client.wait_for_session_ready(Duration::from_secs(30)).await?;
assert!(client.is_socket_ready());
assert!(client.is_session_ready());
# Ok(())
# }
```

The Bot pair-code worker uses only socket readiness. Session readiness never
releases on authentication alone or a spurious ready notification: waiters
recheck the existing readiness predicate after every wake. Critical sync
*settled* does not assert all sync succeeded. `is_connected` retains its existing
socket meaning. `Reachability` and `ConnectionScope` remain richer contracts,
not collapsed into these readiness booleans. Generation identities and success
publication guards are unchanged.
