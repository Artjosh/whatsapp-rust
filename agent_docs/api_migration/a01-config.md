# A01 — shared construction configuration

Base: `6f07e3ab7f6a94764c23dfd6298c312b58419e7b`. No protocol change or new runtime dependency.

## What changed

`ClientBuilder` was already the canonical dynamic assembly. `BotBuilder` now
stores that builder rather than a second set of common fields/defaults and a
build-time forwarding list. Its typestate still requires backend, transport,
HTTP and runtime before `build()` is available. Missing dynamic dependencies
still return `ClientBuilderError` in the same order. The boxed construction
barriers and transactional lifecycle/plugin publication boundary are unchanged.

`ClientOptions` is exported from `whatsapp_rust::client`, the root and prelude.
It is a cloneable, non-exhaustive configuration input: start with `Default`, then
assign fields. `ClientBuilder::with_options` and
`BotBuilder::with_client_options` **replace** the options, without replacing
injected services or callbacks. Subsequent convenience setters override the
corresponding field; watched props accumulate as before.

```rust
use whatsapp_rust::bot::Bot;
use whatsapp_rust::{Client, ClientOptions, PresencePolicy};

let mut options = ClientOptions::default();
options.skip_history_sync = true;
options.presence_policy = PresencePolicy::Manual;
let low_level = Client::builder().with_options(options.clone());
let bot = Bot::builder().with_client_options(options);
// Add platform dependencies/backend, then build either facade.
```

## Defaults and ownership

| Shared option | Default |
| --- | --- |
| version override | None (existing version-fetch policy) |
| caches/resource pools | `CacheConfig::default()` — unchanged |
| skip history sync | false |
| fetch A/B props | true |
| additional watched props | empty |
| presence | Automatic |
| Noise certificate policy | Strict |
| wanted prekeys | None (existing client default, 812) |
| resend limiter override | None (existing client default, burst 20 / 10 per minute) |
| periodic saver | **Client/ClientOptions: disabled; Bot: 30 seconds** |

Passing `ClientOptions::default()` to Bot explicitly disables its saver. To retain
Bot's saver while sharing a value, set `background_saver_interval` to
`Some(Duration::from_secs(30))`, or clone `bot_builder.client_options()` before
customizing. Zero saver intervals still fail with the typed validation error.

Common injected dependencies are stored once in ClientBuilder: runtime,
persistence manager, transport, HTTP, encrypted-payload handlers, durability
hook, history admission, task instrumentation and optional extension
registrations. They are not a second, public dependency bundle. Bot adds only
its storage/backend initialization and facade-specific callbacks, delivery
policy, pairing, device properties and push name. Bot's retained run instrument
is the same Arc as the builder's effective instrument, including the existing
last-setter-wins allocation-meter rule.

By-value setters still accept concrete, non-Clone implementations. `_arc`
setters accept already-erased shared trait objects without allocating another
Arc. Bot now also has `with_runtime_arc`, `with_transport_factory_arc`,
`with_enc_handler_arc` and `with_inbound_durability_hook_arc`. A host can reuse
its runtime/HTTP/transport across sessions; storage remains session scoped.
`CacheConfig` itself can contain shared cache-store Arcs, which cloning options
preserves. Host traits remain implementable and unsealed.

Optional lifecycle (`client-lifecycle`), plugin (`plugins`) and media-backend
(`voip-control`) APIs/validation retain their feature gates in ClientBuilder.
No engine, Tokio or platform dependency is added to the portable configuration.
No changes to run, shutdown, BotHandle or EventDelivery are part of A01.

## Positional constructors: deprecation, not removal

Before:

```rust,ignore
let (client, receiver) = Client::new(runtime, persistence, transport, http, version).await;
```

After (manual host ownership):

```rust,ignore
let mut options = ClientOptions::default();
options.override_version = version;
let (client, receiver) = Client::builder()
    .with_options(options)
    .with_runtime_arc(runtime)
    .with_persistence_manager(persistence)
    .with_transport_factory_arc(transport)
    .with_http_client_arc(http)
    .build().await?
    .into_parts();
```

`new_with_cache_config` migrates the same way, using `with_cache_config(config)`.
The old signatures remain compatibility wrappers through canonical assembly;
they are deprecated for consumers, with no removal release committed yet.
Existing in-crate unit fixtures continue exercising positional compatibility
without deprecation warnings under `cfg(test)`; benchmark-host fixtures use the
builder. There is no source break for existing consumers, only the deprecation
warning (which requires migration for consumers denying warnings).

## Major-sync receiver: choose one owner

`build()` starts ordinary client services, but not the major-sync consumer.
`ClientBuild::into_client()` consumes the build and starts exactly one standard
major-sync worker. `into_parts()` consumes the build and transfers the original,
sole receiver **without** starting that worker; the host must drain it or pass it
to `Client::start_sync_task_worker`. Do not clone it to create independent
observers: async-channel receivers compete. Dropping the manual receiver closes
this route. Bot retains its receiver until its existing launch path starts it.
No Clone implementation or hidden receiver clone was added.

For the standard worker, replace the final `.into_parts()` above with
`.into_client()` and do not spawn a second consumer.

## Reproduce the public contract fixture

`tests/api_a01_config.rs` uses only public APIs and an offline host backend,
HTTP and transport. The standalone `tests/api-a01-consumer` package reuses it
without workspace dependency aliases or default platform adapters:

```sh
RUSTFLAGS='' CARGO_BUILD_JOBS=2 cargo +1.94.1 test \
  --manifest-path tests/api-a01-consumer/Cargo.toml
```

Set a worktree-private `CARGO_TARGET_DIR`. This is a host test, not a browser or
ESP32 execution claim. Feature/portable compile evidence and binary measurements
are recorded in the PR/task report; no RAM or binary reduction is promised by
this refactoring alone.
