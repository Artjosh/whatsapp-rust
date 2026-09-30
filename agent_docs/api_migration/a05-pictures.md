# A05: explicit picture lookup requests and preserved rejections

## New canonical entry

The compatibility calls still work:

```rust,ignore
client.contacts().lookup_profile_picture(&jid, true, known_id).await?;
client.groups().lookup_community_profile_picture(&jid, false, known_id).await?;
```

Use one borrowed request for size, route and advanced options instead:

```rust,no_run
use whatsapp_rust::{Client, ContactError, ProfilePictureRequest,
    ProfilePictureTarget, ProfilePictureType};
use whatsapp_rust::wacore_binary::Jid;
use std::time::Duration;

async fn lookup(client: &Client, jid: &Jid) -> Result<(), ContactError> {
    let outcome = client.contacts().lookup_picture(
        ProfilePictureRequest::new(ProfilePictureTarget::Contact(jid), ProfilePictureType::Full)
            .existing_id(None)
            .timeout(Some(Duration::from_secs(3)))
    ).await?;
    // Explicitly discard Unchanged, NotFound and NotAuthorized if only a
    // freshly found picture matters. Errors are NOT discarded by this conversion.
    let picture = outcome.into_found();
    Ok(())
}
```

`ProfilePictureType::Preview` remains wire `preview`; `Full` remains `image`.
Request/target/size types are exported from both `whatsapp_rust` and
`whatsapp_rust::features`. The shared implementation is the private
`src/features/pictures.rs` module; it is not an additional public facade.
Inputs borrow JIDs and strings, have a constructor and chainable setters, and
are non-exhaustive. No new dependency, feature gate, runtime or image buffer is
introduced. Existing core specs remain available for advanced protocol use.

## Routes and cost

- `Contact(&jid)`: `w:profile:picture`, including existing privacy-token discovery
  (excluding self, groups, bots, newsletter and broadcasts).
- `Group(&jid)`: the same profile-picture IQ, without contact-token discovery.
- `Community(&jid)`: explicit `w:g2` `<pictures><picture parent_group_jid=…>`.

A lookup sends at most one picture IQ. There is no metadata query, unconditional
community fallback or image download. Contact-token lookup reads the existing
store; `common_gid` is used only if no token is available. `invite` and
`persona_id` are sent only on the profile-picture route; the community route
retains its existing size, ID and timeout options. Routes are not assertions
about a JID's metadata: an ordinary group and a community parent both use
`@g.us`, and this API does not discover which one the caller meant.
The PSA/system JID remains short-circuited without an IQ.

## Outcomes and rate limiting

`ProfilePictureLookup` is unchanged: Found, Unchanged, NotFound and NotAuthorized
remain distinct. Conditional no-data responses retain the Rust parser's
existing Unchanged interpretation; **Unchanged supplies no fresh URL and does
not establish that image bytes exist locally**. Omit `existing_id` when bytes
are absent. `into_found()` deliberately drops all non-found states, including
the legacy RateOverlimit variant. It does not convert a failed lookup to None.

Canonical 429 is an error in the existing contact domain:
`ContactError::Iq(IqError::ServerError { code, text, error_type, backoff, response })`.
The original response allocation and all unread stanza metadata survive;
`ErrorChainExt::server_rejection()` recovers the code and optional backoff through
the source chain. Canonical lookup uses `Client::execute` with a private preserving
picture spec, retaining its encoding/correlation/timeout path. The execution
layer attaches the response only to a concrete typed core IQ server rejection;
ordinary parse errors (including non-rejection core errors) remain ParseError. Canonical lookup never emits an empty RateOverlimit, including
embedded picture errors accepted by the existing Rust parser. Other operational
failures remain errors. 401/403 map to NotAuthorized and 404 to NotFound.

| Entry | IQ-level 429 | Embedded picture 429 (Rust legacy tolerance) |
| --- | --- | --- |
| `contacts().lookup_picture(request)` | preserved error | preserved error |
| Legacy contacts/options/group/community lookups | unit RateOverlimit | unit RateOverlimit |
| Legacy `get_profile_picture[_with_timeout]` | preserved error | None |

There is no removal deadline or deprecation warning in this additive change.
Legacy getters intentionally retain their parser and error policy rather than
calling the canonical lookup and accidentally changing embedded-429 behavior.

## Explicit consumer fallback

A consumer may choose to retry the community route only after NotAuthorized.
This is **consumer policy**, not a global protocol rule. The following preserves
the original result if fallback supplies no new picture (including error or
Unchanged), and does not retry a rate-limited primary request:

```rust,no_run
use whatsapp_rust::{Client, ContactError, ProfilePictureLookup as Outcome,
    ProfilePictureRequest as Request, ProfilePictureTarget as Target, ProfilePictureType};
use whatsapp_rust::wacore_binary::Jid;

async fn group_avatar(client: &Client, jid: &Jid, known_id: Option<&str>,
    need_bytes: bool) -> Result<Outcome, ContactError> {
    let existing_id = if need_bytes { None } else { known_id };
    let original = client.contacts().lookup_picture(
        Request::new(Target::Group(jid), ProfilePictureType::Preview)
            .existing_id(existing_id)).await?;
    if !matches!(original, Outcome::NotAuthorized) {
        return Ok(original);
    }
    match client.contacts().lookup_picture(
        Request::new(Target::Community(jid), ProfilePictureType::Preview)
            .existing_id(existing_id)).await {
        Ok(found @ Outcome::Found(_)) => Ok(found),
        _ => Ok(original),
    }
}
```

This models the integration concern in oxidezap/client `avatar.rs` at
`f57c5a1f03492495ba85d752f81c41dbed057b81`: byte demands omit existing_id and
fallback never overwrites a non-destructive refusal with a fallback 404. It is
a local synthetic test scenario, not a PR or full compatibility claim for that
repository (which pins a different whatsapp-rust revision).

## Protocol evidence and limits

Source snapshot: whatspec `1a441f0329c941fcdb238490a6c604550d8a9939`, WA Web
`2.3000.1045368834`, schema 4.3.0. Exact 516-bundle set
`99a75bd7a4961e15051172c8b99fc460d57e58f7eb47ea9c46824657dd083e00` restored
using the pinned native CLI (all sizes/hashes checked); archive SHA-256
`2edd4b3f8dae50b503e0b15920adc774e2680cdb542396d57608bb0374196ee1`.
The IQ IR was queried before reading complete modules using strict oxc AST
extraction (no parse recovery, eval or regex body extraction). Duplicate module
definitions were compared, not silently selected.

In bundle `657a32007737e50b0cb0292ce9c364e21e48291b558129753b25e3ec2a4c13b7`:

- `WASmaxOutProfilePictureGetRequest` bytes 2103138–2104398 and imported
  GetIQ/TCToken mixins establish the existing attributes, target and token child.
- `WASmaxProfilePictureGetRPC` 2112675–2114382 dispatches URL, avatar URLs,
  blob, NoData and Error separately.
- `WASmaxInProfilePictureGetResponseSuccessNoData` 2084630–2085005 checks the
  IQ result envelope only: its name does **not** prove a cache hit.
- `WASmaxInProfilePictureIQErrorRateOverlimitMixin` 2078850–2079373 pins
  code 429/text rate-overlimit; `GetResponseError` 2082063–2082792 reads a
  top-level IQ error. Embedded errors are retained Rust compatibility, not a
  newly inferred Web shape.
- Caller `WAWebGetProfilePicJob` 2564171–2565904 uses preview/image, photoId,
  invite/commonGid/token, returns URL success and rejects other success kinds.

Community caller `WAWebFetchCommunityProfilePic` in bundle
`ffe69f61924eb23c1d65cb89a152e4ce35ee404bd52a7edcced9c037b236f490`
742373–743435 delegates to the group-picture RPC and treats parent/subgroup
routing explicitly. No server-session capture was executed. Rust URL parsing
and tolerant legacy status/no-data mapping are retained, not replaced with the
entire Web avatar/blob response family. A14 may reuse the target and size
vocabulary without touching lookup; own-profile and newsletter mutations still
have their own protocols.
