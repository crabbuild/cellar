<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable provider binding journal

[Container guide](README.md) · [Provider responsibilities](providers.md)

A remote sandbox can outlive the event which created it. The host needs durable
identity and intent before submitting work, including when a response is lost.
`cellar::sdk::containers::BindingJournal` stores those records in reserved cell
SQLite tables. It is a host API; guest KV/SQL cannot forge or erase its records.

This is the persistence contract for remote providers. Add explicit host
[supervision](supervision.md) for scheduled expiry and cleanup. The journal
alone does not schedule work or supply remote fencing.

## Select a host-declared provider slot

Applications can select additional providers declared by the host. `ctx.container`
and Cloudflare's container surface use the default slot, named `container`.

```javascript
const declared = await ctx.containers.list();
const build = ctx.containers.get("builds");
const state = await build.reconcile();
```

| Boundary | Contract |
| --- | --- |
| Host registration | `Source::container_binding(name, provider)?` in Python/JS hosts; `cellar_container::Bindings` in Rust |
| Python / JS / TS selection | `ctx.containers.get("builds")`; same container methods and event authority |
| Discovery | `await ctx.containers.list()` returns names and per-slot capabilities; no remote resource allocation or credential exposure |
| Names | 1–128 ASCII letters/digits/`-_.`; at most 63 additional slots |
| Unknown slot | Refuse before provider effects; no default-provider fallback |
| Durable identity | Provider `binding_name()` must equal the registered slot; E2B/Vercel use their configured journal name |
| Conflicts | Duplicate names, mismatched identities and default/named journal collisions refuse; startup revalidates after later builder overrides |
| Native engine | Owns the existing default slot; extra slots require providers declaring their own binding identity |
| Handles | Process/socket/response facades carry the selected slot through later calls; invocation handles expire with their original event |

Configure the provider's durable name before registering it:

```rust,no_run
use cellar_container::{Bindings, e2b::{Config, E2b}};
use std::sync::Arc;

fn build_binding(config: Config) -> anyhow::Result<Bindings> {
    let mut bindings = Bindings::default();
    bindings.insert("builds", Arc::new(E2b::new(config.binding("builds")?)))?;
    Ok(bindings)
}
```

Rust callers open `bindings.sessions()` once per event and select with
`sessions.get("builds")?`. When combining it with a default `Adapter`, call
`bindings.validate(adapter.provider())?` before admission. Discovery lists the
default slot first, then additional slots in name order. Provider limits apply
per slot; discovery itself must fit the host's response byte limit.

These declarations select configured execution providers. Register their
[maintenance backends](supervision.md) separately to enable scheduled cleanup.

## Reconcile from a later invocation

`container.reconcile()` observes the host-selected binding and checkpoints
verified recovery or expiry. It never creates, resumes or deletes compute.
Reacquire the current event's container/session before calling it.

```javascript
const observed = await ctx.container.reconcile();
if (observed.state === "running") {
  // Fresh event authority, same durable resource generation.
  const output = await ctx.container.execOutput({ cmd: ["/app/check"] });
}
```

| Result `state` | Meaning | `generation` |
| --- | --- | --- |
| `unbound` | No durable provider reservation exists; no provider request is sent | `null` |
| `running` | Verified resource is running; a matching pending create can now be confirmed | Original decimal generation |
| `stopped` | Verified resource exists but is paused, stopped or transitioning; no automatic resume | Original decimal generation |
| `deleting` | Deletion intent is durable, but the resource is still visible | Original decimal generation |
| `deleted` | Retirement was already confirmed, or verified absence now confirms deletion/expiry | Retired decimal generation |
| Error | Unresolved create, ambiguous identity, mismatched metadata, timeout or provider failure | Reservation retained |

The native Docker/Podman provider returns `running` or `stopped` with a `null`
generation: its lifecycle belongs to the native engine rather than this remote
journal. A stopped remote resource remains allocated; explicitly destroy it
before replacement.

| Language | Call |
| --- | --- |
| Rust shared adapter | `session.execute(ctx, Operation::Reconcile, json!({}), limits).await?`; deserialize as `Reconciliation` |
| Python | `observed = await ctx.container.reconcile()`; frozen `ContainerReconciliation` |
| JavaScript / TypeScript | `const observed = await ctx.container.reconcile()`; readonly result |

An empty listing or a 404 for an unconfirmed create cannot authorize a new
allocation. Pending deletion stays pending while the resource is visible, even
after a successful DELETE acknowledgement. Reconciliation checks that evidence
without repeating the DELETE. Use `destroy()` explicitly to request cleanup.
This event-driven API does not enable scheduled maintenance by itself.
[Host supervision](supervision.md) can inspect and retire the same records;
the provider's fencing guarantee remains unchanged.

## Record and checkpoint

| Field | Meaning |
| --- | --- |
| Host binding name | ASCII letters/digits/`-_.`, 1–128 bytes; selected by trusted provider configuration |
| `provider` | Backend identity; no credentials |
| `configuration_digest` | 64 lowercase hex characters; provider hashes its effective configuration |
| `parameters_digest` | 64 lowercase hex characters; hash of the original create parameters, separate from host configuration |
| `generation` | Positive, monotonically increasing resource generation within this binding |
| `request_id` | 32 lowercase hex characters; one stable create identity within a generation |
| `resource_id` | Nullable opaque backend resource ID; once known, it cannot change or be forgotten in that generation |
| `intent` | `Running` or `Deleted` |
| `observation` | `Pending` or `Confirmed`; a lost response remains pending |
| Checkpoint `revision` | Compare-and-exchange version, incremented by each accepted write |
| Checkpoint `activation_epoch` | Provenance of the local activation which wrote it; **not a distributed fencing token** |

Keep API keys, access tokens and user environment secrets outside this journal.
Its fixed record format has no arbitrary credential/configuration payload.

## Reserve before creating

```rust,no_run
use cellar::sdk::{containers::{BindingCheckpoint, BindingJournal, BindingRecord}, Context};

async fn reserve(
    ctx: &Context,
    record: BindingRecord,
) -> anyhow::Result<BindingCheckpoint> {
    let journal = BindingJournal::open(ctx, "builds")?;
    // `None` means this binding must not exist. An existing pending create
    // requires reconciliation; a second call cannot silently replace it.
    journal.compare_exchange(ctx, None, record).await
}
```

The initial record has generation `1`, running intent, pending observation and
no resource ID. The future commits the reservation and waits for the required
native durability proof. Submit the external create only after it succeeds.
If another event changes the revision while the future waits, it returns an
error. An error after writing can leave a retained pending reservation.

## Lifecycle rules

| Step | Journal action | Provider obligation |
| --- | --- | --- |
| Create | Reserve pending running intent with a stable request ID | Await durability, then submit once |
| Acknowledged create | Record resource ID and confirmed running observation | Check current authority and correlate the provider result |
| Lost create response | Retain the pending record | Observe/reconcile the original request; do not infer absence or replay |
| Delete | Record pending deletion intent | Await durability before deleting or fencing the resource |
| Acknowledged deletion | Record confirmed deletion | Supply backend evidence that the generation is retired |
| Replacement | Start generation + 1 with a fresh request ID | Requires confirmed retirement; configuration/provider may now change |

Before using a resource ID, verify its provider and configuration digest against
the host configuration and correlate backend metadata with its request ID.
Include the provider account/tenant identity in that digest without storing its
credentials. Configuration changes require retiring the old generation through
the original provider before starting a replacement.

Within a generation, provider, configuration/parameter digests and request identity are
immutable. Deletion intent cannot be reversed, confirmed work cannot become
pending again except when beginning deletion, and a retired generation cannot
be reopened. Tombstones retain their revisions and count toward the capacity.

E2B's [v2 create contract](https://github.com/e2b-dev/E2B/blob/main/spec/openapi.yml)
provides optional idempotency keys with a finite retention period. Retrying after
expiry can create another resource. A stored request ID alone is therefore not
permission to replay an unresolved creation indefinitely.

## Authority, storage and recovery

| Boundary | Behavior |
| --- | --- |
| Handle | Exactly one original root-cell invocation; reacquire for later events |
| Transactions | Refuse use inside application storage transactions; fixed internal savepoints support native autocommit and implicit batches |
| Atomicity | A failed revision/transition does not partially replace a record or initialize a partial schema |
| Durability | Every successful update awaits native storage sync and rechecks the current checkpoint before returning |
| Business clear | `storage.delete_all()` preserves journal records and markers, including unresolved creates |
| Recovery | Retain records across node restart; refuse unsupported, incomplete or invalid retained schemas before admission |
| Bounds | At most 64 named records per cell and 16 KiB per encoded record; resource IDs at most 4,096 bytes |

Local invocation checks protect access to the journal. They cannot revoke a
mutation already accepted by a remote service. Providers still need service- or
broker-enforced fencing, leases/expiry, reconciliation and orphan cleanup.
A confirmed journal observation records evidence from the provider; it does not
prove that a remote instance is still running at a later time.
