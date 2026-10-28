<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Named sandbox allocations

[Container guide](README.md) · [Provider slots](bindings.md) · [Native capacity](README.md#native-capacity-admission)

`cellar::sdk::containers::SandboxCatalog` is the durable **parent catalog** for
named sandbox execution cells. A host-registered `ChildExecutor` creates and
controls each child through authenticated framework RPC. Parent retirement
schedules exact-resource cleanup independently of application alarms.

```mermaid
flowchart LR
    Business[Business cell] --> Catalog[Protected SQLite catalog]
    Catalog --> A[build / generation 1]
    Catalog --> B[preview / generation 3]
    A -->|Authenticated RPC| ChildA[Sandbox execution cell A]
    B -->|Authenticated RPC| ChildB[Sandbox execution cell B]
    ChildA --> NativeA[One native container]
    ChildB --> NativeB[One native container]
```

The execution topology retains one native container per execution cell. Child
calls use normal cell routing; the business cell keeps their ownership records.
Native host capacity admission applies before Engine creation. A new native
child consults per-class fleet slots when its origin has no container room.
The chosen host still makes the final atomic admission decision.

## Provider slots and sandbox names

| Concept | Example | Meaning |
| --- | --- | --- |
| Host provider slot | `ctx.containers.get("builds")` | Select one configured provider; existing four-language API |
| Named allocation | Catalog reservation for `build-123` | Reserve an independent child execution identity; Rust, Python, JavaScript and TypeScript |
| Namespace binding | `SANDBOXES` | Configured execution-cell namespace; resolved before reservation |
| Parent identity | Scope + random durable incarnation | Survives activation changes; distinguishes a recreated catalog |
| Child identity | Parent identity + sandbox name + generation | Deterministic routing name and configured namespace scope |

No fixed sandbox count is imposed by this catalog. Rows remain in SQLite;
queries materialize at most 256 entries. Storage limits still apply, and actual
compute allocation must also pass host/provider capacity admission.

## Register and use an executor

Register a dedicated class through `Application::sandbox_executor`. The same
registration must be available on the parent and execution hosts. Register the
class and its `SANDBOXES` namespace in the deployment; for Native, also configure
its container image and Engine capacity.

```rust,no_run
use std::sync::Arc;
use cellar::sdk::{containers::SandboxExecutor, Application};
use cellar_container::{ChildExecutor, Native};

// In your Application implementation:
// fn sandbox_executor(&self, class: &str) -> Option<Arc<dyn SandboxExecutor>> {
//     (class == "Sandbox").then(|| self.executor.clone() as Arc<dyn SandboxExecutor>)
// }

# fn configure(configuration_sha256: String) -> anyhow::Result<()> {
let executor = Arc::new(ChildExecutor::new(Arc::new(Native), configuration_sha256)?);
# let _ = executor;
# Ok(())
# }
```

The configuration digest covers provider/account, image and host policy. Keep
credentials out of its manifest. Changing it refuses access to retained children;
keep the original executor available until its cleanup obligations finish.
Ordinary fetch/RPC calls to the dedicated class cannot invoke its executor or
application factory.

```rust,no_run
use cellar::sdk::{containers::SandboxCatalog, Context};
use serde_json::json;

async fn build(ctx: &Context) -> anyhow::Result<()> {
    let sandboxes = SandboxCatalog::open(ctx)?;
    let child = sandboxes.start_child(
        ctx, "build-123", "SANDBOXES", json!({"options": {}}),
    ).await?;
    let result = sandboxes.execute_child(
        ctx, &child, "container.exec_output",
        json!({"options": {"cmd": ["echo", "hello"]}, "max_bytes": 65536}),
    ).await?;
    sandboxes.destroy_child(ctx, &child).await?;
    Ok(())
}
```

| Operation | Bound / behavior |
| --- | --- |
| `start_child` | Durable registration and creation intent precede provider submission; equal names retain the original generation |
| `inspect_child` | Observe/reconcile the same identity; never create or resume |
| `execute_child` | Bounded exec output, files, port readiness/HTTP and capability/running observations |
| `destroy_child` | Persist deletion intent, prove exact resource retirement, queue child data erasure |
| Binary bodies / combined command output | At most 64 KiB per bounded operation |
| Encoded child RPC | At most 256 KiB; metadata and framing also count |
| Process, socket and stream handles | Do not cross child calls in this profile |
| Proven capacity refusal | Creation intent returns to unstarted; the same named generation can be tried later |
| Lost create or command response | Unknown outcome; no automatic mutation replay |

Reopen `SandboxCatalog` in each root-cell event. Parent and full allocation
checkpoints are checked before dispatch and again before returning a child reply.
A child stores its parent incarnation, generation and configuration digest before
provider I/O. Its durable deletion intent and external retirement barrier reject delayed starts.

## Python, JavaScript and TypeScript

The Rust host registers the native executor with
`Source::sandbox_executor("Sandbox", executor)?` for either language adapter.
Declare `Sandbox` and `SANDBOXES` in the deployment, but export only the business
cell's guest class. Retain the executor registration until cleanup is complete.

```typescript
const child = await ctx.sandboxes.start("build-123", "SANDBOXES");
const result = await ctx.sandboxes.execute(child, "container.exec_output", {
  options: { cmd: ["echo", "hello"] },
  max_bytes: 65536,
});
await ctx.sandboxes.destroy(child);
```

```python
child = await ctx.sandboxes.start("build-123", "SANDBOXES")
result = await ctx.sandboxes.execute(child, "container.exec_output", {
    "options": {"cmd": ["echo", "hello"]},
    "max_bytes": 65536,
})
await ctx.sandboxes.destroy(child)
```

| Facade operation | Contract |
| --- | --- |
| `start(name, binding, parameters)` | Default parameters are `{"options": {}}`; duplicate names reconcile the original generation |
| `get(name)` / `list({after, limit})` | Return complete checkpoints; Python uses keyword arguments for `list` |
| `inspect(checkpoint)` | Observe the same child without creating it |
| `execute(checkpoint, operation, input)` | Closed set of bounded container commands shown above |
| `destroy(checkpoint)` | Refuse a stale checkpoint; deletion is repeatable for the same identity |
| `parent()` / `retire()` | Inspect ownership or close admission and schedule cleanup |
| `requestErasure()` / Python `request_erasure()` | Explicit, irreversible parent-data erasure after retirement finishes |

Revisions and generations are decimal **strings** in guest checkpoints. Pass the
whole returned checkpoint; do not convert these strings to JavaScript numbers.
A provider capacity failure may follow a committed catalog reservation. Reopen
`get(name)` before retrying; guest failure outcomes do not promise rollback.

## Manual reservations

The lower-level `reserve`, `confirm_ready`, `begin_delete` and `confirm_deleted`
methods remain available to host controllers. Manual reservations do **not**
register an executor or schedule provider calls automatically. A `fresh: false`
reservation never grants permission to replay an uncertain create.

### Reserve before effects

```rust,no_run
use cellar::sdk::{containers::{SandboxCatalog, SandboxReservation}, Context};

async fn reserve_build(
    ctx: &Context,
    parameters_sha256: &str,
) -> anyhow::Result<SandboxReservation> {
    SandboxCatalog::open(ctx)?
        .reserve(ctx, "build-123", "SANDBOXES", parameters_sha256)
        .await
}
```

The parameter digest is 64 lowercase hex characters identifying the creation
parameters. Keep credentials and raw environment values out of the catalog.

| Result | Controller obligation |
| --- | --- |
| `fresh: true` | The reservation passed the native durability gate; correlate any subsequent child effect to this exact identity |
| `fresh: false` | Reconcile the original child; do not repeat an uncertain create |
| Changed parameters or namespace | Refuse until the old generation has confirmed deletion |
| Error or cancelled reservation | A record may have committed; reopen and inspect it before proceeding |
| Capacity refusal | Keep the identity available for reconciliation; the catalog does not infer whether provider creation began |

Reacquire the catalog for each root-cell event. Stateless handlers, facets,
foreign/stale event handles and application storage transactions cannot use it.
Guest SQL and `storage.delete_all()` cannot forge or erase its records.

## Generation lifecycle

```mermaid
stateDiagram-v2
    [*] --> Reserved: durable reservation
    Reserved --> Ready: verified child acknowledgement
    Reserved --> Deleting: durable cleanup intent
    Ready --> Deleting: durable cleanup intent
    Deleting --> Deleted: exact child retirement proven
    Deleted --> Reserved: next generation / new child identity
```

Each transition compares the full expected checkpoint. A delayed response from
an old generation cannot delete or confirm its replacement. Successful writes
wait for storage sync and recheck both parent and allocation before returning.
Only the host controller may call `confirm_ready` or `confirm_deleted`, using
verified child evidence. These methods record evidence; they do not obtain it.

## Retire a parent without losing children

```mermaid
sequenceDiagram
    participant Parent
    participant Store as Durable catalog
    participant Work as Framework cleanup
    participant Child
    participant Engine
    Parent->>Store: begin_retirement / close admission
    Store->>Work: persist independent cleanup wake
    Work->>Store: claim exact child + persist retry
    Work->>Child: authenticated destroy
    Child->>Engine: delete immutable resource ID
    Child->>Engine: inspect exact ID / prove absence
    Child->>Child: persist deletion tombstone
    Child-->>Work: confirmed retirement
    Work->>Store: confirm child deletion
    Work->>Store: finalize parent when all children are deleted
```

| Case | Behavior |
| --- | --- |
| Active parent, explicit `destroy_child` | Failed cleanup remains scheduled for that child |
| `begin_retirement(ctx).await?` | Close admission and schedule every managed child, including uncertain starts |
| Timeout, cancellation or host restart | Retain the cleanup claim and its next attempt; no application callback required |
| Retry cadence | Claim a retry 30 seconds ahead before I/O; each attempt is bounded to 20 seconds and the host's original deadline |
| DELETE acknowledged but resource still exists | Keep deletion pending and native capacity reserved |
| Last child confirmed deleted | Persist a finalization wake, then retain the parent tombstone |
| Parent class removed from deployment | The persisted 30-second discovery wake runs framework recovery, closes admission and retires managed children |
| Execution class removed | Use the original host executor and retained native container specification for destroy/inspect only |
| Manual reservations also present | Their controller must confirm deletion before parent retirement can finish |

Application alarms and business `delete_all()` cannot erase cleanup work.
`cleanup(ctx, cursor, limit)` remains available for inspection; pages contain at
most 256 rows, using indexed selection without a resident allocation map.

## Place new children by capacity

```mermaid
flowchart TD
    Start[Authenticated native start] --> Owned{Existing owner?}
    Owned -->|Yes| Route[Use existing owner]
    Owned -->|No| Local{Local container room?}
    Local -->|Yes| Admit[Acquire cell, then atomic Engine admission]
    Local -->|No| Fleet[Read fleet class-slot samples]
    Fleet --> Peer[Select live compatible peer with room]
    Peer --> Admit
    Fleet -->|No eligible peer| Refuse[Pre-execution capacity refusal]
    Cleanup[Destroy / inspect] --> Route
```

| Placement rule | Effect |
| --- | --- |
| Instance, class and memory bounds | Advisory slots include native helper memory and retained uncertain reservations |
| Missing class/capacity sample, expired lease, pressure or draining | Peer is ineligible for new native creation |
| Concurrent decisions against one sample | Account for prior selections; a newer sample supersedes that accounting |
| Peer refuses before execution | Invalidate its exact sample and retry within the original deadline |
| Lost response after submission | Stop; retain the same identity and unknown outcome |
| Existing owner | Preserve ownership; placement does not migrate a running sandbox or replay its start |
| Full Engine | Still admit ordinary cleanup/reconciliation routing |

Slot samples are advisory. Capacity may change after placement; a proven Engine
refusal keeps the generation available for a later attempt. An already-owned
child is not automatically relocated after an admission race. Native process and
filesystem recovery still require the separately qualified persistence profile.
Remote providers retain their own admission semantics.

## Erase retired cell data

Child retirement automatically queues its SQLite and replica erasure. Parent
business data is retained until an explicit `request_erasure(ctx).await?` (Rust)
or the guest equivalent above. Erasure refuses live children, active provider
bindings, retained facets, unfinished event-source retirement and consensus cells.

```mermaid
flowchart LR
    Intent[Durable SQLite erasure intent] --> Barrier[Permanent external identity barrier]
    Barrier --> Join[Evict and join cell runtime]
    Join --> Delete[Delete up to 100 replica objects and local entries per pass]
    Delete --> Verify[Observe empty replica subtree and absent local directory]
    Verify --> Receipt[Erased receipt]
    Barrier --> Refuse[Refuse admission and snapshot restore]
    Receipt --> Audit[Periodic audit for delayed writes and returning caches]
```

| Stored data | Result |
| --- | --- |
| Cell SQLite, WAL, local replica files | Removed after the resident runtime and writer stop |
| Current per-cell replica objects | Deleted in bounded batches; a later empty listing confirms removal |
| Ownership epoch | Retained to fence delayed ownership work |
| External barrier and receipt | Retained permanently outside the cell subtree; child identity is hashed |
| Delayed authenticated destroy | Exact identity can return confirmed retirement without restoring SQLite |
| Historical shared logs | Terminal barriers permit normal log GC and suppress replay into the erased cell |
| Offline volumes, bucket versions and backups | Governed by their own retention policies; this is not a secure-media wipe |

Use host `Node::sandbox_erasure_status(scope).await?` to observe `None`, `pending`
or `erased`. An erased receipt records successful collection of that collector's
files and the current shared replica objects. Other nodes audit permanent
barriers when running; an offline disk cannot be erased remotely. Never remove
`cell-retirement/` metadata or the local `cell-retirement.sqlite` database while
delayed calls, replicas or historical logs can still exist.

### Bound fleet collection

Each node consumes pending requests and permanent audit records incrementally.
Provider listing order is not assumed. Discovery and individual records have
separate validation; a corrupt record cannot prevent its neighbors from running.
Attempts retain their scope and concurrency slot until entered local filesystem
work returns, even if the async deadline expires.

```rust,no_run
use cellar::sdk::{NodeConfig, SandboxErasurePolicy};
use std::time::Duration;

fn configure(config: &mut NodeConfig) {
    config.maintenance.sandbox_erasure = SandboxErasurePolicy {
        concurrency: 8,
        scan_period: Duration::from_millis(250),
        attempt_timeout: Duration::from_secs(20),
    };
}
```

| Control | Default / bound |
| --- | --- |
| Concurrent attempts | 8 per node; configurable from 1 to 64 |
| Discovery interval | 250 ms; positive and at most 60 seconds |
| Discovery / individual attempt deadline | 20 seconds; positive and at most 60 seconds |
| Pending scan restart after exhaustion | 1 second; local publication wakes discovery immediately |
| Permanent audit restart after exhaustion | 30 seconds; the scan itself takes additional time |
| SQLite development-store listing | Indexed pages of 128 objects; read transactions close between pages |
| One deletion pass | At most 100 replica objects and 100 local entries |

Pending work normally receives three quarters of available discovery slots;
audits receive the remainder and any unused pending slots. Slow records retain
their own slots while completed work frees capacity for later batches. Driver
shutdown joins active dispatches and releases retained listing responses.
The queue and admission barriers survive cancellation or a failed attempt.

These bounds control per-node work, not the total completion time of an arbitrary
backlog. Size the fleet against measured provider/store latency and the recovery
target; preserve the permanent barriers when applying retention policies.

See the [fleet qualification guide](fleet-qualification.md) for the measured
profile, per-sandbox acceptance checks and repeatable commands.

## Delivery boundary

This development format uses a hard cutover: retained work tables without the
`sandbox-cleanup` reason, or child records without the retained container-spec
field, refuse activation/registration. There is no automatic schema migration.

| Available now | Still required before full production qualification |
| --- | --- |
| Four-language durable names, generations and parent ownership | Deployment-specific placement saturation, stale samples and recovery |
| Authenticated bounded execution and native capacity refusal | Wider partition and ownership-race campaigns |
| Explicit retirement and deployment-removal cleanup | Wider fleet/store failure campaigns and operational retention policies |
| Child and opt-in parent SQLite/replica erasure | Provider-specific persistent filesystem recovery |
| Provider adapter registration | Separate live/profile qualification of remote providers |

This profile does not promise restoration of native process memory or container
filesystem state after host loss. E2B/Vercel live qualification still requires
the separately supplied account configuration.
