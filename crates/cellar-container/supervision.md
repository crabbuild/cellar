<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Host-managed provider cleanup

[Container guide](README.md) · [Durable bindings](bindings.md) · [Provider contract](providers.md)

E2B and Vercel resources can outlive the request that created them. Register
their maintenance backends to inspect and retire those resources through native
internal work. The same registration wraps Rust, Python, JavaScript or TypeScript
applications; cleanup does not need a guest runtime or an application alarm.

## Configure once in the host

```rust,no_run
use anyhow::Context as _;
use cellar::sdk::{containers, Application};
use cellar_container::Provider;
use std::sync::Arc;

fn managed(
    application: Arc<dyn Application>,
    provider: Arc<dyn Provider>,
) -> anyhow::Result<Arc<dyn Application>> {
    let mut supervision = containers::Supervision::default();
    supervision.register(
        provider.maintenance_backend().context("provider has no maintenance backend")?,
        containers::MaintenancePolicy {
            inspect_interval_ms: 10_000,
            maximum_lifetime_ms: 300_000,
            attempt_timeout_ms: 5_000,
            idle_timeout_ms: Some(60_000),
        },
    )?;
    Ok(containers::supervise(application, supervision))
}
```

Use this same provider in the application's adapter or named provider slot.
Register each original configuration needed by outstanding generations. A changed
credential can rotate independently; a changed configuration digest names a
different backend identity. Missing registrations leave cleanup pending.

When cohosting applications, share one backend `Arc` and policy for any repeated
identity. Startup rejects distinct registrations for that identity because the
recovery executor must choose one unambiguous backend configuration.

| Policy | Meaning | Accepted milliseconds |
| --- | --- | --- |
| `inspect_interval_ms` | Schedule the next observation before issuing this attempt | 10–3,600,000 |
| `maximum_lifetime_ms` | Fixed ceiling from the durable creation reservation | 10–86,400,000 |
| `attempt_timeout_ms` | Bound one maintenance attempt; the native handler budget also applies | 10–120,000 |
| `idle_timeout_ms` | Optional retention since the last successful command/file interaction | Positive and no greater than the maximum lifetime |

The policy is persisted for that resource generation. Changing registration
defaults does not extend an existing lifetime. Provider session timeouts remain
separate: a provider can remove compute earlier than the local retention ceiling.
These deadlines bound local admission and schedule cleanup. An unavailable
provider or owner can delay retirement; a hard remote stop requires a separately
qualified provider-enforced lease.

## One durable obligation per resource generation

```mermaid
sequenceDiagram
    participant App as Application event
    participant Slot as Binding admission
    participant DB as Protected cell SQLite
    participant Host as Native internal work
    participant Provider
    App->>Slot: Acquire activation lifecycle guard
    App->>DB: Atomically reserve binding + policy + internal deadline
    DB-->>App: Required durability proof
    App->>Provider: Submit create once
    Provider-->>App: Observed or uncertain outcome
    App->>DB: Checkpoint evidence; retain unknown intent
    App->>Slot: Release after checkpoint durability
    Note over App: Application event ends
    Host->>Slot: Acquire the same binding guard
    Host->>DB: Recheck installation and exact task
    Host->>DB: Claim due task; persist the next attempt
    DB-->>Host: Required durability proof
    Host->>Provider: Inspect original identity; delete when due
    Provider-->>Host: Correlated observation or uncertain outcome
    Host->>DB: Checkpoint exact generation/revision; rearm or retire
    Host->>Slot: Release after checkpoint durability
```

| Boundary | Behavior |
| --- | --- |
| Scheduler | Existing native `resource-reconciliation` work; application alarms keep their own deadlines and retry state |
| Authority | A fresh finite native event for each attempt; original activation/invocation checks and output proof |
| Lifecycle overlap | Application start/destroy/reconcile and maintenance share a per-binding activation guard; reread the installation and task after waiting, before claiming or contacting the provider |
| Completion | Task generation/version and binding revision reject superseded results |
| Command/file result | Require the original confirmed running generation before and after activity durability; a late result after replacement is uncertain |
| Expiry | Earlier of fixed maximum lifetime and optional idle deadline; inspection does not count as activity |
| Late operation | Cannot revive an expired generation or extend its maximum lifetime |
| Business `deleteAll` | Protected binding and cleanup records survive |
| Unknown old creation time | Enabling supervision on an existing unmanaged binding makes it immediately due for retirement |
| Limits | At most 64 retained bindings per cell and 256 backend registrations; one due task per internal event |

Command/file admission returns `ResourceExpired` after local retention expires.
Read-only reconciliation and destruction remain available. A create reservation
that expires before submission can retire with that explicit non-submission
evidence. An earlier uncertain create cannot use that shortcut.

## Lost responses remain cleanup work

| Observation | Controller action |
| --- | --- |
| Create accepted, response lost | Find the original request/resource by verified metadata; never create again |
| Empty listing for an uncertain create | Keep the reservation and retry observation |
| Backend unavailable or wrong configuration | Retain the obligation; no substitute backend dispatch |
| Attempt fails or times out | Keep the task; bounded failure accounting and retry delay capped at 60 seconds |
| DELETE accepted, resource still visible | Keep deletion pending |
| DELETE response lost | Inspect; repeat deletion only for a backend explicitly declaring exact-resource revalidation |
| Verified retirement | Remove that generation's maintenance task and preserve its binding tombstone |

`MaintenanceBackend` belongs to the native host contract. Providers implement
`identity`, `maintain` and optionally `delete_safety`; `maintain` must correlate
the supplied checkpoint and must never allocate compute. E2B/Vercel re-inspect
the exact allocation/session and verify metadata before repeated deletion. A
generic backend defaults to inspection after an uncertain deletion.
The controller holds lifecycle admission while calling `maintain`; backend
implementations must use their journal directly and must not reacquire the guard.

## Qualification boundary

This delivers optional scheduled maintenance for the experimental E2B/Vercel
protocol implementations. It does not strengthen their
`local_checks_and_expiry` fencing declaration. A submitted remote command can
continue during ownership loss, timeout or deletion.

Durable tasks use native wake projection and recovery. The fleet acceptance
fixture uses actual native replication, wake discovery, ownership and node logs
over a private local object store, with fresh node directories.

| Fleet scenario | Verified boundary |
| --- | --- |
| Same-name cold restart | Retire the original generation before any application/operator request |
| Removed application class and lost create response | Reconcile the original request and retire it without a business invocation or cell factory |
| Accepted create during owner suspension | A 2.5-second suspension exceeds the 1-second lease; the old domain fences without success and the successor cleans the pending resource |
| Handoff and cold verification | A live receiver reads the same retirement record, business data and user alarm after native ownership transfer |
| Provider API outage through two cold replacements | Preserve the original obligation and its published retry deadline; restore the provider and retire without business construction or a cleanup-driving request |
| Owner-specific object-store outage | Refuse actual owner I/O, expire its lease and close its domain; an independent successor retires the same resource while the old owner's clients remain isolated |

Each scenario runs against both E2B and Vercel protocol peers and asserts one
create, one delete and joined node shutdown. Provider effects execute through
the real adapters. These peers do not execute cloud VMs or prove account behavior.
A standalone node without replication still cannot automatically discover all
untouched cells on cold startup.

Native Docker/Podman keeps its existing engine lifecycle and cleanup path.
Live E2B/Vercel accounts, broader outages and network partitions, physical cell
deletion and stronger remote fencing remain outstanding. See the
[delivery ledger](../../docs/design/sandbox-provider-progress.md) for current evidence.
