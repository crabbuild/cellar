<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Ordinary durable alarms, cron and native ingress

Purpose: Define shared-group alarm, cron and stateless named-cell ownership and qualification boundaries.

Register an ordinary factory with `Registry::raft_cell` and enable
`RaftClass::durable_callbacks`. The ordinary `Storage::set_alarm`, `get_alarm`
and `delete_alarm` APIs use the cell's private Raft transaction. Transaction
rollback restores its prior installation; acknowledgement follows the selected
namespace's required durability. No idle cell owns a scheduler task or timer.

## Public named-cell ingress

Stateless native and Axum handlers can use declared exported classes and named
bindings through the ordinary `Context`/`Namespace`/`Stub` RPC API. The host
selects exactly one loaded namespace with the compiled class and immutable
deployment configuration. Missing or ambiguous namespaces refuse the request.
Undeclared classes and framework work/query operations do not gain authority
through this adapter. Stateful Raft callers retain their source-journaled
binding protocol and proof requirements.

Provision the deployment's explicit namespace query/command peer grants for
forwarding; voting membership alone supplies no application RPC permission.
The positive alarm fixture waits for a serving RF3 group, then forces the first
cold-cell RPC through a follower. It does not pre-enroll or invoke the cell.
Cold Raft-group startup within a three-second operation budget remains separate
from that gate; the fixture retains its original three-second operation deadline
and six-second HTTP client timeout.

Native and built-in SQL/KV/Queue ingress share the node-owned admission and
command pipeline. Calls retain bounded request/reply storage, one original
deadline, nonce-bearing input and a persistent client stream per cell.
Name lookup may wait for transient readiness within that same deadline before
enrollment or business submission. This wait never retries a submitted mutation.
A fresh quorum observation of a different retained fingerprint is required before a
competing sequence can be retried. An unknown submitted mutation retains its
history and identity; a timeout does not establish non-commit. Missing quorum
or S3 proof never selects Fleet or weaker durability.

The runtime reserves the ingress sequence query and includes this native
contract in the compiled class identity. Use fresh compatible namespaces and
matching voters for this internal cutover. Format markers remain 1.

## Callback ownership

The shared effect scanner discovers the committed installation. A callback
enters application code only after its exact generation claim satisfies quorum
and any stronger namespace minimum. Followers apply bytes rather than invoking
the callback. Work retains the existing node-wide callback slots and byte limit.

Inside an alarm callback, both invocation and constructor-cached Storage hide
the active installation. A newly armed generation is visible immediately, even
at the identical deadline, and old completion cannot remove it. The active
generation belongs to the command frame, not the resident activation. Ordinary
commands and pinned immutable queries observe their own image without acquiring
callback authority. A retained old Context cannot enter a subsequent invocation.

An interrupted callback retains its committed claim and becomes discoverable
after its lease expires. Recovery is at least once; external effects require
application idempotency. `callback_timeout_ms` defaults to 300 seconds and is
bounded to 300 seconds; the lease includes one additional second. The focused
alarm fixture selects a five-second callback budget, matching the existing
bounded Queue fixture. Public request budgets remain unchanged.

## Returned handler failures

A returned application error or caught callback panic after the claim proof
rolls back the attempt's
unsynced application writes. Earlier explicit checkpoints remain committed.
The SQL owner then records retry state in a separate framework transaction and
publishes it through the existing quorum/S3 command path. Failed instances are
retired; their mutated in-memory state cannot return to the activation cache.

Retry state follows `cellar_logic::alarm::alarm_retry`: a two-second base with
exponential backoff and up to six retries after the initial attempt. Claim
attempts and counted application failures are separate. A failed claim proof,
kernel deadline, cancellation or lost authority does not fabricate a returned
handler failure; its retained claim remains recoverable after the lease.

The test profile unwinds, so caught callback panics use this failure path.
Shipped release builds use `panic = "abort"`; an abort is process loss and
recovers through the retained claim/lease. It cannot fabricate a known failure
count. Release process-loss qualification remains a separate gate.

Failure and completion compare the installation generation, attempt, exact lease
and claim phase. Backoff clears that phase before the next attempt can claim it.
The phase is necessary even when the backoff and old lease deadlines coincide.
An old completion cannot remove a persisted retry, committed replacement or
cancellation. The committed due index drives recovery without per-idle-cell
work. Callbacks remain at least once; independent checkpoints and external
operations still require application idempotency.

The private callback contract binds the changed framework schema. Use fresh
compatible namespaces and matching voters for this internal hard cutover;
format markers remain 1 and no legacy schema reader or migration is installed.
The public failure/restart fixture fences and joins every old voter, then
asserts the retry did not run before reopening retained disks. This separates
pending-work recovery from a retry that raced a sequential shutdown. The
exhaustion fixture preserves the full ordinary retry schedule. At `9a6055bd`,
failure/restart passes all 12 native and real-RustFS native/Axum image/minimum
configurations; full RustFS exhaustion passes four configurations. The fixture
observes the synchronous return/unwind after the panic hook, matching the
runtime's caught-failure boundary. Its timing bounds and budgets are unchanged.
Ten owning SQLite tests and native exhaustion retain their separate passing
source `dbc6e979`. See the
[source-pinned checkpoint](progress.md#cg-17-ordinary-alarm-failure-retries).
Earlier request/callback deadline controls remain unresolved release concerns;
a passing component run does not erase them or qualify the complete family.

## Ordinary deployed cron

Select shared services for a cron-only application with
`Registry::raft_primitives()`; no application-owned cell class is required:

```rust
use cellar::sdk::{Output, Registry};

let application = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
    .raft_primitives()?;
```

Supply ordinary cron expressions through the deployment configuration and handle
`Invocation::Scheduled` in the application's stateless handler. Existing ordinary
Raft cell factories already select the same service adapters. An unselected
Registry retains Fleet behavior; selected unsupported capabilities refuse.
Registration creates a reserved `.cron` definition bound to the script,
expressions and deployment/history
configuration. Provision its matching namespace before managed startup; a missing
or mismatched namespace refuses startup. Duplicate schedule scopes refuse before
log opening. Changed/removed declarations refuse instead of silently dropping
retained obligations.

The compiled namespace provision plan selects class `.cron`, supplies matching
`configuration.crons` and `configuration.cron_history` (prefix, deployment code
version and schedule revision), and selects `minimum_durability` as `quorum`
(default) or `s3`. Missing/mismatched metadata and invalid minima refuse. The
receipt reports the selected minimum; identical provisioning requests reconcile
the same receipt. Minimum durability governs protected write output, independently
of read consistency.

One bounded, joined bootstrap owner per node resolves or enrolls the schedule
using a stable creation identity. A retained history is never reinitialized.
The committed alarm projection drives ordinary discovery and callback leases;
there is no per-idle-cell cron task. Installation checks include a claimed alarm
even when the application-facing `get_alarm` hides that active generation.

The existing cron state machine persists its owed occurrence, attempt and history
through `Storage::sync` before invoking the stateless scheduled handler. It also
checkpoints each expression's settlement before continuing. Retained restart
resumes the same occurrence and preserves ordinary retry/no-retry semantics,
declared variables and Context retirement. External side effects remain at least
once and require their own idempotency contract.

Signed cron history inspection resolves the configured namespace and reads a
linearizable pinned image within the original request budget. Trusted framework
reads use read authority and reject non-read-only statements before stepping;
ordinary SQL still cannot access framework tables or mutate through query Storage.

At `13b93404`, basic autonomous cron and all-voter joined retained restart pass
four native and four real-RustFS image/minimum configurations. Declaration
refusals, framework read mutation/privacy guards and ordinary Fleet cron also
pass. The fixture explicitly selects a 30-second callback budget; public request
budgets and production quotas are unchanged. Its RF3 voters share one process
with distinct disks and one SQL worker each. See the
[source-pinned checkpoint](progress.md#cg-17-ordinary-cron-startup-and-retained-recovery)
for negative controls and complete qualification limits. Workflow-specific
schedules remain a separate delivery.

At `17846604`, the same eight native/RustFS configurations pass with no registered
application cell class, alongside explicit selection/refusal and Fleet regression
checks. At `eddda0f2`, compiled CLI provisioning, first-group operator admission
and three independent native daemon processes pass four real-RustFS image/minimum
configurations. Both native and Axum serve linearizable history, all old processes
join before retained restart, and losing two voters refuses both read paths.
The fixture follows the existing ordered boot-admission workflow. Concurrent
startup conflicts in cohort 99 remain retained and unresolved. These are native
processes on one development host, not independent machines or Linux cgroups.
See the [service checkpoint](progress.md#cg-17-cron-services-through-the-daemon-and-cli).

## Qualification and remaining work

The `ordinary-alarms` RustFS suite covers both image forms and Quorum/S3
namespace minima through native/Axum ingress, transactional cancellation and
rollback, same-deadline replacement, pinned-image reads, stale Context refusal,
interrupted retained restart without client traffic and quorum refusal. Its
separate admission regression rejects missing/ambiguous namespaces before any
cell factory runs. These tests exercise no operator CLI. Consult the
[progress record](progress.md) for actual source-pinned results; implemented
tests do not establish a passing qualification.

At `d22ef830`, the native and real-RustFS native/Axum alarm selections pass all
12 image/minimum configurations. The combined cohort remains failed by its
separate Queue enrollment and archive-deadline regression guards; see the
[source-pinned checkpoint](progress.md#cg-17-ordinary-native-ingress-and-durable-alarms).

Archive outages, movement/wake and independent voter/disk-loss qualification
remain required. Ordinary cron additionally needs deployment transitions and
clock discontinuities, concurrent managed boot reconciliation and release
process-loss coverage. EventStream and Workflow remain
separate deliveries. Full primitive compatibility and the combined release gate
remain in the [delivery priorities](remaining-work.md#primitive-delivery-priority).
