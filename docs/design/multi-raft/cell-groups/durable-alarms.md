<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Ordinary durable alarms and native ingress

Purpose: Define shared-group alarm and stateless named-cell ownership and qualification boundaries.

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
The public failure/restart and exhaustion fixtures are implemented. Their
qualification is pending; the earlier foundation checkpoint does not qualify
this new failure path.

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

Failure/backoff exhaustion qualification, archive outages, movement/wake and
independent voter/disk-loss qualification remain required. Cron still
requires its deployment/startup adapter, persisted occurrence/history authority
and matching shared alarm projection. Reuse the existing cron state machine;
do not silently admit unsupported declarations. EventStream and Workflow remain
separate deliveries. Full primitive compatibility and the combined release gate
remain in the [delivery priorities](remaining-work.md#primitive-delivery-priority).
