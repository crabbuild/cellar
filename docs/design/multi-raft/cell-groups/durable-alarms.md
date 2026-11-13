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

## Qualification and remaining work

The `ordinary-alarms` RustFS suite covers both image forms and Quorum/S3
namespace minima through native/Axum ingress, transactional cancellation and
rollback, same-deadline replacement, pinned-image reads, stale Context refusal,
interrupted retained restart without client traffic and quorum refusal. Its
separate admission regression rejects missing/ambiguous namespaces before any
cell factory runs. These tests exercise no operator CLI. Consult the
[progress record](progress.md) for actual source-pinned results; implemented
tests do not establish a passing qualification.

Ordinary failure/backoff exhaustion, archive outages, movement/wake and
independent voter/disk-loss qualification remain required. The current claim
lease is not full parity with `cellar_logic::alarm::alarm_retry`. Cron still
requires its deployment/startup adapter, persisted occurrence/history authority
and matching shared alarm projection. Reuse the existing cron state machine;
do not silently admit unsupported declarations. EventStream and Workflow remain
separate deliveries. Full primitive compatibility and the combined release gate
remain in the [delivery priorities](remaining-work.md#primitive-delivery-priority).
