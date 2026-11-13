<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable native callbacks and Queue

Purpose: Generation-fenced callback discovery and ordinary Queue delivery through shared-group quorum.

This CG17-04/06 implementation reuses the existing cell-group private SQL owner,
checkpoint receipts, command admission and shared effect scheduler. Queue does
not create a Raft group, timer or background task for each broker or message.

## Durable callback contract

`RaftClass::durable_callbacks()` enables ordinary `Invocation::Alarm` callbacks.
Its host must configure `RaftEffectOptions` and checkpoint/receipt capacity.
The runtime-owned Queue service enables the same capability automatically.
Unsupported work reasons and unavailable scheduler capacity refuse admission.

Each installed reason has an independent generation, due time and attempt.
The committed manifest projects the earliest due time into its existing alarm
index; scheduling hints do not authorize execution. The owner rereads a fresh
linearizable image, then compares the complete installation in a private system
command. Public bindings cannot obtain this command contract or its client ID.

A successful claim installs a retry deadline and commits through the ordinary
quorum/S3 checkpoint before entering the callback. Callback code runs on the SQL
execution owner, outside Raft consensus polling. Followers apply captured bytes.
Completion removes only the claimed generation and attempt. Rearming another
generation, or a different reason, survives a late completion. An interrupted
callback leaves retry work in the published image, discoverable after retained
startup without an application request. The retry lease exceeds the scheduler's
maximum operation budget. Callback delivery is at least once; external effects
require application idempotency.

## Queue request and delivery paths

Deployment admission retains and hashes Queue producer bindings and consumer
policies. The runtime registers the reserved Queue service with the deployment's
ordinary native Queue handler. Declared Queue scopes use the existing binding
intent, enrollment, receipt and recovery machinery. Stateless native requests
use the node-owned built-in ingress allocator. Quorum selection refuses writes
when the shared service is unavailable; it cannot select Fleet execution.

The broker reuses the existing SQLite schema and Queue decision core for message
IDs, delays, batch leases, partial acknowledgement, retry limits, purge, retention,
dead-letter transfers and redrive. A Raft producer writes its admitted batch in
its own transaction. The host's consensus batching remains available; Fleet's
timer-based aggregation cannot combine Storage authority from separate Raft
invocations. Enqueue and its next due-work installation publish together.

Before invoking user code, the broker checkpoints the leased batch and its next
wake. The consumer receives the ordinary `Invocation::Queue` and `queue::Event`
APIs. Its Context has no cell identity or broker Storage capability. Declared
cell/SQL/Queue bindings retain the broker invocation's fenced source proof.
Acknowledgement and retry reuse exact message lease generations. An interrupted
consumer leaves leases for expiry and redelivery; committed target effects need
their own idempotency policy. Dead-letter transfers retain stable transfer IDs
and the destination's existing deduplication receipts.

The peer-authenticated operator entrance routes `cellar queue` operations to
the same broker. Its operator contract is absent from producer binding discovery.
The existing authorization binds method, path, body and addressed node. Operator
timeouts preserve an unknown mutation outcome rather than replaying the command.

## Qualification and remaining scope

The owning RF3 drill is `ordinary_queue_callbacks_use_shared_quorum`; the real
backend selection is `rustfs_ordinary_queue_callbacks_use_shared_quorum` in
`native_cell_groups_primitives`. Both materialized and extent images are selected.
The scenario exercises public native ingress, declared producer bindings,
acknowledgement, retry, dead letters, authenticated controls, an injected archive
write outage, retained restart without a new producer request, leader transfer,
exact original request replay and quorum-loss refusal. Kernel unit tests cover
generation replacement, expired attempts and rollback. Test results must be
recorded before treating any of these scenarios as qualified.

These selections do not complete CG-17 or its independent coverage matrix.
EventStream, Workflow, cron and the other framework work reasons still require
their own callback adapters. Cross-deployment Queue consumer catalogs, deployment
removal, clock-discontinuity faults, all-voter-disk-loss recovery and independent
Linux voter resource/load qualification remain separate gates. A real RustFS
backend with host-native RF3 voters is not an independent-host resilience result.
