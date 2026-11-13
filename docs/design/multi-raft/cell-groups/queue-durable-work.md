<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable native callbacks and Queue

Purpose: Generation-fenced callback discovery and ordinary Queue delivery through shared-group quorum.

This CG17-04/06 implementation reuses the existing cell-group private SQL owner,
checkpoint receipts, command admission and shared effect scheduler. Queue does
not create a Raft group, timer or background task for each broker or message.

## Durable callback contract

`RaftClass::durable_callbacks()` enables ordinary `Invocation::Alarm` callbacks.
Its host installs bounded `RaftEffectOptions` defaults and validates checkpoint/receipt capacity.
The runtime-owned Queue service enables the same capability automatically.
Unsupported work reasons and unavailable scheduler capacity refuse admission.

Native callbacks use `RaftEffectOptions::callback_timeout_ms`, defaulting to
300 seconds and bounded to 1–300,000 milliseconds. Scan/control RPCs retain
`operation_timeout_ms` (5 seconds by default, at most 60 seconds). The callback
budget includes checkpoints, binding waits and settlement; Fleet's handler
budget does not extend it. Startup refuses a retry horizon shorter than the
callback budget plus its one-second lease margin. Public routing retains its
60-second ceiling: only the private native-work command can use the extended
budget. The changed private command digest requires fresh compatible namespaces
and matching voters; format markers remain 1, with no legacy reader.

Each installed reason has an independent generation, due time and attempt.
The committed manifest projects the earliest due time into its existing alarm
index; scheduling hints do not authorize execution. The owner rereads a fresh
linearizable image, then compares the complete installation in a private system
command. Public bindings cannot obtain this command contract or its client ID.

A successful claim installs a retry deadline and commits through the ordinary
quorum/S3 checkpoint before entering the callback. Two node-wide callback slots share one `max_command_bytes` ceiling. Discovery
transfers admitted work to the joined command controller and resumes scanning,
allowing another Queue to progress while a handler waits. Idle cells get no tasks.
Ordinary request and S3 settlement capacity remain independent; startup resource
bounds reserve all three ceilings, and metrics expose each lane. Retention and
quiescence include callbacks after response expiry. Callback code runs on the SQL
execution owner, outside Raft consensus polling. Followers apply captured bytes.
Completion removes only the claimed generation and attempt. Rearming another
generation, or a different reason, survives a late completion. An interrupted
callback leaves retry work in the published image, discoverable after retained
startup without an application request. The retry lease exceeds the scheduler's
admitted callback budget. Callback delivery is at least once; external effects
require application idempotency.

## Queue request and delivery paths

Deployment admission retains and hashes Queue producer bindings and consumer
policies. `RaftApplications` resolves each loaded namespace by its exact admitted
schema across cohosted native deployments, without invoking cell factories.
Native daemon startup supplies that immutable catalogue. Required code absent
from the catalogue refuses startup; a same-named class from another deployment
cannot stand in for it.

Queue names select the consumer namespace within the source cluster. An explicit
consumer owns its queues; a namespace with producer bindings and no consumers
can explicitly hold storage-only queues such as a DLQ. Dead-letter references
supply fallback ownership only when no explicit owner exists. Duplicate explicit
owners or ambiguous fallbacks refuse startup/routing. Adding a producer does
not relocate stored messages. Removing a required deployment refuses service;
automatic obligation migration on deployment removal remains a separate gate.
Producer, consumer and DLQ definitions may have different schemas and groups.
A producer needs its outbound binding and its own application namespaces; it
must not provision another Queue namespace for a queue already owned by its
consumer. Provisioning a storage-only Queue namespace declares ownership, so
competing declarations refuse. Automatic deployment changes that move existing
Queue ownership remain unsupported.
Source binding allowlists, namespace grants and independent destination minimum
durability remain enforced. A lost cross-group DLQ response retains the original
transfer ID for recovery rather than allocating a new enqueue.

Declared Queue scopes use the existing binding intent, enrollment, receipt and
recovery machinery. Stateless native requests use the node-owned built-in ingress
allocator. Quorum selection refuses writes when the shared service is unavailable;
it cannot select Fleet execution.

The broker reuses the existing SQLite schema and Queue decision core for message
IDs, delays, batch leases, partial acknowledgement, retry limits, purge, retention,
dead-letter transfers and redrive. A Raft producer writes its admitted batch in
its own transaction. The host's consensus batching remains available; Fleet's
timer-based aggregation cannot combine Storage authority from separate Raft
invocations. Enqueue and its next due-work installation publish together.

A Raft callback leases at most one batch; remaining readiness schedules another
turn. A stalled handler cannot consume attempts for batches it never received.
Raft leases use the callback's remaining monotonic budget plus settlement time;
the unrelated Fleet handler budget does not delay redelivery.
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
The scenario exercises public native/Axum ingress, the actual Queue CLI, declared producer bindings,
acknowledgement, retry, dead letters, authenticated controls, an injected archive
write outage, retained restart without a new producer request, leader transfer,
exact original request replay and quorum-loss refusal. Kernel unit tests cover
generation replacement, expired attempts and rollback. At `efd9c4c9`, the native
and Axum RustFS cases pass in both image modes (**4/4**), including the actual
captured CLI and direct stateless public refusal after quorum loss. The existing
Fleet Queue regression and focused strict Clippy pass. See the
[source-pinned results](progress.md#cg-17-durable-callbacks-and-queue-mvp).

These selections do not complete CG-17 or its independent coverage matrix.
EventStream, Workflow, cron and the other framework work reasons still require
their own callback adapters. Cross-deployment Queue consumer catalogs, longer consumer execution, deployment
removal, lost DLQ replies across groups, active-delivery purge, retention and
clock-discontinuity faults, all-voter-disk-loss recovery and independent
Linux voter resource/load qualification remain separate gates. A real RustFS
backend with host-native RF3 voters is not an independent-host resilience result.
