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
allowing another Queue to progress while a handler waits. One retained callback
per cell keeps duplicate discoveries from consuming both slots. Its cell claim
survives response expiry through physical settlement; fixed two-entry metadata
and retained key bytes stay bounded. Idle cells get no tasks.

Queue wake hints preserve an earlier unclaimed installation across producer
commits. Rearming a claimed installation always replaces its generation, including
same/later deadlines, so old completion cannot erase a new retry or delayed
message. Ordinary alarm installations always replace their generation.
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

At `a5f65e79` and the combined KV/RPC source `9498029b`, the canonical
`queue-primitives` suite passes all six exact
real-RustFS selections in both image modes (**12/12 configurations**). It covers
native/Axum public producers and the captured CLI, separate deployment schemas,
ack/retry/DLQ delivery, retained restart, lost inter-group S3-DLQ completion,
late consumers, active purge, retention replay, leader transfer, quorum refusal,
a 65-second handler and bounded concurrent load. Eleven focused unit/ownership
checks, the Fleet Queue regression and strict library/all-test Clippy pass.
The unit/ownership, Fleet and all-test Clippy results retain `a5f65e79`.
See the [combined-source results](progress.md#cg-17-queue-qualification-with-kv-and-rpc-integration).

The load profile has four producers but at most two requests in flight, with
one per Queue and one SQL worker per node. Each image delivers 16 messages during
archive PUT refusal and 512 under steady load. At `9498029b`, batch enqueue p99 is 1487/1107 ms
for materialized/extents. This is the qualified envelope, with original request
budgets. Earlier four-in-flight and cold first-use S3 controls remain failed;
those envelopes were not rerun on the passing source. Expanded fault scenarios
observe archive readiness before S3-DLQ preparation. The backend is real RustFS;
archive refusal is injected at its PUT adapter, and voters share a native process.

Full CG-17 and its independent API/scenario/configuration matrix remain open.
Queue `max_concurrency` remains a ceiling; this shared owner runs one retained
callback per broker at a time on each node. Clock/deployment changes, movement,
wake, all-voter disk loss, full-size payloads and independent Linux voters/load
need qualification. EventStream, Workflow, cron and other work reasons still
need their own adapters. Delivery remains at least once, with application inbox
or external-provider idempotency at the documented business boundary.

The pending `queue_four_in_flight_preserves_acknowledged_messages` control
offers four producer calls concurrently, with at most two per Queue, alongside
the existing two-in-flight case. It retains the same 512 steady messages,
16 archive-outage messages, one SQL worker, eight ordinary slots, 128-KiB
ordinary byte ceiling and two callback slots. The fixture measures active
producer calls around each send and requires a peak of four. This measures
offered calls, not simultaneous physical socket transmissions. The RustFS
runner requires both image observations, the measured peak and the original
resource envelope before accepting its separate selection. Runner unit tests
pass; the new native and RustFS controls remain unqualified. Initial archive
warmup remains in place, so this control does not close cold first-use S3.

At optimized candidate input `4e9139b4`, the original two-call case passes both
images in 84.456 seconds. The four-call case fails in its materialized image in
26.398 seconds: an ingress command reports an ambiguous runtime deadline.
The fixture retains all three voter disks and joins cleanup. Its failure snapshot
shows one retained callback and SQL operation on the Queue leader, with a
committed entry waiting to apply and 286 apply retries. This observation does
not establish the blocking owner or a repair. The case remains failed, and the
extent image has no result. Failure-only call counters are added for the next
diagnostic run; successful-load observations remain separate.

At diagnostic input `5b61b5ad`, the four-call case fails again in 25.076
seconds. Failure counters record an actual peak of four offered calls, two
failed producers and zero calls still running when their results are collected.
No compiler or other native-test activity is sampled at the attempt's start.
The original admission, byte and callback limits are unchanged. This supplies
a repeatable current-source failure boundary; it does not yet prove which
checkpoint/apply owner prevents completion or qualify the extent image.

Targeted probes at input `34079f45` reproduce the failure and observe 198
apply refusals at the single reserved SQL progress slot. The byte-admission
and worker-result probes do not report those refusals; no checkpoint-wait probe
fires. These observations identify a contended admission boundary, not the
complete cause of the public request failure. All temporary probes are removed.

The bounded FIFO candidate published at `2e1852db`, input `7c0585ac`, makes
apply and SQL completions wait fairly for that existing progress slot. Apply
waiters retain their existing apply admission; completion waiters borrow an
ordinary slot while waiting. An idle progress slot remains available with a
full ordinary queue. Cancellation and shutdown release waiter admission. No
worker, slot, byte ceiling or request deadline is enlarged.

All six admission unit tests pass, as do formatting, layout and focused strict
Clippy for the core library and primitive integration target. The original
two-call load passes both image modes in 78.729 seconds, with 528 unique messages
per image and measured peak two. The four-call case still fails in its
materialized image in 26.205 seconds: ingress sequence nine returns an ambiguous
runtime deadline. Its failure counters confirm peak four, two failed producers
and zero active calls after joining their results. The extent image has no
result. FIFO admission therefore does not close the four-call progress gap;
the regression remains failed and real RustFS qualification remains pending.
