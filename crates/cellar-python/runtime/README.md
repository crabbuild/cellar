<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Python runtime internals and diagnostics

This guide explains implementation invariants behind the
[Python adapter](../README.md). Application examples, API usage, deployment
choices, and default limits belong in that guide.

## Wake the loop without executing Python on producers

```text
Native producer                        Managed asyncio loop
      |                                         |
 enqueue owned message in bounded channel       |
      |                                         |
 coalesce readiness byte ---------------------->| socket reader
      |                                         |
 return without GIL / Python / waiting          drain notification
                                                |
                                       pump starts and replies
                                                |
                                       yield to other tasks
```

The nonblocking Unix socket pair carries readiness only: no request payload or
SDK authority. Admission, replies, cancellation, activation retirement, and host
completion wake the loop. Notification failures fail the runtime; they cannot
be treated as proof of clean shutdown. The loop removes its reader before the
runtime closes the sockets.

| Scheduling rule | Reason |
| --- | --- |
| At most 64 starts per pump pass | Continuous arrivals, including task-capacity refusals, must yield to timers and cancellation. |
| Deferred continuation for remaining starts | Queued work does not wait for another native notification. |
| Eager handler start until first await | Inline completion avoids an extra dispatch turn while retaining the task factory and invocation context. |
| `poll_interval` bounds housekeeping, not normal admission | Notifications wake ordinary work immediately; telemetry deadlines can shorten the wait. |
| Coalesced invalidation/retirement flags | Scan waiting invocations after invalidation and resident objects after owner retirement, rather than scanning everything for ordinary replies. |
| Consume flags before scanning | Concurrent invalidations remain visible for a later pass. |
| Completion queue | Release finished tasks and contexts without scanning unrelated waiting calls. |

A longer housekeeping interval delays discovery of background thread/unraisable
errors and notification-channel faults. It does not delay ordinary admission,
replies, or retirement. Individual handlers remain cooperative and can block
the interpreter despite the pump's fairness rules.

See [wakeup.rs](wakeup.rs), [lifetime.rs](lifetime.rs), and
[runner.py](../python/runner.py).

## Revoke authority before reporting cleanup

```text
Managed invocation finishes
            |
            v
   revoke its SDK sender
            |
   cancel pending SDK futures
            |
   mark managed cleanup complete
            |
   native return may skip a redundant cancellation wakeup
```

Expiration alone never suppresses the native cancellation notification: a
still-running task must wake even if its bridge is already expired. A handler
that cancels itself wakes its native driver even when application code retains
the context. Failed cleanup remains registered for shutdown. Operation and reply
admission still check authority independently of these cleanup optimizations.

| Object or task | Ownership / release rule |
| --- | --- |
| `ctx.storage` handle | Keeps its originating context alive; the context does not cache it, avoiding a normal storage/context cycle. |
| Resident cell | Registry retains an owner-lifetime token, not completed invocation senders, caller scopes, or event-loop references. |
| Retired cell | Owner revocation wakes cleanup even when idle. Registry removal does not collect application-retained aliases. |
| Managed task | Factory enforces `max_tasks`, including callbacks with an empty context; each admission scans the live task registry once. |
| Refused invocation | Closes its unstarted coroutine and context; handler never starts. |
| Shutdown helpers | Up to two explicit supervisor tasks can exceed the ordinary task budget to join work; reported peak includes them. |
| Raw tasks, threads, private native pools | Trusted application code owns them. Replacing the factory or constructing raw tasks is not isolated by these cooperative limits. |

Async generators and the managed default executor close while the event loop is
alive. Remaining tasks or Python threads make the report incomplete. Exceptions
raised during task cancellation use the fixed `task_cleanup` failure stage even
when the pending-task count reaches zero. Constructor failures must not fabricate
successful object creation/retirement counts.

## Byte accounting and failure boundaries

```text
admission --> reserve bytes / mailbox --> copy --> execute --> encode --> reply
                   |                       |          |          |
             refused sends             original      |     result may fail
             release charge            SDK context   |     after effects
                                                      v
                                                effect may commit
```

| Accounting boundary | Contract |
| --- | --- |
| Invocation mailbox | `max_pending_operations` bounds operation/completion entries; zero and unsupported capacities fail before bootstrap. |
| Result mailbox | At most twice `max_invocations` replies. |
| HTTP ingress | Immutable body bytes travel beside JSON metadata. Interpreter copying occurs while the native reservation is held. |
| HTTP completion | Reserve native byte and mailbox capacity before copying into Rust; retain the reservation through response validation. |
| Nested fetch | Request and reply bodies use the same separate-byte path and encoded-envelope/aggregate limits. |
| Mutable application buffers | Copy into immutable bytes before submission. |
| Ordinary HTTP metadata | Shallow encoder snapshots headers with identical JSON escaping/key order; unusual metadata and subclasses use the general SDK encoder. |
| Atomic batch | The entire serialized UTF-8 JSON batch fits `max_value_bytes`, not merely each row. Oversized batches apply no partial writes or alarm changes. |
| SDK reply | Depth limit applies to the returned value before its transport envelope; complete encoded success envelope must fit the byte budget. |
| Minimum result budget | At least 128 bytes for fixed failure envelopes. |

Binary bodies are charged at their base64-equivalent length without constructing
that encoded body. Queued, refused, and cancelled work releases its reservations.
These charges exclude the Python heap, codec temporaries, and native transport
copies; they are not a whole-process memory cap.

A result that cannot be encoded becomes `HostError/unknown` for a known read or
`Ambiguous/unknown` for a possible mutation. The invocation remains usable and
no operation is retried. Native-operation success metrics describe execution,
independently of whether the result crosses the bridge.

A completion refused by its mailbox or aggregate budget fails the invocation
and revokes its sender. Even refusal of the fixed failure envelope leaves the
runtime available for later work. None of these failures proves rollback.

Multi-key reads and initial capture retain at most one decoded stored value
beside the bounded encoded output. They do not collect every value before
checking its size. Read replies return no partial result. Captured fields share
the invocation budget with metadata and the encoded-equivalent body.

See [budget.rs](budget.rs), [bridge.rs](bridge.rs),
[read_batch.rs](../read_batch.rs), and [failure.rs](../failure.rs).

## Initialization, signals, and diagnostic privacy

```text
exclusive main-thread bootstrap
            |
verify inventory and native providers
            |
temporarily discard C initialization stderr
            |
initialize CPython -> flush C output -> restore stderr and flags
            |
import signal; remove implicit default INT handler
            |
import SDK / application; check loaded providers again
            |
start Rust supervisor and normal host/logging workers
```

CPython can print configured paths from C before Python error hooks exist.
Bootstrap therefore redirects process stderr only around C initialization;
a closed descriptor stays closed, and restoration failure refuses startup.
Unrelated threads' stderr writes during that interval would also be discarded:
start host/logging workers afterward and never concurrently replace or close
that descriptor. Later logging uses the original destination.

Disabling initialization signal handlers alone is insufficient: importing
`signal` can install CPython's default INT handler later. Bootstrap imports it
before asyncio and removes that implicit handler while preserving existing
native handlers. Rust retains INT/TERM shutdown ownership.

| Diagnostic source | What crosses the managed boundary |
| --- | --- |
| ABI, filesystem, inventory, distribution, or C initialization failure | Bounded stage messages; paths and expected/actual private values are omitted. |
| Application import, registration, cold descriptor, or handler exception | Fixed failure information without calling arbitrary exception formatting or retaining values/traceback paths. |
| Unraisable/destructor or unhandled Python-thread exception | Failure counters and an incomplete report; default exception printing is replaced before app imports. |
| Rust supervisor failure | Typed native outcomes and its own error source remain in the combined error; host logging controls that source. |
| Explicit app/library output or OS loader diagnostics before `main` | Ordinary process output; embedding is not an output sandbox. |

The owned allocation tracer stops on normal return, host failure, import failure,
and cold validation failure. When tracing is disabled, bootstrap does not stop
an application-owned tracer.

See [initialization.rs](initialization.rs),
[initialization_output.rs](initialization_output.rs),
[environment.rs](environment.rs), and [inventory.rs](inventory.rs).

## Interpret timing and memory measurements

```text
SDK operation
   admitted       native start          native done        loop reply dequeue
      |                 |                    |                     |
      +-- queue wait ---+-- SDK duration -----+-- reply delay ------+
      |                                                            |
      +---------------- admission-to-dequeue latency ---------------+
                                                                   |
                                                     Python consumes later
                                                     (not in reply timing)
```

`bridge.operations` records queued, running, and pending-reply work, successful
and failed native execution, delivered replies, and separate queued/running/
undelivered abandonment. Dropped native futures retain duration and terminal
accounting without calling Python. Balanced terminal counts do not imply that
abandoned mutations rolled back.

| Measurement | Includes | Does not establish |
| --- | --- | --- |
| Interpreter thread CPU | Python and native code on that thread. | Executor/native-pool CPU or Rust-only cost. |
| Handler wall time | Host awaits; simultaneous handlers overlap. | Process elapsed time from the sum. |
| `loop_delay` | Ready-queue checkpoint delay and housekeeping lateness, including blocking Python, GIL contention, and OS scheduling. | A unique cause; time awaiting host I/O is not loop delay. |
| Native SDK duration | Parsing, validation, and host await time. | Rust CPU time. |
| Reply delay | Encoding and managed-loop scheduling up to dequeue. | Later consumption of the Python future. |
| Process peak RSS / `Report::memory` | Whole-process observations before initialization and after joined cleanup; available jemalloc/libc counters. | Per-cell or Python-only memory usage; CPython remains initialized. |
| CPython allocated blocks | Initial, final, and sampled interpreter block counts. | Every native library allocation. |
| Traced current/peak bytes | Registered CPython allocation domains when `trace_allocations` is enabled. | Private extension allocators; disabled/unavailable measurements are `None`. |

Loop checkpoints cover inline pump/handler work and follow resumed handlers, so
long housekeeping waits do not hide short blocking handlers. Histograms have
inclusive upper bounds of 1 us, 10 us, 100 us, 1 ms, 10 ms, 100 ms, 1 s, and 10 s,
then an unbounded bucket. Counts are noncumulative and totals saturate; use
`LatencyReport::upper_bounds_ns()` for the exact layout.

| Bridge byte counter | Interpretation |
| --- | --- |
| Queued / peak charges | Native payload reservations, including encoded-equivalent body charges. |
| `reserved_bytes_total` | Successful reservations, even if later abandoned. |
| `input_copied_bytes` / `output_copied_bytes` | Explicit owned UTF-8/raw-body copies; exclude codec temporaries, Python UTF-8 caches, and transport/library copies. |

Admission checks precede Rust-owned input copies. SDK operation names are bounded
to 64 bytes. Copy counters report raw body bytes, while reservation charges
include the base64-equivalent length; neither is total allocation volume.

`interpreter_snapshot()` reads the latest owned observation without acquiring
the GIL or waiting for a lock. A blocked interpreter cannot refresh it: inspect
`generation` and `age_ns`. `None` means no initial observation or a busy publisher.
Samples include CPU/wall/blocks, handler/loop timing, tasks, invocations, resident
objects, and pending retirements. Retirement observations publish before
application destructors run, even while SDK admission is closed.

`bridge_report()` counters are sampled independently. Check conservation after
quiescence. `native_invocations` counts admission permits held by Rust proxy
futures; `interpreter_snapshot().work.active_invocations` counts Python handler
registrations. They have different lifetimes:

```text
native call cancelled -> native_invocations = 0
                              |
               Python catches CancelledError and stays alive
                              |
               managed active_invocations = 1 -> incomplete shutdown
```

An expired Python task cannot use its old SDK authority. Native payload charges
can reach zero while that task remains in the managed loop. A zero native count
does not establish a completed Python drain.

`Report::complete()` requires zero native admissions and pending work, balanced operation
accounting, complete object retirement, no recorded runtime/host failures, and
complete explicitly recorded native outcomes. It cannot certify arbitrary raw
threads or private allocators.

Memory probes run outside node execution and interpreter ticks. Unavailable
values are `None`; growth includes imports, caches, host work, and the retained
interpreter. One-frame CPython tracing adds CPU/memory overhead and reports its
own bookkeeping separately.

See [telemetry.rs](telemetry.rs), [snapshot.rs](snapshot.rs), and
[memory.rs](memory.rs). Use the [baseline harness](../../../tools/python/baseline.md)
for measurements and [plan 007](../../../docs/design/007-embedded-python-pyo3.md)
for qualification gates.
