# Embedded Python

The [FastAPI order example](../../examples/python-fastapi/README.md) demonstrates
an example-only buffered ASGI HTTP transport, durable cells and verified package
deployment. General ASGI lifespan, WebSocket and streaming support remain outside
the adapter's current SDK contract.

The [PydanticAI agent](../../examples/python-agent/README.md) demonstrates
durable model/tool steps, saved conversations and pending-alarm restart recovery.

`cellar-python` implements Cellar's native `Application` and `Cell` contracts using
PyO3 and **CPython 3.14 or free-threaded CPython 3.14t**. Enable the optional `embed` feature. A compiled
Rust executable includes the Python source and starts one interpreter and one
asyncio loop on the process main thread. No container or Python subprocess is
required. The default feature set does not build or link Python.

The [baseline harness](../../tools/python/baseline.md) compares equivalent native
and Python handlers through real HTTP, checked durable updates and owner-loss
recovery, retaining latency, throughput, memory and bridge-copy evidence.

See the [runnable Python example](../../examples/python/README.md) for installation,
packaging, requests, infrastructure bindings and a smoke test.

## Bootstrap and execution

Call `unsafe { PythonRuntime::run(options, source, |application, stop| { ... }) }`
once on the process main thread, outside Tokio. The safety contract requires
exclusive CPython initialization: start it before any other Python API user or
initializer. Already initialized interpreters and off-main startup are refused. The supervisor closure runs on a joined Rust thread. Register its
`Arc<PythonApplication>` in a native `Host` or `Node`. The host **must** observe
`stop.cancelled()`, stop admission, drain and join before returning. For the CLI
host, use `Host::run_until_observed(stop.clone().cancelled(), move |outcome| stop.record_node(outcome))`. `Host::run()` owns process exit and
would skip Python cleanup. `PythonRuntime` returns a `Report` only after the host
thread and cooperative Python tasks have joined; incomplete cleanup returns an
error containing an `Incomplete` report. The report preserves native node outcomes, pending Python tasks/objects/threads, object cleanup counts and bridge byte reservations; record every owned node through `Stop::record_node`. Interpreter reinitialization is refused.

Handlers are `async def fetch(request, ctx)`, `alarm(scheduled_ms, ctx)`,
`queue(batch, ctx)` and `scheduled(event, ctx)`. Export stateless handlers at module
scope; declare their event kinds through `Source::events`. Register cell classes through `Source::cell_events` (`Source::cell` declares fetch only). Startup validates async handlers and zero-argument constructors before admitting work. Cell constructors take no
arguments; use `ctx.identity` during invocations. Each native activation receives
its own Python object. `ctx.identity` is an immutable `CellIdentity` with `name`, `class_name`, `scope` and `epoch`. Its fields are transient; persist business state through
`ctx.storage`. Retired objects are released by the bridge pump. Use `include_str!`
as in the example to compile Python source into the executable. The SDK facade is
embedded by the adapter. Native artifact digests cover those embedded bytes. A build-bound provisioned inventory additionally verifies runtime and package contents before Python code runs; see the [environment workflow](../../tools/python/README.md). Changes
require rebuilding the executable and restarting the process; Python code
generation hot reload is not implemented.

Python receives owned values and a revocable message sender, never a Rust SDK
context or SQLite handle. Each SDK request executes inside the original Rust
invocation future, under its original execution domain, generation and activation
checks. Cellar's existing write output gates and durability proofs remain in force.
SDK operations start in receive order. Known reads can progress during a pending native call; only one mutation may be outstanding per invocation. Arbitrary SQL, sync and nested fetches conservatively count as mutations. A second submission fails before dispatch. Returning, cancellation and activation retirement invalidate retained contexts;
late operations fail with `ContextExpired`. Queued operations abandoned by a
returning handler are discarded; always await SDK operations. Do not retain
contexts or binding handles between invocations. Live handles used by another invocation or asyncio
loop are refused at both the Python API and native sender. The invocation scope
is an additional caller check; native SDK authority still comes from the original
Rust context.

`ctx.storage` creates a lightweight handle on access. A handle keeps
its originating context alive, but the context does not cache the handle, so
ordinary request cleanup does not depend on collecting a storage/context cycle.
Bind `storage = ctx.storage` locally when reusing it within one invocation.

SDK errors do not automatically retry mutations:
a failed response may follow a committed write. `SDKError.code` distinguishes
`ValidationError`, `Overloaded`, `OwnershipLost`, `Ambiguous`, `HostError`,
`ContextExpired`, `ContextMismatch`, `NotSubmitted` and `Conflict`. Validation,
caller scope, expiration, pre-dispatch admission and conflict refusals have
`outcome="not_committed"` for that operation. A native capacity verdict returned
by a submitted call is `Overloaded/unknown`: it may be propagated by a callee
after that callee committed its own writes. A submitted mutation with no proven refusal is `Ambiguous`
with `outcome="unknown"`; ownership loss also leaves its outcome unknown.
Generic submitted reads retain `HostError`. Native typed routing verdicts are
preserved where available; the adapter never infers rollback by parsing backend
error text. Backend messages are replaced with fixed codes before returning to
Python. Import `ValidationError`, `Overloaded`, `Ambiguous` and `OwnershipLost`
to catch those errors by class. Cancellation remains `asyncio.CancelledError`.
A handler that cancels itself immediately wakes its native driver, even when
application code retains the expired context. Cancellation does not roll back
an acknowledged write. Retired activation objects are released while the loop
is idle, as well as during later requests.

A failed durability proof can reset the cell's native activation. A read arriving
during that cleanup can be refused before recovery finishes, even after the
storage and peer transports are available again. A caller can repeat a known
read-only observation within its original deadline. Submitted business mutations
still require receipt-based reconciliation; a reset does not establish rollback.

## Python API

Import `Request`, `Response`, `Conflict`, `SDKError`, `Snapshot` and `MISSING` from
`cellar`. `Request.body` and `Response.body` are bytes; `await request.json()` and
`Response.json(value)` handle JSON. HTTP is buffered, with bounded bodies.

| API | Behavior |
| --- | --- |
| `await ctx.storage.read_json(key)` | Presence-aware snapshot; JSON null differs from missing |
| `await ctx.storage.get_json(key, default=None)` | Read a JSON value |
| `await ctx.storage.apply(checks={key: snapshot}, puts={...}, deletes=[...])` | One atomic checked batch; `Conflict` applies no mutations |
| `await ctx.storage.put_json(key, value)` / `delete(key)` | Unconditional batch mutation |
| `await ctx.storage.sql(sql, params=())` | Cell-local SQL, returning columns, rows and write count |
| `await ctx.storage.sync()` | Existing Cellar storage durability barrier |
| `await ctx.storage.set_alarm(ms)` / `get_alarm()` / `delete_alarm()` | Durable cell alarm; `apply` can combine alarm and JSON mutations |
| `await ctx.namespace(binding).get_by_name(name).fetch(request)` | Host-routed cell fetch |
| `await ctx.kv(binding).get(key)` / `put(key, value)` / `delete(key)` / `list(...)` | JSON KV; put accepts TTL and metadata, list accepts prefix, limit and cursor |
| `await ctx.sql(binding).query(sql, params=())` | Shared SQL binding; returns the native result envelope |
| `await ctx.queue(binding).send(body, delay_seconds=None)` | JSON queue producer |
| `await message.ack()` / `retry(delay_seconds=None)` | Explicit queue settlement |
| `await batch.ack_all()` / `retry_all(delay_seconds=None)` | Batch settlement through the existing Rust dispatcher |
| `await event.no_retry()` | Suppress retries for a scheduled invocation |
| `await ctx.var(name)` / `now_ms()` | Declared variable and execution-domain wall clock |

Only registered bindings are accessible. Queue delivery and cron retain their
existing at-least-once semantics. Use idempotency keys for business effects.
Queue acknowledgements are settlement decisions inside the current delivery;
the broker persists them when dispatch returns. An acknowledgement followed by
a handler exception is retained; a timeout that drops dispatch can redeliver.
The first conflicting message/batch ack or retry decision wins. Cron retries
retain the original occurrence timestamp across timeout, activation recreation
and owner recovery. The native scheduling cell checkpoints completed expressions
and resumes unfinished work; interrupted history remains unfinished. A new
deployment/schedule configuration replaces previous pending work. `no_retry()` declines retry when that
scheduled handler fails. Settlement controls cannot be reused in another live
invocation or after their original invocation expires.
Queue text bodies become strings; bytes become `{"$bytes":"BASE64"}`; structured
bodies must be JSON-representable. JSON integers must fit signed or unsigned
64 bits; NaN, infinity, non-string object keys and nesting over 64 are rejected.
Binary SQL parameters, SDK streaming, WebSockets, ASGI, workflows, outbox, stream
bindings and `wait_until` are not exposed by this adapter.

Transactions never call back into Python while holding SQLite state. A Python
read/compute/write sequence is not automatically atomic: pass its snapshots to
`apply` to detect changes, and explicitly choose how to handle `Conflict`. Only
`apply` combines mutations in a single transaction; multiple SQL calls are
separate operations.

## Resource and deployment contract

`Options` bounds concurrent invocations, request/response bytes, bridge value
bytes, aggregate native bridge bytes, operations per invocation, operations per atomic batch, live asyncio tasks and resident Python objects. Admission
fails immediately when full. Top-level handlers and nested Python cell calls
share `max_invocations`; a suspended caller continues to occupy a slot. Size
admission for the application's fan-out instead of assuming every slot can
serve an independent HTTP request. Capacity refusal uses the native SDK's
`ApplicationOverloaded` contract: public HTTP returns 503 and SDK calls raise
`Overloaded`, with an `unknown` mutation outcome. A caller may already have
committed before a nested call was refused. The adapter does not retry it,
and ownership/output-proof failures still take precedence. Closed runtime
admission is a lifecycle failure, distinct from capacity exhaustion.
Peer placement candidates that refuse cell capacity may be retried only within
the original fetch routing budget. Exhaustion returns a typed overload instead
of cycling indefinitely between peers. Native cell-capacity refusals retain
`Overloaded/unknown` across the Python bridge, including forwarded failures;
they do not establish that a parent invocation made no writes.
`max_pending_operations` bounds each invocation's
operation/completion mailbox (32 entries by default); zero and unsupported
capacities are refused before bootstrap. The result mailbox holds at most twice
`max_invocations` replies. Refused sends release their byte reservations.
HTTP invocation bodies cross into Python as immutable bytes beside the JSON
metadata. The value and aggregate bridge budgets still charge the complete
base64-equivalent envelope, preserving admission limits without allocating that
encoded body. The interpreter copies the body while its native reservation is
held; queued, refused and cancelled invocations release that reservation normally.
HTTP completions likewise pass an immutable byte body beside their metadata.
Native byte and mailbox capacity are reserved before copying it into Rust; the
reservation remains owned through response validation. Completion metadata and
the body share the same base64-equivalent envelope limit as encoded delivery.
Ordinary HTTP metadata uses a shallow encoder with owned header snapshots and
identical JSON escaping and key order. Unusual metadata and subclasses retain
the general SDK encoder; native HTTP validation and byte limits still apply.
`max_value_bytes` limits the serialized UTF-8 JSON envelope of an entire atomic
batch, rather than each row separately. `max_batch_operations` counts checks,
puts, deletes and an optional alarm decision together. Rejected oversized
batches apply no partial writes or alarm changes.

SDK results also enforce a depth of 64 on the returned value (before its transport
envelope) and `max_value_bytes` on the complete encoded success envelope. The
serializer stops at that byte limit. An unrepresentable native result returns a
fixed `HostError` for known reads or `Ambiguous` for possible mutations, with
`outcome="unknown"`; the invocation remains usable and the adapter never retries.
Native execution can already have committed a write, so a rejected result is
not a validation refusal or rollback. Native-operation success metrics describe
execution, independently of whether its result can cross the bridge. The value
budget must be at least 128 bytes to carry fixed failure envelopes.

`max_value_bytes` also bounds the complete encoded response envelope, including
JSON escaping and base64, separately from body/header limits. A response refused
by its mailbox or aggregate byte budget fails that invocation and revokes its
sender; even refusal of the fixed failure envelope leaves the runtime available
for later work. Previously committed writes are not rolled back.
Exceptions raised by managed tasks while shutdown cancels them also make the
combined report incomplete, even when no tasks remain. The fixed failure stage
is `task_cleanup`; arbitrary exception text and traceback paths are omitted.

For alarms, persist a business occurrence ID in the same `apply` batch as
`alarm_at_ms`. Native retry backoff can change the handler's `scheduled_ms`;
use the persisted identity for the receipt, and commit that receipt with the
business effect. The example uses a checked durable sequence and retains its
current intent so a retry with a different delivery deadline still deduplicates.
The interpreter's asyncio loop wakes on native invocation admission, host
replies, invocation cancellation, activation retirement and host completion.
A nonblocking Unix socket pair carries coalesced readiness bytes; it carries no
request data or SDK authority. Producers never acquire the GIL, call Python or
wait for the interpreter. SDK operations still execute in their original native
invocation. Notification failures fail the runtime instead of claiming a clean
shutdown, and the loop removes its reader before the runtime closes the sockets.
Native proxy destruction also publishes a coalesced retirement flag. The loop
scans resident objects only after consuming that flag, so ordinary requests do
not check every idle cell. Consuming the flag before the scan preserves concurrent
retirements for another pass. Each SDK operation still checks its native owner
and invocation authority independently of this cleanup optimization.

The pump admits at most 64 starts before yielding to the event loop. Continuous
arrivals, including requests rejected at task capacity, cannot indefinitely
postpone timers and task cancellation. A deferred continuation services queued
starts without waiting for another native notification.
Managed invocations start eagerly until their first await; handlers that finish
inline avoid an extra task-dispatch turn and completion callback. They still use
the bounded task factory and invocation context. Individual handlers remain
cooperative and can block the interpreter.

`poll_interval` now bounds housekeeping waits (250 ms by default), rather than
delaying ordinary requests. The telemetry deadline may shorten that wait. A
longer housekeeping interval delays observation of background thread/unraisable
failures and notification-channel faults; it does not delay normal admission,
replies or native retirement. Throughput and capacity still need qualification
against the application's workload and fleet. The bridge report distinguishes queued/peak native bridge charges, reservation totals and explicit UTF-8/body copy bytes; it excludes Python heap, JSON/base64 intermediate allocations and native transport copies.
The managed task factory enforces `max_tasks` even for callbacks with an empty
context. Each admission scans the live asyncio task registry once, including
tasks constructed outside the factory. A refused invocation closes its unstarted
coroutine and context before the pump continues; it never starts the handler.
Shutdown can create up to two explicit supervisor tasks to join existing
work; the reported peak includes them. These are cooperative resource limits,
not isolation from trusted code that constructs raw `asyncio.Task` objects or
replaces the task factory. Raw application threads and external native pools
remain application-owned.

`Options::interpreter` supplies the Python home, executable, standard-library
roots and SOABI. Its default is captured from the selected build-time CPython;
library code never searches for an interpreter at node startup. Initialization
uses CPython's [isolated configuration](https://docs.python.org/3.14/c-api/init_config.html):
no ambient Python paths, user site, `.pth`/customize hooks, argv parsing, automatic
signal handlers or bytecode writes. Import roots must be absolute and are canonicalized before initialization.

Managed startup failures expose bounded stage messages. Filesystem paths,
expected/actual ABI or distribution values, CPython initialization diagnostics
and nested inventory errors are not retained in the public error chain. Handler,
import and registration failures also omit Python exception values and traceback
paths without calling arbitrary exception formatting. Combined `Incomplete`
reports retain typed native outcomes and the Rust supervisor's own error source.
The host controls logging of that source. Application/library output and OS
loader diagnostics before `main` runs remain process output; embedding is not
an output sandbox.

CPython 3.14.8 can print configured paths directly from C before Python's error
hooks exist. The main-thread bootstrap therefore briefly redirects process
stderr to a discard sink around C initialization, flushes C output, and restores
the original descriptor and its flags before application imports or host startup.
A closed stderr stays closed. Start host/logging workers afterward: unrelated
threads' stderr writes during this interval are discarded too, and callers must
not concurrently replace or close that descriptor. Restoration failure refuses
startup. Later Python and native logging uses the original destination.

Bootstrap imports `signal` before asyncio and removes CPython's implicitly
installed default INT handler. Disabling initialization signal handlers alone
does not prevent that later import from installing it. Existing native host
handlers are preserved, so the Rust host retains sole INT/TERM shutdown ownership.

`Options::environment` is an explicit `Environment` value (empty by default).
It sets Python's initial `os.environ`/`os.environb` view without removing or
changing the Rust host's process configuration. Supply application values at
the executable's CLI boundary. Python interpreter override keys are rejected;
configure those through `Interpreter`. Debug output redacts environment names
and values. This is not native-process isolation: native libraries and raw
process APIs still have ordinary process capabilities, and user writes to
`os.environ` retain their normal process effects.

`Options::module_paths` explicitly adds installed dependency directories.
`Source::require_distribution(name, version)` refuses startup on a missing or
mismatched installed distribution. No request installs packages. Build and deploy
a pinned Python runtime and dependency environment on every node. Native projects can carry a `python_environment` declaration that stages, publishes and restores the hash-verified runtime/stdlib/package archive with the executable. The installer requires an explicit `--python-root` matching its immutable prefix and never overwrites different existing contents. Bound package roots become the default import paths; startup needs no Python path flags. The process registers its inventory identity, and generation admission rejects a different declaration before running factories. Follow the [environment build/install workflow](../../tools/python/README.md).

Verified inventories also declare the resolved native dependency graph and OS
cohort. External library files retain exact hashes; macOS system-cache providers
retain the qualified OS build and cache UUID. Installation and initialization
refuse missing or different prerequisites. Actual loaded providers are checked
before initialization and again after imports, before the Rust supervisor starts.
OS libraries are provisioned with the qualified host, outside the environment
archive. OS/library updates require recapture and rebuilding. No compiler,
dependency inspection tool or package installer runs during node startup.

This is trusted native execution: Python libraries can access the host OS and
external services, and native extension crashes affect the entire Cellar process.
Use async libraries for cooperative I/O. CPU-bound Python, a GIL-holding native
extension or cancellation-suppressing code can stall the interpreter; Rust's
handler deadline revokes SDK authority but cannot forcibly stop that Python code.
Use the shipped Unix [`cellar supervise` process watchdog](../../tools/python/README.md#process-supervision)
for bounded startup, liveness, hard termination and restart. Its health endpoint
must execute through Python. A process exit or supervisor record alone is not a
drain proof; inspect the combined native/Python report. Direct library side effects bypass
Cellar's durability and idempotency mechanisms. Background tasks have no durable
execution guarantee and lose SDK authority when their invocation ends. Cooperative
tasks are cancelled at shutdown; the asyncio default thread executor is joined
with a deadline. `Report` rejects remaining tasks or Python threads. Applications
must join threads and private executors created by their libraries; cancelling
`asyncio.to_thread` alone does not stop its underlying function. Multiple Python
application generations and subinterpreters are not supported.

The [design](../../plans/007-embedded-python-pyo3.md) records the broader production
qualification work beyond this implementation.

### Build-time interpreter selection

Set `PYO3_PYTHON` to the selected interpreter when building the `embed` feature.
The build rejects a different Python minor, a non-CPython ABI, mismatched
threading ABI/layout, and
non-shared libpython. It records home, executable, standard-library roots and
SOABI using that interpreter's isolated `-I -S -B` startup. Native-only builds skip
the probe entirely. This layout is not the complete verified deployment inventory
required by [plan 007](../../plans/007-embedded-python-pyo3.md).

Select `python3.14t` through `PYO3_PYTHON` for a free-threaded build. The shared
library, standard library, import roots and native wheels must all match that ABI;
a compiled host cannot switch between `314` and `314t` at runtime. The asyncio
loop still owns handler dispatch and SDK handles. `asyncio.to_thread` and ordinary
Python threads can run computation in parallel; SDK use stays on its original
invocation loop, including when Python inherits contextvars into new threads.

Cellar does not force the GIL back on or force incompatible extensions to run
without it. CPython may enable the GIL when an extension does not declare
free-threaded support. The example's `/health` exposes `free_threaded` (build ABI)
and `gil_enabled` (current runtime state); its free-threaded smoke requires the
GIL to remain disabled after real native-library work. See [CPython's free-threading
contract](https://docs.python.org/3.14/howto/free-threading-python.html).

For a cross-build, provide `CELLAR_PYTHON_BUILD_LAYOUT` with a target-layout JSON
file instead of executing a host interpreter for paths. For free-threaded targets,
set `gil_disabled` to `true` and use the matching `cpython-314t-...` SOABI; both
must agree with the target PyO3 build configuration:

```json
{
  "home": "/opt/cellar/python",
  "executable": "/opt/cellar/python/bin/python3.14",
  "paths": [
    "/opt/cellar/python/lib/python3.14",
    "/opt/cellar/python/lib/python3.14/lib-dynload"
  ],
  "soabi": "cpython-314-x86_64-linux-gnu",
  "version": [3, 14],
  "implementation": "cpython",
  "gil_disabled": false
}
```

The target's matching libpython/PyO3 cross-build configuration is still required.
Paths must exist on the running target and the actual ABI is checked before app
imports. Main-thread ownership checks currently support Linux and macOS. Platform
qualification and verified deployment closure work remain tracked in the
[implementation plan](../../plans/007-embedded-python-pyo3.md#implementation-milestones-and-acceptance-gates).

Public Python APIs include type annotations. `loop.shutdown_asyncgens()` and the managed executor are closed while the loop remains alive. Background exceptions trigger a sanitized runtime failure; exception repr and traceback data are not logged by the runner. The report covers runtime-owned work and explicitly recorded native nodes; it does not certify arbitrary raw Python/native threads or the process-wide heap.

Application-import, cell-registration and cold-descriptor failures expose fixed
stage messages. Python exceptions are discarded at that boundary, without
formatting their values or retaining traceback/path details in the error chain.
Other interpreter failures use a fixed message; native Rust host failures retain
their typed combined report. Application code still controls its own explicit
logging and process side effects.

`PythonApplication::bridge_report()` returns independently sampled atomic counters for a host-owned metrics exporter. Conservation checks apply after quiescence, not during concurrent updates. The example emits the combined report when `CELLAR_SHUTDOWN_REPORT_JSON=1`; its smoke asserts native drain phases and zero remaining managed work/reservations.

`Report::interpreter` records managed-loop wall time and that thread's CPU time
separately from Rust turn accounting. Thread CPU includes Python and native code
running on the interpreter thread; it excludes executor threads and native
library pools. Handler timing includes host awaits and can overlap across
concurrent handlers, so its sum is not process elapsed time. The report also
records whole-process peak RSS and initial/final CPython allocated-block counts.
These are measurements, not per-cell limits or Python-only memory attribution.
`loop_delay` measures ready-queue checkpoint delay and housekeeping timer
lateness, including blocking Python, GIL contention and OS scheduling; it cannot
isolate one cause. Checkpoints cover inline pump/handler work and follow resumed
handlers so a long housekeeping interval does not hide short blocking handlers. Time spent awaiting
host I/O is not counted as event-loop delay.
The exact bounded histogram layout is available through
`LatencyReport::upper_bounds_ns()`: inclusive bounds of 1 µs, 10 µs, 100 µs,
1 ms, 10 ms, 100 ms, 1 s and 10 s, followed by an unbounded bucket. Counts are
noncumulative; totals saturate rather than overflow.

`bridge.operations` follows every admitted SDK operation from mailbox reservation
through native execution and managed-loop reply dequeue. It reports queued,
running and pending-reply work, success/failure, delivered replies and separate
queued/running/undelivered abandonment. Queue wait, native SDK duration, completion
to reply dequeue and admission to reply dequeue have bounded histograms. Native
SDK duration includes parsing, validation and host await time; it is not Rust CPU
time. Reply timing includes encoding and loop scheduling, and excludes subsequent
Python future consumption. Dropped native futures retain their duration and
terminal accounting without calling Python. `Report::complete()` requires zero
pending work and matching admitted/terminal and started/completed counts; this
does not infer rollback of abandoned mutations.

`reserved_bytes_total` counts successful payload reservations, including later
abandonment. `input_copied_bytes` and `output_copied_bytes` count explicit owned
UTF-8 and raw HTTP body copies across the adapter boundary. HTTP body
charges conservatively include their base64-equivalent length, while copy counters
record the raw body length actually copied across the boundary. Admission checks precede Rust-owned
input copies; SDK operation names are bounded to 64 bytes. These counters exclude
codec temporaries, Python UTF-8 caches and native transport/library copies. They
are distinct from allocation bytes.

`PythonApplication::interpreter_snapshot()` reads the latest owned interpreter
observation without acquiring the GIL or waiting for a lock. `None` means no
initial observation or a busy publisher. Check `generation` and `age_ns`: a blocked
interpreter cannot refresh its snapshot. `Options::telemetry_interval` defaults
to 250 ms. Samples include interpreter CPU/wall/allocated blocks, handler and loop
timing, active tasks/invocations, resident registry objects and pending retirements.
Retirement observations publish before destructors run, including during closed
SDK admission. Removing an object from the registry does not certify collection
of aliases retained by application code.

`Options::trace_allocations` optionally starts one-frame CPython allocation
tracing before application imports. It defaults to false. Traced current/peak
bytes and tracer bookkeeping bytes are `None` when disabled or unavailable.
Tracing adds CPU/memory overhead and measures registered allocation domains;
it does not account for every native extension's private allocator. The bootstrap
stops its owned tracer on normal return, host failure, import failure and cold
validation failure. It does not stop application-owned tracing when disabled.

`Report::memory` samples process RSS and available jemalloc/libc heap counters
before interpreter initialization and after joined cleanup. Probes run outside
node execution and interpreter ticks. Unavailable probes are `None`. Growth
includes the host, interpreter, imports and caches. CPython remains initialized;
libc/jemalloc counters exclude private allocators and are not Python heap limits.
Use the [baseline harness](../../tools/python/baseline.md) to measure native/Python
performance. Remaining qualification gates are listed in the
[implementation plan](../../plans/007-embedded-python-pyo3.md#implementation-milestones-and-acceptance-gates).

Unraisable exceptions (including destructor failures) and unhandled Python thread
exceptions produce failure counts and an incomplete runtime report. Default
exception printing is replaced before application imports, so arbitrary exception
text and object representations do not reach the process logs through those hooks.

Python deployment validation rejects queue consumers or cron schedules whose root event handlers were not declared, and classes absent from the source descriptor, before generation activation. Cell constructors must be synchronous; construction failures do not fabricate successful object creation/retirement counts.
