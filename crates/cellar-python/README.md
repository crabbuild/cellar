# Embedded Python

`cellar-python` implements Cellar's native `Application` and `Cell` contracts using
PyO3 and **GIL-enabled CPython 3.14**. Enable the optional `embed` feature. A compiled
Rust executable includes the Python source and starts one interpreter and one
asyncio loop on the process main thread. No container or Python subprocess is
required. The default feature set does not build or link Python.

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
Rust context. SDK errors do not automatically retry mutations:
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
fails immediately when full. Each invocation's operation channel is bounded.
The single interpreter polls bridge messages every millisecond by default;
`poll_interval` controls this latency/idle-CPU tradeoff. This is a first adapter,
not a throughput-qualified Python service. The bridge report distinguishes queued/peak native bridge bytes, reservation totals and explicit UTF-8 copy bytes; it excludes Python heap, JSON/base64 intermediate allocations and native transport copies. Task limits cover managed-loop work, not raw application threads or external native pools.

`Options::interpreter` supplies the Python home, executable, standard-library
roots and SOABI. Its default is captured from the selected build-time CPython;
library code never searches for an interpreter at node startup. Initialization
uses CPython's [isolated configuration](https://docs.python.org/3.14/c-api/init_config.html):
no ambient Python paths, user site, `.pth`/customize hooks, argv parsing, automatic
signal handlers or bytecode writes. Import roots must be absolute and are canonicalized before initialization.

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
a pinned Python runtime and dependency environment on every node. Native projects can carry a `python_environment` declaration that stages, publishes and restores the hash-verified runtime/stdlib/package archive with the executable. The installer requires an explicit `--python-root` matching its immutable prefix and never overwrites different existing contents. Bound package roots become the default import paths; startup needs no Python path flags. The process registers its inventory identity, and generation admission rejects a different declaration before running factories. Follow the [environment build/install workflow](../../tools/python/README.md). System libraries outside the closure remain OS prerequisites and require separate platform qualification.

This is trusted native execution: Python libraries can access the host OS and
external services, and native extension crashes affect the entire Cellar process.
Use async libraries for cooperative I/O. CPU-bound Python, a GIL-holding native
extension or cancellation-suppressing code can stall the interpreter; Rust's
handler deadline revokes SDK authority but cannot forcibly stop that Python code.
Run a process watchdog for hard termination. Direct library side effects bypass
Cellar's durability and idempotency mechanisms. Background tasks have no durable
execution guarantee and lose SDK authority when their invocation ends. Cooperative
tasks are cancelled at shutdown; the asyncio default thread executor is joined
with a deadline. `Report` rejects remaining tasks or Python threads. Applications
must join threads and private executors created by their libraries; cancelling
`asyncio.to_thread` alone does not stop its underlying function. Free-threaded
Python and multiple Python application generations are refused; subinterpreters
are not supported.

The [design](../../plans/007-embedded-python-pyo3.md) records the broader production
qualification work beyond this implementation.

### Build-time interpreter selection

Set `PYO3_PYTHON` to the selected interpreter when building the `embed` feature.
The build rejects a different Python minor, free-threaded or non-CPython ABI, and
non-shared libpython. It records home, executable, standard-library roots and
SOABI using that interpreter's isolated `-I -S -B` startup. Native-only builds skip
the probe entirely. This layout is not the complete verified deployment inventory
required by [plan 007](../../plans/007-embedded-python-pyo3.md).

For a cross-build, provide `CELLAR_PYTHON_BUILD_LAYOUT` with a target-layout JSON
file instead of executing a host interpreter for paths:

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
[completion audit](../../plans/007-embedded-python-audit.md).

Public Python APIs include type annotations. `loop.shutdown_asyncgens()` and the managed executor are closed while the loop remains alive. Background exceptions trigger a sanitized runtime failure; exception repr and traceback data are not logged by the runner. The report covers runtime-owned work and explicitly recorded native nodes; it does not certify arbitrary raw Python/native threads or the process-wide heap.

`PythonApplication::bridge_report()` returns independently sampled atomic counters for a host-owned metrics exporter. Conservation checks apply after quiescence, not during concurrent updates. The example emits the combined report when `CELLAR_SHUTDOWN_REPORT_JSON=1`; its smoke asserts native drain phases and zero remaining managed work/reservations.

`Report::interpreter` records managed-loop wall time and that thread's CPU time
separately from Rust turn accounting. Thread CPU includes Python and native code
running on the interpreter thread; it excludes executor threads and native
library pools. Handler timing includes host awaits and can overlap across
concurrent handlers, so its sum is not process elapsed time. The report also
records whole-process peak RSS and initial/final CPython allocated-block counts.
These are measurements, not per-cell limits or Python-only memory attribution.
`loop_delay` measures lateness after the managed pump's scheduled wake, including
blocking Python, GIL contention and OS scheduling; it cannot isolate one cause.
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
time. Reply timing includes encoding and loop polling, and excludes subsequent
Python future consumption. Dropped native futures retain their duration and
terminal accounting without calling Python. `Report::complete()` requires zero
pending work and matching admitted/terminal and started/completed counts; this
does not infer rollback of abandoned mutations.

`reserved_bytes_total` counts successful payload reservations, including later
abandonment. `input_copied_bytes` and `output_copied_bytes` count explicit owned
UTF-8 copies across the adapter boundary. Admission checks precede Rust-owned
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
Measured Rust/Python performance baselines and remaining qualification still
belong to the [completion audit](../../plans/007-embedded-python-audit.md).

Unraisable exceptions (including destructor failures) and unhandled Python thread
exceptions produce failure counts and an incomplete runtime report. Default
exception printing is replaced before application imports, so arbitrary exception
text and object representations do not reach the process logs through those hooks.

Python deployment validation rejects queue consumers or cron schedules whose root event handlers were not declared, and classes absent from the source descriptor, before generation activation. Cell constructors must be synchronous; construction failures do not fabricate successful object creation/retirement counts.
