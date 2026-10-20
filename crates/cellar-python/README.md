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
`HostError`, `ContextExpired`, `ContextMismatch`, `NotSubmitted` and `Conflict`;
`outcome` is `unknown` for general host errors and `not_committed` for caller
scope, expiration, admission or conflict refusals.

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
not a throughput-qualified Python service. The bridge report counts queued/peak native bridge bytes and bytes transferred across that boundary; it excludes Python heap, JSON/base64 intermediate allocations and native transport copies. Task limits cover managed-loop work, not raw application threads or external native pools.

`Options::interpreter` supplies the Python home, executable, standard-library
roots and SOABI. Its default is captured from the selected build-time CPython;
library code never searches for an interpreter at node startup. Initialization
uses CPython's [isolated configuration](https://docs.python.org/3.14/c-api/init_config.html):
no ambient Python paths, user site, `.pth`/customize hooks, argv parsing, automatic
signal handlers or bytecode writes. Import roots must be absolute and are canonicalized before initialization.

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
a pinned Python runtime and dependency environment on every node. The native
package currently **does not transport libpython, the standard library, site-packages or transitive shared libraries**. The optional build-bound inventory verifies provisioned runtime/package files before startup; it does not publish or restore them. Use an immutable application image or provision those assets through the [verified environment workflow](../../tools/python/README.md). Exact distribution version checks alone do not attest file contents or external OS libraries.

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

`PythonApplication::bridge_report()` returns an atomic live snapshot for a host-owned metrics exporter. The example emits the combined report when `CELLAR_SHUTDOWN_REPORT_JSON=1`; its smoke asserts native drain phases and zero remaining managed work/reservations.
