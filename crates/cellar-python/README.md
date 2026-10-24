# Embedded Python applications on Cellar

`cellar-python` runs Python handlers inside a native Rust Cellar host. Python
owns business logic; Rust owns routing, cell storage, transactions, and durability.
The adapter implements the native `Application` and `Cell` contracts through
PyO3 and **CPython 3.14 or free-threaded CPython 3.14t**.

Enable the optional `embed` feature. The default features do not build or link
Python. Embedded mode uses one interpreter and one asyncio loop on the process
main thread; it needs no Python subprocess or container.

```text
One native Cellar process
+-------------------------------------------------------------------+
| Process main thread                 Joined Rust host thread       |
|                                                                   |
| CPython + asyncio                   Native Host / Node            |
|        |                                     |                    |
| Python handlers <--- invocation --- Original SDK context          |
|        |                                     |                    |
| await ctx.storage.apply(...) -- SDK call ---> cellar-guest        |
|        ^                                     |                    |
|        +----------- result ----------------- Rust storage / I/O   |
+-------------------------------------------------------------------+
              Python values cross the bridge; SDK handles do not.
```

## Choose a starting point

| Goal | Guide |
| --- | --- |
| Run a counter with Pydantic, NumPy, alarms, and bindings | [Runnable Python example](../../examples/python/README.md) |
| Serve a framework application | [FastAPI order service](../../examples/python-fastapi/README.md), using an example-only buffered ASGI transport |
| Persist model/tool steps and resume after restart | [PydanticAI agent](../../examples/python-agent/README.md) |
| Deliver a verified runtime and dependencies | [Environment build/install workflow](../../tools/python/README.md) |
| Measure latency, throughput, memory, and recovery | [Baseline harness](../../tools/python/baseline.md) |
| Work on scheduling, cleanup, or telemetry | [Runtime internals and diagnostics](runtime/README.md) |

The native adapter exposes bounded, buffered HTTP. General ASGI lifespan,
streaming responses, WebSockets, workflows, outbox, stream bindings, binary SQL
parameters, and `wait_until` are outside its current SDK contract.

## Write a Python application

Use `async def` for handlers and synchronous, zero-argument cell constructors.
Define application handlers and named cell classes at module scope. Rust looks
up each registered class by its module attribute name, such as `Counter`.

| Handler | Python signature | Rust registration |
| --- | --- | --- |
| Application HTTP | `async def fetch(request, ctx)` | `Source::events([EventKind::Fetch])` |
| Cell HTTP | `async def fetch(self, request, ctx)` | `Source::cell("Counter")` |
| Cell alarm | `async def alarm(self, scheduled_ms, ctx)` | Include `EventKind::Alarm` in `Source::cell_events` |
| Queue delivery | `async def queue(batch, ctx)` | Include `EventKind::Queue` in `Source::events` |
| Scheduled work | `async def scheduled(event, ctx)` | Include `EventKind::Scheduled` in `Source::events` |

Startup validates handlers and constructors before admitting work. Deployment
validation also rejects undeclared classes, queue consumers without a queue
handler, and cron schedules without a scheduled handler.

### A small durable counter

This complete `app.py` routes every request to the cell named `alice`. `GET`
reads its count; `POST` adds one. Configure the `COUNTERS` namespace binding to
point to the `Counter` class.

```python
from cellar import Conflict, Request, Response


class Counter:
    async def fetch(self, request, ctx):
        if request.method not in ("GET", "POST"):
            return Response.json({"error": "Use GET or POST"}, status=405)

        storage = ctx.storage
        snapshot = await storage.read_json("count")
        count = snapshot.value if snapshot.present else 0
        if type(count) is not int or not 0 <= count <= (1 << 63) - 1:
            raise ValueError("Invalid stored counter")

        if request.method == "POST":
            if count == (1 << 63) - 1:
                return Response.json({"error": "Counter limit reached"}, status=409)
            try:
                await storage.apply(
                    checks={"count": snapshot},
                    puts={"count": count + 1},
                )
            except Conflict:
                return Response.json({"error": "Counter changed; read again"}, status=409)

            await storage.sync()
            count += 1

        return Response.json({"count": count})


async def fetch(request, ctx):
    counter = ctx.namespace("COUNTERS").get_by_name("alice")
    return await counter.fetch(Request("http://counter/", method=request.method))
```

The application handles a proven conflict but does not retry unknown outcomes.
The [full example](../../examples/python/README.md) adds input validation,
idempotency receipts, multiple cell names, and alarm deduplication.

```text
read_json("count") --> Snapshot(present=True, value=7)
                                  |
                       Python computes next value
                                  |
                    apply(checks=7, puts=8)
                                  |
                       One Rust transaction
                         /              \
                    still 7?          changed?
                       |                 |
                   write 8          Conflict: no batch writes
                       |
                  await sync()
                       |
                  return count=8
```

A read/compute/write sequence is not automatically atomic. Pass snapshots into
`apply(checks=...)` to detect intervening writes. `apply` can combine JSON changes
and an alarm change in one transaction; separate SQL calls remain separate
operations. SQLite transaction callbacks never execute Python.

## Bootstrap and execution

Call `PythonRuntime::run` **once, on the process main thread, outside Tokio**.
Its unsafe contract requires exclusive CPython initialization before any other
Python API user or initializer. Already-initialized interpreters and off-main
startup are refused. Reinitialization, subinterpreters, and multiple Python
application generations in one process are unsupported.

```text
main thread                         joined host thread
     |
validate options, source, inventory
initialize CPython once
load SDK + app; validate handlers
     |
     +----------------------------> start Host / Node
run managed asyncio loop                 |
     |                              serve native invocations
     |                                   |
     +---- stop signal -----------------> stop admission + drain + join
     |                                   |
     |<---------------------------- record every native node outcome
cancel/join managed Python work
close async generators + executor
     |
return Report or error with Incomplete report
```

### Rust entry point

The following is an executable template: place `app.py` beside it and enable
`cellar-python`'s `embed` feature. It expects a fleet deployment named
`python-counter`, version `v1`, with the `Counter` class and `COUNTERS` binding.
For a complete configured project, use [examples/python/main.rs](../../examples/python/main.rs).

```rust,ignore
use anyhow::{ensure, Result};
use cellar::sdk::Host;
use cellar_python::{EventKind, Options, PythonRuntime, Source};

fn main() -> Result<()> {
    let source = Source::new(include_str!("app.py"), "app.py")
        .events([EventKind::Fetch])
        .cell("Counter");

    // SAFETY: main exclusively initializes CPython before any Python API use
    // or concurrent initialization. Host/logging workers start in the closure.
    let report = unsafe {
        PythonRuntime::run(Options::default(), source, |application, stop| {
            let monitor = stop.clone();
            Host::fleet()
                .register("python-counter", "v1", move || Ok(application.clone()))?
                .run_until_observed(stop.cancelled(), move |outcome| {
                    monitor.record_node(outcome)
                })
        })
    }?;
    ensure!(report.complete(), "Python host did not drain completely");
    Ok(())
}
```

| Bootstrap requirement | Why it matters |
| --- | --- |
| Observe `stop.cancelled()` | Runtime failure must stop host admission and trigger native drain. |
| Use `Host::run_until_observed` | `Host::run()` owns process exit and can skip Python teardown. |
| Call `Stop::record_node` for every owned node | The final report must retain each real native drain outcome. |
| Join host and cooperative Python work | Process exit alone is not proof of a clean drain. |
| Inspect `Incomplete` on failure | A cleanup error can carry the combined native/Python report. |

The executable embeds both application source and the SDK facade. Source or
runtime changes require a rebuild and process replacement; there is no Python
code-generation hot reload. For a verified package, the host must also register
its compiled inventory identity, as the full example does.

## Python API

Import `Request`, `Response`, `Snapshot`, `MISSING`, and SDK error classes from
`cellar`. Request and response bodies are bytes. `await request.json()` and
`await request.text()` decode buffered input; `Response.json(value)` builds a
JSON response.

### Storage and cell calls

| API | Behavior |
| --- | --- |
| `await ctx.storage.read_json(key)` | Return a presence-aware `Snapshot`. |
| `await ctx.storage.get_json(key, default=None)` | Return the stored value or the supplied missing-key default. |
| `await ctx.storage.read_json_many(keys)` | Read bounded snapshots in one host call. |
| `ctx.initial_storage` | Access snapshots declared for capture before this cell event. |
| `await ctx.storage.apply(checks=..., puts=..., deletes=...)` | Apply one checked atomic batch; also accepts alarm changes. |
| `await ctx.storage.put_json(key, value)` / `delete(key)` | Apply an unconditional mutation. |
| `await ctx.storage.sql(sql, params=())` | Run cell-local SQL; return columns, rows, and write count. |
| `await ctx.storage.sync()` | Await the native storage durability barrier. |
| `await ctx.storage.set_alarm(ms)` / `get_alarm()` / `delete_alarm()` | Manage the durable cell alarm. |
| `await ctx.namespace(binding).get_by_name(name).fetch(request)` | Route a buffered request through the native host. |

`MISSING` is `Snapshot(False)`. It differs from `Snapshot(True, None)`, which
represents a stored JSON null. Bind `storage = ctx.storage` when reusing a handle
within one invocation: each property access creates a lightweight handle.

### Infrastructure and event controls

| API | Behavior |
| --- | --- |
| `await ctx.kv(binding).get(key)` / `put(key, value)` / `delete(key)` / `list(...)` | JSON KV; put supports TTL/metadata, list supports prefix/limit/cursor. |
| `await ctx.sql(binding).query(sql, params=())` | Query a shared SQL binding and receive its native result envelope. |
| `await ctx.queue(binding).send(body, delay_seconds=None)` | Send a JSON queue message. |
| `await message.ack()` / `retry(delay_seconds=None)` | Settle a message in the current delivery. |
| `await batch.ack_all()` / `retry_all(delay_seconds=None)` | Settle the current batch. |
| `await event.no_retry()` | Decline retry if the current scheduled handler fails. |
| `await ctx.var(name)` / `ctx.now_ms()` | Read a declared variable or execution-domain wall-clock time. |

Bindings must be declared in the native deployment. Queue text bodies become
strings; byte bodies use `{"$bytes": "BASE64"}`; structured bodies must be
JSON-representable. JSON integers must fit signed or unsigned 64 bits. NaN,
infinity, non-string object keys, and nesting deeper than 64 are rejected.

## Read snapshots with fewer bridge calls

Choose reads based on when the handler needs the data:

| Method | When data is read | Best fit |
| --- | --- | --- |
| `read_json(key)` | At the awaited SDK call. | Conditional or fresh reads. |
| `read_json_many(keys)` | In one synchronous native operation. | Several keys needed at the same point in a handler. |
| `ctx.initial_storage` | Before Python handler dispatch. | Small fields needed by every invocation of a registered cell event. |

```text
Normal read                         Declared initial read
Rust dispatch -> Python             Rust reads declared keys
                   |                          |
               await read           Rust dispatch + snapshots
                   |                          |
Rust reads <-------+                       Python
     |                                        |
     +-----------> Python               ctx.initial_storage["count"]
```

Register initial reads alongside the cell and event declarations:

```rust,ignore
let source = Source::new(include_str!("app.py"), "app.py")
    .cell("Counter")
    .cell_initial_reads("Counter", EventKind::Fetch, ["count"]);
```

Then a cell handler can use the captured snapshot:

```python
async def increment_from_capture(ctx):
    snapshot = ctx.initial_storage["count"]
    value = snapshot.value if snapshot.present else 0
    await ctx.storage.apply(checks={"count": snapshot}, puts={"count": value + 1})
    await ctx.storage.sync()
```

This helper assumes the application maintains an integer counter within its
chosen range. Captured data is owned; map keys are immutable. Snapshots are
neither reservations nor live caches: they do not refresh after an await or the
handler's own writes. An intervening writer makes a checked update conflict.

| Boundary | Contract |
| --- | --- |
| Capture timing | Runs before handler validation, authentication, or receipt lookup. A capture failure refuses dispatch before Python runs. |
| Capture limits | Declared key count is bounded by `max_batch_operations`; captured values, metadata, and body share `max_value_bytes`. |
| Batched reads | All supplied keys, including duplicates, count toward the batch limit; output has one snapshot per distinct key. |
| Bounded collection | Both paths process one decoded value at a time. Oversized read replies return no partial result. |
| Verified packages | Include matching `handlers.initial_reads`, such as `{"Counter":{"fetch":["count"]}}`, in the descriptor. Startup refuses mismatches. |

## Invocation authority and cell lifetime

```text
Python coroutine                  Original native invocation
       |                                     |
 await SDK operation -- name + values ------>| validate identity / liveness
       |                                     | execute with original Context
       | <-------------- reply ------------- |
       v                                     |
 resume coroutine                            |
       |                                     |
 return / cancel --------------------------> revoke authority
       |
 saved ctx or binding handle -- later call -> refused
```

SDK futures stay inside their original Rust invocation, under its execution
domain, generation, activation, and output-proof checks. Python never receives
a Rust context or SQLite handle.

- SDK operations start in receive order. Known reads may overlap a pending call;
  only one mutation may be outstanding per invocation.
- Arbitrary SQL, sync, and nested fetches count as mutations. Await effects
  sequentially rather than submitting concurrent writes.
- Handles cannot be reused by another invocation or asyncio loop. The native
  sender captures caller-identity accessors at bootstrap and rechecks them;
  rebinding a Python attribute does not replace those checks.
- Returning, cancellation, or activation retirement revokes authority. Queued
  operations abandoned by a returning handler are discarded.
- Background tasks have no durable execution guarantee. They lose SDK authority
  when their invocation ends, even if application code retains the context.

```text
Activation A                  retirement / restart              Activation B
new Counter()                         |                        new Counter()
self fields / caches -----------------X                         fresh fields
ctx.storage --------------------------+-----------------------> durable state
old ctx.identity / handles -----------X                         new invocation
```

`ctx.identity` is an immutable `CellIdentity` with `name`, `class_name`, `scope`,
and `epoch`. Identity fields and object fields belong to the activation; persist
business state in storage. Idle retirement also wakes the loop to release the
object. Removing an object from the registry does not collect aliases retained
by application code.

## Errors, cancellation, and durable retries

Inspect both `SDKError.code` and `SDKError.outcome`. Fixed codes replace backend
exception text before returning to Python; the adapter does not automatically
retry submitted mutations.

| Failure | Outcome | Caller action |
| --- | --- | --- |
| `ValidationError`, `ContextMismatch`, `ContextExpired`, `NotSubmitted`, or a pre-dispatch admission refusal | `not_committed` for the refused operation | Correct the request/context or reconsider admission. |
| `Conflict` | `not_committed` for this batch | Read fresh state before recomputing. |
| `Overloaded` from a submitted native call | `unknown` | A callee may have committed before a nested refusal; reconcile. |
| `OwnershipLost` or `Ambiguous` | `unknown` | Do not infer rollback or blindly replay the mutation. |
| `HostError` for an otherwise unclassified known read | `unknown` | No usable result; retry only according to the read's deadline/policy. |
| `asyncio.CancelledError` | Cancellation, not a commit verdict | Propagate cancellation; previously acknowledged writes remain committed. |

Import `ValidationError`, `Overloaded`, `Ambiguous`, `OwnershipLost`, or
`Conflict` to catch those specific SDK exceptions. A failed durability proof can
reset the activation; reads during cleanup may be refused until recovery ends.
Repeating a known read within its original deadline does not justify repeating
an uncertain business mutation.

```text
Business effect commits --> reply is lost / too large / cancelled
                                      |
                                      v
                              outcome is unknown
                                      |
                                      v
                     look up a durable receipt / reconcile
                     before deciding whether to retry
```

### Alarms, queues, and cron can repeat

| Mechanism | Durable identity and settlement rules |
| --- | --- |
| Alarm | Persist a business occurrence ID with `alarm_at_ms` in one batch. Commit its receipt with the effect. Retry backoff can change `scheduled_ms`; use the stored occurrence ID to deduplicate. |
| Queue | Use business idempotency keys. Ack/retry decisions are persisted when dispatch returns; an ack survives a later handler exception, but a timeout dropping dispatch can redeliver. |
| Conflicting queue decisions | The first conflicting message/batch ack or retry decision wins. Settlement handles remain invocation-scoped. |
| Queue owner recovery | An unknown storage outcome may preserve a lease even before the consumer starts. Redelivery waits for its expiry; recovery does not extend it. Budget for admission, handler, and settlement time in recovery SLOs. |
| Cron | Retries retain the original occurrence timestamp across timeout and recovery. Completed expressions are checkpointed; unfinished work resumes. New deployment/schedule configuration replaces prior pending work. |

## Resource and deployment contract

### Capacity and byte budgets

Defaults are defined by [Options](runtime/mod.rs). Admission fails immediately
when full; the adapter does not build an unbounded queue.

| Option | Default | What it bounds |
| --- | --- | --- |
| `max_invocations` | 128 | Top-level and nested Python calls together; waiting callers retain slots. |
| `max_pending_operations` | 32 | Each invocation's native operation/completion mailbox. |
| `max_operations` | 1,024 | Total SDK operations per invocation. |
| `max_batch_operations` | 128 | Checks, puts, deletes, and an optional alarm decision; also supplied read-batch keys. |
| `max_tasks` / `max_objects` | 1,024 / 4,096 | Managed asyncio tasks / resident Python objects. |
| `max_body_bytes` | 1 MiB | Buffered HTTP body. |
| `max_value_bytes` | 2 MiB | Entire encoded invocation, batch, SDK result, or response envelope; minimum 128 bytes. |
| `max_bridge_bytes` | 32 MiB | Aggregate retained native bridge charges. |
| `shutdown_timeout` | 5 seconds | Cooperative Python cleanup budget; not a hard Python interrupt. |
| `poll_interval` | 250 ms | Maximum housekeeping wait; ordinary work wakes the loop immediately. |
| `telemetry_interval` | 250 ms | Interpreter observation interval. |

A nested call needs another invocation slot while its parent waits. Size capacity
for fan-out. Full application admission uses native `ApplicationOverloaded`
(HTTP 503); submitted SDK calls preserve `Overloaded/unknown`. Closed-runtime
admission is a lifecycle failure. Native peer-capacity retries stay within the
original routing budget; ownership and output-proof failures take precedence.

Task and resident-object saturation reported through an invocation preserve this
same typed overload contract, including an unknown mutation outcome. They never
retry a handler or evict a live Python object independently of native ownership.
Native residency and the Python object budget are separate limits: the CLI has
no resident-cell ceiling unless `CELLAR_MAX_RESIDENT_CELLS` is configured. Set a
native cap below `Options.max_objects`, leaving room for pending activation and
retirement, when serving more distinct cells than the Python cache can hold.
Library hosts configure `ActorPolicy.max_resident` explicitly. Native eviction
retires the corresponding Python object; later activation reloads durable state.

```text
HTTP value crossing the bridge
+----------------------+--------------------------+
| JSON metadata        | separate immutable bytes |
| body field is ""     | actual HTTP body         |
+----------------------+--------------------------+
                |
                v
 charged size = encoded metadata + base64-equivalent body length
 copied bytes = actual owned metadata/body copies
```

Requests, completions, and nested fetches use this byte path. It avoids allocating
a base64 body while preserving the same encoded-envelope limit. Mutable buffers
are copied to immutable bytes. Reservations cover copying and validation and
are released on refusal, cancellation, or completion.

An oversized request is refused before native dispatch. An oversized reply may
follow a committed effect and retain an unknown outcome. Encoding failures and
completion-capacity refusals fail the invocation without rolling back writes;
even a refused failure envelope does not by itself stop later runtime work.
See [byte accounting and failure boundaries](runtime/README.md#byte-accounting-and-failure-boundaries)
for mailbox and serialization details.

### Build-time interpreter selection

Select CPython through `PYO3_PYTHON` when building the `embed` feature. Use a
shared libpython, development headers, and matching runtime/stdlib/native wheels.
Follow the [runnable setup](../../examples/python/README.md#install-and-run) for
the complete local environment.

```sh
# From the repository root, with a checkout-specific CARGO_TARGET_DIR:
export PYO3_PYTHON="$(command -v python3.14)"
cargo build --locked -p cellar-python --features embed --bin cellar-python-example
```

| Build choice | Requirement |
| --- | --- |
| CPython 3.14 | Build and serve with the selected ABI and library layout. |
| Free-threaded 3.14t | Select `python3.14t`; libpython, stdlib, import roots, and wheels must match `cp314t`. The host cannot switch ABI at runtime. |
| Cross-build | Supply `CELLAR_PYTHON_BUILD_LAYOUT` and the target's matching libpython/PyO3 configuration; target paths must exist at runtime. |
| Native-only build | Skips Python probing and linking. |

The build records interpreter home, executable, stdlib roots, and SOABI through
isolated `-I -S -B` startup. It rejects a different minor version, non-CPython ABI,
mismatched threading layout, or non-shared libpython. This layout capture alone
is not a verified deployment inventory.

Free-threaded Python still dispatches handlers and SDK calls on the managed
asyncio loop. Worker threads may compute in parallel, but cannot borrow SDK
authority, even with inherited contextvars. An incompatible extension can cause
CPython to enable the GIL. The example health endpoint distinguishes build ABI
(`free_threaded`) from actual runtime state (`gil_enabled`).

A cross-build layout has this shape:

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

For 3.14t, set `gil_disabled: true` and use the matching `cpython-314t-...` SOABI.
Main-thread ownership checks currently support Linux and macOS.

### Package an immutable Python environment

```text
Pinned CPython + stdlib + installed packages + native prerequisites
                              |
               capture source / descriptor / ABI / hashes
                              |
                    inventory + environment archive
                              |
              cellar build binds inventory into executable
                              |
                     deploy -> install and verify
                              |
          exact immutable Python prefix + new Rust executable
                              |
      verify loaded providers -> import app -> admit native host
```

| Configuration | Purpose |
| --- | --- |
| `Options::interpreter` | Explicit home, executable, stdlib roots, and SOABI; defaults come from the selected build-time interpreter. |
| `Options::module_paths` | Absolute, canonicalized dependency roots; bound package roots become defaults in verified builds. |
| `Source::require_distribution(name, version)` | Refuse startup if an installed distribution is missing or mismatched. |
| `Options::environment` | Explicit initial Python `os.environ`/`os.environb` view; empty by default. Interpreter override keys are rejected. |
| `python_environment` project field | Bind and publish the verified runtime/package archive with the executable. |
| `cellar install --python-root ...` | Restore at the exact immutable prefix; different existing contents are never overwritten. |
| Host runtime inventory identity | Reject generations whose declaration differs before running application factories. |

Startup uses isolated CPython configuration: ambient Python paths, user site,
`.pth`/customize hooks, argv parsing, automatic initialization signal handlers,
and bytecode writes are disabled. Rust retains INT/TERM shutdown ownership.
No compiler, package installer, or dependency inspection tool runs at node startup.

The inventory also binds native dependencies and the qualified OS cohort.
External library hashes and macOS system-cache identities are checked; loaded
providers are verified before initialization and again after imports. OS libraries
remain provisioned by the qualified host. OS/library changes require recapture
and rebuilding. Follow the [environment workflow](../../tools/python/README.md)
for exact capture, deployment, and prerequisite checks.

Python's explicit environment view does not alter the Rust host's initial
configuration. It is not process isolation: Python writes to `os.environ` and
native libraries retain ordinary process effects. Debug formatting redacts
configured environment names and values.

## Shutdown, supervision, and diagnostics

```text
Cooperative Python                    Blocked Python / native extension
        |                                          |
stop signal -> native drain              deadline revokes SDK authority
        |                                          |
cancel tasks, join executor               Python may still be running
        |                                          |
combined native/Python Report            external process watchdog
        |                                terminates / restarts process
check complete(), not just exit status
```

Use async libraries for cooperative I/O. CPU-bound Python, a GIL-holding extension,
or code suppressing cancellation can stall the interpreter. Rust's deadline
revokes SDK authority but cannot forcibly stop that code. The shipped Unix
[`cellar supervise` watchdog](../../tools/python/README.md#process-supervision)
provides bounded startup, liveness, hard termination, and restart; its health
endpoint must execute through Python.

Applications must join their own threads and private executors. Cancelling
`asyncio.to_thread` does not stop its underlying function. Direct library side
effects bypass Cellar durability/idempotency, and native extension crashes can
terminate the whole process. This is trusted native execution.

| Observation | How to interpret it |
| --- | --- |
| `Report::complete()` | Requires managed work, reservations, operation accounting, object retirement, failures, and all recorded native outcomes to be settled/complete. |
| `Incomplete.report` | Retains the combined report when cleanup or host execution fails. |
| `PythonApplication::bridge_report()` | Independently sampled atomic counters; conservation checks apply after quiescence. |
| `PythonApplication::interpreter_snapshot()` | GIL-free, non-waiting observation; check `generation` and `age_ns`. `None` can mean no sample or a busy publisher. |
| `Report::interpreter` | Interpreter-thread CPU, handler/loop timing, allocated blocks, and process peak RSS; not per-cell cost attribution. |
| `Report::memory` | Before/after process and allocator samples; unavailable values are `None`. CPython remains initialized. |
| `Options::trace_allocations` | Optional CPython tracing, off by default; adds overhead and excludes some native allocators. |

The example emits the combined report with `CELLAR_SHUTDOWN_REPORT_JSON=1`.
Task-cleanup exceptions, unraisable exceptions, and unhandled Python-thread
exceptions make the report incomplete even if no tasks remain. Managed diagnostics
use fixed failure stages instead of exception values and traceback paths.
Application logging and pre-main OS loader output remain process output.

See [runtime internals and diagnostics](runtime/README.md) for exact timing scopes,
copy accounting, startup stderr handling, and cleanup invariants. A report covers
runtime-owned work and explicitly recorded nodes; it does not certify arbitrary
native threads or the process-wide heap.

## Validate changes and find the code

Use the repository Rust toolchain, selected CPython, and a checkout-specific
`CARGO_TARGET_DIR` from [AGENTS.md](../../AGENTS.md). After preparing the example's
dependencies, the focused checks are:

```sh
cargo test --locked -p cellar-python --features embed -- --test-threads=1
"$PYO3_PYTHON" tools/python/test_runner.py
python3 tools/docs/check.py
```

| Source | Responsibility |
| --- | --- |
| [application.rs](application.rs) | Source declarations, native application/cell proxies, original-invocation execution. |
| [python/cellar.py](python/cellar.py) | Typed Python facade, contexts, values, storage, bindings, errors. |
| [python/runner.py](python/runner.py) | Managed asyncio loop, handlers, tasks, object retirement, shutdown. |
| [runtime/mod.rs](runtime/mod.rs) | Bootstrap, options, joined host supervision, report types. |
| [runtime/bridge.rs](runtime/bridge.rs) | Native sender, invocation checks, operation admission. |
| [read_batch.rs](read_batch.rs) | Bounded multi-key replies and pre-dispatch snapshot capture. |
| [cellar-guest](../cellar-guest/README.md) | Shared native operations, HTTP limits, checked batches, failure provenance. |
| [runtime guide](runtime/README.md) | Internal scheduling, initialization, accounting, and observability map. |

Use the [baseline harness](../../tools/python/baseline.md) for performance and the
[fleet fixture](../../tools/python/fleet.md) for deployment/recovery qualification.
[Plan 007](../../plans/007-embedded-python-pyo3.md#implementation-milestones-and-acceptance-gates)
tracks the remaining acceptance gates; local checks do not establish production
fleet qualification.
