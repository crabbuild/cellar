# 007: Embedded CPython applications through PyO3

Status: partial implementation; full-plan completion is unproven. The
[requirement-by-requirement audit](007-embedded-python-audit.md) tracks the remaining
implementation, artifact and qualification gates without reducing this plan's scope.

Implementation: [cellar-python](../crates/cellar-python/README.md), with a
[runnable example](../examples/python/README.md). Buffered HTTP, cell JSON and
SQL, checked batches, alarms, KV, shared SQL, queue handlers and cron are wired
through original invocation futures. One interpreter and a joined Rust supervisor
have explicit admission, cancellation and returning-host shutdown contracts.
Source and SDK facade bytes are embedded in the native executable. Installed
package version pins are checked before admitting the host. A build-bound inventory can additionally verify the provisioned runtime, installed files, loaded libpython and source registrations before initialization.

Local validation covers real-node storage conflicts, context expiration,
cancellation, alarms, preserving restart, remote-owner routing and handoff
recovery. The packaged smoke additionally covers Pydantic/native-extension imports,
KV, SQL, queue delivery and cron. These are implementation checks, not broad
production certification. The design below records the intended full contract.

Remaining qualification: qualify the reset/read audit correction on Linux,
diagnose preserved lifecycle failures and complete final-source regression
evidence in both threading ABIs. The Linux installed-host prerequisite/refusal,
offline recovery and replacement steps pass in both ABIs at 97be2cc. Operating
defaults have repeated local measurements and Linux CI workload evidence within
their stated development scope. Interrupted cron delivery, admission/retirement races and
native-versus-Python development measurements now have direct evidence in the audit.
Independent KV, shared-SQL
and queue matrices now cover cancellation, failed proof, explicit reconciliation,
backpressure, resource cleanup and preserving restart. The queue producer drill
also exposed and fixed a native reset race with a still-running alarm. Supported
macOS ARM64/Linux x86_64 package/runtime qualification is recorded by exact version
in the audit. Real three-node tests withhold both follower acknowledgements and
bucket writes, then release actual follower acknowledgements with bucket writes
still held. They exercise explicit sync and final output separately and refuse
success when both proof paths fail. Native-writer conflicts and a stale Python
task resumed after a real ownership fence are directly tested. Local CPU-loop
and GIL-held C-call watchdog drills kill owners, remove local disks, recover
acknowledged state from the development fleet bucket and verify new writes.
Native factories also progress while Python remains blocked.
The owner-loss alarm regression additionally exposed mutable retry deadlines;
the example now commits a stable business occurrence with its alarm and uses
that identity for receipts. The actual example handler survives native timeout,
owner death and disk deletion with one effect despite a changed delivery time.
The shipped Unix `cellar supervise` now provides bounded startup/liveness,
process-group shutdown, joined restarts and per-process health correlation.
Its process tests and a real installed-host forced-KILL/replacement drill pass
locally; the [deployment recipe and service template](../tools/python/README.md#process-supervision)
describe operational policy and the distinction between process exit and drain.
Independent loss of source, native extension, package root and libpython refuses
startup in the disposable bound-host matrix, with the exact runtime restored.
Cancellation-suppressing asyncio tasks, host failures/panics,
background interpreter failure and default executor shutdown have local coverage. Native projects can now bundle and restore a verified CPython/runtime/package
closure. The real recovery drill removes the original runtime, installs from
a private bucket and starts the installed host with a cleared environment,
including numerical native libraries and complete joined shutdown. System
libraries outside that closure remain platform prerequisites. A bounded 1 ms polling
bridge is used initially; notification-driven wakeups are deferred.
Prepared: 2026-10-19.
Source baseline: `4008db9d514c53c62989899302d8811db0182a64`.

## Decision and intended outcome

Add an optional `cellar-python` adapter that embeds native CPython in a compiled
Rust application executable. Developers write request handlers, cell business
logic and alarm handlers in Python, using ordinary compatible Python packages.
Cellar continues to own ingress, placement, SQLite, transactions, output proofs,
activation authority, recovery and shutdown.

Use one process-wide CPython 3.14 or free-threaded CPython 3.14t interpreter and one managed
`asyncio` loop initially. Python runs inside the Cellar application process;
Docker, a Python child process, JavaScript and WebAssembly are not required.
This is trusted application execution, not a sandbox for tenant-supplied code.
A native extension crash can terminate the entire node process.

Implement the adapter above the existing `Application` and `Cell` contracts.
Do not add Python, PyO3 or I/O to `cellar-logic`, and do not replace the native
worker's ownership or output-gate implementation. The compiled Rust host remains
the deployment entry point. This extends the native application model with an
optional interpreter; it does not restore the old Python Workers format.

The first release must demonstrate real Python execution, a third-party package,
atomic cell storage, an alarm, preserving restart and owner-loss recovery. Do
not publish a Python example before that path runs end to end.

## Current implementation and integration points

| Existing component | Relevant contract | Adapter integration |
| --- | --- | --- |
| [Application contracts](../crates/cellar/application/mod.rs) | `Application`, `Cell`, `HandlerFuture`, `Invocation`, `Output` | A Rust adapter converts invocations into Python tasks and responses back into native outputs |
| [Native worker](../crates/cellar/runtime/workers/native/mod.rs) | Installs the original event context while polling application futures | Python SDK requests execute inside that original Rust future |
| [Storage authority](../crates/cellar/application/storage/mod.rs) | Original domain, event and activation checks | Retain all checks on reads, writes, sync and commit |
| [Transactions](../crates/cellar/application/storage/transaction.rs) | Short synchronous transactions and scoped asynchronous transactions | Execute bounded atomic batches without entering Python inside a transaction |
| [Execution services](../crates/cellar/execution/services.rs) | Domain-owned services and application work | Own adapter work, admission, cancellation and cleanup |
| [Native packaging](../crates/cellar/cli/native_deploy/mod.rs) | Verified native executable and application configuration | Add an explicit Python runtime/source artifact inventory |
| [Native application ADR](../docs/adr/001-native-applications.md) | Compiled factories; no JavaScript engine | Keep compiled registration and define interpreter support as a new opt-in adapter |

The native loader requires a registered compiled application. This adapter
implements that contract directly; it requires no JavaScript/Wasm bundler.

## Scope and compatibility contract

The initial supported surface is deliberately narrower than the complete Rust SDK:

| Initial scope | Deferred |
| --- | --- |
| Buffered HTTP request/response; JSON and bytes | Streaming bodies, WebSockets and ASGI compatibility |
| Stateless Python handlers, cells, queues and scheduled handlers | Python-defined workflow callbacks |
| Cell-local JSON, SQL and atomic checked batches; KV and shared SQL bindings | General SQL cursors and arbitrary Python transaction callbacks |
| Cell-local durable alarms and explicit storage sync | Background `wait_until`, facets and compiled-loader Python bindings |
| Declared, bounded cell calls through the host | Full SDK parity and arbitrary remote Python object access |
| A qualified set of native Python dependencies | Every PyPI package and subinterpreters |

Native CPython is the compatibility target. Pure Python packages and compatible
native extensions can be used when their interpreter ABI, operating system,
architecture and native dependencies match the packaged runtime. Import success
is necessary but insufficient: representative calls, concurrency, shutdown and
resource use need qualification. Support a single Python dependency environment
per process, shared by all Python handlers in that process.

Direct Python network clients, filesystem access and native library effects are
outside Cellar's transactional boundary. They may be usable as trusted application
code, but they do not acquire Cellar output gating, cancellation or exactly-once
semantics. Prefer host-backed bindings for effects whose ordering depends on a
cell mutation. External receivers still need idempotency and reconciliation.

## Python application interface

The bundled `cellar` facade wraps private invocation-bound PyO3 bridge objects.
The following handler uses the executable facade API. Its Rust source descriptor
must register `fetch` and the `Orders` class's `fetch`/`alarm` events, and the
deployment must bind `ORDERS` to that class.

```python
from pydantic import BaseModel
from cellar import MISSING, Response

class Order(BaseModel):
    id: str
    total_cents: int

async def fetch(request, ctx):
    # Stateless ingress has binding access, but no cell-local storage.
    order_id = request.path.rsplit("/", 1)[-1]
    return await ctx.namespace("ORDERS").get_by_name(order_id).fetch(request)

class Orders:
    async def fetch(self, request, ctx):
        order = Order.model_validate(await request.json())
        if order.id != ctx.identity.name:
            return Response.json({"error": "order identity mismatch"}, status=400)
        await ctx.storage.apply(
            checks={"order": MISSING},
            puts={"order": order.model_dump(mode="json")},
            alarm_at_ms=await ctx.now_ms() + 60_000,
        )
        return Response.json({"id": order.id}, status=201)

    async def alarm(self, scheduled_ms, ctx):
        await ctx.storage.apply(puts={"reminder_pending": True})
```

This create-only example returns a conflict on duplicate creation; it is not an
idempotent API. A real retryable command stores a request digest and receipt
with its business update and returns the retained receipt on a matching retry.
Alarm delivery can repeat; alarm handlers must tolerate re-execution.
The application supplies method/path validation and translates its Pydantic
validation errors into its chosen client-error response. Arbitrary third-party
exceptions are not automatically classified as SDK validation failures.

Require `async def` handlers initially. A cell's Python instance is transient
and is created lazily on its first admitted invocation. Its identity includes
the owning domain, application generation and activation identity/epoch, never
the business name alone. Constructors take no arguments; immutable identity is
available through the handler's `ctx.identity`. Constructors cannot perform host calls. Async initialization, if added,
needs an explicit gate rather than an implicit constructor convention.

The host descriptor registers the module entry point, declared cell classes and
supported events before node admission. Unsupported events fail explicitly.
`validate_configuration` and `create_cell` remain nonblocking Rust operations;
Python imports, constructor execution and reflection happen on the Python loop,
not in synchronous factory callbacks on the Cellar worker.

## Interpreter and executor model

Use a process-owned `PythonRuntime` guard initialized before starting the node.
The executable's main thread runs the managed Python `asyncio` loop. Explicit
Rust runtimes drive the existing Cellar node and invocation futures on their
own threads. The bootstrap owns the loop, threads and join sequence; no hidden
global Tokio runtime or per-request interpreter is introduced.

Use one coordinated process signal owner. CPython's automatic signal handlers
are disabled. The CLI host owns INT/TERM and returns joined outcomes through
`Host::run_until_observed`; embedded hosts supply their own stop future to
`Node::run_until`. The Python bootstrap keeps the loop alive until the Rust
supervisor returns and then joins it. It installs no competing signal supervisor.

A conceptual invocation has two directions:

1. Rust admits an event and creates a bounded invocation mailbox, reply channel
   and cancellation handle.
2. The Python loop schedules the handler with an opaque invocation handle.
3. A Python SDK method enqueues an owned host-operation request and awaits a
   Python future. It does not call the storage implementation on that thread.
4. The original Rust `HandlerFuture` polls its mailbox and the requested SDK
   operation under the event/domain context installed by the native worker.
5. An owned result enters the bounded reply channel. The managed loop polls it
   at the configured interval and resumes Python without blocking a Rust worker.
6. The handler's final value becomes a native `Output`; the existing host proves
   required writes and releases the output.

In Rust, retain owned `Py<T>` handles only where supported by the selected
PyO3 version. Never retain a `Python<'py>` token, a `Bound<'py, T>` borrow, a
SQLite guard or a synchronous mutex across an await, queue wait or cross-thread
notification. Convert to owned values while attached to Python and detach before
waiting on Rust work.

The initial implementation pins PyO3 and uses owned mailboxes rather than
`pyo3-async-runtimes` or experimental async PyO3 functions. This keeps SDK futures
inside their original invocation and avoids a hidden executor with different
execution-domain authority.

All Python handler tasks share the interpreter and loop. Async I/O can overlap;
Python bytecode is constrained by the GIL. A synchronous computation inside an
async handler can block the Python loop while Rust routing/storage maintenance
continues. Offloading trusted blocking library calls to bounded, supervised
threads may be added, but it provides neither safe thread termination nor a
promise of parallel Python bytecode execution. Scale CPU-bound workloads across
node processes initially.

## Context authority and cross-thread dispatch

The Python context is an opaque handle into a Rust invocation registry, not a
raw `Context`, SQLite connection, domain token or configurable cell ID. A
registry entry retains the original Rust context plus runtime, generation,
activation, invocation and cancellation identity. Python-supplied metadata
cannot select another registry entry or expand a binding grant.

Check authority when accepting an SDK request, immediately before its execution,
and through the SDK's existing storage/commit/output checks. Queueing a request
before fencing or cancellation does not authorize later execution. Stateless
contexts cannot access cell storage. Transaction views cannot escape a batch.

Resolve services only while the original Rust application future is being
polled. A plain `tokio::spawn`, a Python `ContextVar`, a copied event ID or an
OS thread-local cannot replace the original installed context. The bridge must
not recursively call the same cell over HTTP to perform a storage operation;
that can deadlock against the invocation or input gate it is meant to serve.

For initial host operations, preserve Python call order with a bounded per-event
queue and permit at most one outstanding mutating operation per invocation.
Define and test concurrent reads and cancellation explicitly rather than
inferring ordering from task scheduling. Nested calls to other cells use normal
Cellar routing, admission, deadlines and ambiguity handling. The mailbox remains
pollable while the parent awaits a nested call; use existing call-cycle behavior
rather than adding an adapter-only bypass.

Keep Python object registries on the interpreter loop. Rust cell proxies hold
opaque object IDs and activation identity. Retirement marks an object unavailable
before further dispatch; destruction runs on the interpreter loop. Releasing a
Rust proxy must not block waiting for the GIL. Track pending destruction records
until processed, bound their accumulation, and drain them before closing the
loop. Do not silently lose cleanup requests when an ordinary work queue is full.
The adapter uses a Rust-owned activation liveness flag. Proxy drop revokes that
flag without entering Python; the loop observes retirement independently of the
ordinary request mailbox. This requires no Python-specific public `Cell` hook.
The audit separately tracks pending cleanup accounting and retirement races.

## Atomic storage without Python callbacks inside SQLite transactions

Implement `storage.apply` as a bounded declarative batch of checks, puts,
deletes and an optional alarm change. Execute the complete batch through the
existing `Storage::transaction_sync` on the Rust invocation driver. Validate
all keys, encoded values, operation counts and aggregate bytes before starting.
No Python code executes and no asynchronous wait occurs inside that callback.

Checks compare the current presence/value with the expected presence/value.
Missing is distinct from a stored JSON null. If any check fails, roll back all
writes and return a typed `Conflict` result. Disallow duplicate/conflicting
operations on a key and reject malformed alarm actions. A batch either commits
its complete local mutation or has no local effect.

Expose `read_json` as an immutable owned snapshot with explicit presence. A
read-modify-write algorithm computes in Python, then includes its read snapshots
as checks in `apply`. A conflict requires application-specific retry; the host
must not automatically rerun a Python handler that may have called an external
service. Unconditional batches remain available for writes that need no check.

All mutations from the Python SDK use the same storage implementation as Rust.
Do not maintain an adapter-only revision counter that native Rust writes could
bypass. Native and Python updates must contend on the actual SQLite transaction
and checks. Restrict revision-based optimizations to a later shared contract.

Expose storage sync through the existing SDK for explicit proof boundaries.
`apply` completion means local transaction success, not automatic replication
proof at that point. The native handler response still crosses the output gate;
callers waiting for an external effect must use an explicit durable intent and
receiver protocol. A lost response can leave an ambiguous committed result.

Defer arbitrary Python callbacks inside transactions. Moving a Python callback
to another thread and awaiting its storage calls would require a separate design
for transaction-view polling, input-gate ownership, reentrancy, cancellation and
rollback. It cannot be implemented as a synchronous PyO3 call while holding a
SQLite lock.

## Values, errors and request boundaries

Start with bounded buffered bodies and owned response values. Preserve headers
as an ordered list of pairs so repeated fields are not lost. Construct the final
response through the existing Rust response/body APIs. Streaming and upgrades
need their own event-lifetime and backpressure contract before support is added.

The initial storage/RPC codec accepts JSON-compatible values with explicit
integer range checks, finite floats, UTF-8 strings, lists and string-keyed
objects. Bytes use a separate tagged payload. Reject unsupported objects,
cyclic containers, oversized nesting, non-finite floats and integer overflow;
do not silently round Python integers. Applications serialize Decimal, datetime,
NumPy values and Pydantic models explicitly. Do not use pickle as a persistence,
RPC or package-loading format, and do not serialize arbitrary Python exceptions.

| Failure | Python behavior | Host behavior |
| --- | --- | --- |
| Validation or unsupported value | `ValidationError` | No mutation; bounded error response |
| Failed atomic check | `Conflict` | Roll back the complete batch |
| Retired event/domain or activation | `SDKError(code="ContextExpired")` / `OwnershipLost` | Refuse new SDK work; ownership loss does not establish rollback |
| Admission or mailbox capacity | `Overloaded` | Bounded refusal; no unbounded queue growth |
| Failed submitted mutation without a proven refusal | `Ambiguous` with `outcome="unknown"` | Do not imply rollback or blindly replay |
| Native result exceeds the encoded-byte or depth limit | `HostError/unknown` for known reads; `Ambiguous/unknown` for possible mutations | Return a bounded fixed failure; retain prior native effects and keep the invocation usable |
| Handler exception | Sanitized handler failure | No success output; prior committed writes are not undone |
| Native extension crash or process abort | No catchable Python guarantee | Whole-node recovery; accepted effects follow ordinary Cellar recovery rules |

Validation and pre-dispatch overload refusals carry `outcome="not_committed"`;
this describes that operation, not earlier writes by its invocation. A submitted
call returning a native capacity verdict remains `Overloaded/unknown`: a callee
can propagate its nested call's refusal after committing its own writes. Unknown submitted reads
retain `HostError`. The bridge preserves native typed routing verdicts where
available and conservatively classifies generic mutation failures as ambiguous.
Backend error text is replaced with a fixed code before returning to Python.
Some native bindings reduce causes internally; the adapter does not reconstruct
a stronger outcome by parsing their messages.

Cancellation remains cancellation. Do not catch it as a generic retryable
application failure. Tracebacks are bounded and scrubbed of request bodies and
secrets; public responses do not expose paths or internal stack frames by default.

## Durable state and Python object lifetime

SQLite, alarms and native durability proofs remain the source of truth. Module
globals, class fields, asyncio tasks, caches, sockets, thread state and Python
heap objects are transient. Recovery constructs a new interpreter/application
process if needed and new cell objects from durable state. Do not snapshot a
CPython heap or claim transparent continuation of arbitrary Python frames.

Cell-local state changes and alarm updates share a transaction when supplied in
one batch. Alarm handlers use persisted state to determine outstanding work and
must tolerate duplicate invocation. Later workflow bindings must persist model
outputs and nondeterministic choices through the existing workflow journal;
ordinary coroutine suspension is not a durable workflow checkpoint.

Python filesystem writes are not captured as cell SQLite changes. Python
`sqlite3` or SQLAlchemy must not open a live Cellar cell database directly.
Using a separate application database is possible, but it introduces a separate
transaction and recovery authority. External libraries do not become Cellar
bindings merely because they are imported in the same process.

## Cancellation, background work and shutdown

For each invocation, dropping or retiring the Rust driver immediately revokes
host-operation admission and requests cancellation of its Python root task.
Reject late mailbox operations and outputs. A transaction already executing
retains the SDK's authority rechecks and rollback rules; cancellation after a
commit does not establish that the mutation was undone.

Use an owned cleanup lane that survives ordinary request admission closure.
Never block a Rust destructor on Python. The supervisor retains cleanup work
until cancellation is delivered, queued operations settle, object cleanup runs
and the task is observed terminated, or the drain deadline expires.

Do not initially expose post-response `wait_until`. Application-created tasks
must not retain usable SDK context beyond the owning event. Python-created
threads and tasks can still perform raw process effects; the trusted-code model
cannot prevent those effects by expiring an SDK handle. At shutdown, cancel and
observe remaining loop tasks, close async generators and managed executors, and
record any work that cannot be joined.

Shutdown order:

1. Close public ingress and Python submission admission; retain accepted work.
2. Drain native events and cooperative Python handlers with both runtimes alive.
3. Cancel remaining handlers, revoke invocation handles and settle host cleanup.
4. Join node shutdown/durability work; drain Python task/object cleanup.
5. Close the Python loop after its work is settled and join Rust executors last.

The process-owned supervisor returns a combined report containing the existing
node report plus queued/running Python work, pending bridge operations, cleanup
records, unmanaged loop tasks and exhausted budgets. A node-only zero exit or
empty mailbox is not proof that Python work drained.

There is no safe general-purpose hard kill for an in-process Python task or C
extension. An infinite loop, blocked native call or cancellation-suppressing
handler can prevent drain. The supervisor must mark the combined outcome
incomplete and rely on an external process supervisor to terminate/restart the
node when necessary. Do not call `Py_FinalizeEx` while threads may enter Python.
Initialize once per process; do not finalize/reinitialize on cell eviction or
application reload. Default production cleanup ends at process exit after
managed work drains; controlled finalization is a separately tested embedding
mode, not a recovery mechanism.

## Deployment, dependencies and generation changes

Use an immutable Python source/dependency bundle associated with the compiled
host. Resolve and hash-lock dependencies during build, including platform wheels
and native shared-library requirements. Requests must never trigger package
installation. Do not install from the network at node startup.

A Python-enabled deployment inventory must identify:

- Native host digest, target and adapter version.
- CPython major/minor, GIL mode, SOABI, runtime/shared-library inventory and digests.
- Application module/source digest and registration descriptor.
- Locked package artifacts, dependencies and relevant native library inventory.
- Canonical import roots, supported platform and application configuration.

Verify the actual runtime and bundle before loading application code and opening
admission. Development may use an explicitly configured CPython installation
and environment; production initially requires a qualified immutable runtime
image or provisioned runtime closure. A container image is one packaging choice,
not a requirement of embedding. An arbitrary mutable virtualenv is not a
self-contained verified deployment artifact.

Use explicit CPython initialization configuration: import paths, Python home,
site behavior and environment handling are supplied by the executable bootstrap.
Do not discover an interpreter or install packages inside library code. Ambient
`PYTHONPATH`, current-directory imports and user site packages must not silently
change the deployed application. Link libpython for embedding; do not accidentally
use PyO3's extension-module linking mode for the host executable.

The native package now records a typed Python inventory/archive reference for
each supported executable and gates admission with `embedded-python-v1`.
Build, publication and installation verify those artifacts; startup rechecks
the restored files before importing the application. Keep
Cellar-owned format markers at 1 and use the repository's hard-cutover policy
when introducing internal metadata; do not imply old packages gain support.

One interpreter means shared `sys.modules`, native extension state and one
Python dependency environment. The first release permits one Python application
generation per process and refuses in-process Python hot replacement. Python
source changes require draining/replacing the application process, retaining its
durable Cellar data and identity. Ordinary native-only hosts keep their existing
generation behavior. A later design can qualify overlapping Python generations;
changing module names alone is not dependency or global-state isolation.

## Limits, observability and trust

Capture finite startup, request, body, response, JSON-depth, batch-operation,
batch-byte, mailbox, live-task, resident-object and drain limits in the adapter
configuration. Validate them before admission. Begin implementation with named
limits in tests; choose shipping defaults from measurements, not estimates here.
Apply bounded backpressure in both directions and release reservations on error,
cancellation, retired activation and response-conversion failure.

Measure queue wait, bridge round trips, Python handler duration, Rust SDK time,
GIL-related contention, copied bytes, RSS, Python allocations, native allocation
growth, resident objects, pending cleanup and drain outcomes. Python allocation
accounting is not total process memory, particularly with NumPy/native libraries.
Do not advertise per-cell hard memory or CPU isolation in a shared interpreter.
Existing native CPU accounting measures Rust turn execution; it must not be
reported as a measured or enforced Python CPU budget when work runs on the
Python loop. The adapter now provides bounded bridge/SDK latency histograms,
operation lifecycle conservation, explicit boundary-copy counters and age-stamped
live interpreter/work snapshots. Optional CPython tracing reports allocation
bytes; separate host allocator probes expose scoped native memory growth. Loop
scheduling delay is composite, rather than an exclusive GIL measurement. See
the [adapter observation contract](../crates/cellar-python/README.md) for exact
measurement boundaries and defaults. Attribute Python work separately and disclose accounting gaps for
native extensions and application threads.

Trusted Python has ordinary process capabilities and can bypass wrapper APIs.
The context bridge protects correct SDK usage; it is not a security boundary
against malicious native Python code. Run incompatible dependencies, untrusted
code, workloads requiring hard termination, GPU isolation or independent process
control in separate worker processes/containers instead.

## Code organization

The separate adapter crate follows the `cellar-axum` dependency direction:

```text
crates/cellar-python/
  Cargo.toml
  lib.rs
  application.rs          Application/Cell proxies and registration descriptor
  runtime/mod.rs          Interpreter bootstrap, loop ownership and lifecycle
  runtime/bridge.rs       Bounded invocation messages and awaitable completion
  runtime/initialization.rs Isolated CPython initialization and host signals
  runtime/inventory.rs     Verification before interpreter initialization
  runtime/budget.rs        Native bridge resource reservations and copy counters
  runtime/telemetry.rs     Bounded operation lifecycle and latency observations
  runtime/snapshot.rs      Nonwaiting cache for sampled interpreter observations
  runtime/memory.rs        Pre-initialization/post-join host memory probes
  operations.rs           SDK dispatch within the original native invocation
  failure.rs              Proven refusals and conservative mutation ambiguity
  storage.rs              JSON snapshots and checked atomic batch conversion
  values.rs               Owned request/response codecs
  python/cellar.py         Bundled typed Python facade
  python/runner.py         Loop, activation registry, task cleanup and metrics
  tests/                  Public adapter lifecycle and recovery tests
```

Keep all PyO3 dependencies in this crate and centralize their versions in the
workspace manifest. Prefer an explicit embedding feature so native-only builds
do not require libpython. The workspace's all-feature CI will enable that
feature, so provision the selected CPython toolchain in that job and add an
explicit native-only dependency/build check. Do not add a JavaScript engine.

Use existing SDK contracts whenever possible. Any required new public lifecycle
or execution-domain registration hooks must be narrowly scoped, reviewed and
covered by native-only regression tests. Do not expose installed-turn internals
or let the adapter fabricate an activation context.

## Implementation milestones and acceptance gates

| Phase | Deliverable | Required evidence before proceeding |
| --- | --- | --- |
| M0: feasibility | Pin mutually compatible PyO3/async bridge versions and CPython 3.14; prove bootstrap and invocation pump | Python awaits a host operation while another Python task runs; original turn/domain checks pass; cancellation revokes queued work; two embedded domains cannot exchange contexts; no hidden runtime owns host I/O |
| M1: real HTTP | Compiled host imports a pinned Python module and third-party validation package | Success/error/body-limit cases, native-extension import and representative execution, concurrent requests, exception sanitization, complete combined stop report |
| M2: durable cells | Cell dispatch, JSON storage, checked batches, explicit sync and alarms | Atomic rollback, missing versus null, conflicts with native writers, duplicate alarm handling, restart persistence and acknowledged-write proof tests |
| M3: failure/lifecycle | Activation object retirement, fencing, cancellation and supervised shutdown | Node kill and owner takeover preserve acknowledged state; stale Python task cannot mutate after ownership loss; deadlines and noncooperative code produce incomplete drain, never fabricated success |
| M4: packaging | Runtime/source/dependency inventory, verified install and process replacement | Wrong ABI/missing library/tampered source refuses startup; offline deployment; old/new process handoff; runtime artifact recovery; Python example runs from a fresh installation |
| M5: SDK growth | Additional bindings and optional ASGI/stream adapters | Independent authority, cancellation, backpressure, replay and resource tests for each surface |

Core features span all six phases; the milestones are not all accepted. The
[completion audit](007-embedded-python-audit.md) distinguishes local evidence,
published source and remaining qualification:

| Phase | Current assessment | Gate still open |
| --- | --- | --- |
| M0 | Bootstrap, invocation pump, authority and cancellation implemented; direct evidence in both threading ABIs | Final-source platform matrix |
| M1 | HTTP, pinned native packages, bounds and sanitized failures implemented; three complete 97be2cc Linux jobs pass across both ABIs | Complete final-source platform qualification |
| M2 | All S01–S09 storage/alarm requirements have direct evidence within their stated scopes | Final-source regression qualification |
| M3 | Fencing, retirement, takeover and supervised noncooperative shutdown implemented; local lease correction and both ABI suites pass; a controlled reset/read regression corrects the final observation audit | Qualify the published reset/read correction on Linux and diagnose preserved delivery, initial consumer admission and clock/replay failures |
| M4 | Verified packaging, native prerequisites, offline restoration and process replacement have direct evidence locally and in both Linux ABI package steps at 97be2cc | Complete final-source release regression matrix |
| M5 | KV, shared SQL, queues and cron implemented with independent local failure matrices; repeated optimized defaults pass locally and Linux CI defaults pass in both ABIs | Final-source qualification; optional ASGI/streaming remain deferred |

Qualification of source 97be2cc completes the Linux installed-package step in
both push ABI jobs and the conventional PR job. The PR free-threaded job fails
earlier when its final read-only state audit races native reset after a deliberate
proof failure. A controlled public Cell destruction barrier reproduces the same
HTTP 503 with one business attempt. The local correction observes recovery by
repeating only that read-only query within the original HTTP budget; both local
ABI suites and strict Clippy pass, and the correction still needs Linux execution.
The other three 97be2cc jobs complete successfully, including native-node 88/88,
workspace tests/lints, Axum modes and native examples. The
[terminal Linux evidence](evidence/python-linux-97be-2026-10-20.json) preserves all
four outcomes. These results do not qualify later source or explain every
historical lifecycle failure. A bounded native cause diagnostic and opt-in Linux
fresh-fixture consumer replay mode target the preserved initial admission refusal;
passing repetitions alone cannot establish its cause.

Prioritize the audit's [current work](007-embedded-python-audit.md#current-work)
before adding more surfaces. A gate closes only with its stated evidence on
the relevant source, threading ABI and platform; a historical successful CI run
does not qualify later changes.

Do not weaken existing native SDK assertions to fit the Python adapter. Use the
existing SDK, Axum host-parity and fleet recovery harnesses where applicable.
Tests for process aborts, infinite loops and stuck C extensions run in disposable
child hosts so they cannot hang the test runner. Include a test where the client
loses a reply after commit and resolves it with the same receipt key.

Publish qualified package versions and platforms. Start with a pure Python
package, a PyO3-based extension and NumPy or another native numerical package;
exercise actual operations, blocking behavior and teardown rather than import
alone. Establish native Rust versus Python adapter latency, throughput, memory,
copying and recovery baselines without claiming a performance target in advance.

The [recorded development baselines](007-embedded-python-baselines.md) now
provide matched CPython 3.14.5/3.14.5t and native-only measurements, repeated raw
request evidence, memory/copy observations and bucket-only owner-loss recovery.
They state the shared-machine load and development-store limits explicitly;
remaining qualification gates are tracked independently in the audit.

## Alternatives and decisions still needing evidence

- **CPython worker process:** strongest alternative for fault containment,
  independent termination and dependency isolation. Retain it for workloads the
  embedded model cannot safely operate. It needs a separate IPC/storage bridge.
- **Pyodide:** a different JS/Wasm execution stack with a different package ABI;
  not needed to achieve native Python library support in this proposal.
- **Subinterpreters:** defer until extension isolation and lifetime contracts
  are qualified. Free-threaded CPython 3.14t is supported as a distinct build ABI,
  with explicit parallel-worker, ownership, cleanup and package qualification.
- **One interpreter per cell:** reject for the initial design; it multiplies
  interpreter/import cost and assumes extension isolation that is not established.
- **Arbitrary Python transaction callbacks:** defer; the batch API avoids holding
  SQLite locks while calling back across runtimes.

Accepted bootstrap, bridge, retirement, combined reporting and deployment
decisions are recorded in [ADR 004](../docs/adr/004-embedded-python.md). The
[completion audit](007-embedded-python-audit.md) retains the remaining direct
evidence gates for those contracts; accepting a design does not qualify every
failure, package or platform.

## References and planning validation

Primary external references, consulted on 2026-10-19:

- [PyO3: calling Python from Rust](https://pyo3.rs/main/python-from-rust.html).
- [PyO3: parallelism and interpreter attachment](https://pyo3.rs/main/parallelism.html).
- [PyO3: async/await constraints and experimental support](https://pyo3.rs/main/async-await.html).
- [PyO3 async runtime bridge](https://github.com/PyO3/pyo3-async-runtimes).
- [PyO3: building and distribution](https://pyo3.rs/main/building-and-distribution.html).
- [CPython 3.14 initialization configuration](https://docs.python.org/3.14/c-api/init_config.html).
- [CPython 3.14 interpreter lifecycle](https://docs.python.org/3.14/c-api/interp-lifecycle.html).

The original design was prepared from source inspection and primary
documentation before implementation. The current adapter pins PyO3 and its
build configuration to 0.30.1. The initial implementation targeted GIL-enabled
shared CPython 3.14; the subsequent free-threaded amendment supports both ABIs.
The interface and organization above describe the executable implementation.
Qualification results below and in the audit record their exact scope.
Performance baselines and the remaining audit gates are not implied by the
implementation or by acceptance of ADR 004.

Original design-only validation: `python3 tools/docs/check.py` passed. The new plan's
nine local source links, Python sketch syntax, code fences and index entry were
checked, and `git diff --check` passed. No Rust tests were rerun for this
documentation-only change.

## Implementation validation (2026-10-19)

Local validation used Rust 1.98.1, GIL-enabled CPython 3.14.6 and PyO3 0.30.1
on macOS ARM64, with a checkout-specific Cargo target directory.

- `cargo test -p cellar-python --features embed --locked`: passed. The real-node
  harness checks HTTP bytes, native imports and thread-pool calls, foreign-loop
  rejection, checked-batch conflict rollback, missing/null distinction, local SQL,
  alarms, expired contexts, operation/body budgets, cancellation, abandoned writes,
  preserving restart, remote routing and handoff recovery. Separate bounded child
  processes check missing dependencies, supervisor errors/panics, stubborn tasks,
  and fatal background work. The harness also checks interpreter reinitialization
  refusal.
- `python3 examples/python/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" --with-cron`:
  passed against the real packaged host. Pydantic's native extension, HTTP input
  validation, independent cells, alarms, KV, shared SQL, queue retry/settlement,
  a real scheduled occurrence and preserving restart were exercised. A final
  smoke without the minute wait passed after adding the foreign-loop guard.
- `cargo check -p cellar-python --no-default-features --locked`: passed.
- Clippy with `-D warnings`: passed for all `cellar-python` targets/features and
  the `cellar` library.
- Native daemon regression: passed. The broader native-node run passed 86 of 87
  cases; its first bounded bootstrap-observation test hit the existing factory
  entry deadline during concurrent builds. That exact test passed when run alone
  (0.75 seconds). Its assertions and deadlines were unchanged; the initial broad
  run is not recorded as an entirely passing suite.
- `cargo format --check`, `cargo check-layout`, documentation integrity,
  `git diff --check` and the all-feature JavaScript-engine dependency guard passed.

The full workspace suite and Linux CI were not run locally. The workflow now
installs CPython 3.14 and exercises the Python package on Linux. These checks do
not qualify arbitrary Python packages, unclean fleet failures or the separately
provisioned runtime/dependency closure.
