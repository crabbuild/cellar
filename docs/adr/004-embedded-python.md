# ADR 004: Embed trusted Python above the native SDK

Status: accepted implementation decision; production qualification remains open.
Owner: Cellar runtime and SDK contributors. Recorded: 2026-10-20.
Scope: [plan 007](../../plans/007-embedded-python-pyo3.md), extending
[ADR 001](001-native-applications.md).

## Context

Applications need CPython libraries for business logic while retaining Cellar's
cell identity, SQLite transactions, ownership, durable output and recovery.
The interpreter's thread and event loop cannot borrow the native worker's
installed SDK context. Arbitrary Python coroutine frames and native extension
state cannot supply Cellar durability or safe in-process termination.

## Decision

Use the optional `cellar-python` crate with pinned PyO3, one GIL-enabled CPython
3.14 interpreter and one main-thread asyncio loop. Initialize before starting
the Rust supervisor, with explicit home/import roots, isolated configuration
and an explicit Python environment view. Import signal before asyncio and remove
only CPython's implicit INT handler; preserve an existing native handler. The
Rust host owns process signals. Never finalize/reinitialize the interpreter or
replace its source/dependency environment in the same process.

Compile source, facade and class/event registrations into the native executable.
Validate async handler signatures and synchronous no-argument constructors before
admission. Synchronous Rust factories allocate proxies without entering Python;
Python cell objects are created lazily on the managed loop. Each proxy has a
distinct activation lifetime; business names alone do not identify instances.

Pass owned bounded values through invocation mailboxes. Python awaits a private,
revocable bridge; the original Rust handler future polls SDK operations under
its original execution domain/event. Keep host futures in that invocation rather
than moving them onto another executor. Start operations in receive order, allow
known reads to overlap and admit one outstanding mutation per invocation. Queue
and reply capacity, aggregate bytes, values, tasks and objects have finite limits.
The initial reply loop polls at a configured interval; notification-driven
wakeups remain deferred pending measured evidence.

Checked storage batches use the existing synchronous SQLite transaction without
Python callbacks or awaits. Presence-aware checks detect native Rust writers too.
State and alarm changes commit together. Local commit does not imply replication
proof: sync and final responses retain native proof gates. Receipts reconcile
lost responses; the adapter does not automatically replay ambiguous mutations.

Bind a runtime/source/file inventory into verified builds. Native packages pair
the executable, inventory and bounded environment archive under format marker 1
and the `embedded-python-v1` feature gate. Build/publication/install/startup and
generation admission verify the applicable identities before factories or app
imports. Restore verified files into an explicit prefix without overwriting
different existing contents. No runtime/request installs packages. A changed
Python source is a new executable/process, with ordinary native handoff and
durable state recovery.

Revoke SDK admission when native work ends, then observe cancellation, retired
objects, async-generator/default-executor cleanup and native joins. Combined
reports retain native incomplete/fence outcomes and Python failures; zero Python
tasks cannot turn a failed native drain into success. Sanitize managed, unraisable
and raw-thread exception reporting. Attribute interpreter-thread CPU separately;
process RSS and Python block counts are neither per-cell limits nor total native
allocation accounting.

## Alternatives and consequences

Pyodide requires a JavaScript/Wasm execution model and does not meet native
CPython package compatibility. A separate CPython worker remains appropriate for
independent dependency/process isolation, but is not this embedding contract.
General async conversion runtimes do not propagate Cellar turn authority; owned
mailboxes keep SDK execution in the native invocation.

Native extensions share the host process. CPU loops, a GIL-held C call or raw
library work may need an external supervisor to terminate and restart the host.
Direct filesystem/network/library effects remain outside Cellar's transaction
and proof boundaries. Heap objects, globals and coroutine suspension are
transient; compatible packages still require platform/ABI/dependency qualification.
ASGI, streaming, arbitrary Python transaction callbacks, subinterpreters,
free-threaded Python, untrusted-code isolation and full SDK parity remain deferred
as declared in the original plan.

## Compatibility and evidence

Ordinary native-only builds do not initialize or require Python and retain
their existing generation semantics. Business data uses existing native storage
formats; there is no Python heap snapshot or legacy Python/Wasm migration reader.

The [adapter guide](../../crates/cellar-python/README.md),
[bridge/bootstrap harnesses](../../crates/cellar-python/tests/bridge.rs) and
[runtime package/source-upgrade qualification](../../tools/python/README.md)
define executable APIs and checks. Real TERM/INT upgrade tests use different
Python cell code, retain acknowledged state/receipts and require both joined
reports to complete. The [completion audit](../../plans/007-embedded-python-audit.md)
records exact scope and failures. This decision accepts the integration model;
quorum/binding fault matrices, external dependency attestation, remaining telemetry
and measured baselines are still release gates, not waived requirements.
