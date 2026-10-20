# Embedded Python: implementation and completion audit

This ledger audits [plan 007](007-embedded-python-pyo3.md) against the actual
implementation. The objective remains the complete plan, including packaging,
failure qualification, observability and measured baselines. A runnable example
or passing happy-path smoke is insufficient to complete it. Explicitly deferred
features (ASGI, streaming, arbitrary transaction callbacks, untrusted-code
isolation, subinterpreters and free-threaded CPython) remain deferred.

The preceding implementation turn made concrete progress: it added the adapter,
example and integration harness. The full objective is **not achieved**. Prior
command results are recorded in the plan; every affected gate needs current
source and executable evidence before the final completion claim.

Status meanings: **partial** = some implementation/evidence exists;
**missing** = implementation or required direct evidence absent;
**contradicted** = current implementation conflicts with the requirement;
**verified** = the explicitly stated row has direct current evidence (not a
claim that its entire milestone is complete).
Only a fresh, requirement-specific verification can mark a row **verified**.

## Architecture, bootstrap and authority (M0)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| A01 | Separate optional adapter; PyO3 outside logic/core | Dependency graph and native-only build without Python | partial: separate crate, embed feature |
| A02 | Pin CPython 3.14, GIL mode and compatible bridge | Build-time ABI guard plus runtime preflight, pinned Cargo graph | partial: runtime version check, PyO3 0.30.1 |
| A03 | Initialize before node; explicit home/import roots/site/env | Isolated configuration before Python code/imports; hostile environment tests | verified: explicit PyInitConfig + `startup` hostile-environment matrix |
| A04 | Process-owned loop and explicit joined Rust executors | Runtime source, concurrent await test and combined shutdown evidence | partial: separate supervisor and main loop |
| A05 | One coordinated signal owner | Disposable process INT/TERM tests; no competing interpreter handlers | partial: PyO3 disables signal handlers, returning CLI host |
| A06 | No hidden runtime executes SDK operations | Source audit plus instrumented original-turn/domain tests | partial: operation future runs in native invocation |
| A07 | Owned values only across thread/await boundary | Source review and lifecycle tests; no borrowed Python/SQLite guards | partial: owned JSON/native sender |
| A08 | Native authority checked at enqueue, execution, commit/output | Pending-operation cancellation and forced-fence tests | partial: native SDK checks retained; no Python fence test |
| A09 | Two embedded domains cannot exchange live contexts | Simultaneously live contexts in two nodes: cross-use refused | verified: reproduced live cross-domain read, then blocked API and native sender in `bridge` |
| A10 | Opaque invocation identity; metadata cannot choose authority | Cross-event/domain/generation/activation negative tests | partial: each native sender is tied to its driver |
| A11 | FIFO operations, one outstanding mutation per invocation | Concurrent mutation/read/cancel tests; explicit state machine | verified: native admission reservation rejects overlap; same-invocation read progresses during nested mutation; reservations release on drop |
| A12 | Mailbox pollable during nested calls; existing cycle behavior | Nested-call progress, cycle and cancellation tests | partial: driver now polls pending SDK futures and mailbox together; direct same-invocation progress proven; explicit cycle/fence matrix remains |
| A13 | Descriptor declares supported classes/events before admission | Cold validation and invalid registration tests | verified: cold descriptor async/constructor checks plus deployment hook rejecting absent classes, undeclared queue consumers and cron handlers before activation |
| A14 | Native factories never call/import Python | Source/caller audit plus blocked-interpreter factory test | partial: proxies allocated in Rust |
| A15 | Activation identity includes domain, generation and epoch | Retire/recreate and simultaneous same-name/domain tests | partial: unique proxy IDs and original Context |
| A16 | No interpreter-per-cell or hot Python replacement | Process-wide guard and attempted reload tests | partial: one runtime guard; reload behavior needs coverage |

## HTTP, codecs and failures (M1)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| H01 | Buffered bytes/JSON HTTP; ordered repeated headers | Byte and duplicate-header round trips through real ingress | verified: binary body and ordered repeated request/response header round trips through real ingress |
| H02 | Body/response/header/depth/value limits before admission/effects | Exact boundary and over-limit cases, released reservations | partial: body/value guards; header/input-depth coverage incomplete |
| H03 | Finite floats, integer range, keys, cycles and unsupported values | Codec boundary corpus and no mutation assertions | verified: real-node invalid-value corpus (nonfinite, int bounds, keys, unsupported, cycles, depth) has no mutation; valid boundaries round-trip |
| H04 | Typed validation, conflict, expired/ownership, overload, ambiguity | Public Python error tests and native cause classifications | partial: Conflict/SDKError only; distinctions collapsed |
| H05 | Cancellation stays cancellation; no blind retry | Cancel-before/after submission and ambiguous-result reconciliation | partial: task cancellation test; reconciliation missing |
| H06 | Sanitized, bounded failures; no secrets/paths/stack locals | Sentinel request/exception/path tests on public response and logs | partial: managed handler failures sanitized; process-log sentinel checks cover unraisable destructor and raw-thread exceptions, with counted runtime failure; startup/import and all failure paths still need final review |
| H07 | Concurrent Python work during host await | Two-handler synchronization with one blocked host operation | verified: `host_await_progress` barrier test runs another handler during pending SDK fetch |
| H08 | Native-extension representative execution and teardown | Pinned package matrix, real operations and joined report | partial: managed-runtime smoke now executes pinned Pydantic, NumPy, hashing and pure Python inspection; platform matrix remains open |

## Atomic storage, alarms and durability (M2)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| S01 | Bounded declarative atomic batch, no Python/await under transaction | Source audit, malformed batches, aggregate byte/count boundaries | partial: batch uses transaction_sync; budgets incomplete |
| S02 | Presence-aware null/missing checks and full rollback | Conflict/rollback tests asserting all mutations and alarm unchanged | verified: failed check retains all JSON state and previous alarm; presence/null/missing tests pass |
| S03 | No duplicate/conflicting keys or malformed alarms | Negative batch corpus at owning boundary | verified: real bridge corpus refuses duplicate deletes, put/delete overlap, invalid/reserved/oversized keys, malformed snapshots, unknown fields, invalid/conflicting alarms and over-budget batches; no partial writes/alarm changes; exact operation boundary commits |
| S04 | Conflicts with native Rust writers | Concurrent Python snapshot/native mutation/check refusal test | verified: same-cell Rust transaction commits while Python holds a snapshot; Python checked batch refuses and leaves the native value/other write intact with no partial Python mutation |
| S05 | Sync and response output proof retain native semantics | Controlled replication pause/failure, response withheld/refused | missing for Python adapter |
| S06 | Durable alarm mutation atomic with cell state | Crash/recovery and failed-check alarm tests | partial: real alarm executes |
| S07 | Duplicate alarms safe by persisted occurrence | Repeat same occurrence with idempotent durable effect | verified: sample persists an occurrence receipt atomically; bridge invokes the same occurrence twice and observes one increment |
| S08 | State persists over preserving restart | Real-node restart and packaged host restart | partial: existing harness and smoke |
| S09 | Lost committed response resolved with same receipt key | Disposable host reply loss followed by receipt reconciliation | verified in packaged local dev host: proxy discards proof-gated success, host restarts, same-key retry replays sealed result; later writes and key mismatch also checked |

## Failure, cleanup and combined reports (M3)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| L01 | Root cancellation revokes SDK admission immediately | Queued/late writes refused before execution and after fence | partial: expiry/abandoned-operation tests |
| L02 | Owned cleanup survives admission closure; no destructor waits on GIL | Retirement/closure race tests and cleanup counters | partial: polling registry; no explicit cleanup lane/report |
| L03 | Retired activation object cleanup is observed | Destruction counters/weakrefs under eviction, fence and redeploy | partial: weak-reference probe observes actual cell object collection after managed shutdown; independent eviction/redeploy cases remain |
| L04 | Close async generators and managed executor work | Async-generator finally + thread/executor join tests | verified for managed loop: async-generator finally observed; default-executor work joined; raw/native private pools remain application-owned |
| L05 | Combined report includes native report and all bridge/task/cleanup/budget work | Serialized combined report, incomplete cases, queued/running counts | partial: combined native outcomes, host failure source, Python cleanup counts and byte reservations serialize; complete operation/cleanup/metrics accounting remains |
| L06 | Correct shutdown phases with both runtimes alive | Ingress closure/drain/cancel/object cleanup and final-join assertions | partial: basic clean stop and errors |
| L07 | Noncooperative code cannot fabricate complete drain | Disposable asyncio-stubborn, CPU-loop and blocked-C-extension hosts | verified: stubborn asyncio reports incomplete; disposable infinite-Python and GIL-held native-call owners require external termination and cannot produce clean-drain reports |
| L08 | External watchdog supports hard termination/restart | Bounded child-host watchdog drill and recovered acknowledged state | partial: bounded external hard-stop/recovery drill implemented; deployment still must configure a production process supervisor and startup/health deadlines |
| L09 | Unclean owner death/takeover retains acknowledged state | Kill owner, remove local disk, recover from fleet, new write | verified in local development fleet: acknowledged write, hard owner death, complete owner disk deletion, bucket recovery and new write; quorum proof faults remain S05 |
| L10 | Stale Python task cannot mutate after ownership loss | Pause old handler, force fence/takeover, resume old handler, inspect state | verified: real node lease mismatch fences old domain; after lease expiry successor recovers acknowledged state; cancellation-suppressing old Python task resumes and gets ContextExpired, with no late mutation |
| L11 | Initialize once; never finalize while threads enter | Source audit, duplicate runtime refusal and error-path join tests | partial: initial guard and subprocess tests |
| L12 | Raw Python effects/heap/coroutines not claimed durable or isolated | API/docs audit and recovery examples with explicit persisted state | partial: limitations documented |

## Verified deployment and replacement (M4)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| P01 | Immutable source/registration descriptor bound to native artifact | Source/class/event digests and tamper tests | verified: build embeds exact source/registration/file inventory; typed per-executable inventory digest is rechecked before every factory/generation admission |
| P02 | Hash-locked platform wheels and native dependency closure | Lock artifact hashes, transitive inventory and offline install | partial: hash-locked platform wheels install offline; inventory covers bundled files; full OS/native dependency closure and artifact recovery remain open |
| P03 | Runtime identifies CPython/GIL/SOABI/platform and actual files | Inventory capture plus pre-code digest/ABI verification | verified for supported targets: CPython/GIL/SOABI/micro version/target, actual loaded libpython and exact file contents/executable modes; external OS closure tracked in P02 |
| P04 | Native package metadata inventories Python artifacts, format marker 1 | Protocol types, strict validation and old-node feature rejection | verified: typed native runtime references, format marker 1, embedded-python-v1 feature gate and executable/inventory pairing; malformed pairing tests added |
| P05 | Build/deploy/install stage, publish, recover and verify closure | Real bucket package tests, corrupt/missing artifact refusal | verified locally: build/stage, pre-selector verified bucket publication, corrupt/missing blob refusal and real restoration with original runtime absent |
| P06 | Verify runtime/source/dependencies before importing app or admission | Sentinel app not run for each invalid artifact/ABI/environment case | verified: inventory/ABI/source before initialization/import; process digest before application factory/generation; real installed-host and tamper drills |
| P07 | Explicit home, roots, site behavior, no ambient user/PYTHONPATH/CWD | Hostile startup environment and .pth/customize injection tests | verified: `startup` path/site/CWD/sysconfig override tests plus explicit environment view |
| P08 | No network install at startup or request; offline qualified environment | Network disabled/failing installer and clean-runtime deployment smoke | verified locally: hash-locked offline wheel install and archive-restored host launched with cleared environment; no runtime package installer |
| P09 | Wrong ABI/missing library/tampered source refuses startup | Disposable startup matrix with no admitted request or app side effect | partial: startup invalid ABI/home plus bound-inventory missing/changed source, changed extension and extra files refused; package/library loss matrix remains open |
| P10 | One immutable generation per process; ordinary native generations unchanged | In-process replacement refusal, native-only regression, rolling two binaries | partial: native regressions; Python replacement matrix missing |
| P11 | Old/new host handoff and runtime artifact recovery | Versioned process handoff, restored artifact installation, retained state | missing |
| P12 | Runnable example from fresh installation | Documented build/install/run/recovery test from clean environment | verified for local supported package: fresh-prefix archive restore, cleared-environment HTTP/native numerical work/receipt replay and complete preserving stop; platform matrix remains separate |

## Limits, metrics, SDK growth and qualification (M5)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| Q01 | Finite startup/request/body/value/depth/batch/mailbox/task/object/drain limits | Named limits, boundary tests, reservations released on every exit | partial: aggregate bridge bytes, managed tasks, objects and header count/bytes added; startup watchdog/complete reservation and exact-boundary coverage remain |
| Q02 | Bounded backpressure both ways and cleanup accounting | Saturation/cancellation/failure tests with zero leaked reservations | partial: RAII aggregate reservations cover starts, requests, results and completions; unit saturation/drop and joined zero-reservation reports pass; full stress matrix remains |
| Q03 | Queue, bridge, Python/Rust duration, copied bytes, allocations/RSS and cleanup metrics | Instrumented counters/histograms and live snapshot tests | partial: queued/peak/transferred bridge bytes, reservation count, object create/retire, task peak and budget counters recorded; interpreter-thread CPU, handler duration and process peak RSS/allocated-block snapshots added; queue/SDK histograms, allocation byte totals and live interpreter snapshots remain missing |
| Q04 | Python CPU attributed separately; native accounting gaps disclosed | Separate measurements and documented coverage | verified locally: managed-loop thread CPU measured separately from Rust; handler duration/count and memory-snapshot assertions pass; exclusions for native/executor pools documented; cross-platform qualification remains Q09/Q12 |
| Q05 | KV authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python, not only native SDK tests | partial: happy-path smoke |
| Q06 | Shared SQL authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python | partial: happy-path smoke |
| Q07 | Queue producer/consumer settlement, retry, cancel, replay and resource qualification | Adapter-specific independent failure matrix | partial: retry smoke |
| Q08 | Cron retry/no_retry, authority, cancellation and occurrence replay | Adapter-specific independent failure matrix | partial: real tick smoke |
| Q09 | Pure Python, PyO3 native extension and numerical library compatibility | Exact pinned versions/platforms; representative operations/concurrency/teardown | partial: macOS ARM64 CPython 3.14.5/3.14.6 and Linux x86_64 CPython 3.14.8 passed pinned Pydantic/NumPy/pure-library workloads; new archive path Linux CI still pending |
| Q10 | Native versus Python latency/throughput/memory/copy/recovery baselines | Reproducible harness and raw measured evidence, no invented target | missing |
| Q11 | Native-only dependency/build check and no JS all-features | Explicit CI check, local graph/build evidence | partial: local no-embed and JS graph checks |
| Q12 | Full affected test/lint/format/layout/doc qualification; preserve failures | Exact source/toolchain commands and passing scope-specific results | partial: foundation full workspace tests and complete Linux SDK CI passed; new archive/protocol/deployment-hook changes need final affected checks and fresh CI |
| Q13 | Public facade type annotations and accepted implementation decisions | Typed Python API, accepted ADR, build/bootstrap/bridge decision record | partial: public facade type annotations added and immutable CellIdentity; accepted design decision records still need final consistency review |
| Q14 | Current docs describe executable APIs and remaining gates accurately | Plan sketches updated to actual contracts; audit has no unstated exception | partial: obsolete proposal-only and API sketches remain |

## Current work

1. Finish native durability pause/failure qualification through the Python
   adapter, independent KV/SQL/queue/cron failure and backpressure matrices.
2. Qualify two different packaged application versions through process handoff
   and verify external native-library/OS prerequisites.
3. Complete typed errors, cleanup/resource accounting, bridge/SDK timing and
   reproducible Rust/Python latency/throughput/memory/recovery measurements.
4. Run fresh complete CI after the current hardening changes, then reconcile
   every remaining partial/missing audit row and executable documentation.

No scope reduction or production-readiness claim is implied by this ordering.

## Current evidence added after the initial audit

- `cargo test -p cellar-python --features embed --test startup --test bridge --locked`
  passed after the bootstrap and scope fixes. The bootstrap harness runs isolated
  child processes with hostile `PYTHONHOME`, `PYTHONPATH`, user site/startup hooks,
  `.pth`, CWD modules and `_PYTHON_SYSCONFIGDATA_*`. It checks explicit application
  environment values, redacted debug output and unchanged Rust process environment.
- The new cross-domain test failed before the fix with a successful read of the
  other node's `DOMAIN_MARKER`. It now checks refusal at both `Context._call` and
  the native sender while the original context remains live. This is an additional
  caller-scope check; original Rust invocation/domain/commit checks remain intact.
- The bootstrap test also reproduced execution of environment-selected sysconfig
  code despite CPython isolation. ABI preflight now uses builtin runtime facts,
  and Python's initial environment view is explicitly supplied before app imports.
- The host-await barrier test checks that another Python task responds while a
  native SDK call is pending. The private-exception test checks that request and
  internal-path sentinels do not appear in the public failure.
- `PYO3_PYTHON=/nonexistent/cellar-no-python cargo check -p cellar-python --no-default-features --locked`
  passed, showing that the disabled adapter does not probe or require an interpreter.

These results close specific startup and caller-authority gaps. They do not close
the packaging, complete combined reporting, fault-drill, SDK qualification,
observability or performance requirements above. The goal remains active.

Final checks at this checkpoint:

- `cargo clippy -p cellar-python --all-targets --all-features --locked -- -D warnings` passed.
- `cargo test -p cellar-python --all-features --locked` passed, including both
  real-node bridge and isolated-bootstrap subprocess harnesses.
- Packaged Pydantic smoke with `--with-cron` passed after the isolated bootstrap,
  explicit environment view and native sender scope guard were integrated.
- A native-only CI build using a deliberately nonexistent `PYO3_PYTHON` is now
  explicit in the SDK workflow. Linux execution remains unverified locally.

Next executable work is M0's per-invocation mutation admission, mailbox progress
while a nested operation is pending, and pre-admission class/event registration.
M4 must then add a complete typed artifact inventory and verified package
build/publish/install path; the current layout preflight is not that inventory.

## Provisioning, lifecycle and failure qualification checkpoint

Closed direct runtime gaps:

- Native FIFO operation start now polls the invocation mailbox while nested calls
  await. A native RAII mutation reservation refuses overlapping mutations before
  queueing, while known reads can progress. Driver cancellation releases futures
  and reservations without crossing execution domains.
- Root/cell event declarations are explicit. Cold validation rejects absent or
  synchronous handlers, bad constructors and duplicate registrations before the
  Rust supervisor starts. Public Python APIs now carry type annotations and
  immutable cell identity metadata.
- Async-generator finalizers execute during bounded cleanup. Background Python
  failures are sanitized. `Incomplete` retains a serialized combined report and
  the original Rust supervisor error as its source. `Host::run_until_observed`
  supplies actual joined native outcomes; every owned library node must be
  recorded through `Stop::record_node`.
- Aggregate native bridge byte reservations, task/object limits and accounting
  were added. The report exposes peak/queued/transferred bytes, pending
  reservations, task peak, budget exhaustion and object create/retire counts.
  These do not measure or cap arbitrary Python heap or raw native threads.
- Both CPU-loop and GIL-held C-call owners were killed externally after an
  acknowledged write. Tests delete all owner-local databases and recover from
  the development fleet bucket, then verify the next write preserves that state.
- The example's durable alarm records an occurrence receipt in the same checked
  transaction as the increment; duplicate delivery is directly tested.

Verified provisioned environment:

- Added a generated hash lock and independent CPython 3.14.5 environment. Platform
  wheels were downloaded once, then installed with `--no-index --require-hashes
  --only-binary :all:`. The example executes Pydantic 2.12.3 / pydantic-core 2.41.4,
  NumPy 2.3.4 numerical work, native hashing and pure Python typing inspection.
- The build embeds an inventory covering 3,289 local runtime/package files and
  source/event registrations. Pre-initialization checks identify the actual
  loaded libpython, verify target/SOABI/micro version, compare explicit import
  paths and hash the full provisioned tree. The source/distribution descriptor
  must match the compiled application. No external mutable manifest can replace
  the embedded expected inventory.
- The mutation drill passed for changed package source, altered native extension,
  missing file, extra file, and restoration to the original valid environment.
  The valid binary completed `--help` before and after the mutations.
- The inventory initially refused startup after build probes generated bytecode
  after capture. That failure was preserved and fixed with `-B` on the adapter
  probe and disabled build-time bytecode writes, followed by recapture/rebuild.
- The inventory-backed packaged smoke passed with real cron, Pydantic, NumPy,
  storage, alarms, KV, SQL, queue retry, preserving restart and clean joined stop.

Current checks before final formatting: `cargo test -p cellar-python
--all-features --locked`, adapter all-target Clippy with warnings denied, and
`cargo test -p cellar --test native_daemon --locked` passed. The bridge harness
now also checks repeated HTTP headers, header budgets, codec boundaries, task
saturation, alarm rollback and duplicate occurrences. CI uses the hash lock and
captures a Linux inventory; its actual Linux execution remains outstanding.

Remaining work is still the full plan: native protocol metadata and runtime
archive build/publish/install/recovery; OS/native dependency attestation; explicit
forced-fence, native-writer and quorum proof/lost-response cases; independent KV,
SQL, queue and cron failure qualification; complete telemetry and measured
Rust/Python baselines; rolling two-version process qualification; full workspace
and multi-platform CI. No production-readiness claim or goal completion follows
from this checkpoint.

Additional receipt drill: the packaged example accepts an idempotency key, stores
its request amount and sealed response in the same checked transaction as the
increment, and rejects key reuse for a different amount. The HTTP fault proxy
reads the successful native proof-gated response and discards the downstream
connection without sending response bytes. After restarting the host, the same
key/amount returns the original response without a second increment; it still
returns that original result after another keyed write. A different amount for
that key returns 409 and leaves state unchanged. This packaged local-dev drill
passed with the verified independent runtime and real cron.

The example can emit its combined report under `CELLAR_SHUTDOWN_REPORT_JSON=1`;
the smoke asserts native phase completion and zero remaining managed work
and bridge reservations. This stricter inventory-backed smoke passed with real cron. `PythonApplication::bridge_report` exposes a live atomic
snapshot for host-owned metrics export. The final repository-wide run remains in progress. A current native-only check with a nonexistent Python interpreter, adapter all-feature tests and all-target Clippy passed after the task-admission, report and loader-identity refinements.

## Native runtime distribution checkpoint

The runtime/package provisioning gap now has an executable native-package path:

- Added shared typed inventories and per-executable `PythonEnvironmentRef`
  declarations, `embedded-python-v1` gating, bounded format-1 archives and
  executable/inventory/archive digest pairing. Native-only code still has no
  interpreter dependency; the new tar reader is a deployment-tool dependency.
- `PythonProject` is an explicit native project input. Build validates the
  provisioned tree and archive before invoking Cargo, selects its host-build
  interpreter, embeds the inventory, sets loader search paths for the final
  binary, stages its archive and recomputes the native deployment identity.
  Ordinary native projects ignore ambient inventory flags.
- Publication validates all staged blobs before any selector mutation. The
  installer uses bounded bucket reads, checks digests/types/paths/symlinks and
  writes only verified content blobs into a private staging tree. It never calls
  tar unpack. Publication of the prefix atomically refuses existing destinations,
  including empty competing directories. Matching prefixes are read-only verified
  and reused; changed prefixes are refused before executable publication.
- `Host::runtime_inventory` and `CompiledApplications::runtime_inventory` bind
  the process identity to graph/package admission, including later generations.
  Missing or changed identities refuse before application factories. Bound
  package roots are compiled defaults, so the installed host needs no Python
  home/path/site environment flags.
- Four archive/file/bucket tests passed, covering valid restore, modes/links,
  reuse, wrong target/prefix, missing/duplicate/unlisted/corrupt blobs, escaping
  symlinks, tar symlink entries, staged corruption before selector mutation,
  remote corruption before executable publication and refusal to overwrite.
  A separate native admission test proves mismatched identities never enter a
  factory and exact identities do. Feature/pairing integration cases were added.
- The real `python-package-qualification` executable passed. It published the
  actual built example, moved the entire original runtime prefix aside, restored
  it from the private bucket, launched the installed host with `env_clear`, ran
  native NumPy work and receipt replay, proved a complete preserving shutdown,
  checked the installed package in another clean process and restored the
  original tree. No ambient interpreter or site-packages flags were supplied.

Qualification preserved two real failed attempts: the first used an invalid
private-store bucket name; the second requested single-node handoff without a
successor and correctly produced an incomplete native/Python report. The drill
now uses the native development supervisor's preserving-stop endpoint for its
single-node topology, while retaining strict complete-report assertions. No
assertion was weakened or incomplete handoff relabelled as a graceful drain.

Constructor/event validation refinements: async constructors are refused during
cold registration, successful object creation is counted only after construction
succeeds, and constructor failure/recovery is tested without fabricated cleanup
counts. The first synthetic queue declaration omitted required native settings;
that test fixture was corrected. The constructor test also learned the native
nested-call contract: a failed target constructor yields SDK `HostError`, rather
than an HTTP response value. The current adapter test suite passes those cases.

Foundation commit `fc45907` passed the full local workspace unit/integration
suite (including all 87 native-node tests) and both complete Linux SDK workflow
runs, 39385543593 and 39385478532. Linux used CPython 3.14.8 with Pydantic 2.12.3 /
pydantic-core 2.41.4 and NumPy 2.3.4. These results precede the later archive
changes; new package/protocol/hook checks and fresh Linux CI are still required.

The full goal remains active. Outstanding gates include OS/transitive external
library attestation, rolling different-version process handoff, explicit
Python/native writer and stale-Python-after-fence cases, quorum proof pause/fail
qualification, binding-specific fault/replay/backpressure matrices, complete
cleanup/error telemetry and measured Rust/Python latency/throughput/memory/copy/
recovery baselines. The archive milestone does not complete the full plan.

Final archive checkpoint corrections and checks:

- The new deployment validator initially mistook framework-owned SQL/KV/queue
  classes for missing Python factories. It now uses Cellar's existing reserved
  class predicate; regression coverage permits those native classes while still
  refusing a missing Python class. The real restored-host and adapter tests
  caught this before publication of the follow-up commit.
- After correction, adapter all-feature tests and affected Core/adapter
  all-target/all-feature Clippy with `-D warnings` passed. Native artifact
  integration tests (8 cases), all four archive/bucket tests and the separate
  pre-factory runtime identity test passed. The fresh real-package recovery
  drill was rerun successfully against the final validation path.
- Native-only compilation with an invalid interpreter path, documentation
  integrity and the 415-package native dependency graph passed. The new Linux
  workflow uses a private independent runtime and removes/restores it in the
  same real-package qualification drill; that follow-up CI has not run yet.

Final prepared-project package smoke passed after the reserved-class correction, including real cron, preserving restart, lost-response reconciliation and strict combined native/Python report assertions. The fresh removal/restoration package drill also passed against that same validation path.


## Ownership, validation and reporting hardening

- Added a native/Python same-cell conflict harness. Rust commits a transaction
  after Python's snapshot read; Python's stale checked transaction fails, with
  the native value and native-only key retained and no Python-only key written.
- Added a disposable real ownership-loss drill. A live Python task pauses after
  reading acknowledged state and deliberately suppresses cancellation. Replacing
  the published node-lease generation produces `NodeLeaseMismatch`; after its
  live lease expires, a successor recovers the state. Resuming the old task
  refuses its write with `ContextExpired`. Native fencing remains an incomplete
  outcome; managed tasks, objects and reservations still drain to zero.
- The negative batch corpus and exact four-operation boundary exposed a real
  actor panic for a valid far-future alarm. Production timers now use bounded
  checkpoints without firing before their original deadlines; timer identity
  and displaced-arm refusal remain intact. A focused maximum-deadline regression
  and the real Python alarm batch both pass.
- Linux CI runs 39396001527 and 39395996175 failed before package qualification:
  the new archive unit fixture called the one-time process initializer, colliding
  with the native-worker fixture. It now owns and joins a separate execution
  domain. The full local Core library suite passed all 364 tests after correction;
  the CI failures are not relabelled as passing runs.
- Installed sanitized unraisable/thread exception hooks before importing app
  code. Disposable destructor and raw-thread failures check a private sentinel
  against captured process output, assert counted failure and retain incomplete
  reports. These checks pass in the adapter harness.
- Added managed-interpreter thread CPU/wall time, handler counts/durations,
  process peak RSS and CPython allocated-block snapshots. Thread CPU includes
  native code on that thread and excludes executor/native pools; RSS covers the
  whole process, and allocated blocks are neither byte totals nor native heap.
  Concurrent handler durations include awaits and cannot be summed as elapsed
  process time. These distinctions are documented; complete telemetry and
  measured native/Python comparison remain open.

Final hardening validation passed locally:

- All 364 Core library tests, then all 87 native-node integration cases serially
  (657.32 seconds), including lease/fence, proof, owner recovery and drain cases.
- Adapter all-feature unit/bridge/bootstrap/doctest targets, including fresh
  CPU/duration/RSS/count assertions and weak-reference cell collection probe.
- Core and adapter all-target/all-feature Clippy with warnings denied.
- A freshly built provisioned package, real original-runtime removal/archive
  restoration, cleared-environment native numerical/receipt work and complete
  preserving stop. Packaged smoke passed with actual cron, all supported
  bindings, preserving restart and lost-response reconciliation.
- Native-only adapter compilation with a deliberately nonexistent interpreter
  passed after the reporting changes. Formatting and layout (585 Rust files),
  documentation integrity, the 415-package no-JavaScript dependency graph and
  diff whitespace check also passed.

The complete workspace CI must be rerun after the failed prior archive runs.
The full goal remains active, including every unresolved row above.
