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
| A01 | Separate optional adapter; PyO3 outside logic/core | Dependency graph and native-only build without Python | verified locally: optional embed adapter, workspace-centralized pinned dependencies, native-only build with a nonexistent interpreter and all-feature no-JavaScript graph; logic/core contain no Python engine |
| A02 | Pin CPython 3.14, GIL mode and compatible bridge | Build-time ABI guard plus runtime preflight, pinned Cargo graph | verified: CPython/GIL/shared-library build guards, builtin ABI preflight, invalid-ABI startup refusal and exact PyO3/build-config 0.30.1 Cargo lock |
| A03 | Initialize before node; explicit home/import roots/site/env | Isolated configuration before Python code/imports; hostile environment tests | verified: explicit PyInitConfig + `startup` hostile-environment matrix |
| A04 | Process-owned loop and explicit joined Rust executors | Runtime source, concurrent await test and combined shutdown evidence | verified locally: process main-thread loop, explicit joined Rust supervisor, concurrent host-await progress and current combined two-/three-node stop reports |
| A05 | One coordinated signal owner | Disposable process INT/TERM tests; no competing interpreter handlers | verified locally: startup preserves existing native handlers and removes CPython implicit INT handling; real TERM/INT handoff with two compiled Python processes yields complete joined reports |
| A06 | No hidden runtime executes SDK operations | Source audit plus instrumented original-turn/domain tests | verified locally: operation futures remain inside the original native application driver; live cross-domain binding tests refuse handle exchange; driver polls nested calls without a separate SDK executor |
| A07 | Owned values only across thread/await boundary | Source review and lifecycle tests; no borrowed Python/SQLite guards | verified locally: owned strings, JSON, revocation flags, reservations and supported owned Py handles cross the bridge; native operations retain no Python token/borrow or SQLite guard across awaits; current lifecycle suite passes |
| A08 | Native authority checked at enqueue, execution, commit/output | Pending-operation cancellation and forced-fence tests | partial: native SDK checks retained; actual lease-fence/resumed-Python refusal is verified in L10; full queued/execution/commit race matrix remains |
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
| H02 | Body/response/header/depth/value limits before admission/effects | Exact boundary and over-limit cases, released reservations | partial: native and facade depth guards agree at 64; exact native request body/header-count/header-byte limits and real-node exact/over-depth refusal pass; complete response/value/admission and reservation boundary coverage remains |
| H03 | Finite floats, integer range, keys, cycles and unsupported values | Codec boundary corpus and no mutation assertions | verified: real-node invalid-value corpus (nonfinite, int bounds, keys, unsupported, cycles, depth) has no mutation; valid boundaries round-trip |
| H04 | Typed validation, conflict, expired/ownership, overload, ambiguity | Public Python error tests and native cause classifications | partial: public ValidationError/Overloaded/Ambiguous/OwnershipLost classes, explicit outcomes and fixed sanitized messages; native typed routing verdicts preserved; real protocol/codec/batch refusals and ambiguous constructor/absent-binding tests pass; some bindings flatten native causes internally, and live routing-verdict fault coverage remains |
| H05 | Cancellation stays cancellation; no blind retry | Cancel-before/after submission and ambiguous-result reconciliation | partial: task cancellation plus packaged lost-response receipt reconciliation (S09) pass; independent pending-operation/binding cancellation and ambiguity matrices remain |
| H06 | Sanitized, bounded failures; no secrets/paths/stack locals | Sentinel request/exception/path tests on public response and logs | partial: managed handler, destructor and raw-thread failures sanitized and counted; import/registration/descriptor errors expose fixed stage messages without a Python exception source; subprocess tests refuse exception/path sentinels and arbitrary exception `__str__` execution; initialization/loader and complete error-path review remain |
| H07 | Concurrent Python work during host await | Two-handler synchronization with one blocked host operation | verified: `host_await_progress` barrier test runs another handler during pending SDK fetch |
| H08 | Native-extension representative execution and teardown | Pinned package matrix, real operations and joined report | partial: managed-runtime smoke now executes pinned Pydantic, NumPy, hashing and pure Python inspection; platform matrix remains open |

## Atomic storage, alarms and durability (M2)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| S01 | Bounded declarative atomic batch, no Python/await under transaction | Source audit, malformed batches, aggregate byte/count boundaries | partial: batch uses transaction_sync; budgets incomplete |
| S02 | Presence-aware null/missing checks and full rollback | Conflict/rollback tests asserting all mutations and alarm unchanged | verified: failed check retains all JSON state and previous alarm; presence/null/missing tests pass |
| S03 | No duplicate/conflicting keys or malformed alarms | Negative batch corpus at owning boundary | verified: real bridge corpus refuses duplicate deletes, put/delete overlap, invalid/reserved/oversized keys, malformed snapshots, unknown fields, invalid/conflicting alarms and over-budget batches; no partial writes/alarm changes; exact operation boundary commits |
| S04 | Conflicts with native Rust writers | Concurrent Python snapshot/native mutation/check refusal test | verified: same-cell Rust transaction commits while Python holds a snapshot; Python checked batch refuses and leaves the native value/other write intact with no partial Python mutation |
| S05 | Sync and response output proof retain native semantics | Controlled replication pause/failure, response withheld/refused | verified locally: real authenticated three-node fleet plus bucket; sync and final output remain pending while both proof paths are held; actual follower acknowledgements release success with bucket still held; both paths failing yields HTTP error and exactly one handler attempt, never a rollback claim |
| S06 | Durable alarm mutation atomic with cell state | Crash/recovery and failed-check alarm tests | partial: real alarm executes |
| S07 | Duplicate alarms safe by persisted occurrence | Repeat same occurrence with idempotent durable effect | verified: sample persists an occurrence receipt atomically; bridge invokes the same occurrence twice and observes one increment |
| S08 | State persists over preserving restart | Real-node restart and packaged host restart | verified locally: real Python cell restart and remote handoff recover acknowledged state; provisioned host restart and changed-source process upgrade preserve exact receipts; independent queue/cron fixtures also retain SQL effects, attempts, settled broker state and cron history |
| S09 | Lost committed response resolved with same receipt key | Disposable host reply loss followed by receipt reconciliation | verified locally with direct wire comparison: proxy discards proof-gated success; rendered application status/body/header fields persist atomically with the effect and replay exactly before/after restart and after later writes; different-amount reuse refuses; changed-source process upgrade also compares exact application response fields |

## Failure, cleanup and combined reports (M3)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| L01 | Root cancellation revokes SDK admission immediately | Queued/late writes refused before execution and after fence | partial: expiry/abandoned-operation tests |
| L02 | Owned cleanup survives admission closure; no destructor waits on GIL | Retirement/closure race tests and cleanup counters | partial: polling registry; no explicit cleanup lane/report |
| L03 | Retired activation object cleanup is observed | Destruction counters/weakrefs under eviction, fence and redeploy | partial: weak-reference probe observes actual cell object collection after managed shutdown; independent eviction/redeploy cases remain |
| L04 | Close async generators and managed executor work | Async-generator finally + thread/executor join tests | verified for managed loop: async-generator finally observed; default-executor work joined; raw/native private pools remain application-owned |
| L05 | Combined report includes native report and all bridge/task/cleanup/budget work | Serialized combined report, incomplete cases, queued/running counts | verified locally for runtime-owned work and explicitly recorded native nodes: combined outcomes and host failure source, task/invocation/object/retirement gauges, bridge reservations and admitted/terminal operation conservation; live pending SDK work, all native-future/reply drop stages and incomplete lifecycle cases are directly tested; private application/library pools remain application-owned |
| L06 | Correct shutdown phases with both runtimes alive | Ingress closure/drain/cancel/object cleanup and final-join assertions | partial: basic clean stop and errors |
| L07 | Noncooperative code cannot fabricate complete drain | Disposable asyncio-stubborn, CPU-loop and blocked-C-extension hosts | verified: stubborn asyncio reports incomplete; disposable infinite-Python and GIL-held native-call owners require external termination and cannot produce clean-drain reports |
| L08 | External watchdog supports hard termination/restart | Bounded child-host watchdog drill and recovered acknowledged state | partial: bounded external hard-stop/recovery drill implemented; deployment still must configure a production process supervisor and startup/health deadlines |
| L09 | Unclean owner death/takeover retains acknowledged state | Kill owner, remove local disk, recover from fleet, new write | verified in local development fleet: acknowledged write, hard owner death, complete owner disk deletion, bucket recovery and new write; independent quorum proof faults are recorded in S05 |
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
| P10 | One immutable generation per process; ordinary native generations unchanged | In-process replacement refusal, native-only regression, rolling two binaries | verified locally: old running process observes/refuses a new deployment while remaining live; wrong executable rejects new package; changed Python code executes only in the replacement process; native generation regressions passed previously |
| P11 | Old/new host handoff and runtime artifact recovery | Versioned process handoff, restored artifact installation, retained state | verified locally: original runtime removed/restored from bucket, then distinct binaries, application versions and source inventories run in separate processes; TERM/INT handoff preserves state and exact receipts and executes changed Python cell code; both native/Python reports complete |
| P12 | Runnable example from fresh installation | Documented build/install/run/recovery test from clean environment | verified for local supported package: fresh-prefix archive restore, cleared-environment HTTP/native numerical work/receipt replay and complete preserving stop; platform matrix remains separate |

## Limits, metrics, SDK growth and qualification (M5)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| Q01 | Finite startup/request/body/value/depth/batch/mailbox/task/object/drain limits | Named limits, boundary tests, reservations released on every exit | partial: aggregate bridge bytes, managed tasks, objects and header count/bytes added; startup watchdog/complete reservation and exact-boundary coverage remain |
| Q02 | Bounded backpressure both ways and cleanup accounting | Saturation/cancellation/failure tests with zero leaked reservations | partial: RAII aggregate reservations cover starts, requests, results and completions; unit saturation/drop and joined zero-reservation reports pass; full stress matrix remains |
| Q03 | Queue, bridge, Python/Rust duration, copied bytes, allocations/RSS and cleanup metrics | Instrumented counters/histograms and live snapshot tests | verified locally: bounded queue/SDK/reply/round-trip histograms, explicit UTF-8 boundary-copy counters distinct from reservations, lifecycle conservation/gauges, live age-stamped interpreter/work snapshots, loop scheduling delay, optional traced allocation bytes and pre-interpreter/post-join RSS/jemalloc/libc samples; scope/exclusions documented; adapter matrix and pinned package smoke pass; cross-platform and performance baselines remain Q09/Q10/Q12 |
| Q04 | Python CPU attributed separately; native accounting gaps disclosed | Separate measurements and documented coverage | verified locally: managed-loop thread CPU measured separately from Rust; handler duration/count and memory-snapshot assertions pass; exclusions for native/executor pools documented; cross-platform qualification remains Q09/Q12 |
| Q05 | KV authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python, not only native SDK tests | partial: real KV writes/reads across two nodes; retained live/expired handles refuse reads/writes and leave each namespace intact; cancellation, independent backpressure/replay and restart matrices remain |
| Q06 | Shared SQL authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python | partial: actual shared-SQL DDL/DML/query on two nodes; live/expired retained handles refuse inserts, both databases retain only their own row; cancellation, independent backpressure/replay and restart matrices remain |
| Q07 | Queue producer/consumer settlement, retry, cancel, replay and resource qualification | Adapter-specific independent failure matrix | partial: real broker matrix verifies default/explicit ack, handler errors after ack, first conflicting ack/retry decision, same-ID redelivery, retry exhaustion, timeout/cancel expiry, retained consumer-handle authority, producer overlap backpressure, idempotent SQL effects, empty persisted backlog and zero managed work over restart; independent in-flight producer failure/cancellation reconciliation remains |
| Q08 | Cron retry/no_retry, authority, cancellation and occurrence replay | Adapter-specific independent failure matrix | partial: real native alarm/history matrix proves same-occurrence retry, explicit no_retry after error, refusal of live-cross/expired controllers, idempotent SQL effects and exact execution-history/active-alarm persistence over restart; native timeout/cancellation and replay after unclean interruption remain |
| Q09 | Pure Python, PyO3 native extension and numerical library compatibility | Exact pinned versions/platforms; representative operations/concurrency/teardown | partial: macOS ARM64 CPython 3.14.5/3.14.6 and Linux x86_64 CPython 3.14.8 passed pinned Pydantic/NumPy/pure-library workloads; verified archive restoration and changed-source TERM/INT handoff also pass on Linux x86_64 CPython 3.14.5 at fe6d2f8; newer observation/receipt source still needs fresh Linux execution |
| Q10 | Native versus Python latency/throughput/memory/copy/recovery baselines | Reproducible harness and raw measured evidence, no invented target | missing |
| Q11 | Native-only dependency/build check and no JS all-features | Explicit CI check, local graph/build evidence | verified for dependency/build separation: explicit Linux CI native-only check with nonexistent interpreter and all-feature no-JavaScript graph passed on fe6d2f8; current source retains the same feature/dependency graph, with local no-embed and graph checks |
| Q12 | Full affected test/lint/format/layout/doc qualification; preserve failures | Exact source/toolchain commands and passing scope-specific results | partial: both complete Linux SDK workflows on fe6d2f8 passed tests, doctests, Clippy, package restoration/rolling and native examples; current observation/receipt follow-ups have local checks but require fresh complete CI |
| Q13 | Public facade type annotations and accepted implementation decisions | Typed Python API, accepted ADR, build/bootstrap/bridge decision record | verified: public facade annotations and frozen CellIdentity; accepted ADR 004 records bootstrap, bridge, signals, immutable packaging/replacement, cleanup, compatibility and qualification boundaries against implemented contracts |
| Q14 | Current docs describe executable APIs and remaining gates accurately | Plan sketches updated to actual contracts; audit has no unstated exception | verified for current documented surface: main plan, adapter/example/environment guides describe actual facade, source layout, accepted ADR, provisioning and conservative failure outcomes; chronological audit history is explicitly distinguished from current tables; remaining gates are stated |

## Current work

1. Finish independent KV/SQL/queue/cron failure, replay and backpressure matrices.
   Native durability pause/failure tests now pass locally through Python.
2. Verify external native-library/OS prerequisites and complete the platform
   matrix. Two genuinely different source packages now pass process handoff locally.
3. Complete remaining typed-error/cleanup race qualification and reproducible
   Rust/Python latency/throughput/memory/recovery measurements. Runtime-owned
   operation accounting, latency histograms and live interpreter observations
   now have direct local evidence.
4. Run fresh complete CI after the current hardening changes, then reconcile
   every remaining partial/missing audit row and executable documentation.

No scope reduction or production-readiness claim is implied by this ordering.

## Current evidence added after the initial audit

The following checkpoints are chronological history. Their then-current next
steps and missing items are retained to explain failures and subsequent fixes;
the requirement tables and Current work list above are the current status.

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


## Source upgrade and signal ownership qualification

A real process-upgrade drill now accompanies the existing archive-removal test.
Its replacement uses different compiled application/executable/inventory identities
and changed Python cell code. The generator creates a private Rust application,
copies the repository lock and resolves the extra root package offline while
refusing any new dependency versions. It captures and bundles the changed source;
repository sources and provisioned runtime files remain unchanged.

Each TERM/INT case uses a separate private development fleet. The old process
observes the new selector, refuses in-process adoption and keeps serving; its
executable also refuses the replacement package, while the new executable accepts
it. New ingress replays the receipt while the old owner still holds it. After
handoff, a header emitted by the changed Python cell handler proves replacement
code executed. State remains at the acknowledged count, a new command advances
it, and the old receipt still returns the exact pre-upgrade result. Both hosts
must emit complete native/Python reports and zero managed work/reservations.

The initial TERM case passed; INT produced a fully complete native handoff but a
Python `serve_interrupt` failure. Adding a fixed, sanitized failure-stage field
preserved the diagnosis without exception text. Inspection of CPython 3.14.5's
`signal_get_set_handlers` showed that importing `_signal` installs the default INT
handler even with initialization handlers disabled. asyncio imports that module
before the Rust host starts; Tokio subsequently chains that previous handler.
Bootstrap now imports signal first and removes only that implicit Python handler,
preserving an already installed native host handler. Both bootstrap modes pass.
The fixed two-version TERM/INT drill passed; the stronger changed-source variant
also passed, including new handler execution and exact receipt replay. No shutdown
assertion was weakened or Python failure relabelled as a complete drain.

A fresh run of the final generator command and native build passed without manual
project edits. Adapter all-feature tests passed after the signal fix. Strict
Core/adapter all-target/all-feature Clippy passed after correcting two redundant
borrows in the qualification helper. CI now runs the changed-source rolling path;
its actual Linux result remains required. Current CI at commit e95bdf2 predates
these signal/source-upgrade changes and cannot qualify them.

The full objective remains active. Quorum fault injection, independent binding
failure/replay/backpressure matrices, external native-library attestation,
remaining error/cleanup/observability details and measured native/Python baselines
remain open; the source-upgrade gate does not replace those requirements.


The accepted integration decision is now [ADR 004](../docs/adr/004-embedded-python.md),
linked from ADR 001 and the regenerated documentation catalog/explorer. It records
actual bootstrap/bridge/packaging choices while retaining all outstanding release
gates. Documentation integrity passed for 37 documents and 641 local references.
The plan's handler sketch also had an incorrect synchronous `request.json()` call
from the previous pass; it is corrected to the facade's actual awaited method.
Formatting/layout (587 Rust files), native-only compilation with an invalid
interpreter, the 415-package native dependency graph and whitespace checks passed.


## Linux qualification failures and loader correction

The e95bdf2 workflows completed with different failures, preserved as evidence:

- Run 39400143067 passed 84/87 native-node cases. Catalog inventory handoff,
  progressing drain-token renewal and predecessor recovery hit one-second lease
  expiry after slow/ambiguous store operations. Nodes fenced and recovery refused
  unproven witness state. These failures remain under investigation; assertions
  and lease deadlines have not been relaxed. The local prior run passed all 87.
- Run 39400137809 passed the complete test/lint step (workspace unit/integration,
  doctests and strict Clippy), then failed the independent CPython 3.14.5 package
  smoke: the loaded libpython was outside the verified inventory. Verification
  correctly refused startup rather than accepting the ambient interpreter.

GNU loader search places LD_LIBRARY_PATH ahead of DT_RUNPATH. Native Python
builds now request DT_RPATH with --disable-new-dtags for GNU Linux, so the
inventoried libpython directories precede an ambient same-SONAME installation.
The runtime's actual loaded-library identity/hash checks remain intact. Linux
paths containing loader separators/dynamic tokens are refused explicitly.

A disposable Debian GNU-loader probe reproduced replacement under RUNPATH and
passed under the new RPATH arguments, including spaces/commas in the library
directory. An initial Docker probe could not see a host bind mount; the corrected
probe transferred only its owned synthetic source via stdin and passed. A Linux
Rust regression uses the actual argument builder with two distinct same-SONAME
libraries; its execution remains pending fresh Linux CI. The four existing
archive/bucket tests, affected strict Clippy and format/layout checks passed
locally after the argument-builder change (588 Rust files). This is not yet a
claim that the full Linux package workflow passes.

## Native proof faults, binding authority and typed failures (2026-10-20)

The current `cargo test -p cellar-python --all-features --locked` passes the
complete adapter suite, including the following new disposable process modes:

- `--runtime-durability` starts three real authenticated fleet nodes, waits for
  actual append acknowledgements from two distinct followers, and gates peer
  append/data bucket writes independently. Explicit Python sync and ordinary
  final output both remain pending with both paths held. Opening peer append
  traffic releases success while bucket writes stay held. Failing both paths
  returns an HTTP error, runs the failing Python handler once, and preserves
  earlier acknowledged state after access is restored. Failed proof is explicitly
  allowed to leave its one local effect; the test does not infer rollback.
  Lease metadata remains available to isolate data proof faults. Multipart-start
  admission is gated; this small-write test does not qualify faults during
  individual multipart parts or a whole remote object-store outage.
- `--runtime-bindings` uses actual KV/shared-SQL backends on two nodes and a
  configured queue producer. Retained handles cannot cross a live invocation or
  submit after expiry. Both namespaces/databases retain their original marker,
  no forbidden write appears, and the combined report records two complete
  native outcomes. This is authority evidence, not full binding qualification.
- Protocol/codec/batch validation yields `ValidationError/not_committed`; bridge
  operation capacity and concurrent-mutation refusal yield
  `Overloaded/not_committed`. Failed submitted nested calls yield
  `Ambiguous/unknown`, including constructor failure and absent binding. Fixed
  error codes replace backend text before Python receives an error. A unit
  corpus preserves typed native capacity, fence and durability verdicts through
  contextual error chains; direct live routing-fault coverage is still open.

The production change does not add retries or weaken output proof. Generic
native binding failures may have lost their typed cause inside the native SDK;
the adapter preserves uncertainty instead of parsing their messages into a
stronger outcome. Cancellation remains Python cancellation.

Local checks for this checkpoint (Rust 1.98.1, macOS ARM64, host CPython 3.14.6):

- `cargo test -p cellar-python --all-features --locked` passed, including the
  full bridge/startup subprocess suites and typed failure corpus.
- `cargo clippy -p cellar-python --all-targets --all-features --locked -- -D warnings` passed.
- `PYO3_PYTHON=/nonexistent/cellar-native-only cargo check -p cellar-python --no-default-features --lib --locked` passed.
- `cargo format --check`, `cargo check-layout`,
  `python3 tools/check_native_dependencies.py`, `python3 tools/docs/check.py`
  and `git diff --check` passed.

At that checkpoint Linux runs 39405534445 and 39405529346 for 2b9cc82 were
still in progress. Their completed results are recorded below; newer source
still requires fresh Linux execution after being pushed.

## Native boundary and propagated-failure follow-up

Native SDK payloads now enforce the same 64-level nesting boundary as the
facade before effects. A real-node test bypasses the Python encoder: the exact
wire-depth boundary commits, the next level yields `ValidationError/not_committed`,
and the rejected key stays absent. Native request codec tests also accept the
exact 4,096-byte body, 128 headers and 32,768 header bytes, rejecting the next
byte/header. Other input/output/admission boundary rows remain partial.

A submitted capacity verdict is conservatively `Overloaded/unknown`, because a
callee can propagate a nested call's admission refusal after its own writes.
Only the bridge's pre-dispatch capacity refusal proves `not_committed`. A real
Rust cell test writes once and then returns a nested capacity verdict: the
native SDK exposes this handler failure without its inner typed route cause,
so Python receives `Ambiguous/unknown`; readback observes exactly one write.
This directly verifies uncertainty and no adapter replay. It does not claim the
native SDK preserves every nested cause; H04 retains that limitation.

Both Linux runs 39405534445 and 39405529346 on 2b9cc82 completed all workspace
unit/integration tests and doctests, including 365 Core unit tests, all 87
native-node cases and the actual GNU same-SONAME/RPATH regression. Both then
failed Clippy on Linux-only `chunks_exact(2)` in the new loader test. The fixture
now uses `as_chunks::<2>()` and explicitly rejects an unpaired argument. No lint
was suppressed. The independent provisioned package/rolling stages were skipped
after that lint failure and still require fresh successful CI. Earlier lease
timeout failures remain historical evidence, with their assertions unchanged.

Final local checks after the native-depth and propagated-failure changes:
`cargo test -p cellar-python --all-features --locked`,
`cargo clippy -p cellar -p cellar-python --all-targets --all-features --locked -- -D warnings`,
`cargo format --check`, `cargo check-layout`, both repository dependency/docs
guards and `git diff --check` passed. These do not execute Linux-only code on
macOS or establish the remaining production matrix.

## Owned operation accounting and sampled observations (2026-10-20)

The adapter now counts admitted, queued, running, completed, delivered and
abandoned operations independently of byte reservations. An owned observation
follows each event through the original native future and reply; drop records
queued/running/reply abandonment without entering Python. `Report::complete()`
requires zero pending stage gauges and matching terminal/admission and
completed/start totals. Failed/abandoned mutations remain uncertain; accounting
is not rollback evidence. Fixed histograms measure queue wait, native
parse/validation/SDK await, completion-to-loop-dequeue and admission-to-dequeue.
A unit corpus covers all terminal stages, inclusive bucket boundaries and
saturating elapsed totals. Real host-await tests observe pending work live;
joined reports verify histogram counts and cancellation accounting.

The old `transferred_bytes` field counted reservations rather than actual
transfers. It is replaced by accurately named `reserved_bytes_total`, with
separate input/output UTF-8 copy counters. Python strings are borrowed only
within synchronous PyO3 calls; payload/name copies occur after scope, size and
mailbox admission. Operation names are limited to 64 bytes. Explicit owned
Python output strings are metered at creation. Codec temporaries, UTF-8 caches
and native library/transport copies are outside these counters. No legacy
report reader or alternate format marker is introduced.

`interpreter_snapshot()` uses a nonwaiting Rust cache reader: no initial sample
or a busy publisher returns `None`. Generation and age expose staleness when
Python blocks. The managed pump publishes every configured interval (250 ms
default) and before retiring objects, including after SDK admission closes.
Snapshots include CPU/wall/allocated blocks, handler/loop timing and active
task/invocation/object/retirement work. Tests observe a live SDK wait, verify
cache age/busy behavior and detect a controlled 60 ms interpreter blockage.
Loop delay includes Python blocking, GIL contention and OS scheduling; it does
not claim exclusive attribution to one cause. Registry retirement does not
certify collection of application-retained aliases.

Optional one-frame CPython allocation tracing starts before application imports
and reports current/peak traced bytes plus instrumentation memory. It defaults
to disabled; unavailable values are `None`. Bootstrap stops its owned tracer
after success, import/cold-validation failure and Rust supervisor error/panic.
It does not stop application-owned profiling when disabled. Tracing overhead
and private/native allocation-domain exclusions are documented. Host probes
also record RSS and available jemalloc/libc bytes before initialization and
after joined cleanup, outside node execution or interpreter ticks. These are
whole-process snapshots, not per-cell memory bounds; CPython remains initialized.

The full adapter suite passes on host CPython 3.14.6. The independently
provisioned CPython 3.14.5 package smoke passes Pydantic/NumPy/native work, bindings,
cron, preserving restart, lost-response reconciliation and joined shutdown with
operation conservation, copy counters and memory probes checked. The default
package has allocation tracing disabled; the bridge corpus exercises tracing
enabled and verifies failure-path cleanup. Final lint/layout/docs/dependency
checks and fresh Linux qualification for these source changes are recorded
separately when complete. The active fe6d2f8 CI runs do not cover these newer
observability changes.

Local observability checkpoint checks:

- `cargo test -p cellar-python --all-features --locked` passed after the complete
  operation/copy/snapshot/tracing changes, including traced supervisor failures
  and startup import/cold-validation cleanup.
- `cargo clippy -p cellar-python --all-targets --all-features --locked -- -D warnings` passed.
- `PYO3_PYTHON=/nonexistent/cellar-native-only cargo check -p cellar-python --no-default-features --lib --locked` passed.
- Provisioned CPython 3.14.5 package smoke with `--with-cron` passed the stronger
  combined-report checks. Host CPython 3.14.6 covers the traced adapter corpus.
- `cargo format`, `cargo check-layout`, the no-JavaScript dependency guard,
  documentation integrity and `git diff --check` passed.

Linux runs 39410295139 and 39410287131 on fe6d2f8 remain live at this checkpoint.
Their exact handles are retained rather than restarting them; neither covers
the unpushed observability source above. Full completion remains unproven.

## Exact application receipt replay and Linux checkpoint

A direct raw-body probe found a gap hidden by earlier JSON-only assertions:
first response `{"count":9,"alarms":0}` replayed as
`{"alarms":0,"count":9}`. The effect was not duplicated, but the supposedly
sealed response was re-rendered after object-key ordering changed in storage.
The previous test scope proved retained values, not identical response bytes.

The example now renders its response before the checked batch, stores status,
ordered application headers and the rendered ASCII JSON body in the receipt,
and reconstructs those fields directly on replay. No legacy response reader or
state migration was added. The fault proxy retains the actual original bytes;
matching requests compare status/body/application headers before stop, after
preserving restart and after subsequent writes. Transport-generated metadata
is excluded. A different amount still refuses without another increment.

The changed-source fixture adds its revision header only to GET, so it proves
new code executed without altering retained POST receipt headers. The rolling
qualifier now compares owned response bytes/status/application headers rather
than only decoding the returned count. With CPython 3.14.5, fresh bucket install,
original runtime removal/restoration, then both TERM and INT handoff passed with
new writes, exact old receipt replay and complete joined reports. The stricter
packaged smoke also passed with cron and native numerical work.

Fresh private artifacts for this qualification: base inventory
`66fade4ee72b4d6ecb9fe8ca035412549d5c8be0c796363508ef9efb816a081f`,
base archive `83bb350ed6c0f418d9bdef1cbced5e580e8e97ded89ec2f1b9b130cd7ee4d5c3`,
changed-source inventory `d1d10166f32af96c381c1e5e056c1d149a95a5bf0d1572a68530319bcd66a285`
and changed-source archive `f83a3451d54ed2bdb62dd441dfd711438d77cb41441691906b1f50477bba7b7d`.

Linux workflows 39410295139 and 39410287131 on fe6d2f8 both completed
**successfully**, including full workspace tests/doctests/Clippy, the independent
CPython 3.14.5 package, GNU loader binding, runtime archive restore and changed
source TERM/INT process upgrades, plus native examples. These are exact-revision
results. They did not contain the newer metrics or raw-body receipt checks and
do not qualify those changes until another current-source run completes.

Receipt follow-up checks: stronger provisioned `--with-cron` smoke, fresh
runtime restoration and raw-response TERM/INT upgrades passed. Affected
`cargo clippy -p cellar -p cellar-python --all-targets --all-features --locked -- -D warnings`,
`cargo format --check`, module layout, dependency/docs guards and
`git diff --check` passed. No existing assertion was weakened to accept reordered
response bytes. Current-source Linux execution remains required after push.

## Independent queue/cron delivery and startup privacy

The adapter bridge harness now includes two disposable real-node modes:

- `--runtime-delivery` exercises the actual durable queue broker with single-message
  batches and one consumer. It checks successful default ack, error-driven retry,
  bounded exhaustion, explicit ack followed by handler error, conflicting
  message/batch ack/retry decisions and invalid settlement. A native handler
  deadline cancels Python after its SQL effect; late ack is refused, the same
  message ID returns with attempt 2, and the business receipt remains singular.
  Retained controls cannot cross a live invocation or submit after expiry.
  A producer burst admits one operation and refuses 31 overlapping mutations
  with `Overloaded/not_committed`. The test waits for actual zero broker backlog,
  then verifies identical SQL delivery/effect records and empty backlog after
  preserving restart. Explicit acknowledgement is a dispatch decision, not an
  independently durable acknowledgement before the handler returns.
- `--runtime-schedules` uses the real reserved cron cell, alarm and history RPC.
  Its injected UTC clock advances at real speed near a minute boundary in the
  past, preserving lease time progression and the native alarm's independent
  real-host-UTC admission check. Four matching expressions cover held success,
  failure followed by retry, explicit `no_retry()` followed by error, and plain
  success. Retry retains the occurrence ID; checked SQL receipts avoid another
  effect. Live-cross/expired controller use refuses. All execution records and
  the durable next alarm remain identical after preserving restart; only the
  per-read observation timestamp is excluded. Native scheduled timeout and
  unclean-interruption replay remain separate work.

Both fixtures require complete native/Python outcomes and settled bridge
operation accounting. They supplement the existing two-domain binding test and
do not establish the remaining producer/KV/SQL in-flight fault matrices.

Queue settlement outside a queue event, unknown message IDs and cron controls
outside a scheduled event are now proven validation refusals. They yield
`ValidationError/not_committed`; native SDK errors after admission retain their
conservative unknown outcome.

Application import, cell lookup and cold descriptor validation previously
retained raw `PyErr` messages. They now use fixed stage failures without a Python
exception source. A final interpreter-error boundary discards remaining direct
Python exception values while preserving typed Rust host failures. Disposable
startup cases capture process output and test private exception/path sentinels,
including an exception whose `__str__` would print its secret if called. The
supervisor is never entered for these invalid applications. Explicit application
logging is still owned by trusted application code.

Current local checks on macOS ARM64/CPython 3.14.6 passed: the full all-feature
adapter suite, strict all-target/all-feature adapter Clippy, native-only build
with a nonexistent interpreter, formatting, layout (598 Rust files), dependency
guard, documentation integrity and diff whitespace. The initial dependency
guard invocation lacked Cargo on PATH; rerunning with the selected toolchain
passed the 415-package graph. Initial fixture failures were corrected in the
fixture: queue metrics are floating-point values, native alarm admission uses
real UTC, and cron history's observation time changes on every read. Delivery
and durable-record assertions were retained. Current source still requires
fresh Linux execution after push; 7f9c0ef workflows remain live at this checkpoint.
