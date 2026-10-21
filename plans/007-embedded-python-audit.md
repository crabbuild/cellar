# Embedded Python: implementation and completion audit

This ledger audits [plan 007](007-embedded-python-pyo3.md) against the actual
implementation. The objective remains the complete plan, including packaging,
failure qualification, observability and measured baselines. A runnable example
or passing happy-path smoke is insufficient to complete it. Explicitly deferred
features (ASGI, streaming, arbitrary transaction callbacks, untrusted-code
isolation and subinterpreters) remain deferred. Free-threaded CPython 3.14t was
subsequently requested and is an additional supported build/qualification scope.

Core deliverables now span M0–M5, including both CPython threading ABIs. The
full objective is **not achieved**: preserved delivery/native lifecycle failures,
the corrected reset/read qualification and final-source release evidence retain
open gates. Linux installed-package prerequisite qualification now passes in both
threading ABIs at 97be2cc. The bootstrap correction and measured defaults have direct evidence
within the scopes below. A verified row covers only its explicitly
stated evidence and scope.

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
| A02 | Pin CPython 3.14 threading ABI and compatible bridge | Build-time ABI guard plus runtime preflight, pinned Cargo graph | verified: CPython/shared-library and selected-threading-ABI build guards, exact PyO3/build-config 0.30.1 Cargo lock, and a C-only pre-initialization loaded-library probe. Actual opposite-library loading previously caused SIGSEGV in the unbound conventional host; both corrected mismatch directions now refuse normally before admission, while matched controls run and join. Final-source Linux execution remains Q12 |
| A03 | Initialize before node; explicit home/import roots/site/env | Isolated configuration before Python code/imports; hostile environment tests | verified: explicit PyInitConfig + `startup` hostile-environment matrix |
| A04 | Process-owned loop and explicit joined Rust executors | Runtime source, concurrent await test and combined shutdown evidence | verified locally: process main-thread loop, explicit joined Rust supervisor, concurrent host-await progress and current combined two-/three-node stop reports |
| A05 | One coordinated signal owner | Disposable process INT/TERM tests; no competing interpreter handlers | verified locally: startup preserves existing native handlers and removes CPython implicit INT handling; real TERM/INT handoff with two compiled Python processes yields complete joined reports |
| A06 | No hidden runtime executes SDK operations | Source audit plus instrumented original-turn/domain tests | verified locally: operation futures remain inside the original native application driver; live cross-domain binding tests refuse handle exchange; driver polls nested calls without a separate SDK executor |
| A07 | Owned values only across thread/await boundary | Source review and lifecycle tests; no borrowed Python/SQLite guards | verified locally: owned strings, JSON, revocation flags, reservations and supported owned Py handles cross the bridge; native operations retain no Python token/borrow or SQLite guard across awaits; current lifecycle suite passes |
| A08 | Native authority checked at enqueue, execution, commit/output | Pending-operation cancellation and forced-fence tests | verified locally in both threading ABIs: live native poll gates prove one mutation admitted/queued but not started, then dropped by deadline or actual node-lease fence with no effect; three-node authenticated peer/bucket proof holds independently interrupt sync and final output by deadline/fence after a native write, refuse success and late facade/raw-sender mutations, retain one handler attempt and recover acknowledged state without claiming rollback of unacknowledged state; original Context turn/domain/epoch checks and synchronous transaction commit rechecks remain intact; all reports conserve operations and release resources; final Linux matrix remains Q12 |
| A09 | Two embedded domains cannot exchange live contexts | Simultaneously live contexts in two nodes: cross-use refused | verified: reproduced live cross-domain read, then blocked API and native sender in `bridge` |
| A10 | Opaque invocation identity; metadata cannot choose authority | Cross-event/domain/generation/activation negative tests | verified locally in both threading ABIs: simultaneous live same-name domains refuse cross-use through both facade and native sender even with copied identity metadata; retained event, generation and retired-activation contexts refuse mutations; native authority remains the original Rust Context |
| A11 | FIFO operations, one outstanding mutation per invocation | Concurrent mutation/read/cancel tests; explicit state machine | verified: native admission reservation rejects overlap; same-invocation read progresses during nested mutation; reservations release on drop |
| A12 | Mailbox pollable during nested calls; existing cycle behavior | Nested-call progress, cycle and cancellation tests | verified locally in both threading ABIs: same-invocation reads progress during a pending mutation; real A→B→A routing reenters the same Python object/activation while the first A frame awaits B, refuses use of its first context through facade and native sender, and completes all native writes; separate deadline and actual lease-fence cases revoke all four suspended root/outer/middle/inner contexts, refuse late mutations and preserve acknowledged state in the same or successor domain with settled accounting |
| A13 | Descriptor declares supported classes/events before admission | Cold validation and invalid registration tests | verified: cold descriptor async/constructor checks plus deployment hook rejecting absent classes, undeclared queue consumers and cron handlers before activation |
| A14 | Native factories never call/import Python | Source/caller audit plus blocked-interpreter factory test | verified locally: proxy creation uses only immutable Rust source descriptors, atomics and owned lifetime flags; in both CPU-loop and GIL-held C-call owner drills, a Rust root submits a new real cell activation and the native factory counter advances while Python remains blocked and its handler cannot complete |
| A15 | Activation identity includes domain, generation and epoch | Retire/recreate and simultaneous same-name/domain tests | verified locally in both threading ABIs: distinct simultaneous same-name domains, actual pressure/operator eviction with fresh instances and exact next epochs, same-epoch native generation swap with a fresh instance, state recovery and forced-fence successor collection; exhausted IDs refuse without wrapping and concurrent allocations remain unique |
| A16 | No interpreter-per-cell or hot Python replacement | Process-wide guard and attempted reload tests | verified locally and on Linux at 21f6b30: cell proxies share the process runtime, a second runtime is refused, live selector replacement is refused in process, and changed Python source executes only in the distinct replacement executable/process; trusted application mutation of Python globals is outside generation isolation |
| A17 | Support conventional and free-threaded CPython 3.14 ABIs without borrowed authority or implicit GIL dependency | Both ABI builds, actual GIL-off workers, concurrent hooks, mixed-ABI refusal and installed-package recovery | verified for macOS ARM64 and Linux x86_64: both local adapter suites pass; foreign-loop authority refusal, exact concurrent hook counts, mixed-layout/ABI refusal and archive restoration pass; both 0bfd83f Linux free-threaded jobs completed successfully with CPython 3.14.8t adapter/native suites and installed 3.14.5t GIL-off libraries, archive recovery, changed-source TERM/INT handoff, forced-KILL supervisor/restart, receipts/new writes and complete drain; final lifecycle/cancellation source still requires Q12 |

## HTTP, codecs and failures (M1)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| H01 | Buffered bytes/JSON HTTP; ordered repeated headers | Byte and duplicate-header round trips through real ingress | verified: binary body and ordered repeated request/response header round trips through real ingress |
| H02 | Body/response/header/depth/value limits at input and output boundaries | Exact boundary and over-limit cases, released reservations | verified locally in both threading ABIs: real ingress/nested body, ordered headers, input depth, batch count/bytes and handler completions retain exact/next-unit cases; native-writer fixtures prove SDK success envelopes of 65,536 bytes pass and 65,537 refuse, result-value depths 64/65 pass/refuse, and later operations remain usable; bounded serialization returns HostError/unknown for reads or Ambiguous/unknown after a successful SQL insert without rollback or replay; complete reports retain zero leaked reservations; final Linux execution remains Q12 |
| H03 | Finite floats, integer range, keys, cycles and unsupported values | Codec boundary corpus and no mutation assertions | verified: real-node invalid-value corpus (nonfinite, int bounds, keys, unsupported, cycles, depth) has no mutation; valid boundaries round-trip |
| H04 | Typed validation, conflict, expired/ownership, overload, ambiguity | Public Python error tests and native cause classifications | verified locally in both threading ABIs: public error classes, explicit outcomes and fixed messages retain codec/batch/conflict and real expiration/fencing coverage; real native residency exhaustion reaches Python as Overloaded/unknown before the target handler enters, then the same caller reuses its mutation reservation; failed output proof reaches a still-live caller as Ambiguous/unknown with one handler attempt and no replay; ownership routing classification is unit-tested. Some native bindings flatten internal causes: unavailable provenance remains unknown, never inferred rollback. Final Linux execution remains Q12 |
| H05 | Cancellation stays cancellation; no blind retry | Cancel-before/after submission and ambiguous-result reconciliation | verified locally: independent KV/shared-SQL/producer cancellation before submission and during withheld native proof stays CancelledError, failed submitted proof remains Ambiguous/unknown and the adapter attempt count stays one; explicit reconciliation retains singular business effects; native consumer timeout also refuses late ack before same-ID redelivery; packaged S09 covers lost proof-gated response reconciliation; Q08 independently qualifies cron interruption |
| H06 | Sanitized, bounded failures; no secrets/paths/stack locals | Sentinel request/exception/path tests on public response and logs | partial: previous e9b8d15 leaked the bootstrap sentinel on Linux in both ABIs. CPython 3.14.8's unconditional pre-hook codec path dump is reproduced locally and contained by a scoped C-initialization stderr sink; all 36 bootstrap cases and complete adapter suites pass with conventional 3.14.8 and free-threaded 3.14.5t. Descriptor identity/flags and normal later stderr are preserved. All four Linux adapter and installed-package jobs at published 9143fef pass; complete current-source qualification remains Q12. Existing HTTP/nested failure evidence retains bounded fixed messages, exact committed effects and no replay. Typed Incomplete/native outcomes remain intact; arbitrary application/library output and pre-main OS loader diagnostics retain their documented trusted-process scope |
| H07 | Concurrent Python work during host await | Two-handler synchronization with one blocked host operation | verified: `host_await_progress` barrier test runs another handler during pending SDK fetch |
| H08 | Native-extension representative execution and teardown | Pinned package matrix, real operations and joined report | verified for macOS ARM64/Linux x86_64 qualification: pinned Pydantic/core, NumPy, hashing and pure Python inspection perform real work; managed executor operations join and TERM/INT replacement reaps the old process with complete reports; arbitrary private native-library pools are explicitly outside in-process drain accounting; exact versions/platforms remain Q09 |

## Atomic storage, alarms and durability (M2)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| S01 | Bounded declarative atomic batch, no Python/await under transaction | Source audit, malformed batches, aggregate byte/count boundaries | verified locally: native validation precedes a Rust-only transaction_sync closure with owned JSON and no Python/await; max_value_bytes caps the entire serialized batch, max_batch_operations sums checks/puts/deletes/alarm together; malformed corpus, exact/over operation counts and 65,536/65,537-byte aggregate payload cases retain atomic writes/alarm state and release reservations |
| S02 | Presence-aware null/missing checks and full rollback | Conflict/rollback tests asserting all mutations and alarm unchanged | verified: failed check retains all JSON state and previous alarm; presence/null/missing tests pass |
| S03 | No duplicate/conflicting keys or malformed alarms | Negative batch corpus at owning boundary | verified: real bridge corpus refuses duplicate deletes, put/delete overlap, invalid/reserved/oversized keys, malformed snapshots, unknown fields, invalid/conflicting alarms and over-budget batches; no partial writes/alarm changes; exact operation boundary commits |
| S04 | Conflicts with native Rust writers | Concurrent Python snapshot/native mutation/check refusal test | verified: same-cell Rust transaction commits while Python holds a snapshot; Python checked batch refuses and leaves the native value/other write intact with no partial Python mutation |
| S05 | Sync and response output proof retain native semantics | Controlled replication pause/failure, response withheld/refused | verified locally: real authenticated three-node fleet plus bucket; sync and final output remain pending while both proof paths are held; actual follower acknowledgements release success with bucket still held; both paths failing yields HTTP error and exactly one handler attempt, never a rollback claim |
| S06 | Durable alarm mutation atomic with cell state | Crash/recovery and failed-check alarm tests | verified locally: a failed check changes neither intent nor alarm; one checked batch commits intent and alarm; CPU-loop and GIL-held C owners become noncooperative, native delivery times out/retries, owners are killed and all local databases deleted; the actual shipped example alarm method recovers the persisted occurrence receipt with one effect despite a changed native delivery deadline, and another delivery timestamp cannot duplicate it |
| S07 | Duplicate alarms safe by persisted occurrence | Repeat same occurrence with idempotent durable effect | verified: sample persists an occurrence receipt atomically; bridge invokes the same occurrence twice and observes one increment |
| S08 | State persists over preserving restart | Real-node restart and packaged host restart | verified locally: real Python cell restart and remote handoff recover acknowledged state; provisioned host restart and changed-source process upgrade preserve exact receipts; independent queue/cron fixtures also retain SQL effects, attempts, settled broker state and cron history |
| S09 | Lost committed response resolved with same receipt key | Disposable host reply loss followed by receipt reconciliation | verified locally with direct wire comparison: proxy discards proof-gated success; rendered application status/body/header fields persist atomically with the effect and replay exactly before/after restart and after later writes; different-amount reuse refuses; changed-source process upgrade also compares exact application response fields |

## Failure, cleanup and combined reports (M3)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| L01 | Root cancellation revokes SDK admission immediately | Queued/late writes refused before execution and after fence | verified locally for managed cancellation in both threading ABIs: handler return/abandoned work, native timeout and lease fence expire SDK authority; self-cancelled root/cell handlers wake the original native driver despite retained senders and refuse late mutations, including after a committed write; queued/execution/proof-output race coverage is recorded in A08 |
| L02 | Owned cleanup survives admission closure; no destructor waits on GIL | Retirement/closure race tests and cleanup counters | verified locally for runtime-owned cleanup in both threading ABIs: atomic-only Rust retirement, actual weak collection during otherwise idle Python after native drain, and two resident nodes joining after process-global admission closure; cleanup remains independent of SDK admission, and blocked destructors/noncooperative work retain incomplete reports |
| L03 | Retired activation object cleanup is observed | Destruction counters/weakrefs under eviction, fence and redeploy | verified locally in both threading ABIs: actual weak references clear after resident pressure eviction, signed management eviction, native generation swap, idle forced drain, global admission closure and fence-task completion before successor invocation; a cancellation-suppressing task correctly keeps its object alive until it exits |
| L04 | Close async generators and managed executor work | Async-generator finally + thread/executor join tests | verified for managed loop: async-generator finally observed; default-executor work joined; raw/native private pools remain application-owned |
| L05 | Combined report includes native report and all bridge/task/cleanup/budget work | Serialized combined report, incomplete cases, queued/running counts | verified locally for runtime-owned work and explicitly recorded native nodes: combined outcomes and host failure source, task/invocation/object/retirement gauges, bridge reservations and admitted/terminal operation conservation; live pending SDK work, all native-future/reply drop stages and incomplete lifecycle cases are directly tested; private application/library pools remain application-owned |
| L06 | Correct shutdown phases with both runtimes alive | Ingress closure/drain/cancel/object cleanup and final-join assertions | verified locally in both threading ABIs: ingress refusal before final native join, honest pending/timed-out native drain, cancellation and late-call refusal with both runtimes alive, idle object collection, exact state recovery and subsequent clean stops; global interpreter failure joins both native nodes and retains its failed combined report |
| L07 | Noncooperative code cannot fabricate complete drain | Disposable asyncio-stubborn, CPU-loop and blocked-C-extension hosts | verified: stubborn asyncio reports incomplete; disposable infinite-Python and GIL-held native-call owners require external termination and cannot produce clean-drain reports |
| L08 | External watchdog supports hard termination/restart | Bounded child-host watchdog drill and recovered acknowledged state | verified locally on Unix/macOS ARM64: shipped `cellar supervise` has bounded startup/probe/stop/reap policy, instance-correlated Python health, a global restart cap and a systemd template; six direct process tests pass, and the full installed CPython 3.14.5 archive-restore, changed-source TERM/INT handoff and forced-KILL/joined-restart/exact-receipt/new-write/complete-replacement-drain run passes; both 0bfd83f Linux workflows pass the installed supervisor drills in both ABIs; later-source qualification remains Q12 |
| L09 | Unclean owner death/takeover retains acknowledged state | Kill owner, remove local disk, recover from fleet, new write | verified in local development fleet: acknowledged write, hard owner death, complete owner disk deletion, bucket recovery and new write; independent quorum proof faults are recorded in S05 |
| L10 | Stale Python task cannot mutate after ownership loss | Pause old handler, force fence/takeover, resume old handler, inspect state | verified: real node lease mismatch fences old domain; after lease expiry successor recovers acknowledged state; cancellation-suppressing old Python task resumes and gets ContextExpired, with no late mutation |
| L11 | Initialize once; never finalize while threads enter | Source audit, duplicate runtime refusal and error-path join tests | verified for runtime-owned work: main-thread process guard refuses reinitialization; no interpreter-finalization path exists; supervisor success/error/panic and interpreter failure close admission, detach the GIL for the host join, clean owned profiling and retain incomplete work; duplicate-runtime and disposable error-path tests pass; private application pools remain application-owned |
| L12 | Raw Python effects/heap/coroutines not claimed durable or isolated | API/docs audit and recovery examples with explicit persisted state | verified for the documented contract: SDK, adapter, example and accepted ADR explicitly exclude raw effects, coroutine frames, private heaps/pools and untrusted-code isolation; crash/restart examples recover persisted business state/receipts through Cellar rather than Python frames |

## Verified deployment and replacement (M4)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| P01 | Immutable source/registration descriptor bound to native artifact | Source/class/event digests and tamper tests | verified: build embeds exact source/registration/file inventory; typed per-executable inventory digest is rechecked before every factory/generation admission |
| P02 | Hash-locked platform wheels and native dependency closure | Lock artifact hashes, transitive inventory and offline install | verified within recorded package scopes: resolved load contexts, OS cohorts, external provider identities and pre-initialization/post-import gates are implemented. Both local 3.14.5 ABIs pass inventory faults, cold restoration, native operations, receipts and process/supervisor replacement. At 97be2cc, conventional push/PR and free-threaded push jobs complete the full Linux installed-package step, including GNU capture/refusal, offline archive restoration, changed-source TERM/INT handoff and forced-KILL supervisor recovery. The PR free-threaded job stops earlier at a reset/read qualification race; final-source release evidence remains Q12. |
| P03 | Runtime identifies CPython/GIL/SOABI/platform and actual files | Inventory capture plus pre-code digest/ABI verification | verified for supported targets: CPython/GIL/SOABI/micro version/target, actual loaded libpython and exact file contents/executable modes; external OS closure tracked in P02 |
| P04 | Native package metadata inventories Python artifacts, format marker 1 | Protocol types, strict validation and old-node feature rejection | verified: typed native runtime references, format marker 1, embedded-python-v1 feature gate and executable/inventory pairing; malformed pairing tests added |
| P05 | Build/deploy/install stage, publish, recover and verify closure | Real bucket package tests, corrupt/missing artifact refusal | verified locally: build/stage, pre-selector verified bucket publication, corrupt/missing blob refusal and real restoration with original runtime absent |
| P06 | Verify runtime/source/dependencies before importing app or admission | Sentinel app not run for each invalid artifact/ABI/environment case | verified: inventory/ABI/source before initialization/import; process digest before application factory/generation; real installed-host and tamper drills |
| P07 | Explicit home, roots, site behavior, no ambient user/PYTHONPATH/CWD | Hostile startup environment and .pth/customize injection tests | verified: `startup` path/site/CWD/sysconfig override tests plus explicit environment view |
| P08 | No network install at startup or request; offline qualified environment | Network disabled/failing installer and clean-runtime deployment smoke | verified locally: hash-locked offline wheel install and archive-restored host launched with cleared environment; no runtime package installer |
| P09 | Wrong ABI/missing library/tampered source refuses startup | Disposable startup matrix with no admitted request or app side effect | verified locally: invalid ABI/home and changed source/extension refused; the disposable bound-host matrix independently moves source, native extension, entire package root and actual libpython outside the inventory, refuses every startup and restores the exact runtime afterward; unlisted files also refuse startup; both 0bfd83f Linux workflows pass these loss cases in both ABIs; later-source qualification remains Q12 |
| P10 | One immutable generation per process; ordinary native generations unchanged | In-process replacement refusal, native-only regression, rolling two binaries | verified locally: old running process observes/refuses a new deployment while remaining live; wrong executable rejects new package; changed Python code executes only in the replacement process; native generation regressions passed previously |
| P11 | Old/new host handoff and runtime artifact recovery | Versioned process handoff, restored artifact installation, retained state | verified locally: original runtime removed/restored from bucket, then distinct binaries, application versions and source inventories run in separate processes; TERM/INT handoff preserves state and exact receipts and executes changed Python cell code; both native/Python reports complete |
| P12 | Runnable example from fresh installation | Documented build/install/run/recovery test from clean environment | verified for local supported package: fresh-prefix archive restore, cleared-environment HTTP/native numerical work/receipt replay and complete preserving stop; platform matrix remains separate |

## Limits, metrics, SDK growth and qualification (M5)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| Q01 | Finite startup/request/body/value/depth/batch/mailbox/task/object/drain limits | Named limits, boundary tests, reservations released on every exit | partial: configured invocation/task/object/byte admission, operation mailbox capacity, response envelope and existing body/header/depth/batch/startup/drain limits have direct boundary tests; empty/oversized mailboxes refuse before bootstrap and task admission also covers empty-context callbacks; SDK-result envelope/depth boundaries now pass independently through native storage/SQL; shipping-default mixed-load qualification now passes three optimized repetitions per ABI at clean source 7d90f0c, with 12,720 requests, actual 128-invocation saturation/refusal/reuse, exact body boundaries, 3,180 acknowledged updates recovered after owner kill/local disk deletion, and complete joins; raw evidence and scope are in the recorded baselines. Linux shipping-default workloads pass in all four 3e56924 jobs and the three 97be2cc jobs that reach the workload, including both ABIs; final-source qualification remains Q12 |
| Q02 | Bounded backpressure both ways and cleanup accounting | Saturation/cancellation/failure tests with zero leaked reservations | verified locally for runtime-owned queues/reservations in both threading ABIs: independent real-node invocation, task, object, exact aggregate-byte and reply-channel saturation refuses excess work, releases/reuses capacity and keeps later work usable; full/closed operation mailbox tests release every event reservation; native-future/reply drop stages, binding proof faults, cancellation, 32-way mutation pressure and retirement/shutdown conserve all admitted operations with zero leaked reservations; trusted factory replacement/raw tasks/private pools remain outside cooperative admission |
| Q03 | Queue, bridge, Python/Rust duration, copied bytes, allocations/RSS and cleanup metrics | Instrumented counters/histograms and live snapshot tests | verified locally: bounded queue/SDK/reply/round-trip histograms, explicit UTF-8 boundary-copy counters distinct from reservations, lifecycle conservation/gauges, live age-stamped interpreter/work snapshots, loop scheduling delay, optional traced allocation bytes and pre-interpreter/post-join RSS/jemalloc/libc samples; scope/exclusions documented; adapter matrix and pinned package smoke pass; cross-platform and performance baselines remain Q09/Q10/Q12 |
| Q04 | Python CPU attributed separately; native accounting gaps disclosed | Separate measurements and documented coverage | verified locally: managed-loop thread CPU measured separately from Rust; handler duration/count and memory-snapshot assertions pass; exclusions for native/executor pools documented; cross-platform qualification remains Q09/Q12 |
| Q05 | KV authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python, not only native SDK tests | verified locally: real two-domain reads/writes refuse live-cross/expired handles; independent before-submit/in-flight cancellation, held/failed native data proof, conservative unknown failure without adapter replay, explicit idempotent-set reconciliation, 1-admitted/31-refused overlap pressure and preserving restart retain exact effects with complete reports and zero pending operations/reservations |
| Q06 | Shared SQL authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python | verified locally: actual two-domain DDL/DML/queries refuse retained-handle writes; independent before-submit/in-flight cancellation, failed native proof, unknown failure without replay, explicit primary-key receipt reconciliation, 1-admitted/31-refused overlap pressure and restart retain singular effects with complete reports and zero managed work |
| Q07 | Queue producer/consumer settlement, retry, cancel, replay and resource qualification | Adapter-specific independent failure matrix | verified locally: real consumer matrix covers default/explicit ack, handler errors after ack, first conflicting decision, same-ID retry/exhaustion, timeout/cancel expiry and retained-handle authority; independent producer before-submit/in-flight cancellation, held/failed proof, unknown-result reconciliation and overlap pressure run through the real broker; SQL business receipts remain singular and the empty broker/effects persist across restart with complete reports; this drill reproduced and now passes the native running-alarm reset regression |
| Q08 | Cron retry/no_retry, authority, cancellation and occurrence replay | Adapter-specific independent failure matrix | verified locally on standard and free-threaded CPython: real native alarm/history matrix proves same-occurrence retry, explicit no_retry after error, refusal of live-cross/expired controllers, idempotent SQL effects and exact history/alarm persistence over restart; independent timeout and killed-owner/local-disk deletion tests preserve the original occurrence and interrupted history, reject late SQL/no_retry, recover one SQL effect across two attempts and completely join the replacement; current-source Linux qualification remains Q12 |
| Q09 | Pure Python, PyO3 native extension and numerical library compatibility | Exact pinned versions/platforms; representative operations/concurrency/teardown | verified for stated platforms/versions: macOS ARM64 CPython 3.14.5/3.14.6 and 3.14.5t; Linux x86_64 regular 3.14.8 plus installed 3.14.5 and 3.14.5t execute pinned Pydantic/NumPy/pure-library workloads; both 0bfd83f free-threaded Linux jobs completely pass the 3.14.8t adapter suite and 3.14.5t package/archive/rolling/supervisor/native-library qualification; final lifecycle/cancellation source remains Q12 and other platforms/library versions remain unqualified |
| Q10 | Native versus Python latency/throughput/memory/copy/recovery baselines | Reproducible harness and raw measured evidence, no invented target | verified for the stated development scope: [recorded baselines](007-embedded-python-baselines.md) retain two independent nine-case optimized experiments and raw data, including matched CPython 3.14.5/3.14.5t and a true native-only executable; exact HTTP/counter receipts, killed-owner/disk-deletion takeover, copy/memory observations and joined reports pass; the verifier rejects corrupted receipts/metrics/drain/cleanup; shared-machine load and development-store limits are explicit and no production capacity target is claimed |
| Q11 | Native-only dependency/build check and no JS all-features | Explicit CI check, local graph/build evidence | verified for dependency/build separation: explicit Linux CI native-only check with nonexistent interpreter and all-feature no-JavaScript graph passed on fe6d2f8; current source retains the same feature/dependency graph, with local no-embed and graph checks |
| Q12 | Full affected test/lint/format/layout/doc qualification; preserve failures | Exact source/toolchain commands and passing scope-specific results | partial: 97be2cc has three complete successful Linux jobs across both ABIs, including installed packages, native-node 88/88, workspace tests/lints, Axum modes and native examples. The PR free-threaded adapter job fails when its final read races native reset after a deliberately failed proof. A real native Cell destruction barrier reproduces the same HTTP 503 with one business attempt; bounded repetition of only the read-only observation corrects the audit assumption. Both complete local ABI suites, strict Clippy, three conventional controlled repetitions and format/layout for 630 files pass. This correction still needs Linux qualification. Earlier delivery/initial consumer/clock-replay failures retain separate diagnosis; passing retries do not close them. |
| Q13 | Public facade type annotations and accepted implementation decisions | Typed Python API, accepted ADR, build/bootstrap/bridge decision record | verified: public facade annotations and frozen CellIdentity; accepted ADR 004 records bootstrap, bridge, signals, immutable packaging/replacement, cleanup, compatibility and qualification boundaries against implemented contracts |
| Q14 | Current docs describe executable APIs and remaining gates accurately | Plan sketches updated to actual contracts; audit has no unstated exception | verified for current documented surface: main plan, adapter/example/environment guides describe actual facade, source layout, accepted ADR, provisioning and conservative failure outcomes; chronological audit history is explicitly distinguished from current tables; remaining gates are stated |

## Current work

The adapter's core deliverables exist across M0–M5. Completion means closing
acceptance evidence, not adding another happy-path example. M2's S01–S09 rows
are verified within their stated scopes; all phases still share final-source
qualification. Conventional and free-threaded CPython remain equal targets.

| Order | Focus | Remaining gate and evidence |
| --- | --- | --- |
| 1 | P0: qualify the reset/read correction | Q12: the PR free-threaded job at 97be2cc refuses the final read-only audit during native reset. A controlled public Cell destruction barrier reproduces the same failure, and corrected bounded observation passes with one business attempt, acknowledged state, stale-context refusal and complete joins. Both local ABI suites/lints and three conventional repetitions pass. The correction is published in 85cdef4; fresh Linux qualification is running or queued, and all older 97be2cc jobs are terminal. |
| 2 | P0: diagnose preserved reliability failures | M3/Q12: retain the earlier delivery deadline, initial consumer admission and clock/replay failures. The lease-floor correction has direct before/after proof, local native 88/88 and three complete Linux native 88/88 runs at 97be2cc. Isolated macOS consumer replays pass without reproducing the historical refusal. A bounded error-chain diagnostic and opt-in Linux fresh-fixture replay mode target the next occurrence. Keep separate causes, original deadlines/assertions and conservative mutation outcomes. |
| 3 | P1: qualify one final source | Q01/Q12: complete all affected tests/lints and installed-package/refusal/offline restoration/replacement checks against the final source in both ABIs. The GNU packaging blocker is corrected and both ABI package steps pass at 97be2cc; operating-default workloads also pass there. These component results do not replace the final complete matrix. |
| 4 | P1: publish acceptance and deployment scope | Q12/Q14: reconcile every partial row with exact source/runtime/platform evidence and update the existing draft PR. Preserve incomplete-drain and ambiguous-write semantics; development load measurements are not production SLOs or a throughput guarantee. |

Do not expand ASGI/streaming while these core gates remain. Those adapters are
explicitly optional/deferred and need their own independent qualification when
introduced. No scope reduction or production-readiness claim is implied by this
ordering. The chronological checkpoints below preserve earlier CI failures and
successes; they do not replace qualification of later source.

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

## In-flight binding faults, native alarm reset and exact limits

`--runtime-binding-faults` independently exercises real KV writes, shared-SQL
inserts and queue producer calls. Each surface has cancellation before first
submission, cancellation while real data proof is held, failed proof returning
`Ambiguous/unknown`, and explicit caller-driven reconciliation. A separate
SDK-free Python handler observes progress while the original native request is
polled. Mutation attempts remain at one until reconciliation is explicitly
requested. A cancelled in-flight call leaves its local effect in these fixtures;
failed proof can retain or discard unacknowledged work. The initial fixture's
assumption that every failed mutation must remain present was incorrect and was
removed for failed-proof cases only; cancellation, unknown outcome, no-replay and
final singular-effect assertions remain. Reconciliation uses an idempotent KV
set, SQL primary-key receipt and queue consumer business receipt. This does not
give the producer an exactly-once delivery guarantee.

For each binding, a concurrent 32-operation burst admits one mutation and refuses
31 with `Overloaded/not_committed`. Cancel-before-submit leaves no effect. Final
SQL/queue records contain exactly the expected business keys, KV retains the
expected values, and preserving restart retains them all with an empty broker.
Both native outcomes and the Python report must complete, with settled operation
counts and no pending reservations/tasks/objects/threads.

The producer proof drill reproducibly crashed the native core twice with
`cell ... is firing an alarm while not resident`. A pure public-event regression
in `cellar-logic/tests/alarm_reset.rs` then reproduced the same invariant in
under a second before the fix. The ranked hypothesis confirmed by that test was
that reset settled alarms already waiting on a durability barrier, but omitted
an alarm whose handler was still running when another request's proof failed.
Reset now retires that firing operation before replacing residency with cleanup,
using the existing failed-alarm settlement. It retains discoverable wake state,
refuses late consuming-commit completion and still discards the unproven runtime.
The regression covers both running and already-gated alarms, no premature wake
reconciliation, stale completion and runtime-stop completion. The original
unmodified broker fault scenario passes after the fix. No invariant was weakened.
The reset documentation also now states that durable recovery may retain an
unacknowledged write; reset is not a rollback certificate.

`--runtime-limits` qualifies exact input/output body and header boundaries through
real ingress and nested native fetch. It accepts 4,096 bytes, 128 repeated headers
and 32,768 header-name/value bytes, rejecting the next byte/header. Nested
rejections do not invoke the cell. Reqwest's smaller response-header-count bound
initially refused the valid 128-header response; a finite 64-KiB/10-second raw HTTP
reader now verifies that response without changing the adapter limit. A complete
65,536-byte batch envelope commits, the 65,537-byte envelope fails validation and
both prior value and alarm remain unchanged. The owning batch byte/count policy
is now explicit in the adapter guide.

The external CPU-loop and GIL-held-C watchdog cases also observe native factory
progress: a Rust root activates another cell while the interpreter is blocked;
the real factory count advances to two, while the Python handler remains pending.
This proves factory allocation itself does not wait for or enter Python. The
child now explicitly records its native outcome if it exits cooperatively.

Current local validation (Rust 1.98.1, macOS ARM64, CPython 3.14.6):

- All ordinary `cellar-logic` integration tests pass, including the regression
  observed failing before the production fix.
- All 364 host library tests and the native queue/cron integration cases pass.
- All 87 native-node cases passed serially in 654.96 seconds with the reset fix,
  including both cases that failed in the Linux push run described below.
- The full all-feature Python unit/bridge/bootstrap/doctest suite passes with
  binding faults, limit boundaries and blocked-interpreter factory probes.
- Strict Core/adapter/logic all-target/all-feature Clippy passes. Final
  format/layout/dependency/docs/native-only checks are recorded after execution.

Linux PR workflow 39423731169 on 21f6b30 completed successfully: full workspace
tests/doctests/Clippy, CPython 3.14.8 adapter cases, provisioned CPython 3.14.5
package, archive restoration, changed-source TERM/INT handoff and native examples.
The same-source push workflow 39423723894 passed 85/87 native-node cases, then
failed (1) the delayed cold-follower lease replay/receipt assertion and (2) the
progressing drain-token case after node lease expiry. The first is not itself a
lease-expiry diagnostic. Its assertion now prints the original/replayed receipt
data and expiry cut when it fails; its condition is unchanged. These failures
remain evidence requiring investigation rather than discarded flakes. The
superseded 7f9c0ef runs 39420831382/39420823815 were cancelled by normal workflow
concurrency when 21f6b30 was pushed, not completed qualification. Current native
reset/binding/limit changes still require fresh Linux execution after push.

Final checks after the native reset, binding, limit and factory changes passed:
`cargo format --check`, `cargo check-layout` (601 Rust files), the 415-package
all-feature no-JavaScript guard, documentation integrity, native-only adapter
compilation with a nonexistent interpreter, and `git diff --check`. The final
all-feature adapter suite and strict `cellar`/`cellar-python`/`cellar-logic`
all-target/all-feature Clippy also pass after all Rust edits. No internal harness
flag was enabled. These results close their stated local requirements, not the
remaining audit or current-source Linux requirements.


## Process supervisor and package-loss qualification (2026-10-20)

The shipped Unix `cellar supervise` CLI uses a current-thread Rust runtime and
owns the child process group. It has finite startup, sequential HTTP probe,
shutdown and KILL-reap bounds, consecutive-failure reset and a lifetime restart
cap. Health must execute in Python and echo a fresh child-only instance value;
wrong-instance responses cannot establish readiness. Redirects and ambient
proxies are disabled. TERM/INT starts cooperative shutdown; a second signal or
grace expiry escalates. Every restart follows child join. Flushed JSON lifecycle
records and child stderr are separate; the watchdog never labels process exit
as complete drain. A systemd deployment template and operator/CLI guides are
included, without activating a service on the development machine.

Six real process cases pass locally: wrong instance/startup timeout, forced KILL,
unexpected exit/global restart cap, health-failure counter reset, second-signal
escalation and policy/credential-URL rejection. A real independently provisioned
CPython 3.14.5 installed-host feedback drill also passes forced KILL of a private
GIL-held C-loop fixture, joined replacement, exact durable receipt replay, a new
write and complete native/Python handoff with a real peer. The first fixture used
one `sleep` call, which TERM interrupted, allowing legitimate clean shutdown.
It was corrected to re-enter C without yielding; no production stop assertion
was relaxed. Full combined archive/rolling/watchdog evidence is recorded after
execution below.

Startup source/filename NULs formerly reached `CString::NulError`, retaining
private source bytes in the Rust error chain. An isolated regression failed
before the fix and passes after fixed-message prevalidation. The expanded
inventory drill passes independent loss of source, extension, package root and
actual libpython, changed source/extension, and an unlisted file. Each mutation
is restored, and the valid host passes again. Missing files are moved outside
all inventoried roots, independently of extra-file rejection.

Linux push run 39446982367 on dd0df44 completed successfully: full workspace
unit/integration/doctests/strict Clippy, all native-node cases, adapter/binding
fault and limit matrices, package restoration/changed-source TERM+INT handoff,
and native examples. PR run 39446992243 failed when a read-only `/metrics` probe
arrived during deliberate proof-failure reset. The probe now retries only the
fixed read-only metrics/state observations on 500/503 within its original
10-second bound. Mutation attempts and reconciliation are never retried by the
harness. The original binding drill passes locally after this fixture correction.
The native alarm reset invariant/assertions and lease budgets remain unchanged.


Final local evidence for this checkpoint (Rust 1.98.1, macOS ARM64):

- The full combined installed-package command passes archive restoration with
  the original runtime absent, cleared-environment numerical work, changed-source
  TERM/INT handoff, exact receipt replay, forced GIL-held C-loop KILL, joined
  restart, a new write and complete replacement handoff. The killed process
  emits no fabricated clean report. The private original prefix is restored and
  all owned host/supervisor children are gone after the drill.
- The final six supervisor process tests pass after replacing raw Tokio selects
  with the repository's reasoned `asyncrt::select_biased!` macro. Initial strict
  Clippy correctly refused raw polling macros; the fixed code passes strict
  host/adapter all-target/all-feature Clippy without suppressing that rule.
- The full all-feature adapter unit/bridge/bootstrap/doctest suite passes after
  the NUL privacy fix and read-only recovery probe correction, including the
  original independent binding/proof and lifecycle drills.
- `cargo format --check`, `cargo check-layout` (606 Rust files), documentation
  generation/integrity, the 415-package no-JavaScript graph, native-only adapter
  compilation with a nonexistent interpreter, and `git diff --check` pass.
- CI now runs both the expanded source/extension/package-root/libpython loss
  matrix and the full installed supervisor qualification. Fresh Linux results
  for these changes remain pending; previous dd0df44 success is not substituted
  for execution of this source.

The operator service template has not been activated on this development machine.
It is a deployment recipe, not evidence of an actual production fleet rollout.
The remaining requirement rows stay open; this checkpoint does not complete the
full plan.


## Durable alarm identity and owner-loss regression (2026-10-20)

A new alarm crash/recovery test exposed an application bug: native alarm retry
backoff changes the next `scheduled_ms`. The example had used that mutable
deadline as a receipt identity. Both the original fixture and the actual shipped
example method failed with recovered `effects=1` but a missing original occurrence
receipt. The real native retry used a different deadline; this was not a lost
state write or a reason to relax the assertion.

The example now commits a checked durable occurrence sequence, current intent
and the native alarm in one `storage.apply` batch. The handler checks that
intent and commits its stable receipt with the effect. It retains the intent
through redelivery and uses the timestamp only as native scheduling metadata.
There is no mutation replay in the adapter. Each successful scheduling POST
creates a new occurrence and replaces the previous pending native alarm.

The regression executes the example's actual AST-extracted `Counter.alarm`
method in the stdlib-only disposable host, avoiding an independently copied
implementation. It checks failed-batch rollback, acknowledged state+alarm,
noncooperative CPU/C owners, delivery timeout, hard kill, complete owner-local
disk deletion, native retry with an observed delivery deadline greater than the
original intent, stable receipt/effect recovery, and another explicitly delivered
timestamp without a second effect. Both CPU-loop and GIL-held-C cases pass after
the production example fix. The full all-feature adapter suite and strict
adapter Clippy pass afterward. The fresh bound CPython 3.14.5 packaged smoke also passes Pydantic/NumPy,
KV/SQL/queue/cron, two successive alarm occurrences in a separate counter, both
retained after restart, exact lost-response receipts and complete joined reports.

This changes the example's internal alarm state contract. Pre-fix development
state with pending alarms lacks the persisted occurrence identity; use fresh
example state or application-specific reconciliation. No legacy timestamp
reader or silent state migration is introduced.


Final fresh alarm inventory identity:
`66b64ab88609be2dc56100aa33effc62d2ece8591f028eb3ab293972c706ff49`,
with archive
`77cebe4e0dce85df90c7969571e765347e2c61c74d3cbba5dcf633eaee9c015d`.
The bound source changed, so the packaged smoke used a recaptured environment
and fresh project rather than reusing the previously sealed source descriptor.
The last complete combined archive/rolling/supervisor run predates the alarm
example change; fresh full Linux execution still must qualify the final source.


After the alarm fix, final local format/layout checks (606 Rust files), the
415-package all-feature no-JavaScript dependency guard, documentation integrity
and `git diff --check` pass. Linux workflows 39458257880/39458250262 were still
running at d5639c7 when this subsequently discovered alarm fix was prepared.
Their pre-alarm source cannot qualify the corrected example; final-source CI
must run again. Superseded workflow cancellations are not successful evidence.


## Free-threaded CPython support (2026-10-20)

The user requested removal of the GIL-enabled-only restriction. The adapter,
layout capture, inventory protocol and provisioning tools now accept both
`cpython-314-...` and `cpython-314t-...`. The selected PyO3 threading ABI must match
the cross-build layout, runtime SOABI and bound inventory. The runtime still
initializes one process-owned interpreter and one managed loop; ordinary Python
workers can run in parallel, while live SDK handles retain their original loop,
invocation, domain and native commit/output authority.

A real 3.14.5t build failed before this change with the explicit GIL-only guard.
The corrected free-threaded full adapter suite passes. A deliberately mismatched
cross-build layout fails before admission; startup tests independently refuse
opposite-mode SOABIs. Five archive tests pass, including round-trip restoration
of both ABIs and refusal of other ABI strings. No format marker was incremented.

Reply delivery drains owned Rust replies before allocating Python objects, so
an attached thread cannot hold that channel mutex across Python GC/allocation.
Error hooks use an explicit reentrant lock for increments and snapshots; the
reentrancy permits destructor failures during collection on the same thread.
A disposable bootstrap test verifies exactly 3,200 destructor failures and 16
thread failures, sanitized and joined, in both interpreter modes. A real handler
runs two threaded computations and refuses SDK calls/completions from their
foreign loops, including when free-threaded Python inherits contextvars. It
requires `sys._is_gil_enabled()` to remain false in the free-threaded build.

The existing hash lock supplies native `cp314t` wheels for NumPy 2.3.4 and
pydantic-core 2.41.4/Pydantic 2.12.3. A fresh inventoried 3.14.5t package passes
`--expect-free-threaded` smoke with actual numerical work, validation, KV, SQL,
queue/cron, repeated alarm occurrences, exact receipts, restart and complete
joined reports. Its health reports both build ABI and actual runtime GIL state;
Cellar does not force unsupported extensions to run without a GIL. Independent
source/extension/package-root/libpython loss and tamper cases also pass and
restore the exact original environment. The full 3.14t install/rolling/watchdog command also passes archive restoration
with the original runtime absent, changed-source TERM and INT handoff, forced
termination of a stalled interpreter, joined replacement, exact receipt replay,
a new write and complete replacement handoff. The killed process produces no
clean report. All owned child processes have exited after the terminal result.

The regular CPython 3.14.6 full adapter suite and strict adapter Clippy also pass;
strict host+adapter Clippy passed for the free-threaded build. CI now independently
runs the whole workspace and package/restore/handoff/watchdog pipeline for both
`3.14` and `3.14t`. Local evidence does not substitute for fresh completed Linux
matrix jobs, and the broader remaining audit rows stay open.


Final free-threaded evidence uses inventory
`88c40a8298b4a1a27e700af59a2c751c6ea8af7712a480cf40dd49afaa6b8cba`
and the changed-source inventory
`75e5babf09ff50ab1367834f454ad73a70975926e2f32f2a71df16cd58cc1e22`.
The installed runtime/library is CPython 3.14.5t on macOS ARM64. No dependency
versions or lock hashes were relaxed. The final regular-mode suite uses Homebrew
CPython 3.14.6; its full adapter tests and Clippy pass after the reentrant-hook
change. The final free-threaded bootstrap repeat also passes exact concurrent
failure accounting. Five native archive tests, strict host/adapter Clippy,
format/layout (606 Rust files), the 415-package native dependency guard,
documentation generation/integrity, native-only compilation with a nonexistent
interpreter and `git diff --check` pass. The workflow parses with independent
3.14/3.14t matrix entries. Fresh Linux execution of the final source is pending;
pre-amendment workflows cannot qualify the new ABI support.

## Durable cron progress and independent interruption qualification

The Q08 regression exposed activation-local pending cron state: retiring a
scheduling cell after a failed expression discarded its retry identity and
re-armed the next calendar occurrence. The new native regression failed on that
behavior before the fix. Pending occurrence, expression indices, attempt and
in-flight progress now live in the scheduling cell's SQLite state. History and
expression progress commit in one synchronous transaction; the next alarm and
its owed set also commit together through the existing native output gate.
The pure scheduling/backoff policy and native retry ceiling are unchanged.
The configuration fingerprint excludes activation identity, but a changed
deployment/schedule intentionally replaces its old pending work. This uses a
format-1 document and no legacy state reader.

The native integration test now recreates a cell before a retry, interrupts a
resumed fan-out after earlier expressions completed, then checks that only the
remaining expression repeats with its original timestamp. Interrupted history
retains its null completion fields. The complete native cron test passes.

The independent Python `--runtime-schedule-recovery` fixture runs against real
UTC and separate private development fleets. A 1,500 ms native handler timeout
cancels the first attempt and refuses both late SQL mutation and `no_retry` as
`ContextExpired/not_committed`. Another owner is killed after a proof-gated SQL
effect, all its local cell files are removed, its lease expires, and a new
process recovers from the fleet bucket. Both cases retain the original
occurrence, exactly one SQL business effect and two recorded attempts. The
first history record stays `started`, the second completes, the calendar
resumes and the surviving process records complete joined shutdown. This
fixture passes on CPython 3.14.6 and free-threaded CPython 3.14.5t/macOS ARM64.
The final fixtures execute serially against real UTC, following the repository's
lease-sensitive test guidance. Both complete adapter suites pass, including
bootstrap and joined cleanup. Strict host/adapter Clippy, free-threaded adapter
Clippy, formatting/layout (608 Rust files), dependency and documentation guards
and the final native cron regression pass. Fresh Linux execution remains pending.

The first fixture configuration was refused because its recovery backoff did
not cover the lease TTL. A subsequent run with a shifted fixture clock exposed
that native alarm claims use real UTC; its retry deadline was outside the
shifted clock's observation window. The final fixture uses real UTC without
changing the production deadline/backoff policy or weakening occurrence,
history, authority or receipt assertions.

## Invocation cancellation and activation retirement (2026-10-20)

The independent `--runtime-lifetimes`, `--runtime-lifetimes-close` and
`--runtime-lifetimes-cancel` bridge subprocesses exercise real native nodes.
Two reproduced production defects are corrected:

- The idle pump retained its last reply registry entry and task iteration
  references. A cancelled task's traceback consequently retained a retired
  cell even after native ownership ended and explicit collection ran. The
  pump now releases those references. The regression observes weak references
  directly from the caller-owned Rust supervisor, without another Python
  handler that could overwrite the stale local and hide the leak.
- A self-cancelled Python handler closed its context but left the native
  driver asleep when an application or resident cell retained the bridge's
  sender. The root cancellation regression timed out against the original
  15-second native handler budget. A separate watch signal now wakes that
  driver immediately, independently of bounded mailbox capacity. Successfully
  enqueued completions retain their ordered mailbox path; ordinary bridge
  collection cannot close the driver's cancellation channel prematurely.

The lifecycle fixture forces resident pressure eviction and signed management
eviction, then checks actual collection, fresh instance identity, the next
ownership epoch and recovered acknowledged state. A native generation swap
preserves its existing handle/epoch while receiving a fresh Python object.
Independent simultaneously live fleets use the same application, scope and
cell name; copying identity metadata cannot transfer their live SDK handles or
native senders. Retained contexts are also refused after event completion,
generation replacement, forced native drain and process-global admission
closure. Identity allocation now refuses exhaustion instead of wrapping;
concurrent factory/invocation reservations are distinct.

During native shutdown, ingress closes before the final join. A held handler
produces the real incomplete native drain report, receives cancellation and
cannot submit late storage or namespace work. With both runtimes still alive,
its weak reference clears while Python is otherwise idle. Reopening the fleet
recovers the exact acknowledged value and permits new work. A separate global
raw-thread failure closes admission with two resident cells and an outstanding
handler; both native nodes join, actual objects collect and the combined report
preserves the runtime failure. The lease-fence fixture separately proves that
a task suppressing cancellation keeps its object alive until it exits, then
collects that object before the successor cell is invoked.

Self-cancellation is tested in a root handler, a cell handler and after an
acknowledged cell write. All finish before the unchanged native deadline,
refuse retained-context mutations and retain that write; cancellation does not
establish rollback. All fixtures check managed cleanup and settled operation
accounting. Unknown runtime test modes are now refused rather than silently
running the generic supervisor-failure fixture.

The complete standard CPython 3.14.6 and free-threaded 3.14.5t adapter suites
and strict adapter Clippy pass. The negative misspelled-fixture check also
refuses the unknown mode with a failing exit status.
Formatting/layout (613 Rust files), native dependency and documentation guards
pass. Native clock-refusal diagnostics now identify the initial response and
specific capture/release/cancellation stage without changing an assertion,
deadline or retry policy; the exact affected recovery test and strict native
test-target Clippy pass locally. Linux evidence for these changes remains Q12.

## Resource saturation and failed cleanup (2026-10-20)

The new capacity subprocesses independently fill invocation admission, managed
tasks, resident objects, aggregate byte reservations and native reply capacity.
They use real nodes and retain complete operation/reservation accounting. The
operation mailbox now has an explicit `Options::max_pending_operations` limit;
zero and capacities unsupported by Tokio are refused before initialization.
An owning-boundary test fills a two-entry mailbox, refuses the next event,
reuses released capacity and closes the receiver; every accepted and refused
event releases its owned byte reservation.

Three production defects have direct failing-before/passing-after regressions:

- At a four-task cap, an ordinary callback scheduled with an empty Context
  created a fifth task because the factory checked the budget only when an
  invocation ContextVar was present. Factory admission is now independent of
  that variable. Separate bounded supervisor tasks can still join existing
  work while the application budget is full; they do not exempt application
  callbacks from the factory's limit.
- A queued invocation filled all 4,096 bridge bytes. Another handler's normal
  completion and fixed failure envelope were both refused, and the uncaught
  transport error stopped the entire runtime. That failure now expires the
  invocation and wakes its native driver through the existing cancellation
  signal. The queued invocation and a later healthy request both succeed; the
  refused response cannot fabricate success. Byte calibration reaches exactly
  4,096 without changing the configured budget or bypassing ordinary admission.
- Background tasks raised exceptions from their cancellation cleanup. Their
  exceptions were retrieved but discarded, yielding `Report::complete()` with
  `runtime_failed=false` and all pending counts zero. Such failures now set
  `runtime_failed` and fixed stage `task_cleanup`; the combined report remains
  incomplete without copying private exception text. A separate full-budget
  case retains three genuinely unjoined tasks and records the bounded extra
  supervisor work rather than claiming complete drain.

Additional cases admit exactly two root invocations, refuse the third before
Python entry and then reuse the released permit. Exactly two resident cell
objects admit constructors; the third is refused, signed native eviction
collects one object and the third can then be created. A blocked Python loop
lets exactly two native replies accumulate; the third reply fails, accounting
settles and a later request succeeds. The completion-envelope test independently
accepts exactly 65,536 encoded bytes, rejects 65,537 and accepts a subsequent
valid completion while both body and header limits fit.

The complete final adapter suites, all seven capacity cases, nine unit tests,
bootstrap and doctests pass on standard CPython 3.14.6 and free-threaded
CPython 3.14.5t/macOS ARM64. Strict adapter Clippy passes in both modes.
CI now gives the adapter its own early step in each mode; the subsequent native
workspace test step excludes that already tested package while workspace
doctests and Clippy still cover all packages/features. Both matrix entries
and separate test coverage are checked from the parsed workflow.
SDK-result envelope/depth boundaries, broader typed-error/routing races,
external native prerequisites and final Linux execution remain open.

Linux PR job `118840183160` at `bbb992c` subsequently failed 2/87 native-node
cases. `storage_isolated_event_owner_expires_and_recovers_before_replay` lost its
successor to `NodeLeaseExpired`, then its client observed connection refusal.
`live_foreign_recovery_resumes_after_replaced_claimed_metadata_returns` failed
before the injected fault: the new diagnostic proves its initial consumer
response was `{"error":"Unavailable"}`, rather than an empty successful record.
The complete log is retained at
`/tmp/cellar-6a8d-ci-39484178821-standard-failure.log`. A claiming consumer fetch
must not be blindly retried as a read-only probe. The free-threaded PR job and
both push jobs were still live when this failure was recorded. These failures
remain Q12; their assertions/deadlines are unchanged.


## SDK result boundaries and milestone priorities checkpoint

The milestone deliverables are implemented across the supported core surface;
remaining acceptance gates are explicitly ordered in Current work. Optional
ASGI/streaming remain deferred. The original milestone table now links to this
status and distinguishes implementation from final-source qualification.

A new real-node result fixture failed before the fix: a 65,537-byte SDK read
terminated its native invocation and produced HTTP 500 instead of a catchable
SDK error. The retained reproduction is
`/tmp/cellar-6a8d-sdk-results-before.log`. Native output also lacked the facade's
64-level result-value depth boundary.

Result handling now validates depth before wrapping the transport envelope and
serializes through a bounded writer. Unrepresentable results return fixed
`HostError/unknown` for known reads or `Ambiguous/unknown` for possible mutations,
without abandoning an otherwise usable invocation. This happens after native
execution and must never carry the pre-effect `Validation` marker. A minimum
128-byte value budget is validated before bootstrap so fixed errors can fit;
aggregate bridge/reply admission remains independently bounded and fallible.
Native-operation success metrics still measure native execution.

Direct evidence includes exact/over 65,536/65,537-byte read results, depth 64/65
values seeded by a real native writer, repeated refusal followed by valid reads,
and SQL `INSERT ... RETURNING` whose oversized reply refuses while exactly one
row remains. Explicit sync then succeeds. UTF-8/escaping unit boundaries and
minimum-size typed failure envelopes pass. Combined reports complete with
settled operation accounting and no retained reservations.

Final local checks for this checkpoint:

- `cargo test -p cellar-python --features embed --locked` passes with conventional
  CPython 3.14.6 and independent free-threaded CPython 3.14.5t on macOS ARM64:
  10 library tests, the complete real-node/disposable-process bridge harness,
  isolated bootstrap matrix and doctests. Logs are
  `/tmp/cellar-6a8d-sdk-results-regular.log` and
  `/tmp/cellar-6a8d-sdk-results-ft.log`.
- Adapter all-target/all-feature Clippy with warnings denied passes in both ABIs.
- Native-only `cellar`/`cellar-python` check with a nonexistent interpreter passes.
- Format/layout checks cover 615 Rust files; the all-feature dependency check
  reports 415 packages and no JavaScript engine; documentation and diff checks pass.

H02 is verified in this local scope. Q01 retains measured default-limit selection;
A08/A12, H04/H06, P02 and Q12 remain open. Earlier Linux jobs exercise `bbb992c`,
not these result/resource changes, and cannot close their final-source gate.

Both provisioned CPython 3.14.5/3.14.5t environments also pass the latest-source
package smoke with cron: pinned Pydantic/NumPy execution, durable receipt replay,
KV/SQL/queue/alarm behavior, preserving restart and complete joined reports.
The free-threaded run asserts that native-library work leaves the GIL disabled.
The conventional environment initially refused its stale source inventory;
`/tmp/cellar-6a8d-sdk-results-package-regular-before.log` retains the refusal.
Only `source.sha256` differs in the recaptured inventory; all 3,289 runtime/package
file entries are identical. A new inventory/archive/project generation passes
without changing the original artifacts or disabling verification. Successful
logs are `/tmp/cellar-6a8d-sdk-results-package-regular-after.log` and
`/tmp/cellar-6a8d-sdk-results-package-ft.log`.

At `bbb992c`, standard Linux push job `118840158738` completed every workflow
step, including 87/87 native-node cases, adapter tests/lints, baseline harness,
installed libraries/inventory refusal/archive recovery, changed-source rolling
processes, supervisor restart and native examples. Its complete log is retained
at `/tmp/cellar-6a8d-ci-39484171753-standard-success.log`. This does not explain
the same-source standard PR job's two native failures. Both free-threaded jobs
passed their workspace and installed-Python package/recovery steps and reached
native examples; their terminal outcomes must be recorded separately. None of
these earlier-source results qualifies the later resource/result fixes on Linux.


The same `bbb992c` Linux push workflow `39484171753` subsequently completed
successfully in both ABIs. Free-threaded job `118840158260` also passed all 87
native-node tests, the full adapter/workspace checks, installed 3.14.5t libraries,
inventory loss/tamper cases, archive recovery, TERM/INT replacement, forced-KILL
supervisor restart and native examples. Its full log is
`/tmp/cellar-6a8d-ci-39484171753-ft-success.log`. The same-source standard PR
failure remains preserved and unresolved; newer source still needs Q12.


## Original-turn authority and nested-call qualification checkpoint

Eight new disposable real-node cases cover the original Rust invocation before
native execution and after a native write, while sync/output proof is pending.
They keep SDK futures inside their original installed turn; caller-owned Python
fixture controls only observe flags and release barriers.

- [Queued authority fixtures](../crates/cellar-python/tests/support/authority.rs)
  synchronize between complete polls of the original native future. After the
  Python handler enters, the supervisor closes that poll gate and releases its
  real SDK mutation. Bridge counters prove admission increased by one, queued
  work is exactly one and started work did not increase. Native deadline or an
  actual lease mismatch drops it before execution. A cancellation-suppressing
  Python frame then attempts both facade and raw-sender writes: both refuse
  `ContextExpired`. The acknowledged seed remains 7 and all three attempted
  mutation keys remain absent after local or successor recovery. Exactly one
  queued observation is abandoned and every reservation settles.
- [Proof authority fixtures](../crates/cellar-python/tests/support/proof_authority.rs)
  use three nodes and the real authenticated peer transport and bucket. Both
  proof paths are held after a real native mutation. Separate sync/output cases
  prove the request stays pending, then interrupt it by deadline or actual
  lease mismatch. Reopening proof paths cannot resurrect the successful reply
  or Python authority. Facade/raw late writes refuse, the handler attempt count
  stays one, acknowledged seed state survives and the unacknowledged pending
  value may be absent or 1, never interpreted as rollback or replay.
- [Call-cycle fixtures](../crates/cellar-python/tests/support/cycles.rs) perform
  real A→B→A routing. The inner A uses the same Python object and activation
  identity while the outer A awaits B, yet cannot use its distinct invocation's
  context through either bridge path. All three own-context writes complete.
  Independent deadline/fence cases suspend four original contexts, observe all
  losing authority before releasing retained Python frames, refuse late writes
  and preserve the acknowledged cycle state after recovery.

Native `Context::check_turn` still checks the installed turn, execution domain,
retirement, activation epoch and source generation. `storage.apply` retains a
Rust-only synchronous transaction; its native `Guard::finish` rechecks
`activation_scope` before commit. Python callbacks/awaits are absent from that
critical section. No SDK executor or production authority hook was introduced.

The initial cycle-successor fixture omitted the required recovery backoff and
correctly refused startup; its configured rerun passes. The first logged-node
fence fixture changed its generation to an invented session; recovery correctly
refused missing true witnesses. The retained log is
`/tmp/cellar-6a8d-proof-authority-sync-fence-first.log`. Logged-node fixtures now
change the published address to trigger `NodeLeaseMismatch` while preserving
actual generation/log provenance. They wait out the published lease before
recovery. Original non-log generation-mismatch tests remain intact through the
shared fault helper. No production recovery assertion or guard was weakened.

Qualification:

- Full `cargo test -p cellar-python --features embed --locked` passes with
  conventional CPython 3.14.6 and free-threaded CPython 3.14.5t on macOS ARM64:
  10 library tests, the full bridge harness including all eight cases, isolated
  bootstrap and doctests. Logs: `/tmp/cellar-6a8d-authority-regular.log` and
  `/tmp/cellar-6a8d-authority-ft.log`.
- Strict all-target/all-feature adapter Clippy passes in both ABIs. The single
  redundant result wrapper found by Clippy was removed without a lint suppression.
- Format/layout checks cover 619 Rust files. The dependency graph still has
  415 packages and no JavaScript engine. Documentation and diff checks pass.

A08/A12 are verified in this local scope. H04/H06, P02, Q01 and Q12 remain open;
the currently published Linux runs test `0bfd83f`, before these new fixtures.


## Typed native outcomes and current milestone priorities checkpoint

The real residency-capacity fixture holds cell A active with a one-resident
limit. A separate invocation calls cold cell B and receives the native typed
capacity refusal as `Overloaded/unknown`; B never enters Python. The caller can
still read its host variable and issue another mutation-class SDK call to A,
proving its reservation was released. An explicit fresh request to B succeeds
once capacity is free, with exactly one B handler entry. There is no automatic
replay and no inference that an arbitrary routed refusal proves rollback.

The three-node proof fixture now also fails both authenticated peer and bucket
proof paths while the caller remains live. Python catches `Ambiguous/unknown`,
performs a host read and returns the classified result. The callee runs once;
late facade/raw-sender writes refuse, acknowledged seed state survives and the
unacknowledged pending write is not claimed to have rolled back. The initial
fixture had a 30-second backend proof deadline but a 10-second caller budget,
so caller cancellation correctly won before classification. Keeping proof at
2 seconds for this failure-only case makes the intended live-caller gate
observable. Cancellation/fence cases retain their original 30-second proof
budget. Failed timing experiments remain in the local typed-proof logs.

Both full adapter suites and strict all-target/all-feature adapter Clippy pass
on macOS ARM64 with CPython 3.14.6 and independent CPython 3.14.5t. The final
same-caller reservation-reuse addition passes targeted routing tests in both
ABIs. The exact native clock-refusal test and strict native-node test-target
Clippy pass. Format/layout cover 620 Rust files; the dependency guard reports
415 packages and no JavaScript engine. Evidence is retained in
`/tmp/cellar-6a8d-typed-{regular,ft}.log`, their Clippy logs,
`/tmp/cellar-6a8d-routing-reuse-{regular,ft}-final.log` and
`/tmp/cellar-6a8d-native-clock-refusal-message.log`.

The native event-fetch fixture now prints the existing typed `Unavailable`
message on refusal to distinguish admission, identity-resolution and RPC causes.
Its response, assertions, deadlines and mutation policy are unchanged. This is
diagnostic evidence collection, not a fix for the preserved intermittent CI
failures.

Both Linux workflows at published `0bfd83f` completed successfully:
[push 39493103694](https://github.com/crabbuild/cellar/actions/runs/39493103694)
and [PR 39493117351](https://github.com/crabbuild/cellar/actions/runs/39493117351).
All four complete job logs are retained locally. They cover conventional and
free-threaded 3.14.8 adapter builds, installed 3.14.5/3.14.5t libraries and all
workflow package/recovery/replacement/supervisor/native-example gates. They
qualify that revision, not the later authority/cycle and typed-error fixtures.
Earlier failed runs remain unexplained and are preserved above.

H04 now has direct local evidence. The open requirement rows are H06, P02,
Q01 and Q12. Current work orders reliability diagnosis, bootstrap privacy,
external prerequisite attestation, measured defaults and final-source release
evidence. Optional ASGI/streaming do not take priority over these gates.


## Managed bootstrap and response privacy checkpoint

The sentinel regression reproduced retained private values in relative, missing,
wrong-kind and NUL-containing interpreter paths, SOABI mismatch and expected/actual
distribution-version diagnostics. Path checks now refuse invalid input before
CString construction and replace filesystem errors without retaining their
sources. ABI and pin mismatches use fixed messages. CPython initialization no
longer formats the raw configuration diagnostic. Inventory verification retains
its strict checks but drops underlying error sources at the runtime boundary.

The first private distribution-name fixture used an unnormalized dist-info
folder and correctly reached the existing sanitized missing-distribution error.
Normalizing its folder tests a genuine version mismatch; a separate missing-pin
case preserves the original sanitized PyErr path. Empty stdlib roots and a real
encodings import raising a private exception both refuse initialization without
printing their sentinel. All private errors have one source-free message of at
most 128 bytes. Existing imports/descriptors/custom exception __str__ and raw
thread/destructor hooks remain qualified.

A second real-node regression exposed a public HTTP leak: malformed response
headers reached Serde, whose error quoted the private value, and native ingress
formatted the handler's complete error chain. Completion decoding and response
conversion now replace their errors with fixed stages before returning to native
ingress. The native authority checks and output-proof gates remain unchanged.
The regression separately checks bounded response bodies, headers and child logs;
valid subsequent requests still succeed. Nested cell cases write before failing,
reach the live caller as Ambiguous/unknown and retain exactly one write per call.
The local native raw-header transport case is tested separately from public HTTP
wire rejection; redaction does not reinterpret legitimate application data as a
proven refusal. Exact persisted effects and settled accounting rule out rollback
claims or automatic replay.

Final checks on macOS ARM64 pass:

- Complete adapter unit/bridge/bootstrap/doctest suites and strict all-target,
  all-feature Clippy with CPython 3.14.6 and independent CPython 3.14.5t. Both use
  the CI --test-threads=1 invocation; the harness still runs all 33 startup cases.
- Real native CLI builds of inventory-bound CPython 3.14.5/3.14.5t packages, then
  valid/changed source/changed extension/missing source/extension/package root/
  libpython/unlisted-file/restored-runtime drills. For runtime-managed failures,
  the entire captured diagnostic omits inventoried private paths. The OS loader
  can diagnose missing libpython before main; the runtime cannot intercept that
  process output. Arbitrary application/library output is likewise not sandboxed.
- Native-only library checks pass with a nonexistent interpreter. Format/layout
  cover 621 Rust files; the dependency graph remains 415 packages with no
  JavaScript engine. Documentation and diff checks pass.

Logs are /tmp/cellar-6a8d-privacy-{regular,ft}-suite-final.log, corresponding
Clippy logs, package-build logs and inventory logs. Earlier failing regressions
remain in `/tmp/cellar-6a8d-private-path-before.log`, the
`/tmp/cellar-6a8d-private-pin-{expected,actual}-before.log` files and
`/tmp/cellar-6a8d-privacy-response-sentinel-before.log`. No temporary diagnostic
probe remains. The initial nested raw-header fixture expectation also remains
in `/tmp/cellar-6a8d-privacy-response-nested-ft.log`; its transport-versus-wire
assumption was corrected without changing native header behavior.

H06 is verified in this local managed-error scope. P02, Q01 and Q12 remain open.
At published c5acacc, all four Linux adapter steps passed the authority/cycle and
typed-error fixtures; both workflows still run their native workspace/lint gates.
These results do not qualify the later privacy changes. Do not cancel useful
qualification merely to publish this next local commit.

The next P02 survey finds 30 native binaries and 23 distinct direct OS-library
references in each macOS package. Canonicalizing /tmp to /private/tmp removes a
false external-libpython classification. Many actual OS libraries are supplied
by the dyld shared cache (the SDK documents that their disk paths can be absent).
This survey is not transitive prerequisite attestation; P02 remains open.


## Native prerequisite implementation checkpoint (qualification in progress)

Required prerequisite records now extend format marker 1 without a legacy
reader/default. The data contract identifies a qualified OS cohort, canonical
external file sizes/SHA-256s, cache providers, resolved dependencies and the
compiled host's dependencies without binding its build path. Provider lists
retain alternatives from distinct inherited load contexts.

Capture inspects every bundled native binary plus the selected host. GNU ELF
uses readelf and the qualified loader's --list mode with explicit runtime search
roots; Mach-O uses SDK otool metadata, inherited rpaths and weak-load semantics.
A private C constructor sentinel remains absent during inspection. Additional
runtime-loaded libraries can be declared explicitly. macOS provider identity
includes machine, OS build and the actual shared-cache UUID; SDK-compiled C reads
our own kernel-provided dyld metadata with size/version guards. Runtime checks
need no compiler, inspection tool, package installer or application import.

Rust validates resource bounds and graph providers, checks all bundled native
files have records, verifies external prerequisites before installation and
initialization, and verifies actual loaded providers before initialization and
again after application imports/before Rust host admission. Darwin add/remove
callbacks copy owned names and perform no I/O/Python/SDK operations; they avoid
racing the public image-count/name iterator. Linux uses dl_iterate_phdr. Native-only
hosts acquire no Python dependency; the new C helper is OS introspection only.

Local evidence so far:

- Seven focused protocol/native tests pass, including real unlisted dlopen
  refusal, explicit qualification and removal on dlclose; changed cohort,
  missing/tampered external files and missing native graph records refuse.
  The first graph fixture had an incorrect executable bit; matching the actual
  file mode and asserting the dependency-record error fixes the fixture without
  weakening the file verifier. The earlier failed log is preserved.
- Four capture tests compile real C executables/libraries and prove no inspected
  constructor runs. They cover checksum/loss refusal, transitive inherited
  search paths, two distinct parent contexts selecting different approved
  providers for one dependency, and OS-cohort refusal.
- Native archive/prerequisite unit tests and the CLI build pass. Conventional
  CPython 3.14.5 captures 3,289 files under the new schema; inventory digest is
  ce0242ae333547eae67b61dc1aa23f5aee553466f5bc9b289a4770ab0c41a9ea.
  Its bound package, complete inventory fault drill and full example smoke with
  cron, preserving restart, exact receipts and clean joins pass.
- The compiled conventional bootstrap guard proves valid source/host execution
  and complete join. Wrong OS/cache identity, missing/tampered external native
  library and missing dependency record refuse before both source side effect
  and Rust host entry; error chains remain sanitized. Logs are
  /tmp/cellar-6a8d-prerequisites-guard-regular.log and its private build directory.

The free-threaded counterpart subsequently passed the bound package, inventory
fault drill, full cron/restart/receipt smoke and compiled bootstrap guards. Its
terminal log is /tmp/cellar-6a8d-prerequisites-guard-ft.log. P02 remains partial until
both ABI/package modes, strict lints/layout, restoration/replacement and Linux
GNU-loader execution qualify the implementation. Older inventories are retained
as evidence and deliberately refused by the new required metadata. No Cellar
format marker was incremented.

Both c5acacc Linux workflows 39505176770/39505187876 completed successfully in
both ABIs. All four full logs are retained under /tmp/cellar-6a8d-ci-395051*-success.log.
These cover the full authority/cycle/error/native 87/87/package/recovery/rolling/
supervisor/native-example gates, not the subsequent privacy/prerequisite source.
The tested privacy commit e9b8d15 is published to draft PR92; its workflows
39514545895/39514550654 both failed in both ABIs. The push conventional job reports a
`--runtime-delivery` host deadline error; the PR conventional job and both
free-threaded jobs report a private bootstrap sentinel leak in
`private-init-failure`. All four full failure
logs are retained under /tmp/cellar-6a8d-ci-395145*-failure.log. H06 is reopened;
preserved earlier intermittent native failures remain unexplained, and
P02/Q01/Q12 remain open. No final-source acceptance is implied by older green CI.

## CPython 3.14.8 raw diagnostic regression (local correction)

The failed Linux bootstrap privacy check is a real micro-version difference.
CPython 3.14.6 returns a codec initialization error without a path dump;
3.14.8's Python/codecs.c calls _Py_DumpPathConfig unconditionally when importing
encodings fails. That function writes configured paths through C stderr before
Python exception hooks exist, ignoring pathconfig_warnings=0. A fresh local
shared CPython 3.14.8 build from the official source reproduces the leak with a
deliberately failing builtin codec initializer. The original failing log is
/tmp/cellar-6a8d-bootstrap-3148-before.log; only the synthetic private sentinel
is redacted in the test's failure diagnostic.

A scoped C-initialization stderr sink now flushes existing host output, discards
raw bootstrap diagnostics, and restores the original descriptor/flags before
application imports or host startup. A closed stderr remains closed. The
process-wide interval and prohibition on concurrent descriptor replacement are
documented in the public safety contract and ADR. Restoration failure refuses
startup; normal later application/library output remains process-owned.

All 36 disposable startup scenarios pass with the fresh CPython 3.14.8 build,
including the actual codec failure, bad standard-library roots/imports,
closed/cloexec stderr, stdout/stderr identity/flag preservation after success and
failure, and Python stderr use during application import. Logs are
/tmp/cellar-6a8d-bootstrap-3148-after.log and
/tmp/cellar-6a8d-bootstrap-3148-suite.log. Subsequent complete conventional
3.14.8 and free-threaded 3.14.5t adapter suites pass, including all bootstrap,
authority/cycle/proof, binding/cron, lifecycle, watchdog and core HTTP/storage
cases. Strict all-target/all-feature Clippy passes for both affected crates on
3.14.8 and for the adapter on 3.14.5t. Format/check/layout cover 628 Rust files;
the dependency graph remains 415 packages without JavaScript. Logs use
/tmp/cellar-6a8d-prerequisites-bootstrap-{3148,ft}-suite-final.log and corresponding
Clippy logs. Linux execution remains required before H06 closes; native and
new-schema installed-package qualification are continuing.

The delivery host deadline is not yet localized: the combined host_failed flag
does not establish that shutdown itself timed out. Permanent stage context now
distinguishes consumer admission, initial/replacement node admission and each
preserving shutdown. The unchanged-deadline local delivery drill passes, which
is diagnostic evidence only and does not explain the preserved Linux failure.

## Current prerequisite packaging and qualification checkpoint

Both conventional/free-threaded CPython 3.14.5 packages now pass current-source
native-prerequisite refusal guards, inventory mutation/loss, original-runtime
removal and archive restoration, clean-environment real native-library execution,
exact receipt replay, changed-source TERM/INT handoff and forced-KILL supervised
restart. Logs are under
/private/tmp/cellar-6a8d-qualified-prerequisites-bootstrap-{regular,ft}/. The
conventional successful installed-package log has the suffix -profile-matched;
the original failed log is preserved separately. All native library/tests pass,
including node 87/87; checker changes also pass strict all-target/all-feature
Clippy. No cellar_internal_tests cfg was enabled.

The first conventional INT run exceeded the unchanged 15-second package-check
deadline. The old helper discarded child output, so its exact checker role is
unavailable. A separate controlled comparison finds that the private rolling
workspace did not inherit the repository's SHA2/RSA development profiles.
CPU samples place the slow child in SHA256 software compression during inventory
verification. Identical Python source/inventory/archive checks take 13.3–13.6 s
(about 12.9 CPU-s) in the unmatched fixture and 1.4–1.9 s (about 1.2 CPU-s) with
matching profiles. Both complete handoff/supervisor scenarios then pass, with
actual checker times about 1.4–1.5 s and no deadline/assertion changes. The
[raw diagnostic comparison](evidence/python-checker-profile-2026-10-20.json)
identifies the measured binaries and scope. Checker failures now retain output
and report host/package role, expected acceptance and elapsed time.

Qualification-client checks also no longer depend on Python assert statements.
With -O, the old inventory tool both bypasses its filesystem-root guard and
reports PASS for a dummy host that accepts every corrupted artifact. Two complete
CLI regressions fail against the previous tool and pass after unconditional
checks; mutated private fixture contents are restored. The real free-threaded
inventory mutation/loss drill also passes under -O. Logs are
/tmp/cellar-6a8d-qualification-guards-{before-complete,after}.log and
/tmp/cellar-6a8d-inventory-optimized-ft.log.

Bootstrap correction 9143fef is published to draft PR92. All four adapter jobs in
Linux workflows 39520485909/39520490587 pass, including the previously failing
privacy cases; complete workflow qualification is still running. These jobs
do not qualify the later local prerequisite implementation. P02, measured
shipping defaults (Q01) and preserved deadline/final-source qualification (Q12)
remain open; H06 retains its complete packaged/platform qualification gate.

## Shipping-default workload harness checkpoint

The private baseline host now has a separate `python-defaults` mode using the
actual unchanged Python default options. Its native stateless pool is explicitly
raised to 256 with no admission wait to isolate the Python invocation cap; all
effective Python/native fixture limits are recorded. The default-limit driver
rotates echo, checked durable counters and bounded CPU work at increasing
concurrency, tests exact/next-byte request bodies, directly observes all 128
managed invocations held before refusing sixteen excess requests, then requires
capacity reuse. Every acknowledged counter survives owner kill and local disk
deletion, followed by a new write and complete combined join.

Conventional and GIL-off free-threaded CPython 3.14.5 smoke runs pass. The latter
includes concurrency 1/4/16/32; the initial conventional smoke uses 1/4. Both
exercise the actual default body/invocation caps. Six report CLI tests pass,
including under `-O`: valid evidence is accepted while corrupted receipts,
percentiles, saturation observations, native drain and pending operations are
refused. These smoke runs validate the harness, not capacity or production SLOs.
Repeated optimized mixed-load measurements and final-source Linux execution
remain required before closing Q01/Q12. See the [harness guide](../tools/python/baseline.md#shipping-python-defaults).

At clean source 7d90f0c, all six optimized runs now complete: three repetitions
per ABI, each with concurrency 1/4/16/32 and forty mixed operations per worker.
All 12,720 mixed requests and 3,180 acknowledged durable updates pass; six
killed-owner/local-disk-deletion takeovers retain the exact ledger and permit new
writes. Actual default body boundaries and 128 held invocations pass in every
case, including all 96 excess refusals, capacity reuse and combined joins.
The [recorded measurements](007-embedded-python-baselines.md#shipping-default-mixed-workload-qualification)
include compressed raw evidence, source/binary digests, memory/copy observations
and rising latency tails. They establish the stated development workload,
not production capacity or a bound on arbitrary Python heap/native pools.

The exact new CI command also passes locally with a debug free-threaded host;
all six CLI verifier tests pass on both measured and debug datasets, under
ordinary Python and `-O`. The existing native/Python baseline smoke and its
report verifier also pass with newly built debug hosts. Strict example Clippy
passes for embedded free-threaded and native-only modes; the latter builds with
a nonexistent interpreter. Format/layout, dependency graph and docs checks pass.
Linux execution of the new workload, external-prerequisite schema and the
preserved failure diagnoses remain open. The earlier 9143fef free-threaded push
job is terminal success; the three other jobs are still running native examples.

## Actual loaded-library threading ABI correction

Both published 9143fef Linux workflows 39520485909/39520490587 now complete
successfully in conventional/free-threaded modes, including native 87/87, full
adapter, package/archive/replacement/supervisor and native-example qualification.
All four full logs are retained under `/tmp/cellar-6a8d-ci-395204*-full.log`.
Those runs do not qualify the subsequent local prerequisite and workload source.

A separate audit found that the runtime checked the loaded threading ABI through
PyO3 only after constructing Python objects. An actual free-threaded library
aliased into a conventional unbound host reproduces SIGSEGV (-11), before host
admission. Metadata-only SOABI mismatch tests did not expose this failure.
The correction uses opaque `PyInitConfig_HasOption("enable_gil")` before C
initialization or PyO3 attachment. CPython 3.14 includes that option only for
`Py_GIL_DISABLED` builds, so the guard can compare the actual provider with the
compiled threading ABI without reading Python object layouts.

The [raw mismatch/control evidence](evidence/python-loaded-abi-2026-10-20.json)
retains binary/library/source digests, actual matched runtime/GIL states,
combined matched shutdown reports, the original signal failure, and both
corrected opposite-library refusals. Both corrected children exit normally with
the fixed ABI failure and no readiness/complete files; each matched control
runs and completely joins. No original runtime file was replaced. The permanent
tool fails against the original conventional binary and passes the correction.
Linux CI separately provisions the opposite pinned runtime and runs the actual
loader drill under `-O` in both matrix modes. Local full adapter/startup tests
and strict all-target/all-feature Clippy pass in both threading ABIs. The final
CLI regression also passes under `-O` in both directions and still rejects the
original conventional binary with SIGSEGV; format/layout, docs and the native
dependency graph checks pass.
This is a threading-ABI check, not a restriction to GIL-enabled Python.

## GNU interpreter alias capture correction

Both b4c228c Linux workflows 39529949531/39529953263 fail in all four jobs at
the real native-prerequisite capture tests, before adapter/package qualification.
Every case refuses libc's required `ld-linux-x86-64.so.2` dependency. Full logs
are retained under `/tmp/cellar-6a8d-ci-395299*-full.log`.

The same four failures reproduce with a real GNU loader in a private Debian
ARM64 container. Resolving PT_INTERP to a canonical loader path causes GNU
`--list` to emit `/lib/.../ld-linux... => /usr/lib/.../ld-linux...`, while libc
DT_NEEDED requests its bare SONAME. The parser retained the absolute alias but
lost that name. A focused real C/ELF regression fails before the correction on
both Debian ARM64 and Ubuntu 22.04 x86_64. The correction retains the known
qualified interpreter's basename in either listing form, without inventing
basename aliases for unrelated absolute libraries.

All five GNU capture tests now pass on both Linux environments; four macOS
capture tests also pass, with the GNU-only regression explicitly skipped. The
[raw functional evidence](evidence/python-gnu-loader-alias-2026-10-20.json)
includes source/image/loader digests, actual loader output, the before/after map,
the completed libc dependency edge and refusal/constructor tests. The amd64
container uses QEMU, so it establishes parser behavior, not native performance.
No original runtime, live fleet or unrelated container was modified.

The first Ubuntu probe used stock Python 3.10 and correctly could not run the
3.11+ tooling's `hashlib.file_digest`; its failure log is retained separately.
The x86_64 regression/full suite above use provisioned CPython 3.14.5. Provisioning
commands now run inventory tooling with the selected managed interpreter and
state the tooling requirement. These results close this capture defect; the
complete Linux prerequisite installation, replacement and new adapter/default
qualification still require a fresh final-source workflow.

## Native lease attempt/readback budget correction (local qualification)

The preserved drain-token and successor-owner traces both show ambiguous renewals
with less than 250ms of proven authority left before self-fencing. The native effect adapter's
250ms minimum attempt timeout can consume that entire window, contradicting its
intent to leave time for ambiguity readback. The historical traces do not prove
whether those last CAS operations actually committed; that fact remains unknown.

A new public Node/SQLite regression reconstructs this failure class independently
of shutdown. Its first renewal times out before reaching the store. The next
renewal really commits, then its response is held beyond the old authority.
With 218ms left, the previous minimum keeps the attempt pending until the
watchdog fences, and the regression fails. Removing the minimum returns that
attempt as ambiguous after 109ms, with another 109ms available for ordinary
native readback. The node proves the actual stored renewal, remains live across
another full TTL, accepts the next acknowledged business write and completely
joins through the existing preserving/handoff report assertions. The 2.5s cap,
watchdog deadline, native guards and operation-specific proof rules remain intact.

The [raw controlled comparison](evidence/python-lease-readback-2026-10-20.json)
contains source digests, the original failing run and six successful corrected
runs. The private fixture uses a 900ms lease to make the timing window explicit;
no existing test assertion, request deadline or stop budget is extended. This
corrects a demonstrated availability defect without fabricating authority from
an ambiguous reply. The full local native suite passes, including native 88/88
and the original 75-second drain-token case; strict all-target/all-feature Clippy
also passes. The change is local and is not included in the terminal 3e56924
Linux workflows. Both local Python ABI adapter/startup suites and strict Clippy subsequently pass this native correction; Linux qualification remains open.
Earlier consumer
admission/delivery and clock-replay failures retain their separate diagnosis
requirements; this comparison does not claim to explain every historical failure.

## GNU dynamic dependency tokens and current milestone priorities

Both 3e56924 workflows 39531929061/39531931874 are terminal failures in all four
jobs. Adapter/bootstrap, default-load workloads, actual opposite-library ABI
guards and native tests/lints pass; package inventory capture then refuses a
native binary with no resolved dependency context. These runs supersede earlier
in-progress status, but do not provide complete installed-package acceptance.

A complete native Linux ARM64 reproduction with CPython 3.14.5t and pinned
Pydantic/NumPy isolates the unresolved library: the runtime's `libpython3.so`
shim requests `$ORIGIN/../lib/libpython3.14t.so.1.0` in DT_NEEDED, while GNU
`--list` reports the expanded absolute provider. The local correction expands
GNU ORIGIN/LIB/PLATFORM tokens, preserves the original dependency reference in
the inventory and accepts only a provider observed in that actual load context.
LIB/PLATFORM values come from the qualified loader's diagnostics with an empty
environment; full diagnostics are neither retained nor emitted. See GNU's
[documented diagnostic values](https://sourceware.org/glibc/manual/latest/html_node/Dynamic-Linker-Diagnostics-Values.html).

Real compiled C/ELF regressions fail in all six token spellings before the
correction. All six capture test methods pass afterward on native Debian ARM64
and Ubuntu 22.04 x86_64 under QEMU; macOS passes its four relevant methods with
two GNU-only methods skipped. The complete ARM64 runtime/package inventory now
captures and verifies 6,139 files. The full CPython x86_64 reproduction hits an
internal QEMU assertion and supplies no package qualification; real x86_64 CI
remains required. The [raw before/after evidence](evidence/python-gnu-needed-tokens-2026-10-20.json)
records source/image identities, runtime inventory identity and exact test logs.
This is capture/verification evidence using a C host, not the full Cellar
installation, archive restoration or replacement matrix.

The current priority is completing packaging and explaining preserved reliability
failures, then qualifying one final source in both ABIs. M0–M5 feature coverage
does not mean every acceptance gate passed. ASGI/streaming remain optional and
deferred while these core gates are open.


## Local qualification before publishing lease and GNU token corrections

The complete adapter/startup suites and strict all-target/all-feature Clippy now
pass serially with conventional CPython 3.14.5 and free-threaded 3.14.5t on macOS
ARM64 after the native lease correction. Both suites include the delivery,
cron timeout/owner-loss recovery, authority, proof-fault, capacity and lifecycle
harnesses. This is regression qualification; it does not explain the preserved
historical intermittent failures. Rust formatting and module layout pass for
629 files, and the all-feature native dependency graph has 415 reachable packages
with no JavaScript engine. The [raw local qualification](evidence/python-lease-token-qualification-2026-10-20.json)
records the selected runtimes, commands, source identities and terminal logs.

The next CI run moves the complete embedded-package step before the long native
workspace suite. All 20 step bodies remain byte-identical; no test, assertion,
deadline or feature mode is removed. Native workspace and Axum qualification
still run afterward. This gives the current packaging blocker a faster real
Linux feedback loop while retaining the final-source release gate.


## Linux package acceptance and reset/read qualification correction

At 97be2cc, the conventional push/PR jobs and free-threaded push job complete
`Exercise embedded Python package` successfully. That step runs the actual bound
host, inventory/prerequisite refusal guards, offline archive restoration, native
library work, exact receipt replay, changed-source TERM/INT handoff and forced-KILL
supervisor replacement. Both threading ABIs therefore have direct Linux evidence
for the GNU capture correction. All three jobs subsequently complete successfully,
including native-node 88/88, workspace tests/lints, Axum feature modes/generated
contracts and native examples. The fourth job retains the failure below. The
[terminal Linux evidence](evidence/python-linux-97be-2026-10-20.json) records all
four job outcomes and exact source/toolchain scope; later test/diagnostic changes
still require fresh qualification.

The PR free-threaded job 119023823453 instead fails earlier in
`--runtime-proof-authority-failure`: the final `/state` audit returns HTTP 503
after the deliberately failed proof. The original trace contains no actor-phase
snapshot. Five isolated original-case runs pass locally, and holding post-write
proof replies does not reproduce the problem; that prototype is discarded.

The decision core explicitly refuses requests queued during `StopCause::Reset`
when cleanup completes. A real public Cell wrapper now holds native activation
destruction on one caller-owned Rust worker while a read reaches that reset.
Releasing the real stop produces the same 503 and host-failed report under the
unchanged immediate-read audit. The corrected audit repeats only its fixed
read-only `/state` observation, with the complete observation bounded by the
original 20-second HTTP budget. Native proof (2 seconds), handler (10 seconds),
operation (30 seconds) and lease (5 seconds) budgets remain unchanged. This
corrects an assumption about immediate availability after asynchronous reset;
it does not add SDK mutation replay or bypass a native refusal.

The controlled case retains the first refused read, then proves acknowledged
seed state, no stale facade/raw-sender writes, one business handler attempt and
all three complete joined native reports. The destructor hold is bounded and
its timeout fails the case; the pending read is joined normally or aborted on
error before the caller-owned runtime joins. The complete adapter/bootstrap
suites and strict Clippy pass in both local CPython 3.14.5 ABIs, with three more
conventional controlled repetitions. Formatting/layout pass for 630 Rust files.
The [raw reset/read evidence](evidence/python-reset-read-2026-10-20.json) records
the Linux failure, controlled before/after runs, suite logs, source digests and
published package-step results. This demonstrates the matching failure class;
separate historical delivery/initial-consumer/clock failures remain unproven.
The correction is local and still requires publication and final-source Linux CI.


## Bounded native consumer failure diagnostics

The preserved initial consumer `Unavailable` refusal still has no reproduced
cause. Its fixture previously logged only `DeliveryError.message`, which can be
a fixed public envelope while a native admission/transport cause remains in the
error chain. The private fixture now logs that chain, bounded to 2,048 characters.
The public JSON response, fetch identity and mutation/replay policy are unchanged.
The [diagnostic qualification](evidence/python-consumer-diagnostics-2026-10-20.json)
records native-node 88/88, strict all-target/all-feature Clippy and format/layout
success on macOS ARM64. Both live-foreign recovery cases pass. These passing runs
do not exercise the failure branch or explain the historical refusal; the added
diagnostic is intended to distinguish the next actual occurrence.

## Isolated consumer investigation and milestone focus

The SDK workflow adds an opt-in `consumer-replay` dispatch mode for Linux. Each
iteration runs the exact preserved live-foreign recovery fixture against fresh
private state; the first failure stops the run. The bounded native cause and
failure JSON are retained. It does not retry a claiming operation in a live
application, change native deadlines or replace the full ABI matrix. A distinct
concurrency group allows investigation alongside complete qualification.

The [driver evidence](evidence/python-consumer-replay-driver-2026-10-20.json)
records local execution of the exact workflow driver: four invalid bounds refuse
before fixture creation, a real Cargo failure stops before the next iteration,
and two fresh native fixtures pass. This validates investigation controls on
macOS ARM64; Linux execution and reproduction remain open. The existing complete
qualification step bodies and 90-minute budget are unchanged.

The delivery deadline, initial consumer refusal and clock/replay failures remain
separate diagnosis gates. A successful fresh run supplies regression evidence;
it does not establish the cause of a preserved failure. The immediate focus is
M3 reliability and one final-source M0–M5 qualification matrix. New optional
ASGI/stream adapters remain deferred until these gates close.

## Published correction and exact clock/receipt replays

Source 85cdef4 publishes the reset/read correction, bounded native consumer cause
diagnostic and isolated Linux replay workflow. Complete Linux qualification is
submitted in [push 39548047580](https://github.com/crabbuild/cellar/actions/runs/39548047580)
and [PR 39548052722](https://github.com/crabbuild/cellar/actions/runs/39548052722).
The separate [100-fixture consumer investigation](https://github.com/crabbuild/cellar/actions/runs/39548056261)
uses that exact source. These jobs remain nonterminal at this checkpoint; their
submission does not close any acceptance gate.

The [raw clock/receipt replays](evidence/python-clock-replay-2026-10-20.json.gz)
record 20 serial executions of the exact preserved delayed-cold-read fixture on
macOS ARM64, using Rust 1.98.1 and fresh private state. All pass in 355.056 seconds
including Cargo startup. Each independently exercises the forced read deadline,
recovers the proven clock before receipt reuse and reaches checkpoint 2, exact
business total 3 and settled-through 2. Original assertions, native operation
deadlines and replay policy are unchanged. The artifact retains commands, source
and fixture identity, durations, log digests and full raw output. This campaign
does not reproduce or explain the historical Linux clock/replay failure; that
causal gate remains open alongside delivery and initial-consumer diagnosis.
