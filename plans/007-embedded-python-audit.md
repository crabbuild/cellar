# Embedded Python: implementation and completion audit

This ledger audits [plan 007](007-embedded-python-pyo3.md) against the actual
implementation. The objective remains the complete plan, including packaging,
failure qualification, observability and measured baselines. A runnable example
or passing happy-path smoke is insufficient to complete it. Explicitly deferred
features (ASGI, streaming, arbitrary transaction callbacks, untrusted-code
isolation and subinterpreters) remain deferred. Free-threaded CPython 3.14t was
subsequently requested and is an additional supported build/qualification scope.

Core deliverables now span M0–M5, including both CPython threading ABIs. The
full objective is **not achieved**: authority races, typed failures, loader
privacy, external native prerequisites, operating defaults and final-source
qualification retain open gates. A verified row covers only its explicitly
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
| A02 | Pin CPython 3.14 threading ABI and compatible bridge | Build-time ABI guard plus runtime preflight, pinned Cargo graph | verified: CPython/shared-library and selected-threading-ABI build guards, builtin ABI preflight, invalid-ABI startup refusal and exact PyO3/build-config 0.30.1 Cargo lock |
| A03 | Initialize before node; explicit home/import roots/site/env | Isolated configuration before Python code/imports; hostile environment tests | verified: explicit PyInitConfig + `startup` hostile-environment matrix |
| A04 | Process-owned loop and explicit joined Rust executors | Runtime source, concurrent await test and combined shutdown evidence | verified locally: process main-thread loop, explicit joined Rust supervisor, concurrent host-await progress and current combined two-/three-node stop reports |
| A05 | One coordinated signal owner | Disposable process INT/TERM tests; no competing interpreter handlers | verified locally: startup preserves existing native handlers and removes CPython implicit INT handling; real TERM/INT handoff with two compiled Python processes yields complete joined reports |
| A06 | No hidden runtime executes SDK operations | Source audit plus instrumented original-turn/domain tests | verified locally: operation futures remain inside the original native application driver; live cross-domain binding tests refuse handle exchange; driver polls nested calls without a separate SDK executor |
| A07 | Owned values only across thread/await boundary | Source review and lifecycle tests; no borrowed Python/SQLite guards | verified locally: owned strings, JSON, revocation flags, reservations and supported owned Py handles cross the bridge; native operations retain no Python token/borrow or SQLite guard across awaits; current lifecycle suite passes |
| A08 | Native authority checked at enqueue, execution, commit/output | Pending-operation cancellation and forced-fence tests | partial: native SDK checks retained; actual lease-fence/resumed-Python refusal is verified in L10; full queued/execution/commit race matrix remains |
| A09 | Two embedded domains cannot exchange live contexts | Simultaneously live contexts in two nodes: cross-use refused | verified: reproduced live cross-domain read, then blocked API and native sender in `bridge` |
| A10 | Opaque invocation identity; metadata cannot choose authority | Cross-event/domain/generation/activation negative tests | verified locally in both threading ABIs: simultaneous live same-name domains refuse cross-use through both facade and native sender even with copied identity metadata; retained event, generation and retired-activation contexts refuse mutations; native authority remains the original Rust Context |
| A11 | FIFO operations, one outstanding mutation per invocation | Concurrent mutation/read/cancel tests; explicit state machine | verified: native admission reservation rejects overlap; same-invocation read progresses during nested mutation; reservations release on drop |
| A12 | Mailbox pollable during nested calls; existing cycle behavior | Nested-call progress, cycle and cancellation tests | partial: driver now polls pending SDK futures and mailbox together; direct same-invocation progress proven; explicit cycle/fence matrix remains |
| A13 | Descriptor declares supported classes/events before admission | Cold validation and invalid registration tests | verified: cold descriptor async/constructor checks plus deployment hook rejecting absent classes, undeclared queue consumers and cron handlers before activation |
| A14 | Native factories never call/import Python | Source/caller audit plus blocked-interpreter factory test | verified locally: proxy creation uses only immutable Rust source descriptors, atomics and owned lifetime flags; in both CPU-loop and GIL-held C-call owner drills, a Rust root submits a new real cell activation and the native factory counter advances while Python remains blocked and its handler cannot complete |
| A15 | Activation identity includes domain, generation and epoch | Retire/recreate and simultaneous same-name/domain tests | verified locally in both threading ABIs: distinct simultaneous same-name domains, actual pressure/operator eviction with fresh instances and exact next epochs, same-epoch native generation swap with a fresh instance, state recovery and forced-fence successor collection; exhausted IDs refuse without wrapping and concurrent allocations remain unique |
| A16 | No interpreter-per-cell or hot Python replacement | Process-wide guard and attempted reload tests | verified locally and on Linux at 21f6b30: cell proxies share the process runtime, a second runtime is refused, live selector replacement is refused in process, and changed Python source executes only in the distinct replacement executable/process; trusted application mutation of Python globals is outside generation isolation |
| A17 | Support conventional and free-threaded CPython 3.14 ABIs without borrowed authority or implicit GIL dependency | Both ABI builds, actual GIL-off workers, concurrent hooks, mixed-ABI refusal and installed-package recovery | verified for macOS ARM64 and Linux x86_64: both local adapter suites pass; foreign-loop authority refusal, exact concurrent hook counts, mixed-layout/ABI refusal and archive restoration pass; both ac521f3 Linux free-threaded jobs completed successfully with CPython 3.14.8t adapter/native suites and installed 3.14.5t GIL-off libraries, archive recovery, changed-source TERM/INT handoff, forced-KILL supervisor/restart, receipts/new writes and complete drain; final lifecycle/cancellation source still requires Q12 |

## HTTP, codecs and failures (M1)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| H01 | Buffered bytes/JSON HTTP; ordered repeated headers | Byte and duplicate-header round trips through real ingress | verified: binary body and ordered repeated request/response header round trips through real ingress |
| H02 | Body/response/header/depth/value limits at input and output boundaries | Exact boundary and over-limit cases, released reservations | verified locally in both threading ABIs: real ingress/nested body, ordered headers, input depth, batch count/bytes and handler completions retain exact/next-unit cases; native-writer fixtures prove SDK success envelopes of 65,536 bytes pass and 65,537 refuse, result-value depths 64/65 pass/refuse, and later operations remain usable; bounded serialization returns HostError/unknown for reads or Ambiguous/unknown after a successful SQL insert without rollback or replay; complete reports retain zero leaked reservations; final Linux execution remains Q12 |
| H03 | Finite floats, integer range, keys, cycles and unsupported values | Codec boundary corpus and no mutation assertions | verified: real-node invalid-value corpus (nonfinite, int bounds, keys, unsupported, cycles, depth) has no mutation; valid boundaries round-trip |
| H04 | Typed validation, conflict, expired/ownership, overload, ambiguity | Public Python error tests and native cause classifications | partial: public ValidationError/Overloaded/Ambiguous/OwnershipLost classes, explicit outcomes and fixed sanitized messages; native typed routing verdicts preserved; real protocol/codec/batch refusals and ambiguous constructor/absent-binding tests pass; some bindings flatten native causes internally, and live routing-verdict fault coverage remains |
| H05 | Cancellation stays cancellation; no blind retry | Cancel-before/after submission and ambiguous-result reconciliation | verified locally: independent KV/shared-SQL/producer cancellation before submission and during withheld native proof stays CancelledError, failed submitted proof remains Ambiguous/unknown and the adapter attempt count stays one; explicit reconciliation retains singular business effects; native consumer timeout also refuses late ack before same-ID redelivery; packaged S09 covers lost proof-gated response reconciliation; Q08 independently qualifies cron interruption |
| H06 | Sanitized, bounded failures; no secrets/paths/stack locals | Sentinel request/exception/path tests on public response and logs | partial: managed handler, destructor and raw-thread failures sanitized and counted; import/registration/descriptor errors expose fixed stage messages without a Python exception source; subprocess tests refuse exception/path sentinels and arbitrary exception `__str__` execution; source and filename NULs are refused before CString construction (regression proved retained source bytes before the fix); initialization/loader and complete error-path review remain |
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
| L01 | Root cancellation revokes SDK admission immediately | Queued/late writes refused before execution and after fence | verified locally for managed cancellation in both threading ABIs: handler return/abandoned work, native timeout and lease fence expire SDK authority; self-cancelled root/cell handlers wake the original native driver despite retained senders and refuse late mutations, including after a committed write; full execution/commit race coverage remains A08 |
| L02 | Owned cleanup survives admission closure; no destructor waits on GIL | Retirement/closure race tests and cleanup counters | verified locally for runtime-owned cleanup in both threading ABIs: atomic-only Rust retirement, actual weak collection during otherwise idle Python after native drain, and two resident nodes joining after process-global admission closure; cleanup remains independent of SDK admission, and blocked destructors/noncooperative work retain incomplete reports |
| L03 | Retired activation object cleanup is observed | Destruction counters/weakrefs under eviction, fence and redeploy | verified locally in both threading ABIs: actual weak references clear after resident pressure eviction, signed management eviction, native generation swap, idle forced drain, global admission closure and fence-task completion before successor invocation; a cancellation-suppressing task correctly keeps its object alive until it exits |
| L04 | Close async generators and managed executor work | Async-generator finally + thread/executor join tests | verified for managed loop: async-generator finally observed; default-executor work joined; raw/native private pools remain application-owned |
| L05 | Combined report includes native report and all bridge/task/cleanup/budget work | Serialized combined report, incomplete cases, queued/running counts | verified locally for runtime-owned work and explicitly recorded native nodes: combined outcomes and host failure source, task/invocation/object/retirement gauges, bridge reservations and admitted/terminal operation conservation; live pending SDK work, all native-future/reply drop stages and incomplete lifecycle cases are directly tested; private application/library pools remain application-owned |
| L06 | Correct shutdown phases with both runtimes alive | Ingress closure/drain/cancel/object cleanup and final-join assertions | verified locally in both threading ABIs: ingress refusal before final native join, honest pending/timed-out native drain, cancellation and late-call refusal with both runtimes alive, idle object collection, exact state recovery and subsequent clean stops; global interpreter failure joins both native nodes and retains its failed combined report |
| L07 | Noncooperative code cannot fabricate complete drain | Disposable asyncio-stubborn, CPU-loop and blocked-C-extension hosts | verified: stubborn asyncio reports incomplete; disposable infinite-Python and GIL-held native-call owners require external termination and cannot produce clean-drain reports |
| L08 | External watchdog supports hard termination/restart | Bounded child-host watchdog drill and recovered acknowledged state | verified locally on Unix/macOS ARM64: shipped `cellar supervise` has bounded startup/probe/stop/reap policy, instance-correlated Python health, a global restart cap and a systemd template; six direct process tests pass, and the full installed CPython 3.14.5 archive-restore, changed-source TERM/INT handoff and forced-KILL/joined-restart/exact-receipt/new-write/complete-replacement-drain run passes; Linux execution of the new supervisor remains Q12 |
| L09 | Unclean owner death/takeover retains acknowledged state | Kill owner, remove local disk, recover from fleet, new write | verified in local development fleet: acknowledged write, hard owner death, complete owner disk deletion, bucket recovery and new write; independent quorum proof faults are recorded in S05 |
| L10 | Stale Python task cannot mutate after ownership loss | Pause old handler, force fence/takeover, resume old handler, inspect state | verified: real node lease mismatch fences old domain; after lease expiry successor recovers acknowledged state; cancellation-suppressing old Python task resumes and gets ContextExpired, with no late mutation |
| L11 | Initialize once; never finalize while threads enter | Source audit, duplicate runtime refusal and error-path join tests | verified for runtime-owned work: main-thread process guard refuses reinitialization; no interpreter-finalization path exists; supervisor success/error/panic and interpreter failure close admission, detach the GIL for the host join, clean owned profiling and retain incomplete work; duplicate-runtime and disposable error-path tests pass; private application pools remain application-owned |
| L12 | Raw Python effects/heap/coroutines not claimed durable or isolated | API/docs audit and recovery examples with explicit persisted state | verified for the documented contract: SDK, adapter, example and accepted ADR explicitly exclude raw effects, coroutine frames, private heaps/pools and untrusted-code isolation; crash/restart examples recover persisted business state/receipts through Cellar rather than Python frames |

## Verified deployment and replacement (M4)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| P01 | Immutable source/registration descriptor bound to native artifact | Source/class/event digests and tamper tests | verified: build embeds exact source/registration/file inventory; typed per-executable inventory digest is rechecked before every factory/generation admission |
| P02 | Hash-locked platform wheels and native dependency closure | Lock artifact hashes, transitive inventory and offline install | partial: hash-locked platform wheels install offline; the bundled runtime/package/native-extension closure is inventoried and restored from verified bucket artifacts; external OS/native-library prerequisite closure and attestation remain open |
| P03 | Runtime identifies CPython/GIL/SOABI/platform and actual files | Inventory capture plus pre-code digest/ABI verification | verified for supported targets: CPython/GIL/SOABI/micro version/target, actual loaded libpython and exact file contents/executable modes; external OS closure tracked in P02 |
| P04 | Native package metadata inventories Python artifacts, format marker 1 | Protocol types, strict validation and old-node feature rejection | verified: typed native runtime references, format marker 1, embedded-python-v1 feature gate and executable/inventory pairing; malformed pairing tests added |
| P05 | Build/deploy/install stage, publish, recover and verify closure | Real bucket package tests, corrupt/missing artifact refusal | verified locally: build/stage, pre-selector verified bucket publication, corrupt/missing blob refusal and real restoration with original runtime absent |
| P06 | Verify runtime/source/dependencies before importing app or admission | Sentinel app not run for each invalid artifact/ABI/environment case | verified: inventory/ABI/source before initialization/import; process digest before application factory/generation; real installed-host and tamper drills |
| P07 | Explicit home, roots, site behavior, no ambient user/PYTHONPATH/CWD | Hostile startup environment and .pth/customize injection tests | verified: `startup` path/site/CWD/sysconfig override tests plus explicit environment view |
| P08 | No network install at startup or request; offline qualified environment | Network disabled/failing installer and clean-runtime deployment smoke | verified locally: hash-locked offline wheel install and archive-restored host launched with cleared environment; no runtime package installer |
| P09 | Wrong ABI/missing library/tampered source refuses startup | Disposable startup matrix with no admitted request or app side effect | verified locally: invalid ABI/home and changed source/extension refused; the disposable bound-host matrix independently moves source, native extension, entire package root and actual libpython outside the inventory, refuses every startup and restores the exact runtime afterward; unlisted files also refuse startup; Linux execution of the expanded loss cases remains in Q12 |
| P10 | One immutable generation per process; ordinary native generations unchanged | In-process replacement refusal, native-only regression, rolling two binaries | verified locally: old running process observes/refuses a new deployment while remaining live; wrong executable rejects new package; changed Python code executes only in the replacement process; native generation regressions passed previously |
| P11 | Old/new host handoff and runtime artifact recovery | Versioned process handoff, restored artifact installation, retained state | verified locally: original runtime removed/restored from bucket, then distinct binaries, application versions and source inventories run in separate processes; TERM/INT handoff preserves state and exact receipts and executes changed Python cell code; both native/Python reports complete |
| P12 | Runnable example from fresh installation | Documented build/install/run/recovery test from clean environment | verified for local supported package: fresh-prefix archive restore, cleared-environment HTTP/native numerical work/receipt replay and complete preserving stop; platform matrix remains separate |

## Limits, metrics, SDK growth and qualification (M5)

| ID | Requirement | Authoritative evidence required | Current status |
| --- | --- | --- | --- |
| Q01 | Finite startup/request/body/value/depth/batch/mailbox/task/object/drain limits | Named limits, boundary tests, reservations released on every exit | partial: configured invocation/task/object/byte admission, operation mailbox capacity, response envelope and existing body/header/depth/batch/startup/drain limits have direct boundary tests; empty/oversized mailboxes refuse before bootstrap and task admission also covers empty-context callbacks; SDK-result envelope/depth boundaries now pass independently through native storage/SQL; shipping-default workload qualification remains |
| Q02 | Bounded backpressure both ways and cleanup accounting | Saturation/cancellation/failure tests with zero leaked reservations | verified locally for runtime-owned queues/reservations in both threading ABIs: independent real-node invocation, task, object, exact aggregate-byte and reply-channel saturation refuses excess work, releases/reuses capacity and keeps later work usable; full/closed operation mailbox tests release every event reservation; native-future/reply drop stages, binding proof faults, cancellation, 32-way mutation pressure and retirement/shutdown conserve all admitted operations with zero leaked reservations; trusted factory replacement/raw tasks/private pools remain outside cooperative admission |
| Q03 | Queue, bridge, Python/Rust duration, copied bytes, allocations/RSS and cleanup metrics | Instrumented counters/histograms and live snapshot tests | verified locally: bounded queue/SDK/reply/round-trip histograms, explicit UTF-8 boundary-copy counters distinct from reservations, lifecycle conservation/gauges, live age-stamped interpreter/work snapshots, loop scheduling delay, optional traced allocation bytes and pre-interpreter/post-join RSS/jemalloc/libc samples; scope/exclusions documented; adapter matrix and pinned package smoke pass; cross-platform and performance baselines remain Q09/Q10/Q12 |
| Q04 | Python CPU attributed separately; native accounting gaps disclosed | Separate measurements and documented coverage | verified locally: managed-loop thread CPU measured separately from Rust; handler duration/count and memory-snapshot assertions pass; exclusions for native/executor pools documented; cross-platform qualification remains Q09/Q12 |
| Q05 | KV authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python, not only native SDK tests | verified locally: real two-domain reads/writes refuse live-cross/expired handles; independent before-submit/in-flight cancellation, held/failed native data proof, conservative unknown failure without adapter replay, explicit idempotent-set reconciliation, 1-admitted/31-refused overlap pressure and preserving restart retain exact effects with complete reports and zero pending operations/reservations |
| Q06 | Shared SQL authority/cancel/backpressure/replay/resource qualification | Binding-specific tests through Python | verified locally: actual two-domain DDL/DML/queries refuse retained-handle writes; independent before-submit/in-flight cancellation, failed native proof, unknown failure without replay, explicit primary-key receipt reconciliation, 1-admitted/31-refused overlap pressure and restart retain singular effects with complete reports and zero managed work |
| Q07 | Queue producer/consumer settlement, retry, cancel, replay and resource qualification | Adapter-specific independent failure matrix | verified locally: real consumer matrix covers default/explicit ack, handler errors after ack, first conflicting decision, same-ID retry/exhaustion, timeout/cancel expiry and retained-handle authority; independent producer before-submit/in-flight cancellation, held/failed proof, unknown-result reconciliation and overlap pressure run through the real broker; SQL business receipts remain singular and the empty broker/effects persist across restart with complete reports; this drill reproduced and now passes the native running-alarm reset regression |
| Q08 | Cron retry/no_retry, authority, cancellation and occurrence replay | Adapter-specific independent failure matrix | verified locally on standard and free-threaded CPython: real native alarm/history matrix proves same-occurrence retry, explicit no_retry after error, refusal of live-cross/expired controllers, idempotent SQL effects and exact history/alarm persistence over restart; independent timeout and killed-owner/local-disk deletion tests preserve the original occurrence and interrupted history, reject late SQL/no_retry, recover one SQL effect across two attempts and completely join the replacement; current-source Linux qualification remains Q12 |
| Q09 | Pure Python, PyO3 native extension and numerical library compatibility | Exact pinned versions/platforms; representative operations/concurrency/teardown | verified for stated platforms/versions: macOS ARM64 CPython 3.14.5/3.14.6 and 3.14.5t; Linux x86_64 regular 3.14.8 plus installed 3.14.5 and 3.14.5t execute pinned Pydantic/NumPy/pure-library workloads; both ac521f3 free-threaded Linux jobs completely pass the 3.14.8t adapter suite and 3.14.5t package/archive/rolling/supervisor/native-library qualification; final lifecycle/cancellation source remains Q12 and other platforms/library versions remain unqualified |
| Q10 | Native versus Python latency/throughput/memory/copy/recovery baselines | Reproducible harness and raw measured evidence, no invented target | verified for the stated development scope: [recorded baselines](007-embedded-python-baselines.md) retain two independent nine-case optimized experiments and raw data, including matched CPython 3.14.5/3.14.5t and a true native-only executable; exact HTTP/counter receipts, killed-owner/disk-deletion takeover, copy/memory observations and joined reports pass; the verifier rejects corrupted receipts/metrics/drain/cleanup; shared-machine load and development-store limits are explicit and no production capacity target is claimed |
| Q11 | Native-only dependency/build check and no JS all-features | Explicit CI check, local graph/build evidence | verified for dependency/build separation: explicit Linux CI native-only check with nonexistent interpreter and all-feature no-JavaScript graph passed on fe6d2f8; current source retains the same feature/dependency graph, with local no-embed and graph checks |
| Q12 | Full affected test/lint/format/layout/doc qualification; preserve failures | Exact source/toolchain commands and passing scope-specific results | partial: current macOS regular/free-threaded adapter suites, strict adapter/native test-target Clippy, exact clock-refusal recovery, format/layout/dependency/docs guards pass; at c15fdaa, standard Linux push job 118804218808 and free-threaded PR job 118804285570 each passed every gate, including native 87/87, cron, baseline harness, installed libraries/archive/rolling/supervisor and native examples; c15fdaa free-threaded push failed two native-node cases (clock-refusal initial consumer observation and progressing drain-token lease); earlier failures remain preserved below; final lifecycle/cancellation changes still require fresh Linux execution |
| Q13 | Public facade type annotations and accepted implementation decisions | Typed Python API, accepted ADR, build/bootstrap/bridge decision record | verified: public facade annotations and frozen CellIdentity; accepted ADR 004 records bootstrap, bridge, signals, immutable packaging/replacement, cleanup, compatibility and qualification boundaries against implemented contracts |
| Q14 | Current docs describe executable APIs and remaining gates accurately | Plan sketches updated to actual contracts; audit has no unstated exception | verified for current documented surface: main plan, adapter/example/environment guides describe actual facade, source layout, accepted ADR, provisioning and conservative failure outcomes; chronological audit history is explicitly distinguished from current tables; remaining gates are stated |

## Current work

The adapter's core deliverables exist across M0–M5. Completion means closing
acceptance evidence, not adding another happy-path example. M2's S01–S09 rows
are verified within their stated scopes; all phases still share final-source
qualification. Conventional and free-threaded CPython remain equal targets.

| Order | Focus | Remaining gate and evidence |
| --- | --- | --- |
| 1 | Result and authority correctness | H02 result byte/depth boundaries now pass locally in both ABIs, including one retained SQL mutation after a refused reply. A08/A12: deterministically hold work at queued, execution and commit/output boundaries, cancel or fence it, and prove no stale mutation; exercise nested cycles without deadlock. H04: preserve available typed route causes without inventing rollback provenance. |
| 2 | Failure/lifecycle qualification | Q12: diagnose the preserved native event-owner lease, initial consumer admission and drain-token failures; retain assertions and deadlines. Run the latest cancellation/retirement/resource fixes on Linux in both ABIs. A passing retry alone does not explain a failure. |
| 3 | Deployment dependency closure | P02: distinguish bundled artifacts from OS/native prerequisites, attest the supported host requirements and resolved external dependencies, and refuse incompatible/missing prerequisites before admission; qualify offline restoration on each supported target. |
| 4 | Privacy and operating limits | H06: finish bootstrap/loader failure review with path/secret sentinels. Q01: use reproducible workload and saturation measurements to justify shipping defaults and document their operating scope; development baselines are not production SLOs. |
| 5 | Final release evidence | Q12/Q14: run the complete affected tests/lints, installed example, archive recovery and process replacement against the final source in both ABIs; publish exact versions/platforms and reconcile every remaining partial row. |

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
