<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Remaining cell-group work and priorities

Purpose: Prioritize the remaining cell-group MVP, merge gates and full architecture deliverables.

PR [#160](https://github.com/crabbuild/cellar/pull/160) contains the shared runtime
and the accepted public SQL cutover. This ordering preserves the full
CG-01–17 objective in [tasks.json](tasks.json), [design](design.md) and
[qualification](qualification.md). See [progress](progress.md) for exact source
and validation boundaries and [handoff](handoff.md) for executor context.

Follow-up PR [#166](https://github.com/crabbuild/cellar/pull/166) passes the normal
published/unpublished recovery driver in both real RustFS image modes at
`320a40ce`. All four runs permanently fence and delete five source disks, restore
fresh RF3 service on another VM and verify SQL/receipts, fresh S3 writes, retained
restart, retirement and joined shutdown. Published recovery restores both child
histories and newer S3 tails. Source-cached SDK handles also pass fresh writes,
linearizable reads and minimum-version reads before child Fleet promotion.
The complete six-case CG-13 matrix remains open. See [progress](progress.md) for
exact pins, profiles and scope.

The same PR now integrates committed TiKV routing/read optimization source
`19982a1b` at `79f2ccc5`; 63 selected Rust checks and focused strict Clippy pass
with the CI test profile. CG-17's eight-package/32-case design is in progress,
starting with the inventory and evidence gate. See
[optimization progress](optimization-progress.md) for delivered behavior and
open OPT gates. At `b85c609c`, combined-source native RF3 recovery
passes real RustFS closure selection, original SDK retries and uncertain outbox
recovery in both image modes. Held receipts share an admitted verification;
uncovered writes still wait for its release within the original budgets. Seven
archive safety cases and full strict all-target Clippy also pass. See the
[current-source checkpoint](progress.md#cg-13-checkpoint-held-receipt-replays-and-current-source-rustfs)
for hashes, failed controls and fencing limits. The six-case CG-13 task gate and
full disaster profile remain open.

## Optimization completion priorities

### Disposable close checkpoint work and current priorities

The materialized shared-cell close now avoids copying a disposable WAL into the private main file immediately before that file is discarded. The real-close regression independently seals and syncs the result, observes the old main inode across actual cleanup, and rebuilds the correct value from the sealed result. The same fixture fails before the policy change and passes after it. This proves avoided work; it does not qualify throughput or latency.

Committed implementation `aa93bb82` includes `origin/main` `2f083f14`. The full affected library (1,258 passed / 2 existing ignores) and native execution (232 passed / 2 existing ignores) suites pass. All nine strict checks pass. Nine of ten selected SQL/KV/Queue/RPC/recovery checks pass; ordinary Queue fails with an extent-iteration deadline. Combined qualification remains **FAILED**. Original Queue C2/C4 load qualification still fails. Two temporary C2 phase probes reach successful enqueue completion at peak two, then fail the original final consumer drain; one observes 472 of 528 required completions. Their diagnostic sources and all failures remain distinct from the restored candidate. The probe code is removed. Ordinary Queue and batch-proof A/B/B/A controls also reproduce failures on the published parent; they do not establish a candidate-specific regression or close Queue progress.

Completion priorities, in order:

1. **P0: Progress and proof correctness.** Close original-deadline Queue delivery/drain in both image modes, repeatable provider-error handling and remaining receipt/RPC/recovery failures. Published-parent hosted native-framework failures include disaster/token recovery's diagnostic panic and the S3-output pending-response oracle. Classify the actual S3 response before attributing an incorrect acknowledgement.
2. **P1: Complete configured fast paths.** Carry the configured extent apply validation profile into independent image preparation. The public profile currently refuses reuse because execution-derived preparation and committed apply limits differ. Preserve exact-profile equality, negative controls and existing budgets.
3. **P1: Bounded persistence and scheduling.** Measure and reduce SQL queue/image work, physical sync amplification and archive interference. Preserve Ready/sync/completion ordering and progress for leases, elections, control and shutdown; sparse traffic must not wait to fill a batch.
4. **P1: Sustainable capacity.** Prove physical shared-segment deletion and once-only capacity credit, then repeated reserve-aware split/move/merge with retained readers, snapshots and uncertain work.
5. **P2: Combined release and comparison.** Complete authority/partition/wake/recovery, SDK/daemon/feature-mode gates and the source-matched SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 matrix on the original three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared RustFS, 256-byte payloads, Queue batch eight and reversed repetitions.

The latest read-only original RustFS observation still fails the unchanged 6 GiB free-space reserve; no constrained comparison is admitted. All OPT-01–08 and remaining CG/SDK/release/performance gates stay open. See the [source-bound close qualification](disposable-close-qualification-2026-11-18.json) for exact sources, artifacts/profiles, all outcomes, retention limits and capacity evidence.

### Exact live-image reuse and incoming-voter fencing

Committed implementation `53c0fb05` includes `origin/main`
`2f083f14`. A pending producer's independently replayed and
synced image can serve the exact live committed apply on that engine. Engine,
group, encoded input, validation limits, store, result and base checks remain
required. Submission and ticket clones carry weak references. Independent
follower/archive/recovery replay, inventory health, commit/serving proofs,
operation deadlines, banks and workers remain required. A cached result name
without the live capability must independently replay the supplied LTX.

Both image work-count regressions pass after exact replica value, receipt,
version/witness and joined-close checks. Leader base-image reads fall from one
to zero; follower reads stay one each. Materialized apply seals fall from one
to zero. This qualifies reduced duplicate work, **not throughput or latency**.

A full native run exposed outgoing-only application readiness after a Joint
restart. The fix requires incoming membership for application readiness,
producer/enrollment and leader-bound read authority; outgoing followers cannot
campaign. They still age voting leases and vote for legitimate incoming
candidates. Higher-term handoffs retain term evidence without self-voting.
An existing outgoing protocol leader can finish Joint. Six focused cases and
twelve original crash/sync/delivery/image controls pass on their recorded
source. The final combined run rechecks the full suites below.

**Combined owning qualification: FAILED.**
Strict all-feature/all-target Clippy, native-only host/Python facade build,
format/layout/dependency/docs/plan/runner/whitespace checks pass (9/9).
Current-source failed selections:

- `native_cell_groups_primitives--materialized_queue_four_in_flight_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--rooted_queue_four_in_flight_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--queue_four_in_flight_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--queue_load_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--ordinary_kv_bindings_use_managed_shared_groups`.

Full-suite results:

- `extent_apply--full`: PASSED; 15 passed, 0 failed, 0 ignored.
- `cellar--full`: PASSED; 1257 passed, 0 failed, 2 ignored.
- `native_cell_groups_execution--full`: PASSED; 232 passed, 0 failed, 2 ignored.
- `native_cell_groups_storage--full`: PASSED; 22 passed, 0 failed, 2 ignored.

The four original Queue C2/C4 checks retain their exact offered concurrency
and ambiguous deadlines. Independent C4 cases exercise both image modes;
the combined/C2 cases stop at their first failed materialized iteration.
Ordinary Queue and batch-proof sharing, ordinary/Axum SQL, Axum KV,
fragmented KV, ordinary JSON RPC and both selected SQL recovery cases pass.
A failed ordinary KV negative-provider case returns an ambiguous deadline
rather than the required unavailable-payload error; later configurations
inside that test are unrun. See the [source-bound record](prepared-image-and-incoming-voter-qualification-2026-11-18.json) for every
selection, artifact/profile, failed earlier cohort and retained manifest.
The earlier failed membership-crash TempDir auto-cleaned before capture:
its disk state is unavailable, while logs/source/executable and remaining
fixture data are retained. Crash parents now retain their directories.

The read-only original RustFS check at 2026-11-18T11:39:56.787709+00:00 found
0 available bytes, below the unchanged 6,442,450,944-byte reserve.
No constrained campaign is admitted. The native suites use in-memory object
stores. Previous published failures remain; later narrow passes do not replace
them. Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open.

P0 remains repeatable original-deadline SQL/KV/Queue, receipt/RPC and recovery
progress. Then measure persistence batching and shared-worker fairness,
physical reclamation/once-only credit and reserve-aware movement, combined
fault/SDK qualification, and the original three-node SQL/KV/Queue
Quorum/Quorum+S3/Fleet/Bucket C1/4/16 throughput/latency comparison.

### Previous independent Queue image verification

Test-entry source `bbb93651` keeps production identical to
`29a735c9` and includes `origin/main` `2f083f14`.
Independent materialized and extent-backed Queue C4 entry points now run the
same original load oracle. The original combined C4 and C2 tests remain required;
the canonical load body, identities, assertions, deadlines, banks and one SQL
worker are unchanged. RustFS variants are listed only.

**Latest native qualification is FAILED.** Both independent C4 modes, the
original combined C4 and original C2 control fail with ambiguous deadlines.
Both independent modes observe four offered requests; C2 observes two. Each
failure retains its original identities and data. 8/8 affected Clippy and
repository checks pass. This run qualifies no sustained Queue progress and
does not erase earlier full-run or hosted failures.

The unmodified production baseline reproduces C4 failure. Two phase-triggered
samples contain foreground apply/image persistence, private preview, archive
replay and checkpoint verification; overlapping worker samples do not attribute
exclusive request time. Healthy/outage/outage/healthy controls fail in both
the write-only and refined write-plus-checkpoint-read outage experiments. The
refined outage records actual denied reads/writes in both failed runs. Pausing
archive I/O therefore does not resolve the deadline; in-flight work remains a
limit of this control. All temporary probe code is removed. See the
[source-bound Queue record](queue-image-progress-qualification-2026-11-18.json)
for source/executable/profile identities, every outcome and retained manifests.

The read-only original RustFS check at 2026-11-18T09:42:09.811621+00:00 finds
10,035,200 available bytes, below the unchanged
6,442,450,944-byte reserve. Identity and limits match with zero restarts;
no constrained campaign is admitted. Test durations and samples establish no
throughput or latency-percentile gain. Full OPT-01–08 and remaining CG/release
gates remain open.

P0 is repeatable original-deadline SQL/KV/Queue and receipt/RPC/recovery progress,
including both Queue image modes and the C2 control. Foreground image preparation
and persistence require causal measurement before another production change.
Then qualify bounded persistence batching and shared-worker fairness, physical
reclamation and movement, combined authority/recovery/SDK faults, and the full
source-matched SQL/KV/Queue constrained comparison.

### Previous scratch checkpoint

Committed source `9c4f830b` includes fetched `origin/main`
`2f083f14`. Shared-cell materialized enrollment and commands
create disposable scratch without syncing its ancestor directories. Fresh
initial-state/image authority, path bounds, private locks, workspace lifetime and
sealed-image/engine persistence barriers remain required. The legacy group
executor retains its original directory policy.

The early corrected regression is intended-red against production `0da514a4`
and green on the candidate with identical fixture and profile. It commits an
initialization and command, checks every replica's version/witness and exact
value through engine-bound readers, and joins the owners. Only three production
files differ in that comparison. The earlier absent-applier-map fixture panic,
its stopped qualification and all failed data remain explicit in the [source-bound scratch record](cell-scratch-sync-qualification-2026-11-18.json).

Selected owning qualification is **FAILED**: 37/38 Rust selections
and 9/9 Clippy/repository checks pass. Failed selections: `queue_four_in_flight_preserves_acknowledged_messages`.
A selected pass does not establish sustained progress or repair earlier full-run
or hosted SDK failures. Ordinary SQL, KV, Queue and fragmented KV pass; native
SQL recovery passes all eight configurations with an in-memory object store.
Queue C4 fails two producers in the materialized run at four offered requests;
the extent iteration is not reached. Original deadlines and admission bounds
remain unchanged.

The original RustFS read-only check at 2026-11-18T08:27:31.405819+00:00 finds 8,589,312 available bytes, below the unchanged 6,442,450,944-byte reserve. No constrained campaign is admitted.
Native work counts, stack samples and test durations establish no throughput or
latency-percentile gain. Complete OPT-01–08 and remaining CG/release gates stay
open. P0 remains repeatable original-deadline SQL/KV/Queue, receipt/RPC and recovery
progress. Next are persistence/archive batching and shared-worker fairness,
physical reclamation/movement, authority/read fault coverage and the complete
source-matched constrained comparison.

### Previous settlement checkpoint

At the previous compiler input `f8f9bee8`, SQL reconciliation can confirm ownership of
its exact original producer without queuing Resume; an uncertain observation
remains uncached. A matching intermediate RPC receipt in a checked same-term
immutable image reuses the pending view until its term or full cell version
changes. Every settlement fetches and checks current authority. Exact Quorum/S3
terminal proof still governs output, non-commit, retirement and credit release.

Two work-count regressions turn intended-red to green in both image modes at
`e0d61c1b`, with identical fixtures across isolated production changes. Four
unchanged final-RPC checks use one SQL reconciliation and no Resume. The current
source corrects two fixture calls to Cellar's required runtime selector, with
production inputs unchanged. All sixteen focused cases are rechecked, including
exact output, replica version/witness agreement, newer-term non-commit,
unknown replies, once-only callbacks and joined closure.

Owning-boundary qualification is **FAILED**. Full native execution:
225 passed / 0 failed / 2 existing ignores. Full library:
1252 passed / 1 failed / 2 existing ignores. Original Queue C4: **FAILED**.
The selected public RPC/SQL/KV/Queue/recovery and S3 controls report
16/19 passed. Interrupted SQL recovery reports
5/8 configurations passed; its full matrix remains an acceptance requirement.
The [current record](rpc-pending-settlement-qualification-2026-11-17.json)
contains every result, failed cohort, executable, profile and retained manifest.

The initial `e0d61c1b` broad campaign stopped at forbidden direct Tokio selector
calls in the new fixture. Its original failed record and fixtures remain
retained. The corrected campaign rechecks full native execution and the owning
fixture; 21 other results are reused only after exact executable and test-profile
identity, each carrying its original source, log hash and explicit reuse basis.
A reused failure remains failed. No source-specific failure is erased.

Frozen parent/candidate ABBA controls report remote
8/8, park-entry
4/4, and ordinary Queue/fragmented KV/focused SQL recovery
9/12 passed. These isolated controls establish no causal
repair of failed broad runs. Ordinary Queue ABBA is failed / failed / failed / passed:
both candidate runs fail and one parent run fails, leaving the deadline cause open.
The one-configuration SQL diagnostic does not
replace the failed eight-configuration matrix. The
[previous settlement record](pending-command-settlement-qualification-2026-11-17.json)
retains earlier ownership/SQL failures and passing ABBA controls at `6c280bdd`.

Published parent `8393e1ea` passes the hosted disaster diagnostic. Typed RPC
reports three successful jobs and one failed Python 3.14t lifecycle job, where
`host_failed` is true despite one known retained task. This feedback does not
qualify the current source or the full SDK/release matrix.

The read-only original RustFS check finds 0 available bytes at
2026-11-18 06:19 UTC, below the unchanged 6 GiB reserve. Identity, start and
container limits match with no restart or resource/data changes. No constrained
campaign is admitted. Native work counts and elapsed times establish no
throughput or latency-percentile gain. No full OPT/CG, SDK/release or performance
gate is closed by this component.

At that checkpoint, full-library failure cases: `raft::transport::parks::tests::partial_certificate_delivery_wakes_and_rejects_late_replay`.

At that checkpoint, failed selected public cases: `ordinary_queue_callbacks_use_shared_quorum`, `fragmented_kv_payloads_progress_without_reopening_sqlite_per_chunk`, `interrupted_sql_bindings_recover_without_replaying_business_calls`.

The parent `b08df720` origin-denial checkpoint separately passes 23 public routing
cases with 13 explicit RustFS ignores. Its authenticated denial preserves command
ambiguity and current leader advice; see the
[origin-denial record](command-origin-refusal-qualification-2026-11-17.json).
Those routing results are not a new current-source full routing run. Parent
`504dd25b` also has terminal hosted SDK and Node failures. All these native
RF3/TLS checks use in-memory object stores; they do not qualify constrained RustFS
throughput or latency.

| Priority | Remaining gap | Required result |
| --- | --- | --- |
| P0 | Original-deadline progress and command settlement | Close Queue two/four-request progress in both image modes, ordinary Queue operator and fragmented KV deadlines; full SQL recovery, remote transport/admission and park-readiness failures; repeatable busy-receipt/RPC/S3 progress and hosted recovery/SDK qualification. Preserve original IDs, callbacks, exact receipts/bodies and banks. Native nested ingress is three seconds; the five-second callback timeout, five-second constrained operation bound and 800-ms S3 phase control are separate unchanged limits. |
| P1 | SQL, persistence and archive cost | Attribute request-critical-path time and physical amplification before tuning. Explicit read-only SQL, prepared-submission batching and shared persistence already exist; qualify their combined behavior and bounded sparse-traffic latency. |
| P1 | Shared-worker fairness | Qualify competing ingress nodes and stale sequence hints, then prove a busy group or slow callback/archive cannot starve cold-group SQL, lease/control traffic, elections or shutdown. Measure queue wait and p95/p99 under mixed load without increasing resource banks. |
| P1 | Sustainable capacity and placement | Prove physical shared-file/segment deletion and once-only credit after readers, snapshots and archive/transfer owners release. Then qualify repeated split/move/merge and reserve-aware placement under the original bounds. |
| P2 | Combined fault and performance acceptance | Complete the public primitive, authority/partition, SDK/daemon/feature-mode, restart and fenced recovery matrices on one source. Run matched SQL/KV/Queue Quorum, Quorum+S3, Fleet and Bucket comparisons at C1/4/16 on the original three 1-vCPU/2-GiB RF3 nodes and three groups. |

Complete passing cases must report throughput, p50/p95/p99, errors, CPU, I/O and
archive lag. Failed, unmeasured or differently constrained windows cannot
establish an optimization gain.

The `f8f9bee8` checkpoint's original RustFS check reports 0 available bytes at
2026-11-18 06:19 UTC, below its unchanged 6 GiB reserve. No constrained campaign
is admitted; backend identity and limits match, with no pruning, restart,
resource change or reserve relaxation. Native elapsed test times are not
throughput or percentile measurements. All complete
OPT-01–08 packages and remaining CG gates stay open; see
[optimization progress](optimization-progress.md) for source-specific evidence.

## Current constrained performance priorities

At compiler input `7c39c193`, the sender can prove a command was not sent when
stale/missing routing or capacity refuses it before queue acceptance or its first
exchange. Routing may retry only that exact attempt within its original admission,
identity, deadline and ticket bank. Earlier uncertainty stays sticky; failures
after exchange begins remain ambiguous. Three owning transport cases and seven
selected native command/routing controls pass, as do strict all-target and
recovery-consumer Clippy and repository checks. The byte-identical library
executable reports 1,247 passes, one failure and two existing ignores: truncated
read-refusal input retains node/peer receive credit past the original three-second
observation bound. Published and candidate isolated controls both pass; neither
explains or closes that full-run failure. See the
[command-admission record](command-admission-qualification-2026-11-17.json).

The latest [native batching checkpoint](prepared-submission-batching-qualification-2026-11-16.json)
at compiler input `816bcded` passes core, execution, storage, archive/controller,
eight selected public SQL/KV/Queue cases, strict affected-target Clippy and the
full 26-test measuring tool. Both image forms reduce leader proposal persists
for an already queued prefix within the existing limits, preserving owner
admission and read order. Two additional rooted-128 invocations pass their
original 60-second enrollment limit at 22.138 and 53.210 seconds. The variation
and unchanged work counts do not establish a causal latency repair. This native
checkpoint does not replace the constrained matrix below.

Published `8ee50b18` feedback keeps the first priority open: the focused Linux
disaster target passes eight cases and fails three at physical-inventory
`peer_overloaded`, with eight explicit RustFS ignores. Four Typed RPC jobs and
three JavaScript jobs pass. Both native SDK builds and both Python SDK jobs fail
to compile the shared recovery fixture's missing `tracing` dependency. The local
dependency correction passes that consumer's build and strict Clippy check.
The seal phase-capture follow-up has passing exact privacy/CLI controls, an
earlier eleven-pass native suite and a later ten-pass/one-failure suite at
`transition capture-source`. See the
[source-specific record](seal-observations-qualification-2026-11-16.json).
The source-acquire repair passes five owning controls; its earlier full native
sweep retains a separate ambiguous command deadline. The combined seal-admission
candidate now passes the 1,240-test all-features library and thirteen-test native
disaster target, with two and eight explicit ignores respectively. Current-source
Linux and SDK qualification remain required. See
[transition progress](optimization-progress.md#transition-progress-under-bounded-admission).
These failures remain explicit; passing controls are not full qualification.
The original RustFS now reports zero available bytes, below the original 6 GiB
reserve, without any resource or data changes.

The next priorities are:

1. **Close the current correctness/qualification failures.** First diagnose the
   retained truncated-read receive-credit failure above without extending its
   bound or releasing physical ownership early. Preserve original-ID receipt
   reconciliation and Queue deadline work below. The new local proof does not
   settle any possibly transmitted write or establish a causal repair of the
   earlier full-run member-replacement failure. At published
   `bf1bbc68`, all four hosted Typed RPC jobs, all three JavaScript jobs and the
   three filesystem release builds pass. Both completed push-native framework jobs retain the park
   unauthorized-read failure: the server's permission refusal arrives as
   `Query(Unavailable)`. The host-native reproduction and metadata-only ingress
   regression isolate this transport boundary. The authenticated plain-read
   refusal follow-up passes the unchanged native park case in both image formats
   and both codec controls at compiler input `d81c2da3`. The original drop-only
   partial-body control times out while physical receive credit remains held.
   Explicit TLS shutdown passes the selected transport check and full 1,245-test
   library at `d7846d50`; all six park/wake cases also pass. Its broader routing
   run reports 22 passes, one failure and thirteen explicit RustFS ignores.
   `removed_member_advice_cannot_grant_authority` fails with ambiguous
   `Transport(StalePeer)` on original command ID 1 after real member replacement,
   before bad advice injection. Both published/candidate isolated repeats pass;
   they do not explain or repair the full-run failure. Preserve the original
   possible mutation and diagnose route-generation/replay progress. Wider and hosted
   validation remain required. The focused disaster job was
   cancelled and supplies no pass. Keep the older source-specific failures below
   until their own current-source oracles pass.
   At published
   `b64ecd9b`, both Python ABI/SDK jobs and all three filesystem release builds
   pass. Both native framework jobs fail, including
   `partial_park_and_stale_certificate` on Python 3.14. The focused Linux
   disaster target reports twelve passes, one failure and eight explicit RustFS
   ignores. Its failed `disaster_retry_and_old_token_refusal` path also panics
   while collecting observations after the source hosts have been drained.
   The local guard now returns an explicit diagnostic error in that state,
   preserving the primary failure. Its hosted recovery cause still needs a
   fresh original-budget qualification.
   The two Python 3.14 Typed RPC jobs fail while their 3.14t counterparts pass;
   the full 3.14 run fails to reopen read admission after proof recovery and
   reports host failure during shutdown. Node's
   `rpc_requests_and_http_arguments_keep_body_signal_and_authority` also fails;
   the JavaScript package and example jobs pass. The combined Rust SDK gates
   also fail at their requirement that both qualification campaigns succeed.
   Preserve the earlier failures and original mutation
   identities in the [lease](lease-readback-qualification-2026-11-16.json),
   [seal](seal-observations-qualification-2026-11-16.json) and
   [transition](transition-progress-qualification-2026-11-16.json) records.
   Local passes do not establish a causal repair of these hosted failures.
   Keep original deadlines, fencing, exact receipt reconciliation and recovery
   refusal behavior while resolving each owning boundary.
   At `d7846d50`, six of eight selected primitive controls pass, including
   ordinary/Axum SQL and KV, Queue callbacks and interrupted SQL recovery.
   Queue proof sharing fails during fast-queue warmup with an ambiguous runtime
   deadline. The four-in-flight Queue control fails producer sequence 6 with
   ambiguous `Runtime(DeadlineExceeded)` after entering its first Raft target
   attempt with 2,034 ms remaining. Both failures retain their original
   identities and disks. Strict all-target and recovery-consumer Clippy pass;
   the full native disaster runtime target remains unrun on this source.
   See the [scoped refusal and runtime record](read-refusal-qualification-2026-11-17.json).
2. **Continue measured worker and physical-lifetime optimization.** Proposal
   collection is one stage. Profile ordered apply/image publication, producer
   cleanup and transport batching under unchanged banks; qualify actual prepared
   cohorts across hot/cold groups, cancellation, faults and sustained load.
   Physical work must retain its charge through completion, and later entries
   cannot bypass the contiguous applied prefix. Retain the original-budget
   rooted-128 checks and their failed history alongside controlled load repeats.
   The peer-query candidate removes a reproduced serial application-response
   wait within the existing sender worker and query admission. Its six public
   SDK/native/Axum SQL/control cases pass on host-native RF3 groups in both
   image forms. The initial socket-ceiling fixture was refused before its first
   socket because a parked replica had an unclaimed physical route; its join
   masked the error behind a timeout. Those attempts supply no TLS concurrency
   evidence. The final matched ingress-route fixture now passes all three
   cases, including explicit physical-route refusal, socket reuse at the
   two-connection ceiling and cancellation credit release. The full library
   passes 1,243 tests with two existing ignores. The current routing executable
   exactly matches the already tested public/control artifact; affected strict
   Clippy also passes. The full native disaster target passes thirteen cases
   with eight explicit RustFS ignores on this source. Final all-feature/all-target
   strict Clippy passes. Complete hosted qualification and retain the separate
   older Linux failures. See the
   [peer-query checkpoint](peer-query-progress-qualification-2026-11-17.json).
   Complete RL-07 with archive outage, bulk transfer, sustained hot/cold load
   and contention across peers at the physical socket ceiling before accepting
   the full fairness or OPT-06 package.
3. **Run the requested complete constrained comparison.** Build matching Linux
   binaries from the qualified source and compare SQL/KV/Queue in all four
   durability modes on the original three 1 vCPU / 2 GiB nodes, RF3, three groups,
   original entities/payloads and concurrency/repetition schedule. The exact
   RustFS `/data` named volume reports 12.3 MiB available in the latest read-only
   recheck, below the unchanged 6 GiB reserve. Backend identity, image, CPU/memory
   limits, start time and zero restart count remain unchanged. The existing capacity proposal remains unapproved;
   no VM/disk/resource changes, deletion or reserve relaxation were performed.
   Native persistence counts cannot substitute for error-free paired throughput
   and p50/p95/p99 measurements.
4. **Finish combined release qualification.** Complete physical reclamation and
   repeated reserve-aware movement, authority/read partitions, crash/recovery,
   wake, full primitive/SDK/feature/daemon and independent failure-domain cases.
   Keep OPT-01–08 and the remaining CG task gates open until their complete
   named matrices pass on one source.

The [main-170 matrix](three-node-primitive-main-170-2026-11-15.md) uses main
`16c45b26`, public commit `1d663425` and compiler input `362f78db`. All twelve
SQL/KV/Queue mode cases finish on the original three 1 vCPU / 2 GiB nodes and
five-second operation/callback/lease limits. Four pass and eight fail, with
51 request errors. This is the latest complete mode matrix; earlier records
below retain their own source and scope. The later `1d75a95f` cadence candidate
passes SQL quorum's 18 windows without errors; its KV/Queue cases are held by
the unchanged disk reserve. See the [current-source SQL checkpoint](three-node-primitive-main-170-2026-11-15.md#foreground-only-cadence-sql-quorum-re-verification).
That scoped pass does not replace the older full matrix.
The subsequent native enrollment-polling candidate `a8b26ff9` passes its execution,
archive-controller and three selected RustFS controls, while the separate rooted-128
diagnostic still exhausts its original enrollment budget. Its
[source-specific record](enrollment-resolution-qualification-2026-11-16.json)
does not establish a constrained-node throughput/latency gain.

SQL now has an explicit `prepare_read_only` path through fresh quorum selection
and a pinned image, preserving the writable API and source-write proofs. The
benchmark uses it for SQL point reads. Its
[component qualification](sql-readonly-qualification-2026-11-16.json) does not
replace the constrained matrix or close the remaining enrollment, lease-progress
and release gates.

The private SQL workspace constructor also omits creation syncs through a
separate disposable API. Full reservations and synced physical deletion remain;
the original durable constructor and image/Raft/S3 barriers are unchanged. Its
[qualification](scratch-workspace-qualification-2026-11-16.json) records the exact
avoided-work oracle and broader limits. Repeated passing native enrollment runs
do not erase the earlier timeout or establish a constrained-node speedup.

The [archive retirement checkpoint](archive-retirement-qualification-2026-11-16.json)
at compiler input `2ac6b776` avoids a standalone journal replacement while keeping
the next exact intent's durable barriers. Its owning regression passes native
and original RustFS repeats in both image forms. Broader qualification remains
failed: the full measuring-tool target has one original-budget rooted-128
timeout, and materialized Axum KV does not reach its injected archive PUT outage
before its deadline (14/15 RustFS invocations pass). Hosted CI at published
`65f888af` also fails three native disaster cases at `authorize-seal`, the
primitive catalogue's renamed Workflow owner, and named-container capacity
placement. Preserve those failures and diagnose their owning boundaries before
claiming release readiness or comparing rates.
The renamed catalogue owner is now corrected to `resource_read.rs`; catalogue
admission, 13 coverage-guard tests and 44 design-plan tests pass locally, without
changing task/case acceptance. The other failures remain open.

Bounded failure snapshots at test-only input `abc75976` show that the earlier
payload-store outage stops one archive owner in each group with
`Archive(Payload(Storage(...)))`; peers then report `Busy`, and the later PUT
denial counter remains 136. The identical outage-reaching failure occurs in
frozen pre-change and component binaries. Repair this archive-progress path
through exact existing payload-pin/adoption identities and fresh proof checks;
keep missing/corrupt data and failed journal barriers terminal. The three local
native container-capacity passes do not reproduce its separate hosted failure.

The subsequent [generic payload-I/O repair](payload-archive-retry-qualification-2026-11-16.json)
at compiler input `b32cb4bc` keeps that archive owner retryable under its original
exact journal intent and requires fresh byte verification. Missing/corrupt
payloads and journal failures remain terminal. The unchanged owning fixture
turns four expected stopped-owner failures into four passing retries, with
terminal data-refusal controls preserved. Native core/archive/execution,
selected API/recovery and strict Clippy gates pass; all 22 selected RustFS
invocations pass, including two independent Axum KV invocations. Historical
failed owners remain failed at their own source cuts. Broader payload
deadline/backpressure, crash and release coverage remains required.

The subsequent [payload admission repair](payload-archive-pressure-qualification-2026-11-16.json)
at compiler input `8fb09d7f` also preserves archive progress on typed `Overloaded`.
An actual held foreground GET forces shared admission pressure; repeated failures
retain the original exact journal intent without coverage. Joining that GET then
permits fresh byte verification and independently verified exact-cut coverage.
The corrected identical fixture fails twice per image form before the repair and
passes afterward. Final-source controller/Clippy/repository gates pass; the
preceding source's broader native behavior passes retain their own scope and its
failed fixture lint remains recorded. All eight owning RustFS selections pass,
but the full backend campaign remains **FAILED: 8/18**, with ten broader failures
including actual PUT HTTP 500 responses. This closes only the reproduced
operation-admission progress boundary; payload deadlines, uncertain cleanup,
fitting-credit wakes and sustained fairness remain open.

A read-only check positively matches the original Docker and guest Engine and
observes 823,226,368 bytes free, below the unchanged 6 GiB reserve. New constrained
cohorts remain held; no resize, restart, pruning or reserve relaxation was used.
The existing disk-expansion proposal still awaits approval. A later read-only
check of the active RustFS `/data` named Docker volume observes 100% usage and
zero available bytes. Readiness HTTP 200 does not establish write availability;
the broader backend campaign now fails actual PUTs. No new backend or constrained
cohort can be qualified under these conditions. Preserve all failed data and
resolve capacity through the existing approval boundary before another campaign.

Hosted SDK qualification at `753afb8c` still fails three native disaster cases
at `authorize-seal`. It also exposes a performance evidence guard that cannot
extract `s3` operation names and omits five existing native command phases.
The latter error reproduces locally and is repaired at tooling input `0ebbf597`:
19 owning tests pass, and the exact CI guard command runs 343 tests with one skip
and no failures. Complete historical evidence schemas remain explicit; current
phase samples require every current counter. The
[metric qualification](metric-vocabulary-qualification-2026-11-16.json) changes
no Rust runtime inputs. Fresh hosted CI, disaster authorization and container
capacity qualification remain open; the local guard repair does not establish
release readiness.

Native fitting-credit progress is now qualified at `1ac2cced`: the actual reused
archive registry receives the publication's private payload hint while keeping
the original shared bank and namespace capabilities. The
[source-specific record](payload-capacity-wake-qualification-2026-11-16.json)
turns four original S3 deadline failures into four passing two-command invocations
with identical final oracle bytes, real reader joins, exact receipts and fresh
selected-cut byte verification. Background/expired physical-capacity controls,
existing settled retirement assertions, full native core/controller/archive/
execution, selected SQL/KV/Queue/recovery and strict all-target Clippy pass.
This does not close payload cancellation/deadline/uncertain-cleanup coverage or
qualify current RustFS/constrained performance. The latest matched read-only
backend check has only 12,066,816 bytes available, below the original 6 GiB reserve.

Hosted `edeb5c0d5` JavaScript/Node jobs and Python examples pass. Typed RPC full
(3.14) fails Python-provider proof-wait warmup with owner/ingress lease expiry;
the other three RPC jobs pass. Recent bounded renewal observations are ambiguous
before watchdog fencing, under that fixture's original 1,000 ms lease. Do not
extend it, infer noncommit or replay an unknown warmup mutation. Reproduce the
owning lease I/O/confirmation/scheduling boundary and preserve new failure data.
SDK's native 3.14 job also fails four disaster cases, including voter restart and
authorization/capture-source unavailability. Both embedded-Python jobs fail to
compile `python-fleet` because its shared measuring frontend references the
undeclared `crate::object_clients` module. These current failures need separate
repairs; a native payload-wake pass does not replace them.

The frontend module gap is locally repaired at input `dbef0707`; the
[qualification](python-fleet-clients-qualification-2026-11-16.json) preserves the
same two-error compiler control, passing example check, actual cross-runtime
client-factory test, complete embedded-Python test-target compilation and strict
example Clippy. Only `tools/python/fleet.rs` wiring changes after the native
runtime qualification. Full Python adapter runtime/3.14t execution and fresh
hosted CI remain required; the native disaster and Typed RPC lease failures are
separate unresolved boundaries.

The [periodic purge qualification](periodic-purge-qualification-2026-11-16.json)
removes two reproduced periodic file-sync calls per owning test invocation while
preserving actual durable writes, explicit purge and eligible deletion/recovery.
Final input `9b684f00` passes storage 23, core 1,233, controller 46 and strict
affected-target Clippy/repository checks. The legacy fault fixture now seeds
eligible files and retains its original failure/join/reopen assertions and
five-second budget. The core/controller/measuring executable hashes remain
unchanged by that fixture-only follow-up. Earlier failed owners remain recorded.

This is not a rooted-128 latency repair: both focused invocations still exhaust
the original 60 seconds with no periodic purges on any voter; the full measuring
target passes 25 and fails one. Continue at preparation, persistence and physical
close rather than attributing the remaining timeout to this removed work. Shared
host compiler activity prevents an isolated wall-time comparison. The original
RustFS data volume is again observed with zero available bytes, below reserve;
no constrained or real-backend campaign runs and no VM settings change.

The complete failed hosted RPC artifact localizes its owner renewal to about
774 ms inside native COMMIT after starting with 664 ms of authority. Timeout
delivery overruns by only 2 ms, and the watchdog fences before that commit
finishes. This narrows that incident beyond the bounded ambiguous-renewal trace;
it does not separate sync/checkpoint/native-scheduling causes or repair the gate.
Keep the original lease and fence, preserve new disks, and do not replay an
unknown warmup mutation under a fresh identity.

| Priority | Required outcome |
| --- | --- |
| P0: request and authority progress | Resolve SQL/KV/Queue deadline failures and node lease expiry without extending deadlines, weakening fences, increasing banks or retrying an ambiguous mutation under a fresh identity. Queue delivery must verify all acknowledged IDs and bodies; an incomplete drain is unqualified. |
| P0: isolate the failing boundary | Keep the physical-credit wake, cadence, eligible-purge/failure-reopen, dedicated lease-pool and applied-evidence enrollment regressions. Measure their effects in the constrained matrix before attributing earlier lease expiry or latency to a specific boundary. Resolve the remaining preparation, persistence and physical-close costs behind the cumulative rooted-128 enrollment timeout; its successful SQL resolution checks do not establish request success. |
| P0: restore qualification | Restore original-backend write capacity without deleting retained failures or relaxing the reserve. Requalify Python runtime/free-threaded modes after the repaired frontend module, and reproduce current native disaster/voter restart and typed-RPC lease failures at their owning boundaries. Preserve the historical container-capacity failure despite current JS CI passes. Extend native payload-credit progress to cancellation/deadline/uncertain-cleanup coverage and real-backend qualification with exact identity/proof and terminal data refusals. Keep original deadlines and refusal assertions. A later narrow pass cannot replace a failed broader owner. |
| P1: prove performance | Obtain complete passing SQL/KV/Queue cases in every durability mode, then a matched-source before/after comparison with p50/p95/p99, errors, CPU, I/O and archive lag. Failed or unmeasured windows cannot establish a speedup. |
| P1: finish lifecycle and public contracts | Close full CG-17, remaining authority/read partition cases, physical reclamation with retained owners, once-only capacity settlement and repeated reserve-aware movement. |
| P1: qualify the release | Run combined-source recovery/crash/wake/transition and feature/SDK/Axum/daemon gates, plus independent physical failure domains. Three containers in one Colima VM do not qualify domain loss. |

No complete OPT-01 through OPT-08 package or remaining CG release gate is closed
by this comparison or by a scoped native optimization pass.

## Primitive delivery priority

Full public primitive compatibility is the required Quorum product contract.
The remaining work includes missing adapters and failing runtime gates; a scoped
SQL/KV or filesystem pass is not the final supported scope.

At combined source `c13a4745`, the public filesystem/inbox adapter passes both
native and real-RustFS image forms. Queue claim/lease coalescing passes all eight
native/RustFS image/minimum configurations and preserves denied-publication
refusal before consumer entry. Queue fault/retention, ordinary alarm failures,
Fleet conformance and selected real-RustFS Workflows pass. Both load campaigns
remain FAILED: native producer sequence 9 and RustFS producer sequence 7 exceed
their original request budgets before final drain. An initial RustFS proof run
also fails managed trust-refresh startup; the later pass does not explain it.
See the [current qualification](progress.md#cg-17-combined-filesystem-and-queue-proof-qualification).

At source `6282bb46`, the EventStream source journal and shared work bridge pass
40 owning units and six Fleet conformance tests. The `11c223a7` follow-up fixes Workflow test placement and passes strict
library/all-test Clippy without changing production code.
This does not qualify the authenticated destination or public EventStream path.
See the [source checkpoint](progress.md#cg-17-eventstream-source-journal-and-shared-work).

The [current checkpoint](progress.md#current-integration-checkpoint) includes
main `189edd9a`. Runtime `701b6d6f1` closes a physically reproduced checkpoint
stall: an exact applied source checkpoint can resume under the original ready
barrier and live invocation fence while another cell's later committed apply is
blocked. New entry, original deadlines and S3 archival requirements remain.
The focused cohort passes **8/10 selections**: both backend/image checkpoint and
continuation cases, both entry readiness/deadline controls and both SDK controls.
Both selected named EventStream cases still fail initial activation on node 0
with materialized images. The other public selections were not rerun. This does
not close cold binding activation or the full primitive MVP.

Earlier runtime `086f1663` reuses an unchanged Quorum entry image for a
first immutable dependent read. Its baseline precedes maintenance/constructors;
dirty and S3 sources retain checkpoints. Ordinary SDK queries select read-only
dispatch. The regression passes native and real RustFS in both images for clean
Quorum, dirty Quorum and clean S3 sources, and both final SDK controls pass.
Previous idle archive reuse retains **32/32 owning selections at `cfd83b13`**,
with original quotas and exact proofs. These are scoped checkpoints.

**The last complete sixteen-case public cohort remains FAILED: 6/16 at
`086f1663`.** Both SDK
source-proof and both history controls, registration and Fleet pass. All eight
EventStream selections fail: five activations and three later Consume paths.
Native Queue fails producer sequence 33; RustFS Queue drain expires without a
labelled phase, so producer completion remains unproven. Final cohort 301 passes
8/18 selections including the two new regressions. The intermediate SDK handoff
failure is retained; its final control retries only known pre-admission
`NotReady` with unchanged fences and one original five-second budget. No public
stability, full primitive acceptance or latency improvement is established.

1. **Close multi-step request and cold S3 binding progress.** The clean-read
   optimization and fenced per-cell checkpoint continuation do not close cold
   activation or cumulative source preparation/checkpoint and settlement work
   under original request deadlines. Isolate the delayed broker
   transition using exact parent/child identities under original budgets;
   successful metadata polling does not prove child progress. Reproduce any
   checkpoint/readiness or archive-proof scheduling boundary before changing it.
   Do not infer noncommit from one
   voter's retained images, weaken proof, extend deadlines or repeat broad
   campaigns in place of a focused failing boundary. Then finish autonomous
   interrupted binding/delivery/ack recovery, safe unused-sequence sealing, long
   polling, deletion/recreation and retention through movement and fresh-history
   recovery. The [EventStream contract](event-stream-durability.md) keeps separate
   source, destination and output boundaries.
   At integration `d05987085`, ordinary Workflow Axum extent `create/capped`
   and a materialized filesystem initial write to cell 1 still exceed their
   original deadlines. The filesystem failure precedes outage/movement/wake.
   Include these public paths in the shared request-progress diagnosis without
   assuming they share a cause or that earlier lifecycle passes remain current.
   Coordinated successor `177c7f7f5` passes all eighteen scoped Workflow
   configurations, including that creation path. Preserve the earlier failure;
   it does not establish an exclusive cause or accept filesystem lifecycle.
   Prioritize public reconciliation's remaining image reopen, EventStream and
   original-deadline S3-source binding progress before another full cohort.
2. **Close Queue and shared startup failures.** Keep the passing coalesced proof
   and fault oracles. At `086f1663`, native producer sequence 33 exceeds its
   deadline; RustFS drain expires without a labelled phase. Establish that stage
   before attributing the failure to post-producer delivery.
   Resolve both under unchanged load and budgets. Full delivery of either
   528-message campaign and load metrics are not established.
   Qualify both image forms.
   Resolve the managed trust-refresh failure and qualify concurrent boot
   reconciliation. General failed-startup joining and disk retention are now
   implemented, with the typed profile-refusal regression passing at `49509b38`.
   Do not attribute intermittent failures to an unproven cause.
3. **Complete bindings, durable time and Workflow lifecycle.** Qualify independent
   target-S3 minima, interruption before target proof, unused child sequence
   sealing, concurrent/cross-class creation and large calls. Complete SQL
   sessions, KV TTL/revision, schedule deployment changes, clock discontinuities,
   long continuous callbacks, movement/wake and cold-start contention. Ordinary
   and scheduled Workflow passed all eight real-RustFS native/Axum configurations
   at `7e4347e9`; this does not accept its complete family matrix. The
   [read-deadline follow-up](progress.md#cg-17-workflow-native-read-progress)
   at `54a8379d` fixes blocked-local replica routing and unscheduled membership
   read retries. Twelve selected checks pass on the recorded workspace voter
   volume, and the clean expanded RustFS campaign passes all five cases in both
   image modes (10/10 configurations), including native/Axum, schedules and
   repeated concurrent status/recreation. Preserve the failed default-temp
   creation/enrollment controls and keep their cause open; independent-voter,
   full-size, long-callback and combined lifecycle/recovery gates remain. Reuse
   bounded shared owners rather than per-idle-cell tasks.
   The [Workflow replacement/wake gate](progress.md#cg-17-workflow-replacement-and-wake)
   passes native and all four real-RustFS native/Axum image configurations at
   `4fe5d3bdd`, after correcting late-event classification for fully removed
   voters. It covers committed waiting steps/streams, S3 replacement, public-read
   wake and retained restart. Keep the earlier Axum failure and initial-readiness
   controls; in-flight effects and the full family fault matrix remain open.
   The [committed delete gate](progress.md#cg-17-workflow-committed-delete-settlement)
   now passes all four real-RustFS native/Axum image configurations at `d05987085`:
   held callbacks cancel, absence is proven and deleted IDs recreate, including
   after retained restart. Retirement waits for the committed-result handoff so
   it cannot invalidate its own immutable preparation. The broader same-source
   Workflow suite at that source remains failed by the separate extent creation
   deadline above; the coordinated successor's eighteen configurations pass.
4. **Extend payload and provider lifecycle qualification.** The
   [filesystem/Blob/payload checkpoint](progress.md#cg-17-filesystem-blob-and-retained-payload-roots)
   passes scoped real-store movement/wake, archive outage, fenced disk-loss
   recovery, actual-byte verification and retained-cut/reader collection gates.
   Complete the wider KV lifecycle and interrupted-transfer matrix, scheduled
   collection policy, explicit unknown-owner reconciliation, provider-process
   recovery and archive outage/load qualification. Preserve the recorded small
   operator-GET deadline failure and independent provider commit semantics.
   Deliver HTTP/streaming, WebSocket reconnect/stale-owner handling, container and
   external-provider fencing/reconciliation, and stateless/ephemeral and optional
   adapter conformance. See [payload storage](payload-storage.md) and
   [Workflow durability](workflow-durability.md).
5. **Qualify and cut over at one release revision.** Complete CG-13 managed
   recovery and all CG-17 API/scenario/configuration gates, then CG-15/16 with
   constrained independent Linux voters, partitions, replacement, split/merge,
   park/wake, archive outages, physical reclamation, crashes and fenced all-voter
   disk loss. Qualify late metadata after physical removed-member reclamation,
   when the joined roster entry is replaced by its persistent tombstone; the
   Workflow retirement gate does not exercise that boundary. Bound archive
   verification by generation/byte capacity as well as
   replay work. Require complete coverage evidence, public SDK/native/Axum/CLI/
   admin and Fleet/bucket/feature-mode regressions, then remove legacy Raft paths.
   Quorum refusal must never fall back to weaker durability.

The primitive catalogue remains partial. Full-plan acceptance is **13/17 tasks
and 147/195 cases**; no complete primitive family is accepted. Full CLI plan
preflight also retains the missing historical
`CG-01-06-gates-e32115aa-02/CG-01-record.json` evidence gate. The reviewed daemon
source anchor is refreshed separately after current runtime qualification; neither
that refresh nor verifier unit passes replace complete release evidence.

## Current boundary

CG-01–12 and CG-14 passed their recorded scoped acceptance gates. Shared group owners,
cell-specific SQLite histories/receipts/effects/versions, complete checkpoints,
archives/reclamation and fenced fresh-history recovery have scoped coverage.
Managed startup, directory refresh, membership send fences, capacity admission,
peer/S3 replacement, stop/return/resume and retained initial/joined restart pass
component cases. Automatic S3 voter recovery reaches terminal removal while
retaining the obsolete engine and disk. Production platform fencing and the complete fault matrix remain in CG-13–16.

An earlier eleven-target campaign recorded 308 passes, 2 failures and 39
explicit skips. Subsequent fixes and qualification retain their own source
records. At `84942c4e`, all ten CG-07 acceptance commands passed, including
the five exact public cases and strict all-target Clippy. The managed lifecycle
passed both real RustFS image modes. The constrained Linux campaign also
passed both modes with failed-voter replacement. Full release qualification
remains in CG-14–16.

The request case qualifies follower writes, explicit namespace/process grant and
revocation, remote minimum-version/linearizable reads, forwarded S3 acknowledgement,
joined leader-stop failover, retained restart/deduplication and quorum-loss refusals
in both image forms. The public counter requires all six native/Axum addresses to
serve original retry receipts before and after retained restart. Current-source
RustFS results and all exact source/binary limits are recorded in
[progress](progress.md). Earlier disk-erasure and other checkpoints retain their
recorded sources and are not relabeled as this campaign.

Ingress without a local replica can serve reads and writes through trusted group
routes. SDK command forwarding retains the original owner and deadline and
requires namespace write permission. Local SQL, effect and archive proof remain
distinct from authenticated remote output. The managed acceptance exercises
native and Axum ingress with zero local groups. SDK enrollment now uses that
path and preserves the exact creation record at both durability minima.
A configured managed router now refreshes its voter routes after a replacement.
The native and real RustFS cases cover the retired route, joined-voter reads,
original receipt replay and a fresh quorum write. Cold return after all birth
voters are replaced and stopped passes the focused native and RustFS cases.
The public SDK provisions a declared managed standby and reserves, publishes
and confirms its first full-range group. The CLI and admin API admit capacity
and initialize the rooted archive. The managed SQL daemon discovers the group
and waits for the exact archive and fresh quorum before opening application
ingress. At `cd1d8315`, the native process gate and constrained Linux RustFS
gate passed in both image formats. The latter uses separate voter containers
and disks in one VM with fixture capacity. It covers native and Axum reads and
writes, voter SIGKILL, receipt replay, retained restart and quorum-loss refusal.
Production capacity admission, interrupted-operation schedules and sustained
load qualification remain open.

The managed native and real RustFS request cases now cover foreign cluster,
namespace and cell histories through both HTTP hosts, and foreign fleet,
namespace and physical group requests through the admin API. Valid retries
preserve the original receipt after those refusals. The named public authority
and occupancy-one cases pass their recorded native scope.

CG-13 and CG-15 are in progress. CG-14 has passed its scoped harness gate.
CG-17 is in progress. The independent evidence gate and starter API catalog
are implemented; public primitive adapters and the complete matrix remain open.
CG-15 and CG-16 depend on CG-17.
CG-16 is pending the combined release gate. Individual checkpoints cover operator-driven split and merge,
placement recommendations, fenced parking and wake, managed recovery and
parts of the qualification matrix. The task-level evidence remains open.

The managed cold-recovery case now erases every owned source voter disk and
reopens from RustFS in both image modes. It verifies restored SQL and receipts
through a fresh quorum, resumes interrupted archive setup, and retires the old
histories. The independent Docker workflow at `b4dd07b5` adds permanent launch
revocation and complete disk erasure for an unpublished split; broader transition
and effect scenarios remain open. The combined constrained Linux campaign at `27fde17e`
passed native and Axum SQL, leader loss, quorum refusal, archive outage,
retained maintenance and failed-voter repair in both image modes. The voters
had separate cgroups and disks in one VM. Independent process crashes during
Joint, peer activation cancellation and multi-namespace routing still need
their own qualification.

## Open case audit

The [implementation index](implementation-plan.md) records thirteen accepted
tasks and 147 of 195 required cases. CG-07 has a complete acceptance record
for its five public cases and all required commands at `84942c4e`.
Each remaining task still needs the exact target, case oracles and evidence
specified by `plan.py show <task>`. The index counts acceptance through complete
task records.

CG-11 now has its complete five-case/ten-command record at `ddb545e4`, with six
additional catch-up, monotonic-trust, authentication/scope and joined-shutdown
regressions. The 64-cell/seven-group workflow passes natively and against real
RustFS in both image modes on the same scheduler executable. The final fix makes
inspection trust catch up within the original request deadline while preserving
exact boot/identity fences and monotonic installed cuts. Existing resource banks,
leases, deadlines, overflow assertions and recovery reserve are unchanged.

Native and RustFS records are linked from [progress](progress.md). The RustFS
server uses one CPU/2 GiB; voters are host-native fixtures. Both owned services
are stopped and objects retained. Earlier failed controls remain retained.
Full load, physical peaks, independent domains and soak remain CG-15/16 gates;
this functional acceptance does not waive the occupancy-128 load failure.

CG-09 has a complete seven-case, 12-command acceptance record at `f4d46d98`.
The publication-erasure case also passes both real RustFS image modes.
CG-10 has a complete five-case, ten-command record at `8b410f25`. Movement,
pre-copy ownership and partial merge also pass real RustFS in both image modes.
The public SDK now follows successor groups for strong reads and receipt
inspection. See [progress](progress.md) for exact artifacts and limits.

| Task | Checked-in coverage | Acceptance still required |
| --- | --- | --- |
| CG-11 | **Accepted: 5/5 cases and ten commands at `ddb545e4`.** Six focused regressions and the full 64-cell/seven-group native/RustFS workflow pass in both image modes. | Functional acceptance is closed. Release-scale profiles remain CG-15/16. |
| CG-12 | **Accepted: 5/5 native cases and ten commands at `2cd71d32`.** Five cases cover both image modes; seven additional protocol/routing regressions pass. Includes all-voter eligibility, interrupted delivery, first read/write wake, retained restart and leader loss. | Functional acceptance is closed. RustFS, physical/load and wake-latency profiles remain CG-15/16. |
| CG-13 | At `b85c609c`, three native RF3 suites pass real RustFS closure selection, recovery-seeded SDK retries and uncertain outbox recovery in both image modes. Seven archive safety cases pass with unchanged budgets. Earlier Docker permanent-fence/disk-erasure results retain `320a40ce`. | Implement stable managed-group recovery admission, then close the three missing lifecycle/fence/all-cells entry points, all six exact case gates and the full 256-cell `rustfs_disaster` profile. Scoped native or Docker drills do not accept the complete recovery gate. |
| CG-14 | **Accepted: 5/5 cases at `23414b47`.** Owned, source-pinned serial campaigns now enforce disk bounds and retain operation, replica, archive-lag, file/engine, kernel and joined native-close evidence. The five-process Quorum matrix passes both modes at occupancies 1/2. | Harness implementation is closed. CG-15 must qualify full profiles, complete historical controls and authority/lifecycle adapters, and resolve the retained S3 deadline miss; a passing reduced reproduction does not waive it. |
| CG-17 | In progress. Ordinary RPC roots/sync, bounded activations, configuration and same-cell interleaving have scoped coverage. Canonical SDK commands, queries, receipts and enrollment now wait for source proof, retain independent target contracts and support recursive forwarding. At `2f42cb5f`, the public KV cursor checkpoint passes three owning native selections, focused strict Clippy and all six primitive real-RustFS cases, including current-incarnation discovery, with matching executable hashes and both image modes. The earlier intermittent S3 failure remains unresolved. See the [checkpoint](progress.md#cg-17-storage-keyset-cursors). | Finish generation-aware named bindings, other output/dispatch adapters, remaining Storage methods, compiled export/deployment audit and real evidence producers. Deliver committed due work and remaining family/provider adapters, then pass all 32 cases and the matrix in [qualification](qualification.md#full-primitive-compatibility-cg-17). |
| CG-15 | Checkpoints cover individual public faults and archive recovery. | Run the history oracle, full crash and transition matrix, archive reclamation under load, wake profiles, soak and independent-domain loss. |
| CG-16 | Public shared-group SQL has scoped coverage. The per-cell host and its seed, archive and inspection wire adapters are retired. | Audit the remaining low-level verification surface, prove full public operations and joined shutdown in all modes, and bind release evidence to one source. |

## First delivery of quorum SQL through public ingress

An explicitly configured Raft namespace defaults to three voters and a
quorum durability minimum. Public Raft commands default to quorum durability
and queries default to linearizable reads. A quorum acknowledgement requires
a committed command and its applied receipt. Explicit S3 durability waits for
archive proof. The release should define which SQL capabilities are available
through the shared backend and preserve typed refusals for the rest.

CG-11's placement, adaptive partitioning and bounded request/control path are
accepted at `ddb545e4`. CG-12's fenced parking and demand-driven fresh-quorum wake
are accepted natively at `2cd71d32`. The next feature priorities are:

1. **Complete shared SDK execution and primitive adapters.** Ordinary JSON RPC
   now suspends on shared workers and settles independent root transactions from
   exact quorum/S3 proofs. A pending invocation retains its original receipt
   identity and refuses whole-handler reexecution. The RF3 case at `77075d36`
   covers both image modes, partial failure, leader transfer and lost-response
   replay. Ordinary Storage sync now shares those checkpoint and admission
   owners; see its [current qualification](progress.md#cg-17-ordinary-storage-sync).
   Persistent native activation passes the [native and real-RustFS checkpoint](progress.md#cg-17-persistent-native-activation).
   Declared variable admission and configured factory definitions pass the native
   checkpoint at `f09613c1`, along with both real-RustFS cases and the existing
   production daemon/CLI regression. Strict all-target Clippy and repository
   checks pass; see the [configuration checkpoint](progress.md#cg-17-declared-native-deployment-configuration).
   Same-cell interleaving now passes the [native and real-RustFS checkpoint](progress.md#cg-17-same-cell-rpc-interleaving)
   at `3150398c`, including private/atomic exclusion, leader fencing, parked
   shutdown and retained restart without replay. Canonical dependent SDK calls
   now pass the [current checkpoint](progress.md#cg-17-dependent-canonical-sdk-dispatch)
   at `fcab2005`: 25 selected native tests, the existing daemon/CLI campaign and
   four real-RustFS cases pass. Commands, queries, receipts and enrollment retain
   source proof, target policy, original deadlines and bounded child ownership;
   recursive calls and callback-error refusal are covered. Next finish
   generation-aware ordinary named bindings and other output/dispatch adapters. Every
   dependent dispatch must carry current authority and wait for its source
   transaction proof while preserving the target's separate durability contract.
   Then complete the remaining
   Storage methods, the compiled export and
   deployment audit and committed due work for the
   filesystem, payload, Queue, EventStream, Workflow and provider adapters.
   Preserve Fleet/bucket and feature-mode conformance. See the
   [independent transaction checkpoint](progress.md#cg-17-independent-rpc-transaction-checkpoints).
2. **Finish CG-13 managed recovery from S3 alongside CG-17.** The `b85c609c`
   closure, held-receipt retry and outbox cases pass real RustFS in both image modes. Next
   implement explicit recovery admission/selection for a stable managed group;
   do not require a fabricated split/merge or weaken the Fleet disruption,
   directory, fence or new-incarnation checks. Then implement the three missing
   lifecycle/fence/all-cells case entry points, pass all six exact gates on one
   source, and run the full 256-cell/five-node disaster profile. Preparations
   remain sequential under the unchanged one-operation pool. Current-source
   permanent Docker launch revocation and constrained voter qualification remain
   distinct from the passing native fencing-contract fixtures. See [progress](progress.md).
3. **Complete CG-15 release qualification.** After CG-17 acceptance, resolve the
   occupancy-128 failure within its original bounds. Run load, reclamation, crash,
   partition, wake and disk-loss profiles, including Joint crashes, peer activation
   cancellation and multi-namespace ingress. Record physical peaks and the planned
   wake percentiles. Harness fixes and scoped campaigns can proceed earlier.
4. **Ship through CG-16.** Complete public default cutover and the remaining legacy
   audit after CG-17 compatibility and combined qualification pass on one source.

The [TiKV-informed optimization proposal](tikv-informed-optimization.md) defines
additional direct-routing, cache-repair, cleanup and worker-efficiency packages.
Reuse accepted CG-11/12 behavior and qualify new optimization oracles separately;
the proposal neither reopens those task acceptances nor claims measured speedups.

## P0: finish the MVP and its merge gates

CG-07 has passed its five named public cases and all required commands at
`84942c4e`. Its native and constrained Linux RustFS results are recorded in
[progress](progress.md). Those accepted SQL gates are complete at their
recorded source and scope. Expanded fault, cancellation, load and combined
release qualification belongs to CG-14–16.

The performance runner retains the original 60-second enrollment budget and
five-second per-call cap. On `beeaa1b3`, the focused occupancy-128 case passed
with 45.112 seconds for enrollment and 150.69 seconds for the full data checks.
The original occupancy matrix still failed at occupancy 128 after 66 cells.
The smaller occupancies enrolled in 2.448 and 12.553 seconds. A process sample
places substantial time in blocking file synchronization. The manifest update
now uses one directory barrier while retaining every page-file barrier and
quarantining uncertain writes. These results establish scoped correctness and
an unresolved matrix gap; they do not qualify the load profiles.

CG-10's full operator correctness gate is accepted at `8b410f25`, including
complete write/effect/receipt reconciliation and physical pre-copy admission.
Its real RustFS drills pass both image modes. Continue with CG-13 managed lifecycle and recovery; preserve the retained
S3 load failure as a CG-15 release gate. Preparation uses
private staging and complete sealed-archive replay; incremental transfer
performance and sustained load remain qualification work.

The former per-cell host and wire paths are retired. The generic `Replica`
still executes shared groups. Remaining legacy cleanup must audit cell-scoped
archive and recovery verification APIs and their shared image/capture dependencies.

Local and remote request attempts must keep the same routing session,
admission and original deadline. Incoming RPCs execute locally; leader hints
and discovery supply destinations, never authority. Preserve typed refusal
and ambiguity, original receipts and physical responder observations.

The remaining RustFS disaster gate permanently fences every old writer,
erases all voter disks, and restores a new incarnation from complete S3
closure. Verify original receipts and client floors, old-process and launch
refusal, fresh quorum, new writes and retained restart in both image forms.
Local reclamation under load requires archive coverage and complete outcome
accounting. Keep loopback, process and independent-host scopes explicit.

## Source-sealing implementation boundary

The unmanaged verification directory roots an exact split or merge plan in the
range head CAS. It retains Planned, Prepared, SealAuthorized and Cancelled
metadata across owner restart. Authorization and cancellation compete on the
same prepared head. Ordinary range publication, import and disaster recovery
must acquire the topology head without an active transition. Routing continues
to select the original parent ranges until the verified transfer publishes.

The managed transition holds the fleet disruption slot from admission and
binds source seal and child installation to the exact plan and physical
replicas. The parent drains accepted proposals before committing the seal.
The child install, fresh-term quorum set and verified source archive closure
pass their focused native and real RustFS gates. Atomic publication and
replicated child activation have a focused native test that also checks
retained cell values, original receipt replay and a new quorum write on each
transferred cell. All seven planned CG-09 cases, including controller loss at
every phase, publication-erasure closure and retirement pins, pass the complete
12-command native gate at `f4d46d98`. Broader process-crash qualification
remains in CG-15.

The sealed-source journal accepts one explicit archive hold per planned parent.
Each hold must belong to the namespace-bound archive and independently restore
the matching replicated seal and its complete image closure. The directory
records the exact ordered set only after verification. The transition owner
retains those holds until retirement and re-verifies them after restart.

## Default backend, capabilities and deployment

An explicitly configured Raft SQL namespace uses shared groups, defaults to
quorum minimum durability and three voters, and defaults commands to quorum.
CG-07 qualifies its namespace provisioning, SDK, CLI, daemon and public ingress
at the recorded boundary. Broad default selection across all cell classes and
the final deployment cutover require CG-17 compatibility and CG-15/16 release
qualification.
Missing quorum must refuse admission without selecting weaker durability.
The intended contract is three-replica shared groups, quorum writes and
asynchronous S3 archives, with measured archive lag/RPO. An explicit S3 write
requires its separate complete coverage proof.

CG-17 requires equivalent public adapters for all Cellar primitives, including
Workflow/Queue callbacks, WebSockets and mutable-container behavior. Its
[compatibility contract](qualification.md#full-primitive-compatibility-cg-17)
defines implementation and acceptance; narrowing the supported product scope
cannot substitute for delivery. Preserve capability refusals during development
and keep CG-17 open until the complete matrix passes. Existing Fleet data requires a separate deployment decision:
this plan supplies no online Fleet-to-Raft conversion and must not reinterpret
old data as a new Raft history.

The shared-group SDK and counter ingress default to linearizable reads.
Callers can select a receipt-based version floor or explicitly permit stale
reads. The recorded public request cases cover these policies; the combined
release must retain their oracles independently of storage-backend selection.

## P1: complete the requested architecture

These are required follow-ups, not removed scope or optional substitutes for the
full hard cutover.

| Task | Deliverable and dependency |
| --- | --- |
| CG-09 | Irreversible source sealing and whole-cell transfer, with cancellation/seal serialization, quorum installation and atomic range publication. Preserve cell histories, receipts, effect IDs and remote recovery closure. |
| CG-10 | Split hot groups and merge compatible adjacent ranges using CG-09; recover partially completed transitions without reopening a sealed source. |
| CG-11 | Automatic RF3 selection, adaptive split/merge, durable continuation, whole-group balancing/repair and bounded inventory/discovery with recovery reserve. |
| CG-12 | Fenced group parking and authority-checked wakeup. Resume actual membership/quorum under the original request deadline; measure cold response latency rather than assuming immediate readiness. |
| CG-13 | Managed S3 recovery of complete groups and incomplete transfers/splits/merges, including zero-quorum fencing, fresh incarnations and unfinished lifecycle journals. |
| CG-14 | **Accepted harness infrastructure.** Reproducible process/RustFS campaigns with real cells/groups, source/binary/profile hashes, enforced resource bounds, complete ledgers, archive lag and joined owners. Full release execution remains CG-15. |
| CG-17 | Full public primitive adapters, capability/budget validation and the complete RF3/RustFS lifecycle matrix. Acceptance depends on CG-07/08, CG-10, CG-12, CG-13 and CG-14; implementation can begin with the existing SQL/effect foundation. Required before CG-15 and CG-16. |
| CG-15 | Full load/crash, transition, wakeup and disaster-recovery matrix. Measure the documented profiles and failure schedules; no capacity or TiKV performance-parity claim follows from correctness tests. |
| CG-16 | Delete remaining predecessor consumers/codecs, finish feature-off/full release checks, publish the unified API/runbooks and qualified operating envelope. No one-cell special path, compatibility reader or production mode toggle. |
