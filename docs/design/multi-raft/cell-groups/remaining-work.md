<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Remaining cell-group work and priorities

Purpose: Prioritize the remaining cell-group MVP, merge gates and full architecture deliverables.

Machine-readable qualification and benchmark outputs are retained separately.
Keep generated run outputs outside this documentation tree; the prose summaries
preserve their source, scope and unresolved failures.

The current delivery is [PR #184](https://github.com/crabbuild/cellar/pull/184),
synced with main `2a37acf6a` (#185), with its documentation conflict resolved.
Merged runtime `10fb1775d` batches the Outbox claim, retry reservation and
binding intent into their mandatory source proof before dispatch. Its new
control passes Quorum and S3 sources against real RustFS, including scoped
archive refusal, autonomous recovery and unchanged destination records after
receipt replay. Both full six-thread library runs pass 1,428 tests with zero
failures and two existing ignores. Build, strict Clippy and repository checks
pass. The current library gate is green; prior failed runs retain their pins.

The [merged qualification](progress.md#cg-17-outbox-proof-and-main-185-qualification)
still fails original EventStream Consume in native, Axum and RustFS paths.
The real campaign passes 2/5 complete functions. Container recovery passes
3/4 profiles and Sandbox recovery 2/4, including both complete Quorum Sandbox
lifecycles. Remaining S3 failures are file reads: Container materialized
`docker-a Read5`, Sandbox materialized `FileRead2`, and Sandbox extent `Read5`
after original lost-Exec receipt recovery. No deadline, identity, durability
minimum, ambiguity rule or fencing requirement has changed. Default cutover,
full primitive acceptance and independent Linux/load/disk-loss gates remain open.

Earlier runtime `a763e2f92`, based on main `49828ff6b` after #183/#182, preserves the
configured background archive interval after quiet completion; new S3 requests
still wake immediately. Its real controller regression fails before the fix and
passes both image forms afterward. Six native archive functions and two full
six-thread library runs pass (1,420 tests and two existing ignores per run).
The [earlier qualification](progress.md#cg-17-quiet-archive-cadence-and-merged-source-qualification)
still fails both EventStream replays and the combined RustFS campaign:
3/8 complete functions, with 16/22 Docker/Sandbox configuration markers.
Earlier failed library runs retain their source pins; that library gate
passes on `a763e2f92`. Default cutover and full primitive acceptance remain open.

The `f668d0193`/`8b9fece2d` archive results and the #183 optimization results
below precede this integration. They do not qualify the combined runtime.

## Current optimization follow-up

Merged [#183](https://github.com/crabbuild/cellar/pull/183) is the base for the
verified ingress-successor follow-up at compiler inputs `99a34233`. Four fixed
node-local metadata slots retain single-use advice between independent requests;
Fetch and RPC replenish only after the original success selection. Both public
RF3 image-mode regressions fail their new oracle before the change and pass twice
each afterward, preserving their individual commits and all-voter catch-up.

The expanded owning campaign passes **93/94** selections. An Axum Fetch deadline
failure remains retained despite passing separate pre-change and candidate
controls. The unchanged SQL/KV/Queue service campaign passes **3/13** selections:
SQL, KV and Queue callbacks each pass **1/2**; combined C2 **0/2**, separate extent
load **0/1**, and C4 **0/4**. Full service and low-latency/throughput qualification
remain open. See [the exact scope and retained failures](optimization-progress.md#verified-ingress-successors-across-requests).

The preceding committed-pair checkpoint at `bb1b2cac` passed 48 selected controls
and **5/13** service selections. The separate captures are not a matched
performance comparison. Their failures remain in the [earlier evidence](optimization-progress.md#committed-preparation-with-main-182).

The next optimization priorities are:

1. **P0: Close repeatable ordinary service.** Reproduce the SQL/KV,
   callback and Queue C2/C4 deadline/timeout failures with their original identities,
   typed ambiguity, admission banks and 3,000-ms RPC / 5,000-ms callback /
   30-second drain windows. Preserve exact delivery/uniqueness and the independent
   producer-quiescence proof at retained restart/close. Keep all failed controls.
2. **P0: Account for the remaining persistence path.** Separate packed-manifest
   file sync, directory sync, selector publication and the point at which an
   eligible committed successor becomes available. Current aggregate manifest
   means are about 152–329 ms; refusal counters do not establish a quota repair.
   Require an owning red-to-green regression and the complete unchanged service
   campaign for each next reduction or overlap of measured work.
3. **P1: Close complete optimization and recovery acceptance.** Finish the
   remaining OPT routing/read negative paths, bounded scheduling fairness,
   physical reclamation and reserve-aware lifecycle work, then the combined
   SDK/public/fault and fenced recovery matrix. The broader primitive delivery
   milestones below remain required.
4. **P1: Run the requested constrained comparison after admission passes.**
   Require fresh evidence for three 1-vCPU/2-GiB RF3 nodes/three groups and the
   original shared RustFS reserve before the SQL/KV/Queue Quorum, Quorum+S3,
   Fleet and Bucket matrix at C1/4/16 with reversed repeats. The last original
   RustFS witness reports zero available bytes against the unchanged 6-GiB
   reserve. No new original-store probe or constrained case is admitted here.

## Primitive delivery order

The immediate delivery order is:

1. Close ordinary request deadlines on `10fb1775d`. EventStream Consume fails
   both native three-second replays, Axum and RustFS. Correlate target ACK
   acceptance, response delivery and source continuation before changing the
   path; a later terminal receipt is not evidence of timely acceptance.
   For S3 Container/Sandbox, measure the exact file-read proof and reconciliation
   path: materialized Container `Read5`, materialized Sandbox `FileRead2` and
   extent Sandbox `Read5` remain failed. Remove redundant work only with an
   owning regression that preserves current ownership, prior mutation proof
   and uncertain-operation reconciliation. Both Quorum Sandbox lifecycles pass.
   Preserve 5s/3s/60s budgets, identities, source/target minima and ambiguous
   outcomes. Re-run all original refusal, requested-minimum and output-fault
   gates on the corrected source; these five real functions do not qualify that
   full matrix. Keep the now-passing full parallel library gate. Workflow's
   earlier passes still need current source and combined recovery qualification.
2. Complete the primitive API inventory and public/recovery contracts. Current
   native filesystem, alarms and Queue gates pass, but their full lifecycle
   matrix remains required. Close Container/provider scope, WebSocket/session
   ownership and remaining callback/binding coverage before full CG-17 acceptance.
3. Qualify every primitive through managed replacement, movement, split/merge,
   wake, archive outage, reclamation and fenced zero-quorum recovery. Managed
   Workflow S3 replacement, public wake and retained restart pass both image
   forms; `bf5056b33` also retains eight closed voter directories. The complete
   cross-primitive and fresh-incarnation matrix remains open.
4. Run the unified public daemon, independent Linux voter, load, crash and
   disk-loss matrix on one final source. Earlier Linux SQL/KV qualification
   remains pinned to `41e68a7bf`; it does not qualify subsequent runtime edits.
   Default backend cutover and removal of legacy Raft paths follow these gates.

The [full delivery milestones](#primitive-delivery-priority) and task acceptance
definitions retain their original requirements; no acceptance case is waived.

## Optimization status before #182

### Current main: service deadlines remain open

PR #167 is merged. Reverification of main `0f3b18e7`, compiler inputs
`31dfbc55`, passes the original SQL binding and Queue callback cases. The
original KV case fails rooted `PublicLarge` with a definite runtime overload.
Two fixed, unchanged rooted Queue C4 controls reach load and fail three and four
producers respectively with deadline errors. Both report joined cleanup without
errors. These are native RF3 fixtures, not the constrained Colima comparison.

A one-function SQL continuation scheduling experiment at inputs `28b5d89c`
also fails both fixed C4 repetitions, with four producer failures in each.
Its cleanup reports pass, but it closes neither the service deadline nor a
throughput/latency gate. The runtime change is rejected and restored to main.
All failed media, immutable executables and independently checked copies remain
retained outside this documentation tree. See the
[current-main evidence](optimization-progress.md#merged-main-service-reverification).

Live apply observations at compiler inputs `be721db4` now separate image
preparation, immutable SQLite inspection and changed-manifest persistence.
Seven owning controls pass, including real image/manifest sync failures and
cancelled cleanup in both image modes. Two original C4 repetitions still fail
three and four producers with joined cleanup. Manifest preparation averages
about 91–109 ms in the aggregate snapshots; immutable inspection about
0.9–1.2 ms. These are diagnostic operation means, not request percentiles or a
speedup. See the [phase measurements](optimization-progress.md#live-apply-phase-observations).

The priorities at this checkpoint were:

1. **Account for manifest/image persistence and receipt settlement barriers.**
   The live-apply phase split is implemented and tested. Count actual file and
   directory barriers within the measured expensive preparation paths before
   choosing the next change. The current C4 snapshots show no SQL admission
   refusals; changing admission limits is not an established repair.
2. **Reduce demonstrated work within existing bounds.** Assess opportunistic
   preparation/publication of a contiguous committed prefix, preserving exact
   receipts, Ready/sync ordering, complete root publication, original deadlines,
   FIFO and physical task custody. Existing engine persistence batching and
   per-image directory batching already exist; their presence does not qualify
   end-to-end apply batching or justify removing a persistence barrier.
3. **Close repeatable Quorum service before performance acceptance.** Require
   unchanged SQL/KV/Queue conformance, C2/C4 load and drain in both image modes,
   source-proof refusal, original-ID reconciliation and joined recovery/cleanup.
   Retain every failed control when evaluating a candidate.
4. **Run the requested constrained comparison after its admission gates pass.**
   The original RustFS setup still lacks a passing capacity witness against its
   unchanged 6 GiB reserve. No new RustFS probe or constrained case was admitted
   during this reverification. Three-node resource evidence and the complete
   SQL/KV/Queue comparison remain open.

No fully working primitive release or low-latency/throughput improvement is
qualified by this checkpoint. The broader OPT/CG/SDK/recovery gates below remain
open.

## Prior integration checkpoints

An earlier optimization checkpoint integrates main `a85ab6f6c` (#180). Compiler inputs
`4bcd59fd` pass **1,410 library tests / two existing ignores**, **29/30 selected
native controls**, strict all-feature/all-target Cellar/container Clippy,
**60 Python tests** and repository checks. The S3 read-source reuse case fails
its original target-entry deadline in materialized mode 2 with an ambiguous
parent result; complete S3 read-source acceptance remains open. All failed media
and exact closed copies remain retained. See
[main #180 integration](optimization-progress.md#integration-with-main-180).
Service, full OPT/CG/recovery/release and constrained performance acceptance
remain open.

The preceding checkpoint includes main `09cdcf423` (#179). At compiler inputs
`8240ea6c`, intermediate RPC checkpoint retirement also runs on the existing
joined cleanup owner. SQL validates the exact receipt before handoff and fresh
authority after join; admission, workspace and continuation custody remain held.
The byte-identical RF3 owning regression passes twice after failing twice on the
prior runtime. The full library passes **1,387 tests / two existing ignores**,
alongside **47 selected retirement/receipt/recovery/checkpoint/SDK/admission
controls**, strict all-feature/all-target host/core Clippy and repository checks.
See [intermediate checkpoint retirement](optimization-progress.md#intermediate-rpc-checkpoint-retirement).

**Earlier service acceptance remains failed.** The `8240ea6c` selection passes
**53 of 59 executed selections**; the twelve prior-runtime controls are counted
separately (**three pass**). One already-ignored directed diagnostic earns no
execution credit; its eviction scenario is covered by the passing complete S3
read-source case. Two repetitions with reversed case/runtime order give:

| Original service case | Prior runtime (`98bf1829`) | Candidate (`8240ea6c`) |
| --- | --- | --- |
| SQL bindings | 0/2 | 1/2 |
| KV bindings | 1/2 | 2/2 |
| Queue callbacks | 2/2 | 2/2 |
| Queue C2 load/drain | 0/2 | 1/2 |
| Rooted C4 selections | 0/2 | 0/2 |
| Materialized C4 selections | 0/2 | 0/2 |

All four candidate C4 failures reach load. Two prior C4 failures precede load;
the other six C4 cases report joined cleanup without errors. Startup cleanup
joins are not established by absent markers. The successful candidate C2 run
has both exact **528 deliveries / 528 unique IDs** drains. These observations
establish neither service repeatability nor an end-to-end throughput or latency
gain. Earlier failures retain their scope in the
[implementation ledger](optimization-progress.md).

| Priority | Work required before acceptance |
| --- | --- |
| **P0: Repeatable service and recovery** | Close SQL and Queue C2/C4 deadline/drain failures under the original **3,000-ms RPC / 5,000-ms callback / 30-second drain** windows, exact receipts and unchanged admission. Measure current-source target image/persistence, private capture I/O and bounded-worker interference before selecting another change. Keep successful KV/callback repetitions, checkpoint repeatability and complete-prefix recovery as separate gates. |
| **P1: Shared execution and routing** | Prove fair bounded SQL/apply/persistence/archive service, control/election/shutdown progress, routing/read partitions and public lost-response reconciliation across elections/splits. Preserve Ready/sync ordering, command identity, image pins and S3 proof. |
| **P1: Sustainable capacity and release** | Prove physical shared-segment deletion and once-only capacity credit, repeated reserve-aware movement, interrupted park/wake/recovery, and combined primitive/SDK/Axum/daemon/fault/crash acceptance. |
| **P2: Constrained performance** | Run the original three-node, **1 vCPU / 2 GiB each**, RF3/three-group SQL/KV/Queue comparison against shared RustFS, including Quorum, Quorum+S3, Fleet and Bucket at C1/4/16 with reversed repeats. |

The read-only original RustFS witness at **2026-11-22T15:25:23.264655+00:00**
reports **zero available bytes**, below the unchanged **6 GiB reserve**.
Identity, configuration, resources and start match, with zero restarts and no
case admitted. A fresh three-node resource witness remains open. Full
OPT/CG/recovery/SDK/release and constrained performance acceptance remain open.

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

### Captured receipt-image reuse

Code `4e779bd1` removes a redundant SQLite reopen during receipt reconciliation.
The SQL owner may inspect its live captured connection only when fresh applied
authority selects the exact pending result and the connection has no active
transaction or retained read snapshot. Writable access invalidates reuse. Later
images use immutable inspection. Receipt validation, before/after authority
checks, logical-writer release, retirement and S3 output gates remain unchanged;
no result cache, worker, bank or deadline is added. Main `2f083f14` remains included.
See the qualification record.

At compiler inputs `2e0ae209`, byte-identical native RF3 tests pass in both image
modes while the corresponding unchanged runtime waits on an injected immutable
read. Exact receipts, stale-proof refusal, replay, one callback, all three voter
values and cleanup/export are checked. This qualifies removal of that I/O
dependency, not a workload percentile. The full library passes **1,268 tests /
two existing ignores** and execution passes **237 / two existing ignores**.
Strict all-feature/all-target Clippy, format, layout and native dependencies pass.
Original checkpoint submission, SDK source-proof/fencing and reply-bridge controls
also pass.

**Combined qualification remains FAILED:** thirteen of nineteen selected primitive
controls pass. All four original Queue load selections fail with **13 explicitly
ambiguous deadline producer errors**. One materialized C2 phase completes the
unchanged **528-message drain / 528 unique IDs**, then its rooted phase fails.
The combined C4 selection fails before its rooted phase. All five load phases
report successful three-node/three-host cleanup and complete synthetic exports;
this does not establish full-prefix recovery acceptance.

The broad run also fails rooted ordinary KV `PublicLarge` with an ambiguous RPC
deadline (**2,957 ms remained at its first target attempt**) and the rooted
ordinary Queue callback path. Fresh controls use preserved earlier/candidate
executables with identical owning fixture inputs. The earlier runtime fails KV
at `ReadLarge` (**2,977 ms remained**), while the candidate passes both image
modes in isolation. Both runtimes fail the rooted ordinary Queue callback path.
These observations neither erase broad-run failures nor establish a causal
repair or new regression. Failed KV work can hide a separately returned cleanup
error under its primary error; its successful cleanup is not retroactively known.
The main and paired-control original inventories retain **77,809 files /
2,874,535,899 bytes**, verified closed copies and **51 memory exports / 31,768
objects / 618,390,760 bytes**. Earlier attempts and their media remain retained.

Remaining priorities, in order:

1. **P0 — Repeatable service:** profile and reduce target service and shared-worker
   interference for large KV, Queue callbacks and C2/C4 delivery/drain. Keep the
   original 3,000-ms stateless RPC and 5,000-ms callback windows, source proofs,
   admission banks and ambiguous command identities.
2. **P0 — Recovery and release:** reverify retained recovery failures on current
   source and complete SDK, hosted, feature/backend and fault acceptance.
3. **P1 — Complete OPT-01–08:** finish routing/read negative paths, measured
   persistence/ReadIndex/archive batching and fairness, physical reclamation with
   once-only credit, reserve-aware movement and interrupted lifecycle coverage.
4. **P1 — Constrained comparison:** run the original three-node, 1-vCPU/2-GiB,
   RF3/three-group SQL/KV/Queue matrix on shared RustFS with original workload
   sizes, C1/4/16, 60-second windows and reversed repetitions. A read-only probe at
   **2026-11-20T18:34:45.092185+00:00** reports **6,905,856 available bytes**, below
   the unchanged **6 GiB reserve**. Original identity, configuration, start and
   zero restarts match; no case is admitted. This is not a fresh three-node
   resource witness. Complete OPT and combined release gates remain open.

### Queue ingress: current deadline boundary

Two further source-frozen native diagnostic cohorts reproduce **14 ambiguous
deadline producer errors** across materialized/rooted C4. All fourteen match a
unique Queue cell, command ID, normalized-input digest and offered batch.
Temporary inputs `76318ae0` and `dad293ff` are retained privately; they were
removed and inputs restored to `9748e402` before the qualification-only cleanup
reporting change below. Runtime code still includes main `2f083f14`. See the
source-bound ingress record.

The unchanged fixture has a **3,000-ms stateless RPC operation deadline** and a
separate **5,000-ms callback deadline**. Earlier generic five-second descriptions
of these public Queue RPCs were incorrect and are corrected in the prior
diagnostic record. Downstream commands receive only the remaining RPC window;
no deadline or resource limit has changed. Explicit five-second SDK/checkpoint
controls remain separate evidence.

The eight calls with ingress spans spend **0.58–2.03 seconds in the local FIFO**
before command service. Four use the existing positive-receipt successor hint;
four perform a fresh sequence observation taking **59–389 ms** after an earlier
failure cleared that hint. There is no history-lookup span for these failed
calls. Controller queueing, pending-invocation construction and routing are
short after dispatch. These instrumented phase ranges overlap other work and
are not request percentiles or throughput. The four inherited-hint failures
also mean that eliminating the fallback observation alone is not an established
repair.

All four newly observed checkpoint proof scopes dropped at shutdown **begin
after the failed load enters cleanup**. They repeatedly receive reconciliation
views without the expected exact version; they are not waiting on one hung
mailbox reply. Their shutdown lifetimes do not establish a steady-state
checkpoint scheduling cause. Retain the exact-proof/newer-term/joined-closure
rules; no producer may be retired merely because its response deadline expired.
This does not accept full-prefix shutdown or recovery.

The immediate **P0** is to reduce measured command/image service work and FIFO
interference, with a deterministic matched boundary test before a production
change. Preserve positive-receipt handoff, fresh-quorum consumed-other proof and
the original 3,000/5,000-ms windows. Then pass the unchanged Queue C2/C4 exact
drain and SDK fencing controls, remaining primitives, recovery and release gates.
OPT-01–08 and the full constrained comparison remain open. These diagnostic
cohorts reach neither the final 528-message drain nor a causal performance fix.

Code `26f539a3` also closes a reporting gap: the original producer error could
hide a separately returned cleanup/export error. The Queue fixture now retains
those errors and a final cleanup result without changing calls, order, primary
failure or assertions. Current compiler inputs are `7f240007`; only that test
helper differs from `9748e402`, and runtime inputs are unchanged. Strict Clippy
for the library/native primitive target and its build pass. All four original
load selections still fail with **12 ambiguous deadline producer errors**, but
each now independently reports successful three-node/three-host cleanup and
complete memory export, with zero cleanup errors. Neither combined selection
reaches its rooted phase; no final exact drain is reached. These new markers do
not retroactively prove earlier cleanup success, and cleanup fault/error branches
are not injected by this cohort. The new original inventory retains **5,964
files / 61,194,407 bytes**, with verified closed copies and **four memory exports /
3,949 objects / 16,808,511 bytes**. Full-prefix recovery stays open.

The two diagnostic cohorts' original inventories total **4,845 files / 55,036,594
bytes**, with verified closed copies and **four synthetic-store snapshots / 3,185 objects / 13,358,111
bytes**. Copies are nontransactional diagnostics; older unexported backends stay
incomplete evidence. A read-only original RustFS probe at
**2026-11-20T15:52:33.990177+00:00** still reports **zero available bytes**, below
the unchanged **6 GiB reserve**, with original identity/configuration/start and
zero restarts. No matrix case is admitted; this is not a fresh three-node
resource witness.

### Queue critical path: diagnostic follow-up

The qualified production inputs remain `9748e402`; temporary instrumentation
`66277653` was removed after both fresh native C4 cohorts closed normally and
their originals, closed copies, source, executable and logs were independently
verified. The fetched main remains `2f083f14`. This adds diagnosis, not a
production change or a Queue repair. See the
source-bound diagnostic record.

Rooted and materialized C4 both fail, retaining **six ambiguous deadline producer
errors**. All **1,541 trace events** parse with timestamps. Queue identity is
derived using the production namespace/HMAC rules before matching command IDs;
an ID alone is insufficient across cells. Five returned errors have one observed
local preparation candidate. Those candidates enter preparation with only
**0.46–2.41 seconds** left in the original three-second stateless RPC window: four complete
successful local settlement after their deadline, and one expires during writer
admission. The sixth identity has multiple preparation scopes and lacks a traced
normalized-input digest, so no unique request attribution is claimed for it.
These observations do not retroactively establish a successful public response,
authorize replay or replace the original exact drain oracle, which is not reached.

Three native callback checkpoint proof scopes remain owned until shutdown drops
them. Their scope lifetimes include shutdown and are not successful proof
latencies. Temporary synchronous tracing can perturb scheduling; overlapping
phase intervals are not added or reported as request latency or throughput.
The cause of these missing terminal proofs remains unproven.

That checkpoint called for tracing routing, ownership and authority before
local preparation with the exact input digest, and investigating retained callback
checkpoint producers. The later ingress record above narrows that diagnosis.
Any production change needs a deterministic matched
regression plus the original Queue C2/C4 drain and SDK writer-release/fencing
controls within unchanged budgets. Recovery full-prefix, remaining primitive
coverage, OPT-01–08 and SDK/release acceptance stay open as listed below.

This cohort retains **2,514 files / 27,175,837 bytes**, including **two concrete
synthetic-store snapshots / 1,670 objects / 6,939,415 bytes**. Copies are
nontransactional diagnostics. Earlier unexported synthetic backends remain an
irreparable historical evidence gap. Raw identities, keys, logs and paths stay
private; failed histories are not reset or manually reissued.

An additional privacy scan found **351 raw command-ID occurrences in ten older
diagnostic JSON files**. The current tree replaces those Debug strings with
SHA-256 fingerprints, preserving every other JSON value and all surrounding
bytes. The original **1,439,465 bytes** are privately retained and hash-verified;
the diagnostic record includes the original/redacted file-hash mapping.
Fingerprints alone do not identify a cell/request. Prior Git history is unchanged.

A fresh read-only original RustFS probe at
**2026-11-20T13:45:15.206685+00:00** reports **zero available bytes**, below the
unchanged **6 GiB reserve**. Identity, configuration and start still match the
original with zero restarts; **no benchmark is admitted**. This is a RustFS-only
probe, not a fresh resource witness for all three nodes. The original three-node
1-vCPU/2-GiB SQL/KV/Queue durability/concurrency comparison remains open.

### Checkpoint submission: current qualification and gaps

Source `beb4add4` includes main `2f083f14`; compiler inputs are
`9748e4020fa3b595ed4d034d143d9c0e3955de8e58a37b8ce7b88a5a6d84d9b9`.
A staged RPC checkpoint now polls its existing owned submission future once
before unrelated queued SQL occupies the worker. Pending work keeps its wake;
a ready proof or error remains owned until normal checkpoint and reply-bridge
processing. Logical writer release, live authority, exact applied receipts,
required S3 coverage, deadlines and resource banks remain unchanged.

The byte-identical host-native RF3 regression changes from **no complete peer
checkpoint progress in four configurations** to **both peer views reaching the
exact checkpoint while source SQL remains held**, in both image modes with
Quorum/S3. Leader apply and the caller remain gated. Final value, original receipt
replay, single callback execution and joined cleanup pass. This fixture has one
shared group and one SQL worker per node; it qualifies the ordering mechanism.
Its elapsed time is not workload throughput or a request latency percentile.

The final source passes both ready/pending future lifecycle tests, all four
original SDK source-proof/fencing configurations and all four original checkpoint
reply controls: exactly **two handler polls for each of three commands**. Full
library: **1,268 passed / 0 failed / 2 existing ignores**.
Full execution: **234 passed / 0 failed / 2 existing ignores**.
Strict all-feature/all-target Clippy and repository checks pass.
The original sixteen application selections report **12 passed / 4 failed**;
see the source-bound record
for every stage and artifact/profile hash.

All four Queue load selections still fail. Rooted and materialized C4 fail
producer calls; combined C2 completes the materialized **528-message exact drain**
then fails rooted producers, and combined C4 fails its first materialized phase.
**8 returned producer errors** and their original histories/media remain
retained. These failures do not establish data loss; their final drain oracle is
not reached. Earlier same-source C4 passes and failures remain intermittent
observations. This scheduling change does not establish a causal Queue repair.

The earlier uncontended-writer retention experiment passed its new eight-case
control but failed the original SDK writer-release-before-fencing assertion.
That experiment was removed; the original assertion and deadline stay intact.
Its old failing SDK fixture did not export all synthetic backend/error evidence;
later custody fixes do not repair that historical gap. New SDK/checkpoint controls
retain concrete synthetic memory, passing/failing disks and returned command
errors after joined cleanup. Copies are nontransactional diagnostics.
The failed five-second observation run and compile preflights remain recorded
separately from the valid matched regression.

Remaining priorities:

1. **P0 — Repeatable delivery:** original-bound Queue C2/C4 producer/source-RPC/
   consumer progress and exact acknowledged-message drain; complete historical,
   large and streaming KV and remaining primitive/backend-fault coverage.
2. **P0 — Recovery and SDK:** reverify the earlier recovered-voter shutdown cut
   (committed **7**, applied/archived **6**, no unresolved local producers), then
   close full-prefix/fenced recovery and current-source SDK/routing/read-refusal,
   feature/daemon and release gates. No complete recovery repair follows here.
3. **P1 — OPT-01–08:** measured persistence/ReadIndex/archive batching and hot/cold
   fairness, physical shared-file release/once-only credits and repeated
   reserve-aware interrupted placement/lifecycle.
4. **P2 — Original comparison:** original RustFS has **10,493,952 available bytes**
   below its unchanged **6 GiB reserve** at **2026-11-20T13:18:33.719286+00:00**;
   original container identity/configuration/start match with zero restarts.
   No benchmark is admitted. Preserve the original three 1-vCPU/2-GiB nodes,
   RF3/three groups/shared RustFS; SQL/KV/Queue x Quorum/Quorum+S3/Fleet/Bucket x
   C1/4/16, 256-byte values, Queue batch eight, reversed repetitions and original
   60-second windows. This probe is not a fresh three-node runtime-resource witness.

Full OPT/remaining CG/SDK/release/performance acceptance remains open. No
constrained throughput, latency-percentile or Fleet/Bucket conclusion is claimed.
Earlier sections retain their original source boundaries.

### Append replication overlap: current qualification and gaps

Source `691c6d87` includes main `2f083f14`; compiler inputs are
`ba33983912c795014fc00a3b2caa9f7e3e37ce7ed2d131465a329e7f7b34d24b`. Eligible stable-term Ready append messages
now enter the bounded outbox after exact log-worker admission while local sync
runs. The node schedules only eligible histories in that batch; waiting groups
sleep when append output drains. Follower replies, read/applied publication,
SDK receipts and required S3 coverage retain their existing proofs. Worker,
admission, timeout and service-resource limits stay fixed.

The matched owner regression changes **zero to two early append messages** under
held leader sync. Both actual peer disks advance; follower replies and leader
publication remain gated, restart restores exact bytes, and cancellation retains
the physical owner. A wrapper control exposed missing group scheduling before
the corrected source passed **eight focused controls**. The complete current
execution target now passes **234 / 0 / 2 existing ignores**, following ten
isolated fixture controls. Fixtures explicitly join follower Applied publication
and previously admitted peer frames within their original five-/twenty-second
bounds; every original assertion remains. Prior failures stay retained.

Full library **1,266 passed / 2 existing ignores** and selected primitives
**12 passed / 4 failed** are reused only after Cargo emits byte-identical
executables and profiles for current inputs. Ordinary/Axum SQL and KV, ordinary
Queue callbacks, operator reads, long-callback/cross-deployment controls and
discovery pass. All four remaining primitive failures are original Queue load
cases. Strict Clippy passes without an override; repository checks pass.

New mutation qualification retains node disks and concrete synthetic-memory
objects, including successful fault controls: **31 snapshots / 22,463 objects /
336,442,442 bytes**. Keys stay in private manifests; exported copies are
nontransactional diagnostics. Earlier failed SQL/Queue synthetic backends were
not exported; those histories remain incomplete backend evidence despite retained
node media, raw identities/logs and verified copies. No history is reset or
mutation reissued. See the source-bound record.

Remaining priorities:

1. **P0 — Queue progress:** original-bound C2/C4 producer/source-RPC/consumer
   progress and exact acknowledged-message drain.
2. **P0 — Recovery and release:** the earlier materialized recovery-close case
   reports committed index **7**, applied/archived **6**, with no unresolved local
   producers. Complete shutdown-prefix/fenced-recovery, current-source SDK,
   routing/read refusal, historical/large/streaming KV, backend-fault and release
   acceptance remain open.
3. **P1 — Complete OPT-01–08:** measured fairness and persistence/ReadIndex/archive
   batching, physical shared-file release/once-only credit, reserve-aware
   placement and interrupted lifecycle.
4. **P2 — Original constrained comparison:** read-only inspection at
   **2026-11-20T08:44:08.665066+00:00** finds **13,475,840 available
   bytes**, below the unchanged **6,442,450,944-byte reserve**.
   Original identity/resources/start match, with zero restarts; no case is
   admitted. Preserve three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared
   RustFS; SQL/KV/Queue x Quorum/Quorum+S3/Fleet/Bucket x C1/4/16, 256-byte
   values, Queue batch eight, reversed repetitions and original 60-second windows.

These controls qualify a bounded overlap mechanism. No constrained throughput,
latency percentile, Fleet/Bucket conclusion or complete OPT/CG task is accepted.
Earlier sections below retain their original source boundaries.


### Owned callback discovery and current priorities

Compiler inputs `de694bac16735691391100bf6888797fa3adb1b1ed671cbc4f762fde78a38a4f`
include main `2f083f14`. When the exact cell history already has a physically
retained native callback and no independent work is possible, discovery now
advances its cursor without another fresh barrier and SQL scan. Compiled alarms,
KV service/binding capabilities and call/outbox/inbox work retain discovery.
The original post-scan callback check and all execution, generation, quorum,
receipt and required S3 proofs remain. Worker, admission and deadline limits stay
fixed. Three bounded prepare/submit/settle histograms expose the retained command
phases; they do not represent business request latency.

The byte-identical matched RF3/three-group regression observes **three extra SQL
queries before / zero after** across three completed discovery turns in each of
four configurations per source. The same delivery identity remains held; releasing
the candidate drains exactly two messages in both image modes. This qualifies
less background work, not constrained throughput, percentiles or a Fleet/Bucket
comparison. A final-source replay also passes both modes.

Two subsequent targeted diagnostic captures reproduce all **eight original C4
invocations** and retain all **26 returned producer failures**. The first probe
lacks full cell-history correlation and cannot attribute individual failures.
The refined probe uses canonical Queue identities: completed writer waits reach
**1,608 ms**, entry rebase maxima are **13–98 ms**, and SQL-worker queue medians
are zero with nonzero tails. Submission/settlement add cost. These are instrumented
phase observations including setup/teardown, not business latency or a causal
repair. Tracing-interleaved failure records are reconstructed in a separate copy;
original bytes and verified node/copy manifests remain retained. All probes are
removed and exact published compiler inputs restored. Next investigate measured
same-cell writer/persistence/finalization work while preserving every proof;
missed-wake and retry-policy changes lack supporting evidence. See the
diagnostic record.

Full library **1,266 passed / 2 existing ignores**, including three independent
effect/alarm/KV-capability guards. The fifteen selected native cases are **9
passed / 6 failed**; the original eleven-case subset remains **6 / 5**. Ordinary
and Axum SQL/KV, both Queue operator controls, a 65-second callback with another
Queue progressing and cross-deployment fault controls pass. Original ordinary
Queue, C2 and independent C4 deadline/drain failures remain. Combined C4 fails
during setup before measured load. A cold KV read/write availability failure
prevents one S3-boundary case from reaching archive denial; a separate final-source
cohort passes that boundary in both modes. It does not repair the failed history
or establish sustained availability.

The failed native cohort's **2,135 files / 46,838,985 bytes** match its closed copy.
Its load aggregation returned only the first producer error, so complete returned
identities are unavailable there. A test-only correction preserves all eight
returned producer failures and their command identities in a separate independent
C4 cohort; both original load cases still fail. It also preserves two expected
S3 fault responses. That cohort's **876 files / 22,452,951 bytes** match a verified
closed copy. Consumer observations are not durable acknowledgement proofs. Raw
identities and logs stay private; no failed history is reset or mutation reissued.
The rebuilt library artifact/profile are byte-identical to the full-library pass.
Strict/repository checks and **344 performance guards / 1 existing skip** pass;
Clippy retains its explicit dead-code warning override. See the
source-bound qualification.

Remaining priorities:

1. **P0 — Queue and availability:** original-bound producer/source-RPC/consumer
   progress and exact acknowledged-message drain; cold and historical
   streaming/large-value KV availability. Preserve identities, data and proofs.
2. **P0 — Release acceptance:** current-source routing/read-refusal, SDK, Python
   lifecycle and full primitive/backend qualification. Prior-source hosted passes
   do not qualify the changed executable.
3. **P1 — Full OPT-01–08 and remaining CG gates:** measured fair persistence,
   ReadIndex/archive batching; physical shared-file release and once-only credit;
   reserve-aware placement; interrupted lifecycle and fenced recovery.
4. **P2 — Original constrained comparison:** read-only inspection at
   **2026-11-20T03:11:34Z** reports **zero available bytes** against the unchanged
   **6,442,450,944-byte reserve**, with original identity/resources/start and zero
   restarts. No case is admitted. Preserve three 1-vCPU/2-GiB Colima nodes,
   RF3/three groups/shared RustFS; SQL/KV/Queue ×
   Quorum/Quorum+S3/Fleet/Bucket × C1/4/16, 256-byte values, Queue batch eight,
   reversed repetitions and original 60-second windows.

No complete OPT or CG task is accepted. Full optimization, SDK/release and
original performance work remain open.


### Inline KV reads and current priorities

Compiler inputs `cb932af1de5d86d5ea01dc7d06507ad47a3d7ce8c3aa5bd608b4bbdb56c9a57b`
include main `2f083f14`. Eligible warm stateless KV gets now use a fresh
quorum-pinned image before selecting a mutation sequence. Original namespace
authorization, history selection, FIFO, request admission and deadline remain.
The shared codecs preserve values and revisions; scalar length checks bound
returned buffers. Cold initialization, external values and larger replies retain
the original command path. Only an explicit eligibility result permits that
fallback; other read errors retain their cause. Stateful source calls stay on
their original journaled protocol. Stale private reads refuse before decoding,
and S3-minimum reads still require selected-image archive coverage.

The byte-identical matched RF3 regression records **three read commits before /
zero after** in both image modes. A real write advances the group and fresh
quorum loss refuses. Six successful native HTTP samples are **490–1,341 ms before /
128–133 ms after**. These are sequential reads of six-byte text in a host-native,
two-group correctness fixture, not the original constrained 256-byte workload,
throughput, percentiles or a Fleet/Bucket comparison.

Full library **1,263 passed / 2 existing ignores** and all nine strict/repository
checks pass. The original eleven-case application cohort is **6 passed / 5
failed**: ordinary/Axum SQL and KV and both Queue operator-read controls pass;
all five Queue callback/progress/drain cases fail. The two new read cases pass,
including actual archive denial, independently successful Quorum metadata reads,
S3 business-read refusal, stale refusal, restored coverage, restart and quorum loss.
The first successful S3 fixture cleaned up its disks and discarded its faulted
write response body; it supplies no terminal-media/returned-ID evidence. The
test-only retention correction passes a separate independent cohort, preserving
two returned ambiguous IDs and six original node directories. Its **200 files /
2,735,810 bytes** and the failed original cohort's **1,890 files / 44,266,324 bytes**
match verified closed copies. Freshly rebuilt byte-identical library executable
and profile preserve the full-library result. See the
source-bound qualification.

Remaining priorities:

1. **P0 — Queue and availability:** close original callback, C2/C4 producer/source
   RPC/consumer deadlines and exact acknowledged-message drain. Earlier KV
   streaming/large-value failures remain separate historical failures; this
   passing cohort does not establish their complete causal repair or sustained
   availability. Preserve failed identities, data, original bounds and proofs.
2. **P0 — Release acceptance:** complete current-source routing/read-refusal,
   Python lifecycle and SDK qualification. Hosted passes on prior sources do not
   qualify a changed executable.
3. **P1 — Full OPT-01–08 and remaining CG gates:** measured persistence,
   ReadIndex/archive batching and fairness; physical shared-file reclamation and
   once-only credit; reserve-aware movement; interrupted lifecycle/recovery;
   complete primitive/backend/crash/historical qualification.
4. **P2 — Original constrained comparison:** the 2026-11-19T22:21:42Z read-only
   RustFS observation has **663,552 available bytes** against the unchanged
   **6,442,450,944-byte reserve**. Identity/resources/start remain unchanged,
   restarts are zero and no case is admitted. Preserve three 1-vCPU/2-GiB Colima
   nodes, RF3/three groups/shared RustFS, SQL/KV/Queue ×
   Quorum/Quorum+S3/Fleet/Bucket × C1/4/16, 256-byte values, Queue batch eight and
   reversed repetitions. No resource/storage change is proposed.

No complete OPT or CG task is accepted by this checkpoint. The full optimization,
SDK/release and original performance objective remains open.

### Retained log cache reuse and current priorities

Compiler inputs `2efed90e495f23cc30777713bb38b46142ad414ee29b0d4e9dac89a9215eaebb`
include main `2f083f14`. Metadata-only persistence candidates now share immutable
retained entries. Appends build a private map; checkpoints copy only the surviving
suffix; snapshot installation replaces the candidate map. Existing validation,
sync/publication ordering, receipts, capacity charges, workers and deadlines stay
unchanged. This removes entry-map cloning; no end-to-end throughput or latency
gain is qualified.

The byte-identical real-engine regression observes entry reuse **false before /
true after**, with held-sync invisibility, no premature completion, committed
rewrite refusal, unchanged retained views, exact reopen and joined shutdown on
both sides. Its RF3-configured local group does not establish RF3 serving. Full
library **1,263 / 2 existing ignores**, storage **22 / 2 owned process-fixture
ignores**, and snapshot **5 / 2 owned process-fixture ignores** pass.

The original eleven-case application cohort remains **5 passed / 6 failed**.
Ordinary/Axum SQL, ordinary KV and both Queue operator-read controls pass. Axum KV
refuses extent-backed `PublicLarge` with `Runtime(Overloaded)` after 190 ms,
before its large put completes. The identical baseline case and a later actual
execution of the same candidate binary pass both image modes. This establishes
an intermittent failure with unproven cause; the failed cohort remains failed,
and full KV acceptance stays open. All five ordinary/C2/combined and independent
C4 Queue progress/drain cases fail. Available failed-run disks and verified
closed copies remain retained, including **9,353 files / 811,572,380 bytes**.
Successful isolated fixture cleanup leaves no terminal disks and grants no
retained recovery-media evidence.

Three bounded diagnostic cohorts retain those failures and expose additional
phase boundaries. A refusal probe fails earlier in extent-mode source streaming,
before its large-request target; its zero events do not refute the original
`Overloaded`. A test-only stream probe passes once, then fails extent-backed
`PublicLarge` with an ambiguous deadline after every stream step completes.
The combined resource/refusal probe also passes once. Its second trial completes
both modes' large put/get, then a post-restart public `Read` returns 503 with an
ambiguous deadline at 3,134 ms and no Budget/frame refusal. This small KV read
currently enters the command/binding path. A pure pinned-reader path remains a
candidate requiring cold initialization, TTL/revision and external-value proof
preservation. These are distinct unresolved failures, not causal repairs.
All probes are removed and exact published compiler inputs restored. The three
cohorts retain verified failed-run media and closed copies. See the
diagnostic record; instrumented timing
and overlapping background counters do not qualify an end-to-end speedup.

Eight initial strict/repository checks pass. The ninth, the 44-case plan suite,
first rejects the changed manifest anchor. After review, only that anchor and
the source revision are repinned to `d13558f85`; all other anchors and task
statuses remain unchanged. All 44 tests and direct reviewed-source preflight
then pass. The broader plan CLI separately cannot load historical CG-01 evidence
in this checkout; its delivery gate remains open.

The preceding native Queue diagnostic still finds separate log-append,
commit-index and applied-state publication writes, with node-call means roughly
31–50 ms. Across complete diagnostic runs including setup, 733 of 735 appends
contain one entry. These temporary-probe observations identify persistence work;
they are not current-source performance qualification or a matched Fleet/Bucket
comparison. Required durability boundaries remain unchanged.

Remaining priorities:

1. **P0 — Queue progress and large KV admission:** close original Queue deadlines
   and acknowledged-message drain, and diagnose intermittent Axum KV large-value
   refusal. Preserve original identities, failed data, limits and proof checks.
2. **P0 — Release acceptance:** obtain current-source routing/read-refusal,
   Python lifecycle and SDK acceptance; retain historical source boundaries.
3. **P1 — OPT-01–08 and remaining CG gates:** complete measured persistence,
   ReadIndex/archive batching and fairness, physical reclamation/once-only credit,
   reserve-aware movement, interrupted transitions/recovery and complete
   primitive/backend/crash/historical evidence.
4. **P2 — Original constrained performance:** the 2026-11-19T16:48:49Z read-only
   RustFS observation still has zero available bytes against the unchanged 6 GiB
   reserve, stable identity/resources and zero restarts. No case is admitted.
   Preserve the original three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared
   RustFS; SQL/KV/Queue × Quorum/Quorum+S3/Fleet/Bucket × C1/4/16, 256-byte values,
   Queue batch eight and reversed repetitions.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open. See
the source-bound record for
source/profile/executable/log bindings, original failures, isolated controls,
retained media and environment limits.

### Retired-voter drain verification and current priorities

Compiler inputs `029d9a82fe1a7da0c985758ccae9593166c885bb466c64834d316e1c69f15a58` include main `2f083f14`.
All production inputs are byte-identical to published `1ba5fb538`. This follow-up
corrects a managed lifecycle test race and retains its node disks/error phases.

The exact S3 replacement case first passes in isolation, then the first serial
repetition fails after replacement reaches Complete and verifies source data.
Its drain scan requests status from candidate **0**, also the verified obsolete
voter **0**; that runtime detaches between lookup and status, returning
`Raft runtime: NotFound`. The fixture now excludes only this removed voter from
initial drain leader selection and maps the result to the original host index.
Unexpected `NotFound` from any eligible voter still fails. This is a test
selection correction, with no production retry or recovery-policy change.

The same lifecycle oracle passes both materialized and extents modes: exact S3
receipts, replay/fresh values, final membership, retained debt, lost terminal CAS
reconciliation, actual drain handoff/no unresolved producers, invocation counts
six/eight, joined retained restart, cold quorum-read and oldboot grant refusals.
The failed trial's **128 files / 1,766,043 bytes** and candidate's **364 files /
5,203,294 bytes** remain at their original paths and match verified closed copies.
The initial context-only control had no available terminal disks under its prior
TempDir cleanup; it is not represented as retained recovery media.

Full managed qualification now passes **38 / 0 failed / 15 explicit RustFS
ignores**. Library **1,262 / 2 existing ignores** and disaster **13 / 8 explicit
RustFS ignores** also pass; reuse requires freshly rebuilt byte-identical
executables and complete profiles. All nine strict/repository checks pass.
These host-native synthetic-store results do not qualify the original constrained
Colima/RustFS comparison or establish a throughput/latency gain.

Remaining priorities:

1. **P0 — Queue progress:** close original producer/source-RPC/consumer deadlines
   and acknowledged-message drain in both image modes. Preserve ambiguous IDs,
   exact receipts, workers, bounds and required proofs. The preceding application
   selection remains six passed/five failed; SQL/KV/RPC controls pass, while
   ordinary Queue, C2 and C4 progress/drain cases remain open.
2. **P0 — Current-source release acceptance:** obtain hosted routing/read-refusal,
   Python lifecycle and SDK acceptance. The earlier hosted S3 `NotFound` has no
   exact phase context, so the local diagnosis does not independently prove its
   cause. Preserve earlier failures and their source boundaries.
3. **P1 — OPT-01–07:** finish routing/read partition coverage, measured persistence/
   ReadIndex/archive batching and fairness, physical shared-file reclamation with
   once-only capacity credit, reserve-aware movement and interrupted lifecycle/
   transition/recovery qualification.
4. **P1 — OPT-08:** complete primitive/SDK/Axum/daemon/feature/backend-fault/crash
   and historical CG evidence.
5. **P2 — Original constrained performance:** keep the original three 1-vCPU/2-GiB
   Colima nodes, RF3/three groups/shared RustFS; SQL/KV/Queue across Quorum,
   Quorum+S3/Fleet/Bucket C1/4/16, 256-byte values, Queue batch eight and reversed
   repetitions. The 2026-11-19T14:27:07Z read-only original RustFS observation
   still finds zero available bytes against the unchanged 6 GiB reserve, stable
   identity/resources and zero restarts. No performance case is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open.
See source-bound qualification
for exact source/executable/profile/log hashes, intermittent controls, retained
media, reuse and environment limits.

### Import completion and current priorities

Compiler inputs `66f1af10e0ddacf27440ecd3f7e6bb0438ea6649aefd659f97fc34f1debff7bd` include main `2f083f14`.
The engine now drops its consumed transfer seed before waking the import caller,
after durable import and any fatal closed flag. Other prepared-root owners keep
their admission and pins. The causal real-engine regression observes **1 recovery
slot / 84,867,072 reserved bytes at completion before the fix**, then **0 / 0**
with byte-identical test inputs. Held-sync invisibility and no-premature-wake,
exact durable inactive mapping, joined close and retained reopen pass on both
sides. This verifies ownership completion; reserved bytes are not physical RSS
or reclaimed disk, and the empty unmanaged transfer is not RF3 serving proof.

The full library passes **1,262 / 2 existing ignores**. Its first attempt failed
the partial-park fixture's initial direct enter with typed `NotReady`. The fixture
now uses production's existing bounded retry for the same certificate and
original deadline; all persisted-park, fresh-election and late-replay refusal
assertions remain. Full managed qualification remains **37 passed / 1 failed /
15 explicit RustFS ignores**: S3 group replacement/joined restart returns
`Raft runtime: NotFound`. Full disaster qualification passes **13 / 8 explicit
RustFS ignores**. Native reuse requires rebuilt byte-identical executables and
profiles; both source digests and the test-only delta are recorded. All nine
strict/repository checks pass. Original failed fixtures and verified closed
copies remain retained.

The original Queue C2 and both C4 diagnostic runs still fail their progress
oracles. Across 425 phase events, the suspected pure-query handoff never occurs;
that hypothesis was not selected for a fix. SQL/KV/RPC controls from the preceding
application selection pass, but its five Queue failures remain open. No end-to-end
throughput or latency gain is qualified.

Remaining priorities:

1. **P0 — Queue progress:** close original producer/source-RPC/consumer deadlines
   and acknowledged-message drain in both image modes; retain ambiguous IDs,
   exact receipts, current workers, bounds and durability requirements.
2. **P0 — Recovery and lifecycle:** diagnose S3 replacement/joined-restart
   `NotFound`, retained routing/recovery and Python shutdown failures. Published
   parent SDK 3.14 independently has the same S3 failure; its library and disaster
   targets pass. The separate standalone disaster run was cancelled before a
   test footer, with cause unproven. Parent RPC 3.14 passes; other release modes
   still require current-source acceptance.
3. **P1 — OPT-01–07:** finish routing/read refusal and partition coverage,
   measured persistence/ReadIndex/archive batching and fairness, physical shared
   file reclamation with once-only credit, repeated reserve-aware movement and
   interrupted park/wake/transition/recovery qualification.
4. **P1 — OPT-08 and release:** complete primitive/SDK/Axum/daemon/feature/
   backend-fault/crash and historical CG evidence.
5. **P2 — Original performance matrix:** retain the three original 1-vCPU/2-GiB
   Colima nodes, RF3/three groups/shared RustFS; SQL/KV/Queue across Quorum,
   Quorum+S3, Fleet and Bucket, C1/4/16, 256-byte values, Queue batch eight and
   reversed repetitions. At 2026-11-19T13:14:03Z the original RustFS has zero
   available bytes against the unchanged 6 GiB reserve, stable identity/resources
   and zero restarts. No performance case is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open.
See source-bound qualification
for exact source/executable/profile/log bindings, failed attempts, reuse and
native/environment limits.

### Checkpoint reply progress and current priorities

Compiler inputs 98eba6ce include main 2f083f14. After the existing checkpoint
proof, the SQL owner drives one bounded owned-work pass to relay its reply before
polling the dependent handler. Fresh shutdown, original deadline, closed reply,
activation, term and full executor authority checks surround that continuation.
Contended query and safe-root FIFO writer handoffs remain; reply wakes survive
park. Workers, banks, identities, deadlines, fsync and durability requirements
remain unchanged.

The byte-identical public RF3 test runs three commands in each materialized/
extents × Quorum/S3 configuration. Baseline commands need **3 handler polls**;
candidate commands need **2**. Exact receipts, fresh linearizable values/version/
witness, replay without re-invocation and joined shutdown pass on both sides.
This establishes one avoided poll per checkpoint, not a throughput or latency
gain.

The broader candidate has identical production inputs; final source adds only
the independently compiled regression and root declarations. The full library
passes **1,261 cases / 2 existing ignores** using the exact same executable under
a fresh short temporary root. Initial 13 mock Docker socket failures exceeded
SUN_LEN under a longer root; both attempts remain recorded. Native Queue passes.
Shared application selection remains **6 passed / 5 failed**: signed-read
controls, proof sharing, SQL, KV and RPC continuation pass; ordinary Queue, C2,
both independent C4 images and combined C4 fail their original progress oracles.
Available failed fixtures match verified closed copies. All nine strict/
repository checks pass.

Current priorities:

1. **P0 — Queue progress:** close original producer/source-RPC/consumer deadlines
   and acknowledged-message drain in both image modes. Diagnostic enqueue SQL
   body cost is small relative to writer admission, checkpoint waits, image work
   and durable settlement. Preserve ambiguous command IDs and exact receipts.
2. **P0 — Recovery and lifecycle:** close retained local recovery failures.
   At preceding head 04f728d4, hosted SDK native 3.14 fails both group replacement/
   joined-restart cases with Raft runtime NotFound. Native 3.14t fails the rooted
   replica-routing refusal assertion; separate disaster diagnostics
   fails transfer recovery-slot release. Full 3.14t RPC shutdown also fails.
   These preceding-source observations do not qualify the new source.
3. **P1 — Complete OPT-01–07:** finish routing/read refusal and partition coverage,
   measured persistence/ReadIndex/archive batching and fairness, physical shared
   file release with once-only capacity credit, repeated reserve-aware movement,
   and optimized wake/interrupted-transition/recovery qualification.
4. **P1 — Combined release and performance:** finish current-source primitive,
   SDK/Axum/daemon/feature/backend-fault/crash and historical CG evidence (OPT-08),
   then the original constrained SQL/KV/Queue comparison.

The latest retained original RustFS observation has zero available bytes
against the unchanged 6 GiB reserve. No original Colima performance case is
admitted. Keep three original 1-vCPU/2-GiB nodes, RF3/three groups/shared RustFS,
Quorum/Quorum+S3/Fleet/Bucket, C1/4/16, 256-byte values, Queue batch eight and
reversed repetitions. Complete OPT-01–08 and remaining CG/SDK/release/performance
gates remain open. See source-bound qualification
for exact inputs, separate executable/profile/log hashes, setup exclusions and
retention/reuse limits.

### Signed Queue operator reads and current priorities

Compiler inputs `7595d14b9c18bb6efd222f0dd19e192a0424958870d966fd40be58079fe081a4` include `origin/main` `2f083f14`. When local history and initialized Queue schema are available, signed `info` and `peek` use the existing fresh linearizable pinned reader after the original operator authorization. The target namespace's S3 minimum still requires current archive proof of the selected image. Stale consistency refuses. The broker and query share metrics/peek SQL, response shaping, configured retention, limits and console byte budget. Missing local history or Queue schema/statistics uses the existing authorized initialization command with the original admission, arguments and deadline; other read errors do not authorize a mutation retry. Pause, resume, purge and redrive keep ordinary mutation execution.

The same real RF3 regression fails before the change: two signed reads advance an idle Queue's committed index **4 → 6**, under stable leadership. The byte-identical oracle passes after the change with **4 → 4 in both image modes**. Pause/resume remain mutations and subsequent info observes pause without another append. Both modes also pass signed authentication/body-tamper/name-scope/limit refusals, exact stale-consistency refusal, S3 info/peek refusal behind a committed pause with denied archive writes, recovery after archive coverage returns, and refusal after losing two Raft hosts. Quorum read service remains available during the archive fault. These are avoided-work and correctness results, not throughput or latency percentiles.

**Combined application qualification remains FAILED.** The focused shared application selection has **6 passed / 5 failed**. The full library has **1,261 passed / 2 existing ignores**; the full native Queue target passes. The original batch claim/lease proof-sharing case passes. Controls: SQL PASSED, KV PASSED, RPC continuation PASSED. Current failed cases are `ordinary_queue_callbacks_use_shared_quorum`, `queue_load_preserves_acknowledged_messages`, `materialized_queue_four_in_flight_preserves_acknowledged_messages`, `rooted_queue_four_in_flight_preserves_acknowledged_messages`, `queue_four_in_flight_preserves_acknowledged_messages`. Queue producer failures reach their first target attempt with only 844–2,462 ms remaining; the ordinary Queue failure is an ambiguous source-command deadline in rooted mode. These observations locate progress gaps without establishing a causal bottleneck or speedup. Failed identities and available fixtures remain retained and verified against closed copies.

All nine strict/repository checks pass at this compiler-input digest. The prior `7562115bb` full managed/disaster selection retains three failed recovery gates: extents S3 seed failover settlement, extents fresh outbox-lane settlement, and materialized pending recovery forming a fresh quorum. Those full targets were not rerun here. Current-source hosted SDK/release acceptance remains required. See the source-bound qualification for exact profiles, executable/log hashes, excluded build attempts, source binding and scope.

Remaining priorities, in order:

1. **P0 — Queue progress:** close the original producer, source-RPC and delivery/drain deadlines at their existing workers, admission banks and timeouts. Measure ingress allocation/queueing and actual image/persistence phases before selecting another optimization. Preserve ambiguous command IDs and required durability proofs.
2. **P0 — Recovery and hosted acceptance:** close the three retained recovery failures and obtain current-source disaster/full SDK acceptance. The preceding hosted Python 3.14 lifecycle shutdown failure also remains a release gap.
3. **P1 — Sustainable shared execution and capacity:** finish routing/read refusal and partition coverage, measured batching/fairness, physical shared-file release with once-only credit, repeated reserve-aware movement, and interrupted park/wake/transition/recovery (OPT-01–07).
4. **P1 — Combined release:** complete primitive/SDK/Axum/daemon/feature/backend-fault/crash and historical CG evidence (OPT-08).
5. **P2 — Original constrained comparison:** rerun SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 on the original three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared RustFS, 256-byte values, Queue batch eight and reversed repetitions. The read-only observation at 2026-11-19T06:25:10.843969+00:00 finds **15,716,352 bytes** available against the unchanged **6-GiB reserve**; original identity/resources remain stable with zero restarts. No run is admitted and no end-to-end gain is qualified.

Full OPT-01–08 and remaining CG/SDK/release/performance gates remain open.

### Retained peer endpoints and current priorities

Compiler inputs `4164eb74b62e36240adddfa0b688c387ee10c83cd8c9a2a4dfc57094cb4c211d` change test fixtures only. A stopped managed node can retain an authenticated advertised view while the OS reassigns its loopback port. The shared fixture now remembers the first logical owner of each Raft port and rejects foreign reuse before publication. It retains metadata, with no socket custody; same-node reuse remains allowed. Selection checks at most 64 candidates. Production uniqueness guards, durability proofs, boot/command retries, resources and deadlines remain unchanged.

The same real-node regression fails with the duplicate-address refusal under the original selection policy, then passes in both image modes with only the selection helper changed. It checks retained node/disk/engine lineage, newer process/epoch, joined shutdown and release of the stopped port. Zero-group standbys isolate this pre-attachment guard; they do not replace RF3 recovery. Five original provisioning controls pass, including an exact typed duplicate-address refusal before filesystem work.

**Combined owning qualification remains FAILED.** The full managed target has **37 passed / 1 failed / 15 explicit RustFS ignores**. The full disaster target has **11 passed / 2 failed / 8 explicit RustFS ignores**. Complete-closure recovery passes all four parent/child and materialized/extents combinations, with no duplicate-address refusal in the disaster log. Remaining failures are extents S3 seed failover settlement (`[Seeding, Joint]`), extents fresh outbox-lane settlement, and materialized pending recovery forming a fresh quorum. Available failed disks and snapshots are retained and verified against closed copies. These observations do not establish a causal regression comparison.

All nine strict/repository checks pass. The rebuilt library executable and profile are byte-identical to the prior 1,261-pass / 2-ignore run; that result is reused explicitly, rather than reported as a new full-library run. On published predecessor `f82283053`, hosted complete-closure recovery still fails the duplicate-address guard (12 passed / 1 failed / 8 ignored). Corrected-source hosted SDK acceptance remains required. See the source-bound qualification.

Remaining priorities, in order:

1. **P0 — Application and recovery progress:** close the six original-deadline Queue failures recorded on production checkpoint `f82283053`, plus the three owning failures above. Profile actual image preparation, persistence and ingress phases without increasing workers, banks or deadlines. Preserve SQL/KV/RPC regression coverage and exact command identities.
2. **P0 — Hosted acceptance:** run the corrected fixture through the original hosted disaster and complete SDK sequence. A sufficient reproduced port-collision mechanism and a local complete-closure pass do not close the whole release gate.
3. **P1 — Bounded optimization and capacity:** finish routing/read refusal and partition coverage, measured batching/fairness, physical shared-file reclamation with once-only credit, repeated reserve-aware movement, and interrupted park/wake/recovery qualification (OPT-01–07).
4. **P1 — Combined release:** complete primitive/SDK/Axum/daemon/feature/backend-fault/crash and historical CG evidence (OPT-08).
5. **P2 — Original matched comparison:** retain all SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 cases on three Colima 1-vCPU/2-GiB nodes, RF3/three groups/shared original RustFS, 256-byte payloads, Queue batch eight and reversed repetitions. The read-only observation at 2026-11-19T04:28:57.081176+00:00 still finds zero available bytes below the unchanged 6-GiB reserve, stable identity/resources and zero restarts. No constrained comparison is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open. No end-to-end throughput or latency gain is qualified by this fixture correction.

### Queue schema reopen work and current priorities

Compiler inputs `2a7231be5eee34670d16c063fe2ce68ffe030f3a4a1d2cdb3e9e5903b22413d4` skip the Queue stats bootstrap aggregate when its existing stats row is present. Raft opens the schema for each private command image; previously the aggregate scanned the entire backlog before `ON CONFLICT` discarded its result. Schema creation, trigger definitions and missing-row rebuilds remain. Workers, banks, deadlines, identities, APIs and durability proofs stay unchanged.

The byte-identical bundled-SQLite regression fails on the original SQL at 512 messages (8,831 VM callbacks versus 129 when empty), then passes with the schema-only change. Missing-stats rebuilding and subsequent delete/purge trigger metrics pass before and after. The exact linked SQLite 3.53.2 kernel uses 69,759 versus 122 VM steps at 4,096 messages, and 170,127 versus 122 at 10,000. These are initializer work counts, with no command throughput or latency-percentile claim.

Current-source validation passes the full library (**1,261 passed / 2 existing ignores**), full native broker (**1 passed**) and original SQL/KV/RPC continuation controls in both image modes. **Combined application qualification remains FAILED:** ordinary shared-Raft Queue callbacks, batch proof sharing, C2, independent materialized/rooted C4 and combined C4 all fail. The C2/C4 producer failures reach their original peak two/four concurrency; final acknowledged-message/completion oracles remain unpassed. First-target budgets are 2,625 ms for C2, 1,627 ms for materialized C4 and 1,168 ms for rooted C4. These observations do not establish a causal regression or repair. All nine strict/repository checks pass, including all-feature/all-target Clippy and native-only facade compilation. See the source-bound record for exact source/profile/executable/log hashes, the invalid initial selector setup and verified retained failures.

The hosted original serial disaster target passes **13 cases / 8 explicit RustFS ignores** on preceding diagnostic commit `6783ed04`, compiler inputs `e92708ce`. The broader SDK native 3.14 job on that same source fails `disaster_retry_and_old_token_refusal` and `transition_disaster_selects_complete_closure` (11 passed / 2 failed / 8 ignored). Both failure sections identify **duplicate peer address** as the provisioning refusal. The rejected condition is now known; the collision origin and repair remain unproven. These preceding-source results do not qualify this Queue candidate. Full SDK and earlier archive-cleanup acceptance remain open.

Priorities remain:

1. **P0 — Queue progress and endpoint admission:** diagnose and close all six original-deadline Queue failures, then establish and repair the duplicate-peer-address condition in the full hosted sequence while preserving the production refusal. Profile physical image preparation, persistence and ingress waits while retaining SQL/KV/RPC regression coverage. SQL apply already has its own progress bank; post-failure snapshots do not justify raising producer ceilings. Cumulative SQL-publication unsuccessful counts include pending reconciliation and are not command-conflict counts.
2. **P1 — Complete bounded optimization:** finish routing/read refusal and partition coverage, measured persistence batching and fairness, verified physical reclamation/once-only credit, reserve-aware movement and interrupted park/wake/recovery qualification (OPT-01–07).
3. **P1 — Combined release:** complete the primitive/SDK/Axum/daemon/feature/backend-fault/crash suite and historical CG evidence (OPT-08). Preserve earlier failures when independent controls pass.
4. **P2 — Original matched comparison:** retain SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16, three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared original RustFS, 256-byte payloads, Queue batch eight and reversed repetitions. The read-only observation at 2026-11-19T03:03:20.790083+00:00 finds zero available bytes below the unchanged 6-GiB reserve, stable identity/resources and zero restarts. No constrained comparison is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open.

### Cached committed RPC conflicts and current priorities

Implementation `6a6878dd` includes `origin/main` `2f083f14` and retains one final committed command ID and input digest in the existing bounded native activation. A different input at that ID can be refused before reopening private SQLite. The exact current image/route/leader/class fence, schema, registered admission and authority checks remain required. Matching inputs and other IDs keep ordinary receipt inspection; SQL ingress still needs its fresh quorum consumed-other proof before selecting another sequence. Intermediate checkpoints, output bytes, image custody and absence/non-commit authority are not cached. Workers, deadlines, stream identities, activation slots and banks stay unchanged.

The same byte-identical native RF3 regression fails before the change: the first conflict creates three materialized private preparations or one rooted workspace. The candidate rejects two conflicts with zero private preparations in both modes, preserves exact duplicate result/version and constructor/cache counts, physically inspects an older ID after a newer commit, and executes the following valid write once. This proves avoided work; it does not measure throughput or latency. Current compiler inputs `1e9199b4ae9e00076242ba003748c362047c9220e8a6b61a348eaec362a14085` pass the full library (**1,259 passed / 2 existing ignores**), full activation lifecycle, full effects (**10 passed / 1 existing ignore**), eleven selected execution controls, ordinary KV and all four published/unpublished recovery image combinations. The native selection totals are **26 passed / 6 failed / 1 existing ignore**; combined application qualification remains **FAILED**.

The six failed cases are ordinary SQL (materialized concurrent first-use ambiguous deadline), RPC continuation (materialized source final-completion ambiguous deadline), ordinary Queue (rooted ambiguous deadline), original C2 Queue (deadline elapsed), materialized C4 Queue (producer deadline) and rooted C4 Queue (pre-load queue-emptying deadline; load oracle unrun). The materialized C4 first target attempt has only 718 ms left; the rooted setup attempt has 2,408 ms left. These observations identify progress costs at different phases, not incorrect acknowledgements or a causal consensus-algorithm regression. The independent parent/candidate/candidate/parent RPC comparison passes all four runs with identical build profiles; it does not erase or explain the original broader-run failure. The hosted provisioning refusal remains undiagnosed despite the local recovery pass. The other hosted native job fails an archive-seed cleanup ownership/byte assertion; that managed target was not rerun in this selection. Python ABI/SDK and filesystem release jobs pass on the published parent; Rust aggregate jobs remain red because of their native prerequisites. All nine strict/repository checks pass, and all 24 reviewed source anchors remain byte-identical. See the source-bound cache qualification for source/profile/executable/log hashes, failed setup controls and retained-filesystem verification.

Follow-up `95e95a35e` distinguishes the provisioning configuration guards with static reason-specific `Invalid` messages. Every original predicate, evaluation order, admission bank and typed refusal remains. The five owning native provisioning controls pass in both image modes. All-feature library Clippy, native-only facade compilation and all seven repository checks pass; this qualifies the diagnostic change only. Temporary SQL phase probes pass twice, while one archive probe fails replacement reconciliation and another passes. These results do not explain the original SQL deadline or hosted cleanup assertion. The hosted `607ca35b` disaster target again fails retained-voter provisioning (12 passed / 1 failed / 8 ignored); its exact guard remains unknown until the diagnostic source runs. See the provisioning diagnostic record for source and validation boundaries.

Completion priorities remain:

1. **P0 — Runtime progress and recovery:** close original SQL, RPC continuation and Queue deadlines, including C2/C4 delivery/drain bounds in both image modes. Diagnose the hosted retained-voter provisioning refusal; local passes alone do not close hosted SDK acceptance.
2. **P1 — Routing and read coverage:** finish negative-path repair, authority/ambiguous-outcome isolation, demand-refresh/recovery lookup cuts and partition coverage for OPT-01/02/03.
3. **P1 — Sustainable shared execution:** measure batching efficiency and SQL/image/archive interference under load while preserving ordering and fairness (OPT-06). Prove shared-file/segment deletion and once-only capacity credit before repeated reserve-aware movement (OPT-04/05).
4. **P1 — Lifecycle and combined release:** qualify wake and interrupted-transition/recovery paths (OPT-07), then the complete primitive/SDK/Axum/daemon/feature/backend-fault/crash suite and historical CG evidence (OPT-08).
5. **P2 — Matched comparison:** rerun SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 on the original three 1-vCPU/2-GiB nodes, RF3/three groups/shared RustFS, 256-byte payloads, Queue batch eight and reversed repetitions. The latest read-only observation at 2026-11-19T01:33:45.303616+00:00 finds zero available bytes against the unchanged 6-GiB reserve; the original identity/resources remain stable with zero restarts. No constrained comparison is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open.


### Configured rooted previews and current priorities

Implementation `070e6abe` includes `origin/main` `2f083f14`. Rooted preview verification now uses the node's actual committed-apply profile for enrollment, commands and reopened RPC writers. Previously it derived that profile from execution bounds; the exact-profile guard correctly refused reuse when those settings differed. The new path independently validates the captured LTX under the configured apply limits and binds the seal to that same profile. Execution/scratch/capture bounds, workers, deadlines, public APIs and exact engine/group/input/profile/store checks remain unchanged.

The identical physical fixture and build profile fail the over-limit-preview oracle with the former selectors restored, then pass with the configured selectors. This control retains the new plumbing; it is not a pristine published-parent benchmark. Current compiler inputs `321cde1a365fac246e0796b98c390e8b6234feac76c92fd3b7194d43478fe914` pass the full library (**1,259 passed / 2 existing ignores**), full native effects (**10 passed / 1 existing ignore**), eleven selected execution controls, three RPC controls, ordinary KV and Queue, and disaster retry/old-token refusal. The remaining selected native case, ordinary SQL, **fails**: materialized mode has a deadline at another phase, and rooted mode times out during concurrent first-use ingress. `IngressRace` is that scenario's name. The original nine strict/repository checks pass after reviewing the changed SQL pool anchor and rerunning all 44 plan tests. The initial source-drift rejection remains recorded. An additional full plan CLI validation still fails on missing historical CG-01 evidence; that gate remains open. Native selection totals are **27 passed / 1 failed / 1 existing ignore**; combined application qualification remains **FAILED**.

Failure diagnostics now have a separate retained directory/store handle and do not index erased source roots or recreate voter directories. The original erasure, fencing, retry, token and cleanup assertions remain. The prior hosted failure did not reproduce locally; a passing recovery case does not resolve its underlying production cause or full SDK qualification.

Priorities remain:

1. **SQL and Queue progress:** diagnose the current SQL deadlines and close original Queue C2/C4 delivery/drain bounds. Ordinary Queue passes here; the earlier C2/C4 failures remain open and were not rerun in this selection.
2. **Shared-runtime performance:** measure configured rooted reuse, persistence batching, fairness and interference at the original limits. This change supplies no qualified throughput or latency gain.
3. **Capacity and lifecycle:** qualify physical reclamation with retained owners, once-only credits, reserve-aware movement and the remaining recovery/SDK cases.
4. **Original constrained comparison:** rerun SQL/KV/Queue across Quorum, Quorum+S3, Fleet and Bucket at the requested topology and resource limits. The read-only original RustFS observation at 2026-11-18T21:53:35.836264+00:00 still finds **0 available bytes**, below the unchanged **6 GiB** reserve, so no comparison is admitted.

Full OPT-01–08 and remaining CG/SDK/release/performance gates stay open. See the source-bound qualification for source, executable/profile/log hashes, failed controls and retained-filesystem scope.

### Separate S3 output and mutation readiness

Implementation `5546c388` includes `origin/main` `2f083f14`. A committed S3 response could leave while a later unrelated-cell mutation was held at the real group-manifest sync. The trace observes a successful S3 response at 1,243 ms within the original 5,000 ms budget. Mutation preparation intentionally tolerates a bounded unrelated-cell prefix; output had incorrectly reused that exception. Missing archive data or data loss was not established.

S3 command output now requests an atomic whole-prefix authority check from the existing group owner. It retains the exact committed receipt and archive proof until applied equals committed, then performs the existing route, leader/catalog, token and deadline checks. Preparation keeps its original per-cell prefix optimization. Budgets, workers, deadlines, public APIs and wire markers remain unchanged.

The paired regression requires preparation authority to succeed during the hold while retaining every original S3 pending-response, receipt/version/replica, handler-count and cleanup assertion. Before the fix, preparation passes and the original output oracle fails in the first materialized/fresh case; the other three iterations are unrun. The identical fixture/profile passes all four materialized/rooted and fresh/duplicate cases after the fix. All temporary phase instrumentation is removed.

Current source `35fa251497d583e62d8c19dfb611d95ed023dd09f52758f3071b0801c02d8db8` passes the full native effects target (10 passed / 1 existing ignore), eleven selected native execution controls and all nine strict checks. The controls retain unrelated-cell preparation, shared persistence, intervening-read ordering, event capacity, readiness deadlines and exact live-image work counts. This is a scoped run; the prior full library/native execution results belong to source391b and are not reattributed to this source.

Original application-boundary feedback (**FAILED**):

- `native_cell_groups_primitives--ordinary_sql_bindings_use_managed_shared_groups`: **FAILED**.
- `native_cell_groups_primitives--ordinary_kv_bindings_use_managed_shared_groups`: **PASSED**.
- `native_cell_groups_primitives--ordinary_queue_callbacks_use_shared_quorum`: **FAILED**.

The initial rooted concurrent SQL ingress failure records zero S3 coverage/output-check starts on all three nodes. Pinned parent391b/candidate35fa controls in A/B/B/A order: A:PASSED; B:FAILED; B:FAILED; A:FAILED. The additional current-source S3 SQL error/recovery control is PASSED. Initial failures remain recorded; controls do not erase them.

The read-only original RustFS observation at 2026-11-18T19:58:09.774810+00:00 finds 0 available bytes, below the unchanged 6 GiB reserve. Original identity/limits and the inspection pair are stable, with zero restarts. No constrained comparison is admitted.

The original Queue C2/C4 failures on source391b remain open; those minimum-Quorum load cases were not rerun for this S3-only output fix. The published-parent disaster/token recovery failure still requires diagnosis, and local S3 success does not close hosted SDK acceptance. Remaining priorities are original-deadline Queue progress, recovery/SDK faults, actual extent-profile propagation, measured batching/fairness, physical reclamation/once-only credit and reserve-aware movement, then the full original constrained comparison. Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open. No throughput or latency gain is claimed. See the source-bound S3 fence record.

### Disposable close checkpoint work and current priorities

The materialized shared-cell close now avoids copying a disposable WAL into the private main file immediately before that file is discarded. The real-close regression independently seals and syncs the result, observes the old main inode across actual cleanup, and rebuilds the correct value from the sealed result. The same fixture fails before the policy change and passes after it. This proves avoided work; it does not qualify throughput or latency.

Committed implementation `aa93bb82` includes `origin/main` `2f083f14`. The full affected library (1,258 passed / 2 existing ignores) and native execution (232 passed / 2 existing ignores) suites pass. All nine strict checks pass. Nine of ten selected SQL/KV/Queue/RPC/recovery checks pass; ordinary Queue fails with an extent-iteration deadline. Combined qualification remains **FAILED**. Original Queue C2/C4 load qualification still fails. Two temporary C2 phase probes reach successful enqueue completion at peak two, then fail the original final consumer drain; one observes 472 of 528 required completions. Their diagnostic sources and all failures remain distinct from the restored candidate. The probe code is removed. Ordinary Queue and batch-proof A/B/B/A controls also reproduce failures on the published parent; they do not establish a candidate-specific regression or close Queue progress.

Completion priorities, in order:

1. **P0: Progress and proof correctness.** Close original-deadline Queue delivery/drain in both image modes, repeatable provider-error handling and remaining receipt/RPC/recovery failures. Published-parent hosted native-framework failures include disaster/token recovery's diagnostic panic and the S3-output pending-response oracle. Classify the actual S3 response before attributing an incorrect acknowledgement.
2. **P1: Complete configured fast paths.** Carry the configured extent apply validation profile into independent image preparation. The public profile currently refuses reuse because execution-derived preparation and committed apply limits differ. Preserve exact-profile equality, negative controls and existing budgets.
3. **P1: Bounded persistence and scheduling.** Measure and reduce SQL queue/image work, physical sync amplification and archive interference. Preserve Ready/sync/completion ordering and progress for leases, elections, control and shutdown; sparse traffic must not wait to fill a batch.
4. **P1: Sustainable capacity.** Prove physical shared-segment deletion and once-only capacity credit, then repeated reserve-aware split/move/merge with retained readers, snapshots and uncertain work.
5. **P2: Combined release and comparison.** Complete authority/partition/wake/recovery, SDK/daemon/feature-mode gates and the source-matched SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 matrix on the original three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared RustFS, 256-byte payloads, Queue batch eight and reversed repetitions.

The latest read-only original RustFS observation still fails the unchanged 6 GiB free-space reserve; no constrained comparison is admitted. All OPT-01–08 and remaining CG/SDK/release/performance gates stay open. See the source-bound close qualification for exact sources, artifacts/profiles, all outcomes, retention limits and capacity evidence.

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
inside that test are unrun. See the source-bound record for every
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
source-bound Queue record
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
its stopped qualification and all failed data remain explicit in the source-bound scratch record.

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
The current record
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
previous settlement record
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
origin-denial record.
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
command-admission record.

The latest native batching checkpoint
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
source-specific record.
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
   identities in the lease,
   seal and
   transition records.
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
   See the scoped refusal and runtime record.
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
   peer-query checkpoint.
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
source-specific record
does not establish a constrained-node throughput/latency gain.

SQL now has an explicit `prepare_read_only` path through fresh quorum selection
and a pinned image, preserving the writable API and source-write proofs. The
benchmark uses it for SQL point reads. Its
component qualification does not
replace the constrained matrix or close the remaining enrollment, lease-progress
and release gates.

The private SQL workspace constructor also omits creation syncs through a
separate disposable API. Full reservations and synced physical deletion remain;
the original durable constructor and image/Raft/S3 barriers are unchanged. Its
qualification records the exact
avoided-work oracle and broader limits. Repeated passing native enrollment runs
do not erase the earlier timeout or establish a constrained-node speedup.

The archive retirement checkpoint
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

The subsequent generic payload-I/O repair
at compiler input `b32cb4bc` keeps that archive owner retryable under its original
exact journal intent and requires fresh byte verification. Missing/corrupt
payloads and journal failures remain terminal. The unchanged owning fixture
turns four expected stopped-owner failures into four passing retries, with
terminal data-refusal controls preserved. Native core/archive/execution,
selected API/recovery and strict Clippy gates pass; all 22 selected RustFS
invocations pass, including two independent Axum KV invocations. Historical
failed owners remain failed at their own source cuts. Broader payload
deadline/backpressure, crash and release coverage remains required.

The subsequent payload admission repair
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
metric qualification changes
no Rust runtime inputs. Fresh hosted CI, disaster authorization and container
capacity qualification remain open; the local guard repair does not establish
release readiness.

Native fitting-credit progress is now qualified at `1ac2cced`: the actual reused
archive registry receives the publication's private payload hint while keeping
the original shared bank and namespace capabilities. The
source-specific record
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
qualification preserves the
same two-error compiler control, passing example check, actual cross-runtime
client-factory test, complete embedded-Python test-target compilation and strict
example Clippy. Only `tools/python/fleet.rs` wiring changes after the native
runtime qualification. Full Python adapter runtime/3.14t execution and fresh
hosted CI remain required; the native disaster and Typed RPC lease failures are
separate unresolved boundaries.

The periodic purge qualification
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

All four requested stages remain required: ordinary paths, complete primitives,
managed recovery and qualified public cutover. PR #182 integrates merged #181
and #167 at runtime `0adc0eb8d`. The archive-retirement isolation regression and
three existing real-RustFS archive functions pass both images. Library and static
checks pass, but the canonical Docker/Sandbox campaign is **FAILED: 6/10
functions, 18/25 configuration markers**. Both extent Container and both Quorum
Sandbox lifecycle profiles pass; several original S3/request deadlines and the
extent explicit-Ready oracle remain open. Remote enrollment's original 800-ms
control still fails before the retained-S3 precondition. See
[exact results and custody](progress.md#cg-17-independent-publication-during-archive-retirement).

The archive-key correction at `bd238f365` passes five owning tests and all six
real RustFS floor/Delete/Start configurations. It preserves every original proof,
identity, minimum and budget. The wider native baseline has 25 passing functions
(including all 22 storage persistence/crash functions and Axum KV), two failing
EventStream functions and 135 unrun functions. Prioritize the recovered extent
Read6 typed refusal and public Consume ingress deadline alongside the ordinary
Container/Sandbox deadlines. Failure-only Read diagnostics preserve the original
cause before selecting a production change. Separate successful traces and
pre-#167 results cannot clear canonical failures.
On `1b6dfb3f7`, canonical public SQL passes 3/3 real-RustFS functions and KV passes
2/2, including native SDK/actual CLI and Axum, both image modes, original recovery
budgets and quorum refusal. SQL interrupted binding recovery and large KV
payload/condition/expiry paths pass. Independent constrained Linux daemon
qualification now passes all four SQL/KV configurations at `41e68a7bf`: both
images and Quorum/S3 minima, three one-CPU/3-GiB voters with separate retained
ext4 directories, and real RustFS at one CPU/2 GiB. Native/Axum daemon handlers,
compiled CLI, rollback, renewed startup leases, leader kill, retained restart,
archive-write refusal/restoration, minority refusal and joined shutdown pass.
The exact EventStream read replay
passes with unchanged production, while activation/Consume reproduces its
deadline failure; neither earlier failure is considered fixed. Verified source,
executables, raw results and failed media are retained separately.
All [32 required cases](primitive-acceptance-cases.md) remain in scope.

Deliver and close these milestones in order:

1. **Ordinary paths on combined main.** Preserve the passing SQL/KV host-native
   and independent Linux daemon gates, then fix
   the exact current Container/Sandbox deadline and confirmation failures, remote
   enrollment boundary and #167's SQL/Queue service failures. Keep
   source IDs, typed ambiguity, minima and all original request/recovery budgets.
   Preserve failed media before repeating a case. No default cutover while these
   paths remain unqualified.
2. **CG17-01/02 complete acceptance packages.** Finish the partial public API and
   deployment inventory and real Storage/SQL/KV drivers. The independent validator
   exists; a partial inventory and aliases of focused helpers cannot satisfy the
   full cases. Cover transactions, rollback, conditional updates, consistency,
   versions, binding boundaries and supported feature/entry-point configurations.
3. **CG17-03 through CG17-07 feature contracts.** Complete callbacks and binding
   recovery, due work, SQL sessions/KV expiry, Queue, EventStream, Workflow,
   filesystem/payload and receipt GC. Finish ordinary Container application
   entrypoints, interactive/network/WebSocket APIs, provider fencing and volume
   recovery. Every public operation needs its documented behavior and real
   acceptance evidence; bounded exec is not the complete Container family.
4. **CG17-08, CG-13 and CG-15 recovery/release gates.** Exercise every stateful
   family through independent Linux-voter leader loss, partitions, replacement,
   retained restart, split/merge/placement and park/wake. Use real RustFS outage,
   bounded archive debt, physical reclamation and permanently fenced all-voter
   disk loss with fresh incarnation. Run load, cancellation, capacity and joined
   shutdown with complete data/effect ledgers on one frozen source.
5. **CG-16 cutover.** Route SDK, daemon, native/Axum, CLI/admin and examples through
   the qualified shared backend, refuse missing quorum and remove per-cell Raft
   plumbing. Keep read consistency explicit and preserve other supported
   durability contracts. Existing Fleet data is not silently reinterpreted.

Close the goal only when all required case oracles and dependency gates pass,
the complete API/configuration/scenario report passes the independent validator,
and the release source passes the integrated public/fault matrix and hard-cutover
checks. Record verified coverage and missing cells after each delivery; generated
task credit changes only with complete accepted evidence. Historical CG-01
custody is still missing from the full local plan validator. Earlier timing,
post-restart named S3 Relay7 and post-reclamation failures retain their scope.

Runtime `6836d6a6c` delivers the ordinary named-provider lifecycle handoff and
nonwaiting shared-group background admission. The actual custom-provider Start
passes four native and four real RustFS/Docker configurations, with one admitted
backend and zero legacy direct calls. Three native profiles pass 12 configurations;
26 library units and seven ordinary native provider functions also pass. Public
Create preserves its entry proof and exactly three versions; direct journal
Create keeps two. The original version-count failure remains retained; neither
budgets nor assertions changed. These are scoped passes, not full primitive-family
acceptance. See the [current checkpoint](progress.md#current-integration-checkpoint).

Latest `origin/main` `76a440139` is merged at `0cee7536f`. The incoming LTX
reload regression, 26 library units and seven ordinary native provider tests
pass with strict affected Clippy and formatting. The shared-provider results
remain pinned to `6836d6a6c`; the complete combined release gate remains required.

Runtime `4f87197ec` delivers Raft-only binding preparation: intent plus execution
claim share a proof, and final observation plus command result share the output
proof. Create uses two committed versions rather than four. Existing durable CAS,
original IDs, five-second requests and Quorum/S3 minimums remain. Native provider
gates pass 24 configurations; real macOS/RustFS/Docker cleanup passes four, with
actual S3 write-refusal counters and exact receipt-only recovery. Combined
constrained Linux/RustFS passes all 16 fixed functions and 54 configurations.
This is a scoped delivery, not full primitive-family acceptance.

**Deliver normal native Container/sandbox adapters and close the failures.**
The ordinary final-source Fleet suite is 3/5: an accepted-create successor closes
before cleanup and retained restart stops with `NodeLeaseMismatch`. Preserve
those roots as the outage fixture now does, and diagnose without replacing IDs or
extending budgets. Earlier provider-outage, native extent/S3 Read and macOS S3
supervision Delete failures remain open; passing controls do not prove a fix.

The ordinary named-provider guard handoff is implemented and qualified for the
custom Start profile. Native Container needs an async activation prologue,
request-bound provider transport and full logical history. Old Contexts must never borrow newer global
grants. Qualify the new core behind authenticated per-operation transport and
independent process/infrastructure fencing, including permanent old-history
revocation. Then finish WebSockets and every API/provider/feature/deployment/clock/retention/
cancellation/recovery ledger. Fresh-history DR cannot bypass execution revocation.

RF3 shares one three-CPU/4-GiB Linux process/container with zero swap and ext4
disks; RustFS has one CPU/2 GiB in the same VM. This does not qualify independent
voter/controller faults. The fixture controller has volatile state and stays live
through voter restart. Native Docker remains ephemeral; other provider-volume
contracts still apply. The current inventory is partial at 18 entries/305 symbols. Formal
CG-17 acceptance remains 0/32; historical CG-01 custody is also missing. See the
[current checkpoint](progress.md#current-integration-checkpoint) for source pins,
failed controls, proof counts and retained evidence.

Runtime `feb22476c` delivers declared provider journals and local invocation/proof
authority through shared-group storage. Twelve RF3 configurations pass: eight
native, including small caller-input limits, and four real RustFS. Declaration
checks, the existing Fleet journal/recovery test, locked build, strict affected
Clippy and repository checks pass. An unfinished S3 checkpoint stays explicitly
uncertain and never enters provider work. Real provider execution fencing,
supervision, sandbox and native Container adapters remain required; this is their
journal foundation. Inventory expands to 244 public symbols and remains partial.
See the [current checkpoint](progress.md#current-integration-checkpoint) for exact
controls, the corrected new-test expectation and limits.

**The Linux binding/Consume/Ack settlement slice now passes:** all eleven fixed
RustFS functions and 34 image/minimum/fault configurations at `5b3cd177e`.
The original public/command/maintenance budgets and proof gates are unchanged.
Linux voters share a cgroup capped at three CPUs/four GiB with swap disabled;
RustFS has a separate one-CPU/two-GiB cap, and voter files use ext4. The frozen
binary, source and actual cgroup/mount observations are retained. This accepts
the scoped Linux delivery, without closing a full primitive family or CG-17.

Runtime `9dbdc597d` fixes lost private S3 settlement outputs: durable confirmation
metadata, receipt pins and private receipt-only recovery now survive the original
response deadline without replaying business work. The controlled red fails
before physical SQL admission is carried through; the unchanged RustFS control
passes both images afterward. Live checkpoint/unused-sequence recovery passes
all eight configurations and retained named-call recovery passes all four.

The earlier **macOS** full `binding-settlement` campaign remains **FAILED,
5/11 functions pass**, at `9dbdc597d`. Ordinary Quorum-source bindings pass both images, but S3 sources miss
sequence 7 after restart or sequence 1. Native extent Consume returns an ambiguous
parent after the child Ack; ordinary Axum passes both images. Independent-target
S3 cases fail initial materialized event activation, leaving extent unrun. New
interrupted-Ack cases first failed fixture class registration and a wrong
single-group teardown check. Corrected test source `5b3cd177e` passes 7/8
configurations, including all four S3-source live/restart/image combinations;
the remaining Quorum/extent/live case fails initial node-0 activation before the
fault. Its whole macOS Ack gate remains failed. Linux passes all eight Ack cuts
and the entire settlement campaign. Keep the macOS failures open; the Linux
comparison does not isolate their cause. Next qualify the merged provider/work
source and complete the API/lifecycle matrix. See the
[current checkpoint](progress.md#current-integration-checkpoint) for sources,
counts, failed controls and scope. Selected recovery passes do not close CG-17.

### Main Queue backport audit

These source-pinned records are historical. Use the current checkpoint for the
latest binding/provider results and remaining priorities.

The [2026-11-19 Queue audit](progress.md#cg-17-queue-transfer-payload-identity)
finds and implements complete-payload binding for retained DLQ transfer receipts.
Previously changed bytes could reuse the old acknowledgement after redrive had
removed the message. The receiver now preflights the complete batch and returns
known conflicts as Quorum/S3-protected RPC errors; capacity/storage/proof failures
retain their existing failure semantics. Runtime `3500e49c5` passes all three
scoped real-RustFS cases and six native/Axum image configurations, including
retained restart and the existing lost-S3-reply fault case. The older standalone
campaign remains failed. Public backport `9667e6e14` requires the corresponding
shared-runtime integration for that qualification. This internal schema cutover
requires fresh Queue roots and matching Raft namespaces.

### Previous archive and public-path results

Runtime `85901c941` adds two bounded checkpoint creates with exact GET
confirmation and recovers failed uploads from their durable reservation intent,
without a redundant ticket fsync. Both red regressions reproduce; native and real
RustFS correctness branches pass in both image modes. Fifteen archive functions
pass at pipeline source `0d2de06bb`; six selected RustFS controller functions and
the native reservation regression pass at final runtime `85901c941`. Every remote
digest, physical recovery and output proof remains. These are selected results.

Runtime `8fa46e7f1` overlaps fresh bounded registry control reads and carries
confirmed retirement cleanup in the next required durable intent. Quiet and
shutdown cleanup still persist; every allocation and publication keeps its
barrier. Both owning red regressions reproduce, then pass native/real RustFS in
both image modes. Twenty selected archive/controller functions, the locked build
and strict affected Clippy pass. Every physical and output proof remains.

The original public binding gates remain **FAILED, 2/4 configurations pass on
each backend**. Quorum sources pass. Native materialized/S3 and extent/S3 sequence
1 fail; RustFS materialized/S3 sequence 1 and extent/S3 sequence 3 fail. Child
replies precede parent response expiry; the exact final failing stage is unproven.
Earlier registry-only RustFS
passes 3/4 but fails extent/S3 sequence 7 after restart. Its diagnostic and the
focused I/O/stack profile also fail; they do not establish an exclusive cause or
deadline fix. Preserve the original IDs, budgets and final proof gates. Do not
repeat broad timing runs without a new causal implementation change.

Close autonomous recovery/sealing and exact maintenance-command settlement,
EventStream Consume/Ack and managed startup/joined cleanup. Their prior failures
and unknown identities remain required work. The recovery/sealing implementations exist; their complete
public matrix has not passed. The separately owned replacement RustFS lab is
qualified for these scoped checks and stopped with its volume retained. Old lab
contents remain unavailable; native in-process voters do not qualify independent
Linux voter resource limits.

Then deliver complete primitive/provider/feature/lifecycle cases and actual
per-API acceptance ledgers. All 32 formal cases remain required. The inventory is
partial and the generated implementation index retains the correct in-progress
status. See the [current checkpoint](progress.md#current-integration-checkpoint)
for source pins, original failures, diagnostics and qualification limits.

Earlier runtime `ee2452a94` bounds incomplete primitive operator uploads with the
original deadline and closes their connection before dispatch. The old runtime
fails both image forms; the fix passes native 2/2 and real RustFS 2/2, including
stream release, no mutation and healthy follow-up requests. Isolated 25-MiB CLI
and unchanged full public KV checks each pass real RustFS 2/2. Earlier publication
failures remain retained and unexplained; successful timings do not prove a fix.

Earlier unchanged controls at test-only `73f41e2b0` fail native extent's initial activation and the
RustFS materialized S3-source delivery `Read` before `Consume`; native materialized
passes, RustFS extent is unrun. Preserve original budgets and typed ambiguous
outcomes. Complete every primitive family and the 32 planned evidence gates;
these scoped passes do not close CG-17. See the current checkpoint for source
pins, failed controls and the separate prior streamed-KV delivery.

Earlier production `34e16afa8` fixes foreign KV revision conditions by binding complete
logical cell history; the operator regression proves the earlier bug. Native
4/4 passes; test-only stage instrumentation `e76598cfd` passes native 4/4 and
real RustFS 4/4, with both image forms and target minima. Fleet conformance and
token validation pass 2/2. Full public KV remains **3/4**: native extent's first
large read after payload restoration and retained restart fails with ambiguous
sequence 50. Earlier uninstrumented RustFS materialized/S3 fails with ambiguous
sequence 4; its cause is unresolved despite the later traced pass. No full
family or task acceptance follows. See the current checkpoint for source pins,
the schema-bound hard cutover and retained failed controls.

Runtime `2da96dcd6` delivers typed SQL failure ownership: five native value/error
tests and both public SQL tests pass, with a byte-identical failing baseline.
The separate retained-restart campaign is **failed: native 7/8, RustFS 2/8**.
Native restart observes a lease-expired fenced node; RustFS fails four group
startups, one readiness check and one ambiguous enrollment. Preserve the 21
failed voter disks and original identities. See the current checkpoint for exact
stages, sources and limits; no complete binding or primitive gate is accepted.

Basic managed SQL/KV daemon delivery now passes all four constrained Linux/RustFS
image/minimum profiles at `f4db9e450`: ordinary SDK calls, compiled CLI onboarding,
native/Axum application adapters, rollback, retained restart, leader kill/rejoin,
archive-write denial/restoration, quorum refusal and joined stop. Explicit managed
origin policy and startup lease renewal are delivered. See the current checkpoint
for exact source pins, cgroup proof, failed controls and scope.

This is NativeDaemon hosting with two groups/four cells, not the complete SQL/KV
API or primitive family. External Axum hosting, local follower catch-up, full
backend outages and same-ID uncertain stateless replay are not established here.
The earlier `db35703e9` owning 24 configurations pass on their separate source,
but its original public Consume cohort is still failed (0/3); `5425b30c5` RustFS
source-S3 bindings still miss the original five-second budget. Those failures
remain required work.

Deliver next in this order:

1. Build on the integrated Linux-qualified delivery at `2ad84151f`. Connect
   execution claims to framework-owned cleanup and normal Container/sandbox
   adapters, then WebSockets. Qualify durable independent controller enforcement
   through crashes, cancellation and old-history revocation.
   Preserve the earlier macOS final-parent deadline and cold-activation failures
   as open gates. Complete managed cold startup/cleanup on the qualified release
   platforms within their original request deadlines. Measure apply, publication and verification on
   the actual accepted proposal; share only exact independently verified work
   under the existing admission limits. Then close original-deadline S3-source
   `Read`/`Consume` and autonomous interrupted bindings with safe sequence sealing.
   Preserve original identities, full source/target proof gates and unknown
   outcomes. Earlier managed startup/lease, CLI publication and post-outage
   large-read failures still require recovery/regression closure; newer scoped
   passes do not establish an exclusive cause or waive those controls.
2. Deliver real public-family acceptance drivers and complete per-API ledgers as
   each family closes. The declared target currently has **0/32 planned acceptance
   entry-point functions**. Do not replace full case oracles with aliases of
   focused helpers. Finish SQL sessions, KV TTL/revision/conditions and independent
   binding/stream ownership, then Queue leases/ack/drain, durable alarms/cron and
   Workflow execution across deployment and clock changes.
3. Complete filesystem/payload publication and retention closure, HTTP/WebSocket
   and actual provider execution fencing/reconciliation. Review the complete
   symbol/feature/provider inventory and run the independent coverage matrix.
4. Finish CG-13/17 movement, wake, archive retention/reclamation and fresh-history
   disaster recovery on independent constrained voters, then CG-15/16 cutover.

All 32 CG-17 cases and existing dependencies remain mandatory. Acceptance stays
**13/17 tasks, 147/195 cases**. Historical CG-01 evidence and the documented older
custody gaps remain open. Current failed controls, disks, logs and binaries are
retained; owned runtimes are stopped after qualification. A new profile's pass
cannot erase an existing failure or authorize the full default cutover.

### Earlier binding checkpoints

Previously, runtime `6c876b46` retained proven uncontended source writers
across owned binding I/O; scoped synchronous-wait guards still force yielding for
dependent writers. The causal storage-denial baseline fails on an unnecessary
reopen. Final test-wiring source `eeeb4029` passes native/RustFS and both images:
four configurations, zero reopens, exact parent replay and one child effect.
Eight compatibility selections and five owning units pass; named bindings cover
all eight backend/image/source-minimum configurations with original budgets.

**That complete original public cohort is FAILED, 6/16.** All eight
EventStream selections fail activation or Consume; both Queue load selections
fail production. Native materialized producer sequence 7 expires. RustFS delivers
528 materialized messages, then extents producer sequence 23 expires; no complete
extents drain result exists. SDK source-proof/recursive dispatch, current-history
resolution, registration and Fleet reference pass. Final locked build, strict
library/all-test Clippy and repository checks pass. Frozen evidence and 45 failed
voter disks are verified; owned RustFS/VM are stopped with objects retained.
This closes a writer-lifetime behavior, not a full primitive family or release
gate. Do not infer stability or an exclusive timing cause from isolated passes.

Previously, runtime `7937adc1` pipelined two archive-tail entry downloads
under the existing byte/operation reservations, preserving ordered object checks,
independent replay and physical verification. A serial baseline fails the overlap
oracle; native/real-RustFS and both images pass the data, bounded-window,
corruption/join and unchanged-serving-state checks. Eleven archive safety cases
pass. Named bindings pass all eight backend/image/source-minimum configurations,
including retained restart and original five-second S3 requests. Their prior
deadlines do not reproduce here; this is not exclusive cause, speedup or stability
evidence.

**That complete original public cohort is FAILED, 7/16.** SDK
source-proof/history, registration, real-RustFS Queue and Fleet reference pass.
All eight EventStream selections fail initial activation or Consume. An Axum
Consume begins with 2,999 ms of 3,000 ms remaining, so inspect execution inside
the actual target command. Two initial catalog history selections take 451/835 ms
before readiness polling expires; inspect autonomous catalog/broker reconciliation.
Neither observation proves an exclusive cause. Native Queue materialized 528
passes, then extents producer sequence 11 expires; no complete extents drain
result exists. RustFS passes both 528-message image forms with p99 1,157/2,614 ms.
Aggregate 502 retains 27 failed voter disks and verifies the frozen evidence.
Final build, strict library/all-test Clippy and repository checks pass; owned
RustFS/VM are stopped. Complete primitive and release gates remain open.

Previously, runtime `2b4ae45e` added one fresh retained-target lookup after a
known pre-initializer refusal. A different creator still needs durable source
selection before child dispatch; the candidate's own proof, uncertain identities,
normal command admission and current fences remain. A causal public regression
fails with only the fallback removed and passes native/real RustFS and both images
at final source: four configurations with original eight-slot admission, one SQL
worker/voter, exact parent replay and one child effect/initialization.

**That runtime's original public cohort is FAILED, 8/16.** SDK source-proof/history,
registration, native Queue, target-S3 Axum and Fleet reference pass. Seven
EventStream selections fail initial activation or Consume. RustFS Queue passes
the materialized 528-message/unique-ID oracle, then extents drain/wait expires. The previous
14/16 at `a0fe85d1` is historical, not current acceptance. Frozen pre-feature/current
native ordinary/Axum controls both pass 2/2 with identical test/backend source;
current controls use the failed cohort's binary. These do not erase failures or
establish stability, an exclusive regression cause or speedup.

Compatibility cohort 455 is 5/6 at `055c2183`, with production identical to final `2b4ae45e`; later
changes add the causal fixture. Both named Quorum configurations, interrupted
recovery and read regressions pass. RustFS named source-S3 materialized command 3/extents command 7
exceeded original budgets there; current cohort 499 passes all eight configurations.
Before that feature, instrumented cohort 450 passes and 451
fails both selected activation cases. Native initial configure took 2.1 of three seconds,
then polling expired. Shared-worker/provisioning and S3 proof phase costs remain
investigation boundaries, not established exclusive causes.

Full backend-outage/cold-discovery qualification retains its previous assertion
gap. All failed controls remain recorded. The complete primitive/release gate is
open; the new pressure regression closes one behavior, not a complete family.

Earlier catalog enrollment joining, bounded suspended scanning, exact checkpoint
receipt reuse, clean Quorum entry-image reuse and idle archive-proof reuse retain
their scoped checkpoints. Full primitive support and release cutover remain open.

1. **Close public activation, Ack and Queue settlement.** Exact S3 responses
   now progress during certified unrelated apply, but current target-S3 activation
   still fails in both ingresses. Selected catalogs from prior 787 are active by
   shutdown, not proven ready by the deadline. Finish readiness and exact source
   settlement with the already committed child Ack; preserve outer sequence **2**
   and prior Queue producer **21/native**, **25/RustFS** unknown outcomes. Complete
   producer/drain oracles under the original limits. Then qualify lost reply,
   transfer, generation, pruning and slow-consumer/GC behavior. Scoped output
   progress does not replace the complete public family gate.
2. **Finish S3 binding stability and interrupted recovery.** Current named bindings
   pass all eight backend/image/source-minimum configurations; preserve earlier
   source-S3 deadline failures and establish combined lifecycle progress. Preserve
   the original
   five-second budget, exact birth/route, physical custody, coverage,
   version/witness and stored receipt checks. Reduce bounded replay/settlement
   work without weakening proofs or repeatedly running unchanged broad matrices.
   Retained images do not establish timely archival or current serving state.
   Qualify genuine cold discovery and full backend outage, then both images and
   all source/target minima. Finish autonomous interrupted binding/delivery/ack
   recovery, safe unused-sequence sealing, long polling, deletion/recreation and
   retention through movement/fresh-history recovery. Preserve unknown mutation
   identities. Resolve trust refresh and concurrent boot reconciliation; the
   [EventStream contract](event-stream-durability.md) keeps source, destination
   and output boundaries separate.
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
| CG-17 | In progress. Candidate `8e3dafe0a`: 18 owning tests pass; real RustFS is FAILED, 4/10 whole functions. Physical Container/Sandbox has a separate failed 3/6-function source boundary. | Close original S3 completion deadlines and the deterministic public handoff gate; qualify integrated physical Container/Sandbox; finish public drivers, GC/WebSockets and the full 32-case matrix. See the [current checkpoint](progress.md#cg-17-cooperative-inspection-handoff-candidate). |
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
