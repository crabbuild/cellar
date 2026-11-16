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

## Current constrained performance priorities

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

| Priority | Required outcome |
| --- | --- |
| P0: request and authority progress | Resolve SQL/KV/Queue deadline failures and node lease expiry without extending deadlines, weakening fences, increasing banks or retrying an ambiguous mutation under a fresh identity. Queue delivery must verify all acknowledged IDs and bodies; an incomplete drain is unqualified. |
| P0: isolate the failing boundary | Keep the physical-credit wake, cadence, dedicated lease-pool and applied-evidence enrollment regressions. Measure their effects in the constrained matrix before attributing earlier lease expiry or latency to a specific boundary. Resolve the remaining preparation, persistence and physical-close costs behind the cumulative rooted-128 enrollment timeout; its successful SQL resolution checks do not establish request success. |
| P0: restore qualification | Repair the stale primitive owner path and reproduce native disaster authorization, container capacity sampling and the materialized Axum KV archive-outage failure. Keep uncertain outcomes, original identities, deadlines and refusal assertions. A later narrow pass cannot replace a failed broader owner. |
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
   disk loss. Bound archive verification by generation/byte capacity as well as
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
