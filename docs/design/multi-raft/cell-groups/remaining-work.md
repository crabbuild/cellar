<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Remaining cell-group work and priorities

Purpose: Prioritize the remaining cell-group MVP, merge gates and full architecture deliverables.

PR [#160](https://github.com/crabbuild/cellar/pull/160) contains the shared runtime
and the accepted public SQL cutover. This ordering preserves the full
CG-01–17 objective in [tasks.json](tasks.json), [design](design.md) and
[qualification](qualification.md). See [progress](progress.md) for exact source
and validation boundaries and [handoff](handoff.md) for executor context.

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
CG-17 is TODO: full public primitive compatibility is a required implementation
task, not an established capability. CG-15 and CG-16 now depend on CG-17.
CG-16 is pending the combined release gate. Individual checkpoints cover operator-driven split and merge,
placement recommendations, fenced parking and wake, managed recovery and
parts of the qualification matrix. The task-level evidence remains open.

The managed cold-recovery case now erases every owned source voter disk and
reopens from RustFS in both image modes. It verifies restored SQL and receipts
through a fresh quorum, resumes interrupted archive setup, and retires the old
histories. Independent voter processes and permanent Docker launch revocation
remain separate gates. The combined constrained Linux campaign at `27fde17e`
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
| CG-13 | Historical inventory, portable operator handoff, durable assignments, real SDK voters and standalone post-fence controller steps are delivered. At `1095507e`, fresh/retained capacity admission passes real RustFS in both modes; held-background self-inspection and joined-close regressions pass. Two operator descriptions and 26 missing-proof refusals preserve the Fleet heads. Earlier full-recovery failures retain their original sources. | Complete the managed Docker coordinator, including initial archive/intent preparation and the positive seed-to-serving flow after every source disk is deleted. Address the retained liveness failures within original bounds, then pass all six cases on one clean source. Startup, metadata controls and diagnostic passes do not establish the combined gate. |
| CG-14 | **Accepted: 5/5 cases at `23414b47`.** Owned, source-pinned serial campaigns now enforce disk bounds and retain operation, replica, archive-lag, file/engine, kernel and joined native-close evidence. The five-process Quorum matrix passes both modes at occupancies 1/2. | Harness implementation is closed. CG-15 must qualify full profiles, complete historical controls and authority/lifecycle adapters, and resolve the retained S3 deadline miss; a passing reduced reproduction does not waive it. |
| CG-17 | TODO; no primitive compatibility cases are accepted. Existing SQL and alarm/outbox/inbox gates remain scoped foundations. The audit refines delivery into eight packages and 32 required cases. | Begin inventory/evidence enforcement and the shared activation/Storage/async execution foundation. Complete due work, provider fencing, payload closure, public binding/filesystem semantics, retention/schema/time rules and all primitive adapters, then pass the complete matrix in [qualification](qualification.md#full-primitive-compatibility-cg-17). Preserve ordinary Fleet/bucket regressions; missing providers or unsupported families keep the task open. |
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

1. **Finish CG-13 interrupted-transition recovery from S3.** Managed request
   binding to historical launch inventory is implemented at `5e179d75`; portable
   operator handoff follows at `0c85daf5`/`3d780669`, with durable post-launch
   assignments at `d589e538` and a real SDK managed voter process at `0c653ce9`.
   Post-fence controller steps and immediate fresh/retained capacity admission
   follow at `1095507e`, with scoped RustFS/native passes in [progress](progress.md).
   Complete the managed Docker coordinator, including initial intent/archive
   preparation and positive seed-to-serving after all source disks are deleted.
   The earlier full native closure fails during merged-child installation before
   recovery. Address that and the retained
   retirement/archive failures within the original bounds, then qualify all six
   cases on one clean source: closure selection,
   permanent revocation, fresh histories/quorum, exact retry and outbox ambiguity.
2. **Deliver CG-17 full primitive compatibility before broad cutover.** Start
   CG17-01 inventory/report validation and CG17-02 shared activation/Storage
   integration alongside transition work. Follow with async transaction/executor
   fencing, committed due-work discovery, ordinary SQL/KV/filesystem semantics
   and actual payload closure. Build Queue/EventStream/Workflow and the remaining
   public/provider adapters on that foundation. The eight packages require 32
   exact cases plus independently validated API/scenario/configuration evidence,
   with schema, clock, retention and Fleet/bucket/mixed-mode regressions.
   Implementation can start on CG-07/08 now; acceptance retains all lifecycle
   dependencies and the complete failure matrix. The SQL MVP does not establish
   this broader compatibility.
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
