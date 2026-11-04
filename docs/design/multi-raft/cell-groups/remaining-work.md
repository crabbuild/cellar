<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Remaining cell-group work and priorities

Purpose: Prioritize the remaining cell-group MVP, merge gates and full architecture deliverables.

PR [#160](https://github.com/crabbuild/cellar/pull/160) contains the shared runtime
and the canonical SDK/fleet cutover in progress. It is a draft implementation
checkpoint, not a completed MVP or release. This ordering preserves the full
CG-01–16 objective in [tasks.json](tasks.json), [design](design.md) and
[qualification](qualification.md). See [progress](progress.md) for exact source
and validation boundaries and [handoff](handoff.md) for executor context.

## Current boundary

CG-01–06 and CG-08 have passed their recorded component gates. The implementation
now has shared group owners, cell-specific SQLite histories and receipts, complete
manifests/checkpoints, group archives/reclamation, fenced fresh-history recovery,
indexed effects, bounded caches, canonical namespace/group startup and logical
SDK handles. Managed boot, directory refresh, Node stop ownership and shared
membership send fences now pass their recorded component cases. Explicit standby
inventory, cold capacity admission, first RF3 group startup and retained ordinary
restart also pass local cases with two independent cells in both image forms.
Resource-backed peer replacement, joint finish, settlement replay, retained source
physical debt, actual leader handoff and retained joined-member startup now pass
the focused managed case. Old-boot learner-grant reuse refuses before submission.
Catalog maintenance now has a focused local case covering readiness, actual
leader handoff, owned stop, signed offline confirmation, retained paused return,
fresh resume and capacity/disruption settlement. Initial and joined current voters run
in both image forms; both independent cells retain receipts/tokens and serve new
writes through the returned leader. The complete public lifecycle gate remains
required, including failure/cancellation ownership. Shared follower reads, source-age policies and actual responder
metadata advance that cutover. Bounded
public replica preferences now run through one shared-controller invocation,
including per-attempt deadlines, an untried observed leader fallback and physical
SQL/transport retention. Both image forms pass the focused routing cases and the
complete execution suite. This component result does not qualify the public
SDK consumers or managed orchestration.

Groups added after startup now retain node-wide archive pools and independent
verifier resources. Dynamic RF3 S3 writes, refusal of missing archive identity,
explicit initialization/retry and joined close with a held archive GET pass local
managed cases in both image forms. Managed S3 learner activation now has a
data-plane implementation: fresh quorum capture, a journaled protected upload,
independent destination verification and the existing physical install fences.
Source hints may change after leader failure while destination boot/member/permit
and transfer mode remain fixed. Normal archived replacement/maintenance and the
S3 refusal/failover/debt matrix pass scoped native and real-RustFS cases. The
combined native run still has one checkpoint TCP retry failure; bounded failover
readiness and earlier maintenance failures remain unresolved despite later passes.
Automatic abandoned-seed journal cleanup and the complete public
lifecycle gate remain required. See the latest [progress checkpoint](progress.md).

CG-07 remains IN_PROGRESS. The native_cell_groups_public target now passes its
sdk_many_cells_share_group case through public startup, native/Axum ingress and
retained process restart. The other four original public acceptance cases remain
required and have not been delivered.
CG-09–16 remain TODO. Historical local Docker/RustFS recovery and effects results
do not qualify the latest combined SDK/fleet source, independent hosts, sustained
load or a release. No production legacy/shared switch or old-format migration is
part of the target.

## P0: finish the MVP and its merge gates

The public group-control prerequisite now has an implementation:
`RaftGroupHandle::observe` samples actual physical log/owner metadata and cell
occupancy; `archive_status` keeps publication separate from coverage; guarded
`transfer_leadership` preserves the caller's term/revision and checks the original
deadline at owner execution. The counter inventory consumes the observation API.
The performance/fleet consumer port now uses shared layout, replicated enrollment,
logical versions and physical witnesses locally; its full native RF3 gate remains
failing. The owned-workspace checkpoint passes occupancy 32 in the matrix
and isolated diagnostic case, including the original explicit S3 assertions.
The matrix reaches occupancy 128 and refuses enrollment after 58 cells under
the original 60-second setup budget and five-second per-call cap. The full suite
has eighteen passes and one failure. Earlier 28/31/44-cell enrollment and explicit
S3 failures remain preserved; this result establishes no fixed cell-count limit
or cause for the remaining refusal. Workspace creation now syncs its child and
owned parent rather than repeating the open store's ancestor barriers; rooted
private opens retain that exact directory owner. Disposable read-cache directories
use ordinary creation. Child/parent failures retain their charge and quarantine.
Preserved probes place more time in physical apply preparation than publication
or collection in that sample. Measure image/page preparation and sync costs next
without raising bounds or weakening the applied-equals-committed enrollment fence.
The node-owned live extent collector now has focused shutdown and RF3 pressure
coverage, including the independent archive-verifier store. It keeps existing
control admission through physical completion and preserves reader/manifest pins.
That does not supply manifest-page collection or sustained-load qualification.
Warm metadata barriers and a bounded verifier cache reduce repeated physical
work without supplying archive proof; incoming extent bytes remain independently
checked even when their destination is cached. Finish repeated physical
preparation, checkpoint work and cross-group interference, live manifest-page
ownership, then finish the whole-group disaster operator. Retain
the original HTTP/read/retry options, workload ledger and independent oracles;
report measured physical group counts separately from logical cell counts.

Public recovered-node startup now has its owned activation path: an explicit
fencing authority plus exact retained seed/engine pins, actual fresh quorum
activation, catalog completion and joined shutdown. The public Node drill restores original S3 receipts/state after all old voter
directories are erased, then verifies new writes and retained restart in both
image forms, including the recorded real-RustFS run. Its three embedded Nodes
and contract fence do not qualify the Docker permanent launch-fencing drill,
separate voter processes/hosts or multi-group load.
Use this API in the executable whole-group recovery port next. Preserve complete
archive cuts before fencing, use disjoint replacement member/node identities,
and sequence each group against the current catalog root through activation;
preplanning every group against one stale root is invalid. Keep partial recovered
inventory explicit and fail closed for unseeded groups.

| Order | Deliverable | Required result |
| --- | --- | --- |
| 1 | Managed S3 learner activation and archive holds (CG-06/07) | Expand the scoped native/RustFS results to the full failure/cancellation and fenced failed-voter lifecycle. Preserve complete manifest/image/receipt closure, destination boot/catalog checks, physical install, catch-up and fresh quorum before promotion. Complete automatic discovery/reconciliation of abandoned seed journals and safe hold retirement during outages and restart. A surviving original voter still needs a live physical-inventory proof for catalog settlement; its outage must retain the debit. Finish orchestration for that retained debt and qualify stop/offline ownership; a seed or remote object supplies no activation authority. |
| 2 | Public SDK surface and executable consumers (CG-07) | Port the original daemon/native/Axum fixtures and performance/fleet runners to namespace capabilities, physical group assignment and replicated cell enrollment. The local all-binary check clears the original 27 compilation errors; qualify the final combined source. Clear the remaining 128-cell enrollment deadline, qualify S3 at every planned occupancy, finish live manifest-page ownership and qualify owned extent collection under the original workload bounds. Retain every original HTTP/read/retry option, exact term/revision transfer guard, actual responder metadata and all five original public gates. Incorporate the implemented cold birth, repair, maintenance stop/return/resume and physical debt into the complete public lifecycle case, including retained nonvoting inventory and failure/cancellation ownership. |
| 3 | Public, build and backend qualification (CG-07 and MVP portions of CG-14/15/16) | Diagnose the current checkpoint TCP retry `Overloaded`, intermittent five-second successor-readiness timeout and earlier eight-turn maintenance failure; repeats cannot waive failed runs. Deliver the four remaining public cases below while retaining the SDK case; port the old inspection cancellation oracle to the managed shared protocol, and clear strict lint failures without suppressions. Review stale plan anchors and reconcile generated index/Purpose drift. Run affected SDK CI feature modes. Extend current-source real RustFS component recovery to constrained independent voters and replication/reclamation under bounded mixed load, retaining exact binaries, ledgers, failures and resource/latency measurements. |

The public counter and first SDK gate are qualified at their recorded local
boundary. Managed boot/directory/stop ownership is implemented at its recorded
component boundary. Cold inventory/capacity and nonempty initial-group startup
pass the recorded local gate. Peer replacement, actual handoff and retained
joined-member restart pass the focused managed case. Basic catalog maintenance
stop/return/resume now passes its local role/image matrix. Original consumers and
public acceptance, lifecycle failure/cancellation, complete S3 qualification and backend
qualification still block the complete MVP. A successful metadata-only standby boot supplies no
complete-inventory or capacity proof; an Active-node fixture cannot replace the
original public lifecycle gate. The currently refused managed paths remain
closed until actual authorization, permanent claims, resource accounting and joined service ownership
are wired; removing guards or dropping consumer options is not a cutover.

Public replica routing belongs in one controller-owned invocation. Local and
remote attempts must retain the same routing session and admission; a timed-out
physical read remains owned while another permitted attempt runs. Incoming RPCs
execute locally rather than recursively forwarding. Peer discovery and leader
hints choose a destination but never supply authority. Do not implement retries
by creating fresh independent deadlines or attempt banks.

The original public merge gates are:

- `sdk_many_cells_share_group`: public startup/handles, independent SQLite cells,
  one configured group and all replicas; observe actual group/RawNode counts.
- `managed_group_lifecycle`: repair, drain, stop/return/resume and capacity move
  complete groups while preserving cell counts and physical debt.
- `ingress_cannot_cross_cell_authority`: native/Axum/admin tenant, cell and group
  binding refusals, with immutable command identity across retries.
- `occupancy_one_uses_shared_path`: one-cell and multi-cell configurations use
  the same host, manifest, snapshot and archive implementation.
- `feature_off_fleet_contract_unchanged`: existing non-Raft ownership/durability
  modes pass their actual regression matrix.

Run the original counter process/restart drill after its port, including late
startup failure, both HTTP hosts, remote minimum-version/linearizable reads,
permission and cross-cell token refusal, exact retained receipts and joined close.
Do not delete options or weaken assertions to make consumers compile.

The RustFS recovery gate deletes every old voter disk behind the accepted
fencing-authority/Docker contract, then restores a new incarnation from complete
S3 evidence. Verify restored images/receipts/client floors, original S3 minima,
permanent old-launch/process refusal, fresh quorum activation, new writes and
retained restart in both image forms. Under load, verify archive coverage before
local reclamation and account for accepted, refused and ambiguous outcomes.
Local loopback qualification must remain labeled as such.

## Default backend, capabilities and deployment

Quorum is already the command API default, and the namespace declaration
already defaults to quorum minimum durability and three voters. Those defaults
do not select the shared-group backend for newly created cells. Complete the
namespace factory, SDK, CLI, daemon, native/Axum/admin ingress and examples as
one public cutover. Missing quorum must refuse admission without selecting a
weaker durability mode. The intended deployment contract is three-replica shared
groups, quorum writes and asynchronous S3 archives, with measured archive lag/RPO.
An explicit S3 write still requires its separate complete coverage proof.

Resolve the default backend's capability matrix before enabling it broadly.
Workflow/Queue callbacks, WebSockets and mutable-container behavior need equivalent
adapters or an explicit supported product scope; the native SQL counter cases
do not establish their coverage. Preserve capability refusals while these remain
unimplemented. Existing Fleet data requires a separate deployment decision:
this plan supplies no online Fleet-to-Raft conversion and must not reinterpret
old data as a new Raft history.

Read consistency remains explicit. Queries currently default to stale reads;
quorum writes do not make those queries linearizable. Complete and qualify the
chosen public read policy independently of changing the default storage backend.

## P1: complete the requested architecture

These are required follow-ups, not removed scope or optional substitutes for the
full hard cutover.

| Task | Deliverable and dependency |
| --- | --- |
| CG-09 | Irreversible source sealing and whole-cell transfer, with cancellation/seal serialization, quorum installation and atomic range publication. Preserve cell histories, receipts, effect IDs and remote recovery closure. |
| CG-10 | Split hot groups and merge compatible adjacent ranges using CG-09; recover partially completed transitions without reopening a sealed source. |
| CG-11 | Group placement, failure-domain/reserve accounting, load observations, hysteresis and hot-cell isolation over the same transition/membership owners. |
| CG-12 | Fenced group parking and authority-checked wakeup. Resume actual membership/quorum under the original request deadline; measure cold response latency rather than assuming immediate readiness. |
| CG-13 | Managed S3 recovery of complete groups and incomplete transfers/splits/merges, including zero-quorum fencing, fresh incarnations and unfinished lifecycle journals. |
| CG-14 | Reproducible process/RustFS harnesses with real independent cells/groups, source/binary/profile hashes, owned resources and complete outcome ledgers. |
| CG-15 | Full load/crash, transition, wakeup and disaster-recovery matrix. Measure the documented profiles and failure schedules; no capacity or TiKV performance-parity claim follows from correctness tests. |
| CG-16 | Delete remaining predecessor consumers/codecs, finish feature-off/full release checks, publish the unified API/runbooks and qualified operating envelope. No one-cell special path, compatibility reader or production mode toggle. |

The next functional slice is archived managed replacement/maintenance and exact
S3 activation, including stop/offline ownership and archive hold retirement,
followed by public lifecycle/default-backend
integration and complete MVP public/backend gates, then
transition/topology/wake features and the final managed recovery/load/release matrix. Passing one layer
does not mark another layer or the end-to-end goal complete.
