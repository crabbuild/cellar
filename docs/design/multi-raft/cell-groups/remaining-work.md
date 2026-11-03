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
membership send fences now pass their recorded component cases. Full nonempty managed group
orchestration, capacity/inventory bootstrap and the complete public gate remain
unqualified. Shared follower reads, source-age policies and actual responder metadata advance that cutover. Bounded
public replica preferences now run through one shared-controller invocation,
including per-attempt deadlines, an untried observed leader fallback and physical
SQL/transport retention. Both image forms pass the focused routing cases and the
complete execution suite. This component result does not qualify the public
SDK consumers or managed orchestration.

CG-07 remains IN_PROGRESS. The native_cell_groups_public target now passes its
sdk_many_cells_share_group case through public startup, native/Axum ingress and
retained process restart. The other four original public acceptance cases remain
required and have not been delivered.
CG-09–16 remain TODO. Historical local Docker/RustFS recovery and effects results
do not qualify the latest combined SDK/fleet source, independent hosts, sustained
load or a release. No production legacy/shared switch or old-format migration is
part of the target.

## P0: finish the MVP and its merge gates

| Order | Deliverable | Required result |
| --- | --- | --- |
| 1 | Managed whole-group lifecycle and cold inventory bootstrap (CG-07) | Qualify the implemented boot/directory/Node stop owners with nonempty groups. Establish executable fresh-standby → range/inventory → capacity → managed birth ordering; then qualify ownership discovery, repair, drain, maintenance, stop/return/resume and accounting as complete groups with separate cell/image occupancy. Reuse CAS journals, permanent claims, exact boot fencing and physical disruption debt. Join entered work on failure/cancellation before releasing resources. |
| 2 | Public SDK surface and executable consumers (CG-07) | Retain the implemented public counter, explicit namespace/archive provisioning and typed refusal/identity contracts. Port the original daemon/native/Axum fixtures and performance/fleet runners to namespace capabilities, physical group assignment and replicated cell enrollment. Retain every original HTTP/read/retry behavior and actual responder metadata. Exercise the implemented controller routing with one original deadline, invocation, input, token and global attempt budget; preserve namespace/process grants and fresh destination admission. |
| 3 | Managed S3 learner activation and archive holds (CG-06/07) | Implement the currently refused exact-reference S3 path without peer fallback. Verify and retain the complete manifest/image/receipt closure; preserve local owner and uncertain outcomes through actual sync. Reauthorize the destination boot and catalog, attach the actual installed member, catch up and obtain fresh quorum before promotion/success. Complete safe archive-hold retirement. A seed receipt or S3 object alone is not activation authority. |
| 4 | Public, build and backend qualification (CG-07 and MVP portions of CG-14/15/16) | Deliver the four remaining public cases below while retaining the SDK case; resolve public binary compilation, port the old inspection cancellation oracle to the managed shared protocol, and clear strict lint failures without suppressions. Run affected feature modes from SDK CI. Refresh same-source real RustFS recovery and replication/reclamation under bounded mixed load, retaining the exact binaries, ledgers, failures and resource/latency measurements. |

The public counter and first SDK gate are qualified at their recorded local
boundary. Managed boot/directory/stop ownership is implemented at its recorded
component boundary. Full nonempty managed orchestration and cold inventory/capacity
bootstrap still block fleet and performance qualification, so they precede the
dependent consumer ports. A successful metadata-only standby boot supplies no
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

The remaining execution sequence is managed startup and dependent public
consumer ports, S3 activation, the complete MVP public/backend gates, then transition/topology/wake
features and the final managed recovery/load/release matrix. Passing one layer
does not mark another layer or the end-to-end goal complete.
