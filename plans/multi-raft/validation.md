<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Package validation

This historical record covers the initial design package and planning checks.
Current implementation evidence is recorded per task, starting with
[MR-01](evidence/MR-01.json).
Runtime implementation, Raft safety and fleet performance remain work-package
deliverables. At this initial validation point, all tasks were TODO.

Validated on 2026-10-19 against source revision
`0efd8f4d871199542f97f9fe0b74086a38606d02`.

| Check | Observed result |
| --- | --- |
| `python3 plans/multi-raft/plan.py validate` | Passed: 17 tasks, 104 proposed runtime cases, 51 source anchors and package-local links |
| `python3 plans/multi-raft/plan.py preflight` | Passed: all 51 source hashes match |
| `python3 plans/multi-raft/plan.py ready` | MR-01 is the only initially ready task |
| `python3 plans/multi-raft/plan.py capacity million-cells` | 3,000 replicas/node, 1,000 leaders/node and 2,000,000 logical heartbeat deliveries/second under the stated assumptions |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s plans/multi-raft -p 'test_*.py' -v` | All 17 checker tests passed |
| `python3 tools/docs/check.py` | Passed: 36 documents, 6 crate READMEs, 10 visual guides, 6 flows and 616 local references |
| `git diff --check` plus a scan of every new package file | No trailing whitespace or missing final newlines |

The checker tests exercise invalid dependencies, duplicate identities, unknown
invariants, unsafe paths, missing evidence, empty test inventories, broken links,
source drift and capacity assumptions. They exercise planning-tool behavior,
not a Raft implementation. The capacity output is arithmetic, not a benchmark.

At that initial validation point, no Rust runtime code or dependencies changed. Rust compilation, the 104 proposed
runtime cases, fault qualification and scale benchmarks have not been run.
This initial result did not establish completion of any implementation package.
Consult tasks.json and the per-task evidence for subsequent implementation work.


MR-04 transport implementation and scoped qualification are recorded in
[MR-04 evidence](evidence/MR-04.json), pinned to source
`24261b84fe9c9b71e18b1e521fb821bd14c0c8d1`. Nine transport cases, eight frame/lifecycle
cases, the embedded-node service case and 37 core regressions passed on the final
Rust source. The broader 88-case node run also passed; its relationship to the
final transport-only queue refinement is recorded in the evidence. Three real
child peer hosts exercise TLS, two groups, independent leaders, one-way loss,
duplication, reordering and delayed Raft responses. These checks do not qualify
SQLite/LTX, S3, end-to-end SDK serving, production performance or capacity.


MR-05 verified image implementation and scoped qualification are recorded in
[MR-05 evidence](evidence/MR-05.json), pinned to source
`fd82ca8deec8c8c56ac9a0e527af5a10956bbc9c`. All 15 image cases and 46 contract, Raft and
existing replication/storage regressions passed. Four SIGKILL cuts exercise
image publication and selection through a fixture marker; this is not yet
qualification of engine apply markers. Actual SQLite queries use injected
readers, retain immutable generations and refuse mutations. LTX core/all-feature
builds, Clippy, formatting, layout, dependency policy, documentation and all 17
planning-tool tests passed. Command/SDK serving, S3 and scale remain pending.


Partial MR-06 image/apply integration is recorded in
[MR-06 apply evidence](evidence/MR-06-apply-progress.json), pinned to source
`c2266dc1818bbd80f653674db93477078cf0e79b`. Four integration cases and 61 regressions passed.
Four SIGKILL cuts cover the actual engine marker and verified images, including
after marker fsync but before public cache completion. Clippy and repository
checks passed. This does not complete MR-06: private command/capture production,
receipts and six named command cases remain required.


MR-06 private command implementation and component qualification are recorded in
[MR-06 evidence](evidence/MR-06.json), pinned to source
`3b526e7bb54e5b40cda314db9341c92298aa3107`. All 14 command/apply cases, including the seven
required gates, passed. Real WAL capture, a multi-file checkpoint cut, durable
receipts, expiry floors and nine SIGKILL cuts are covered. All 61 regressions
passed on the same production code; final owning tests and Clippy also include
the subsequent test-only checkpoint/expiry assertions. Formatting, layout,
dependency policy, documentation and all 17 planning-tool cases passed. This is
not yet SDK/node command serving: MR-07 must bind authorization, live authority,
output gates and cancellation/eviction ownership. S3, repair and scale remain
pending.


Partial MR-07 catalog/bootstrap work is recorded in
[MR-07 catalog evidence](evidence/MR-07-catalog-progress.json), pinned to source
`cff6a84ee446d5538f519eed5cea243be7931ea9`. Seven catalog cases passed, along with 82
regressions and the owning embedded-node lifecycle case. The record identifies
the subsequent catalog-only refinement and final owning reruns. Conditional
creation, lost responses, conflicting modes/identities, missing stable state and
parked voting admission are covered. Public SDK/actor/ingress integration and
five named MR-07 gates remain outstanding; MR-07 is still IN_PROGRESS.


Partial MR-07 registration/execution/lifecycle work is recorded in
[MR-07 registration evidence](evidence/MR-07-registration-progress.json), pinned to
`78dd39ab2b98d19672e9cf7b2b48e9fdc810ff96`. All 34 owning cases pass (9 host,
15 actual command/apply integration and 10 pure lifecycle cases). Restricted
registration, normalized request binding, strict read-only queries, exact
receipt gates, cancellation ownership and prior-term retries are covered at
their component boundaries. The 164-case broader run includes all 88 embedded
node cases; 14 additional existing pure-core regressions also pass. The evidence
records the final schema-check refinement and owning reruns. Clippy and repository
checks pass. This does not qualify the missing node-owned dispatcher or public
handles/ingress: four planned host cases remain absent and MR-07 remains
IN_PROGRESS. S3 archive/restore and full end-to-end serving remain pending.


Partial MR-07 node-host serving is recorded in
[host evidence](evidence/MR-07-host-progress.json), pinned to
`67c9f003977594eb58aa2d5e7ce7d866724d02a6`. All 44 owning cases pass: 17 host,
16 command/apply and 11 pure lifecycle cases. All six planned host case names
are present. Actual node-owned SDK commands and committed-image queries run
through native HTTP and Axum; two independent groups replicate across three
embedded nodes. Ten consecutive failover runs pass. The executable drill checks
all six endpoints, joined process shutdown, retained receipts and new writes
after restart. Cancellation, unpolled expiry and directory-join ownership have
explicit tests. Readiness deferrals retain frozen work under the original
deadline rather than rerunning business callbacks.

Axum default/all-feature suites pass 27/33 cases including their compile doctest;
21 existing Fleet/actor/runtime/storage regressions also pass. Those broader runs
precede the last Raft host readiness and example startup fixes; final owning
cases and Clippy were rerun afterward, as recorded in the evidence. Formatting,
layout, dependency policy, documentation, the 17 planning-tool cases and source
preflight pass. The baseline has 68 anchors, including the Axum integration.
MR-07 stays IN_PROGRESS pending its complete mode-isolation and submitted-node
transition audit. Strong query consistency, S3 archive/restore, snapshot install,
membership repair and later delivery/scaling qualification remain pending.

MR-07 is complete at source `b52a299656c2b565980d23c9b46675e5aaba3b72`; see
[final evidence](evidence/MR-07.json) and the [host audit](host-audit.md). All 47
owning cases pass (20 host, 16 command/apply, 11 pure core), including all six
planned host cases. Five repeated TLS partition runs each cover an overwritten
submission and an unacknowledged entry retained/committed by the healthy pair.
Callback counts, persisted/committed observations, visible values and old-node
retirement distinguish the outcomes. Live startup refuses mode, mutable-factory
and required-command-manifest conflicts before catalog/log admission.

Both Axum feature suites pass 27/33 cases including their compile doctest;
21 existing Fleet/actor/runtime/storage regressions pass. Those broader runs
precede only the example startup-cleanup refinement. Final Clippy, owning and
exact host commands, formatting and layout include that refinement. The actual
executable first fails late startup with an occupied final listener, resumes
the same disks and then verifies six endpoints, joined shutdown, retained
receipts and new writes across restart. This remains the MR-07 boundary;
read consistency, S3 archive/restore and the other original packages remain open.

Partial MR-08 source `961c006280a90517b8206d3d403ecf3559152a50` implements local
Stale/AtLeast/safe ReadIndex queries and authenticated opaque versions; see
[local evidence](evidence/MR-08-local-progress.json). All 71 owning cases pass:
10 read, 20 host, 16 command/apply, 14 runtime and 11 pure lifecycle cases.
Three process helper entry points remain ignored and are excluded from that
count. Axum feature suites pass 27/33 cases including their compile doctest.
Clippy with warnings denied, formatting, layout and dependency policy pass.

The initial healed-former-leader read timed out because RawNode can drop a
ReadIndex across role/term changes. The controller now checks readiness, retries
after a checked role change under the original deadline and correlates status
and submission generations. Three repeated election cases and three repeated
minority cases pass. The real counter executable passes late startup failure,
disk reuse, six endpoints, joined shutdown, retained receipts and writes after
restart with the new key configuration. Completed unpolled response payloads
retain byte admission; consuming or dropping a response releases capacity.

MR-08 remains IN_PROGRESS. Bounded network replica selection, explicit leader
fallback, its required case and the remaining read-contract audit are still
required. Counter HTTP does not yet expose stronger query options/tokens.
S3 cell archives/restores, snapshots, membership repair, effects, operations,
release/scaling and the remaining original packages stay in scope. These
single-machine embedded-node fixtures establish no production capacity claim.

Partial MR-08 source `4c0916dc5fec5259213ed19de0f91a3c30abb39f` adds a separately
owned authenticated TLS query plane, bounded member attempts and explicit
leader fallback; see [network evidence](evidence/MR-08-network-progress.json)
and the [read audit](read-audit.md). Current-source checks pass 83 owning cases
(13 read, 20 host, 16 execution, 14 runtime, 9 transport, 11 core), 12 Raft unit
cases and the focused Node query-listener join/rebind case. Four ignored owning
helper entry points are excluded. Axum feature suites pass 27/33 cases including
their compile doctest. Clippy, formatting, layout and dependency checks pass.

Three repetitions each pass same-consistency/deadline fallback, independent LTX
catch-up during a waiting RPC and forwarding from a voter without query code.
The real counter process drill passes late startup failure, disk reuse, six
endpoints, joined shutdown and retained receipts/new writes after restart with
the added query listener. A preliminary 88-case Node run passed before the final
RPC forwarding/gateway guards and test refinements; it is retained separately
and is not claimed as full Node qualification at this source revision.

All seven named MR-08 cases are now present, but MR-08 remains IN_PROGRESS.
Active captured ordinary Context/Storage, counter HTTP query/token options,
broader RPC fault cases and directory/ingress routing outside registered group
membership still require implementation/audit. All later original packages,
including S3 archive/restore and complete scaling/release qualification, remain
open. These fixtures make no production-capacity claim.


## MR-09 committed archive and SDK durability boundary

Source `2298fd442f0b867695b564527a66b61684aff2c2` completes MR-09 with
[exact evidence](evidence/MR-09.json) and an [archive audit](archive-audit.md).
All seven required cases are implemented and pass in the 26-case owning target.
Native SDK tests restore acknowledged state and exact receipt/minimum-durability
rows from object storage after deleting source storage, strengthen retries
without business reexecution, resume after restart, bound outage admission,
retain recovery material and reject late success for an unpolled expired waiter.
Real three-voter TLS isolation/healing tests prove minority refusal and restored
committed state after leader replacement. Image pressure defers a committed
apply while preserving its runtime admission, then resumes without corruption.
Shutdown joins hung I/O and releases the workspace before reporting completion.

The full owning run passes 251 Rust cases: 26 archive, 6 contracts, 16 execution,
22 host, 16 LTX, 17 reads, 14 runtime, 10 storage, 9 transport, 11 pure lifecycle,
16 Raft library and 88 Node cases. Six ignored process entry points are invoked
by their owning parents and excluded. Axum feature runs pass 27/33 including the
compile doctest; the actual counter executable passes six-endpoint remote reads,
late startup cleanup and retained-receipt restart. All 23 recorded commands pass,
including Clippy in affected feature modes, format/layout, dependency policy,
docs, 17 plan tests and reviewed source preflight. The two changed baseline
files are the image guide and bucket conditional-store adapter; their reviewed
hashes and baseline revision now match this source.

This milestone does not qualify physical S3, power loss, zones, production
capacity/performance, atomic member snapshot installation, automatic repair,
committed effects or a full release. MR-10 through MR-17 retain their original
scope; the full goal remains unachieved.


## MR-10 physical snapshot progress

The [physical snapshot record](evidence/MR-10-physical-progress.json) pins source
`24410fb754b055078255ccf5d595c0893f0d1b0e`. All 218 Rust cases pass: nine snapshot,
26 archive, 16 execution, 22 host, 16 LTX, 17 reads, 14 runtime, 10 storage and
88 Node. Six ignored process entry points are invoked by their owning parents.
All 13 commands pass, with executable/log digests retained. Clippy denies warnings
for affected targets; format checks 628 Rust files. Layout, dependency policy,
docs, 17 plan tests and the reviewed 68-anchor preflight pass. The one changed
baseline anchor is the LTX guide's independent snapshot/applied reference rule.

New cases use real SQLite/LTX and Raft Engine, actual RawNode tickets, manually
forwarded messages and bounded chunks. They cover coherent physical installation,
wrong history/schema/configuration/position, incomplete/corrupt transfer and name
collision refusal, old readers through GC, compacted boundary terms, durable
committed replay preserving an uncommitted suffix, independent snapshot retention
across newer state/restart and mixed applied/snapshot marker refusal. Seven
process-kill cuts cover image sync/rename/directory sync, engine sync/reply and
publication. Process kill is not power-loss qualification.

MR-10 remains IN_PROGRESS. Source checkpoint fixtures run with publication
quiesced before RawNodes start; native checkpoint scheduling/retention handoff,
empty/configuration-only snapshots, authenticated node-owned transfer and native
host readiness/cancellation integration remain unfinished. Fresh-member S3/peer
seeding and missing-quorum normal-restore refusal still require implementation
and qualification. The [snapshot audit](snapshot-audit.md) records these limits.
This progress does not complete MR-10, a release or the original 17-package goal.


## MR-10 runtime checkpoint and reference progress

The [runtime progress record](evidence/MR-10-runtime-progress.json) pins source
`0260abb94c15ee0375bea0f2508ac40ce1274f65`. All 147 Rust cases pass: 15 snapshot,
17 LTX, 26 archive, 16 execution, 22 host, 17 reads, 14 runtime, 10 storage,
9 transport and the affected public Node Raft lifecycle case. Seven ignored
process children are invoked by owning parents and excluded. All 17 commands
pass with executable/log digests, including Clippy, both cellar-ltx backend feature
modes, format (629 Rust files), layout, dependency policy, docs, 17 plan tests and
the reviewed 68-anchor preflight. The one changed baseline anchor is the LTX
guide's no-I/O monotonic reference contract.

Tests cover runtime-owned checkpoint persistence behind a held sync, an older
capture/newer apply/late retirement/restart, completion loss, failed sync after
joined producer shutdown, reopened-engine material refusal, busy-apply refusal,
empty configuration installation/first LTX/restart and refusal to erase committed
data. Generation reference tests cover no-I/O handoff, stale retains/releases,
64-key admission and complete image identity when a reclaimed name is reused.
Physical incoming completion now carries an opaque exact-ticket/digest reference;
its producer can be dropped after install without losing the durable image.

MR-10 remains IN_PROGRESS. This record does not qualify native automatic
checkpoint scheduling, snapshot RPCs, host transfer/readiness ownership,
replacement-member authorization or missing-quorum recovery refusal. It does not
qualify the largest admitted image: LTX overhead can exceed current encoded/object
limits and must be addressed consistently before enabling native scheduling.
The prior full 88-case Node run remains pinned separately in the physical progress
record; only the affected Raft lifecycle case is rerun here. Physical S3, power
loss, zones, production capacity and the original complete goal remain unqualified.


## MR-10 cancellation progress

The [cancellation record](evidence/MR-10-abort-progress.json) pins source
`c6aaca08153d039717a07195d05a8b0248ac6fe5`: 160 Rust cases across ten targets and all
18 commands pass. Its 22 snapshot cases include exact-ticket cancellation,
same/higher-term voting, joined older physical apply, duplicate/late completion
refusal, retry installation, irreversible install refusal and failed sync/restart
recovery. The final source also handles the new cancellation event explicitly in
the existing runtime fixture. Seven ignored process children are invoked by owning
parents; seven snapshot install process-kill cuts remain covered. Owning Clippy,
cellar-ltx feature builds/Clippy, format/layout, dependency policy, docs, 17 plan
tests and reviewed preflight pass.

At that earlier source this remained component progress: native scheduling,
publication/admission and fresh-member recovery were unfinished. Current native
peer-seed qualification is recorded below. The
[feature milestone order](implementation-plan.md#feature-priorities-and-delivery-milestones)
keeps recovery through public Node first, followed by automatic repair, durable
business features, operations, release qualification and larger scale/federation.
MR-10 and the overall 17-package goal remain open. No physical S3, power-loss,
zones, production capacity or release qualification is established by this record.


## MR-10 native recovery completion

The [MR-10 completion evidence](evidence/MR-10.json) pins source
`879d13e2608e94cf3ef5c92d4f3e05029f2a678d`: 317 Rust cases across 14 binaries and all 22 commands pass.
Runtime tests pin `1cd373cf7bc994e8ae1890f3934d977ea8b7c7af`; the final source
adds only three shared-test-helper lint annotations. The exact delta, initial
failed lint output and per-command source revisions are retained; production
code is unchanged. Clippy and remaining repository checks pass at the final
source. This includes 42 snapshot cases, 29 archive, all 88 Node cases, all 16 domain
cases, both executor lifecycle cases, six protocol contract and 22 library cases,
plus the owning Raft regressions. Clippy, both cellar-ltx backend modes, format
(640 Rust files), layout, dependency policy, docs, 17 planning tests, all 32 required
snapshot names and the reviewed 71-anchor preflight pass. Eight ignored child
entry points run through owning parents and are excluded from pass totals.

Native peer and exact-reference S3 learner seeding require a fresh safe ReadIndex.
Images seal before atomic group/configuration/binding/applied initialization and
permanent-claim reconciliation. S3 restores cell images and committed LTX tails;
it restores no old votes, process identity, leadership or quorum. Missing selected
payloads refuse initialization even when a newer complete head exists. A busy
private writer does not block S3 metadata selection or expose speculative state.

Public joined-node restart preserves verified state and vote zero, serves retained
causal reads before voters start, and refuses strong reads until quorum returns.
Three claim faults reconcile exact synced state without a blank reset. Sixteen
fresh-learner process kills cover eight cuts each for peer and S3 seeding: image
fsync, rename, directory fsync, engine sync before/after, engine reply, permanent
claim and public attachment. Seven existing-replica snapshot cuts remain covered.
Every fresh-seed restart verifies all-or-absent group state and claim ordering,
then resumes follower reads and subsequent replication without callback replay.

MR-10 is complete at the native recovery boundary. MR-11 through MR-17 and the
full delivery goal remain open. That MR-10 source uses protocol identity
`cellar-raft-ltx-1/members-1/seeds-1/s3-1`, with numeric markers 1; use a fresh
experimental namespace/prefix and local state. Ordinary stores are in-memory;
process-kill fixtures checkpoint successful conditional mutations and rehydrate
them after child exit, then copy exact node file bytes into exclusively owned
restart directories. These checks establish process-crash recovery, without
physical S3, power-loss, zones, production capacity, larger-scale, federation or
complete release qualification.


## MR-11 repair-plan foundation

The [MR-11 plan foundation](evidence/MR-11-plans-progress.json) pins source
`8d4c5cdd594c8e54629b10b383522261aabca078`: 231 Rust cases and all 25 checks pass.
The 13 membership cases include nine pure placement contracts and four real-Node
repair cases. Six ledger unit cases cover bounded persisted plans and phase
validation; the owning regressions include 42 snapshot, 29 archive, 28 library
and the affected public Node Raft lifecycle case. Format checks 645 Rust files;
Clippy, both cellar-ltx modes, layout, dependency policy, docs, 17 planning tests
and 71-anchor preflight pass. Eight ignored child entry points execute through
owning parents and are excluded.

Public begin/advance/status APIs persist one immutable repair plan, admit and
seed a learner, check actual matched progress, enter explicit joint consensus,
finish under both majorities, or abort while still an unpromoted learner.
Native cases refuse empty promotion and minority reconfiguration, reconcile a
lost/unpolled response and stale/rebound plans, then resume joint state through
public Node restart. An incoming majority without the old outgoing majority
cannot finish or serve a fresh strong read. Later replication resumes without
replaying business callbacks during recovery.

MR-11 remains IN_PROGRESS. The partial inventory reports five of seven required
names; `automatic_no_traffic_repair` and `leader_balance_with_lag` remain absent.
Automatic scheduling, dynamic standby activation, trusted process-view
distribution and the six-node repair gate remain required. The full inventory
checker is unchanged. This is explicit administrative repair, without automatic
redundancy repair or complete milestone qualification.

The current internal identity is
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1`; numeric markers remain 1.
Use fresh namespace identities, storage prefix and local state. Current Node
coverage is the affected Raft lifecycle case; MR-10 retains its separate
all-88-Node historical qualification. No physical S3, power-loss, zone, production
capacity, larger-scale, federation or complete release qualification is claimed.
