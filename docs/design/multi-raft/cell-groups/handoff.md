<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation handoff

The executable design and implementation plan is delivered. This handoff scopes
the remaining end-to-end implementation and hard cutover, including the full task
and release gates, for subsequent execution.
This handoff does not claim completion of the
shared runtime, a production cutover or measured TiKV
performance parity. The target is the same partitioned multi-Raft pattern: many
whole cells per group, one Raft machine per group replica, and many groups sharing
node resources. A group spans voter nodes; it is not all cells on one node.

## Starting point

Use branch `codex/shared-cell-groups` and the existing
[PR 160](https://github.com/crabbuild/cellar/pull/160).
CG-04 is now DONE at `69e41e86` after its complete scoped acceptance gate:
83 individual exact-case passes, all required commands, and the supporting 238-case
regression gate. `.cellar/cell-groups-evidence/CG-04-qualified/checkpoint.json`
retains 98 distinct command records and 288 source hashes. The plan runner
validates the evidence and now selects CG-05 as the next eligible task. Preserve
all acceptance cases; the task-boundary audit removed no requirements or checks.

CG-05 is now IN_PROGRESS. Its native source captures an exact selected complete
root, configuration and membership, and retains an admitted immutable closure
through bounded streaming and cancellation. Both image forms and empty enrollment
are represented. See the latest [progress](progress.md) for source evidence and
limits. Receiver verification and atomic storage installation now pass their
partial gate, including cold recovery of the current and older checkpoint roots.
Exact Raft snapshot acceptance and persistence now pass their owner/runtime gate.
Local checkpoint publication and native Raft-generated catch-up now pass their
partial gate. Authenticated checkpoint streams now pass their partial transport gate.
Automatic entry-count checkpointing at controller apply boundaries now passes its
partial gate. Durable whole-group membership and leadership handoff now pass
an additional partial gate. Authenticated fresh-learner installation, catch-up,
promotion and deleted-voter-disk replacement now pass a native partial gate.
Complete the remaining fault/pressure checks and all five CG-05 acceptance cases.
CG-06 follows with archival reclamation. Automatic
production request/voter startup and public SDK/fleet binding belong to CG-07;
effects/cache lifecycle belong to CG-08, and final legacy removal to CG-16. Track
these remaining capabilities in their owning tasks.
Shared RustFS, independent voter failure domains, power loss and load remain unqualified.

The latest pressure checkpoint at `a2036e50` passes all 142 native
shared-execution tests and eight scoped commands. Its record under
`.cellar/cell-groups-evidence/CG-05-checkpoint-pressure/` binds
327 source hashes and one executable. Two new cases transfer 32 cells with
bounded chunks and scratch in both image forms. A held receiver does not prevent
two original voters from advancing other cell versions. Separate item/byte limits
and materialized scratch contention refuse without refunding live owners. Complete
received digests remain at the captured cut and do not publish before installation.

The old source tree becomes reclaimable after transfer release. A unique old cell
image stays readable through its independent physical pin, then is actually deleted
after that reader releases. Production runtime is unchanged. The pressure fixture
uses direct physical streaming in one process; it is not a measured RSS/TLS/load or
RustFS qualification. The complete execution run also retains the earlier seed and
explicit joint-consensus crash coverage. Next finish missing mid-stream and
controller-lifecycle faults and all five exact CG-05 acceptance cases.

The earlier joint-restart checkpoint at `5f30cc6b` passes 16 selected
seed/membership tests and nine scoped commands. Its record under
`.cellar/cell-groups-evidence/CG-05-joint-process-restart/` binds 326 source
hashes and one executable. Four owned SIGKILL scenarios cover replicated explicit
joint entry and exit in both image forms. Three retained replica disks reopen
with new process identities; complete roots, membership revisions and incoming/
outgoing voter and learner sets match before admission. Production TLS/controllers
regain quorum, commit a cell, finish the pending transition, commit another cell
and satisfy linearizable reads for all four cells at unchanged logical sequences.

Production runtime code is unchanged. Shared crash/catalog fixture helpers keep
the seed regressions passing. These are replicated-boundary cuts with all original
replicas in one killed child; every in-flight/automatic-transition crash boundary,
independent domains, power loss, RustFS/load and zero-quorum remain unqualified.
Next address large-group transfer/pin pressure and consolidate all five exact
CG-05 acceptance cases, retaining the remaining mid-stream and lifecycle faults.

The earlier seed-process checkpoint at `b997a48b` passes 11 selected seed tests
and eight scoped commands. The record under
`.cellar/cell-groups-evidence/CG-05-seed-process-crash/` binds 324 source hashes
and one executable. Two parent tests perform eight owned SIGKILL runs across both
image forms: durable stage, before engine sync, after sync and after publication.
Production TLS source/receiver transfer real quorum-authorized seeds. Cold reopen
checks the complete selected/retained root and local receipt; a stage refuses
startup and a recovered learner refuses valid writes without quorum.

The frozen synthetic catalog preserves pre-fault external claims only for the
fixture. It is not S3 durability or a recovery API. All source voters and the
learner share the killed child; independent voter failure domains, power loss,
mid-stream cuts, joint restart, RustFS/load and zero-quorum recovery remain open.
The newer joint-restart checkpoint above covers retained-disk explicit transition
restart/completion. Bounded pressure and the exact CG-05 gate remain open.

The earlier persistence checkpoint at `3ea90fb5` passes 9 selected seed tests
and eight scoped commands. Its record under
`.cellar/cell-groups-evidence/CG-05-seed-persistence/` binds 323 source hashes
and one executable. In both image forms, queued source-process revocation refuses
before the seed fence; revocation after entering the fence retains physical owners
and reconciles the complete seed. Sync errors before/after the underlying sync
close all engine admission and cold recovery verifies the complete root and local
receipt together. Recovered learners still refuse valid proposals without quorum.
Production receiver/storage code was unchanged. These are native one-process
fixtures, not process-kill, power-loss, managed fleet or RustFS qualification.

The newer seed-process checkpoint above covers the staged and engine-boundary
process-kill cases. Joint-transition recovery, mid-stream revocation/crash, signed
minority receipts, pressure and the full CG-05 gate remain open.

The earlier receiver-fault checkpoint at `3db498ea` passes 167 selected Rust
cases across shared execution/catalog/storage and eight gate commands. Its record
under `.cellar/cell-groups-evidence/CG-05-seed-receiver-faults/` binds 322 source
hashes and three executables. Two new cases cover ten malformed reply and eleven
chunk/end scenarios in both image forms, followed by successful same-engine install.
A newer published NodeView revokes an admitted pre-install grant and stale source
hints. Current and retained roots remain unchanged. Both seed and ordinary
checkpoint loops now observe physical verifier failure while network input is
pending; reproduced held-open offset failures no longer retain admission until
timeout. Unchanged legacy snapshot/storage suites were not rerun in this scoped gate.

The newer persistence checkpoint above adds queued/entered revocation and sync
errors. Transfer-time revocation, process-kill outcomes, joint-transition restart,
signed minority receipts, pressure and all five exact CG-05 cases remain open.

The earlier seed-fault/reconnect checkpoint at `de552f6d` passes 238 Rust
cases and eight commands. Its record under
`.cellar/cell-groups-evidence/CG-05-seed-fault-reconnect/` binds 321 source hashes
and five copied executables. Both image forms refuse eleven invalid seed requests,
five post-reply source faults and revoked/expired destination sessions without
minting startup permission. After retained cold recovery, fresh runtime/transport/
controller owners reconnect over TLS with a new process generation, regain quorum
and commit. Isolation cannot advance applied state or satisfy a linearizable read;
after stepdown new proposals refuse. This is one OS process with explicit routes
and clock advancement, not managed fleet or process-kill qualification.

The newer receiver checkpoint above adds reply/chunk faults and pre-install
process revocation. The remaining queued/entered persistence and full-task gaps
are listed above. Source capture fairness and controller pressure remain open.

The earlier learner-install checkpoint at `ab2a308a` passes 238 Rust cases,
eight gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-learner-install-final/` binds 320 source hashes and
five copied executables. Authenticated seed exchange, permanent claims, non-serving
staging, atomic local receipt, joined-member bootstrap and same-owner unknown-result
reconciliation are implemented. Both image forms replace a deleted voter disk,
catch up the post-capture suffix, promote/retire through joint consensus, transfer
leadership and commit new state. Cold recovery checks both retained and current
closures. Held-sync receipt refusal and materialized caller cancellation pass.
The tests caught and fixed noncanonical configuration equality after joint leave.

The newer checkpoint above extends this installation slice with source wire faults,
revoked/expired destination sessions, retained-peer reconnection and minority
commit/read refusal. Preserve the remaining acceptance requirements listed above.

The earlier seed-source checkpoint at `0c3769ec` passes 236 selected Rust
cases, eight gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-seed-source/` binds 312 source hashes and five
copied executables. `seed_authority` uses a fresh group ReadIndex, including for
empty groups, and binds a complete applied root to a committed joining learner.
Capture requires that exact root; the source then tolerates ordinary writes while
revoking on membership/leadership/shutdown/deadline changes. Read and checkpoint
budgets remain owned through physical work. Both image forms, receiver verification,
stale/invalid targets, bounded admission, held-sink revocation, final-chunk expiry,
isolated ready-leader refusal and independent group progress pass local tests.

That source-only checkpoint did not authorize destination startup. The newer
learner-install checkpoint above supplies authenticated exchange, permanent claims,
installation, catch-up/promotion and native deleted-disk replacement. Continuous-write
capture fairness and the full five-case CG-05/RustFS/load/crash gates remain open.

The receiver checkpoint at `cb164e02` passes 226 selected Rust cases,
9 gate commands and 37 validator tests. Its evidence binds 295 source hashes
and 6 copied executables under
`.cellar/cell-groups-evidence/CG-05-checkpoint-receiver/`. Three native cases cover
complete receive/install, both representations and empty cells, held engine sync,
transfer refusal/bounds, later real quorum writes and joined storage reopen.
A missing checkpoint-only page refuses recovery while the current tree remains
complete. A reproduced cached-destination corruption gap is fixed by independent
destination reread/sync. Preserve failed and interrupted diagnostic evidence.

The latest owner/runtime checkpoint at `096cfb13` passes 146 selected Rust
cases, 8 gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-snapshot-owner/` binds 296 source hashes
and 3 copied executables. Explicit `step_checkpoint` admits a verified
closure, binds the exact RawNode Ready to engine persistence, and advances applied
state and acknowledgement only after the matching completion. Matching retained
logs commit/replay without installing received roots. Both image forms, held
sync, invalid/overflow refusal, subsequent quorum writes and joined runtime close
are covered. The tests construct the envelope from a captured source; they do
not qualify Raft-generated outbound snapshots or snapshot RPC.

The latest local-publication checkpoint at `742b1e6d` passes 222 selected
Rust cases, 8 gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-local-checkpoint/` binds 298 source hashes
and 5 copied executables. The owner admits an exact captured current cut,
preserves every later log entry and applied/hard state through publication, and
emits `CheckpointPublished` after its persistence ticket completes. Outbound capture
pins the retained checkpoint named by Raft's exact snapshot, after current apply
has advanced. Tests prove actual RawNode-generated catch-up, suffix replay, cold
leader engine/root recovery and re-stream, plus held publication shutdown in both
image formats. Materialized source recovery uses its fixture filename resolver;
extent recovery and destination replay use production adapters.

The latest transport checkpoint at `ceb7ed31` passes 227 selected Rust cases,
8 gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-snapshot-transport/` binds 306 source hashes
and 5 copied executables. Five TLS cases qualify both physical image forms,
real Raft-generated catch-up and retry after refusal, suffix/cold recovery,
dropped delivery ownership, negative authenticated framing, revocation/replacement
and unrelated-group progress during an admitted receive. Controller registrations
retain exact appliers/routes. Reserved coalesced snapshot feedback survives
ordinary request pressure; transport ownership lasts through feedback admission
or shutdown. The wire acknowledgement remains admission only. See
[progress](progress.md) for exact limits and retained intermediate diagnostics.

The latest membership checkpoint at `1814b278` passes 233 selected Rust
cases, 8 gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-group-membership/` binds 310 source hashes
and 5 copied executables. Route/revision-bound configuration entries atomically
sync membership/configuration and the unchanged complete cell root before live
Raft changes. Learner bindings, promotion checks, explicit/automatic joint changes,
leader handoff, demotion fencing and retryable discovery lag are implemented.
Two real-TCP controller cases cover both image forms; a third holds configuration
sync through continued operation and cancelled/joined shutdown. Automatic joint
leave now replicates immediately without another write or tick.

That earlier membership fixture intentionally did not start learner 4. The newer
learner-install fixture above covers that integration. Metadata reopen still cannot
replace complete physical recovery or serving permission. Birth-member bootstrap
retains its original contract; joined bootstrap requires the new local seed receipt. Preserve all five CG-05 cases, outstanding crash/pressure cuts and
shared RustFS/load qualification. See [progress](progress.md) for exact evidence
and the retained intermediate failures.

The latest controller checkpoint at `c27dfb41` passes 230 selected Rust
cases, 8 gate commands and 37 validator tests. Its record under
`.cellar/cell-groups-evidence/CG-05-controller-checkpoints/` binds 308 source
hashes and 5 copied executables. The controller holds a successor apply ticket,
captures its preceding durable root on a blocking owner, and publishes through
reserved completion capacity. Exact ticket/root validation prevents capture from
racing current apply. The matching persistence event releases successor work;
the runtime admission reply alone cannot. One slot spans this lifecycle. The
transport's shared checkpoint pool and initial endpoint registrations are reused.
Busy admission backs off until a later apply boundary, allowing writes to continue.

The three new cases prove repeated compaction across two groups in both image
forms and progress/recovery while the checkpoint pool is saturated. The policy
retains the final idle tail. Continuous-saturation fairness, exhausted apply and
completion budgets, held sync/cancelled close, checkpoint failure/quarantine capacity
and retained-root pressure remain qualification gaps. The newer learner-install
checkpoint above covers native fresh-disk replacement; receiver crash/sync-error
cuts remain required. All five declared CG-05 acceptance cases remain required. See
[progress](progress.md) for limits and intermediate failures.

The source checkpoint at `b15da415` passes 138 Rust cases, eight gate commands
and 37 validator tests. Transfer
`.cellar/cell-groups-evidence/CG-05-checkpoint-source/` with its predecessors;
its record binds 290 source hashes and three copied executables. The four new
cases cover concurrent real quorum writes while streaming a held cut, both image
representations, empty enrollment, obsolete-page retention/release, bounds,
cancelled physical ownership, stale/foreign/unpublished roots, corruption and
closed engines. Those are the preceding embedded source tests; the receiver slice above extends
storage coverage. At that source checkpoint, snapshot RPC, membership recovery
and new process/RustFS/load qualification were still open.

The following records describe the preceding incremental checkpoints.

The preceding reviewed source baseline is `69e41e86`, adding bounded explicit local
provisioning from already selected initial-member assignments. The controller owns
birth/bootstrap, blocking root recovery and attachment. Exact directory/group
requests coalesce; waiter admission is independent from commands. Failed recovered
owners remain charged and retry without reopening pinned page stores. Joined close
waits for physical work. Registration refusals preserve healthy groups, while
shared engine uncertainty remains node-wide. Five new native cases cover both
image modes, actual application receipts, ownership/capacity refusals, cancelled
physical close and all-voter joined engine/page-store recovery. A missing selected
page refuses without reset; restored bytes plus fresh quorum recover the receipt.
The record `.cellar/cell-groups-evidence/CG-04-local-provisioning-final/checkpoint.json`
binds 238 Rust cases, twelve commands, 288 source inputs and 4 executables.
Transfer this ignored evidence with its predecessors. This incremental checkpoint
preceded the complete CG-04 gate above. Automatic production dispatch/voter
startup and managed fleet authorization remain in CG-07. Membership recovery,
public cutover, shared RustFS, independent-process/power-loss and load remain
unqualified under their later task gates.

The preceding reviewed source baseline is `41121a7d`, adding retained group quarantine.
Group-local failure revokes admission and runtime execution while retaining group
capacity, apply tickets and ambiguous producers. Captured leader authority refuses;
late persistence cannot restore it. Shared engine failures and lost shared SQL
worker replies remain node-wide. The original group cause and quarantine count
are observable; recovery requires joined restart. Two native cases cover both
image modes with actual manifest/engine sync faults. The healthy group completes
controller enrollment/command/exact retry and a linearizable SQL read while the
failed producer remains retained. Capacity refusal and joined cleanup are checked.
The record `.cellar/cell-groups-evidence/CG-04-group-quarantine-final/checkpoint.json`
binds 233 Rust cases, twelve commands, 283 source inputs and 4 executables.
Transfer this ignored directory and its predecessors. CG-04 remains IN_PROGRESS.
Next wire bounded request-driven provisioning into registration, then complete
configuration lifecycle and public host/SDK integration under the plan dependencies.
Shared RustFS disaster recovery and load remain unqualified.
The preceding reviewed source baseline is `4326421a`, adding bounded dynamic group
registration and empty-controller startup. Exact engine/root/applier page ownership
and exclusive transport registration precede existing bootstrap revalidation.
The applier is registered before attachment can emit events; active membership
still requires a successful attachment and serving requires current-term quorum.
Pending groups share capacity with active groups, and queued/unconsumed work uses
the existing item/byte budget. Lost waiters cannot abandon attachment; shutdown
joins it before closing physical services. Failed slots release before response.
Four native cases cover both image modes, live TLS progress during a catalog
stall, ownership/capacity refusals, failed registration retry, cancelled close,
and all-voter joined engine/page-store restart into empty controllers with exact
receipt replay. Image-store handles remain live in one process.
The record `.cellar/cell-groups-evidence/CG-04-dynamic-registration/checkpoint.json`
binds 231 Rust cases, twelve commands, 281 source inputs and 4 executables.
Transfer this ignored evidence directory with previous checkpoints. CG-04 remains
IN_PROGRESS. Next isolate group-local failures without closing healthy groups
(shared engine sync failures still quarantine the engine), then wire
request-driven provisioning/lazy birth into registration. Configuration lifecycle,
public SDK cutover and shared RustFS/load remain unfinished.
The preceding reviewed source baseline is `fbc15053`, adding standalone local and
remote receipt inspection with distinct authenticated purpose and explicit command
permission. Registered contract/digest/identity validation precedes immutable SQL
on a fresh catalog-bound quorum image. Exact retained results and ambiguous
absence carry signed cell observations; lookup never invokes callbacks or retires
another producer. Corrupt schema, expiry and conflict refuse. SQL admission stays
bounded; cancelled queued reads retain physical ownership through completion.
Controller read service references are weak, so closed handles cannot retain the
SQL engine claim. Three native cases cover both image modes; remote cases use
four TLS endpoints in one OS process. Three wire cases and one added SQLite case
cover identity, empty results, absence, capacity and schema loss.
The record at `.cellar/cell-groups-evidence/CG-04-receipt-rpc/checkpoint.json`
binds 227 Rust cases, twelve commands, 279 source inputs and 4 executables.
Transfer the ignored evidence directory with previous checkpoints. CG-04 remains
IN_PROGRESS. Next implement bounded dynamic controller registration/lazy birth,
then isolate per-group failures. Public SDK/discovery and configuration lifecycle
remain open; shared-group RustFS recovery and load remain unqualified.
The preceding reviewed source baseline is `0fe665d0`, adding remote enrollment over
the same bounded write exchange and exact controller registration as commands.
Query, Command and Enrollment have distinct authenticated mandatory purposes.
Explicit namespace/process write grants gate initialization; discovery/read grants
and voter membership do not. Captured creation identity, inputs, preview and
original routing budget survive transport. Receiver fresh catalog/leader checks
precede controller ownership, which survives disconnect, revocation and expiry.
Exact committed creation records survive later mutations and duplicate retries.
Four both-image native cases plus three wire cases qualify these boundaries.
The record at `.cellar/cell-groups-evidence/CG-04-enrollment-rpc/checkpoint.json`
binds 216 Rust cases, eleven commands, 276 source inputs and 4 executables.
Transfer this directory with previous evidence. CG-04 remains IN_PROGRESS. Next
implement standalone safe receipt lookup: validate the registered contract and
request identity, inspect the committed cell image without invoking application
code, and keep absence ambiguous unless the existing reconciliation proof excludes
the original proposal. Public SDK/discovery, dynamic roster and per-group failure
isolation follow. Shared RustFS recovery and load remain unqualified.
The preceding reviewed source baseline is `5fd44437`, adding remote commands over
the bounded TLS application RPC lane. Mandatory authenticated command purpose and
independent namespace/process write grants prevent read/voter authority from
becoming write permission. The exact controller owns accepted work through
response loss or revocation, verifies fresh catalog binding and real leader
execution authority, and returns committed receipts with logical cell tokens.
Three both-image native cases cover nonmember ingress, grant/identity/follower
refusals, duplicates/conflicts, held-SQL disconnect/revocation and cleanup. Wire
and transport tests cover bounded independent grants and authenticated payloads.
The record is `.cellar/cell-groups-evidence/CG-04-command-rpc/checkpoint.json`:
209 Rust cases, eleven commands, 273 source inputs and 4 executables. Transfer
this directory with prior evidence. CG-04 remains IN_PROGRESS. Next implement
remote enrollment and standalone safe receipt lookup, then public-host discovery/
SDK binding, dynamic roster and per-group isolation. Destination caching still
requires replicated transfer seals; shared RustFS/load remains unqualified.
The preceding reviewed source baseline is `06652e36`, adding bounded range hints
shared across query, command and enrollment invocations. Entry/byte reservations,
LRU eviction and verified head invalidation bound retention; a superseded in-flight
lookup cannot refill the cache. Three catalog cases measure real metadata reads;
a native case runs both image modes during a catalog outage, retaining local
quorum authority and fresh remote admission. The record is
`.cellar/cell-groups-evidence/CG-04-dispatch-route-cache-final/checkpoint.json`: 202 Rust
cases, eleven commands, 265 source inputs and 4 executables. Transfer this
directory with prior evidence. CG-04 remains IN_PROGRESS. Next implement explicit
write-authorized command/enrollment RPC and safe receipt lookup, reusing existing
controller ownership. Destination cache removal depends on replicated transfer
seals/admission invalidation; stale hints must not grant serving authority.
Public-host discovery/SDK binding, dynamic roster, per-group failure isolation,
configuration lifecycle and shared RustFS/load qualification remain incomplete.
The preceding reviewed source baseline is `c25f0d97`, adding explicit nonmember
read discovery on the shared query lane. Discovery allocates no local replica or
voting identity; namespace/process grants remain independent. New TLS fixtures
cover both image modes, foreign/missing grants, original-budget leader retries,
held-SQL revocation, stale grant revisions, late replies under replaced discovery,
route/peer bounds and cleanup. The record is
`.cellar/cell-groups-evidence/CG-04-nonmember-queries/checkpoint.json`: 198 Rust cases,
eleven commands, 262 source inputs and 4 executables. Transfer this directory
with prior evidence. CG-04 remains IN_PROGRESS. Next address bounded destination
routing-cache integration, write-authorized mutation/enrollment RPC and safe
receipt lookup. Public-host automatic discovery and SDK binding, dynamic roster,
per-group failure isolation and RustFS/load qualification remain incomplete.
The preceding reviewed source baseline is `da79a696`, adding attached-peer shared
query RPC on the existing TLS lane. Physical group and logical cell identity,
route epoch/birth, actual read authority and signed cell observations remain bound.
Host-owned weak gateway registration checks engine/domain owners; replies retain
admission and cancelled/expired reads retain physical SQL until joined shutdown.
Shared and old-host queries use one read-only grant/revocation guard. The record is
`.cellar/cell-groups-evidence/CG-04-remote-queries-final/checkpoint.json`: 195 Rust cases,
eleven commands, 260 source inputs and 4 executables. Three native cases run
both image modes; three new wire cases and existing transport/host regressions
also pass. Transfer this directory with previous evidence. CG-04 remains
IN_PROGRESS. Nonmember shared discovery, bounded destination route caching and
write-authorized command/enrollment RPC are next integration gaps; the gateway
currently resolves the catalog per RPC. Public SDK cutover, dynamic roster,
per-group failure isolation, configuration/cache lifecycle and RustFS/load remain
unqualified.
The preceding reviewed source baseline is `f77d0d93`, adding registered,
catalog-routed local enrollment to the shared controller. Initializer digests bind
voting schema; old-host validation refuses this unsupported registration. Creation
identity and captured inputs remain owned independently from response lifetime.
Commands and enrollments share admission, response deadlines and physical shutdown.
Exact enrollment records survive subsequent mutations; conflicts refuse without
reinitialization. The partial record is
`.cellar/cell-groups-evidence/CG-04-enrollment-controller-qualified/checkpoint.json`: 90
selected Rust cases, ten commands, 254 source inputs and four executables.
Eight new native cases cover registration and lifecycle/fault boundaries, with
seven running both image modes. Transfer this directory with prior evidence.
CG-04 remains IN_PROGRESS; public SDK/remote RPC integration, lazy birth/dynamic
roster, per-group failure isolation and configuration/cache lifecycle are pending.
The public host has not cut over; shared-group RustFS/load remains unqualified.
The preceding reviewed source baseline is `8c615ac9`, adding bounded command
ownership to the shared node controller. Response disconnect/deadline cannot drop
entered SQL or ambiguous submission ownership. Only never-attempted preparations
can be sealed; exact receipts or newer applied-term proof resolve submitted work.
Received stage errors release completed ownership, while lost replies require
resolution or joined shutdown. The partial record is
`.cellar/cell-groups-evidence/CG-04-command-owner/checkpoint.json`: 83 selected Rust
cases, nine commands, 248 source inputs and three executables. Seven new
native cases each cover both image modes; three unit cases cover submission seals.
Transfer this directory with earlier evidence. CG-04 remains IN_PROGRESS; routed
enrollment, remote dispatch, SDK integration, dynamic roster and per-group failure
isolation remain gaps. The public host has not cut over; RustFS/load is unqualified.
The preceding reviewed source baseline is `ee684c33`, adding a fixed-roster node
controller that drives existing shared TLS/apply owners without fixture pumping.
Its partial record is `.cellar/cell-groups-evidence/CG-04-event-controller-qualified/checkpoint.json`:
58 shared-execution cases, eight commands, 244 source inputs and
one executable. The preceding `61aa7752` gate passed 289 Rust regressions before
Clippy found a duplicated test fixture import; only that test import changed, and
the affected execution suite was rerun. Transfer both gates with prior evidence.
Six new cases
cover two groups on one SQL worker, both image modes, exact ownership, real
manifest sync failure, cancelled close and final-handle cleanup. Commands still
require caller-retained invocations; post-response resolution is not implemented.
Dynamic roster and per-group failure isolation remain gaps. CG-04 stays IN_PROGRESS.
The preceding reviewed source baseline is `eb99a242`, connecting physical groups
to the existing production TLS transport without constructing the legacy runtime.
Its partial record is `.cellar/cell-groups-evidence/CG-04-physical-transport/checkpoint.json`:
192 native cases, 91 library cases, nine commands, 241 source inputs and
eight selected executables. Transfer that directory with prior evidence. The
three new native cases qualify shared peer pools and routed command settlement
over TLS in both image modes. They still use fixture-driven runtime events, not
a production controller after ingress cancellation. Complete-group snapshots
explicitly refuse. CG-04 remains IN_PROGRESS.
The preceding reviewed source baseline is `1f014f42`, adding retained routed
command phases over the existing catalog, consensus and SQL owners. Its partial
record is `.cellar/cell-groups-evidence/CG-04-routed-commands-qualified/checkpoint.json`:
105 native cases, eight commands, 229 source inputs and four selected executables.
Transfer that directory and the retained failed `CG-04-routed-commands` and
`CG-04-routed-commands-final` gates with prior evidence. Cancellation tests retain the invocation; production controller
ownership after ingress cancellation remains unqualified. The command phases
preserve captured inputs across proven-non-commit retry and never equate proposal
acceptance with durability. S3 minima refuse pending real shared archive proof.
CG-04 remains IN_PROGRESS; follow the next execution priorities below.
The preceding source baseline is `8723db2d`, adding catalog-bound query
invocations over the existing runtime/SQL owners. Its partial record is
`.cellar/cell-groups-evidence/CG-04-routed-queries/checkpoint.json`: 103 native
cases across four affected suites, eight successful commands, 227 source inputs
and four selected executables. Transfer the complete directory with prior evidence.
This qualifies local routed queries, including authenticated cell tokens,
unchanged deadlines/attempt budgets and cancellation ownership. It does not
qualify live movement, command/enrollment routing, remote transports or the public
SDK/controller cutover. CG-04 remains IN_PROGRESS.
The preceding source baseline is `7a79511d`, adding registered queries with exact
cell observations on the existing workers. Its partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered-queries/checkpoint.json`: 319 selected
Rust cases, seventeen successful commands, 225 source inputs and sixteen selected
executables. Transfer its entire directory with prior evidence. That checkpoint
preceded the local catalog-routed query integration; CG-04 remains IN_PROGRESS. The
explicit temporary `QueryVersion::GroupCut` and `invoke_group_query` must be
removed with the old host by CG-16.
The preceding baseline is `b28078af`, adding registered commands on shared
SQL workers and logical command/effect identities. Its partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered/checkpoint.json`: 295 selected Rust
cases, seventeen successful commands, 221 source inputs and fifteen selected
executables. Transfer its entire directory with prior evidence. It does not
complete CG-04, qualify shared effect scheduling or implement the public cutover.
The preceding `1ec2eeaa`
checkpoint qualifies selected-image recovery and physical apply fault coverage.
Its retained record is `.cellar/cell-groups-evidence/CG-04-apply-recovery/checkpoint.json`:
260 selected Rust cases, fifteen commands, 205 source inputs and twelve executables.
Transfer that entire directory with prior evidence. It qualifies joined cold
restart, not whole-process kill or power loss. The preceding `c9100ec7` checkpoint
qualifies committed preparation on shared workers (`2085cc96`). The preceding `ed84df5d` checkpoint retains exact
cell-version witnesses in authenticated manifests and query/receipt observations. The preceding read
checkpoint at `3da4481e` decouples pin capacity from transport limits and repairs
the fixture macro. The initial read implementation is `88c4717d`. Earlier checkpoints cover prepared submission sealing
and exact rejection cleanup (`0cf89c40`) and bounded proposal acceptance and
enrollment capacity (`446dd905`). Earlier checkpoints cover shared-worker initialization
and term fencing (`6e09bbc7`, formatted at `82ae6877`), receipt lookup (`91c3ecfc`),
applied-barrier reconciliation (`8a0cf640`), the fixed SQL pool (`51dff0e5`),
the SQL producer (`f807b2f7`) and shared-node runtime (`aec4029e`).
See the latest checkpoint in [progress](progress.md) for qualification and limits.
The branch integrates `origin/main` at `8a93ffd7` (documentation consolidation,
PR 161). The design package now lives in `docs/design/multi-raft/cell-groups/`;
use the commands below. Historical evidence keeps its original tested source,
paths and hashes. Recheck origin before publishing later changes.

The [TiKV investigation](../density-decision.md) pins the supplied checkout at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`. Follow its Region ownership and shared
worker model, adapting storage to independent SQLite/LTX cells. Do not copy KV
transactions, split a cell's SQLite history, or assume TiKV sizing thresholds.

Read [design](design.md), [protocols](protocols.md),
[implementation order](implementation-plan.md) and
[qualification](qualification.md). `tasks.json` is the source for task ownership,
dependencies, steps, exact tests and acceptance oracles. `profiles.json` freezes
candidate workload and performance gates; those numbers are targets, not results.

## Current implementation checkpoint

The CG-04 node runtime is committed in
`crates/cellar/durability/raft/cell_groups/runtime/`. It multiplexes physical owners,
batches engine persistence across groups, retains event admission through apply,
and joins submitted physical work on close. Native cases use independent SQLite
images and actual node workers; see [progress](progress.md) for qualification.
The prior uncommitted runtime draft has been incorporated into this source.

The per-cell SQL producer is now committed at `f3d48bdc`. It adds
`cell_groups/execution_authority.rs`, `execution/cell.rs`, shared receipt
transaction helpers in `execution/command.rs`, and `tests/support/cell_group_sql.rs`,
with changes to physical image capture, owner/runtime authority and native tests.
It separates a selected cell image/version from group progress and requires exact
receipt settlement in both image modes. The partial gate passes at `f807b2f7`:
133 native tests and eight verification commands. It does not complete CG-04.

The fixed SQL pool now accepts cell-history jobs on the existing blocking workers.
Admission, pending proposals and retained outputs survive waiter cancellation;
node/group quotas and an exclusive engine lease bound their ownership. Extent
workspaces are created and closed on those workers. The partial gate passes at
`51dff0e5`: 184 selected Rust tests and ten verification commands. It does not
complete CG-04 or qualify the production dispatcher.

Per-cell reconciliation now runs on the pool's reserved progress path. A verified
current-term applied cell pin can be inspected on a follower. Matching original
receipts resolve from later images; absence requires a strictly newer applied
term before closing the old producer. Foreign engines, wrong proposals, stale
cell pins and an unapplied new-term barrier refuse without releasing ownership.
Missing producers report uncertainty. Production retry dispatch and real
snapshot/compaction integration remain required.

Receipt lookup now uses `receipts::lookup_identity`, preserving identity, floor,
sequence and allocation checks independently from new-client admission. Command
admission retains both ceilings. Native fixtures now cover saturated identity
tables, replicated expiry floors and conflicting identities in both image modes,
including callback suppression and later successful command admission.

Initialization now runs through `CellGroupSqlPool::initialize` and
`CellPrivateEnrollment`, using `CellEnrollmentAuthority` from the real leader.
Logical name admission covers both commands and initialization, including competing
incarnations. `CellEnrollmentState` reads verified current-term applied creation
state on leaders or followers; `resolve_enrollment` preserves the original record,
requires a newer applied term for absent/conflicting old producers, and never
reexecutes callbacks. Prepared outputs carry their term; the guarded submission
path checks it on the actual owner before append, including after that node
regains leadership. Cached acceptance grants no new authority.
The four native cases exercise materialized/extents, concurrent creation, other
cell progress, three cells in one group, retries after mutation, cancelled close,
and absent/conflicting/committed enrollment recovery. All 191 selected Rust cases
and all-target Clippy pass at `6e09bbc7`. The retained partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-initialization/checkpoint.json`; it includes the
initial formatting failure. Formatting-only successor verification is recorded in
`.cellar/cell-groups-evidence/CG-04-initialization-format/gate-commands.json`: all
six checks pass, including formatting, layout, dependencies, docs, preflight and
37 validator tests. These records do not mark CG-04 DONE.

The current admission change counts applied cells and accepted pending creations
against `max_cells_per_group` before append. Pending acceptance is keyed by logical
cell name, binds proposal identity to exact bytes, and returns the original
position on a retry. Conflicting bytes refuse; another proposal for the same
pending name waits. Independent cells remain eligible. Once applied, stale bases
and existing enrollments refuse another append and require receipt/creation
resolution. Acceptance metadata has a separate shared node budget so saturation
cannot prevent Ready or completion work. All 195 selected Rust cases and ten
verification commands pass at `446dd905`; the source-bound checkpoint is
`.cellar/cell-groups-evidence/CG-04-proposal-admission/checkpoint.json`, with 187
source inputs and eight copied executables. See [progress](progress.md) for the
retained intermediate failures and qualification limits. This does not complete
CG-04.

Prepared SQL submission now shares an irreversible decision across clones. Only
an actual owner's definitive pre-append capacity refusal seals the preparation;
a generic queue error leaves its outcome unresolved. The exact seal can be
recovered locally after a lost response, and the worker accepts it only for that
same preparation and engine. It closes the private producer before releasing
name admission. A fresh preparation with the same proposal ID cannot be closed by
the old proof. Accepted preparations retain their original position/digest even
after apply; this remains provisional acceptance, never commitment. Prepared
submission also closes byte-only proposal entry points on that owner.

Next connect catalog routing for SDK creation/commands and the production host
controller to the registered workers. Local catalog-routed query invocations now
consume exact cell-version observations. Use `CellSqlPrepared::submit` and its typed result;
never release a producer from generic overload or same-term absence. Preserve
the original request fingerprint, deadline and attempt budget across redirects. Do not synthesize a legacy `GroupScope` to
satisfy callback/effect identities. The arbitrary-callback `stage` API remains a verification boundary; use
`stage_registered` for compiled commands. Public SDK routing is still pending. Real compaction/production retries
and preview/cache lifecycle remain required. These cases do not complete CG-04.

The remaining coupling is concrete: `execution::CommandAuthority` binds the
old `GroupLog`/`GroupStatus`; `execution::images::Published` exposes `GroupScope`;
and the old `host_sql::Cell` still keys its execution state by that scope. The
new pool path already keys by cell history and exact image pins; replace the
production dispatcher with that path while keeping group authority separate.
Reuse the existing receipt transaction and callback machinery; do not manufacture
a synthetic cell-scoped group to satisfy the old interfaces.
A later mutation of another cell must not settle the first cell's pending output.

The registered command boundary now carries `CellHistory` through
`CommandContext`, inbox authentication, effect targets and system-client identity.
`CellGroupSqlPool::stage_registered` validates the operation, full class schema,
normalized input digest and authenticated identity before receipt lookup, then
uses the existing shared worker and exact proposal settlement path. Signed effect
wire and the class manifest use distinct marker-1 domains; there is no old-wire
reader. The temporary single-cell host projects its actual logical binding.
The shared command path derives identity directly from cell execution authority.

Registered query contexts still expose `CommittedVersion`/`GroupScope`. Replace
that boundary with the selected logical cell version and witness before the public
cutover; never infer a mutation sequence from group progress. The catalog/SDK
adapter must select the authorized class and preserve invocation deadline,
fingerprint and retry budget. Full shared effect scheduling remains CG-08 work
before CG-07. Reuse the existing SQL workers and joined shutdown.
A cancelled caller cannot release physical callback ownership or allow a second
producer for that cell.

Shared read selection and SQL execution now exist. Use
`CellGroupRuntime::read_authority` with the original absolute domain deadline,
then consume its pin through `CellGroupSqlPool::query`. Strong reads correlate
fresh quorum to that invocation and wait for contiguous apply; Stale/AtLeast
select logical cell versions. The immutable pin, node/group reservations and
worker-owned query survive publication and cancellation. A returned query result
borrows retained bytes and exposes its logical `CellVersion`.

Registered query integration must still replace
`application/consensus/class.rs::QueryContext`'s `CommittedVersion`/`GroupScope`
identity with the logical cell version. Preserve operation/schema validation,
read-only callback restrictions, version-token authentication and installed
transfer lineage. `CellExecutionAuthority` and `CellReconciliationAuthority`
still grant no strong-read authority; use the new request-owned read path.
Do not synthesize a cell-scoped legacy group or treat the group barrier index
as the target cell's mutation position. SDK token issuance should consume `CellSqlQueryResult::observation()` or
`CellCommittedReceipt::observation()`. These retain the selected version's actual
cell decision witness. Real compaction integration must preserve it; never use
the latest group progress or the retried command's proposal as a substitute.


## First executor session

### Design delivery check, 2026-10-30

The requested deliverable is the executable design and implementation plan.
The existing runtime checkpoints remain implementation evidence at their stated
boundaries; this delivery does not complete CG-04 or authorize a release claim.
Fetching origin confirmed that the branch contains `origin/main` at `8a93ffd7`.
The supplied TiKV checkout still matches the investigation pin above.

Package validation passes: 16 tasks, 95 required cases, 14 invariants and nine
profiles. Repository documentation validation also passes. In the current working
tree, preflight deliberately refuses source drift in
`crates/cellar/durability/raft/host_sql/mod.rs`; consequently 36 of the 37 plan
validator tests pass and the actual-source preflight test errors. Do not refresh
the baseline merely to silence this refusal.

At that design-delivery check, `cell_groups/apply/` and
`host_sql/cell_groups/apply.rs` were uncommitted drafts. Subsequent execution
completed that adapter at `2085cc96` and repaired the regression fixture at
`c9100ec7`; see the current checkpoint in [progress](progress.md). The untracked
`diagram/` directory remains separate existing work and is excluded from this
implementation checkpoint.

### Resume implementation

Inspect `git status` and the existing diff before editing. The physical owner in
`crates/cellar/durability/raft/cell_groups/owner.rs` and its native execution
fixture now use real three-voter Raft with complete group manifests and separate
SQLite images. The initial fixture compilation failures have been repaired.
The owner now runs on the shared node scheduler; public SDK and per-cell SQL
integration remain incomplete. Preserve its persistence, completion and physical
lease boundaries during integration;
do not mistake the low-level owner cases for the full CG-04 gate.

The SQL checkpoint includes `native_raft_execution` and `native_raft_rooted`
as well as the shared-group execution suite. Both cell receipt cases include
the final callback/owner-close extension. Preserve authority revocation, exact
committed entry matching and duplicate callback suppression during integration.
The pool now provides applied-barrier receipt reconciliation; production retry
dispatch, real compaction integration and preview/cache retention still need the
owning host. Reuse the old executor's current-term
applied-barrier rule when replacing it: a submitted producer cannot be discarded
from a sampled leader status, timeout or missing response. Resolve the original
command against the authoritative cell receipt before admitting another producer. These regressions supplement, and do not replace, the full task
gate printed by `show CG-04`. Registered SDK dispatch remains absent; registered query callbacks now consume
the existing read-authority and immutable-query paths.

The selected version's complete commit witness now persists in the authenticated
cell leaf and flows through query and receipt observations. The source-bound gate
at `ed84df5d` qualifies exact entry matching, unchanged-cell and older-pin
provenance, restart/image-collection recovery and token round-trips in both image
modes. Actual log-compaction and group-snapshot integration remain unqualified.

Local query invocations now connect catalog sessions to registered query workers.
Next connect command/enrollment routing and the public host without synthesizing
legacy group identities. Preserve original invocation deadlines,
contracts and retry identities. Query callbacks must receive logical cell versions;
returned tokens must use the selected observation. `CellGroupApplier` now consumes the startup manifest-page owner, and
`CellGroupSqlPool::prepare_apply` prepares committed cell images and complete
manifests on the existing workers. Consume its retained output with
`CellGroupPreparedApply::complete`; that queues owner publication and is not a
commit acknowledgement. The new native fixture uses this adapter on all three
voters. Registered host dispatch must now consume these APIs instead of copying
fixture image preparation. Keep physical apply position separate from provenance
in later group transfers.

Run from the repository root with Python 3.11+ and Rust 1.98.1:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-3617-cell-groups"
python3 docs/design/multi-raft/cell-groups/plan.py validate
python3 docs/design/multi-raft/cell-groups/plan.py preflight
python3 docs/design/multi-raft/cell-groups/plan.py show CG-05
python3 -m unittest discover -s docs/design/multi-raft/cell-groups -p 'test_*.py'
```

Choose a different private target directory for a different checkout. The plan
runner validates and prints commands; it does not execute Rust tests or provision
infrastructure. `ready` now returns CG-05 because CG-01 through CG-04 have passed
their scoped gates. Start CG-05 under its declared requirements. Global preflight
rejects any further unreviewed source drift; record
a reviewed successor source after reconciling each implementation change.

The four DONE records require their retained evidence directories listed in
[README](README.md). Transfer those artifacts with the source when validating on
another machine. Missing evidence must refuse; do not replace it with a pass flag
or reset completed tasks merely to bypass validation.

An earlier passing partial CG-04 record is
`.cellar/cell-groups-evidence/CG-04-apply-final/checkpoint.json` at `c9100ec7`:
254 selected Rust cases, fifteen successful commands, 202 source inputs and
12 copied executables. Transfer the entire directory and its failed predecessor
`CG-04-apply` with the prior evidence. It qualifies committed image preparation
on the existing SQL workers and retention through held publication, not direct
image-I/O cancellation/crash cuts or registered host serving. See the priorities
below before continuing CG-04.

The preceding durable-witness record is
`.cellar/cell-groups-evidence/CG-04-witness/checkpoint.json` at `ed84df5d`:
214 selected Rust cases, thirteen successful commands, 194 source inputs and nine
copied executables. Transfer the full directory with prior evidence. It qualifies
durable selected-version witnesses and read/receipt observations; registered SDK
serving, real compaction, CG-04 and the release gate remain incomplete.

The preceding coherent-read record is
`.cellar/cell-groups-evidence/CG-04-reads-final/checkpoint.json` at `3da4481e`:
204 selected Rust cases, twelve successful commands, 194 source inputs and eight
copied executables. Transfer the entire directory, including intermediate failures,
with the prior evidence. This qualifies low-level coherent reads and immutable
SQL in both image modes; it does not complete registered SDK query serving,
production compaction, CG-04 or the release gate.

The preceding submission record is
`.cellar/cell-groups-evidence/CG-04-submission/checkpoint.json` at `0cf89c40`:
197 selected Rust cases, ten successful commands, 189 source inputs and eight
copied executables. Transfer the entire directory, including intermediate failures,
with the prior evidence. Both image modes exercise rejection cleanup for creation
and mutation, exact proof matching, remembered acceptance and delayed retries.
This remains a partial CG-04 checkpoint; it does not qualify production dispatch,
coherent reads, RustFS or the hard cutover.

The earlier receipt-lookup partial CG-04 record is
`.cellar/cell-groups-evidence/CG-04-receipt-lookup/checkpoint.json` at `91c3ecfc`:
187 selected Rust cases, ten successful commands, 181 source inputs and eight
copied executables. Transfer its entire directory with the earlier checkpoints.
It qualifies saturated-capacity lookup, replicated expiry/conflict and existing
reconciliation paths in both image modes; it does not complete CG-04.

The prior reconciliation record is
`.cellar/cell-groups-evidence/CG-04-reconciliation/checkpoint.json` at `8a0cf640`:
181 selected Rust cases, nine successful commands, 179 source inputs and eight
copied executables. Transfer its entire directory along with the prior records.
It qualifies the new pool reconciliation boundary in both image modes, not actual
compaction, production retry dispatch or the full task gate. See [progress](progress.md)
for the historical tested boundary and subsequent lookup/refusal qualification.

The prior partial CG-04 evidence is retained at
`.cellar/cell-groups-evidence/CG-04-sql-pool/checkpoint.json`. Transfer that entire
directory, including copied binaries and intermediate/successful logs, with the
prior SQL-producer, runtime/owner and DONE-task evidence. The pool checkpoint
binds 178 source inputs, eight executables, 184 selected Rust cases and ten
verification commands. It cannot stand in for the complete CG-04
contract. Three RustFS-specific cases remain explicitly unrun at this checkpoint.

## Next execution priorities

Deliver CG tasks in dependency order and keep the status at its actual qualified
boundary. At the end of each turn, record the completed behavior, evidence and
limits, then identify the most critical remaining gap and the concrete next step.
Do not let plan maintenance replace implementation.

1. Finish the full CG-05 acceptance gate around the implemented checkpoint,
   learner and membership paths. Bounded 32-cell transfer and physical reader
   retention/release now pass. Finish the remaining fault/lifecycle requirements
   below, then consolidate the proof into all five exact cases. Preserve permanent claims, local install receipts, complete
   physical recovery and unrelated-group progress.
2. Complete remaining CG-05 fault/lifecycle requirements: mid-stream process
   revocation and crash, minority signed receipt refusal, continuous capture
   fairness, exhausted apply/completion budgets, held sync/cancelled close,
   quarantine capacity and retained-root pressure. Explicit joint restart after
   replicated entry/exit now passes; this does not qualify every in-flight or
   automatic-transition crash cut. Idle/time/byte checkpoint triggers require an
   explicit cut-reservation contract. Keep the full task gate ahead of CG-06.
3. Deliver CG-06 archive/reclamation with verified complete checkpoints and
   contiguous tails. Upload completion alone cannot authorize reclaiming local
   recovery dependencies or an S3 durability acknowledgement.
4. Follow the declared dependency order through CG-08 and CG-07 for effect/cache
   scheduling and production SDK/fleet cutover. Retain catalog and current-term
   authorization, coalesced preparation, physical ownership after disconnect and
   group-local quarantine. Shared storage uncertainty stays node-wide. A missing
   receipt stays ambiguous; callbacks cannot be replayed automatically.
5. Continue topology, placement, hibernation and fresh-incarnation recovery gates.
   Remove `TransportRuntime::Legacy`, cell-scoped transport, `QueryVersion::GroupCut`
   and `RaftClass::invoke_group_query` with the old host in CG-16. These transitional
   paths must not become permanent modes. Never derive cell sequence or witness
   from a group log position. Final qualification reruns all gates at one revision.

Continue from `group_snapshot_transport/seed/`,
`cell_groups/directory/joined_bootstrap.rs`, `cell_groups/apply/recovery.rs` and
`tests/support/cell_group_seed_install.rs`. Seed metadata and an imported snapshot
cannot mint the local authenticated-install receipt. Keep configuration, membership,
receipt and complete root atomic. Reconcile unknown results through the exact
permanent claim and the existing page owner; retain physical owners through
cancellation and joined shutdown. Use canonical configuration equality when
comparing replay with live RawNode membership.

The opportunity is to reuse the existing shared consensus, SQL, read, apply and
provisioning paths. Avoid adding per-cell workers or another serving mode while
integrating the remaining protocols.

## Delivery sequence

| Milestone | Tasks | Required outcome |
| --- | --- | --- |
| Contracts and storage | CG-01–03, scoped gates already passed | Separate cell/group history, complete durable roots, bounded range routing and startup preparation |
| Shared runtime | CG-04–06 | Real multi-cell consensus and execution, complete-group snapshots, verified S3 archive and safe reclamation |
| Public cutover | CG-08 then CG-07 | Preserve effects and idle caches; route SDK and managed fleet through the shared runtime |
| Adaptive groups | CG-09–12 | Fenced transfers, split/merge, placement and coordinated hibernation/wake |
| Recovery and qualification | CG-14, CG-13, CG-15 | Reproducible load harness, fenced fresh-incarnation recovery, real RustFS and independent-domain tests |
| Final delivery | CG-16 | Remove the superseded one-cell architecture and pass all release gates at one revision |

Use the generated dependency order for exact scheduling. A hot cell may occupy
one group through the same runtime. Do not introduce a permanent old/new mode or
an old-format migration reader; preserve marker 1 with explicit domain separation
and refuse unsupported persisted layouts. Never delete existing user data as
part of the hard cutover.

CG-04's completed gate remains in its source-bound evidence. CG-05's complete
brief prints the five required acceptance cases, owning paths and exact commands.
The source, receiver and owner checkpoints above establish partial boundaries;
they cannot replace full snapshot, membership and lost-disk recovery qualification.

## Release boundary

Preserve local quorum as the Quorum acknowledgement boundary. S3 protection
requires a verified complete checkpoint and contiguous tail; uploaded log bytes
alone do not authorize reclaiming local recovery dependencies. A failed complete
group restore refuses admission. Disaster recovery requires the chosen fencing
authority API, Docker qualification, permanent old-host restart prevention and
fresh group/cell incarnations, including incomplete topology transitions.

Measure occupancies 1/8/32/64/128, both image modes, resource use and request
latency under the frozen profiles. Report wake latency by its actual phases and
include timeouts and refusals. No fixed cost saving or immediate hibernation
response is promised before measurement. Local Docker/RustFS results and
independent failure-domain evidence are separate gates.

Retain source, binary and output hashes, including failed runs. Update task status
only after its full gate passes. CG-16 reruns the complete capability matrix at
one release revision; `plan.py release` must continue to refuse until then.
Keep the requested MR-13/MR-14 progress JSON files excluded from the PR. Reuse the
active PR, report qualification limits and leave merging to a separate action.
