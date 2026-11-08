<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation handoff

Purpose: Current implementation checkpoint, evidence transfer and next executor steps.

## Current CG-11 delivery

At `a352f77d`, retained successor boots resume removed-member cleanup with the
original source witness, exact disk/engine lineage and fresh rooted capacity.
Removed replicas start parked without serving or voting; physical debt remains
charged until collection and atomic settlement. `RaftHostOptions.member_cleanup`
starts a node-owned cleanup worker with exclusive ownership and joined stop.
Combined movement workers instead enable `reclaim_removed_members`.

The retained-restart case passes both native and real RustFS image modes,
including restart before GC, restart after GC/before settlement, stale-process
and missing-witness refusals, no serving attachment and later receipt replay
without a second refund. Background cleanup owns a private catalog client;
external status controllers likewise use independent clients to the same backend.
See [progress](progress.md) for qualification and retained failures.

CG-11 stays IN_PROGRESS. Complete durable automatic partition execution and
adaptive placement, combined-worker deployment policy, the missing failure-domain/
reserve and bounded-fanout cases, concurrent child-receipt readiness, late faults,
larger inventories, mixed load and measured peaks. Configured SDK startup does
not select a daemon's combined-worker deployment policy. Independent voter
failure domains remain unqualified.

## Current accepted boundary

CG-01–10 have complete scoped task records. CG-07's public SQL/native/Axum/
daemon gates and CG-09's publication/source-fencing gates retain their recorded
sources. CG-10 passes all five exact cases and ten required commands at
`8b410f25bc4475f3520c2682625ead49e47ea385`. The three new RustFS movement,
pre-copy and partial-merge suites pass both image modes on that source and native
executable. The backend is stopped, objects retained. See [progress](progress.md)
for full source, executable, CLI and artifact hashes.

CG-11 now has a bounded node-local sizing/admission scheduler at `d0ef46ec`.
Its two native checks and fixed real RustFS suite pass both image modes, including
lost reservation confirmation, actual quorum loss, retained follower return and
exact minority-command reconciliation before joined shutdown. The checkpoint is
not full CG-11 acceptance. Its frozen source, executable, logs and remaining case
oracles are recorded in [progress](progress.md).

The new request-path fixes provide bounded successor-group discovery, automatic
strong-read relocation and public remote receipt inspection. Read-only retries
retain the original identity, catalog floor, deadline and attempt bank. Incoming
RPCs execute locally; remote receipt metadata never becomes a local SQL/archive
proof. Receipt replies retain their stored minimum separately from achieved
durability at marker 1. Valid engine admission atomically replaces advisory
routes. Keep concrete transport at the SDK invocation edge; storing it in Shared
breaks default-limit application type checking.

At `502198bd`, exact-plan execution consent and a node-owned joined transition
service connect admitted plans to journal-driven execution. Activated child groups
start their existing archive publisher without a host restart or foreground S3
request. Native and real RustFS cases pass both image modes through coordinator
replacement, automatic split, retained-parent return and subsequent reads/S3 writes.
The CHECKPOINT records in [progress](progress.md) pin source, executable and logs;
they do not close the full CG-11 task.

At `e582f380`, live source retirement validates every final voter's applied
tombstone before draining the target runtime, SQL, archive, seed and effect owners.
The existing joined directory worker now handles bounded target discovery for
read-first and write-first ingress after parent removal and retained restart.
One child can run beside its parent within the unchanged two-history bound.
Physical source storage remains charged; retirement does not reclaim disks.
See [progress](progress.md) for the frozen-source qualification.

At `cb3d0ca2`, fresh seal authorization requires a managed observer and verifies
configured destination headroom before the irreversible directory decision. It
retains physical parents, charges missing reserved/live members, uses settled
membership and preserves resource floors. The node control budget owns at most
six inventories. Refusal leaves parents writable and the prepared plan cancellable.
Selected authorization replay needs no new health observation. Native and real
RustFS cases cover all three refusal causes in both image modes; see progress.md.

The shared test fixture now uses 32-MiB images and a 256-MiB generation pool
within its unchanged two-slot, 1-GiB deployment budget. An explicit 64-MiB/
512-MiB profile proves the original storage overcommit still refuses. Keep the
original stack, capacity and deadline limits. The Fleet seal future is heap-owned
at its API boundary: the full split/merge regression retains expected CAS-error
backtrace capture and passes the default stack. Installed bounds/attestations are
separate from measured RSS and temporary disk peaks.

At `ee7d7b62`, `poll` discovers a complete bounded active local inventory and
rotates sampling and fresh split/adjacent-merge selection. Its state is charged
to the existing node control budget. `resume` verifies the exact persisted operator
plan and the actual admission read remains persisted-plan-only. New native and
RustFS cases prove before/after-CAS uncertainty, missing-plan refusal, cold sampler
recovery and node-owned split execution; both local children are discovered while
the physically retained parent is excluded. See [progress](progress.md).

At `1154e5f1`, explicit operator collection verifies selected retirement and
released holds, atomically syncs permanent source refusal with engine Clean,
and joins bounded page/image work before releasing the local registry slot.
Metadata is quota-bounded and charged to the existing cache budget; physical work
uses the existing preparation bank/semaphore. Retained startup and exact replay
preserve refusal. Three storage faults/quota checks, all eight native transition
cases, both scheduler cases, the full merge regression and the fixed real RustFS
reclamation suite pass. Integrations cover both modes. See progress.md for hashes
and the concurrent startup timeout retained as a development attempt.

At `8e94cd67`, rooted cleanup captures complete qualified occupancy before
physical collection, keeps the original disruption through all node settlements,
and refunds exact source slots once from local engine receipts. Byte and memory
floors stay conservative. Two-parent verification returns the actual joined replay
lane while immutable proof metadata remains charged in the same byte bank. The
one-operation ceiling is unchanged; ordinary archive/recovery admission is unchanged.

All nine native transition cases, both scheduler cases, the full merge regression,
source-storage and budget tests, and the fixed real RustFS settlement case pass.
Integrations cover both modes. The new split-to-merge cycle retains the unchanged
two-slot/1-GiB profile, loses the last actual settlement CAS response and same-task
confirmation, replaces services, and preserves exact results/latest versions,
old-token reads and S3 writes. It is not same-group membership balancing or a
voter-process restart drill. See progress.md for source, hashes and development
attempts, including the corrected new receipt-test version expectation.

Keep work on CG-11 until every gap closes. Durable cleanup discovery,
historical/retained-successor cleanup and configured node startup are implemented
above. Next complete durable automatic partition ownership, adaptive placement
and combined-worker deployment policy, then failure-domain repair/reserve and
bounded fanout. Resolve concurrent child-receipt readiness and qualify late
failures, larger inventories/quotas, mixed load and measured peaks. Task-level
acceptance remains open.
Consent remains explicit and irreversible sealing requires the recorded authority.
After CG-11 closes, continue with CG-12's full fenced park/wake lifecycle. CG-13 requires CG-14 infrastructure for
managed interrupted-transition recovery and permanent Docker fencing with all
voter disks erased. Keep CG-15/16's combined-source load/crash, public cutover and
legacy audit gates open. Full public capability coverage and existing Fleet data
conversion are not implied by scoped SQL acceptance.

Preserve the occupancy-128 gap under CG-14: the focused case passes, but the
original matrix still fails its unchanged 60-second enrollment window. CG-10
qualifies private staging, sealed replay and logical admission, with host-native
voters and fixture capacity. Further movement cycles, incremental transfer
performance, hard RSS, process/failure-domain qualification and release profiles
remain open. Current [priorities](remaining-work.md) retain the full design.

The following checkpoints preserve earlier source and qualification history.

## CG-07 cold router return after birth-voter replacement

The router returns after three rooted moves and joined retirement of the birth
processes. Preserve the prompt refusal of quorum reads on durably retired
voters and the physical settlement proof. Complete the original public
lifecycle and authority gates and qualify constrained voters.
[Source and test records](progress.md)

## CG-07 catalog-driven startup and public request qualification

The combined fresh RustFS prefix case now uses public provisioning and archive
commands, SDK voter readiness and enrollment, and native and Axum ingress.
Qualify retained cold router return after all birth voters retire and finish
the remaining managed lifecycle, public gates and constrained voter runs.
[Source and test records](progress.md)

## CG-07 daemon-owned first-group readiness

The daemon callback verifies the rooted archive, attaches the authorized local
voter and establishes fresh quorum before binding ingress. Replace the fixture
archive-ready rendezvous with bounded observation of the exact rooted intent and
registry. Preserve capacity attestation and boot trust checks.
[Source and test records](progress.md)

## CG-07 rooted first-group archive initialization

Archive setup runs through the CLI and admin API. Preserve the exact rooted
intent and verify it before startup. Move physical activation into the daemon
readiness path and establish fresh quorum before ingress.
[Source and test records](progress.md)

## CG-07 public first-group operator workflow

The CLI and admin path observe standby and reserve and publish the first
group using the exact rooted plan. Both RustFS process modes use the CLI
for capacity and metadata. Keep one-use archive initialization and daemon
activation under explicit ownership while moving them out of the fixture.
[Source and test records](progress.md)

## CG-07 independent managed daemon voters on RustFS

The real-backend process case now passes with both materialized images and
extents. It retains exact request and recovery ledgers. The fixture still
supplies capacity and the operator steps. Move these steps into public
management while preserving the exact standby, reservation, confirmation
and archive intent. Qualify constrained voter processes and the wider
lifecycle gates separately.
[Source and test records](progress.md)

## CG-07 managed daemon startup and request path

The normal native daemon starts three independent managed voters from standby
through a fixture operator. Their shared group serves two cells after fresh
quorum and survives a leader hard kill and retained restart. The daemon installs
shutdown ownership before Raft startup and joins the owner if asynchronous
readiness is cancelled. Next run the process gate against real RustFS and move
the fixture operator's capacity and first-group orchestration into the public
operator path. Preserve the original failures and request bounds.
[Source and test records](progress.md)

## CG-07 managed group archive provisioning

The SDK prepares the one-use archive initializer for a selected managed history
and verifies a saved intent without writes after setup. The managed native and
RustFS request fixture uses that path. Port the original daemon process gate
from per-cell registration to shared groups. Have the operator persist exact
setup, let each daemon attach its authorized local voter, and gate ingress on
a fresh quorum observation. Preserve every original refusal and restart oracle.
[Source and test records](progress.md)

## CG-07 managed first-group provisioning

The SDK reserves, publishes and confirms the first group from a declared
standby. The test keeps the Live birth decision across cancellation, verifies
conflicting and lost confirmation outcomes and reopens the exact reservation.
The managed request path passes on native storage and real RustFS.
[Source and test records](progress.md)

Next, integrate the public first-group operations into ordinary CLI and daemon
setup. Keep the archive creation intent distinct from retained open and require
actual voter activation and quorum before public serving.

## CG-07 managed namespace standby provisioning

The public SDK checks the compiled schema, publishes the standby and confirms
its fleet declaration. Exact retries recover lost replies. A new fleet request
at the new revision can reuse the same standby operation after a root race.
The managed native/Axum path starts from that standby and passes on native
storage and real RustFS. Existing registry replay and reset oracles also pass
on the shared fixture. [Source and test records](progress.md)

Next, wire CLI and daemon setup to the first-group reservation, exact range
publication, archive initialization and voter activation. Preserve the existing
capacity proofs and immutable operation identities through each boundary.

## CG-07 managed router membership refresh

The managed directory learns actual replacement membership for configured
routers. The native and real RustFS cases verify rejection of the retired
route, the exact old receipt, a fresh write through the replacement leader
and read-back from the joined voter. They also rerun the native/Axum request
path. [Source and test records](progress.md)

Next, wire ordinary namespace provisioning and exercise the public startup
and request path from an empty managed deployment. Cold router return after
all birth voters retire and the complete public lifecycle gate remain open.

## CG-07 routed SDK enrollment

The existing ingress path now creates cells through the SDK. It preserves
creation identity and achieved durability, including exact creation replay at
S3 durability. Eight native cases and the real RustFS managed case pass.
[Source and test records](progress.md)

Ordinary namespace provisioning, the complete public authority and lifecycle
gates, reviewed source preflight and the remaining cell-group tasks stay open.

## CG-07 nonmember ingress and idle connection retirement

The configured ingress path passes the seven scoped native cases and the
managed request case against real RustFS. It serves both read and write
requests through an ingress with zero local groups. The archive denial case
separates quorum acknowledgement from explicit S3 coverage. The sender now
retires a connection after the receiver's idle timeout before sending the
next queued request. The focused expiry regression and original counter
process case pass with their existing operation bounds.

Next, qualify SDK enrollment through this path and connect ordinary namespace
provisioning. Refresh router membership across replacement, complete the
public lifecycle and authority cases, and keep the earlier RustFS successor
readiness failure open. [Source and test records](progress.md)

## CG-07 movement discovery and basic request priorities

The lost-observation regression proves durable movement Begin within the
original deadline. It preserves the debit and leaves the learner inactive.
The full managed native target passes. Real RustFS confirms the regression
and archived maintenance, and reproduces the successor readiness timeout.
The [progress record](progress.md) retains all three backend outcomes.

The [basic request acceptance sequence](remaining-work.md) starts with
trusted route discovery from ingress hosting no replica, then exercises normal
namespace provisioning, failover, retained restart and quorum-loss refusal.
Keep the original successor deadline and staged-target close oracle while
isolating the RustFS failure.

## Managed request routing checkpoint

A public SDK command sent to a follower previously attempted local SQL and
returned NotLeader despite an observed healthy leader. The pre-fix managed HTTP
case reproduced this twice, with failed binaries/logs and voter directories
retained. Queued originating SDK commands now retain one controller invocation,
identity, captured inputs, admission and original deadline through one forwarding
hop. Incoming RPCs and the lower-level controller command API stay local-only.
The destination independently checks its route, namespace process permission and
current execution authority. Possible transmission keeps an error ambiguous;
there is no automatic uncertain mutation retry or new identity.

Trusted embedders can grant/revoke command origins after managed discovery has
verified the actual peer boot/endpoints. Host namespace capabilities and current
transport identity/revision checks are mandatory. Membership grants no permission;
revocation retains a revision tombstone without undoing admitted work. Permissions
belong to the running transport and require trusted policy after restart.

Command output now distinguishes local inspected SQL receipts from authenticated
remote results. Common output metadata is available for both; local_receipt is
present only for the local variant. Wire encoding, effects and physical oracles
retain local proof requirements. Existing tests make those requirements explicit.
Temporary diagnostic logs are removed and no assertion or lint is suppressed.

The new managed native/Axum case passes in both image forms: cold capacity and
group birth, replicated enrollment of two independent cells, follower writes,
remote minimum-version/linearizable reads with actual responder identity,
conflicting retries, cross-cell tokens, absent/revoked/stale-process/foreign-
namespace permission refusals, forwarded S3 acknowledgement, joined leader-stop
failover, retained restart with original receipts/floors and new writes, then
quorum-loss write/read refusals. The counter declares explicit namespace origins;
its public process gate now requires original receipt success through every one
of six native/Axum addresses before and after retained restart.

The complete eleven-target final-01 native campaign is **FAILED: 308 passed,
2 failed, 39 explicit skips**. The new managed HTTP case and public SDK/process
gate pass; the public gate takes 140.55s. The managed
target fails eight-turn maintenance replacement settlement and fresh successor
readiness after closing an S3 source service, including an Unobserved close
report. Original bounds/assertions and failure disks/logs remain unwaived. These
are previously recorded failure classes; the forwarding fix does not establish
their causes. No narrower component pass makes the full campaign green.

RustFS comparison: managed request run1: PASS (53.37s); managed request run2: PASS (50.61s); archived maintenance: PASS (88.53s); automatic S3 recovery: PASS (45.41s).
Matrix status is **PASSED**. Both request invocations cover both image
forms and pass; every comparison case retains its own result. Signed readiness
and conditional probes pass. The immutable-ID/owner/resource/mount-verified,
pinned one-CPU/two-GiB backend is reused with fresh buckets/prefixes; backend and
dedicated VM are stopped with data retained. Voters remain host-native embedded
Nodes with fixture capacity/isolation. Joined stop is not a process-crash drill;
independent constrained voters, permanent fencing, partitions and load remain
unqualified.

Source-final-01 binds 2,726 Rust/Python/manifest inputs above
3ccc3e51f03458a2a5715401b8e5f6b963146276 to SHA256
da3514ed9ea2dd614a45f3150f92d2280f75795287b7469a5883f72564b1a87c.
FAILED native record SHA256 is
6a4f24b43031c9b9cab3a996edd3b3bf280ecf494a2a231d997912b0e75098e7.
Its individual managed request proof is extracted explicitly from that failed
campaign (SHA256 8f6e418e605c0d56a635b245ded5cc423d7657cf8f548bdc40109cdb3bdac1a8).
RustFS record SHA256 is
646ebe709b457b540a97a12e85e43aade6aee1fbbb445a5d37d84a2650330695.
Default stack, opt-level 0, assertions/overflow checks, quotas and original
operation/movement bounds remain intact. Code inputs are checked unchanged.

Format/layout/dependencies/docs, native-only Cellar/Python-library and diff checks
pass. Strict library Clippy and plan tests/validate/preflight remain red, with no
Clippy diagnostic-key additions or silent source-anchor refresh. Earlier unseeded
maintenance/ambiguous-write/transfer/TCP/enrollment/inspection failures and older
disk-erasure qualification keep their own source and scope. CG-07 remains
IN_PROGRESS; four public gates, default/capability integration and CG-09–16 remain
open. Automatic command selection still needs local group status: route-only
ingress requires bounded leader discovery. Queries still default stale.

## Member observation renewal checkpoint

A deterministic managed regression now reproduces one lost local-member
observation after leadership handoff. Its foreground catalog client is independent
of the controller and background trust refresh, over the same actual backend.
Before the fix, that read consumes the entire original five-second renewal budget;
local snapshots show a healthy quorum and empty work banks. The peer observation
now receives a share of the remaining time, reserving time for other members and
final validation. A transient refusal leaves the history pending; retry obtains
all three fresh safe ReadIndex/durable-apply observations. Both image forms pass
without resuming the lost read. Root, boot, configuration and proof-age checks
remain intact. No write retry, stale proof or longer deadline is introduced.

The lifecycle harness also reconciles actual leadership before/after maintenance
using fresh term/revision guards, rather than treating a historical survivor and
one admitted handoff as completed election. It observes actual target leadership
inside one original five-second envelope. The eight movement turns and all
durability/capacity assertions remain unchanged. Bounded local status/admission
snapshots are recorded on failure before cleanup; the original error still fails.
Temporary tagged tracing and its focused prototype are removed.

Source-final-02 binds 2,722 Rust/Python/manifest inputs above
6b70a5357a470f75b7e36ec201f44f31ea7da536 to SHA256
7ad7649846ca9405e6be33738b6d851f256a887e4af08c4f86004c590039554b.
The complete affected managed target passes **17 cases with 6 explicit backend
skips**, in 307.21s. Native record SHA256 is
e589e438c251e71d8ddba76abf8eb9055b63b7a08abec06526a402b431424441.

Four same-source RustFS invocations pass: the lost-observation regression
32.19s, the unchanged full archived maintenance case
twice (85.71s and 100.80s),
and automatic S3 voter recovery through terminal removal
40.46s. Both image forms run; maintenance additionally
covers initial and joined voters. Record SHA256 is
d3c69d70f2e2fe99a840582cdf7248f5c833abbeb939a54b125ea95fa18e4040.
The immutable-ID/owner/resource/mount-verified one-CPU/two-GiB backend is reused
with fresh buckets/prefixes because retained Docker networks exhausted default
subnets. No retained resource is deleted; backend and dedicated VM are stopped
with data retained. Nodes remain host-native with fixture capacity/isolation;
independent constrained Linux voters and production fencing/load remain unqualified.

The earlier unseeded renewal timeout was not captured at a causal low-level seam.
An instrumented full case also failed an ambiguous sequence-4 S3 command before
owned resume; another diagnostic run missed restored-target leadership. Those
failures and the initial final-source 15-pass/2-fail campaign (NotReady maintenance
and eight-turn peer replacement) are retained and unwaived. The new regression
proves recovery from a lost observation, not every prior failure's root cause.

Format/layout/dependencies/docs, native-only Cellar/Python-library and diff checks
pass. Strict library Clippy and plan tests/validate/preflight remain red; diagnostic
comparison adds no key. Reviewed source anchors are not refreshed and no lint is
suppressed. The earlier seven-target/SDK and disk-erasure results remain bound to
their recorded source; this checkpoint qualifies the affected managed boundary.
CG-07 stays IN_PROGRESS; four public gates, default/capability cutover, CI/load,
unexplained failures and CG-09–16 remain open.

Next: deliver the public managed lifecycle and ingress-authority gates; investigate
the unseeded command/transfer/replacement failures using the retained snapshots
and original bounds. Finish actual Docker isolation/resource-provider qualification,
then default/capability wiring and the remaining CG-09–16 objective.

## Automatic recovery checkpoint

The Node-owned automatic failure service now passes the complete local
shared-group path in both image forms: default failure detection, rooted
exclusion, exact-boot fixture isolation, S3 replacement, fresh stable RF3,
independent archive verification, finalization and journal-confirmed removal.
The fixture first holds isolation and proves repair has not started. Shutdown
joins that callback and releases control admission. A duplicate service refuses
without another charge. Two service restarts preserve the rooted operation;
archive payload refusal blocks terminal progression, and restored reads require
new physical S3 GETs before removal. The old engine remains byte-identical and
retained, active movements/reservations clear, and both cells retain original
receipts and accept new explicit-S3 writes. No business mutation drives repair.

This new path first aborted on the unchanged default stack. Sanitized poll-frame
evidence and repeated red binaries are retained. Control turns, recovery/removal
phases and live verification now retain boxed futures under the existing worker
owner, admission and original deadline. A subsequent receiver refusal hid the
actual missing-spool I/O error. Archive download now joins and returns the typed
consumer failure; the malformed-checkpoint assertion is tightened accordingly.
A real-RustFS repeat exposed about 1.96 MiB of application poll frames before
the HTTP client. Catalog turns now own boxed work before their future is
constructed, shrinking nested control frames without moving lock or deadline
checks. Managed replay creates and syncs its private scratch directory before receiving
materialized image bytes. No stack, quota, grace or operation deadline is raised.

Source-automatic-qualified binds 2,721 Rust/Python/manifest inputs
above 29f277ddf228f33068c229583efb487e01749f07 to SHA256
e8c8f4379f8587ee70c5b011dcaf11442104fde90915da5359199f95ebfaaa3b. Seven native targets pass **267 cases with 36 explicit skips**;
the public SDK/native/Axum process/restart gate passes in 64.65s.
Native/public record SHA256 is
b5d4f68e12ad56e0676e6f29974ce9035584579e46edf0b8c470816359ef151c.

Three exact lifecycle cases pass against pinned real RustFS in both image
forms: automatic recovery, isolated replacement and normal replacement/joined
restart. Their durations are 43.82s, 26.58s, 37.99s.
The same campaign then FAILS archived joined-voter maintenance after accepting
leadership handoff: the original five-second survivor-proof renewal expires.
The 38.54-second failed fixture and all disks are retained; no deadline is
raised and no later pass waives it. The disk-loss case was not reached there.
Campaign record SHA256 is
13b1f1f4a6b88cfd6362e5ad07fe79a6b5bd15e4555b0a580fecf7ce61dc0243.

A separate same-source exact public fresh-incarnation recovery case passes in
both image forms after every old voter directory and verifier cache is erased,
in 20.67s. Its record SHA256 is
f739ea9c2027c50ecfa1c1c02176a59a8a9415f2447ade8128000c24ae836298.
Signed readiness/conditional probes pass for both owned backends; each one-CPU/
two-GiB backend and VM is stopped with data retained. These are host-native
embedded Nodes and fixture isolation, not independent constrained voter containers,
permanent Docker launch revocation, sustained load or the full public lifecycle.
The backend matrix is not green.

Format/layout/dependencies/docs, native-only Cellar/Python-library checks and
diff checks pass. Strict library Clippy and plan tests/validate/preflight remain
red; the saved diagnostic comparison records 0 added diagnostic
keys. No lint suppression or source-anchor refresh is applied. Earlier TCP,
bounded readiness/maintenance, 128-cell enrollment and inspection failures remain
unexplained and unwaived despite later passes. CG-07 remains IN_PROGRESS;
CG-09–16, the public/default/capability cutover and the full goal remain unfinished.

Next: diagnose the current RustFS joined-maintenance proof timeout under the
original five-second bound, then deliver the public managed lifecycle/ingress
gates and orchestrate retained debt on surviving original voters, including
joined cancellation and uncertain outcomes.
Qualify actual Docker isolation/resource providers over independent constrained
voters. Then finish namespace/SDK/CLI/daemon/admin/example defaults, capability
coverage and the other public gates, followed by CG-09–16. Preserve live original-
voter physical closure and quorum admission; no weaker fallback is allowed.

## Quarantined voter checkpoint

Shared managed startup now verifies current host authority and prepared namespace
declarations against one root before opening mutable engine state. It still
requires the later engine-bound boot CAS. A saved pre-isolation capability cannot
modify an already quarantined log. Shared physical route refresh discards a
retained initial endpoint only with rooted permanent isolation, so an unusable
old endpoint cannot prevent publication of an admitted learner's route. Durable
votes, seed owners and resource debt are unchanged until their existing proofs.

The new managed isolation case joins the complete obsolete Node, rejects premature
and wrong-process evidence, and verifies typed startup refusal without fleet/log
mutation. S3 replacement must restore both cells, obtain fresh stable RF3, preserve
receipts, reconcile a lost settlement confirmation and attribute the isolated
obsolete source. The retained log stays byte-identical and the replacement keeps
its physical capacity floor. Two log-mutation reproducers and the eight-turn
blocked-route failure are preserved. Diagnostic probes are removed from source.

Source-isolated-final SHA256 is
cc2e83093bfe91439dce852e25e3340108b885f3b79b3a05e1453a9596f3feb1
above 9070eb15, for 2,719 Rust/Python/manifest inputs. Seven native targets pass
266 cases/35 explicit skips; public SDK/native/Axum process/restart also passes.
Native/public record SHA256 is
60c4918d707c26a95efefa112dfa8400b194c67faa8643b71edd20586b812732.
Four same-source real RustFS cases pass: isolated replacement, normal replacement
and joined restart, archived maintenance and all-old-voter-disks/verifier-cache
erased public recovery. Record SHA256 is
3ea848910a5843bded56eb0140933cb2bd4120545bc32be37d6c9bee838e0d16.
Backend/VM are stopped and data retained. Both image forms run with original
quotas/bounds. Host-native embedded Nodes and fixture fencing do not qualify
permanent Docker launch revocation, independent constrained voters or load.

Format/layout/dependencies/docs and native-only library checks pass. Strict
library Clippy still reports 220 errors/202 dead-code reports. The only changed
message removes the now-used `is_isolated` from a grouped unused-method warning;
expanded item/count comparison adds no diagnostic. Plan tests retain 35 pass,
one index/Purpose failure and one reviewed-protocol drift error. No suppression
or automatic source-anchor refresh is applied.

Next: complete automatic failed-voter and retained surviving-voter-debt
orchestration, terminal survivor/archive verification and the full public managed
lifecycle gate. Preserve the live physical-closure requirement for surviving
original voters. Finish four missing public gates, capability/default cutover,
performance/inspection/lint/plan/CI modes and CG-09–16. Earlier unexplained
failures remain open despite current passes. The full goal is unfinished and
PR #160 remains draft.

## Idle/restart seed retirement checkpoint

A bounded node-owned sweep now retires authenticated retained seed journals during
idle operation and restart. It validates history/purpose/engine/archive identity,
shares live sources' group locks, reconciles the exact intent and confirms abort
before syncing empty state. Unknown/missing/corrupt state never initializes a
replacement or permits retirement of an unknown owner. Cleanup can reopen an old
immutable catalog binding without serving or membership authority. Physical joins
precede lock/admission release, including errors/panics. Joined close retains an
entered remote read even after its waiter is cancelled. One 272-KiB workspace is
charged to existing provisioning capacity; ordinary S3 admission is per journal.
The source and cleanup modules live under `controller/archive/seed/`; public Rust
paths and configured ceilings/deadlines/profiles remain unchanged.

CG-07-seed-cleanup-01/source-final SHA256 is
9d8d111ff2121fe9802d186b04710039e480c478366b0de9f0d372719e995603
for 2,718 inputs above acd56a9f. The complete native campaign passes 265 cases/34
explicit skips. A test-only inventory-before-close adjustment reran managed;
production and seven other binaries are unchanged. Two journal cases and focused
cleanup pass. Current public SDK/native/Axum process/restart passes in 132.07s.
Four real RustFS cases pass in both image forms, including idle/solo-restart debt
retirement, cancelled close and all-voter-disks/verifier-cache-erased recovery.
Backend/VM are stopped; data and exact sources/binaries/failures are retained.
See [progress](progress.md) for provenance and timings.

Strict library Clippy remains 220 diagnostics/206 normalized keys without additions.
Plan index/Purpose and reviewed-protocol drift remain red. Preserve earlier TCP
overload, five-second readiness, eight-turn maintenance, enrollment and inspection
failures; passes supply no causal fix or SLA. Next: fenced failed-voter/retained
survivor-debt orchestration, four remaining public gates, capability/default
cutover, collection/performance/inspection/lint/plan/release modes and CG-09–16.
Lost ownership still needs proper recovery authority. Independent constrained
voters, permanent Docker fencing and load remain unqualified. The full hard
cutover is incomplete and PR #160 stays draft.

## Managed S3 learner checkpoint at acd56a9f

Managed `from_s3` activation now captures a fresh-quorum complete checkpoint,
journals a separate upload generation and authenticates its ticket/reference in
the seed reply. The pending generation and inherited graph protect every object.
The destination independently verifies the complete closure through its serving
receiver before READY1; live source checks precede End and the existing engine
boot/catalog/install fence remains authoritative. Transfer data supplies neither
a disaster-recovery head nor an S3 command acknowledgement. Marker-1 seed requests
require `from_s3` and replies require a tagged data mode; old shapes refuse.

One owned source job retains the original transport/read/metadata/checkpoint/S3
resources through upload, handoff and cleanup. Joined close stops new admission
and drives transport and source cleanup together. A failed retirement retains its
durable intent and an observable error; a subsequent seed reconciles that intent.
Automatic background/restart cleanup remains required.

A reproduced source-failover regression found that managed coalescing included
source member/node hints in destination identity. The comparator now retains the
exact destination boot/member/permit and mode, while the next completed attempt
may reauthorize a new source. An entered attempt retains its original hint and
deadline. A mode change cannot adopt the retained preparation.

The new fault case verifies actual
target-only S3 payload refusal, a retained nonserving preparation and resource
debit, no peer fallback, source-service stop and fresh election. Full membership
repair must obtain a fresh quorum. Catalog settlement separately requires live
physical inventory from surviving original voters, so their outage retains the
debit; it cannot be converted into an offline proof by a timeout. The normal full
settlement/replay gates remain required. Explicit RustFS variants cover S3
replacement/restart, archived maintenance and this failover/debt case.

Candidate source SHA256 is
65cbd074ab06749b4849ef06d38c23e0df396385c63e3b74bc6fb48159cdf5e7
for 2,717 inputs above 41a67844. Native results are 264 pass/one fail/34 explicit
skips. Execution's checkpoint TCP retry reports `Overloaded`; its unchanged
focused repeat passes, without clearing the failed full run. An earlier extent
successor-readiness timeout remains unresolved despite three isolated and later
complete managed passes. The original archived-maintenance eight-turn failure
also remains preserved. Bounded failure-only observations cannot extend a gate.

Four same-source real RustFS cases pass: S3 replacement/restart 34.15s, archived
maintenance 85.93s, denied-payload/failover/debt 33.04s, and public all-voter-disks
plus verifier-cache-erased recovery 17.56s. Both image forms run; the backend and
dedicated VM are stopped with data retained. An initial signed S3 probe returned
503 before tests; keep that failed setup too. The passing setup checks signed-read
readiness before conditional writes. Embedded Nodes/contract fencing supply no
independent-voter, permanent Docker fencing or load qualification. Strict library
Clippy is 220 errors, down one with identical production inputs and no added keys;
plan/index/protocol drift remains red. See [progress](progress.md) for boundaries.

Next: diagnose the checkpoint retry and bounded readiness/maintenance failures.
Preserve the earlier failed runs and all original five-second calls/eight-turn
reconciliation bounds. Finish abandoned hold cleanup,
fenced failed-voter/public lifecycle ownership, default/capability cutover,
manifest-page collection and the existing performance/inspection/lint/plan gates.
Permanent Docker launch fencing, independent constrained voters, sustained load
and CG-09–16 remain required. The complete goal is unfinished.

## Dynamic archive ownership checkpoint at 41a67844

Canonical startup now retains node-wide pools and independent verifier resources
for groups added after boot. Preparation opens existing archive identity/counters
before bootstrap, charges both store bounds to the existing provisioning bank and
retains the private owner through uncertain attachment/learner installation.
Successful physical attachment hands it directly to the actor's bounded archive
lane. Joined node close releases preparations, archive jobs and then immutable
configuration; cancelled waiters cannot release an entered archive owner.

CG-07-dynamic-archives-01/source-final SHA256 is
9cb0776c7a699f6e7af5f1aa6ed03d3bd09f277967a6bcbb5d70051a12d39f7d,
above 8c0a42f02eb78060adaa5c87472ab34f95b6e5c7. Twelve managed cases pass,
including dynamic RF3 S3 writes, missing identity refusal followed by explicit
synced initialization/retry, and joined close while an archive GET is held. Both
image forms run through native Nodes/SQLite/TLS with an in-memory fault adapter.
Seven native targets pass 263 cases with 31 explicit skips and unchanged source.
The same binaries pass public fresh-incarnation recovery against real pinned,
constrained RustFS in 30.33 seconds for both image forms after erasing all source
voter directories and verifier cache. Backend and dedicated VM are stopped.
This is embedded-Node/contract-fence coverage, not managed archive qualification
against RustFS, permanent Docker fencing, independent voter containers or load.
Public SDK process, full performance and full library gates were not rerun here;
the preceding performance/inspection/release gaps remain open.

Format/layout/dependency/docs and native-only library checks pass. Strict Cellar
library Clippy remains red with 221 errors, one fewer than before and no new keys;
202 are dead-code errors. Plan tests remain 35 pass/one fail/one error, with
generated index/Purpose and reviewed-protocol drift refusals preserved.

Next: qualify archived managed replacement and maintenance, then implement the
currently refused exact-reference S3 learner path. Preserve complete manifest,
image and receipt closure, uncertain outcomes, actual destination boot/catalog
authorization, physical installation and fresh-quorum catch-up. Do not treat a
seed or remote object as activation authority. Finish archive hold retirement,
public/default-backend gates, live manifest-page collection, permanent Docker
fencing/load qualification and CG-09–16. The full goal remains incomplete.

At that checkpoint, the peer seed exchange lived in `group_snapshot_transport/seed/`: `client.rs`
authenticates the source checkpoint and stages the exact managed grant;
`install.rs` receives peer chunks and reauthorizes before durable installation.
The authenticated reply carried no archive reference. The S3 path needed to
bind and retain an exact archive cut alongside that grant, check the complete
destination against the granted marker and preserve the same install/deadline
fences. Removing the `from_s3` refusal alone cannot provide that contract.

## Owned workspace barrier qualification checkpoint

This checkpoint supersedes the validation and performance boundaries below.
CG-07-wide-enrollment-01/source-08 SHA256 is
f9449fbd8937d6a2b5f9a871859bf1e7469b767df51f86f8720a2fb8fff61c57,
above d268cf0bfc78c49f8c2d48f17a4a735f42028d81. Workspace creation syncs its
new child and owned parent, reusing the open inventory's ancestor barriers.
Rooted private executors retain and validate the exact workspace directory owner;
materialized paths keep their full preparation. Read-cache directories are
disposable. No deadline, quota, profile, stack, authority fence or original
assertion changes. Child/parent sync failure retains charge and quarantine;
physical close/deletion still precedes refunds.

Five LTX targets pass 66 cases, eight shared-group native targets pass 262, and
the two affected legacy execution targets pass 71. Their explicit process/backend
skips remain recorded. Public SDK/native/Axum restart passes in 155.92 seconds.
Same-source public recovery passes both image forms against real constrained,
pinned RustFS in 19.30 seconds after erasing every source voter directory and the
old verifier cache. Backend and dedicated VM are stopped afterward. The drill
uses embedded native Nodes and a contract fence; permanent Docker launch fencing,
independent voter containers, managed recovery and sustained load remain open.

The original performance suite still has eighteen passes and one failure:
occupancy-128 enrollment reaches 58 cells, then returns ambiguous DeadlineExceeded
under its original sixty-second population budget and five-second call cap.
Both 32-cell cases and reclamation pressure pass. Preserved probes locate more
time in physical apply preparation than publication or collection in that sample;
the image/page split still needs measurement. Two startup-pipeline experiments
failed and were removed, along with every temporary diagnostic log/test. The
original scheduler and applied-equals-committed enrollment fence remain. Exact
failed sources, executables and fixture disks are retained.

Feature/build/static checks and strict LTX library Clippy pass. Strict Cellar
library Clippy retains the same 222 diagnostic keys; plan index/Purpose and
reviewed-protocol drift failures remain. Full-library inspection was not rerun.
CG-07 remains IN_PROGRESS; PR #160 stays draft and the goal remains active.

Next: retain node-owned archive resources for groups activated after startup,
including joined stop/drain ownership, then implement the currently refused
exact-reference S3 learner path. `group_host/archive.rs` registers only the initial
prepared roster; `controller/provision/mod.rs` explicitly refuses S3 transfer.
Keep archive initialization explicit and journaled, reauthorize the actual
destination boot/catalog and require physical install, catch-up and fresh quorum.
Live manifest-page collection, remaining public/default capability gates,
deployment/read policy and CG-09–16 remain required.

## Archive metadata/cache qualification checkpoint

This checkpoint supersedes the performance and validation boundary below.
CG-07-wide-group-diagnosis-01/source-08 SHA256 is
4b55360d8d064379763d69f3d57b13b34074bef86b9328568da773b636b775e0,
above 3dc6e25cb23a87a263edf865d24c626d087ebee4. The open owner may reuse its
completed immutable root/page barriers, while still checking bytes; reopen and
failed reads invalidate that observation. A charged verifier cache stays below
one quarter of each existing ceiling until ordinary collection pressure. Serving
collection remains eager. Incoming buffered bytes are checked even when a valid
destination exists. No deadline, quota, optimization or assertion is relaxed.

Forty-five LTX and 261 native cases pass, with their owned process/backend skips
recorded. The public native/Axum retained-restart case passes in 179.65 seconds.
Same-source public recovery passes both image forms against real constrained,
pinned RustFS in 33.02 seconds after erasing all source voter disks and verifier
cache; backend and dedicated VM are stopped afterward. This is an embedded-Node,
contract-fence boundary, not permanent Docker launch fencing or independent voter
containers. Sources, exact executables, failed runs and fixture disks are retained.

The full performance gate now has eighteen passes and one failure. Occupancy 32
passes the matrix and isolated S3 diagnostic; occupancy 128 is reached but fails
enrollment after 44 observed cells under the original 60-second population budget
and five-second per-call cap. Measure enrollment, complete checkpoint and
collector interference next without raising bounds or assuming a fixed cell-count
limit. Strict Cellar library Clippy retains the same 222 diagnostic keys; plan
index/Purpose and reviewed-protocol drift failures remain. Full-library inspection
was not rerun. CG-07 remains IN_PROGRESS and PR #160 stays draft.

Next implementation priorities are managed dynamic archive attachment and exact
S3 learner activation, owned live manifest-page collection, complete public
lifecycle/ingress/default-backend gates and whole-group Docker-fenced recovery.
Capabilities, deployment/read policy and CG-09–16 remain required. See
[progress](progress.md) and [remaining work](remaining-work.md) for exact limits.

## Owned live extent collection checkpoint

The node-owned collector now covers the serving and separate archive-verifier
extent inventories after their complete reference adoption. It uses the existing
control bank and service registry, with sixteen deletions per physical turn;
reader, manifest, receive and uncertain-publication pins remain authoritative.
A cancelled close waiter cannot release admitted physical work or advertise
storage closure early. Advisory resource samples expose progress, deferrals and
failures. This does not add live manifest-page collection or weaken the offline
engine collector's joined-ownership guard.

Capture CG-07-live-collection-01/source-01 SHA256 is
a1ea8aa8ab265debeef983589f385eb8cf4e398327fd4ee22aaa0eb7af05a931.
Initial locked compilation, LTX six apply/36 store (one child helper ignored),
held-deletion close and RF3 pressure checks pass. The pressure case configures
sixteen roots for each image role and runs eighty additional commands with the
original request budgets, final S3 proof, exact duplicate receipt and every-voter
minimum-version read. Exact source/executables are preserved before broader
regression. Final seven-target native regression passes 254 cases with 29 explicit
backend/process skips; three startup cases and the service-registry unit pass.
Native-only and LTX feature modes, strict LTX library Clippy and static checks pass.
The exact current-source public recovery case passes against real pinned RustFS
in both image forms in 24.47 seconds. Its source voter disks and verifier cache are
erased; fresh activation, receipts, new writes and retained restart pass. It uses
embedded native Nodes and a contract fence, with the backend and dedicated VM
stopped afterward; permanent Docker launch fencing and independent voter containers
are not qualified. Source-after comparisons are unchanged.

The full native performance gate is still red: sixteen pass, including the new
collector pressure case, and two wider-group cases refuse during enrollment after
31 cells. The matrix reports ambiguous Runtime(DeadlineExceeded); isolated
32-cell diagnostics reports an elapsed deadline with a changed leader/not-ready
sample. The original budgets remain; 128 and the prior source's S3 failure point
are not reached. Preserve both earlier and current failures. Strict library
Clippy still has 222 diagnostics/202 dead_code with unchanged code/message/primary
file keys. Full-library inspection, plan and remaining CI gates stay open. See
[progress](progress.md) for exact scope; the MVP is incomplete.

Next: resolve the wider-group S3 deadline and repeated complete verification,
finish owned live manifest-page reclamation, dynamic managed archive attachment
and exact-reference S3 learner activation. Preserve the old failed sources and
bounds. The whole-group disaster operator, permanent Docker launch fence,
remaining public gates, default capabilities/deployment/read policy and CG-09–16
remain required. The goal is active and PR #160 stays draft.

## Executable consumer and image ownership checkpoint

This section supersedes the historical consumer checkpoints below. The restored
consumer port uses canonical shared ranges, explicit replicated enrollment,
logical versions and physical commit witnesses. Native and Python routing vectors
agree at occupancies 1, 32 and 128. Resource observations count actual groups and
logical cells separately. Configured archive ceilings remain separate from the
effective serial publication lane. Wider groups fit the original replay bank
using smaller tails. Local source 14 all-binary compilation clears the original
27 performance errors; final combined-source qualification is separate.

Capture CG-07-perf-resume-01 preserves the earlier failures and exact source23
executable. Python source 20 passes 282 tests with one environment skip. Native 23
has fifteen passes and two failures: both RF3 scenarios reach the one-cell
second group's S3 command (logical cell 6) and return an ambiguous DeadlineExceeded
under the original five-second command budget. Occupancy 128 is not reached.
The instrumented isolated 32 run 24 passes in 83.44 seconds, but does not clear
those failures. Its full 32-cell checkpoint verification takes roughly 4–5 seconds;
a complete background job reaches 5.206 seconds. Investigate repeated complete
checkpoint verification and cross-group archive-lane interference next. Keep
all original deadlines, quotas and explicit S3 assertions.

The materialized retention failure is corrected locally. Exact terminal command
and enrollment reconciliation retires private previews while keeping their pins
through physical completion. A fresh applied image transfers its pin into the
complete manifest graph without retaining an extra permanent host reference.
Startup recovers every physical group's current/checkpoint closure before
releasing conservative filename references, with no routes reachable yet.
Uncertain publication, checkpoint and reader pins still protect their closures.
The public native/Axum process drill 19 passes 80 additional commands before and
after retained restart using the original 64-image bank, in 166.22 seconds. Its
exact source/artifacts are retained. Extent preparation similarly makes only
fresh roots transient and preserves existing retention; all six LTX apply cases
pass, including live-reader and exact existing-retention collection checks.
This supplies correct eligibility, not an owned live extent collector or a
sustained-load qualification. The offline reclaim_cell_storage guard remains.

Qualification 25 is terminal: all seven targets pass 254 cases with 29 explicit
backend/process skips. Execution 179, storage 15, archive 30/controller 16, managed 10,
host 3 and public 1 pass. The extended public drill finishes in 135.69 seconds.
Postchecks 26 pass all binary targets, three startup cases, native-only libraries,
LTX feature-off/all-feature builds and static checks. Strict library Clippy keeps
222 diagnostics (202 dead_code), with unchanged message/primary-file keys.
Source 25's 2710-input manifest SHA256 is
8c2d708074d6c265dd69fe9edb62694495998a517900700a1cc3e97e97eefb92;
both final comparisons have no changed inputs. The failed full performance gate,
full-library inspection oracle, plan drift and remaining CI are still open.
No current-source Docker/RustFS or release claim.

Managed setup persists standby/full-range intents, reserves all physical births
before selecting ranges, initializes exact archive bindings, attaches RF3 and
enrolls logical cells. These adapters still need deployed managed qualification,
actual-leader enrollment routing, dynamic node-owned archive attachment and
exact-reference S3 learner startup. The disaster operator remains the older
per-cell workflow and is not shared-group recovery. Complete disjoint replacement
identities, whole-group seeding into public Node paths, serial namespace activation,
explicit partial inventory and permanent Docker launch revocation before its
all-voter-disks-deleted RustFS run. Preserve every original refusal, state,
receipt and restart oracle. Then finish the four missing public gates and CG-09–16.

The goal remains active and PR #160 draft. Preserve both requested MR basename
exclusions, the eight user diagrams and all failed source/artifact captures.

## Latest combined public recovery checkpoint

Read progress and the ignored public_recovery_work continuation first. Final
capture CG-07-public-recovery-14 and startup-13 have identical 2136-file Rust
mappings. Archive thirty, managed ten, archive-controller sixteen, host three,
public SDK one, counter/native-only and static checks pass. All three startup
unit cases pass; full library is 1062 passed, the same retired inspection failure
and two Docker skips. Strict lint retains 222/202 dead_code with unchanged keys;
plan and 27 performance-build errors remain. All failures and original names
are preserved.

Real-RustFS capture CG-07-public-recovery-rustfs-15 passes public recovered
startup/activation, receipts/floors, actual inventory, new writes and retained
restart in both image forms. It uses fixture fencing and three embedded Nodes
in one process, not the Docker permanent launch-fencing drill or load gate.
The owned backend is stopped with its volume retained. Inspect preservation and
publication records for exact commit, backup and any later live handles.

Next restore/adapt the 21-file executable-consumer WIP recorded below. The new
public recovery authority/activation/close owner is ready for that integration.
Finish disjoint replacement members, serial catalog activation/partial inventory,
managed archive ownership/enrollment, strict Python oracles and 128-cell budgets.
Then complete all original public gates, exact managed S3 learner activation and
source-qualified Docker/RustFS load/recovery. Keep the full CG-01–16 goal active
and PR #160 draft. Preserve all eight diagrams and both MR basename exclusions.

## Public recovered-node startup in qualification

Read the new progress checkpoint and ignored public_recovery_work continuation
first. Capture CG-07-public-recovery-11 passes the complete native archive target
(30 passed, six backend cases ignored), including public Node recovery after
source disk deletion, both image forms, held-verification close, restored S3
receipts/floors, actual physical inventory, new writes and retained restart.
The component checks at capture 11 are terminal: managed ten, archive-controller
sixteen, host three, public SDK one, counter and native-only pass. Static checks
pass; strict lint/plan/package retain recorded failures. The later joined-startup
cleanup fix passes all three startup unit cases at startup-13. Full library is
1062 passed, the same retired inspection failure and two Docker skips. Final capture CG-07-public-recovery-14 uses the identical Rust mapping as
startup-13. Archive thirty, managed ten, archive-controller sixteen, host three,
public SDK one, counter and native-only are green again. Static/lint/plan checks
and real-RustFS public recovery are still being finalized; inspect terminal
records before a final qualification claim. Keep the goal active and PR #160 draft.

The 21-file performance consumer WIP is preserved at the path recorded in
perf_cutover_work, with manifest SHA256
56a5989ea7e61312e0c265a89bbe745877e839b549ede39d9f413603680e6366.
It is not qualified or in the proposed checkpoint. Restore/adapt it after this
public recovery checkpoint; preserve the new services.recovery field and fixed
transport_query_reply_write Python metric inventory. The intermediate bin check
cleared 27 errors, but that did not complete Python/managed/disaster consumers.
Finish disjoint recovery members, serial catalog activation/partial inventory,
managed dynamic archive ownership/enrollment, Python fixtures/oracles and coherent
128-cell budgets. Do not publish compile-only legacy disaster behavior.

## Latest public physical-group controls checkpoint

Capture CG-07-group-controls-02 passes all ten managed cases, retaining all nine
parent names. The added public SDK case covers both image forms: actual physical
identity/occupancy, guarded transfer refusals and deadlines, real election,
receipt replay, old-token reads and new writes. The original membership-floor
case now holds real engine sync to prove a queued transfer expires before mutation.
Public counter inventory observes all three actual group owners.

The SDK exposes physical log observations and separate historical archive status.
Neither grants quorum, serving or compaction permission; retained entry count is
not disk usage. Guarded transfer preserves caller term/revision/target and checks
the invocation deadline at owner execution. Lost/expired replies remain uncertain
about admission. There is no extra worker, bank or raised resource ceiling.

The Rust manifest has 2133 inputs, SHA256
09f5447c42949fb573c90a8d28fc0d5fc7ecedaa3bf10efbaef705d6c19029cd.
Shared execution passes 179 cases with two ignored fixtures. The three host
cases, public SDK case, counter build, native-only library compilation and five
static checks pass. All 181 execution names remain. Strict Clippy retains 222
diagnostics, 202 dead_code, with unchanged keys; package builds retain 27
performance errors. Plan validation retains its index/Purpose and reviewed-anchor
failures; all seven recorded inputs match 824e0a20. Do not refresh baselines merely
to pass. The first failed capture and corrected source/artifacts are retained.
Full library and RustFS were not rerun; the dedicated backend VM is stopped.

Next feature priority remains the original performance/fleet consumer cutover:
physical group layout/placement, replicated cell enrollment, cell versions/tokens,
all HTTP/read/retry options and whole-group recovery. Complete the original four
remaining public gates, lifecycle failure/cancellation and retained nonvoting debt.
Then implement exact-reference S3 learner activation/archive-hold retirement and
refresh same-source RustFS recovery/load. Keep CG-07 IN_PROGRESS, CG-09–16 TODO,
the goal active and PR #160 draft. Consult group_controls_work in both ignored
continuations for preservation/publication and any later live handles. Preserve
the eight diagrams and exclude both requested MR progress basenames.

## Latest managed maintenance checkpoint

This section supersedes the historical checkpoints below. Source
CG-07-managed-maintenance-16 passes all nine managed Node cases (102.06 seconds),
retaining all eight earlier names. Both image forms complete repair, maintenance stop submitted by the survivor leader, actual Node join/signed offline proof, retained paused return,
fresh resume, terminal capacity/disruption settlement and returned-leader writes
for initial and joined members. Duplicate receipts and old tokens survive. Direct
paused writes, old-boot stop and wrong-child same-new-boot resume refuse; the wrong
child preserves PayloadConflict, an unchanged root and paused admission.

Implementation adds bounded complete physical-group leader-load sampling, exact
returned-boot retained startup without genesis/missing-claim creation, and a
barrier-captured temporary application refusal. Effect scheduling handles the
resume race without quarantining a temporary refusal. Peer maintenance stop/resume
release their transport waiters after exact rooted preflight and bounded worker
enqueue, returning Unconfirmed while fresh proof and guard sync remain Node-owned.
This breaks the three-RPC/two-slot metadata dependency without raising capacity
or accepting queued work. Local/non-maintenance semantics stay intact. Reconcile
uncertainty through actual joined closure or fresh resumed proof, using the same
rooted intent; a submission storm competes with the worker's proof budget.

The final Rust manifest has 2130 files, SHA256
55e9323085d42f58d0fa77d5eb4b806eb8412e3528c7e129aab14a74b04b789d.
Broader qualification is terminal. Catalog 23 and shared execution 179 pass;
two original owned process fixtures remain ignored and all 181 names remain.
Unit build, scan five, admission three, fleet 28 (two existing Docker Engine
cases ignored), fleet-action wire six and query-wire twenty pass. Full library
is 1061 passed, the same retired inspection-protocol failure and two ignored.
All 1059 parent names remain with five scan cases added. Host three, public SDK
one, counter/effects builds, retained effects smoke, locked native-only library
compilation and five static checks pass. Strict Clippy remains 222/202 dead_code
with unchanged diagnostic keys; package builds retain 27 performance errors.
Plan validation remains 35 passed, one generated index/Purpose failure and one
reviewed-anchor error, with all seven recorded inputs unchanged from 004d1051.
No baseline, assertion, timeout, quota or stack was relaxed. Local opt-level-zero
profiles keep assertions/overflow checks; effects smoke uses the SQLite store.

Consult managed_maintenance_work in both ignored continuations for final checks,
immutable evidence, publication and any later owned handles. All current checks
are terminal. Preserve failed captures, source/artifacts and the eight diagrams. Both requested MR progress basenames
remain excluded. No final-source RustFS, independent-host, load or release claim.

Next feature priority is original executable consumers and public lifecycle/
ingress/occupancy/feature-off acceptance, including the 27 performance compilation
errors, retained nonvoting inventory and lifecycle failure/cancellation
ownership. Then implement exact-reference
S3 learner activation/archive-hold retirement and refresh same-source RustFS
recovery plus replication/reclamation load. The retired inspection oracle, strict
lint and plan anchor/index gates remain required. CG-09–16 retain their complete
scope. Keep the goal active and PR #160 draft; do not merge.

## Latest managed peer replacement checkpoint

The follow-up on e8f9352d exports the nested startup cause through the SDK and
adds managed_group_replacement_and_joined_restart. All eight managed cases pass;
all seven original cases remain. Both image forms qualify actual peer seeding,
RawNode catch-up, joint finish, durable settlement replay, retained removed-member
physical debt, actual leader handoff, survivor writes and canonical retained
joined-member startup. Duplicate receipts, old tokens and new writes survive;
the old target-boot grant refuses before submission with no root mutation.
No runtime guard, timeout or quota was changed. The fault drill reproduces a
committed terminal CAS with lost write/confirmation replies. Provider I/O from
that settlement now becomes Unconfirmed at the movement reconciler boundary;
the next invocation resolves its permanent receipt. Corruption/authority errors
retain their typed failures. Initial promotion NotReady is also uncertain;
reconcile the same admitted intent and retain its debit.

Final checks pass the library build, 28 fleet unit and six wire cases (two existing Docker Engine cases ignored), native-only
library compilation and five static checks. Strict Clippy remains 222/202 dead_code
with the same diagnostic keys; package compilation retains 27 performance errors.
The added plan-validator run exposes two pre-existing failures: reviewed anchor
drift and the generated index/Purpose mismatch. Their inputs match e8f9352d.
Review the anchors and generator contract; never refresh hashes merely to pass.

This remains a local TLS/SQLite component gate with fixture catalog/attestations.
It does not complete managed_group_lifecycle. Next implement/qualify the rooted
maintenance gate, readiness, owned stop, offline proof, retained return and
fresh resume/capacity settlement for a joined member, including failure and
cancellation ownership. Then port original fleet/performance consumers, repair
the retired inspection oracle and strict lint/build gates, implement exact S3
learner seeding/archive holds and refresh same-source RustFS recovery/load.
CG-09–16 remain required. Keep the PR draft and goal active. Consult
managed_lifecycle_work in both ignored continuations for captures, immutable
artifacts, preservation, publication and any owned live handles. Preserve all
eight user diagrams and exclude both requested MR progress basenames.

## Latest cold managed group checkpoint

The follow-up on b9be4de7 implements explicit standby inventory and exact-root
first range selection, journaled import freeze, catalog-owned namespace execution
and boot-bound capacity at initial birth admission. An Active hint cannot reuse
old-boot capacity. The negative admission case preserves the root; fresh actual
Ready/Active admission permits the identical proposal. Unselected reservations
retain their physical references.

Final source CG-07-cold-managed-group-06 passes all seven managed Node cases and
23 catalog cases. Both image forms complete RF3 genesis, two independent cells,
joined close, canonical retained startup, duplicate receipts, old-token reads and
new quorum writes. Deployment bounds are explicit fixture attestations. Full
shared execution is 179 passed, two owned process fixtures ignored. Twenty wire
cases, three host cases and one public SDK case pass, along with counter/effects
builds, the original retained smoke, native-only library compilation and five
static checks. Full library has 1056 passed, one retired inspection failure and
two ignored. Package builds still fail on 27 performance errors; strict lint remains 222 diagnostics, 202 dead_code.

All qualification jobs are terminal. Consult cold_bootstrap_work in the ignored
continuation for immutable artifacts, source hashes, failed attempts, preservation
and publication. Next feature priority is joined-member startup and whole-group
repair/drain/maintenance/stop/return/resume with real debt/accounting, then original
consumers/public gates, exact-reference S3 learner activation and current-source
RustFS recovery/load. The full managed lifecycle and CG-09–16 remain required.
Keep PR #160 draft and the end-to-end goal incomplete. Preserve all eight user
diagrams and exclude both requested MR progress basenames.

## Latest shared membership and managed ownership work

The user-ordered merged-source checkpoint is published to draft PR #160 at
2e400133 on origin/main 981a5417. This follow-up implements the shared
finite Ready/send fence, exact physical message admission retention, committed
configuration announcements and joined managed boot/directory/Node stop owners.
The entered-socket regression reproduced a send-fence/unreachable-hint cycle;
source-04 releases the source permit after physical exchange before advisory
feedback, retaining each actual checkpoint producer independently.

Source-04 passes full shared execution179/0/2, managed component3, membership9/0/1,
wire20, host3, public SDK1, counter/effects builds, original effects retained smoke
and five static checks. Full library1055/1/2 retains only the retired inspection
protocol failure. Strict Clippy222/202dead remains a failing gate, with no newly
unused methods; performance compilation still has27 errors. No release/MVP claim.

Final review reproduced rejected advertised endpoints and an invalid log owner
consuming a rooted boot. Source-06 validates node/disk before engine opening and
all immutable descriptor fields before pin/boot mutation, preserves advertised
addresses separately from bind addresses, and passes all five managed Node cases
plus the membership selection (nine passed, one owned child fixture ignored).
The final broader chain is terminal: 179 execution cases passed, two child
fixtures ignored; library 1055 passed, one retired inspection-protocol failure
and two ignored; wire 20, host three and public SDK one passed. Counter/effects
builds, the original retained effects smoke and five static checks pass. The
locked native-only cellar/cellar-python library check passes with default
features disabled and no Python runtime. Strict Clippy retains 222 diagnostics,
202 dead_code, with the same code/message/primary-file set as source-04.
Component Cargo builds still fail on 27 performance-binary errors.
Consult managed_fence_work in the ignored continuation for exact immutable
artifacts, failed attempts, backups, publication and any remaining owned handles.

Next feature priority: nonempty managed group lifecycle and legitimate cold
range/inventory/capacity/birth bootstrap, then original consumers/inspection,
exact-reference S3 learner activation, remaining public gates and current-source
RustFS recovery/load. Active-node fixture input and metadata-only standby startup
cannot establish that full gate. CG-09–16 remain required; keep PR draft and do
not merge. Preserve eight untracked diagrams and exclude the requested MR JSONs.

## Latest merged-source qualification and requested next work

Local merge 9df1389f contains counter checkpoint 41ab8b4d and origin/main
981a5417 (PR #165). Capture CG-07-public-counter-03 passes the rebuilt counter,
original public SDK case, twenty query-wire cases, three namespace-host cases
(both image forms), effects build/original retained smoke and all five static
checks. Full normal library is 1055 passed, one failed and two ignored; the
failure remains the retired inspection-cancellation protocol. Performance binary
compilation and strict lint remain separate failing gates; see progress for
final source-bound diagnostics. No current combined RustFS/CI matrix claim.

The user requests finishing this qualification and updating draft PR #160 before
implementing the shared membership fence, managed boot, owned directory refresh
and Node stop services. The upstream fence changes the retired owner only; the
shared runtime must retain prior messages through physical sends and freeze a
finite Ready boundary before membership publication. Managed startup must bind
and sync the exact boot before group attachment/publication, use managed group
permits, join directory work on shutdown, and install Node's existing stop owner.
Keep all original guards until their real replacements and tests exist. Preserve
the remaining public cases, S3 learner activation and CG-09–16 scope.

Newest state: local counter checkpoint41ab8b4d and merge9df1389f include newly
fetched main981a5417 (PR165). PR160 remains at published fc4a8d31; do not describe
the new merged source as published or qualified. Three merge conflicts are
resolved without restoring the predecessor dispatcher or weakening assertions.
Post-merge source03 qualification is next. Record/poll its exact owned handle;
do not edit Rust while live. Source02/earlier evidence retains its original base.
Audit the upstream membership-send fence against the separate CellGroupOwner
Ready/effect/send ownership before qualifying managed moves. The next complete
feature remains actual managed boot/directory/stop integration and dependent
consumer ports; four original public gates and all backend/CG09–16 scope remain.

Latest unpublished work on fc4a8d31 ports the public counter to one shared group
and two independent cells per node, with explicit public namespace/archive setup,
replicated enrollment and typed native/Axum projection. No production verification
imports or per-cell registration remain in that executable. Its original smoke
passes three full drills; the extra HTTP contract and missing-catalog guard pass.
The native_cell_groups_public target now exists and passes the original
sdk_many_cells_share_group case. The other four public cases remain missing;
do not replace their managed/admin/feature-off oracles with the counter result.
Source-02's overall native Cargo invocation still fails on twenty-seven old
performance API errors despite emitting its fresh public test executable.
Current-source checks: wire20 pass; host3 pass across both image forms; normal
library build and original two-run effects smoke pass. Full library1049/1/2 retains
only the old inspection-cancellation failure. Strict library Clippy234/214dead_code
is unchanged by code/message/primary-file comparison. All five static checks pass.
The next feature dependency is actual managed startup: registration refuses
managed namespaces/node-directory options; Replica bootstrap is unmanaged; Node
startup returns no Fleet stop services. Preserve capability digests, exact
catalog/node/disk/process/storage-instance boot binding, synced lifecycle guards,
permanent claims and fresh group authorization when wiring those owners. Do not
make fleet/performance callers compile by dropping their managed behavior. Read
public_counter_work in both ignored continuations for exact source/artifact hashes,
retained setup failure, completed cases and owned live handles. Do not edit Rust
while an owned build/test/format job remains live. No timeout/quota/stack increase
or assertion weakening is permitted. This newest section governs over historical
checkpoints below; the goal and CG-07 remain incomplete.

PR #160 is the authorized draft checkpoint; do not merge. Preserve/exclude the
user's eight diagram files and both MR progress basenames. Keep the canonical PR
checkout and isolated effects checkout aligned after publication. All CG-01–16
requirements remain intact, and the end-to-end goal remains incomplete.

Start with [remaining work and priorities](remaining-work.md). The next MVP work
is the focused public SDK surface and counter/native/Axum/performance/fleet
consumer ports, then managed directory/startup and whole-group lifecycle,
exact-reference S3 learner activation/archive holds, and all original public,
build and current-source RustFS/DR/load gates. Transfers, split/merge, placement,
hibernation, managed recovery and full load/release qualification remain required.

Latest source adds controller-owned public replica candidates, per-attempt
limits and an untried observed leader fallback under the original invocation,
input, minimum token, deadline and attempt bank. Authenticated reply advice is
not authority. Incoming RPCs execute locally. Queries retain physical SQL and
transport ownership after waiter expiry; responses expose the actual replica.
A reproduced SQL deadline-envelope race is fixed without increasing timeouts or
changing ordinary storage refusals.

Fixed-08 passes twenty-one focused cases and the complete native execution suite:
177 passed, zero failed, two ignored. Fixed-07's forty routing repetitions apply
to unchanged production/new cases. The library has 1049 passed, one failed and
two ignored; all twenty wire cases pass. Host-fixed-09 passes all three host cases
across both image forms. Strict library Clippy still has 234 diagnostics, with no
additional code/message/primary-file diagnostic versus the prior checkpoint.
The overall native Cargo command still fails on unconverted counter/performance
APIs. Do not present the emitted component artifact as a package pass.

The effects example now retries only definite-unsubmitted readiness refusals
under the same command ID/input/deadline, and lets Node drain before inspecting
Raft's final report. Original close assertions and the original smoke are intact;
close diagnostics are removed. All six original smoke trials pass at
drain-fixed-12, twelve process runs including retained-disk restart.
Production/library Rust matches fixed-08; the only later Rust
changes are the fixture's owning Entry import and this example. Effects smoke
uses the SQLite development object store. Current combined RustFS, managed DR,
load and public orchestration remain unqualified.

Read public_replica_routing_work in the ignored continuation for exact source
hashes, immutable executables, retained failures, backup and owned handles. Finish
every owned job before staging Rust. An observation timeout is not failure; never
restart a live build/test merely because output has not arrived. Historical
sections below describe scoped checkpoints; this newest section governs.

Latest unpublished WIP fixes retained-preparation reuse and the completion phase
of an already synced seed. Boundary probes reproduced the previous three native
failures: repeated sync of the same retained page root, and a source-process
watch rejecting completion after persistence succeeds. The probes are removed.
The original LOCK, no-extra-sync, queued-revocation, uncertainty, recovery and
fresh-quorum assertions remain. An added entered-fence expiry case preserves the
original deadline and checks cold recovery of the actual completed write.

CG-07-seed-retained-fixed-02 passes all three formerly failing native cases,
including both image forms and the added entered-fence expiry cut. Only that new
case has a two-second operation deadline inside the unchanged peer five-second
guard; original cases retain ten seconds. Its complete native execution suite
passes: 167 passed, zero failed and two ignored in 452.06 seconds. All owned
build/test/check jobs are terminal. The full WIP, source inputs and immutable
executables are preserved under the normal checkout-specific qualification target.
Fixed-01's production/library and example Rust are identical to fixed-02. Its
normal unit target and effects binary build; original two-run effects smoke and
five static checks pass. Full library: 1048 passed, one failed, two ignored
(retired inspection protocol). Strict Clippy: 235 diagnostics, including 215
dead_code, with none in the six production files changed for these fixes.
The overall native Cargo command still fails on fifteen counter-binary API errors.

The earlier gateway-backend-fixed-03 checkpoint has 164 passed, three failed and
two ignored; all 21 targeted gateway/cold cases and 39 lifecycle cases pass.
Fixed-02 resolves those remaining three failures and passes the complete native
suite. This component result does not replace the failing package/library and
strict Clippy gates.

Canonical archive startup creates/syncs its scratch parent before archive work.
The effects example retries typed busy/overload with the same command identity,
input and original deadline. Eight retained runs pass at the earlier fixed-02
source and two more at gateway-backend-fixed-03. These are SQLite development
object-store smoke runs, not RustFS, managed recovery or load qualification.
Fixture backend declarations and cold oracles retain exact immutable namespace
metadata. Runtime/domain/schema/backend guards remain unchanged.

Next port public counter/performance binaries and real managed shared inspection
cancellation,
finish canonical managed directory/startup and exact-reference S3 learner
activation. All five original CG-07 public gates, refreshed real Docker-fenced
RustFS recovery/load qualification, CG-09 through CG-16 and the combined hard
cutover remain required. Strict Clippy and the old inspection cancellation test
are open. At that earlier checkpoint the WIP remained unpublished; no final
system/MVP completion was claimed.
Historical sections below describe scoped checkpoints, not combined qualification.

The executable design and implementation plan is delivered. This handoff scopes
the remaining end-to-end implementation and hard cutover, including the full task
and release gates, for subsequent execution.
This handoff does not claim completion of the
shared runtime, a production cutover or measured TiKV
performance parity. The target is the same partitioned multi-Raft pattern: many
whole cells per group, one Raft machine per group replica, and many groups sharing
node resources. A group spans voter nodes; it is not all cells on one node.

## Starting point

Reuse codex/shared-cell-groups and [PR 160](https://github.com/crabbuild/cellar/pull/160).
Keep the isolated effects checkout and PR checkout aligned at each published checkpoint.
Preserve the user's untracked diagram and both excluded MR progress basenames.
The current goal remains incomplete: the SDK now uses the shared host, while
managed startup, remaining public consumers and the final combined release/load
envelope are unqualified. Historical sections below describe earlier checkpoints;
the newest progress section governs current implementation and validation.

CG-01 through CG-06 are DONE at their scoped native boundaries. Their refreshed
records remain under .cellar/cell-groups-evidence/CG-01-06-gates-e32115aa-02/.
CG-06 also passes the actual Docker-fenced, all-voter-disks-deleted RustFS drill
in both image forms at f8aacd05. Fresh history/genesis/quorum activation, receipt
restoration without replay, S3 minima, old-launch refusal, new writes and retained
restart are qualified at that unmanaged local boundary. Its Docker record is
.cellar/cell-groups-evidence/CG-06-docker-f8aacd05/qualification.json.

CG-08 is now DONE at 4ea70e483e84ca88d77ca062278edb5ff2389646. All twenty-two current-source commands
pass, including all ten original required commands, ten exact native effects
cases, twenty-four library regressions and the full shared execution target
(164 passed, 2 ignored), and the affected archive-controller suite
(16 passed, 20 explicit RustFS ignores). The same-source/executable real RustFS case passes
both image modes with seven signed probes and independent full archive replay.
The backend and dedicated VM are stopped, data retained and default context
preserved. Source preflight and unchanged task evidence validation pass.
Record: .cellar/cell-groups-evidence/CG-08-gates-4ea70e48-01/record.json
SHA256: a072fb6fd8957b6afd5fdf4dffc88c312a0e34c2e1f5c4cfd81b4d2456ea9d0c
Executable SHA256: 85ceb50508a519e2b1fd5198f47969185438f77c30d09e9de6ab884fd6a7cce6
All source hashes, real streams, original capture record and executable are
retained locally and backed up. See [progress](progress.md) for scope and failures.

The namespace-host component now passes its ten-command checkpoint. It has
bounded namespace/class registrations, history-derived SQL paths, logical cell
handles, controller-owned queries and original enrollment deadlines. All three
new host cases pass both image forms, including eight cells in one physical group
on every voter and held-query shutdown. Execution/archive/effects regressions and
strict checks pass. Evidence is under
.cellar/cell-groups-evidence/CG-07-namespace-host-01/record.json.
This binds already bootstrapped services; canonical startup, public SDK routing,
ingress and managed whole-group lifecycle remain unqualified. Move to those
consumers next, using this component rather than another per-cell binding owner.

CG-07's synchronous command-admission checkpoint now passes all nine focused
commands: the exact new regression, full execution/effects/archive suites,
all-target strict Clippy, format/layout and dependency/docs checks. The record is
.cellar/cell-groups-evidence/CG-07-command-admission-fixed-02/record.json.
Its source patch, 4680 source hashes, three executables and actual streams are
retained and independently backed up. This is a shared-runtime prerequisite;
the public SDK/fleet cutover and all original CG-07 acceptance cases remain open.
Next implement the canonical namespace/class registry and shared host with logical
cell handles, then complete managed whole-group lifecycle and accounting.

The latest managed routing/repair work converts Begin/Promote/Finish to actual
physical-group controls (Abort is also implemented). Rooted history, permit/plan,
phase/revision and full occupant placement checks precede normal configuration;
actual durable apply plus a fresh quorum precede success. Native RawNode catch-up
and original deadline/uncertain-result rules remain. At that historical checkpoint Activate was still the old per-cell consumer.
The later peer learner work has converted it; S3 exact-reference transfer and
current-source qualification remain open, without peer fallback for S3.

Managed route discovery projects committed joining bindings through rooted node
views. Configuration apply/checkpoint install trigger reconciliation; ordinary
known-peer traffic and cell writes keep their hot path. One missing-target retry
is bounded before frame admission. Stable physical/checkpoint factory lifetimes
survive advisory refresh; individual transfers retain exact route generations.
Explicit replacement/removal still revokes old owners and stale refreshes. New
factory views are selected once per transfer, not per chunk. Two new parked-engine
registration regressions are uncompiled/unexecuted. The existing query-auth test
body is byte-identical after moving its fixture to a reusable sibling.

Final CG-07-managed-routing-advance-04 retains 4726 inputs (SHA256
bfdbc3c3ff638d7cfd20b8034626e825e33c9fd09e3e9be78beb8713e6f14053)
and the same 36 inherited library errors, with no new route/repair diagnostic or
non-vendor warning. Intermediate type errors and their fixes remain recorded.
The standard unit-test compile attempt, CG-07-managed-routing-test-01, stopped at
that ordinary-library failure before test modules compiled; it ran no tests.
Read the newest progress section and exact owned handles before editing. Full
managed startup, all five original gates, fresh native/RustFS/load qualification,
archive hold retirement and CG-09–16 remain open. Do not publish unqualified WIP.

The latest managed-genesis implementation replaces the receiver's old cell
registration/activation with activate_managed_initial_group. Catalog I/O enters
the installed execution domain even when polled elsewhere. The SDK rechecks its
namespace capability, rooted birth and actual initial voter plus synced boot,
then delegates whole-group recovery/attachment to the existing provisioning
owner. It returns only the actual durable fleet guard, never a quorum/serving
certificate. Original deadlines, late-reply suppression, owned physical joins,
retained-preparation retry and current-roster/shared-engine failure checks remain.
Attached retries validate without reopening pages or adopting the existing
registration lifecycle. Immutable namespace/directory/backend profiles share the
node image inventories and a single preparation/parallel bank. Their caches and
fixed peer metadata are charged within the original total. Canonical startup
configures these explicit resources; the managed startup gate is not lifted.

The first two locked builds report 36 errors rather than 40, retiring all four
genesis receiver errors with no direct new code diagnostics or non-vendor
warnings. Final capture CG-07-managed-genesis-advance-04 includes original
unconsumed-reply expiry, the existing 60-second maximum and typed catalog-error
preservation. Its terminal result also has 36 remaining errors and no direct
new diagnostic/non-vendor warning. All 4723 final source inputs are retained
(SHA256 25b5a30439963dea58e34d16208b4f2572f37d6fa257c1c35dc2c8afca4ae3c6),
and all captured Rust inputs match at validation closure. All four compiler
handles are terminal. Final formatting, layout, dependency isolation,
documentation and diff checks pass; all owned validation processes are terminal.
Static review retains all 43 original assertions plus seven new profile refusal
assertions, without executing or compiling them. Read its review-resolution.json
and the newest progress section before editing; no Rust edit while an owned
build/check is live. Existing provisioning
behavioral assertions remain, with namespace profile/backend registration and
new early-owner/budget/duplicate refusals. The tests are still uncompiled and
unexecuted. Next use these profiles/owners for managed peer/S3 learner activation,
real group repair and trusted target routes, then remaining legacy retirement,
node-directory/managed startup and all five original public gates. No managed
native/RustFS qualification or full-goal completion is claimed.

The latest path correction makes canonical startup, controller provisioning and
independent archive replay share ManifestPageStore::group_pages_path. Serving
owners use groups/<cluster>/<namespace>/<group>/<incarnation>/pages under the
same node base; the verifier still has an independent base. Existing physical
fault/restart test bodies are byte-identical outside their independent expected
path helper. Their original assertions remain unexecuted and uncompiled. The
workspace-volume locked library check still has the same 40 errors, with no new
primary path-code error or non-vendor warning. CG-07-provision-path-advance-01
retains 4722 source inputs (SHA256
87cdfd5273f790446ac1753f5593bdfe88d3e5ee8f0a6fb2a095298e5496bad3).
Final formatting, layout, dependency isolation, documentation and diff checks
pass; all owned validation processes are terminal. The new path is implemented
but unqualified. Continue namespace-bound image
profiles and existing controller-owned managed activation/registration, followed
by real repair/genesis consumers, old caller retirement and the original gates.

The newest seed implementation adds prepare_managed_group_seed on the existing
shared complete-checkpoint stream. Fresh root admission binds the active target
reservation and the selected source's member/node/disk/process boot. Authenticated
metadata must contain the exact Seeding permit/plan; revisions cannot regress and
catalog time consumes the original deadline. The serialized engine checks its
last synced destination boot at stage and before install fence. Late delivery
cannot erase durable receipt/debt or report late startup success. Managed joined
bootstrap/revalidation requires the actual receipt, permanent claim, rooted Live
group and exact running boot. Both genesis-guarded and later-enrolled source
origins retain normal successor checks. Unmanaged entry points check namespace
ownership. Catalog preflight/staging/joined delivery use the original transport
deadline plus process/shutdown cancellation; catalog and handshake time is deducted
before forwarding, and ownership reads stay inside directory operation bounds.
All three checks report the same 40 outstanding
legacy errors, with no direct new seed/storage/bootstrap/source-auth diagnostic
or non-vendor warning. All 4722 captured source inputs are retained and all Rust
inputs matched the final CG-07-managed-seed-advance-03 at its validation point. The workspace volume is writable
again; the reviewed build/checks return to its normal checkout-specific target.
The final formatting, layout, dependency, documentation and diff checks pass;
all owned validation processes are terminal.
Five new descriptive profile/origin/guard regressions are unexecuted and grant no
physical/quorum proof. No managed/native/RustFS qualification follows. Read the
newest progress section and invariant-source-review.json. Next use existing
controller provision/registration ownership for managed learner activation, add
actual group repair begin/advance and managed genesis consumers, finish managed
S3 learner restore, remove old seed/inspection/host callers and pass all five
original CG-07 cases. Hold retirement, refreshed native/backend gates and CG-09–16
remain open. The subsequent path correction described above unifies provisioning
and canonical startup. Its one image-backend profile still needs immutable
namespace-bound profiles sharing node admissions. Retain/extract the common
seed_transport deadline/process/shutdown helpers when removing old seed RPCs.
Do not enable managed canonical startup or publish before qualification.

The newest query cutover removes retired per-cell SDK query admission/types,
host query execution, routing loop and request/reply body codec. QueryRoute now
accepts only a physical group plus logical cell, refusing the old Cell variant.
Current query/command/enrollment/receipt framing remains purpose separated. Common
MAC/framing, exchange, origin permission and deadline/cancellation/revocation regions
are byte-identical to baseline; the source review is not executed qualification.
Wire security oracles now use logical tokens/physical routes. The original grant
oracle uses synced shared engines/start_shared transport and retains all 18
assertions; a new regression requires old route/request/reply refusal. Tests are
unexecuted. The sequential locked check falls from 65 to 40 errors, with no primary
query-path error or non-vendor warning. Remaining host.rs errors concern predecessor
startup services. All 4719 captured source inputs are retained and all Rust inputs
still match the reviewed final CG-07-query-cutover-advance-03 capture. All three
sequential checks report 40 errors. Review restored exact returned-token equality
in the ported wire oracle before the final formatted source; tests remain unrun.
Format/layout, native dependency,
documentation and diff checks pass; all owned validation jobs are terminal.
The workspace target remains EPERM;
only this checkout's ignored temporary system-disk target is reused. No new tests,
Clippy, RustFS or managed qualification is claimed. See the newest progress section.
Next finish actual complete-group managed repair/learner seed and genesis owners,
remove old seed/inspection/host consumers, install directory/startup ownership and
pass all five original CG-07 cases. Convert old public read/Axum fixtures without
weakening oracles. Hold retirement, refreshed native/backend gates, CG-09–16 and
the release matrix remain open. Do not publish unqualified WIP or mark the goal done.

The newest action work routes fleet execution through the installed shared SDK,
with synchronous original-deadline/control admission, exact synced boot checks,
internal observers and unchanged authenticated remote receiver checks. Restart
handoff/resume now address GroupHistory. Actual serialized group owners recheck
RF3 term/revision/committed and survivor matched/active progress, plus the exact
synced quiescence selector for transfer. Expiry is tightened to the original call;
late entered work stays uncertain. Obsolete per-cell fleet action dispatch is
removed and restart reporting carries physical histories. The first diagnostic
falls from 90 to 70 errors; after fixing stale report types the final rebuilt
library reports 65. All captured Rust inputs match; no primary diagnostic is in
the converted action/readiness/handoff/resume paths. Four new regressions are
unexecuted and no runtime/backend qualification follows. Format/layout, native
dependency, documentation and diff checks pass; all owned jobs are terminal. Read
CG-07-group-actions-advance-01/-02 and the newest progress section. The workspace
volume still returned EPERM after cleanup; the final rebuild uses a fresh ignored
checkout-specific system-disk target, as disclosed and recorded in environment.json.
Preserve the original evidence and user diagrams. Next remove legacy query
consumers while retaining shared authenticated framing/security oracles; finish
actual whole-group managed learner/repair/genesis and installed directory/startup.
All five original CG-07 cases, explicit hold retirement, refreshed native/backend
qualification and CG-09–16 remain required. The goal is incomplete.

The newest fleet decision core uses physical GroupHistory throughout reservation,
topology and isolated repair, with GroupRepairPlacement results and no cell-name
identity. One replica costs one engine slot while full image/staging/memory debt
remains charged. Namespace-qualified group IDs stay distinct; two current
incarnations cannot coexist in topology, and a new incarnation cannot reuse or
erase old reservation debt. Docker recovery now takes RangeDirectory handles and
checks rooted group authorization before repair observations. All 96 enabled core
tests and strict all-target Clippy pass, including three new identity/debt cases;
the first missing-import build failure is retained. Eight binaries and 4718 source
inputs are hashed under CG-07-group-model-advance-02. The runtime diagnostic falls
from 100 to 90 errors, with no primary error/warning in the converted paths and no
captured Rust input changes. Format/layout, dependency, documentation and diff
checks pass; all owned validation jobs are terminal. No public SDK, managed,
Docker or RustFS qualification follows from pure core tests. Preserve shared TLS query/command/enrollment/receipt framing while
removing the old per-cell query consumers, then complete group action RPCs,
directory ownership, startup and all original CG-07 gates. Persistent hold
retirement, refreshed native/backend gates and CG-09–16 remain unfinished.

The newest implementation replaces both managed removal verifier callers with
complete group replay and explicit private physical inventories/shared pools.
A persistent acquire journal chooses one intent before S3 mutation, restores the
same fixed cut after restart, and reconciles its exact allocation after lost
responses. A new intent needs a fresh registry proof that the previous revision
can no longer commit. Receiver admission precedes page scans and follows physical
ownership through cancellation. Format passes. The locked diagnostic falls from
112 to 100 errors, with no primary error/warning in the new paths. All captured
Rust inputs still match. Read CG-07-managed-archive-advance-01 and the newest
progress section. Format/layout, dependency isolation, documentation and diff
checks pass in the corrected toolchain environment; all owned jobs are terminal.
Five new regressions and Clippy are unexecuted while the library fails. Persistent hold retirement remains explicit and
unfinished; do not release on drop or borrow a publisher hold. Next close remaining
group models/action RPCs/legacy consumers/directory/startup, convert old verifier
fixtures, and pass all original CG-07 and refreshed native/backend gates. CG-09–16
and full release qualification remain open. No runtime changes are published.

The newest work cuts fleet quorum RPCs over to the actual shared SDK gateway
and complete physical group roots. Published cuts commit full membership plus
configuration; authenticated subtree mutation summaries distinguish data changes
from protocol-only progress. Fleet archive verification now uses complete group
checkpoint/tail replay and exact persistent holds. Live receipt payloads are
hash-addressed under the existing page bound while index rows and batch byte
ceilings remain unchanged. Per-cell fleet quorum requests/read variants and the
old host RPC implementation are removed. The first library check has 120 errors;
the second and final checks each have 112, with no primary error in the new
metadata/coverage/summary/receipt modules. The final check includes configuration
order and descriptor ownership review. All 4714 source inputs are captured and
all captured Rust inputs still match. Read CG-07-fleet-root-advance-01/-02/-03
source captures and the newest progress section. Format/layout, dependency,
documentation and diff checks pass. Regressions and Clippy are unexecuted while
the library fails. All owned jobs are terminal. No native/backend qualification
or publishable runtime is claimed.
Next implement the owned hold-acquire journal and full replay receivers for both
managed verification adapters; do not reintroduce a single-image archive or fake
scope. Then group placement/repair, action RPCs, node directory, managed startup
and all original CG-07 gates. The changed marker-1 root shape needs fresh full
qualification; earlier CG-06/08 backend evidence remains historical/source-bound.

The newest SDK work adds physical node inventory and authenticated inspection
from the real shared engine/roster, preserving pending provisioning debt,
quarantined/unattached slots and original deadline/budget ownership. Transport
routing uses a weak shared-SDK gateway and the actual installed trust cut. Local
node profiles can now be configured; managed startup is still gated. The first
sequential diagnostic reports 124 errors with no primary error in the new node
metadata modules. The second locked check also reports 124 errors with all
captured Rust bytes unchanged. Follower-safe membership metadata is implemented: its distinct
ReadIndex context binds the origin member and accepts replies only from the
current leader, while execution/seed contexts and follower execution refusal
remain unchanged. The new two-image, three-engine regression is unexecuted while
the library fails. Read CG-07-node-inventory-advance-01/-02 captures and the newest
progress section. Next convert the whole-group fleet observation/publication
codec and complete-root archive/removal proofs, then repair/restart RPCs, the
installed directory owner, managed startup and all five original CG-07 cases.
This is local unqualified implementation, not a native/backend release gate.

The newest SDK work adds guarded physical-group enrollment and permit fencing
with typed refused/uncertain outcomes and a fresh quorum observation after durable
apply. Minimum membership reads defer ReadIndex until apply and re-arm after a
configuration overtakes the barrier, retaining their original budget/deadline.
A fixed control-service registry joins actual movement/restart worker lifetimes
before consensus/storage shutdown; internal observers do not retain the last
external SDK lifetime. Startup checks installed host/boot/lifecycle identity.
Managed canonical startup remains gated. The first diagnostic reports 165
errors; the final captured check reports 157 and no direct diagnostic in the new control/read/service modules. The new
SDK now retains the real transport and sends exact-boot stop/fence RPCs on its
existing authenticated lane, with original deadline and control admission
captured before returning the waiter. The receiver owns admitted mutations and
no cell/group alias is chosen. Captures are CG-07-physical-controls-advance-01 and
-02. The new real-sync and service ownership
regressions are unrun while the library is failing; read the source capture under
CG-07-physical-controls-advance-01 and newest progress section. Do not treat this
as native or RustFS qualification. Next implement the physical RPC/inspection
owners and whole-group archive/removal proofs, then managed startup and all five
original CG-07 acceptance cases.

The latest spare work replaces namespace import's cell inventory with an
admitted physical-group set and an immutable frozen topology cut. It settles
unselected pending births without dropping debt, verifies whole-group quorum
coverage and every selected range, and performs exact-owner adoption plus a
revision-advancing topology unlock. Managed restart birth settlement and the
maintenance/resource/operator inventories now take physical directories. Public
movement/debt views omit a cell field; standalone connection requires explicit
group_directory limits. The first locked diagnostic has 194 errors; its joined
method mismatch and unused re-exports were corrected before the follow-up.
The second diagnostic has 177 errors and no own warning; the import modules
emit no direct diagnostic. Resource/movement/topology/repair models now use
GroupHistory, with final capture CG-07-group-import-advance-03. The new real-CAS
freeze regression remains unrun until the canonical crate builds. Read the three
captures and newest progress section. No
new managed runtime, RustFS, load or release qualification is claimed. Continue
with physical SDK/control RPC owners and whole-group archive/removal proofs,
then managed startup and all five original CG-07 acceptance cases.

Review found that several admitted candidates can share a logical range and
epoch. The fleet range-debt key now includes physical group/incarnation identity;
the directory retains exclusive selected-range ownership. A new invariant test
covers distinct pending keys at the same range/epoch. The third sequential check
reported 174 errors with no own warning before that fix; the final reviewed
source capture is CG-07-group-import-advance-04. The final locked library check reports 174 remaining integration errors;
all captured Rust bytes remain unchanged. The new admission, freeze, import and
inventory modules emit no direct diagnostic, which is not qualification. Both
new regressions remain unexecuted until the canonical library builds.

The newest spare work converts fleet bindings/birth records to physical group
histories and ranges, adds guarded managed genesis and exact local-boot startup
ordering, and adds a fresh ReadIndex membership observation independent of cells.
Inventory traverses complete physical range pages and retained reservation debt.
Earlier cell-bearing records refuse under the new marker-1 protocol identity.
The first diagnostic reported 202 errors and overlapped formatting; it is retained
without qualification. The latest sequential check reports 198 remaining errors
after the later inventory, quorum, boot, typed-error and runtime-history edits.
All new core assertions remain unexecuted, managed startup is
still gated, and old import/cancellation/inspection/movement/restart consumers
must be converted. The separate primary directory pager passes all seven checks
at 66d5ea6a, including nineteen native catalog cases. Its reviewed baseline
passes all four post-gate checks; PR 160 and both checkouts are at c54307c5.
All WIP and diagrams are preserved; managed cutover changes remain unqualified. See the newest progress section and ignored
CG-07-managed-group-advance-01 through -04 records before editing.

The spare SDK is now a group_client component with close, drain and resource
owners. Node shutdown/stop proofs use complete group counts and explicit producer
quiescence; one unfinished command per group is no longer assumed. Transfer uses
actual owner progress with term/revision fencing. Namespace stores are released
by the controller's joined owner, independently of response cancellation. The
latest spare library check has 114 remaining fleet/query/predecessor-host errors;
no canonical acceptance case is qualified. See the newest progress section.
The minimal closed-manifest-pin fix passes twelve checks in the primary checkout
and is published on PR 160 at 110b7a4e (runtime 51d136fb). Both checkouts share
that base with all WIP and eight diagrams preserved. Keep its source-scoped evidence separate from the
unqualified cutover. Its recovery regression retains closed log/group handles
through fresh recovery in both image forms.

CG-07 is IN_PROGRESS in the spare PR checkout. Canonical startup now assembles
namespace registrations, whole-group placements, selected-root recovery, shared
SQL/image resources, controller archival/effects and logical SDK handles. Public
admission and remote ingress are being connected to the same physical owners.
These changes are uncommitted and intentionally incomplete: the first locked
library check reported 132 unconverted SDK/fleet/query/shutdown integration
errors; the second reports 117 after the physical-query and native-effect edits.
No executable or acceptance case is qualified by either run. Preserve all
dirty/new Rust files and the user's diagrams. Do not restore
legacy startup, reinterpret GroupScope.cell or add a mode flag. See the latest
progress section and the diagnostic artifacts in
.cellar/cell-groups-evidence/CG-07-canonical-startup-advance-01/.
The exact source state and remaining work are recorded in the spare checkout's
.cellar/cell-groups-evidence/CG-07-cutover-progress.json.
The previous source preflight is not evidence for these new runtime inputs.
Read plan.py show CG-07 and the existing production callers before editing.

1. Replace canonical host/startup/registration with group placement and cell
   enrollment; do not add a legacy/shared toggle. Use the existing shared runtime,
   controller, coalesced provisioning, directory and registered dispatch owners.
2. Replace per-cell binding capacity with bounded namespace/class registrations
   and separate group/cell occupancy. SDK handles stay cell-addressed and carry
   logical cell versions with their original commit witnesses. Preserve typed
   ambiguity, caller command identities, strong reads and ingress authorization.
3. Convert managed import, capacity, repair, drain, maintenance, stop/return/resume
   and topology to complete group replicas while retaining CAS journals,
   permissions and disruption debt. Update native/Axum/admin/CLI/examples together.
4. Qualify all five original CG-07 cases and its complete source/evidence gate.
   Occupancy one must use the same path. Feature-off Fleet contracts remain intact.

The source-bound investigation record in the spare PR checkout is ignored:
.cellar/cell-groups-evidence/CG-07-startup-inspection-e8edfd12.json.
It records integration gaps, not implementation evidence. CG-09 through CG-16
remain TODO: transfers, topology, placement, hibernation, managed recovery,
workload/crash qualification and deletion of superseded consumers.
The full goal stays active. Final CG-16 repeats every required capability and
profile at one release source; earlier isolated passes cannot qualify it.

Required nullable recovery metadata stays an internal marker-1 hard cutover.
There is no legacy reader or state migration. Prepared recovery data alone is
never engine/serving authority. Keep unknown outcomes and physical joins owned.

The prior whole-group fencing-barrier source is
65c5176975907fb52a53deca58e1ae1f5ba7adec, with 211 native cases and four real RustFS
resume/race scenarios. Its evidence remains at
`.cellar/cell-groups-evidence/CG-06-recovery-fence/`. Historical qualifications
remain scoped to their recorded sources.

The prior bounded-outage qualification source is
1159e9358c106fe1260c3479759e909e413257a4. CG-06 remains IN_PROGRESS. Its
original bounded full-S3-backend outage case now passes in both image forms. The
normal case faults the adapter shared by catalog and archive. The real RustFS
case stops and restarts the owning runner's actual backend container, preserving
its pinned identity, owner, image, volume, resource limits and endpoint. Actual
S3 probe I/O fails during stop and recovers after start; no real-mode wrapper
fault substitutes for that outage.

Warm local Quorum commands progress to the explicit nine-entry retention ceiling
while fresh remote discovery refuses without executing a callback. S3 commands
may commit locally but cannot return S3, and stored minima survive downgrade
attempts. At capacity, callbacks/initialization refuse before execution and
known receipts remain available. Independent complete archive recovery precedes
publication of the held exact checkpoint. Every voter then selects its complete
cut and retires actual debt, allowing new work. A failed remote invocation
retries with unchanged captured inputs, identity, deadline and budget. All three
voter images and logical versions remain exact; eight callbacks occur once.

Twenty exact real RustFS cases pass in both image forms, with seven signed
probes and four owner-checked backend stop/start transitions. Twenty-three
checks pass with 270 Rust cases, seven copied executables and 2580 source
hashes. All four native shared-group suites, all 22 storage cases, locked
all-target Clippy, format/layout, dependency/docs/preflight/plan validation, 37
plan tests, five controller tests, system-Python hashing and independent
source/artifact/selection/credential/backend-transition audit pass. Evidence is
under .cellar/cell-groups-evidence/CG-06-full-backend-outage/. The first-source
inventory mismatch and copied executable reproduction remain retained; the
correction verifies actual indexes 2, 3 and 4 on all voters and fills the
unchanged entry ceiling with one additional real command. Earlier dependency and
physical-reclamation qualifications and the unresolved 8dfe9ff3 slow-reader
timing limitation remain retained.

Four original CG-06 cases pass: whole-group dependencies, bounded S3 outage,
archive tails and physical reclaim/restart. The remaining original case and full
task gate require fenced unmanaged recovery into fresh group/cell incarnations
and a serving replacement quorum. Inventory every possible old voter, including
joins after the selected cut. Use the accepted fencing-authority API plus Docker
qualification, permanently prevent old-host restart, preserve selected-cut
results/receipts, reject old tokens, initialize new histories and require fresh
quorum before serving.

CG-08 effect/cache/result lifetimes precede CG-07 public SDK/fleet cutover.
Irreversible source sealing and activation, split/merge, placement, hibernation,
managed recovery, real load/crash qualification and final legacy removal follow.
Do not add a new cached-authority shortcut: live Seal/Install/Activate controls
still need their durable transition lifecycle and exact publication proofs.
Current outage evidence covers existing static groups, warm local routes and
fresh remote refusals. Host-native voters share one machine and RustFS one VM;
global physical disk bounds, sustained throughput, independent domains and
deleted-voter fresh-incarnation disaster serving remain unqualified. Preserve
all predecessor evidence, excluded MR-13/MR-14 progress basenames and the user's
untracked diagram. The full sixteen-task goal remains active and incomplete.

CG-01 through CG-05 are DONE at their declared native task boundaries. CG-05
closes at `e0550acd` with all five exact snapshot cases and eighteen passing
commands, backed by 205 Rust cases, 337 source hashes and five executables.
Its source-bound record is
`.cellar/cell-groups-evidence/CG-05-completion-admission/qualification/checkpoint.json`.
Independent hashes, full `check_evidence` and the closure audit in [progress](progress.md)
pass. CG-04's earlier 83 exact-case/238-regression gate remains retained at
`69e41e86`; no task requirement or acceptance oracle was removed.

The final CG-05 slice fixes wrong-size wake starvation and transient item charges
from byte-rejected budget attempts. It exposes advisory runtime lane usage and
the existing exact-ticket checkpoint publication boundary. Both image forms fill
all eight completion slots with actual fsynced work, retain the ninth checkpoint
through admission and a held real engine fence, then finish its exact successor.
Two concurrent writers share one checkpoint slot through a bounded 64-enrollment
trace, inspect both newer checkpoints before the writers finish, and verify every
cell and both selected closures on all three nodes. Every new native scenario runs
directly inside the exact resource-bound acceptance case.

Continue with CG-06 complete group S3 archival, verified checkpoint/tail coverage,
safe log reclamation and fenced unmanaged recovery. Follow with CG-08 effects/cache
lifetime, then CG-07 public SDK/fleet cutover. Idle-node orphan maintenance must be
invoked by managed startup with the complete durable-reference inventory. The
entry-count policy retains the idle tail; idle/time/byte checkpoint policy and
coordinated wake remain future work. Real RustFS/load, broader crash schedules,
independent domains, managed disaster recovery and final removal of the old host
remain required later gates. The full end-to-end goal is not complete.

The following checkpoints retain their historical source and partial boundaries.

The preceding checkpoint at `bad0387c` removes checkpoint scheduling's
dependence on free SQL apply slots. The bounded pending scan still visits other
groups when SQL is full, starts checkpoint work within its own capacity, then
defers SQL work without increasing its limit. Two physical image-barrier cases
hold node-zero apply in both image forms while another group's preceding complete
checkpoint publishes. Healthy voters apply its successor; node zero retains its
current root, suffix and SQL ownership until release. Both new cases also run
inside the exact resource-bound snapshot case.

All five exact snapshot cases, 152 execution cases and nineteen storage cases
pass, along with all sixteen commands including the prescribed gate. Its record
under `.cellar/cell-groups-evidence/CG-05-checkpoint-apply-scheduling/qualification/`
binds 335 source hashes and three executables. Both original and diagnostic
failures retain their source/output/executable hashes; independent verification
and `check_evidence` pass. No timeout, assertion or physical fence was weakened.
CG-05 remains IN_PROGRESS pending completion-lane saturation and sustained capture
fairness qualification. S3 archival, public cutover and deployment gates stay open.

The preceding checkpoint at `5cfa9fb6` adds production idle-node orphan
maintenance and authenticated retry on the same interrupted learner disk. All five
exact snapshot cases, 150 execution cases, nineteen Raft-storage cases and 38
relevant LTX cases pass. All nineteen commands pass, including the complete CG-05
prescribed gate and all-target Clippy for the affected crates. Its record under
`.cellar/cell-groups-evidence/CG-05-node-orphan-retry/` binds 335 source hashes,
six executables and frozen inputs; independent hashes and `check_evidence` pass.
CG-05 remains IN_PROGRESS pending broader lifecycle/pressure qualification.

`ReplicaLog::reclaim_cell_storage` requires joined runtime/SQL ownership and one
exact applier per physical engine group, plus every additional durable retained
root. It verifies current/checkpoint closures before bounded deletion, merges
references across shared image stores, preserves live pins and holds both engine
claims through cancelled work. Missing/duplicate/foreign/oversized/busy inventories
refuse. Managed orchestration must supply the complete inventory and call this
idle-node boundary; it is not a serving-node collector or Raft log truncation API.

Four new SIGKILL scenarios interrupt prefix/synced dependencies in both forms.
Original voter disks resume, establish fresh quorum and commit a newer cell;
the same learner disk collects unselected debris and receives a fresh TLS seed.
Ordinary routing refuses until the authenticated install receipt exists. The
learner catches a live suffix; another cold reopen verifies all four independent
cell sequences and current/checkpoint closures. Original voter disks remain intact.
Shared image tests preserve both groups and an independent reader, collect one
file per request and join cancellation. Continue capture fairness, remaining
controller crash/completion-budget and retained-root pressure qualification.

The preceding controller-publication checkpoint at `370dcf34` retains the actual
held publication-sync and cancelled-close cuts in both forms. The healthy two-voter
quorum commits another group's writes while the stalled node retains its lifecycle
slot, captured source, bytes and engine/SQL ownership. Release selects the preceding
checkpoint and joins shutdown. Its source-bound record remains under
`.cellar/cell-groups-evidence/CG-05-controller-checkpoint-shutdown-final/`.

The earlier exact-acceptance checkpoint at `f076731a` delivers
`native_cell_groups_snapshot` with all five required native oracles. Each passes
individually with `--exact --test-threads=1`, and all ten prescribed task commands
pass. Fifteen total commands include 144 execution regression cases and package
checks. The record under `.cellar/cell-groups-evidence/CG-05-snapshot-acceptance/`
binds 332 source hashes and two executables; both independent hashes and the
plan's full `check_evidence` validator pass. CG-05 remains IN_PROGRESS pending the
controller/lifecycle and orphan-retry audit; the new gate is not a release claim.

Shared root-level engine helpers now live in `cell_group_engine_fixture.rs`;
the existing execution-only restart method stays in the original target. Exact
cases compose the real fixtures and preserve owned-child names. Writes now commit
in the replacement and unrelated groups while a learner install fence is held;
cold restart explicitly checks empty and populated cell sequences/images. The
composite membership test constructs futures outside its poll frame after phase
probes reproduced a debug stack overflow. The standalone case passed and the
fixed complete composite passes; no stack limits, timeouts or assertions changed.

The pending-publication cancelled-shutdown and idle-node orphan/retry cuts
now pass above. Continue remaining controller lifecycle/saturation and capture
fairness qualification. Preserve fresh quorum, physical-owner retention and
complete selected-root checks. RustFS/load, independent-domain and public host
cutover qualification remain open.

The earlier interrupted-transfer checkpoint at `6e56f21f` passes all 144
shared-execution tests and nineteen native Raft-storage tests, with eight scoped commands.
Its record under `.cellar/cell-groups-evidence/CG-05-seed-transfer-cuts/` binds
330 source hashes and two executables. The seed crash matrix now includes
12 owned SIGKILL scenarios: four new cases hold partial or synced dependency bytes
in both image modes. Cold recovery preserves the empty stage and refuses startup
without a local install receipt. The production TLS source and receiver perform
these interrupted transfers; the parent checks reached cuts and retained extent
lengths before cold reopen.

Two new revocation tests replace the source process while a physical image write
is held. A refused network waiter does not refund checkpoint/transport admission;
physical release drains it without publishing any seed. Cold recovery again
refuses startup. These revocation cases use an independently authenticated captured
peer. Production runtime code is unchanged. No automatic orphan cleanup/retry,
all-wire-boundary, power-loss, independent-domain or RustFS/load claim is made.
That checkpoint preceded the exact acceptance target delivered above; its
controller/lifecycle and retry-availability limits remain explicit.

The earlier starting fixture mapping for the exact target is below. The newer
checkpoint above implements that target and records its exact five-case gate. Reuse the
real fixture owners and direct assertions, without launching another test binary
as an oracle or replacing any required case.

| Required case | Existing behavior to audit and reuse |
| --- | --- |
| `snapshot_contains_every_cell_at_cut` | `checkpoint_fixture::coherent` captures both forms while quorum writes advance. |
| `partial_snapshot_never_serves` | `refuse_checkpoint`, seed receiver faults and seed crash cuts reject partial/corrupt/foreign state. |
| `whole_group_learner_and_joint_change` | `seed_install`, `membership_lifecycle` and `membership_crash` cover learner promotion, replacement, quorum loss and explicit joint restart. |
| `snapshot_empty_cell_and_extent_restart` | `tcp_publication` transfers the complete populated/empty closure and reopens retained checkpoint plus current suffix in both forms. |
| `snapshot_memory_and_pin_bounds` | `transfer_pressure` and `local_shutdown` retain bounded physical work and old reader images through concurrent apply/shutdown. |

The shared imports, `Voter`, `Cluster` and image/apply helpers are now extracted
without semantic changes into `cell_group_engine_fixture.rs`. Both integration
targets include them at their root, preserving support-module paths and exact
ignored child names used by owned-process tests.

The earlier pressure checkpoint at `a2036e50` passes all 142 native
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

1. Implement CG-06 complete-group checkpoints plus contiguous committed tails.
   Replay cell-addressed LTX, enrollments, membership and administrative entries;
   validate every cell base chain and dependency before publishing coverage.
2. Keep Quorum progress separate from S3 settlement. Bound archive lag/unarchived
   bytes and tie actual local truncation/purge to covering checkpoints, configured
   archive policy and all live group/reader/transition pins. Verify unmanaged
   fresh-incarnation seeding through the existing fencing authority.
3. Follow CG-08 then CG-07 for effect/cache scheduling and production SDK/fleet
   cutover. Preserve catalog/current-term authorization, captured inputs, exact
   receipt reconciliation, cancellation ownership and group-local quarantine.
4. Complete CG-09 through CG-12 for irreversible transfer, split/merge, placement
   and authority-checked coordinated hibernation/wake. Retain independent cell
   histories and versions; never derive them from a physical group log position.
5. Build CG-14's real-infrastructure harness, then qualify managed/in-transition
   S3 recovery in CG-13 and the complete crash/load/domain-loss matrix in CG-15.
   CG-16 removes the old host, `TransportRuntime::Legacy`, cell-scoped transport,
   `QueryVersion::GroupCut` and `RaftClass::invoke_group_query`, and reruns all gates
   at one immutable source. These are required parts of the full hard cutover.

Start from `cell_groups/checkpoint/`, `cell_groups/reclaim.rs`, existing `archive/`
and `disaster.rs`, plus `cache.rs`/`storage.rs` for group log cuts and pins. Read
`plan.py show CG-06` before creating its planned archive module/native target.
Preserve fresh quorum, permanent fencing authority and complete physical recovery;
source metadata and uploaded objects alone cannot establish those proofs.

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
