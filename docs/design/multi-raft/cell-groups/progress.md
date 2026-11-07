<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

Purpose: Source-pinned cell-group implementation checkpoints and qualification limits.

The executable design and implementation plan is delivered. Remaining
end-to-end implementation and hard cutover work is specified in the
[executor handoff](handoff.md).
CG-01 through CG-06 and CG-08 have passed their scoped native gates.
CG-07 and CG-09 through CG-15 are in progress. CG-16 remains pending
the combined release gate. Follow the [quorum SQL priorities](remaining-work.md#first-delivery-of-quorum-sql-through-public-ingress)
for the next delivery.

## Named case audit and public read readiness

The plan validator verified the source-pinned evidence records for CG-01–06
and CG-08. Those records account for 115 of the 163 required named cases.
All five CG-07 cases are implemented in the planned public target.
CG-09 through CG-16 have component and process coverage at their recorded
revisions. Their planned acceptance targets and task records are outstanding.
The audit identified drift in 14 of 24 source anchors. The successor baseline
at `bcf10c76458e97cc1696bddbebf85d9dfb7d32aa` reviews that drift and pins
current host options, the durable cache and the Fleet transition owner.
All 37 plan tests, including preflight, pass with this baseline.

The shared read target at `1d571a411f873fde8d3b4c1cdfcabb65259f1970` passed
all 16 cases in 60.59 seconds. With the local consensus channels cut, a voter
served the historical SQLite image and refused recent-source and linearizable
queries. Both policies recovered when the channels healed. A protocol-only
leadership transfer advanced the term and physical apply on all three voters
while the same logical cell witness and version remained readable. The suite
log SHA-256 is
`4868cb212a2a90e14ffb302eee7892c0b61e1648d903fc3352f007717db8fa30`.

All five named CG-07 cases passed on `b51be13aa9ff6ab429882d5306bbcaf95a324e90`
in the native public target. The additional harness freshness case passed,
and the opt-in RustFS lifecycle case was skipped in that invocation. The
suite took 441.62 seconds. Its command log SHA-256 is
`98ede1eb69f26ee37916bec8ae2a90c9843705114a8f9e4eeab0d1bf4d8be026`.

The same public target at `bcf10c76458e97cc1696bddbebf85d9dfb7d32aa` passed
the five named cases and harness freshness in 578.06 seconds. The opt-in
RustFS lifecycle case was ignored. This run shares its source revision with
the constrained materialized pass and extent failure below. Its command log
SHA-256 is `ddbb0358cee44b7a6c5fcd442ee093c0cda923857efa343a936433714f6249f1`.

The native CG-09 operator split case passed in 15.80 seconds on the same
runtime. It lost the confirmation for the first child reservation, advanced
the Fleet revision independently, reopened the CLI controller and completed
the same plan in key order. It also verified exact cancellation replay. The
case log SHA-256 is
`98bc23f770f5de9c2272e938b95d7721c92cdb5d7a2642a5387c8caf323e200a`.
This is coverage of a split admission cut. The named transition acceptance
target still needs the restart and single-writer oracles across all phases.

The native CG-10 split-then-merge case passed in 96.74 seconds on the same
runtime. It exercised reopened operator preparation, late-write preservation
and merged SQL. The case log SHA-256 is
`47f6c76aea5e8ee76cc66b3ea857ed5f2e5ca34586dd56c222162c2486f61e35`.

The host-native public lifecycle at `1d571a411f873fde8d3b4c1cdfcabb65259f1970`
passed the materialized mode against RustFS. The extent mode settled the
replacement and recovered its lost terminal reply. It then hit the deadline
replaying the original command with S3 durability. The leader had applied
through index 9 and retained one archive settlement. This is a distinct
failure from the constrained daemon fixture startup. The RustFS suite record
SHA-256 is `152a321a7a3c90e0327d9bffe7f10e31ce303c3e32add4049a78a96ba10e2beb`.
The diagnostic rerun at `65a01840fc84bda9c89ebfce896ab5e3dfc9f4b1` passed both
image modes in 75.31 seconds. Its record SHA-256 is
`30e706d558904b7c293c43ae7cb4aed812721209eec737643b59c1428f564c21`.
The rerun adds each host's archive position, attempts and typed error to
the retained failure report. It did not reproduce the extent deadline,
which remains open.

## CG-09/10 operator split through merged SQL

At `be683a3b394c06e3ffb63aede32acdac74be75d0`, the combined native case
passed in 68.42 seconds. A lost reply leaves the first child reserved.
A fresh CLI controller resumes the same plan after an unrelated Fleet
revision advances. It recovers both reserved ranges, confirms the original
admission receipt, and reopens the exact split with the planned decision.
That split passes archive preparation, source sealing, child activation,
SQL receipt replay and source retirement. The resulting child ranges merge,
and the merged group serves the late writes. The case log SHA-256 is
`10e6b800db50052c47157fea088ef572c53c50903723fd5cc20f32ca21cc0579`.

The same source passed the separate cancellation replay in 9.15 seconds and
the managed recovery-barrier case in 95.00 seconds. The recovery case starts
from that operator-admitted split, fences the selected old histories, erases
the fixture-owned voter disks and serves restored SQL through a fresh quorum.
It uses the native fixture authority. The RustFS rerun and permanent Docker
launch revocation remain separate qualification gates. The cancellation and
recovery log SHA-256 digests are
`164717789fdb4e43d7e6bad037a3a5220f48af1c26a349c053e05be4e82c9774`
and `ac3add4a43284ad9fff17d501e02d525038a7f0a2e1c614f207ef9bb0ad7113a`.

The RustFS rerun on `960836c767697277c8a9317c6158506a25933631` completed
the materialized recovery scenario. The extent mode reached the restart
of a released recovery voter and received `FleetCatalogError::Unconfirmed`
during startup. The ordinary split/merge case was skipped after this suite
failure. The record SHA-256 is
`cf2fed801695bcceced9af9d115716e66b24b3b52eb834d5d7410dabdf0fbf15`.
Keep this startup outcome separate from the native operator continuity pass.

At `edb5b31cea34dd41e022929313cbf97d52e95e4c`, the real RustFS recovery case
passed both image modes in 317.57 seconds, including the released voter
restart. The earlier `Unconfirmed` result did not recur. The ordinary
split/merge case then aborted with a stack overflow in the host-native test
thread. The combined campaign failed. Its record SHA-256 is
`0cbc96e2de5e66b87c4c879295613f265cfbe3924b6560f3ffdac6637cb5beff`.
The recovery and split/merge logs have SHA-256 digests
`2df030ee400167891cf8e986c903fe7bf73c59784eb4d535e931bccd521d0fe6`
and `2248031d17090dd90757b6cf8f1ec4a46335a5451515acc76df79204a0802dce`.

A local phase-only instrumented rerun of `edb5b31c` reproduced the overflow
in the materialized case. It confirmed child activation and merged SQL, then
aborted inside child archive initialization before parent retirement.
The diagnostic log SHA-256 is
`eb36148ebf9d31e2e1598daa966729b76c6d6d8ca38b939cc52ef86d98a46a00`.

The fixture now shares a joined execution task for retained voter startup
across merge and recovery. An instrumented diagnostic at `edb5b31c` with
that change passed both image modes on real RustFS in 230.92 seconds.
All three child voters restarted, and the case verified S3 receipt replay
and completed retirement. One earlier attempt with the change stopped
at a split admission assertion before reaching the restart. The passing
diagnostic log SHA-256 is
`cf0a9d77fdb7a9e367edd226411efee77b40aa75e3ade12c388dbd5f2c867875`.

The clean RustFS campaign at `c2776f4c` passed materialized recovery.
Extent recovery stopped before merge when a fresh post-transfer write used
the seed command's expired retry horizon. The runner stopped before the
ordinary split/merge case. The record SHA-256 is
`30d344e094da69ab958ba44c74ad8850750cd41623006556169c3f222ad5b925`.
At `af2efbc8`, fresh fixture writes compute their own retry horizon. Exact
receipt replays retain their original inputs and retry horizon. The next
campaign runs ordinary split/merge before recovery to capture that result
independently of the recovery path.

On that clean revision the ordinary split/merge case passed both RustFS
image modes in 231.58 seconds. It restarted all child voters, verified the
merged SQL and S3 receipt, and completed retirement. The command log SHA-256
is `0c4e7ae95007266aee26a778adbd990a24cf0f6280156e16c8f66a08ccb7411f`.
The separate recovery case passed both modes on the same executable in
274.03 seconds. It erases test-owned source voter disks and verifies restored
SQL and receipt continuity through a fresh quorum with fixture fencing.
The combined real RustFS record SHA-256 is
`ca843a9c8bf589d64c87801c6960ec99327e54fd438749f99355dcb52b124adb`.

## CG-12 alarm, peer and minority wake

At `a6bd8f64975ffe71842302bf4d1974362abb7eea`, the first two cases in
`native_cell_groups_hibernate` passed in both materialized and extent image
modes. Three managed TLS voters park at the same committed cut. A durable
alarm wakes them without a client and commits one update. After repark,
the surviving voters stay parked during the leader's joined stop, then
its retained restart wakes them through peer traffic. All voters apply a
fresh term before any application read, and the alarm result survives.

A separate case wakes the former leader with two voters stopped. It refuses
a linearizable query and a quorum command while the commit index remains
at the park cut. Restarting both retained disks restores a fresh quorum.
The exact command then commits once and its replay returns the original
receipt. The native suite took 100.92 seconds. Its log SHA-256 is
`dd332599f792dd63ae48b3bbc37aed0ccc86d596a91167f8a1aff6602cd6ddd7`.
These are correctness cases with a fixture Fleet store and real Raft Engine
disks. Wake latency distributions and the remaining fault cuts are open.

## CG-09/10 and CG-13 managed serving checkpoint

At `6e6be12d403e1d7b3341531a0b664cbdb8299753`, the managed recovery
and split/merge cases both passed against RustFS in materialized and extent
image modes. Recovery used a fixture fencing authority and host-native
voters. After the replacements formed a fresh quorum, the case verified
linearizable SQL reads, archived receipt lookup, old version-token refusal,
a new quorum write and retry, and a new S3 acknowledgement. These results
qualify the serving checkpoint. A later retirement candidate passed the native
fence and Fleet-slot release case. Its extent-backed RustFS run stopped during
replacement quorum setup when the group controller closed.

## CG-13 cold managed recovery and retirement

At `13fb77e92e749d4b01a9c2c272ffd1ba601eb7fb`, the managed recovery case
passed natively in 91.67 seconds and against RustFS in 245.01 seconds. The
RustFS case exercised both materialized and extent-backed images. The test
joins every source voter, checks the complete requested physical lineage,
erases the owned voter directories and refuses an old boot on an empty disk.
It then drops the pre-erasure verifier and reconstructs the selected archive
cuts in an empty workspace. The replacement quorum serves restored SQL,
original receipts, new quorum writes and explicit S3 writes. The case
interrupts replacement archive setup at registry and identity creation, then
resumes the same binding. It also verifies the permanent fence again before
retiring old histories and releasing the Fleet disruption slot. The archive
holds and physical inventory debt stay recorded.

The ordinary managed split/merge regression also passed in both image modes in
195.97 seconds. The complete RustFS suite record SHA-256 is
`18ac3ebfb43f9484bf7eeca601abf0607ea103975be3d3b63b2c144abe1b30fa`.
The native test log SHA-256 is
`bd43761d841e306fc1d9eb66c1cb92677482cf3ae41dbcbdacf825521a8b5f92`.

The source voters are host-native Nodes and the fence uses a fixture authority.
The managed Docker drill still requires permanent launcher revocation,
independent voter processes and deletion of all their disks.

## CG-07 RustFS lifecycle follow-up

The RustFS source-service failover case passed on the executable source of
PR #160 at `6e6be12d`. The joined-voter maintenance case progressed through
stop, return, resume and a successful leadership handoff. The resumed leader
reported ready with its applied and committed indices both at 18. An exact
receipt replay requesting S3 durability expired with one archive settlement
outstanding. The retained journal reached index 18 by shutdown. This narrows
the observed failure to the S3 acknowledgement path after resume. The failed
RustFS lifecycle suite record SHA-256 is
`dcd124b69d21bbc57f98801201b4141931cb674d2e41ad9026dd75c4b423c352`.
The combined public quorum gate remains open.

## CG-07 constrained Linux SQL qualification

The daemon qualification at `9d6cf6e9c8dc20583c4177c7896ebf0673977449`
passed against real RustFS in both image modes. Each run used three separate
Linux voter containers with one effective CPU, 3 GiB of memory, zero swap and
separate disks. The public CLI provisioned the managed namespace and first
group. Native and Axum requests covered retained restart, leader SIGKILL with
a successor elected before restart, exact receipt replay and quorum loss.
The archive fault left catalog operations available. Quorum writes and
linearizable reads continued while S3 acknowledgement refused until the
original receipt obtained archive coverage. The same run exercised operator
park and wake through a fresh quorum.

The materialized and extent-backed runs took 64.18 and 65.84 seconds. Each
mode recorded one SIGKILL exit and three joined normal exits. The suite record
SHA-256 is `839c373ec7e52c4feb914ead588c91af05ca2a6a13765e0d2bb7d7eea101542b`.
This is a single-VM public daemon result. The combined independent-process
repair, drain and maintenance gate and sustained load qualification remain open.

## CG-07 shared-group directory CI port

All 16 `native_raft_directory` cases passed on `a69670357de38d4850821f81b26e9feb64110f5e` in
8.84 seconds. The suite uses two real groups, a rooted Fleet observer for
physical metadata, and the same engine across a retained restart. It verifies
that unpolled inspection calls retain their reservation while business SQL
completes, then releases every reservation on joined shutdown. The entered
metadata case pauses the read, observes pending closure, releases the gate and
checks that no read owner remains. The command log SHA-256 is `6cb4e5e275690b5390c8d797f0377e7de1ba5c7a52e1aef0e27c15def1a39c44`.
The `native_raft_reads` and other predecessor consumer targets still require
ports to the canonical SDK.

## CG-07 retained group images during node collection

On `6521cf5c`, all three `native_raft_extents` cases passed in 22.61 seconds.
The public host refuses extent activation without storage admission before
catalog mutation. With eight and twelve extent roots, the fixture provisions
two physical groups, enrolls cells and verifies S3 receipts against the
in-memory object store. It checks that the collector reclaims files after a
restart with one group unattached, then reattaches that group and verifies
its saved SQL state and exact receipt without running the callback again.
The run also checks persisted checkpoints, bounded inventory and joined closure.
The command log SHA-256 is
`2ad1a3150d5dfadfcea3ae7b713dd08f23c987e670b60c9aa593308625a9caa8`.
The same runtime passed all 16 `native_raft_directory` cases in 33.80 seconds
(command log SHA-256 `93a8643767ee4139061dfe41704b046e96bbbe1b5a857a0a1c5ffe2d755f5f60`).
This covers local image collection and retained restart.

## CG-07 shared-group Fleet permit CI port

On `c0e6c74f`, all eight `native_raft_fleet_permits` cases passed in 119.18
seconds. They use the public managed node and resource movement APIs with real
TLS peers, retained engines and an in-memory object store. The cases preserve
the exact permit after a lost reply and a controller reopen, fence delayed Begin
requests, and refuse joint consensus progress and linearizable reads when the
old majority is absent. A returning leader can apply its own removal before
replying, so the fixture obtains the exact terminal proof from the next leader.
It verifies peer and S3 learner installs, callback suppression, and retained
capacity debt when a restarted voter cannot attest its former boot. The command
log SHA-256 is `ddfecfc85d3125365d7c80a96246b00af94ff5d1c1065c725060b71b6aa1fc55`.

At `987a7247`, the same suite passed seven cases and timed out waiting
for an S3 command receipt in
`fleet_permit_legacy_startup_cannot_seed_managed_history`. The isolated retry
passed in 18.48 seconds. The full-suite timeout remains open. The suite and
retry log SHA-256 digests are
`217f84fe6493b4e11e0747616fa42cc5241d5b9a2a0235e82bc9f8833b3bd53b`
and `803b5ad45836b7edfc1931028a51be63ebe2592f2a1bbbbc9094f1f5c6be8c30`.

## CG-07 constrained Linux failed-voter repair attempt

The separate repair candidate at `c97a5be76b3a05747a52d80d8442260438c900fd`
booted a managed standby, seeded the archive on real RustFS and recorded
platform isolation of the failed voter. The materialized campaign reached
`Repairing` at Fleet revision 59. The worker recorded a blocked history
with an `Observation` error, zero execution attempts and zero settled
movements. The run timed out after 161.433 seconds. The extent-backed
campaign did not start. The run record SHA-256 is
`147d750bdc98f8cfaea45fe4ccb4065b4666101daee925360c49962f651af812`.
The failing observation still needs to be isolated before rerunning the
fresh-quorum and receipt checks.

The instrumented candidate `c4ad4796` recorded 137
`RecoveryUnproven` results at repair selection before the standby became
unreachable near the deadline. The standby advertised 512 MiB for the
replica. The planner also charged 128 MiB of measured-policy overhead,
256 MiB of seed headroom and a 64 MiB reserve, for a minimum of 960 MiB.
The candidate at `d95157a5` declares the complete standby budget and checks
the qualified limit before injecting the voter failure. It still needs the
real RustFS replacement and receipt checks in both image modes.
The instrumented run record SHA-256 is
`e2e20d29a7b961e24d91e9e70fd7ff261875b9570aba3c72c5a3e0b64e008b2f`.

With that budget, `d95157a5` admitted one movement at Fleet revision 60.
The materialized campaign recorded 108 execution attempts with no settled
movement or verified replacement, and stopped after 154.992 seconds. The
extent-backed campaign did not start. The controller is a Registered boot,
while the movement receiver requires an exact origin whose rooted state
permits execution. Move the test worker to an authorized boot and preserve
the receiving guard. The corrected-budget run record SHA-256 is
`daae13a6d902a335dd29b7ac8857ed57eed297e2b1c9e46a3c56ab5973c6ec0d`.

## CG-07 automatic recovery origin authority

On `136474ac`, the native recovery case passed both image modes in 44.75
seconds. An exact Registered observer fails admission before it acquires
the control service or calls the platform. The fixture verifies that the
catalog revision and the observer's resource charge do not change. An
Active voter then resumes interrupted isolation, replays from the fixture
object store, verifies the replacement quorum and reaches terminal removal.
The worker checks its exact origin boot against each recovery turn's rooted
catalog view. The command log SHA-256 is
`8e221bd5689cc5f8578a98fbaff3e27c2937a99d9dd51335f0ab57a01405d5ad`.

The separate constrained Linux RustFS candidate at `458d6950` passed
the materialized maintenance stop, return and resume sequence, including
the resumed leader's SQL receipt checks. The parent verified the worker's
exact Active boot and joined the killed voter. The repair stayed Reserved,
with 95 execution attempts and no settled movement or verified replacement.
Worker traces repeatedly reported `Unconfirmed` during the admitted turn.
The materialized campaign stopped after 155.439 seconds. The extent-backed
campaign did not start. Trace leader observation and the Begin action
separately before rerunning the fresh-quorum and receipt checks. The run
record SHA-256 is
`8aa9f58a72869a9a85d234b4962faffc555cc3bd2e5d725831eeaa723e6f39f0`.

The materialized candidate at `b726c0fc` traced leader observation and
movement actions separately. The authorized worker advanced to `Activate`
after the quorum consumed the repair permit. It recorded 86 unconfirmed
standby activations, 93 execution attempts and zero settled movements.
The standby exited near the deadline with no attached group, and the
campaign stopped after 216.632 seconds. The extent-backed run did not start.
Capture the receiver's typed activation cause before changing seed or
attachment behavior. The run record and stage trace SHA-256 digests are
`a5c0130bacc4447639844bc241708282bb6659555c2e217df95b0d24d5e2e6eb`
and `84bad5dfab98c2bcba045669c1c8414d5f18497a12b6ee60a8cd6a19b6127585`.

## CG-07 retained S3 seed endpoints

The constrained materialized RustFS candidate at `85631a1d` reported 83
standby activation failures caused by an unexpected TLS EOF during the seed
exchange. The returned leader had reopened its retained engine. The run took
198.3 seconds, made 90 execution attempts and settled no movement. The extent
campaign did not start. The run record and standby trace SHA-256 digests are
`a774b66d59490a04721abc78151831550b53690ee1a2b31f5f92c3dcbf009a9d`
and `2c4d2d2d2875d17faa9a3b0a9e4c265773955a5c9a5fa318cde7c2771d922882`.

Retained startup previously registered seed endpoints only when periodic local
checkpointing was enabled. The native regression reopens and requalifies the
four exact boots with a checkpoint transport pool and no periodic policy.
Before the fix it reserved the repair and then left the learner unconfirmed
for all remaining turns. At `bb63c2fa` the same replacement and joined restart
passed in both image modes, in 44.41 seconds natively and 65.2 seconds against
real RustFS. The independent native managed failure-recovery regression
passed in 44.89 seconds. The RustFS proof uses host-native voters and fixture
capacity admission. The constrained Docker rerun is a separate gate.
The failing baseline, passing native test and RustFS record SHA-256 digests are
`80cd8d7c3982128b1a4b4a917afa31a983fbdb484cb5dac517fc5701707328a2`,
`f2fd1ace93fd7ed68645939be38ca45c66cce375112694a2181d6fd01abc2ac2`
and `588d0f2b5ca6dbce3e580241eaad66bc5ad3e5c63fa32e47016d62d2b4f282d4`.

The separate peer-seed replacement run at `bb63c2fa` reached joint consensus
and missed settlement within the eight-turn fixture. An isolated retry of the
same binary passed in 54.44 seconds. The failed and passing command log
SHA-256 digests are
`fdfba994b40a40f18f4047cd684c3b795aa26975ae16d02090579316b1116d29`
and `e5a49699531baa155794619d754b95eb822c153bca9ebc0a47869ebbd301428b`.

The next constrained materialized run on candidate `8a6ab6caaf82` passed
the previous standby seed failure. It promoted the replacement, observed
the stable voter set `[2, 3, 4]` and checked that the removed engine's files
were retained. The fixture then refused startup of the replacement while
inspecting the original birth voters at Fleet revision 60. Movement finished
at revision 63. The fixture exited after 88.545 seconds, before the replacement
served SQL, and the extent campaign did not start. The suite record and
materialized process ledger SHA-256 digests are
`a202df3cc2ac53e243221046085526c25e7db9ad44ec9c5f84d75c22fa5bf9a5`
and `0deca8b3aba071ab2deb379c3d745380249244c8c93e90ba1c2966273314dc57`.

Candidate `c554b2a5eb187c482cabc7a56e2202d4e2f26d56` restricts the original
birth inspection to original cold voters. The standby still checks the
selected boot, applied manifest, promotion through a fresh quorum and local
SQL readiness before ingress. The candidate passed both constrained RustFS
image modes in 102.951 and 113.427 seconds. Replacement member 4 served
native and Axum SQL as leader, replayed the archived receipt with S3
durability and accepted fresh quorum writes. The suite record SHA-256 is
`c377e7206ef3f5194ed9e706743afcd15275c7cdf4cf512cc8ec645bcc2e464c`.

The PR-source harness at `bcf10c76458e97cc1696bddbebf85d9dfb7d32aa` adds the
replacement leader drain, joined shutdown proof and linearizable SQL on the
surviving quorum before inducing quorum loss. The native daemon and targeted
Clippy pass. The native daemon log SHA-256 is
`4cc00ce662f271e88b11a12c06d9107178044f8f2844a1805a68a0a7fb9473a4`.
The constrained materialized campaign passed in 220.838 seconds. Its
replacement leader handed off in one attempt, and both adapters served
linearizable reads through the surviving quorum before refusing quorum
requests after another voter stopped.

The extent campaign stopped after retained restart, before repair. Both
survivors exhausted their daemon node lease renewal deadlines against the
fixture's shared SQLite database and self-fenced. Raft catalog and archive
traffic used RustFS. The extent failure took 98.157 seconds. The combined
record SHA-256 is
`4495160cf492f7bf3dd3b94c524fbe040924af022b08c1563bbbc4ac53a50149`.

Candidate `e7a3929f519cf53b773364def3ba6daf079d4746` uses a separate prefix in
the same isolated RustFS bucket for daemon leases and peer authentication.
The host-native extent daemon passed against real RustFS in 177.578 seconds.
It independently read all three live leases before SQL and passed successor
election, retained restart, resumed reads and writes, and quorum-loss refusal.
The record and process ledger SHA-256 digests are
`f206ced9602327252bf3c1a7b703560b6aba1513486238a2e773617c0a715c66`
and `fa00109ae0415db5c26964eec971eb16cef2a11cae7a5edc4dcc0deab1628f51`.
The constrained Linux rerun at `848264e9d6d9aa17118f0e789726e44780cea7ed`
passed both image modes in 236.370 and 103.031 seconds. Each mode verified
three live RustFS leases and passed native and Axum SQL, hard leader loss,
retained restart, archive-payload outage, failed-voter repair and joined
replacement leader drain. The surviving quorum served strong reads and
refused quorum-dependent requests after another voter stopped. The voters
used separate Linux containers and disks in one VM. The suite record SHA-256
is `195da0b33169f2fb336b3d39f35f51f67db6fe24f378ed5b351577c29dd3034c`.

The public SDK suite at that same revision had four passes, two failures
and one opt-in RustFS skip in 541.03 seconds. Ingress authority timed out
replaying an explicit S3 receipt after the archive write fault cleared.
Managed lifecycle reached terminal replacement in extent mode, then timed
out replaying an explicit S3 receipt. Some owners reported
`Archive(Expired)` while peers had published through the applied cut.
Both callers received ambiguous command deadlines. The cause of the
deadlines remains open. The command log SHA-256 is
`016b9e42ef31fcb10145fce5e6038efa53364a861e4fdc069b8b25d9cba50508`.

## CG-07 reuse of a peer's verified archive

The failed public runs retained local archive journals. In ingress, the
waiting voter still held index 7 and had a pending publication at index 11.
Two peers held index 11. In extent replacement, one voter still held
index 7 with a pending publication at index 9 while three peers held
index 9. The isolated baseline ingress case passed. The isolated lifecycle
case again reached terminal replacement and timed out during explicit S3
settlement. Its log SHA-256 is
`fd7422278fadaa2d49139bc1b631f4b2f9f999ecb0b6b296f0cbbee73ff96682`.

The controlled regression holds one voter's registry update, lets another
voter publish the exact committed cut and denies further archive payload
writes. The baseline returned an ambiguous deadline replaying the original
receipt. At `393bffe46d51742f12e506ef9a78871e7f8c979d`, the waiting owner
journals an exact acquire intent when the selected peer archive matches its
local applied manifest. It independently verifies the retained hold before
settling the receipt or advancing local reclamation. It retries ambiguous
acquisitions by exact intent and clears only a superseded intent.

The regression passed in both image modes using the native fixture store
and real RustFS. It returned the original receipt and ran the SQL callback
once. The baseline failure and native pass log SHA-256 digests are
`7961604522fa544f01c6b0e6b8c029be01225da77271b7787590ebed2501ed66`
and `24381fc9c70ab3897d48c86a3e7e370eda136f34cb337466af41191c0b83ffc8`.
The focused RustFS record SHA-256 is
`57794691e2cc6515c8e6bdad98f4aacbd702f20dedd711a6ee241c7590929d84`.
The broader native archive suite passed all 19 enabled cases with 22 opt-in
skips in 308.66 seconds. Its log SHA-256 is
`011d5804de7fe166f83d5721fc7c1ca3a00417b4ea9622e461dd730270521edc`.
The full public suite at that revision finished in 550.64 seconds with five
passes, one failure and one opt-in skip. The ingress case passed through
explicit S3 replay after outage. Materialized lifecycle passed. The extent
replacement exhausted eight turns at joint consensus, before the injected
terminal CAS confirmation loss or receipt replay. The resource debit remained
rooted. The command log SHA-256 is
`81098b48132affa35864e6f91abc41fa85af8702ddccd51ef46a6eacb55ee975`.
The pinned Linux build lost its Docker context before any voters ran. A
replacement dedicated lab passed seven signed RustFS operations and the
same-source build was restarted there.

The first run of that rebuilt image reached the materialized voter's paused
maintenance return, then the CLI returned `wait_timeout` during resume.
Its outcome was unknown. The extent mode did not start. The run record
SHA-256 is `919a73af8ad89d6645a8ca57722b00f07765db3f0c50dce7febec474d8db8016`.
An isolated rerun of the same image passed both modes in 87.586 and 80.875
seconds. It verified real RustFS leases, archive outage and receipt replay,
retained maintenance, failed-voter repair, joined drain and quorum loss.
The rerun record SHA-256 is
`7cca3be5daf3aa5047e21ca0980e6f0b297b491dd42dc6ebce1c6ef7cb11dd36`.

An extent-only native diagnostic passed in 35.59 seconds. It recorded an
initial `NotReady` during learner promotion, then joint consensus, the
injected lost terminal CAS reply and successful settlement replay.
The earlier repeated joint-consensus stall did not recur.
The diagnostic log SHA-256 is
`aa7944369390f485d7bb46b88cb18bb30695d4d87a6a846955540e5929e9abe2`.

Four exact native public cases passed at `393bffe4`, including lifecycle,
multi-cell SDK, occupancy one and the feature-off Fleet contract.
The remaining ingress case timed out during the first-group standby CLI
invocation, before the SQL and S3 checks. The campaign retains both that
result and the earlier full-suite result where ingress passed. The exact
ingress repeat passed in 101.09 seconds on the same executable and original
command bounds. All five named public cases have individual passes at
`393bffe4`. The earlier timeout remains recorded. The repeat log SHA-256 is
`052ce8e5e6323fef7f1ebbc824f31483740fd11b292d42c03fd9e24425ab4bf3`.
The layout check passed. The format check found one missing declaration
separator in the shared read test. The repository formatter corrected that
file on the PR branch. Strict all-target Clippy on `393bffe4` failed with
202 library errors, beginning with unused predecessor per-cell consensus
code. Its log SHA-256 is
`a126388e2ecdd032f4bba27180f100f1ddaa7aa32f6ec4046bbfce91909aecb7`.
The locked all-target compile check also failed. It reported 179 errors
in `native_raft_operations` and its support files, beginning with legacy
cell registration and placement types. The log SHA-256 is
`8fed26b31b593ce59cff1f16dad0b2ea02b6622cd03fdf7ac54521fa9cd8a81a`.

## CG-07 shared-group shutdown operations

At `abd2d5f72e6377bf340d6b6564923df15d224c5b`, both ported operations
cases passed in 30.55 seconds. Each uses three managed voters with retained
Raft Engine disks and covers materialized and extent-backed images. The
archive-lag case denies archive payload writes in the fixture store, confirms
a newer quorum receipt than the archived cut, and checks the joined shutdown
report. A new process on the same disk serves a linearizable read and replays
the exact receipt with S3 durability after the outage clears. The pending
proposal case holds a SQL callback while shutdown refuses new reads and writes,
then releases the callback and verifies its receipt and joined shutdown.

The native command was
`cargo test --locked -p cellar --test native_cell_groups_managed -- shutdown:: --nocapture --test-threads=1`.
The log SHA-256 is
`b2a0e830752fbb37f666898265a16e2390ec88f8d8f2191814a137aadaf67c81`.
This run used a fixture object store. The separate constrained RustFS
results retain their executable revisions.

The locked operations compile check on this source still reported 162 errors.
The locked all-target build stopped in `native_raft_membership` with 190 errors.
Both targets still use predecessor per-cell registration and placement.
Their log SHA-256 values are
`3ab880f5635d5b9e6ca5118c82adfce67611c57839cbc58d449ec91686a79e03` and
`36da340843c09c5cab5ca5cde8a59f8197426d4ec286f6d99567b782d37449b4`.

## CG-07 shared membership authority

At `a288aaaee84b423409ff89a84c6dc9c8b822ca15`, the native SDK handoff case
partitions a voter and commits a newer SQL receipt on the majority. The
leader observes the target below the committed index and returns NotReady.
Wrong term, wrong membership revision, self and unknown targets all return
typed invalid-input errors. After healing, the caught-up voter serves the
exact receipt and becomes leader. The old leader then serves a local
minimum-version read of the successor's quorum write and refuses handoff.
The full native shared read target passed all 18 cases in 42.28 seconds.
Its log SHA-256 is
`d94dd509d5ed1f2fc9e29172b6cba5d4f61727ea340879293ec0fc349a36b9af`.

The `managed-minority` suite passed against real RustFS in both image modes.
It reserves a whole-group move while the original quorum is live and
verifies an exact S3 receipt, then joins two voter services. The survivor's
reconciliation returns Unconfirmed. The durable member claims, committed
index, membership revision and catalog root stay at their original values.
The spare owns zero groups and the pending reservation retains its debit.
The exact test finished in 18.46 seconds. This run used host-native TLS
voters and fixture capacity. The native fixture-store version also passed
both image modes in 18.98 seconds. Its log SHA-256 is
`670016bc4fd794b46c38d7fa302a1cc321bf4d9636f06225ccd40942e6a7cc78`.
The RustFS record SHA-256 is
`85b693c8506b4608b58b74044222a275b09a24cbac6c0b0a0ff30f1a5e23b53c`.

The locked `native_raft_membership` check still failed with 170 errors, down
from 190 after moving both cases. The remaining test helpers still call
per-cell registration, activation and repair APIs. Its log SHA-256 is
`c705d18f28cfe7fdc6f30ea345c1ce0c5f74af07c449d23b2c8c814f3804d7bc`.

## CG-07 standby admission and retained restart

The `managed-standby` suite on `cad524e8630aff2e6e34fd09e2cedf52901463b4`
passed two exact cases against real RustFS, each in materialized and extent
image modes. The host-native TLS fixture uses three voters and one spare
with fixture capacity evidence. The standby refusal case commits Seeding
while quorum is live and obtains the exact catalog grant. After joining two
voter services, direct S3 activation is unconfirmed and leaves no claim,
group attachment or archive payload read on the spare. Both seed callbacks
run exactly once. The case finished in 26.20 seconds.

The second case installs the spare from S3, promotes it, settles the move
and restarts the joined voter before its peers on the same engine and disk.
It verifies both local minimum-version reads and strong-read refusal while
alone. After the other voters return, it checks new S3 writes and
linearizable reads on the joined voter, with the original SQL callback count
and stale boot refusal. The case finished in 59.45 seconds.
The RustFS record SHA-256 is
`0334db306257e46e37f565a2b1769253dbf0f113e3098bb18feb04624d220c6e`.

The peer-seeded version passed both image modes in 60.52 seconds using the
fixture store. Its log SHA-256 is
`8acdca55e67bd02e7d784fbe64876db433b904a2836644425ed972e1b5a6b9a3`.
The two execution-level joint crash cases passed in 8.45 seconds total.
They kill a child process at persisted joint and post-joint cuts, reopen
the original engines and complete consensus. Their native log SHA-256 is
`651eb11d8391847ae484c4dd0aca371942ce26e9dc3000f72792ad1bcb15040d`.

The locked legacy membership check reported 154 errors after the two ports.
Its log SHA-256 is
`99a92b19e0f36a6ccf9a8f3dc52473f7df2255210bc4a2705f0a5ea23ba0f791`.
Strict Clippy for the managed target failed at the library with 202 errors,
starting with unused predecessor code. Its log SHA-256 is
`6b7af8ebfac106ca404f5a4a86bf8ae53b7355021e901d8abc060e86bf96997d`.
The active managed move still binds execution to its original process
identities. Completion also requires proof from the reserved target boot.
Restart during an active joint configuration needs a managed continuation
oracle that checks same-disk ownership, fresh quorum and retention of the
original capacity debit.

## CG-07 voter and learner read readiness

The public shared-group read target at `9ab1334894c6e6d0ce41c2a5421fc3ba56448951`
passed all 17 cases in 89.21 seconds. The learner case observes three voters
and one learner on real TLS peers, confirms the empty manifest and local read
refusal, then enrolls a cell and commits a quorum write. Every replica serves
the exact receipt version and witness through local minimum-version and
linearizable reads. The learner has no command readiness. The suite log
SHA-256 is `2adcd471c9b135af3759dbe66adb62dc996ad1781fb516931427aba711576869`.

## CG-07 shared cell version key rotation

On `d6f79a1b`, all eight `native_cell_groups_contracts` tests passed. The
rotated signer verifies the original cell version token after a lost catalog
write reply, a lost confirmation read and a reopened namespace. The reopened
catalog refuses to drop the unexpired verifier. The command log SHA-256 is
`58dd3bb6b0ba959bc24b4ed210cc66ab07d53bc45d64d695bf1ea0fdc2a13deb`.

## CG-07 forwarded ReadIndex over a lost reply

At `310b0f4a`, all seven ported `native_cell_groups_reads` cases passed in
20.28 seconds on three embedded voters with separate disks and real TLS TCP
links. The directional cut holds both a local follower SQL read and a
membership observation pending. Both finish after healing in the original
term, with the same leader and invocation deadlines. The cases also verify
minority refusals, the physical barrier after a new election, explicit
historical reads, local minimum-version reads, catch-up while a remote read
waits, SQLite authorization and an unpolled query's physical deadline.

The initial port reproduced a lost ReadIndex response that expired after
healing. The first retry-enabled suite at `9d7ffefc` passed that case and
hit a five-second setup write deadline in the follower minimum-version case.
The passing run verifies committed enrollment on all three voters before the
baseline write. The passing and failed suite log SHA-256 digests are
`f7c981efd4770c7721ab725f935ed6d71e04583476404d9bce2b4ef25b07c1dd`
and `24a70fce419804efeb251b5558698c274fc4635814d8436cfd39f197f2e8bf1f`.
Both existing follower quorum cases and the managed recovery regression in
both image modes passed against the same runtime. Their log SHA-256 digests
are `d27330fb8cb3ed0866106f5aa4f384097d22e660b4245490f52defdce2c555eb`
and `b852971cfc0ea4aeff6d5fcbabdf700c478440091ede6a1ab5d0c048dab90118`.
Targeted Clippy passed with warnings.

## CG-07 public replica routing and read budgets

At `310da5ac`, the public read target passed all 14 cases in 63.25 seconds
on three embedded voters with separate disks and TLS TCP links. This adds
bounded replica fallback, delayed-reply isolation, a pinned historical image,
32 quorum writes during an entered follower read, typed peer saturation and
expiry, and byte retention across 16 completed unpolled replies. The first
saturation port observed deadlines because it polled replies sequentially.
The passing run polls all callers within their original deadlines. It also
waits for the former leader to apply the new term and write before testing the
healed replica's linearizable read.

A voter with the same cell schema and no local query callback can route an
explicit read to a compatible replica. The recipient checks the exact query
contract, and an origin with a locally known contract refuses a mismatched
digest. The baseline reproduced the forwarding refusal. The read and contract
suite log SHA-256 digests are
`1519d95dc20c84db26383c5076792f71d3ab59aeb069c0c7b67c8f73e1760ad4`
and `13cb85042eca2d0243ff396bee77b6d2563ef36709bf32f23578c5f00d7dd54f`.
The contract suite passed all nine cases, including deadline bounds and
tampering at every byte of a shared cell version token. Both existing
controller replica-routing cases passed in 16.28 seconds.

## CG-09 rooted decisions and retained source sealing

The unmanaged source seal consumes a permit bound to the selected directory
journal, exact parent route and open replica engine. On `ad3797e1`, the native
three-voter fixture drained the accepted enrollment, rejected delayed work,
committed the seal, published its checkpoint and reopened all three retained
disks. A fresh-term no-op committed while the retained cut still refused writes
and linearizable reads. Replaying the exact permit returned the original cut.
The catalog and protocol targets passed 25 and 7 tests on the same source.

The first fixture used a direct image helper that rejected the administrative
entry. The replay uses the production applier. A tight manual tick loop then
advanced election time faster than disk sync. The passing replay pumps the
explicit campaign with the original deadline. Managed preparation, source
archive holds, child install and publication remain open.

## CG-09 independently verified source archive holds

`5b1d2f6f` replays the exact archived cuts of all parents before selecting the
hold set in the transition journal. The same held set can be reopened after a
lost directory confirmation. An incomplete set, a cut from before sealing, a
released hold or a replacement hold for the same cut is refused. The native
host target passed five cases, with its explicit RustFS case run separately.
That case passed against real RustFS with both materialized and extent-backed
images. The fixture uses the unmanaged verification boundary. Managed
preparation, child installation and publication still need their own proofs.

## CG-07 retained voter discovery and process quorum loss

`47ce0d5d` passed the constrained Linux daemon gate against real RustFS in both
image modes. The process ledger records a successful linearizable read, two
joined voter shutdowns, typed write and linearizable-read refusals, and an
historical read on the surviving voter. The same run retains its process-kill
and retained-disk restart evidence.

`e720f5b4` passed native initial and joined voter restart, managed ingress and
daemon process cases. The opened engine supplies retained local replica
placements within declared namespaces. Attachment checks selected birth,
boot and membership authority. The managed ingress case also passed against
real RustFS in both image modes with host-native voters.

## CG-07 feature-off native Fleet and bucket

`feature_off_fleet_contract_unchanged` runs the compiled native counter
through both Fleet and bucket durability modes. It verifies two-process
routing, application reload, graceful ownership handoff and retained
bucket restoration. The ordinary daemon test uses the same fixture in
bucket mode. The cases use a local development object store.

The separate fourteen-target native regression ran with
`--no-default-features` on `7634ac0f` and recorded 172 passes,
zero failures and 4 explicit skips. It covers Actor, cells,
Node, LTX replication, storage, log, daemon, events, queue, workflow,
WebSockets and containers. The broader release matrix remains separate.

Regression log SHA256 `036e58d8505e3d3bd45cd1a32e1f56561341ffbb4dce0aac0a7fdcddb086423d`.
Public feature-off log SHA256 `71d0c55bad7ae7e3352e13e12b67b03860e87753269a3a286de0ac93acea7c14`.

## CG-07 public occupancy and ingress authority gates

`native_cell_groups_public` exercises one and two enrolled SQL cells on
the same three-voter counter executable, with one observed physical group per
voter. Both occupancies use the same catalog, enrollment, archive snapshot
and retained restart paths. The native and Axum adapters can request verified
S3 coverage by retrying the original quorum command with the same identity,
payload and retry deadline. The single-cell run refuses reads and writes to
the unconfigured second cell.

`ingress_cannot_cross_cell_authority` invokes the same managed request fixture
from the public target. The managed test inventory retains the original
case names. The authority case includes the real admin, native and Axum paths
in both image modes. The occupancy fixture uses the development object store.
These local runs use one machine and fixture credentials and do not qualify the
constrained ordinary daemon path.

Isolated occupancy test log SHA256 `d0e6c1dcb5b5705d14e8a534f2f8871974d2fdbe5360f3373b7d93a478d07a8d`.
Standalone authority test log SHA256 `54c3f88fff11c17fb16d6d1355f604887651485cbb19948cc8d2c7f46754fe2b`.

## CG-06 Docker replay with a retained qualification network

Both materialized and extent recovery runs pass on
`5a97e85e6a6d75de247ab3a0b601e90d84d14bb1`. The dedicated Linux Docker profile
runs real RustFS and three constrained voter containers. The sole-launcher
controller permanently revokes the old generation and deletes its three
owned disks. Every replacement verifies restored images and exact receipts,
then forms a fresh quorum, accepts new writes and joins retained restart.
The fixture qualifies one group and three independent SQL cells in one local VM.

The original retry on `725214bd` stops before starting voters or RustFS because
Docker cannot allocate another default network. The successor accepts an
explicit unused private subnet, retains the earlier networks, and records each
actual subnet and network ID. Both runs join all containers and stop the VM.

Materialized and extent report SHA256 values are `a4847c10c34187ef0649d22f14fc767427082c1d69f70df2d2100b928141008f` and `a5e8e1fc84bef07b705381e089b36f71a5d9bfc921c4407ce21eb6ad7eaceb1b`.

## CG-07 managed ingress authority

The managed operator rejects a foreign fleet or namespace and refuses
confirmation of a different physical group. The original reservation replays
and the fleet revision stays fixed until valid confirmation. Native and Axum
requests through the zero-replica router refuse an unlisted cell and a canonical
cell ID paired with a foreign cluster or namespace. A token from another cell
cannot establish a read floor. A subsequent quorum read and the original
command replay confirm that the selected history and value are intact.

Both native cases pass on the cold-router base with the authority patch. The
owned RustFS case passes in both image modes on candidate
`3e44504c2e803f4adfe19313303968e620fe2d47`.
The fixture uses host-native voters, explicit capacity attestations and a
development peer grant setup. The standalone public-target gate and the
constrained ordinary daemon request path remain open. RustFS and its dedicated
VM stop with data retained.

RustFS record SHA256 `4f8d822d9ed31315a2c404c17b0fd0ca64d58f5ecd3bf2c514cd0ae3cec40691`.
Operator CLI SHA256 `89b44643dbdd2d2364c167d344b371538ea6135f9084d1b02780e253bd478a8e`.

## CG-07 cold router return after birth-voter replacement

A retained voter whose durable configuration excludes it refuses quorum reads
without waiting out the read deadline. The earlier single-replacement fixture
reproduced on both the published and candidate binaries. Instrumentation showed
that settlement observed the exact retired history with one outstanding work
reservation. Prompt read refusal preserves the original physical-debt check,
five-second calls and eight reconciliation turns.

The cold router returns from its retained disk after all three birth voters
have been replaced and their processes have joined shutdown. It installs routes
from the surviving membership, reads from a joined voter and replays the
original receipt before committing a new quorum write. The router retains
zero local replicas and the stopped voter disks and capacity records remain.

Three focused native cases and two real RustFS cases pass. Both router cases
exercise materialized and extent images. The entire managed native target
passes with 26 passed and 10 explicit skips. These are
embedded same-host voters and fixture capacity. The earlier broad campaign
retains its original failure record and source scope. The RustFS backend and
VM stop with data retained.

Source manifest `6e912fc3b1ec80705ec0daf1800a9808b2fcaeefb843f591dc70d0a948f396dc`.
Focused native record `ae1960871364f6c176e18f0c3acb50545bd720f781cd24117508689b73469200`.
Full managed native record `03412bfc51fb49ff1c21754cede3e6728030b2ac48e09e28c7295a4fe565e845`.
RustFS record `48ab59d5a250cebe899b4d01b6b3569ff548dc6308dfdf49acbfd3da137b9ed8`.

## CG-07 catalog-driven startup and public request qualification

Managed startup waits on the exact rooted archive and registry under its
original deadline. A missing registry never creates counters or attaches a
voter. An election supersedes the pending ReadIndex and the same startup
deadline governs the next fresh quorum observation. The process test uses the
catalog for archive readiness and retains fixture boot ordering and attestation.

Four native cases, both independent daemon RustFS image modes and the combined
native/Axum RustFS case pass. The request case starts in an isolated fresh
prefix, uses the public first-group and archive commands, attaches the voters
through the SDK readiness owner and then enrolls two independent cells. It
verifies remote native and Axum reads and writes, preserved receipts, retained
restart and quorum-loss refusals in both image forms. These are same-host
process and embedded cases. The RustFS backend and VM stop with data retained.

Source manifest `9352d069e187b5c8d9e039be312609c25c5f2774caa9541c9767c56308f18105`.
Native record `1cb5c6c9fb4398f18a5e70216c23e19ec81a82fcbed3c6e25216539964a65759`.
RustFS record `339ec79daa9d92896a9a12a1bcf6178bb54308553db4461e13dfedbf7cd695e3`.
CLI binary `527fa3d65b291dc8465687f811a4b5178c2ad6713beb39f878a721e1b5fd5e9b`.

## CG-07 daemon-owned first-group readiness

The daemon startup callback uses the installed managed boot, directory and
archive pool. It verifies the rooted first-group archive, attaches or validates
the exact local initial voter and establishes fresh quorum under the original
startup deadline. The post-barrier check binds the membership to the current
fleet history, local boot and engine. Ingress binding follows the barrier.
The shared read gate waits for a durably applied entry in the current term
before asking for ReadIndex. The prior standalone case timed out when its
first request was issued before Raft accepted current-term reads.

Four native cases and both real RustFS image modes pass. The native operator
case verifies that a retained attachment refuses readiness after loss of its
majority. The RustFS cases verify cold startup, leader loss and retained
restart with the daemon-owned barrier. The fixture supplies capacity attestation
and startup rendezvous. Voters share a host and use development peer
authentication. The backend and VM stop with data retained.

Source manifest `c05e1ebae002008ebd1e84f1ebed98e4a594fe0d0790f493d403ef55c98f8cb6`.
Native record `9adde470d0947965c1b80d6d2642cdfea344fb9aa1553a91447028040cc55095`.
RustFS record `f786339f658c83094826062e2fc8df381e2e145745e78367c566729f8846c91f`.
CLI binary `0ebce06b8f6bffad7035799949a058b126a6474fff817b9acd50a4014eaa97bf`.

## CG-07 rooted first-group archive initialization

The authenticated archive command verifies the original first-group plan and
requires its live selected history. It journals the exact namespace binding
in the fleet root before entering the one-use initializer. An exact retry
verifies the existing registry without creating counters. The native operator
case checks permission, early refusal, conflicting request and replay after
deleting the isolated fixture registry. The retry leaves that registry absent.

Four native cases and both real RustFS image modes pass. The daemon fixture
uses the CLI and admin API for capacity, group publication and archive setup.
Voter startup and retained restart verify the exact rooted archive intent.
The fixture supplies capacity attestation and physical voter activation.
Host-native voters share a machine and use development peer authentication.
The RustFS backend and VM stop with data retained.

Source manifest `88725305464f1766d8eeb4ad52f8c66fa094fb20592fdac0a5577c8619e4eee5`.
Native record `16e6d5d47603355a76248d13dc03b5dbacaab8fd91e51fc52b4ff84b1c57d3d8`.
RustFS record `829fad729d5ea63ad54207daff4eda4cb98353eafdf9f7d390839c532b6157cc`.
CLI binary `7423d3d18c45402114635a9b2fbfdd440f26ebf017d160a7ef988f3be2936f6f`.

## CG-07 public first-group operator workflow

The fleet reservation now binds the exact first-range publication digest in
the same CAS as its admitted birth. Read-only reopen verifies the original
identity, payload, committed projection and directory operation. A changed
publication ID conflicts without advancing either root.

The public CLI and authenticated admin adapter expose standby, reserve and
publish. The process fixture also uses the existing CLI capacity commands.
The native case rejects missing grants, a mismatched namespace, publication
before reservation and a changed plan. It retries the original confirmation
after a lost reply and activates the group for two independent cells.

Four native cases and both independent-process RustFS image modes pass. The
process cases use the actual CLI and admin API for capacity and first-group
metadata. The fixture still supplies the deployment attestation, archive
intent and daemon activation. RustFS hosts the Raft catalog, fleet coordination
and archives. Peer authentication uses the development store. Voters are
host-native processes on one machine. The backend and VM stop with data
retained. Archive initialization and voter activation still need a public
operator workflow.

Source manifest `bf62b059a7d2e172ec81e0549d41a22e185f99f97955bddd02f3957d00382d87`.
Native record `f3b2cde1c6d4a2d498996c9ae5ebd4cbd3bcd1179d477e9891afe25cd6ed6a95`.
RustFS record `be2ccf3175600c6c99f772af9f44aeac69365d850946f35406dc852deb523465`.
CLI binary `33420164f6daa4681ceab857f2da2481620fd93000f0baa4dfc111d42b73d7aa`.

## CG-07 independent managed daemon voters on RustFS

The native daemon process fixture accepts an explicit loopback RustFS endpoint,
bucket and isolated prefix. The same binary passes the development-store
baseline and both RustFS image modes. The RustFS provider hosts the Raft
catalog, fleet coordination and archive records. Daemon peer authentication
uses the fixture's development store. Voters are separate host-native
processes with fixture capacity on one machine.

Both runs start from declared standbys, verify cold RF3 activation before
ingress, and exercise independent reads and writes for two cells in the same
group. Their separate recovery ledgers verify exact retries after a leader
hard kill, retained-disk resume and fresh writes for both cells. The dedicated
backend and VM stop with data retained. Public operator orchestration,
independent constrained voter hosts and sustained-load qualification remain
open.

Source manifest `ef2417a12645458942d9b63bfd53e0ccc52845291e581979889d6fc1f6f20b53`.
Native record `8ecfdf060e7f7b4e03033f673a8a1f06f80b739866638253723a7e253f7aedbc`.
RustFS record `f777af781135ab70da22c164fa3791c00f07f0edf44992fc8c882d359ad53556`.

## CG-07 managed daemon startup and request path

The native daemon now installs its shutdown channel before the Raft host starts.
The stop reporter and HTTP driver share that channel. The asynchronous startup
callback runs after transport installation and before ingress. Cancellation
during that callback closes the installed owner and releases the engine lock.

The process case preserves the original refusal and shutdown checks and now
uses one physical group for two independent SQLite cells. Three daemon
processes start as standbys. The fixture admits capacity using authenticated
observations, confirms the first shared group, persists and verifies the
archive intent, attaches each voter and requires fresh quorum before ingress.
The test performs real TLS replication and follower reads, hard-kills the
leader, verifies exact command retries and resumes the same voter disk.

The first shared-process candidate stalled at enrollment. The first managed
candidate entered the development catalog outside its execution domain. The
second revealed that the managed stop reporter could start before a shutdown
sender existed. The corrected process case and its ledger rerun pass. Three
embedded regressions pass, including non-Raft daemon routing and handoff.
The process case uses the development SQLite object store and fixture capacity.
The earlier RustFS component evidence remains separate. A generic operator
CLI, RustFS with independent voter processes and the broader lifecycle and
release gates remain open.

Source manifest `a28c5abbe7fdb6edc0a6ff858bb8a0538b6e403e6fd4a966a1a9d6bd70df3b0c`.
Process record `357d57f0b5f24c784979cbb9d02ef6ed8cd804252c1d823de92e56c101808f20`.
Recovery ledger record `f5c4927962d15d4f1b0d21ee147c6dcb5e17bda9f20f89039ffe0079c2702d48`.
Embedded regression record `6f088fbe58e22c580fefc12f227391638288c1be46a84305922dd4b5d2cd3896`.

## CG-07 managed group archive provisioning

The SDK now opens archive provisioning for an exact declared managed namespace.
The prepare and verification operations require a live fleet history and its
selected physical birth. The operator persists and syncs the one-use intent
before committing. Verification checks the exact saved binding, identity and
counters without creating metadata. Incomplete initialization still refuses
admission and requires archive authority recovery.

The managed fixture initializes through the SDK, then reopens the provisioning
handle and verifies the persisted intent. The native cases cover missing
archive refusal before activation, rejection of a different intent, archive
shutdown ownership and the two-cell native/Axum request path. The same request
case passes against real RustFS in both image modes with host-native voters
and fixture capacity. Ordinary CLI and daemon startup still need to orchestrate
these operations and wait for actual voter activation and fresh quorum.

Source manifest `32d407be21812e173179dce6541b33266792937133ee059110596017dc9ef26a`.
Native record `14462fe3d49d98c6d9500e71d5ad2516f2752b863defdb74fea4d05c08f3bbb9`.
RustFS record `5644d5020538d0e733fd64e0a38808737bd381bc075cedd433fbfa01823d0c4d`.

## CG-07 managed first-group provisioning

The public SDK observes the exact managed standby and reserves one full-range
RF3 group through existing capacity and separation checks. The prepared
reservation retains the original standby digest and range operation. It can
be reopened from the original request after selection or a lost response.
Publication resolves the immutable birth decision before the directory CAS.
A cancellation cannot displace that Live decision, and an unselected birth
cannot admit a voter. Confirmation and physical activation remain separate.

The cold-start case pauses before range publication and verifies that a
concurrent cancellation sees the same Live birth. A conflicting confirmation
leaves the selected range without Live fleet authorization. The test then
loses an actual confirmation reply, reopens the original reservation and
replays confirmation. It also verifies that a missing published birth refuses
without recreating metadata. The pre-capacity refusal, inventory debt,
physical activation and retained restart assertions remain in place.

Three native cases and the native/Axum request case against real RustFS pass.
The request fixture uses the SDK first-group path from a fresh standby. Both
image forms run in the request and cold-capacity cases. RustFS uses host-native
voters with fixture capacity, and the backend and VM stop with data retained.
The ordinary CLI and daemon workflow still needs archive setup, capacity
admission and activation wiring.

Source manifest `442c44e9b2712f1a911ab187d9906b8301ff76ad620cb6bbdfa44be96567552d`.
Native record `414decd4610f9933539b1697c1d74d0ff7eccc8c2b5f6c22e4ebca6fb55581bf`.
RustFS record `fd4922bb25e0e88fe8c3ff436755f1f631e1659f770f05e855e0b512af87f22e`.

## CG-07 managed namespace standby provisioning

`FleetCatalog::provision_managed_namespace` validates the compiled class and
fleet-bound namespace before pinning the exact declaration request. The same
operation publishes the empty directory and commits the fleet declaration.
The returned registration can be used for managed host startup. Group birth
still requires the existing capacity and placement proofs.

The tests lose the declaration reply after the actual CAS, refuse changed
payload under that identity and replay the exact declaration. A separate cut
commits an unrelated fleet update while the standby head is publishing. A new
request at the new fleet revision reuses the original logical operation and
proves that the standby root is identical. Cold inventory reads the published
standby directly. The managed native/Axum path starts from this SDK setup, then
exercises the existing RF3 birth, activation and two-cell request path.

Six focused native cases and the managed request case against real RustFS
pass. Two existing registry cases now run on the shared fixture and retain
their original replay, scope, missing-page and rollback assertions. The older
legacy target still fails to compile against the retired per-cell host API.
The backend case runs both image formats with host-native voters and fixture
capacity. The backend and dedicated VM stop with their data retained.
Normal CLI/daemon orchestration of first-group reservation and activation
remains open.

Source manifest `c9926108e4504b1ff9a4f220e0b1551824f02108f612f1e3878b23c558b494fe`.
Native record `2dc3943876d3a235550fc1bf76ab432980ebee49b2f98c18f9cf9d854ae10f5c`.
RustFS record `c8193186b5410d301ecbcb7bc5d31f8506cbe4a0996b00922f103e2047a299c5`.

## CG-07 managed router membership refresh

A configured ingress without a local group refreshes its routes through the
existing managed directory service. One bounded turn resolves group authority
against the exact rooted fleet cut. The directory and the receiving quorum RPC
each use a private bounded catalog read turn. The router verifies the authenticated
quorum membership observation and the enrolled physical peers,
including joined-member disk identity. The installed route follows the committed
membership revision and contains the current and outgoing voters. Request grants,
mutation identity, deadline and destination authority stay independent.

The baseline completed voter replacement and timed out waiting for the new
route. The first candidate reached the replacement and exposed a test oracle
that expected the original receipt value after a later committed write. The
corrected oracle verifies the latest value, the exact original receipt, and a
fresh quorum write read back through its returned version token. The native cases
pass (21.31s replacement with a router, 29.53s original replacement
and joined restart, and 68.74s native/Axum requests).

The earlier RustFS replacement attempt failed during settlement with
`FleetCatalog::Overloaded`. Native comparisons reproduced contention while the
receiving RPC still used the foreground catalog turn. The request case passed.
The isolated-reader revision passes both cases against
the same constrained RustFS backend (24.58s replacement and 67.93s
native/Axum requests), in both image formats. The backend and dedicated VM stop
with their data retained. Voters are host-native with fixture capacity. The
older S3-source successor-readiness failure retains its separate evidence.

Source manifest `f2d150294cef69a9959cd0110c5c8b6af540ec82994c71d8e78133fd779284a5`.
Native record `121d71a1d89f54d07cfc17a5a488338f62780d581ef7f0b1a5aa8cae54a670c4`.
RustFS record `ed0a5cde2d751ae85132f1244f41f71a8a09b254c7fc8edbde5fa15a3ad43c37`.

## CG-07 SDK enrollment without a local replica

SDK enrollment shares the same trusted leader selection as commands. The
controller captures the creation identity, initializer inputs and preview before
routing. The destination still checks the route and its actual leader authority.
Remote and locally inspected creation results retain separate ownership. The
SDK returns the exact creation record and achieved durability for either result.

The managed native and Axum case rejects enrollment before the ingress has a
write grant, then enrolls its second cell through the router. The S3 retry
returns the exact original creation record after complete archive coverage.
The ingress has no local replica. Both image formats run. All eight scoped
native cases pass, including the earlier grant, revocation, deadline and
archive checks and the original public counter process case. The managed case
takes 67.41 seconds and the public process case
takes 137.73 seconds.

The same managed executable passes against real RustFS in
72.11 seconds. The immutable backend identity and
signed provider probes pass. The dedicated backend and VM stop with data
retained. Voters are host-native embedded nodes with fixture capacity. The
earlier RustFS successor readiness failure remains open on its recorded source.

The source manifest binds 2,728 inputs to
`7140f54b5c12cfb08195f6cbdaba32588b6fa5ba38828bf77f4b038fe32667c7`.
The native record SHA256 is `b7f9632bafeb505027114f83f3eed9613fca1d2654bb665fda18e5806328341a`.
The RustFS record SHA256 is `71e693b794736a74e6e87969271fd2c49a4e6764af28af0d3f2fb33e10cc5bcd`.
The plan index generator preserves its Purpose line and the index test passes.
Reviewed source preflight still reports drift.

## CG-07 ingress without a local replica

The combined candidate adds automatic read routing and a read-only command
probe on trusted group routes. The probe uses the command grant and selects
one peer before any mutation is submitted. SDK queries preserve consistency
and tokens through bounded replica attempts. Managed hosts can grant reads
after the ingress process has published its current boot.

The first combined candidate on 1b3eac2e passes five routing cases and the
four-process managed native/Axum request case. Both image forms run. The
managed case proves a zero-group ingress, separate read and command permissions,
quorum writes, version floor and linearizable reads, cross-cell refusal, exact
receipt replay after leader stop and retained restart, and refusals after quorum
loss. It runs in 55.56 seconds. The standalone SDK
counter process case fails at the explicit remote strong read with HTTP 503
Unavailable. The earlier read-only candidate fails at the same assertion after
retained restart. The combined failure occurs before the first normal stop.
The runs retain their logs, disk roots and original bounds.

The first combined record SHA256 is `a1216231b0c7128bd2c1c3e5720ade8e83cd2b9636c3bf330cfb800ed9d21114` and its source manifest
SHA256 is `19725e7b24a05bdceba9eb31eef09fd2c762a899ecc4c2a6d1b63312bc06b391`. This candidate awaits real RustFS
qualification. The source manifest and full test outcomes remain tied to this
checkpoint.

The archive denial variant receives a quorum acknowledgement and refuses the
explicit S3 acknowledgement during denied writes. Its exact S3 retry after
restoration fails with a TLS EOF. The temporary counter trace captures that
same transport error on a phase-zero version-floor read to member 1. The
receiver bounds its idle header read, but the original sender retains its
socket while waiting for a request. A focused regression waits through that
idle interval and fails at the first exact retry with no leader selected.
The red record SHA256 is `06b8f40441466b7906a763698555afc60c5ad6a61961d28420ce20d9d599cd48`.

The idle socket fix observes peer closure before the next queued request.
It retires the old connection before transmission and sends each mutation once.
The same expiry regression passes in both image formats. All seven native cases
pass on the final source. The managed HTTP case takes
67.83 seconds and the original counter process
drill takes 140.34 seconds. The managed case returns
a quorum receipt while archive writes are refused, withholds the explicit S3
acknowledgement and returns that acknowledgement on the exact retry after
archive access is restored.

The same managed binary passes against the verified real RustFS backend in
71.19 seconds. Both image formats run with the
archive-write fault limited to the archive prefix. Signed readiness and
conditional probes pass. The dedicated RustFS container and VM are stopped
with their data retained. Voters remain host-native with fixture capacity.
The earlier RustFS successor readiness failure remains open on its recorded
source.

The final source manifest SHA256 is `be85dfd746e849ce98ed0dbe5c8b48ec5b1da83ef0bdd3c4d23e84dc05a3721c`.
The native record SHA256 is `db5a0bf906cdebf2886af58afe92d0ee1111a044050a5b6e1787bab09610be82`. The RustFS record SHA256 is
`91a4a9ff56009aed4281c587f39d1d26d48b1f765298ee881d8184866444fda4`. The subsequent CI-only commit changes no frozen build or harness
inputs. Earlier failed runs retain their original assertions, logs and disks.

## CG-07 lost movement observation

A managed regression drops the first foreground catalog read while the leader
and quorum stay live. The old movement discovery samples each candidate once
and misses the leader. The fixed turn allows one more read-only sweep within
the original deadline. Boot, group binding, sample age and current-root guards
still apply; no membership action is retried within the turn. The test verifies
fresh quorum evidence of durable Seeding, an unchanged physical debit and no
learner activation in both image forms.

The exact red run fails on missing durable Begin; the green run passes in
14.37 seconds. Record SHA256 values:

- Red: `4e141feda39aee045607e844708fcae42e64e85df408099728a95179669a491b`
- Green: `a22c8eef63fb0761e61e44c0ab7e66dd57ad9d8e97c6319c74f9333cdb2080d8`

The complete managed native target passes 19 cases with eight explicit backend
skips in 371.39 seconds. This includes the two lifecycle cases
that failed in the earlier eleven-target campaign. The current source manifest
binds 2,727 inputs to `c719c603552528b9252773b332c601e3a5f6cbe6083bac03257ef63d5618f65d`. The managed record SHA256
is `bec0485c1a712f0773e8d96b9ca0ead487903698efa4df7258230bead7e5e436`.

The real RustFS matrix fails overall. The lost-observation case passes in
17.30 seconds and archived maintenance passes in
82.90 seconds. The S3 source failover case fails
in 14.21 seconds. Both surviving voters report term
2 with no leader at the original five-second readiness deadline. The staged
destination again reports Unobserved on close. The failure logs and disks
are retained. The RustFS record SHA256 is `f6443912bc12a44f2112807756d5c0fcd28afa2f38d2ba28e011796742c5b7a7`. The signed
readiness and conditional probes pass. The dedicated constrained RustFS
container and VM are stopped with data retained. Voters are host-native
embedded Nodes.

The intermittent maintenance and successor-readiness failures passed isolated
replays on the prior source, including the temporary instrumented runs. The
RustFS recurrence leaves successor liveness and the destination close report
open. The discovery fix establishes behavior under a lost observation.
The earlier full native campaign remains failed at its recorded source.

## CG-07 managed public command routing

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

## CG-07 bounded member observation renewal

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

## CG-07 automatic shared-group failure recovery

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

## CG-07 quarantined voter replacement and startup refusal

CG-07-seed-cleanup-01/source-isolated-final binds 2,719 Rust/Python/manifest
inputs above 9070eb15bff289f52f3c3faddd606575fdefcaf1 to SHA256
cc2e83093bfe91439dce852e25e3340108b885f3b79b3a05e1453a9596f3feb1.
The complete seven-target native campaign passes **266 cases with 35 explicit
skips**; the public SDK/native/Axum process/restart gate also passes in 114.80s.
Combined native/public record SHA256 is
60c4918d707c26a95efefa112dfa8400b194c67faa8643b71edd20586b812732.
All cases use the same frozen source; local opt-level 0, debug assertions,
overflow checks, quotas, five-second calls and eight movement turns are retained.

A new physically joined obsolete-voter fixture reproduced two distinct faults.
Saved startup authority refused only after changing the quarantined Raft log;
two fresh diagnostic fixtures identify the same `.raftlog` mutation. Startup now
checks current host authorization and namespace declarations in one catalog turn
before opening the engine. The later engine-bound boot CAS remains mandatory.
Replacement then exhausted eight turns because shared route refresh kept the
quarantined initial endpoint, preventing publication of the admitted learner's
route. Refresh now discards that retained endpoint only with rooted isolation,
including when membership is otherwise unchanged. Votes and physical debt remain.

The new case passes both image forms, requiring exact typed startup refusal,
unchanged fleet root/engine files, S3 payload reads, fresh stable RF3, restored
cell-local images/versions and receipts, lost settlement confirmation/exact replay,
`isolated_sources` attribution and the replacement's physical capacity floor.
The old log remains byte-identical and quarantined. Surviving-original-voter
physical closure is still required; the full managed failover/debt case passes.
The earlier red runs, diagnostic sources/binaries and failed fixture disks remain.

Four exact cases pass against pinned real RustFS in both image forms: isolated
S3 replacement 23.06s, normal replacement/joined restart 36.26s, archived
initial/joined maintenance 84.55s, and public fresh-incarnation recovery after all
old voter directories and verifier cache are erased 18.04s. Record SHA256 is
3ea848910a5843bded56eb0140933cb2bd4120545bc32be37d6c9bee838e0d16.
Signed readiness and conditional operations pass; the one-CPU/two-GiB backend
and dedicated VM are stopped with data retained. These are host-native embedded
Nodes with fixture fencing, not constrained independent voter containers,
permanent Docker launch revocation, sustained load or the full lifecycle gate.

The current campaign passes the previously failing TCP retry, but does not
explain or waive that failure, earlier bounded readiness/maintenance failures,
128-cell enrollment or inspection failures. Format/layout/dependency/docs and
native-only Cellar/Python-library checks pass. Strict library Clippy remains red
with 220 reports (202 dead-code reports). Its only message change is that the
grouped unused-method warning no longer includes the now-used `is_isolated`;
expanded item/count comparison adds no unused item or other diagnostic. No lint
is suppressed. Plan tests retain 35 passes, one generated-index/Purpose failure
and one reviewed-protocol drift error; validate/preflight refuse. SDK CI profiles
and complete public/default/capability gates remain required. CG-07
stays IN_PROGRESS; CG-09–16 and the full hard cutover remain unfinished.

## CG-07 owned idle and restart seed retirement

CG-07-seed-cleanup-01/source-final binds 2,718 Rust/Python/manifest inputs above
acd56a9fc2f14937af269e0c6e1ca54bb047ccb1 to SHA256
9d8d111ff2121fe9802d186b04710039e480c478366b0de9f0d372719e995603.
The preceding candidate has SHA256
7ad9ac05a2da65ade95b300b5462523369f8d847197efce23900eec95fe87480.
Only the managed test's inventory measurement moved before engine close, with an
additional sealed-admission assertion. All production inputs and seven other test
binaries are unchanged; the complete managed target was rerun. Combined record
SHA256 is c53bfd8c79acb345e3a27e64a751b9760c8c818fdb62473896eb1da49bf0433b.

Abandoned seed-upload journals now reconcile without another learner request.
One streaming node-owned sweep validates history path, canonical purpose, engine
creation and immutable namespace archive identity. It shares live sources' group
locks, skips their entered work and reconciles only the original reserve/upload
intent. Confirmed abort precedes syncing the empty record. Backend failure keeps
the intent and observable debt; missing/corrupt/foreign metadata never initializes
a replacement or authorizes retirement of an unknown owner. Cleanup may reopen an
old immutable binding without granting current membership or serving authority.

The existing provisioning bank charges one 272-KiB journal workspace per archived
node. Ordinary 8-KiB S3 admission is per journal. Physical joins precede release of
locks/admission, including panic/error paths. Error capacity cannot stop scanning
other entries; weak lock bookkeeping does not accumulate every past history.
Close stops new sweeps and joins entered work before engine/configuration release.
No configured ceiling, original five-second call/eight-turn bound, stack, profile,
authority fence or durability assertion is relaxed.

The regression first failed at idle retirement: actual denied registry writes
left a pending journal/error that never retried after backend recovery. The fixed
case requires idle retirement, then another retained intent through source close
and fresh-quorum repair. After every old host joins, the source alone reopens that
intent without quorum or another learner request. A gated archive read keeps close
pending and charged after its waiter is cancelled; release permits durable
retirement and joined shutdown without changing fleet authority. Both image forms
run. Existing-only journal tests reject wrong engine/purpose and missing state or
identity without reconstruction.

Seven native targets pass **265 cases with 34 explicit backend/process skips**:
managed 14, snapshot 5, archive-controller 16, archive/recovery 31, host 3,
storage 16 and execution 180. Two targeted library journal tests pass. The final
focused cleanup case passes in 32.52 seconds and complete managed rerun in 221.10
seconds. Current public SDK/native/Axum process/restart passes in 132.07 seconds;
its counter executable and process evidence remain retained.

Four cases pass against pinned real RustFS at one CPU/2 GiB after signed
conditional-operation checks: replacement/restart 36.48s, initial/joined
maintenance 80.70s, refusal/failover plus idle/solo-restart cleanup and cancelled
close 36.15s, and fresh recovery after erasing every old voter directory/verifier
cache 17.66s. Both image forms run. Record SHA256 is
1fb2db0919585b80bb19311abdf382597a21acb154f59382021c176864590d7e.
Backend/VM are stopped with data retained. These embedded-voter cases and contract
fence do not qualify constrained independent Linux voters, permanent Docker launch
revocation or sustained load. The public SDK drill separately uses native processes.

Format/layout, dependency/docs and native-only Cellar/cellar-python checks pass,
including the unavailable Python sentinel. Strict library Clippy retains **220
diagnostics (202 dead-code)** and the same 206 normalized diagnostic keys, with no
added keys or suppressions; the seed module path move is normalized. Plan checks
retain 35 passes, one generated-index/Purpose failure and one reviewed-protocol
drift error; validation/preflight still refuse the drift.

TCP retry passes this complete execution target, fifteen earlier exact repeats
and 33 checkpoint cases with frame/stage error context. Its original overload
failure remains unexplained. Earlier five-second readiness, eight-turn maintenance,
128-cell enrollment and inspection failures remain open; passes do not waive them
or establish a failover SLA. Full performance/library suites were not rerun here.
Finish fenced failed-voter/survivor-debt orchestration, the remaining public gates,
capability/default cutover, remaining release profiles and CG-09–16. CG-07 remains
IN_PROGRESS and PR #160 remains draft.

## CG-07 managed S3 learner data plane and source failover at acd56a9f

Capture CG-07-managed-s3-seeding-01/source-candidate binds 2,717
Rust/Python/manifest inputs above 41a67844278247df18c16df3c781a3c60bc6dd71
to SHA256 65cbd074ab06749b4849ef06d38c23e0df396385c63e3b74bc6fb48159cdf5e7.
No original five-second call, eight-turn reconciliation bound, profile, quota,
stack setting or authority fence changes.

Managed `from_s3` activation now uses the existing fresh ReadIndex checkpoint
authority, a separate fsynced seed-upload journal and the established namespace
archive registry. A pending generation and inherited parent graph protect the
complete checkpoint. The authenticated grant binds its ticket/reference and
snapshot. The destination independently verifies S3 bytes through its serving
receiver before READY1; live source checks precede End and the existing
boot/catalog/engine installation fence remains required. This transfer supplies
neither a disaster-recovery head nor an S3 command coverage proof. Data modes are
explicit marker-1 fields; missing/old shapes and peer-mode fallback refuse.

Source jobs retain transport, metadata, read, checkpoint and S3 admissions through
physical completion and cleanup; close stops new admission and joins source work
with transport. An uncertain retirement retains its journal and observable debt;
a later seed reconciles the exact intent before allocating again. Automatic
abandoned-journal discovery/cleanup on idle nodes and restart remains unfinished.

An exact red regression reproduced `Unconfirmed(Invalid("conflicting provision
binding"))` after source failure: coalescing incorrectly included source
member/node hints. The fixed comparator preserves destination boot/member/permit
and data mode while reauthorizing the next source. Entered attempts retain their
original hint/deadline. Tests require real target-only payload refusal before
attachment, retained preparation/debit, no peer fallback, source-service stop,
fresh election and completed membership repair. Replacement-local reads check
actual responder, original values and versions; new writes require S3 proof.
Catalog settlement still refuses without a live physical-inventory proof from an
offline surviving original voter and retains the debit. No completion guard is
relaxed. Normal full settlement, lost-reply replay, drain and retained restart
remain covered separately.

The frozen-source native campaign has **264 passes, one failure and 34 explicit
backend/process skips** across seven targets: managed 14, snapshot 5,
archive-controller 16, archive/recovery 31, host 3, storage 16 and execution 179.
`checkpoint_tcp_failed_transfer_allows_raft_retry` fails with transport
`Overloaded`; its unchanged focused repeat passes in 7.17 seconds. The full
execution run remains failed. Snapshot process-kill, partial-install, ownership
and memory/pin gates pass. All source inputs remain unchanged.

The same binaries pass four explicit cases against pinned real RustFS constrained
to one CPU and 2 GiB, after verifying signed conditional operations. S3 replacement
and retained restart take 34.15 seconds; archived maintenance's initial/joined
voter matrix takes 85.93 seconds; payload-refusal/source-failover/debt checks take
33.04 seconds; public recovery after erasing every old voter directory and verifier
cache takes 17.56 seconds. Both image forms run throughout. Backend and dedicated
VM are stopped; data is retained. Record SHA256 is
b7d35381cdc4d2a387ce4bfcca28b12c5841c02288d3625364a3891ba52c88f2.
An earlier backend setup returned S3 HTTP 503 after health readiness, before any
Cellar case; that failure is retained. The second setup first checks a bounded
signed read, then the conditional probe. These use embedded Nodes and a contract
fence, not independent voter containers, permanent Docker launch revocation or
sustained load; no five-second failover SLA is established.

The extent failover case previously exceeded its five-second ready-successor
window in a complete managed run. Three isolated repeats and subsequent complete
managed runs pass; no failure snapshot was triggered in those repeats. Root cause
remains unresolved. Bounded owner observations now run only after a failed window
and cannot rescue/extend it. The earlier eight-turn archived-maintenance failure
is also preserved. Later passes do not waive either failure.

Strict library Clippy retains **220 errors** versus 221 at the preceding
checkpoint, with no added diagnostic keys or production suppressions. The one
removed error is the seed client's argument count; all production inputs match
the candidate. The 202 dead-code errors remain. Plan tests still have 35 passes,
one generated-index/Purpose failure and one reviewed-source drift error. Public
SDK process, full performance and full library gates were not rerun; prior
128-cell enrollment and inspection failures remain open. CG-07 stays IN_PROGRESS
and PR #160 stays draft. Finish automatic seed debt retirement, fenced failed-voter
and public lifecycle/capability/default gates, then the remaining release profiles
and CG-09–16. See [remaining work](remaining-work.md).

## CG-07 archive ownership for post-startup groups

Capture CG-07-dynamic-archives-01/source-final binds 2,714 Rust/Python/manifest
inputs above 8c0a42f02eb78060adaa5c87472ab34f95b6e5c7 to SHA256
9cb0776c7a699f6e7af5f1aa6ed03d3bd09f277967a6bcbb5d70051a12d39f7d.
No request deadline, quota, profile, authority fence or durability assertion changes.

Canonical startup retains archive provisioning resources for groups added after
boot, including a node that starts with no groups. Preparations share the original
archive pools and independent verifier image inventory. Both serving and verifier
store bounds are charged to the existing provisioning bank. The selected birth,
boot/deadline and existing catalog archive identity/counters are checked before
local bootstrap. Missing metadata never allocates a replacement archive.

Preparation retains the private archive store with its physical group owner
through retries and learner installation. Actual attachment transfers it directly
to the controller's bounded archive lane, avoiding a recursive wait on that actor's
public registration queue. Existing preparation and archive joins retain those
resources through node close; immutable provisioning resources release after the
worker joins. Attachment alone grants no S3 acknowledgement or fresh quorum.

The managed target passes twelve cases. New cases cover RF3 startup from empty
nodes, independent cells and explicit S3 settlement in both image forms; missing
archive refusal without genesis/catalog revision changes; explicit synced operator
initialization and retry of the same birth/capacity slot; and a held archive GET
that keeps joined close pending after its waiter expires. Quorum writes still
complete independently of asynchronous archival. These use real native Nodes,
SQLite and TLS with an instrumented in-memory object store. They do not qualify
managed S3 learners, managed archived replacement/maintenance, permanent Docker
fencing or sustained RustFS load.

Seven native targets pass 263 cases with 31 explicit backend/process skips:
managed 12, archive-controller 16, host 3, storage 16, archive/recovery 31,
snapshot 5 and execution 180. Sources remain unchanged after the serial campaign.
The same binaries pass the public recovery drill against pinned, constrained real
RustFS in 30.33 seconds for both image forms after erasing every source voter
directory and old verifier cache. Backend and dedicated VM are stopped afterward.
That drill uses embedded Nodes and a contract fence; it does not qualify the new
managed archive cases against RustFS, permanent Docker launch revocation,
independent voter containers or sustained load. The public SDK process drill,
full performance suite and full library suite were not rerun at this checkpoint.
The preceding 8c0a42f0 performance/inspection/release gaps remain open.

Formatting, layout, native dependency/docs checks and the native-only
Cellar/cellar-python library check pass, including an unavailable Python sentinel.
Strict Cellar library Clippy still fails with 221 errors (202 dead-code), down
from 222: the retained preparation wrapper removes the private-interface error;
no diagnostic key was added or suppressed. Plan tests retain 35 passes, one
index/Purpose failure and one reviewed-protocol drift error; validation/preflight
still refuse the stale generated index. No source anchor was silently refreshed.

The next functional priority is the currently refused exact-reference S3 learner
path, including retained closure/receipt proofs, destination boot/catalog checks,
physical install, catch-up and fresh quorum before promotion. Full public
lifecycle/default-backend gates, archive hold retirement, live manifest-page
collection and CG-09–16 remain required. CG-07 stays IN_PROGRESS; PR #160 stays draft.

## CG-07 owned workspace directory barriers

Capture CG-07-wide-enrollment-01/source-08 binds 2,712 Rust/Python/manifest
inputs above d268cf0bfc78c49f8c2d48f17a4a735f42028d81 to SHA256
f9449fbd8937d6a2b5f9a871859bf1e7469b767df51f86f8720a2fb8fff61c57.
The original optimization, stack, quotas, deadlines and assertions remain.

Workspace creation now reuses the complete ancestor barriers established by its
open, exclusively locked extent inventory. It still syncs the new child and
parent before returning the admitted owner. Rooted private initialization and
execution validate and retain that exact directory owner; arbitrary materialized
paths keep their full directory preparation. Anonymous read-cache directories
use ordinary creation because they supply no recovery or persistence authority.
Failed child/parent barriers still quarantine and retain the complete reservation.
Last-pin cleanup still joins physical handles and syncs deletion before refund.

Preserved regressions fail before the fix: workspace creation performs twelve
directory syncs rather than the two required barriers, and native initialization
fails on the redundant ancestor sync. The new tests also inject child and parent
sync failures separately and verify conservative charges, quarantine and recovery.
The native case commits both initialization and a subsequent SQL mutation while
redundant ancestor syncs remain armed to fail.

At this source, five LTX targets pass 66 cases with four owned child helpers
ignored. Eight shared-group native targets pass 262 cases: execution 180,
storage 16, archive/recovery 31, archive-controller 16, managed 10, host 3 and
public SDK/native/Axum/restart 1, plus five snapshot cases. Their 31 explicit
backend/process skips remain recorded. The public drill preserves its original assertions
and eighty additional writes on each side of restart; it passes in 155.92 seconds.
The affected legacy execution targets also pass: 21 execution and 50 rooted
cases, with five explicit backend/process skips. These protect the existing
callers during the cutover; predecessor removal remains required in CG-16.

The original performance suite has eighteen passes and one failure in 439.35
seconds. Both 32-cell scenarios, warm-cache checks and the sixteen-root pressure
case pass. Occupancy 128 still fails enrollment with ambiguous
Runtime(DeadlineExceeded), with 58 cells observed under the original 60-second
population budget and five-second call cap. Earlier diagnostic sources and their
exact executables remain preserved. Stage timing isolated workspace setup and
manifest preparation; publication and collection were smaller in that sample.
Two bounded-pipeline experiments failed to clear the deadline and were removed,
along with all temporary diagnostic logs and the duplicate diagnostic test. The
original scheduler and the applied-equals-committed enrollment fence remain.
These measurements do not establish a production latency or density envelope.

All binary targets, native-only Cellar/Python compilation with an unavailable
Python sentinel, LTX feature-off/all-feature builds, strict LTX library Clippy,
formatting, layout, dependency isolation, docs and diff checks pass. Strict Cellar
library Clippy retains 222 diagnostics, including 202 dead_code, with unchanged
code/message/primary-file keys. The full library was not rerun; its prior
inspection-protocol failure remains open. The plan suite retains 35 passes,
one generated-index/Purpose mismatch failure and one reviewed-protocol drift
error; validate/preflight stop at the stale index. No suppression or automatic
baseline refresh was added.

The exact archive executable passes public recovery against real pinned RustFS
in both image forms in 19.30 seconds. Every source voter directory and the old
verifier cache are erased before fresh-incarnation restoration. Complete images,
receipts, client floors/S3 minima, fresh-quorum activation, old-token refusal,
new writes and retained restart pass. Backend identity/resources and a signed
conditional-S3 probe are checked; its one-CPU/two-GiB container and dedicated VM
are stopped afterward with data/evidence retained. This embeds three native Nodes
and a contract fencing authority. Permanent Docker launch revocation, independent
voter containers/hosts, managed recovery and sustained load remain unqualified.
Record SHA256 is
38e947c6a5c1e5c053fd47798a4d678f95dba2f72afadf7d427566a1e4ec712e.
Source-after comparisons remain unchanged.

CG-07 remains IN_PROGRESS and the full hard cutover is incomplete. Next functional
priorities are managed dynamic archive ownership, its stop/drain integration and
exact-reference S3 learner activation. Continue measuring physical image/page
preparation before selecting the next performance change. Live manifest-page
collection, the remaining public/default-backend gates and CG-09–16 remain required.


## CG-07 archive metadata and warm verifier checkpoint

Capture CG-07-wide-group-diagnosis-01/source-08 binds 2,712
Rust/Python/manifest inputs above 3dc6e25cb23a87a263edf865d24c626d087ebee4 to
SHA256 4b55360d8d064379763d69f3d57b13b34074bef86b9328568da773b636b775e0.
Fetched main remains 981a5417d9359d1f992e2dbf214eec7d221b6777, zero commits
behind. Original optimization, stack, deadlines, quotas and assertions remain.

Immutable extent-root and manifest-page owners now remember their own completed
file and directory barriers. Every preparation still reads and checks the bytes;
only redundant barriers for an unchanged immutable file may be reused. Reopening
clears the observation, failed reads invalidate it, metadata-only reads establish
no persistence, and failed page barriers retain quarantine. This grants neither
an image seal nor remote coverage by itself.

The independent verifier keeps eligible files as a charged cache below one quarter
of each existing inventory ceiling. Any dimension reaching pressure, or orphan
objects without roots, enables its ordinary owned collection. Serving collection
remains eager. No pins, banks, capacity or cached archive proofs are added; remote
GETs, complete closure and independent replay remain required. Buffered extent
imports now check supplied bytes before destination reuse. Previously a valid
warm destination could hide wrong supplied bytes of the same length.

Preserved regressions demonstrate redundant root/page syncs, complete idle cache
eviction and acceptance of wrong buffered input before their fixes. New public
archive coverage forges incoming extent bytes while preserving valid object/index
hashes and the original expected extent identity: both cold and warm verification
refuse, while the original four-cell archive remains usable. The first attempt at
that public test failed fixture setup by assuming one chunk per extent; it is
preserved as a setup failure, not evidence of the production bug.

The same source passes six LTX apply and 39 extent-store cases, with one owned
child helper ignored. Seven native targets pass 256 cases with 29 explicit
backend/process skips: execution 179, storage 16, archive/recovery 31,
archive-controller 16, managed 10, host 3 and public SDK/native/Axum/restart 1.
The public process drill passes in 179.65 seconds with its original assertions
and eighty additional writes on each side of restart.
Five additional snapshot cases pass, with two owned child helpers ignored:
partial-install refusal, complete multi-cell cuts, extent restart, pin/memory
bounds and whole-group learner/joint membership. All binary targets, native-only
Cellar/Python compilation with an unavailable Python sentinel, LTX feature-off
and all-feature builds, and strict LTX library Clippy pass. Strict Cellar library
Clippy still fails with 222 diagnostics, including 202 dead_code; diagnostic
code/message/primary-file keys are unchanged. No suppressions or baseline refresh
were added. The full library was not rerun; its prior inspection-protocol failure
remains open.

The exact archive executable passes public recovery against real pinned RustFS
in both image forms in 33.02 seconds. All source voter directories and the old
verifier cache are erased before restoring complete images/receipts, client
floors and S3 minima into fresh incarnations. Fresh quorum activation, old-token
refusal, new writes and retained restart pass. The backend enforces one CPU/two
GiB with checked identity/resources and a signed conditional-S3 probe. Backend
and dedicated VM are stopped with their data retained. This uses three embedded
native Nodes and a contract fencing authority; permanent Docker launch revocation,
independent voter containers, managed recovery and sustained load remain
unqualified. Record SHA256 is
b8adf98e6f019da487c493a71c2107ec4beee255e531b17723eaf074ff4d082f.
All source-after comparisons remain unchanged.

Formatting, layout, dependency isolation, docs and diff checks pass. The plan
suite has 35 passes, one generated-index/Purpose mismatch failure and one
reviewed-source drift error for the consensus protocol. Validate/preflight stop
at the stale generated index. These failures remain explicit; no automatic
baseline refresh or task-status changes were made.

The complete performance suite improves to eighteen passes and one failure in
483.34 seconds. Both 32-cell scenarios now pass their original enrollment, S3,
duplicate-receipt and every-voter read checks. The 16-root pressure case and new
warm-cache case also pass. The matrix reaches occupancy 128, then fails enrollment
with ambiguous Runtime(DeadlineExceeded); the observed large group has 44 cells
under the unchanged 60-second population budget and five-second per-call cap.
Quorum and collector observations remain healthy at that sample. This does not
establish a cause, fixed cell-count boundary, density claim or a passing load gate.
Earlier source failures, exact executables and fixture disks remain preserved.

CG-07 remains IN_PROGRESS. Measure the remaining enrollment, checkpoint and
collector work, then finish managed archive attachment, exact-reference S3 learner
activation, live manifest-page ownership, the remaining public gates and full
Docker-fenced qualification. The default backend, capability/deployment/read
decisions and CG-09–16 remain required; see [remaining work](remaining-work.md).

## CG-07 owned live extent collection

Capture CG-07-live-collection-01/source-01 binds the implementation to SHA256
a1ea8aa8ab265debeef983589f385eb8cf4e398327fd4ee22aaa0eb7af05a931.
Its initial and regression checks use the same frozen Rust/Python/manifest inputs,
above d04471d8e3e30f9d64f3d6a1ffacd6234bda0333. Fetched main remains
981a5417d9359d1f992e2dbf214eec7d221b6777, with zero commits behind.

The shared host now owns one live extent collector across the serving and
independent archive-verifier inventories. Only complete startup reference
adoption constructs its inventory grants. Complete current/checkpoint graphs,
readers, staged receives and uncertain publications retain their pins. Disposable
empty workspace roots become transient without changing an existing retention.
The collector uses the existing control bank: a fixed 4 KiB service charge plus
checked temporary marking memory, with at most sixteen physical deletions per
turn. Idle inventories require no filesystem scan; last-reader release and new
object imports rearm collection. Marks include only locally present objects,
protecting dependencies imported under a pinned, incomplete receive. Physical
calls retain their inventory handles, working memory and shutdown-registry lease
until completion. Capacity refunds follow deletion and directory sync. Typed
advisory observations distinguish deferred admission, progress and stopped/failing
collection from durability proof.

Initial checks pass: locked library compilation, six LTX apply cases, 36 LTX
store cases (one owned child-process helper ignored), the physical close test and
the RF3 pressure case. The close test holds a real deletion barrier, cancels a
public close waiter, verifies that storage and both admission charges remain,
then releases the barrier and joins closure with zero outstanding charge.
The RF3 case uses two groups/three independent cells and a 16-root ceiling in
both physical image roles. Eighty additional commands cross that bound while
retaining the original request deadlines and S3 minimum for the last write.
Duplicate receipt/witness/result checks and minimum-version reads from every
voter survive; obsolete serving objects drain below sixteen with healthy
collection observations. It passes in 107.03 seconds excluding compilation.

The same-source seven-target native regression passes 254 cases with 29 explicit
backend/process skips: storage 15, archive/recovery 30, archive-controller 16,
execution 179, managed 10, host 3 and public SDK/native/Axum/restart 1. The public
process drill passes in 174.22 seconds, retaining eighty additional writes on
each side of restart and every original assertion. All three startup ownership
cases pass. All binary targets, formatting, layout and dependency checks pass.
The complete performance gate still fails: sixteen cases pass, including the new
collector pressure case, and two wider-group cases fail. Both stop during
32-cell-group enrollment after the observed manifest reaches 31 cells. The matrix
case returns ambiguous Runtime(DeadlineExceeded); isolated diagnostics returns
"deadline has elapsed" with a changed leader and the sampled member not ready.
The original request budgets remain unchanged. Occupancy 128 and the earlier
explicit-S3 failure point are not reached. Preserve this earlier refusal as well
as the source 23 S3 failures; neither cause nor scalable performance is qualified.
Strict library Clippy still fails with 222 diagnostics, including 202 dead_code.
Code/message/primary-file diagnostic keys match public-recovery 14 exactly,
without line offsets; nothing is suppressed or baseline-refreshed. Native-only
cellar/cellar-python compilation with the unavailable Python-runtime sentinel,
LTX feature-off/all-feature compilation, strict LTX library Clippy and the existing
control-service registry unit case pass. Final docs checks pass.

The exact same-source archive executable passes the public recovery case against
real pinned RustFS in 24.47 seconds, in both image forms. RustFS enforces one CPU
and two GiB; its image, owner, mounts, loopback endpoint and resource limits are
checked, alongside an independent signed conditional-S3 probe. The fixture deletes
all source voter directories and the old verifier cache, restores complete images
and receipts, obtains fresh-quorum activation, preserves S3 minima/client floors,
rejects old tokens, serves new writes and survives retained restart. Its backend
and dedicated VM are stopped, retaining owned data and evidence. The test embeds
three native Nodes with independent domains/disks/TLS and uses a contract fencing
authority. It does not qualify permanent Docker launch revocation, separate voter
containers/hosts, managed recovery or sustained replication/reclamation load.
Both source-after comparisons have no changed inputs. Exact initial and broader
regression executables, failed sources/logs and the recovery artifact are retained
in the checkout-private qualification directory. Original deadlines, quotas,
stack and optimization profiles remain unchanged. This does not collect manifest pages,
authorize Raft log truncation or bypass offline engine reclamation admission.
It does not qualify sustained load, current-source Docker permanent fencing,
managed S3 learner activation or the default-backend cutover. CG-07 stays
IN_PROGRESS, CG-09–16 TODO and PR #160 draft.

## CG-07 shared consumer and retention checkpoint

Capture CG-07-perf-resume-01/source-inputs-25 binds 2710 Rust, Python,
manifest and profile inputs (2142 Rust files) to SHA256
8c2d708074d6c265dd69fe9edb62694495998a517900700a1cc3e97e97eefb92.
Checks 25 and 26 finish with no changed inputs. The source includes fetched main
981a5417d9359d1f992e2dbf214eec7d221b6777, zero commits behind.

The performance/fleet consumer port now models frozen canonical ranges with
occupancies 1–128, distinct logical histories and physical group witnesses,
explicit RF3 initialization/enrollment and actual physical resource observations.
Managed setup journals its standby/range/archive intents. Wider groups retain
the original replay bank by reducing tail batch sizes. Configured archive ceilings
are reported separately from the effective serial lane. These are implemented
adapters, not deployed managed or density/performance qualification.

Image ownership now releases excess permanent host references. Exact terminal
command/enrollment reconciliation retires disposable previews while retaining
their pins through physical completion. A fresh applied image transfers its pin
into the complete manifest graph. Startup recovers every physical group's current
and checkpoint closures before releasing conservative filename retention, before
routes become reachable. Existing image retention, uncertain publication and
reader/checkpoint pins remain authoritative. Fresh extent preparation follows the
same pin ownership contract and preserves an existing root's retention. This
does not add a live extent collector or bypass offline reclamation admission.

- All Cellar binary targets compile, clearing the original 27 performance errors.
- Seven native targets pass 254 cases with 29 explicit backend/process skips:
  archive/recovery 30, archive-controller 16, shared execution 179, host 3, managed 10,
  public SDK/native/Axum/restart 1 and storage 15. The public drill now adds 80 real
  zero-delta commands on each side of retained restart, preserving all original
  receipt, token, remote-read, business-value and joined-shutdown assertions and
  the original 64-image bank. It passes in 135.69 seconds at this source.
- All three startup ownership/barrier unit cases pass. Full-library execution is
  not rerun here; its prior retired inspection-protocol failure remains open.
- All six LTX apply cases pass at source 22; both changed LTX Rust files are
  identical in source 25. The new regression keeps live readers during collection,
  reclaims a fresh unpinned root and preserves exact existing host retention.
- Native-only cellar/cellar-python libraries, LTX feature-off and all-feature
  compilation, formatting, layout, dependency isolation, docs and diff checks pass.
- Python tooling passes 282 cases with one environment skip. Its Python sources
  are unchanged since that source 20 run.
- Strict library Clippy still fails with 222 diagnostics, including 202 dead_code.
  Message/primary-file diagnostic keys exactly match public-recovery 14; no
  diagnostic is suppressed or baseline refreshed. Plan drift and full CI remain.

The new complete native performance gate is still **failing**: source 23 has
fifteen passes and two failures, both at logical cell 6 in the one-cell second
group's explicit S3 command alongside a 32-cell first group. The original
five-second command deadline returns an ambiguous DeadlineExceeded. Occupancy 128
is not reached. Isolated instrumented 32/source 24 passes in 83.44 seconds, which
does not clear those failures. Its complete 32-cell checkpoint verification takes
roughly 4–5 seconds, and one full background job reaches 5.206 seconds. Preserve
those failures and investigate repeated verification and archive-lane interference
without raising deadlines, changing requested durability or removing assertions.

No current-source Docker/RustFS, permanent launch-fencing, independent-host,
managed S3 learner, sustained load or release claim follows. The older executable
disaster operator still needs its whole-group port, disjoint identities, serial
catalog activation and explicit partial inventory. Dynamic managed archive
ownership, live extent collection, default-backend capability/deployment/read
policy and four original public gates remain open. See
[remaining priorities](remaining-work.md). CG-07 remains IN_PROGRESS, CG-09–16 TODO,
the goal active and PR #160 draft. Both requested MR basenames stay excluded and
the eight user diagrams remain unchanged and unstaged.

## CG-07 combined public recovery qualification

Runtime commit f644d9d69b24bc44024a88f33f80dc21fbffd3d0 contains the recovered
public-node startup and joined late-error cleanup. It includes fetched main
981a5417d9359d1f992e2dbf214eec7d221b6777, with zero commits behind. Final capture
CG-07-public-recovery-14 binds 2136 Rust inputs to manifest SHA256
009e0283f389ca2b4c09220a0c2b708e9e673cd350fa4d1701c1281530033977.
Its Rust mapping exactly matches startup-13, which ran the new ownership unit
case and the full library. Documentation publication does not change that code.

- Native archive/recovery: 30 passed, six backend cases ignored, 72.45 seconds.
- Managed Node lifecycle: ten passed, retaining all ten parent names.
- Archive controller: sixteen passed, twenty backend cases ignored.
- Host: three passed. Existing public SDK/native/Axum process/restart gate: one
  passed. Counter build and locked native-only cellar/cellar-python library
  compilation pass, including the unavailable Python-runtime sentinel.
- All three startup unit cases pass. Full library: 1062 passed, the same retired
  inspection-protocol oracle failed, two Docker cases ignored. The new held-worker
  regression first reproduced return-before-join, then passed with explicit
  public-service closure before infrastructure teardown.
- Performance-tool Python: 277 passed and one environment case skipped.
  Multi-Raft tool Python: ten passed and one Docker case skipped.
- Formatting, layout, dependency isolation, docs and diff checks pass.
- Strict Clippy remains 222 diagnostics, 202 dead_code, with unchanged diagnostic
  keys. Plan checks remain 35 passed, one generated-index/Purpose mismatch and
  one reviewed protocol.rs anchor error. Package commands still fail on the 27
  unconverted performance-runner errors; emitted integration artifacts are not
  package-build successes. No failure was suppressed or baseline refreshed.

Capture CG-07-public-recovery-rustfs-15 passes the exact public SDK recovery case
against real pinned RustFS in 19.70 seconds, covering both image forms. It uses
the same final archive executable and Rust mapping. The owned backend enforces
one CPU/two GiB, verifies its image, label and loopback endpoint, and passes an
independent signed S3/conditional-object probe. The test removes all source voter
directories and the old verifier cache, independently verifies the fixture fence,
restores complete images and receipts, obtains fresh quorum activation, preserves
S3 minima and client floors, rejects old tokens, serves new writes and survives
retained restart. The backend is stopped with its owned data volume retained.

This is one test process embedding three real Nodes with separate domains/disks
and TLS. Its fencing authority is a contract fixture. It does not qualify the
Docker controller's permanent launch fence, separate voter processes/hosts,
managed disaster recovery, multi-group recovery orchestration or sustained
replication/reclamation load. Those original requirements remain open. No stack,
existing request deadline, admission quota or optimization profile was raised.
Earlier source/failures, exact executables and the unqualified consumer WIP are
preserved. Keep CG-07 IN_PROGRESS, CG-09–16 TODO and PR #160 draft.

## CG-07 public recovered-node startup checkpoint

Capture CG-07-public-recovery-11 binds 2136 Rust inputs (manifest SHA256
8380c9d3a06e0c54eb7942589d1fb4c6799f40611a478707f2c7c18a0085e658).
The full native archive target passes 30 cases in 62.34 seconds; six explicit
RustFS cases remain ignored. All 36 inventory names remain. Managed ten, archive-controller sixteen (twenty backend cases ignored), host
three, public SDK one, counter build and native-only cellar/cellar-python library
checks also pass at this source. Five static checks pass. Strict Clippy retains
222 diagnostics, 202 dead_code, with unchanged keys. Plan checks retain their
known index/Purpose and reviewed-anchor failures. A subsequent startup cleanup regression reproduced return-before-join under a
held admitted control worker. The startup guard now retains the public service
and awaits its close before controller/runtime/SQL/log teardown. Successful
startup relinquishes that temporary clone. All three startup unit cases pass at
CG-07-public-recovery-startup-13, including the new held-worker/physical-lock
regression and both original cases. Its 2136-input manifest SHA256 is
4bb879b357ee3f1d0e406d0f73448a70d72c9de35ca44d731ef5dc871a217441.
The full library executes 1065 cases: 1062 pass, the same retired inspection
protocol oracle fails, and two Docker cases remain ignored. All original unit
names remain; the new cleanup case adds one. Final combined component checks
must rebind this cleanup correction before publication.

The public `RaftCellsServices::recovery` capability resumes an already seeded
unmanaged log. Startup finds the original request through the selected birth,
immutable genesis and catalog binding, independently verifies the infrastructure
fence and checks the exact local seed receipt/engine/disk. Missing authority or
rejected verification refuses. Managed recovery cannot use this unmanaged lane.
One node-owned control service uses the existing control bank and original
queued permit deadlines to request actual Raft activation, reconcile its exact
catalog intent and join entered work before consensus/storage shutdown. Startup
can return pending; that supplies no serving or quorum authority. Retained
restart verifies the original fence and pins again. Exact activation receipts
remain necessary when another recovery owns the namespace lock.

The added public Node case runs materialized and extent images after removing
every old voter directory and the old verification cache. It restores archived
SQL state, original receipt bytes and S3 minima, client floors and fresh logical
histories. It verifies independent cell state, actual group identity/occupancy,
archive-required policy and one actual group per voter. Old valid tokens refuse
against the new incarnation; new writes use it and duplicate calls do not rerun
the callback. A retained restart repeats serving checks. A held independent
verification call proves shutdown joins that entered recovery work. Its authority
is a contract fixture; this is not Docker permanent-fencing qualification.

Broader archive verification exposed fixture contract drift. Reopened recovery
and outage gateways now preserve the full immutable namespace configuration,
including image mode. The strict no-selected-archive assertion now expects the
exact Unconfirmed contract introduced by f50b99af rather than the retired Invalid
string; it still rejects success and all other failures. The RPC fixture now
supplies a sibling namespace scratch base instead of nesting it inside a cell's
disposable SQLite directory. The original strict cleanup refusal stays intact.
The outage case now reports an early command result instead of masking it behind
a settlement wait; all original outage/deadline, callback, capacity, archive and
reclamation assertions remain. Earlier failures and executables are retained.
The performance metric inventory also includes transport_query_reply_write;
the performance-tool campaign has 277 passed and one environment skip;
the multi-Raft tool campaign has ten passed and one Docker skip.

This does not complete CG-07, its four missing original public gates or the
combined MVP. Package builds still fail on the 27 unconverted performance-runner
errors. The larger executable consumer port is preserved as unqualified work
outside the checkout, with its original files and diff, for the next slice.
Current-source Docker/RustFS recovery/load and full release qualification remain
open. The full library run retains the known inspection-oracle failure. See the ignored public_recovery_work/perf_cutover_work continuation
entries for exact captures, backups and any live handles.

## CG-07 public physical-group controls checkpoint

Capture CG-07-group-controls-02 binds 2133 Rust inputs (manifest SHA256
09f5447c42949fb573c90a8d28fc0d5fc7ecedaa3bf10efbaef705d6c19029cd).
All ten managed Node cases pass in 118.40 seconds; all nine parent names remain.
Shared execution passes 179 cases with the two original owned child fixtures
ignored, retaining all 181 names. Host three, public SDK one, counter build,
locked native-only cellar/cellar-python compilation and five static checks pass.
The public process drill retains native/Axum ingress and restart and now checks
metadata from all three actual physical owners as well as resource counts.

The SDK exposes local physical-group observation with identity, role, membership
revision, persisted/committed/applied positions, installed checkpoint, retained
entry count and applied cell occupancy. One owner turn and one short durable-cache
read produce the sample. Entry counts do not estimate disk bytes; these samples
grant no quorum or serving authority. Archive-controller publication is sampled
separately and cannot supply retained physical coverage or compaction permission.
No per-cell Raft owner, worker or admission bank was added.

Guarded leadership transfer retains the caller's exact term, membership revision
and target. The deadline starts at invocation and the actual owner checks it
after queuing, before mutation. Success establishes admission, not election;
expiry or close after dispatch does not establish whether a handoff was admitted.
Both image forms pass stale term/revision, invalid target, timeout bounds,
unpolled expiry and closed-owner refusals, followed by a real election, duplicate
receipts, old-token reads and new writes through the new leader. A held real
engine sync in the existing membership-floor case proves queued expiry refuses
the transfer while retaining the original leader. All original assertions remain.

The first capture retains two new private-method diagnostics and the explicitly
interrupted failed compiler. The corrected source completes its checks without
internal-test cfg or a raised timeout, quota, stack or optimization setting.
Package commands still fail on exactly 27 unported performance-runner errors;
emission and direct execution of an integration artifact is not a package pass.
Strict Clippy remains 222 diagnostics, 202 dead_code, with unchanged diagnostic
keys. The plan suite remains 35 passed, one index/Purpose failure and one reviewed
anchor error; all seven recorded inputs of those failures match 824e0a20.
No baseline or assertion was relaxed. Full-library and real RustFS qualification
were not rerun in this checkpoint. The dedicated RustFS VM is currently stopped.

CG-07 stays IN_PROGRESS and PR #160 stays draft. Next port the original executable
consumers and remaining public lifecycle/ingress/occupancy/feature-off gates,
including failure/cancellation and retained nonvoting inventory. Then finish
exact-reference S3 learner activation/archive holds and refresh same-source
RustFS recovery plus bounded replication/reclamation load. These local TLS/SQLite
fixtures do not establish independent-host capacity, sustained load or release.

## CG-07 managed maintenance checkpoint

The follow-up on 004d1051 implements and locally qualifies catalog-owned
maintenance for repaired cell groups. Capture CG-07-managed-maintenance-16 binds
2130 Rust inputs (manifest SHA256
55e9323085d42f58d0fa77d5eb4b806eb8412e3528c7e129aab14a74b04b789d).
All nine managed Node cases pass in 102.06 seconds; all eight earlier names remain.
The new case runs both initial and joined replacement members in both image forms.

Each cycle starts with an actual resource-backed peer repair and settlement
reconciliation, then moves the real target leader to a survivor. Rooted readiness
covers one physical group containing two independent cells. The survivor leader
originates the owned stop. Actual Node join and signed closure precede offline
confirmation; the surviving quorum accepts writes while that member is offline.
Canonical retained startup preserves the engine and both images while admission
stays paused. Old-boot stop and same-new-boot wrong-child resume requests refuse;
the latter preserves PayloadConflict, the root and paused admission. Fresh owned
resume proof, capacity/disruption settlement and actual leadership on the returned
member precede duplicate-receipt, old-token and new-write verification. A subsequent
maintenance intent proves the disruption slot was released.

Shared fleet metadata now samples actual RawNode leadership across the complete
physical roster, with at most sixteen owners per turn. Roster revision, physical
population and the original deadline bound the scan. Unknown, failed or incomplete
owners supply no headroom. Counts describe groups independently of cell occupancy.
Five private unit cases cover bounded complete scanning and invalid/changed cuts.

Paused retained startup now requires the exact current returned-boot lineage,
Live history, settled voting member, retained local state and pre-existing permanent
claim. Initial-member return uses read-only claim reobservation and existing group
state; it cannot create genesis or a missing claim. Joined startup retains its seed
receipt and claim checks. The directory rechecks the synced quiescence guard after
catalog I/O. Generic new-group and learner startup gates remain in force. Temporary
maintenance refusal is captured under the registration barrier and exposed as
Unavailable without submission by that invocation. Effect scheduling defers that
captured refusal even if resume races with a later flag read; permanent close
retains its distinct cause and prior mutation uncertainty remains sticky.

The regression with a stop submitted by the survivor leader exposed a metadata dependency cycle: the stop RPC,
its matched-progress action back to the survivor leader, and the leader's fresh
inspection require three nested slots in the same two-slot peer bank. Resume has
the same dependency. A new peer maintenance stop/resume first validates exact
rooted authority/payload, then queues the existing bounded Node worker and returns
Unconfirmed. Physical transport completion releases its metadata before the
nested proof finishes. The worker revalidates context, gathers fresh proof and
syncs the exact guard; a later exact retry can observe that effect. Queueing grants
no acceptance, closure, serving or capacity. Local and non-maintenance calls keep
their original behavior. No deadline, quota, stack or promotion check was raised.

Failed captures retain missing leader load, retained-return refusal, unlocalized
timeouts, temporary refusal reported as Closed, and the nested proof overload.
The first queued-proof run also preserves an aggressive uncertain-resubmission
storm; the final case uses the earlier physical join/signed-closure oracle,
retrying submission overload rather than competing with its admitted worker.
The pre-fix source08 join oracle fails and the final source passes with the same
shutdown deadline. Temporary production diagnostics are absent from the final
source. Source15 also passes all nine cases before the final typed-refusal check.

The normal unit build and the five scan, three admission, 28 fleet catalog,
six fleet-action wire and twenty query-wire cases pass. The fleet selection has
thirty names; its two existing Docker Engine cases remain ignored without their
explicit owned harness. Earlier prose counted selected cases as passed; the
preserved logs show 28 passed and two ignored. Broader final-source qualification
is terminal: all 23 catalog and 179 shared execution cases pass, with the two
original owned process fixtures ignored. All 181 execution names remain. Full
library is 1061 passed, one retired inspection-protocol failure and two ignored
Docker Engine cases; all 1059 parent names remain and the five scan tests are
added. Three host cases, the original public SDK case, counter/effects builds and
the retained effects smoke pass. The effects smoke uses the SQLite development
object store. Locked native-only cellar/cellar-python library compilation and all
five static checks pass.

Package builds still fail only on 27 unconverted performance-runner errors.
Strict library Clippy retains 222 diagnostics, 202 dead_code, with no added or
removed code/message/primary-file keys. The plan suite remains 35 passed, one
index/Purpose failure and one reviewed source-anchor error. All seven recorded
inputs of those two failures are byte-identical to 004d1051; no baseline or
assertion was relaxed. Profiles remain opt-level zero with debug assertions and
overflow checks, without internal-test cfg. Source, exact binaries, inventories,
failures and checks are preserved in the capture and its workspace-volume backup.
The earlier full suites below retain their own source boundaries.
This remains in-process TLS/SQLite qualification with a conditional fixture
catalog and explicit fixture deployment attestations. The original public lifecycle
and failure/cancellation gate, consumer/build/lint gates, exact S3 learner activation,
current-source RustFS recovery/load, optimized CI and complete feature matrix remain
required. CG-07 stays IN_PROGRESS, CG-09–16 stay TODO and PR #160 remains draft.

## CG-07 managed peer replacement and drain checkpoint

The follow-up on e8f9352d adds a four-Node managed lifecycle case and exposes
`CellGroupControllerError` beside `CellGroupStartupError` through the public SDK.
Callers can match the nested refusal without a verification import. Provider I/O
during terminal settlement now returns Unconfirmed, because the CAS may have committed before its confirmation read failed. Corruption and
authority errors retain their typed failures. Deadlines, resource ceilings and
original test assertions are unchanged. The cold cluster/workload helper is shared with the existing retained-start case.

Capture CG-07-managed-lifecycle-09 binds 2127 Rust inputs (manifest SHA256
f699ac38509185457b41feeb04d8a51e6cad8ece1bf0021e260b254ce7ca2151).
All eight managed cases pass in 48.58 seconds; the seven original names remain. In both image forms, a capacity-qualified spare
receives a two-cell group through authenticated peer seeding, actual RawNode
catch-up, joint consensus and terminal reservation settlement. A lost terminal
reply reconciles through its permanent receipt. The removed member remains in
physical inventory with its completed permit sequence and both retained cell
images. Settlement grants no disk reclamation.

The actual leader's Handoff shutdown reports one accepted/released group and no
unavailable groups. Its surviving quorum accepts new writes. All three surviving
disks then reopen through canonical initial/joined-member startup, preserving
exact duplicate receipts, old-token reads, independent images and new writes.
Reusing the old-boot learner permit refuses before submission without changing
the fleet root. Cleanup joins entered owners even when replacement fails.

Initial captures preserve Seeding Unconfirmed, the missing SDK error export,
the inventory probe before rooted directory acknowledgement and the actual
post-CAS confirmation loss escaping as Storage. The final case drops that
terminal CAS reply and its confirmation read, observes Unconfirmed, and proves
receipt reconciliation from a new invocation. Temporary diagnosis found `Unconfirmed(NotReady)` at promotion while the learner
caught up; no seed failure or relaxed promotion check was required. Bounded
fresh-cut reconciliation retains the same permit and debit. Diagnostic-only
production edits are absent from the final source.

The normal library build, 28 fleet catalog unit tests, six fleet-action wire
tests, locked native-only cellar/cellar-python library check and all five static
checks pass. Strict library Clippy retains the same 222 diagnostics, 202 dead_code,
with no added or removed code/message/primary-file keys. Package compilation
still fails on the same 27 performance-runner errors. The plan-validator suite
has 35 passed, one failure and one error: the generated index/Purpose mismatch
and reviewed source-anchor drift. Every relevant plan file and the first drifting
source file is byte-identical to e8f9352d; neither baseline nor assertion was
weakened. These checks remain failing gates. The earlier complete execution,
public SDK and full-library results retain their e8f9352d source boundary.

This is in-process loopback qualification with real TLS and SQLite, a conditional
fixture catalog and explicit fixture deployment attestations. It is not real
RustFS, independent-host, load, optimized CI or the complete public lifecycle
gate. Catalog maintenance stop/return/resume, consumer/build/lint gates, managed
S3 learner activation and current-source RustFS remain required. CG-07 stays
IN_PROGRESS, CG-09–16 stay TODO and PR #160 remains draft. Final static/lint
results and publication are retained in this capture's final-checks directory
and the ignored managed_lifecycle_work continuation.

## CG-07 cold managed group qualification checkpoint

The follow-up on b9be4de7 adds an explicit standby directory genesis with its
permanent pin, immutable journal and operation index. It proves zero selected
groups before capacity admission; missing metadata still refuses. First range
selection requires rooted birth reservations and an exact standby-root CAS.
Selected ranges cannot return to standby. Missing tree fields reject. Import
freeze journals its head transition, including a real zero-group inventory.
Fleet birth identity reservation and materialization enter their owning execution
domain; public callers need not provide one.

Initial placement now revalidates every voter's current opaque capacity receipt.
The direct post-restart regression fails before this guard: an Active descriptor
admits a new birth using stale capacity, advancing root revision 19 to 20. The
fixed path refuses without a root change; actual new-boot qualification and
activation then let the identical proposal succeed. Its unselected reservation
remains inventoried. Existing group ownership and serving survive readmission.

Capture CG-07-cold-managed-group-06 binds 2126 Rust inputs (manifest SHA256
4f5a778a4c2bed7db7fd9bc0c30bc4f2ee962341e2fce08a03059fe01381ff01).
All seven actual-Node managed cases pass. In both image forms, three registered standby nodes qualify
and activate capacity, reserve/select one group, reach RF3 quorum and enroll two
independent SQLite cells. Canonical retained startup, exact duplicate results,
old-token reads and new quorum writes pass. Deployment resource bounds are explicit
fixture attestations, not kernel measurements. The original missing-pin failure,
import head/journal disagreement, caller-domain backtrace, fixture compilation
errors and stale-capacity admission remain preserved with their exact sources.

The final broader chain is terminal: all 23 catalog and 179 shared execution cases
pass, with two owned process fixtures intentionally ignored. Library builds; all
twenty wire cases pass. Full library is 1056 passed, the same retired inspection
protocol failure, and two ignored. All 1058 earlier names remain and the strict
missing-tree parser test is added. Three host cases, the original public SDK
counter drill, counter/effects builds and retained effects smoke pass. The effects
smoke uses the SQLite development object store. Locked native-only library
compilation, format, layout, native dependencies, docs and diff checks pass.

Package builds still exit 101 on 27 unconverted performance-binary errors despite
emitting current native executables. Strict Clippy fails with 222 diagnostics,
including 202 dead_code. These are failing merge gates, not package passes.
Profiles remain local opt-level zero with debug assertions/overflow checks and
without internal test cfg, raised timeouts/capacities/stacks or lint suppressions.
No optimized CI, complete feature matrix or current RustFS/load claim follows.

This qualifies local cold initial-group/ordinary-retained startup. Joined-member
startup, repair, drain, maintenance, stop/return/resume and physical disruption
accounting still need the original public lifecycle gate. Exact-reference S3
learner activation, remaining consumers/public gates, current-source RustFS
recovery/load and CG-09–16 remain required; CG-07 and the full goal stay incomplete.

## CG-07 final shared ownership qualification checkpoint

Source-06 preserves the finite shared membership fence and physical send/hint
fix from source-04. It additionally checks authorization node/disk against log
options before namespace/engine opening, and validates the immutable node, disk,
label and server-name descriptor before permanent node pins or boot mutations.
Advertised addresses remain explicit and may differ from listener bind addresses.
The existing registration matcher reuses that descriptor predicate while keeping
its independent pinned-process/transport permission requirement.

Both regressions fail on source-05: a wildcard listener with a loopback advertised
endpoint is rejected, and a mismatched log owner consumes a rooted boot before
startup refuses it. On source-06 all five actual-Node managed cases pass, including
both image forms, fresh/retained ordinary boots, recreated-engine refusal,
foreign-boot stop refusal, declaration and owner/descriptor mismatch before a
rooted boot mutation, explicit advertised endpoints, periodic trust refresh and
shutdown joining an entered catalog read. The membership selection remains nine
passed, zero failed and one intentionally ignored owned child fixture.
Source manifest SHA256:
aa7327d2a3ae4f1244264cb6d59a179e250c7ab24119229efd638a966d080c1c.

Source-04's full shared execution suite passes 179 cases with two owned child
fixtures ignored. Normal library builds; twenty wire cases pass; full library is
1055 passed, one retired-protocol inspection failure and two ignored. All three
host cases, counter build/original public SDK process drill, effects build/original
retained smoke and five static checks pass. Strict Clippy reports 222 diagnostics,
including 202 dead_code, down from 234/214. Three newly grouped messages contain
only residual methods from older groups; no method becomes newly unused. Package
builds still fail on 27 unconverted performance-binary errors. Source-06 changes
only the four recorded owner/endpoint/test files from that broad checkpoint.
Its complete broader chain is now terminal: 179 shared execution cases pass with
two owned child fixtures ignored; all earlier 181 names remain. Normal library
builds, all twenty wire cases pass, and full library is 1055 passed, the same
retired inspection-protocol failure, and two ignored. All three host cases,
counter build/original public SDK process drill, effects build/original retained
smoke, and all five static checks pass. Strict Clippy is unchanged from source-04:
222 diagnostics, including 202 dead_code. Component Cargo commands still exit101
on the same 27 unconverted performance errors despite emitting fresh executables.
The failed source-05 regressions and source-03 socket repeats remain retained.
The locked native-only library check also passes for cellar and cellar-python
with default features disabled and PYO3_PYTHON pointing to a nonexistent runtime.
This is a compile check, not the complete feature-off behavioral matrix.

This qualifies the recorded local ownership boundaries, not the full managed
lifecycle, optimized CI matrix, current RustFS backend or a release.

## CG-07 shared membership and managed ownership implementation

The follow-up on 2e400133 implements the shared owner's configuration send
fence and canonical managed boot/directory/Node stop ownership. Capture
CG-07-managed-fence-04 binds 2125 Rust inputs (manifest SHA256
87c26e4e7757623a41454c57005a3611c0e1539c512fdf7894af39da8664533d). The merged origin/main remains
981a5417d9359d1f992e2dbf214eec7d221b6777; the preceding published checkpoint and
its independent qualification are recorded below.

The original new shared regression fails before the fix: configuration Apply
overtakes an owned prior runtime message. The shared owner now includes one
pending Ready, freezes that finite boundary, joins prior physical sends, emits
an exact persisted-term/index commit announcement and joins its deliveries before
membership publication. Later heartbeat work cannot keep extending the boundary.
The runtime's exact output admission survives a dropped Delivery waiter through
ordinary SendRequest and checkpoint producer/transfer ownership.

The socket regression then reproduced an ownership cycle twice: a completed
failed send retained its output permit while awaiting an unreachable hint; that
hint waited for an idle owner, which was blocked by the configuration fence.
Source-04 ends the send fence after physical exchange and before advisory
feedback. A checkpoint's independent producer retains its own admission through
actual source exit. Transport metadata remains charged until its task exits.
The held-event and entered-socket cases pass in both image forms, inspect both
terminal commit announcements, and elect/apply another group while the first
is fenced. Original joint-membership, sync, follower-quorum and crash cases pass.
The selected membership result is nine passed, zero failed and one intentionally
ignored owned child fixture, executed by the crash cases.

Managed startup validates the actual compiled/declaration/rooted capability set,
pins the physical engine, binds an ordinary or authorized maintenance boot,
syncs its exact lifecycle guard, then publishes endpoints and attaches through
managed permits. Retained route reconstruction uses durable current membership;
joined members use the existing seed-receipt/permanent-claim permit. A declared
standby may precede first range publication without fabricating an empty root.
One separately budgeted directory service owns timed rooted reads and bounded
peer batches, preserves its original deadlines, closes admission on superseded
local authority and joins entered work on shutdown. Internal observers do not
retain the last external SDK owner. Node receives its existing stop services from
the actual bound Core authorization and probe signer rather than None.

All three new actual-Node managed component cases pass: both image forms complete
fresh/retained ordinary boots, refuse a recreated pinned engine before consuming
a boot, and reject a foreign boot through the installed Node stop reporter.
Declaration mismatch refuses before a rooted boot mutation. Independent timed
trust refresh and shutdown joining an entered catalog read also pass. This does
not qualify nonempty managed startup/repair/maintenance, capacity activation,
complete inventory or the original managed_group_lifecycle public gate.

Production cargo check passes. The source-04 broader checks are complete as
recorded above; source-06 repeats them after the additional owner/endpoint
regressions. The newest checkpoint
governs the exact final source and validation boundary. The component Cargo builds
still emit their current executables but fail the package gate on 27 unconverted
performance-binary errors. Full public consumers, exact S3 learner activation,
combined-source RustFS recovery/load and CG-09–16 remain required. Failed initial
schema/endpoint/group-event fixture attempts and the physical ownership regression
are retained, with no timeout/capacity/stack increases or production suppressions.

## CG-07 current-main qualification checkpoint

Counter checkpoint 41ab8b4d is committed with the source-02 qualification
below. While its checks ran, origin/main advanced to
981a5417d9359d1f992e2dbf214eec7d221b6777 (PR #165). Merge 9df1389f includes that
main plus the counter work. The three conflicts preserve the shared-only host
configuration surface, upstream incompressible seed data with CellHistory
callbacks, and every exact metrics assertion. The predecessor host dispatcher is
not restored. Upstream shutdown, snapshot, pool and qualification changes remain.

Merged-source capture CG-07-public-counter-03 binds Rust inputs to merge
9df1389f (source manifest SHA256
32adfacc6ad54f9760928487e2b5ab018c1cbef5c3a691792f42db89f86dc8b0).
The counter builds and the original sdk_many_cells_share_group public case passes
with the actual rebuilt counter (SHA256
a5ebebc8086dcc17892bf706962a2539cb08589b20ebeabef58a581ba95453d8).
All three replicas observe one physical group and two independent cells. The
normal library builds: all twenty query-wire cases pass; full library has
1055 passed, one failed and two ignored. All 1052 earlier test names remain,
with six upstream additions. The sole failure is the retired-protocol inspection
cancellation fixture. All three namespace-host cases pass across both image
forms; the effects build and original two-process retained smoke pass using the
SQLite development object store. Format, layout, native dependencies, docs and
diff checks pass. Normal local profiles use opt-level zero and no internal test
cfg; this does not qualify the optimized CI campaign or feature matrix.

The public/host Cargo commands emit fresh component executables but still exit
101 on 27 unconverted performance-binary errors. They are not package passes.
Strict library Clippy fails with 234 diagnostics, including 214 dead_code.
The one changed diagnostic reflects upstream renaming targets to yield_for_seed
in snapshot transport; the remaining code/message/primary-file set is unchanged.
All owned qualification jobs are terminal. Keep
PR #160 a draft: the complete public target, managed orchestration, combined-source
RustFS recovery/load and CG-09–16 remain unqualified.

The upstream membership send-fence fix changes the retained legacy Replica.
Audit/reproduce its applicability to CellGroupOwner before qualifying managed
membership transitions: that owner has a separate Ready/effect/transport lifetime.
Merging the old Replica fix alone does not establish the shared owner's finite
configuration fence or final commit delivery to a removed peer. Preserve all
original deadlines, ownership reservations and destination/catalog authority.

## CG-07 public counter and provisioning in progress

Counter checkpoint 41ab8b4d on fc4a8d31 ports the counter from two per-cell registrations to
one physical group with independent logical histories, SQLite state and receipts.
The node's log/runtime/controller capacities are explicitly one group. Fresh
public namespace/archive provisioning persists the exact archive intent before
creating counters; retained setup opens only existing metadata. Missing catalog
with retained voter disks refuses rather than reconstructing a namespace.
The public provisioning facade reuses the host's immutable range initializer;
it exposes no legacy catalog or verification-only interface and grants no serving
or activation authority.

The HTTP adapter uses CellVersionToken, one original query deadline/attempt bank,
explicit one/two ingress authorization, full-process/checked-sequence query IDs,
and actual replica metadata. Logical version fields and physical commit witness
fields stay separate. Typed application refusals remain accessible without
verification imports, and ambiguous command errors retain the original ID plus
cause. Node drain precedes the unchanged final quiescence predicate. The original
counter smoke and its assertions/timeouts are unchanged.

The locked counter build passes at CG-07-public-counter-01. Three original smoke
drills pass, each forcing late startup failure and then completing both retained
process phases on all six HTTP endpoints. All replicas report one controller
group and two manifest cells. One immediate repeat cannot bind the fixture's
non-reusing port before process startup; no listener remains. The retained repeat
wrapper checks the original documented free-port precondition, observes 29.10
seconds of socket cleanup, then passes both repeats without altering the smoke.

An additional real native/Axum contract run passes 581 requests, including
seventeen invalid option sets per endpoint, duplicate recognized headers,
authentication, the fixed cell allowlist, forged/cross-cell minima, contract
validation before duplicate lookup, typed conflicts/sequence gaps and unchanged
cell values/receipts. This covers counter ingress; admin and forged tenant/group
acceptance remain unqualified. A separate missing-catalog refusal with three
copied voter disks returns before creating a catalog or archive intent; every
voter file hash is unchanged. This is a startup guard test, not disaster recovery.
Counter executable SHA256:
a6af26253a601a38298c29568fc008e2be92fa3c254e5b139ff4d0ccce5a6ad5.

Source-02 creates native_cell_groups_public with the original
sdk_many_cells_share_group case. The emitted fresh executable passes that exact
case, retaining its process evidence and observed group/cell inventory. The
remaining four original public cases are not stubbed or marked passed. Source-02
changes only this new integration target's Rust inputs relative to source-01;
production/example Rust is identical. Source manifest SHA256:
e93ddd25b82771d7f1c8199567033eaa4cb734de95cec3c160161bd386f13104.
Public test executable SHA256:
9b2987f67f9fca1515311da94c7f3520eef3a2bb44d170bd6e6983b376765825.
The overall Cargo command still exits 101 on twenty-seven unconverted performance
binary errors; a fresh component executable is not a package pass.

The normal library target builds; all twenty wire cases pass. Full library:
1049 passed, one failed, two ignored. The sole failure remains
inspection_revocation_and_process_refresh_cancel_an_admitted_inbound_call, whose
fixture still sends the retired per-cell inspection protocol. All three current
namespace-host cases pass across both image forms. The effects target builds and
its original two-run retained smoke passes with the SQLite development store.
Format, layout, dependency, documentation and diff checks pass. Strict library
Clippy still reports 234 diagnostics, including 214 dead_code; code/message/primary
file comparison finds no added or resolved diagnostic against host-fixed-09.
No suppression, assertion weakening or timeout/capacity/stack increase is used to
obtain a library/package pass.

No current combined-source RustFS, full public target or package qualification is
claimed. The next feature dependency is canonical managed startup: registration
still refuses managed namespaces/node-directory options, Replica attachment uses
only unmanaged bootstrap, and Node startup returns no Fleet stop services. Wire
those owners to the existing exact-boot/group authorizations and synced lifecycle
guard before qualifying managed/performance consumers. Keep every admission guard
until its real replacement is active. Original public fixture ports, remaining
consumers, S3 learner activation and all CG-09–16 scope remain required.

## CG-07 bounded replica routing checkpoint

The shared controller now owns bounded public replica preferences: one to eight
explicit candidates, per-attempt deadlines and an optional untried observed leader
fallback all share one invocation, fingerprint, input, token, global deadline and
attempt bank. Incoming RPCs execute locally. Authenticated leader advice supplies
no authority; every destination retains namespace/process grants and exact
cell/group admission. Remote responses expose the actual authenticated replica.
Wrong-history/schema minima fail preflight before destination retries.

Expired physical SQL retains the original dispatcher and controller admission
until its callback exits. Outbound work retains dispatcher admission through the
physical exchange, and a successful remote result retains its transport charges.
Releasing a waiter cannot refund resources still owned by physical work.

The new routing fixture reproduced a timer race: SQL reports its inner deadline
as Execution(Storage(RuntimeError::DeadlineExceeded)), while the invocation's
outer timer reports a runtime deadline. Fixed-06 normalizes only that exact typed
SQL deadline at dispatch; ordinary storage errors stay fatal. Probe-05's failure
and exact source/executable are retained. No timeout or capacity was increased.
The new fixture then exposed an early cleanup observation; fixed-07 waits for
actual joined transport/controller cleanup before requiring exactly zero. Held
physical callbacks still require their exact item count and charged bytes.

Fixed-07 passes nineteen focused cases and forty routing repetitions, twenty per
image form. Its full suite has 175 passed, two failed and two ignored: original
cancellation oracles expected metadata admission to vanish after waiter abort
while SQL still ran. Fixed-08 preserves their SQL, cancellation and blocked-close
checks, requires the exact pre-abort reservation while SQL remains held, and
requires zero after joined physical shutdown. Production/new routing cases are
unchanged. Fixed-08 passes twenty-one focused cases and the complete execution
suite: **177 passed, zero failed, two ignored**, in 406.47 seconds. Its source
manifest SHA256 is
0f65bed56b6632a843b0e3e3bc5cfac03ad4145dbc9b14d75844a48c7feaf2e6;
native executable SHA256 is
b35cdf1d25e57d34476d2e27a1fd4a0944c781996f7e05014b31d94458025462.
Fixed-07's forty-repeat evidence applies to the unchanged production and cases.

The normal unit target builds; all twenty query-wire cases pass. The full library
has **1049 passed, one failed, two ignored**. The remaining failure is the old
managed inspection fixture using its retired per-cell protocol. Host-fixed-09
adds the owning raft::prelude::Entry import to a shared integration fixture; all
three namespace-host cases pass, including both image forms. The native Cargo
command still exits 101 on unconverted counter/performance APIs despite emitting
the fresh test executable. This is component qualification, not a package pass.
Strict library Clippy retains 234 diagnostics, including 214 dead_code; comparison
of code/message/primary file finds no additional diagnostic against the previous
shared-read checkpoint. No lint suppression is added.

Effects qualification retains baseline/current failures rather than attributing
them to routing. Three baseline/current comparisons capture definite-unsubmitted
NotReady after failover in both executables, plus a current incomplete-shutdown
report after the complete business result. Probe-10 samples unchanged close
semantics: SQL is idle, no groups failed, but one call can remain and the outcome
is Unobserved. Readiness-fixed-11 retries only definite-unsubmitted NotReady or
NotLeader with the same command ID/input and original deadline; five of six
fresh smoke trials pass both retained runs, while one still completes the
business work then refuses shutdown quiescence. This isolates close ordering
from failover readiness.

Drain-fixed-12 lets Node seal admission and drain its producers before inspecting
the final joined Raft report. The exact NoUnresolvedLocalProducers/no-failed-group
predicate remains; unknown mutation outcomes are never accepted as quiescence.
All temporary close diagnostics are removed. Its effects build passes, followed
by all six original smoke trials: twelve successful process runs including a
retained-disk restart per trial, with unchanged assertions and deadlines. Its
executable SHA256 is
1b86a52b35b3d6c38abb808430eeca2f053a6d5b0f22d7e249abf85caccc2297.
Its source manifest is
af29d82f0446507a4f4a089d5806a9e39537af065fd79460acba2a7a77f26f2e.
Relative to fixed-08, Rust changes are confined to the owning test import and the
effects example; production/library Rust is identical. The effect store here is
the SQLite development backend, not RustFS qualification.

PR #160 remains a draft. Public SDK/consumer ports, managed startup, exact-reference
S3 learner activation, all five original public acceptance gates, refreshed
combined-source RustFS/DR/load gates and CG-09–16 remain required. See
[remaining work and priorities](remaining-work.md). Historical captures preserve
all earlier failures and their source/binary boundaries.

## CG-07 shared reads and PR #160 MVP scope

PR #160 publishes the canonical shared SDK/fleet work as a draft checkpoint.
[Remaining work and priorities](remaining-work.md) orders the MVP deliverables
and names the original public merge gates. CG-07 and the end-to-end goal remain
incomplete; all CG-09–16 requirements remain in scope.

The counter port exposed missing public read behavior. Two real follower/SQLite
cases reproduce NotReady before production changes in
CG-07-shared-read-follower-before-01, in both image forms. Core-fixed-01 adds
origin-bound safe ReadIndex forwarding in a separate private context domain.
Selection requires current-term quorum and contiguous local apply through the
correlated barrier. Source generations revoke retained follower proofs on a
term/leader change, failure or shutdown. Seed and metadata contexts retain their
own admission rules; read proofs confer no mutation or seed authority.

Recent-source reads now have an explicit age constraint in the SDK and wire.
Only valid current-term leader traffic refreshes follower source observations.
The captured expiry caps the physical SQL deadline, including entered callbacks;
later traffic cannot renew an old proof. Default SDK stale reads use the injected
host setting; disconnected historical reads require explicit selection. Results
expose the actual selecting member and node. Namespace/process grants, bounded
attempts, original invocation identity and joined physical cleanup remain enforced.

The source-age fixture initially pumped fresh leader traffic while selecting its
first proof. Tagged probes reproduced that race; fixed-03 selects the non-barrier
proof directly, preserving every expiry assertion and deadline. Forty repetitions
pass, twenty per image form. Debug probes are removed. Fixed-04 adds actual TLS
follower queries with minimum-version, linearizable and recent-source constraints,
exact observations/responder metadata and transport-permit release. These are
three engines in one process, not independent-host qualification.

Fixed-04's full execution run has 172 passed, one failed and two ignored. Its only
failure is an older router oracle expecting an authorized follower linearizable
read to refuse. Fixed-05 updates that oracle to check exact successful follower
and leader observations, callback counts and released permits, retaining namespace/
process denial, global retry bounds and the router's absence of local replicas.
All fifteen focused cases pass on fixed-05. The complete native execution suite passes: 173 passed, zero failed and two
ignored, in 408.03 seconds on the normal default stack.
Source manifest SHA256:
64fa92f41bb3e29fb7873ebf0ae5a532ba4c007049c6e1ae47ffcb6039e2f565.

Core-fixed-01's unit and effects targets build. Its full library suite has 1048
passed, one failed and two ignored; the failed inspection-cancellation fixture
still sends the retired per-cell protocol. The original two-run effects smoke
passes using the SQLite development object store. Strict library Clippy fails
with 234 diagnostics, including 214 dead_code. Production and example Rust are
unchanged through fixed-05; only integration fixtures changed after core-fixed-01.
Format, layout, native dependency, documentation and diff checks pass at fixed-04;
publication checks cover the final documentation and router fixture separately.

The overall fixed-05 native Cargo command fails on 34 unconverted performance-
binary API errors despite emitting the fresh native executable used above. Counter
and performance consumer ports, public replica routing, managed directory/startup,
exact-reference S3 learner activation and all five public acceptance cases remain
open. No full-package, strict Clippy, current-source managed RustFS/DR/load, release
or MVP pass is claimed. Historical real RustFS results retain their earlier scope.

## CG-07 retained preparation and completed seed installation

The preceding gateway/backend checkpoint (CG-07-gateway-backend-fixed-03) passes
all 21 targeted cases and has 164 passed, three failed and two ignored in the
full native execution suite. Its normal unit-test target and effects executable
build. The original two-run effects smoke and 39 focused lifecycle cases pass on
that same source. Format, layout, native dependency, documentation and diff checks
pass. This effects smoke uses the local SQLite-backed development object store;
it is not current RustFS or managed disaster-recovery qualification.

The three remaining native failures are reproduced independently in
CG-07-seed-retained-probe-01. Tagged boundary probes show that seed persistence
succeeds before a post-sync source watch rejects completion in both image forms.
The retained-owner retry adds exactly one sync of the same page directory. These
probes have been removed and the original assertions remain unchanged.

CG-07-seed-retained-fixed-01 separates source liveness before submission from
local completion after successful engine persistence. The queued engine still
checks the complete grant and exact managed boot before entering its fence.
After successful persistence, completion retains destination engine/member/disk/
process identity, current boot, shutdown and original deadline checks, followed
by fresh catalog bootstrap. It no longer requires the source process to remain
alive after the bytes and receipt have been durably installed. This grants no
quorum or serving authority. Sync errors remain uncertain and fail closed.

Provisioning retries now reuse an already verified physical closure only with
the same live page and engine owners, exact selected marker, complete checkpoint
retention and no pending/quarantined publication. A changed selection recovers
on the retained applier. Marker equality cannot clear uncertainty. The original
LOCK and no-additional-sync assertions remain. The seed persistence cases also
hold an entered fence through its unchanged original deadline and require a
refused late response while cold recovery preserves the actual synced root.

Fixed-01 builds the normal unit target and effects executable; its original
retained-disk smoke passes both runs. The full ordinary library suite has 1048
passed, one failed and two ignored. Its remaining inspection-cancellation test
still sends the retired per-cell protocol and must be ported to the actual shared
managed protocol. Format, layout, native dependency, docs and diff checks pass.
Strict library Clippy reports the same 235 diagnostics (215 dead_code); none have
a primary span in the six production files changed for these two fixes.

The new expiry cut initially held work past the fixture peer's unchanged
five-second disconnect guard. Fixed-02 gives only that new case a two-second
operation deadline; all other cases retain their original ten-second deadline.
It passes retained-owner reuse and both complete seed-persistence cases, including
all queued/entered revocation, expiry, pre/post-sync error and cold-reopen cuts.
Production/library and example Rust match fixed-01 byte for byte; only the new
expiry fixture deadline changes. The complete native execution suite now passes:
167 passed, zero failed and two ignored, in 452.06 seconds on the normal default
stack. Its three focused regressions also pass. Capture source manifest SHA256:
1a60ff1d210b4f7c3432fabc6f4a669a88539d71271bf6faaed391f6e07c29d6.
The overall native Cargo command still fails on fifteen unconverted counter-binary
API errors even when it emits this fresh native test executable. No full-package,
strict Clippy, managed/RustFS, load or release pass is claimed. All owned jobs are
terminal; the WIP, source inputs, immutable executables and passing/failed evidence
are preserved in the checkout-specific target's qualification-evidence directory.
CG-07 and the end-to-end goal remain incomplete; CG-09 through CG-16 are required.

The next critical MVP work is the public counter/performance and actual shared
managed inspection cutover, followed by canonical managed directory/startup and
exact-reference S3 learner activation. Preserve all original public acceptance
behaviors, then refresh real Docker-fenced RustFS recovery and replication/
reclamation under load on the combined source before publication.

## CG-07 canonical S3 scratch ownership and gateway backend declarations

Canonical S3 enrollment's Image(Io(NotFound)) failure is now diagnosed and fixed.
The checkpoint receiver's anonymous materialized image spool required a parent
that canonical startup never created. An unchanged source-12 executable reproduced
it on fresh data. Creating only the three archive-spools parents made both original
smoke runs pass. A tagged image-reader probe emitted no failed-image path, and is
removed; cellar-ltx generation.rs is restored byte for byte to its prior source.
Canonical archive startup now creates and syncs this node-owned directory on its
execution domain before registering any archive owner. S3 proof, scratch quotas,
original timeouts and all physical receipt/fencing checks remain.

The fix also exposed intermittent typed Execution(Busy) in the finite effects
example. Its caller now backs off only busy/overload under the same command ID,
contract, input and original deadline, including ambiguous outcomes. It does not
claim rollback or create a fresh business command. CG-07-canonical-s3-fixed-02
builds the normal library unit target and effects binary. The original two-run
retained-disk smoke passes, and three additional original two-run trials pass:
eight retained runs total with executable SHA256
4106a1a1c819f1335e62dffa91417646498bbf19821778baa8e18d69d3797995.
Capture source manifest SHA256:
231fa577c83a1d06ccfe1694e6e8a0e45ef5d259988684b9dc45b21f4e59cce1.
This is the public SDK/effects path with a SQLite-backed development object store,
not RustFS, managed fleet, zero-quorum recovery or a load envelope. All 39 prior
focused boundary cases also pass with the scratch fix (fixed-01); the subsequent
fixed-02 change is example retry policy only.

The sixteen native gateway failures were caused by extent fixture setup declaring
a materialized namespace. Registered, custom-retention, effects and reclamation
fixtures now declare the actual backend at birth. Archive reopen forwards the
same immutable backend; a new fixture assertion refuses a contradictory setup.
The read-only RangeDirectory namespace configuration accessor is public for
embedding resources; it neither refreshes routing nor grants startup authority.
Production schema/backend/domain/runtime identity guards remain unchanged.

A standalone original gateway case reproduces on the immutable source-11 binary.
CG-07-gateway-backend-fixed-01 emits a fresh native execution test artifact while
its overall Cargo command still fails on fifteen old counter binary API errors.
All sixteen original failing gateway cases now pass, retaining every original
assertion and both backend variants. Source manifest SHA256:
a0460dd39dc55b4401073c370110c64198ded0030dbfb40ece11ecb07018497e.
The later fixed-03 full shared execution suite has 164 passed, three failed and
two ignored; its 21 targeted cases pass. Retained-owner retry, seed revocation and the
old library inspection cancellation test remain open until new evidence proves
otherwise. No combined/full-package or strict Clippy pass is claimed.

Next fix remaining owner/seed regressions, port public counter/performance callers
and managed shared-protocol cancellation, finish managed node-directory/startup
and exact-reference S3 learner activation, then all five original public gates.
Current-source real Docker-fenced RustFS qualification, persistent hold retirement,
CG-09 through CG-16 and combined load/release gates remain required. The full goal
is still incomplete and this WIP remains unpublished.

## CG-07 predecessor retirement and executable qualification

The unpublished cutover now compiles the production library and its ordinary
unit-test target. The previous 32 integration errors are resolved by removing
the predecessor per-cell host dispatcher, its managed birth/registration API
and inbound per-cell seed/inspection dispatch. Used host options, observation
contracts and storage-policy regression assertions remain. Unreachable
predecessor helpers still require the CG-16 deletion sweep; no warning
suppression or compatibility dispatcher was added.

Canonical startup now joins physical infrastructure on explicit failure even
with retained caller handles. Cancellation schedules the same owned cleanup.
Peer/query routes are installed only after every local root attaches and the
retained inventory is checked. Both existing startup tests use real shared-group
engines, directory births and TLS peers. A default-stack overflow in permanent
claim revalidation was reproduced, traced through the macOS crash stack and
fixed by boxing catalog work before constructing nested timeout futures, with
an additional heap boundary around canonical initialization. Startup boxing
alone was insufficient (test-05); the catalog boundary fixes the original test
(test-07), and both original tests pass without diagnostic probes (test-11).
No stack limit, timeout, resource ceiling or assertion was relaxed.

The existing poisoned-accounting regression requires close to return its
corruption error, retains old handles, and reopens the physical engine before
dropping them. This verifies release while preserving the corruption oracle.
The old storage-family tests do not prove managed startup or serving authority.

The effects executable now builds with explicit namespace capabilities/group
placement, separate logical cell histories, enrollment initializers and public
request/error values. Development provisioning persists one-use archive intents
before committing counters; retained startup only opens them. A fresh harmless
S3 command after observed settlement replaces the advisory archive-position wait.
This executable is not yet qualified: its local SQLite-backed object-store smoke
first refused missing initializers (test-11), then, after that fix, failed S3
enrollment during checkpoint capture with Image(Io(NotFound)) (test-12).
The failed run's voter disks, catalog, creation intents and logs are retained.
Do not reduce durability or recreate archive counters to pass this case.

Current evidence:

- CG-07-legacy-retirement-test-11 retains 4730 source inputs (manifest SHA256
  a48f8733addb9559a02e1b4e61398b7432edfc304393afb5034aeb731326aa70).
  All 39 focused cases pass, including both startup tests, 30 host policy cases,
  four low-level storage genesis, two route-lifetime and one query-auth case.
- The full ordinary library suite has 1048 passed, one failed and two ignored.
  The failure sends the retired per-cell inspection protocol; its cancellation
  oracle must be ported to the real shared managed protocol, not suppressed.
- A fresh native execution artifact was emitted before Cargo failed on 15 old-API
  counter executable errors. The compiler event (fresh:false), source hashes and
  immutable executable are retained. Its full suite has 148 passed, 19 failed and
  two ignored. Sixteen failures stop at gateway Identity validation; the other
  failures are retained-page-owner retry and seed persistence WrongIdentity in
  both image modes. These are open regressions, not passing qualification.
- Test-12 changes only example initializers. Library and native-fixture Rust
  sources match test-11 byte for byte. The normal locked unit build and effects
  executable build pass. It retains 4730 inputs (manifest SHA256
  af3055a1e8692bf0a384f2c31d1baaf6a65a13c1a070c10f6701c47116b12e16).
  Format, layout, native dependency, documentation and diff checks pass.
  Strict library Clippy fails with 235 diagnostics (215 dead_code and 20 other
  lints); the complete classification is in static/clippy-audit.json. No Clippy
  or combined release pass is claimed.

Next resolve canonical S3 enrollment's missing checkpoint image using the actual
smoke repro. Diagnose gateway identity without weakening namespace/backend/domain
checks; existing extent fixtures may advertise a materialized namespace. Preserve
all owner reuse and seed revocation assertions. Port the counter example to many
cells in one explicit group, then the remaining executable consumers. Complete
managed node-directory/startup integration, exact-reference S3 learner transfer
and all five original CG-07 public gates. Refresh real Docker-fenced RustFS
qualification on current source before publication. CG-09 through CG-16 and final
combined/load gates remain required. No new RustFS backend or recovery drill ran
in this checkpoint; historical scoped CG-06 evidence remains separate.

## CG-07 controller-owned managed peer learner activation

The Activate movement receiver now selects its physical history and enters the
same node provisioning manager as initial startup. Its public SDK call uses the
installed execution domain, original bounded deadline, exact namespace owner,
rooted birth and current physical boot. Pre-submission refusals remain distinct
from outcomes after controller submission. Its returned local membership receipt
is descriptive; promotion still requires actual catch-up and fresh quorum.

Managed staging compares the expected permit and target boot with the fresh
catalog admission before creating a mapping. The source exchange still requires
its own fresh-quorum grant. Source endpoints come from rooted trust and current
node views. After complete checkpoint verification and persistence, attachment
uses actual committed membership and physical bindings, without a cell alias.
S3 learner activation explicitly refuses before submission; the exact-reference
archive transfer remains unimplemented and never falls back to a peer.

The manager saves the applier/page owner before installation. Cancelled, expired
or lost replies retain that physical preparation and its capacity. The parallel
permit spans recovery and transfer. A completed install whose response was lost
is resumed using the durable seed receipt and permanent claim, with selected-root
recovery on the same page owner. It is not seeded again or given blank genesis.
Entered work is joined before closing transport/runtime dependencies. Retries
coalesce only for the same directory, route, boot, exact permit and source.

Locked library capture CG-07-managed-learner-advance-01 reports 32 remaining
legacy errors, removing all four errors in the Activate receiver. No new primary
error or non-vendor warning appears in the delivered learner implementation.
This does not establish successful runtime compilation or execution. The capture
contains 4728 source inputs (manifest SHA256
a4291efcce24441e5ebd48cea89ad3dae028a3dec322396b254b4b663492bea9).
No managed lifecycle, native race, backend or load test ran on this source.
Final format, module layout, native dependency, documentation and diff checks
passed. All owned compiler and static-check processes are terminal.

Next remove the remaining legacy host/catalog/seed/inspection consumers while
retaining common physical deadline/process/shutdown services. Finish managed
node-directory/startup and exact-reference S3 learner transfer, then execute all
five original CG-07 public cases and the real RustFS recovery drill on current
source. Persistent archive hold retirement, CG-09–16 and the complete release
matrix remain open. The complete goal remains active and WIP is unpublished.

## CG-07 managed group repair controls and peer-route lifetimes

Begin, Promote and Finish movement consumers now call physical RaftGroupHandle
repair APIs. The controls require the rooted group and exact admitted permit,
check immutable birth/schema, actual current occupants and target boot/trust/
physical-host separation, then use ordinary configuration proposals. Exact
permit/plan/phase and expected revision govern retries; aborted/fenced/later
permits cannot masquerade as progress. Successful mutation requires durable
apply and a fresh post-apply quorum, with original deadline and closure checks.
Actual RawNode readiness and catch-up enforcement is unchanged. Abort is also a
real group control. Learner Activate still uses its old consumer and remains a
required conversion; no S3 fallback or fabricated cell alias is added.

Managed advisory routing can resolve committed joining-member/disk bindings
through rooted node views. It changes no configuration or serving authority.
Discovery lag leaves known voters usable and unresolved peers unavailable;
initial identities are taken from the authorized birth route, not guessed from
node IDs. Ordinary cell apply does not scan routes. Reconciliation runs after
configuration apply/checkpoint install and ordinary frames retry once for an
unknown target, before frame allocation/admission. Existing queue, peer, snapshot
and physical worker limits are unchanged.

A bounded per-group lifetime token separates physical registration from advisory
route generations. Refresh and node-view changes retain the existing checkpoint
factory. Each transfer still captures an exact immutable route and rejects old
generations; per-chunk checking allocates no selected endpoint. Explicit route
replacement/removal revokes the old factory, and a stale refresh or old guard
cannot adopt/remove its successor. Group/cache guards are released before the
registry lock, and peer authorization runs after its validation guard drops.
These are implementation invariants, not executed qualification claims.

Two new registration/generation regressions use parked real synced engines,
filesystem stores and transport/TLS listeners. They cover factory continuity,
old selection rejection, physical-owner cleanup and explicit successor retention.
They do not establish a fresh quorum, an active network checkpoint stream or
RustFS behavior. The existing query authentication test body is byte-identical;
only its shared fixture moved to a sibling file, retaining embedded TLS paths.

Final library capture CG-07-managed-routing-advance-04 has the same 36 inherited
errors as the prior goal turn, including four old activation errors. No primary
error or non-vendor warning is added by the delivered route/repair code. The
intermediate control build's five type/import errors were corrected; its real
stream is retained with the other captures. Final source includes 4726 inputs
(manifest SHA256
bfdbc3c3ff638d7cfd20b8034626e825e33c9fd09e3e9be78beb8713e6f14053),
with every captured Rust input matching at closure. The standard locked unit-test
compile (--lib --no-run) also stopped at the same 36 ordinary-library errors
before test modules compiled. No tests ran. All builds use the required restored
workspace-volume target. No managed/native/backend/load qualification follows,
and the WIP remains unpublished with the full goal incomplete.
Final format, module layout, native dependency, documentation and diff checks
all passed. All owned compile and static-check processes are terminal.

Next finish controller-owned managed peer/S3 learner staging, complete-image/
receipt install and registration using the existing immutable namespace profiles.
Preserve exact-reference S3 semantics and original fencing/retained debt. Then
remove remaining legacy host/catalog/seed/inspection consumers, retain common
physical transport deadline/process services, finish node-directory/managed
startup and all five original CG-07 public cases. Qualify these controls, trusted
route discovery and both regression modules using actual native/race/RustFS
harnesses. Persistent archive hold retirement, CG-09–16 and the full release
matrix remain required.

## CG-07 controller-owned managed genesis and namespace profiles

The restart-genesis receiver now calls activate_managed_initial_group on the
canonical SDK, replacing its retired cell lookup/registration/activation path.
The invocation enters its installed execution domain for catalog I/O, even if
polled on another runtime. It uses the exact registered namespace capability, actual rooted
Live birth, admitted initial voter and last-synced local boot. It reauthorizes
before submission and after attachment, checks the selected root and actual
membership, and returns a descriptive persisted fleet guard. That receipt grants
no fresh quorum, seed, serving or physical-release authority. Existing quorum
readiness and permanence/lost-disk checks remain separate and unchanged.

The controller owns managed initial preparation, applier, selected pages, roster
reservation and transport attachment. Running attempts retain their original
deadline; another waiter cannot extend it. A completed attempt can be retried
with a fresh invocation while reusing its retained physical owner. Entered work
is joined after cancellation/expiry. Real late attachment remains registered,
but delivery cannot report late success. Both queued delivery and consuming an
unread successful reply check its original deadline; managed admission retains
the existing 60-second maximum. An attached retry performs fresh boot/catalog
checks under shared preparation/parallel admission without reopening its page
store, reattaching consensus or adopting another registration's lifecycle.
Current roster/quarantine and shared-engine fatal-error checks remain in force.
Typed startup refusal and unconfirmed outcomes stay distinct; catalog pressure,
conflict, corruption and owning causes are preserved at the receiving boundary.

Provisioning now has immutable CellGroupProvisionNamespace profiles. Each binds
an exact directory owner and declared image backend; duplicate namespaces,
foreign owners and representation mismatches refuse. Canonical startup clones
its single node image inventories into these profiles and configures explicit
CellGroupProvisionLimits. All namespaces share one preparation bank and parallel
limit. Fixed peer metadata and advertised directory-cache capacities are charged
once against the same total byte ceiling, then subtracted from available active
preparation capacity. No resource limit is enlarged. Usage/metrics expose that
configuration retention separately, and existing joined shutdown drops it after
physical work has joined.

Existing provisioning fixtures register their actual namespace owners and image
representation for both backends. Their coalescing, pressure, retained-owner,
joined-cancellation, missing-page restart and healthy-group assertions remain.
Additional checks refuse duplicate profiles, insufficient configuration budget
and an unconfigured foreign owner before its first materialization. These test
modules have not compiled or executed while the runtime library fails. They
provide no managed lifecycle, sync, quorum or backend qualification yet.

The first two locked workspace-volume library builds reduce errors from 40 to
36, removing all four retired genesis receiver errors without new primary
startup/provision/profile diagnostics or non-vendor warnings. The final reviewed
capture is CG-07-managed-genesis-advance-04, also terminal with the same 36
remaining errors. All four real compiler streams and formatter results are
retained. The final 4723 source inputs have manifest SHA256
25b5a30439963dea58e34d16208b4f2572f37d6fa257c1c35dc2c8afca4ae3c6;
all captured Rust inputs match at this validation point. Static review records
11 unchanged native invariant owners, including permanent claims, serialized
boot checks, RawNode catch-up, complete-root attachment and common physical
transport cancellation. This is not executed qualification. The restored
workspace-volume target is used for all four captures. Formatting, module
layout, native dependency isolation, documentation and diff checks pass. All
owned validation processes are terminal. Static assertion review retains all
43 original provisioning assertions (with explicit profile arguments) and seven
additional refusals; the tests are still uncompiled and unexecuted. Latest
origin/main is an ancestor of the working branch. Canonical managed startup
remains gated and unqualified WIP is not published.

Next finish controller-owned managed learner activation, exact-reference S3
restore, real group repair begin/advance and trusted replacement peer routes.
Retire the remaining old host/catalog/seed/inspection consumers while retaining
common physical transport deadline/process services. Installed node-directory
ownership, managed startup sequencing and all five original CG-07 public cases
remain open, followed by current-source native/race/RustFS qualification, explicit
archive hold retirement, CG-09–16 and the complete release matrix.

## CG-07 shared physical page path for provisioning and restart

Canonical startup, controller provisioning and independent archive replay now
use ManifestPageStore::group_pages_path. Serving owners use the same configured
node base and groups/<cluster>/<namespace>/<group>/<incarnation>/pages inventory.
The archive verifier retains its independent base. This removes provisioning's
flat path, which could reopen a different page store after canonical restart.
The ordinary exclusive lock, selected-root validation and failure behavior stay
unchanged; no flat-path reader or migration is added.

The existing provisioning fault and restart fixtures now independently expect
that canonical directory. Static comparison confirms every test body and
assertion outside the path helper is byte-identical, including missing-page
refusal, retained-owner retry and joined cancellation. Both materialized and
extent cases remain. These tests have not executed or compiled while the runtime
library fails; static retention is not physical recovery qualification.

The workspace-volume locked build is terminal with the same 40 error-message/path
pairs as the managed-seed capture, with no primary diagnostic or non-vendor
warning in this change. CG-07-provision-path-advance-01 captures 4722 source inputs
(manifest SHA256
87cdfd5273f790446ac1753f5593bdfe88d3e5ee8f0a6fb2a095298e5496bad3).
All captured Rust inputs match at this validation point. Formatting, module
layout, native dependency isolation, documentation and diff checks pass. All
owned validation processes are terminal. The new layout and managed peer-seed
implementation remain unqualified and unpublished.

Next configure immutable namespace-bound provisioning profiles using shared node
image admissions, wire controller-owned managed learner activation/registration,
and add real group repair/genesis consumers. Remove remaining legacy callers
while retaining shared transport deadline/process services. Managed canonical
startup, all five original CG-07 gates, physical race tests, fresh RustFS, explicit
archive hold retirement, CG-09–16 and the release matrix remain required.

## CG-07 managed complete-group peer seed admission and fencing

The shared transport now has prepare_managed_group_seed, using the same complete
checkpoint stream, receiver pools and retained engine work as unmanaged seeding.
The destination must have a fresh active physical reservation for its exact boot.
The selected source must be a rooted seed owner with the exact current member,
node, disk and process boot, and a voter in the authenticated checkpoint. The
source descriptor must carry this exact consumed permit/repair in Seeding phase.
Subsequent authorization cannot regress its original catalog revision. Catalog and handshake time are deducted before forwarding the remaining original
timeout. Review also put catalog preflight, staging and joined-permit delivery
under the existing transport deadline, process revocation and shutdown wrapper.
Namespace ownership reads count inside directory operation bounds. Queued engine
work retains its grant even after the metadata waiter times out.

Managed staging and installation retain the permanent member/engine claim and
check the exact last-synced local boot in the serialized storage worker. A queued
quiescence, stop or fence refuses further installation. Once accepted, physical
work remains owned through actual sync; expiry/close afterward suppresses late
success without erasing a durable receipt. Managed joined-member bootstrap and
permit revalidation require that receipt, the permanent claim, the rooted Live
group and the current exact boot. An old active permit is not restart authority.
Unmanaged joined/seed entry points also check actual namespace ownership.

The staged base preserves the source's real enrollment origin. Genesis guards
remain genesis guards; an imported group's committed enrollment is installed by
ordinary snapshot successor validation. Normal membership successors, recovery
lineage, checkpoint encoding and RawNode promotion checks are unchanged, as
recorded in static source review. A staged learner remains outside the birth
configuration and cannot acquire startup/serving authority from this projection.
No marker bump, old reader, cell alias, no-op action or resource increase is added.

The previous goal turn removed the legacy query path and reduced the diagnostic
to 40 errors. All three managed-seed captures also report 40. The reviewed final
CG-07-managed-seed-advance-03 includes deadline composition and has the same error-message/path multiset as the query
capture, with no primary error or non-vendor warning in the new seed, storage,
joined-bootstrap or source-authorization code. Its 4722 source inputs are bound
by manifest SHA256
7746dd2c6a4902499291d91f15f3bbcb09bebb1bbc534b16ec5985cf09efa0d5;
all captured Rust inputs matched at that validation point. The first capture used the temporary target.
Workspace-volume writes are now restored; the reviewed build and follow-up checks
return to the normal checkout-specific target required by AGENTS.md.
Formatting, module layout, native dependency isolation, documentation and diff
checks pass for the final capture; all owned validation processes are terminal.

Five new profile/origin/boot-guard regressions remain unexecuted while the library
fails. Their metadata/guard fixtures grant no catalog, sync, quorum or installation
proof. No new runtime, Clippy, managed lifecycle or RustFS qualification is claimed.
This implements the lower managed peer-seed boundary; SDK/controller-owned learner
activation and managed canonical startup are still gated. Next wire this path into
the existing provision/registration owner, add group repair begin/advance and
managed genesis consumers, finish managed S3 learner restore and remove old
seed/inspection/host callers. All five original CG-07 cases, physical race/native/
RustFS qualification, explicit hold retirement, CG-09–16 and the release matrix
remain required. Unqualified WIP is not published and the goal stays incomplete.

## CG-07 query hard cutover and retained authentication

The retired per-cell SDK no longer admits business queries. Its query options,
routing/result types, pending query operation, host execution branch and remote
routing loop are removed. The old query_route module and request/reply body codec
are deleted. QueryRoute accepts only a physical-group plus logical-cell identity;
the Cell history variant and its reader are gone. Current cell queries, commands,
enrollments and receipt lookups keep their separate purpose-bound bodies on the
same authenticated peer query lane. Marker 1 remains unchanged, with no old reader,
translation, production switch or increased resource bound.

The common framing/MAC helpers, exchange, origin-grant validator and deadline/
cancellation/revocation controller are byte-identical to their baseline regions.
This is recorded as static source review, not executed security qualification.
Existing query codec oracles now use logical cell tokens and physical group routes,
preserving channel/exporter, digest, sequence, response correlation, process,
non-voting origin, bounds and typed-failure checks. The original grant test moves
to real synced shared engines and start_shared transport with all 18 assertions
retained: namespace/process isolation, tombstone pressure, in-flight revocation,
regrant refusal and physical pool retention/retirement. Its parked runtimes grant
no serving or quorum proof. A new regression supplies complete superseded route,
request and reply inputs and requires refusal. These tests remain unexecuted.

The previous goal turn implemented shared fleet actions and restart readiness,
reducing the locked library diagnostic from 90 to 65 errors. The first check
reports 40 errors; the reviewed, formatted final capture
CG-07-query-cutover-advance-03 also reports 40, with no primary error or non-vendor
warning in the converted query code. Review restored exact token equality in a
ported wire oracle before the final capture; the tests remain unexecuted. All
three sequential captures and their real streams are retained. Two remaining
host.rs errors concern unchanged predecessor startup service types, rather than
query execution. Its 4719 source inputs are retained (manifest SHA256
86d951269719dd948dcd2e0849eb9efa7c0d0d63fde87edb2fcf501b21db56dc),
and every captured Rust input still matches. Formatting, module layout, native
dependency isolation, documentation and diff checks pass; all owned validation
jobs are terminal. The system-disk target remains
checkout-specific and ignored because the normal workspace-volume probe still
returns EPERM. No other checkout artifacts were reused or cleaned.

This is hard-cutover implementation progress, not runtime qualification. No new
runtime tests, Clippy or RustFS drill ran while the canonical library fails.
Earlier core/native/backend records retain their original source scope. Remaining
legacy SDK/Axum/read examples and low-level SQL fixtures must be converted without
weakening their behavioral oracles. Complete-group managed repair/learner seeding,
registration/genesis, old seed/inspection callers, installed directory ownership
and managed startup are the critical next work. All five original CG-07 public
gates, explicit persistent hold retirement, refreshed native/backend qualification,
CG-09 through CG-16 and the complete release matrix remain open.

## CG-07 shared fleet actions and restart readiness

Fleet movement/restart action dispatch now uses the installed shared SDK gateway.
Local execution checks the exact synced boot and installed runtime owner; remote
execution retains the existing authenticated action lane, rooted receiver checks
and exact target. Invocation captures its original deadline and control admission
before polling, with the existing 128 KiB reservation. Internal observers do not
retain the external SDK lifetime. Late delivery or an interrupted entered action
remains unconfirmed; it does not establish rollback. Quiescing hosts can perform
their restart handoff, while fenced/stopped boots refuse action admission.

Restart handoff and resume callers now address complete GroupHistory values. The
actual group owner checks term, membership revision, stable RF3 configuration,
committed floor and each required survivor's matched/active progress before a
transfer. The runtime rechecks the exact synced quiescence selector after queueing
and tightens its evidence expiry to the original call deadline. The shared and
remaining legacy owners use one extracted predicate, preserving the existing
checks and actual caught-up leadership transfer. Replication metadata alone
grants no quorum, health or seed authority. Restart progress reports carry the
physical history. The obsolete per-cell fleet action dispatch modules are removed.

The first locked library check reports 70 errors, down from the prior 90. The
reviewed final source fixes five remaining restart-report type mismatches and
reports 65 errors. There is no primary diagnostic in the new action, shared-owner,
restart-gate or converted handoff/resume paths. All 4719 captured source inputs
are retained in CG-07-group-actions-advance-02 (manifest SHA256
248e011a2bdaa4ec19a2f93ee2b80cae44f14da43d06b70517bf76684edd6f40),
and every captured Rust input still matches. This is compiler progress, not runtime
qualification. Four new readiness/identity/quiescence regressions are unexecuted
while the library fails. No new runtime tests, Clippy or RustFS qualification ran.
The previous 96 enabled pure-core test results remain scoped to their capture.
Formatting, module layout, native dependency isolation, documentation and diff
checks pass. All owned build/formatter/check jobs are terminal.

After the user cleaned the workspace volume, a new lock open and independent
checkout-specific probes still returned EPERM there. The failed formatter attempt
is retained. The existing repository formatter completed directly; the final
locked rebuild used a fresh ignored checkout-specific target on the system disk.
No other checkout's artifacts were reused or cleaned. The capture's environment
record documents this temporary deviation from the normal workspace-volume rule.

The next critical cutover removes the old query routing/body codec while preserving
the authenticated framing, node/process and grant/revocation checks shared by
cell queries, commands, enrollments and receipts. Complete-group learner seeding
already has authenticated grants and joined-member bootstrap; managed repair,
registration/genesis and activation must use those real owners. Installed directory
ownership, managed startup and all five original CG-07 public cases remain open.
Persistent hold retirement, refreshed native/backend gates and CG-09 through CG-16
are still required. No unqualified runtime changes are published.

## CG-07 physical-group resource, repair and topology decisions

The pure fleet reservation ledger, topology view/targets/moves and isolated-repair
view/results now identify complete GroupHistory values. Cell names no longer enter
these models. A physical replica still costs one engine slot; its combined image,
staging and memory footprint stays charged under the unchanged resource rules.
Resource reservations retain exact namespace, group and incarnation identity.
Old-incarnation reservations cannot be reused or silently erased by a new history.
Topology rejects two current incarnations of one namespace/group ID, while equal
opaque group IDs in different namespaces remain distinct. The isolated repair
result is GroupRepairPlacement, carrying the same revision/member fences for every
cell. Its deterministic tie-break uses physical history, with no cell alias.

The Docker failure-recovery adapter now supplies complete RangeDirectory handles.
Before sampling isolated-repair resources it checks current rooted group
authorization. This neither infers failure from absence nor grants membership,
release, recovery or deletion authority. Existing physical debt, exact boot,
capacity/domain checks, work bounds and RF3 assertions remain unchanged.

The previous goal turn made implementation progress on complete-group replay and
persistent acquire journals, reducing the captured library diagnostic to 100
errors with static checks passing. This turn changes the authoritative group
models. Its first test compilation caught a missing BTreeSet import in a new
regression; that failed attempt is retained. The corrected capture passes all 96
enabled cellar-logic tests and strict all-target Clippy. Three new executed
regressions cover namespace-qualified group IDs, conflicting current incarnations
and retained old-incarnation debt. No existing planner oracle was weakened.

The eight executed pure-core binaries are retained and hashed on the workspace
volume. Capture .cellar/cell-groups-evidence/CG-07-group-model-advance-02/ binds
4718 source inputs (manifest SHA256
e974a366a32ad5982c07d1de70ae79f250934d69c8e9b98cd9319c14a1691b62).
The locked runtime check now reports 90 errors, down from 100, with no primary
error or warning in the converted fleet models or Docker adapter. All captured
Rust inputs still match. Pure-core results do not qualify public SDK/fleet startup,
Docker isolation, RustFS or managed lifecycle execution. The library and all five
original CG-07 public gates still need to pass. Formatting, layout, dependency,
documentation and diff checks pass; all owned validation jobs are terminal.

Investigation confirms that the old per-cell query transport shares framing with
logical-cell queries, commands, enrollments and receipts. Keep that authenticated
shared infrastructure while removing legacy query routing/codecs/host consumers.
Then finish group-native learner/restart/leadership control RPCs, installed node
directory ownership and managed startup. Explicit persistent hold retirement after
fencing/joining, old fixture cutover and refreshed native/backend gates remain
required. CG-09 through CG-16 and the complete feature-off/load/crash/release matrix
remain open. No unqualified runtime changes are published.

## CG-07 managed complete-group replay and durable acquire ownership

Automatic failed-node removal and management lifecycle verification now use one
shared FleetGroupArchiveVerifier contract. It opens the exact namespace-owned
physical group archive, selects survivor candidates from rooted fleet membership,
freezes the complete quorum-proven cut, acquires independent persistent retention
and restores the full checkpoint plus contiguous tail. The former per-cell birth,
cell archive and single-image verifier factories are removed from both callers.
Public configuration now supplies explicit private materialized/extent image
inventories, page bounds and node-shared archive/checkpoint/tail/scratch pools.

An acquire journal is keyed by the exact verification basis and pending group,
independently of later business writes. Before the registry sees a mutation, the
fleet catalog conditionally stores the complete intent and observes its winner.
Concurrent controllers send only that persisted winner. Restart restores its
original fixed cut even before a live receipt was appended. Unknown responses
retain the same allocation; a fresh registry read must prove its revision was
consumed without its hold before a conditional journal successor is prepared.
An independently confirmed hold still reconciles after unrelated allocations.
Each method retains its original deadline and rechecks rooted basis before and
after work. No publisher-owned hold, new allocation on timeout, one-cell alias,
format bump or compatibility reader is used.

Receiver admission now precedes the blocking page-inventory scan. The existing
checkpoint byte/item bank reserves receiver workspace plus bounded page inventory
metadata. Its reservation follows the entered worker, receiver and recovered
closure; cancellation cannot release it while physical work still owns it.
Pressure and limit failures remain typed. No configured quotas are increased.
Persistent holds are deliberately not released on drop or timeout. Explicit
terminal retirement still needs a fence for every user and joined physical work;
the existing per-registry hold ceiling is unchanged.

The previous goal turn completed the captured 112-error integration diagnostic,
static checks and preservation of the prior complete-root work. This turn changes
the authoritative implementation. Formatting passes. The locked library check
now reports 100 errors, down from 112; no primary error or warning is reported in
the new verifier, acquire journal, admission code or converted lifecycle callers.
This is a failing compiler diagnostic, not qualification of those modules.
The capture under .cellar/cell-groups-evidence/CG-07-managed-archive-advance-01/
binds 4718 source inputs (manifest SHA256
87c1ddfc914865321c39cd67a7aff912c55044180340fb7fd7811bb85e1d4d3b).
All captured Rust inputs still match. A dependency-check attempt lacked Cargo on
PATH; its failing stderr is retained separately. With the required toolchain
environment, format/layout, native dependency isolation, documentation and diff
checks all pass. All owned formatter, compiler and static jobs are terminal.
No tests or Clippy ran because the library still fails. Origin was fetched and remains
81cbbc2f72f302751dd471cf06cf51394146ac63, with the branch zero commits behind.
Five new regressions cover lost-response allocation reconciliation, consumed
revision and foreign-purpose refusal, and receiver inventory pressure/lifetimes.
They remain unexecuted while the canonical library fails. No native, managed,
RustFS, load or release qualification is claimed for this work.

Next finish the remaining group-native topology/repair consumers, authenticated
learner/restart/leadership RPCs and installed node-directory owner, and convert
legacy query/seed/host consumers without restoring the one-cell runtime. Managed
startup and all five original CG-07 public cases remain required, including the
hard cutover of old verifier fixtures. Then qualify explicit hold retirement and
refresh complete native/backend gates for the changed marker-1 root shape.
CG-09 through CG-16 and the combined feature-off/load/crash/release matrix remain
open; the original end-to-end goal is not complete.

## CG-07 complete-group fleet publication and archive receipts

Fleet membership RPCs now use the shared SDK gateway and actual physical group
owner. Both local and remote paths reauthorize the exact rooted fleet history,
retain the original deadline and use fresh safe ReadIndex plus durable apply.
The body carries the complete GroupManifest, full membership/configuration and
same-owner progress. No cell, SQL image alias or per-cell query is selected.
Published cuts contain a complete root and exact membership commitment; the
zero-cell root is explicit and missing publication cannot construct a cut.
Configuration set order is canonicalized without changing joint roles. Delivery
age extends once through the original SDK invocation. Metadata descriptors move
their membership ledger into the returned proof rather than cloning it across
response retention; complete RPC/capture work reserves the existing 1 MiB
physical metadata envelope from the unchanged configured control bank.

Manifest page references now authenticate the maximum physical cell mutation
position throughout each subtree. Updates recompute it along existing AVL paths;
metadata reads obtain it directly from the selected root without scanning cells.
Full tree verification recomputes the summary from actual pages and leaves.
Complete-root prefix comparison distinguishes metadata-only progress from later
cell changes, rejects a conflicting root at the same position, and rejects a
changed root whose latest mutation predates the fixed cut. It also checks exact
route, configuration commitment and recovery lineage. Physical group positions
remain distinct from logical cell versions and original receipt witnesses.

Fleet archive coverage now independently restores every checkpoint dependency
and replays the contiguous group tail under an exact persistent archive hold.
It compares the complete root and configuration commitment. A locator or uploaded
single image cannot supply this proof. The caller must own the hold and replay
receiver, preserve its original deadline, and retain entered physical work.
The fleet verifier suppresses late delivery and keeps pressure, timeout and
conflict failures distinct. This proof supplies no current health, membership,
write acknowledgement or physical release authority.

Receipt index rows retain their original 4 KiB bound. Complete live evidence is
stored in an immutable, content-hashed object under the existing catalog page
bound; one append batch retains the original aggregate encoded-byte ceiling.
Readers require the exact object/hash and revalidate the full root/hold shape.
Orphan objects or index pages grant no receipt. Marker-1 readers refuse the old
inline/single-image shape; no fallback reader, format bump or resource quota
increase is added. The superseded per-cell fleet observation requests, read
variant and host RPC implementation are removed. Other legacy adapters remain
in the unfinished cutover.

The previous goal turn made progress on physical node inventory and follower
quorum metadata, with a captured 124-error library diagnostic. This turn changes
the authoritative implementation. The first locked check reports 120 errors;
the second and final checks each report 112, with no primary diagnostic in the
new group metadata, complete-root coverage, manifest-summary or receipt modules.
The final check includes configuration-order review and captures 4714 source
inputs (manifest SHA256
fb11c46a225c6859ef9ac482740721b2efbea260fa4dbf762265e4d907ec008b).
Every captured Rust input still matches the current source. This is a failing
compiler diagnostic, not qualification of those modules. Captures and actual
streams are under
.cellar/cell-groups-evidence/CG-07-fleet-root-advance-01/, -02/ and -03/.
New codec, complete-root prefix, authenticated subtree mutation and bounded
receipt-shape regressions are unexecuted while the canonical library fails.
No native, managed, RustFS, load or release qualification is claimed. Existing
CG-06/08 backend results remain scoped to their original artifacts; the new
marker-1 manifest shape requires fresh qualification before publication.
Formatting, module layout, native dependency isolation, documentation and diff
checks all pass. No new tests or Clippy ran because the library still fails.
The final compiler owner and watcher, all formatter jobs and the static runner
are terminal; no owned build or backend job remains live.

Critical next work is the managed verifier's owned archive-hold intent journal
and complete replay-receiver integration in both automatic removal and management
control. Do not borrow a publisher's disposable hold or retry an uncertain acquire
with a new allocation. Then finish group-native placement/repair models,
learner/restart/leadership RPCs, installed node-directory ownership and managed
startup. All five original CG-07 acceptance cases, CG-09 through CG-16 and the
combined feature-off/load/crash/release matrix remain required.

## CG-07 physical SDK inventory and follower quorum metadata

The SDK now obtains local and authenticated remote node observations from the
shared controller and actual cell-group engine catalog. A complete engine scan
retains unattached/quarantined histories and checks that the roster and durable
slot count did not change during capture. Pending attachment/provisioning debt is
reported independently of durable slots; a zero engine count cannot erase it.
Startup bounds come from the actual controller/runtime/log capacities. Current
fleet enrollment is the exact trust cut installed in the transport, not a separate
catalog refresh. The SDK keeps original invocation deadlines and response
reservations; internal metadata observers do not retain external node lifetime.

The transport has a weak gateway to the real shared SDK and validates its runtime
and execution-domain owner. Node probes no longer upgrade an old cell gateway or
invent a cell identity. The marker-1 node body requires the new
pending_group_registrations field and refuses the old single-history spelling.
Its new regression preserves concurrent debt independently of empty disk slots,
refuses the old body and checks scalar overflow. Local profile observation can
now be configured; managed startup, node-directory publication and mutation
controllers remain gated until their whole-group protocols are complete.

Review found that the prior shared metadata reader required a local leader,
which cannot supply the three voter observations required by fleet recovery.
Metadata-only safe ReadIndex now forwards from followers. Its distinct context
binds the origin member and fresh invocation nonce; incoming requests must match
that authenticated member and responses must come from the current leader.
Execution/seed contexts remain local. Selection waits for the actual local
durable apply/root/configuration, records same-owner progress and rejects a term
change. The original byte/item/deadline banks and leader-only execution readiness
are unchanged. A new three-engine TCP regression covers both image forms, two
cells, joint configuration on every voter, forged nonleader replies, released
read banks and continued refusal of follower execution/transfer authority.

The prior goal turn only reverified CG-06 status and is classified as no progress
for the full cutover. This turn changes the authoritative implementation. The
first locked diagnostic reports 124 remaining errors, down from 157. The
second locked diagnostic also reports 124 after the follower metadata changes,
with no primary diagnostic in the new inventory/read-owner/gateway modules. All
captured Rust bytes remain unchanged. Captures and real
streams are under .cellar/cell-groups-evidence/CG-07-node-inventory-advance-01/ and
-02/. Format check, layout (2026 Rust files), native dependency isolation (496 reachable
packages), documentation links and whitespace checks all pass. No new regression
has executed while the canonical library is failing. No managed runtime, RustFS,
load, or release qualification is claimed here.

Critical next work is the authenticated whole-group fleet observation/publication
body and complete-root archive/removal proofs. Then finish repair/learner and
restart/leadership RPCs, installed node-directory ownership and managed startup,
and run all five original CG-07 acceptance cases. CG-09 through CG-16 and the
combined feature-off/release matrix remain required. Existing CG-06/08 backend
evidence stays scoped to its previously qualified source artifacts.

## CG-07 physical SDK controls and joined service ownership

Local SDK group handles now expose guarded fleet enrollment, exact permit
fencing and fresh physical membership observation. Configuration admission is
not completion: the call waits for its durable apply floor, then obtains a fresh
safe ReadIndex under the original invocation deadline. Exact already committed
guards/fences reconcile through a new observation. Refused means this invocation
submitted no configuration; an error after a possible send remains Unconfirmed.
It never authorizes automatic retry, reservation release or node removal.

The bounded read owner can defer its quorum barrier until the requested minimum
is durably applied, without choosing an application cell. If a later configuration
overtakes a pending barrier, the same invocation re-arms within its original
reservation and deadline. A held minimum consumes the existing fixed node/group
read banks and does not busy-poll or enlarge a quota. The new native regression
holds actual engine sync, checks non-completion and retained admission, then
requires the real current configuration and quorum barrier in both image forms.
It also checks an unreachable floor's deadline and release of its reservations.

The new SDK has a fixed two-slot control-service registry sharing the configured
control byte/item bank. Movement/restart workers hold their registration through
actual destructor completion; independent done signals occur after retained
worker fields are dropped. Cancelling a close waiter keeps admission closed and
does not erase the live worker. Node closure signals and joins these services
before controller/transport/storage shutdown. Internal observers retain metadata
without retaining the final external SDK lifetime, so a service cannot self-own
an otherwise abandoned node. Actual control admission is included in SDK resource
observations. Service startup refreshes the installed host and checks its exact
node/disk/engine/process boot and synced lifecycle guard; managed canonical
startup remains gated while the remaining consumers are unconverted.

The previous goal turn made progress: physical namespace import and inventory
interfaces advanced, with an unchanged-source library diagnostic of 174 errors
and full preservation of 136 changed/new files and all eight user diagrams.
Current source is local and unqualified. The first sequential locked library
check reports 165 remaining errors; the final captured check reports 157, with no direct diagnostic in the new SDK,
read-owner or service-worker modules. Actual transport is now retained by the
new SDK, which submits persisted exact-boot lifecycle requests on the existing
authenticated stop/fence lane. It captures deadline and control admission before
returning the waiter; the receiver independently owns admitted guard mutations.
No local business group is required. A lost/expired reply remains Unconfirmed.
Formatting passes over 2025 Rust files; native dependency isolation passes with
496 reachable packages. Source captures and actual streams are retained under
.cellar/cell-groups-evidence/CG-07-physical-controls-advance-01/ and -02/. The new native and
service-lifetime regressions remain unrun until the canonical library compiles.
No RustFS, sustained-load, managed runtime or release qualification is claimed.

Reflection: connect the physical SDK/control RPC and inspection owners, convert
whole-group archive/restart/removal verification and occupancy, and finish managed
canonical startup. Then run all five original CG-07 acceptance cases and the full
affected feature matrix. CG-07 remains IN_PROGRESS; CG-09 through CG-16 and final
combined qualification remain required.

## CG-07 physical-group import and lifecycle inventory cutover

The new import path accepts RangeDirectory and an opaque frozen physical-group
inventory. Shared birth reservation now enters the namespace's CAS admission
index before topology publication. Admission freeze arbitrates against disaster
recovery, then a second CAS locks the directory. The exact locked head is retained
by digest. Delayed admissions and stale topology writers cannot enter that cut;
unknown responses retain the same operation and permanent identity claims.

Inventory settles every physical birth, including unselected pending groups.
Cancellation competes with materialization at one immutable decision key and
retains the group identity and fleet/node references. Selected lazy births stay
live; absent voter disks do not authorize imported genesis. Coverage consumes a
fresh cell-independent quorum membership capability and verifies the exact birth,
guard, stable RF3 roles, node storage pins and permanent group member/engine
claims. Handoff independently walks every frozen selected range and requires
its exact admission and coverage witness, in addition to the complete intent set.
The adopted owner is checked by actual managed birth/bootstrap effects. Topology
unlock advances the head again, so a predecessor expected head cannot reappear.

Restart birth settlement now uses physical group decisions and exact opaque
fleet reservations. Maintenance, capacity, evacuation, resource and platform
interfaces retain RangeDirectory handles. Standalone management connection takes
explicit group directory limits; movement/debt projections no longer contain a
cell field. The marker-1 protocol adds group-import-1 with a hard cutover.

All of this remains local and unqualified. The first sequential locked library
check reports 194 remaining integration errors, including one new joined-method
name mismatch corrected after the check ended. Its two unused predecessor
re-exports were also removed. The new native regression holds an actual admission
CAS while freeze wins, drops a freeze reply, and asserts exact inventory count,
stale topology refusal and retained reopen without object listing. It has not
executed: the canonical library still fails compilation. The follow-up check and
its exact source capture are under CG-07-group-import-advance-02. That check
ends with 177 remaining errors and no own-crate warning. The new admission,
freeze, import and inventory modules emit no direct diagnostics. Resource,
movement, topology, repair and service models were then converted to GroupHistory;
the final capture is CG-07-group-import-advance-03. None of these diagnostics
qualifies the canonical runtime or replaces the earlier directory pager checkpoint.

Review found that several admitted candidates can share a logical range and
epoch. The fleet range-debt key now includes physical group/incarnation identity;
the directory retains exclusive selected-range ownership. A new invariant test
covers distinct pending keys at the same range/epoch. The third sequential check
reported 174 errors with no own warning before that fix; the final reviewed
source capture is CG-07-group-import-advance-04. The final locked library check reports 174 remaining integration errors;
all captured Rust bytes remain unchanged. The new admission, freeze, import and
inventory modules emit no direct diagnostic, which is not qualification. Both
new regressions remain unexecuted until the canonical library builds.

Reflection: finish the physical SDK/control RPC owners and complete-group archive
verification used by movement, restart and removal. Then enable managed canonical
startup and run all five original CG-07 public acceptance cases. Existing per-cell
host/query/control consumers still need removal or conversion. CG-07 remains
IN_PROGRESS, and CG-09 through CG-16 and the combined release matrix remain open.

## CG-07 group-native managed authority work

Unqualified cutover work now removes the application cell from FleetGroupBinding.
Fleet histories carry the exact physical GroupHistory, range, route epoch and
original creation identity. Range reservations and permanent group IDs retain
the existing rooted CAS journal, node references and unknown-outcome behavior.
Namespace/class declarations are checked explicitly, without deriving a class
from a synthetic group cell. The marker stays 1; the protocol identity adds
cell-group-fleet-1 and earlier cell-bearing bindings refuse decoding.

Managed group genesis now writes its configuration-owned fleet guard in the
first engine sync. Existing bare histories refuse retrofitting. Imported histories
can only resume an existing matching guard. Catalog bootstrap checks the exact
host/node/disk/engine/process boot, and the engine worker rechecks the synced
lifecycle guard before creating or resuming a managed group. Quiescence, accepted
stop and fencing block this work; a matching durable resume can reopen admission.
Membership envelopes retain full route binding and guarded permit sequencing
through enrollment, fence, learner repair and joint-consensus steps. Checkpoint
capture/decoding checks the guard's physical history.

The shared runtime has a new cell-independent quorum membership observation.
It sends a fresh safe ReadIndex, waits for contiguous durable apply, and selects
one coherent configuration behind that barrier. Node/group metadata reservations
are acquired before cloning membership and remain charged through response
retention. The SDK captures the original observation deadline before polling.
This metadata grants no eligibility, archive, deletion or release lease.

Fleet inventory now traverses physical range pages and checks both directions:
each selected route needs a rooted history, and every retained reservation must
keep its exact namespace/birth/node references, including pending births outside
the selected tree. Import and cancellation require complete group-native terminal
proofs; a predecessor cell inventory or a caller-supplied Cancelled value refuses.
The additive range pager passes its separate primary checkpoint at 66d5ea6a.
It bounds each response, pins its continuation to an immutable directory cut,
and uses authenticated tree paths without listing object-store keys.

The first managed-authority library diagnostic ended with 202 errors in unconverted
consumers. It overlapped an owned formatter; pre/post-format snapshots and the
complete output are retained as diagnostic only. No new core authority module
emitted a diagnostic in that run, which is not qualification. The successive
sequential library checks report 205, then 198, then 198 remaining integration
errors. The last run includes typed fleet-authority failures, a definite refusal
for missing imported genesis without quarantining an otherwise healthy engine,
and public execution-domain clocks in the new external native assertions.
All new core assertions remain unexecuted. CG-07 remains IN_PROGRESS and
managed canonical startup remains closed until its real consumers are converted.

Scope addition: native_cell_groups_catalog.rs and its existing directory fixture
cover the physical inventory pager; no original CG-07 case or oracle is replaced.
The first pager build caught a missing catalog re-export before any test executed;
that failure is retained. The corrected checkpoint passes all seven commands:
the exact new case, nineteen catalog cases, strict all-target Clippy, formatting,
layout, native dependencies and docs. No case is ignored. Native record SHA256:
8061d03d7902d21f291fb82b16bcf04c91d20716465ef5292eecd7c754dcd91a;
executable SHA256: 43f6e2f3654ca92b623b3960c04bbd443a17128a1391728f35c725bdb20b8d48.
The six-file source snapshot, exact executable and actual streams are retained
and independently backed up. Both checkouts share runtime base 66d5ea6a with all
92 preserved WIP files and eight diagrams unchanged across that fast-forward.
The reviewed source-baseline document passes all four post-gate checks and is
published with this component on PR 160 through c54307c5. Both checkouts now
share that head; managed cutover changes remain local and unqualified.
Records live under .cellar/cell-groups-evidence/CG-07-managed-group-advance-01/
through -04/ in the spare checkout, and CG-07-directory-inventory-01/ and
CG-07-directory-inventory-fixed-02/ in the primary checkout. These are metadata
fixture results, with no new RustFS, managed runtime, load or release qualification.

Reflection: finish group-native import/cancellation and physical inspection/RPC
consumers, connect the actual movement/restart owners and managed canonical
startup, then qualify the original five public SDK/ingress/lifecycle cases.
The full cutover, CG-09–16 and final combined release/load/crash matrix remain open.

## CG-07 whole-group shutdown, drain and resource ownership

The spare cutover now connects Node shutdown to the logical SDK's physical close
owner. The bounded close report contains whole GroupHistory replicas, complete
aggregate group/archive counts and separate unavailable coverage. Retained calls
are not interpreted as one unfinished command per group. Producer quiescence is
observed only behind sealed public admission, an idle application-work registry,
healthy group/provisioning state and idle physical SQL. Unobserved outcomes never
become a clean stop because a counter happens to be zero. Controller, inventory
and engine close failures remain distinct, and no report grants disk deletion.

Shared owners expose real replication progress and recheck term, membership
revision, stable incoming voters, recent activity and catch-up on leadership
transfer. Node drain gives each physical group one bounded turn under the original
absolute shutdown deadline. Offered attempts, owner acceptance and observed
successor leadership remain separate observations, including timeout races.
No cell receives a worker or timer. Stale-term/revision assertions were added to
the real membership fixture; they are not yet executed in the uncompiled cutover.

Resource observations now describe shared namespace/controller/runtime/SQL
admissions and actual capacities. Serving and independent archive-verifier image
stores have separate passive accounting; cached manifest counts are metadata,
not serving authority. The SDK implementation is split by responsibility under
application/consensus/group_client/. The controller's existing join owner releases
namespace image resources even when a close waiter is cancelled. Unpolled
enrollment and receipt futures defer cloning those physical resources.

A retained-closed-metadata regression exposed cached manifest pins surviving
physical engine closure. Both image forms fail at 5e7aadb6 with directory-lock
refusal. The minimal cache/storage fix releases only derived pins after the
closed fence and physical engine join, before terminal completion; independent
readers retain their own pins. Both regressions pass with the fix. Its twelve
checks also pass full execution (165, two fixture ignores), archive data (29,
five RustFS ignores), effects (ten, one RustFS ignore), namespace host (three)
and controller-archive suites, plus all-target strict Clippy and repository checks.
The first controller target invocation used an incorrect name and was rejected
before execution; the corrected target passes. Actual streams and five executable
copies are retained under
.cellar/cell-groups-evidence/CG-07-closed-manifest-pins-fixed-01/ in the primary
checkout and its workspace-volume backup. This fix is committed at 51d136fb
and published on PR 160 through 110b7a4e, including the reviewed baseline.
Both checkouts share that base; every preserved WIP file and all eight user
diagrams match their pre-sync hashes. It does not qualify the new Node/SDK/fleet
pipeline or another RustFS drill.

The cutover's latest locked library check still fails with 114 unconverted fleet,
query-wire and predecessor-host integration errors. Its new Node shutdown,
resource and transfer modules emit no diagnostics in that run, which is not an
executable qualification. The earlier missing import and recursive cleanup-type
graph were corrected without increasing recursion, stack, quotas or deadlines.
Layout, native dependency and documentation checks pass after the lifecycle
changes. The new lifecycle/stop-proof assertions remain unrun. CG-07 stays IN_PROGRESS;
finish whole-group fleet authority and public routing/ingress, convert their real
consumers, then run all five original acceptance cases. CG-09–16 and the final
combined release matrix remain required.

## CG-07 canonical startup and logical SDK cutover work

CG-07 remains IN_PROGRESS. The unpublished spare checkout now replaces the
canonical startup's per-cell birth loop with namespace capabilities, an explicit
initial range plan and whole-group assignments. Assignments resolve permanent
group reservations and recheck directory selection before bootstrap. Startup
recovers complete selected roots through the existing appliers before attaching
shared owners, and refuses retained groups missing from its placement inventory.
There is no legacy/shared startup flag. The published PR checkpoint is
110b7a4e; this incomplete cutover has not been pushed.

The new SDK uses logical CellHistory, CellVersion and CommitWitness values and
separates administrative group handles. It owns joined closure independently of
response cancellation and explicitly reports unobserved mutation outcomes rather
than manufacturing clean shutdown or deletion proof. Materialized and extent
namespaces share node image inventories and SQL workers; independent archive
verification uses separate shared inventories. Startup opens only existing
namespace-owned archive bindings and refuses missing identities or counters.

Public admission and application-work registration now share the Node's short
maintenance/drain barrier. Commands, enrollments and queries retain their work
leases through queued ownership, unresolved completion and undelivered replies.
Remote query ingress is being moved onto the same controller queue. Gateway
private paths derive from logical history; namespace/schema and exact controller
ownership remain checked. Native effect endpoints now use weak logical handles
instead of looking up per-cell physical bindings.

Inspection found a further query-expiry gap: async timeout can precede physical
callback completion. The new work transfers upstream admission into that SQL
reader and tracks its physical lifetime separately. The held-query fixture now
also exercises an expired response, global admission retention, work visibility,
cancelled close and release after callback exit. Directory regressions additionally
cover history-based placement lookup, wrong namespace/incarnation and a superseded
range. These new assertions have not run while the canonical library is broken.

The first locked library check failed with 132 compiler errors. They primarily
identify unconverted fleet, maintenance, query-wire, resource and shutdown
consumers of the old SDK types. A second check after the physical-query and
native-effect changes failed with 117 errors; the effect adapter's fourteen
old-handle errors are removed. Neither run qualifies a build or executable.
The retained streams, source manifests and new
source copies are under
`.cellar/cell-groups-evidence/CG-07-canonical-startup-advance-01/`.
This is diagnostic evidence, not qualification. Subsequent runtime edits require
another check and all five original CG-07 acceptance cases. Managed namespaces
currently refuse with ManagedAdmissionRequired before catalog writes; whole-group
managed lifecycle is unfinished. No CG task status or release gate was relaxed.

The next critical work is actual Node shutdown/resource/drain conversion and
whole-group fleet authority, followed by public remote routing and ingress,
canonical native fixtures, Axum/admin/CLI/examples and the unchanged CG-07 gate.
CG-09 through CG-16 remain required for the end-to-end hard cutover.

## CG-07 namespace host component

CG-07 remains IN_PROGRESS. CellGroupHost now binds compiled namespace/class
registrations to an exact existing shared controller, runtime, transport and SQL
engine. Its immutable registry has explicit namespace and byte limits; the byte
reservation includes configured invocation, route-cache and directory-cache
capacities. Cell handles contain logical CellHistory and share that registry.
Obtaining or cloning them neither enrolls cells nor adds permanent cell bindings.
The component does not create another consensus algorithm or a legacy mode.

Namespace SQL resources validate schema, image backend, execution limits and
absolute private paths. Materialized directories and extent caches derive their
suffix from the full logical history digest, retaining that identity independently
of group placement. Only admitted workers open SQL. The host refuses reentrant
callback host calls and requires configured matching effect ownership for classes
with private background commands. These bindings grant no leader or bootstrap
authority; every operation still obtains runtime/catalog authority.

Commands and queries enter the existing bounded controller queue synchronously.
Unpolled or disconnected query responses retain admission and physical callback
ownership; close seals all handles and joins the controller's SQL/transport/runtime
owners. Enrollment now captures its original deadline when construction is called,
so delayed polling cannot renew it. Receipt lookup preserves the exact command
binding and returns ambiguous absence without releasing a producer.

All ten checkpoint commands pass. Three new native host cases pass both image
forms over real three-voter engines/TLS and independent SQLite state. They enroll
and mutate eight cells under a one-group limit, verify every replica's values and
logical observations, reject wrong tenant/history/token bindings, retain exact
receipts without callback replay and keep registry bytes constant after obtaining
more handles. A held query exercises dropped response admission, global overload,
cancelled close and engine-replacement refusal until the physical callback exits.
Duplicate namespace, foreign controller and metadata exhaustion refuse without
closing the healthy controller. Delayed enrollment expires before initialization.

The full execution target passes 165 cases (two child-process fixture entries
ignored by the ordinary run), archive-controller passes sixteen (twenty explicit
RustFS ignores), and effects pass ten (one explicit RustFS ignore). Locked host
check, all-target strict Clippy, format/layout, dependency and documentation checks
pass. One SQL worker, one queued item and one pending cell per group remain in the
native fixture; its existing node/group byte ceilings are unchanged. The fixture
now explicitly declares its namespace image backend for the new host boundary.

Record: .cellar/cell-groups-evidence/CG-07-namespace-host-01/record.json
(SHA256 61132e3338e6a729205708ae9cc9043156c51455fecf0ae16df6689939c135d5). It retains all 4685 source hashes, including
five new Rust files, the tracked source patch, copied changed/new files, actual
streams and four executables. An independent complete workspace-volume backup is
retained. No acceptance command, oracle, quota, timeout or validator was weakened.
This is native component qualification, without a new real RustFS, load, managed
lifecycle or complete public SDK/fleet claim.

The host currently binds already bootstrapped services. The critical next step is
to replace canonical Node startup and SDK consumers: separate namespace/class
registration from group placement/provisioning and replicated cell enrollment;
carry logical versions/witnesses and explicit consistency through local and remote
routing, native/Axum ingress, CLI and examples. Managed capacity, repair, drain,
maintenance, stop/return/resume and inventory must operate on complete group
replicas while preserving CAS journals, permissions and physical debt. Keep the
spare checkout's two canonical startup edits and user diagrams. Do not restore
legacy startup, add a legacy/shared toggle or disguise groups as GroupScope.cell.
All five original native_cell_groups_public cases and the complete CG-07 gate
remain required. CG-09 through CG-16 and the full goal remain unfinished.

## CG-07 synchronous shared-command admission

CG-07 remains IN_PROGRESS. The shared controller now captures registered command
admission synchronously and queues construction before returning its response
waiter. Its existing node worker owns preparation, routing, SQL and publication;
an unpolled or dropped response cannot remove that work. Capture retains the
original absolute deadline before queueing. The dispatcher also reserves its
invocation budget when command construction is called, and an unpolled
construction cannot defer its timeout or bypass its configured capacity.

Preparation failures before SQL have no unresolved producer to quarantine.
Once SQL enters, the existing exact reconciliation, sticky ambiguity, receipt,
physical join and group quarantine rules remain intact. No worker per cell or
second execution queue is introduced. All original limits and lifecycle assertions
remain unchanged. Private effect commands still use their sealed registration.

All nine focused-checkpoint commands pass with 4680 source hashes and three
retained executables. The new exact regression passes both image modes. The full
execution target passes 165 cases, with zero failures and two ignored child-process
entry points controlled by their parent crash tests. Effects pass ten cases with
one explicit RustFS ignore; archive-controller passes sixteen with twenty explicit
RustFS ignores. All-target locked strict Clippy, format, layout, dependency and
documentation checks pass. These native suites do not establish a new real-backend,
public SDK/fleet, managed lifecycle, load or release qualification.

Focused record: .cellar/cell-groups-evidence/CG-07-command-admission-fixed-02/record.json
(SHA256 5a44b6f76da74b72eddf45e34cd93c7ec1eb9cf12b67e41acc43fe49331920f6). The record binds the reviewed base revision plus its exact source
patch, all source hashes, actual streams and executables. Artifacts and an
independent complete backup are retained on the workspace volume.

The valid baseline fails because unpolled construction retains zero invocation
slots instead of one. Its exact source and failing executable are preserved under
CG-07-command-admission-baseline-02. The earlier baseline-01 omitted a key-helper
argument and executed no case. Fixed-01 passed the first mode's constructor
assertions, then its fixture attempted a second SQL owner and correctly received
AlreadyExists. Fixed-02 inspects the existing controller pool with the same
zero-active-cell assertion. Those failures and their analyses remain retained;
no acceptance oracle, timeout, ownership check or resource cap was weakened.

The critical next vertical slice is canonical shared host/SDK startup: bounded
namespace/class registrations, group range placement and provisioning, replicated
cell enrollment, logical cell handles/versions/witnesses and explicit consistency.
Then convert managed capacity, repair, drain, maintenance, stop/return/resume and
inventory to whole group replicas with separate cell/image occupancy. Preserve
CAS journals, permissions and disruption debt. The spare checkout's two incomplete
canonical node-service/startup edits remain owned work; never restore legacy
startup or add a legacy/shared flag. All five original public CG-07 cases and its
complete gate remain required before DONE.

## CG-08 complete effects, cache and RustFS gate

CG-08 is DONE at its bounded shared-runtime effects/cache boundary. Qualified
source: 4ea70e483e84ca88d77ca062278edb5ff2389646, including origin/main
81cbbc2f72f302751dd471cf06cf51394146ac63. All twenty-two commands pass: the unchanged
ten-command task gate, five additional exact effects cases, twenty-four library
regressions, the complete shared execution target (164 passed, 2 ignored),
the affected archive-controller suite (16 passed, 20 explicit RustFS ignores),
and dependency/documentation checks. The task's reviewed source anchors still
match. Locked all-target check/Clippy, format and layout pass; no source edits
occurred during the run.

The ten exact native effects cases cover both image forms where applicable:
logical delivery/proof identity across physical bindings, exact cold reopening,
due discovery without client traffic, hot/cold fairness, cancellation and actual
leader transfer, lost receiver replies, held callback/delivery shutdown, S3 claim
and completion coverage, and real materialized cleanup failure. The cleanup-fault
case retains ambiguous outcome and one-group capacity, refuses another group's
registration, and returns the original receipt on the new leader without replay.
No worker, timer or SQL connection is retained for every idle cell.

Serial RustFS at the preceding 8a source exposed a committed-output race despite
its passing native gate. SQL had retired before fresh output authority became
available; retrying SQL reconciliation then produced missing-origin. The current
pending-output phase retains the exact receipt and archive proof, waits only for
a fresh serving fence under the original deadline, and preserves typed terminal
errors. It cannot resurrect a SQL producer, replay a callback or quarantine a
healthy group. The deterministic physical manifest-sync regression fails before
the fix and passes both image modes for new and duplicate receipt paths. Failed
setup attempts, the valid failing baseline and corrected passing snapshots are
retained under the CG-08-output-repro/fixed checkpoint directories.

The explicit real RustFS case passes at this same source and exact effects
executable in both image modes. Seven signed S3 probes pass. Claim settlement
precedes delivery; final complete image/receipt closure is independently restored.
This is a host-native TLS fixture with the RustFS container in one local VM,
not a measured workload or independent-domain qualification. The owned backend
and dedicated VM are stopped, data retained; default Docker context stays colima.

Task record: .cellar/cell-groups-evidence/CG-08-gates-4ea70e48-01/record.json
(SHA256 a072fb6fd8957b6afd5fdf4dffc88c312a0e34c2e1f5c4cfd81b4d2456ea9d0c). Effects executable SHA256:
85ceb50508a519e2b1fd5198f47969185438f77c30d09e9de6ab884fd6a7cce6. All 4680 tracked source hashes, stdout/stderr,
the original runner record and executable are retained and backed up on the
workspace volume. Commands with empty stdout reference their real nonempty
stderr as the generic output artifact; exact case outputs remain untouched.
The unchanged task evidence validator passes. Current RustFS evidence is under
.cellar/cell-groups-evidence/CG-08-effects-progress-783c270b/rustfs-4ea70e48/.
Earlier failing fixture runs and predecessor qualifications remain recorded.

Recorded scope update: shared native fixture helpers under crates/cellar/tests/support/
and the repeatable tools/multi-raft/ RustFS effects selector support the authorized
local verification. The original archive selector's twenty cases are unchanged.
Acceptance names, oracles, commands, limits and validator rules are unchanged.

The next critical implementation is CG-07: replace canonical per-cell birth/host
startup and per-cell binding capacity with group placement and replicated
enrollment; carry logical cell versions/witnesses through SDK/ingress; convert
managed lifecycle and accounting to complete groups. Live transfers, topology,
hibernation, managed recovery, load/crash qualification and final legacy removal
remain required. This task gate does not qualify the public SDK/fleet cutover.

## Earlier CG-08 implementation checkpoints

The complete successor gate passes all twenty commands at 8a4c5ae0, including
164 execution cases with zero failures and two explicit RustFS ignores. The
subsequent fully serial real RustFS case exposed a production output-phase race:
SQL had committed and retired, but a later group entry temporarily prevented
fresh serving authority after archive settlement. The driver retried SQL
reconciliation with no remaining origin and returned an ambiguous missing-origin
error. The passing native gate and the serial RustFS failure remain retained;
the owned backend and VM stopped normally, with data and default context intact.

The correction retains the exact receipt and archive proof in a distinct pending
output phase. Progress retries only current authority under the original deadline;
it cannot reopen SQL, replay a callback or turn completed SQL into quarantine
debt. Both newly committed commands and duplicate receipts use this phase. The
regression holds a real manifest-directory sync on another prepared cell under
the original one-item SQL limit. It establishes committed greater than applied,
requires the first S3 response to remain pending, then checks its exact result,
receipt retry, one callback execution, healthy group and every replica's applied
successor. The valid baseline fails before the correction; the focused corrected
case passes materialized and extent modes for both entry paths in 2.29 seconds.
Its snapshots, executable and real streams are retained under
.cellar/cell-groups-evidence/CG-08-output-repro-04/ and
.cellar/cell-groups-evidence/CG-08-output-fixed-02/. Earlier fixture construction
failures remain recorded separately. No production refusal, deadline or quota
was weakened. The new runtime requires a fresh complete gate and serial RustFS
qualification before CG-08 can become DONE.

At e8edfd12, all ten unchanged required commands pass, along with four extra
effects cases and twenty-four library regressions. The same-source/executable
real RustFS effects case passes both image modes with all seven signed probes.
Its record, logs and executable are retained under
.cellar/cell-groups-evidence/CG-08-effects-progress-783c270b/rustfs-e8edfd12/.
The owned backend and dedicated VM are stopped; data and default context remain.

The expanded execution suite passes 162 cases, fails two and explicitly ignores
two RustFS cases. Both failures use an old summary fixture whose outbox table
contains only next_at; lane-head discovery now also requires lane and sequence.
The fixture correction adds those identities and a third, immediately due row
blocked behind its lane's older sequence. Exact summary assertions still require
the other lane's time 40 and now all three pending rows. Both original failed
cases pass after this test-only correction, in both image forms. Production
validation, resource limits, deadlines and acceptance oracles stay intact.
The failed full gate, source inputs, both regression executables and real streams
remain under .cellar/cell-groups-evidence/CG-08-gates-e8edfd12-01/ and are backed up.
The corrected partial run lives under
.cellar/cell-groups-evidence/CG-08-apply-summary-fixture-e8edfd12-01/.
CG-08 stays IN_PROGRESS until the complete gate passes at the successor source.

The successor implementation is under qualification on codex/shared-cell-groups.
The committed due-index foundation is 783c270b. The node controller now owns fair
group/cell discovery, exact committed-image scans, private alarm/outbox commands,
claim durability, fenced delivery and joined shutdown. No cell owns a timer or
resident execution worker. The expanded predecessor compile check passes.

All five predecessor execution cases now pass in both image modes: alarms, lost
receiver replies, actual leader transfer, delivery shutdown and held alarm
shutdown. The successful native-10 run passes five cases in 7.11 seconds after an
18.44-second build. Its retained executable SHA256 is
e9fa57084a95fe02e0af8d8ddaceda5ccd0b8b9efd00c1b1ee92a61dcc06a8b7.
The preceding run passes four cases but times out on an ordinary follower
pre-vote under the healthy leader lease. The test-only correction uses the real
TimeoutNow transfer and caught-up target check inside the original deadline,
preserving every custody/state oracle. Both source-bound records, executables
and real logs remain under
.cellar/cell-groups-evidence/CG-08-effects-progress-783c270b/.

The native_cell_groups_effects target now passes eight cases with zero failures
and one explicitly ignored RustFS case, in 33.05 seconds after a 3m45s build. Each
case exercises materialized and extent modes, including all five original CG-08
case names. The source-bound native-11 record and executable are retained locally;
executable SHA256:
54d1af2d19bba8aacf206f23d04ca3fb1f3b23f1dafc554bbd49dc079760ffb9.

Cold reopening checks the committed base, exact receipt retry, an older coherent
read and an unchanged Raft term. Discovery leaves 32 cold cells unopened; a cell
re-arming through four alarm generations rotates behind the other due cell.
Identity qualification boots two independent physical group bindings, comparing
exact signed deliveries and receiver proofs while suppressing duplicate execution
under each. This does not qualify live CG-09 transfer. The normal S3 adapter case
requires archived claim coverage before send and independently verifies the final
complete image/receipt closure. Its explicit local RustFS variant remains unrun.

The cache review found materialized settlement closed SQLite while retaining its
private database and LTX files. Shared-cell close now discards those bounded
scratch paths before releasing the private lock, including failed opens and
post-open authority revocation. Sealed images and uncertain proposal custody are
separate. The passing cold-reopen case asserts only the lock inode remains before
injecting a bogus disposable database and checking exact reconstruction. Affected
enrollment regressions remain pending.

The cleanup I/O failure case now passes. It holds the real private cleanup after
commit, transfers leadership through TimeoutNow, returns a typed ambiguous storage
error, and retains the failed invocation and quarantined group's capacity. With
the group/apply limits tightened to one, an actual fresh registration refuses
with Overloaded. The new leader returns the original receipt without callback
replay; both serving replicas retain the value and joined shutdown releases work.
The first test attempted to count retained capacity using the active-group metric;
its failure remains as native-12. Native-13 corrects the active/quarantined
observation and proves quota refusal directly without raising any limit.

The real RustFS effects case passes in both image forms at e4901835, with seven
signed S3 probes, exact source/backend/executable checks and independently restored
final images/receipts. Its executable SHA256 remains
54d1af2d19bba8aacf206f23d04ca3fb1f3b23f1dafc554bbd49dc079760ffb9.
The backend and dedicated VM are stopped, data retained, and default Docker context
remains colima. The fixed --suite effects selection preserves the original twenty
archive cases. Twenty-four source-matched library regressions also pass. Strict
Clippy initially identified sibling-only fixture functions as unused in the
execution target; its shared fixture now carries the same scoped annotation as
other native targets. Production lint, test oracles and qualification commands
remain unchanged. These source-scoped records are retained under
.cellar/cell-groups-evidence/CG-08-effects-progress-783c270b/.

Private-operation discovery now bounds its SQLite projection before allocating
the scheduling preference string. Its corruption/permit regression and strict
Clippy pass; the complete successor CG-08 evidence gate remains required. The later CG-07
public SDK/fleet cutover is still unqualified. CG-06's prior
source-scoped closure remains intact; the full hard cutover is unfinished.

## CG-06 complete native and local disaster-recovery gate

CG-06 is DONE at its declared unmanaged archive/reclamation/recovery boundary.
Qualified native source: e32115aa3218dc6eaa7ed53e134f413f26305938, containing
origin/main 81cbbc2f72f302751dd471cf06cf51394146ac63. Its executable inputs match
the Docker-qualified f8aacd05 source; their only differences are plan documents.
CG-07–16 and the complete hard cutover remain unfinished.

All 123 unique commands in the unchanged CG-01–06 gate refresh pass: 110 exact
native cases, nineteen pure-logic unit tests, all-target locked check/Clippy,
format/layout over 1990 Rust files, native dependency and documentation checks.
Six copied test executables and 3257 source hashes bind the new records. The
unavailable historical CG-01–05 raw records have been superseded by fresh exact
native gate evidence at this source; their earlier results remain historical.
No acceptance command, oracle or validator was weakened.

Current records live under
.cellar/cell-groups-evidence/CG-01-06-gates-e32115aa-02/ and are backed up on the
workspace volume. CG-06-record.json SHA256:
29cfff144bb38f5cda6968e5a8d2cf163ff440df0562ec8948059176ab7827b8.
Its native executable SHA256:
8b18b5677e07862c892dcea3a02fceb36a11234a2636b20d6efae87f4bddd5ac.
Each task record independently passes the unchanged source/artifact/command
validator. Full package validation passes with six DONE tasks, all source-bound
records and 44 local links; no unavailable-record exemption is used.

The reclaim case originally passed but its stderr observations interrupted the
libtest result line in the merged log. That original output is retained. The
unchanged case passes again with stdout and stderr retained and hashed separately;
the strict one-case result requirement stays intact. The original gate runner's
initial unsupported ls-tree exclude-pathspec failure is also retained.

Both complete Docker/RustFS disk-loss drills below pass, including exact physical
recovery and retained restart. These establish local correctness only; managed
recovery, external effects, independent failure domains, sustained load and final
SDK/fleet cutover belong to later gates. The dedicated VM is stopped and data
retained. CG-08's bounded effect scheduler and safe cache lifetimes now have
priority, followed by CG-07 public SDK/fleet cutover.

## CG-06 Docker-fenced, all-voter-disks-deleted recovery

Source: f8aacd052591b7d3a31ea7ca426bef10d9d845cf, containing freshly fetched
origin/main 81cbbc2f72f302751dd471cf06cf51394146ac63. At this source CG-06
remained IN_PROGRESS; the complete gate above closes it.

The actual shared-group Docker/RustFS drill now passes in both materialized and
extent modes. Three voter processes replicate three independent SQLite cells in
one group. Four registered source commands receive S3 acknowledgements. The
trusted sole-launcher controller fsyncs permanent generation revocation, confirms
all three immutable old containers and voter volumes are absent, and only then
signs the exact recovery request. Reloading the controller refuses the revoked
generation; missing/forged certificates and recreated old disks are refused.
Its private key and launcher ledger never enter a voter or operator container.

Three cold S3 restorations select the same complete genesis binding under fresh
group/cell/node/member/disk/engine identities. Pending application writes and all
read modes refuse. Actual TLS Raft replication commits and applies activation
before serving. Exact old receipt results preserve their original incarnation
without callbacks, Quorum retries retain stored S3 minimums, expired client floors
and conflicts refuse, and old tokens remain valid only for their old history.
New S3 writes succeed. Every fresh voter stops and resumes its retained disk with
new process identities; imported and new receipts still avoid replay, another new
write succeeds, and all three replicas' values, versions and incarnations match.
Every remaining owned process joins normally; both runs retain their object and
new voter volumes. All voter and operator containers capture one CPU/3 GiB with
no extra swap; each RustFS captures one CPU/2 GiB.

The runner and native node live in tools/multi-raft/group_disaster.py and
tools/multi-raft/node/. Configurations are copied through the Docker API with
exact byte readback before startup. A real Docker regression verifies snapshot
isolation from later host replacements and exclusion of the controller key.
A separate 150-read probe reproduced 38 truncated JSON reads through mutable
virtiofs bind mounts; that transport is no longer used. Retained failed runs also
cover the initial unshared output path, wildcard transport endpoint mismatch and
an oracle that incorrectly expected genesis version after a later write.
The corrected oracle keeps exact original results and checks the current cell
observation (alpha version 2 after the first new write, beta version 1), with zero
replayed callbacks. No production assertion or protocol was weakened.

Scoped record: .cellar/cell-groups-evidence/CG-06-docker-f8aacd05/qualification.json
(SHA256 08cf2112d4f91519c00d481a6cea3f8c0e378a242ecef258ac67a182c265e77d).
Deployed executable SHA256:
ea5e554d781cdd8004891d28ec8ecfb0abb0664ac5e372a135f0585842045be8.
The record binds both reports, source manifest, raw ledger/log hashes and actual
container/VM resources. Private credentials/signing material stay outside Git;
sanitized scoped evidence is copied locally and backed up on the workspace volume.
This is a local correctness result, without managed recovery, external effects,
sustained load, independent hosts, Amazon S3 or a recovery-time SLA claim.

The original five-case CG-06 native gate and all-target checks still need a fresh
complete record. Historical CG-01–05 raw records remain unavailable; package
validation refuses the missing CG-01 record. Restore or rerun the original gates,
without changing validation or historical task boundaries. Then prioritize CG-08
bounded effect discovery/execution and exact-root cold-cache reopening, followed
by CG-07 public SDK/fleet cutover.

## Earlier CG-06 native quorum and receipt checkpoint

Source: 9a0bad9124f0089055a0ba63b2952e5ef5e77853, including origin/main 81cbbc2f72f302751dd471cf06cf51394146ac63. CG-06 remains IN_PROGRESS.
Fresh recovery now installs the complete imported root and physical Raft genesis
in one synced engine publication. Permanent seed records bind the exact recovery
request, genesis commitment, node, member, disk and engine. Ordinary bootstrap
refuses recovered births; recovered startup revalidates the seed and external
fencing authority. Replacement publication retains the recovery lock until a
catalog-bound activation is committed and applied through the fresh quorum.

The original `fenced_group_seed_new_incarnations` oracle now exercises registered
commands. It writes actual receipts and an expired client floor after the base
checkpoint, archives their LTX tails, and deletes every old native voter directory,
private SQL files, extent store and the previous
verification cache, then seeds and activates three fresh voters. Production TLS
transports, controllers, SQL workers and the replacement group's archive owner
serve retries. Exact opaque results survive without callback replay; Quorum
retries honor stored S3 minimums through actual replacement-history coverage.
Expired sequences and conflicting inputs refuse. Old tokens remain valid for
their original history at the same wall time and fail with WrongHistory for the
fresh history. A new registered write and retained-quorum restart preserve the
imported receipts and client floors.

At this source, archive/catalog suites pass 47 cases; five explicitly ignored
RustFS cases pass when selected against the owned real backend, each in both
materialized and extent modes. Seven signed S3 operations verify conditional
creation, CAS and readback behavior. All-target locked Cellar/logic Clippy,
format/layout over 1985 Rust files, native dependency and documentation checks
pass. The RustFS backend and dedicated VM are stopped with data retained; the
default Docker context remains colima.

Scoped record: .cellar/cell-groups-evidence/CG-06-recovery-genesis-settlement/receipt-recovery-qualification.json (SHA256 eb0efe6af7d4e6bed3a9801d821e0ea8e48058f7ce949cdc01912a74642fe25d). Records bind the tested source
inputs, copied test artifact, exact selections, logs and backend cleanup. This is
a physical/receipt recovery checkpoint, not a full CG-06 DONE or release record.
Earlier failed fixture builds, capture-admission refusal, extent-schema mismatch
and cache-admission failure remain recorded beside the corrected passing run.
The catalog golden now verifies the required recovery-origin wrapper and refuses
the older bare birth record; no compatibility reader was added.

The remaining feature gate is actual Docker fencing. The current native and
RustFS drills use an explicit contract-test authority; they do not qualify
permanent infrastructure launch/restart revocation. The existing Docker harness
uses the older per-cell runtime. Replace its voter path with actual shared-group
nodes, retain its ownership checks/signing flow, revoke the old deployment in a
durable sole-launcher policy, erase all owned voter containers and volumes, then
cold-recover through a fresh quorum. Reopening the launcher must still refuse
the old deployment. Complete that drill and the original CG-06 gate before DONE.

Historical CG-01–05 raw gate directories are currently unavailable in this
checkout. Their prior source-scoped results remain historical; complete package
validation requires restoring those records or rerunning the unchanged exact
gates. This checkpoint does not replace them or weaken the validator. The 37 validator
unit tests pass and all 24 reviewed source anchors match; complete validation
currently refuses the missing CG-01 record.
After CG-06, prioritize CG-08 bounded due-work scheduling and qualification of
the current close-on-settle SQL policy, then CG-07 public SDK/fleet cutover.

## CG-06 fresh-history data import and pending admission

Source: 7052d218d60181d7b0bb5a4c12686e80e294a61c. CG-06 remains IN_PROGRESS. A fenced,
complete source cut can now be prepared into dedicated replacement stores using
`CellGroupRecoveryPool::prepare`. Every cell receives the replacement group's
fresh incarnation, sequence 1, a new enrollment record and a genesis witness.
SQLite bytes stay exact; materialized generation identity is rederived and extent
history metadata is rebound while verified payload identities are reused. The
source intent and its retained remote hold preserve original-witness provenance.
No application callback or Raft engine is created by this data preparation.

The replacement manifest carries mandatory nullable recovery lifecycle metadata.
Pending roots refuse writes, enrollment, stale/strong reads, receipt reconciliation,
membership changes and learner seed authority. No-ops preserve pending state.
Snapshot receive and selected-root reopen retain the full marker; storage refuses
forgetting or replacing recovery lineage. Deterministic apply checks the exact
activation operation, proposal and seed commitment, preserves the first activation
on an identical retry and rejects conflicting activation. A catalog-bound leader
activation admission API and actual replacement-quorum activation remain required.

One node-wide import pool separately bounds cells, restored bytes, metadata and
image workspace. Result/root clones retain import admission. Cancellation during
actual disk sync retains source pins and permits until entered physical work
finishes; shutdown joins it. Clones of the fenced data proof share one verified
cut and its existing admission when preparing multiple destinations.

At this source, seven native shared-group suites pass 246 cases; 36 recovery-related
unit cases and eleven legacy disaster cases also pass. The exact real RustFS
import case passes in both image forms with seven signed S3 probes. It checks
all-cell byte equality, fresh identities/witnesses, old-token rejection, admission
limits and stable retries. All-target locked Cellar/logic Clippy, format/layout
(1971 Rust files), native dependency/docs/preflight/plan checks, 37 plan tests and
five backend-controller tests pass. The owned RustFS backend/profile are stopped
with data retained and the default Docker context preserved. Independent audit
checks 2588 source inputs, Git blobs/symlinks, copied executables, exact case
selection and credential exclusion.

Evidence: `.cellar/cell-groups-evidence/CG-06-recovery-import/qualification/record.json`
(SHA256 `1c7ea62c6bd5c1e81357961c4acba22bb2820d89bc3556ec3bcaf82a43e1c0a3`); independent audit
`1eae08c2bd03eb6fc42d81ae665533716aff5c90cc9217e92221733119d5b75d`. The initial test-fixture private-import compilation error and
its interrupted compiler are retained in `focus-01/`; corrected source passes.
The initial uncommitted-baseline preflight refusal is retained and the pinned
source preflight passes. Earlier socket/deadline/host-pressure and slow-reader
limitations remain historical; no causal fix is claimed for them.

This follows the repository's internal marker-1 hard cutover: older intermediate
cell-group manifest/archive shapes have no compatibility reader. Current native
and RustFS fixtures use the complete new shape. Qualification here covers data
preparation and admission barriers. The replacement still has no engine genesis,
node/member/disk/engine seed binding, fenced publication or serving permission.
Docker voter fencing, fresh-quorum serving after erasing every voter disk, actual
registered-command duplicate receipt recovery and the original
`fenced_group_seed_new_incarnations` oracle remain pending.

Next, consume the complete typed preparation through atomic fresh-genesis storage
and durable physical identity claims. Ordinary bootstrap must refuse an empty or
missing replacement seed after publication. Publish exactly once under the
existing recovery lock, then admit activation only with a catalog-bound permit
and prove its actual commit/apply through a fresh quorum. Reverify fencing on
resume and keep the source hold while its provenance is referenced. Complete the
accepted Docker authority drill and the original full CG-06 gate before marking
this task DONE. Then CG-08 due effects and safe cold-cache eviction precede CG-07
public SDK/fleet cutover; transfer, split/merge, placement, hibernation, load/crash
qualification and final legacy removal remain required.

## CG-06 exact whole-group recovery intents and fencing barrier

Source: 65c5176975907fb52a53deca58e1ae1f5ba7adec. CG-06 remains IN_PROGRESS. The new
`RangeDirectory::fence_group_recovery` boundary independently verifies an exact
held complete-group archive before claiming the namespace or asking infrastructure
to fence it. Canonical operator bytes bind the source birth, archive hold,
replacement identities and original topology mutation. Unknown outcomes retain
that exact request; no latest-head substitution, abort or unfence is supplied.

The recovery lock shares the directory-head CAS with topology publication and
the namespace lifecycle CAS with fleet import. Old source bootstrap and competing
topology mutations refuse once selected. The existing fencing-authority contract
accepts a typed whole-group request, requires permanent external inventory/launch
fencing and independently verifies the winning bounded receipt, including after
reopening the catalog. Archive corruption prevents any fencing call. The current
static unmanaged boundary refuses joint, repair and fleet membership records.

Both image forms pass exact resume and topology-race scenarios against memory
and real RustFS. The actual S3 case covers four scenarios and seven signed probes,
including lost CAS/fence responses, failed independent verification, immutable
intent resume, all-cell image checks and startup/import/publication refusals.
All five native shared-group suites pass (211 cases), as do eleven legacy
disaster cases, locked all-target Cellar/logic Clippy, formatting/layout over 1966
Rust files, native dependency/docs/preflight/plan checks, 37 plan tests and five
backend-controller tests. Independent audit verifies 2583
source hashes, copied executables, exact selection and credential exclusion.
Owned RustFS backends/profiles are stopped with data retained.

Two stack failures are retained with copied executables and diagnosis. The new
fixture's recovery adapter is boxed; the existing extent helper uses its actual
schema contract. A deterministic learner-replacement regression from by-value
recovery-lock metadata is corrected by boxing that rare metadata in the common
directory head. The exact default-stack test passes; canonical record bytes,
stack limits and test assertions are unchanged by the layout correction.

Initial controller-suite socket-address/command-deadline failures and a checkpoint
fixture-driver deadline remain retained, not claimed fixed. Eight controller and
four checkpoint comparisons pass on both copied source revisions. A contemporaneous
extreme host-load snapshot is context, not proof of causation. The full unchanged
source repeat passes; the earlier 8dfe9ff3 slow-reader timing limitation remains.
This is not sustained-load, independent-domain, Docker voter-fence or deleted-voter
replacement-serving qualification.

Evidence: `.cellar/cell-groups-evidence/CG-06-recovery-fence/`. Qualification record
SHA256: `0c94cfd5071d49e59bdefd0d1fae78aff15237602eebcfc4ade48283d50b254c`. Independent audit SHA256: `f2b13f7590220b06a7c23368563b176ce2abcf5d9506869b21538dde20db2c12`.
Prior outage/dependency/reclamation evidence remains unchanged.

The critical next feature is fresh-history image/receipt/client-floor restoration,
fresh engine genesis, one fenced replacement publication, committed activation and
fresh-quorum admission. The returned `FencedCellGroupRecovery` still contains
original-history data and grants no engine, voting or serving permission. Qualify
Docker permanent restart/launch revocation including later/removed voters, delete
all owned voter disks, restore from real RustFS, reject old tokens, prove no
callback replay, perform new writes and restart the retained replacement quorum.
Only then run the full original CG-06 gate. CG-08 effects/cache/result lifetimes
precede CG-07 SDK/fleet cutover; CG-09–16 remain mandatory.

## CG-06 bounded Quorum progress during a full S3 backend outage

Source: 1159e9358c106fe1260c3479759e909e413257a4. CG-06 remains IN_PROGRESS. The
original quorum_continues_bounded_s3_outage case now runs in both image forms,
alongside an exact real RustFS counterpart. It exercises existing static-group
local-quorum durability, routing, owned SQL, archive settlement and reclamation
without changing production behavior or introducing a cached-authority shortcut.

Each scenario explicitly selects a nine-entry group ceiling with one progress
slot. All three voters first verify the actual retained indexes 2, 3 and 4: one
election no-op and two enrollments after genesis snapshot index 1. A warm local
command and remote receipt retry establish current routes and archived state.
The fixture then disables the adapter shared by catalog and archive; the RustFS
counterpart instead requests actual Docker stop/start from its owning runner,
never toggling that adapter's outage flag. Actual S3 probe I/O fails while the
backend is stopped and returns the same probe bytes after restart.

Warm local commands continue through the actual quorum. A later S3 command
commits locally but cannot settle; four subsequent Quorum mutations reach the
exact retained-entry ceiling, preserving every A/B value and logical version on
all three voters. The next command and initialization refuse before their
callbacks. A known Quorum receipt remains available at capacity. S3 output
expires with its original deadline, and a retry asking for Quorum still obeys
the stored S3 minimum. The healthy group is not quarantined.

A separate, already-warmed remote receiver still performs fresh catalog GETs
during the outage and refuses before its callback. The same originating
invocation survives that error and later retries with its exact captured inputs,
identity, original deadline and attempt budget. No failed response becomes a
new-command or non-commit proof.

An exact prepared checkpoint cannot publish beyond verified coverage while S3 is
stopped; the selected checkpoint, retained prefix and debt remain unchanged.
Restart uses the same RustFS container, image, volume, endpoint and resource
limits. The durable journal catches up, independent complete recovery verifies
both A and B, and the original failed S3 command returns its exact result
without callback replay. The completed verifier releases its one-source pool
before subsequent settlement. The same prepared checkpoint then selects
successfully; every voter selects its exact complete cut, retires actual debt to
zero and releases physical source quota before reuse. New local work and the
retained remote invocation commit, with eight total callbacks and the expected
voter state.

The narrow Python controller accepts only four ordered stop/start transitions
for the two image modes. It checks the captured container ID, owner label,
pinned image, CPU/memory, port bindings and mounts before and after every
operation, uses explicit Docker context, and retains actual state observations.
Requests/replies are bounded, atomic local files. The native fixture performs no
Docker process calls. Failed children are joined and preserve output; cleanup
restores only the same verified owned backend. Five protocol/ownership tests
reject foreign or substituted containers and unexpected actions, verify the
fixed transitions, and retain failed-child output.

Twenty exact real RustFS cases pass in both image forms with seven signed
protocol probes and four recorded actual backend transitions. Twenty-three
regression/check commands pass with 270 Rust cases, seven copied executables and
2580 source hashes. All four shared-group native suites, all 22 storage cases,
locked all-target Cellar/logic/LTX Clippy, format/layout,
dependency/docs/preflight/plan checks, 37 plan tests, five controller tests and
system-Python hashing pass. Source/artifact/selection/credential and
backend-transition audit passes under
.cellar/cell-groups-evidence/CG-06-full-backend-outage/.

The original first-source failure and copied-binary reproduction remain
retained: an expected count of four came from the older three-cell fixture,
whereas registered setup enrolls two cells. The successor asserts the physical
log inventory on every voter and adds one real mutation to reach the unchanged
nine-entry ceiling. No original oracle, quota, deadline, payload or production
behavior was weakened. The first real stop/start run and a copied-binary run on
a fresh backend both return503 for the first probe GET after health200. The
successor retries only idempotent Generic read failures within the retained
remote invocation original deadline; missing/authentication failures still
refuse and exact data must match before proceeding. The outage adapter remains
noRetry and no mutation retry or production behavior changes. Both failed real
traces, container observations, copied executable and stopped backend data
remain retained. Earlier qualifications and the unresolved 8dfe9ff3 slow-reader
timing limitation remain retained.

Reflection: four original CG-06 cases now pass: complete-group dependencies,
bounded S3 outage, archive tails and physical reclaim/restart. The remaining
original case is fenced_group_seed_new_incarnations: permanent old-host fencing,
complete old-voter inventory including later joins, fresh group/cell histories,
exact selected-cut results, old-token rejection and a serving replacement
quorum. Use the accepted fencing-authority API plus Docker qualification. The
full CG-06 gate is still pending. CG-08 lifetimes precede CG-07 production
SDK/fleet cutover, followed by irreversible transitions, split/merge, placement,
hibernation, managed recovery, measured load and final legacy removal. This is
bounded static-group outage qualification, not a throughput benchmark,
independent-domain proof, global physical disk bound, voter-disk erasure or
completed hard cutover.

## CG-06 complete-group dependency acknowledgement

Source: `dc0ad3c245a2f9e3dd4f261003c868e9a43f19b8`. CG-06 remains IN_PROGRESS.
The original `group_archive_requires_all_dependencies` case and an exact real
RustFS counterpart now run delay and corruption phases in both image forms. This
qualifies the existing protected-object reuse, complete recovery and S3
settlement paths; production behavior, markers, deadlines and existing resource
limits are unchanged. The recorded scope extension adds the owning
archive-dependency fixture and its shared test exports without changing any
original case or acceptance oracle.

Each phase commits a real registered B mutation before selecting the complete
group archive. B now has different SQLite data from A. The fixture finds an
actual materialized image chunk or extent unique to B, outside A's entire image
closure, and checks its content digest. A separate recovery client persists and
fsyncs its exact acquire intent before obtaining a durable hold. It owns a
bounded two-operation I/O pool, one checkpoint receiver and one tail-operation
pool; the controller's original pools remain unchanged.

The delay phase holds GETs for that exact B image object. A later registered A
command commits to the local quorum but its S3 output stays pending. Another A
command receives a Quorum acknowledgement, and all three voters expose the exact
A/B data and versions. The actual remote registry selection stays at the
preceding cut. Independent whole-group recovery is also pending with its one
receiver occupied. Releasing the GET permits complete historical recovery and
A's exact S3 result. Receivers and tail reservations return to zero before
subsequent work, and the known command resolves locally without callback replay
while preserving its stored S3 minimum.

The corruption phase overwrites that actual stored B image object with
same-length, different-digest bytes and reads them back. Independent recovery
refuses with the exact object-digest failure. A later A entry still commits, but
its S3 output fails ambiguously through the typed archive error; the archive
owner stops and its selected head cannot advance. Another A Quorum command
succeeds, all voter data remains correct and the group is healthy. Repairing
only the injected bytes permits complete recovery through a fresh physical
receiver. It does not silently restart the stopped archive owner or lower the
original command's S3 minimum. Internal exact receipt lookup observes the
original result without callback replay and supplies no external S3
acknowledgement.

Nineteen exact real RustFS controller cases pass in both image forms, with seven
signed protocol probes. Twenty-two regression/check commands pass with 269
distinct Rust cases across seven copied executables and 2576 source inputs. All
four native shared-group suites, all 22 storage cases, locked all-target
Cellar/logic/LTX Clippy, format/layout, dependency/docs/preflight/plan
validation, 37 Python tests and system-Python hashing pass. Independent
source/artifact/selection and credential-exclusion audit passes. Evidence under
`.cellar/cell-groups-evidence/CG-06-archive-dependencies/` binds raw outputs,
copied executables, orchestration, reviewed plan inputs and the preceding
physical-reclamation qualification. The physical purge/restart case also passes
at this source. Earlier retained failures and the unresolved `8dfe9ff3`
slow-reader timing limitation remain recorded at their original source
boundaries.

Reflection: three of the five original CG-06 cases now pass: complete-group
dependencies, archive tails and physical reclaim/restart. The next critical work
is the original full-backend S3 outage case: exercise local cached and remote
fresh-discovery paths separately, establish the exact bounded Quorum admission
boundary, and refuse S3 outputs until complete coverage recovers. Preserve
catalog/epoch fencing; durable source sealing and activation are required before
introducing any new cached-authority shortcut. Fenced fresh-incarnation serving
from a replacement quorum remains the other original case. The accepted
fencing-authority API plus Docker qualification must inventory every possible
old voter, including later joins. CG-08 lifetimes precede CG-07 public SDK/fleet
cutover, followed by transitions, split/merge, placement, hibernation, managed
recovery, real load and final legacy removal. No full task, production cutover,
independent-domain qualification or zero-quorum disaster-serving claim is made.

## CG-06 physical log reclamation and retained-disk recovery

Source: `7741e4049704f3e1c6519b736e62deaf263c39a2`. CG-06 remains IN_PROGRESS.
The original `group_reclaim_restart_keeps_history` case now runs in both image
forms, together with an exact real RustFS counterpart. It uses the production
purge, checkpoint, image/page maintenance, SQL and recovery APIs. Production
behavior and marker-1 records are unchanged. The new scenario explicitly selects
4 KiB target log files, an 8 KiB purge threshold and manual purge so filesystem
observations cannot race automatic maintenance; all prior fixture defaults and
quotas remain unchanged.

Two real groups share each engine. The primary commits 24 registered commands
across two SQLite cells, retaining exact results and alarm state. Independently
verified complete S3 coverage permits its exact checkpoint. Eight subsequent
entries remain in the local suffix during archive lag. The sibling retains its
entire uncheckpointed history. Purge must remove actual original Raft files and
reduce their aggregate file length, while both histories remain byte-for-byte
intact and an old image reader remains readable. A retained checkpoint streams
into an independent private receiver after purge; an ahead live voter still
rejects rollback. Quiesced node services then perform bounded image/page
maintenance over the complete two-group inventory, preserving reader, current
and checkpoint closures.

All three voter disks close and reopen under new process generations, preserving
their group/cell identities. Recovery verifies every selected image, effect
summary, checkpoint and log suffix, including the sibling. Fresh quorum resolves
all 24 exact receipts without callback replay, refuses local compaction without
a newly verified archive frontier and commits two new commands over TLS. This is
retained-disk restart, not deletion of all voter disks or fresh-incarnation
disaster recovery. No cached status grants archive or serving authority.

Real RustFS observations from this controlled test geometry:

| Image mode | Raft file bytes before → after purge | Original files removed | Retained suffix entries |
| --- | ---: | ---: | ---: |
| materialized | 474445 → 256891 | 42 | 8 |
| extents | 474469 → 256817 | 42 | 8 |

These measure actual file lengths at the purge boundary, not allocated
filesystem blocks, a global disk bound, throughput or production sizing.
Reader/transition holds and sustained workloads need their broader gates.

Eighteen exact RustFS controller cases pass in both image forms with seven
signed S3 protocol probes. Twenty-two regression/check commands pass with 268
distinct Rust cases across seven copied executables, including all four native
shared-group suites and 22 storage cases. Final-source locked all-target
Cellar/logic/LTX Clippy, format/layout, dependency/docs/preflight/plan checks,
37 Python tests and system-Python hashing pass. Source/artifact/selection and
credential-exclusion audit binds 2575 source inputs, copied
outputs/binaries/orchestration and frozen plan inputs under
`.cellar/cell-groups-evidence/CG-06-physical-log-reclaim/`.

Source-bound failures remain retained. The initial helper visibility failure did
not run a case. A copied binary reproduces the subsequent oracle-reader failure:
`GenerationReader` refuses reads above 64 KiB. The helper now reads every byte
in bounded chunks without raising the reader limit, shrinking payloads or
dropping retained owners. A second copied-binary reproduction confirms that an
ahead live voter must reject the older retained cut. Positive data verification
now uses the existing private archive receiver, while an explicit negative
transfer preserves the live rollback guard and one-slot pool limits. A further
copied-binary timeout exposes the test retaining the archive pool's sole
verification slot past its completed checks; it now drops that result before
awaiting a new cut, while the durable journal retains the remote hold. The quota
and deadline remain unchanged. The first successful Rust run emitted
observations inside the libtest progress line; its wrapper incorrectly refused
selection. Raw output and the original record stay unchanged. Validated
observation normalization now preserves exact matching and the zero-failure
summary; five checks reject wrong, failed, empty and incomplete selections.
Final-source Clippy also requires the project diagnostic macro instead of std
println; the structured observations use that existing API without suppressing
the lint. The archive-store fixture is declared once per integration target, and
the archive-only scenario lives under the owning archive harness with its
existing target-specific exports. Source-bound duplicate-module, unused-helper
and reexport-visibility compile/lint failures remain retained; no production
lint setting or test assertion changed. Original assertions, task cases and
acceptance oracles remain intact. Earlier qualifications and the unexplained
`8dfe9ff3` slow-reader timing limitation remain retained.

Reflection: two original CG-06 cases now pass: archive tails and physical
reclaim/restart. Complete archive-dependency acknowledgement, full S3-backend
outage authority and fenced fresh-incarnation serving remain required, together
with the full task gate. Investigation confirms that Seal/Install/Activate wire
entries exist but live apply still rejects them and manifests lack the durable
transition lifecycle. Before introducing a cache-based outage shortcut,
cancellation/authorization must share one durable decision, sources must seal,
and children must install and activate under an exact publication. Existing
local cached and remote fresh-discovery paths must first be qualified separately
against a stopped backend; the next independent deliverable is the original
complete-group dependency acknowledgement gate. The independent reclamation case
does not waive this dependency. Finish CG-08 effect/cache/result lifetimes
before CG-07 public SDK/fleet cutover, then source transitions, topology,
placement, hibernation, managed recovery, measured load and final legacy
removal. The full goal remains active and incomplete.

## CG-06 retained-log admission and progress capacity

Source: `ad73f03f278f5845b311fe929d184cc947382b2d`. CG-06 remains IN_PROGRESS.
Shared-group logs now account for logical retained entries and outstanding
reservations against explicit node and group byte/count ceilings. The ledger
reconstructs selected durable debt on reopen. Preexisting debt is preserved when
a restart uses smaller limits, and new admission waits until it fits.
Encoded-entry bytes include bounded accounting overhead; these are logical
limits, not physical Raft Engine file-byte bounds.

New SQL callbacks and initializers reserve a worst-case command envelope before
execution. Known receipt reconciliation remains available at the ceiling. A
preparation carries an exact engine/group/entry lease through submission and
uncertain outcomes. Only sealed pre-append rejection or authoritative
reconciliation releases unused preparation capacity. Entered storage retains the
lease after waiter loss, then transfers its charge into selected durable entries
after sync/cache publication without a release gap or double debit. Consumed
leases cannot authorize another append.

Local, inherited follower, election and configuration entries use the same
ledger. Empty election and automatic configuration entries have separately
reserved progress capacity. A joint transition promises its automatic leave
before it can enter RawNode, avoiding capacity acquisition after collected apply
work changes configuration. Capacity refusal on a deferred tick is nonfatal.
Role changes revoke concurrent execution authority before accounting. Actual
selected checkpoints and suffix replacement retire debt; archive status alone
does not. Existing archive-required compaction still needs independently
verified complete coverage.

Three exact real-engine cases pass for entry ceilings/reopen, independent
node/group byte ceilings with slack counts, suffix replacement, exact lease
identity and held physical sync after all caller owners disappear. Both native
SQL image forms pass before-callback and initializer refusal, known receipt
recovery, sibling progress, exact local checkpoint selection/source release and
resumed writes. They fill ordinary admission before a real automatic joint
transition and require its automatic leave through the unchanged single progress
entry. The local checkpoint scenario does not claim a full S3 backend outage or
physical file purge.

All seventeen exact real RustFS controller cases pass in both image forms, with
seven signed S3 protocol probes. Twenty-two regression/check commands pass with
267 distinct Rust cases and seven copied executables; all 22 storage cases and
four native shared-group suites run. Locked all-target Cellar/logic/LTX Clippy,
format/layout, native dependency/docs/preflight/plan checks, 37 Python tests and
system-Python hashing pass. Source/artifact/selection and credential-exclusion
audit passes across 2574 source inputs. Records, copied binaries/outputs,
orchestration and frozen plan inputs are under
`.cellar/cell-groups-evidence/CG-06-log-retention/`.

Source-bound failures remain retained: initial accessor/type/layout compile
checks, the coherent-hardstate correction to a wrong-term lease negative, the
native fixture's impossible leader-only wait on followers, and the aggregate
snapshot test's default-stack overflow. Copied binaries reproduce these fixture
failures. The successor keeps the original identity assertions, quotas and
deadlines and waits for the exact selected cut, retired debt and released
checkpoint source on each voter. The aggregate snapshot case uses the existing
non-inlined fixture constructor to keep large async construction off its poll
frame; the exact case and complete suite pass on the original default stack. No
temporary DEBUG instrumentation remains. The earlier `8dfe9ff3` slow-reader
timeout remains unexplained despite unchanged passing reruns; this change is not
claimed to fix it. The RustFS controller matrix does not delete voter disks.
Original CG-06 cases, acceptance oracles and evidence array remain unchanged;
four original cases/full task gate remain incomplete.

Reflection: next qualify a full backend S3 outage with fresh catalog/epoch
authority, without caching ownership past fencing. Complete and qualify physical
shared-engine file reclamation constrained by every
checkpoint/archive/reader/transfer and sibling hold; logical admission alone
does not establish a disk bound. Complete fenced fresh-group/cell-incarnation
recovery and replacement-quorum serving with the complete inventory of possible
old voters, including joins after the selected cut, using the accepted
fencing-authority API plus Docker qualification. Then finish CG-08
effect/cache/result lifetimes before CG-07 public SDK/fleet cutover. Topology,
placement, hibernation, managed recovery, measured load and final legacy removal
follow. Voters remain host-native tasks and RustFS one local VM; independent
failure domains and the full hard cutover remain unqualified.

## CG-06 owned archive activation and checkpoint drain

Source: `ab0abf26dea4df436b5ced926f28a9ede4f360fd`. CG-06 remains IN_PROGRESS. A
deterministic regression at `79323600` reproduced a checkpoint overtaking
pending archive activation. Registration now owns an activation intent that
closes later checkpoint admission across retries. Rejected or unentered work
releases its intent. An already admitted checkpoint shares one exact
group/cut-bound lease between its owner and physical storage, so that work can
drain while new admission is closed. Storage rejects foreign-group, wrong-cut
and non-checkpoint lease use. The existing 4 KiB metadata charge bounds the
lease; quotas and serialized marker-1 records are unchanged.

Once metadata persistence enters, physical work retains the activation even
after the waiter disappears. The irreversible local requirement becomes
effective only after sync and cache/root publication. Failed or uncertain writes
cannot publish the intent. Frontier reset happens once before I/O, so a delayed
duplicate completion cannot clear newer verified coverage. No mutex spans I/O or
an await. Cancellation before entry releases the temporary barrier; it never
clears an existing durable policy.

Three exact storage cases pass. The original recovery case now checks refusal of
later admission and ownership across cloned physical leases. The new checkpoint
case uses actual persisted entries and a prepared checkpoint, tests invalid
lease bindings, then drains an earlier checkpoint into the real engine while
activation waits. The new held-sync case drops the waiter and client activation,
proves physical work still owns the closed barrier, releases the actual
filesystem gate and joins the storage queue before checking durable policy.
These establish the owning boundaries; sustained load fairness is still pending.

All seventeen exact RustFS cases pass in both image forms with seven signed
protocol probes. Twenty-two regression/check commands pass with 262 distinct
Rust cases across seven copied executables, including all nineteen storage cases
and the four native shared-group suites. Locked all-target Cellar/logic/LTX
Clippy, format/layout, dependency/docs/preflight/plan checks, 37 Python tests
and system-Python hashing pass. Records under
`.cellar/cell-groups-evidence/CG-06-retention-admission/` bind 2571 source
hashes, selections, copied binaries/outputs, orchestration and frozen plan
inputs. Independent source/artifact/selection and credential-exclusion audit
passes. The original failing regression is retained with its source and
executable. Original CG-06 cases, oracles and quotas remain unchanged; four
original cases/full gate remain pending. The earlier `8dfe9ff3` native
slow-reader timeout remains retained and unexplained despite unchanged
successful reruns; this ownership change is not claimed to fix it. The RustFS
controller matrix does not delete voter disks.

Reflection: implement explicit retained-log byte/entry admission across node and
group before new callbacks, preserving known receipt reconciliation. Include
inherited follower entries, control entries, suffix replacement, restart and
uncertain physical outcomes, with reserved progress capacity so collected Ready,
apply and checkpoint work can drain. Then qualify a full S3 backend outage,
including fresh catalog authority without weakening ownership checks. Physical
shared-engine file purge must honor every checkpoint, archive, reader, transfer
and sibling constraint. Fenced fresh-incarnation serving needs complete
possible-old-voter lineage and the accepted fencing-authority API plus Docker
qualification. CG-08 effect/cache/result lifetimes precede CG-07 public
SDK/fleet cutover. Topology, placement, hibernation, managed recovery, measured
load and legacy removal follow. Voters remain host-native tasks and RustFS one
local VM; independent failure domains and the full hard cutover remain
unqualified.

## CG-06 archive-aware local checkpoint compaction

Source: `8dfe9ff3a316d0579b656af4818dc57d839a6765`. CG-06 remains IN_PROGRESS.
Archive registration now syncs an irreversible local `archive_required` bit
before object-store I/O. The marker-1 group application record requires this
field, without a legacy decoder or migration. Ordinary writes and peer snapshot
installation preserve the local requirement. Reopen reconstructs the requirement
with no trusted in-memory frontier; the journaled current remote hold must be
independently verified again before local compaction.

Each engine-local group shares one bounded frontier across its handles. Only
independently verified complete archive coverage advances it. The route/history
and local retained term must match; equal-index coverage must match the exact
root. Older conflicting evidence cannot replace a newer cut. A reconciled intent
behind the retained checkpoint grants no frontier and waits for a newer complete
cut. Archive status remains advisory. The frontier grants neither S3 output nor
serving authority, and retains no replay workspace per idle group. The durable
journal continues to own the current remote hold.

The automatic scheduler checks the frontier before capture, allowing quorum
apply and a sibling group's checkpoint to continue while archival is held. The
actual Raft owner checks its exact captured cut, and the storage worker repeats
admission before any local checkpoint compaction batch. A prepared source
remains a data capability and cannot bypass either boundary. Already admitted
checkpoints retain owner and physical-storage guards until completion; archive
activation retries their drain before syncing policy. This avoids retracting a
checkpoint while retaining its sole source slot. No mutex spans an await or
physical I/O. Registration also checks the actual local birth before policy
changes.

The new native and real RustFS scenario holds actual registry publication in
both image forms. Quorum adds another cell while the configured group's
checkpoint and log prefix stay intact. A sibling publishes through the original
one-source pool and one controller checkpoint slot. Direct owner admission and
raw persistence of the prepared local checkpoint both refuse. After actual
complete coverage, the same preparation publishes; another archive outage
preserves later committed log entries without quarantine. Independent recovery
checks every selected cell. The storage regression checks activation refusal
while an earlier admission is held, durable activation after drain,
missing-field codec refusal, inability of ordinary writes to clear policy and
recovered admission remaining closed without fresh coverage.

All seventeen exact RustFS cases pass in both image forms, with seven signed S3
probes. All twenty-two regression/check commands pass with 260 distinct Rust
cases across seven copied executables. These include the four native
shared-group suites, all seventeen storage tests, nine snapshot unit cases,
cache/receipt/budget/journal/physical-join primitives, eighteen wire cases and
fixed metric cardinality. Locked all-target Cellar/logic/LTX Clippy,
format/layout, dependency/docs/preflight/plan checks, 37 Python tests and
system-Python hashing pass. Records under
`.cellar/cell-groups-evidence/CG-06-archive-compaction/` bind 2571 source
hashes, exact selections, frozen plan inputs and executable/output hashes.
Independent source/artifact/selection and credential-exclusion audit passes. The
initial capture/import compile refusal, recovery-test lock-scope lint and native
zero-operation refusal remain source-bound. A copied executable reproduced the
native failure; typed status identified the invalid private metadata correlation
ID. The correction uses a valid nonzero ID while preserving public zero-ID
rejection and the independent physical sync sequence. The fixture uses runtime
capture and orders sibling work before retaining its original source slot.
Original cases, oracles and quotas remain unchanged. The first broad regression
attempt timed out in the existing native slow-reader case; unchanged
copied-executable isolated and full ordered-controller reruns passed. The
passing complete rerun retains that failure and its unresolved timing limitation
without changing code, timeouts, quotas or assertions. Four original CG-06
cases/full gate remain pending.

Reflection: add explicit node-wide unarchived-byte/entry admission, including
inherited peer and control work, pre-callback reservations and capacity for
checkpoint/physical completion progress. Qualify a full S3 backend outage,
including the catalog-binding dependency without weakening ownership checks.
This compaction gate alone does not establish that disk bound. Follow with
physical shared-engine Raft file purge under every
checkpoint/archive/reader/transfer/sibling constraint, and fenced
fresh-incarnation serving with complete old-voter lineage inventory.
Peer-install integration of deployment archive policy, controller process-kill
and multi-head/physical-read shutdown cuts still need qualification. Archive
activation fairness under sustained checkpoint traffic also needs an adversarial
gate. CG-08 lifetimes precede CG-07 SDK/fleet cutover. Topology, placement,
hibernation, managed recovery, measured load and legacy removal remain. Voters
are host-native and RustFS is one local VM; independent failure domains and the
production hard cutover remain unqualified.

## CG-06 remote S3 output ownership through physical delivery

Runtime source: `25938fd86d13a2755066660797b07aff955ec1ad`. Validation-only
metric inventory correction: `debdc30c8327aad35757c6b849e029c1bbdab691`. CG-06
remains IN_PROGRESS. Commands, enrollment and receipt lookups now transmit their
effective S3 requirement and return authenticated achieved durability. Unknown
durability codes and replies below the original requirement refuse before result
use. Existing exporter-bound MACs bind the body digest to the exact request,
purpose, peer process and complete group/cell history. Command/receipt tokens
retain their signed observation checks; enrollment returns the original creation
record, including after later mutations. Marker-1 hard cutover adds no legacy
reader or runtime mode flag.

The receiver still needs an explicit provider binding to its exact runtime and
active archive group. It uses the independently recovered complete-cut proofs,
namespace/stored minima, current catalog/leader checks and original deadlines
already required locally. An unbound receiver refuses before mutation when S3 is
required. A stored-minimum duplicate resolves SQL before refusing an unbound
owner. Possible transmission keeps mutation failures ambiguous. No transport
retry replays application callbacks, and receipt absence remains ambiguous.

A bounded owned reply now retains the native S3 output with its encoded bytes.
That output owns physical coverage, S3 admission and journal/remote-hold
lifetime. The existing transport reservation accounts for encoded bytes. Quorum
outputs release resolved SQL as before. During writing, an aggregate owns both
the TLS stream and reply: stream destruction precedes proof release, even if the
entire serving future is destroyed. Successful flush precedes consumption.
Cancellation, revocation and timeout cannot leave an acknowledged proof-free
response in the writer. No per-cell task, timer or metadata-only proof cache is
introduced.

All sixteen exact RustFS cases pass in both image forms, plus seven signed S3
protocol probes. Three new cases cover all remote output kinds and namespace/
stored minima, original creation retries, unbound receiver refusal, archive-wait
deadlines/cancellation and actual slow command/receipt TLS delivery. Independent
wire clients use a bounded sequence of exact retries to fill small real kernel
buffers without reading replies, holding a 28 KiB body within the unchanged 64
KiB S3 envelope. TLS header reads prefetch and a single reply fits the buffers.
The new fixed physical-write observation identifies the held phase without
client reads. Proof/coverage and byte admission remain retained while SQL is
free and a same-cell Quorum successor commits. Another S3 operation overloads
within the original one-slot limit. Drain succeeds; disconnect, grant revocation
and timeout join physical release. Truncated bodies cannot become successful
replies; exact retries return the original result without callback replay. The
earlier thirteen archive/local-output scenarios also pass.

Twenty regression/check commands pass with 232 distinct Rust cases across seven
copied executables, including all eighteen wire codec/authentication cases,
receipt/ budget primitives, fixed performance outcomes/cardinality, the four
native shared-group suites and LTX clones. Runtime all-target Clippy,
format/layout, dependency/docs/preflight/plan checks, 37 Python tests and
system-Python hashing pass. Runtime Clippy and 231 passing regressions retain
their original source and hashes. The validation successor fixes the two
archive-readiness gauge names/count already added on main, plus the matching API
comment. Its exact metrics fixture, focused Clippy, format and repository checks
pass. An independently checked two-file delta excludes runtime behavior or quota
changes. Records under `.cellar/cell-groups-evidence/CG-06-s3-remote/` bind 2570
source hashes, exact selections, logs/executables, frozen plan inputs and
predecessors. Independent source/artifact/selection and credential-exclusion
audit passes. The initial import-path failures retain their exact sources and
outputs. The failed raw probe and copied-executable reproductions also remain
retained: its operation length needed u16, and reading a TLS header prefetched
the full body, while a single undrained reply still fit the socket buffers. The
final probe fills a bounded retry window and observes the physical write phase.
The initial native metrics inventory failure and copied-executable reproduction
remain retained. PR #159 added two fixed readiness gauges while its inventory
expected 111. The correction requires both names and exactly 113 resource
samples, preserving duplicate/type/size checks and one-versus-32-group identity
equality. An explicit released-command admission barrier identifies the
successor phase. No original case or existing node quota was weakened. Four
original CG-06 cases and its full gate remain open.

Reflection: next implement explicit unarchived-byte/disk admission and qualify
bounded operation during a full S3 outage; the archive-wrapper faults above are
not that gate. Fresh destination catalog binding can still depend on S3 until
replicated transfer seals are integrated; resolve that dependency without
weakening ownership admission. Add safe physical shared-engine Raft file purge
with complete checkpoint/archive/reader/transfer constraints, then fenced
fresh-incarnation serving with every possible old-voter lineage, including joins
after the cut. Fencing-authority API plus Docker qualification remains accepted.
Multi-head hold retention, process-kill restart and physical-read shutdown cuts
still need broader qualification. Incremental committed tails and smaller sealed
attestations backed by actual remote holds remain performance opportunities.
CG-08 lifetimes precede CG-07 SDK/fleet cutover; topology, placement,
hibernation, managed recovery, load and final legacy removal remain. Voters
share one host and RustFS one local VM; independent hosts, measured load and
fenced replacement serving remain unqualified.

## CG-06 local receipt S3 observations and bounded result reads

Source: `cde0eea2f516486578fdee7a4cc0b8be9ba8f72a`. CG-06 remains IN_PROGRESS. Local command,
enrollment and receipt outputs now share the explicitly bound controller archive
owner. `CellReceiptInvocation::select` returns an opaque quorum selection; its
consuming `response` settles required durability. Existing `execute` runs both.
S3 admission takes the selected bytes before SQL read admission releases. The
selection queues no archive work until response; dropping it refunds admission.
Request, namespace and stored receipt minima are never lowered.

The complete independently recovered group must cover the quorum cell history/
version and equal-version decision. Known receipts remain exact across later
images. An initially absent receipt is read again from the actual archive, which
may now contain the command. Return that image's actual observation and token;
never attest an old absence against a changed image. None remains ambiguous and
never releases a producer. Empty results remain distinct. Lookup runs no command,
initializer or receipt maintenance; expiry, conflict and sequence gaps stay typed.

Result length is inspected before payload fetch. The same S3 permit grows for
actual bytes without another item slot or increased node ceilings. A legitimate
40 KiB result exceeds the unchanged 64 KiB fixture S3 envelope with duplicate-read
headroom: S3 returns typed Overloaded before allocation, then Quorum still reads
the intact result. Original SQL/S3 node quotas remain unchanged. The new expiry
scenario deliberately fills a two-receipt ceiling so normal replicated maintenance
prunes an expired prefix while retaining the latest receipt and permanent floor.

S3 outputs retain complete physical coverage, shared pool admission and the journal
lock. Catalog proof rechecks enter the captured execution domain and remain bounded
by the original deadline for all three output kinds. Receipt output additionally
checks a fresh correlated quorum read's current leader/route and version floor.
That read fence tolerates later cell mutations without retaining a writer's base.
Remote S3 delivery remains guarded on both ends, including provider-configured
receivers that discover a stored S3 minimum. A rejected committed command duplicate
clears SQL ownership first and remains ambiguous to its remote caller.

All thirteen exact RustFS cases pass in both image formats, plus seven signed S3
probes. The four new cases cover later receipts replacing initial absence, known
results at newer cuts, signed observations, empty/absent results, namespace/stored
minima, unsubmitted-phase drops, foreign providers, typed conflict/expiry, payload
admission, read deadlines/cancellation and actual TLS receiver refusal without
callback replay or quarantine. The earlier archive/command/enrollment cases pass.

All sixteen check/regression commands pass: 205 distinct Rust cases across six
copied executables, locked all-target Cellar/logic/LTX Clippy, format/layout,
dependency/docs/preflight/plan checks, 37 Python tests and system-Python hashing.
Records under `.cellar/cell-groups-evidence/CG-06-s3-receipts/` bind
2567 source hashes, exact selections, frozen plan inputs, executable/output
hashes and predecessors. Independent source/hash/selection and credential-exclusion
audit passes. The initial direct-caller domain panic retains its exact source,
executable, output and minimized backtrace. Production scoping fixes it; no global
host handle or assertion workaround was added. The intermediate four-case success
and final deadline-bound source remain distinguishable. The fixture macro-lint
refusal is retained; cancellation now polls the response and observes an actual
archive attempt before dropping it. No original task oracle
or existing quota was weakened. Four original CG-06 cases/full gate remain pending.

Reflection: implement remote S3 output proofs through physical delivery, preserving
authenticated purpose, achieved durability, route/version and original deadlines.
Then explicit unarchived-byte/disk admission, physical shared-engine Raft purge
and fenced fresh-incarnation serving with every possible old-voter lineage.
Qualify multi-head leases, process-kill restart and physical-read shutdown cuts.
Incremental committed tails and smaller sealed attestations backed by real remote
hold leases remain performance opportunities. CG-08 lifetimes/maintenance precede
CG-07 SDK/fleet cutover; topology, placement, hibernation, managed recovery, load
and final legacy removal remain. Voters share one host and RustFS one local VM;
independent hosts, measured load and fenced replacement serving remain unqualified.

## CG-06 local enrollment S3 response settlement

Source: `c2801b6586ffc8b3b765312e0042986e881d773a`. CG-06 remains IN_PROGRESS. The explicit
`CellGroupDispatcher::with_s3` binding now also settles local enrollment responses.
The existing fair archive lane owns both output kinds under independent bounded
S3 admission. Enrollment reserves its bounded original creation record before
releasing resolved SQL output retention; no original SQL/command quota changes.

Required durability is the maximum of request and immutable namespace minimum,
captured in the invocation fingerprint. Exact runtime ownership and active archive
registration precede initialization. After commitment, producer/preparation
ownership clears before the archive wait. A complete independently recovered group
must contain the exact history, creation ID, schema and initial image commitment.
An older cut waits; conflicting creation refuses. Subsequent mutations preserve
that original provenance without retaining superseded initial bytes. Proof and
exact retry never rerun initialization.

The response rechecks catalog birth, actual leader authority and its exact current
creation record under the original deadline. `CellSqlEnrollment::durability`
reports S3 only while retaining its private complete-coverage proof, pool admission
and journal lock. Active outputs backpressure the shared proof pools until release;
idle groups retain no replay workspace. Remote enrollment explicitly refuses S3
before transmission until its wire contract is integrated.

Nine exact RustFS cases pass in both image formats. The two new cases also exercise
request and namespace minimums, a foreign runtime refusing before initialization,
publication-held output, a later same-cell Quorum mutation during the S3 wait,
retained proof slots, original-record duplicates and conflicting creation IDs.
Deadline and dropped-reply branches keep the one-command/one-SQL-slot quotas,
refund admission, allow a Quorum successor during archive outage and retry the
same creation with exactly one initializer invocation. Seven signed S3 probes
and all earlier local command/archive/refusal cases pass.

All fourteen regression/check commands pass: 195 distinct Rust cases across six
copied executables, locked all-target Clippy for Cellar/logic/LTX, format/layout,
dependency/docs/preflight/plan checks, 37 Python tests and system-Python hashing.
The retained records under `.cellar/cell-groups-evidence/CG-06-s3-enrollment/`
bind 2566 source hashes, frozen plan inputs, exact selections, executable
and output hashes, orchestration sources and prior qualification. Independent
source/hash/selection and credential-exclusion audit passes. No original task
case, oracle or pre-existing quota was weakened. The initial fixture-allocation
Clippy refusal and its exact source/log remain retained; the correction changes
only two literals. Four original CG-06 cases and its full gate remain pending;
this is partial task evidence.

Reflection: finish local receipt lookup, preserving typed expiry/conflict and the
ambiguity of an absent row, then remote command/enrollment/receipt output proofs
with retention through physical delivery. Add explicit unarchived-byte/disk
admission, physical shared-engine Raft file purge and fenced new-incarnation
serving with complete old-voter lineage inventory. Qualify multi-head proof
retention, controller process-kill restart and physical-read shutdown cuts.
A performance opportunity is incremental committed-tail scheduling and smaller
sealed output attestations backed by real hold leases, qualified under load.
CG-08 lifetimes/maintenance precede CG-07 public SDK/fleet cutover; topology,
placement, hibernation, managed recovery, load and final legacy removal remain.
Voters share one host and RustFS one local VM; independent hosts, measured load,
fenced replacement-quorum serving and the full hard cutover remain unqualified.

## CG-06 local command S3 response settlement

Source: `3d564f3db2c2eefc87119e7b3395f1bca17b99fa`. CG-06 remains IN_PROGRESS. An explicit
`CellGroupDispatcher::with_s3` binding now settles local commands through the
existing node archive lane. It checks the exact runtime owner and active group
registration. Required durability is the maximum of request, namespace and stored
receipt minimums. Unbound dispatchers retain their refusal; receipt lookup,
enrollment and remote S3 response gates remain unfinished.

SQL resolution clears producer ownership before archival. A separate bounded S3
stage acquires admission before taking over the committed result bytes; resolved
preparations no longer retain duplicate SQL admission. Existing SQL/command quotas
are unchanged. The first same-cell successor test reproduced Overloaded because
the waiting receipt still held the SQL pool's only operation permit. Its source
snapshot, executable and output are retained. The fixed test passes with that same
one-slot SQL quota, including an explicit zero retained-SQL-operation assertion.

The fair archive lane takes at most 32 receipt proofs per group turn. A published
complete cut is independently replayed and its exact immutable receipt is read
without callbacks. History/version/position, equal-version decision and every
stored receipt field/result must match. A later image can prove an older receipt;
an advisory index or sibling progress cannot. The response rechecks catalog birth
and live leader authority before its token. It retains physical coverage/admission,
S3 byte/item retention and the exclusive journal lock. Weak owner uses block
predecessor-hold retirement until consumers release the coverage. Idle groups keep
no replay workspace; held outputs intentionally backpressure those shared pools.

Seven exact RustFS cases pass in both image modes, including positive S3 responses,
a later same-cell quorum mutation during archive outage, publication-held output,
Quorum retries honoring a stored S3 minimum, proof-slot retention, callback-once
retries, one-command-slot expiry/cancellation isolation and missing-B dependency
refusal while A's receipt and quorum remain healthy. Foreign runtimes refuse
before SQL. A namespace S3 minimum overrides a Quorum request, persists that
minimum and keeps its response pending until publication. The three earlier setup/publication/refusal scenarios still pass.
The actual S3 client performs catalog/archive I/O and conditional writes.

All fourteen regression/check commands pass: 193 distinct Rust cases across six
executables, locked all-target Clippy for Cellar/logic/LTX, format/layout,
dependency/docs/preflight/plan checks, 37 Python tests and system-Python hashing.
The records under `.cellar/cell-groups-evidence/CG-06-s3-commands/` bind
2564 source hashes, frozen inputs, copied executables, exact selections,
output hashes and predecessors. Independent source/hash/selection/credential
exclusion audit passes. The source-bound Clippy large-enum failure is retained;
boxing the private proof bundle fixes it without public variant or contract changes.
No original required case/oracle or pre-existing quota was weakened. Four original
CG-06 cases and its full task gate remain pending; this is partial evidence.

Reflection: integrate S3 enrollment, receipt lookup and remote output contracts
using this retained owner, then explicit unarchived-byte/disk admission. Qualify
multi-head retention, controller process-kill restart and physical-read shutdown
cuts, physical shared-engine Raft log purge and fenced recovery into fresh group/
cell incarnations. Keep the complete old-voter lineage inventory, including later
joins. CG-08 cache/effect/result lifetime work precedes CG-07 public SDK/fleet
cutover. Native voters share one host and RustFS one local VM; no independent
failure-domain, load, complete disaster recovery or full hard cutover is claimed.

## CG-06 local RustFS archive and response-boundary qualification

Source: `d739bf795f7e58c1c27b2c6c496f9f57355e3c02`. Production runtime bytes are unchanged from `7914177d`.
CG-06 remains IN_PROGRESS. The local qualification backend is real RustFS, as
requested. [The runner](../../../../tools/multi-raft/cell_groups.py) verifies the
owned backend's image, running state, endpoint and actual cgroup limits, then
creates a fresh bucket and isolated scenario/image prefixes. Explicit S3 clients
use ETag conditional writes without a memory or ambient-credential fallback.

All three exact ignored cases pass in both materialized and extent modes:
controller publication while quorum advances, lost publication response/readback
with exact-intent reconciliation and joined close, and refusal of unproved S3
command/receipt/enrollment responses, including a namespace S3 minimum. Catalog
setup and archive recovery use RustFS. Lost-response injection follows a real
successful conditional write. The refusal case does not qualify successful S3
acknowledgements; shared dispatch still refuses those until exact operation proofs
are connected. Existing assertions and required CG-06 cases remain unchanged.

Seven signed S3 protocol operations and all ten regression commands pass, including
183 native execution/archive/controller/snapshot cases, all-target Cellar Clippy,
format/layout, dependency/docs/preflight/plan checks and 37 Python tests. System
Python 3.9 evidence hashing passes. The record under
`.cellar/cell-groups-evidence/CG-06-rustfs-controller/` binds 2561
source hashes, copied executables, exact selections and output hashes, plus prior
qualification and retained setup/runner/Clippy failures. Independent source/hash,
selection and credential-exclusion audit passes. No credentials enter evidence.

The initial health check reached an unrelated VM's port forward because Docker
allocated its port inside the guest. The helper now selects a free host port
explicitly; signed requests then verify the credential-bound service. The access
key uses SDK CI's 20-character profile. Source-bound corrections retain the system
Python hashing failure and the target-specific shared-fixture dead-code failure.

Reflection: extend this executable RustFS path with positive S3 command, receipt
and enrollment cases while adding exact cell-version/result/current-route proof
ownership. Preserve captured identity, original deadlines and callback-once
behavior. Then bound unarchived-byte/disk admission, physically reclaim Raft files
and qualify fenced fresh-incarnation serving. These host-native voters and a
single local RustFS VM do not qualify container node limits, load, process-kill
restart, independent hosts or disaster recovery. Four original CG-06 cases and
its full gate remain pending; CG-08 lifetimes precede CG-07 public cutover.

## CG-06 controller-owned archive publication

Source: `7914177d6f17e3f6d647a39dc496122157283591`. CG-06 remains IN_PROGRESS.
The existing node controller now owns one fair archive lane, using a deduplicated
dirty-group queue and its existing timer. Clean groups need no roster scan. Apply
and checkpoint-install progress coalesce while object-store work runs outside the
SQL lane. Registration opens only the namespace-bound expected registry, with a
private verifier store and node-shared source/I/O/upload/replay admission.

An exclusively locked journal binds the node engine creation and archive identity.
It fsyncs canonical reservation/publication intents before remote mutation, using
an atomic file replacement plus directory sync/readback. Missing established
identity/state refuses rather than recreating metadata. Attempts reload selected
state after uncertain local writes. Restart/retry reconciles an exact publication;
if absent, it abort-fences the ticket and reconciles again before reallocation.
A new head and predecessor retirement are journaled before the old hold is released.
Published markers remain advisory metadata; physical root/replay admission does
not escape the job or remain reserved for every registered group.

The sealed archive execution capability accepts the controller's captured domain
token without ownership of domain shutdown. Scoped blocking-work accounting spans
abandoned async waiters; close joins logical jobs and their physical closures,
then closes private stores and releases journal locks before runtime/SQL teardown.
Typed checkpoint `NotReady` and admission overload retry after progress; corrupt
metadata and journal loss stop archival. They do not become S3 acknowledgements.

Both image modes qualify held publication while the real quorum continues, lost
publication response plus failed readback, persisted exact-intent reconciliation,
cancelled registration response, joined close, a single current remote hold and
independent recovery of every cell afterward. The first uncertain-publication
scenario found a lost idle-tail wake: recovery of M could clear dirty progress for
already-applied N. Completion now observes both runtime applied state and consumed
apply events, and the unchanged regression proves N archives without another request.
Unit cases qualify actual journal reopen/missing-state refusal and physical drain
after dropping a blocking waiter. This does not qualify a process-kill controller
restart or a bounded S3 outage ledger.

All 40 gate commands pass: nineteen exact archive cases, two exact controller cases,
two exact lifecycle/journal unit cases, 157 execution cases, five snapshot cases and
four LTX generation cases (189 distinct Rust cases across six copied executables).
Locked all-target build/Clippy for Cellar/logic/LTX, formatting/layout, dependencies,
docs, preflight, plan validation and all 37 Python tests pass. The partial record at
`.cellar/cell-groups-evidence/CG-06-controller-archive/qualification/checkpoint.json`
binds 344 source hashes, frozen plan inputs, hashed logs/binaries and the preceding
catalog-cut checkpoint. Independent source/artifact/exact-selection verification
passes. Source snapshots retain the initial error-wrapper compile failure, native
idle-tail regression, prospective-root refusal regression and Clippy reply-type
and duplicate-module failures. Prospective/stale public roots still refuse as
invalid; only trusted owner-current capture can return typed retryable NotReady. No acceptance case, assertion,
oracle or existing quota was weakened. This is not full CG-06 task evidence.

Reflection and next priorities: connect pending S3 outputs to retained independent
coverage proofs, verifying exact command/enrollment/receipt identity, cell version,
result and current routing authority. Preserve captured inputs and original deadlines;
never rerun callbacks. Add explicit unarchived-byte/disk admission before claiming
bounded Quorum progress through S3 outages. Then qualify local shared-engine Raft
file purge and fenced recovery under fresh group/cell incarnations. The source fence
inventory must cover every possible old voter lineage, including later joins.
Automatic remote/private-cache maintenance and controller process-kill restart still
need qualification. The controller currently uploads complete coalesced cuts;
incremental tail scheduling and performance require the measured-load gates.

CG-08 effects/cache lifetime work precedes CG-07 public SDK/fleet cutover, followed
by topology, placement, hibernation, managed recovery, RustFS/load/fault qualification
and final legacy removal. Shared dispatch still refuses S3 durability. The other
four original CG-06 cases and its full task gate remain pending. These scenarios
use the actual in-memory object-store adapter; no real RustFS, Docker fence,
fresh-quorum serving, load or full hard cutover is claimed. Transfer ignored evidence
and its predecessors for audit.

## CG-06 namespace archive identity and stable cut capture

Source: `df0ba1f67c0253a57127a215540f08bb13135279`. CG-06 remains IN_PROGRESS.
The namespace catalog now
owns the expected archive registry identity and its object-store/data location.
A one-use initializer binds the namespace root and exact group birth route before
registry creation; persist its canonical bytes before mutation. A fresh catalog
owner can reopen only that immutable identity. Competing initializers, missing
bindings and missing counters refuse rather than replacing or repairing state.
Interrupted initialization remains fail-closed and requires authority recovery.

`CellGroupRuntime::capture_checkpoint` performs bounded metadata capture on the
existing worker, using its owner and node checkpoint pool. Physical I/O remains
outside that worker. A held source consumes its quota, permits subsequent quorum
writes, and refunds on final release. New capture refuses after joined shutdown.
`upload_cut` retains the captured root/configuration/permanent membership before
upload starts and writes a zero-entry tail for that same cut. It never recaptures
a moving source after remote I/O. A held B upload allows A to commit while the old
cut still uploads, independently verifies and publishes in both image forms.
The same single-source checkpoint pool can admit the later independent receiver.

The all-three-voter erasure scenario now also deletes every local archive intent
journal and drops the uploader/registry owners. It reconstructs the expected
registry identity through a fresh catalog owner from remote namespace metadata
before physically recovering the exact cut. No local intent journal or voter
metadata supplies that identity. This remains data-only recovery with externally
retained untrusted checkpoint/tail locators, without a fence or replacement voters.

All 32 gate commands pass: nineteen exact archive cases, 157 execution cases, five
snapshot cases and four LTX generation regressions (185 distinct Rust cases across
four executables), all-target build/Clippy for Cellar/logic/LTX, formatting/layout,
dependency isolation, docs, preflight, plan validation and 37 Python tests.
The partial record at
`.cellar/cell-groups-evidence/CG-06-catalog-cut/qualification/checkpoint.json`
binds 336 source hashes, four copied executables, frozen plan inputs, hashed
outputs and the preceding protected-publication checkpoint. Independent source,
artifact and exact-selection verification passes. No original required case,
oracle or pre-existing quota was weakened. The first final Python run caught the
unrefreshed catalog source anchor: only reexports changed, moving the unchanged
NamespaceConfig/BirthProposal excerpts by one line. That failed source-bound output
and its old baseline are retained under `plan-baseline-failure/`. After reviewing
and pinning all anchors to the qualified source, docs, preflight, plan validation
and all 37 Python tests pass. Rust source stayed fixed; its passing checks were
retained. This record is not full task evidence.

Reflection and next priorities: compose controller-owned publication with durable
journaling and exact unknown-outcome reconciliation, joined cancellation/shutdown,
verified-result lifetime ownership and fair bounded retry. Then implement actual
S3 command/enrollment/receipt output settlement against exact cell versions/results
and current routing authority, preserving captured inputs without callback replay.
Add explicit unarchived-byte/disk admission so bounded Quorum progress does not
require S3. Follow with physical local shared-engine Raft file purge and fenced
unmanaged recovery under fresh group/cell incarnations. The permanent fence inventory
must cover every possible old voting lineage, including joins after the archived cut.

CG-08 effect/cache lifetime composition precedes CG-07 public SDK/fleet cutover;
then topology, placement, hibernation, managed recovery, RustFS/load/fault qualification
and final legacy removal remain. Shared dispatch still refuses S3 durability. The
other four original CG-06 acceptance cases and full task gate remain pending.
These native scenarios use the actual in-memory object-store adapter. No real
RustFS, Docker fence, fresh-quorum serving, measured load or full DR is claimed.
Preserve and transfer ignored evidence and its predecessors for audit.

## CG-06 protected publication and remote object collection

Source: `873484774c1ad5fb261257e4890291f8ad956f6e`. CG-06 remains IN_PROGRESS.
A complete immutable checkpoint/tail can now be published with a persistent remote hold, using a sealed
committed-source witness and independent physical replay. This is historical
complete-cut coverage; command S3 settlement and local Raft file purge remain
separate unfinished boundaries.

Immutable keys now contain a registry ID and never-reused allocation generation.
Marker 1 makes a hard cutover, with no legacy reader or deletion of old namespaces.
Pending generations retain their exact selected parent, and uploads reuse only
objects in that protected closure. Fresh creation is a one-use capability, with
its intent durably recorded before mutation. Opening requires the owner's exact
persisted creation identity. Missing/corrupt established state and uncertain
identity creation without complete state refuse; neither resets counters.

Tail capture retains a typed committed-source witness before upload. New source
writes may advance while B is delayed, and the older sealed cut can still publish
after every dependency verifies. Publication atomically consumes the reservation,
selects a non-regressing descriptor and creates an explicit remote hold. An exact
persisted intent reconciles a lost response/readback cancellation without another
allocation or publication, including after the source engines close. Serialized
locators/intents cannot create source authority. Retired/aborted intents never
resurrect; reader holds and pending parents remain independent of newer heads.

Collection fully expands and hashes every protected closure before any DELETE.
It excludes marked keys, complete pending generations and newer generations.
Actual remote deletions preserve current recovered data; an old-generation DELETE
held while identical bytes are reuploaded and published cannot erase the new copy.
Corrupt B blocks both new coverage and complete collection. A physically valid
old checkpoint outside the new upload's protected parent refuses publication.
Compact marks/candidates and source witnesses have explicit node admission.
A two-owner test retains a witness through clones, refuses another capture while
quorum writes continue, and refunds only after the final witness drops.

All 30 commands pass: seventeen exact archive cases, 156 execution cases, five
snapshot cases and four LTX generation regressions (182 distinct Rust cases across
four executables), all-target build/Clippy for Cellar/logic/LTX, formatting/layout,
dependency isolation, docs, preflight, plan validation and 37 Python tests.
The partial record at
`.cellar/cell-groups-evidence/CG-06-protected-head/qualification/checkpoint.json`
binds 334 source hashes, four copied executables, frozen inputs, hashed outputs
and the preceding independent-verifier record. Independent source/artifact/exact-
selection verification passes. The first qualification build at `a5d506a6` caught one
missed unboxing branch after the Clippy enum-layout fix; its source-bound failed
output is retained under `build-failure/`. The correction and full gate use the
source above. No required case/oracle or pre-existing quota was weakened.

Reflection and next priorities: wire protected publication into the node controller
and persist the expected registry identity in authoritative namespace/owner state.
Then settle actual S3 command/enrollment/receipt outputs against exact cell versions,
results and current routing authority; preserve captured identities/inputs and
never rerun callbacks. Replace the Quorum-only fingerprint/guards only when those
proofs exist. Add explicit unarchived-byte/disk admission while Quorum can continue
without S3. Follow with local shared-engine file purge and fenced unmanaged seeding
under fresh group/cell incarnations. Persist source birth/provenance and fence all
possible old voting lineages, including joins after the selected archived cut.

CG-08 still owns controller/cache lifetime composition and private-cache maintenance;
retain verified result admission alongside derived raw manifest roots. Then CG-07
cuts public SDK/fleet routing over, followed by topology, placement, hibernation,
managed recovery, RustFS/load/domain qualification and final legacy removal.
The native fixture uses the actual in-memory object-store adapter; no real RustFS,
Docker fence, fresh-incarnation serving or performance claim follows. The other
four original CG-06 cases and full task gate remain pending. Shared dispatch still
refuses S3 durability. Transfer the ignored evidence and its predecessors for audit.

## CG-06 archive verification without voter metadata

Source: `14955f2dc2b01a6f6ab6b75b0122c812e5de6765`. CG-06 remains IN_PROGRESS.
The archive verifier owns dedicated physical stores and needs no GroupLog, bootstrap/member claim,
replacement genesis or surviving voter metadata. Its typed checkpoint is distinct
from the engine-bound snapshot output. Shared physical decoding preserves the
live receiver's selected-base, membership, owner and installation checks; private
archive roots have a freshly minted owner token that live engines refuse.

Both image forms recover the exact checkpoint/tail cut, all four cells including
empty enrollment, and permanent membership after closing all three source engines,
dropping their physical owners and deleting every voter log/page/image directory
and source extent directory. Destination stores are opened only after erasure.
A separate native scenario holds a remote dependency while another execution
domain advances the healthy source quorum, then verifies newer and older archive
cuts independently. Exact-cut engine publication and source capture refuse the
private archive root; engine-selected state stays unchanged. Closed stores refuse
both outstanding and new receives. New private materialized replay results release
retention after proof pins close, and complete remote recovery succeeds after
actual private-image eviction. Existing host retention is preserved.

All 24 commands pass: eleven exact archive cases, 156 execution cases, five snapshot
cases and four LTX generation regressions (176 distinct Rust cases across four
executables), all-target build/Clippy for Cellar/logic/LTX, formatting/layout,
dependency isolation, docs, CG-06 preflight, plan validation and 37 Python tests.
The partial record at
`.cellar/cell-groups-evidence/CG-06-isolated-verifier/qualification/checkpoint.json`
binds 326 source hashes, copied executables, frozen plan inputs and command
outputs. Independent source/artifact/exact-selection verification passes. Transfer
this ignored evidence with its committed-tail predecessor to reproduce the audit.
No required task case, oracle, quota or readiness assertion was weakened.

Reflection and next priorities: data integrity alone cannot authorize S3 settlement
or truncation. Implement conditionally published, GC-protected coverage of the
exact complete committed cut, then bounded outage admission/S3 output settlement,
archive-aware shared-engine file purge and fenced unmanaged recovery into fresh
group/cell incarnations. Preserve source birth/provenance and an authoritative
inventory of all possible old voting lineages, including voters joined after the
selected archive cut; fencing only selected-cut members is insufficient. Follow
with CG-08 effects/cache lifetimes and CG-07 public SDK/fleet cutover, then topology,
placement, hibernation, managed recovery and final legacy removal. Controller
composition must retain the verified archive result alongside derived manifest
roots: a raw `PreparedGroupManifest` clone does not carry its parent archive quota.
Qualify that ownership propagation in CG-08 before managed integration.

The erasure drill uses real local Raft Engine/SQLite/LTX and the actual in-memory
object-store adapter. It does not qualify RustFS, Docker fencing, a new serving
incarnation, process/machine failure isolation, measured load or the production
hard cutover. The other four original CG-06 acceptance cases and full task gate
remain pending. Shared dispatch still refuses S3 durability. The preceding
committed-tail source/evidence is preserved below.

## CG-06 committed-tail replay and extension after compaction

Partial source checkpoint: `7a29c2a9bff5c2daddf37f53c50bc5d575e0b069`.
CG-06 remains IN_PROGRESS. A complete immutable tail now binds its original group
checkpoint, every contiguous entry and the final root/configuration/permanent
membership. Capture copies only the exact committed/applied suffix under the
engine read lock; encoding and object-store work happen afterward. Independent
verification recovers the checkpoint and replays actual cell-addressed LTX,
enrollment, no-op, joint-membership and leave entries without business callbacks,
synthetic cells or engine installation. Final roots, images and membership must
verify against the advertised complete cut.

`extend_tail` reuses existing entry objects and captures only the newer local
suffix. Both image forms publish a real local checkpoint and assert the old
prefix is compacted before extension. Starting again from the original local
cut refuses; extension succeeds with exactly two PUTs for the new entry and
replacement index. The old tail index is deleted before independent recovery,
proving that the flattened replacement directly names its complete closure.
This is logical prefix compaction, not qualification of physical log-file purge.

Live apply retains its original ticket/commit/owner checks around shared physical
transition code. The new membership-root preparation API also lets the lower-level
fixture handle real configuration entries. Materialized archive replay recomputes
incoming LTX in admitted scratch when its destination is cached, then independently
verifies/syncs that destination. A warm seal cannot substitute for the supplied
delta. Extent replay and every final closure retain their independent checks.
Replay admission and pins stay charged with the result and through cancelled
physical work. A materialized case holds the exact warm result-image read, drops
the caller, commits another cell on the healthy quorum, and proves domain shutdown
waits for release before every admission charge refunds.

All 22 commands pass at the committed source: nine archive cases selected
individually, 156 execution cases, five snapshot cases and four generation
regressions, plus all-target build/Clippy, formatting/layout, dependency isolation,
docs, source preflight, plan validation and 37 Python tests. The 174 distinct Rust
cases run across four executables. The retained record
`.cellar/cell-groups-evidence/CG-06-committed-tail/qualification/checkpoint.json`
binds 323 source hashes, four binaries, frozen plan inputs, hashed outputs, both
failed fixture stages and the preceding checkpoint. Independent source/artifact/
exact-selection verification passes. The original
`archive_tails_cover_enrollment_and_controls` requirement passes with four real
enrollments, later independent mutations, explicit joint demotion/leave and a
real transferred-leader term no-op. The other four original CG-06 cases remain
pending; no oracle, resource limit, readiness assertion, exact entry count or
timeout was relaxed.

The retained failures first show a fixture decoding configuration bytes as cell
commands, then a direct campaign refused by a healthy leader lease. The fixture
now shares canonical membership-root preparation and uses production leadership
transfer. Seven other cases already passed before that final setup correction.
Warm wrong-LTX tests positively observe the ordinary cached `Exists` shortcut,
then require archive replay to reject a correctly hashed but wrong SQLite delta.
Missing/corrupt base or entry bytes, omitted/noncontiguous ranges, forged final roots
and damaged warm destinations also refuse without changing selected engine roots.

**Reflection and next step:** verification still takes an existing destination
engine/applier. Build an isolated data-verification store independent of a serving
engine's advancing cut and surviving voter disks, then implement published,
GC-protected coverage and S3 settlement with bounded outage admission. Continue
archive-aware physical log reclamation and fenced fresh-incarnation seeding.
Shared dispatch still refuses S3 durability. Real RustFS/load and zero-quorum
host/disk-loss qualification, managed recovery, public SDK/fleet cutover and final
legacy removal remain open; this native data-recovery gate does not qualify them.

## CG-06 independently recovered object-store checkpoints

Partial source checkpoint: `650fc63892eab59e6fd5023fa90d5f361a8c7302`.
CG-06 is IN_PROGRESS. The group-owned archive now stores complete checkpoints
without synthetic cell identities. One node pool bounds upload/download jobs
and retained metadata across all group handles. One-chunk channels connect
domain-owned physical sources/receivers to object-store I/O; source and receiver
pools separately retain closure and scratch admission through cancellation.

An immutable bounded index binds the complete group marker and every ordered,
cell-addressed chunk. Existing objects are GET-verified and reused without PUT;
conditional creates require exact read-back even after an ambiguous PUT response.
Corrupt existing payloads refuse overwrite. Upload returns an untrusted locator.
Independent verification downloads every dependency, runs the physical receiver
and checks its complete marker against the index. Preparation leaves the selected
engine roots unchanged and grants no serving authority.

All fifteen gate commands pass: four new archive cases selected individually,
all five existing snapshot acceptance cases, all-target build/Clippy,
formatting/layout, native dependency isolation, docs, CG-06 source preflight,
plan validation and 37 Python validator tests. Nine distinct Rust cases run across
two executables; ignored snapshot child entry points are exercised by their
process-crash parents. The source-bound record
`.cellar/cell-groups-evidence/CG-06-checkpoint-archive/qualification/checkpoint.json`
binds 317 source hashes, both executables, hashed logs, frozen plan inputs and
the preceding CG-05 checkpoint. An independent hash/source/selection audit passes.

The new cases use real three-voter Raft Engine and SQLite/LTX with both materialized
and extent images, empty enrollment, and the actual in-memory object-store adapter
with instrumented faults. They verify unchanged-cut reuse without repeat PUTs,
recovery after source-engine closure, a missing/corrupt cell despite another
cell's later cut, ambiguous PUT confirmation, shared cross-group admission,
chunk/index bounds, cancellation, complete-GET timeout and malformed/rehashed
index refusal. Omitting a cell or substituting the advertised cut cannot produce
a verified checkpoint. The five original CG-06 required cases remain pending;
these narrower checkpoint tests do not replace them or their oracles.

**Reflection and next step:** the critical gap is a complete checkpoint plus
contiguous committed cell/control tail and a published, GC-protected coverage
owner. Shared dispatch still refuses S3 durability. Only confirmed complete
coverage may release S3 outputs or feed the archive retention/reclamation policy.
Then implement bounded outage admission, actual shared-engine log reclamation
and fenced unmanaged seeding under fresh group/cell incarnations. Current objects
follow bounded source chunks; request coalescing and large-group limits need
qualification before any cost/throughput claim. Follow with CG-08 effects/cache
lifetime and CG-07 public SDK/fleet cutover. Real RustFS/load, managed recovery,
independent domains and final removal of the old architecture remain later gates.

## CG-05 complete native snapshot/rebuild gate

CG-05 is DONE at `e0550acd95a78cdf1852a408620eb054ee505669` after the complete native gate
and a requirement-by-requirement closure audit. All five required cases pass
individually with exact one-test selection. The expanded gate passes 156 shared
execution cases, nineteen storage cases, six budget cases and nineteen legacy runtime
regressions: 205 distinct Rust cases across five executables. All eighteen
commands pass, including all-target build/Clippy, formatting/layout, dependency
isolation, docs, source preflight and 37 plan-validator tests. No required case,
oracle, command, deadline, configured resource limit or profile was weakened.

The record
`.cellar/cell-groups-evidence/CG-05-completion-admission/qualification/checkpoint.json`
binds 337 source hashes, five executables, frozen plan inputs, both budget
reproductions, intermediate trials and the preceding checkpoint. Independent
verification and `check_evidence` pass. Task status and the generated implementation
index now select CG-06 as the next dependency-ready task.

| CG-05 requirement | Direct current native evidence |
| --- | --- |
| One complete same-cut root, membership/configuration and all cells, while later writes continue | `snapshot_contains_every_cell_at_cut`: both image forms, exact selected closure, concurrent cell advances and stale/foreign/corrupt refusal. |
| Verify all receiver dependencies before atomic installation; interrupted state never serves | `partial_snapshot_never_serves`: actual TLS/engine receivers, physical byte verification, missing/corrupt/foreign/budget refusal, revocation/sync cuts, owned seed SIGKILL and fresh authenticated same-disk retry. |
| Whole-group learner, joint membership, handoff and disk replacement preserve permanent claims/quorum | `whole_group_learner_and_joint_change`: complete learner install/catch-up before promotion, explicit/implicit joint changes, leadership handoff, deleted-voter replacement, joint restart and minority refusal. |
| Empty/populated cells and retained checkpoint/current suffix survive physical recovery | `snapshot_empty_cell_and_extent_restart`: Raft-generated TLS catch-up, both representations, independent sequence-1 cells, cold restart and missing-closure refusal. |
| Bounded transfer, live pins, cancellation, joined owners and unrelated-group progress | `snapshot_memory_and_pin_bounds`: 32-cell streaming/scratch pressure, old-reader retention/deletion, shared-store orphan reconciliation, held publication/install shutdown, full SQL apply capacity, eight-slot completion saturation and two concurrent writers through one checkpoint slot. |

The final implementation fixes two admission faults. A partial refund used to
wake only the first registered waiter, even when its larger request still could
not fit. Release/shrink now wake registered waiters and retain a stored signal for
late worker registration. Separate item and byte atomics could also expose a
temporary item charge from a byte-rejected attempt. Both dimensions now reserve
and refund in one short mutex scope; notifications run after unlocking. Four new
regressions plus the existing two accounting cases pass. The original two wake
failures and subsequent concurrent-reservation failure retain exact source,
output and executable hashes. The notification-only probe passes the former
cases before demonstrating the second fault; the final Cargo gate validates both
fixes together.

`CellGroupRuntime::usage` exposes advisory lane counters without request admission.
`checkpoint_before_apply` exposes the controller's existing exact-ticket publication
boundary for embedding owners. Eight real fsynced apply completions fill its fixed
admission; a ninth checkpoint retains its source while waiting. Admission refunds
before the actual storage fence, while the source remains charged. Releasing that
fence selects the complete checkpoint; the successor then applies. Both image
forms run directly in the resource acceptance case on explicitly owned, joined
current-thread runtimes.

Two independent writers additionally enroll 32 real cells per group. While one
receiver holds node-zero capture capacity, both groups continue applying. After
release, both have newer checkpoints at the 25-entry cut, before either writer
finishes its remaining seven entries. All three nodes recover the selected current
and checkpoint closures and every independent sequence-1 cell. This is a bounded
native progress trace, not a measured starvation or latency bound for arbitrary
traffic.

**Reflection and next step:** the snapshot/rebuild task's declared native
requirements now have implementation and current-source evidence. The next
critical feature task is CG-06: complete group S3 checkpoint plus contiguous
cell-addressed committed tail, independently verified coverage, safe local log
reclamation and fenced unmanaged seeding under fresh incarnations. CG-08 effects/
cache lifetime and CG-07 SDK/fleet cutover follow. Entry-count policy retains its
idle final tail; idle/time/byte policy and coordinated wake remain future work.
Real RustFS, measured load, remaining crash/deployment schedules, independent
failure domains, managed zero-quorum recovery and removal of the old architecture
remain required in their later task/release gates. CG-06 through CG-16 remain TODO.

## CG-05 checkpoint progress while SQL apply capacity is full

Qualified source: `bad0387cb025fbba62f37aa42ba80b81d75366dd`. The production controller now
scans its bounded pending queue even when every SQL apply slot is occupied.
Checkpoint capture/publication uses independent capacity and can start before
SQL work is deferred. This removes a reproduced dependency between unrelated
groups without increasing SQL concurrency or releasing physical ownership early.

All five required snapshot cases, 152 shared-execution cases and nineteen
Raft-storage cases pass: 176 distinct Rust cases. All sixteen commands pass,
including the complete prescribed CG-05 native gate, all-target build/Clippy,
formatting/layout, dependency isolation, docs, source preflight and 37 plan-validator
tests. The record
`.cellar/cell-groups-evidence/CG-05-checkpoint-apply-scheduling/qualification/checkpoint.json`
binds 335 source hashes, three executables, frozen plan inputs and the prior
checkpoint. Independent hashes and `check_evidence` pass. CG-05 stays IN_PROGRESS.

Two new cases hold actual node-zero image-directory persistence, once with
materialized apply and once with extent apply. The sole SQL apply slot and its
retained completion stay charged. A second group commits a successor, which the
other two voters apply. Node zero publishes that group's preceding complete
checkpoint while preserving its unchanged current root and unapplied suffix.
After physical release both groups apply, independent sequence-1 cells verify,
and controller shutdown joins all owners. The exact resource-bound snapshot case
directly runs both scenarios.

Both cases failed before the production change at the same five-second checkpoint
deadline. A second reproduction first verified the other voters' applied successor;
diagnostics showed one active and one pending apply, zero active checkpoints and
zero checkpoint sources. The worker stopped scanning solely because SQL capacity
was full. Moving that capacity check after checkpoint admission makes both cases
pass. The initial and diagnostic reproductions retain their exact source copies,
outputs and executables; an intermediate diagnostic-format compiler failure is
also retained. No timeout, assertion, resource bound or persistence fence changed.

**Reflection and next step:** checkpoint capture is now independent of occupied
SQL apply capacity. Publication with an exhausted completion lane and capture
fairness under sustained traffic still need direct qualification before closing
CG-05. Then deliver CG-06 complete S3 checkpoint/tail coverage and safe Raft log
reclamation, followed by effects/cache lifetime and SDK/fleet cutover. Real RustFS,
load, independent failure domains, zero-quorum recovery and final legacy removal
remain open. This gate does not combine earlier LTX binaries into its case count.

## CG-05 idle-node orphan reclamation and authenticated cold retry

Qualified source: `5cfa9fb652c32cc75934ef3d0a32e22ffc009f35`. `ReplicaLog::reclaim_cell_storage`
now reconciles the complete physical engine inventory and collects bounded batches
of unselected image/page files. Both materialized and extent images are supported.
The complete prescribed CG-05 command gate passes at this source: all five exact
snapshot cases, all-target build and Clippy, formatting and layout. The expanded
gate also passes 150 shared-execution cases, nineteen Raft-storage cases and
38 relevant LTX cases, plus dependency isolation, docs, source preflight and
37 plan-validator tests. All nineteen command logs pass.

The record
`.cellar/cell-groups-evidence/CG-05-node-orphan-retry/checkpoint.json`
binds 335 source hashes, six copied executables, frozen plan inputs, the prior
checkpoint and intermediate trial outputs. Independent hashes and the plan's
`check_evidence` pass. CG-05 remains IN_PROGRESS; the native gate does not close
the remaining lifecycle/pressure obligations or qualify the production cutover.

Maintenance requires joined runtime and SQL ownership and holds both exclusive
claims through physical completion. It runs on the existing engine worker, where
group creation/publication cannot interleave. It refuses missing, duplicate,
foreign, oversized or busy inventories. Before deletion it verifies every group's
current and retained checkpoint closures, merges image references across shared
stores and retains caller-supplied durable archive/transition roots. The node must
exclusively own those image stores. Existing live reader pins and file/directory
sync barriers still govern deletion and capacity refunds. This is idle-node orphan
maintenance; it supplies no archive coverage, Raft log truncation or serving authority.

Two new shared-inventory cases use two groups with one image store in each form.
They retain both groups' current/checkpoint images, collect an actual orphan with
`max_files=1`, preserve and read an independent old image, then collect it after
its reader releases. A queued request retains engine/SQL claims after its waiter
is aborted, and cancelled engine close waits for physical completion.

Two new retry cases run four additional owned SIGKILL scenarios: partial and synced
dependency writes in both image forms. The original voter disks reopen with fresh
process identities, establish a new term/quorum and commit a newer cell before
retry. The same learner disk collects its unselected transfer debris while its
empty stage still refuses bootstrap. Fresh TLS seed installation supplies the
authenticated local receipt; ordinary routing refuses before that receipt. The
learner then applies a live Raft suffix. A second cold reopen verifies all four
independent sequence-1 cells and both current/checkpoint closures. These cases
retain the voter disks; they do not exercise zero-quorum S3 recovery.

The supplemental `generation` library filter selected zero tests. Its output is
retained as non-qualifying diagnostic evidence; the actual `private_clone` and
`extent_store` integration targets supply the relevant image-store regressions.
Compiler failures during fixture wiring and a correctly refused pre-seed route
are retained in the intermediate logs. Context probes identified the premature
ordinary route installation. The fixtures now assert that refusal and install the
route only after the authenticated receipt; source checkpoint registration follows
the fresh route. No production transport/fencing check or assertion was relaxed.

**Reflection and next step:** same-disk orphan cleanup and authenticated retry are
implemented at the idle-node API boundary. Managed orchestration must eventually
invoke this boundary with every durable reference. Finish sustained-write capture
fairness and remaining controller/completion-budget/root-pressure qualification
before closing CG-05. Then implement CG-06 complete S3 checkpoint plus contiguous
tail coverage and safe local Raft log reclamation. CG-08 effects/cache lifetime and
CG-07 SDK/fleet cutover follow; topology, placement, hibernation, fenced recovery
under fresh incarnations, real RustFS/load and final legacy removal remain open.

## CG-05 controller publication ownership through cancelled shutdown

Qualified source: `370dcf3465b3161c9e69e8b1f398bc9d49450c0c`. Both new controller publication tests pass
in the complete native execution suite: 146 cases, plus nineteen native
Raft-storage cases and the updated exact `snapshot_memory_and_pin_bounds` case.
Ten scoped commands pass, including Clippy for all three integration targets,
formatting, layout, dependency isolation, docs, source preflight and 37 plan
validator tests. The other four snapshot acceptance cases and all-target checks
were not rerun at this source; their complete earlier gate remains below.

The record
`.cellar/cell-groups-evidence/CG-05-controller-checkpoint-shutdown-final/checkpoint.json`
binds 332 source hashes, three copied executables, frozen plan inputs and
intermediate trial outputs. Independent source/output/executable hashes pass.
This partial record is not a combined full task gate and keeps CG-05 IN_PROGRESS.
Production runtime behavior is unchanged. The first scoped gate passed all
selected Rust cases but failed Clippy on an unused helper in the shared legacy
storage target. That method was replaced with fixture-local predicate setup.
The failed source, outputs and three executables remain hash-bound in
`previous_attempt`; no assertions or fault scenarios were removed.

The filesystem fault now supports an opt-in sync predicate. Earlier ordinary
append/commit syncs pass. Once the controller owns its checkpoint lifecycle slot
and captured source, the next actual log sync holds. The test waits for all voters
to persist the other group's election before arming this cut; it does not guess a
sync count. Both materialized and extent cases check the following directly:

- Source count, retained bytes and the one controller slot stay charged. Another
  receiver refuses with `Busy`; the prior root and applied position stay selected.
- Four writes to the other group commit and apply on the two healthy voters while
  this node's shared engine is stalled. No checkpoint completion is credited early.
- Cancelled close and wait retain physical ownership. Replacement runtime and SQL
  pool ownership refuse until the real sync returns.
- Release joins controller shutdown, selects the captured complete checkpoint,
  keeps the committed successor unapplied and drains checkpoint admission. A new
  runtime can then acquire the engine. The exact resource-bound acceptance case
  directly composes both scenarios.

This qualifies pending publication through cancelled controller shutdown in both
image forms. It does not close every controller crash, completion-budget, capture
fairness or retained-root pressure schedule. Native process tests do not qualify
independent domains, power loss, real RustFS/load or the SDK/fleet cutover.

**Reflection and next step:** interrupted-seed retry is now the critical recovery
availability gap. The extent inventory deliberately keeps partial known files
charged after reopen, and immutable import verifies existing names instead of
silently overwriting them. Retry needs complete node-reference reconciliation
before orphan collection, preserving all selected current/checkpoint roots and
live pins across groups. Do not add group-local deletion of shared objects or
weaken corruption refusal. Implement and test that reconciliation and a fresh
authenticated retry before closing CG-05 and proceeding to CG-06 archive/reclaim.

## CG-05 exact native snapshot acceptance target

Qualified source: `f076731a2d384bff65914828409c01506438c4a2`. The declared
`native_cell_groups_snapshot` target now contains all five required named cases,
plus two ignored child entry points used by its actual process-kill parents.
Every required case passes separately with `--exact --test-threads=1`; inventory
and exact one-test results pass the plan's evidence validator. The
execution target's 144 regression cases also pass. Fifteen commands pass: all ten
prescribed CG-05 commands, the execution regression suite, dependency isolation,
docs, source preflight and 37 plan-validator tests. The prescribed commands include
all-target checks and Clippy for cellar/cellar-logic, formatting and layout.

The record `.cellar/cell-groups-evidence/CG-05-snapshot-acceptance/checkpoint.json`
binds 332 source hashes, two executables, frozen plan inputs and intermediate
logs. Independent hash verification and `check_evidence` pass. No required case
or command was removed or renamed. This record does not mark the task complete;
remaining lifecycle/availability gaps below keep CG-05 IN_PROGRESS.

The cases directly compose existing real-engine, SQLite/LTX, TLS and owned-crash
fixtures. They do not infer an oracle from another test process's success. The
engine helper extraction preserves its body byte-for-byte after keeping the
execution-only import and restart method in the original target. The process-kill
children retain their exact names and run the same snapshot executable. Fixture
progress messages use ordinary test capture, leaving the validator's required
per-case pass lines unambiguous while retaining failure diagnostics.

| Exact case | Direct native coverage |
| --- | --- |
| `snapshot_contains_every_cell_at_cut` | Both image forms, two concurrent cell advances, exact original closure, stale/foreign cut refusal and corrupt physical bytes. |
| `partial_snapshot_never_serves` | Receiver corruption/partial/foreign/budget failures, exact Raft persistence, forged TLS seed data, revoked source, sync failures and twelve owned seed SIGKILL scenarios. |
| `whole_group_learner_and_joint_change` | Fresh quorum seed authority, deleted-voter replacement, complete learner install and catch-up before promotion, explicit/implicit joint changes, leader handoff, four explicit joint SIGKILL scenarios, sync ownership and minority refusal. |
| `snapshot_empty_cell_and_extent_restart` | Raft-generated TLS catch-up, empty and populated enrollment, retained checkpoint/current suffix cold recovery and missing closure refusal in both forms. |
| `snapshot_memory_and_pin_bounds` | 32-cell transfers, scratch/metadata admission, unique old reader preservation and actual deletion after release, cancellation, joined publication/installation shutdown and controller pool contention. |

The audit strengthened two fixture oracles. Replacement-group and unrelated-group
writes now commit while the receiver is physically held at its install fence;
the learner later obtains those cell changes through the Raft suffix. Cold
checkpoint restart explicitly verifies empty enrollment remains at sequence 1,
transaction 0 and an empty image, while alpha/beta/suffix retain populated images
at sequence 1.

The initial composite membership test overflowed its stack even with boxed
futures. Phase probes located the failure entering seed installation, whose
standalone counterpart passed. A non-inlined future factory releases construction
temporaries before polling nested fixtures; the complete original case then
passes in both image modes. No stack limit, timeout, assertion or fault was
relaxed. Temporary phase probes were removed and their logs retained. Production
runtime behavior is unchanged.

**Reflection and next step:** the exact test target and its prescribed command
gate are delivered. The next concrete test should hold controller checkpoint capture, fill its
lifecycle capacity, then cancel close and verify that physical owners remain
charged until release and shutdown joins them. Follow with staged-orphan cleanup
and authenticated retry on the same disk; safe
refusal of interrupted state alone does not prove availability. Retain explicit
gaps for continuous capture fairness, minority signed receipts and retained-root
pressure. This is native qualification, not real RustFS/load, independent domains
or production SDK/fleet cutover. Complete the remaining CG-05 obligations before
CG-06 archive/reclamation; the full hard cutover remains pending.

## CG-05 interrupted seed dependency writes and process revocation

Qualified source: `6e56f21f2209ce3f02d78ffeeb3150a763a1476e`. All 144 native shared-execution tests
and nineteen native Raft-storage tests pass. Three ignored child fixtures are invoked
by their process-crash parents and are not standalone passes. Eight scoped commands
pass: Clippy for both targets, both complete suites, formatting, layout, dependency
isolation, docs, source preflight and 37 plan-validator tests. The record
`.cellar/cell-groups-evidence/CG-05-seed-transfer-cuts/checkpoint.json` binds
330 source hashes, two executables, frozen plan inputs and intermediate logs.
Independent hash verification passes. Other suites were not rerun at this source.

The authenticated seed crash matrix now runs six cuts in each image form: staged,
partial dependency write, synced dependency, before engine sync, after engine sync
and publication. Four new SIGKILL scenarios use the production TLS source/receiver
and hold either an anonymous materialized-image spool or an extent payload. The
partial cut writes and syncs a prefix; the second cut finishes the file sync but
stops before complete group installation. The child observes nonzero physical
bytes, the unchanged empty stage, no checkpoint and refused bootstrap before
signaling the parent to kill it. The parent joins the killed child, checks the
recorded extent file length against its encoded full length, then cold-opens the
original learner disk with a new process identity. Both new cuts select the empty
stage with no authenticated local receipt and refuse startup.

Two new transfer-revocation cases hold a physical dependency write and replace
the source process. The network waiter returns stale/closed authority while the
physical receiver remains admitted: both checkpoint metadata and transport data
reservations remain charged. Releasing the write lets the receiver observe the
revocation and finish cleanup; admission drains without publishing a root or
checkpoint. Cold recovery independently confirms the empty stage and refused
bootstrap. These cases use an independently authenticated captured peer; the
crash cases use the production source server.

The shared filesystem fixture adds two opt-in dependency faults. Default behavior
and existing fault modes are unchanged. Production runtime code is unchanged.
All original voters and the learner share the killed child; this is not device
power-loss or independent-domain qualification. These cuts do not cover every
wire/manifest boundary, automatic cleanup and fresh-seed retry after a partial
orphan, measured load, RustFS or zero-quorum recovery. Existing pressure, joint
restart and seed installation cases remain green in the complete execution run.

**Reflection and next step:** interrupted dependency writes now have direct native
failure and restart evidence. The next critical step is to audit the five exact
CG-05 acceptance oracles against these fixtures, implement their required test
target and close remaining controller/lifecycle saturation gaps. Keep unverified
orphan cleanup/retry, continuous capture fairness, minority signed receipts and
retained-checkpoint pressure explicit. Broader fault/load requirements remain in
the release matrix. Complete CG-05 before CG-06 verified S3 archive/reclamation;
production SDK/fleet cutover and final legacy removal remain pending.

## CG-05 bounded checkpoint transfer and reader retention

Qualified source: `a2036e5074066eb7585ea82a2eb62331368ccca5`. All 142 native shared-execution tests
pass, including two new pressure cases; two ignored child fixtures are invoked by
their process-crash parents and are not standalone passes. Eight scoped commands
pass: execution-target Clippy, the complete execution suite, formatting, layout,
dependency isolation, docs, source preflight and 37 plan-validator tests. The
record `.cellar/cell-groups-evidence/CG-05-checkpoint-pressure/checkpoint.json`
binds 327 source hashes, one executable, frozen plan inputs and intermediate
logs. Independent hash verification passes. Other integration suites were not
rerun at this source.

Both image forms transfer 32 cells, including empty enrollment, through the
production source and independent receiver. The aggregate payload exceeds the
source reservation; emitted chunks stay within 4093 bytes. Materialized reception
uses one image-sized scratch file across the entire group. A second valid stream
refuses with `ScratchFull` without refunding the first receiver's reservation or
publishing partial state. Extent reception uses no materialized scratch file.
Independent receiver item and byte saturation refuse without leaking admission.

While the transfer is held inside its first image, two original voters commit
changes to two cells and attempt physical reclamation. The captured tree remains
present until the source releases it. The receiver verifies every original cell's
digest and sequence at the captured cut; it never substitutes the newer images.
A completed but unpublished receiver still leaves the prior durable root selected
and retains its admission. Source, receiver and scratch usage return to zero only
after their physical owners finish.

An independent old physical reader remains valid after the source and its tree
pin are gone. The pressure cohort has unique cell contents, so other cells cannot
accidentally preserve that obsolete image. Reclamation keeps it while the reader
exists, and deletes it after the reader releases. This proves actual retention
and release in both materialized and extent stores.

Production runtime behavior is unchanged. This is a direct physical-stream test
with three real engines in one process, not a TLS pressure benchmark or measured
RSS ceiling. The independent read pin is a physical view, not new quorum authority.
It does not qualify continuous-write capture fairness, all controller saturation
paths, independent failure domains, RustFS or zero-quorum recovery. Earlier seed
and explicit joint-consensus process-crash cases also pass in the full suite.

**Reflection and next step:** bounded 32-cell transfer and reader-image retention
now have native evidence. The critical remaining CG-05 work is the missing
mid-stream revocation/crash and controller-lifecycle pressure coverage, followed by
all five exact acceptance cases at one source. Keep minority signed-receipt refusal,
continuous capture fairness and retained-checkpoint lifecycle pressure explicit.
Complete that task gate before CG-06 verified S3 archive/reclamation; production
SDK/fleet cutover and final legacy removal remain pending.

## CG-05 explicit joint-consensus restart and completion

Qualified source: `5f30cc6b28ae01c2e6411507ea11716ef94e6456`. All 16 selected seed/membership
tests pass. Two new parent cases cover four owned process kills: after all replicas
apply explicit joint entry and after all apply joint exit, in both image forms.
Two ignored child fixtures are invoked by their parents and are not standalone
passes. Nine scoped commands pass: Clippy for the execution target, seed tests,
membership tests, formatting, layout, dependency isolation, docs, source preflight
and 37 plan-validator tests. The record
`.cellar/cell-groups-evidence/CG-05-joint-process-restart/checkpoint.json` binds
326 source hashes, one executable, frozen plan inputs and intermediate logs.
Independent hash verification passes; broader predecessor suites remain historical.

The original group has three voters and two independent SQLite cells. Explicit
joint entry demotes member 3: incoming voters are 1/2, outgoing voters remain
1/2/3 and learner-next is 3. The child is killed with SIGKILL after the selected
replicated boundary. Every original replica disk is reopened with a new process
identity, preserving its permanent claims. Before runtime admission, recovery
checks the exact complete root, membership revision and both configuration sets.
After joint exit, the recovered configuration instead contains voters 1/2 and
learner 3, with no outgoing voters or learner-next entries.

Production controllers and TLS transport establish a fresh leader. The recovered
group commits a new cell under its restored configuration, explicitly completes
any pending joint transition, and commits another cell after completion. All three
replicas apply the four-cell state. Fresh linearizable reads verify both original
and both new cell histories with per-cell sequence 1; group log progress does not
change those logical sequences. The demoted member remains non-serving.

The seed and membership suites share owned-child SIGKILL/join and bounded frozen
synthetic-catalog helpers. Seed crash regression cases remain green. Production
runtime behavior was unchanged. All original replicas share the killed child and
all recovered replicas share the parent; the frozen catalog is test input, not a
production recovery backend. These cuts occur after replicated entry/exit, not
at every in-flight configuration fence or automatic-transition crash boundary.
Independent failure domains, device power loss, RustFS/load and zero-quorum
recovery remain unqualified.

**Reflection and next step:** explicit joint membership now survives process loss
and completes with fresh quorum authority. The next critical gap is bounded
large-group transfer and retained-pin pressure, followed by consolidating the
existing evidence into all five exact CG-05 acceptance cases. Mid-stream fault,
minority receipt and controller-pressure requirements remain open. Complete CG-05
before CG-06 verified S3 archive/reclamation; the full hard cutover remains pending.

## CG-05 seed installation across process kills

Qualified source: `b997a48ba686fceb2531cd96ae2734c9bed84fc9`. All 11 selected seed tests pass,
including two new parent tests covering four process-kill cuts in each image form.
One ignored child fixture is invoked explicitly by each parent; it is not counted
as a standalone pass. Eight scoped commands pass: Clippy for the execution target,
seed tests, formatting, layout, dependency isolation, docs, source preflight and
37 plan-validator tests. The source-bound record
`.cellar/cell-groups-evidence/CG-05-seed-process-crash/checkpoint.json` binds
324 source hashes, one copied executable, frozen plan inputs and intermediate
logs. Independent hash verification passes. Broader predecessor suites remain
historical results, not additional current-source passes.

Each owned child starts a real three-voter shared group with two SQLite cells,
commits a fresh learner and transfers its seed using the production TLS sender and
receiver. The parent waits for a complete reached-cut signal and kills only that
child, checks its Unix exit signal is SIGKILL, and joins it. Cuts occur after durable
staging, before the destination engine sync, after that sync and after publication.
The child and parent execute the same test binary, retained with this checkpoint.

Cold recovery opens the original learner disk with a new process identity and
verifies the selected root plus physical closure. The staged-only cut has no
checkpoint or authenticated local receipt and refuses startup. The pre-sync cut
allows either complete old or complete new state; both observed runs retained the
new seed. Post-sync and publication cuts require the complete seed, both cells,
retained checkpoint, learner membership and local receipt. Attaching a recovered
learner still refuses a valid enrollment without quorum and cannot advance its root.

The parent restores a bounded frozen copy of this fixture's synthetic catalog,
including the permanent destination claim captured before the fault. It never
manufactures a new claim from recovered disk contents. This fixture catalog is
not an object-store durability implementation or S3 disaster-recovery protocol.
All source voters and the destination share the killed child process. The tests
qualify these learner-install crash cuts, not independent voter domains, device
power loss, mid-stream crash cuts, joint-transition restart, RustFS or zero-quorum
recovery. Production runtime behavior was unchanged.

**Reflection and next step:** seed engine-boundary process kills now have direct
native evidence. The next critical gap is restarting and completing joint consensus
from retained disks. Follow with mid-stream cancellation/crash and pressure coverage,
map the existing proof to all five exact CG-05 cases, and complete that gate before
CG-06 verified S3 archive/reclamation. CG-05 remains IN_PROGRESS and the full hard
cutover remains unfinished.

## CG-05 seed persistence revocation and sync outcomes

Qualified source: `3ea90fb5d67d7c66aa842781788c403aa2afc80a`. All 9 selected seed tests pass,
including two new tests covering four persistence cuts in each image form.
Eight scoped commands pass: Clippy for the execution integration target, selected
seed tests, formatting, layout, dependency isolation, docs, source preflight and
37 plan-validator tests. The record
`.cellar/cell-groups-evidence/CG-05-seed-persistence/checkpoint.json` binds
323 source hashes, one copied executable, frozen plan inputs and retained
intermediate logs. Independent hash verification passes. Broader predecessor
results are historical; this checkpoint does not rerun the full shared suites.

A held node-revision fence and the actual engine queue metric establish that the
seed install is queued. Reopening the source engine with a fresh process and
publishing its NodeView through the catalog revokes the admitted grant. The
engine refuses it with WrongIdentity before publication. Both live and cold
recovered destination roots remain the empty stage, with no checkpoint or startup
receipt. The pending install retains physical receive admission until the held
engine work completes.

A separate cut revokes the source after the seed enters the filesystem fence.
The install retains its owners until sync completes and preserves the complete
seed despite late revocation. The receipt cannot appear before the fence.
Cold recovery revalidates the selected and retained roots, both cell images and
learner membership. A recovered learner refuses a valid enrollment without quorum.

Injected failures immediately before and after the underlying sync return typed
I/O errors. The uncertain engine refuses metadata reads, further writes and
bootstrap; joined runtime/storage shutdown reports the failure. Reopening verifies
the complete root and local receipt together. The pre-sync cut permits either
complete old or complete new selection; the post-sync error requires the new
seed. Both observed pre-sync runs recovered the seed, illustrating why failure
cannot imply rollback. This uses an independently authenticated TLS fixture fed
by a real quorum-authorized cut; production receiver/storage behavior was unchanged.

**Reflection and next step:** queued and entered process revocation plus sync-error
reconciliation now have direct coverage in both image forms. The most critical
remaining gap is actual process-kill installation and joint-transition recovery.
Next add child-process cuts around the seed fence and verify complete recovery,
then finish transfer-time revocation, signed minority receipts, large/empty-group
pressure and all five exact CG-05 acceptance cases. No OS process kill, power loss,
managed fleet, shared-group RustFS/load or zero-quorum recovery is qualified here.
CG-05 remains IN_PROGRESS; archive/reclamation and production hard cutover remain
required downstream work.

## CG-05 receiver faults and physical verifier completion

Qualified source: `3db498eacf4bd0621b7a0ecdf11ee68fe2a3ed32`. All 167 selected Rust cases pass across shared
execution, catalog and storage. All eight commands pass: all-target Clippy for
Cellar/core/LTX, those three native suites, formatting/layout, dependency isolation,
docs, source preflight and 37 plan-validator tests. The source-bound record is
`.cellar/cell-groups-evidence/CG-05-seed-receiver-faults/checkpoint.json`, with
322 source hashes, three copied executables, frozen plan inputs and retained
intermediate logs. Independent hash verification passes. The unchanged legacy
snapshot/storage suites were not repeated in this scoped gate; their earlier
results remain in the predecessor record. CG-05 remains IN_PROGRESS.

Adversarial receive tests exposed a production resource-lifetime defect in both
seed and ordinary group-checkpoint transport: a physical worker could reject a
chunk while the network loop waited for the peer's next header. The connection
and network admission survived until timeout. Both loops now observe physical
completion throughout network reads and bounded-channel sends. Failure closes the
exchange promptly. Explicit End, complete closure verification and the real engine
fence remain necessary for installation. Work already admitted to persistence is
still owned until completion. Both original failures are retained in the evidence.

Two new cases use independent TLS test peers in both image modes. Their payload
comes from a real quorum-authorized complete source; the deliberately small fixture
buffers less than 2 MiB of chunk payload and is not a production streaming
benchmark. Ten faulty
replies cover exporter authentication, correlation, nonce, source process, payload
digest, impossible/noncanonical barrier, unknown fields, invalid descriptor and
oversized payload. They cannot create a destination stage. Eleven chunk/end faults
cover wrong binding/sequence/digest, unknown fields, bad physical offset, premature
End, oversized chunk, disconnect after the first part, replay, and malformed End
offset/body. They cannot change the staged root, install a checkpoint or authorize
bootstrap. Physical and wire admission release after every case. A valid transfer
then installs both cells on the same engine and identities.

The fixture captures a later cut and admits its signed descriptor, then closes and
reopens the source engine with a new process generation. Normal catalog/storage
publication produces a newer NodeView. Accepting that view revokes the pending
session; both current and retained destination roots stay unchanged and an old
source hint explicitly returns StalePeer. This is revocation before install begins,
not a process-kill or revocation during queued/entered engine persistence. The
ordinary checkpoint protocol also has a held-open malformed-offset regression.

**Reflection and next step:** seed reply/chunk refusal and pre-install process
revocation now have direct native coverage, and verifier failure no longer retains
network admission until timeout. Next qualify revocation during transfer/queued
persistence, sync-error/process-kill installation outcomes and joint-transition
restart. Signed minority receipt refusal, large/empty group pressure, retained pins,
sustained capture fairness and all five exact CG-05 cases remain required. No
managed fleet, independent-process, shared-group RustFS/load/power-loss/zero-quorum
qualification is claimed. Complete CG-05 before verified S3 archive/reclamation
and the effects/cache plus SDK/fleet cutover tasks.

## CG-05 seed refusal and restarted voter reconnection

Qualified source: `de552f6daabf786c14fe9df97565538ba5d302b4`. All 238 Rust cases and eight gate commands
pass, including all-target Clippy for Cellar/core/LTX, five native suites,
formatting/layout, dependency isolation, docs, source preflight and 37 plan-validator
tests. `.cellar/cell-groups-evidence/CG-05-seed-fault-reconnect/checkpoint.json`
binds 321 source hashes, five copied executables, frozen plan inputs and
retained intermediate logs. Independent hash verification passes. These strengthen
two existing replacement cases; the test count is unchanged. CG-05 remains IN_PROGRESS.

Both image modes now exercise independent wire probes against the production TLS
seed source. Eleven invalid requests cover exporter authentication, stale process,
wrong disk/incarnation/schema, zero and noncanonical nonce, zero and excessive
timeout, unknown fields and an ineligible voter. Five post-reply scenarios cover
wrong Ready sequence, a previous exchange's binding, unknown Ready fields, source
endpoint revocation and receiver disconnect. The probe independently verifies reply
framing, exporter-bound MAC, correlation and digest. Every scenario releases the
source pin and wire admission. A normal complete transfer later succeeds in the
same fixture, so refusal is not inferred from an unusable source.

Production destination sessions also refuse source revocation after the signed
descriptor and expiry while the user retains the session. Neither changes the
staged root, installs a checkpoint or grants joined-member bootstrap. The same
engine and identities then complete installation and replacement.

After cold recovery, the replacement now starts a new runtime, transport and
controller with a new process generation. Explicit physical routes remove retired
voter 3 and advertise the new endpoint. Survivors reconnect over TLS, the replacement
regains quorum and commits another cell. A linearizable read succeeds. Isolating
that group then leaves a deliberately stale ready flag: a proposal is admitted,
but cannot advance applied state or the selected root, and a fresh linearizable
read expires. After check-quorum stepdown, new proposals refuse. Admission remains
distinct from commitment. The fixture uses no event pumping after controller start.

Two intermediate failures were fixture mistakes, retained in the evidence: the
first route still advertised retired voter 3; the second advanced only the restarted
voter's clock while survivor leader leases remained frozen. The final fixture
advances every voter and uses normal caught-up leadership handoff if a survivor
wins first. No production check or safety assertion was weakened.

**Reflection and next step:** native reconnect and minority commit/read refusal are
now covered. Next test malformed/replayed seed replies and chunks at the receiver,
source process-generation revocation during admitted work, and sync failures or
process kills across seed installation and joint configuration. Signed receipt
refusal, large/empty group pressure, retained pins, sustained capture fairness and
the five exact CG-05 acceptance cases remain open. These tests restart runtime
owners in one OS process; they do not qualify managed fleet orchestration,
independent-process failure, power loss, shared-group RustFS/load or zero-quorum
recovery. Complete CG-05 before CG-06 archive/reclamation and CG-08/CG-07 cutover.

## CG-05 authenticated learner installation and replacement

Qualified source: `ab2a308a937f349d3482a825d4b669d042cb03c3`. All 238 selected Rust cases and eight
gate commands pass, including all-target Clippy for Cellar/core/LTX, five native
suites, formatting/layout, dependency isolation, docs, source preflight and 37
plan-validator tests. `.cellar/cell-groups-evidence/CG-05-learner-install-final/checkpoint.json`
binds 320 source hashes, five copied executables, frozen plan inputs and logs.
Transfer this ignored evidence with its predecessors. CG-05 remains IN_PROGRESS.

A dedicated authenticated seed exchange now binds the exact physical identities,
fresh group ReadIndex barrier and canonical complete checkpoint. Destination
staging reconciles permanent node/disk/engine and member claims but cannot serve.
Only a verified complete transfer followed by the actual engine fence installs a
local seed receipt atomically with configuration, membership and both root cuts.
The receipt never travels in snapshots; generic snapshot installation cannot mint
it. Joined-member bootstrap requires this receipt, the unchanged permanent claim
and active membership. Startup still needs physical recovery and fresh quorum.
The internal marker-1 group application record now requires `manifest` and `seed`;
the previous bare-manifest shape refuses. No legacy reader or data deletion is added.

Two new tests use production controllers, real TLS, independent native engines
and SQLite/LTX in both image forms. They delete voter 3's fixture disk before
transfer and prove its old identity cannot recreate storage. An isolated source
cannot create a stage. Staged cancellation retries retain exact identities and
the original deadline; transient source-slot ownership is allowed to finish.
Initial-member bootstrap and generic-snapshot bootstrap refuse the staged learner.
A held engine sync cannot publish the receipt. Cancelling the materialized caller
retains physical owners through sync; `recover_selected` reconciles using the same
page store without bypassing its lock. The new learner installs every prior cell,
catches up a later Raft suffix while another group progresses, joins the voter set
through joint consensus, retires member 3, takes leadership and commits another
cell. Cold reopen verifies current and retained checkpoints, then starts a new
process owner that is correctly not quorum-ready.

The tests also exposed a production joint-leave bug: RawNode's historical hash
iteration order can differ from pure configuration replay. Owner completion now
compares validated canonical configurations, preserving all sets and auto-leave
state. Fixture setup was split to avoid an oversized combined debug future; no
stack limit or safety assertion was relaxed. Intermediate failures and the final
passing runs are retained, and temporary debug probes were removed. The first
broad gate hit eight macOS socket-address allocation errors; all ten remote RPC
cases passed unchanged in a targeted recheck, followed by the complete final gate.
Host samples showed thousands of transient loopback sockets. The failed broad
run and diagnostics are retained; no global network settings were changed.

**Reflection and next step:** the fresh-disk replacement path is implemented and
locally exercised. The immediate remaining CG-05 work is adversarial seed-frame
and process-revocation coverage, sync-error/process-kill and joint-transition crash
cuts, restarted-peer reconnection, minority write/receipt refusal, and the complete
five-case gate including empty-cell and pressure/pin scenarios. Sustained-write
capture fairness and controller pressure cuts remain open. Complete those gates,
then deliver CG-06 verified S3 archive/reclamation, CG-08 effects/caches and CG-07
SDK/fleet cutover. Managed fleet orchestration, shared-group RustFS/load, power-loss
and zero-quorum DR remain unqualified; native disk deletion is not those proofs.

## CG-05 live-quorum learner seed source

Qualified source: `0c3769eccbb845a5943ef60e771c107f1800d9f2`. All 236 selected Rust cases and eight
gate commands pass, including all-target Clippy for Cellar/core/LTX, five native
suites, formatting/layout, dependency isolation, docs, source preflight and 37
plan-validator tests. `.cellar/cell-groups-evidence/CG-05-seed-source/checkpoint.json`
binds 312 source hashes, five copied executables, frozen plan inputs and logs.
Transfer this ignored evidence with its predecessors. CG-05 remains IN_PROGRESS.

A fresh correlated group ReadIndex now authorizes a complete applied seed root,
including a group with no cells. Only a committed new learner with the exact
node/disk binding outside both voter sets qualifies. Pending requests and retained
roots share existing node/group read budgets. No per-cell worker or new serving
mode is introduced. Capture consumes a non-cloneable authority and requires the
exact still-current root; if apply advanced first, a fresh selection is required.
Once captured, ordinary writes may continue while the immutable cut streams.
Membership changes, leadership loss/handoff, shutdown and deadline expiry revoke
the source. Checks before and after sink calls and final completion prevent a
revoked stream from reporting a successful End.

Three new cases use production controllers and real TCP quorum traffic. Both
image forms stream complete cuts into existing receiver verification. Tests cover
uncommitted/wrong/retired member refusal, follower refusal, stale capture, empty
groups, per-group admission exhaustion, independent group progress, leadership
transfer and shutdown. Removing membership while a source blocks in its sink
revokes completion but retains pool admission until physical work exits. Expiry
during an empty group's final header chunk also refuses completion. An isolated
leader still reporting ready cannot issue a fresh seed; another group continues
real writes. Initial and strengthened targeted runs passed and are retained.

**Reflection and next step:** source authorization is now implemented, but no new
destination is admitted by this API. The requested process identity is bound, not
authenticated. The next feature-critical slice is authenticated seed RPC with
process revocation, permanent destination node/disk/engine claims, complete atomic
installation and joined-member runtime attachment. Coordinate capture with the
controller apply boundary or bound fresh-selection retries under sustained writes;
source refusal is safe but does not establish capture fairness. Then demonstrate
real catch-up, promotion and replacement after deleting a voter disk, one-voter-loss tolerance
and minority write refusal. Preserve all five CG-05 cases and remaining receiver,
joint-transition and pressure/crash cuts. CG-06 verified archive/reclamation follows,
then CG-08 effects/caches and CG-07 public SDK/fleet cutover. Shared-group RustFS,
load, power-loss and zero-quorum DR remain unqualified.

## CG-05 durable membership and leadership handoff

Qualified source: `1814b27896c3c21166186fe8fbd1a1a6bdbb721a`. All 233 selected Rust cases and 8
gate commands pass, including all-target Clippy for Cellar/core/LTX, five native
suites, format/layout, dependency isolation, docs, source preflight and 37 validator
tests. `.cellar/cell-groups-evidence/CG-05-group-membership/checkpoint.json` binds
310 source hashes, 5 copied executables, frozen plan inputs and logs.
Transfer this ignored evidence with its predecessors. CG-05 remains IN_PROGRESS.

The owner/runtime now accepts canonical, bounded V2 membership intents bound to
one complete route and observed membership revision. Pure replay computes the
configuration without changing the live RawNode. Ordered apply preserves every
cell leaf while advancing the group position and membership revision. The engine
independently verifies the committed entry and atomically syncs configuration,
permanent member bindings and the complete root. Only its matching completion
changes live Raft progress and advances apply. Explicit and implicit joint changes
use this same boundary. Raft-generated automatic leave starts replication immediately;
it does not wait for a later write or tick.

Fresh IDs first enter as learners with node/disk bindings. The permanent high-water
mark prevents retired-ID reuse. Promotion requires leader-observed replication
through its durable committed cut. Handoff targets another caught-up incoming voter
and revokes captured producer authority while it proceeds. Synced voter checks
also fence demoted/removed leaders before the owner consumes cache publication.
New learner routes may lag committed membership: ordinary and checkpoint sends
return retryable stale-peer failures without closing the node.

Three new tests cover production controllers and real TCP Raft in both physical
image forms, plus held configuration sync and cancelled/joined runtime close.
Lifecycle cases preserve cell identity/version, exercise explicit and automatic
joint changes, stale/foreign/malformed refusal, learner binding, unsafe-promotion
refusal, retired-ID refusal, real leadership transfer, new-leader writes,
self-demotion fencing, unrelated-group writes and automatic checkpoints. Joined
engine metadata reopen retains the exact final configuration and revision. No
fixture event pumping follows controller startup in these two lifecycle cases.
The separate held-sync case proves old root/configuration/applied state remain
selected until sync and that shutdown joins the accepted write.

Initial private Raft accessor and missing-import compile failures are retained.
The lifecycle timeout reproduced delayed automatic joint-leave replication; the
owner now broadcasts the Raft-generated entry through normal Ready persistence.
The same lifecycle assertions pass. Diagnostic logs, including the timeout
backtrace, remain alongside the final committed-source gate. Preflight also caught
the changed storage-engine anchor; review pinned its new validation to the tested
revision. Its initial refusal and passing rerun are retained; no gate was weakened.

**Reflection and next step:** fresh learner 4 is deliberately not started in this
slice. The feature-critical gap is a seed/bootstrap capability tied to committed
learner membership, its permanent node/disk claim and a complete verified root.
Implement actual new-host installation/catch-up/promotion, then replace a voter
after deleting its disk. Metadata reopen alone is not physical recovery or serving
permission. Prove one-voter-loss tolerance and minority refusal and complete all
five declared CG-05 cases. Remaining receiver crash/sync-error and controller
pressure/fairness cuts stay required; they do not replace fresh-member recovery
as the next feature priority. Then deliver CG-06 verified archive/reclamation,
CG-08 effects/caches and CG-07 public SDK/fleet cutover. No shared-group RustFS,
load, power-loss or zero-quorum recovery qualification is claimed by this gate.

## CG-05 automatic checkpoints at controller apply boundaries

Qualified source: `c27dfb416784804b30a38b1a90812c607ed3b00f`. All 230 selected Rust cases and 8
gate commands pass, including all-target Clippy for Cellar/core/LTX, the five
native suites, format/layout, dependency isolation, docs, preflight and 37 validator
tests. `.cellar/cell-groups-evidence/CG-05-controller-checkpoints/checkpoint.json`
binds 308 source hashes, 5 copied executables, frozen plan inputs and logs.
Transfer this ignored evidence with its predecessors. CG-05 remains IN_PROGRESS.

An explicit controller policy selects an applied cut before its successor starts
physical apply. One node slot spans blocking capture, reserved publication admission
and persistence. The controller holds the exact successor ticket, so a hot group's
current root cannot advance during capture. A private completion-lane operation
checks that ticket, engine and exact current root while preserving any later log
suffix. Only the matching persisted `CheckpointPublished` event releases successor
apply; admission/event ordering is handled either way. The existing public manual
publication checks remain unchanged.

The policy uses the transport's configured node pool and owns initial checkpoint
endpoint registrations. Busy pool/admission retries back off until a later apply
boundary, letting apply proceed. Capture joins precede engine-claim release;
accepted publications remain runtime-owned through shutdown. Hard failures retain
tickets and quarantine the group; shared log uncertainty closes the node. Usage
reports pending checkpoints, actual publications and transient retries.

Three new cases use production controllers and real TCP Raft traffic after startup,
with no fixture event pumping. Repeated interleaved writes to two groups compact
successive prefixes in both image formats. A held receiver saturates the leader's
checkpoint pool while both groups continue applying; releasing it permits later
checkpoint publication. Every voter retains the applied suffix and advances its
checkpoint; joined close releases all pool admission. Initial polling borrow-conflict
and duplicate-TCP fixture failures are retained; the same assertions passed after
repair. No assertion was weakened.

**Reflection and next step:** this entry-count policy retains the idle final tail.
The critical next gap is scheduler/persistence pressure qualification: continuous
saturation fairness, exhausted apply/completion budgets, held sync and cancelled
close, checkpoint failure/quarantine capacity, and retained-root slot pressure.
Add deterministic coverage at those boundaries before time/byte-trigger expansion.
Then complete receiver crash/sync-error cuts, whole-group learner/joint membership
and lost-disk replacement, and run all five declared CG-05 acceptance cases. CG-06
archive/reclamation follows; upload completion alone cannot authorize reclaim.
No new shared-group RustFS, load, power-loss or zero-quorum DR qualification is
claimed. Public cutover and legacy removal remain open.

## CG-05 authenticated checkpoint transport

Qualified source: `ceb7ed3149202660855d32df1ec5feed7775e068`. All 227 selected Rust cases pass across shared
execution/catalog/storage and the existing single-cell storage/snapshot suites.
All 8 gate commands pass, including all-target Clippy for Cellar/core/LTX,
formatting/layout, dependency isolation, docs, source preflight and 37
plan-validator tests. The partial record at
`.cellar/cell-groups-evidence/CG-05-snapshot-transport/checkpoint.json` binds
306 source hashes, 5 copied executables, frozen plan inputs and command logs.
Transfer this ignored directory with its predecessors. CG-05 remains IN_PROGRESS.

Dedicated TLS data streams now carry complete checkpoints between physical group
peers. Start and chunk purposes bind the exact history/incarnation, node/disk/process,
member, schema, snapshot envelope, ordered part identity and byte digest to the
TLS exporter and start tag. Bounded headers and <=64 KiB chunks precede allocation;
an authenticated End is mandatory. Extent identities resolve from the receiver's
verified manifest. The exact retained source cut streams on a blocking owner;
receiver verification and sealing run on another. Wire, physical workspace and
scratch reservations stay with their owners through cancellation and shutdown.

Controller registrations retain the applier/route endpoint. Removing/replacing a
route or dropping its registration revokes stream admission. The independently
verified closure enters `step_checkpoint`; the wire reply is admission only.
The existing exact Ready/persistence completion still controls Raft's acknowledgement.
A node explicitly enables the shared checkpoint pool; automatic checkpoint
selection/publication is not yet wired into the controller.

Snapshot status feedback now has a separate 256-slot/1-MiB node budget and reserved
queue space. It coalesces by known peer while retaining admission, then mutates
Raft progress after the current Ready settles. Transport ownership lasts through
feedback admission or shutdown, so ordinary queue contention or an I/O reply timeout
cannot silently discard required snapshot failure feedback. Immediate controller
snapshot refusals also retain feedback; ordinary unreachable hints remain advisory.

Five new real-TLS cases cover actual RawNode-generated catch-up in both image
representations, suffix replay/cold source reopen, a dropped delivery waiter, and a
refused first transfer followed by Raft's own successful retry. The harness does
not replay that failed message. Independent wire probes refuse invalid exporter,
process, body digest, transfer binding, sequence, chunk size and premature End;
revocation during an admitted receive leaves the old root selected. Replacing the
old registration guard preserves its replacement. Disconnect releases receive
admission; a different group commits and applies a real cell write while that
admission is occupied. Materialized source cold recovery still uses the fixture's
generation-ID resolver; extent recovery and destination replay use production adapters.

The initial visibility and fixture trait-import compile diagnostics are retained.
The first all-target Clippy run was deliberately interrupted when review identified
the snapshot-feedback admission gap; only the final committed-source gate counts.
No assertion was weakened. Existing native held-sync/owner tests remain regression
coverage, rather than an independent TLS receiver crash or power-loss qualification.

The next gap recorded at this transport checkpoint was automatic controller-owned
checkpoint selection/publication: coalesce work, coordinate with advancing apply, and bound
capture/publication retries so hot groups can checkpoint without starving unrelated
ones. Then qualify source/receiver pressure, stalled physical I/O and hint saturation,
receiver process-kill/sync-error cuts, and retained-root slot exhaustion. Complete
whole-group learner/joint configuration and lost-disk replacement before the full
CG-05 gate. All five declared acceptance cases remain required. CG-06 archive/reclaim
and later public cutover/legacy removal remain open. No new shared-group RustFS,
load, performance-parity or zero-quorum DR qualification is claimed.

## CG-05 local publication and native Raft snapshot catch-up

Qualified source: `742b1e6d4e7155e16fb1b4eba0cce3b9056944ae`. All 222 selected Rust cases pass across shared
execution/catalog/storage and the existing single-cell storage/snapshot suites.
All 8 gate commands pass, including all-target Clippy for Cellar/core/LTX,
formatting/layout, dependency isolation, docs, source preflight and 37
plan-validator tests. The partial record at
`.cellar/cell-groups-evidence/CG-05-local-checkpoint/checkpoint.json` binds
298 source hashes, 5 copied executables, frozen plan inputs and command logs.
Transfer this ignored directory with its predecessors. CG-05 remains IN_PROGRESS.

An unstreamed source becomes a bounded local publication capability. The owner
requires its exact applied cut, engine and page owner, and runs publication on the
existing persistence lane. The distinct local engine operation preserves hard
state, applied state and every later log entry while compacting the represented
prefix. Mixed, foreign and stale preparations refuse. `CheckpointPublished`
arrives only after the matching engine completion; local compaction does not
advance the controller's applied-state counter. Admission and physical pins remain
owned through queued sync and joined runtime shutdown.

`capture_snapshot` selects the retained complete closure and original metadata
named by the exact snapshot from Raft Storage. Later current-root updates cannot
substitute a newer cut. A concurrent replacement may refuse capture; a captured
source owns the old immutable closure until streaming ends.

Three new cases cover both image formats, a real uncommitted suffix surviving
held publication and later quorum commit, and a partitioned existing voter restored
by an actual RawNode-generated snapshot followed by suffix replay. The leader's
engine is then closed and reopened: the retained snapshot and exact tail must
match, both physical roots are recovered, and the older checkpoint streams again.
Materialized source recovery uses the fixture's explicit generation-ID resolver;
extent recovery and destination suffix replay use production adapters. The initial
materialized cold-recovery failure from using the wrong resolver is retained with
the compile diagnostics. No assertion was weakened. Joined runtime shutdown during
accepted local publication is covered in both representations.

The gap recorded at this local-publication checkpoint was authenticated RPC and its controller-owned
source/publication lifecycle. These cases use native streaming callbacks; they do
not qualify network framing/authentication, transfer cancellation/retries, failed
snapshot reporting, automated checkpoint policy, learners/joint membership or disk
replacement. Receiver process-kill/sync-error/cancelled-waiter cuts and root-slot
pressure also remain open. All five full CG-05 acceptance cases remain required.
There is no new shared-group RustFS, load or zero-quorum DR qualification. CG-06
archival/reclamation and the later public cutover and legacy removal remain open.

## CG-05 Raft snapshot acceptance and persistence

Qualified source: `096cfb132397d9588ba9138b1cf474210fc062f3`. All 146 selected Rust cases pass across shared
execution/catalog/storage. All 8 gate commands pass, including all-target
Clippy for Cellar/core/LTX, formatting/layout, dependency isolation, docs,
source preflight and 37 plan-validator tests. The partial record at
`.cellar/cell-groups-evidence/CG-05-snapshot-owner/checkpoint.json` binds
296 source hashes, 3 copied executables, frozen plan inputs and command logs.
Transfer this ignored directory with its predecessors. CG-05 remains IN_PROGRESS.

The explicit owner/runtime `step_checkpoint` API admits only a complete verified
receiver closure matching the snapshot envelope, route, selected base, engine,
page-store owner and permanent replica membership. Bounds and overflowing cuts
are checked before RawNode mutation. Ordinary peer ingress still rejects snapshot
messages. Admission reports whether Raft selected the snapshot or retained its
matching log; neither result is a persistence receipt or serving authority.

A selected snapshot binds to the exact Ready and existing persistence ticket,
exclusive owner lease and shared engine fence. The old applied view remains until
the matching completion is consumed. Only then does the owner advance to the
verified cut and emit the acknowledgement and `CheckpointInstalled` event.
Controller publication observes that event. The prepared receiver and its budget
remain owned through queued sync and joined runtime shutdown. If Raft already has
the matching log, it persists the commit advance and replays entries without
selecting the received checkpoint.

Five new native cases cover a partitioned existing voter, both image formats,
invalid envelopes and overflowing cuts, held sync with no early acknowledgement,
subsequent real quorum writes, matching-log replay and shutdown during accepted
snapshot persistence. The fixture builds the snapshot envelope from captured
source state and uses the native streaming callback. It does not exercise a
source RawNode-generated snapshot or authenticated snapshot RPC.

The gap recorded at this owner checkpoint was source-side local publication that preserves
the unapplied log suffix, followed by bounded snapshot transport and actual Raft
peer catch-up. The receiver-install write must not be reused as a local leader
checkpoint: it replaces the received state and truncates prior entries. Then
qualify receiver process-kill/sync-error/cancellation and root-slot boundaries,
whole-group learners, joint membership and lost-disk recovery. All five full CG-05
acceptance cases remain required. No new shared-group RustFS, load or zero-quorum
DR qualification is claimed. CG-06 archival/reclamation and the later public
cutover and legacy-removal tasks remain open.

## CG-05 verified receiver and atomic installation

Qualified source: `cb164e025e129330f576450d214cef0e8fe92aa1`. All 226 selected Rust cases pass across shared
execution/catalog/storage, existing single-cell storage/snapshot suites and the
LTX library. All 9 gate commands pass, including all-target Clippy for
Cellar, its pure core and LTX, formatting/layout, dependency isolation, docs,
source preflight and all 37 plan-validator tests. The partial record at
`.cellar/cell-groups-evidence/CG-05-checkpoint-receiver/checkpoint.json` binds
295 source hashes, 6 copied executables, frozen plan inputs and command logs.
Transfer this ignored directory with its predecessors. CG-05 remains IN_PROGRESS.

A receiver now stages ordered bounded parts against the destination engine's
selected base. It checks route/history/cut/membership, every manifest page and
independently sealed image, then verifies and syncs the complete tree. Flat images
use one admitted scratch spool; extent objects use bounded buffers. Repeated flat
transfers reread and sync the existing destination even when its seal is cached.
A regression reproduced acceptance of a corrupt cached destination before this
fix; that failure and the interrupted candidate gate are retained separately.

The prepared checkpoint owns the complete destination closure and receive
reservation. The engine's group snapshot variant atomically selects the snapshot,
configuration, membership and applied root behind the shared durability fence.
Mixed contracts, stale/conflicting bases and incomplete transfers refuse. A
separate retained checkpoint root survives later ordinary apply; recovery must
verify both it and the current applied tree before startup or reclamation.

Three new cases use a lagging existing voter of the real embedded three-voter
fixture. Both image forms and canonical empty enrollment survive byte-for-byte
transfer, a held engine sync, subsequent quorum writes through the production
apply pool, joined shutdown of all prior owners, and cold reopen of the receiver. Before
publication, the selected root stays unchanged and owner startup refuses. The
receiver reservation remains charged after the caller drops its prepared handle
while engine sync is held. A checkpoint-only page is proved absent from the
current tree, then deleted: recovery refuses despite a complete current tree.
Restoring it permits storage startup without renewing quorum authority. Truncated,
noncontiguous, corrupt, foreign-history, oversized and scratch-limited transfers
leave the prior marker selected; existing cached destination corruption also
refuses. Extent receiving does not require a materialized-image scratch spool.

The tests invoke the verified engine installation boundary directly. They do not
qualify Raft Ready snapshot acceptance or snapshot RPC, fresh learners, joint
configuration, disk replacement, receiver process-kill/sync-error/cancelled-waiter
cuts, checkpoint root-slot exhaustion, shared-group RustFS, load or zero-quorum DR.
No full CG-05 acceptance case has been replaced with these narrower cases.

Next work recorded at this receiver checkpoint was to bind its result to the exact Raft snapshot
effect, integrate bounded snapshot transport and completion, and qualify peer
catch-up plus crash/cancellation/root-slot boundaries. Then deliver whole-group
learner/joint membership and lost-disk recovery and run all five declared CG-05
acceptance cases. CG-06 archival reclamation follows that task; public host/SDK
cutover and the later topology, hibernation, DR and legacy-removal gates remain open.

## CG-05 complete-root capture and bounded streaming

Qualified source: `b15da415c9c9c0663274d0aedd520feb37d90d2d`. All 138 selected Rust cases pass across
shared execution, catalog and manifest storage, including the four new checkpoint
source cases. Eight gate commands pass: all-target Clippy, those regression suites,
formatting, layout, dependency isolation, docs, source preflight and 37 plan-validator
tests. `.cellar/cell-groups-evidence/CG-05-checkpoint-source/checkpoint.json` binds
290 source hashes, three copied executables and retained command/diagnostic logs.
The plan inputs are frozen alongside the evidence. Transfer this ignored local
directory with its predecessor evidence; this is a partial CG-05 gate.

The first source-side snapshot slice adds a shared checkpoint pool and a
non-cloneable physical source. Capture checks the exact selected group root,
engine owner, committed/applied cut and membership revision. It copies membership
and configuration under that same cache lock, and retains the complete immutable
closure. The source streams canonical manifest pages, including empty enrollment,
plus materialized image bytes or extent manifests and verified objects. It does
not aggregate group images or consult a later mutable manifest during transfer.

Admission accounts for source slots, retained metadata, bounded stream workspace,
cell count and total transfer size. Borrowed chunks are at most 64 KiB, one extent
buffer is at most 4 MiB, and the source keeps its charge until its physical worker
releases it. Missing/corrupt data, a closed engine or a sink error fails the stream;
failed sources cannot be reused. Streaming grants no receiver or voting authority.

The CG-05 test scope now includes the existing three-voter execution fixture and
`support/cell_group_checkpoint.rs` so this slice exercises real Raft commits,
SQLite/LTX, both image stores and engine-selected roots without duplicating that
fixture. The five full snapshot acceptance cases remain unchanged and outstanding.
The shared fixture additionally handles canonical empty enrollment; existing
execution regression checks cover that helper change.

Gaps recorded at this source checkpoint: build the receiver's bounded staged closure,
verify/sync every dependency, and publish snapshot metadata plus the group root
atomically through the engine. Then connect whole-group transport, learner
catch-up, joint membership and lost-disk replacement. The current header denotes
active groups only; topology/park state must become part of the durable root before
those operations are enabled. CG-05 remains IN_PROGRESS. No RustFS/load, receiver
install, membership-change, independent-process or power-loss qualification is
claimed by this capture slice.

## CG-04 scoped execution gate complete

CG-04 is DONE at `69e41e865c267dff1dec5bce6f2b3d0966be09db`. All 83 declared acceptance cases pass
individually, with the required test inventory, all-target check, formatting,
layout and all-target Clippy commands. The evidence record
`.cellar/cell-groups-evidence/CG-04-qualified/checkpoint.json` validates through the
plan runner and retains 98 distinct command records, 288 source hashes
and 4 copied executables, including the supporting 238-case regression gate.
Transfer it together with the local-provisioning and predecessor evidence.

The completed scope is native shared-group execution: one Raft owner per group
replica, independent cell SQLite histories, bounded node services, durable apply,
cell receipts and reads, authenticated dispatch, response-independent ownership,
group failure isolation and explicit local provisioning/recovery. All acceptance
cases remain intact. Completion does not claim production SDK/fleet cutover or
independent-process, power-loss, shared RustFS or performance qualification.

The critical next task is CG-05: complete-group snapshots, atomic installation,
learner catch-up and membership recovery. It unlocks CG-06 archive/reclamation and
then effect/fleet integration. Automatic production request/voter orchestration
and public SDK binding belong to CG-07; final legacy removal belongs to CG-16.
Keep those dependencies explicit in subsequent execution.

## CG-04 bounded local provisioning

Qualified source: `69e41e865c267dff1dec5bce6f2b3d0966be09db`.

`CellGroupController::configure_provisioning` installs explicit immutable node
filesystem, image-store, manifest, peer and preparation limits. `provision`
accepts an already selected range assignment; it materializes the immutable birth,
checks configured initial members, acquires permanent member/engine claims,
recovers the complete selected root on the owning blocking execution domain and
attaches through the existing runtime and exclusive transport registration. It
neither assigns ranges nor creates a Raft group for an individual cell. Managed
fleet bootstrap still refuses pending CG-07 authorization.

Exact directory-owner/group requests share one preparation. Pending and active
groups share the controller capacity; recovery items, bytes and parallelism have
explicit bounds. Response waiters have separate admission, including queued and
unconsumed replies, so preparation does not recursively consume command permits.
Active stores remain bounded by the configured per-group limits and maximum
roster size; image stores keep their independent quotas. Settings reject invalid
physical identities, duplicate endpoints and unbounded paths before I/O.

Failure before recovery frees its preparation and group reservations. After
recovery, an attachment refusal retains the exact page/root/applier owners and
their preparation budget. A retry revalidates catalog/bootstrap and reuses these
owners rather than reopening an engine-pinned directory. The applier enters the
event roster before attachment can emit events. Registration identity/capacity
refusals preserve healthy groups; closed services or uncertain shared-engine
failures remain node-wide. Shutdown joins admitted catalog/filesystem/attachment
work before transport, SQL and runtime shutdown, even when waiters disconnect.
Success means locally attached; serving still requires fresh current-term quorum.

Five native cases cover both image modes with real engines, filesystem operations
and TLS. They exercise same-group coalescing, dropped waiters, bounds and foreign
bindings, single-command-slot operation, actual enrollment/mutation/exact retry,
pre-recovery catalog failure and post-recovery attachment refusal with page-owner
reuse. A blocked physical directory sync retains engine ownership across cancelled
close. Missing initial peers refuse before genesis; a wrong configured local
endpoint retains preparation while the existing group still executes a command.
All voter engines and manifest stores reopen into empty controllers; removing a
selected page refuses without resetting the marker, restoration permits recovery,
and fresh quorum returns the original receipt and SQLite value. Image-store
handles stay in the process, so this is not independent-process/power-loss evidence.

All 238 selected Rust cases and twelve gate commands pass, including all-target
Clippy, formatting, layout, dependency isolation, docs, source preflight and 37
validator cases. `.cellar/cell-groups-evidence/CG-04-local-provisioning-final/checkpoint.json`
binds 288 source inputs and 4 copied executables. Diagnostic failures and
reruns are retained separately; transfer this ignored directory with predecessors.

The provisioning checkpoint below the full gate is scoped to explicit local
startup. CG-07 integrates automatic request-driven provisioning and assigned-voter
startup, preserving original operation identity,
deadline and captured inputs while using this bounded owner from local and remote
dispatch. Read/receipt requests must never implicitly enroll missing cells.
Configuration/membership lifecycle, managed fleet authorization and public
host/SDK integration remain unfinished. Later CG tasks still own snapshots,
archive/reclamation, effects, topology, hibernation, fenced recovery, load and
legacy removal. Shared-group RustFS and load remain unqualified.

## CG-04 retained group quarantine

Qualified source: `41121a7d08f283db6a83982ebfcba2feced036ca`.

A group-local apply/runtime failure now quarantines the group instead of closing
healthy groups on the node. The controller first refuses application admission,
then revokes runtime execution through reserved control capacity that bypasses a
waiting apply. Captured leader authority becomes invalid. Pending apply tickets,
unresolved command/enrollment producers and the group slot remain retained until
joined shutdown. Quarantine cannot be cleared by re-registration; a late physical
persistence completion cannot restore the revoked execution fence.

`group_failure` reports the original cause; usage distinguishes active, pending
and quarantined groups. Shared log-engine I/O/corruption/closure and lost shared
SQL worker replies still close the node. Unknown group events and controller
ownership violations also retain the fail-closed boundary. Quarantine does not
promise capacity beyond the configured node/group budgets; retained work remains
charged rather than discarded to make another request fit.

Both image modes pass real manifest directory-sync faults while an ambiguous
producer remains owned. A dynamically registered healthy group then completes
controller enrollment, command, exact retry and a linearizable SQLite query on
the same engine, single SQL worker and TLS peers. The failed group's selected root
stays unchanged, captured authority and retries refuse, callback counts exclude
replay, and a third registration cannot reuse its occupied capacity. Joined close
releases the producer. A separate real Raft-engine sync fault still stops both
groups and refuses replacement runtime admission. Existing physical cancellation
and lost-worker-reply tests remain in the gate.

All 233 selected Rust cases and twelve gate commands pass, including all-target
Clippy, formatting, layout, dependency isolation, docs, source preflight and 37
validator cases. `.cellar/cell-groups-evidence/CG-04-group-quarantine-final/checkpoint.json`
binds 283 source inputs and 4 copied executables. Initial fixture compilation
errors and diagnostic reruns are retained with this ignored local evidence;
transfer it with the predecessor checkpoints.

CG-04 remains IN_PROGRESS. The next critical gap is bounded request-driven group
provisioning: catalog selection/birth and physical root preparation must transfer
into the existing registration owner, coalesced by physical group rather than
cell. Configuration lifecycle and public host/SDK integration remain unfinished.
Later CG tasks still own snapshots, archive/reclamation, effects, topology,
hibernation, fenced recovery, load qualification and removal of the legacy host.
This checkpoint is not independent-process/power-loss, RustFS or load qualification.

## CG-04 dynamic controller registration

Qualified source: `4326421a94c4f9a257bf6ceb8e0f7e518eacf3de`.

`CellGroupController::register` now adds a group without replacing the controller
or creating per-cell workers. Controllers may start empty. Pending and active
groups share a capacity bound; queued attachment and unconsumed replies retain
the existing item/byte budget. Command, enrollment and receipt dispatch consult
the live roster under short locks rather than copying it for each invocation.

Admission binds the bootstrap permit, selected root, applier and physical route
to the same engine/history/schema. The applier must own the exact manifest page
store that verified the root. Its event registration precedes polling attachment,
so early consensus and apply events remain owned. Runtime attachment revalidates
the catalog permit before active membership is published. Registration alone
never grants application authority; current-term quorum remains required.

The transport registration refuses adoption of an existing route. Its cleanup
compares exact route ownership so it cannot erase a replacement. A disconnected
waiter cannot abandon attachment or release its group slot. Definite failures
release group capacity before responding. Shutdown joins entered catalog/runtime
attachment before closing physical dependencies. Unexpected competing runtime
owners or runtime-wide closure keep the existing conservative node shutdown.

Four new native cases run both image modes over real engines and TLS. Held
catalog reads leave an already active group serving. Duplicate and pending/active
capacity checks, foreign engines/roots/page owners, failed catalog retry and
cancelled close exercise ownership boundaries. Recovery joins all controllers,
closes and reopens all voter engines under a new process generation, reopens page
stores and registers selected roots into empty controllers. Fresh quorum returns
the prior exact receipt without another SQL mutation. Image-store handles remain
in the same process; this does not qualify independent-process or power loss.
The first diagnostic run exposed an obsolete catalog stall key and an attempted
page-store reopen while the engines correctly retained page pins. The corrected
fixtures preserve those safeguards and are retained with the passing evidence.

All 231 selected Rust cases and twelve commands pass, including all-target Clippy,
formatting, layout, dependency isolation, docs, source preflight and 37 validator
cases. `.cellar/cell-groups-evidence/CG-04-dynamic-registration/checkpoint.json`
binds 281 source inputs and 4 copied executables. Transfer this ignored
local directory with prior checkpoints; it is not included in the PR.

CG-04 remains IN_PROGRESS. The critical next gap is per-group failure isolation:
a group-local apply/command failure currently closes the node controller.
Quarantine that group, revoke serving authority and retain its physical owners
through cleanup, while proving that a healthy group continues. Shared log-engine
sync failure must still quarantine every group on the affected engine.
Request-driven provisioning/lazy birth must still connect catalog selection and
bounded local preparation to this registration API. Configuration lifecycle and public SDK discovery/cutover remain
open, followed by the CG-05–16 dependencies. Shared-group RustFS recovery and load
remain unqualified; no throughput or latency gain is claimed.

## CG-04 standalone receipt inspection

Qualified source: `fbc15053f8dcfd3b46078abad3cbbae7b68da8d6`.

Local and remote receipt inspection now select an immutable committed cell image
through a correlated quorum read. The registered command contract, payload digest
and authenticated identity are validated before lookup. Inspection neither stages
a producer nor invokes the command or initializer; it does not install receipt
schema or run receipt maintenance. Initial images may lack receipt tables;
partial schema or schema loss after mutation refuses as corruption.

The existing bounded RPC exchange carries a distinct authenticated Receipt purpose.
Explicit namespace/process command permission and the exact controller/SQL owner
are required; read permission alone cannot inspect command results. Fresh receiver
catalog binding checks group, schema, epoch and birth. Success binds the original
command identity and signed cell observation, distinguishing a retained empty
result from absence. Absence remains ambiguous even after a quorum read: the
original command may still arrive or commit. Lookup cannot release its producer.
Replicated expiry, conflicts and sequence gaps remain typed refusals.

Three native cases run both image modes. Real TLS ingress exercises permission,
identity, retry/deadline, consumed-attempt, reply-admission and gateway-drop checks.
A prepared writer remains owned while lookup sees the old image, while the shared
SQL reservation budget is saturated, and after its receipt becomes committed.
Cancellation of a queued remote lookup preserves an entered controller writer.
A closed controller uses a weak SQL handle, allowing a replacement pool after
joined shutdown even when the closed controller handle remains retained. Three
wire cases and a real SQLite schema-loss case qualify serialization and inspection
boundaries. The initial fixture failures and their corrected bounded admission
setup are retained as diagnostic logs; production limits were not loosened.

All 227 selected Rust cases and twelve gate commands pass, including all-target
Clippy, formatting, layout, native dependency isolation, docs, source preflight
and 37 plan-validator cases. The record at
`.cellar/cell-groups-evidence/CG-04-receipt-rpc/checkpoint.json` binds 279
source inputs and 4 copied executables. Transfer the entire directory with
prior checkpoints; local ignored evidence is not included in the PR.

CG-04 remains IN_PROGRESS. The critical next gap is dynamic group lifecycle:
replace the fixed controller roster with bounded registration and lazy birth,
while preserving exact consensus, transport, apply and SQL ownership. Qualify
attach/close races, cancellation and recovery before publishing serving authority.
Per-group failure isolation is the next companion gap: a failed group must stop
serving while unrelated groups continue, and retained physical work must join.
Public SDK/discovery cutover, configuration handling, complete-group snapshots,
archive/reclamation and shared RustFS/load qualification remain open. No latency,
throughput, independent-process failure or production-cutover claim is made.

## CG-04 remote enrollment through the shared write owner

Qualified source: `0fe665d0267f1b831b172aad5e8d435f5bf0cd39`.

`CellEnrollmentInvocation` now consumes catalog attempts over the same bounded
write exchange as commands. An authenticated mandatory purpose distinguishes
Query, Command and Enrollment; marker 1 has no reader for the old boolean purpose.
The existing exact controller registration and namespace/process write grants
admit compiled initializers. Read permission, discovery and voter membership
cannot authorize creation. The router still allocates no local group replica.

Creation identity, captured clock/random inputs and preview identity are frozen
before discovery and retained through retries. Original attempt/deadline budgets
remain in force. The receiver checks fresh catalog group/schema/epoch/birth and
actual leader authority before handing initialization to the controller. Followers
return NotLeader; a leader awaiting apply still returns NotReady internally.
Disconnect, permission revocation and response expiry cannot discard accepted
controller ownership. A possibly transmitted invocation cannot be reused as fresh
local enrollment.

Success returns the exact committed creation record with bounded reply admission.
The original enrollment image survives later mutations; duplicate retries return
that record without initialization or reverting current cell state. The record
is not a current-version token. Creation/incarnation conflicts remain typed.
Command and enrollment requests share physical TLS exchange and reservation code,
so there is no second per-peer write connection or independent budget.

Four new native cases run four TLS endpoints in one OS process in both image
modes. They cover missing/read-only write permission, voter/follower refusal,
exact creation receipts, duplicate creation after a later remote command,
creation/incarnation conflicts, consumed attempts, original deadlines, reply
reservations, gateway drop and refusal to treat routed work as fresh local work.
Held initialization tests disconnect and revocation. A held committed manifest
publication tests response expiry followed by exact creation recovery. Three
wire cases verify captured inputs/preview, bounded payloads, authenticated purpose
separation and creation/cell/schema/image reply binding.

All 216 selected Rust cases and eleven gate commands pass, including all-target
Clippy, formatting, layout, native dependency isolation, docs, source preflight
and 37 plan-validator cases. The record at
`.cellar/cell-groups-evidence/CG-04-enrollment-rpc/checkpoint.json` binds 276
source inputs and 4 copied executables. Transfer this entire directory with
prior checkpoints. Targeted runs remain diagnostic evidence.

CG-04 remains IN_PROGRESS. The next request-path gap is standalone safe receipt
lookup after an ambiguous response. Public SDK/discovery, dynamic roster and
lazy group birth, per-group failure isolation and configuration lifecycle remain.
The shared lane still serializes exchanges per peer. Complete-group snapshots,
archive/reclamation, shared RustFS recovery, independent-process faults and load
remain unqualified; no throughput or latency improvement is claimed.

## CG-04 write-authorized remote commands

Qualified source: `5fd4443796a6e990fbe70c16acb7fa5578b7e763`.

`CellCommandInvocation::execute_remote` sends one consumed catalog attempt over
the bounded application RPC lane. The authenticated route has a mandatory command
purpose; command payloads/results have distinct marker-1 domains. Read grants and
voter membership never create write permission. `NamespaceCommandOrigin` grants
one namespace to an exact physical process, with independently bounded revision
tombstones. Endpoint checks, rooted process refresh, fleet revocation and peer-pool
retention include command grants. Discovery still allocates no local replica.

A host-owned `CellCommandGateway` binds the exact controller/runtime owner. The
request carries the caller's captured time/random inputs, immutable command
identity, operation contract, retry horizon and remaining deadline. The receiver
controller checks the current catalog group/schema/epoch/birth and actual leader
authority before SQL. It retains admitted work independently from the connection;
disconnect, timeout or write-grant revocation cannot undo an admitted mutation or
release its physical producer. Final registration drop refuses new commands.

Success returns the exact committed command identity/result and authenticated
logical cell observation/token. Reply admission remains charged until consumption.
Original deadlines/attempt budgets survive retries. Conflict, expiry, sequence-gap
and not-leader refusals remain typed. Once remote transmission may occur, errors
remain conservatively ambiguous; neither admission nor response loss proves
non-commit. Committed duplicate retries return the stored result without another
application callback.

Three new native cases run both image modes over four TLS endpoints in one OS
process. A nonmember router with no group storage tests missing/read-only,
wrong-process and foreign-namespace write permission, voter non-authority, follower
refusal, exact controller binding, committed duplicate/conflict handling, reply
reservations and registration drop. Held physical SQL tests disconnect and grant
revocation: the controller keeps ownership, then resolves the command and returns
its committed receipt on retry without a second callback. Three wire cases cover
captured inputs, payload/purpose separation, exact command/history/token binding
and typed failures. A transport case checks bounded tombstones, stale revision
refusal, independent read grants and pool cleanup.

All 209 selected Rust cases and eleven gate commands pass, including all-target
Clippy, formatting, layout, native dependency isolation, docs, source preflight and
37 plan-validator cases. The record at
`.cellar/cell-groups-evidence/CG-04-command-rpc/checkpoint.json` binds 273 source
inputs and 4 copied executables. Transfer that entire directory with prior
checkpoints. Targeted compile/test logs are retained as diagnostic evidence.
The initial follower assertion ran without a follower gateway and failed before
leadership admission. After adding the gateway, diagnostics exposed follower
authority requests returning NotReady and becoming generic Unavailable remotely.
The owner now reports NotLeader for that role, with leader readiness and stale
authority checks preserved. Direct and remote tests assert the precise refusal.

CG-04 remains IN_PROGRESS. Remote enrollment and standalone safe receipt lookup
are the next request-path gaps; public-host automatic discovery/SDK binding, lazy
birth/dynamic roster, per-group isolation and configuration lifecycle remain.
The RPC lane serializes exchanges per peer; no load or latency gain is claimed.
Destination catalog reads remain until replicated transfer sealing and serving
invalidation are integrated. Complete-group snapshots/archive, shared RustFS
recovery, independent-process failure, power loss and load remain unqualified.

## CG-04 bounded invocation route cache

Qualified source: `06652e36869144624e0ca4d860782ef512018791`.

`CellGroupDispatcher` clones now share bounded namespace range hints across query,
command and enrollment invocations. One cached range serves many cells; no
per-cell cache, worker or Raft owner is allocated by routing. Explicit entry and
reserved-byte limits bound retention, with LRU eviction. The conservative 8 KiB
reservation per slot remains charged even while the slot is empty.

Hints come only from a verified directory lookup. `refresh_routes` observes the
catalog in the owning execution domain; a newer head invalidates older hints.
An older in-flight lookup cannot refill the cache after that observation, and a
same-revision/different-digest observation refuses. Failed refresh retains only
historical hints. Redirect confirmation, original deadlines, attempt budgets,
exact cell identities and submitted-command ambiguity remain enforced.

Three catalog cases disable page caching and measure actual object-store calls.
After warming one range, 64 other cells and a command route with zero metadata
reads. Byte-limited LRU eviction, successor-head invalidation and a deliberately
held stale lookup pass. A fourth native case runs both physical image modes over
real TLS quorum owners: after removing the catalog head, local reads, a command
and enrollment succeed using the hint plus actual runtime authority. Remote
admission refuses because the destination still requires a fresh catalog check.
The fixture restores metadata and joins all physical owners.

All 202 selected Rust cases and eleven gate commands pass, including all-target
Clippy, formatting, layout, native dependency isolation, docs, source preflight and
37 plan-validator cases. The source-bound record at
`.cellar/cell-groups-evidence/CG-04-dispatch-route-cache-final/checkpoint.json` binds
265 source inputs and 4 copied executables. Transfer the complete directory
with previous checkpoints, including the initial `CG-04-dispatch-route-cache`
Clippy failure. That fixture called Tokio timeout directly; the successor uses
the execution-domain timeout without changing assertions or production behavior.
Targeted diagnostic logs are retained separately.

CG-04 remains IN_PROGRESS. This removes repeat caller-side mapping reads; it does
not remove destination catalog admission or establish end-to-end latency gains.
Destination caching needs replicated transfer sealing and admission invalidation,
not a timeout that treats a stale hint as permission. The next critical delivery
is write-authorized command/enrollment RPC with response-independent producer
ownership and safe receipt lookup. Public-host discovery/SDK binding, lazy birth,
dynamic roster, per-group failure isolation and configuration lifecycle remain.
Complete-group snapshots/archive, shared RustFS recovery, independent-process
failures, power loss and load remain unqualified.

## CG-04 nonmember read discovery

Qualified source: `c25f0d97d5c98c6111ea2165b38a11a08613c0ca`.

`RaftTransport::install_cell_query_group` installs explicit trusted discovery for
physical groups absent from the router. It creates no local group storage,
RawNode, voting identity or mutation permission. The shared query path uses that
view or an attached group's view; destination namespace grants, live membership,
catalog binding and actual read authority are still required. Removing discovery
invalidates captured routes and retires unused peer pools; it does not revoke the
destination's independently managed read grant.

Read-only routes share the node's route cap, endpoint consistency checks and peer
pools with consensus and other queries. They participate in process-view refresh
and router-route resource accounting. Route revisions cannot move backwards or
change schema, and existing member identities cannot silently change node/disk.
The sender revalidates captured route identity before sending and after receiving;
a late authenticated reply under a replaced route cannot become a success.

Three new native cases run both materialized and extent images over four real TLS
endpoints in one OS process. The fourth node has no stored cell groups or attached
Raft owner before or after reads. Discovery alone, a foreign-namespace grant and a
wrong-process grant refuse before SQL. Explicit permission permits leader strong
reads and follower stale/AtLeast reads. The current strong-read contract refuses
a follower; the test verifies that retrying the leader preserves the original
invocation deadline and attempt budget. Discovery shares three physical pools
across multiple routes and enforces its configured capacity.

Revocation while a callback is held ends the remote read waiter and drains
transport reservations while physical SQL remains owned. A stale grant revision
cannot reopen admission; a newer explicit grant permits a fresh read after cleanup.
Replacing discovery while SQL is held refuses the old reply and permits a fresh
attempt. Neither mechanism grants consensus authority or creates a router replica.

All 198 selected Rust cases and eleven gate commands pass, including all-target
Clippy, formatting, layout, native dependency isolation, docs, source preflight and
37 plan-validator cases. The source-bound record at
`.cellar/cell-groups-evidence/CG-04-nonmember-queries/checkpoint.json` contains 262
source inputs and 4 copied executables. Transfer that complete directory with
prior checkpoints. Diagnostic targeted runs are retained separately: the first
used a local-only default Raft message before the intended absent-group assertion;
the fixture now sends a valid heartbeat. The next assumed follower strong reads;
the corrected fixture asserts the refusal and preserved-budget leader retry.

CG-04 remains IN_PROGRESS. Discovery is explicitly installed by a trusted caller;
public-host automatic discovery and SDK binding are still missing. The destination
still resolves the catalog per RPC: bounded, correctly invalidated routing state
is the next cost/latency opportunity. Write-authorized command/enrollment RPC and
safe receipt lookup, lazy birth/dynamic roster, per-group failure isolation and
configuration/cache lifecycle remain. Complete-group snapshots, shared RustFS
recovery, independent-process failures, power loss and load remain unqualified.

## CG-04 shared query RPC

Qualified source: `da79a69604d58be1c7d9663ebb69c3f6ded173f6`.

Catalog-routed `CellQueryInvocation::execute_remote` now uses the existing
node-owned TLS query lane. The authenticated marker-1 route separates physical
`GroupHistory` from logical `CellHistory`; its payload also binds the catalog
route epoch and birth. There is no reader for the former untagged query route.
The temporary old-host query arm remains explicit until CG-16. Request/reply
channel binding, sequence checks, node/disk/process/member identity and bounded
framing remain shared with the existing transport.

`RaftTransport::install_cell_query_gateway` registers a namespace dispatcher on
the exact runtime/SQL engine and execution domain. The host owns the registration;
the transport stores a weak reference. A separate admission owner refuses new
reads after its final host handle is dropped, even while an accepted query retains
the gateway data. Destination catalog and coherent runtime
read authority are revalidated before registered SQL runs. Replies carry the
actual selected cell observation and signed cell token. Foreign histories,
incarnations and signing keys cannot produce a successful result. The caller
also enforces its requested AtLeast minimum.

Both query paths use the same read-only origin grant/revocation guard, peer pools,
connection exchange and byte/item banks. These permissions cannot authorize a
mutation or consensus message. A fully authenticated query refusal keeps the
connection reusable; transport/protocol failures discard it. The single peer
sender assigns wire sequence numbers after dequeueing, so concurrent ingress
cannot invert them and cause false replay refusals. Each attempt is
consumed once; retries and catalog-confirmed redirects retain the original caller
deadline and total attempt budget. Returned replies keep transport admission until
dropped. Disconnect and expiry cancel read waiters without releasing physically
entered SQL or image ownership; controller shutdown still joins those workers.

Three new native cases each run both materialized and extent images on three
real TLS endpoints in one process. They cover two cells sharing a group, strong/
stale/AtLeast reads, exact cell observations, wrong tokens/keys/incarnations,
foreign SQL ownership, duplicate/dropped gateway registrations, concurrent-cell
requests, retained responses,
retry budgets and cancellation/deadline shutdown. A fresh linearizable barrier
still completes while a query callback is held. A catalog replacement after route
capture refuses at the old destination before callback execution; a confirmed
redirect cannot invent its new physical owner. That fixture changes metadata only
and does not qualify live movement. Three new wire cases cover bounded parsing,
physical/logical identity and channel binding, token authentication, typed failures
and payload-domain separation.

All 195 selected Rust cases and all eleven gate commands pass. This includes the
wire tests, full transport-module regressions, shared execution/catalog suites and
existing host suite, plus all-target Clippy for Cellar/cellar-logic/cellar-ltx with
warnings denied, formatting, layout, native dependency isolation, docs, source
preflight and 37 validator cases. The record at
`.cellar/cell-groups-evidence/CG-04-remote-queries-final/checkpoint.json` binds 260
source inputs and 4 copied executables. Transfer the complete evidence directory
with prior checkpoints. The first diagnostic compile caught a cleanup hook in the
wrong owner; it was fixed before tests. The first committed gate passed Clippy,
eight wire cases and 63 transport cases, then was stopped during native testing
after review found the registration lifetime gap. The separate admission owner
and held-callback assertions fix that gap; this successor repeats the full gate.
A second candidate was stopped during Clippy to move sequence assignment into
send order and add concurrent-cell coverage. All diagnostic and interrupted gates
remain separate from this final qualification.

CG-04 remains IN_PROGRESS. This checkpoint routes between attached group peers;
nonmember shared discovery and its real-TLS revocation qualification remain open.
The destination currently resolves the catalog per RPC, so bounded route-cache
integration is a critical cost/latency opportunity. Public SDK binding and explicit
write-authorized command/enrollment RPC, including safe follower receipt lookup,
are still missing. Lazy birth/dynamic roster, per-group failure isolation and
configuration/cache lifecycle also remain. Shared-group RustFS recovery, separate
process failure, power loss and load are unqualified; the public host has not cut
over. Continue with these concrete integration gaps before claiming CG-04 DONE.

## CG-04 controller-owned enrollment

Qualified source: `f77d0d939347ead7129b39e72f76fcff11b99635`.

`RaftClass::initializer` registers one compiled initialization callback with an
explicit contract digest. That digest participates in the voting schema identity;
changing or omitting it cannot claim the same namespace schema. Declaration order
with commands does not change the resulting identity. Existing old-host class
validation refuses this registration rather than silently ignoring the callback.
Shared initialization receives the logical cell history and captured host inputs
through its private transaction on the existing bounded SQL workers.

`CellGroupDispatcher::enroll` freezes creation identity, schema and host inputs
before catalog discovery. `CellGroupController::enroll` takes fresh local
invocations into the same queue and byte/item budget used by commands. Both use
one response-deadline implementation and one physical shutdown owner. A dropped
or expired response cannot discard entered initialization or ambiguous submission.
Unconsumed creation replies remain charged. The SQL pool distinguishes completed
initialization errors from lost worker replies, preserving unresolved ownership
through controller failure and joined cleanup.

The driver seals only never-attempted late preparations. Submitted creations
resolve through their original immutable enrollment record or the existing newer
applied-term absence/conflict proof, without automatic initializer replay. A
retry after later cell mutations returns the original creation record and never
restores the initial image. A different creation ID or logical incarnation is a
conflict. Request/namespace S3 minima continue to refuse without archive proof.

Eight new native cases cover registration refusal plus lifecycle/fault behavior;
seven run both materialized and extent modes over three in-process TLS endpoints.
They hold initialization or committed manifest sync, disconnect/expire responses,
verify command/enrollment shared admission, preserve exact creation identity after
mutation, refuse conflicts without callbacks, and cancel shutdown while physical
work is held. Completed initializer errors permit an explicit retry; a physical
capture panic exercises a lost worker reply and joined cleanup. A unit case checks
initializer digest compatibility, declaration order, invalid/duplicate registration
and old-host validation refusal. The existing command-manifest compatibility case
also passes. The initial lifecycle fixture incorrectly began a new cell's client
sequence at 2; production correctly refused it. The fixture now starts at 1.
A subsequent compile caught a missing Box constructor in the common completion
envelope; that was corrected before qualification. The `bd3069d7` gate passed all
90 Rust cases, then Clippy rejected a redundant unit expression. That expression
was removed and the complete gate repeated at this successor. Both gates and
diagnostic logs are retained.

All 90 selected Rust cases pass: 73 shared execution, 15 catalog, one initializer
unit case and one existing host compatibility case. All ten gate commands pass,
including all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied,
formatting, layout, dependency isolation, docs, source preflight and 37 validator
cases. The partial record at
`.cellar/cell-groups-evidence/CG-04-enrollment-controller-qualified/checkpoint.json` binds
254 source inputs and four copied executables. Transfer its complete directory
with prior evidence. The gate does not qualify shared-group RustFS, separate-process
crashes, power loss or load behavior.

CG-04 remains IN_PROGRESS. The critical gap is now integration of public handles
and remote command/query/enrollment dispatch with these shared owners, including
catalog-confirmed retries and safe follower receipt lookup. Enrollment currently
requires a group already attached to the fixed controller roster; lazy group birth
and dynamic registration remain required. Per-group failure isolation and
configuration/cache lifecycle are also incomplete. Complete-group snapshots,
archive/reclamation, effects, topology, RustFS/load and legacy removal remain later
gates. The public host has not cut over.

## CG-04 controller-owned commands

Qualified source: `8c615ac91bc3c13e6c25da4f4ab584a73e609207`.

`CellGroupController::command` transfers a fresh local invocation into the
existing node worker. The separate response handle preserves the original
deadline; dropping or timing out that handle does not cancel SQL preparation,
submission or receipt resolution. Count/byte admission covers queued commands,
active resolution and unconsumed responses, including errors. SQL capture/apply
and result bytes keep their separate pool budgets. Admission refusal returns the
original invocation, preserving any recovery state. One shared progress timer
and applied-event notifications drive retries; continuous traffic cannot reset
the timer and indefinitely postpone progress.

A late preparation that has never attempted submission is sealed across all
clones and closed through its exact rejection capability. Entering, accepted and
uncertain attempts cannot produce that seal. An attempted submission error stays
uncertain; a later capacity refusal cannot prove the earlier attempt absent.
After submission, resolution requires the exact receipt or the existing newer
applied-term absence proof. It never replays a callback automatically. Retention
is bounded by admission, not by a timeout that discards uncertain state. On a
fatal unresolved command error, the controller retains the invocation through
physical SQL/runtime joins before releasing ownership.

SQL staging now distinguishes a received completed error from a lost worker
reply. The former cannot hide an installed producer and releases this invocation
without closing the node; the latter remains ambiguous and retained. The public
stage API keeps its existing error type. Physical image capture, callbacks and
manifest publication continue to run on the existing SQL workers.

Seven new native cases each run in both materialized and extent modes over three
real in-process TLS endpoints. They hold callbacks or committed manifest sync,
disconnect or expire responses, assert bounded ownership and exact identity retry,
verify unconsumed-response charging, and cancel shutdown while physical work is
held. Completed callback errors permit an explicit successful retry. A physical
image-capture panic loses the stage reply and requires joined cleanup. Three unit
cases prove clone sealing, entering/accepted refusal and uncertainty after an
attempted error. The initial fixture used an invalid retry horizon; the corrected
fixture preserves one valid horizon. An initial callback-panic injection was
contained by Cellar's transaction and therefore did not simulate a lost reply;
the final injection occurs during subsequent physical capture. Failed logs remain
with the checkpoint and are not counted as qualification.

The gate passes 83 selected Rust cases (65 execution, 15 catalog, three submission)
and nine commands: affected tests, all-target Clippy for Cellar/cellar-logic/
cellar-ltx with warnings denied, formatting, layout, dependencies, docs, source
preflight and 37 plan-validator cases. The partial record at
`.cellar/cell-groups-evidence/CG-04-command-owner/checkpoint.json` binds 248
source inputs and three copied test binaries. Transfer its complete directory
with prior evidence. This qualifies local shared-controller command ownership,
not real RustFS, separate-process crash, power-loss or load behavior.

CG-04 remains IN_PROGRESS. The critical next delivery is catalog-routed enrollment
with stable creation identity, then remote shared command/query dispatch and the
public SDK binding to this owner. Missing origin producers on followers still
need a safe leader receipt-lookup route. Dynamic roster, per-group failure
isolation and configuration/cache lifecycle are unresolved runtime gaps.
Complete-group snapshots/archive, effects, topology, RustFS/load qualification and
legacy removal remain later gates; the public host has not cut over.

## CG-04 autonomous event controller

Qualified source: `ee684c3394ffccbfa84bfc5d94876de281f68b0b`.

`CellGroupController` owns a fixed roster's exclusive runtime event stream and
drives existing physical TLS transport, SQL preparation and runtime publication
without fixture pumping. Exact runtime/event/transport and engine/pool/applier
bindings refuse foreign owners even when they share an execution domain. One
node cleanup task runs bounded apply jobs, serializes each group's work and
rotates pending groups. Retained tickets preserve event and physical admission
through SQL backpressure; a shared node timer backs off retries. Only runtime
post-sync `Applied` events advance published progress. Transport queue admission
is not commit evidence. Immediate transient send refusals produce bounded advisory
hints; fatal transport/runtime/apply errors close the node and remain observable.

Close requests and final-public-handle drop stop the controller. Cleanup joins
transport and physical SQL work before releasing the runtime's engine claim,
retaining outstanding tickets/appliers until all joins complete. Cancelling a
close waiter cannot release a blocked physical producer or permit replacement.
This controller has a fixed startup roster and currently fails the entire node
on a fatal group apply error; dynamic registration and per-group failure isolation
remain required. It does not yet own command invocations or replace the SDK host.

Six new native cases qualify autonomous TLS replication in both image modes,
two groups sharing peer pools and one SQL worker, admission retry while that
worker is blocked, exact receipt settlement and same-domain foreign-owner refusal.
Real manifest directory-sync faults leave the engine-selected root unchanged.
Cancelled close and final-handle drop while physical sync is blocked retain
ownership until release; no interrupted preparation is published. The initial
shutdown test failed because the harness retained old pool handles while attempting
replacement. It now releases those handles after verifying physical joins; the
production lease checks and assertions remain intact.

All 58 shared-execution cases and eight gate commands pass at this revision,
including all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied,
format (1,829 Rust files), layout, dependencies, docs, source preflight and 37
plan-validator cases. The preceding `61aa7752` run passed 289 Rust cases (198
native across seven suites and 91 transport-filtered library cases), then Clippy
rejected duplicate loading of the physical fault fixture. This successor reuses
the existing module; production code and assertions are unchanged. The affected
execution suite was rerun; other regressions retain their original revision.
The partial record
`.cellar/cell-groups-evidence/CG-04-event-controller-qualified/checkpoint.json`
binds 244 source inputs and the execution test binary. Transfer it with the
retained `CG-04-event-controller/` gate and prior evidence. Four ignored process
helpers ran through their parents in the earlier regression run; its RustFS seed
case was skipped. Local TLS endpoints share a process. This is not RustFS,
independent-failure-domain, power-loss or load qualification.

CG-04 remains IN_PROGRESS. The most critical next gap is command ownership after
caller disconnect: move fresh invocations into this controller and distinguish
response deadlines from bounded resolution ownership. Preserve exact origin and
inputs through uncertainty; never replay ambiguous callbacks or treat missing
local state as non-commit. Before adding cancellation cleanup, seal only a
preparation that has never attempted submission: the current submission state
resets to Open after an error, which cannot establish that append did not occur.
Retain a distinct uncertain-attempt state and qualify timeout/disconnect after SQL
entry and provisional acceptance. Then complete routed enrollment, shared query
RPC and SDK integration. Configuration/cache lifecycle, snapshots/archive,
effects, topology, real RustFS/load and final legacy removal remain later gates.

## CG-04 physical-group TLS transport

Qualified source: `eb99a2427e8661dc228ea7a1d3d1f56a5e0937aa`.

`RaftTransport::start_shared` starts directly on the physical-group runtime.
`GroupRoute<GroupHistory>` and typed peers share the existing node-owned TLS
connections, control/data pipelines, byte/item budgets and joined shutdown.
Route installation requires an existing selected group, its schema, committed
member/disk binding and exact local process/listeners. Authorization rechecks
current routes and membership through queueing, I/O and response delivery.
Physical routes participate in node-view refresh, pool revocation/pruning and
combined group limits. Conflicting query/inspection endpoints refuse.

Marker-1 frames use typed histories and a new protocol digest. Physical consensus
has no cell key; old untagged wire layouts refuse. Requests and responses bind
full replica identity, schema, sequence, payload and TLS channel. The exclusive
runtime transport claim survives until physical tasks exit. Delivery is only
receiver admission, not a Raft persistence or commit proof. Unsupported
complete-group snapshot messages refuse. The temporary legacy runtime/frame arms
are explicitly scheduled for deletion with the old host in CG-16.

Three new native cases use real TLS endpoints. Two groups replicate three
independent SQLite cells each on the same peer pools. Unknown groups, wrong
schema/local process, conflicting grants and unsupported snapshots refuse.
Removing one group's route retains the other group and its peer pools; restoring
it delivers again. A second transport refuses until joined close releases its
claim. Existing routed command cancellation, exact receipt and leader-transfer
assertions also pass over TLS in both image modes. A new frame case verifies
history/incarnation binding and refusal of old wire/protocol layouts.

All 283 selected Rust cases pass: 192 native cases across seven suites and
91 library cases selected by the transport filter. All nine commands pass,
including all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied,
formatting (1,826 Rust files), layout, dependency isolation, docs, source preflight
and all 37 plan-validator cases. The partial record at
`.cellar/cell-groups-evidence/CG-04-physical-transport/checkpoint.json` binds
241 source inputs and eight selected executables. Initial private-import and
legacy-membership-validator compile failures, the corrected targeted run and
formatter log are retained. Four process helpers run through their parent cases;
the RustFS-specific seed case is skipped. Prior checkpoints retain their original
source/scope. These local endpoints share a process; they do not qualify independent failure
domains, RustFS, power loss or load. Latest fetched `origin/main` is included and
the requested MR-13/MR-14 progress JSON files remain excluded.

CG-04 remains IN_PROGRESS. The critical next gap is the production controller:
retain command invocations independently from callers, drive existing shared
transport/apply/completion events, and maintain bounded reconciliation after the
response deadline. The fixtures still pump events, so a cancelled preparation
future is not evidence of controller ownership after ingress disconnect. Reuse
the now-working physical transport and SQL/apply owners; qualify disconnects after
SQL entry and provisional acceptance without replaying ambiguous callbacks. Then
complete routed enrollment and public SDK/shared-query RPC integration.
Configuration/cache lifecycle, complete-group snapshots/archive, effects,
topology, real RustFS/load and final legacy removal remain later requirements.

## CG-04 catalog-routed command phases

Qualified source: `1f014f42264c73b1d5e3cd387f65ffed45140ecb`.

`CellGroupDispatcher::command` freezes logical command identity, input, namespace
minimum durability, original routing budget and domain-captured time/randomness.
Preparation retains its original runtime, SQL pool, exact route and physical
preparation IDs before entering SQL. A cancelled response or routing retry resumes
that producer instead of executing a second destination's callback. Submission
reuses exact shared acceptance and pre-append seals. Acceptance is provisional;
settlement requires the exact receipt, while absence requires the existing newer
applied-term barrier. Errors preserve command identity and conservative ambiguity.
Returned receipts retain their output admission and sign the cell observation.
Both request and namespace S3 minima refuse until real shared archive proof exists.

Two new three-voter cases cover both image modes, held-callback cancellation,
retry through a follower while retaining the original producer, duplicate proposal
acceptance, same-term uncertainty, exact receipt recovery after another cell
advances the group, and a real leader transfer. A proven non-commit can retry with
the same captured time/randomness. S3 refusal precedes callback execution.
The leadership fixture checks the caller's actual cell and uses the production
applier where installed; the held newer-term barrier assertion remains unchanged.

All 105 native cases pass across execution, catalog, read and runtime suites;
one runtime helper is invoked by its parent. All eight commands pass, including
all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied,
formatting, layout, dependency isolation, docs, source preflight and all 37
plan-validator cases. The partial record at
`.cellar/cell-groups-evidence/CG-04-routed-commands-qualified/checkpoint.json` binds
229 source inputs and four selected executables. The failed gate at
`CG-04-routed-commands` is retained: all native cases passed, then Clippy rejected
the large command error. Its typed cause is now boxed, preserving all assertions.
A second gate at `CG-04-routed-commands-final` retained the passing native tests
and fixture lint failure; the direct Tokio select now uses Cellar's required
fair-polling macro. Earlier compile and fixture failures are retained too. No RustFS
or performance qualification was run. Latest fetched `origin/main` at `8a93ffd7` is included;
the requested MR-13/MR-14 JSON files remain excluded.

CG-04 remains IN_PROGRESS. The critical next gap is production controller ownership
of these phases after ingress cancellation, including an explicit bounded
reconciliation lifecycle beyond the response deadline. The current tests retain
the invocation while cancelling its preparation future; they do not qualify a
controller after the caller drops. Adapt the existing transport's cell-scoped
identity to physical group history, drive transport/apply/completion events, and
qualify exact resolution after caller disconnect. Then complete routed enrollment
and public SDK binding. Missing-origin follower receipt routing, configuration and
cache lifecycle remain unfinished. Reuse existing workers and resource owners;
complete-group snapshots/archive, effects, topology, real RustFS/load and the final
hard cutover remain later task gates.

## CG-04 catalog-routed query invocations

Qualified source: `8723db2df7436478cbc5efc9147d65d3b7651247`.

`CellGroupDispatcher` binds a registered class to its configured namespace,
version keys and execution domain. Queries freeze bounded input, operation and
consistency before discovery. Clones share invocation admission, and the canonical
fingerprint includes logical history and routing limits. Deadline capture precedes
validation and hashing. Existing catalog sessions perform lookup, cached routing,
member retries and exact redirect confirmation without replenishing deadline or
attempt count. Each attempt is consumed before asynchronous destination work.

Local execution authenticates AtLeast tokens, checks the destination's full route
and logical history, and reuses existing runtime read authority and registered SQL
workers. Foreign domain clocks and engines refuse. The response token signs the
selected cell observation. Dropped routing releases only invocation admission;
entered SQL retains its independent input, image and worker ownership through
joined shutdown. Directory metadata cannot create a physical group owner.

The two new three-voter cases cover both image modes, real directory lookup,
linearizable execution, a lost response retry, follower AtLeast, foreign
cell/domain/class/engine rejection, invalid redirect ticket invalidation, original
timeout/attempt exhaustion, a confirmed replacement with no physical owner, and
cancelled routing while a callback is held. The replacement case tests only
metadata confirmation and refusal; it does not qualify live group transfer.

All 103 native cases pass across the shared-group execution, catalog, existing
read and runtime suites. The runtime process helper is invoked by its parent.
This targeted gate complements the prior 319-case registered-query checkpoint at
its original source; it does not rerun the complete previous selection.
All eight commands pass, including all-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,821 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases. The partial record is
`.cellar/cell-groups-evidence/CG-04-routed-queries/checkpoint.json`, binding
227 source inputs and four selected executables. The initial missing type
qualification, corrected targeted run and formatter logs are retained separately.
No RustFS or performance qualification was run. The branch contains latest fetched
`origin/main` at `8a93ffd7`; the requested MR-13/MR-14 JSON files remain excluded.

CG-04 remains IN_PROGRESS. The most critical gap is routed command/enrollment
execution with retained ambiguous outcomes and exact receipt/creation settlement.
Next extend the same dispatcher and qualify enrollment, command, lost response and
linearizable query together. Production node-controller event/transport integration
and replacing the public SDK's old host binding remain required; the native
fixture still pumps shared-runtime events. Retry/cache/configuration, shared effects,
snapshots/archive, physical fault and real RustFS/load gates remain unfinished.

## CG-04 registered queries with exact cell observations

Qualified source: `7a79511d4b8606d11177778a3fe8adf71a8d901a`.

`CellGroupSqlPool::query_registered` dispatches compiled queries through the
existing coherent query owner and fixed SQL workers. It validates operation
kind/contract and full schema before opening the selected image, bounds retained
operation/input storage, and preserves the original authority deadline. The
callback receives the exact `CellHistory` and `CellVersionObservation` returned
in its result, including the selected cell's decision witness. It never derives
a mutation sequence from group log progress.

The two new three-voter cases exercise materialized and extent images. An older
A pin returns its historical state after A and B mutate; a fresh linearizable A
query returns A's sequence and witness independently of B's later group progress.
Tokens round-trip the selected observation. Invalid contract, kind, operation,
schema and input refuse before callback execution. Read-only restrictions,
contained panics and SQL deadline interruption remain enforced. Cancelled waiters
retain input/image/worker ownership, and shutdown joins entered callbacks before
releasing reservations.

The temporary old adapters explicitly use `QueryVersion::GroupCut` and
`RaftClass::invoke_group_query`. These preserve the actual old in-memory group cut
without fabricating a cell sequence. CG-16 requires removing both with the old
host; the final context exposes the selected cell observation directly. This is
an integration checkpoint, not a second permanent production mode.

All 319 selected Rust cases pass: 280 native cases across thirteen suites,
30 Cellar library cases, five pure cell-group core cases and four LTX image-store
cases. The existing native read suite additionally covers current-term barriers,
minority refusal, follower catch-up, delayed replies and version authentication.
Six process helpers run through parent cases; three RustFS-specific cases remain
unrun. These results do not qualify the new shared dispatcher or infrastructure.

All seventeen commands pass, including all-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,819 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases. The retained checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered-queries/checkpoint.json`, binding
225 source inputs and sixteen selected executables. Pre-commit library, targeted
query, format and Clippy logs are retained separately. The branch contains latest
fetched `origin/main` at `8a93ffd7`; the requested MR-13/MR-14 JSON files remain
unchanged from main.

CG-04 remains IN_PROGRESS. The most critical gap is catalog invocation sessions
and production SDK dispatch into registered enrollment, command and query workers,
with namespace-authorized classes, exact receipt settlement and original deadline,
fingerprint and attempt budgets. Next qualify that actual routed path under lost
responses and redirects. Reuse the existing owners instead of adding a worker
pool. Retry/cache and configuration handling, shared effects, complete-group
snapshots/archive, process-kill cuts and real RustFS/load gates remain unfinished.

## CG-04 registered commands and logical effect identity

Qualified source: `b28078afbd243c18ac7c671a481a3e369c086ab2`.

`CellGroupSqlPool::stage_registered` now dispatches a real compiled `RaftClass`
on the existing fixed workers. Its retained input is bounded by the producer
budget. The worker verifies operation kind/contract, full class schema, normalized
input digest and authenticated inbox/reserved client identity before receipt
lookup, including duplicates. It uses the existing private receipt transaction,
prepared submission and exact committed settlement paths. The shared path obtains
logical identity from `CellExecutionAuthority`; it never constructs `GroupScope`.

`CommandContext::identity()` and `RaftEffectTarget` carry `CellHistory`. System
clients and signed effect lanes/requests bind the logical cell, so normal physical
group reassignment preserves identity and a fresh cell incarnation separates it.
Canonical marker-1 records omit physical groups and use distinct signature/lane
and compiled-class domains. Old group-bearing payloads and old signatures refuse;
there is no compatibility reader. The temporary old host projects its actual
logical binding into the same command/effect interfaces.

The new real three-voter tests use two cells and production image preparation in
both storage modes. Callbacks see the exact cell history and persist alarm state.
A's receipt survives B advancing group progress; duplicates do not run callbacks.
Wrong contract, input digest, operation and schema produce their specific typed
errors even on duplicate requests, without retaining cell admission. Unit cases
cover placement-independent signed identity, new-incarnation separation, and old
wire/domain refusal. This does not qualify live transfers or shared effect dispatch.

All 295 selected Rust cases pass: 256 native cases across twelve suites, 30 Cellar
library cases, five pure cell-group core cases and four LTX image-store cases.
The native gate includes the existing alarm/outbox/inbox, extent-host and host
regressions. All seventeen commands pass, including all-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,818 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases. The retained checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered/checkpoint.json`, binding 221 source
inputs and fifteen selected executables. Failed compile, missing test delimiter,
Clippy type-alias and omitted test submission-transition attempts are retained
with the corrected targeted runs. Six process helpers run through parent tests;
three RustFS-specific cases remain unrun. The branch contains latest fetched
`origin/main` at `8a93ffd7`.

CG-04 remains IN_PROGRESS and CG-08 remains TODO. The most critical next gap is
registered query context and version observations, followed by catalog/SDK routing
into these workers with original deadline, fingerprint and retry budgets. Query
contexts still carry the old committed group version; never invent a cell sequence
from that index. Production cache/retry and configuration handling, process-kill
cuts, shared effects and later snapshot/archive/RustFS/performance gates remain.
The opportunity is to complete one dispatcher over the existing consensus, SQL,
read and recovery owners, then remove the superseded production host.

## CG-04 physical apply faults and selected-image recovery

Qualified source: `1ec2eeaab2beaa8d8e82c834cbb5dbf692b5e7d1`.

`CellGroupApplier::recover` reconstructs only the engine-selected complete root.
It verifies every selected physical image, logical binding and geometry, and
retains the same manifest-page owner for subsequent apply. Materialized IDs use
the existing deterministic derivation; filenames supply no authority. Extent
roots verify the full cell history and schema. Missing or corrupt selected closure
refuses. Startup must still consume the catalog permit and establish current-term
quorum before serving.

Six new native cases exercise both storage modes. At actual image and manifest
directory-sync cuts, cancelling preparation and then cancelling close retains
worker, budget and completion ownership. A replacement pool refuses until cleanup
joins; exact preparation can then retry. Injected directory-sync failure preserves
the old selected root and releases preparation reservations. Cold restart closes
all prior engines, stores and execution domains, selects the published cell A,
ignores prepared but unpublished B, and replays B from the durable committed log.
It refuses serving authority without fresh quorum and refuses a missing selected
image directory without changing the marker.

This is joined cold restart, not whole-process kill or power-loss qualification.
The first recovery run hit an ownership refusal because the fixture still held
engine-owned manifest pins. Closing the complete owner before reopening fixed the
fixture; ownership rules and assertions were preserved.

All 260 selected Rust cases pass: 233 native cases across ten suites, 23 Cellar
library cases and four LTX image-store cases. Fifteen commands pass, including
all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied, formatting
(1,815 Rust files), layout, dependency isolation, docs, source preflight and all 37
plan-validator tests. The partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-apply-recovery/checkpoint.json`, binding 205
source inputs and twelve copied executables. Intermediate failures and passing
targeted runs are retained. Six process helpers run via parent cases; three
RustFS-specific cases remain unrun. The branch contains latest fetched
`origin/main` at `8a93ffd7`; the requested MR-13/MR-14 JSON files remain excluded.

CG-04 remains IN_PROGRESS. The most critical gap is the registered SDK/catalog
dispatcher: callback contexts still expose legacy cell-scoped group identity.
Next carry logical cell identity and selected-version observations through command,
query, receipt and effect validation, then use the existing shared worker path with
original deadlines, request fingerprints and retry budgets. Production cache/retry
lifecycle, configuration-entry handling and process-kill cuts remain open. Group
snapshot/compaction, RustFS recovery, performance and public cutover remain later
gates. The opportunity is to connect the existing owners, reads and recovery into
one production dispatcher rather than build another runtime.

## CG-04 committed image preparation on shared workers

Qualified source: `c9100ec774d66f30c14eae009f9a9c76d2bc0c33`. Runtime implementation: `2085cc96`.

`CellGroupApplier` owns the same manifest-page store used to verify startup. It
checks the exact persisted committed entry, engine, group and preceding root
before preparing cell-addressed images. Enrollment and mutation work support
materialized and extent storage, derive bounded alarm/outbox hints from the actual
SQLite image, preserve logical versions and exact commit witnesses, and refuse
malformed scheduling state. Membership/topology administration remains refused
pending its owner integration.

`CellGroupSqlPool::prepare_apply` uses the existing fixed SQL workers and their
reserved progress lane. Apply has a separate byte/item budget, so retained SQL
producers cannot consume its admission. `CellSqlLimits::node_bytes` bounds each
of the producer and apply budgets independently; account for their combined
ceiling. Usage exposes both budgets separately. The returned preparation retains
its reservation across clones, delivery and owner completion. Completion queues
publication; only durable owner apply establishes the new selected root.

Materialized duplicate preparation now reopens the exact verified image through
`GenerationStore::pin_prepared`. It does not reset retirement or infer publication.
Cold images require independent byte verification and persistence barriers.
Regression tests cover restart, corruption, mismatched expected state, reader pins
and reclamation. Both real three-voter cases cover enrollment, mutation, derived
hints, forged/foreign/stale work, duplicate preparation, saturated command admission,
old-root selection before publication and retention during a held log sync.

All 254 selected Rust cases pass: 227 native cases across ten suites, 23
Cellar library cases and four LTX image-store cases. All-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,812 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-apply-final/checkpoint.json` binds 202 source
inputs, 12 copied executables and fifteen successful commands. The failed gate
at `CG-04-apply` is retained: the newly inlined apply branch overflowed an existing
recovery fixture's stack. The branch is now heap allocated; normal stack limits
and assertions remain unchanged. Earlier fixture, materialized retry and Clippy
failures are retained too. Six process helpers run through parent tests; three
RustFS-specific cases remain unrun.

CG-04 remains IN_PROGRESS. The most critical next validation is cancellation,
shutdown and crash injection while this new image/manifest preparation is held;
the held publication sync does not qualify those earlier cuts. Then integrate the
registered SDK/catalog dispatcher with logical cell identities and observations.
Production retry/cache lifecycle and configuration-entry integration remain open.
No complete shared-group snapshot, compaction, RustFS or performance claim is made.

## CG-04 durable selected-version commit witnesses

Qualified source: `ed84df5daa0ae65fdfb09e9556f21f3e1ea12ec2`.

Authenticated cell manifest pages now require a complete selected-version
`CommitWitness`: original group history, term/index and proposal identity. The
local installation/mutation position remains separate for future group transfers.
Marker-1 pages without this required field refuse; there is no legacy decoder or
proposal inferred from later group progress. Shape checks bound IDs, namespace,
positions and initial enrollment identity. Ordinary update preparation requires
the current group and exact mutation position; the physical Raft owner additionally
matches the witness to the actual committed entry's proposal before publication.
Repeated enrollment and unchanged-cell progress preserve the selected witness.

Immutable-query results expose their exact `CellVersionObservation`; committed
receipt results expose the selected-version observation, which can cover an older
command receipt. Both use the authenticated leaf rather than the entry cache.
Native tests reject a forged witness despite otherwise correct apply/image data,
preserve older pinned observations after a newer cell mutation, round-trip signed
tokens, and recover exact witnesses after restart and image collection in both
physical image modes. Codec tests reject missing/malformed witness fields.

All 214 selected Rust cases pass: 193 native cases across eight suites and 21
library cases covering SQL workers, receipts, budgets, options and manifest pages.
All-target Clippy for Cellar/cellar-logic with warnings denied, formatting (1,806
Rust files), layout, dependency isolation, docs, source preflight and all 37
plan-validator tests pass. The retained partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-witness/checkpoint.json` binds 194 source inputs,
nine copied executables and thirteen successful command logs. Pre-commit compile
and formatting logs are retained separately. Four process helpers run through
parent tests; three RustFS-specific cases remain unrun.

CG-04 remains IN_PROGRESS. This proves witness persistence and observations at the
owning storage/apply/read boundaries, not actual log compaction, group snapshot
installation, registered SDK serving or S3 recovery. Next integrate registered
callback identity, query/version-token dispatch and catalog invocation sessions;
complete the production apply adapter, retry/compaction and cache lifecycle.

## CG-04 read-pin capacity and repaired qualification

Qualified source: `3da4481e09c3767face30469ffc855d302a7c142`.

Read-image retention now uses `RaftRuntimeOptions::max_read_pin_bytes`, independently
of transport request capacity. The default remains 64 MiB. Node and group read
reservations use the explicit pin ceiling; ordinary peer/apply admission remains
separate. Validation bounds configured capacity by the native image-size envelope
and pending-read count using wide arithmetic. The new unit case admits the full
native image limit without raising transport limits, accepts the minimum metadata
reservation and upper envelope, and refuses out-of-range settings. The native
read fixture now uses `cellar::asyncrt::select!`.

All 204 selected Rust cases pass: 186 native cases across seven suites, 11 SQL
queue/extent-worker tests, four receipt tests, two budget tests and one runtime
configuration test. All-target Clippy for Cellar/cellar-logic with warnings denied,
formatting (1,806 Rust files), layout, dependency isolation, docs, source preflight
and all 37 plan-validator tests pass. The checkpoint at
`.cellar/cell-groups-evidence/CG-04-reads-final/checkpoint.json` binds 194 source
inputs, eight copied executables and twelve successful command logs. It retains
the failed full gate at `88c4717d` and earlier targeted compile/stack diagnostics
separately. No test limits or assertions were relaxed. Four process helpers run
through parent tests; three RustFS-specific cases remain unrun.

This qualifies the low-level coherent-read and immutable-query boundary in both
image modes. CG-04 remains IN_PROGRESS. Registered SDK/catalog dispatch, callback
identity and version-token integration, real compaction/retry dispatch and cache
lifecycle remain required. In particular, the selected leaf retains its last
mutation position but not the corresponding mutation proposal ID: preserve a
complete authenticated cell commit witness before issuing SDK version observations
from that image after log compaction. Group progress is never that witness.

## CG-04 coherent cell reads and immutable SQL queries

Implementation source: `88c4717d`.

The shared node runtime now owns bounded read requests with original absolute
domain deadlines. Linearizable requests use unique nonce/counter contexts and
current-term ReadIndex, then wait for contiguous group apply before pinning the
target cell. Raw or forwarded read requests cannot borrow those internal contexts.
A minority leader cannot substitute a cached commit position. A read of unchanged
A waits behind committed B while retaining A's own version. Stale/AtLeast can
select committed follower images; AtLeast compares logical cell history and
sequence/image, never unrelated group indexes.

Separate node/group admission retains waiting reads and delivered pins. Image
selection grows the existing item reservation to its logical byte size without
consuming ordinary peer/apply capacity. The pin is consumed once by an immutable
query on the existing SQL workers. Queries can coexist with an unresolved writer
and read only committed images; an older pin remains coherent across a later
publication of the same cell. SQL output retains its own reservation through
borrowed result accessors. Wrong engines, routes, histories, expired deadlines
and revoked strong-read terms refuse.

SQL progress hooks check the domain deadline, waiter cancellation and shutdown.
Callbacks retain worker and pin ownership until they return; callback panics are
contained and close joins entered work. Deadline checks also cover queued replies,
so a ready result cannot bypass expiry when the receiver resumes late.

Both image modes pass targeted strong-read and lifecycle cases. The first native
compile used an unavailable clock accessor; fixtures now run clock reads inside
the owning domain. Expanding the strong cases with a pending AtLeast read and
an older same-cell pin exposed unnecessary async forwarding layers overflowing
the default fixture stack. Reexports remove those layers; a visibility error in
that intermediate change was corrected. The enhanced cases pass on the normal
stack without weakening assertions.

The retained gate at `.cellar/cell-groups-evidence/CG-04-reads/gate-commands.json`
completed four test commands: 203 selected Rust cases passed. The fifth command,
all-target Clippy, failed because `tests/support/cell_group_sql_reads.rs` uses the
forbidden `tokio::select!` macro. Subsequent commands in that gate did not run.
This source was unqualified at that point; the repaired source and full gate are
recorded above. No task status advances.

The repair required replacing that fixture macro with
`cellar::asyncrt::select!` and separating the logical read-image pin budget from
`RaftRuntimeOptions::max_request_bytes`: the current transport ceiling is 512 MiB,
whereas valid native images can reach 10,000,000,000 bytes. The repair above adds
the explicit bounded pin-byte option and validation test, preserves node/group accounting, count limits
and cancellation ownership, and retains this failed gate separately.

Registered SDK dispatch, callback identity and version-token integration, catalog
sessions, production retry/compaction and cache lifecycle remain required. This
is partial CG-04 implementation, not the production cutover or release gate.

## CG-04 prepared submission and rejection cleanup

Qualified source: `0cf89c403f4a500d18116499b8fc0f219195cd54`.

A prepared SQL attempt now carries its engine, group and preparation term, with
one submission decision shared by all clones. Exact accepted retries return the
original provisional position/digest, including after apply. Only an actual
owner's definitive pre-append capacity refusal seals the preparation and returns
a rejection proof. A queue overload remains uncertain. A lost rejection response
can be recovered from the local seal without contacting a closed runtime.
The first prepared submission closes raw byte-only proposal paths on that owner;
an uncertain append observation fails the owner closed rather than permitting
another append or manufacturing a rejection proof.

SQL workers match rejection proofs to the exact preparation, not merely the
business proposal ID. Foreign engines, other cells and replacement producers
refuse old proofs. Cleanup closes the writer before releasing logical-name
admission. Queued submissions, prepared outputs and retained proofs keep their
original byte/operation reservations through cancellation and lost responses.
The real three-voter tests cover initialization and mutation in both image modes,
including a fresh preparation with the same proposal identity after rejection.

Intermediate failures exposed two fixture accounting issues. A one-slot runtime
could refuse a read-only status poll while retaining admitted peer work; the
fixture now retries that poll while pumping the cluster under its existing
deadline. Completed enrollment outputs then correctly exhausted the SQL pool's
four retained-operation reservations. The fixture now asserts that accounting,
drops the completed phase's outputs, and asserts zero reservations/bytes before
preparing mutations. Resource limits and rejection assertions remain unchanged.
All 197 selected Rust cases pass at this source: 182 native cases across seven
suites, 11 SQL queue/extent-worker unit tests and four receipt unit tests.
All-target Clippy with warnings denied, formatting (1,802 Rust files), layout,
dependency isolation, docs, source preflight and all 37 plan-validator tests pass.
The checkpoint at `.cellar/cell-groups-evidence/CG-04-submission/checkpoint.json`
binds 189 source inputs, ten successful command logs and eight copied executables.
It retains the initial failures, diagnostic backtraces and repaired targeted run
separately. Four process helpers run through parent tests; three RustFS-specific
cases remain unrun.

The two new cases raise the CG-04 contract to 12 required cases and the package
to 92. No task status advances. Registered SDK/catalog dispatch, coherent reads,
production retry/compaction integration and cache lifecycle remain required;
this checkpoint does not qualify the complete CG-04 or release gate.

## CG-04 bounded proposal acceptance and enrollment capacity

Qualified source: `446dd9057f10c8221122d0d3a9a21254adeb448f`.

The actual group owner now counts pending accepted creations alongside applied
cells before appending a new
enrollment. Existing cells continue accepting mutations at the configured limit.
Leader-local acceptance records bind a proposal to its exact bytes and original
position; retries cannot append a pending mutation twice. Changed bytes under the
same pending identity refuse, and independent proposals for a pending logical
name wait. Mutation bases are checked against the selected cell image. After
apply, callers resolve receipts or creation records instead of resubmitting stale
images; a refusal does not prove that the original request failed.

Acceptance retains a separate shared node budget until durable apply, role/term
loss, terminal failure or owner destruction. It retains no LTX payload or image pin and cannot
consume Ready/completion admission. A current-term applied barrier remains
mandatory before a new leader admits work, so inherited enrollments count even
when the previous process's pending map is gone.

The preliminary native run passed 25 cases and exposed a restart-fixture issue:
restarting only the prior leader leaves live followers' leader leases intact, so
its immediate pre-vote need not elect it. The fixture now restarts all voters from
their retained disks before requiring inherited apply. A later run exposed an
extra async constructor layer overflowing the fixture's default stack; that
layer was removed. A larger-stack diagnostic is retained only for diagnosis,
not qualification. It also exposed the fixture transport discarding overloaded
peer admission. The fixture now retains and retries those exact frames under its
existing deadlines, as the direct-owner fixture already does. All 27 shared-group
execution cases now pass on the normal stack.

All 195 selected Rust cases pass at the qualified source: 180 native cases across
seven suites, 11 SQL queue/extent-worker unit tests and four receipt unit tests.
All-target Clippy with warnings denied, formatting (1,800 Rust files), layout,
dependency isolation, docs, source preflight and all 37 plan-validator tests pass.
The checkpoint at
`.cellar/cell-groups-evidence/CG-04-proposal-admission/checkpoint.json` binds 187
source inputs, ten successful command logs and eight copied executables, with
intermediate failures and the larger-stack diagnostic retained separately. Four
process helpers run through parent tests; three RustFS cases remain unrun.

Four distinct admission cases are now required by the CG-04 executor gate,
bringing the package to 90 required cases. Their oracles are separate from the
existing SQL fairness and receipt-settlement cases. Registered dispatch, catalog
sessions, coherent reads, production retry/compaction and cache lifecycle remain
required. No task status or release claim advances.

## CG-04 shared-worker initialization and enrollment

Implementation source: `6e09bbc7`.

`CellEnrollmentAuthority` now binds an actual leader, current-term applied prefix,
route and logical cell name before a cell image exists. Initialization uses the
existing fixed SQL workers and admission budgets. One logical name reservation
covers commands and initializers, so a competing incarnation cannot bypass a
running or unresolved producer. Worker-owned SQLite initialization captures and
verifies its complete initial LTX image; followers apply those bytes without
running the callback. Root workspace preparation is shared with ordinary commands.

Prepared SQL outputs now carry their preparation term. `propose_at_term` checks
that fence on the real RawNode owner in the same turn as proposal admission.
Retained output cannot be appended under a newer term even after the original
node becomes leader again. The raw unfenced protocol API remains a verification
boundary; registered dispatch must use the fenced path.

Committed retries return the immutable creation record without opening SQLite or
rerunning initialization after later mutations. `CellEnrollmentState` observes
verified current-term applied creation state on followers as well as leaders.
Exact records prove commitment; missing/conflicting records retire old producers
only after a strictly newer applied term. Resolution closes the writer before
releasing name admission, while retained outputs keep their byte reservation.
Cancelled request and close waiters cannot release an entered initializer.

Four native cases cover both image modes: competing histories of one name,
cancelled initialization and exact resume, another cell committing while its peer
initializer is blocked, three independent cells under one physical group on each
voter, creation retries after mutation, conflicting creation refusal, cancelled
close and workspace release, follower resolution of absent/conflicting/committed
creations, and rejection of old prepared output after leadership returns.

All 191 selected Rust cases pass at `6e09bbc7`: 176 native cases across seven
suites, 11 SQL queue/extent-worker unit tests and four receipt unit tests.
All-target Clippy passes with warnings denied. The retained partial checkpoint
at `.cellar/cell-groups-evidence/CG-04-initialization/checkpoint.json` binds 184
source inputs, eight copied executables and the command logs. The gate stopped
at two formatting differences; that failure is retained. `82ae6877` changes only
line wrapping in those two authority checks. Successor verification is recorded
separately in `CG-04-initialization-format/gate-commands.json`: formatting
(1,798 Rust files), layout, dependency isolation, documentation, source preflight
and all 37 plan-validator tests pass. No Rust behavior changed and the Rust tests
were not repeated for line wrapping. Four process helpers execute through parent
tests; three RustFS cases remain explicitly unrun. No task status advances.

Configured group capacity policy, catalog routing sessions, registered SDK
creation/command callbacks, coherent reads,
production retry/compaction integration and preview/cache lifecycle remain open.
This is partial CG-04 implementation, not the final hard cutover or release gate.

## CG-04 receipt lookup independent from admission

Qualified source: `91c3ecfc020e0d42738e4a6395045eb8a44c51f9`.

Reconciliation now uses a read-only identity lookup that preserves sequence/floor,
request identity and result-allocation checks independently from capacity for new
clients or retained receipts. The command path still enforces both ceilings and
uses the same identity inspection; no extra client lookup is added to normal
admission. A full identity table can no longer leave an absent older-term
producer unresolved merely because it cannot admit a new client.

Both native image-mode fixtures fill the selected cell's identity capacity with
another committed client after leadership changes, then reconcile the original
absent request. The changed cell image forces actual receipt lookup. A subsequent
new-client command still refuses at the original limit; increasing its explicit
capacity allows a fresh command from the committed base. Separate cases replicate
bounded receipt maintenance through the real group and return CommandExpired for
the pruned original receipt, and CommandConflict for a different request that won
the same client sequence. They prove callback suppression, producer admission
release, retained-output accounting and a valid next command after the refusals.
Private SQLite tests preserve capacity admission, sequence gaps, floors and result
allocation bounds.

All 187 selected Rust cases pass: 172 native cases across seven suites, 11 SQL
queue/extent-worker unit tests and four receipt unit tests. All-target Clippy
with warnings denied, formatting (1,795 Rust files), layout, dependency isolation,
docs, source preflight and all 37 plan-validator tests pass. The checkpoint at
`.cellar/cell-groups-evidence/CG-04-receipt-lookup/checkpoint.json` binds 181 source
inputs, ten successful command logs and eight copied executables. The preliminary
four-case native pass is retained separately. Four process helpers execute through
parent tests; three RustFS cases remain explicitly unrun. No task status advances.

This remains partial CG-04 implementation. Concurrent initialization, coherent
reads, registered dispatch, production retry and actual compaction integration,
and preview/cache lifecycle remain required before its complete gate.

## CG-04 applied-barrier receipt reconciliation

Implementation source: `8a0cf640ae70f3280f02e281f81b55a8823205fd`.

The existing SQL pool now reconciles the exact pending proposal against the
original request identity in a selected immutable cell image. The new
`CellReconciliationAuthority` is issued from a verified durable current-term
applied group root, including on followers. It grants no write, strong-read or
S3 authority. A matching receipt may resolve from a later cell image without
requiring the original log entry. Its position identifies the inspected image.
A missing receipt releases the old producer only behind a strictly newer applied
term. Expiry/conflict remain distinct refusals; missing producers report uncertainty.

The worker validates engine, route, cell history and proposal identity, rechecks
pin freshness after receipt inspection, and closes the disposable writer before
releasing cell admission. Reconciliation uses reserved progress capacity; retained
receipt outputs keep their existing byte reservation.

Both image-mode tests use real three-voter elections and SQLite/LTX. They hold
the new-term apply on the old leader and reject premature reconciliation; advance
B again on the new leader while its first producer remains on the old follower;
then recover B's original receipt and retire A's absent older-term proposal.
They also cover same-term absence refusal, stale and foreign proofs, mismatched
proposals, unknown producer uncertainty, retained-output budgets, duplicate
callback suppression and reopening A from its committed base.

All 181 selected Rust tests pass: 170 native cases across seven suites and 11
SQL queue/extent-worker unit tests. All-target Clippy with warnings denied,
formatting (1,795 Rust files), layout, dependency isolation, docs, reviewed source
preflight and all 37 plan-validator tests pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-reconciliation/checkpoint.json` binds 179
source inputs, nine successful command logs and eight copied executables.
The initial fixture failure is retained: `MsgTransferLeader` is a local
administrative request, correctly rejected by the peer boundary. The corrected
fixture delivers the current leader's `MsgTimeoutNow` directive and drives the
real election, persistence and apply. Four process helpers run through parent
tests; three RustFS cases remain explicitly unrun. This remains partial CG-04
implementation; no task status advances.

Next, split read-only receipt lookup from new-client capacity admission and
qualify saturated identity tables, expiry floors and conflicting identities.
Then integrate real snapshot/compaction, production retry dispatch, concurrent
initialization, coherent query serving and preview/cache lifecycle. No full
CG-04, RustFS, performance or hard-cutover qualification is claimed.

## CG-04 fixed node SQL pool

Qualified source: `51dff0e5abec7c2355a80e84d1b2e3ae2504e3b1`.

`CellGroupSqlPool` now uses the existing fixed blocking worker pool and fairness
queue. Requests have typed logical cell keys; the old host adapter remains
separate during the staged cutover. The node creates no worker per enrolled cell.
A cell's admission lease remains with queued work, its physical callback and its
unresolved proposal, including when a caller cancels. A lost stage response can
resume the retained proposal without running another callback. Exact settlement
releases that cell; unrelated group progress cannot do so.

Node/group quotas reserve capture capacity before queueing. Once capture ends,
the reservation shrinks to the retained entry/result bound while keeping its
operation slot. Prepared-response clones and receipt delivery retain that charge.
A separate progress slot per worker permits settlement without needing ordinary
queue admission. Extent workspaces are created and destroyed on the admitted
blocking worker, so rejected requests cannot release their last physical workspace
on an async executor.

The pool validates the exact engine behind execution authority and claims an
exclusive SQL lease on that engine. Its cleanup task owns every worker join;
cancelling a close waiter cannot release a blocked callback. The engine lease
also survives through retained response admission, preventing a replacement pool
from bypassing live output reservations. Queue/stage/publication timers use the
existing node performance registry.

All 184 selected Rust tests pass: 168 native cases across seven suites, 11 SQL
queue/extent-worker unit tests and five budget tests. Both new pool cases use
actual three-voter replication and cover blocked/cancelled callbacks, independent
cell progress, group quota refusal, exact resume, duplicate receipts, engine
fencing, cancelled close and retained-response ownership. Four process helpers
are invoked by parent tests; three RustFS-specific cases remain explicitly unrun.

All-target Clippy with warnings denied, formatting (1,794 Rust files), module
layout, dependency isolation, docs, reviewed source preflight and all 37 plan
validator tests pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-sql-pool/checkpoint.json` binds 178 source inputs,
ten command logs and eight copied executables. Intermediate logs retain the
initial verification-export compilation error and direct-Tokio-select Clippy
failure, as well as passes preceding the final ownership refinements. Only the
final gate qualifies the committed source; no task status advances here.

Registered SDK dispatch, concurrent enrollment, strong-read serving, preview/cache
lifecycle and receipt reconciliation after leadership changes or compaction
still need integration.
Closing SQL does not prove an ambiguous submitted command failed; the owning host
must reconcile against committed receipts before admitting a new producer. CG-04
and the full hard-cutover goal remain incomplete.

## CG-04 per-cell SQL producer

Implementation source: `f3d48bdc`; qualified successor:
`f807b2f70e01e3cd8c741557889e1965fd14ec49`.

The synchronous `CellPrivateExecutor` executes one cell's private SQLite and
receipt transaction, captures its physical LTX update and encodes the actual
cell-addressed group entry. Materialized and extent-backed images share the
existing receipt admission, maintenance and transaction implementation. Physical
capture no longer requires a synthetic cell-scoped group identity.

The real leader issues an authority pin for the selected cell, route and term.
Another cell can advance without invalidating that base. Leadership loss, owner
failure or owner drop revokes the observation; the producer checks again after
capture. Settlement requires fresh authority, the exact committed entry bytes
and expected cell image/version. Duplicate receipts come from the selected
immutable cell image without another callback. An unresolved proposal remains
busy; closing its writer does not establish a failed proposal outcome.

The two new real-three-voter cases use the same client/sequence in two distinct
cells, commit B before A, refuse settling A with B's authority, then verify each
receipt and independent SQLite value on all voters. Both image modes also stop
the consensus owner while a SQL callback is blocked, then require a typed
Unavailable refusal and unchanged selected cell version after release.

All 133 selected native cases pass: 13 shared execution, 15 catalog, 15 active
storage, 21 prior execution, 50 rooted and 19 prior runtime cases. Four ignored
process helpers are invoked by their parent tests; three explicitly ignored
RustFS cases were not run. All-target Clippy with warnings denied, formatting
(1,790 Rust files), layout, native dependency isolation, docs, reviewed source
preflight and all 37 plan-validator tests pass. The partial record at
`.cellar/cell-groups-evidence/CG-04-cell-sql/checkpoint.json` binds 173 source inputs,
eight successful command logs and six copied executables. It retains preliminary
runs and the initial two-fixture Clippy failure; the successor removes only an
unnecessary result wrapper and borrow, without changing assertions.

The fixed SQL pool, fair per-cell admission, concurrent initialization,
registered SDK dispatch and coherent read serving still require integration.
The producer currently reopens after settlement and conservatively refuses
exact-entry settlement after compaction; receipt reconciliation, preview
retention and cache lifecycle must be integrated with the owning host. CG-04
remains IN_PROGRESS. No full cutover, RustFS drill or performance qualification
is claimed by this checkpoint.

## Earlier plan delivery and SQL draft handoff

The implementation package contains 16 dependency-ordered tasks, 86 required
cases, 14 invariants and nine qualification profiles. Each task specifies owning
paths, implementation steps, acceptance oracles and executable test commands.
The plan runner prints those commands and checks evidence; it does not execute
the implementation or provision infrastructure.

At plan delivery the working tree retained an unfinished per-cell SQL producer,
leader-issued execution authority, shared receipt helpers and tests for both
image formats. That draft has since been committed and qualified at the partial
SQL checkpoint above. The plan delivery itself advanced no task status or
runtime qualification.

Plan delivery checks pass: package validation, reviewed source preflight, all
37 validator unit tests and repository documentation validation (113 documents,
2,314 local references). `show CG-04` emits the full executor brief. The release
command correctly refuses incomplete implementation evidence. The branch
contains the latest fetched `origin/main` at `8a93ffd7`; neither requested
MR-13/MR-14 progress JSON exclusion differs from main.

## CG-04 attachment admission and main integration

Source: `aec4029e77124848dc6b155c098f8eec9ccdacc4`.

Attachment now reserves ordinary node capacity before catalog revalidation and
RawNode construction, retaining it through dispatch or failure. A public-runtime
regression holds a real vote's engine sync and fills the sole request slot; a
second attachment refuses with overload before claiming an owner. Releasing sync
allows the queued protocol work and shutdown to finish at the same limits.
The first fixture attempted the normal multi-peer pump with one slot and refused
transport overload before the target assertion; the final fixture isolates the
startup boundary without relaxing admission.

The branch includes `origin/main` at `8a93ffd7`. Its documentation consolidation
moves this package to `docs/design/multi-raft/cell-groups/`. Commands, links,
catalog entries, source-root discovery and the plan-only release-input exclusion
now follow that location. Validator tests exercise both the new design path and
historical `plans/` bookkeeping. Existing evidence retains its original source,
paths and hashes; moved documentation is not fabricated runtime qualification.

All 60 selected Rust cases pass: 11 execution, 15 catalog, 15 active storage
and 19 active existing-runtime cases. All-target Clippy with warnings denied,
formatting (1,786 Rust files), layout, dependency isolation, documentation,
source preflight and all 37 plan-validator tests pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-runtime-admission/checkpoint.json` retains
165 source inputs, eight command logs, four executables and intermediate failure
logs. The larger source inventory includes main's documentation/tooling changes.
CG-04 remains IN_PROGRESS; this does not qualify per-cell SQL integration, SDK
serving, real RustFS recovery, performance or the final hard cutover.

## CG-04 shared-node runtime

Source: `865f253da0a14d730b62f77b17cefc6d1497890b`.

One execution-domain task now multiplexes physical cell-group owners on each
node. It uses the existing engine, mailbox and admission primitives, with a
single in-flight persistence batch containing writes from multiple group
histories. Exact engine receipts complete the corresponding owner tickets.
Group turns and deferred transport/proposal work remain bounded; ticks coalesce.
An admitted transport frame is not a persistence or application acknowledgement.

Apply events retain admission through image preparation and physical root
publication. Completion requests have reserved capacity. Already-started physical
phases can finish when ordinary event delivery is full; one reserved settlement
slot permits releasing an apply reservation without admitting more ordinary
output. A two-slot test exposed the original circular wait, and the fix changes
phase progress rather than increasing the limit.

Shutdown closes admission and joins submitted engine work before releasing node
runtime ownership. Cancelling the caller's close waiter cannot release that
ownership. The existing runtime shares the terminal completion primitive with
the new worker; its behavior remains covered by its native regression suite.

All 59 selected Rust cases pass: ten execution, 15 catalog, 15 active storage
and 19 active existing-runtime regressions. Two process helpers are ignored by
direct execution and exercised by their parent tests. All-target Clippy for
`cellar`/`cellar-logic` with warnings denied, formatting (1,786 Rust files), layout,
native dependency isolation, docs, source preflight and all 37 plan-validator
tests pass. The retained partial record is
`.cellar/cell-groups-evidence/CG-04-runtime/checkpoint.json`, binding 85 source
inputs, eight command logs and four native executables. The initial two-slot
failure and successful intermediate repeats are retained without claiming a
source-bound intermediate qualification. CG-04 remains IN_PROGRESS.

This is the shared consensus/persistence scheduler. Per-cell SQL preparation and
fair admission, concurrent initialization, exact receipt settlement and SDK read
serving still need integration. The full CG-04 gate remains required. Complete
group snapshots, membership/topology, managed startup, RustFS qualification and
the final hard cutover remain unfinished.

## CG-04 physical consensus owner checkpoint

Source: `51e30eabde1ca76bf0b23963dd06d0e8130f8ee7` (successor to `0951440e`).
CG-04 remains IN_PROGRESS. The physical owner now attaches one real RawNode to
each group replica using the catalog's initial-member permit and a completely
verified selected root. The permit is revalidated before attachment. Each
GroupLog shares an exclusive owner lease; persistence requests retain it until
physical engine work finishes, even if the owner and its awaiting future are
dropped. A duplicate attachment cannot race an outstanding write.

Ready and LightReady persistence completes before dependent messages, committed
application or read barriers are released. Image preparation is an external
effect and cannot advance Raft apply. Completion must match the exact owner,
entry digest/position, preceding root, engine and page-store owner. The new root
is published through the existing engine transaction before advancing the
contiguous prefix. No-ops preserve all cell versions. ReadIndex output remains
a barrier, not SDK serving authority.

The local three-voter harness replicates three independent SQLite histories and
interleaved LTX updates. It holds both followers' first apply while two entries
commit, rejects foreign and incorrect-image completions, then restarts one
follower and replays its committed tail. The restart joins the engine; it is not
a process-kill or power-loss test. Separate tests exercise minority read refusal,
cancellation while a real log sync is held, and 40 versions with a backpressured
follower that later reaches the exact final SQLite bytes. A peer counter-boundary
regression rejects input before changing the owner's term or applied state.

Ready collection accounts for small-entry overhead and the two in-flight batches'
maximum entries. A saturated apply queue backpressures new protocol requests.
Ticks coalesce behind pending Ready work so a stalled sync does not accumulate
unbounded heartbeat/election messages. Large physical effect and phase payloads
are boxed, reducing the space paid by every lightweight event.

All 36 selected Rust cases pass: six physical-owner cases, 15 catalog cases and
15 active storage cases. The ignored process fixture is run by its parent crash
test. All-target Clippy for `cellar`/`cellar-logic` with warnings denied,
formatting (1,783 Rust files), layout, dependency isolation, docs, source preflight
and all 37 plan-validator tests pass. The retained partial record is
`.cellar/cell-groups-evidence/CG-04-owner/checkpoint.json`, binding 82 source inputs,
eight successful command logs and three executables. It is not a CG-04 DONE record.

The initial owner fixture compilation and pre-vote persistence
expectation failed and are retained. Pre-vote has no durable vote; the corrected
sync test holds an actual data append after election. An initial Clippy run
rejected oversized enum variants; the implementation fixes their layout without
suppressing the lint. That source-bound failed gate remains at
`.cellar/cell-groups-evidence/CG-04-owner-failed-0951440e/gate-commands.json`;
earlier uncommitted fixture failure logs are retained under the final checkpoint's
`intermediate/` directory and are not treated as source-bound qualification.

The existing shared scheduler and SQL worker still consume the prior single-cell
owner. Next, integrate this physical owner with those node resources, bounded
per-cell producers, fair admission, exact receipt settlement and coherent reads.
The complete CG-04 six-case gate remains required, including real enrollment
concurrency and callback ownership. Snapshot/configuration/topology requests
currently refuse; their complete-group protocols, managed startup, RustFS,
independent failure domains and final hard cutover remain unfinished.

## Plan delivery and working-state audit — 2026-10-29

The package has 16 tasks, 86 required cases, 14 invariants and nine qualification
profiles. Its contract/evidence validation and CG-04 brief generation pass.
Freshly fetched `origin/main` is `1be27d234d4674d4d014785c60cecbfe394bdab9`,
already an ancestor of the delivery branch. The supplied TiKV checkout still
matches the investigation pin. Neither excluded MR-13/MR-14 progress JSON file
appears in the branch diff.

There is additional uncommitted CG-04 owner work in this checkout. It is not
part of the reviewed baseline or completed evidence. The new execution test
target currently fails compilation: `GroupRoute` is ambiguous through glob
imports and the fixture calls the private `ImageCommitment::sha256` method.
The global preflight and its unit test refuse the changed `cache.rs`; the other
36 validator tests pass. Preserve this refusal until the implementation is
reviewed and a successor baseline is recorded. Do not reset those edits or
silently update hashes to make the handoff appear qualified.

In a separate checkout of `fde3e6f9` with this plan update and the three retained
DONE evidence directories, source preflight and all 37 validator tests pass.
The release command exits nonzero with `implementation tasks/evidence incomplete`,
as required. Repository docs validation passes with 869 local references. These
checks qualify the committed plan handoff; they do not qualify the dirty runtime
prototype. An initial export-based validation attempt had incorrect Git isolation;
its temporary local metadata changes were restored before this independent rerun.

The [handoff](handoff.md) separates this working state from the committed
starting point and lists the remaining CG-04 acceptance cases. No runtime task
is advanced by this plan delivery.

## CG-04 durable enrollment checkpoint

Source: `c187c47773fd22d4658de1d912b6096f92a8e26d` (successor to `9f937f22`).
CG-04 remains IN_PROGRESS.

Each authenticated cell leaf now requires the original creation operation,
logical cell history, schema and initial image commitment. Publication includes
that record in the same complete root as the current image. Mutations refuse a
changed record; sequence one must match the initial image and later versions
advance its LTX position. Page decoding rejects missing/foreign creation records
at marker 1. No old-layout reader or inferred enrollment is introduced. The
physical pin-tree budget includes the record's additional cell identity.

Ordered enrollment preparation checks the route, preceding complete root,
contiguous apply position and committed lifecycle phase before invoking a physical
image producer. Exact retries keep the latest image, receipts and cell version
and advance only the group prefix. This unchanged-root path reuses image/page
pins without page I/O and refuses quarantined stores. Engine publication still
requires the exact preceding branch and committed position.

Native recovery exercises actual SQLite/LTX and Raft Engine in materialized and
extent modes. It mutates a cell, restarts, reclaims the original image, then
replays enrollment without invoking the image producer. The latest values and
receipts survive another restart, while the neighboring cell remains unchanged.
Sealed/retired ranges, competing creations and invalid apply positions refuse.
Existing process-kill cases also verify creation identity after recovery.

The new page-wire tests preserve canonical JSON ordering when corrupting identity
fields, so format ordering cannot mask a missing identity check. A quarantine
fixture fails the actual manifest directory-sync barrier and checks fault
consumption. Its initial run selected an image-file-only fault and failed because
no page I/O was interrupted; that failed run is retained separately.

All 32 selected Rust cases pass at this source: 15 catalog cases, 15 active
storage cases and two page-wire unit cases. The ignored process fixture is run
by its parent crash test. All-target Clippy for `cellar`/`cellar-logic` with warnings
denied, formatting (1,781 Rust files), layout, dependency isolation, repository
docs, source preflight and all 37 plan-validator tests pass. The retained partial
record is `.cellar/cell-groups-evidence/CG-04-enrollment/checkpoint.json`, binding
79 source inputs, nine successful command logs and three executables.
The failed fault-injection run remains under
`.cellar/cell-groups-evidence/CG-04-enrollment-failed-9f937f22/`.

This is an ordered preparation/storage checkpoint. The existing `Replica` and
shared worker still consume the staged single-cell log/apply types. The next
implementation must connect physical `GroupHistory` and complete-root application
to that real consensus owner, then wire independent SQLite execution, fair
admission, receipts and reads. The six CG-04 real-Raft acceptance cases remain
required. No live shared owner, RustFS, load or hard-cutover qualification is
claimed by this checkpoint.

## CG-03 completion audit and CG-04 continuation

The audit checks the four CG-03 steps against retained evidence at
`0df009a24d88408a8efc40e356f794ca35513a95`:

- Paged directory tests exercise exact coverage, bounded discovery, conditional
  publication and recovery from lost replies using real conditional operations.
- Enrollment tests check pure creation identity and sealed/retired admission;
  the task explicitly assigns real SQLite enrollment to CG-04.
- Native member bootstrap exercises synced physical genesis, permanent claims,
  restart, unknown outcomes and refusal after claimed local records are removed.
  Storage boundary tests separately verify physical identities and inventory.
- Route tests check trusted-catalog confirmation, cached operation during catalog
  failure, original deadlines and attempt budgets, cancellation and command ambiguity.

The full required command gate, source/log/artifact hashes and dependency evidence
validate. CG-03 is DONE only at that catalog/routing/admission-preparation boundary.
The record retains 77 source inputs, 20 successful commands, three executables and
44 distinct Rust cases. It does not qualify live shared dispatch, network peer
authentication, managed startup, current-term serving authority or RustFS.

CG-04 now addresses original enrollment identity in the durable manifest, followed
by shared group ownership and execution. Its full six-case real-Raft execution
gate remains required; storage-only tests cannot substitute for that gate.

## Plan handoff at b3b7eabd

The package contains 16 dependency-ordered tasks, 86 required cases, 14 invariants
and nine qualification profiles. Each task supplies source anchors, owned paths,
implementation steps, exact test commands and acceptance oracles. CG-01 and CG-02
retain their historical DONE evidence. CG-03 remains IN_PROGRESS pending the
implementation executor's completion audit; CG-04 through CG-16 remain TODO.
This handoff does not advance runtime status merely because a plan validates.

The reviewed source is `0df009a24d88408a8efc40e356f794ca35513a95`. Existing routing
sessions now retain the original logical request, command identity, deadline and
attempt budget across retries. Relocation requires exact confirmation against the
configured trusted catalog; cached requests need no catalog read. Cancelled
lookup cannot refund an attempt, and a possibly submitted command retains its
ambiguous outcome. Catalog confirmation does not authenticate a network peer or
grant serving authority. Actual shared dispatch remains CG-04/CG-07 work.

The retained record at
`.cellar/cell-groups-evidence/CG-03-routing-final/record.json` binds 77 source
inputs, 20 successful command logs, three test executables and 44 distinct Rust
test cases to that revision. Its gate includes all-target check and Clippy,
formatting, layout, dependency isolation and docs checks. All 37 plan-validator
tests pass. The earlier Clippy rejection of a forbidden test macro is retained;
the final source replaces it with a permitted selector without suppressing the
lint. These are existing local qualification results, not a RustFS drill or a
shared runtime performance claim.

For this handoff, package validation, reviewed-source preflight, validator tests,
executor-brief generation and repository docs checks are rerun. The release gate
must still refuse incomplete implementation. The branch includes freshly fetched
`origin/main` at `1be27d234d4674d4d014785c60cecbfe394bdab9`; the supplied TiKV checkout
matches the investigation pin. The requested MR-13/MR-14 progress JSON files
remain excluded from the PR.

## CG-03 physical bootstrap and inventory checkpoint

Source: `5d0d8e5ede3b4efedc211c40122a1a26b43968b4`. CG-03 remains IN_PROGRESS.

Physical handles now carry `GroupHistory` independently of application cells.
Real-engine genesis atomically syncs the mapping, initial membership and complete
empty group root. The same persistence worker owns both typed storage paths,
quotas, sync and cache publication. Required mapping application domains refuse
old untagged records; an engine cannot mix legacy and cell-group application
families. Existing single-cell consumers remain staged until the final cutover.

The initial unmanaged-member bootstrap API verifies the current selected birth,
permanent node-storage pin and exact member claim after actual genesis sync.
Claims bind group history, birth, member, node, disk and engine instance. Unknown
confirmation grants no permit; same-identity restart reconciles it. Tests remove
one owned group's records while retaining the original engine identity, then
verify that the permanent claim refuses fresh genesis. A separate blank-engine
fixture exercises the node pin. Foreign nodes/catalog owners and retired routes
also refuse; managed admission still requires its later rooted fleet integration.

Physical inventory now captures every shared history, including unattached or
retained members, and an optional initializing slot. A domain-separated digest
binds all four physical IDs without a cell name. The bounded existing wire checks
the complete occupied count; an already durable pending slot counts once. Legacy
cell-scoped lookup cannot match a group-native row. Real-engine tests verify
inventory across restart and while genesis sync is held, including wire refusal
when the node count omits a slot.

All 216 distinct Rust cases pass across selected genesis, physical inventory,
membership, core, directory, catalog, manifest, host, snapshot and storage suites.
The shared manifest recovery fixtures now use actual cell-group genesis for both
image representations and process-kill cuts. The RustFS-specific snapshot case
remains explicitly ignored; child fixtures are invoked by their parent tests.
These results do not qualify a live shared-group host or independent failure
resilience. Source preflight, all 37 plan-validator tests, formatting, layout,
dependency isolation and repository docs checks pass. The dependency script's
initial missing-PATH failure is retained separately from its successful rerun.

All-target Clippy for `cellar` and `cellar-logic` also passes with warnings denied.
The partial evidence record is retained at
`.cellar/cell-groups-evidence/CG-03-bootstrap/checkpoint.json`, binding 76 source
inputs, 14 successful command logs and eight executables to the source revision.
This is a checkpoint, not a CG-03 DONE record.

A catalog bootstrap permit is not complete image recovery or serving authority.
The runtime still must verify the selected image/manifest closure, revalidate its
startup permit and obtain current-term quorum. Authenticated bounded redirects,
the shared runtime, managed fleet integration and CG-04 through CG-16 remain
required. No RustFS disaster drill, load result, performance improvement or full
CG-03/release qualification is claimed by this checkpoint.

## Plan handoff at 98aff95a — 2026-10-29

The package supplies the target architecture, protocol state machines, dependency
ordered executor briefs, exact test commands and source-bound acceptance gates.
CG-01 and CG-02 retain their historical DONE evidence, CG-03 is IN_PROGRESS, and
CG-04 through CG-16 remain TODO. The final release gate remains closed.

The branch includes freshly fetched `origin/main` at
`1be27d234d4674d4d014785c60cecbfe394bdab9`. The supplied TiKV checkout matches the
investigation pin `b3995dab28308f09cbb7c125bf32f789dbc203fe`.

Uncommitted physical bootstrap, replica-handle and permanent member-claim changes
remain in this checkout, including two new Rust files and native test changes.
They are preserved for the next implementation executor and are not qualified by
this documentation handoff. Review their full diff and test results before
committing them or advancing the source baseline. Do not restart this work from
the earlier metadata-only checkpoint.

Package structural validation passes: 16 tasks, 83 required cases, 14 invariants
and nine profiles. Working-tree source preflight correctly refuses drift in
`crates/cellar-logic/consensus.rs`; 34 of 37 validator tests pass, with the three
source-sensitive checks failing against that unfinished source. These failures
are not waived. The unchanged committed source is checked separately from the
draft runtime.

Handoff verification: a temporary read-only export of committed source
`aead5cc04a9788ab2f2ee5f3fba3d6468fde5b75` passes the source preflight function and
all 37 plan-validator unit tests. This export excludes the draft runtime and
does not requalify historical DONE evidence. Working-tree structural validation,
CG-03 brief generation, repository documentation checks and `git diff --check`
pass. `release` exits nonzero for incomplete implementation evidence, as required.
The requested MR-13/MR-14 progress JSON files have no diff from `origin/main`.

## Plan handoff at 462aa2eb — 2026-10-29

The package describes the complete replacement architecture, ordered implementation
briefs and source-bound qualification gates. CG-01 and CG-02 retain their existing
DONE evidence; CG-03 remains IN_PROGRESS and CG-04 through CG-16 remain TODO.
The next executor starts with physical group identity, real genesis/member-claim
integration and bounded redirects, then follows the task graph. This handoff adds
no runtime implementation or new runtime qualification claim.

The branch includes fetched `origin/main` at
`1be27d234d4674d4d014785c60cecbfe394bdab9`. The supplied TiKV checkout remains at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`. Existing local DONE evidence must
accompany a fresh checkout to validate those checkpoints. Final release remains
closed; a validated plan does not prove shared-group performance or recovery.

Handoff checks pass: package validation (16 tasks, 83 required cases, 14 invariants,
nine profiles), reviewed source preflight, all 37 validator unit tests, repository
documentation checks and `git diff --check`. The CG-03 executor brief renders
successfully. `release` exits nonzero as expected because implementation evidence
is incomplete. Runtime suites were not rerun for these plan-only edits.

## Historical plan handoff — 2026-10-29

This section records the state at `dcc52840`; the directory checkpoint below
supersedes its working-diff and source-preflight limitations.

The package validator passes: 16 tasks, 83 required cases, 14 invariants and nine
profiles. Task briefs include source excerpts, file scope, dependencies, exact
commands and evidence requirements. The TiKV investigation pin still matches the
supplied checkout. The branch includes freshly fetched `origin/main` at
`1be27d234d4674d4d014785c60cecbfe394bdab9`.

Uncommitted CG-03 directory work remains in this checkout: immutable range pages,
root publication and mutation journals, with native catalog tests. The observed
native run passes seven tests. This is not a complete CG-03 qualification and is
not included in a DONE record. Preserve the working diff for its next executor.

The source preflight correctly refuses the draft `catalog.rs` module exports
against the recorded baseline. Consequently the current-checkout validator suite
passes 36 of 37 tests; its actual-source preflight test refuses that drift. Do not
blindly refresh the baseline or mark CG-03 complete. Review and qualify the draft,
commit its source, then record a successor baseline with updated excerpt lines.
Birth/disk-claim integration and bounded redirects remain outstanding. The final
release gate remains closed.

## CG-01 — complete at the contract boundary

Qualified source: `ee7f4a8d2dec220904f5a6860041f12c14db1cf5`.

- Dependency-free cell/group histories, validated cell IDs, monotone counters,
  canonical routing hashes and collision-safe half-open ranges.
- Logical cell versions and signed tokens independent of physical log position.
  Normal transfer preserves history; disaster recovery changes incarnation.
- Materialized and extent commitment adapters with geometry validation.
- A bounded, domain-separated entry codec for cell-addressed mutations and typed
  enrollment, membership, checkpoint, seal/install/activate, park and retire.
  Enrollment carries replayable initial LTX bytes; decoding borrows payloads.

The [entry codec contract](../../../../crates/cellar/application/consensus/group_protocol/README.md)
documents wire layout and checks that remain the runtime owner's responsibility.
The contracts are exposed through the unstable verification surface. Production
consumers still use the old contracts until CG-07; legacy removal follows in CG-16.

The retained source-bound record is
`.cellar/cell-groups-evidence/CG-01-ee7f4a8d2dec/record.json`. It binds the tested
Git revision, source inputs, native test executable and all 13 command logs by
hash. The complete gate passed:

- All-target checks and Clippy for `cellar` and `cellar-logic`, with locked builds
  and Clippy warnings denied.
- All four required CG-01 cases selected individually by exact name, plus test
  inventory verification to reject an empty selection.
- All 7 native cell-group contract cases, 6 existing native Raft contract cases
  and 18 core library cases: 31 distinct Rust tests.
- Repository formatting, module layout, native dependency isolation and docs.

Tests include SQLite reconstruction from enrollment bytes, an independent wire
vector, truncated/oversized metadata, cross-cell/history refusal, token tampering
and transfer versus disaster identity. These are contract tests, not proof of a
running multi-cell group, a RustFS drill or a performance claim.

The earlier short-ID token-bound failure and intermediate logs remain under
ignored local evidence. The corrected regression passes. Code review also caught
and removed a root-only enrollment gap before the final gate: nonempty enrollment
must include its initial replayable LTX chain.

## Remaining implementation

CG-01 through CG-03 have passed their scoped task gates. CG-04 is IN_PROGRESS;
CG-05 through CG-16 remain TODO. Historical entries below retain the intermediate
checkpoints and their limitations.

The reviewed source baseline is the CG-04 enrollment checkpoint at `c187c477`.
A new executor must receive all three ignored DONE-task evidence directories
before validating those historical checkpoints. Current CG-04 source changes
remain subject to their own verification and recorded review.

No shared runtime, split/merge, hibernation latency, real RustFS recovery, measured
density or final hard cutover is qualified. The release gate remains closed until
all task and deployment evidence passes at one final revision.


## CG-02 — manifest pages in progress

The first storage slice implements a bounded immutable AVL manifest and an
exclusively owned filesystem page store. Leaves bind logical versions, geometry,
last mutation and effect summaries; local image filenames do not affect roots.
Updates compare the old cell version and copy only the changed tree path.
Page publication syncs files and directories and quarantines uncertain writes.

The new native storage target uses real SQLite/LTX and sealed generation pins.
All three current cases pass: path-copy bounds across 64 independently sealed
SQLite cells, unchanged image commitments and old-root reads; restart and
malformed/missing page refusal; conflicts/capacity, replica-independent roots and
injected page-directory sync failure. These are page-store checks; the complete CG-02
gate has not run. The first build failed on an unavailable error derive and a
follow-up build caught argument mismatches. The first running suite exposed a
missing link-aware metadata method in the fault fixture; it now delegates to the
real filesystem instead of weakening the page-store inventory check.

The engine root marker, image adapters for both representations, physical root
pins/reclamation and actual engine crash-cut tests remain outstanding. No
manifest can yet grant shared-group serving authority. The [component contract](../../../../crates/cellar/durability/raft/cell_groups/README.md)
records this boundary. Checkpoint logs are retained under
`.cellar/cell-groups-evidence/CG-02-manifest-pages/`. The CG-02 scope includes the consensus module's unstable
verification exports so the native tests exercise the actual component.

Clippy passes for the library and new native target with warnings denied. The
36 plan-validator tests, source preflight, native dependency isolation and docs
checks also pass. These checks do not substitute for the still-missing CG-02
engine publication, pin reclamation and both-image-mode qualification cases.


### CG-02 engine publication checkpoint

Complete-root preparation now checks each leaf against an actual image pin,
syncs the manifest closure and holds image pins through engine persistence. The
root binds group history, range/epoch, birth/schema, membership revision and the
exact applied position. Raft Engine atomically selects that root with log/hard
state and publishes its cache only after sync. Legacy image accessors refuse a
shared-group root, and normal apply cannot skip an index or change its route.

The local application record uses explicit marker-1 image/group domains; old
bare-image record shapes refuse. No disk migration or deletion is performed.
The existing image variant remains a staging contract until the public cutover.

New native tests exercise a stalled real-engine sync, restart, missing-cell-image
refusal and rejection of preparation from a previous engine instance. Five
manifest cases and the 19-case existing storage suite pass, including the
engine-instance check. All-target Clippy passes after boxing the larger metadata
variant; the failed lint output is retained. Final checks are recorded under
`.cellar/cell-groups-evidence/CG-02-engine-root/`.
The standalone storage child-process fixture is intentionally ignored and is
launched by its parent persistence tests.

Incremental apply preparation, durable root/pin reclamation, root-specific process
kill cuts and full extent-mode recovery tests remain CG-02 work. The production
shared-group host, topology and final release gates remain pending.


The engine-root checkpoint passes all-target Clippy, formatting and module layout,
24 directly selected native tests, dependency isolation and docs checks. The
reviewed source baseline is `4387fedd`. All 37 plan-validator tests pass. Updating
the baseline exposed a verifier bug: historical evidence was compared backward
against the moving review baseline. Evidence now covers every runtime change
since the frozen plan starting revision. A regression test ensures later baseline
reviews neither invalidate historical inputs nor waive earlier changed files.
This correction does not make CG-02 DONE or relax the final release-source gate.

### CG-02 incremental preparation checkpoint

Ordinary cell mutations now prepare against a complete prior root without scanning
or copying all cell pins. The physical pin tree mirrors the authenticated AVL
manifest, shares unchanged subtrees and copies changed paths/rotations. Each
mutation checks the changed image, expected cell version and next group position.
Page reads and allocations are logarithmic; lookup comparisons are bounded by
O(log² cells). Root clones are constant-time and memory admission charges the
complete owned tree, including node and cell-ID allocations.

The engine checks the exact base root, so a staged successor from a competing
branch cannot replace the selected branch merely because its index is next.
Identical already-selected roots remain retryable. Capabilities also bind the
page-store instance; reopening requires complete preparation. A coherent root can
return a selected cell leaf plus an independent physical image pin.

Native cases cover one update among 64 real SQLite cells, bounded page reads and
writes, competing-branch refusal, identical retry, page-owner replacement, and
96 incremental enrollments across AVL rotations. Reclamation checks keep the old
image while its reader survives, reclaim it after release, and retain all 64
selected images through the engine's root owner after caller handles are dropped.
The first incremental run exposed an overly strict retry base check; the corrected
run is retained beside that failure under ignored local evidence.

This is not the complete pin/reclamation gate. Page reclamation, authoritative
retention across uncertain engine outcomes, root-specific process-kill cuts and
full extent-mode recovery remain CG-02 work. The production shared-group host and
the full hard-cutover goal remain pending; CG-02 stays IN_PROGRESS.

Runtime checkpoint `b71f62b5` passes all 7 native manifest cases and 19 existing
Raft storage cases, all-target Clippy with warnings denied, formatting, layout,
native dependency isolation and docs checks. All 37 plan-validator tests and the
reviewed source preflight pass. The initial source-drift refusal correctly required
review of the changed cache anchor before advancing the baseline. Source and
executable hashes, failed/corrected test output and final check logs are retained
under `.cellar/cell-groups-evidence/CG-02-incremental-pins/`.

### CG-02 reclamation and uncertain publication checkpoint

Prepared pin-tree nodes now lease their physical manifest pages and keep the
directory lock alive. Bounded collection scans an explicit inventory window,
removes only unleased pages, and refunds page/byte quotas only after the directory
sync. Remove or sync failure quarantines the owner without speculative refunds.
Every reopened store requires root inventory reconciliation before collection.

The engine now reserves old/candidate closures before writing and selects the new
retention reference only after sync and cache publication. Dropping an armed
reservation on failure keeps both page/image closures and blocks collection until
recovery; pre-write refusals cancel unarmed reservations. The page owner retains
uncertain images even after request and engine handles disappear. Complete
recovery automatically retains a root only when it matches the selected engine
marker. Additional snapshot/archive/transition roots have an explicit bounded
retained set, independent of disposable caller handles.

The owning shared host must still supply the complete catalog/checkpoint/transition
inventory to `reconcile_retained`. This is a trusted owner contract; page filenames
cannot discover authority, and these storage fixtures do not qualify that future
host integration. CG-02 also still requires the root-specific process-kill matrix
and full extent-mode recovery qualification. It remains IN_PROGRESS.

Runtime checkpoint `f41f470f` passes 11 native manifest cases and 19 existing Raft
storage cases. New checks cover independent snapshot/transition retention,
collection during stalled apply, quotas after removal/barrier failures, and
two-group batch refusal before any write. A real failure injected after fsync
retains both images after request/engine handles drop; restart selects the new
durable root before reclaiming the superseded image/page.

All-target Clippy with warnings denied, formatting, layout, native dependency
isolation, docs, source preflight and all 37 plan-validator tests pass. The first
test build required owned fixture paths; the first lint run required replacing
an unnecessary clone used to construct a one-element slice. Neither fix weakened
an assertion. Source hashes, copied test executables and failed/corrected logs
are retained under `.cellar/cell-groups-evidence/CG-02-reclamation/`. This is a
storage checkpoint, not the complete CG-02 evidence gate or a RustFS qualification.


### CG-02 — complete storage qualification

Qualified source: `ec9d277e50416f786fe3c1cd851b4147f9cd4b43`.
The retained record is `.cellar/cell-groups-evidence/CG-02-ec9d277e5041/record.json`.
It binds 45 source inputs, 16 successful command logs and three copied native
executables. All five required cases were present in the inventory and each
passed when selected individually. The complete gate includes all-target checks
and Clippy, formatting/layout, dependency isolation, docs and 37 validator tests.

The 14-case manifest suite covers both materialized and extent-backed SQLite
images. Thirty actual child-process kills span image and manifest writes/sync,
engine sync, publication and reclamation. Recovery selects a complete old or new
root, checks both cells' independent values and receipt rows, and verifies that
selected data remains readable after orphan reclamation. Missing or corrupt
manifest pages, sibling images, extent roots and non-header extents refuse
complete recovery. Extent updates also preserve an unchanged shared tail extent.

All 19 existing Raft storage cases and the generation-store image sync regression
pass. The latter exercises five additional child-process cuts with the shared
fault adapter's original behavior. Standalone child fixtures remain intentionally
ignored by direct suite execution and are launched by their parent tests.

This completes CG-02 at its storage boundary. Process kills do not simulate power
loss. Receipt rows are physical-state checks, not SDK deduplication qualification.
The shared host must still integrate authoritative root inventories, admission,
quorum, snapshots and archives. No RustFS drill, split/merge, hibernation latency,
density claim or final hard cutover is qualified by these tests. CG-03 is next.


## CG-03 — enrollment admission in progress

The first CG-03 slice adds deterministic admission using the existing bounded
cell-addressed entry codec. Requests verify group history, route epoch, birth,
schema and the cell's half-open range. Sealed or retired lifecycle state refuses
delayed enrollment. Concurrent preparations must be checked again in committed
log order; only the selected original creation record can resolve a retry.

The bounded canonical `EnrollmentRecord` binds logical cell history, original
creation operation, schema and initial image commitment. It survives normal
transfer without binding to the old physical group. The host must retain it
atomically with enrollment, preserve it through mutations and include it in
snapshots/archives. Returning an existing record grants no quorum/read authority
and must never restore the initial image over newer state.

Three native admission cases pass: creation conflict/retry and route identity,
sealed/retired refusal, and bounded canonical record decoding. Negative record
checks preserve field order so rejection cannot be attributed only to JSON map
reordering. Empty records require the canonical empty image and zero checksum.
The initial build required explicit image wire fields; an intermediate running
suite exposed the absent empty-image checksum check. Failed and corrected logs
are retained with the checkpoint rather than suppressed.

This is a pure admission boundary. The paged range tree and conditional namespace
root journal, lost birth/root response reconciliation, permanent blank-disk
refusal integration and bounded route discovery remain CG-03 work. CG-04 must
connect admission and durable creation records to the real shared group host.
CG-03 remains IN_PROGRESS; the production hard cutover is not complete.


Enrollment checkpoint `b1d27582` passes its three native cases, all-target Clippy
with warnings denied, formatting/layout, native dependency isolation, docs,
source preflight and all 37 plan-validator tests. Evidence under
`.cellar/cell-groups-evidence/CG-03-enrollment/checkpoint.json` binds 40 source
inputs, nine successful checks and a copied native executable. The failed initial
build/test outputs and a dependency-check environment failure remain retained;
the latter passed after restoring the Cargo path. This is a partial checkpoint,
not a CG-03 DONE record.

### CG-03 range-directory checkpoint

Source `a0c11969763edbe86ee08fc7d1650129058c8ee0` adds bounded immutable AVL range
pages, path-copy replacement, a conditional head selecting the exact mutation
journal and operation/group indexes, and permanent genesis pinning. Historical
retry confirmation survives later root updates. Duplicate IDs, stale parents,
changed coverage, wrong epochs, reused operations and missing/corrupt selected
dependencies refuse. Retired group IDs remain reserved. A cached discovery path
does not refresh S3 metadata or list the namespace.

All eight native cell-group catalog tests pass. These include concurrent CAS,
lost response/restart, initialization racing a later mutation, 128-range bounded
lookup/path-copy checks, and 80 alternating split/merge metadata transitions. An
independent flat partition checks every resulting boundary plus unrelated cell
keys; original views remain readable throughout churn. The 13 existing native
fleet-catalog regressions also pass with the shared fault adapter. The adapter's
new barriers are inactive unless explicitly armed for directory operations.

The directory has no serving authority. Birth/disk claims, managed birth
authorization, deadline/hop-bounded client redirects and the real shared host
remain outstanding. No live split/merge, Raft quorum or RustFS qualification is
claimed. CG-03 remains IN_PROGRESS and CG-04 through CG-16 remain TODO. The full
hard-cutover goal is active; the earlier plan-only handoff does not satisfy it.

The partial record is retained under
`.cellar/cell-groups-evidence/CG-03-directory/checkpoint.json`. It includes source
and executable hashes, command output and the corrected initial fixture compile
failure. This checkpoint is separate from the still-incomplete CG-03 task gate.
The record binds 63 source inputs, nine successful command logs and two copied
native executables. All-target Clippy with warnings denied, formatting/layout,
native dependency isolation, docs, source preflight and all 37 plan-validator
tests pass at this checkpoint.

### CG-03 cell-independent birth checkpoint

Validation source: `40c3c0b143f72496f0345be890413ed855c241ae` (declaration-spacing successor).

Source `9db4ba8fc91e6d67de244c1a684001393bcc9e56` binds a group birth to physical
history, range/epoch, creation operation, namespace policy and canonical initial
members. It has no single-cell ownership field; range boundaries still carry full
cell routing keys. The bounded catalog encoding uses
the distinct kind 186; the provisional overlapping tag was corrected before this
checkpoint. No older birth layout is decoded or migrated by the new API.

Global group-ID reservation uses the existing permanent key and the entire
canonical birth. Different namespaces, incarnations, creation IDs or placements
cannot reuse that key. The range directory now verifies every new route against
its exact reserved birth before publication. Losing a topology CAS retains the
reservation. Selected ranges can remain unmaterialized until their first use.

Lazy materialization checks current range selection and the reservation before
conditional creation, then checks selection again. Unknown outcomes reconcile
with the original encoded request. A verified birth observation remains metadata:
it refuses wrong catalog owners, missing or changed immutable data and retired
routes, and grants no permission to initialize a voter. Managed namespaces refuse
the unmanaged APIs before I/O; their rooted fleet authorization remains to be
integrated.

All ten native catalog tests and all 16 existing node-directory regressions pass,
including the distinct wire-tag assertion. The new cases cover canonical placement,
lazy creation, missing confirmations after actual writes, exact retry after reopen,
global cross-namespace conflicts, retired-range refusal and managed-policy refusal.
Existing bounded routing, races and the 80-transition churn oracle still pass.

Physical `GroupBootstrap`, engine mappings and replica identities still carry the
old cell-scoped identity. The next step is to make that durable log path explicitly
group-scoped, then connect the verified birth to real synced genesis, node storage
pins and permanent member claims. A synthetic cell name is not an acceptable
adapter. The required `birth_unknown_and_claimed_blank_refuse` integration case
remains absent and CG-03 remains IN_PROGRESS. Redirect budgets, the shared host and
all later task/release gates remain outstanding.

The partial evidence record at
`.cellar/cell-groups-evidence/CG-03-birth/checkpoint.json` binds 67 source inputs,
nine successful command logs and two copied native executables. All-target Clippy
with warnings denied, formatting/layout, native dependency isolation, docs, source
preflight and all 37 plan-validator tests pass. The failed declaration-spacing
check is retained with its source revision and corrected result. This is not a
CG-03 DONE record or proof of shared-group disk-loss recovery.


### CG-03 physical engine registry checkpoint

Source: `d72ea068a527d2027e806ca78ab390256e8634fa`.

The engine catalog now keys histories by `GroupHistory`, excluding the application
cell name. Synced mapping records use the explicit `cellar-group-mapping-1` domain
and four physical identity fields. The old bare `scope` mapping shape refuses on
recovery; no compatibility decoder or disk migration is introduced.

The staged single-cell host retains its real cell binding separately as application
metadata. Lookup, create retries, seeded attachment, pending-slot accounting and
physical inventory reject a different cell binding for an existing physical
history. These checks prevent an adapter caller from turning another cell name
into a second handle or phantom slot. The inventory wire still uses its existing
cell-scoped digest until its consumers undergo the later cutover.

This is a storage prerequisite, not the full physical bootstrap conversion.
`GroupBootstrap`, replica handles, node pins and permanent member claims still
need the cell-independent path before the new birth API may start a voter.
The required claimed-blank-disk integration case remains absent. CG-03 stays
IN_PROGRESS; CG-04 through CG-16 and the final release gate remain pending.


Validation at this source passes 19 pure-core cases, 27 selected library cases,
all four genesis cases, 14 native manifest cases, 19 native storage cases,
16 node-directory cases, 10 cell-group catalog cases, 33 host cases and
54 snapshot cases. The new engine tests inspect the stored physical mapping,
restart the actual engine, reject cell aliases at lookup, creation, pending-slot
and inventory boundaries, and rewrite both catalog and group records to prove old-domain, unknown-domain,
zero-history and invalid-cell refusal. Existing managed genesis sync-barrier and
bootstrap claim tests still pass. The snapshot suite includes seeded restart,
lost-claim confirmation and selected-payload refusal; its explicitly ignored
RustFS case was not run. Parent-launched process fixtures remain intentionally
ignored by direct suite execution.

All-target Clippy with warnings denied, formatting/layout, native dependency
isolation, docs, source preflight and all 37 plan-validator tests pass. The partial
record is retained under
`.cellar/cell-groups-evidence/CG-03-physical-identity/checkpoint.json` with source,
executable and command-output hashes: 72 source inputs, 13 successful command logs,
eight executables and 194 distinct Rust cases. An interrupted build using the machine's
older default compiler and a command naming a nonexistent test target are retained
as non-qualifying attempts. Final checks use Rust 1.98.1 and actual test targets.
These results do not qualify the shared host, new group member claims, RustFS
recovery, hibernation, performance or the final hard cutover.
