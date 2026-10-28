# Fleet operations implementation progress

## Deliver failed connection observations before outer cancellation

Cell and node inspection now bound TCP connection attempts to half the remaining
call budget. This leaves time to deliver `Unreachable` before the host expires
the outer SDK call. Expired local admission remains unavailable without polling
the connection; successful TCP advances the inspection stage, so subsequent TLS
or metadata timeouts remain unknown health. Existing exact-boot, trust-revision,
authentication and cancellation checks are preserved.

The new stalled-dial regression fails on the prior behavior because the live
caller receives only its outer timeout. It passes with the bounded dial. All
12 selected transport/inspection tests and all 11 deployment-binary tests pass
on default native stacks with serial execution. Host library/tests/binary Clippy,
formatting and module layout pass across 1,729 Rust files. The managed endpoint
also reports per-boot detection health, failure counts, last errors and sample
ages, allowing a deployed stall to be located without granting health authority.

These results establish the local correction, not restored deployed RF3. The
next gate repeats the five-node crash campaign under foreground writes and
independently checks replacement membership, replica readback, archive catch-up
and terminal removal. The preceding failed campaign and retained storage remain
available outside build/runtime directories; performance, maintenance, capacity
changes and full fleet qualification remain open.

## Recreated fleet: writes survive a crash; automatic detection remains open

Source `b60e8f42aff238c50e548e9cb50425aca888703f` merges current `main` and
recreates the dedicated Colima deployment after cleanup. Its source-matched
Linux image reaches measured Active capacity on all five nodes, each limited
to 1 CPU and 3 GiB memory. All 15 managed cells form RF3 groups, with nine copies
per node. The original 1-GiB node extent limit and bounded filesystem policy are
unchanged. All 30 initial writes succeed, and all 45 replicas pass minimum-version
readback with archive-index catch-up. This verifies the idle-writer capacity fix
in the deployed path; it is not a TPS or latency qualification.

The owned recovery service starts, then the exact node-5 container is killed
while serial foreground counter writes continue. Another 780 commands confirm,
bringing every cell to sequence/value 54. One ambiguous response reconciles
using its original command ID and retry deadline; no command remains unresolved.
All 36 surviving replicas subsequently pass readback and archive checks.
However, the controller stays at catalog revision 52 and never starts isolation
or repair during the approximately 312-second campaign. The nine lost replicas
are not replaced. The fleet therefore survives this failure for writes but does
not yet demonstrate automatic healing.

A direct TCP probe from the controller to the stopped container's control port
also times out. The inspection transport and its outer SDK call share a deadline;
the host cancels expired calls before a timed-out dial can be delivered as
`Unreachable`. Closed-listener native tests exercise immediate refusal and did
not cover this stalled-connect path. The next delivery is a regression and a
bounded connection phase that leaves time to return the failure. TLS/metadata
timeouts, stale identity and local overload must remain unknown observations.

All four surviving native owners join shutdown with exit code zero and no
unfinished Raft work; the deliberately crashed node retains exit 137. Only the
seven exact owned fixture containers are removed. Every backing file, volume,
command journal and failed observation is retained, with primary records outside
both runtime data and the build target.

On the merged source, 20 selected native tests pass (13 SQL/private-execution
and seven startup cases), along with 12 managed-runner tests and ten plan tests.
Formatting and module layout cover 1,728 Rust files; native dependencies and
documentation checks pass. Full CI, restored RF3 under traffic, terminal removal,
maintenance/return, capacity changes, density, performance and soak remain open.
All nodes share one physical VM. The next deployment must repeat automatic
recovery after the stalled-connect correction before advancing its qualification.

## Measured admission and shared writer capacity

The bounded five-node deployment at native source `a4b0b4e8`, with the admission
runner correction at `40758734`, reached Active on every node using retained
kernel, native occupancy and filesystem evidence. All 15 managed cells formed
RF3 groups, with nine copies per node. Docker mount enumeration is unordered;
the runner now compares complete bindings by destination and rejects duplicate
destinations, changed mounts and changed limits.

The first write campaign exposed a native availability defect. One successful
write per node retained an idle private writer reserving 580 MiB of each 1-GiB
extent inventory. The sixth cell repeatedly returned `Overloaded` despite empty
SQL queues and zero archive lag. After background storage preparation finished,
the cached cell accepted a write in 38 ms while another cell on that node was
refused in 1 ms. These observations diagnose admission, not fleet TPS or p99.
All five native owners subsequently joined shutdown with exit code zero. Their
backing files, volumes, request identities and failed observations were retained
at shutdown. Those resources later disappeared during the fresh deployment retry,
as described below.

Source `abea384316a32d82340b8f91fcd2a86fd74145bb` releases an idle extent-backed writer when keeping it would leave
no room for another equally bounded writer. Pending proposals retain their full
reservation, and existing cache reuse continues with spare capacity. A native
regression failed with the original `Refused(Overloaded)` and now passes across
two SQL owners sharing one constrained inventory. It also checks active-work
reservation, committed reads and duplicate receipts. All 13 selected SQL/private
execution tests and 12 managed-runner tests pass. Documentation and native
dependency checks pass. Host library/tests and deployment-binary Clippy,
formatting of 1,700 Rust files, module layout and ten plan tests also pass.

The source-matched Linux image built successfully at `0388e2c4`. Its fresh
deployment lost the Colima SSH connection during the initial backing-file write,
before RustFS or any native node started. Subsequent inspection found the
dedicated profile, Docker context, backing directory and qualification/target
directory absent. The passing checks were observed, but their raw local artifacts
are no longer available. Recreate the lab and retain new primary evidence outside
the build target before repeating the deployed comparison; do not treat the
interrupted attempt as a write or recovery pass.

The fresh deployed comparison and recovery-under-traffic campaign remain the
next gates. This is progress through admission and managed RF3 formation; full
fleet qualification, throughput, latency, maintenance and scale operations are
not complete. Workload-aware leadership and desired capacity/resize remain open.

## Bounded deployment storage

Source `84e1b99a2c7a76e6767704dc2f3251564fa788fb` adds bounded deployment storage.
The managed runner now provisions separate persistent ext4 filesystems for
metadata, shared Raft log/snapshots, cell images/extents/cache, recovery replay
and RustFS data. It retains exact backing-file, loop-device and filesystem UUID
identities. Backing files are exclusively created and prewritten; an unknown
format outcome is never retried. Native containers use read-only roots and
bounded temporary filesystems and logs. The policy installs explicit snapshot
and extent-cache limits and charges all shared pools separately from group
reservations. Native validation derives the required WAL/capture workspace
reservation; a small image quota alone does not make a small inventory valid.

Fresh mounted Raft filesystems are now accepted even though their mountpoint
already exists. Native Create validation still refuses retained state. The
existing canonical admission-retry behavior is preserved.

The local verification includes eleven native deployment tests and 254 Python
tests passing, with one additional Python test skipped. Host tests/binary Clippy,
formatting, module layout, native dependency rules, documentation and ten plan
tests pass. A real 64-MiB Colima filesystem refused growth with ENOSPC and retained
previously synced data after remount. A separate RustFS probe successfully
created a bucket with read-only root and bounded data/log/tmp filesystems.
Earlier failed attempts and final evidence remain outside runtime data and the PR.

At that earlier storage-delivery stage, **the bounded five-node deployment was not yet qualified.** Its documented
15-cell configuration needs 19 GiB of backing plus a 4-GiB host reserve. The
retained host observation had about 8 GiB available; preflight refused allocation
before Engine mutations. That stage claimed no new Linux image or Ready/Active/RF3 recovery campaign. All five nodes in the previous idle startup fixture joined shutdown
with exit code zero. Only its seven owned containers were removed; volumes and
diagnostics remain retained. No managed native fleet is currently running.
Loop-backed volumes require explicit mapping restoration/verification after a VM
restart; this lab path does not implement retained fleet return.

Next priorities are:

1. Supply sufficient backing capacity, deploy the bounded profile, collect real
   platform evidence, admit Ready/Active capacity, and verify RF3 writes through
   detection, isolation, archive replay, repair and terminal removal.
2. Complete departure/return evidence, repeatable maintenance, second-failure
   and grace policy, and gated rolling maintenance.
3. Deliver workload-aware leadership and desired capacity/resize, then complete
   fault/density, matched TPS/p99 and soak qualification.

This step closes deployment storage and mounted-startup gaps. It does not close
the full end-to-end fleet goal or establish a performance improvement.

## Managed Colima startup and local fleet inspection

Source `7fa8f714fc0f12cb1f6c2a72e66d838df49bee3a` adds the executable
[managed deployment harness](../../../tools/perf/raft_fleet.md). It provisions
three to five fresh native containers and RustFS, bootstraps the managed catalog,
installs extent-backed namespaces, and exposes explicit capacity, managed-birth
and owned recovery operations. Operator requests are synced before dispatch and
retain their original identity and revision across unknown responses. The
initializer also reconciles its exact requests after later catalog changes.
The existing static benchmark mode remains available.

Ordinary local inspection previously attempted a forbidden self transport route,
blocking local capacity admission and repair resource collection. It now uses
the bounded host path with the same boot, trust-revision and delivery checks as
physical inventory. Native regressions cover local Ready/Active admission,
local repair candidates, stale revisions, retained replies, overload and shutdown.
The deployment wrapper also reconciles committed activation receipts before
requiring a live Engine or process; new activation still requires platform
evidence. A native regression reproduces the previous unconfirmed retry and
verifies that changed payloads remain refused.

**65 selected tests pass:** 23 native enrollment/capacity/recovery cases, four
inspection wire/authentication cases, eleven deployment-binary cases and 27
Python runner cases. Host tests/binary Clippy passes. Default native stacks,
timeouts and resource assertions are unchanged. The source manifest covers
2,368 build-context inputs; native cohort inputs remain unchanged through the
wrapper correction. Formatting, layout, native dependencies, documentation and
ten plan tests pass. The manifest validates 493 required cases and 117 source
anchors; this does not certify every planned scenario. Earlier failed fixtures and deployed attempts remain retained
outside runtime data. No raw evidence is included in this PR.

The fresh five-node RustFS deployment reaches Registered on all nodes, with
distinct admitted boot identities and zero histories. Live Engine inspection and
kernel cgroup reads confirm 1 CPU, 3 GiB memory, swap disabled and restart disabled per node. The
dedicated VM exposes 8 CPUs and 23,036,559,360 memory bytes; all nodes explicitly
share one physical host. This verifies managed startup, not capacity admission,
RF3 traffic, recovery, density or performance. The startup report keeps
`capacity_qualified=false` and `deployed_recovery_qualified=false`.
The final `managed-startup-v7` run uses image
`sha256:8ad00981a02f09ed515f2fd8750c03bee0b8e615daeb4574f1d4ac5146c80f95`,
with the committed source label and complete source-manifest match. All five
nodes in the preceding run joined with exit code zero before its fixture
containers were removed; their volumes and diagnostic records remain retained.

Next: establish the enforceable storage/memory envelope and retained platform
attestation, admit Ready/Active capacity, then run managed RF3 writes and
extent-backed automatic recovery/removal under foreground traffic. Shared-log,
scratch and retained-file bounds must be covered by the deployment resource
policy; startup configuration alone is insufficient. Intact return, simultaneous
failure/rolling/grace policy, workload-aware leadership, desired capacity/resize,
full CI, density, TPS/p99 and soak remain open. FO-10 and the full fleet goal are
not complete.

## Docker recovery resource inputs

`FleetDockerResources` implements movement and isolated-repair evidence for bound
Ready/Active Docker nodes. It checks fresh rooted capacity and authenticated
native occupancy, validates live container limits around an Engine memory sample,
charges cache, and retains digest-addressed evidence. Disk overhead and
resident/peak work remain explicit conservative deployment bounds; the embedding
runtime must enforce them. Missing or changed measurements refuse the attempt.
The provider does not activate nodes or supply departure/return settlement proof.

**982 distinct selected tests pass**: eight adapter cases, two real Engine cases,
one native binding/provider-interface case, seven recovery, two detection, three
movement-service, nine isolated-repair, four topology-service, twelve evacuation,
two maintenance settlement, two isolation, three removal, thirteen catalog and
914 other host library tests. Host/admin/logic all-target Clippy, formatting,
layout, native dependencies, documentation and ten plan tests pass. The audit
binds 1,791 Rust/Cargo/config inputs and retained executable/result hashes; the
source commit is `2990d411feb71ba45fb0e64111266bda2694469b`. The acceptance inventory
validates 486 required cases and 114 source anchors; this does not certify all
planned scenarios.
Evidence remains outside runtime data in `docker-resources-v2`.

The first verification correctly rejected a 3,221,225,472-byte configured memory
limit on a VM exposing only 2,053,578,752 bytes. The original real Engine case
preserves this refusal and its 3-GiB isolation/retention checks. A separate 256-MiB
container verifies positive live memory sampling and the same retention contract.
Both pass; no production guard or native deadline was relaxed. The failed probe
and measured effective limits remain retained in `docker-resources-v1`.

Next: runnable managed deployment startup in the order Ready, container binding,
Active, recovery service; then the complete five-node RustFS extent-backed
recovery/removal flow under foreground writes. Departure/return evidence,
simultaneous-failure and rolling/grace policy, workload-aware leadership,
desired capacity/resize, full CI and resource/TPS/p99/soak qualification remain
open. This milestone does not complete FO-06 or the full fleet goal.

## Docker isolation adapter

The `FleetDockerIsolation` provider binds an enrolled boot to a
single-use Linux container, retaining its immutable Docker Engine/container IDs
and start identity in create-only records. An independent container claim
prevents rebinding that resource to another boot. The recovery operation is
durable before restart-policy or kill requests; only current terminal inspection
and stored evidence can yield the isolation attestation. No container or volume
is deleted. The provider is for explicitly contained native workers under
exclusive deployment lifecycle control, with automatic restart disabled.

Six new adapter tests cover lost update/kill replies and controller recreation,
identity/start/policy changes, missing containers, immutable claims and operation
conflicts, cancellation and bounded responses. Those cases pass, together with
the native public binding case and a real Colima Engine case. The latter creates
a digest-pinned, network-disabled test container with limits of 1 CPU and 3 GiB,
writes a volume marker, pauses the container, verifies isolation and reads the retained
marker before test-only cleanup. A recreated adapter returns the same evidence.
It qualifies the Engine boundary, not a deployed Cellar/RustFS recovery fleet.
The test's default Colima VM has 2 CPUs and 2 GiB; the configured container limit
does not establish 3 GiB available capacity or five-node density.
The public binding case verifies real Ready enrollment, immutable retry and
refusal after evacuation; activation still precedes the evacuation transition.

**979 distinct selected tests pass**: the eight new boundary cases, seven
recovery, two detection, three movement-service, nine isolated-repair, four
topology-service, twelve evacuation, two maintenance settlement, two isolation,
three removal, thirteen catalog and 914 other host library tests. Host/admin/logic
all-target Clippy, formatting, layout and native dependency checks pass. The
audit binds 1,789 Rust/Cargo/config inputs and retained executable/result hashes;
the exact source commit is `5d48dbabf532a3a6cde24856eabab51615c27339`. Documentation,
ten plan tests and source preflight pass. The acceptance inventory validates
483 required cases and 112 source anchors; it does not certify every planned scenario. Evidence remains
outside runtime data in `docker-isolation-v4`.

Earlier failed probes retain the malformed fixture-ID, Ready-to-evacuation setup
and test-selector lint failures. Production identity, capacity, deadline and
resource guards were preserved. Combined deployment, extent-backed automatic
recovery, foreground writes, density, performance and full CI remain open. Next:
wire binding into deployment enrollment and install the adapter in the surviving
nodes' recovery services, then qualify the actual five-node RustFS workflow.
These services remain opt-in; this milestone does not complete FO-06 or the goal.

## Automatic terminal removal

The recovery worker now follows settled repair with a bounded complete-inventory
walk, fresh RF3 observations and physical archive replay. It admits isolated
Finalizing, renews the proof on the next phase and invokes the existing atomic
Removed/slot-release boundary. `FleetFailureRecoveryOptions` now requires an
explicit archive host, verifier and limits. Restarted workers resume Finalizing
without repeating Draining-only repair admission or external isolation.

A new regression on the predecessor reproduced the missing terminal transition:
one RF3 repair settled, then 281 successful worker turns retained Draining and
the disruption slot. The first implementation's complete-removal case passes.
An independent test polling adapter corrected observed catalog-admission
contention; operation deadlines and safety assertions are unchanged. The expanded
seven-case recovery cohort passes, including interruption, lost-response and
archive-failure cases. A recreated controller uses fresh replay resources and
renews runtime proofs after durable Finalizing. Archive failure retains the node
and disruption slot until replay succeeds; terminal acknowledgement loss is
resolved by the typed immutable receipt. Historical receipt reads preserve a
newly admitted operator disruption.

**971 distinct selected tests pass**: seven recovery, two detection, three
movement-service, nine isolated-repair, four topology-service, twelve evacuation,
two maintenance settlement, two isolation, three removal, thirteen catalog and
914 host library tests. Host/admin/logic all-target Clippy, formatting, layout
and native dependency checks pass. The audit binds 1,783 Rust/Cargo/config
inputs and retained executable/result hashes to source
`40d0ba86d1a747f9f498634f11c81ed4d692e3fd`. Documentation, ten plan tests and source
preflight pass. The acceptance inventory validates 475 cases and 105 source
anchors; this does not certify execution of every planned scenario. Artifacts remain outside runtime data in
`automatic-removal-v3`; earlier failed probes are retained.

The new automatic-removal cases use materialized SQLite images. Extent-backed
automatic removal, production isolation, simultaneous-failure policy, foreground
writes during recovery, density, TPS/p99, repeatability and soak remain open.
Historical qualification below applies only to its stated predecessor. This
milestone does not complete FO-06 or enable the service by default.


The full fleet operations goal remains active. **FO-01, FO-02 and FO-03 are
complete at their documented local adapter boundaries.** FO-04, FO-06, FO-07,
FO-08 and FO-10 are in progress; FO-05 and FO-09 remain pending. Healthy desired
placement has an explicit owned convergence service and authenticated operator
SDK/admin/CLI access. An explicit SDK controller now selects and executes
replacements after trusted node isolation. Operators can now register, qualify
and activate added capacity. An explicit SDK service now connects failure detection to durable exclusion,
trusted isolation, replica repair and terminal removal. Deployment recovery wiring,
grace/rolling/second-failure maintenance and resize remain unfinished.
The intact operator sequence now includes separate stop/return/resume grants,
read-only status and explicit Node-owned execution; successor qualification is
recorded below. Verified intact-return
resume closes the SDK capacity/slot boundary. Graceful
evacuation, explicit joined stop and terminal removal are now exposed through
the SDK, admin API and CLI. Audited cancel-and-restart recovery now resolves
stale departure proofs before stop acceptance.
The task manifest is the package-status source of truth. Current milestones are
recorded above; later sections retain predecessor evidence.

## Automatic failure admission and isolation handoff

The explicit Node-owned recovery service now connects fresh failure detection,
permanent exact-boot exclusion, trusted platform isolation and the existing
resource-bounded repair executor. A separate durable evacuation purpose prevents
it from adopting operator evacuation or maintenance. Root admission precedes the
platform callback. Controller recreation retains the original operation and boot;
lost external replies and catalog confirmations require reconciliation. Once the
intent is admitted, cancellation and replacement boots are refused because a
platform action may still complete. Physical storage and resource debt stay owned.

A full movement book previously suppressed every recovery turn: a retained
negative run retried two stalled moves for 286 rounds without collecting a single
failure observation. Recovery now uses retry windows even with both slots full.
Isolation adds no movement debt; repair independently checks the durable book.
The regression verifies unchanged reservations through isolation, recreates the
controller, settles both original moves and checks RF3 SQL continuity.

Two evacuation fixtures also failed on the retained pre-change executable. The
excluded-registration path now reports Unavailable when no admitted learner can
continue. A bare permit still fails explicitly with CommandConflict because it
has no reserved destination boot. The successful voter-replacement fixture now
uses actual capacity admission, resource reservation and physical settlement,
then retains its archive, graceful stop, concurrent write and terminal removal
checks. No quorum, resource, deadline or membership assertion was relaxed.

Source `e1eccc5c501a823a72f0b8cf9ed4332863b66485` integrates main
`ed42c8ed`, including replay budgets and durable root ownership. Merge resolution
preserves canonical-empty archives, independent extent verification, producer
image origin and lifecycle expiry even on replay memo hits. Empty replay reports
zero image work. New rooted-storage tests compare heads before/after operations,
including the namespace birth-inventory head; snapshot storage and retention may
neither create an archive head nor modify existing heads.

**1,128 distinct selected tests and the real daemon process campaign pass**:
four recovery, two detection, three movement-service, nine isolated-repair,
four topology-service, twelve evacuation, two maintenance settlement, two isolation,
three removal, thirteen catalog, 85 archive, 50 rooted-storage, 25 permit/import
and 914 host library tests. Three RustFS-only cases remain ignored because they
require isolated deployment credentials. The recovery cases exercise controller
loss, wrong-boot evidence, incomplete namespace coverage, manual-operation
isolation, lost responses and actual repaired RF3 histories. The isolation
provider is trusted fixture evidence after actual process join, not a deployment
driver.

The audit binds all 1,781 Rust/Cargo/config inputs, retained executables and
results to the source commit. The final continuation reran the changed rooted
suite and retained earlier passing cohorts only when a fresh Cargo build produced
identical executable hashes. Host/admin/logic all-target Clippy, formatting,
layout, native dependencies, documentation, ten plan tests and source preflight
pass. The 472-case acceptance inventory and 103 source anchors validate; that
inventory does not certify execution of every planned scenario. Artifacts remain
outside runtime data in `failure-recovery-main-v1` and `failure-recovery-main-v2`.
Earlier failed probes remain retained. A pre-integration check caught an overly
broad registration error mapping; its correction preserves the unchanged restart
continuation-conflict assertion. The daemon uses a custom harness: its successful
exit and exact campaign marker are audited separately from libtest counts.

Full CI is not established. Published predecessor `79c660fa` failed the Typed RPC
lifecycle workflow, and a push-triggered SDK job reported a Python initial-read
failure. Their causes remain unconfirmed; this selected native qualification does
not establish those Python lifecycle checks or new-head CI success.

Next: implement the exact-container Docker/Colima provider and automatic terminal
removal, then qualify under foreground writes. Probe fairness under sustained due
work or repeated turn expiry also needs qualification; five-second failure grace
is not a recovery-time promise. Second-failure/grace/rolling policy, maintenance
repeatability, workload-aware leadership, desired capacity/resize, full CI and
deployed resource/TPS/p99/soak remain open. This feature stays default-off and
FO-06 remains in progress. The experimental protocol adds `/failure-recovery-1`;
deploy matching binaries with fresh catalog data and numeric format markers at 1.

## Exact-boot failure detection

The explicit Node-owned failure monitor now observes other enrolled processes
over authenticated node-control RPC. It shares the movement worker's admission,
budget and joined lifecycle, gives admitted work priority, and rotates through
at most four probes per round with a 500-ms per-probe ceiling. The pure classifier
requires repeated fresh direct-connect failures spanning the five-second default
grace. Unknown outcomes, collection gaps and changed boots reset failure history.
Catalog errors invalidate observations; changed Root refuses batch publication.
Status reads age observations without requiring worker progress, and stopped
monitors report Unknown. Healthy denotes node connectivity, not Raft quorum.

This is the failure-observation component of automatic recovery. It does not
admit evacuation, mint isolation, change membership or release resources. The
production isolation adapter and durable transition into existing isolated repair
remain the next implementation boundary. Source `0f9b525a` passes **907 distinct
selected cases**: four pure classifier tests, 885 host library tests, two native
failure-monitor scenarios, three movement-service, nine isolated-repair-service
and four topology-service scenarios. The native tests exercise real closed-node
TLS failure, held object-store reads, unchanged catalog/membership authority and
owned shutdown. Existing services are covered because they share the worker.
Host/admin/logic all-target Clippy, formatting, layout and native dependency checks
pass. Frozen sources, executables and results are retained outside runtime data
in `failure-detection-v1`; the source inventory contains 1,722 inputs. This is local
functional qualification, not deployed fault detection or TPS/p99 qualification.
The public operation remains explicit and default-off. FO-06 and the full fleet
goal remain in progress.

## Request-local maintenance validation

Maintenance return, resume and status now carry their already-validated
predecessor records through the same request and immutable Root. Returned boot
confirmation still checks the canonical request and committed journal; resume
completion reuses the returned owner that its intent already verified. Status
can reuse the offline predecessor checked by a cancellation refusal. This
removes repeated historical validation without retaining evidence across calls
or changing physical, quorum, matched-progress, deadline or sample-age checks.
No public API, record format or protocol identity changes in this step.

The actual two-history CLI restart fixture exposed 247 object reads, including
11 reads of the signed shutdown proof, for one terminal SDK status request.
Task-scoped measurement excludes background trust refresh. The successor uses
150 reads and three shutdown-proof reads: 39.3% and 72.7% reductions respectively.
Its regression failed on the original implementation and now passes for both
image representations. Each scenario also verifies that a subsequent status
request refuses a missing signed closure or canonical boot request, even at
the same Root after a successful read.

Shared-host, unoptimized diagnostic samples measured terminal resume waits at
4.23–4.37 seconds before this change and 3.42–3.50 seconds afterward. Full CLI
terminal status took 394–397 ms before and 318–338 ms afterward. These small
samples establish reduced work in this fixture; they do not qualify production
TPS/p99, repeatability or larger fleets. Temporary diagnostics are removed.

An unchanged baseline execution retained an extent `Unavailable` failure. An
intermediate candidate also retained a five-second preparation wait failure
despite the service reaching Ready. Preparation and overall timing repeatability
remain open. Source `46202468` passes **946 distinct selected cases**: 881 library,
14 operator, four return, nine resume, 15 stop, 14 preparation and nine CLI cases.
Both complete CLI restart scenarios include the read-count and missing-record
regressions. Host/admin all-target Clippy, formatting, layout and native dependency
checks pass. The audit matches all **1,717 frozen Rust/Cargo/config inputs**,
retained executable hashes and result hashes. These are selected functional
checks, not a repeatability or deployment pass. Artifacts are outside runtime
data under `qualification/fleet-operations/` in
`maintenance-baseline-v1`, `maintenance-read-probe-v1`,
`maintenance-regression-negative-v2`, `maintenance-lineage-probe-v2` and
`maintenance-lineage-final-v1`. Earlier failed probes are retained too.

The preceding published head `8a2b64b6` also has a failing JavaScript/Node workflow
at `cellar-container`'s
`expired_owner_cannot_acknowledge_an_accepted_create_and_successor_cleans_it`:
the retained CI log reports `execution domain is closed`. Its cause is unconfirmed;
this local maintenance qualification does not resolve that separate failure or
establish full CI success.

Next: finish preparation/resume reliability within the existing bounds and qualify
larger history counts. The next feature delivery is automatic single-node repair:
conservative failure classification, production exact-boot isolation and restored
RF3 under foreground writes. Trusted isolation already supplied by an operator or
test is not automatic failure handling. Grace expiry, second-failure policy and
gated rolling maintenance follow that boundary; workload-aware leaders and desired
capacity/resize retain their full scope. Deployed RustFS/Colima qualification must
cover these transitions, resource limits and foreground TPS/p99. The fleet goal
remains active.

## Extent-aware fleet lifecycle and cold repair

Managed and imported extent namespaces now use tagged fleet publication state,
archive coverage and stored receipts. The 85-byte observation bound is unchanged.
Root archive heads retain the exact producer image origin separately from
configuration-only snapshot progress. A materialized digest cannot substitute
for a root commitment. The protocol suffix is `/extent-fleet-proofs-1`; deploy
matching binaries with fresh namespaces and coordination prefixes, keeping
numeric format markers at 1.

Independent extent verification requires a separate exclusively owned, initially
empty scratch inventory. It freshly downloads the selected remote closure and
seals SQLite. Entered physical jobs retain its lease after cancellation; retries
reconcile scratch only after those jobs exit. Missing material and expired work
issue no proof. Physical inventory quarantine still refuses recovery.

Dynamic managed registration previously selected materialized SQL storage
unconditionally. It now selects the namespace's actual representation before
publishing its binding. Empty managed extent hosts refuse missing physical policy
before claiming their boot. The management connection carries the original
immutable `image_storage` choice. Actual CLI maintenance, retained return,
redistribution and terminal removal now have extent scenarios.

Canonical-empty archives carry configuration without an image body. Cold extent
seeding now keeps the fresh grant, learner/barrier and exact-boot checks while
admitting that complete empty state. The cold-repair fixture now uses actual
capacity reservation and movement admission; its old bare permit correctly
refused managed placement. Both storage modes promote the learner and retire the
old voter before their first S3-acknowledged SQL command, with exactly one callback.
The pre-fix extent case refused activation; no safety assertion or bound changed.

Source `a4312bff4599bba8ad110614e9a467a4d9502a39` integrates main
`3ffbadcc5e4a7bf938771ee514c2205402e6379e`, including the bounded archive
physical-job scheduler and buffered extent imports. Merge resolution preserves
independent per-cell archive completion and the shared physical job limit.
The merged source passes **1,021 distinct selected cases**: 881 library,
35 native fleet, nine CLI, two extent-host, 32 rooted archive, seven extent seed,
17 admin-console, 13 catalog and 25 membership-permit cases. All-target
host/admin Clippy passes. Format/layout, native dependency rules, documentation,
ten plan tests, the 458-case manifest and preflight against 102 source anchors
pass. The manifest validates the acceptance inventory; it does not claim every
planned scenario has executed. The audit matches **1,717 frozen Rust/Cargo/config
inputs**, retained executable hashes and passing result hashes to that code
commit. The final cohorts execute the actual CLI restart, populated-image
movement/removal, namespace import, cold empty replacement and failed/cancelled
independent archive verification. Three RustFS-only cases are explicitly skipped.
Artifacts are retained outside runtime data under
`qualification/fleet-operations/extent-fleet-main-v1/`. These are selected local
functional checks; no full-CI, repeatability, large-fleet or deployment pass is
claimed. Earlier failures remain retained separately.

Earlier finalization cohorts exposed intermittent catalog confirmation and
five-second terminal-resume waits. The unchanged catalog case passed alone and
in its complete three-case archive cohort. A broader 14-case operator execution
retained terminal-wait failures in both representations; selected functional
cohorts subsequently passed under the same bounds. A temporary phase timeline
reached Complete at about 3.8 seconds and spent about 1.3–1.4 seconds in one
verification turn for two histories. It identifies limited headroom, not a
repeatability or density pass. Temporary instrumentation was removed. Earlier
failed runs remain external; final selected qualification is recorded below.

Next: measure and reduce verification/status work within the existing deadlines
and freshness bounds, then qualify larger fleets before rolling maintenance.
Grace expiry, second-failure policy, production isolation, workload-aware leaders
and desired capacity/resize remain feature work. No TPS/p99 gain or deployed
RustFS/Colima qualification is claimed. The full fleet goal remains active.

## Intact operator stop, retained return and terminal resume

The predecessor milestone at `43c080a1` extended preparation/cancellation through
separately granted stop, return and resume. The Node-owned controller retains bounded SDK
child references, confirms actual signed join, and preserves the original slot
and exclusion until fresh M9 terminal proof. A separate operator terminal CAS
then removes only its own active pointer. GET remains read-only; CLI waits for
actual offline confirmation, canonical retained-return context or full terminal
completion according to its command. The platform starts the retained process
separately under the exact M8 process identity.

A cancellation that loses to signed join is recorded as refused without releasing
resources; the same operation can continue through return/resume. Return admission
accepts the M7 proof before the next worker checkpoint. Incorrect unexecuted
resume-profile grants can be corrected before SDK consent exists; admitted scope
cannot be rebound. Failed live observations with unchanged metadata and no staged
mutation back off without Root churn. Maximum-size attestation data stays in its
own canonical grant; checkpoints retain references under the 4-KiB bound.

Main `cd9a9689` is integrated, including its extent-backed recovery changes.
Materialized canonical-empty archive and lifecycle deadline behavior are retained
behind the common representation interface. At that predecessor, fleet enrollment/import and namespace Root validation
explicitly rejected extent-backed images while ordinary extent recovery remained
available. The current extent milestone above implements this compatibility gap;
its larger-fleet and deployment qualification remain open.

The initial `operator-lifecycle-v1`/`v2` checks retained compile/lint defects.
`operator-lifecycle-v3` passed all-target host/admin Clippy and built native tests,
then the first new cancellation race aborted on a default-stack overflow. The
retained crash trace and generated instructions identify the combined lifecycle
poll frame (about 1.12 MB) nested under the driver frame (about 0.75 MB), before
caller frames. Stop, return and resume now use separate boxed phase futures and
checkpoint calls. No stack, timeout, retry or resource bound was increased.
`operator-lifecycle-v4` then passed both new cancellation races on default stacks.
Its full CLI case aborted earlier during initial fixture startup: the combined
test poll frame contributed about 0.67 MB beneath the host startup chain. The
test is now split into startup, stop, return and resume helpers, with the same
assertions and production calls. `operator-lifecycle-v5` reached the returned,
paused boot but its deliberately wrong digest reused the old capacity revision;
the SDK correctly refused it as PayloadConflict before live proof. The fixture
now uses a new intended profile revision, retaining the exact Conflict assertion
for the actual live mismatch. Later deadline probes are retained separately.
Service diagnostics showed Ready/Offline while the last HTTP snapshot was still
Verifying/Stopping; the fixture now polls the result concurrently with Root-driven
route refresh instead of repeating unchanged all-pairs probes. A controlled CLI
case independently becomes Ready at 850 ms inside a one-second budget: the old
fixed 500-ms poll returns WaitTimeout, while all nine candidate CLI cases pass
with shorter final-window delays and unchanged deadlines/submit-once semantics.
Temporary HTTP body tracing was removed. Maintenance audit SQLite work now runs
on the console-owned blocking domain before dispatch and through completion.
The next full run correctly refused a stale peer hint in the manually driven
resume walk; that fixture now refreshes current discovery before each Root turn
without bypassing StalePeer or the live five-second proof envelope. An earlier
source's one-off cancellation deadline remains unexplained despite an unchanged
binary passing its diagnostic run. These outcomes are not density or reliability
qualification. Stop retry investigation then identified `TransportError::StalePeer`
being collapsed into Conflict during local preflight. The controller interpreted
discovery lag as invalid Ready and produced new consent/checkpoint cycles.
Preflight now retains the typed observation error; actual scope/root/epoch
conflicts remain unchanged. The native RPC future-floor test checks the exact
stale observation, unchanged Root and live node before valid fencing.

Timestamped retained-binary tracing then showed progress through serialized leader
checks and the second history starting near the final CLI deadline, rather than
a permanent second-history deadlock. Restart and resume walks now memoize only
successful immutable catalog validation at the same Root revision. Every call
still reads Root and checks relevant pins; revision changes repeat full canonical
validation. Saved candidates omit the memo and reject injection. Physical state,
profile, quorum, matched progress, leadership and sample-age checks remain live.

The first memo candidate completed terminal work but its CLI observer missed the
outer deadline; the unchanged binary passed the complete restart on a diagnostic
run. An earlier resume cohort had one cleanup-result assertion failure whose cause
remains unproven; its unchanged isolated case and subsequent complete cohort
passed. Broader handoff qualification separately reproduced snapshot Overloaded in
the fixture's leader-polling helper after a requested transfer. That helper now
waits on typed backpressure within its original five-second deadline and still
requires an actually observed survivor leader. Other errors fail immediately;
the final observed-leader, data and proof assertions are unchanged. Stop cleanup
keeps its original accepted outcomes and now reports the unexpected error.

The subsequent broad operator cohort exposed a default-stack overflow during
fixture startup in the existing departure-cancellation test. Its retained native
crash trace and ARM prologues place about 538 KiB in the monolithic test poll frame
beneath the startup chain. Fixture construction now happens in the thin test
entry point before the unchanged cancellation/restart body. The previously
aborting case passes on the rebuilt default-stack binary. No assertion, stack,
startup, deadline or resource limit was relaxed.

Source `43c080a1` passes **1,054 distinct selected cases in 1,054 executions
of completed passing cohorts**: 826 library, 110 native fleet, nine CLI, two
ordinary extent-host, 32 rooted extent, 20 snapshot, 17 admin-console, 13 catalog
and 25 membership-permit cases. The final native binary passes the full intact
restart and both cancellation races, all nine existing operator CLI cases and
the selected restart/controller/service/stop/return/resume regressions. Three
RustFS-only cases are explicitly ignored; the in-process store and loopback TLS
supply local correctness evidence. Host/admin all-target Clippy and console
typecheck pass.

The audit matches all **1,678 frozen Rust/Cargo/configuration inputs**, the changed
console permission type, executable hashes and passing result hashes to the code
commit. Only completed executions of bit-identical rebuilt artifacts are reused.
Artifacts remain outside runtime data under
`qualification/fleet-operations/operator-lifecycle-v19/` and
`operator-lifecycle-regressions-v4/`. Earlier failed cohorts, diagnostic runs,
crash traces and prologues remain retained separately; timing repeatability under
shared-host load is not qualified. This milestone claims lifecycle correctness
and less repeated catalog work, not a TPS gain. Format/layout, native dependency rules, docs, ten plan tests, the 445-case
manifest and preflight with 102 reviewed source anchors pass. Repository
preflight is retained separately in `operator-lifecycle-checks-v1/`.

Next feature order: extent-aware fleet proof integration and scalable bounded
verification/convergence, then grace expiry, second-failure recovery, gated rolling
maintenance and production isolation, followed by workload-aware leaders and
desired capacity/resize. Empty/retired return and deployed fault/performance/soak
remain unqualified. FO-07/FO-08 and the full fleet goal remain in progress.

## Maintenance management and Node-owned preparation

Source `777f560f` delivers audited `prepare-maintenance`, `cancel-maintenance`
and `maintenance-status` commands through the management SDK, admin API and
shipped CLI. Preparation requires the independent `fleet_maintenance` grant;
cancellation requires `fleet_cancel`. Other operating, stop, capacity and
registration grants cannot admit preparation. Catalog, node, original process,
operation, request, revision and immutable birth choice retain their canonical
meaning across an unknown response. GET status reads one Root and never drives
work. Mutation waiting submits once and then polls only GET; cancellation of a
preparation returns blocked exit 4, while the cancellation command's own wait
succeeds only at its canonical terminal receipt.

`RaftFleetControl::start_maintenance_service` explicitly starts the default-off
preparation/cancellation worker. The enrolled Node job set owns it, selects its
installed catalog backend/domain and holds the bounded control budget. Independent
clients share one per-Node service slot. Node drain/shutdown cancels and joins it;
requester-domain loss does not terminate already-owned work. Recreated workers
recover Root intent and repeat ephemeral proof. Active rounds share one original
deadline, a soft quantum and at most sixteen complete steps, separately from
idle/blocked pacing. This service grants no stop, return, resume or capacity consent.

**955 distinct selected cases pass in 955 executions of completed passing
cohorts:** 785 library, 107 native fleet, 13 catalog, 25 membership-permit, eight
CLI and 17 admin-console cases. Five new cases cover actual preparation and
matched leadership, cancellation through the synced Fence/terminal receipt,
requester loss, independent-client singleton admission, Node shutdown during a
held Root CAS, audited unknown replay, permission separation, read-only queued
status and the cancelled-preparation wait result. Actual SQL remains usable.
Existing refresh/controller, stop/return/resume, scoped-stop, operator and
ordinary-boot cohorts pass on default stacks. Host/admin all-target Clippy,
console typecheck, format/layout, native dependency rules, docs, ten plan tests,
the 438-case manifest and preflight with 100 reviewed input anchors pass.

The earlier `maintenance-management-v3` cohort passed the shutdown case and failed
the requester-loss case with Overloaded. The unchanged-binary diagnostic located
it in the phase reader: the fixture shared the receiving host's serialized
catalog client. The fixture now uses independent reader/discovery clients,
matching the management adapter, without increasing retries, deadlines, stacks
or resource bounds. The CLI probe separately reproduced cancelled preparation
returning exit 0; the regression now requires blocked exit 4 after exactly one
POST and one GET. Initial lint probes were corrected without suppressing warnings.
These are separate from older-source Overloaded and quorum/dispatch causes that
remain unproven in their retained records.

Audits match all **1,592 frozen Rust/Cargo/configuration inputs**, the changed
console permission type and retained executable/result hashes to `777f560f`.
Primary artifacts remain outside runtime data under
`qualification/fleet-operations/maintenance-management-v4/`,
`maintenance-management-regressions-v1/` and `maintenance-management-checks-v1/`.
Earlier probes remain retained separately; raw evidence is excluded from the PR.

**Next priorities:** complete separately authorized maintenance stop, signed
offline confirmation, retained return and verified resume through management,
including operator-pointer terminal cleanup and cancellation races. Then deliver
grace expiry, rolling/second-failure recovery and production isolation, followed
by scalable live verification/convergence, load-aware leadership and desired
capacity/resize. Empty/retired return, full CI and loaded language modes, deployed
RustFS/Colima at enforced 1-vCPU/3-GiB limits, large-fleet freshness, TPS/p99 and
soak remain unqualified. The complete maintenance workflow and full goal remain
active; this milestone claims no throughput gain.

## Stop-consent refresh and read-only operator status

Source `119063ec` closes the SDK stale stop-consent boundary.
A separately authorized refresh names the exact prior consent and a fresh owned
Ready assessment. The operation, gate, boot, process and physical stop child stay
fixed. The original admitted consent anchors the physical selector for its whole
life; bounded canonical journal checks reject an orphan original and avoid an
unbounded predecessor walk. Competing refreshes use one expected-Root CAS.
Relevant input drift and changed predecessor/scope payloads refuse.

A queued original selector consumes the current same-scope consent and the
receiving Node renews actual physical/quorum proof. An accepted stop can finish
while a refresh CAS is held: later publication preserves the exact signed joined
closure, allowing offline confirmation and retained-engine return/resume. Lost
refresh confirmation reconciles before current live prerequisites. Cancellation
fencing still defeats delayed stop. Refresh restores no samples or clocks and
releases neither capacity nor the disruption slot.

`restart_stop_consent` reads historical canonical consent. The new
`restart_operator_status` reads a selected Root without a driver or observer,
reporting intent, durable phase, SDK execution attempt, historical Ready reference
and cancellation/terminal facts. Polling performs no mutation or physical work.
Absent means absent at that snapshot; Ready is historical assessment. Neither
confers live-health or lifecycle authority.

**933 distinct selected cases pass in 934 executions of completed passing
cohorts:** 785 library, 103 native fleet, 13 catalog, 25 membership-permit and seven
CLI cases. Seven new native refresh cases cover Root drift, unknown confirmation,
signed closure through actual stop/return/resume/capacity restoration, a held CAS
across actual join, competing writers, changed scope/predecessor, current-input
refusal, multiple generations/cancellation and an unadmitted original. One new
status case verifies durable read-only progress across adapters and snapshots;
existing preparation cases also verify Ready references. All 12 controller cases,
44 restart, nine resume, four return, eight stop, eight scoped-stop, nine operator
and ordinary-boot checks pass. Default-stack verification remains unchanged.

The earlier seven-case overlap probe passed six and failed because the fixture
passed one adapter's snapshot to another; the diagnostic reproduced
`Invalid("snapshot owner")`. The fixture now uses each adapter's own matching
snapshot. No production guard, assertion, timeout, retry, stack or resource bound
was relaxed, and no warning was suppressed. Earlier-source Overloaded,
dispatch-timeout and quorum-observation causes remain unproven; their retained
records are not current-source qualifications.

Host/admin all-target Clippy, format/layout, native dependencies, docs, ten plan
tests, the 433-case manifest and preflight with 100 reviewed input anchors pass.
Audits match **1,584 frozen Rust/Cargo/configuration inputs** and retained
executable/result hashes. Primary artifacts remain outside runtime data in
`qualification/fleet-operations/stop-refresh-v3/`, `stop-refresh-regressions-v1/`
and `stop-refresh-checks-v1/`; the earlier overlap probe and diagnostic remain
retained separately. The fingerprint adds `/stop-consent-refresh-1`, with numeric
markers still 1, matching binaries/fresh prefixes and no legacy reader/migration.

This delivers SDK refresh and read-only status. Management SDK/admin/CLI
sequencing, owned periodic execution and full fleet goal remain active. Full CI,
loaded language modes, deployed RustFS/Colima, enforced 1-vCPU/3-GiB limits,
large-fleet freshness, TPS/p99 and soak are unqualified. No throughput gain is
claimed.

**Next feature delivery:** expose maintenance operations and status through
management SDK/admin/CLI and own their execution under Node shutdown/join.
Sequence the separate stop/return/resume grants using the existing SDK receipts
and stable stop anchor. Then deliver grace/rolling/second-failure recovery and
production failure classification/isolation, followed by balancing, capacity/resize,
scalable convergence and resource-enforced qualification. FO-07 and FO-08 remain
in progress until their complete operator workflows pass.

## Durable maintenance preparation and cancellation controller

Source `94702658` adds the SDK operator controller above the guarded
maintenance transitions. A Root-journaled human intent fixes the original boot,
grace and birth-resolution choice. Its operator pointer serializes intent; M1
still must acquire the actual shared disruption slot. Bounded turns persist
child attempts before effects, own gate admission and physical quiescence as
separate steps, settle admitted work, hand off leadership and publish actual
Ready proof. Dropped/recreated controllers repeat live physical/quorum evidence.
Independent controllers compare exact step predecessors before advancing.

The stable human operation is distinct from an underlying SDK execution attempt.
M1 fixes its entire immutable request before its Root CAS; after a definite CAS
loss, a coordination CAS fences the old request before choosing a fresh SDK
operation. Admitted M1 and all later steps retain that exact execution scope.
The preparation probes exposed a committed gate followed by physical Conflict;
separating catalog admission from quiescence preserves that admitted gate and the
receiving node's current-epoch boundary. No failed physical request becomes Ready.

Separate human cancellation is intent only. Before M1, a Root CAS fences older
uncommitted work. An admitted preparation uses its canonical SDK cancellation;
a rooted gate requires the synced exact-process Fence. Pending winning work
retains its original resolution and must settle. Only the checked SDK receipt
permits terminal cancellation and operator-pointer release. The target remains
excluded Ready after admitted cancellation, with retained occupancy charged.
Historical retry cannot clear a later operator. Generic projection forgery refuses.

**925 distinct selected cases pass in 927 executions of completed passing
cohorts:** 785 library, 95 native fleet, 13 catalog, 25 membership-permit and seven
CLI cases. Eleven new native controller cases cover real SQL continuity, intent
races, protected rows, controller recreation, competing step predecessors, held
M1 CAS loss, four cancellation cuts, old-stop fencing and lost human/SDK/terminal
confirmations. Existing return/resume, stop, scoped-stop, 44 restart, nine operator
and ordinary-boot regressions pass. The unchanged operator stack regression also
passes on default stacks.

The first 44-case current-source restart cohort passed 43 and failed the dynamic
leader handoff case with `consensus operation failed: Overloaded`. That case
passed in isolation and the full 44-case confirmation passed without source,
assertion or bound changes. Its cause remains unproven; the failed log is retained
in `restart-operator-regressions-v1/`. Earlier preparation probes and the Clippy
unused-unit diagnostic remain retained separately. No timeout, retry, stack,
startup/resource bound or assertion was relaxed, and no warning was suppressed.
The earlier dispatch-timeout and quorum-observation causes remain unproven;
their older-source records are not current-source qualifications.

Host/admin all-target Clippy, format/layout, native dependency rules, docs, ten
plan tests, the 425-case manifest and preflight with 100 reviewed input anchors
pass. Audits bind all
**1,581 frozen repository Rust/Cargo/configuration inputs** and retained executable
and result hashes. Primary artifacts are outside runtime data in
`qualification/fleet-operations/restart-operator-v4/`,
`restart-operator-followups-v1/`, `restart-operator-handoff-isolation-v1/` and
`restart-operator-checks-v1/`; the initial regression run is retained separately.
The protocol fingerprint adds `/maintenance-operator-1`, retaining marker 1,
matching-binary/fresh-prefix requirements and no legacy reader or migration.

This delivers SDK preparation and cancellation orchestration. It does not yet
provide management SDK/admin/CLI sequencing for stop, return and resume or an
owned periodic operation service. Full CI and loaded language modes, deployed
RustFS/Colima, enforced 1-vCPU/3-GiB limits, large-fleet freshness, TPS/p99 and soak
remain unqualified. No throughput gain is claimed. The full fleet goal is active.

**Next feature delivery:** expose the durable operator workflow and read-only
status through management SDK/admin/CLI; complete separately authorized
stop/return/resume and in-place stale stop-consent refresh. Then deliver grace
expiry, rolling operations, subsequent-process/second-failure recovery and
production failure classification/isolation. Follow with load-aware balancing,
desired capacity/resize and scalable convergence. FO-07 and FO-08 remain in
progress until those complete workflows and their acceptance boundaries pass.

## Verified maintenance resume M9 SDK delivery

Source `aab046e5` closes the intact-return SDK path from stop through capacity
restoration. A live paged walk checks the exact returned boot, complete current
inventory, actual profile and settled RF3. All current voters must be matched at
the serialized leader. A paused returned voter that regained leadership receives
an owned handoff; its history remains pending until surviving leadership is
observed. No retired membership is recreated and retained occupancy stays charged.

Separate resume consent carries intent only. The receiving Node independently
renews current physical/quorum/profile proof before its engine consumes the exact
synced quiescence and an owner-clock evidence deadline. Only then do reversible
public/SDK gates open. Root remains RestartPending, capacity excluded and the
slot held. A fresh Resumed-phase walk and one Root CAS install Active, its matching
capacity row and terminal completion, releasing only the original slot. Relevant
Root drift requires fresh same-scope consent; drift after physical reopening
retains exclusion and the slot until renewed completion. Exact historical journal
readers reconcile lost consent/completion confirmations without restoring samples.

**914 distinct selected current-source cases pass in 929 passing executions:**
785 library, 84 native fleet, 13 catalog, 25 membership-permit and seven CLI cases.
Nine new native resume cases cover intact primary/follower return, SQL continuity,
capacity/slot release and another maintenance admission, missing voter, stale live
evidence, receiving renewal after consent expiry, Root drift before/after opening,
lost confirmations, forged scope/projections and regained leadership. Ten actual
engine lifecycle cases and the third-voter matched/active check pass. Focused
engine/owner, preparation/operator and dispatch isolation runs repeat within the
distinct counts. All 44 existing restart regressions and nine operator cases pass.

The initial current-source library cohort passed 784 and failed the unchanged
10-ms dispatch-timeout test when its expected actor request was absent. That case
passed in isolation and the full 785-case confirmation passed without changing
source, assertions or bounds. Its cause remains unproven. Earlier resume probes
are retained: a duplicated terminal Node/selector exceeded the existing 4-KiB
record limit; the corrected terminal fact binds the separate canonical Node by
digest and derives the intent selector from existing scope fields. Strict negative
fixtures now use the actual serialized state name and disjoint namespace-import
request IDs. No timeout, retry, stack, startup/resource bound or assertion was
relaxed, and no warning was suppressed.

Host/admin all-target Clippy, format/layout, native dependency rules, docs, ten
plan tests, the 414-case manifest and source preflight with 100 reviewed input anchors
pass. Audits bind all **1,572 frozen repository Rust/Cargo/configuration inputs**
to the source commit and verify executable/result hashes. Primary artifacts stay
outside runtime data in `qualification/fleet-operations/restart-resume-v3/`,
`restart-resume-regressions-v2/` and `restart-resume-checks-v1/`. The failed initial
library log remains in `restart-resume-regressions-v1/`; earlier probes and the
prior-source engine probe are retained separately. The protocol fingerprint adds
`/restart-resume-1`, with numeric markers still 1 and matching binaries/fresh
prefixes required. No migration or legacy reader is added.

These are shared-host native fault-store tests. Full CI and loaded language modes,
deployed RustFS/Colima, enforced 1-vCPU/3-GiB limits, large-fleet verification within
the freshness envelope, TPS/p99 and soak remain unqualified. Empty/retired returned
node qualification and repair after grace are distinct from this tested intact
path. Earlier broad archive/snapshot/RPC results belong to their stated source;
the general intermittent quorum Unavailable cause also remains unproven. No
throughput gain is claimed.

**Next feature delivery:** complete the maintenance operation driver across
SDK/admin/CLI, durable progress and in-place stale stop-consent refresh. Then
deliver grace expiry, rolling operations, subsequent-process/second-failure recovery
and production failure classification/isolation. Follow with workload balancing,
desired capacity/resize and scalable convergence. FO-07 stays in progress until
its complete operator workflow and failure policy are delivered; the full fleet
goal remains active.

## Retained-engine return M8 delivery

Source `05b38d4b` implements the M8 SDK backend after the separately confirmed
M6/M7 stop and offline boundaries. `admit_restart_return` records exact human
intent bound to the signed offline predecessor and a distinct intended process.
Only the exclusive owner of the retained node/disk/engine can consume that
consent. Its canonical boot CAS preserves RestartPending, resource charges,
placement exclusion and the disruption slot. An unknown startup confirmation
retains its original admitted nonce and signer binding across exact retry.

The serialized engine consumes the exact persisted maintenance stop and syncs
the fresh boot directly into quiescence. Startup holds both reversible admission
gates, including failed attempts; it installs the exact selector before endpoint
publication and binds SDK admission before the peer query gateway. Consensus
and archive recovery can catch up while public and SDK application writes stay
blocked. Generic operation/boot projections, the old process, wrong process and
replacement engine refuse. No capacity, resume or slot-release proof is issued.

**902 distinct current-source cases pass in 913 executions:** 782 library,
75 native fleet, 13 catalog, 25 membership-permit and seven CLI cases. Focused
runs repeat eight engine cases, two preparation cases and one operator case
within those distinct counts. The four new return cases cover a joined primary,
lost consent/startup confirmations, refusal without signed offline proof,
wrong/reused process, operation and boot forgery, and replacement-engine refusal.
Actual retained-engine reopen verifies cleanup after rejected startup. Ordinary
empty-engine resume and replacement refusal also pass. All nine operator cases
and all 44 existing restart regressions pass.

The first probes are retained, including compile diagnostics and two incorrect
negative fixtures: cancellation needed the existing authenticated route revision
barrier; rejected startup has no completed Raft-host report. The corrected
fixture requires SDK-node join and actual retained-engine reopen. Broader runs
then exposed real stack overflows in preparation and operator cancellation.
New maintenance confirmation futures are boxed, and ordinary startup again calls
its existing boot binder directly. The crash/disassembly audit measured an extra
82,368-byte poll frame in the discarded combined wrapper; boxing its future had
not removed that poll frame. The unchanged operator regression now passes alone
and in the full cohort. Node mutation protection reuses the existing lookup and
decoded rows. No stack, timeout, retry, startup/resource bound or assertion was
relaxed, and no warning was suppressed.

Host/admin all-target Clippy, formatting/layout, native dependency rules, docs,
ten plan tests, the 402-case manifest and 100 reviewed source anchors pass.
Audits match all **1,561 frozen Rust/Cargo/configuration inputs** to this source
commit and verify retained executable/result hashes. Primary artifacts stay
outside runtime data under `qualification/fleet-operations/`, in
`restart-return-v7/`, `restart-return-followups-v1/` and
`restart-return-checks-v5/`; failed probes `restart-return-v1/` through `v6/`
remain retained. The protocol fingerprint adds `/restart-return-1`; matching
binaries and fresh prefixes are required. Numeric markers stay at 1, without
migration or legacy readers.

These shared-host native tests use an in-process fault store. Full current-head
CI and loaded language modes, deployed RustFS/Colima, enforced 1-vCPU/3-GiB
resources, large-fleet freshness, TPS/p99 and soak remain unqualified. Earlier
archive/snapshot/RPC results below belong to their stated source. The earlier
general quorum Unavailable cause remains unproven. No throughput gain is claimed.

**Next feature delivery:** M9 must verify current inventory/configuration,
retained or retired copies, fresh matched catch-up and the actual profile, then
own resume before capacity/slot release. It must recheck leadership after return;
a paused voter can still become leader. Follow with the complete maintenance
SDK/admin/CLI operation driver, stale-consent refresh, grace/rolling/second-failure
handling and production isolation. FO-07 remains in progress until that workflow
is usable end to end; the full fleet goal stays active.

## Guarded maintenance stop and offline confirmation

Source `676752f5` implements the M6/M7 SDK backend. A separate canonical human
stop consent binds exact Ready, process and child identity. The receiving Node
renews physical inventory, both-survivor quorum and matched surviving leadership
under its actual reversible admission gate. The serialized engine consumes the
exact synced quiescence and an evidence deadline on its own clock; a same-epoch
cancel Fence, newer gate, changed boot or expired evidence refuses shutdown.
Node-owned work survives a disconnected request waiter. Local quiescence proof
avoids a self-RPC deadlock without replacing physical or quorum checks.

Only actual signed Node/domain/engine joined closure can become the M7 offline
fact. Lost consent and offline confirmations reconcile from the exact journal.
Neither confirmation releases resource charges, capacity exclusion or the
disruption slot, nor permits a new boot or resume. Relevant Root basis drift
currently requires fenced cancellation and a new preparation; an in-place
stale-consent refresh is still required. The namespace protocol fingerprint now
ends in `/restart-stop-1`; matching binaries and fresh prefixes are required.
Numeric Cellar format markers stay at 1, with no migration or legacy reader.

**857 distinct current-source cases pass in 864 executions:** eight new native
stop cases, 44 restart regressions, eight scoped-stop cases, 781 library cases,
nine native operator cases and seven CLI cases. Seven engine lifecycle cases
are also run as a focused cohort and repeat within the library count. Coverage
includes primary/follower and empty-node shutdown, live renewal after expired
Ready, missing-survivor refusal, lost Root confirmations, cancellation races,
basis drift, forged generic projections and retained disruption/capacity debt.
All positive shutdown cases require actual join and the signed closure.

Host/admin all-target Clippy passes with warnings denied. Formatting/layout,
native dependency rules, docs, ten plan tests and the 397-case manifest pass;
100 reviewed source anchors are refreshed. Audits match **1,557 frozen
Rust/Cargo/configuration inputs** to the source commit and verify retained
executable/result hashes. Artifacts stay outside runtime data in
`qualification/fleet-operations/restart-stop-v2/` and
`restart-stop-checks-v1/`. The initial Clippy attempt's visibility and enum-size
diagnostics are retained; they were fixed without suppressing warnings.
No timeout, retry, stack/startup/resource bound or assertion was relaxed.

These shared-host native tests use an in-process fault store. Full CI,
deployed RustFS/Colima, enforced 1-vCPU/3-GiB resources, large-fleet freshness,
TPS/p99 and soak remain unqualified. Earlier broader archive/snapshot/RPC
results below belong to their stated source. No throughput gain is claimed.

**Next feature delivery:** M8 retained-engine return under a fresh process with
startup admission excluded, then M9 verified catch-up and capacity resume before
slot release. Complete the maintenance SDK/admin/CLI workflow after those
boundaries, followed by grace expiry, rolling/second-failure handling and
in-place stale-consent refresh. FO-07 and the full fleet goal remain active.

## Current-main integration of RestartReady

Source `167c32ba` integrates M5 `8665f7e3` with main `a9da48ac`, including immutable
object-store extent snapshots and loaded-Python RPC checks. Export and Raft-guide
conflicts preserve both fleet verification and the upstream root archive API.
The new root archive test now captures existing namespace birth-inventory heads
and verifies that snapshot data publication creates or changes no head. This
preserves the data-only archive oracle while recognizing fleet birth tracking.

**996 distinct current-source cases pass:** 779 library, 81 archive, 47 snapshot,
21 rooted, 58 native fleet, seven CLI and three native typed RPC cases. The native
fleet count includes all eight new readiness cases and 36 restart regressions.
Two directly ignored snapshot fixtures are excluded; their parent process
matrices pass. The third ignored entry is the isolated RustFS rooted-archive
drill, which was not run. Loaded Python modes and full current-head CI remain
separate qualification gates.

The first library cohort had 778 passes and one failure in the unchanged 10-ms
RPC routing timeout test: it expected a pending actor request but the channel
was closed. That case passed in isolation and the complete 779-case confirmation
cohort passed. Its timing cause remains unproven; the failed log is retained.
The initial rooted cohort also retains its invalid no-existing-head assertion
failure. No timeout, retry, stack/startup bound, resource limit or assertion was
relaxed. The rooted fixture now compares the full before/after head inventory.

Host/admin all-target Clippy, formatting/layout for 1,458 Rust files, native
dependency rules, docs, ten plan tests and the 387-case manifest pass. The source
audit verifies **1,552 frozen Rust/Cargo/config inputs**, executable and result
hashes. After the rooted-test-only fix, a current-source rebuild confirms every
other executable is byte-identical; their recorded passing results remain
attributable to this build. The final documentation publication refreshes all
100 reviewed preflight anchors.

Artifacts remain outside runtime data under `qualification/fleet-operations/`
in `restart-readiness-main-v1/`, `restart-readiness-main-followups-v1/`,
`restart-readiness-main-v2/` and `restart-readiness-main-checks-v1/`.
These shared-host native results do not qualify deployed RustFS, enforced
1-vCPU/3-GiB resources, large-fleet readiness inside the unchanged freshness
bound, TPS/p99 or soak. The earlier general quorum Unavailable cause remains
unproven. No throughput improvement is claimed.

The next delivery remains the complete operator maintenance workflow: M6
guarded exact-process stop and joined closure, then M7–M9 retained-engine return
under a fresh process, catch-up and verified capacity resume before slot release,
with SDK/admin/CLI exposure. FO-07 remains in progress until that workflow is
usable end to end; its title now states that scope. The full goal stays active.

## RestartReady M5 delivery and first qualification

Source `8665f7e3` delivers owned point-in-time RestartReady through the public
verification SDK. `FleetRestartVerification::publish_ready` accepts only the live
walk after complete canonical inventory and matched surviving leadership. It
retains actual peer sample ages in the catalog owner's clock without persisting
clock values, repeats exact physical quiescence and inventory, checks settlement
and rooted inputs, and enforces the existing five-second evidence envelope at
the Root CAS. A historical candidate cannot restore that live authority.

The bounded `restart-ready/` record binds the exact request, process through its
gate parent, rooted input and candidate digest. Generic catalog mutations cannot
create, replace or delete this projection. `FleetCatalog::restart_readiness`
recovers an exact journal-confirmed historical receipt after a lost confirmation
or controller restart, including after later cancellation. Altered process or
principal refuses. The target remains RestartPending and quiesced, capacity stays
excluded and the disruption slot stays held. Readiness supplies no stop, new
boot, resume or slot-release authority.

Handoff also preserves actual peer sample age when translating the remaining
freshness budget to the Raft owner's clock. Queued execution cannot renew that
age. Business write acknowledgement has no new catalog dependency.

**844 distinct current-source cases pass in 844 executions:** eight readiness,
36 existing restart regressions, 779 library cases including two freshness-budget
cases, five peer-observation cases, nine native operator cases and seven CLI
cases. Host/admin all-target Clippy passes with warnings denied. Formatting and
layout for 1,454 Rust files, native dependency rules, docs, ten plan tests and the
387-case manifest pass. The audit verifies all **1,548 frozen Rust/Cargo/config
inputs**, retained executables and result hashes against this source commit.
The final documentation-only publication also refreshes the 100 reviewed
preflight anchors.

Artifacts are outside runtime data in `qualification/fleet-operations/` under
`restart-readiness-v4/` and `restart-readiness-checks-v1/`. Earlier failed probes
remain retained: fixtures constructed a sleep outside its domain, shared a
bounded adapter turn, then paired a foreign snapshot with an independent
adapter; cancellation also needed the authenticated route revision barrier
following Ready publication. A diagnostic formatting branch failed compilation
and was corrected. The complete current cohort passes with snapshot/adapter
pairing and the existing authenticated revision barrier. No timeout, retry,
stack/startup bound, resource limit or assertion was relaxed.

These are shared-host native tests using the fault-injecting in-process store.
They do not qualify full CI, deployed RustFS, 1-vCPU/3-GiB enforcement, readiness
at hundreds of affected cells inside the unchanged freshness bound, TPS/p99 or
soak. Earlier general quorum Unavailable behavior remains unproven. Broader
archive/snapshot/Node qualification below belongs to its stated earlier source.

**Next feature priority:** complete M6 exact-process guarded stop and joined
shutdown, then M7–M9 retained-engine return under a fresh process, catch-up and
verified capacity resume before slot release, with SDK/admin/CLI exposure.
Follow with rolling/grace/second-failure behavior, production failure
classification/isolation, load-aware leadership balancing and desired
capacity/resize. FO-07 remains in progress until the complete maintenance
workflow is delivered; the full fleet goal remains active.

## Prior main integration and delivery qualification

Source `7f873686` integrates removal proof waiting from `cbf19149` with main
`784edc1b`, including the upstream RPC and recovery pipeline changes. The merge
preserves fleet seed guards, canonical-empty archives, original image publication
positions and independent per-cell publication completion. Configuration tails
now validate the actual tip membership used by fleet coverage. The fixed-size
physical replay memo still checks lifecycle expiry after acquiring its exclusive
workspace, including on a hit; canonical-empty verification cannot hide a changed
configuration. Prepared snapshot sources retain scratch-pressure cleanup on
individual and global publication failures. The protocol fingerprint binds both
fleet and configuration-tail features; numeric markers remain 1.

**984 distinct core/SDK cases pass in 984 executions, plus 21 admin tests.** This
includes all 777 library tests, 81 native archive cases, 47 snapshot cases, 16
rooted-recovery cases, 51 native fleet cases, seven CLI cases and five Node module
cases. Two snapshot helper entries are ignored when invoked directly and belong
to the process-kill harness; they are excluded from the passing count. The native
snapshot suite includes the parent crash matrices. Node qualification uses Node
24.19.0 on macOS; Linux CI for this integrated source remains a separate gate.

Host/admin all-target Clippy and Node module-target Clippy pass with warnings
denied. Formatting/layout for 1,451 Rust files, native dependency rules,
documentation, ten plan tests, the 377-case manifest and source preflight with
100 reviewed anchors pass. The audit maps **1,545 frozen Rust/Cargo/configuration
inputs** to the source commit and verifies retained executable/result hashes.
Artifacts remain outside runtime data in `qualification/fleet-operations/` under
`proof-wait-main-v4/`, `proof-wait-main-followups-v1/`,
`proof-wait-main-js-v1/` and `proof-wait-main-checks-v1/`.

Earlier failures and the intentionally interrupted build remain retained. After
the merge, the isolated-removal scenario overflowed the default stack during
startup. A diagnostic probe and the actual crash trace identified overlapping
setup/removal poll frames, including approximately 600 KiB for the larger removal
phase. Startup now completes before that phase is constructed and polled. All
operations and assertions remain, and the complete cohort passes on the default
stack. Temporary diagnostics were removed. No timeout, retry, stack/startup limit,
resource bound or assertion was relaxed.

Full CI and deployment qualification remain open. The parent `90529386` Rust SDK
event-delivery failures described below are not declared fixed by local fleet
results. The original intermittent quorum Unavailable cause also remains unproven;
current typed waiting and the full selected operator cohort pass. No new deployed
RustFS, 1-vCPU/3-GiB, TPS/p99 or soak result is claimed.

M5 RestartReady is delivered above. The next target is M6–M9 separate
exact-process stop/join, original-engine return under
a fresh process, catch-up and verified resume before slot release, exposed through
SDK/admin/CLI. Then complete rolling/grace/second-failure behavior, production
failure classification/isolation, load-aware leader balancing and desired
capacity/resize. The full fleet-operations goal remains active.

## Resumable removal proof verification

Source `cbf19149` delivers typed waiting at the read-only quorum boundaries of
operator departure. A failed history observation retains that same pending
history and checked count. After exact signed joined shutdown, a failed terminal
survivor renewal retains the completed inventory and disruption slot. Neither
result supplies finalization, stop or removal authority. The catalog produces
terminal waiting before its Root mutation; mutation and confirmation errors
cannot become waiting. The adapter rechecks the exact Root and original deadline
before acknowledging progress.

SDK/admin/CLI progress now includes nullable `blocker`, with `history_quorum` or
`terminal_quorum` and an explicit observation reason. Status exposes nullable
local `verification` only at the same node, operation and Root. Root drift hides
stale progress and refuses the old request; controller restart repeats proof.
CLI `--wait` follows acknowledged progress under its existing deadline and ends
on unknown mutation outcomes. Matching SDK/admin/CLI deployment is required for
this internal JSON cutover. No catalog dependency was added to write acknowledgement.

**89 distinct focused Rust cases pass in 92 executions, plus 21 admin tests.**
The focused cases comprise 51 fleet library, seven CLI, nine native operator,
five peer-observation, three isolated-removal, four finalization, two joined
removal and eight scoped-stop cases. The two new native scenarios use a bounded
TCP fault against an actual voter, verify S3-level SQL continuity while history
proof is blocked, and require explicit stop plus a real signed joined closure
before testing terminal renewal. The slot stays held until connectivity returns
and the exact removal completes. All seven new library/CLI/native cases pass.

All-target host/admin Clippy, formatting/layout for 1,423 Rust files, native
dependency rules, documentation, ten plan tests and the 375-case manifest pass.
The audit maps all **1,517 frozen Rust/Cargo/configuration inputs** to the source
commit and verifies retained executable/result hashes. Artifacts remain outside
runtime data in checkout-private `qualification/fleet-operations/proof-wait-v6/`,
`proof-wait-followups-v2/` and `proof-wait-checks-v3/`.

Earlier failed probes remain retained. A control comparison reproduced both a
repair-role CommandConflict and an isolated-removal stack overflow on the
published source as well as the candidate. The repair-role fixture had inserted
an unreserved low-level permit that correctly lacked managed destination-boot
authority. It now uses actual capacity qualification and canonical resource-backed
movement admission, preserving learner/joint/RF3 and SQL assertions. The large
isolated-removal scenario now boxes its test future on the default stack. Both
cohorts pass on the final source. No timeout, retry, stack/startup limit, resource
bound or assertion was relaxed.

The previously failing cancel-and-restart operator case passes in the current
nine-case cohort. The cause of its intermittent quorum Unavailable remains
unproven; explicit waiting is an operational improvement, not a claim that the
underlying availability issue was fixed. Full release qualification remains open.
The parent source `90529386` passes the full Linux Node SDK workflow, confirming
the declared-port correction. Its Rust SDK CI failed three event-delivery cases
in [Python 3.14](https://github.com/crabbuild/cellar/actions/runs/40511364732/job/122213834852)
and one in [3.14t](https://github.com/crabbuild/cellar/actions/runs/40511364732/job/122213835367),
including ambiguous RPC results and lease-expiry shutdown. These are retained
release gaps; no full-CI pass is claimed.

Next deliver the maintenance workflow as a vertical feature: fresh RestartReady
(M5), separate exact-process guarded stop/join, retained-engine return under a
fresh process, catch-up and verified resume before slot release (M6–M9), with
SDK/admin/CLI access. Then complete rolling/grace/second-failure policy,
production failure classification/isolation, load-aware leadership balancing and
desired capacity/resize. Resource-enforced RustFS/Colima performance, failure and
soak qualification remains required. The full goal stays active; these native
fault-store runs make no deployed TPS or p99 claim.

## Guarded restart leadership reconciliation

Source `4eaeda94` includes the handoff implementation in `1119c6a6` and the
published-container fixture port correction.

`FleetRestartVerification::reconcile_next` now owns the handoff step after
original-voter materialization. Each voting history checks the current settled
membership, both surviving voters and their publications, then asks the actual
leader to check its replication tracker. A transfer selects an eligible survivor
with fresh capacity. `HandoffRequested` acknowledges admission only and retains
the pending history; another turn must observe a surviving leader before the
historical matched entry is published. Reopening the controller repeats actual
inspection instead of trusting an old request acknowledgement.

Receiving authorization independently pins the gate, operation, rooted inputs,
history and exact boot. The leader checks term, configuration, settled RF3 and
both survivors' recent matched progress. The Raft owner checks an evidence expiry
before execution and checks the last synced quiescence selector before transfer.
That expiry is captured on the Raft owner's clock before gathering proof and
tightened by the SDK request deadline. A longer request timeout cannot extend
the five-second evidence window; catalog and engine clock origins are not
interchangeable. A synced cancellation fence defeats queued transfers. Already
admitted transfer work and ordinary elections may still change leadership, so
later readiness and stop must renew proof.

Observation on a surviving leader can finish while an application write waits
for archive publication. Transfer still requires accepted work to drain. The
historical candidate exposes matched coverage without granting RestartReady,
stop, rejoin, capacity or disruption-slot release. Business write acknowledgement
acquires no catalog dependency.

The latest frozen source passes **28 focused Rust cases and five Node module
cases**. The Rust cases include seven
native handoff cases, four original-voter cases, nine runtime inspection cases,
two queued-fence/deadline cases and six wire cases. The native cases exercise
actual SQL continuity and S3-level acknowledgements, controller reopen, a dynamic
replacement leader, retained copies, a missing second survivor and a held archive
write. The earlier frozen probe passed 115 core executions, including all 45
movement cases, and 78 permit/CLI/library executions. These earlier results are
attributed to that probe rather than counted as final-source qualification.

All-target host/admin Clippy and Node module-target Clippy pass with warnings
denied. Formatting/layout for 1,420 Rust files, native dependency rules,
documentation, ten plan tests, the 368-case manifest and source preflight with
97 reviewed anchors pass. The audit maps all **1,514 frozen Rust/Cargo/configuration
inputs** to the source commit and verifies retained executable and result hashes.
Final-source evidence stays outside runtime data in checkout-private
`qualification/fleet-operations/restart-handoff-v7/`, `restart-handoff-js-v1/`
and `restart-handoff-checks-v2/`. Earlier successful and unsuccessful probes
retain their own source attribution. No stack/startup bound was raised; the held
archive test uses a boxed future on the default test stack.

Broader qualification **did not pass**. One 27-case resource cohort had 26 passes
and a failure in
`fleet_operator_cancels_stale_departure_and_restarts_with_fresh_proof`.
The same retained binary passed that case in isolation, then failed in both an
operator cohort and another isolated probe. Temporary diagnostics reproduced an
unavailable live quorum observation before the first history was verified.
The cause remains open; no timeout, retry, resource bound or assertion was
relaxed. The temporary diagnostics were removed. A previous retained host passed
an operator cohort using the current CLI; that component comparison is not a
complete old-head qualification and does not establish causality.

The published Node lifecycle fixture now requests its declared port 8080.
Previously its ephemeral listener worked through macOS port mapping while Linux
connected directly to port 8080. The full module suite passes on macOS with Node
24.19.0. The subsequent [Linux workflow passes](https://github.com/crabbuild/cellar/actions/runs/40511364597), confirming the fixture correction.
The previous published head also has
[event-delivery CI failures](https://github.com/crabbuild/cellar/actions/runs/40491368640/job/122146532958)
with ambiguous RPC results; those remain separate release gaps. No full-CI pass
is claimed.

Next deliver reliable proof renewal with explicit pending/blocked observation
status and RestartReady (M5), preserving unknown mutation outcomes. Then complete
exact-process stop/join, retained-engine return under a fresh process, catch-up
and verified capacity resume before slot release (M6–M9). Follow with maintenance
SDK/admin/CLI, rolling/grace/second-failure handling, production failure
classification/isolation and desired capacity/resize. Resource-enforced
RustFS/Colima performance, faults and soak remain unqualified. The full goal
stays active; these shared-host native tests make no new TPS or p99 claim.

## Owned original-voter materialization

Implementation `703c5bcb65fa469303c29d79260e322b48dd5220` delivers the missing
construction path in restart runtime reconciliation. `reconcile_next` materializes
at most one original voter per turn or verifies the pending history. The existing
immutable Live birth and permanent member claim remain the intent. Commands pin
the canonical gate parent, operation, relevant inputs, history digest and exact
destination boot. The receiver independently checks current authorization, then
uses the existing guarded registration and initial-member pipeline. Imported or
dynamic members, retired/occupied slots and claimed blank engines refuse fresh
genesis. No new membership authority is introduced.

A matching activation receipt is followed by complete physical inspection. The
target cut may absorb only the pending genesis history; any other physical change
invalidates the walk. Unknown replies retain the same pending history and physical
owner. A reopened controller reinspects actual state and reconciles unattached
original voters without resetting occupied engines. All three voters must exist
before the separate fresh quorum/publication turn consumes the history. A synced
maintenance fence prevents further controller dispatch even if the cancellation
root CAS failed. Already-admitted immutable initialization may still finish;
cancellation does not erase its history or charges.

The shared authenticated fleet-action lane carries explicit Movement and
RestartGenesis commands with separate receiving authorities. Existing deadlines,
reply ownership, metadata/connection budgets and the 4 KiB body bound remain.
Old flat movement bodies and the old channel purpose are refused as an internal
wire hard cutover. The construction result remains historical: it grants no
RestartReady, owned handoff, stop, fresh boot, capacity or disruption-slot release.

**195 distinct cases pass in 264 passing-cohort executions on this source.**
Four new native cases cover all original voters, real S3-level acknowledgements
and SQL continuity, an interrupted permanent claim with controller reopen, changed
root inputs and a synced fence with unchanged Root. Existing initial-genesis,
runtime, settlement, movement, resource/topology, enrollment/operator, catalog,
physical-inventory, standalone permit/import, CLI and library cohorts also pass.
Overlapping cases are counted once. The fixture explicitly hands off through the
existing API if needed; this does not claim owned M4 handoff.

All-target host/admin Clippy passes with warnings denied. Formatting and layout
for 1,413 Rust files, native dependency rules, documentation, ten plan tests and
the 358-case manifest pass. The audit matches **1,507 frozen Rust/Cargo/configuration
inputs** to the implementation commit and verifies executable and output hashes.
Artifacts stay outside runtime data in checkout-private
`qualification/fleet-operations/restart-genesis-v1/`,
`restart-genesis-followups-v1/`, `restart-genesis-compat-v1/` and
`restart-genesis-checks-v1/`. Publication-only document/preflight checks are
separately retained. No timeout, retry, stack/startup bound, resource bound or
production assertion was relaxed.

One initial movement probe exhausted its existing bounded turns after Joint with
Unconfirmed. Its exact retained binary then passed the isolated case and two
complete serial cohorts with the same bounds. The failed log remains retained;
its cause is unconfirmed and no production movement fix is claimed. Full-head CI
is still required. Published head `2b1d92fd`
[failed the existing Linux Node lifecycle case](https://github.com/crabbuild/cellar/actions/runs/40481653350/job/122113182000)
with a five-second `/wake` timeout. The separate Linux reproduction image and
locked inputs are prepared; that failure remains unresolved.

Next deliver leader-local matched progress and guarded, observed handoff (M4),
including election behavior while quiesced, then final proof renewal and
RestartReady (M5). Follow with exact-process stop/join, fresh-process retained-engine
return, catch-up and verified capacity resume (M6–M9), rolling/grace/second-failure
handling and maintenance operator surfaces. Production failure classification
and isolation, desired capacity/resize and resource-enforced RustFS/Colima
performance, fault and soak qualification remain open. These native tests use an
in-process fault-injecting object store on a shared host; they make no new TPS,
p99 or deployed 1-vCPU/3-GiB claim. The full goal remains active.

## Bounded restart runtime inspection

Implementation `16d16fa1` delivers `prepare_restart_verification`, an owned
bounded walk of complete canonical history and target physical inventory.
Preparation owns exact-gate quiescence after M2 clears. Each voting-history turn
obtains actual safe-ReadIndex and durable publication proof from the target and
both current survivors. The settled managed membership journal determines those
voters; immutable genesis is not treated as current placement. Exact boots,
current enrollment, RF3, settled permits, term/leader agreement, failure domains
and both survivor publications covering the fixed target cut are checked under
the original deadline and existing five-second evidence-age envelope.

Retired copies stay accounted without votes or release credit. Empty targets
still consume the full canonical inventory. Unknown physical slots, unsettled
roles, missing rightful genesis and relevant root drift refuse completion.
Failed attempts retain the same pending history. `finish` requires exact physical
coverage, repeats quiescence and checks a second complete physical cut before
publishing a bounded immutable receipt index. A synced cancellation fence defeats
that replay even when its root CAS timed out. Candidate data remains historical;
it cannot authorize RestartReady, handoff, stop, a new boot, capacity or slot release.

**155 distinct cases pass on this source, in 162 executions.** This includes nine
new native runtime cases and six physical-inventory cases, plus the existing
settlement, movement, gate, preparation, catalog, standalone permit/import,
CLI/operator and library fleet cohorts. Seven cases overlap between cohorts
and are counted once. The new native cases cover primary and follower targets,
both surviving voters, writable quorum refusing maintenance, actual
replacement and retired-copy accounting, empty capacity, missing genesis,
root churn and synced cancellation with an unchanged root. A quiesced primary
refuses new business commands; the fixture then uses the existing explicit
transfer API and observes the survivor leader before verifying SQL continuity.
That fixture transfer does not claim runtime-owned M4 handoff.

All-target host/admin Clippy passes with warnings denied. The audit matches all
**1,502 frozen Rust/Cargo/configuration inputs** to this implementation commit
and verifies retained executable and output hashes. Evidence stays outside
runtime data under checkout-private `qualification/fleet-operations/` runs
`restart-runtime-v4/` and `restart-runtime-followups-v1/`. The physical role byte
has strict wire decoding. Byte-aligned scalar storage keeps maximum-size wire
copies plus decoded inventory inside the unchanged 1 MiB reservation.

Formatting/layout for 1,408 Rust files, native dependency rules, documentation,
ten plan tests, the 354-case manifest and source preflight with 95 anchors pass.
Static checks are retained in `restart-runtime-checks-v1/`; publication-only
documentation checks are retained separately.

The existing Node.js 24 published-container lifecycle case also passes locally
on macOS (`restart-runtime-js-v1/`). The previous published head `d5872e39`
[failed that case in Linux CI](https://github.com/crabbuild/cellar/actions/runs/40473349010/job/122085492229)
with a five-second `/wake` timeout. A local pass does not establish its Linux
outcome; latest-head CI must repeat it. Deployed RustFS/Colima, enforced
1-vCPU/3-GiB resource bounds, TPS/p99, fault and soak qualification remain open.

Failed probes remain retained. They found a test using a private accessor, a
new validator incorrectly requiring the quiesced target's application-paused
profile to be false, and fixtures sending new writes to a quiesced leader. The
validator now requires a paused target and unpaused survivors. The fixtures
assert target refusal, observe an explicit handoff, and retain the same SQL,
quorum, cancellation and resource assertions. No deadline, stack/startup bound,
resource/value bound or production assertion was relaxed.

Next deliver rightful genesis materialization for M3, leader-local matched
progress plus owned observed handoff for M4, and final proof renewal/RestartReady
for M5. Then deliver exact-process stop/join, fresh-process retained-engine
return, catch-up and verified capacity resume before slot release (M6–M9),
followed by rolling/grace/second-failure handling and maintenance operator
surfaces. Production failure classification/isolation, desired capacity/resize,
scaled convergence and resource-enforced deployment qualification remain
unfinished. The full fleet-operations goal remains active.

## Owned restart settlement turns

Implementation `ce0c199d` composes rooted M2 work through
`reconcile_restart_work`. Each explicit turn resolves one pending birth or
advances one admitted movement under its original deadline. Finish versus
Cancel for an undecided birth is explicit intent; either policy preserves a
permanent winner. The chosen resolution is journaled and cannot be rebound
while active. Missing movement services, rebound evidence and resource-only
debits never grant progress or release credit.

A root CAS records the exact birth, history digest and canonical gate parent
before namespace I/O. Its active claim holds gate cancellation across lost
admission, namespace-winner or terminal replies. Only the owned path can update
these protected records. One terminal CAS confirms the actual history state,
stores its receipt and removes its own pointer. A changed-root conflict retains
the same namespace decision and uses a fresh confirmation identity. The
historical `restart_birth_settlement` query verifies admission and completion
journals, returns chosen resolution plus actual winner, and cannot clear a
later claim. Operator failure mapping reports unresolved settlement as Blocked
with Unknown outcome.

**140 distinct cases pass on this source, in 145 executions:** six new owned
settler cases, four earlier settlement cases, 41 movement, two preparation,
four gate, thirteen catalog, 25 standalone permits/import, five CLI/operator
and 45 library fleet cases. Five cases also appear in the movement cohort and
are counted once. The new native cases exercise explicit Finish with a lost
namespace response, immutable resolution choice, both namespace winners,
controller reopen after lost admission/terminal confirmation, a held terminal
CAS losing to independent root churn, generic-forgery refusal, cancellation
blocking, historical receipts preserving a later active claim, actual movement
completion/SQL continuity and typed service/debit blockers.

All-target host/admin Clippy passes with warnings denied. The source audit
matches **1,496 frozen Rust/Cargo/configuration inputs** to the implementation
commit. Primary evidence remains outside runtime data under checkout-private
`qualification/fleet-operations/restart-settler-v4/` and
`restart-settler-followups-v1/`. Earlier qualification belongs to its recorded
sources, not an aggregate count.

Formatting/layout for 1,402 Rust files, native dependency checks, documentation,
ten plan tests, the 345-case manifest and source preflight with 95 input anchors
pass. Static outputs are in `restart-settler-checks-v1/`; documentation-only
publication checks are separately retained. Full latest-head CI and deployed
RustFS/Colima resource, performance and soak qualification remain pending.

Failed and interrupted probes remain retained. They include the new catalog
error's initially missing operator mapping and a gate fixture whose existing
zero-pending assertion correctly rejected the new pending-birth setup. A
separate helper now asserts those retained references; the original assertions
remain intact. The conflict racer reads its own adapter's snapshot and retains
the production snapshot-owner check. One candidate was deliberately stopped
before tests to make Finish/Cancel an explicit immutable choice. No deadline,
stack/startup bound, value/resource bound or runtime assertion was relaxed.

This delivers owned turns for rooted work. `CatalogClear` still grants no live
configuration, survivor, handoff, RestartReady, stop or capacity proof. Next
implement the complete bounded hosted/pending walk, rightful genesis
materialization where needed, fresh both-survivor and matched/published proof,
observed handoff and readiness. Then deliver guarded stop/join, retained-engine
fresh-process return and verified capacity resume, followed by grace/second
failure/rolling execution and operator maintenance surfaces. Production failure
classification/isolation, scaled convergence, balancing/resize and resource-
enforced RustFS/Colima performance/fault/soak qualification remain unfinished.
The full goal remains active.

## Incoming settlement and original-boot continuation

Implementation `d552e43b` advances M2 beyond preparation. An already admitted
repair can now finish through RestartPending, including placement before Raft
Begin, dormant metadata registration, fresh learner seeding, joint completion
and terminal settlement. The membership authorization pins the resource
reservation's original destination boot. Current trust must match that pin;
registration and fresh seeding also recheck the still-active canonical debit.
Fresh placement remains excluded. Standalone low-level permits retain their
existing API but cannot grant managed physical admission without this proof.

`pending_restart_work` checks the exact active operation and full namespace
inventory at one current root. It counts all reserved births involving the
target, returns the next scope and lists unresolved debits and movements. A
resource-only reservation involving a hosted history stays visible even if its
destination differs from the maintenance target. Bounded pages retain only one
pending birth and the active resource book; stale roots, missing namespaces and
unknown enrollment successors refuse. Empty status is not live readiness.

`recover_birth_reservation` reconstructs a lost capability from the original
admitted mutation and permanent journal, exact index changes, namespace root,
schema and birth digest. The reopened controller can contend on the existing
namespace name key and confirm its actual Live or Cancelled winner. A live birth
cannot be erased and a cancelled name cannot be recreated. Lost confirmations
reconcile the same request and retain pending references meanwhile.

**134 distinct cases pass on this source, in 137 executions:** four new
settlement cases, 39 movement, two preparation, four gate, thirteen catalog,
25 low-level permits/import coverage, five CLI/operator and 45 library fleet
cases. Three settlement cases also occur in the movement cohort and are counted
once. The new native cases verify a previously unstarted learner and an actual
joint transition finishing after exclusion, SQL continuity, refusal of unrelated
metadata/new resource admission, retained resource-only debt, both birth winners,
controller reopen, lost confirmations and stale/incomplete-input refusal.
All-target host/admin Clippy passes with warnings denied. The audit matches all
**1,493 frozen Rust/Cargo/configuration inputs** to the implementation commit.
Earlier 200-case qualification belongs to its separately recorded source below.

Formatting/layout for 1,399 Rust files, native dependency checks, documentation,
ten plan tests, the 339-case manifest and source preflight with 95 anchors pass.
The plan baseline now references this implementation. Static-check outputs are
retained in `restart-settlement-checks-v1/`; documentation-only publication
checks remain separately attributed. Latest-head CI and deployed resource,
performance and soak qualification are still pending.

Primary executable/output evidence is retained outside runtime data in
checkout-private `qualification/fleet-operations/restart-settlement-v5/` and
`restart-settlement-followups-v1/`. Earlier failed probes are retained, including
observer/setup issues and learner refusal before the original-boot proof reached
every admission boundary. The v4 run was deliberately stopped before tests to
preserve standalone permit compatibility. The final joint fixture enters real
Joint through the existing managed repair APIs before exercising restart
continuation. No deadline, startup/stack bound, resource bound or runtime
assertion was relaxed. These are shared-host native tests with an in-process
fault-injecting object store, not deployed RustFS resource/performance proof.

Next feature delivery remains sequential:

1. Compose owned settlement turns, then complete the bounded hosted/pending
   history walk, fresh proof from both surviving voters, matched/published
   progress, observed leader handoff and revalidated RestartReady.
2. Deliver guarded maintenance stop/join, retained-engine return under a fresh
   process, catch-up/capacity resume, grace/second-failure/rolling execution and
   the SDK/admin/CLI maintenance workflow.
3. Finish production failure classification and trusted isolation, then scaled
   convergence/desired capacity, measured balancing/resize and enforced
   RustFS/Colima traffic, fault, TPS/p99 and soak qualification.

M2 controller composition, current runtime configuration inspection and M3–M9
are not complete. The full goal and existing task statuses remain active.

## Current main and post-response background work

Implementation `4d7ba0fd615874b60738ded7874c54a335f4e5ff` includes main
`927d3f34` through merge `6816581f`. The RPC tunnel conflict preserves main's
receiving quotas, original deadlines and reply ownership together with
maintenance admission. A maintenance refusal closes its RPC reply before any
request-buffer reservation. This is branch integration, not PR merge.

A new native regression exposed a gap after an HTTP response had completed:
accepted `wait_until` work attempted its SDK write after the gate had sealed SDK
admission and received Unavailable. The retained pre-fix case fails at that
exact boundary. The gate now waits for complete application event lifetimes
before sealing SDK admission, then samples work again after the registration
barrier. The same case commits the background write, completes quiescence,
refuses new SDK work and verifies SQL continuity after cancellation. The earlier
`f0ea0f71` qualification below covered held handlers, not this post-response edge.

**200 distinct focused cases pass:** twelve evacuation/preparation/gate, thirteen
admission/lifecycle/transport, 111 RPC, 28 archive/recovery, seven capacity, five
accepted-work shutdown, six external ingress, sixteen effects/alarms and two
background-SQL cases. Separately repeating four gate and two preparation cases
gives 206 candidate case executions; duplicates are excluded from the distinct
count. RPC coverage includes request/reply quotas, retained aliases, upload
bounds, caller deadlines and stalled reply retirement from the merged main.

All-target host/admin Clippy with warnings denied, Rust formatting/layout for
1,395 files, native dependency checks, documentation links, ten plan tests, the
335-case manifest and source preflight with 95 input anchors pass. The audit
matches all **1,489 frozen Rust/Cargo/configuration inputs** to `4d7ba0fd` and
verifies retained executable and output hashes. Current artifacts are outside
runtime data in checkout-private `qualification/fleet-operations/` under
`restart-gate-v7/`, `restart-gate-followups-v5/`, `restart-gate-checks-v3/` and
`restart-gate-plan-checks-v3/`. The failing pre-fix native executable, inputs and
output remain in `restart-gate-background-repro-v1/`.

No deadline, startup/stack bound, value bound or runtime assertion was relaxed.
These are shared-host native tests with an in-process fault-injecting store.
Full latest-head CI and deployed RustFS/Colima limits, TPS/p99 and soak remain
unqualified. The gate still grants no RestartReady, stop, new boot or capacity.
M2 incoming-action settlement, complete fresh both-survivor proof, owned observed
handoff/readiness, guarded stop/rejoin and operator surfaces remain next.

## Reversible maintenance admission

Implementation `f0ea0f7102e24206533f1e177bc674c62206fcd0` adds a physical
maintenance gate after M1 preparation.
A higher rooted node epoch makes preparation-only cancellation refuse. The
engine syncs a distinct maintenance selector before new public and peer
application admission close. The Node driver lets accepted handlers finish
before sealing SDK business admission, then waits for registered application
work. Consensus and control remain running. Gate metadata cannot stop the Node
or permit a fresh boot.

Cancellation owns a synced exact-process fence before Ready and shared-slot
release. It preserves permanent shutdown, retained charges and the need to
requalify capacity. Lost replies and a changed-root retry retain the exact child
fence; historical retries cannot release a later slot. The generic admin action
retains Operate permission and an explicit `fleet_quiesce` audit label. The full
maintenance SDK/admin/CLI workflow is still unfinished.

**88 distinct focused cases pass on the committed source:** eleven
evacuation/preparation/gate, thirteen admission/lifecycle/transport, 28 archive
and recovery, seven capacity, five accepted-work shutdown, six external ingress,
sixteen effects/alarms and two background-SQL cases. The three gate cases and two
preparation cases are also run separately, for 93 candidate case executions;
they are not added twice to the distinct count. The gate cases verify accepted
HTTP → SDK write completion, continued consensus and actual leadership transfer,
refusal of stop from gate metadata, timeout cancellation, changed-root fencing,
lost confirmation recovery and protection of a later operation's disruption slot.

All-target host/admin Clippy with warnings denied, Rust formatting/layout, native
dependency checks, documentation links, ten plan tests, the 334-case manifest and
source preflight with 95 input anchors pass. The audit matches all **1,476 frozen
Rust/Cargo/configuration inputs** to `f0ea0f71` and verifies retained executable
and output hashes. Primary artifacts remain outside runtime data in
checkout-private `qualification/fleet-operations/restart-gate-v6/`,
`restart-gate-followups-v3/`, `restart-gate-followups-v4/`,
`restart-gate-checks-v2/` and `restart-gate-plan-checks-v2/`.

Earlier probes are retained. They found an exhaustive admin match missing
Quiesce, dispatch before the observer installed the new authenticated catalog
revision, and an oversized nested cancellation row. The final row references
its immutable canonical parent by ID and digest within the existing 4-KiB
value bound. The race harness also needed the store's release notification
when clearing its held CAS; its exact Conflict assertion remains. One isolated
diagnostic returned Unconfirmed in fixture initialization, before gate code;
the unchanged preparation control passes. Its cause is unresolved. The effects
selection initially expected fifteen listed cases and stopped before running
them; the corrected selection executes all sixteen. Plan input anchors exclude
manifest-created outputs, as required by the unchanged validator. No deadline,
startup bound, stack bound, catalog value bound or runtime assertion was relaxed.

These are shared-host native tests with an in-process fault-injecting store.
Full latest-head SDK CI and deployed RustFS/Colima resource limits, TPS/p99 and
soak remain release gates. No new throughput or independent-node capacity claim
is made. No raw evidence files are included in the PR.

The gate is a prerequisite within M2–M4. Next implement incoming-action
settlement, complete fresh both-survivor inspection and owned observed handoff,
then readiness and exact joined maintenance stop. Retained-engine rejoin,
capacity resume, grace expiry, second-failure/rolling execution and operator
surfaces remain part of the same unfinished goal.

## Intact restart preparation

Implementation `dac800a693b3f25cc99761f2d0f51ceb0045bd50` delivers the catalog's
M1 preparation boundary. It records exact process and grace intent, rereads
complete inventory and atomically claims the disruption slot shared with
evacuation while moving Active to RestartPending. Existing voters and consensus
continue. New incoming placement and capacity admission refuse. The requested
grace remains descriptive until a later owned stop is admitted.

A new boot cannot treat RestartPending as restart permission. Preparation-only
cancellation requires the exact unchanged original node epoch and returns Ready
without capacity or resource credit. Later physical gate/readiness steps must
advance that epoch before dispatch; they cannot use metadata cancellation to
reopen an admitted shutdown. Parent identity, process, grace and principal stay
immutable across retries. Historical admission/cancellation receipts cannot
rewrite current node state or release a later operation's slot.

The new native cases obtain an actual rooted capacity certificate for a running
leader, invalidate it at preparation, refuse a fresh incoming birth, preserve
all three original voters and verify SQL 7 → 12 → 21. Lost admission and
cancellation confirmations reconcile across adapter recreation; cross-mode
restart/evacuation contention and generic slot deletion refuse. A separate case
physically joins an ordinary shutdown, attempts intact-volume Resume with a
fresh process and confirms boot refusal without replacing rooted identity or
deleting storage. Its later-epoch injection also refuses preparation-only
cancellation. Deployment resource values in these cases are explicit trusted
fixture attestations, not measurements of enforced container limits.

**46 distinct focused candidate cases have passing results across the aggregate
and targeted rerun:** eight evacuation/preparation, 28 stop/replay lifecycle,
seven capacity and three operator departure/cancellation cases. The two initial
preparation probes are included in the eight-case cohort and are not added to
the distinct count. All-target host/admin Clippy with warnings denied, Rust
formatting/layout, native dependency checks, documentation links, ten plan tests,
the 329-case manifest and 89-anchor source preflight pass. Static-check outputs
are retained in `restart-preparation-checks-v1/` beside the native runs.
The audit matches all 1,471 frozen Rust/Cargo/configuration inputs to `dac800a6`
and verifies native executable, CLI-child and output hashes. Later changes are
documentation/manifest updates only.

The broad stop/replay cohort initially passed 27/28. The existing isolated-removal
failed-survivor case returned Unconfirmed; it passes unchanged in the retained
candidate (4.39 seconds) and pre-change control (3.83 seconds). The cause remains
unresolved. An earlier strengthened preparation case sampled local enrollment
before the capacity revision reached its cached trust view. Its precondition now
uses the authenticated minimum-revision observation; the exact epoch assertion
remains. Clippy also rejected a direct test filesystem check; retained storage
is now checked through injected execution-domain filesystem metadata. No deadline,
stack bound, production guard or assertion was relaxed. All original outputs
remain outside runtime data beside retained executables in checkout-private
`qualification/fleet-operations/restart-preparation-v1/` through
`restart-preparation-v4/` and `restart-preparation-followups-v1/`.

This M1 boundary did not complete maintenance delivery. The later reversible
gate is described above. M2–M9 still require incoming-action settlement, observed
handoff and both-survivor readiness, guarded maintenance stop, intact-engine rejoin/capacity resume,
grace-expiry and second-failure handling, rolling execution and operator
SDK/admin/CLI surfaces. No new throughput or independent-node resource claim is
made. Full latest-head SDK CI and deployed qualification remain release gates.

## Cancellation and fresh departure recovery

Implementation `641d259687b7f9ef6890e2826eb56609a48fbaec` adds audited
`cancel-evacuation` through SDK/admin/CLI. Before finalization, cancellation
requires the exact rooted exclusion and grants no placement. After finalization,
the catalog owns an authenticated exact-process fence RPC and requires a synced
acknowledgement before atomically rooting Ready, its cancellation receipt and
disruption-slot release. An accepted stop cannot be cancelled; permanent
quarantine cannot be undone. No resource reservation, retained binding, archive
or volume is deleted. The higher Ready epoch invalidates prior capacity.

Both parent cancellation identity and child fence identity remain explicit.
An unknown commit retries the same parent payload. A definite changed-root loss
requires a fresh parent revision/ID while retaining the child fence for that
unchanged process/epoch. Caller-provided acknowledgement fields are rejected.
Historical committed retries need no observer and cannot restore Ready or
release a later operation's slot. Current status separately exposes historical
cancellation and the current node state, including later terminal removal.

The new native scenario passes on the retained committed-source binary:
new cancelled births invalidate a fresh stop; a held cancellation CAS loses to
another history after its real process fence; a fresh parent with the same
child fence reconciles a lost confirmation after management restart. Fresh
capacity requalification and a new operation then replay all four histories,
reject delayed old stop/fence requests, join the real owner, remove it and verify
archive-backed SQL continuity. Separate targeted cases pass for retained resource-only debt,
accepted-stop refusal and permanent-isolation refusal. CLI and admin cases pass.
**201 distinct focused candidate cases pass across the aggregate and targeted
rerun:** 63 resource/movement, 28 stop/replay lifecycle, six evacuation,
75 archive/recovery, seven capacity, five CLI-process and seventeen admin cases.
The audit verifies all 1,466 frozen Rust/Cargo/configuration inputs against
`641d2596`, plus retained executable, CLI-child and output hashes. The four
initial focused probes duplicate cases in these cohorts and are not added to
the distinct count. These native tests use the in-process fault-injecting object
store; they do not qualify a deployed RustFS fleet.

All-target host/admin Clippy with warnings denied, Rust formatting/layout,
native dependency checks, documentation links, ten plan tests, the 327-case
manifest and 89-anchor source preflight pass. Source anchors at `eebf1d76`
include the admin workflow documentation; its Rust inputs remain `641d2596`.
Qualification artifacts are outside runtime data in checkout-private
`qualification/fleet-operations/cancellation-operator-v2/`,
`cancellation-operator-followups-v1/` and `cancellation-operator-checks-v1/`.
Earlier probes and their failures remain beside them. Raw evidence files are
not included in the PR. Full latest-head SDK CI and deployed RustFS/Colima
resource-tier, throughput/p99 and soak qualification remain release gates.

The first broad resource attempt passed 62/63. The existing graceful-departure
case timed out while waiting for keep-set redistribution. That same case passes
unchanged in the retained candidate (19.71 seconds) and pre-change control
(17.45 seconds), with each control/candidate CLI child matched to its own retained
source. The original failed aggregate remains retained. This comparison does
not establish the timeout's cause or qualify deployed performance.

Initial probes also encountered native startup/availability failures. The
retained pre-change control failed in base startup as well; a later unchanged
candidate run passed the complete new sequence. The first accepted-stop probe
mistakenly selected the archive harness and ran zero tests. It was corrected to
fleet enrollment, where the real case passes. Final qualification now explicitly
requires a nonempty executed test selection. Original failures, the zero-test
selection and all retained executables remain outside runtime data. No startup,
request, stack or timing bound was increased, and no runtime assertion was
weakened to obtain a pass.

## Graceful departure operator delivery

Operators can redistribute a healthy keep set excluding a departing node, root
its `Draining` exclusion, verify all histories, fence its exact process,
explicitly request stop under a separate grant, and consume its signed joined
closure in terminal removal. SDK/admin/CLI requests retain catalog pin,
principal, expected revision and canonical request identity. No serialized
candidate or closure can substitute for the host-owned proofs.

One bounded in-memory walk checks one history per successful request. A
management restart repeats it; already committed finalization/removal reconciles
from the journal. `--wait` advances acknowledged partial verification only and
returns immediately after unknown outcomes. The CLI checks phase, process and
stop selector as well as request identity. Status uses descriptive metadata
instead of boot authorization, so Finalizing/Removed stay visible without
permitting a boot. Complete unresolved reservation charges are exposed even
when they have no executable movement. Partial verification releases neither
those charges nor the disruption slot. Volumes and archives remain retained.

The management server must explicitly supply a surviving observer and bounded
replay services. The archive handle binds to the management execution domain,
including SDK calls outside an HTTP task. Existing-root standalone connections
still refuse live proof and stop dispatch without those services; exact
committed receipt reconciliation remains available. This is the graceful
reachable-node path. It does not supply production isolation or same-identity
maintenance/rejoin.

Merged implementation `5a5f5fcb2b5fc75677bfc880a85f5c1a41d8760a` includes main
`5db6b475` and the departure feature `cfcb54b5`. It passes **166 distinct focused
cases**: all 62 resource/movement cases,
75 archive/recovery cases, five CLI-process cases, seventeen admin cases and
seven capacity cases. Two new native probes and five unchanged candidate
regression probes also pass; five comparisons against the retained pre-change
binary pass. The full resource rerun took 437.02 seconds and archive/recovery
69.74 seconds. All 1,464 frozen Rust/Cargo/configuration inputs match that source
commit, with native executable, CLI-child and output hashes checked. The primary
artifacts remain outside runtime data under checkout-private
`qualification/fleet-operations/departure-operator-v3/`,
`departure-operator-followups-v1/`, `departure-operator-checks-v1/` and
`departure-operator-console-v1/`. No raw evidence files are included in the PR.
The departure probe completes in 16.36 seconds: two real RF3 movements exclude
a former voter, lost exclusion/finalization/removal confirmations reconcile,
replay repeats after management restart, forged proof fields and wrong stop
process refuse, the real Node/domain owner joins, and surviving SQL advances
7 → 12 → 21 → 28. Descriptive status remains available after removal while the
boot guard refuses it. Ten mismatched/unknown HTTP receipt variants each cause
exactly one CLI request. All-target host/admin Clippy with warnings denied and
console TypeScript checking pass. Deployment throughput and scale qualification
remain pending.

The first aggregate resource attempt passed 57/62. Five existing cases failed
with a deadline, unavailable readiness or an ambiguous command response: isolation
completion retry, topology lost replies, collection under writes, collection
shutdown and operator convergence. The host had about 22.6 GiB swapped out.
All five cases pass unchanged in both the candidate and retained `4c5456d6`
control binaries, and the complete unchanged candidate rerun passes 62/62.
These observations establish no reproduced regression; they do not prove that
host pressure caused the failures. The original failed aggregate, all comparison
runs and immutable binaries remain retained. No timing budget was enlarged.
Rust formatting/layout, native dependency checks, local documentation links,
ten plan tests, the 325-case manifest, 89-anchor source preflight and console
TypeScript checking pass. Full latest-head SDK CI remains a release gate.

The original departure probe reached terminal removal but its status read used
boot authorization and therefore refused Removed metadata. The read now uses a
private descriptive node projection; the prohibition on boot remains tested.
A second probe initially reached a stale-peer refusal after exclusion and before
resource admission. The existing authenticated discovery barrier now precedes
that admission. The final debt oracle checks retained full charges and refusal
of terminal removal after partial proof. Unrelated charges do not serialize
historical proof reads or become reclaimable credit. Original binaries, outputs
and frozen input hashes remain outside runtime data; no deadline was enlarged
and no safety guard was bypassed.

## Next feature delivery order

| Priority | Feature boundary | Deliverable acceptance |
| --- | --- | --- |
| 1 | Maintenance/rejoin M2–M9 (FO-07/FO-08) | M1 catalog preparation and original-epoch cancellation are implemented. The reversible physical gate and its synced cancellation are implemented. Next settle incoming actions and complete observed handoff/both-survivor readiness, then exact joined stop, retained-engine restart with a fresh process and verified Active rejoin through SDK/API/CLI. Hold the shared disruption slot through catch-up/capacity requalification; complete grace-expiry, second-failure and rolling execution. Departure cancel-and-restart cannot undo an accepted stop; in-place certificate refresh is not implemented. |
| 2 | Production failure policy (FO-06) | A configured trusted platform adapter supplies exact-boot isolation. Suspicion/timeouts cannot authorize it. Admit automatic repairs only with the existing quorum, inventory, retained-charge and two-action bounds; simultaneous failures with lost quorum refuse. |
| 3 | Scalable convergence and desired capacity (FO-04/FO-09) | Bounded collection/planning at 500 and 1,500 histories, invalidation on changed rooted inputs and renewed live proof. Resolve the documented planner charge and retained-state-fit gaps through measured bounded work, before claiming those fleet sizes. Fewer cells alone cannot grant physical capacity credit. |
| 4 | Workload balancing and resize (FO-05/FO-07/FO-09) | Fresh role/load costs, measured leader-transfer benefit, and safe profile grow/shrink through maintenance/rejoin. Optional performance changes require matched throughput or latency gains. |
| Release gate | Deployment qualification (FO-10) | Runnable native management-service wiring, RustFS/Colima fleet changes under traffic, enforced 1-vCPU/3-GiB node limits, faults, throughput/p99/overhead and soak. Shared-host native correctness probes do not establish independent-node capacity. |

## Add-capacity operator delivery

Node add/status/qualify/activate now has typed SDK contracts, authenticated
admin routes and actual CLI commands. Distinct `fleet_register` and
`fleet_capacity` grants precede durable canonical audit. The capacity-granted
operator explicitly attests deployment limits; the catalog still owns full
inventory, exact boot/profile and authenticated live checks. An embedded
management adapter can install a running observer explicitly. Existing-root
standalone connections retain no observer and refuse capacity admission.
Registration alone grants no capacity and provisions no infrastructure.

Historical node admissions retain original request identity, action, state and
applied revision. Current status independently resolves exact rooted capacity;
a new boot cannot inherit it from an old Active descriptor or old receipt.
The CLI validates response catalog, node, action and identity before displaying
success. Accepted audits cannot be downgraded by a late failed retry. Browser
permission types include the new independent grants; no browser mutation UI is
implied.

Implementation source `4c5456d6648a76e86ce63fc2c1f52dc49ec64990` passes
**88 distinct focused cases**: all sixty
resource/movement cases, seven capacity cases, four CLI process cases and
seventeen admin cases. Two additional native probes pass before the complete
cohort. The resource cohort took 417.47 seconds; the targeted six-node workflow
passes in 17.50 seconds with three generated movements. All-target host/admin
Clippy with warnings denied, Rust formatting/layout, native dependency and docs
checks, ten plan tests and 322-case manifest validation pass. Console TypeScript
checking also passes. The audit matches all 1,455 frozen Rust/Cargo/configuration
inputs to that commit, checks the frontend inputs and verifies retained native,
CLI-child and output hashes. The 89-anchor preflight is refreshed after reviewing
the eight changed anchors. Source and immutable binary/output attribution are recorded
outside runtime data under checkout-private
`qualification/fleet-operations/node-operator-v2/`, `node-operator-checks-v2/`
and `node-operator-console-v1/`. Full latest-head SDK CI remains open.
Scenarios exercise lost confirmations in all three mutations, role and
catalog/payload rejection, adding a real sixth node then moving a voter onto it,
RF3/S3 SQL continuity, and actual engine restart with stale evidence refused and
fresh requalification. Separate checks cover mismatched successful HTTP
receipts and audit monotonicity. Console TypeScript checking passes.
The initial add-capacity scenario twice reached its 40-second redistribution
wait without completion. Its request also excluded both spare nodes, reducing
the keep set to four. A direct eight-second preview returned
`Topology(BalanceUnproven)`: retained old bindings leave no fresh target for a
required recovery witness. The corrected add-capacity scenario keeps all six
nodes and retains the four-node preview refusal as a negative assertion. It
requires three generated moves including the new node; no deadline or safety
bound changed. The original binary independently passes reboot, CLI, admin and
seven existing capacity cases. Original failures and the diagnostic binary remain
outside runtime data. Temporary diagnostics were removed before final qualification.
No deployment capacity, throughput or production platform provider is claimed.

The graceful departure workflow composes the capacity, placement and existing
fenced inventory/closure boundaries. Maintenance/rejoin and safe cancellation
remain the next lifecycle features. Capacity activation remains distinct from
balanced placement and storage-release authority.

## Independent required repair with retained debt

The required-repair controller now uses an admitted action's retry/backoff window
for one independent selection turn when a local slot remains. Due admitted
effects retain priority. The existing two-action global and per-node limits
remain unchanged. Healthy topology collection keeps its stricter empty-book
contract.

The selector accepts unrelated reservations only through complete scoped
ledgers. Shared charge construction gives planning and final atomic admission
the same runtime/attested/floor high water and full unresolved peaks. Old-boot
materialization cannot create credit; a rebound engine refuses progress. A
reservation for the current history still prevents another admission. A busy
initializer can remain on a surviving node only when a rooted charged row
covers its exact history digest, and it cannot receive another learner.
Physical settlement and joined worker-closure requirements remain unchanged.

Three native scenarios now cover the missing workflow: a real receiver seed
read held while another admission overlaps its initializer, two controllers
racing the last slot and recreating their workers, and an uncertain admission
fenced while a different real Seeding movement remains charged. They compare
the old book/debit bytes after independent settlements, verify RF3 and S3 SQL
continuity, then release and finish the original movement. A dedicated RAII
counter proves the exact held read stays owned after its waiter is dropped.
Four additional pure cases cover retained growth, slot exhaustion, old-boot
credit and engine rebinding. Implementation source
`0db5164b550ff53c77c9728aba121698728840f6` is recorded in the retained audit.
The frozen implementation passes **77 distinct focused cases**: all 58 native resource/movement cases, fifteen pure selector
cases and four service units. The three new native scenarios also pass as
separate probes before the full cohort. The final native cohort took 386.42
seconds. All-target host/logic Clippy with warnings denied passes. The source
audit matches all 1,442 frozen Rust/Cargo/configuration inputs to that commit
and verifies retained executable/output hashes. These are local correctness
checks, not deployed failure, scale or throughput qualification.

Merge source `2c1f53fdb2d1f9eb7645eeb0158ee3003e5b76a5` incorporates main
`b3ff49a6` and resolves the archive publication conflict by retaining independent
per-cut completion on an owned execution-domain blocking worker. Snapshot cleanup
stays off the host loop; the batch still joins before shutdown completes. All
75 archive tests pass, including main's two snapshot-close cases and this
branch's partial-batch/stalled-peer cases. The merged source passes **152
distinct focused cases**: archive 75, resource/movement 58, selector 15 and service
units four, plus the three repeated native probes. The resource cohort completed
in 394.69 seconds. All-target host/logic Clippy with warnings denied passes.
The audit matches all **1,451 frozen Rust/Cargo/configuration inputs** to this
commit and verifies immutable executable/output hashes. Primary artifacts remain
outside runtime data under checkout-private
`qualification/fleet-operations/repair-fairness-merged-v1/` and
`repair-fairness-merged-checks-v1/`. Earlier pre-merge runs remain separately
attributed above. Full latest-head SDK CI and deployed qualification stay open.
Repository formatting/layout, native dependency and docs checks,
ten plan tests and 318-case manifest validation pass. The 88-anchor baseline
was refreshed after reviewing the five changed anchors. No raw evidence is added
to the PR.

The first probe stopped before reaching fairness: the fixture tried immediate
execution before peers installed the new permit cut. Targeted observations
showed every peer at revision 26 while admission was at 27; the existing
readiness barrier advanced all to 27. Test readers now use independent catalog
clients so active RPCs cannot collide with their local admission mutex. This
retains the same object-store root and CAS authority. A source-worker assertion
was corrected to observe the receiver's actual retained read owner. No deadline,
stack, quota, closure guard or correctness assertion was relaxed. Temporary
diagnosis logs were removed; prior attempts and immutable binaries remain
outside runtime data.

## Initial isolated-node repair delivery

`start_isolated_repair_service` now connects a rooted evacuation and permanent
exact-boot isolation to automatic source/spare selection, atomic resource/permit
admission and the existing joined movement/settlement worker. The pure selector
preserves both surviving voters, allocates above the current member high-water,
excludes occupied/retired bindings and respects capacity, headroom, rooted
keep/domain policy and capability compatibility. Admission independently renews
proofs and retains the selected reserve.

The initial worker walked one immutable candidate index per pass and checked each history
against the current root. Admitted work runs first. It keeps its cursor on an
admitted group until settlement removes the failed voter; minority or unconfirmed
groups are recorded and skipped so independent majority groups can progress.
The initial fairness covered unadmitted histories; an admitted stalled movement
blocked new selection until the book settled. The independent-repair delivery
above removes that limitation within the existing bounds. A competing-turn fence
CAS resolves prospective
old-root admissions without granting membership, isolation or resource-release
authority. Node stop and explicit service shutdown join callbacks and owned work.

The native scenarios cover actual failed-leader shutdown, three replacements
without business traffic, wrong-scope input, lost admission confirmation, worker
ownership and cancellation, an independent majority alongside blocked minorities,
waiting for trusted isolation, an incompatible high-capacity spare, and delayed
admission raced by a second controller. Each completed repair checks fresh RF3
membership and S3 SQL values 7 to 12 to 21. The stopped node's nonempty engine
marker and shared log remain retained. A further scenario fills a spare's
registration table with eight metadata-only histories; zero engine histories
must not admit another registration. Selection and admission check conservative
registration headroom separately from physical engine capacity and retained floors.

The initial native attempt reached three settlements and verified SQL, then
failed an incorrect test-only retained-log path (`raft` instead of `raft/log`).
The corrected five scenarios and focused old regressions pass at the preceding
source. An intermediate 55-case cohort passed 52 and failed three: two existing
fenced/aborted cases caught metadata incorrectly charged as engine occupancy;
the correction checks registration headroom independently. The minority fixture
also had an ambiguous initial SQL write before controller checks, whose cause
remains unproven. Phase context was added without changing assertions or budgets.
The corrected source passes all 55 native resource/movement cases, eleven pure
selector cases and four service unit cases: **70 distinct focused cases**. Three
additional targeted probes pass both cancellation regressions and the minority
scenario before the full cohort. The exact earlier binary also passes its
minority case in isolation; this does not establish the timing failure's cause.
The final native cohort completed in 345.86 seconds. No budget or assertion was
relaxed. Original failures and immutable binaries remain outside runtime data.

Rust implementation source `abec54c927e141d8a47cf1dc4107b249daa8a47e` also passes
all-target `cellar-logic`/`cellar` Clippy with warnings denied, Rust formatting and
module layout, native dependency and documentation checks, ten plan tests,
311-case manifest validation and 88-anchor preflight. The source audit matches
all 1,440 frozen Rust/Cargo/configuration inputs to that commit and verifies
retained executable/output hashes. Primary artifacts are checkout-private under
`qualification/fleet-operations/isolated-repair-runtime-v6/` and
`isolated-repair-final-checks-v1/`, outside runtime data. These are focused local
correctness checks; full latest-head SDK CI and deployed qualification remain open.

The service remains explicit and default-off. Exact declared capability-digest
equality conservatively excludes unknown namespace supersets. Deployment
resource/isolation attestations are fixture authority in local tests. Cursor
exhaustion is diagnostic, not an evacuation/removal/storage-release certificate.
This slice handles one explicitly rooted isolated node; automatic failure
classification, production isolation, simultaneous-isolation policy, full-fleet
recovery feasibility, lifecycle operator integration and deployed RustFS/Colima
scale, fault, throughput/latency and soak qualification remain unfinished.

## Large-inventory stress probe

FO-10 qualification has started; none of its release gates is complete.
The first manual five-node/500-history native attempt committed all 500 managed
births but failed during materialization with `Unavailable`, after the progress
marker for 350 completed histories. It exited after 827.85 seconds, before the
collection measurement. The exact failing history and stage were not retained
in that first error context. This is an unresolved stress failure, not a
500-history collection or fleet-throughput result. The smaller regression with
an added post-measurement archive catch-up check passed on the same binary.

This debug native fixture uses 20 ms Raft ticks and two ticks per heartbeat,
five times the production default heartbeat rate at 100 ms ticks. Its five nodes
share one runtime process on the shared host; the explicit 64 MiB control pools
and 1,024-group limits are fixture configuration, not enforced 1-vCPU/3-GiB
capacity or independent physical failure domains. Failure at this setting does
not determine the production-default capacity limit. No proof, command or turn
deadline was increased, and no 500-history result is claimed.

The follow-up prototype adds exact registration/activation/initial-write
context and a smaller 400-history resident-group probe that omits initial SQL
writes. That probe passed and joined shutdown in 762.80 seconds, with exactly
240 serving copies per node. It is not a matched control for the 500-history
failure: registered inventory size also differs, and shared-host timing is
uncontrolled. It neither reproduces the error nor establishes a cause. Expensive
empty-state startup remains an observation, not a diagnosed scheduler defect.
The prototype, source hashes, immutable binary and outcomes are retained outside
the repository; this inconclusive experiment adds no production change. The intended default configuration and complete convergence
still require separate verification. The explicit isolated-node repair service above is the next delivered feature
slice; scale qualification stays open. Preserve the failing run and immutable
binary in external `qualification/fleet-operations/scale-v1/`; the unsuccessful
OS stack sampling probe yielded no output and establishes no bottleneck.

## Explicit active-collection pacing delivery

`start_topology_service_with_collection` separates active collection from
movement and idle polling. `FleetTopologyCollectionOptions` bounds the interval
(50 ms–60 s), histories per round (1–16) and soft quantum (1–250 ms). Defaults
are 50 ms, 16 histories and 100 ms. Quantum checks occur between complete history
proofs; original deadlines, before/after exact-root checks and node-owned joined
cancellation remain intact. Failed rounds retain the original backoff. Existing
`start_topology_service` callers keep one history per movement interval.

The matched native workload uses five real nodes, eight RF3 histories, one hot
counter cell and sequential quorum-acknowledged SQL increments during the first
collection. It pauses before planning, verifies unchanged rooted intent and zero
admitted moves, checks the final counter/version, and joins the service/node
owners. Three runs per mode on the shared host produced these medians:

| Mode | First collection | Foreground write p99 |
| --- | --- | --- |
| Existing collection cadence | 7,182 ms | 68.534 ms |
| Explicit active collection bounds | 2,884 ms | 59.990 ms |

Collection time fell about 60%. Foreground throughput medians were 23.48 and
24.61 successful writes/s for this single-cell serialized workload; these are
not maximum fleet TPS measurements. No foreground regression was observed in
this small sample. Container limits, RustFS behavior and 500/1,500-history
convergence remain unqualified. Batch size alone at the original interval gave
only a small change within shared-host variation and is not a claimed win.

Memory admission now distinguishes intrinsic `Limit` from temporary `Overloaded`
without lowering retained charges or increasing node defaults. Tests exercise
five-node/500-history retained charges against 16 MiB and 64 MiB pools, temporary
pressure, release and overflow. These are budget ownership tests, not measured
process-memory or deployed-capacity evidence.

The focused runtime tests at source `0b0d278cb57eee502774a16be95404f4ed530800`
pass three workload runs, twelve existing topology cases, two operator scenarios,
three admitted-movement service cases and three collection/memory unit tests.
The fast API is exercised through actual generated moves and the authenticated
CLI/admin convergence scenario. The earlier batch-only experiment had one
admitted-service deadline failure; that same case passed in isolation on both
candidate and control, and the final focused service run passed all three.
The cause remains unproven and failed attempts are retained outside runtime data.

All-target host Clippy with warnings denied, Rust formatting/layout, native
dependency and documentation checks pass. Ten plan tests, manifest validation
and 88-anchor preflight pass. The focused audit matches 1,432 Rust/Cargo inputs
to that committed source and verifies retained executable/output hashes. These
23 test executions are the current focused regression scope; the older complete
cohort below is attributed to its original source.

## Operator SDK, admin API and CLI delivery

`cellar::sdk::fleet_operations::RaftFleetControl`, authenticated admin routes
under `/api/v1/fleets/{fleet}/raft`, and the dedicated `cellar raft fleet` CLI
expose keep-set rebalance, operation status, paginated movement admissions,
wait and cancellation. The existing-root server connection verifies the complete
namespace inventory without initializing roots or starting a controller.
The [runnable workflow](operator-workflows.md#delivered-keep-set-workflow) documents
configuration, permissions, catalog pins, exact retries and result phases.

Mutations require a catalog-generation digest, exact request ID and expected
root revision. Separate topology/cancellation grants precede durable audit;
server authentication supplies the principal. An existing audit request still
reconciles the real catalog, and a late failed retry cannot downgrade a confirmed
admission. Wait expiry leaves durable intent unchanged. Historical convergence,
cancelled partial work and physical resource release remain distinct.

Rust source `4051b2baa61f8b7b6c719d0fc1f39e7e72dd3420` passes **484 selected
native cases across the recorded cohort and unchanged read rerun**, including
all 141 required FO-03 cases, three CLI cases and two real five-node operator
scenarios, plus **20 admin tests**. The operator scenarios use two live RF3
histories, actual CLI child processes and authenticated TCP admin requests;
they verify lost replies, canonical retries, permissions, catalog mismatch,
pagination, partial cancellation and SQL values 7 to 12 to 21. Two pre-existing
process-test child entrypoints remain ignored for direct execution.

The first read cohort passed 20 of 22 cases; two failed during startup deadlines.
Both cases passed in isolation on candidate and published control binaries,
and the complete unchanged 22-case read rerun passed. Their cause remains
unproven. Original failures are retained and no startup budget was increased.
A separate fenced-movement probe reproduced a rejected activation with a charged
pending initializer and no physical history. That test now checks physical
absence throughout initializer exit, then requires zero slots before settlement;
production behavior, fencing and activation budgets are unchanged.

All-target host/admin Clippy with warnings denied, Rust format/layout, native
dependency and documentation checks, ten plan tests, manifest validation and
88-anchor preflight pass. The audit matches all 1,431 Rust/Cargo/configuration
inputs to the committed source and checks retained executables and output
hashes. JavaScript formatting (80 sources), 34 affected JavaScript fixture tests
and console TypeScript checking pass. Primary artifacts and failed attempts are
outside runtime data in checkout-private
`qualification/fleet-operations/operator-final-v2/`.

FO-08 remains in progress: node add/activate/drain/stop/remove/resize, dry-run
and rolling maintenance commands are unfinished. Failure-driven controller
recovery, leader/load policy, retirement and deployed RustFS/Colima resource,
performance, fault and soak qualification remain open. The local correctness
fixtures do not establish 500/1,500-group capacity or foreground TPS/p99.

## Durable desired placement and owned convergence

The current feature adds canonical topology operations, a persistent keep-set
placement fence and per-operation indexed movement admission. One root CAS
excludes departing nodes from new birth/member/resource references. Previously
admitted births and permits can finish; node states, epochs and physical guards
are preserved. Topology and evacuation admission serialize at the same root.
Generic edits cannot mint or erase the active operation, policy or progress.

Owned admission appends an exact movement reference in the same CAS as its
debit, book, permit and retained-binding index. Rooted status validates canonical
origin and current-transition journals. Completion consumes a current complete
no-move preview with renewed node freshness; cancellation first prevents new
admissions, settles prior effects and restores the earlier allowed policy while
recording partial moves. An old retry cannot clear a newer active operation.
Successful convergence retains its keep policy. None of these transitions
authorizes node closure, storage release or infrastructure deletion.

The explicit topology service shares the existing node-owned movement worker.
Admitted work has priority; an empty-book round collects at most one history,
refreshes deployment evidence at planning, then admits one generated move or
verified terminal transition. Root changes and settled moves cause recollection.
Conservative memory admission covers retained inputs and solver/output work;
physical blocking work retains its reservation even after waiter expiry.
This is count-based admission, not measured RSS or enforced platform capacity.

Child IDs bind the operation and exact root. A prospective immutable request
remains unresolved data. After a bounded competing-turn window, a distinct
superseding root CAS can resolve an unconfirmed old admission while the book is
empty. That CAS and the original old-root admission cannot both win. No Raft
permit is fenced or settled by this metadata transition. Actual admitted work
retains its original debt and must finish through the existing proof boundaries.

The feature hard-cuts to `/topology-intent-1` after `/topology-bindings-1`, retaining
numeric markers at 1 and requiring fresh matching coordination/data roots and
engines. Startup stays explicit and default-off. Rust source
`94eb26c01b4cf2c1ad9ef953dd3cad5c4a04501d`, including the latest `main` integration,
passes **479 selected native cases**, including all 12 topology scenarios and
all 141 required FO-03 cases. All-target host Clippy with warnings denied,
format/layout/dependency/docs checks, ten plan tests, validation and source-anchor
preflight pass. All 1,421 frozen Rust/Cargo/configuration inputs and the immutable
binary/output hashes were audited against that committed source. Two existing
process-test child entrypoints remain ignored for direct execution. Artifacts
and failed attempts are outside runtime data in checkout-private
`qualification/fleet-operations/topology-intent-v8/`.

The cohort is enrollment 155, Raft units 183, catalog 13, permits 25, directory 16,
host 32, execution 21, reads 22, six targeted archive/seed cases and six rooted
cases. Main integration also passes the 21 affected JavaScript fixture cases
and RPC TypeScript checking. Full SDK CI and deployed RustFS/Colima resource,
fault, TPS/p99, controller-overhead and soak qualification remain open.

The Linux concurrent reconciler case reproduced a standard-stack overflow during
setup. Caller-only boxing isolated the cause; boxing at the production API
boundary then passed the unchanged concurrent test. Original failure, phase
probes and both successful experiments are retained outside runtime data. No
stack, deadline, concurrency assertion or durability check was relaxed. This
isolated Linux result does not qualify the full SDK or new topology protocol.

A late failed activation can still own a pending initializer. Its charged slot
is not a durable history: the native test now checks complete physical absence
throughout initializer exit and requires zero slots before later admission.
Lost-confirmation faults are bound to the mutating task so background trust
refreshes cannot consume them. The existing generic catalog pagination case
caught placement validation running without an enabled policy; the inactive
path preserves its opaque-table contract, while rooted topology policy retains
all target checks.

## Next feature delivery order

The isolated-node repair service now delivers selection and admission with
retained unrelated debt, beyond the previous manual movement primitives. FO-03 is complete at its documented
boundary and is not the next implementation target. Keep-set operator access is
also delivered. The next work should extend these usable boundaries:

1. **Evacuation/removal operator workflow (FO-07/FO-08):** node add/status/
   qualify/activate is now delivered through the guarded SDK, authenticated API
   and CLI. Next connect evacuation admission, healthy redistribution, complete
   fenced inventory, joined stop and verified terminal removal to the same
   catalog-pinned audit surface. Show unresolved movement debt and planning
   refusals explicitly; the reduced four-node preview demonstrates that admitted
   capacity does not imply a provable requested layout. Cover ambiguous retries
   and verified rejoin. Repair cursor exhaustion cannot replace terminal proof.
2. **Failure policy and deployment isolation (FO-06/FO-07):** supply a real
   trusted provider, conservative suspect/failed policy, concurrent-failure rules
   and restart/rejoin protection. A timeout cannot establish physical isolation;
   a lost quorum remains blocked. The new controller handles one rooted isolated
   node operation, not arbitrary simultaneous failure management.
3. **Scalable convergence (FO-04/FO-06):** avoid complete recollection after every
   healthy-fleet move by retaining only journal-confirmed stable configuration;
   renew moved-group quorum and node capacity, and invalidate on external birth,
   membership, boot or policy changes. The explicit collection API removes the
   fixed per-history interval when selected; 500-history convergence still needs
   measurement. At five nodes/500 histories, existing retained planner charges
   total about 21.5 MiB before probes, above the default 16 MiB pool. This is a
   calculated admission bound, not measured RSS or a supported capacity claim.
4. **Maintenance and capacity lifecycle (FO-07/FO-09):** deliver guarded stop,
   verified rejoin and one-at-a-time rolling maintenance, then effective profile
   reconciliation, retained-state fit, safe resize and committed cell retirement.
   Placement completion cannot authorize deleting retained storage.
5. **Load/performance and release qualification (FO-05/FO-10):** measure the
   intended default configuration and enforced 1-vCPU/3-GiB RustFS/Colima topology
   at 500/1,500 histories. Diagnose the retained stress failure, then target
   measured scheduler, convergence and foreground TPS/p99 limits. Workload-aware
   leader balancing, RustFS fault tests, steady-state overhead and soak remain
   release gates. Small native correctness runs establish none of those limits.

FO-04, FO-06, FO-07 and FO-08 stay in progress until their full acceptance
conditions pass. Full SDK CI remains a release gate. Feature slices and test
coverage should continue to advance without relabeling unfinished packages DONE.

## Owned topology collection and generated-move delivery

The healthy-fleet path now connects authenticated physical inventory and current
typed capacity to three safe-ReadIndex observations per live history, automatic
assignment generation, independent resource/permit admission and the node-owned
movement service. The collector walks one history per bounded call, refreshes
node samples after the complete walk and runs the pure solver in joined blocking
work. Stable rooted configuration can outlive a single observation freshness
window; actual movement admission reacquires quorum and target capacity.

`FleetTopologyPreview::first_movement` describes only the first serial move at
its original inventory revision. After verified settlement, callers recollect
before admitting the next move. The native scenario executes two generated
replacements across five nodes and two live histories without caller-driven
repair phases, then verifies SQL values 7 to 12 to 21. A temporal scenario writes
during a walk longer than the node freshness window and verifies that the real
seed includes those newer writes. Missing inventory, unqualified capacity,
changed roots and unmaterialized genesis refuse progress.

Retired dynamic copies need permanent provenance because current Raft membership
prunes removed learner bindings. Movement admission now roots an exact
group/member/node lookup alongside its row, book and permit. Collection verifies
the original admission and retained terminal receipt even after a later move
supersedes the group's current permit. Physical occupancy and floors remain
charged. Only a journal-confirmed unused Fenced proposal may be replaced by a
later permit sequence on the same disk/engine; generic index edits refuse. This
schema hard-cuts to `/topology-bindings-1`, retaining numeric markers at 1 and
requiring fresh matching coordination/data roots and engines.

The later durable-operation section above connects this path to a persistent
placement fence and paced admission rounds, preserving admitted work and
reconciling uncertain outcomes. Operator add/drain/status/cancel should drive
that same operation. Changed/unavailable-boot recovery, maintenance/rejoin, workload
leader balancing and deployed fault/performance qualification remain required.
Healthy per-move execution does not complete FO-04 or enable automatic fleet
management.

## Automatic assignment generation delivery

`raft_fleet::balance_topology` now generates a complete equal-copy RF3 assignment
for an explicit final keep-set. Kept nodes receive the floor or ceiling of the
average live-copy count; departing nodes receive zero. Remainder allocations
prefer existing serving copies, with deterministic node-ID ties and alternate
allocations when blocked. Direct surplus-to-deficit moves preserve retained
source occupancy, reject occupied targets and respect current receiving
eligibility. The existing topology verifier checks the entire staged path and a
complete direct RF3 restoration witness for every kept-node loss before any
result is returned. The result pins input/policy revisions and exact capacity
scopes; it grants no catalog or membership authority.

One budget covers generation, forward checks, alternate assignments, staged
paths and recovery searches. Candidate storage is capped at one million actions;
iterative search frames are bounded by the number of serving copies. The first
contraction attempt exhausted the unchanged 50-million-unit budget by exploring
equivalent move orders. Distinct-history availability checks and constrained
destination selection fix that case without increasing the budget. Domain
constraints that may become legal after a different replacement are not used to
prune the availability upper bound. `BalanceUnproven` remains a constrained-search
refusal, not a global infeasibility proof. Neutral reshuffles, intermediate
storage, weighted assignments, workload scoring and leader balancing are outside
this API.

Rust source `57f988ed332be48cc866cdbfad081e5a1faa6e5f` passes all **76 ordinary
decision-core integration cases**, including 25 topology cases. Automatic
five-to-six expansion generates 250 replacements and 250 live copies per node;
five-to-four contraction generates 300 replacements and 375 live copies per
survivor. Each includes every final-node loss scenario under retained physical
charges. New cases also cover alternate remainder allocations, domain/recovery
refusal, occupied/excluded/capacity refusal, fresh complete stable input,
deterministic reordered input, exact shared-budget exhaustion, empty inventory,
no recovery spare and a 400-layout two-group matrix. The prior 400-pair explicit
topology matrix remains passing.

All-target decision-core Clippy with warnings denied, doc-test command,
format/layout/dependency/docs checks, ten plan tests, plan validation and source
anchor preflight pass. The doc-test command selects zero examples. All 1,616
frozen inputs remain unchanged through verification. Immutable binaries,
source manifests, logs and failed attempts are outside runtime data in
checkout-private `qualification/fleet-operations/balance-v4/` (earlier attempts
are retained alongside it). The ordinary test-target manifest disables internal
library tests; no external internal-test harness was enabled.

This delivered automatic candidate generation in FO-04. The later owned
collection section above connects its first move to guarded admission; durable
topology intent and automatic admission remain unfinished. FO-04 stays in
progress; optional FO-05 workload policy and FO-09
operator scaling are not completed by this pure planner. No deployed movement,
TPS/p99 or Colima resource qualification is claimed.

## Aborted-movement settlement delivery

`settle_aborted_movement` adds a distinct terminal receipt for the exact consumed
abort. All three original seed owners must independently observe its applied
`Aborted` phase, exact plan/permit and stable original RF3 configuration before
physical worker-exit samples. The destination is empty or retains the exact
admitted learner history. A removed learner may never receive the abort commit
index; its local phase is not used as terminal quorum proof. Its actual occupied
engine, deployment evidence and installed pools raise the retained physical
floor. The receipt exposes historical `retained_target()` and grants no deletion.

The same bounded root CAS retains the original debit/permit/journals, records
the typed result and removes only its exact active book reference. Canonical
retries precede live prerequisites, including after later admission and target
shutdown. Complete, Fenced and Aborted receipts cannot be reinterpreted. Explicit
reconciliation and the node-owned service drive this boundary without deciding
to abort a plan themselves. Unsettled seeding/joint work, running workers and
unproved boot changes retain their debt through this API.

Source `eb3c1a6d69b76d66dac2446bf2d413ff7aa412fd` passes **466 selected native cases**,
including all 141 required FO-03 cases and five new abort cases. The cohort is
enrollment 143, Raft units 183, catalog 12, permits 25, directory 16, host 32,
execution 21, reads 22, six targeted archive/seed cases and six rooted execution
cases. Two pre-existing ignored execution cases are excluded. All-target host
Clippy with warnings denied and repository format/layout/dependency/docs checks
pass. All 202 SDK v5 frozen inputs remain unchanged. Immutable binaries, frozen
inputs, logs and the audit remain outside runtime data in checkout-private
`qualification/fleet-operations/aborted-movement-v6/`.

The cases verify actual S3 seed/abort, retained physical history,
lost terminal confirmation, adapter recreation, distinct proof kinds/payloads,
unchanged historical begin retries, later capacity, target shutdown, original RF3
values 7 to 12 to 21, automatic empty-target settlement and rejection of late
activation, active/joint work, a source archive worker after waiter loss/deadline,
and an unavailable original owner.

The first attempt expected an error for an exact already committed begin retry;
the API correctly returned historical Aborted status. The test now asserts that
status, plan, unchanged membership revision and consumed sequence. The next
attempt reproduced a standard-stack overflow in the existing concurrent fixture.
Startup probes located it before reconciliation; measured reconciliation future
state was 26,824 bytes. Sharing one guarded terminal future restores the original
concurrent fixture on the standard stack (26,768-byte state). Uninstrumented
executor cases pass, and temporary probes are removed. No proof, concurrency
assertion, stack limit or deadline was relaxed; failed attempts are retained.

This schema hard-cuts to `/aborted-movement-1`, keeping numeric markers at 1.
Use fresh matching coordination/data roots and engines. Changed/unavailable-boot
recovery, trusted platform/closure integration, fleet planning/admission,
operator workflows and deployed qualification remain unfinished. Automatic
management stays default-off; no TPS/p99 or enforced Colima qualification is
claimed. Full SDK CI remains open.

## Owned admitted-movement controller delivery

`start_movement_service` now installs one explicit worker in the enrolled node's
joined host job set. It reconstructs admitted moves and advances them without
caller-driven phase loops. A private catalog turn uses the node's installed
backend and execution domain after matching the requested rooted fleet. A closed
starter capability refuses; caller and node domains need not be identical.
Periodic metadata reads cannot occupy the foreground catalog adapter's turn.

One round has one original discovery/evidence/execution budget and at most one
move attempt. Retained work is bounded to two reservations and two MiB of control
metadata admission. Failed rounds/moves use bounded exponential backoff and
positive jitter. Joint and terminal work gets priority, with another ready move
receiving a fairness turn after three priority selections. Missing book rows do
not count as completion: permanent settlement must be verified before reclaiming
a local slot. The abort settlement above supersedes this checkpoint's Aborted
cleanup gap; unproved boot changes still retain debt.

Explicit shutdown, handle drop and node drain stop the worker. Physical node
shutdown joins it through the existing job set, including a blocked deployment
evidence callback. Submitted seeds/configurations retain their existing physical
owners and durable intent. Restart reconstructs that intent rather than assuming
cancellation rolled anything back.

Source `c3f96398378dffddf9a11bc8c3a0129a90c7e6c5` passes **461 selected native cases**,
including all 141 required FO-03 cases and seven new service cases. The cohort
contains enrollment 138, Raft units 183, catalog 12, permits 25, directory 16,
host 32, execution 21, reads 22, six targeted archive/seed cases and six rooted
execution cases. Two pre-existing ignored execution cases are excluded.

The new cases verify real remote S3 execution after service restart and a lost
terminal response; cross-adapter singleton and closed-starter refusal; node
shutdown joining blocked evidence; rebound-evidence refusal; bounded options,
missing-work retention, joint/fairness scheduling and saturating retry jitter.
All-target host Clippy with warnings denied, formatting, layout, dependency and
docs checks pass. Immutable binaries, frozen inputs, logs and the qualification
audit remain outside runtime data in checkout-private
`qualification/fleet-operations/owned-service-v2/`. All 202 SDK v5 frozen inputs
remain unchanged. The first attempt's three cross-domain refusals are retained.
The production fix selects the enrolled node's backend/owner; no fixture
assertion, stack or deadline was relaxed.

This delivers automatic execution of already admitted work. New placement and
failure-driven admission, aborted/changed-boot cleanup, deployment isolation
integration, operator workflows and deployed qualification remain unfinished.
Startup stays explicit and default-off. Latest SDK CI reached deeper real-node
tests but failed on a container HTTP timeout and a Python-host stack overflow;
local native successes do not establish full SDK or deployment qualification.

## Explicit resumable movement execution delivery

`pending_movements` reconstructs the bounded active membership work from the
canonical resource book and journals. `reconcile_movement` uses fresh quorum
progress to drive guarded begin, actual learner activation, joint completion and
verified terminal settlement. An enrolled origin with zero business groups can
coordinate all remote effects. The receiving process independently resolves the
exact active reservation, resource digest, history, rooted permit and both boots.
Purpose/channel/nonce/body-bound controls never accept caller-supplied Raft
configuration. Local fallback supports the destination as the controller origin.

Each invocation tries at most four pinned leader candidates with a 500-ms probe
budget each, then performs at most one seed and one configuration action or one
terminal settlement CAS. The reservation, committed Raft phase and permanent
receipt are the durable cursor. Returned phase is observed before the action;
`settled` requires the terminal proof. Recreated adapters recover pending work or
find its admitted receipt before live prerequisites, including after a later
reservation and destination shutdown. Aborted progress remains unsettled.

Rust source `c115b4a1591dd63a91fdd14943005b19ee89827d` passes **454 selected native cases**:
enrollment 135, Raft units 179, catalog 12, permits 25, directory 16, host 32,
execution 21, reads 22, six targeted archive/seed cases and six rooted-execution
cases. All 141 required FO-03 cases and six new FO-06 cases are covered. The new
composed cases verify remote S3 restoration, adapter recreation each turn,
duplicate controllers consuming one permit, uncertainty after terminal CAS,
later capacity admission, retained later work, and a coordination-read outage.
The restored target serves an S3-acknowledged value 12 through an AtLeast token;
a fresh majority write reaches 21 after that target stops. Wire tests cover
purpose/channel/body/reply binding, exact boot scope and original deadlines.
All-target host Clippy with warnings denied, format, layout, dependency and docs
checks pass. Two pre-existing ignored execution cases are outside the count.

SDK successor `ceb3e5c15e040d88ec54379b33bd16e3ec3a8484` preserves every frozen Rust/configuration input.
It fixes inherited non-DOM cancellation declarations and supplies the real
runtime stream/delegate factories to outdated test fixtures. All 3 streaming,
74 JavaScript-tooling and 65 RPC-fixture cases pass, as do SDK/RPC type checks and
closed guest builds. Primary immutable binaries, frozen inputs and logs remain
outside runtime data in `qualification/fleet-operations/movement-execution-v6/`
and `execution-sdk-integration-v5/`.

Initial failed attempts are retained. A composed test exhausted the standard
stack during fixture startup before execution; diagnostic future sizes were
113,616 bytes for the composed case and 26,808 for reconciliation. Heap-owning
the composed fixture fixes startup without changing production. The caller now
re-reads after typed uncertain storage responses. A separate next-admission
StalePeer correctly required actual discovery acknowledgement of the settlement
cut. All safety/value assertions and operation/cohort deadlines remain. Temporary
diagnostic probes were removed. Old-head CI failures are preserved separately;
local successes do not claim completion of the new GitHub CI run.

FO-06 is in progress. The owned service above supersedes this checkpoint's
polling gap. New assignment/admission generation, required versus optional
admission scheduling and terminal cleanup for aborted/changed boots remain. Automatic fleet
management stays default-off. No deployed RustFS/Colima or performance/fault/soak
qualification is claimed. The protocol hard cut ends in `/fleet-execution-1`;
use matching binaries and fresh coordination/data roots and engine directories.

## Fenced unused-movement settlement delivery

`settle_fenced_movement` adds terminal cleanup for an admitted move that never
started. Each original voter must independently apply the exact committed
cancellation fence before physical samples are collected. Every admitted boot
must still match, seed workers and initializers must have exited, and complete
destination inventory must prove no physical history binding. The common
settlement CAS retains the permit/resource journals and monotonic physical floor,
records a distinct fenced outcome and removes only the exact active book row.
A successful-replacement call cannot reinterpret a fenced receipt.

Three new native cases pass at source
`f4ef708d5f808650374d2bfb88949d4652a3b62f`: lost CAS confirmation, adapter recreation,
rejection of delayed begin/activation and proof/payload rebinding, later capacity
admission, historical retry after destination shutdown, retained floor/resource
records and continued business writes; separate cases refuse seeding/aborted work
and an unavailable original owner. The integrated source passes **442 fleet/core
cases**, including all 141 required FO-03 cases, plus **6 native rooted-execution cases**.
The cohort comprises enrollment 132, Raft units 176, catalog 12, permits 25,
directory 16, host 32, execution 21, reads 22 and six targeted archive cases.
All-target host Clippy with warnings denied, formatting, layout, dependency and
documentation checks pass. Frozen inputs, immutable binaries, logs and the
source-specific audit remain outside runtime data in the checkout-private
`qualification/fleet-operations/fenced-movement-v2/` and the unchanged-source
`fenced-movement-snapshot-v3/`. The two seed cases run in the snapshot harness;
the earlier zero-match archive filter is retained and excluded from the count.
The first focused attempt is retained: its lost-reply fixture passed a snapshot from the
old adapter to its replacement and correctly received a snapshot-owner refusal.
The test now obtains its replacement adapter's own snapshot. No production
check, deadline or assertion was relaxed.

This source integrates main `9f1e7df19d77bb7cc77a44fbb02c9f7bcbc0b491`, including
extent-backed execution. Dynamic managed registration explicitly retains the
public materialized-image selector. Fleet publication proof refuses an extent
root where replay requires a full-file SHA-256; a focused regression covers that
hash-domain boundary. The protocol hard cut now ends in
`/isolated-removal-1/images-1/fenced-movement-1`; use fresh matching roots/engines.
The merge-format failure and the subsequent compiler failures from old image
fields/storage initialization remain retained separately. The new hash-domain
regression passes in the 176-case Raft unit cohort.

FO-06 now depends directly on FO-04. Explicit resumable execution precedes the
optional FO-05 workload policy. Successful moves and fenced unused reservations
have terminal adapter paths; aborted/changed-boot cleanup, owned execution,
assignment generation, operator workflows and deployed qualification remain.

## Terminal isolated-node removal delivery

Source `cfda0e7823c72f49a10b90967104af077be19dae` adds a distinct terminal path for nodes
with admitted deployment isolation. `begin_isolated_finalization` validates the
exact quarantined boot and journal after complete survivor/archive verification;
it requires no RPC to the unavailable node and produces no graceful stop selector.
`complete_isolated_evacuation` renews current inventory, survivor quorum and
process proofs before one CAS records the isolation receipt, preserves the
quarantined disk/engine tombstone, marks Removed and releases the exact shared
disruption slot. The reachable closure-based path retains its signed proof and
public API. Typed selector/journal checks prevent switching evidence kinds on a
retry.

Three native cases pass on the frozen implementation: actual resource-backed S3
replacement through movement settlement, physical replay and terminal removal;
a later survivor failure; and a later incomplete namespace. The composed case
also covers lost finalization/removal replies, adapter recreation, rejection of
graceful proof substitution, admission of the next node operation, retry of the
old receipt without releasing the new slot, retained storage and subsequent
business writes. Primary source/binary artifacts are outside runtime data in
`qualification/fleet-operations/isolated-removal-v1/`. The pre-merge source
passes all 429 affected core/host cases and all-target Clippy plus repository
checks in `isolated-removal-v2/`.

Main `80026d16e951ecea785a817cbbd8b305a8f2d63e` was then integrated through
merge source `7ca741d46894a8744a54efe3410eb0b44a5898f0`. The fleet Raft and consensus API files are unchanged by
the merge; shared application, node, storage and binding changes are included.
The unchanged merged binaries complete **429 core/host cases** across the
initial and resumed runs: enrollment 129, catalog 12,
permits 25, directory 16, host 32, execution 21, reads 22 and Raft units 172.
Integration checks then caught an ignored authorizer-installation result in a
new-main SQLite test and an old typed-RPC test-helper call. Successors
`d5705f39249fbf7bc312460daa2b33dc67df56b7` and `c5f40e8cb7e24f91b2cb5e9b0a4f71fb1680bd88`
correct those fixtures while preserving all production Rust and Cargo inputs.

The SQLite successor passes six targeted empty-archive/publication-origin cases
and four binding tests. The final RPC fixture successor passes all three native
typed-RPC cases, all-target Clippy with warnings denied, formatting, layout,
dependency and documentation checks. The combined **435 fleet/core cases
cover all 141 required FO-03 cases**. The broad cohort was not repeated for these
unrelated test-only corrections. Frozen inputs, immutable binaries, logs and audit
remain outside runtime data in `isolated-removal-main-v3/`,
`isolated-removal-main-resume-v6/`, `isolated-removal-archive-v4/`,
`isolated-removal-compat-v7/` and `isolated-removal-permit-repro-v5/`. The original integration-check failures are retained. Two initial permit cases failed with startup
readiness and no-leader outcomes. The same binaries pass 20 exact differential
repetitions and all 25 permit cases without changes; their cause remains
unestablished and the original failure is retained. No
assertion, timeout or stack bound was relaxed.

FO-07 is now in progress for this delivered evacuation boundary. Rolling restart,
resize, reversible admission gates, cancellation and complete operator/controller
orchestration remain unfinished. Deployment isolation still comes from trusted
platform evidence supplied by the caller; no independent platform integration or
RustFS/Colima performance qualification is claimed.

## Isolated-node repair delivery

Source `3834c560e1215f7555ec796a50265df7f1a497f6` adds `attest_node_isolation`: after rooted evacuation
exclusion, a trusted deployment assertion permanently quarantines the exact boot.
The caller establishes process/worker shutdown and prevents restart; Cellar checks
scope and journal linkage. Saved startup capabilities and generic quarantine
clearance refuse. Exact lost-response retries preserve the original acceptance.
This does not create a graceful-shutdown receipt or reclaim the retained engine.

Resource-backed admission pins an isolated obsolete voter while requiring fresh
survivor quorum. Completion substitutes its admitted isolation proof only after
new RF3 excludes that exact member; surviving voters and target still supply live
physical inventories. Receipts retain isolation provenance and physical floors.
Three actual peer/S3-seeded replacements exercise recycling of the two-slot book
while the old node remains offline, with business writes after every replacement.

This work exposed a route dependency: fresh repair admission required endpoints
for every current voter, so quarantine prevented repair from starting. Placement
now uses the complete durable node set; network routes omit explicitly isolated
endpoints. The original voter still counts toward Raft quorum and failure-domain
checks until committed removal. The failing reproduction remains retained.

All eight affected core/host suites pass **426 cases** on that behavioral source:
enrollment 126, catalog 12, permits 25, directory 16, host 32, execution 21,
reads 22 and Raft units 172. Clippy then caught a test's direct filesystem call.
Successor `bdb20e2bcaf7e2be4e5ba52d1bfc6ef7d5ede000` changes only that retained-directory
assertion to use the injected filesystem through the execution domain. All
production Rust and Cargo/configuration inputs are identical. Its three isolation
cases, all-target host Clippy with warnings denied, formatting, layout, dependency
and documentation checks pass; the broad cohort was not repeated for this
test-only correction.

Frozen sources, logs and immutable binaries remain outside runtime data in
checkout-private `qualification/fleet-operations/isolation-v5/` and
`isolation-fixture-v6/`. Initial route failures, the missing test-import build and
the Clippy failure are retained separately. No assertion, timeout or stack bound
was relaxed. No deployed RustFS, enforced Colima limits, throughput/latency or
soak is claimed.
At that source, terminal isolated-node removal, deployment fencing integration
and an automatic controller remained separate work. The terminal path above is
the newer delivery; platform integration and automatic orchestration remain pending.

## Merged-main host qualification

Source `7863043eb3f91c35e8e1e4b1d82099363af6d1bc` integrates main
`d3206c924031a3f2ca3387b542c02877bb91423d`, including file-backed streamed
snapshots and the RPC scope/pump fixes. Dynamic history admission shares the
node's snapshot scratch pool. Streamed replay preserves canonical empty archives,
exact publication-origin positions and independent per-history batch completion.

Eleven complete core/Raft suites pass **569 cases**: enrollment 123, catalog 12,
permits 25, archive 73, snapshot 46, directory 16, Raft units 172, host 32,
execution 21, reads 22 and LTX integration 27. Local-store and cell-limit suites
add nine; four streamed snapshot/LTX suites add 20. All 141 required FO-03 cases
and ten movement cases pass. Ignored helper cases are not counted; the snapshot
helpers also execute through their passing parent crash matrices.

All-target Clippy with warnings denied, formatting, layout, native dependencies
and documentation checks pass. The source manifest freezes 1,252 Rust files and
21 Cargo/configuration inputs. Logs, copied immutable binaries, hashes and audit
are retained outside runtime data in `qualification/fleet-operations/` under this
checkout's private target, generations `movement-completion-merge-v8/` and
`movement-completion-merge-v9/`. Ignored local mirrors and supplemental regression
records preserve the earlier qualification records.

The v8 directory deadline test timed out waiting for its fault gate. The same
unchanged binary passed 20 focused repetitions and all 16 directory cases in v9;
its root cause remains unestablished. The original failure is retained separately,
as are the v6 missing scratch-field build failure and v7 invalid harness target.
No assertion, deadline or stack bound was relaxed. Other successful v8 suites
were retained; v9 completes the remaining checks on identical frozen inputs.
No evidence-directory files are added. Deployed RustFS, enforced Colima resources,
fleet TPS/latency and soak remain unqualified.

## Previous completion qualification

Source `c7a1152b7366a799cc7fe41b1bbda6d63493cd0c` passes all 141 required FO-03 cases and
seven complete suites: enrollment 123, catalog 12, permits 25, archive 58,
snapshot 46, directory 16 and Raft units 166: **446 passed**. Local-store and
native cell-limit suites add **nine passes**. Two snapshot helper fixtures are
intentionally ignored directly and execute through passing parent crash matrices.
The ten movement cases pass both focused and within the complete enrollment suite.

All-target Clippy with warnings denied, formatting, layout, native dependencies
and documentation checks pass on that source. All 1,210 Rust files and 21
Cargo/configuration inputs are frozen. Primary logs, copied immutable executables,
source hashes and the qualification audit are retained outside runtime data in
`qualification/fleet-operations/movement-completion-v4/` under this checkout's
private target. Small ignored mirrors and supplemental FO-01–FO-03 regression
records preserve the original completion records; FO-04 retains an explicitly
partial movement-completion record.

The first build's missing test import and the later full-boot record size refusal
remain separately retained in generations v1 and v3. Generation v2 qualifies its
superseded selected-source version only. The compact all-provider implementation
uses the existing 4-KiB value bound and passes the complete v4 cohort. Failed and
superseded attempts are not counted as the passing qualification. No evidence
directory files are added. RustFS, enforced resources, TPS/latency and soak remain
unqualified.

## Previous admission qualification and artifact retention

Behavioral source `07315b2c16c6ddad33118bd0a2afdf82230b2cfa`, including latest
main's local-store WAL fix and resource-backed membership admission, passes all
141 required FO-03 cases and seven complete suites: enrollment 117, catalog 12,
permits 25, archive 58, snapshot 46, directory 16 and Raft units 166, **440 passed**.
The local-store suite adds four passed cases and native cell limits adds five.
Two snapshot helper fixtures are intentionally ignored directly and run through
passing parent crash matrices.

That run stopped at formatting in the new movement test. Formatting successor
`397c70c384d5777fffb94563748902cb39b87db0` changes only that fixture's layout;
its bytes exactly equal rustfmt output from the behavioral source. All production
Rust and Cargo/configuration inputs are identical. The four movement cases,
all-target Clippy with warnings denied, formatting, layout, native dependencies
and docs pass on the successor. The full 440-test cohort was not repeated for
this formatting-only change. A duplicate source-anchor/output declaration was
then corrected in plan metadata; validation, source preflight and all ten plan
validator tests pass in the published metadata successor.

All 1,206 Rust files and 21 Cargo/configuration inputs are frozen with their
respective source pins. Primary logs, immutable executables, hashes and the
formatting audit are retained outside runtime data at
`qualification/fleet-operations/movement-v4/`, `movement-format-v5/` and
`movement-package-v6/` under this checkout's unique target. Small logs and exact
completion records are mirrored into ignored `.cellar/raft-perf` paths. Failed
formatting, fixture and metadata attempts remain separately retained and are
not counted as passing checks. Earlier artifacts deleted by operator cleanup
are unavailable. No files under `plans/multi-raft/evidence/` are added to this PR.

## Completed FO-03 lifecycle

Trusted enrollment, complete managed-history inventory, guarded initial voters,
dynamic registration and evacuation candidates now extend through terminal
removal. A recreated adapter resumes a rooted Finalizing operation without
changing its accepted stop authority. It owns current RF3 quorum observations
and physical archive replay at the fixed publication prefix. After replay it
renews every live group's quorum proof and probes survivor processes again.

Only complete current inventory and exact boot-pinned signed closure after
physical owners join authorize one CAS that records the terminal receipt,
changes the node to Removed and releases the shared disruption slot. Physical
history, disk/engine identity and resource reservations remain. Exact retries
reconcile the permanent request journal even after lost terminal replies.
Incomplete import, a later failed survivor or unknown physical closure refuses
completion and retains the slot.

The native composed case performs an actual S3-seeded voter replacement, joint
completion, scoped stop during survivor writes, physical joins, Finalizing
resumption, terminal removal, admission of the next disruption and another
business write. These local tests qualify the adapter, not the future automatic
controller or unreachable-owner isolation workflow. Deployed RustFS, enforced
resource limits, density, throughput/latency and soak remain FO-10 gates.

## Archive publication progress

Requalification after merging main reproduced an existing cross-history progress
barrier. The second cell's object-store head had advanced to position 2 while
the host still reported position 1. Batch-level result delivery retained its
producer slot behind another cell's stalled read-back. The upload coordinator
also stopped polling member publications while it awaited that read-back.

Archive batches now report each verified history independently and keep polling
member futures during upload. A completed history drops its recovery material
and releases its capture reservation after its own publication finishes. Its
small bounded completion returns to the host independently; the physical batch
owner stays in the Node's joined job set. Producer, byte and replay-workspace
bounds are retained. Shared payload dependencies still require their own
successful verification before any referencing head can advance.

A scoped head/payload gate deterministically exercises an independent empty
history while a data-bearing history stalls in upload verification. It failed
before the coordinator fix and passes afterward. The original native S3 write
case retains its progress deadline and durability assertions. Forty consecutive
runs passed on the unchanged production implementation at `47c7f7a1`; the
successors change the regression's selection macro to Cellar's required
execution-domain macro and separate stop-reply fixtures from physical join.
The retained self-stop waiter deterministically returns `Closed` after its own
response domain shuts down; closure stays unconfirmed until real physical
owners join, then the exact signed closure and survivor writes succeed. The
final cohort repeats both cases, including three runs of the deterministic gate. Failed intermediate runs and the Clippy macro
failure are retained outside runtime data; no failed attempt is counted as a
passing cohort. Temporary diagnostic logs were removed. This establishes
independent publication progress, not a fleet TPS or latency qualification.

## FO-04 resource-backed membership admission

`reserve_movement` combines an exact RepairPlan and source with the resource
request. It owns target physical inventory and source safe ReadIndex under one
original deadline. Stable RF3, current boot/process/engine, exact history and
birth/schema, latest settled permit, member high water and staged voter failure
domains must agree. One CAS installs the target debit, bounded active book and
exact membership permit. The opaque result still requires actual group
consumption and learner initialization. Generic writes cannot mint a linked
permit or delete/replace one while its resource debit remains active. Exact
canonical journal reconciliation precedes current probes, including after target
shutdown and adapter recreation.

Four native cases cover S3 seed through joint completion and a following write,
lost CAS confirmation and exact retry, minority refusal, and two independent
writers racing for the last target slot. The loser installs neither debit nor
permit; another request refuses at the engine-slot capacity boundary. Each
writer owns its snapshot. Hosts use an independent control adapter and install
the winning cut before the next capacity decision. Fixture ownership,
backpressure and stale-cut failures are retained; no production admission check
was relaxed to make the cases pass.

## FO-04 successful movement completion

The all-reachable `complete_movement` path owns fresh terminal target quorum and
physical inventory of the target and every original voter. Admission pins all possible seed
providers, including providers different from the selected quorum source.
Canonical compact fingerprints bind every boot field within the existing 4-KiB
catalog value bound. Completion checks locally durable completed permit sequence,
stable RF3, unchanged boots/current trust, and physical seed/initializer exit.
Retired voters use their durable local cache; local observers use bounded host
control without a manufactured loopback transport route.

One CAS records the permanent completion, retains a separate monotonic physical
high-water floor and removes the exact reservation from the active book. The
original resource row, latest consumed permit and request journals remain.
Subsequent admission charges physical occupancy plus every unresolved movement.
Exact committed retries precede live probes, including after target shutdown.
Generic mutations cannot forge or delete the completion or retained floor.

Six new native cases pass for successful release with retained occupancy,
lost confirmation after peer seeding from a different voter, unstarted work,
unreachable selected and other seed providers, and an actual S3 metadata worker
that survives learner waiter loss and its deadline. The four admission cases
also pass on the same frozen source. The full regression cohort is recorded
above; these cases do not complete planner qualification.

## FO-04 explicit topology verifier

Final topology source `3af38b41a574b06e6df5f0f831cc7bdc959ca796` separates
current replica-receiving eligibility from final membership in the keep-set.
It passes **66 public core cases**, including **15 topology cases**, core
all-target Clippy with warnings denied, the downstream host-library build,
formatting, layout, dependency and documentation checks. `topology-v2/` retains
its frozen 1,256 Rust files and 21 configuration inputs, logs, immutable core
executables and audit outside runtime data. The native movement and host
all-target Clippy checks remain attributed to the parent below; they were not
repeated for this pure input-field addition. No broader host rerun is claimed.

Source `82221ec049e0da835363e14ab143d2585cc0212c` passes all seven ordinary
public core suites, **65 cases**, including 14 topology cases and the 400-pair
matrix. All ten native movement cases, core and host all-target Clippy with
warnings denied, formatting, layout, dependency and docs checks also pass.
Generation `topology-v1/` retains frozen inputs, logs and immutable executables
outside runtime data. This is additional targeted verification; the broader
host cohort remains pinned to its merged-main source above.

The pure `raft_fleet::preview_topology` API accepts a complete proposed RF3
assignment and final keep-set, then searches for a legal serial sequence of
direct replacements. It checks every intermediate stable triple and target seed
peak. Completion adds retained target occupancy; no source slot, byte or memory
is credited. The final keep-set and current replica-receiving eligibility are separate inputs;
a kept maintenance node cannot receive new copies. Exact boot/process/profile
scopes and the rooted input/policy
revisions remain in the witness. Serial dependencies require actual terminal
movement completion before the next real permit.

Each kept node has a separate loss scenario. All groups retain their two healthy
voters while bounded search assigns fresh eligible recovery bindings. The entire
restoration assignment and its staged path must fit before the verifier returns.
One work limit covers every search. Exhaustion returns no partial preview;
`RecoveryUnproven` means no direct witness was found, not global infeasibility
across arbitrary survivor reshuffles. Inputs must be stable and complete, with
fresh node ledgers and no unsettled reservations. Adapter-owned authentication
and current health remain required; this pure result grants no authority.

The verification fixtures cover the symmetric 500-cell five-to-six assignment
(250 replacements), five-to-four evacuation (300 replacements), retained source
slots, every final-node loss, staging deadlocks, alternate move orders, recovery
backtracking across disk/memory limits, retired bindings, exclusion, stale and
incomplete inputs, exhausted search and arithmetic overflow. A small exhaustive
matrix covers 400 old/new voter-triple pairs. These are executable mathematical
witnesses; they are not live movement, TPS or resource-envelope qualification.

## Next feature priorities

1. Collect a complete authenticated fleet view at pinned root, history, policy,
   boot and profile cuts. Include physical floors, retained bindings, unsettled
   permits and genesis debt. Feed the generated planner with actual observations;
   caller-supplied counts or metadata alone cannot establish a complete view.
2. Persist canonical topology intent and connect the generated assignment to
   guarded staged admission and the existing owned executor. Exclude the full
   departing keep-set, reconcile ambiguous outcomes, require terminal dependencies
   and replan after relevant inventory/capacity changes. Deliver a live add/drain
   vertical workflow before adding optional workload policy.
3. Recover changed/unavailable boots with verified physical closure or trusted
   platform evidence and exact durable ownership continuity. A new process,
   current catalog boot or synced lifecycle guard alone is not joined-owner
   proof. Deliver resumable restart/rejoin, then resize and rolling maintenance.
4. Expose authenticated operator add/drain/remove, status/cancellation and
   scaling commands over the verified backend transitions.
5. Complete deployed RustFS/Colima correctness and resource qualification, resolve
   remaining real-node CI failures, then measure foreground TPS/p99 and controller
   overhead with fault and soak runs. Performance changes require measured gains.

FO-04, FO-06 and FO-07 remain in progress. FO-05 and FO-08 through FO-10 retain
their load-policy, operator, capacity-scaling and deployment qualification scope.
Admitted moves now have an owned automatic executor; new fleet planning/admission
and the operator workflows remain unfinished. Optional workload policy must not
delay required recovery delivery. Fleet management remains default-off.

## Historical implementation checkpoints

The sections below preserve earlier source-specific results and then-current
status. Current package status and retained qualification are stated above.

## Implemented catalog foundation

Source `4832486df3eff29ab3b5ed61f572513e59b37828` adds the private
`crates/cellar/durability/raft/fleet_catalog/` component and an unstable verification
export. Production host/controller integration follows in the remaining packages.

- All root changes use the existing no-hidden-retry `Bucket::put_cas` adapter.
- A genesis pin prevents silently recreating an initialized fleet after root loss.
- Five immutable indexes hold policy, nodes, histories, operation projections and
  admission projections. A separate permanent mutation journal records exact
  request digests and original applied revisions.
- Lost responses can be reconciled by a fresh adapter after later revisions.
  Repeated mutation identity with different payload is refused.
- Hash-radix pages use at most 64 KiB, 128 leaf entries and a bounded depth of 16.
  Snapshot-bound cursors avoid bucket listing and preserve historical reads.
- Root, request, record, page, key and batch sizes are bounded. One admitted
  catalog turn per shared adapter supplies immediate backpressure and bounded
  index-path memory. Attempts use the execution-domain deadline and refuse a
  closed domain.

The adapter stores atomic metadata projections; it does not decide whether an
operator state transition is legal or authorize a membership change. Typed
membership/lifecycle decisions and proof checks belong to FO-02 through FO-09.
The raw projection tests establish CAS atomicity and referencing behavior, not
an end-user evacuation certificate. No business write-acknowledgement path calls
this component yet.

## Completed verification

On macOS arm64 with Rust 1.98.1 and a checkout-specific target:

| Check | Result |
| --- | --- |
| `native_raft_fleet_catalog` | 11 passed, including competing writers, lost replies, stale request identity, orphan metadata, invalid records, pagination, cancellation, backpressure and closed-domain refusal |
| `native_raft_directory` | 16 existing native cases passed, including node identity, process refresh, inspection, cached quorum routes and joined shutdown |
| Focused Clippy | Passed with `-D warnings`; existing third-party Raft warnings remain outside the changed code |
| Rust formatting and module layout | Passed |
| Native dependency check | 454 reachable packages, no JavaScript engine |
| Documentation checks | Passed |
| Plan validator tests | 10 passed |

Logs, executable digest, source identity and the FO-01 completion record are
retained under ignored `.cellar/raft-perf/fleet-operations/implementation/fo01/`.
Initial fixture and test-harness failures are retained separately. No raw evidence
was added under `plans/multi-raft/evidence/`. A fresh checkout must restore retained
artifacts before auditing a DONE record; missing logs are not treated as a pass.

These results qualify local conditional in-memory storage and native directory
regressions. Physical RustFS semantics, five/six-node operation, performance,
failure/recovery histories and the 24-hour soak remain FO-10 gates.

## Implemented membership guard

Source `cc470bb268f81bbc22f1f52e78c496e62ccb64af` adds rooted opaque catalog
capabilities and commits each group's enrollment, consumed sequence and exact
permit with configuration apply. Unused cancellation commits a same-configuration
fence; started learners abort through the existing repair state machine, and
joint transitions still finish under both majorities. Snapshots, archives, engine
reopen and successor validation retain and scope-check the guard.

Managed SDK begin/advance/add-learner calls refuse missing authorization, including
idempotent shortcuts. Fresh managed learners require registered standby activation
with their capability. Legacy fresh peer/archive `Joining` startup refuses them
before claiming a member; intact retained joined engines can resume normally.
Large control payloads and optional fleet state are boxed, and physical snapshot
completion reservations cover the expanded bounded ledger. At that checkpoint
the internal protocol ended in `/fleet-1`; older records refuse startup and require fresh experimental
namespaces, with numeric format markers retained at 1.

Read the [component contract](../../../crates/cellar/durability/raft/fleet_catalog/README.md)
for the distinction between generic catalog projections, opaque authorizations,
committed group state and fresh terminal quorum proof. Local fleet status is not
a reservation-release certificate; typed controller reconciliation comes later.

| Guard verification | Result |
| --- | --- |
| `native_raft_fleet_permits` | 7 passed: delayed cancellation replay, three retained-engine restart boundaries, joint cancellation/minority refusal, peer/archive seed roundtrip, quorumless fencing and guarded SDK/legacy startup ingress |
| Core member-state tests | 15 passed, including 3 new scope/codec/successor cases |
| Protocol cutover test | 1 passed; earlier namespace protocols refused |
| Existing native regressions | 187 passed: archive 53, directory 16, catalog 11, membership 27, runtime 19, snapshot 44 and storage 17 |
| Regression process fixtures | 5 helper cases remain ignored in direct listings and are invoked by their passing parent cases |
| Focused Clippy, formatting, layout, native dependencies | Passed; 901 Rust files, 454 reachable packages and no JavaScript engine |
| Documentation and plan validator tests | Passed; 10 validator cases |

FO-02 also names the legacy startup regression explicitly, bringing the overall
acceptance inventory to 98. Only FO-01 and FO-02 are complete at their documented
local boundaries; the remaining fleet packages and qualification gates are open.

The completion record, source/executable digests and exact log hashes are retained
at ignored `.cellar/raft-perf/fleet-operations/implementation/fo02/completion.json`;
its logs use the adjacent `fo02-*.log` paths. Initial argument-type and empty-engine
Resume fixture failures remain retained. No assertion was weakened. Managed
recovery cases use controlled retained-engine reopen; existing unmanaged abrupt
process-kill regressions also passed. Abrupt managed fleet faults, deployed RustFS,
resource tiers, performance and soak remain unqualified FO-10 work.

## FO-03 progress: conditional birth cancellation

Source `c40606d804037829f3e810b8da397b35ed192f77` adds the low-level cancellation
fence at the immutable cell-name birth key. Birth and cancellation use the same
conditional create; a cancelled winner refuses delayed creation and name reuse.
A live winner remains live and must use its ordinary quorum-backed lifecycle.
The returned `BirthDisposition` exposes the exact observed winner. Lost replies
and original-deadline expiry remain ambiguous until exact-key reconciliation,
including expiry after a successful tombstone write. Caller membership vectors
are bounded before synchronous sorting or storage access.

The birth codec requires explicit nullable `cancelled_by`; the experimental
namespace protocol now ends in `/fleet-1/enrollment-1`. Numeric markers stay at 1,
and earlier namespaces have no compatibility reader. Use fresh experimental
namespaces and matching binaries. No raw evidence was added to the tracked plans.

Five birth cases pass, including held late writers, both race winners, lost
confirmation, deadlines before/after commit, malformed identity and input bounds.
The final regression cohort also passed 63 existing cases: directory 16, catalog
11, membership permits 7 and host 29. The protocol cutover unit, focused Clippy,
formatting/layout, native dependencies, documentation and 10 validator tests
passed. Logs are retained under ignored `implementation/fo03-*.log` within the
usual `.cellar/raft-perf/fleet-operations/` directory.

`implementation/fo03/birth-boundary.json` is explicitly a partial progress record,
not completion evidence. FO-03 remains `IN_PROGRESS`: nine required native cases
are still absent, and `check-list FO-03` correctly refuses the incomplete listing.
Trusted dynamic nodes, route revocation, empty-host readiness, dormant registration
and complete inventory/birth admission are not delivered by this substep. The
acceptance inventory contained 103 named cases at that checkpoint; its remaining
scope is intact.

## FO-03 progress: rooted empty hosts

Source `3df4364c3d878d76b8e12ccca92c0e63daff7254` adds typed bounded node registration and opaque
host authorization, explicit compiled namespace/class declarations, permanent
fleet engine-instance pins and node controls independent of a first cell. The
declaration digest includes the current protocol, schema, durability and class.
Startup rechecks rooted enrollment and mandatory namespace storage identity before
publishing the endpoint. Intact engines retain their pin; changing the catalog
prefix cannot authorize a replacement engine under the old node/disk identity.

An enrolled host can start with zero business groups. Local `observe_node` reports
actual engine occupancy, unattached/retained histories, declared capabilities and
the explicitly identified startup enrollment cut. Registration remains ineligible
for placement. The observation is not remote capacity readiness, a fresh quorum
or removal proof. Existing participating histories must already be enrolled, and
the legacy automatic controller is disabled on these hosts.

Six additional native cases cover local empty-host startup/control bounds/owned
handoff, stale authorization, registration retries after lost replies, capability
mismatch, intact empty-engine restart and replacement-engine refusal. They extend
the acceptance inventory to 109 without replacing the nine still-required dynamic
enrollment/readiness cases. The source inventory now has 59 reviewed anchors.
FO-03 remains `IN_PROGRESS` and FO-04 through FO-10 retain their complete scope.

The final native cohort passed 124 cases: enrollment 11 (five birth plus six host
cases), catalog 11, permits 7, directory 16, host 29, membership 27 and operations
23. The membership process helper remains ignored in direct listings and runs
through its passing process-kill parent. Another 34 library cases passed: startup
2 and transport 32. Focused Clippy with warnings denied, format/layout checks over
908 Rust files, native dependencies (454 packages, no JavaScript engine), plan
validation, all 10 validator tests and documentation checks passed.

The cohort exposed a pre-existing metrics test expecting the former 23-operation,
20-counter vocabulary, while the reviewed baseline already exported 26 and 34.
Source `06651034` restores exact 26/34 and 634-series assertions and explicitly
checks archive instruments. The one-versus-32-group cardinality/response-bound
test passes; no telemetry behavior or assertion strength was reduced. The initial
noncanonical temporary-directory fixture failure and stale-assertion cohort logs
remain retained alongside the successful logs.

`implementation/fo03/host-enrollment-boundary.json` is a partial record, not task
completion. `check-list FO-03` still refuses the listing missing nine required
cases. These local runs do not qualify remote readiness, full inventory, RustFS,
deployed resource limits, fleet scaling, performance or the soak.

## FO-03 progress: dynamic dormant histories

Source `473e40ed114a5408e6adda78265327e6a11b2511` implements dynamic dormant
registration at its local SDK boundary. A host
prepares one bounded metadata read, rechecks current node/history enrollment and
the immutable birth, then publishes through the serialized actor. The shared
registry returns owned metadata under short locks and refuses name replacement.
Physical occupancy includes unattached engine histories; an occupied slot cannot
be reset by registration. No member or SQL image is created by this operation.

The six dynamic cases cover limits/idempotency/conflicting histories, cancelled
reads, close/join ownership, stale rooted history, undeclared classes and occupied
engine slots. The enrollment target now has 17 passing cases. The broader cohort
passed 166 native cases, including effects and query routing; the final focused
run adds the close case and reruns all 17 enrollment cases after preserving
temporary storage failure as unavailable rather than a permanent binding conflict.
All 126 Raft library units also passed. The total acceptance inventory is now
114, with eight required FO-03 cases still absent; this does not complete FO-03.

Focused Clippy (including query/effect/controller targets), formatting/layout over
910 Rust files, native dependency checks (454 packages, no JavaScript engine),
documentation checks and the 10 plan validator tests passed. The partial record
is retained at `implementation/fo03/dynamic-history-boundary.json`, alongside the
exact log hashes and the still-refused complete FO-03 test listing. It separates
the broader registry regression run from the final enrollment-only storage-error
classification rerun. No deployed RustFS, capacity-ready, TPS/p99 or soak claim is
made by this local SDK verification.

## FO-03 progress: rooted transport trust

Source `a155d22a7bbee57de3eccde135d8691e812068a6` installs a complete bounded
node-table cut before managed process discovery. The directory worker refreshes
the current root, reuses the validated immutable node cut when that exact root
is unchanged, and discovers enrolled nodes through exact namespace directory
keys without configured addresses. The fleet and namespace engine pins must
agree. Bucket objects and traffic cannot expand this trusted set.

Trust installation rejects older cuts, missing retained identities, changed pins
and removed-identity revival. Final `Removed` status cancels peer pools,
query/inspection grants and physical seed epochs. Restart, resize, draining and
evacuated states keep transport available while membership work resolves;
placement eligibility remains separate. This transport adapter does not prove
that publishing `Removed` is legal; the later lifecycle/removal workflow must
establish that proof first.

The one owned discovery batch holds a 4 MiB control reservation through metadata
and endpoint completion. Its read-only catalog adapter has a separate bounded
turn. A held background root read therefore cannot take the foreground history
admission turn. Managed hosts require that metadata capacity plus a 4 KiB request
and two administrative items. Local `fleet_trust_revision` acknowledges the last
installed complete cut, while the original startup enrollment remains distinct.
Neither field is authenticated remote readiness or a removal certificate.

| Verification | Result |
| --- | --- |
| Nine-target native cohort | 169 passed: directory 16, effects 16, catalog 11, enrollment 19, permits 7, host 29, membership 27, operations 23 and reads 21 |
| New enrollment boundaries | A live empty host observes a newer root without creating a group; a held discovery read permits foreground registration, then joins and releases its budget on close |
| Raft library tests | 128 passed, including new unknown-node/pin and revocation/grant/seed-owner cases |
| Focused Clippy | Passed with warnings denied; existing vendor warnings remain |
| Formatting, layout and native dependencies | Passed: 912 Rust files and 454 reachable packages, no JavaScript engine |
| Documentation, source preflight and plan validation | Passed; 116 required cases, 66 source anchors and all 10 validator tests |

The initial shared-turn cohort exposed two enrollment failures. Its failed log
is retained as `implementation/fo03-trust-native-shared-turn-failed.log` under the
usual ignored artifact directory. The final complete native and Raft unit runs
follow the isolated-reader fix; assertions were not weakened. The membership
process helper remains ignored in direct listings and is invoked by its passing
parent. Logs and executable hashes are recorded in the partial
`implementation/fo03/trust-boundary.json` record.

Eight required FO-03 cases are still absent, and its complete listing still
refuses qualification. Authenticated remote node observations and the required
trusted-revision acknowledgements by move participants remain to implement.
Complete import/birth admission, full retired-engine lifecycle, physical-host
policy and eligibility inspection are also pending. These local results make no
RustFS deployment, fleet-operation, TPS/p99 or soak claim.

## FO-03 progress: authenticated node observations

Source `164359427707f5bc80a64dd8d981debef7cd5d5e` includes the node RPC from
`c84540a701cf1474b7ae809d4fc3ec100c9bfc35`, the merge of `origin/main` at
`47f4a8924a54b25b579f17ad2fc5a46eb97a6ba6`, and conservative age through SDK
delivery. `RaftCells::inspect_peer_node` needs no cell scope or member. Both
physical processes must be rooted and discovered, and both installed trust cuts
must reach the requested minimum revision. Metadata grants no namespace query,
group membership, capacity activation or removal authority.

The response distinguishes startup enrollment from the installed current state
and epoch. Real TLS cases verify two empty hosts, reciprocal discovery, current
restart/resize/drain classification, unknown targets and insufficient revisions.
Reply age includes administrative queue residence and retention by an unpolled
caller; an old reply cannot appear fresh merely because its consumer waited.

Bodies are bounded at 16 KiB and share the existing independent metadata slots
and peer banks. Host response bytes remain charged through delivery. Managed
control capacity now covers one 4 MiB discovery read, a 64 KiB response and a
4 KiB request across three items. The exact-floor case serves cached metadata
with both catalog readers held; undersized startup refuses before engine pinning.
A blackholed TLS case verifies joined shutdown and the retained caller permit.

The protocol hard-cuts to `/fleet-1/enrollment-1/node-probes-1`, retaining numeric
markers at 1. The older enrollment protocol is explicitly refused. Use fresh
experimental namespaces and matching participants; no compatibility reader was
added. The main merge keeps upstream's complete archive-metric vocabulary checks
and snapshot failure diagnostics.

| Merged-tree verification | Result |
| --- | --- |
| Native cohort across 13 targets | 307 passed, including 24 enrollment cases, archive 53, snapshot 44, membership 27 and storage 17 |
| Raft library tests | 132 passed, including strict node-body and channel/purpose/request authentication cases |
| Upstream startup confirmation | 4 passed |
| Clippy, formatting and layout | Passed with warnings denied for affected targets; 921 Rust files |
| Native dependency, documentation and plan checks | Passed; no JavaScript engine, 119 required cases, 67 source anchors and 10 validator tests |

The post-merge cohort ran in two stages: archive/directory/effects/catalog passed
96 cases before the retained-age fixture constructed its sleep outside the
domain. After fixing only that test's domain entry, enrollment and the remaining
targets passed another 211. Production source did not change between these runs.
The 13-target Clippy run passed; focused lib/enrollment Clippy reran after that
fixture correction. The 132-unit run predates the fixture-only correction.

Earlier complete pre-merge native qualification (306 cases), the initial fixture
compile error, the Clippy fixture cleanup and the post-merge failed cohort remain
retained separately. The partial `implementation/fo03/node-observation-boundary.json`
record stores exact log and executable hashes and these scope distinctions.
Raw artifacts remain ignored, outside `plans/multi-raft/evidence/`.

The low-level node probe supplies required-revision observations. The full move
executor still must wait for its participants and validate current root policy;
this implementation does not complete that executor or the final quorum proof.
Complete inventory/import, birth/drain admission, retired binding lifecycle,
unknown-node ingress and strict physical-host policy remain FO-03 delivery work.
The complete FO-03 listing still refuses six missing cases; the task remains
`IN_PROGRESS`. RustFS deployment, full fleet workflows, TPS/p99, enforced Colima
resource tiers and the endurance soak remain unqualified.

## FO-03 progress: rooted managed birth admission

Fresh managed namespace roots now bind birth admission to the exact fleet
incarnation and scoped catalog key. Ordinary birth/cancellation cannot create
histories in those namespaces. A reservation CAS installs the complete initial
history descriptor and permanent name index before returning a birth capability.
The root maintains a bounded history count, without scanning every history for
each admission. Initial RF3 admission requires active, engine-pinned node records
on three distinct declared physical hosts.

The reservation and node-state update contend on the same root. An admitted
writer delayed past drain remains inventoried; a writer whose reservation CAS
loses cannot create a birth. Exact immutable-birth reconciliation changes the
history to Live or Cancelled. Timeouts and lost replies retain the reference,
and fresh adapters can reconcile the original admission after later root changes.
Pending/cancelled histories cannot mint membership authority. Name tombstones
remain permanent. These adapter tests do not certify a full node evacuation.

History descriptors now retain schema, namespace-root and live-birth digests,
initial node/member references and admission identity. Host startup, dormant
registration and group enrollment compare the actual birth and schema. Ordinary
bootstrap refuses fresh/bare managed genesis; it only resumes an already
enrolled managed group. Guarded initial-member initialization remains a required
integration step. Registering an observed import does not quiesce legacy creators,
convert its namespace or certify inventory completeness.

The protocol hard-cuts to `/birth-admission-1`; numeric markers remain 1 and there
is no compatibility reader for earlier namespace/history records. Qualification
and exact source/artifact identity are retained in the partial
`implementation/fo03/managed-birth-boundary.json` record under ignored
`.cellar/raft-perf/fleet-operations/`. This substep does not complete FO-03.

Source `dec09d328b0c8be6881a3d07f2effdd76e781ce6` is qualified locally on macOS arm64/Rust 1.98.1:

| Managed-birth verification | Result |
| --- | --- |
| Native regressions across 13 targets | 317 passed, including 34 enrollment cases |
| Raft library and startup confirmation | 132 and 4 passed |
| Affected-target Clippy | Passed with warnings denied |
| Formatting, module layout and native dependencies | Passed; 923 Rust files, 454 reachable packages, no JavaScript engine |
| Plan and documentation checks | Passed; 129 required cases, 67 reviewed source anchors and 10 validator tests |

The final native cohort includes corruption classification and ordinary managed
genesis refusal. The earlier 315-case cohort and 48-case focused cohort are retained
with their narrower source scope; the initial duplicate-helper compile failure is
retained separately. No assertions were weakened. Final source bytes were not
changed after qualification. The partial record retains log and executable hashes;
mutable target-cache paths do not imply that binary bytes were archived.

Six mandatory FO-03 cases remain absent. The new reservation race and declared
domain cases cover their adapter boundaries; they do not replace the full
lifecycle, import, participant-acknowledgement or movement-policy cases.
Guarded member initialization, complete import, group-identity conflict
reconciliation and actual lifecycle execution remain open. Initial admission
currently requires distinct declared physical hosts. Before same-host Colima
qualification, implement an explicit rooted domain policy; never relabel colocated
containers as independent physical failure domains. RustFS, resource enforcement,
TPS/p99 and soak remain unqualified. Keep fleet management default-off.

## FO-03 progress: guarded initial voters

Source `db751b13429db2750892510ae4c43410b796a256` connects confirmed births in fresh managed namespaces to
actual initial voters through both startup Replica registrations and
`activate_managed_initial_member` on running empty hosts. Genesis and fleet
enrollment share one engine sync before cache publication, permanent member-claim
confirmation and runtime attachment. The explicit Genesis origin starts at index
1; Committed enrollment still requires a later configuration entry. Missing
origin fields, bare-slot retrofit, rebound guards and origin changes are refused.

Dynamic initialization reuses the owned learner SQL/attachment pipeline, the
original deadline, a 4 MiB metadata reservation and one initializer per host.
Dropping a submitted caller can leave a successfully initialized member; same-capability
retry observes its actual state. Shutdown cancels held claims and joins ownership.
A matching declaration digest cannot bypass the namespace's coordination-key
owner. Initializer inspection now counts a slot once during transfer to the
synced engine catalog, under one catalog lock.

Cold managed startup resolves exact rooted process views before membership-route
construction, so a self-address hint is sufficient when required views exist.
Retained histories still require their startup recovery registrations; this is
address discovery, not automatic enrollment of unlisted or retired engine slots.
The protocol hard-cuts to `/managed-genesis-1`, with numeric markers at 1 and no
legacy reader for the new managed enrollment shape.

| Guarded-genesis verification | Result |
| --- | --- |
| Final native cohort across 13 targets | 323 passed, including all 40 enrollment cases |
| Raft library and startup confirmation | 136 and 4 passed |
| Clippy | Affected targets plus library passed with warnings denied |
| Formatting, layout and native dependencies | Passed; 926 Rust files, 454 reachable packages, no JavaScript engine |
| Plan and documentation checks | Passed; 135 required cases, 67 source anchors and 10 validator tests |

The six new native cases use actual empty SDK hosts, native SQLite/TLS, quorum
and archive acknowledgements, linearizable reads, data/guard recovery after
restart, held-claim caller cancellation, shutdown, stale history and foreign
coordination scope. Two new pure membership cases check origin/fence semantics;
two actual storage cases check sync visibility, reopen and non-retrofit.
Node activation and distinct physical-host labels are trusted fixture inputs;
these tests do not qualify physical capacity activation or independent machines.

Initial compilation failures remain retained. The first runtime cohort exposed
a real double count of an initializing slot after sync; the production counter
was fixed without weakening its count assertion. The next cohort exposed a test
waiting for SQL insertion rather than final member publication; that wait now
uses the actual installed membership. Both failed cohorts remain retained beside
the final successful run. Final source bytes were unchanged after qualification.

The partial `implementation/fo03/managed-genesis-boundary.json` record under
ignored `.cellar/raft-perf/fleet-operations/` pins source, log and executable hashes.
Cache paths are mutable and binary bytes were not archived. No raw artifacts were
added to `plans/multi-raft/evidence/`. FO-03 remains IN_PROGRESS with six mandatory
cases absent; FO-04 through FO-10 remain pending.

The next birth/drain integration must explicitly resume an already admitted birth
across exclusion: dynamic metadata registration currently refuses Draining nodes,
including this pending-birth case. Complete import cuts, retired-binding lifecycle,
unknown ingress, actual participant acknowledgements and an explicit rooted domain
policy remain open. Keep fleet management default-off. RustFS deployment, Colima
limits, throughput/latency and the soak remain unqualified.

## Next implementation boundary

FO-03 adds the trusted dynamic node set, empty managed host readiness, bounded
dormant registration and complete managed history/birth enrollment. It must keep
storage pins and retired bindings immutable and separate restart/resize exclusions
from evacuation. FO-04 through FO-10 retain their complete planning, telemetry,
reconciliation, lifecycle, API/CLI, scaling and qualification scope.

`node/raft_startup.rs`, `host.rs` and `host_directory.rs` now accept zero groups
only with explicit rooted enrollment and compiled namespace/class declarations;
node controls no longer borrow a first-cell binding. Remote node observations now
bind both physical processes and a minimum installed trust revision, with current
enrollment separate from startup state. The remaining work includes complete
import/birth admission, physical-host placement policy, unknown-node ingress and
binding acknowledgements to the actual movement executor. Current-root trust
refresh, transport revocation and metadata RPCs do not complete that movement
protocol. Dynamic binding mutations now belong to the
serialized host, with bounded reader access and no lock guard crossing an await.
Retained active histories still reopen through startup recovery registrations;
the new metadata path refuses resetting their physical slots. A local observation,
dummy business cell or successful TCP connect cannot substitute for the remaining
enrollment/readiness proof.

## FO-03 progress: admitted birth settlement

Source `deb67f903e230579593205dbedc47a6547e4a91e` fixes initialization retry through startup-recovered handles.
The host validates the actual birth and declared namespace, then observes the
retained original-voter guard without recreating the engine or SQLite image.
The restart case retries the same capability; another archived write and read
verify the accumulated data. An in-flight initializer still cannot return
an attachment receipt early.

A confirmed fresh managed birth can now register its original voters after
rooted drain, restart or resize exclusion. The actual SDK race case holds the
birth writer, excludes all three nodes, confirms the retained inventory and then
initializes the voters and acknowledges a quorum/archive write. An excluded
noninitial spare cannot use the exception. In the opposite root-CAS ordering,
both the delayed and fresh reservation refuse; no birth, dormant binding or
local voter appears. Trusted state changes remain fixture inputs; these cases
do not implement or certify evacuation, capacity activation or process stop.

The final affected native cohort passed **154 cases across seven targets**,
including all **41 enrollment cases**. All **136 Raft unit tests**, affected
Clippy with warnings denied, formatting/layout over 926 Rust files, native
dependency checks (454 packages, no JavaScript engine), plan validation,
10 validator tests and documentation checks passed. The two reproduced failures
remain retained. No Rust bytes changed after qualification.

The ignored partial record is `implementation/fo03/birth-continuation-boundary.json`
under `.cellar/raft-perf/fleet-operations/`; adjacent logs use
`fo03-birth-continuation-*.log`. It records exact source, log and observed executable
hashes; mutable cache paths do not archive the binaries. No raw evidence is
tracked. There is no new TPS/p99 or deployed RustFS/Colima qualification.

FO-03 remains `IN_PROGRESS`: five mandatory cases are still absent
(`fleet_enrollment_unknown_node`, `fleet_enrollment_route_revocation`,
`fleet_enrollment_retired_binding`, `fleet_enrollment_incomplete_import` and
`fleet_enrollment_physical_host`). `check-list FO-03` correctly refuses the
incomplete listing. The acceptance inventory stays at 135 cases and 67 source
anchors; FO-04 through FO-10 remain pending.

## FO-03 progress: group identity admission

Source `c4d2c0ec7b409a136aa4dd663464f20a7e1fc9ba` confirms the namespace's permanent group-ID claim before
fleet admission, then indexes the history, cell name and fleet-wide group ID in
one root CAS. A conflicting claim in another namespace refuses before a history
is admitted. A pre-admission claim left by cancellation or a losing root turn
grants no birth or membership authority; retain it and reconcile the original
request. The immutable namespace claim also prevents a competing catalog from
stealing the ID between confirmation and root admission.

The experimental protocol now ends in `/group-reservations-1`. Fleet heads
require its digest and the permanent genesis pin binds it. Earlier or missing
protocol state refuses initialization and reads. Use fresh fleet roots, fresh
namespaces and matching binaries; numeric markers remain 1. No legacy reader or
state migration was added.

Four new native cases cover aliases across names/incarnations/namespaces/data
prefixes, an existing unmanaged claim, cancellation before root CAS and lost
claim confirmation with exact retry after later revisions and adapter restart.
The catalog's malformed-record case also refuses missing/foreign head protocols.
The duplicate admission was reproduced before its fix; no assertion was weakened.

The final cohort passed **328 native cases across 13 targets**, including all
**45 enrollment cases**, plus **136 Raft unit tests and four startup tests**.
Affected Clippy with warnings denied, format/layout over 926 Rust files,
native dependencies (454 packages, no JavaScript engine), plan validation,
10 validator tests and documentation checks passed. No Rust bytes changed after
qualification. The initial manual native total omitted the preceding birth/drain
case; the retained correction records all 13 summaries totaling 328, zero failed.

The ignored partial record `implementation/fo03/group-reservations-boundary.json`
under `.cellar/raft-perf/fleet-operations/` stores exact source/log/executable
hashes. Adjacent logs use `fo03-group-reservations-*.log`; executable cache paths
do not archive binary bytes. No raw evidence is tracked. These runs do not
qualify RustFS, Colima resource enforcement, fleet automation, TPS/p99 or soak.

The acceptance inventory now contains 139 cases and 67 source anchors. FO-03
remains `IN_PROGRESS` with the same five mandatory cases absent: unknown-node
ingress, move-participant revision acknowledgements, actual retired-binding
lifecycle, complete imports and rooted physical-host policy. The complete FO-03
listing still correctly refuses qualification. FO-04 through FO-10 remain pending.

## FO-03 progress: authoritative namespace births and freeze

Source `2bb08a0f783e051cb6ad1510bbeb2979030db5ad` indexes canonical birth intents before publication in every
new-protocol Raft namespace. Birth decoding requires its indexed intent and
permanent identity claim; validly framed bucket objects alone cannot mint a
`GroupBirth`. Namespace and fleet adapters reuse bounded immutable radix pages
while retaining their own storage, errors and CAS roots. Logical record bounds
remain separate from the fixed 64-KiB index-page limit; paging returns at most
sixteen intents and no path uses bucket listing for authority.

`freeze_births` orders against admission through the namespace root CAS. If
admission won, pending/live/cancelled names remain enumerable from that exact
frozen root and admitted writers may settle. If freeze won, a delayed admission
cannot publish a new birth. Exact pending proposals can be conditionally
cancelled to fence late writers. Lost replies retain the same operation;
missing heads, pins, pages or intents refuse authority and cannot silently
recreate an empty inventory. The genesis metadata is prepared before namespace
publication. The protocol now ends in `/birth-inventory-1`; use fresh experimental
fleet/namespace roots and matching binaries, with markers remaining 1.

Seven new native cases cover both race orderings, delayed cancellation, lost
freeze confirmation/reopen, missing metadata and occupied-root refusal,
140-entry bounded scans under 1-KiB logical record limits, foreign cursors and
untracked forged decisions. The existing three-node/two-cell SDK case now
freezes new births before all its writes, queries and leader failover, which
continue successfully. This qualifies birth quiescence, not membership quiescence.

The final run passed **335 native cases across 13 targets**, including all
**52 enrollment cases**, **136 Raft unit tests and four startup tests**. Clippy
with warnings denied, formatting/layout over 930 Rust files, native dependency
checks (454 packages, no JavaScript engine), plan validation, all 10 validator
tests and documentation checks passed on the final source. No Rust bytes changed
after that qualification. Three existing native fixtures became reviewed source
anchors, bringing the inventory to **146 required cases and 70 anchors**.

Failed compilation, format/lint and native cohorts remain retained. The native
failures identified archive fixtures selecting the new inventory head by its
filename and a directory fixture counting namespace pins as node-storage pins.
The selectors now identify their owning key spaces and assert the separate pin
counts; all corruption, immutability and no-voting-authority assertions remain.
The initial missing-anchor validation was fixed by recording the reviewed input;
the validator was not weakened. The partial record is ignored
`implementation/fo03/import-inventory-boundary.json` under
`.cellar/raft-perf/fleet-operations/`, with adjacent `fo03-import-inventory-*.log`
files and exact source/log/executable hashes. Cache paths do not archive binaries.
No raw evidence is tracked and no new TPS/p99 or deployed capacity claim is made.

**Complete import is not delivered.** Next, integrate membership fencing, exact
fleet history/namespace coverage, ownership handoff and controlled cancellation;
a frozen birth cut grants no removal certificate. FO-03 remains `IN_PROGRESS`
with the same five mandatory cases missing: unknown ingress, movement revision
acknowledgements, retired bindings, complete import and rooted domain policy.
Its full listing correctly refuses qualification. FO-04 through FO-10 remain
pending and automatic fleet management stays default-off.

## FO-03 progress: quorum membership observation

Source `3fcffcf38bc7dc0899445bd35bb6fd5960d9f0bf` adds
`RaftCellHandle::observe_fleet_membership`. Each invocation uses a new safe
ReadIndex and waits for coherent durable apply and the local route revision. The
opaque result binds the actual birth/schema and physical node/disk/process,
retains both joint voter sets and learner roles, and exposes the committed fleet
sequence, joined bindings and repair phase. It needs no SQL image or application
query. An additional bounded 64-KiB/item administrative reservation survives
through SDK reply delivery; cancellation, deadlines and shutdown release it.
Business write acknowledgement adds no catalog or observation call.

All **340 native cases across 13 targets**, **136 Raft library units** and **four
startup-confirmation cases** passed on the existing local macOS arm64 setup.
The permit target now has 12 passing cases, including five new cases covering
empty images, missing enrollment and foreign births, cached-state refusal during
partition and successor recovery, seeding/joint/terminal roles, and bounded
cancellation/pressure/shutdown. Its existing joint-recovery case also refuses new
observations with only the new majority, including after physical reopen, then
observes terminal membership once both majorities recover. Five process helpers
remain ignored in direct listings and run through their passing parent cases.

Clippy with warnings denied, formatting/layout over 932 Rust files, dependency
checks (454 reachable packages and no JavaScript engine), documentation, plan
validation and all 10 validator tests passed. The inventory now has 151 required
cases and 71 reviewed source anchors. Initial test logs retain the incorrect
fixture process generation and skipped command sequence; both were corrected
without weakened assertions. Final qualified Rust hashes are unchanged.

`implementation/fo03/quorum-observation-boundary.json` is a retained **partial**
record under ignored `.cellar/raft-perf/fleet-operations/`. It records exact source,
log and executable hashes; mutable build paths do not archive binary bytes.
No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains `IN_PROGRESS`: 52 enrollment cases and five companion observation
cases pass, while its five mandatory complete-import/trust/lifecycle cases are
still missing. The combined listing correctly fails `check-list FO-03`.
A saved group observation supplies no lease, complete fleet inventory, archive
coverage or removal certificate. At that checkpoint, complete-import traversal,
remote/controller integration, ownership handoff/cancellation and typed lifecycle
gating remained required. The namespace-import substep below supplies the
per-namespace handoff. FO-04 through FO-10 stay pending; fleet management remains
default-off.
These results do not qualify RustFS deployment, enforced Colima limits, fleet
scaling, TPS/p99 or the soak, and claim no performance improvement.

## FO-03 progress: rooted namespace import

Source `900a677fb6481a259d1e24073d9dc1aacf785507` implements the per-namespace import
runtime over an exact frozen birth cut. Rooted Preparing, Inventory, Coverage, Handoff and Complete
phases settle every canonical intent, retain cancelled names and require each
live group's opaque safe-ReadIndex observation. Coverage verifies the actual
namespace storage pin, permanent member/disk claims and catalog-scoped committed
guard. Handoff rereads the complete retained witness set; missing metadata refuses
adoption. Bounded turns use no object listing or business SQL image.

Stop/resume retains the same cut, claims and physical references. A late namespace
claim cannot advance a stopped root. Original stored candidates reconcile lost
replies across adapter recreation, including a namespace adoption whose terminal
fleet-root reply is lost. Once Handoff is admitted, cancellation refuses. Only
Complete plus the exact adoption marker admits later managed births.

New births carry their managed owner in the canonical birth record. Three actual
managed hosts can import an empty immutable-unmanaged namespace, admit a later
birth, initialize its guard in the first engine sync, serve native SQL writes and
recover intact after physical restart. Old imported births retain their original
identity and replay their committed enrollment. Catalog-scoped group bindings
prevent another catalog prefix with the same fleet ID from reusing cached guards
or membership permits.

The experimental protocol now ends in `/namespace-import-1`. New required birth
owner, history-origin and catalog-binding fields hard-cut earlier records; use
fresh experimental coordination/data prefixes, namespaces and matching binaries.
`FleetGroupBinding::new` requires the catalog fingerprint; use
`FleetCatalog::group_binding` to obtain the descriptor. Descriptors grant no
authority. Numeric markers stay at 1, without a migration or compatibility reader.

All **349 native cases across 13 targets**, **136 Raft library units** and
**four startup-confirmation cases** passed on macOS arm64/Rust 1.98.1. Enrollment
has 53 passing cases and permits/import has 20, including nine new import/genesis
cases. Five direct-list process helpers run through passing parent cases. Clippy
with warnings denied, format/layout over 938 Rust files, native dependency checks
(454 reachable packages, no JavaScript engine), documentation, plan validation
and all 10 validator tests passed. The manifest retains 160 required cases and
71 reviewed source anchors; this substep advances no task status.

Initial import tests exposed a full-page continuation being mistaken for proof
of another entry and an oversized composed management future overflowing the
ordinary test stack. Two-entry lookahead retains exact count checks; boxing work
at domain adapters keeps ordinary stacks sufficient. A larger-stack diagnostic
is retained separately and is not qualification. No stack override or debug
instrumentation remains in source. Initial Clippy rejected an unnecessary unit
expression; its equivalent arm was corrected before the final snapshot.

An initial full native cohort had two readiness failures in existing enrollment
cases. Both passed in isolation and in the full rerun without source or timeout
changes. Their root cause remains unestablished; retain the failed cohort and
track readiness timing stability during broader qualification. Assertions were
not weakened and no qualified Rust bytes changed after the final snapshot.

The **partial** record `implementation/fo03/namespace-import-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains source/log/executable hashes,
initial failures and the full rerun. Mutable build-cache paths do not archive
binary bytes. No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. The combined enrollment/permit listing still
correctly refuses five missing mandatory cases: `fleet_enrollment_unknown_node`,
`fleet_enrollment_route_revocation`, `fleet_enrollment_retired_binding`,
`fleet_enrollment_incomplete_import` and `fleet_enrollment_physical_host`.
Per-namespace Complete is not fleet-wide compiled-namespace coverage, capacity
activation, current quorum health, archive coverage or a node-removal receipt.
Remote/controller integration and lifecycle gates remain required. FO-04 through
FO-10 stay pending; automatic fleet management remains default-off. These local
checks make no RustFS/Colima/resource-limit, TPS/p99 or soak qualification claim.

## FO-03 progress: fleet namespace registry and inventory

Source `d98ca0fe98f0755006b565e197548c6092280341` adds an append-only paged registry of exact namespace,
class, schema and data-root identities. Managed startup checks actual prepared
catalogs and compiled classes against it before pinning the engine or publishing
an endpoint. History enrollment, import and new managed-birth admission require
the same declaration. Distinct hosts can support different declared subsets;
capability digests do not bypass the registry.

The fleet head permanently retains the namespace count. Missing registry/index
state, deletion and count regression refuse admission rather than creating an
empty replacement set. At most 64 namespaces are declared. Original immutable
candidates reconcile lost replies after adapter recreation and later additions;
changed identities, classes or data roots cannot replace a declaration.

`check_inventory` reads every rooted declaration and verifies the exact supplied
catalog handles. Each imported namespace must have completed handoff and retain
its witness pages and permanent claims. Historical node tombstones remain valid
references; this is not a new health observation. The checker then streams the
current canonical birth indexes, including post-import births, and all fleet
histories in both directions. It checks exact counts, identity indexes, initial
node references, permanent group claims and immutable decisions. Reservations
before intent publication and pending confirmations retain their references.
Offline endpoints and cancelled births cannot disappear from inventory.

The experimental protocol now ends in `/fleet-namespaces-1`; the fleet head's
namespace count is mandatory. Use fresh experimental fleet/data prefixes,
namespaces and matching binaries, without a compatibility reader or migration.
Numeric markers remain 1. Existing business write acknowledgement adds no registry
or inventory call. See the [registry contract](../../../crates/cellar/durability/raft/fleet_catalog/namespaces/README.md).

**356 distinct native cases passed across 13 targets**: the first eleven complete
targets passed 295 cases; a complete snapshot/storage rerun passed another 61
(snapshot 44, storage 17). Enrollment has 55 passing cases and permits/import has
25. Five process helpers remain ignored in direct listings and run through their
passing parent cases. **136 Raft library units** and **four startup-confirmation
cases** also passed. Clippy with warnings denied, format/layout over 944 Rust
files, native dependency checks (454 packages, no JavaScript engine), documentation,
plan validation and all 10 validator tests passed. The manifest retains 167
required cases and 71 reviewed anchors; this substep advances no task status.

Seven new cases cover complete namespace coverage, declaration retries/scope,
exactly full canonical pages with offline/cancelled histories, the 64-namespace
bound, registry no-reset behavior, compiled-host refusal/intact resume and pending
birth confirmations. Existing post-import genesis/restart and 141-intent cases
also use the global checker. Missing retained coverage is tested again after
import completion. Fixture node ownership and cloned-reference lint errors were
corrected without weakened assertions or suppression. A pre-fence run was
intentionally interrupted after Clippy so review could add the permanent count
and no-reset test; that native run is not counted as qualification.

The initial full native cohort had one failure: `missing_quorum_restore_refused`
returned `Ambiguous` on its first setup write, before quorum removal. It passed
isolated unchanged in 2.72 seconds and then in the full 44-case snapshot rerun.
The timing root cause remains unestablished. The failed cohort is retained; the
successful target scopes are reported separately rather than relabeling the
initial cohort as passed. Earlier enrollment readiness instability is not claimed
resolved by these checks. No qualified Rust bytes changed after the final snapshot.

The **partial** record `implementation/fo03/fleet-registry-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source, log and executable
hashes, run provenance, failures and interrupted attempts. Mutable build-cache
paths do not archive binary bytes. No raw evidence is tracked under
`plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. This checked inventory is not a capacity, current
quorum, archive, empty-node or removal receipt. Typed capacity and maintenance
operations must still consume a revalidated exact root and establish their own
proofs and physical-work joins. The same five mandatory FO-03 cases remain absent;
FO-04 through FO-10 remain pending and automatic fleet management stays default-off.
No RustFS/Colima/resource-enforcement, TPS/p99 or soak claim is made.

## FO-03 progress: current learner admission and retired bindings

Source `45b4924344028f2716d3064165c56339c764b23c` replaces startup-cached learner eligibility with a fresh
bounded host/history authorization read. It checks the actual engine pin,
namespace and history binding before initializing a fresh member. Registered to
Active can receive a replica without restarting; startup Active cannot bypass a
current Draining exclusion. The metadata read uses the original seed deadline
and an independent 4 MiB control reservation, released before bulk seeding.
An existing synced member still needs its exact committed permit to resume after
exclusion. Business-write acknowledgement adds no catalog read.

`fleet_enrollment_retired_binding` exercises actual peer seeding, joint consensus
and native SQL. It physically stops an outgoing voter before final removal,
finishes with the surviving joint majorities, then reopens that stale intact
engine without a cell registration. Both metadata registration and physical
bootstrap refuse a fresh member identity, retaining the original slot and guard.
The failed learner is aborted and its ID stays consumed. A distinct enrolled
engine receives a newer member ID through archive seeding and serves verified
native writes and reads. Retirement/reopen and replacement-host stops verify
physical Raft joins. Privileged catalog fixture mutations stand in for the pending
reconciler; this is not an evacuation/removal certificate.

**357 native cases passed across 13 complete targets**, including 56 enrollment
and 25 permit/import cases. Five direct-list process helpers run through their
passing parent cases. **136 Raft library units** and **four startup-confirmation
cases** also passed. Clippy with warnings denied, format/layout over 945 Rust
files, native dependency checks (454 packages, no JavaScript engine), documentation,
source preflight, plan validation and all 10 validator tests passed. The manifest
retains 167 required cases and 71 reviewed source anchors; no package status
advances in this substep.

The new test consistently reproduced learner admission refusal before seeding.
Targeted diagnostics showed startup Registered alongside current Active; refreshing
rooted authorization corrects that stale cache and tests the inverse exclusion.
Reopened-host discovery also exposed a temporary old-endpoint Unreachable result;
the existing bounded fixture retry now handles it while still requiring a valid
response at the requested trust revision. A now-redundant catch-all arm was removed
before qualification. Initial integration import/context errors and diagnostic
failures remain retained. No assertion, timeout or lint was weakened; no stack
override or debug instrumentation remains. Earlier readiness/setup timing failures
remain unresolved and are not claimed fixed by this qualification.

The **partial** record `implementation/fo03/retired-binding-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source, log and executable
hashes. Mutable build-cache paths do not archive executable bytes. No raw evidence
is tracked under `plans/multi-raft/evidence/`. This substep changes no wire format.

FO-03 remains **IN_PROGRESS**. The combined enrollment/permit listing still
correctly refuses four missing mandatory cases: `fleet_enrollment_unknown_node`,
`fleet_enrollment_route_revocation`, `fleet_enrollment_incomplete_import` and
`fleet_enrollment_physical_host`. Complete inventory consumption in typed
capacity/maintenance admission, move-participant trust acknowledgements and
explicit failure-domain policy remain required. FO-04 through FO-10 remain
pending. Automatic fleet management stays default-off; no deployed RustFS/Colima,
resource-enforcement, TPS/p99 or soak qualification is claimed.

## FO-03 progress: catalog-scoped move admission

Source `b5d25b4b0425d2db673f043fe68585ea990c29b5` gates a fresh managed repair on authenticated trust
acknowledgements from its actual source and target processes. The opaque
membership authorization retains its source root revision. The existing metadata
RPC requires both processes to install at least that cut; the source revalidates
the target process, disk, engine and epoch before running the configuration and
committed-guard checks again. Already committed exact plans continue without a
new acknowledgement, including when the target is unreachable.

The preflight uses the original deadline and a 64 KiB control reservation.
Retries follow directory polling, capped at 250 ms between attempts. Cancellation,
expiry, local drain and shutdown refuse an unsubmitted action. Physical RPC work
is joined before releasing its reservations; an unpolled SDK waiter retains its
separate 4 KiB request reservation until delivery. Refusal does not consume the
permit or release the root reservation. Business-write acknowledgement adds no
catalog or observation call.

A deterministic per-host catalog gate reproduced the missing admission boundary:
the prior path returned Seeding while the target still had the older trust cut.
The native regression checks no learner/sequence consumption while blocked,
continued native SQL, cancellation, expiry, rejection above the existing
60-second SDK bound, eventual admission, idempotent continuation and abort with
a stopped target, and joined shutdown of an unpolled
60-second attempt while its unresolved root reservation remains retained.

Further review found actual cross-catalog acknowledgement confusion. Two hosts
using distinct coordination prefixes could share a fleet ID, namespace storage,
capability digest, physical records, epochs and sufficient revision numbers.
A counted TCP proxy reproduced an accepted reply from the other catalog. Trust
snapshots, reuse/successor checks and authenticated node routes now bind the exact
catalog fingerprint. The regression requires an actual connection to the foreign
process and a signed refusal; missing discovery cannot satisfy it. Unit checks
also reject foreign trust installation/reuse and absent or zero route scope.

`fleet_enrollment_unknown_node` publishes a real unrooted storage pin/process
view, then sends correctly authenticated metadata and Control/Data frames for
unknown and dormant histories. Signed refusals arrive before body admission;
rooted nodes, engine groups, dormant metadata and application callbacks remain
unchanged. No listing is used to expand trust.

The experimental protocol hard-cuts to `/catalog-trust-1`. The catalog fingerprint
is mandatory in node metadata routes. Use fresh experimental fleet/data prefixes,
namespaces and matching binaries. Numeric format markers remain 1; no migration
or compatibility reader is provided.

**360 native cases passed across 13 complete targets**, including 59 enrollment
and 25 permit/import cases. Five direct-list process helpers run through their
passing parent cases. **136 Raft library units** and **four startup-confirmation
cases** passed. Clippy with warnings denied, format/layout over 949 Rust files,
native dependency checks (454 packages, no JavaScript engine), documentation,
source preflight, plan validation and all 10 validator tests passed. The manifest
retains 169 required cases and 71 reviewed source anchors; no package status
advances in this substep.

Initial fixture compile failures, the reproduced early-consumption and accepted
foreign-catalog replies, and intermediate failed fixture checks remain retained.
The shutdown fixture was corrected to distinguish the SDK's intentional 4 KiB
waiter retention from joined physical work; raw Raft probes now use the separate
transport protocol digest. No guard, membership, refusal or timeout assertion was
weakened. No stack override or debug instrumentation remains. The first cohort
was deliberately interrupted after Clippy and its two focused cases to add exact
catalog scoping; its partial native run is not counted as qualified. A second
attempt was stopped after Clippy to test a suspected deadline mismatch; that hypothesis was disproved
by the existing SDK rejection of managed timeouts above 60 seconds. The final
case retains that boundary check; no production timeout behavior was changed.
Earlier readiness/setup timing instability remains unresolved.

The **partial** record `implementation/fo03/catalog-trust-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source/log/executable
hashes and run provenance. Mutable build-cache paths do not archive executable
bytes. No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. Its three absent mandatory cases remain
`fleet_enrollment_route_revocation`, `fleet_enrollment_incomplete_import` and
`fleet_enrollment_physical_host`. Fresh-move trust acknowledgement is implemented;
complete route/process revocation with physical joins, typed inventory-consuming
lifecycle admission and rooted failure-domain policy still require delivery and
qualification. FO-04 through FO-10 remain pending. Automatic fleet management
stays default-off; no deployed RustFS/Colima, resource-enforcement, TPS/p99 or soak
qualification is claimed.

## FO-03 progress: physical revocation and successor completion

Source `1a6dc10e691fa10c292ff88e883692dfc2e03fec` adds the complete native route/process revocation
case. Its managed learner retains the same engine after a joined SDK shutdown,
resumes under a new process during RestartPending, catches up verified native SQL,
and completes joint promotion. All original peers acknowledge the new process.
A blocked request to the previous process closes while its server remains live;
the old publisher cannot replace the new directory view.

An empty peer preserves an already admitted transport call through RestartPending,
ResizePending, Draining and Evacuated. Removed closes and joins physical I/O and
releases metadata reservations before the SDK waiter is consumed. A valid late
advisory directory publication cannot restore rooted trust. The case also runs
the actual source/target trust-acknowledgement scenario and joins all owning SDK
nodes. Catalog state/permit edits remain privileged fixture inputs, not operator
evacuation, removal or capacity-activation certificates.

The first broader run found a repair-completion race. A new proposal had been
accepted while its waiter still observed the previous Complete repair. The
waiter returned CommandConflict; a subsequent safe quorum read proved the exact
new permit had committed Seeding at position 7. The conflict decision now waits
until the accepted position applies. The existing retirement case reproduced the
failure both in the cohort and alone; operation-specific error context identified
the second Begin. Tagged diagnostics were removed before final qualification.
The fix introduces no retry or reclassification of an error as success.

**361 native cases passed across 13 complete targets**, including 60 enrollment
and 25 permit/import cases. Five direct-list process helpers run through their
passing parents. **136 Raft library units** and **four startup-confirmation cases**
passed. Clippy with warnings denied, format/layout over 951 Rust files, native
dependency checks (454 packages, no JavaScript engine), documentation, source
preflight, plan validation and all 10 validator tests passed. The plan retains
169 required cases and 71 reviewed source anchors. This is a fresh complete
cohort; the prior source's 360-case record remains separately retained.

Initial compilation and ordinary-stack assembly failures remain retained. The
scenario boundaries use boxed futures; no stack override, timeout extension,
assertion weakening, lint suppression or debug instrumentation remains. The first
qualification attempt stopped after its enrollment failure and is not counted as
a passing cohort. Earlier readiness/setup timing instability remains unresolved;
the demonstrated successor-completion race is a separate finding.

The **partial** record `implementation/fo03/route-revocation-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/` retains exact source/log/executable
hashes and the failed-attempt provenance. Mutable build-cache paths do not archive
executable bytes. No raw evidence is tracked under `plans/multi-raft/evidence/`.
This substep changes no wire format; the experimental protocol still ends in
`/catalog-trust-1`.

FO-03 remains **IN_PROGRESS**. Its two absent mandatory cases are
`fleet_enrollment_incomplete_import` and `fleet_enrollment_physical_host`.
Typed lifecycle admission must consume a revalidated complete inventory and
operation-specific live proofs. Rooted node-versus-physical-host placement policy
must apply to later movement, beyond existing birth checks. FO-04 through FO-10
remain pending. Automatic fleet management stays default-off. No deployed
RustFS/Colima, resource-enforcement, TPS/p99 or soak qualification is claimed.

## FO-03 progress: rooted placement and evacuation admission

Source `67e3d6f4d2193f87ea7db122ac832b53c2c3d8b1` records explicit `PhysicalHosts` or `Nodes` replica
separation. The default requires distinct physical hosts. The head retains the
current choice and the genesis pin its initial choice; incompatible initialization,
missing policy fields and unknown values refuse. `Nodes` permits an explicitly
colocated fleet without claiming independence from loss of a shared host.

Managed birth admission uses that rooted policy. A fresh managed repair checks
the actual durable configuration plus its proposed learner after source/target
trust acknowledgements. Rooted physical-host identities supply separation;
advisory regions, zones and process labels do not. A refusal consumes no member
or permit sequence and leaves the root reservation retained. Exact committed
plans still reconcile after exclusion. Native coverage fences the unused permit,
then performs a legal replacement through peer seed, joint completion and S3 SQL;
the explicit colocated policy also completes real RF3 birth and replacement.

`begin_evacuation` now consumes complete inventory inside an exclusion-admission
turn. A single root CAS records the canonical operation projection, raises one
Active node's epoch to Draining and claims the disruption slot. The head retains
a digest of that slot, so missing data and generic deletion/reset cannot create
an apparently free slot. The immutable catalog-scoped operation-request object
binds operation ID, mutation identity, principal, original revision and node
before root CAS. Its binding survives a losing candidate. Admitted retries use
the journal and preserve the original applied revision, including after a later
incomplete namespace is declared. Admission reads verify canonical and journal
linkage; historical counts and node epochs grant no lease or removal authority.

Six native lifecycle cases cover authenticated observation of exclusion, refusal
of fresh target placement, continued S3 SQL, omitted/unadopted namespaces, missing
canonical history, a held CAS losing to a concurrent declaration, permanent
operation identity, lost replies through a recreated adapter, missing request
bytes, generic slot-deletion refusal, racing disruptions, cancellation and the
original deadline. Metadata and owning SDK nodes are joined. Catalog fixture
state inputs remain privileged; this is not capacity-activation qualification.

**369 native cases passed across 13 complete targets**, including 67 enrollment,
12 catalog and 25 permit/import cases. Five directly ignored process helpers run
through their passing parent cases. **136 Raft library units** and **four startup
confirmation cases** passed. Clippy with warnings denied, formatting/layout over
955 Rust files, native dependency checks (454 packages, no JavaScript engine),
documentation, source preflight, plan validation and all 10 validator tests
passed. The plan retains 176 required cases and 71 reviewed source anchors.

The first filtered two-target run passed its 12 catalog cases and 59 enrollment
cases, with seven enrollment cases excluded by the filter and one fixture
failure: it tried to register a new dormant history after exclusion. The fixture
now registers before exclusion and verifies that a subsequent call refuses.
No production guard was relaxed. A red regression also reproduced operation-ID
reuse reaching inventory checking after a losing CAS; the permanent request
binding corrects that gap. Initial and intermediate logs remain retained. No
assertion, timeout or lint was weakened, and no stack override or debug probe
remains. Earlier readiness/setup timing instability remains unresolved.

The **partial** record `implementation/fo03/lifecycle-placement-boundary.json`
under ignored `.cellar/raft-perf/fleet-operations/` retains exact source, log and
executable hashes. Build-cache paths do not archive executable bytes. No raw files
are tracked under `plans/multi-raft/evidence/`. The protocol hard-cuts to
`/fleet-admission-1`: use fresh experimental fleet/data prefixes, namespaces and
matching binaries. Numeric markers stay at 1, with no compatibility reader or
migration. New head/pin fields and canonical request linkage are mandatory.

FO-03 remains **IN_PROGRESS**. The remaining absent mandatory case is
`fleet_enrollment_incomplete_import`, whose oracle is terminal removal
certification, beyond exclusion admission. Capacity activation/readiness and
terminal lifecycle integration remain required. This increment moves no replicas
automatically, stops no process, certifies no node absence, marks no node
Evacuated/Removed, releases no disruption or membership reservation, and
reclaims no storage. FO-04 through FO-10 remain pending and fleet automation
stays default-off. RustFS/Colima limits, TPS/p99, full scaling workflows and soak
qualification remain unproven.

## FO-03 progress: remote quorum observations

Source `cc27382774f7acf4d589dfbe7de4fe3620019d43` adds
`RaftCells::observe_peer_fleet_membership` for an empty coordinator. The target
independently reads a rooted history at least as new as the requested trust cut,
checks its actual birth/schema, performs safe ReadIndex, and captures a coherent
configuration and publication sample. The opaque response binds the exact
original or joined member, node, disk, process and engine. Retained results expose
the complete invocation age through SDK delivery and supply no lease.

A distinct RPC purpose and TLS exporter authenticate catalog, scope, history,
nonce and both physical endpoints. Both peers retain metadata reservations;
both hosts retain response reservations. EOF, caller cancellation, the original
deadline, physical revocation and shutdown cancel and join nested SDK work.
No dormant registration, local replica, SQL image or business query is required
at the coordinator. Rooted node trust creates no seeding or membership-mutation
grant. The existing write acknowledgement path calls none of this metadata work.

Five focused native cases passed before final formatting. They cover image-free
original members, retained response age, dormant refusal, an injected older root,
actual held target catalog IO with continued native S3 SQL, minority refusal,
cancellation, shutdown, learner binding, joint roles and stable replacement.
The initial fixture used the unmanaged/colocated constructor and correctly failed
managed birth reservation before reaching the RPC; it now uses declared managed
births on distinct hosts. No assertion or timeout was weakened.

The first broad attempt failed an existing host cache-pressure case with
`node is closing` before Raft startup. A second attempt failed archive setup
with `node authority was not established before startup deadline`. Three exact
unchanged-binary cache replays passed. Temporary fixture tracing then produced
20 passing exact replays and a complete passing 29-case host target without
reproducing either startup failure. The fixture is restored byte-for-byte;
these diagnostic runs do not replace final source qualification. No production
logic, lease setting, assertion or deadline was changed, and the startup cause
remains unresolved. Both failed attempts and the probe logs are retained.

Final source qualification passed **374 native cases across 13 complete targets**,
including 72 enrollment, 12 catalog and 25 permit/import cases. Five process
helpers remain ignored in direct listings and run through passing parents.
Another **140 Raft library tests and four startup-confirmation cases passed**.
Affected-target/lib Clippy with warnings denied, formatting/layout over 961 Rust
files, native dependencies (454 reachable packages, no JavaScript engine),
documentation, source preflight and all 10 plan-validator tests passed.

Nineteen changed Rust files stayed identical from final formatting through the
successful qualification matrix and match the committed source. Temporary
fixture tracing was restored exactly before that matrix. The **partial** record
`implementation/fo03/peer-quorum-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/` retains source, log and executable hashes,
including both earlier failed attempts. Mutable build-cache paths do not archive
executable bytes. The repeated green matrix does not establish the cause of the
earlier startup failures.

The plan retains 181 required cases and 71 reviewed source anchors.
The protocol ends in `/fleet-quorum-1`;
use fresh experimental coordination/data prefixes, namespaces and matching
binaries. Numeric markers stay at 1, with no compatibility reader or migration.
No raw evidence is tracked under `plans/multi-raft/evidence/`.

FO-03 remains **IN_PROGRESS**. The missing mandatory
`fleet_enrollment_incomplete_import` case still requires the actual removal
certification boundary. Remote quorum observations are inputs to that verifier,
not complete inventory, archive coverage, physical-work joins or terminal
Evacuated/Removed receipts. Capacity activation, automatic placement, the
reconciler, operator workflows, scaling and deployed qualification remain open.

## FO-03 progress: coherent publication cuts

Source `ef6cca13d15864e25235f66c5f0fe04f84447c54` adds opaque `FleetPublishedCut` and
`FleetPublishedImage` values to authenticated remote quorum observations. A
single cache read captures the durable apply marker, exact published image,
configuration and physical member. A pending publication yields no cut; a cut
with no image proves canonical empty state. Ordinary status inspection cannot
transport this stronger statement.

Fixed-cut comparison covers configuration-only progress without requiring a
new SQL image. Intact restore may publish the same image at a later protocol-only
position. Equal image state can cover that alias; changed data must follow the
entire minimum applied cut. Different catalog/history or membership revisions
refuse comparison. A name-only snapshot handoff remains pending until the exact
durable and published markers match.

The final source matrix passed **376 native cases across 13 complete targets**,
including 74 enrollment, 12 catalog and 25 permit/import cases. Another **145 Raft
library tests and four startup-confirmation cases passed**. Five directly ignored
process helpers execute through passing parents. Affected-target/lib Clippy with
warnings denied, format/layout over 965 Rust files, native dependencies,
documentation, source preflight and all 10 plan-validator tests passed. The plan
retains 183 required cases and 71 reviewed source anchors.

Two new native cases verify configuration-only permit fencing, all three
survivors' coverage, full intact-cluster restart and later S3 writes, plus a held
real private-SQL callback whose uncommitted image never enters the cut. After
release, S3 acknowledgement, publication position and the actual native SQL
result agree. Four new cut units and one wire unit cover prefix conflicts,
canonical empty versus pending, exact markers, restore aliases, strict bounded
encoding and ordinary inspection's inability to supply a cut.

The first private-SQL fixture waited for sequence 1, but the existing gate holds
sequence 2. It timed out before reaching the observation. The corrected fixture
performs its initial sequence-1 write and then holds sequence 2. No production
guard, assertion or deadline was weakened. Earlier startup/readiness failures
remain unexplained; this increment claims no startup fix.

Sixteen changed Rust files stayed byte-identical through final qualification and
match the committed source. The **partial** record
`implementation/fo03/published-cut-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/` retains source/log/executable hashes and the
failed initial fixture log. Mutable build-cache hashes do not archive executable
bytes. No raw files are tracked under `plans/multi-raft/evidence/`.

The experimental protocol now ends in `/fleet-quorum-1/fleet-cuts-1`; use fresh
fleet/data prefixes, namespaces and matching binaries. Numeric markers remain 1,
with no compatibility reader or migration.

FO-03 remains **IN_PROGRESS**. These historical cuts certify neither current
health nor archive payload/replay coverage. Canonical-empty archive proof and
the actual terminal verifier remain required; the missing mandatory case is
still `fleet_enrollment_incomplete_import` at removal certification. Capacity
readiness, automatic placement/reconciliation, maintenance/operator commands,
scaling and deployed RustFS/Colima/performance/soak gates remain open. Fleet
automation stays default-off, with no performance improvement claim.

## FO-03 progress: canonical-empty archives

Source `886f85df887a904b402478b476a0745329761bc1` adds explicit canonical-empty archives for
cold groups. A cut captures the actual durable configuration, membership/fleet
guard, scope and birth/schema binding while proving the durable image is absent.
A missing publication of applied data refuses capture rather than becoming
empty. Empty captures retain bounded membership data without copying log tails.
The existing bounded producer scheduler now publishes cold resident groups.

The manifest requires explicit null snapshot-image, snapshot-object and final
image fields. Empty state forbids tails, dependencies and position divergence;
half-present state or zero-TXID images refuse. No SQLite image or payload object
is fabricated. `restore_state` returns `None` only for validated canonical empty
state and never deletes an existing generation. Image-only `restore` refuses it.
The first real SQL image establishes a complete snapshot; an older empty cut
cannot replace newer archived data. Mutable-head reconciliation and immutable
exact-reference reads retain their existing conditional-write contracts.

Authenticated S3 seeding uses the same live-quorum, learner, process, history,
engine and fleet-permit checks. An empty seed installs the existing empty snapshot
representation without an application callback. No archive value supplies voting,
activation, reservation-release or terminal-removal authority.

The final frozen-source matrix passed **382 native cases across 13 complete
targets**, including 56 archive, 75 enrollment and 46 snapshot cases. Another
**147 Raft library tests and four startup-confirmation cases passed**. Five ignored
process helpers execute through passing parents. Affected-target/lib Clippy with
warnings denied, formatting/layout over 968 Rust files, native dependencies,
documentation, source preflight and all 10 plan-validator tests passed. The plan
retains 189 required cases and 73 reviewed source anchors.

Six new native cases cover lost conditional-head replies, retained empty
references, first-image transition, refusal to erase live data, two-empty and
mixed batches, cold S3 seed/first write, actual minority refusal without target
group or member claim, and a managed cold repair through S3 seed, joint consensus
and the first native durable SQL command. Two new manifest units enforce strict
null-field presence and refusal of contradictory/payload-bearing empty states.
The managed test uses privileged rooted capacity/permit fixture inputs; it does
not qualify the pending controller or capacity-readiness adapter.

The first fixture compile passed borrowed paths to `GenerationStore::open`,
which requires owned paths. Both new paths were corrected. Six focused native
cases then passed before formatting; the cold-minority fixture was strengthened
before final qualification to inspect the actual peer attempt, physical engine
and absent member claim. No production guard, assertion, deadline or lint was
weakened. The first full frozen matrix passed 373 native cases and failed nine
across six targets. Three fixture assumptions accounted for four case executions:
producer/close checks observed archive IO before their actual SQL write, and the
bounded-report fixture assumed cold histories could never archive. All three
reproduced unchanged. Corrected fixtures require the actual published SQL cut
before close, or an explicit archive outage while preserving zero callbacks and
all four lagged histories. Their focused checks passed. Five other timing/readiness
cases passed unchanged individual replays; their causes remain unexplained. No
startup, timing or performance fix is claimed.

A second attempt passed 227 native cases and failed two, with six targets never
executed because their files were absent. All 13 expected native executable paths
were later confirmed missing while the target directory remained present with
ample free space. The cause is unknown. The managed cold test also repeated a
broad-only `NotLeader` result; permanent phase context was added without changing
source selection, retries or deadlines. A new task-isolated build retained both
executables before a passing full 75-case enrollment target and exact host-parity
case. Neither failure is claimed fixed by changing the build directory.

Fourteen changed Rust files stayed byte-identical through qualification and match
the committed source. The **partial** record
`implementation/fo03/empty-archive-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/` retains source/log/executable hashes and the
initial fixture failure and both broad attempts. The final run retains actual
executable bytes copied before execution, with verified hashes and build inputs,
outside the mutable target directory. Earlier attempts retain their stated
artifact limitations. No raw files are tracked under `plans/multi-raft/evidence/`.

The protocol ends in `/fleet-cuts-1/empty-archives-1`; use fresh experimental
fleet/data prefixes, namespaces and matching binaries. Numeric markers stay at
1 with no compatibility reader or migration.

FO-03 remains **IN_PROGRESS**. Next bind exact selected archive references and
physical verification into an owned terminal verifier alongside fresh survivor
cuts, complete inventory, settled permits and physical-work joins/fences. The
mandatory `fleet_enrollment_incomplete_import` removal-certification case remains
absent. Capacity readiness, placement/reconciliation, maintenance/operator APIs,
scaling and deployed RustFS/Colima/performance/soak qualification remain required.
Fleet automation stays default-off.

## Main integration: cold archive recovery

Source `181d2dba679459cfe4c4b869b4366db5c0b3de85` merges main
`906eb36a7a2ad2a8687310adcac880d8f4f0e0cc`. Archive manifest and replay conflicts
were resolved while retaining explicit canonical-empty state and main's configured
image/decode bounds: 64 MiB default, opt-in up to 256 MiB. These size profiles do
not qualify capacity or recovery latency at 1-vCPU/3-GiB limits.

The merged source passed **382 native cases across 13 complete Raft targets**,
**12 cases across three typed-RPC integration targets**, **151 Raft library
cases**, and **4 startup-confirmation cases**. Five directly ignored process
helpers run through passing parents. Affected-target/lib Clippy with warnings
denied, formatting and layout over 1,091 Rust files, native dependency checks,
documentation, plan validation and all 10 plan-validator tests passed. All
1,163 tracked Rust files and captured Cargo/configuration inputs stayed identical
through the run. All 35 compiled executable files were copied and hash-checked
before the test run; integration, unit and startup suites executed from retained
copies. The partial record is
`implementation/fo03/empty-archive-main-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/`.

Source anchors are refreshed to this merged source. Earlier failed attempts and
their limits remain recorded above; a passing integration run establishes no
startup fix, TPS improvement or deployed RustFS/Colima qualification. FO-03 stays
**IN_PROGRESS**, with `fleet_enrollment_incomplete_import` still missing at actual
removal certification. The next implementation boundary remains exact archive
verification, terminal survivor proof/receipt/slot release, then capacity readiness.

## FO-03 progress: physically verified archive coverage

Source `e083e6a2c58b245cd378a189351aff0c0776d488` adds
`CellArchive::verify_published_at(reference, minimum, verifier, timeout)` and the
opaque `VerifiedArchiveCoverage`. Exact immutable reads, payload hashes, complete
prefix coverage and physical replay must all pass before a proof is returned.
Canonical-empty archives use the same gate without fabricating SQL state.
Missing selected material never substitutes a newer head. Configuration changes
require a new attempt, and failed replay quarantines the scratch owner.

Manifests now require the producer's `image_position`, independently of the
archive applied/snapshot position. Full snapshots preserve that publication origin;
tails cannot place it before their last data entry. Cold state uses explicit null.
Legitimate producer/restart aliases remain comparable by full image state and
configuration. The request's original deadline spans object IO, workspace wait,
physical work and delayed delivery. A waiter that expires behind another replay
refuses before touching scratch state. Blocking work retains its inputs through
its execution owner. No business-write acknowledgement calls this verifier.

Four new native cases exercise cold/first/later data, retained references,
configuration changes, replay-capacity refusal/quarantine, full intact restart,
held payload IO, unpolled delivery, missing selected references and exact producer
origin preservation across protocol-only progress and recovered aliases. The
fleet fixture supplies privileged capacity/permit inputs; this is not operator
or controller qualification. A manifest unit adds required/bounded-origin cases.

The frozen source passed **386 native Raft cases across 13
complete targets**, **12 typed-RPC cases**, **152
Raft unit cases**, and **4 startup cases**. Five directly
ignored process helpers execute through passing parents. Affected-target/lib
Clippy, formatting/layout over 1,094 Rust files, native dependencies, docs, source
preflight, plan validation and all 10 plan-validator tests passed. The plan retains
193 required cases and 74 reviewed source anchors. All 1166 tracked
Rust files and captured build inputs stayed identical through qualification.
The 35 compiled executable files were retained before the
run; integration/unit/startup suites executed from those copies. The partial
record is `implementation/fo03/archive-coverage-boundary.json` under ignored
`.cellar/raft-perf/fleet-operations/`.

An initial unit build failed because its direct archive fixture omitted the new
deadline field; explicit `None` repaired that fixture. A new fleet assertion then
incorrectly equated different producers' publication origins: the observed image
was identical at origins 3 and 4. The failed binary and diagnostic replay are
retained. Exact-origin assertions moved to the real capture boundary; fleet
coverage retains strict image/configuration checks and now exercises intact
restart aliases. Earlier timing/readiness failures remain unresolved. No startup
fix, enforced-resource qualification, TPS gain or latency gain is claimed.

The experimental protocol ends in `/empty-archives-1/archive-cuts-1`; use fresh
fleet/data prefixes, namespaces and matching binaries. Markers stay at 1 with no
legacy reader or migration. FO-03 remains **IN_PROGRESS**. Consume these archive
proofs in terminal survivor verification, immutable receipts and disruption-slot
release; close `fleet_enrollment_incomplete_import` at actual removal certification.
Capacity readiness and FO-04 through FO-10 remain required. Automation stays off.

## FO-03 progress: inventory-consuming evacuation candidates

Source `f130ed3ef58f05d3423a00d88d850af9c3d47fe7` connects complete rooted inventory to
`FleetEvacuationVerification`. Preparation checks the current root, canonical
active operation/journal, exact Draining node and every declared namespace/history.
Each pending live history owns three authenticated safe-ReadIndex observations,
checks stable RF3, current survivor identities/engine pins, the exact settled
latest permit and rooted failure-domain separation, then physically verifies the
selected immutable archive. Voter comparisons use logical sets because raft-rs
ConfState vectors have no ordering guarantee.

`freeze_live` retains one fixed historical publication cut while ordinary writes
and archive catch-up continue. Repeated replay attempts keep that boundary.
Reserved births block; canonical cancellations are consumed explicitly. A bounded
immutable receipt index and digest-addressed candidate manifest cover every
rooted history. Rooted-input changes invalidate the attempt. Unrelated Operations
progress is allowed, while a missing or corrupted original operation refuses.
Candidate publication confirms the final index page and leaves node state, the
root and disruption ownership unchanged. Stored bytes cannot recreate proofs
or supply terminal removal/stop authority after a crash.

Five new native cases cover complete inventory and stable retries, failed
archive reads without progress, unconsumed permits, reserved/cancelled births,
missing imports, fixed cuts across later S3 writes, operation corruption, and
real S3 learner seeding through joint-to-stable voter replacement. The departing
original voter retains physical history; the three survivors continue writes
and queries. Capacity/permit metadata remains privileged fixture input.

Frozen-source qualification passed **223 native cases across five complete
targets**: enrollment 83, catalog 12, permits 25, archive 57 and snapshot 46,
plus **152 Raft unit cases**. Two snapshot process helpers remain ignored in
direct listings and execute through their passing parent crash matrices. Lib and
affected-target Clippy with warnings denied, formatting/layout over 1,098 Rust
files, native dependencies (465 packages, no JavaScript engine), documentation,
plan validation and all ten validator tests passed. All 1170 Rust files and
captured Cargo inputs stayed unchanged and match committed Git blobs. Six test
executables were retained and hash-checked. This is an incremental affected-scope
run; the earlier 386-case/13-target matrix remains qualified at its own source.
The plan now retains **198 required cases and 74 reviewed source anchors**.

Initial compilation and diagnostic failures remain retained. Concurrent survivor
calls reproduced typed `Observation(Overloaded)`; sequencing respects the bounded
metadata turn and one original deadline. Fixtures now wait for actual installed
trust after rooted changes. Publication verifies its final empty index page.
The replacement-specific teardown requires its actual one-replica state and
joined owners; existing empty-observer assertions remain intact. No proof check,
original deadline or durability assertion was weakened.

The ignored partial record is
`implementation/fo03/candidate-boundary.json` under
`.cellar/raft-perf/fleet-operations/`, with source/log/executable hashes and retained
failed attempts. No raw evidence is tracked under `plans/multi-raft/evidence/`.
The combined listing and passed-case audit still identify
`fleet_enrollment_incomplete_import` as absent at the required terminal boundary.
FO-03 remains **IN_PROGRESS**: fresh terminal survivor verification, physical
joins/fences, atomic terminal receipt/slot release and capacity readiness remain.
FO-04 through FO-10 are pending. This increment changes no existing protocol
suffix and claims no startup fix, RustFS deployment, enforced resource tier,
automation, TPS/latency gain or soak qualification.

## FO-03 progress: rooted boots and Finalizing fence

Source `f06b286edbeb6285001bc75b5ecc32edd68ba779` orders every managed boot, including intact restart,
against Finalizing through a root CAS before endpoint publication. A boot claim
binds its nonce, process, permanent engine pin and original node epoch to the
canonical request and permanent journal. Ordinary state changes retain that
claim; a newer admitted boot invalidates older candidates and running-host
credentials. Concurrent startup CAS conflicts fail cleanly; owned controller
reconciliation remains pending.

`FleetEvacuationCandidate::begin_finalization` rechecks complete inventory and
the active disruption, owns an authenticated probe of the exact departing
process, then atomically roots its canonical Finalizing projection and higher
node epoch. The original deadline is checked immediately before head CAS.
Exact committed retries reconcile without live inputs, including after adapter
recreation. Reusing an identity with different inputs refuses. The disruption
slot remains held and the process remains owned; this observation grants no
physical stop, terminal removal, capacity release or volume deletion authority.

Five new native cases cover retained boot identity across state changes, fresh
inventory/probe admission, lost replies and exact retries, actual intact-engine
boot refusal after Finalizing, invalidation by an admitted restart, and a held
finalization CAS losing to a real concurrent restart through a separate adapter.
The original owner joins before each retained engine is reopened. Survivor S3
writes and scratch/owner cleanup remain asserted.

Broader qualification exposed a restart liveness bug: a rooted boot refresh
removed old physical pools, while directory discovery recreated only pools still
present. The corrected path recreates pools for retained group/query obligations
and refreshes the same logical grants after verified discovery. It rejects old
views, preserves membership/grant revisions, avoids pools for empty discovery
and retains final Removed revocation. A new transport regression covers that
boundary. Controlled TLS sinks now obtain a real rooted boot and retain its exact
published view; attempting a second publication under one process correctly
refuses. Intact restart now asserts exactly one additional root revision and
node epoch, while retaining engine identity. No assertion or deadline was weakened.

Frozen-source verification passed **396 native Raft cases across 13 targets**,
**153 Raft unit cases**, **12 RPC cases** and **four startup-confirmation cases**.
The managed enrollment target passed all 88 cases. Five helper cases remain
ignored in direct listings and are not included in those passed-case totals.
Clippy for the library and selected targets passed with warnings denied, as did
formatting/layout over 1,101 Rust files, native dependencies (465 packages, no
JavaScript engine), documentation, plan validation and its ten validator tests.
The plan retains **204 required cases and 74 reviewed source anchors**.

A final test-import scope correction required rerunning enrollment. Rebuilding
all selected targets confirmed that every other executable was byte-identical
to the passing broad run. The qualification record retains 35 actual executables
(18 test executables), their hashes, both source manifests and the linked result
ledgers. All 1,173 Rust files and 21 Cargo/configuration inputs match the committed
Git blobs. The combined audit contains 46 passing stages and still identifies
only `fleet_enrollment_incomplete_import` as missing at the actual terminal
removal boundary. This is functional qualification against conditional in-memory
storage, native TLS and actual filesystem/SQLite, not deployed capacity proof.

Initial fixture/codec failures and the failed broad qualification attempts remain
retained. The first broad run stopped after its current native target joined;
the next run passed 86 enrollment cases but refused two reused-process fixture
publications. Those failures are not counted as passes. The final run uses the
corrected frozen source and independently retained actual executables.

The partial record is `implementation/fo03/finalization-fence-boundary.json`
under ignored `.cellar/raft-perf/fleet-operations/`. No raw evidence is tracked
under `plans/multi-raft/evidence/`. Node records require explicit nullable `boot`,
and the experimental protocol now ends in `/archive-cuts-1/fleet-finalization-1`.
The immediately preceding protocol is explicitly refused. Numeric markers remain
1; use fresh prefixes/namespaces and matching binaries, without a migration or
compatibility reader.

FO-03 remains **IN_PROGRESS**. Physical stop/join fencing, restart-safe resumption
while Finalizing, fresh terminal survivor checks, atomic terminal receipt/slot
release and capacity readiness remain required. The terminal
`fleet_enrollment_incomplete_import` case is still absent. FO-04 through FO-10
remain pending; this increment establishes no deployed RustFS/Colima capacity,
TPS/latency improvement, automation or soak qualification.

## FO-03 progress: exact-boot stops and signed joined closure

Source `024acc19fd70edcc4374280341140a8fabe8a964` pins the host's closure verification key in its rooted
boot claim and fsyncs a separate bounded local lifecycle record before endpoint
publication. The real engine worker persists exact operation/epoch/boot/process
stop acceptance before a Node-owned task signals shutdown. Caller loss does not
cancel that task. A synced local fence defeats delayed stops at its epoch;
accepted stops cannot be cancelled or rebound. Missing lifecycle metadata refuses
engine Resume. The guard adds no object-store call to business acknowledgement.

The outer host signs and conditionally publishes closure only after the Node
domain and physical Raft owners join. The CLI joins its core thread too. Closure
publication owns a separate bounded reporting domain and reconciles a lost PUT
response through exact bounded readback. Verification checks the exact selector
and the rooted boot key and returns an opaque proof. The signature assumes
trusted host code/private keys and coordination credentials. It supplies no
fresh survivor/archive health, application completion, terminal node transition,
volume deletion or disruption-slot release authority.

The local SDK and existing authenticated private management endpoint accept the
scoped stop/fence DTO. Managed hosts refuse generic management Stop. The admin
adapter enforces Operate permission, identical guard/audit request IDs and an
accepted, unconfirmed status. Legacy Actor-lease discovery remains a gap: a
standalone authenticated Raft-control-lane RPC is still needed for an empty
Raft-only node and a fleet controller.

New native tests hold the actual engine sync and engine-lock drop, discard the
caller, and wait for autonomous completion without another shutdown command.
They refuse closure before physical join, verify publication after a lost PUT
response, reject checksum-correct signature tampering, exercise local fence
retry/late-stop refusal, and reject new stop admission after inventory changes.
These focused stop fixtures use an empty managed departing host. Composing an
actual voter replacement under writes with scoped stop, joined closure and a
terminal removal receipt remains an end-to-end acceptance gap.
Engine tests cover retained fences/accepted stops, fresh boot nonces despite a
reused process hint, expired queued work, failed-sync ambiguity and missing
mandatory lifecycle state. The first broader cohort aborted on a deterministic
startup stack overflow in the unchanged retained-binding recovery test after
79 enrollment cases passed. The retained published control passes that exact
case; crash frames identify the nested startup polling path. The failed cohort
and isolated reproduction remain retained and are not counted as a passing run.
Boxing an inner helper was insufficient and was removed. The fix boxes the
composite startup future at the SDK/domain boundary, preserving its public async
API and cancellation ownership while preventing callers from embedding all
nested startup state. The unchanged recovery case passes with the original
stack configuration. In the retained arm64 debug executables, its main polling
frame falls from approximately 825 KiB to 497 KiB; this is a compiler-frame
measurement, not a steady-state memory or TPS claim. Temporary diagnostic output
is removed.

The audited pre-coalescing cohort passes **400 native Raft cases across 13
targets**, **157 Raft units**, **12 RPC cases**, **93 legacy Node cases**, **four
startup cases** and **19 admin cases**. Its 56 stages include warnings-denied
Clippy, formatting/layout, dependencies, documentation and plan checks. Forty
actual executables (22 test executables), the frozen 1,179 Rust files and 21
Cargo/configuration inputs, exact source tree and patch are retained. Five
process helper cases remain ignored in direct listings and are excluded from
passed totals.

Final review then reproduced duplicate shutdown-signal queueing with 10,000
accepted retries while the driver can still be joining physical I/O. The
production delivery helper remembers one successful or failed delivery outcome
per reporter, rather than accumulating signals. The regression fails with the
original repeated-send behavior; the real held-engine-lock case also exercises
32 exact accepted requests. Only the reporter and that native retry test change
in Rust after the audited broad cohort. Final-source verification passes **400 native Raft cases across 13 targets**,
**158 Raft units**, **12 RPC cases**, **four startup cases** and **19 admin cases**.
All 55 stages pass, including Clippy for the library/selected targets and admin
with warnings denied, formatting/layout over 1,107 Rust files, native dependencies
(465 packages, no JavaScript engine), documentation, plan validation and ten
validator cases. The final record retains 39 actual executables (21 test
executables); all 1,179 Rust files and 21 Cargo/configuration inputs match the
implementation commit. The 93 legacy Node cases remain attributed to the earlier
audited tree, without claiming a final-source rerun. Both source trees are
independently checked against their Git blobs, and the exact earlier patch is
retained. The plan has **213 required cases and 80 source anchors** and still
identifies only `fleet_enrollment_incomplete_import` as absent at its actual
terminal removal boundary.

The partial record is `implementation/fo03/physical-stop-boundary.json` under
ignored `.cellar/raft-perf/fleet-operations/`. It retains source/executable/log
hashes, both successful cohorts, the startup failure/control/fix evidence and the
expected failing duplicate-signal regression. These are native TLS, actual
filesystem/SQLite and conditional in-memory-store checks. No raw evidence is
tracked under `plans/multi-raft/evidence/`.

The experimental protocol now ends in `/fleet-finalization-1/fleet-stop-1`.
Use fresh coordination/data prefixes, namespaces, **engine directories** and
matching binaries. Numeric markers remain 1, with no compatibility reader or
migration. Automatic management remains default-off. FO-03 remains **IN_PROGRESS**;
this increment does not close the actual terminal
`fleet_enrollment_incomplete_import` case or claim deployed RustFS, resource,
performance or soak qualification.

## FO-03 progress: standalone Raft stop control

Source `43bcd17ab9c8d750fa2a2c6d76d8d18a102a6093` adds
`RaftCells::request_peer_fleet_stop` on authenticated Raft control. Both origin
and target may have zero business groups; no Actor lease or dummy cell is used.
A separate TLS exporter/MAC purpose binds the physical route, body digest,
request nonce and reply. New receiver work verifies the origin's current rooted
boot and permanent boot admission. The original origin budget covers queueing
and transport; the receiver includes prefix/header residence from TLS admission
and retains its absolute deadline through the bounded Node-owned worker.
Lost callers/replies remain unconfirmed and cannot undo admitted physical work.

The actual retired-voter case now composes guarded S3 learner seeding, joint
membership completion, rooted Finalizing, Raft-control stop during survivor
writes, joined retained physical history, exact signed closure and another
survivor write. Four new native RPC cases cover empty endpoints/fencing, caller
loss during real engine sync, expired metadata work and removed-origin refusal.
Four wire units cover authentication/purpose/channel/body binding, exact rooted
endpoints, bounded canonical inputs and receiver deadline residence.

All **58 qualification stages** pass: **404 native Raft cases across 13 targets**
(including 96 enrollment), **162 Raft units**, **12 RPC cases**, **four startup
cases** and **19 admin cases**. Warnings-denied Clippy for the library/selected
targets and admin, formatting/layout (1,111 authored Rust files), native
dependencies (465 packages, no JavaScript engine), documentation, plan validation
and ten validator cases pass. The audit retains 39 actual executables (21 test
executables); all 1,183 Rust files and 21 Cargo/configuration inputs match this
implementation commit. Five direct-listing process helpers remain ignored and
excluded from the passed totals. Earlier legacy Node results remain attributed
to their earlier audited source rather than a current-source rerun.

The plan now has 221 required cases and 80 source anchors. The remaining absent
FO-03 name is `fleet_enrollment_incomplete_import` at actual terminal removal;
that listing fact does not complete resumption, terminal proof or capacity
readiness. FO-03 remains **IN_PROGRESS**. The partial record at ignored
`implementation/fo03/stop-rpc-boundary.json` retains the final cohort, actual
executable/source/log hashes, earlier passing smoke runs and the initial test
fixture compile failure. No raw evidence is tracked under
`plans/multi-raft/evidence/`.

The experimental protocol now ends in
`/fleet-finalization-1/fleet-stop-1/fleet-stop-rpc-1`. Use fresh coordination/data
prefixes, namespaces, engine directories and matching binaries; markers remain
1, with no migration or compatibility reader. Terminal removal, slot release,
actual capacity readiness, deployed RustFS/Colima, performance and soak remain
unqualified. Automatic fleet management stays default-off.

## FO-03 progress: attested capacity readiness

Source `489a47cdc2f175e0ad874449b3cb552da2805524` implements typed Ready/Active
admission. The adapter owns complete current inventory and an authenticated node
probe, verifies the exact synced boot and installed host/consensus/engine bounds,
then commits the state transition and readiness record together. Ready remains
excluded. Platform CPU/memory/storage limits and retained usage are explicitly
trusted deployment attestation with an evidence reference/digest, not independently
measured kernel limits. Installed buffer totals are not process RSS.

Current capacity authorization checks the exact node epoch/boot and permanent
journal. A bare Active descriptor supplies no quantified capacity, and historical
acceptance after shutdown/exclusion supplies no current authority. Profile
revisions cannot change limits, an already qualified Active boot requires
maintenance before limit changes, and intact new boots must qualify again.
The directory's process-generation reuse refusal remains enforced; the new
restart case covers the refused attempt, joined cleanup and a valid fresh
process generation. Concurrent exclusion defeats an activation held at CAS.
Expired reads and incomplete/unfit evidence cause no activation.

Seven new native capacity cases pass. Current-source regressions pass **103
enrollment cases**, **16 directory cases** and **162 Raft units**, including the
focused metadata and four stop-RPC cases. Library/enrollment/directory Clippy with
warnings denied, formatting/layout, native dependencies, documentation, plan
validation and ten validator tests pass. All 1,186 Rust files and 21 Cargo/config
inputs match this implementation commit; retained actual executables and logs
are audited. The other native targets, legacy Node, admin and general RPC suites
remain attributed to their earlier-source cohorts rather than a current rerun.

The plan inventories 228 cases and 80 source anchors. The ignored partial record
`implementation/fo03/capacity-boundary.json` retains successful and failed attempts.
FO-03 remains IN_PROGRESS: terminal resumption, fresh final proofs, atomic
removal/slot release and the actual terminal incomplete-import case remain open.
FO-04 must combine current capacity authorization with fresh occupancy,
per-history eligibility and resource/movement reservations. Automatic management,
measured platform limits, deployed RustFS/Colima, TPS/latency and soak remain
unqualified. No raw evidence is tracked under `plans/multi-raft/evidence/`.

The current experimental protocol ends in
`/fleet-stop-rpc-1/capacity-readiness-1`. Use fresh coordination/data prefixes,
namespaces, engine directories and matching binaries; numeric markers remain 1.

## FO-04 progress: conservative resource ledger

Source `c79245d1c7308071e0d29e1964ad33a7c872be8c` begins FO-04 with the dependency-free `raft_fleet` component.
Its resource ledger charges complete physical high water, unresolved incremental
peak growth and explicit policy reserve independently for engine slots, durable
bytes and memory. It bounds the reservation inventory and active movement count.
Attribution is cumulative and exact-boot/process/profile scoped; older-scope debt
keeps its full charge. Stale, incomplete, conflicting, overflowing or partially
invalid inputs refuse without mutating the accepted ledger. Lower physical
readings do not free retained state, and no resource-release API is exposed.

All 51 ordinary decision-core integration cases pass, including 17 new accounting
cases and 34 existing cases. Crate/test Clippy passes with warnings denied;
formatting, layout and native dependency checks pass. The runner freezes source
and Cargo inputs, copies the actual executed binaries and records their hashes.
Logs and the partial `fo04/ledger-boundary.json` record remain under ignored
`.cellar/raft-perf/fleet-operations/implementation/`. No files were added under
`plans/multi-raft/evidence/`.

This is a pure accounting boundary, not an atomic catalog reservation, an
authenticated physical sample or executed fleet movement. The local last-slot
case is not the required two-catalog-writer race. All twelve native FO-04 cases
remain required, and none is claimed complete by these pure tests. Next integrate
current typed capacity, complete fresh physical observations and rooted movement
reservations in one admission CAS, then implement complete staged placement and
one-node-loss simulation. FO-03 terminal removal remains open and gates lifecycle
operations; it does not block this dependency-free accounting component.
FO-04 is `IN_PROGRESS`; FO-05 through FO-10 remain `TODO`. The fleet remains
default-off, with no new throughput, latency or deployed-capacity qualification.

## FO-04 progress: atomic resource debits

Source `c8e6815e50c70bd582619c5a63dbbf7a0820b67b` consumes current typed capacity
in `FleetCatalog::reserve_resources`. It owns complete inventory and an
authenticated exact-boot node probe, combines explicit deployment occupancy
attestation with installed runtime floors, then installs the journal-linked
resource row and active movement book through one expected-root CAS. Physical
high water and unresolved old-boot/profile growth remain charged. The initial
execution profile bounds active resource debits to two fleet-wide. Generic
metadata writes cannot delete or replace admitted rows or reset the book.

The six new native cases pass, including an actual last-slot race between
independent catalog adapters, an exclusion winning against a held resource CAS,
lost successful CAS response/confirmation with target shutdown and adapter
recreation, original-deadline cancellation without a detached local writer,
invalid/incomplete input refusal, and unmaterialized genesis debt. The seven
capacity cases and earlier-protocol refusal also pass. The full current native
cohort passes 137 cases: enrollment 109, catalog 12 and directory 16. All 162
selected Raft unit cases pass; focused checks overlap those totals. Clippy for
the library and these integration targets passes with warnings denied. The
runner freezes all 1,193 Rust files and 21 Cargo/configuration inputs and retains
the actual executed binaries and hashes. All inputs match this code commit. Formatting/layout, native dependencies (465 reachable packages, no JavaScript engine), documentation, plan validation, ten validator tests and source preflight pass. The partial record and logs are retained at ignored `implementation/fo04/resource-boundary.json` and adjacent `fo04-resources-v2-*` paths under `.cellar/raft-perf/fleet-operations/`. No raw evidence is tracked.

The resource cases currently run through the existing
`native_raft_fleet_enrollment` fixture. The complete
`native_raft_fleet_placement` target and all its required acceptance boundaries
remain deliverables; a passing resource case does not close FO-04.

This is resource-only admission. The historical receipt grants no learner or
membership authority. Complete physical history attribution, linking each debit
with its exact membership permit, source/target work ownership and terminal
release remain required. Unknown legacy permit attribution and noncancelled
genesis obligations refuse instead of counting them as free. Before releasing
an active debit, retain its physical high water independently of the completed
row so unused-growth release cannot also credit retained bytes or slots.
Complete final/staged assignment and one-node-loss restoration feasibility are
still pending. Deployed resource measurements, RustFS and performance are not
qualified; automatic fleet management remains default-off.

The experimental protocol now ends in `/capacity-readiness-1/resource-reservations-1`
so earlier writers cannot bypass the new append-only resource-book rules. Markers
stay at 1; use fresh coordination/data prefixes, namespaces, engine directories
and matching binaries. No compatibility reader or migration is added.

## FO-04 progress: authenticated physical history attribution

The resource consumer now owns a complete authenticated engine-history snapshot.
It matches original-voter references against full-scope digest, immutable birth
and physical member ID. A Reserved or unmaterialized Live birth remains blocked;
a durable voter is charged while another history can reserve unused capacity.
The prospective learner's occupied history refuses even when aggregate capacity
is available. Retired/unregistered engine histories remain visible after intact
restart, without a business registration or live quorum for that history.

The compact binary snapshot is bounded to 4,096 histories and 311,341 wire bytes,
within a 1-MiB response/metadata reservation. Capture validates the engine/node
count cut and preserves pending initialization. The request mode is MAC-bound;
ordinary node probes explicitly omit inventory. Response charges and original
age survive unpolled delivery and cancellation. Resource rows retain a canonical
inventory input digest scoped by their exact boot/profile and fleet root.

The physical-inventory source at `8b57107d55996763a6e9bdb2e80d1af8848189e6`
passed all-target Clippy and 138 native cases (enrollment 110, catalog 12 and
directory 16). Its Raft unit cohort passed 165 and failed one startup test with
temporary transport overload. The retained executable reproduced that failure
and also passed a subsequent focused run. The test now retries only explicit
heartbeat overload under its original bounded wait and still requires success;
this source change remains in qualification with the terminal-removal work.
The failed cohort is retained and is not reported as a passing unit run.

Earlier failed attempts remain retained: the
retention fixture constructed its timer outside the execution domain; a raw
unrooted-node frame lacked the new required request field; Clippy required a
shared observation type and fixed-size chunk API. All-target Clippy additionally
found the filesystem fault adapter loaded three times in library tests; these
consumers now share one owning test module. Assertions and fault behavior were
preserved. The strengthened warm-node case also refuses an extra debit against
an actual occupied slot plus an existing reservation despite a zero caller slot
attestation. No failed attempt is treated as a pass.

Typed membership-permit integration and terminal resource release remain open.
Keep retained physical high water independently of completed debit rows before
releasing movement slots. Complete staged assignment, one-node-loss restoration
feasibility, owned reconciliation and operator workflows remain required.
FO-03 and FO-04 stay `IN_PROGRESS`; this snapshot grants no membership, removal,
resource release, deletion or health lease. Platform occupancy remains separately
deployment-attested; RustFS/Colima performance and resource enforcement are not
qualified. The managed fleet remains default-off.

The experimental protocol ends in `/resource-reservations-1/physical-inventory-1`.
Resource records require the inventory digest and node requests require explicit
mode. Keep markers at 1 and use fresh coordination/data prefixes, namespaces,
engine directories and matching binaries; no compatibility reader or migration.

## Historical priorities before movement execution

This earlier snapshot is retained as history. The current feature queue appears
near the start of this document.

| Priority | Boundary | Required result |
| --- | --- | --- |
| P0 | FO-04 placement | Atomically link resource debits and exact membership permits; execute and release real moves under fresh terminal proof while retaining physical high water. Complete staged feasibility and single-failure restoration headroom. |
| P1 | FO-05 load and FO-06 reconciliation | Fresh scoped load observations and a domain-owned resumable reconciler. Restart, cancellation and ambiguous replies converge on one exact operation. |
| P2 | FO-07 maintenance and FO-08 operator workflows | Shared-slot restart/resize/rejoin/cancel/remove, explicit unreachable-owner isolation, authenticated idempotent SDK/admin/CLI and typed outcomes. |
| P3 | FO-09 scaling | Desired/effective/observed horizontal and vertical capacity; fewer cells alone cannot establish retained-state shrink fit. |
| Release gate | FO-10 deployed qualification | Actual RustFS and five/six/four-node operations, original three-node/1,500-group density, enforced 1-vCPU/3-GiB limits, faults and 24-hour soak. Successful TPS, bytes/sec and p50/p95/p99; prefer p99 below 200 ms, hard ceiling 500 ms. |

Fleet management remains default-off. No local feature test establishes a TPS
or latency improvement. Optional performance changes require matched throughput
or latency gains. Preserve the investigation order: archive-head failures and
repeated replica work, control queueing by traffic class, SQLite copying versus
validation versus synchronization, archive batch flush reasons, then idle-group
scheduling cost. Earlier workspace contention was negligible; the sixteen-frame
control window is already implemented.

## Expanded operating design

The [lifecycle execution contract](lifecycle-execution.md) adds the authority and
linearization point of each effect, crash/lost-response resumption, bounded
catalog batches, empty-host activation acknowledgements and the separate desired,
effective-placement and observed-platform resource profiles. It specifies the
prospective shrink ceiling and the post-stop fit check before reducing platform
limits. Future departing nodes are excluded through the pinned target set while
only the current evacuation owns disruption work. Optional balancing retains
predictions and fresh outcome observations rather than assuming movement helps.

The planning examples now include exact symmetric placement tables. Five-to-six
moves 250 copies and produces 25 cells on each six-node triple; five-to-four
moves E's 300 copies and produces 125 cells on each remaining triple. A separate
arithmetic check parsed the documented tables, verified all resulting per-node
counts and six-node failed-pair classifications, and found feasible balanced
leader assignments through a bounded flow calculation. These are design checks,
not execution of the real seed/membership protocols or measured TPS gains.

The expanded contracts are assigned to the existing pending acceptance cases.
That design update retained 103 cases, with no package status advanced. Plan validation,
all 10 validator tests, local documentation links and diff whitespace checks pass
for this documentation update. Deployed capacity, lifecycle, performance and soak
qualification remain pending.

The [planning details](planning-details.md) specify exact resource accounting,
staged feasibility, worked five-to-six/five-to-four changes, failed-pair
classification, heterogeneous role costs, fewer-cell sizing and the removal
proof boundary. Maintenance now has a phase-by-phase decision table and measured
service-stage lower bounds for convergence. The operator workflow defines the
required preview fields and distinguishes a clean non-admission conflict from
an unknown submission outcome.

The earlier planning expansion added eleven future runtime cases across FO-04,
FO-07, FO-09 and FO-10, bringing the inventory to 97 at that checkpoint; subsequent
guard and birth work brought it to 103 before the six host-enrollment cases.
That expansion marked no additional package done.
The design also corrects the earlier 25% load statement: serving copies grow
from 300 to 375 after RF3 restoration on four survivors; that is not a prediction
of immediate follower work or throughput.

For this documentation expansion, plan validation and its 10 validator tests,
repository documentation checks and diff whitespace checks passed. A separate
arithmetic check enumerated all ten failed pairs and verified the copy counts,
fewer-cell count bounds and leader-transfer rate example. Those checks do not
execute the new native acceptance cases. No new benchmark or full runtime
qualification was claimed by that documentation expansion. FO-02 native guard
verification is recorded separately above; full fleet qualification is pending.
