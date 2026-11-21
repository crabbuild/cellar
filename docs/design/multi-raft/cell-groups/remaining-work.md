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

## Primitive delivery priority

**Prioritize full CG-17 closure.** All 32 cases, including public adapters,
provider fencing, WebSockets, deployment/clock changes, retention and recovery,
remain required. The [current checkpoint](progress.md#current-integration-checkpoint)
pins the actual results; selected passes do not close a primitive family.

Production `6c3471b5b` closes the stranded deletion-intent transition: both
maintenance and public Reconcile now drive a proven `Deleted/Pending` intent
through the stable-ID fenced deletion protocol even when no Delete receipt
exists. Unknown earlier mutations still block; completed effects are not replayed.

Both new regressions and the original live-write, restart, live-exec, basic and
expiry profiles pass **28/28 native and 28/28 real RustFS configurations**, with
26 library unit tests passing. Original budgets, IDs, minima and assertions
remain. These focused passes do not establish every primitive's full contract
or all response/load guarantees. See the
[current checkpoint](progress.md#cg-17-deletion-intent-recovery).

The earlier bounded archive packing and its failed controls remain source-pinned
below. Broader native archive/controller gates and complete release acceptance
remain unresolved. The generated implementation index is current; full plan
validation/preflight still requires missing historical CG-01 evidence.

The next feature deliveries are:

1. Connect normal native Container/Sandbox and interactive APIs to the
   request-bound controller and actual fenced execution. Deliver the owned
   asynchronous invocation prologue, per-operation authorization, independent
   controller/backend faults and real provider-volume/fresh-incarnation recovery.
2. Finish safe receipt reclamation with origin/recovery floors, WebSockets and
   every public primitive/feature/deployment/clock/retention/lifecycle ledger.
   Resolve the broader cancelled-import, archive-outage remote-admission and
   remote-enrollment gates; qualify the original response budgets under load.
   Focused provider passes do not override those independent gates.
3. Run the complete constrained Linux/RustFS recovery and release matrix on one
   source before default backend cutover and CG-13/15/16 acceptance. Restore
   verified CG-01 custody or requalify the actual required cases.

The [controller contract](../../../../crates/cellar-container/controller.md)
explains the delivered adapter and restart ownership. Current tests use a
controlled protocol peer and one native RF3 process. Inventory remains partial
at 18 entries/318 symbols. CG-17 stays open at 0/32; the whole design and hard
cutover goal remain active. The source-pinned checkpoints below are historical.

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
| CG-17 | In progress. `6c3471b5b` fixes durable deletion intent with no controller receipt. Focused provider configurations pass 28/28 native and 28/28 real RustFS; 26 library tests pass. | Deliver normal native Container/Sandbox, independent fenced/authenticated execution and volume recovery, receipt GC, WebSockets and every full API/feature/fault ledger. Broader recovery/load gates and all 32 formal cases remain required. See the [current checkpoint](progress.md#cg-17-deletion-intent-recovery). |
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
