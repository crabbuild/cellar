# Outbox implementation status

Updated 2026-10-15. The goal remains the entire
[outbox, stream, and subscription design](002-outbox-streams-subscriptions.md).
The feature is under implementation and is not ready for normal deployment or
release. The consolidated event-delivery work merged in PR #46; PRs #47–#62
are superseded. Follow-ups use one active PR against updated main; subsequent
changes reuse that PR while it is open, without a stack. The native fixture now
exercises source, catalog, stream, and consumer turns together; that local evidence does not satisfy the fleet/release gates.

## Current priorities and readiness

The core bucket-backed producer-to-consumer path works in the exercised cases:
atomic emission, durable dispatch/deduplication, resumable reads, leased fetch,
inbox effects, acknowledgment and preserving restart. It is **not fully release
qualified**. Do not use a green SDK suite as evidence for untested ownership or
storage failures. The ordered list below supersedes historical next-step notes.

| Order | Critical gap | Kind | Acceptance gate |
| --- | --- | --- | --- |
| 1 | Proof, ownership and recovery under failure | Missing qualification; unresolved recovery failures | Record each source commit/data proof/discovery/append/lease/ack cut. Audit original accepted receipts before replay. Missing witnesses/corrupt retained bundles must refuse recovery without sealing or installing a successor; qualify recovery availability only under retained-proof assumptions. Exercise isolated expired owners, follower-only proof and supported store-loss cases; stale owners must not authorize settlement, checkpoints or visible success. |
| 2 | Intermittent Node startup/shutdown failures | Observed failures with unestablished causes | Reproduce cancelled-bootstrap timeout, explicit-policy closed-domain failure and initial clock-fixture activation deadline with stage diagnostics and retained startup metadata; identify the cause, add a regression at the failing boundary, and pass the original failing scenario without relaxing deadlines/assertions. Passing isolated replays do not close this gate. |
| 3 | Retention, lease and destructive-lifecycle races | Incomplete qualification | Cover clock rollback, expiry versus extend/ack/reset, two-consumer effects, grant/adoption changes and interrupted source/facet cleanup. No protected event may be reclaimed, obsolete receipt accepted or retained inbox effect repeated. |
| 4 | Metadata exhaustion and recovery headroom | Incomplete capacity/operational profile | Exercise lane/receipt/tombstone limits, large protected histories, large SQL/directory cleanup, restore/compaction and long soaks. Preserve exact replay at capacity; report fairness, bounded memory and required storage headroom. |
| 5 | Complete operational coverage | Missing implementation and qualification | Finish resident aggregate gauges, facet/historical/namespace inventory and WAL/LTX/bucket physical accounting. Coverage failures must remain explicit; inventories cannot claim a global drain proof. |

The current slice tests live foreign recovery resumption after the same claimed
root is erased or replaced. Two signed reads must return explicit Unavailable
while retaining the old owner and acknowledged evidence. Return the actual
metadata after both operations finish and require the first fresh audit to
succeed under the original budget without a read retry, restart, operator retry
or business-command replay. Host services, lease generation and persisted lineage
must remain identical through the audit. Both initial native cases pass in
20.04s, with live_recovery=true and read_retries=0. All 73 serial Node cases pass
in 610.00s, including all fifteen clock/refusal cases. Both live cases retain zero
read retries in the full run. Probe/full executable hashes match:
bdc7608188830061228431baacaefb2e9c8afaa6e03a4a61778c0bb8effac03b.
Workspace all-target/all-feature checking (8.06s) and Clippy with warnings denied
(7.08s), formatting (518 Rust files), docs (36 documents/six flows/388 references),
dependency policy (403 packages/no JavaScript engine), and whitespace checks pass.
All eight captured Rust sources stayed fixed throughout qualification; no separate
repeat of the clock suite was needed. The library/log/core/CLI/Python/doc-test
results below remain prior unchanged-production evidence, not reruns in this
slice. Production code and owned markers remain unchanged.

The next selected loss-prevention check is historical bundle GC with structurally
readable but invalid footer row bounds/transaction identity. Source inspection
finds structural footer decoding before coverage comparison; it is not yet a
reproduced deletion defect. Require a concrete native regression before changing
production code. Invalid/unproven retained payload must survive GC, while valid
independently covered objects still collect. This does not close the broader
fault/capacity matrix or earlier intermittent-startup cause audits.

The previous slice extends the claim-phase root-loss boundary to a different
node taking ownership through a cold route. Its own startup may succeed; the
foreign cell read must return an explicit Unavailable verdict within the original
three-second budget after erasing/replacing the predecessor root. Join that
recoverer before returning actual captured metadata. Require unchanged old cell
ownership, acknowledged follower batches and preexisting bucket objects, then
recover original event/clock/source/inbox/checkpoints before receipt replay.
Both initial native cases pass in 23.24s. Runtime diagnostics show the missing
session root and changed-lineage guards refusing the actual recovery, followed
by a higher-epoch foreign successor with the original clock, one protected
event/effect, final checkpoint two and producer settlement two. This selected
retry joins and restarts the failed foreign recoverer; it does not establish
in-place resumption on that same running process. The frozen focused run passes
in 18.15s and all thirteen clock/refusal cases pass in 137.62s, using the same
saved executable SHA256: b1d234f7d6d55d07ad2cee041676bc15a3b1950da46d3ec64f5140a2de595e7a.
All 71 serial Node cases pass in 580.93s with that same executable. Workspace
all-target/all-feature checking (9.66s), Clippy with warnings denied (8.01s),
formatting (518 Rust files), docs (36 documents/six flows/388 references),
dependency policy (403 packages/no JavaScript engine) and whitespace checks pass.
All eight captured Rust source hashes stayed fixed throughout qualification.
The library/log/core/CLI/Python and doc-test results below are prior evidence for
unchanged production code; this test-only slice did not repeat those suites.
No production recovery/admission or Cellar-owned markers changed. That slice
leaves same-process resumption to the follow-up above. Other predecessor,
rollback, process, physical-loss, capacity and intermittent-startup cause audits
remain open.

The previous slice qualifies metadata loss after a real predecessor recovery
claim, before the member gather and final seal. A selected same-name startup
holds its ordinary-bucket GET of the actual Recovering root. Erasure and a
replacement lineage must each produce the specific recovery refusal, preserve
the old cell owner, acknowledged follower .batch bytes and existing bucket
objects, and never fabricate/overwrite a root. Safe uploads and follower seals
may advance before the final read refuses. After failed startup joins, returning
the actual claimed metadata must recover the original event/clock/source/inbox/
checkpoints before receipt reuse. Both initial cases pass in 21.44s. The first
frozen clock run passed 10/11 in 108.00s: an older post-seal case failed before
root injection because it immediately expected a blocked bucket upload. Fleet
proof can release output before the independent upload task runs. Four saved
executable replays passed and do not erase that failure. The fixture now waits
for the real blocked attempt within the unchanged three-second operation budget,
then performs the exact original bucket-prefix audit. Its first amended run
observed the late blocked upload after 4.518ms and passed; twelve further saved
runtime diagnostic runs pass with three concurrent fixture processes. These
runs diagnose the scheduler assumption but are not the serial qualification.
The final focused run passes both new cuts in 22.81s, followed by all eleven
clock/refusal cases in 128.34s. Their saved executable SHA256 matches:
f503309d013e48a05c9f9778737e60081da83060556afebfdcdb3838f9d3a86e.
All 69 serial Node cases pass in 568.16s with the same executable, as do eight
native log cases (6.33s), 242 library cases (6.53s) and all 15 decision-core cases.
All six actor, six ingress, five protocol, seven artifact and three CLI cases
pass. Workspace all-target/all-feature checking (7.82s), Clippy with warnings
denied (6.88s), formatting (518 Rust files), docs (36 documents/six flows/388
references), dependency policy (403 packages/no JavaScript engine), and 129 Python
guards pass. Python reports two unclosed-SQLite ResourceWarnings without a failed
test. All eight captured Rust sources remain fixed through qualification.
Production recovery/admission code and all Cellar-owned markers remain unchanged.
The previous doc-test evidence below still applies to unchanged production code;
this test-only follow-up did not repeat it.

The previous slice addresses reproduced startup-GC data loss. A competing boot
holds an actual no-log lease response while the live generation opens its log
and receives real Bucket proof. The old sweep deletes that generation's bundle.
With follower data appends excluded and the original writer force-joined/local
directory removed, cold recovery restores no database. The saved pre-fix
executable fails the original-prefix audit in 1.06s (no command replay).

Historical collection now runs outside admission and requires fresh contiguous
per-cell coverage or persisted epoch retirement for every row, including
validated checkpoint hints. Maintenance pages at most 512 same-lineage objects
under a two-second deadline. The focused regression passes in 1.76s and all
eight native log cases pass in 6.63s with matching executable hashes. They
require eventual collection after another process uploads recovery data and
preservation of excluded/corrupt/unproven objects.

The previous full Node suite was red: 66/67 passed in 521.13s; the cold-source/
follower restart failed during catalog activation (`__catalogResolve`). Both
pre-GC and new saved runtimes also produced activation deadlines in isolated
replays. Preserve those failed runs; passing replays alone do not close a gate.

Temporary stage probes then reproduced two cold-start deadline failures with
actual recovery sealed in 29ms, successful catalog reads, and both peers waiting
in `__streamResolve`. The fixture forbids their partition-owner CAS writes but
issued no partition request on the permitted replacement until after joining all
three startup futures. Its binding activation could therefore wait indefinitely
for a claimant that had never been asked to claim. Start the replacement's real
signed read-only audit inside its startup future while both peers resolve their
bindings. The original post-startup state audit and all ownership, clock,
history, receipt and shutdown assertions remain. No production recovery/admission
code, proof, schema marker or operation deadline changes.

The one-variable ordering probe passes ten runs (cold startup 134–426ms), using
identical runtime diagnostics before/after the fixture change. All temporary
probes are removed. The final cleaned fixture passes seven isolated cold runs,
all nine clock/refusal cases (115.12s) and all 67 serial Node cases (530.24s),
with the independent post-startup audit retained. All three use the same saved
Node runtime SHA256:
2242ce2cd049aef574edc758835a6f850390cea9a0496f92489d463a7a385fe5.

An earlier cleaned-fixture clock run was red: 8/9 passed in 120.09s and the
unchanged missing-lease case reported an initial activation deadline before its
metadata-loss cut. That failure's cause remains unestablished. Six isolated
instrumented replays and 21 subsequent diagnostic runs passed; they do not
explain the failed run. Original destination/source/witness startup errors now
name the role and retain the fixture directory for inspection after the test
process terminates. Failure-only cold-start diagnostics retain all three node
outcomes. The traced stream-resolution dependency cycle is diagnosed; the
initial/catalog-only contexts and the older cancelled-bootstrap/closed-domain
failures retain their separate cause audits.

Remaining selected local checks pass: eight native log cases (6.16s), 242 library
cases (7.99s), 15 decision-core, six actor, six ingress, five protocol, seven
artifact and three CLI cases. Workspace all-target/all-feature checking
(240.28s), Clippy with warnings denied (82.80s), formatting of 518 Rust files,
docs (36 documents/six flows/388 references), the 403-package dependency policy
without a JavaScript engine, and all 129 Python guards pass. A compiler attempt
failed when this checkout's target directory disappeared; the workspace volume
was writable, the directory was recreated, and only unfinished checks were
retried. The compiler failure and passing retry remain recorded. All seven
captured Rust source hashes stayed fixed through qualification. The five GC
implementation/fixture sources remain identical to 90f219f, and activation
runtime code is restored byte for byte after probing. The previous workspace
doc tests (11 passed, one existing ignored example) remain unchanged-production
code evidence; this test-only follow-up did not rerun them.

Local Rust is 1.97.0/aarch64 macOS. These local results do not complete the
release fault/capacity matrix or establish the causes of earlier intermittent
startup failures. Hosted Rust 1.98.1 validation remains separate. Further proof
qualification must cover same-process resumption and other foreign/predecessor
cut points, then metadata rollback, independent-process and physical storage/host
cuts.

The broader proof/fault matrix,
other predecessor recovery cut points, metadata rollback and independent-process
clock-refusal matrix remain open.
Selected local rollback/reopen and subscription handoff cases below do not close
that matrix. The startup/shutdown cause audit remains open and
must resume on recurrence, followed by physical store/host loss and the
remaining commit/proof and lifecycle/capacity cuts. The current process fixtures
cover selected follower-only source acceptance and destination append/fetch/ack
boundaries; they do not complete the follower-proof failure matrix.
PRs #41 and #42 cover selected lost-fetch handoff and unproven-ack forced
closure cases; neither is independent-process evidence. The process harnesses
already cover append, ack and whole-fleet loss at selected boundaries; the
current slice extends them to fetch loss with live/expired leases. Earlier
intermittent no-event bootstrap/recovery failures remain unexplained. Passing
reruns and any one backend/cut cannot qualify the complete failure matrix.

SQLite-only event deployment remains explicitly unsupported until local recovery
inventory/discovery exists and is qualified. Multi-partition streams, push
consumer runners and exactly-once external effects are outside the first-release
contract, not missing implementations of its promised behavior. External effects
still require destination idempotency or reconciliation.

Each continuation must select the highest actionable correctness risk, record
the work and evidence gained, and leave its acceptance gate open until satisfied.
SDK work takes precedence only when it addresses unsafe replay, authorization,
acknowledgment or another concrete correctness risk. No new loss/corruption bug
is asserted merely because its failure case is not yet qualified.

All Cellar-owned protocol, format and schema markers stay at 1 during internal
development. Layout changes use hard cutovers with recreated state and rebuilt
participants. Later sections retain historical verification notes; their old
version numbers, migrations, legacy readers and fleet-upgrade tests describe
removed implementations and are not current supported behavior.

## Current follow-up: retained root loss at lease installation

A new native cut reproduces a continuity gap after successful predecessor
recovery. Identity capture reads the original root; recovery seals that exact
node/lineage/generation and publishes real per-cell payload. Hold the core's
next actual lease read with its unchanged sealed bytes/token, erase the root,
then return the response. The stale Match CAS is rejected, but an absent
readback selects a new Absent CAS and admits the successor under the retained
lineage. The read-only audit reaches epoch 2 with the recovered logical clock
intact. This establishes a retained-root continuity defect, not data loss.
The pre-fix run fails in 5.81s; a repeat from the same saved executable fails in
7.41s. Both preserve the observed sealed-proof precondition and original
operation/startup budgets.

The bucket lease writer now refuses an Absent guard when identity capture saw a
retained root. This is a definite pre-transport refusal, before serialization or
applied-log notification; the same immutable provenance already guards the
predecessor recovery read. True first boot remains allowed. No new root read,
wire field, schema marker, compatibility path or retry/deadline policy is added.
The regression requires ordinary startup refusal, no recreated root or changed
cell owner, unchanged payload/follower bytes from the cut, and directory release.
Return the actual captured sealed metadata with only post-join expiry shortened,
then require recovery of the original event/clock/source/inbox/checkpoints before
receipt reuse. The first post-fix case passes (26.20s) and records the definite
pre-transport refusal. A companion control replaces the root with a different
valid lineage and no folded log at the same sealed-read cut. It must preserve
that replacement and original proof while the existing core lineage guard
refuses authority; returning the actual sealed root must recover. Final focused
qualification passes all nine cases (121.95s); the final serial Node suite passes
all 67 cases (658.25s). Focused/full executable hashes match and all three
changed Rust files remain fixed through both runs. Library, ownership/log and
workspace/tooling qualification also passes: 239 library cases (7.55s), all 15
decision-core cases, six actor, six ingress, seven log and five protocol cases.
All seven artifact and three CLI tests pass. Workspace all-target/all-feature
check (104.07s), Clippy with warnings denied (212.11s), formatting of 516 Rust
files and workspace doc tests (11 passed, one existing Axum example ignored)
pass. Docs validate 36 documents/six flows/388 references; dependencies check
403 packages with no JavaScript engine. All 129 Python guards pass. Saved
focused/full runtime SHA256 is
4e7d844be8e5b5eda14dd0d63870cb1edb610016eaa62e3adc922f42277a4e3f.
All three changed Rust files remain identical through qualification and the
move to updated main (same base tree). Local Rust is 1.97.0/aarch64 macOS;
hosted Rust 1.98.1 and the process drivers remain separately unqualified.
Erasure during predecessor recovery, metadata rollback, independent-process
and physical fault matrices remain open.
Concurrent-startup GC is now a reproduced loss case, rather than the earlier
source-inspection candidate. The current follow-up above records its evidence;
complete qualification and remaining physical/metadata cuts stay open.

## Current follow-up: delivery after recovery backpressure

The preserved 79d5a8b Node executable passes 50 additional isolated same-name
metadata-loss cases; those repeats do not establish the earlier timeout's cause.
A separate one-variable probe waits 15s after the recovered-prefix audit and
before acknowledgment. Its second run reproduces the original five-second
post-ack assertion (25.23s overall). At the cutoff, source attempts are 10,
dispatch generation is 11, the original event remains pending, last error is
StreamFull, and retry_at_ms is 21.292s in the future. Destination checkpoint is
1, end is 2 and the lease is absent. Thus this reproduction is a legitimate
saved retry delay, not overdue work or an already accepted event waiting for
source settlement. No operation budget or dispatch policy changes in the probe;
its source is restored byte-for-byte. The initial run did not capture this state,
so these findings do not retroactively prove its exact scheduling path.

The corrected fixture advertises a real proxy for the chosen replacement, then
holds its next automatic __streamAppend after auditing the original recovered
prefix and stale receipt. The phase wait is bounded by the existing 60s maximum
retry plus the original 3s operation budget. Only after a real submission arrives
does it obtain a fresh lease and acknowledge the retained first event. Require
that work to finish inside the original 3s held-request interval, compare the
complete decoded append with the original captured request, and release that
same request. The original five-second settlement observation, source identity,
envelope, inbox and checkpoint assertions remain. No source replay, operator
rearm, row repair, shorter backoff or synthetic proof is introduced.

All three paired 15s controls pass (45.77s, 54.14s and 43.46s overall).
They wait 23.418s, 32.003s and 19.154s for actual automatic submissions and hold
those requests only 54ms, 80ms and 43ms while fetching and acknowledging. The
complete original append, source identity, inbox effects and final checkpoint 2
survive each control. Both temporary probes restore source byte-for-byte; the
final fixture has no injected delay. Final qualification passes below.
The first final focused group passes four cases but three foreign-name cases
expire at the proxy phase gate. A live, read-only inspection during the follower
case records refusal-source as the actual epoch-two owner, also present in its
local stream database path. Local dispatch cannot visit the replacement proxy.
The pre-existing owner-key CAS-only selection fault previously applied only to
same-name cases. Apply it to all seven cases and to cold source/witness restarts,
then explicitly require the chosen successor. Only those peers' conditional
writes to the one stream owner key are refused; reads, node leases, metadata
proof and follower services stay real. Preserve the failed executable, source,
logs and live owner snapshot. This fixture failure is not a recovery defect. The next group passes six cases
and refuses cold-source activation: with peer claims excluded, activation needs
the selected successor that the fixture had started only afterward. Start both
cold follower servers and that successor in one heap-pinned three-way join;
preserve the original activation deadlines and complete each readiness check.
That change passes activation, but the cold case still times out at the dispatch
phase gate in a subsequent six-pass/one-failure group. Retain that failure and
capture source retry state, destination acceptance and actual owner at the
unchanged phase cutoff before identifying its cause. No production scheduler
or retry-policy change is justified by that observation alone.
The cutoff snapshot then shows attempts advancing to 10 with StreamFull, a
future retry and the intended stream owner: automatic dispatch is running.
An independent repeat inspects both real owner records while the gate is held;
the producer is on the same replacement (producer epoch 3, stream epoch 2).
Its local appends correctly bypass the peer proxy. Exclude only the replacement's
conditional writes to that producer owner key and assert distinct source/stream
owners before selecting the next submission. Other peers may still own/recover
the producer; no source or stream data is edited. The original diagnostic run
and repeat remain preserved. These are corrections to remote-phase fixture
selection, not production routing, retry or recovery changes.

This is native domain evidence; recovery/install interleavings, metadata rollback,
process clock-refusal and physical fault qualification remain open.

Final focused qualification passes all seven cases (65.69s). The final serial
Node suite passes all 65 cases (497.11s). The saved focused/full executable
SHA256 matches: 007f275618a0c35b98b77f4baced37bc893768fb78b76330afaf6bc83973487a.
The changed Rust source remains byte-identical throughout both runs. This
follow-up changes one native fixture and documentation; production sources
remain those qualified at 79d5a8b. Workspace all-target/all-feature check passes
(383.24s), as does Clippy with warnings denied (45.31s). All seven artifact and
three CLI cases pass; formatting checks 516 Rust files. Docs validate 36 documents,
six flows and 388 references; the dependency audit checks 403 packages without a
JavaScript engine. All 129 Python guards pass. Hosted run 38813621162 executes
zero steps because GitHub reports failed payments or an exhausted spending limit;
hosted Rust 1.98.1 qualification remains unavailable. Local Rust is 1.97.0 on
aarch64 macOS. Independent-process drivers have not been rerun for the new
session namespace. The earlier sample did not capture causality state; these
controls address demonstrated timing/placement assumptions without claiming
complete startup/recovery availability or a physical-fault qualification.

## Current follow-up: missing node lease recovery evidence

A native log regression reproduces two premature-success paths. After a real
follower fsync proves one SQL write, join both original domains, remove leader
local state and verify no bucket payload exists. Delete only the old node lease
root, preserving the cold follower bytes. Both ensure_recovered and explicit
recover incorrectly return success (1.05s; repeat from the same pre-fix executable 1.04s).
A separate decision-core regression shows a missing lease bypasses RecoverNodeLog
and proceeds toward a claim. These failures establish a metadata-absence defect;
they do not establish a complete process/store-loss contract.

The core now sends named-owner missing leases through the ordinary recovery
interlock. Log reads require a present, identity-matching node lease; only own-node
first boot may read an optional root before installation. A present no-log lease
and a present successor generation retain their recovery-before-replacement
meaning. No schema, wire marker or version changes. The regression requires
refusal without fabricated metadata or a restored cell, unchanged cold follower
proof, and correct original SQL after the saved metadata returns, before command
replay. Initial complete native-log qualification passes all seven cases (4.60s).
The initial full Node admission case passes (8.84s): with actual authenticated
peer routing and blocked payload uploads, a missing destination root refuses
requests without changing the original owner/epoch or bucket payload prefix.
Return the saved original proof fields with post-join expiry shortened; audit
higher-epoch recovery before receipt reuse. Retained logical clock, exact event
bytes, pending source identity, inbox effect and checkpoints all survive.
All 13 decision-core tests, 238 library tests (6.92s), six actor, six external
ingress and seven final native log cases (4.52s) pass.

The first full Node run passes 60 cases and fails two older delayed-reply
fixtures: they deliberately erase a node root to drive NodeLeaseMissing, then
expect recovery to proceed while the evidence is still absent. Preserve that
original missing-lease fence and complete join. The corrected private-SQLite
fault uses DELETE RETURNING to atomically retain the exact removed root (no
GET/DELETE window can discard an intervening log CAS), then conditionally
restores those proof fields with only expiry shortened before successor
recovery. The missing-metadata case above independently requires refusal until
valid evidence returns. The corrected four delayed-reply cases pass (27.67s), preserving the original
fencing reasons and operation deadlines. Final serial Node qualification passes
all 62 tests (491.61s). Saved focused/full-start/full-end executable SHA256 is
79ed1c3d616d7fdec9ee47e24f90fc02a1cdb1dc6afad96c1ca0d3ae6c42e716;
all seven changed Rust files retain identical bytes throughout that qualification.
Artifact (seven) and CLI (three) tests pass. Workspace/all-target/all-feature
check and Clippy with warnings denied pass. Formatting checks 514 Rust files;
docs validate 36 documents/6 flows/388 local references; the dependency audit
checks 403 packages without a JavaScript engine. The 127 Python guards also pass.
All 15 additional integration tests pass across native cells, streams, log
protocol, outbox, replication, runtime, storage and transactions. Workspace
documentation tests pass: 11 passed and one existing Axum example ignored.
Local Rust 1.97.0/aarch64 macOS. Hosted runs 38789605008 and 38789598754 for
implementation commit 120126b were refused before any steps: GitHub reports
failed account payments or a spending limit. Hosted toolchain qualification
(Rust 1.98.1) remains unavailable.

### Current follow-up: same-name identity continuity

After the preceding qualification, change only the replacement node name in
the missing-destination-metadata fixture to the erased owner's name. Keep the
old disk removed, real follower proof, blocked payload uploads, original 30s
stream lease, 3s operation budget and original assertions. The replacement
incorrectly claims epoch two and returns HTTP success with the old lease active:
it has lost the previously proven future expiry clock. The original owner was
epoch one with that same node name. This reproduces in 3.63s and repeats in
4.93s from a saved probe executable (SHA256
95a522ace8fa804acd696a9032afea3d26cf5311586eaeba34677d33036824db).

A missing own-node root looked like first boot, while cells naming this node
used the same-node ownership shortcut. The fix captures a random 128-bit lineage
before actor/log construction, copies it across retained-root restarts, and
persists it in owned cell records and node leases. Existing-cell admission,
claim confirmation, ambiguous CAS reconciliation, remote routing, output proof,
release and epoch GC require the same lineage. Recovery compares the expected
owner lineage with the same root read used for its folded proof decision.
Missing or changed identity cannot be treated as a no-log completion. A
mismatched lease sample is discarded so later independent requests can observe
returned metadata rather than remaining stuck for the old lease TTL.

Log sessions, bundles and follower directories use node/lineage/process-generation.
Startup bundle collection stays within the captured lineage; follower GC cannot
use a fresh root to certify or discard an old lineage’s fragments, even if the
node name and generation match. Required wire fields and directory layout are a
hard cutover with recreated development state; all owned markers remain 1.
Process-fault evidence readers and their negative guards select the full session.
This slice runs their guard tests; the independent-process fault matrix has not
been rerun against the new namespace. Keep that evidence separate from the
current native-domain cases.

The same-name Node regression requires fresh-lineage refusal with the original
owner and payload prefix intact, joins that refused node, returns the saved
original proof metadata with only expiry shortened, and restarts under the
retained lineage. Peer owner-key writes are refused during this selected
successor cut; real follower reads, lease traffic and recovery remain available.
It then requires a higher epoch, the retained future clock, exact event bytes,
pending obligation, checkpoint and inbox effects before receipt reuse.

This test exposed a second defect: a stopped Ambiguous request retained its dead
remote route, so independent later reads kept using the old endpoint. Stopped
attempts now queue guarded route invalidation without waiting for cleanup or
replaying the submitted request. The new library regression failed before the
fix and passes after it; it requires immediate Stop, exactly one invalidation,
a dropped cleanup receiver and no queued dispatch. Ready retries retain their
existing awaited invalidation and captured budget.

The earlier probe restored all original source bytes and retains its failing
runtime separately. The historical 62-case results above qualify only the
pre-lineage implementation. Two targeted negative controls disable only the
same-name lineage predicate or only mismatch-cache eviction. The core regression fails at the intended claim
boundary or the next-request refresh assertion respectively. Both restore the
production source byte-for-byte before formatting and final qualification.
The first full lineage run passes 61 Node cases and fails two older missing-
witness fixtures before recovery, because their manually seeded lease JSON
omits the required lineage. Update only those three synthetic lease records
and the corresponding loss-record path to the current session shape; retain the
original witness, predecessor-generation, directory-release and refusal
assertions and deadlines. Production and all other test Rust bytes are unchanged.
New qualification is recorded below. Loss after lineage capture and before
startup recovery/install, metadata rollback, independent-process and physical
store-loss gates remain open. The
full design remains incomplete and not release-ready.


### Final qualification for node lineage

All 15 decision-core tests and 239 library tests pass (7.13s). The six actor,
six external-ingress, seven log (4.49s) and five log-protocol tests pass. Both
corrected predecessor/lost-witness cases pass without weakening their original
refusals or deadlines. The five focused clock/recovery cases pass (40.38s).
The final serial Node run passes all 63 tests (492.94s); focused/full-start/
full-end executable SHA256 is
056234fdacf8b10ea6cd3f9ac9faaadf116fb0750449637e5f55cb0baf5b9228.
All 16 changed Rust files retain identical bytes from the final focused run
through Node qualification. Production and previously qualified test sources
also match their hashes from the initial core/library/log runs; only the native
Node fixture records and diagnostic lookup changed after the first full run.
All seven artifact and three CLI tests pass.
All 11 additional integration cases pass across cells, event streams, outbox,
replication, runtime, storage and transactions. Workspace all-target/all-feature
check (79.47s), Clippy with warnings denied (91.19s) and formatting of 514 Rust
files pass. Workspace doc tests pass: 11 passed, one existing Axum example
ignored. Documentation validates 36 documents, six flows and 388 references;
dependency audit checks 403 packages without a JavaScript engine. All 129
Python evidence guards pass. Local Rust remains 1.97.0/aarch64 macOS; this does
not supply hosted Rust 1.98.1 qualification or independent-process evidence.


## Current follow-up: root continuity during startup

Hold the first actual own-root GET response, including its original bytes and
object token, after the store has read it. The old destination is joined and
its disk removed; actual followers retain the proven clock-only StreamFull
update and both destination replica and session-bundle publication were blocked.
Delete the root before releasing that unchanged response to lineage capture.
The pre-fix implementation admits epoch two under the old lineage and returns
success with the expired subscription lease active. It loses the proven future
clock. This reproduces in 7.77s and repeats in 7.86s from the saved executable
(SHA256 b8676ebf5e773976d6e154718e674df54767e011a39d0ec07206f19b16c7fde8).

Lineage capture remembered the identity but discarded whether it came from a
retained root. The subsequent optional own-root read mistook the erasure for
first boot, skipped predecessor recovery and installed a no-log successor.
A captured retained-root requirement now refuses that second missing read before
GC or the clean latch. First boot still permits an absent root; preserving
restarts still recover from their retained metadata. No wire, schema or marker
changes; every Cellar-owned marker remains 1.

The regression requires startup refusal and complete directory release, with
unchanged original cell owner, bucket payload and bounded exact follower-file
bytes. Returning the saved metadata changes only expiry after join; the restart
must recover the proven clock, inactive lease, exact history, pending producer
identity, inbox effects and checkpoint before receipt reuse. A companion cut
replaces the root with a different lineage and no folded log after capture;
it must preserve that replacement record and the original proof while refusing.
The private read barrier supplies no metadata, witness or success verdict.
An initial focused run passes six cases and times out in the older same-name
case while waiting for the pending second event after acknowledgment. Its
original five-second observation deadline remains unchanged. An isolated repeat,
a diagnostic seven-case run and ten isolated repeats pass; they do not establish
the failure's cause. The failure now retains the last source snapshot and UTC
at the original cutoff, then samples destination state read-only. Distinguish a
future persisted retry, delayed settlement after acceptance and overdue work
before changing production behavior or the fixture's phase boundary.

The initial log-boundary run also fails before the metadata cut because its
write reports Bucket rather than Fleet proof. A same-runtime repeat and 50
isolated diagnostic repeats pass. Source inspection finds that only per-cell
uploads were delayed: the bundle loop's initial tick could still upload through
a plain node-log bucket. Changing only the private fixture's flush interval to
1ms reproduces the exact proof-selection failure: live WAL (28,872 bytes), local
rows (1,11), one committed follower entry and a published session bundle.

The fixture now rejects only that session's bundle PUTs; metadata CAS, reads,
retained-object injection and loss diagnostics remain real. Under the same 1ms
control, seven bundle writes are actually refused and Fleet proof succeeds,
followed by the original missing-root refusal and retained-witness recovery.
Both timing probes restore source byte-for-byte; production and final fixture
intervals/deadlines are unchanged. The original full boundary failure did not
record its object inventory; the paired control establishes and excludes the
uncontrolled bundle route without claiming a physical-fault qualification.

These are native domain/store cuts, not independent-process or physical-fault
qualification. The post-ack availability failure, recovery/install interleavings
and metadata rollback stay open.



### Qualification of the startup root-continuity fix

The seven focused clock/recovery cases pass (61.01s). The final serial Node run
passes all 65 tests (511.94s), including both new capture cuts and the older
same-name case. Focused/full-start/full-end executable SHA256 is
04de23236e977a3cf15552f28969e87f5011863170996845f56d8fcea46688fd.
All seven changed Rust files remain byte-identical through that final focused
and Node qualification. The 15 decision-core and 239 library cases also pass;
production sources match their hashes from those earlier phases. The diagnostic
and bundle-exclusion additions affect only test sources. Actor, external ingress,
log and log-protocol boundary suites pass (six actor, six ingress, seven log,
five protocol cases). All seven artifact and three CLI cases pass. Workspace
all-target/all-feature check (81.99s), Clippy with warnings denied (91.90s), and
formatting of 516 Rust files pass. Workspace doc tests pass: 11 passed and one
existing Axum example ignored. Docs validate 36 documents, six flows and 388
local references; dependency audit checks 403 packages without a JavaScript
engine. All 129 Python evidence guards pass. Local Rust is 1.97.0/aarch64 macOS;
hosted Rust 1.98.1 and independent-process/physical qualification remain separate.
The initial post-ack observation failure remains unexplained; these passing
results do not close it.


## Current follow-up: proven and unproven clock-only refusals

Two native cases retain the exact original event/lease and a second pending
producer obligation. A signed append is held before submission, then only the
destination UTC steps beyond the ordinary 30s lease. Capture its complete proven
StreamFull response, roll UTC back immediately, and require the original lease
still present with inactive authority, checkpoint zero and unchanged payload.
This excludes expiry maintenance as the source of the measured clock update.
The source must consume the typed refusal within its original 3s call budget.

Exercise bucket proof and actual authenticated follower proof. In follower mode,
refuse both direct stream-replica and old-session bundle uploads before the cut;
compare every byte of the bounded private payload prefixes before recovery.
Force/join the destination, prohibit shutdown repair and discard its local disk.
All nodes use explicit 120s authority TTL/recovery backoff, preserving the 30s
stream lease, 3s operation policy and normal startup/shutdown deadlines. A
post-join conditional write expires only the old node's liveness field, retaining
its folded log evidence. This is controlled authority loss, not natural expiry.
A binding-free fresh entrance audits a different higher-epoch owner before
receipt reuse, with ordinary UTC still below the original expiry. Only a
read-only Ambiguous old-owner transport failure can repeat within the audit.

Recovered logical time must retain the future floor before any replay can repair
it. Required history/backpressure and the original producer identity/sequence
survive; the old receipt is refused, a new lease redelivers exactly once in the
same inbox, and acknowledgment releases capacity for the exact pending envelope.
The proven cases finish checkpoint two, next sequence three/settled two, effect sum
three and complete node/proxy/domain joins with caller executors still usable.
The initial corrected cases pass (14.14s). A third proven-refusal case joins both
original followers and drops their service references, then restarts both from
retained directories with heap-pinned concurrent startup. Neither original
follower domain supplies the recovery tail. This native cold-reopening case
passes (13.60s); graceful follower close does not qualify process/crash/power loss.

A complementary case holds publication after a local clock-only expired-extension
commit, with fleet proof disabled. A read-only forensic connection sees the new
clock and unchanged lease/checkpoint; it supplies no output proof. The original
caller must remain pending and then receive Ambiguous, never an exposed typed
LeaseExpired. Force closure before releasing publication to fail, prohibit
shutdown repair, discard the old disk and preserve folded recovery metadata.
Audit the retained proven prefix before receipt reuse: the old live lease and
one inbox effect survive, its original ack succeeds once, and replay is
AlreadyAcknowledged. This initial case passes (7.96s). Ambiguity by itself never
proves rollback; the explicit publication/disk faults establish this selected
absence case. A targeted negative control removes only append clock capture:
both original recovery cases fail the pre-loss read-only clock audit (4.00s),
showing expiry maintenance cannot mask the missing write. Production bytes restore
exactly (SHA256 57836cf4f53260deee19e0a89099944e476dab394aae8289e8d4ed785d91d710).
Final restored qualification passes all six clock-focused Node tests (39.72s),
including the earlier bounded/required retention cases, and all 61 serial Node
tests (459.16s). Saved focused, full-run start and full-run end executable SHA256
matches: 05ff0e3984b683831a046d12048770e1fcd5e41b8e2a4d6e6f20d2108d2ae1c2.
The five changed Rust test files retain identical bytes throughout qualification;
production code remains identical to the parent implementation. All seven
artifact and three CLI tests pass. Workspace/all-target/all-feature check and
Clippy with warnings denied pass. Formatting checks 514 Rust files; docs validate
36 documents/6 flows/388 references; dependency audit checks 403 packages without
a JavaScript engine. All 127 Python guards and whitespace checks pass. Local
Rust 1.97.0/aarch64 macOS; hosted toolchain qualification remains separate.

Early fixture attempts used short-lived witnesses, covered only direct replica
uploads and removed the old folded log record with liveness. Those cuts did not
establish retained-proof recovery. The corrected fault keeps all authority/CAS
clients and proof metadata intact and blocks both payload paths. Initial audits
through an old entrance received a closed-owner Ambiguous transport result; a
fresh binding-free entrance performs the read-only audit. Compilation mistakes
in fixture arguments/shadowing and one unused import were corrected. These are
not claimed runtime defects. A final focused attempt exposed a comparator that
inserted null for an absent optional log field in bucket-only records; the audit
now preserves field absence while retaining the authority/proof checks.

This qualifies selected native domains/cores and a private SQLite bucket, not
independent processes, natural node expiry, physical
clock/store/power faults or the whole release matrix. Folded node-record loss is
a separate unqualified metadata-loss contract; deleting that record is not an
alternative way to test recovery with retained proof. Historical startup/shutdown
causes and remaining lifecycle/capacity/operational gates stay open.

## 2026-10-15 expiry authority across refused mutations

Real native contexts, host-minted source authority, broker RPC encoding and SQLite
reproduce a stale-settlement defect without background maintenance. At time
100000, accept an event and lease it through 130000. At 130001, attempt an oversized
append, required-capacity append, duplicate append, successful/failed configuration
or coordinated retirement that remains blocked. Roll UTC back to 100000: all six
original cases incorrectly acknowledge the old receipt and advance checkpoint.
The oversized/required-capacity, duplicate and failed-configuration cases also
reproduce from a saved immutable test executable.

An isolated probe persists only the append clock outside its transaction: all
three append cases pass while the two configuration cases and retirement still
fail. This identifies lost clock authority; acknowledgment policy needs no change.
Append only persisted time on insertion, rollback discarded observations, and
scheduling sampled UTC without committing it. A shared store clock now captures
authorized mutations before their rollback scope. Configuration creates its first
clock atomically; existing streams persist it first. Maintenance, subscriptions
and lifecycle use that same boundary, and scheduling reuses persisted time.
Retirement refusals keep clock authority while terminal receipt replay remains
unchanged. All owned markers stay 1; no layout or compatibility changes.

Six initial fixed cases pass (0.28s), each in place and after SQLite close/reopen.
Final regressions also cover grant rotation with exact configuration replay,
denied append/configuration/retirement and time advancing between samples within
one mutation. Preserve accepted records, identities, lease, checkpoint and failed
configuration revision. Denied requests/read-only observations must not acquire
time authority. All 238 serial library tests pass (5.06s), including nine new
regressions covering 22 scenarios. All 57 serial Node tests pass (423.45s), with
matching pre/post-run archived executable SHA256
acb65b773f21df2d1bf71ece31820ab1aba8c9133516004da37c29f08625be52.
Workspace/all-target/all-feature check and Clippy with warnings denied pass.
Formatting checks 512 Rust files; docs validate 36 documents/6 flows/388
references; the dependency audit checks 403 packages without a JavaScript engine.
All 127 Python guards and whitespace checks pass. Local Rust 1.97.0/aarch64
macOS; hosted toolchain qualification remains separate.

These are local native broker/SQLite tests, not independent-process or fleet
proof. Next qualify recovery of the newer clock on a refused RPC through actual
owner/output/follower gates before any replay can repair it. Physical clock/store
loss, historical startup/shutdown causes and the complete release gates remain
open; the full design is not release-ready.

## 2026-10-15 domain clocks and retention across rollback

Native domains now accept a captured WallClock provider, and both node service
structs expose optional wall_clock infrastructure before bootstrap. None keeps
system UTC. Providers follow scoped polls, tasks, blocking work and cleanup;
negative samples clamp to zero. Monotonic budgets/timers and peer signature
clock/entropy keep their separate host boundaries. All native/example/adapter
call sites explicitly select the default. This is an internal source API
cutover; add the field when rebuilding hosts. Owned markers remain 1.

Two real-node cases use a destination-only clock step with the ordinary 30s
stream lease and 3s operation policy. Their explicit 120s node TTL and matching
recovery backoff keep the 31s wall step separate from authority expiry; startup,
shutdown and invocation deadlines are not raised. With one-record capacity and
10s age, a live lease must preserve the exact aged record and the producer's
second pending obligation, including a recorded StreamFull refusal. Observe
expiry through ordinary extension, roll the clock back, then audit read-only
status before new mutations. Durable logical time and inactive old authority
must survive joined handoff to a different, higher-epoch owner with system UTC.

Bounded mode reclaims the old prefix, expires its optional subscription and
requires an audited reset reporting the skipped offset. Required mode preserves
original records and backpressure after expiry/handoff; a new higher lease
replays exact bytes without repeating the retained inbox effect. Its ack releases
capacity for the original second obligation. Both modes finish checkpoint two,
producer next sequence three/settled two, one plus two inbox effects and complete
node/observer drain with usable caller core executors.

The standalone domain clock test passes, including nested domains, async/blocking
inheritance, rollback, nonnegative samples and independent monotonic waits.
Initial node attempts failed fixture TTL/backoff validation and an incorrect
outbox diagnostic path, before the intended qualification. After correcting those
fixture assumptions, both node cases pass (11.58s); these failures are not claimed
product defects. Final focused domain/external/node cases pass
(0.11s/0.68s/16.98s). Full workspace/all-feature library and integration tests pass
(1450.84s), including all 57 serial Node cases (417.43s). The saved Node executable
has matching pre/post-run SHA256
ae8bb37b7847c2e9285a80519fc943ac2415146c370db7c40ec57b0c68f1fc83.
Workspace doctests pass (11 executed, one existing ignored); no-default-feature
Axum tests and counter binary check pass. Workspace/all-target/all-feature check
and Clippy with warnings denied pass. Formatting checks 511 Rust files; docs
validate 36 documents/6 flows/388 references; the dependency audit checks 403
packages without a JavaScript engine. All 127 Python guards and whitespace checks
pass. Local Rust 1.97.0/aarch64 macOS; hosted toolchain qualification is separate.

Additional time-observation paths, including rejected appends and failed
configuration, physical clock/store failures and the remaining release gates are
still unqualified. Historical startup/shutdown causes remain unresolved; passing
workspace reruns do not establish their cause.

## 2026-10-15 subscription settlement races

Three real-node cases hold the original signed acknowledgment and extension
requests before forwarding them to the stream owner. Separate consumer cells
originate both requests; the destination still uses normal authorization,
transaction, ownership and proof gates. Signed operator status audits the
subscription before either old request can execute. Competing fetch IDs receive
LeaseBusy while each original/replacement lease is live.

Exercise three durable transitions while those requests are held: reset to the
same original offset under a new incarnation; actual expiry and a higher lease
generation for the same records; and acknowledgment followed by a lease for the
next event. Audit the replacement before release. Old reset receipts must return
SubscriptionReset, replaced receipts StaleReceipt, and an already committed ack
AlreadyAcknowledged. The old extension must never change replacement expiry;
checkpoint, identity and full lease must remain unchanged after both calls finish.
Forward the exact captured request bytes. Replaying the retained reset operation
must preserve the live replacement and later settled progress. The current lease
alone can advance the checkpoint, and both nodes/proxy must fully join while
caller-owned core executors remain usable.

Keep the ordinary 30-second lease and three-second operation budget. The expiry
case waits outside the calls until near expiry, captures both requests while the
old lease is still live, and crosses actual expiry within the original call
budget. Neither timeout nor domain closure can qualify stale-receipt refusal.
An absolute extension repeated before acknowledgment keeps the same expiry.
Reset/expiry redelivery preserves original encoded records and one effect in the
same durable projection inbox; next-event processing adds only that event's
effect. This does not promise global deduplication across separate databases or
external effects.

The initial three cases pass (53.77s). A targeted negative control bypasses only
the pure policy's extension receipt comparison: the next-lease case fails in
3.16s because the obsolete extension succeeds. Restore the production source
exactly before qualification; this intentional failure establishes guard
sensitivity, not a bug in the unchanged runtime. All three restored cases pass
(58.29s). The saved immutable test executable passes all 55 serial Node cases
(434.53s), with no other task-owned Rust builds/test suites running alongside it.
Workspace/all-target/all-feature check and Clippy with warnings denied pass,
as do seven artifact/three CLI tests, all 127 Python guards, formatting (510
Rust files), docs (36 documents/6 flows/386 references), dependency checks (403
packages/no JavaScript engine) and whitespace checks. Local checks use Rust
1.97.0/aarch64 macOS. The changes are native qualification and operator/SDK
guidance; all owned markers remain 1.

These are separate native domains/core executors with real peer RPC and a private
SQLite bucket. They do not qualify independent processes, clock rollback,
protected-history reclamation, physical faults or the full release matrix.
Next address durable clock/retention races, retaining the unresolved historical
startup/shutdown investigation and remaining proof, lifecycle, capacity and
operational work. The full design remains incomplete.

## 2026-10-15 delayed append replies race guarded operator retry

Two native cases now hold an actual append acceptance or actual Forbidden reply
while the same live source accepts a signed operator retry. The refusal comes
from durable publication-grant revocation; ordinary resource configuration
restores that grant before retry. There is no synthetic reply or proof adapter.
Source incarnation, lane and producer sequence remain the original identity.

The operator commits a higher dispatch generation while the original RPC is
still pending. Fixed-cardinality host observations confirm that phase, without
being used as receipts. After releasing the old response, require a fresh
submission blocked at the peer proxy: that barrier proves the old callback
finished and released its lane reservation. Socket-write completion alone is
insufficient. The whole old-callback cut must stay inside the source's unchanged
operation budget; a timeout or domain closure cannot qualify this boundary.

Signed inspection must find one pending row, next sequence two, settlement zero,
the newer generation and pending status without a stale blocked reason/error.
The old acceptance cannot settle it; the old refusal cannot overwrite retry.
Replaying the original operator receipt must leave the current revision unchanged.
Only then may the new request proceed, retaining the original append bytes and
event/envelope identity, producing one stream offset and one inbox effect despite
redelivery. Final progress must be next sequence two/settlement one; the retained
retry receipt must still replay without rearming. Both nodes return complete
drain reports and the proxy joins; their core executors are separate and caller-
owned.

Both cases pass on the unchanged dispatcher (13.58s). A targeted negative control
temporarily disables only its dispatch-generation comparison. Both tests fail
at the fresh-submission barrier: the accepted reply incorrectly settles the lane
to zero pending rows; the Forbidden reply incorrectly marks it blocked. The
negative runtime takes 13.85s. Production source bytes are restored exactly
before qualification, and both restored cases pass (11.24s). These intentional
mutation failures validate test sensitivity; they are not bugs claimed in the
unchanged implementation. The existing generation guard is essential and already
correct at these cuts.

The serial Node suite passes all 52 cases (365.04s). After reusing expected
canonical decimal strings to resolve comparison-allocation lint errors, the two
final focused cases pass again (10.11s) and workspace/all-target/all-feature
Clippy passes with warnings denied. The string checks are preserved; no lint is
suppressed. Workspace check, seven artifact/three CLI tests, all 127 Python
guards, formatting (509 Rust files), docs (36 documents/6 flows/386 references),
dependency checks (403 packages/no JavaScript engine) and whitespace checks pass.
Local validation uses Rust 1.97.0/aarch64 macOS; hosted toolchain qualification
is separate. Runtime/SDK behavior and all owned version-one markers remain
unchanged; changes are native qualification and operator documentation.
These are separate embedded nodes/execution domains
and core executors with a real loopback proxy/private SQLite bucket. They do not
qualify independent processes, physical host/store loss, delayed seals, other
source lifecycle transitions or the full release matrix.

Next qualify subscription expiry/reset/extend against acknowledgment and competing
consumers. Preserve the separate historical startup/shutdown cause audit and all
remaining lifecycle, capacity and operational gates. The full goal remains the
entire design.

## 2026-10-15 independent factory-entry observation

The historical startup/shutdown failures remain unresolved. The unchanged full
Node suite passes all 48 cases (353.06s). An immutable unchanged executable also
passes 100 isolated replays: 34 cancelled-bootstrap, 33 explicit-policy and 33
failed-predecessor recovery cases. These passing replays do not establish or
repair the earlier failure causes.

A separate deterministic fixture defect is now reproduced: the entry observer
uses `spawn_blocking` plus a blocking channel receive on the same host pool that
must execute the application factory. With one blocking worker, the observer
prevents factory entry, or can itself queue behind a factory awaiting its release.
Three before-fix runs fail at the original entry cutoff (5.32s/5.27s/5.30s),
reporting startup polled, directory held and startup still pending. The final
before-fix run uses the same new bounded-host test as the fixed source. This
constraint differs from the original fixture's default blocking-pool capacity;
it does not prove that starvation caused the historical failures.

The observer now awaits an async channel, leaving the factory's host blocking
worker available. The factory records its actual entry Instant. A five-second
entry cutoff is captured before startup, so a receiver delayed until after the
cutoff cannot accept a genuinely late entry merely because its message is queued.
Timely entry observed later remains valid. The original restart cutoff, directory
retention, factory join, node drain and caller-executor assertions are unchanged.
The new one-worker variant has a separate 30-second outer watchdog to terminate
an observer queued behind the held factory; neither original five-second limit
is raised. A queued-notification test checks both sides of the actual cutoff.

Both cancellation cases pass together (1.01s), the queued-deadline guard passes,
and ten repeated runs of both cancellation cases pass (20 cases). The final
unchanged-budget serial Node suite passes all 50 cases (359.22s), including
explicit-policy, predecessor recovery, real event delivery and complete drain.
Workspace/all-target/all-feature check and Clippy with warnings denied pass,
as do seven artifact/three CLI tests and all 127 Python guards. Formatting
checks 509 Rust files; docs validate 36 documents/6 flows/386 references; the
dependency check finds 403 packages with no JavaScript engine. Whitespace checks
pass. Local checks use Rust 1.97.0/aarch64 macOS. Only the native fixture and
documentation change; production runtime/SDK code and all owned markers remain
unchanged. The historical timeout, explicit-policy closure and predecessor-
shutdown expiry audits remain open.

Next qualify delayed delivery settlement against source-control generation
changes. Continue retaining the original startup failure diagnostics and resume
that cause investigation when new evidence distinguishes it from fixture
starvation. Physical host/store/power, unrestricted placement, lifecycle,
capacity and operational release gates remain incomplete. The full goal remains
the entire design.

## 2026-10-15 cold destination append recovery before automatic retry

`fleet_faults.py --follower-only --restart-followers` now extends the original
append cut to loss of every original process and recovery from retained follower
disks. As in ack/fetch, archive actual committed CLB1/CLE1 batches, exclude new
destination LTX/session-log bucket data before replacement, remove destination-
local data, retain peer disks and require expired leases/new PIDs/generations.

An unsettled source can otherwise repair missing acceptance automatically, even
without an ordinary source command replay. A returning peer that acquires the
stream could append locally and bypass the network gate. The private native
fixture therefore supports `blocked_claim`: while `claim-block` exists, refuse
only puts to the exact selected stream ownership object. Reads, other ownership
and node lease writes, and actual retained follower responses remain unchanged.
The wrapper retains the original no-retry conditional client and supplies no
proof. A real refused claim must be observed before the destination starts.

The replacement destination claims/recovers through the ordinary path. Both
peer replacements must be ready and an identical automatic append actually
blocked before the original accepted event/envelope, source receipt/pending
obligation, empty inbox and checkpoint zero are audited. Only then may the held
reply be dropped and retries resumed. Source ownership may move between returning
peers; neither can acquire the selected stream during measurement. Offline audit
rechecks the exact private configurations/markers and original follower archives.
The same accepted event must settle and produce one effect/checkpoint with full
process/proxy/store joins. This is a selected placement-controlled recovery cut,
not qualification of unrestricted placement or missing witness disks.

The initial SQLite run passed its state/effect audits but failed proxy cleanup:
the killed source had already disconnected its held-response socket, so shutdown
returned ENOTCONN. Cleanup then skipped other proxy listeners. The proxy now
accepts only that already-disconnected outcome at the explicit drop call; other
errors remain failures. Cleanup attempts every remaining proxy close before
propagating failure. A real TCP-reset regression fails against the prior proxy
with the same error and passes with the fix. The failed trace is retained.

Final placement-gated cold append runs pass on SQLite (4.25s) and private pinned
RustFS S3 (6.78s), including actual refused claims and blocked retries. The new
immutable executable SHA-256 is
`c8c01f76512b150a31f46c99a75aa2b42c81a5e1d54a5cf0700ccd76472c3bb6`.
Ordinary smoke/live-follower append, cold ack, private S3 cold live-fetch and
live-owner before-commit cases pass with that same executable. All 127 Python
guards pass. The native claim-client test verifies exact-key refusal, unchanged
bytes/read access, other ownership/lease writes, release and stale-CAS rejection.
Workspace/all-target/all-feature check and Clippy with warnings denied plus
seven artifact/three CLI tests pass. Core runtime/SDK code and owned markers
remain unchanged; only private example Rust and qualification tools change.
All six saved recovery ledgers pass offline audit. Formatting (509 Rust files),
documentation (36 documents/6 flows/386 references), dependency checks (403
packages/no JavaScript engine) and whitespace checks pass. Local checks use
Rust 1.97.0/aarch64 macOS; hosted toolchain qualification remains separate.

Next address the observed startup/shutdown failures and remaining stale
settlement/reply interleavings. Unrestricted/local-owner placement, physical
host/store/power, missing-disk, lifecycle, capacity and operational gates remain
open. Passing these selected cases does not explain earlier intermittent
bootstrap/recovery failures. The full goal remains incomplete.

## 2026-10-15 cold destination acknowledgment and fetch recovery

The ack/fetch process drivers now accept `--follower-only --restart-followers`.
All original processes are SIGKILLed after an actual proven destination reply
and pre-loss audit. The original destination ensemble's committed CLB1/CLE1
batches are archived with checksum/range inspection; stream LTX and original
destination-session log bucket objects must remain unchanged with all original
memory gone. Destination-local data is deleted. Peer disks are preserved and
rechecked before their independent replacement processes start.

`destination_disks.py` requires old lease expiry, distinct process IDs and new
peer node generations. Every replacement starts before readiness waits, since
recovery can need another peer's still-activating listener. A saved recovery
prefix must precede request replay. Ack recovers checkpoint one, no lease and
the original inbox receipt before AlreadyAcknowledged replay. Live fetch retains
the original lease/response; expired fetch keeps its original 30-second node/
stream timing and obtains higher lease authority before stable replay. Both
fetches apply one effect and acknowledge only the saved receipt. Complete
survivor/proxy/store joins and offline archive-byte reinspection remain required.

An initial S3 live-fetch run recovered the exact lease and one effect but failed
the evidence gate because ownership moved to the restarted producer, bypassing
the destination-only measuring proxy. This was a missing harness observation,
not an observed lost event or duplicate effect. The harness now places paused
method-specific proxies on both returning peers' advertised routes too. The
assertion and exact-byte comparison remain unchanged. A final S3 ack run owned
by the returning producer exercises that route and passes. The failed trace is
retained; passing reruns do not explain unrelated historical failures.

All six final cold-disk cases pass: ack/live-fetch/expired-fetch on SQLite
(4.52s/4.48s/33.27s) and private pinned RustFS S3 (6.02s/5.95s/34.84s). They use
the unchanged immutable executable SHA-256
`931d05edf72aff8740efb1db608c6b17ef7147a4a96663378190260c8082fcfd`.
The 124 Python guards include missing/reordered custody, stale/reused process
authority, changed witness files/epochs, bucket repair, early replay and archive
mutation. Unit custody fixtures are synthetic; only process runs supply actual
disk/state evidence. Core runtime/SDK Rust code and owned markers are unchanged.
Ordinary ack, live-fetch and live-follower append cases pass too; all nine saved
ledgers pass offline audit. Formatting (509 Rust files), documentation (36
documents/6 flows/386 references), dependency checks (403 packages/no JavaScript
engine) and whitespace checks pass. No new Rust build/test claims are made for
this Python/documentation change; the unchanged executable comes from the prior
Rust 1.97.0/aarch64 macOS qualification.

These are selected cold retained-disk destination ack/fetch cuts among processes
on one machine. Cold pending-append recovery is next: its unsettled source can
otherwise repair missing acceptance automatically, so it needs an independent
pre-replay gate. Missing follower disks, consumer-local replay, other stale
settlement/reply interleavings and physical host/store/power, startup-cause,
lifecycle, capacity and operational gates remain open. The full goal remains
incomplete.

## 2026-10-15 independent-process live source takeover

`live_owner.py` extends the embedded running-poll case to four independent
processes on one machine. A private producer-only `owner_fault` configuration
wraps its real ordinary, conditional and listing clients. The conditional wrapper
keeps the original no-retry client; the small private listing caps its materialized
inventory at 1000 objects. An isolation marker causes actual store calls to fail
and records bounded operation/path evidence. It never supplies durability credit
or changes other nodes' clients. All leader writes use explicit bucket proof.

The original order/receipt is accepted and settled before a different stale probe
is submitted. The callback publishes an entered observation and stays inside its
native poll, either before starting a transaction or after uncommitted business/
outbox writes. The external measuring client records the original/probe intent,
actual refused lease operations, current lease expiry and a live/non-stopped old
PID. A different successor must recover the original source under a higher epoch
and pass a saved source/event/inbox/checkpoint audit before callback release.
No SIGSTOP, source-command replay or synthetic proof repairs that prefix.

After release within the unchanged 10-second handler budget, the old callback
must record closed-domain storage/commit refusal even though it then attempts a
success-shaped response. The original request must terminate without success,
and the old process must join with `NodeLeaseExpired` and no graceful-stop report.
The joined old source's DB/WAL family is archived and read only through disposable
copies: original business value/receipt, lane, next sequence 2, settlement 1 and
zero pending events must remain. A second recovered-prefix audit precedes one
consumer effect/checkpoint and complete preserving survivor/store cleanup.
Complete offline audit rechecks the forensic archive's bytes.

All four final cases pass: before-tx/before-commit on SQLite (2.91s/2.79s) and
private pinned RustFS S3 (4.74s/4.40s). Ordinary smoke, resumed-owner and source
follower-only cases also pass. The immutable final executable SHA-256 is
`931d05edf72aff8740efb1db608c6b17ef7147a4a96663378190260c8082fcfd`.
All 117 Python guards pass, including missing/reordered stages, changed probe
intent, paused/dead/reused PIDs, stale authority/policy, false refusal, stale
commits, source replay/prefix loss, archive mutation and incomplete cleanup.
Workspace/all-target/all-feature check and Clippy with warnings denied, seven
artifact tests, three CLI tests, formatting (509 Rust files), dependency checks
(403 packages/no JavaScript engine), documentation validation (36 documents,
6 flows, 386 references) and whitespace checks pass. Checks use Rust 1.97.0/aarch64 macOS; the
initial adapter lifetime compile error was a fixture setup issue, not a runtime
failure. Core runtime/SDK code and owned markers remain unchanged. Earlier library,
Node and other process qualifications are not superseded by these selected runs.

This qualifies the two independent-process source callback cuts under an injected
node-client outage. Destination follower-disk recovery is next, followed by other
stale settlement/reply interleavings and the existing physical-host/store/power,
startup-cause, lifecycle, capacity and operational gates. The full goal remains
incomplete.

## 2026-10-15 an executing source poll spans fenced takeover

The next native fixture holds a real source callback within a synchronous poll
at two cuts: after retaining storage/stream handles but before a transaction,
and after writing business state plus an outbox event but before commit. It
isolates only the old node's object-store client; the observer and successor
retain healthy independent clients. Actual refused store calls and old-domain
closure must occur while the callback remains held. No SIGSTOP, manual domain
closure, synthetic proof or source-command replay supplies the boundary.

Before releasing that callback, a distinct node/domain/core executor must recover
the source by read, install a higher ownership epoch and expose the original
business value, producer incarnation/revision/progress and exact accepted event.
The second cut specifically keeps an uncommitted event present in the old
connection throughout recovery; it must not appear in the successor's prefix.
After release, the original retained handle or commit must refuse with closed-
domain authority. Even though application code catches that error and returns
a success-shaped response, the original HTTP request must not succeed.

The old node must return `NodeLeaseExpired` after joining, then a second audit
must find the same recovered state and accepted envelope. Only afterward may a
consumer process the original event, producing one inbox effect. Successor drain
and continued caller-executor operation are required. The focused two-cut case
passes (10.07s). The final case adds explicit original event/value/sequence checks
and confirms the callback has not ended before the successor audit. All 48 native
Node tests pass (99.41s), as do seven artifact tests, three CLI tests, 111 Python
guards, formatting (508 Rust files), docs (36 documents/6 flows/386 references),
dependency checks (403 packages/no JavaScript engine) and whitespace checks.
Workspace/all-target/all-feature check and Clippy with warnings denied also
pass. Initial test-helper compile errors were fixture setup errors, not
production failures. No production Rust or owned marker changes are made;
earlier library, SDK and independent-process suites are not rerun or superseded
by this fixture-only slice. Checks use Rust 1.97.0/aarch64 macOS rather than
claiming CI's configured toolchain.

This extends PR #52's local storage regression to actual embedded Node routing,
automatic expiry/fencing and takeover while the callback is still entered. It
uses a private SQLite bucket on one machine, independent execution domains and
core executors, and an injected client-specific outage. Independent-process
isolation, physical hosts/power/store faults, other stale settlement/reply cuts,
destination follower-disk recovery and lifecycle/capacity gates remain open.
The full goal is still incomplete.

## 2026-10-15 retained storage admission inside an executing turn

The stale-owner audit found a concrete SDK admission gap. `Context::storage`
checked domain admission when creating a handle, but retained `Storage` handles
only checked activation/source identity. A callback already executing while its
domain closes could continue synchronous access and commit an open transaction.
An installed original I/O context also let a retained handle write while another
execution domain was temporarily entered.

Two focused native regressions fail before the fix: the wrong-domain write
returns success, and domain closure after business/outbox writes still permits
commit (0.02s). These are actual storage-boundary failures, not fixture decoding
or bootstrap errors. Storage now independently compares the installed I/O
context's services with the current domain. Ordinary access and commit additionally
require open-domain admission. Cleanup retains original activation/connection
identity without requiring open admission, so rollback can undo the unfinished
transaction after closure. No new business authority is granted by cleanup.

Both regressions pass after the fix (0.03s): retained KV/SQL/cursor access and new
transactions refuse after closure; the unfinished transaction rolls back both
business state and its second event, retaining the earlier event, producer next
sequence and settlement watermark. Owned markers remain one, with no
compatibility path or schema change.

Broader validation passes all 229 library tests, 47 native Node tests (100.46s),
seven artifact tests, three CLI tests and the native event-stream, outbox, SQL,
storage and transaction suites. Workspace/all-target/all-feature check and
Clippy with warnings denied pass, as do formatting (507 Rust files), 111 Python
guards, docs (36 documents/6 flows/386 references), the dependency audit
(403 packages/no JavaScript engine) and whitespace checks. The SDK guides, API
map, example and generated asynchronous flow/2x preview describe the admission
boundary; the preview was rendered and inspected.

A rebuilt executable, archived with SHA-256
`617bfe9ac441dfeb07fdf3d96e3152f4358a0848002f395380dc5e8aab11f165`,
passes eleven process runs: ordinary stream smoke, resumed-owner fencing and
follower-only append/ack/live-fetch/expired-fetch on both SQLite and private
RustFS S3. All ten saved fault ledgers pass offline audit. The checks use
Rust 1.97.0 on aarch64 macOS; CI's configured toolchain is not claimed as locally
exercised. These passing reruns do not establish the causes of earlier intermittent
startup failures or close their separate gates.

This closes the reproduced local SDK gap, not the full live stale-owner process
matrix. An SQLite statement already admitted before concurrent closure and
output/durability gates retain their existing semantics; this change does not
claim preemption or rollback of a previously committed result. Qualifying an old
process still executing across takeover remains the highest correctness work,
followed by destination follower-disk and other commit/proof/lifecycle/capacity
cuts. The full goal remains incomplete.

## 2026-10-15 follower-only subscription fetch and ack recovery

`ack_faults.py --follower-only` and `fetch_faults.py --follower-only` share the
destination proof gate from the append case, with operation-specific held replies,
restart identities and recovery audits. The original destination uses real fleet
durability; producer/consumer leader writes explicitly use bucket proof. Arming
follows the accepted-event audit and precedes the target fetch/ack. Actual stream
upload refusal, the installed fleet-ack marker and unchanged stream LTX plus
original-session log bucket bytes are required before loss and restart. The
destination is SIGKILLed, its private local data deleted, and its actual old node
lease observed expired before replacement starts. Followers retain their disks.

Ack audits checkpoint one, no active lease, one proven inbox effect and the
saved receipt digest before replaying only that exact receipt. The result must
be `AlreadyAcknowledged` without another fetch, effect or checkpoint advance.
Fetch first observes an ambiguous original response with no effect. Recovery
must audit the original event and checkpoint zero before another fetch. Live
mode retains the exact original lease/response; expired mode observes expiry
and obtains a higher lease generation, then repeats that fresh receipt/response
unchanged. Two successful fetch replays produce exactly one inbox effect; only
the saved receipt is acknowledged. Original stream lease and transport budgets
are unchanged. Fetch proof policy is checked against its recorded live/expired
node-authority policy instead of inferring timing from elapsed wall time.

The shared proof auditor runs before dependent operations and in complete saved
evidence. Mutation guards reject LTX or node-log bucket bypass, missing stages,
false proof, retained local data, stale ownership/policy and replay before proof
recovery. Method-specific guards continue to require exact wire/receipt bytes,
subscription identity and lease/checkpoint/effect invariants. All survivors must
join with complete preserving shutdown.

All six selected follower-only runs pass: ack, live fetch and expired fetch on
both private SQLite and pinned RustFS S3. The eight new saved ledgers, including
ordinary SQLite ack/live-fetch regressions, pass offline audit. The shared proof
auditor also accepts the prior SQLite/S3 append evidence. All 111 Python guards,
formatting (506 Rust files), docs (36 documents/6 flows/386 references), dependency
audit (403 packages/no JavaScript engine) and whitespace checks pass. Runtime
cases use the unchanged current executable SHA-256
`44d5806b33ca9b3d82231a53596535878565b36b68985a21ff0ff9e3524ce9e2`,
built with Rust 1.97.0 on aarch64 macOS. No production Rust or Cellar-owned version
markers change, and this qualification slice does not rerun or supersede earlier
Rust suites. The ordinary expired-fetch process case is not rerun here; its
existing oracle guards remain in the Python suite.

This remains selected process-loss qualification on one machine with live
retained followers. It does not qualify follower disk destruction, physical
hosts, power/store loss, live stale-owner execution or every commit/proof cut.
The full implementation/release goal remains incomplete.

## 2026-10-15 follower-only destination append recovery

`fleet_faults.py --follower-only` selects real fleet durability for the original
destination and explicit bucket proofs for producer/consumer leader writes.
It waits for an installed destination follower ensemble, arms the existing
replica-publication refusal, and measures destination LTX plus the original
session's log objects before the command. A complete successful append reply is
held while an independent audit verifies the original event and source's pending
obligation. The proof gate requires a refused stream LTX upload, the fleet-ack
marker and unchanged destination/session bucket bytes.

After SIGKILL, the harness inventories and deletes only the destination's private
data, observes actual old lease expiry and checks unchanged bucket data again.
It keeps the original source RPC waiting through replacement startup and a saved
audit of the original accepted envelope, pending source, empty inbox and checkpoint
zero. Only then is the held reply dropped and retry released. The exact append
request/response bytes must repeat; one accepted offset, one inbox effect and
checkpoint one survive the retry/redelivery. Every surviving process joins with
complete preserving shutdown. The separate proof auditor runs before each
dependent boundary as well as during the full saved-ledger audit.

The first measuring run failed because dropping the original response before
recovery allowed a retry to take a new route after authority expiry and settle
the source before audit. That run does not qualify recovery and is retained as
a harness failure, not a production loss bug. Holding the original response
through recovery closes that measuring race without changing runtime policies,
timeouts or assertions. An oracle mutation now rejects early response loss.

Final follower-only runs pass on SQLite and private RustFS S3; ordinary SQLite
append-loss also passes. All use the existing current executable with SHA-256
`44d5806b33ca9b3d82231a53596535878565b36b68985a21ff0ff9e3524ce9e2`.
The 106 Python guards pass, including refusal of bucket bypass through LTX or
node-log objects, missing proof stages, retained local data, stale authority and
early response loss. Production Rust and owned markers remain unchanged.
Formatting (506 Rust files), docs (36 documents/6 flows/386 references), dependency
audit (403 packages/no JavaScript engine) and whitespace checks also pass. The
current runtime binary's earlier Rust suites remain separate evidence; this
Python/documentation slice does not rerun or supersede them.

This qualifies one destination append boundary with live retained followers on
one machine, not follower disk loss, independent physical hosts, power/store
loss or every commit/proof cut. Destination follower-only fetch/ack and live
stale-owner execution are next, with unexplained startup and lifecycle/capacity
gates still open. The full goal remains incomplete.

## 2026-10-15 returning proof after a recorded recovery refusal

The selected acceptance gate is recovery availability when a valid follower
witness returns after the predecessor has already recorded missing proof.
Refusal must preserve the recovering predecessor, and a later retry must use
current evidence rather than treat the earlier loss diagnostic as authority.

A native log fixture commits a real fleet-proven SQLite write, stops both
original execution domains, deletes the original leader's local directory and
checks that neither cell LTX nor session log data exists in the bucket. Recovery
first contacts a real follower store opened on an empty directory. It must
refuse with no complete witness, preserve the old generation in recovering state,
record a blocked diagnostic and expose no restored cell. The fixture then routes
to a newly opened follower store on the retained disk, checks the exact original
range and entry bytes, and retries recovery twice. The restored SQL state must
contain exactly the single original row without command replay; the predecessor
must be sealed and the earlier diagnostic unchanged.

The focused case passes (2.81s), followed by all six native log tests (1.33s)
and four protocol tests (0.69s). Workspace/all-target/all-feature Clippy with
warnings denied passes (41.76s), as do formatting (506 Rust files), 101 Python
oracle guards, docs (36 documents/6 flows/386 references), dependency checks
(403 packages/no JavaScript engine) and whitespace checks. This test-only slice
changes no production Rust or version markers; it does not rerun or supersede
the preceding full Node and process qualifications. Checks use Rust 1.97.0 on
aarch64 macOS, not CI's configured toolchain.

This fixture uses independent native execution domains and actual follower
handlers through the current log wire. It is not independent-process, physical
network, host or power-loss qualification, and does not close the destination
append/fetch/ack or stale-owner gates. Those remain the highest correctness
priorities, followed by the unexplained startup failures and lifecycle/capacity
cuts in the ordered list above.

## 2026-10-15 current log wire and follower layout at version one

The remaining owned ranged-tail marker is now one in both JSON requests
(`tail_format`) and binary responses (`CLT1`). The range's fragment epoch, base,
end and completeness fields are unchanged. Epochs and sequence counters remain
runtime values, including values greater than one. Unsupported markers and
missing declarations are refused; there is no entries-only fallback or format
negotiation. Every participant must be rebuilt together.

Follower session discovery no longer checks the former flat node/state.json
layout. Native load/persist entry points also require a valid node/lineage/generation
identity before reading cached or on-disk state, so direct SDK calls cannot
re-enable that layout or write it. Existing current-layout disk bytes are
unchanged; former flat-layout development state must be recreated. No layout
migration or compatibility reader is added.
The same audit found old individual-entry and unchecksummed batch decoders.
Those readers and their cleanup branches are removed too. Current loading/tails
reject `.entry`/`.entries` files without reading their payload; only checksum-
protected committed `.batch` files supply entries. The existing missing-directory
and checksum/range completeness refusals remain in place. Unsupported development
files are left for inspection/recreation instead of silently migrating them.

Protocol coverage asserts the encoder's JSON marker and binary magic as well as
decoder acceptance/refusal, keeps nontrivial epoch/range counters, and retains
torn-frame checks. A native follower-GC case exercises current-generation
discovery, keeps an active fragment unsealed, seals only a closed fragment,
rejects native access to flat sessions and old entry layouts, and leaves
unsupported files unchanged. A
targeted scan found no other non-one owned version markers in
the inspected core; application event schema versions and external codec versions
are not normalized by this policy.

Validation passes all 227 library tests, 47 native node tests (328.86s), five
native log tests, four protocol tests including current-only GC/access, seven
artifact tests and three CLI tests. The 101 Python oracle guards, format check
(505 Rust files), docs (36 documents/6 flows/386 references), dependency audit
(403 packages/no JavaScript engine), whitespace and workspace/all-target/all-
feature Clippy with warnings denied also pass. Real source follower-disk restart
passes on SQLite/S3 (3.43s/5.93s), live-follower recovery passes on both backends
(2.71s/5.72s), and ordinary streams smoke passes (1.58s), all on the same current
executable. The final comment-only rebuild is byte-identical to the tested
binary. These checks use Rust 1.97.0/aarch64 macOS; CI's configured toolchain is
not claimed as locally exercised.

The full outbox goal remains incomplete. The next correctness work returns to
restoration of valid proof after refusal and destination follower-only append/
fetch/ack cuts, followed by the unresolved startup, lifecycle, capacity and
operational gates. This hard cutover does not establish mixed-build support or
close any unqualified failure boundary.

## 2026-10-15 refuse recovery after durability proof loss

The prior all-fleet S3 failure is not evidence that arbitrary disk loss must be
recoverable. Fleet proof certifies follower fsync or bucket data; a surviving
leader's local SQLite file does not certify a complete node-log prefix. Eventual
delivery already requires recoverable storage. When a required witness is
unavailable, refusing startup is safe. Restoring valid evidence can permit a
later retry; destroying every authoritative copy cannot be hidden as successful
recovery.

Inspection found two unsafe continuations beyond that initial refusal. First,
after all members were considered conclusive (including a long-expired,
unreachable member), recovery wrote a loss record and proceeded to drain/seal
the log despite possible fleet acknowledgments. A valid native startup
regression reproduced successor admission on that path. Second, an unreadable
retained bundle was recorded and skipped, so the uncovered-row scan could
answer as if no outstanding bytes remained. A diagnostic is not durability
evidence and cannot authorize either continuation.

Recovery now retains the recovering predecessor and refuses to install a
successor whenever a log may have fleet acknowledgments but no complete witness.
The empty-ensemble/ack-marker case also refuses. Conclusive failures retain a
`recovery_blocked` diagnostic; inconclusive failures retain their existing
refusal. Neither expiry grace nor a diagnostic grants permission to abandon
acknowledged writes. Unreadable retained bundles record diagnostics and refuse
every scan, including after that diagnostic is cached. The record remains a
timestamped observation, while the current log state remains authoritative.

The original predecessor-recovery fixture lacked the required paced_handoff
lease field and could fail at decoding while claiming to cover witness refusal.
It now supplies the current lease shape and asserts the actual witness reason.
The new startup case covers long-dead members and an empty ensemble, preserves
the prior generation/recovering state, and checks that failed startup releases
the local directory. The bundle regression covers repeated scans after a
diagnostic exists. This change adds no version marker, compatibility reader or
local-SQLite fallback.

The valid pre-fix startup regression admitted a successor (failed in 1.62s),
and the pre-fix corrupt-bundle regression accepted both empty folds (failed in
0.03s). Those are the behavioral reproductions; the initial malformed lease and
fixture compile failures are not counted as proof. With the fix, all 47 native
node cases pass (349.35s), including both missing-proof inputs and the strengthened
predecessor refusal. All five native log cases pass (2.37s), including the repeated
corrupt scan and real successful follower recovery. Library tests pass 227 cases,
native artifacts/CLI/log protocol pass 7/3/3 cases, and Python guards pass 101.
Workspace/all-target/all-feature Clippy passes with warnings denied (3m47s),
format checks 505 Rust files, docs validate 36 documents/6 flows/386 references
and dependency audit checks 403 packages without a JavaScript engine.

A freshly built executable also passes follower-disk restart on SQLite/S3
(3.10s/5.66s), live-follower recovery on SQLite/S3 (3.03s/5.39s), and ordinary
streams smoke (1.11s). These retain exact receipt/envelope checks, pre-replay
audits, expected inbox/checkpoint results and complete process/server shutdown.
The restart cases explicitly keep destination/consumer leader commits bucket-
backed while the source uses follower-only proof; they are not successful recovery
claims for destroyed witnesses. Rust 1.97.0/aarch64 macOS and symbols-disabled,
nonincremental profiles were used; CI's configured toolchain is not claimed as
locally exercised.

This prevents false successful recovery. It does not recreate destroyed proof,
retroactively repair development logs already sealed after loss, establish
physical-disk/power tolerance, or close the older unexplained startup/recovery
failures. Wider destination append/fetch/ack proof cuts, safe restoration of
missing evidence and remaining lifecycle/capacity/operational gates stay open.
An additional pre-existing internal-development policy gap was found during this
audit: the owned tail wire still uses tail_format 2 / CLT2, and follower session
discovery retains a former flat-layout branch. Normalize those with a hard
cutover to marker one and current-layout-only readers; do not add compatibility
negotiation. This safety fix does not claim that broader audit is complete.

## 2026-10-15 recover source proof after follower restart

The source-proof driver adds an explicit `--restart-followers` mode. It SIGKILLs
every original process, archives the producer's original ensemble's committed
follower batches and independently rechecks unchanged source bucket data while
no original process survives. It then archives/deletes leader-local data,
observes each original node's persisted authority expiry and starts all
replacements before waiting for activation. Each follower must retain the same
files but register a new PID/node generation. The actual new source owner may be
a restarted follower; direct owner metadata must show a higher epoch and match
the serving source. Original receipts, pending obligation and accepted envelope
bytes must survive before any consumer effect or acknowledgment. No source POST
is replayed. Complete audits recheck the source and follower archives.

The first oracle incorrectly required a positive state.json watermark and
rejected actual successful SQLite recovery. Inspection of the immutable killed
disk showed the seed at zero and committed batches spanning later sequences.
The measuring code now decodes the current CLB1/CLE1 batch format independently,
checks full SHA-256 footers, entry/header/filename ranges, and identifies original
source/ownership-epoch frames. This follows the runtime's documented batch commit
point; it does not relax source identity or durability requirements. The original
failed trace remains retained.

The broader all-fleet S3 case found a separate open recovery gap. Destination's
persisted log had `may_have_fleet_acks=true`, `tiered=0`, and sole ensemble member
producer. Deleting producer-local data also deleted that destination session's
only follower batches. Destination restart refused with `no complete true
witness` and consumer/successor event activation failed. No successful recovery
is claimed for that trace. The implementation currently fails closed; whether
and how to recover safely from the surviving former leader's local data, or
prevent this availability loss through proof policy, requires further work.

An explicit `--peer-proof bucket` case isolates source follower restart by keeping
destination/consumer leader commits bucket-backed. Source proof remains fleet
only, with real refused replica uploads and original source LTX still absent from
the bucket after all SIGKILLs. Both SQLite and private S3 runs recover the original
source after restarting followers from retained disk, then apply exactly two
effects and checkpoint two. This is additional source-proof evidence; it does
not replace or close the failing all-fleet case. Defaults retain fleet proof on
peers; every saved run records its actual peer policy.
The bucket inventory includes both cell LTX and producer-session node-log
bundle/recovered objects. Either new object or changed bytes reject a
follower-only claim, including while every original process is dead. The live-
follower regression also passes on both backends under this stronger inventory.

Final source-only restart cases pass on SQLite (3.30s) and private RustFS S3
(6.21s), with the acknowledging destination process restarted and then serving
the original pending source under epoch two. All original processes are dead
at the bucket/data archive boundary; producer-session bucket bundle count is
zero. Live-follower regressions pass on SQLite/S3 (2.68s/5.24s). Oracle guards
pass 101 cases; native artifact/CLI suites pass 7/3 cases. Workspace/all-target/
all-feature Clippy passes with warnings denied (1m30s including a package-cache
lock wait), formatting checks 505 Rust files, docs validate 36 documents/6 flows/
386 references and dependency audit checks 403 packages with no JavaScript
engine. Rust example source hashes and the reused executable's SHA256 are
verified unchanged from the preceding qualified build; this turn changes only
the Python measuring/oracle code and documentation.

Next correctness work starts with the combined lost-witness recovery boundary,
then destination follower-only append/fetch/ack cuts. Physical disk destruction,
power/host loss, the earlier unexplained startup/recovery intermittents and the
remaining lifecycle/capacity/operational gates remain open. All owned markers
stay one; this turn adds no runtime schema/protocol reader or migration.

## 2026-10-15 follower-only source acceptance

The selected risk is an acknowledged source state/outbox transaction existing
only on the fleet log when the leader loses all local state. The new
`follower_faults.py` uses real independent fleet processes and an opt-in replica
ObjectStore decorator. Only producer replica put and multipart initiation are
refused; ownership/catalog/discovery clients remain healthy and no adapter can
mint durability credit. Required retention keeps the second command's outbox
row pending behind the first event.

Acceptance requires an original successful source response, independently
unchanged source LTX bucket bytes across that command, a real refused source
upload, an installed fleet ensemble/ack marker and the original receipt plus
pending row before SIGKILL. The measuring client inventories/deletes only the
killed producer's local directory after archiving its stable pending database/WAL,
observes actual old node-authority expiry and
starts a distinct successor. Reads must recover the same receipt, source
incarnation/sequence and obligation under a higher epoch before any consumer
effect or acknowledgment. No source POST replay is permitted. Consumption then
must preserve the archived pending envelope digest and apply exactly two inbox
effects and checkpoint two; all survivors and the
private server must join completely. Saved prefix audits gate further work and
the complete offline oracle rejects bucket bypass, premature replay, missing
local-data loss, live authority takeover or incomplete shutdown.

The initial SQLite run reached successful follower-only acceptance and audited
the pending source before SIGKILL, but its first recovery read hit the still-live
killed producer route and received ConnectionRefused. The measuring driver now
observes actual persisted node-lease expiry before starting takeover; it does
not swallow the error, lengthen production deadlines or replay the command.
The original failed ledger is retained separately from passing evidence.

The source-proof case passes on both SQLite and the private Docker RustFS S3
backend, with four saved audits each and exact pending-envelope recovery.
Shared measuring-client changes also pass both resumed-owner backend cases and
the ordinary streams smoke test. The Python oracle suite passes 93 cases;
workspace/all-target/all-feature Clippy passes with warnings denied. Formatting
checks 505 Rust files, native artifact/CLI suites pass 7/3 cases, docs validate
36 documents/6 flows/386 references, and the dependency audit checks 403 reachable
packages with no JavaScript engine. Verification uses Rust 1.97.0 on aarch64
macOS with symbols-disabled, nonincremental profiles; CI's configured toolchain
is not claimed as locally exercised. A style lint in the new fixture constructor
was corrected before the passing Clippy run. Refusal-journal writes serialize on
the owning blocking executor so concurrent source uploads cannot interleave
saved JSON evidence.

This is selected source acceptance/local-leader-data-loss evidence. It does not
qualify follower disk loss, destination append/fetch/ack follower-only boundaries,
physical host/power loss, every stale owner interleaving, earlier unexplained
startup/recovery failures or the full capacity/lifecycle matrix. Owned markers
stay at one, with no compatibility layer.
The next source-proof gate restarts the acknowledging follower process from its
disk while leader-local data is absent; then expand destination follower-only
cuts. An ensemble that remains live cannot by itself qualify follower restart
or physical disk persistence.

## 2026-10-15 resume an expired source owner

The independent process harness now pauses the producer at its real input gate
after source proof and before first dispatch. It observes SIGSTOP via waitpid,
archives the stable source family, reads actual authority expiry from the private
bucket, and starts a distinct successor. Before SIGCONT, direct owner metadata
must show a higher epoch and an actual healthy owner; that owner must settle the
original command/event while the consumer still has no effect/checkpoint.

The original process resumes within its unchanged 10-second invocation budget,
then must emit the specific NodeLeaseExpired fencing report, exit with failure,
return no successful original response, and emit no graceful-stop report. Its
old source database must retain the original receipt, pending row and zero
attempts/settlement. A second successor audit precedes any command retry or
consumer effect. Command retry is duplicate with the same receipt; consumption
adds one effect and checkpoint one. Survivors and the private S3 server join.

The initial actual SQLite run reached all behavior gates but failed archive
revalidation: opening a WAL database read-only had created an empty WAL/SHM
family in the saved image. A focused regression reproduced that mutation before
the fix. New forensic inspection queries disposable copies and never opens the
measured archive itself. Failed original evidence is retained rather than edited.
Final shared-helper SQLite and private RustFS S3 runs pass (3.30s/8.01s);
the existing before-first-send SIGKILL regression passes (1.63s), and ordinary
smoke passes (1.50s). Both show genuine NodeLeaseExpired after resume and unchanged source
semantics, with independent higher-epoch ownership and event digests. Python
guards pass 83 cases. Workspace/all-target/all-feature Clippy passes with
warnings denied (22.67s); formatting checks 504 Rust files, docs validate 36
documents/6 flows/386 references, and dependency audit checks 403 packages
without a JavaScript engine. Whitespace checks pass. Verification uses Rust
1.97.0/aarch64 macOS with symbols-disabled, nonincremental profiles.

This is bucket-only proof with explicit 1000ms node authority. It qualifies a
paused owner resumed with a held invocation before first dispatch, not every
in-flight stale SQL/write/reply cut, physical partition, power/store loss or
follower-only proof. Existing handler/stream budgets and owned version-one
markers remain unchanged. Wider release qualification and the unexplained
startup/recovery failures remain open.

## 2026-10-15 independent-process fetch loss

The existing separate-process harness is extended for __streamNext, with an
opt-in example route using next_with_id and proven inbox/saved receipt writes.
Before destination SIGKILL, it saves the original accepted event, checkpoint zero
and live lease. The consumer must observe Ambiguous and zero effects. Recovery
is audited before proxy release and every subsequent retry; two successful
fetches must apply only one effect, then only the saved receipt advances the
checkpoint. Offline audits require the complete kill/restart/replay/shutdown
chain, while runtime prefix audits validate actual state before marking retry
safe. Failed audits cannot authorize another attempt.

The first actual run was rejected: 30.7-second destination recovery outlasted
the original 30-second stream lease. The fixture's original node-authority TTL
was also 30 seconds. That trace is not live-lease recovery evidence and is not
repaired by replay. Two explicit modes now retain that stream lifetime: live
mode records a 1-second node lease and requires exact original wire/lease
recovery; expired mode retains the 30-second node lease and requires observed
expiry plus fresh receipt generation before stable replay. The optional bounded
fixture authority setting leaves existing harness policies unchanged.

Current Python guards pass 75 cases, including both modes, early replay, changed
bytes, changed/expired authority, missing/foreign SIGKILL, duplicate effects,
missing saved receipt and incomplete/mismatched store cleanup. Formatting checks
504 Rust files. Both live/expired fetch modes pass on SQLite (3.71s/32.64s)
and private RustFS S3 (7.30s/34.07s) with the same archived executable. Each
records an observed destination SIGKILL, replacement PID, original payload/event
receipts, two exact fetch replay responses, one inbox effect, saved-receipt ack
and complete final shutdown. The S3 prefix validator explicitly permits only
CreateBucket 503 retries before store/application startup, with a matching
started/removed server identity; that event cannot hide application failures.
The existing independent-process ack-loss harness passes (32.72s), and ordinary
example smoke passes (1.33s). Workspace/all-target/all-feature Clippy passes with
warnings denied (2m 24s); native dependency audit checks 403 packages without a
JavaScript engine; docs validate 36 documents/6 flows/386 references; whitespace
checks pass. A final negative guard rejects disappearance of any previously
proven consumer effect or saved receipt before retry, preventing replay from
repairing the measurement. All four actual fetch ledgers pass that final oracle.
Local Rust 1.97.0/aarch64 macOS uses symbols-disabled, nonincremental profiles.
These loopback process cuts do not qualify power loss, follower-only proof,
object-store persistence loss, physical hosts or concurrently executing owners.
All owned markers remain one; no compatibility readers or migrations are added.

## 2026-10-15 unproven acknowledgment and forced owner closure

The native unproven-ack fixture decorates the destination's real ReplicaStorage
client for the exact stream LTX prefix and selects bucket-only proof through
NodeFleet::fleet_durability=false. Both ordinary and multipart publications are
held after arming, then refused without forwarding writes. Lease/catalog/source
storage and the successor's independent client remain healthy.

An already proven inbox effect and saved lease receipt precede the cut. Once a
real stream upload is held, a test-only read-only SQLite connection on the
blocking executor independently observes local checkpoint one, no lease and the
exact saved ack receipt. This is forensic observation, not a public read path.
The actual caller must still return Ambiguous while publication is held, before
forced closure. No success/proof adapter or RPC deadline extension is installed.

Forced Node::cancel closes admission and joins; the fixture then observes the
old node's actual authority lease expire before asking a successor to claim.
An immediate recovery probe had returned Unavailable while that lease could
still be live, so startup/transport/lease budgets are retained and authority
expiry is an explicit prerequisite. Before any receipt retry, recovery must
show a different owner and higher epoch, original subscription/checkpoint zero,
and the exact unexpired original lease. Bucket-only proof and refused uploads
make checkpoint one unavailable to recovery in this cut. The healthy consumer's
proven inbox effect remains one; retrying the saved receipt must acknowledge
once and then report AlreadyAcknowledged.

A second event then acquires a new lease and the independently expected inbox
sum three. The original receipt must preserve checkpoint one and the second
lease; only its fresh receipt advances checkpoint two. Both node domains and
the observer join. Three focused final-source runs pass (initial 10.10s), and
all 46 serial Node integration tests pass in 386.81s. Formatting checks 504 Rust
files; docs validate 36 documents, 6 flows and 386 references; the dependency
audit checks 403 packages without a JavaScript engine. Workspace/all-target/all-feature
Clippy passes with warnings denied (12.60s); whitespace checks pass.
Local Rust 1.97.0/aarch64 macOS uses symbols-disabled, nonincremental dev/test
profiles. This covers forced in-process domain closure and failed local-bucket publication, not power
loss, an independent process kill, follower-only proof, physical network failure
or concurrently executing old owners. The broader release matrix and earlier
unexplained startup/recovery failures remain open. Production code, version-one
markers and existing lease/RPC/shutdown budgets are unchanged by this slice.

## 2026-10-15 lost fetch response and owner handoff

The native peer fault proxy now captures and drops __streamNext responses as
well as append/ack. The new SDK fixture supplies an explicit retained caller
fetch ID and saves its recovered receipt with inbox/business effects in one
transaction, proving that commit before acknowledgment.

The test audits the actual complete proven fetch response and independent
checkpoint-zero/live-lease status before permitting reply loss. The original
caller must report Ambiguous and have no applied effect. A competing fetch ID
must report LeaseBusy without changing the lease. After complete joined owner
handoff, independent owner metadata must show a different node and higher epoch;
status must retain the original subscription identity, checkpoint and unexpired
lease before any successful replay. The same fetch ID must recover the exact
receipt, issue/expiry times and encoded records. Reprocessing that live batch
must leave the inbox effect at one. Only acknowledgment advances checkpoint one.

The same fetch ID is then reused after ack for event two: it must obtain a
higher lease generation and different token, with the independent inbox sum
three. Replaying the old receipt must leave this second lease intact; only its
fresh receipt advances checkpoint two. Both nodes, proxy and observer join.
Three focused runs pass (initial 6.17s), and the complete serial Node suite
passes all 45 cases in 357.62s. Formatting checks 503 Rust files; docs validate
36 documents, 6 flows and 386 references; the native dependency audit checks
403 reachable packages without a JavaScript engine. Workspace/all-target/all-feature
Clippy passes with warnings denied (54.55s); whitespace checks pass.
Local verification uses Rust 1.97.0/aarch64 macOS with symbols-disabled,
nonincremental dev/test profiles. Production code, owned markers and deadlines
are unchanged by this slice. This is a lost proven reply with orderly joined handoff over a local
SQLite-backed object store; abrupt process/storage loss, physical network
isolation, post-expiry fetch replay, unproven ack and concurrently executing
old-owner cuts remain separately unqualified.

## 2026-10-15 event startup supervision

A deterministic regression exposes a separate startup liveness bug. prepare()
marks generation readiness pending before the driver polls event activation.
The driver may select an authority fence or actor exit first. Activation's
cancellation guard is created inside its first poll, so that early exit cannot
finish readiness. Node::start retains the generation (and its readiness sender)
while waiting only for the latch, leaving startup pending after its supervisor
has exited. This is not established as the cause of the earlier no-event
bootstrap/recovery failures.

The production startup path now waits on readiness and supervisor completion
together. Completed supervisor errors/outcomes fail startup; a completed
supervisor also wins over a simultaneously ready latch. Activation failure keeps
the existing close/join behavior. A pending live supervisor remains pending until
activation, and successful startup retains its original live join handle. No
new timeout, shutdown shortcut, public API or version marker is introduced.

The regression fails before the fix in 0.01s and passes after it. Four focused
embedded tests cover the pre-activation exit, completed outcomes with either
latch state, pending-to-ready success with live supervision, and close/join on
activation refusal. Full local verification passes 227 library tests (6.87s)
and all 44 serial Node integration tests (341.40s). Formatting checks all 502
Rust files, docs check 36 documents/6 flows/386 references, and whitespace checks
pass. Workspace/all-target/all-feature Clippy passes with warnings denied (1m 38s);
the dependency audit checks 403 reachable packages without a JavaScript engine. Verification uses Rust 1.97.0 on aarch64 macOS with symbols-disabled,
nonincremental dev/test profiles. The wider failure/recovery/capacity matrix and
the unexplained earlier startup/shutdown failures remain open.

## 2026-10-15 proxy failure diagnosis and qualification

PRs #27, #34, #36, #37 and #38 are merged. PR #39 is open. The
closed-domain wake-publication panic has a reproduced fix in #38; other
intermittent startup/shutdown failures remain unexplained.

A captured full serial Node run passes 41/42 in 364.87s. Delayed proven ack
takeover fails with an unlabeled timeout. The executable's inode, size and
modification time remain unchanged throughout that run. Earlier factory-entry,
explicit-policy bootstrap and predecessor-recovery shutdown failures all pass
in this run and in 24 targeted attempts (eight rounds of three), without a
production repair. Passing reruns do not close those failures.

After merging current main, three bounded four-case rounds produce two complete
passes and a third late-ack failure at **wait for held ack release**. The three
startup/recovery cases pass in all three rounds. The test has already captured
the proven acknowledgment, joined the fenced destination, and audited the
successor checkpoint/identity/history before this wait fails. Observing the proxy
supervisor while waiting exposes `Broken pipe` on the tenth ack-only attempt.
An existing connection to the intentionally fenced destination can fail while
other connections hold the proven reply; the proxy previously allowed only new
connection refusal at that cut. Its supervisor then exited and dropped the
held-response task, leaving the release observer pending.

The helper now accepts connection-refused/broken-pipe/reset/aborted/not-connected
transport errors only after the fixture explicitly marks its destination closing.
Parsing and unrelated I/O failures remain errors. A real TCP malformed-response
case proves the closure mode cannot hide an unframed response. The release wait
also supervises the proxy so unrelated failures preserve their original cause.
This repairs the reproduced helper failure, not the separately unexplained
startup/recovery failures or a product acknowledgment durability bug.

Failure-only diagnostics now observe startup polling, completed result and the
existing directory lock. All timed waits in delayed ack takeover identify their
stage: first/second delivery, proven response capture, destination fence/join,
held response release and original caller completion. Existing budgets and
correctness assertions are unchanged. The helper correction does not establish a green release gate. Local results use Rust 1.97.0/aarch64 macOS with
symbols-disabled dev/test profiles; historical sections preserve their exact
source and evidence scope. All owned markers remain 1.

Final local validation of the helper correction: all 44 Node integration cases
pass in 343.77s, and ten focused repetitions each pass delayed ack takeover and
the malformed-response guard (20 cases total). Native Node Clippy passes with warnings denied (11.02s). Formatting checks
502 Rust files; docs validate 36 documents, 6 flows and 386 references. The captured pre-fix
41/42 run and subsequent narrowed failures remain part of the evidence. No Node
lease, startup, readiness, RPC or shutdown deadline was relaxed. Production Rust
and all owned version-1 markers are unchanged by this correction. These green
runs do not establish causes or repairs for the earlier intermittent
startup/recovery failures, and do not close the wider release matrix.

## Current code

- `Storage::tx` delegates to the existing synchronous savepoint machinery.
- Stream age maintenance quiesces when the first retained record is protected
  by a required checkpoint or live lease. Lease-expiry work remains scheduled;
  checkpoint/lifecycle/configuration transitions transactionally recompute the
  deadline. Indexed minimum checkpoint, bounded leases and one oldest record
  bound eligibility observation without scanning retained payloads. Eligible sweeps retain their existing
  bounded continuation interval and other alarms/work reasons remain intact.
- `cellar outbox census [CLASS]` walks one bounded bucket-directory page and
  probes source owners through signed inspection, including root producers
  unseen by destination inventories. It reports source/empty/unavailable without
  lanes/payloads/tokens; partial failure prints the page/cursor and fails the exit.
  `census1` continuations bind fleet/prefix/class and filtering never expands the
  listing budget. This is live root census, not facet/historical inventory or
  aggregate gauges. The real RustFS CLI roundtrip verifies one-prefix pagination,
  a capacity-blocked undispatched source, empty cells, foreign cursor rejection
  and unavailable owners. Broader backend/ownership/cold/facet gates remain open.
- `Storage::outbox(&OutboxOptions)` exposes bounded source diagnostics through
  the owning activation: total backlog, per-lane rows/bytes, destination identity,
  retry deadline/attempts, retained error and seal state. Pages contain at most
  100 lanes and never copy event bodies or full bindings. Continuations bind the
  source scope and incarnation; pages observe current state rather than a frozen
  snapshot. Empty inspection creates no source/schema/wake state.
- `Storage::retry_outbox_lane(&OutboxRevision)` requires the observed source
  incarnation, dispatch generation, next sequence and settled watermark. The
  pure core rejects a changed revision or exhausted generation. A successful
  retry advances the generation to fence an older in-flight reply and commits
  wake work atomically, without changing event bytes or skipping sequences.
  The existing bulk SDK retry now advances pending-lane generations too and
  refuses exhaustion before changing any lane.
  The authenticated operator route now exposes this foundation.
- `cellar outbox inspect/retry/status/audit` reaches the source owner through
  fleet-HMAC ingress and distinct source RPC provenance. Source control methods
  run before application constructors or handlers. Event authority retains its
  prior wire shape. The original input, ownership, output and discovery gates
  cover both local and tunneled source commands.
- Source retry receipts are committed with wake work and retained under an
  independent version-1 control marker. Their operation/digest/operator/reason/
  expected-revision/result identities resolve replay and reject conflicting
  requests. The pure core owns admission/replay/capacity decisions. Status and
  bounded audit survive business reset and owner handoff; unknown operations
  return `OperationUnknown`. No receipt expiry or sequence abandonment is added.
  New operations cap at the captured receipt limit or 10,000 per source;
  exact replay remains available at capacity. Request/RPC/receipt/client-reply
  budgets are 16/64/4/1024 KiB, and revision files are read through a 4 KiB cap.
- `Context::stream`, typed `Event`/`Stream` handles, resolved resource identities,
  binding publication permissions, domain ownership, and fallible envelope
  decoding are present. `WorkerConfigOptions::stream_bindings` is propagated
  into native worker configuration; existing constructors explicitly supply
  empty bindings.
- Native `ApplicationConfig` now models typed `streams` and `stream_bindings`.
  Packages install both reserved SQLite event classes and require
  `event-delivery-v1`. Publication validation, managed ingestion and fleet
  loading reject unmarked/oversized/malformed event metadata, mismatched
  principals/scopes/grants, conflicting environment names and incompatible
  subscription limits. Parsing bounds each declaration before cloning it.
- Loaded deployments retain desired definitions separately from resolved
  application bindings. Generation construction repeats validation under its
  captured host limits, checks graph-wide resource/name uniqueness, installs
  the built-in event classes for programmatic hosts, and retains definitions
  with their declaring script through `Generation::event_streams`. This data
  is desired state, not an activation receipt.
- Node startup and adoption now reconcile captured definitions through the
  authenticated owner route. Stable deployment operations replay safely;
  readiness also verifies the catalog's current lifecycle, accepted revision
  and definition digest, rather than trusting historic configuration receipts.
  Every explicit binding independently resolves the partition's full resource
  identity, current grant subset and live subscription names, after an authorized
  catalog lifecycle check rejects creating/updating/retiring/deleted resources.
  This also applies to bindings with no local resource declaration. Generation-local
  readiness gates emission, reads and consumption; public startup ingress waits
  for activation, and failed adoption preserves the serving generation.
  Already committed configuration is not rolled back across resources.
- Deployed event bindings require replication and durable wake discovery,
  including on one bucket-backed node. SQLite-only startup is rejected before
  provisioning. A real Node test exposed unqualified local restart inventory
  and uncovered internal alarms during handoff; no deadline extension or drain
  assertion relaxation was used to conceal that limitation. Local-only event
  support requires a separate discovery/recovery implementation and proof.
- `Storage::emit` and the source SQLite outbox record immutable envelopes,
  persistent source incarnation, per-destination lane sequences, counters,
  and wake intent in the same transaction. Internal savepoints prevent caught
  emission errors from leaving partial state. Pending delivery prevents
  `delete_all`; an empty reset preserves protocol identity/history.
- Outbox, inbox, stream and catalog schemas all have version-1 markers.
  Activation rejects unmarked, unsupported or incomplete event layouts before
  schema writes. Current-state reopening retains obligations and identities;
  lower host limits refuse incompatible retained payloads.
- Fleet, peer and native host markers stay at 1. Fresh marker creation is
  concurrent and reopenable; unknown layouts fail closed. Development state is
  recreated on hard cutover, with all participants rebuilt. No fleet upgrade
  command or old-build compatibility qualification remains in scope.
- Event encoding enforces a byte budget during serialization, before building
  the decoded payload, and bounds final envelope encoding inside the savepoint.
  Key limits apply before copying. Admission failures expose typed transaction,
  family, permission, payload, producer-capacity and outbox-capacity outcomes;
  zero event versions are rejected before they can strand a durable obligation.
  Integer payloads outside i64/u64 are rejected instead of rounded through f64.
- Internal work has independent deadlines and persistent generations. Wake
  triggers project the minimum of application alarms and internal work,
  advance installation identity even when the minimum stays unchanged, and
  participate in commit/rollback and restore hints.
- Native alarm dispatch selects due internal work independently of application
  alarm handling. The source dispatcher sends one bounded lane batch through
  RPC after explicit discovery/data proof, retains a retry deadline, and
  fences response cleanup with a dispatch generation and echoed identity.
- Dispatch authority uses the principal/grant stored in the immutable source
  lane, independently of the current application script name or environment.
  New application emission still checks the current script and binding. The
  native fixture adopts a renamed script with no bindings, proves a previously
  committed event still appends, rejects new emission, and then restores the
  original application configuration. Destination grants remain authoritative.
- Explicit storage sync independently waits for discovery without consuming
  the response's original gates. Removing an application alarm refreshes
  discovery if another deadline remains.
- Dependency-free decisions cover contiguous append deduplication, cursor
  expiry, exact-batch acknowledgment, idempotent lease extension, retention
  protection, retry timing, and captured event API limits.
- Protocol metadata integers use decimal strings. Event bodies remain encoded
  bytes carried as base64, preserving integer payloads through the binary64
  structured codec. Hard transport batch limits apply even under unrestricted
  host API policy.

- Host-minted event RPC provenance travels through native runtime routing and
  authenticated peer tunnels. Ordinary application RPC cannot target either
  reserved event class. Receivers authorize persisted resource grants rather
  than trusting publication flags in packet arguments. Emission also checks
  the source activation's current application bindings.
- `__EventCatalog` persists pending creation/update intent, operation identity,
  canonical definition digest and retry work before contacting `__EventStream`.
  Activation becomes durable catalog state only after identity-checked stream
  acceptance. Retries preserve the operation and original authorized caller.
- The stream cell atomically stores ordered event bytes and producer high-water
  marks. Watermarks survive payload retention. Reads and fetches select bounded
  metadata before copying bytes, avoiding count-times-event-size allocations.
- Required and bounded retention restrict oldest-prefix reclamation by live
  leases and required checkpoints. Stream counters and retention floors move
  atomically. Optional subscriptions explicitly expire when they lose history.
  Dispatch persists receiver batch-capacity hints and adapts oversized batches
  without dropping, bypassing, or partly accepting an event prefix.
- Typed subscription handles resolve deployed names. Fetch request identity,
  batch receipt, checkpoint, lease generation, token, retry deadline, and last
  acknowledgment are persisted. Same-request fetch replay returns the same live
  batch; competing fetches return `LeaseBusy`. Ack/retry/absolute extension use
  exact receipts and the dependency-free policies. Logical time is persisted
  even when a deadline operation returns a typed error.
- Long polls subscribe before observing durable state, release transactions
  before awaiting, and use the execution-domain clock and cancellation. Waiter
  budgets apply both per partition and per host. Host dispatch reservations cap
  concurrency and prevent two in-flight batches for the same source lane;
  SQLite deadlines remain the recovery authority.
- `Storage::inbox(scope).accept(&record)` composes a digest receipt with business
  state and further emission in the same transaction. Matching retained
  receipts skip effects, conflicts fail, capacity rejects new identities, and
  bounded explicit pruning ends the selected deduplication guarantee. Receipts
  and accounting survive storage reset; there is no automatic expiry.

- Subscription administration now has owner-routed status, guarded pause/resume,
  explicit reset, deletion, and paginated durable audit receipts. UUID/generation
  guards and operation digests prevent delayed commands from affecting a
  successor. Reset reports skipped offsets and clears old lease/ack authority;
  deleting the last required subscription is refused until retention changes.
- Deletion retains subscription tombstones. Reconciliation can explicitly
  recreate a live name with a new ID; stale receipts and fetches cannot attach
  to it. Consumer handles retain their first resolved identity and include it
  in every fetch. Current consumer grants remain usable even when an unrelated
  administrative permission in the local binding is stale.
- The version-1 stream schema uses live-name subscription uniqueness, preserving
  retained deleted identities. Earlier development layouts require recreated
  state; the native fixture exercises deletion/recreation and durable audit
  capacity without a schema migration.
- `tx.seal(&stream, operation)` requires an existing fully settled source lane.
  It disables emission and records a terminal seal obligation with wake intent
  atomically. The dispatcher proves and retries that obligation through the
  same host/lane reservation and generation-fence path as append. Destination
  sealing checks immutable source/grant identity and exact terminal high water.
  Source reopen/reset retains pending seal work; old append and seal retries
  remain idempotent while new emission fails with `ProducerSealed`.
- Storage reset now preserves stream schemas, producer watermarks, subscription
  and audit metadata, and catalog tombstones as well as source/inbox state.
- Stream retirement records catalog intent before delegated closure. Coordinated
  mode checks sealed registered producers, caught-up subscriptions and no live
  leases; the named external procedure is an explicit attestation about unknown
  sources. Force records a loss acknowledgment and closes ordinary append/read/
  fetch/ack/configuration authority. Pending source rows remain blocked and
  inspectable; neither mode silently retargets or discards them.
- Terminal partition closure survives owner eviction and bounded event/metadata
  cleanup. Row and byte budgets apply to metadata too. The catalog waits for
  completed partition cleanup before retaining its deleted UUID tombstone and
  allowing the live name to bind a new UUID. Lost closure replies reconcile
  through the same immutable operation; historical configuration replay cannot
  revive retired authority.
- Version-1 stream and catalog schemas contain retirement state and immutable
  operation receipts. Current catalog grant checks precede configuration and
  retirement receipt disclosure, including for the original operation caller.
  Duplicate coordinated retirement rechecks receipts after its async preflight.
  Nullable operator provenance distinguishes SDK and operator receipt ownership,
  and missing current-layout columns fail before
  base schema mutation. Terminal catalog receipts retain the captured payload
  range and counters after bounded cleanup. Authenticated retirement/status CLI
  commands enforce revision, operation, caller and explicit destructive intent.

The dispatcher targets `__streamAppend` and `__streamSeal` on `__EventStream`; both event system
cell factories and their internal maintenance handlers now exist. Configuration
and consumption are available through programmatic native bindings. Native
project/manifest metadata now survives publication and generation loading, and
Node startup/adoption now performs owner-routed activation and binding readiness.
The development contracts are implemented but release qualification is incomplete, so
this does not constitute a released deployed primitive yet.

## Requirement audit

| Requirement | Evidence now | Work remaining |
| --- | --- | --- |
| Short producer APIs and atomic emission | Source tables, `tx`/`emit`, bounded serializer, typed admission errors, native source test, storage rollback/capacity/reopen tests | Complete public error/overflow classification and adoption budgets |
| Resource identity and authorization | Host RPC provenance, reserved classes, durable catalog reconciliation, independent receiver grants, receipt revocation, deletion/recreation and terminal UUID tombstones; validated native declarations/bindings; owner-routed startup/adoption with current-state proof and independent binding resolution; optional name selectors resolve and pin UUIDs once per generation | Complete name/adoption race and administrative authority qualification |
| Stream append/read and deduplication | Persisted destination schema, built-in factory, atomic producer watermark, bounded read API; duplicate acceptance after payload expiry; exact-byte append retry after a proven peer reply is lost; version-1 stream/catalog and source/inbox schemas; feature/format capability | Wider fault, separate-host/storage and adversarial qualification |
| Durable outbox delivery | Real destination integration, generation-fenced cleanup, blocked permanent errors, adaptive batch limits, host/lane reservations | Cancellation/routing/fairness qualification and operational controls |
| Subscriptions and settlement | Durable leases/checkpoints/ack receipts; same-fetch replay, busy competitors, retry, absolute extension, bounded long polling; proven ack reply loss and unchanged receipt replay return `AlreadyAcknowledged` with one effect/checkpoint | Complete deadline/error/race matrix and production lifecycle/crash qualification |
| Recovery and ownership | Source reopen; live subscription lease survives broker eviction/reopen; bucket-backed owner handoff and removed-class recovery preserve delivery, receipts and business state; existing output/wake protocols reused | Wider follower/owner-loss, source/destination/consumer crash and response-loss qualification |
| Retention and backpressure | Both modes, lease exclusion, bounded sweeps, optional cursor expiry; required-consumer pressure resumes after ack; coordinated/forced retirement and catalog tombstones | Complete lifecycle limits, source destruction crash/lease/policy and SQL-reset capacity qualification, retention continuation/capacity evidence |
| Consumer inbox | Atomic storage helper, digest conflict, capacity, rollback, reopen and bounded pruning tests; actual bucket-backed Node processing/retry/ack suppresses duplicate effects; runnable producer/consumer example verifies retained receipts across preserving restart | Complete proof/crash qualification |
| Operational controls | Source retry/seal and pending-reset protection; signed owner-routed source/stream/subscription CLI/API, guarded pause/resume/reset/delete and catalog retirement/status, immutable operation receipts, bounded subscription and destination-observed source inventory, metadata reads and payload opt-in; guarded source destruction/recreation, exact terminal evidence, retained loss receipts, bounded remote/local facet cleanup and incarnation fencing; namespace catalog inventory and fixed-cardinality event metrics | Catalog namespace/facet/historical census, aggregate owner-state/physical metrics and broader control qualification |
| Compatibility and deployment | Native metadata/feature validation, binding validation under host limits, generalized wake projection; activation/readiness and failed-adoption preservation; peer/native/fleet/schema markers at 1; hard-cutover development policy and incompatible source-policy refusal | Fresh-state/reopen fault qualification; destination/consumer policy-adoption matrix |
| Examples and documentation | Full joint design, implemented transaction/inbox API guide, cataloged standalone stream-backed application and actual executable smoke with preserving restart | Extend examples as remaining lifecycle controls land; release qualification remains separate |
| Release criteria | Local SDK/storage/policy and integrated native fixture | Full workspace gates, supported single-node/fleet proofs, capacity, lifecycle and fault matrix |

## Next implementation step

The host/routing failure classification is implemented below. Continue the full
SDK preflight/codec/timeout/cancellation mapping audit, aggregate metrics and wider
inventories, alongside the complete release matrix.
The bounded/canonical Stream::read acceptance gap found by that audit is repaired
below; it does not close the complete client/codec/subscription response audit.

Investigate the observed cancelled-bootstrap deadline/DomainClosed failure; isolated
repeats and a complete node replay pass, but its cause remains unestablished.

Complete the remaining release fault matrix across independent processes/physical
hosts and supported storage backends, including followers, expired/isolated owners,
source cleanup cut points, grant/policy races and whole-fleet recovery. Keep saved
independent measuring ledgers and audit before replay at every ambiguous boundary.
The process harness and before-first-send SIGKILL case supply scoped single-node
SQLite-bucket evidence. The real peer proxy adds append/ack response loss across
three embedded nodes in one process, not the entire fleet fault matrix.
The independent-process append-loss driver also runs on a fresh private RustFS
S3 bucket; its passed case proves network-store recovery at that boundary, without
qualifying store/follower loss or separate physical hosts.
The whole-fleet driver also proves one bucket-mode recovery boundary on both
backends after killing all three processes and discarding their local node data,
with a pending source and proven unacked consumer effect. Wider cut points and
follower-only/store/power faults remain required.
The independent ack-loss driver also audits checkpoint/effect/receipt before
destination SIGKILL and lost reply, then replays only the persisted exact receipt
after recovery audit. Its SQLite/RustFS cases cover that selected ack boundary;
consumer loss and the remaining lease/ack cuts are still required.

Source destruction/recreation now has a durable closed-admission lifecycle,
exact destination evidence, immutable loss receipts, source/work/lane fences,
registered facet cleanup and fresh producer/facet identities. Explicit replay
resumes outage-delayed cleanup; remote and local removal counts are bounded.
Qualify abrupt loss, lease/grant/policy changes and large business SQL reset,
and measure physical SQLite/WAL/LTX/bucket growth and maintenance fairness.
Cold wakes, hot streams, compaction/restore, lag/backpressure, memory and offline
soaks retain their full capacity scope.

Finish catalog namespace/facet/historical source discovery, aggregate owner-state/physical metrics, the complete
name-resolution/policy/error/adoption matrix and
qualified SQLite-only recovery inventory. Current subscription, accepted-source
and namespace catalog listings are bounded live views; accepted-source inventory
is not a census of undispatched producers or an all-producer drain proof.
Fixed-cardinality host event observations are not cold-cell authoritative gauges.
Run the full workspace/release gates and audit every design requirement before
claiming completion. Historical verification notes below do not replace current
requirement-by-requirement evidence.

Retain the source operator's separate host-minted authority and owner RPC/tunnel
path when extending controls. Never substitute raw deletion or a sequence skip
for the audited lifecycle.
Qualify source operator cancellation, real response truncation and stale-owner
routing beyond the current two-node handoff test. Cold activation now has a
separate framework recovery pool for restored sources whose class is absent.
The core rejects unknown fresh scopes; admission then checks retained source
identity under the restored owner epoch and closes non-source connections.
This executor has no application bindings or callbacks and can dispatch stored
obligations and source controls. Business calls remain unavailable. Internal
work has priority over unavailable application alarms; those alarms back off
without using the abandonment budget. Re-registering the class returns business
execution to the application through ordinary generation movement. Wider crash,
follower and cancellation qualification remains required.

Exercise fresh initialization, same-build reopening and current-layout admission
under real fleet faults, including dropped marker CAS responses and the complete
destination/consumer lower-host-policy matrix. Historical migrations and prior
executables are outside the internal development policy.

Remaining correctness audits include configuration retries during revocation,
unknown/expired receipt classification after maintenance, long-poll cancellation
and waiter limits, permanent versus transient error mapping, incompatible lower
host policies, destructive source operations, control-message allocation bounds, and
maintenance scheduling for protected old history. Keep adapter effects separate
from pure protocol decisions. Source and destination tables currently use hex
text identities; the final schema still requires review. Layout changes use
development hard cutovers without migration machinery.
Native definitions retain explicit creation UUIDs. `BindingConfig` now supports
omitted IDs for catalog name resolution; runtime `Binding` identities stay fully
resolved and pin to the activated generation. Resource definition capture alone
must not be mistaken for successful activation.

Continue real failure qualification through discovery, replication, follower
proof, ownership changes, output gates and shutdown. The runnable example uses
actual bucket-backed Node services and requires complete preserving shutdown;
it verifies explicit retry and clean restart, not crash or actual response loss.
The lower-level fixture deliberately supplies a successful proof adapter and
no replication; it cannot establish those production guarantees. Do not mark
the goal complete from a local passing delivery test.

## Verification and environment

The checkout is `codex/outbox-design`, based on
`c35c1b257a5804d367a2aeffe65b8a3ba5c1aa3a`, with main through `6a8e252`
merged to preserve the Axum/external-ingress and workflow additions. The
implementation is submitted as PR #27 at the user's request. Operator tooling, examples and
production event failure qualification remain part of the active goal. Do not
discard the original design or documentation edits.

Use the installed toolchain via
`PATH=/Users/haipingfu/.cargo/bin:$PATH` and the checkout-specific mounted target:
`CARGO_TARGET_DIR=/Users/haipingfu/Workspace/crabbuild-target/cellar-2a16-outbox`.
Local rustc is 1.97.0; SDK CI specifies 1.98.1. `hex` was added as a direct
workspace dependency; the lock change only adds it to Cellar's dependency list,
with no package/version changes.

The latest completed focused run passed 67 Cellar library tests, 11 event policy
tests, the integrated native event-delivery fixture, the native source-outbox
test, and existing native transaction and Queue tests: 82 total. The integrated
fixture covers catalog idempotence, durable destination append, exact i64 payload
preservation, duplicate append after retention, unauthorized public RPC, current
grant revocation/restoration, stable live fetch replay, competing fetch exclusion,
retry, idempotent absolute extension, repeated ack, forged-token rejection,
live lease eviction/reopen, lease expiry and redelivery, bounded-history cursor
expiry, long-poll notification, adaptive atomic batching and required-consumer
backpressure. New evidence covers pause with required retention, resume,
explicit skipped ranges, reset invalidation, operation replay/conflict, old
command and fetch rejection, delete/recreate identity, paginated audit history,
operation capacity, resource grant checks despite stale local permissions,
legacy schema migration, terminal producer sealing, source reopen with pending
seal work, rejected new emission, duplicate terminal acceptance, and protocol
metadata preservation through reset. Latest evidence also covers coordinated
retirement's local preconditions, forced closure with a live lease and pending
source row, a dropped closure reply, restart during two-row cleanup batches,
catalog deletion/recreation, old-resource request rejection, and a retained
blocked source obligation. The fixture seeds an earlier catalog's pending
creation and verifies migration without losing its operation or authority.
After grant revocation, original callers cannot bypass current authorization
through configuration/status/retirement receipt replay. Its scope is local
storage/native turns and injected successful proofs, not production fleet
delivery qualification.

The source fixture verifies that an event serializer generating ten million
elements stops within the configured byte budget, caught admission failures
leave other business writes intact and consume no sequence, invalid event
versions are rejected, and integer payloads outside the supported range fail
without rounding. Storage tests additionally reject a first oversized envelope
without retaining source identity/schema initialization or outbox wake work.
One preexisting loader test initially failed because its drop probe used ambient
host registries during a foreign-domain drop. The probe now captures weak owner
registry references; the regression deliberately holds a different domain's
registry lock while dropping both retained worker slots. The final parallel
library run passes all 67 tests, including that regression.

Workspace Clippy across all targets and features with warnings denied passed
on the latest source state.
Documentation integrity, whitespace, formatter and the all-feature dependency
inspector pass; the latter reports 387 reachable packages and no JavaScript
engine. The formatter covers 358 Rust files. Clippy also type-checks all
workspace targets and features. The completed focused command is:

```sh
cargo test -p cellar --lib --test native_outbox --test native_event_stream \
  --test event_stream_policy --test native_transactions --test native_queue --locked
```

The latest deployment-focused run passed four native artifact tests plus the
native source and integrated event fixtures. It exercises event package
publication/loading, retained desired definitions, current binding projection,
reserved class installation, missing feature/schema refusal, forged principals,
scope/grant/subscription mismatch, conflicting environment names, lower host
limits and generation validation after tampering. The native CLI gate also
passes. That fixture proves the metadata contract, not runtime provisioning:
no automatic resource activation or real fleet proof is exercised by it.
Workspace/all-target/all-feature Clippy with warnings denied passes after these
deployment and stored-principal dispatch changes; formatting covers 359 Rust
files. The updated integrated fixture additionally verifies script rename and
binding removal across real generation adoption: old pending events reach the
destination under their original grant, while the new application cannot emit.

The complete workspace/all-feature library and integration test command now
passes, including CLI/artifact checks, the external Cargo dev/rebuild/restart
workflow, 16 public-node lifecycle/recovery tests, and all new event fixtures:

```sh
cargo test --workspace --all-features --lib --tests --locked
cargo test --workspace --all-features --doc --locked
```

All nine doctests pass in the separate stable target directory
`/Users/haipingfu/Workspace/crabbuild-target/cellar-2a16-outbox-docs`. Two attempts
in the main shared target failed to link runnable documentation examples because
transitive library artifacts were missing in rlib form. Building ordinary
libraries changed which artifacts were missing but did not resolve that cache.
An isolated complete build passed without changing source or weakening the tests.
The Cargo dev regression concurrently built an external project against the
main target; keep the doctest cache separate from that workflow. No other
checkout's build cache was cleaned.

The activation changes passed 90 focused regressions: 67 Cellar library tests,
four artifact tests, the integrated stream fixture, all 17 public Node tests,
and the native source fixture:

```sh
cargo test -p cellar --lib --test native_node --test native_artifacts \
  --test native_outbox --test native_event_stream --locked
```

The actual public Node event test uses a single-node development bucket without
a synthetic successful-gate adapter. It checks unsupported local-only startup
and joined cleanup, owner-routed declaration activation, delivery, durable inbox
redelivery, failed binding adoption, valid adoption, retired-resource replay
refusal and complete shutdown. A subsequent lifecycle review added catalog
resolution before partition binding resolution, closing adoption while delegated
retirement remains pending. The Node test now also refuses external bindings to
that retiring resource, without a local declaration that could mask the missing
catalog check. Both this Node test and the integrated stream fixture passed again
after that change. Workspace/all-target/all-feature Clippy with warnings denied
also passed on the latest source. Formatting covers 362 Rust files; documentation
integrity, whitespace and the all-feature dependency inspector pass. The complete
workspace and doctest results above predate activation changes; do not imply the
complete release matrix was repeated or achieved by these focused checks.

Event example, capacity, deployment cutover and event-specific production fleet
fault gates remain outstanding. Existing node/log/container suites are regression
coverage; they do not qualify the event primitive's complete activation path or
its full failure matrix. This file records work in progress, not production
readiness evidence.

The source schema and compatibility milestone passed 98 focused tests (72
library, five artifact, one CLI, one integrated stream, 18 public Node, and one
source SDK test) plus workspace/all-target/all-feature Clippy with warnings
denied. The public Node test refuses the old fleet marker before replacing its
predecessor, restarts after explicit upgrade, and refuses peer protocol 5.
Source activation under a smaller host policy fails without admitting storage;
a compatible domain reopens the original bytes and next producer sequence.
Legacy migrations preserve pending seal intent, consumer receipts and durable
deadlines. Unknown/missing markers across all four event schema families fail
before base schema mutation. A failed legacy accounting check rolls migration
back without repairing or discarding retained records.

Fleet upgrade tests scan more than one lease page, refuse an unconfirmed or live
fleet, and exercise eight concurrent identical CAS operations with immutable
receipt replay/conflict checks and retained cell/wake bytes. The actual shipped
CLI help and missing-confirmation errors pass for both binary names. The final
wire review also aligned the tunnel version with peer version 6 and added a
separate authenticated version-5 tunnel refusal. The isolated doctest gate passes
all nine tests on that final source, and workspace/all-target/all-feature Clippy
with warnings denied also passes after the tunnel change. Formatting covers
364 Rust files; documentation, whitespace and the dependency inspector pass.
The full workspace/all-feature library and integration command now passes on
the final source, including the external Cargo rebuild/restart regression and
both public Node event tests with the tunnel refusal. The separate doctest and
final Clippy commands also completed successfully. These tests still do not run the actual previous binary,
inject every crash boundary, or qualify the complete release matrix.

The draft-PR follow-up merges main through `6a8e252`, preserving optional
listeners and external ingress behind event resource activation. The Axum
counter explicitly supplies empty event metadata; the fleet upgrade CLI test
now follows main's single `cellar` executable after removal of its CLI alias.
The workspace/all-feature library and integration command passes 341 tests,
including the development build/restart regression, both public Node event
regressions, external ingress and the new Axum suites. The final bulk retry
fencing adjustment also passes all 14 focused event policy/source/stream tests.
All 11 workspace doctests pass in the separate cache. Final-source Clippy with
warnings denied, formatting across 400 Rust files, documentation (35 documents,
6 flows, 362 references), whitespace and native dependency inspection (401
packages; no JavaScript engine) pass. GitHub CI did not start: its check
annotation reports failed account payments or a spending-limit restriction.
This does not change the outstanding event release qualification gates.

The source-operator milestone passes the complete workspace/all-feature library
and integration command: 344 tests, including the external development
build/restart workflow and all Axum suites. The focused source/event/CLI/library/
public-Node command passes 96 tests. All 11 workspace doctests pass in the
separate target cache; final-source all-target/all-feature Clippy with warnings
denied and formatting across 405 Rust files pass. Documentation validates 35
documents, 6 flows and 364 references; whitespace and the 401-package native
dependency graph pass, with no JavaScript engine.

The new actual two-node test sends signed commands through a non-owner, proves
that unsigned/oversized requests are refused and inspection does not construct
the application cell, repairs a grant-blocked event, and verifies immutable
retry replay/conflict/caller guards, stale producer refusal, audit pagination,
settled no-op retry, delivery and receipt recovery after owner handoff. Business
reset retains the receipt and an unknown operation has a typed result. The SDK
fixture uses an isolated domain with a two-receipt budget: capacity rejection
leaves metadata/wake deadlines unchanged and exact replay remains available.
Schema tests retain receipts across activation and refuse corrupt accounting;
wire tests preserve the original event authority JSON shape. Treating a received
reply as lost exercises idempotent replay, not actual network truncation. The
complete production event failure matrix and wider orphan-source failure qualification
remain outstanding.

The removed-class recovery milestone passes 347 workspace/all-feature library
and integration tests, 110 focused tests and all 11 workspace doctests.
All-target/all-feature Clippy with warnings denied passes. A bucket-backed
public Node regression removes the source class, recovers its pending delivery
and operator receipt without application code, and reintroduces the class with
business state intact. A native worker regression retains an unavailable
application alarm through eight attempts without consuming its abandonment
budget and closes rejected non-source activations. These checks qualify those
paths, not the complete crash, follower-proof or cancellation matrix.

Public `Cursor::decode` and `Receipt::decode` now retain `InvalidCursor` and
`StaleReceipt` across malformed base64, JSON, protocol integers and identities.
They do not expose supplied token bytes in diagnostics. The token regression
corpus also preserves valid positions above binary64 precision through i64::MAX.
This closes that SDK boundary; deadline, RPC-reply and overflow classification
still require their own qualification.
All three token regressions and the existing integrated native stream test pass
after this change; final-source workspace/all-target/all-feature Clippy with
warnings denied also passes. The 347-test workspace result above applies to the
preceding recovery milestone.

Stream and subscription operator controls now delegate a supplied resource
binding through signed `/streams/` ingress and repeat current receiver grant
checks. RPC provenance limits operators to the stream control methods and cannot
authorize configuration or append. `cellar stream` exposes describe/read/status/
audit; `cellar subscription` exposes describe/pause/resume/reset/delete. Both
families use the same authenticated operator label for immutable receipt status
and replay. SDK event packets omit that optional field and SDK receipts without
it remain readable. Subscription operation receipts persist operator identity
inside their existing JSON field; no SQLite layout change is required.

Requests/binding files, expected identity files, read bytes and replies are
bounded to 16/4/256/1024 KiB; pages stop at 100 records. Metadata reads remove both
payload and its encoded envelope; payload display requires opt-in. Destructive
CLI changes require confirmation flags and the observed subscription identity.
An actual two-node regression passes signed forwarding, grant-claim refusal,
metadata/payload/continuation reads, pause/reset/resume/delete, stale identity,
immutable replay/conflict/operator guards, audit pagination, typed unknown status,
owner handoff recovery and revocation even for an existing receipt. The test
replays received replies; it does not inject actual network response loss.
Resource inventories, stream retirement CLI, exported metrics and the full
control-message fault/capacity matrix remain outstanding.

Read projection also rejects malformed page shapes and a response naming another
resource before issuing a resume cursor. Its regression reproduced a mutable
JSON-indexing panic before the fix. The first broader workspace run failed an
Axum service response assertion; the enhanced diagnostic assertion preserves
the same response/handler requirements. An isolated rerun and twelve concurrent
repeatability runs pass, but the original cause remains unproven. The final
workspace gate is rerun with Clippy scheduled afterward to avoid adding compiler
load during short-lease host tests.

That final runtime source passes all 354 workspace/all-feature library and
integration tests, including the Axum service assertion and the full 21-test
public Node suite, plus all 11 doctests. The two shipped CLI tests pass again
after replacing ambient fixture writes with owned temporary files. Final-source
all-target/all-feature Clippy with warnings denied passes. The earlier service
failure remains recorded above; passing reruns do not establish its cause.

Stream retirement now has owner-routed `retire` and `retirement-status` commands.
The CLI requires a revision, immutable operation, bounded reason and explicit
confirmation, plus either an all-producer coordination attestation or a force
data-loss acknowledgment. Operator authority permits catalog retirement/status
and its partition preflight/closure; configuration and append remain excluded.
Both catalog and partition receipts retain the authenticated operator label and
reject replay by another operator. Current grant checks remain independent.

Catalog schema 3 and stream schema 4 add nullable operator columns. Migrations
preserve earlier receipts with NULL provenance; current markers missing those
columns, or declaring another type, fail before base schema mutation. Terminal
receipts retain captured records/bytes and the exact affected retained payload
range after bounded cleanup. The range derives from the closure's record count
and immutable end offset; retirement closes append and retention only removes
prefixes. Range/counter/provenance acceptance is checked before the catalog
records a terminal result. Older SDK status JSON remains readable.

The actual two-node regression passes both coordinated and forced retirement:
unsealed producers and undrained subscriptions refuse coordination; a source
seal and consumer acknowledgment permit it. Force captures the unconsumed range.
Receipts survive owner handoff and preserve replay/conflict/operator guards,
typed unknown status, deleted-stream read refusal and historic deployment
refusal. The logical migration test preserves old receipt fields and enforces
new provenance bounds; it is not previous-binary fleet cutover qualification.
The coordination procedure is an attestation, not proof of unseen producers.
Resource inventories, exported metrics, whole-source destruction, runnable
examples and the complete lifecycle/fault/capacity matrix remain outstanding.

The first full retirement gate run reported `NodeFenced` in two native actor
checks while doctests were compiling. The exact same actor binary passes all
six tests after the competing compile finishes, with lease/deadline policies
unchanged. The cause is unproven; the full workspace gate is rerun sequentially
with Clippy scheduled after tests. No fencing or drain assertion was weakened.

The final sequential retirement gate passes 358 workspace/all-feature library
and integration tests, including all 22 public Node tests and the development
rebuild/restart regression. All 11 doctests pass in the separate target cache;
all-target/all-feature Clippy with warnings denied passes on final source.
The earlier actor failures remain recorded above; passing reruns do not establish
their cause or qualify the complete production fault/capacity matrix.


The cataloged `cellar-example-streams` executable now demonstrates a business
transaction and `tx.emit`, bounded resumable reads, a required subscription and
a local projection composed with `tx.inbox`. Its explicit `db.sync` precedes
remote retry/ack. The smoke test verifies duplicate/conflicting commands, two
source cells, stream delivery, cursor resumption, committed effects before retry,
a preserving process restart, retained-inbox suppression before acknowledgment,
an empty fetch and a fresh effect after restart. Both shutdowns require complete
ingress, application, cell and pending-dispatch drain reports. The example owns
its executors and a private SQLite-backed object-store fleet, and persists a
randomly generated fleet secret across restart. It does not qualify a bucketless
SQLite node, abrupt process failure, network response loss or a production fleet.

`examples/durable-streams/README.md` provides the runnable walkthrough and smoke
command. The storage and asynchronous-work guides distinguish implemented APIs
from remaining release criteria. The example is included in the 35-entry catalog.
The all-feature executable build and actual smoke pass; all-target/all-feature
workspace check and Clippy with warnings denied pass. Formatting, native
dependency inspection (401 reachable packages, no JavaScript engine), catalog
source/guide existence, documentation integrity (35 documents, 6 flows, 370 local
references) and whitespace checks pass. The default-feature executable check,
all eight required CLI/artifact regressions and the cookbook catalog regression
also pass. Resource inventories and bounded exported metrics are the next
operational priorities, alongside the still-open source destruction and
full lifecycle/fault/capacity matrix.


Subscription inventory is wired through the SDK's `Stream::subscriptions`,
`cellar subscription list` and signed `subscriptions` commands on the existing
owner route. The current durable administration grant is checked on each page.
A primary-key range reads at most 101 metadata rows for a 100-record page, even
with unrestricted host limits. Deleted incarnations remain visible while
retained; no lease token, payload or envelope is returned. The `subs1` cursor
is capped at 256 bytes, bound to the stream UUID and distinct from event-read
cursors. Receiver and client/operator acceptance validate identity, ordering,
page bounds and exact continuation position. Pages are live observations, not a
fleet census or snapshot. No persisted layout or wire authority fields change.

Focused validation passes: both cursor/error and malformed-response corpora,
all eight CLI/artifact regressions and an actual two-node pagination/lease/
tombstone/handoff/revocation test. All-target/all-feature workspace check passes.
The final broader workspace run passes all 361 library/integration tests,
including all 23 public Node tests and the Axum forwarding assertion.
All-target/all-feature Clippy with warnings denied, all 11 doctests and formatting
(413 Rust files) pass. The rebuilt default-feature stream example passes its
actual delivery/inbox/ack/preserving-restart smoke with complete shutdown
reports. Documentation integrity (35 documents, 6 flows, 370 local references),
native dependency inspection (401 packages, no JavaScript engine) and whitespace
checks pass. These checks are not the full production release qualification. Catalog
stream/source inventories, exported metrics, whole-source destruction and the
complete lifecycle/fault/capacity matrix remain open.


The first broader subscription-inventory gate passed all 23 public Node tests
but failed the Axum forwarding test during startup: node authority was not
established before its startup deadline. Two isolated runs of the exact same
`errors-f316316f0fc88d26` binary and four repeatability runs (two at a time) pass
without code, lease, startup-deadline or drain-policy changes. The cause remains
unproven. The local reload/replication diagnostic appears in successful runs as
well and is not evidence of the cause. The full workspace gate is rerun before
compiler/doc/example gates; the original failure remains part of the evidence.


Catalog inventory work exposed an unmet creation requirement: proposed resource
grants could authorize their own initial creation. The guard now requires a
private provisioning claim from an exact trusted deployment declaration for both
catalog and partition creation. SDK and resource-operator handles cannot mint it.
`Generation::event_provisioning_request` prepares this request for a library host;
it still needs ordinary owner/recovery/proof routing and is not readiness proof.
The native fixture uses fresh state and explicitly checks SDK creation refusal.

## Internal development policy (2026-10-13)

The user superseded earlier compatibility milestones: every Cellar-owned protocol,
format and schema marker stays at 1. Hard cutovers recreate development state and
rebuild participants. No old-build compatibility qualification, schema migration,
fleet upgrade command or upgrade receipt is required. Prior verification entries
below/above describe historical work, not current compatibility promises. Current
code removes those paths, retains creation authority and durability/ownership
checks, and keeps upstream SQLite WAL/LTX codec identifiers unchanged.

Catalog/source inventories, bounded exported metrics and the remaining actual
crash, response-loss, recovery and capacity qualification remain open.
The first full gate passed 78 library tests but refused three legacy fixtures:
the source-recovery fixture seeded outbox tables without a marker, the inbox
fixture deleted its marker before reopening, and the reset/replication fixture
seeded stream/catalog tables without markers. These fixtures now initialize
current version-1 state and retain their original recovery, rollback, deduplication,
reset and replication assertions. No production validation was relaxed.
An independently executed catalog regression also refused the fixture's stale
serving bindings during declaration validation. Its valid trusted provisioning
graph is now separate from the intentionally stale SDK serving graph; receiver
revocation checks and declaration validation remain unchanged. Those regressions
passed on the corrected fixtures. The native development rebuild/restart case
passed in 804.77 seconds across its separate unchanged readiness deadlines.
The full Cargo run then stopped at two outbound HTTP deadlines. The exact
`native_egress-bc2323df6faa7849` executable passed all five tests in isolation,
but a repeat overlapping the remaining runtime executables reproduced those
timeouts. After runtime checks finished, four single-test and four whole-executable
runs passed without code, timeout, lease or drain changes. The cause remains
unproven; these results do not establish that compiler or system load caused it.
The 65 remaining compiled workspace executables ran directly, including the
corrected catalog regression and all 23 public Node tests. Only the tooling
executable lacked `rustfmt` because that direct harness omitted Cargo from PATH;
all six tooling cases passed after restoring the normal PATH. No uninterrupted
full Cargo pass is claimed for this cutover. All final executables have passing
results; the HTTP timing failures remain part of the evidence.
All-target/all-feature Clippy passed with warnings denied. Final formatting
checked 412 Rust files; documentation validated 35 documents, 6 flows and 370
local references, and whitespace checks passed. All Cellar-owned protocol,
format and schema markers are 1; upstream LTX/SQLite encoding and runtime
revision/sequence counters retain their meanings. No development state outside
private test fixtures was reset.


## Destination source inventory (implemented; qualification incomplete)

Adds SDK `Stream::sources`, signed `sources` operator commands and `cellar stream
sources`. Pages require current resource administration and expose only observed
producer lane, source/incarnation, captured principal/grant, accepted sequence
watermark and seal state. A primary-key range bounds SQL to 101 rows, pages to
100 records, and namespace-bound `sources1` continuations to 256 bytes. Receivers
and clients validate lane derivation, ordering, continuation tail, metadata bounds
and canonical decimal watermarks. Payloads, bearer tokens and source retry state
are absent. This is not an undispatched-source census or all-producer drain proof.

All-target/all-feature workspace check and documentation validation passed.
Eight focused source tests passed, including namespace-bound cursors, page-tail
validation, large decimal watermarks, forged lane identity and sensitive field
rejection. The focused public Node/CLI gate passed for retained seals after payload
expiry, real two-node owner handoff and administration revocation during pagination.
Its test uses actual bucket-backed Node services and unchanged proof/shutdown
checks; a zero-event source seal is not fabricated through the SDK. No
protocol/schema version bump.
Catalog-wide/fleet-wide inventory, exported metrics, audited whole-source
destruction and the remaining actual failure/capacity matrix stay in scope.


The initial Node/CLI gate passed both CLI tests and 23 existing Node tests, but
the new inventory test reproduced an incomplete entrance-node handoff drain.
Diagnostics showed its source seal was locally accepted, zero owned/occupied
cells, and two terminal adoption operations still waiting for pacing permits.
The core records terminal failed acknowledgements only after ownership release,
but the driver extended its stall window only for successful acknowledgements.
With more cells than release slots, completion of a failed first batch was not
recognized as progress and the next batch was cut off.

The core now exposes settled handoff count (successes plus terminal failures);
the driver uses that count for progress while retaining the original absolute
shutdown deadline, admission gates, durability proofs, and successful-adoption
count. Seven core routing tests passed, including terminal-only counting, paced
continuation and duplicate/stale reply exclusion. The unchanged full drain
assertion now passes: all 24 public Node and both CLI tests passed, including
source metadata after payload expiry, owner handoff, grant revocation, and the
absolute-deadline regression. Temporary warning diagnostics are removed.
The build script now declares its sole file input, build.rs, so unrelated test
file edits do not trigger Cargo's default package-wide build-script rerun.
Broader workspace qualification was attempted before this milestone was pushed;
its failures and independent replays are recorded below.

The follow-up marker audit found telemetry's `v0-unstable` metadata and the KV
large-value `v2` reference/object prefix. They now use `1` and `v1` respectively,
with matching producer, reader, collector and test callers. Epoch fencing and
collection rules are unchanged. Existing development KV blobs require recreation;
there is no old-reference reader, migration or dual-prefix collector. That
workspace suite was compiled before these two marker edits; its results do not
qualify the final codec changes. The existing KV integration was rerun afterward.

The broader all-feature Cargo run failed dev's unchanged 600-second first-build
readiness stage, two outbound HTTP cases, three external ingress cases and the
facet case. The all-feature Node binary reported five failures and then stopped
producing terminal results for its remaining event cases for over ten minutes.
It was terminated so `--no-fail-fast` could continue other executables; this is a
failed, incomplete gate. A one-second OS stack sample collected data but stalled
while resolving symbols and supplied no usable diagnosis. Memory compression and
other compilation were observed, but neither establishes the cause. No test
deadline, fencing rule or shutdown assertion was weakened. Exact-binary replay
and final marker qualification followed, as recorded below.
The later replication case failed with `Resource temporarily unavailable` and
Axum errors, host-parity, lifecycle and SDK-events suites also failed startup or
ingress checks. The full command exited 101 with ten failing targets. Passing
individual replication checks earlier in that run do not establish a passing
replication suite. An exact-binary serial Node replay followed independently
with unchanged per-operation deadlines and drain proofs.
The exact all-feature Node executable then passed all 24 tests in isolation with
serial test scheduling (194.28 seconds), including the five prior failures, the
source-inventory regression and all drain assertions. This scheduling comparison
does not prove the cause or turn the failed concurrent run into a pass.
Serial exact-binary replays also passed external ingress (5), facets (1), Axum
errors (1), host parity (4), lifecycle (2) and SDK events (1). Outbound HTTP
retained both deadline failures (3/5 passed); the supplied-credential replication
case again returned OS error 35 (3/4 passed). These two failures remain
reproducible in isolation. The dev build case was not repeated in this replay.
Final KV, CLI and artifact tests rebuilt the snapshot containing the
telemetry/KV version-1 edits with bounded compiler concurrency and passed (one
KV case, two CLI cases and six artifact cases). The KV integration retains its
large-value restore, orphan cleanup, failed deletion, concurrent sweep/put and
later-epoch protection assertions under the `v1` reference/object layout.

Final all-target/all-feature Clippy passed with warnings denied, and all 11
workspace doctests passed. Formatting checked 413 Rust files; documentation
validated 35 documents, 6 flows and 370 references. The all-feature dependency
graph retains 401 reachable packages with no JavaScript engine; whitespace checks
passed. These successes do not supersede the failed/incomplete broad runtime gate
or establish full release qualification.

## Runtime qualification diagnostics (2026-10-13)

The first instrumented native HTTP replay reproduced its 30-second deadline
while the first `network` call was pending. Service fetch/RPC/stream, loopback,
restricted environment and broker routing had completed. A later stage-marked
run passed, and all three replays with client-builder boundary markers passed.
The diagnostic native S3 case passed; generic store construction took about
2.4 seconds of the existing peer's 3-second accept window. An isolated client
construction probe measured native-certificate builds at about 0.74 and 0.16
seconds, while disabling that one input completed below the log's millisecond
resolution. These observations do not establish the cause of the earlier
HTTP deadline or S3 error. No production trust configuration, client factory,
credential provider, timeout or proof policy changed.

Temporary client/run logs and the construction probe were removed. The HTTP
fixture now reports a fixed phase label on its original deadline error, and
S3 accept/header failures identify the reader kind and request attempt. Final
serial HTTP and replication suites passed all 5 and 4 cases respectively;
focused all-feature Clippy passed with warnings denied. The failed broad gate
and earlier isolated failures remain part of the evidence; these diagnostic
passes do not prove a fix or establish full release qualification.

Catalog inventory is the next implementation priority. Namespace inspection
needs its own host-minted capability bound to tenant/application, catalog scope
and the read-only listing method. Existing stream administration remains
resource-scoped and must not mint that capability. Inventory must use bounded
primary-key pages, retain deleted identities, expose only lifecycle/identity
metadata, and use namespace-bound continuations. It cannot substitute for
whole-fleet producer discovery or a coordinated retirement proof.

## Catalog namespace inventory (implemented; release qualification incomplete)

Adds `cellar stream list --tenant/--application`, signed `/catalogs/` ingress,
namespace-bound host RPC authority and bounded scalar metadata pages. Resource
administration cannot mint the namespace capability or invoke the listing method.
Current identity/lifecycle, operation and accepted/desired revision columns commit
with definition transitions; listings copy no configuration, grants, tokens or
payloads. Cursor and response acceptance bind namespace, order and page tail.
The catalog schema remains 1; a prior layout lacking metadata columns is refused
before base schema mutation and must be recreated. No migrations are introduced.

Compilation and targeted qualification passed. New checks cover bounds, cursor namespace,
decimal revisions, sensitive-field rejection and method/scope capability limits.
The real bucket-backed Node fixture creates three resources, retires one, resumes
one-record pages after owner handoff and requires full original shutdown proofs.
The reset/replication fixture initializes the current scalar catalog columns while
preserving its existing reset and restore assertions. Whole-fleet inventory,
metrics, source destruction and the full fault/capacity matrix remain open.

The first routed integration run failed with a remote RPC error. A direct-owner
probe reproduced `InvalidRequest: event RPC arguments must be a single object`:
the new request producer had omitted the event protocol's one-element argument
array. After correcting that, the direct-owner result exposed a missing operator
`ok` to `result` normalization. Both contracts are now exercised through the real
event decoder and response projection, including malformed reply envelopes.
The original two-node test then passed (7.49 seconds), with retained tombstones,
one-record continuation after handoff, unsigned-request refusal, namespace/path
mismatch refusal and complete original shutdown reports. No deadline or proof
assertion was changed.

The wider targeted gate passed 88 library, 6 artifact, 2 CLI, 1 catalog delivery/
retention/revocation and all 25 Node tests (229.46 seconds for the Node suite).
Workspace all-target/all-feature Clippy passed with warnings denied. Formatting
checked 417 Rust files; documentation validated 35 documents, 6 flows and 370
references; the dependency graph remains 401 reachable packages without a
JavaScript engine. All 11 final workspace doctests passed. This is targeted qualification,
not a new uninterrupted full workspace runtime pass or release completion.

## Event host observations (in progress)

Adds an execution-domain-owned fixed-size metric registry, public
`HostServices::event_metrics()` snapshots and fleet-authenticated internal
`GET /event-metrics` (Prometheus text) / `?format=json` exports. No cell, event,
stream, subscription, payload or caller labels enter the registry. Counters and
histogram buckets saturate rather than overflowing; JSON counters/sums preserve
full integer precision with decimal strings. Timers retain their original host
clock and registry across cancellation in another domain. Missing clocks after
closure are counted explicitly rather than replaced by invented elapsed time.

Current observations cover dispatch RPC outcomes/duration, local append and
deduplicated record counts, per-new-record source timestamp to local append age,
ack processing/replay/duration, cursor-expiration observations and due-discovery
listing failures. Active waiter/inflight dispatch gauges use existing bounded
reservations. These are host-lifetime observations, not durable acknowledgment
receipts or a census of dormant/cold cells. Backlog, bounded blocked-reason,
stream allocated/logical byte and offset, active-lease, subscription-lag and
retention-blocked byte gauges still require authoritative owner-state snapshots.
The full metrics requirement therefore remains unfinished.

Two registry tests passed (saturation, exact export, bucket accumulation, host
isolation and cancellation/closed-clock attribution). The real two-node test
passed in 10.65 seconds: actual emission/append/ack appears in SDK and signed
JSON/text exports, unsigned reads are refused, sibling-host counters remain
untouched, and both nodes retain original complete shutdown assertions. The wider
targeted gate passed 90 library, 6 artifact, 2 CLI, 1 catalog delivery/retention/
revocation and all 26 Node tests (239.44 seconds for the Node suite). Workspace
all-target/all-feature Clippy passed with warnings denied; formatting checked
419 Rust files, documentation validated 35 documents, 6 flows and 370 references,
and the dependency graph remains 401 packages without a JavaScript engine.
All 11 workspace doctests passed.
No persistent marker or compatibility path is added. This is targeted
qualification; the complete runtime, fault and capacity matrix remains open.

## Subscription owner observations

Owner-routed subscription status and bounded administration inventory now include
sample time, logical unread offset distance, retained unread record count, oldest
retained unread append age, lost-history state and lease activity. Core arithmetic
handles the full signed offset range. Owner time clamps to the durable logical
clock without advancing it or expiring leases; each returned row adds at most
one primary-key timestamp lookup without payload copies or unread-range scans.
All integer fields use canonical decimal strings. SDK/operator acceptance checks
lag/age/lease consistency and target identity; retained deleted incarnations
remain inventory entries. The internal development markers remain 1, with no
legacy response fallback.

The core lag regression passed for empty streams, lost history, maximum offsets
and invalid checkpoints. The persisted stream fixture passed new assertions for
lease activity, acknowledgment clearing lag, retention leaving positive unread
lag with null retained age, and an expired stored lease remaining visible as
inactive. All 26 serial Node cases passed (233.93 seconds), including lag/age assertions
across real owner handoff and the original revocation, tombstone, payload-exclusion
and complete shutdown gates. Final source qualification passed 91 library cases,
the core lag test, workspace all-target/all-feature Clippy with warnings denied,
and 11 workspace doctests (one existing ignored case). Formatting checked 419
Rust files; documentation validated 35 documents, 6 flows and 370 references;
dependency checks retained 401 packages without a JavaScript engine, and
whitespace checks passed. All 2 CLI and 6 artifact smoke checks passed.
The latest published-head CI run (`51be79a`) could not start because its annotation
reports failed account payments or a spending-limit restriction. No CI success
or new uninterrupted full workspace runtime pass is claimed.

This adds authoritative per-subscription diagnostics, not aggregate gauges in
`/event-metrics` or a fleet/cold-cell census. Outbox oldest age and bounded blocked
reasons, stream physical/logical byte and offset observations, retention-blocked
bytes, aggregate lease/lag gauges, whole-fleet discovery, source destruction and
the full release fault/capacity matrix remain unfinished.

## Source backlog observations

Outbox inspection now includes sampled wall time, whole-source oldest pending
age, and seven fixed permanent blocked-reason counts. Lane metadata includes
its own oldest pending age and typed blocked reason. Blocked terminal seals
count even with no pending event payload; null age means no pending event rows.
Wall-clock skew clamps negative age to zero. These are source observations,
not cold-cell/fleet totals or aggregate series in the host metric export.

Per-lane row/byte and source reason counters update with emission, deletion,
block/retry and acceptance in the same SQLite transaction. Reason classification
uses typed permanent failure codes, never parsed human messages. Explicit
covering age indexes read at most one timestamp; lane inspection no longer
counts/sums its pending range. Restore audits counters and blocked states and
requires accounting triggers/age indexes. SDK and signed operator page acceptance
checks source/lane identity, totals, fixed reasons and age/cursor consistency.
The schema remains 1. Earlier development layouts require recreation; there is
no migration or legacy response fallback.

The first compile exposed a missing semicolon in the cursor helper extraction;
it was fixed before qualification. All 94 library cases, 6 artifact and 2 CLI
checks passed, including new accounting rollback/restore, corrupted counter /
missing index/trigger refusal, indexed query plans, decimal precision and forged
response cases. The persisted stream delivery/retention/revocation test passed
with the new layout. All 26 serial Node cases passed (237.01 seconds), including source retry,
handoff and removed-class recovery with current age/reason observations and the
original full shutdown gates. The native outbox SDK fixture initially failed its
whole-response equality check because new sampled ages advanced by one
millisecond. Its comparison now derives creation timestamps from those ages,
retaining every original metadata/counter/reason/wake-deadline equality check.
The exact SDK replay passed (0.20 seconds), including operation capacity and
immutable receipts. Workspace all-target/all-feature Clippy passed with warnings
denied; 11 workspace doctests passed with one existing ignored case. Formatting
checked 419 Rust files; docs validated 35 documents, 6 flows and 370 references;
dependency checks retained 401 packages without a JavaScript engine, and
whitespace checks passed. This is targeted qualification; the earlier failed
full workspace runtime gate and remaining release matrix are not superseded.

Complete aggregate export, stream physical/logical byte/offset and retention
observations, whole-fleet discovery, source destruction and actual process/
response-loss/follower/ownership/capacity release qualification remain open.

## Stream retention observations

Adds typed `Stream::stats` and owner-routed stream describe observations under
current administration grants: retained rows/bytes, earliest/end offsets, sample
time, active leases, expired subscriptions, oldest retained append age, deletion
frontier, protected bytes and age-expired bytes blocked against deletion. Typed
SDK/operator acceptance checks identity and metric consistency. Metadata is
bounded by the definition's 128-subscription cap; frontier/timestamp indexes and
exact offset reads avoid event-range aggregation.

Private modulo-2^63 byte-prefix positions measure exact retained windows below
the modulus without imposing a lifetime byte-admission ceiling. They are not
delivery identity counters; offsets and sequences remain checked/non-wrapping.
Byte accounting commits with append and survives prefix deletion/retirement.
Restore validates contiguous offsets, encoded sizes, timestamp order and byte
positions before base-schema mutation. The schema remains 1 and old layouts
require recreated development state without migration.

SQLite allocated/used page observations use a fixed framework helper after
activation checks. They are explicitly not WAL, local LTX, bucket, or filesystem
allocated-block measurements; complete physical usage and aggregate export
remain unfinished. Logical retention observations cannot substitute for those
remaining storage/capacity requirements.

The first gate passed existing library/CLI/artifact/outbox and core checks but
failed the new stream/Node description paths because SQLite denied the allocation
pragma query. It was moved to the fixed storage boundary helper rather than
broadening public pragma permissions. The persisted stream replay passed,
including age-blocked lease retention, required-checkpoint protection, empty
streams and post-reclamation observations. New metadata guard cases passed for
wrap, prefix reclamation, inconsistent counts/positions/times and missing layouts/
indexes, preserving business state before schema mutation. The corrected gate
passed 97 library cases, 2 core cases, 6 artifact, 2 CLI, 1 persisted stream,
1 native outbox and all 26 serial Node cases (230.76 seconds). Workspace
all-target/all-feature Clippy passed with warnings denied. All 11 workspace
doctests passed with one existing ignored case. Formatting checked 420 Rust
files; docs validated 35 documents, 6 flows and 370 references; dependency checks
retained 401 packages without a JavaScript engine; whitespace checks passed.
This is targeted qualification, not completion of the earlier failed full
workspace runtime gate or the remaining process-fault/capacity matrix.

## Actual process loss and independent receipt ledger

The durable-stream executable has an explicit `--fault-harness` mode with a
one-record required stream, source command/producer receipts, atomic consumer
receipt/effect archive and an owning-request hold after `db.sync` and before ack.
The ordinary walkthrough has no enabled hold/probe controls. Normal framework
ownership, discovery, replication, output, lease and shutdown gates are retained.

The independent client fsyncs its plan/attempts before issuing commands, records
source acknowledgments and payload digests, archives encoded-envelope digests,
and saves each original snapshot. A real proxy consumes the source's successful
HTTP response and shuts the client socket. Actual SIGKILL exits are checked for
an acknowledged source with pending capacity pressure and for proven consumer
local effects before ack; forced loss cannot supply a graceful stop report.
Every restart is audited before ambiguous source retry or consumer redelivery.
Retained lease expiry is observed before fresh fetch; redelivery adds no duplicate
local effect. Archived effect receipts account for intentionally reclaimed events.
The final projection equals its two distinct receipts and checkpoint 2.
The final host requires all original complete preserving-shutdown assertions.

The first process run passed both post-kill audits but its premature fresh fetch
failed while the restored lease was active. The harness now waits for the original
lease expiry and checks that expiry did not advance the checkpoint; no lease or
handler deadline was weakened. Three subsequent real runs passed four audits, two
SIGKILLs and one dropped source response. The final rebuilt executable digest is
`996b91316f82d53cb1aff53d285d7445ce383f07b58440775908cf72b4e11928`.
Evidence remains in `/private/tmp/cellar-stream-process-faults-20261014-d`, with
logs, the independent JSONL ledger, private state and `summary.json`. The final
normal executable smoke also passed. Nine oracle guards reject missing client/
upstream acknowledgments, unissued operations, changed payload/envelope/producer
identity, duplicate acceptance/effects, early checkpoints, missing archives and
premature/misordered replay. Audit-only rechecks of both passing journals passed.
Final workspace all-target/all-feature Clippy passed with warnings denied;
11 workspace doctests passed with one existing ignored case. Formatting checked
420 Rust files; docs validated 35 documents, 6 flows and 370 references; the
all-feature dependency graph retained 401 packages without a JavaScript engine;
whitespace checks passed. No new complete workspace runtime gate is claimed.

This qualifies actual single-node process loss and source HTTP response loss
under persistent SQLite-bucket assumptions. It does not qualify source loss
before the first dispatch attempt, destination append/ack response loss,
separate producer/destination/consumer hosts, followers, network/store/power
faults, every commit/proof cut point, capacity soaks or the full release matrix.
Those requirements, complete physical storage/aggregate metrics, whole-fleet
inventory and guarded source destruction remain unfinished.


## Terminal destination evidence for source destruction

The catalog owner exposes exact-resource terminal evidence through a distinct
read-only platform RPC capability, also used by `cellar stream retirement-proof
--resource RESOURCE.json`. It checks the immutable UUID, namespace, family and
name against the retained deleted catalog row and its matching committed
retirement receipt. The response contains only scalar resource identity,
operation, digest, terminal state and accepted revision. It does not read or
return grant/configuration/payload bodies and cannot configure, publish, retire
or inspect another namespace through the same capability. Platform verification
continues after application and original administration grants are revoked.
All Cellar-owned markers remain 1; no migration or mixed-build reader is added.

This is a prerequisite, not an implementation of whole-source destruction.
Closing emission and serving authority, capturing/rechecking lane generation,
persisting verified evidence with an audited discard receipt, bounded cleanup,
retained source tombstones and fresh-incarnation recreation remain unfinished.
Destination watermarks are unchanged by this read-only operation.


The targeted runtime gate passed 99 library cases, 6 artifact cases, the persisted
stream fixture and 25 of 26 serial Node cases, including the extended catalog
proof test. That test rejects live-resource evidence, revokes the original
application/admin grants, verifies terminal evidence through the platform
capability, rejects the revoked resource status grant and compares the exact
proof after owner handoff. An unchanged stream-control case failed shutdown with
`NodeLeaseExpired`; its exact isolated replay passed. The cause remains unproven,
so this is not an uninterrupted full runtime pass. Initial CLI validation used
an earlier binary while sources changed; the rebuilt CLI then exposed a new
test's incorrect expectation of an error code for plain UUID validation. The
assertion now checks the actual specific resource-incarnation error.

Workspace all-target/all-feature Clippy passed with warnings denied. All 11
workspace doctests passed with one existing ignored case. Formatting covers
421 Rust files; docs check 35 documents, 6 flows and 370 references; the all-feature
dependency check retains 401 packages without a JavaScript engine. This evidence
does not qualify source destruction or the remaining complete release matrix.

The final rebuilt CLI passes both existing test cases, including missing/invalid/
oversized resource input and rejection of inventory/binding options before fleet
access. No runtime deadline or shutdown assertion was weakened.


## Source loss before the first dispatch attempt

`examples/durable-streams/before_send.py` now qualifies this previously missing
single-node cut point with the actual compiled executable. The opt-in source
request acquires the ordinary `block_concurrency_while` input gate before its
business/outbox transaction. It calls `db.sync`, then exposes the proven command
receipt and a lane with zero attempts and dispatch generation. The observer
fsyncs its independent command plan and boundary evidence, verifies no destination
record and kills the still-pending process. Neither the source HTTP reply nor a
client acknowledgment is fabricated; the command is proven but ambiguous to the
original HTTP caller.

Before any framework restart, the client copies the killed source's complete
SQLite family and reads only that forensic copy. The original business state,
source incarnation, next sequence, pending obligation and zero attempts are
verified after SIGKILL. The envelope digest is recorded before retry. SQLite/WAL
hashes authenticate the archived durable bytes; SHM read marks are mutable
bookkeeping and excluded. After restart, the routed source/destination/effect/
checkpoint snapshot is audited before source replay or consumer processing.
Dispatch may recover without client retry. The command replay must return the
original receipt as a duplicate; destination offset/event/producer/payload and
encoded-envelope identity match the killed image. The consumer creates exactly
one receipt/effect and checkpoint 1, and final preserving shutdown uses all
original drain assertions.

The initial real run passed the boundary, post-kill forensic audit, recovery and
final effect checks. The final real run also authenticates the saved database/WAL
in audit-only mode. Evidence is retained in
`/private/tmp/cellar-stream-before-send-20261014-b`; the executable digest is
`034c7cd9d1e124141ff9791d2ab3cef65604d59b9ab81d90e91894a55752a90a`.
Its final summary reports two runtime audits, one actual SIGKILL and one effect;
the forensic pre-restart audit is additional. Sixteen oracle tests pass, including
nonzero pre-send/post-kill attempts, lost source state, changed incarnation,
missing pending/accepted work, premature effects/checkpoints and replay-before-
audit refusals. The ordinary executable smoke passes. The prior process fault
scenario also passes on this exact binary, with four audits, two SIGKILLs and one
dropped source HTTP response; evidence remains in
`/private/tmp/cellar-stream-process-faults-20261014-e`.

This is actual source loss before first send under persistent single-node
SQLite-bucket assumptions. It does not qualify separate-host destination/ack
response loss, followers, network/store/power faults, every cut point, capacity
soaks or complete release readiness. Whole-source destruction remains unfinished;
its serving/admission lifetimes must fence suspended contexts and retained handles,
in addition to durable closure, bounded cleanup and tombstone recreation.


Final workspace all-target/all-feature Clippy passes with warnings denied; all
11 workspace doctests pass with one existing ignored case. Formatting checks
421 Rust files. Docs validate 35 documents, 6 flows and 370 references; the
all-feature dependency check retains 401 packages without a JavaScript engine;
whitespace checks pass. Both final measuring journals pass audit-only rechecks.
This is targeted qualification, not a new uninterrupted full runtime gate.


## Source closure, destruction and recreation

The source control path now accepts guarded destruction and recreation operations.
Destruction commits its exact request/digest/operator, captured source incarnation,
source admission generation, lane snapshot, current work generation and initial
receipt in one transaction. Dispatch generations advance to reject older replies.
Cached admission closes only after that transaction commits, before any await;
ordinary application constructors/handlers, SDK operations and suspended turns
cannot continue using the closed source. Retained storage/cursor/inbox/facet
handles keep their original source generation. Activation and instance reset
reconstruct the fence from durable state; recreation never revalidates old handles.

Framework controls and original outbox recovery remain admitted under private
host provenance. Each verification event makes at most one owner-routed retirement
proof call outside SQLite. Required evidence covers pending obligations and lanes
without an accepted terminal seal. Fully settled, accepted sealed lanes preserve
their live destination. Source incarnation, lane hash and captured work generation
are rechecked before recording proof. Every needed proof must commit before the
source can enter cleanup; verification evidence itself passes `db.sync` before
reclamation. Live destinations return `PendingDelivery` without discarded rows;
the accepted closure remains durable and can be inspected/retried independently
of its closed application. Drain/seal before destruction or retire every affected
destination through its independent administration authority.

Cleanup batches at most 1,000 event rows under a bounded encoded-byte budget,
then inbox receipts and lane/seal/batch-limit metadata in bounded batches. Business
storage uses the existing transactional reset, while the source administration
schema, immutable request identity and terminal receipt remain retained. The
receipt reports exact captured/discarded event counts and bytes and the original
reason/loss acknowledgment. Destination watermarks are not erased by source
cleanup. Recreation requires the exact deleted incarnation, retains the old
receipt, mints a fresh random producer incarnation and advances admission generation.
An old destruction replay returns its original terminal receipt without closing
that successor. CLI `outbox destroy/recreate` exposes this host path with explicit
confirmation flags; destruction additionally requires textual loss acknowledgment.
All Cellar-owned markers stay 1; prior control layouts require recreated state.

The two new Node cases cover a live destination refusal, closed admission across
owner handoff, independent destination retirement, exact discard counts, durable
receipt replay/conflict/operator checks and fresh identity/business-state reset.
The second case closes a sealed source with a live destination, drops an actual
suspended handler, rejects a retained storage handle after recreation in the same
activation and compares the unchanged live destination watermark. Its first run
exposed fixture ordering (seal before proven settlement), and the next exposed
missing required inventory parameters. The corrected fixture preserves every
original runtime deadline and assertion. A restore guard also refuses mismatched
work and missing retirement evidence before any base-schema mutation.

The first compiler pass found missing imports and an incremental Rust compiler
panic; compilation with incremental state disabled exposes normal diagnostics.
A later native reset branch needed explicit failure propagation because its
caller returns a turn tuple rather than `Result`. No cache cleanup, protocol
migration, proof bypass or deadline extension was used to hide those failures.

This implements and tests the base source database lifecycle. Independent
facet-family cleanup and recreation isolation, large SQL table-reset cost,
exhaustive source cleanup crash/lease/grant/policy races and aggregate/physical
capacity qualification remain unfinished. Full design completion is not claimed.


The larger targeted run passed all 28 serial Node cases, 6 artifact cases,
2 CLI cases, the persisted stream fixture, the native facet fixture and the
native outbox SDK fixture. The new restore fixture initially initialized an
event schema outside a transaction; the corrected library replay passes all
100 cases, including both pre-mutation refusal checks. The dependency-free
source admission test passes; all 11 workspace doctests pass with one existing
ignored case. Workspace all-target/all-feature Clippy passes with warnings
denied. Formatting covers 422 Rust files; docs validate 35 documents, 6 flows
and 370 references; dependencies retain 401 packages without a JavaScript engine;
whitespace checks pass.

The first attempt at these final checks failed with no space on the private
build volume. Only this checkout's disposable incremental caches were removed;
source, binaries, snapshots and other checkouts were preserved. Repeated checks
ran with incremental compilation disabled. These are targeted gates and corrected
replays, not a new uninterrupted full workspace runtime pass or completion of
the remaining source/fleet/capacity release matrix.


Final Clippy and formatting checks also pass after the restore-fixture correction.
The rebuilt ordinary durable-stream smoke passes. The real before-first-send
SIGKILL case passes again on this binary, with two recovery/final audits, one
kill and one effect. Its digest is
`1b8426e420b3d383d928522fbe3d056ff35dd56e5e92cd2236d8f77bfb15c7a4`;
evidence remains in `/private/tmp/cellar-source-lifecycle-before-send-20261014`.
This requalifies ordinary generation-zero SDK behavior and that cut point; it is
not source-destruction crash injection or independent facet-family qualification.


## Independent source facet-family reclamation and isolation

Top-level facet storage names now register in `__sys_cell_facet_names` before an
SDK open. The registry follows the source database's proof/recovery, survives
ordinary business reset and records children even when they are absent from the
current process. Source cleanup reads one registry key, proves the closed source,
then requests subtree deletion outside SQLite. The original source/operation/work
snapshot is rechecked before removing that key; interrupted/partial cleanup keeps
it available for recovery. The terminal source receipt is recorded only after
all registered families have completed.

The host serializes cleanup with begun opens through the root's opening lock and
checks resident owner epoch before deletion. Each batch lists at most 101 replica
objects and deletes at most 100 from one exact facet subtree. Local replicas and
dirty tails are closed/credited through the original backend deletion path; the
root database, its proof/receipt and sibling streams are preserved. Existing
ordinary facet deletion retains its API. Local directory removal remains a
separate filesystem cost requiring capacity qualification.

Recreated sources derive physical facet names from producer incarnation and the
logical user name. Public logical names, startup IDs and properties stay intact.
A delayed cleanup cannot target a successor's differently qualified namespace.
Native startup carries a captured source admission check into the host, including
after waits and restore; a stale parent cannot materialize a child. The permit
uses an opaque validator to avoid a recursive concrete channel/activation type.
Ordinary activation retirement retains its existing diagnostic wording, separately
from source-incarnation refusal. All owned markers stay 1; earlier outbox layouts
missing the registry are refused before base-schema mutation and require recreated
state, without migration.

The real Node source tests now write both child and nested-child state before
closure. The warm test verifies old local family removal and zero child values
after same-activation source recreation. The owner-handoff case verifies that a
new owner reclaims a registered child it never opened and that neither child nor
nested state reappears after recreation. Original handle, pending-turn, source
identity, loss-count, receipt and live-destination-watermark assertions remain.
A backend test seeds 205 replica objects and verifies remaining counts 105, 5 and
0 across three batches, while root/sibling/other-root objects retain their exact
bytes. A delayed native startup test advances source admission and verifies its
child file remains unopened.

Early checks caught a shared-table initialization guard error, a borrowed request
in the fixture and incorrect durable-namespace declaration of a facet class.
The bootstrap regression caught changed activation-retirement wording; the
original assertion/message is preserved rather than weakened. No deadlines,
ownership/proof checks or shutdown assertions were loosened.

The targeted runtime gate passes all 28 serial Node cases, 6 artifact cases,
2 CLI cases and the stream, native facet and native outbox fixtures. The corrected
library replay passes 101 cases, including bounded replica cleanup. Workspace
all-target/all-feature Clippy passes with warnings denied, and all 11 workspace
doctests pass with one existing ignored case. This is targeted source/facet
qualification, not completion of cleanup crash/lease/policy races, large local
filesystem/SQL-reset capacity, the full workspace runtime gate or the complete
outbox/stream/subscription release matrix.

Final library qualification includes the delayed source-startup refusal and passes
all 101 cases. Final Clippy passes with warnings denied; formatting checks 422
Rust files. Docs validate 35 documents, 6 flows and 370 references; dependencies
retain 401 packages without a JavaScript engine; whitespace checks pass.


## Interrupted source facet cleanup and explicit resumption

An authenticated replay of an unfinished destruction now re-arms the original
source's durable Outbox work before passing proof. Background cleanup failures
retain their five-minute outage backoff, but a deliberate operator replay can
resume after service returns. The original operation/digest/operator and source
incarnation checks still precede this mutation. A terminal replay returns its
retained receipt without scheduling work against a successor.

The serial Node lifecycle case injects a replica client that actually deletes
facet objects in the persistent SQLite bucket, records their exact paths in an
independent test ledger, then loses the deletion replies. Further facet listings
fail until the interrupted owner fully shuts down. Before replacement startup,
a separate caller-owned execution domain verifies every recorded object is absent.
The source remains `cleaning` with exact captured/discarded rows and bytes;
business calls and premature recreation are refused. The replacement has fresh
local state and no bindings to the retired destination. It reads the recovered
closure/counts before operator replay, completes cleanup, then proves root/child/
nested-child business state is empty after recreation. Original loss, ownership,
operator conflict, receipt replay and complete final shutdown assertions remain.

The case exposed that replay previously returned a nonterminal receipt without
advancing its durable outage deadline. The fixture also caught attempted graph
cloning, a direct bucket audit outside its execution domain and unsupported
adoption of a retired destination binding. These were corrected without changing
runtime deadlines, retirement adoption checks or shutdown expectations. This is
injected replica response-loss/outage plus graceful owner replacement evidence;
it is not SIGKILL, power-loss, expired-lease or isolated-owner qualification.
The focused case and all 28 serial Node cases pass (208.51 seconds for the
complete Node run). Workspace all-target/all-feature Clippy passes with warnings
denied; formatting checks 423 Rust files. Docs validate 35 documents, 6 flows
and 370 references; dependencies retain 401 packages without a JavaScript engine,
and whitespace checks pass.


## Bounded local facet reclamation

Source facet reclamation no longer invokes recursive `remove_dir_all` on an
async host turn. The injected `FileSystem::remove_dir_batch(path, limit)` contract
removes at most the requested number of files/directories and reports completion
only when the root is absent. The direct provider streams one directory entry
at a time, keeps one traversal path, closes each iterator before descending and
unlinks symlinks without following them. It can resume from the same root after
process-local state is lost. A zero budget is refused; an absent root completes
idempotently; other filesystem errors propagate. Custom providers default to an
explicit unsupported error and must implement the contract to reclaim sources;
there is no unbounded fallback.

The replication backend completes its at-most-100-object remote sweep before
admitting at-most-100-path local batches on the owning blocking executor. The
nonreplicated host deletion path uses the same injected method. Executor refusal
propagates without panic. Incomplete local work returns false, so the original
source registry key and durable cleanup obligation remain retained. Existing
ordinary facet deletion keeps its separate API and behavior.

Tests seed 205 remote objects and 205 local files under the same closed subtree.
Remote counts 105, 5, 0 are followed by local counts 105, 5 and complete removal;
terminal completion is withheld through incomplete local batches. Exact remote
root/sibling/other-root and local root bytes remain unchanged. A nested-tree test
reconstructs the direct provider each turn, independently inventories removals
and asserts no batch exceeds 17 entries, including directories. It verifies
symlink targets outside the subtree survive and root symlinks are unlinked.
Final qualification passes 102 library cases, both serial source lifecycle cases,
workspace/all-target/all-feature Clippy with warnings denied, 423-file formatting,
11 doctests with one existing ignored case, docs (35/6/370), dependencies (401,
no JavaScript engine) and whitespace checks. Unix symlink behavior is exercised;
the Windows-specific directory-symlink unlink branch follows the installed Rust
standard library contract but has not been executed on Windows. These
removal-count/memory bounds do not prove a
wall-time or byte-I/O bound for arbitrary directory depth, filesystem latency,
large individual unlink or SQLite business table reset. Capacity and abrupt-loss
qualification remain required.


## Proven destination append reply loss across embedded nodes

The Node suite now runs destination, producer and consumer as independently
started nodes with distinct data directories/execution domains over the private
persistent SQLite bucket. Source/consumer factory probes confirm business state
materializes on their intended hosts; the destination materializes neither. It
owns the warmed stream before the producer starts. Its advertised private peer
address points to a loopback TCP proxy forwarding unmodified HTTP/1 establishment
and tunneled request/response framing, including the original authenticated
handshake. No production receiver, output proof, routing or ownership gate is
replaced.

The proxy captures the complete successful append reply after the real destination
output gate. Before closing sockets, an independent measuring path verifies the
acknowledged command state, known `{value:1}` payload, exact source incarnation,
sequence 1/next sequence 2, one pending source row with settled prefix 0, and one
destination offset whose full envelope matches the captured request. Only then
is the loss released. Framework retry sends identical encoded request bytes and
receives identical acceptance bytes for the same resource/lane/accepted prefix.
Destination records remain exactly unchanged. A separate consumer commits/proves
an inbox effect, explicitly retries its batch, then redelivers/acknowledges with
one effect and checkpoint 1. All three nodes must complete original preserving
shutdown, and the proxy joins every connection. Runtime deadlines remain intact.

The focused case and all 29 serial Node cases pass (212.63 seconds for the full
run). Workspace/all-target/all-feature Clippy passes with warnings denied;
formatting checks 424 Rust files, docs validate 35 documents/6 flows/370 references,
dependencies retain 401 packages without a JavaScript engine and whitespace
checks pass.
This is actual peer append response loss across embedded nodes in one test
process, with in-process measurement snapshots and a persistent SQLite bucket.
It does not qualify independent physical hosts/process failures, saved fsynced
measuring ledgers, destination ack response loss, follower loss, power loss or
all other commit/proof cut points. Those release requirements remain open.


## Proven subscription acknowledgment reply loss

The real peer reply proxy now selects an armed RPC method and records method,
exact scope and opaque request/response bytes. Its helper was renamed to
`peer_reply_fault.rs`; it continues forwarding the original authenticated tunnel
and framing. Capturing an append does not consume an armed acknowledgment fault,
and vice versa. The original append-loss assertions are retained in the combined
three-node case.

After one proven inbox effect and explicit batch retry, the consumer redelivers
without repeating the effect. In that same inbox transaction it saves the exact
new batch receipt, then `db.sync` proves the receipt/effect before acknowledgment.
The proxy captures a complete successful `acknowledged` response from the real
destination. Independent status reads verify checkpoint 1 and no live lease
while that reply is held. Only then does the proxy close sockets. The consumer
reports reply loss or the transport's safe `AlreadyAcknowledged` replay; a
premature successful acknowledgment is refused by the test boundary.

Before explicitly replaying the receipt, the measuring client inspects the
retained processed total. `/ack-stored` uses the SDK's public subscription ack
method with the persisted original receipt, adding no local effect or new batch
fetch. Captured replay request bytes match exactly, including subscription UUID,
incarnation, generation, token and range; the reply is `AlreadyAcknowledged`.
A subsequent empty processing call retains one effect and checkpoint 1. Original
independent-host factory probes, exact append acceptance, runtime deadlines,
complete node shutdown and joined proxy connection assertions remain intact.
A compile check caught the held HTTP future retaining the consumer through its
shutdown move; explicit future completion/drop fixes that test ownership.

The focused combined case and all 29 serial Node cases pass (216.22 seconds
for the full run). Workspace/all-target/all-feature Clippy passes with warnings
denied; formatting checks 424 Rust files, docs validate 35 documents/6 flows/370
references, dependencies retain 401 packages without a JavaScript engine and
whitespace checks pass. This is actual append/ack network response-loss evidence between embedded
nodes over a persistent SQLite bucket, with in-process measurement snapshots.
It does not complete independent process/physical-host failures, fsynced saved
measuring ledgers, follower/isolated-owner/expired-lease/power loss, all commit/proof
cut points, capacity or the full workspace release gate.


## Refreshed executable fault evidence on f46b878

A locked all-feature build of `cellar-example-streams` at source revision
`f46b8788c0baa9632d641f90b682b8da026e66da` produces executable SHA-256
`c384539175bdeca45bb3b74eafe3b6d502dc0e623f431a0bf21594e9b245dfa1`.
The unchanged ordinary smoke and both process-fault drivers pass on that exact
binary. The process driver observes source HTTP response loss and two SIGKILLs
(pending acknowledged source obligation; proven consumer effect before ack),
with four independent saved audits, preserved original lease expiry, two distinct
final effects/checkpoint 2 and complete final preserving shutdown. The first-send
driver observes a proven but HTTP-unacknowledged source SIGKILL with zero attempts,
audits the killed SQLite/WAL image before restart and the recovered source before
replay, and finishes with one effect/checkpoint 1. Both audit-only commands recheck
the saved ledgers/image; all 16 oracle guards pass.

Private evidence is retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-stream-current-evidence-aylyo6uy`.
`qualification.json` records the source revision, build command, binary digest,
ledger hashes and exact scope; each driver retains its own summary, logs, private
state and ledger. This refresh follows the bounded local cleanup and current
append/ack embedded-node test changes. It is single-node persistent SQLite-bucket
process evidence, not independent physical hosts, followers, power loss, source
cleanup loss, arbitrary commit/proof cut points or capacity qualification.

The workspace feature-unified build subsequently produced SHA-256
`8d94fc339e1d29d8c9755ce1225a810fb7c4061c5993fc12a70b9e79b9acdaf0`.
That image is archived as `workspace-image/cellar-example-streams` inside the
same private evidence directory. Ordinary smoke, both actual process drivers
and both saved-ledger audits were rerun successfully against the archived image;
`workspace-process-faults` and `workspace-before-send` retain their fresh evidence.
The manifest keeps both build variants distinct rather than assigning earlier
fault results to an overwritten Cargo target file.

The first serial full-workspace library/integration gate passed the library and
native integration cases through the Cargo dev workflow, then stopped on two
loader-policy cases with `no output-gate channel`. Their runtime-only host never
installed a gate adapter because their parent previously made no storage writes;
durable facet-name registration now writes parent state. The fixture now serves
its per-domain gate contract like the neighboring native loader/cell fixtures,
tracks invocation and verifies facet registration actually reaches it. Its
runtime-only responder is not fleet durability evidence; production gate code
and all budget/domain/shutdown assertions are unchanged. The focused native
loader and all six loader-policy cases pass, including the invalid-environment
subprocess case. The corrected serial full-workspace library/integration gate passes: 99 test
reports, 391 passed cases, no failures or ignored cases. This includes all 29
Node cases and the actual Cargo dev workflow, plus the remaining SDK/workspace
targets. `workspace-tests.log` and its digest are retained in the private evidence
manifest. Workspace/all-target/all-feature Clippy passes with warnings denied,
11 workspace doctests pass with one existing ignored case, formatting checks
424 Rust files and documentation validates 35 documents/6 flows/370 references.
All 16 fault oracle tests and saved-ledger re-audits pass; whitespace is clean.
Full-workspace test success does not close the independent-host/follower/power/capacity or full
commit/proof qualification matrix.


## Name-selected deployment bindings with pinned delivery identities

`BindingConfig`/`ResourceConfig` now separate deployment selectors from runtime
`Binding`/`Resource`. Config JSON may omit `resource.id`; explicit IDs preserve
their serialized shape and require exact matching. Counts, fields, grant subsets
and a 64 KiB declaration encoding cap validate before worker construction.
Programmatic configuration call sites convert resolved bindings with `.into()`.
Stored lane and event formats retain mandatory UUIDs; all markers stay 1.

A private `event-name-resolution-v1` authority is bound to one catalog namespace,
selector, environment, principal and grant and permits only `__catalogResolveName`.
It reads one indexed nondeleted name row, rejects nonactive state, and checks the
actual durable grant. No provisioning/mutation capability is added. Requested
permissions live in a bounded RPC body (128 KiB including framing, 64 KiB decoded
JSON), not in the 16 KiB authority header; body identity must match the header.
The existing exact catalog/partition identity and grant checks repeat afterward,
closing lookup/recreation/revocation races through UUID checks. Only all-successful
resolution installs a once-pinned generation map before admission. A failed
candidate preserves the serving generation; reactivation cannot retarget its map.

Context stream handles clone resolved bindings. Emit/seal checks compare current
resolved identity, never a name wildcard, so retained old handles cannot publish
to a successor. Background obligations keep immutable stored binding identities.
The two-node case resolves external names, proves delivery, rejects missing names,
missing/revoked grants and wrong families, preserves serving code after failures,
then retires/recreates the name under another UUID. A new generation resolves the
successor while the retained old handle stays on its original UUID and emission
is refused; new delivery reaches the successor with exact value 2. Original node
shutdown and runtime deadlines remain. The fixture retirement request was fixed
to use the catalog's observed revision 3 rather than an invented successor 4.
Unit guards cover namespace/method scope, no authority promotion, maximum-size
subscription declarations without oversized headers, and once-pinned UUIDs.
Artifact metadata checks cover omitted versus malformed IDs.

The focused real-node case and complete serial workspace gate pass: 394 cases
across 99 reports, including 30 Node cases. After Clippy caught implementations
following test modules, item order was corrected without behavior/assertion changes;
final library replay passes 104 cases and Clippy-D passes for workspace/all-targets/
all-features. Formatting checks 426 Rust files; 11 doctests pass (one existing
ignored), docs validate 35/6/370, dependencies retain 401 packages without a JS
engine, 16 oracle guards and whitespace checks pass. Ordinary smoke and both real
process-fault drivers pass on archived workspace-image SHA-256
`83fb2875e434f1e47ceadf37a645230a65bb4ccd41d3c6f77d5dec3720fbb1f9`,
with saved-ledger audits passing. Private evidence is retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-named-binding-evidence-1baheykw`
with a manifest identifying the item-order-only correction after image qualification. Wider name lookup/revocation/adoption fault races and the existing full
process/follower/ownership/capacity release matrix remain unqualified.


## Independent-process destination append loss and recovery

The standalone stream executable has an opt-in bounded `--fleet-fixture FILE`
mode with unique node identity, shared private bucket path, loopback peer/advertise
addresses and explicit resource-declaration role. Producer, destination and
consumer run in separate processes/directories. Their factory-owned diagnostics
prove where source and effect state are actually hosted. Ordinary configuration,
all owned markers (1), original operation/handler/lease policies and complete
shutdown assertions remain intact.

The new `fleet_faults.py` forwards the real authenticated HTTP/1 tunnel and framing,
then holds a complete successful append reply. Its fsynced measuring ledger records
the original command, acknowledgment, payload/envelope identity and raw peer-byte
digests. Before observed destination SIGKILL, it audits one pending source row and
exactly one proven destination acceptance. Automatic append attempts cannot reach
the restarted destination until a separate recovery audit verifies that acceptance
survived. Retry sends identical request bytes and returns identical acceptance;
source settlement and consumer inbox redelivery finish with one effect/checkpoint.
Original/replacement process IDs, actual kill exit, stage order and each complete
final shutdown are required by saved-evidence audit.

The first run reached successful destination recovery/settlement but its first
consumer request hit a stale pooled RPC connection. The harness now audits effects
and checkpoint before repeating an ambiguous consumer operation; if a lease was
installed, it waits for that original expiry. Expected connection refusal is
recorded only during the deliberate outage/teardown. No protocol deadlines or
proof gates were relaxed. A subsequent run passes with one interruption audit.
Ten new oracle guards reject mutated original commands, lost proven acceptance,
retry-before-audit, wrong/foreign process identity, changed captured bytes,
missing reply loss, premature recovery/shutdown, duplicate effects and incomplete
final shutdown. All 26 existing/new Python oracle cases pass. Final diagnostic
pattern matching replaces an unnecessary unwrap; ordinary routes retain their
original response fields outside fault mode. Final Clippy-D passes for workspace/
all-target/all-feature, 11 doctests pass with one existing ignored case, formatting
checks 426 Rust files, docs validate 35/6/370, dependencies retain 401 packages
without a JavaScript engine and whitespace checks pass. The SDK production core
is unchanged from the preceding 394-case workspace gate; this batch qualifies the
fixture and measuring client directly rather than claiming another workspace run.

On archived executable SHA-256
`9fe1d1bb1dff2691f507e366440123bed4b2dc8b3fba1da6bdc51113ed279a76`,
the fresh independent-process case passes with five saved audits, three initial
processes, one replacement destination, one observed SIGKILL/reply drop and one
identical append retry. Ordinary smoke, both earlier process-fault drivers and
all three audit-only commands pass on the same image. Private evidence, binary,
ledger hashes, logs, source patch and scope manifest are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-independent-stream-parent-r5j0ub8t`.
The final surviving producer/consumer and replacement destination have distinct
complete preserving-shutdown reports.

This is independent-process evidence on one machine using a persistent SQLite
bucket. It is not separate physical hosts, network object storage, follower loss,
destination acknowledgment SIGKILL, power loss, source cleanup loss or complete
capacity/release qualification.

## Network-store destination recovery (2026-10-14)

The opt-in standalone fleet fixture accepts either its existing SQLite bucket
path or a bounded loopback S3 endpoint/name. The application explicitly builds
ordinary, paginated and no-retry conditional ownership clients. Test credentials
are captured only at this process boundary and never saved in fixture configs.
No production API, deadline, proof gate, schema or version marker changes.

`fleet_faults.py --rustfs-image IMAGE` owns a fresh Docker RustFS server, random
loopback port, random credentials and new bucket. Its ledger records backend,
actual image ID and server/bucket identity without credentials. Cleanup removes
only that container after the original producer/consumer and replacement
destination have returned complete preserving shutdown. The saved oracle rejects
missing, foreign or premature store cleanup. Its 27 guards pass.

The first attempt stopped during setup because RustFS health preceded S3 bucket
readiness (HTTP 503). Its partial evidence is retained; it is not a passed run.
The driver now uses the established fleet harness's bounded startup-only 503
retry. This does not change node ownership/replication timeouts or conditional
write retries. A fresh run passes against RustFS image
`sha256:040304b66e029a5cde4bed140b41513e925909839a9b912a40a98340610d1f66`.
Four saved independent audits prove one accepted event before destination
SIGKILL and after recovery, before allowing automatic append retry. Original
request/reply bytes match; source settlement, one inbox effect and checkpoint 1
survive. Actual process IDs, reply drop, replacement startup and all final
shutdowns are checked again by audit-only mode.

Archived executable SHA-256
`51c2e4e1c25a60a13553b80b9730c1113b3efdc2737796cea9a57d3e4f2b9889`
and private evidence are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-stream-network-evidence-cfq3dp2e`.
Ordinary smoke and the default SQLite fleet case pass on the same executable;
the latter has five saved audits, including one consumer interruption audit.
Both backend ledgers pass independent audit-only rechecks. Focused example all-feature
Clippy-D passes; documentation validates 35 documents/6 flows/370 references,
dependency checks retain 401 packages without a JavaScript engine and whitespace
checks pass. The previous 394-case workspace gate applies to the unchanged core;
this batch exercises the changed host and measuring harness directly.

This qualifies one separate-process destination append/reply-loss cut point
using a network object store on the same machine. It does not establish
follower-only proof, store loss, whole-fleet recovery without local data,
physical-host/power loss, acknowledgment SIGKILL, source cleanup loss, every
commit/proof cut point or capacity/soak qualification.


## Whole-fleet bucket recovery after local data loss (2026-10-14)

The opt-in standalone fleet configuration now captures `fleet_durability`
explicitly. Ordinary walkthrough and the existing append-loss driver retain
fleet mode; `whole_fleet.py` selects bucket mode on every initial/replacement
node. No SDK production core, deadline, proof gate or version marker changes.
The fixture JSON is internal and is regenerated on cutover.

The new case starts independent destination/producer/consumer processes. It
proves one accepted command and a retained consumer inbox effect, releases its
batch without acknowledgment and commits a second source command blocked by
required retention. A fsynced independent audit verifies both original source
receipts, pending accounting, immutable accepted event/effect and checkpoint 0.
Only then does the driver observe all three SIGKILL exits, inventory the stopped
local files and remove the three private node data directories. Store data,
source commands, measuring ledger and logs remain outside the discarded paths.

Replacement nodes recover the catalog/stream, business source/outbox state and
consumer inbox from the bucket. Before any source POST or consumer processing,
a recovery audit requires the same receipts/event/effect, checkpoint 0 and one
pending obligation. Source commands are never replayed in this case. Consumer
redelivery must add zero effects; ack releases capacity for automatic pending
source dispatch. Subsequent processing adds one new effect, final total 1100
across two receipts and checkpoint 2. Every replacement returns complete
preserving shutdown.

Both RustFS S3 and SQLite bucket modes pass four saved independent audits, three
actual SIGKILLs and three local node-directory discards. Audit-only rechecks pass.
The eight new oracle guards reject incomplete/foreign process loss, premature or
missing local discard/replacement, replay before audit, repeated effects, lost
accepted event/effect/pending source, mutated commands, incomplete shutdown,
wrong proof mode and premature/missing private store cleanup. All 35 original/new
Python guards pass. The existing fleet-mode append/reply-loss case passes five
audits on this image, and ordinary smoke passes. Focused all-feature example
Clippy-D passes, formatting checks 426 Rust files, docs validate 35/6/370,
dependencies remain 401 without a JavaScript engine, and whitespace checks pass.
Production core is unchanged from the prior 394-case full workspace gate; no
new complete-workspace run is claimed for this fixture/harness batch.

Archived binary SHA-256
`b813cd9d2e9451a2811f44ee801e49706cacb080da73fbaa9ab62a20dc276068`,
ledgers, file inventories, logs and source/configuration manifest remain at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-whole-fleet-evidence-oapnsvzs`.

This proves bucket-only whole-fleet recovery with lost local node data at one
selected boundary on one machine. The object store remains alive and both
caller executors in killed nodes are lost abruptly. This does not prove fleet
follower-only acknowledgments/recovery, object-store/power loss, separate
physical hosts, every commit/proof cut point, independent ack SIGKILL, source
cleanup loss or full lifecycle/capacity/soak qualification.


## Independent-process proven acknowledgment loss (2026-10-14)

The standalone opt-in consumer now stores an encoded exact batch receipt in
its effect/inbox transaction and syncs before remote ack. Its test diagnostics
expose only the receipt digest; a separate replay route decodes the stored
receipt and calls the SDK acknowledgment API without fetching another batch.
Ordinary routes, SDK production core, all proof/deadline/lease policies and
owned markers (1) remain unchanged. The existing real peer proxy can now target
append or ack while retaining authenticated tunnel/framing and complete reply
capture.

`ack_faults.py` starts independent destination/producer/consumer processes.
After source acceptance settles and is audited, the client holds the complete
ack reply and audits checkpoint 1, no live lease, the proven consumer effect and
receipt digest before destination SIGKILL. It drops the original socket reply;
the consumer records one new effect and an actual lost-ack outcome. Replacement
destination startup precedes a recovery audit of the same checkpoint/effect/
receipt. Only then does it release the proxy and replay the stored receipt.
Captured request bytes match the original ack; the SDK returns
`AlreadyAcknowledged`, with no new fetch, effect or checkpoint advance.

The first RustFS attempt recovered all audited state but then hit a stale pooled
RPC connection during exact replay. Its partial evidence is retained and excluded
from passed qualification. The measuring client now records transport ambiguity
and audits the unchanged checkpoint/effect/receipt again before retrying the same
stored receipt. Original deadlines and lease policies remain intact. A synthetic
corruption test also exposed shared objects between its fabricated stage rows;
the fixture now copies each snapshot independently so changing a recovery digest
cannot change its earlier evidence.

Fresh SQLite and RustFS S3 cases pass four and five saved audits respectively
(the RustFS case includes one replay-interruption audit), each with observed
destination SIGKILL/reply loss, one identical receipt replay and complete final
shutdown of all surviving/replacement processes. Audit-only rechecks pass.
Nine new oracle guards reject receipt/RPC substitution, checkpoint/lease/effect
loss, missing/wrong process or original reply loss, premature replay/restart,
mutated stored receipt/captured bytes, wrong idempotent status, duplicate effects,
incomplete shutdown, missing/premature private store cleanup and unaudited replay
ambiguity. All 44 original/new Python guards pass.

On archived executable SHA-256
`f521ac787753620af084f9957f73af174700f1889f80ff897777c54b061ecda2`,
ordinary smoke plus the previous fleet-mode append-loss (five audits) and whole-
fleet SQLite bucket-loss (four audits/three SIGKILLs/three local discards) cases
pass with saved re-audits. Focused example all-feature Clippy-D passes; formatting
checks 426 Rust files, docs validate 35/6/370, dependencies remain 401 without a
JavaScript engine and whitespace checks pass. Core is unchanged from the prior
394-case workspace gate; no new full-workspace run is claimed for this batch.
Private image, ledgers, logs, source patch and qualification manifest are retained
at `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-independent-ack-evidence-_aueu9u7`.

This qualifies one independent-process ack-proof/destination-loss boundary on
one machine with SQLite/RustFS storage. It does not qualify consumer process
loss while replaying a receipt, follower-only proof, object-store/power loss,
physical hosts, all lease/ack or source-cleanup cuts, full policy/lifecycle races
or capacity/soak release requirements.


## Bounded root-source fleet census (2026-10-14)

`cellar outbox census [CLASS]` walks the existing durable bucket cell-directory
index, then performs authenticated owner-routed source inspection. It needs no
new registration publication or state schema. Each call asks for one provider
page of at most 100 prefixes and runs at most four concurrent source probes in
key order. Each inspection requests one lane, validates its complete owner page
and projects only source identity, total pending event rows/bytes and fixed
observations. No lane/principal/grant/payload/receipt metadata is returned.

The `census1` continuation retains an opaque provider token, bound by a digest
to backend, bucket/prefix, endpoint and optional class. Input/decoded provider
continuations are bounded and canonical; malformed/foreign tokens fail before
listing/probing. Filtering cannot trigger more provider pages, so an empty page
may still have a continuation. Only stream/catalog classes are excluded; other
framework roots remain candidates. A current source, no current outbox identity
and an unavailable observation have distinct row states. Failed probes remain
explicit rows; the CLI prints the usable page/cursor, sets `complete_probes: false`
and returns a failed exit. It cannot substitute zero counts for unobserved data.

The actual two-process RustFS CLI smoke qualifies three one-prefix pages over
a settled source, a capacity-blocked pending source absent from destination
`stream sources`, and an empty cell. It requires matching original incarnation/
receipt scope and pending counts, no duplicate source or lane metadata, and
foreign-class cursor rejection. After complete node shutdown with the bucket
retained, a new CLI call must return all three unavailable rows without pending
counts, false complete-probe flag and a failed exit. The normal output/proof,
owner route and authorization are reused; no business SQLite is read or mutated
directly from the bucket. Cold inspection may restore/progress durable work.

All 106 library cases and both native CLI integration cases pass, including
bounds/class/cursor guards and help/input rejection. The first focused command
used the physical file name as its Rust test filter and matched zero tests; the
correct logical `outbox_cli::census` filter then ran/passed both guards, and the
full library replay passed. Workspace/all-target/all-feature Clippy-D, 427-file
formatting, docs (35/6/370), dependencies (401/no JavaScript engine), 44 Python
fault oracle guards and whitespace checks pass. This does not claim another
complete workspace integration/fault run. The build briefly filled the mounted
target volume; archived tested images were preserved, and only obsolete Oct-13
Cellar library/rmeta cache files in this checkout's target were removed. No
source/evidence/current binaries or other checkout caches were removed.

Private paired CLI/example images, fixture/ledger/logs, source patch and manifest
remain at `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-census-evidence-dr58nnxt`.
CLI SHA-256: `e2b880f98faabdbedce23f066a641e398188d45778ed4de9b19bb6ea1584b067`.
Example SHA-256: `feb746e3e580b472e79bc5b2d82392456fd1671843513cf5ec07cb0f53e372fb`.
All owned markers remain 1.

This supplies live root-source census for undispatched producers. It is not a
snapshot, all-producer drain proof, facet/historical source or catalog namespace
inventory, nor aggregate authoritative gauges. Other cloud backends, removed-
class/cold/facet coverage, ownership/listing races, scale/latency and physical
accounting remain qualification/implementation work.


## Quiescent protected retention maintenance (2026-10-14)

Previously, the oldest retained append time always selected an age deadline,
including when a required checkpoint prevented deletion. Once that age elapsed,
maintenance rearmed at the existing one-second minimum forever without possible
progress. Scheduling now reads one oldest primary-key record and compares its
offset against the same required-checkpoint/live-lease deletion frontier used by
sweeping. Protected prefixes have no age wake; lease cleanup keeps its expiry
wake. Checkpoint/retry/lease/lifecycle transitions already recompute work, and
accepted configuration now recomputes its deadline in that configuration's
transaction so changed retention or age limits cannot leave eligible work dormant.

The frontier reads the indexed minimum required checkpoint and at most 129 live
leases, rejecting more than the current 128-subscription bound. Scheduling clamps
wall time to durable logical time. Checked age-deadline overflow refusal remains
active even with a protected prefix, avoiding acceptance that could become
unacknowledgeable when protection lifts. Eligible expired records continue in
bounded row/byte sweeps with the existing one-second minimum; clearing this
reason leaves application alarms and other work intact. No schema, protocol,
version marker, source/destination identity or durability gate changes.

Four new production-SQL tests use a synthetic activation, not a fleet proof
adapter. They require no periodic work for a protected required prefix, progress-
triggered rearming, one-record bounded continuation, retained application alarm,
live/expired lease handling, configuration rollback/release, durable-clock clamp
and protected deadline-overflow refusal. All 110 library cases pass. The native
stream fixture now checks that a live lease's expiry is its only deadline and a
paused required checkpoint has no age deadline; it retains protection/age, receipt,
reclamation, authorization and lifecycle assertions. That integration case passes.

The first serial Node run passed 29/30 but cancelled bootstrap hit its original
five-second deadline and a worker panicked with `DomainClosed`. Its selected
original diagnostic is retained separately (not misrepresented as a complete raw
log). One isolated replay and five repeats pass without changes. A full replay
with backtraces passes all 30 cases in 256.57s. No cancellation fix, relaxed budget
or established root cause is claimed; investigate this observed failure before
closing release qualification. Event delivery/ownership/lifecycle cases passed
in both runs. Workspace/all-target/all-feature Clippy-D, 11 doctests (one existing ignored),
428-file formatting, docs (35/6/370), dependencies (401/no JavaScript engine),
44 fault oracle guards and whitespace checks pass. This batch does not claim another complete-workspace integration gate.

On archived CLI/example images, the real RustFS whole-fleet case passes four
audits, three SIGKILLs and three discarded local node directories. Independent
SQLite ack loss passes four audits, destination SIGKILL/reply drop and identical
stored receipt replay. Both saved audit-only checks pass. Real RustFS census
passes three one-prefix pages, undispatched/empty/failed-probe cases and scoped
cursor refusal. Ordinary smoke passes. Private evidence, images, initial failure,
isolated/full replay logs, source patch and manifest are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-retention-scheduling-evidence-4mi5bznc`.
Example SHA-256: `1c312594d0016938fd06fbdaff78cbf2ccd352bf556dea6573596f1fdb815530`.
CLI SHA-256: `d86d84fa4d574742fd2e58c8a6dc4be815935bf0a77f681ea7c60415e5bac6cd`.

This removes periodic age work that cannot reclaim a protected prefix. It does
not qualify hot-stream/large-metadata/wake fairness, complete clock/lease/config
races, follower/power/store/physical-host loss or the remaining capacity/soak
matrix. Aggregate/physical metrics and wider census remain implementation work.


## Fallible bootstrap/adoption work admission (2026-10-14)

Twenty more isolated original cancellation cases passed. The intermittent
five-second timeout remains unreproduced and its cause unestablished. The
public-node test now records an independent directory-lock probe before each
replacement attempt and labels failure as an old held directory or pending
replacement bootstrap. A diagnostic lock is released immediately; Node::start
still performs its own exclusivity check. Factory join, caller executors,
original five-second limit and every lifetime assertion remain intact.

A separate deterministic regression exposed a concrete panic hazard: an
ExecutionDomain::run poll can be admitted, then close before a later blocking
effect in that same poll. Both embedded generation build and metrics sampling
used infallible blocking admission and panicked with DomainClosed. The new test
captures that exact boundary, requiring a typed refusal and zero application-
factory/metrics-sampler invocations. Its first compile needed a test-only
Generation error extraction correction because Generation lacks Debug. Once
corrected, both old calls panic and the behavioral regression fails; that is
the baseline signal, not the earlier compiler error.

Private helpers now use try_blocking for bootstrap/adoption generation builds
and initial/periodic metrics sampling. Closed admission returns an error before
callbacks run; metrics maintenance ends on direct DomainClosed. Already-started
blocking work remains owned and joined before releasing the node directory.
No new task is allowed after closure, no deadline is increased and no durability,
lease, readiness, binding or protocol/schema marker changes. All owned markers
remain 1.

The deterministic test passes after the fix, as do all 111 library cases and
all 30 serial Node cases (275.51s), including the enhanced cancellation fixture,
real event append/ack loss, catalog/source/subscription handoff, removed-class
recovery, source destruction, generation adoption and complete shutdown proofs.
Workspace/all-target/all-feature Clippy-D, 429-file formatting, documentation
(35/6/370), dependencies (401/no JavaScript engine), all 44 saved fault-oracle
guards and whitespace checks pass. This is not another full-workspace integration or complete release
matrix pass. The earlier bootstrap timeout audit remains open; proving this
admission panic does not prove it caused that timeout.

Private twenty-run logs, selected baseline/fixed regression diagnostics, source
patch and qualification manifest are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-bootstrap-diagnosis-lvy5ulth`.
Other missing implementation and follower/power/store/physical-host, lifecycle,
policy, cleanup and capacity/soak qualifications retain their full scope.

## Selected offline source capacity qualification (2026-10-14)

An opt-in Backlog cell commits many distinct commands into one source/lane. The
fixture captures a bounded row quota, pauses actual append traffic and requires
OutboxFull for the extra command. Its business write and next sequence roll back.
Four audits verify every original command/receipt, pending accounting and producer
identity before the offline soak, before SIGKILL, after recovery before retry and
after complete delivery. No source POST replay repairs acknowledged state.

The one-record required stream forces batch-limit adaptation and consumer-paced
retention. Resumable cursors avoid rereading acknowledged history still retained
until the next capacity sweep. Each accepted envelope is independently recorded;
inbox effects are proved before redelivery, which adds zero effects, and exact
checkpoint progression is checked. All replacement/surviving processes report
complete preserving shutdown before the private RustFS server is removed.

The initial eight-command probe exposed a harness cursor error and was retained
as failed evidence. Its corrected SQLite case passed eight acceptances/effects
and checkpoint 8. The first 64-command RustFS probe exposed another harness error:
the proxy rejected unarmed StreamFull responses, hiding advertised batch limits
from the producer. That probe was interrupted and is not qualification. The
proxy now forwards unarmed errors unchanged, with a real socket regression;
armed proven-response-loss cases still require success.

The fresh RustFS case passed 64 commands, four audits, one producer SIGKILL, a
30-second offline soak, 64 acceptances/effects, checkpoint 64 and complete final
shutdown. Saved audit-only checks pass for both successful cases. Observed p95
producer response was 96.3 ms; consumer redelivery-plus-ack was 598.5 ms. These
are local case measurements, not performance guarantees. RSS, residency/cache
and bounded live local SQLite/WAL/LTX file observations are saved; allocation
does not measure unique physical extents or bucket bytes.

Example build and focused all-feature Clippy-D passed; ordinary smoke, all 52
Python oracle/proxy guards, 430-file formatting, docs (35/6/370), dependencies
(401/no JavaScript engine) and whitespace checks pass. No framework Rust changes
were made in this batch; this is not a new complete-workspace integration gate.
All Cellar-owned markers remain 1, with internal hard cutover and no legacy readers.

Private images, successful and failed evidence are retained at
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-outbox-capacity-evidence-xjhtzzjh`.
Example SHA-256: `b360d5a607778cd1e66c80497980626453f25d6df3ba96c5270e393e7f424a3b`.
This qualifies one source/lane at its configured row quota on one machine. Many
cold sources, long soaks, memory SLOs, compaction/restore capacity and the complete
follower/power/store/physical-host, lifecycle and policy matrices remain open.

## Many-source cold recovery reproducer: qualification fails (2026-10-14)

`many_sources.py` adds one independently acknowledged command/event per Order
cell and a saved oracle for distinct source/lane identities, protected acceptance,
remaining pending obligations, no source replay, exact effects/checkpoints,
resource limits and every final shutdown. A bounded opt-in `stream_rows` fixture
setting leaves ordinary examples and protocol markers unchanged. Counts cap at
192, stream rows at 20, residency at 128 and route cache at 256.

The first probes exposed harness assumptions. Redelivery can include additional
new records unless the protected window fills first. Source owners can move onto
other fleet nodes; a proxy cannot pause their local appends. The current case
therefore uses a full required-retention window for backpressure, accepts current
fleet owners with original identities, and restarts all three processes. Every
replacement is started before awaiting any readiness because fleet-mode recovery
can need a peer's retained log. Cleanup continues after a failed drain and closes
all owned processes/proxy/store/ledger; its regression verifies that behavior.

The corrected 144-command RustFS audit proved eight protected acceptances and
136 pending obligations before all three observed SIGKILLs. Source owners were
distributed across producer/consumer/destination under residency pressure. All
commands and receipts were independently audited before loss. The 30-second
protected-backlog observation retains RSS/residency/cache/local-file samples.
It does not prove successful recovery or consume any original event after loss.

Both fresh concurrent-restart cases (eight commands/SQLite and 144/RustFS) fail
resource activation with a peer SendRequest connection closed before completion.
The SQLite failure reaches producer/consumer activation; RustFS has two ready
replacements before the third fails. RustFS cleanup additionally reports one
pending dispatch even though ingress/work/cells are drained and no drain deadline
elapsed. The saved source/image/config/log evidence is a production failure
reproducer. Do not increase readiness/operation/drain deadlines, replay commands,
relax shutdown or switch durability modes to turn these cases green. Diagnosis
must distinguish recovering follower-only listeners, stale pooled connections
and ownership transitions. The earlier isolated bootstrap timeout remains a
separate audit; this evidence does not establish its cause.

Example build, focused all-feature Clippy-D, ordinary smoke, all 62 Python oracle/
proxy/cleanup guards, 430-file formatting, docs (35/6/370), dependencies (401/no
JavaScript engine) and whitespace checks pass. No full-workspace gate was rerun.
Current many-source process qualification is explicitly failed, not complete.
Earlier small producer-only probes and harness failures are retained separately
and do not satisfy the current all-process case or release matrix.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-many-source-evidence-jlt5pat1`.
Example SHA-256: `e09dfaa99b5e7a8aa8a7c8e02789ea2b8bf08b0c937104aae764ea5a2f20748c`.
The objective remains active with this recovery/admission failure at the next
priority; the full implementation, qualification and requirement audit retain
their existing scope.

## Activation-specific transport replay (2026-10-14)

The general RPC router correctly stops an ambiguous failed/truncated exchange:
it cannot safely repeat an arbitrary application handler. It previously flattened
that error's type before returning it, and event activation also stopped after
one exchange even though its configuration operation is durably idempotent and
its other methods only resolve/read state. Startup peer connection closure could
therefore end otherwise recoverable resource activation.

Remote RPC now preserves a private network-failure context for Reqwest/Hyper/I/O
errors, including interrupted response bodies. Header/protocol/authority failures
retain ordinary errors. General dispatch's ambiguity classification and refusal
remain unchanged. The activation helper alone repeats tagged transport errors,
using the same authority and encoded body; a fresh request ID avoids a previous
abort tombstone. Backoff caps at 200 ms under the original enclosing activation
deadline, with no detached retry owner. Successful responses still pass the same
resource/revision/digest/grant checks before binding pin/readiness. Permanent
delivery/protocol/codec failures and response-channel closure are not replayed.

The one-attempt regression compiled and failed with the tagged peer-closed error;
earlier test compilation corrections are not the behavioral baseline. Four final
regressions pass: network replay, unchanged general-RPC ambiguity, permanent
permission refusal, untagged protocol refusal and caller-deadline cancellation
with no surviving retry task. All 115 library and 30 serial Node cases pass
(281.98s for Node), including actual append/ack loss, ownership, removed-class
recovery, binding/retirement and unchanged shutdown deadlines.

The archived example passes fresh SQLite eight-source recovery: three SIGKILLs,
original protected history, autonomous delivery without source requests, eight
acceptances/effects, checkpoint 8 and all final shutdown reports. A RustFS
144-source replay also passes three SIGKILLs, 144 original acceptances/effects,
checkpoint 144, complete replacement shutdown and owned server removal. Its
drain was 105.78s on this machine; saved audit-only checks pass for both cases.
The ordinary example smoke and all 62 Python oracle/proxy/cleanup guards pass.
Workspace/all-target/all-feature Clippy-D passes (2m19s), as do all 10 Cellar
doctests, 430-file formatting, docs (35/6/370), dependencies (401/no JavaScript
engine) and whitespace checks. This is not another complete-workspace integration
or complete release-matrix pass.

The first RustFS 144-source run on this same image still failed the original
activation deadline, followed by a spawn panic at execution/domain.rs:668 after
domain closure. A diagnostic replay with RUST_BACKTRACE=1 passed instead, so it
did not identify the panic's caller. Neither replay establishes that deadline/
panic cause or closes the prior pending-dispatch shutdown failure. All failed
cases remain preserved and excluded from passing qualification. Large recovery,
late task admission, maintenance fairness and release qualification stay open.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-activation-replay-evidence-aj1xd665`.
Example SHA-256: `855eebf8a2c0fa80c8071bf309fb65171d58a0db514d60676506cf6feaf8be7f`.
All owned markers remain 1. This scoped fix does not complete the design's full
implementation, fault/capacity/lifecycle matrix or requirement audit.

## Startup wake pressure and closed-domain gated replies (2026-10-14)

The opt-in diagnostic 144-source RustFS run reproduced the activation deadline
and panic. RPC stage logs stopped at catalog configure/resolve. Host snapshots
showed all 16 activation slots in owner-log recovery with 113–119 additional
activations queued, while only one or two cells were resident. The backtrace
identified `host_turn::send_answer_after_arm_gates` called from native alarm
failure settlement after the execution domain had closed. This replaces the
earlier unknown-caller/unknown-pressure account with concrete evidence; it does
not establish the cause of the separate original five-second bootstrap timeout.

Reply delivery now uses fallible task admission. When a begun turn settles after
closure, refusal drops the reply value and discovery-gate receivers in their
originating domain. The returned closed completion remains visible to the driver;
no success or handler error bypasses an unproven gate. Both closed-poll tests
failed with the original infallible spawn; they pass after the fix, as does normal
admitted delivery waiting for discovery. Successful values drop exactly once.

Shared `node/wake_discovery.rs` orders boot/periodic discovery after the captured
generation's event-resource activation. Its first store read and first cold hint
cannot consume admission ahead of catalog/partition readiness. Embedded and CLI
bootstraps retain predecessor recovery, node authority and internal serving.
Readiness failure/cancellation cannot launch discovery; no-event hosts proceed
immediately. Durable wake publications are neither consumed nor discarded while
waiting, and the regular scan resumes after activation. Catalog configuration
can directly reconcile a retained pending operation before the general scan.
Tests cover pending activation, success, failure, cancellation and no-event hosts.

All 121 library and 30 serial Node cases pass (286.15s for Node), including
removed-class source recovery, native outbox append/ack loss, source retirement,
ownership and original lifetime/deadline/shutdown assertions. The diagnostic
helpers preserve the original readiness deadline and record host metadata only;
activation debug logs report method/stage/timing/success without reply payloads.
Temporary debug tags are removed. Diagnostic observations add traffic and are
kept separate from the final qualification runs.

The final archived image passes untraced RustFS runs with 144 and 192 independent
sources, each exceeding one host's 128-resident limit. Each audits original
accepted/pending state, observes all three SIGKILLs, recovers unchanged protected
history and delivers/consumes every original event without source requests before
completion. Exact inbox effects, checkpoints 144/192, all three complete final
shutdown reports and owned server removal pass; both saved audit-only checks
pass. Drains took 95.03s and 140.39s on this machine, not SLO guarantees. Ordinary
smoke, all 62 Python guards, docs (35/6/370), dependencies (401/no JavaScript
engine) and whitespace checks pass. The failed diagnostic/backtraces, baseline
and fixed regressions and all images/configuration/evidence are preserved.
Workspace/all-target/all-feature Clippy-D passes (1m56s), along with the final
121-library replay, all 10 Cellar doctests and 432-file formatting. These checks
do not substitute for another complete-workspace integration or release-matrix
run.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-large-recovery-diagnosis-dkejbs8w`.
Final example SHA-256: `ffd5dbd8f516552166c08b7014c92f4133added3024206325d84580181837515`.
These fixes address the observed startup-pressure and gated-reply panic paths.
They do not close general wake fairness, every failed-startup/shutdown/network
cut, the prior pending-dispatch failure's full audit, follower/power/store/physical-
host loss, broader capacity/SLO accounting or the full design requirement audit.
All Cellar-owned markers remain 1 and the full goal remains active.

## Current contract audit: implementation and evidence kept separate (2026-10-14)

This is a working audit of the complete design, not a completion claim. Historical
test notes above prove only their stated source/image/configuration and boundary.
The CI-equivalent workspace library/integration run passed 411 tests across 99
executables against `d9ab3d8`, before the subsequent read-decoder fix. The
compatibility instruction in the storage-model paragraph is corrected: all owned
markers remain 1 and development layouts use hard cutovers without migrations.

| Contract item | Current authoritative implementation/evidence | Completion limit or missing evidence |
| --- | --- | --- |
| One cell transaction; root/savepoint rollback; caught emit failure consumes no identity | `application/storage/outbox.rs`, `storage/sqlite/outbox.rs`, native_outbox and storage rollback/capacity tests | Full transaction-view escape, abandoned async transaction and unfinished write-cursor cut matrix still needs explicit qualification |
| Encode once under a byte budget; immutable bytes and source/lane identity | event_stream encoding, SQLite outbox, opaque base64 transport, exact peer-retry ledgers | Independent identity test vectors and every overflow/adversarial decoder case require audit; no global producer-order promise |
| Source accepted only after data and discovery; explicit sync covers both | outbox dispatch sync, wake projection/arm gates, source-before-send forensic harness | Every commit/publication/proof cut, follower-only proof and power/store loss remain open |
| No task alone owns delivery; removed-class recovery | persistent work generations, owner restore and recovery worker, native removed-class test; many-source all-process ledgers | Every owner isolation/expiry/source/facet recovery cut is not proven by these selected cases |
| Receiver authentication plus durable publish/read/consume/admin grants | host-minted Authority, runtime RPC provenance, catalog/store authorization and native revocation tests | Complete cross-tenant, revocation/reconciliation/adoption race matrix is not yet qualified |
| Creation cannot authorize itself; configuration intent, stable operation, proof and current-state readiness | catalog reconciliation, deployment resource validation, event_resources, creation-authority regression | Complete interrupted create/update and multi-resource failed-adoption cuts remain open |
| Name selection resolves a UUID once; old handles cannot attach to recreation | name_resolution, BindingConfig/readiness pinning, named-binding Node test | Wider concurrent name/adoption/revocation scenarios remain unqualified |
| Atomic contiguous append and durable dedup beyond retention | store append, cellar-logic decisions, retained producer watermarks, native stream and actual append reply-loss cases | Complete duplicate suffix/digest/offset exhaustion and follower-only/physical-host loss matrix remains open |
| Per-lane reservations, generation-fenced settlement and adaptive batches | bounded dispatcher/service and stored receiver hints; capacity harness | General cancellation/routing/fairness and all stale-completion interleavings remain open |
| Raw read cursor is independent of subscriptions and validates every returned batch | bounded/canonical server read plus repaired client acceptance, nine decoder regressions and native stream integration | Scope/identity/budget/continuity checks are qualified locally; complete codec and hostile-peer response matrix remain open |
| Fetch identity, lease authority, checkpoint and retention share one transaction | subscription_store and cellar-logic lease policies, native stream integration | Lost fetch reply, expiry/ack/extend/retry races and lost-wake/long-poll cancellation matrix not fully qualified |
| Exact ack and absolute extension; lost reply does not duplicate effects or progress | subscription receipts and actual embedded/independent ack-loss ledgers | Other consumer/follower/owner-loss cuts and full deadline/logical-clock matrix remain open |
| Unknown event schemas remain inspectable without automatic settlement | raw Envelope and fallible typed decode API | Audit all read/fetch envelope validation and hostile replies independently of decoder implementation |
| Local inbox effect/receipt atomicity, digest conflict, explicit bounded pruning | SQLite inbox and native/process projection cases | Two-consumer receipt race and every proof/pruning/crash boundary remain open; external effects require application idempotency |
| Bounded/required retention, lease protection, explicit cursor expiry, poison-event handling | stream retention/store and lifecycle controls; required one/eight-record capacity cases | Complete policy/clock/lease/maintenance interleavings and large protected metadata capacity remain open |
| No automatic correctness receipt/watermark expiry; explicit seal and tombstones | producer seal, subscription operations, catalog/stream retirement schemas | Metadata exhaustion and long-outage/recreation/late-packet full matrix remain open |
| Source destruction closes authority, proves terminal destinations and retains loss evidence | source_destruction, retirement proofs, native lifecycle and bounded facet cleanup tests | Every abrupt cleanup/lease/grant/policy cut and large SQL/directory reset capacity remain open |
| Ordinary reset preserves source, inbox and destination protocol state | SQLite reset preservation and PendingDelivery refusal tests | Full reset-vs-in-flight destruction/adoption interleavings remain unqualified |
| Startup/adoption readiness and finite owned work; no unproven output on closed admission | event_resources, wake_discovery and gated reply tests; untraced 144/192-source recoveries | General wake fairness, every failed-startup/shutdown/network wait and the prior pending-dispatch audit remain open |
| Supported host and hard-cutover compatibility contract | bucket-backed activation gate and version-1 markers; unsupported-layout and host-policy refusal tests | Qualified SQLite-only recovery/discovery is absent; external codecs and sequence/revision counters are separate |
| Typed caller, pressure, consumer and authority outcomes | DeliveryError/ErrorCode plus local policy failures | Host RPC now retains OwnershipLost/Ambiguous plus local/peer phase classification; complete SDK preflight, codec, timeout and cancellation mapping audit is still required |
| All envelope/metadata transport allocations stay within captured/hard budgets | bounded encoding, receiver budgets, subscription batch checks, repaired read acceptance, structured RPC ceiling | Complete codec/response-validation audit is not yet complete; global transport ceiling and caller decode budgets are separate |
| Logical bytes plus physical storage headroom/accounting | per-source/stream counters, SQLite page observations and live local file samples | Full WAL/LTX/bucket/filesystem accounting and restore/compaction headroom are not implemented/qualified as a complete profile |
| Export authoritative backlog, blocked lanes, offsets, leases, lag and retention gauges with bounded labels | fixed host operation metrics and owner-sampled source/stream/subscription diagnostics | Fleet root collector now exports checked owner-sampled durable gauges with coverage refusal; resident-host gauges, full physical profile and historical/facet inventories remain gaps |
| Bounded operators and complete resource/source inventories | signed source/stream/subscription controls, namespace catalog inventory, root census and destination watermarks | **Implementation gap:** fleet catalog namespace/facet/historical-source census is incomplete; pages are live observations, not drain proof/snapshot |
| Native declarations, feature validation, same local development protocol and examples | deployment adapters/Node activation, durable-streams native application and process harnesses | Complete packaging/dev-policy matrix and deployment/operator examples still need audit against the current SDK |
| Enumerate commit/proof/wake/append/lease/ack/retention/ownership interleavings | decision-core tests plus dedicated native and process cases | Full short-interleaving model enumeration and independent invariant coverage are not demonstrated |
| Exact release crash matrix across supported proof modes/backends | saved single/independent/all-process cases with original receipts and pre-replay audits | Follower-only, isolated/expired owner, storage/power/physical-host faults and all specified cut points remain unqualified |
| Capacity, lifecycle and operational release gates | selected row-quota, slow required consumer and many-source short-soak cases | Transaction/append profiles, long soaks, compaction/restore, general fairness, physical memory/storage SLOs and assigned operational owners remain incomplete |

Priority follows correctness and missing scope: complete typed failure
classification and aggregate/inventory implementation. Continue the original
failure/capacity/lifecycle qualification matrix and final requirement audit; no
single table row or passing scoped case closes the entire goal.

## Refreshed workspace gate and bounded read acceptance (2026-10-14)

The CI-equivalent command `cargo test --workspace --all-features --lib --tests
--locked -- --test-threads=1` passes 411 tests across 99 test executables on
`d9ab3d8`, with zero failed/ignored cases. It includes the real Cargo development
workflow, compiled daemon lifecycle, Node/event integrations, Axum durability/SSE/
WebSocket ownership, decision core, storage and formatter tests. This is evidence
for that exact pre-decoder-fix source state, not the entire release fault matrix.

The contract audit found that Stream::read trusted server-side budgets and used
direct integer parsing. Extracting the existing acceptance path and adding six
tests produced five behavioral failures: noncanonical positions, excessive batch
count, skipped offsets/next cursor, foreign envelope identity and ignored requested
position. A test-only borrow error was corrected before this behavioral baseline;
it is not counted as reproduction evidence.

The client now clamps outgoing budgets to captured host and hard protocol limits,
validates the requested cursor, and independently checks canonical decimal
metadata, resource scope, retained interval and contiguous offset advancement.
Returned count is checked before per-record processing; encoded size is checked
against event and remaining batch budgets before base64 allocation. Decoded size
and checked total accounting apply afterward. Source incarnation/scope, positive
sequence/version, timestamp, family/key limits, rederived lane and logical event ID
are validated before returning records. Wrong identity or malformed input cannot
create a valid next cursor. Read-only replay still changes no checkpoint and raw
unknown application event versions remain inspectable with original encoded bytes.

Nine acceptance regressions cover these failures, i64-end/above-binary64 positions,
unrestricted policy's hard record ceiling, invalid encoding and zero event version.
All 130 library cases and the native stream delivery/retention/revocation integration
pass, as do the rebuilt ordinary stream example and all 62 Python guards. The
post-change gate is scoped accordingly; the earlier 411-test workspace run must
not be relabeled as testing this later source. Full codec/subscription response
validation, typed OwnershipLost/Ambiguous mapping, aggregate metrics and broader
inventories remain work, along with the existing crash/lifecycle/capacity matrix.

Evidence root:
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-outbox-workspace-audit-4eom26wk`.
Example SHA-256: `5c46eae8349018dfa73a307bc8bd466b10e9a0bd9f001081eaf631b38a5594e5`.
The design's remaining migration sentence is corrected to the authorized internal
hard-cutover policy. All Cellar-owned markers stay 1; full completion remains
unproven and the goal is active.


## Typed event host failures preserve execution certainty (2026-10-14)

The named `OwnershipLost` and `Ambiguous` outcomes now exist in `ErrorCode`.
`node/event_failure.rs` separates pre-execution route refusal from a failed output
gate after execution. A fenced authority or exhausted stale route yields ownership
loss; a request refused before submission or a never-connected peer yields
unavailability. Lost submitted results, truncated reply bodies, unproven output
and otherwise unclassified dispatched failures remain ambiguous. Typed ownership
loss does not prove that an earlier attempt failed to commit.

The invocation event path uses the existing egress gate verdict before handing
its request to the host channel. Refusal cannot send an event packet; a lost
submitted result retains cancellation for its exact request identity. Ordinary
RPC uses its existing path. Authenticated peer host replies carry a bounded fixed
verdict header; missing/unknown verdicts cannot turn an unknown result into safe
non-execution. Existing DeliveryError codes and the private activation transport
tag remain in the error chain, so activation-only retries keep their original
policy/deadline and ordinary ambiguous RPC never gains automatic replay. Framework
protocol/schema markers stay 1.

Six new cases exercise unsent pooled request preservation, local/peer gate agreement, transport certainty and tag
preservation, unknown peer verdicts, zero submission after source-gate refusal,
and exact cancellation after a lost submitted result. The existing real peer
append/ack reply-loss integration now requires a typed ambiguous lost-ack outcome
before replaying the original persisted receipt. It retains independent acceptance,
checkpoint/effect audits, exact retry bytes and complete node shutdown checks.

The stricter real-network assertion initially failed: the caller received
AlreadyAcknowledged after the proxy dropped a proven ack. Source inspection found
that send_inner retried every pooled failure when its body was buffered, bypassing
the dispatcher ambiguity policy. It now uses Hyper 1.11.0 try_send_request and
retries only when take_message returns the unsent original request. Serialized
requests are never replayed by this pool, including ordinary RPC/fetch. Returned
requests retain their body and headers, including streamed bodies, without clone
or re-encoding. This corrects an observed implementation gap, not the assertion.
The failed gate is retained separately from the pending post-fix gates. The
post-pool integration then exposed an unframed-response parser gap in the proxy:
the required cancellation sends a valid bodyless 204 response, which the fixture
rejected and used to terminate its listener. Subsequent source retries became
Unavailable. The parser now accepts bodyless 101/204/304 responses while retaining
framing checks for ordinary responses; a real socket regression verifies that
the following framed reply remains intact. The strict lost-ack assertion and all
original pre-replay/receipt/shutdown audits remain required.

Final qualification passes 136 library tests (including the six new host/pool
cases) and all 31 serial Node cases (including the bodyless parser and strict
proven append/ack loss tests). The Node suite takes 276.10 seconds and retains its
original deadlines, ownership, receipt and shutdown assertions. Workspace/all-
target/all-feature Clippy with warnings denied, 436-file formatting, 62 unchanged
Python guards, docs (35/6/370), dependencies (401/no JavaScript engine) and whitespace
checks pass. Earlier passing results before the pool fix and both failed gates
remain separate; none is relabeled as qualifying the later source. The earlier
411-test workspace integration and 11-workspace-doctest gates predate these host/
transport changes and do not establish a refreshed complete release gate.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-outcomes-evidence-lnz62meb`.
All owned format/schema/protocol markers remain 1. A test-only
Debug-bound assertion error was corrected before this gate; it is not a behavioral
baseline. These host outcomes do not complete the full SDK preflight/codec/timeout/
cancellation mapping audit, aggregate gauges, complete inventories, physical
storage accounting or the remaining fault/capacity/lifecycle release matrix.


## Owner-routed fleet state collection (2026-10-14)

`cellar event metrics` now collects current durable outbox rows/bytes/age and
fixed blocked reasons, stream rows/bytes/offset sums, protection/retention bytes,
live leases, expired/live subscriptions, logical/retained lag and oldest retained
unread age. Root SQLite page allocation is counted once per observed root. Stream
partition counts include retained retired instances, with a separate retired count.
The platform StateAuthority is exact-scope/method bound, authenticated through
fleet ingress, and cannot authorize an application invocation or mutation. Native
sampling runs under the original owner's activation/output/discovery gates and
works through the framework handler, including removed-class recovery. It shares
the existing indexed stream observation calculations; metadata work remains bounded
by 128 live subscriptions with exact timestamp reads. Sampling never advances a
checkpoint, expires a lease or changes durable logical time.

The collector uses bounded provider pages (100 prefixes) and four concurrent
probes. Its root/page cap defaults to 10,000 and cannot exceed 100,000. Duplicate
or invalid directory entries, failed probes, failed/repeated listing tokens and
caps remain explicit incomplete coverage. No unreadable state becomes a successful
zero total: incomplete Prometheus emits coverage only; JSON uses gauges=null and
separate observed_gauges, and the CLI exits unsuccessfully. Complete means a
successful live root traversal, not a consistent fleet snapshot or drain proof.
Global labels contain no resource/cell/subscription identities. Canonical decimal
samples, complete fixed metric sets and accounting are checked before any total
changes; sums use checked u128, and age metrics take maxima.

Qualification is recorded after the final source/binary gates below. Earlier
140-library/31-Node results predate the last retired-partition, incomplete-output
and collection-acceptance refinements; they are not relabeled as final evidence.
The new path does not complete resident-host aggregate gauges, facet/WAL/LTX/
bucket/filesystem physical profiles, historical producer/fleet namespace inventory,
or the original fault/capacity/lifecycle/release matrix. All owned markers stay 1.


Final library checks pass 140 cases; the first broad Node gate passes 30 cases but
fails the existing cross-fleet admitted-write drain case with NodeLeaseExpired.
The same test passes isolated with unchanged one-second lease/eight-second shutdown
limits. Its cause is not established; concurrent compilation is an observation,
not proof of scheduling causality. Preserve the failed gate alongside the pending
complete no-build replay and targeted latest cold-first assertion. Do not relax
lease, readiness, durability or shutdown checks to conceal this failure.


The complete no-build replay passes all 31 Node cases in 292.42 seconds, including
the unchanged fleet-drain case. The latest targeted removed-class test also passes
in 48.37 seconds: the platform sample is the first direct source inspection after
restart and observes one source, one pending row and one Forbidden-blocked lane
without an Account implementation. Metrics authentication and lag-after-ack plus
ordinary/forced retired-partition sampling are covered by the Node gate. The
original lease-expiry failure remains unestablished, not erased by these passes.
Workspace/all-target/all-feature Clippy-D, 440-file formatting, 62 Python guards,
docs (35/6/370), dependency inspection (401/no JavaScript engine) and whitespace
checks pass. No fresh full-workspace release-matrix claim is made.

Archived CLI/example binaries pass the expanded RustFS census/metrics exercise.
The independent commands leave one capacity-blocked source obligation and one
protected stream record. Complete collection sees five roots, two producers, one
pending outbox row, one stream row, one record of logical/retained lag and exact
protected-byte accounting. A one-root cap yields incomplete coverage and gauges=null;
after both processes stop, all five state probes are unavailable, authoritative
gauges remain null and Prometheus omits backlog series rather than emitting zero.
The original source census, signed source inventory, shutdown/store cleanup and
independent measuring ledger checks remain. These are selected same-machine RustFS
process observations, not every backend, long-soak/fairness or power/store-loss
qualification. Complete live traversal is not a snapshot or drain proof.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-state-qualification-1omyqbhf`.
CLI SHA-256: `09970a182fa3f7f96ce58797b5e03c62328284ba8fcb4629c794df53676d94f0`.
Example SHA-256: `c270b64106215dc050ec96122ba90f6cecf094ed5b2e83aa1501cf21c1b0e76a`.
All Cellar-owned markers remain 1. Remaining resident-host metrics, physical
profiles, broad historical/facet/namespace inventories, SDK outcome/codec audits
and the complete failure/capacity/lifecycle/release requirements keep the goal active.


## Bounded fleet descriptor and retained history inventory (2026-10-14)

Whole-fleet root descriptor discovery now identifies catalog namespaces, retained
stream UUIDs and current source/history availability, without out-of-band tenant/
application selection. It reads one bounded provider page and four owner probes;
its inventory1 token binds bucket/prefix/endpoint/region. Root namespace identity
is rederived from scalar catalog metadata and checked against the hashed owner
scope. Empty catalogs report missing retained namespace identity explicitly.
Partial probes print unavailable rows/continuation and return a failed exit.

Scoped resource pages reuse the bounded validated catalog reader and include
tombstones. Producer pages reuse the bounded lane/watermark reader and preserve
source/lane identity checks, while exposing retirement/cleanup-complete flags.
Inspection found that retirement intentionally purges these rows after retaining
terminal identity/receipt; the implementation does not invent historical lanes
or call terminal absence an active empty producer set. Root source history reads
retained operation sequence ranges, including retries rather than scanning past
them, and projects old/new producer incarnations, lifecycle and loss counts.
The full receipt, operator reason/acknowledgment, token and payload are excluded.
Supported recreation preserves this ledger and its scope/sequence continuation.

A distinct exact-scope/method platform InventoryAuthority, signed ingress and
native framework handler retain original activation/output/discovery gates and
cannot authorize mutations or be replaced with metrics/resource authority. All
owned markers stay 1. Source/module namespace and borrow/build issues were corrected
before qualification; they are not behavioral reproduction evidence. Final gates
and archived process evidence are recorded below when complete. This does not
close facet inventories, every historical/backend/ownership/capacity cut, physical
profiles, host metrics, codec/outcome audits or the full release matrix.


The first behavioral gate passes 144 library cases and the native event integration,
but the new retired-partition assertion fails. An isolated run reproduces it;
inspection identifies a query for a nonexistent cleaned column. Retirement actually
records completion with finished_ms. The query is corrected to that durable field,
and unavailable ingress responses are structured JSON. Preserve both failed gates
separately; do not weaken the terminal-cleanup assertion or count compile fixes as
behavioral reproduction. Corrected full gates are recorded below.


The corrected final gate passes **144 library tests**, the native event-stream
integration and **all 31 serial Node tests** (279.05 seconds), including terminal
retirement cleanup and paged old/new incarnation history across source recreation.
Workspace/all-target/all-feature Clippy with warnings denied, 444-file formatting,
62 Python guards, documentation (35 documents/6 flows/370 references), native
dependencies (401 packages/no JavaScript engine) and whitespace checks pass.
A dependency-check invocation without Cargo on PATH failed before inspection;
the corrected PATH invocation passes. These are scoped gates, not a refreshed
complete workspace integration/release matrix.

The archived corrected CLI and example pass the actual RustFS census/metrics/
inventory exercise. Five bounded descriptor pages discover all five roots, recover
the catalog namespace and stream UUID, and agree with independently observed
producer watermarks. Scoped resource/producer pages, fresh empty source history,
foreign-fleet cursor refusal and all-five-owner unavailability after complete
shutdown pass. The ledger records owned store removal. This supplies same-machine
RustFS process evidence; facet/historical restore and the complete backend/owner/
failure/capacity qualification remain open.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-inventory-qualification-13ibixvc`.
CLI SHA-256: `deb9c040a2514985e9bcfbcb71ecd4ae29c86d21b8dd0ffd6f6f830c549c59b2`.
Example SHA-256: `484aaac7787e6707316a717409625f824fa36a5fd42162dac597cb1bf0c8db9b`.
Initial failed behavior logs remain archived separately from corrected passes.
Cellar-owned peer protocol, native-host protocol, wake format and schema markers
remain **1**, with hard development cutovers rather than backward compatibility.
The implementation goal remains active; the remaining requirement audit and
qualification must continue before release.


## Subscription reply acceptance audit (2026-10-14)

The client previously checked resource/name and receipt offsets but did not compare
subscription UUID/incarnation with the identity resolved for its fetch. It also
accepted unvalidated envelope identity and arbitrary issued/expiry intervals.
Fetch acceptance now checks all resolved receipt identity fields, exact nonempty
contiguous range, canonical timestamps and positive lease duration within captured
host lifetime. Raw and leased reads share one bounded envelope decoder, including
remaining-byte allocation checks and family/source/incarnation/lane/event identity.
Malformed base64/JSON becomes a typed InvalidRequest; unknown application event
versions and exact integer payloads remain inspectable. Retry policy/negative
extension deadline preflight and extension response lower-bound checks are included.
All owned markers stay 1. Current qualification results will be recorded below;
this does not close the full transport/codec/cancellation or lease/fault matrix.

The first library gate passes 150 cases and fails one new oversized-envelope
fixture: its size*8 string is still below the default host allocation bound, so
InvalidRequest for malformed JSON is correct. The fixture now exceeds the actual
captured host base64 bound. No acceptance check or error expectation was relaxed;
this failed fixture result is retained separately from final qualification.

A second library run passes 152 cases and reproduces the same oversized fixture
failure. Inspection finds the source still uses size*8: a concurrently running
formatter rewrote its earlier snapshot after the correction. The fixture is now
corrected only after that formatter is terminal; production acceptance code is
unchanged. Rebuilt native binaries from the final compile run separately while
the corrected library gate is prepared. Preserve both failed fixture gates.

Next audited priority: storage/sqlite/wake_record.rs still drops previous
alarm-only __sys_cell_wake_* triggers and installs the current work projection;
work.rs explicitly tests upgrading that layout. This is a remaining internal
compatibility path contrary to the hard-cutover policy. Replace it with explicit
old-layout refusal before base-schema mutation and retain fresh/current-layout
wake identity, reopen and rollback coverage. This finding is not yet fixed or
qualified by the subscription gates.

The corrected serial library gate passes **153 tests** (363.01 seconds), including
all nine new subscription acceptance regressions and the nine existing raw-read
acceptance regressions. Rebuilt native stream integration passes (12.43 seconds);
all **31 rebuilt serial Node cases** pass (293.26 seconds), including exact lost
append/ack reply handling and complete shutdown. Final formatting checks 445 Rust
files; 62 Python guards, docs (35 documents/6 flows/370 references), native
dependencies (401/no JavaScript engine) and whitespace checks pass. Final source workspace/all-target/all-feature Clippy with warnings denied
passes (11m10s). These are scoped library/stream/Node
checks, not a refreshed entire workspace release or process-fault matrix.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-subscription-acceptance-119lzq9p`.
It retains both failed fixture gates, lint-style failure, corrected source/logs
and archived rebuilt native binaries. No receipt, deadline, shutdown or caller
budget assertion is relaxed. Full transport allocation, cancellation, lease-race,
follower/power/store-loss, physical metrics and release qualification remain open.


## Alarm projection hard-cutover admission (2026-10-14)

The existing built legacy-upgrade test passes, reproducing acceptance of the
previous alarm-only wake projection. The new refusal regression fails against the before-change code with
"previous projection was silently upgraded", independently confirming the policy
violation. Its failed log is retained. The implementation
now checks for each previous __sys_cell_wake_* trigger at schema entry before DDL,
journal pragmas or activation writer-epoch updates. It returns typed
UnsupportedProtocol with development-state recreation guidance. The DROP TRIGGER
upgrade path is removed; all Cellar-owned markers remain 1.

The refusal regression checks untouched schema/journal mode, retained alarm time
and wake epoch/sequence, plus no installed storage after failed open. A positive
current-layout regression checks same-epoch reopen, independent alarm/work
removal, consumed tombstones, new-epoch sequence retention, and obsolete-work
refusal. Final scoped qualification is recorded below; this does not close remaining schema,
backend/follower/power/store-loss, maintenance/capacity or release-matrix work.


### Next SDK acceptance priorities from current-source inspection

These are concrete remaining checks, not completed qualification or new protocol
semantics. Audit all returned metadata before reporting successful mutation or
exposing receipt authority; preserve operation/fetch/receipt identity on retry.

| Surface | Current evidence | Remaining implementation/qualification |
| --- | --- | --- |
| Configuration and status (`event_stream/client.rs`) | Request definition/operation preflight; replies only deserialize Configuration | Bind returned resource/operation to request, recognize catalog states, bound error metadata, and test historical operation replay versus current status |
| Subscription lifecycle change (`event_stream/lifecycle.rs`) | Operation/previous identity equality and current resource/name validation | Validate returned action/reason/state/current UUID/incarnation transitions, checkpoint/skipped range/time, including reset/delete and replay |
| Stream audit (`event_stream/lifecycle.rs`) | Caller page bounds; returned AuditPage only deserializes | Enforce count/order/continuation, identity and receipt validity without exposing tokens; test malformed, repeated, out-of-page and foreign records |
| Retirement (`event_stream/retirement.rs`) | Resource/operation equality | Validate state, affected range, error/provenance bounds and transitions; preserve terminal loss receipt after cleanup |
| Retry (`event_stream/subscription.rs`) | Receipt preflight and captured delay cap; RPC reply discarded | Require canonical nonnegative retry_at_ms consistent with delay; malformed successful replies cannot count as accepted retry |
| Shared event RPC (`event_stream/mod.rs`, `error.rs`) | Host execution certainty is typed; bounded global codec elsewhere | Audit request/response allocation limits and codec/timeout/cancellation classification; do not confuse malformed submitted results with proof of non-execution |

The fetch/extension and raw-read gates above address their specific acceptance
rules; they do not establish correctness of these other operations. Prioritize
configuration/lifecycle/audit acceptance together so their invariants share one
reviewable set, then complete typed transport and fault/cancellation qualification.

The freshly rebuilt library test binary passes all **7 work/wake tests**
(0.15 seconds), including the new refusal and current-layout/tombstone regressions
plus existing rollback, alarm independence, restore and generation-exhaustion
checks. The binary and SHA-256 are archived separately. Full library/native and
Clippy gates are still live; these seven cases are not a substitute for them.

The broad final run passes **154 library tests** (11.84 seconds) and the native
stream integration (11.00 seconds), then finishes Node **30 passed/1 failed**
(344.43 seconds). The final predecessor-recovery case reports execution domain
is closed. It passes isolated on the same rebuilt binary with unchanged policies
and deadlines (10.74 seconds). The cause remains unestablished; do not label the
isolated pass a fix or attribute it to load without evidence. Preserve both logs.
Native outbox was not reached because Cargo stops at the Node failure; it runs
separately. A full unchanged Node replay is pending. Workspace/all-target/all-
feature Clippy-D passes (21m54s including its build-lock wait); 445-file formatting,
62 Python guards, docs (35/6/370), dependencies (401/no JavaScript engine) and
whitespace checks pass. The broader failure audit remains open.

The separate rebuilt native outbox target passes its transaction/authority/
publication-grant integration (0.46 seconds). Exact rebuilt stream/Node/outbox
binaries and hashes are archived. The unchanged full Node replay is still live;
its result cannot be inferred from the isolated predecessor pass.

The unchanged complete Node replay passes **all 31 cases** (305.32 seconds),
including predecessor recovery and complete shutdown. This passes the selected
current-source gate; it does not establish why the earlier 30/1 run failed or
complete its separate failure audit. No code, deadline, lease, receipt or shutdown
assertion changed between the failed full run, isolated pass and full passing
replay. The exact archived binaries, both full logs, isolation log and source
hashes retain that distinction.

Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-wake-cutover-f3ljgfrj`.
All owned markers remain 1. Old-projection refusal and current-projection identity
recovery are qualified by the before/after and full scoped gates. Next work is the
SDK acceptance audit above, followed by remaining transport/cancellation, physical
metrics, capacity/ownership/backend and full release qualification. The full
implementation goal remains active.


## SDK administrative response acceptance (2026-10-14)

Current SDK configuration replies now bind resource/operation and active accepted
revision/digest to the exact requested definition. Pending creation requires
revision 1; pending update retains an earlier accepted revision. Current status
can legitimately report a later operation, while historical configure replay must
match its original operation and definition. Known scalar metadata is bounded
before copying; error text allows the server's 1024 Unicode characters/4096 bytes.
No whole untrusted configuration object is cloned for acceptance.

Lifecycle receipt validation checks identities, operation/action/reason/operator,
state, incarnation preservation/invalidation, checkpoint/time and loss-range
consistency. Reset validates its exact target; the receipt does not carry the
previous checkpoint, so acceptance cannot independently reconstruct a skipped
range's old start. Audit pages validate receipt scope, principal/grant bounds,
unique operation IDs, ordered sequence/count/byte bounds and exact continuation,
while allowing legitimate sequence gaps and other authorized principals.

Retirement validates state, identity, operator/error/range bounds and terminal
range/count/byte accounting; initial pending intent need not yet carry partition
loss counters. Existing shared closure acceptance now rejects impossible byte/
row accounting and offset exhaustion. Retry can no longer report success after
discarding a malformed reply; its canonical nonnegative deadline must cover the
requested delay. All owned markers remain 1. Qualification is in progress; these
changes do not close full codec/allocation, execution-certainty/cancellation,
physical metrics, historical/facet/backend/capacity or the release matrix.

The initial library check passes **168 cases** (19.24 seconds), and the initial
workspace/all-target/all-feature Clippy-D passes (9m06s including build-lock
waits). These precede the final raw audit-count and partial-retirement-field
checks; they are not final-source qualification. Finished source adds two further
regressions (170 expected library cases). Final serial library/outbox/stream/Node,
source Clippy-D and formatter-check handles remain live. Docs/dependency/whitespace
and 62 Python guards pass. No initial result completes the outstanding gates.


The final pre-merge SDK run passes **170 library cases**, native stream, all
**31 Node cases**, and native outbox; final workspace/all-target/all-feature
Clippy-D and 447-file formatting pass. Exact source hashes and logs are retained
under `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-sdk-administration-9q78hy5_`.
Those results qualify the saved pre-merge source only.

PR #27 now incorporates main at `88886a8` through merge commit `6ccc964`.
The merge retains both dashboard/cron management and event operator routing,
reply budgets, RPC authority, outbox modules and version-1 cutover assertions.
The published merge alone passes **206 Cellar tests** (162 library, 3 CLI,
39 Node, stream and outbox), **24 dashboard/FleetCare tests**, workspace
Clippy-D, 489-file formatting, 64 Python guards, docs (35/6/373), and the
403-package/no-JavaScript-engine dependency audit. Merge evidence is retained
under `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-pr27-merge-evidence-2ebna8hu`.
The SDK batch was restored byte-for-byte except for AGENTS' merged main guidance.
Combined SDK/merge qualification is running separately; neither earlier run
proves that combined source.

The next shared reply boundary audit finds two concrete gaps in
`event_stream/error.rs` and `event_stream/mod.rs`: `unwrap_reply` clones entire
success/error values and accepts a reply carrying both branches, while structured
response decoding and invalid error envelopes can escape without a typed
execution-certainty verdict. These are post-submission results; malformed bytes
cannot establish that a mutation did not commit. Preserve valid protocol errors
and their bounded pressure/cursor metadata, reject contradictory envelopes, and
classify undecodable results as `Ambiguous` without copying payload text into
messages. Full transport allocation, cancellation and cut-point qualification
remain distinct requirements.


## Enumerated core interleavings (2026-10-14)

`crates/cellar-logic/tests/event_interleavings.rs` adds two exhaustive six-event
permutation scenarios (720 orders each). The wake case varies three delayed PUT
confirmations across predecessor retirement, tombstone retirement and cache loss.
Its independent persisted-key set forbids either old certificate from deleting
the successor's epoch and requires republishing to retain its exact identity.
The subscription case varies acknowledgment/replay, expiry, extension, required
retention and committed incarnation invalidation. An independent decision table
checks advance versus duplicate/reset/expired outcomes; retention cannot delete
unacknowledged required history. These exercise actual pure-core methods.
Qualification is pending. They do not model the SQLite transaction, proof or
output gate that authorizes those facts; combined commit/proof/append/ownership
model enumeration and the full crash matrix remain required.


The combined SDK/merge library passes **178 cases** (54.42 seconds).
The first native stream execution fails at the analytics receipt unwrap
(`native_event_stream.rs:886`, 4.24 seconds); the fixture did not retain the
actual error response. The exact unchanged binary then passes isolated
(12.10 seconds) and in a full serial replay (36.04 seconds). No validator,
retention window, lease/deadline or assertion changed. The one-second history
window is a hypothesis, not an established cause. Preserve all three logs;
this remains an open release failure audit. Rebuilt native outbox passes
(12.68 seconds). Workspace/all-target/all-feature Clippy-D passes (17m11s,
including the lock wait), and 492-file formatting and docs (35/6/373) pass.
The complete Node run is still live and has reported a bootstrap-cancellation
failure; its terminal diagnostics and unchanged verification remain pending.
These results do not establish a complete passing combined SDK gate.

Both core enumeration cases pass **1,440 orders** in 0.02 seconds; the targeted
Clippy-D gate passes too. The 10m17s Cargo elapsed time includes its shared build
lock wait. Keep enumeration scope separate from SQLite/proof/network/crash tests.


The rebuilt Node binary finishes **38 passed/1 failed** (490.80 seconds):
`cancelled_bootstrap_retains_directory_until_started_application_factory_has_joined`
expires its unchanged five-second restart deadline after sampling the old directory
as unavailable, with replacement startup pending. That sample does not prove the
lock stayed held throughout replacement startup. The same binary fails isolated
(46.26 seconds) with the same message. All event-specific Node cases pass, but
that does not qualify the failed lifecycle gate or establish its cause.

A diagnostic-only fixture change distinguishes replacement inputs from
`Node::start`; it performs the same calls and preserves deadlines/assertions.
The rebuilt diagnostic case passes once (33.53 seconds), then the unchanged
repeat fails earlier (10.70 seconds) on the initial factory-entry channel timeout.
No production fix is claimed. Both failure points require tracing; setup/executor
or filesystem delays and unfinished cleanup are hypotheses, not proven causes.
The SDK batch remains uncommitted pending that audit. The reply-boundary follow-up
is specified in `/tmp/cellar-event-reply-boundary-plan.txt`; it is not implemented.

Core enumeration is published in **86b7fe8** on PR #27, which remains mergeable.
Current combined evidence is retained under
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-sdk-merged-evidence-7im_h1n4`.
The full implementation goal remains active, including unresolved native failures,
shared reply/codec/cancellation checks and the complete release matrix.


## Shared submitted-reply boundary (2026-10-14)

Two new regressions fail on the prior decoder: success clones its payload, and a
contradictory ok/error object retains a permanent Forbidden verdict. The new
shared decoder consumes its sole branch, bounds wire bytes at 64 MiB before
JSON/structured decoding, bounds encoded error metadata at 16 KiB and message
text at 4096 bytes, and maps malformed or unprojectable submitted results to a
generic Ambiguous without reflecting body text. Valid protocol codes and exact
metadata remain intact. Both bound and authorized event calls use this boundary;
pre-submission argument errors and host execution-certainty verdicts are unchanged.
Seven regressions cover moved success allocations, null results, contradictory
and invalid envelopes, error budgets, structured/JSON success and protocol errors,
invalid bytes/capability/remote-error/cyclic results, and an oversized valid JSON
success. Final-source qualification is pending. All owned markers remain 1.
This does not qualify every operation-specific acceptance error, transport body
allocation, timeout/cancellation or crash boundary.

Startup diagnosis retains the repeated initial-factory and restart deadline
failures. Bounded macOS sample/debugger probes returned no stacks; their owned
sampler/debugger processes were stopped at 20/30 seconds, and the debugger child
was resumed. Debugger-timed runs are diagnostic only. Temporary tagged
[DEBUG-outbox-startup] traces now distinguish directory acquisition, reload
certificate, blocking generation admission/entry/completion, root completion,
domain join and directory release. A fixture subscriber captures only that target.
The native stream fixture now reports absent-receipt error metadata instead of
panicking without its response. Original calls, deadlines and assertions remain;
these are instrumentation, not production fixes, and must be cleaned up after
root-cause evidence is obtained.


The peer success path at `node/mod.rs` still uses unbounded
`response.into_body().collect()` before the SDK decoder. Bound event/control
collection itself at the shared wire ceiling. A length-limit refusal is a
submitted-result Ambiguous and must not carry RemoteRpcTransportFailure:
activation retries only that network tag, never malformed/oversized protocol
results. Genuine body transport errors must retain the tag and execution
certainty. Preserve caller cancellation precedence and post-head abort-disarm
semantics for already-proven handlers and their waitUntil work. Add chunked
unknown-length/declared-size/error tests and verify no suffix collection after
budget refusal. This is the next concrete transport implementation gap; the new
SDK decoder alone does not close it.


The instrumented reply-boundary run passes **185 library tests**, native stream,
**39 Node tests** and native outbox (226 cases total); workspace Clippy-D passes.
All five unchanged traced bootstrap repeats pass. Traces show the expected passing
order: factory completion, old-domain join and directory release, then replacement
build. They do not capture or establish the earlier failures' causes. Evidence is
retained under `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-reply-traced-evidence-996bp16y`.
The owned sample/debugger attempts returned no stacks within their bounded waits;
no production fix follows from those attempts. A transient build-volume observation
of 13 GiB free, later 40 GiB, is not evidence of failure causation. No unrelated
process or cache was stopped or cleaned.

Temporary startup tracing and its fixture subscriber are removed. The final
untraced source passes **all 226 selected Rust cases**: 185 library (28.65s),
native stream (12.02s), all 39 serial Node cases (370.95s) and native outbox
(0.32s). Workspace/all-target/all-feature Clippy-D passes (12m15s including build
lock wait); 493-file formatting, 64 Python guards, docs (35/6/373) and whitespace
checks pass. The native fixtures retain only clearer failure diagnostics, with
unchanged calls, deadlines and assertions. The seven reply regressions have
before/after evidence: both initial regressions fail on the old implementation;
all seven pass after the shared decoder change. This qualifies the administrative
acceptance and outer decoder batch, not the whole process-fault or release matrix.
Earlier native failures remain recorded and unexplained; no passing replay is
called their fix. Next implementation priority is bounded peer response collection
without false network retry tags, followed by cancellation and remaining release
cuts, physical metrics and inventory/recovery/capacity gaps. The full goal remains
active, and all owned markers remain 1.

Final untraced evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-sdk-reply-final-evidence-u9etj6yq`. Exact rebuilt library/stream/Node/outbox binaries, hashes, source diff and gate logs are archived.


## Bounded peer event result collection (2026-10-14)

`node/event_reply.rs` now collects event/control responses using the SDK's shared
64 MiB wire ceiling. Declared oversize is refused before body polling; unknown-
length overflow stops without collecting the suffix. The original route deadline
continues through the body phase and cannot be reset by response-head arrival.
Protocol overflow and deadline exhaustion return Ambiguous without the activation
network retry tag. Genuine body transport errors retain that tag and their typed
execution certainty. Caller cancellation retains precedence, including ties with
ready bodies/expired deadlines, and releases stalled body buffers. The existing
post-proven-head abort disarm remains in the caller; no collection failure may
kill a proven handler's later waitUntil work. All owned markers remain 1.

Twelve new tests cover exact/unknown-length budgets, declared-size admission,
suffix refusal, I/O tag preservation, the actual shared ceiling, cancellation/
dropped-sender ties, stalled-body/future release, actual Hyper declared-overflow
versus truncated responses over an in-memory duplex, original-deadline exhaustion
and deadline cleanup/ties. Combined library/Node/stream/outbox and workspace
Clippy-D qualification is running. Hyper duplex exercises its Incoming body/error
contract; it is not peer authentication, physical-host or store-fault evidence.
Whole request/head deadline behavior, upstream transport allocation, every
cancellation/ownership cut and the full release matrix remain separate gates.


The initial limited-collector stage finishes **195 passed/2 failed** library
cases. Both failures show genuine body I/O errors losing RemoteRpcTransportFailure
after Limited boxes them: a concrete io::Error fixture and actual truncated Hyper
Incoming both reproduce it. Clippy also refuses one redundant test async wrapper.
Initial evidence is retained under
`/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-collector-initial-ob4u1czt`.
These are before-fix results, not final qualification.

The final collector reads frames into one buffer whose allocation growth is
capped by the wire byte limit, rather than retaining a chunk list plus unused
trailer maps. It ignores trailers and yields after bounded frame/byte work, even
for immediately ready empty frames. This keeps cancellation/deadlines observable
without per-frame accumulation. Wrapping each concrete body error directly
preserves genuine I/O tags; overflow/allocation/deadline refusals remain untagged
Ambiguous. The redundant test wrapper is removed. Four more regressions cover
coalescing tiny frames, unused trailers, cancellation under always-ready empty
frames, and a ready result under the largest captured budget. The locked Tokio
sleep implementation uses checked instant addition/far-future fallback; no new
budget rejection or timer policy is introduced. Sixteen collector tests (201
expected total library cases) and fresh combined/Clippy gates are running.


Final-source workspace/all-target/all-feature Clippy-D passes (5m06s), along with
495-file formatting, 64 Python guards, documentation (35/6/373), dependency
(403 packages/no JavaScript engine) and whitespace checks. The refined combined
Rust run is compiling; the earlier 195/2 run remains before-fix evidence and
cannot qualify this source.


The refined library passes **all 201 cases** (23.75 seconds), including all
**16 collector regressions**. The two I/O-tag failures now pass with concrete
error wrapping, while declared/observed overflow and deadline/cancellation
refusals remain untagged. The always-ready empty-frame cancellation test passes,
as does the maximum captured-budget case. Native stream passes (12.54 seconds).
The complete 39-case Node suite is running; native outbox follows it. These
results do not refresh the full process-fault/backend/cancellation release matrix.


The final collector batch passes **all 242 selected Rust cases**: 201 library
(23.75s), native stream (12.54s), all 39 serial Node cases (394.21s), and native
outbox (1.52s). Final workspace/all-target/all-feature Clippy-D, 495-file formatting,
64 Python guards, docs (35/6/373), the 403-package/no-JavaScript-engine dependency
audit and whitespace checks pass. The failed initial 195/2 stage remains archived
separately; both concrete-I/O tag regressions pass after frame-level wrapping.
No original lease, deadline, acknowledgment or shutdown assertion was weakened.
Evidence root: `/private/var/folders/rn/k2b7mvbs2dgb__6ys6_h94pm0000gn/T/cellar-event-collector-final-evidence-cwgk96g8`. Exact rebuilt binaries/hashes, source and logs
are retained. This closes the peer event/control body collector gap, not every
request/head/local-invocation deadline, upstream allocation or cancellation/
ownership/process-fault cut. Earlier native startup/drain/analytics failures keep
their separate unestablished-cause audits. All markers remain 1 and the full goal
remains active; next prioritize whole-operation request/head/local cancellation
and the remaining physical metrics/inventory/backend/capacity release work.


## One deadline across entered event RPC dispatch (2026-10-14)

Current source gives node event/control RPC dispatch one immutable operation
budget across routing, retry/invalidation, local execution/output-gate waiting,
request submission, response head and body. Cancellation is selected once around
the operation and wins ready-result/deadline ties. Explicit never-connected/stale
verdicts update submission certainty; pre-invocation timeout/cancellation is
Unavailable, exhausted known-stale ownership is OwnershipLost, and a possibly
submitted mutation is Ambiguous. Deadline/cancellation verdicts do not carry the
activation network retry tag and do not undo prior commits. Existing typed
handler/host outcomes remain intact. A local request owns its activity before
its wait future is polled, so refusal/drop cannot leave a routed activity behind.
The peer abort guard is still disarmed after a proven response head.

Eight new tests cover unpolled expired waits, refusal/route certainty, modeled
post-submission effects/drop, cancellation/deadline/result ties, existing outcomes
and maximum budgets, actual LocalRequest activity release, and actual dispatcher
routing timeout with its CancelRoute message. The modeled atomic effect is not
SQLite durability evidence. Initial Clippy catches fixture type setup errors
(RpcData versus gated RpcOutcome, and an unwrap_err Debug bound); tests are
corrected to the actual API without changing production contracts. Qualification
is pending. SDK source egress-gate/channel-queue waits and every physical/process/
ownership/cancellation cut remain separate work; this entered-dispatch budget
does not establish complete end-to-end SDK qualification. All markers remain 1.


The SDK source RPC now captures its host operation budget before egress proof and
retains it through channel submission and result waiting. A pending proof timeout
returns Unavailable without submitting; queued-result timeout returns Ambiguous
and cancels that exact request. Two source-domain regressions exercise a held
real GateReq and an unconsumed real RpcCallReq. These are added after the initial
node compilation attempt; qualification of the combined source remains pending.
The node captures a later host budget, while the earlier source cancellation
bounds source-originated calls; no monotonic timestamp is serialized or compared
across hosts. Ordinary RPC does not gain an event deadline. All markers remain 1.


Event RPC deadline budgets include the canonical, nonnegative `__streamNext`
wait when it is within the captured host polling policy, plus the host transport
budget. That combined absolute deadline is captured once per source/entered-node
operation and is never reset at response headers or a retry. This preserves
empty-poll results when the default 30-second wait exceeds the 15-second transport
budget. Budget inspection accepts at most 16 KiB and bounded structured objects/
values/depth, reads only the array/body/wait field without alias expansion, and
never grants extra time to malformed or over-policy waits. Two parser regressions
cover JSON and structured arguments, canonicality, caps and saturating addition.
The source changed during the earlier build; fresh qualification is required.
All Cellar-owned markers remain 1.


## Product contract and developer entry points (2026-10-15)

PR #27 is merged. Follow-up work uses codex/event-delivery-product on the merged
main baseline, preserving the pending node/source deadline changes above. The
new docs/26-event-delivery.md is the developer entry point: source atomicity,
stream reads versus leased consumption, inbox/proof-before-ack, replay identities,
retention pressure, typed outcomes, and owner-routed operations. It is registered
in the offline catalog and linked from SDK/storage/work/example documentation.
The stale application-owned-outbox flow now describes the implemented framework
path and explicitly retains its under-development status. Search, flow selection,
step navigation, failure details, guide target and the regenerated PNG were
checked in an isolated agent-browser session. Documentation checks pass for
36 documents/6 flows/386 local references; dependency checks cover 403 packages
with no JavaScript engine, and 62 Python fault-harness guards pass.

ErrorCode::as_str and Display use canonical wire names; DeliveryError documents
is_permanent as automatic outbox lane blocking, not universal retry authority.
Missing bindings return typed InvalidRequest and closed domains Unavailable. A
new regression checks canonical names and typed errors under application context.
Rust selected tests, workspace Clippy and rustdoc remain live or queued; source
changed after the intermediate build began, so it cannot qualify the final patch.
The branch remains a draft until final-source validation passes. Full backend,
ownership, fault, capacity, physical metrics and release gates remain open.
All Cellar-owned markers remain 1.


## Submitted mutation acceptance outcomes (2026-10-15)

A current-source audit found that inner success-body validation could report
InvalidRequest, StaleReceipt or ConfigConflict after an operation may already
have committed. The SDK now applies a shared submitted_result boundary only
after call_bound/call_authorized has accepted a success branch. Configuration,
retirement, subscription lifecycle change, fetch, acknowledgment, retry and lease
extension classify invalid success metadata as generic Ambiguous. Their local
preflight checks and valid broker error branches retain their original codes
and metadata; read-only observers retain their existing validation behavior.
No body/validator context is reflected through this new error boundary.
Three regressions cover unknown acknowledgment statuses, known settlement
results, definitive broker refusals, successful null values and private-detail
removal. Formatting 498 and whitespace pass; Rust/Clippy/rustdoc remain pending.
The older intermediate compile began before this change and cannot qualify it.
All Cellar-owned markers remain 1.


The developer-guide outcome audit compares its handling table against all 33
current ErrorCode variants. Eight omitted outcomes now have explicit guidance:
initialization, invalid cursor, too-small batch, unsupported protocol, subscription
and operation capacity, unknown operations and pending delivery. Replay guidance
retains fetch/operation identity; capacity guidance preserves correctness metadata,
and unsupported layouts use the version-1 hard cutover policy. This is a docs-only
follow-up: Rust source remains identical to c7be23d for queued validation.


## Final-source validation and activation stack diagnosis (2026-10-15)

A fresh target avoids the measured dependency-directory scan bottleneck. Local
qualification uses Rust 1.97.0/aarch64 macOS, incremental 0, debug symbols 0 and
split-debuginfo off with normal dev/test assertions. Prior generated artifacts
are retained, and all intentionally interrupted builds remain unqualified.
The first fresh run passes all 218 library cases in 16.69s, then the stream fixture
fails an obsolete string assertion for an unbound producer after adoption. The
fixture now requires DeliveryError::InvalidRequest; its corrected complete stream
case passes in 11.67s. Native outbox passes in 0.70s. Workspace/all-target/all-feature
Clippy-D passes; rustdoc succeeds with seven private-link warnings in files
unchanged by this PR. These are pre-stack-fix results, not final qualification.

The full 39-case Node binary aborts at catalog inventory after its first 10 cases
pass. The unchanged isolated catalog case also aborts with stack overflow. The
crash report has a flat activation/startup poll chain. Offline disassembly shows
activate_inner reserving 209680 bytes in its poll frame. The candidate moves the
joined dispatch_rpc_call future behind Box::pin; no task, deadline, assertion or
worker-stack limit changes. The same isolated case passes in 6.81s, and its
activation poll frame reserves 25168 bytes under the same toolchain/profile.
The before binary/hash, crash report, both failed logs and before/after assembly
are retained separately. Existing catalog integration is the regression seam.
No fixed future-size assertion is added across platforms/toolchains.

The complete selected 259-case suite, workspace Clippy-D and rustdoc are now
running/queued against this candidate; it is not yet published or fully qualified.
This stack diagnosis does not establish the causes of earlier bootstrap, drain,
analytics-receipt or predecessor-recovery failures. All owned markers remain 1.


The candidate completes the selected run with 218 library cases passing (14.05s),
stream integration passing (32.07s), and 37/39 Node cases passing (314.14s). Native
outbox is run directly from the freshly built binary after Cargo stops on the
Node failures and passes (1.84s). Workspace/all-target/all-feature Clippy-D passes
(6m29s), and current-source rustdoc succeeds (4m43s) with the same seven private-link
warnings in unchanged files. The formerly crashing catalog test passes inside
the full serial Node suite as well as in isolation.

Two startup cases remain failures: cancelled bootstrap times out waiting for
factory entry, and the explicit-policy test's child returns execution domain is
closed. The same unchanged binary passes those cases separately in 3.23s and
15.16s; these replays do not establish either failure's cause or repair. A
bounded three-round repeated isolated probe is running. Preserve the failed
full-suite evidence separately from passing replays. PR34 remains a draft and
these startup/recovery audits remain open; no all-green Node or release claim
is made. No worker stack, timeout, lease or assertion was enlarged or weakened.


The three-round unchanged-binary startup probe completes with all six attempts
passing (cancelled bootstrap: 6.189/0.867/0.872s; explicit policy: 13.539/11.773/
11.947s). These do not explain or erase the failed full suite. The rebuilt real
cellar-example-streams executable also passes examples/durable-streams/smoke.py:
atomic producer, resumable read, inbox redelivery, acknowledgment and preserving
restart. This qualifies the documented local walkthrough, not the complete
backend/crash/ownership/release matrix.

## Storage-isolated event owner qualification

The next correctness gate now has an event-specific native scenario in
`tests/support/event_owner_outage.rs`. A node-specific fault client refuses new
ordinary/CAS writes, reads, deletes and listings. Its fixed small pagination
adapter is test infrastructure only. An independent healthy client and executor
serve the successor; the old client remains isolated through the final audit.

Before the outage, the test proves event one, its consumer inbox effect and live
lease, then retains event two behind a revoked publish grant. It requires the
real `NodeLeaseExpired` outcome, closed host admission and a live caller executor.
Before any retry, the successor must recover the exact source incarnation/lane
revision, pending row, business count, accepted envelope, effect and checkpoint.
A still-live lease must retain its original generation/range/expiry. After natural
lease expiry, redelivery must preserve the inbox effect count; the original
receipt must return `StaleReceipt` against the replacement lease without advancing
the checkpoint. Fresh ack, authorized grant repair and explicit outbox retry then
complete both events with checkpoint two and the independently expected sum three.

The focused case passes in 15.32s on Rust 1.97.0/aarch64 macOS with the existing
symbols-disabled/nonincremental profiles. The initial import collision and two
fixture declaration failures are retained separately: a successor binding must
respect the revoked grant, and an existing administrative revision cannot be
reissued under a different deployment operation. The final successor only binds
to the existing resource; it does not reconfigure state to make recovery pass.
The full pre-fix serial Node run passes 37/40 in 394.99s, including the new case.
Failures are stop-receipt HTTP connection loss, catalog handoff, and producer
lease expiry during the lost-append/ack test's final shutdown. The first and third
causes remain unestablished; passing replay cannot close them.

This is injected local object-store unavailability followed by an expired-owner
recovery. It does not qualify physical network isolation, a concurrent takeover
while the old process remains running, delayed in-flight completion fencing,
follower-only proof, power/store destruction, every commit/proof cut or the
unresolved intermittent startup failures. Those correctness gates remain ahead
of cosmetic SDK or additional metrics work. Production Rust and owned version-1
markers are unchanged.

### Confirmed wake-publication admission panic

The catalog failure includes an actual `DomainClosed` panic through
`wake_entry::launch_arm_gate`: a transaction finishes in an already-entered poll
after its host domain closes, then calls infallible `asyncrt::spawn` for discovery.
A deterministic regression closes the domain inside its entered poll and calls
that same launcher. Before the fix it reproduces the same panic in 0.04s; the
initial compile/setup failures above are not this production reproduction.

The launcher now uses `try_spawn`. Refusal drops the publication sender but still
returns its receiver, so the real output-gate collector fails. It cannot turn
missing discovery proof into a successful response or an absent gate. Started
publication remains domain-owned and tracked. This fixes the proven panic path,
not the cause of the domain's closure or every startup/lease failure. Final-source
qualification passes 219 library cases (14.08s), native stream (11.59s), all
40 serial Node cases (307.60s) and native outbox (2.09s). The deterministic
regression passes after the fix and all three previously failed integration
cases pass in this run. Only the wake-admission panic has a reproduced cause and
specific repair; the stop-receipt connection loss and producer shutdown expiry
remain cause audits. After a test-only return-container correction for `async_yields_async`, all 219
library cases pass again (11.34s). Production code and Node fixture bytes remain
unchanged from the 40-case pass. Formatting checks 499 Rust files; docs check
36 documents/6 flows/386 references and the dependency audit checks 403 packages
without a JavaScript engine. Final workspace/all-target/all-feature Clippy passes with warnings denied
(4m03s). Its initial failure was the regression test returning an awaitable receiver from
an async block. No lint suppression, policy change or assertion relaxation was used.

## Late append reply across source-owner fencing

The peer fault proxy now supports a separate delayed-reply mode. It captures a
complete successful append response, pauses subsequent append submissions before
they reach the stream, and exposes separate release controls for the old response
and new submissions. Existing drop-reply behavior remains a distinct mode.

`event_late_reply::late_proven_append_reply_cannot_settle_a_fenced_source_successor`
uses independent destination/source/successor executors and real authenticated
peer RPC. It records the original business command, envelope and proven append
acceptance while the source obligation is still pending. Removing only the test
source's node lease drives a real `NodeLeaseMissing` fence and joined domain.
The source fixture uses the same 5-second authority-readback window as the
existing missing-lease regression; operation/readiness/shutdown budgets are not
extended. A binding-only successor recovers the original incarnation, lane,
sequence and pending row before append replay is allowed.

The old response is then released while new append submissions remain paused.
The successor must still show one pending row, next sequence two and settled
watermark zero. Resuming submissions must produce byte-identical request and
acceptance replies, exactly one retained stream record, one inbox effect and
checkpoint one. All final node shutdowns and the proxy must join. A transport
close on the old socket is expected after fencing; unrelated proxy errors fail.

The focused case passes in 11.21s. The initial non-cloneable deployment-graph
fixture compile error is retained separately and is not production evidence.
All 41 serial Node cases pass (342.05s), including the previous drop-reply,
storage-outage and catalog tests. Formatting checks 500 Rust files and workspace/
all-target/all-feature Clippy passes with warnings denied (31.10s). Docs validate
36 documents/6 flows/386 references and dependency audit checks 403 packages
without a JavaScript engine. Whitespace checks pass.
Production Rust and version-1 markers are unchanged from a62a244; its 219-library
and native stream/outbox results retain that exact production-source scope.

This covers delayed append response delivery after a source domain has joined.
It does not qualify an old owner still executing during concurrent takeover,
lease/ack completions across destination takeover, physical network isolation,
follower-only proof or the full crash/storage-loss matrix. These remain higher
priority than SDK polish or more aggregate metrics. Earlier intermittent startup
and shutdown failures retain their separate unestablished-cause audits.

## Delayed proven ack across destination fencing

`event_late_reply::delayed_proven_ack_survives_destination_fencing_without_consuming_the_next_lease`
extends the correctness matrix to destination-owner failure. The proxy captures
one complete successful ack response without pausing unrelated appends. A consumer
has already committed/proven its inbox effect and saved the exact receipt. The
test independently observes checkpoint one and no lease before removing only
this test destination's node lease and requiring `NodeLeaseMissing` plus joined
closed admission. The existing missing-authority fixture's 5-second readback
window is retained; operation/readiness/shutdown budgets are not extended.

Before replay, binding-only activation and signed inspection must recover the
same subscription identity, checkpoint one, no lease and exact accepted event.
A read of the authoritative partition owner record must show a different node
and strictly higher epoch. Placement may choose any healthy host; a replacement
entrance alone is not evidence that the partition changed owner. The old ack
response is released only after these audits. The original caller may receive
`Acknowledged` or `Ambiguous`, since the ack was committed/proven before the cut.
Replaying the original saved receipt must return `AlreadyAcknowledged`.

A second event then acquires a fresh lease and its local effect. Replaying the
first receipt again must leave checkpoint one and the second lease's full summary
unchanged. Only the fresh receipt may advance checkpoint two. The independently
expected effect total is three. Final node shutdowns, proxy and observer join.
Expected upstream `ConnectionRefused` after controlled destination closure closes
the tunnel without fabricating a response; unrelated proxy errors still fail.

The focused scenario passes in 7.10s and passes again inside the full serial
Node run. That run fails overall: 39/42 pass in 534.24s. The unchanged cancelled-
bootstrap case times out waiting for factory entry; the explicit-policy child
hits its bootstrap deadline; failed-predecessor recovery reports `NodeLeaseExpired`
at shutdown. Their causes remain unestablished, and this is not a green gate.
A live process sample during the apparent initial stall shows the test had
already returned an error and was formatting/symbolicating its backtrace; only
two samples were obtained, so this does not establish the startup failure cause.
The run was not restarted or interrupted. Workspace/all-target/all-feature Clippy
passes with warnings denied (52.27s), formatting checks 500 Rust files, docs check
36 documents/6 flows/386 references, dependency audit checks 403 packages without
a JavaScript engine, and whitespace checks pass. The startup/recovery cause audit
is the next priority before further fault-case additions. Production code and
owned markers stay unchanged from
a62a244. This covers a committed/proven ack whose response crosses joined owner
fencing; it does not qualify lease-fetch response loss, an unproven ack cut, an
old owner still executing during takeover, physical network isolation or
follower-only proof. Those remain correctness priorities, along with the earlier
unestablished startup/shutdown failure causes.
## Local SDK preflight standardization

PR #34 is merged. PR #36 separately contains startup-failure diagnostics; its
39/39 Node run does not establish the causes of the two earlier intermittent
failures. Its correction to the current-status summary is independent of this
SDK change.

Authority minting now returns `Forbidden` for principal/permission refusal and
`InvalidRequest` for malformed bindings or a missing append source. Configure
returns `InvalidRequest` for an invalid definition. Subscription construction
separates invalid names from valid names without a local grant. Receiver grant
checks and submitted-result ambiguity are unchanged; owned markers stay at 1.
The developer guide documents the local-refusal contract.

Before destination provisioning, the native source fixture checks seven typed
refusals and verifies that no catalog or stream partition activates. Local
verification passes 218 library cases (9.82s), native stream (11.91s) and native
outbox (0.65s). Compilation takes 7m29s with Rust 1.97.0/aarch64 macOS and the
captured symbols-disabled, nonincremental dev/test profile. Formatting checks
498 Rust files; documentation checks 36 documents/6 flows/386 references; the
403-package dependency audit remains free of JavaScript engines. Workspace
Clippy passes with all targets/features and warnings denied. Complete SDK outcome
auditing and broader backend, ownership,
fault, capacity and release gates remain open.

## Startup failure observation boundaries (2026-10-15)

The startup fixtures use one-second leases and five-second entry/readiness waits.
Failure-only diagnostics now report whether the cancelled-bootstrap startup future
completed (including its error) or remains pending at entry timeout. The policy
child labels node start, readiness and drain stages; on closed-domain readiness
failure it polls the supervisor completion once without waiting or changing the
failed result. Its wrapper reports a child failure without assuming configuration
was consulted. Original deadlines, assertions and public node policies are intact.

The diagnostic Node suite passes all 39 cases in 288.58s, with RUST_BACKTRACE=1
and no other task-owned checks scheduled during runtime. This is an observation,
not proof that compiler overlap or any particular startup race caused the prior
failures. Earlier 37/39 failure evidence remains retained. Workspace/all-target/
all-feature Clippy-D for this test-only change passes in 2m32s. No production Rust
changed since 69b191d. The factory-entry and closed-domain root-cause audits remain
open.

## Concurrent source retirement verification (2026-10-15)

The console adapter scenario reproduced a conflict when request-time source
retirement verification and durable outbox work awaited the same catalog proof.
The worker advanced the unchanged closure from `checking` to `verified` and
installed a newer work generation; the older request then incorrectly reported a
changed lane set. Verification now discards that superseded completion without
rewriting evidence, lifecycle state or the newer deadline. Replaced source
identity/generation and a changed snapshot in the current installation still
fail closed.

The real-node console test retains terminal stream proof, completed source
cleanup, fresh-incarnation recreation, historic destruction replay isolation and
complete returned shutdown assertions. This covers the observed overlap, not the
remaining complete destruction fault, capacity, facet or lease-policy matrix.
All owned schema/protocol markers remain 1.
