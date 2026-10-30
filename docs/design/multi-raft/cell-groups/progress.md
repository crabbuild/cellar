<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

The executable design and implementation plan is delivered. The active goal
continues through end-to-end implementation and hard cutover; see the
[executor handoff](handoff.md).
CG-01 through CG-03 have passed their scoped gates;
CG-04 is IN_PROGRESS and CG-05 through CG-16 remain TODO. Intermediate contracts
do not qualify the public shared runtime. No production legacy/shared mode flag
is being added.

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
