<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

The delivered design and implementation plan covers CG-01 through CG-16 and a
hard cutover. The active goal resumes end-to-end implementation after the plan
handoff recorded below. Intermediate contracts do not qualify a shared runtime.
No production legacy/shared mode flag is being added.

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

The [entry codec contract](../../../crates/cellar/application/consensus/group_protocol/README.md)
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

CG-01 and CG-02 have passed their complete task gates. CG-03 is IN_PROGRESS;
CG-04 through CG-16 remain TODO. The latest storage qualification is recorded below; the earlier sections
retain the intermediate checkpoints and their limitations.

The reviewed source baseline is `a0c11969763edbe86ee08fc7d1650129058c8ee0`.
The catalog anchor adds the directory module/export block; its unchanged namespace
and birth excerpts move by six lines. Other source anchors remain unchanged. A new
executor must receive both ignored DONE-task evidence folders before validation.

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
manifest can yet grant shared-group serving authority. The [component contract](../../../crates/cellar/durability/raft/cell_groups/README.md)
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
