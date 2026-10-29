<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

The delivered design and implementation plan covers CG-01 through CG-16 and a
hard cutover. Implementation is active and follows the full dependency order. Intermediate contracts do not qualify a shared
runtime. No production legacy/shared mode flag is being added.

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

CG-02 is IN_PROGRESS; CG-03 through CG-16 remain TODO. CG-02 must complete
complete paged group manifests, real engine publication, recovery refusal for
missing dependencies and image/pin reclamation tests in both image modes.

The reviewed source baseline advances to the qualified CG-01 revision. Original
host/storage source anchors remain unchanged; newly produced contract files are
bound by the CG-01 evidence record. Review changed inputs before refreshing the
baseline for later tasks. A new executor must receive the ignored evidence
folder before validating this DONE checkpoint.

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
