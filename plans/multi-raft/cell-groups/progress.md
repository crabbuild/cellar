<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

The delivered design and implementation plan covers CG-01 through CG-16 and a
hard cutover. The current handoff is the executable plan; further runtime work
follows its dependency order. Intermediate contracts do not qualify a shared
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

CG-02 through CG-16 remain TODO. CG-02 is dependency-eligible and must implement
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
