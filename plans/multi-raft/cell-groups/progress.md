<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

The active objective is the entire design and a hard cutover, including all CG-01
through CG-16 qualification gates. Intermediate contracts do not qualify a shared
runtime. No production legacy/shared mode flag is being added.

## CG-01 — in progress

Implemented contracts:

- Dependency-free validated cell IDs, logical cell histories and physical group
  histories; nonzero mutation, route and membership counters refuse overflow.
- Canonical namespace routing preimage and adapter SHA-256, collision-safe ordered
  keys, half-open ranges, adjacent merge and whole-key split boundaries.
- Cell versions compare mutation sequence within one logical history, rejecting
  equal-sequence conflicting image commitments. Commit witnesses remain separate.
- Domain-separated signed cell-version tokens authenticate schema, cell history,
  sequence, LTX/image commitment and the original physical commit witness. Transfer
  can preserve the token's history; disaster recovery's new incarnation refuses it.
- Materialized/extent commitment conversion validates the actual extent reference
  layout at the host boundary. Core contracts add no dependencies or I/O.

The new contracts are exposed through the existing unstable verification surface
while host integration is pending. Existing public runtime consumers still use the
old contracts and will switch together at CG-07, before legacy removal at CG-16.

Remaining CG-01 work includes the cell-addressed data and typed administrative
entry codec, bounded decoder tests, complete task checks and source-bound evidence.
CG-02 and later tasks remain TODO. No task is DONE and no benchmark, RustFS drill,
hibernation latency, recovery guarantee or production cutover is claimed here.

Checks completed for this intermediate change:

- `native_cell_groups_contracts`: 3 cases pass, including the smallest and largest
  cell IDs in the signed format, tampering, truncation and transfer/DR identity.
- `native_raft_contract`: all 6 existing cases pass.
- `native_raft_reads`: exact `query_token_auth_scope` and
  `version_key_deadline_and_payload_limits` regressions pass.
- `cellar-logic --lib`: all 18 cases pass, including 4 new group-contract cases.
- Clippy for the affected libraries and new native test passes with `-D warnings`;
  module layout, native dependency isolation and repository documentation pass.

The first native run exposed an incorrect minimum token length for short IDs.
The bound now derives from the fixed wire fields, and a one-byte ID regression
passes. Local logs, including that failed run, remain under the ignored
`.cellar/cell-groups-evidence/CG-01-in-progress/` directory. They are intermediate
checks, not the complete source-bound CG-01 evidence record.

The original source preflight was inspected before edits. `version.rs` is now an
intentional CG-01 change; update its reviewed baseline at the completed source
revision before dependent implementation starts. Do not replace a mismatched hash
without reading the changed contract.
