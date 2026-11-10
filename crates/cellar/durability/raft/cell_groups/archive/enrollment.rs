// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Original creation provenance in a complete independently recovered group.
use super::*;
use crate::raft::cell_groups::EnrollmentRecord;

pub(in crate::raft::cell_groups) fn verify(
    coverage: &CellGroupArchiveCoverage,
    store: &CellGroupArchiveStore,
    expected: &EnrollmentRecord,
) -> Result<bool> {
    store.require_live()?;
    let root = coverage.recovery().manifest();
    if root.marker().route() != store.route() || !store.owner.owns(root) {
        return Err(CellGroupArchiveError::Invalid(
            "S3 enrollment verifier owner/route",
        ));
    }
    let Some((leaf, _)) = root.pin_cell(&expected.cell().cell) else {
        // An older complete cut can precede this committed creation.
        return Ok(false);
    };
    if &leaf.enrollment != expected {
        return Err(CellGroupArchiveError::Invalid(
            "S3 enrollment creation record",
        ));
    }
    // The full coverage already verified every cell image. The immutable
    // creation record, including its initial commitment, survives later images;
    // retaining superseded initial image bytes is not required.
    store.require_live()?;
    Ok(true)
}
