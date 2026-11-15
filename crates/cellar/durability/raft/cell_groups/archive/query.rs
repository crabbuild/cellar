// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A read with an S3 minimum waits for complete, independently recovered coverage
//! of its selected logical version. It never writes an empty command for proof.

use super::*;
use crate::raft::version::CellVersionObservation;

pub(in crate::raft::cell_groups) fn verify(
    coverage: &CellGroupArchiveCoverage,
    store: &CellGroupArchiveStore,
    expected: &CellVersionObservation,
) -> Result<bool> {
    store.require_live()?;
    let root = coverage.recovery().manifest();
    if root.marker().route() != store.route() || !store.owner.owns(root) {
        return Err(CellGroupArchiveError::Invalid(
            "S3 query verifier owner/route",
        ));
    }
    let Some((leaf, _)) = root.pin_cell(&expected.version.history.cell) else {
        return Ok(false);
    };
    if leaf.version.history != expected.version.history {
        return Err(CellGroupArchiveError::Invalid("S3 query history"));
    }
    if leaf.version.sequence < expected.version.sequence {
        return Ok(false);
    }
    if leaf.version == expected.version && leaf.witness != expected.witness {
        return Err(CellGroupArchiveError::Invalid("S3 query decision"));
    }
    // Complete coverage verifies the image closure and its committed ancestry.
    // A later cut may supersede the observed value, as with any strong read; it
    // must cover this same logical history, never a recreated cell's history.
    store.require_live()?;
    Ok(true)
}
