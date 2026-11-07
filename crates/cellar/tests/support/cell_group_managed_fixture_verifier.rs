// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private replay inventory for the embedded managed fixture.
use super::*;
use cellar::application::consensus::verification::cell_groups::CellGroupArchiveTailLimits;

pub async fn open(
    fixture: &Fixture,
    root: &Path,
    rooted: bool,
    tails: CellGroupArchiveTailLimits,
) -> Result<FleetGroupArchiveVerifier> {
    super::archive_verifier::open(
        &fixture.domain,
        root,
        rooted,
        tails,
        limits(),
        checkpoint_limits(),
    )
    .await
}

pub async fn open_for_domain(
    domain: &ExecutionDomain,
    root: &Path,
    rooted: bool,
    tails: CellGroupArchiveTailLimits,
) -> Result<FleetGroupArchiveVerifier> {
    super::archive_verifier::open(domain, root, rooted, tails, limits(), checkpoint_limits()).await
}
