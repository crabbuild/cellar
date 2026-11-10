// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit archive initialization for the managed fixture.

use super::*;

pub fn configure(config: &mut RaftCellsConfig) {
    config.shared.checkpoints = Some(checkpoint_limits());
    config.shared.archive = Some(CellGroupNodeArchiveOptions {
        io: Default::default(),
        uploads: Default::default(),
        tails: Default::default(),
    });
}

pub async fn open_provisioning(
    fixture: &Fixture,
    adapter: &Catalog,
) -> Result<RaftManagedArchiveProvisioning> {
    Ok(fixture
        .catalog
        .open_managed_archive_provisioning(
            adapter,
            &fixture.declaration,
            DirectoryLimits::default(),
            CellGroupArchiveLimits::default(),
            TIMEOUT,
        )
        .await?)
}

pub async fn initialize(
    fixture: &Fixture,
    adapter: &Catalog,
    history: GroupHistory,
    disk: &Path,
) -> Result<()> {
    let prepared = open_provisioning(fixture, adapter)
        .await?
        .prepare_archive(history, TIMEOUT)
        .await?;
    // Sync the exact operator intent before entering the one-use operation.
    let intent = disk.join("archive-initialization");
    use std::io::Write as _;
    let mut file = std::fs::File::create_new(&intent)?;
    file.write_all(prepared.encoded_intent())?;
    file.sync_all()?;
    std::fs::File::open(disk)?.sync_all()?;
    prepared.commit(fixture.domain.as_ref()).await?;
    open_provisioning(fixture, adapter)
        .await?
        .verify_archive(history, &std::fs::read(intent)?, TIMEOUT)
        .await?;
    Ok(())
}
