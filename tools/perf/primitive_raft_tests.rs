// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_ltx::{
    internal::{NodeExtentStore, StoreError},
    DirectFileSystem,
};

#[test]
fn writer_disk_bank_admits_bounded_overlap_and_releases_physical_ownership() -> Result<()> {
    let mut host = RaftHostOptions::default();
    host.images.max_bytes = 128 * 1024 * 1024;
    host.execution.max_image_bytes = host.images.max_bytes / 8;
    let bounds = RootPrivateOptions::workspace_bounds(host.execution)?;
    let charge = bounds.reservation()?;
    let directory = tempfile::tempdir()?;
    let legacy = raft_limits::extents(&host)?;
    ensure!(
        legacy.inventory.max_bytes < charge * 2,
        "legacy control must refuse overlap"
    );
    let control = NodeExtentStore::open(
        directory.path().join("legacy"),
        Arc::new(DirectFileSystem),
        legacy.inventory,
    )?;
    let first = control.create_workspace([1; 32], bounds)?;
    assert!(matches!(
        control.create_workspace([2; 32], bounds),
        Err(StoreError::Limit)
    ));
    drop(first);
    assert_eq!(control.usage()?.workspaces, 0);

    let options = extents(&host)?;
    let store = NodeExtentStore::open(
        directory.path().join("overlap"),
        Arc::new(DirectFileSystem),
        options.inventory,
    )?;
    let mut retained = Vec::new();
    for slot in 1..=WRITER_WORKSPACES {
        retained.push(store.create_workspace([u8::try_from(slot)?; 32], bounds)?);
    }
    assert_eq!(options.inventory.max_workspaces, WRITER_WORKSPACES);
    assert_eq!(store.usage()?.workspaces, WRITER_WORKSPACES);
    assert_eq!(store.usage()?.bytes, charge * WRITER_WORKSPACES as u64);
    assert!(matches!(
        store.create_workspace([64; 32], bounds),
        Err(StoreError::Limit)
    ));
    drop(retained.pop().context("retained workspace")?);
    let replacement = store.create_workspace([64; 32], bounds)?;
    drop(replacement);
    drop(retained);
    assert_eq!(store.usage()?.workspaces, 0);
    assert_eq!(store.usage()?.bytes, 0);
    assert!(!store.usage()?.quarantined);
    Ok(())
}
