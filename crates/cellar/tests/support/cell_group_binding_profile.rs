// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in diagnostics on one complete ordinary binding configuration. This is
//! not the four-configuration public gate or a formal CG-17 acceptance driver.
use super::*;

pub(super) async fn run(rooted: bool) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::binding=debug,cellar::raft::checkpoint=debug,cellar::raft::group_archive=debug,cellar::raft::cell_groups::controller::effects=debug")
        .with_test_writer()
        .try_init()
        .map_err(|error| anyhow::anyhow!("profile subscriber: {error}"))?;
    let store = Arc::new(catalog_fault::Store::around(rustfs::store(
        "profile-s3-named-bindings",
        rooted,
    )?));
    store
        .observe_group_archive_io
        .store(true, Ordering::Release);
    Box::pin(bindings::run(rooted, Durability::S3, store)).await
}
