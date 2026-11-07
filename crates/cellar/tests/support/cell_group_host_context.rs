// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical source-seal and host fixtures shared by native integration targets.
#![allow(clippy::disallowed_methods)]
// Common engine fixture helpers are selected by several independent targets.
#![allow(dead_code, unused_imports)]
#[path = "cell_group_archive_store.rs"]
mod archive_store;
#[path = "cell_group_checkpoint.rs"]
mod checkpoint_fixture;
#[path = "raft_storage_fault.rs"]
mod fault;
#[path = "raft_ltx_fixture.rs"]
mod images;
#[path = "cell_group_runtime.rs"]
pub mod runtime_fixture;
include!("cell_group_engine_fixture.rs");
use crate::rustfs;

pub(crate) async fn sealed_source_archives_require_exact_live_holds() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::sealed_source_archives(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

pub(crate) async fn rustfs_sealed_source_archives() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::sealed_source_archives(
            rooted,
            rustfs::store("sealed-source-archives", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

pub(crate) async fn source_seal_blocks_delayed_work() -> Result<()> {
    Box::pin(runtime_fixture::source_seal_blocks_delayed_work()).await
}

pub(crate) async fn namespace_host_many_cells_share_one_physical_group() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::host_many_cells(rooted)).await?;
    }
    Ok(())
}

pub(crate) async fn namespace_host_query_cancellation_retains_physical_join() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::host_query_shutdown(rooted)).await?;
    }
    Ok(())
}

pub(crate) async fn namespace_host_bounds_identity_and_enrollment_deadline() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::host_registration_and_deadline(rooted)).await?;
    }
    Ok(())
}
