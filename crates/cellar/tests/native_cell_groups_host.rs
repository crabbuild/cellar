// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared namespace host boundary; canonical SDK/fleet cutover remains separate.
#![allow(clippy::disallowed_methods)]
// Common engine fixture helpers are selected by several independent targets.
#![allow(dead_code, unused_imports)]
#[path = "support/cell_group_archive_store.rs"]
mod archive_store;
#[path = "support/cell_group_checkpoint.rs"]
mod checkpoint_fixture;
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
mod images;
#[path = "support/cell_group_runtime.rs"]
mod runtime_fixture;
include!("support/cell_group_engine_fixture.rs");
#[path = "support/cell_group_rustfs.rs"]
mod rustfs;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn sealed_source_archives_require_exact_live_holds() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::sealed_source_archives(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_sealed_source_archives() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::sealed_source_archives(
            rooted,
            rustfs::store("sealed-source-archives", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn source_seal_blocks_delayed_work() -> Result<()> {
    Box::pin(runtime_fixture::source_seal_blocks_delayed_work()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn namespace_host_many_cells_share_one_physical_group() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::host_many_cells(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn namespace_host_query_cancellation_retains_physical_join() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::host_query_shutdown(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn namespace_host_bounds_identity_and_enrollment_deadline() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(runtime_fixture::host_registration_and_deadline(rooted)).await?;
    }
    Ok(())
}
