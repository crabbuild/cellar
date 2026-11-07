// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared namespace host contracts and physical source-seal regression.
#[path = "support/cell_group_host_context.rs"]
mod fixture;

pub use fixture::rustfs;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn sealed_source_archives_require_exact_live_holds() -> anyhow::Result<()> {
    Box::pin(fixture::sealed_source_archives_require_exact_live_holds()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_sealed_source_archives() -> anyhow::Result<()> {
    Box::pin(fixture::rustfs_sealed_source_archives()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn source_seal_blocks_delayed_work() -> anyhow::Result<()> {
    Box::pin(fixture::source_seal_blocks_delayed_work()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn namespace_host_many_cells_share_one_physical_group() -> anyhow::Result<()> {
    Box::pin(fixture::namespace_host_many_cells_share_one_physical_group()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn namespace_host_query_cancellation_retains_physical_join() -> anyhow::Result<()> {
    Box::pin(fixture::namespace_host_query_cancellation_retains_physical_join()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn namespace_host_bounds_identity_and_enrollment_deadline() -> anyhow::Result<()> {
    Box::pin(fixture::namespace_host_bounds_identity_and_enrollment_deadline()).await
}
