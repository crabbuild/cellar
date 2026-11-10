// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK/native/Axum routing oracles. No direct runtime injection.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_leader_direct_write() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::cached_leader_direct_write()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_repairs_leader_only() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::election_repairs_leader_only()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_outage_on_cached_path() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::metadata_outage_on_cached_path()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_storm_is_bounded() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::refresh_storm_is_bounded()).await
}
