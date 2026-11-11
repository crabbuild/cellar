// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK/native/Axum routing oracles. No direct runtime injection.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
#[path = "support/cell_group_rustfs.rs"]
pub mod rustfs;
#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_leader_direct_write() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::cached_leader_direct_write()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_leader_keeps_placement() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::busy_leader_keeps_placement()).await
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
async fn cached_advice_respects_origin_revocation() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::cached_advice_respects_origin_revocation()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_cached_advice_respects_origin_revocation() -> anyhow::Result<()> {
    Box::pin(
        managed_fixture::requests::cached_advice_respects_origin_revocation_with_store(
            rustfs::store,
        ),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_storm_is_bounded() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::refresh_storm_is_bounded()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_probe_storm_is_bounded() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::leader_probe_storm_is_bounded()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_cohort_has_fresh_barrier() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::read_cohort_has_fresh_barrier()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_preferences_preserve_proof() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::read_preferences_preserve_proof()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_repairs_only_overlap() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::split_repairs_only_overlap()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_lookup_cannot_resurrect_parent() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::old_lookup_cannot_resurrect_parent()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_refresh_keeps_range() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::membership_refresh_keeps_range()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_membership_refresh_keeps_range() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::membership_refresh_keeps_range_with_store(rustfs::store))
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_split_repairs_only_overlap() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::split_repairs_only_overlap_with_store(rustfs::store)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_old_lookup_cannot_resurrect_parent() -> anyhow::Result<()> {
    Box::pin(
        managed_fixture::requests::old_lookup_cannot_resurrect_parent_with_store(rustfs::store),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn endpoint_refresh_keeps_membership() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::endpoint_refresh_keeps_membership()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_endpoint_refresh_keeps_membership() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::endpoint_refresh_keeps_membership_with_store(rustfs::store))
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unrelated_follower_keeps_leader() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::unrelated_follower_keeps_leader()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_unrelated_follower_keeps_leader() -> anyhow::Result<()> {
    Box::pin(managed_fixture::requests::unrelated_follower_keeps_leader_with_store(rustfs::store))
        .await
}
