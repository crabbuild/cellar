// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in native execution against an explicitly selected, isolated Linux
//! Engine. No ambient socket, image, object-store credential or durability fallback.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::consensus::*;
use managed_fixture::{ManagedCluster, TIMEOUT};

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
#[path = "support/cell_group_configuration.rs"]
mod configuration;
#[path = "support/cell_group_docker_backend.rs"]
mod docker_backend;
#[path = "support/cell_group_docker_execution.rs"]
mod docker_execution;
#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker and a pinned local scaffold image"]
async fn docker_backend_joins_processes_and_retains_original_receipts() -> anyhow::Result<()> {
    docker_backend::qualify().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker and a pinned local scaffold image"]
async fn docker_backend_shutdown_fences_processes_when_journal_fails() -> anyhow::Result<()> {
    docker_backend::qualify_failed_shutdown().await
}
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
#[path = "support/cell_group_named_service.rs"]
pub mod named_service;
#[path = "support/cell_group_rustfs.rs"]
pub mod rustfs;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker, a pinned local scaffold image and real RustFS"]
async fn rustfs_native_docker_executes_and_recovers_shared_quorum() -> anyhow::Result<()> {
    docker_execution::qualify(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker, a pinned local scaffold image and real RustFS"]
async fn rustfs_native_docker_refuses_execution_without_source_proof() -> anyhow::Result<()> {
    docker_execution::qualify(true).await
}

#[path = "support/cell_group_sandbox_execution.rs"]
mod sandbox_execution;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker, a pinned local scaffold image and real RustFS"]
async fn rustfs_native_sandboxes_execute_and_cleanup_through_shared_quorum() -> anyhow::Result<()> {
    sandbox_execution::qualify(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker, a pinned local scaffold image and real RustFS"]
async fn rustfs_native_sandboxes_refuse_unproved_parent_dispatch() -> anyhow::Result<()> {
    sandbox_execution::qualify(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker, a pinned local scaffold image and real RustFS"]
async fn rustfs_native_sandbox_retirement_preserves_requested_s3_minimum() -> anyhow::Result<()> {
    sandbox_execution::qualify_minimum_custody().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Linux Docker, a pinned local scaffold image and real RustFS"]
async fn rustfs_native_sandbox_deletion_preserves_input_proof() -> anyhow::Result<()> {
    sandbox_execution::qualify_delete_input().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic: original extent/S3 Start against isolated Docker and RustFS"]
async fn profile_rustfs_extent_s3_sandbox_start() -> anyhow::Result<()> {
    sandbox_execution::profile_start().await
}
