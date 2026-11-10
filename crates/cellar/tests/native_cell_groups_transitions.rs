// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-09 source fencing and managed child installation/publication contracts.
//! Sealed sources, complete publication closures and retained per-cell effects.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::cell_groups::*;
use cellar::sdk::{consensus::*, *};
use managed_fixture::{fault, limits, request, start_node, Fixture, TIMEOUT};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[path = "support/cell_group_transition_context.rs"]
mod bootstrap;
#[path = "support/cell_group_transition_catalog.rs"]
mod catalog;
#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
pub use source_fixture::rustfs;
#[path = "support/cell_group_host_context.rs"]
mod source_fixture;

pub use source_fixture::runtime_fixture::catalog_fault;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_authorization_cancel_race() -> Result<()> {
    catalog::seal_authorization_cancel_race().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn source_seal_blocks_delayed_work() -> Result<()> {
    Box::pin(source_fixture::source_seal_blocks_delayed_work()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_requires_install_and_publication() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::child_requires_install_and_publication(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_keeps_required_archive_pins() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::qualify_retirement(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await
        .with_context(|| format!("retirement qualification, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_retirement_keeps_required_archive_pins() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "retirement-pins",
            rooted,
        )?));
        Box::pin(bootstrap::transition::qualify_retirement(rooted, store))
            .await
            .with_context(|| format!("RustFS retirement qualification, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transition_restart_retains_single_writer() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::restart::qualify(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await
        .with_context(|| format!("phase restart, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_transition_restart_retains_single_writer() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "phase-restart",
            rooted,
        )?));
        Box::pin(bootstrap::transition::restart::qualify(rooted, store))
            .await
            .with_context(|| format!("RustFS phase restart, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfer_preserves_tokens_receipts_effects() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::effects::qualify(
            rooted,
            Arc::new(fault::Store::default()),
            false,
        ))
        .await
        .with_context(|| format!("effect transfer, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_transfer_preserves_tokens_receipts_effects() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "effect-transfer",
            rooted,
        )?));
        Box::pin(bootstrap::transition::effects::qualify(
            rooted, store, false,
        ))
        .await
        .with_context(|| format!("RustFS effect transfer, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_requires_remote_transfer_closure() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::closure::qualify(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await
        .with_context(|| format!("published closure disk loss, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_publication_requires_remote_transfer_closure() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "publication-loss",
            rooted,
        )?));
        Box::pin(bootstrap::transition::closure::qualify(rooted, store))
            .await
            .with_context(|| format!("RustFS published closure disk loss, extents={rooted}"))?;
    }
    Ok(())
}

fn route() -> GroupRoute {
    GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    }
}
