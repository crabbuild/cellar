// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-09 source fencing and managed child installation/publication contracts.
//! Phase-crash, effect continuity and complete retirement acceptance remain explicit gates.
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
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
#[path = "support/cell_group_rustfs.rs"]
pub mod rustfs;
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
