// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-10 whole-cell partitions, operator admission and partial merge failure.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::cell_groups::*;
use cellar::sdk::{consensus::*, *};
use managed_fixture::{fault, limits, request, start_node, Fixture, TIMEOUT};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

// The common harness also compiles CG-09-only entrypoints. Their owning
// target still checks and executes them; this target selects split/merge.
#[expect(
    dead_code,
    reason = "CG-09 entrypoints belong to native_cell_groups_transitions"
)]
#[path = "support/cell_group_transition_context.rs"]
mod bootstrap;
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
#[path = "support/cell_group_split_merge_catalog.rs"]
mod partition;
pub use source_fixture::rustfs;
#[path = "support/cell_group_host_context.rs"]
mod source_fixture;

pub use source_fixture::runtime_fixture::catalog_fault;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_equal_union_whole_cells() -> Result<()> {
    partition::split_equal_union_whole_cells().await?;
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::merge::qualify_operator_split(rooted)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_requires_adjacent_compatible_ranges() -> Result<()> {
    eprintln!("merge geometry start");
    Box::pin(partition::merge_requires_adjacent_compatible_ranges()).await?;
    eprintln!("merge geometry complete");
    for rooted in [false, true] {
        eprintln!("managed merge start, extents={rooted}");
        bootstrap::transition::qualify_split_merge(rooted).await?;
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
