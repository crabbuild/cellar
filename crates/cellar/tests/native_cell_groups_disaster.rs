// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-13 managed recovery of unpublished parents and acknowledged child tails.
//! Real TLS voters and disks; platform fencing is qualified separately.
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
#[expect(
    dead_code,
    reason = "shared transition fixtures also expose non-disaster entry points"
)]
mod bootstrap;
#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
pub use source_fixture::rustfs;
#[path = "support/cell_group_host_context.rs"]
mod source_fixture;

pub use source_fixture::runtime_fixture::catalog_fault;

#[path = "support/cell_group_managed_process.rs"]
mod process;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transition_disaster_selects_complete_closure() -> Result<()> {
    use bootstrap::transition::RecoveryPoint;
    for rooted in [false, true] {
        for point in [
            RecoveryPoint::UnpublishedParents,
            RecoveryPoint::PublishedChildren,
        ] {
            Box::pin(bootstrap::transition::qualify_disaster(
                rooted,
                Arc::new(catalog_fault::Store::default()),
                point,
            ))
            .await
            .with_context(|| format!("managed disaster {point:?}, extents={rooted}"))?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_transition_disaster_selects_complete_closure() -> Result<()> {
    use bootstrap::transition::RecoveryPoint;
    for rooted in [false, true] {
        for (point, cohort) in [
            (RecoveryPoint::UnpublishedParents, "disaster-parent-cuts"),
            (RecoveryPoint::PublishedChildren, "disaster-child-tails"),
        ] {
            Box::pin(rustfs::with_operator_store(cohort, rooted, |store| {
                bootstrap::transition::qualify_disaster(
                    rooted,
                    Arc::new(catalog_fault::Store::around(store)),
                    point,
                )
            }))
            .await
            .with_context(|| format!("RustFS managed disaster {point:?}, extents={rooted}"))?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disaster_outbox_ambiguity_refuses() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::disaster_effects::qualify(
            rooted,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await
        .with_context(|| format!("managed uncertain outbox recovery, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_disaster_outbox_ambiguity_refuses() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(rustfs::with_operator_store(
            "disaster-outbox",
            rooted,
            |store| {
                bootstrap::transition::disaster_effects::qualify(
                    rooted,
                    Arc::new(catalog_fault::Store::around(store)),
                )
            },
        ))
        .await
        .with_context(|| format!("RustFS uncertain outbox recovery, extents={rooted}"))?;
    }
    Ok(())
}
