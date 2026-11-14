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
#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;
pub use source_fixture::rustfs;
#[path = "support/cell_group_rustfs_operator.rs"]
mod rustfs_operator;
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
async fn disaster_retry_and_old_token_refusal() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(bootstrap::transition::qualify_disaster(
            rooted,
            Arc::new(catalog_fault::Store::default()),
            bootstrap::transition::RecoveryPoint::PublishedChildren,
        ))
        .await
        .with_context(|| format!("seeded command retries and old tokens, extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_disaster_retry_and_old_token_refusal() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(with_operator_store(
            "disaster-seeded-retries",
            rooted,
            |store| {
                bootstrap::transition::qualify_disaster(
                    rooted,
                    Arc::new(catalog_fault::Store::around(store)),
                    bootstrap::transition::RecoveryPoint::PublishedChildren,
                )
            },
        ))
        .await
        .with_context(|| {
            format!("RustFS seeded command retries and old tokens, extents={rooted}")
        })?;
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
            Box::pin(with_operator_store(cohort, rooted, |store| {
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
        Box::pin(qualify_outbox(
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
        Box::pin(with_operator_store("disaster-outbox", rooted, |store| {
            qualify_outbox(rooted, Arc::new(catalog_fault::Store::around(store)))
        }))
        .await
        .with_context(|| format!("RustFS uncertain outbox recovery, extents={rooted}"))?;
    }
    Ok(())
}

async fn with_operator_store<T, Work: std::future::Future<Output = Result<T>>>(
    case: &str,
    rooted: bool,
    work: impl FnOnce(Arc<dyn object_store::ObjectStore>) -> Work,
) -> Result<T> {
    let config = rustfs_operator::OperatorStoreConfig {
        rooted,
        connection: rustfs::StoreConfig::load(case, rooted)?,
    };
    let store = config.connection.open(None)?;
    let verified = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let result = rustfs_operator::OPERATOR
        .scope((config, verified.clone()), work(store))
        .await?;
    ensure!(
        verified.load(Ordering::Acquire) == 1,
        "expected one completed cold operator verification"
    );
    Ok(result)
}

async fn qualify_outbox(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    use bootstrap::transition::disaster_effects::{State, SCENE};
    let state = Arc::new(State::default());
    SCENE
        .scope(state.clone(), async {
            Box::pin(bootstrap::transition::qualify_disaster(
                rooted,
                store,
                bootstrap::transition::RecoveryPoint::PublishedChildren,
            ))
            .await?;
            ensure!(
                state.checked.load(Ordering::Acquire),
                "recovered outbox audit was not reached"
            );
            Ok(())
        })
        .await
}
