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

mod formal;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_disaster_blocks_old_lifecycle() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(formal::cancel_import(rooted)).await?;
        Box::pin(formal::import(
            rooted,
            formal::backend("managed-disaster-old-lifecycle", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fence_all_possible_old_writers() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(formal::resume(
            rooted,
            formal::backend("fence-old-writers-resume", rooted)?,
        ))
        .await?;
        Box::pin(formal::topology_wins(
            rooted,
            formal::backend("fence-old-writers-topology", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_disks_deleted_restore_all_cells() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(formal::all_disks_deleted_rooted(
            rooted,
            formal::backend("all-disks-deleted", rooted)?,
        ))
        .await?;
    }
    Ok(())
}

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

/// Exercise the artifact boundary with real managed voters. Raw synthetic bytes
/// remain in the private memory snapshot, while the CI report stays metadata-only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zz_failure_diagnostics_keep_private_bytes_and_canonical_state() -> Result<()> {
    qualify_failure_observations(&[]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zz_six_node_failure_observations_preserve_canonical_state() -> Result<()> {
    qualify_failure_observations(&[10, 11, 12]).await
}

async fn qualify_failure_observations(extra_nodes: &[u8]) -> Result<()> {
    use bootstrap::transition::diagnostics::{memory, observations};
    use managed_fixture::ManagedCluster;
    use object_store::ObjectStore as _;
    let mut cluster = Box::pin(ManagedCluster::start(false, extra_nodes)).await?;
    let expected_nodes = 3 + extra_nodes.len();
    for root in &mut cluster.roots {
        root.disable_cleanup(true);
        eprintln!(
            "retained managed transition disk: {}",
            root.path().display()
        );
    }
    let checked = async {
        let fixture = &cluster.fixtures[0];
        let key = object_store::path::Path::from("diagnostic-private/sentinel");
        let private_bytes = b"test-only-private-object-not-for-ci";
        fixture
            .store
            .inner
            .put(&key, private_bytes.to_vec().into())
            .await?;
        let before = fixture.catalog.read(TIMEOUT).await?.revision();
        let directory = fixture
            .domain
            .run(cluster.directory.observe())
            .await??
            .digest();
        managed_fixture::operator::qualify_seal_observation(fixture, &cluster.directory).await?;
        observations(&cluster).await?;
        memory(&fixture.store, cluster.roots[0].path(), "control").await?;
        let report = std::fs::read(
            cluster.roots[0]
                .path()
                .join("transition-failure-observations.json"),
        )?;
        ensure!(!report
            .windows(private_bytes.len())
            .any(|bytes| bytes == private_bytes));
        let report: serde_json::Value = serde_json::from_slice(&report)?;
        ensure!(report["format"] == 1 && report["transactional"] == false);
        ensure!(report["catalog_revision"] == before);
        let expected = [
            "case",
            "catalog_revision",
            "control_before_probes",
            "format",
            "nodes",
            "physical",
            "sample_phase",
            "transactional",
        ];
        let keys = report.as_object().context("observation object")?;
        ensure!(keys.len() == expected.len() && expected.iter().all(|key| keys.contains_key(*key)));
        let nodes = report["nodes"].as_array().context("node observations")?;
        let physical = report["physical"]
            .as_array()
            .context("physical observations")?;
        ensure!(nodes.len() == expected_nodes && nodes.iter().all(|row| row["available"] == true));
        ensure!(
            physical.len() == expected_nodes
                && physical.iter().all(|row| row["available"] == true
                    && row["histories"].as_u64().is_some()
                    && row["initializing"] == false)
        );
        ensure!(fixture.catalog.read(TIMEOUT).await?.revision() == before);
        ensure!(
            fixture
                .domain
                .run(cluster.directory.observe())
                .await??
                .digest()
                == directory
        );
        let snapshot = cluster.roots[0].path().join("transition-memory-control");
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(snapshot.join("manifest.json"))?)?;
        let object = manifest["objects"]
            .as_array()
            .context("memory inventory")?
            .iter()
            .find(|row| row["object"] == key.as_ref())
            .context("retained private object")?;
        let file = object["file"].as_str().context("private object filename")?;
        ensure!(!file.contains('/') && !file.contains('\\'));
        ensure!(std::fs::read(snapshot.join(file))? == private_bytes);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = futures_util::future::join_all(
        std::mem::take(&mut cluster.nodes)
            .into_iter()
            .zip(&cluster.hosts)
            .map(|(node, host)| async move {
                ensure!(matches!(
                    node.shutdown(ShutdownMode::Preserve).await?,
                    NodeOutcome::Stopped(_)
                ));
                Ok::<_, anyhow::Error>(host.close().await?)
            }),
    )
    .await;
    cluster.fixtures[0].domain.shutdown().await;
    checked?;
    for report in joined {
        report?;
    }
    Ok(())
}
