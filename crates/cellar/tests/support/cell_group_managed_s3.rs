// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refusing S3 bytes retains the exact nonserving learner, without peer fallback.
use super::*;
use crate::managed_fixture::seed_failures::{restart_seed_cleanup, retained_seed_debt};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_minority_cannot_reconfigure_from_s3() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(minority_with_store(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_minority_cannot_reconfigure_from_s3() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-s3-minority",
            rooted,
        )?));
        Box::pin(minority_with_store(rooted, store)).await?;
    }
    Ok(())
}

async fn minority_with_store(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let result = Box::pin(check_minority_replacement(&cluster)).await;
    let stopped = close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| format!("S3 minority with extent images={rooted}"))?;
    stopped
}

async fn check_minority_replacement(cluster: &ManagedCluster) -> Result<()> {
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStore as _;

    let (seed, primary, _, _, execution) = Box::pin(reserve_replacement(cluster, true)).await?;
    let source = &cluster.hosts[primary];
    let spare = &cluster.hosts[3];
    let group = source.group(cluster.history)?;
    let fixture = &cluster.fixtures[0];
    let cell = source.cell(seed.cells[1].clone())?;
    let original = &seed.receipts[1];
    let s3 = cell
        .command(
            &cell.contract("Add")?,
            11_i64.to_be_bytes().to_vec(),
            CommandOptions {
                durability: Durability::S3,
                ..CommandOptions::new(original.id, seed.retry)
            },
        )
        .await?;
    ensure!(
        s3.durability == Durability::S3
            && s3.version == original.version
            && s3.witness == original.witness
            && s3.result == original.result
    );

    let list_claims = || {
        let history = cluster.history;
        let selector = format!(
            "/claims/{}/{}/",
            hex::encode(history.group.bytes()),
            hex::encode(history.incarnation.bytes()),
        );
        fixture
            .store
            .list(None)
            .try_filter_map(move |object| {
                let claim = object.location.as_ref().contains(&selector);
                std::future::ready(Ok(claim.then_some(object.location)))
            })
            .try_collect::<std::collections::BTreeSet<_>>()
    };
    let before = group.observe(TIMEOUT).await?;
    let claims = list_claims().await?;
    ensure!(claims.len() == 3, "expected the original three claims");
    let cut = fixture.catalog.read(TIMEOUT).await?;
    ensure!(
        spare.observe_node(TIMEOUT).await?.node.groups == 0
            && matches!(spare.group(cluster.history), Err(RuntimeError::NotFound))
    );
    for (index, voter) in cluster.hosts[..3].iter().enumerate() {
        if index != primary {
            let report = voter.close().await?;
            ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
        }
    }
    let result = fixture
        .catalog
        .reconcile_movement(&cut, &execution, source, Duration::from_millis(800))
        .await;
    ensure!(
        matches!(result, Err(FleetCatalogError::Unconfirmed)),
        "minority admitted a repair with S3 available: {result:?}"
    );
    let after = group.observe(TIMEOUT).await?;
    ensure!(
        after.membership_revision == before.membership_revision
            && after.status.committed == before.status.committed
    );
    let retained = fixture.catalog.read(TIMEOUT).await?;
    ensure!(retained.revision() == cut.revision());
    let pending = fixture
        .catalog
        .pending_reservations(&retained, TIMEOUT)
        .await?;
    ensure!(pending.len() == 1 && pending[0].reservation == execution.reservation);
    ensure!(
        spare.observe_node(TIMEOUT).await?.node.groups == 0
            && matches!(spare.group(cluster.history), Err(RuntimeError::NotFound))
    );
    ensure!(
        list_claims().await? == claims,
        "minority changed the durable claims"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_s3_seed_refusal_preserves_admission_and_retries_after_failover() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(refused_with_store(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_s3_seed_refusal_and_source_failover() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-s3-failover",
            rooted,
        )?));
        Box::pin(refused_with_store(rooted, store)).await?;
    }
    Ok(())
}

async fn refused_with_store(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let work = Box::pin(replace_with_retry(&cluster, true, true))
        .await
        .and_then(|_| retained_seed_debt(&cluster));
    let stopped = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    let result = match (work, stopped) {
        (Ok(debt), Ok(())) => Box::pin(restart_seed_cleanup(&cluster, debt)).await,
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(stopped)) => Err(anyhow::anyhow!(
            "S3 learner failure: {error:#}; joined shutdown failure: {stopped:#}"
        )),
    };
    cluster.fixtures[0].domain.shutdown().await;
    if result.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained failed managed S3 disk: {}", root.keep().display());
        }
    }
    result
}
