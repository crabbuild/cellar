// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refusing S3 bytes retains the exact nonserving learner, without peer fallback.
use super::*;
use crate::managed_fixture::seed_failures::{restart_seed_cleanup, retained_seed_debt};

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
