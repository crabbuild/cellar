// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A repaired member returns only through rooted maintenance and fresh resume.
use super::*;
pub(super) use crate::managed_fixture::maintenance::handoff_to;
use crate::managed_fixture::maintenance::maintained_with_loss;
#[path = "cell_group_managed_maintenance_loss.rs"]
mod loss;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_group_maintenance_stop_return_and_resume() -> Result<()> {
    for rooted in [false, true] {
        for joined in [false, true] {
            Box::pin(maintained(rooted, joined)).await?;
        }
    }
    Ok(())
}

async fn maintained(rooted: bool, joined: bool) -> Result<()> {
    Box::pin(maintained_with_store(
        rooted,
        joined,
        Arc::new(fault::Store::default()),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_archive_maintenance_stop_return_and_resume() -> Result<()> {
    for rooted in [false, true] {
        for joined in [false, true] {
            let store = Arc::new(fault::Store::around(rustfs::store(
                &format!("managed-archive-maintenance-joined-{joined}"),
                rooted,
            )?));
            Box::pin(maintained_with_store(rooted, joined, store)).await?;
        }
    }
    Ok(())
}

async fn maintained_with_store(rooted: bool, joined: bool, store: Arc<fault::Store>) -> Result<()> {
    Box::pin(maintained_with_loss(rooted, joined, store, false)).await
}
