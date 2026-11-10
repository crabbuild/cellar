// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A lost read-only observation cannot consume the complete renewal budget.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_maintenance_recovers_one_lost_membership_observation() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(maintained_with_loss(
            rooted,
            true,
            Arc::new(fault::Store::default()),
            true,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_maintenance_recovers_one_lost_membership_observation() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "maintenance-lost-observation",
            rooted,
        )?));
        Box::pin(maintained_with_loss(rooted, true, store, true)).await?;
    }
    Ok(())
}
