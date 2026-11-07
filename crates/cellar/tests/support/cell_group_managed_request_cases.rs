// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed test entry points shared with the public acceptance target.

use super::*;
use crate::managed_fixture::requests::{request_path, run_native};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_native_axum_read_write_and_retained_restart() -> Result<()> {
    Box::pin(run_native()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_native_axum_read_write_and_retained_restart() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(request_path(
            rooted,
            Arc::new(fault::Store::around(rustfs::store("managed-http", rooted)?)),
        ))
        .await?;
    }
    Ok(())
}
