// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit local RustFS configuration; never fall back to memory or ambient IAM.
use anyhow::{ensure, Result};
use object_store::{aws::AmazonS3Builder, path::Path, prefix::PrefixStore, ObjectStore};
use std::{env, sync::Arc};

pub fn store(case: &str, rooted: bool) -> Result<Arc<dyn ObjectStore>> {
    let endpoint = env::var("CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT")?;
    ensure!(
        endpoint.starts_with("http://127.0.0.1:"),
        "local RustFS endpoint"
    );
    let prefix = env::var("CELLAR_CELL_GROUPS_RUSTFS_PREFIX")?;
    ensure!(!prefix.is_empty(), "isolated RustFS prefix");
    let prefix = Path::parse(format!(
        "{prefix}/{case}/{}",
        if rooted { "extents" } else { "materialized" }
    ))?;
    let store = AmazonS3Builder::new()
        .with_bucket_name(env::var("CELLAR_CELL_GROUPS_RUSTFS_BUCKET")?)
        .with_endpoint(endpoint)
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(Arc::new(PrefixStore::new(store, prefix)))
}
