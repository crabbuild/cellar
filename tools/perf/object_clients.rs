// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit application-owned S3 clients used by the measuring frontends.

use anyhow::Result;
use cellar::bucket::{Bucket, StorageBackend};
use object_store::{aws::AmazonS3Builder, RetryConfig};
use std::sync::Arc;

pub(super) struct Clients {
    pub(super) data: Bucket,
    pub(super) lease: Bucket,
}

pub(super) fn connect(
    builder: AmazonS3Builder,
    conditional_retry: RetryConfig,
    name: String,
) -> Result<Clients> {
    let data = bucket(&builder, conditional_retry.clone(), &name)?;
    // A pooled connection is driven by the runtime that first opened it.
    // Lease CAS must not borrow a connection owned by application work.
    let lease = bucket(&builder, conditional_retry, &name)?;
    Ok(Clients { data, lease })
}

fn bucket(builder: &AmazonS3Builder, conditional_retry: RetryConfig, name: &str) -> Result<Bucket> {
    let ordinary = Arc::new(builder.clone().build()?);
    let conditional = Arc::new(builder.clone().with_retry(conditional_retry).build()?);
    Bucket::from_clients(
        ordinary.clone(),
        conditional,
        ordinary,
        StorageBackend::S3,
        name.to_owned(),
        String::new(),
    )
}

#[cfg(test)]
#[path = "object_clients_tests.rs"]
mod tests;
