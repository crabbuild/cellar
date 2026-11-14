// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit isolated S3 clients shared by independent managed processes.
use super::*;
use cellar::bucket::{Bucket, StorageBackend};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore},
    prefix::PrefixStore,
};

struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "managed recovery uses exact rooted keys".into(),
        })
    }
}

pub(super) fn open(
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    access: &str,
    secret: &str,
    bucket_prefix: &str,
) -> Result<Bucket> {
    ensure!(
        (endpoint == "http://rustfs:9000" || endpoint.starts_with("http://127.0.0.1:"))
            && (prefix.starts_with("cellar-cg-dr-") || prefix.starts_with("cell-groups/")),
        "isolated RustFS run required"
    );
    let builder = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_access_key_id(access)
        .with_secret_access_key(secret)
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch);
    let store = Arc::new(PrefixStore::new(builder.clone().build()?, prefix));
    // A conditional write's lost reply must remain available for exact SDK
    // reconciliation. Ordinary immutable reads keep their separate retry policy.
    let cas = Arc::new(PrefixStore::new(
        builder
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()?,
        prefix,
    ));
    Bucket::from_clients(
        store,
        cas,
        Arc::new(NoListing),
        StorageBackend::S3,
        bucket.to_owned(),
        bucket_prefix.to_owned(),
    )
}
