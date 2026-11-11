// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit local RustFS configuration; never fall back to memory or ambient IAM.
use anyhow::{ensure, Result};
use object_store::{aws::AmazonS3Builder, path::Path, prefix::PrefixStore, ObjectStore};
use std::{env, sync::Arc};

/// The same explicit connection opens the source store and the cold operator.
/// Never infer a store prefix from a pending operation or publication request.
#[derive(Clone)]
pub(crate) struct StoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub access: String,
    pub secret: String,
}

pub fn store(case: &str, rooted: bool) -> Result<Arc<dyn ObjectStore>> {
    configured_store(case, rooted, None)
}

pub fn outage_store(case: &str, rooted: bool) -> Result<Arc<dyn ObjectStore>> {
    // A stopped backend must expose its actual connection failure promptly.
    // Other RustFS cases keep their existing adapter retry policy.
    configured_store(
        case,
        rooted,
        Some(object_store::RetryConfig {
            max_retries: 0,
            ..Default::default()
        }),
    )
}

// The external backend controller owns this host directory, outside node storage.
#[allow(clippy::disallowed_methods)]
pub fn control_directory() -> Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from(env::var("CELLAR_CELL_GROUPS_RUSTFS_CONTROL")?);
    ensure!(
        path.is_absolute() && path.is_dir(),
        "explicit owned backend control"
    );
    Ok(path)
}

fn configured_store(
    case: &str,
    rooted: bool,
    retry: Option<object_store::RetryConfig>,
) -> Result<Arc<dyn ObjectStore>> {
    StoreConfig::load(case, rooted)?.open(retry)
}

impl StoreConfig {
    pub(crate) fn load(case: &str, rooted: bool) -> Result<Self> {
        let endpoint = env::var("CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT")?;
        ensure!(
            endpoint.starts_with("http://127.0.0.1:"),
            "local RustFS endpoint"
        );
        let prefix = env::var("CELLAR_CELL_GROUPS_RUSTFS_PREFIX")?;
        ensure!(!prefix.is_empty(), "isolated RustFS prefix");
        Ok(Self {
            endpoint,
            bucket: env::var("CELLAR_CELL_GROUPS_RUSTFS_BUCKET")?,
            prefix: Path::parse(format!(
                "{prefix}/{case}/{}",
                if rooted { "extents" } else { "materialized" }
            ))?
            .to_string(),
            access: env::var("AWS_ACCESS_KEY_ID")?,
            secret: env::var("AWS_SECRET_ACCESS_KEY")?,
        })
    }

    pub(crate) fn open(
        &self,
        retry: Option<object_store::RetryConfig>,
    ) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_endpoint(&self.endpoint)
            .with_access_key_id(&self.access)
            .with_secret_access_key(&self.secret)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch);
        if let Some(retry) = retry {
            builder = builder.with_retry(retry);
        }
        Ok(Arc::new(PrefixStore::new(
            builder.build()?,
            Path::parse(&self.prefix)?,
        )))
    }
}
