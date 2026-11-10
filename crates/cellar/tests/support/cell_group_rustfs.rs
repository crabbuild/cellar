// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit local RustFS configuration; never fall back to memory or ambient IAM.
use anyhow::{ensure, Context as _, Result};
use object_store::{aws::AmazonS3Builder, path::Path, prefix::PrefixStore, ObjectStore};
use std::{
    env,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// The same explicit connection opens the source store and the cold operator.
/// Never infer a store prefix from a pending operation or publication request.
#[derive(Clone)]
pub struct OperatorStoreConfig {
    pub rooted: bool,
    pub endpoint: String,
    pub bucket: String,
    pub prefix: String,
    pub access: String,
    pub secret: String,
}

tokio::task_local! {
    static OPERATOR: (OperatorStoreConfig, Arc<AtomicUsize>);
}

pub async fn with_operator_store<T, Work: Future<Output = Result<T>>>(
    case: &str,
    rooted: bool,
    work: impl FnOnce(Arc<dyn ObjectStore>) -> Work,
) -> Result<T> {
    let config = OperatorStoreConfig::load(case, rooted)?;
    let store = config.open(None)?;
    let verified = Arc::new(AtomicUsize::new(0));
    let result = OPERATOR
        .scope((config, verified.clone()), work(store))
        .await?;
    ensure!(
        verified.load(Ordering::Acquire) == 1,
        "expected one completed cold operator verification"
    );
    Ok(result)
}

pub fn operator_config() -> Option<OperatorStoreConfig> {
    OPERATOR.try_with(|(config, _)| config.clone()).ok()
}

pub fn operator_verified() -> Result<()> {
    OPERATOR
        .try_with(|(_, verified)| verified.fetch_add(1, Ordering::AcqRel))
        .context("explicit cold operator store scope")?;
    Ok(())
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
    OperatorStoreConfig::load(case, rooted)?.open(retry)
}

impl OperatorStoreConfig {
    fn load(case: &str, rooted: bool) -> Result<Self> {
        let endpoint = env::var("CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT")?;
        ensure!(
            endpoint.starts_with("http://127.0.0.1:"),
            "local RustFS endpoint"
        );
        let prefix = env::var("CELLAR_CELL_GROUPS_RUSTFS_PREFIX")?;
        ensure!(!prefix.is_empty(), "isolated RustFS prefix");
        Ok(Self {
            rooted,
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

    fn open(&self, retry: Option<object_store::RetryConfig>) -> Result<Arc<dyn ObjectStore>> {
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
