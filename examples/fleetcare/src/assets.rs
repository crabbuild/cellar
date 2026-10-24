// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Publish immutable technician documentation, then use the normal SDK asset
//! resolver and generation binding. Publication is application bootstrap work.
use anyhow::{Context, Result};
use cellar::{
    assets::{AssetCache, AssetCacheOptions, AssetResolver},
    bucket::Bucket,
    protocol::*,
};
use sha2::{Digest, Sha256};

pub const MANUAL: &[u8] = b"FleetCare technician manual v1\nInspect device, reserve inventory, record work, attach evidence.\n";

pub async fn install(
    graph: &mut cellar::sdk::DeploymentGraph,
    bucket: &Bucket,
    cache: &AssetCache,
) -> Result<()> {
    let digest = format!("{:x}", Sha256::digest(MANUAL));
    let mut index = AssetIndex {
        schema_version: 1,
        config: AssetConfig {
            binding: Some("ASSETS".into()),
            run_worker_first: RunWorkerFirst::Routes(vec![
                "/api/*".into(),
                "/diagnostics/*".into(),
            ]),
            ..Default::default()
        },
        ..Default::default()
    };
    index.entries.insert(
        "/assets/manual.txt".into(),
        AssetEntry {
            sha256: digest.clone(),
            bytes: MANUAL.len() as u64,
            content_type: Some("text/plain".into()),
        },
    );
    bucket
        .put(
            &asset_blob_key(&digest).context("invalid manual digest")?,
            MANUAL.to_vec(),
        )
        .await?;
    let bytes = serde_json::to_vec(&index)?;
    let reference = AssetManifestRef {
        index: "assets.json".into(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        file_count: 1,
        total_bytes: MANUAL.len() as u64,
    };
    bucket.put("fleetcare/assets/v1/assets.json", bytes).await?;
    graph.primary.asset_binding = Some("ASSETS".into());
    graph.primary.assets = Some(
        AssetResolver::load(
            bucket,
            "fleetcare/assets/v1",
            &reference,
            false,
            None,
            cache,
        )
        .await?,
    );
    Ok(())
}

/// The daemon executable owns this preflight runtime and captures its own
/// command arguments and credentials. The Cellar SDK receives built clients.
#[allow(clippy::disallowed_methods)]
pub fn daemon_bootstrap(graph: &mut cellar::sdk::DeploymentGraph) -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let value = |key: &str| -> Result<String> {
        let i = args
            .iter()
            .position(|a| a == key)
            .with_context(|| format!("missing {key}"))?;
        args.get(i + 1)
            .cloned()
            .with_context(|| format!("missing {key} value"))
    };
    let name = value("--bucket")?;
    let client = std::sync::Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(&name)
            .with_endpoint(value("--endpoint")?)
            .with_region(value("--region")?)
            .with_allow_http(true)
            .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID")?)
            .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY")?)
            .build()?,
    );
    let bucket = Bucket::from_clients(
        client.clone(),
        client.clone(),
        client,
        cellar::bucket::StorageBackend::S3,
        name,
        String::new(),
    )?;
    let cache = AssetCache::new(AssetCacheOptions::new(std::env::var(
        "CELLAR_ASSET_CACHE_DIR",
    )?))?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(install(graph, &bucket, &cache))
}
