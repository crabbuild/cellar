use anyhow::Result;
use cellar::assets::{AssetCache, AssetResolver};
use cellar::bucket::Bucket;
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::DeploymentGraph;
use cellar::protocol::{
    asset_blob_key, AssetConfig, AssetEntry, AssetIndex, AssetManifestRef, RunWorkerFirst,
};
use sha2::{Digest, Sha256};

pub async fn resolver(
    bucket: &Bucket,
    prefix: &str,
    version: &[u8],
    not_found: &str,
    cache: &AssetCache,
) -> Result<AssetResolver> {
    let mut index = AssetIndex {
        schema_version: 1,
        config: AssetConfig {
            binding: Some("ASSETS".into()),
            html_handling: Some("force-trailing-slash".into()),
            not_found_handling: Some(not_found.into()),
            run_worker_first: RunWorkerFirst::Routes(vec![
                "/api/*".into(),
                "!/api/static/*".into(),
            ]),
            headers: Some(
                "/*\n  X-Asset: shared\n  X-Remove: discard\n/plain.txt\n  ! X-Remove\n".into(),
            ),
            redirects: Some("/old /docs 302\n/proxy /plain.txt 200\n".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    for (path, bytes, content_type) in [
        ("/plain.txt", version, Some("text/plain")),
        ("/index.html", b"home".as_slice(), Some("text/html")),
        ("/docs/index.html", b"docs".as_slice(), Some("text/html")),
        ("/404.html", b"absent".as_slice(), Some("text/html")),
        ("/opaque", b"raw".as_slice(), None),
        ("/雪.txt", b"snow".as_slice(), Some("text/plain")),
        ("/broken.txt", b"correct".as_slice(), Some("text/plain")),
        ("/gone.txt", b"gone".as_slice(), Some("text/plain")),
    ] {
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        index.entries.insert(
            path.into(),
            AssetEntry {
                sha256: sha256.clone(),
                bytes: bytes.len() as u64,
                content_type: content_type.map(str::to_string),
            },
        );
        if path != "/gone.txt" {
            bucket
                .put(
                    &asset_blob_key(&sha256).unwrap(),
                    (if path == "/broken.txt" {
                        b"corrupt".as_slice()
                    } else {
                        bytes
                    })
                    .to_vec(),
                )
                .await?;
        }
    }
    let bytes = serde_json::to_vec(&index)?;
    let reference = AssetManifestRef {
        index: "assets.json".into(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        file_count: index.entries.len() as u32,
        total_bytes: index.entries.values().map(|entry| entry.bytes).sum(),
    };
    bucket.put(&format!("{prefix}/assets.json"), bytes).await?;
    AssetResolver::load(bucket, prefix, &reference, false, None, cache).await
}
pub fn graph(
    script: &str,
    source: &str,
    resolver: Option<AssetResolver>,
    classes: Vec<String>,
) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: source.into(),
            script_name: script.into(),
            do_classes: classes,
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: script.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: Some("ASSETS".into()),
        loader_bindings: Vec::new(),
        assets: resolver,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}
