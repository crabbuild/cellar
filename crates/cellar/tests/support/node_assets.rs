// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual SDK nodes keep cache directories and maintenance budgets independent.
use super::*;
#[allow(dead_code)]
#[path = "assets.rs"]
mod asset_fixture;
use sha2::{Digest as _, Sha256};

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn embedded_nodes_keep_asset_caches_and_shutdown_independent() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("caller-assets-core")?;
    let loading = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = loading
        .run(async { Bucket::open_dev(&directory.path().join("assets.sqlite")) })
        .await??;
    let mut nodes = Vec::new();
    let mut paths = Vec::new();
    for (name, content, max_bytes) in [
        ("assets-a", "first host", 0),
        ("assets-b", "second host", 1024 * 1024),
    ] {
        let root = directory.path().join(format!("{name}-cache"));
        let mut options = AssetCacheOptions::new(&root);
        options.max_bytes = max_bytes;
        let cache = AssetCache::new(options)?;
        let (config, services) = inputs(name, &directory.path().join(name), &core, None, 0).await?;
        let mut graph = graph();
        graph.primary.assets = Some(
            loading
                .run(asset_fixture::resolver(
                    &bucket,
                    name,
                    content.as_bytes(),
                    "404-page",
                    &cache,
                ))
                .await??,
        );
        graph.primary.asset_binding = Some("ASSETS".into());
        let node = Node::start(config, services, graph, applications("unused", None)?).await?;
        ready(&node).await?;
        let digest = format!("{:x}", Sha256::digest(content.as_bytes()));
        paths.push(root.join(&digest[..2]).join(digest));
        nodes.push(node);
    }
    loading.shutdown().await;
    let client = reqwest::Client::builder().no_proxy().build()?;
    for (node, expected) in nodes.iter().zip(["first host", "second host"]) {
        let url = format!("http://{}/plain.txt", node.info().public_address);
        let response = client.get(&url).send().await?;
        ensure!(response.status() == 200 && response.text().await? == expected);
        let response = client.get(&url).header("Range", "bytes=1-4").send().await?;
        ensure!(response.status() == 206 && response.text().await? == expected[1..5]);
        ensure!(client.head(&url).send().await?.bytes().await?.is_empty());
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while paths[0].exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    ensure!(
        paths[1].exists(),
        "one host's zero-byte target evicted the other host's assets"
    );
    let first = nodes.remove(0);
    let NodeOutcome::Stopped(report) = first.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("asset host did not stop");
    };
    ensure!(report.ingress_drained && report.application_work_drained && report.cells_drained);
    let second = nodes.pop().unwrap();
    let response = client
        .get(format!("http://{}/plain.txt", second.info().public_address))
        .send()
        .await?;
    ensure!(response.status() == 200 && response.text().await? == "second host");
    ensure!(paths[1].exists());
    let NodeOutcome::Stopped(report) = second.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("sibling asset host did not stop");
    };
    ensure!(report.ingress_drained && report.application_work_drained && report.cells_drained);
    ensure!(
        core.handle().spawn(async { 42 }).await? == 42,
        "asset shutdown closed the caller executor"
    );
    Ok(())
}
