// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Process-level native ingress, routing, bucket restore and reload coverage.

// Process orchestration runs outside the cell execution boundary and uses
// host wall-clock deadlines while child nodes own their execution domains.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, Context as _, Result};
use cellar::bucket::Bucket;
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Node {
    process: Child,
    log: PathBuf,
    diagnostics: PathBuf,
    ingress: String,
    internal: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.process.try_wait().ok().flatten().is_none() {
            let _ = self.process.kill();
        }
        let _ = self.process.wait();
    }
}

impl Node {
    async fn start(root: &Path, name: &str, bucket: Option<&Path>) -> Result<Self> {
        Self::start_program(
            root,
            name,
            bucket,
            env!("CARGO_BIN_EXE_cellar-native-counter"),
            None,
        )
        .await
    }

    async fn start_program(
        root: &Path,
        name: &str,
        bucket: Option<&Path>,
        program: &str,
        source: Option<&Path>,
    ) -> Result<Self> {
        let log = root.join(format!("{name}.log"));
        let stdout = File::create(&log)?;
        // Listener announcements are stdout data. Merging stderr into the
        // same file can splice a concurrent diagnostic into their lines.
        let diagnostics = root.join(format!("{name}-stderr.log"));
        let stderr = File::create(&diagnostics)?;
        let mut command = Command::new(program);
        command
            .env_clear()
            .args([
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                "127.0.0.1:0",
            ])
            .env("CELLAR_NODE", name)
            .env("CELLAR_TEST_DATA_DIR", root.join(format!("{name}-data")))
            .env("CELLAR_TOKIO_THREADS", "2")
            .env("CELLAR_PLACEMENT_WEIGHT", "1")
            .env("CELLAR_REBALANCE_INTERVAL_MS", "0")
            .env("CELLAR_DEPLOY_POLL_S", "3600")
            .env("CELLAR_DURABILITY", "bucket")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "8000")
            .env("RUST_LOG", "warn")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if let Some(bucket) = bucket {
            command
                .env("CELLAR_NATIVE_FLEET", "1")
                .env("CELLAR_BUCKET", "cellar-dev")
                .env("CELLAR_INTERNAL_DEV_STORE", bucket);
        }
        if let Some(source) = source {
            command.env("CELLAR_TEST_SCRIPT_PATH", source);
        }
        let mut node = Self {
            process: command.spawn()?,
            log,
            diagnostics,
            ingress: String::new(),
            internal: String::new(),
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let text = std::fs::read_to_string(&node.log)?;
            let address = |prefix: &str| {
                text.lines().find_map(|line| {
                    line.strip_prefix(prefix)
                        .and_then(|rest| rest.split_whitespace().next())
                        .map(|address| format!("http://{address}"))
                })
            };
            if let (Some(ingress), Some(internal)) = (
                address("cellar listening on "),
                address("cellar internal listening on "),
            ) {
                node.ingress = ingress;
                node.internal = internal;
                return Ok(node);
            }
            if let Some(status) = node.process.try_wait()? {
                bail!(
                    "native node {name} exited during boot: {status}\n{}",
                    node.logs()?
                );
            }
            if Instant::now() >= deadline {
                bail!("native node {name} did not boot\n{}", node.logs()?);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn logs(&self) -> Result<String> {
        Ok(format!(
            "stdout:\n{}\nstderr:\n{}",
            std::fs::read_to_string(&self.log)?,
            std::fs::read_to_string(&self.diagnostics)?
        ))
    }

    async fn ready(&mut self, client: &Client) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if client
                .get(format!("{}/.well-known/cellar/health", self.ingress))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
            {
                return Ok(());
            }
            if let Some(status) = self.process.try_wait()? {
                bail!("node exited before readiness: {status}\n{}", self.logs()?);
            }
            if Instant::now() >= deadline {
                bail!("native readiness timed out\n{}", self.logs()?);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn stop(&mut self, client: &Client) -> Result<()> {
        client
            .post(format!("{}/shutdown", self.internal))
            .send()
            .await?
            .error_for_status()?;
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(status) = self.process.try_wait()? {
                anyhow::ensure!(
                    status.success(),
                    "native shutdown failed: {status}\n{}",
                    self.logs()?
                );
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("native node did not complete shutdown");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn counter(client: &Client, node: &Node, name: &str, write: bool) -> Result<(u64, String)> {
    let response = client
        .request(
            if write { Method::POST } else { Method::GET },
            format!("{}/counters/{name}", node.ingress),
        )
        .send()
        .await?;
    let status = response.status();
    let version = response
        .headers()
        .get("x-native-version")
        .context("missing compiled version")?
        .to_str()?
        .to_string();
    let body = response.text().await?;
    anyhow::ensure!(
        status.is_success(),
        "native counter failed: {status} {body}"
    );
    Ok((
        serde_json::from_str::<Value>(&body)?["count"]
            .as_u64()
            .context("missing count")?,
        version,
    ))
}

async fn deployment(
    bucket: &Bucket,
    version: &str,
    executable: &std::path::Path,
) -> Result<String> {
    deployment_with_assets(bucket, version, version, executable).await
}

async fn deployment_with_assets(
    bucket: &Bucket,
    version: &str,
    asset_revision: &str,
    executable: &Path,
) -> Result<String> {
    use cellar::native_deploy::{current_target, executable_identity, ApplicationConfig, Package};
    let artifact = executable_identity(executable, current_target())?;
    let root = executable.parent().context("executable has no directory")?;
    let assets = root.join(format!("assets-{asset_revision}"));
    std::fs::create_dir_all(assets.join("counters"))?;
    std::fs::write(
        assets.join("index.html"),
        format!("native assets {asset_revision}"),
    )?;
    std::fs::write(assets.join("counters/tenant-acme"), "static counter shadow")?;
    std::fs::write(assets.join("counters/static.txt"), "static route exclusion")?;
    std::fs::write(
        assets.join(format!("{asset_revision}-only.txt")),
        format!("only {asset_revision}"),
    )?;
    std::fs::write(
        assets.join("_headers"),
        format!("/*\n  X-Asset-Version: {asset_revision}\n"),
    )?;
    std::fs::write(assets.join("_redirects"), "/old / 302\n")?;
    let config: ApplicationConfig = serde_json::from_value(json!({
        "script_name": "native-counter", "application_version": version,
        "compatibility_date":"2026-01-01",
        "cells": ["Counter"],
        "bindings": [{"type": "durable_object_namespace", "name": "COUNTERS", "class_name": "Counter"}],
        "assets": {"directory":format!("assets-{asset_revision}"), "binding":"ASSETS",
            "not_found_handling":"single-page-application",
            "run_worker_first":["/counters/*","!/counters/static.txt","/assets-binding/*"]},
    }))?;
    let staged = root.join(format!("package-{version}-{asset_revision}"));
    let primary = config.stage_manifest(vec![artifact.clone()], root, &staged)?;
    std::fs::copy(executable, staged.join(&artifact.sha256))?;
    let package = Package {
        primary,
        cohosted: Vec::new(),
    };
    package.publish_staged(bucket, &staged).await?;
    Ok(package.primary.version)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compiled_daemon_routes_persists_reloads_and_hands_off() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let root = tempfile::tempdir()?;
    // Freeze the compiled artifact: another Cargo feature build can replace
    // target/debug binaries while a process test is still exercising reload.
    let executable = root.path().join("compiled-host");
    std::fs::copy(env!("CARGO_BIN_EXE_cellar-native-counter"), &executable)?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    // Embedded applications use the same listener and Actor without a bucket.
    let mut local = Node::start(root.path(), "native-local", None).await?;
    local.ready(&client).await?;
    assert_eq!(
        counter(&client, &local, "alice", true).await?,
        (1, "v1".into())
    );
    assert_eq!(counter(&client, &local, "bob", false).await?.0, 0);
    assert_eq!(counter(&client, &local, "alice", false).await?.0, 1);
    assert_eq!(
        client
            .post(format!("{}/reload", local.internal))
            .send()
            .await?
            .status(),
        503
    );
    local.stop(&client).await?;

    // A real local development store backs leases and LTX for two processes.
    // This proves production protocol integration, not cloud failure coverage.
    let database = root.path().join("bucket.sqlite");
    let bucket = Bucket::open_dev(&database)?;
    deployment(&bucket, "v1", &executable).await?;
    let pointer = cellar::fleet::read_current_pointer(&bucket).await?;
    let manifest: cellar::protocol::Manifest = serde_json::from_slice(
        &bucket
            .get(&format!("{}/manifest.json", pointer.prefix))
            .await?
            .unwrap()
            .0,
    )?;
    let package = cellar::native_deploy::Package {
        primary: manifest,
        cohosted: Vec::new(),
    };
    let installed = root.path().join("native-node");
    package
        .install(&bucket, cellar::native_deploy::current_target(), &installed)
        .await?;
    let installed = installed.to_str().unwrap();
    let mut first = Node::start_program(
        root.path(),
        "native-first",
        Some(&database),
        installed,
        None,
    )
    .await?;
    let mut second = Node::start_program(
        root.path(),
        "native-second",
        Some(&database),
        installed,
        None,
    )
    .await?;
    first.ready(&client).await?;
    second.ready(&client).await?;
    for node in [&first, &second] {
        let response = client
            .get(format!("{}/", node.ingress))
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.headers()["x-asset-version"], "v1");
        assert_eq!(response.text().await?, "native assets v1");
        let response = client
            .get(format!("{}/assets-binding/", node.ingress))
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.headers()["x-native-version"], "v1");
        assert_eq!(response.text().await?, "native assets v1");
        let response = client
            .get(format!("{}/counters/static.txt", node.ingress))
            .send()
            .await?
            .error_for_status()?;
        assert!(response.headers().get("x-native-version").is_none());
        assert_eq!(response.text().await?, "static route exclusion");
        let response = client
            .get(format!("{}/missing-navigation", node.ingress))
            .header("sec-fetch-mode", "navigate")
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.text().await?, "native assets v1");
        let redirect_client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let response = redirect_client
            .get(format!("{}/old", node.ingress))
            .send()
            .await?;
        assert_eq!(response.status(), 302);
        assert_eq!(response.headers()["location"], "/");
    }
    assert_eq!(counter(&client, &first, "tenant-acme", true).await?.0, 1);
    assert_eq!(counter(&client, &second, "tenant-acme", true).await?.0, 2);
    assert_eq!(counter(&client, &first, "tenant-acme", false).await?.0, 2);

    // Updating assets changes the deployment without compiling new code or
    // requiring another application registration. Business state stays intact.
    let asset_revision = deployment_with_assets(&bucket, "v1", "v1-assets", &executable).await?;
    for node in [&first, &second] {
        let outcome: Value = client
            .post(format!("{}/reload", node.internal))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(outcome["version"], asset_revision);
        let response = client
            .get(format!("{}/assets-binding/", node.ingress))
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.headers()["x-native-version"], "v1");
        assert_eq!(response.headers()["x-asset-version"], "v1-assets");
        assert_eq!(response.text().await?, "native assets v1-assets");
        assert_eq!(
            counter(&client, node, "tenant-acme", false).await?,
            (2, "v1".into())
        );
    }

    let v2_deployment = deployment(&bucket, "v2", &executable).await?;
    // Asset version-skew fallback serves a new content path even while the
    // native application still executes its boot generation.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = client
            .get(format!("{}/v2-only.txt", first.ingress))
            .send()
            .await?;
        if response.status().is_success() && response.text().await? == "only v2" {
            break;
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "native asset fallback did not refresh"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        counter(&client, &first, "tenant-acme", false).await?.1,
        "v1"
    );
    for node in [&first, &second] {
        let outcome: Value = client
            .post(format!("{}/reload", node.internal))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(outcome["outcome"], "adopted");
        assert_eq!(outcome["version"], v2_deployment);
        let response = client
            .get(format!("{}/assets-binding/", node.ingress))
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.headers()["x-native-version"], "v2");
        assert_eq!(response.headers()["x-asset-version"], "v2");
        assert_eq!(response.text().await?, "native assets v2");
        assert_eq!(
            counter(&client, node, "tenant-acme", false).await?,
            (2, "v2".into())
        );
    }
    let instance = client
        .get(format!("{}/counters/tenant-acme", first.ingress))
        .send()
        .await?
        .error_for_status()?
        .headers()["x-native-instance"]
        .to_str()?
        .to_string();
    client
        .post(format!("{}/reload", first.internal))
        .send()
        .await?
        .error_for_status()?;
    let fresh = client
        .get(format!("{}/counters/tenant-acme", first.ingress))
        .send()
        .await?
        .error_for_status()?
        .headers()["x-native-instance"]
        .to_str()?
        .to_string();
    assert_ne!(
        instance, fresh,
        "forced reload must reconstruct compiled application state"
    );
    // A declared but incompatible executable must not adopt, even when its
    // application code version is registered in the current process.
    let mut artifact = cellar::native_deploy::executable_identity(
        &executable,
        cellar::native_deploy::current_target(),
    )?;
    artifact.target = "different-target".into();
    let config: cellar::native_deploy::ApplicationConfig = serde_json::from_value(json!({
        "script_name": "native-counter", "application_version": "v2", "cells": ["Counter"],
        "bindings": [{"type": "durable_object_namespace", "name": "COUNTERS", "class_name": "Counter"}],
    }))?;
    cellar::native_deploy::Package {
        primary: config.manifest(vec![artifact.clone()])?,
        cohosted: Vec::new(),
    }
    .publish(
        &bucket,
        &std::collections::BTreeMap::from([(artifact.sha256, executable.clone())]),
    )
    .await?;
    for node in [&first, &second] {
        let response = client
            .post(format!("{}/reload", node.internal))
            .send()
            .await?;
        assert_eq!(response.status(), 422);
        let failure: Value = response.json().await?;
        assert!(failure["error"]
            .as_str()
            .unwrap()
            .contains("running executable is not declared"));
        assert_eq!(
            counter(&client, node, "tenant-acme", false).await?,
            (2, "v2".into())
        );
    }
    // Unknown compiled versions fail before adoption and leave v2 serving.
    deployment(&bucket, "v3", &executable).await?;
    for node in [&first, &second] {
        let response = client
            .post(format!("{}/reload", node.internal))
            .send()
            .await?;
        assert_eq!(response.status(), 422);
        let failure: Value = response.json().await?;
        assert!(failure["error"]
            .as_str()
            .unwrap()
            .contains("no compiled application registered"));
        assert_eq!(
            counter(&client, node, "tenant-acme", false).await?,
            (2, "v2".into())
        );
    }
    // Move ownership through the real graceful drain. The surviving process
    // restores the committed SQLite state from the retained replication path.
    first.stop(&client).await?;
    assert_eq!(
        counter(&client, &second, "tenant-acme", true).await?,
        (3, "v2".into())
    );
    second.stop(&client).await?;
    deployment(&bucket, "v2", &executable).await?;
    let mut restarted = Node::start_program(
        root.path(),
        "native-restarted",
        Some(&database),
        installed,
        None,
    )
    .await?;
    restarted.ready(&client).await?;
    assert_eq!(
        counter(&client, &restarted, "tenant-acme", false).await?,
        (3, "v2".into())
    );
    restarted.stop(&client).await?;
    Ok(())
}
