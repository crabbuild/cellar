//! Qualify a real staged Node package against the local durable bucket protocol.
#![cfg(all(feature = "node", unix))]
// Test process orchestration runs outside the cell execution domain.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, Package},
};
use reqwest::Client;
use serde_json::Value;
use std::{
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

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
    async fn start(root: &Path, name: &str, bucket: &Path, program: &Path) -> Result<Self> {
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
        command
            .env("CELLAR_NATIVE_FLEET", "1")
            .env("CELLAR_BUCKET", "cellar-dev")
            .env("CELLAR_INTERNAL_DEV_STORE", bucket);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CELLAR_NODE_PACKAGE built with examples/node/project.json"]
async fn published_node_package_installs_boots_and_recovers_from_bucket() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let staged = PathBuf::from(
        std::env::var_os("CELLAR_NODE_PACKAGE").context("CELLAR_NODE_PACKAGE required")?,
    )
    .canonicalize()?;
    let package: Package = serde_json::from_slice(&std::fs::read(staged.join("package.json"))?)?;
    package.validate()?;
    let runtime = package
        .primary
        .native
        .as_ref()
        .context("native deployment required")?
        .node
        .first()
        .context("Node runtime required")?;
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let database = root.join("bucket.sqlite");
    let bucket = Bucket::open_dev(&database)?;
    package.publish_staged(&bucket, &staged).await?;
    let installed = root.join("installed-node-host");
    package
        .install_with_runtimes(
            &bucket,
            current_target(),
            &installed,
            None,
            Some(&runtime.prefix),
        )
        .await?;
    let check = Command::new(&installed)
        .env_clear()
        .arg("--check-native-package")
        .arg(staged.join("package.json"))
        .output()?;
    ensure!(
        check.status.success(),
        "installed host rejected package: {}",
        String::from_utf8_lossy(&check.stderr)
    );
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    // Distinct node identities and data directories force the second process
    // to recover SQLite from bucket durability, not the first process's files.
    for (name, before) in [("node-package-first", 0), ("node-package-second", 3)] {
        let mut node = Node::start(root, name, &database, &installed).await?;
        node.ready(&client).await?;
        let response = client
            .get(format!("{}/compatibility", node.ingress))
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        ensure!(
            status.is_success(),
            "packaged npm failed: {status} {body}\n{}",
            node.logs()?
        );
        let compatibility: Value = serde_json::from_str(&body)?;
        ensure!(compatibility["nativeAddon"] == true);
        ensure!(compatibility["cjs"] == "cellarNode");
        ensure!(compatibility["component"] == "<strong>Cellar + Node</strong>");
        let response = client
            .get(format!("{}/alice", node.ingress))
            .send()
            .await?
            .error_for_status()?;
        ensure!(serde_json::from_str::<Value>(&response.text().await?)?["count"] == before);
        let response = client
            .post(format!("{}/alice", node.ingress))
            .body("3")
            .send()
            .await?
            .error_for_status()?;
        ensure!(serde_json::from_str::<Value>(&response.text().await?)?["count"] == before + 3);
        node.stop(&client).await?;
    }
    Ok(())
}
