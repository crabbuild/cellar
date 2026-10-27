// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Qualify a real staged Node package against the local durable bucket protocol.
#![cfg(all(feature = "node", unix))]
// Test process orchestration runs outside the cell execution domain.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    cli_output::{Format, Output},
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

async fn install_staged(root: &Path, staged: &Path) -> Result<PathBuf> {
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
    let database = root.join("bucket.sqlite");
    let bucket = Bucket::open_dev(&database)?;
    package.publish_staged(&bucket, staged).await?;
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
    Ok(installed)
}

async fn run_in_domain(future: impl std::future::Future<Output = Result<()>>) -> Result<()> {
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(future).await;
    domain.shutdown().await;
    result?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CELLAR_NODE_PACKAGE built with examples/node/project.json"]
async fn published_node_package_installs_boots_and_recovers_from_bucket() -> Result<()> {
    run_in_domain(node_package_scenario()).await
}

async fn node_package_scenario() -> Result<()> {
    let staged = PathBuf::from(
        std::env::var_os("CELLAR_NODE_PACKAGE").context("CELLAR_NODE_PACKAGE required")?,
    )
    .canonicalize()?;
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let database = root.join("bucket.sqlite");
    let installed = install_staged(root, &staged).await?;
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

async fn agent_result(client: &Client, node: &Node, id: usize) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let response = client
            .get(format!("{}/agents/task-{id}", node.ingress))
            .send()
            .await?;
        if response.status().is_success() {
            let value: Value = serde_json::from_str(&response.text().await?)?;
            ensure!(value["status"] != "failed", "agent failed: {value}");
            if value["status"] == "completed" {
                ensure!(value["count"] == 3, "wrong tool effect: {value}");
                ensure!(value["toolReceipts"] == 1, "wrong tool receipts: {value}");
                ensure!(value["modelSteps"] == 2, "wrong model steps: {value}");
                let messages = value["messages"].as_array().context("missing transcript")?;
                ensure!(messages
                    .iter()
                    .any(|message| message["role"] == "toolResult"
                        && message["content"][0]["text"] == "3"));
                let final_message = messages.last().context("empty transcript")?;
                ensure!(final_message["role"] == "assistant");
                ensure!(final_message["stopReason"] == "stop");
                return Ok(value);
            }
        }
        ensure!(
            Instant::now() < deadline,
            "agent task-{id} did not complete\n{}",
            node.logs()?
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CELLAR_PI_PACKAGE built with examples/pi-agent/project.json"]
async fn pi_agent_package_runs_concurrent_tasks_and_recovers_after_sigkill() -> Result<()> {
    run_in_domain(pi_agent_package_scenario()).await
}

async fn pi_agent_package_scenario() -> Result<()> {
    let staged =
        PathBuf::from(std::env::var_os("CELLAR_PI_PACKAGE").context("CELLAR_PI_PACKAGE required")?)
            .canonicalize()?;
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let database = root.join("bucket.sqlite");
    let installed = install_staged(root, &staged).await?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut first = Node::start(root, "pi-package-first", &database, &installed).await?;
    first.ready(&client).await?;
    // The packaged app.ts entry has no fault hooks or test-only provider wrapper.
    let results = futures_util::future::join_all((0..8).map(|id| {
        let client = &client;
        let node = &first;
        async move {
            let response = client
                .post(format!("{}/agents/task-{id}", node.ingress))
                .body(r#"{"prompt":"Add three"}"#)
                .send()
                .await?;
            ensure!(
                response.status() == 202,
                "acceptance failed: {}",
                response.status()
            );
            agent_result(client, node, id).await
        }
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    for (method, path, body, expected) in [
        ("POST", "/agents/bad-json", "{", 400),
        ("POST", "/agents/empty", r#"{"prompt":" "}"#, 400),
        ("PUT", "/agents/task-0", "", 405),
        ("GET", "/agents/absent", "", 404),
        ("GET", "/invalid", "", 404),
        ("POST", "/agents/task-0", r#"{"prompt":"Add four"}"#, 409),
    ] {
        let response = client
            .request(method.parse()?, format!("{}{path}", first.ingress))
            .body(body)
            .send()
            .await?;
        ensure!(
            response.status().as_u16() == expected,
            "wrong status for {method} {path}"
        );
    }
    Output::new(Format::Text).line(format_args!(
        "PACKAGED_PI: 8 concurrent tasks completed; HTTP validation and conflicts passed"
    ))?;
    Output::new(Format::Text).line(format_args!("PACKAGED_PI_TRANSCRIPT: {}", results[0]))?;
    first.process.kill()?;
    first.process.wait()?;

    let mut second = Node::start(root, "pi-package-second", &database, &installed).await?;
    second.ready(&client).await?;
    for (id, before) in results.iter().enumerate() {
        let recovered = agent_result(&client, &second, id).await?;
        ensure!(
            &recovered == before,
            "transcript or state changed after recovery for {id}"
        );
    }
    // Concurrent duplicates must acknowledge durable results without more work.
    for outcome in futures_util::future::join_all((0..32).map(|index| {
        client
            .post(format!("{}/agents/task-{}", second.ingress, index % 8))
            .body(r#"{"prompt":"Add three"}"#)
            .send()
    }))
    .await
    {
        ensure!(
            outcome?.status() == 200,
            "duplicate submission did not return completed task"
        );
    }
    for (id, before) in results.iter().enumerate() {
        ensure!(agent_result(&client, &second, id).await? == *before);
    }
    second.stop(&client).await?;
    Output::new(Format::Text).line(format_args!("PACKAGED_PI: SIGKILL recovery preserved 8 complete transcripts; 32 concurrent retries made no changes; host joined"))?;
    Ok(())
}

async fn filesystem_turn(
    client: &Client,
    node: &Node,
    session: usize,
    turn: &str,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let response = client
            .get(format!(
                "{}/sessions/session-{session}/turns/{turn}",
                node.ingress
            ))
            .send()
            .await?;
        if response.status().is_success() {
            let value: Value = response.json().await?;
            ensure!(
                value["status"] != "interrupted",
                "filesystem agent interrupted: {value}"
            );
            if value["status"] == "completed" {
                return Ok(value);
            }
        }
        ensure!(
            Instant::now() < deadline,
            "filesystem session {session}/{turn} timed out\n{}",
            node.logs()?
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CELLAR_FS_PACKAGE built with examples/filesystem-agent/project.json"]
async fn filesystem_agent_package_restores_session_files_and_followup_turns() -> Result<()> {
    run_in_domain(filesystem_package_scenario()).await
}

async fn filesystem_package_scenario() -> Result<()> {
    let staged =
        PathBuf::from(std::env::var_os("CELLAR_FS_PACKAGE").context("CELLAR_FS_PACKAGE required")?)
            .canonicalize()?;
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let database = root.join("bucket.sqlite");
    let installed = install_staged(root, &staged).await?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut first = Node::start(root, "filesystem-package-first", &database, &installed).await?;
    first.ready(&client).await?;
    // Exercise the production entry restored from its verified archive, with
    // independent cell roots and no qualification-only hooks in that package.
    let original = futures_util::future::join_all((0..4).map(|id| {
        let client = &client;
        let node = &first;
        async move {
            let response = client
                .post(format!("{}/sessions/session-{id}/turns/one", node.ingress))
                .json(&serde_json::json!({"prompt":format!("session {id} durable notes")}))
                .send()
                .await?;
            ensure!(
                response.status() == 202,
                "filesystem package acceptance: {}\n{}",
                response.status(),
                node.logs()?
            );
            filesystem_turn(client, node, id, "one").await
        }
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    first.process.kill()?;
    first.process.wait()?;
    std::fs::remove_dir_all(root.join("filesystem-package-first-data"))?;
    let mut second = Node::start(root, "filesystem-package-second", &database, &installed).await?;
    second.ready(&client).await?;
    for (id, before) in original.iter().enumerate() {
        ensure!(
            &filesystem_turn(&client, &second, id, "one").await? == before,
            "packaged filesystem run changed during recovery"
        );
        let duplicate = client
            .post(format!(
                "{}/sessions/session-{id}/turns/one",
                second.ingress
            ))
            .json(&serde_json::json!({"prompt":format!("session {id} durable notes")}))
            .send()
            .await?;
        ensure!(
            duplicate.status() == 200,
            "packaged filesystem duplicate refused"
        );
        let conflict = client
            .post(format!(
                "{}/sessions/session-{id}/turns/one",
                second.ingress
            ))
            .json(&serde_json::json!({"prompt":"different input"}))
            .send()
            .await?;
        ensure!(
            conflict.status() == 409,
            "packaged filesystem accepted conflicting input"
        );
        let followup = client
            .post(format!(
                "{}/sessions/session-{id}/turns/two",
                second.ingress
            ))
            .json(&serde_json::json!({"prompt":"read:/notes.txt"}))
            .send()
            .await?;
        ensure!(
            followup.status() == 202,
            "packaged filesystem follow-up refused"
        );
        let answer = filesystem_turn(&client, &second, id, "two").await?;
        ensure!(
            answer["text"] == format!("session {id} durable notes"),
            "packaged session lost or shared file content: {answer}"
        );
    }
    second.stop(&client).await?;
    Output::new(Format::Text).line(format_args!("PACKAGED_FILESYSTEM: 4 isolated sessions; SIGKILL and empty-node recovery preserved runs, receipts and follow-up file reads; host joined"))?;
    Ok(())
}
