// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

#[path = "support/native.rs"]
mod native;

struct Images {
    socket: PathBuf,
    sandbox: String,
    fence: String,
    platform: String,
}

impl Images {
    fn configured() -> Result<Self> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
        let manifest: Value = serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
        Ok(Self {
            socket: std::env::var_os("CELLAR_SANDBOX_ENGINE_SOCKET")
                .context(
                    "set CELLAR_SANDBOX_ENGINE_SOCKET to an isolated Docker/Podman Unix socket",
                )?
                .into(),
            sandbox: manifest["image"]["reference"]
                .as_str()
                .context("pinned image reference")?
                .into(),
            platform: manifest["image"]["platform"]
                .as_str()
                .context("pinned image platform")?
                .into(),
            fence: std::env::var("CELLAR_SANDBOX_FENCE_IMAGE")
                .context("set CELLAR_SANDBOX_FENCE_IMAGE to a preloaded fence image ID")?,
        })
    }

    async fn verify(&self) -> Result<()> {
        let docker = cellar::docker::Docker::new(&self.socket);
        let ping = docker.call("GET", "/_ping", None).await?;
        ensure!(ping.status.is_success(), "engine ping: {}", ping.message());
        for (image, platform) in [
            (&self.sandbox, Some(self.platform.as_str())),
            (&self.fence, None),
        ] {
            let reply = docker
                .call("GET", &format!("/images/{image}/json"), None)
                .await?;
            ensure!(
                reply.status.is_success(),
                "preload image {image}: {}",
                reply.message()
            );
            if let Some(platform) = platform {
                let info = reply.json()?;
                ensure!(
                    format!(
                        "{}/{}",
                        info["Os"].as_str().unwrap_or_default(),
                        info["Architecture"].as_str().unwrap_or_default()
                    ) == platform,
                    "Sandbox image platform does not match its pinned manifest"
                );
            }
        }
        Ok(())
    }
}

fn fixture() -> Result<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "fixture build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    Ok(root.join("dist/sandbox-module.mjs"))
}

#[test]
#[ignore = "requires isolated CELLAR_SANDBOX_ENGINE_SOCKET and preloaded CELLAR_SANDBOX_FENCE_IMAGE"]
fn published_sandbox_exec_files_and_processes_use_the_pinned_image() -> Result<()> {
    let images = Images::configured()?;
    let directory = tempfile::tempdir()?;
    let source = Source::module()
        .workers_lifetimes()
        .events([EventKind::Fetch])
        .cell_events(
            "Sandbox",
            [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
        );
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(fixture()?),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run(directory.path(), application, &images))
        },
    )
}

async fn run(path: &Path, application: Arc<dyn Application>, images: &Images) -> Result<()> {
    images.verify().await?;
    let namespace = format!(
        "sandbox-image-{}",
        path.file_name().unwrap().to_string_lossy()
    );
    let host = format!(
        "{:x}",
        Sha256::digest(format!("javascript-test\0namespace:{namespace}").as_bytes())
    );
    let docker = cellar::docker::Docker::new(&images.socket);
    let (node, address) = native::start_configured(path, application, |config, services, graph| {
        // Real local startup includes fencing and image boot, not peer latency.
        config.runtime.handler_budget = Duration::from_secs(120);
        services.containers = Some(cellar::container::EngineOptions {
            socket: images.socket.clone(),
            namespace: Some(namespace),
            runtime: None,
            dns: Vec::new(),
        });
        graph.primary.options.do_classes = vec!["Sandbox".into()];
        graph.primary.options.bindings = vec![("Sandbox".into(), "Sandbox".into())];
        graph.primary.containers = vec![cellar::container::ContainerSpec {
            class_name: "Sandbox".into(),
            image: images.sandbox.clone(),
            instance_type: Some("basic".into()),
            max_instances: Some(1),
            runtime: None,
        }];
        graph.primary.fence_image = Some(images.fence.clone());
    })
    .await?;
    let result = exercise(&address).await;
    // Always shut down this host, including after a failed application assertion.
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let cleanup = assert_no_owned_containers(&docker, &host).await;
    let drained = shutdown.and_then(|outcome| {
        match outcome {
            NodeOutcome::Stopped(report) => {
                eprintln!(
                    "Sandbox shutdown: cells={}, application={}, timed_out={}",
                    report.cells_drained, report.application_work_drained, report.drain_timed_out
                );
                ensure!(
                    report.cells_drained
                        && report.application_work_drained
                        && !report.drain_timed_out,
                    "native host did not drain owned Sandbox work"
                );
            }
            _ => anyhow::bail!("native Sandbox host did not stop"),
        }
        Ok(())
    });
    let failures = [
        ("application", result),
        ("shutdown", drained),
        ("cleanup", cleanup),
    ]
    .into_iter()
    .filter_map(|(stage, result)| result.err().map(|error| format!("{stage}: {error:#}")))
    .collect::<Vec<_>>();
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

async fn request(
    client: &reqwest::Client,
    address: &str,
    route: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    let response = client
        .get(format!("{address}{route}"))
        .query(query)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{route}: {status} {body}");
    serde_json::from_str(&body).with_context(|| format!("{route} did not return JSON: {body}"))
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(125))
        .build()?;
    let result = exercise_routes(&client, address).await;
    // Drain the SDK's own monitor before revoking its native activation, even
    // when an application assertion fails after the container has started.
    let destroyed = async {
        let response = client.get(format!("{address}/__destroy")).send().await?;
        ensure!(
            response.status().is_success() && response.text().await? == "destroyed",
            "explicit Sandbox teardown failed"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    result?;
    destroyed
}

async fn exercise_routes(client: &reqwest::Client, address: &str) -> Result<()> {
    let result = request(
        client,
        address,
        "/exec",
        &[(
            "cmd",
            r#"sh -c "printf 'cellar-sandbox-image'; printf 'diagnostic' >&2; exit 7""#,
        )],
    )
    .await?;
    ensure!(
        result["stdout"] == "cellar-sandbox-image"
            && result["stderr"] == "diagnostic"
            && result["exitCode"] == 7,
        "published SDK/image command result: {result}"
    );
    let written = request(client, address, "/write", &[]).await?;
    let content = written["content"]
        .as_str()
        .context("published file content")?;
    ensure!(
        content.starts_with("written at ") && content.ends_with('\n'),
        "unexpected file: {written}"
    );
    let read = request(
        client,
        address,
        "/exec",
        &[("cmd", r#"node -e "process.stdout.write(JSON.stringify(require('fs').readFileSync('/workspace/note.txt', 'utf8')))""#)],
    )
    .await?;
    let actual: String =
        serde_json::from_str(read["stdout"].as_str().context("encoded file bytes")?)?;
    ensure!(
        actual == content && read["exitCode"] == 0,
        "file did not survive a later event: {read}"
    );
    let process = request(client, address, "/serve", &[]).await?;
    let id = process["id"].as_str().context("published process ID")?;
    let processes = request(client, address, "/processes", &[]).await?;
    ensure!(
        processes
            .as_array()
            .context("published process list")?
            .iter()
            .any(|p| p["id"] == id && matches!(p["status"].as_str(), Some("running" | "starting"))),
        "background process did not survive a later request: {processes}"
    );
    Ok(())
}

async fn assert_no_owned_containers(docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let filters = serde_json::to_string(&json!({"label": [format!("cellar.host={host}")]}))?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("all", "true")
        .append_pair("filters", &filters)
        .finish();
    let reply = docker
        .call("GET", &format!("/containers/json?{query}"), None)
        .await?;
    ensure!(
        reply.status.is_success(),
        "list owned containers: {}",
        reply.message()
    );
    let containers = reply.json()?;
    ensure!(
        containers.as_array().context("container list")?.is_empty(),
        "Sandbox host leaked containers: {containers}"
    );
    Ok(())
}
