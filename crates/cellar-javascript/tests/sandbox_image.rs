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

#[path = "support/image_provider.rs"]
mod image_provider;
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
        .container_provider(Arc::new(image_provider::DiagnosticNative(
            cellar_container::Native,
        )))
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
                .block_on(run(
                    directory.path(),
                    application,
                    &images,
                    Scenario::Published,
                ))
        },
    )
}

#[test]
#[ignore = "requires isolated CELLAR_SANDBOX_ENGINE_SOCKET and preloaded CELLAR_SANDBOX_FENCE_IMAGE"]
fn portable_files_use_daemon_archives_across_events() -> Result<()> {
    let images = Images::configured()?;
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("files.mjs");
    std::fs::write(&entry, include_str!("support/files.mjs"))?;
    let code = include_str!("support/files.mjs").replace("export ", "");
    let source = Source::new(
        format!("var CellarApp = (() => {{{code}; return {{cells,fetch}}; }})();"),
        "files.mjs",
    )
    .events([EventKind::Fetch])
    .cell("Sandbox");
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(entry),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run(
                    directory.path(),
                    application,
                    &images,
                    Scenario::Portable,
                ))
        },
    )
}

#[test]
#[ignore = "requires isolated CELLAR_SANDBOX_ENGINE_SOCKET and preloaded CELLAR_SANDBOX_FENCE_IMAGE"]
fn published_sandbox_rpc_files_use_the_pinned_image() -> Result<()> {
    let images = Images::configured()?;
    let directory = tempfile::tempdir()?;
    let entry = fixture()?.with_file_name("sandbox-rpc.mjs");
    let source = Source::module()
        .container_provider(Arc::new(image_provider::DiagnosticNative(
            cellar_container::Native,
        )))
        .workers_lifetimes()
        .events([EventKind::Fetch])
        .cell_events(
            "Sandbox",
            [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
        );
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(entry),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let result = run(
                        directory.path(),
                        application.clone(),
                        &images,
                        Scenario::Rpc,
                    )
                    .await;
                    let usage = application.bridge_usage();
                    ensure!(
                        usage.active_invocations == 0 && usage.retained_bytes == 0,
                        "SDK RPC bridge resources leaked: {usage:?}"
                    );
                    result
                })
        },
    )
}

#[test]
#[ignore = "requires isolated CELLAR_SANDBOX_ENGINE_SOCKET and preloaded CELLAR_SANDBOX_FENCE_IMAGE"]
fn published_sandbox_preview_authorization_tracks_the_active_runtime() -> Result<()> {
    let images = Images::configured()?;
    let directory = tempfile::tempdir()?;
    let entry = fixture()?.with_file_name("sandbox-preview.mjs");
    let source = Source::module()
        .container_provider(Arc::new(image_provider::DiagnosticNative(
            cellar_container::Native,
        )))
        .workers_lifetimes()
        .events([EventKind::Fetch])
        .cell_events(
            "Sandbox",
            [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
        );
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(entry),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let result = run(
                        directory.path(),
                        application.clone(),
                        &images,
                        Scenario::Preview,
                    )
                    .await;
                    let usage = application.bridge_usage();
                    result.with_context(|| format!("preview bridge={usage:?}"))?;
                    ensure!(
                        usage.active_invocations == 0 && usage.retained_bytes == 0,
                        "preview resources leaked: {usage:?}"
                    );
                    Ok(())
                })
        },
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Published,
    Portable,
    Rpc,
    Preview,
}

async fn run(
    path: &Path,
    application: Arc<dyn Application>,
    images: &Images,
    scenario: Scenario,
) -> Result<()> {
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
            network_helper: None,
        });
        graph.primary.options.do_classes = vec!["Sandbox".into()];
        graph.primary.options.bindings = vec![("Sandbox".into(), "Sandbox".into())];
        graph.primary.containers = vec![cellar::container::ContainerSpec {
            class_name: "Sandbox".into(),
            image: images.sandbox.clone(),
            instance_type: Some("basic".into()),
            max_instances: Some(1),
            runtime: None,
            exposed_ports: if scenario == Scenario::Preview {
                vec![8080]
            } else {
                Vec::new()
            },
        }];
        graph.primary.fence_image = Some(images.fence.clone());
        if scenario == Scenario::Rpc {
            graph
                .primary
                .options
                .vars
                .push(("SANDBOX_TRANSPORT".into(), "rpc".into()));
        }
    })
    .await?;
    let result = if scenario == Scenario::Rpc {
        exercise_rpc(&address, &docker, &host).await
    } else if scenario == Scenario::Preview {
        exercise_preview(&address, &docker, &host).await
    } else if scenario == Scenario::Portable {
        async {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(125))
                .build()?;
            for route in ["write", "read"] {
                let response = client.get(format!("{address}/{route}")).send().await?;
                let status = response.status();
                let body = response.text().await?;
                ensure!(
                    status.is_success() && body == "ok",
                    "portable {route}: {status} {body}"
                );
            }
            Ok(())
        }
        .await
    } else {
        exercise(&address, &docker, &host).await
    };
    // Always shut down this host, including after a failed application assertion.
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let cleanup = assert_no_owned_containers(&docker, &host).await;
    let networks = remove_owned_networks(&docker, &host).await;
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
        ("networks", networks),
    ]
    .into_iter()
    .filter_map(|(stage, result)| result.err().map(|error| format!("{stage}: {error:#}")))
    .collect::<Vec<_>>();
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

async fn exercise_rpc(address: &str, docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(125))
        .build()?;
    let mut upload = Some(tokio::spawn(client.get(format!("{address}/upload")).send()));
    let result = async {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let response = client.get(format!("{address}/progress")).send().await?;
                let status = response.status();
                let bytes = response.bytes().await?;
                ensure!(
                    status.is_success(),
                    "SDK upload progress: {status}: {}",
                    String::from_utf8_lossy(&bytes)
                );
                let value: Value = serde_json::from_slice(&bytes)?;
                if value["size"].as_u64().is_some_and(|n| n > 0) {
                    ensure!(
                        value["published"] == false && value["exit"] == 0,
                        "SDK upload was published before EOF: {value}"
                    );
                    break;
                }
                ensure!(
                    !upload
                        .as_ref()
                        .context("upload task missing")?
                        .is_finished(),
                    "upload ended before incremental file data was visible"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("SDK upload did not publish a prefix before release")??;
        ensure!(
            !upload
                .as_ref()
                .context("upload task missing")?
                .is_finished(),
            "SDK upload buffered the whole source"
        );
        client
            .get(format!("{address}/release"))
            .send()
            .await?
            .error_for_status()?;
        let response = upload
            .take()
            .context("upload task already joined")?
            .await??;
        let status = response.status();
        let bytes = response.bytes().await?;
        ensure!(
            status.is_success(),
            "SDK upload: {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        let value: Value = serde_json::from_slice(&bytes)?;
        let size = 2 * 1024 * 1024 + 17;
        ensure!(
            value["bytes"] == size
                && value["locked"] == true
                && value["result"]["success"] == true
                && value["result"]["bytesWritten"] == size,
            "SDK input transfer: {value}"
        );
        let result: Value = client
            .get(format!("{address}/verify"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            result["exit"] == 0 && result["size"] == size && result["valid"] == true,
            "SDK file bytes: {result}"
        );
        let response = client.get(format!("{address}/download")).send().await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        ensure!(
            status.is_success(),
            "SDK binary download: {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        ensure!(
            bytes.len() == size as usize
                && bytes
                    .iter()
                    .enumerate()
                    .all(|(i, byte)| *byte == (i % 256) as u8),
            "SDK binary download changed file bytes"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        if let Err(error) = diagnose_owned_containers(docker, host).await {
            eprintln!("RPC image diagnostic: {error:#}");
        }
    }
    // Always release and join the caller, including an assertion failure. A
    // cancelled observation is not permission to abandon accepted upload work.
    let released = client.get(format!("{address}/release")).send().await;
    let joined = match upload.take() {
        Some(task) => task
            .await
            .context("join SDK upload")?
            .map(|_| ())
            .map_err(anyhow::Error::from),
        None => Ok(()),
    };
    let destroyed = client.get(format!("{address}/destroy")).send().await;
    result?;
    released?.error_for_status()?;
    joined?;
    let destroyed = destroyed?;
    ensure!(
        destroyed.status().is_success(),
        "SDK RPC destroy failed: {}",
        destroyed.text().await?
    );
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

async fn exercise(address: &str, docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(125))
        .build()?;
    let result = exercise_routes(&client, address).await;
    if result.is_err() {
        // Capture this test host's state before explicit SDK destroy removes
        // the evidence. Diagnostics never replace an application failure.
        if let Err(error) = diagnose_owned_containers(docker, host).await {
            eprintln!("Sandbox failure diagnostics: {error:#}");
        }
    }
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
    let streamed = request(client, address, "/__stream", &[]).await?;
    ensure!(
        streamed["released"] == true,
        "SDK did not deliver stdout before command completion: {streamed}"
    );
    let events = streamed["events"].as_array().context("SDK stream events")?;
    ensure!(
        events.iter().any(|event| event["type"] == "stdout"
            && event["data"]
                .as_str()
                .is_some_and(|data| data.contains("cellar-last")))
            && events.iter().any(|event| event["type"] == "stderr"
                && event["data"]
                    .as_str()
                    .is_some_and(|data| data.contains("cellar-stderr"))),
        "SDK lost final stdout/stderr: {streamed}"
    );
    ensure!(
        events.iter().any(|event| event["exitCode"] == 7),
        "SDK lost command exit: {streamed}"
    );
    exercise_streamed_http(client, address, "http", "release").await?;
    exercise_streamed_http(client, address, "rpc", "rpc_release").await?;
    Ok(())
}

fn stream_events(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| {
            line.strip_prefix("data:")
                .and_then(|data| serde_json::from_str(data.trim()).ok())
        })
        .collect()
}

async fn exercise_streamed_http(
    client: &reqwest::Client,
    address: &str,
    mode: &str,
    release: &str,
) -> Result<()> {
    let mut response = client
        .get(format!("{address}/__stream_{mode}"))
        .send()
        .await?
        .error_for_status()?;
    ensure!(response.headers()["content-type"]
        .to_str()?
        .starts_with("text/event-stream"));
    let mut bytes = Vec::new();
    loop {
        let chunk = response
            .chunk()
            .await?
            .context("SDK HTTP stream ended before first stdout")?;
        ensure!(
            bytes.len() + chunk.len() <= 65536,
            "SDK HTTP stream exceeds fixture bound"
        );
        bytes.extend_from_slice(&chunk);
        if stream_events(&bytes).iter().any(|event| {
            event["type"] == "stdout"
                && event["data"]
                    .as_str()
                    .is_some_and(|data| data.contains("cellar-first"))
        }) {
            break;
        }
    }
    // This independent request permits the still-running command to finish.
    let released = client
        .get(format!("{address}/__stream_{release}"))
        .send()
        .await?;
    ensure!(released.status().as_u16() == 204 && released.bytes().await?.is_empty());
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= 65536,
            "SDK HTTP stream exceeds fixture bound"
        );
        bytes.extend_from_slice(&chunk);
    }
    let events = stream_events(&bytes);
    ensure!(
        events.iter().any(|event| event["type"] == "stdout"
            && event["data"]
                .as_str()
                .is_some_and(|data| data.contains("cellar-last"))),
        "SDK HTTP stream lost final stdout"
    );
    ensure!(
        events.iter().any(|event| event["type"] == "stderr"
            && event["data"]
                .as_str()
                .is_some_and(|data| data.contains("cellar-stderr"))),
        "SDK HTTP stream lost stderr"
    );
    ensure!(
        events
            .iter()
            .any(|event| event["type"] == "complete" && event["exitCode"] == 7),
        "SDK HTTP stream lost exit status"
    );
    Ok(())
}

async fn diagnose_owned_containers(docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let filters = serde_json::to_string(&json!({"label": [format!("cellar.host={host}")]}))?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("all", "true")
        .append_pair("filters", &filters)
        .finish();
    let response = docker
        .call("GET", &format!("/containers/json?{query}"), None)
        .await?;
    ensure!(
        response.status.is_success(),
        "list diagnostic containers: {}",
        response.message()
    );
    for container in response
        .json()?
        .as_array()
        .context("diagnostic container list")?
    {
        let id = container["Id"]
            .as_str()
            .filter(|id| id.len() == 64 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .context("diagnostic container ID")?;
        let response = docker
            .call("GET", &format!("/containers/{id}/json"), None)
            .await?;
        ensure!(
            response.status.is_success(),
            "inspect diagnostic container: {}",
            response.message()
        );
        let inspected = response.json()?;
        ensure!(
            inspected["Config"]["Labels"]["cellar.host"] == host,
            "diagnostic container belongs to another host"
        );
        eprintln!("Sandbox container {id} state: {}", inspected["State"]);
        // Only this isolated fixture's image logs; never dump container env or
        // unlabelled resources. The retained output is bounded independently.
        let logs = docker
            .call(
                "GET",
                &format!("/containers/{id}/logs?stdout=1&stderr=1&tail=40"),
                None,
            )
            .await?;
        ensure!(
            logs.status.is_success(),
            "container diagnostic logs unavailable"
        );
        eprintln!(
            "Sandbox container {id} recent logs: {}",
            logs.message().chars().take(8_000).collect::<String>()
        );
    }
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

// Every run deliberately has a fresh host namespace. Remove only its empty,
// labelled bridges, or repeated qualification exhausts Docker's subnet pool.
async fn remove_owned_networks(docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let filters = serde_json::to_string(&json!({"label": [format!("cellar.host={host}")]}))?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("filters", &filters)
        .finish();
    let reply = docker
        .call("GET", &format!("/networks?{query}"), None)
        .await?;
    ensure!(
        reply.status.is_success(),
        "list owned networks: {}",
        reply.message()
    );
    let networks = reply.json()?;
    let prefix = host.get(..24).context("invalid test host identity")?;
    for network in networks.as_array().context("network list")? {
        let id = network["Id"].as_str().context("network ID")?;
        ensure!(
            id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid network ID"
        );
        let inspected = docker.call("GET", &format!("/networks/{id}"), None).await?;
        ensure!(
            inspected.status.is_success(),
            "inspect owned network: {}",
            inspected.message()
        );
        let inspected = inspected.json()?;
        let name = inspected["Name"].as_str().context("network name")?;
        ensure!(
            inspected["Labels"]["cellar.host"] == host
                && inspected["Driver"] == "bridge"
                && (name == format!("cellar-{prefix}")
                    || name == format!("cellar-{prefix}-internal")),
            "network ownership mismatch"
        );
        ensure!(
            inspected["Containers"]
                .as_object()
                .is_some_and(|entries| entries.is_empty()),
            "owned bridge still has attached containers"
        );
        let removed = docker
            .call("DELETE", &format!("/networks/{id}"), None)
            .await?;
        ensure!(
            removed.status.is_success(),
            "remove owned network: {}",
            removed.message()
        );
    }
    Ok(())
}

async fn exercise_preview(
    address: &str,
    docker: &cellar::docker::Docker,
    host: &str,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(125))
        .build()?;
    let result = async {
        let response = client.get(format!("{address}/setup")).send().await?;
        let status = response.status();
        let body = response.text().await?;
        ensure!(status.is_success(), "preview setup {status}: {body}");
        let exposed: Value = serde_json::from_str(&body)?;
        ensure!(exposed["port"] == 8080, "SDK exposed a different service");
        let original_url = exposed["url"].as_str().context("preview URL missing")?;
        let mut preview = url::Url::parse(original_url)?;
        let hostname = preview
            .host_str()
            .context("preview host missing")?
            .to_string();
        let local: std::net::SocketAddr = address.trim_start_matches("http://").parse()?;
        // Route the generated hostname through the isolated local ingress. No
        // DNS publication, external gateway or hosted account is involved.
        preview
            .set_scheme("http")
            .map_err(|_| anyhow::anyhow!("preview scheme"))?;
        preview
            .set_port(Some(local.port()))
            .map_err(|_| anyhow::anyhow!("preview port"))?;
        preview.set_path("/echo");
        preview.set_query(Some("binary=yes"));
        let mut wrong = preview.clone();
        let wrong_host = "8080-preview-invalidtoken0000.preview.test";
        ensure!(
            hostname != wrong_host,
            "test invalid token matched authorization"
        );
        wrong.set_host(Some(wrong_host))?;
        let routed = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve(&hostname, local)
            .resolve(wrong_host, local)
            .timeout(Duration::from_secs(125))
            .build()?;
        let response = routed
            .post(preview.clone())
            .header("x-sandbox-preview-proxy", "1")
            .header("x-sandbox-preview-port", "3000")
            .header("x-sandbox-preview-token", "forged")
            .header("x-sandbox-preview-sandbox-id", "other")
            .body(vec![0_u8, 255, 42])
            .send()
            .await?;
        if response.status().as_u16() != 207 {
            let status = response.status();
            anyhow::bail!("preview forward {status}: {}", response.text().await?);
        }
        ensure!(
            response.headers()["x-preview-method"] == "POST"
                && response.headers()["x-preview-path"] == "/echo?binary=yes",
            "preview changed method or path"
        );
        ensure!(
            response.headers()["x-preview-host"] == hostname
                && response.headers()["x-preview-internal"] == "clean",
            "preview forwarded forged routing headers"
        );
        ensure!(
            response.bytes().await?.as_ref() == [0_u8, 255, 42],
            "preview changed binary bytes"
        );
        preview_websocket(&routed, preview.clone(), &hostname, false).await?;
        ensure!(
            routed.get(wrong).send().await?.status().as_u16() == 404,
            "invalid preview token reached the service"
        );
        client
            .get(format!("{address}/stop"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            routed.get(preview.clone()).send().await?.status().as_u16() == 410,
            "stopped runtime accepted an old preview"
        );
        assert_no_owned_containers(docker, host).await?;
        let ports: Value = client
            .get(format!("{address}/ports"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            ports == json!([]),
            "stopped runtime advertised active previews"
        );
        client
            .get(format!("{address}/wake"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            routed.get(preview.clone()).send().await?.status().as_u16() == 410,
            "runtime replacement silently reactivated preview authorization"
        );
        let reactivated: Value = client
            .get(format!("{address}/setup"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(
            reactivated["url"] == original_url,
            "explicit re-exposure lost durable preview authorization"
        );
        ensure!(
            routed.get(preview.clone()).send().await?.status().as_u16() == 207,
            "explicit re-exposure did not activate the new runtime"
        );
        preview_websocket(&routed, preview.clone(), &hostname, true).await?;
        client
            .get(format!("{address}/unexpose"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            routed.get(preview).send().await?.status().as_u16() == 404,
            "revoked preview reached the service"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let destroyed = client.get(format!("{address}/destroy")).send().await;
    result?;
    destroyed?.error_for_status()?;
    Ok(())
}

async fn preview_websocket(
    client: &reqwest::Client,
    url: url::Url,
    hostname: &str,
    abrupt: bool,
) -> Result<()> {
    use fastwebsockets::{Frame, OpCode, Role, WebSocket};
    let response = client
        .get(url)
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("x-sandbox-preview-token", "forged")
        .header("x-sandbox-preview-port", "3000")
        .send()
        .await?;
    ensure!(
        response.status().as_u16() == 101,
        "SDK preview upgrade: {}",
        response.text().await?
    );
    ensure!(
        response.headers()["x-preview-host"] == hostname
            && response.headers()["x-preview-internal"] == "clean",
        "WebSocket preview leaked routing headers"
    );
    let mut socket = WebSocket::after_handshake(response.upgrade().await?, Role::Client);
    for (opcode, bytes) in [
        (OpCode::Text, b"preview".to_vec()),
        (OpCode::Binary, vec![0, 255, 42]),
    ] {
        socket
            .write_frame(Frame::new(true, opcode, None, bytes.clone().into()))
            .await?;
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.read_frame()).await??;
        ensure!(
            frame.opcode == opcode && frame.payload.as_ref() == bytes,
            "preview changed WebSocket bytes"
        );
    }
    if !abrupt {
        socket
            .write_frame(Frame::close(1000, b"preview done"))
            .await?;
    }
    Ok(())
}
