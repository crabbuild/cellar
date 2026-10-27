// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[path = "support/image_provider.rs"]
mod image_provider;
#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;

#[test]
#[ignore = "requires isolated engine, CELLAR_SANDBOX_V1_IMAGE and CELLAR_SANDBOX_FENCE_IMAGE"]
fn published_sandbox_v1_files_use_matching_helper_and_owned_native_exec() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/cloudflare-v1");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "Sandbox 1.x build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    let source = Source::module()
        .container_provider(Arc::new(image_provider::DiagnosticNative))
        .workers_lifetimes()
        .events([EventKind::Fetch])
        .cell_events(
            "Workspace",
            [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
        );
    joined_node::run(
        Options {
            max_body_bytes: 64 * 1024,
            max_operations: 2048,
            turn_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/fixture.mjs")),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(exercise(application))
        },
    )
}

async fn exercise(application: Arc<dyn Application>) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let socket = PathBuf::from(
        std::env::var_os("CELLAR_SANDBOX_ENGINE_SOCKET").context("isolated engine socket")?,
    );
    let image =
        std::env::var("CELLAR_SANDBOX_V1_IMAGE").context("prebuilt Sandbox 1.x image ID")?;
    ensure!(
        image.starts_with("sha256:")
            && image.len() == 71
            && image[7..].bytes().all(|b| b.is_ascii_hexdigit()),
        "use an immutable local image ID"
    );
    let fence = std::env::var("CELLAR_SANDBOX_FENCE_IMAGE").context("preloaded fence image")?;
    let docker = cellar::docker::Docker::new(&socket);
    let inspected = docker
        .call("GET", &format!("/images/{image}/json"), None)
        .await?;
    ensure!(inspected.status.is_success(), "preload matching image");
    let info = inspected.json()?;
    ensure!(
        info["Architecture"] == "amd64"
            && info["Os"] == "linux"
            && info["Config"]["Labels"]["cellar.sandbox.sdk"] == "1.0.3"
            && info["Config"]["Labels"]["cellar.sandbox.helper"]
                == "sha256:fc1740c3cba26461f0ab1771287c84018ab2cca43a70ebb823087c85d780aaec",
        "Sandbox 1.x image identity mismatch"
    );
    eprintln!("Sandbox 1.0.3 image: {image}");
    let namespace = format!(
        "sandbox-v1-{}",
        directory.path().file_name().unwrap().to_string_lossy()
    );
    let host = format!(
        "{:x}",
        Sha256::digest(format!("javascript-test\0namespace:{namespace}").as_bytes())
    );
    let (node, address) =
        native::start_configured(directory.path(), application, |config, services, graph| {
            config.runtime.handler_budget = Duration::from_secs(120);
            services.containers = Some(cellar::container::EngineOptions {
                socket: socket.clone(),
                namespace: Some(namespace),
                runtime: None,
                dns: Vec::new(),
            });
            graph.primary.options.do_classes = vec!["Workspace".into()];
            graph.primary.options.bindings = vec![("WORKSPACES".into(), "Workspace".into())];
            graph.primary.containers = vec![cellar::container::ContainerSpec {
                class_name: "Workspace".into(),
                image,
                instance_type: Some("basic".into()),
                max_instances: Some(1),
                runtime: None,
                exposed_ports: Vec::new(),
            }];
            graph.primary.fence_image = Some(fence);
        })
        .await?;
    let result = async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(125))
            .build()?;
        for route in ["write", "read", "faults", "destroy"] {
            let response = client.get(format!("{address}/{route}")).send().await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(status.is_success(), "{route}: {status}: {body}");
            if route == "read" {
                let value: Value = serde_json::from_str(&body)?;
                let bytes = (0..2 * 1024 * 1024 + 17)
                    .map(|i| (i % 251) as u8)
                    .collect::<Vec<_>>();
                ensure!(
                    value["read"] == bytes.len()
                        && value["sha256"] == format!("{:x}", Sha256::digest(bytes)),
                    "binary file round trip: {value}"
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    let cleanup = cleanup(&docker, &host).await;
    ensure!(
        matches!(stopped?,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "native host did not join"
    );
    cleanup?;
    result
}

async fn cleanup(docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let filters = serde_json::to_string(&json!({"label":[format!("cellar.host={host}")]}))?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("all", "true")
        .append_pair("filters", &filters)
        .finish();
    let reply = docker
        .call("GET", &format!("/containers/json?{query}"), None)
        .await?;
    ensure!(
        reply.status.is_success() && reply.json()?.as_array().is_some_and(Vec::is_empty),
        "Sandbox 1.x leaked owned containers"
    );
    let reply = docker
        .call("GET", &format!("/networks?{query}"), None)
        .await?;
    ensure!(reply.status.is_success(), "list owned networks");
    for network in reply.json()?.as_array().context("network list")? {
        let id = network["Id"].as_str().context("network ID")?;
        ensure!(
            id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid network ID"
        );
        let inspected = docker.call("GET", &format!("/networks/{id}"), None).await?;
        let inspected = inspected.json()?;
        ensure!(
            inspected["Labels"]["cellar.host"] == host
                && inspected["Driver"] == "bridge"
                && inspected["Containers"]
                    .as_object()
                    .is_some_and(|v| v.is_empty()),
            "network ownership or attachment mismatch"
        );
        ensure!(
            docker
                .call("DELETE", &format!("/networks/{id}"), None)
                .await?
                .status
                .is_success(),
            "remove owned network"
        );
    }
    Ok(())
}
