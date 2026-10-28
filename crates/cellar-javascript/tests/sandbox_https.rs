// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[path = "support/counting_provider.rs"]
mod counting_provider;

#[path = "support/image_cleanup.rs"]
mod image_cleanup;
#[path = "support/image_provider.rs"]
mod image_provider;
#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;

#[test]
#[ignore = "requires isolated engine, CELLAR_SANDBOX_V1_IMAGE and CELLAR_SANDBOX_FENCE_IMAGE"]
fn published_sandbox_image_uses_owned_https_interception_and_rotates_ca() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/cloudflare-v1");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "Sandbox 1.x build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    let counts = Arc::new(counting_provider::Counts::default());
    let source = Source::module()
        .container_provider(Arc::new(counting_provider::CountingProvider {
            inner: Arc::new(image_provider::DiagnosticNative(
                cellar_container::Native.with_https_interception(),
            )),
            counts: counts.clone(),
        }))
        .workers_lifetimes()
        .workers_http_streams()
        .events([EventKind::Fetch, EventKind::WorkerRpc])
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
        NodeOptions::new(root.join("dist/https.mjs")),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(exercise(application, counts))
        },
    )
}

async fn exercise(
    application: Arc<dyn Application>,
    counts: Arc<counting_provider::Counts>,
) -> Result<()> {
    use std::sync::atomic::Ordering;
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
    let helper = std::env::var("CELLAR_SANDBOX_NETWORK_IMAGE").context("preloaded helper image")?;
    let tls = cellar::container::HttpsInterceptionOptions::public_web_roots()?;
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
        "sandbox-https-{}",
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
                runtime: Some("runc".into()),
                dns: Vec::new(),
                network_helper: Some(
                    cellar::container::NetworkHelperOptions::new(
                        helper,
                        "/opt/cellar-egress/bin/cellar-egress".into(),
                        "127.0.0.11:53".parse().unwrap(),
                    )
                    .with_https_interception(tls),
                ),
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
    eprintln!("CELLAR_SANDBOX_HTTPS_OWNER={host}");
    let result = async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(125))
            .build()?;
        let mut previous_ca = None;
        for _ in 0..2 {
            for route in ["install", "routing", "destroy"] {
                eprintln!("Sandbox HTTPS route: {route}");
                let response = client.get(format!("{address}/{route}")).send().await?;
                let status = response.status();
                let body = response.text().await?;
                ensure!(
                    status.is_success(),
                    "{route}: {status}: {}",
                    body.chars().take(4096).collect::<String>()
                );
                let value: Value = serde_json::from_str(&body)?;
                if route == "routing" {
                    ensure!(
                        value["replies"] == 3
                            && value["mismatch"] == true
                            && value["untrusted"] == true
                            && value["http"] == true,
                        "TLS qualification result"
                    );
                    let ca = value["ca"].as_str().context("public CA digest")?.to_owned();
                    ensure!(
                        ca.len() == 64 && previous_ca.as_ref() != Some(&ca),
                        "a replacement run reused its CA"
                    );
                    previous_ca = Some(ca);
                }
                ensure!(
                    counts.active.load(Ordering::SeqCst) == 0
                        && counts.unreleased.load(Ordering::SeqCst) == 0,
                    "{route}: unreleased exec resources"
                );
            }
        }
        ensure!(
            counts.created.load(Ordering::SeqCst) == counts.released.load(Ordering::SeqCst)
                && counts.created.load(Ordering::SeqCst) >= 4,
            "joined process handles"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = &result {
        let evidence = format!("{error:#}");
        eprintln!(
            "Sandbox 1.0.3 route failure: {}",
            evidence.chars().take(4096).collect::<String>()
        );
    }
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    let cleanup = image_cleanup::cleanup(&docker, &host).await;
    let outcome = stopped?;
    ensure!(
        matches!(&outcome,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "native host did not join: {outcome:?}; qualification: {result:?}"
    );
    cleanup?;
    result
}
