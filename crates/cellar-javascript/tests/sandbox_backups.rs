// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real published DirectoryBackup, matching shim and production HTTP routing.
#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)] // Finite external campaign joins its own host.

use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::*};
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[path = "support/image_cleanup.rs"]
mod image_cleanup;
#[path = "support/image_provider.rs"]
mod image_provider;
#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;

#[test]
#[ignore = "requires isolated Engine, matching Sandbox 1.0.3 and native network helper image IDs"]
fn published_directory_backups_use_owned_http_interception_and_native_storage() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/cloudflare-v1");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "Sandbox backup build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    joined_node::run(
        Options {
            max_body_bytes: 64 * 1024,
            max_operations: 2048,
            turn_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/backups.mjs")),
        Source::module()
            .container_provider(Arc::new(image_provider::DiagnosticNative(
                cellar_container::Native.with_http_interception(),
            )))
            .workers_lifetimes()
            .workers_http_streams()
            .events([EventKind::Fetch, EventKind::WorkerRpc])
            .cell_events("Workspace", [EventKind::Fetch]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(exercise(application))
        },
    )
}

fn image(name: &str) -> Result<String> {
    let image = std::env::var(name).with_context(|| format!("missing {name}"))?;
    ensure!(
        image.starts_with("sha256:")
            && image.len() == 71
            && image[7..].bytes().all(|byte| byte.is_ascii_hexdigit()),
        "use an immutable image ID for {name}"
    );
    Ok(image)
}

async fn exercise(application: Arc<dyn Application>) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let socket = PathBuf::from(
        std::env::var_os("CELLAR_SANDBOX_ENGINE_SOCKET").context("isolated Engine socket")?,
    );
    let sandbox = image("CELLAR_SANDBOX_V1_IMAGE")?;
    let fence = image("CELLAR_SANDBOX_FENCE_IMAGE")?;
    let helper = image("CELLAR_SANDBOX_NETWORK_IMAGE")?;
    let docker = cellar::docker::Docker::new(&socket);
    let inspected = docker
        .expect(
            "GET",
            &format!("/images/{sandbox}/json"),
            None,
            "inspect matching Sandbox image",
        )
        .await?
        .json()?;
    ensure!(
        inspected["Architecture"] == "amd64"
            && inspected["Os"] == "linux"
            && inspected["Config"]["Labels"]["cellar.sandbox.sdk"] == "1.0.3"
            && inspected["Config"]["Labels"]["cellar.sandbox.helper"]
                == "sha256:fc1740c3cba26461f0ab1771287c84018ab2cca43a70ebb823087c85d780aaec",
        "Sandbox helper identity mismatch"
    );
    let namespace = format!(
        "sandbox-backups-{}",
        directory.path().file_name().unwrap().to_string_lossy()
    );
    let host = format!(
        "{:x}",
        Sha256::digest(format!("javascript-test\0namespace:{namespace}").as_bytes())
    );
    let bucket = Bucket::open_dev(&directory.path().join("objects.db"))?;
    let replica_storage = ReplicaStorage::new(
        bucket.store.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let (node, address) =
        native::start_configured(directory.path(), application, |config, services, graph| {
            // Image/fsync qualification has a dedicated budget. Small runtime tests
            // retain their original deadlines and operation limits.
            config.runtime.handler_budget = Duration::from_secs(120);
            services.containers = Some(cellar::container::EngineOptions {
                socket: socket.clone(),
                namespace: Some(namespace),
                runtime: Some("runc".into()),
                dns: Vec::new(),
                network_helper: Some(cellar::container::NetworkHelperOptions::new(
                    helper,
                    "/opt/cellar-egress/bin/cellar-egress".into(),
                    "127.0.0.11:53".parse().unwrap(),
                )),
            });
            graph.primary.options.do_classes = vec!["Workspace".into()];
            graph.primary.options.bindings = vec![("WORKSPACES".into(), "Workspace".into())];
            graph.primary.options.blob_bindings =
                vec![("BACKUPS".into(), "sandbox-backups".into())];
            graph.primary.containers = vec![cellar::container::ContainerSpec {
                class_name: "Workspace".into(),
                image: sandbox,
                instance_type: Some("basic".into()),
                max_instances: Some(1),
                runtime: None,
                exposed_ports: Vec::new(),
            }];
            graph.primary.fence_image = Some(fence);
            services.fleet = Some(NodeFleet {
                lease_bucket: bucket.clone(),
                bucket,
                replica_storage,
                replication: ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
                log_transport: Arc::new(SignedPeerTransport::from_clients(
                    services.peer_http.clone(),
                    services.peer_http.clone(),
                    services.peer_auth.clone(),
                )),
                log: LogOptions {
                    record_publish_timeout: Duration::from_secs(3),
                    ..Default::default()
                },
                fleet_durability: false,
            });
        })
        .await?;
    eprintln!("CELLAR_SANDBOX_BACKUP_OWNER={host}");
    let result = async {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(125))
            .build()?;
        for route in [
            "install", "routing", "backup", "restore", "destroy", "delete",
        ] {
            eprintln!("Sandbox 1.0.3 HTTP/backup route: {route}");
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
                    value["replies"]
                        .as_array()
                        .is_some_and(|replies| replies.len() == 4),
                    "retained connection probe"
                );
            } else if route == "backup" {
                ensure!(
                    value["format"] == "tar+zstd/1"
                        && value["size"].as_u64().is_some_and(|size| size > 0)
                        && value["sha256"]
                            .as_str()
                            .is_some_and(|hash| hash.len() == 64),
                    "published archive record"
                );
            } else if route == "restore" {
                let bytes = (0..2 * 1024 * 1024 + 17)
                    .map(|i| (i % 251) as u8)
                    .collect::<Vec<_>>();
                ensure!(
                    value["bytes"] == bytes.len()
                        && value["sha256"] == format!("{:x}", Sha256::digest(bytes)),
                    "restored binary payload"
                );
            } else {
                ensure!(
                    value.as_object().is_some_and(
                        |value| value.len() == 1 && value.values().all(|value| value == true)
                    ),
                    "lifecycle acknowledgement"
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    let cleanup = image_cleanup::cleanup(&docker, &host).await;
    let outcome = stopped?;
    ensure!(
        matches!(&outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "native host did not join: {outcome:?}"
    );
    cleanup?;
    result
}
