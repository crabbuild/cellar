// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real published DirectoryBackup, matching shim and production HTTP routing.
#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)] // Finite external campaign joins its own host.

use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::{Bucket, MultipartMaintenancePolicy},
    sdk::*,
};
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[path = "support/backup_campaign.rs"]
mod backup_campaign;
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
#[ignore = "requires isolated Engine, matching Sandbox 1.0.3 and native network helper image IDs"]
fn published_directory_backups_use_owned_http_interception_and_native_storage() -> Result<()> {
    if std::env::var_os(backup_campaign::ROOT).is_none() {
        return backup_campaign::run(
            "published_directory_backups_use_owned_http_interception_and_native_storage",
        );
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/cloudflare-v1");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "Sandbox backup build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    let counts = Arc::new(counting_provider::Counts::default());
    let mut node = NodeOptions::new(root.join("dist/backups.mjs"));
    node.executable =
        PathBuf::from(std::env::var_os(backup_campaign::LAUNCHER).context("owned Node launcher")?);
    joined_node::run(
        Options {
            max_body_bytes: 64 * 1024,
            max_operations: 2048,
            turn_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        node,
        Source::module()
            .container_provider(Arc::new(counting_provider::CountingProvider {
                inner: Arc::new(image_provider::DiagnosticNative(
                    cellar_container::Native.with_http_interception(),
                )),
                counts: counts.clone(),
            }))
            .workers_lifetimes()
            .workers_http_streams()
            .events([EventKind::Fetch, EventKind::WorkerRpc])
            .cell_events("Workspace", [EventKind::Fetch]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(exercise(application, counts))
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

async fn exercise(
    application: Arc<dyn Application>,
    counts: Arc<counting_provider::Counts>,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    let directory =
        PathBuf::from(std::env::var_os(backup_campaign::ROOT).context("campaign directory")?);
    let phase = std::env::var(backup_campaign::PHASE)?;
    ensure!(
        matches!(phase.as_str(), "create" | "crash" | "recover"),
        "unknown campaign phase"
    );
    let node_directory = directory.join(format!("node-{phase}"));
    std::fs::create_dir(&node_directory)?;
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
        directory.file_name().unwrap().to_string_lossy()
    );
    let host = backup_campaign::owner(&directory);
    let bucket = Bucket::open_dev_with_multipart_policy(
        &directory.join("objects.db"),
        MultipartMaintenancePolicy {
            upload_ttl: if phase == "recover" {
                None
            } else {
                Some(Duration::from_secs(3600))
            },
            idle_interval: Duration::from_millis(10),
            backlog_interval: Duration::from_millis(1),
        },
    )?;
    if phase == "recover" {
        // The third process opens production storage first. The controller
        // never opens/checkpoints SQLite between SIGKILL and this cold client.
        let connection = rusqlite::Connection::open(directory.join("objects.db"))?;
        ensure!(
            backup_campaign::upload_counts(&connection)? == (2, 1),
            "crash recovery lost or replayed unpublished uploads before host maintenance"
        );
    }
    let replica_storage = ReplicaStorage::new(
        bucket.store.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let (node, address) =
        native::start_configured(&node_directory, application, |config, services, graph| {
            // Image/fsync qualification has a dedicated budget. Small runtime tests
            // retain their original deadlines and operation limits.
            config.runtime.handler_budget = Duration::from_secs(120);
            let offset = if phase == "recover" {
                2 * 60 * 60 * 1000
            } else {
                0
            };
            services.wall_clock = Some(Arc::new(move || {
                i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                )
                .unwrap()
                    + offset
            }));
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
    let result =
        async {
            let client = reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(125))
                .build()?;
            let state = client
                .get(format!("{address}/state"))
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            ensure!(
                state["running"] == false,
                "fresh process reused a live container"
            );
            if phase == "create" {
                ensure!(
                    state["backup"].is_null(),
                    "create phase reused an existing backup"
                );
            } else {
                let expected: Value =
                    serde_json::from_slice(&std::fs::read(directory.join("record.json"))?)?;
                ensure!(
                    state["backup"] == expected,
                    "fresh native process did not recover the saved archive record"
                );
            }
            if phase == "recover" {
                let resources: Vec<String> = serde_json::from_slice(&std::fs::read(directory.join("crash-resources.json"))?)?;
                ensure!(resources.len() == 2, "missing actual crash resource identities");
                for id in resources {
                    ensure!(docker.call("GET", &format!("/containers/{id}/json"), None).await?.status.as_u16() == 404, "replacement host did not reap the exact crashed resource");
                }
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let connection = rusqlite::Connection::open(directory.join("objects.db"))?;
                        if backup_campaign::upload_counts(&connection)? == (0, 0) { return Ok::<_, anyhow::Error>(()); }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }).await??;
            }
            let routes: &[&str] = if phase == "create" {
                &[
                    "install",
                    "routing",
                    "backup",
                    "pre-abort",
                    "cancel-after-create",
                    "cancel-transfer",
                    "lost-acks",
                    "faults",
                ]
            } else if phase == "crash" {
                &["install", "restore", "lost-create"]
            } else {
                &[
                    "install",
                    "restore",
                    "pre-abort",
                    "cancel-after-create",
                    "cancel-transfer",
                    "lost-acks",
                    "faults",
                    "restore",
                    "destroy",
                    "delete",
                ]
            };
            for &route in routes {
                eprintln!("Sandbox 1.0.3 HTTP/backup route: {route}");
                let executions = counts.exec_calls.load(Ordering::SeqCst);
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
                            && value["size"].as_u64().is_some_and(|size| size > 64 * 1024)
                            && value["sha256"]
                                .as_str()
                                .is_some_and(|hash| hash.len() == 64),
                        "published archive record"
                    );
                } else if route == "restore" {
                    let mut state = 0x6d2b79f5_u32;
                    let bytes = (0..2 * 1024 * 1024 + 17)
                        .map(|_| {
                            state ^= state << 13;
                            state ^= state >> 17;
                            state ^= state << 5;
                            state as u8
                        })
                        .collect::<Vec<_>>();
                    ensure!(
                        value["bytes"] == bytes.len()
                            && value["sha256"] == format!("{:x}", Sha256::digest(bytes)),
                        "restored binary payload"
                    );
                } else {
                    let fields = match route {
                        "pre-abort" => 2,
                        "cancel-after-create" => 3,
                        "cancel-transfer" => 4,
                        "lost-acks" => 5,
                        "lost-create" => 4,
                        "faults" => 5,
                        _ => 1,
                    };
                    ensure!(
                        value.as_object().is_some_and(|value| value.len() == fields
                            && value.values().all(|value| value == true)),
                        "lifecycle acknowledgement"
                    );
                }
                if route == "pre-abort" {
                    ensure!(
                        counts.exec_calls.load(Ordering::SeqCst) == executions,
                        "pre-aborted SDK operation allocated a process"
                    );
                }
                ensure!(
                    counts.active.load(Ordering::SeqCst) == 0
                        && counts.unreleased.load(Ordering::SeqCst) == 0,
                    "{route}: SDK process handles survived explicit retirement"
                );
                if route == "backup" {
                    let state = client
                        .get(format!("{address}/state"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<Value>()
                        .await?;
                    std::fs::write(
                        directory.join("record.json"),
                        serde_json::to_vec(&state["backup"])?,
                    )?;
                }
            }
            if phase == "crash" {
                let request = client.get(format!("{address}/crash-part")).send();
                tokio::pin!(request);
                tokio::time::timeout(Duration::from_secs(20), async {
                    loop {
                        cellar::asyncrt::select! {
                            response = &mut request => anyhow::bail!("crash barrier returned before SIGKILL: {response:?}"),
                            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                        }
                        let connection = rusqlite::Connection::open(directory.join("objects.db"))?;
                        if backup_campaign::upload_counts(&connection)? == (2, 1) {
                            let size: u64 = connection.query_row("SELECT length(body) FROM __sys_local_parts", [], |row| row.get(0))?;
                            ensure!(size > 64 * 1024, "crash did not follow a real larger-than-frame part");
                            ensure!(counts.active.load(Ordering::SeqCst) > 0
                                && counts.created.load(Ordering::SeqCst) > counts.released.load(Ordering::SeqCst)
                                && counts.unreleased.load(Ordering::SeqCst) == 0,
                                "crash barrier requires a live SDK process, not an implicitly dropped handle");
                            eprintln!("Sandbox hard crash ready: two uploads, one persisted part, pending receipt and live SDK process");
                            std::fs::write(directory.join("crash-ready"), b"committed-part, pending-receipt, live-sdk-process")?;
                            break;
                        }
                    }
                    // Keep the HTTP exchange and execution alive. Only the
                    // outside parent may cross this actual hard-crash barrier.
                    match request.await { Ok(response) => anyhow::bail!("crash barrier unexpectedly responded: {}", response.status()), Err(error) => anyhow::bail!("crash exchange ended without external kill: {error}") }
                }).await??;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    let cleanup = image_cleanup::cleanup(&docker, &host).await;
    let outcome = stopped?;
    ensure!(
        matches!(&outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "native host did not join: {outcome:?}; campaign result: {result:?}"
    );
    cleanup?;
    result
}
