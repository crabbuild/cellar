// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated child routing and exact Engine deletion through a running host.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::{containers::*, *};
use cellar_container::{ChildExecutor, Native};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "support/native_engine.rs"]
mod peer;

struct App {
    executor: Arc<ChildExecutor>,
    factories: Arc<AtomicUsize>,
}

struct Root;

impl Application for App {
    fn sandbox_executor(&self, class: &str) -> Option<Arc<dyn SandboxExecutor>> {
        (class == "Sandbox").then(|| self.executor.clone() as Arc<dyn SandboxExecutor>)
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Parent"
    }

    fn create_cell(&self, _: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        if identity.class == "Sandbox" {
            self.factories.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("sandbox must not enter application factory");
        }
        Ok(Arc::new(Root))
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            Ok(Output::Http(
                context
                    .namespace("PARENTS")?
                    .get_by_name("alice")
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("cleanup must not call the application alarm");
            };
            let catalog = SandboxCatalog::open(&context)?;
            let path = request.url.rsplit('/').next().unwrap();
            let value = match path {
                "start" | "duplicate" => {
                    let first = catalog
                        .start_child(&context, "build", "SANDBOXES", json!({"options":{}}))
                        .await?;
                    let second = catalog
                        .start_child(&context, "preview", "SANDBOXES", json!({"options":{}}))
                        .await?;
                    ensure!(first.execution_scope != second.execution_scope);
                    let capabilities = catalog
                        .execute_child(&context, &first, "container.capabilities", json!({}))
                        .await?;
                    ensure!(
                        capabilities["max_body_bytes"] == 65536
                            && capabilities["max_resources"] == 0
                    );
                    ensure!(catalog
                        .execute_child(
                            &context,
                            &first,
                            "container.exec",
                            json!({"options":{"cmd":["sh"]}})
                        )
                        .await
                        .is_err());
                    let output = catalog
                        .execute_child(
                            &context,
                            &first,
                            "container.exec_output",
                            json!({"options":{"cmd":["echo","ok"]},"max_bytes":1024}),
                        )
                        .await?;
                    ensure!(output["exit_code"] == 7);
                    json!(true)
                }
                "uncertain" => {
                    let error = catalog
                        .start_child(&context, "uncertain", "SANDBOXES", json!({"options":{}}))
                        .await
                        .unwrap_err();
                    ensure!(
                        error
                            .downcast_ref::<SandboxFailure>()
                            .is_some_and(|error| error.kind == SandboxFailureKind::Unknown),
                        "uncertain create: {error:#}"
                    );
                    json!(true)
                }
                "delete-unproven" => {
                    let first = catalog.get(&context, "build")?.unwrap();
                    ensure!(catalog.destroy_child(&context, &first).await.is_err());
                    ensure!(
                        catalog.get(&context, "build")?.unwrap().state
                            == SandboxAllocationState::Deleting
                    );
                    json!(true)
                }
                "capacity" => {
                    let error = catalog
                        .start_child(&context, "third", "SANDBOXES", json!({"options":{}}))
                        .await
                        .unwrap_err();
                    ensure!(
                        error
                            .downcast_ref::<SandboxFailure>()
                            .is_some_and(|error| error.kind == SandboxFailureKind::Capacity),
                        "capacity reply: {error:#}"
                    );
                    json!(true)
                }
                "replace" => {
                    let first = catalog.get(&context, "build")?.unwrap();
                    catalog.destroy_child(&context, &first).await?;
                    catalog
                        .start_child(&context, "third", "SANDBOXES", json!({"options":{}}))
                        .await?;
                    ensure!(catalog
                        .execute_child(&context, &first, "container.running", json!({}))
                        .await
                        .is_err());
                    json!(true)
                }
                "unauthorized" => {
                    let child = catalog.get(&context, "preview")?.unwrap();
                    let target = context
                        .namespace("SANDBOXES")?
                        .get_by_name(&child.execution_name);
                    ensure!(target.fetch(&context, request).await.is_err());
                    json!(true)
                }
                "retire" => {
                    catalog.begin_retirement(&context).await?;
                    json!(true)
                }
                "status" => serde_json::to_value(catalog.parent(&context)?)?,
                "allocations" => {
                    serde_json::to_value(catalog.list(&context, None, 256)?.allocations)?
                }
                "erase" => {
                    let parent = catalog.parent(&context)?.unwrap();
                    let mut erasure = catalog.request_erasure(&context);
                    let mut resume_after = None;
                    // Model a busy worker delaying the request's continuation
                    // while cleanup observes its newly committed intent.
                    futures_util::future::poll_fn(|cx| {
                        if let Some(delay) = &mut resume_after {
                            if std::future::Future::poll(std::pin::Pin::as_mut(delay), cx)
                                .is_pending()
                            {
                                return std::task::Poll::Pending;
                            }
                        }
                        let result = erasure.as_mut().poll(cx);
                        if result.is_pending() && resume_after.is_none() {
                            let mut delay =
                                Box::pin(tokio::time::sleep(Duration::from_millis(300)));
                            let _ = std::future::Future::poll(delay.as_mut(), cx);
                            resume_after = Some(delay);
                        }
                        result
                    })
                    .await?;
                    ensure!(resume_after.is_some(), "erasure proof did not suspend");
                    json!(parent.scope)
                }
                "retired" => {
                    ensure!(catalog
                        .parent(&context)?
                        .is_some_and(|parent| parent.state == SandboxParentState::Deleted));
                    ensure!(catalog
                        .list(&context, None, 256)?
                        .allocations
                        .iter()
                        .all(|allocation| allocation.state == SandboxAllocationState::Deleted));
                    ensure!(catalog
                        .start_child(&context, "build", "SANDBOXES", json!({"options":{}}))
                        .await
                        .is_err());
                    json!(true)
                }
                _ => anyhow::bail!("unknown child test route"),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

async fn request(client: &reqwest::Client, address: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{address}/{path}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{path}: {status} {body}");
    Ok(serde_json::from_str(&body)?)
}

#[test]
fn authenticated_children_enforce_capacity_and_retire_without_application_callbacks() -> Result<()>
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let peer = peer::Peer::start().await?;
        let factories = Arc::new(AtomicUsize::new(0));
        let app = Arc::new(App {
            executor: Arc::new(ChildExecutor::new(Arc::new(Native), "c".repeat(64))?),
            factories: factories.clone(),
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()?;
        for recovered in [false, true] {
            let (node, address) = native::start_with_configuration(
                directory.path(),
                app.clone(),
                vec!["Parent".into(), "Sandbox".into()],
                vec![
                    ("PARENTS".into(), "Parent".into()),
                    ("SANDBOXES".into(), "Sandbox".into()),
                ],
                |config, services, graph| {
                    peer.configure(config, services, graph);
                    config.actor.core.operation_deadline_ms = Some(15_000);
                    graph.primary.containers[0].class_name = "Sandbox".into();
                    services.containers.as_mut().unwrap().capacity = Some(
                        cellar::container::CapacityLimits::new(2, 16 * 1024 * 1024 * 1024).unwrap(),
                    );
                },
            )
            .await?;
            let result = async {
                if !recovered {
                    for path in ["start", "duplicate", "capacity", "unauthorized"] {
                        ensure!(request(&client, &address, path).await? == true);
                    }
                    peer.engine.defer_remove.store(true, Ordering::SeqCst);
                    ensure!(request(&client, &address, "delete-unproven").await? == true);
                    ensure!(
                        !peer.engine.containers.lock().unwrap().is_empty(),
                        "fixture failed to retain unproven deletion"
                    );
                    peer.engine.defer_remove.store(false, Ordering::SeqCst);
                    ensure!(request(&client, &address, "replace").await? == true);
                    ensure!(
                        peer.engine.app_creates() == 3,
                        "duplicate or refused create reached Engine"
                    );
                    request(&client, &address, "retire").await?;
                    tokio::time::timeout(Duration::from_secs(45), async {
                        loop {
                            if request(&client, &address, "status").await?["state"] == "deleted" {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
                    ensure!(
                        peer.engine.containers.lock().unwrap().is_empty(),
                        "retirement left physical Engine containers"
                    );
                }
                ensure!(request(&client, &address, "retired").await? == true);
                ensure!(factories.load(Ordering::SeqCst) == 0);
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(shutdown, NodeOutcome::Stopped(report)
                if report.cells_drained
                    && report.application_work_drained
                    && !report.drain_timed_out));
            result?;
        }
        peer.stop().await?;
        Ok(())
    })
}

#[test]
fn uncertain_create_is_not_replayed_and_parent_cleanup_survives_restart() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let peer = peer::Peer::start().await?;
        let app = Arc::new(App {
            executor: Arc::new(ChildExecutor::new(Arc::new(Native), "c".repeat(64))?),
            factories: Arc::new(AtomicUsize::new(0)),
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()?;
        for recovered in [false, true] {
            let (node, address) = native::start_with_configuration(
                directory.path(),
                app.clone(),
                vec!["Parent".into(), "Sandbox".into()],
                vec![
                    ("PARENTS".into(), "Parent".into()),
                    ("SANDBOXES".into(), "Sandbox".into()),
                ],
                |config, services, graph| {
                    peer.configure(config, services, graph);
                    config.actor.core.operation_deadline_ms = Some(15_000);
                },
            )
            .await?;
            let result = async {
                if !recovered {
                    peer.engine.lose_next_create.store(true, Ordering::SeqCst);
                    ensure!(request(&client, &address, "uncertain").await? == true);
                    ensure!(request(&client, &address, "uncertain").await? == true);
                    ensure!(
                        peer.engine.app_creates() == 1,
                        "unknown create was replayed"
                    );
                    peer.engine.refuse_remove.store(true, Ordering::SeqCst);
                    request(&client, &address, "retire").await?;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                } else {
                    tokio::time::timeout(Duration::from_secs(45), async {
                        loop {
                            if request(&client, &address, "status").await?["state"] == "deleted" {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
                    ensure!(peer.engine.containers.lock().unwrap().is_empty());
                    ensure!(request(&client, &address, "retired").await? == true);
                    ensure!(peer.engine.app_creates() == 1);
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            // Host shutdown can complete physical cleanup; the parent must
            // still recover its unacknowledged durable obligation next run.
            peer.engine.refuse_remove.store(false, Ordering::SeqCst);
            let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(shutdown, NodeOutcome::Stopped(report)
                if report.cells_drained
                    && report.application_work_drained
                    && !report.drain_timed_out));
            result?;
        }
        peer.stop().await?;
        Ok(())
    })
}

#[test]
fn named_children_spill_to_a_peer_with_container_capacity_and_cleanup_when_full() -> Result<()> {
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let left_peer = peer::Peer::start().await?;
        let right_peer = peer::Peer::start().await?;
        let app = Arc::new(App {
            executor: Arc::new(ChildExecutor::new(Arc::new(Native), "c".repeat(64))?),
            factories: Arc::new(AtomicUsize::new(0)),
        });
        let mut nodes = Vec::new();
        for (name, engine) in [("child-left", &left_peer), ("child-right", &right_peer)] {
            nodes.push(
                native::start_named(
                    &directory.path().join(name),
                    app.clone(),
                    name,
                    |config, services, graph| {
                        engine.configure(config, services, graph);
                        config.actor.lease_ttl_ms = 5000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(5);
                        config.actor.core.operation_deadline_ms = Some(15_000);
                        config.maintenance.fleet_sample_period = Duration::from_millis(50);
                        config.maintenance.wake_period = Duration::from_millis(100);
                        config.startup.bootstrap_timeout = Duration::from_secs(20);
                        services.core_executor = core.handle().clone();
                        services.containers.as_mut().unwrap().capacity = Some(
                            cellar::container::CapacityLimits::new(1, 16 * 1024 * 1024 * 1024)
                                .unwrap(),
                        );
                        services.fleet = Some(NodeFleet {
                            replica_storage: ReplicaStorage::new(
                                bucket.store.clone(),
                                ReplicaStorageOptions {
                                    prefix: bucket.prefix.clone(),
                                    ..Default::default()
                                },
                            )
                            .unwrap(),
                            lease_bucket: bucket.clone(),
                            bucket: bucket.clone(),
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
                            fleet_durability: true,
                        });
                        graph.primary.options.do_classes = vec!["Parent".into(), "Sandbox".into()];
                        graph.primary.options.bindings = vec![
                            ("PARENTS".into(), "Parent".into()),
                            ("SANDBOXES".into(), "Sandbox".into()),
                        ];
                    },
                )
                .await?,
            );
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()?;
        let address = &nodes[0].1;
        let result = async {
            // Wait for peer membership/capacity publication, not a guessed node
            // choice. The test below still requires one real create per Engine.
            tokio::time::sleep(Duration::from_millis(250)).await;
            ensure!(request(&client, address, "start").await? == true);
            ensure!(
                left_peer.engine.app_creates() == 1 && right_peer.engine.app_creates() == 1,
                "container capacity did not place across both hosts"
            );
            ensure!(request(&client, address, "duplicate").await? == true);
            ensure!(request(&client, address, "capacity").await? == true);
            ensure!(request(&client, address, "unauthorized").await? == true);
            request(&client, address, "retire").await?;
            tokio::time::timeout(Duration::from_secs(45), async {
                loop {
                    if request(&client, address, "status").await?["state"] == "deleted" {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            ensure!(left_peer.engine.containers.lock().unwrap().is_empty());
            ensure!(right_peer.engine.containers.lock().unwrap().is_empty());
            let children = request(&client, address, "allocations").await?;
            for child in children.as_array().unwrap() {
                let scope = child["execution_scope"].as_str().unwrap();
                wait_erased(&nodes[0].0, scope).await?;
                let prefix = format!("cells/{scope}/");
                let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
                let keys = observer.run(bucket.list(&prefix)).await;
                observer.shutdown().await;
                let keys = keys??;
                ensure!(
                    keys.iter()
                        .all(|key| key.location.as_ref() == format!("{prefix}own.json")),
                    "child replica objects remained: {keys:?}"
                );
            }
            ensure!(app.factories.load(Ordering::SeqCst) == 0);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        for (node, _) in nodes {
            let shutdown = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(shutdown, NodeOutcome::Stopped(report)
                if report.cells_drained
                    && report.application_work_drained
                    && !report.drain_timed_out));
        }
        left_peer.stop().await?;
        right_peer.stop().await?;
        result
    })
}

#[test]
fn sandbox_sqlite_erasure_is_terminal_across_restart() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let directory = tempfile::tempdir()?;
            let peer = peer::Peer::start().await?;
            let app = Arc::new(App {
                executor: Arc::new(ChildExecutor::new(Arc::new(Native), "c".repeat(64))?),
                factories: Arc::new(AtomicUsize::new(0)),
            });
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()?;
            let mut erased_parent = None;
            for restarted in [false, true] {
                let (node, address) = native::start_with_configuration(
                    directory.path(),
                    app.clone(),
                    vec!["Parent".into(), "Sandbox".into()],
                    vec![
                        ("PARENTS".into(), "Parent".into()),
                        ("SANDBOXES".into(), "Sandbox".into()),
                    ],
                    |config, services, graph| peer.configure(config, services, graph),
                )
                .await?;
                let result = async {
                    if !restarted {
                        request(&client, &address, "start").await?;
                        ensure!(
                            !client
                                .get(format!("{address}/erase"))
                                .send()
                                .await?
                                .status()
                                .is_success(),
                            "live parent erasure was accepted"
                        );
                        let children = request(&client, &address, "allocations").await?;
                        request(&client, &address, "retire").await?;
                        tokio::time::timeout(Duration::from_secs(45), async {
                            loop {
                                if request(&client, &address, "status").await?["state"] == "deleted"
                                {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                            Ok::<_, anyhow::Error>(())
                        })
                        .await??;
                        for child in children.as_array().unwrap() {
                            let scope = child["execution_scope"].as_str().unwrap();
                            wait_erased(&node, scope).await?;
                            ensure!(
                                !directory.path().join(scope).exists(),
                                "child SQLite directory remained"
                            );
                        }
                        let scope = request(&client, &address, "erase")
                            .await?
                            .as_str()
                            .unwrap()
                            .to_owned();
                        wait_erased(&node, &scope).await?;
                        ensure!(
                            !directory.path().join(&scope).exists(),
                            "parent SQLite directory remained"
                        );
                        erased_parent = Some(scope);
                    }
                    ensure!(
                        !client
                            .get(format!("{address}/status"))
                            .send()
                            .await?
                            .status()
                            .is_success(),
                        "erased parent was reopened"
                    );
                    ensure!(
                        node.sandbox_erasure_status(erased_parent.as_ref().unwrap())
                            .await?
                            == Some("erased")
                    );
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                let _ = node.shutdown(ShutdownMode::Preserve).await?;
                result?;
            }
            peer.stop().await
        })
}

async fn wait_erased(node: &Node, scope: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if node.sandbox_erasure_status(scope).await? == Some("erased") {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?
}

#[test]
fn removed_deployment_retires_children_without_guest_callbacks() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let directory = tempfile::tempdir()?;
            let peer = peer::Peer::start().await?;
            let app = Arc::new(App {
                executor: Arc::new(ChildExecutor::new(Arc::new(Native), "c".repeat(64))?),
                factories: Arc::new(AtomicUsize::new(0)),
            });
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()?;
            let (node, address) = native::start_with_configuration(
                directory.path(),
                app.clone(),
                vec!["Parent".into(), "Sandbox".into()],
                vec![
                    ("PARENTS".into(), "Parent".into()),
                    ("SANDBOXES".into(), "Sandbox".into()),
                ],
                |config, services, graph| peer.configure(config, services, graph),
            )
            .await?;
            request(&client, &address, "start").await?;
            let children = request(&client, &address, "allocations").await?;
            let _ = node.shutdown(ShutdownMode::Preserve).await?;
            let (node, _) = native::start_with_configuration(
                directory.path(),
                app,
                Vec::new(),
                Vec::new(),
                |config, services, graph| {
                    peer.configure(config, services, graph);
                    graph.primary.containers.clear();
                },
            )
            .await?;
            let result = async {
                // Only the persisted discovery alarm drives this removed parent.
                for child in children.as_array().unwrap() {
                    let scope = child["execution_scope"].as_str().unwrap();
                    wait_erased(&node, scope).await?;
                    ensure!(!directory.path().join(scope).exists());
                }
                ensure!(peer.engine.app_creates() == 2, "cleanup replayed creation");
                ensure!(peer.engine.containers.lock().unwrap().is_empty());
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let _ = node.shutdown(ShutdownMode::Preserve).await?;
            peer.stop().await?;
            result
        })
}
