// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fleet recovery qualification against a persistent cloud-protocol peer, not a VM.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{containers::*, *};
use cellar_container::ChildExecutor;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "support/e2b.rs"]
mod peer;
#[path = "../../cellar/tests/support/store_outage.rs"]
mod store_outage;

const RECOVERY_LIMIT: Duration = Duration::from_secs(300);
const REQUEST_CONCURRENCY: usize = 8;

struct App(Arc<ChildExecutor>);

struct Parent;

fn payload(index: usize) -> String {
    STANDARD.encode(
        (0..4096)
            .map(|offset| (index.wrapping_add(offset) % 251) as u8)
            .collect::<Vec<_>>(),
    )
}

impl Application for App {
    fn sandbox_executor(&self, class: &str) -> Option<Arc<dyn SandboxExecutor>> {
        (class == "Sandbox").then(|| self.0.clone() as Arc<dyn SandboxExecutor>)
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Parent"
    }

    fn create_cell(&self, _: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        ensure!(
            identity.class == "Parent",
            "dedicated executor reached business factory"
        );
        Ok(Arc::new(Parent))
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("unexpected business callback");
            };
            let url = url::Url::parse(&request.url)?;
            let parent = url
                .path_segments()
                .context("path missing")?
                .next()
                .context("parent missing")?;
            let target = context.namespace("PARENTS")?.get_by_name(parent);
            Ok(Output::Http(target.fetch(&context, request).await?))
        })
    }
}

impl Cell for Parent {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("cleanup invoked business code");
            };
            let url = url::Url::parse(&request.url)?;
            let parts = url
                .path_segments()
                .context("path missing")?
                .collect::<Vec<_>>();
            let catalog = SandboxCatalog::open(&context)?;
            let value = match parts[1] {
                "start" => {
                    let index: usize = parts[2].parse()?;
                    context
                        .storage()?
                        .put_json("business", &json!("preserved"))?;
                    let child = catalog
                        .start_child(
                            &context,
                            &format!("build-{index:05}"),
                            "SANDBOXES",
                            json!({"options":{"env":[["HELLO","world"]]}}),
                        )
                        .await?;
                    catalog
                        .execute_child(
                            &context,
                            &child,
                            "container.file.write",
                            json!({"path":"/tmp/fleet-proof.bin","data":payload(index),"mode":384}),
                        )
                        .await?;
                    serde_json::to_value(child)?
                }
                "read" => {
                    let index: usize = parts[2].parse()?;
                    let child = catalog
                        .get(&context, &format!("build-{index:05}"))?
                        .context("allocation disappeared")?;
                    ensure!(catalog.inspect_child(&context, &child).await?["state"] == "ready");
                    let bytes = catalog
                        .execute_child(
                            &context,
                            &child,
                            "container.file.read",
                            json!({"path":"/tmp/fleet-proof.bin","max_bytes":4096}),
                        )
                        .await?;
                    ensure!(
                        bytes == payload(index),
                        "provider file changed across reconnect"
                    );
                    serde_json::to_value(child)?
                }
                "retire" => serde_json::to_value(catalog.begin_retirement(&context).await?)?,
                "status" => {
                    ensure!(context.storage()?.get_json("business")? == Some(json!("preserved")));
                    serde_json::to_value(catalog.parent(&context)?)?
                }
                _ => anyhow::bail!("unknown fixture route"),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

async fn start(
    path: &Path,
    name: &str,
    app: Arc<dyn Application>,
    bucket: cellar::bucket::Bucket,
    core: tokio::runtime::Handle,
) -> Result<(Node, String)> {
    native::start_named(path, app, name, |config, services, graph| {
        config.actor.lease_ttl_ms = 5000;
        config.actor.core.max_resident = 64;
        config.actor.core.max_cached_routes = 2048;
        config.actor.core.max_activations = 16;
        config.actor.core.max_evictions = 16;
        config.actor.core.operation_deadline_ms = Some(15_000);
        config.startup.recovery_retry_backoff = Duration::from_secs(5);
        config.startup.bootstrap_timeout = Duration::from_secs(45);
        config.maintenance.fleet_sample_period = Duration::from_millis(100);
        config.maintenance.wake_period = Duration::from_millis(100);
        config.runtime.handler_budget = Duration::from_secs(10);
        services.core_executor = core;
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
            bucket,
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
    })
    .await
}

async fn call(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}/{route}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{route}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

fn summary(samples: &[u64]) -> Value {
    let mut values = samples.to_vec();
    values.sort_unstable();
    json!({
        "count": values.len(),
        "p50_ms": values[(values.len() * 50).div_ceil(100).saturating_sub(1)],
        "p95_ms": values[(values.len() * 95).div_ceil(100).saturating_sub(1)],
        "max_ms": values.last(),
        "within_60_seconds": values.iter().filter(|ms| **ms <= 60_000).count(),
        "within_300_seconds": values.iter().filter(|ms| **ms <= 300_000).count(),
    })
}

async fn stop(node: Node) -> Result<()> {
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(ref report) if report.cells_drained
        && report.ingress_drained && report.application_work_drained
        && report.pending_application_events == 0 && report.pending_dispatches == 0
        && report.log_quiesced && report.local_durability_closed
        && !report.drain_timed_out && !report.deadline_elapsed),
        "incomplete shutdown: {outcome:?}"
    );
    Ok(())
}

async fn campaign(
    count: usize,
    outage_duration: Duration,
    core: tokio::runtime::Handle,
) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (isolated, fault) = store_outage::Outage::wrap(&bucket)?;
    let peer = peer::Peer::start().await?;
    let app: Arc<dyn Application> = Arc::new(App(Arc::new(ChildExecutor::new(
        peer.provider_with_lifetime("container", 3600)?,
        "c".repeat(64),
    )?)));
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let original_path = directory.path().join("original");
    let (original, address) = start(
        &original_path,
        "sandbox-original",
        app.clone(),
        isolated,
        core.clone(),
    )
    .await?;
    let mut allocations = Vec::new();
    let setup = async {
        for index in 0..count {
            allocations
                .push(call(&client, &address, &format!("{}/start/{index}", index % 2)).await?);
        }
        ensure!(peer.state.creates.load(Ordering::SeqCst) == count);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = setup {
        let _ = stop(original).await;
        peer.stop().await?;
        return Err(error);
    }
    let failure = Instant::now();
    peer.state.unavailable.store(true, Ordering::SeqCst);
    fault.isolate();
    let fenced = tokio::time::timeout(
        Duration::from_secs(20),
        original.run_until(std::future::pending()),
    )
    .await??;
    ensure!(matches!(
        fenced,
        NodeOutcome::Fenced {
            reason: FenceReason::NodeLeaseExpired,
            ..
        }
    ));
    ensure!(
        fault.refused() > 0 && peer.state.remaining() == count,
        "fault lost provider resources"
    );
    let mut nodes = Vec::new();
    for index in 0..2 {
        nodes.push(
            start(
                &directory.path().join(format!("successor-{index}")),
                &format!("sandbox-successor-{index}"),
                app.clone(),
                bucket.clone(),
                core.clone(),
            )
            .await?,
        );
    }
    let result = async {
        // Prove provider outage is exercised before restoring it. This is an
        // observation; neither a failed read nor retry can replay creation.
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                ensure!(call(&client, &nodes[0].1, "0/read/0").await.is_err());
                if peer.state.refused.load(Ordering::SeqCst) > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("the provider outage was not exercised")??;
        tokio::time::sleep(outage_duration.saturating_sub(failure.elapsed())).await;
        peer.state.unavailable.store(false, Ordering::SeqCst);
        let repair_ms = failure.elapsed().as_millis() as u64;
        let recovered: Vec<_> = tokio::time::timeout(
            RECOVERY_LIMIT.saturating_sub(failure.elapsed()),
            stream::iter(allocations.iter().enumerate())
                .map(|(index, expected)| {
                    let address = &nodes[index % 2].1;
                    let client = &client;
                    async move {
                        loop {
                            if let Ok(observed) =
                                call(client, address, &format!("{}/read/{index}", index % 2)).await
                            {
                                ensure!(
                                    &observed == expected,
                                    "reconnect changed allocation {index}"
                                );
                                return Ok::<_, anyhow::Error>((
                                    index,
                                    failure.elapsed().as_millis() as u64,
                                ));
                            }
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                })
                .buffer_unordered(REQUEST_CONCURRENCY)
                .try_collect::<Vec<_>>(),
        )
        .await
        .context("a sandbox exceeded the 300-second recovery target")??;
        ensure!(
            peer.state.creates.load(Ordering::SeqCst) == count
                && peer.state.deletes.load(Ordering::SeqCst) == 0,
            "reconnect recreated or retired a live sandbox"
        );

        let cleanup = Instant::now();
        peer.state.defer_delete.store(true, Ordering::SeqCst);
        for (index, (_, address)) in nodes.iter().enumerate() {
            call(&client, address, &format!("{index}/retire")).await?;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        ensure!(
            peer.state.remaining() == count,
            "unproven deletion finished cleanup"
        );
        peer.state.defer_delete.store(false, Ordering::SeqCst);
        // One accepted deletion loses its reply. The durable same-identity
        // retry must observe absence, without creating a replacement resource.
        peer.state.lost_delete.store(true, Ordering::SeqCst);
        let erased: Vec<_> = tokio::time::timeout(
            RECOVERY_LIMIT.saturating_sub(cleanup.elapsed()),
            stream::iter(allocations.iter().enumerate())
                .map(|(index, expected)| {
                    let node = &nodes[index % 2].0;
                    async move {
                        let scope = expected["execution_scope"]
                            .as_str()
                            .context("child scope missing")?;
                        loop {
                            if node.sandbox_erasure_status(scope).await? == Some("erased") {
                                return Ok::<_, anyhow::Error>((
                                    index,
                                    cleanup.elapsed().as_millis() as u64,
                                ));
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                })
                .buffer_unordered(REQUEST_CONCURRENCY)
                .try_collect::<Vec<_>>(),
        )
        .await
        .context("a sandbox exceeded the 300-second cleanup target")??;
        ensure!(peer.state.remaining() == 0 && peer.state.creates.load(Ordering::SeqCst) == count);
        for (index, (_, address)) in nodes.iter().enumerate() {
            ensure!(
                call(&client, address, &format!("{index}/status")).await?["state"] == "deleted"
            );
        }
        let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
        let inspected = observer
            .run(async {
                for child in &allocations {
                    let prefix = format!("cells/{}/", child["execution_scope"].as_str().unwrap());
                    ensure!(
                        bucket
                            .list(&prefix)
                            .await?
                            .iter()
                            .all(|object| object.location.as_ref() == format!("{prefix}own.json")),
                        "completed erasure retained replica data"
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await;
        observer.shutdown().await;
        inspected??;

        // The original disk was offline throughout recovery and erasure.
        // Rejoining must collect its cache without reopening retired children.
        let cache = Instant::now();
        let (returning, _) = start(
            &original_path,
            "sandbox-original",
            app.clone(),
            bucket.clone(),
            core.clone(),
        )
        .await?;
        let caches = tokio::time::timeout(RECOVERY_LIMIT, async {
            loop {
                if allocations.iter().all(|child| {
                    let scope = child["execution_scope"].as_str().unwrap();
                    [
                        original_path.clone(),
                        directory.path().join("successor-0"),
                        directory.path().join("successor-1"),
                    ]
                    .iter()
                    .all(|path| !path.join(scope).exists())
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        let returning_stopped = stop(returning).await;
        caches.context("fleet cache collection after rejoin exceeded 300 seconds")?;
        returning_stopped?;
        ensure!(
            fault.is_isolated(),
            "the failed client's store was repaired to assist recovery"
        );
        let recovered = recovered
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let erased = erased
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        Ok(json!({
            "format": 1,
            "profile": "e2b-protocol-peer",
            "sandboxes": count,
            "parents": 2,
            "successor_nodes": 2,
            "lease_ttl_ms": 5000,
            "max_resident_per_node": 64,
            "file_bytes_per_sandbox": 4096,
            "provider_lifetime_seconds": 3600,
            "request_concurrency": REQUEST_CONCURRENCY,
            "erasure_concurrency_per_node": 8,
            "erasure_scan_ms": 250,
            "erasure_attempt_timeout_ms": 20000,
            "audit_pass_pause_ms": 30000,
            "empty_pending_pause_ms": 1000,
            "provider_repair_ms": repair_ms,
            "recovery_target_ms": 300000,
            "recovery": summary(&recovered.values().copied().collect::<Vec<_>>()),
            "cleanup": summary(&erased.values().copied().collect::<Vec<_>>()),
            "returning_cache_ms": cache.elapsed().as_millis(),
            "creates": peer.state.creates.load(Ordering::SeqCst),
            "deletes": peer.state.deletes.load(Ordering::SeqCst),
            "samples": (0..count).map(|index| json!({
                "sandbox": index,
                "recovery_ms": recovered[&index],
                "cleanup_ms": erased[&index],
            })).collect::<Vec<_>>(),
        }))
    }
    .await;
    peer.state.unavailable.store(false, Ordering::SeqCst);
    peer.state.defer_delete.store(false, Ordering::SeqCst);
    let mut shutdown = Ok(());
    for (node, _) in nodes {
        if let Err(error) = stop(node).await {
            shutdown = Err(error);
        }
    }
    peer.stop().await?;
    shutdown?;
    result
}

fn exercise(count: usize, outage: u64) -> Result<()> {
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()?;
    let report = host.block_on(campaign(
        count,
        Duration::from_secs(outage),
        core.handle().clone(),
    ))?;
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text)
        .bytes(format!("SANDBOX_FLEET_REPORT={}\n", serde_json::to_string(&report)?).as_bytes())?;
    Ok(())
}

#[test]
fn sandbox_fleet_reconnects_and_erases_every_child_after_owner_loss() -> Result<()> {
    exercise(24, 5)
}

#[test]
#[ignore = "fleet-scale qualification: run explicitly and retain the per-sandbox report"]
fn sandbox_fleet_scale_qualification() -> Result<()> {
    let count = std::env::var("CELLAR_SANDBOX_QUALIFICATION_COUNT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(512);
    ensure!(
        (128..=4096).contains(&count) && count % 2 == 0,
        "qualification count must be even and 128..=4096"
    );
    exercise(count, 30)
}
