// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual signed inspection, owner forwarding and post-handoff observations.
use super::{call, client, inputs, ready, response, stopped, Executor};
use anyhow::{ensure, Result};
use cellar::engine_api::RpcData;
use cellar::filesystem_state::FilesystemState;
use cellar::sdk::{filesystem::*, *};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Notify;

const CHUNK_BYTES: usize = 64 * 1024; // Internal format-1 content geometry.

fn graph() -> DeploymentGraph {
    let mut graph = super::graph();
    graph.primary.options.do_classes.push("Plain".into());
    graph
}

#[derive(Default)]
struct Signals {
    factories: AtomicUsize,
    application_rpc: AtomicUsize,
    read_only_probes: AtomicUsize,
    started: Notify,
    release: Notify,
    finished: Notify,
}

struct Workspace(Arc<Signals>);

impl Cell for Workspace {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let request = match invocation {
                Invocation::Fetch(request) => request,
                Invocation::CellRpc { .. } => {
                    self.0.application_rpc.fetch_add(1, Ordering::SeqCst);
                    return Ok(Output::Rpc(RpcData::Json("{\"unexpected\":true}".into())));
                }
                _ => return Ok(Output::Done),
            };
            let fs = context.fs()?;
            if request.url.ends_with("/prepare") {
                fs.write_file("/private-name", &vec![7; 3 * CHUNK_BYTES])?;
                let file = fs.open("/private-name", OpenOptions::read_write())?;
                file.ftruncate(CHUNK_BYTES as u64)?;
                file.ftruncate((3 * CHUNK_BYTES) as u64)?;
                file.ftruncate(CHUNK_BYTES as u64)?;
                file.pwrite(&vec![9; CHUNK_BYTES], (2 * CHUNK_BYTES) as u64)?;
                let orphan = fs.open(
                    "/private-orphan",
                    OpenOptions::read_write().create_new(true),
                )?;
                orphan.write_all(&vec![5; CHUNK_BYTES])?;
                fs.unlink("/private-orphan")?;
                let duplicate = orphan.dup()?;
                let signals = self.0.clone();
                context.wait_until(async move {
                    signals.started.notify_one();
                    signals.release.notified().await;
                    duplicate.close()?;
                    orphan.close()?;
                    file.close()?;
                    fs.sync().await?;
                    signals.finished.notify_one();
                    Ok(())
                })?;
                Ok(response("prepared"))
            } else {
                let data = fs.read_file("/private-name")?;
                ensure!(
                    data.len() == 3 * CHUNK_BYTES
                        && data[..CHUNK_BYTES].iter().all(|b| *b == 7)
                        && data[CHUNK_BYTES..2 * CHUNK_BYTES].iter().all(|b| *b == 0)
                        && data[2 * CHUNK_BYTES..].iter().all(|b| *b == 9),
                    "recovered filesystem bytes differ"
                );
                if fs.capabilities()?.read_only {
                    let error = fs.write_file("/private-name", b"forbidden").unwrap_err();
                    ensure!(error.errno == Errno::EROFS && error.outcome == Outcome::NotApplied);
                    let storage = context.storage()?;
                    storage.tx(|tx| {
                        ensure!(tx.fs()?.read_file("/private-name")? == data);
                        tx.put_json("read_only_recovery_probe", &json!(true))
                    })?;
                    storage.sync().await?;
                    ensure!(storage.get_json("read_only_recovery_probe")? == Some(json!(true)));
                    self.0.read_only_probes.fetch_add(1, Ordering::SeqCst);
                }
                Ok(response("recovered"))
            }
        })
    }
}

fn applications(signals: Arc<Signals>) -> Result<CompiledApplications> {
    applications_with_policy(signals, Policy::default())
}

fn applications_with_policy(signals: Arc<Signals>, policy: Policy) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let factory = signals.clone();
        let plain = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let cell = context.namespace("ACCOUNTS")?.get_by_name("workspace");
                    if request.url.ends_with("/forge") {
                        ensure!(
                            cell.rpc(&context, "__filesystemState", RpcData::Json("{}".into()))
                                .await
                                .is_err(),
                            "application RPC minted filesystem observation authority"
                        );
                        return Ok(response("forged request refused"));
                    }
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                factory.factories.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Workspace(factory.clone())))
            })?
            .cell("Plain", move |_, _| {
                plain.factories.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Workspace(plain.clone())))
            })?
            .filesystem("Account", policy)?,
        ))
    })
}

async fn observe(client: &reqwest::Client, node: &Node, scope: &str) -> Result<FilesystemState> {
    try_observe(client, node, scope)
        .await?
        .ok_or_else(|| anyhow::anyhow!("filesystem observation unavailable"))
}

async fn try_observe(
    client: &reqwest::Client,
    node: &Node,
    scope: &str,
) -> Result<Option<FilesystemState>> {
    let path = format!("/filesystem-state/{scope}");
    let response = PeerAuth::new([11; 32], "filesystem-observer")?
        .sign(
            client
                .post(format!("http://{}{path}", node.info().internal_address))
                .body("{}"),
            "POST",
            &path,
            b"{}",
            &node.info().node,
        )?
        .send()
        .await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
        return Ok(None);
    }
    ensure!(
        status == 200,
        "filesystem observation {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    let value: Value = serde_json::from_slice(&bytes)?;
    let sample: FilesystemState = serde_json::from_value(value["result"].clone())?;
    sample.validate(scope)?;
    ensure!(
        !value.to_string().contains("private-name")
            && !value.to_string().contains("private-orphan"),
        "filesystem observation exposed a name"
    );
    Ok(Some(sample))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_inspection_is_authenticated_owner_routed_and_read_only() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            bucket
                .put(
                    "fleet/peer-auth.json",
                    serde_json::to_vec(&json!({"version":1,"key":"0b".repeat(32)}))?,
                )
                .await?;
            Ok::<_, anyhow::Error>(bucket)
        })
        .await??;
    let core = Executor::new("filesystem-inspection-core")?;
    let signals = Arc::new(Signals::default());
    let (mut config, services) = inputs(
        "filesystem-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.cell_limits.filesystem.gc_batch_chunks = 1;
    let owner = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&owner).await?;
    let http = client()?;
    let path = "/filesystem-state/Account:unused";
    ensure!(
        http.post(format!("http://{}{path}", owner.info().internal_address))
            .body("{}")
            .send()
            .await?
            .status()
            == 401
    );
    let unused = observe(&http, &owner, "Account:unused").await?;
    ensure!(unused.enabled && unused.usage.is_none() && unused.live_handles == 0);
    let disabled = observe(&http, &owner, "Plain:unused").await?;
    ensure!(!disabled.enabled && disabled.usage.is_none());
    ensure!(
        signals.factories.load(Ordering::SeqCst) == 0,
        "observation constructed application cells"
    );
    ensure!(call(&http, &owner, reqwest::Method::POST, "/prepare").await? == "prepared");
    signals.started.notified().await;
    let scope = format!(
        "Account:{}",
        cellar_native::cell_id::encode_hex(&cellar_native::cell_id::for_name(
            "cells:v1:13:embedded-node:Account",
            "workspace"
        ))
    );
    let (mut config, services) = inputs(
        "filesystem-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.cell_limits.filesystem.gc_batch_chunks = 1;
    let entrance = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&entrance).await?;
    // The signed request enters a different node from the current owner.
    let held = observe(&http, &entrance, &scope).await?;
    ensure!(held.enabled && held.live_handles == 3 && held.pinned_inodes == 2);
    let usage = held.usage.as_ref().unwrap();
    ensure!(
        usage.inodes == 3
            && usage.logical_bytes == (4 * CHUNK_BYTES) as u64
            && usage.unlinked_inodes == 1
            && usage.unlinked_allocated_bytes == CHUNK_BYTES as u64
            && usage.discarded_allocated_bytes <= CHUNK_BYTES as u64
            && usage.allocated_bytes == (3 * CHUNK_BYTES) as u64 + usage.discarded_allocated_bytes
    );
    ensure!(
        call(&http, &entrance, reqwest::Method::GET, "/forge").await? == "forged request refused"
    );
    ensure!(signals.application_rpc.load(Ordering::SeqCst) == 0);
    let fleet = cellar::management::FleetClient::new(bucket, false);
    let observed = management.run(fleet.filesystem(&scope)).await??;
    ensure!(observed.epoch == held.epoch && observed.live_handles == 3);
    signals.release.notify_one();
    signals.finished.notified().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let sample = observe(&http, &entrance, &scope).await?;
            let usage = sample.usage.unwrap();
            if sample.live_handles == 0
                && sample.pinned_inodes == 0
                && usage.unlinked_inodes == 0
                && usage.discarded_ranges == 0
            {
                ensure!(usage.inodes == 2 && usage.allocated_bytes == (2 * CHUNK_BYTES) as u64);
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    ensure!(
        signals.factories.load(Ordering::SeqCst) == 1,
        "inspection invoked an application constructor"
    );
    stopped(owner).await?;
    let recovered = management.run(fleet.filesystem(&scope)).await??;
    ensure!(
        recovered.epoch > held.epoch
            && recovered.live_handles == 0
            && recovered.usage.as_ref().unwrap().logical_bytes == (3 * CHUNK_BYTES) as u64
    );
    ensure!(call(&http, &entrance, reqwest::Method::GET, "/probe").await? == "recovered");
    stopped(entrance).await?;
    management.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_pending_reclamation_recovers_without_application_access() -> Result<()> {
    pending_reclamation_recovers(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_readonly_recovery_reclaims_without_application_access() -> Result<()> {
    pending_reclamation_recovers(true).await
}

async fn pending_reclamation_recovers(read_only: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            bucket
                .put(
                    "fleet/peer-auth.json",
                    serde_json::to_vec(&json!({"version":1,"key":"0b".repeat(32)}))?,
                )
                .await?;
            Ok::<_, anyhow::Error>(bucket)
        })
        .await??;
    let core = Executor::new("filesystem-pending-gc-core")?;
    let signals = Arc::new(Signals::default());
    let (mut config, services) = inputs(
        "gc-predecessor",
        &directory.path().join("old"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.shutdown = ShutdownPolicy {
        total_ms: 1_000,
        token_wait_ms: 0,
        no_progress_ms: 200,
    };
    config.cell_limits.filesystem.gc_batch_chunks = 1;
    let owner = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&owner).await?;
    let http = client()?;
    ensure!(call(&http, &owner, reqwest::Method::POST, "/prepare").await? == "prepared");
    tokio::time::timeout(Duration::from_secs(5), signals.started.notified()).await?;
    let scope = format!(
        "Account:{}",
        cellar_native::cell_id::encode_hex(&cellar_native::cell_id::for_name(
            "cells:v1:13:embedded-node:Account",
            "workspace"
        ))
    );
    let held = observe(&http, &owner, &scope).await?;
    ensure!(held.live_handles == 3 && held.usage.as_ref().unwrap().unlinked_inodes == 1);
    let old_services = owner.services();
    // Leave the accepted background descriptors open. The bounded stop must
    // report forced retirement, join that work and revoke its handle pins.
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        owner.shutdown(ShutdownMode::Handoff),
    )
    .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    ensure!(
        report.drain_timed_out
            && !report.application_work_drained
            && report.pending_application_events > 0,
        "pending descriptors were concealed by shutdown: {report:?}"
    );
    ensure!(
        old_services.application_work().pending_events == 0,
        "forced retirement left events running"
    );
    std::fs::remove_dir_all(directory.path().join("old"))?;

    let (mut config, services) = inputs(
        "gc-successor",
        &directory.path().join("fresh"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    config.cell_limits.filesystem.gc_batch_chunks = 1;
    // Usage can exceed a newly lowered quota. Maintenance must still be able
    // to reclaim retained bytes rather than permanently refusing the cell.
    config.cell_limits.filesystem.max_allocated_bytes = (2 * CHUNK_BYTES) as u64;
    let successor = Node::start(
        config,
        services,
        graph(),
        applications_with_policy(
            signals.clone(),
            Policy {
                read_only,
                ..Policy::default()
            },
        )?,
    )
    .await?;
    ready(&successor).await?;
    let recovered = tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            // Node readiness precedes per-cell recovery/admission. A bounded
            // read-only retry accepts only the documented unavailable status;
            // malformed successful responses and authentication errors fail.
            if let Some(sample) = try_observe(&http, &successor, &scope).await? {
                let usage = sample.usage.as_ref().unwrap();
                if usage.unlinked_inodes == 0 && usage.discarded_ranges == 0 {
                    break Ok::<_, anyhow::Error>(sample);
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    let usage = recovered.usage.unwrap();
    ensure!(
        recovered.epoch > held.epoch && recovered.live_handles == 0 && recovered.pinned_inodes == 0
    );
    ensure!(
        usage.inodes == 2
            && usage.logical_bytes == (3 * CHUNK_BYTES) as u64
            && usage.allocated_bytes == (2 * CHUNK_BYTES) as u64
    );
    ensure!(
        signals.factories.load(Ordering::SeqCst) == 1,
        "reclamation constructed application code"
    );
    ensure!(call(&http, &successor, reqwest::Method::GET, "/probe").await? == "recovered");
    ensure!(signals.read_only_probes.load(Ordering::SeqCst) == usize::from(read_only));
    stopped(successor).await?;
    management.shutdown().await;
    Ok(())
}
