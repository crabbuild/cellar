// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native Blob calls cross real cell turns and output gates, without engine initialization.

use anyhow::Result;
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use futures_util::TryStreamExt;
use serde_json::json;
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const SCRIPT: &str = "native-blob";
const SCOPE: &str = "Objects:one";

#[path = "support/blob_contract.rs"]
mod blob_contract;
use blob_contract::Signals;

#[tokio::test]
async fn explicit_blob_provider_refuses_rebinding_and_closed_hosts() -> Result<()> {
    let root = tempfile::tempdir()?;
    let provider = cellar::bucket::Bucket::open_dev(&root.path().join("provider.sqlite"))?;
    let domain = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let services = domain.services();
    services.configure_blob_store(provider.clone()).await?;
    assert!(services
        .configure_blob_store(provider.clone())
        .await
        .is_err());
    domain.shutdown().await;
    assert!(services.configure_blob_store(provider).await.is_err());
    Ok(())
}

async fn worker(context: Context, invocation: Invocation, signals: Arc<Signals>) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        anyhow::bail!("fetch expected");
    };
    let path = url::Url::parse(&request.url)?.path().to_owned();
    blob_contract::run_path(context, &path, signals, true).await
}

struct Objects(Storage, Arc<Signals>);

impl Cell for Objects {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let calls = self
                .0
                .get_json("calls")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
                + 1;
            self.0.put_json("calls", &json!(calls))?;
            worker(context, invocation, self.1.clone()).await
        })
    }
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec!["Objects".into()],
            bindings: Vec::new(),
            blob_bindings: vec![
                ("DATA".into(), "native-objects".into()),
                ("OTHER".into(), "other-objects".into()),
            ],
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: SCRIPT.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}

async fn open(runtime: &RuntimeManager) -> Result<()> {
    runtime
        .restore_cell(
            SCOPE,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(SCOPE.into(), 1, true).await?;
    runtime.publish_cell(SCOPE, 1)
}

async fn call(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            SCOPE.into(),
            None,
            RuntimeFetch {
                url: format!("http://objects/{path}"),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_blob_preserves_records_streams_multipart_and_durability_gates() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

#[derive(Default)]
struct GateSignals {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    held: std::sync::atomic::AtomicBool,
}

async fn run() -> Result<()> {
    let gate_signals = Arc::new(GateSignals::default());
    let signals = Arc::new(Signals::default());
    let worker_signals = signals.clone();
    let cell_signals = signals.clone();
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, worker_signals.clone()))
    })
    .cell("Objects", move |context, _| {
        Ok(Arc::new(Objects(context.storage()?, cell_signals.clone())))
    })?;
    let generation = Generation::build_native(
        1,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            SCRIPT.into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().join("cells"),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    open(&runtime).await?;
    call(&runtime, "no-bucket").await?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    cellar::host_channels::set_blob_store(bucket.clone());
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let captured = signals.clone();
    let captured_gate = gate_signals.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"native Blob proof adapter stops";_=stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            captured.gates.lock().unwrap().push(gate.ticket);
            if !captured_gate.held.swap(true, Ordering::SeqCst) {
                captured_gate.entered.notify_one();
                captured_gate.release.notified().await;
            }
            let result = if captured.refuse.load(Ordering::SeqCst) {
                Err(cellar_logic::RequestError::DurabilityUnproven)
            } else {
                Ok(())
            };
            let _ = gate.reply.send(result);
        }
    });
    let caller = runtime.clone();
    let basic = cellar::asyncrt::spawn(async move { call(&caller, "basic").await });
    gate_signals.entered.notified().await;
    assert!(
        bucket
            .head_blob("blob/native-objects/record")
            .await?
            .is_none(),
        "no Blob write may precede its cell proof"
    );
    gate_signals.release.notify_one();
    let reply = basic.await??;
    assert!(reply.write_position.is_some());
    assert_eq!(reply.body, b"checked");
    call(&runtime, "preflight").await?;
    signals.refuse.store(true, Ordering::SeqCst);
    call(&runtime, "rejected").await?;
    assert!(bucket
        .head_blob("blob/native-objects/rejected")
        .await?
        .is_none());
    signals.refuse.store(false, Ordering::SeqCst);
    call(&runtime, "list").await?;
    call(&runtime, "multipart").await?;
    call(&runtime, "stage-owned").await?;
    call(&runtime, "stage-foreign").await?;
    call(&runtime, "resumable-create").await?;
    runtime.swap_out_cell(SCOPE, 1).await?;
    runtime.start_cell(SCOPE.into(), 1, false).await?;
    runtime.publish_cell(SCOPE, 1)?;
    // Reconstruct the configured backend as well as the cell activation.
    cellar::host_channels::set_blob_store(cellar::bucket::Bucket::open_dev(
        &directory.path().join("bucket.sqlite"),
    )?);
    call(&runtime, "resumable-finish").await?;
    call(&runtime, "resume").await?;
    call(&runtime, "finish-resume").await?;
    let reply = call(&runtime, "stream").await?;
    assert_eq!(reply.status, 200);
    assert!(reply.body.is_empty());
    let size = reply
        .stream
        .unwrap()
        .try_fold(0usize, |size, chunk| async move {
            assert!(chunk.iter().all(|byte| *byte == 6));
            Ok(size + chunk.len())
        })
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(size, (8 << 20) + 1);
    let caller = runtime.clone();
    let suspended = cellar::asyncrt::spawn(async move { call(&caller, "cancel-stream").await });
    signals.stream_started.notified().await;
    runtime.swap_out_cell(SCOPE, 1).await?;
    assert!(suspended.await?.is_err());
    assert!(signals.stream_dropped.load(Ordering::SeqCst));
    assert!(bucket
        .head_blob("blob/native-objects/cancelled")
        .await?
        .is_none());
    assert!(signals
        .gates
        .lock()
        .unwrap()
        .iter()
        .any(|ticket| ticket.channel == cellar_logic::Channel::Blob
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    shutdown.send(true)?;
    adapter.await?;
    Ok(())
}
