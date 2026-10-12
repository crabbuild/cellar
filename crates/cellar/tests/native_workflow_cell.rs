//! Production native Workflow turns and persisted journals. No engine initialization.

use anyhow::{anyhow, Result};
use cellar::application::workflow::{
    Backoff, CreateOptions, Delay, Duration as WfDuration, Event, RestartFrom, RestartOptions,
    Retries, SendEvent, State, StepConfig, StepFailure, StepKind, Steps,
};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use cellar::engine_api::{
    Compat, HttpResponse, RpcData, RuntimeOptions, WorkerConfigOptions, WorkflowBinding,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native::clone::{Object, StructuredValue, Value as CloneValue};
use cellar::runtime::RuntimeManager;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Notify;

const SCRIPT: &str = "native-workflow-cell";
const SINK: &str = "Sink:effects";
#[derive(Default)]
struct Signals {
    first: AtomicUsize,
    last: AtomicUsize,
    retries: Mutex<Vec<u64>>,
    delays: Mutex<Vec<u64>>,
    permanent: AtomicUsize,
    parallel: Mutex<Vec<u64>>,
    capped: AtomicUsize,
    timeout_started: Notify,
    release_timeout: Notify,
    timeout_finished: AtomicBool,
    race_started: Notify,
    release_race: Notify,
    race_calls: AtomicUsize,
    hung_started: Notify,
    hung_dropped: AtomicBool,
    gates: Mutex<Vec<(String, cellar::actor::GateTicket)>>,
}
struct Dropped(Arc<Signals>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.hung_dropped.store(true, Ordering::SeqCst);
    }
}
struct Sink(Storage);
impl Cell for Sink {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(value),
                ..
            } = invocation
            else {
                return Err(anyhow!("expected effect RPC"));
            };
            let mut effects = self.0.get_json("effects")?.unwrap_or_else(|| json!([]));
            effects
                .as_array_mut()
                .unwrap()
                .push(serde_json::from_str::<Value>(&value)?);
            self.0.put_json("effects", &effects)?;
            Ok(Output::Rpc(RpcData::Json("null".into())))
        })
    }
}
async fn note(context: &Context, value: &str) -> Result<()> {
    context
        .rpc_cell(
            SINK.into(),
            None,
            "note".into(),
            RpcData::Json(serde_json::to_string(value)?),
        )
        .await?;
    Ok(())
}
fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec!["Sink".into(), cellar::deploy::workflow_class(SCRIPT)],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: vec![WorkflowBinding {
                environment: "WF".into(),
                workflow: "orders".into(),
                class: "Orders".into(),
            }],
            vars: vec![("SETTING".into(), "native".into())],
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
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}
fn field(value: &StructuredValue, key: &str) -> StructuredValue {
    value
        .property(&key.encode_utf16().collect::<Vec<_>>())
        .unwrap()
        .unwrap()
}
fn complex() -> StructuredValue {
    StructuredValue {
        root: CloneValue::Reference(0),
        objects: vec![Object::Object(vec![
            ("self".into(), CloneValue::Reference(0)),
            ("lone".into(), CloneValue::String(vec![0xd800])),
            ("zero".into(), CloneValue::Number(-0.0)),
        ])],
    }
}
fn check_complex(value: &StructuredValue) {
    assert_eq!(field(value, "self").root, value.root);
    assert_eq!(field(value, "lone").root, CloneValue::String(vec![0xd800]));
    let CloneValue::Number(value) = field(value, "zero").root else {
        panic!("number");
    };
    assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
}
fn retry_config(limit: f64) -> StepConfig {
    StepConfig {
        retries: Some(Retries {
            limit,
            delay: Delay::Fixed(WfDuration::Milliseconds(0.0)),
            backoff: Some(Backoff::Constant),
        }),
        ..Default::default()
    }
}
async fn workflow(
    context: Context,
    event: Event,
    steps: Steps,
    signals: Arc<Signals>,
) -> Result<StructuredValue> {
    assert!(context.identity().is_none());
    assert!(context.storage().is_err());
    assert_eq!(context.var("SETTING"), Some("native"));
    assert_eq!(event.workflow_name, "orders");
    match event.instance_id.as_str() {
        "main" => {
            let payload = event.payload;
            let counted = signals.clone();
            let ctx = context.clone();
            let value = steps
                .do_step("first", move |attempt| async move {
                    assert_eq!(
                        (attempt.count, attempt.attempt, attempt.retry_limit),
                        (1, 1, 5.0)
                    );
                    counted.first.fetch_add(1, Ordering::SeqCst);
                    note(&ctx, "first").await?;
                    Ok(payload)
                })
                .await?;
            steps.sleep("nap", WfDuration::Milliseconds(5.0)).await?;
            let received = steps
                .wait_for_event("approval", "approve".into(), Some("1 second".into()))
                .await?;
            check_complex(&field(&received, "payload"));
            let counted = signals.clone();
            steps
                .do_step("last", move |_| async move {
                    counted.last.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                })
                .await
        }
        "retry" => {
            let delay_signals = signals.clone();
            let config = StepConfig {
                retries: Some(Retries {
                    limit: 2.0,
                    delay: Delay::Dynamic(Arc::new(move |attempt, error| {
                        delay_signals.delays.lock().unwrap().push(attempt.attempt);
                        assert_eq!(error.message, vec![0xd800]);
                        assert!(attempt.retry_delay.is_none());
                        Box::pin(async { Ok(WfDuration::Milliseconds(0.0)) })
                    })),
                    backoff: Some(Backoff::Linear),
                }),
                ..Default::default()
            };
            steps
                .do_with_config("retry", config, move |attempt| async move {
                    signals.retries.lock().unwrap().push(attempt.attempt);
                    if attempt.attempt < 3 {
                        return Err(StepFailure {
                            record: cellar::application::workflow::ErrorRecord {
                                name: "Error".encode_utf16().collect(),
                                message: vec![0xd800],
                            },
                            non_retryable: false,
                        }
                        .into());
                    }
                    StructuredValue::from_json(json!("retried"))
                })
                .await
        }
        "permanent" => {
            let counted = signals.clone();
            let failed = steps
                .do_step("failed", move |_| async move {
                    counted.permanent.fetch_add(1, Ordering::SeqCst);
                    Err(StepFailure::non_retryable("declined").into())
                })
                .await
                .unwrap_err();
            assert_eq!(failed.to_string(), "declined");
            steps.wait_for_event("gate", "approve".into(), None).await
        }
        "nested" => {
            let nested = steps.clone();
            steps
                .do_step("outer", move |_| async move {
                    nested
                        .do_step("inner", |_| async { Ok(StructuredValue::undefined()) })
                        .await
                })
                .await
        }
        "parallel" => {
            let mut futures = Vec::new();
            for _ in 0..3 {
                let counted = signals.clone();
                let ctx = context.clone();
                futures.push(steps.do_step("same", move |attempt| async move {
                    if attempt.count == 3 {
                        note(&ctx, "detached").await?;
                    }
                    counted.parallel.lock().unwrap().push(attempt.count);
                    StructuredValue::from_json(json!(attempt.count))
                }));
            }
            drop(futures.pop());
            StructuredValue::array(futures_util::future::try_join_all(futures).await?)
        }
        "capped" => {
            steps
                .do_step("big", move |_| async move {
                    signals.capped.fetch_add(1, Ordering::SeqCst);
                    Ok(StructuredValue {
                        root: CloneValue::String(vec![b'x' as u16; (1 << 20) + 1]),
                        objects: Vec::new(),
                    })
                })
                .await
        }
        "deadline" => steps.wait_for_event("expiry", "approve".into(), None).await,
        "timeout" => {
            let mut config = retry_config(0.0);
            config.timeout = Some(WfDuration::Milliseconds(5.0));
            let error = steps
                .do_with_config("timeout", config, move |_| async move {
                    signals.timeout_started.notify_one();
                    signals.release_timeout.notified().await;
                    note(&context, "late timeout effect").await?;
                    signals.timeout_finished.store(true, Ordering::SeqCst);
                    StructuredValue::from_json(json!("late result"))
                })
                .await
                .unwrap_err();
            assert!(error.to_string().contains("timed out"));
            StructuredValue::from_json(json!("timeout caught"))
        }
        "race" => {
            let counted = signals.clone();
            steps
                .do_step("held", move |_| async move {
                    counted.race_calls.fetch_add(1, Ordering::SeqCst);
                    counted.race_started.notify_one();
                    counted.release_race.notified().await;
                    Ok(StructuredValue::undefined())
                })
                .await?;
            steps.wait_for_event("gate", "approve".into(), None).await
        }
        "hung" => {
            let _drop = Dropped(signals.clone());
            signals.hung_started.notify_one();
            futures_util::future::pending().await
        }
        "atomic-event" => {
            match steps
                .wait_for_event("consume", "approve".into(), None)
                .await
            {
                Ok(value) => Ok(value),
                Err(_) => StructuredValue::from_json(json!("application caught the storage error")),
            }
        }
        _ => Err(anyhow!("unknown workflow")),
    }
}
async fn worker(context: Context, invocation: Invocation) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        return Err(anyhow!("expected fetch"));
    };
    let url = url::Url::parse(&request.url)?;
    let mut paths = url.path().trim_start_matches('/').split('/');
    let action = paths.next().unwrap();
    let id = paths.next().unwrap();
    let binding = context.workflow("WF")?;
    let instance = if action == "create" {
        binding
            .create(
                &context,
                CreateOptions {
                    id: Some(id.into()),
                    params: Some(complex()),
                    ..Default::default()
                },
            )
            .await?
    } else {
        binding.get(&context, id).await?
    };
    let value = match action {
        "create" => json!("created"),
        "status" => {
            let status = instance.status(&context).await?;
            if let Some(output) = status.output {
                if id == "main" {
                    check_complex(&output);
                }
            }
            json!({"state":status.state, "error":status.error.map(|error| error.message_lossy())})
        }
        "pause" => {
            instance.pause(&context).await?;
            json!("paused")
        }
        "resume" => {
            instance.resume(&context).await?;
            json!("resumed")
        }
        "event" => {
            instance
                .send_event(
                    &context,
                    SendEvent {
                        event_type: "approve".into(),
                        payload: Some(complex()),
                    },
                )
                .await?;
            json!("sent")
        }
        "restart" => {
            instance
                .restart(&context, RestartOptions::default())
                .await?;
            json!("restarted")
        }
        "selected" => {
            instance
                .restart(
                    &context,
                    RestartOptions {
                        from: Some(RestartFrom {
                            name: "last".encode_utf16().collect(),
                            count: 1,
                            kind: StepKind::Do,
                        }),
                    },
                )
                .await?;
            json!("restarted")
        }
        "terminate" => {
            instance.terminate(&context).await?;
            json!("terminated")
        }
        "delete" => {
            instance.delete(&context).await?;
            json!("deleted")
        }
        _ => return Err(anyhow!("unknown action")),
    };
    Ok(Output::Http(HttpResponse {
        status: 200,
        body: serde_json::to_vec(&value)?,
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}
async fn open(runtime: &RuntimeManager, scope: &str) -> Result<()> {
    runtime
        .restore_cell(
            scope,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(scope.into(), 1, true).await?;
    runtime.publish_cell(scope, 1)
}
fn scope(id: &str) -> String {
    cellar::engine_api::workflow_cell_scope(SCRIPT, "orders", id)
}
async fn call(runtime: &RuntimeManager, action: &str, id: &str) -> Result<Value> {
    let reply = runtime
        .fetch_worker(
            format!("http://worker/{action}/{id}"),
            "GET".into(),
            Vec::new(),
            Vec::new(),
        )
        .await?;
    assert_eq!(reply.status, 200);
    Ok(serde_json::from_slice(&reply.body)?)
}
async fn state(runtime: &RuntimeManager, id: &str, expected: State) -> Result<()> {
    assert_eq!(
        call(runtime, "status", id).await?["state"],
        serde_json::to_value(expected)?
    );
    Ok(())
}
async fn fire(runtime: &RuntimeManager, id: &str, op: u64) -> Result<()> {
    let scope = scope(id);
    let at = runtime
        .alarm(&scope)
        .ok_or_else(|| anyhow!("missing alarm for {id}"))?;
    let remaining = at.saturating_sub(cellar::ownership_store::now_ms() as i64);
    if remaining > 0 {
        cellar::asyncrt::sleep(Duration::from_millis(remaining as u64)).await;
    }
    runtime.fire_alarm(op, scope, at).await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_workflow_journals_controls_retries_and_cancellation() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let handler_signals = signals.clone();
    let registry = Registry::new(|context, invocation| Box::pin(worker(context, invocation)))
        .cell("Sink", |context, _| Ok(Arc::new(Sink(context.storage()?))))?
        .workflow("Orders", move |context, event, steps| {
            Box::pin(workflow(context, event, steps, handler_signals.clone()))
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
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    let flusher = Arc::new(cellar::wake::WakeFlusher::new());
    cellar::wake_entry::set_arm_gate(cellar::wake_entry::ArmGate {
        bucket: bucket.clone(),
        flusher: flusher.clone(),
    });
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().into(),
            replication: None,
            wake: Some(flusher),
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    open(&runtime, SINK).await?;
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(tx);
    let target = runtime.clone();
    let router = cellar::asyncrt::spawn(async move {
        let mut opened = std::collections::HashSet::from([SINK.to_string()]);
        while let Some(call) = cellar::asyncrt::select_biased! { "native workflow routing stops"; _=stop.changed()=>None, call=calls.recv()=>call, }
        {
            let result = async {
                if opened.insert(call.scope.clone()) {
                    open(&target, &call.scope).await?;
                }
                Ok(target
                    .rpc(
                        call.scope,
                        call.name,
                        call.method,
                        call.args,
                        Some(cellar::engine_api::next_request_id()),
                    )
                    .await?
                    .data)
            }
            .await;
            let _ = call.reply.send(result);
        }
        Ok::<_, anyhow::Error>(())
    });
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let mut stop = shutdown.subscribe();
    let counted = signals.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! { "native workflow proof stops"; _=stop.changed()=>None, gate=gates.recv()=>gate, }
        {
            counted
                .gates
                .lock()
                .unwrap()
                .push((gate.scope.clone(), gate.ticket));
            let _ = gate.reply.send(Ok(()));
        }
    });
    call(&runtime, "create", "main").await?;
    assert!(
        !bucket.list("").await?.is_empty(),
        "creation must publish its wake before acknowledging"
    );
    call(&runtime, "pause", "main").await?;
    state(&runtime, "main", State::Paused).await?;
    call(&runtime, "resume", "main").await?;
    fire(&runtime, "main", 1).await?;
    state(&runtime, "main", State::Waiting).await?;
    assert_eq!(signals.first.load(Ordering::SeqCst), 1);
    runtime.swap_out_cell(&scope("main"), 1).await?;
    runtime.start_cell(scope("main"), 1, true).await?;
    runtime.publish_cell(&scope("main"), 1)?;
    fire(&runtime, "main", 2).await?;
    state(&runtime, "main", State::Waiting).await?;
    call(&runtime, "pause", "main").await?;
    call(&runtime, "event", "main").await?;
    call(&runtime, "resume", "main").await?;
    fire(&runtime, "main", 3).await?;
    state(&runtime, "main", State::Complete).await?;
    assert_eq!(signals.first.load(Ordering::SeqCst), 1);
    assert_eq!(signals.last.load(Ordering::SeqCst), 1);
    call(&runtime, "selected", "main").await?;
    fire(&runtime, "main", 4).await?;
    state(&runtime, "main", State::Complete).await?;
    assert_eq!(signals.first.load(Ordering::SeqCst), 1);
    assert_eq!(signals.last.load(Ordering::SeqCst), 2);
    call(&runtime, "create", "retry").await?;
    for op in 5..=7 {
        fire(&runtime, "retry", op).await?;
    }
    state(&runtime, "retry", State::Complete).await?;
    assert_eq!(*signals.retries.lock().unwrap(), [1, 2, 3]);
    assert_eq!(*signals.delays.lock().unwrap(), [1, 2]);
    call(&runtime, "create", "permanent").await?;
    fire(&runtime, "permanent", 8).await?;
    call(&runtime, "event", "permanent").await?;
    fire(&runtime, "permanent", 9).await?;
    state(&runtime, "permanent", State::Complete).await?;
    assert_eq!(signals.permanent.load(Ordering::SeqCst), 1);
    call(&runtime, "create", "nested").await?;
    fire(&runtime, "nested", 10).await?;
    state(&runtime, "nested", State::Errored).await?;
    assert!(call(&runtime, "status", "nested").await?["error"]
        .as_str()
        .unwrap()
        .contains("cannot start inside"));
    call(&runtime, "create", "parallel").await?;
    fire(&runtime, "parallel", 20).await?;
    state(&runtime, "parallel", State::Complete).await?;
    while signals.parallel.lock().unwrap().len() < 3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        *signals.parallel.lock().unwrap(),
        [1, 2, 3],
        "occurrence counts are assigned when called and discarded futures remain cell-owned"
    );
    call(&runtime, "create", "capped").await?;
    fire(&runtime, "capped", 21).await?;
    state(&runtime, "capped", State::Errored).await?;
    assert_eq!(signals.capped.load(Ordering::SeqCst), 1);
    call(&runtime, "create", "timeout").await?;
    let fired = runtime.clone();
    let timeout = cellar::asyncrt::spawn(async move { fire(&fired, "timeout", 11).await });
    signals.timeout_started.notified().await;
    loop {
        if call(&runtime, "status", "timeout").await?["state"] == "complete" {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!signals.timeout_finished.load(Ordering::SeqCst));
    signals.release_timeout.notify_one();
    timeout.await??;
    while !signals.timeout_finished.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    state(&runtime, "timeout", State::Complete).await?;
    call(&runtime, "create", "race").await?;
    let fired = runtime.clone();
    let race = cellar::asyncrt::spawn(async move { fire(&fired, "race", 12).await });
    signals.race_started.notified().await;
    call(&runtime, "pause", "race").await?;
    state(&runtime, "race", State::WaitingForPause).await?;
    call(&runtime, "resume", "race").await?;
    let nudged = runtime.clone();
    let concurrent = cellar::asyncrt::spawn(async move { fire(&nudged, "race", 22).await });
    state(&runtime, "race", State::Running).await?;
    signals.release_race.notify_one();
    race.await??;
    concurrent.await??;
    assert_eq!(signals.race_calls.load(Ordering::SeqCst), 1);
    state(&runtime, "race", State::Waiting).await?;
    call(&runtime, "terminate", "race").await?;
    state(&runtime, "race", State::Terminated).await?;
    call(&runtime, "restart", "race").await?;
    let fired = runtime.clone();
    let old_generation = cellar::asyncrt::spawn(async move { fire(&fired, "race", 23).await });
    signals.race_started.notified().await;
    call(&runtime, "restart", "race").await?;
    call(&runtime, "terminate", "race").await?;
    signals.release_race.notify_one();
    old_generation.await??;
    state(&runtime, "race", State::Terminated).await?;
    assert_eq!(
        signals.race_calls.load(Ordering::SeqCst),
        2,
        "an older drive cannot overwrite a restarted and terminated generation"
    );
    call(&runtime, "create", "hung").await?;
    let fired = runtime.clone();
    let hung = cellar::asyncrt::spawn(async move { fire(&fired, "hung", 13).await });
    signals.hung_started.notified().await;
    call(&runtime, "delete", "hung").await?;
    hung.await??;
    assert!(signals.hung_dropped.load(Ordering::SeqCst));
    assert!(call(&runtime, "status", "hung").await.is_err());
    assert!(runtime.alarm(&scope("hung")).is_none());
    // A real SQLite failure between metadata and alarm writes must roll both
    // back. Fixture changes only happen after retiring the production cell.
    assert!(call(&runtime, "status", "create-fault").await.is_err());
    let create_scope = scope("create-fault");
    runtime.swap_out_cell(&create_scope, 1).await?;
    let create_db = directory
        .path()
        .join(&create_scope)
        .join("ltx/e1/db.sqlite");
    {
        let db = rusqlite::Connection::open(&create_db)?;
        db.execute_batch("CREATE TRIGGER refuse_alarm BEFORE INSERT ON __sys_cell_alarm BEGIN SELECT RAISE(ABORT,'injected creation alarm failure'); END;")?;
    }
    runtime.start_cell(create_scope.clone(), 1, false).await?;
    runtime.publish_cell(&create_scope, 1)?;
    assert!(call(&runtime, "create", "create-fault").await.is_err());
    assert!(runtime.alarm(&create_scope).is_none());
    runtime.swap_out_cell(&create_scope, 1).await?;
    {
        let db = rusqlite::Connection::open(&create_db)?;
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM __sys_cell_kv WHERE k='__wf.meta'",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            0
        );
        db.execute_batch("DROP TRIGGER refuse_alarm;")?;
    }
    runtime.start_cell(create_scope.clone(), 1, false).await?;
    runtime.publish_cell(&create_scope, 1)?;
    call(&runtime, "create", "create-fault").await?;
    call(&runtime, "create", "atomic-event").await?;
    fire(&runtime, "atomic-event", 14).await?;
    let atomic_scope = scope("atomic-event");
    runtime.swap_out_cell(&atomic_scope, 1).await?;
    let atomic_db = directory
        .path()
        .join(&atomic_scope)
        .join("ltx/e1/db.sqlite");
    {
        let db = rusqlite::Connection::open(&atomic_db)?;
        db.execute_batch("CREATE TRIGGER refuse_consumption BEFORE UPDATE ON __sys_cell_kv WHEN NEW.k LIKE '__wf.%.step.%' BEGIN SELECT RAISE(ABORT,'injected completion ledger failure'); END;")?;
    }
    runtime.start_cell(atomic_scope.clone(), 1, false).await?;
    runtime.publish_cell(&atomic_scope, 1)?;
    call(&runtime, "event", "atomic-event").await?;
    fire(&runtime, "atomic-event", 15).await?;
    state(&runtime, "atomic-event", State::Running).await?;
    runtime.swap_out_cell(&atomic_scope, 1).await?;
    {
        let db = rusqlite::Connection::open(&atomic_db)?;
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM __sys_cell_kv WHERE k LIKE '__wf.%.event.%'",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            1,
            "a failed completion write must leave its acknowledged event buffered"
        );
        let ledger = db.query_row(
            "SELECT v FROM __sys_cell_kv WHERE k LIKE '__wf.%.step.%'",
            [],
            |row| row.get::<_, Vec<u8>>(0),
        )?;
        let ledger = cellar::native::clone::decode(&ledger)?;
        assert_eq!(field(&ledger, "status").root, CloneValue::from("waiting"));
        db.execute_batch("DROP TRIGGER refuse_consumption;")?;
        db.execute(
            "UPDATE __sys_cell_alarm SET at_ms=?, retry=0, counted_retry=0, generation=random()",
            [cellar::ownership_store::now_ms() as i64],
        )?;
    }
    runtime.start_cell(atomic_scope.clone(), 1, false).await?;
    runtime.publish_cell(&atomic_scope, 1)?;
    fire(&runtime, "atomic-event", 16).await?;
    state(&runtime, "atomic-event", State::Complete).await?;
    call(&runtime, "create", "deadline").await?;
    fire(&runtime, "deadline", 24).await?;
    let deadline_scope = scope("deadline");
    runtime.swap_out_cell(&deadline_scope, 1).await?;
    {
        let db = rusqlite::Connection::open(
            directory
                .path()
                .join(&deadline_scope)
                .join("ltx/e1/db.sqlite"),
        )?;
        let (key, value) = db.query_row(
            "SELECT k,v FROM __sys_cell_kv WHERE k LIKE '__wf.%.step.%'",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        let mut record = cellar::native::clone::decode(&value)?;
        record.set_property(
            "deadline".encode_utf16().collect(),
            &StructuredValue::from_json(json!(0))?,
        )?;
        db.execute(
            "UPDATE __sys_cell_kv SET v=? WHERE k=?",
            rusqlite::params![cellar::native::clone::encode(&record)?, key],
        )?;
    }
    runtime.start_cell(deadline_scope.clone(), 1, false).await?;
    runtime.publish_cell(&deadline_scope, 1)?;
    call(&runtime, "event", "deadline").await?;
    fire(&runtime, "deadline", 25).await?;
    state(&runtime, "deadline", State::Errored).await?;
    assert!(call(&runtime, "status", "deadline").await?["error"]
        .as_str()
        .unwrap()
        .contains("timed out"));
    assert!(signals
        .gates
        .lock()
        .unwrap()
        .iter()
        .any(|(cell, ticket)| cell == &scope("main")
            && ticket.channel == cellar_logic::Channel::CellRpc
            && ticket.position.is_some()
            && ticket.epoch == Some(1)));
    shutdown.send(true)?;
    router.await??;
    adapter.await?;
    for id in [
        "main",
        "retry",
        "permanent",
        "nested",
        "timeout",
        "race",
        "hung",
        "create-fault",
        "atomic-event",
        "parallel",
        "capped",
        "deadline",
    ] {
        runtime.swap_out_cell(&scope(id), 1).await?;
    }
    runtime.swap_out_cell(SINK, 1).await?;
    Ok(())
}
