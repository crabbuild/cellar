//! Native cron turns, routed effects and real wake entries. Clock fixtures
//! are installed only while the cell is retired. No JS engine initialization.

use anyhow::{anyhow, Result};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use cellar::engine_api::{
    Compat, HttpResponse, RpcData, RuntimeFetch, RuntimeOptions, WorkerConfigOptions,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Notify;

const SCRIPT: &str = "native-cron";
const SINK: &str = "CronSink:test";
const FAIL: &str = "0 0 1 1 *";
const DECLINE: &str = "0 0 1 JAN *";
const SUCCESS: &str = "0 0 1 jan *";
#[derive(Default)]
struct Signals {
    calls: Mutex<Vec<(String, i64)>>,
    retained: Mutex<Option<Context>>,
    attempts: AtomicUsize,
    mode: AtomicUsize,
    first_gate: Notify,
    release_gate: Notify,
    background_entered: Notify,
    release_background: Notify,
    background_finished: AtomicBool,
    nested_finished: AtomicBool,
    dropped: AtomicBool,
}
struct DropFlag(Arc<Signals>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
    }
}
struct Sink(Storage);
impl Cell for Sink {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { args, .. } = invocation else {
                return Err(anyhow!("expected sink RPC"));
            };
            let RpcData::Json(args) = args else {
                return Err(anyhow!("expected JSON"));
            };
            let mut notes = self.0.get_json("notes")?.unwrap_or_else(|| json!([]));
            notes
                .as_array_mut()
                .unwrap()
                .push(serde_json::from_str::<Value>(&args)?);
            self.0.put_json("notes", &notes)?;
            Ok(Output::Rpc(RpcData::Json(
                notes.as_array().unwrap().len().to_string(),
            )))
        })
    }
}
async fn note(context: &Context, value: Value) -> Result<()> {
    context
        .rpc_cell(
            SINK.into(),
            None,
            "note".into(),
            RpcData::Json(serde_json::to_string(&value)?),
        )
        .await?;
    Ok(())
}
async fn worker(context: Context, invocation: Invocation, signals: Arc<Signals>) -> Result<Output> {
    let Invocation::Scheduled(controller) = invocation else {
        return Err(anyhow!("expected scheduled event"));
    };
    assert!(context.identity().is_none());
    assert!(context.storage().is_err());
    assert_eq!(context.var("CONFIG"), Some("same script bindings"));
    if let Some(previous) = signals.retained.lock().unwrap().replace(context.clone()) {
        let error = previous
            .wait_until(async { Ok(()) })
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("event context has retired")
                || error.contains("outside its executing turn")
        );
    }
    if controller.cron != FAIL {
        assert!(signals.background_finished.load(Ordering::SeqCst));
        assert!(signals.nested_finished.load(Ordering::SeqCst));
    }
    signals
        .calls
        .lock()
        .unwrap()
        .push((controller.cron.clone(), controller.scheduled_ms));
    let attempt = if controller.cron == FAIL {
        signals.attempts.fetch_add(1, Ordering::SeqCst) + 1
    } else {
        0
    };
    note(
        &context,
        json!({"cron":controller.cron,"at":controller.scheduled_ms,"attempt":attempt}),
    )
    .await?;
    if controller.cron == FAIL && signals.mode.load(Ordering::SeqCst) == 3 {
        let captured = signals.clone();
        context.wait_until(async move {
            let _drop = DropFlag(captured.clone());
            captured.background_entered.notify_one();
            futures_util::future::pending::<Result<()>>().await
        })?;
        return Ok(Output::Done);
    }
    if controller.cron == FAIL && attempt == 1 {
        let captured = signals.clone();
        let child = context.clone();
        context.wait_until(async move {
            captured.background_entered.notify_one();
            captured.release_background.notified().await;
            note(&child, json!({"background":true})).await?;
            let nested = captured.clone();
            child.wait_until(async move {
                nested.nested_finished.store(true, Ordering::SeqCst);
                Ok(())
            })?;
            captured.background_finished.store(true, Ordering::SeqCst);
            // This is allSettled work; failure must not create another retry.
            Err(anyhow!("background failure after its durable effect"))
        })?;
        return Err(anyhow!("first occurrence failed"));
    }
    if controller.cron == DECLINE {
        controller.no_retry();
        return Err(anyhow!("explicitly decline this expression's retry"));
    }
    if controller.cron == FAIL && signals.mode.load(Ordering::SeqCst) == 2 {
        return Err(anyhow!("keep owing this occurrence"));
    }
    Ok(Output::Done)
}
fn graph(expressions: Vec<String>) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec!["CronSink".into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: vec![("CONFIG".into(), "same script bindings".into())],
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
        crons: expressions,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}
fn generation(id: u64, expressions: Vec<String>, signals: Arc<Signals>) -> Result<Generation> {
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, signals.clone()))
    })
    .cell("CronSink", |context, _| {
        Ok(Arc::new(Sink(context.storage()?)))
    })?;
    Generation::build_native(
        id,
        graph(expressions),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            SCRIPT.into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )
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
async fn arm(runtime: &RuntimeManager, scope: &str) -> Result<HttpResponse> {
    let reply = runtime
        .fetch_cell(
            scope.into(),
            None,
            RuntimeFetch {
                url: "https://cron.cellar.internal/arm".into(),
                method: "POST".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await?;
    assert_eq!(reply.status, 204);
    Ok(reply)
}
async fn seed(
    runtime: &RuntimeManager,
    data: &std::path::Path,
    scope: &str,
    at: i64,
) -> Result<()> {
    runtime.swap_out_cell(scope, 1).await?;
    {
        let db = rusqlite::Connection::open(data.join(scope).join("ltx/e1/db.sqlite"))?;
        db.execute("INSERT INTO __sys_cell_alarm(scope,at_ms,retry,counted_retry,generation) VALUES (?,?,0,0,random()) ON CONFLICT(scope) DO UPDATE SET at_ms=excluded.at_ms,retry=0,counted_retry=0,generation=random()",rusqlite::params![scope,at])?;
    }
    runtime.start_cell(scope.into(), 1, false).await?;
    runtime.publish_cell(scope, 1)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_cron_preserves_occurrences_retries_drains_and_activation_lifetimes() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(45), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let expressions = vec![
        FAIL.into(),
        "invalid control-plane entry".into(),
        DECLINE.into(),
        SUCCESS.into(),
    ];
    let built = generation(1, expressions.clone(), signals.clone())?;
    let scope = cellar_logic::cron::reserved_cell(SCRIPT);
    assert_eq!(built.cron_cell(), Some(scope.clone()));
    assert!(built
        .reserved_classes()
        .contains(&cellar_logic::cron::RESERVED_CLASS.into()));
    let directory = tempfile::tempdir()?;
    let data = directory.path().join("cells");
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    let flusher = Arc::new(cellar::wake::WakeFlusher::new());
    cellar::wake_entry::set_arm_gate(cellar::wake_entry::ArmGate {
        bucket: bucket.clone(),
        flusher: flusher.clone(),
    });
    let runtime = RuntimeManager::start(
        built,
        RuntimeOptions {
            data_dir: data.clone(),
            replication: None,
            wake: Some(flusher),
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    for scope in [&scope, SINK] {
        open(&runtime, scope).await?;
    }
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(tx);
    let host = runtime.clone();
    let router = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {"native cron routing adapter stops";_=stop.changed()=>None,call=calls.recv()=>call,}
        {
            let result = host
                .rpc(
                    call.scope,
                    call.name,
                    call.method,
                    call.args,
                    Some(cellar::engine_api::next_request_id()),
                )
                .await
                .map(|reply| reply.data);
            let _ = call.reply.send(result);
        }
    });
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let mut stop = shutdown.subscribe();
    let captured = signals.clone();
    let cron_scope = scope.clone();
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let sampled = tickets.clone();
    let proofs = cellar::asyncrt::spawn(async move {
        let mut held = false;
        while let Some(gate) = cellar::asyncrt::select_biased! {"native cron proof adapter stops";_=stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            sampled
                .lock()
                .unwrap()
                .push((gate.scope.clone(), gate.ticket));
            if !held
                && gate.scope == cron_scope
                && gate.ticket.channel == cellar_logic::Channel::CellRpc
            {
                held = true;
                captured.first_gate.notify_one();
                captured.release_gate.notified().await;
            }
            let _ = gate.reply.send(Ok(()));
        }
    });
    let armed = arm(&runtime, &scope).await?;
    assert!(armed.write_position.is_some());
    let planned = cellar_logic::cron::next_after(
        &cellar_logic::cron::parse(FAIL)?,
        cellar::ownership_store::now_ms() as i64,
    )
    .unwrap();
    assert_eq!(runtime.alarm(&scope), Some(planned));
    arm(&runtime, &scope).await?;
    assert_eq!(runtime.alarm(&scope), Some(planned));
    assert!(
        !bucket.list("").await?.is_empty(),
        "arm acknowledgement must publish a wake entry"
    );
    let occurrence =
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")?.timestamp_millis();
    seed(&runtime, &data, &scope, occurrence).await?;
    arm(&runtime, &scope).await?;
    assert_eq!(
        runtime.alarm(&scope),
        Some(occurrence),
        "a due alarm is a missed occurrence"
    );
    let host = runtime.clone();
    let fire_scope = scope.clone();
    let firing =
        cellar::asyncrt::spawn(async move { host.fire_alarm(1, fire_scope, occurrence).await });
    signals.first_gate.notified().await;
    assert_eq!(signals.calls.lock().unwrap().len(), 1);
    assert!(!signals.background_finished.load(Ordering::SeqCst));
    signals.release_gate.notify_one();
    signals.background_entered.notified().await;
    assert_eq!(
        signals.calls.lock().unwrap().len(),
        1,
        "the next expression must await the preceding event's background work"
    );
    signals.release_background.notify_one();
    firing.await??;
    assert_eq!(
        *signals.calls.lock().unwrap(),
        vec![
            (FAIL.into(), occurrence),
            (DECLINE.into(), occurrence),
            (SUCCESS.into(), occurrence)
        ]
    );
    let retry = runtime.alarm(&scope).unwrap();
    assert!((1..=4000).contains(&(retry - cellar::ownership_store::now_ms() as i64)));
    arm(&runtime, &scope).await?;
    assert_eq!(
        runtime.alarm(&scope),
        Some(retry),
        "arm must preserve the activation's pending retry"
    );
    let wait = retry - cellar::ownership_store::now_ms() as i64;
    if wait >= 0 {
        cellar::asyncrt::sleep(Duration::from_millis((wait + 5) as u64)).await;
    }
    runtime.fire_alarm(2, scope.clone(), retry).await?;
    assert_eq!(
        signals.calls.lock().unwrap().last(),
        Some(&(FAIL.into(), occurrence))
    );
    assert_eq!(
        signals.calls.lock().unwrap().len(),
        4,
        "retry only the failed expression, keeping the original occurrence"
    );
    assert_eq!(runtime.alarm(&scope), Some(planned));
    assert!(tickets
        .lock()
        .unwrap()
        .iter()
        .any(|(cell, ticket)| cell == &scope
            && ticket.channel == cellar_logic::Channel::CellRpc
            && ticket.epoch == Some(1)));
    // Owed expressions intentionally remain activation-local, as before.
    signals.mode.store(2, Ordering::SeqCst);
    seed(&runtime, &data, &scope, occurrence).await?;
    runtime.fire_alarm(3, scope.clone(), occurrence).await?;
    assert!(runtime.alarm(&scope).unwrap() < planned);
    runtime.swap_out_cell(&scope, 1).await?;
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    arm(&runtime, &scope).await?;
    assert_eq!(runtime.alarm(&scope), Some(planned));
    // Retiring a suspended child event must retire its retained context and
    // drop its background future before a replacement activation opens.
    signals.mode.store(3, Ordering::SeqCst);
    seed(&runtime, &data, &scope, occurrence).await?;
    let host = runtime.clone();
    let fire_scope = scope.clone();
    let firing =
        cellar::asyncrt::spawn(async move { host.fire_alarm(4, fire_scope, occurrence).await });
    signals.background_entered.notified().await;
    runtime.swap_out_cell(&scope, 1).await?;
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    cellar::asyncrt::timeout(Duration::from_secs(2), firing).await???;
    assert!(signals.dropped.load(Ordering::SeqCst));
    assert_eq!(
        runtime.alarm(&scope),
        Some(occurrence),
        "retired work must not settle or rewrite the persisted occurrence"
    );
    // Adoption changes the actual config used by the automatic system cell.
    runtime.swap_out_cell(&scope, 1).await?;
    let next = generation(2, vec!["unsatisfiable".into()], signals.clone())?;
    runtime.adopt(next);
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    arm(&runtime, &scope).await?;
    if let Some(at) = runtime.alarm(&scope) {
        assert!(at <= cellar::ownership_store::now_ms() as i64);
        let (alarm, _, _) = runtime.fire_alarm(5, scope.clone(), at).await?;
        assert!(
            alarm.at_ms().is_none(),
            "invalid expressions must clear the final alarm snapshot"
        );
    }
    // Successful fire_alarm returns its final snapshot before its detached
    // drive necessarily publishes the turn's alarm move to the registry cache.
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while runtime.alarm(&scope).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    runtime.swap_out_cell(&scope, 1).await?;
    runtime.swap_out_cell(SINK, 1).await?;
    let next = generation(3, Vec::new(), signals)?;
    runtime.adopt(next);
    assert!(runtime.cron_cell().is_none());
    shutdown.send(true)?;
    router.await?;
    proofs.await?;
    Ok(())
}
