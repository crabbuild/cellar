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
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
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
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}

fn generation(id: u64, expressions: Vec<String>, signals: Arc<Signals>) -> Result<Generation> {
    generation_from_graph(id, graph(expressions), signals)
}

fn generation_from_graph(
    id: u64,
    graph: DeploymentGraph,
    signals: Arc<Signals>,
) -> Result<Generation> {
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, signals.clone()))
    })
    .cell("CronSink", |context, _| {
        Ok(Arc::new(Sink(context.storage()?)))
    })?;
    Generation::build_native(
        id,
        graph,
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

async fn executions(runtime: &RuntimeManager, scope: &str, query: Value) -> Result<Value> {
    let reply = runtime
        .rpc(
            scope.into(),
            None,
            "__cronHistory".into(),
            RpcData::Json(query.to_string()),
            Some(cellar::engine_api::next_request_id()),
        )
        .await?;
    assert!(
        reply.write_position.is_none(),
        "history must not arm or write"
    );
    let RpcData::Json(value) = reply.data else {
        return Err(anyhow!("expected JSON history"));
    };
    Ok(serde_json::from_str(&value)?)
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
    let empty = executions(&runtime, &scope, json!({})).await?;
    assert_eq!(empty["items"], json!([]));
    assert!(empty["active_alarm"].is_null());
    assert!(
        runtime.alarm(&scope).is_none(),
        "history must not arm a schedule"
    );
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
    let journal = executions(&runtime, &scope, json!({})).await?;
    let attempts = journal["items"].as_array().unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0]["status"], "completed");
    assert_eq!(attempts[1]["status"], "failed_no_retry");
    assert_eq!(attempts[2]["status"], "failed");
    assert_eq!(attempts[2]["background_errors"], 1);
    assert_eq!(attempts[2]["retry_requested"], true);
    assert_eq!(attempts[2]["configuration"]["deployment_version"], "test");
    assert_eq!(attempts[2]["configuration"]["prefix"], "test/");
    assert_eq!(attempts[2]["runtime_generation"], 1);
    assert!(attempts
        .iter()
        .all(|a| a["ended_at"].is_number() && a["duration_ms"].is_number()));
    let first_page = executions(&runtime, &scope, json!({"limit":1})).await?;
    let older = executions(
        &runtime,
        &scope,
        json!({"limit":1,"before":first_page["next_cursor"].as_str().unwrap().parse::<u64>()?}),
    )
    .await?;
    assert_eq!(older["items"][0]["expression"], DECLINE);
    let filtered = executions(&runtime, &scope, json!({"expression":FAIL})).await?;
    assert_eq!(filtered["items"].as_array().unwrap().len(), 1);
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
    let retried = executions(&runtime, &scope, json!({"expression":FAIL})).await?;
    assert_eq!(retried["items"][0]["status"], "completed");
    assert_eq!(retried["items"][0]["attempt"], 2);
    assert_eq!(retried["items"][0]["scheduled_at"], occurrence);
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
    let interrupted = executions(&runtime, &scope, json!({"expression":FAIL})).await?;
    assert_eq!(interrupted["items"][0]["status"], "started");
    assert!(interrupted["items"][0]["ended_at"].is_null());
    assert!(interrupted["items"][0]["duration_ms"].is_null());
    assert!(interrupted["items"][0]["retry_requested"].is_null());
    assert_eq!(interrupted["active_alarm"], occurrence);
    // Real attempts enforce bounded retention, including across activations.
    runtime.swap_out_cell(&scope, 1).await?;
    runtime.adopt(generation(99, vec![SUCCESS.into(); 260], signals.clone())?);
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    seed(&runtime, &data, &scope, occurrence).await?;
    runtime.fire_alarm(98, scope.clone(), occurrence).await?;
    let mut before = None;
    let mut count = 0;
    loop {
        let page = executions(&runtime, &scope, json!({"limit":100,"before":before})).await?;
        count += page["items"].as_array().unwrap().len();
        assert_eq!(page["retention_limit"], 256);
        assert!(page["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["status"] == "completed"));
        before = page["next_cursor"]
            .as_str()
            .map(str::parse::<u64>)
            .transpose()?;
        if before.is_none() {
            break;
        }
    }
    assert_eq!(count, 256);
    // Restore the original interrupted occurrence fixture before continuing
    // the adoption assertions; the retention run armed a future occurrence.
    seed(&runtime, &data, &scope, occurrence).await?;
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
    // Disabling the schedule must remove an already armed future occurrence.
    let future_alarm = cellar::ownership_store::now_ms() as i64 + 60_000;
    seed(&runtime, &data, &scope, future_alarm).await?;
    assert_eq!(runtime.alarm(&scope), Some(future_alarm));
    runtime.swap_out_cell(&scope, 1).await?;
    let mut disabled = graph(Vec::new());
    disabled.primary.cron_revision = Some("disabled-revision".into());
    runtime.adopt(generation_from_graph(3, disabled, signals.clone())?);
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    arm(&runtime, &scope).await?;
    // The arm response can precede publication of the event driver's final
    // alarm snapshot, just like the alarm turn checked above.
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while runtime.alarm(&scope).is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(
        runtime.generation().cron_revision(),
        Some("disabled-revision")
    );
    runtime.swap_out_cell(&scope, 1).await?;
    let db = rusqlite::Connection::open(data.join(&scope).join("ltx/e1/db.sqlite"))?;
    let remaining: u64 = db.query_row(
        "SELECT count(*) FROM __sys_cell_alarm WHERE scope=? AND at_ms IS NOT NULL",
        [&scope],
        |row| row.get(0),
    )?;
    assert_eq!(remaining, 0, "empty override left a persisted alarm");
    drop(db);
    let next = generation(4, Vec::new(), signals)?;
    runtime.adopt(next);
    assert!(runtime.cron_cell().is_none());
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    let archived = executions(&runtime, &scope, json!({"limit":1})).await?;
    assert_eq!(archived["items"].as_array().unwrap().len(), 1);
    assert!(
        runtime.alarm(&scope).is_none(),
        "archived history inspection rearmed a removed schedule"
    );
    runtime.swap_out_cell(&scope, 1).await?;
    shutdown.send(true)?;
    router.await?;
    proofs.await?;
    Ok(())
}
