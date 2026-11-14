// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production native Workflow turns and persisted journals. No engine initialization.

use anyhow::{anyhow, Context as _, Result};
use cellar::application::workflow::{BindingSettings, Retention, State};
use cellar::application::Registry;
use cellar::engine_api::{Compat, RpcData, RuntimeOptions, WorkerConfigOptions, WorkflowBinding};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native::clone::{StructuredValue, Value as CloneValue};
use cellar::runtime::RuntimeManager;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

#[path = "support/workflow_contract.rs"]
mod workflow_contract;
use workflow_contract::*;

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
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
                settings: BindingSettings {
                    default_retention: Retention {
                        success: Some("1 day".into()),
                        error: None,
                    },
                    schedules: vec!["* * * * *".into()],
                    definition: Some(serde_json::from_value(json!({"version":"orders-v1","nodes":[{"id":"first","label":"Prepare order","kind":"activity","step":"first"}],"edges":[]})).unwrap()),
                },
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
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
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
    let value = call(runtime, "status", id).await?;
    assert_eq!(
        value["state"],
        serde_json::to_value(expected)?,
        "Workflow {id}: {value}"
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
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut limits = cellar::sdk::CellLimits::default();
    limits.workflow.max_stream_bytes = 3 * 1024 * 1024;
    domain.services().configure_cell_limits(limits)?;
    domain
        .run(async { cellar::asyncrt::timeout(Duration::from_secs(30), run()).await? })
        .await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let handler_signals = signals.clone();
    let worker_signals = signals.clone();
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, worker_signals.clone()))
    })
    .cell("Sink", |context, _| Ok(Arc::new(Sink(context.storage()?))))?
    .workflow("Orders", move |context, event, steps| {
        Box::pin(workflow(context, event, steps, handler_signals.clone()))
    })?;
    let mut deployment = graph();
    // A referenced script's workflow schedule must not replace the primary cron cell.
    let mut auxiliary = graph().primary;
    auxiliary.script_name = "workflow-auxiliary".into();
    auxiliary.options.script_name = "workflow-auxiliary".into();
    auxiliary.options.do_classes = vec![cellar::deploy::workflow_class("workflow-auxiliary")];
    deployment.cohosted.push(auxiliary);
    let registry = Arc::new(registry) as Arc<dyn cellar::application::Application>;
    let generation = Generation::build_native(
        1,
        deployment,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([
            (SCRIPT.into(), registry.clone()),
            ("workflow-auxiliary".into(), registry.clone()),
        ]),
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
    open(&runtime, &sink_scope()).await?;
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(tx);
    let target = runtime.clone();
    let routed_signals = signals.clone();
    let router = cellar::asyncrt::spawn(async move {
        let mut opened = std::collections::HashSet::from([sink_scope()]);
        let mut tasks = Vec::new();
        while let Some(call) = cellar::asyncrt::select_biased! { "native workflow routing stops"; _=stop.changed()=>None, call=calls.recv()=>call, }
        {
            if opened.insert(call.scope.clone()) {
                if let Some(name) = call
                    .name
                    .as_deref()
                    .and_then(|name| name.strip_prefix("orders/"))
                {
                    if name.starts_with("cron-") {
                        routed_signals.extra_cells.lock().unwrap().push(name.into());
                    }
                }
                open(&target, &call.scope).await?;
            }
            let target = target.clone();
            tasks.push(cellar::asyncrt::spawn(async move {
                let result = target
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
            }));
        }
        for task in tasks {
            task.await?;
        }
        Ok::<_, anyhow::Error>(())
    });
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let mut stop = shutdown.subscribe();
    let gate_tickets = Arc::new(std::sync::Mutex::new(Vec::new()));
    let counted = gate_tickets.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! { "native workflow proof stops"; _=stop.changed()=>None, gate=gates.recv()=>gate, }
        {
            counted
                .lock()
                .unwrap()
                .push((gate.scope.clone(), gate.ticket));
            let _ = gate.reply.send(Ok(()));
        }
    });
    // The operator path uses the same durable controls and validates the
    // declared binding and instance identity before changing state.
    open(&runtime, &scope("console")).await?;
    let console_request = |body: Value| cellar::engine_api::RuntimeFetch {
        url: "http://cell/".into(),
        method: "POST".into(),
        body: body.to_string().into_bytes().into(),
        headers: vec![],
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    };
    for command in [
        json!({"operation":"start","params":{"from":"dashboard"}}),
        json!({"operation":"pause"}),
        json!({"operation":"status"}),
        json!({"operation":"resume"}),
        json!({"operation":"send_event","event_type":"approval","payload":{"approved":true}}),
        json!({"operation":"history","cursor":0}),
        json!({"operation":"history_search","cursor":0,"query":"workflow"}),
        json!({"operation":"terminate","rollback":false}),
        json!({"operation":"restart"}),
    ] {
        let expected_generation = if command["operation"] == "start" {
            None
        } else {
            let observed = runtime.fetch_cell(scope("console"),None,console_request(json!({"binding":"WF","instance":"console","command":{"operation":"status"}})),None).await?;
            let observed: Value = serde_json::from_slice(&observed.body)?;
            Some(
                observed["result"]["generation"]
                    .as_str()
                    .context("missing workflow generation")?
                    .to_string(),
            )
        };
        let reply = runtime
            .fetch_cell(
                scope("console"),
                None,
                cellar::engine_api::RuntimeFetch {
                    url: "http://cell/".into(),
                    method: "POST".into(),
                    body: serde_json::to_vec(
                        &json!({"binding":"WF","instance":"console","command":command,"expected_generation":expected_generation}),
                    )?
                    .into(),
                    headers: vec![],
                    request_id: Some(cellar::engine_api::next_request_id()),
                    order: None,
                    parent: None,
                },
                None,
            )
            .await?;
        assert_eq!(
            reply.status,
            200,
            "operator refused {command}: {}",
            String::from_utf8_lossy(&reply.body)
        );
        let result: Value = serde_json::from_slice(&reply.body)?;
        if command["operation"] == "status" {
            assert_eq!(result["result"]["status"], "paused");
            assert_eq!(
                result["result"]["definitionSnapshot"]["definition"]["version"],
                "orders-v1"
            );
        }
        if command["operation"] == "history" {
            assert!(result["result"]["count"].as_u64().unwrap() > 0);
            assert!(result["result"]["events"][0]["generation"].is_string());
            assert!(!result["result"]["definitions"]
                .as_object()
                .unwrap()
                .is_empty());
        }
    }
    let reply = runtime.fetch_cell(scope("console"), None, cellar::engine_api::RuntimeFetch {
        url:"http://cell/".into(),method:"POST".into(),
        body:serde_json::to_vec(&json!({"binding":"WF","instance":"wrong-id","command":{"operation":"terminate"}}))?.into(),
        headers:vec![],request_id:Some(cellar::engine_api::next_request_id()),order:None,parent:None,
    },None).await?;
    assert_eq!(reply.status, 400);
    let stale = runtime.fetch_cell(scope("console"), None, console_request(json!({"binding":"WF","instance":"console","command":{"operation":"pause"},"expected_generation":"stale-generation"})),None).await?;
    assert_eq!(stale.status, 400);
    assert!(String::from_utf8_lossy(&stale.body).contains("generation changed"));
    runtime.swap_out_cell(&scope("console"), 1).await?;

    assert_eq!(
        runtime.cron_cell(),
        Some(cellar_logic::cron::reserved_cell(SCRIPT))
    );
    let cron_scope = cellar_logic::cron::reserved_cell(SCRIPT);
    open(&runtime, &cron_scope).await?;
    runtime
        .fetch_cell(
            cron_scope.clone(),
            None,
            cellar::engine_api::RuntimeFetch {
                url: "https://cron.cellar.internal/arm".into(),
                method: "POST".into(),
                body: Vec::new().into(),
                headers: vec![],
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await?;
    let occurrence =
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")?.timestamp_millis();
    // Install the clock fixture only after the cron owner has retired.
    runtime.swap_out_cell(&cron_scope, 1).await?;
    {
        let database = rusqlite::Connection::open(
            directory.path().join(&cron_scope).join("ltx/e1/db.sqlite"),
        )?;
        database.execute("INSERT INTO __sys_cell_alarm(scope,at_ms,retry,counted_retry,generation) VALUES (?,?,0,0,random()) ON CONFLICT(scope) DO UPDATE SET at_ms=excluded.at_ms,retry=0,counted_retry=0,generation=random()",rusqlite::params![cron_scope,occurrence])?;
    }
    runtime.start_cell(cron_scope.clone(), 1, false).await?;
    runtime.publish_cell(&cron_scope, 1)?;
    runtime
        .fire_alarm(114, cron_scope.clone(), occurrence)
        .await?;
    let scheduled_id = signals
        .extra_cells
        .lock()
        .unwrap()
        .iter()
        .find(|id| id.starts_with("cron-"))
        .unwrap()
        .clone();
    fire(&runtime, &scheduled_id, 115).await?;
    assert_eq!(
        call(&runtime, "status", &scheduled_id).await?["output"],
        json!({"cron":"* * * * *","scheduled_ms":occurrence})
    );
    let cron_reply = runtime
        .rpc(
            cron_scope.clone(),
            None,
            "__cronHistory".into(),
            RpcData::Json("{}".into()),
            Some(cellar::engine_api::next_request_id()),
        )
        .await?;
    let RpcData::Json(cron_body) = cron_reply.data else {
        anyhow::bail!("expected JSON cron history");
    };
    let cron_journal: Value = json!({"result":serde_json::from_str::<Value>(&cron_body)?});
    assert_eq!(
        cron_journal["result"]["items"][0]["status"],
        "workflow_accepted"
    );
    assert_eq!(
        cron_journal["result"]["items"][0]["workflow_instance"],
        scheduled_id
    );
    assert_eq!(cron_journal["result"]["items"][0]["workflow_binding"], "WF");
    runtime.swap_out_cell(&cron_scope, 1).await?;
    call(&runtime, "batch", "unused").await?;
    call(&runtime, "create", "rollback").await?;
    fire(&runtime, "rollback", 100).await?;
    state(&runtime, "rollback", State::Running).await?;
    fire(&runtime, "rollback", 101).await?;
    runtime.swap_out_cell(&scope("rollback"), 1).await?;
    runtime.start_cell(scope("rollback"), 1, false).await?;
    runtime.publish_cell(&scope("rollback"), 1)?;
    fire(&runtime, "rollback", 102).await?;
    state(&runtime, "rollback", State::Errored).await?;
    assert_eq!(signals.v2_forward.load(Ordering::SeqCst), 2);
    assert_eq!(
        *signals.v2_rollback.lock().unwrap(),
        vec![
            ("charge".into(), 1),
            ("charge".into(), 2),
            ("reserve".into(), 1)
        ]
    );
    assert_eq!(
        call(&runtime, "status", "rollback").await?["rollback"]["outcome"],
        "complete"
    );
    let history = call(&runtime, "history", "rollback").await?;
    let ids = history
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert!(ids.windows(2).all(|window| window[1] > window[0]));
    assert!(history
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["type"] == "rollback_attempt_errored"));
    assert_eq!(
        call(&runtime, "history-resume", "rollback").await?,
        json!(history
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["id"].as_u64().unwrap() > 3)
            .collect::<Vec<_>>())
    );
    call(&runtime, "create", "rollback-failed").await?;
    fire(&runtime, "rollback-failed", 103).await?;
    fire(&runtime, "rollback-failed", 104).await?;
    assert_eq!(
        call(&runtime, "status", "rollback-failed").await?["rollback"]["outcome"],
        "failed"
    );
    call(&runtime, "create", "terminate-rollback").await?;
    fire(&runtime, "terminate-rollback", 105).await?;
    call(&runtime, "terminate-rollback", "terminate-rollback").await?;
    fire(&runtime, "terminate-rollback", 106).await?;
    fire(&runtime, "terminate-rollback", 107).await?;
    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
    state(&runtime, "terminate-rollback", State::Terminated).await?;
    call(&runtime, "create", "sensitive").await?;
    fire(&runtime, "sensitive", 108).await?;
    let sensitive = call(&runtime, "filtered", "sensitive").await?;
    assert_eq!(sensitive.as_array().unwrap().len(), 1);
    assert_eq!(sensitive[0]["output"], "[REDACTED]");
    let reply = runtime.fetch_cell(scope("sensitive"), None, console_request(json!({"binding":"WF","instance":"sensitive","command":{"operation":"history","cursor":0}})),None).await?;
    assert_eq!(reply.status, 200);
    let sensitive_history: Value = serde_json::from_slice(&reply.body)?;
    assert!(!String::from_utf8_lossy(&reply.body).contains("input-secret"));
    assert!(sensitive_history["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["input"] == "[REDACTED]"));

    for (query, expected) in [
        ("input-secret", false),
        ("[REDACTED]", true),
        ("absent-search-term", false),
    ] {
        let reply=runtime.fetch_cell(scope("sensitive"),None,console_request(json!({"binding":"WF","instance":"sensitive","command":{"operation":"history_search","cursor":0,"query":query}})),None).await?;
        assert_eq!(reply.status, 200);
        let result: Value = serde_json::from_slice(&reply.body)?;
        assert_eq!(
            !result["result"]["events"].as_array().unwrap().is_empty(),
            expected
        );
        assert!(result["result"]["scanned"].as_u64().unwrap() > 0);
        assert_eq!(result["result"]["at_end"], true);
        assert!(!String::from_utf8_lossy(&reply.body).contains("input-secret"));
    }
    assert!(
        cellar::application::workflow::OperatorAction::HistorySearch {
            cursor: 0,
            query: "x".into()
        }
        .is_read()
    );
    call(&runtime, "create", "rollback-held").await?;
    fire(&runtime, "rollback-held", 120).await?;
    let driving = runtime.clone();
    let held = cellar::asyncrt::spawn(async move { fire(&driving, "rollback-held", 121).await });
    signals.rollback_started.notified().await;
    call(&runtime, "pause", "rollback-held").await?;
    state(&runtime, "rollback-held", State::WaitingForPause).await?;
    signals.release_rollback.notify_one();
    held.await??;
    state(&runtime, "rollback-held", State::Paused).await?;
    let attempts = signals.v2_rollback.lock().unwrap().len();
    runtime.swap_out_cell(&scope("rollback-held"), 1).await?;
    runtime.start_cell(scope("rollback-held"), 1, false).await?;
    runtime.publish_cell(&scope("rollback-held"), 1)?;
    call(&runtime, "resume", "rollback-held").await?;
    fire(&runtime, "rollback-held", 122).await?;
    state(&runtime, "rollback-held", State::Errored).await?;
    assert_eq!(signals.v2_rollback.lock().unwrap().len(), attempts + 1);
    assert_eq!(
        signals.v2_rollback.lock().unwrap().last().unwrap().0,
        "reserve"
    );
    call(&runtime, "create", "stream").await?;
    call(&runtime, "create", "bad-delay").await?;
    fire(&runtime, "bad-delay", 129).await?;
    state(&runtime, "bad-delay", State::Waiting).await?;
    call(&runtime, "event", "bad-delay").await?;
    fire(&runtime, "bad-delay", 130).await?;
    state(&runtime, "bad-delay", State::Complete).await?;
    assert_eq!(signals.bad_delay_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        call(&runtime, "close-subscription", "stream").await?,
        json!(true)
    );
    let observing = runtime.clone();
    let live = cellar::asyncrt::spawn(async move { call(&observing, "live", "stream").await });
    signals.subscribed.notified().await;
    fire(&runtime, "stream", 109).await?;
    state(&runtime, "stream", State::Waiting).await?;
    let events = live.await??;
    assert_eq!(events.as_array().unwrap().len(), 1);
    assert_eq!(events[0]["step"], "bytes");
    assert_eq!(
        call(&runtime, "read-stream", "stream").await?,
        2 * 1024 * 1024 + 17
    );
    call(&runtime, "create", "stream-retry").await?;
    fire(&runtime, "stream-retry", 123).await?;
    state(&runtime, "stream-retry", State::Waiting).await?;
    runtime.swap_out_cell(&scope("stream-retry"), 1).await?;
    runtime.start_cell(scope("stream-retry"), 1, false).await?;
    runtime.publish_cell(&scope("stream-retry"), 1)?;
    fire(&runtime, "stream-retry", 124).await?;
    state(&runtime, "stream-retry", State::Complete).await?;
    assert_eq!(signals.stream_retries.load(Ordering::SeqCst), 2);
    assert_eq!(
        call(&runtime, "read-stream", "stream-retry").await?,
        2 * 1024 * 1024 + 17
    );
    call(&runtime, "create", "stream-rollback").await?;
    fire(&runtime, "stream-rollback", 125).await?;
    fire(&runtime, "stream-rollback", 126).await?;
    state(&runtime, "stream-rollback", State::Errored).await?;
    assert_eq!(signals.stream_compensations.load(Ordering::SeqCst), 1);
    call(&runtime, "create", "stream-quota").await?;
    fire(&runtime, "stream-quota", 127).await?;
    fire(&runtime, "stream-quota", 128).await?;
    state(&runtime, "stream-quota", State::Errored).await?;
    assert_eq!(signals.stream_retries.load(Ordering::SeqCst), 4);
    runtime.swap_out_cell(&scope("stream"), 1).await?;
    runtime.start_cell(scope("stream"), 1, false).await?;
    runtime.publish_cell(&scope("stream"), 1)?;
    call(&runtime, "event", "stream").await?;
    fire(&runtime, "stream", 110).await?;
    state(&runtime, "stream", State::Complete).await?;
    assert_eq!(signals.stream_runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        call(&runtime, "read-stream", "stream").await?,
        2 * 1024 * 1024 + 17
    );
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
    let prior_run = call(&runtime, "history", "main").await?;
    assert_eq!(
        prior_run.as_array().unwrap().last().unwrap()["type"],
        "workflow_completed"
    );
    assert_eq!(
        prior_run
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["type"] == "workflow_completed")
            .count(),
        1
    );
    call(&runtime, "create", "retry").await?;
    for op in 5..=7 {
        fire(&runtime, "retry", op).await?;
    }
    state(&runtime, "retry", State::Complete).await?;
    assert_eq!(*signals.retries.lock().unwrap(), [1, 2, 3]);
    assert_eq!(*signals.delays.lock().unwrap(), [1, 2]);
    call(&runtime, "create", "configured").await?;
    for op in 30..=32 {
        fire(&runtime, "configured", op).await?;
    }
    state(&runtime, "configured", State::Complete).await?;
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
    assert!(gate_tickets
        .lock()
        .unwrap()
        .iter()
        .any(|(cell, ticket)| cell == &scope("main")
            && ticket.channel == cellar_logic::Channel::CellRpc
            && ticket.position.is_some()
            && ticket.epoch == Some(1)));
    // A deployment change must not rewrite a completed execution's diagram.
    runtime.swap_out_cell(&scope("main"), 1).await?;
    let mut next_graph = graph();
    next_graph.primary.options.workflow_bindings[0]
        .settings
        .definition
        .as_mut()
        .unwrap()
        .version = "orders-v2".into();
    let next = Generation::build_native(
        2,
        next_graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(SCRIPT.into(), registry.clone())]),
    )?;
    runtime.adopt(next);
    runtime.start_cell(scope("main"), 1, true).await?;
    runtime.publish_cell(&scope("main"), 1)?;
    let observed = runtime
        .fetch_cell(
            scope("main"),
            None,
            console_request(
                json!({"binding":"WF","instance":"main","command":{"operation":"status"}}),
            ),
            None,
        )
        .await?;
    let observed: Value = serde_json::from_slice(&observed.body)?;
    assert_eq!(
        observed["result"]["definitionSnapshot"]["definition"]["version"],
        "orders-v1"
    );
    let restarted = runtime.fetch_cell(scope("main"),None,console_request(json!({"binding":"WF","instance":"main","command":{"operation":"restart"},"expected_generation":observed["result"]["generation"]})),None).await?;
    assert_eq!(restarted.status, 200);
    let current = runtime
        .fetch_cell(
            scope("main"),
            None,
            console_request(
                json!({"binding":"WF","instance":"main","command":{"operation":"status"}}),
            ),
            None,
        )
        .await?;
    let current: Value = serde_json::from_slice(&current.body)?;
    assert_eq!(
        current["result"]["definitionSnapshot"]["definition"]["version"],
        "orders-v2"
    );
    let mut cursor = 0;
    let mut versions = std::collections::BTreeSet::new();
    let mut recorded_input = false;
    let mut recorded_output = false;
    loop {
        let reply = runtime.fetch_cell(scope("main"),None,console_request(json!({"binding":"WF","instance":"main","command":{"operation":"history","cursor":cursor}})),None).await?;
        assert_eq!(reply.status, 200);
        let page: Value = serde_json::from_slice(&reply.body)?;
        for snapshot in page["result"]["definitions"].as_object().unwrap().values() {
            versions.insert(
                snapshot["definition"]["version"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
        let events = page["result"]["events"].as_array().unwrap();
        for event in events {
            assert!(event["generation"].is_string());
            if event["stepName"] == "first" {
                recorded_input |= event["input"]["encoding"] == "cellar_structured_clone";
                recorded_output |= event["output"]["encoding"] == "cellar_structured_clone";
            }
        }
        let Some(last) = events.last() else {
            break;
        };
        cursor = last["eventId"].as_u64().unwrap();
    }
    assert_eq!(
        versions,
        std::collections::BTreeSet::from(["orders-v1".to_string(), "orders-v2".to_string()])
    );
    assert!(
        recorded_input && recorded_output,
        "structured values must not hide event metadata"
    );
    shutdown.send(true)?;
    router.await??;
    adapter.await?;
    for id in [
        "main",
        "rollback",
        "rollback-failed",
        "rollback-held",
        "bad-delay",
        "terminate-rollback",
        "stream",
        "stream-retry",
        "stream-rollback",
        "stream-quota",
        "sensitive",
        "batch-existing",
        "batch-new",
        "retry",
        "configured",
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
    let extras = signals.extra_cells.lock().unwrap().clone();
    for id in extras {
        runtime.swap_out_cell(&scope(&id), 1).await?;
    }
    runtime.swap_out_cell(&sink_scope(), 1).await?;
    Ok(())
}
