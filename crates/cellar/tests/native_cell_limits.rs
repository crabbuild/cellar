// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Lifted API ceilings exercised through production native turns and storage.
#![allow(clippy::disallowed_methods)]
#[path = "support/html.rs"]
mod fixture;
#[path = "support/cell_limits_observation.rs"]
mod observation;

use anyhow::{anyhow, ensure, Context as _, Result};
use cellar::application::{blob, kv, queue, workflow};
use cellar::engine_api::{HttpResponse, QueueBinding, RuntimeFetch, WorkflowBinding};
use cellar::generation::Generation;
use cellar::runtime::RuntimeManager;
use cellar::sdk::{
    Cell, CellLimits, Context, ExecutionDomain, HandlerFuture, Invocation, Output, Registry,
};
use cellar::storage::{SqlRequest, SqlResponse};
use cellar_native::clone::StructuredValue;
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};

const SCRIPT: &str = "lifted";
const QUEUE: &str = "lifted-queue";
const ID: &str = "lifted-kv";

#[test]
fn lifted_queue_consumer_settings_keep_final_attempt_representable() -> Result<()> {
    use cellar_logic::queue::{
        expire_lease, validate_config_with_limits, ExpiredLeaseAction, Limits, QueueConfig,
    };
    let config = QueueConfig {
        max_batch_size: 101,
        max_batch_timeout_seconds: 61,
        max_retries: 101,
        max_concurrency: Some(251),
        delivery_delay_seconds: 100_000,
        retry_delay_seconds: Some(100_000),
    };
    assert!(validate_config_with_limits(&config, &Limits::default()).is_err());
    validate_config_with_limits(&config, &Limits::unrestricted())?;
    let final_attempt = expire_lease(1, u16::MAX - 1, u16::MAX - 1, None, false);
    assert_eq!(final_attempt.attempt, u16::MAX);
    assert_eq!(final_attempt.action, ExpiredLeaseAction::Exhausted);
    Ok(())
}

fn response() -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: b"checked".to_vec(),
        headers: vec![],
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://test/{path}"),
        method: "GET".into(),
        body: Vec::new().into(),
        headers: vec![],
        request_id: None,
        order: None,
        parent: None,
    }
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

struct SqlProbe {
    mode: u8,
}

impl Cell for SqlProbe {
    fn invoke(self: Arc<Self>, context: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            let scope = &context.identity().unwrap().scope;
            let prepared = |sql: &str, first: bool| {
                cellar::storage::sql_run(
                    scope,
                    &SqlRequest::Prepared {
                        sql: sql.into(),
                        params: vec![],
                        first,
                    },
                )
            };
            if self.mode == 2 {
                let SqlResponse::Prepared(result) = prepared("SELECT 1 UNION ALL SELECT 2", true)?
                else {
                    unreachable!()
                };
                assert_eq!(result.rows.len(), 1);
                assert!(prepared("SELECT 1 UNION ALL SELECT 2", false).is_err());
                return Ok(response());
            }
            let columns = format!(
                "SELECT {}",
                (0..101)
                    .map(|i| format!("{i} AS c{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let large_sql = format!("SELECT 1 /*{}*/", "x".repeat(100_001));
            if self.mode == 0 {
                assert!(storage.sql(&columns, &[]).is_err());
                assert!(storage.sql(&large_sql, &[]).is_err());
                assert!(storage.sql("SELECT zeroblob(3000000)", &[]).is_err());
                return Ok(response());
            }
            storage.sql(&columns, &[])?;
            storage.sql(&large_sql, &[])?;
            storage.sql("CREATE TABLE large(value BLOB)", &[])?;
            storage.sql("INSERT INTO large VALUES(zeroblob(3000000))", &[])?;
            let binds = vec![json!(1); 101];
            storage.sql(&format!("SELECT {}", vec!["?"; 101].join(",")), &binds)?;
            let SqlResponse::Prepared(rows) = prepared("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100001) SELECT x FROM n", false)? else { unreachable!() };
            assert_eq!(rows.rows.len(), 100_001);
            let SqlResponse::Prepared(wide) = prepared("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<9) SELECT printf('%.*c',4000000,'x') FROM n", false)? else { unreachable!() };
            assert_eq!(wide.rows.len(), 9);
            assert_eq!(wide.rows[0][0].as_str().unwrap().len(), 4_000_000);
            assert!(storage
                .sql("ATTACH DATABASE ':memory:' AS extra", &[])
                .is_err());
            assert!(storage.sql("SELECT * FROM __sys_cell_kv", &[]).is_err());
            Ok(response())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_limits_are_host_local_and_first_still_truncates() -> Result<()> {
    for mode in 0..3 {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let mut limits = if mode == 0 {
            CellLimits::default()
        } else {
            CellLimits::unrestricted()
        };
        if mode == 2 {
            limits.sql.max_rows = 1;
        }
        domain.services().configure_cell_limits(limits)?;
        domain
            .run(async {
                let directory = tempfile::tempdir()?;
                let app = Registry::new(|_, _| Box::pin(async { Ok(response()) }))
                    .cell("Probe", move |_, _| Ok(Arc::new(SqlProbe { mode })))?;
                let generation = Generation::build_native(
                    1,
                    fixture::graph("sql-limits", "", vec!["Probe".into()]),
                    fixture::generation_options(),
                    HashMap::from([(
                        "sql-limits".into(),
                        Arc::new(app) as Arc<dyn cellar::sdk::Application>,
                    )]),
                )?;
                let runtime = RuntimeManager::start(
                    generation,
                    fixture::runtime_options(directory.path().into()),
                )?;
                open(&runtime, "Probe:test").await?;
                assert_eq!(
                    runtime
                        .fetch_cell("Probe:test".into(), None, request("sql"), None)
                        .await?
                        .status,
                    200
                );
                runtime.swap_out_cell("Probe:test", 1).await?;
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        assert_eq!(domain.services().cell_limits(), limits);
        domain.shutdown().await;
    }
    Ok(())
}

#[tokio::test]
async fn policies_pin_defaults_validate_and_reject_live_reconfiguration() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let limits = CellLimits::unrestricted();
    limits.validate()?;
    first.services().configure_cell_limits(limits)?;
    first.services().configure_cell_limits(limits)?;
    assert!(first
        .services()
        .configure_cell_limits(CellLimits::default())
        .is_err());
    assert_eq!(second.services().cell_limits(), CellLimits::default());
    assert!(second.services().configure_cell_limits(limits).is_err());
    let mut invalid = limits;
    invalid.queue.max_retries = u16::MAX;
    assert!(invalid.validate().is_err());
    invalid = limits;
    invalid.kv.max_list_limit = i64::MAX as usize;
    assert!(invalid.validate().is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multipart_backlog_uses_the_host_budget() -> Result<()> {
    for unrestricted in [false, true] {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let mut limits = CellLimits::unrestricted();
        if !unrestricted {
            limits.blob.max_multipart_backlog_bytes = 3;
        }
        domain.services().configure_cell_limits(limits)?;
        domain
            .run(async {
                let directory = tempfile::tempdir()?;
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
                cellar::host_channels::set_blob_store(bucket);
                let registry = Registry::new(move |context, _| {
                    Box::pin(async move {
                        let bucket = context.blob("B")?;
                        let upload = bucket
                            .create_multipart(&context, "parts", Default::default())
                            .await?;
                        let two = upload.upload_part(&context, 2, vec![2; 4]).await;
                        if unrestricted {
                            let one = upload.upload_part(&context, 1, vec![1]).await?;
                            assert_eq!(upload.complete(&context, &[one, two?]).await?.size, 5);
                        } else {
                            assert!(two.unwrap_err().to_string().contains("waiting for part"));
                            upload.abort(&context).await?;
                        }
                        Ok(response())
                    })
                });
                let mut graph = fixture::graph("parts", "", vec![]);
                graph.primary.options.blob_bindings = vec![("B".into(), "parts".into())];
                let generation = Generation::build_native(
                    1,
                    graph,
                    fixture::generation_options(),
                    HashMap::from([(
                        "parts".into(),
                        Arc::new(registry) as Arc<dyn cellar::sdk::Application>,
                    )]),
                )?;
                let runtime = RuntimeManager::start(
                    generation,
                    fixture::runtime_options(directory.path().join("cells")),
                )?;
                assert_eq!(
                    runtime
                        .fetch_worker("http://test/parts".into(), "GET".into(), vec![], vec![])
                        .await?
                        .status,
                    200
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        domain.shutdown().await;
    }
    Ok(())
}

async fn data(
    context: Context,
    _: Invocation,
    observed: Arc<observation::Observation>,
) -> Result<Output> {
    observed.phase("kv_short_ttl")?;
    let namespace = context.kv("KV")?;
    namespace
        .put_text(
            &context,
            "short-ttl",
            "value",
            kv::PutOptions {
                expiration_ttl: Some(30.0),
                ..Default::default()
            },
        )
        .await?;
    let key = "k".repeat(600);
    let value = vec![7; 26 * 1024 * 1024];
    observed.phase("kv_large_put")?;
    namespace
        .put_bytes(
            &context,
            &key,
            value.clone(),
            kv::PutOptions {
                metadata: Some(json!({"large":"x".repeat(2048)})),
                ..Default::default()
            },
        )
        .await?;
    observed.phase("kv_large_get")?;
    let Some(kv::ReadValue::Bytes(read)) =
        namespace.get(&context, &key, kv::ReadType::Bytes).await?
    else {
        unreachable!()
    };
    assert_eq!(read, value);
    observed.phase("kv_bulk_get")?;
    let keys = (0..101).map(|i| format!("missing-{i}")).collect::<Vec<_>>();
    namespace
        .get_many(&context, &keys, kv::ReadType::Text)
        .await?;
    let entries = (0..1002).map(|i| json!({"key":format!("page-{i:04}"),"value":[1],"tag":"bytes","metadata":null,"expiresAt":null})).collect::<Vec<_>>();
    observed.phase("kv_seed_page")?;
    context
        .rpc_cell(
            cellar::engine_api::kv_cell_scope(ID, 0),
            Some(cellar_logic::kv::cell_name(ID, 0)),
            "__kvPut".into(),
            cellar::application::encode_rpc(&StructuredValue::from_json(
                json!([{ "entries":entries }]),
            )?)?,
        )
        .await?;
    observed.phase("kv_list_page")?;
    let page = namespace
        .list(
            &context,
            kv::ListOptions {
                prefix: "page-".into(),
                limit: Some(1001),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(page.keys.len(), 1001);
    assert!(!page.list_complete);
    observed.phase("queue_single")?;
    let producer = context.queue("Q")?;
    producer
        .send(
            &context,
            queue::Body::Bytes(vec![1; 150_000]),
            queue::SendOptions {
                delay_seconds: Some(100_000),
            },
        )
        .await?;
    observed.phase("queue_batch")?;
    producer
        .send_batch(
            &context,
            (0..101).map(|_| queue::BatchEntry {
                body: queue::Body::Bytes(vec![2; 3000]),
                delay_seconds: None,
            }),
            Default::default(),
        )
        .await?;
    observed.phase("queue_metrics")?;
    assert_eq!(producer.metrics(&context).await?.backlog_count, 102.0);
    let objects = context.blob("B")?;
    observed.phase("blob_seed_page")?;
    for i in 0..1002 {
        objects
            .put(
                &context,
                &format!("page-{i:04}"),
                vec![1],
                Default::default(),
            )
            .await?;
        observed.blob_put_completed();
    }
    observed.phase("blob_list_page")?;
    let page = objects
        .list(
            &context,
            blob::ListOptions {
                limit: Some(1001),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(page.objects.len(), 1001);
    assert!(page.truncated);
    let workflow = context.workflow("WF")?;
    observed.phase("workflow_large_create")?;
    let instance = workflow
        .create(
            &context,
            workflow::CreateOptions {
                id: Some("x".repeat(120)),
                params: Some(StructuredValue::from_json(json!("x".repeat(2_300_000)))?),
                retention: workflow::Retention {
                    success: Some(workflow::Duration::from("90 days")),
                    error: None,
                },
                ..Default::default()
            },
        )
        .await?;
    observed.phase("workflow_large_event")?;
    instance
        .send_event(
            &context,
            workflow::SendEvent {
                event_type: "e".repeat(120),
                payload: Some(StructuredValue::from_json(json!("x".repeat(2_300_000)))?),
            },
        )
        .await?;
    observed.phase("workflow_batch")?;
    workflow
        .create_batch(
            &context,
            (0..101)
                .map(|i| workflow::CreateOptions {
                    id: Some(format!("batch-{i}")),
                    ..Default::default()
                })
                .collect(),
        )
        .await?;
    Ok(response())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifted_bindings_persist_large_values_batches_and_pages() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_cell_limits(CellLimits::unrestricted())?;
    // Keep storage alive until the domain has joined, including a timeout.
    let directory = tempfile::tempdir()?;
    let observed = Arc::new(observation::Observation::new());
    let mut failure_observation = None;
    let result = domain.run(async {
        observed.phase("setup")?;
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
        cellar::host_channels::set_kv_blob_store(bucket.clone());
        cellar::host_channels::set_blob_store(bucket.clone());
        let flusher = Arc::new(cellar::wake::WakeFlusher::new());
        cellar::wake_entry::set_arm_gate(cellar::wake_entry::ArmGate { bucket, flusher: flusher.clone() });
        let data_observer = observed.clone();
        let registry = Registry::new(move |context, event| Box::pin(data(context, event, data_observer.clone())))
            .workflow("Flow", |_, event, steps| Box::pin(async move {
                if event.instance_id == "x".repeat(120) {
                    let output = steps.run_with("s".repeat(400), workflow::StepConfig {
                        retries: Some(workflow::Retries {
                            limit: 10001.0,
                            delay: workflow::Delay::Fixed(workflow::Duration::from("1 second")),
                            backoff: None,
                        }),
                        ..Default::default()
                    }, |_| async { StructuredValue::from_json(json!("x".repeat(2_300_000))) }).await?;
                    return Ok(output);
                }
                steps.sleep("long-sleep", "2 years").await?;
                Ok(StructuredValue::undefined())
            }))?;
        let wf_class = cellar::deploy::workflow_class(SCRIPT);
        let mut graph = fixture::graph(SCRIPT, "", vec![cellar::deploy::KV_CLASS.into(), cellar::deploy::QUEUE_CLASS.into(), wf_class.clone(), cellar_logic::cron::RESERVED_CLASS.into()]);
        graph.primary.options.kv_bindings = vec![("KV".into(), ID.into())];
        graph.primary.options.blob_bindings = vec![("B".into(), "lifted-objects".into())];
        graph.primary.options.queue_bindings = vec![QueueBinding { environment: "Q".into(), queue: QUEUE.into(), delivery_delay: 0 }];
        graph.primary.options.workflow_bindings = vec![WorkflowBinding { environment: "WF".into(), workflow: "flow".into(), class: "Flow".into(), settings: Default::default() }];
        graph.primary.crons = vec!["* * * * *".into(); 128];
        let generation = Generation::build_native(1, graph, fixture::generation_options(),
            HashMap::from([(SCRIPT.into(), Arc::new(registry) as Arc<dyn cellar::sdk::Application>)]))?;
        assert!(generation.cron_cell().is_some());
        let mut options = fixture::runtime_options(directory.path().join("cells"));
        options.wake = Some(flusher);
        let runtime = RuntimeManager::start(generation, options)?;
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
        cellar::host_channels::set_rpc_call_tx(tx);
        let target = runtime.clone();
        let router = cellar::asyncrt::spawn(async move {
            let mut opened = std::collections::HashSet::new();
            while let Some(call) = cellar::asyncrt::select_biased! { "test adapter stops"; _=stopped.changed()=>None, call=calls.recv()=>call, } {
                let result = async {
                    if opened.insert(call.scope.clone()) { open(&target, &call.scope).await?; }
                    Ok(target.rpc(call.scope, call.name, call.method, call.args, None).await?.data)
                }.await;
                let _ = call.reply.send(result);
            }
        });
        let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
        cellar::host_channels::set_gate_tx(tx);
        let mut stopped = stop.subscribe();
        let adapter = cellar::asyncrt::spawn(async move {
            while let Some(gate) = cellar::asyncrt::select_biased! { "test proof adapter stops"; _=stopped.changed()=>None, gate=gates.recv()=>gate, } {
                let _ = gate.reply.send(Ok(()));
            }
        });
        let result = cellar::asyncrt::timeout(Duration::from_secs(60), async {
            observed.phase("fetch_worker")?;
            let reply = runtime.fetch_worker("http://test/data".into(), "GET".into(), vec![], vec![]).await?;
            observed.phase("workflow_alarm_wait")?;
            ensure!(reply.status == 200 && reply.body == b"checked", "{}: {}", reply.status, String::from_utf8_lossy(&reply.body));
            let wf_scope = |id: &str| cellar::engine_api::workflow_cell_scope(SCRIPT, "flow", id);
            for (op, id) in [(1, "x".repeat(120)), (2, "batch-0".into())] {
                observed.phase(if op == 1 { "workflow_large_step" } else { "workflow_long_sleep" })?;
                let scope = wf_scope(&id);
                let deadline = cellar::asyncrt::timeout(Duration::from_secs(2), async {
                    loop { if let Some(at) = runtime.alarm(&scope) { break at; } tokio::task::yield_now().await; }
                }).await.map_err(|_| anyhow!("workflow wake was not published"))?;
                runtime.fire_alarm(op, scope.clone(), deadline).await?;
                let status = runtime.rpc(scope, None, "__wfStatus".into(), cellar::application::encode_rpc(&StructuredValue::array(vec![])?)?, None).await?;
                let status = cellar::application::decode_rpc_result(&status.data)?.to_json()?;
                assert_eq!(status["status"], if op == 1 { "complete" } else { "waiting" });
            }
            observed.phase("application_work_drain")?;
            while cellar::asyncrt::services().application_work().pending_events > 0 { tokio::task::yield_now().await; }
            assert!(runtime.alarm(&cellar::engine_api::queue_cell_scope(QUEUE)).is_none(), "unrestricted queue must not arm age-based deletion");
            observed.phase("complete")?;
            Ok::<_, anyhow::Error>(())
        }).await;
        let snapshot = observed.snapshot(Some(cellar::asyncrt::services().application_work().pending_events));
        eprintln!("cell-limit observations: {snapshot}");
        if !matches!(&result, Ok(Ok(()))) {
            failure_observation = Some(snapshot);
        }
        let _ = stop.send(true);
        router.await?;
        adapter.await?;
        result??;
        Ok::<_, anyhow::Error>(())
    }).await;
    domain.shutdown().await;
    let result = result
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    if let Err(error) = result {
        let evidence = failure_observation.unwrap_or_else(|| observed.snapshot(None));
        let saved = std::fs::write(
            directory.path().join("failure.json"),
            serde_json::to_vec_pretty(&evidence)?,
        );
        let retained = directory.keep();
        return Err(error).with_context(|| {
            format!(
                "cell-limit observations={evidence}; evidence_saved={saved:?}; retained fixture {}",
                retained.display()
            )
        });
    }
    Ok(())
}
