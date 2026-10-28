// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real native broker/consumer turns, with routing and proof adapters. No JS
//! engine initialization; persisted SQLite and local wake-entry storage.

#[path = "support/system_schema.rs"]
mod system_schema;

use anyhow::{anyhow, Result};
use cellar::application::queue::{self, BatchEntry, Body, Event, SendOptions};
use cellar::application::{decode_rpc_result, encode_rpc, Context, Invocation, Output, Registry};
use cellar::engine_api::{
    Compat, HttpResponse, QueueBatch, QueueBinding, QueueContentType, QueueMessage, QueueMetrics,
    QueueOutcome, RuntimeFetch, RuntimeOptions, WorkerConfigOptions,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::host_channels::QueueDispatchReq;
use cellar::native::clone::{Object, StructuredValue, Value as CloneValue};
use cellar::runtime::RuntimeManager;
use futures_util::{FutureExt, StreamExt};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

const Q: &str = "native-main";
const DLQ: &str = "native-dead";
const GROUP: &str = "native-group";

#[derive(Default)]
struct Signals {
    mode: AtomicUsize,
    event: Mutex<Option<Event>>,
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-queue".into(),
            do_classes: vec![cellar::deploy::QUEUE_CLASS.into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: vec![
                QueueBinding {
                    environment: "Q".into(),
                    queue: Q.into(),
                    delivery_delay: 0,
                },
                QueueBinding {
                    environment: "GROUP".into(),
                    queue: GROUP.into(),
                    delivery_delay: 0,
                },
            ],
            queue_consumers: vec![cellar::protocol::QueueConsumerConfig {
                queue: Q.into(),
                max_batch_size: 2,
                max_batch_timeout: 0,
                max_retries: 1,
                dead_letter_queue: Some(DLQ.into()),
                max_concurrency: Some(2),
                retry_delay: Some(1),
            }],
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "native-queue".into(),
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

fn response(value: Value) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: serde_json::to_vec(&value).unwrap(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

fn cycle() -> StructuredValue {
    StructuredValue {
        root: CloneValue::Reference(0),
        objects: vec![Object::Object(vec![
            (CloneValue::from("self"), CloneValue::Reference(0)),
            (CloneValue::from("text"), CloneValue::String(vec![0xd800])),
        ])],
    }
}

async fn worker(context: Context, invocation: Invocation, signals: Arc<Signals>) -> Result<Output> {
    match invocation {
        Invocation::Queue(batch) => {
            queue::dispatch(batch, move |event| async move {
                *signals.event.lock().unwrap() = Some(event.clone());
                assert_eq!(event.queue(), Q);
                assert_eq!(event.messages().len(), 2.min(event.messages().len()));
                match signals.mode.load(Ordering::SeqCst) {
                    1 => {
                        event.messages()[0].ack()?;
                        if event.messages().len() > 1 {
                            event.messages()[1].retry(Some(0))?;
                            event.messages()[1].ack()?;
                            event.messages()[1].retry(None)?;
                        }
                        return Err(anyhow!("consumer error after explicit decisions"));
                    }
                    2 => {
                        event.ack_all()?;
                        event.retry_all(Some(0))?;
                        return Err(anyhow!("ack-all survives exception"));
                    }
                    3 => {
                        event.retry_all(Some(0))?;
                        event.ack_all()?;
                        return Err(anyhow!("spend final retry"));
                    }
                    4 => {
                        event.messages()[0].ack()?;
                        panic!("native consumer panic");
                    }
                    _ => event.ack_all()?,
                }
                Ok(())
            })
            .await
        }
        Invocation::Fetch(request) => {
            let producer = context.queue("Q")?;
            assert!(context.queue("missing").is_err());
            if request.url.ends_with("/preview-budget") {
                let queue = context.queue("GROUP")?;
                for _ in 0..6 {
                    queue
                        .send(
                            &context,
                            Body::Bytes(vec![0xff; 128_000]),
                            Default::default(),
                        )
                        .await?;
                }
                return Ok(response(json!("large messages queued")));
            }
            if request.url.ends_with("/group") {
                let producer = context.queue("GROUP")?;
                let results = futures_util::future::join_all((0..64).map(|index| {
                    producer.send(
                        &context,
                        Body::Text(format!("group-{index}")),
                        Default::default(),
                    )
                }))
                .await;
                let counts = results
                    .into_iter()
                    .map(|result| Ok(result?.metrics.backlog_count as i64))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(response(json!(counts)));
            }
            if request.url.ends_with("/wake-failure") {
                let error = context
                    .queue("GROUP")?
                    .send(
                        &context,
                        Body::Text("committed but unacknowledged".into()),
                        Default::default(),
                    )
                    .await
                    .err()
                    .unwrap();
                assert!(error.to_string().contains("wake-entry gate"));
                return Ok(response(json!("wake failure checked")));
            }
            if request.url.ends_with("/two") {
                producer
                    .send_batch(
                        &context,
                        [
                            BatchEntry {
                                body: Body::Text("one".into()),
                                delay_seconds: None,
                            },
                            BatchEntry {
                                body: Body::Text("two".into()),
                                delay_seconds: None,
                            },
                        ],
                        Default::default(),
                    )
                    .await?;
                return Ok(response(json!("two checked")));
            }
            let metrics = producer
                .send_batch(
                    &context,
                    [
                        BatchEntry {
                            body: Body::Text("ack".into()),
                            delay_seconds: None,
                        },
                        BatchEntry {
                            body: Body::Bytes(vec![255, 1]),
                            delay_seconds: None,
                        },
                        BatchEntry {
                            body: Body::Json(json!({"n":3})),
                            delay_seconds: None,
                        },
                        BatchEntry {
                            body: Body::Structured(cycle()),
                            delay_seconds: None,
                        },
                    ],
                    Default::default(),
                )
                .await?
                .metrics;
            assert_eq!(metrics.backlog_count, 4.0);
            assert!(metrics.backlog_bytes > 10.0);
            assert!(metrics.oldest_message_timestamp_ms.is_some());
            assert_eq!(producer.metrics(&context).await?.backlog_count, 4.0);
            for (body, options) in [
                (Body::Bytes(vec![0; 128001]), SendOptions::default()),
                (
                    Body::Text("delay".into()),
                    SendOptions {
                        delay_seconds: Some(86401),
                    },
                ),
            ] {
                assert!(producer.send(&context, body, options).await.is_err());
            }
            assert!(producer
                .send_batch(&context, [], Default::default())
                .await
                .is_err());
            assert!(producer
                .send_batch(
                    &context,
                    (0..101).map(|_| BatchEntry {
                        body: Body::Text("x".into()),
                        delay_seconds: None
                    }),
                    Default::default()
                )
                .await
                .is_err());
            assert!(producer
                .send_batch(
                    &context,
                    (0..3).map(|_| BatchEntry {
                        body: Body::Bytes(vec![0; 100000]),
                        delay_seconds: None
                    }),
                    Default::default()
                )
                .await
                .is_err());
            assert_eq!(producer.metrics(&context).await?.backlog_count, 4.0);
            Ok(response(json!("binding checked")))
        }
        _ => Err(anyhow!("unexpected native Queue event")),
    }
}

fn scope(queue: &str) -> String {
    cellar::engine_api::queue_cell_scope(queue)
}

fn request(path: &str, body: Vec<u8>) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://cell/{path}"),
        method: "POST".into(),
        body: body.into(),
        headers: Vec::new(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}

async fn operator(runtime: &RuntimeManager, queue: &str, body: Value) -> Result<Value> {
    let response = runtime
        .fetch_cell(
            scope(queue),
            Some(queue.into()),
            request("", serde_json::to_vec(&body)?),
            None,
        )
        .await?;
    assert_eq!(
        response.status,
        200,
        "{}",
        String::from_utf8_lossy(&response.body)
    );
    Ok(serde_json::from_slice::<Value>(&response.body)?["result"].clone())
}

async fn open(runtime: &RuntimeManager, queue: &str) -> Result<()> {
    let scope = scope(queue);
    runtime
        .restore_cell(
            &scope,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(scope.clone(), 1, true).await?;
    runtime.publish_cell(&scope, 1)
}

async fn fire(runtime: &RuntimeManager, queue: &str, op: u64) -> Result<()> {
    let scope = scope(queue);
    let deadline = runtime
        .alarm(&scope)
        .ok_or_else(|| anyhow!("Queue did not arm"))?;
    let current = cellar::ownership_store::now_ms() as i64;
    if deadline >= current {
        ensure_short(deadline - current)?;
        cellar::asyncrt::sleep(Duration::from_millis((deadline - current + 5) as u64)).await;
    }
    runtime.fire_alarm(op, scope, deadline).await?;
    Ok(())
}

fn ensure_short(ms: i64) -> Result<()> {
    anyhow::ensure!(ms < 3000, "unexpected distant Queue deadline {ms}");
    Ok(())
}

async fn call_worker(runtime: &RuntimeManager, path: &str) -> Result<Value> {
    let response = runtime
        .fetch_worker(
            format!("http://worker/{path}"),
            "GET".into(),
            Vec::new(),
            Vec::new(),
        )
        .await?;
    Ok(serde_json::from_slice(&response.body)?)
}

fn settlement(call: &QueueDispatchReq, result: cellar::engine_api::QueueDispatchResult) -> Value {
    json!({"leaseId":call.lease_id,"leases":call.leases,"outcome":if result.outcome==QueueOutcome::Ok{"ok"}else{"exception"},"ackAll":result.ack_all,"retryBatch":{"retry":result.retry_batch.retry,"delaySeconds":result.retry_batch.delay_seconds},"explicitAcks":result.explicit_acks,"retryMessages":result.retry_messages.into_iter().map(|retry|json!({"msgId":retry.msg_id,"delaySeconds":retry.delay_seconds})).collect::<Vec<_>>()})
}

async fn settle(runtime: &RuntimeManager, queue: &str, body: Value) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            scope(queue),
            None,
            request("__qSettle", serde_json::to_vec(&body)?),
            None,
        )
        .await
}

fn database(directory: &std::path::Path, queue: &str) -> std::path::PathBuf {
    directory.join(scope(queue)).join("ltx/e1/db.sqlite")
}

fn dummy() -> QueueBatch {
    QueueBatch {
        queue: Q.into(),
        messages: vec![QueueMessage {
            id: "test".into(),
            timestamp_ms: 1,
            body: vec![1],
            content_type: QueueContentType::Bytes,
            attempts: 1,
        }],
        metrics: QueueMetrics {
            backlog_count: 0.0,
            backlog_bytes: 0.0,
            oldest_message_timestamp_ms: None,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_queue_preserves_grouping_leases_settlement_and_transfers() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(45), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let captured = signals.clone();
    let registry = Registry::new(move |context, invocation| {
        Box::pin(worker(context, invocation, captured.clone()))
    })
    .cell(cellar::deploy::QUEUE_CLASS, |_, _| {
        Err(anyhow!("runtime must supply Queue factory"))
    })?;
    let generation = Generation::build_native(
        1,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-queue".into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let data = directory.path().join("cells");
    let bucket_path = directory.path().join("bucket.sqlite");
    let bucket = cellar::bucket::Bucket::open_dev(&bucket_path)?;
    let flusher = Arc::new(cellar::wake::WakeFlusher::new());
    cellar::wake_entry::set_arm_gate(cellar::wake_entry::ArmGate {
        bucket: bucket.clone(),
        flusher: flusher.clone(),
    });
    let runtime = RuntimeManager::start(
        generation,
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
    for queue in [Q, DLQ, GROUP] {
        open(&runtime, queue).await?;
    }
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(tx);
    let host = runtime.clone();
    let router = cellar::asyncrt::spawn(async move {
        let mut pending = futures_util::stream::FuturesUnordered::new();
        loop {
            cellar::asyncrt::select_biased! {"Queue routing adapter admits concurrent producer events";
                _=stop.changed()=>break,
                call=calls.recv()=>{let Some(call)=call else{break};let host=host.clone();pending.push(async move {let result=host.rpc(call.scope,call.name,call.method,call.args,Some(cellar::engine_api::next_request_id())).await.map(|reply|reply.data);let _=call.reply.send(result);});},
                _=pending.next(),if !pending.is_empty()=>{},
            }
        }
    });
    let (dispatch, mut batches) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_queue_dispatch_tx(dispatch);
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let mut stop = shutdown.subscribe();
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let sampled = tickets.clone();
    let proofs = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"Queue proof adapter stops";_=stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            sampled.lock().unwrap().push(gate.ticket);
            let _ = gate.reply.send(Ok(()));
        }
    });
    let refused = runtime
        .queue_service(runtime.next_generation_id(), "native-queue", dummy())
        .await;
    anyhow::ensure!(
        refused.is_err() && signals.event.lock().unwrap().is_none(),
        "an unknown generation entered the current Queue consumer instead of refusing before effects"
    );
    assert_eq!(call_worker(&runtime, "basic").await?, "binding checked");
    let peek = operator(&runtime, Q, json!({"op":"peek"})).await?;
    assert_eq!(peek["messages"].as_array().unwrap().len(), 4);
    let bounded = operator(&runtime, Q, json!({"op":"peek","console":true,"limit":100})).await?;
    assert_eq!(bounded["messages"], peek["messages"]);
    assert_eq!(bounded["truncated"], false);
    assert!(bounded["observedAt"].is_number());
    assert_eq!(bounded["messages"][0]["deliveryAttempts"], 0);
    assert_eq!(bounded["messages"][0]["lease"]["active"], false);
    let ids = peek["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(ids.iter().all(|id| id.len() == 36 && &id[14..15] == "7"));
    fire(&runtime, Q, 1).await?;
    let mut first = batches.recv().await.unwrap();
    let mut second = batches.recv().await.unwrap();
    assert!(batches.try_recv().is_err());
    assert_eq!(first.generation, 1);
    assert_eq!(first.script, "native-queue");
    assert_eq!(first.batch.messages.len(), 2);
    assert_eq!(second.batch.messages.len(), 2);
    assert_eq!(first.batch.metrics.backlog_count, 2.0);
    assert_eq!(second.batch.metrics.backlog_count, 0.0);
    let leased = operator(&runtime, Q, json!({"op":"peek","console":true})).await?;
    assert!(leased["messages"]
        .as_array()
        .unwrap()
        .iter()
        .all(|m| m["deliveryAttempts"] == 1
            && m["lease"]["active"] == true
            && m["lease"]["generation"] == 1
            && m["lease"]["expiresAt"].is_number()));
    let moved = std::mem::replace(&mut first.batch, dummy());
    signals.mode.store(1, Ordering::SeqCst);
    let result = runtime
        .queue_service(first.generation, &first.script, moved)
        .await?;
    assert_eq!(result.outcome, QueueOutcome::Exception);
    assert_eq!(result.explicit_acks.len(), 1);
    assert_eq!(result.retry_messages[0].delay_seconds, Some(0));
    let old = settlement(&first, result);
    assert_eq!(settle(&runtime, Q, old.clone()).await?.status, 204);
    let saved = signals.event.lock().unwrap().take().unwrap();
    assert!(saved.ack_all().is_err());
    assert!(saved.messages()[0].ack().is_err());
    assert!(saved.messages()[1].retry(None).is_err());
    let moved = std::mem::replace(&mut second.batch, dummy());
    match &moved.messages[0].content_type {
        QueueContentType::Json => assert_eq!(
            serde_json::from_slice::<Value>(&moved.messages[0].body)?,
            json!({"n":3})
        ),
        _ => panic!("JSON body changed"),
    };
    assert_eq!(
        cellar::native::clone::decode(&moved.messages[1].body)?,
        cycle()
    );
    signals.mode.store(2, Ordering::SeqCst);
    let result = runtime
        .queue_service(second.generation, &second.script, moved)
        .await?;
    assert!(result.ack_all);
    assert!(!result.retry_batch.retry);
    assert_eq!(
        settle(&runtime, Q, settlement(&second, result))
            .await?
            .status,
        204
    );
    assert_eq!(
        operator(&runtime, Q, json!({"op":"info"})).await?["backlogCount"],
        1
    );
    assert_eq!(settle(&runtime, Q, old).await?.status, 409);
    fire(&runtime, Q, 2).await?;
    let mut retry = batches.recv().await.unwrap();
    assert_eq!(retry.batch.messages[0].attempts, 2);
    signals.mode.store(3, Ordering::SeqCst);
    let moved = std::mem::replace(&mut retry.batch, dummy());
    let result = runtime
        .queue_service(retry.generation, &retry.script, moved)
        .await?;
    assert!(!result.ack_all);
    assert!(result.retry_batch.retry);
    assert_eq!(
        settle(&runtime, Q, settlement(&retry, result))
            .await?
            .status,
        204
    );
    assert_eq!(
        operator(&runtime, Q, json!({"op":"info"})).await?["stored"],
        0
    );
    let dead = operator(&runtime, DLQ, json!({"op":"peek"})).await?;
    assert_eq!(dead["messages"][0]["deadLetterSource"], Q);
    assert_eq!(dead["messages"][0]["provenance"]["sourceQueue"], Q);
    assert!(dead["messages"][0]["provenance"]["transferId"].is_string());
    assert_eq!(dead["messages"][0]["deliveryAttempts"], 0);
    assert_eq!(dead["messages"][0]["attempts"], 0);
    operator(&runtime, Q, json!({"op":"pause"})).await?;
    assert_eq!(
        operator(&runtime, DLQ, json!({"op":"redrive"})).await?["redriven"],
        1
    );
    let redriven = operator(&runtime, Q, json!({"op":"peek"})).await?;
    assert_eq!(redriven["messages"][0]["attempts"], 0);
    assert!(redriven["messages"][0]["deadLetterSource"].is_null());
    assert_eq!(
        operator(&runtime, DLQ, json!({"op":"info"})).await?["stored"],
        0
    );
    // Target receipts deduplicate even after redrive deletes the target row.
    runtime.swap_out_cell(&scope(DLQ), 1).await?;
    system_schema::assert_system_tables(
        &database(&data, DLQ),
        &[
            "__sys_queue_messages",
            "__sys_queue_meta",
            "__sys_queue_stats",
            "__sys_queue_transfer_receipts",
        ],
    )?;
    let db = rusqlite::Connection::open(database(&data, DLQ))?;
    let receipt: (String, String, String) = db.query_row(
        "SELECT transfer_id,message_id,dead_letter_source FROM __sys_queue_transfer_receipts",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    drop(db);
    runtime.start_cell(scope(DLQ), 1, false).await?;
    runtime.publish_cell(&scope(DLQ), 1)?;
    let transfer = json!([{ "transfers":[{"transferId":receipt.0,"deadLetterSource":receipt.2,"entry":{"id":receipt.1,"body":[255,1],"contentType":"bytes","size":2}}] }]);
    let replay = runtime
        .rpc(
            scope(DLQ),
            None,
            "__queueDlqAcceptBatch".into(),
            encode_rpc(&StructuredValue::from_json(transfer.clone())?)?,
            None,
        )
        .await?;
    assert_eq!(
        decode_rpc_result(&replay.data)?.to_json()?["backlogCount"],
        0
    );
    let mut collision = transfer;
    collision[0]["transfers"][0]["entry"]["id"] = json!("different-id");
    let reply = runtime
        .rpc(
            scope(DLQ),
            None,
            "__queueDlqAcceptBatch".into(),
            encode_rpc(&StructuredValue::from_json(collision)?)?,
            None,
        )
        .await?;
    assert!(decode_rpc_result(&reply.data)
        .err()
        .unwrap()
        .to_string()
        .contains("collided"));
    operator(&runtime, Q, json!({"op":"resume"})).await?;
    signals.mode.store(0, Ordering::SeqCst);
    fire(&runtime, Q, 3).await?;
    let mut call = batches.recv().await.unwrap();
    let moved = std::mem::replace(&mut call.batch, dummy());
    let result = runtime
        .queue_service(call.generation, &call.script, moved)
        .await?;
    settle(&runtime, Q, settlement(&call, result)).await?;
    // A purge hides live leases and removes them on settlement, even on retry.
    call_worker(&runtime, "two").await?;
    fire(&runtime, Q, 4).await?;
    let mut call = batches.recv().await.unwrap();
    let purge = operator(&runtime, Q, json!({"op":"purge"})).await?;
    assert_eq!(purge["deleted"], 0);
    assert_eq!(purge["leased"], 2);
    assert_eq!(purge["metrics"]["backlogCount"], 0);
    assert_eq!(
        operator(&runtime, Q, json!({"op":"peek"})).await?["messages"][0]["state"],
        "purging"
    );
    signals.mode.store(3, Ordering::SeqCst);
    let moved = std::mem::replace(&mut call.batch, dummy());
    let result = runtime
        .queue_service(call.generation, &call.script, moved)
        .await?;
    settle(&runtime, Q, settlement(&call, result)).await?;
    assert_eq!(
        operator(&runtime, Q, json!({"op":"info"})).await?["stored"],
        0
    );
    // Reopen an expired lease fixture, then prove that the old generation is
    // fenced and configured retry delay is applied once before redelivery.
    call_worker(&runtime, "two").await?;
    fire(&runtime, Q, 5).await?;
    let expired = batches.recv().await.unwrap();
    let stale =
        json!({"leaseId":expired.lease_id,"leases":expired.leases,"outcome":"ok","ackAll":true});
    runtime.swap_out_cell(&scope(Q), 1).await?;
    let db = rusqlite::Connection::open(database(&data, Q))?;
    db.execute(
        "UPDATE __sys_queue_messages SET leased_until=?1",
        [cellar::ownership_store::now_ms() as i64 - 1],
    )?;
    drop(db);
    runtime.start_cell(scope(Q), 1, false).await?;
    runtime.publish_cell(&scope(Q), 1)?;
    let expired_snapshot = operator(&runtime, Q, json!({"op":"peek","console":true})).await?;
    assert_eq!(expired_snapshot["messages"][0]["lease"]["active"], false);
    assert!(expired_snapshot["messages"][0]["lease"]["expiresAt"].is_number());
    assert_eq!(expired_snapshot["messages"][0]["deliveryAttempts"], 1);
    operator(&runtime, Q, json!({"op":"resume"})).await?;
    fire(&runtime, Q, 6).await?;
    assert!(batches.try_recv().is_err());
    assert_eq!(settle(&runtime, Q, stale).await?.status, 409);
    assert_eq!(
        operator(&runtime, Q, json!({"op":"peek"})).await?["messages"][0]["attempts"],
        1
    );
    fire(&runtime, Q, 7).await?;
    let mut call = batches.recv().await.unwrap();
    assert_eq!(call.batch.messages[0].attempts, 2);
    assert_eq!(call.leases[0].generation, 2);
    signals.mode.store(0, Ordering::SeqCst);
    let moved = std::mem::replace(&mut call.batch, dummy());
    let result = runtime
        .queue_service(call.generation, &call.script, moved)
        .await?;
    settle(&runtime, Q, settlement(&call, result)).await?;
    assert_eq!(
        call_worker(&runtime, "preview-budget").await?,
        "large messages queued"
    );
    let preview = operator(
        &runtime,
        GROUP,
        json!({"op":"peek","console":true,"limit":100}),
    )
    .await?;
    assert_eq!(preview["truncated"], true);
    let messages = preview["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3);
    assert!(messages.iter().all(|message| message["size"] == 128_000));
    // The limit applies before loading bodies and includes base64 expansion
    // within the web transport's response cap. Preview never consumes messages.
    assert!(serde_json::to_vec(&preview)?.len() < 1024 * 1024);
    assert_eq!(
        operator(&runtime, GROUP, json!({"op":"info"})).await?["backlogCount"],
        6
    );
    operator(&runtime, GROUP, json!({"op":"purge"})).await?;
    let grouped = call_worker(&runtime, "group").await?;
    let counts = grouped
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_i64().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert!(counts.len() < 64, "independent sends must coalesce");
    assert!(counts.contains(&64));
    assert_eq!(
        operator(&runtime, GROUP, json!({"op":"info"})).await?["stored"],
        64
    );
    assert!(
        !bucket.list("").await?.is_empty(),
        "producer acknowledgement must publish a wake entry"
    );
    operator(&runtime, GROUP, json!({"op":"purge"})).await?;
    let db = rusqlite::Connection::open(&bucket_path)?;
    db.execute_batch("CREATE TRIGGER refuse_wake BEFORE INSERT ON __sys_local_objects BEGIN SELECT RAISE(FAIL,'injected wake PUT failure'); END;")?;
    assert_eq!(
        call_worker(&runtime, "wake-failure").await?,
        "wake failure checked"
    );
    db.execute_batch("DROP TRIGGER refuse_wake")?;
    assert_eq!(
        operator(&runtime, GROUP, json!({"op":"info"})).await?["stored"],
        1,
        "failed discovery acknowledgement must retain its committed message"
    );
    // Time fixtures are installed only while the cell is retired. Visibility
    // must expire immediately, before bounded alarm reclamation finishes.
    runtime.swap_out_cell(&scope(GROUP), 1).await?;
    {
        let mut db = rusqlite::Connection::open(database(&data, GROUP))?;
        let tx = db.transaction()?;
        tx.execute("DELETE FROM __sys_queue_messages", [])?;
        let expired_at =
            cellar::ownership_store::now_ms() as i64 - cellar_logic::queue::RETENTION_MS - 1;
        for index in 0..259 {
            tx.execute("INSERT INTO __sys_queue_messages(id,body,content_type,size,enqueued_at,visible_at) VALUES (?,x'78','text',1,?,?)",rusqlite::params![format!("expired-{index}"),expired_at,expired_at])?;
        }
        for index in 0..8 {
            tx.execute("INSERT INTO __sys_queue_transfer_receipts(transfer_id,message_id,dead_letter_source,expires_at) VALUES (?,?,NULL,?)",rusqlite::params![format!("receipt-{index}"),format!("accepted-{index}"),expired_at])?;
        }
        tx.commit()?;
    }
    runtime.start_cell(scope(GROUP), 1, false).await?;
    runtime.publish_cell(&scope(GROUP), 1)?;
    let info = operator(&runtime, GROUP, json!({"op":"info"})).await?;
    assert_eq!(info["stored"], 259);
    assert_eq!(info["backlogCount"], 0);
    operator(&runtime, GROUP, json!({"op":"resume"})).await?;
    fire(&runtime, GROUP, 8).await?;
    assert_eq!(
        operator(&runtime, GROUP, json!({"op":"info"})).await?["stored"],
        3
    );
    fire(&runtime, GROUP, 9).await?;
    assert_eq!(
        operator(&runtime, GROUP, json!({"op":"info"})).await?["stored"],
        0
    );
    assert!(runtime.alarm(&scope(GROUP)).is_none());
    assert!(tickets
        .lock()
        .unwrap()
        .iter()
        .any(|ticket| ticket.channel == cellar_logic::Channel::Queue
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    // The consumer helper drops authority even when its future never settles.
    let saved = Arc::new(Mutex::new(None));
    let captured = saved.clone();
    let mut future = queue::dispatch(dummy(), move |event| {
        *captured.lock().unwrap() = Some(event);
        futures_util::future::pending::<Result<()>>()
    });
    assert!(future.as_mut().now_or_never().is_none());
    drop(future);
    assert!(saved.lock().unwrap().as_ref().unwrap().messages()[0]
        .ack()
        .is_err());
    let saved = Arc::new(Mutex::new(None));
    let captured = saved.clone();
    let result = queue::dispatch(dummy(), move |event| async move {
        event.messages()[0].ack()?;
        *captured.lock().unwrap() = Some(event);
        panic!("after ack")
    })
    .await?;
    let Output::Queue(result) = result else {
        panic!("expected Queue result")
    };
    assert_eq!(result.outcome, QueueOutcome::Exception);
    assert_eq!(result.explicit_acks, ["test"]);
    assert!(saved
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .retry_all(None)
        .is_err());
    let mut incoming = dummy();
    incoming.messages[0].content_type = QueueContentType::Json;
    incoming.messages[0].body = br#"[-0,1e400,"\ud800"]"#.to_vec();
    let result=queue::dispatch(incoming,|event|async move {
        let Body::JsonValue(value)=&event.messages()[0].body else {panic!("JSON must retain its UTF-16 and binary64 graph")};
        let CloneValue::Reference(root)=value.root else {panic!("array")};
        let Object::DenseArray {elements,..}=&value.objects[root as usize] else {panic!("array")};
        assert!(matches!(elements[0],CloneValue::Number(number) if number==0.0 && number.is_sign_negative()));
        assert!(matches!(elements[1],CloneValue::Number(number) if number==f64::INFINITY));
        assert_eq!(elements[2],CloneValue::String(vec![0xd800]));
        event.ack_all()?;Ok(())
    }).await?;
    assert!(
        matches!(result,Output::Queue(result) if result.outcome==QueueOutcome::Ok && result.ack_all)
    );
    for queue in [Q, DLQ, GROUP] {
        runtime.swap_out_cell(&scope(queue), 1).await?;
    }
    shutdown.send(true)?;
    router.await?;
    proofs.await?;
    Ok(())
}
