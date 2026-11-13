// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production native turns without Engine::init. Host adapters supply routing
//! and accept proof tickets; the local bucket exercises real blob persistence.

#[path = "support/system_schema.rs"]
mod system_schema;

#[path = "support/kv_contract.rs"]
mod kv_contract;

use anyhow::{anyhow, Result};
use cellar::application::kv::{ListOptions, Namespace, PutOptions, ReadType, ReadValue};
use cellar::application::{
    decode_rpc_result, encode_rpc, Cell, Context, HandlerFuture, Invocation, Output, Registry,
    Storage,
};
use cellar::engine_api::{
    Compat, HttpResponse, RpcData, RuntimeFetch, RuntimeOptions, WorkerConfigOptions,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

const IDENTITY: &str = "native-kv/namespace";

fn scope() -> String {
    cellar::engine_api::kv_cell_scope(IDENTITY, 0)
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-kv".into(),
            do_classes: vec![cellar::deploy::KV_CLASS.into(), "Caller".into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: vec![("KV".into(), IDENTITY.into())],
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "native-kv".into(),
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

fn response(body: impl Into<Vec<u8>>) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: body.into(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

fn text(value: Option<ReadValue>) -> String {
    match value {
        Some(ReadValue::Text(value)) => value,
        _ => panic!("expected text"),
    }
}

fn bytes(value: Option<ReadValue>) -> Vec<u8> {
    match value {
        Some(ReadValue::Bytes(value)) => value,
        _ => panic!("expected bytes"),
    }
}

struct Caller {
    namespace: Namespace,
    storage: Storage,
}

impl Cell for Caller {
    fn invoke(self: Arc<Self>, context: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.storage.put_json("before", &json!(true))?;
            Ok(response(text(
                self.namespace
                    .get(&context, "A/one", ReadType::Text)
                    .await?,
            )))
        })
    }
}

async fn binding(context: Context, invocation: Invocation) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        return Err(anyhow!("expected fetch"));
    };
    let namespace = context.kv("KV")?;
    assert!(context.kv("missing").is_err());
    if request.url.ends_with("/large") {
        kv_contract::large(&context, &namespace).await?;
        return Ok(response("large checked"));
    }
    if request.url.ends_with("/orphan") {
        assert!(namespace
            .put_bytes(
                &context,
                "never-committed",
                vec![99; cellar_logic::kv::MAX_INLINE_VALUE_BYTES + 1],
                Default::default()
            )
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("FAIL_AFTER_BLOB_WRITE"));
        assert!(namespace
            .get(&context, "never-committed", ReadType::Bytes)
            .await?
            .is_none());
        return Ok(response("orphan checked"));
    }
    if request.url.ends_with("/restore") {
        assert_eq!(
            bytes(namespace.get(&context, "large", ReadType::Bytes).await?).len(),
            cellar_logic::kv::MAX_VALUE_BYTES
        );
        assert_eq!(
            text(namespace.get(&context, "A/one", ReadType::Text).await?),
            "first"
        );
        return Ok(response("restore checked"));
    }
    if request.url.ends_with("/revision-writer") {
        namespace
            .put_text(&context, "conditional/sdk", "sdk", Default::default())
            .await?;
        namespace
            .delete_many(&context, &["conditional/deleted-by-sdk".into()])
            .await?;
        return Ok(response("revision written"));
    }
    kv_contract::basic(&context, &namespace).await?;
    namespace
        .put_text(
            &context,
            "expiring",
            "short",
            PutOptions {
                expiration: Some(1.0),
                expiration_ttl: Some(1.01),
                metadata: Some(Value::Null),
            },
        )
        .await?;
    let page = namespace
        .list(
            &context,
            ListOptions {
                prefix: "expiring".into(),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(page.keys[0].metadata, Some(Value::Null));
    assert!(page.keys[0].expiration.is_some());
    namespace
        .put_text(&context, "delete-one", "x", Default::default())
        .await?;
    namespace.delete(&context, "delete-one").await?;
    namespace
        .put_text(&context, "delete-many", "x", Default::default())
        .await?;
    namespace
        .delete_many(&context, &["delete-many".into(), "missing".into()])
        .await?;
    assert!(namespace
        .get(&context, "delete-one", ReadType::Text)
        .await?
        .is_none());
    assert!(namespace
        .get(&context, "delete-many", ReadType::Text)
        .await?
        .is_none());
    Ok(response("binding checked"))
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

fn request(bytes: Vec<u8>) -> RuntimeFetch {
    RuntimeFetch {
        url: "http://cell/".into(),
        method: "POST".into(),
        body: bytes.into(),
        headers: Vec::new(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}

async fn operator(runtime: &RuntimeManager, body: Value) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            scope(),
            Some(cellar_logic::kv::cell_name(IDENTITY, 0)),
            request(serde_json::to_vec(&body)?),
            None,
        )
        .await
}

async fn rpc(runtime: &RuntimeManager, method: &str, body: Value) -> Result<Value> {
    let result = runtime
        .rpc(
            scope(),
            None,
            method.into(),
            encode_rpc(&cellar::native::clone::StructuredValue::from_json(json!(
                [body]
            ))?)?,
            Some(cellar::engine_api::next_request_id()),
        )
        .await?;
    decode_rpc_result(&result.data)?.to_json()
}

async fn worker(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_worker(
            format!("http://worker/{path}"),
            "GET".into(),
            Vec::new(),
            Vec::new(),
        )
        .await
}

async fn fire(runtime: &RuntimeManager, op: u64) -> Result<()> {
    // The reply can arrive before the turn driver publishes its alarm moves
    // to the host cache. Wait for that publication rather than racing it.
    let deadline = cellar::asyncrt::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(deadline) = runtime.alarm(&scope()) {
                break deadline;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| anyhow!("KV did not publish its durable wake for operation {op}"))?;
    let current = cellar::ownership_store::now_ms() as i64;
    if deadline >= current {
        cellar::asyncrt::sleep(Duration::from_millis((deadline - current + 5) as u64)).await;
    }
    runtime.fire_alarm(op, scope(), deadline).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_kv_preserves_bindings_expiry_blob_protocol_and_operator_routes() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(60), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    std::env::set_var(
        "CELLAR_TEST_KV",
        "min-ttl-ms=1000,blob-sweep-ms=1000,no-sweep",
    );
    let registry = Registry::new(|context, invocation| Box::pin(binding(context, invocation)))
        .cell("Caller", |context, _| {
            Ok(Arc::new(Caller {
                namespace: context.kv("KV")?,
                storage: context.storage()?,
            }))
        })?
        .cell(cellar::deploy::KV_CLASS, |_, _| {
            Err(anyhow!("runtime must supply reserved factory"))
        })?;
    let generation = Generation::build_native(
        1,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-kv".into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    assert!(generation
        .reserved_classes()
        .contains(&cellar::deploy::KV_CLASS.into()));
    let directory = tempfile::tempdir()?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
    cellar::host_channels::set_kv_blob_store(bucket.clone());
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
    open(&runtime, &scope()).await?;
    open(&runtime, "Caller:test").await?;
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let (tx, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(tx);
    let host = runtime.clone();
    let routes = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let count = routes.clone();
    let router = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {"native KV routing adapter stops";_=stop.changed()=>None,call=calls.recv()=>call,}
        {
            assert_eq!(call.scope, scope());
            assert_eq!(
                call.name.as_deref(),
                Some(cellar_logic::kv::cell_name(IDENTITY, 0).as_str())
            );
            assert!(matches!(call.args, RpcData::Structured(_)));
            count.lock().unwrap().insert(call.method.clone());
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
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let collected = tickets.clone();
    let pause = Arc::new(AtomicBool::new(false));
    let paused = pause.clone();
    let (withheld, mut held) = tokio::sync::mpsc::unbounded_channel();
    let (tx, mut gates) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(tx);
    let mut stop = shutdown.subscribe();
    let gate_router = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"native KV gate adapter stops";_=stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            collected
                .lock()
                .unwrap()
                .push((gate.scope.clone(), gate.ticket));
            if gate.ticket.channel == cellar_logic::Channel::Service
                && paused.swap(false, Ordering::SeqCst)
            {
                let _ = withheld.send(gate);
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
    assert_eq!(worker(&runtime, "basic").await?.body, b"binding checked");
    assert_eq!(
        *routes.lock().unwrap(),
        ["__kvGet", "__kvPut", "__kvDelete", "__kvList"]
            .into_iter()
            .map(String::from)
            .collect()
    );
    let caller = runtime
        .fetch_cell("Caller:test".into(), None, request(Vec::new()), None)
        .await?;
    assert_eq!(caller.body, b"first");
    assert!(caller.write_position.is_some());
    assert!(tickets
        .lock()
        .unwrap()
        .iter()
        .any(|(scope, ticket)| scope == "Caller:test"
            && ticket.channel == cellar_logic::Channel::CellRpc
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    cellar::asyncrt::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        serde_json::from_slice::<Value>(
            &operator(&runtime, json!({"op":"get","key":"expiring"}))
                .await?
                .body
        )?["result"]["found"],
        false
    );
    let info =
        serde_json::from_slice::<Value>(&operator(&runtime, json!({"op":"info"})).await?.body)?;
    assert_eq!(
        info["result"]["stored"].as_i64().unwrap() - info["result"]["live"].as_i64().unwrap(),
        1,
        "expiry filter must hide unswept rows"
    );
    std::env::set_var("CELLAR_TEST_KV", "min-ttl-ms=1000,blob-sweep-ms=1000");
    runtime.rpc(scope(),None,"__kvPut".into(),encode_rpc(&cellar::native::clone::StructuredValue::from_json(json!([{"entries":[{"key":"wake-expiry","value":[1],"tag":"bytes","expiresAt":1}]}]))?)?,None).await?;
    fire(&runtime, 1).await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&operator(&runtime, json!({"op":"info"})).await?.body)?
            ["result"]["stored"],
        info["result"]["live"]
    );
    assert_eq!(worker(&runtime, "large").await?.body, b"large checked");
    let prefix = cellar_logic::kv::BlobRef::v1_object_prefix(&scope());
    assert_eq!(bucket.list(&prefix).await?.len(), 1);
    let bounded = operator(
        &runtime,
        json!({"op":"get","key":"large","max_bytes":1024*1024}),
    )
    .await?;
    let bounded: Value = serde_json::from_slice(&bounded.body)?;
    assert_eq!(bounded["result"]["valueOmitted"], true);
    assert_eq!(bounded["result"]["size"], cellar_logic::kv::MAX_VALUE_BYTES);
    assert!(bounded["result"].get("value").is_none());
    assert_eq!(bounded["result"]["metadata"], "{\"large\":true}");
    let raw = operator(&runtime, json!({"op":"get","key":"large"})).await?;
    assert_eq!(raw.status, 200);
    let raw = serde_json::from_slice::<Value>(&raw.body)?;
    assert_eq!(raw["result"]["valueEncoding"], "base64");
    assert_eq!(raw["result"]["metadata"], "{\"large\":true}");
    assert!(tickets
        .lock()
        .unwrap()
        .iter()
        .any(|(scope, ticket)| scope == &self::scope()
            && ticket.channel == cellar_logic::Channel::Service
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    runtime.swap_out_cell(&scope(), 1).await?;
    runtime.start_cell(scope(), 1, false).await?;
    runtime.publish_cell(&scope(), 1)?;
    assert_eq!(worker(&runtime, "restore").await?.body, b"restore checked");
    std::env::set_var(
        "CELLAR_TEST_KV",
        "min-ttl-ms=1000,blob-sweep-ms=1000,fail-after-blob",
    );
    assert_eq!(worker(&runtime, "orphan").await?.body, b"orphan checked");
    assert_eq!(bucket.list(&prefix).await?.len(), 2);
    assert!(runtime.alarm(&scope()).is_some());
    std::env::set_var("CELLAR_TEST_KV", "min-ttl-ms=1000,blob-sweep-ms=1000");
    let bucket_db = rusqlite::Connection::open(directory.path().join("bucket.sqlite"))?;
    bucket_db.execute_batch("CREATE TRIGGER refuse_gc BEFORE DELETE ON __sys_local_objects BEGIN SELECT RAISE(FAIL,'injected blob deletion failure'); END;")?;
    fire(&runtime, 2).await?;
    assert_eq!(
        bucket.list(&prefix).await?.len(),
        2,
        "failed collection must leave the live and orphan objects"
    );
    assert!(
        runtime.alarm(&scope()).is_some(),
        "failed collection must retain its durable wake"
    );
    bucket_db.execute_batch("DROP TRIGGER refuse_gc")?;
    fire(&runtime, 22).await?;
    assert_eq!(bucket.list(&prefix).await?.len(), 1);
    // Hold the collector after it reads its live set. A concurrent put must
    // stay behind the protocol lock until the stale snapshot is swept.
    pause.store(true, Ordering::SeqCst);
    assert_eq!(
        operator(&runtime, json!({"op":"delete","keys":["large"]}))
            .await?
            .status,
        200
    );
    let alarm = fire(&runtime, 3);
    tokio::pin!(alarm);
    let gate = cellar::asyncrt::select_biased! {"collector reaches its real egress seam";result=&mut alarm=>{result?;return Err(anyhow!("alarm unexpectedly finished before held gate"));},gate=held.recv()=>gate.unwrap(),};
    let new_value = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        vec![55; cellar_logic::kv::MAX_INLINE_VALUE_BYTES + 1],
    );
    let put = operator(
        &runtime,
        json!({"op":"put-base64","key":"during-sweep","value":new_value}),
    );
    tokio::pin!(put);
    assert!(
        cellar::asyncrt::timeout(Duration::from_millis(60), &mut put)
            .await
            .is_err()
    );
    gate.reply
        .send(Ok(()))
        .map_err(|_| anyhow!("collector lost held gate"))?;
    alarm.await?;
    assert_eq!(put.await?.status, 200);
    assert_eq!(bucket.list(&prefix).await?.len(), 1);
    let result = operator(&runtime, json!({"op":"get","key":"during-sweep"})).await?;
    assert_eq!(result.status, 200);
    // Current collectors must spare later-epoch subtrees.
    let digest = "0".repeat(64);
    let future = cellar_logic::kv::BlobRef::v1(2, &digest)?.object_key(&scope());
    bucket.put(&future, vec![2]).await?;
    operator(&runtime, json!({"op":"delete","keys":["during-sweep"]})).await?;
    fire(&runtime, 4).await?;
    assert!(bucket.get(&future).await?.is_some());
    assert_eq!(bucket.list(&prefix).await?.len(), 1);
    // A complete batch commits atomically and reclamation is bounded.
    let count = cellar_logic::sweep::BATCH_ROWS + 3;
    let entries=(0..count).map(|index|json!({"key":format!("expired/{index:04}"),"value":[65],"tag":"bytes","expiresAt":1})).collect::<Vec<_>>();
    let reply = runtime
        .rpc(
            scope(),
            None,
            "__kvPut".into(),
            encode_rpc(&cellar::native::clone::StructuredValue::from_json(
                json!([{ "entries":entries }]),
            )?)?,
            None,
        )
        .await?;
    assert!(matches!(
        decode_rpc_result(&reply.data)?.root,
        cellar::native::clone::Value::Undefined
    ));
    fire(&runtime, 5).await?;
    let first =
        serde_json::from_slice::<Value>(&operator(&runtime, json!({"op":"info"})).await?.body)?
            ["result"]
            .clone();
    assert_eq!(
        first["stored"].as_i64().unwrap() - first["live"].as_i64().unwrap(),
        3
    );
    assert!(runtime.alarm(&scope()).is_some());
    fire(&runtime, 6).await?;
    assert_eq!(rpc(&runtime,"__kvList",json!({"prefix":"expired/","limit":1000,"after":"","now":cellar::ownership_store::now_ms()})).await?,json!({"keys":[],"complete":true}));
    let invalid = runtime
        .fetch_cell(scope(), None, request(b"not JSON".to_vec()), None)
        .await?;
    assert_eq!(invalid.status, 400);
    assert_eq!(
        serde_json::from_slice::<Value>(&invalid.body)?["error"],
        "KV_ERROR: invalid request body"
    );
    for body in [json!({"op":"put","key":"","value":[]}), json!({"op":"wat"})] {
        assert_eq!(operator(&runtime, body).await?.status, 400);
    }
    let saved = operator(
        &runtime,
        json!({"op":"put","key":"operator","value":[255,1],"metadata":"{\"operator\":true}"}),
    )
    .await?;
    assert_eq!(saved.status, 200);
    assert!(saved.write_position.is_some());
    let saved = operator(&runtime, json!({"op":"get","key":"operator"})).await?;
    assert!(saved.observed_position.is_some());
    assert_eq!(
        serde_json::from_slice::<Value>(&saved.body)?["result"]["value"],
        json!([255, 1])
    );
    check_conditional_edits(&runtime).await?;
    runtime.swap_out_cell(&scope(), 1).await?;
    system_schema::assert_system_tables(
        &directory
            .path()
            .join("cells")
            .join(scope())
            .join("ltx/e1/db.sqlite"),
        &["__sys_kv", "__sys_kv_meta"],
    )?;
    // Prepare the pre-revision schema only while this private fixture is inactive.
    // Reactivation must evolve it through the production namespace owner.
    let old_db = rusqlite::Connection::open(
        directory
            .path()
            .join("cells")
            .join(scope())
            .join("ltx/e1/db.sqlite"),
    )?;
    old_db.execute_batch("ALTER TABLE __sys_kv DROP COLUMN revision; DELETE FROM __sys_kv_meta WHERE name='revision';")?;
    drop(old_db);
    runtime.start_cell(scope(), 1, false).await?;
    runtime.publish_cell(&scope(), 1)?;
    let legacy = checked_operator(&runtime, json!({"op":"get","key":"operator"})).await?;
    assert_eq!(legacy["value"], json!([255, 1]));
    assert_eq!(legacy["metadata"], "{\"operator\":true}");
    assert_eq!(legacy["revision"], "0");
    assert_eq!(checked_operator(&runtime, json!({"op":"edit","key":"operator","value":"/wE=","tag":"bytes","metadata":"{\"operator\":true}","condition":{"revision":"0"},"expiration_mode":{"mode":"preserve"}})).await?["ok"], true);
    assert_eq!(
        checked_operator(&runtime, json!({"op":"get","key":"operator"})).await?["revision"],
        "1"
    );
    runtime.swap_out_cell(&scope(), 1).await?;
    runtime.swap_out_cell("Caller:test", 1).await?;
    shutdown.send(true)?;
    router.await?;
    gate_router.await?;
    std::env::remove_var("CELLAR_TEST_KV");
    Ok(())
}

// These requests execute real owner turns and pass through the same output gate
// as SDK writes. No test mutates the active namespace's SQLite file.
async fn checked_operator(runtime: &RuntimeManager, body: Value) -> Result<Value> {
    let reply = operator(runtime, body).await?;
    assert_eq!(
        reply.status,
        200,
        "{}",
        String::from_utf8_lossy(&reply.body)
    );
    Ok(serde_json::from_slice::<Value>(&reply.body)?["result"].clone())
}

async fn check_conditional_edits(runtime: &RuntimeManager) -> Result<()> {
    let edit = |key: &str, revision: Value, value: &str, expiration: Value| {
        json!({
            "op":"edit","key":key,"condition":{"revision":revision},
            "value":value,"tag":"bytes","metadata":"null","expiration_mode":expiration,
        })
    };
    let never = json!({"mode":"never"});
    let preserve = json!({"mode":"preserve"});
    let expires = cellar::ownership_store::now_ms() + 60_000;
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/key",
                Value::Null,
                "AP8B",
                json!({"mode":"at","timestamp_ms":expires})
            )
        )
        .await?["ok"],
        true
    );
    let first = checked_operator(runtime, json!({"op":"get","key":"conditional/key"})).await?;
    assert_eq!(first["value"], json!([0, 255, 1]));
    assert_eq!(
        first["metadata"], "null",
        "JSON null metadata is not absent metadata"
    );
    assert_eq!(first["expiration"], expires);
    assert_eq!(
        checked_operator(
            runtime,
            edit("conditional/key", Value::Null, "YQ==", never.clone())
        )
        .await?["conflict"],
        true,
        "create cannot overwrite a live key"
    );
    let revision = first["revision"].clone();
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/key",
                revision.clone(),
                "Ag==",
                preserve.clone()
            )
        )
        .await?["ok"],
        true
    );
    let second = checked_operator(runtime, json!({"op":"get","key":"conditional/key"})).await?;
    assert_ne!(second["revision"], revision);
    assert_eq!(
        second["expiration"], expires,
        "preserve retains absolute expiry exactly"
    );
    assert_eq!(
        checked_operator(
            runtime,
            edit("conditional/key", revision.clone(), "Aw==", never.clone())
        )
        .await?["conflict"],
        true
    );
    assert_eq!(
        checked_operator(
            runtime,
            json!({"op":"delete","keys":["conditional/key"],"condition":{"revision":revision}})
        )
        .await?["conflict"],
        true
    );
    assert_eq!(checked_operator(runtime, json!({"op":"delete","keys":["conditional/key"],"condition":{"revision":second["revision"]}})).await?["ok"], true);
    assert_eq!(
        checked_operator(
            runtime,
            edit("conditional/key", Value::Null, "BA==", never.clone())
        )
        .await?["ok"],
        true
    );
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/key",
                second["revision"].clone(),
                "BQ==",
                never.clone()
            )
        )
        .await?["conflict"],
        true,
        "delete/recreate cannot authorize an old edit"
    );

    // Every writer must advance revisions, including SDK puts, bulk RPC puts,
    // old CLI/operator puts and deletion performed through the SDK binding.
    for key in ["conditional/sdk", "conditional/deleted-by-sdk"] {
        checked_operator(runtime, edit(key, Value::Null, "AA==", never.clone())).await?;
    }
    let before_sdk = checked_operator(runtime, json!({"op":"get","key":"conditional/sdk"})).await?;
    let before_delete = checked_operator(
        runtime,
        json!({"op":"get","key":"conditional/deleted-by-sdk"}),
    )
    .await?;
    assert_eq!(
        worker(runtime, "revision-writer").await?.body,
        b"revision written"
    );
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/sdk",
                before_sdk["revision"].clone(),
                "AA==",
                preserve.clone()
            )
        )
        .await?["conflict"],
        true
    );
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/deleted-by-sdk",
                before_delete["revision"].clone(),
                "AA==",
                preserve.clone()
            )
        )
        .await?["conflict"],
        true
    );
    let reply = runtime.rpc(scope(), None, "__kvPut".into(), encode_rpc(&cellar::native::clone::StructuredValue::from_json(json!([{"entries":[{"key":"conditional/sdk","value":[9],"tag":"bytes"},{"key":"conditional/bulk","value":[8],"tag":"bytes"}]}]))?)?, None).await?;
    assert!(matches!(
        decode_rpc_result(&reply.data)?.root,
        cellar::native::clone::Value::Undefined
    ));
    let bulk = checked_operator(runtime, json!({"op":"get","key":"conditional/sdk"})).await?;
    assert_ne!(bulk["revision"], before_sdk["revision"]);
    checked_operator(
        runtime,
        json!({"op":"put","key":"conditional/sdk","value":[7]}),
    )
    .await?;
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/sdk",
                bulk["revision"].clone(),
                "AA==",
                never.clone()
            )
        )
        .await?["conflict"],
        true
    );

    // An unswept expired row counts as absent, and cannot preserve a live token.
    let expires = cellar::ownership_store::now_ms() + 80;
    checked_operator(
        runtime,
        edit(
            "conditional/expires",
            Value::Null,
            "AA==",
            json!({"mode":"at","timestamp_ms":expires}),
        ),
    )
    .await?;
    let expiring =
        checked_operator(runtime, json!({"op":"get","key":"conditional/expires"})).await?;
    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/expires",
                expiring["revision"].clone(),
                "AQ==",
                preserve.clone()
            )
        )
        .await?["conflict"],
        true
    );
    assert_eq!(
        checked_operator(
            runtime,
            edit("conditional/expires", Value::Null, "AQ==", never.clone())
        )
        .await?["ok"],
        true
    );

    let blob = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        vec![201; cellar_logic::kv::MAX_INLINE_VALUE_BYTES + 1],
    );
    checked_operator(
        runtime,
        edit("conditional/blob", Value::Null, &blob, never.clone()),
    )
    .await?;
    let original = checked_operator(runtime, json!({"op":"get","key":"conditional/blob"})).await?;
    assert_eq!(original["value"], blob);
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/blob",
                original["revision"].clone(),
                "AA==",
                preserve
            )
        )
        .await?["ok"],
        true
    );
    assert_eq!(
        checked_operator(
            runtime,
            edit(
                "conditional/blob",
                original["revision"].clone(),
                &blob,
                never
            )
        )
        .await?["conflict"],
        true
    );
    let persisted = checked_operator(runtime, json!({"op":"get","key":"conditional/key"})).await?;
    runtime.swap_out_cell(&scope(), 1).await?;
    runtime.start_cell(scope(), 1, false).await?;
    runtime.publish_cell(&scope(), 1)?;
    assert_eq!(
        checked_operator(runtime, json!({"op":"get","key":"conditional/key"})).await?["revision"],
        persisted["revision"],
        "reactivation preserves the revision"
    );
    Ok(())
}
