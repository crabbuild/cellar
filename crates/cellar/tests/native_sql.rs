//! Native SQL runs in production turns without initializing the JS engine.
//! Host channel adapters stand in for fleet routing and durability proofs.

use anyhow::{anyhow, Result};
use cellar::application::sql::{BindValue, Database};
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

const IDENTITY: &str = "native-test/database";

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-sql".into(),
            do_classes: vec![cellar::deploy::SQL_CLASS.into(), "Caller".into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: vec![("DB".into(), IDENTITY.into())],
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "native-sql".into(),
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

fn response(value: impl Into<Vec<u8>>) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: value.into(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

struct Caller {
    database: Database,
    storage: Storage,
}

impl Cell for Caller {
    fn invoke(self: Arc<Self>, context: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.storage.put_json("before", &json!(true))?;
            let value = self
                .database
                .prepare("SELECT count(*) AS n FROM data")
                .first(&context, Some("n"))
                .await?;
            Ok(response(value.unwrap().to_string()))
        })
    }
}

async fn check_binding(context: Context) -> Result<Output> {
    let database = context.sql("DB")?;
    assert!(context.sql("missing").is_err());
    let created = database.exec(&context, "CREATE TABLE data(id INTEGER PRIMARY KEY, value BLOB, flag INTEGER, missing); SELECT 1").await?;
    assert_eq!(created.count, 2);
    let original =
        database.prepare("INSERT INTO data(value,flag,missing) VALUES (?,?,?) RETURNING id");
    let bound = original.bind([
        BindValue::typed_elements([-1.0, 1.5, 257.0]),
        true.into(),
        f64::NAN.into(),
    ]);
    let inserted = bound.run(&context).await?;
    assert!(inserted.success);
    assert_eq!(inserted.results[0]["id"], json!(1));
    assert!(inserted.meta.changed_db);
    assert_eq!(inserted.meta.changes, 1);
    assert_eq!(inserted.meta.last_row_id, 1);
    assert_eq!(inserted.meta.served_by, "cellar");
    assert!(inserted.meta.served_by_primary);
    assert!(original
        .run(&context)
        .await
        .unwrap_err()
        .message
        .contains("parameter bindings"));
    assert_eq!(
        database
            .prepare("SELECT value,flag,missing FROM data")
            .first(&context, None)
            .await?,
        Some(json!({"value":[255,1,1],"flag":1,"missing":null}))
    );
    assert_eq!(
        database
            .prepare("SELECT NULL AS n")
            .first(&context, Some("n"))
            .await?,
        Some(Value::Null)
    );
    assert_eq!(
        database
            .prepare("SELECT 1 WHERE 0")
            .first(&context, None)
            .await?,
        None
    );
    let missing = database
        .prepare("SELECT 1 AS x")
        .first(&context, Some("missing"))
        .await
        .unwrap_err();
    assert_eq!(missing.family, "SQL_COLUMN_NOTFOUND");
    assert_eq!(missing.cause(), "Column not found");
    assert_eq!(
        database
            .prepare("SELECT 1 AS x,2 AS x,3 AS __proto__")
            .first(&context, None)
            .await?,
        Some(json!({"x":2,"__proto__":3}))
    );
    assert_eq!(
        database
            .prepare("SELECT 1 AS x,2 AS y")
            .raw(&context, true)
            .await?,
        vec![vec![json!("x"), json!("y")], vec![json!(1), json!(2)]]
    );
    assert!(database
        .prepare("SELECT ?")
        .bind_json([json!({"invalid":true})])
        .is_err());
    let invalid = database
        .prepare("SELECT ?")
        .bind_json([json!([1.5])])?
        .all(&context)
        .await
        .unwrap_err();
    assert_eq!(invalid.family, "SQL_TYPE_ERROR");

    let statements = [
        database.prepare("INSERT INTO data(id) VALUES (2)"),
        database.prepare("INSERT INTO data(id) VALUES (1)"),
    ];
    assert_eq!(
        database
            .batch(&context, &statements)
            .await
            .unwrap_err()
            .family,
        "SQL_ERROR"
    );
    assert_eq!(
        database
            .prepare("SELECT count(*) AS n FROM data")
            .first(&context, Some("n"))
            .await?,
        Some(json!(1))
    );
    let results = database
        .batch(
            &context,
            &[
                database.prepare("INSERT INTO data(id) VALUES (2)"),
                database.prepare("SELECT id FROM data ORDER BY id"),
            ],
        )
        .await?;
    assert_eq!(
        results[1].results,
        vec![
            json!({"id":1}).as_object().unwrap().clone(),
            json!({"id":2}).as_object().unwrap().clone()
        ]
    );
    let exec = database
        .exec(
            &context,
            "INSERT INTO data(id) VALUES (3); SELECT * FROM nonexistent;",
        )
        .await
        .unwrap_err();
    assert_eq!(exec.family, "SQL_EXEC_ERROR");
    assert!(database
        .prepare("SELECT id FROM data WHERE id=3")
        .first(&context, None)
        .await?
        .is_none());
    assert_eq!(
        database
            .exec(&context, "SELECT 'unterminated")
            .await
            .unwrap_err()
            .family,
        "SQL_EXEC_ERROR"
    );
    assert_eq!(
        database
            .prepare("SELECT 1; SELECT 2")
            .all(&context)
            .await
            .unwrap_err()
            .family,
        "SQL_ERROR"
    );
    assert_eq!(
        database
            .prepare("PRAGMA writable_schema=ON")
            .all(&context)
            .await
            .unwrap_err()
            .family,
        "SQL_ERROR"
    );

    let session = database.with_session(Some("  opaque-token  "))?;
    let shared = session.clone();
    assert_eq!(session.get_bookmark(), Some("opaque-token".into()));
    assert!(session
        .prepare("SELECT * FROM missing_session_table")
        .all(&context)
        .await
        .is_err());
    assert_eq!(session.get_bookmark(), Some("opaque-token".into()));
    session.prepare("SELECT 1").all(&context).await?;
    assert_eq!(shared.get_bookmark(), Some("cellar:primary".into()));
    for constraint in [None, Some("first-primary"), Some("first-unconstrained")] {
        let session = database.with_session(constraint)?;
        assert_eq!(session.get_bookmark(), None);
        session.batch(&context, &[]).await?;
        assert_eq!(session.get_bookmark(), Some("cellar:primary".into()));
    }
    assert_eq!(
        database.with_session(Some(" \t ")).unwrap_err().family,
        "SQL_SESSION_ERROR"
    );
    assert_eq!(database.dump().await.unwrap_err().family, "SQL_ERROR");

    assert!(database
        .migrate(
            &context,
            "bad.sql",
            "CREATE TABLE rolled_back(x); INSERT INTO nonexistent VALUES (1);",
            "__sys_sql_migrations"
        )
        .await
        .is_err());
    assert!(database
        .prepare("SELECT * FROM rolled_back")
        .all(&context)
        .await
        .is_err());
    database
        .migrate(
            &context,
            "quoted'name.sql",
            "CREATE TABLE migrated(x); INSERT INTO migrated VALUES (7);",
            "__sys_sql_migrations",
        )
        .await?;
    assert_eq!(
        database
            .migrations(&context, "__sys_sql_migrations")
            .await?,
        vec!["quoted'name.sql"]
    );
    assert!(database
        .prepare("SELECT name FROM __sys_sql_migrations")
        .all(&context)
        .await
        .is_err());
    assert!(database
        .migrations(&context, "__sys_cell_kv")
        .await
        .is_err());
    assert!(database.migrations(&context, "history").await.is_err());
    database
        .migrate(
            &context,
            "custom.sql",
            "CREATE TABLE custom_migrated(x);",
            "__sys_sql_migrations_custom",
        )
        .await?;
    assert_eq!(
        database
            .migrations(&context, "__sys_sql_migrations_custom")
            .await?,
        vec!["custom.sql"]
    );
    assert!(database
        .migrate(
            &context,
            "forbidden.sql",
            "DROP TABLE __sys_cell_metadata;",
            "__sys_sql_migrations"
        )
        .await
        .is_err());
    assert_eq!(
        database
            .migrations(&context, "__sys_sql_migrations")
            .await?,
        vec!["quoted'name.sql"]
    );
    assert!(database
        .migrate(
            &context,
            "quoted'name.sql",
            "CREATE TABLE duplicate_migration(x);",
            "__sys_sql_migrations"
        )
        .await
        .is_err());
    assert!(database
        .prepare("SELECT * FROM duplicate_migration")
        .all(&context)
        .await
        .is_err());
    assert!(database
        .migrate(&context, "invalid.sql", "SELECT 1", "a; DROP TABLE data")
        .await
        .is_err());
    // first() must retain only one row even when its source exceeds the cap.
    assert_eq!(database.prepare("WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<100001) SELECT n FROM seq").first(&context,Some("n")).await?,Some(json!(1)));
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

async fn operator(runtime: &RuntimeManager, bytes: Vec<u8>) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            cellar::engine_api::sql_cell_scope(IDENTITY),
            Some(IDENTITY.into()),
            RuntimeFetch {
                url: "http://cell/".into(),
                method: "POST".into(),
                body: bytes.into(),
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
async fn native_sql_sdk_and_operator_use_protected_migration_history() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run_native_sql_test()).await??;
    Ok(())
}

async fn run_native_sql_test() -> Result<()> {
    let registry = Registry::new(|context, _| Box::pin(check_binding(context)))
        .cell("Caller", |context, _| {
            Ok(Arc::new(Caller {
                database: context.sql("DB")?,
                storage: context.storage()?,
            }))
        })?
        .cell(cellar::deploy::SQL_CLASS, |_, _| {
            Err(anyhow!(
                "reserved SQL factory must be supplied by the runtime"
            ))
        })?;
    let generation = Generation::build_native(
        1,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-sql".into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    assert!(generation
        .reserved_classes()
        .iter()
        .any(|class| class == cellar::deploy::SQL_CLASS));
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().into(),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    let scope = cellar::engine_api::sql_cell_scope(IDENTITY);
    open(&runtime, &scope).await?;
    open(&runtime, "Caller:test").await?;
    let (shutdown, mut rpc_shutdown) = tokio::sync::watch::channel(false);
    let (rpc, mut calls) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_rpc_call_tx(rpc);
    let host = runtime.clone();
    let routed = Arc::new(Mutex::new(0));
    let count = routed.clone();
    let router = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {
            "native SQL test stops the host adapter before accepting another call";
            _=rpc_shutdown.changed()=>None,
            call=calls.recv()=>call,
        } {
            assert_eq!(call.scope, cellar::engine_api::sql_cell_scope(IDENTITY));
            assert_eq!(call.name.as_deref(), Some(IDENTITY));
            assert!(matches!(call.args, RpcData::Structured(_)));
            *count.lock().unwrap() += 1;
            let result = host
                .rpc(
                    call.scope,
                    call.name,
                    call.method,
                    call.args,
                    Some(cellar::engine_api::next_request_id()),
                )
                .await
                .map(|result| result.data);
            let _ = call.reply.send(result);
        }
    });
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let collected = tickets.clone();
    let (gates, mut gated) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let mut gate_shutdown = shutdown.subscribe();
    let gate_router = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {
            "native SQL test stops the durability adapter before accepting another sample";
            _=gate_shutdown.changed()=>None,
            gate=gated.recv()=>gate,
        } {
            collected.lock().unwrap().push(gate.ticket);
            let _ = gate.reply.send(Ok(()));
        }
    });
    assert_eq!(
        runtime
            .fetch_worker(
                "http://worker/".into(),
                "GET".into(),
                Vec::new(),
                Vec::new()
            )
            .await?
            .body,
        b"binding checked"
    );
    assert!(*routed.lock().unwrap() > 30);
    let request = RuntimeFetch {
        url: "http://caller/".into(),
        method: "GET".into(),
        body: Vec::new().into(),
        headers: Vec::new(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    };
    let caller = runtime
        .fetch_cell("Caller:test".into(), None, request, None)
        .await?;
    assert_eq!(caller.body, b"2");
    assert!(caller.write_position.is_some());
    assert!(
        !tickets.lock().unwrap().is_empty(),
        "caller writes must gate its outbound SQL RPC"
    );
    let ticket = tickets.lock().unwrap()[0];
    assert_eq!(ticket.channel, cellar_logic::Channel::CellRpc);
    assert_eq!(ticket.epoch, Some(1));
    assert!(ticket.position.is_some());

    let invalid = operator(&runtime, b"not JSON".to_vec()).await?;
    assert_eq!(invalid.status, 400);
    assert_eq!(
        serde_json::from_slice::<Value>(&invalid.body)?["error"],
        json!("SQL_ERROR: invalid request body")
    );
    let result=operator(&runtime,serde_json::to_vec(&json!({"exec":{"sql":"SELECT 42 AS n; SELECT value FROM data WHERE id=1", "rows":true}}))?).await?;
    assert_eq!(result.status, 200);
    assert_eq!(
        serde_json::from_slice::<Value>(&result.body)?["result"]["results"][0]["rows"],
        json!([[42]])
    );
    assert!(result.observed_position.is_some());
    let migrated = operator(
        &runtime,
        serde_json::to_vec(
            &json!({"migrate":{"name":"cli.sql","sql":"INSERT INTO migrated VALUES (8)"}}),
        )?,
    )
    .await?;
    assert_eq!(migrated.status, 200);
    assert!(migrated.write_position.is_some());
    let bad = operator(
        &runtime,
        serde_json::to_vec(&json!({"exec":{"sql":"SELECT * FROM missing_cli_table"}}))?,
    )
    .await?;
    assert_eq!(bad.status, 400);
    assert!(serde_json::from_slice::<Value>(&bad.body)?["error"]
        .as_str()
        .unwrap()
        .starts_with("SQL_EXEC_ERROR: "));

    let mut args = cellar::native::clone::StructuredValue::from_json(
        json!([[{"sql":"SELECT 1 AS n","params":[]}]]),
    )?;
    for object in &mut args.objects {
        if let cellar::native::clone::Object::Object(properties) = object {
            if properties
                .iter()
                .any(|(key, _)| key == &cellar::native::clone::Value::from("sql"))
            {
                properties.push(("first".into(), cellar::native::clone::Value::Undefined));
            }
        }
    }
    let result = runtime
        .rpc(
            scope.clone(),
            None,
            "__sqlQuery".into(),
            encode_rpc(&args)?,
            None,
        )
        .await?;
    assert_eq!(
        decode_rpc_result(&result.data)?.to_json()?[0]["rows"],
        json!([[1]])
    );
    let result = runtime
        .rpc(
            scope.clone(),
            None,
            "__sqlQuery".into(),
            RpcData::Json(json!([[{"sql":"SELECT count(*) FROM data"}]]).to_string()),
            None,
        )
        .await?;
    assert!(matches!(result.data, RpcData::Json(_)));
    assert_eq!(
        decode_rpc_result(&result.data)?.to_json()?[0]["rows"],
        json!([[2]])
    );
    let failed = runtime
        .rpc(
            scope.clone(),
            None,
            "__sqlQuery".into(),
            encode_rpc(&cellar::native::clone::StructuredValue::from_json(
                json!([[{"sql":"SELECT * FROM nonexistent"}]]),
            )?)?,
            None,
        )
        .await?;
    let error = decode_rpc_result(&failed.data).unwrap_err();
    assert!(error
        .downcast_ref::<cellar::native::rpc::RemoteError>()
        .is_some());
    assert!(error.to_string().starts_with("SQL_ERROR: no such table"));
    // A too-large RETURNING result refuses the result after draining the
    // statement. Its committed write must still gate the remote error.
    let refused = runtime
        .rpc(
            scope.clone(),
            None,
            "__sqlQuery".into(),
            encode_rpc(&cellar::native::clone::StructuredValue::from_json(
                json!([[{
                    "sql": "INSERT INTO data(id,value) VALUES (3,zeroblob(1048576)) RETURNING value"
                }]]),
            )?)?,
            None,
        )
        .await?;
    assert!(refused.write_position.is_some());
    assert!(decode_rpc_result(&refused.data)
        .unwrap_err()
        .to_string()
        .contains("query result exceeds"));
    let read = runtime
        .rpc(
            scope.clone(),
            None,
            "__sqlQuery".into(),
            encode_rpc(&cellar::native::clone::StructuredValue::from_json(
                json!([[{
                    "sql": "SELECT length(value) FROM data WHERE id=3"
                }]]),
            )?)?,
            None,
        )
        .await?;
    assert!(read.observed_position.is_some());
    assert_eq!(
        decode_rpc_result(&read.data)?.to_json()?[0]["rows"],
        json!([[1048576]])
    );
    runtime.swap_out_cell(&scope, 1).await?;
    runtime.start_cell(scope.clone(), 1, false).await?;
    runtime.publish_cell(&scope, 1)?;
    let restored = operator(
        &runtime,
        serde_json::to_vec(&json!({"statements":[{"sql":"SELECT sum(x) FROM migrated"}]}))?,
    )
    .await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&restored.body)?["result"][0]["rows"],
        json!([[15]])
    );
    runtime.swap_out_cell(&scope, 1).await?;
    runtime.swap_out_cell("Caller:test", 1).await?;
    shutdown.send(true)?;
    router.await?;
    gate_router.await?;
    Ok(())
}
