//! Production native turns with no engine initialization. The proof adapter
//! records actual host gate requests; it does not emulate fleet replication.

use anyhow::{anyhow, Result};
use cellar::application::storage::{SqlCursor, SqlValue};
use cellar::application::{
    Cell, Context, HandlerFuture, Invocation, ListOptions, Output, Registry, Storage, StoredClone,
};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native::clone::{Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use futures_util::FutureExt;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

fn clone_value(value: serde_json::Value) -> StructuredValue {
    StructuredValue::from_json(value).unwrap()
}

fn cycle() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::Object(vec![("self".into(), Value::Reference(0))])],
    }
}

fn response(value: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: value.as_bytes().to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

#[derive(Default)]
struct Signals {
    cursor: Mutex<Option<SqlCursor>>,
    started: Notify,
    release: Notify,
}

struct Store {
    storage: Storage,
    signals: Arc<Signals>,
}

fn check_bulk(storage: &Storage) -> Result<()> {
    storage.put_many_json(&[
        ("a:1".into(), json!(1)),
        ("a:2".into(), json!(2)),
        ("a:3".into(), json!(3)),
        ("b".into(), json!(4)),
    ])?;
    let keys: Vec<_> = storage
        .get_many(&["b".into(), "a:1".into(), "b".into(), "missing".into()])?
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(keys, vec!["a:1", "b"]);
    let options = ListOptions {
        prefix: Some("a:".into()),
        start_after: Some("a:1".into()),
        end: Some("a:4".into()),
        limit: Some(2),
        reverse: true,
        ..Default::default()
    };
    assert_eq!(
        storage
            .list_clone(&options)?
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        vec!["a:3", "a:2"]
    );
    assert!(storage
        .list(&ListOptions {
            limit: Some(0),
            ..Default::default()
        })?
        .is_empty());
    let mut first = storage.list_cursor(&ListOptions {
        prefix: Some("a:".into()),
        ..Default::default()
    })?;
    assert_eq!(first.next().unwrap()?.0, "a:1");
    storage.delete("a:1")?;
    storage.put_json("a:15", &json!(15))?;
    assert_eq!(first.next().unwrap()?.0, "a:15");
    let second = storage.list_cursor(&options)?;
    assert!(first
        .next()
        .unwrap()
        .unwrap_err()
        .to_string()
        .contains("invalidated"));
    assert_eq!(
        second
            .map(|row| row.map(|(key, _)| key))
            .collect::<Result<Vec<_>>>()?,
        vec!["a:3", "a:2"]
    );
    storage.put_many_clone(&[
        ("clone".into(), cycle()),
        ("number".into(), clone_value(json!(7))),
    ])?;
    assert_eq!(storage.get_clone("clone")?.unwrap().value, cycle());
    assert!(storage.get_json("clone").is_err());
    let tagged = StoredClone {
        value: cycle(),
        stored_stubs: true,
    };
    storage.put_many_stored_clone(&[("tagged".into(), tagged)])?;
    assert!(
        storage.get_many_clone(&["tagged".into()])?[0]
            .1
            .stored_stubs
    );
    let invalid = StructuredValue {
        root: Value::Reference(99),
        objects: Vec::new(),
    };
    assert!(storage
        .put_many_clone(&[
            ("must-not-land".into(), clone_value(json!(true))),
            ("invalid".into(), invalid)
        ])
        .is_err());
    assert!(storage.get("must-not-land")?.is_none());
    assert_eq!(
        storage.delete_many(&["a:2".into(), "a:2".into(), "missing".into()])?,
        1
    );
    Ok(())
}

fn check_transactions(storage: &Storage) -> Result<()> {
    assert!(storage.rollback().is_err());
    let escaped = storage.transaction_sync(|transaction| {
        transaction.put_json("committed", &json!(true))?;
        Ok(transaction.clone())
    })?;
    assert!(escaped
        .get_json("committed")
        .unwrap_err()
        .to_string()
        .contains("already committed"));
    assert!(escaped.rollback().is_err());
    assert_eq!(storage.get_json("committed")?, Some(json!(true)));
    let child = Mutex::new(None);
    let result: Result<()> = storage.transaction_sync(|outer| {
        outer.put_json("outer", &json!(1))?;
        let inner: Result<()> = storage.transaction_sync(|inner| {
            inner.put_json("outer", &json!(2))?;
            inner.put_json("inner", &json!(true))?;
            Err(anyhow!("inner rollback"))
        });
        assert!(inner.is_err());
        assert_eq!(outer.get_json("outer")?, Some(json!(1)));
        assert!(outer.get_json("inner")?.is_none());
        storage.transaction_sync(|inner| {
            inner.put_many_json(&[("bulk-in-transaction".into(), json!(true))])?;
            *child.lock().unwrap() = Some(inner.clone());
            Ok(())
        })?;
        Err(anyhow!("outer rollback"))
    });
    assert!(result.is_err());
    assert!(storage.get_json("outer")?.is_none());
    assert!(storage.get_json("bulk-in-transaction")?.is_none());
    assert!(child
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .get("anything")
        .unwrap_err()
        .to_string()
        .contains("rolled back"));
    let result = storage.transaction_sync(|transaction| {
        transaction.put_json("explicit", &json!(true))?;
        transaction.rollback()?;
        transaction.rollback()?;
        assert!(transaction
            .put_json("invalid-after-rollback", &json!(true))
            .is_err());
        Ok(42)
    })?;
    assert_eq!(result, 42);
    assert!(storage.get_json("explicit")?.is_none());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<()> = storage.transaction_sync(|transaction| {
            transaction.put_json("panicked", &json!(true))?;
            panic!("rollback the callback panic")
        });
    }));
    assert!(panic.is_err());
    assert!(storage.get_json("panicked")?.is_none());
    storage.transaction_sync(|transaction| transaction.put_json("after-panic", &json!(true)))?;
    let before = storage.get_alarm()?;
    let rollback: Result<()> = storage.transaction_sync(|transaction| {
        transaction.set_alarm(4_000_000_000_000)?;
        let error = transaction.sync().now_or_never().unwrap().unwrap_err();
        assert!(error.to_string().contains("open transaction"));
        Err(anyhow!("rollback alarm"))
    });
    assert!(rollback.is_err());
    assert_eq!(storage.get_alarm()?, before);
    storage.transaction_sync(|transaction| transaction.set_alarm(4_000_000_000_000))?;
    assert_eq!(storage.get_alarm()?, Some(4_000_000_000_000));
    storage.delete_alarm()?;
    Ok(())
}

fn check_sql(storage: &Storage) -> Result<()> {
    let mut cursor=storage.sql_cursor("CREATE TABLE items(n INTEGER PRIMARY KEY,b BLOB); INSERT INTO items VALUES(1,x'ff'),(2,x'00'); SELECT n,b,9e999 AS inf FROM items ORDER BY n",&[])?;
    assert_eq!(cursor.column_names(), &["n", "b", "inf"]);
    assert_eq!(cursor.rows_read(), 1);
    assert_eq!(cursor.rows_written(), 0);
    let row = cursor.next().unwrap()?;
    assert_eq!(row["n"], SqlValue::Integer(1));
    assert_eq!(row["b"], SqlValue::Blob(vec![255]));
    assert_eq!(row["inf"], SqlValue::Real(f64::INFINITY));
    assert_eq!(cursor.rows_read(), 2);
    assert_eq!(cursor.next().unwrap()?["n"], SqlValue::Integer(2));
    assert!(cursor.next().is_none());
    assert_eq!(cursor.rows_written(), 2);
    let row = storage
        .sql_cursor(
            "SELECT ? AS nan,? AS inf,? AS blob,? AS int",
            &[
                SqlValue::Real(f64::NAN),
                SqlValue::Real(f64::NEG_INFINITY),
                SqlValue::Blob(vec![0, 255]),
                SqlValue::Integer(i64::MAX),
            ],
        )?
        .one()?;
    assert_eq!(row["nan"], SqlValue::Null);
    assert_eq!(row["inf"], SqlValue::Real(f64::NEG_INFINITY));
    assert_eq!(row["blob"], SqlValue::Blob(vec![0, 255]));
    assert_eq!(row["int"], SqlValue::Integer(i64::MAX));
    let row = storage
        .sql_cursor("SELECT 1 AS duplicate,2 AS duplicate,3 AS __proto__", &[])?
        .one()?;
    assert_eq!(row["duplicate"], SqlValue::Integer(2));
    assert_eq!(row["__proto__"], SqlValue::Integer(3));
    assert!(storage
        .sql_cursor("SELECT 1 WHERE 0", &[])?
        .one()
        .unwrap_err()
        .to_string()
        .contains("no results"));
    assert!(storage
        .sql_cursor("SELECT n FROM items", &[])?
        .one()
        .unwrap_err()
        .to_string()
        .contains("multiple results"));
    assert!(storage.sql_cursor("SELECT ?", &[]).is_err());
    assert!(storage.sql_cursor("DROP TABLE __sys_cell_kv", &[]).is_err());
    assert!(storage.sql_database_size()? > 0);
    let (remainder, rows, count) =
        storage.sql_ingest("INSERT INTO items VALUES (3,x'0102'); INSERT INTO items VALUES (")?;
    assert_eq!(count, 1);
    assert_eq!(rows, 1);
    assert!(remainder.contains("INSERT INTO items VALUES ("));
    let (_, rows, count) = storage.sql_ingest(&format!("{remainder}4,x'0304');"))?;
    assert_eq!((rows, count), (1, 1));
    // Read cursor tokens retain their activation after a transaction ends.
    let cursor = storage.transaction_sync(|transaction| {
        transaction.sql_cursor("SELECT n FROM items ORDER BY n", &[])
    })?;
    assert_eq!(cursor.to_raw_array()?.len(), 4);
    Ok(())
}

impl Cell for Store {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("unexpected event"));
            };
            let path = url::Url::parse(&request.url)?.path().to_string();
            let storage = &self.storage;
            match path.as_str() {
                "/bulk" => {
                    check_bulk(storage)?;
                    Ok(response("bulk"))
                }
                "/transaction" => {
                    check_transactions(storage)?;
                    Ok(response("transaction"))
                }
                "/sql" => {
                    check_sql(storage)?;
                    Ok(response("sql"))
                }
                "/sync" => {
                    storage.sync().await?;
                    Ok(response("sync"))
                }
                "/suspend" => {
                    let mut cursor = storage.sql_cursor("SELECT n FROM items ORDER BY n", &[])?;
                    self.signals.started.notify_one();
                    self.signals.release.notified().await;
                    assert_eq!(cursor.next_raw()?, Some(vec![SqlValue::Integer(1)]));
                    Ok(response("resumed"))
                }
                "/capture" | "/held-write" => {
                    let sql = if path == "/held-write" {
                        "INSERT INTO items VALUES(42,x'0102') RETURNING n"
                    } else {
                        "SELECT n FROM items ORDER BY n"
                    };
                    *self.signals.cursor.lock().unwrap() = Some(storage.sql_cursor(sql, &[])?);
                    Ok(response("captured"))
                }
                "/foreign" => {
                    let mut cursor = self.signals.cursor.lock().unwrap();
                    assert!(cursor.as_mut().unwrap().next_raw().is_err());
                    Ok(response("foreign refused"))
                }
                "/consume" => {
                    let mut cursor = self.signals.cursor.lock().unwrap().take().unwrap();
                    assert_eq!(cursor.next_raw()?, Some(vec![SqlValue::Integer(1)]));
                    Ok(response("consumed"))
                }
                "/retired" => {
                    let mut cursor = self.signals.cursor.lock().unwrap().take().unwrap();
                    assert!(cursor.next_raw().is_err());
                    Ok(response("retired refused"))
                }
                "/after-drop" => {
                    assert_eq!(
                        storage
                            .sql_cursor("SELECT n FROM items WHERE n=42", &[])?
                            .one()?["n"],
                        SqlValue::Integer(42)
                    );
                    Ok(response("committed after drop"))
                }
                "/delete-all" => {
                    storage.put_json("delete", &json!(true))?;
                    storage
                        .sql_cursor("CREATE TABLE \"quoted\"\"table\"(x INTEGER)", &[])?
                        .close()?;
                    storage.set_alarm(4_000_000_000_000)?;
                    storage.delete_all()?;
                    assert!(storage.get_json("delete")?.is_none());
                    assert!(storage
                        .sql_cursor("SELECT * FROM \"quoted\"\"table\"", &[])
                        .is_err());
                    let expected = if context.identity().unwrap().class == "ClearingStore" {
                        None
                    } else {
                        Some(4_000_000_000_000)
                    };
                    assert_eq!(storage.get_alarm()?, expected);
                    storage.put_json("after-delete", &json!(true))?;
                    assert_eq!(storage.get_json("after-delete")?, Some(json!(true)));
                    storage.delete_alarm()?;
                    Ok(response("deleted"))
                }
                _ => Ok(response("read")),
            }
        })
    }
}

fn deployment(script: &str, classes: Vec<String>, delete_alarm: bool) -> LoadedDeployment {
    LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: script.into(),
            do_classes: classes,
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat {
                delete_all_deletes_alarm: delete_alarm,
                ..Default::default()
            },
        },
        script_name: script.into(),
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
    }
}

async fn fetch(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            scope.into(),
            None,
            RuntimeFetch {
                url: format!("http://cell{path}"),
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
async fn native_storage_transactions_cursors_and_proofs_use_production_turns() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let mut registry = Registry::new(|_, _| Box::pin(async { Ok(response("stateless")) }));
    for class in ["Store", "Other", "ClearingStore"] {
        let signals = signals.clone();
        registry = registry.cell(class, move |context, _| {
            Ok(Arc::new(Store {
                storage: context.storage()?,
                signals: signals.clone(),
            }))
        })?;
    }
    let application = Arc::new(registry) as Arc<dyn cellar::application::Application>;
    let mut graph = DeploymentGraph::single(deployment(
        "native-store",
        vec!["Store".into(), "Other".into()],
        false,
    ));
    graph.cohosted.push(deployment(
        "native-clear",
        vec!["ClearingStore".into()],
        true,
    ));
    let generation = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([
            ("native-store".into(), application.clone()),
            ("native-clear".into(), application),
        ]),
    )?;
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
    for scope in ["Store:test", "Other:test", "ClearingStore:test"] {
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
        runtime.publish_cell(scope, 1)?;
    }
    let (gates, mut requests) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let collected = tickets.clone();
    let gate_adapter = cellar::asyncrt::spawn(async move {
        while let Some(request) = cellar::asyncrt::select_biased! {
            "native storage test stops its proof adapter";
            _=shutdown_rx.changed()=>None,
            request=requests.recv()=>request,
        } {
            collected.lock().unwrap().push(request.ticket);
            let _ = request.reply.send(Ok(()));
        }
    });
    for path in ["/bulk", "/transaction", "/sql", "/sync"] {
        assert_eq!(fetch(&runtime, "Store:test", path).await?.status, 200);
    }
    let ticket = tickets.lock().unwrap().last().copied().unwrap();
    assert_eq!(ticket.channel, cellar_logic::Channel::Sync);
    assert_eq!(ticket.epoch, Some(1));
    assert!(ticket.position.is_some());
    let host = runtime.clone();
    let suspended =
        cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", "/suspend").await });
    signals.started.notified().await;
    assert_eq!(fetch(&runtime, "Store:test", "/read").await?.body, b"read");
    signals.release.notify_one();
    assert_eq!(suspended.await??.body, b"resumed");
    fetch(&runtime, "Store:test", "/capture").await?;
    assert_eq!(
        fetch(&runtime, "Other:test", "/foreign").await?.body,
        b"foreign refused"
    );
    assert_eq!(
        fetch(&runtime, "Store:test", "/consume").await?.body,
        b"consumed"
    );
    let error = fetch(&runtime, "Store:test", "/held-write")
        .await
        .err()
        .expect("unfinished write cursor must block output");
    assert!(error.to_string().contains("unfinished SQL write cursor"));
    drop(signals.cursor.lock().unwrap().take()); // Outside the native turn.
    let committed = fetch(&runtime, "Store:test", "/after-drop").await?;
    assert!(committed.observed_position.is_some());
    fetch(&runtime, "Store:test", "/capture").await?;
    runtime.swap_out_cell("Store:test", 1).await?;
    runtime.start_cell("Store:test".into(), 1, false).await?;
    runtime.publish_cell("Store:test", 1)?;
    assert_eq!(
        fetch(&runtime, "Store:test", "/retired").await?.body,
        b"retired refused"
    );
    for scope in ["Store:test", "ClearingStore:test"] {
        assert_eq!(
            fetch(&runtime, scope, "/delete-all").await?.body,
            b"deleted"
        );
    }
    for scope in ["Store:test", "Other:test", "ClearingStore:test"] {
        runtime.swap_out_cell(scope, 1).await?;
    }
    shutdown.send(true)?;
    gate_adapter.await?;

    // Drop must finalize raw cursors and cache entries under their owning
    // Cells installation before dropping the connections.
    let path = directory.path().join("drop.sqlite");
    let cells = cellar::storage::Cells::default();
    let installed = cells.install();
    cellar::storage::open("drop:test", path.to_str().unwrap())?;
    let (id, _, _, _, _) = cellar::storage::sql_cursor_start(
        "drop:test",
        "CREATE TABLE t(n); INSERT INTO t VALUES(1),(2); SELECT n FROM t",
        &[],
    )
    .map_err(anyhow::Error::msg)?;
    assert_ne!(id, 0);
    drop(installed);
    drop(cells);
    let connection = rusqlite::Connection::open(path)?;
    connection.execute("INSERT INTO t VALUES(3)", [])?;
    Ok(())
}
