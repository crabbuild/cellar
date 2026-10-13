//! Async savepoints and input gates through production native turns. No engine init.

use anyhow::{anyhow, Result};
use cellar::application::storage::{SqlCursor, SqlValue};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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
    started: Notify,
    release: Notify,
    child_started: Notify,
    factories: AtomicUsize,
    order: Mutex<Vec<usize>>,
    escaped: Mutex<Option<Storage>>,
    cursor: Mutex<Option<SqlCursor>>,
}

struct Store {
    storage: Storage,
    signals: Arc<Signals>,
}

async fn pause(signals: &Signals) {
    signals.started.notify_one();
    signals.release.notified().await;
}

fn cancelled_slot(context: Context, storage: Storage, remaining: usize) -> HandlerFuture<()> {
    context.block_concurrency_while(move |context| async move {
        if remaining > 0 {
            cancelled_slot(context, storage, remaining - 1).await?;
        } else {
            let result = storage.transaction(&context, |_| async { Ok(()) }).await;
            assert!(result
                .err()
                .unwrap()
                .to_string()
                .contains("nested too deeply"));
        }
        Ok(())
    })
}

fn write(storage: &Storage, key: &str, value: i64) -> Result<()> {
    storage.put_json(key, &value.into())?;
    storage.sql_cursor(
        "INSERT INTO items(key,n) VALUES(?,?)",
        &[key.into(), value.into()],
    )?;
    Ok(())
}

fn absent(storage: &Storage, key: &str) -> Result<()> {
    assert!(storage.get(key)?.is_none(), "{key} survived rollback");
    let row = storage
        .sql_cursor("SELECT count(*) AS n FROM items WHERE key=?", &[key.into()])?
        .one()?;
    assert_eq!(row["n"], SqlValue::Integer(0));
    Ok(())
}

impl Cell for Store {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected fetch"));
            };
            let path = url::Url::parse(&request.url)?.path().to_string();
            let storage = self.storage.clone();
            match path.as_str() {
                "/init" => {
                    storage.sql_cursor("CREATE TABLE items(key TEXT,n INTEGER)", &[])?;
                    storage.put_json("value", &0.into())?;
                    Ok(response("initialized"))
                }
                "/read" => Ok(response(
                    &storage.get_json("value")?.unwrap_or(0.into()).to_string(),
                )),
                "/probe" => {
                    for key in [
                        "error",
                        "panic",
                        "creation",
                        "detached-error",
                        "explicit",
                        "timeout",
                        "timeout-child",
                        "retired",
                        "retired-child",
                        "critical",
                    ] {
                        absent(&storage, key)?;
                    }
                    storage
                        .transaction(&context, |transaction| async move {
                            transaction.put_json("probe", &true.into())?;
                            Ok(())
                        })
                        .await?;
                    Ok(response("clean"))
                }
                "/commit" => {
                    let signals = self.signals.clone();
                    let root = storage.clone();
                    let (view, cursor) = storage
                        .transaction(&context, move |transaction| async move {
                            transaction.put_json("value", &1.into())?;
                            write(&transaction, "commit", 1)?;
                            pause(&signals).await;
                            assert_eq!(root.get_json("value")?, Some(1.into()));
                            let inner = root.tx(|inner| {
                                write(inner, "sync-child", 2)?;
                                Ok(inner.clone())
                            })?;
                            assert!(inner.get("sync-child").is_err());
                            assert!(transaction.sync().await.is_err());
                            let cursor =
                                transaction.sql_cursor("SELECT n FROM items ORDER BY n", &[])?;
                            Ok((transaction, cursor))
                        })
                        .await?;
                    assert!(view
                        .get("commit")
                        .unwrap_err()
                        .to_string()
                        .contains("already committed"));
                    assert!(view.rollback().is_err());
                    assert_eq!(cursor.to_raw_array()?.len(), 2);
                    Ok(response("committed"))
                }
                "/error" | "/panic" | "/creation-panic" => {
                    let key = match path.as_str() {
                        "/error" => "error",
                        "/panic" => "panic",
                        _ => "creation",
                    };
                    let result: Result<()> = if path == "/creation-panic" {
                        storage
                            .transaction(&context, move |transaction| -> HandlerFuture<()> {
                                write(&transaction, key, 9).unwrap();
                                panic!("transaction construction panic");
                            })
                            .await
                    } else {
                        let signals = self.signals.clone();
                        let panic = path == "/panic";
                        storage
                            .transaction(&context, move |transaction| async move {
                                write(&transaction, key, 9)?;
                                transaction.set_alarm(9_999_999_999_999)?;
                                *signals.escaped.lock().unwrap() = Some(transaction.clone());
                                pause(&signals).await;
                                assert!(!panic, "transaction polling panic");
                                Err(anyhow!("transaction callback failed"))
                            })
                            .await
                    };
                    assert!(result.is_err());
                    absent(&storage, key)?;
                    assert!(self
                        .signals
                        .escaped
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .get(key)
                        .is_err());
                    assert_eq!(storage.get_alarm()?, None);
                    storage
                        .transaction(&context, |transaction| async move {
                            transaction.put_json("value", &2.into())?;
                            Ok(())
                        })
                        .await?;
                    Ok(response("rolled back"))
                }
                "/nested" => {
                    let signals = self.signals.clone();
                    let root = storage.clone();
                    let nested_context = context.clone();
                    storage
                        .transaction(&context, move |outer| async move {
                            write(&outer, "outer", 3)?;
                            let first_signals = signals.clone();
                            let first =
                                root.transaction(&nested_context, move |child| async move {
                                    first_signals.order.lock().unwrap().push(1);
                                    write(&child, "nested-error", 4)?;
                                    pause(&first_signals).await;
                                    Err::<(), _>(anyhow!("nested transaction failed"))
                                });
                            let second_signals = signals.clone();
                            let second_root = root.clone();
                            let second =
                                root.transaction(&nested_context, move |child| async move {
                                    second_signals.order.lock().unwrap().push(2);
                                    absent(&child, "nested-error")?;
                                    write(&child, "nested-commit", 5)?;
                                    second_root
                                        .transaction_sync(|sync| {
                                            write(sync, "inner-sync-error", 6)?;
                                            Err::<(), _>(anyhow!("sync rollback"))
                                        })
                                        .unwrap_err();
                                    Ok(child)
                                });
                            assert!(first.await.is_err());
                            let child = second.await?;
                            assert!(child.get("nested-commit").is_err());
                            Ok(())
                        })
                        .await?;
                    assert_eq!(*self.signals.order.lock().unwrap(), vec![1, 2]);
                    absent(&storage, "nested-error")?;
                    absent(&storage, "inner-sync-error")?;
                    assert!(storage.get("outer")?.is_some());
                    assert!(storage.get("nested-commit")?.is_some());
                    Ok(response("nested"))
                }
                "/detached-child" | "/detached-error" | "/explicit" => {
                    let explicit = path == "/explicit";
                    let fail = path == "/detached-error";
                    let key = if explicit {
                        "explicit"
                    } else if fail {
                        "detached-error"
                    } else {
                        "detached-child"
                    };
                    let signals = self.signals.clone();
                    let nested_context = context.clone();
                    let result = storage
                        .transaction(&context, move |outer| async move {
                            let child_signals = signals.clone();
                            drop(outer.transaction(&nested_context, move |child| async move {
                                write(&child, key, 7)?;
                                if explicit {
                                    child_signals.child_started.notify_one();
                                } else {
                                    child_signals.started.notify_one();
                                }
                                child_signals.release.notified().await;
                                if explicit {
                                    assert!(child.get(key).is_err());
                                }
                                Ok(())
                            }));
                            if explicit {
                                signals.child_started.notified().await;
                                outer.rollback()?;
                                outer.rollback()?;
                                assert!(outer.get(key).is_err());
                                signals.started.notify_one();
                            }
                            if fail {
                                Err(anyhow!("detached parent failed"))
                            } else {
                                Ok(outer)
                            }
                        })
                        .await;
                    if fail {
                        assert!(result.is_err());
                        absent(&storage, key)?;
                    } else if explicit {
                        let view = result?;
                        assert!(view.get(key).is_err());
                        absent(&storage, key)?;
                    } else {
                        assert!(result?.get(key).is_err());
                        assert!(storage.get(key)?.is_some());
                    }
                    Ok(response("drained"))
                }
                "/same-event" => {
                    let signals = self.signals.clone();
                    let first = storage.transaction(&context, move |transaction| async move {
                        transaction.put_json("value", &10.into())?;
                        *signals.cursor.lock().unwrap() =
                            Some(transaction.sql_cursor("SELECT n FROM items ORDER BY n", &[])?);
                        pause(&signals).await;
                        Ok(())
                    });
                    self.signals.started.notified().await;
                    assert!(storage
                        .get("value")
                        .unwrap_err()
                        .to_string()
                        .contains("outside the active transaction"));
                    assert!(self
                        .signals
                        .cursor
                        .lock()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .next_raw()
                        .is_err());
                    let second = storage.transaction(&context, |transaction| async move {
                        assert_eq!(transaction.get_json("value")?, Some(10.into()));
                        transaction.put_json("value", &11.into())?;
                        Ok(())
                    });
                    self.signals.release.notify_one();
                    first.await?;
                    second.await?;
                    drop(self.signals.cursor.lock().unwrap().take());
                    Ok(response("ordered"))
                }
                "/outer-block" => {
                    context
                        .block_concurrency_while(move |context| async move {
                            let first = storage.transaction(&context, |transaction| async move {
                                transaction.put_json("value", &20.into())?;
                                Ok(())
                            });
                            let second = storage.transaction(&context, |transaction| async move {
                                assert_eq!(transaction.get_json("value")?, Some(20.into()));
                                transaction.put_json("value", &21.into())?;
                                Ok(())
                            });
                            futures_util::try_join!(first, second)?;
                            Ok(())
                        })
                        .await?;
                    Ok(response("outer block"))
                }
                "/queued-cancel" => {
                    self.signals.order.lock().unwrap().clear();
                    let signals = self.signals.clone();
                    let nested_context = context.clone();
                    storage
                        .transaction(&context, move |outer| async move {
                            let first_signals = signals.clone();
                            let first =
                                outer.transaction(&nested_context, move |child| async move {
                                    first_signals.order.lock().unwrap().push(1);
                                    first_signals.child_started.notify_one();
                                    first_signals.release.notified().await;
                                    child.put_json("queue-first", &true.into())?;
                                    Ok(())
                                });
                            signals.child_started.notified().await;
                            // Reserve and abandon a middle slot via the public
                            // depth limit while the first owns its savepoint.
                            cancelled_slot(nested_context.clone(), outer.clone(), 62).await?;
                            let last_signals = signals.clone();
                            let last =
                                outer.transaction(&nested_context, move |child| async move {
                                    last_signals.order.lock().unwrap().push(3);
                                    assert_eq!(child.get_json("queue-first")?, Some(true.into()));
                                    Ok(())
                                });
                            signals.started.notify_one();
                            futures_util::try_join!(first, last)?;
                            Ok(())
                        })
                        .await?;
                    assert_eq!(*self.signals.order.lock().unwrap(), vec![1, 3]);
                    Ok(response("queue ordered"))
                }
                "/blocks-in-transaction" => {
                    let inner_context = context.clone();
                    let root = storage.clone();
                    let signals = self.signals.clone();
                    storage
                        .transaction(&context, move |outer| async move {
                            let view = outer.clone();
                            inner_context
                                .block_concurrency_while(move |context| async move {
                                    // Explicitly nested blocks inherit the callback;
                                    // an unrelated same-event future does not.
                                    root.put_json("block-root", &true.into())?;
                                    assert_eq!(view.get_json("block-root")?, Some(true.into()));
                                    root.transaction(&context, |child| async move {
                                        cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                                        write(&child, "block-child", 50)?;
                                        Ok(())
                                    })
                                    .await?;
                                    view.put_json("value", &50.into())?;
                                    Ok(())
                                })
                                .await?;
                            drop(inner_context.block_concurrency_while(move |_| async move {
                                write(&outer, "block-detached", 51)?;
                                pause(&signals).await;
                                Ok(())
                            }));
                            Ok(())
                        })
                        .await?;
                    assert!(storage.get("block-detached")?.is_some());
                    Ok(response("blocks drained"))
                }
                "/timeout" | "/retire" => {
                    let timeout = path == "/timeout";
                    let key = if timeout { "timeout" } else { "retired" };
                    let child_key = if timeout {
                        "timeout-child"
                    } else {
                        "retired-child"
                    };
                    let nested_context = context.clone();
                    let signals = self.signals.clone();
                    storage
                        .transaction(&context, move |outer| async move {
                            write(&outer, key, 30)?;
                            let child_signals = signals.clone();
                            drop(outer.transaction(&nested_context, move |child| async move {
                                write(&child, child_key, 31)?;
                                child_signals.started.notify_one();
                                futures_util::future::pending::<()>().await;
                                Ok(())
                            }));
                            futures_util::future::pending::<()>().await;
                            Ok(())
                        })
                        .await?;
                    Ok(response("unexpected"))
                }
                "/critical" => {
                    let inner_context = context.clone();
                    storage
                        .transaction(&context, move |outer| async move {
                            write(&outer, "critical", 40)?;
                            inner_context
                                .block_concurrency_while(|_| async {
                                    Err::<(), _>(anyhow!("enclosing critical failure"))
                                })
                                .await?;
                            Ok(())
                        })
                        .await?;
                    Ok(response("unexpected"))
                }
                "/foreign" => {
                    let saved = self
                        .signals
                        .escaped
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .clone();
                    let result = saved.transaction(&context, |_| async { Ok(()) }).await;
                    assert!(result.is_err());
                    Ok(response("foreign refused"))
                }
                "/sync-boundary" => {
                    storage.tx(|transaction| {
                        let future = transaction.transaction(&context, |_| async { Ok(()) });
                        assert!(futures_util::FutureExt::now_or_never(future)
                            .unwrap()
                            .is_err());
                        Ok(())
                    })?;
                    Ok(response("sync boundary"))
                }
                path => Err(anyhow!("unknown path {path}")),
            }
        })
    }
}

fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://cell{path}"),
        method: "GET".into(),
        body: Vec::new().into(),
        headers: Vec::new(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}

async fn fetch(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(scope.into(), None, request(path), None)
        .await
}

async fn waiting(task: &mut cellar::asyncrt::TaskHandle<Result<HttpResponse>>, label: &str) {
    match cellar::asyncrt::timeout(Duration::from_millis(20), task).await {
        Err(_) => {}
        Ok(Ok(Ok(response))) => panic!(
            "{label} completed before release: {} {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        ),
        Ok(Ok(Err(error))) => panic!("{label} failed before release: {error:#}"),
        Ok(Err(error)) => panic!("{label} task failed before release: {error}"),
    }
}

fn deployment() -> LoadedDeployment {
    LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-transactions".into(),
            do_classes: vec!["Store".into()],
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
            compat: Compat::default(),
        },
        script_name: "native-transactions".into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn asynchronous_native_transactions_preserve_atomicity_and_lifecycle() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(50), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let factory_signals = signals.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Ok(response("worker")) })).cell(
        "Store",
        move |context, _| {
            factory_signals.factories.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::new(Store {
                storage: context.storage()?,
                signals: factory_signals.clone(),
            }))
        },
    )?;
    let generation = Generation::build_native(
        1,
        DeploymentGraph::single(deployment()),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-transactions".into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
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
    for scope in ["Store:test", "Store:other"] {
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
        fetch(&runtime, scope, "/init").await?;
    }
    let (gates, mut requests) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let (shutdown, mut shut) = tokio::sync::watch::channel(false);
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(request) = cellar::asyncrt::select_biased! {
            "stop the transaction test's proof adapter";
            _=shut.changed()=>None,
            request=requests.recv()=>request,
        } {
            let _ = request.reply.send(Ok(()));
        }
    });
    for path in [
        "/commit",
        "/error",
        "/panic",
        "/nested",
        "/detached-child",
        "/detached-error",
        "/explicit",
        "/queued-cancel",
        "/blocks-in-transaction",
    ] {
        let host = runtime.clone();
        let mut operation =
            cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", path).await });
        signals.started.notified().await;
        waiting(&mut operation, path).await;
        assert_eq!(fetch(&runtime, "Store:other", "/read").await?.body, b"0");
        let host = runtime.clone();
        let mut read =
            cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", "/read").await });
        waiting(&mut read, "queued read").await;
        signals.release.notify_one();
        assert_eq!(operation.await??.status, 200);
        assert_eq!(
            read.await??.status,
            200,
            "a callback rollback must admit the queued reader"
        );
        assert_eq!(
            signals.factories.load(Ordering::Relaxed),
            2,
            "ordinary transaction errors must not reset either cell"
        );
    }
    for path in [
        "/creation-panic",
        "/same-event",
        "/outer-block",
        "/foreign",
        "/sync-boundary",
    ] {
        let scope = if path == "/foreign" {
            "Store:other"
        } else {
            "Store:test"
        };
        assert_eq!(fetch(&runtime, scope, path).await?.status, 200);
    }
    assert_eq!(fetch(&runtime, "Store:test", "/read").await?.body, b"21");

    // An ordinary disconnect does not cancel the cell-owned transaction.
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let host = runtime.clone();
    let operation = cellar::asyncrt::spawn(async move {
        host.fetch_cell(
            "Store:test".into(),
            None,
            request("/commit"),
            Some(cancelled),
        )
        .await
    });
    signals.started.notified().await;
    cancel.send(()).unwrap();
    assert!(operation.await?.is_err());
    let host = runtime.clone();
    let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", "/read").await });
    waiting(&mut read, "queued read").await;
    signals.release.notify_one();
    assert_eq!(read.await??.body, b"1");
    assert_eq!(signals.factories.load(Ordering::Relaxed), 2);

    // A real critical-section deadline destroys the root and child in either
    // drop order without leaving a savepoint or poisoning the next activation.
    let host = runtime.clone();
    let operation =
        cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", "/timeout").await });
    signals.started.notified().await;
    let host = runtime.clone();
    let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", "/read").await });
    waiting(&mut read, "queued read").await;
    assert!(operation
        .await?
        .err()
        .unwrap()
        .to_string()
        .contains("waited for too long"));
    assert!(read.await?.is_err());
    assert_eq!(
        fetch(&runtime, "Store:test", "/probe").await?.body,
        b"clean"
    );
    assert_eq!(signals.factories.load(Ordering::Relaxed), 3);

    assert!(fetch(&runtime, "Store:test", "/critical")
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("enclosing critical failure"));
    assert_eq!(
        fetch(&runtime, "Store:test", "/probe").await?.body,
        b"clean"
    );
    assert_eq!(signals.factories.load(Ordering::Relaxed), 4);

    let host = runtime.clone();
    let operation =
        cellar::asyncrt::spawn(async move { fetch(&host, "Store:test", "/retire").await });
    signals.started.notified().await;
    runtime.swap_out_cell("Store:test", 1).await?;
    runtime.start_cell("Store:test".into(), 1, false).await?;
    runtime.publish_cell("Store:test", 1)?;
    assert_eq!(
        fetch(&runtime, "Store:test", "/probe").await?.body,
        b"clean"
    );
    assert!(cellar::asyncrt::timeout(Duration::from_secs(2), operation)
        .await??
        .is_err());
    for scope in ["Store:test", "Store:other"] {
        runtime.swap_out_cell(scope, 1).await?;
    }
    shutdown.send(true)?;
    adapter.await?;
    Ok(())
}
