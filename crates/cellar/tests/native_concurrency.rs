// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host input gates through production native turns, without engine startup.

use anyhow::{anyhow, Result};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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

#[derive(Default)]
struct Signals {
    started: Notify,
    release: Notify,
    prior_started: Notify,
    prior_release: Notify,
    prior_resumed: AtomicBool,
    factories: AtomicUsize,
    old_storage: Mutex<Option<Storage>>,
    old_context: Mutex<Option<Context>>,
    initialized: AtomicBool,
    child_dropped: AtomicBool,
    invocations: AtomicUsize,
}

struct DropFlag(Arc<Signals>);

struct QueuedRpcBytes(Arc<AtomicBool>);

impl AsRef<[u8]> for QueuedRpcBytes {
    fn as_ref(&self) -> &[u8] {
        b"queued-rpc"
    }
}

impl Drop for QueuedRpcBytes {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.child_dropped.store(true, Ordering::Release);
    }
}

struct Counter {
    storage: Storage,
    signals: Arc<Signals>,
}

fn nested(context: Context, depth: usize) -> HandlerFuture<()> {
    context.block_concurrency_while(move |context| async move {
        if depth > 0 {
            nested(context, depth - 1).await?;
        }
        Ok(())
    })
}

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.signals.invocations.fetch_add(1, Ordering::Relaxed);
        assert!(
            self.signals.initialized.load(Ordering::Acquire),
            "even the synchronous invoke boundary waits for constructor blocks"
        );
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected fetch"));
            };
            match url::Url::parse(&request.url)?.path() {
                "/read" => {
                    assert!(self.signals.initialized.load(Ordering::Acquire));
                    Ok(response(
                        self.storage
                            .get_json("value")?
                            .unwrap_or(0.into())
                            .to_string(),
                    ))
                }
                "/hold" | "/fail" | "/panic" => {
                    let path = url::Url::parse(&request.url)?.path().to_string();
                    let signals = self.signals.clone();
                    context
                        .block_concurrency_while(move |context| async move {
                            let storage = context.storage()?;
                            storage.put_json("value", &7.into())?;
                            signals.started.notify_one();
                            signals.release.notified().await;
                            assert_eq!(storage.get_json("value")?, Some(7.into()));
                            assert!(path != "/panic", "critical callback panic");
                            if path == "/fail" {
                                return Err(anyhow!("critical callback failed"));
                            }
                            storage.put_json("value", &8.into())?;
                            Ok(())
                        })
                        .await?;
                    Ok(response("held"))
                }
                "/detached" | "/nested" => {
                    let is_nested = url::Url::parse(&request.url)?.path() == "/nested";
                    let signals = self.signals.clone();
                    let work = context.block_concurrency_while(move |context| async move {
                        let work = context.block_concurrency_while(move |context| async move {
                            signals.started.notify_one();
                            signals.release.notified().await;
                            context.storage()?.put_json("value", &9.into())?;
                            Ok(())
                        });
                        drop(work);
                        Ok(())
                    });
                    if is_nested {
                        work.await?;
                    } else {
                        drop(work);
                    }
                    Ok(response("nested"))
                }
                "/prior" => {
                    self.signals.prior_started.notify_one();
                    self.signals.prior_release.notified().await;
                    self.signals.prior_resumed.store(true, Ordering::Release);
                    Ok(response(
                        self.storage.get_json("value")?.unwrap().to_string(),
                    ))
                }
                "/remember" => {
                    *self.signals.old_storage.lock().unwrap() = Some(self.storage.clone());
                    *self.signals.old_context.lock().unwrap() = Some(context);
                    Ok(response("remembered"))
                }
                "/nested-fail" => {
                    let signals = self.signals.clone();
                    context
                        .block_concurrency_while(move |context| async move {
                            let failing = signals.clone();
                            drop(context.block_concurrency_while(move |_| async move {
                                failing.started.notify_one();
                                failing.release.notified().await;
                                Err::<(), _>(anyhow!("nested critical callback failed"))
                            }));
                            drop(context.block_concurrency_while(move |_| async move {
                                let _flag = DropFlag(signals);
                                futures_util::future::pending::<()>().await;
                                Ok(())
                            }));
                            Ok(())
                        })
                        .await?;
                    Ok(response("unexpected"))
                }
                "/timeout" => {
                    let signals = self.signals.clone();
                    context
                        .block_concurrency_while(move |_| async move {
                            signals.started.notify_one();
                            let _flag = DropFlag(signals);
                            futures_util::future::pending::<()>().await;
                            Ok(())
                        })
                        .await?;
                    Ok(response("unexpected"))
                }
                "/stale" => {
                    assert!(self
                        .signals
                        .old_storage
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .get_json("value")
                        .is_err());
                    assert!(self
                        .signals
                        .old_context
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .storage()
                        .is_err());
                    Ok(response("stale refused"))
                }
                "/depth" => {
                    nested(context, 64).await?;
                    Ok(response("unexpected"))
                }
                "/stateless" => unreachable!(),
                path => Err(anyhow!("unknown path {path}")),
            }
        })
    }
}

fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://cell{path}"),
        method: "GET".into(),
        headers: Vec::new(),
        body: Vec::new().into(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}

async fn fetch(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell("Counter:test".into(), None, request(path), None)
        .await
}

async fn assert_waiting<T>(future: &mut cellar::asyncrt::TaskHandle<T>) {
    assert!(cellar::asyncrt::timeout(Duration::from_millis(20), future)
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_critical_sections_retain_host_gate_and_activation_lifetimes() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(45), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let factory_signals = signals.clone();
    let registry = Registry::new(|context, _| {
        Box::pin(async move {
            assert!(context
                .block_concurrency_while(|_| async { Ok(()) })
                .await
                .is_err());
            Ok(response("stateless refused"))
        })
    })
    .cell("Counter", move |context, identity| {
        let first = factory_signals.factories.fetch_add(1, Ordering::AcqRel) == 0;
        let initialization = factory_signals.clone();
        if first {
            drop(context.block_concurrency_while(move |context| async move {
                initialization.started.notify_one();
                initialization.release.notified().await;
                context.storage()?.put_json("value", &1.into())?;
                initialization.initialized.store(true, Ordering::Release);
                Ok(())
            }));
        }
        if identity.scope == "Counter:broken" {
            drop(context.block_concurrency_while(|context| async move {
                drop(context.block_concurrency_while(|_| async {
                    Err::<(), _>(anyhow!("constructor critical section failed"))
                }));
                drop(context.block_concurrency_while(|_| async {
                    futures_util::future::pending::<()>().await;
                    Ok(())
                }));
                Ok(())
            }));
        }
        Ok(Arc::new(Counter {
            storage: context.storage()?,
            signals: factory_signals.clone(),
        }))
    })?;
    let graph = DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-gates".into(),
            do_classes: vec!["Counter".into()],
            bindings: vec![],
            vars: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            blob_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            node: "test".into(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: "native-gates".into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: Vec::new(),
    });
    let generation = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-gates".into(),
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
    runtime
        .restore_cell(
            "Counter:test",
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell("Counter:test".into(), 1, true).await?;
    runtime.publish_cell("Counter:test", 1)?;
    let (gates, mut requests) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let (shutdown, mut shut) = tokio::sync::watch::channel(false);
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(request) = cellar::asyncrt::select_biased! {
            "stop the native input-gate test's proof adapter";
            _=shut.changed()=>None,
            request=requests.recv()=>request,
        } {
            let _ = request.reply.send(Ok(()));
        }
    });
    let host = runtime.clone();
    let mut initializing = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
    signals.started.notified().await;
    assert_waiting(&mut initializing).await;
    signals.release.notify_one();
    assert_eq!(initializing.await??.body, b"1");
    assert_eq!(
        runtime
            .fetch_worker("http://worker/".into(), "GET".into(), vec![], vec![])
            .await?
            .body,
        b"stateless refused"
    );

    // A pre-existing event's continuation is held back as well as new input.
    let host = runtime.clone();
    let mut prior = cellar::asyncrt::spawn(async move { fetch(&host, "/prior").await });
    signals.prior_started.notified().await;
    let host = runtime.clone();
    let holding = cellar::asyncrt::spawn(async move { fetch(&host, "/hold").await });
    signals.started.notified().await;
    signals.prior_release.notify_one();
    assert_waiting(&mut prior).await;
    assert!(!signals.prior_resumed.load(Ordering::Acquire));
    let host = runtime.clone();
    let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
    assert_waiting(&mut read).await;
    signals.release.notify_one();
    assert_eq!(holding.await??.body, b"held");
    assert_eq!(prior.await??.body, b"8");
    assert_eq!(read.await??.body, b"8");

    // Dropping the result doesn't drop the block, and the outer block drains
    // a child started without awaiting it before answering its own caller.
    for path in ["/nested", "/detached"] {
        let host = runtime.clone();
        let mut nested = cellar::asyncrt::spawn(async move { fetch(&host, path).await });
        signals.started.notified().await;
        if path == "/nested" {
            assert_waiting(&mut nested).await;
        } else {
            assert_eq!((&mut nested).await??.body, b"nested");
        }
        let host = runtime.clone();
        let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
        assert_waiting(&mut read).await;
        signals.release.notify_one();
        if path == "/nested" {
            assert_eq!(nested.await??.body, b"nested");
        }
        assert_eq!(read.await??.body, b"9");
    }

    // Cancelling an RPC behind an unrelated input gate must drop its actual
    // queued payload and driver before that gate opens. A returned caller
    // error alone cannot prove admission/resource cleanup.
    let host = runtime.clone();
    let mut holding = cellar::asyncrt::spawn(async move { fetch(&host, "/hold").await });
    signals.started.notified().await;
    let entered = signals.invocations.load(Ordering::Acquire);
    let dropped = Arc::new(AtomicBool::new(false));
    let payload = bytes::Bytes::from_owner(QueuedRpcBytes(dropped.clone()));
    let (cancel_rpc, cancelled_rpc) = tokio::sync::oneshot::channel();
    let host = runtime.clone();
    let mut queued = cellar::asyncrt::spawn(async move {
        host.rpc_cancellable(
            "Counter:test".into(),
            None,
            "never-entered".into(),
            cellar::engine_api::RpcData::Structured(payload),
            Some(cellar::engine_api::next_request_id()),
            Some(cancelled_rpc),
        )
        .await
    });
    assert_waiting(&mut queued).await;
    cancel_rpc.send(()).unwrap();
    assert!(queued.await?.is_err());
    cellar::asyncrt::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::Acquire) {
            cellar::asyncrt::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("cancelled RPC retained its queued payload behind a live gate"))?;
    assert_waiting(&mut holding).await;
    assert_eq!(signals.invocations.load(Ordering::Acquire), entered);
    signals.release.notify_one();
    assert_eq!(holding.await??.body, b"held");

    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let host = runtime.clone();
    let holding = cellar::asyncrt::spawn(async move {
        host.fetch_cell(
            "Counter:test".into(),
            None,
            request("/hold"),
            Some(cancelled),
        )
        .await
    });
    signals.started.notified().await;
    cancel.send(()).unwrap();
    assert!(holding.await?.is_err());
    let host = runtime.clone();
    let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
    assert_waiting(&mut read).await;
    signals.release.notify_one();
    assert_eq!(read.await??.body, b"8");
    assert_eq!(signals.factories.load(Ordering::Acquire), 1);

    for path in ["/fail", "/panic"] {
        fetch(&runtime, "/remember").await?;
        let host = runtime.clone();
        let holding = cellar::asyncrt::spawn(async move { fetch(&host, path).await });
        signals.started.notified().await;
        let host = runtime.clone();
        let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
        assert_waiting(&mut read).await;
        signals.release.notify_one();
        let error = holding.await?.err().expect("failed block rejects");
        assert!(error.to_string().contains("critical callback"));
        assert!(read
            .await?
            .err()
            .expect("queued event must be refused")
            .to_string()
            .contains("critical callback"));
        assert_eq!(fetch(&runtime, "/read").await?.body, b"7");
        assert_eq!(fetch(&runtime, "/stale").await?.body, b"stale refused");
    }
    assert_eq!(signals.factories.load(Ordering::Acquire), 3);
    assert!(fetch(&runtime, "/depth")
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("nested too deeply"));
    assert_eq!(fetch(&runtime, "/read").await?.body, b"7");
    assert_eq!(signals.factories.load(Ordering::Acquire), 4);

    runtime
        .restore_cell(
            "Counter:broken",
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell("Counter:broken".into(), 1, true).await?;
    runtime.publish_cell("Counter:broken", 1)?;
    let invocations = signals.invocations.load(Ordering::Relaxed);
    let broken = runtime
        .fetch_cell("Counter:broken".into(), None, request("/read"), None)
        .await
        .err()
        .unwrap();
    assert!(broken
        .to_string()
        .contains("constructor critical section failed"));
    assert_eq!(signals.invocations.load(Ordering::Relaxed), invocations);
    runtime.swap_out_cell("Counter:broken", 1).await?;

    // Failure opens the gate immediately even with an unwakeable sibling.
    let host = runtime.clone();
    let failing = cellar::asyncrt::spawn(async move { fetch(&host, "/nested-fail").await });
    signals.started.notified().await;
    let host = runtime.clone();
    let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
    assert_waiting(&mut read).await;
    signals.release.notify_one();
    assert!(failing
        .await?
        .err()
        .unwrap()
        .to_string()
        .contains("nested critical callback failed"));
    assert!(read.await?.is_err());
    assert!(signals.child_dropped.load(Ordering::Acquire));
    assert_eq!(fetch(&runtime, "/read").await?.body, b"7");

    // A real domain timer must resume an unwakeable callback and reset it.
    signals.child_dropped.store(false, Ordering::Release);
    let host = runtime.clone();
    let timed = cellar::asyncrt::spawn(async move { fetch(&host, "/timeout").await });
    signals.started.notified().await;
    let host = runtime.clone();
    let mut read = cellar::asyncrt::spawn(async move { fetch(&host, "/read").await });
    assert_waiting(&mut read).await;
    assert!(timed
        .await?
        .err()
        .unwrap()
        .to_string()
        .contains("waited for too long"));
    assert!(read.await?.is_err());
    assert!(signals.child_dropped.load(Ordering::Acquire));
    assert_eq!(fetch(&runtime, "/read").await?.body, b"7");

    // Retirement releases the process gate before a same-epoch reopen,
    // without waiting for the old callback's I/O or its timer.
    let host = runtime.clone();
    let old = cellar::asyncrt::spawn(async move { fetch(&host, "/hold").await });
    signals.started.notified().await;
    runtime.swap_out_cell("Counter:test", 1).await?;
    runtime.start_cell("Counter:test".into(), 1, false).await?;
    runtime.publish_cell("Counter:test", 1)?;
    assert_eq!(fetch(&runtime, "/read").await?.body, b"7");
    assert!(cellar::asyncrt::timeout(Duration::from_secs(2), old)
        .await??
        .is_err());
    runtime.swap_out_cell("Counter:test", 1).await?;
    shutdown.send(true)?;
    adapter.await?;
    Ok(())
}
