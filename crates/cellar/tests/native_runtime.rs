use anyhow::{anyhow, Result};
use cellar::application::{
    Cell, Context, HandlerFuture, Invocation, Output, Registry, Request, Storage,
};
use cellar::engine_api::{
    Compat, HttpResponse, RpcData, RuntimeFetch, RuntimeOptions, WorkerConfigOptions,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use futures_util::TryStreamExt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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

fn complex_value() -> cellar::native::clone::StructuredValue {
    use cellar::native::clone::{Object, StructuredValue, Value, ViewKind};
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                ("self".into(), Value::Reference(0)),
                ("text".into(), Value::String(vec![0xd800, 0x96ea])),
                ("view".into(), Value::Reference(2)),
            ]),
            Object::ArrayBuffer {
                bytes: vec![1, 2, 3, 4, 5, 6, 7, 8],
                max_length: None,
                immutable: false,
            },
            Object::View {
                buffer: 1,
                kind: ViewKind::Uint16,
                offset: 2,
                length: 4,
                flags: 0,
            },
        ],
    }
}

#[derive(Default)]
struct Signals {
    started: Notify,
    release: Notify,
    background_finished: Notify,
    captured: Mutex<Option<Storage>>,
    factories: AtomicUsize,
    polls: AtomicUsize,
    dropped: AtomicBool,
    retired_resumed: AtomicBool,
}

struct DropFlag(Arc<Signals>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
    }
}

struct Counter(Arc<Signals>, Storage);

struct TrackedBody {
    chunks: std::collections::VecDeque<Vec<u8>>,
    dropped: Arc<AtomicBool>,
}

impl Drop for TrackedBody {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl futures_util::Stream for TrackedBody {
    type Item = std::result::Result<Vec<u8>, String>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.chunks.pop_front().map(Ok))
    }
}

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = self.1.clone();
            *self.0.captured.lock().unwrap() = Some(storage.clone());
            match invocation {
                Invocation::Alarm { .. } => {
                    assert_eq!(storage.get_alarm()?, None);
                    storage.put_json("fired", &true.into())?;
                    Ok(Output::Done)
                }
                Invocation::CellRpc { args, .. } => Ok(Output::Rpc(args)),
                Invocation::Fetch(request) => {
                    let path = url::Url::parse(&request.url)?.path().to_string();
                    match path.as_str() {
                        "/write" | "/suspend" | "/fail" | "/panic" => {
                            let value = storage
                                .get_json("count")?
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                                + 1;
                            storage.put_json("count", &value.into())?;
                            if path == "/suspend" {
                                self.0.started.notify_one();
                                self.0.release.notified().await;
                                // Resuming on another worker must install this cell again.
                                assert_eq!(storage.get_json("count")?, Some(value.into()));
                            }
                            if path == "/fail" {
                                return Err(anyhow!("failed after commit"));
                            }
                            assert!(path != "/panic", "panic after commit");
                            Ok(response(value.to_string()))
                        }
                        "/cancel" => {
                            let signals = self.0.clone();
                            let background_storage = storage.clone();
                            context.wait_until(async move {
                                signals.release.notified().await;
                                background_storage.put_json("background", &true.into())?;
                                signals.background_finished.notify_one();
                                Ok(())
                            })?;
                            let guard = DropFlag(self.0.clone());
                            self.0.started.notify_one();
                            std::future::poll_fn(|_| {
                                self.0.polls.fetch_add(1, Ordering::SeqCst);
                                std::task::Poll::<()>::Pending
                            })
                            .await;
                            drop(guard);
                            unreachable!()
                        }
                        "/retire" => {
                            let signals = self.0.clone();
                            context.wait_until(async move {
                                let guard = DropFlag(signals.clone());
                                signals.started.notify_one();
                                signals.release.notified().await;
                                signals.retired_resumed.store(true, Ordering::SeqCst);
                                storage.put_json("stale", &true.into())?;
                                drop(guard);
                                Ok(())
                            })?;
                            Ok(response("background"))
                        }
                        "/body" => {
                            let mut stream = context.take_body(request.body)?;
                            let mut bytes = Vec::new();
                            while let Some(chunk) =
                                stream.try_next().await.map_err(anyhow::Error::msg)?
                            {
                                bytes.extend(chunk);
                            }
                            Ok(response(bytes))
                        }
                        "/clone" => {
                            let value = complex_value();
                            if storage.get_clone("complex")?.is_none() {
                                storage.put_clone("complex", &value)?;
                            }
                            let clone = storage.get_clone("complex")?.unwrap();
                            assert_eq!(clone.value, value);
                            assert!(!clone.stored_stubs);
                            assert!(storage.get_json("complex").is_err());
                            assert!(
                                matches!(storage.get("complex")?, Some(cellar::storage::StoredValue::Structured(bytes)) if bytes.starts_with(&[0xff,15]))
                            );
                            storage.put_stored_clone(
                                "tagged",
                                &cellar::application::StoredClone {
                                    value: value.clone(),
                                    stored_stubs: true,
                                },
                            )?;
                            let tagged = storage.get_clone("tagged")?.unwrap();
                            assert!(tagged.stored_stubs);
                            assert_eq!(tagged.value, value);
                            assert!(matches!(
                                storage.get_clone("count")?.unwrap().value.root,
                                cellar::native::clone::Value::Number(4.0)
                            ));
                            Ok(response("clone"))
                        }
                        "/proxy" => {
                            storage.put_json("forwarded", &true.into())?;
                            Ok(Output::Http(
                                context
                                    .fetch_cell(
                                        "Counter:test".into(),
                                        None,
                                        Request {
                                            url: "http://cell/read".into(),
                                            method: "GET".into(),
                                            headers: Vec::new(),
                                            body: Vec::new().into(),
                                            entrypoint: None,
                                        },
                                    )
                                    .await?,
                            ))
                        }
                        "/arm" => {
                            storage.set_alarm(0)?;
                            Ok(response("armed"))
                        }
                        "/background" => Ok(response(
                            storage
                                .get_json("background")?
                                .unwrap_or(false.into())
                                .to_string(),
                        )),
                        "/fired" => Ok(response(
                            storage
                                .get_json("fired")?
                                .unwrap_or(false.into())
                                .to_string(),
                        )),
                        "/sql" => {
                            storage
                                .sql("CREATE TABLE IF NOT EXISTS counter(value INTEGER)", &[])?;
                            storage.sql("INSERT INTO counter VALUES (?)", &[7.into()])?;
                            let (_, rows, _) =
                                storage.sql("SELECT sum(value) FROM counter", &[])?;
                            Ok(response(rows[0][0].to_string()))
                        }
                        _ => Ok(response(
                            storage.get_json("count")?.unwrap_or(0.into()).to_string(),
                        )),
                    }
                }
                _ => Err(anyhow!("unexpected test event")),
            }
        })
    }
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-test".into(),
            do_classes: vec!["Counter".into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: vec![("GREETING".into(), "native".into())],
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "native-test".into(),
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

async fn fetch(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            "Counter:test".into(),
            Some("named".into()),
            request(path),
            None,
        )
        .await
}

// A single runtime owns the process execution domain. Crucially, this test
// never calls Engine::init or Generation::build (the JavaScript path).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compiled_cells_use_the_production_runtime_without_initializing_v8() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let signals = Arc::new(Signals::default());
    assert!(Generation::build_native(
        0,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into()
        },
        Default::default()
    )
    .is_err());
    let factory_signals = signals.clone();
    let registry = Registry::new(|context, invocation| {
        Box::pin(async move {
            if let Invocation::Fetch(request) = invocation {
                if url::Url::parse(&request.url)?.path() == "/proxy" {
                    return Ok(Output::Http(
                        context
                            .fetch_cell(
                                "Counter:test".into(),
                                None,
                                Request {
                                    url: "http://cell/read".into(),
                                    method: "GET".into(),
                                    headers: Vec::new(),
                                    body: request.body,
                                    entrypoint: None,
                                },
                            )
                            .await?,
                    ));
                }
            }
            Ok(response(
                context.var("GREETING").unwrap_or("missing").to_string(),
            ))
        })
    })
    .cell("Counter", move |context, identity| {
        assert_eq!(identity.class, "Counter");
        factory_signals.factories.fetch_add(1, Ordering::SeqCst);
        if let Some(previous) = factory_signals.captured.lock().unwrap().as_ref() {
            assert!(
                previous.get_json("count").is_err(),
                "retired storage cannot borrow the next opening at the same epoch"
            );
        }
        Ok(Arc::new(Counter(
            factory_signals.clone(),
            context.storage()?,
        )))
    })?;
    let applications = std::collections::HashMap::from([(
        "native-test".into(),
        Arc::new(registry) as Arc<dyn cellar::application::Application>,
    )]);
    let mut duplicate = graph();
    duplicate.cohosted.push(graph().primary);
    assert!(Generation::build_native(
        0,
        duplicate,
        GenerationOptions {
            node: "test".into(),
            region: "test".into()
        },
        applications.clone()
    )
    .is_err());
    let generation = Generation::build_native(
        1,
        graph(),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        applications,
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
        b"native"
    );
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
    let (calls, mut called) = tokio::sync::mpsc::unbounded_channel();
    let (shutdown, mut gate_shutdown) = tokio::sync::watch::channel(false);
    let mut router_shutdown = gate_shutdown.clone();
    cellar::host_channels::set_do_call_tx(calls);
    let routing_runtime = runtime.clone();
    let router = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {
            "test shutdown closes the host router before accepting new calls";
            _ = router_shutdown.changed() => None,
            call = called.recv() => call,
        } {
            let result = routing_runtime
                .fetch_cell(
                    call.scope,
                    call.name,
                    RuntimeFetch {
                        url: call.url,
                        method: call.method,
                        body: call.body,
                        headers: call.headers,
                        request_id: call.request_id,
                        order: call.order,
                        parent: call.parent,
                    },
                    call.cancel,
                )
                .await;
            let _ = call.reply.send(result);
        }
    });
    let (gates, mut gated) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let collected_tickets = tickets.clone();
    let gate_router = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {
            "test shutdown closes the gate adapter before accepting new samples";
            _ = gate_shutdown.changed() => None,
            gate = gated.recv() => gate,
        } {
            collected_tickets.lock().unwrap().push(gate.ticket);
            let _ = gate.reply.send(Ok(()));
        }
    });
    let first = fetch(&runtime, "/write").await?;
    assert_eq!(first.body, b"1");
    assert!(first.write_position.is_some());
    assert!(signals
        .captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .get_json("count")
        .is_err());

    let waiting_runtime = runtime.clone();
    let suspended =
        cellar::asyncrt::spawn(async move { fetch(&waiting_runtime, "/suspend").await });
    signals.started.notified().await;
    let read = fetch(&runtime, "/read").await?;
    assert_eq!(read.body, b"2");
    assert_eq!(read.write_position, None);
    assert!(read.observed_position.is_some());
    signals.release.notify_one();
    assert_eq!(suspended.await??.body, b"2");

    let failure = match fetch(&runtime, "/fail").await {
        Err(error) => error,
        Ok(_) => panic!("failure returned success"),
    };
    assert!(failure.to_string().contains("failed after commit"));
    assert!(cellar::engine_api::failed_write_position(&failure).is_some());
    let panic = match fetch(&runtime, "/panic").await {
        Err(error) => error,
        Ok(_) => panic!("panic returned success"),
    };
    assert!(panic
        .to_string()
        .contains("native handler panicked: panic after commit"));
    assert!(cellar::engine_api::failed_write_position(&panic).is_some());
    assert_eq!(fetch(&runtime, "/read").await?.body, b"4");
    assert_eq!(fetch(&runtime, "/sql").await?.body, b"7");
    assert_eq!(fetch(&runtime, "/clone").await?.body, b"clone");
    let dropped_body = Arc::new(AtomicBool::new(false));
    let source = TrackedBody {
        chunks: std::collections::VecDeque::from([b"part".to_vec(), b"s".to_vec()]),
        dropped: dropped_body.clone(),
    };
    let stream_id =
        cellar::http_streams::register_body_stream(Box::pin(source)).map_err(anyhow::Error::msg)?;
    let mut streamed = request("/body");
    streamed.body = cellar::engine_api::RequestBody::Stream(stream_id);
    assert_eq!(
        runtime
            .fetch_cell("Counter:test".into(), None, streamed, None)
            .await?
            .body,
        b"parts"
    );
    assert!(dropped_body.load(Ordering::SeqCst));
    assert_eq!(
        runtime
            .fetch_worker(
                "http://worker/proxy".into(),
                "GET".into(),
                Vec::new(),
                Vec::new()
            )
            .await?
            .body,
        b"4"
    );
    assert_eq!(fetch(&runtime, "/proxy").await?.body, b"4");
    let ticket = tickets.lock().unwrap()[0];
    assert_eq!(ticket.channel, cellar_logic::Channel::CellRpc);
    assert_eq!(ticket.epoch, Some(1));
    assert!(ticket.position.is_some());

    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let cancelling_runtime = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move {
        cancelling_runtime
            .fetch_cell(
                "Counter:test".into(),
                None,
                request("/cancel"),
                Some(cancelled),
            )
            .await
    });
    signals.started.notified().await;
    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        signals.polls.load(Ordering::SeqCst),
        1,
        "pending without a waker must not spin"
    );
    cancel.send(()).unwrap();
    assert!(pending.await?.is_err());
    signals.release.notify_one();
    cellar::asyncrt::timeout(
        Duration::from_secs(2),
        signals.background_finished.notified(),
    )
    .await?;
    assert!(signals.dropped.load(Ordering::SeqCst));
    assert_eq!(fetch(&runtime, "/background").await?.body, b"true");

    assert_eq!(fetch(&runtime, "/arm").await?.body, b"armed");
    // The driver reports turn-end alarm moves after the handler sends its
    // reply. Observe that publication before invoking the scheduler.
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while runtime.alarm("Counter:test") != Some(0) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let services = cellar::asyncrt::services();
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while services.application_work().pending_events != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let before = services.application_work();
    let queued_alarm = runtime.fire_alarm(1, "Counter:test".into(), 0);
    assert_eq!(services.application_work().pending_events, 1);
    let foreign = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    foreign.run(async move { drop(queued_alarm) }).await?;
    assert_eq!(services.application_work().pending_events, 0);
    assert_eq!(
        services.application_work().retired_events,
        before.retired_events + 1
    );
    assert_eq!(foreign.services().application_work().pending_events, 0);
    assert_eq!(foreign.services().application_work().retired_events, 0);
    assert_eq!(
        runtime.alarm("Counter:test"),
        Some(0),
        "an unpolled alarm consumed its claim"
    );
    foreign.shutdown().await;
    let (_, _, alarm_write) = runtime.fire_alarm(1, "Counter:test".into(), 0).await?;
    assert!(alarm_write.is_some());
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while runtime.alarm("Counter:test").is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(fetch(&runtime, "/fired").await?.body, b"true");
    let rpc = runtime
        .rpc(
            "Counter:test".into(),
            None,
            "echo".into(),
            RpcData::Json("[1]".into()),
            None,
        )
        .await?;
    assert!(matches!(rpc.data, RpcData::Json(ref value) if value == "[1]"));
    let clone_rpc = runtime
        .rpc(
            "Counter:test".into(),
            None,
            "echo".into(),
            cellar::application::encode_rpc(&complex_value())?,
            None,
        )
        .await?;
    assert_eq!(
        cellar::application::decode_rpc(&clone_rpc.data)?,
        complex_value()
    );

    assert_eq!(signals.factories.load(Ordering::SeqCst), 1);
    signals.dropped.store(false, Ordering::SeqCst);
    assert_eq!(fetch(&runtime, "/retire").await?.body, b"background");
    signals.started.notified().await;
    runtime.swap_out_cell("Counter:test", 1).await?;
    runtime.start_cell("Counter:test".into(), 1, false).await?;
    runtime.publish_cell("Counter:test", 1)?;
    assert_eq!(fetch(&runtime, "/read").await?.body, b"4");
    assert_eq!(fetch(&runtime, "/clone").await?.body, b"clone");
    signals.release.notify_one();
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while !signals.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!signals.retired_resumed.load(Ordering::SeqCst));
    assert_eq!(signals.factories.load(Ordering::SeqCst), 2);
    runtime.swap_out_cell("Counter:test", 1).await?;
    shutdown.send(true)?;
    router.await?;
    gate_router.await?;
    Ok(())
}
