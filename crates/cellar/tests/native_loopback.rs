//! Loaded loopback capabilities keep their loader lifetime and host authority.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::application::http::{Headers, OutboundRequest, Response};
use cellar::application::loader::{Outbound, Worker, WorkerCode};
use cellar::application::service::Fetcher;
use cellar::application::streams::FixedLengthStream;
use cellar::application::{
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use cellar::engine_api::{HttpResponse, RuntimeFetch};
use cellar::generation::Generation;
use cellar::host_channels::set_gate_tx;
use cellar::native::clone::{decode, encode, Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

fn properties() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                (Value::from("self"), Value::Reference(0)),
                (Value::from("bytes"), Value::Reference(1)),
            ]),
            Object::ArrayBuffer {
                bytes: vec![1, 2, 3],
                max_length: None,
                immutable: false,
            },
        ],
    }
}

fn request(path: &str) -> Request {
    Request {
        url: format!("http://loopback/{path}"),
        method: "POST".into(),
        headers: Vec::new(),
        body: Vec::new().into(),
        entrypoint: None,
    }
}

fn output(body: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: body.as_bytes().to_vec(),
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

#[derive(Default)]
struct Signals {
    codes: AtomicUsize,
    app_drops: AtomicUsize,
    pending_drops: AtomicUsize,
    echo_calls: AtomicUsize,
    echo: Mutex<Option<Fetcher>>,
    old_context: Mutex<Option<Context>>,
    hold: AtomicBool,
    pending: Notify,
    cancel: Notify,
    dropped: Notify,
    app_dropped: Notify,
}

struct AppLifetime(Arc<Signals>);

impl Drop for AppLifetime {
    fn drop(&mut self) {
        assert!(cellar::host_loader::loader_registry().try_lock().is_ok());
        self.0.app_drops.fetch_add(1, Ordering::SeqCst);
        self.0.app_dropped.notify_one();
    }
}

struct Pending(Arc<Signals>);

impl Drop for Pending {
    fn drop(&mut self) {
        self.0.pending_drops.fetch_add(1, Ordering::SeqCst);
        self.0.dropped.notify_one();
    }
}

fn application(signals: Arc<Signals>) -> Arc<dyn Application> {
    let lifetime = Arc::new(AppLifetime(signals));
    let handler = Arc::new(
        move |context: Context, invocation: Invocation| -> HandlerFuture<Output> {
            let lifetime = lifetime.clone();
            Box::pin(async move {
                assert!(context.identity().is_none());
                assert!(context.loader("WORKERS").is_err());
                match invocation {
                    Invocation::Fetch(incoming) => {
                        let name = incoming
                            .entrypoint
                            .as_ref()
                            .map_or("default", |e| e.name.as_str());
                        match name {
                            "main" => {
                                assert!(incoming.url.ends_with("seed"));
                                let echo = context
                                    .exported_entrypoint("echo")?
                                    .with_props(&properties())?;
                                // The target handle selects the entrypoint and
                                // props even when forwarding a named request.
                                assert_eq!(echo.fetch(&context, incoming).await?.body, b"echo");
                                assert_eq!(
                                    context
                                        .exported_entrypoint("default")?
                                        .fetch(&context, request("default"))
                                        .await?
                                        .body,
                                    b"default"
                                );
                                *lifetime.0.echo.lock().unwrap() = Some(echo);
                                *lifetime.0.old_context.lock().unwrap() = Some(context.clone());
                                Ok(output("seeded"))
                            }
                            "echo" => {
                                assert_eq!(
                                    decode(&incoming.entrypoint.as_ref().unwrap().props)?,
                                    properties()
                                );
                                lifetime.0.echo_calls.fetch_add(1, Ordering::SeqCst);
                                if incoming.url.ends_with("pending") {
                                    let _pending = Pending(lifetime.0.clone());
                                    lifetime.0.pending.notify_one();
                                    futures_util::future::pending::<()>().await;
                                    unreachable!()
                                }
                                if incoming.url.ends_with("stream") {
                                    let mut reader = incoming.into_body_reader(&context)?;
                                    let bytes = reader.bytes(&context).await?;
                                    let pipe = FixedLengthStream::new(bytes.len() as f64)?;
                                    let writer = pipe.writable.get_writer()?;
                                    context.wait_until_byte_stream(&pipe.readable, async move {
                                        writer.write(bytes).await?;
                                        writer.close().await?;
                                        Ok(())
                                    })?;
                                    return Ok(Output::Http(
                                        Response::from_byte_stream(
                                            200,
                                            &pipe.readable,
                                            &Headers::new(),
                                        )?
                                        .into_http(&context)?,
                                    ));
                                }
                                Ok(output("echo"))
                            }
                            "default" => Ok(output("default")),
                            _ => Err(anyhow!("unexpected entrypoint {name}")),
                        }
                    }
                    Invocation::Rpc {
                        entrypoint,
                        props,
                        operation,
                    } => {
                        assert_eq!(entrypoint, "echo");
                        assert_eq!(decode(&props)?, properties());
                        match operation {
                            cellar::WorkerRpcOperation::Get { path } => {
                                assert_eq!(path, vec!["constant"]);
                                Ok(Output::Bytes(encode(&properties())?))
                            }
                            cellar::WorkerRpcOperation::Call { path, args } => {
                                assert_eq!(path, vec!["echo"]);
                                assert_eq!(decode(&args)?, properties());
                                Ok(Output::Bytes(args))
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            })
        },
    );
    let main = handler.clone();
    Arc::new(
        Registry::new(|_, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    unreachable!()
                };
                assert!(request
                    .entrypoint
                    .as_ref()
                    .is_none_or(|entry| entry.name == "default"));
                Ok(output("default"))
            })
        })
        .entrypoint("main", move |context, invocation| {
            assert!(matches!(&invocation, Invocation::Fetch(request) if request.entrypoint.as_ref().is_some_and(|e| e.name == "main")));
            main(context, invocation)
        })
        .unwrap()
        .entrypoint("echo", move |context, invocation| {
            assert!(matches!(&invocation, Invocation::Fetch(request) if request.entrypoint.as_ref().is_some_and(|e| e.name == "echo"))
                || matches!(&invocation, Invocation::Rpc { entrypoint, .. } if entrypoint == "echo"));
            handler(context, invocation)
        })
        .unwrap(),
    )
}

fn stream_request(context: &Context) -> Result<Request> {
    let pipe = FixedLengthStream::new(6.0)?;
    let writer = pipe.writable.get_writer()?;
    context.wait_until_byte_stream(&pipe.readable, async move {
        writer.write(b"abc".to_vec()).await?;
        writer.write(b"def".to_vec()).await?;
        writer.close().await?;
        Ok(())
    })?;
    Request::with_byte_stream(
        context,
        "http://loopback/stream",
        "post",
        &Headers::new(),
        &pipe.readable,
    )
}

struct Root(Arc<Signals>);

impl Root {
    fn worker(&self, context: &Context) -> Result<Worker> {
        let signals = self.0.clone();
        context.loader("WORKERS")?.get(context, "shared", move |_| {
            Box::pin(async move {
                signals.codes.fetch_add(1, Ordering::SeqCst);
                Ok(WorkerCode::new(application(signals)))
            })
        })
    }

    fn echo(&self) -> Fetcher {
        self.0.echo.lock().unwrap().clone().unwrap()
    }

    fn consumer(&self, context: &Context, mode: &str) -> Result<Worker> {
        let signals = self.0.clone();
        let retained_drops = signals.app_drops.load(Ordering::SeqCst);
        let mode_copy = mode.to_string();
        let mut code = WorkerCode::new(Arc::new(Registry::new(move |context, _| {
            let mode = mode_copy.clone();
            let signals = signals.clone();
            Box::pin(async move {
                if mode == "cancel-broker" {
                    return Ok(Output::Http(
                        context
                            .fetch(OutboundRequest::get("http://local/pending"))
                            .await?,
                    ));
                }
                let outgoing = stream_request(&context)?;
                let result = if mode == "env" {
                    let graph = context.loaded_environment()?.unwrap();
                    let target = context.environment_fetcher(&graph.root)?;
                    target.fetch(&context, outgoing).await?
                } else {
                    context
                        .fetch(OutboundRequest {
                            url: outgoing.url,
                            method: outgoing.method,
                            headers: outgoing.headers,
                            body: Some(outgoing.body),
                            redirect: Default::default(),
                        })
                        .await?
                };
                let mut result = Response::from_http(result);
                assert_eq!(result.text(&context).await?, "abcdef");
                // A supplied local broker consumes exactly the one fetch slot.
                let error = context
                    .fetch(OutboundRequest::get("http://unused.invalid/"))
                    .await
                    .err()
                    .unwrap();
                assert!(error.to_string().contains("subrequest limit of 1"));
                assert_eq!(signals.app_drops.load(Ordering::SeqCst), retained_drops);
                Ok(output(&mode))
            })
        })));
        code.limits = Some(cellar::WorkerInvocationLimits {
            cpu_ms: None,
            sub_requests: Some(1),
        });
        if mode == "env" {
            code.outbound = Outbound::Deny;
            let marker = code.environment_fetcher(self.echo())?;
            code.environment.as_mut().unwrap().root = marker;
        } else {
            code.outbound = Outbound::Broker(self.echo());
        }
        context.loader("WORKERS")?.load(context, code)
    }
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(incoming) = invocation else {
                unreachable!()
            };
            match incoming.url.rsplit('/').next().unwrap() {
                "seed" => Ok(Output::Http(
                    self.worker(&context)?
                        .entrypoint("main")
                        .fetch(&context, request("seed"))
                        .await?,
                )),
                "cached" => {
                    let worker = context.loader("WORKERS")?.get(&context, "shared", |_| {
                        Box::pin(async {
                            panic!("a retained local capability must keep the named load")
                        })
                    })?;
                    assert_eq!(
                        worker
                            .entrypoint("echo")
                            .with_props(&properties())?
                            .fetch(&context, request("echo"))
                            .await?
                            .body,
                        b"echo"
                    );
                    Ok(output("cached"))
                }
                "fetch" | "gated" => {
                    if incoming.url.ends_with("gated") {
                        context
                            .storage()?
                            .put_json("written", &serde_json::json!(true))?;
                    }
                    Ok(Output::Http(
                        self.echo().fetch(&context, request("echo")).await?,
                    ))
                }
                "rpc" => {
                    let echo = self.echo();
                    for operation in [
                        cellar::WorkerRpcOperation::Get {
                            path: vec!["constant".into()],
                        },
                        cellar::WorkerRpcOperation::Call {
                            path: vec!["echo".into()],
                            args: encode(&properties())?,
                        },
                    ] {
                        assert_eq!(decode(&echo.rpc(&context, operation).await?)?, properties());
                    }
                    Ok(output("rpc"))
                }
                "missing" => {
                    let worker = self.worker(&context)?;
                    let error = worker
                        .entrypoint("missing")
                        .fetch(&context, request("missing"))
                        .await
                        .err()
                        .unwrap();
                    assert_eq!(error.to_string(), "no native entrypoint named missing");
                    let error = worker
                        .entrypoint("missing")
                        .rpc(
                            &context,
                            cellar::WorkerRpcOperation::Get { path: Vec::new() },
                        )
                        .await
                        .err()
                        .unwrap();
                    assert_eq!(error.to_string(), "no native entrypoint named missing");
                    Ok(output("missing"))
                }
                "stale" => {
                    let old = self.0.old_context.lock().unwrap().take().unwrap();
                    assert!(old.exported_entrypoint("echo").is_err());
                    assert!(self.echo().fetch(&old, request("echo")).await.is_err());
                    Ok(output("stale"))
                }
                "env" | "broker" => {
                    let mode = incoming.url.rsplit('/').next().unwrap();
                    Ok(Output::Http(
                        self.consumer(&context, mode)?
                            .entrypoint("default")
                            .fetch(&context, request(mode))
                            .await?,
                    ))
                }
                "env-retain" | "broker-retain" => {
                    let mode = if incoming.url.ends_with("env-retain") {
                        "env"
                    } else {
                        "broker"
                    };
                    let consumer = self.consumer(&context, mode)?;
                    drop(self.0.echo.lock().unwrap().take());
                    // Only the supplied capability in the pending code/worker
                    // configuration now keeps the original application alive.
                    Ok(Output::Http(
                        consumer
                            .entrypoint("default")
                            .fetch(&context, request(mode))
                            .await?,
                    ))
                }
                "null-env-release" => {
                    let mut code = WorkerCode::new(Arc::new(Registry::new(|context, _| {
                        Box::pin(async move {
                            assert!(context.loaded_environment()?.is_none());
                            Ok(output("null"))
                        })
                    })));
                    code.environment_fetcher(self.echo())?;
                    code.environment = Some(StructuredValue {
                        root: Value::Null,
                        objects: Vec::new(),
                    });
                    let consumer = context.loader("WORKERS")?.load(&context, code)?;
                    drop(self.0.echo.lock().unwrap().take());
                    Ok(Output::Http(
                        consumer
                            .entrypoint("default")
                            .fetch(&context, request("null"))
                            .await?,
                    ))
                }
                "cancel" | "cancel-broker" => {
                    let controller = cellar::native::web::AbortController::new();
                    let call = if incoming.url.ends_with("cancel-broker") {
                        self.consumer(&context, "cancel-broker")?
                            .entrypoint("default")
                            .fetch(&context, request("pending"))
                    } else {
                        self.echo().fetch_with_signal(
                            &context,
                            request("pending"),
                            controller.signal(),
                        )
                    };
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! {"caller cancels a local capability and its broker chain";
                        _=self.0.cancel.notified()=>{}, _=&mut call=>return Err(anyhow!("pending local target answered")),
                    }
                    if incoming.url.ends_with("cancel-broker") {
                        // Leaving this event drops the consumer wait, whose
                        // handler drops its pending local broker wait.
                        return Ok(output("cancelled"));
                    }
                    controller.abort(None)?;
                    assert!(call.await.is_err());
                    Ok(output("cancelled"))
                }
                "dispose-captured" => {
                    let worker = self.worker(&context)?;
                    let echo = self.echo();
                    let captured = echo.fetch(&context, request("echo"));
                    worker.dispose(&context)?;
                    assert_eq!(captured.await?.body, b"echo");
                    assert!(echo.fetch(&context, request("echo")).await.is_err());
                    Ok(output("disposed"))
                }
                _ => unreachable!(),
            }
        })
    }
}

async fn call(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            "Root:first".into(),
            None,
            RuntimeFetch {
                url: format!("http://root/{path}"),
                method: "POST".into(),
                headers: Vec::new(),
                body: Vec::new().into(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_loopback_lifetimes_env_brokers_streams_cancellation_and_disposal() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let parents = signals.clone();
    let app = Registry::new(|_, _| Box::pin(async { Ok(output("default")) }))
        .cell("Root", move |_, _| Ok(Arc::new(Root(parents.clone()))))?;
    let mut graph = fixture::graph(
        "native-loopback",
        "this is not JavaScript",
        vec!["Root".into()],
    );
    graph.primary.loader_bindings = vec!["WORKERS".into()];
    let generation = Generation::build_native(
        1,
        graph,
        fixture::generation_options(),
        std::collections::HashMap::from([(
            "native-loopback".into(),
            Arc::new(app) as Arc<dyn Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = Arc::new(RuntimeManager::start(
        generation,
        fixture::runtime_options(directory.path().into()),
    )?);
    let (send, mut gates) = mpsc::unbounded_channel();
    set_gate_tx(send);
    let (send, mut held) = mpsc::unbounded_channel();
    let gating = signals.clone();
    let driver = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gating.hold.load(Ordering::SeqCst)
                && gate.ticket.channel == cellar_logic::Channel::Service
            {
                send.send(gate).unwrap();
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
    runtime
        .restore_cell(
            "Root:first",
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell("Root:first".into(), 1, true).await?;
    runtime.publish_cell("Root:first", 1)?;
    for (path, expected) in [
        ("seed", "seeded"),
        ("cached", "cached"),
        ("fetch", "echo"),
        ("rpc", "rpc"),
        ("missing", "missing"),
        ("stale", "stale"),
        ("env", "env"),
        ("broker", "broker"),
    ] {
        assert_eq!(call(&runtime, path).await?.body, expected.as_bytes());
    }
    assert_eq!(signals.codes.load(Ordering::SeqCst), 1);
    assert_eq!(signals.app_drops.load(Ordering::SeqCst), 0);
    signals.hold.store(true, Ordering::SeqCst);
    let before = signals.echo_calls.load(Ordering::SeqCst);
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "gated").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.ticket.epoch, Some(1));
    assert_eq!(signals.echo_calls.load(Ordering::SeqCst), before);
    signals.hold.store(false, Ordering::SeqCst);
    gate.reply.send(Ok(())).unwrap();
    assert_eq!(pending.await??.body, b"echo");
    for (count, path) in ["cancel", "cancel-broker"].into_iter().enumerate() {
        let caller = runtime.clone();
        let pending = cellar::asyncrt::spawn(async move { call(&caller, path).await });
        signals.pending.notified().await;
        signals.cancel.notify_one();
        assert_eq!(pending.await??.body, b"cancelled");
        signals.dropped.notified().await;
        assert_eq!(signals.pending_drops.load(Ordering::SeqCst), count + 1);
    }
    // An absent environment must not retain capabilities that were discarded
    // with it, and the origin configuration itself is only a weak link.
    assert_eq!(call(&runtime, "null-env-release").await?.body, b"null");
    signals.app_dropped.notified().await;
    assert_eq!(signals.app_drops.load(Ordering::SeqCst), 1);
    for (count, path, expected) in [(2, "env-retain", "env"), (3, "broker-retain", "broker")] {
        assert_eq!(call(&runtime, "seed").await?.body, b"seeded");
        assert_eq!(signals.codes.load(Ordering::SeqCst), count);
        assert_eq!(call(&runtime, "stale").await?.body, b"stale");
        assert_eq!(call(&runtime, path).await?.body, expected.as_bytes());
        signals.app_dropped.notified().await;
        assert_eq!(signals.app_drops.load(Ordering::SeqCst), count);
    }
    assert_eq!(call(&runtime, "seed").await?.body, b"seeded");
    assert_eq!(signals.codes.load(Ordering::SeqCst), 4);
    assert_eq!(call(&runtime, "stale").await?.body, b"stale");
    assert_eq!(call(&runtime, "dispose-captured").await?.body, b"disposed");
    drop(signals.echo.lock().unwrap().take());
    signals.app_dropped.notified().await;
    assert_eq!(signals.app_drops.load(Ordering::SeqCst), 4);
    runtime.swap_out_cell("Root:first", 1).await?;
    driver.abort();
    Ok(())
}
