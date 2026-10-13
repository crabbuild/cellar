//! Native loaders and loaded facets through the production runtime, without V8.
use anyhow::{anyhow, Result};
use cellar::application::facets::{Class, CompletionError, Startup};
use cellar::application::loader::{Entrypoint, Worker, WorkerCode};
use cellar::application::service::Fetcher;
use cellar::application::{
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::native::clone::{decode, encode, Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

fn graph() -> StructuredValue {
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
        url: format!("http://loaded/{path}"),
        method: "POST".into(),
        headers: Vec::new(),
        body: Vec::new().into(),
        entrypoint: None,
    }
}

fn response(text: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.as_bytes().to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

#[derive(Default)]
struct Signals {
    codes: AtomicUsize,
    failures: AtomicUsize,
    constructors: AtomicUsize,
    app_drops: AtomicUsize,
    pending_drops: AtomicUsize,
    rpc_calls: AtomicUsize,
    entrypoint: Mutex<Option<Entrypoint>>,
    class: Mutex<Option<Class>>,
    pending: Notify,
    cancel: Notify,
    dropped: Notify,
    app_dropped: Notify,
    background_started: Notify,
    background_release: Notify,
    background_finished: Notify,
    self_started: Notify,
    self_resume: Notify,
    self_finished: Notify,
    self_result: Mutex<Option<std::result::Result<(), String>>>,
    self_handle: Mutex<Option<Fetcher>>,
    self_event_handle: Mutex<Option<Fetcher>>,
    hold_self: AtomicBool,
}

struct AppDrop(Arc<Signals>);

impl Drop for AppDrop {
    fn drop(&mut self) {
        assert!(
            cellar::host_loader::loader_registry().try_lock().is_ok(),
            "application dropped inside loader registry lock"
        );
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

#[derive(Debug)]
struct Failure(u32);

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "code failure {}", self.0)
    }
}

impl std::error::Error for Failure {}

struct Leaf {
    props: StructuredValue,
    _application: Arc<AppDrop>,
}

impl Cell for Leaf {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            assert_eq!(context.loaded_environment()?.unwrap(), &graph());
            assert_eq!(context.facet().unwrap().props, self.props);
            if matches!(&invocation, Invocation::Fetch(request) if request.url.ends_with("retain-self"))
            {
                let original = context.clone();
                let signals = self._application.0.clone();
                context.wait_until(async move {
                    signals.self_started.notify_one();
                    signals.self_resume.notified().await;
                    let result = async {
                        // Mint a fresh self capability after its loader has
                        // been disposed. Only the active facet keeps code live.
                        let handle = original
                            .exported_entrypoint("entry")?
                            .with_props(&graph())?;
                        assert_eq!(
                            handle.fetch(&original, request("nested-self")).await?.body,
                            b"nested"
                        );
                        let bytes = handle
                            .rpc(
                                &original,
                                cellar::WorkerRpcOperation::Call {
                                    path: vec!["echo".into()],
                                    args: encode(&graph())?,
                                },
                            )
                            .await?;
                        assert_eq!(decode(&bytes)?, graph());
                        *signals.self_handle.lock().unwrap() = Some(handle);
                        Ok::<_, anyhow::Error>(())
                    }
                    .await
                    .map_err(|error| error.to_string());
                    *signals.self_result.lock().unwrap() = Some(result);
                    signals.self_finished.notify_one();
                    Ok(())
                })?;
            }
            let storage = context.storage()?;
            let calls = storage
                .get_json("calls")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + 1;
            storage.put_json("calls", &serde_json::json!(calls))?;
            Ok(response(&calls.to_string()))
        })
    }
}

fn child(signals: Arc<Signals>) -> Result<Arc<dyn Application>> {
    let probe = Arc::new(AppDrop(signals.clone()));
    let factory = probe.clone();
    let registry = Registry::new(move |context, invocation| {
        let probe = probe.clone();
        Box::pin(async move {
            let _keep = probe.clone();
            assert_eq!(context.loaded_environment()?.unwrap(), &graph());
            assert!(
                context.var("AMBIENT").is_none(),
                "loaded code receives no ambient parent vars"
            );
            assert!(
                context.loader("WORKERS").is_err(),
                "loader authority is not ambient"
            );
            match invocation {
                Invocation::Fetch(incoming) => {
                    let entry = incoming.entrypoint.as_ref().unwrap();
                    assert_eq!(entry.name, "entry");
                    if incoming.url.ends_with("empty") {
                        assert!(entry.props.is_empty());
                        return Ok(response("empty"));
                    }
                    if incoming.url.ends_with("primitive") {
                        assert_eq!(decode(&entry.props)?.root, Value::Number(17.0));
                        return Ok(response("17"));
                    }
                    assert_eq!(decode(&entry.props)?, graph());
                    match incoming.url.rsplit('/').next().unwrap() {
                        "nested-self" => {
                            let handle =
                                context.exported_entrypoint("entry")?.with_props(&graph())?;
                            let bytes = handle
                                .rpc(
                                    &context,
                                    cellar::WorkerRpcOperation::Call {
                                        path: vec!["echo".into()],
                                        args: encode(&graph())?,
                                    },
                                )
                                .await?;
                            assert_eq!(decode(&bytes)?, graph());
                            *probe.0.self_event_handle.lock().unwrap() = Some(handle);
                            Ok(response("nested"))
                        }
                        "pending" => {
                            let _pending = Pending(probe.0.clone());
                            probe.0.pending.notify_one();
                            futures_util::future::pending::<()>().await;
                            unreachable!()
                        }
                        "limits" | "one-limit" => {
                            let count = if incoming.url.ends_with("one-limit") {
                                1
                            } else {
                                2
                            };
                            for _ in 0..count {
                                drop(context.rpc_cell(
                                    "unused:root".into(),
                                    None,
                                    "unused".into(),
                                    cellar::engine_api::RpcData::Json("[]".into()),
                                ));
                            }
                            let error = context
                                .rpc_cell(
                                    "unused:root".into(),
                                    None,
                                    "unused".into(),
                                    cellar::engine_api::RpcData::Json("[]".into()),
                                )
                                .await
                                .err()
                                .unwrap();
                            assert!(error
                                .to_string()
                                .contains(&format!("subrequest limit of {count}")));
                            Ok(response(&count.to_string()))
                        }
                        _ => Ok(response("loaded")),
                    }
                }
                Invocation::Rpc {
                    entrypoint,
                    operation,
                    props,
                } => {
                    probe.0.rpc_calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(entrypoint, "entry");
                    assert_eq!(decode(&props)?, graph());
                    let cellar::WorkerRpcOperation::Call { path, args } = operation else {
                        unreachable!()
                    };
                    assert_eq!(decode(&args)?, graph());
                    if path == ["pending"] {
                        let _pending = Pending(probe.0.clone());
                        probe.0.pending.notify_one();
                        futures_util::future::pending::<()>().await;
                        unreachable!()
                    }
                    if path == ["background"] {
                        let signals = probe.0.clone();
                        context.wait_until(async move {
                            signals.background_started.notify_one();
                            signals.background_release.notified().await;
                            signals.background_finished.notify_one();
                            Ok(())
                        })?;
                    } else {
                        assert_eq!(path, vec!["echo"]);
                    }
                    Ok(Output::Bytes(args))
                }
                _ => unreachable!(),
            }
        })
    })
    .cell("Leaf", move |context, _| {
        assert_eq!(context.loaded_environment()?.unwrap(), &graph());
        factory.0.constructors.fetch_add(1, Ordering::SeqCst);
        let props = context.facet().unwrap().props.clone();
        assert!(
            props == graph() || props.root == Value::Undefined || props.root == Value::Number(17.0)
        );
        Ok(Arc::new(Leaf {
            props,
            _application: factory.clone(),
        }))
    })?;
    Ok(Arc::new(registry))
}

struct Parent(Arc<Signals>);

impl Parent {
    fn worker(&self, context: &Context) -> Result<Worker> {
        let signals = self.0.clone();
        context
            .loader("WORKERS")?
            .get(context, "shared", move |context| {
                Box::pin(async move {
                    context.storage()?.get_alarm()?;
                    signals.codes.fetch_add(1, Ordering::SeqCst);
                    let mut code = WorkerCode::new(child(signals)?);
                    code.environment = Some(graph());
                    code.limits = Some(cellar::WorkerInvocationLimits {
                        cpu_ms: None,
                        sub_requests: Some(2),
                    });
                    Ok(code)
                })
            })
    }

    fn entry(&self) -> Entrypoint {
        self.0.entrypoint.lock().unwrap().clone().unwrap()
    }
}

impl Cell for Parent {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(incoming) = invocation else {
                unreachable!()
            };
            match incoming.url.rsplit('/').next().unwrap() {
                "environment-limit" => {
                    let mut code = WorkerCode::new(Arc::new(Registry::new(|_, _| {
                        Box::pin(async { Ok(Output::Done) })
                    })));
                    code.environment = Some(StructuredValue {
                        root: Value::Reference(0),
                        objects: vec![Object::ArrayBuffer {
                            bytes: vec![0; 1024 * 1024 + 1],
                            max_length: None,
                            immutable: false,
                        }],
                    });
                    let before = cellar::host_loader::loader_registry().lock().unwrap().len();
                    let worker = context.loader("WORKERS")?.load(&context, code)?;
                    let error = worker.wait_ready().await.err().unwrap();
                    assert!(error.to_string().contains("environment exceeds size limit"));
                    assert_eq!(
                        cellar::host_loader::loader_registry().lock().unwrap().len(),
                        before
                    );
                    Ok(response("bounded"))
                }
                "empty-environment" => {
                    let app = Registry::new(|context, _| {
                        Box::pin(async move {
                            assert!(context.loaded_environment()?.is_none());
                            Ok(response("empty env"))
                        })
                    });
                    let mut code = WorkerCode::new(Arc::new(app));
                    code.environment = Some(StructuredValue {
                        root: Value::Null,
                        objects: Vec::new(),
                    });
                    let worker = context.loader("WORKERS")?.load(&context, code)?;
                    Ok(Output::Http(
                        worker
                            .entrypoint("default")
                            .fetch(&context, request("hello"))
                            .await?,
                    ))
                }
                "last-handle" => {
                    let mut code = WorkerCode::new(child(self.0.clone())?);
                    code.environment = Some(graph());
                    let worker = context.loader("WORKERS")?.load(&context, code)?;
                    let entry = worker.entrypoint("entry").with_props(&graph())?;
                    drop(worker);
                    let response = entry.fetch(&context, request("hello")).await?;
                    drop(entry);
                    Ok(Output::Http(response))
                }
                "get" => {
                    let worker = self.worker(&context)?;
                    let mut props = graph();
                    let entry = worker.entrypoint("entry").with_props(&props)?;
                    let class = worker.cell_class("Leaf").with_props(&props)?;
                    props.objects.clear();
                    *self.0.entrypoint.lock().unwrap() = Some(entry.clone());
                    *self.0.class.lock().unwrap() = Some(class);
                    drop(worker);
                    let call = entry.fetch_with_tail(&context, request("hello"));
                    let result = call.response.await?;
                    let report: serde_json::Value = serde_json::from_str(&call.report.await?)?;
                    assert_eq!(report[0]["outcome"], "ok");
                    assert_eq!(report[0]["event"]["response"]["status"], 200);
                    Ok(Output::Http(result))
                }
                "rpc" => {
                    let result = self
                        .entry()
                        .rpc(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["echo".into()],
                                args: encode(&graph())?,
                            },
                        )
                        .await?;
                    assert_eq!(decode(&result)?, graph());
                    Ok(response("rpc"))
                }
                "limits" => Ok(Output::Http(
                    self.entry().fetch(&context, request("limits")).await?,
                )),
                "one-limit" => Ok(Output::Http(
                    self.entry()
                        .with_limits(cellar::WorkerInvocationLimits {
                            cpu_ms: None,
                            sub_requests: Some(1),
                        })
                        .fetch(&context, request("one-limit"))
                        .await?,
                )),
                "primitive-props" => Ok(Output::Http(
                    self.entry()
                        .with_props(&StructuredValue {
                            root: Value::Number(17.0),
                            objects: Vec::new(),
                        })?
                        .fetch(&context, request("primitive"))
                        .await?,
                )),
                "undefined-props" => Ok(Output::Http(
                    self.entry()
                        .with_props(&StructuredValue {
                            root: Value::Undefined,
                            objects: Vec::new(),
                        })?
                        .fetch(&context, request("empty"))
                        .await?,
                )),
                "default-facet" | "primitive-facet" => {
                    let mut class = self.worker(&context)?.cell_class("Leaf");
                    if incoming.url.ends_with("primitive-facet") {
                        class = class.with_props(&StructuredValue {
                            root: Value::Number(17.0),
                            objects: Vec::new(),
                        })?;
                    }
                    let name = incoming.url.rsplit('/').next().unwrap();
                    let stub = context.facets()?.get(&context, name, move |_| {
                        Box::pin(async move { Ok(Startup::new(class)) })
                    })?;
                    Ok(Output::Http(stub.fetch(&context, request("fetch")).await?))
                }
                "facet" => {
                    let class = self.0.class.lock().unwrap().clone().unwrap();
                    let stub = context.facets()?.get(&context, "loaded", move |_| {
                        Box::pin(async move { Ok(Startup::new(class)) })
                    })?;
                    Ok(Output::Http(stub.fetch(&context, request("fetch")).await?))
                }
                "retain-self" => {
                    let class = self.0.class.lock().unwrap().clone().unwrap();
                    let stub = context.facets()?.get(&context, "loaded", move |_| {
                        Box::pin(async move { Ok(Startup::new(class)) })
                    })?;
                    stub.fetch(&context, request("retain-self")).await?;
                    Ok(response("retained"))
                }
                "stale-self" => {
                    let handle = self.0.self_handle.lock().unwrap().clone().unwrap();
                    assert!(handle.fetch(&context, request("hello")).await.is_err());
                    assert!(handle
                        .rpc(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["echo".into()],
                                args: encode(&graph())?,
                            }
                        )
                        .await
                        .is_err());
                    Ok(response("retired"))
                }
                "active-self" => {
                    let handle = self.0.self_handle.lock().unwrap().clone().unwrap();
                    assert_eq!(
                        handle.fetch(&context, request("hello")).await?.body,
                        b"loaded"
                    );
                    let stale = self.0.self_event_handle.lock().unwrap().clone().unwrap();
                    let error = stale
                        .rpc(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["echo".into()],
                                args: encode(&graph())?,
                            },
                        )
                        .await
                        .unwrap_err();
                    assert!(error.to_string().contains("event has retired"));
                    Ok(response("active facet, retired event"))
                }
                "gated-self" => {
                    context
                        .storage()?
                        .put_json("self-call", &serde_json::json!(true))?;
                    let handle = self.0.self_handle.lock().unwrap().clone().unwrap();
                    let error = handle
                        .fetch(&context, request("hello"))
                        .await
                        .err()
                        .unwrap();
                    assert!(error.to_string().contains("activation has retired"));
                    Ok(response("retired behind proof"))
                }
                "failed" => {
                    let signals = self.0.clone();
                    let failed = context
                        .loader("WORKERS")?
                        .get(&context, "failed", move |_| {
                            Box::pin(async move {
                                signals.failures.fetch_add(1, Ordering::SeqCst);
                                Err(Failure(42).into())
                            })
                        })?;
                    for _ in 0..2 {
                        let error = failed
                            .entrypoint("entry")
                            .fetch(&context, request("hello"))
                            .await
                            .err()
                            .unwrap();
                        assert_eq!(
                            error
                                .downcast_ref::<CompletionError>()
                                .unwrap()
                                .cause
                                .downcast_ref::<Failure>()
                                .unwrap()
                                .0,
                            42
                        );
                    }
                    Ok(response("failed once"))
                }
                "dispose-captured" => {
                    let worker = self.worker(&context)?;
                    let entry = self.entry();
                    let captured = entry.fetch(&context, request("hello"));
                    worker.dispose(&context)?;
                    assert_eq!(captured.await?.body, b"loaded");
                    assert!(entry.fetch(&context, request("hello")).await.is_err());
                    Ok(response("disposed"))
                }
                "cancel" => {
                    let call = self.entry().fetch(&context, request("pending"));
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! { "caller drops a loaded native fetch";
                        _=self.0.cancel.notified()=>{}, _=&mut call=>return Err(anyhow!("pending loaded call unexpectedly answered")),
                    }
                    Ok(response("cancelled"))
                }
                "pre-aborted-rpc" => {
                    let controller = cellar::native::web::AbortController::new();
                    let reason = cellar::native::web::AbortReason::error("already stopped");
                    controller.abort(Some(reason.clone()))?;
                    let error = self
                        .entry()
                        .rpc_with_signal(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["pending".into()],
                                args: encode(&graph())?,
                            },
                            controller.signal(),
                        )
                        .await
                        .unwrap_err();
                    assert!(error
                        .downcast_ref::<cellar::native::web::AbortReason>()
                        .unwrap()
                        .ptr_eq(&reason));
                    Ok(response("pre-aborted"))
                }
                path @ ("cancel-rpc" | "cancel-rpc-signal") => {
                    let entrypoint = self.entry();
                    let controller = cellar::native::web::AbortController::new();
                    let operation = cellar::WorkerRpcOperation::Call {
                        path: vec!["pending".into()],
                        args: encode(&graph())?,
                    };
                    let call = if path == "cancel-rpc-signal" {
                        entrypoint.rpc_with_signal(&context, operation, controller.signal())
                    } else {
                        entrypoint.rpc(&context, operation)
                    };
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! { "caller drops or aborts a loaded native RPC";
                        _=self.0.cancel.notified()=>{}, _=&mut call=>return Err(anyhow!("pending loaded RPC unexpectedly answered")),
                    }
                    if path == "cancel-rpc-signal" {
                        let reason =
                            cellar::native::web::AbortReason::error("loaded RPC caller stopped");
                        controller.abort(Some(reason.clone()))?;
                        assert!(call
                            .await
                            .unwrap_err()
                            .downcast_ref::<cellar::native::web::AbortReason>()
                            .unwrap()
                            .ptr_eq(&reason));
                        let bytes = entrypoint
                            .rpc(
                                &context,
                                cellar::WorkerRpcOperation::Call {
                                    path: vec!["echo".into()],
                                    args: encode(&graph())?,
                                },
                            )
                            .await?;
                        assert_eq!(decode(&bytes)?, graph());
                    }
                    Ok(response("cancelled"))
                }
                "background-rpc" => {
                    let bytes = self
                        .entry()
                        .rpc(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["background".into()],
                                args: encode(&graph())?,
                            },
                        )
                        .await?;
                    assert_eq!(decode(&bytes)?, graph());
                    Ok(response("background"))
                }
                _ => unreachable!(),
            }
        })
    }
}

fn deployment() -> LoadedDeployment {
    LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: "this is not JavaScript".into(),
            script_name: "native-loader".into(),
            do_classes: vec!["Root".into()],
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: vec![("AMBIENT".into(), "parent-only".into())],
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: "native-loader".into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: vec!["WORKERS".into()],
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    }
}

async fn call(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            scope.into(),
            Some("parent-name".into()),
            RuntimeFetch {
                url: format!("http://native/{path}"),
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
async fn native_loader_shares_handles_limits_cancellation_and_loaded_facet_retirement() -> Result<()>
{
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let (send, mut gates) = mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(send);
    let (hold, mut held) = mpsc::unbounded_channel();
    let gating = signals.clone();
    let gate_driver = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gate.ticket.channel == cellar_logic::Channel::Service
                && gating.hold_self.load(Ordering::SeqCst)
            {
                hold.send(gate).unwrap();
                continue;
            }
            let _ = gate.reply.send(Ok(()));
        }
    });
    let parents = signals.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Ok(response("default")) }))
        .cell("Root", move |_, _| Ok(Arc::new(Parent(parents.clone()))))?;
    let generation = Generation::build_native(
        1,
        DeploymentGraph::single(deployment()),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-loader".into(),
            Arc::new(registry) as Arc<dyn Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = Arc::new(RuntimeManager::start(
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
    )?);
    let scope = "Root:first";
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
    assert_eq!(call(&runtime, scope, "get").await?.body, b"loaded");
    assert_eq!(call(&runtime, scope, "get").await?.body, b"loaded");
    assert_eq!(
        call(&runtime, scope, "empty-environment").await?.body,
        b"empty env"
    );
    assert_eq!(
        call(&runtime, scope, "environment-limit").await?.body,
        b"bounded"
    );
    assert_eq!(
        signals.codes.load(Ordering::SeqCst),
        1,
        "derived handles retain the binding's named cache"
    );
    assert_eq!(call(&runtime, scope, "rpc").await?.body, b"rpc");
    assert_eq!(call(&runtime, scope, "limits").await?.body, b"2");
    assert_eq!(call(&runtime, scope, "one-limit").await?.body, b"1");
    assert_eq!(call(&runtime, scope, "failed").await?.body, b"failed once");
    assert_eq!(signals.failures.load(Ordering::SeqCst), 1);
    assert_eq!(call(&runtime, scope, "facet").await?.body, b"1");
    assert_eq!(call(&runtime, scope, "facet").await?.body, b"2");
    assert_eq!(signals.constructors.load(Ordering::SeqCst), 1);
    assert_eq!(call(&runtime, scope, "primitive-props").await?.body, b"17");
    assert_eq!(
        call(&runtime, scope, "undefined-props").await?.body,
        b"empty"
    );
    assert_eq!(call(&runtime, scope, "default-facet").await?.body, b"1");
    assert_eq!(call(&runtime, scope, "primitive-facet").await?.body, b"1");
    assert_eq!(signals.constructors.load(Ordering::SeqCst), 3);
    let caller = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "cancel").await });
    signals.pending.notified().await;
    signals.cancel.notify_one();
    assert_eq!(pending.await??.body, b"cancelled");
    signals.dropped.notified().await;
    assert_eq!(signals.pending_drops.load(Ordering::SeqCst), 1);
    let before = signals.rpc_calls.load(Ordering::SeqCst);
    assert_eq!(
        call(&runtime, scope, "pre-aborted-rpc").await?.body,
        b"pre-aborted"
    );
    assert_eq!(signals.rpc_calls.load(Ordering::SeqCst), before);
    for (index, path) in ["cancel-rpc", "cancel-rpc-signal"].into_iter().enumerate() {
        let caller = runtime.clone();
        let pending =
            cellar::asyncrt::spawn(async move { call(&caller, "Root:first", path).await });
        signals.pending.notified().await;
        signals.cancel.notify_one();
        assert_eq!(pending.await??.body, b"cancelled");
        signals.dropped.notified().await;
        assert_eq!(signals.pending_drops.load(Ordering::SeqCst), index + 2);
        assert_eq!(call(&runtime, scope, "rpc").await?.body, b"rpc");
    }
    assert_eq!(
        call(&runtime, scope, "background-rpc").await?.body,
        b"background"
    );
    signals.background_started.notified().await;
    signals.background_release.notify_one();
    signals.background_finished.notified().await;
    runtime.swap_out_cell(scope, 1).await?;
    runtime.start_cell(scope.into(), 1, false).await?;
    runtime.publish_cell(scope, 1)?;
    assert_eq!(call(&runtime, scope, "facet").await?.body, b"3");
    assert_eq!(signals.constructors.load(Ordering::SeqCst), 4);
    assert_eq!(
        call(&runtime, scope, "retain-self").await?.body,
        b"retained"
    );
    signals.self_started.notified().await;
    assert_eq!(
        call(&runtime, scope, "dispose-captured").await?.body,
        b"disposed"
    );
    assert_eq!(
        signals.app_drops.load(Ordering::SeqCst),
        0,
        "a running root facet retains the disposed worker"
    );
    signals.self_resume.notify_one();
    signals.self_finished.notified().await;
    signals.self_result.lock().unwrap().take().unwrap().unwrap();
    assert_eq!(
        call(&runtime, scope, "active-self").await?.body,
        b"active facet, retired event"
    );
    let other = "Root:other";
    runtime
        .restore_cell(
            other,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(other.into(), 1, true).await?;
    runtime.publish_cell(other, 1)?;
    signals.hold_self.store(true, Ordering::SeqCst);
    let before = signals.rpc_calls.load(Ordering::SeqCst);
    let caller = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "Root:other", "gated-self").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.scope, other);
    runtime.swap_out_cell(scope, 1).await?;
    assert_eq!(
        signals.app_drops.load(Ordering::SeqCst),
        0,
        "a captured call retains code only until its proof wait completes"
    );
    signals.hold_self.store(false, Ordering::SeqCst);
    gate.reply.send(Ok(())).unwrap();
    assert_eq!(pending.await??.body, b"retired behind proof");
    assert_eq!(signals.rpc_calls.load(Ordering::SeqCst), before);
    signals.app_dropped.notified().await;
    assert_eq!(signals.app_drops.load(Ordering::SeqCst), 1);
    runtime.start_cell(scope.into(), 1, false).await?;
    runtime.publish_cell(scope, 1)?;
    let before = signals.rpc_calls.load(Ordering::SeqCst);
    assert_eq!(call(&runtime, scope, "stale-self").await?.body, b"retired");
    assert_eq!(signals.rpc_calls.load(Ordering::SeqCst), before);
    assert_eq!(call(&runtime, scope, "last-handle").await?.body, b"loaded");
    signals.app_dropped.notified().await;
    assert_eq!(
        signals.app_drops.load(Ordering::SeqCst),
        2,
        "last derived handle releases its loaded backend while the parent remains alive"
    );
    runtime.swap_out_cell(scope, 1).await?;
    runtime.swap_out_cell(other, 1).await?;
    assert!(cellar::host_loader::loader_registry()
        .lock()
        .unwrap()
        .is_empty());
    gate_driver.abort();
    Ok(())
}
