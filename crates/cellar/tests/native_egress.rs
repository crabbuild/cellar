//! Native service/HTTP capabilities through production runtime dispatch.
use anyhow::{anyhow, Result};
use cellar::application::http::{Headers, MethodError, OutboundRequest, Redirect, Response};
use cellar::application::loader::{Outbound, WorkerCode};
use cellar::application::streams::FixedLengthStream;
use cellar::application::{
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::host_channels::{set_gate_tx, set_svc_call_tx, set_svc_rpc_tx};
use cellar::native::clone::{decode, encode, Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{future::Future, pin::Pin, task::Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};

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

fn request(path: &str) -> Request {
    Request {
        url: format!("http://service/{path}"),
        method: "POST".into(),
        headers: Vec::new(),
        body: Vec::new().into(),
        entrypoint: None,
    }
}

fn props() -> StructuredValue {
    StructuredValue {
        root: Value::Number(7.0),
        objects: Vec::new(),
    }
}

#[derive(Default)]
struct Signals {
    origin: String,
    services: AtomicUsize,
    hold: AtomicBool,
    peer_requests: Mutex<Vec<String>>,
    peer_pending: Notify,
    peer_closed: Notify,
    service_pending: Notify,
    service_dropped: Notify,
    cancel: Notify,
    background_started: Notify,
    background_release: Notify,
    background_finished: Notify,
}

struct PendingService(Arc<Signals>);

impl Future for PendingService {
    type Output = Result<Output>;

    fn poll(self: Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.0.service_pending.notify_one();
        Poll::Pending
    }
}

impl Drop for PendingService {
    fn drop(&mut self) {
        self.0.service_dropped.notify_one();
    }
}

struct Root(Arc<Signals>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(incoming) = invocation else {
                unreachable!()
            };
            let path = incoming.url.rsplit('/').next().unwrap();
            match path {
                "service" | "gated" => {
                    if path == "gated" {
                        context
                            .storage()?
                            .put_json("written", &serde_json::json!(true))?;
                    }
                    Ok(Output::Http(
                        context
                            .service("TARGET")?
                            .with_props(&props())?
                            .fetch(&context, request("service"))
                            .await?,
                    ))
                }
                "rpc" => {
                    let bytes = context
                        .service("TARGET")?
                        .with_props(&props())?
                        .rpc(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["echo".into()],
                                args: encode(&props())?,
                            },
                        )
                        .await?;
                    assert_eq!(decode(&bytes)?, props());
                    Ok(response("rpc"))
                }
                "service-stream" => {
                    let pipe = FixedLengthStream::new(6.0)?;
                    let writer = pipe.writable.get_writer()?;
                    context.wait_until_byte_stream(&pipe.readable, async move {
                        writer.write(b"abc".to_vec()).await?;
                        writer.write(b"def".to_vec()).await?;
                        writer.close().await?;
                        Ok(())
                    })?;
                    let request = Request::with_byte_stream(
                        &context,
                        "http://service/stream",
                        "post",
                        &Headers::new(),
                        &pipe.readable,
                    )?;
                    let mut result = Response::from_http(
                        context
                            .service("TARGET")?
                            .with_props(&props())?
                            .fetch(&context, request)
                            .await?,
                    );
                    assert_eq!(result.text(&context).await?, "abcdef");
                    Ok(response("streamed"))
                }
                "loopback" => Ok(Output::Http(
                    context
                        .exported_entrypoint("self")?
                        .with_props(&props())?
                        .fetch(&context, request("self"))
                        .await?,
                )),
                "deny-env" | "broker" | "redirect-limit" => {
                    let mode = path.to_string();
                    let target = context.service("TARGET")?.with_props(&props())?;
                    let origin = self.0.origin.clone();
                    let child_mode = mode.clone();
                    let app = Registry::new(move |context, _| {
                        let mode = child_mode.clone();
                        let origin = origin.clone();
                        Box::pin(async move {
                            match mode.as_str() {
                                "deny-env" => {
                                    let error = context
                                        .fetch(OutboundRequest::get("http://denied.invalid/"))
                                        .await
                                        .err()
                                        .unwrap();
                                    assert!(error
                                        .to_string()
                                        .contains("not permitted to access the internet"));
                                    let graph = context.loaded_environment()?.unwrap();
                                    let Object::Object(fields) = &graph.objects[0] else {
                                        unreachable!()
                                    };
                                    let get = |key: &str| {
                                        fields
                                            .iter()
                                            .find(|(name, _)| name == &Value::from(key))
                                            .unwrap()
                                            .1
                                            .clone()
                                    };
                                    assert_eq!(get("FETCHER"), get("AGAIN"));
                                    assert!(context.environment_fetcher(&get("BAD")).is_err());
                                    let fetcher = context.environment_fetcher(&get("FETCHER"))?;
                                    Ok(Output::Http(
                                        fetcher.fetch(&context, request("service")).await?,
                                    ))
                                }
                                "broker" => Ok(Output::Http(
                                    context
                                        .fetch(OutboundRequest::get(
                                            "http://broker-only.invalid/broker",
                                        ))
                                        .await?,
                                )),
                                "redirect-limit" => {
                                    let error = context
                                        .fetch(OutboundRequest::get(format!("{origin}/redirect")))
                                        .await
                                        .err()
                                        .unwrap();
                                    assert!(
                                        error.to_string().contains("subrequest limit of 1"),
                                        "{error}"
                                    );
                                    Ok(response("limited"))
                                }
                                _ => unreachable!(),
                            }
                        })
                    });
                    let mut code = WorkerCode::new(Arc::new(app));
                    match mode.as_str() {
                        "deny-env" => {
                            code.outbound = Outbound::Deny;
                            let marker = code.environment_fetcher(target)?;
                            let graph = code.environment.as_mut().unwrap();
                            let Value::Reference(index) = &marker else {
                                unreachable!()
                            };
                            let Object::Object(route) = &mut graph.objects[*index as usize] else {
                                unreachable!()
                            };
                            // A supplied environment route is a capability.
                            route[0].0 = Value::from("__cellar$loaderSvc");
                            let bad = graph.objects.len() as u32;
                            graph.objects.push(Object::Object(vec![(
                                Value::from("__cellar$loaderSvc"),
                                Value::Number(99.0),
                            )]));
                            let Object::Object(fields) = &mut graph.objects[0] else {
                                unreachable!()
                            };
                            fields.extend([
                                (Value::from("FETCHER"), marker.clone()),
                                (Value::from("AGAIN"), marker),
                                (Value::from("BAD"), Value::Reference(bad)),
                            ]);
                        }
                        "broker" => code.outbound = Outbound::Broker(target),
                        _ => {
                            code.limits = Some(cellar::WorkerInvocationLimits {
                                cpu_ms: None,
                                sub_requests: Some(1),
                            })
                        }
                    }
                    let worker = context.loader("WORKERS")?.load(&context, code)?;
                    Ok(Output::Http(
                        worker
                            .entrypoint("default")
                            .fetch(&context, request("run"))
                            .await?,
                    ))
                }
                "network" => {
                    let mut invalid = OutboundRequest::get(format!("{}/invalid", self.0.origin));
                    invalid.method = "BAD METHOD".into();
                    assert!(context
                        .fetch(invalid)
                        .await
                        .err()
                        .unwrap()
                        .is::<MethodError>());
                    for body in [None, Some(Vec::new().into())] {
                        let name = if body.is_some() { "empty" } else { "none" };
                        let mut request = OutboundRequest::get(format!("{}/{name}", self.0.origin));
                        request.body = body;
                        let mut result = Response::from_http(context.fetch(request).await?);
                        assert_eq!(result.text(&context).await?, "ok");
                    }
                    let mut manual = OutboundRequest::get(format!("{}/redirect", self.0.origin));
                    manual.redirect = Redirect::Manual;
                    assert_eq!(context.fetch(manual).await?.status, 302);
                    let mut error = OutboundRequest::get(format!("{}/redirect", self.0.origin));
                    error.redirect = Redirect::Error;
                    assert!(context.fetch(error).await.is_err());
                    let mut followed = Response::from_http(
                        context
                            .fetch(OutboundRequest::get(format!("{}/redirect", self.0.origin)))
                            .await?,
                    );
                    assert_eq!(followed.text(&context).await?, "ok");
                    Ok(response("network"))
                }
                "gated-network" => {
                    context
                        .storage()?
                        .put_json("before-http", &serde_json::json!(true))?;
                    let mut result = Response::from_http(
                        context
                            .fetch(OutboundRequest::get(format!(
                                "{}/gated-network",
                                self.0.origin
                            )))
                            .await?,
                    );
                    assert_eq!(result.text(&context).await?, "ok");
                    Ok(response("network"))
                }
                "cancel-network" => {
                    let controller = cellar::native::web::AbortController::new();
                    let call = context.fetch_with_signal(
                        OutboundRequest::get(format!("{}/pending", self.0.origin)),
                        controller.signal(),
                    );
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! {"an external abort ends a pending HTTP head";
                      _=self.0.cancel.notified()=>{},_=&mut call=>return Err(anyhow!("pending HTTP request unexpectedly answered")),
                    }
                    controller.abort(None)?;
                    assert!(call.await.is_err());
                    Ok(response("cancelled"))
                }
                "cancel-service" => {
                    let controller = cellar::native::web::AbortController::new();
                    let call = context
                        .service("TARGET")?
                        .with_props(&props())?
                        .fetch_with_signal(&context, request("pending"), controller.signal());
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! {"caller abort cancels a pending service handler";
                      _=self.0.cancel.notified()=>{},_=&mut call=>return Err(anyhow!("pending service unexpectedly answered")),
                    }
                    let reason = cellar::native::web::AbortReason::error("caller stopped");
                    controller.abort(Some(reason.clone()))?;
                    assert!(call
                        .await
                        .err()
                        .unwrap()
                        .downcast_ref::<cellar::native::web::AbortReason>()
                        .unwrap()
                        .ptr_eq(&reason));
                    Ok(response("cancelled"))
                }
                "pre-aborted-service-rpc" => {
                    let controller = cellar::native::web::AbortController::new();
                    let reason = cellar::native::web::AbortReason::error("already stopped");
                    controller.abort(Some(reason.clone()))?;
                    let error = context
                        .service("TARGET")?
                        .rpc_with_signal(
                            &context,
                            cellar::WorkerRpcOperation::Call {
                                path: vec!["pending".into()],
                                args: encode(&props())?,
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
                "cancel-service-rpc" | "drop-service-rpc" | "cancel-service-rpc-gated" => {
                    if path == "cancel-service-rpc-gated" {
                        context
                            .storage()?
                            .put_json("rpc-written", &serde_json::json!(true))?;
                    }
                    let fetcher = context.service("TARGET")?.with_props(&props())?;
                    let controller = cellar::native::web::AbortController::new();
                    let call = fetcher.rpc_with_signal(
                        &context,
                        cellar::WorkerRpcOperation::Call {
                            path: vec!["pending".into()],
                            args: encode(&props())?,
                        },
                        controller.signal(),
                    );
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! { "caller cancellation releases a service RPC handler or gate";
                        _=self.0.cancel.notified()=>{}, _=&mut call=>return Err(anyhow!("pending service RPC unexpectedly answered")),
                    }
                    if path != "drop-service-rpc" {
                        let reason = cellar::native::web::AbortReason::error("RPC caller stopped");
                        controller.abort(Some(reason.clone()))?;
                        assert!(call
                            .await
                            .unwrap_err()
                            .downcast_ref::<cellar::native::web::AbortReason>()
                            .unwrap()
                            .ptr_eq(&reason));
                        if path != "cancel-service-rpc-gated" {
                            let bytes = fetcher
                                .rpc(
                                    &context,
                                    cellar::WorkerRpcOperation::Call {
                                        path: vec!["echo".into()],
                                        args: encode(&props())?,
                                    },
                                )
                                .await?;
                            assert_eq!(decode(&bytes)?, props());
                        }
                    }
                    Ok(response("cancelled"))
                }
                _ => unreachable!(),
            }
        })
    }
}

fn deployment(script: &str) -> LoadedDeployment {
    LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: "this is not JavaScript".into(),
            script_name: script.into(),
            do_classes: if script == "native-egress" {
                vec!["Root".into()]
            } else {
                Vec::new()
            },
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
        script_name: script.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: vec!["WORKERS".into()],
        assets: None,
        services: if script == "native-egress" {
            vec![("TARGET".into(), "target".into(), Some("entry".into()))]
        } else {
            Vec::new()
        },
        crons: Vec::new(),
        cron_revision: None,
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
async fn native_services_outbound_env_brokers_http_gates_and_cancellation() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

#[test]
fn embedded_http_does_not_read_process_environment() -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "native_services_outbound_env_brokers_http_gates_and_cancellation",
            "--nocapture",
        ])
        .env("CELLAR_FETCH_TIMEOUT_S", "not-a-host-policy")
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("http_proxy", "http://127.0.0.1:9")
        .env("ALL_PROXY", "http://127.0.0.1:9")
        .env("all_proxy", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "embedded HTTP read process settings:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

async fn run() -> Result<Arc<Signals>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signals = Arc::new(Signals {
        origin: format!("http://{}", listener.local_addr()?),
        ..Default::default()
    });
    let peer = signals.clone();
    let server = cellar::asyncrt::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let peer = peer.clone();
            cellar::asyncrt::spawn(async move {
    let mut bytes=Vec::new(); let mut byte=[0;1];
    while !bytes.ends_with(b"\r\n\r\n") {if socket.read(&mut byte).await.unwrap_or(0)==0 {return;}bytes.extend_from_slice(&byte);}
    let headers=String::from_utf8(bytes).unwrap();let path=headers.split_whitespace().nth(1).unwrap().to_string();peer.peer_requests.lock().unwrap().push(headers);
    if path=="/pending" {peer.peer_pending.notify_one();let _=socket.read(&mut byte).await;peer.peer_closed.notify_one();return;}
    let response=if path=="/redirect" {"HTTP/1.1 302 Found\r\nLocation: /ok\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"} else {"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"};
    let _=socket.write_all(response.as_bytes()).await;
   }).detach();
        }
    });
    let root_signals = signals.clone();
    let target_signals = signals.clone();
    let primary = Registry::new(|context, invocation| {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                unreachable!()
            };
            assert_eq!(request.entrypoint.as_ref().unwrap().name, "self");
            assert_eq!(
                decode(&request.entrypoint.as_ref().unwrap().props)?,
                props()
            );
            let forwarded = context
                .service("TARGET")?
                .with_props(&props())?
                .fetch(&context, request)
                .await?;
            assert_eq!(forwarded.body, b"target");
            Ok(response("self"))
        })
    })
    .cell("Root", move |_, _| Ok(Arc::new(Root(root_signals.clone()))))?;
    let target = Registry::new(move |context, invocation| {
        let signals = target_signals.clone();
        Box::pin(async move {
            assert!(context.identity().is_none());
            signals.services.fetch_add(1, Ordering::SeqCst);
            match invocation {
                Invocation::Fetch(request) => {
                    let entry = request.entrypoint.as_ref().unwrap();
                    assert_eq!(entry.name, "entry");
                    assert_eq!(decode(&entry.props)?, props());
                    if request.url.ends_with("pending") {
                        return PendingService(signals).await;
                    }
                    if request.url.ends_with("stream") {
                        assert_eq!(request.method, "POST");
                        assert_eq!(
                            request.header_list()?.get("content-length")?,
                            Some("6".into())
                        );
                        let mut reader = request.into_body_reader(&context)?;
                        let bytes = reader.bytes(&context).await?;
                        let pipe = FixedLengthStream::new(bytes.len() as f64)?;
                        let writer = pipe.writable.get_writer()?;
                        context.wait_until_byte_stream(&pipe.readable, async move {
                            writer.write(bytes).await?;
                            writer.close().await?;
                            Ok(())
                        })?;
                        return Ok(Output::Http(
                            Response::from_byte_stream(200, &pipe.readable, &Headers::new())?
                                .into_http(&context)?,
                        ));
                    }
                    Ok(response(if request.url.ends_with("broker") {
                        "broker"
                    } else {
                        "target"
                    }))
                }
                Invocation::Rpc {
                    entrypoint,
                    props: properties,
                    operation,
                } => {
                    assert_eq!(entrypoint, "entry");
                    assert_eq!(decode(&properties)?, props());
                    let cellar::WorkerRpcOperation::Call { path, args } = operation else {
                        unreachable!()
                    };
                    if path == ["pending"] {
                        return PendingService(signals).await;
                    }
                    if path == ["background"] {
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
    });
    let generation = Generation::build_native(
        1,
        DeploymentGraph {
            primary: deployment("native-egress"),
            cohosted: vec![deployment("target")],
        },
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([
            (
                "native-egress".into(),
                Arc::new(primary) as Arc<dyn Application>,
            ),
            ("target".into(), Arc::new(target) as Arc<dyn Application>),
        ]),
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
    let (send, mut calls) = mpsc::unbounded_channel();
    set_svc_call_tx(send);
    let dispatcher = runtime.clone();
    let services = cellar::asyncrt::spawn(async move {
        while let Some(call) = calls.recv().await {
            let dispatcher = dispatcher.clone();
            cellar::asyncrt::spawn(async move {
                cellar::host_service::dispatch_fetch(Some(&dispatcher), call).await;
            })
            .detach();
        }
    });
    let (send, mut calls) = mpsc::unbounded_channel();
    set_svc_rpc_tx(send);
    let dispatcher = runtime.clone();
    let rpc = cellar::asyncrt::spawn(async move {
        while let Some(call) = calls.recv().await {
            let dispatcher = dispatcher.clone();
            cellar::asyncrt::spawn(async move {
                cellar::host_service::dispatch_rpc(Some(&dispatcher), call).await;
            })
            .detach();
        }
    });
    let (send, mut gates) = mpsc::unbounded_channel();
    set_gate_tx(send);
    let (held_send, mut held) = mpsc::unbounded_channel();
    let gating = signals.clone();
    let gate_driver = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gating.hold.load(Ordering::SeqCst)
                && matches!(
                    gate.ticket.channel,
                    cellar_logic::Channel::Service | cellar_logic::Channel::Fetch
                )
            {
                held_send.send(gate).unwrap();
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
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
    for (path, expected) in [
        ("service", "target"),
        ("rpc", "rpc"),
        ("service-stream", "streamed"),
        ("loopback", "self"),
        ("deny-env", "target"),
        ("broker", "broker"),
        ("network", "network"),
        ("redirect-limit", "limited"),
    ] {
        assert_eq!(call(&runtime, scope, path).await?.body, expected.as_bytes());
    }
    {
        let peers = signals.peer_requests.lock().unwrap();
        let none = peers
            .iter()
            .find(|head| head.starts_with("GET /none "))
            .unwrap()
            .to_ascii_lowercase();
        assert!(!none.contains("content-length:"));
        let empty = peers
            .iter()
            .find(|head| head.starts_with("GET /empty "))
            .unwrap()
            .to_ascii_lowercase();
        assert!(empty.contains("content-length: 0"));
        assert_eq!(
            peers
                .iter()
                .filter(|head| head.starts_with("GET /ok "))
                .count(),
            1,
            "manual, error and limit redirects never replay at the destination"
        );
        assert!(!peers.iter().any(|head| head.contains("/invalid")));
    }
    signals.hold.store(true, Ordering::SeqCst);
    let before = signals.services.load(Ordering::SeqCst);
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "gated").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.ticket.epoch, Some(1));
    assert_eq!(gate.ticket.channel, cellar_logic::Channel::Service);
    assert_eq!(signals.services.load(Ordering::SeqCst), before);
    signals.hold.store(false, Ordering::SeqCst);
    gate.reply.send(Ok(())).unwrap();
    assert_eq!(pending.await??.body, b"target");
    signals.hold.store(true, Ordering::SeqCst);
    let before = signals.services.load(Ordering::SeqCst);
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "gated").await });
    let gate = held.recv().await.unwrap();
    signals.hold.store(false, Ordering::SeqCst);
    gate.reply
        .send(Err(cellar_logic::RequestError::DurabilityUnproven))
        .unwrap();
    assert!(pending.await?.is_err());
    assert_eq!(signals.services.load(Ordering::SeqCst), before);
    // The actual HTTP peer must observe no request while proof is held/refused.
    signals.hold.store(true, Ordering::SeqCst);
    let caller = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "gated-network").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.ticket.channel, cellar_logic::Channel::Fetch);
    assert_eq!(gate.ticket.epoch, Some(1));
    assert!(!signals
        .peer_requests
        .lock()
        .unwrap()
        .iter()
        .any(|head| head.contains("/gated-network")));
    signals.hold.store(false, Ordering::SeqCst);
    gate.reply.send(Ok(())).unwrap();
    assert_eq!(pending.await??.body, b"network");
    signals.hold.store(true, Ordering::SeqCst);
    let before = signals.peer_requests.lock().unwrap().len();
    let caller = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "gated-network").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.ticket.channel, cellar_logic::Channel::Fetch);
    signals.hold.store(false, Ordering::SeqCst);
    gate.reply
        .send(Err(cellar_logic::RequestError::DurabilityUnproven))
        .unwrap();
    assert!(pending.await?.is_err());
    assert_eq!(signals.peer_requests.lock().unwrap().len(), before);
    let caller = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "cancel-network").await });
    signals.peer_pending.notified().await;
    signals.cancel.notify_one();
    assert_eq!(pending.await??.body, b"cancelled");
    signals.peer_closed.notified().await;
    let caller = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "Root:first", "cancel-service").await });
    signals.service_pending.notified().await;
    signals.cancel.notify_one();
    assert_eq!(pending.await??.body, b"cancelled");
    signals.service_dropped.notified().await;
    let before = signals.services.load(Ordering::SeqCst);
    assert_eq!(
        call(&runtime, scope, "pre-aborted-service-rpc").await?.body,
        b"pre-aborted"
    );
    assert_eq!(signals.services.load(Ordering::SeqCst), before);
    for path in [
        "cancel-service-rpc",
        "drop-service-rpc",
        "cancel-service-rpc-gated",
    ] {
        let before = signals.services.load(Ordering::SeqCst);
        let gated = path.ends_with("gated");
        signals.hold.store(gated, Ordering::SeqCst);
        let caller = runtime.clone();
        let pending =
            cellar::asyncrt::spawn(async move { call(&caller, "Root:first", path).await });
        let gate = if gated {
            let gate = held.recv().await.unwrap();
            assert_eq!(gate.ticket.channel, cellar_logic::Channel::Service);
            Some(gate)
        } else {
            signals.service_pending.notified().await;
            None
        };
        signals.cancel.notify_one();
        assert_eq!(pending.await??.body, b"cancelled");
        if let Some(gate) = gate {
            assert!(gate.reply.is_closed());
            assert_eq!(signals.services.load(Ordering::SeqCst), before);
        } else {
            signals.service_dropped.notified().await;
        }
        signals.hold.store(false, Ordering::SeqCst);
        assert_eq!(call(&runtime, scope, "rpc").await?.body, b"rpc");
    }
    // Direct embedders have no SDK guard or external cancel channel.
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move {
        caller
            .rpc_service(
                1,
                "target",
                "entry".into(),
                encode(&props()).unwrap(),
                cellar::WorkerRpcOperation::Call {
                    path: vec!["pending".into()],
                    args: Vec::new(),
                },
            )
            .await
    });
    signals.service_pending.notified().await;
    pending.abort();
    signals.service_dropped.notified().await;
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    cancel.send(()).unwrap();
    let before = signals.services.load(Ordering::SeqCst);
    assert!(runtime
        .rpc_service_cancellable(
            1,
            "target",
            "entry".into(),
            encode(&props())?,
            cellar::WorkerRpcOperation::Call {
                path: vec!["pending".into()],
                args: Vec::new()
            },
            Some(cancelled)
        )
        .await
        .is_err());
    assert_eq!(signals.services.load(Ordering::SeqCst), before);
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let bytes = runtime
        .rpc_service_cancellable(
            1,
            "target",
            "entry".into(),
            encode(&props())?,
            cellar::WorkerRpcOperation::Call {
                path: vec!["background".into()],
                args: encode(&props())?,
            },
            Some(cancelled),
        )
        .await?;
    assert_eq!(decode(&bytes)?, props());
    signals.background_started.notified().await;
    // Completion relinquishes the cancel channel while background work lives.
    assert!(cancel.send(()).is_err());
    signals.background_release.notify_one();
    signals.background_finished.notified().await;
    runtime.swap_out_cell(scope, 1).await?;
    server.abort();
    services.abort();
    rpc.abort();
    gate_driver.abort();
    Ok(signals)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_http_factory_applies_to_every_redirect_mode_and_limited_worker() -> Result<()> {
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let builds = Arc::new(AtomicUsize::new(0));
    let observed = builds.clone();
    let factory = cellar::sdk::HttpClientFactory::new(move || {
        observed.fetch_add(1, Ordering::SeqCst);
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-cellar-host", "caller-owned".parse().unwrap());
        reqwest::Client::builder()
            .no_proxy()
            .default_headers(headers)
            // Cellar must still enforce each invocation's requested mode.
            .redirect(reqwest::redirect::Policy::none())
    });
    domain
        .services()
        .configure_http(cellar::sdk::HttpPolicy::default(), Some(factory))?;
    let signals = domain.run(run()).await??;
    {
        let requests = signals.peer_requests.lock().unwrap();
        assert!(!requests.is_empty());
        assert!(
            requests
                .iter()
                .all(|request| request.contains("x-cellar-host: caller-owned")),
            "a redirect mode bypassed the application's client configuration: {requests:?}"
        );
        assert!(
            builds.load(Ordering::SeqCst) < requests.len(),
            "HTTP clients were rebuilt for every fetch"
        );
    }
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn http_configuration_is_pinned_to_its_host_and_rejects_invalid_deadlines() -> Result<()> {
    use cellar::sdk::{ExecutionDomain, HttpClientFactory, HttpPolicy};
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    for request_timeout in [Duration::ZERO, Duration::MAX] {
        assert!(domain
            .services()
            .configure_http(HttpPolicy { request_timeout }, None)
            .is_err());
    }
    let policy = HttpPolicy {
        request_timeout: Duration::from_secs(3),
    };
    let factory = HttpClientFactory::new(reqwest::Client::builder);
    domain
        .services()
        .configure_http(policy, Some(factory.clone()))?;
    domain
        .services()
        .configure_http(policy, Some(factory.clone()))?;
    assert_eq!(domain.services().http_policy(), policy);
    assert!(domain
        .services()
        .configure_http(HttpPolicy::default(), Some(factory))
        .is_err());
    assert!(domain
        .services()
        .configure_http(
            policy,
            Some(HttpClientFactory::new(reqwest::Client::builder))
        )
        .is_err());
    let defaults = ExecutionDomain::new(tokio::runtime::Handle::current());
    assert_eq!(defaults.services().http_policy(), HttpPolicy::default());
    assert!(defaults.services().configure_http(policy, None).is_err());
    defaults.shutdown().await;
    domain.shutdown().await;
    assert!(domain.services().configure_http(policy, None).is_err());
    Ok(())
}

// Environment changes occur only in an exact-test subprocess.
#[test]
fn command_line_http_policy_resolves_environment_once() -> Result<()> {
    use cellar::sdk::{ExecutionDomain, HttpPolicy};

    const CHILD: &str = "CELLAR_HTTP_POLICY_ADAPTER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("1") {
        let policy = HttpPolicy::from_env()?;
        assert_eq!(policy.request_timeout, Duration::from_secs(3));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let domain = ExecutionDomain::new(runtime.handle().clone());
        domain.services().configure_http(policy, None)?;
        std::env::set_var("CELLAR_FETCH_TIMEOUT_S", "unusable");
        assert!(HttpPolicy::from_env().is_err());
        assert_eq!(domain.services().http_policy(), policy);
        runtime.block_on(domain.shutdown());
        return Ok(());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "command_line_http_policy_resolves_environment_once",
            "--nocapture",
        ])
        .env_clear()
        .env(CHILD, "1")
        .env("CELLAR_FETCH_TIMEOUT_S", "3")
        .output()?;
    anyhow::ensure!(
        output.status.success(),
        "CLI HTTP policy adapter failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
