// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Business namespaces and entity stubs over production native cell turns.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::application::cells::Stub;
use cellar::application::http::{Headers, Response};
use cellar::application::streams::FixedLengthStream;
use cellar::application::{
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use cellar::engine_api::{HttpResponse, RpcData, RuntimeFetch};
use cellar::generation::Generation;
use cellar::host_channels::{set_do_call_tx, set_gate_tx, set_rpc_call_tx};
use cellar::native::clone::{decode, Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

const SCRIPT: &str = "native-cells";
const ALPHA: &str = "customer:α";

fn request(path: &str) -> Request {
    Request {
        url: format!("http://entity/{path}"),
        method: "post".into(),
        headers: Vec::new(),
        body: Vec::new().into(),
        entrypoint: None,
    }
}

fn output(text: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.as_bytes().to_vec(),
        headers: Vec::new(),
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

fn graph() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: vec![Value::Reference(1), Value::Reference(1)],
                properties: Vec::new(),
            },
            Object::Object(vec![
                ("self".into(), Value::Reference(1)),
                ("units".into(), Value::String(vec![0xd800, 0x96ea])),
                ("buffer".into(), Value::Reference(2)),
            ]),
            Object::ArrayBuffer {
                bytes: vec![1, 2, 3],
                max_length: None,
                immutable: false,
            },
        ],
    }
}

#[derive(Default)]
struct Signals {
    stub: Mutex<Option<Stub>>,
    old: Mutex<Option<Context>>,
    calls: AtomicUsize,
    pending_drops: AtomicUsize,
    hold: AtomicBool,
    pending: Notify,
    cancel: Notify,
    dropped: Notify,
    background_started: Notify,
    background_resume: Notify,
    background_done: Notify,
}

struct Pending(Arc<Signals>);

struct ReplyWake {
    runtime: Arc<RuntimeManager>,
    invoked: AtomicBool,
}

impl futures_util::task::ArcWake for ReplyWake {
    fn wake_by_ref(wake: &Arc<Self>) {
        let (_, guard) = wake
            .runtime
            .begin_rpc_reply_wait(cellar::engine_api::next_request_id());
        drop(guard);
        wake.invoked.store(true, Ordering::SeqCst);
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.0.pending_drops.fetch_add(1, Ordering::SeqCst);
        self.0.dropped.notify_one();
    }
}

struct Account(Arc<Signals>);

impl Cell for Account {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            match invocation {
                Invocation::Fetch(incoming) => {
                    assert_eq!(incoming.method, "POST");
                    let path = incoming.url.rsplit('/').next().unwrap();
                    if path == "pending" {
                        let _guard = Pending(self.0.clone());
                        self.0.pending.notify_one();
                        futures_util::future::pending::<()>().await;
                        unreachable!()
                    }
                    if path == "break" {
                        return Err(anyhow!("__CELLAR_ACTOR_ABORT__:entity closed"));
                    }
                    if path == "stream" {
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
                            Response::from_byte_stream(200, &pipe.readable, &Headers::new())?
                                .into_http(&context)?,
                        ));
                    }
                    if path == "name" {
                        return Ok(output(
                            context
                                .identity()
                                .unwrap()
                                .name
                                .as_deref()
                                .unwrap_or("unnamed"),
                        ));
                    }
                    if path == "background-check" {
                        assert_eq!(
                            context.storage()?.get_json("rpc-background")?,
                            Some(serde_json::json!(true))
                        );
                        return Ok(output("background persisted"));
                    }
                    let storage = context.storage()?;
                    let count = storage
                        .get_json("count")?
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0)
                        + 1;
                    storage.put_json("count", &serde_json::json!(count))?;
                    Ok(output(&count.to_string()))
                }
                Invocation::CellRpc { method, args } => {
                    if method == "pending" {
                        let _guard = Pending(self.0.clone());
                        self.0.pending.notify_one();
                        futures_util::future::pending::<()>().await;
                        unreachable!()
                    }
                    if method == "background" {
                        let signals = self.0.clone();
                        let child = context.clone();
                        context.wait_until(async move {
                            signals.background_started.notify_one();
                            signals.background_resume.notified().await;
                            child
                                .storage()?
                                .put_json("rpc-background", &serde_json::json!(true))?;
                            signals.background_done.notify_one();
                            Ok(())
                        })?;
                        return Ok(Output::Rpc(args));
                    }
                    if method == "failure" || method == "exit" {
                        let message = if method == "failure" {
                            "account unavailable"
                        } else {
                            "__CELLAR_PROCESS_EXIT__:instance exited"
                        };
                        return Ok(Output::Rpc(RpcData::Structured(
                            cellar::native::rpc::encode_error(
                                message,
                                "Error",
                                Some("upstream rejected"),
                            )?
                            .into(),
                        )));
                    }
                    assert_eq!(method, "echo");
                    let RpcData::Structured(bytes) = &args else {
                        unreachable!()
                    };
                    assert_eq!(decode(bytes)?, graph());
                    Ok(Output::Rpc(args))
                }
                _ => unreachable!(),
            }
        })
    }
}

struct Coordinator(Arc<Signals>);

impl Coordinator {
    fn stub(&self) -> Stub {
        self.0.stub.lock().unwrap().clone().unwrap()
    }
}

impl Cell for Coordinator {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(incoming) = invocation else {
                unreachable!()
            };
            match incoming.url.rsplit('/').next().unwrap() {
                "init" => {
                    assert!(context.namespace("UNKNOWN").is_err());
                    assert!(context.exported_namespace("missing").is_err());
                    let ns = context.namespace("ACCOUNTS")?;
                    let exported = context.exported_namespace("Account")?;
                    let id = ns.id_from_name(ALPHA);
                    assert_eq!(id, exported.id_from_name(ALPHA));
                    assert!(context.namespace("OTHERS")?.get(&id).is_err());
                    assert!(ns.id_from_string(&"0".repeat(64)).is_err());
                    let stub = ns.get(&id)?;
                    assert_eq!(stub.id().name().as_deref(), Some(ALPHA));
                    assert_eq!(stub.name(), Some(ALPHA));
                    let parsed = ns.id_from_string(&id.to_string().to_uppercase())?;
                    assert_eq!(parsed, id);
                    assert!(parsed.name().is_none());
                    assert_eq!(stub.fetch(&context, request("count")).await?.body, b"1");
                    assert_eq!(
                        ns.get(&parsed)?
                            .fetch(&context, request("count"))
                            .await?
                            .body,
                        b"2"
                    );
                    let beta = ns.get_by_name("warehouse:β");
                    assert_eq!(beta.fetch(&context, request("count")).await?.body, b"1");
                    let long = ns.get_by_name(&"🚀".repeat(257));
                    assert!(long.name().is_none());
                    assert_eq!(
                        long.fetch(&context, request("name")).await?.body,
                        b"unnamed"
                    );
                    *self.0.stub.lock().unwrap() = Some(stub);
                    *self.0.old.lock().unwrap() = Some(context.clone());
                    Ok(output("initialized"))
                }
                "retained" => Ok(Output::Http(
                    self.stub().fetch(&context, request("count")).await?,
                )),
                "name" => Ok(Output::Http(
                    self.stub().fetch(&context, request("name")).await?,
                )),
                "background-check" => Ok(Output::Http(
                    self.stub()
                        .fetch(&context, request("background-check"))
                        .await?,
                )),
                "stale" => {
                    let old = self.0.old.lock().unwrap().take().unwrap();
                    assert!(old.namespace("ACCOUNTS").is_err());
                    assert!(self.stub().fetch(&old, request("count")).await.is_err());
                    Ok(output("stale"))
                }
                "stream" => {
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
                        "http://entity/stream",
                        "post",
                        &Headers::new(),
                        &pipe.readable,
                    )?;
                    let mut response =
                        Response::from_http(self.stub().fetch(&context, request).await?);
                    assert_eq!(response.text(&context).await?, "abcdef");
                    Ok(output("streamed"))
                }
                "rpc" => {
                    assert_eq!(self.stub().call(&context, "echo", &graph()).await?, graph());
                    assert!(self
                        .stub()
                        .call(
                            &context,
                            "echo",
                            &StructuredValue {
                                root: Value::Null,
                                objects: Vec::new()
                            }
                        )
                        .await
                        .is_err());
                    Ok(output("rpc"))
                }
                "rpc-errors" => {
                    let error = self
                        .stub()
                        .call(&context, "failure", &graph())
                        .await
                        .err()
                        .unwrap();
                    let remote = error
                        .downcast_ref::<cellar::native::rpc::RemoteError>()
                        .unwrap();
                    assert_eq!(remote.message, "account unavailable");
                    assert!(matches!(
                        &remote.value.objects[1],
                        Object::Error { cause: Some(_), .. }
                    ));
                    assert_eq!(
                        self.stub().fetch(&context, request("name")).await?.body,
                        ALPHA.as_bytes()
                    );
                    assert_eq!(
                        self.stub()
                            .call(&context, "exit", &graph())
                            .await
                            .err()
                            .unwrap()
                            .to_string(),
                        "instance exited"
                    );
                    let before = self.0.calls.load(Ordering::SeqCst);
                    assert_eq!(
                        self.stub()
                            .call(&context, "echo", &graph())
                            .await
                            .err()
                            .unwrap()
                            .to_string(),
                        "instance exited"
                    );
                    assert_eq!(self.0.calls.load(Ordering::SeqCst), before);
                    let fresh = context.namespace("ACCOUNTS")?.get(self.stub().id())?;
                    *self.0.stub.lock().unwrap() = Some(fresh);
                    Ok(output("remote errors"))
                }
                "route-error" => {
                    let error = self
                        .stub()
                        .fetch(&context, request("route-error"))
                        .await
                        .err()
                        .unwrap();
                    assert!(error
                        .downcast_ref::<cellar::cell_dispatch::RoutedRequestError>()
                        .is_some());
                    assert_eq!(
                        self.stub().fetch(&context, request("name")).await?.body,
                        ALPHA.as_bytes()
                    );
                    Ok(output("retryable"))
                }
                "broken" => {
                    let error = self
                        .stub()
                        .fetch(&context, request("break"))
                        .await
                        .err()
                        .unwrap();
                    assert_eq!(error.to_string(), "entity closed");
                    let before = self.0.calls.load(Ordering::SeqCst);
                    assert_eq!(
                        self.stub()
                            .fetch(&context, request("name"))
                            .await
                            .err()
                            .unwrap()
                            .to_string(),
                        "entity closed"
                    );
                    assert_eq!(
                        self.stub()
                            .rpc(&context, "echo", RpcData::Json("[]".into()))
                            .await
                            .err()
                            .unwrap()
                            .to_string(),
                        "entity closed"
                    );
                    assert_eq!(self.0.calls.load(Ordering::SeqCst), before);
                    let fresh = context.namespace("ACCOUNTS")?.get(self.stub().id())?;
                    assert_eq!(
                        fresh.fetch(&context, request("name")).await?.body,
                        ALPHA.as_bytes()
                    );
                    *self.0.stub.lock().unwrap() = Some(fresh);
                    Ok(output("fresh"))
                }
                "gated" => {
                    context
                        .storage()?
                        .put_json("before-call", &serde_json::json!(true))?;
                    Ok(Output::Http(
                        self.stub().fetch(&context, request("name")).await?,
                    ))
                }
                "cancel" => {
                    let controller = cellar::native::web::AbortController::new();
                    let call = self.stub().fetch_with_signal(
                        &context,
                        request("pending"),
                        controller.signal(),
                    );
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! {"cancel a pending entity fetch";_=self.0.cancel.notified()=>{},_=&mut call=>return Err(anyhow!("pending entity answered")),}
                    controller.abort(None)?;
                    assert!(call.await.is_err());
                    Ok(output("cancelled"))
                }
                "pre-abort" => {
                    let controller = cellar::native::web::AbortController::new();
                    controller.abort(None)?;
                    let before = self.0.calls.load(Ordering::SeqCst);
                    assert!(self
                        .stub()
                        .fetch_with_signal(&context, request("pending"), controller.signal())
                        .await
                        .is_err());
                    assert_eq!(self.0.calls.load(Ordering::SeqCst), before);
                    assert!(self
                        .stub()
                        .rpc_with_signal(
                            &context,
                            "pending",
                            RpcData::Json("[]".into()),
                            controller.signal()
                        )
                        .await
                        .is_err());
                    assert!(self
                        .stub()
                        .call_with_signal(&context, "pending", &graph(), controller.signal())
                        .await
                        .is_err());
                    assert_eq!(self.0.calls.load(Ordering::SeqCst), before);
                    assert_eq!(
                        self.stub().fetch(&context, request("name")).await?.body,
                        ALPHA.as_bytes()
                    );
                    Ok(output("pre-aborted"))
                }
                "cancel-rpc" | "cancel-gated-rpc" => {
                    if incoming.url.ends_with("cancel-gated-rpc") {
                        context
                            .storage()?
                            .put_json("before-cancelled-rpc", &serde_json::json!(true))?;
                    }
                    let controller = cellar::native::web::AbortController::new();
                    let call = self.stub().call_with_signal(
                        &context,
                        "pending",
                        &graph(),
                        controller.signal(),
                    );
                    tokio::pin!(call);
                    cellar::asyncrt::select_biased! { "abort a pending structured cell RPC";
                        _=self.0.cancel.notified()=>{},
                        _=&mut call=>return Err(anyhow!("pending RPC answered")),
                    }
                    let reason = cellar::native::web::AbortReason::error("RPC caller stopped");
                    controller.abort(Some(reason.clone()))?;
                    let error = call.await.err().unwrap();
                    assert!(error
                        .downcast_ref::<cellar::native::web::AbortReason>()
                        .unwrap()
                        .ptr_eq(&reason));
                    Ok(output("rpc cancelled"))
                }
                _ => unreachable!(),
            }
        })
    }
}

async fn call(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            "Coordinator:root".into(),
            None,
            RuntimeFetch {
                url: format!("http://coordinator/{path}"),
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

async fn own(runtime: &RuntimeManager, scope: &str) -> Result<()> {
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
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_business_namespaces_identity_streams_rpc_gates_retirement_and_cancellation(
) -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let coordinator = signals.clone();
    let accounts = signals.clone();
    let others = signals.clone();
    let app = Registry::new(|_, _| Box::pin(async { Ok(output("default")) }))
        .cell("Coordinator", move |_, _| {
            Ok(Arc::new(Coordinator(coordinator.clone())))
        })?
        .cell("Account", move |_, _| {
            Ok(Arc::new(Account(accounts.clone())))
        })?
        .cell("Other", move |_, _| Ok(Arc::new(Account(others.clone()))))?;
    let mut deployment = fixture::graph(
        SCRIPT,
        "this is not JavaScript",
        vec!["Coordinator".into(), "Account".into(), "Other".into()],
    );
    deployment.primary.options.bindings = vec![
        ("ACCOUNTS".into(), "Account".into()),
        ("OTHERS".into(), "Other".into()),
    ];
    let generation = Generation::build_native(
        1,
        deployment,
        fixture::generation_options(),
        std::collections::HashMap::from([(SCRIPT.into(), Arc::new(app) as Arc<dyn Application>)]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = Arc::new(RuntimeManager::start(
        generation,
        fixture::runtime_options(directory.path().into()),
    )?);
    let ns = cellar::native::cell_id::Namespace::new(
        "Account",
        format!("cells:v1:{}:{SCRIPT}:Account", SCRIPT.len()),
    );
    own(&runtime, "Coordinator:root").await?;
    for name in [ALPHA.to_string(), "warehouse:β".into(), "🚀".repeat(257)] {
        own(&runtime, &ns.id_from_name(&name).scope()).await?;
    }
    let (send, mut calls) = mpsc::unbounded_channel();
    set_do_call_tx(send);
    let routing = runtime.clone();
    let fetches = cellar::asyncrt::spawn(async move {
        while let Some(call) = calls.recv().await {
            let routing = routing.clone();
            cellar::asyncrt::spawn(async move {
                if call.url.ends_with("route-error") {
                    let _ = call.reply.send(Err(anyhow::Error::new(
                        cellar::cell_dispatch::RoutedRequestError(
                            cellar_logic::RequestError::NodeUnavailable,
                        ),
                    )));
                    return;
                }
                let mut guard = call.body_guard;
                let response = routing
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
                if response.is_ok() {
                    guard.disarm();
                }
                let _ = call.reply.send(response);
            })
            .detach();
        }
    });
    let (send, mut calls) = mpsc::unbounded_channel();
    set_rpc_call_tx(send);
    let routing = runtime.clone();
    let rpcs = cellar::asyncrt::spawn(async move {
        while let Some(call) = calls.recv().await {
            let response = routing
                .rpc_cancellable(
                    call.scope,
                    call.name,
                    call.method,
                    call.args,
                    call.request_id,
                    call.cancel,
                )
                .await
                .map(|out| out.data);
            let _ = call.reply.send(response);
        }
    });
    let (send, mut gates) = mpsc::unbounded_channel();
    set_gate_tx(send);
    let gating = signals.clone();
    let (held_send, mut held) = mpsc::unbounded_channel();
    let gates = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gating.hold.load(Ordering::SeqCst)
                && gate.ticket.channel == cellar_logic::Channel::CellRpc
            {
                held_send.send(gate).unwrap();
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
    for (path, expected) in [
        ("init", "initialized"),
        ("retained", "3"),
        ("name", ALPHA),
        ("stale", "stale"),
        ("stream", "streamed"),
        ("rpc", "rpc"),
        ("rpc-errors", "remote errors"),
        ("route-error", "retryable"),
        ("broken", "fresh"),
        ("pre-abort", "pre-aborted"),
    ] {
        assert_eq!(call(&runtime, path).await?.body, expected.as_bytes());
    }
    signals.hold.store(true, Ordering::SeqCst);
    let before = signals.calls.load(Ordering::SeqCst);
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "gated").await });
    let gate = held.recv().await.unwrap();
    assert_eq!(gate.ticket.epoch, Some(1));
    assert_eq!(signals.calls.load(Ordering::SeqCst), before);
    signals.hold.store(false, Ordering::SeqCst);
    gate.reply.send(Ok(())).unwrap();
    assert_eq!(pending.await??.body, ALPHA.as_bytes());
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "cancel").await });
    signals.pending.notified().await;
    signals.cancel.notify_one();
    assert_eq!(pending.await??.body, b"cancelled");
    signals.dropped.notified().await;
    assert_eq!(signals.pending_drops.load(Ordering::SeqCst), 1);
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "cancel-rpc").await });
    signals.pending.notified().await;
    signals.cancel.notify_one();
    assert_eq!(pending.await??.body, b"rpc cancelled");
    signals.dropped.notified().await;
    assert_eq!(signals.pending_drops.load(Ordering::SeqCst), 2);
    assert_eq!(call(&runtime, "name").await?.body, ALPHA.as_bytes());

    // Cancelling before the output gate opens never sends an RPC packet.
    signals.hold.store(true, Ordering::SeqCst);
    let before = signals.calls.load(Ordering::SeqCst);
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "cancel-gated-rpc").await });
    let gate = held.recv().await.unwrap();
    signals.cancel.notify_one();
    assert_eq!(pending.await??.body, b"rpc cancelled");
    assert!(gate.reply.is_closed());
    signals.hold.store(false, Ordering::SeqCst);
    assert_eq!(signals.calls.load(Ordering::SeqCst), before);

    let alpha = ns.id_from_name(ALPHA).scope();
    // A caller that drops its runtime wait also cancels an unwakeable handler.
    let caller = runtime.clone();
    let target = alpha.clone();
    let pending = cellar::asyncrt::spawn(async move {
        caller
            .rpc(
                target,
                None,
                "pending".into(),
                RpcData::Json("[]".into()),
                None,
            )
            .await
    });
    signals.pending.notified().await;
    pending.abort();
    assert!(pending.await.is_err());
    signals.dropped.notified().await;
    assert_eq!(signals.pending_drops.load(Ordering::SeqCst), 3);

    // An authenticated abort arriving ahead of dispatch prevents execution.
    let request_id = cellar::engine_api::next_request_id();
    runtime.abort_fetch(&alpha, request_id);
    let before = signals.calls.load(Ordering::SeqCst);
    let (cancel, reply_wait) = runtime.begin_rpc_reply_wait(request_id);
    let invoked = AtomicBool::new(false);
    assert!(cellar::cell_dispatch::receive_rpc(
        async {
            invoked.store(true, Ordering::SeqCst);
            Ok(())
        },
        Some(cancel)
    )
    .await
    .is_err());
    assert!(!invoked.load(Ordering::SeqCst));
    drop(reply_wait);
    assert!(runtime
        .rpc(
            alpha.clone(),
            None,
            "pending".into(),
            RpcData::Json("[]".into()),
            Some(request_id)
        )
        .await
        .is_err());
    assert_eq!(signals.calls.load(Ordering::SeqCst), before);

    // A peer abort releases a held response after the handler returned,
    // without retracting that handler's retained background work.
    let request_id = cellar::engine_api::next_request_id();
    let (cancel, reply_wait) = runtime.begin_rpc_reply_wait(request_id);
    let (held, holding) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let caller = runtime.clone();
    let target = alpha.clone();
    let arguments = cellar::application::encode_rpc(&graph())?;
    let pending = cellar::asyncrt::spawn(async move {
        cellar::cell_dispatch::receive_rpc(
            async move {
                let result = caller
                    .rpc(
                        target,
                        None,
                        "background".into(),
                        arguments,
                        Some(request_id),
                    )
                    .await?;
                let _held_reply_resource = release;
                held.send(()).unwrap();
                futures_util::future::pending::<()>().await;
                Ok(result)
            },
            Some(cancel),
        )
        .await
    });
    holding.await?;
    signals.background_started.notified().await;
    runtime.abort_fetch(&alpha, request_id);
    assert!(pending.await?.is_err());
    assert!(
        released.await.is_err(),
        "the held reply's resource must drop"
    );
    drop(reply_wait);
    // Retired/replaced reply guards cannot remove a newer registration.
    let id = cellar::engine_api::next_request_id();
    let (old_receive, old_guard) = runtime.begin_rpc_reply_wait(id);
    let (mut new_receive, new_guard) = runtime.begin_rpc_reply_wait(id);
    assert!(old_receive.await.is_err());
    drop(old_guard);
    assert!(matches!(
        new_receive.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    runtime.abort_fetch(&alpha, id);
    new_receive.await?;
    drop(new_guard);
    // Reply publication and guard retirement wake arbitrary consumers outside
    // the registry lock, including one that reenters that same registry.
    for explicit_abort in [false, true] {
        let id = cellar::engine_api::next_request_id();
        let (mut receive, guard) = runtime.begin_rpc_reply_wait(id);
        let wake = Arc::new(ReplyWake {
            runtime: runtime.clone(),
            invoked: AtomicBool::new(false),
        });
        {
            let waker = futures_util::task::waker(wake.clone());
            let mut cx = std::task::Context::from_waker(&waker);
            assert!(
                std::future::Future::poll(std::pin::Pin::new(&mut receive), &mut cx).is_pending()
            );
        }
        if explicit_abort {
            runtime.abort_fetch(&alpha, id);
            receive.await?;
        }
        drop(guard);
        assert!(wake.invoked.load(Ordering::SeqCst));
    }
    signals.background_resume.notify_one();
    signals.background_done.notified().await;
    runtime.swap_out_cell(&alpha, 1).await?;
    runtime.start_cell(alpha.clone(), 1, false).await?;
    runtime.publish_cell(&alpha, 1)?;
    assert_eq!(call(&runtime, "retained").await?.body, b"4");
    assert_eq!(call(&runtime, "name").await?.body, ALPHA.as_bytes());
    assert_eq!(
        call(&runtime, "background-check").await?.body,
        b"background persisted"
    );
    runtime.swap_out_cell("Coordinator:root", 1).await?;
    runtime.swap_out_cell(&alpha, 1).await?;
    fetches.abort();
    rpcs.abort();
    gates.abort();
    Ok(())
}
