// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native cancellation uses the production event driver and routed call guard.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::{
    application::{
        events::*, http::Response, Cell, Context, HandlerFuture, Invocation, Output, Registry,
        Request, Storage,
    },
    engine_api::{HttpResponse, RequestBody, RuntimeFetch},
    generation::Generation,
    native::clone::{StructuredValue, Value},
    runtime::RuntimeManager,
};
use futures_util::Stream;
use serde_json::{json, Value as Json};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::sync::Notify;

const SOURCE: &str = "Source:one";
const TARGET: &str = "Target:one";

#[derive(Default)]
struct Probe {
    controllers: Mutex<HashMap<String, AbortController>>,
    target_signal: Mutex<Option<AbortSignal>>,
    unused: Mutex<Option<AbortSignal>>,
    retiring: Mutex<Option<AbortSignal>>,
    old_context: Mutex<Option<Context>>,
    gate_started: Notify,
    gate_release: Notify,
    target_started: Notify,
    target_aborted: Notify,
    background_release: Notify,
    background_done: Notify,
    timer_started: Notify,
    hold_gate: AtomicBool,
    calls: AtomicUsize,
    gates: AtomicUsize,
    callback_errors: AtomicUsize,
}

#[derive(Default)]
struct BodyProbe {
    polls: AtomicUsize,
    dropped: AtomicBool,
    entered: Notify,
}

struct PendingBody(Arc<BodyProbe>);

impl Stream for PendingBody {
    type Item = std::result::Result<Vec<u8>, String>;

    fn poll_next(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.0.polls.fetch_add(1, Ordering::SeqCst);
        self.0.entered.notify_one();
        Poll::Pending
    }
}

impl Drop for PendingBody {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
    }
}

fn body() -> Result<(RequestBody, Arc<BodyProbe>)> {
    let probe = Arc::new(BodyProbe::default());
    let id = cellar::http_streams::register_body_stream(Box::pin(PendingBody(probe.clone())))
        .map_err(anyhow::Error::msg)?;
    Ok((RequestBody::Stream(id), probe))
}

fn output(context: &Context, report: Json) -> Result<Output> {
    Ok(Output::Http(
        Response::new(200, serde_json::to_vec(&report)?, Vec::new()).into_http(context)?,
    ))
}

struct Source(Arc<Probe>, Storage);

impl Cell for Source {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected source fetch"));
            };
            let path = url::Url::parse(&request.url)?.path().to_string();
            if path == "/unused" {
                *self.0.unused.lock().unwrap() = Some(context.abort_timeout(20.0)?);
                return output(&context, json!("unused"));
            }
            if path == "/retire" {
                let signal = context.abort_timeout(80.0)?;
                *self.0.retiring.lock().unwrap() = Some(signal.clone());
                *self.0.old_context.lock().unwrap() = Some(context.clone());
                self.0.timer_started.notify_one();
                signal.cancelled().await;
                return Err(anyhow!("retired timer resumed"));
            }
            if path == "/verify" {
                let old = self.0.old_context.lock().unwrap().clone().unwrap();
                assert!(old.abort_timeout(0.0).is_err());
                assert!(context.request_signal().is_some());
                return output(
                    &context,
                    json!({"stored":self.1.get_json("committed")?,"oldSignalGone":old.request_signal().is_none()}),
                );
            }
            let controller = AbortController::new();
            if path == "/pre" {
                controller.abort(Some(AbortReason::from_value(StructuredValue {
                    root: Value::Null,
                    objects: Vec::new(),
                })))?;
            }
            let signal = if path == "/timeout" {
                context.abort_timeout(2.0)?
            } else {
                controller.signal()
            };
            self.0
                .controllers
                .lock()
                .unwrap()
                .insert(path.clone(), controller);
            if path == "/gate" {
                self.1.put_json("committed", &json!("before abort"))?;
            }
            let destination = match path.as_str() {
                "/routed" => "/wait",
                "/ok" => "/ok",
                _ => "/plain",
            };
            let request = Request {
                url: format!("https://target{destination}"),
                method: "POST".into(),
                headers: Vec::new(),
                body: request.body,
                entrypoint: None,
            };
            match context
                .fetch_cell_with_signal(TARGET.into(), None, request, signal.clone())
                .await
            {
                Ok(response) => Ok(Output::Http(response)),
                Err(error) => {
                    let reason = error
                        .downcast_ref::<AbortReason>()
                        .ok_or_else(|| anyhow!("routed abort lost its reason: {error}"))?;
                    let exception = reason.dom_exception();
                    output(
                        &context,
                        json!({"same":reason.ptr_eq(&signal.reason().unwrap()),"code":exception.as_ref().map(DomException::code),"name":exception.as_ref().map(DomException::name)}),
                    )
                }
            }
        })
    }
}

struct Target(Arc<Probe>, Storage);

impl Cell for Target {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected target fetch"));
            };
            let path = url::Url::parse(&request.url)?.path().to_string();
            let signal = context.request_signal().expect("incoming signal");
            *self.0.target_signal.lock().unwrap() = Some(signal.clone());
            if path == "/ok" {
                return output(&context, json!("ok"));
            }
            if path == "/inspect" {
                return output(
                    &context,
                    json!({"disconnected":self.1.get_json("disconnected")?,"background":self.1.get_json("background")?}),
                );
            }
            let owner = context.clone();
            let storage = self.1.clone();
            let probe = self.0.clone();
            let background = path == "/wait";
            let listener = Listener::new(move |target, event| {
                let signal = owner
                    .request_signal()
                    .expect("signal remains owned while aborting");
                assert!(target.ptr_eq(&signal.event_target()));
                assert_eq!(event.event_phase(), Event::AT_TARGET);
                assert!(signal.aborted());
                assert_eq!(
                    signal.reason().unwrap().web_error().unwrap().to_string(),
                    "The client has disconnected"
                );
                let result = (|| -> Result<()> {
                    storage.put_json("disconnected", &json!(true))?;
                    if background {
                        let probe = probe.clone();
                        let storage = storage.clone();
                        owner.wait_until(async move {
                            probe.background_release.notified().await;
                            storage.put_json("background", &json!(true))?;
                            probe.background_done.notify_one();
                            Ok(())
                        })?;
                    }
                    Ok(())
                })();
                if result.is_err() {
                    probe.callback_errors.fetch_add(1, Ordering::SeqCst);
                }
                probe.target_aborted.notify_one();
                result.map_err(|error| {
                    EventDispatchError::Thrown(AbortReason::error(&error.to_string()))
                })
            });
            signal.set_on_abort(Some(listener));
            let mut reader = request.into_body_reader(&context)?;
            self.0.target_started.notify_one();
            reader.bytes(&context).await?;
            Err(anyhow!("pending input unexpectedly completed"))
        })
    }
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

fn request(path: &str, body: RequestBody) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("https://source{path}"),
        method: "POST".into(),
        body,
        headers: Vec::new(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}

async fn call(
    runtime: &RuntimeManager,
    scope: &str,
    path: &str,
    body: RequestBody,
) -> Result<HttpResponse> {
    runtime
        .fetch_cell(scope.into(), None, request(path, body), None)
        .await
}

async fn dropped(probe: &BodyProbe) -> Result<()> {
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while !probe.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_abort_routes_preserve_proofs_incoming_cleanup_background_and_timer_retirement(
) -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let probe = Arc::new(Probe::default());
    let source = probe.clone();
    let target = probe.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
        .cell("Source", move |c, _| {
            Ok(Arc::new(Source(source.clone(), c.storage()?)))
        })?
        .cell("Target", move |c, _| {
            Ok(Arc::new(Target(target.clone(), c.storage()?)))
        })?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        Generation::build_native(
            1,
            fixture::graph("native-events", "", vec!["Source".into(), "Target".into()]),
            fixture::generation_options(),
            HashMap::from([(
                "native-events".into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        fixture::runtime_options(directory.path().join("cells")),
    )?;
    open(&runtime, SOURCE).await?;
    open(&runtime, TARGET).await?;
    let (stop, mut gate_stop) = tokio::sync::watch::channel(false);
    let (sender, mut gates) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    cellar::host_channels::set_gate_tx(sender);
    let p = probe.clone();
    let gate_adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"event proof adapter stops";_=gate_stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            p.gates.fetch_add(1, Ordering::SeqCst);
            if gate.ticket.channel == cellar_logic::Channel::CellRpc
                && gate.ticket.position.is_some()
                && !p.hold_gate.swap(true, Ordering::SeqCst)
            {
                assert_eq!(gate.ticket.epoch, Some(1));
                p.gate_started.notify_one();
                p.gate_release.notified().await;
            }
            let _ = gate.reply.send(Ok(()));
        }
    });
    let (sender, mut calls) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::DoCallReq>();
    cellar::host_channels::set_do_call_tx(sender);
    let mut call_stop = stop.subscribe();
    let host = runtime.clone();
    let p = probe.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {"event route adapter stops";_=call_stop.changed()=>None,call=calls.recv()=>call,}
        {
            p.calls.fetch_add(1, Ordering::SeqCst);
            assert!(call.deliver_abort_to_handler);
            let result = host
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
    let (input, producer) = body()?;
    let host = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&host, SOURCE, "/gate", input).await });
    probe.gate_started.notified().await;
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    let controller = probe.controllers.lock().unwrap()["/gate"].clone();
    controller.abort(Some(AbortReason::from_value(StructuredValue {
        root: "abort behind proof".into(),
        objects: Vec::new(),
    })))?;
    let response = pending.await??;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?,
        json!({"same":true,"code":null,"name":null})
    );
    dropped(&producer).await?;
    assert_eq!(producer.polls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    probe.gate_release.notify_one();
    let before = probe.gates.load(Ordering::SeqCst);
    let (input, producer) = body()?;
    let response = call(&runtime, SOURCE, "/pre", input).await?;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?["same"],
        true
    );
    dropped(&producer).await?;
    assert_eq!(producer.polls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.gates.load(Ordering::SeqCst), before);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    let (input, producer) = body()?;
    let host = runtime.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&host, SOURCE, "/routed", input).await });
    probe.target_started.notified().await;
    producer.entered.notified().await;
    assert_eq!(producer.polls.load(Ordering::SeqCst), 1);
    probe.controllers.lock().unwrap()["/routed"].abort(Some(AbortReason::error("caller abort")))?;
    let response = pending.await??;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?["same"],
        true
    );
    probe.target_aborted.notified().await;
    dropped(&producer).await?;
    assert!(probe
        .target_signal
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .aborted());
    assert_eq!(probe.callback_errors.load(Ordering::SeqCst), 0);
    probe.background_release.notify_one();
    probe.background_done.notified().await;
    let stored = call(
        &runtime,
        TARGET,
        "/inspect",
        RequestBody::Bytes(Vec::new().into()),
    )
    .await?;
    assert_eq!(
        serde_json::from_slice::<Json>(&stored.body)?,
        json!({"disconnected":true,"background":true})
    );
    let (input, producer) = body()?;
    let response = call(&runtime, SOURCE, "/timeout", input).await?;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?,
        json!({"same":true,"code":23,"name":"TimeoutError"})
    );
    probe.target_aborted.notified().await;
    dropped(&producer).await?;
    assert_eq!(probe.callback_errors.load(Ordering::SeqCst), 0);
    let response = call(
        &runtime,
        SOURCE,
        "/ok",
        RequestBody::Bytes(Vec::new().into()),
    )
    .await?;
    assert_eq!(serde_json::from_slice::<Json>(&response.body)?, json!("ok"));
    let target_signal = probe.target_signal.lock().unwrap().clone().unwrap();
    probe.controllers.lock().unwrap()["/ok"].abort(None)?;
    assert!(!target_signal.aborted());
    call(
        &runtime,
        SOURCE,
        "/unused",
        RequestBody::Bytes(Vec::new().into()),
    )
    .await?;
    cellar::asyncrt::sleep(Duration::from_millis(40)).await;
    assert!(!probe.unused.lock().unwrap().as_ref().unwrap().aborted());
    let host = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move {
        call(
            &host,
            SOURCE,
            "/retire",
            RequestBody::Bytes(Vec::new().into()),
        )
        .await
    });
    probe.timer_started.notified().await;
    runtime.swap_out_cell(SOURCE, 1).await?;
    assert!(pending.await?.is_err());
    cellar::asyncrt::sleep(Duration::from_millis(120)).await;
    assert!(!probe.retiring.lock().unwrap().as_ref().unwrap().aborted());
    open(&runtime, SOURCE).await?;
    let response = call(
        &runtime,
        SOURCE,
        "/verify",
        RequestBody::Bytes(Vec::new().into()),
    )
    .await?;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?,
        json!({"stored":"before abort","oldSignalGone":true})
    );
    stop.send(true)?;
    adapter.await?;
    gate_adapter.await?;
    Ok(())
}
