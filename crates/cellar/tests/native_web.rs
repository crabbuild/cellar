// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Native Web values cross routed cell turns without initializing an engine.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::application::{
    http::{Headers, Response, Url},
    Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use cellar::{
    engine_api::{HttpResponse, RequestBody, RuntimeFetch},
    generation::Generation,
    runtime::RuntimeManager,
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
const SOURCE: &str = "Source:one";
const PEER: &str = "Peer:one";
#[derive(Default)]
struct Signals {
    entered: Notify,
    release: Notify,
    held: AtomicBool,
    calls: Mutex<Vec<Vec<(String, String)>>>,
    gates: Mutex<Vec<cellar::actor::GateTicket>>,
    retained: Mutex<Option<(Headers, Url)>>,
}
struct Source(Arc<Signals>);
impl Cell for Source {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(incoming) = invocation else {
                return Err(anyhow!("fetch required"));
            };
            let url = incoming.parsed_url()?;
            let mode = url.pathname();
            if mode == "/stored" {
                let value = context.storage()?.get_json("headers")?;
                return Ok(Output::Http(
                    Response::new(200, serde_json::to_vec(&value)?, Vec::new())
                        .into_http(&context)?,
                ));
            }
            if mode == "/detach" {
                let mut response = Response::new(200, "detached", Vec::new());
                let old = response.header_list()?;
                old.append("Before", "one")?;
                response.headers_mut().push(("Raw".into(), "two".into()));
                old.append("Detached", "ignored")?;
                let current = response.header_list()?;
                assert!(!current.has("detached")?);
                current.append("Live", "three")?;
                return Ok(Output::Http(response.into_http(&context)?));
            }
            let h = incoming.header_list()?;
            assert_eq!(h.get("x-repeat")?.as_deref(), Some("first, second"));
            h.append("Set-Cookie", "a=1")?;
            h.append("set-cookie", "b=2")?;
            h.set("X-Author", "Mixed")?;
            context.storage()?.put_json("headers", &json!(h.wire()))?;
            url.set_hostname("Peer.EXAMPLE");
            url.set_pathname("target");
            let params = url.search_params();
            params.append("雪", "a b")?;
            let mut request = Request::new(
                &url.href(),
                "poſt",
                &h,
                RequestBody::Bytes(b"owned body".to_vec().into()),
            )?;
            // The constructor copies the iteration view. Explicit raw list assignment
            // forwards the caller's casing and repeated entries at the transport seam.
            assert_eq!(request.headers[0].0, "set-cookie");
            request.set_header_list(&h)?;
            assert_eq!(request.headers, h.wire());
            let malformed = h.copy_list();
            malformed.append_utf16(&"X-Lone".encode_utf16().collect::<Vec<_>>(), &[0xd800])?;
            assert!(request.set_header_list(&malformed).is_err());
            assert_eq!(request.headers, h.wire());
            assert!(Request::new(
                "http://peer/",
                "GET",
                &malformed,
                RequestBody::Bytes(Vec::new().into())
            )
            .is_err());
            assert!(
                Request::new("relative", "GET", &h, RequestBody::Bytes(Vec::new().into())).is_ok()
            );
            assert!(Request::new(
                "http://peer/",
                "TRACE",
                &h,
                RequestBody::Bytes(Vec::new().into())
            )
            .is_err());
            let peer = context.fetch_cell(PEER.into(), None, request).await?;
            let mut response = Response::from_http(peer);
            let list = response.header_list()?;
            let alias = list.clone();
            let mut iterator = list.entries();
            assert!(iterator.next().is_some());
            cellar::asyncrt::sleep(Duration::from_millis(1)).await;
            alias.append("X-Async", "after-await")?;
            list.append("Set-Cookie", "returned=1")?;
            list.append("set-cookie", "returned=2")?;
            context.storage()?.put_json("returned", &json!(true))?;
            assert_eq!(response.headers(), list.wire());
            *self.0.retained.lock().unwrap() = Some((list, url));
            Ok(Output::Http(response.into_http(&context)?))
        })
    }
}
struct Peer;
impl Cell for Peer {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("fetch required"));
            };
            assert_eq!(request.method, "POST");
            assert_eq!(request.url, "http://Peer.EXAMPLE/target?%E9%9B%AA=a+b");
            let h = request.header_list()?;
            assert_eq!(h.get_set_cookie(), ["a=1", "b=2"]);
            let u = request.parsed_url()?;
            assert_eq!(u.hostname(), "peer.example");
            assert_eq!(u.search_params().get("雪").as_deref(), Some("a b"));
            let RequestBody::Bytes(body) = request.body else {
                return Err(anyhow!("owned bytes required"));
            };
            assert_eq!(body.as_ref(), b"owned body");
            context.storage()?.put_json("received", &json!(h.wire()))?;
            let mut response = Response::new(
                202,
                b"peer".to_vec(),
                vec![("X-Peer".into(), "received".into())],
            );
            let list = response.header_list()?;
            list.set("x-peer", "edited")?;
            Ok(Output::Http(response.into_http(&context)?))
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
async fn call(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            SOURCE.into(),
            None,
            RuntimeFetch {
                url: format!("http://source{path}"),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: vec![
                    ("X-Repeat".into(), "first".into()),
                    ("x-repeat".into(), "second".into()),
                ],
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_headers_and_urls_retain_wire_shape_across_cell_calls() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let source = signals.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
        .cell("Source", move |_, _| Ok(Arc::new(Source(source.clone()))))?
        .cell("Peer", |_, _| Ok(Arc::new(Peer)))?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        Generation::build_native(
            1,
            fixture::graph("native-web", "", vec!["Source".into(), "Peer".into()]),
            fixture::generation_options(),
            HashMap::from([(
                "native-web".into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        fixture::runtime_options(directory.path().join("cells")),
    )?;
    open(&runtime, SOURCE).await?;
    open(&runtime, PEER).await?;
    let (stop, mut gate_stop) = tokio::sync::watch::channel(false);
    let (send, mut gates) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    cellar::host_channels::set_gate_tx(send);
    let captured = signals.clone();
    let gate_adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"Web proof adapter stops";_=gate_stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            let hold = gate.ticket.channel == cellar_logic::Channel::CellRpc
                && gate.ticket.position.is_some()
                && !captured.held.swap(true, Ordering::SeqCst);
            captured.gates.lock().unwrap().push(gate.ticket);
            if hold {
                captured.entered.notify_one();
                captured.release.notified().await;
            }
            let _ = gate.reply.send(Ok(()));
        }
    });
    let (send, mut calls) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::DoCallReq>();
    cellar::host_channels::set_do_call_tx(send);
    let mut call_stop = stop.subscribe();
    let host = runtime.clone();
    let captured = signals.clone();
    let call_adapter = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {"Web route adapter stops";_=call_stop.changed()=>None,call=calls.recv()=>call,}
        {
            captured.calls.lock().unwrap().push(call.headers.clone());
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
    let caller = runtime.clone();
    let pending = cellar::asyncrt::spawn(async move { call(&caller, "/send").await });
    signals.entered.notified().await;
    assert!(signals.calls.lock().unwrap().is_empty());
    signals.release.notify_one();
    let response = pending.await??;
    assert_eq!(response.status, 202);
    assert_eq!(response.body, b"peer");
    assert!(response.write_position.is_some());
    assert_eq!(
        response.headers,
        vec![
            ("set-cookie".into(), "returned=1".into()),
            ("set-cookie".into(), "returned=2".into()),
            ("x-async".into(), "after-await".into()),
            ("x-peer".into(), "edited".into())
        ]
    );
    let sent = signals.calls.lock().unwrap()[0].clone();
    assert_eq!(sent[0], ("X-Repeat".into(), "first".into()));
    assert_eq!(sent[1], ("X-Repeat".into(), "second".into()));
    assert!(signals
        .gates
        .lock()
        .unwrap()
        .iter()
        .any(|ticket| ticket.channel == cellar_logic::Channel::CellRpc
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    let detached = call(&runtime, "/detach").await?;
    assert_eq!(
        detached.headers,
        vec![
            ("before".into(), "one".into()),
            ("live".into(), "three".into()),
            ("raw".into(), "two".into())
        ]
    );
    runtime.swap_out_cell(SOURCE, 1).await?;
    runtime.start_cell(SOURCE.into(), 1, false).await?;
    runtime.publish_cell(SOURCE, 1)?;
    let stored = call(&runtime, "/stored").await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored.body)?,
        json!(sent)
    );
    // Web value aliases carry data and can outlive an event or activation.
    let (headers, url) = signals.retained.lock().unwrap().take().unwrap();
    headers.set("X-Peer", "outside")?;
    url.search_params().append("retained", "yes")?;
    assert_eq!(url.search_params().get("retained").as_deref(), Some("yes"));
    stop.send(true)?;
    gate_adapter.await?;
    call_adapter.await?;
    Ok(())
}
