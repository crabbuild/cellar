//! Native byte pipes cross real runtime turns and existing routing/proof gates.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::{
    application::{
        http::{Headers, Response},
        streams::*,
        Cell, Context, HandlerFuture, Invocation, Output, Registry, Request, Storage,
    },
    engine_api::{HttpResponse, RequestBody, RuntimeFetch},
    generation::Generation,
    runtime::RuntimeManager,
};
use futures_util::StreamExt;
use serde_json::{json, Value as Json};
use std::{
    collections::HashMap,
    future::Future,
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
const PEER: &str = "Peer:one";
#[derive(Default)]
struct Probe {
    hold: AtomicBool,
    calls: AtomicUsize,
    acks: AtomicUsize,
    gate_entered: Notify,
    gate_release: Notify,
    first_queued: Notify,
    produced: Notify,
    discarded: Notify,
    retiring: Notify,
    retired: AtomicBool,
    retirement_done: Notify,
    old: Mutex<Option<Context>>,
    readable: Mutex<Option<ByteReadable>>,
}
struct Never(Arc<Probe>);
impl Future for Never {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Self::Output> {
        self.0.retiring.notify_one();
        Poll::Pending
    }
}
impl Drop for Never {
    fn drop(&mut self) {
        self.0.retired.store(true, Ordering::SeqCst);
        self.0.retirement_done.notify_one();
    }
}
struct Source(Arc<Probe>, Storage);
fn output(context: &Context, value: Json) -> Result<Output> {
    Ok(Output::Http(
        Response::new(200, serde_json::to_vec(&value)?, Vec::new()).into_http(context)?,
    ))
}
impl Cell for Source {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected fetch"));
            };
            let path = url::Url::parse(&request.url)?.path().to_owned();
            if path == "/verify" {
                let old = self.0.old.lock().unwrap().clone().unwrap();
                let fresh = IdentityTransformStream::new();
                assert!(old
                    .wait_until_byte_stream(&fresh.readable, async { Ok(()) })
                    .is_err());
                assert!(old.storage().is_err());
                assert!(Request::with_byte_stream(
                    &old,
                    "https://peer",
                    "POST",
                    &Headers::new(),
                    &fresh.readable
                )
                .is_err());
                // Refusing an old context must not claim or fail an unrelated pipe.
                let r = fresh.readable.get_reader()?;
                let w = fresh.writable.get_writer()?;
                let written = w.write(vec![9]);
                assert_eq!(r.read().await?.value.unwrap().bytes(), [9]);
                written.await?;
                w.close().await?;
                return output(
                    &context,
                    json!({"committed":self.1.get_json("committed")?,"acks":self.1.get_json("acks")?,"discarded":self.1.get_json("discarded")?,"retired":self.0.retired.load(Ordering::SeqCst)}),
                );
            }
            if path == "/retire" {
                let pipe = IdentityTransformStream::new();
                *self.0.old.lock().unwrap() = Some(context.clone());
                *self.0.readable.lock().unwrap() = Some(pipe.readable.clone());
                context.wait_until_byte_stream(&pipe.readable, Never(self.0.clone()))?;
                return Ok(Output::Http(
                    Response::from_byte_stream(200, &pipe.readable, &Headers::new())?
                        .into_http(&context)?,
                ));
            }
            if path == "/incomplete" {
                let pipe = IdentityTransformStream::new();
                context.wait_until_byte_stream(&pipe.readable, async { Ok(()) })?;
                return Ok(Output::Http(
                    Response::from_byte_stream(200, &pipe.readable, &Headers::new())?
                        .into_http(&context)?,
                ));
            }
            if path == "/discard" {
                let pipe = IdentityTransformStream::new();
                let w = pipe.writable.get_writer()?;
                let probe = self.0.clone();
                let storage = self.1.clone();
                context.wait_until_byte_stream(&pipe.readable, async move {
                    let error = w.write(vec![1, 2, 3]).await.unwrap_err();
                    assert_eq!(error.to_string(), "The HTTP body was dropped");
                    storage.put_json("discarded", &json!(true))?;
                    probe.discarded.notify_one();
                    Ok(())
                })?;
                return Ok(Output::Http(
                    Response::from_byte_stream(200, &pipe.readable, &Headers::new())?
                        .into_http(&context)?,
                ));
            }
            self.1.put_json("committed", &json!("before stream"))?;
            let pipe = FixedLengthStream::new(6.0)?;
            let w = pipe.writable.get_writer()?;
            let probe = self.0.clone();
            let storage = self.1.clone();
            context.wait_until_byte_stream(&pipe.readable, async move {
                let first = w.write(b"abc".to_vec());
                probe.first_queued.notify_one();
                first.await?;
                probe.acks.fetch_add(1, Ordering::SeqCst);
                storage.put_json("acks", &json!(1))?;
                w.write(b"def".to_vec()).await?;
                probe.acks.fetch_add(1, Ordering::SeqCst);
                storage.put_json("acks", &json!(2))?;
                w.close().await?;
                probe.produced.notify_one();
                Ok(())
            })?;
            if path == "/routed" {
                let request = Request::with_byte_stream(
                    &context,
                    "https://peer/consume",
                    "post",
                    &Headers::new(),
                    &pipe.readable,
                )?;
                return Ok(Output::Http(
                    context.fetch_cell(PEER.into(), None, request).await?,
                ));
            }
            Ok(Output::Http(
                Response::from_byte_stream(200, &pipe.readable, &Headers::new())?
                    .into_http(&context)?,
            ))
        })
    }
}
struct Peer;
impl Cell for Peer {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("expected fetch"));
            };
            let headers = request.header_list()?;
            let length = headers.get("content-length")?;
            let transfer = headers.get("transfer-encoding")?;
            let mut reader = request.into_body_reader(&context)?;
            let bytes = reader.bytes(&context).await?;
            output(
                &context,
                json!({"body":bytes,"length":length,"transfer":transfer}),
            )
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
                url: format!("https://source{path}"),
                method: "GET".into(),
                body: RequestBody::Bytes(Vec::new().into()),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_shared_streams_retain_proofs_backpressure_body_cleanup_and_retirement() -> Result<()>
{
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let probe = Arc::new(Probe::default());
    let p = probe.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("unused stateless")) }))
        .cell("Source", move |context, _| {
            Ok(Arc::new(Source(p.clone(), context.storage()?)))
        })?
        .cell("Peer", |_, _| Ok(Arc::new(Peer)))?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        Generation::build_native(
            1,
            fixture::graph("native-streams", "", vec!["Source".into(), "Peer".into()]),
            fixture::generation_options(),
            HashMap::from([(
                "native-streams".into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        fixture::runtime_options(directory.path().join("cells")),
    )?;
    open(&runtime, SOURCE).await?;
    open(&runtime, PEER).await?;
    let (stop, mut gate_stop) = tokio::sync::watch::channel(false);
    let (gate_tx, mut gates) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    cellar::host_channels::set_gate_tx(gate_tx);
    let p = probe.clone();
    let gate_adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"stream proof adapter stops";_=gate_stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            if gate.ticket.channel == cellar_logic::Channel::CellRpc
                && gate.ticket.position.is_some()
                && p.hold.swap(false, Ordering::SeqCst)
            {
                p.gate_entered.notify_one();
                p.gate_release.notified().await;
            }
            let _ = gate.reply.send(Ok(()));
        }
    });
    let mut call_stop = stop.subscribe();
    let (call_tx, mut calls) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::DoCallReq>();
    cellar::host_channels::set_do_call_tx(call_tx);
    let host = runtime.clone();
    let p = probe.clone();
    let call_adapter = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {"stream route adapter stops";_=call_stop.changed()=>None,call=calls.recv()=>call,}
        {
            p.calls.fetch_add(1, Ordering::SeqCst);
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
    let mut response = call(&runtime, "/stream").await?;
    assert!(response.write_position.is_some());
    probe.first_queued.notified().await;
    assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
    let mut stream = response.stream.take().unwrap();
    assert_eq!(
        stream.next().await.unwrap().map_err(anyhow::Error::msg)?,
        b"abc"
    );
    assert_eq!(
        stream.next().await.unwrap().map_err(anyhow::Error::msg)?,
        b"def"
    );
    assert!(stream.next().await.is_none());
    probe.produced.notified().await;
    assert_eq!(probe.acks.load(Ordering::SeqCst), 2);
    probe.acks.store(0, Ordering::SeqCst);
    probe.hold.store(true, Ordering::SeqCst);
    let host = runtime.clone();
    let routed = cellar::asyncrt::spawn(async move { call(&host, "/routed").await });
    probe.gate_entered.notified().await;
    probe.first_queued.notified().await;
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.acks.load(Ordering::SeqCst), 0);
    probe.gate_release.notify_one();
    let response = routed.await??;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?,
        json!({"body":b"abcdef","length":"6","transfer":null})
    );
    probe.produced.notified().await;
    assert_eq!(probe.acks.load(Ordering::SeqCst), 2);
    let response = call(&runtime, "/discard").await?;
    drop(response);
    probe.discarded.notified().await;
    let mut response = call(&runtime, "/incomplete").await?;
    let error = response
        .stream
        .as_mut()
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error,
        "The byte stream producer finished without closing its stream"
    );
    assert!(response.stream.as_mut().unwrap().next().await.is_none());
    let mut response = call(&runtime, "/retire").await?;
    probe.retiring.notified().await;
    let mut stream = response.stream.take().unwrap();
    let pending = cellar::asyncrt::spawn(async move { stream.next().await });
    runtime.swap_out_cell(SOURCE, 1).await?;
    let error = pending.await?.unwrap().unwrap_err();
    assert_eq!(error, "The byte stream producer was dropped");
    if !probe.retired.load(Ordering::SeqCst) {
        cellar::asyncrt::timeout(Duration::from_secs(2), probe.retirement_done.notified()).await?;
    }
    assert!(probe.retired.load(Ordering::SeqCst));
    assert!(!probe.readable.lock().unwrap().as_ref().unwrap().locked());
    open(&runtime, SOURCE).await?;
    let response = call(&runtime, "/verify").await?;
    assert_eq!(
        serde_json::from_slice::<Json>(&response.body)?,
        json!({"committed":"before stream","acks":2,"discarded":true,"retired":true})
    );
    stop.send(true)?;
    call_adapter.await?;
    gate_adapter.await?;
    Ok(())
}
