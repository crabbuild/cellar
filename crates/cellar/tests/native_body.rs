//! Owned upload readers, typed bodies, proofs and cleanup without engine startup.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::application::{
    http::{Blob, BodyInit, FormData, FormValue, Headers, Response},
    Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
};
use cellar::{
    engine_api::{HttpResponse, RequestBody, RuntimeFetch},
    generation::Generation,
    runtime::RuntimeManager,
};
use futures_util::Stream;
use serde_json::json;
use std::{
    collections::{HashMap, VecDeque},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::sync::Notify;
const SOURCE: &str = "Upload:one";
const PEER: &str = "Peer:one";
#[derive(Default)]
struct Probe {
    pulls: AtomicUsize,
    dropped: AtomicBool,
    entered: Notify,
}
struct Producer {
    chunks: VecDeque<std::result::Result<Vec<u8>, String>>,
    pending: bool,
    probe: Arc<Probe>,
}
impl Stream for Producer {
    type Item = std::result::Result<Vec<u8>, String>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.probe.pulls.fetch_add(1, Ordering::SeqCst);
        if let Some(chunk) = self.chunks.pop_front() {
            return Poll::Ready(Some(chunk));
        }
        if self.pending {
            self.probe.entered.notify_one();
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
    }
}
fn producer(
    chunks: VecDeque<std::result::Result<Vec<u8>, String>>,
    pending: bool,
) -> Result<(RequestBody, Arc<Probe>)> {
    let probe = Arc::new(Probe::default());
    let id = cellar::http_streams::register_body_stream(Box::pin(Producer {
        chunks,
        pending,
        probe: probe.clone(),
    }))
    .map_err(anyhow::Error::msg)?;
    Ok((RequestBody::Stream(id), probe))
}
#[derive(Default)]
struct Signals {
    held: AtomicBool,
    gate_entered: Notify,
    gate_release: Notify,
    calls: AtomicUsize,
    saved: Mutex<Option<Context>>,
    retained: Mutex<Option<(Blob, FormData)>>,
}
struct Upload(Arc<Signals>);
impl Cell for Upload {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Err(anyhow!("fetch required"));
            };
            let mode = request.parsed_url()?.pathname();
            if mode == "/stored" {
                return Ok(Output::Http(
                    Response::from_json(&context.storage()?.get_json("file")?)?
                        .into_http(&context)?,
                ));
            }
            let mut reader = request.into_body_reader(&context)?;
            assert!(!reader.body_used());
            if mode == "/ignore" {
                drop(reader);
                return Ok(Output::Http(
                    Response::empty(204, Vec::new()).into_http(&context)?,
                ));
            }
            if mode == "/cancel" || mode == "/retire" {
                *self.0.saved.lock().unwrap() = Some(context.clone());
                reader.form_data(&context).await?;
                return Err(anyhow!("pending input resumed"));
            }
            if mode == "/failure" {
                reader.form_data(&context).await?;
                return Err(anyhow!("failed input succeeded"));
            }
            let form = reader.form_data(&context).await?;
            assert!(reader.body_used());
            let FormValue::File(file) = form.get("upload").unwrap() else {
                return Err(anyhow!("binary upload required"));
            };
            assert_eq!(file.bytes(), [0, 255, 192, 128]);
            assert_eq!(file.name(), "雪.bin");
            let original = reader.bytes(&context).await?;
            assert!(original.windows(4).any(|bytes| bytes == [0, 255, 192, 128]));
            let blob = reader.blob(&context).await?;
            assert_eq!(blob.bytes(), original);
            context.storage()?.put_json(
                "file",
                &json!({"name":file.name_utf16(),"bytes":file.bytes(),"type":file.content_type()}),
            )?;
            let request = Request::with_body(
                "http://peer/form",
                "POST",
                &Headers::new(),
                &BodyInit::FormData(form.clone()),
            )?;
            let mut received =
                Response::from_http(context.fetch_cell(PEER.into(), None, request).await?);
            assert_eq!(received.blob(&context).await?.bytes(), file.bytes());
            let request = Request::with_body(
                "http://peer/file",
                "poſt",
                &Headers::new(),
                &BodyInit::File(file.clone()),
            )?;
            let mut response =
                Response::from_http(context.fetch_cell(PEER.into(), None, request).await?);
            let reply = response.blob(&context).await?;
            assert_eq!(reply.bytes(), file.bytes());
            assert_eq!(reply.content_type(), "app/binary");
            *self.0.retained.lock().unwrap() = Some((reply.clone(), form));
            Ok(Output::Http(
                Response::from_body(201, &BodyInit::Blob(reply), &Headers::new())?
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
                return Err(anyhow!("fetch required"));
            };
            assert_eq!(request.method, "POST");
            let multipart = request
                .header_list()?
                .get("content-type")?
                .is_some_and(|kind| kind.starts_with("multipart/form-data; boundary="));
            let mut reader = request.into_body_reader(&context)?;
            let blob = if multipart {
                let FormValue::File(file) =
                    reader.form_data(&context).await?.get("upload").unwrap()
                else {
                    return Err(anyhow!("file required"));
                };
                assert_eq!(file.name(), "雪.bin");
                assert!(reader
                    .bytes(&context)
                    .await?
                    .windows(4)
                    .any(|bytes| bytes == [0, 255, 192, 128]));
                file.blob().clone()
            } else {
                let blob = reader.blob(&context).await?;
                assert_eq!(reader.bytes(&context).await?, blob.bytes());
                blob
            };
            assert_eq!(blob.bytes(), [0, 255, 192, 128]);
            assert_eq!(blob.content_type(), "app/binary");
            context
                .storage()?
                .put_json("received", &json!(blob.bytes()))?;
            Ok(Output::Http(
                Response::from_body(200, &BodyInit::Blob(blob), &Headers::new())?
                    .into_http(&context)?,
            ))
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
fn fetch(path: &str, body: RequestBody, kind: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://upload{path}"),
        method: "POST".into(),
        body,
        headers: vec![("Content-Type".into(), kind.into())],
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}
async fn call(
    runtime: &RuntimeManager,
    path: &str,
    body: RequestBody,
    kind: &str,
) -> Result<HttpResponse> {
    runtime
        .fetch_cell(SOURCE.into(), None, fetch(path, body, kind), None)
        .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_upload_bodies_retain_proofs_reads_reopen_and_source_cleanup() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let upload = signals.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
        .cell("Upload", move |_, _| Ok(Arc::new(Upload(upload.clone()))))?
        .cell("Peer", |_, _| Ok(Arc::new(Peer)))?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        Generation::build_native(
            1,
            fixture::graph("native-body", "", vec!["Upload".into(), "Peer".into()]),
            fixture::generation_options(),
            HashMap::from([(
                "native-body".into(),
                Arc::new(registry) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        fixture::runtime_options(directory.path().join("cells")),
    )?;
    open(&runtime, SOURCE).await?;
    open(&runtime, PEER).await?;
    let (stop, mut gate_stop) = tokio::sync::watch::channel(false);
    let (sender, mut gates) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    cellar::host_channels::set_gate_tx(sender);
    let captured = signals.clone();
    let gate_adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"body proof adapter stops";_=gate_stop.changed()=>None,gate=gates.recv()=>gate,}
        {
            if gate.ticket.channel == cellar_logic::Channel::CellRpc
                && gate.ticket.position.is_some()
                && !captured.held.swap(true, Ordering::SeqCst)
            {
                assert_eq!(gate.ticket.epoch, Some(1));
                captured.gate_entered.notify_one();
                captured.gate_release.notified().await;
            }
            let _ = gate.reply.send(Ok(()));
        }
    });
    let (sender, mut calls) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::DoCallReq>();
    cellar::host_channels::set_do_call_tx(sender);
    let mut call_stop = stop.subscribe();
    let host = runtime.clone();
    let captured = signals.clone();
    let call_adapter = cellar::asyncrt::spawn(async move {
        while let Some(call) = cellar::asyncrt::select_biased! {"body route adapter stops";_=call_stop.changed()=>None,call=calls.recv()=>call,}
        {
            captured.calls.fetch_add(1, Ordering::SeqCst);
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
    let form = FormData::new();
    form.append_blob(
        "upload",
        &Blob::from_bytes(&[0, 255, 192, 128], "APP/BINARY")?,
        Some("雪.bin"),
    );
    let wire = form.encode()?;
    let (body, probe) = producer(
        wire.bytes
            .chunks(3)
            .map(|chunk| Ok(chunk.to_vec()))
            .collect(),
        false,
    )?;
    let caller = runtime.clone();
    let kind = wire.content_type.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "/upload", body, &kind).await });
    signals.gate_entered.notified().await;
    assert_eq!(signals.calls.load(Ordering::SeqCst), 0);
    assert!(probe.dropped.load(Ordering::SeqCst));
    signals.gate_release.notify_one();
    let response = pending.await??;
    assert_eq!(response.status, 201);
    assert_eq!(response.body, [0, 255, 192, 128]);
    assert!(response.write_position.is_some());
    assert_eq!(
        response.headers,
        [("content-type".into(), "app/binary".into())]
    );
    assert_eq!(signals.calls.load(Ordering::SeqCst), 2);
    let (body, probe) = producer(VecDeque::from([Ok(b"unused".to_vec())]), false)?;
    assert_eq!(
        call(&runtime, "/ignore", body, &wire.content_type)
            .await?
            .status,
        204
    );
    assert_eq!(probe.pulls.load(Ordering::SeqCst), 0);
    assert!(probe.dropped.load(Ordering::SeqCst));
    let (body, probe) = producer(VecDeque::from([Err("source failure".into())]), false)?;
    assert!(call(&runtime, "/failure", body, &wire.content_type)
        .await
        .err()
        .expect("failed source returned success")
        .to_string()
        .contains("source failure"));
    assert!(probe.dropped.load(Ordering::SeqCst));
    let (body, probe) = producer(VecDeque::new(), true)?;
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    let caller = runtime.clone();
    let kind = wire.content_type.clone();
    let pending = cellar::asyncrt::spawn(async move {
        caller
            .fetch_cell(
                SOURCE.into(),
                None,
                fetch("/cancel", body, &kind),
                Some(cancelled),
            )
            .await
    });
    probe.entered.notified().await;
    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
    assert_eq!(probe.pulls.load(Ordering::SeqCst), 1);
    cancel.send(()).unwrap();
    assert!(pending.await?.is_err());
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        while !probe.dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let (body, probe) = producer(VecDeque::new(), true)?;
    let caller = runtime.clone();
    let kind = wire.content_type.clone();
    let pending =
        cellar::asyncrt::spawn(async move { call(&caller, "/retire", body, &kind).await });
    probe.entered.notified().await;
    runtime.swap_out_cell(SOURCE, 1).await?;
    assert!(pending.await?.is_err());
    assert!(probe.dropped.load(Ordering::SeqCst));
    runtime.start_cell(SOURCE.into(), 1, false).await?;
    runtime.publish_cell(SOURCE, 1)?;
    assert!(signals
        .saved
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .take_body(RequestBody::Bytes(Vec::new().into()))
        .is_err());
    let stored = call(
        &runtime,
        "/stored",
        RequestBody::Bytes(Vec::new().into()),
        "",
    )
    .await?;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored.body)?,
        json!({"name":"雪.bin".encode_utf16().collect::<Vec<_>>(),"bytes":[0,255,192,128],"type":"app/binary"})
    );
    let (blob, form) = signals.retained.lock().unwrap().take().unwrap();
    assert_eq!(blob.bytes(), [0, 255, 192, 128]);
    form.append("outside", FormValue::text("alive"));
    assert!(form.has("outside"));
    stop.send(true)?;
    gate_adapter.await?;
    call_adapter.await?;
    Ok(())
}
