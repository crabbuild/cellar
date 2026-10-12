//! HTML callbacks and response chunks retain the original native cell turn.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::application::{
    html_rewriter::{Content, ContentType, Element, ElementHandlers, Error, HtmlRewriter},
    http::Response,
    Cell, Context, HandlerFuture, Invocation, Output, Registry,
};
use cellar::{
    engine_api::{HttpResponse, RuntimeFetch},
    generation::Generation,
    runtime::RuntimeManager,
};
use futures_util::{FutureExt, Stream, StreamExt};
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
const SCRIPT: &str = "native-html";
const SCOPE: &str = "Page:one";
#[derive(Default)]
struct Signals {
    hold: AtomicBool,
    refuse: AtomicBool,
    gates: Mutex<Vec<cellar::actor::GateTicket>>,
    gate_entered: Notify,
    gate_release: Notify,
    handler_entered: Notify,
    handler_dropped: Notify,
    handler_cancelled: AtomicBool,
    source_entered: Notify,
    source_dropped: Notify,
    source_cancelled: AtomicBool,
    source_pulls: AtomicUsize,
    deferred_dropped: AtomicBool,
    deferred_pulls: AtomicUsize,
    read_release: Mutex<Option<tokio::sync::oneshot::Sender<Vec<u8>>>>,
    saved: Mutex<Option<(Element, Context)>>,
}
enum ProducerKind {
    Source,
    Deferred,
}
struct Producer {
    chunks: VecDeque<std::result::Result<Vec<u8>, String>>,
    signals: Arc<Signals>,
    kind: ProducerKind,
    pending: Option<tokio::sync::oneshot::Receiver<Vec<u8>>>,
}
impl Stream for Producer {
    type Item = std::result::Result<Vec<u8>, String>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if let Some(chunk) = self.chunks.pop_front() {
            match self.kind {
                ProducerKind::Source => self.signals.source_pulls.fetch_add(1, Ordering::SeqCst),
                ProducerKind::Deferred => {
                    self.signals.deferred_pulls.fetch_add(1, Ordering::SeqCst)
                }
            };
            return Poll::Ready(Some(chunk));
        }
        if let Some(receiver) = self.pending.as_mut() {
            match std::future::Future::poll(Pin::new(receiver), cx) {
                Poll::Pending => {
                    self.signals.source_entered.notify_one();
                    return Poll::Pending;
                }
                Poll::Ready(Ok(bytes)) => {
                    self.pending = None;
                    self.signals.source_pulls.fetch_add(1, Ordering::SeqCst);
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(Err(_)) => {
                    self.pending = None;
                    return Poll::Ready(None);
                }
            }
        }
        Poll::Ready(None)
    }
}
impl Drop for Producer {
    fn drop(&mut self) {
        match self.kind {
            ProducerKind::Source => {
                self.signals.source_cancelled.store(true, Ordering::SeqCst);
                self.signals.source_dropped.notify_one()
            }
            ProducerKind::Deferred => self.signals.deferred_dropped.store(true, Ordering::SeqCst),
        }
    }
}
struct HandlerGuard(Arc<Signals>);
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        self.0.handler_cancelled.store(true, Ordering::SeqCst);
        self.0.handler_dropped.notify_one();
    }
}
struct Page(Arc<Signals>);
impl Cell for Page {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(worker(context, invocation, self.0.clone()))
    }
}
async fn worker(context: Context, invocation: Invocation, signals: Arc<Signals>) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        return Err(anyhow!("expected fetch"));
    };
    let mode = url::Url::parse(&request.url)?.path().to_string();
    if mode == "/count" {
        return Ok(Output::Http(
            Response::new(
                200,
                serde_json::to_vec(&context.storage()?.get_json("writes")?)?,
                Vec::new(),
            )
            .into_http(&context)?,
        ));
    }
    let mut rewriter = HtmlRewriter::new();
    let input = match mode.as_str() {
        "/gate" | "/refuse" => {
            rewriter = rewriter.on(
                "p",
                ElementHandlers::new().element(|c, e| {
                    Box::pin(async move {
                        cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                        let storage = c.storage()?;
                        let count = storage
                            .get_json("writes")?
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0)
                            + 1;
                        storage.put_json("writes", &json!(count))?;
                        e.set_attribute("written", "yes")
                    })
                }),
            );
            Response::new(
                201,
                "<p>hello</p>",
                vec![
                    ("Content-Length".into(), "999".into()),
                    ("X-Keep".into(), "kept".into()),
                ],
            )
        }
        "/cancel-handler" | "/retire" => {
            let callback = signals.clone();
            rewriter = rewriter.on(
                "p",
                ElementHandlers::new().element(move |c, e| {
                    let signals = callback.clone();
                    Box::pin(async move {
                        e.append(
                            Content::Stream(Box::pin(Producer {
                                chunks: VecDeque::from([Ok(b"unused".to_vec())]),
                                signals: signals.clone(),
                                kind: ProducerKind::Deferred,
                                pending: None,
                            })),
                            ContentType::Text,
                        )?;
                        *signals.saved.lock().unwrap() = Some((e, c));
                        let _drop = HandlerGuard(signals.clone());
                        signals.handler_entered.notify_one();
                        std::future::pending::<Result<()>>().await
                    })
                }),
            );
            Response::streaming(
                200,
                Box::pin(Producer {
                    chunks: VecDeque::from([Ok(b"<p>x</p>".to_vec())]),
                    signals: signals.clone(),
                    kind: ProducerKind::Source,
                    pending: None,
                }),
                Vec::new(),
            )
        }
        "/pending-input" => {
            let (send, receive) = tokio::sync::oneshot::channel();
            *signals.read_release.lock().unwrap() = Some(send);
            Response::streaming(
                200,
                Box::pin(Producer {
                    chunks: VecDeque::from([Ok(b"<p>x".to_vec())]),
                    signals: signals.clone(),
                    kind: ProducerKind::Source,
                    pending: Some(receive),
                }),
                Vec::new(),
            )
        }
        "/bounded" => Response::streaming(
            200,
            Box::pin(Producer {
                chunks: (0..100).map(|_| Ok(vec![b'x'; 64 << 10])).collect(),
                signals: signals.clone(),
                kind: ProducerKind::Source,
                pending: None,
            }),
            Vec::new(),
        ),
        "/panic" => {
            rewriter = rewriter.on(
                "p",
                ElementHandlers::new()
                    .element(|_, _| Box::pin(async { panic!("native handler panic") })),
            );
            Response::new(200, "<p>x</p>", Vec::new())
        }
        "/producer-error" => Response::streaming(
            200,
            Box::pin(Producer {
                chunks: VecDeque::from([Err("input failure".into())]),
                signals: signals.clone(),
                kind: ProducerKind::Source,
                pending: None,
            }),
            Vec::new(),
        ),
        "/nested" => {
            rewriter = rewriter.on(
                "div",
                ElementHandlers::new().element(|_, e| {
                    Box::pin(async move {
                        e.on_end_tag(|_, t| {
                            Box::pin(async move {
                                t.before(&format!("[{}]", t.name()?), ContentType::Text)?;
                                t.remove()
                            })
                        })
                    })
                }),
            );
            Response::new(200, "<div><div>x</div></div>", Vec::new())
        }
        _ => Response::new(200, "<p>normal</p>", Vec::new()),
    };
    Ok(Output::Http(
        rewriter.transform(&context, input)?.into_http(&context)?,
    ))
}
async fn open(runtime: &RuntimeManager) -> Result<()> {
    runtime
        .restore_cell(
            SCOPE,
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell(SCOPE.into(), 1, true).await?;
    runtime.publish_cell(SCOPE, 1)
}
async fn call(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            SCOPE.into(),
            None,
            RuntimeFetch {
                url: format!("http://worker{path}"),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}
async fn body(mut response: HttpResponse) -> Result<Vec<u8>> {
    let mut bytes = response.body;
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            bytes.extend(chunk.map_err(anyhow::Error::msg)?);
        }
    }
    Ok(bytes)
}
fn reset(signals: &Signals) {
    for notify in [
        &signals.source_entered,
        &signals.source_dropped,
        &signals.handler_entered,
        &signals.handler_dropped,
    ] {
        while notify.notified().now_or_never().is_some() {}
    }
    signals.handler_cancelled.store(false, Ordering::SeqCst);
    signals.source_cancelled.store(false, Ordering::SeqCst);
    signals.deferred_dropped.store(false, Ordering::SeqCst);
    signals.source_pulls.store(0, Ordering::SeqCst);
    signals.deferred_pulls.store(0, Ordering::SeqCst);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_html_preserves_durability_bounded_input_and_cancellation() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let factory = signals.clone();
    let registry = Registry::new(|_, _| Box::pin(async { Err(anyhow!("stateless unused")) }))
        .cell("Page", move |_, _| Ok(Arc::new(Page(factory.clone()))))?;
    let generation = Generation::build_native(
        1,
        fixture::graph(SCRIPT, "", vec!["Page".into()]),
        fixture::generation_options(),
        HashMap::from([(
            SCRIPT.into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        generation,
        fixture::runtime_options(directory.path().join("cells")),
    )?;
    open(&runtime).await?;
    let (send, mut gates) =
        tokio::sync::mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    cellar::host_channels::set_gate_tx(send);
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let captured = signals.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(gate) = cellar::asyncrt::select_biased! {"HTML proof adapter stops";_=stopped.changed()=>None,gate=gates.recv()=>gate,}
        {
            let wrote = gate.ticket.position.is_some();
            captured.gates.lock().unwrap().push(gate.ticket);
            if wrote && captured.hold.load(Ordering::SeqCst) {
                captured.gate_entered.notify_one();
                captured.gate_release.notified().await;
            }
            let result = if wrote && captured.refuse.load(Ordering::SeqCst) {
                Err(cellar_logic::RequestError::DurabilityUnproven)
            } else {
                Ok(())
            };
            let _ = gate.reply.send(result);
        }
    });
    signals.hold.store(true, Ordering::SeqCst);
    let mut response = call(&runtime, "/gate").await?;
    assert_eq!(response.status, 201);
    assert!(response
        .headers
        .contains(&("Content-Length".into(), "999".into())));
    assert!(response.headers.contains(&("X-Keep".into(), "kept".into())));
    signals.gate_entered.notified().await;
    let mut output = response.stream.take().unwrap();
    assert!(
        output.next().now_or_never().is_none(),
        "no rewritten chunk may precede its callback write proof"
    );
    signals.hold.store(false, Ordering::SeqCst);
    signals.gate_release.notify_one();
    let mut bytes = Vec::new();
    while let Some(chunk) = output.next().await {
        bytes.extend(chunk.map_err(anyhow::Error::msg)?)
    }
    assert_eq!(bytes, b"<p written=\"yes\">hello</p>");
    drop(output);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body(call(&runtime, "/count").await?).await?)?,
        json!(1)
    );
    assert!(signals
        .gates
        .lock()
        .unwrap()
        .iter()
        .any(|ticket| ticket.channel == cellar_logic::Channel::Response
            && ticket.position.is_some()
            && ticket.epoch == Some(1)));
    signals.refuse.store(true, Ordering::SeqCst);
    let mut output = call(&runtime, "/refuse").await?.stream.unwrap();
    assert!(output.next().await.unwrap().is_err());
    assert!(output.next().await.is_none());
    drop(output);
    signals.refuse.store(false, Ordering::SeqCst);
    assert_eq!(
        body(call(&runtime, "/nested").await?).await?,
        b"<div><div>x[div][div]"
    );
    for (path, message) in [
        (
            "/panic",
            "HTMLRewriter handler panicked: native handler panic",
        ),
        ("/producer-error", "input failure"),
    ] {
        reset(&signals);
        let mut stream = call(&runtime, path).await?.stream.unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap_err(), message);
        assert!(stream.next().await.is_none());
        if path == "/producer-error" {
            assert!(signals.source_cancelled.load(Ordering::SeqCst));
        }
    }
    reset(&signals);
    let response = call(&runtime, "/cancel-handler").await?;
    signals.handler_entered.notified().await;
    drop(response);
    signals.handler_dropped.notified().await;
    signals.source_dropped.notified().await;
    assert!(signals.handler_cancelled.load(Ordering::SeqCst));
    assert!(signals.source_cancelled.load(Ordering::SeqCst));
    assert!(signals.deferred_dropped.load(Ordering::SeqCst));
    assert_eq!(signals.deferred_pulls.load(Ordering::SeqCst), 0);
    let (token, _) = signals.saved.lock().unwrap().take().unwrap();
    assert_eq!(
        token
            .tag_name()
            .unwrap_err()
            .downcast_ref::<Error>()
            .unwrap()
            .name(),
        "TypeError"
    );
    reset(&signals);
    let response = call(&runtime, "/pending-input").await?;
    signals.source_entered.notified().await;
    drop(response);
    assert!(
        !signals.source_cancelled.load(Ordering::SeqCst),
        "cancellation retains one accepted pending read"
    );
    signals
        .read_release
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .send(b"</p>".to_vec())
        .map_err(|_| anyhow!("accepted read lost"))?;
    signals.source_dropped.notified().await;
    assert_eq!(signals.source_pulls.load(Ordering::SeqCst), 2);
    reset(&signals);
    let response = call(&runtime, "/bounded").await?;
    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
    assert!(
        signals.source_pulls.load(Ordering::SeqCst) <= 4,
        "bounded output must stop feeding input"
    );
    drop(response);
    signals.source_dropped.notified().await;
    reset(&signals);
    let mut response = call(&runtime, "/retire").await?;
    signals.handler_entered.notified().await;
    runtime.swap_out_cell(SCOPE, 1).await?;
    // Swap-out retires the activation and wakes its driver; the detached
    // answered event may still be dropping its background future on another
    // thread. Observe both drops before inspecting their cleanup flags.
    cellar::asyncrt::timeout(Duration::from_secs(2), async {
        futures_util::future::join(
            signals.handler_dropped.notified(),
            signals.source_dropped.notified(),
        )
        .await;
    })
    .await?;
    assert!(signals.handler_cancelled.load(Ordering::SeqCst));
    assert!(signals.source_cancelled.load(Ordering::SeqCst));
    assert!(signals.deferred_dropped.load(Ordering::SeqCst));
    assert_eq!(signals.deferred_pulls.load(Ordering::SeqCst), 0);
    // Retirement closes the output even when the async user handler never wakes.
    assert_eq!(
        response
            .stream
            .as_mut()
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap_err(),
        "HTMLRewriter turn retired before completion"
    );
    assert!(response.stream.as_mut().unwrap().next().await.is_none());
    let (token, old) = signals.saved.lock().unwrap().take().unwrap();
    assert!(token.tag_name().is_err());
    assert!(HtmlRewriter::new()
        .transform(&old, Response::empty(204, Vec::new()))
        .is_err());
    runtime.start_cell(SCOPE.into(), 1, false).await?;
    runtime.publish_cell(SCOPE, 1)?;
    assert_eq!(
        body(call(&runtime, "/normal").await?).await?,
        b"<p>normal</p>"
    );
    stop.send(true)?;
    adapter.await?;
    Ok(())
}
