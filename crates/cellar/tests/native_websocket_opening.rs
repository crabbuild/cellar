//! Native turn ownership and durability; the connector/proof verdicts are adapters.
#[path = "support/html.rs"]
mod fixture;
use anyhow::{anyhow, Result};
use cellar::{
    application::{
        loader::{Outbound, WorkerCode},
        websocket::WebSocket,
        Cell, Context, HandlerFuture, Invocation, Output, Registry, Request,
    },
    engine_api::{HttpResponse, RuntimeFetch},
    generation::Generation,
    runtime::RuntimeManager,
    ws_registry::{self as ws, OutboundWsOpen},
};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
const SCOPE: &str = "Probe:one";
#[derive(Default)]
struct Probe {
    socket: Mutex<Option<WebSocket>>,
    old: Mutex<Option<Context>>,
}
fn output(value: Value) -> Result<Output> {
    Ok(Output::Http(HttpResponse {
        status: 200,
        body: serde_json::to_vec(&value)?,
        headers: vec![],
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}
struct Root(Arc<Probe>);
impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let path = url::Url::parse(&request.url)?.path().to_owned();
            match path.as_str() {
                "/invalid" => {
                    for (url, protocols) in [
                        ("not a url", vec![]),
                        ("http://peer", vec![]),
                        ("ws://peer", vec!["bad token".into()]),
                        ("ws://peer", vec!["a".into(), "a".into()]),
                    ] {
                        assert!(context.connect_websocket(url, protocols).await.is_err());
                    }
                    let before =
                        ws::has_regular_websocket(context.identity().unwrap().scope.as_str());
                    drop(context.connect_websocket("ws://peer", vec![]));
                    assert_eq!(
                        before,
                        ws::has_regular_websocket(context.identity().unwrap().scope.as_str())
                    );
                    output(json!(true))
                }
                "/deny" | "/broker" => {
                    let app = Registry::new(|context, _| {
                        Box::pin(async move {
                            let error = context
                                .connect_websocket("ws://peer", vec![])
                                .await
                                .err()
                                .unwrap()
                                .to_string();
                            output(json!(error))
                        })
                    });
                    let mut code = WorkerCode::new(Arc::new(app));
                    code.outbound = if path == "/deny" {
                        Outbound::Deny
                    } else {
                        Outbound::Broker(context.exported_entrypoint("default")?)
                    };
                    let worker = context.loader("WORKERS")?.load(&context, code)?;
                    let response = worker
                        .entrypoint("default")
                        .fetch(
                            &context,
                            Request {
                                url: "http://loaded".into(),
                                method: "GET".into(),
                                headers: vec![],
                                body: vec![].into(),
                                entrypoint: None,
                            },
                        )
                        .await?;
                    Ok(Output::Http(response))
                }
                "/old" => {
                    let old = self.0.old.lock().unwrap().clone().unwrap();
                    assert!(old.connect_websocket("ws://peer", vec![]).await.is_err());
                    output(json!(true))
                }
                "/send" | "/foreign" => {
                    let socket = self.0.socket.lock().unwrap().clone().unwrap();
                    let sent = socket.send_text(&context, "secret").is_ok();
                    assert_eq!(sent, path == "/send");
                    output(json!(sent))
                }
                "/remember" => {
                    *self.0.old.lock().unwrap() = Some(context);
                    output(json!(true))
                }
                _ => {
                    context
                        .storage()?
                        .put_json("before-opening", &json!(path))?;
                    match context
                        .connect_websocket("ws://peer/echo", vec!["chat.v1".into()])
                        .await
                    {
                        Ok(socket) => {
                            *self.0.socket.lock().unwrap() = Some(socket);
                            output(json!(true))
                        }
                        Err(error) => output(json!(error.to_string())),
                    }
                }
            }
        })
    }
}
fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://probe{path}"),
        method: "GET".into(),
        body: vec![].into(),
        headers: vec![],
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}
async fn call(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<Value> {
    Ok(serde_json::from_slice(
        &runtime
            .fetch_cell(scope.into(), None, request(path), None)
            .await?
            .body,
    )?)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_opening_owns_its_gate_and_cancellation() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(45), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let probe = Arc::new(Probe::default());
    let state = probe.clone();
    let app = Registry::new(|_, _| Box::pin(async { Err(anyhow!("unused worker")) }))
        .cell("Probe", move |_, _| Ok(Arc::new(Root(state.clone()))))?;
    let mut graph = fixture::graph("native-ws-opening", "", vec!["Probe".into()]);
    graph.primary.loader_bindings = vec!["WORKERS".into()];
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        Generation::build_native(
            1,
            graph,
            fixture::generation_options(),
            HashMap::from([(
                "native-ws-opening".into(),
                Arc::new(app) as Arc<dyn cellar::application::Application>,
            )]),
        )?,
        fixture::runtime_options(directory.path().into()),
    )?;
    for scope in [SCOPE, "Probe:other"] {
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
    }
    let (gate_send, mut gates) = mpsc::unbounded_channel::<cellar::host_channels::GateReq>();
    cellar::host_channels::set_gate_tx(gate_send);
    let (held_send, mut held) = mpsc::unbounded_channel();
    let proofs = cellar::asyncrt::spawn(async move {
        while let Some(gate) = gates.recv().await {
            if gate.ticket.channel == cellar_logic::Channel::WsSelf {
                let _ = held_send.send(gate);
            } else {
                let _ = gate.reply.send(Ok(()));
            }
        }
    });
    let (connect, mut requests) = mpsc::unbounded_channel();
    cellar::host_channels::set_outbound_ws_tx(connect);
    assert_eq!(call(&runtime, SCOPE, "/invalid").await?, true);
    assert!(requests.try_recv().is_err());
    assert!(held.try_recv().is_err());
    assert!(call(&runtime, SCOPE, "/deny")
        .await?
        .as_str()
        .unwrap()
        .contains("not permitted"));
    assert!(call(&runtime, SCOPE, "/broker")
        .await?
        .as_str()
        .unwrap()
        .contains("cannot broker"));
    assert!(requests.try_recv().is_err());
    assert!(held.try_recv().is_err());
    assert_eq!(call(&runtime, SCOPE, "/remember").await?, true);
    assert_eq!(call(&runtime, SCOPE, "/old").await?, true);
    for refused in [true, false] {
        let host = runtime.clone();
        let running = cellar::asyncrt::spawn(async move { call(&host, SCOPE, "/gated").await });
        let gate = held.recv().await.unwrap();
        assert_eq!(gate.scope, SCOPE);
        assert_eq!(gate.ticket.epoch, Some(1));
        assert!(gate.ticket.position.is_some());
        assert!(
            cellar::asyncrt::timeout(Duration::from_millis(20), requests.recv())
                .await
                .is_err(),
            "dial escaped its durability proof"
        );
        gate.reply
            .send(if refused {
                Err(cellar_logic::RequestError::DurabilityUnproven)
            } else {
                Ok(())
            })
            .unwrap();
        if refused {
            assert!(running
                .await??
                .as_str()
                .unwrap()
                .contains("refusing to send"));
            assert!(requests.try_recv().is_err());
        } else {
            let request = requests.recv().await.unwrap();
            assert_eq!(request.scope, SCOPE);
            assert!(request.pull.is_none());
            let (out, mut frames) = mpsc::unbounded_channel();
            ws::ws_register(request.id, out);
            request
                .reply
                .send(Ok(OutboundWsOpen {
                    protocol: Some("chat.v1".into()),
                    declined: None,
                }))
                .ok()
                .unwrap();
            assert_eq!(running.await??, true);
            assert!(ws::has_regular_websocket(SCOPE));
            let host = runtime.clone();
            let sending = cellar::asyncrt::spawn(async move { call(&host, SCOPE, "/send").await });
            let gate = held.recv().await.unwrap();
            assert!(frames.try_recv().is_err());
            gate.reply.send(Ok(())).unwrap();
            assert_eq!(sending.await??, true);
            assert!(matches!(frames.recv().await,Some(ws::WsOut::Text(text)) if text=="secret"));
            assert_eq!(call(&runtime, "Probe:other", "/foreign").await?, false);
            ws::ws_unregister(request.id);
        }
    }
    for before_proof in [true, false] {
        let pending = request("/cancel");
        let id = pending.request_id.unwrap();
        let host = runtime.clone();
        let running = cellar::asyncrt::spawn(async move {
            host.fetch_cell(SCOPE.into(), None, pending, None).await
        });
        let gate = held.recv().await.unwrap();
        let mut opening = if before_proof {
            None
        } else {
            gate.reply.send(Ok(())).unwrap();
            Some(requests.recv().await.unwrap())
        };
        runtime.abort_fetch(SCOPE, id);
        assert!(running.await?.is_err());
        if let Some(opening) = opening.as_mut() {
            opening.reply.closed().await;
            assert!(!ws::has_regular_websocket(SCOPE));
        } else {
            assert!(requests.try_recv().is_err());
        }
        assert!(!ws::has_regular_websocket(SCOPE));
    }

    proofs.abort();
    let _ = proofs.await;
    Ok(())
}
