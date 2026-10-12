// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Native socket ownership and ordered streaming through production turns.
use anyhow::{anyhow, Result};
use cellar::application::websocket::{SocketEvent, WebSocket};
use cellar::application::{Cell, Context, HandlerFuture, Invocation, Output, Registry};
use cellar::engine_api::{
    Compat, HttpResponse, HttpResponseWebSocket, RuntimeFetch, RuntimeOptions, WorkerConfigOptions,
};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use cellar::ws_registry::{self as ws, WsIn, WsOut};
use cellar_native::clone::{Object, StructuredValue, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Signals {
    started: Notify,
    resume: Notify,
    hold: AtomicBool,
    socket: Mutex<Option<WebSocket>>,
    abandoned: AtomicU64,
    worker_done: Notify,
}

fn attachment() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Map(vec![
                (Value::from("date"), Value::Reference(1)),
                (Value::from("self"), Value::Reference(0)),
            ]),
            Object::Date(123456789.0),
        ],
    }
}

fn response(text: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.as_bytes().to_vec(),
        stream: None,
        headers: vec![],
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

struct Room(Arc<Signals>);
impl Cell for Room {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(request) => match url::Url::parse(&request.url)?.path() {
                    "/hib" | "/regular" => {
                        let pair = context.websocket_pair()?;
                        pair.server.serialize_attachment(&context, &attachment())?;
                        if request.url.ends_with("/hib") {
                            pair.server.accept(&context)?;
                            assert!(ws::has_regular_websocket(
                                &context.identity().unwrap().scope
                            ));
                            pair.server
                                .accept_hibernating(&context, vec!["room".into(), "🦀".into()])?;
                            assert!(!ws::has_regular_websocket(
                                &context.identity().unwrap().scope
                            ));
                            assert_eq!(pair.server.tags(&context)?, vec!["room", "🦀"]);
                            context.set_websocket_auto_response(Some((
                                "ping".into(),
                                "pong".into(),
                            )))?;
                            assert_eq!(
                                context.websocket_auto_response()?,
                                Some(("ping".into(), "pong".into()))
                            );
                            assert!(context
                                .set_websocket_auto_response(Some(("🦀".repeat(513), "x".into())))
                                .is_err());
                            assert!(context
                                .set_websocket_auto_response(Some(("x".into(), "🦀".repeat(513))))
                                .is_err());
                            assert_eq!(
                                pair.server.deserialize_attachment(&context)?,
                                Some(attachment())
                            );
                            *self.0.socket.lock().unwrap() = Some(pair.server.clone());
                        } else {
                            pair.server.accept(&context)?;
                            pair.server.accept(&context)?;
                        }
                        pair.server.send_text(&context, "welcome")?;
                        Ok(Output::Http(pair.client.into_response(
                            &context,
                            vec![("x-native".into(), "yes".into())],
                        )?))
                    }
                    "/inspect" => {
                        let sockets = context.websockets(Some("room"))?;
                        assert_eq!(sockets.len(), 1);
                        let socket = &sockets[0];
                        assert_eq!(socket.deserialize_attachment(&context)?, Some(attachment()));
                        assert_eq!(socket.tags(&context)?, vec!["room", "🦀"]);
                        assert!(socket.auto_response_timestamp(&context)?.is_some());
                        assert!(context.websockets(Some("missing"))?.is_empty());
                        Ok(response("metadata survived"))
                    }
                    "/broadcast" => {
                        for socket in context.websockets(Some("room"))? {
                            socket.send_text(&context, "between")?;
                        }
                        Ok(response("broadcast"))
                    }
                    "/stale" => {
                        assert!(self
                            .0
                            .socket
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .send_text(&context, "stale")
                            .is_err());
                        assert!(self
                            .0
                            .socket
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .serialize_attachment(&context, &attachment())
                            .is_err());
                        Ok(response("stale refused"))
                    }
                    "/foreign" => {
                        let id = self.0.socket.lock().unwrap().as_ref().unwrap().id();
                        assert!(context.websocket(id).is_err());
                        Ok(response("foreign refused"))
                    }
                    "/abandon" => {
                        let pair = context.websocket_pair()?;
                        pair.server.accept(&context)?;
                        self.0.abandoned.store(pair.server.id(), Ordering::Release);
                        Err(anyhow!("upgrade failed"))
                    }
                    "/cleanup" => {
                        assert!(context
                            .websocket(self.0.abandoned.load(Ordering::Acquire))
                            .is_err());
                        Ok(response("abandoned upgrade cleaned"))
                    }
                    path => Err(anyhow!("unknown path {path}")),
                },
                Invocation::WebSocketOpen { id, .. } => {
                    context.websocket(id)?.send_text(&context, "opened")?;
                    Ok(Output::Done)
                }
                Invocation::WebSocketMessage { id, data } => {
                    let socket = context.websocket(id)?;
                    match data {
                        WsIn::Text(text) if text == "stream" || text == "fail-late" => {
                            context.storage()?.put_json("phase", &1.into())?;
                            socket.send_text(&context, "first")?;
                            self.0.started.notify_one();
                            self.0.resume.notified().await;
                            socket.send_text(
                                &context,
                                if text == "stream" {
                                    "last"
                                } else {
                                    "discard-last"
                                },
                            )?;
                            if text == "fail-late" {
                                return Err(anyhow!("late rejection"));
                            }
                        }
                        WsIn::Text(text) if text == "fail" => {
                            socket.send_text(&context, "discard")?;
                            return Err(anyhow!("rejected handler"));
                        }
                        WsIn::Text(text) if text == "mixed" => {
                            socket.send_text(&context, "captured")?;
                            return Ok(Output::WebSocket(
                                vec![(id, WsOut::Text("manual".into()))].into(),
                            ));
                        }
                        WsIn::Text(text) => socket.send_text(&context, text)?,
                        WsIn::Binary(bytes) => socket.send_binary(&context, bytes)?,
                    }
                    Ok(Output::Done)
                }
                Invocation::WebSocketClose { id, .. } => {
                    let socket = context.websocket(id)?;
                    assert!(socket.send_text(&context, "closed").is_err());
                    assert!(socket.close(&context, 1006, "reserved").is_err());
                    assert!(socket.close(&context, 4001, "🦀".repeat(31)).is_err());
                    socket.close(&context, 4001, "done")?;
                    Ok(Output::Done)
                }
                _ => Err(anyhow!("unexpected invocation")),
            }
        })
    }
}

fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://room{path}"),
        method: "GET".into(),
        headers: vec![],
        body: vec![].into(),
        request_id: Some(cellar::engine_api::next_request_id()),
        order: None,
        parent: None,
    }
}
async fn fetch(runtime: &RuntimeManager, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell("Room:test".into(), None, request(path), None)
        .await
}
async fn message(runtime: &RuntimeManager, id: u64, text: &str) -> Result<ws::WsDispatch> {
    let (started, _) = tokio::sync::oneshot::channel();
    runtime
        .ws_message("Room:test".into(), id, WsIn::Text(text.into()), started)
        .await
}
async fn text(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<WsOut>) -> String {
    match receiver.recv().await.unwrap() {
        WsOut::Text(text) => text,
        _ => panic!("expected text"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_websocket_metadata_capture_and_request_ownership() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    cellar::asyncrt::timeout(Duration::from_secs(30), run()).await??;
    Ok(())
}

async fn run() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let worker_signals = signals.clone();
    let cell_signals = signals.clone();
    let registry = Registry::new(move |context, _| {
        let signals = worker_signals.clone();
        Box::pin(async move {
            let pair = context.websocket_pair()?;
            assert!(pair.server.accept_hibernating(&context, vec![]).is_err());
            pair.server.accept(&context)?;
            let server = pair.server;
            let pump_context = context.clone();
            context.wait_until(async move {
                loop {
                    match server.next(&pump_context).await? {
                        SocketEvent::Text(text) => {
                            server.send_text(&pump_context, format!("worker:{text}"))?
                        }
                        SocketEvent::Close(..) => {
                            signals.worker_done.notify_one();
                            break;
                        }
                        _ => {}
                    }
                }
                Ok(())
            })?;
            Ok(Output::Http(pair.client.into_response(&context, vec![])?))
        })
    })
    .cell("Room", move |_, _| Ok(Arc::new(Room(cell_signals.clone()))))?;
    let graph = DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: "native-websocket".into(),
            do_classes: vec!["Room".into()],
            bindings: vec![],
            vars: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            blob_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            node: "test".into(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: "native-websocket".into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        containers: vec![],
        fence_image: None,
        container_images: Vec::new(),
    });
    let generation = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "native-websocket".into(),
            Arc::new(registry) as Arc<dyn cellar::application::Application>,
        )]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
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
    )?;
    runtime
        .restore_cell(
            "Room:test",
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell("Room:test".into(), 1, true).await?;
    runtime.publish_cell("Room:test", 1)?;
    let (gates, mut requests) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let (held, mut held_requests) = tokio::sync::mpsc::unbounded_channel();
    let gate_signals = signals.clone();
    let (shutdown, mut stop) = tokio::sync::watch::channel(false);
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(request) = cellar::asyncrt::select_biased! {
            "stop this test's scripted durability adapter";
            _=stop.changed()=>None,
            request=requests.recv()=>request,
        } {
            if gate_signals.hold.load(Ordering::Acquire)
                && request.ticket.channel == cellar_logic::Channel::WsHibernatable
            {
                held.send(request).unwrap();
            } else {
                let _ = request.reply.send(Ok(()));
            }
        }
    });
    let response = fetch(&runtime, "/hib").await?;
    assert_eq!(response.status, 101);
    let Some(HttpResponseWebSocket::Cell(target)) = response.websocket else {
        panic!("cell upgrade")
    };
    assert_eq!(target.scope, "Room:test");
    let id = target.id;
    let (output, mut frames) = tokio::sync::mpsc::unbounded_channel();
    ws::ws_register(id, output);
    assert_eq!(text(&mut frames).await, "welcome");
    assert_eq!(
        ws::ws_auto_response("Room:test", id, "ping"),
        Some("pong".into())
    );
    assert_eq!(
        fetch(&runtime, "/inspect").await?.body,
        b"metadata survived"
    );

    runtime
        .restore_cell(
            "Room:other",
            &cellar_logic::RestoreSpec {
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            },
        )
        .await?;
    runtime.start_cell("Room:other".into(), 1, true).await?;
    runtime.publish_cell("Room:other", 1)?;
    assert_eq!(
        runtime
            .fetch_cell("Room:other".into(), None, request("/foreign"), None)
            .await?
            .body,
        b"foreign refused"
    );
    runtime.swap_out_cell("Room:other", 1).await?;
    runtime
        .ws_open("Room:test".into(), id, "chat".into())
        .await?;
    assert_eq!(text(&mut frames).await, "opened");
    let (started, _) = tokio::sync::oneshot::channel();
    let binary = runtime
        .ws_message(
            "Room:test".into(),
            id,
            WsIn::Binary(vec![0, 255, 4]),
            started,
        )
        .await?;
    ws::ws_emit_batch(binary.frames);
    assert!(matches!(frames.recv().await, Some(WsOut::Binary(bytes)) if bytes == vec![0, 255, 4]));

    signals.hold.store(true, Ordering::Release);
    let host = runtime.clone();
    let streaming = cellar::asyncrt::spawn(async move { message(&host, id, "stream").await });
    signals.started.notified().await;
    let first = held_requests.recv().await.unwrap();
    assert_eq!(first.scope, "Room:test");
    assert_eq!(first.ticket.epoch, Some(1));
    assert!(first.ticket.position.is_some());
    assert!(frames.try_recv().is_err());
    assert_eq!(fetch(&runtime, "/broadcast").await?.body, b"broadcast");
    held_requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(()))
        .unwrap();
    assert!(
        frames.try_recv().is_err(),
        "later gate must not cross the reserved first frame"
    );
    first.reply.send(Ok(())).unwrap();
    assert_eq!(text(&mut frames).await, "first");
    assert_eq!(text(&mut frames).await, "between");
    signals.resume.notify_one();
    let dispatch = streaming.await??;
    assert_eq!(dispatch.frames.len(), 1);
    assert!(dispatch.write_position.is_some());
    assert!(
        frames.try_recv().is_err(),
        "final capture leaves through the Actor's dispatch gate"
    );
    ws::ws_emit_batch(dispatch.frames);
    assert_eq!(text(&mut frames).await, "last");

    let host = runtime.clone();
    let failing = cellar::asyncrt::spawn(async move { message(&host, id, "fail-late").await });
    signals.started.notified().await;
    let first = held_requests.recv().await.unwrap();
    fetch(&runtime, "/broadcast").await?;
    held_requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(()))
        .unwrap();
    signals.resume.notify_one();
    assert!(failing.await?.is_err());
    first.reply.send(Ok(())).unwrap();
    assert_eq!(text(&mut frames).await, "first");
    assert_eq!(text(&mut frames).await, "between");
    assert!(
        frames.try_recv().is_err(),
        "rejected final capture is discarded"
    );
    cellar::asyncrt::timeout(Duration::from_secs(1), ws::ws_await_flushes(id)).await?;
    signals.hold.store(false, Ordering::Release);
    assert!(message(&runtime, id, "fail").await.is_err());
    cellar::asyncrt::timeout(Duration::from_secs(1), ws::ws_await_flushes(id)).await?;
    let combined = message(&runtime, id, "mixed").await?;
    assert_eq!(combined.frames.len(), 2);
    ws::ws_emit_batch(combined.frames);
    assert_eq!(text(&mut frames).await, "captured");
    assert_eq!(text(&mut frames).await, "manual");

    runtime.swap_out_cell("Room:test", 1).await?;
    runtime.start_cell("Room:test".into(), 1, false).await?;
    runtime.publish_cell("Room:test", 1)?;
    assert_eq!(fetch(&runtime, "/stale").await?.body, b"stale refused");
    assert_eq!(
        fetch(&runtime, "/inspect").await?.body,
        b"metadata survived"
    );
    let dispatch = message(&runtime, id, "reopened").await?;
    ws::ws_emit_batch(dispatch.frames);
    assert_eq!(text(&mut frames).await, "reopened");
    runtime
        .ws_closed("Room:test".into(), id, 1000, "peer".into(), true)
        .await?;
    assert!(matches!(frames.recv().await, Some(WsOut::Close(4001, reason)) if reason == "done"));
    ws::ws_unregister(id);

    let regular = fetch(&runtime, "/regular").await?;
    let Some(HttpResponseWebSocket::Cell(target)) = regular.websocket else {
        panic!("regular cell upgrade")
    };
    assert!(ws::has_regular_websocket("Room:test"));
    let regular_id = target.id;
    let (output, mut regular_frames) = tokio::sync::mpsc::unbounded_channel();
    ws::ws_register(regular_id, output);
    assert_eq!(text(&mut regular_frames).await, "welcome");
    let regular = message(&runtime, regular_id, "regular").await?;
    assert!(
        regular.frames.is_empty(),
        "regular sends use their detached host gate"
    );
    assert_eq!(text(&mut regular_frames).await, "regular");
    ws::ws_unregister(target.id);
    assert!(!ws::has_regular_websocket("Room:test"));
    assert!(fetch(&runtime, "/abandon").await.is_err());
    assert_eq!(
        fetch(&runtime, "/cleanup").await?.body,
        b"abandoned upgrade cleaned"
    );
    assert!(!ws::has_regular_websocket("Room:test"));

    let refused = fetch(&runtime, "/hib").await?;
    let Some(HttpResponseWebSocket::Cell(target)) = refused.websocket else {
        panic!("refusal upgrade")
    };
    let refused_id = target.id;
    let (output, mut refused_frames) = tokio::sync::mpsc::unbounded_channel();
    ws::ws_register(refused_id, output);
    assert_eq!(text(&mut refused_frames).await, "welcome");
    signals.hold.store(true, Ordering::Release);
    let host = runtime.clone();
    let refusing =
        cellar::asyncrt::spawn(async move { message(&host, refused_id, "fail-late").await });
    signals.started.notified().await;
    let first = held_requests.recv().await.unwrap();
    fetch(&runtime, "/broadcast").await?;
    held_requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(()))
        .unwrap();
    signals.resume.notify_one();
    assert!(refusing.await?.is_err());
    first
        .reply
        .send(Err(cellar_logic::RequestError::DurabilityUnproven))
        .unwrap();
    assert!(matches!(
        refused_frames.recv().await,
        Some(WsOut::Close(1011, _))
    ));
    assert!(
        refused_frames.try_recv().is_err(),
        "refused proof drops every frame behind it"
    );
    cellar::asyncrt::timeout(Duration::from_secs(1), ws::ws_await_flushes(refused_id)).await?;
    signals.hold.store(false, Ordering::Release);
    ws::ws_unregister(refused_id);

    let worker = runtime
        .fetch_worker("http://worker/".into(), "GET".into(), vec![], vec![])
        .await?;
    let Some(HttpResponseWebSocket::Worker(owner)) = worker.websocket else {
        panic!("worker upgrade")
    };
    let (output, mut worker_frames) = tokio::sync::mpsc::unbounded_channel();
    ws::ws_register(owner.id(), output);
    let incoming = owner.inbound();
    incoming
        .send(SocketEvent::Text("ping".into()))
        .await
        .map_err(|_| anyhow!("worker queue closed"))?;
    assert_eq!(text(&mut worker_frames).await, "worker:ping");
    drop(incoming);
    drop(owner);
    signals.worker_done.notified().await;

    runtime.swap_out_cell("Room:test", 1).await?;
    shutdown.send(true)?;
    adapter.await?;
    Ok(())
}
