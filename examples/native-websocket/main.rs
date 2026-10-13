//! Outbound WebSockets with event-owned Workers and persistent business cells.
//! Set CELLAR_WEBSOCKET_PEER to the peer's ws:// or wss:// origin before running.

use anyhow::{bail, Result};
use cellar::sdk::{
    http::Response,
    websocket::{Message, ReadyState, SocketEvent, UpgradeResponse},
    Application, Cell, Context, HandlerFuture, Invocation, Output, Registry,
};
use cellar::{daemon::NativeDaemon, native::web::Headers};
use std::sync::Arc;

use cellar::{
    engine_api::{Compat, WorkerConfigOptions},
    fleet::LoadedDeployment,
    generation::DeploymentGraph,
};

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "native-websocket-client".into(),
            do_classes: vec!["Peer".into(), "EchoRoom".into()],
            bindings: vec![
                ("PEERS".into(), "Peer".into()),
                ("ROOMS".into(), "EchoRoom".into()),
            ],
            sql_bindings: vec![],
            kv_bindings: vec![],
            blob_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: String::new(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: "native-websocket-client".into(),
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
    })
}

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn output(context: &Context, value: serde_json::Value) -> Result<Output> {
    Ok(Output::Http(
        Response::new(200, value.to_string(), Vec::new()).into_http(context)?,
    ))
}

fn upgrade_request() -> cellar::sdk::Request {
    cellar::sdk::Request {
        url: "https://room/upgrade".into(),
        method: "GET".into(),
        headers: vec![("upgrade".into(), "websocket".into())],
        body: Vec::new().into(),
        entrypoint: None,
    }
}

async fn consume(
    context: &Context,
    mut response: cellar::engine_api::HttpResponse,
) -> Result<Output> {
    if response.status != 101 {
        if let Some(stream) = response.stream.take() {
            use futures_util::TryStreamExt;
            for bytes in stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(anyhow::Error::msg)?
            {
                response.body.extend_from_slice(&bytes);
            }
        }
        bail!(
            "upgrade response {}: {}",
            response.status,
            String::from_utf8_lossy(&response.body)
        );
    }
    let socket = context.accept_websocket(&mut response).await?;
    anyhow::ensure!(
        context.accept_websocket(&mut response).await.is_err(),
        "an upgrade was consumed twice"
    );
    socket.send_text(context, "client hello")?;
    socket.send_binary(context, vec![0, 128, 255])?;
    let mut messages = Vec::new();
    loop {
        match socket.next(context).await? {
            SocketEvent::Text(text) => messages.push(serde_json::json!(text)),
            SocketEvent::Binary(bytes) => messages.push(serde_json::json!(bytes)),
            SocketEvent::Close(code, reason, clean) => {
                return output(
                    context,
                    serde_json::json!({
                        "messages": messages,
                        "close": [code, reason, clean],
                    }),
                )
            }
            SocketEvent::Open(_) => {}
        }
    }
}

async fn worker_server(context: &Context) -> Result<Output> {
    let pair = context.websocket_pair()?;
    pair.server.accept(context)?;
    pair.server.send_text(context, "worker greeting")?;
    let server = pair.server;
    let background = context.clone();
    context.wait_until(async move {
        loop {
            match server.next(&background).await? {
                SocketEvent::Text(text) => {
                    server.send_text(&background, format!("reply:{text}"))?
                }
                SocketEvent::Binary(bytes) => {
                    server.send_binary(&background, bytes)?;
                    server.close(&background, 4001, "complete")?;
                }
                SocketEvent::Close(..) => break,
                SocketEvent::Open(_) => {}
            }
        }
        Ok(())
    })?;
    Ok(Output::Http(
        pair.client.into_response(context, Vec::new())?,
    ))
}

struct EchoRoom(String);

impl Cell for EchoRoom {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match invocation {
                Invocation::Fetch(request) => {
                    if request.url.ends_with("/state") {
                        return output(&context, storage.get_json("state")?.unwrap_or_default());
                    }
                    storage.put_json("state", &serde_json::json!({ "owner": self.0 }))?;
                    let pair = context.websocket_pair()?;
                    pair.server
                        .accept_hibernating(&context, vec!["room".into()])?;
                    pair.server.send_text(&context, "room greeting")?;
                    Ok(Output::Http(
                        pair.client.into_response(&context, Vec::new())?,
                    ))
                }
                Invocation::WebSocketMessage { id, data } => {
                    let socket = context.websocket(id)?;
                    match data {
                        Message::Text(text) => {
                            socket.send_text(&context, format!("reply:{text}"))?
                        }
                        Message::Binary(bytes) => {
                            storage.put_json(
                                "state",
                                &serde_json::json!({
                                    "owner": self.0,
                                    "received": bytes,
                                }),
                            )?;
                            socket.send_binary(&context, bytes)?;
                            socket.close(&context, 4001, "complete")?;
                        }
                    }
                    Ok(Output::Done)
                }
                Invocation::WebSocketClose { code, .. } => {
                    let mut state = storage
                        .get_json("state")?
                        .unwrap_or_else(|| serde_json::json!({}));
                    state["close"] = serde_json::json!(code);
                    storage.put_json("state", &state)?;
                    Ok(Output::Done)
                }
                _ => Ok(Output::Done),
            }
        })
    }
}

struct PeerCell(String);

impl Cell for PeerCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match invocation {
                Invocation::Fetch(request) => {
                    if request.url.contains("/consume/") {
                        let response = if request.url.ends_with("/worker") {
                            context
                                .exported_entrypoint("default")?
                                .fetch(
                                    &context,
                                    cellar::sdk::Request {
                                        url: "https://worker/service".into(),
                                        ..upgrade_request()
                                    },
                                )
                                .await?
                        } else {
                            context
                                .namespace("ROOMS")?
                                .get_by_name("shared")
                                .fetch(&context, upgrade_request())
                                .await?
                        };
                        return consume(&context, response).await;
                    }
                    if request.url.ends_with("/state") {
                        return output(&context, storage.get_json("state")?.unwrap_or_default());
                    }
                    storage.put_json("opening", &serde_json::json!(true))?;
                    let socket = context
                        .connect_websocket(format!("{}/echo", self.0), vec!["cell.v1".into()])
                        .await?;
                    assert_eq!(socket.ready_state(), ReadyState::Open);
                    socket.send_text(&context, "cell hello")?;
                    socket.send_binary(&context, vec![0, 128, 255])?;
                    output(
                        &context,
                        serde_json::json!({
                            "connected": true,
                            "protocol": socket.protocol(),
                        }),
                    )
                }
                Invocation::WebSocketOpen { id, protocol } => {
                    let socket = context.websocket(id)?;
                    assert_eq!(socket.protocol(), protocol);
                    assert_eq!(socket.ready_state(), ReadyState::Open);
                    storage.put_json(
                        "state",
                        &serde_json::json!({
                            "protocol": protocol,
                            "messages": [],
                        }),
                    )?;
                    Ok(Output::Done)
                }
                Invocation::WebSocketMessage { id, data } => {
                    let mut state = storage.get_json("state")?.unwrap();
                    let (value, last) = match data {
                        Message::Text(text) => (serde_json::json!(text), false),
                        Message::Binary(bytes) => (serde_json::json!(bytes), true),
                    };
                    state["messages"].as_array_mut().unwrap().push(value);
                    storage.put_json("state", &state)?;
                    if last {
                        context.websocket(id)?.close(&context, 1000, "done")?;
                    }
                    Ok(Output::Done)
                }
                Invocation::WebSocketClose {
                    id,
                    code,
                    was_clean,
                    ..
                } => {
                    assert_eq!(context.websocket(id)?.ready_state(), ReadyState::Closed);
                    let mut state = storage.get_json("state")?.unwrap();
                    state["close"] = serde_json::json!([code, was_clean]);
                    storage.put_json("state", &state)?;
                    Ok(Output::Done)
                }
                _ => Ok(Output::Done),
            }
        })
    }
}

fn application(peer: String) -> Result<Arc<dyn Application>> {
    let worker_peer = peer.clone();
    Ok(Arc::new(
        Registry::new(move |context, invocation| {
            let peer = worker_peer.clone();
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    bail!("expected HTTP");
                };
                let url = url::Url::parse(&request.url)?;
                if url.path() == "/room-state" {
                    let request = cellar::sdk::Request {
                        url: "https://room/state".into(),
                        headers: Vec::new(),
                        ..upgrade_request()
                    };
                    return Ok(Output::Http(
                        context
                            .namespace("ROOMS")?
                            .get_by_name("shared")
                            .fetch(&context, request)
                            .await?,
                    ));
                }
                if url.path() == "/service" {
                    return worker_server(&context).await;
                }
                if let Some(kind) = url.path().strip_prefix("/bind/") {
                    let response = if kind == "worker" {
                        context
                            .exported_entrypoint("default")?
                            .fetch(
                                &context,
                                cellar::sdk::Request {
                                    url: "https://worker/service".into(),
                                    ..upgrade_request()
                                },
                            )
                            .await?
                    } else {
                        context
                            .namespace("ROOMS")?
                            .get_by_name("shared")
                            .fetch(&context, upgrade_request())
                            .await?
                    };
                    if kind == "discard" {
                        let mut response = response;
                        drop(context.accept_websocket(&mut response));
                        return output(&context, serde_json::json!(true));
                    }
                    return consume(&context, response).await;
                }
                if let Some(name) = url
                    .path()
                    .strip_prefix("/cells/")
                    .and_then(|path| path.split('/').next())
                {
                    let cell = context.namespace("PEERS")?.get_by_name(name);
                    return Ok(Output::Http(cell.fetch(&context, request).await?));
                }
                if url.path() == "/decline" {
                    let headers =
                        Headers::from_pairs([("x-session", "first"), ("x-session", "second")])?;
                    return match context
                        .upgrade_websocket(format!("{peer}/decline"), headers)
                        .await?
                    {
                        UpgradeResponse::Declined(response) => Ok(Output::Http(response)),
                        UpgradeResponse::Upgraded(_) => bail!("expected a declined upgrade"),
                    };
                }
                if url.path() != "/echo" {
                    let result = context
                        .connect_websocket(format!("{peer}{}", url.path()), vec!["chat.v1".into()])
                        .await;
                    return output(
                        &context,
                        serde_json::json!({ "error": result.err().map(|e| e.to_string()) }),
                    );
                }
                let socket = context
                    .connect_websocket(format!("{peer}/echo"), vec!["chat.v1".into()])
                    .await?;
                socket.send_text(&context, "hello 🦀")?;
                socket.send_binary(&context, vec![0, 128, 255])?;
                let mut messages = Vec::new();
                loop {
                    match socket.next(&context).await? {
                        SocketEvent::Open(protocol) => assert_eq!(protocol, socket.protocol()),
                        SocketEvent::Text(text) => messages.push(serde_json::json!(text)),
                        SocketEvent::Binary(bytes) => {
                            messages.push(serde_json::json!(bytes));
                            socket.close(&context, 1000, "done")?;
                        }
                        SocketEvent::Close(code, _, clean) => {
                            return output(
                                &context,
                                serde_json::json!({
                                    "protocol": socket.protocol(),
                                    "messages": messages,
                                    "close": [code, clean],
                                }),
                            );
                        }
                    }
                }
            })
        })
        .cell("EchoRoom", |_, _| {
            Ok(Arc::new(EchoRoom(std::env::var("CELLAR_NODE")?)))
        })?
        .cell("Peer", move |_, _| Ok(Arc::new(PeerCell(peer.clone()))))?,
    ))
}

fn main() -> Result<()> {
    let peer = std::env::var("CELLAR_WEBSOCKET_PEER")?;
    NativeDaemon::local(graph())
        .register("native-websocket-client", "test", move || {
            application(peer.clone())
        })?
        .run()
}
