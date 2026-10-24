// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::support;
use anyhow::{bail, Result};
use cellar::sdk::{
    websocket::{Message, SocketEvent},
    Cell, Context, HandlerFuture, Invocation, Output, Request,
};
use std::sync::Arc;

pub struct Room;

impl Cell for Room {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match event {
                Invocation::Fetch(request) => {
                    support::method(&request, &["GET"])?;
                    let url = url::Url::parse(&request.url)?;
                    if url.path().ends_with("/state") {
                        return support::json(
                            &context,
                            serde_json::json!({
                                "messages": context.storage()?.get_json("messages")?.unwrap_or(serde_json::json!(0)),
                            }),
                        );
                    }
                    if !request.headers.iter().any(|(k, v)| {
                        k.eq_ignore_ascii_case("upgrade") && v.eq_ignore_ascii_case("websocket")
                    }) {
                        return support::text(&context, 426, "use a WebSocket client");
                    }
                    let pair = context.websocket_pair()?;
                    pair.server
                        .accept_hibernating(&context, vec!["room".into()])?;
                    Ok(Output::Http(
                        pair.client.into_response(&context, Vec::new())?,
                    ))
                }
                Invocation::WebSocketMessage { id, data } => {
                    let storage = context.storage()?;
                    let count = storage
                        .get_json("messages")?
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    storage.put_json(
                        "messages",
                        &serde_json::json!(count
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("message overflow"))?),
                    )?;
                    let socket = context.websocket(id)?;
                    match data {
                        Message::Text(text) => socket.send_text(&context, text)?,
                        Message::Binary(bytes) => socket.send_binary(&context, bytes)?,
                    }
                    Ok(Output::Done)
                }
                _ => Ok(Output::Done),
            }
        })
    }
}

pub async fn client(context: &Context, outbound: bool) -> Result<Output> {
    // Consume a cell upgrade locally; the same handle also accepts outbound upgrades.
    let request = Request {
        url: "https://room/rooms/client".into(),
        method: "GET".into(),
        headers: vec![("upgrade".into(), "websocket".into())],
        body: Vec::new().into(),
        entrypoint: None,
    };
    let socket = if outbound {
        let peer = context
            .var("WEBSOCKET_PEER")
            .ok_or_else(|| support::reject(400, "set WEBSOCKET_PEER in project .dev.vars"))?;
        context.connect_websocket(peer, Vec::new()).await?
    } else {
        let mut response = context
            .namespace("ROOMS")?
            .get_by_name("client")
            .fetch(context, request)
            .await?;
        context.accept_websocket(&mut response).await?
    };
    socket.send_text(context, "native Rust hello")?;
    loop {
        match socket.next(context).await? {
            SocketEvent::Text(text) => {
                socket.close(context, 1000, "done")?;
                return support::json(context, serde_json::json!({ "echo": text }));
            }
            SocketEvent::Close(..) => bail!("peer closed before echo"),
            _ => {}
        }
    }
}
