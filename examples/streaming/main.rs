// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native Rust counterparts of app.ts, app.js and app.py.

use anyhow::{bail, Result};
use cellar::{
    native::web::Headers,
    sdk::{
        http::Response,
        streams::IdentityTransformStream,
        websocket::{Message, SocketEvent},
        Cell, Context, HandlerFuture, Host, Invocation, Output, Registry, Request,
    },
};
use std::sync::Arc;

fn text(context: &Context, status: u16, body: &str) -> Result<Output> {
    Ok(Output::Http(
        Response::new(status, body, Vec::new()).into_http(context)?,
    ))
}

fn progress(context: &Context) -> Result<Output> {
    let pipe = IdentityTransformStream::new();
    let writer = pipe.writable.get_writer()?;
    let producer = context.clone();

    // The producer belongs to this response. Each awaited write preserves native
    // backpressure and the output gate; dropping the body cancels the producer.
    context.wait_until_byte_stream(&pipe.readable, async move {
        for step in 1..=3 {
            let event = format!("event: progress\nid: {step}\ndata: {{\"step\":{step}}}\n\n");
            producer
                .write_stream_chunk(&writer, event.into_bytes())
                .await?;
        }
        producer
            .write_stream_chunk(&writer, b"event: done\ndata: complete\n\n".to_vec())
            .await?;
        producer.finish_stream(&writer).await?;
        Ok(())
    })?;

    let headers = Headers::from_pairs([
        ("content-type", "text/event-stream"),
        ("cache-control", "no-cache"),
    ])?;
    Ok(Output::Http(
        Response::from_byte_stream(200, &pipe.readable, &headers)?.into_http(context)?,
    ))
}

fn worker_socket(context: &Context) -> Result<Output> {
    let pair = context.websocket_pair()?;
    pair.server.accept(context)?;
    let socket = pair.server;
    let consumer = context.clone();

    // Workers retain a consumer in the opening invocation. Cell sockets below
    // instead receive a new invocation for each native WebSocket event.
    context.wait_until(async move {
        loop {
            match socket.next(&consumer).await? {
                SocketEvent::Text(text) => socket.send_text(&consumer, text)?,
                SocketEvent::Binary(bytes) => socket.send_binary(&consumer, bytes)?,
                SocketEvent::Close(..) => return Ok(()),
                SocketEvent::Open(_) => {}
            }
        }
    })?;

    Ok(Output::Http(
        pair.client.into_response(context, Vec::new())?,
    ))
}

struct Room;

impl Cell for Room {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match event {
                Invocation::Fetch(request) => {
                    if url::Url::parse(&request.url)?.path().ends_with("/state") {
                        let count = storage
                            .get_json("messages")?
                            .unwrap_or(serde_json::json!(0));
                        return text(
                            &context,
                            200,
                            &serde_json::json!({"messages": count}).to_string(),
                        );
                    }
                    let pair = context.websocket_pair()?;
                    pair.server
                        .accept_hibernating(&context, vec!["echo".into()])?;
                    Ok(Output::Http(
                        pair.client.into_response(&context, Vec::new())?,
                    ))
                }
                Invocation::WebSocketMessage { id, data } => {
                    let count = storage
                        .get_json("messages")?
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0);
                    storage.put_json("messages", &serde_json::json!(count + 1))?;

                    // The native output gate proves the writes before releasing
                    // this echo. Resolve the socket with the current context.
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

async fn fetch(context: Context, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let path = url.path();
    if request.method != "GET" {
        return text(&context, 405, "Use GET");
    }
    let room = path
        .strip_prefix("/rooms/")
        .and_then(|route| route.split_once('/'));
    let room = room.filter(|(name, kind)| {
        !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
            && matches!(*kind, "ws" | "state")
    });
    let is_socket = path == "/ws" || room.is_some_and(|(_, kind)| kind == "ws");
    if is_socket
        && !request.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("websocket")
        })
    {
        return text(&context, 426, "Use a WebSocket client");
    }
    match path {
        "/events" => progress(&context),
        "/ws" => worker_socket(&context),
        _ => {
            if let Some((name, _)) = room {
                return Ok(Output::Http(
                    context
                        .namespace("ROOMS")?
                        .get_by_name(name)
                        .fetch(&context, request)
                        .await?,
                ));
            }
            text(
                &context,
                404,
                "Use /events, /ws, /rooms/demo/ws or /rooms/demo/state",
            )
        }
    }
}

fn main() -> Result<()> {
    Host::fleet()
        .register("streaming-example", "v1", || {
            Ok(Arc::new(
                Registry::new(|context, event| {
                    Box::pin(async move {
                        let Invocation::Fetch(request) = event else {
                            bail!("expected HTTP");
                        };
                        fetch(context, request).await
                    })
                })
                .cell("Room", |_, _| Ok(Arc::new(Room)))?,
            ))
        })?
        .run()
}
