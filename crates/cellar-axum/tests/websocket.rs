// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
mod support;
use anyhow::{ensure, Result};
use axum::{response::Response, routing::get, Router};
use cellar::sdk::{websocket::Message, Cell, Context, HandlerFuture, Invocation, Output};
use cellar_axum::{
    ApplicationBuilder, ApplicationDefinition, AxumCell, CellContext, CellWebSocketUpgrade,
};
use fastwebsockets::{Frame, OpCode, Role, WebSocket};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

async fn opening(
    CellContext(context): CellContext,
    upgrade: CellWebSocketUpgrade,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let result = (|| {
        context
            .storage()?
            .put_json("opened", &serde_json::json!(true))?;
        let (socket, response) = upgrade.accept_hibernating(vec!["echo".into()])?;
        socket.send_text(&context, "ready")?;
        Ok(response)
    })();
    result.map_err(|error: anyhow::Error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
    })
}

struct Events;

impl Cell for Events {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if let Invocation::WebSocketMessage { id, data } = invocation {
                let socket = context.websocket(id)?;
                match data {
                    Message::Text(text) => socket.send_text(&context, format!("echo:{text}"))?,
                    Message::Binary(bytes) => {
                        context
                            .storage()?
                            .put_json("message", &serde_json::json!(bytes))?;
                        socket.send_binary(&context, bytes)?;
                        socket.close(&context, 1000, "done")?;
                    }
                }
            }
            Ok(Output::Done)
        })
    }
}

fn definition() -> Result<ApplicationDefinition> {
    ApplicationBuilder::default()
        .cell("Counter", |_, _| {
            Ok(Arc::new(AxumCell::with_cell(
                Router::new().route("/sockets/{name}", get(opening)),
                Arc::new(Events),
            )))
        })?
        .cell_route("/sockets/{name}", "COUNTERS", "Counter", "name")?
        .build()
}

async fn connect(url: &str) -> Result<WebSocket<TcpStream>> {
    connect_path(url, "/sockets/alpha").await
}

async fn connect_path(url: &str, path: &str) -> Result<WebSocket<TcpStream>> {
    let address = url.strip_prefix("http://").unwrap();
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes()).await?;
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        ensure!(headers.len() < 16384);
        headers.push(stream.read_u8().await?);
    }
    let headers = String::from_utf8(headers)?;
    ensure!(
        headers.starts_with("HTTP/1.1 101"),
        "upgrade declined: {headers}"
    );
    ensure!(headers
        .to_ascii_lowercase()
        .contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="));
    Ok(WebSocket::after_handshake(stream, Role::Client))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cell_upgrade_messages_and_close_use_the_existing_transport_on_both_hosts() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", definition).await?;
        let mut socket =
            tokio::time::timeout(Duration::from_secs(10), connect(&host.url)).await??;
        let ready = socket.read_frame().await?;
        ensure!(ready.opcode == OpCode::Text && ready.payload.as_ref() == b"ready");
        socket
            .write_frame(Frame::text(b"hello".as_slice().into()))
            .await?;
        let echo = socket.read_frame().await?;
        ensure!(echo.payload.as_ref() == b"echo:hello");
        socket
            .write_frame(Frame::binary(vec![0, 128, 255].into()))
            .await?;
        let echo = socket.read_frame().await?;
        ensure!(echo.opcode == OpCode::Binary && echo.payload.as_ref() == [0, 128, 255]);
        ensure!(socket.read_frame().await?.opcode == OpCode::Close);
        drop(socket);
        host.stop().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_websocket_upgrade_survives_owner_handoff() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
    let a = support::start_with(
        &directory.path().join("a"),
        false,
        "",
        "a",
        Some(bucket.clone()),
        definition,
    )
    .await?;
    let mut socket = connect(&a.url).await?;
    ensure!(socket.read_frame().await?.payload.as_ref() == b"ready");
    socket
        .write_frame(Frame::binary(vec![0, 128, 255].into()))
        .await?;
    ensure!(socket.read_frame().await?.opcode == OpCode::Binary);
    ensure!(socket.read_frame().await?.opcode == OpCode::Close);
    drop(socket);
    let b = support::start_with(
        &directory.path().join("b"),
        true,
        "",
        "b",
        Some(bucket),
        definition,
    )
    .await?;
    // A is the owner; B's HTTP ingress must return the existing remote target.
    let mut remote = connect(&b.url).await?;
    ensure!(remote.read_frame().await?.payload.as_ref() == b"ready");
    remote
        .write_frame(Frame::binary(vec![0, 128, 255].into()))
        .await?;
    ensure!(remote.read_frame().await?.opcode == OpCode::Binary);
    ensure!(remote.read_frame().await?.opcode == OpCode::Close);
    drop(remote);
    a.stop_mode(cellar::sdk::ShutdownMode::Handoff).await?;
    let mut recovered = connect(&b.url).await?;
    ensure!(recovered.read_frame().await?.payload.as_ref() == b"ready");
    recovered
        .write_frame(Frame::binary(vec![0, 128, 255].into()))
        .await?;
    ensure!(recovered.read_frame().await?.opcode == OpCode::Binary);
    ensure!(recovered.read_frame().await?.opcode == OpCode::Close);
    drop(recovered);
    b.stop().await?;
    Ok(())
}

async fn worker(
    CellContext(context): CellContext,
    upgrade: CellWebSocketUpgrade,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let result = (|| {
        let (socket, response) = upgrade.accept()?;
        let background = context.clone();
        context.wait_until(async move {
            loop {
                match socket.next(&background).await? {
                    cellar::sdk::websocket::SocketEvent::Text(text) => {
                        socket.send_text(&background, format!("worker:{text}"))?
                    }
                    cellar::sdk::websocket::SocketEvent::Binary(bytes) => {
                        socket.send_binary(&background, bytes)?;
                        socket.close(&background, 1000, "done")?;
                    }
                    cellar::sdk::websocket::SocketEvent::Close(..) => break,
                    cellar::sdk::websocket::SocketEvent::Open(_) => {}
                }
            }
            // Nested background work belongs to the same drain registry.
            background.wait_until(async { Ok(()) })?;
            Ok(())
        })?;
        Ok(response)
    })();
    result.map_err(|error: anyhow::Error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_sockets_and_nested_wait_until_are_owned_until_close() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", || {
            ApplicationBuilder::new(Router::new().route("/worker", get(worker)))
                .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))?
                .build()
        })
        .await?;
        let mut socket = connect_path(&host.url, "/worker").await?;
        socket
            .write_frame(Frame::text(b"hello".as_slice().into()))
            .await?;
        ensure!(socket.read_frame().await?.payload.as_ref() == b"worker:hello");
        socket
            .write_frame(Frame::binary(vec![1, 2, 3].into()))
            .await?;
        ensure!(socket.read_frame().await?.payload.as_ref() == [1, 2, 3]);
        ensure!(socket.read_frame().await?.opcode == OpCode::Close);
        drop(socket);
        host.stop().await?;
    }
    Ok(())
}

async fn binding(
    CellContext(context): CellContext,
) -> Result<String, (axum::http::StatusCode, String)> {
    let result = async {
        let mut response = context
            .namespace("COUNTERS")?
            .get_by_name("alpha")
            .fetch(
                &context,
                cellar::sdk::Request {
                    url: "https://binding/sockets/alpha".into(),
                    method: "GET".into(),
                    headers: vec![("upgrade".into(), "websocket".into())],
                    body: Vec::new().into(),
                    entrypoint: None,
                },
            )
            .await?;
        let socket = context.accept_websocket(&mut response).await?;
        ensure!(
            context.accept_websocket(&mut response).await.is_err(),
            "upgrade capability was consumed twice"
        );
        loop {
            match socket.next(&context).await? {
                cellar::sdk::websocket::SocketEvent::Text(text) if text == "ready" => break,
                cellar::sdk::websocket::SocketEvent::Open(_) => {}
                _ => anyhow::bail!("missing ready frame"),
            }
        }
        socket.send_text(&context, "bound")?;
        let text = match socket.next(&context).await? {
            cellar::sdk::websocket::SocketEvent::Text(text) => text,
            _ => anyhow::bail!("missing bound echo"),
        };
        socket.send_binary(&context, vec![0, 128, 255])?;
        while !matches!(
            socket.next(&context).await?,
            cellar::sdk::websocket::SocketEvent::Close(..)
        ) {}
        Ok(text)
    }
    .await;
    result.map_err(|error: anyhow::Error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_binding_upgrades_transfer_without_requiring_network_handshake_headers() -> Result<()> {
    for external in [false, true] {
        let directory = tempfile::tempdir()?;
        let host = support::start(directory.path(), external, "", || {
            ApplicationBuilder::new(Router::new().route("/binding", get(binding)))
                .cell("Counter", |_, _| {
                    Ok(Arc::new(AxumCell::with_cell(
                        Router::new().route("/sockets/{name}", get(opening)),
                        Arc::new(Events),
                    )))
                })?
                .cell_route("/sockets/{name}", "COUNTERS", "Counter", "name")?
                .build()
        })
        .await?;
        let response = support::client()?
            .get(format!("{}/binding", host.url))
            .send()
            .await?;
        ensure!(
            response.status() == 200,
            "binding upgrade: {}",
            response.text().await?
        );
        ensure!(response.text().await? == "echo:bound");
        host.stop().await?;
    }
    Ok(())
}
