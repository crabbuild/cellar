// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Real native daemon, RFC6455 peer and source compatibility oracle.
#![allow(clippy::disallowed_methods)]
use anyhow::Result;
use base64::Engine;
use fastwebsockets::{Frame, OpCode, Payload, Role, WebSocket};
use reqwest::Client;
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

#[path = "support/native_ws_node.rs"]
mod node;
use node::Node;

async fn peer(
    mut stream: TcpStream,
    events: mpsc::UnboundedSender<(String, HashMap<String, String>)>,
) -> Result<()> {
    let mut raw = Vec::new();
    while !raw.ends_with(b"\r\n\r\n") {
        raw.push(stream.read_u8().await?);
        anyhow::ensure!(raw.len() < 16_384, "oversized handshake");
    }
    let text = String::from_utf8(raw)?;
    let path = text
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_string();
    let headers: HashMap<_, _> = text
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().into()))
        .collect();
    events.send((path.clone(), headers.clone()))?;
    if path == "/hang" {
        assert_eq!(
            stream.read(&mut [0; 1]).await?,
            0,
            "a cancelled handshake retained its peer"
        );
        events.send(("/cancelled".into(), HashMap::new()))?;
        return Ok(());
    }
    if path == "/decline" {
        stream.write_all(b"HTTP/1.1 418 Teapot\r\nContent-Length: 8\r\nX-Declined: yes\r\nConnection: close\r\n\r\ndeclined").await?;
        return Ok(());
    }
    let mut digest = Sha1::new();
    digest.update(headers.get("sec-websocket-key").unwrap().as_bytes());
    digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let accept = base64::engine::general_purpose::STANDARD.encode(digest.finalize());
    let protocol = if path == "/wrong-protocol" {
        "unrequested"
    } else {
        headers
            .get("sec-websocket-protocol")
            .map(String::as_str)
            .unwrap_or("")
    };
    stream.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\nSec-WebSocket-Protocol: {protocol}\r\n\r\n").as_bytes()).await?;
    if path == "/wrong-protocol" {
        assert_eq!(stream.read(&mut [0; 1]).await?, 0);
        return Ok(());
    }
    let mut socket = WebSocket::after_handshake(stream, Role::Server);
    loop {
        let frame = socket.read_frame().await?;
        match frame.opcode {
            OpCode::Text => {
                let payload = frame.payload.to_vec();
                // The production pump must assemble fragmented messages.
                socket
                    .write_frame(Frame::new(
                        false,
                        OpCode::Text,
                        None,
                        Payload::Owned(payload[..1].to_vec()),
                    ))
                    .await?;
                socket
                    .write_frame(Frame::new(
                        true,
                        OpCode::Continuation,
                        None,
                        Payload::Owned(payload[1..].to_vec()),
                    ))
                    .await?;
            }
            OpCode::Binary => {
                socket
                    .write_frame(Frame::binary(frame.payload.to_vec().into()))
                    .await?
            }
            OpCode::Close => break,
            _ => {}
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_outbound_websockets_use_production_transport() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let root = tempfile::tempdir()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("ws://{}", listener.local_addr()?);
    let (send, mut events) = mpsc::unbounded_channel();
    let (errors, mut failures) = mpsc::unbounded_channel();
    let peers = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let send = send.clone();
            let errors = errors.clone();
            tasks.spawn(async move {
                if let Err(error) = peer(stream, send).await {
                    let _ = errors.send(error.to_string());
                }
            });
        }
    });
    let executable = root.path().join("native-websocket");
    std::fs::copy(env!("CARGO_BIN_EXE_cellar-native-websocket"), &executable)?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut node = Node::start(
        root.path(),
        "native",
        executable.to_str().unwrap(),
        &origin,
        None,
    )
    .await?;
    let expected =
        json!({"protocol":"chat.v1","messages":["hello 🦀",[0,128,255]],"close":[1000,true]});
    let value: Value = client
        .get(format!("{}/echo", node.url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(value, expected);
    assert_eq!(events.recv().await.unwrap().0, "/echo");
    let response = client.get(format!("{}/decline", node.url)).send().await?;
    assert_eq!(response.status(), 418);
    assert_eq!(response.headers()["x-declined"], "yes");
    assert_eq!(response.text().await?, "declined");
    let (_, headers) = events.recv().await.unwrap();
    assert_eq!(headers["x-session"], "first, second");
    let value: Value = client
        .get(format!("{}/wrong-protocol", node.url))
        .send()
        .await?
        .json()
        .await?;
    assert!(value["error"]
        .as_str()
        .unwrap()
        .contains("unrequested WebSocket subprotocol"));
    assert_eq!(events.recv().await.unwrap().0, "/wrong-protocol");
    let value: Value = client
        .get(format!("{}/cells/tenant-acme/open", node.url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(value, json!({"connected":true,"protocol":"cell.v1"}));
    assert_eq!(events.recv().await.unwrap().0, "/echo");
    let cell_expected =
        json!({"protocol":"cell.v1","messages":["cell hello",[0,128,255]],"close":[1000,true]});
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let value: Value = client
                .get(format!("{}/cells/tenant-acme/state", node.url))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if value.get("close").is_some() {
                assert_eq!(value, cell_expected);
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    // Retiring an opening must close hyper's handshake driver and its peer.
    let url = format!("{}/hang", node.url);
    let abort_client = client.clone();
    let opening = tokio::spawn(async move { abort_client.get(url).send().await });
    assert_eq!(events.recv().await.unwrap().0, "/hang");
    opening.abort();
    let _ = opening.await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await?
            .unwrap()
            .0,
        "/cancelled"
    );
    node.stop(&client).await?;
    peers.abort();
    let _ = peers.await;
    assert!(failures.try_recv().is_err(), "peer failed");
    Ok(())
}
