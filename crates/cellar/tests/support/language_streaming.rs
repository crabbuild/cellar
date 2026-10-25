// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Network assertions shared by the optional language adapters.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use fastwebsockets::{Frame, OpCode, Role, WebSocket};
use std::time::{Duration, Instant};

async fn reset(client: &reqwest::Client, address: &str, phase: &str) -> Result<()> {
    let started = Instant::now();
    let response = client
        .get(format!("{address}/reset"))
        .send()
        .await
        .with_context(|| format!("reset before {phase} failed after {:?}", started.elapsed()))?;
    let status = response.status();
    if !status.is_success() {
        // Keep the host's failure category: error_for_status discards this body.
        let body = response.text().await?;
        anyhow::bail!(
            "reset before {phase} returned {status} after {:?}: {body}",
            started.elapsed()
        );
    }
    Ok(())
}

pub async fn check(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    for path in ["/sse", "/cell/sse", "/consume/sse", "/outbound/sse"] {
        reset(&client, address, path).await?;
        let mut response = client.get(format!("{address}{path}")).send().await?;
        ensure!(
            response.status().is_success(),
            "stream {path} failed: {}",
            response.text().await?
        );
        ensure!(response.headers()["content-type"] == "text/event-stream");
        let first = tokio::time::timeout(Duration::from_secs(3), response.chunk())
            .await??
            .unwrap();
        ensure!(
            first.as_ref() == b"event: update\nid: 1\ndata: first\n\n",
            "unexpected first chunk: {first:?}"
        );
        ensure!(
            tokio::time::timeout(Duration::from_millis(50), response.chunk())
                .await
                .is_err(),
            "stream completed before release"
        );
        if path.starts_with("/cell/") {
            let phase = client
                .get(format!("{address}/cell/phase"))
                .send()
                .await?
                .text()
                .await?;
            ensure!(
                phase == "1",
                "stream did not retain native storage authority: {phase}"
            );
        }
        client
            .get(format!("{address}/release"))
            .send()
            .await?
            .error_for_status()?;
        let rest = response.text().await?;
        ensure!(
            rest == "id: 2\ndata: second\n\n",
            "unexpected final chunk: {rest}"
        );
        if path.starts_with("/cell/") {
            ensure!(
                client
                    .get(format!("{address}/cell/phase"))
                    .send()
                    .await?
                    .text()
                    .await?
                    == "2"
            );
        }
    }
    let outbound = client.get(format!("{address}/outbound-ws")).send().await?;
    ensure!(
        outbound.status().is_success(),
        "outbound socket failed: {}",
        outbound.text().await?
    );
    ensure!(outbound.text().await? == "outbound");
    let large = client
        .get(format!("{address}/large"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    ensure!(
        large.len() == 5 * 2048,
        "stream was limited by its total length"
    );
    for (index, chunk) in large.as_chunks::<2048>().0.iter().enumerate() {
        ensure!(chunk.iter().all(|byte| *byte == index as u8));
    }
    reset(&client, address, "cancel-read").await?;
    let cancelled_read = client
        .get(format!("{address}/cancel-read"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    ensure!(
        cancelled_read == "cancelled",
        "pending body read did not cancel"
    );
    let mut failed = client
        .get(format!("{address}/error"))
        .send()
        .await?
        .error_for_status()?;
    let _ = failed.chunk().await;
    ensure!(
        failed.text().await.is_err(),
        "producer failure became clean EOF"
    );

    reset(&client, address, "cancel producer / WebSocket upgrades").await?;
    let mut cancelled = client
        .get(format!("{address}/sse"))
        .send()
        .await?
        .error_for_status()?;
    ensure!(cancelled.chunk().await?.is_some());
    drop(cancelled);
    // Both upgrades travel through the public network handshake; cell upgrades
    // additionally cross the language-to-native namespace forwarding boundary.
    for path in ["/cell/ws", "/worker-ws"] {
        let response = client
            .get(format!("{address}{path}"))
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-version", "13")
            .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
            .send()
            .await?;
        ensure!(
            response.status().as_u16() == 101,
            "upgrade {path} failed: {}",
            response.text().await?
        );
        let upgraded = response.upgrade().await?;
        let mut socket = WebSocket::after_handshake(upgraded, Role::Client);
        for (opcode, payload) in [
            (OpCode::Text, b"hello".to_vec()),
            (OpCode::Binary, vec![0, 255, 1]),
        ] {
            socket
                .write_frame(Frame::new(true, opcode, None, payload.clone().into()))
                .await?;
            let frame = tokio::time::timeout(Duration::from_secs(3), socket.read_frame()).await??;
            ensure!(
                frame.opcode == opcode && frame.payload.as_ref() == payload,
                "incorrect socket echo"
            );
        }
        socket.write_frame(Frame::close(1000, b"done")).await?;
    }
    Ok(())
}
