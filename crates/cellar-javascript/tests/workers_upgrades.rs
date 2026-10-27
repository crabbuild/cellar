// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use fastwebsockets::{Frame, OpCode, Role, WebSocket};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/container_peer.rs"]
mod peer;
#[path = "support/polled_socket.rs"]
mod polled_socket;

#[test]
fn workers_and_objects_forward_pair_and_container_upgrades_with_joined_cleanup() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
    let built = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        built.status.success(),
        "fixture build: {}",
        String::from_utf8_lossy(&built.stderr)
    );
    joined_node::run(
        Options {
            max_body_bytes: 2048,
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/web-upgrade.mjs")),
        Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(run(application))
        },
    )
}

async fn run(application: Arc<cellar_javascript::JavaScriptApplication>) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let peer = peer::Peer::start_with_http_port(8080).await?;
    let (node, address) = native::start_configured(
        directory.path(),
        Arc::new(polled_socket::ApplicationProbe(application.clone())),
        |config, services, graph| {
            peer.configure(config, services, graph);
            config.runtime.handler_budget = Duration::from_secs(5);
        },
    )
    .await?;
    let result = exercise(&address).await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let usage = application.bridge_usage();
    result.with_context(|| format!("upgrade bridge={usage:?}, shutdown={shutdown:?}"))?;
    ensure!(
        matches!(shutdown?, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    ensure!(
        usage.active_invocations == 0,
        "upgrade leaked bridge resources: {usage:?}"
    );
    ensure!(
        peer.websockets.load(std::sync::atomic::Ordering::SeqCst) == 0,
        "container socket leaked"
    );
    Ok(())
}

async fn exercise(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()?;
    for route in ["worker/pair", "do/pair", "do/port"] {
        for abrupt in [false, true] {
            let response = client
                .get(format!("{address}/{route}"))
                .header("connection", "upgrade")
                .header("upgrade", "websocket")
                .header("sec-websocket-version", "13")
                .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
                .send()
                .await?;
            ensure!(
                response.status().as_u16() == 101,
                "{route} upgrade: {}",
                response.text().await?
            );
            if route.ends_with("pair") {
                ensure!(response.headers()["x-upgrade-kind"] == "pair");
            }
            if route.starts_with("do/") {
                ensure!(response.headers()["x-relay"] == "worker");
            }
            let mut socket = WebSocket::after_handshake(response.upgrade().await?, Role::Client);
            for (opcode, payload) in [
                (OpCode::Text, b"hello".to_vec()),
                (OpCode::Binary, vec![0, 255, 42]),
            ] {
                socket
                    .write_frame(Frame::new(true, opcode, None, payload.clone().into()))
                    .await?;
                let frame =
                    tokio::time::timeout(Duration::from_secs(3), socket.read_frame()).await??;
                ensure!(
                    frame.opcode == opcode && frame.payload.as_ref() == payload,
                    "{route}: incorrect echo"
                );
            }
            if !abrupt {
                socket.write_frame(Frame::close(3001, b"done")).await?;
            }
        }
    }
    let accepted = client.get(format!("{address}/accept")).send().await?;
    let status = accepted.status();
    let body = accepted.text().await?;
    ensure!(
        status.is_success() && body == "[0,255,42]",
        "accepted namespace: {status} {body}"
    );
    let state: serde_json::Value = client
        .get(format!("{address}/do/state"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        state["messages"] == 5,
        "message callbacks lost native storage authority: {state}"
    );
    // A native close must wake a bridge whose local receiver never accepts.
    let stalled = client
        .get(format!("{address}/do/stalled"))
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .send()
        .await?;
    ensure!(
        stalled.status().as_u16() == 101,
        "stalled receiver upgrade failed"
    );
    let mut stalled = WebSocket::after_handshake(stalled.upgrade().await?, Role::Client);
    stalled
        .write_frame(Frame::text(b"unaccepted".to_vec().into()))
        .await?;
    drop(stalled);
    let declined = client
        .get(format!("{address}/do/decline"))
        .header("upgrade", "websocket")
        .send()
        .await?;
    ensure!(declined.status().as_u16() == 302 && declined.text().await? == "container response");
    ensure!(
        client
            .get(format!("{address}/do/destroy"))
            .send()
            .await?
            .status()
            .as_u16()
            == 204
    );
    let native = client
        .get(format!("{address}/native/open"))
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .send()
        .await?;
    ensure!(
        native.status().as_u16() == 101,
        "native polled upgrade failed"
    );
    let mut native = WebSocket::after_handshake(native.upgrade().await?, Role::Client);
    let borrowed = client
        .get(format!("{address}/native/borrow"))
        .send()
        .await?;
    let status = borrowed.status();
    let body = borrowed.text().await?;
    ensure!(
        status.is_success() && body == "refused",
        "polled socket event ownership: {status} {body}"
    );
    ensure!(
        tokio::time::timeout(Duration::from_millis(25), native.read_frame())
            .await
            .is_err(),
        "borrow refusal closed the original live socket"
    );
    ensure!(client
        .get(format!("{address}/native/retire"))
        .send()
        .await?
        .status()
        .is_success());
    let ended = tokio::time::timeout(Duration::from_secs(3), native.read_frame()).await?;
    ensure!(
        ended.is_err() || ended?.opcode == OpCode::Close,
        "opening event retired without closing its socket"
    );
    Ok(())
}
