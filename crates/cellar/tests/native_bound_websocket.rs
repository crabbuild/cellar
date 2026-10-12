// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Bound upgrade capabilities use production local and cross-node transports.
#![allow(clippy::disallowed_methods)] // Process orchestration is outside application turns.
#[path = "support/native_ws_node.rs"]
mod node;
use anyhow::{Context as _, Result};
use node::Node;
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;

async fn exchange(client: &Client, node: &Node, path: &str, greeting: &str) -> Result<()> {
    let response = client.get(format!("{}{path}", node.url)).send().await?;
    let status = response.status();
    let text = response.text().await?;
    anyhow::ensure!(
        status.is_success(),
        "{} {path}: {status}: {text}\n{}",
        node.log.display(),
        std::fs::read_to_string(&node.log)?
    );
    assert_eq!(
        serde_json::from_str::<Value>(&text)?,
        json!({"messages":[greeting,"reply:client hello",[0,128,255]],"close":[4001,"complete",true]})
    );
    Ok(())
}
async fn closed_state(client: &Client, node: &Node, code: u16) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let response = client
                .get(format!("{}/room-state", node.url))
                .send()
                .await?;
            let status = response.status();
            let text = response.text().await?;
            anyhow::ensure!(status.is_success(), "state failed {status}: {text}");
            let value: Value = serde_json::from_str(&text)?;
            if value["close"] == code {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .with_context(|| {
        format!(
            "{} bound cell close handler did not report {code}",
            node.log.display()
        )
    })??;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_local_cell_worker_and_cross_node_upgrades() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(120), run()).await??;
    Ok(())
}
async fn run() -> Result<()> {
    let root = tempfile::tempdir()?;
    let executable = root.path().join("native-bound");
    std::fs::copy(env!("CARGO_BIN_EXE_cellar-native-websocket"), &executable)?;
    let executable = executable.to_str().unwrap();
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut local = Node::start(root.path(), "local", executable, "ws://127.0.0.1:1", None).await?;
    for (path, greeting) in [
        ("/bind/cell", "room greeting"),
        ("/bind/worker", "worker greeting"),
        ("/cells/client/consume/cell", "room greeting"),
        ("/cells/client/consume/worker", "worker greeting"),
    ] {
        exchange(&client, &local, path, greeting).await?;
    }
    closed_state(&client, &local, 4001).await?;
    let response = client
        .get(format!("{}/bind/discard", local.url))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.json::<Value>().await?, true);
    closed_state(&client, &local, 1006).await?;
    local.stop(&client).await?;

    // The same compiled graph is deployed on two real nodes. The first room
    // call establishes its lease; consuming from the other node then exercises
    // the parked, authenticated peer upgrade rather than local dispatch.
    let bucket = root.path().join("fleet.sqlite");
    let mut first = Node::start_fleet(root.path(), "first", executable, &bucket).await?;
    let mut second = Node::start_fleet(root.path(), "second", executable, &bucket).await?;
    exchange(&client, &first, "/bind/cell", "room greeting").await?;
    closed_state(&client, &first, 4001).await?;
    exchange(&client, &second, "/bind/cell", "room greeting").await?;
    // The existing physical server reports an application-initiated close as
    // an unclean server close, while the client receives its 4001 frame.
    closed_state(&client, &second, 1006).await?;
    let state: Value = client
        .get(format!("{}/room-state", second.url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        state["owner"], "first",
        "the second node must consume a remote owner's upgrade"
    );
    exchange(
        &client,
        &second,
        "/cells/fleet-client/consume/cell",
        "room greeting",
    )
    .await?;
    first.stop(&client).await?;
    second.stop(&client).await?;
    Ok(())
}
