// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-specific storage outage with independent pre-replay event audits.
//! This is an injected local-store fault, not physical network isolation.

use super::*;
use anyhow::Result;
use serde_json::{json, Value};
#[path = "store_outage.rs"]
mod store_outage;
pub(crate) use store_outage::Outage;

pub(super) fn source_scope() -> String {
    let script = "embedded-node";
    let namespace = format!("cells:v1:{}:{script}:Account", script.len());
    let id = cellar_native::cell_id::for_name(&namespace, "events");
    format!("Account:{}", cellar_native::cell_id::encode_hex(&id))
}

async fn inspect(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(operator(
        client,
        node,
        &source_scope(),
        &json!({"op":"inspect"}),
        "outage-audit",
    )
    .await?["result"]
        .clone())
}

async fn subscription(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "outage-audit",
    )
    .await?["result"]
        .clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_isolated_event_owner_expires_and_recovers_before_replay() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let old_core = Executor::new("isolated-event-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (isolated, outage) = Outage::wrap(&bucket)?;
    let (mut config, services) = inputs(
        "isolated-event-owner",
        &directory.path().join("old"),
        &old_core,
        Some(isolated),
        0,
    )
    .await?;
    config.cell_limits.events.lease_ms = 10_000;
    let owner = Node::start(config, services, graph(1, false), applications("old")?).await?;
    super::super::ready(&owner).await?;
    let client = client()?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "old:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while inspect(&client, &owner).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        call(
            &client,
            &owner,
            reqwest::Method::POST,
            "/process-save?source=projection"
        )
        .await?
            == "1"
    );
    call(&client, &owner, reqwest::Method::POST, "/revoke-publish").await?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "old:2");
    let pending = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = inspect(&client, &owner).await?;
            if state["lanes"][0]["blocked_reason"] == "Forbidden" {
                return Ok::<_, anyhow::Error>(state);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    ensure!(
        pending["pending_rows"] == "1"
            && pending["lanes"][0]["revision"]["settled_through"] == "1"
            && pending["lanes"][0]["revision"]["next_sequence"] == "3"
    );
    let history = stream_operator(
        &client,
        &owner,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "outage-audit",
    )
    .await?["result"]["records"]
        .clone();
    ensure!(history.as_array().is_some_and(|records| records.len() == 1));
    ensure!(
        history[0]["event"]["payload"] == json!({"value":1})
            && history[0]["event"]["sequence"] == "1"
            && history[0]["event"]["source"] == source_scope()
    );
    let leased = subscription(&client, &owner).await?;
    ensure!(leased["checkpoint"] == "0" && leased["observation"]["lease_active"] == true);
    let old_services = owner.services();
    outage.isolate();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        owner.run_until(std::future::pending()),
    )
    .await??;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseExpired
            },
        "outage returned {outcome:?}"
    );
    ensure!(
        outage.refused() > 0,
        "outage did not refuse real storage calls"
    );
    ensure!(
        old_services
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err(),
        "fenced domain remained open"
    );
    ensure!(
        old_core.handle().spawn(async { 7 }).await? == 7,
        "fence stopped caller executor"
    );

    // The successor has an independent healthy client and executor. The old
    // client remains isolated throughout recovery and the final audit.
    let new_core = Executor::new("successor-event-core")?;
    let (config, services) = inputs(
        "successor-event-owner",
        &directory.path().join("new"),
        &new_core,
        Some(bucket),
        0,
    )
    .await?;
    let mut declaration = graph(2, false);
    declaration.primary.event_streams[0]
        .grants
        .iter_mut()
        .find(|grant| grant.id == "application")
        .unwrap()
        .publish = false;
    declaration.primary.options.stream_bindings[0].publish = false;
    // This successor binds to the already configured resource. Reissuing its
    // administrative revision under a new deployment operation is a conflict;
    // recovery must not create a new configuration to make the audit pass.
    declaration.primary.event_streams.clear();
    let successor = Node::start(config, services, declaration, applications("new")?).await?;
    super::super::ready(&successor).await?;
    let recovered = inspect(&client, &successor).await?;
    ensure!(
        recovered["incarnation"] == pending["incarnation"]
            && recovered["pending_rows"] == "1"
            && recovered["lanes"][0]["revision"] == pending["lanes"][0]["revision"],
        "source obligation changed before replay: {recovered}"
    );
    ensure!(call(&client, &successor, reqwest::Method::GET, "/read").await? == "new:2");
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );
    let recovered_history = stream_operator(
        &client,
        &successor,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "outage-audit",
    )
    .await?["result"]["records"]
        .clone();
    ensure!(
        recovered_history == history,
        "accepted envelope changed before replay"
    );
    let recovered_lease = subscription(&client, &successor).await?;
    ensure!(
        recovered_lease["identity"] == leased["identity"] && recovered_lease["checkpoint"] == "0",
        "checkpoint advanced during outage"
    );
    if recovered_lease["observation"]["lease_active"] == true {
        ensure!(
            recovered_lease["lease"] == leased["lease"],
            "recovery changed the live lease"
        );
    }
    tokio::time::timeout(Duration::from_secs(12), async {
        while subscription(&client, &successor).await?["observation"]["lease_active"] == true {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/process-save?source=projection"
        )
        .await?
            == "1",
        "redelivery repeated inbox effect"
    );
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "StaleReceipt"
    );
    ensure!(
        subscription(&client, &successor).await?["checkpoint"] == "0",
        "obsolete receipt advanced checkpoint"
    );
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/ack-stored?source=projection"
        )
        .await?
            == "Acknowledged"
    );
    call(
        &client,
        &successor,
        reqwest::Method::POST,
        "/restore-publish",
    )
    .await?;
    call(&client, &successor, reqwest::Method::POST, "/retry-outbox").await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while inspect(&client, &successor).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/process-ack?source=projection"
        )
        .await?
            == "3"
    );
    ensure!(subscription(&client, &successor).await?["checkpoint"] == "2");
    ensure!(
        outage.is_isolated(),
        "old client was reconnected to repair recovery"
    );
    stopped(successor).await?;
    Ok(())
}
