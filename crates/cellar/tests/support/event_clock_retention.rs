// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual owner/proof paths under a caller-owned wall clock. Only time moves;
//! source obligations, leases, retention and takeover use ordinary node paths.

use super::event_settlement_race::{fetch, settle, status};
use super::*;
use serde_json::{json, Value};
use std::sync::atomic::AtomicI64;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn wall() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub(super) fn number(value: &Value) -> Result<i64> {
    Ok(value.as_str().context("decimal string missing")?.parse()?)
}

pub(super) async fn owner(
    observer: &ExecutionDomain,
    bucket: &cellar::bucket::Bucket,
) -> Result<Value> {
    observer
        .run(async {
            let (bytes, _) = bucket
                .get(&format!("cells/{}/own.json", stream_scope()?))
                .await?
                .context("stream owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

pub(super) async fn history(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "clock-retention",
    )
    .await?["result"]["records"]
        .clone())
}

pub(super) async fn pending(client: &reqwest::Client, node: &Node) -> Result<Value> {
    let namespace = "cells:v1:13:embedded-node:Account";
    let cell = cellar_native::cell_id::for_name(namespace, "producer");
    let scope = format!("Account:{}", cellar_native::cell_id::encode_hex(&cell));
    Ok(operator(
        client,
        node,
        &scope,
        &json!({"op":"inspect"}),
        "clock-retention",
    )
    .await?["result"]
        .clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_retention_bounded_expiry_survives_rollback_and_owner_handoff() -> Result<()> {
    run(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clock_retention_required_checkpoint_survives_expiry_rollback_and_handoff() -> Result<()> {
    run(true).await
}

async fn run(required: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("clock-retention-destination")?;
    let source_core = Executor::new("clock-retention-source")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let offset = Arc::new(AtomicI64::new(0));
    let selected = offset.clone();
    let (mut config, mut services) = inputs(
        "clock-destination",
        &directory.path().join("destination"),
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    // A 31s wall step must not manufacture a node-authority expiry. This
    // fixture's explicit 120s node TTL leaves the ordinary stream lease (30s),
    // operation (3s), shutdown and monotonic timer policies unchanged.
    config.actor.lease_ttl_ms = 120_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(120);
    services.wall_clock = Some(Arc::new(move || wall() + selected.load(Ordering::SeqCst)));
    let mut declaration = graph(1, false);
    let definition = &mut declaration.primary.event_streams[0];
    definition.max_records = 1;
    definition.max_age_ms = 10_000;
    definition.retention = if required {
        Retention::Required
    } else {
        Retention::Bounded
    };
    definition.subscriptions[0].required = required;
    let destination =
        Node::start(config, services, declaration, applications("destination")?).await?;
    super::super::ready(&destination).await?;
    let first_owner = owner(&observer, &bucket).await?;
    ensure!(first_owner["node"] == "clock-destination");
    let (config, services) = inputs(
        "clock-source",
        &directory.path().join("source"),
        &source_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut bindings = graph(1, false);
    bindings.primary.event_streams.clear();
    let source = Node::start(config, services, bindings, applications("source")?).await?;
    super::super::ready(&source).await?;
    let client = client()?;
    for cell in ["producer", "projection"] {
        ensure!(
            call(
                &client,
                &source,
                reqwest::Method::GET,
                &format!("/?source={cell}")
            )
            .await?
                == "source:0"
        );
    }
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::POST,
            "/write?source=producer"
        )
        .await?
            == "source:1"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while status(&client, &destination).await?["end"] != "2" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for first accepted event")??;
    let original = fetch(
        &client,
        &source,
        "000000000000000000000000000000d1",
        "projection",
    )
    .await?;
    ensure!(original["processed"] == 1);
    let receipt = Receipt::decode(
        original["batch"]["receipt"]
            .as_str()
            .context("original receipt missing")?,
    )?;
    let initial = status(&client, &destination).await?;
    let accepted = history(&client, &destination).await?;
    ensure!(accepted
        .as_array()
        .is_some_and(|records| records.len() == 1));
    ensure!(initial["checkpoint"] == "0" && initial["observation"]["lease_active"] == true);
    let issued = number(&original["batch"]["issued_ms"])?;
    let expires = number(&original["batch"]["expires_ms"])?;
    ensure!(expires - issued == 30_000);
    offset.store(issued + 15_000 - wall(), Ordering::SeqCst);
    let aged = status(&client, &destination).await?;
    ensure!(
        aged["observation"]["lease_active"] == true
            && number(&aged["observation"]["oldest_retained_age_ms"])? >= 10_000
    );
    ensure!(
        call(
            &client,
            &source,
            reqwest::Method::POST,
            "/write?source=producer"
        )
        .await?
            == "source:2"
    );
    let obligation = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = pending(&client, &source).await?;
            if snapshot["pending_rows"] == "1"
                && snapshot["lanes"][0]["last_error"]
                    .as_str()
                    .is_some_and(|error| error.starts_with("StreamFull:"))
            {
                return Ok::<_, anyhow::Error>(snapshot);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("observe capacity refusal behind live lease")??;
    ensure!(
        history(&client, &destination).await? == accepted,
        "live lease lost aged payload"
    );
    let protected = status(&client, &destination).await?;
    ensure!(protected["checkpoint"] == "0" && protected["lease"] == initial["lease"]);

    let expiry_cut = expires + 1_000;
    offset.store(expiry_cut - wall(), Ordering::SeqCst);
    let expired = settle(
        &client,
        &source,
        "projection",
        "extend",
        &receipt,
        expires + 60_000,
    )
    .await?;
    ensure!(
        matches!(
            expired["error"].as_str(),
            Some("LeaseExpired" | "StaleReceipt")
        ),
        "expired receipt extended: {expired}"
    );
    offset.store(0, Ordering::SeqCst);
    // Status does not advance durable time. Audit before any new mutation can
    // repair a lost logical clock or accidentally resurrect the old lease.
    let rolled = status(&client, &destination).await?;
    ensure!(
        number(&rolled["observation"]["observed_ms"])? >= expiry_cut
            && rolled["observation"]["lease_active"] == false
            && rolled["checkpoint"] == "0",
        "clock rollback resurrected authority: {rolled}"
    );
    if required {
        ensure!(
            history(&client, &destination).await? == accepted,
            "expired lease defeated required checkpoint"
        );
        ensure!(pending(&client, &source).await?["pending_rows"] == "1");
    }
    stopped(destination)
        .await
        .context("join clocked owner before takeover")?;
    let recovered = status(&client, &source).await?;
    let successor = owner(&observer, &bucket).await?;
    ensure!(
        successor["node"] != first_owner["node"]
            && successor["epoch"]
                .as_u64()
                .context("successor epoch missing")?
                > first_owner["epoch"]
                    .as_u64()
                    .context("original epoch missing")?
    );
    ensure!(
        recovered["identity"] == initial["identity"]
            && recovered["checkpoint"] == "0"
            && number(&recovered["observation"]["observed_ms"])? >= expiry_cut
            && recovered["observation"]["lease_active"] == false,
        "handoff lost durable expiry: {recovered}"
    );
    let stale = settle(&client, &source, "projection", "ack", &receipt, 0).await?;
    ensure!(
        matches!(
            stale["error"].as_str(),
            Some("LeaseExpired" | "StaleReceipt")
        ),
        "old receipt acknowledged after rollback: {stale}"
    );
    if required {
        ensure!(history(&client, &source).await? == accepted);
        let replay = fetch(
            &client,
            &source,
            "000000000000000000000000000000d2",
            "projection",
        )
        .await?;
        ensure!(
            replay["processed"] == 1 && replay["batch"]["records"] == original["batch"]["records"]
        );
        ensure!(
            number(&replay["batch"]["issued_ms"])? >= expiry_cut
                && replay["batch"]["receipt"] != original["batch"]["receipt"]
        );
        let replacement = Receipt::decode(
            replay["batch"]["receipt"]
                .as_str()
                .context("replacement receipt missing")?,
        )?;
        ensure!(
            settle(&client, &source, "projection", "ack", &replacement, 0).await?
                == json!({"status":"Acknowledged"})
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while pending(&client, &source).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("capacity release delivers original pending event")??;
    let released = status(&client, &source).await?;
    ensure!(released["end"] == "3" && released["earliest"] == "2");
    if !required {
        ensure!(released["state"] == "expired" && released["checkpoint"] == "0");
        ensure!(
            fetch(
                &client,
                &source,
                "000000000000000000000000000000d2",
                "projection"
            )
            .await?
                == json!({"error":"CursorExpired"})
        );
        let reset = json!({"kind":"change","change":{"operation":"000000000000000000000000000000d3","expected":released["identity"],
            "action":{"kind":"reset","cursor":{"resource":resource().id,"partition":0,"next":"2"}},"reason":"explicitly skip intentionally expired bounded history"}});
        let result =
            stream_operator(&client, &source, &binding(true), reset, "clock-retention").await?;
        ensure!(
            result["result"]["checkpoint"] == "1"
                && result["result"]["skipped"] == json!({"first":"1","last":"1"})
        );
    }
    let second = fetch(
        &client,
        &source,
        "000000000000000000000000000000d4",
        "projection",
    )
    .await?;
    ensure!(second["processed"] == 3 && second["batch"]["records"][0]["offset"] == "2");
    let current = Receipt::decode(
        second["batch"]["receipt"]
            .as_str()
            .context("second receipt missing")?,
    )?;
    ensure!(
        settle(&client, &source, "projection", "ack", &current, 0).await?
            == json!({"status":"Acknowledged"})
    );
    ensure!(status(&client, &source).await?["checkpoint"] == "2");
    let finished = pending(&client, &source).await?;
    ensure!(
        finished["pending_rows"] == "0"
            && finished["incarnation"] == obligation["incarnation"]
            && finished["lanes"][0]["revision"]["lane"]
                == obligation["lanes"][0]["revision"]["lane"]
            && finished["lanes"][0]["revision"]["next_sequence"] == "3"
            && finished["lanes"][0]["revision"]["settled_through"] == "2"
    );
    stopped(source)
        .await
        .context("complete recovered clock source drain")?;
    observer.shutdown().await;
    ensure!(
        destination_core.handle().spawn(async { 7 }).await? == 7
            && source_core.handle().spawn(async { 7 }).await? == 7
    );
    Ok(())
}
