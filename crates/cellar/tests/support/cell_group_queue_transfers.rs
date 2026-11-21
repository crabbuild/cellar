// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable transfer identity through real delivery, purge and retained restart.

use super::*;
use base64::Engine as _;

async fn submit(cluster: &ManagedCluster, transfers: serde_json::Value) -> Result<(u16, String)> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!(
            "http://{}/transfer",
            cluster.nodes[0].info().public_address
        ))
        .json(&json!([{ "transfers": transfers }]))
        .send()
        .await?;
    Ok((response.status().as_u16(), response.text().await?))
}

async fn accepted(
    cluster: &ManagedCluster,
    transfers: serde_json::Value,
    count: u64,
) -> Result<()> {
    let (status, body) = submit(cluster, transfers).await?;
    ensure!(status == 200, "Queue transfer refused: {status} {body}");
    let metrics: serde_json::Value = serde_json::from_str(&body)?;
    ensure!(
        metrics["backlogCount"].as_f64() == Some(count as f64),
        "transfer metrics: {metrics}"
    );
    Ok(())
}

async fn exercise(s: &mut Scenario) -> Result<()> {
    // Reach the transfer receipt through the ordinary producer and consumer;
    // the consumer retries until its configured dead-letter limit is reached.
    send(&s.cluster, QUEUE, &["deadfault".into()]).await?;
    s.empty(QUEUE).await?;
    let dead = admin(&s.cluster, DEAD, json!({"op":"peek"})).await?;
    let rows = dead["messages"].as_array().context("DLQ inspection rows")?;
    ensure!(
        rows.len() == 1,
        "ordinary delivery did not reach DLQ: {dead}"
    );
    let row = &rows[0];
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(row["bodyBase64"].as_str().context("DLQ bytes")?)?;
    ensure!(bytes == b"deadfault");
    let transfer = json!({
        "transferId": row["provenance"]["transferId"].as_str().context("original transfer")?,
        "deadLetterSource": row["deadLetterSource"],
        "entry": {"id": row["id"], "body": bytes, "contentType": row["contentType"], "size": bytes.len()}
    });
    admin(&s.cluster, DEAD, json!({"op":"purge"})).await?;
    s.empty(DEAD).await?;
    archive_ready(&s.cluster, &[s.cluster.history, s.queue, s.dead]).await?;
    restart(&mut s.cluster).await?;
    ready(&s.cluster, &[s.cluster.history, s.queue, s.dead]).await?;
    // The message row is gone. Only its durable receipt can authorize replay.
    accepted(&s.cluster, json!([transfer.clone()]), 0).await?;
    for (field, value) in [
        ("body", json!([101, 101, 97, 100, 102, 97, 117, 108, 116])),
        ("contentType", json!("bytes")),
        ("id", json!("changed-message")),
    ] {
        let mut collision = transfer.clone();
        collision["entry"][field] = value;
        let (status, body) = submit(&s.cluster, json!([collision])).await?;
        ensure!(
            status == 409 && body.contains("collided"),
            "changed {field} did not return a settled conflict: {status} {body}"
        );
        s.empty(DEAD).await?;
    }
    let mut collision = transfer.clone();
    collision["deadLetterSource"] = json!("another-queue");
    let (status, body) = submit(&s.cluster, json!([collision.clone()])).await?;
    ensure!(
        status == 409 && body.contains("collided"),
        "changed provenance did not return a settled conflict: {body}"
    );

    let mut invalid = transfer.clone();
    invalid["entry"]["size"] = json!(2.5);
    let (status, body) = submit(&s.cluster, json!([invalid])).await?;
    ensure!(
        status == 409 && body.contains("size does not match"),
        "invalid size was not a settled refusal: {status} {body}"
    );
    s.empty(DEAD).await?;

    // A collision later in one batch must roll back both a preceding message
    // and its receipt, allowing that preceding transfer to be submitted again.
    let fresh = json!({"transferId":"fresh-transfer", "deadLetterSource":null,
        "entry":{"id":"fresh-message","body":[1,2,3],"contentType":"bytes","size":3}});
    let mut changed = fresh.clone();
    changed["entry"]["body"] = json!([1, 2, 4]);
    let (status, body) = submit(&s.cluster, json!([fresh.clone(), changed])).await?;
    ensure!(
        status == 409 && body.contains("collided"),
        "in-batch conflict was not settled: {status} {body}"
    );
    s.empty(DEAD).await?;
    let (status, body) = submit(&s.cluster, json!([fresh.clone(), collision])).await?;
    ensure!(
        status == 409 && body.contains("collided"),
        "mixed batch did not refuse: {body}"
    );
    s.empty(DEAD).await?;
    accepted(&s.cluster, json!([fresh.clone(), fresh.clone()]), 1).await?;
    admin(&s.cluster, DEAD, json!({"op":"purge"})).await?;
    accepted(&s.cluster, json!([fresh]), 0).await?;
    accepted(&s.cluster, json!([transfer]), 0).await?;
    Ok(())
}

pub async fn qualify(real: bool, external: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(if real {
            catalog_fault::Store::around(rustfs::store(
                if external {
                    "queue-transfer-axum"
                } else {
                    "queue-transfer-native"
                },
                rooted,
            )?)
        } else {
            catalog_fault::Store::default()
        });
        let mut s = Scenario::start(
            rooted,
            store,
            external,
            5000,
            None,
            2,
            Durability::Quorum.into(),
        )
        .await?;
        let domain = s.cluster.fixtures[0].domain.clone();
        let result = domain.run(exercise(&mut s)).await;
        let retain = !matches!(&result, Ok(Ok(())));
        let cleanup = s.close(retain).await;
        if let Err(error) = &cleanup {
            eprintln!("Queue transfer cleanup: {error:#}");
        }
        result??;
        cleanup?;
        eprintln!(
            "QUEUE_TRANSFER_IDENTITY realRustFS={real} extents={rooted} axum={external}: PASS"
        );
    }
    Ok(())
}
