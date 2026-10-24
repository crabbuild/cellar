// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Force a real published node-lease mismatch, then join its fenced domain.
use anyhow::{ensure, Context, Result};
use cellar::{
    bucket::Bucket,
    sdk::{ExecutionDomain, FenceReason, Node, NodeOutcome},
};
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn owner(bucket: &Bucket, node: &mut Node) -> Result<Value> {
    let name = node.info().node.clone();
    // This operator fault owns a separate execution domain. Lease I/O remains
    // independent of the proof gates; changing the address retains the original
    // generation and log-session provenance for actual survivor recovery.
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let replaced = domain
        .run(async {
            let key = format!("nodes/{name}.json");
            let (bytes, _) = bucket
                .get(&key)
                .await?
                .context("owner lease was not published")?;
            let mut lease: Value = serde_json::from_slice(&bytes)?;
            lease["addr"] = json!("http://127.0.0.1:1");
            bucket.put(&key, serde_json::to_vec(&lease)?).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    replaced??;
    let outcome = tokio::time::timeout(Duration::from_secs(10), node.finished())
        .await
        .context("owner did not fence after its lease identity changed")??;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseMismatch
            },
        "owner stopped without the required observed fence: {outcome:?}"
    );
    // Node::finished returns only after the domain and its owned work join.
    Ok(json!({"code":3,"reason":"NodeLeaseMismatch","domain_joined":true}))
}

pub(super) async fn refuses_new_request(
    client: &reqwest::Client,
    url: &str,
    provider: &str,
) -> Result<()> {
    // Issue this mutation once, after fencing. A timeout is not proof of joined
    // admission: the old listener must close or explicitly refuse the request.
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client
            .get(format!(
                "{url}/rust/increment?target={provider}&delta=1&id=fenced-reentry"
            ))
            .send(),
    )
    .await
    .context("fenced listener retained an unanswered new request")?;
    if let Ok(response) = response {
        ensure!(
            response.status().is_server_error(),
            "fenced node admitted a new request: {}",
            response.status()
        );
    }
    Ok(())
}
