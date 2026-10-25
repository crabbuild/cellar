// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::matrix::request;
use anyhow::{ensure, Context as _, Result};
use cellar_python::PythonApplication;
use serde_json::{json, Value};
use std::time::Duration;

pub(super) async fn control(
    client: &reqwest::Client,
    url: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    request(client, url, "python", "python-lifecycle-control", query).await
}

pub(super) async fn state(client: &reqwest::Client, url: &str, probes: u64) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let value = control(client, url, &[("action", "state")]).await?;
            ensure!(
                value["finished"] == false,
                "suppression task finished before its release"
            );
            if value["cancellations"]
                .as_u64()
                .is_some_and(|count| count > 0)
                && value["probes"]
                    .as_u64()
                    .is_some_and(|count| count >= probes)
            {
                return Ok::<_, anyhow::Error>(value);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("Python task did not suppress native cancellation")?
}

pub(super) async fn probe(client: &reqwest::Client, url: &str, count: u64) -> Result<Value> {
    control(client, url, &[("action", "probe")]).await?;
    let value = state(client, url, count).await?;
    for operation in ["write", "read", "rpc"] {
        ensure!(
            value["refusals"][operation]
                == json!({"code":"ContextExpired","outcome":"not_committed"}),
            "late Python {operation} acquired authority: {value}"
        );
    }
    ensure!(
        value["refusals"]["raw"] == "ContextExpired",
        "late raw Python bridge acquired authority: {value}"
    );
    Ok(value)
}

pub(super) async fn resources(
    python: &PythonApplication,
    native: usize,
    guest: usize,
) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let bridge = python.bridge_report();
            let snapshot = python.interpreter_snapshot();
            last = json!({"bridge":bridge,"snapshot":snapshot});
            if bridge.native_invocations == native
                && bridge.queued_bytes == 0
                && bridge.pending_reservations == 0
                && bridge.operations.settled()
                && snapshot
                    .as_ref()
                    .is_some_and(|sample| sample.work.active_invocations == guest)
            {
                return Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| {
        format!("Python resource baseline differs: expected native={native}, guest={guest}; {last}")
    })?
}

pub(super) fn cancelled(value: &Value, caller: &str) -> Result<()> {
    let expected = if caller == "python" {
        json!({"cancelled":true})
    } else {
        json!({"code":"Cancelled","outcome":"unknown"})
    };
    ensure!(
        value["result"] == expected && value["followup_writes"] == 0,
        "caller cancellation lost its outcome or original authority: {value}"
    );
    Ok(())
}
