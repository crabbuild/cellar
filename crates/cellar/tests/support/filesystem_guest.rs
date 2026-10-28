// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};

#[path = "filesystem_lifetime.rs"]
mod lifetime;
pub(crate) use lifetime::observe;

// This client-side harness runs outside the native node's execution domain.
#[allow(clippy::disallowed_methods)]
pub(crate) async fn initial(address: &str, cancellation: &lifetime::Cancellation) -> Result<()> {
    let client = reqwest::Client::new();
    for path in [
        "/stateless-fs",
        "/one/exercise",
        "/one/replay",
        "/one/expired",
        "/one/metadata",
        "/one/arguments",
        "/one/wire-options",
        "/one/wire-unsigned-0",
        "/one/wire-unsigned-1",
        "/one/wire-unsigned-2",
        "/one/wire-signed",
        "/one/wire-batch",
        "/one/namespace",
        "/one/reserved-mkdir",
        "/one/reserved-rmdir",
        "/one/reserved-unlink",
        "/one/reserved-at",
        "/one/host-admission",
        "/one/authority-capture",
        "/one/authority-closed",
        "/one/authority-expired",
        "/one/authority-file",
        "/authority-owner/authority-live",
        "/two/isolated",
    ] {
        let reply = client.get(format!("{address}{path}")).send().await?;
        let status = reply.status();
        let text = reply.text().await?;
        ensure!(
            status.is_success() && text == "ok",
            "filesystem {path}: {status} {text}"
        );
    }
    let cancelled = client
        .get(format!("{address}/lifetime/cancel-start"))
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await?;
    ensure!(
        cancelled.status().is_server_error(),
        "deadline-held filesystem invocation returned success"
    );
    cancellation.wait().await?;
    let reply = client
        .get(format!("{address}/lifetime/cancel-check"))
        .send()
        .await?;
    let status = reply.status();
    let text = reply.text().await?;
    ensure!(
        status.is_success() && text == "ok",
        "filesystem cancellation cleanup: {status} {text}"
    );
    let reply = client.get(format!("{address}/one/gc-start")).send().await?;
    ensure!(
        reply.status().is_success(),
        "filesystem GC setup failed: {}",
        reply.text().await?
    );
    let mut last = String::new();
    for _ in 0..40 {
        let reply = client.get(format!("{address}/one/gc-check")).send().await?;
        let status = reply.status();
        let body = reply.text().await?;
        ensure!(
            status.is_success(),
            "filesystem GC check failed: {status} {body}"
        );
        if body == "done" {
            return Ok(());
        }
        last = body;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("durable filesystem maintenance did not reclaim the orphan: {last}")
}

pub async fn recovered(address: &str) -> Result<()> {
    for path in ["/one/recovered", "/lifetime/cancel-recovered"] {
        let reply = reqwest::get(format!("{address}{path}")).await?;
        let status = reply.status();
        let text = reply.text().await?;
        ensure!(
            status.is_success() && text == "ok",
            "filesystem recovery {path}: {status} {text}"
        );
    }
    Ok(())
}

#[allow(dead_code)]
pub async fn tools(address: &str) -> Result<()> {
    let reply = reqwest::get(format!("{address}/one/tools")).await?;
    let status = reply.status();
    let text = reply.text().await?;
    ensure!(
        status.is_success() && text == "ok",
        "filesystem tools: {status} {text}"
    );
    Ok(())
}

#[allow(dead_code)]
pub async fn tools_recovered(address: &str) -> Result<()> {
    let reply = reqwest::get(format!("{address}/one/tools-recovered")).await?;
    let status = reply.status();
    let text = reply.text().await?;
    ensure!(
        status.is_success() && text == "ok",
        "filesystem tool recovery: {status} {text}"
    );
    Ok(())
}

#[allow(dead_code)]
pub async fn tools_budget(address: &str, recovered: bool) -> Result<()> {
    let path = if recovered {
        "tools-budget-recovered"
    } else {
        "tools-budget"
    };
    let reply = reqwest::get(format!("{address}/one/{path}")).await?;
    let status = reply.status();
    let text = reply.text().await?;
    ensure!(
        status.is_success() && text == "ok",
        "filesystem tool budget: {status} {text}"
    );
    Ok(())
}

#[allow(dead_code, clippy::disallowed_methods)]
async fn session_status(address: &str, turn: &str, wanted: &str) -> Result<serde_json::Value> {
    for _ in 0..80 {
        let reply = reqwest::get(format!("{address}/sessions/alpha/turns/{turn}")).await?;
        let status = reply.status();
        let body = reply.text().await?;
        ensure!(status.is_success(), "agent status failed: {status} {body}");
        let value: serde_json::Value = serde_json::from_str(&body)?;
        if value["status"] == wanted {
            return Ok(value);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("agent did not reach {wanted}")
}

#[allow(dead_code)]
pub async fn session_initial(address: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let reply = client
        .post(format!("{address}/sessions/alpha/turns/one"))
        .header("content-type", "application/json")
        .body(serde_json::json!({"prompt":"pause-after-write"}).to_string())
        .send()
        .await?;
    let status = reply.status();
    let body = reply.text().await?;
    ensure!(
        status.is_success(),
        "agent acceptance failed: {status} {body}"
    );
    let run = session_status(address, "one", "interrupted").await?;
    ensure!(
        run["step"] == 1 && run["phase"] == "model",
        "agent lost completed tool boundary: {run}"
    );
    let blocked = client
        .post(format!("{address}/sessions/alpha/turns/other"))
        .header("content-type", "application/json")
        .body(serde_json::json!({"prompt":"must wait"}).to_string())
        .send()
        .await?;
    ensure!(
        blocked.status() == reqwest::StatusCode::CONFLICT,
        "concurrent turn entered the same workspace"
    );
    let reply = reqwest::get(format!("{address}/sessions/alpha/snapshot")).await?;
    let body = reply.text().await?;
    let value: serde_json::Value = serde_json::from_str(&body)?;
    ensure!(
        value["text"] == "pause-after-write" && value["model_calls"]["0"] == 1,
        "agent failed to prove its saved decision/effect: {value}"
    );
    Ok(value["version"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing agent file version"))?
        .to_owned())
}

#[allow(dead_code)]
pub async fn session_recovered(address: &str, version: &str) -> Result<()> {
    let client = reqwest::Client::new();
    ensure!(
        client
            .get(format!("{address}/release-agent"))
            .send()
            .await?
            .status()
            .is_success(),
        "model recovery release failed"
    );
    let reply = client
        .post(format!("{address}/sessions/alpha/turns/one"))
        .header("content-type", "application/json")
        .body(serde_json::json!({"prompt":"pause-after-write","resume":true}).to_string())
        .send()
        .await?;
    let status = reply.status();
    let body = reply.text().await?;
    ensure!(status.is_success(), "agent resume failed: {status} {body}");
    session_status(address, "one", "completed").await?;
    let reply = reqwest::get(format!("{address}/sessions/alpha/snapshot")).await?;
    let body = reply.text().await?;
    let value: serde_json::Value = serde_json::from_str(&body)?;
    ensure!(
        value["text"] == "pause-after-write"
            && value["version"] == version
            && value["model_calls"]["0"] == 1,
        "recovery repeated a saved model decision or file mutation: {value}"
    );
    let reply = client
        .post(format!("{address}/sessions/alpha/turns/followup"))
        .header("content-type", "application/json")
        .body(serde_json::json!({"prompt":"read:/notes.txt"}).to_string())
        .send()
        .await?;
    ensure!(
        reply.status().is_success(),
        "follow-up turn was refused after completion"
    );
    let run = session_status(address, "followup", "completed").await?;
    ensure!(
        run["text"] == "pause-after-write",
        "follow-up lost the session workspace: {run}"
    );
    Ok(())
}
