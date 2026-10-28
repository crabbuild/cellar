// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Verify joined image qualification, then remove only empty owned networks.

use anyhow::{ensure, Context as _, Result};
use serde_json::json;

pub async fn cleanup(docker: &cellar::docker::Docker, host: &str) -> Result<()> {
    let filters = serde_json::to_string(&json!({"label":[format!("cellar.host={host}")]}))?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("all", "true")
        .append_pair("filters", &filters)
        .finish();
    let reply = docker
        .call("GET", &format!("/containers/json?{query}"), None)
        .await?;
    ensure!(
        reply.status.is_success() && reply.json()?.as_array().is_some_and(Vec::is_empty),
        "Sandbox 1.x leaked owned containers"
    );
    let reply = docker
        .call("GET", &format!("/networks?{query}"), None)
        .await?;
    ensure!(reply.status.is_success(), "list owned networks");
    for network in reply.json()?.as_array().context("network list")? {
        let id = network["Id"].as_str().context("network ID")?;
        ensure!(
            id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid network ID"
        );
        let inspected = docker.call("GET", &format!("/networks/{id}"), None).await?;
        let inspected = inspected.json()?;
        ensure!(
            inspected["Labels"]["cellar.host"] == host
                && inspected["Driver"] == "bridge"
                && inspected["Containers"]
                    .as_object()
                    .is_some_and(|v| v.is_empty()),
            "network ownership or attachment mismatch"
        );
        ensure!(
            docker
                .call("DELETE", &format!("/networks/{id}"), None)
                .await?
                .status
                .is_success(),
            "remove owned network"
        );
    }
    Ok(())
}
