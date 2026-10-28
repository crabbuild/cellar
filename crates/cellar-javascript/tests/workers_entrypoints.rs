// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, Options, Source};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};
#[path = "support/joined_node.rs"]
mod joined_node;
#[path = "support/native.rs"]
mod native;

#[test]
fn worker_entrypoints_use_native_routes_props_and_stateless_instances() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/cloudflare-v1");
    let build = std::process::Command::new("node")
        .arg(root.join("build.mjs"))
        .output()?;
    ensure!(
        build.status.success(),
        "gateway fixture build: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    joined_node::run(
        Options {
            max_body_bytes: 4096,
            ..Default::default()
        },
        NodeOptions::new(root.join("dist/gateways.mjs")),
        Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch, EventKind::WorkerRpc])
            .cell_events("Counter", [EventKind::Fetch, EventKind::CellRpc]),
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(exercise(application))
        },
    )
}

async fn exercise(application: Arc<dyn Application>) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let (node, address) = native::start_configured(directory.path(), application, |_, _, graph| {
        graph.primary.services = vec![(
            "SELF".into(),
            "javascript-test".into(),
            Some("Greeter".into()),
        )];
    })
    .await?;
    let result = check_routes(&address).await;
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(matches!(outcome, NodeOutcome::Stopped(report)
        if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
    result
}

async fn get_json(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}/{route}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{route}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn check_routes(address: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    for route in ["fixed-response", "fixed-request"] {
        let response = client
            .get(format!("{address}/{route}"))
            .send()
            .await?
            .error_for_status()?;
        if route == "fixed-response" {
            // External ingress owns framing. The guest head's inferred length
            // must not bypass the native streaming response policy.
            ensure!(response.headers().get("content-length").is_none());
            ensure!(response
                .headers()
                .get("transfer-encoding")
                .is_some_and(|value| value == "chunked"));
        } else {
            ensure!(
                response
                    .headers()
                    .get("x-input-length")
                    .is_some_and(|value| value == "3"),
                "{route}: {:?}",
                response.headers()
            );
        }
        ensure!(response.bytes().await?.as_ref() == [0, 255, 42], "{route}");
    }
    let value = get_json(&client, address, "loopback").await?;
    ensure!(
        value["greeting"] == "captured:javascript-test:worker" && value["prefix"] == "captured"
    );
    ensure!(
        value["bytes"] == json!([0, 255]) && value["sum"] == 7 && value["counts"] == json!([1, 1])
    );
    ensure!(
        value["error"]
            == json!({"name":"TypeError","message":"gateway failure","code":"VisibleCode"}),
        "loopback result: {value}"
    );
    ensure!(value["refused"] == json!(["ownSecret", "ctx", "env"]));

    for (route, expected) in [
        (
            "service",
            json!({"greeting":"hello:javascript-test:service","refused":true}),
        ),
        ("default", json!({"label":"class-default","rootProps":null})),
        ("plain", json!({"plain":true})),
    ] {
        ensure!(
            get_json(&client, address, route).await? == expected,
            "{route}"
        );
    }
    let response = client
        .get(format!("{address}/background-start"))
        .send()
        .await?
        .error_for_status()?;
    ensure!(response.text().await? == "retained");
    ensure!(
        get_json(&client, address, "background-finish").await?
            == json!({"marker":"javascript-test"})
    );

    let value = get_json(&client, address, "invalid").await?;
    ensure!(
        value["refused"] == 7 && value["effects"] == 0,
        "invalid service admission: {value}"
    );
    let value = get_json(&client, address, "unrepresentable").await?;
    ensure!(
        value == json!({"effects":1,"outcome":"unknown"}),
        "lost Worker result: {value}"
    );
    let value = get_json(&client, address, "unsupported-values").await?;
    let expected = json!({
        "argumentsRefused": 5,
        "propsRefused": 5,
        "inputEffects": 0,
        "outputEffects": 4,
        "outcomes": ["unknown", "unknown", "unknown", "unknown"],
        "error": {"name":"TypeError", "message":"cloned error", "cycle":true}
    });
    ensure!(value == expected, "unsupported value transfer: {value}");

    let response = client
        .post(format!("{address}/fetch"))
        .body(vec![0, 255, 42])
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response.headers()["x-prefix"] == "fetch"
            && response.bytes().await?.as_ref() == [0, 255, 42]
    );
    ensure!(
        client
            .get(format!("{address}/save"))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?
            == "saved"
    );
    ensure!(
        get_json(&client, address, "borrow").await?
            == json!({"code":"ContextMismatch","outcome":"not_committed"})
    );
    ensure!(
        get_json(&client, address, "object").await?
            == json!({"result":"cell:javascript-test:object","named":"other"})
    );

    let value = get_json(&client, address, "gateways").await?;
    ensure!(value["backup"] == 403 && value["mount"] == 403);
    ensure!(value["backupText"] == "no directory backup operation holds a grant");
    ensure!(value["control"] == "DirectoryBackupGateway control methods require control props");
    Ok(())
}
