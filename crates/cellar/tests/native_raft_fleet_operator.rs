// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// These child invocations exercise the CLI process boundary, outside any Node.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use serde_json::Value;
use std::process::Command;

fn command(arguments: &[&str]) -> Result<std::process::Output> {
    Ok(Command::new(env!("CARGO_BIN_EXE_cellar"))
        .arg("raft")
        .args(arguments)
        .env_remove("CELLAR_ADMIN_TOKEN")
        .env_remove("CELLAR_ADMIN_URL")
        .output()?)
}

#[test]
fn fleet_operator_cli_help_and_invalid_input_have_stable_exit_codes() -> Result<()> {
    let help = command(&["fleet", "--help"])?;
    ensure!(help.status.success());
    let help = String::from_utf8(help.stdout)?;
    ensure!(
        help.contains("operation cancel")
            && help.contains("nodes add")
            && help.contains("nodes activate")
            && help.contains("CELLAR_ADMIN_TOKEN")
            && help.contains("6 wait timeout")
    );
    let invalid = command(&["fleet", "rebalance", "--json"])?;
    ensure!(invalid.status.code() == Some(2));
    let error: Value = serde_json::from_slice(&invalid.stdout)?;
    ensure!(
        error["error"]["code"] == "invalid_input" && error["error"]["outcome"] == "not_submitted"
    );
    let unpinned = command(&[
        "fleet",
        "operation",
        "status",
        "01010101010101010101010101010101",
        "--fleet",
        "test",
        "--json",
    ])?;
    ensure!(unpinned.status.code() == Some(2));
    let error: Value = serde_json::from_slice(&unpinned.stdout)?;
    ensure!(error["error"]["message"]
        .as_str()
        .is_some_and(|m| m.contains("--catalog-id")));
    Ok(())
}

#[test]
fn fleet_operator_cli_unavailable_transport_preserves_unknown_outcome() -> Result<()> {
    let root = tempfile::tempdir()?;
    let token = "test-transport-secret-never-returned-by-the-cli";
    let file = root.path().join("credential");
    std::fs::write(&file, token)?;
    let output = command(&[
        "fleet",
        "status",
        "--fleet",
        "test",
        "--admin-url",
        "http://127.0.0.1:0",
        "--token-file",
        file.to_str().unwrap(),
        "--json",
    ])?;
    ensure!(output.status.code() == Some(5));
    let error: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(error["error"]["code"] == "unavailable" && error["error"]["outcome"] == "unknown");
    ensure!(!String::from_utf8(output.stdout)?.contains(token));
    ensure!(!String::from_utf8(output.stderr)?.contains(token));
    Ok(())
}

#[test]
fn fleet_operator_cli_rejects_credential_urls_before_transport() -> Result<()> {
    let root = tempfile::tempdir()?;
    let file = root.path().join("credential");
    std::fs::write(&file, "test-management-secret-token-with-over-32-bytes")?;
    let output = command(&[
        "fleet",
        "status",
        "--fleet",
        "test",
        "--admin-url",
        "http://user:password@127.0.0.1:0",
        "--token-file",
        file.to_str().unwrap(),
        "--json",
    ])?;
    ensure!(output.status.code() == Some(2));
    let error: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(error["error"]["outcome"] == "not_submitted");
    ensure!(!String::from_utf8(output.stdout)?.contains("password"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_operator_node_cli_refuses_mismatched_admission_receipts() -> Result<()> {
    let root = tempfile::tempdir()?;
    let credential = root.path().join("credential");
    let descriptor = root.path().join("node.json");
    let token = "test-node-admission-token-with-at-least-32-bytes";
    std::fs::write(&credential, token)?;
    let node = hex::encode([1; 16]);
    let request = hex::encode([4; 16]);
    let catalog = hex::encode([3; 32]);
    std::fs::write(
        &descriptor,
        serde_json::to_vec(&serde_json::json!({
            "node_id":node,"disk_id":hex::encode([2;16]),"physical_host":hex::encode([5;16]),
            "label":"added-node","server_name":"localhost","capability_digest":catalog,"storage_instance":null,
        }))?,
    )?;
    for variant in 0..4 {
        let mut receipt = serde_json::json!({"reconciled":false,"admission":{
            "catalog_id":catalog,"request_id":request,"principal":"deployer","expected_revision":1,
            "node_id":node,"action":"register","admitted_state":"Registered",
            "applied_revision":2,"observed_revision":2,
        }});
        match variant {
            0 => receipt["admission"]["node_id"] = serde_json::json!(hex::encode([99; 16])),
            1 => receipt["admission"]["request_id"] = serde_json::json!(hex::encode([99; 16])),
            2 => {
                receipt["admission"]["action"] = serde_json::json!("activate");
                receipt["admission"]["admitted_state"] = serde_json::json!("Active");
            }
            _ => receipt["admission"]["expected_revision"] = serde_json::json!(0),
        }
        let router = axum::Router::new().route(
            "/api/v1/fleets/test/raft/nodes",
            axum::routing::post(move || async move { axum::Json(receipt) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        });
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_cellar"))
            .args([
                "raft",
                "fleet",
                "nodes",
                "add",
                "--fleet",
                "test",
                "--admin-url",
                &url,
                "--catalog-id",
                &catalog,
                "--descriptor",
                descriptor.to_str().unwrap(),
                "--operation-id",
                &request,
                "--expected-revision",
                "1",
                "--token-file",
                credential.to_str().unwrap(),
                "--json",
            ])
            .env_remove("CELLAR_ADMIN_TOKEN")
            .env_remove("CELLAR_ADMIN_URL")
            .kill_on_drop(true)
            .output()
            .await?;
        let _ = stop.send(());
        server.await??;
        ensure!(
            output.status.code() == Some(5),
            "variant {variant}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        let error: Value = serde_json::from_slice(&output.stdout)?;
        ensure!(error["error"]["code"] == "unavailable" && error["error"]["outcome"] == "unknown");
        ensure!(
            !String::from_utf8(output.stdout)?.contains(token)
                && !String::from_utf8(output.stderr)?.contains(token)
        );
    }
    Ok(())
}
