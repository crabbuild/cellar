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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_operator_departure_cli_checks_receipts_and_never_retries_unknown() -> Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let root = tempfile::tempdir()?;
    let credential = root.path().join("credential");
    let token = "test-departure-token-with-at-least-32-bytes";
    std::fs::write(&credential, token)?;
    let node = hex::encode([1; 16]);
    let request = hex::encode([2; 16]);
    let operation = hex::encode([3; 16]);
    let catalog = hex::encode([4; 32]);
    let process = hex::encode([5; 16]);
    for variant in 0..13 {
        let mut receipt = serde_json::json!({"progress": {
            "catalog_id":catalog,"operation_id":operation,"request_id":request,"node_id":node,
            "expected_revision":1,"phase":"finalizing","checked_histories":1,"total_histories":1,
            "applied_revision":2,"observed_revision":2,"candidate_digest":hex::encode([6;32]),
            "closure_digest":null,"process":process,"stop_id":null,"terminal":false
        },"reconciled":false});
        let verb = match variant {
            8 => "stop",
            9 => "remove",
            10..=12 => "cancel-evacuation",
            _ => "prepare-removal",
        };
        match variant {
            0 => receipt["progress"]["node_id"] = serde_json::json!(hex::encode([9; 16])),
            1 => receipt["progress"]["operation_id"] = serde_json::json!(hex::encode([9; 16])),
            2 => receipt["progress"]["request_id"] = serde_json::json!(hex::encode([9; 16])),
            3 => receipt["progress"]["candidate_digest"] = Value::Null,
            4 => receipt["progress"]["process"] = Value::Null,
            5 => receipt["progress"]["checked_histories"] = serde_json::json!(0),
            6 => {
                receipt["progress"]["phase"] = serde_json::json!("removed");
                receipt["progress"]["terminal"] = serde_json::json!(true);
                receipt["progress"]["closure_digest"] = serde_json::json!(hex::encode([7; 32]));
                receipt["progress"]["stop_id"] = serde_json::json!(request);
            }
            7 => {
                receipt = serde_json::json!({"error":{"code":"unavailable","outcome":"unknown","message":"ambiguous fence"},"phase":null,"terminal":null})
            }
            8 | 9 => {
                receipt["progress"]["phase"] = serde_json::json!(if variant == 8 {
                    "stop_requested"
                } else {
                    "verifying_removal"
                });
                receipt["progress"]["applied_revision"] = Value::Null;
                receipt["progress"]["candidate_digest"] = Value::Null;
                receipt["progress"]["observed_revision"] = serde_json::json!(1);
                receipt["progress"]["stop_id"] = serde_json::json!(if variant == 8 {
                    request.clone()
                } else {
                    hex::encode([9; 16])
                });
                receipt["progress"]["process"] = if variant == 8 {
                    serde_json::json!(hex::encode([9; 16]))
                } else {
                    Value::Null
                };
            }
            10..=12 => {
                receipt["progress"]["phase"] = serde_json::json!("cancelled");
                receipt["progress"]["terminal"] = serde_json::json!(variant != 11);
                receipt["progress"]["checked_histories"] = serde_json::json!(0);
                receipt["progress"]["candidate_digest"] = Value::Null;
                receipt["progress"]["fence_id"] = serde_json::json!(if variant == 10 {
                    hex::encode([9; 16])
                } else {
                    request.clone()
                });
                if variant == 12 {
                    receipt["progress"]["process"] = serde_json::json!(hex::encode([9; 16]));
                }
            }
            _ => anyhow::bail!("unexpected receipt variant"),
        }
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let router = axum::Router::new().route(
            &format!("/api/v1/fleets/test/raft/nodes/{node}/{verb}"),
            axum::routing::post(move || {
                let receipt = receipt.clone();
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    (
                        if variant == 7 {
                            axum::http::StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            axum::http::StatusCode::OK
                        },
                        axum::Json(receipt),
                    )
                }
            }),
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
        let mut args = vec![
            "raft",
            "fleet",
            "nodes",
            verb,
            &node,
            "--fleet",
            "test",
            "--admin-url",
            &url,
            "--catalog-id",
            &catalog,
            "--operation-id",
            &operation,
            "--request-id",
            &request,
            "--expected-revision",
            "1",
            "--token-file",
            credential.to_str().unwrap(),
            "--json",
        ];
        if variant == 8 {
            args.extend(["--expected-process", &process]);
        } else if variant >= 10 {
            args.extend(["--expected-process", &process, "--fence-id", &request]);
        } else {
            args.push("--wait");
        }
        if variant == 9 {
            args.extend(["--stop-id", &request]);
        }
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_cellar"))
            .args(args)
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
        ensure!(
            count.load(Ordering::SeqCst) == 1,
            "mismatch or unknown response must never be retried"
        );
        let error: Value = serde_json::from_slice(&output.stdout)?;
        ensure!(error["error"]["code"] == "unavailable" && error["error"]["outcome"] == "unknown");
        ensure!(
            !String::from_utf8_lossy(&output.stdout).contains(token)
                && !String::from_utf8_lossy(&output.stderr).contains(token)
        );
    }
    Ok(())
}
