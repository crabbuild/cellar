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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_operator_maintenance_prepare_wait_refuses_cancelled_as_ready() -> Result<()> {
    maintenance_wait(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_operator_maintenance_prepare_wait_observes_ready_before_deadline() -> Result<()> {
    maintenance_wait(false).await
}

async fn maintenance_wait(cancelled: bool) -> Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let root = tempfile::tempdir()?;
    let credential = root.path().join("credential");
    let token = "fixture-maintenance-cancel-wait-token-with-at-least-32-bytes";
    std::fs::write(&credential, token)?;
    let node = hex::encode([1; 16]);
    let request = hex::encode([2; 16]);
    let operation = hex::encode([3; 16]);
    let catalog = hex::encode([4; 32]);
    let process = hex::encode([5; 16]);
    let admission = serde_json::json!({"catalog_id":catalog,"node_id":node,
        "operation_id":operation,"request_id":request,"expected_revision":1,
        "applied_revision":2,"observed_revision":2,"reconciled":false});
    let status = serde_json::json!({"catalog_id":catalog,"node_id":node,
        "operation_id":operation,"request_id":request,"expected_revision":1,
        "applied_revision":2,"observed_revision":4,"principal":"fixture",
        "process":process,"boot":hex::encode([6;16]),"enrollment_epoch":1,
        "grace_seconds":120,"birth_resolution":"cancel","phase":"cancelled","cancellation_refused":false,
        "execution_operation":null,"readiness_request":null,
        "cancellation_request":hex::encode([9;16]),"cancelled_revision":4});
    let mut status = status;
    if !cancelled {
        status["phase"] = serde_json::json!("verifying");
        status["cancellation_request"] = Value::Null;
        status["cancelled_revision"] = Value::Null;
        status["execution_operation"] = serde_json::json!(hex::encode([8; 16]));
    }
    let started = Arc::new(std::sync::OnceLock::new());
    let post_started = started.clone();
    let get_started = started.clone();
    let posts = Arc::new(AtomicUsize::new(0));
    let gets = Arc::new(AtomicUsize::new(0));
    let post_count = posts.clone();
    let get_count = gets.clone();
    let router = axum::Router::new()
        .route(
            &format!("/api/v1/fleets/test/raft/nodes/{node}/prepare-maintenance"),
            axum::routing::post(move || {
                post_count.fetch_add(1, Ordering::SeqCst);
                let _ = post_started.set(std::time::Instant::now());
                let value = admission.clone();
                async move { (axum::http::StatusCode::ACCEPTED, axum::Json(value)) }
            }),
        )
        .route(
            &format!("/api/v1/fleets/test/raft/maintenance/{operation}"),
            axum::routing::get(move || {
                get_count.fetch_add(1, Ordering::SeqCst);
                let mut value = status.clone();
                // Backend readiness advances with time independently of GET.
                if !cancelled
                    && get_started
                        .get()
                        .is_some_and(|at| at.elapsed() >= std::time::Duration::from_millis(850))
                {
                    value["phase"] = serde_json::json!("ready");
                    value["readiness_request"] = serde_json::json!(hex::encode([7; 16]));
                }
                async move { axum::Json(value) }
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
    let seconds = if cancelled { "2" } else { "1" };
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_cellar"))
            .args([
                "raft",
                "fleet",
                "nodes",
                "prepare-maintenance",
                &node,
                "--operation-id",
                &operation,
                "--request-id",
                &request,
                "--expected-revision",
                "1",
                "--expected-process",
                &process,
                "--grace-seconds",
                "120",
                "--birth-resolution",
                "cancel",
                "--fleet",
                "test",
                "--catalog-id",
                &catalog,
                "--admin-url",
                &url,
                "--token-file",
                credential
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("fixture token path"))?,
                "--wait",
                "--timeout-seconds",
                seconds,
                "--json",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await;
    let _ = stop.send(());
    server.await??;
    let output = output??;
    let value: Value = serde_json::from_slice(&output.stdout)?;
    if cancelled {
        ensure!(
            output.status.code() == Some(4) && value["error"]["code"] == "blocked",
            "cancelled preparation: {value}"
        );
        ensure!(gets.load(Ordering::SeqCst) == 1);
    } else {
        ensure!(
            output.status.success() && value["phase"] == "ready",
            "late readiness: {value}"
        );
        ensure!(gets.load(Ordering::SeqCst) > 1 && gets.load(Ordering::SeqCst) <= 16);
    }
    ensure!(posts.load(Ordering::SeqCst) == 1);
    ensure!(
        !String::from_utf8(output.stdout)?.contains(token)
            && !String::from_utf8(output.stderr)?.contains(token)
    );
    Ok(())
}

async fn waiting_cli(unknown: bool) -> Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let root = tempfile::tempdir()?;
    let credential = root.path().join("credential");
    let token = "proof-wait-token-with-at-least-32-bytes";
    std::fs::write(&credential, token)?;
    let node = hex::encode([1; 16]);
    let request = hex::encode([2; 16]);
    let operation = hex::encode([3; 16]);
    let catalog = hex::encode([4; 32]);
    let waiting = serde_json::json!({"progress":{
        "catalog_id":catalog,"operation_id":operation,"request_id":request,"node_id":node,
        "expected_revision":1,"observed_revision":1,"phase":"verifying_before_stop",
        "checked_histories":0,"total_histories":1,"applied_revision":null,"candidate_digest":null,
        "closure_digest":null,"process":null,"stop_id":null,"fence_id":null,"terminal":false,
        "blocker":{"stage":"history_quorum","history":hex::encode([7;32]),"reason":"unavailable"}
    },"reconciled":false});
    let mut complete = waiting.clone();
    complete["progress"]["blocker"] = Value::Null;
    complete["progress"]["phase"] = serde_json::json!("finalizing");
    complete["progress"]["checked_histories"] = serde_json::json!(1);
    complete["progress"]["applied_revision"] = serde_json::json!(2);
    complete["progress"]["observed_revision"] = serde_json::json!(2);
    complete["progress"]["candidate_digest"] = serde_json::json!(hex::encode([6; 32]));
    complete["progress"]["process"] = serde_json::json!(hex::encode([5; 16]));
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let router=axum::Router::new().route(&format!("/api/v1/fleets/test/raft/nodes/{node}/prepare-removal"),axum::routing::post(move || {
        let n=calls.fetch_add(1,Ordering::SeqCst);
        let value=if n==0 {waiting.clone()} else if unknown {
            serde_json::json!({"error":{"code":"unavailable","outcome":"unknown","message":"ambiguous fence"},"phase":null,"terminal":null})
        } else {complete.clone()};
        async move {(if n>0 && unknown {axum::http::StatusCode::SERVICE_UNAVAILABLE} else {axum::http::StatusCode::OK},axum::Json(value))}
    }));
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
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_cellar"))
            .args([
                "raft",
                "fleet",
                "nodes",
                "prepare-removal",
                &node,
                "--operation-id",
                &operation,
                "--request-id",
                &request,
                "--expected-revision",
                "1",
                "--fleet",
                "test",
                "--admin-url",
                &url,
                "--catalog-id",
                &catalog,
                "--token-file",
                credential.to_str().unwrap(),
                "--wait",
                "--timeout-seconds",
                "2",
                "--json",
            ])
            .kill_on_drop(true)
            .output(),
    )
    .await;
    let _ = stop.send(());
    server.await??;
    let output = output??;
    let value: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(
        count.load(Ordering::SeqCst) == 2,
        "unexpected proof/fence retries: {value}"
    );
    if unknown {
        ensure!(output.status.code() == Some(5) && value["error"]["outcome"] == "unknown");
    } else {
        ensure!(
            output.status.success()
                && value["progress"]["phase"] == "finalizing"
                && value["progress"]["blocker"].is_null()
        );
    }
    ensure!(
        !String::from_utf8(output.stdout)?.contains(token)
            && !String::from_utf8(output.stderr)?.contains(token)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_operator_proof_wait_cli_advances_only_confirmed_progress() -> Result<()> {
    waiting_cli(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_operator_proof_wait_cli_stops_after_unknown_fence() -> Result<()> {
    waiting_cli(true).await
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
            "closure_digest":null,"process":process,"stop_id":null,"terminal":false,"blocker":null
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
