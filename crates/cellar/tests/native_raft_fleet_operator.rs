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
