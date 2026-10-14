//! The shipped CLI exposes the Rust delivery workflow.
use anyhow::{ensure, Result};
use std::{io::Write, process::Command};

#[test]
fn default_cli_builds_rust_and_retains_fleet_operator_commands() -> Result<()> {
    let executable = env!("CARGO_BIN_EXE_cellar");
    {
        let help = Command::new(executable).arg("--help").output()?;
        ensure!(help.status.success(), "help failed: {:?}", help);
        let help = String::from_utf8(help.stdout)?;
        ensure!(help.contains("cellar build PROJECT.json"));
        ensure!(help.contains("outbox|stream|subscription|blob"));
        ensure!(help.contains("Run a compiled application executable"));
        let version = Command::new(executable).arg("--version").output()?;
        ensure!(version.status.success());
        ensure!(
            String::from_utf8(version.stdout)?.trim()
                == concat!("cellar ", env!("CARGO_PKG_VERSION"))
        );
        let sql = Command::new(executable).args(["sql", "--help"]).output()?;
        ensure!(sql.status.success(), "SQL operator help failed: {:?}", sql);
        ensure!(String::from_utf8(sql.stdout)?.contains("cellar sql"));
        let outbox = Command::new(executable)
            .args(["outbox", "--help"])
            .output()?;
        ensure!(outbox.status.success());
        let help = String::from_utf8(outbox.stdout)?;
        ensure!(
            help.contains("cellar outbox retry SCOPE") && help.contains("--expected REVISION.json")
        );
        let invalid = Command::new(executable)
            .args(["outbox", "inspect", "Account:source", "--limit", "101"])
            .output()?;
        ensure!(!invalid.status.success());
        ensure!(
            String::from_utf8(invalid.stderr)?.contains("invalid source diagnostic page bounds")
        );
        let retry = Command::new(executable)
            .args(["outbox", "retry", "Account:source"])
            .output()?;
        ensure!(!retry.status.success());
        ensure!(String::from_utf8(retry.stderr)?.contains("requires --operation"));
        for family in ["stream", "subscription"] {
            let output = Command::new(executable).args([family, "--help"]).output()?;
            ensure!(output.status.success(), "{family} help failed: {output:?}");
            let help = String::from_utf8(output.stdout)?;
            ensure!(
                help.contains("--binding BINDING.json")
                    && help.contains("--confirm-reset")
                    && help.contains("--payload")
                    && help.contains("cellar stream sources")
                    && help.contains("cellar stream list")
            );
        }
        for action in ["reset", "delete"] {
            let output = Command::new(executable)
                .args(["subscription", action, "projection"])
                .output()?;
            ensure!(!output.status.success());
            ensure!(String::from_utf8(output.stderr)?
                .contains(&format!("{action} requires --confirm-{action}")));
        }
        let retirement = Command::new(executable)
            .args(["stream", "retire"])
            .output()?;
        ensure!(!retirement.status.success());
        ensure!(String::from_utf8(retirement.stderr)?.contains("retire requires --confirm-retire"));
        let forced = Command::new(executable)
            .args(["stream", "retire", "--confirm-retire", "--force"])
            .output()?;
        ensure!(!forced.status.success());
        ensure!(String::from_utf8(forced.stderr)?
            .contains("forced retirement requires --data-loss TEXT"));
        let coordinated = Command::new(executable)
            .args(["stream", "retire", "--confirm-retire"])
            .output()?;
        ensure!(!coordinated.status.success());
        ensure!(String::from_utf8(coordinated.stderr)?
            .contains("coordinated retirement requires --procedure TEXT"));
    }
    Ok(())
}

#[test]
fn stream_operator_cli_refuses_invalid_inputs_before_fleet_access() -> Result<()> {
    let executable = env!("CARGO_BIN_EXE_cellar");
    for (flag, value, message) in [
        ("--limit", "101", "invalid catalog list bounds"),
        ("--limit", "0", "invalid catalog list bounds"),
        ("--cursor", "subs1.%%%", "InvalidCursor"),
        ("--payload", "true", "invalid catalog list option"),
    ] {
        let output = Command::new(executable)
            .args([
                "stream",
                "list",
                "--tenant",
                "tenant",
                "--application",
                "app",
                flag,
                value,
            ])
            .output()?;
        ensure!(!output.status.success());
        ensure!(String::from_utf8(output.stderr)?.contains(message));
    }
    let directory = tempfile::tempdir()?;
    let mut binding = tempfile::NamedTempFile::new_in(directory.path())?;
    binding.write_all(&serde_json::to_vec(&serde_json::json!({
            "environment":"EVENTS","resource":{"tenant":"tenant","application":"app","name":"events","id":"00000000000000000000000000000001","family":"events"},
            "principal":"app","grant":"reader","read":true,"administer":false
        }))?)?;
    for (flag, value, message) in [
        ("--limit", "101", "invalid stream read bounds"),
        ("--max-bytes", "262145", "invalid stream read bounds"),
        ("--cursor", "v1.%%%", "InvalidCursor"),
    ] {
        let output = Command::new(executable)
            .args(["stream", "read", "--binding"])
            .arg(binding.path())
            .args([flag, value])
            .output()?;
        ensure!(!output.status.success());
        ensure!(String::from_utf8(output.stderr)?.contains(message));
    }
    let output = Command::new(executable)
        .args(["stream", "describe", "--binding"])
        .arg(binding.path())
        .output()?;
    ensure!(!output.status.success());
    ensure!(
        String::from_utf8(output.stderr)?.contains("binding does not grant this operator action")
    );
    for (flag, value, message) in [
        ("--limit", "101", "invalid subscription list bounds"),
        ("--limit", "0", "invalid subscription list bounds"),
        ("--cursor", "subs1.%%%", "InvalidCursor"),
    ] {
        let output = Command::new(executable)
            .args(["subscription", "list", "--binding"])
            .arg(binding.path())
            .args([flag, value])
            .output()?;
        ensure!(!output.status.success());
        ensure!(String::from_utf8(output.stderr)?.contains(message));
    }
    for (flag, value, message) in [
        ("--limit", "101", "invalid source list bounds"),
        ("--limit", "0", "invalid source list bounds"),
        ("--cursor", "sources1.%%%", "InvalidCursor"),
    ] {
        let output = Command::new(executable)
            .args(["stream", "sources", "--binding"])
            .arg(binding.path())
            .args([flag, value])
            .output()?;
        ensure!(!output.status.success());
        ensure!(String::from_utf8(output.stderr)?.contains(message));
    }
    let mut oversized = tempfile::NamedTempFile::new_in(directory.path())?;
    oversized.write_all("x".repeat((16 << 10) + 1).as_bytes())?;
    let output = Command::new(executable)
        .args(["stream", "read", "--binding"])
        .arg(oversized.path())
        .output()?;
    ensure!(!output.status.success());
    ensure!(String::from_utf8(output.stderr)?.contains("operator input file exceeds"));
    Ok(())
}
