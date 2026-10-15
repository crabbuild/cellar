//! The shipped CLI exposes the Rust delivery workflow.
use anyhow::{ensure, Context, Result};
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
        let census = Command::new(executable)
            .args(["outbox", "census", "--help"])
            .output()?;
        ensure!(census.status.success());
        ensure!(String::from_utf8(census.stdout)?.contains("unavailable is never empty"));
        let census = Command::new(executable)
            .args(["outbox", "census", "--limit", "101"])
            .output()?;
        ensure!(!census.status.success());
        ensure!(String::from_utf8(census.stderr)?.contains("between 1 and 100"));
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
        for action in ["destroy", "recreate"] {
            let denied = Command::new(executable)
                .args(["outbox", action, "Account:source"])
                .output()?;
            ensure!(!denied.status.success());
            ensure!(
                String::from_utf8(denied.stderr)?.contains(&format!("requires --confirm-{action}"))
            );
        }
        let no_loss = Command::new(executable)
            .args([
                "outbox",
                "destroy",
                "Account:source",
                "--confirm-destroy",
                "--incarnation",
                "00000000000000000000000000000001",
                "--operation",
                "00000000000000000000000000000002",
                "--reason",
                "test",
            ])
            .output()?;
        ensure!(!no_loss.status.success());
        ensure!(String::from_utf8(no_loss.stderr)?.contains("requires --data-loss TEXT"));
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
    let missing = Command::new(executable)
        .args(["stream", "retirement-proof"])
        .output()?;
    ensure!(!missing.status.success());
    ensure!(String::from_utf8(missing.stderr)?.contains("retirement-proof requires --resource"));
    for flag in [
        "--tenant",
        "--application",
        "--cursor",
        "--limit",
        "--binding",
    ] {
        let output = Command::new(executable)
            .args(["stream", "retirement-proof", flag, "ignored"])
            .output()?;
        ensure!(!output.status.success());
        ensure!(
            String::from_utf8(output.stderr)?.contains("invalid catalog retirement-proof option")
        );
    }
    let mut resource = tempfile::NamedTempFile::new_in(directory.path())?;
    resource.write_all(br#"{"tenant":"tenant","application":"app","name":"events","family":"events","id":"not-a-uuid"}"#)?;
    let invalid = Command::new(executable)
        .args(["stream", "retirement-proof", "--resource"])
        .arg(resource.path())
        .output()?;
    ensure!(!invalid.status.success());
    ensure!(String::from_utf8(invalid.stderr)?
        .contains("stream resource incarnation must be 32 lowercase hexadecimal digits"));
    let mut too_large = tempfile::NamedTempFile::new_in(directory.path())?;
    too_large.write_all(&vec![b'x'; 4097])?;
    let oversized = Command::new(executable)
        .args(["stream", "retirement-proof", "--resource"])
        .arg(too_large.path())
        .output()?;
    ensure!(!oversized.status.success());
    ensure!(String::from_utf8(oversized.stderr)?.contains("operator input file exceeds"));
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

/// Exercise the shipped host, its deployment watcher, and the signed management
/// route against a private local object store. No live fleet or user state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standalone_host_adopts_cron_configuration_and_joins_before_stop_receipt() -> Result<()> {
    use cellar::management::{FleetClient, NodeAction, PageRequest, StopMode};
    use serde_json::json;
    use std::time::Duration;
    let directory = tempfile::tempdir()?;
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let executable = std::path::Path::new(env!("CARGO_BIN_EXE_cellar-native-counter"));
    let database = directory.path().join("objects.sqlite");
    let bucket = domain.run(async {
        let bucket = cellar::dev::open_local_bucket(&database)?;
        bucket.put("fleet/peer-auth.json", json!({"version":1,"key":"0b".repeat(32)}).to_string().into_bytes()).await?;
        let config: cellar::native_deploy::ApplicationConfig = serde_json::from_value(json!({
            "script_name":"native-counter", "application_version":"v1", "cells":["Counter"],
            "bindings":[{"type":"durable_object_namespace","name":"COUNTERS","class_name":"Counter"}],
            "crons":["0 0 1 1 *"]
        }))?;
        let artifact = cellar::native_deploy::executable_identity(executable, cellar::native_deploy::current_target())?;
        let manifest = config.manifest(vec![artifact])?;
        let prefix = format!("deploy/native-counter/{}", manifest.version);
        bucket.put(&format!("{prefix}/manifest.json"), serde_json::to_vec(&manifest)?).await?;
        let pointer = json!({"script_name":"native-counter","version":manifest.version,"prefix":prefix,"rollout":{"percent":100}});
        for key in ["deploy/current.json", "deploy/native-counter/current.json"] {
            bucket.put(key, serde_json::to_vec(&pointer)?).await?;
        }
        Ok::<_,anyhow::Error>(bucket)
    }).await??;
    let log_path = directory.path().join("node.log");
    // The process-test harness owns this stdout file outside the node domain.
    #[allow(clippy::disallowed_methods)]
    let log = std::fs::File::create(&log_path)?;
    let mut process = tokio::process::Command::new(executable)
        .env_clear()
        .env("CELLAR_NATIVE_FLEET", "1")
        .env("CELLAR_INTERNAL_DEV_STORE", &database)
        .env("CELLAR_WATCH", directory.path().join("runtime"))
        .env("CELLAR_NODE", "console-cli")
        .env("CELLAR_DEPLOY_POLL_S", "1")
        .env("CELLAR_SHUTDOWN_REPORT_JSON", "1")
        .env("RUST_LOG", "error")
        .args([
            "--no-control-plane",
            "--bucket",
            "cellar-dev",
            "--listen",
            "127.0.0.1:0",
            "--internal-listen",
            "127.0.0.1:0",
        ])
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()?;
    let result = domain
        .run(async {
            let client = FleetClient::new(bucket, false);
            let generation = cellar::asyncrt::timeout(Duration::from_secs(30), async {
                loop {
                    if let Ok(node) = client.node("console-cli").await {
                        let generation = node.generation;
                        if let Ok(state) = client
                            .node_action("console-cli", &generation, NodeAction::State)
                            .await
                        {
                            if state["capabilities"]["stop"] == true {
                                return generation;
                            }
                        }
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("standalone management readiness")?;
            let before = client.cron_configuration("native-counter").await?;
            let saved = client
                .update_crons("native-counter", &before.deployment_revision, None, vec![])
                .await?;
            cellar::asyncrt::timeout(Duration::from_secs(15), async {
                loop {
                    let observations = client
                        .cron_propagation("native-counter", PageRequest::default())
                        .await?;
                    if observations.items.iter().any(|node| {
                        node.status == "adopted" && node.observed_revision == saved.revision
                    }) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("standalone cron adoption")??;
            let acknowledgment = client
                .node_action(
                    "console-cli",
                    &generation,
                    NodeAction::Stop {
                        operation_id: "stop-cli".into(),
                        mode: StopMode::Handoff,
                    },
                )
                .await?;
            ensure!(
                acknowledgment["receipt"]["domain_joined"] == false,
                "acceptance fabricated completion"
            );
            let status = cellar::asyncrt::timeout(Duration::from_secs(30), process.wait())
                .await
                .context("standalone stop join")??;
            ensure!(status.success(), "standalone stop failed: {status}");
            let receipt = client
                .stop_receipt("console-cli", &generation, "stop-cli")
                .await?
                .ok_or_else(|| anyhow::anyhow!("missing persisted stop receipt"))?;
            ensure!(
                receipt.domain_joined
                    && receipt.phase == "complete"
                    && receipt
                        .proof
                        .as_ref()
                        .is_some_and(|proof| proof.complete(StopMode::Handoff)),
                "standalone exit has incomplete proof: {receipt:?}"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result.map_err(|error| {
        // Read harness diagnostics after its execution domain has joined.
        #[allow(clippy::disallowed_methods)]
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        anyhow::anyhow!("{error:#}\nprivate host log:\n{log}")
    })?;
    // The process harness reads its log after the node and reporting domains join.
    #[allow(clippy::disallowed_methods)]
    let log = std::fs::read_to_string(&log_path)?;
    let report = log
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|value| value["event"] == "daemon_node_stopped")
        .context("standalone exit omitted its JSON shutdown report")?;
    ensure!(
        report["outcome"] == "stopped"
            && report["mode"] == "handoff"
            && report["ingress_drained"] == true
            && report["application_work_drained"] == true
            && report["cells_drained"] == true
            && report["local_durability_closed"] == true
            && report["drain_timed_out"] == false
            && report["deadline_elapsed"] == false,
        "standalone JSON report has incomplete proof: {report}"
    );
    Ok(())
}
