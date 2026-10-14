//! The shipped CLI exposes the Rust delivery workflow.
use anyhow::{ensure, Context, Result};
use std::process::Command;

#[test]
fn default_cli_builds_rust_and_retains_fleet_operator_commands() -> Result<()> {
    let executable = env!("CARGO_BIN_EXE_cellar");
    let help = Command::new(executable).arg("--help").output()?;
    ensure!(help.status.success(), "help failed: {:?}", help);
    let help = String::from_utf8(help.stdout)?;
    ensure!(help.contains("cellar build PROJECT.json"));
    ensure!(help.contains("Run a compiled application executable"));
    let version = Command::new(executable).arg("--version").output()?;
    ensure!(version.status.success());
    ensure!(
        String::from_utf8(version.stdout)?.trim() == concat!("cellar ", env!("CARGO_PKG_VERSION"))
    );
    let sql = Command::new(executable).args(["sql", "--help"]).output()?;
    ensure!(sql.status.success(), "SQL operator help failed: {:?}", sql);
    ensure!(String::from_utf8(sql.stdout)?.contains("cellar sql"));
    let dev = Command::new(executable).args(["dev", "--help"]).output()?;
    ensure!(dev.status.success(), "dev help failed: {:?}", dev);
    ensure!(String::from_utf8(dev.stdout)?.starts_with("cellar dev PROJECT.json"));
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
        let log = std::fs::read_to_string(log_path).unwrap_or_default();
        anyhow::anyhow!("{error:#}\nprivate host log:\n{log}")
    })
}
