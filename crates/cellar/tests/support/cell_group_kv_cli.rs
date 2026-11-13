// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The actual frozen CLI reaches the same managed KV namespace as the SDK.
#![allow(clippy::disallowed_methods)]
use super::*;

pub(super) async fn run(cluster: &ManagedCluster, backend: &rustfs::StoreConfig) -> Result<()> {
    use object_store::ObjectStore as _;
    cluster.fixtures[0]
        .store
        .put(
            &object_store::path::Path::from("fleet/peer-auth.json"),
            serde_json::to_vec(&json!({"version":1,"key":"0b".repeat(32)}))?.into(),
        )
        .await?;
    command(cluster, backend, &["put", NAMESPACE, "cli-key", "from-cli"]).await?;
    ensure!(command(cluster, backend, &["get", NAMESPACE, "cli-key"]).await? == b"from-cli");
    let info = command(cluster, backend, &["info", NAMESPACE, "--json"]).await?;
    let info: serde_json::Value = serde_json::from_slice(&info)?;
    ensure!(
        info["keys"].as_u64().is_some_and(|keys| keys > 0),
        "KV CLI info: {info}"
    );
    command(cluster, backend, &["delete", NAMESPACE, "cli-key"]).await?;
    eprintln!("actual KV CLI put/get/info/delete: PASS");
    Ok(())
}

async fn command(
    cluster: &ManagedCluster,
    backend: &rustfs::StoreConfig,
    args: &[&str],
) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt as _;
    let executable = std::env::var_os("CELLAR_CELL_GROUPS_KV_CLI")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_cellar").into());
    let mut command = tokio::process::Command::new(executable);
    command.arg("kv").args(args).args([
        "--bucket",
        &format!("s3://{}/{}", backend.bucket, backend.prefix),
        "--endpoint",
        &backend.endpoint,
        "--region",
        "us-east-1",
    ]);
    for node in &cluster.nodes {
        let info = node.info();
        command.args(["--entrance", &format!("{}={}", info.node, info.advertise)]);
    }
    command
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command.spawn()?;
    let mut out = child.stdout.take().context("KV CLI stdout")?;
    let mut err = child.stderr.take().context("KV CLI stderr")?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = cellar::asyncrt::timeout(Duration::from_secs(20), async {
        tokio::try_join!(
            child.wait(),
            out.read_to_end(&mut stdout),
            err.read_to_end(&mut stderr)
        )
    })
    .await;
    let status = match result {
        Ok(Ok(result)) => result.0,
        _ => {
            let _ = child.start_kill();
            child.wait().await?;
            anyhow::bail!("KV CLI did not complete and was joined");
        }
    };
    let error = String::from_utf8_lossy(&stderr)
        .replace(&backend.access, "[redacted]")
        .replace(&backend.secret, "[redacted]");
    ensure!(status.success(), "KV CLI {args:?}: {status}: {error}");
    Ok(stdout)
}
