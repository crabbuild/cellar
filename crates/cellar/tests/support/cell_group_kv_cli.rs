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
    ensure!(
        command(cluster, backend, &["get", NAMESPACE, "warm"]).await? == b"quorum",
        "CLI selected a different namespace from the SDK"
    );
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

pub(super) async fn large(cluster: &ManagedCluster, backend: &rustfs::StoreConfig) -> Result<()> {
    let sdk_value = command(cluster, backend, &["get", NAMESPACE, "large"]).await?;
    ensure!(sdk_value.len() == cellar_logic::kv::MAX_VALUE_BYTES);
    ensure!(sdk_value
        .iter()
        .enumerate()
        .all(|(index, byte)| *byte == (index % 251) as u8));
    drop(sdk_value);
    let value = (0..cellar_logic::kv::MAX_VALUE_BYTES)
        .map(|index| (index % 251) as u8 ^ 5)
        .collect::<Vec<_>>();
    let path = cluster.roots[0].path().join("cli-large-value.bin");
    std::fs::write(&path, &value)?;
    command(
        cluster,
        backend,
        &[
            "put",
            NAMESPACE,
            "cli-large",
            "--path",
            path.to_str().context("CLI value path")?,
        ],
    )
    .await?;
    ensure!(command(cluster, backend, &["get", NAMESPACE, "cli-large"]).await? == value);
    eprintln!("actual KV CLI 25-MiB SDK read and file put/get: PASS");
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
    let profile = args.first() == Some(&"put") && args.contains(&"--path");
    let before = profile.then(|| {
        cluster
            .hosts
            .iter()
            .map(|host| host.performance())
            .collect::<Vec<_>>()
    });
    let payload_before = profile.then(|| {
        (
            cluster.fixtures[0]
                .store
                .cell_payload_get_calls
                .load(Ordering::Acquire),
            cluster.fixtures[0]
                .store
                .cell_payload_put_calls
                .load(Ordering::Acquire),
        )
    });
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
    if let Some(before) = before {
        for (node, (before, host)) in before.iter().zip(&cluster.hosts).enumerate() {
            let after = host.performance();
            let operations = before
                .operations
                .iter()
                .zip(&after.operations)
                .filter(|(a, b)| a.started != b.started || a.sum_us != b.sum_us)
                .map(|(a, b)| {
                    json!({
                        "name":b.name,"started":b.started.checked_sub(a.started),
                        "succeeded":b.succeeded.checked_sub(a.succeeded),
                        "failed":b.failed.checked_sub(a.failed),
                        "abandoned":b.abandoned.checked_sub(a.abandoned),
                        "sumUs":b.sum_us.checked_sub(a.sum_us)
                    })
                })
                .collect::<Vec<_>>();
            let counters = before
                .counters
                .iter()
                .zip(&after.counters)
                .filter(|(a, b)| a.value != b.value)
                .map(|(a, b)| json!({"name":b.name,"delta":b.value.checked_sub(a.value)}))
                .collect::<Vec<_>>();
            eprintln!(
                "KV CLI performance: {}",
                json!({"node":node,
                "sameSource":before.source==after.source,
                "available":before.available && after.available,
                "saturated":before.saturated || after.saturated,
                "operations":operations,"counters":counters})
            );
        }
    }
    if let Some((gets, puts)) = payload_before {
        eprintln!(
            "KV CLI payload calls: gets={} puts={}",
            cluster.fixtures[0]
                .store
                .cell_payload_get_calls
                .load(Ordering::Acquire)
                - gets,
            cluster.fixtures[0]
                .store
                .cell_payload_put_calls
                .load(Ordering::Acquire)
                - puts
        );
    }
    ensure!(status.success(), "KV CLI {args:?}: {status}: {error}");
    Ok(stdout)
}
