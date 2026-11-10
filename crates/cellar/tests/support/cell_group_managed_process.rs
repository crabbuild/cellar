// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real independent Node/RaftCells ownership against the captured RustFS roots.
use super::*;
use cellar::sdk::fleet_operations::{
    RaftFleetConnection, RaftFleetNamespace, RaftFleetNamespaceScope,
};
use serde_json::{json, Value};
use std::{io::Write as _, path::Path, process::Stdio};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_voter_process_boot_restart_and_stop() -> Result<()> {
    for rooted in [false, true] {
        let (store, connection) = rustfs::process_store("managed-voter-process", rooted)?;
        let fixture = Fixture::with_store(
            rooted,
            managed_fixture::counter_class()?,
            Arc::new(catalog_fault::Store::around(store)),
        )
        .await?;
        let root = tempfile::tempdir()?;
        let checked = async {
            let mut previous: Option<Value> = None;
            for generation in [76, 77] {
                let boot = voter(
                    &fixture,
                    &connection,
                    root.path(),
                    generation,
                    previous.is_some(),
                )
                .await
                .with_context(|| {
                    format!("managed voter process, extents={rooted}, generation={generation}")
                })?;
                if let Some(old) = previous {
                    ensure!(
                        old["node"] == boot["node"]
                            && old["disk"] == boot["disk"]
                            && old["engine"] == boot["engine"]
                            && old["process"] != boot["process"]
                            && old["boot"] != boot["boot"],
                        "retained voter did not preserve engine ownership"
                    );
                }
                previous = Some(boot);
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        fixture.domain.shutdown().await;
        if let Err(error) = checked {
            let retained = root.keep();
            return Err(error.context(format!(
                "owned voter data retained at {}",
                retained.display()
            )));
        }
    }
    Ok(())
}

async fn voter(
    f: &Fixture,
    store: &rustfs::OperatorStoreConfig,
    root: &Path,
    generation: u8,
    retained: bool,
) -> Result<Value> {
    let listeners = (0..4)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().map(|a| a.port()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let certs = root.join("tls");
    std::fs::create_dir_all(&certs)?;
    for (name, bytes) in [
        (
            "ca.der",
            include_bytes!("../../transport/tcp/fixtures/ca.der").as_slice(),
        ),
        (
            "server.der",
            include_bytes!("../../transport/tcp/fixtures/server.der").as_slice(),
        ),
        (
            "key.der",
            include_bytes!("../../transport/tcp/fixtures/key.der").as_slice(),
        ),
    ] {
        let path = certs.join(name);
        if !path.exists() {
            private(&path, bytes)?;
        }
    }
    let connection = RaftFleetConnection {
        cluster_id: hex::encode(f.declaration.namespace.cluster.bytes()),
        incarnation_id: hex::encode([80; 16]),
        coordination_prefix: "hosts/".into(),
        namespaces: vec![RaftFleetNamespace {
            namespace_id: hex::encode(f.declaration.namespace.namespace.bytes()),
            schema_digest: hex::encode(f.declaration.namespace.schema_digest),
            catalog_prefix: "managed/".into(),
            scope: RaftFleetNamespaceScope::StoreRoot,
            managed: true,
            image_storage: f.declaration.namespace.image_storage,
            group_directory: Default::default(),
        }],
    };
    let config = json!({"endpoint":store.endpoint, "bucket":store.bucket, "prefix":store.prefix,
        "access":store.access, "secret":store.secret, "bucket_prefix":"tenant/",
        "connection":connection, "principal":"managed-test", "token":hex::encode([121;32]),
        "node":f.spec.node.bytes(), "disk":f.spec.disk.bytes(), "process":vec![generation;16],
        "label":f.spec.label, "server_name":"localhost", "bind":"127.0.0.1", "advertise":"127.0.0.1",
        "peer_ports":[ports[0],ports[1],ports[2]], "api_port":ports[3],
        "ca":certs.join("ca.der"), "certificate":certs.join("server.der"), "key":certs.join("key.der"),
        "fence_public":ed25519_dalek::SigningKey::from_bytes(&[147;32]).verifying_key().to_bytes()});
    let path = root.join(format!("config-{generation}.json"));
    private(&path, &serde_json::to_vec(&config)?)?;
    let stdout = root.join(format!("stdout-{generation}.log"));
    let stderr = root.join(format!("stderr-{generation}.log"));
    drop(listeners);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?;
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_cellar-perf-cell-group"))
        .env("CELLAR_GROUP_ACTION", "managed-node")
        .env("CELLAR_GROUP_CONFIG", &path)
        .env("CELLAR_GROUP_DATA", root.join("data"))
        .env("CELLAR_GROUP_RETAINED", retained.to_string())
        .env("CELLAR_GROUP_NODE", "0")
        .env("CELLAR_GROUP_GENERATION", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::from(std::fs::File::create(&stdout)?))
        .stderr(Stdio::from(std::fs::File::create(&stderr)?))
        .kill_on_drop(true)
        .spawn()?;
    let endpoint = format!("http://127.0.0.1:{}/control", ports[3]);
    let request = |body: Value| {
        client
            .post(&endpoint)
            .bearer_auth(hex::encode([121; 32]))
            .header("x-cellar-node", hex::encode(f.spec.node.bytes()))
            .header("x-cellar-disk", hex::encode(f.spec.disk.bytes()))
            .header("x-cellar-process", hex::encode(vec![generation; 16]))
            .json(&body)
    };
    let checked = async {
        let observed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                ensure!(
                    child.try_wait()?.is_none(),
                    "managed voter exited during startup"
                );
                if let Ok(response) = request(json!({"action":"observe"})).send().await {
                    ensure!(
                        response.status() != 401 && response.status() != 409,
                        "wrong configured voter identity"
                    );
                    if response.status().is_success() {
                        return Ok::<_, anyhow::Error>(response.json::<Value>().await?);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("managed voter startup deadline")??;
        ensure!(observed["groups"] == 0 && observed["registered_histories"] == 0);
        ensure!(
            observed["boot"]["node"] == json!(f.spec.node.bytes())
                && observed["boot"]["process"] == json!(vec![generation; 16])
        );
        let no_auth = client
            .post(&endpoint)
            .json(&json!({"action":"observe"}))
            .send()
            .await?;
        ensure!(
            no_auth.status() == 401,
            "unauthenticated process control succeeded"
        );
        let mut stale = request(json!({"action":"observe"})).build()?;
        stale.headers_mut().insert(
            "x-cellar-process",
            reqwest::header::HeaderValue::from_str(&hex::encode(vec![generation - 1; 16]))?,
        );
        let stale = client.execute(stale).await?;
        ensure!(
            stale.status() == 409 && stale.json::<Value>().await?["outcome"] == "not_submitted"
        );
        let duplicate = request(json!({"action":"observe"}))
            .header("x-cellar-process", hex::encode(vec![generation; 16]))
            .send()
            .await?;
        ensure!(
            duplicate.status() == 409,
            "ambiguous process headers admitted control"
        );
        let absent = json!({"cell":"Counter:absent","incarnation":vec![9;16]});
        let denied = request(
            json!({"action":"read","cell":absent["cell"],"incarnation":absent["incarnation"],
            "invocation":vec![90;16],"minimum":null}),
        )
        .send()
        .await?;
        ensure!(denied.status() == 503, "empty standby admitted a read");
        let retry_until_ms = u64::try_from(cellar::asyncrt::wall_ms())?
            .checked_add(60_000)
            .context("retry deadline")?;
        let denied = request(
            json!({"action":"write","cell":absent["cell"],"incarnation":absent["incarnation"],
            "session":vec![91;16],"sequence":1,"retry_until_ms":retry_until_ms,"delta":1,"s3":false}),
        )
        .send()
        .await?;
        ensure!(denied.status() == 503, "empty standby admitted a write");
        ensure!(request(json!({"action":"stop"}))
            .send()
            .await?
            .status()
            .is_success());
        let status = tokio::time::timeout(Duration::from_secs(20), child.wait()).await??;
        ensure!(status.success(), "managed voter shutdown failed");
        let output = std::fs::read_to_string(&stdout)?;
        ensure!(
            output
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|value| value["managed_voter"] == "stopped" && value["joined"] == true),
            "managed voter omitted joined shutdown proof"
        );
        Ok::<_, anyhow::Error>(observed["boot"].clone())
    }
    .await;
    if let Err(error) = checked {
        let _ = child.kill().await;
        let joined = child.wait().await;
        let diagnostics = format!(
            "stdout={}\nstderr={}",
            std::fs::read_to_string(stdout).unwrap_or_default(),
            std::fs::read_to_string(stderr).unwrap_or_default()
        )
        .replace(&store.access, "[REDACTED]")
        .replace(&store.secret, "[REDACTED]");
        return Err(error.context(format!("owned child join={joined:?}; {diagnostics}")));
    }
    checked
}

fn private(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
