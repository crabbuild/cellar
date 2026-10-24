// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production process startup, retained-history resume and refusal boundaries.
#![allow(clippy::disallowed_methods)]
#[path = "../../../examples/raft-counter/counter.rs"]
#[allow(dead_code)]
mod counter;
#[path = "support/html.rs"]
#[allow(dead_code)]
mod graph;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, *};
use serde_json::Value;
use std::{
    fs::File,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const SECRET: &str = "fixture-secret-raft-cli";

fn child(root: PathBuf, mode: String) -> Result<()> {
    let class = counter::definition()?;
    let namespace = NamespaceId::new([2; 16])?;
    let api = counter::CounterApi::new(SECRET.into(), namespace, &class)?;
    let serving = api.clone();
    let prepared_root = root.clone();
    let verify_join = mode == "bind-fail";
    let verify_directory = root.join("disk/raft");
    let factories = AtomicUsize::new(0);
    let result = Host::local(graph::graph("raft-cli", "", vec![]))
        .register("raft-cli", "test", move || {
            // Exactly one application instance must serve HTTP and register Raft.
            ensure!(factories.fetch_add(1, Ordering::SeqCst) == 0, "Raft startup constructed a second application instance");
            std::fs::write(root.join("factory-called"), b"1")?;
            counter::application(serving.clone(), counter::definition()?)
        })?
        .raft_cells(if mode == "missing-app" { "absent" } else { "raft-cli" }, move |node| async move {
            std::fs::write(prepared_root.join("prepared"), node.data_dir.as_os_str().as_encoded_bytes())?;
            if mode == "cancel-prepare" {
                std::future::pending::<()>().await;
            }
            let control = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let data = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let query = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let ordinal = mode.strip_prefix("cluster-").map(str::parse::<u8>).transpose()?;
            let index = ordinal.unwrap_or(0);
            let id = NodeId::new([index + 3; 16])?;
            let disk = DiskLineage::new([index + 4; 16])?;
            let mut process = [0; 16];
            process[..4].copy_from_slice(&std::process::id().to_be_bytes());
            let process = ProcessGeneration::new(process)?;
            let peer = RaftPeerNode {
                node: id, disk, process, label: node.node,
                server_name: "localhost".into(),
                control_address: control.local_addr()?,
                data_address: data.local_addr()?,
                query_address: query.local_addr()?,
            };
            let mut peers = if ordinal.is_some() {
                std::fs::write(prepared_root.join(format!("peer-{index}.tmp")), serde_json::to_vec(&serde_json::json!({
                    "pid": std::process::id(), "label": peer.label,
                    "control": peer.control_address, "data": peer.data_address, "query": peer.query_address,
                }))?)?;
                std::fs::rename(prepared_root.join(format!("peer-{index}.tmp")), prepared_root.join(format!("peer-{index}.json")))?;
                let deadline = Instant::now() + Duration::from_secs(15);
                while !(0..3).all(|i| prepared_root.join(format!("peer-{i}.json")).exists()) {
                    ensure!(Instant::now() < deadline, "peer startup rendezvous expired");
                    cellar::asyncrt::sleep(Duration::from_millis(25)).await;
                }
                (0..3u8).map(|i| -> Result<_> {
                    let file: Value = serde_json::from_slice(&std::fs::read(prepared_root.join(format!("peer-{i}.json")))?)?;
                    let mut generation = [0; 16];
                    generation[..4].copy_from_slice(&u32::try_from(file["pid"].as_u64().context("pid missing")?)?.to_be_bytes());
                    Ok(RaftPeerNode {
                        node: NodeId::new([i + 3; 16])?, disk: DiskLineage::new([i + 4; 16])?, process: ProcessGeneration::new(generation)?,
                        label: file["label"].as_str().context("label missing")?.into(), server_name: "localhost".into(),
                        control_address: serde_json::from_value(file["control"].clone())?,
                        data_address: serde_json::from_value(file["data"].clone())?,
                        query_address: serde_json::from_value(file["query"].clone())?,
                    })
                }).collect::<Result<Vec<_>>>()?
            } else { vec![peer] };
            if mode == "missing-peer-auth" {
                let mut remote = peers[0].clone();
                remote.node = NodeId::new([42; 16])?;
                remote.label = "unreachable-fixture-peer".into();
                peers.push(remote);
            }
            let cluster = ClusterId::new([1; 16])?;
            let mut config = NamespaceConfig::new(cluster, namespace, NamespaceMode::Raft,
                if mode == "bad-schema" { [99; 32] } else { class.schema_digest() });
            config.voters = if ordinal.is_some() { VoterPolicy::ThreeVoters } else { VoterPolicy::SingleVoterDevelopment };
            let cells = ["one", "two"].into_iter().enumerate().map(|(i, name)| -> Result<_> {
                Ok(RaftCellRegistration {
                    class: "Counter".into(), placement: RaftCellPlacement::Replica,
                    namespace: config.clone(),
                    birth: BirthProposal {
                        scope: GroupScope { cluster, namespace, cell: format!("Counter:{name}"),
                            group: GroupId::new([i as u8 + 5; 16])?, incarnation: IncarnationId::new([7; 16])? },
                        voters: peers.iter().enumerate().map(|(i, peer)| Ok(InitialMember { node: peer.node, member: MemberId::new(i as u64 + 1)? })).collect::<Result<Vec<_>>>()?, learners: vec![],
                    },
                })
            }).collect::<Result<Vec<_>>>()?;
            let log_mode = if ordinal.is_some() || mode == "create" || mode == "bad-schema" || mode == "bind-fail" {
                LogOpenMode::Create
            } else { LogOpenMode::Resume };
            Ok(NativeRaftCells {
                config: RaftCellsConfig {
                    infrastructure: NodeRaftConfig {
                        log: ReplicaLogOptions::new(node.data_dir.join("raft"), id, disk, process, log_mode),
                        consensus: RaftRuntimeOptions { tick_interval_ms: Some(25), ..Default::default() },
                        transport: TransportOptions::default(),
                    },
                    catalog: CatalogOptions { prefix: "raft-cli".into(), ..Default::default() },
                    cells, peers, query_origins: vec![], host: RaftHostOptions::default(),
                },
                listeners: NodeRaftListeners { control, data, query },
                tls: RaftTls::new(
                    vec![include_bytes!("../transport/tcp/fixtures/ca.der").to_vec().into()],
                    vec![include_bytes!("../transport/tcp/fixtures/server.der").to_vec().into()],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(include_bytes!("../transport/tcp/fixtures/key.der").to_vec()).into(),
                )?,
                store: cellar::bucket::Bucket::open_dev(&prepared_root.join("catalog.sqlite"))?.raft_store(),
                versions: VersionKeys::new(VersionSigningKey::new([29; 32], u64::MAX)?, vec![], Duration::from_secs(86400))?,
            })
        }, move |service| {
            if std::env::var("CELLAR_RAFT_CLI_CHILD").as_deref() == Ok("bind-fail") {
                anyhow::bail!("fixture binding failed");
            }
            *api.cells.lock().map_err(|_| anyhow::anyhow!("binding poisoned"))? = Some(service);
            Ok(())
        })?
        .run_until(std::future::pending());
    if verify_join {
        ensure!(result.is_err(), "failed binding admitted a node");
        // Stay in this process after the daemon returned. A surviving storage
        // worker/owner would still hold the engine lock and refuse this open.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(async {
                let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
                let opened = domain
                    .run(async {
                        let log = cellar::sdk::consensus::verification::ReplicaLog::open(
                            ReplicaLogOptions::new(
                                verify_directory,
                                NodeId::new([3; 16])?,
                                DiskLineage::new([4; 16])?,
                                ProcessGeneration::new([250; 16])?,
                                LogOpenMode::Resume,
                            ),
                            Arc::new(cellar_ltx::DirectFileSystem),
                        )
                        .await?;
                        log.close().await?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .await?;
                domain.shutdown().await;
                opened
            })?;
        cellar::note!("failed Raft binding physically joined before daemon returned");
    }
    result
}

struct Process {
    child: Child,
    log: PathBuf,
    diagnostics: PathBuf,
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl Process {
    fn spawn(
        root: &Path,
        mode: &str,
        data_args: &[&std::ffi::OsStr],
        override_dir: bool,
    ) -> Result<Self> {
        let log = root.join(format!("{mode}-stdout.log"));
        let diagnostics = root.join(format!("{mode}-stderr.log"));
        let mut cmd = Command::new(std::env::current_exe()?);
        cmd.env_clear()
            .env("CELLAR_RAFT_CLI_CHILD", mode)
            .env("CELLAR_RAFT_CLI_ROOT", root)
            .env("CELLAR_NODE", format!("raft-cli-{mode}"))
            .env("CELLAR_TOKIO_THREADS", "3")
            .env("CELLAR_REBALANCE_INTERVAL_MS", "0")
            .env("CELLAR_PLACEMENT_WEIGHT", "1")
            .env("CELLAR_SHUTDOWN_TOTAL_MS", "6000")
            .env("CELLAR_SHUTDOWN_REPORT_JSON", "1")
            .env("RUST_LOG", "warn")
            .args([
                "--listen",
                "127.0.0.1:0",
                "--internal-listen",
                "127.0.0.1:0",
            ])
            .args(data_args)
            .stdout(Stdio::from(File::create(&log)?))
            .stderr(Stdio::from(File::create(&diagnostics)?));
        if mode.starts_with("cluster-") {
            cmd.env("CELLAR_TEST_BUCKET", "cellar-dev")
                .env("CELLAR_INTERNAL_DEV_STORE", root.join("fleet.sqlite"));
        }
        if override_dir {
            cmd.env("CELLAR_TEST_DATA_DIR", root.join("override"));
        }
        Ok(Self {
            child: cmd.spawn()?,
            log,
            diagnostics,
        })
    }

    fn logs(&self) -> Result<String> {
        Ok(format!(
            "{}\n{}",
            std::fs::read_to_string(&self.log)?,
            std::fs::read_to_string(&self.diagnostics)?
        ))
    }

    async fn exited(&mut self, success: bool) -> Result<String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait()? {
                let logs = self.logs()?;
                ensure!(
                    status.success() == success,
                    "unexpected exit {status}\n{logs}"
                );
                return Ok(logs);
            }
            ensure!(
                Instant::now() < deadline,
                "child did not exit\n{}",
                self.logs()?
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn addresses(&mut self) -> Result<(String, String)> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let logs = self.logs()?;
            let addr = |prefix| {
                logs.lines().find_map(|line| {
                    line.strip_prefix(prefix)
                        .and_then(|s| s.split_whitespace().next())
                        .map(|s| format!("http://{s}"))
                })
            };
            if let (Some(public), Some(internal)) = (
                addr("cellar listening on "),
                addr("cellar internal listening on "),
            ) {
                return Ok((public, internal));
            }
            ensure!(
                self.child.try_wait()?.is_none() && Instant::now() < deadline,
                "startup failed\n{logs}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

async fn qualify() -> Result<()> {
    for (name, args, override_dir, expected) in [
        ("missing-dir", vec![], false, "requires --data-dir"),
        (
            "relative-dir",
            vec!["--data-dir", "relative"],
            false,
            "absolute path",
        ),
        (
            "duplicate-dir",
            vec![
                "--data-dir",
                "/tmp/never-open-raft",
                "--data-dir",
                "/tmp/never-open-raft",
            ],
            false,
            "only once",
        ),
        (
            "override-dir",
            vec!["--data-dir", "/tmp/never-open-raft"],
            true,
            "refuses test/dev directory overrides",
        ),
    ] {
        let root = tempfile::tempdir()?;
        let args: Vec<_> = args.iter().map(std::ffi::OsStr::new).collect();
        let logs = Process::spawn(root.path(), name, &args, override_dir)?
            .exited(false)
            .await?;
        ensure!(
            logs.contains(expected) && !logs.contains("cellar listening on "),
            "wrong refusal\n{logs}"
        );
        ensure!(
            !root.path().join("prepared").exists(),
            "rejected CLI entered Raft preparation"
        );
    }
    for (mode, expected) in [
        ("missing-app", "absent from the active deployment"),
        ("bad-schema", "mode/schema disagrees"),
        (
            "missing-peer-auth",
            "requires a fleet bucket for shared peer authentication",
        ),
    ] {
        let root = tempfile::tempdir()?;
        let disk = root.path().join("disk");
        let logs = Process::spawn(
            root.path(),
            mode,
            &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
            false,
        )?
        .exited(false)
        .await?;
        ensure!(
            logs.contains(expected) && !logs.contains("cellar listening on "),
            "wrong refusal\n{logs}"
        );
        ensure!(
            !disk.join("raft").exists(),
            "invalid registration opened the log"
        );
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(7))
        .build()?;
    let root = tempfile::tempdir()?;
    let disk = root.path().join("disk");
    let args = [std::ffi::OsStr::new("--data-dir"), disk.as_os_str()];
    // Binding failure must join the engine so a fresh process can resume the
    // actual retained history. The callback cannot turn startup into admission.
    let logs = Process::spawn(root.path(), "bind-fail", &args, false)?
        .exited(false)
        .await?;
    ensure!(
        logs.contains("fixture binding failed")
            && logs.contains("physically joined before daemon returned")
            && !logs.contains("cellar listening on "),
        "{logs}"
    );
    for (mode, first_sequence, expected_one, expected_two) in [
        ("resume", 1, 3, 4),
        ("restart", 2, 6, 8),
        ("restart-after-refused-create", 3, 9, 12),
    ] {
        if mode == "restart-after-refused-create" {
            let refusal = Process::spawn(root.path(), "create", &args, false)?
                .exited(false)
                .await?;
            ensure!(
                !refusal.contains("cellar listening on "),
                "retained history admitted Create"
            );
        }
        let mut process = Process::spawn(root.path(), mode, &args, false)?;
        let (public, internal) = process.addresses().await?;
        for (index, name, delta, expected) in
            [(0, "one", 3, expected_one), (1, "two", 4, expected_two)]
        {
            let command = format!("{}:{first_sequence}", hex::encode([index + 50; 16]));
            let retry = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis()
                + 60_000)
                .to_string();
            // Retry only the same immutable request and preserve any ambiguity.
            let deadline = Instant::now() + Duration::from_secs(15);
            let receipt: Value = loop {
                let response = client
                    .post(format!("{public}/counters/{name}?delta={delta}"))
                    .bearer_auth(SECRET)
                    .header("x-command-id", &command)
                    .header("x-retry-until-ms", &retry)
                    .send()
                    .await?;
                let status = response.status();
                let body = response.text().await?;
                if status.is_success() {
                    break serde_json::from_str(&body)?;
                }
                ensure!(
                    status == 503 && Instant::now() < deadline,
                    "command failed {status}: {body}\n{}",
                    process.logs()?
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            ensure!(
                receipt["value"] == expected,
                "retained state lost: {receipt}"
            );
            let query: Value = client
                .get(format!("{public}/counters/{name}"))
                .bearer_auth(SECRET)
                .header("x-read-consistency", "linearizable")
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            ensure!(
                query["value"] == expected
                    && query["group"] == receipt["group"]
                    && query["incarnation"] == receipt["incarnation"],
                "wrong history: {query}"
            );
        }
        let reload = client.post(format!("{internal}/reload")).send().await?;
        ensure!(
            !reload.status().is_success(),
            "Raft definition was reloaded without rebinding"
        );
        client
            .post(format!("{internal}/shutdown"))
            .send()
            .await?
            .error_for_status()?;
        let logs = process.exited(true).await?;
        let report: Value = logs
            .lines()
            .find_map(|line| {
                serde_json::from_str::<Value>(line)
                    .ok()
                    .filter(|v| v["event"] == "daemon_node_stopped")
            })
            .context("joined CLI report absent")?;
        ensure!(
            report["raft"]["owners_closed"] == true
                && report["raft"]["startup_incomplete"] == false
                && report["raft"]["groups"] == 2
                && report["raft"]["retain_replica_disks"] == true,
            "wrong close proof: {report}"
        );
    }
    let root = tempfile::tempdir()?;
    let disk = root.path().join("disk");
    let mut process = Process::spawn(
        root.path(),
        "cancel-prepare",
        &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
        false,
    )?;
    let deadline = Instant::now() + Duration::from_secs(20);
    while !root.path().join("prepared").exists() {
        ensure!(
            process.child.try_wait()?.is_none() && Instant::now() < deadline,
            "prepare did not start\n{}",
            process.logs()?
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    ensure!(Command::new("/bin/kill")
        .args(["-TERM", &process.child.id().to_string()])
        .status()?
        .success());
    let logs = process.exited(false).await?;
    ensure!(
        logs.contains("startup interrupted by SIGTERM") && !logs.contains("cellar listening on "),
        "{logs}"
    );
    // Three separate production CLI processes use a shared catalog and peer
    // authentication key, two independent groups and real TLS replication.
    let root = tempfile::tempdir()?;
    // The real dev supervisor initializes its local store before starting
    // children. Concurrent first-open WAL/schema setup is not peer readiness.
    cellar::bucket::Bucket::open_dev(&root.path().join("fleet.sqlite"))?;
    cellar::bucket::Bucket::open_dev(&root.path().join("catalog.sqlite"))?;
    let mut cluster = Vec::new();
    for i in 0..3 {
        let disk = root.path().join(format!("disk-{i}"));
        cluster.push(Process::spawn(
            root.path(),
            &format!("cluster-{i}"),
            &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
            false,
        )?);
    }
    let mut addresses = Vec::new();
    for index in 0..cluster.len() {
        match cluster[index].addresses().await {
            Ok(address) => addresses.push(address),
            Err(error) => {
                let logs = cluster
                    .iter()
                    .map(Process::logs)
                    .collect::<Result<Vec<_>>>()?;
                anyhow::bail!(
                    "three-daemon startup failed: {error:#}\n{}",
                    logs.join("\n--- peer ---\n")
                );
            }
        }
    }
    for name in ["one", "two"] {
        let command = format!("{}:1", hex::encode([90; 16]));
        let retry = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            + 60_000)
            .to_string();
        let deadline = Instant::now() + Duration::from_secs(20);
        let (leader, receipt) = loop {
            let mut committed = None;
            for (index, (public, _)) in addresses.iter().enumerate() {
                let response = client
                    .post(format!("{public}/counters/{name}?delta=11"))
                    .bearer_auth(SECRET)
                    .header("x-command-id", &command)
                    .header("x-retry-until-ms", &retry)
                    .send()
                    .await?;
                let status = response.status();
                let body = response.text().await?;
                if status.is_success() {
                    committed = Some((index, serde_json::from_str::<Value>(&body)?));
                    break;
                }
                ensure!(status == 503, "unexpected command failure {status}: {body}");
            }
            if let Some(result) = committed {
                break result;
            }
            ensure!(
                Instant::now() < deadline,
                "three-voter CLI group did not elect"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        ensure!(receipt["value"] == 11, "wrong committed result: {receipt}");
        for (index, (public, _)) in addresses
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != leader)
        {
            let query: Value = client
                .get(format!("{public}/counters/{name}"))
                .bearer_auth(SECRET)
                .header("x-read-consistency", "at-least")
                .header(
                    "x-min-version",
                    receipt["version_token"]
                        .as_str()
                        .context("version token absent")?,
                )
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            ensure!(
                query["value"] == 11
                    && query["served_by"] == index as u64 + 1
                    && query["group"] == receipt["group"],
                "follower query failed: {query}"
            );
        }
    }
    for (process, (_, internal)) in cluster.iter_mut().zip(&addresses) {
        client
            .post(format!("{internal}/shutdown"))
            .send()
            .await?
            .error_for_status()?;
        let logs = process.exited(true).await?;
        ensure!(
            logs.contains("\"owners_closed\":true"),
            "three-voter owner did not join\n{logs}"
        );
    }
    cellar::note!("production Raft CLI qualification passed: directory and registration refusals, failed binding joins, two retained groups, linearizable queries, process restart, semantic closure, startup cancellation, and three daemon processes with two groups over TLS and local follower reads");
    Ok(())
}

fn main() -> Result<()> {
    if let Ok(mode) = std::env::var("CELLAR_RAFT_CLI_CHILD") {
        return child(
            std::env::var_os("CELLAR_RAFT_CLI_ROOT")
                .context("child root missing")?
                .into(),
            mode,
        );
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()?
        .block_on(qualify())
}
