// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::process::{Child, Command, Stdio};

pub(super) struct Process {
    pub(super) child: Child,
    pub(super) control: PathBuf,
    sequence: u64,
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
    pub(super) fn spawn(config: &Config) -> Result<Self> {
        std::fs::create_dir_all(&config.control)?;
        ensure!(
            config.directory.exists() == config.retained,
            "worker directory does not match cold/retained startup"
        );
        let path = config.control.join("config.json");
        atomic_json(&path, config)?;
        let log = File::create(config.control.join("worker.log"))?;
        // Inherit the controller's owned process group so its timeout/signal
        // cleanup also reaches children if the controller itself cannot unwind.
        // Credentials are inherited in the environment, never placed in files.
        let child = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                CHILD_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_CONFIG, path)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        Ok(Self {
            child,
            control: config.control.clone(),
            sequence: 0,
        })
    }

    pub(super) async fn file(&mut self, name: &str, timeout: Duration) -> Result<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            let path = self.control.join(name);
            if path.exists() {
                return Ok(serde_json::from_slice(&std::fs::read(path)?)?);
            }
            ensure!(
                self.child.try_wait()?.is_none(),
                "worker exited waiting for {name}; see {}",
                self.control.display()
            );
            ensure!(
                Instant::now() < deadline,
                "worker timed out waiting for {name}; see {}",
                self.control.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub(super) async fn raw(&mut self, action: Action) -> Result<Value> {
        self.sequence += 1;
        atomic_json(
            &self.control.join("request.json"),
            &Request {
                sequence: self.sequence,
                action,
            },
        )?;
        let deadline = Instant::now() + START_TIMEOUT + Duration::from_secs(30);
        loop {
            let path = self.control.join("response.json");
            if path.exists() {
                let response: Value = serde_json::from_slice(&std::fs::read(path)?)?;
                if response["sequence"] == self.sequence {
                    return Ok(response);
                }
            }
            ensure!(
                self.child.try_wait()?.is_none(),
                "worker exited during request {}; see {}",
                self.sequence,
                self.control.display()
            );
            ensure!(
                Instant::now() < deadline,
                "worker request {} expired; see {}",
                self.sequence,
                self.control.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub(super) async fn call(&mut self, action: Action) -> Result<Value> {
        let response = self.raw(action).await?;
        ensure!(
            response["ok"] == true,
            "worker operation failed: {response}"
        );
        Ok(response["value"].clone())
    }

    pub(super) async fn stop(&mut self) -> Result<Value> {
        let response = self.call(Action::Stop).await?;
        ensure!(
            response["owners_closed"] == true,
            "worker did not close its owners"
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait()? {
                ensure!(status.success(), "worker shutdown exit: {status}");
                return Ok(response);
            }
            ensure!(
                Instant::now() < deadline,
                "worker did not exit after closure"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

pub(super) struct Ledger {
    pub(super) path: PathBuf,
    pub(super) started: Instant,
    pub(super) value: Value,
}

impl Ledger {
    pub(super) fn save(&self) -> Result<()> {
        atomic_json(&self.path, &self.value)
    }

    pub(super) fn phase(&mut self, name: &str, detail: Value) -> Result<()> {
        let elapsed = u64::try_from(self.started.elapsed().as_micros())?;
        self.value["events"]
            .as_array_mut()
            .context("events")?
            .push(json!({"name": name, "elapsed_us": elapsed, "detail": detail}));
        eprintln!("process-recovery phase={name} elapsed_us={elapsed}");
        self.save()
    }
}

pub(super) async fn leader(processes: &mut [Process], killed: Option<usize>) -> Result<usize> {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        for (index, process) in processes.iter_mut().enumerate() {
            if Some(index) != killed && process.call(Action::Status).await?["ready"] == true {
                return Ok(index);
            }
        }
        ensure!(
            Instant::now() < deadline,
            "no command-ready surviving leader"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(super) fn counter(performance: &Value, name: &str) -> Result<u64> {
    let items = performance["counters"]
        .as_array()
        .context("performance counters missing")?;
    let value = items
        .iter()
        .find(|item| item["name"] == name)
        .context("counter missing")?;
    Ok(value["value"]
        .as_str()
        .context("counter value missing")?
        .parse()?)
}

pub(super) fn same_receipt(current: &Value, previous: &Value) -> Result<()> {
    for name in ["value", "group", "incarnation", "durability"] {
        ensure!(current[name] == previous[name], "duplicate changed {name}");
    }
    let index = current["index"].as_u64().context("current index")?;
    let old = previous["index"].as_u64().context("old index")?;
    ensure!(
        index >= old && (index != old || current["term"] == previous["term"]),
        "duplicate history regressed"
    );
    Ok(())
}

pub(super) async fn run(quota: u64) -> Result<()> {
    let work = PathBuf::from(
        std::env::var_os("CELLAR_LARGE_IMAGE_WORKDIR")
            .context("admitted worker directory required")?,
    );
    ensure!(work.is_absolute(), "worker directory must be absolute");
    let directory = tempfile::Builder::new()
        .prefix("process-recovery-")
        .tempdir_in(work)?
        .keep();
    // Keep failed and successful files until the evidence runner has consumed
    // the result and independently verified that every child joined.
    let prefix = format!(
        "process-recovery-qualification/{:032x}",
        rand::random::<u128>()
    );
    eprintln!(
        "process-recovery directory={} object_prefix={prefix}",
        directory.display()
    );
    let mut ledger = Ledger {
        path: directory.join("evidence.json"),
        started: Instant::now(),
        value: json!({"format": 1, "failure_mode": "sigkill", "backend": "rustfs", "storage": "extents",
            "quota": quota, "directory": directory, "prefix": prefix, "complete": false, "events": []}),
    };
    ledger.save()?;
    let result = drill(&directory, quota, &prefix, &mut ledger).await;
    match &result {
        Ok(()) => ledger.value["complete"] = true.into(),
        Err(error) => ledger.value["failure"] = format!("{error:#}").into(),
    }
    ledger.save()?;
    result
}

async fn drill(directory: &Path, quota: u64, prefix: &str, ledger: &mut Ledger) -> Result<()> {
    let source_directory = directory.join("preparation");
    std::fs::create_dir(&source_directory)?;
    let source = if quota == 10_000_000_000 {
        let source = PathBuf::from(
            std::env::var_os("CELLAR_LARGE_IMAGE_FIXTURE")
                .context("10 GB source fixture required")?,
        );
        ensure!(
            std::fs::metadata(&source)?.permissions().readonly(),
            "golden fixture must be read-only"
        );
        ensure!(
            std::fs::metadata(&source)?.len() == quota / 4096 * 4096,
            "golden fixture size mismatch"
        );
        source
    } else {
        fixture::make_template(&source_directory, quota, true)?
    };
    let reserve = (quota / 16).min(32 * 1024 * 1024) / 512 * 512;
    let (template, image, payload_bytes, samples) =
        fixture::prepared_template(&source, &source_directory, quota, reserve)?;
    ledger.value["image_bytes"] = image.length.into();
    let definition = fixture::recovery_definition(Arc::new(AtomicUsize::new(0)))?.0;
    let registration = registration(definition.schema)?;
    let store = store(prefix)?;

    // Reserve all three lanes for every member. The replacement reservations
    // remain held until immediately before its post-kill process is spawned.
    let mut reservations = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..4 {
        let sockets = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        peers.push(Peer {
            addresses: [
                sockets[0].local_addr()?,
                sockets[1].local_addr()?,
                sockets[2].local_addr()?,
            ],
        });
        reservations.push(sockets);
    }
    let configs = (0..4)
        .map(|index| Config {
            disaster: false,
            retained: false,
            generation: 0,
            index,
            quota,
            prefix: prefix.into(),
            directory: directory.join(format!("worker-{index}")),
            control: directory.join(format!("control-{index}")),
            peers: peers.clone(),
            source: None,
            payload_bytes,
            sample_ids: samples.ids,
            sample_hashes: samples.hashes,
        })
        .collect::<Vec<_>>();
    let mut processes = Vec::new();
    for index in 0..3 {
        reservations[index].clear();
        processes.push(Process::spawn(&configs[index])?);
    }
    let mut logs = Vec::new();
    let mut pids = Vec::new();
    for (index, process) in processes.iter_mut().enumerate() {
        let ready = process.file("ready.json", START_TIMEOUT).await?;
        ensure!(
            ready["pid"] == process.child.id() && ready["initially_empty"] == true,
            "worker identity mismatch"
        );
        pids.push(process.child.id());
        let data = PathBuf::from(ready["data_dir"].as_str().context("data directory")?);
        ensure!(
            data.starts_with(&configs[index].directory),
            "worker data escaped its directory"
        );
        let peer = peers[index].native(index)?;
        logs.push(ReplicaLogOptions::new(
            data.join("raft"),
            peer.node,
            peer.disk,
            peer.process,
            LogOpenMode::Create,
        ));
    }
    ensure!(
        pids.iter().all(|pid| *pid != std::process::id())
            && pids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == 3,
        "workers must have independent OS processes"
    );
    fixture::preload(template, image, registration.clone(), quota)(logs, store.clone()).await?;
    std::fs::remove_dir_all(&source_directory)?;
    ensure!(
        !source_directory.exists(),
        "prepared source retained after preload"
    );
    for process in &processes {
        atomic_json(&process.control.join("start.json"), &json!(true))?;
    }
    for process in &mut processes {
        process.file("started.json", START_TIMEOUT).await?;
    }
    let original = leader(&mut processes, None).await?;
    let retry_until_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 86_000_000;
    ledger.phase(
        "request_before_loss",
        json!({"client": 1, "sequence": 1, "retry_until_ms": retry_until_ms, "input": 0}),
    )?;
    let first = processes[original]
        .call(Action::Command {
            client: 1,
            retry_until_ms,
        })
        .await?;
    ensure!(first["value"] == 8, "initial write mismatch");
    let status = processes[original].call(Action::Status).await?;
    ensure!(
        status["ready"] == true && status["callbacks"] == 1,
        "killed node must be the owner"
    );
    ledger.phase(
        "before_fault",
        json!({"pids": pids, "owner_index": original, "receipt": first}),
    )?;
    ledger.phase(
        "fault",
        json!({"pid": processes[original].child.id(), "signal": 9}),
    )?;
    processes[original].child.kill()?;
    let status = processes[original].child.wait()?;
    use std::os::unix::process::ExitStatusExt as _;
    ensure!(
        status.signal() == Some(9),
        "original worker was not terminated by SIGKILL: {status}"
    );
    ledger.phase(
        "owner_killed",
        json!({"pid": processes[original].child.id(), "signal": status.signal()}),
    )?;
    std::fs::remove_dir_all(&configs[original].directory)?;
    ensure!(
        !configs[original].directory.exists(),
        "original local state survived erasure"
    );
    ledger.phase(
        "local_state_erased",
        json!({"directory": configs[original].directory}),
    )?;
    let source = leader(&mut processes, Some(original)).await?;
    let admitted = processes[source]
        .call(Action::BeginRepair {
            obsolete: original as u64 + 1,
        })
        .await?;
    let minimum = admitted["started_at"].as_u64().context("admission index")?;
    ledger.phase("admitted", admitted)?;
    let namespace = Catalog::new(
        store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(registration.namespace.clone())
    .await?;
    let birth = namespace.birth(CELL).await?.context("cell birth missing")?;
    let archive = namespace.cell_archive(
        &birth,
        cellar_ltx::LtxHost::default(),
        ArchiveLimits {
            max_image_bytes: quota,
            max_snapshot_bytes: 12_000_000_000,
            ..Default::default()
        },
    )?;
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if archive
            .recovery_metadata()
            .await?
            .is_some_and(|head| head.position().index.get() >= minimum)
        {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "archive did not cover repair admission"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ledger.phase("archive_ready", json!({"minimum_index": minimum}))?;
    ensure!(!configs[3].directory.exists(), "replacement is not empty");
    let mut fresh = configs[3].clone();
    fresh.source = Some(source);
    reservations[3].clear();
    ledger.phase(
        "replacement_spawn_started",
        json!({"directory": fresh.directory, "absent": true}),
    )?;
    processes.push(Process::spawn(&fresh)?);
    ensure!(
        !pids.contains(&processes[3].child.id()) && processes[3].child.id() != std::process::id(),
        "replacement reused an existing PID"
    );
    let ready = processes[3].file("ready.json", START_TIMEOUT).await?;
    ensure!(
        ready["initially_empty"] == true && ready["pid"] == processes[3].child.id(),
        "replacement identity mismatch"
    );
    ledger.phase("replacement_process_ready", ready)?;
    atomic_json(&processes[3].control.join("start.json"), &json!(true))?;
    let started = processes[3].file("started.json", START_TIMEOUT).await?;
    ensure!(
        started["from_s3"] == true,
        "replacement did not select S3 recovery"
    );
    let performance = &started["performance"];
    ensure!(
        counter(performance, "object_get_body_bytes")? >= image.length,
        "replacement did not fetch the complete image from object storage"
    );
    ensure!(
        counter(performance, "recovery_peer_received_bytes")? == 0,
        "replacement used peer image recovery"
    );
    ledger.phase("seed_installed", started)?;
    let read = processes[3]
        .call(Action::Read {
            expected: 8,
            token: None,
            full: false,
        })
        .await?;
    ledger.phase("first_read", read)?;
    for finish in [false, true] {
        let source = leader(&mut processes, Some(original)).await?;
        processes[source]
            .call(Action::AdvanceRepair { finish })
            .await?;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    while processes[3].call(Action::Status).await?["ready"] != true {
        ensure!(
            Instant::now() < deadline,
            "replacement did not become leader"
        );
        let source = leader(&mut processes, Some(original)).await?;
        if source == 3 {
            break;
        }
        let reply = processes[source].raw(Action::Transfer).await?;
        ensure!(
            reply["ok"] == true || reply["overloaded"] == true,
            "leadership transfer refused: {reply}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    ledger.phase(
        "promoted",
        json!({"pid": processes[3].child.id(), "member": 4}),
    )?;
    ledger.phase(
        "request_after_recovery",
        json!({"client": 2, "sequence": 1, "retry_until_ms": retry_until_ms, "input": 0}),
    )?;
    let next = processes[3]
        .call(Action::Command {
            client: 2,
            retry_until_ms,
        })
        .await?;
    ensure!(next["value"] == 9, "replacement durable write mismatch");
    processes[3]
        .call(Action::Read {
            expected: 9,
            token: Some(next["token"].as_str().context("version token")?.into()),
            full: false,
        })
        .await?;
    ledger.phase("first_write_verified", next.clone())?;
    for (client, receipt) in [(1, &first), (2, &next)] {
        same_receipt(
            &processes[3]
                .call(Action::Command {
                    client,
                    retry_until_ms,
                })
                .await?,
            receipt,
        )?;
    }
    ensure!(
        processes[3].call(Action::Status).await?["callbacks"] == 1,
        "replacement reexecuted a receipt"
    );
    for (index, process) in processes.iter_mut().enumerate() {
        if index != original && index != 3 {
            ensure!(
                process.call(Action::Status).await?["callbacks"] == 0,
                "survivor reexecuted a callback"
            );
            process
                .call(Action::Read {
                    expected: 9,
                    token: Some(next["token"].as_str().context("version token")?.into()),
                    full: false,
                })
                .await?;
        }
    }
    ledger.phase(
        "receipts_and_quorum_reads_verified",
        json!({"callbacks_before_kill": 1, "replacement_callbacks": 1}),
    )?;
    processes[3]
        .call(Action::Read {
            expected: 9,
            token: Some(next["token"].as_str().context("version token")?.into()),
            full: true,
        })
        .await?;
    ledger.phase(
        "payloads_verified",
        json!({"payload_bytes": payload_bytes, "sample_ids": samples.ids}),
    )?;
    for (index, process) in processes.iter_mut().enumerate() {
        if index != original {
            let closed = process.stop().await?;
            ledger.phase(
                "worker_joined",
                json!({"pid": process.child.id(), "member": index + 1, "report": closed}),
            )?;
        }
    }
    ledger.phase(
        "cleanup_joined",
        json!({"workers": 4, "hard_killed": 1, "gracefully_closed": 3}),
    )?;
    Ok(())
}
