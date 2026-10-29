// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! All original OS processes and their private disks are retired before the
//! operator runs. The only application-state source for the fresh quorum is S3.
use super::controller::{counter, leader, same_receipt, Ledger, Process};
use super::*;
use futures_util::future::BoxFuture;
use std::{os::unix::process::ExitStatusExt, sync::Mutex};

/// The fixture controller is the sole launcher of these synthetic replicas.
/// It retains joined Child handles and never exposes a restart operation. This
/// models an infrastructure fence; production uses its own deployment authority.
struct RetiredFleet {
    processes: Mutex<Vec<Process>>,
    directories: Vec<PathBuf>,
}

impl RetiredFleet {
    fn check(&self, request: &DisasterFenceRequest) -> Result<Vec<u8>> {
        let mut processes = self
            .processes
            .lock()
            .map_err(|_| anyhow::anyhow!("fence lock"))?;
        ensure!(
            processes.len() == 3 && self.directories.len() == 3,
            "incomplete retired fleet"
        );
        let mut receipt = request.digest().to_vec();
        for (process, directory) in processes.iter_mut().zip(&self.directories) {
            ensure!(
                process
                    .child
                    .try_wait()?
                    .is_some_and(|exit| exit.signal() == Some(9)),
                "old replica still runnable"
            );
            ensure!(!directory.exists(), "old replica disk remains");
            receipt.extend_from_slice(&process.child.id().to_be_bytes());
        }
        Ok(receipt)
    }
}

impl DisasterFenceAuthority for RetiredFleet {
    fn fence<'a>(&'a self, request: &'a DisasterFenceRequest) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.check(request) })
    }

    fn verify<'a>(
        &'a self,
        request: &'a DisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ensure!(
                self.check(request)? == evidence,
                "retirement evidence changed"
            );
            Ok(())
        })
    }
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
    let prefix = format!("full-quorum-recovery/{:032x}", rand::random::<u128>());
    eprintln!(
        "process-recovery directory={} object_prefix={prefix}",
        directory.display()
    );
    let mut ledger = Ledger {
        path: directory.join("evidence.json"),
        started: Instant::now(),
        value: json!({"format":1,"failure_mode":"full_quorum_sigkill","backend":"rustfs","storage":"extents",
            "quota":quota,"directory":directory,"prefix":prefix,"complete":false,"events":[]}),
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

fn peers(count: usize) -> Result<(Vec<Peer>, Vec<Vec<std::net::TcpListener>>)> {
    let mut peers = Vec::new();
    let mut reservations = Vec::new();
    for _ in 0..count {
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
    Ok((peers, reservations))
}

async fn wait_archive(process: &mut Process, minimum: u64) -> Result<Value> {
    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        let status = process.call(Action::Status).await?;
        if status["archived"]
            .as_u64()
            .is_some_and(|index| index >= minimum)
            && status["archive"]["readiness"] == "current"
        {
            return Ok(status);
        }
        ensure!(
            Instant::now() < deadline,
            "archive did not catch up: {status}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn drill(directory: &Path, quota: u64, prefix: &str, ledger: &mut Ledger) -> Result<()> {
    let preparation = directory.join("preparation");
    std::fs::create_dir(&preparation)?;
    let source = if quota == 10_000_000_000 {
        let source = PathBuf::from(
            std::env::var_os("CELLAR_LARGE_IMAGE_FIXTURE").context("10 GB source required")?,
        );
        ensure!(
            source.metadata()?.permissions().readonly()
                && source.metadata()?.len() == quota / 4096 * 4096,
            "fixture size/permissions"
        );
        source
    } else {
        fixture::make_template(&preparation, quota, true)?
    };
    let reserve = (quota / 16).min(32 * 1024 * 1024) / 512 * 512;
    let (template, image, payload_bytes, samples) =
        fixture::prepared_template(&source, &preparation, quota, reserve)?;
    ledger.value["image_bytes"] = image.length.into();
    let definition = fixture::recovery_definition(Arc::new(AtomicUsize::new(0)))?.0;
    let registration = registration(definition.schema)?;
    let store = store(prefix)?;
    let (old_peers, mut old_reservations) = peers(4)?;
    let mut configs = (0..3)
        .map(|index| Config {
            index,
            disaster: false,
            retained: false,
            generation: 0,
            quota,
            prefix: prefix.into(),
            directory: directory.join(format!("original-{index}")),
            control: directory.join(format!("control-original-{index}")),
            peers: old_peers.clone(),
            source: None,
            payload_bytes,
            sample_ids: samples.ids,
            sample_hashes: samples.hashes,
        })
        .collect::<Vec<_>>();
    let mut processes = Vec::new();
    for index in 0..3 {
        old_reservations[index].clear();
        processes.push(Process::spawn(&configs[index])?);
    }
    let mut logs = Vec::new();
    let mut old_pids = Vec::new();
    for (index, process) in processes.iter_mut().enumerate() {
        let ready = process.file("ready.json", START_TIMEOUT).await?;
        ensure!(
            ready["pid"] == process.child.id() && ready["initially_empty"] == true,
            "original identity"
        );
        old_pids.push(process.child.id());
        let data = PathBuf::from(ready["data_dir"].as_str().context("data directory")?);
        ensure!(
            data.starts_with(&configs[index].directory),
            "worker data escaped directory"
        );
        let peer = configs[index].native(index)?;
        logs.push(ReplicaLogOptions::new(
            data.join("raft"),
            peer.node,
            peer.disk,
            peer.process,
            LogOpenMode::Create,
        ));
    }
    fixture::preload(template, image, registration.clone(), quota)(logs, store.clone()).await?;
    std::fs::remove_dir_all(&preparation)?;
    for process in &processes {
        atomic_json(&process.control.join("start.json"), &true)?;
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
    let first = processes[original]
        .call(Action::Command {
            client: 1,
            retry_until_ms,
        })
        .await?;
    ensure!(
        first["value"] == 8 && first["durability"] == "quorum",
        "original write"
    );
    ledger.phase(
        "quorum_write_before_loss",
        json!({"pids":old_pids,"receipt":first}),
    )?;
    let ready = wait_archive(
        &mut processes[original],
        first["index"].as_u64().context("receipt index")?,
    )
    .await?;
    let limits = ArchiveLimits {
        max_image_bytes: quota,
        max_snapshot_bytes: 12_000_000_000,
        ..Default::default()
    };
    let operator = S3DisasterRecovery::open(
        store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
        registration.namespace.clone(),
        cellar_ltx::LtxHost::default(),
        limits,
    )
    .await?;
    let reference = operator
        .archive_reference(&registration.birth.scope)
        .await?;
    ensure!(
        reference.position()?.index.get() >= first["index"].as_u64().context("index")?,
        "archive omits acknowledged write"
    );
    ledger.phase(
        "archive_current_before_loss",
        json!({"status":ready,"reference":reference}),
    )?;
    ledger.phase("fault", json!({"pids":old_pids,"signal":9}))?;
    for process in &mut processes {
        process.child.kill()?;
    }
    for (process, config) in processes.iter_mut().zip(&configs) {
        let exit = process.child.wait()?;
        ensure!(
            exit.signal() == Some(9),
            "old worker did not die by SIGKILL"
        );
        std::fs::remove_dir_all(&config.directory)?;
        ensure!(!config.directory.exists(), "old disk survived");
        ledger.phase("replica_killed_and_erased", json!({"pid":process.child.id(),"signal":exit.signal(),"directory":config.directory,"absent":true}))?;
    }
    ensure!(!preparation.exists(), "fixture preparation survived");
    let retired = RetiredFleet {
        processes: Mutex::new(processes),
        directories: configs
            .iter()
            .map(|config| config.directory.clone())
            .collect(),
    };
    let request = DisasterRecoveryRequest {
        source: registration.birth.scope.clone(),
        archive: reference,
        replacement: replacement(&registration.birth)?,
    };
    let fence = operator.fence_request(request.clone()).await?;
    ledger.phase(
        "disaster_preflight_started",
        json!({"fence_digest":hex::encode(fence.digest())}),
    )?;
    let birth = operator.recover(request, &retired).await?;
    ledger.phase("new_history_committed", json!({"group":hex::encode(birth.scope.group.bytes()),"incarnation":hex::encode(birth.scope.incarnation.bytes())}))?;
    let (new_peers, mut reserved) = peers(3)?;
    for (index, config) in configs.iter_mut().enumerate() {
        config.disaster = true;
        config.peers = new_peers.clone();
        config.directory = directory.join(format!("fresh-{index}"));
        config.control = directory.join(format!("control-fresh-{index}"));
    }
    ledger.phase("fresh_quorum_spawn_started", json!({"directories":configs.iter().map(|config| &config.directory).collect::<Vec<_>>(),"all_absent":configs.iter().all(|config| !config.directory.exists())}))?;
    let mut fresh = Vec::new();
    for index in 0..3 {
        reserved[index].clear();
        fresh.push(Process::spawn(&configs[index])?);
    }
    let fresh_pids = fresh.iter().map(|p| p.child.id()).collect::<Vec<_>>();
    ensure!(
        fresh_pids
            .iter()
            .all(|pid| !old_pids.contains(pid) && *pid != std::process::id()),
        "process identity reused"
    );
    for process in &mut fresh {
        let ready = process.file("ready.json", START_TIMEOUT).await?;
        ensure!(
            ready["initially_empty"] == true && ready["pid"] == process.child.id(),
            "fresh quorum not empty"
        );
        ledger.phase("fresh_process_ready", ready)?;
        atomic_json(&process.control.join("start.json"), &true)?;
    }
    for process in &mut fresh {
        let started = process.file("started.json", START_TIMEOUT).await?;
        ensure!(started["from_s3"] == true, "not a disaster seed");
        ensure!(
            counter(&started["performance"], "object_get_body_bytes")? >= image.length,
            "incomplete S3 body reads"
        );
        ensure!(
            counter(&started["performance"], "recovery_peer_received_bytes")? == 0,
            "peer supplied image"
        );
        ledger.phase("fresh_seed_installed", started)?;
    }
    let owner = leader(&mut fresh, None).await?;
    for process in &mut fresh {
        let read = process
            .call(Action::Read {
                expected: 8,
                token: None,
                full: false,
            })
            .await?;
        ledger.phase("restored_read", read)?;
    }
    let stale = fresh[owner]
        .raw(Action::Read {
            expected: 8,
            token: Some(first["token"].as_str().context("old token")?.into()),
            full: false,
        })
        .await?;
    ensure!(
        stale["ok"] == false && stale["wrong_incarnation"] == true,
        "old history token accepted: {stale}"
    );
    let duplicate = fresh[owner]
        .call(Action::Command {
            client: 1,
            retry_until_ms,
        })
        .await?;
    ensure!(
        duplicate["value"] == 8
            && duplicate["group"] != first["group"]
            && duplicate["incarnation"] != first["incarnation"],
        "old receipt not rebound"
    );
    ensure!(
        fresh[owner].call(Action::Status).await?["callbacks"] == 0,
        "old callback replayed"
    );
    ledger.phase(
        "old_receipt_and_token_verified",
        json!({"receipt":duplicate,"wrong_incarnation":true,"callbacks":0}),
    )?;
    let second = fresh[owner]
        .call(Action::Command {
            client: 2,
            retry_until_ms,
        })
        .await?;
    ensure!(
        second["value"] == 9 && second["durability"] == "quorum",
        "new quorum write"
    );
    ledger.phase("first_write_verified", second.clone())?;
    let again = fresh[owner]
        .call(Action::Command {
            client: 2,
            retry_until_ms,
        })
        .await?;
    same_receipt(&again, &second)?;
    for process in &mut fresh {
        process
            .call(Action::Read {
                expected: 9,
                token: Some(second["token"].as_str().context("new token")?.into()),
                full: true,
            })
            .await?;
    }
    ensure!(
        fresh[owner].call(Action::Status).await?["callbacks"] == 1,
        "new callback duplicated"
    );
    ledger.phase("quorum_payloads_and_receipts_verified", json!({"workers":3,"callbacks":1,"payload_bytes":payload_bytes,"sample_ids":samples.ids,"sample_hashes":samples.hashes}))?;
    let status = wait_archive(
        &mut fresh[owner],
        second["index"].as_u64().context("new index")?,
    )
    .await?;
    ledger.phase("new_history_archive_current", status)?;
    for process in &mut fresh {
        let report = process.stop().await?;
        ledger.phase(
            "fresh_worker_joined",
            json!({"pid":process.child.id(),"report":report}),
        )?;
    }
    drop(fresh);
    let mut resumed = Vec::new();
    for config in &mut configs {
        config.retained = true;
        config.generation = 1;
        config.control = directory.join(format!("control-resumed-{}", config.index));
        resumed.push(Process::spawn(config)?);
    }
    for process in &mut resumed {
        let ready = process.file("ready.json", START_TIMEOUT).await?;
        ensure!(
            ready["initially_empty"] == false,
            "restart lost retained disks"
        );
        atomic_json(&process.control.join("start.json"), &true)?;
    }
    for process in &mut resumed {
        process.file("started.json", START_TIMEOUT).await?;
    }
    let owner = leader(&mut resumed, None).await?;
    let receipt = resumed[owner]
        .call(Action::Command {
            client: 2,
            retry_until_ms,
        })
        .await?;
    ensure!(
        receipt["value"] == 9 && receipt["group"] == second["group"],
        "restart lost receipt"
    );
    for process in &mut resumed {
        process
            .call(Action::Read {
                expected: 9,
                token: Some(second["token"].as_str().context("token")?.into()),
                full: false,
            })
            .await?;
        ensure!(
            process.call(Action::Status).await?["callbacks"] == 0,
            "restart repeated callback"
        );
    }
    ledger.phase("restart_reads_and_receipts_verified", json!({"pids":resumed.iter().map(|p|p.child.id()).collect::<Vec<_>>(),"value":9,"callbacks":0}))?;
    for process in &mut resumed {
        let report = process.stop().await?;
        ledger.phase(
            "resumed_worker_joined",
            json!({"pid":process.child.id(),"report":report}),
        )?;
    }
    retired.check(&fence)?;
    ledger.phase(
        "cleanup_joined",
        json!({"workers":9,"hard_killed":3,"gracefully_closed":6}),
    )?;
    Ok(())
}
