// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical reclamation must preserve other groups and an uncommitted suffix.
use super::*;
use std::collections::BTreeMap;

fn payload(group: u8, index: u64) -> Vec<u8> {
    // Incompressible, reproducible bytes measure actual disk reclamation,
    // including engine rewrite files rather than just compressed append files.
    let mut state = (u64::from(group) << 56) ^ index ^ 0x9e3779b97f4a7c15;
    (0..4096)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn inventory(path: &Path) -> Result<BTreeMap<std::ffi::OsString, u64>> {
    let mut files = BTreeMap::new();
    for entry in std::fs::read_dir(path.join("log"))? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_some_and(|ext| ext == "raftlog" || ext == "rewrite")
        {
            match entry.metadata() {
                Ok(metadata) => {
                    files.insert(entry.file_name(), metadata.len());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(files)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_worker_reclaims_compacted_files_and_preserves_other_groups() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut configuration = options(&path, LogOpenMode::Create, 1);
    configuration.target_file_bytes = 4096;
    configuration.purge_threshold_bytes = 8192;
    // Leave durable garbage for the next owner, as after process loss between
    // checkpoint sync and physical purge. No foreground purge is requested.
    configuration.purge_interval_ms = None;
    let log = start(&domain, configuration.clone(), Arc::new(DirectFileSystem)).await?;
    let group = log.create_group(bootstrap(10)).await?;
    let other = log.create_group(bootstrap(11)).await?;
    for index in 2..=65 {
        let mut writes = Vec::new();
        for (group, byte) in [(&group, 10), (&other, 11)] {
            let mut write = GroupWrite::new(group);
            write.entries = vec![entry(index, 7, &payload(byte, index))];
            write.hard_state = Some(hard(group, 7, 2, index)?);
            writes.push(write);
        }
        log.persist(OperationId::new(index), writes).await?;
    }
    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(66, 7, b"uncommitted suffix")];
    write.applied = Some(DurableApply {
        position: LogPosition {
            index: LogIndex::new(65),
            term: Term::new(7),
        },
        image: None,
    });
    log.persist(OperationId::new(66), vec![write]).await?;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 65;
    snapshot.mut_metadata().term = 7;
    snapshot
        .mut_metadata()
        .set_conf_state(group.initial_state()?.conf_state);
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::Checkpoint(snapshot));
    log.persist(OperationId::new(67), vec![write]).await?;
    log.close().await?;
    let before = inventory(&path)?;
    let before_bytes: u64 = before.values().sum();
    ensure!(before.len() > 16 && before_bytes > 200_000);

    configuration.mode = LogOpenMode::Resume;
    configuration.process = ProcessGeneration::new([2; 16])?;
    configuration.purge_interval_ms = Some(20);
    let log = start(&domain, configuration.clone(), Arc::new(DirectFileSystem)).await?;
    let reclaimed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let after = inventory(&path)?;
            let after_bytes: u64 = after.values().sum();
            if after_bytes < before_bytes * 3 / 4
                && before.keys().any(|name| !after.contains_key(name))
            {
                break Ok::<_, anyhow::Error>((after.len(), after_bytes));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    log.close().await?;
    let (files, bytes) = reclaimed.map_err(|_| {
        anyhow::anyhow!(
            "idle worker failed to reclaim real log files: {:?}",
            inventory(&path)
        )
    })??;
    eprintln!(
        "shared Raft log: {} files/{before_bytes} bytes -> {files} files/{bytes} bytes",
        before.len()
    );
    ensure!(log
        .performance_snapshot()
        .operations
        .iter()
        .any(|op| op.name == "engine_purge" && op.succeeded > 0));

    configuration.process = ProcessGeneration::new([3; 16])?;
    let log = start(&domain, configuration, Arc::new(DirectFileSystem)).await?;
    let group = log.group(&bootstrap(10).scope)?;
    let other = log.group(&bootstrap(11).scope)?;
    ensure!(group.first_index()? == 66 && group.last_index()? == 66 && group.term(65)? == 7);
    ensure!(group.initial_state()?.hard_state == hard(&group, 7, 2, 65)?);
    ensure!(
        &group.entries(66, 67, None, GetEntriesContext::empty(false))?[0].data[..]
            == b"uncommitted suffix"
    );
    ensure!(other.first_index()? == 2 && other.last_index()? == 65);
    let entries = other.entries(2, 66, None, GetEntriesContext::empty(false))?;
    ensure!(
        entries.len() == 64
            && entries
                .iter()
                .all(|entry| entry.data.as_ref() == payload(11, entry.index))
    );
    ensure!(other.initial_state()?.hard_state == hard(&other, 7, 2, 65)?);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_purge_failure_closes_and_joins_storage_before_reopen() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    let mut configuration = options(&path, LogOpenMode::Create, 1);
    configuration.purge_interval_ms = Some(20);
    let log = start(
        &domain,
        configuration,
        Arc::new(FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let group = log.create_group(bootstrap(10)).await?;
    let _release = gate.arm(FAIL_SYNC);
    tokio::time::timeout(Duration::from_secs(5), async {
        while group.applied().is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    ensure!(matches!(log.close().await, Err(LogError::Io(_))));
    ensure!(log
        .performance_snapshot()
        .operations
        .iter()
        .any(|op| op.name == "engine_purge" && op.failed == 1));
    // The failed maintenance pass returns only after releasing the engine and
    // directory lock. Its synced group must still recover under a new owner.
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.group(&bootstrap(10).scope)?;
    ensure!(group.first_index()? == 2 && group.applied()?.position.index.get() == 1);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
