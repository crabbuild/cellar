//! Real-engine persistence, crash cuts, identity isolation and host lifetime.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_storage_fault.rs"]
mod fault;

use ::raft::prelude::{Entry, HardState, Snapshot};
use ::raft::{GetEntriesContext, Storage};
use anyhow::{bail, ensure, Result};
use cellar::application::consensus::verification::{
    DurableApply, GroupBootstrap, GroupLog, GroupWrite, LogError, LogOpenMode, ReplicaLog,
    ReplicaLogOptions, SnapshotChange,
};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_ltx::{DirectFileSystem, FileSystem};
use fault::*;
use protobuf::Message as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn options(path: &Path, mode: LogOpenMode, process: u8) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        path.to_owned(),
        NodeId::new([5; 16]).unwrap(),
        DiskLineage::new([6; 16]).unwrap(),
        ProcessGeneration::new([process; 16]).unwrap(),
        mode,
    )
}

fn bootstrap(id: u8) -> GroupBootstrap {
    GroupBootstrap {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: format!("Counter:{id}"),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        member: MemberId::new(1).unwrap(),
        birth_digest: [9; 32],
        voters: (1..=3).map(|id| MemberId::new(id).unwrap()).collect(),
        learners: Vec::new(),
    }
}

fn entry(index: u64, term: u64, payload: &[u8]) -> Entry {
    Entry {
        index,
        term,
        data: payload.to_vec().into(),
        ..Default::default()
    }
}

fn hard(group: &GroupLog, term: u64, vote: u64, commit: u64) -> Result<HardState> {
    let mut hard = group.initial_state()?.hard_state;
    hard.term = term;
    hard.vote = vote;
    hard.commit = commit;
    Ok(hard)
}

async fn start(
    domain: &ExecutionDomain,
    options: ReplicaLogOptions,
    fs: Arc<dyn FileSystem>,
) -> Result<ReplicaLog> {
    Ok(domain.run(ReplicaLog::open(options, fs)).await??)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_survives_restart() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.create_group(bootstrap(10)).await?;
    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(hard(&group, 7, 2, 1)?);
    let receipt = log.persist(OperationId::new(1), vec![write]).await?;
    assert_eq!(receipt.operation, OperationId::new(1));
    assert_eq!(receipt.groups, vec![group.identity().clone()]);
    log.close().await?;
    assert!(group.initial_state().is_err());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.group(&bootstrap(10).scope)?;
    assert_eq!(group.initial_state()?.hard_state.vote, 2);
    assert_eq!(group.initial_state()?.hard_state.term, 7);
    let mut conflict = GroupWrite::new(&group);
    conflict.hard_state = Some(hard(&group, 7, 3, 1)?);
    assert!(matches!(
        log.persist(OperationId::new(2), vec![conflict]).await,
        Err(LogError::Invalid(_))
    ));
    assert_eq!(group.initial_state()?.hard_state.vote, 2);
    log.close().await?;
    domain.shutdown().await;

    for cut in ["before-sync", "after-sync", "after-reply"] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("node");
        seed(&path).await?;
        kill_at_cut(&path, cut).await?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = start(
            &domain,
            options(&path, LogOpenMode::Resume, 3),
            Arc::new(DirectFileSystem),
        )
        .await?;
        let group = log.group(&bootstrap(10).scope)?;
        let state = group.initial_state()?.hard_state;
        assert_eq!(state.vote, 2, "a process kill must not reset a stable vote");
        if cut == "before-sync" {
            assert!([7, 9].contains(&state.term));
        } else {
            assert_eq!(state.term, 9);
        }
        if state.term == 9 {
            assert_eq!(state.commit, 2);
            assert_eq!(group.last_index()?, 2);
            assert_eq!(
                &group.entries(2, 3, None, GetEntriesContext::empty(false))?[0].data[..],
                b"committed-cut"
            );
        } else {
            assert_eq!(state.commit, 1);
            assert_eq!(group.last_index()?, 1);
        }
        log.close().await?;
        domain.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_sync_boundary() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    let fs = Arc::new(FaultFileSystem { gate: gate.clone() });
    let log = start(
        &domain,
        options(&directory.path().join("node"), LogOpenMode::Create, 1),
        fs,
    )
    .await?;
    let group = log.create_group(bootstrap(10)).await?;
    let release = gate.arm(HOLD_SYNC);
    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(2, 2, b"uncommitted")];
    write.hard_state = Some(hard(&group, 2, 1, 2)?);
    let selected = log.clone();
    let mut pending =
        domain.spawn(async move { selected.persist(OperationId::new(1), vec![write]).await })?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    assert_eq!(
        group.last_index()?,
        1,
        "a stalled append cannot publish its cache"
    );
    assert_eq!(group.initial_state()?.hard_state.commit, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    drop(release);
    pending.await??;
    assert_eq!(group.last_index()?, 2);
    assert_eq!(group.initial_state()?.hard_state.commit, 2);
    let _failure = gate.arm(FAIL_SYNC);
    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(hard(&group, 3, 2, 2)?);
    assert!(matches!(
        log.persist(OperationId::new(2), vec![write]).await,
        Err(LogError::Io(_))
    ));
    assert!(
        group.initial_state().is_err(),
        "a failed sync quarantines cached reads as well"
    );
    assert!(matches!(
        log.persist(OperationId::new(3), Vec::new()).await,
        Err(LogError::Closed)
    ));
    domain.shutdown().await;

    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    let _failure = gate.arm(FAIL_DIRECTORY_SYNC);
    let result = start(
        &domain,
        options(&directory.path().join("node"), LogOpenMode::Create, 1),
        Arc::new(FaultFileSystem { gate }),
    )
    .await;
    assert!(
        result.is_err(),
        "initial queue directory sync cannot be bypassed"
    );
    domain.shutdown().await;
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    *gate.sync_path.lock().unwrap() = Some(directory.path().to_owned());
    let _failure = gate.arm(FAIL_PARENT_SYNC);
    let result = start(
        &domain,
        options(
            &directory.path().join("new/parent/node"),
            LogOpenMode::Create,
            1,
        ),
        Arc::new(FaultFileSystem { gate }),
    )
    .await;
    assert!(
        result.is_err(),
        "new directory ancestry must survive a parent-directory sync boundary"
    );
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_suffix_replaced() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.create_group(bootstrap(10)).await?;
    let mut write = GroupWrite::new(&group);
    write.entries = vec![
        entry(2, 2, b""),
        entry(3, 2, b"old"),
        entry(4, 2, b"discard"),
    ];
    write.hard_state = Some(hard(&group, 2, 1, 2)?);
    log.persist(OperationId::new(1), vec![write]).await?;
    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(3, 3, b"new")];
    write.hard_state = Some(hard(&group, 3, 2, 2)?);
    log.persist(OperationId::new(2), vec![write]).await?;
    assert_eq!(group.last_index()?, 3);
    let mut forbidden = GroupWrite::new(&group);
    forbidden.entries = vec![entry(2, 3, b"changed committed history")];
    assert!(matches!(
        log.persist(OperationId::new(3), vec![forbidden]).await,
        Err(LogError::Invalid(_))
    ));
    let mut write = GroupWrite::new(&group);
    write.applied = Some(DurableApply {
        position: LogPosition {
            index: LogIndex::new(2),
            term: Term::new(2),
        },
        image: None,
    });
    log.persist(OperationId::new(4), vec![write]).await?;
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 2;
    snapshot.mut_metadata().term = 2;
    snapshot
        .mut_metadata()
        .set_conf_state(group.initial_state()?.conf_state);
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::Checkpoint(snapshot));
    log.persist(OperationId::new(5), vec![write]).await?;
    log.purge().await?;
    log.close().await?;
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.group(&bootstrap(10).scope)?;
    assert_eq!(group.first_index()?, 3);
    assert_eq!(group.term(2)?, 2);
    assert!(group.term(1).is_err());
    assert_eq!(group.last_index()?, 3);
    let entries = group.entries(3, 4, Some(0), GetEntriesContext::empty(false))?;
    assert_eq!(
        entries.len(),
        1,
        "size zero must still yield the first available entry"
    );
    assert_eq!(&entries[0].data[..], b"new");
    assert!(group
        .entries(4, 5, None, GetEntriesContext::empty(false))
        .is_err());
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_group_isolation() -> Result<()> {
    assert!(
        Entry::parse_from_bytes(&[0x08, 99]).is_err(),
        "unknown entry enums retain the upstream strict decoder contract"
    );
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let mut groups = Vec::new();
    for id in 10..13 {
        groups.push(log.create_group(bootstrap(id)).await?);
    }
    assert_eq!(
        groups.iter().map(GroupLog::numeric_key).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(log.create_group(bootstrap(10)).await?.numeric_key(), 1);
    let writes = groups
        .iter()
        .enumerate()
        .map(|(id, group)| {
            let mut write = GroupWrite::new(group);
            write.entries = vec![entry(2, 2, &[id as u8])];
            write.hard_state = Some(hard(group, 2, 1, 2).unwrap());
            write
        })
        .collect();
    log.persist(OperationId::new(1), writes).await?;
    let other = start(
        &domain,
        options(&directory.path().join("other"), LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let foreign = other.create_group(bootstrap(10)).await?;
    assert!(matches!(
        log.persist(OperationId::new(2), vec![GroupWrite::new(&foreign)])
            .await,
        Err(LogError::WrongIdentity)
    ));
    other.close().await?;
    let mut alias = bootstrap(10);
    alias.scope.cell = "Counter:forged-alias".into();
    assert!(matches!(
        log.create_group(alias).await,
        Err(LogError::WrongIdentity)
    ));
    log.close().await?;
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    for (id, key) in [(10, 1), (11, 2), (12, 3)] {
        let group = log.group(&bootstrap(id).scope)?;
        assert_eq!(group.numeric_key(), key);
        assert_eq!(
            &group.entries(2, 3, None, GetEntriesContext::empty(false))?[0].data[..],
            &[id - 10]
        );
    }
    assert_eq!(log.create_group(bootstrap(13)).await?.numeric_key(), 4);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_loss_identity() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    seed(&path).await?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut wrong = options(&path, LogOpenMode::Resume, 2);
    wrong.disk = DiskLineage::new([55; 16])?;
    assert!(matches!(
        domain
            .run(ReplicaLog::open(wrong, Arc::new(DirectFileSystem)))
            .await?,
        Err(LogError::WrongIdentity)
    ));
    assert!(matches!(
        domain
            .run(ReplicaLog::open(
                options(&path, LogOpenMode::Create, 2),
                Arc::new(DirectFileSystem)
            ))
            .await?,
        Err(LogError::AlreadyExists)
    ));
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert!(
        domain
            .run(ReplicaLog::open(
                options(&path, LogOpenMode::Resume, 3),
                Arc::new(DirectFileSystem)
            ))
            .await?
            .is_err(),
        "directory ownership must exclude a second engine"
    );
    log.close().await?;
    std::fs::remove_dir_all(path.join("log"))?;
    assert!(matches!(
        domain
            .run(ReplicaLog::open(
                options(&path, LogOpenMode::Resume, 3),
                Arc::new(DirectFileSystem)
            ))
            .await?,
        Err(LogError::LostStableState)
    ));
    std::fs::create_dir(path.join("log"))?;
    assert!(matches!(
        domain
            .run(ReplicaLog::open(
                options(&path, LogOpenMode::Resume, 3),
                Arc::new(DirectFileSystem)
            ))
            .await?,
        Err(LogError::LostStableState)
    ));
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_storage_refused() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    seed(&path).await?;
    kill_at_cut(&path, "torn-write").await?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(ReplicaLog::open(
            options(&path, LogOpenMode::Resume, 3),
            Arc::new(DirectFileSystem),
        ))
        .await?;
    assert!(
        matches!(result, Err(LogError::Corrupt(_))),
        "a torn frame cannot become an empty voter"
    );
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn birth_sync_retry_reuses_numeric_key() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    log.close().await?;
    domain.shutdown().await;
    kill_at_cut(&path, "birth-after-sync").await?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 3),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_eq!(log.groups()?.len(), 1);
    assert_eq!(log.create_group(bootstrap(11)).await?.numeric_key(), 1);
    assert_eq!(log.create_group(bootstrap(12)).await?.numeric_key(), 2);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_metadata_survives_process_kill() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    seed(&path).await?;
    kill_at_cut(&path, "snapshot-after-sync").await?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 3),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.group(&bootstrap(10).scope)?;
    let state = group.initial_state()?;
    assert_eq!(state.hard_state.term, 9);
    assert_eq!(state.hard_state.commit, 8);
    assert_eq!(state.conf_state.learners, vec![4]);
    assert_eq!(group.first_index()?, 9);
    assert_eq!(group.last_index()?, 9);
    assert_eq!(group.term(8)?, 7);
    assert_eq!(group.applied()?.position.index.get(), 8);
    assert_eq!(
        group
            .snapshot(8, 1)?
            .get_metadata()
            .get_conf_state()
            .learners,
        vec![4]
    );
    assert_eq!(
        &group.entries(9, 10, None, GetEntriesContext::empty(false))?[0].data[..],
        b"snapshot-tail"
    );
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_purge_preserves_group() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut configuration = options(&path, LogOpenMode::Create, 1);
    configuration.target_file_bytes = 4096;
    configuration.purge_threshold_bytes = 8192;
    let log = start(&domain, configuration, Arc::new(DirectFileSystem)).await?;
    let group = log.create_group(bootstrap(10)).await?;
    for index in 2..=17 {
        let mut write = GroupWrite::new(&group);
        write.entries = vec![entry(index, 7, &vec![index as u8; 4096])];
        write.hard_state = Some(hard(&group, 7, 2, index)?);
        log.persist(OperationId::new(index), vec![write]).await?;
    }
    log.close().await?;
    domain.shutdown().await;
    kill_at_cut(&path, "purge-delete").await?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 3),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.group(&bootstrap(10).scope)?;
    assert_eq!(group.initial_state()?.hard_state.commit, 17);
    let entries = group.entries(2, 18, None, GetEntriesContext::empty(false))?;
    assert_eq!(entries.len(), 16);
    for entry in entries {
        assert!(entry.data.iter().all(|&byte| byte == entry.index as u8));
    }
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_budget_and_domain_close() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut configuration = options(&directory.path().join("node"), LogOpenMode::Create, 1);
    configuration.max_entries_per_group = 1;
    configuration.max_groups = 1;
    let log = start(&domain, configuration, Arc::new(DirectFileSystem)).await?;
    let group = log.create_group(bootstrap(10)).await?;
    assert!(matches!(
        log.create_group(bootstrap(11)).await,
        Err(LogError::Overloaded)
    ));
    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(2, 2, b"first"), entry(3, 2, b"exceeds bound")];
    write.hard_state = Some(hard(&group, 2, 1, 1)?);
    assert!(matches!(
        log.persist(OperationId::new(1), vec![write]).await,
        Err(LogError::Invalid(_))
    ));
    assert_eq!(group.last_index()?, 1);
    tokio::time::timeout(Duration::from_secs(5), domain.shutdown()).await?;
    assert!(matches!(log.groups(), Err(LogError::Closed)));
    assert!(group.last_index().is_err());
    // Retained public handles cannot keep a shutdown engine or directory lock.
    let next = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &next,
        options(&directory.path().join("node"), LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    log.close().await?;
    next.shutdown().await;
    Ok(())
}

async fn seed(path: &Path) -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let group = log.create_group(bootstrap(10)).await?;
    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(hard(&group, 7, 2, 1)?);
    log.persist(OperationId::new(1), vec![write]).await?;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn kill_at_cut(path: &Path, cut: &str) -> Result<()> {
    let signal = path.parent().unwrap().join(format!("{cut}.signal"));
    let output = path.parent().unwrap().join(format!("{cut}.log"));
    let file = std::fs::File::create(&output)?;
    let mut child = Child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "storage_process_fixture",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("CELLAR_RAFT_TEST_ROOT", path)
            .env("CELLAR_RAFT_TEST_CUT", cut)
            .env("CELLAR_RAFT_TEST_SIGNAL", &signal)
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        while !signal.exists() {
            if let Some(status) = child.0.try_wait()? {
                bail!(
                    "child exited before {cut}: {status}; {}",
                    std::fs::read_to_string(&output)?
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    child.0.kill()?;
    let status = child.0.wait()?;
    ensure!(
        !status.success(),
        "fixture must be killed, not shut down normally"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "process-kill fixture, launched only by parent persistence tests"]
async fn storage_process_fixture() -> Result<()> {
    let path = std::path::PathBuf::from(std::env::var("CELLAR_RAFT_TEST_ROOT")?);
    let cut = std::env::var("CELLAR_RAFT_TEST_CUT")?;
    let signal = std::path::PathBuf::from(std::env::var("CELLAR_RAFT_TEST_SIGNAL")?);
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(Some(signal.clone()));
    let mut configuration = options(&path, LogOpenMode::Resume, 2);
    if cut == "purge-delete" {
        configuration.target_file_bytes = 4096;
        configuration.purge_threshold_bytes = 8192;
    }
    let log = start(
        &domain,
        configuration,
        Arc::new(FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    if cut == "birth-after-sync" {
        let _release = gate.arm(CRASH_AFTER_SYNC);
        log.create_group(bootstrap(11)).await?;
        bail!("birth sync gate did not hold");
    }
    let group = log.group(&bootstrap(10).scope)?;
    let mode = match cut.as_str() {
        "before-sync" => CRASH_BEFORE_SYNC,
        "after-sync" | "snapshot-after-sync" => CRASH_AFTER_SYNC,
        "after-reply" => OFF,
        "torn-write" => TORN_WRITE,
        "purge-delete" => CRASH_AFTER_DELETE,
        _ => bail!("unknown crash cut"),
    };
    let _release = gate.arm(mode);
    if cut == "purge-delete" {
        log.purge().await?;
        bail!("purge did not reach an actual obsolete-file deletion");
    } else {
        let mut write = GroupWrite::new(&group);
        if cut == "snapshot-after-sync" {
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = 8;
            snapshot.mut_metadata().term = 7;
            let mut conf = group.initial_state()?.conf_state;
            conf.learners.push(4);
            snapshot.mut_metadata().set_conf_state(conf);
            write.snapshot = Some(SnapshotChange::Install {
                snapshot,
                applied: DurableApply {
                    position: LogPosition {
                        index: LogIndex::new(8),
                        term: Term::new(7),
                    },
                    image: None,
                },
            });
            write.entries = vec![entry(9, 9, b"snapshot-tail")];
            write.hard_state = Some(hard(&group, 9, 2, 8)?);
        } else {
            write.entries = vec![entry(2, 9, b"committed-cut")];
            write.hard_state = Some(hard(&group, 9, 2, 2)?);
        }
        log.persist(OperationId::new(2), vec![write]).await?;
    }
    std::fs::write(&signal, b"reply observed")?;
    std::fs::File::open(&signal)?.sync_all()?;
    loop {
        std::thread::park();
    }
}
