// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{
    cache::{DurableApply, GroupWrite, SnapshotChange},
    options::{LogOpenMode, ReplicaLogOptions},
    storage::{GroupBootstrap, ReplicaLog},
};
use ::raft::prelude::{Entry, EntryType, HardState, Snapshot};
use cellar_logic::consensus::*;
use std::sync::Arc;
use std::time::Duration;

use super::super::storage_fault as fault;

fn options(path: &std::path::Path) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        path.join("log"),
        NodeId::new([1; 16]).unwrap(),
        DiskLineage::new([2; 16]).unwrap(),
        ProcessGeneration::new([3; 16]).unwrap(),
        LogOpenMode::Create,
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
        voters: vec![MemberId::new(1).unwrap()],
        learners: vec![],
    }
}

fn entry(index: u64, term: u64, data: &[u8]) -> Entry {
    Entry {
        index,
        term,
        data: data.to_vec().into(),
        ..Default::default()
    }
}

fn reference(group: &GroupLog, archived: LogIndex) -> anyhow::Result<Usage> {
    let cache = group.read()?;
    let mut expected = Usage::default();
    if cache.applied.position.index > archived {
        expected.bytes = cache.applied.image.as_ref().map_or(0, |image| image.length);
    }
    for entry in cache
        .entries
        .values()
        .filter(|entry| entry.index > archived.get().max(1))
    {
        expected.bytes += u64::from(protobuf::Message::compute_size(entry)) + 128;
        expected.entries +=
            u64::from(entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty());
    }
    Ok(expected)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unchanged_durable_history_does_not_reenter_group_cache() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.create_group(bootstrap(1)).await?;
    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(2, 1, b"retained command")];
    log.persist(OperationId::new(1), vec![write]).await?;
    let mut retained = Retained::default();
    let expected = retained.observe(&group, LogIndex::new(0))?;
    assert_eq!(expected.entries, 1);
    assert_eq!(expected, reference(&group, LogIndex::new(0))?);

    // An unchanged accounting lookup must not revisit the log cache. Hold its
    // write lock on a domain-owned blocking worker to make a second scan fail
    // deterministically; release and join both workers before asserting.
    let locked_group = group.clone();
    let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = domain.blocking(move || {
        let guard = locked_group.cache.write().unwrap();
        let _ = locked_tx.send(());
        let _ = release_rx.recv();
        drop(guard);
    })?;
    locked_rx.await?;
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let reader = domain.blocking(move || {
        let result = retained.observe(&group, LogIndex::new(0));
        let _ = done_tx.send(result);
    })?;
    let early = domain
        .run(crate::asyncrt::timeout(Duration::from_secs(1), done_rx))
        .await?;
    release_tx.send(())?;
    holder.await?;
    reader.await?;
    log.close().await?;
    domain.shutdown().await;
    assert!(
        early.is_ok(),
        "unchanged admission accounting reentered the group cache"
    );
    assert_eq!(early???, expected);
    Ok(())
}

fn image(length: u64) -> crate::raft::cache::CommittedImage {
    crate::raft::cache::CommittedImage {
        name: format!("image-{length}"),
        commitment: [4; 32].into(),
        length,
        ltx_epoch: 1,
        txid: 1,
        checksum: 1,
        page_size: 4096,
    }
}

fn snapshot(group: &GroupLog, index: u64, term: u64) -> anyhow::Result<Snapshot> {
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = index;
    snapshot.mut_metadata().term = term;
    snapshot
        .mut_metadata()
        .set_conf_state(group.read()?.conf_state.clone());
    Ok(snapshot)
}

fn check(
    retained: &mut Retained,
    group: &GroupLog,
    archived: u64,
    entries: u64,
) -> anyhow::Result<Usage> {
    let archived = LogIndex::new(archived);
    let usage = retained.observe(group, archived)?;
    assert_eq!(usage, reference(group, archived)?);
    assert_eq!(usage.entries, entries);
    assert_eq!(retained.observe(group, archived)?, usage);
    Ok(usage)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_accounting_tracks_replacement_images_snapshots_and_archive_progress(
) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.create_group(bootstrap(1)).await?;
    let mut retained = Retained::default();
    assert_eq!(check(&mut retained, &group, 0, 0)?.bytes, 0);

    let mut protocol = entry(4, 2, b"configuration bytes");
    protocol.set_entry_type(EntryType::EntryConfChangeV2);
    let mut write = GroupWrite::new(&group);
    write.entries = vec![
        entry(2, 2, b""),
        entry(3, 2, b"old"),
        protocol,
        entry(5, 2, b"discard"),
    ];
    write.hard_state = Some(HardState {
        term: 2,
        vote: 1,
        commit: 2,
        ..Default::default()
    });
    log.persist(OperationId::new(1), vec![write]).await?;
    let before = check(&mut retained, &group, 0, 2)?;
    // Empty/protocol entries consume bytes without consuming data-entry slots.
    assert!(before.bytes > 4 * 128);

    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(3, 3, b"new"), entry(4, 3, b"last")];
    write.hard_state = Some(HardState {
        term: 3,
        vote: 2,
        commit: 2,
        ..Default::default()
    });
    log.persist(OperationId::new(2), vec![write]).await?;
    let replaced = check(&mut retained, &group, 0, 2)?;
    assert!(replaced.bytes < before.bytes);

    let mut forbidden = GroupWrite::new(&group);
    forbidden.entries = vec![entry(2, 3, b"changed committed entry")];
    assert!(matches!(
        log.persist(OperationId::new(3), vec![forbidden]).await,
        Err(LogError::Invalid(_))
    ));
    assert_eq!(check(&mut retained, &group, 0, 2)?, replaced);

    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(HardState {
        term: 3,
        vote: 2,
        commit: 4,
        ..Default::default()
    });
    write.applied = Some(DurableApply {
        position: LogPosition {
            index: LogIndex::new(4),
            term: Term::new(3),
        },
        image: Some(image(8192)),
    });
    log.persist(OperationId::new(4), vec![write]).await?;
    assert_eq!(
        check(&mut retained, &group, 0, 2)?.bytes,
        replaced.bytes + 8192
    );
    assert!(check(&mut retained, &group, 3, 1)?.bytes > 8192);
    assert_eq!(check(&mut retained, &group, 4, 0)?.bytes, 0);
    // A caller's archive observation is part of the key, even if it regresses.
    assert_eq!(
        check(&mut retained, &group, 0, 2)?.bytes,
        replaced.bytes + 8192
    );

    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::Checkpoint(snapshot(&group, 4, 3)?));
    log.persist(OperationId::new(5), vec![write]).await?;
    assert_eq!(check(&mut retained, &group, 0, 0)?.bytes, 8192);

    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::Install {
        snapshot: snapshot(&group, 8, 4)?,
        applied: DurableApply {
            position: LogPosition {
                index: LogIndex::new(8),
                term: Term::new(4),
            },
            image: Some(image(16384)),
        },
    });
    write.entries = vec![entry(9, 4, b"snapshot suffix")];
    write.hard_state = Some(HardState {
        term: 4,
        vote: 1,
        commit: 8,
        ..Default::default()
    });
    log.persist(OperationId::new(6), vec![write]).await?;
    let installed = check(&mut retained, &group, 4, 1)?;
    assert!(installed.bytes > 16384);
    assert_eq!(
        check(&mut retained, &group, 8, 1)?.bytes,
        installed.bytes - 16384
    );
    log.close().await?;
    assert!(matches!(
        retained.observe(&group, LogIndex::new(8)),
        Err(LogError::Closed)
    ));

    let mut reopened = options(root.path());
    reopened.mode = LogOpenMode::Resume;
    reopened.process = ProcessGeneration::new([9; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            reopened,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.group(&bootstrap(1).scope)?;
    assert_eq!(check(&mut retained, &group, 4, 1)?, installed);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accounting_does_not_reuse_another_group_in_the_same_persisted_batch() -> anyhow::Result<()>
{
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let first = log.create_group(bootstrap(1)).await?;
    let second = log.create_group(bootstrap(2)).await?;
    let mut first_write = GroupWrite::new(&first);
    first_write.entries = vec![entry(2, 1, b"first")];
    let mut second_write = GroupWrite::new(&second);
    second_write.entries = vec![entry(2, 1, b"second"), entry(3, 1, b"extra")];
    log.persist(OperationId::new(1), vec![first_write, second_write])
        .await?;
    let mut retained = Retained::default();
    let first_usage = check(&mut retained, &first, 0, 1)?;
    let second_usage = check(&mut retained, &second, 0, 2)?;
    assert!(second_usage.bytes > first_usage.bytes);
    assert_eq!(check(&mut retained, &first, 0, 1)?, first_usage);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accounting_changes_only_after_sync_and_refuses_failed_storage() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let group = log.create_group(bootstrap(1)).await?;
    let mut retained = Retained::default();
    let original = check(&mut retained, &group, 0, 0)?;
    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(2, 1, b"awaiting sync")];
    let release = gate.arm(fault::HOLD_SYNC);
    let owned_log = log.clone();
    let persist =
        domain.spawn(async move { owned_log.persist(OperationId::new(1), vec![write]).await })?;
    let held = domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(2),
            gate.entered.notified(),
        ))
        .await?;
    let during = retained.observe(&group, LogIndex::new(0));
    drop(release);
    persist.await??;
    held?;
    assert_eq!(during?, original);
    check(&mut retained, &group, 0, 1)?;

    let mut write = GroupWrite::new(&group);
    write.entries = vec![entry(3, 1, b"failed sync")];
    let release = gate.arm(fault::FAIL_SYNC);
    let failed = log.persist(OperationId::new(2), vec![write]).await;
    drop(release);
    assert!(matches!(failed, Err(LogError::Io(_))));
    assert!(matches!(
        retained.observe(&group, LogIndex::new(0)),
        Err(LogError::Closed)
    ));
    assert!(log.close().await.is_err());
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_accounting_refuses_a_poisoned_group_cache() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.create_group(bootstrap(1)).await?;
    let mut retained = Retained::default();
    check(&mut retained, &group, 0, 0)?;
    let poisoned = group.clone();
    assert!(
        domain
            .blocking(move || {
                std::panic::catch_unwind(|| {
                    let _guard = poisoned.cache.write().unwrap();
                    panic!("injected cache-lock poison");
                })
                .is_err()
            })?
            .await?
    );
    let result = retained.observe(&group, LogIndex::new(0));
    log.close().await?;
    domain.shutdown().await;
    assert!(matches!(result, Err(LogError::Corrupt(_))));
    Ok(())
}
