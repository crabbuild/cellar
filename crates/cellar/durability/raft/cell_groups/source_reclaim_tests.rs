// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{Permit, Source};
use crate::raft::cell_groups::{ManifestPageStore, ManifestTree};
use crate::{
    asyncrt::ExecutionDomain,
    raft::{
        cache::GroupWrite,
        options::{LogOpenMode, ReplicaLogOptions},
        storage::{CellGroupBootstrap, LogError, ReplicaLog},
    },
};
use cellar_logic::{cell_group::*, consensus::*};
use raft::prelude::{Entry, HardState};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

fn bootstrap(id: u8) -> CellGroupBootstrap {
    CellGroupBootstrap {
        route: GroupRoute {
            history: GroupHistory {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                group: GroupId::new([id; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            range: CellRange::all(),
            epoch: RouteEpoch::new(1).unwrap(),
            birth_digest: [9; 32],
            schema_digest: [8; 32],
        },
        member: MemberId::new(1).unwrap(),
        voters: vec![MemberId::new(1).unwrap()],
        learners: vec![],
    }
}

async fn retired(
    log: &ReplicaLog,
    root: &std::path::Path,
    id: u8,
) -> anyhow::Result<(
    crate::raft::cache::GroupLog<GroupHistory>,
    ManifestPageStore,
    Source,
)> {
    let group = log.create_cell_group(bootstrap(id)).await?;
    let marker = group.group_manifest()?.unwrap();
    let mut pages = ManifestPageStore::open(
        &root.join(format!("pages-{id}")),
        Arc::new(cellar_ltx::DirectFileSystem),
        128,
        1024 * 1024,
        16,
    )?;
    let tree = ManifestTree::new(marker.route().clone(), Default::default())?;
    let mut current = tree.prepare_complete(
        &group,
        &mut pages,
        marker.root(),
        marker.position(),
        marker.membership_revision(),
        |_| unreachable!("empty root"),
    )?;
    for index in [2, 3] {
        let position = LogPosition {
            index: LogIndex::new(index),
            term: Term::new(2),
        };
        let mut prepared = tree.prepare_unchanged(&pages, &current, position)?;
        if index == 2 {
            prepared
                .marker
                .seal_source(TransitionId::new([6; 16])?, [7; 32], [8; 32])?;
        } else {
            prepared
                .marker
                .retire_source(TransitionId::new([6; 16])?, [10; 32])?;
        }
        let mut write = GroupWrite::new(&group);
        write.entries.push(Entry {
            index,
            term: 2,
            ..Default::default()
        });
        write.hard_state = Some(HardState {
            term: 2,
            vote: 1,
            commit: index,
            ..Default::default()
        });
        write.group_manifest = Some(prepared.clone());
        log.persist_cells(OperationId::new(index), vec![write])
            .await?;
        current = prepared;
    }
    let h = group.identity().scope;
    let source = Source {
        history: [
            h.cluster.bytes(),
            h.namespace.bytes(),
            h.group.bytes(),
            h.incarnation.bytes(),
        ],
        birth: [9; 32],
        root: None,
        member: 1,
        operation: [6; 16],
        plan: [7; 32],
        retirement: [8; 32],
        publication: [10; 32],
        seal_index: 2,
        seal_term: 2,
        index: 3,
        term: 2,
    };
    source.check_marker(&group.group_manifest()?.unwrap())?;
    pages
        .retention
        .prepare_source_reclamation(&group, &source)?;
    Ok((group, pages, source))
}

fn options(root: &std::path::Path) -> ReplicaLogOptions {
    let mut options = ReplicaLogOptions::new(
        root.join("log"),
        NodeId::new([1; 16]).unwrap(),
        DiskLineage::new([2; 16]).unwrap(),
        ProcessGeneration::new([3; 16]).unwrap(),
        LogOpenMode::Create,
    );
    options.max_groups = 1;
    options.max_source_tombstones = 1;
    options.purge_interval_ms = None;
    options
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_clean_refuses_reincarnation_and_releases_exact_slot_after_sync(
) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = options(root.path());
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let (group, mut pages, source) = retired(&log, root.path(), 5).await?;
    let history = group.identity().scope;
    assert!(matches!(
        log.create_cell_group(bootstrap(11)).await,
        Err(LogError::Overloaded)
    ));
    assert!(pages
        .retention
        .detach_reclaimed_source(&group, &source)
        .is_err());
    group.consensus_claimed.store(true, Ordering::Release);
    assert!(matches!(
        log.reclaim_source(Permit::new(&log, source.clone())?).await,
        Err(LogError::Overloaded)
    ));
    group.consensus_claimed.store(false, Ordering::Release);
    let ack = log
        .reclaim_source(Permit::new(&log, source.clone())?)
        .await?;
    assert!(!ack.record().complete);
    assert!(matches!(group.group_manifest(), Err(LogError::NotFound)));
    assert!(group.log_retention().unwrap().reserve(1024, false).is_err());
    pages.retention.detach_reclaimed_source(&group, &source)?;
    pages.reconcile_retained(&[])?;
    assert_eq!(pages.reclaim(256)?.pages, 0);
    let ack = log.finish_source_reclamation(ack).await?;
    assert!(ack.record().complete);
    assert_eq!(
        log.reclaim_source(Permit::new(&log, source.clone())?)
            .await?
            .record(),
        ack.record()
    );
    for changed in 0..3 {
        let mut old = bootstrap(5);
        match changed {
            0 => (),
            1 => old.route.history.incarnation = IncarnationId::new([12; 16])?,
            _ => old.route.history.namespace = NamespaceId::new([13; 16])?,
        }
        assert!(matches!(
            log.create_cell_group(old).await,
            Err(LogError::WrongIdentity)
        ));
    }
    log.create_cell_group(bootstrap(11)).await?;
    let (other, _other_pages, next) = retired(&log, root.path(), 11).await?;
    assert!(matches!(
        log.reclaim_source(Permit::new(&log, next)?).await,
        Err(LogError::Overloaded)
    ));
    assert!(other
        .group_manifest()?
        .unwrap()
        .source_seal()
        .unwrap()
        .retirement()
        .is_some());
    assert_eq!(log.cell_groups()?.len(), 1);
    log.close().await?;
    options.mode = LogOpenMode::Resume;
    options.process = ProcessGeneration::new([14; 16])?;
    let mut unfunded = options.clone();
    unfunded.max_cache_bytes = super::CACHE_BYTES - 1;
    assert!(matches!(
        domain
            .run(ReplicaLog::open(
                unfunded,
                Arc::new(cellar_ltx::DirectFileSystem)
            ))
            .await?,
        Err(LogError::Overloaded)
    ));
    let log = domain
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert!(
        log.source_reclamation(history)
            .await?
            .unwrap()
            .record()
            .complete
    );
    assert!(matches!(
        log.create_cell_group(bootstrap(5)).await,
        Err(LogError::WrongIdentity)
    ));
    assert_eq!(log.cell_groups()?.len(), 1);
    assert_eq!(
        log.cell_groups()?[0].identity().scope,
        bootstrap(11).route.history
    );
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_clean_keeps_capacity_until_sync_even_when_waiter_is_lost() -> anyhow::Result<()> {
    use crate::raft::storage_fault as fault;
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let (group, _pages, source) = retired(&log, root.path(), 5).await?;
    let history = group.identity().scope;
    let entered = gate.entered.notified();
    let release = gate.arm(fault::HOLD_SYNC);
    let permit = Permit::new(&log, source)?;
    let owner = log.clone();
    let waiter = domain.spawn(async move { owner.reclaim_source(permit).await })?;
    domain
        .run(crate::asyncrt::timeout(Duration::from_secs(5), entered))
        .await??;
    assert_eq!(log.cell_groups()?.len(), 1);
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(log.cell_groups()?.len(), 1);
    drop(release);
    let ack = log.source_reclamation(history).await?.unwrap();
    assert!(!ack.record().complete);
    assert!(log.cell_groups()?.is_empty());
    assert!(matches!(group.group_manifest(), Err(LogError::NotFound)));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_clean_sync_failure_fences_engine_and_reopens_one_atomic_state() -> anyhow::Result<()>
{
    use crate::raft::storage_fault as fault;
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let mut options = options(root.path());
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let (group, _pages, source) = retired(&log, root.path(), 5).await?;
    let history = group.identity().scope;
    let permit = Permit::new(&log, source)?;
    let release = gate.arm(fault::FAIL_SYNC);
    assert!(matches!(
        log.reclaim_source(permit).await,
        Err(LogError::Io(_))
    ));
    assert!(matches!(
        log.create_cell_group(bootstrap(11)).await,
        Err(LogError::Closed)
    ));
    assert!(matches!(group.group_manifest(), Err(LogError::Closed)));
    assert!(log.close().await.is_err());
    drop(release);
    options.mode = LogOpenMode::Resume;
    options.process = ProcessGeneration::new([15; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    if let Some(ack) = log.source_reclamation(history).await? {
        assert!(!ack.record().complete);
        assert!(log.cell_groups()?.is_empty());
        assert!(matches!(
            log.create_cell_group(bootstrap(5)).await,
            Err(LogError::WrongIdentity)
        ));
    } else {
        assert_eq!(log.cell_groups()?.len(), 1);
        assert!(log
            .cell_group(&history)?
            .group_manifest()?
            .unwrap()
            .source_seal()
            .unwrap()
            .retirement()
            .is_some());
    }
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
