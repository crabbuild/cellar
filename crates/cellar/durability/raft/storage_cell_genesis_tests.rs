// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::asyncrt::ExecutionDomain;
use ::raft::Storage as _;
use cellar_logic::{
    cell_group::{CellRange, RouteEpoch},
    consensus::*,
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
        voters: (1..=3).map(|n| MemberId::new(n).unwrap()).collect(),
        learners: vec![],
    }
}

fn options(root: &std::path::Path) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        root.join("log"),
        NodeId::new([1; 16]).unwrap(),
        DiskLineage::new([2; 16]).unwrap(),
        ProcessGeneration::new([3; 16]).unwrap(),
        LogOpenMode::Create,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cell_group_genesis_restores_physical_handles_and_exact_birth() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut opts = options(root.path());
    let initial = bootstrap(5);
    let log = domain
        .run(ReplicaLog::open(
            opts.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.create_cell_group(initial.clone()).await?;
    let marker = group.group_manifest()?.unwrap();
    assert_eq!(marker.route(), &initial.route);
    assert_eq!(
        marker.position(),
        LogPosition {
            index: LogIndex::new(1),
            term: Term::new(1)
        }
    );
    assert_eq!(marker.root().cells(), 0);
    assert_eq!(group.identity().scope, initial.route.history);
    assert!(group.applied().is_err());
    assert_eq!(
        log.create_cell_group(initial.clone()).await?.numeric_key(),
        group.numeric_key()
    );
    for field in 0..4 {
        let mut wrong = initial.clone();
        match field {
            0 => wrong.route.schema_digest = [7; 32],
            1 => wrong.route.epoch = RouteEpoch::new(2)?,
            2 => wrong.member = MemberId::new(2)?,
            _ => wrong.voters.pop().map(|_| ()).unwrap(),
        }
        assert!(matches!(
            log.create_cell_group(wrong).await,
            Err(LogError::WrongIdentity)
        ));
    }
    log.create_cell_group(bootstrap(7)).await?;
    assert_eq!(log.cell_groups()?.len(), 2);
    let inventory = log.cell_group_inventory(None)?;
    assert_eq!(inventory.histories().len(), 2);
    assert_eq!(inventory, log.physical_inventory(None)?);
    let row = inventory.group_history(&initial.route.history).unwrap();
    assert_eq!(row.member(), initial.member);
    assert_eq!(row.birth_digest(), initial.route.birth_digest);
    assert!(row.in_configuration());
    assert_eq!(row.completed_sequence(), 0);
    assert!(inventory
        .group_history(&bootstrap(8).route.history)
        .is_none());
    // Both a pending existing slot and a genuinely new slot must survive the
    // bounded node wire representation, with the correct occupied count.
    for (pending, count) in [(initial.route.history, 2), (bootstrap(8).route.history, 3)] {
        let captured = log.cell_group_inventory(Some(&pending))?;
        assert_eq!(captured.histories(), inventory.histories());
        let mut view = crate::raft::node_inspection_wire::tests::view();
        view.node.groups = count;
        view.node.reserved_bytes = u64::from(count) * view.node.profile.bytes_per_group;
        view.unattached_engine_histories = 2;
        let encoded = crate::raft::physical_inventory::encode(&view, &captured)?;
        let (_, decoded) = crate::raft::physical_inventory::decode(&encoded)?;
        assert_eq!(*decoded, captured);
        view.node.groups -= 1;
        view.node.reserved_bytes -= view.node.profile.bytes_per_group;
        assert!(crate::raft::physical_inventory::encode(&view, &captured).is_err());
    }
    assert!(matches!(log.groups(), Err(LogError::WrongIdentity)));
    let old = GroupBootstrap {
        scope: GroupScope {
            cluster: initial.route.history.cluster,
            namespace: initial.route.history.namespace,
            group: GroupId::new([10; 16])?,
            incarnation: initial.route.history.incarnation,
            cell: "actual-old-cell".into(),
        },
        member: initial.member,
        birth_digest: initial.route.birth_digest,
        voters: initial.voters.clone(),
        learners: vec![],
    };
    assert!(matches!(
        log.create_group(old).await,
        Err(LogError::WrongIdentity)
    ));
    let mut write = GroupWrite::new(&group);
    write.entries.push(Entry {
        index: 2,
        term: 2,
        data: b"control-prefix".to_vec().into(),
        ..Default::default()
    });
    let mut hard = group.initial_state()?.hard_state;
    hard.term = 2;
    hard.commit = 2;
    hard.vote = 1;
    write.hard_state = Some(hard.clone());
    let receipt = log.persist_cells(OperationId::new(1), vec![write]).await?;
    assert_eq!(receipt.groups, vec![group.identity().clone()]);
    let instance = log.storage_instance()?;
    log.close().await?;
    opts.mode = LogOpenMode::Resume;
    opts.process = ProcessGeneration::new([6; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            opts.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.storage_instance()?, instance);
    let recovered = log.create_cell_group(initial.clone()).await?;
    assert_eq!(recovered.identity().scope, initial.route.history);
    assert_eq!(recovered.identity().process, opts.process);
    assert_eq!(recovered.group_manifest()?, Some(marker));
    assert_eq!(recovered.initial_state()?.hard_state, hard);
    assert_eq!(
        recovered.entries(2, 3, None, ::raft::GetEntriesContext::empty(false))?[0]
            .data
            .as_ref(),
        b"control-prefix"
    );
    assert_eq!(log.cell_groups()?.len(), 2);
    assert_eq!(log.cell_group_inventory(None)?, inventory);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cell_group_genesis_stays_invisible_until_actual_sync() -> anyhow::Result<()> {
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
    let initial = bootstrap(5);
    let release = gate.arm(fault::HOLD_SYNC);
    let writer = log.clone();
    let proposal = initial.clone();
    let create = domain.spawn(async move { writer.create_cell_group(proposal).await })?;
    let held = domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await?;
    let hidden = matches!(
        log.cell_group(&initial.route.history),
        Err(LogError::NotFound)
    );
    let count = log.group_count();
    let pending = log.cell_group_inventory(Some(&initial.route.history));
    drop(release);
    let group = create.await??;
    held?;
    assert!(hidden);
    assert_eq!(count?, 0);
    let pending = pending?;
    assert!(pending.histories().is_empty());
    assert_eq!(
        pending.initializing(),
        Some(
            log.cell_group_inventory(None)?
                .group_history(&initial.route.history)
                .unwrap()
                .scope_digest()
        )
    );
    assert_eq!(group.group_manifest()?.unwrap().route(), &initial.route);
    assert_eq!(log.group_count()?, 1);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_requirement_is_local_irreversible_and_recovered() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut opts = options(root.path());
    let initial = bootstrap(5);
    let log = domain
        .run(ReplicaLog::open(
            opts.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.create_cell_group(initial.clone()).await?;
    let marker = group.group_manifest()?.unwrap();
    assert!(group.archive_compaction.permits(&marker)?);
    let admitted = group.archive_compaction.admit(&marker)?.unwrap();
    let activation = group.archive_compaction.begin_activation()?;
    assert!(matches!(
        log.require_cell_group_archive(&initial.route.history, activation.clone())
            .await,
        Err(LogError::Overloaded)
    ));
    assert!(!group.read()?.archive_required);
    assert!(
        group.archive_compaction.admit(&marker)?.is_none(),
        "a pending archive activation must stop later checkpoint admissions"
    );
    let physical = admitted.clone();
    drop(admitted);
    assert!(matches!(
        log.require_cell_group_archive(&initial.route.history, activation.clone())
            .await,
        Err(LogError::Overloaded)
    ));
    drop(physical);
    log.require_cell_group_archive(&initial.route.history, activation)
        .await?;
    assert!(group.read()?.archive_required);
    assert!(!group.clone().archive_compaction.permits(&marker)?);
    let record = {
        let cache = group.read()?;
        let mapping = MappingRecord::new_cells(group.numeric_key(), &initial, &cache.conf_state)?;
        GroupRecord::from_cache(mapping, &cache)?
    };
    let mut encoded = serde_json::to_value(record)?;
    encoded["application"]["state"]
        .as_object_mut()
        .unwrap()
        .remove("archive_required");
    assert!(records::decode::<GroupRecord>(&serde_json::to_vec(&encoded)?).is_err());
    log.persist_cells(OperationId::new(17), vec![GroupWrite::new(&group)])
        .await?;
    assert!(group.read()?.archive_required);
    log.close().await?;
    opts.mode = LogOpenMode::Resume;
    opts.process = ProcessGeneration::new([6; 16])?;
    let resumed = domain
        .run(ReplicaLog::open(
            opts,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let restored = resumed.cell_group(&initial.route.history)?;
    assert!(restored.read()?.archive_required);
    assert!(!restored.archive_compaction.permits(&marker)?);
    resumed.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_activation_drains_an_admitted_checkpoint_at_storage() -> anyhow::Result<()> {
    use crate::raft::cell_groups::{
        CellGroupCheckpointLimits, CellGroupCheckpointPool, ManifestLimits, ManifestPageStore,
        ManifestTree,
    };
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let initial = bootstrap(5);
    let group = log.create_cell_group(initial.clone()).await?;
    let genesis = group.group_manifest()?.unwrap();
    let earlier = group.archive_compaction.admit(&genesis)?.unwrap();
    let source = group.clone();
    let directory = root.path().join("pages");
    let (pages, next) = domain
        .blocking(move || -> anyhow::Result<_> {
            let mut pages = ManifestPageStore::open(
                &directory,
                Arc::new(cellar_ltx::DirectFileSystem),
                1024,
                16 * 1024 * 1024,
                16,
            )?;
            let tree = ManifestTree::new(genesis.route().clone(), ManifestLimits::default())?;
            let prepared = tree.prepare_complete(
                &source,
                &mut pages,
                genesis.root(),
                genesis.position(),
                genesis.membership_revision(),
                |_| unreachable!("empty group"),
            )?;
            let next = tree.prepare_unchanged(
                &pages,
                &prepared,
                LogPosition {
                    index: LogIndex::new(2),
                    term: Term::new(1),
                },
            )?;
            Ok((pages, next))
        })?
        .await??;
    let mut write = GroupWrite::new(&group);
    write.entries.push(Entry {
        index: 2,
        term: 1,
        ..Default::default()
    });
    let mut hard = group.initial_state()?.hard_state;
    hard.commit = 2;
    write.hard_state = Some(hard);
    log.persist_cells(OperationId::new(20), vec![write]).await?;
    let mut write = GroupWrite::new(&group);
    write.group_manifest = Some(next.clone());
    log.persist_cells(OperationId::new(21), vec![write]).await?;
    let pool = CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
        max_sources: 1,
        max_retained_bytes: 32 * 1024 * 1024,
        max_cells: 64,
        max_transfer_bytes: 32 * 1024 * 1024,
        max_chunk_bytes: 4093,
    })?;
    let capture = pool.clone();
    let source = group.clone();
    let cut = next.clone();
    let checkpoint = domain
        .blocking(move || capture.capture(&source, &cut)?.prepare_publication())?
        .await??;
    let admitted = group
        .archive_compaction
        .admit(checkpoint.marker())?
        .unwrap();
    let activation = group.archive_compaction.begin_activation()?;
    assert!(!group.archive_compaction.permits(checkpoint.marker())?);
    assert!(group
        .archive_compaction
        .admit(checkpoint.marker())?
        .is_none());

    let mut wrong_cut = GroupWrite::new(&group);
    wrong_cut.snapshot = Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
    wrong_cut.checkpoint_admission = Some(earlier.clone());
    assert!(matches!(
        log.persist_cells(OperationId::new(22), vec![wrong_cut])
            .await,
        Err(LogError::WrongIdentity)
    ));
    drop(earlier);
    let foreign = log.create_cell_group(bootstrap(7)).await?;
    let mut wrong_group = GroupWrite::new(&foreign);
    wrong_group.snapshot = Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
    wrong_group.checkpoint_admission = Some(admitted.clone());
    assert!(matches!(
        log.persist_cells(OperationId::new(23), vec![wrong_group])
            .await,
        Err(LogError::WrongIdentity)
    ));
    let mut missing_cut = GroupWrite::new(&group);
    missing_cut.checkpoint_admission = Some(admitted.clone());
    assert!(matches!(
        log.persist_cells(OperationId::new(24), vec![missing_cut])
            .await,
        Err(LogError::WrongIdentity)
    ));
    let mut unadmitted = GroupWrite::new(&group);
    unadmitted.snapshot = Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
    assert!(matches!(
        log.persist_cells(OperationId::new(25), vec![unadmitted])
            .await,
        Err(LogError::Overloaded)
    ));
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::CellGroupCheckpoint(checkpoint.clone()));
    write.checkpoint_admission = Some(admitted);
    // This unpolled physical request now owns the final checkpoint lease.
    // Activation must wait, then storage must accept its inherited exact cut.
    let saving = log.persist_cells(OperationId::new(26), vec![write]);
    assert!(matches!(
        log.require_cell_group_archive(&initial.route.history, activation.clone())
            .await,
        Err(LogError::Overloaded)
    ));
    saving.await?;
    assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(next.marker()));
    assert_eq!(group.first_index()?, 3);
    log.require_cell_group_archive(&initial.route.history, activation)
        .await?;
    assert!(group.read()?.archive_required);
    assert!(!group.archive_compaction.permits(next.marker())?);
    assert_eq!(pool.usage().sources, 1);
    drop(checkpoint);
    assert_eq!(pool.usage().sources, 0);
    log.close().await?;
    drop(pages);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_activation_is_retained_by_entered_storage() -> anyhow::Result<()> {
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
    let initial = bootstrap(5);
    let group = log.create_cell_group(initial.clone()).await?;
    let marker = group.group_manifest()?.unwrap();
    let abandoned = group.archive_compaction.begin_activation()?;
    assert!(!group.archive_compaction.permits(&marker)?);
    drop(abandoned);
    assert!(group.archive_compaction.permits(&marker)?);
    assert!(!group.read()?.archive_required);

    let activation = group.archive_compaction.begin_activation()?;
    let release = gate.arm(fault::HOLD_SYNC);
    let mut writing =
        Box::pin(log.require_cell_group_archive(&initial.route.history, activation.clone()));
    let pending = domain
        .run(crate::asyncrt::timeout(
            Duration::from_millis(50),
            writing.as_mut(),
        ))
        .await?;
    assert!(pending.is_err(), "actual metadata sync remains held");
    domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await??;
    drop(writing);
    drop(activation);
    assert!(!group.archive_compaction.permits(&marker)?);
    assert!(!group.read()?.archive_required);
    drop(release);
    log.reserve_node_revision().await?;
    assert!(group.read()?.archive_required);
    assert!(!group.archive_compaction.permits(&marker)?);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_log_admission_accounts_selected_entries_and_reopen() -> anyhow::Result<()> {
    use crate::raft::cell_groups::{CellGroupLogCapacity, CellGroupLogRetentionLimits};
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut settings = options(root.path());
    settings.cell_group_retention = CellGroupLogRetentionLimits {
        node: CellGroupLogCapacity {
            bytes: 8192,
            entries: 5,
        },
        group: CellGroupLogCapacity {
            bytes: 4096,
            entries: 3,
        },
        progress_node: CellGroupLogCapacity {
            bytes: 2048,
            entries: 2,
        },
        progress_group: CellGroupLogCapacity {
            bytes: 1024,
            entries: 1,
        },
    };
    let log = domain
        .run(ReplicaLog::open(
            settings.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let group = log.create_cell_group(bootstrap(5)).await?;
    let sibling = log.create_cell_group(bootstrap(7)).await?;
    let data = |index| Entry {
        index,
        term: 1,
        data: vec![9; 256].into(),
        ..Default::default()
    };
    let mut first = GroupWrite::new(&group);
    first.entries = vec![data(2), data(3)];
    log.persist_cells(OperationId::new(50), vec![first]).await?;
    let selected = group.log_retention_usage()?;
    assert_eq!(selected.retained_entries, 2);
    assert_eq!(selected.reserved_entries, 0);
    let mut full = GroupWrite::new(&group);
    full.entries.push(data(4));
    assert!(matches!(
        log.persist_cells(OperationId::new(51), vec![full]).await,
        Err(LogError::Overloaded)
    ));
    assert_eq!(group.last_index()?, 3);
    assert_eq!(group.log_retention_usage()?, selected);
    // Empty Raft control entries have their own bounded space. Another data
    // entry cannot consume it, and a healthy sibling still has node capacity.
    let mut control = GroupWrite::new(&group);
    control.entries.push(Entry {
        index: 4,
        term: 1,
        ..Default::default()
    });
    log.persist_cells(OperationId::new(52), vec![control])
        .await?;
    let mut other = GroupWrite::new(&sibling);
    other.entries.push(data(2));
    log.persist_cells(OperationId::new(53), vec![other]).await?;
    let before = log.cell_group_log_retention_usage()?;
    assert_eq!(before.retained_entries, 4);
    assert_eq!(before.reserved_entries, 0);
    log.close().await?;
    drop(group);
    drop(sibling);
    drop(log);
    settings.mode = LogOpenMode::Resume;
    settings.process = ProcessGeneration::new([9; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            settings,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.cell_group_log_retention_usage()?, before);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_log_lease_survives_waiter_loss_and_transfers_once() -> anyhow::Result<()> {
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
    let group = log.create_cell_group(bootstrap(5)).await?;
    let sibling = log.create_cell_group(bootstrap(7)).await?;
    let entry = Entry {
        index: 2,
        term: 1,
        data: vec![1; 256].into(),
        ..Default::default()
    };
    let lease = group.log_retention().unwrap().reserve_entry(&entry)?;
    let reserved = group.log_retention_usage()?;
    assert_eq!(
        (reserved.retained_entries, reserved.reserved_entries),
        (0, 1)
    );
    let mut wrong = GroupWrite::new(&sibling);
    wrong.entries.push(entry.clone());
    wrong.log_reservations.insert(2, lease.clone());
    assert!(matches!(
        log.persist_cells(OperationId::new(60), vec![wrong]).await,
        Err(LogError::WrongIdentity)
    ));
    let mut changed = GroupWrite::new(&group);
    changed.entries.push(Entry {
        term: 2,
        ..entry.clone()
    });
    changed.log_reservations.insert(2, lease.clone());
    assert!(matches!(
        log.persist_cells(OperationId::new(61), vec![changed]).await,
        Err(LogError::WrongIdentity)
    ));
    let release = gate.arm(fault::HOLD_SYNC);
    let mut write = GroupWrite::new(&group);
    write.entries.push(entry.clone());
    write.log_reservations.insert(2, lease.clone());
    let mut saving = Box::pin(log.persist_cells(OperationId::new(62), vec![write]));
    assert!(domain
        .run(crate::asyncrt::timeout(
            Duration::from_millis(50),
            saving.as_mut()
        ))
        .await?
        .is_err());
    domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await??;
    drop(saving);
    assert_eq!(group.log_retention_usage()?, reserved);
    drop(release);
    log.reserve_node_revision().await?;
    let selected = group.log_retention_usage()?;
    assert_eq!(
        (selected.retained_entries, selected.reserved_entries),
        (1, 0)
    );
    assert_eq!(selected.retained_bytes, reserved.reserved_bytes);
    // Keeping the original lease does not double-charge selected debt. It also
    // cannot authorize another write after its exact reservation was consumed.
    assert!(matches!(
        lease.validate(group.log_retention().unwrap(), &entry),
        Err(LogError::WrongIdentity)
    ));
    drop(lease);
    assert_eq!(group.log_retention_usage()?, selected);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
