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
