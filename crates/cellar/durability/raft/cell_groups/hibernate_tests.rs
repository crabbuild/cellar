// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::GroupManifest;
use cellar_logic::{
    cell_group::{CellRange, GroupHistory, GroupRoute, RouteEpoch},
    consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
};

fn marker(index: u64) -> GroupManifest {
    let mut marker = GroupManifest::genesis(GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    })
    .unwrap();
    marker.position = LogPosition {
        index: LogIndex::new(index),
        term: Term::new(2),
    };
    marker
}

#[test]
fn park_prepare_binds_committed_position_and_membership() {
    let mut marker = marker(3);
    let generation = ParkGeneration::new(3).unwrap();
    let term = Term::new(2);
    let membership = MembershipRevision::new(1).unwrap();
    for (generation, term, membership) in [
        (ParkGeneration::new(2).unwrap(), term, membership),
        (generation, Term::new(3), membership),
        (generation, term, MembershipRevision::new(2).unwrap()),
    ] {
        assert!(marker.prepare_park(generation, term, membership).is_err());
        assert!(marker.park_cut().is_none());
    }
    marker.prepare_park(generation, term, membership).unwrap();
    let cut = marker.park_cut().unwrap();
    assert_eq!(cut.generation(), generation.get());
    assert_eq!(cut.position(), marker.position());
    assert_eq!(cut.membership_revision(), membership.get());
    assert!(marker.is_active());

    let bytes = serde_json::to_vec(&marker).unwrap();
    assert_eq!(
        serde_json::from_slice::<GroupManifest>(&bytes).unwrap(),
        marker
    );
    let mut value = serde_json::to_value(&marker).unwrap();
    value["park"]["term"] = 1.into();
    assert!(serde_json::from_value::<GroupManifest>(value).is_err());
}

#[test]
fn park_cut_survives_advance_and_refuses_checkpoint_regression() {
    let mut prepared = marker(3);
    prepared
        .prepare_park(
            ParkGeneration::new(3).unwrap(),
            Term::new(2),
            MembershipRevision::new(1).unwrap(),
        )
        .unwrap();
    let mut advanced = prepared.clone();
    advanced.position.index = LogIndex::new(4);
    advanced.membership_revision = 4;
    advanced.check().unwrap();
    assert!(advanced.preserves_park_cut(&prepared));
    assert!(!marker(4).preserves_park_cut(&prepared));
    assert!(advanced
        .prepare_park(
            ParkGeneration::new(3).unwrap(),
            Term::new(2),
            MembershipRevision::new(1).unwrap(),
        )
        .is_err());

    let mut forged = marker(5);
    forged
        .prepare_park(
            ParkGeneration::new(5).unwrap(),
            Term::new(2),
            MembershipRevision::new(1).unwrap(),
        )
        .unwrap();
    assert!(!forged.preserves_park_cut(&advanced));

    let mut next = advanced.clone();
    next.position.index = LogIndex::new(5);
    next.prepare_park(
        ParkGeneration::new(5).unwrap(),
        Term::new(2),
        MembershipRevision::new(4).unwrap(),
    )
    .unwrap();
    assert!(next.preserves_park_cut(&advanced));
    assert!(!advanced.preserves_park_cut(&next));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn park_acknowledgement_waits_for_sync_and_reopens_closed() -> anyhow::Result<()> {
    use crate::{
        application::consensus::group_protocol::{
            CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
        },
        asyncrt::ExecutionDomain,
        raft::{
            catalog::{Catalog, CatalogOptions, InitialMember, NamespaceConfig},
            cell_groups::{
                CellGroupBirthSpec, CellGroupEffect, CellGroupRuntime, DirectoryLimits,
                ManifestPageStore, ManifestTree, RangeDirectory,
            },
            completion::RuntimeError,
            options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
            storage_fault as fault,
        },
    };
    use ::raft::prelude::{Entry, HardState, Message, MessageType};
    use cellar_logic::consensus::{DiskLineage, NodeId, ProposalId};
    use cellar_ltx::DirectFileSystem;
    use std::{sync::Arc, time::Duration};

    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = ReplicaLogOptions::new(
        directory.path().join("log"),
        NodeId::new([11; 16])?,
        DiskLineage::new([12; 16])?,
        ProcessGeneration::new([13; 16])?,
        LogOpenMode::Create,
    );
    options.purge_interval_ms = None;
    let gate = fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let route = marker(1).route().clone();
    let member = MemberId::new(1)?;
    let leader = MemberId::new(2)?;
    let catalog = Catalog::new(
        Arc::new(object_store::memory::InMemory::new()),
        CatalogOptions::default(),
    )?;
    let namespace = domain
        .run(catalog.open_namespace(NamespaceConfig::new(
            route.history.cluster,
            route.history.namespace,
            cellar_logic::NamespaceMode::Raft,
            route.schema_digest,
        )))
        .await??;
    let directory_owner = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let request = directory_owner.prepare_group_birth(CellGroupBirthSpec {
        history: route.history,
        range: route.range,
        epoch: route.epoch,
        creation: ProposalId::new([16; 16])?,
        voters: (1..=3)
            .map(|n| -> anyhow::Result<_> {
                Ok(InitialMember {
                    member: MemberId::new(n)?,
                    node: NodeId::new([n as u8 + 10; 16])?,
                })
            })
            .collect::<anyhow::Result<_>>()?,
        learners: vec![],
    })?;
    domain
        .run(directory_owner.reserve_group_birth(&request))
        .await??;
    let initialize = directory_owner
        .prepare_initialization(ProposalId::new([17; 16])?, &[request.route().clone()])?;
    domain.run(directory_owner.publish(&initialize)).await??;
    let birth = domain
        .run(directory_owner.materialize_group_birth(&request))
        .await??;
    let permit = domain
        .run(directory_owner.bootstrap_group_member(&birth, &log))
        .await??;
    let group = permit.group().clone();
    let route = permit.route().clone();
    let at = marker(2).position();
    let generation = ParkGeneration::new(at.index.get())?;
    let membership = MembershipRevision::new(1)?;
    let genesis = group.group_manifest()?.unwrap();
    let store_group = group.clone();
    let page_path = directory.path().join("pages");
    let (pages, prepared) = domain
        .blocking(move || -> anyhow::Result<_> {
            let mut pages = ManifestPageStore::open(
                &page_path,
                Arc::new(DirectFileSystem),
                128,
                1024 * 1024,
                16,
            )?;
            let tree = ManifestTree::new(genesis.route().clone(), Default::default())?;
            let base = tree.prepare_complete(
                &store_group,
                &mut pages,
                genesis.root(),
                genesis.position(),
                genesis.membership_revision(),
                |_| unreachable!("empty genesis"),
            )?;
            let mut prepared = tree.prepare_unchanged(&pages, &base, at)?;
            prepared
                .marker
                .prepare_park(generation, at.term, membership)?;
            Ok((pages, prepared))
        })?
        .await??;
    let cut = prepared.marker().park_cut().unwrap().clone();
    let acknowledge = |group: &GroupLog<GroupHistory>| {
        let mut write = GroupWrite::new(group);
        write.local_park = Some(LocalParkChange::Acknowledge(cut.clone()));
        write
    };
    let operation = OperationId::new(1);
    assert!(matches!(
        log.persist_cells(operation, vec![acknowledge(&group)])
            .await,
        Err(LogError::WrongIdentity)
    ));

    let mut apply = GroupWrite::new(&group);
    let data = CellGroupEntry {
        binding: GroupEntryBinding {
            group: route.history,
            route_epoch: route.epoch,
            birth_digest: route.birth_digest,
            schema_digest: route.schema_digest,
            proposal: ProposalId::new([14; 16])?,
        },
        command: GroupCommand::PreparePark {
            generation,
            term: at.term,
            membership,
        },
    }
    .encode(&route, GroupEntryLimits::default())?;
    apply.entries.push(Entry {
        index: at.index.get(),
        term: at.term.get(),
        data: data.into(),
        ..Default::default()
    });
    let hard = HardState {
        term: at.term.get(),
        vote: leader.get(),
        commit: at.index.get(),
        ..Default::default()
    };
    apply.hard_state = Some(hard.clone());
    apply.group_manifest = Some(prepared.clone());
    log.persist_cells(operation, vec![apply]).await?;
    let (runtime, mut events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([18; 32]),
            ..Default::default()
        },
    )
    .await?;
    runtime.attach(permit, prepared).await?;
    assert!(matches!(
        runtime
            .acknowledge_park(route.history, leader, cut.clone())
            .await,
        Err(RuntimeError::NotReady)
    ));
    let mut heartbeat = Message {
        from: leader.get(),
        to: member.get(),
        term: at.term.get(),
        commit: at.index.get(),
        ..Default::default()
    };
    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
    runtime.step(route.history, heartbeat).await?;
    let event = domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            events.recv(),
        ))
        .await??
        .expect("heartbeat response");
    assert!(matches!(
        event.effect(),
        Ok(CellGroupEffect::Message(message))
            if message.get_msg_type() == MessageType::MsgHeartbeatResponse
    ));
    drop(event);
    assert!(matches!(
        runtime
            .acknowledge_park(route.history, member, cut.clone())
            .await,
        Err(RuntimeError::NotReady)
    ));

    let release = gate.arm(fault::HOLD_SYNC);
    let requester = runtime.clone();
    let history = route.history;
    let requested_cut = cut.clone();
    let mut pending = domain.spawn(async move {
        requester
            .acknowledge_park(history, leader, requested_cut)
            .await
    })?;
    let entered = domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await?;
    let hidden = group.read()?.local_park.is_none();
    let unresolved = futures_util::poll!(&mut pending).is_pending();
    drop(release);
    entered?;
    assert!(hidden);
    assert!(unresolved);
    let receipt = pending.await??;
    assert_eq!(receipt.voter, *group.identity());
    assert_eq!(receipt.leader, leader);
    assert_eq!(receipt.cut, cut);
    assert_ne!(receipt.sync_sequence, 0);
    let duplicate = runtime
        .acknowledge_park(route.history, leader, cut.clone())
        .await?;
    assert_eq!(duplicate.voter, receipt.voter);
    assert_eq!(duplicate.cut, receipt.cut);
    assert!(duplicate.sync_sequence > receipt.sync_sequence);
    runtime.close().await?;
    let acknowledged = LocalParkRecord {
        cut: cut.clone(),
        process: options.process.bytes(),
        phase: LocalParkPhase::Acknowledged,
    };
    assert_eq!(group.read()?.local_park.as_ref(), Some(&acknowledged));

    // An ordinary Ready that advances the term closes the acknowledgement
    // in the same engine batch, even when the applied park cut has not moved.
    let mut higher_term = GroupWrite::new(&group);
    higher_term.hard_state = Some(HardState {
        term: at.term.get() + 1,
        vote: 0,
        ..hard.clone()
    });
    let next = group.read()?.changed(&higher_term, 16, route.history)?;
    assert_eq!(next.local_park.as_ref(), Some(&acknowledged.closed()));

    log.close().await?;
    drop(group);
    drop(pages);
    options.mode = LogOpenMode::Resume;
    options.process = ProcessGeneration::new([15; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let group = log.cell_group(&route.history)?;
    assert_eq!(group.read()?.local_park.as_ref(), Some(&acknowledged));
    assert_ne!(options.process.bytes(), acknowledged.process);
    assert!(matches!(
        log.persist_cells(operation, vec![acknowledge(&group)])
            .await,
        Err(LogError::WrongIdentity)
    ));

    log.close_retained_park(&group).await?;
    log.close_retained_park(&group).await?;
    assert_eq!(group.read()?.hard_state, hard);
    assert_eq!(
        group.read()?.local_park.as_ref(),
        Some(&acknowledged.closed())
    );
    assert!(matches!(
        log.persist_cells(operation, vec![acknowledge(&group)])
            .await,
        Err(LogError::WrongIdentity)
    ));
    log.close().await?;

    let log = domain
        .run(ReplicaLog::open(options, Arc::new(DirectFileSystem)))
        .await??;
    let group = log.cell_group(&route.history)?;
    let record = group.read()?.local_park.clone().unwrap();
    assert_eq!(record, acknowledged.closed());
    assert_eq!(group.group_manifest()?.unwrap().park_cut(), Some(&cut));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
