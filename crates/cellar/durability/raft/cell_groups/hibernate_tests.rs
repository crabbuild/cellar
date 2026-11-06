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
async fn park_acknowledgement_reopens_and_closes_on_disk() -> anyhow::Result<()> {
    use crate::{
        application::consensus::group_protocol::{
            CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
        },
        asyncrt::ExecutionDomain,
        raft::{
            cell_groups::{ManifestPageStore, ManifestTree},
            options::{LogOpenMode, ReplicaLogOptions},
            storage::CellGroupBootstrap,
        },
    };
    use ::raft::prelude::{Entry, HardState};
    use cellar_logic::consensus::{DiskLineage, NodeId, ProposalId};
    use cellar_ltx::DirectFileSystem;
    use std::sync::Arc;

    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = ReplicaLogOptions::new(
        directory.path().join("log"),
        NodeId::new([11; 16])?,
        DiskLineage::new([12; 16])?,
        ProcessGeneration::new([13; 16])?,
        LogOpenMode::Create,
    );
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let route = marker(1).route().clone();
    let member = MemberId::new(1)?;
    let group = log
        .create_cell_group(CellGroupBootstrap {
            route: route.clone(),
            member,
            voters: vec![member, MemberId::new(2)?, MemberId::new(3)?],
            learners: vec![],
        })
        .await?;
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
        vote: member.get(),
        commit: at.index.get(),
        ..Default::default()
    };
    apply.hard_state = Some(hard.clone());
    apply.group_manifest = Some(prepared);
    log.persist_cells(operation, vec![apply]).await?;
    for _ in 0..2 {
        let receipt = log
            .persist_cells(operation, vec![acknowledge(&group)])
            .await?;
        assert_eq!(receipt.groups, vec![group.identity().clone()]);
    }
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
