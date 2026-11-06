// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::{
    asyncrt::ExecutionDomain,
    raft::{
        catalog::{Catalog, CatalogOptions, InitialMember, NamespaceConfig, VoterPolicy},
        cell_groups::{
            CellGroupBirthSpec, CellGroupBootstrapPermit, DirectoryLimits, RangeDirectory,
        },
    },
};
use cellar_logic::{
    cell_group::{CellRange, GroupHistory, GroupRoute, RouteEpoch},
    consensus::{ClusterId, GroupId, IncarnationId, NamespaceId, NodeId, ProposalId},
};
use std::sync::Arc;

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

#[test]
fn park_certificate_requires_exact_complete_voter_evidence() -> anyhow::Result<()> {
    use crate::raft::member_state::GroupMembership;
    use ::raft::prelude::ConfState;
    use cellar_logic::consensus::DiskLineage;

    let mut marker = marker(3);
    marker.prepare_park(
        ParkGeneration::new(3)?,
        Term::new(2),
        MembershipRevision::new(1)?,
    )?;
    let cut = marker.park_cut().unwrap();
    let route = marker.route();
    let voters: Vec<_> = (1..=3)
        .map(|id| -> anyhow::Result<_> {
            Ok(ReplicaIdentity {
                scope: route.history,
                member: MemberId::new(u64::from(id))?,
                node: NodeId::new([id; 16])?,
                disk: DiskLineage::new([id + 3; 16])?,
                process: ProcessGeneration::new([id + 6; 16])?,
            })
        })
        .collect::<anyhow::Result<_>>()?;
    let leader = &voters[0];
    let evidence = || {
        voters
            .iter()
            .rev()
            .map(|voter| ParkAcknowledgement {
                voter: voter.clone(),
                leader: leader.member,
                cut: cut.clone(),
                sync_sequence: 10,
            })
            .collect::<Vec<_>>()
    };
    let certificate = ParkCertificate::assemble(route, cut.clone(), leader, &voters, evidence())?;
    let conf = ConfState {
        voters: vec![3, 1, 2],
        ..Default::default()
    };
    let membership = GroupMembership::initial(&conf);
    certificate.validate(route, &conf, &membership)?;
    assert_eq!(
        serde_json::from_slice::<ParkCertificate>(&serde_json::to_vec(&certificate)?)?,
        certificate
    );
    let mut other_route = route.clone();
    other_route.history.incarnation = IncarnationId::new([11; 16])?;
    assert!(certificate
        .validate(&other_route, &conf, &membership)
        .is_err());
    let mut joint = conf.clone();
    joint.voters_outgoing.push(1);
    assert!(certificate.validate(route, &joint, &membership).is_err());

    let mut partial = evidence();
    partial.pop();
    assert!(ParkCertificate::assemble(route, cut.clone(), leader, &voters, partial).is_err());
    for field in 0..6 {
        let mut replies = evidence();
        match field {
            0 => replies[0].voter = replies[1].voter.clone(),
            1 => replies[0].voter.process = ProcessGeneration::new([12; 16])?,
            2 => replies[0].voter.disk = DiskLineage::new([12; 16])?,
            3 => replies[0].leader = voters[1].member,
            4 => replies[0].sync_sequence = 0,
            _ => {
                replies[0].cut = GroupParkCut::prepare(
                    ParkGeneration::new(4)?,
                    Term::new(2),
                    MembershipRevision::new(1)?,
                    LogPosition {
                        term: Term::new(2),
                        index: LogIndex::new(4),
                    },
                    1,
                )?
            }
        }
        assert!(ParkCertificate::assemble(route, cut.clone(), leader, &voters, replies).is_err());
    }
    Ok(())
}

async fn bootstrap(
    domain: &ExecutionDomain,
    log: &ReplicaLog,
    voters: u8,
) -> anyhow::Result<CellGroupBootstrapPermit> {
    let route = marker(1).route().clone();
    let catalog = Catalog::new(
        Arc::new(object_store::memory::InMemory::new()),
        CatalogOptions::default(),
    )?;
    let mut config = NamespaceConfig::new(
        route.history.cluster,
        route.history.namespace,
        cellar_logic::NamespaceMode::Raft,
        route.schema_digest,
    );
    if voters == 1 {
        config.voters = VoterPolicy::SingleVoterDevelopment;
    }
    let namespace = domain.run(catalog.open_namespace(config)).await??;
    let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let request = directory.prepare_group_birth(CellGroupBirthSpec {
        history: route.history,
        range: route.range,
        epoch: route.epoch,
        creation: ProposalId::new([16; 16])?,
        voters: (1..=voters)
            .map(|id| -> anyhow::Result<_> {
                Ok(InitialMember {
                    member: MemberId::new(u64::from(id))?,
                    node: NodeId::new([id + 10; 16])?,
                })
            })
            .collect::<anyhow::Result<_>>()?,
        learners: vec![],
    })?;
    domain
        .run(directory.reserve_group_birth(&request))
        .await??;
    let initialize =
        directory.prepare_initialization(ProposalId::new([17; 16])?, &[request.route().clone()])?;
    domain.run(directory.publish(&initialize)).await??;
    let birth = domain
        .run(directory.materialize_group_birth(&request))
        .await??;
    Ok(domain
        .run(directory.bootstrap_group_member(&birth, log))
        .await??)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn park_acknowledgement_waits_for_sync_and_reopens_closed() -> anyhow::Result<()> {
    use crate::{
        application::consensus::group_protocol::{
            CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
        },
        raft::{
            cell_groups::{CellGroupEffect, CellGroupRuntime, ManifestPageStore, ManifestTree},
            completion::RuntimeError,
            options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
            storage_fault as fault,
        },
    };
    use ::raft::prelude::{Entry, HardState, Message, MessageType};
    use cellar_logic::consensus::DiskLineage;
    use cellar_ltx::DirectFileSystem;
    use std::time::Duration;

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
    let member = MemberId::new(1)?;
    let leader = MemberId::new(2)?;
    let permit = bootstrap(&domain, &log, 3).await?;
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
        certificate: None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leader_fences_park_preparation_through_timeout_apply() -> anyhow::Result<()> {
    use crate::raft::{
        cell_groups::{
            CellGroupApplier, CellGroupApplyImages, CellGroupEffect, CellGroupRuntime,
            ManifestPageStore, ManifestTree,
        },
        completion::RuntimeError,
        options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
    };
    use anyhow::Context as _;
    use cellar_logic::consensus::DiskLineage;
    use cellar_ltx::{
        generation::{GenerationStore, ReadServices},
        DirectFileSystem,
    };
    use std::time::Duration;

    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("log"),
                NodeId::new([11; 16])?,
                DiskLineage::new([12; 16])?,
                ProcessGeneration::new([13; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let permit = bootstrap(&domain, &log, 1).await?;
    let history = permit.route().history;
    let member = permit.group().identity().member;
    let group = permit.group().clone();
    let page_directory = root.path().join("pages");
    let images_directory = root.path().join("images");
    let (applier, genesis) = domain
        .blocking(move || -> anyhow::Result<_> {
            let mut pages = ManifestPageStore::open(
                &page_directory,
                Arc::new(DirectFileSystem),
                128,
                1024 * 1024,
                16,
            )?;
            let marker = group.group_manifest()?.context("genesis marker")?;
            let tree = ManifestTree::new(marker.route().clone(), Default::default())?;
            let genesis = tree.prepare_complete(
                &group,
                &mut pages,
                marker.root(),
                marker.position(),
                marker.membership_revision(),
                |_| unreachable!("empty genesis"),
            )?;
            let images = CellGroupApplyImages::Materialized {
                images: GenerationStore::open(
                    images_directory,
                    Arc::new(DirectFileSystem),
                    Default::default(),
                )?,
                services: ReadServices {
                    now_unix_millis: Arc::new(crate::asyncrt::wall_ms),
                    fill_random: Arc::new(|bytes| bytes.fill(17)),
                },
            };
            let applier = CellGroupApplier::new(
                group,
                pages,
                images,
                Default::default(),
                Default::default(),
                32,
            )?;
            Ok((applier, genesis))
        })?
        .await??;
    let (runtime, mut events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([19; 32]),
            ..Default::default()
        },
    )
    .await?;
    runtime.attach(permit, genesis).await?;
    runtime.campaign(history).await?;

    let mut cut = None;
    for phase in ["election", "park", "timeout"] {
        let event = domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(10),
                events.recv(),
            ))
            .await??
            .context("expected committed apply")?;
        let work = event.retain_apply().context("apply effect")?;
        drop(event);
        if phase == "timeout" {
            assert!(work.entry.data.is_empty());
            assert!(!runtime.status(history).await?.ready);
        }
        let prepare = work.clone();
        let applier = applier.clone();
        let next = domain
            .blocking(move || applier.prepare(&prepare))?
            .await??;
        runtime.complete_apply(history, work, next).await?;
        let event = domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(10),
                events.recv(),
            ))
            .await??
            .context("expected durable apply")?;
        let Ok(CellGroupEffect::Applied { manifest, .. }) = event.effect() else {
            anyhow::bail!("unexpected cell-group effect");
        };
        let selected = manifest.park_cut().cloned();
        let status = runtime.status(history).await?;
        match phase {
            "election" => {
                assert!(status.ready);
                let request = ParkPrepareRequest {
                    proposal: ProposalId::new([20; 16])?,
                    term: status.term,
                    membership: MembershipRevision::new(manifest.membership_revision())?,
                    deadline_ms: log.domain().mono_ms()? + MAX_PARK_NEGOTIATION_MS,
                };
                let stale = ParkPrepareRequest {
                    term: Term::new(status.term.get() - 1),
                    ..request
                };
                assert!(matches!(
                    runtime.prepare_park(history, stale).await,
                    Err(RuntimeError::StaleCompletion)
                ));
                let prepared = runtime.prepare_park(history, request).await?;
                let duplicate = runtime.prepare_park(history, request).await?;
                assert_eq!(prepared.proposal.position, duplicate.proposal.position);
                assert!(prepared.next_due_unix_millis.is_none());
                assert!(!runtime.status(history).await?.ready);
                cut = Some((request, prepared.proposal.position));
            }
            "park" => {
                let (request, position) = cut.context("park preparation")?;
                let selected = selected.context("committed park cut")?;
                assert_eq!(selected.position(), position);
                assert!(!status.ready);
                assert!(matches!(
                    runtime.read_index(history, vec![1]).await,
                    Err(RuntimeError::NotReady)
                ));
                let receipt = runtime.acknowledge_park(history, member, selected).await?;
                assert_eq!(receipt.cut.position(), position);
                let replica = receipt.voter.clone();
                let sequence = receipt.sync_sequence;
                let certificate = ParkCertificate::assemble(
                    manifest.route(),
                    receipt.cut.clone(),
                    &replica,
                    std::slice::from_ref(&replica),
                    vec![receipt],
                )?;
                let certified = runtime.certify_park(history, certificate.clone()).await?;
                assert_eq!(certified.certificate(), &certificate);
                assert!(certified.sync_sequence() > sequence);
                let retry = runtime.certify_park(history, certificate.clone()).await?;
                assert_eq!(retry.certificate(), &certificate);

                let later = runtime
                    .acknowledge_park(history, member, certificate.cut().clone())
                    .await?;
                let conflict = ParkCertificate::assemble(
                    manifest.route(),
                    later.cut.clone(),
                    &replica,
                    std::slice::from_ref(&replica),
                    vec![later],
                )?;
                assert_ne!(conflict, certificate);
                assert!(matches!(
                    runtime.certify_park(history, conflict).await,
                    Err(RuntimeError::Storage(LogError::WrongIdentity))
                ));
                let group = log.cell_group(&history)?;
                assert_eq!(
                    group
                        .read()?
                        .local_park
                        .as_ref()
                        .and_then(LocalParkRecord::certificate),
                    Some(&certificate)
                );
                domain
                    .run(async { crate::asyncrt::sleep_until(request.deadline_ms).await })
                    .await?;
                runtime.tick().await?;
            }
            _ => {
                let (_, position) = cut.context("park preparation")?;
                let selected = selected.context("retained park cut")?;
                assert_eq!(selected.position(), position);
                assert!(status.ready);
                assert_eq!(status.applied.index.get(), position.index.get() + 1);
                assert!(matches!(
                    runtime.acknowledge_park(history, member, selected).await,
                    Err(RuntimeError::NotReady)
                ));
            }
        }
    }
    runtime.close().await?;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
