// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Whole-group configuration changes over actual TCP Raft and production apply.
use super::*;
use cellar::application::consensus::verification::JoiningMember;
use cellar_logic::consensus::OperationId;
use raft::{
    prelude::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2},
    Storage,
};

pub(super) fn change(
    member: u64,
    kind: ConfChangeType,
    transition: ConfChangeTransition,
) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            node_id: member,
            change_type: kind,
            ..Default::default()
        }]
        .into(),
        transition,
        ..Default::default()
    }
}

pub(super) async fn configure_at(
    fixture: &RuntimeCluster,
    leader: usize,
    route: &GroupRoute,
    command: ConfChangeV2,
    joins: &[JoiningMember],
) -> Result<CellGroupProposal> {
    let group = fixture.cluster.voters[leader]
        .log
        .cell_group(&route.history)?;
    let label = format!("{command:?}");
    let request = CellGroupMembershipChange::new(route, &group.membership()?, command, joins)?;
    let accepted = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match fixture.runtimes[leader]
                .configure(route.history, request.clone())
                .await
            {
                Ok(accepted) => return Ok::<_, anyhow::Error>(accepted),
                // Retry only a definite pre-admission refusal, preserving the
                // same observed revision. A lost response is never retried.
                Err(RuntimeError::NotReady) => tokio::task::yield_now().await,
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await??;
    controller::applied(fixture, route.history, accepted.position.index)
        .await
        .with_context(|| format!("configuration apply: {label}"))?;
    Ok(accepted)
}

pub(super) async fn configure(
    fixture: &RuntimeCluster,
    route: &GroupRoute,
    command: ConfChangeV2,
    joins: &[JoiningMember],
) -> Result<CellGroupProposal> {
    configure_at(fixture, 0, route, command, joins).await
}

/// Hold an actual runtime message across a committed membership change. The
/// configuration may not invalidate the old physical send's membership binding.
pub(crate) async fn membership_delivery_fence(rooted: bool) -> Result<()> {
    membership_delivery_fence_case(rooted, false).await
}

pub(crate) async fn membership_physical_delivery_fence(rooted: bool) -> Result<()> {
    membership_delivery_fence_case(rooted, true).await
}

async fn membership_delivery_fence_case(rooted: bool, physical: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    for _ in 0..16 {
        fixture.pump(&[], &mut BTreeMap::new()).await?;
        tokio::task::yield_now().await;
    }
    let (transports, sink) = if physical {
        // Declare the sink before any pool is installed. A physical node has
        // one endpoint across groups; replacing one group's existing endpoint
        // would correctly be refused as a stale peer before admitting a send.
        for (node, runtime) in fixture.runtimes.iter().enumerate() {
            fixture.transports.push(tcp::start(runtime, node).await?);
        }
        let sink = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        for history in fixture.applied.keys() {
            let mut route = fixture.tcp_route(*history)?;
            route.peers[2].control_address = sink.local_addr()?;
            fixture.transports[0].install_physical_group(route)?;
        }
        (std::mem::take(&mut fixture.transports), Some(sink))
    } else {
        (Vec::new(), None)
    };
    for _ in 0..2 {
        fixture.runtimes[0].tick().await?;
    }
    let prior = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = fixture.events[0].recv().await.context("leader event")?;
            match event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))? {
                CellGroupEffect::Message(message) if message.to == 3 => {
                    return Ok::<_, anyhow::Error>(event)
                }
                CellGroupEffect::Message(message) => {
                    fixture.network.push_back((route.history, message.clone()))
                }
                _ => anyhow::bail!("expected settled protocol message"),
            }
        }
    })
    .await??;
    let (prior, socket) = if let Some(sink) = sink {
        let delivery = transports[0].submit_cell_group_event(&prior)?;
        let (socket, _) = tokio::time::timeout(Duration::from_secs(5), sink.accept()).await??;
        // The physical handshake is entered. Dropping both response owners must
        // not refund the runtime admission still held by the actual SendRequest.
        drop(delivery);
        drop(prior);
        (None, Some(socket))
    } else {
        (Some(prior), None)
    };
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    let request = CellGroupMembershipChange::new(
        &route,
        &group.membership()?,
        change(
            3,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Explicit,
        ),
        &[],
    )?;
    let accepted = fixture.runtimes[0]
        .configure(route.history, request)
        .await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while group.initial_state()?.hard_state.commit < accepted.position.index.get() {
            fixture.pump(&[route.history], &mut held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    // A membership fence is group-local even though sends, persistence and
    // output admission are node-wide. Elect/apply another group while held.
    fixture.runtimes[0].campaign(second).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture.pump(&[route.history], &mut held).await?;
            let status = fixture.runtimes[0].status(second).await?;
            if status.ready
                && fixture.applied[&second]
                    .iter()
                    .all(|i| *i >= status.applied.index)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    // Inject later protocol work while the finite prior-send boundary is held.
    for _ in 0..8 {
        fixture.runtimes[0].tick().await?;
        fixture.pump(&[route.history], &mut held).await?;
    }
    assert!(
        !held.contains_key(&route.history),
        "configuration Apply overtook an owned prior message"
    );
    assert!(group.membership()?.revision() < accepted.position.index);
    drop(prior);
    drop(socket);
    let notices = tokio::time::timeout(Duration::from_secs(5), async {
        let mut notices = Vec::new();
        while notices.len() < 2 {
            let event = fixture.events[0]
                .recv()
                .await
                .context("commit announcement")?;
            if event.group() != route.history {
                match event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))? {
                    CellGroupEffect::Message(message) => {
                        fixture.network.push_back((event.group(), message.clone()))
                    }
                    CellGroupEffect::Applied { manifest, .. } => {
                        fixture
                            .applied
                            .get_mut(&event.group())
                            .context("other group")?[0] = manifest.position().index
                    }
                    _ => anyhow::bail!(
                        "unexpected other-group work while awaiting configuration announcement"
                    ),
                }
                continue;
            }
            let CellGroupEffect::Message(message) =
                event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))?
            else {
                anyhow::bail!("configuration skipped terminal message fence");
            };
            assert_eq!(
                message.get_msg_type(),
                MessageType::MsgAppend,
                "fenced group emitted a nonterminal message after releasing its prior send"
            );
            assert!(message.entries.is_empty());
            assert_eq!(
                (message.index, message.log_term, message.commit),
                (
                    accepted.position.index.get(),
                    accepted.position.term.get(),
                    accepted.position.index.get()
                )
            );
            notices.push(event);
        }
        let targets = notices
            .iter()
            .map(|event| match event.effect().unwrap() {
                CellGroupEffect::Message(message) => message.to,
                _ => unreachable!(),
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(targets, std::collections::BTreeSet::from([2, 3]));
        Ok::<_, anyhow::Error>(notices)
    })
    .await??;
    for _ in 0..2 {
        fixture.runtimes[0].tick().await?;
        fixture.pump(&[route.history], &mut held).await?;
    }
    assert!(
        !held.contains_key(&route.history),
        "configuration skipped its terminal announcement deliveries"
    );
    drop(notices);
    fixture.drive(Some(accepted.position.index)).await?;
    assert_eq!(group.membership()?.revision(), accepted.position.index);
    fixture.transports = transports;
    fixture.close().await
}

pub(crate) async fn membership_lifecycle(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    fixture.connect_tcp().await?;
    for transport in &fixture.transports {
        transport.enable_group_checkpoints(CellGroupCheckpointPool::new(
            checkpoint_fixture::limits(),
        )?)?;
    }
    let mut limits = controller::limits();
    limits.checkpoints = Some(CellGroupCheckpointPolicy {
        interval_entries: 2,
        max_parallel: 1,
    });
    let controllers = controller::start_with_limits(&mut fixture, limits).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    // Administrative observations address the physical group, with no chosen
    // cell. Every invocation obtains its own current-term ReadIndex barrier.
    let deadline = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() + 10_000 })
        .await?;
    let observation = fixture.runtimes[0]
        .quorum_membership(route.history, deadline)
        .await?;
    assert_eq!(observation.replica().scope, route.history);
    assert_eq!(observation.root().route(), &route);
    assert_eq!(observation.configuration().voters, vec![1, 2, 3]);
    assert!(observation.barrier().index.get() > 0);
    assert!(observation.root().position().index >= observation.barrier().index);
    assert!(observation.membership().revision() <= observation.barrier().index);
    assert!(fixture.runtimes[0].usage().read_pins.items > 0);
    drop(observation);
    let expired = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await?;
    assert!(matches!(
        fixture.runtimes[0]
            .quorum_membership(route.history, expired)
            .await,
        Err(RuntimeError::DeadlineExceeded)
    ));
    let original = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let root = group.group_manifest()?.context("root")?.root();
    let demote = change(
        3,
        ConfChangeType::AddLearnerNode,
        ConfChangeTransition::Explicit,
    );
    let stale = CellGroupMembershipChange::new(&route, &group.membership()?, demote.clone(), &[])?;
    let foreign = fixture.extra[&second][0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("second root")?;
    let wrong_group =
        CellGroupMembershipChange::new(foreign.route(), &group.membership()?, demote.clone(), &[])?;
    let last = group.last_index()?;
    assert!(fixture.runtimes[0]
        .configure(route.history, wrong_group)
        .await
        .is_err());
    assert_eq!(group.last_index()?, last);
    let entered = configure(&fixture, &route, demote, &[]).await?;
    let deadline = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() + 10_000 })
        .await?;
    let observation = fixture.runtimes[0]
        .quorum_membership(route.history, deadline)
        .await?;
    assert_eq!(observation.membership().revision(), entered.position.index);
    assert!(observation.barrier().index >= entered.position.index);
    assert_eq!(observation.configuration().voters, vec![1, 2]);
    assert_eq!(observation.configuration().voters_outgoing, vec![1, 2, 3]);
    assert_eq!(observation.configuration().learners_next, vec![3]);
    assert_eq!(observation.root().root(), root);
    drop(observation);
    for voter in &fixture.cluster.voters {
        let log = voter.log.cell_group(&route.history)?;
        let conf = log.initial_state()?.conf_state;
        assert_eq!(conf.voters, vec![1, 2]);
        assert_eq!(conf.voters_outgoing, vec![1, 2, 3]);
        assert_eq!(conf.learners_next, vec![3]);
        assert_eq!(log.membership()?.revision(), entered.position.index);
        assert_eq!(log.group_manifest()?.context("root")?.root(), root);
    }
    let last = group.last_index()?;
    assert!(fixture.runtimes[0]
        .configure(route.history, stale)
        .await
        .is_err());
    assert_eq!(group.last_index()?, last);
    let last = group.last_index()?;
    let term = fixture.runtimes[0].status(route.history).await?.term.get();
    let mut malformed = group
        .entries(
            entered.position.index.get(),
            entered.position.index.get() + 1,
            None::<u64>,
            raft::GetEntriesContext::empty(false),
        )?
        .remove(0);
    let mut bytes = malformed.data.to_vec();
    bytes.extend_from_slice(&[8, 0]); // Duplicate transition with conflicting value.
    malformed.data = bytes.into();
    malformed.index = last + 1;
    malformed.term = term + 1;
    let message = raft::prelude::Message {
        msg_type: raft::prelude::MessageType::MsgAppend,
        from: 2,
        to: 1,
        term: term + 1,
        index: last,
        log_term: group.term(last)?,
        entries: vec![malformed].into(),
        ..Default::default()
    };
    assert!(fixture.runtimes[0]
        .step(route.history, message)
        .await
        .is_err());
    assert_eq!(
        fixture.runtimes[0].status(route.history).await?.term.get(),
        term
    );
    assert_eq!(group.last_index()?, last);
    configure(&fixture, &route, ConfChangeV2::default(), &[]).await?;
    assert_eq!(group.initial_state()?.conf_state.learners, vec![3]);

    let join = JoiningMember {
        member: MemberId::new(4)?,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([14; 16])?,
    };
    configure(
        &fixture,
        &route,
        change(
            4,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Auto,
        ),
        std::slice::from_ref(&join),
    )
    .await?;
    assert_eq!(
        group
            .membership()?
            .joined(join.member)?
            .context("bound learner")?
            .0,
        join
    );
    // Checkpoint sends have the same retryable route-discovery refusal as
    // ordinary append traffic; no stream or source owner is admitted.
    let snapshot = raft::prelude::Message {
        msg_type: raft::prelude::MessageType::MsgSnapshot,
        from: 1,
        to: 4,
        ..Default::default()
    };
    assert!(matches!(
        fixture.transports[0].submit_physical(&route.history, snapshot),
        Err(TransportError::StalePeer)
    ));
    controller::wait_for(async || Ok(controllers[0].usage().send_refusals > 0)).await?;
    assert!(
        !controllers[0].usage().closing,
        "discovery lag must not close the node"
    );
    let promotion = CellGroupMembershipChange::new(
        &route,
        &group.membership()?,
        change(4, ConfChangeType::AddNode, ConfChangeTransition::Auto),
        &[],
    )?;
    let last = group.last_index()?;
    assert!(matches!(
        fixture.runtimes[0]
            .configure(route.history, promotion)
            .await,
        Err(RuntimeError::NotReady)
    ));
    assert_eq!(group.last_index()?, last);
    configure(
        &fixture,
        &route,
        change(3, ConfChangeType::AddNode, ConfChangeTransition::Auto),
        &[],
    )
    .await?;
    configure(
        &fixture,
        &route,
        change(4, ConfChangeType::RemoveNode, ConfChangeTransition::Auto),
        &[],
    )
    .await?;
    let reused = CellGroupMembershipChange::new(
        &route,
        &group.membership()?,
        change(
            4,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Auto,
        ),
        &[join],
    )?;
    assert!(fixture.runtimes[0]
        .configure(route.history, reused)
        .await
        .is_err());
    assert_eq!(group.membership()?.high_water(), 4);

    // Implicit joint consensus must generate and durably apply its own empty
    // leave entry. The harness never manufactures that automatic command.
    let implicit = configure(
        &fixture,
        &route,
        change(
            3,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Implicit,
        ),
        &[],
    )
    .await?;
    controller::wait_for(async || {
        for voter in &fixture.cluster.voters {
            let log = voter.log.cell_group(&route.history)?;
            if log.membership()?.revision() <= implicit.position.index
                || !log.initial_state()?.conf_state.voters_outgoing.is_empty()
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
    .context("automatic joint leave did not finish")?;
    configure(
        &fixture,
        &route,
        change(3, ConfChangeType::AddNode, ConfChangeTransition::Auto),
        &[],
    )
    .await?;
    assert_eq!(
        fixture.runtimes[0]
            .reconciliation_authority(route.history, a.clone())
            .await?
            .cell(),
        original.cell()
    );

    let progress = fixture.runtimes[0]
        .replication_status(route.history)
        .await?;
    assert_eq!(progress.replica.scope, route.history);
    assert_eq!(progress.replica.member, MemberId::new(1)?);
    for (term, revision) in [
        (
            Term::new(progress.term.get() + 1),
            progress.membership_revision,
        ),
        (
            progress.term,
            LogIndex::new(progress.membership_revision.get() + 1),
        ),
    ] {
        assert!(matches!(
            fixture.runtimes[0]
                .transfer_leadership(
                    route.history,
                    cellar::application::consensus::LeadershipTransfer {
                        target: MemberId::new(2)?,
                        expected_term: term,
                        expected_revision: revision
                    }
                )
                .await,
            Err(RuntimeError::Invalid(_))
        ));
    }

    // Move leadership using the real TimeoutNow protocol. The fixture does
    // not advance election ticks or inject votes to manufacture this handoff.
    assert!(fixture.runtimes[0]
        .transfer_leader(route.history, 4)
        .await
        .is_err());
    controller::wait_for(async || {
        match fixture.runtimes[0].transfer_leader(route.history, 2).await {
            Ok(()) => Ok(true),
            Err(RuntimeError::NotReady) => Ok(false),
            Err(error) => Err(error.into()),
        }
    })
    .await?;
    assert!(original.check().is_err());
    controller::wait_for(async || Ok(fixture.runtimes[1].status(route.history).await?.ready))
        .await?;
    let producer = fixture.runtimes[1]
        .execution_authority(route.history, a.clone())
        .await?;
    let cell = history(&route, "after-new-leader")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(73);")?;
    let accepted = fixture.runtimes[1]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &cell, &bytes, rooted)?,
        )
        .await?;
    controller::applied(&fixture, route.history, accepted.position.index).await?;
    // Demote the new current leader. Captured producer authority must be
    // revoked while another group on the original node continues serving.
    configure_at(
        &fixture,
        1,
        &route,
        change(
            2,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Explicit,
        ),
        &[],
    )
    .await?;
    configure_at(&fixture, 1, &route, ConfChangeV2::default(), &[]).await?;
    assert!(producer.check().is_err());
    assert!(!fixture.runtimes[1].status(route.history).await?.ready);
    assert!(fixture.runtimes[0].status(second).await?.ready);
    let second_root = fixture.cluster.voters[0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("second")?;
    let cell = history(second_root.route(), "unrelated")?;
    let accepted = fixture.runtimes[0]
        .propose(
            second,
            apply::enrollment_bytes(second_root.route(), &cell, &bytes, false)?,
        )
        .await?;
    controller::applied(&fixture, second, accepted.position.index).await?;
    let conf = group.initial_state()?.conf_state;
    let membership = group.membership()?;
    assert_eq!(conf.voters, vec![1, 3]);
    assert_eq!(conf.learners, vec![2]);
    assert_eq!(membership.high_water(), 4);
    assert!(group.checkpoint_manifest()?.is_some());
    for controller in &controllers {
        assert_eq!(controller.usage().quarantined_groups, 0);
        controller.close().await?;
    }
    drop(controllers);
    fixture.cluster.voters[0].log.close().await?;
    let reopened = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                fixture.cluster._root.path().join("node-1/log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([11; 16])?,
                ProcessGeneration::new([21; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let restored = reopened.cell_group(&route.history)?;
    assert_eq!(restored.initial_state()?.conf_state, conf);
    assert_eq!(restored.membership()?, membership);
    assert_eq!(
        restored
            .group_manifest()?
            .context("restored root")?
            .membership_revision(),
        membership.revision().get()
    );
    // Metadata reopening alone is not permission to serve or a disk-replacement proof.
    reopened.close().await?;
    fixture.close().await
}

pub(crate) async fn membership_sync() -> Result<()> {
    for close in [false, true] {
        let class = RaftClass::new("Counter", 1, [81; 32])?;
        let (mut fixture, route, _, _) = Box::pin(registered::setup(false, &class)).await?;
        let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("before")?;
        let membership = group.membership()?;
        let conf = group.initial_state()?.conf_state;
        let request = CellGroupMembershipChange::new(
            &route,
            &membership,
            change(
                3,
                ConfChangeType::AddLearnerNode,
                ConfChangeTransition::Explicit,
            ),
            &[],
        )?;
        let accepted = fixture.runtimes[0]
            .configure(route.history, request)
            .await?;
        let mut held = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !held.contains_key(&route.history) {
                fixture.pump(&[route.history], &mut held).await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        let work = held.remove(&route.history).context("configuration apply")?;
        let mut incomplete = GroupWrite::new(&group);
        incomplete.conf_state = Some(conf.clone());
        incomplete.membership = Some(membership.clone());
        assert!(fixture.cluster.voters[0]
            .log
            .persist_cells(OperationId::new(991), vec![incomplete])
            .await
            .is_err());
        let (sql, applier) = fixture.appliers[&(0, route.history)].clone();
        let prepared = sql.prepare_apply(&applier, work).await?;
        let gate = fixture.cluster.voters[0].gate.clone();
        let release = gate.arm(fault::HOLD_SYNC);
        prepared.complete(&fixture.runtimes[0]).await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        assert_eq!(group.initial_state()?.conf_state, conf);
        assert_eq!(group.membership()?, membership);
        assert_eq!(
            fixture.runtimes[0].status(route.history).await?.applied,
            before.position()
        );
        if close {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), fixture.runtimes[0].close())
                    .await
                    .is_err()
            );
        }
        drop(release);
        if close {
            fixture.runtimes[0].close().await?;
        } else {
            fixture.drive(Some(accepted.position.index)).await?;
        }
        assert_eq!(group.membership()?.revision(), accepted.position.index);
        assert_eq!(
            group
                .group_manifest()?
                .context("after")?
                .membership_revision(),
            accepted.position.index.get()
        );
        assert_eq!(group.initial_state()?.conf_state.learners_next, vec![3]);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn membership_quorum_floor() -> Result<()> {
    for rooted in [false, true] {
        let class = RaftClass::new("Counter", 1, [81; 32])?;
        let (mut fixture, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
        fixture.connect_tcp().await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let runtime = fixture.runtimes[0].clone();
        controller::wait_for(async || Ok(runtime.status(route.history).await?.ready)).await?;
        let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("before")?;
        let request = CellGroupMembershipChange::new(
            &route,
            &group.membership()?,
            change(
                3,
                ConfChangeType::AddLearnerNode,
                ConfChangeTransition::Explicit,
            ),
            &[],
        )?;
        let gate = fixture.cluster.voters[0].gate.clone();
        let release = gate.arm(fault::HOLD_SYNC);
        let accepted = runtime.configure(route.history, request).await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        let deadline = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 10_000 })
            .await?;
        let read =
            runtime.quorum_membership_after(route.history, accepted.position.index, deadline);
        tokio::pin!(read);
        assert!(tokio::time::timeout(Duration::from_millis(30), &mut read)
            .await
            .is_err());
        assert!(runtime.usage().read_pins.items > 0);
        drop(release);
        let observed = tokio::time::timeout(Duration::from_secs(10), &mut read).await??;
        assert_eq!(observed.membership().revision(), accepted.position.index);
        assert!(observed.barrier().index >= accepted.position.index);
        assert!(observed.root().position().index >= observed.barrier().index);
        assert_eq!(observed.configuration().learners_next, vec![3]);
        drop(observed);
        controller::wait_for(async || Ok(runtime.usage().read_pins.items == 0)).await?;
        let unreachable = LogIndex::new(accepted.position.index.get() + 10_000);
        let deadline = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 30 })
            .await?;
        assert!(matches!(
            runtime
                .quorum_membership_after(route.history, unreachable, deadline)
                .await,
            Err(RuntimeError::DeadlineExceeded)
        ));
        controller::wait_for(async || Ok(runtime.usage().read_pins.items == 0)).await?;
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn membership_quorum_followers() -> Result<()> {
    use raft::prelude::{Entry, Message, MessageType};
    for rooted in [false, true] {
        let class = RaftClass::new("Counter", 1, [81; 32])?;
        let (mut fixture, route, cell, _) = Box::pin(registered::setup(rooted, &class)).await?;
        fixture.connect_tcp().await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        controller::wait_for(async || Ok(fixture.runtimes[0].status(route.history).await?.ready))
            .await?;
        let accepted = configure_at(
            &fixture,
            0,
            &route,
            change(
                3,
                ConfChangeType::AddLearnerNode,
                ConfChangeTransition::Explicit,
            ),
            &[],
        )
        .await?;
        let expected = fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("selected group")?;
        assert_eq!(expected.root().cells(), 2);

        let mut context = b"cellar-group-membership-read-1\0".to_vec();
        context.extend_from_slice(&2u64.to_be_bytes());
        context.extend_from_slice(&[97; 16]);
        context.extend_from_slice(&1u64.to_be_bytes());
        let status = fixture.runtimes[1].status(route.history).await?;
        let forged = Message {
            msg_type: MessageType::MsgReadIndexResp,
            from: 3,
            to: 2,
            term: status.term.get(),
            index: expected.position().index.get(),
            entries: vec![Entry {
                data: context.into(),
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        assert!(matches!(
            fixture.runtimes[1].step(route.history, forged).await,
            Err(RuntimeError::Invalid(_))
        ));

        for node in 0..3 {
            let runtime = &fixture.runtimes[node];
            let deadline = fixture
                .cluster
                .domain
                .run(async { cellar::asyncrt::mono_ms() + 10_000 })
                .await?;
            let observed = runtime
                .quorum_membership_after(route.history, accepted.position.index, deadline)
                .await?;
            assert_eq!(observed.replica().member.get(), node as u64 + 1);
            assert_eq!(
                observed.engine(),
                fixture.cluster.voters[node].log.storage_instance()?
            );
            assert_eq!(observed.root(), &expected);
            assert!(observed.barrier().index >= accepted.position.index);
            assert_eq!(observed.barrier().term, observed.status().term);
            assert_eq!(observed.status().leader, Some(1));
            assert_eq!(observed.configuration().learners_next, vec![3]);
            if node != 0 {
                assert!(!observed.status().ready);
                // A metadata barrier cannot become local leader/execution or
                // membership-transfer authority on either follower.
                assert!(matches!(
                    runtime.replication_status(route.history).await,
                    Err(RuntimeError::NotLeader)
                ));
                assert!(matches!(
                    runtime
                        .execution_authority(route.history, cell.clone())
                        .await,
                    Err(RuntimeError::NotLeader)
                ));
            }
            drop(observed);
            controller::wait_for(async || Ok(runtime.usage().read_pins.items == 0)).await?;
        }
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}
