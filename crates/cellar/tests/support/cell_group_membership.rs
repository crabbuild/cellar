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
