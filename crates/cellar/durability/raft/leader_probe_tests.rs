// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Coalesced discovery retains physical ownership, quotas and caller deadlines.
#![allow(clippy::disallowed_methods)]
use super::super::shared_fixture::Fixture;
use super::*;
use cellar_logic::cell_group::{CellRange, RouteEpoch};
use cellar_logic::consensus::{GroupId, IncarnationId};
use futures_util::FutureExt;

#[test]
fn busy_or_no_leader_retries_the_installed_placement() {
    assert_eq!(
        ProbeFailures {
            overloaded: true,
            ..Default::default()
        }
        .repair(),
        LeaderProbeOutcome::Retry(CommandFailure::Overloaded)
    );
    assert_eq!(
        ProbeFailures {
            no_leader: true,
            ..Default::default()
        }
        .repair(),
        LeaderProbeOutcome::Retry(CommandFailure::NotLeader { leader: None })
    );
    assert_eq!(
        ProbeFailures {
            no_leader: true,
            overloaded: true,
            ..Default::default()
        }
        .repair(),
        LeaderProbeOutcome::Retry(CommandFailure::NotLeader { leader: None })
    );
}

#[test]
fn unreachable_probes_preserve_placement_and_do_not_infer_membership_change() {
    for error in [
        TransportError::Timeout,
        TransportError::Io("connection reset".into()),
    ] {
        let mut failures = ProbeFailures::default();
        failures.observe(error).unwrap();
        assert_eq!(
            failures.repair(),
            LeaderProbeOutcome::Retry(CommandFailure::Unavailable)
        );
    }
}

#[test]
fn missing_or_stale_members_need_membership_discovery() {
    assert_eq!(
        ProbeFailures::default().repair(),
        LeaderProbeOutcome::Membership
    );
    for (no_leader, overloaded) in [(false, false), (true, false), (false, true), (true, true)] {
        assert_eq!(
            ProbeFailures {
                membership: true,
                unreachable: true,
                no_leader,
                overloaded
            }
            .repair(),
            LeaderProbeOutcome::Membership
        );
    }
    for error in [TransportError::UnknownGroup, TransportError::StalePeer] {
        let mut failures = ProbeFailures::default();
        failures.observe(error).unwrap();
        assert_eq!(failures.repair(), LeaderProbeOutcome::Membership);
    }
}

#[test]
fn probe_authority_failures_preserve_the_terminal_cause() {
    for error in [
        TransportError::Identity,
        TransportError::Authentication,
        TransportError::Refused,
    ] {
        let mut failures = ProbeFailures::default();
        assert!(matches!(
            failures.observe(error.clone()),
            Err(actual) if actual == error
        ));
    }
}

fn captured() -> (CellRoute, Arc<GroupRoute<GroupHistory>>, Key) {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let route = CellRoute {
        history,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    };
    let view = Arc::new(GroupRoute {
        scope: history,
        schema_digest: route.schema_digest,
        revision: 1,
        peers: vec![],
    });
    let key = Key {
        history,
        route: Arc::downgrade(&view),
        configuration: None,
        epoch: route.epoch.get(),
        birth_digest: route.birth_digest,
    };
    (route, view, key)
}

#[tokio::test]
async fn cancelled_waiters_leave_owner_and_delivery_charged() -> anyhow::Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let state = &fixture.transport.handle.0;
    let (_, _view, key) = captured();
    let baseline = state.outgoing.control.usage();
    let tasks = state.usage().tasks;
    let mut first = state.leader_probes.admit(state, key.clone())?;
    let (owner, guard) = first.owner.take().unwrap();
    let second = state.leader_probes.admit(state, key.clone())?;
    let third = state.leader_probes.admit(state, key)?;
    assert!(second.owner.is_none() && third.owner.is_none());
    assert_eq!(state.usage().tasks, tasks + 1);
    assert_eq!(
        state.outgoing.control.usage(),
        (baseline.0 + OWNER_BYTES + 2 * WAITER_BYTES, baseline.1 + 3)
    );
    drop(first);
    drop(second);
    assert_eq!(
        state.outgoing.control.usage(),
        (baseline.0 + OWNER_BYTES + WAITER_BYTES, baseline.1 + 2)
    );
    owner.complete(Ok(LeaderProbeOutcome::Leader(MemberId::new(2)?)));
    drop(guard);
    assert!(state
        .leader_probes
        .shared
        .pending
        .lock()
        .unwrap()
        .is_empty());
    assert_eq!(state.usage().tasks, tasks);
    // Joined physical work leaves retained output charged until its last reader.
    assert_eq!(
        state.outgoing.control.usage().0,
        baseline.0 + OWNER_BYTES + WAITER_BYTES
    );
    let deadline = state.domain.mono_ms()? + 1000;
    assert_eq!(
        third.wait(state, deadline).await?,
        LeaderProbeOutcome::Leader(MemberId::new(2)?)
    );
    drop(third);
    assert_eq!(state.outgoing.control.usage(), baseline);
    assert_eq!(state.leader_probes.limit.usage(), (0, 0));
    fixture.close().await
}

#[tokio::test]
async fn shorter_joiner_deadline_does_not_cancel_the_owner() -> anyhow::Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let state = &fixture.transport.handle.0;
    let (_, _view, key) = captured();
    let mut first = state.leader_probes.admit(state, key.clone())?;
    let (owner, guard) = first.owner.take().unwrap();
    let second = state.leader_probes.admit(state, key)?;
    assert_eq!(
        second.wait(state, state.domain.mono_ms()? + 10).await,
        Err(TransportError::Timeout)
    );
    drop(second);
    assert_eq!(state.leader_probes.limit.usage(), (OWNER_BYTES, 1));
    assert_eq!(state.leader_probes.shared.pending.lock().unwrap().len(), 1);
    owner.complete(Ok(LeaderProbeOutcome::Retry(CommandFailure::Unavailable)));
    drop(guard);
    assert_eq!(
        first.wait(state, state.domain.mono_ms()? + 1000).await?,
        LeaderProbeOutcome::Retry(CommandFailure::Unavailable)
    );
    drop(first);
    assert_eq!(state.leader_probes.limit.usage(), (0, 0));
    fixture.close().await
}

#[test]
fn endpoint_configuration_and_logical_generations_never_join() {
    let (_, view, key) = captured();
    assert!(key.matches(&key.clone()));
    let mut other = key.clone();
    other.epoch += 1;
    assert!(!key.matches(&other));
    other = key.clone();
    other.birth_digest = [7; 32];
    assert!(!key.matches(&other));
    other = key.clone();
    other.history.incarnation = IncarnationId::new([8; 16]).unwrap();
    assert!(!key.matches(&other));
    other = key.clone();
    let replacement = Arc::new((*view).clone());
    other.route = Arc::downgrade(&replacement);
    assert!(!key.matches(&other));
    let generation = Arc::new(());
    other = key.clone();
    other.configuration = Some(Arc::downgrade(&generation));
    assert!(!key.matches(&other));
    let mut verified = key;
    verified.configuration = Some(Arc::downgrade(&generation));
    assert!(verified.matches(&other));
    let next = Arc::new(());
    other.configuration = Some(Arc::downgrade(&next));
    assert!(!verified.matches(&other));
}

#[tokio::test]
async fn metadata_and_task_limits_are_borrowed_and_returned() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        TransportOptions {
            max_peers: 4,
            queue_items: 1,
            ..Default::default()
        },
        vec![],
    )
    .await?;
    let state = &fixture.transport.handle.0;
    let (_, _view, key) = captured();
    let baseline = state.outgoing.control.usage();
    let first = state.leader_probes.admit(state, key.clone())?;
    assert!(matches!(
        state.leader_probes.admit(state, key.clone()),
        Err(TransportError::Overloaded)
    ));
    assert_eq!(
        state.outgoing.control.usage(),
        (baseline.0 + OWNER_BYTES, baseline.1 + 1)
    );
    drop(first);
    assert_eq!(state.outgoing.control.usage(), baseline);
    assert!(state
        .leader_probes
        .shared
        .pending
        .lock()
        .unwrap()
        .is_empty());
    let full = state
        .leader_probes
        .limit
        .acquire(state.leader_probes.limit.capacity().0)
        .unwrap();
    assert!(matches!(
        state.leader_probes.admit(state, key.clone()),
        Err(TransportError::Overloaded)
    ));
    assert_eq!(state.outgoing.control.usage(), baseline);
    drop(full);
    let mut guards = Vec::new();
    while state.usage().tasks < state.task_limit() {
        guards.push(state.reserve_task()?);
    }
    assert!(matches!(
        state.leader_probes.admit(state, key),
        Err(TransportError::Overloaded)
    ));
    assert_eq!(state.outgoing.control.usage(), baseline);
    assert_eq!(state.leader_probes.limit.usage(), (0, 0));
    assert!(state
        .leader_probes
        .shared
        .pending
        .lock()
        .unwrap()
        .is_empty());
    drop(guards);
    fixture.close().await
}

#[tokio::test]
async fn dropped_unpolled_owner_wakes_every_waiter() -> anyhow::Result<()> {
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let state = &fixture.transport.handle.0;
    let (_, _view, key) = captured();
    let mut first = state.leader_probes.admit(state, key.clone())?;
    let second = state.leader_probes.admit(state, key)?;
    drop(first.owner.take());
    let observed = state.performance.snapshot();
    assert_eq!(
        observed.counters[Counter::CellLeaderProbeOwners as usize].value,
        1
    );
    assert_eq!(
        observed.counters[Counter::CellLeaderProbeFinishes as usize].value,
        1
    );
    for admitted in [&first, &second] {
        assert_eq!(
            admitted.wait(state, state.domain.mono_ms()? + 1000).await,
            Err(TransportError::Closed)
        );
    }
    drop(first);
    drop(second);
    assert_eq!(state.leader_probes.limit.usage(), (0, 0));
    fixture.close().await
}

// A real peer socket holds the TLS handshake. No grant, quorum, SQL execution or
// synthetic transport result is involved in this physical lifetime regression.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn physical_probe_survives_first_cancellation_and_close_joins_it() -> anyhow::Result<()> {
    use crate::raft::version::VersionSigningKey;
    use cellar_logic::cell_group::CellId;
    use cellar_logic::consensus::{DiskLineage, ProcessGeneration};
    use std::time::Duration;
    let fixture = Fixture::start(Default::default(), vec![]).await?;
    let control = TcpListener::bind("127.0.0.1:0").await?;
    let data = TcpListener::bind("127.0.0.1:0").await?;
    let query = TcpListener::bind("127.0.0.1:0").await?;
    let (route, _, _) = captured();
    fixture.transport.install_cell_query_group(GroupRoute {
        scope: route.history,
        schema_digest: route.schema_digest,
        revision: 1,
        peers: vec![RaftPeer {
            replica: ReplicaIdentity {
                scope: route.history,
                node: NodeId::new([4; 16])?,
                member: MemberId::new(2)?,
                disk: DiskLineage::new([5; 16])?,
                process: ProcessGeneration::new([6; 16])?,
            },
            label: "node-4".into(),
            server_name: "localhost".into(),
            control_address: control.local_addr()?,
            data_address: data.local_addr()?,
            query_address: query.local_addr()?,
        }],
    })?;
    let cell = CellHistory {
        cluster: route.history.cluster,
        namespace: route.history.namespace,
        cell: CellId::new("Counter:one")?,
        incarnation: IncarnationId::new([10; 16])?,
    };
    let keys = VersionKeys::new(
        VersionSigningKey::new([11; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(60),
    )?;
    let state = &fixture.transport.handle.0;
    let baseline = state.usage().tasks;
    let deadline = state.domain.mono_ms()? + 5000;
    let mut first = Box::pin(fixture.transport.discover_cell_command_leader(
        &route,
        &cell,
        keys.clone(),
        1,
        deadline,
    ));
    assert!(first.as_mut().now_or_never().is_none());
    let (socket, _) = tokio::time::timeout(Duration::from_secs(2), query.accept()).await??;
    let mut second = Box::pin(fixture.transport.discover_cell_command_leader(
        &route,
        &cell,
        keys,
        1,
        deadline + 5000,
    ));
    assert!(second.as_mut().now_or_never().is_none());
    assert_eq!(state.usage().tasks, baseline + 1);
    assert_eq!(
        state.leader_probes.limit.usage(),
        (OWNER_BYTES + WAITER_BYTES, 2)
    );
    assert_eq!(Arc::strong_count(&fixture.transport.handle), 1);
    drop(first);
    assert_eq!(state.leader_probes.shared.pending.lock().unwrap().len(), 1);
    tokio::time::timeout(Duration::from_secs(2), fixture.transport.close()).await?;
    assert_eq!(second.await, Err(TransportError::Closed));
    assert_eq!(state.usage().tasks, 0);
    assert_eq!(state.leader_probes.limit.usage(), (0, 0));
    drop(socket);
    let observed = state.performance.snapshot();
    assert_eq!(
        observed.counters[Counter::CellLeaderProbeOwners as usize].value,
        1
    );
    assert_eq!(
        observed.counters[Counter::CellLeaderProbeFinishes as usize].value,
        1
    );
    fixture.close().await
}
