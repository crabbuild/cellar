// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed RF3 park and wake over real Raft Engine disks and TLS peers.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;

#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::{
    verification::{
        cell_groups::{CellRoutingError, CellRoutingFailure},
        TransportError,
    },
    FleetBootBinding,
};
use cellar::sdk::{
    consensus::*,
    fleet_operations::{
        RaftFleetControl, RaftFleetParkPhase, RaftFleetParkRequest, RaftFleetParkStatus,
        RaftFleetTransitionGroup,
    },
    NodeOutcome, ShutdownMode,
};
use managed_fixture::{
    counter_class, leader, movement, request, seed_cells, start_node, ArchiveSetup,
    FirstGroupSetup, Fixture, ManagedCluster, TIMEOUT,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

const ALARM_DELAY_MS: u64 = 10_000;
const WAKE_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alarm_and_peer_wake_no_client() -> Result<()> {
    for rooted in [false, true] {
        let fired = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let notify = fired.clone();
        let callback_count = calls.clone();
        let class = counter_class()?
            .alarms(RaftAlarmLimits {
                max_alarms: 1,
                max_payload_bytes: 16,
            })?
            .alarm("Wake", [212; 32], move |context, input| {
                let due = u64::from_be_bytes(input.try_into()?);
                ensure!(context.inputs().now_unix_millis >= due);
                context
                    .sql()
                    .execute("UPDATE counter SET value=value+1", [])?;
                callback_count.fetch_add(1, Ordering::SeqCst);
                notify.notify_one();
                Ok(())
            })?
            .command("Arm", [213; 32], |context, _| {
                let due = context
                    .inputs()
                    .now_unix_millis
                    .checked_add(ALARM_DELAY_MS)
                    .context("alarm deadline overflow")?;
                context.set_alarm("Wake", due, &due.to_be_bytes())?;
                Ok(due.to_be_bytes().to_vec())
            })?;
        let mut first = Box::pin(Fixture::with_class(rooted, class)).await?;
        first.effects = Some(RaftEffectOptions::default());
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            first,
            &[],
            ArchiveSetup::Disabled,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await?;
        let result = Box::pin(exercise_wakes(&mut cluster, fired, calls)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("parked wake with extent-backed images={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn exercise_wakes(
    cluster: &mut ManagedCluster,
    fired: Arc<Notify>,
    calls: Arc<AtomicUsize>,
) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let initial_leader = leader(&cluster.hosts, cluster.history).await?;
    let cell = &seed.cells[0];
    let handle = cluster.hosts[initial_leader].cell(cell.clone())?;
    let armed = handle
        .command(
            &handle.contract("Arm")?,
            vec![],
            CommandOptions::new(CommandId::new(ClientId::new([214; 16])?, 1)?, seed.retry),
        )
        .await?;
    let (_, term, parked) = park(cluster, 221)
        .await
        .context("park with a future alarm")?;
    ensure!(
        calls.load(Ordering::SeqCst) == 0,
        "alarm ran before park delivery"
    );

    // The callback must enter before any query, command or operator wake.
    tokio::time::timeout(WAKE_TIMEOUT, fired.notified())
        .await
        .context("parked alarm did not wake the voters")?;
    let wake_barrier = parked
        .generation
        .checked_add(1)
        .context("park index overflow")?;
    let active = observe_wake(cluster, term, wake_barrier).await?;
    let read = read_cell(cluster, active, cell, 215).await?;
    ensure!(
        read.result == 8_i64.to_be_bytes()
            && read.version.sequence.get() == armed.version.sequence.get() + 1
            && read.witness.position.index.get() > parked.generation
            && calls.load(Ordering::SeqCst) == 1,
        "the alarm must commit one update after the parked cut"
    );

    // The alarm is consumed. Only the retained voter's protocol traffic can
    // wake the two peers, which remain parked throughout the joined stop.
    let (stopped, term, parked) = park(cluster, 224).await.context("park after the alarm")?;
    let boot = stop_parked_voter(cluster, stopped).await?;
    require_parked(cluster, term, parked.generation).await?;
    Box::pin(resume_parked_voter(cluster, stopped, boot)).await?;
    let active = observe_wake(cluster, term, parked.generation).await?;
    let after_restart = read_cell(cluster, active, cell, 216).await?;
    ensure!(
        after_restart.result == read.result
            && after_restart.witness == read.witness
            && after_restart.version == read.version
            && calls.load(Ordering::SeqCst) == 1,
        "retained restart changed the cell state or replayed the alarm"
    );
    Ok(())
}

async fn read_cell(
    cluster: &ManagedCluster,
    replica: usize,
    cell: &CellHistory,
    id: u8,
) -> Result<QueryResult> {
    let handle = cluster.hosts[replica].cell(cell.clone())?;
    let read = handle
        .query(
            ProposalId::new([id; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await?;
    ensure!(read.served_by == MemberId::new(replica as u64 + 1)?);
    Ok(read)
}

async fn park(cluster: &ManagedCluster, id: u8) -> Result<(usize, Term, RaftFleetParkStatus)> {
    let replica = leader(&cluster.hosts, cluster.history).await?;
    let host = &cluster.hosts[replica];
    let before = host.group(cluster.history)?.status().await?;
    let fixture = &cluster.fixtures[0];
    let control = RaftFleetControl::new(
        &fixture.domain,
        fixture.catalog.clone(),
        vec![cluster.directory.clone()],
    )?
    .with_node_observer(host.clone());
    let revision = fixture.catalog.read(TIMEOUT).await?.revision();
    let selection = RaftFleetParkRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(cluster.history.namespace.bytes()),
        group: RaftFleetTransitionGroup {
            group_id: hex::encode(cluster.history.group.bytes()),
            incarnation_id: hex::encode(cluster.history.incarnation.bytes()),
        },
        operation_id: hex::encode([id; 16]),
        request_id: hex::encode([id + 1; 16]),
        settlement_request_id: hex::encode([id + 2; 16]),
        expected_revision: revision,
    };
    let parked = control
        .park_group(&request(id, revision).principal, &selection, TIMEOUT)
        .await?;
    ensure!(parked.phase == RaftFleetParkPhase::Delivered);
    require_parked(cluster, before.term, parked.generation).await?;
    Ok((replica, before.term, parked))
}

async fn require_parked(cluster: &ManagedCluster, term: Term, generation: u64) -> Result<()> {
    for host in &cluster.hosts {
        let sample = host.group(cluster.history)?.status().await?;
        ensure!(
            !sample.ready
                && sample.term == term
                && sample.applied.index.get() == generation
                && sample.committed == sample.applied.index,
            "voter did not retain the delivered park cut: {sample:?}"
        );
    }
    Ok(())
}

async fn observe_wake(cluster: &ManagedCluster, term: Term, generation: u64) -> Result<usize> {
    tokio::time::timeout(WAKE_TIMEOUT, async {
        loop {
            let mut observations = Vec::with_capacity(cluster.hosts.len());
            for host in &cluster.hosts {
                observations.push(host.group(cluster.history)?.status().await?);
            }
            if let Some(index) = observations.iter().enumerate().find_map(|(index, sample)| {
                (sample.ready && sample.leader == Some(index as u64 + 1)).then_some(index)
            }) {
                let leader = &observations[index];
                if leader.term > term
                    && observations.iter().all(|sample| {
                        sample.term == leader.term
                            && sample.leader == leader.leader
                            && sample.applied.term == leader.term
                            && sample.applied.index.get() > generation
                    })
                {
                    return Ok::<_, anyhow::Error>(index);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("voters did not apply a new-term entry after parking")?
}

async fn stop_parked_voter(cluster: &mut ManagedCluster, index: usize) -> Result<FleetBootBinding> {
    let node = cluster.nodes.remove(index);
    let old = cluster.hosts.remove(index);
    let boot = old
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("old parked boot")?;
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = old.close().await?;
    ensure!(
        report.groups_total == 1
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    Ok(boot)
}

async fn resume_parked_voter(
    cluster: &mut ManagedCluster,
    index: usize,
    boot: FleetBootBinding,
) -> Result<()> {
    let fixture = &cluster.fixtures[index];
    let node = start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let started = async {
        let generation = 51 + u8::try_from(index)?;
        let (config, services) = fixture
            .config(&node, LogOpenMode::Resume, generation)
            .await?;
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match started {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("restart parked voter, shutdown={stopped:?}")));
        }
    };
    cluster.nodes.insert(index, node);
    cluster.hosts.insert(index, host);
    let returned = cluster.hosts[index]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("retained parked boot")?;
    ensure!(
        returned.node == boot.node
            && returned.disk == boot.disk
            && returned.engine == boot.engine
            && returned.process != boot.process
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wake_requires_fresh_quorum_barrier() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start_with_archives(
            rooted,
            &[],
            ArchiveSetup::Disabled,
        ))
        .await?;
        let result = Box::pin(exercise_minority_wake(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("minority wake with extent-backed images={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn exercise_minority_wake(cluster: &mut ManagedCluster) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let (survivor, term, parked) = park(cluster, 221).await?;
    let mut stopped = Vec::new();
    for index in (0..cluster.nodes.len())
        .rev()
        .filter(|index| *index != survivor)
    {
        stopped.push((index, stop_parked_voter(cluster, index).await?));
    }
    require_parked(cluster, term, parked.generation).await?;
    let cell = &seed.cells[0];
    let host = &cluster.hosts[0];
    let handle = host.cell(cell.clone())?;
    let read = handle
        .query(
            ProposalId::new([218; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                routing: QueryRouting::Local,
                timeout: Duration::from_millis(250),
                ..Default::default()
            },
        )
        .await
        .expect_err("a parked voter cannot serve a fresh read alone");
    ensure!(
        quorum_unavailable(&read),
        "unexpected minority read error: {read:?}"
    );

    let id = CommandId::new(ClientId::new([219; 16])?, 1)?;
    let options = CommandOptions::new(id, seed.retry);
    let delta = 5_i64.to_be_bytes().to_vec();
    let write = handle
        .command(
            &handle.contract("Add")?,
            delta.clone(),
            CommandOptions {
                timeout: Duration::from_millis(250),
                ..options.clone()
            },
        )
        .await
        .expect_err("a parked voter cannot acknowledge a quorum write alone");
    ensure!(
        write.command == id && quorum_unavailable(&write.cause),
        "unexpected minority write error: {write:?}"
    );
    let minority = host.group(cluster.history)?.status().await?;
    ensure!(!minority.ready && minority.committed.get() == parked.generation);

    for (index, boot) in stopped.into_iter().rev() {
        Box::pin(resume_parked_voter(cluster, index, boot)).await?;
    }
    let active = observe_wake(cluster, term, parked.generation).await?;
    let before = read_cell(cluster, active, cell, 220).await?;
    ensure!(
        before.result == seed.receipts[0].result
            && before.version == seed.receipts[0].version
            && before.witness == seed.receipts[0].witness,
        "the minority changed the cell"
    );
    let handle = cluster.hosts[active].cell(cell.clone())?;
    let contract = handle.contract("Add")?;
    let committed = handle
        .command(&contract, delta.clone(), options.clone())
        .await?;
    ensure!(
        committed.id == id
            && committed.durability == Durability::Quorum
            && committed.result == 12_i64.to_be_bytes()
            && committed.version.sequence.get() == before.version.sequence.get() + 1
    );
    let replay = handle.command(&contract, delta, options).await?;
    ensure!(
        replay.id == committed.id
            && replay.result == committed.result
            && replay.version == committed.version
            && replay.witness == committed.witness
    );
    Ok(())
}

fn quorum_unavailable(error: &CellDispatchError) -> bool {
    matches!(
        error.refusal(),
        Some(
            CommandFailure::Unavailable
                | CommandFailure::VersionUnavailable
                | CommandFailure::NotLeader { .. }
        )
    ) || matches!(
        error,
        CellDispatchError::Runtime(
            RuntimeError::DeadlineExceeded | RuntimeError::NotLeader | RuntimeError::NotReady
        ) | CellDispatchError::Transport(TransportError::Timeout)
            | CellDispatchError::Routing(CellRoutingError {
                reason: CellRoutingFailure::Deadline | CellRoutingFailure::AttemptsExhausted,
                ..
            })
    )
}
