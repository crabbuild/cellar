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
        RaftFleetControl, RaftFleetFirstGroupVoter, RaftFleetParkPhase, RaftFleetParkRequest,
        RaftFleetParkStatus, RaftFleetTransitionAdmissionRequest, RaftFleetTransitionChild,
        RaftFleetTransitionGroup, RaftFleetTransitionPartition,
    },
    NodeOutcome, ShutdownMode,
};
use managed_fixture::{
    counter_class, inspect_current, leader, movement, request, seed_cells, start_node,
    ArchiveSetup, FirstGroupSetup, Fixture, ManagedCluster, TIMEOUT,
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

fn park_control(cluster: &ManagedCluster, replica: usize) -> Result<RaftFleetControl> {
    let fixture = &cluster.fixtures[0];
    Ok(RaftFleetControl::new(
        &fixture.domain,
        fixture.catalog.clone(),
        vec![cluster.directory.clone()],
    )?
    .with_node_observer(cluster.hosts[replica].clone()))
}

async fn park_request(cluster: &ManagedCluster, id: u8) -> Result<RaftFleetParkRequest> {
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    Ok(RaftFleetParkRequest {
        catalog_id: park_control(cluster, 0)?.catalog_id().into(),
        namespace_id: hex::encode(cluster.history.namespace.bytes()),
        group: RaftFleetTransitionGroup {
            group_id: hex::encode(cluster.history.group.bytes()),
            incarnation_id: hex::encode(cluster.history.incarnation.bytes()),
        },
        operation_id: hex::encode([id; 16]),
        request_id: hex::encode([id + 1; 16]),
        settlement_request_id: hex::encode([id + 2; 16]),
        expected_revision: revision,
    })
}

async fn park(cluster: &ManagedCluster, id: u8) -> Result<(usize, Term, RaftFleetParkStatus)> {
    let replica = leader(&cluster.hosts[..3], cluster.history).await?;
    let before = cluster.hosts[replica]
        .group(cluster.history)?
        .status()
        .await?;
    let control = park_control(cluster, replica)?;
    let selection = park_request(cluster, id).await?;
    let parked = control
        .park_group(
            &request(id, selection.expected_revision).principal,
            &selection,
            TIMEOUT,
        )
        .await?;
    ensure!(parked.phase == RaftFleetParkPhase::Delivered);
    require_parked(cluster, before.term, parked.generation).await?;
    Ok((replica, before.term, parked))
}

async fn require_parked(cluster: &ManagedCluster, term: Term, generation: u64) -> Result<()> {
    for host in cluster.hosts.iter().take(3) {
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
            for host in cluster.hosts.iter().take(3) {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_park_and_stale_certificate() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("off,cellar::raft::query_connection=debug")
        .with_test_writer()
        .try_init();
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start_with_archives(
            rooted,
            &[7],
            ArchiveSetup::Disabled,
        ))
        .await?;
        let result = Box::pin(exercise_interrupted_park(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("interrupted park with extent-backed images={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn exercise_interrupted_park(cluster: &mut ManagedCluster) -> Result<()> {
    let seed = seed_cells(&cluster.hosts[..3], cluster.history).await?;
    let replica = leader(&cluster.hosts[..3], cluster.history).await?;
    ensure!(cluster.hosts[3].resource_usage().host.controller.groups == 0);
    let router = cluster.hosts[3].cell(seed.cells[0].clone())?;
    // Discovery and an active node do not grant namespace data access.
    // Fleet-authorized quorum discovery may independently wake a group;
    // exercise the data permission refusal before selecting the parked cut.
    let unauthorized = router
        .query(
            ProposalId::new([220; 16])?,
            &router.contract("Get")?,
            vec![],
            QueryOptions::default(),
        )
        .await
        .expect_err("a zero-replica router needs an explicit namespace grant");
    ensure!(
        matches!(
            unauthorized.as_ref(),
            CellDispatchError::Transport(TransportError::Io(_) | TransportError::Refused)
        ),
        "unexpected unauthorized router result: {unauthorized:?}"
    );
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    for voter in cluster.hosts.iter().take(3) {
        inspect_current(voter, cluster.peers[3].node, cut.revision()).await?;
        voter.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: cluster.peers[3].clone(),
        })?;
        voter.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: cluster.peers[3].clone(),
        })?;
    }

    let selection = park_request(cluster, 225).await?;
    let principal = request(225, selection.expected_revision).principal;
    let control = park_control(cluster, replica)?;
    let before = cluster.hosts[replica]
        .group(cluster.history)?
        .status()
        .await?;
    // Lose confirmation of the real Fleet CAS. The reservation committed,
    // but this controller has not proposed PreparePark or delivered anything.
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = control
        .park_group(&principal, &selection, TIMEOUT)
        .await
        .expect_err("lost admission confirmation must be an unknown result");
    ensure!(
        lost.outcome == cellar::sdk::fleet_operations::RaftFleetOutcome::Unknown,
        "wrong lost-confirmation outcome: {lost:?}"
    );
    let pending = control
        .park_group_status(&principal, &selection, TIMEOUT)
        .await?
        .context("the unknown park admission was lost")?;
    ensure!(pending.phase == RaftFleetParkPhase::Negotiating);
    let untouched = cluster.hosts[replica]
        .group(cluster.history)?
        .status()
        .await?;
    ensure!(untouched.ready && untouched.applied == before.applied);
    drop(control);

    // A new controller reconstructs the exact request from rooted state.
    let control = park_control(cluster, replica)?;
    let delivered = control.park_group(&principal, &selection, TIMEOUT).await?;
    ensure!(
        delivered.phase == RaftFleetParkPhase::Delivered
            && delivered.generation == pending.generation
    );
    require_parked(cluster, before.term, delivered.generation).await?;
    // The very first public linearizable read wakes without an operator call.
    let first_read = router
        .query(
            ProposalId::new([230; 16])?,
            &router.contract("Get")?,
            vec![],
            QueryOptions::default(),
        )
        .await
        .context("first authorized router read after park")?;
    ensure!((1..=3).contains(&first_read.served_by.get()));
    ensure!(
        first_read.result == seed.receipts[0].result
            && first_read.version == seed.receipts[0].version
            && first_read.witness == seed.receipts[0].witness
    );
    observe_wake(cluster, before.term, delivered.generation).await?;
    // Park a new generation so the first write must independently wake it.
    let (_, term, write_park) = park(cluster, 231).await?;
    let handle = cluster.hosts[3].cell(seed.cells[0].clone())?;
    let id = CommandId::new(ClientId::new([228; 16])?, 1)?;
    let options = CommandOptions::new(id, seed.retry);
    let contract = handle.contract("Add")?;
    let delta = 3_i64.to_be_bytes().to_vec();
    let committed = handle
        .command(&contract, delta.clone(), options.clone())
        .await
        .context("first authorized router command after park")?;
    ensure!(
        committed.result == 10_i64.to_be_bytes()
            && committed.durability == Durability::Quorum
            && committed.witness.position.index.get() > write_park.generation
    );
    let active = observe_wake(cluster, term, write_park.generation).await?;

    // An old successful operation is historical. Its retry must not deliver
    // its old certificate again after newer commands have closed the cut.
    let replayed = control.park_group(&principal, &selection, TIMEOUT).await?;
    // observed_revision describes the latest Fleet read, not the original
    // settlement. Every historical operation binding must remain identical.
    let mut historical = delivered.clone();
    historical.observed_revision = replayed.observed_revision;
    ensure!(replayed == historical && replayed.observed_revision >= delivered.observed_revision);
    let read = read_cell(cluster, active, &seed.cells[0], 229).await?;
    ensure!(
        read.result == committed.result
            && read.version == committed.version
            && read.witness == committed.witness
    );
    let replay = handle.command(&contract, delta, options).await?;
    ensure!(
        replay.result == committed.result
            && replay.version == committed.version
            && replay.witness == committed.witness
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parked_restart_and_leader_loss() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start_with_archives(
            rooted,
            &[],
            ArchiveSetup::Disabled,
        ))
        .await?;
        let result = Box::pin(exercise_parked_restart(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("parked restart with extent-backed images={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn exercise_parked_restart(cluster: &mut ManagedCluster) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let (_, term, parked) = park(cluster, 231).await?;
    let mut stopped = Vec::new();
    for index in (0..cluster.nodes.len()).rev() {
        stopped.push((index, stop_parked_voter(cluster, index).await?));
    }
    ensure!(cluster.nodes.is_empty() && cluster.hosts.is_empty());
    for (index, boot) in stopped.into_iter().rev() {
        Box::pin(resume_parked_voter(cluster, index, boot)).await?;
    }
    let active = observe_wake(cluster, term, parked.generation).await?;
    for (index, cell) in seed.cells.iter().enumerate() {
        let read = read_cell(cluster, active, cell, 234 + index as u8).await?;
        let expected = &seed.receipts[index];
        ensure!(
            read.result == expected.result
                && read.version == expected.version
                && read.witness == expected.witness,
            "all-voter restart changed a retained image or receipt"
        );
    }
    let handle = cluster.hosts[active].cell(seed.cells[0].clone())?;
    let replay = handle
        .command(
            &handle.contract("Add")?,
            7_i64.to_be_bytes().to_vec(),
            CommandOptions::new(seed.receipts[0].id, seed.retry),
        )
        .await?;
    ensure!(
        replay.result == seed.receipts[0].result
            && replay.version == seed.receipts[0].version
            && replay.witness == seed.receipts[0].witness
    );

    // Lose the parked leader permanently for this turn. An API request to
    // one survivor must wake the other and establish an ordinary RF3 quorum.
    let (lost, term, parked) = park(cluster, 237).await?;
    let lost_member = lost as u64 + 1;
    let _boot = stop_parked_voter(cluster, lost).await?;
    require_parked(cluster, term, parked.generation).await?;
    let handle = cluster.hosts[0].cell(seed.cells[0].clone())?;
    let committed = handle
        .command(
            &handle.contract("Add")?,
            5_i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(ClientId::new([240; 16])?, 1)?, seed.retry),
        )
        .await?;
    ensure!(
        committed.result == 12_i64.to_be_bytes()
            && committed.durability == Durability::Quorum
            && committed.witness.position.index.get() > parked.generation
            && committed.witness.position.term > term
    );
    for host in &cluster.hosts {
        let status = host.group(cluster.history)?.status().await?;
        ensure!(status.term > term && status.leader != Some(lost_member));
    }
    Ok(())
}

// Releasing the callback is mandatory even when a preceding assertion fails.
struct ReleaseCommand(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for ReleaseCommand {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn park_requires_all_voters_and_no_pending_work() -> Result<()> {
    for rooted in [false, true] {
        let entered = Arc::new(Notify::new());
        let notify = entered.clone();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let receive = Arc::new(std::sync::Mutex::new(receive));
        let effect_receive = receive.clone();
        let effect_notify = entered.clone();
        let class = counter_class()?.command("Held", [241; 32], move |context, _| {
            notify.notify_one();
            receive
                .lock()
                .map_err(|_| anyhow::anyhow!("held callback lock"))?
                .recv_timeout(TIMEOUT)
                .context("held command was not released")?;
            context
                .sql()
                .execute("UPDATE counter SET value=value+1", [])?;
            Ok(8_i64.to_be_bytes().to_vec())
        })?;
        let class = class
            .alarms(RaftAlarmLimits {
                max_alarms: 1,
                max_payload_bytes: 1,
            })?
            .alarm("HeldEffect", [244; 32], move |context, _| {
                effect_notify.notify_one();
                effect_receive
                    .lock()
                    .map_err(|_| anyhow::anyhow!("held effect lock"))?
                    .recv_timeout(TIMEOUT)
                    .context("held effect was not released")?;
                context
                    .sql()
                    .execute("UPDATE counter SET value=value+1", [])?;
                Ok(())
            })?
            .command("ArmEffect", [245; 32], |context, _| {
                context.set_alarm("HeldEffect", context.inputs().now_unix_millis + 200, &[])?;
                Ok(vec![])
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
        let release = ReleaseCommand(Some(send.clone()));
        let result = Box::pin(exercise_park_admission(
            &mut cluster,
            entered,
            release,
            send,
        ))
        .await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("park admission with extent-backed images={rooted}"))?;
        joined?;

        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[7, 8, 9])).await?;
        let result = Box::pin(exercise_topology_park_admission(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| {
            format!("topology park admission with extent-backed images={rooted}")
        })?;
        joined?;
    }
    Ok(())
}

async fn exercise_park_admission(
    cluster: &mut ManagedCluster,
    entered: Arc<Notify>,
    release: ReleaseCommand,
    effect_release: std::sync::mpsc::SyncSender<()>,
) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let replica = leader(&cluster.hosts, cluster.history).await?;
    let handle = cluster.hosts[replica].cell(seed.cells[0].clone())?;
    let command = handle.contract("Held")?;
    let producer = tokio::spawn(async move {
        handle
            .command(
                &command,
                vec![],
                CommandOptions::new(CommandId::new(ClientId::new([242; 16])?, 1)?, seed.retry),
            )
            .await
            .map_err(anyhow::Error::from)
    });
    tokio::time::timeout(TIMEOUT, entered.notified()).await?;
    let selection = park_request(cluster, 243).await?;
    let principal = request(243, selection.expected_revision).principal;
    let control = park_control(cluster, replica)?;
    let refused = control
        .park_group(&principal, &selection, Duration::from_millis(250))
        .await;
    drop(release);
    let committed = producer.await??;
    let failure = refused.expect_err("park admitted an unresolved execution capability");
    ensure!(
        matches!(
            failure.code,
            cellar::sdk::fleet_operations::RaftFleetFailureCode::WaitTimeout
                | cellar::sdk::fleet_operations::RaftFleetFailureCode::Blocked
                | cellar::sdk::fleet_operations::RaftFleetFailureCode::Busy
                | cellar::sdk::fleet_operations::RaftFleetFailureCode::Unavailable
        ),
        "unexpected park refusal: {failure:?}"
    );
    ensure!(committed.result == 8_i64.to_be_bytes());
    let active = leader(&cluster.hosts, cluster.history).await?;
    let read = read_cell(cluster, active, &seed.cells[0], 246).await?;
    ensure!(read.witness == committed.witness && read.version == committed.version);
    if control
        .park_group_status(&principal, &selection, TIMEOUT)
        .await?
        .is_some()
    {
        control.wake_group(&principal, &selection, TIMEOUT).await?;
    }
    drop(control);

    // A claimed callback owns the same execution fence even though there is
    // no foreground command waiting for it. Parking must not abandon it.
    let effect_release = ReleaseCommand(Some(effect_release));
    let handle = cluster.hosts[active].cell(seed.cells[0].clone())?;
    handle
        .command(
            &handle.contract("ArmEffect")?,
            vec![],
            CommandOptions::new(CommandId::new(ClientId::new([251; 16])?, 1)?, seed.retry),
        )
        .await?;
    tokio::time::timeout(TIMEOUT, entered.notified()).await?;
    let selection = park_request(cluster, 251).await?;
    let principal = request(251, selection.expected_revision).principal;
    let control = park_control(cluster, active)?;
    let refused = control
        .park_group(&principal, &selection, Duration::from_millis(250))
        .await;
    drop(effect_release);
    ensure!(
        refused.is_err(),
        "park admitted an unresolved effect callback"
    );
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let read = read_cell(cluster, active, &seed.cells[0], 248).await?;
            if read.result == 9_i64.to_be_bytes() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("claimed alarm did not finish after park refusal")??;
    if control
        .park_group_status(&principal, &selection, TIMEOUT)
        .await?
        .is_some()
    {
        control.wake_group(&principal, &selection, TIMEOUT).await?;
    }
    drop(control);

    // RF3 can still acknowledge ordinary quorum writes with one voter absent,
    // but all-voter parking must fail. Expiry reopens normal Raft admission.
    let active = leader(&cluster.hosts, cluster.history).await?;
    let absent = (active + 1) % 3;
    let _boot = stop_parked_voter(cluster, absent).await?;
    // Vector indices change on removal; retain the exact physical leader.
    let mut leader_index = None;
    for (index, host) in cluster.hosts.iter().enumerate() {
        let status = host.group(cluster.history)?.status().await?;
        if status.ready
            && status.leader == Some(active as u64 + 1)
            && host.identity().node == cluster.fixtures[active].spec.node
        {
            leader_index = Some(index);
        }
    }
    let leader_index = leader_index.context("remaining quorum leader")?;
    let selection = park_request(cluster, 247).await?;
    let principal = request(247, selection.expected_revision).principal;
    let control = park_control(cluster, leader_index)?;
    let refused = control
        .park_group(&principal, &selection, Duration::from_millis(500))
        .await;
    ensure!(refused.is_err(), "a missing configured voter was parked");
    let pending = control
        .park_group_status(&principal, &selection, TIMEOUT)
        .await?
        .context("missing-voter park reservation")?;
    ensure!(pending.phase == RaftFleetParkPhase::Negotiating);
    let handle = cluster.hosts[leader_index].cell(seed.cells[0].clone())?;
    let committed = handle
        .command(
            &handle.contract("Add")?,
            2_i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(ClientId::new([250; 16])?, 1)?, seed.retry),
        )
        .await?;
    ensure!(
        committed.result == 11_i64.to_be_bytes()
            && committed.durability == Durability::Quorum
            && committed.witness.position.index.get() > pending.generation
    );
    control.wake_group(&principal, &selection, TIMEOUT).await?;
    Ok(())
}

async fn exercise_topology_park_admission(cluster: &mut ManagedCluster) -> Result<()> {
    let seed = seed_cells(&cluster.hosts[..3], cluster.history).await?;
    let replica = leader(&cluster.hosts[..3], cluster.history).await?;
    let control = park_control(cluster, replica)?;
    let namespace = hex::encode(cluster.history.namespace.bytes());
    let basis = control.transition_basis(&namespace, TIMEOUT).await?;
    let admission = RaftFleetTransitionAdmissionRequest {
        catalog_id: basis.catalog_id,
        namespace_id: namespace.clone(),
        operation_id: hex::encode([151; 16]),
        request_id: hex::encode([152; 16]),
        expected_fleet_revision: basis.fleet_revision,
        expected_directory_digest: basis.directory_digest,
        class: "Counter".into(),
        partition: RaftFleetTransitionPartition::Split {
            parent: RaftFleetTransitionGroup {
                group_id: hex::encode(cluster.history.group.bytes()),
                incarnation_id: hex::encode(cluster.history.incarnation.bytes()),
            },
            pivot_cell: "Counter:park-pivot".into(),
        },
        children: [153_u8, 154]
            .into_iter()
            .map(|id| RaftFleetTransitionChild {
                group: RaftFleetTransitionGroup {
                    group_id: hex::encode([id; 16]),
                    incarnation_id: hex::encode([id; 16]),
                },
                request_id: hex::encode([id; 16]),
                creation_id: hex::encode([id; 16]),
                voters: std::array::from_fn(|member| RaftFleetFirstGroupVoter {
                    node_id: hex::encode(cluster.fixtures[3 + member].spec.node.bytes()),
                    member_id: member as u64 + 1,
                }),
            })
            .collect(),
    };
    let principal = request(151, basis.fleet_revision).principal;
    let transition = control
        .admit_transition(
            &principal,
            &namespace,
            &admission.operation_id,
            &admission,
            TIMEOUT,
        )
        .await?;
    ensure!(transition.operation_id == admission.operation_id);
    let selection = park_request(cluster, 155).await?;
    let rejected = control
        .park_group(&principal, &selection, TIMEOUT)
        .await
        .expect_err("unresolved topology admission must fence park");
    ensure!(
        matches!(
            rejected.code,
            cellar::sdk::fleet_operations::RaftFleetFailureCode::Conflict
                | cellar::sdk::fleet_operations::RaftFleetFailureCode::Blocked
                | cellar::sdk::fleet_operations::RaftFleetFailureCode::Busy
        ),
        "unexpected topology park refusal: {rejected:?}"
    );
    ensure!(control
        .park_group_status(&principal, &selection, TIMEOUT)
        .await?
        .is_none());
    let handle = cluster.hosts[replica].cell(seed.cells[0].clone())?;
    let committed = handle
        .command(
            &handle.contract("Add")?,
            2_i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(ClientId::new([158; 16])?, 1)?, seed.retry),
        )
        .await?;
    ensure!(committed.result == 9_i64.to_be_bytes() && committed.durability == Durability::Quorum);
    Ok(())
}
