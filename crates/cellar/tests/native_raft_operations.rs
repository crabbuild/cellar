//! Public Node stop observations and retained replica restart. The conditional
//! in-memory archive fixture is not physical S3 or a deployment fence provider.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_controller_native.rs"]
#[allow(dead_code)]
mod automatic;
#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_catalog_fault.rs"]
#[allow(dead_code)]
mod faults;
#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod image_fault;
#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod log_fault;
#[path = "support/raft_management_native.rs"]
mod management;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;
#[path = "support/raft_seed_fault.rs"]
#[allow(dead_code)]
mod seed_fault;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, NodeOutcome, ShutdownMode, ShutdownPolicy};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_seals_raft_before_joined_receipt() -> Result<()> {
    management::run(management::Scenario::PendingSql).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_reports_raft_archive_lag() -> Result<()> {
    management::run(management::Scenario::ArchiveLag).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_keeps_raft_ambiguity_incomplete() -> Result<()> {
    management::run(management::Scenario::Ambiguous).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_raft_cancel_has_no_graceful_proof() -> Result<()> {
    management::run(management::Scenario::Cancelled).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_raft_close_error_is_incomplete() -> Result<()> {
    management::run(management::Scenario::CloseError).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_raft_physical_join_preserves_deadline() -> Result<()> {
    management::run(management::Scenario::JoinDeadline).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_raft_cancelled_start_is_incomplete() -> Result<()> {
    management::cancelled_start().await
}

async fn wait_status(
    cell: &RaftCellHandle,
    ready: impl Fn(&CellStatus) -> bool,
) -> Result<CellStatus> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match cell.status().await {
                Ok(status) if ready(&status) => return Ok(status),
                Ok(_) | Err(CommandFailure::Overloaded) => (),
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

fn two_registrations(schema: [u8; 32]) -> Result<Vec<RaftCellRegistration>> {
    let first = registration(schema)?;
    let mut second = first.clone();
    second.birth.scope.cell = "Counter:two".into();
    second.birth.scope.group = GroupId::new([10; 16])?;
    Ok(vec![first, second])
}

fn registration(schema: [u8; 32]) -> Result<RaftCellRegistration> {
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        schema,
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    Ok(RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace,
        class: "Counter".into(),
        birth: BirthProposal {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                cell: "Counter:one".into(),
                group: GroupId::new([8; 16])?,
                incarnation: IncarnationId::new([9; 16])?,
            },
            voters: vec![InitialMember {
                member: MemberId::new(1)?,
                node: NodeId::new([1; 16])?,
            }],
            learners: vec![],
        },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_archive_lag() -> Result<()> {
    let (gate, _) = public_node::Gate::new();
    gate.release();
    let definition = public_node::counter(Arc::new(AtomicUsize::new(0)), gate)?;
    let registration = registration(definition.schema)?;
    let root = tempfile::tempdir()?;
    let store = Arc::new(faults::Store::default());
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells(
        &node,
        definition.application.clone(),
        store.clone(),
        registration.clone(),
        false,
    )
    .await?;
    let cell = service.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    public_node::ready(&cell).await?;
    store.archive_unavailable.store(true, Ordering::SeqCst);
    let ack = cell
        .command(
            definition.increment.clone(),
            7i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await?;
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        node.shutdown(ShutdownMode::Preserve),
    )
    .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    let raft = report.raft.context("missing Raft close result")?;
    ensure!(!raft.startup_incomplete);
    let closed = raft.result?;
    ensure!(closed.groups_total == 1 && closed.groups.len() == 1 && closed.omitted_groups == 0);
    ensure!(closed.groups_with_archive_lag == 1 && closed.unfinished.is_empty());
    let group = &closed.groups[0];
    ensure!(
        group.scope == registration.birth.scope
            && group.applied.index >= ack.version.position.index
    );
    ensure!(group.committed >= group.applied.index && group.archived < ack.version.position.index);
    ensure!(
        closed.node.node == NodeId::new([1; 16])?
            && closed.node.disk == DiskLineage::new([10; 16])?
            && closed.node.process == ProcessGeneration::new([11; 16])?
    );
    ensure!(service.is_closed() && service.close().await? == closed);
    ensure!(matches!(
        cell.query(definition.get.clone(), vec![], QueryOptions::default())
            .await,
        Err(CommandFailure::Unavailable)
    ));
    store.archive_unavailable.store(false, Ordering::SeqCst);
    // Reopening with a new process while the old public handles remain alive
    // proves the old physical engine owner released its exclusive directory.
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let resumed = public_node::cells(
        &node,
        definition.application.clone(),
        store,
        registration.clone(),
        true,
    )
    .await?;
    let resumed_cell = resumed.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    public_node::ready(&resumed_cell).await?;
    let value = resumed_cell
        .query(
            definition.get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(value.result == 7i64.to_be_bytes());
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected resume stop: {outcome:?}")
    };
    let second = report.raft.context("missing resumed Raft report")?.result?;
    ensure!(
        second.storage_instance == closed.storage_instance
            && second.node.process == ProcessGeneration::new([12; 16])?
    );
    ensure!(resumed.close().await? == second);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn node_shutdown_hands_off_to_published_voter() -> Result<()> {
    automatic::shutdown_handoff().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn node_shutdown_report_bounds_preserve_complete_lag_count() -> Result<()> {
    automatic::bounded_shutdown_report().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_pending_proposal() -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate.clone())?;
    let registration = registration(definition.schema)?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells(
        &node,
        definition.application.clone(),
        Arc::new(faults::Store::default()),
        registration.clone(),
        false,
    )
    .await?;
    let cell = service.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    public_node::ready(&cell).await?;
    cell.command(
        definition.increment.clone(),
        1i64.to_be_bytes().to_vec(),
        public_node::options(1)?,
    )
    .await?;
    wait_status(&cell, |status| status.command_ready).await?;
    let pending = tokio::spawn(cell.command(
        definition.increment.clone(),
        2i64.to_be_bytes().to_vec(),
        public_node::options(2)?,
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .context("writer did not enter")?;
    let mut stop = tokio::spawn(node.shutdown(ShutdownMode::Preserve));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !service.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    ensure!(matches!(
        cell.command(
            definition.increment,
            3i64.to_be_bytes().to_vec(),
            public_node::options(3)?
        )
        .await,
        Err(CommandFailure::Unavailable)
    ));
    // The callback still owns SQL and its directory. A semantic deadline or
    // dropped caller cannot turn this into a physical join.
    ensure!(tokio::time::timeout(Duration::from_millis(100), &mut stop)
        .await
        .is_err());
    gate.release();
    let receipt = pending.await??;
    ensure!(receipt.result == 3i64.to_be_bytes());
    let outcome = tokio::time::timeout(Duration::from_secs(15), stop).await???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    ensure!(report.application_work_drained && !report.drain_timed_out);
    let closed = report.raft.context("missing Raft report")?.result?;
    ensure!(
        closed.unfinished.is_empty()
            && closed.groups[0].applied.index >= receipt.version.position.index
    );
    ensure!(service.close().await? == closed && calls.load(Ordering::SeqCst) == 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_shutdown_reports_submitted_ambiguity() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definition = public_node::counter(Arc::new(AtomicUsize::new(0)), gate)?;
    let registration = registration(definition.schema)?;
    let root = tempfile::tempdir()?;
    let store = Arc::new(faults::Store::default());
    let node = public_node::node_with_shutdown(
        root.path(),
        definition.application.clone(),
        ShutdownPolicy {
            total_ms: 500,
            token_wait_ms: 0,
            no_progress_ms: 200,
        },
    )
    .await?;
    let service = public_node::cells(
        &node,
        definition.application.clone(),
        store.clone(),
        registration.clone(),
        false,
    )
    .await?;
    let cell = service.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    public_node::ready(&cell).await?;
    let first = cell
        .command(
            definition.increment.clone(),
            1i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await?;
    wait_status(&cell, |status| {
        status.command_ready && status.archived >= first.version.position.index
    })
    .await?;
    let attempts = store.archive_attempts.load(Ordering::Acquire);
    store.archive_hang.store(true, Ordering::Release);
    let mut options = public_node::options(2)?;
    let id = options.id;
    options.durability = Durability::S3;
    let pending =
        tokio::spawn(cell.command(definition.increment, 2i64.to_be_bytes().to_vec(), options));
    wait_status(&cell, |status| {
        status.applied.index > first.version.position.index
            && status.archived < status.applied.index
            && store.archive_attempts.load(Ordering::Acquire) > attempts
    })
    .await?;
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        node.shutdown(ShutdownMode::Preserve),
    )
    .await??;
    store.archive_hang.store(false, Ordering::Release);
    store.archive_resume.notify_waiters();
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    ensure!(!report.application_work_drained && report.drain_timed_out);
    let closed = report.raft.context("missing Raft report")?.result?;
    ensure!(closed.groups_with_archive_lag == 1);
    ensure!(
        closed.unfinished
            == vec![UnfinishedCommand {
                scope: registration.birth.scope,
                id
            }]
    );
    ensure!(matches!(pending.await?, Err(CommandFailure::Ambiguous {command}) if command==id));
    ensure!(service.close().await? == closed);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_shutdown_reports_parked_archive_unknown() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let registrations = two_registrations(definition.schema)?;
    let root = tempfile::tempdir()?;
    let store = Arc::new(faults::Store::default());
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        registrations.clone(),
        false,
        RaftHostOptions::default(),
        TransportOptions::default(),
    )
    .await?;
    for (index, registration) in registrations.iter().enumerate() {
        let cell = service.cell(
            registration.namespace.namespace,
            &registration.birth.scope.cell,
        )?;
        public_node::ready(&cell).await?;
        let mut options = public_node::options(1)?;
        options.durability = Durability::S3;
        cell.command(
            definition.increment.clone(),
            (index as i64 + 1).to_be_bytes().to_vec(),
            options,
        )
        .await?;
        wait_status(&cell, |status| status.command_ready).await?;
    }
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    let before = report.raft.context("missing first Raft report")?.result?;
    ensure!(before.groups_total == 2 && before.groups_with_unknown_archive == 0);
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let resumed = public_node::cells(
        &node,
        definition.application.clone(),
        store,
        registrations[0].clone(),
        true,
    )
    .await?;
    let cell = resumed.cell(
        registrations[0].namespace.namespace,
        &registrations[0].birth.scope.cell,
    )?;
    public_node::ready(&cell).await?;
    ensure!(
        cell.query(
            definition.get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            }
        )
        .await?
        .result
            == 1i64.to_be_bytes()
    );
    ensure!(matches!(
        resumed.cell(
            registrations[1].namespace.namespace,
            &registrations[1].birth.scope.cell
        ),
        Err(CommandFailure::Unavailable)
    ));
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    let after = report.raft.context("missing second Raft report")?.result?;
    ensure!(
        after.groups_total == 2
            && after.groups.len() == 1
            && after.omitted_groups == 1
            && after.groups_with_unknown_archive == 1
    );
    ensure!(
        after.storage_instance == before.storage_instance && calls.load(Ordering::Acquire) == 2
    );
    ensure!(service.close().await? == before && resumed.close().await? == after);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_resource_pressure() -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate.clone())?;
    let registrations = two_registrations(definition.schema)?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        Arc::new(faults::Store::default()),
        registrations.clone(),
        false,
        RaftHostOptions {
            max_requests: 4,
            ..Default::default()
        },
        TransportOptions::default(),
    )
    .await?;
    let first = service.cell(
        registrations[0].namespace.namespace,
        &registrations[0].birth.scope.cell,
    )?;
    let other = service.cell(
        registrations[1].namespace.namespace,
        &registrations[1].birth.scope.cell,
    )?;
    for cell in [&first, &other] {
        public_node::ready(cell).await?;
        cell.command(
            definition.increment.clone(),
            1i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await?;
        wait_status(cell, |status| status.command_ready).await?;
    }
    let pending = tokio::spawn(first.command(
        definition.increment.clone(),
        2i64.to_be_bytes().to_vec(),
        public_node::options(2)?,
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .context("writer did not enter")?;
    // Unpolled replies retain admission permits. These three requests must not
    // enter SQL behind the held producer or release their permits speculatively.
    let mut queued = Vec::new();
    for _ in 0..3 {
        queued.push(first.command(
            definition.increment.clone(),
            3i64.to_be_bytes().to_vec(),
            public_node::options(3)?,
        ));
    }
    ensure!(matches!(
        other
            .query(definition.get.clone(), vec![], QueryOptions::default())
            .await,
        Err(CommandFailure::Overloaded)
    ));
    let status = wait_status(&other, |status| status.command_ready).await?;
    ensure!(status.term.get() > 0 && status.leader.is_some());
    drop(queued.pop());
    let mut options = public_node::options(1)?;
    options.id = CommandId::new(ClientId::new([8; 16])?, 1)?;
    // One free request slot still cannot reserve the private SQL result. The
    // explicit refusal must discard that private stage without committing it.
    let before_calls = calls.load(Ordering::Acquire);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let refused = other
            .command(
                definition.increment.clone(),
                7i64.to_be_bytes().to_vec(),
                options.clone(),
            )
            .await;
        ensure!(
            matches!(refused, Err(CommandFailure::Overloaded)),
            "result pressure did not refuse the private stage: {refused:?}"
        );
        if calls.load(Ordering::Acquire) > before_calls {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "the refused request never reached private execution"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(queued);
    let before = other
        .query(
            definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(before.result == 1i64.to_be_bytes());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let receipt = loop {
        let result = other
            .command(
                definition.increment.clone(),
                7i64.to_be_bytes().to_vec(),
                options.clone(),
            )
            .await;
        match result {
            Ok(receipt) => break receipt,
            Err(CommandFailure::Overloaded) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(error) => return Err(error.into()),
        }
    };
    ensure!(receipt.result == 8i64.to_be_bytes());
    let read = other
        .query(
            definition.get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(read.result == 8i64.to_be_bytes());
    gate.release();
    ensure!(pending.await??.result == 3i64.to_be_bytes());
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop: {outcome:?}")
    };
    let closed = report.raft.context("missing Raft report")?.result?;
    ensure!(closed.unfinished.is_empty() && closed.groups_with_unknown_archive == 0);
    ensure!(service.close().await? == closed);
    Ok(())
}
