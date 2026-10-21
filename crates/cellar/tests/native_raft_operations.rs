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
    let attempts = store.archive_attempts.load(Ordering::Acquire);
    store.archive_hang.store(true, Ordering::Release);
    let mut options = public_node::options(2)?;
    let id = options.id;
    options.durability = Durability::S3;
    let pending =
        tokio::spawn(cell.command(definition.increment, 2i64.to_be_bytes().to_vec(), options));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = cell.status().await?;
            if status.applied.index > first.version.position.index
                && status.archived < status.applied.index
                && store.archive_attempts.load(Ordering::Acquire) > attempts
            {
                return Ok::<_, anyhow::Error>(status.applied);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
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
