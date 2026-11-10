// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cancel a controller observation and resume solely from the rooted operation.
//! Voter process-kill and independent-host schedules belong to CG-15.
use super::*;
use std::{cell::RefCell, collections::BTreeSet};

const PHASES: &[TransitionPhase] = &[
    TransitionPhase::Planned,
    TransitionPhase::Prepared,
    TransitionPhase::SealAuthorized,
    TransitionPhase::Sealed,
    TransitionPhase::Installed,
    TransitionPhase::Archived,
    TransitionPhase::Published,
    TransitionPhase::Active,
    TransitionPhase::Retired,
];

tokio::task_local! {
    static SCHEDULE: RefCell<Schedule>;
}

#[derive(Default)]
struct Schedule {
    children: Vec<GroupRoute>,
    observed: Vec<TransitionPhase>,
}

pub(crate) async fn qualify(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    SCHEDULE
        .scope(RefCell::new(Schedule::default()), async {
            Box::pin(run_plan(rooted, store, Decision::Authorize)).await?;
            let seen = SCHEDULE.with(|state| state.borrow().observed.clone());
            ensure!(seen == PHASES, "incomplete phase schedule: {seen:?}");
            Ok(())
        })
        .await
}

pub(super) fn register(children: &[GroupRoute]) {
    let _ = SCHEDULE.try_with(|state| state.borrow_mut().children = children.to_vec());
}

pub(super) async fn checkpoint(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    phase: TransitionPhase,
) -> Result<()> {
    let Ok((children, next)) = SCHEDULE.try_with(|state| {
        let state = state.borrow();
        (state.children.clone(), state.observed.len())
    }) else {
        return Ok(());
    };
    ensure!(
        PHASES.get(next) == Some(&phase),
        "unexpected restart phase {phase:?} at {next}"
    );
    Box::pin(recover_controller(cluster, directory, transition, phase))
        .await
        .with_context(|| format!("controller loss during {phase:?}"))?;
    Box::pin(check_writer(cluster, &children, phase, next))
        .await
        .with_context(|| format!("single writer after {phase:?} restart"))?;
    SCHEDULE.with(|state| state.borrow_mut().observed.push(phase));
    Ok(())
}

async fn controller(cluster: &ManagedCluster) -> Result<(FleetCatalog, RangeDirectory)> {
    let f = &cluster.fixtures[0];
    let fleet = Fixture::open_catalog(&f.domain, f.store.clone(), cluster.history.cluster)?;
    let catalog = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let namespace = f
        .domain
        .run(catalog.existing_namespace(f.declaration.namespace.clone()))
        .await??;
    Ok((
        fleet,
        RangeDirectory::new(namespace, DirectoryLimits::default())?,
    ))
}

async fn recover_controller(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
    expected: &ReopenedManagedCellGroupTransition,
    phase: TransitionPhase,
) -> Result<()> {
    let f = &cluster.fixtures[0];
    let operation = expected.plan.operation();
    let (lost, old_directory) = controller(cluster).await?;
    let (initial, reads) =
        fault::count_reads(lost.reopen_managed_transition(&old_directory, operation, TIMEOUT))
            .await;
    let initial = initial?.context("persisted operation before controller loss")?;
    ensure!(initial.journal.as_ref().context("persisted phase")?.phase() == phase);
    let key = reads
        .keys()
        .find(|key| key.ends_with("/cell-groups/head"))
        .context("actual directory observation read")?
        .clone();
    let gate = Arc::new(fault::RegistryGate::default());
    gate.armed.store(true, Ordering::Release);
    // Drop the in-flight caller only after it reaches the real provider read.
    // This loses its response and caches, without faulting the voters' clients.
    tokio::time::timeout(TIMEOUT, async {
        let mut work = Box::pin(fault::gate_read(key, gate.clone(),
            lost.reopen_managed_transition(&old_directory, operation, TIMEOUT)));
        cellar::asyncrt::select! {
            outcome = &mut work => anyhow::bail!("controller completed before interruption: {:?}", outcome.err()),
            () = gate.entered.notified() => (),
        }
        drop(work);
        Ok::<_, anyhow::Error>(())
    }).await.context("controller did not reach its interrupted read")??;
    gate.resume.notify_one();
    drop(lost);
    drop(old_directory);

    let (resumer, fresh) = controller(cluster).await?;
    let fleet_before = resumer.read(TIMEOUT).await?.revision();
    let view_before = f.domain.run(fresh.observe()).await??;
    let retained = resumer
        .reopen_managed_transition(&fresh, operation, TIMEOUT)
        .await?
        .context("cold controller lost the original operation")?;
    ensure!(
        retained.request == expected.request && retained.plan.encode() == expected.plan.encode()
    );
    ensure!(retained.journal == initial.journal);
    let replay = resumer
        .plan_managed_transition(&retained.request, &fresh, &retained.plan, TIMEOUT)
        .await?;
    ensure!(Some(&replay) == retained.journal.as_ref() && replay.phase() == phase);
    ensure!(
        resumer.read(TIMEOUT).await?.revision() == fleet_before,
        "resume allocated a new Fleet operation"
    );
    ensure!(
        f.domain.run(fresh.observe()).await??.revision() == view_before.revision(),
        "exact phase replay changed active ranges"
    );
    ensure!(
        f.domain
            .run(directory.active_transition())
            .await??
            .context("controller loss released topology")?
            .1
            == replay
    );
    Ok(())
}

async fn check_writer(
    cluster: &ManagedCluster,
    children: &[GroupRoute],
    phase: TransitionPhase,
    ordinal: usize,
) -> Result<()> {
    for cell_index in 0..2 {
        Box::pin(check_cell_writer(
            cluster, children, phase, ordinal, cell_index,
        ))
        .await?;
    }
    Ok(())
}

async fn check_cell_writer(
    cluster: &ManagedCluster,
    children: &[GroupRoute],
    phase: TransitionPhase,
    ordinal: usize,
    cell_index: u8,
) -> Result<()> {
    let invocation = |tag| {
        let mut bytes = [140 + ordinal as u8; 16];
        bytes[14] = cell_index;
        bytes[15] = tag;
        ProposalId::new(bytes)
    };
    let cell = CellHistory {
        cluster: cluster.history.cluster,
        namespace: cluster.history.namespace,
        cell: CellId::new(format!("Counter:cold-{cell_index}"))?,
        incarnation: IncarnationId::new([91; 16])?,
    };
    let key = cell_routing_key(cell.namespace, &cell.cell);
    let child = children
        .iter()
        .find(|route| route.range.contains(&key))
        .context("exact destination range")?
        .history;
    let expected = match phase {
        TransitionPhase::Planned | TransitionPhase::Prepared | TransitionPhase::SealAuthorized => {
            Some(cluster.history)
        }
        TransitionPhase::Active | TransitionPhase::Retired => Some(child),
        _ => None,
    };
    let mut ready = BTreeSet::new();
    let mut writer = None;
    for (index, host) in cluster.hosts.iter().enumerate() {
        for history in [cluster.history, child] {
            let group = match host.group(history) {
                Ok(group) => group,
                Err(RuntimeError::NotFound) => continue,
                Err(error) => return Err(error.into()),
            };
            let status = match group.status().await {
                Ok(status) => status,
                Err(RuntimeError::NotFound) => continue,
                Err(error) => return Err(error.into()),
            };
            if status.ready && status.leader == Some((index % 3 + 1) as u64) {
                ready.insert(history);
                if Some(history) == expected {
                    writer = Some(host);
                }
            }
        }
    }
    ensure!(
        ready == expected.into_iter().collect::<BTreeSet<_>>(),
        "writable histories after restart: {ready:?}, expected {expected:?}"
    );
    if let Some(history) = expected {
        let writer = writer.context("no serving leader after controller restart")?;
        let handle = writer.cell(cell.clone())?;
        let query = handle
            .query(
                invocation(1)?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(query.served_by_node == writer.identity() && query.version.history == cell);
        // A zero delta is a real replicated mutation with a new receipt, while
        // retaining the original fixture's independently checked business values.
        let sequence = match phase {
            TransitionPhase::Planned => 1,
            TransitionPhase::Prepared => 2,
            TransitionPhase::SealAuthorized => 3,
            TransitionPhase::Active => 4,
            TransitionPhase::Retired => 5,
            _ => anyhow::bail!("non-serving phase cannot allocate a probe command"),
        };
        let options = CommandOptions::new(
            CommandId::new(ClientId::new([149; 16])?, sequence)?,
            preparation::retry_deadline()?,
        );
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                0i64.to_be_bytes().to_vec(),
                options.clone(),
            )
            .await?;
        ensure!(
            receipt.result == query.result
                && receipt.witness.group == history
                && receipt.version.history == cell
        );
        let before_retry = handle
            .query(
                invocation(2)?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?;
        let replay = handle
            .command(
                &handle.contract("Add")?,
                0i64.to_be_bytes().to_vec(),
                options,
            )
            .await?;
        let after_retry = handle
            .query(
                invocation(3)?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            replay.id == receipt.id
                && replay.result == receipt.result
                && replay.durability == receipt.durability
                && replay.version.history == receipt.version.history
                && replay.version.sequence >= receipt.version.sequence
                && replay.witness.group == history,
            "retry changed receipt identity/result/durability: {receipt:?}, replay={replay:?}"
        );
        ensure!(
            before_retry.version == after_retry.version
                && before_retry.result == after_retry.result,
            "duplicate command advanced the logical cell: {before_retry:?}, after={after_retry:?}"
        );
    }
    Ok(())
}
