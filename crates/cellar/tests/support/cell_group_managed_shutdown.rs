// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public shutdown while a shared group owns unarchived data or entered SQL.
use super::*;
use managed_fixture::{
    archives, counter_class, leader, movement, seed_cells, ArchiveSetup, FirstGroupSetup,
    ManagedCluster,
};
use std::sync::atomic::AtomicUsize;

#[path = "raft_read_gate.rs"]
mod gate;

const STOP_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_archive_lag() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start(rooted, &[])).await?;
        let result = Box::pin(archive_lag(&mut cluster)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("archive lag with extent images={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn archive_lag(cluster: &mut ManagedCluster) -> Result<()> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let primary = leader(&cluster.hosts, cluster.history).await?;
    let service = cluster.hosts[primary].clone();
    let identity = service.identity();
    let group = service.group(cluster.history)?;
    let cell = service.cell(seed.cells[0].clone())?;
    let previous = seed.receipts[1].witness.position.index;
    let archived = tokio::time::timeout(TIMEOUT, async {
        loop {
            if let Some(marker) = group.archive_status()?.and_then(|status| status.published) {
                if marker.position().index >= previous {
                    return Ok::<_, anyhow::Error>(marker.position());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("baseline archive did not publish")??;
    let store = cluster.fixtures[0].store.clone();
    let before = store.denied_group_archive_writes.load(Ordering::Acquire);
    store
        .deny_group_archive_writes
        .store(true, Ordering::Release);
    let result =
        async {
            let add = cell.contract("Add")?;
            let id = CommandId::new(seed.receipts[0].id.client, 2)?;
            let options = CommandOptions::new(id, seed.retry);
            let delta = 2i64.to_be_bytes().to_vec();
            let written = cell.command(&add, delta.clone(), options.clone()).await?;
            ensure!(written.durability == Durability::Quorum);
            tokio::time::timeout(TIMEOUT, async {
                while store.denied_group_archive_writes.load(Ordering::Acquire) == before {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("archive writer did not encounter the outage")?;
            let node = cluster.nodes.remove(primary);
            cluster.hosts.remove(primary);
            let NodeOutcome::Stopped(report) =
                tokio::time::timeout(STOP_TIMEOUT, node.shutdown(ShutdownMode::Preserve)).await??
            else {
                anyhow::bail!("voter did not stop")
            };
            let raft = report.raft.context("Raft shutdown report")?;
            ensure!(!raft.startup_incomplete && report.application_work_drained);
            let closed = raft.result?;
            ensure!(
                closed.groups_total == 1
                    && closed.omitted_groups == 0
                    && closed.groups_with_archive_lag == 1
                    && closed.groups_with_unknown_archive == 0
                    && closed.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
            );
            let [group] = closed.groups.as_slice() else {
                anyhow::bail!("expected one physical group in the close report")
            };
            ensure!(
                group.history == cluster.history
                    && group.applied.index >= written.witness.position.index
                    && group.committed == group.applied.index
                    && group.archived.is_some_and(|cut| cut.index >= archived.index
                        && cut.index < written.witness.position.index)
                    && !group.failed
            );
            ensure!(closed.node == identity);
            ensure!(service.is_closed() && service.close().await? == closed);
            let refused = cell
                .query(
                    ProposalId::new([93; 16])?,
                    &cell.contract("Get")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await
                .expect_err("the old handle must refuse after shutdown");
            ensure!(matches!(
                refused.as_ref(),
                CellDispatchError::Runtime(RuntimeError::Closed)
            ));
            Ok::<_, anyhow::Error>((written, closed, options, delta))
        }
        .await;
    store
        .deny_group_archive_writes
        .store(false, Ordering::Release);
    let (written, closed, options, delta) = result?;

    // Keep the closed service and cell alive while acquiring the same disk.
    let fixture = &cluster.fixtures[primary];
    let node = start_node(
        cluster.roots[primary].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 60).await?;
    archives::configure(&mut config);
    let resumed = Box::pin(node.start_raft_cells(config, services)).await?;
    cluster.nodes.insert(primary, node);
    cluster.hosts.insert(primary, resumed.clone());
    let returned = resumed
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("resumed boot")?;
    ensure!(
        returned.engine == closed.storage_instance.bytes()
            && resumed.identity().disk == closed.node.disk
    );
    ensure!(resumed.identity().process != closed.node.process);
    let read = resumed.cell(seed.cells[0].clone())?;
    let value = read
        .query(
            ProposalId::new([94; 16])?,
            &read.contract("Get")?,
            vec![],
            QueryOptions {
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        value.result == written.result
            && value.version == written.version
            && value.witness == written.witness
    );
    let active = leader(&cluster.hosts, cluster.history).await?;
    let replay = cluster.hosts[active].cell(seed.cells[0].clone())?;
    let receipt = replay
        .command(
            &replay.contract("Add")?,
            delta,
            CommandOptions {
                durability: Durability::S3,
                ..options
            },
        )
        .await?;
    ensure!(
        receipt.id == written.id
            && receipt.result == written.result
            && receipt.version == written.version
    );
    ensure!(receipt.witness == written.witness && receipt.durability == Durability::S3);
    let node = cluster.nodes.remove(primary);
    cluster.hosts.remove(primary);
    let NodeOutcome::Stopped(report) =
        tokio::time::timeout(STOP_TIMEOUT, node.shutdown(ShutdownMode::Preserve)).await??
    else {
        anyhow::bail!("resumed voter did not stop")
    };
    let returned = report
        .raft
        .context("resumed Raft shutdown report")?
        .result?;
    ensure!(
        returned.storage_instance == closed.storage_instance
            && returned.node == resumed.identity()
            && resumed.close().await? == returned
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_pending_proposal() -> Result<()> {
    for rooted in [false, true] {
        let (gate, mut entered) = gate::Gate::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let block = gate.clone();
        let count = calls.clone();
        let class = counter_class()?.command("HeldAdd", [85; 32], move |context, _| {
            count.fetch_add(1, Ordering::SeqCst);
            block.wait()?;
            let value: i64 = context.sql().query_row(
                "UPDATE counter SET value=value+2 RETURNING value",
                [],
                |row| row.get(0),
            )?;
            Ok(value.to_be_bytes().to_vec())
        })?;
        let fixture = Fixture::with_class(rooted, class).await?;
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            fixture,
            &[],
            ArchiveSetup::Disabled,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await?;
        let result = Box::pin(pending_sql(&mut cluster, gate, &mut entered)).await;
        let joined = movement::close(cluster.nodes, &cluster.hosts).await;
        cluster.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("pending SQL with extent images={rooted}"))?;
        joined?;
        ensure!(calls.load(Ordering::SeqCst) == 1);
    }
    Ok(())
}

async fn pending_sql(
    cluster: &mut ManagedCluster,
    gate: Arc<gate::Gate>,
    entered: &mut tokio::sync::mpsc::UnboundedReceiver<()>,
) -> Result<()> {
    let _release = gate::Release(gate.clone());
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    let primary = leader(&cluster.hosts, cluster.history).await?;
    let service = cluster.hosts[primary].clone();
    let cell = service.cell(seed.cells[0].clone())?;
    let options = CommandOptions::new(CommandId::new(seed.receipts[0].id.client, 2)?, seed.retry);
    let pending = tokio::spawn(cell.command(&cell.contract("HeldAdd")?, vec![], options));
    tokio::time::timeout(TIMEOUT, entered.recv())
        .await?
        .context("SQL callback did not enter")?;

    let node = cluster.nodes.remove(primary);
    cluster.hosts.remove(primary);
    let mut stop = tokio::spawn(node.shutdown(ShutdownMode::Preserve));
    let check = async {
        let other = service.cell(seed.cells[1].clone())?;
        let get = other.contract("Get")?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let result = other
                    .query(
                        ProposalId::new([95; 16])?,
                        &get,
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::Stale,
                            stale_policy: StaleReadPolicy::DisconnectedHistorical,
                            routing: QueryRouting::Local,
                            timeout: Duration::from_millis(100),
                            ..Default::default()
                        },
                    )
                    .await;
                if result.is_err_and(|error| {
                    matches!(
                        error.as_ref(),
                        CellDispatchError::Runtime(RuntimeError::Closed)
                    )
                }) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("shutdown did not seal new ingress")??;
        let refused = cell
            .command(
                &cell.contract("HeldAdd")?,
                vec![],
                CommandOptions::new(CommandId::new(seed.receipts[0].id.client, 3)?, seed.retry),
            )
            .await
            .expect_err("shutdown must refuse new commands");
        ensure!(
            !refused.ambiguous
                && matches!(
                    refused.cause.as_ref(),
                    CellDispatchError::Runtime(RuntimeError::Closed)
                )
        );
        ensure!(tokio::time::timeout(Duration::from_millis(100), &mut stop)
            .await
            .is_err());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    gate.release();
    let receipt = pending.await?;
    let outcome = tokio::time::timeout(STOP_TIMEOUT, stop).await???;
    check?;
    let receipt = receipt?;
    ensure!(receipt.result == 9i64.to_be_bytes());
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("voter did not stop")
    };
    ensure!(report.application_work_drained && !report.drain_timed_out);
    let closed = report.raft.context("Raft shutdown report")?.result?;
    ensure!(closed.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
    let [group] = closed.groups.as_slice() else {
        anyhow::bail!("expected one physical group in the close report")
    };
    ensure!(
        group.history == cluster.history && group.applied.index >= receipt.witness.position.index
    );
    ensure!(service.close().await? == closed);
    Ok(())
}
