// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Repeated progress must not requeue SQL for the same live pending producer.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn calls(snapshot: &RaftPerformanceSnapshot, name: &str) -> Result<u64> {
    anyhow::ensure!(snapshot.available && !snapshot.saturated);
    let operation = snapshot
        .operations
        .iter()
        .find(|operation| operation.name == name)
        .context("SQL operation counter")?;
    Ok(operation.succeeded + operation.failed)
}

pub(crate) async fn pending_progress(rooted: bool) -> Result<()> {
    let executions = Arc::new(AtomicUsize::new(0));
    let observed = executions.clone();
    let class = Arc::new(RaftClass::new("Counter", 1, [215; 32])?.command(
        "Add",
        [216; 32],
        move |context, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            context.sql().execute("UPDATE value SET n=n+1", [])?;
            Ok(b"pending-result".to_vec())
        },
    )?);
    let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let runtime = fixture.runtimes[0].clone();
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        VersionKeys::new(
            VersionSigningKey::new([217; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?,
        CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 3,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?;
    let mut work_results = Vec::new();
    for (index, cell) in [&a, &b].into_iter().enumerate() {
        let now = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 })
            .await?;
        let mut invocation = dispatcher
            .command(
                cell.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([218; 16])?, 1)?,
                    contract: class.contract("Add").context("Add")?,
                    input: vec![],
                    durability: Durability::Quorum,
                    retry_until_ms: now + 60_000,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(15),
                    max_attempts: 4,
                },
                None,
            )
            .await?;
        invocation.begin().await?;
        anyhow::ensure!(matches!(
            invocation
                .prepare(&runtime, &pool, fixture.cell_options(cell).await?)
                .await?,
            CellCommandPreparation::Prepared
        ));
        let before_view = runtime
            .reconciliation_authority(route.history, cell.clone())
            .await?;
        let term = before_view.term();
        let version = before_view.cell().version.clone();
        let accepted = command_dispatch::accepted(&mut invocation).await?;
        let before = fixture.cluster.voters[0].log.performance_snapshot();
        for _ in 0..4 {
            let error = invocation
                .settle()
                .await
                .err()
                .context("pending command resolved")?;
            anyhow::ensure!(error.ambiguous);
            anyhow::ensure!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Uncertain)
            ));
            anyhow::ensure!(pool.usage()?.active_cells == 1);
        }
        let after = fixture.cluster.voters[0].log.performance_snapshot();
        // Capture the performance assertion, then resolve and join the real
        // producer even when the old implementation fails this assertion.
        work_results.push((|| -> Result<()> {
            anyhow::ensure!(
                calls(&after, "sql_publish")? - calls(&before, "sql_publish")? == 1,
                "unchanged pending view requeued SQL reconciliation"
            );
            anyhow::ensure!(
                calls(&after, "sql_other")? == calls(&before, "sql_other")?,
                "known live pending producer required SQL resume"
            );
            Ok(())
        })());
        if index == 0 {
            fixture.drive(Some(accepted.position.index)).await?;
            let receipt = command_dispatch::committed(&mut invocation).await?;
            let receipt = receipt.local_receipt().context("local receipt")?;
            anyhow::ensure!(receipt.receipt.result() == b"pending-result");
            anyhow::ensure!(receipt.version.sequence.get() == 2);
            for replica in &fixture.runtimes {
                let selected = replica
                    .reconciliation_authority(route.history, cell.clone())
                    .await?;
                anyhow::ensure!(selected.cell().version == receipt.version);
                anyhow::ensure!(selected.cell().witness == receipt.witness);
            }
        } else {
            // Exclude the unreplicated old proposal with a real newer-term
            // quorum barrier. Its cell version stays identical: term must be
            // part of the pending-view key, and a fresh proof must still run.
            let mut transfer = Message::default();
            transfer.set_msg_type(MessageType::MsgTimeoutNow);
            transfer.from = 1;
            transfer.to = 2;
            transfer.term = term.get();
            fixture.runtimes[1].step(route.history, transfer).await?;
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    fixture
                        .pump_with_network(&[], &mut BTreeMap::new(), |_, message| {
                            !(message.from == 1
                                && message.term <= term.get()
                                && message.get_msg_type() == MessageType::MsgAppend)
                        })
                        .await?;
                    let status = fixture.runtimes[1].status(route.history).await?;
                    if status.ready
                        && status.term > term
                        && fixture.applied[&route.history]
                            .iter()
                            .all(|index| *index >= status.applied.index)
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .context("newer-term barrier excluding the unreplicated command")??;
            let newer = runtime
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            anyhow::ensure!(newer.term() > term && newer.cell().version == version);
            anyhow::ensure!(before_view.check().is_err());
            anyhow::ensure!(matches!(
                invocation.settle().await?,
                CellCommandSettlement::NotCommitted
            ));
        }
        anyhow::ensure!(pool.usage()?.active_cells == 0);
        anyhow::ensure!(executions.load(Ordering::SeqCst) == index + 1);
    }
    anyhow::ensure!(dispatcher.usage() == (0, 0));
    drop(dispatcher);
    drop(pool);
    drop(runtime);
    fixture.close().await?;
    for result in work_results {
        result?;
    }
    Ok(())
}
