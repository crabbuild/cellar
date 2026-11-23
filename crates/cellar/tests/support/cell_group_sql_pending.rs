// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Repeated progress must not requeue SQL for the same live pending producer.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) fn calls(snapshot: &RaftPerformanceSnapshot, name: &str) -> Result<u64> {
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

struct ReleaseWork(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for ReleaseWork {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

pub(crate) async fn stale_pending_progress(rooted: bool) -> Result<()> {
    use super::apply_faults::physical;
    let root = tempfile::tempdir()?;
    let faults = physical::Faults::new(root.path().join("node-1/pages"), None);
    let filesystem = Arc::new(physical::FaultFileSystem {
        faults: faults.clone(),
    });
    let executions = Arc::new(AtomicUsize::new(0));
    let observed = executions.clone();
    let class = Arc::new(RaftClass::new("Counter", 1, [219; 32])?.command(
        "Add",
        [220; 32],
        move |context, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            context.sql().execute("UPDATE value SET n=n+1", [])?;
            Ok(b"retained-result".to_vec())
        },
    )?);
    let (mut fixture, route, a, b) =
        Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
    let (pool, applier) = fixture.appliers[&(0, route.history)].clone();
    let runtime = fixture.runtimes[0].clone();
    // As in the pipeline fixture, stage B through the native verification
    // producer before occupying the one-worker pool with A. Physical apply has
    // its existing independent progress reservation, with no larger bank.
    let mut producer_b = fixture.private_cell(&b).await?;
    let (mut producer_b, prepared_b) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(prepared) =
                producer_b.stage(cell_attempt(223)?, true, |tx| {
                    tx.execute("UPDATE value SET n=n+11", [])?;
                    Ok(b"unrelated-b".to_vec())
                })?
            else {
                anyhow::bail!("B preparation")
            };
            producer_b.mark_submitted(prepared.proposal())?;
            Ok((producer_b, prepared))
        })?
        .await??;
    let accepted_b = accepted_private(&runtime, &prepared_b).await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture.pump(&[route.history], &mut held).await?;
            if held.contains_key(&route.history)
                && runtime.status(route.history).await?.committed >= accepted_b.position.index
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("B committed before physical apply")??;
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        VersionKeys::new(
            VersionSigningKey::new([221; 32], u64::MAX)?,
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
    let now = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms() as u64 })
        .await?;
    let mut invocation = dispatcher
        .command(
            a.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([222; 16])?, 1)?,
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
            .prepare(&runtime, &pool, fixture.cell_options(&a).await?)
            .await?,
        CellCommandPreparation::Prepared
    ));
    let original = runtime
        .reconciliation_authority(route.history, a.clone())
        .await?;
    let term = original.term();
    let version = original.cell().version.clone();
    command_dispatch::accepted(&mut invocation).await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let release = ReleaseWork(Some(release));
    *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
        let _ = entered.send(());
        let _ = wait.recv_timeout(Duration::from_secs(15));
    }));
    let b_work = held.remove(&route.history).context("held B work")?;
    let mut applying = Box::pin(pool.prepare_apply(&applier, b_work));
    cellar::asyncrt::select! {
        result = &mut applying => anyhow::bail!("held B apply finished: {:?}", result.err()),
        result = tokio::time::timeout(Duration::from_secs(5), started) => result??,
    }
    let before = fixture.cluster.voters[0].log.performance_snapshot();
    let mut settlement = Box::pin(invocation.settle());
    cellar::asyncrt::select! {
        result = &mut settlement => anyhow::bail!("queued reconciliation finished: {}", result.is_ok()),
        result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let usage = pool.usage()?;
                // Include the existing independent completion reservation.
                if usage.queued_or_running + usage.completion_reservations == 2 { break; }
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        }) => result.context("reconciliation queued behind B apply")??,
    }
    let mut transfer = Message::default();
    transfer.set_msg_type(MessageType::MsgTimeoutNow);
    transfer.from = 1;
    transfer.to = 2;
    transfer.term = term.get();
    fixture.runtimes[1].step(route.history, transfer).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture
                .pump_with_network(&[route.history], &mut held, |_, message| {
                    !(message.from == 1
                        && message.term <= term.get()
                        && message.get_msg_type() == MessageType::MsgAppend)
                })
                .await?;
            if runtime.status(route.history).await?.term > term
                && fixture.runtimes[1].status(route.history).await?.ready
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("new term persisted while old reconciliation remains queued")??;
    anyhow::ensure!(original.check().is_err() && held.is_empty());
    drop(release);
    let applied_b = applying.await?;
    let error = settlement
        .await
        .err()
        .context("stale observation resolved a command")?;
    anyhow::ensure!(error.ambiguous);
    anyhow::ensure!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Execution(ExecutionError::Uncertain)
    ));
    anyhow::ensure!(pool.usage()?.active_cells == 1);
    let after = fixture.cluster.voters[0].log.performance_snapshot();
    let work = (|| -> Result<()> {
        anyhow::ensure!(calls(&after, "sql_publish")? - calls(&before, "sql_publish")? == 1);
        anyhow::ensure!(
            calls(&after, "sql_other")? == calls(&before, "sql_other")?,
            "stale observation required redundant SQL resume"
        );
        Ok(())
    })();
    let error = invocation
        .settle()
        .await
        .err()
        .context("unapplied term resolved a command")?;
    anyhow::ensure!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::NotReady)
    ));
    let target = fixture.runtimes[1]
        .status(route.history)
        .await?
        .applied
        .index;
    applied_b.complete(&runtime).await?;
    reconciliation::apply_through(&mut fixture, target).await?;
    let current = runtime.reconciliation_authority(route.history, a).await?;
    anyhow::ensure!(current.term() > term && current.cell().version == version);
    anyhow::ensure!(matches!(
        invocation.settle().await?,
        CellCommandSettlement::NotCommitted
    ));
    anyhow::ensure!(executions.load(Ordering::SeqCst) == 1 && pool.usage()?.active_cells == 0);
    let b_view = runtime.reconciliation_authority(route.history, b).await?;
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            let receipt = producer_b.reconcile(prepared_b.proposal(), b_view)?;
            let CellReconciliation::Committed(receipt) = receipt else {
                anyhow::bail!("B receipt")
            };
            anyhow::ensure!(receipt.receipt.result() == b"unrelated-b");
            producer_b.close()?;
            Ok(())
        })?
        .await??;
    drop(invocation);
    anyhow::ensure!(dispatcher.usage() == (0, 0));
    drop(current);
    drop(original);
    drop(dispatcher);
    drop(pool);
    drop(runtime);
    fixture.close().await?;
    work
}
