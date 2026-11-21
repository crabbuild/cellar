// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A refused reservation has no callback to replay and no physical owner to cancel.
use super::*;

struct Release(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn refusals(log: &ReplicaLog, name: &str) -> Result<u64> {
    Ok(log
        .performance_snapshot()
        .counters
        .iter()
        .find(|counter| counter.name == name)
        .context("admission counter")?
        .value)
}

async fn replace_pool(
    fixture: &mut RuntimeCluster,
    group: GroupHistory,
    configure: impl FnOnce(&mut CellSqlLimits),
) -> Result<CellGroupSqlPool> {
    let (old, applier) = fixture.appliers.remove(&(0, group)).context("SQL owner")?;
    let mut limits = old.usage()?.limits;
    configure(&mut limits);
    old.close().await?;
    drop(old);
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits)?;
    fixture.appliers.insert((0, group), (pool.clone(), applier));
    Ok(pool)
}

pub(crate) async fn run(expire: bool) -> Result<()> {
    run_mode(if expire {
        Completion::Deadline
    } else {
        Completion::Credit
    })
    .await
}

pub(crate) async fn shutdown() -> Result<()> {
    run_mode(Completion::Shutdown).await
}

#[derive(Clone, Copy, Debug)]
enum Completion {
    Credit,
    Deadline,
    Shutdown,
}

async fn run_mode(mode: Completion) -> Result<()> {
    for rooted in [false, true] {
        for group_limited in [true, false] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = calls.clone();
            let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
                "Add",
                [82; 32],
                move |ctx, input| {
                    assert_eq!(input, b"capacity-input");
                    counted.fetch_add(1, Ordering::AcqRel);
                    ctx.sql().execute("UPDATE value SET n=n+7", [])?;
                    Ok(7_i64.to_be_bytes().to_vec())
                },
            )?);
            let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
            let pool = replace_pool(&mut fixture, route.history, |limits| {
                // Isolate group bytes from the node's item ceiling. Neither
                // production nor constrained-benchmark limits change.
                limits.max_pending_cells = if group_limited { 2 } else { 1 };
                if group_limited {
                    limits.group_pending_cells = 2;
                    limits.group_bytes = 96 * 1024 * 1024;
                }
            })
            .await?;
            let controllers =
                controller::start_with_limits(&mut fixture, controller::limits()).await?;
            let dispatcher = CellGroupDispatcher::new(
                &fixture.cluster.domain,
                fixture.cluster.directory.clone(),
                class.clone(),
                remote_queries::keys(89)?,
                CellDispatchLimits {
                    max_cached_routes: 1,
                    route_cache_bytes: 8192,
                    max_invocations: 2,
                    retained_bytes: 128 * 1024,
                    max_input_bytes: 1024,
                },
            )?;
            let retry_until_ms = fixture
                .cluster
                .domain
                .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
                .await?;
            let request = CellCommandRequest {
                id: CommandId::new(ClientId::new([188; 16])?, 1)?,
                contract: class.contract("Add").context("Add")?,
                input: b"capacity-input".to_vec(),
                durability: Durability::Quorum,
                retry_until_ms,
            };
            let options = fixture.cell_options(&a).await?;
            let authority = fixture.runtimes[0]
                .execution_authority(route.history, a)
                .await?;
            let (entered, started) = tokio::sync::oneshot::channel();
            let (release, held) = std::sync::mpsc::sync_channel(1);
            let release = Release(Some(release));
            let caller = pool.clone();
            let attempt = cell_attempt(137)?;
            let holder = fixture.cluster.domain.spawn(async move {
                caller
                    .stage(options, authority, attempt, true, move |tx| {
                        tx.execute("UPDATE value SET n=n+1000", [])?;
                        let _ = entered.send(());
                        held.recv_timeout(Duration::from_secs(15))?;
                        anyhow::bail!("held verification callback rolls back")
                    })
                    .await
            })?;
            let result: Result<()> = async {
                tokio::time::timeout(Duration::from_secs(5), started).await??;
                let held = pool.usage()?;
                let counter = if group_limited {
                    "sql_group_admission_refusals"
                } else {
                    "sql_node_admission_refusals"
                };
                let before = refusals(&fixture.cluster.voters[0].log, counter)?;
                let call = controllers[0].dispatch_command(
                    &dispatcher,
                    b.clone(),
                    request.clone(),
                    CellRoutingLimits {
                        timeout: Duration::from_millis(if matches!(mode, Completion::Deadline) {
                            200
                        } else {
                            5000
                        }),
                        max_attempts: 1,
                    },
                    fixture.cell_options(&b).await?,
                )?;
                let mut response = Box::pin(call.response());
                let premature =
                    tokio::time::timeout(Duration::from_millis(50), &mut response).await;
                anyhow::ensure!(
                    premature.is_err(),
                    "capacity refusal escaped instead of waiting: {:?}",
                    premature.map(|value| value.map(|_| ()))
                );
                assert_eq!(calls.load(Ordering::Acquire), 0);
                assert_eq!(
                    refusals(&fixture.cluster.voters[0].log, counter)?,
                    before + 1,
                    "controller ticks must not poll the full admission bank"
                );
                assert_eq!(pool.usage()?.retained_bytes, held.retained_bytes);
                assert_eq!(pool.usage()?.retained_operations, held.retained_operations);
                assert_eq!(controllers[0].usage().retained_commands, 1);
                assert_eq!(dispatcher.usage().1, 1, "waiting invocation stays charged");
                if matches!(mode, Completion::Shutdown) {
                    drop(response);
                    assert!(tokio::time::timeout(
                        Duration::from_millis(50),
                        controllers[0].close()
                    )
                    .await
                    .is_err());
                    assert_eq!(pool.usage()?.retained_bytes, held.retained_bytes);
                    assert_eq!(calls.load(Ordering::Acquire), 0);
                    drop(release);
                    tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await??;
                    assert_eq!(pool.usage()?.running_workers, 0);
                    assert_eq!(
                        calls.load(Ordering::Acquire),
                        0,
                        "joined close admitted the queued callback"
                    );
                } else if matches!(mode, Completion::Deadline) {
                    let error = response.await.err().context("original capacity deadline")?;
                    assert!(matches!(
                        error.cause.as_ref(),
                        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                    ));
                    wait_for(async || {
                        Ok(controllers[0].usage().retained_commands == 0
                            && dispatcher.usage().1 == 0)
                    })
                    .await?;
                    assert_eq!(calls.load(Ordering::Acquire), 0);
                    drop(release);
                    wait_for(async || Ok(pool.usage()?.retained_operations == 0)).await?;
                    assert_eq!(
                        calls.load(Ordering::Acquire),
                        0,
                        "expired wait entered SQL after credit returned"
                    );
                } else {
                    drop(release);
                    let committed = response.await?;
                    let receipt = committed.local_receipt().context("capacity receipt")?;
                    assert_eq!(receipt.receipt.result(), 7_i64.to_be_bytes());
                    assert_eq!(receipt.version.history, b);
                    assert_eq!(receipt.version.sequence.get(), 2);
                    let expected = receipt.version.clone();
                    drop(committed);
                    wait_for(async || {
                        Ok(controllers[0].usage().retained_commands == 0
                            && dispatcher.usage().1 == 0)
                    })
                    .await?;
                    let replay = controllers[0]
                        .dispatch_command(
                            &dispatcher,
                            b.clone(),
                            request,
                            CellRoutingLimits {
                                timeout: Duration::from_secs(5),
                                max_attempts: 1,
                            },
                            fixture.cell_options(&b).await?,
                        )?
                        .response()
                        .await?;
                    assert_eq!(
                        replay
                            .local_receipt()
                            .context("original-ID receipt")?
                            .version,
                        expected
                    );
                    assert_eq!(
                        calls.load(Ordering::Acquire),
                        1,
                        "wait or receipt replay reran the callback"
                    );
                }
                Ok(())
            }
            .await;
            // Every return path drops the release guard before joining workers.
            let held_result = tokio::time::timeout(Duration::from_secs(5), holder).await??;
            assert!(held_result.is_err());
            for owner in controllers {
                owner.close().await?;
            }
            fixture.close().await?;
            result.with_context(|| format!("SQL capacity wait, extents={rooted}, group={group_limited}, completion={mode:?}"))?;
        }
    }
    Ok(())
}

pub(crate) async fn terminal_refusals() -> Result<()> {
    for rooted in [false, true] {
        for oversized in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let counted = calls.clone();
            let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
                "Add",
                [82; 32],
                move |ctx, _| {
                    counted.fetch_add(1, Ordering::AcqRel);
                    ctx.sql().execute("UPDATE value SET n=n+1000", [])?;
                    anyhow::bail!(cellar_logic::consensus::CommandFailure::Overloaded)
                },
            )?);
            let (mut fixture, route, _, cell) = Box::pin(registered::setup(rooted, &class)).await?;
            let pool = if oversized {
                replace_pool(&mut fixture, route.history, |limits| limits.group_bytes = 1).await?
            } else {
                fixture.appliers[&(0, route.history)].0.clone()
            };
            let controllers =
                controller::start_with_limits(&mut fixture, controller::limits()).await?;
            let dispatcher = CellGroupDispatcher::new(
                &fixture.cluster.domain,
                fixture.cluster.directory.clone(),
                class.clone(),
                remote_queries::keys(89)?,
                CellDispatchLimits {
                    max_cached_routes: 1,
                    route_cache_bytes: 8192,
                    max_invocations: 2,
                    retained_bytes: 128 * 1024,
                    max_input_bytes: 1024,
                },
            )?;
            let retry_until_ms = fixture
                .cluster
                .domain
                .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
                .await?;
            let result: Result<()> = async {
                let call = controllers[0].command(
                    invocation(
                        &dispatcher,
                        &class,
                        &cell,
                        Duration::from_secs(5),
                        retry_until_ms,
                    )
                    .await?,
                    fixture.cell_options(&cell).await?,
                )?;
                let error = tokio::time::timeout(Duration::from_secs(1), call.response())
                    .await?
                    .err()
                    .context("terminal overload")?;
                assert!(matches!(
                    error.cause.as_ref(),
                    CellDispatchError::Execution(ExecutionError::Refused(
                        cellar_logic::consensus::CommandFailure::Overloaded
                    ))
                ));
                wait_for(async || {
                    Ok(controllers[0].usage().retained_commands == 0 && dispatcher.usage().1 == 0)
                })
                .await?;
                assert_eq!(calls.load(Ordering::Acquire), usize::from(!oversized));
                assert_eq!(pool.usage()?.retained_operations, 0);
                Ok(())
            }
            .await;
            for owner in controllers {
                owner.close().await?;
            }
            fixture.close().await?;
            result.with_context(|| {
                format!("terminal SQL refusal, extents={rooted}, oversized={oversized}")
            })?;
        }
    }
    Ok(())
}
