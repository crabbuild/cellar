// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable SQL state is never a committed base or receipt authority.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn idle_cache_reopens_exact_base() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [221; 32])?.command(
            "Add",
            [222; 32],
            move |context, input| {
                observed.fetch_add(1, Ordering::SeqCst);
                let delta = i64::from_be_bytes(input.try_into()?);
                let before: i64 = context
                    .sql()
                    .query_row("SELECT n FROM value", [], |row| row.get(0))?;
                assert_eq!(before, if delta == 7 { 0 } else { 7 });
                context.sql().execute("UPDATE value SET n=n+?1", [delta])?;
                Ok((before + delta).to_be_bytes().to_vec())
            },
        )?);
        let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            class.clone(),
            VersionKeys::new(
                VersionSigningKey::new([223; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(30),
            )?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
            .await?;
        let term = fixture.runtimes[0].status(route.history).await?.term;
        let first = add(
            &fixture,
            &controllers[0],
            &dispatcher,
            &class,
            &a,
            (1, 7),
            retry_until,
        )
        .await?;
        assert_eq!(first.receipt().receipt.result(), 7_i64.to_be_bytes());
        assert_eq!(first.receipt().version.sequence.get(), 2);
        drop(first);
        idle(&fixture, &controllers, route.history, &a, 2).await?;
        let deadline = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 5000 })
            .await?;
        let old = fixture.runtimes[0]
            .read_authority(
                route.history,
                a.clone(),
                CellReadConsistency::Stale,
                deadline,
            )
            .await?;
        let version = old.cell().version.clone();
        for node in 0..3 {
            let options = fixture.cell_options_at(&a, node).await?;
            fixture
                .cluster
                .domain
                .blocking(move || -> Result<()> {
                    match options {
                        CellSqlOptions::Materialized(options) => {
                            if node == 0 {
                                assert!(options.directory.is_dir());
                            }
                            if options.directory.is_dir() {
                                let mut entries = std::fs::read_dir(&options.directory)?;
                                let only = entries.next().context("private lock inode")??;
                                assert_eq!(only.file_name(), "working.lock");
                                assert!(
                                    entries.next().is_none(),
                                    "settled private scratch was retained"
                                );
                                // Reacquiring this exact private lock proves the
                                // settled producer closed before cache eviction.
                                drop(
                                    options
                                        .host
                                        .filesystem()
                                        .lock_file(&options.directory.join("working.lock"))?,
                                );
                                std::fs::remove_dir_all(&options.directory)?;
                                std::fs::create_dir_all(&options.directory)?;
                                std::fs::write(
                                    options.directory.join("working.sqlite"),
                                    b"discarded cache is not SQLite",
                                )?;
                            }
                        }
                        CellSqlOptions::Extents(options) => {
                            let usage = options.images.usage()?;
                            assert_eq!(usage.workspaces, 0);
                            assert_eq!(usage.workspace_bytes, 0);
                            if options.cache_directory.is_dir() {
                                std::fs::remove_dir_all(options.cache_directory)?;
                            }
                        }
                    }
                    Ok(())
                })?
                .await??;
        }
        // A keeps its group and leadership. The next command must open from
        // the verified committed image, including its receipt/client tables.
        let next = add(
            &fixture,
            &controllers[0],
            &dispatcher,
            &class,
            &a,
            (2, 11),
            retry_until,
        )
        .await?;
        assert_eq!(next.receipt().receipt.result(), 18_i64.to_be_bytes());
        assert_eq!(next.receipt().version.sequence.get(), 3);
        drop(next);
        idle(&fixture, &controllers, route.history, &a, 3).await?;
        let duplicate = add(
            &fixture,
            &controllers[0],
            &dispatcher,
            &class,
            &a,
            (1, 7),
            retry_until,
        )
        .await?;
        assert_eq!(duplicate.receipt().receipt.result(), 7_i64.to_be_bytes());
        assert_eq!(duplicate.receipt().version.sequence.get(), 3);
        drop(duplicate);
        idle(&fixture, &controllers, route.history, &a, 3).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.runtimes[0].status(route.history).await?.term, term);
        let result = fixture.appliers[&(0, route.history)]
            .0
            .query(
                fixture.cell_options(&a).await?,
                old,
                move |database, selected| {
                    assert_eq!(selected, &version);
                    let value: i64 =
                        database
                            .connection()
                            .query_row("SELECT n FROM value", [], |row| row.get(0))?;
                    Ok(value.to_be_bytes().to_vec())
                },
            )
            .await?;
        assert_eq!(result.result(), 7_i64.to_be_bytes());
        drop(result);
        for (node, runtime) in fixture.runtimes.iter().enumerate() {
            for (cell, expected, sequence) in [(&a, 18_i64, 3_u64), (&b, 0_i64, 1_u64)] {
                let deadline = fixture
                    .cluster
                    .domain
                    .run(async { cellar::asyncrt::mono_ms() + 5000 })
                    .await?;
                let authority = runtime
                    .read_authority(
                        route.history,
                        cell.clone(),
                        CellReadConsistency::Stale,
                        deadline,
                    )
                    .await?;
                let result = fixture.appliers[&(node, route.history)]
                    .0
                    .query(
                        fixture.cell_options_at(cell, node).await?,
                        authority,
                        move |database, selected| {
                            assert_eq!(selected.sequence.get(), sequence);
                            let value: i64 = database.connection().query_row(
                                "SELECT n FROM value",
                                [],
                                |row| row.get(0),
                            )?;
                            Ok(value.to_be_bytes().to_vec())
                        },
                    )
                    .await?;
                assert_eq!(result.result(), expected.to_be_bytes());
                drop(result);
            }
        }
        for controller in &controllers {
            controller.close().await?;
        }
        for (pool, _) in fixture.appliers.values() {
            let usage = pool.usage()?;
            assert_eq!(usage.running_workers, 0);
            assert_eq!(usage.active_cells, 0);
            assert_eq!(usage.retained_bytes, 0);
        }
        drop(controllers);
        fixture.close().await?;
    }
    Ok(())
}

async fn idle(
    fixture: &RuntimeCluster,
    controllers: &[CellGroupController],
    group: GroupHistory,
    cell: &CellHistory,
    sequence: u64,
) -> Result<()> {
    controller::wait_for(async || {
        for runtime in &fixture.runtimes {
            if runtime
                .reconciliation_authority(group, cell.clone())
                .await?
                .cell()
                .version
                .sequence
                .get()
                != sequence
            {
                return Ok(false);
            }
        }
        for controller in controllers {
            if controller.usage().retained_commands != 0
                || controller.usage().active_apply != 0
                || controller.usage().pending_apply != 0
            {
                return Ok(false);
            }
        }
        for (pool, _) in fixture.appliers.values() {
            let usage = pool.usage()?;
            if usage.active_cells != 0
                || usage.retained_operations != 0
                || usage.retained_bytes != 0
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
}

async fn add(
    fixture: &RuntimeCluster,
    controller: &CellGroupController,
    dispatcher: &CellGroupDispatcher,
    class: &RaftClass,
    cell: &CellHistory,
    (sequence, delta): (u64, i64),
    retry_until_ms: u64,
) -> Result<CellCommandResponse> {
    let invocation = dispatcher
        .command(
            cell.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([224; 16])?, sequence)?,
                contract: class.contract("Add").context("Add")?,
                input: delta.to_be_bytes().to_vec(),
                durability: Durability::Quorum,
                retry_until_ms,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(5),
                max_attempts: 4,
            },
            None,
        )
        .await?;
    Ok(controller
        .command(invocation, fixture.cell_options(cell).await?)?
        .response()
        .await?)
}

/// The cleanup fails after a committed receipt exists, while the private lock
/// is still owned. A real leader handoff must preserve its ambiguous outcome.
pub(crate) async fn cleanup_failure_retains_outcome() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let gate = fault::Gate::new(None);
    let class = Arc::new(RaftClass::new("Counter", 1, [227; 32])?.command(
        "Add",
        [228; 32],
        move |context, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            context.sql().execute("UPDATE value SET n=n+1", [])?;
            Ok(b"committed".to_vec())
        },
    )?);
    let (mut fixture, route, cell, _) = Box::pin(registered::setup(false, &class)).await?;
    let controllers = controller::start_with_limits(
        &mut fixture,
        CellGroupControllerLimits {
            max_groups: 1,
            max_parallel_apply: 1,
            ..controller::limits()
        },
    )
    .await?;
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        VersionKeys::new(
            VersionSigningKey::new([229; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?,
        CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?;
    let retry_until_ms = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
        .await?;
    let request = CellCommandRequest {
        id: CommandId::new(ClientId::new([230; 16])?, 1)?,
        contract: class.contract("Add").context("Add")?,
        input: Vec::new(),
        durability: Durability::Quorum,
        retry_until_ms,
    };
    let routing = CellRoutingLimits {
        timeout: Duration::from_secs(5),
        max_attempts: 4,
    };
    let invocation = dispatcher
        .command(cell.clone(), request.clone(), routing, None)
        .await?;
    let CellSqlOptions::Materialized(mut options) = fixture.cell_options(&cell).await? else {
        anyhow::bail!("materialized private state required");
    };
    let directory = options.directory.clone();
    options.host = options
        .host
        .with_filesystem(Arc::new(fault::FaultFileSystem { gate: gate.clone() }));
    let held = gate.arm(fault::HOLD_FAIL_PRIVATE_CLEANUP);
    let call = controllers[0].command(invocation, CellSqlOptions::Materialized(options))?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let pool = &fixture.appliers[&(0, route.history)].0;
    assert_eq!(pool.usage()?.active_cells, 1);
    assert_eq!(controllers[0].usage().retained_commands, 1);
    let old_term = fixture.runtimes[0].status(route.history).await?.term;
    let mut transferred = false;
    controller::wait_for(async || {
        if !transferred {
            match fixture.runtimes[0].transfer_leader(route.history, 2).await {
                Ok(()) => transferred = true,
                Err(RuntimeError::NotReady) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        let status = fixture.runtimes[1].status(route.history).await?;
        Ok(status.ready && status.term > old_term)
    })
    .await?;
    drop(held);
    let error = call
        .response()
        .await
        .err()
        .context("cleanup failure cannot acknowledge success")?;
    assert!(error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Execution(ExecutionError::Storage(_))
    ));
    assert!(error
        .to_string()
        .contains("injected private scratch cleanup failure"));
    controller::wait_for(async || Ok(controllers[0].usage().quarantined_groups == 1)).await?;
    assert_eq!(controllers[0].usage().groups, 0);
    assert_eq!(controllers[0].usage().retained_commands, 1);
    assert_eq!(pool.usage()?.active_cells, 0);
    assert!(!controllers[0].usage().closing);
    let mut replacement = registration::prepare(&fixture, 31, &cell).await?;
    let refused = controllers[0]
        .register(replacement.inputs.remove(0))
        .err()
        .context("quarantined group retains the only registration slot")?;
    assert!(matches!(
        refused.error,
        CellGroupControllerError::Runtime(RuntimeError::Overloaded)
    ));
    drop(refused);
    drop(replacement);
    assert_eq!(controllers[0].usage().pending_registrations, 0);
    assert_eq!(controllers[0].usage().quarantined_groups, 1);
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            assert!(directory.join("working.sqlite").is_file());
            Ok(())
        })?
        .await??;
    // The next leader reads the original replicated receipt. Neither the lost
    // cleanup response nor a discarded private image can rerun the mutation.
    let invocation = dispatcher
        .command(cell.clone(), request, routing, None)
        .await?;
    let receipt = controllers[1]
        .command(invocation, fixture.cell_options_at(&cell, 1).await?)?
        .response()
        .await?;
    assert_eq!(receipt.receipt().receipt.result(), b"committed");
    assert_eq!(receipt.receipt().version.sequence.get(), 2);
    drop(receipt);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(controllers[0].usage().retained_commands, 1);
    controller::wait_for(async || {
        for runtime in fixture.runtimes.iter().skip(1) {
            if runtime
                .reconciliation_authority(route.history, cell.clone())
                .await?
                .cell()
                .version
                .sequence
                .get()
                != 2
            {
                return Ok(false);
            }
        }
        for (node, controller) in controllers.iter().enumerate().skip(1) {
            let usage = fixture.appliers[&(node, route.history)].0.usage()?;
            if controller.usage().retained_commands != 0
                || usage.active_cells != 0
                || usage.retained_operations != 0
            {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await?;
    for (node, runtime) in fixture.runtimes.iter().enumerate().skip(1) {
        let deadline = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 5000 })
            .await?;
        let authority = runtime
            .read_authority(
                route.history,
                cell.clone(),
                CellReadConsistency::Stale,
                deadline,
            )
            .await?;
        let result = fixture.appliers[&(node, route.history)]
            .0
            .query(
                fixture.cell_options_at(&cell, node).await?,
                authority,
                |database, version| {
                    assert_eq!(version.sequence.get(), 2);
                    let value: i64 =
                        database
                            .connection()
                            .query_row("SELECT n FROM value", [], |row| row.get(0))?;
                    Ok(value.to_be_bytes().to_vec())
                },
            )
            .await?;
        assert_eq!(result.result(), 1_i64.to_be_bytes());
        drop(result);
    }
    for controller in &controllers {
        controller.close().await?;
    }
    assert_eq!(controllers[0].usage().retained_commands, 0);
    for (pool, _) in fixture.appliers.values() {
        assert_eq!(pool.usage()?.active_cells, 0);
        assert_eq!(pool.usage()?.running_workers, 0);
    }
    drop(controllers);
    fixture.close().await
}
