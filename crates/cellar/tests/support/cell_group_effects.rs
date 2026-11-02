// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The production node controller owns all timers, transport and apply work.
//! No fixture event pumping or client traffic drives an armed alarm to commit.

use super::*;
use cellar::application::consensus::{RaftAlarmLimits, RaftEffectOptions};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn alarms_without_clients() -> Result<()> {
    alarms(false, false).await
}

pub(crate) async fn shutdown_joins_alarm() -> Result<()> {
    alarms(true, false).await
}

pub(crate) async fn hot_and_cold_alarms() -> Result<()> {
    alarms(false, true).await
}

async fn alarms(hold_callback: bool, rearm_hot: bool) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::cell_groups::controller::effects=debug")
        .with_test_writer()
        .try_init();
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let hot_calls = Arc::new(AtomicUsize::new(0));
        let hot = hot_calls.clone();
        let hot_at_other = Arc::new(AtomicUsize::new(usize::MAX));
        let fairness = hot_at_other.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Arc::new(std::sync::Mutex::new(Some(entered)));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Arc::new(std::sync::Mutex::new(held));
        let class = Arc::new(
            RaftClass::new("Counter", 1, [if rearm_hot { 225 } else { 201 }; 32])?
                .alarms(RaftAlarmLimits {
                    max_alarms: 2,
                    max_payload_bytes: 128,
                })?
                .alarm("Wake", [202; 32], move |context, input| {
                    assert_eq!(input, b"wake");
                    observed.fetch_add(1, Ordering::SeqCst);
                    if hold_callback {
                        let _ = entered
                            .lock()
                            .unwrap()
                            .take()
                            .context("one callback")?
                            .send(());
                        held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                    }
                    if rearm_hot {
                        if context.identity().cell.as_str() == "registered-a" {
                            let count = hot.fetch_add(1, Ordering::SeqCst) + 1;
                            if count < 4 {
                                context.set_alarm(
                                    "Wake",
                                    context.inputs().now_unix_millis,
                                    b"wake",
                                )?;
                            }
                        } else {
                            fairness.store(hot.load(Ordering::SeqCst), Ordering::SeqCst);
                        }
                    }
                    context.sql().execute("UPDATE value SET n=n+1", [])?;
                    Ok(())
                })?
                .command("Arm", [203; 32], |context, _| {
                    context.set_alarm("Wake", context.inputs().now_unix_millis + 50, b"wake")?;
                    Ok(b"armed".to_vec())
                })?,
        );
        let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
        let mut cold = Vec::new();
        let empty = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(0);")?;
        for ordinal in 0..32 {
            let cell = history(&route, &format!("cold-{ordinal}"))?;
            let accepted = fixture.runtimes[0]
                .propose(
                    route.history,
                    apply::enrollment_bytes(&route, &cell, &empty, rooted)?,
                )
                .await?;
            fixture.drive(Some(accepted.position.index)).await?;
            cold.push(cell);
        }
        let keys = VersionKeys::new(
            VersionSigningKey::new([204; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?;
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            class.clone(),
            keys,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 32 * 1024,
            },
        )?;
        let retry_until_ms = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
            .await?;
        for cell in [&a, &b].into_iter().take(if hold_callback { 1 } else { 2 }) {
            let mut invocation = dispatcher
                .command(
                    cell.clone(),
                    CellCommandRequest {
                        id: CommandId::new(ClientId::new([205; 16])?, 1)?,
                        contract: class.contract("Arm").context("Arm")?,
                        input: Vec::new(),
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
            invocation.begin().await?;
            assert!(matches!(
                invocation
                    .prepare(
                        &fixture.runtimes[0],
                        &fixture.appliers[&(0, route.history)].0,
                        fixture.cell_options(cell).await?
                    )
                    .await?,
                CellCommandPreparation::Prepared
            ));
            let accepted = command_dispatch::accepted(&mut invocation).await?;
            fixture.drive(Some(accepted.position.index)).await?;
            let receipt = command_dispatch::committed(&mut invocation).await?;
            assert_eq!(receipt.receipt().receipt.result(), b"armed");
        }
        // Seed already committed due work before starting the node controller.
        // Its initial marker hints, not another client request or fixture pump,
        // must discover both cells; one-slot admission stays unchanged.
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        assert!(fixture.events.is_empty());
        let cold_opens = Arc::new(AtomicUsize::new(0));
        for (node, controller) in controllers.iter().enumerate() {
            let mut resources = BTreeMap::new();
            for cell in [&a, &b] {
                resources.insert(cell.clone(), fixture.cell_options_at(cell, node).await?);
            }
            let unexpected = cold_opens.clone();
            controller.configure_effects(vec![CellGroupEffectRegistration::new(
                dispatcher.clone(),
                RaftEffectOptions {
                    metadata_bytes: 64 * 1024,
                    ..Default::default()
                },
                move |cell| {
                    resources.get(cell).cloned().ok_or_else(|| {
                        unexpected.fetch_add(1, Ordering::SeqCst);
                        ExecutionError::Invalid("cold cell must not open for discovery")
                    })
                },
            )?])?;
        }
        if hold_callback {
            tokio::time::timeout(Duration::from_secs(5), started)
                .await
                .with_context(|| {
                    format!(
                        "alarm callback never entered; usage={:?}; calls={}",
                        controllers
                            .iter()
                            .map(|controller| controller.usage())
                            .collect::<Vec<_>>(),
                        calls.load(Ordering::SeqCst)
                    )
                })??;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let pool = &fixture.appliers[&(0, route.history)].0;
            assert_eq!(pool.usage()?.active_cells, 1);
            assert_eq!(controllers[0].usage().retained_commands, 1);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
                    .await
                    .is_err()
            );
            assert_eq!(pool.usage()?.active_cells, 1);
            release.send(())?;
            controllers[0].close().await?;
            // Closing waited for the actual SQL callback. It did not publish
            // the unsubmitted private write or erase the durable alarm debt.
            for runtime in fixture.runtimes.iter().skip(1) {
                let selected = runtime
                    .reconciliation_authority(route.history, a.clone())
                    .await?;
                assert_eq!(selected.cell().version.sequence.get(), 2);
                assert!(selected.cell().effects.next_alarm_unix_millis.is_some());
            }
        } else {
            // Only observations follow. The controller's durable index and shared
            // SQL/consensus workers must discover and execute both cells unaided.
            let completed = controller::wait_for(async || {
                for runtime in &fixture.runtimes {
                    for cell in [&a, &b] {
                        let selected = runtime
                            .reconciliation_authority(route.history, cell.clone())
                            .await?;
                        let sequence = if rearm_hot && cell == &a { 6 } else { 3 };
                        if selected.cell().version.sequence.get() != sequence
                            || selected.cell().effects.next_due_unix_millis().is_some()
                        {
                            return Ok(false);
                        }
                    }
                }
                // Root publication precedes physical producer/response disposal.
                // Inspect through the same one-slot pool only after ownership
                // has drained; keep the original overall progress deadline.
                for controller in &controllers {
                    let usage = controller.usage();
                    if usage.active_effects != 0 || usage.retained_commands != 0 {
                        return Ok(false);
                    }
                }
                for (pool, _) in fixture.appliers.values() {
                    let usage = pool.usage()?;
                    if usage.active_cells != 0 || usage.retained_operations != 0 {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .await;
            if let Err(error) = completed {
                let status = fixture.runtimes[0].status(route.history).await;
                let alpha = fixture.runtimes[0]
                    .reconciliation_authority(route.history, a.clone())
                    .await
                    .map(|authority| (authority.cell().version.clone(), authority.cell().effects));
                let beta = fixture.runtimes[0]
                    .reconciliation_authority(route.history, b.clone())
                    .await
                    .map(|authority| (authority.cell().version.clone(), authority.cell().effects));
                return Err(error.context(format!("alarm progress: status={status:?}; alpha={alpha:?}; beta={beta:?}; usage={:?}; callbacks={}", controllers.iter().map(|controller| controller.usage()).collect::<Vec<_>>(), calls.load(Ordering::SeqCst))));
            }
            assert_eq!(calls.load(Ordering::SeqCst), if rearm_hot { 5 } else { 2 });
            if rearm_hot {
                assert_eq!(hot_calls.load(Ordering::SeqCst), 4);
                assert!(
                    hot_at_other.load(Ordering::SeqCst) <= 1,
                    "a re-armed cell must rotate behind the other due cell"
                );
            }
            for (node, runtime) in fixture.runtimes.iter().enumerate() {
                let pool = &fixture.appliers[&(node, route.history)].0;
                for cell in [&a, &b] {
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
                    let result = pool
                        .query(
                            fixture.cell_options_at(cell, node).await?,
                            authority,
                            |database, _| {
                                let n: i64 = database.connection().query_row(
                                    "SELECT n FROM value",
                                    [],
                                    |row| row.get(0),
                                )?;
                                Ok(n.to_be_bytes().to_vec())
                            },
                        )
                        .await?;
                    let expected = if rearm_hot && cell == &a {
                        4_i64
                    } else {
                        1_i64
                    };
                    assert_eq!(result.result(), expected.to_be_bytes());
                }
            }
        }
        assert!(controllers[0].usage().effect_turns > 0);
        assert_eq!(cold_opens.load(Ordering::SeqCst), 0);
        for runtime in fixture.runtimes.iter().skip(usize::from(hold_callback)) {
            for cell in &cold {
                let selected = runtime
                    .reconciliation_authority(route.history, cell.clone())
                    .await?;
                assert_eq!(selected.cell().version.sequence.get(), 1);
                assert_eq!(selected.cell().effects.next_due_unix_millis(), None);
            }
        }
        for controller in &controllers {
            controller.close().await?;
            assert_eq!(controller.usage().active_effects, 0);
            assert_eq!(controller.usage().retained_commands, 0);
        }
        for (pool, _) in fixture.appliers.values() {
            assert_eq!(pool.usage()?.active_cells, 0);
            assert_eq!(pool.usage()?.retained_bytes, 0);
            assert_eq!(pool.usage()?.queued_or_running, 0);
        }
        drop(controllers);
        fixture.close().await?;
    }
    Ok(())
}
