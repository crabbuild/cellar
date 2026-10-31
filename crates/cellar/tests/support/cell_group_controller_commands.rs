// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Response lifetimes through real TLS, Raft Engine, SQLite and physical apply.
use super::apply_faults::physical;
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

async fn wait_for(mut predicate: impl AsyncFnMut() -> Result<bool>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate().await? {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

pub(super) async fn invocation(
    dispatcher: &CellGroupDispatcher,
    class: &RaftClass,
    cell: &CellHistory,
    timeout: Duration,
    retry_until_ms: u64,
) -> Result<CellCommandInvocation> {
    Ok(dispatcher
        .command(
            cell.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([188; 16])?, 1)?,
                contract: class.contract("Add").context("Add")?,
                input: b"owned".to_vec(),
                durability: Durability::Quorum,
                retry_until_ms,
            },
            CellRoutingLimits {
                timeout,
                max_attempts: 4,
            },
            None,
        )
        .await?)
}

// phase=0 holds callback execution; phase=1 holds the committed manifest sync.
// mode=0 disconnects, mode=1 expires the response, mode=2 cancels node close.
pub(crate) async fn owned_commands(phase: u8, mode: u8) -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let faults = physical::Faults::new(root.path().join("node-1/pages"), None);
        let filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Arc::new(Mutex::new(held));
        let callback_entered = entered.clone();
        let callback_held = held.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |context, _| {
                if count.fetch_add(1, Ordering::SeqCst) == 0 && phase == 0 {
                    let _ = callback_entered
                        .lock()
                        .unwrap()
                        .take()
                        .context("callback signal")?
                        .send(());
                    callback_held
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(15))?;
                }
                context.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(b"owned-result".to_vec())
            },
        )?);
        let (mut fixture, route, a, b) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        let bounds = CellGroupControllerLimits {
            max_commands: 1,
            ..controller::limits()
        };
        let controllers = controller::start_with_limits(&mut fixture, bounds).await?;
        assert!(fixture.events.is_empty()); // No fixture pumping below this point.
        let keys = VersionKeys::new(
            VersionSigningKey::new([89; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?;
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            class.clone(),
            keys.clone(),
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        if phase == 1 {
            *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
                let _ = entered
                    .lock()
                    .unwrap()
                    .take()
                    .expect("apply signal")
                    .send(());
                held.lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15))
                    .expect("apply release");
            }));
        }
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
            .await?;
        let mut used = invocation(
            &dispatcher,
            &class,
            &b,
            Duration::from_secs(15),
            retry_until,
        )
        .await?;
        used.begin().await?;
        let original_deadline = used.attempt().context("routed invocation")?.deadline_ms();
        let refused = controllers[0]
            .command(used, fixture.cell_options(&b).await?)
            .err()
            .context("fresh invocation required")?;
        assert!(matches!(
            refused.error.cause.as_ref(),
            CellDispatchError::Invalid(_)
        ));
        assert_eq!(
            refused
                .invocation
                .attempt()
                .context("returned original owner")?
                .deadline_ms(),
            original_deadline
        );
        drop(refused);
        let mut oversized = fixture.cell_options(&b).await?;
        match &mut oversized {
            CellSqlOptions::Materialized(o) => {
                o.directory = std::path::PathBuf::from("x".repeat(4097))
            }
            CellSqlOptions::Extents(o) => {
                o.cache_directory = std::path::PathBuf::from("x".repeat(4097))
            }
        }
        let refused = controllers[0]
            .command(
                invocation(
                    &dispatcher,
                    &class,
                    &b,
                    Duration::from_secs(15),
                    retry_until,
                )
                .await?,
                oversized,
            )
            .err()
            .context("option admission")?;
        assert!(matches!(
            refused.error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::Invalid(_))
        ));
        drop(refused);
        assert_eq!(dispatcher.usage().1, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let timeout = if mode == 1 {
            Duration::from_millis(400)
        } else {
            Duration::from_secs(15)
        };
        let call = controllers[0].command(
            invocation(&dispatcher, &class, &a, timeout, retry_until).await?,
            fixture.cell_options(&a).await?,
        )?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(pool.usage()?.active_cells, 1);
        assert_eq!(controllers[0].usage().retained_commands, 1);
        assert_eq!(dispatcher.usage().1, 1);
        if phase == 1 {
            let status = fixture.runtimes[0].status(route.history).await?;
            assert!(status.committed > status.applied.index);
        }
        let rejected = controllers[0]
            .command(
                invocation(
                    &dispatcher,
                    &class,
                    &b,
                    Duration::from_secs(10),
                    retry_until,
                )
                .await?,
                fixture.cell_options(&b).await?,
            )
            .err()
            .context("bounded admission")?;
        assert!(!rejected.error.ambiguous);
        assert!(matches!(
            rejected.error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::Overloaded)
        ));
        assert!(rejected.invocation.attempt().is_none());
        assert_eq!(dispatcher.usage().1, 2);
        drop(rejected);
        assert_eq!(dispatcher.usage().1, 1);
        if mode == 1 {
            let error = call.response().await.err().context("response deadline")?;
            assert!(error.ambiguous);
            assert!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
            ));
        } else {
            drop(call);
        }
        if mode == 2 {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
                    .await
                    .is_err()
            );
            assert!(apply::pool(&fixture, 0).is_err());
            assert!(CellGroupRuntime::start(
                fixture.cluster.voters[0].log.clone(),
                RaftRuntimeOptions::default()
            )
            .await
            .is_err());
        }
        // Neither a lost waiter nor its deadline can free the entered producer.
        assert_eq!(pool.usage()?.active_cells, 1);
        assert_eq!(controllers[0].usage().retained_commands, 1);
        assert_eq!(dispatcher.usage().1, 1);
        release.send(())?;
        if mode == 2 {
            tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await??;
            assert_eq!(pool.usage()?.running_workers, 0);
            assert_eq!(pool.usage()?.active_cells, 0);
            assert_eq!(controllers[0].usage().retained_commands, 0);
            assert_eq!(dispatcher.usage().1, 0);
        } else {
            wait_for(async || {
                Ok(controllers[0].usage().retained_commands == 0
                    && pool.usage()?.active_cells == 0
                    && dispatcher.usage().1 == 0)
            })
            .await?;
            let sealed = phase == 0 && mode == 1;
            let state = fixture.runtimes[0]
                .reconciliation_authority(route.history, a.clone())
                .await?;
            assert_eq!(
                state.cell().version.sequence.get(),
                if sealed { 1 } else { 2 }
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            // Retrying the same identity reads a committed receipt, or executes
            // once after the exact untouched preparation was sealed and closed.
            let retry = controllers[0].command(
                invocation(
                    &dispatcher,
                    &class,
                    &a,
                    Duration::from_secs(15),
                    retry_until,
                )
                .await?,
                fixture.cell_options(&a).await?,
            )?;
            let response = retry.response().await?;
            assert_eq!(response.receipt().receipt.result(), b"owned-result");
            assert_eq!(response.receipt().version.history, a);
            assert_eq!(response.receipt().version.sequence.get(), 2);
            assert_eq!(calls.load(Ordering::SeqCst), if sealed { 2 } else { 1 });
            let now = fixture
                .cluster
                .domain
                .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
                .await?;
            assert_eq!(
                keys.verify_cell(response.token(), &a, route.schema_digest, now)?,
                response.receipt().observation()
            );
            drop(response);
            wait_for(async || {
                Ok(controllers[0].usage().retained_commands == 0 && dispatcher.usage().1 == 0)
            })
            .await?;
            // Successful, unpolled replies also retain node admission.
            let unpolled = controllers[0].command(
                invocation(
                    &dispatcher,
                    &class,
                    &b,
                    Duration::from_secs(15),
                    retry_until,
                )
                .await?,
                fixture.cell_options(&b).await?,
            )?;
            wait_for(async || Ok(dispatcher.usage().1 == 0 && pool.usage()?.active_cells == 0))
                .await?;
            assert_eq!(controllers[0].usage().retained_commands, 1);
            let refused = controllers[0]
                .command(
                    invocation(
                        &dispatcher,
                        &class,
                        &a,
                        Duration::from_secs(15),
                        retry_until,
                    )
                    .await?,
                    fixture.cell_options(&a).await?,
                )
                .err()
                .context("unpolled result admission")?;
            assert!(matches!(
                refused.error.cause.as_ref(),
                CellDispatchError::Runtime(RuntimeError::Overloaded)
            ));
            drop(refused);
            assert_eq!(unpolled.response().await?.receipt().version.history, b);
            wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
        }
        for controller in &controllers {
            controller.close().await?;
        }
        drop(controllers);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn owned_command_failure(lost_reply: bool) -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let directory = root
            .path()
            .join(if rooted { "rooted-0" } else { "node-1/images" });
        let faults = physical::Faults::new(directory, None);
        let filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |context, _| {
                context.sql().execute("UPDATE value SET n=n+1", [])?;
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    if lost_reply {
                        // Application panics are contained by the transaction.
                        // Lose the worker reply during subsequent physical capture.
                        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(|| {
                            panic!("injected lost SQL stage reply during image capture");
                        }));
                    } else {
                        anyhow::bail!("injected callback refusal");
                    }
                }
                Ok(b"retry-result".to_vec())
            },
        )?);
        let (mut fixture, route, a, _) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            class.clone(),
            VersionKeys::new(
                VersionSigningKey::new([89; 32], u64::MAX)?,
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
        let call = controllers[0].command(
            invocation(
                &dispatcher,
                &class,
                &a,
                Duration::from_secs(15),
                retry_until,
            )
            .await?,
            fixture.cell_options(&a).await?,
        )?;
        let error = call.response().await.err().context("failed stage")?;
        assert!(error.ambiguous);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if lost_reply {
            assert!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Refused(CommandFailure::Unavailable))
            ));
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await?,
                Err(CellGroupControllerError::Command(_))
            ));
            assert_eq!(pool.usage()?.running_workers, 0);
            assert_eq!(controllers[0].usage().retained_commands, 0);
            assert_eq!(dispatcher.usage().1, 0);
            assert!(pool.close().await.is_err());
            fixture.appliers.remove(&(0, route.history));
        } else {
            assert!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Storage(_))
            ));
            wait_for(async || {
                Ok(controllers[0].usage().retained_commands == 0 && dispatcher.usage().1 == 0)
            })
            .await?;
            assert!(!controllers[0].usage().closing);
            assert_eq!(pool.usage()?.active_cells, 0);
            let state = fixture.runtimes[0]
                .reconciliation_authority(route.history, a.clone())
                .await?;
            assert_eq!(state.cell().version.sequence.get(), 1);
            let retried = controllers[0]
                .command(
                    invocation(
                        &dispatcher,
                        &class,
                        &a,
                        Duration::from_secs(15),
                        retry_until,
                    )
                    .await?,
                    fixture.cell_options(&a).await?,
                )?
                .response()
                .await?;
            assert_eq!(retried.receipt().receipt.result(), b"retry-result");
            assert_eq!(retried.receipt().version.sequence.get(), 2);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            drop(retried);
            controllers[0].close().await?;
        }
        for controller in &controllers[1..] {
            controller.close().await?;
        }
        drop(controllers);
        drop(pool);
        fixture.close().await?;
    }
    Ok(())
}
