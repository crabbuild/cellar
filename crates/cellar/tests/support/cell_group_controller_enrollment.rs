// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered creation on the shared controller, SQL pool and physical TLS peers.
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

async fn enroll(
    dispatcher: &CellGroupDispatcher,
    cell: &CellHistory,
    tag: u8,
    timeout: Duration,
) -> Result<CellEnrollmentInvocation> {
    Ok(dispatcher
        .enroll(
            cell.clone(),
            CellEnrollmentRequest {
                creation: ProposalId::new([tag; 16])?,
                durability: Durability::Quorum,
            },
            CellRoutingLimits {
                timeout,
                max_attempts: 4,
            },
            None,
        )
        .await?)
}

fn dispatcher(fixture: &RuntimeCluster, class: Arc<RaftClass>) -> Result<CellGroupDispatcher> {
    Ok(CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class,
        VersionKeys::new(
            VersionSigningKey::new([89; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?,
        CellDispatchLimits {
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?)
}

// phase 0 holds initialization; phase 1 holds committed manifest publication.
// mode 0 disconnects; 1 expires the response; 2 cancels physical shutdown.
pub(crate) async fn owned_enrollment(phase: u8, mode: u8) -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let faults = physical::Faults::new(root.path().join("node-1/pages"), None);
        let filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Arc::new(Mutex::new(held));
        let init_entered = entered.clone();
        let init_held = held.clone();
        let base = || RaftClass::new("Counter", 1, [81; 32]);
        let missing = base()?;
        let class = Arc::new(
            base()?
                .initializer([90; 32], move |cell, tx| {
                    let first = count.fetch_add(1, Ordering::SeqCst) == 0;
                    observed.lock().unwrap().push((cell.clone(), tx.inputs()));
                    if first && phase == 0 {
                        let _ = init_entered
                            .lock()
                            .unwrap()
                            .take()
                            .context("one initializer")?
                            .send(());
                        init_held
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(15))?;
                    }
                    tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                    Ok(())
                })?
                .command("Add", [82; 32], |context, _| {
                    context.sql().execute("UPDATE value SET n=n+1", [])?;
                    Ok(b"updated".to_vec())
                })?,
        );
        assert_ne!(missing.schema_digest(), class.schema_digest());
        assert!(base()?.initializer([0; 32], |_, _| Ok(())).is_err());
        assert!(base()?
            .initializer([90; 32], |_, _| Ok(()))?
            .initializer([90; 32], |_, _| Ok(()))
            .is_err());
        let different =
            base()?
                .initializer([91; 32], |_, _| Ok(()))?
                .command("Add", [82; 32], |_, _| Ok(vec![]))?;
        assert_ne!(different.schema_digest(), class.schema_digest());
        let (mut fixture, route, existing, _) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        let controllers = controller::start_with_limits(
            &mut fixture,
            CellGroupControllerLimits {
                max_commands: 1,
                ..controller::limits()
            },
        )
        .await?;
        let dispatcher = dispatcher(&fixture, class.clone())?;
        let cell = history(&route, "new-enrollment")?;
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let rejected = dispatcher
            .enroll(
                cell.clone(),
                CellEnrollmentRequest {
                    creation: ProposalId::new([170; 16])?,
                    durability: Durability::S3,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(5),
                    max_attempts: 4,
                },
                None,
            )
            .await
            .err()
            .context("S3 refusal")?;
        assert!(!rejected.ambiguous);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        if phase == 1 {
            *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
                let _ = entered
                    .lock()
                    .unwrap()
                    .take()
                    .expect("apply entered")
                    .send(());
                held.lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(15))
                    .expect("apply released");
            }));
        }
        let timeout = if mode == 1 {
            Duration::from_millis(400)
        } else {
            Duration::from_secs(15)
        };
        let call = controllers[0].enroll(
            enroll(&dispatcher, &cell, 170, timeout).await?,
            fixture.cell_options(&cell).await?,
        )?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(seen.lock().unwrap()[0].0, cell);
        assert_eq!(pool.usage()?.active_cells, 1);
        assert_eq!(dispatcher.usage().1, 1);
        assert_eq!(controllers[0].usage().retained_commands, 1);
        // Enrollment consumes the same controller budget as ordinary commands.
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
            .await?;
        let command = dispatcher
            .command(
                existing.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([171; 16])?, 1)?,
                    contract: class.contract("Add").context("Add")?,
                    input: vec![],
                    durability: Durability::Quorum,
                    retry_until_ms: retry_until,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(15),
                    max_attempts: 4,
                },
                None,
            )
            .await?;
        let refused = controllers[0]
            .command(command, fixture.cell_options(&existing).await?)
            .err()
            .context("shared admission")?;
        assert!(matches!(
            refused.error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::Overloaded)
        ));
        drop(refused);
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
        }
        assert_eq!(controllers[0].usage().retained_commands, 1);
        assert_eq!(pool.usage()?.active_cells, 1);
        release.send(())?;
        if mode == 2 {
            tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await??;
            assert_eq!(pool.usage()?.running_workers, 0);
            assert_eq!(pool.usage()?.active_cells, 0);
            assert_eq!(controllers[0].usage().retained_commands, 0);
            assert_eq!(dispatcher.usage().1, 0);
        } else {
            wait_for(async || {
                Ok(
                    controllers[0].usage().retained_commands == 0
                        && pool.usage()?.active_cells == 0,
                )
            })
            .await?;
            let state = fixture.runtimes[0]
                .enrollment_state(route.history, cell.clone())
                .await?;
            if phase == 0 && mode == 1 {
                assert!(state.record().is_none());
            } else {
                assert_eq!(
                    state.record().context("committed creation")?.creation(),
                    ProposalId::new([170; 16])?
                );
            }
            let original = controllers[0]
                .enroll(
                    enroll(&dispatcher, &cell, 170, Duration::from_secs(15)).await?,
                    fixture.cell_options(&cell).await?,
                )?
                .response()
                .await?;
            let record = original.record().clone();
            drop(original);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if phase == 0 && mode == 1 { 2 } else { 1 }
            );
            wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
            let command = dispatcher
                .command(
                    cell.clone(),
                    CellCommandRequest {
                        id: CommandId::new(ClientId::new([171; 16])?, 1)?,
                        contract: class.contract("Add").context("Add")?,
                        input: vec![],
                        durability: Durability::Quorum,
                        retry_until_ms: retry_until,
                    },
                    CellRoutingLimits {
                        timeout: Duration::from_secs(15),
                        max_attempts: 4,
                    },
                    None,
                )
                .await?;
            let updated = controllers[0]
                .command(command, fixture.cell_options(&cell).await?)?
                .response()
                .await?;
            assert_eq!(updated.receipt().version.sequence.get(), 2);
            drop(updated);
            wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
            let duplicate = controllers[0].enroll(
                enroll(&dispatcher, &cell, 170, Duration::from_secs(15)).await?,
                fixture.cell_options(&cell).await?,
            )?;
            wait_for(async || Ok(pool.usage()?.active_cells == 0 && dispatcher.usage().1 == 0))
                .await?;
            assert_eq!(controllers[0].usage().retained_commands, 1); // Unconsumed creation response remains charged.
            let duplicate = duplicate.response().await?;
            assert_eq!(duplicate.record(), &record);
            drop(duplicate);
            wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
            for incarnation in [false, true] {
                let mut conflict = cell.clone();
                if incarnation {
                    conflict.incarnation = IncarnationId::new([172; 16])?;
                }
                let rejected = controllers[0]
                    .enroll(
                        enroll(&dispatcher, &conflict, 173, Duration::from_secs(15)).await?,
                        fixture.cell_options(&conflict).await?,
                    )?
                    .response()
                    .await
                    .err()
                    .context("creation conflict")?;
                assert!(matches!(
                    rejected.cause.as_ref(),
                    CellDispatchError::Execution(ExecutionError::Refused(
                        CommandFailure::CommandConflict
                    ))
                ));
                wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if phase == 0 && mode == 1 { 2 } else { 1 }
            );
            assert!(!controllers[0].usage().closing);
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

pub(crate) async fn owned_enrollment_failure(lost_reply: bool) -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let faults = physical::Faults::new(
            root.path()
                .join(if rooted { "rooted-0" } else { "node-1/images" }),
            None,
        );
        let filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.initializer(
            [90; 32],
            move |_, tx| {
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    if lost_reply {
                        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(|| {
                            panic!("injected initialization capture reply loss")
                        }));
                    } else {
                        anyhow::bail!("injected initializer failure");
                    }
                }
                Ok(())
            },
        )?);
        let (mut fixture, route, _, _) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatcher = dispatcher(&fixture, class)?;
        let cell = history(&route, "failed-enrollment")?;
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let error = controllers[0]
            .enroll(
                enroll(&dispatcher, &cell, 174, Duration::from_secs(15)).await?,
                fixture.cell_options(&cell).await?,
            )?
            .response()
            .await
            .err()
            .context("failed initializer")?;
        assert!(error.ambiguous);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if lost_reply {
            assert!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Refused(CommandFailure::Unavailable))
            ));
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await?,
                Err(CellGroupControllerError::Enrollment(_))
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
            assert!(fixture.runtimes[0]
                .enrollment_state(route.history, cell.clone())
                .await?
                .record()
                .is_none());
            let result = controllers[0]
                .enroll(
                    enroll(&dispatcher, &cell, 174, Duration::from_secs(15)).await?,
                    fixture.cell_options(&cell).await?,
                )?
                .response()
                .await?;
            assert_eq!(result.record().creation(), ProposalId::new([174; 16])?);
            drop(result);
            assert_eq!(calls.load(Ordering::SeqCst), 2);
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

pub(crate) async fn enrollment_requires_registration() -> Result<()> {
    let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?);
    let (fixture, route, _, _) = Box::pin(registered::setup(false, &class)).await?;
    let dispatcher = dispatcher(&fixture, class)?;
    let error = dispatcher
        .enroll(
            history(&route, "unregistered")?,
            CellEnrollmentRequest {
                creation: ProposalId::new([175; 16])?,
                durability: Durability::Quorum,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(5),
                max_attempts: 4,
            },
            None,
        )
        .await
        .err()
        .context("initializer required")?;
    assert!(!error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Registration(RegistrationError::Invalid("missing initializer"))
    ));
    assert_eq!(dispatcher.usage().1, 0);
    drop(dispatcher);
    fixture.close().await
}
