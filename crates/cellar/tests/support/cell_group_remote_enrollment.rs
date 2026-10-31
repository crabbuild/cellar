// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Enrollment traverses real TLS and the shared controller without router replicas.
use super::query_origins::Router;
use super::remote_queries::{dispatcher, keys, wait_for};
use super::*;
use cellar::application::consensus::NamespaceCommandOrigin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

async fn invocation(
    dispatch: &CellGroupDispatcher,
    cell: &CellHistory,
    tag: u8,
    timeout: Duration,
) -> Result<CellEnrollmentInvocation> {
    let mut call = dispatch
        .enroll(
            cell.clone(),
            CellEnrollmentRequest {
                creation: ProposalId::new([tag; 16])?,
                durability: Durability::Quorum,
            },
            CellRoutingLimits {
                timeout,
                max_attempts: 8,
            },
            None,
        )
        .await?;
    call.begin().await?;
    Ok(call)
}

fn grant(fixture: &RuntimeCluster, router: &Router, index: usize, revision: u64) -> Result<()> {
    let history = fixture.cluster.birth.route().history;
    fixture.transports[index].grant_command_origin(NamespaceCommandOrigin {
        cluster: history.cluster,
        namespace: history.namespace,
        revision,
        peer: router.peer.clone(),
    })?;
    Ok(())
}

pub(crate) async fn remote_enrollment() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(
            RaftClass::new("Counter", 1, [81; 32])?
                .initializer([90; 32], move |_, tx| {
                    count.fetch_add(1, Ordering::SeqCst);
                    tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                    Ok(())
                })?
                .command("Add", [82; 32], |ctx, _| {
                    ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                    Ok(b"updated".to_vec())
                })?,
        );
        let (mut fixture, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let cell = history(&route, "remote-enrollment")?;
        let gateway = fixture.transports[0].install_cell_command_gateway(
            dispatch.clone(),
            controllers[0].clone(),
            fixture.cell_options(&cell).await?,
        )?;
        let router = Router::start(&fixture).await?;
        router
            .transport
            .install_cell_query_group(fixture.tcp_route(route.history)?)?;
        let mut call = invocation(&dispatch, &cell, 215, Duration::from_secs(15)).await?;
        let deadline = call.attempt().unwrap().deadline_ms();
        assert!(call
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        router.grant(&fixture, 1)?;
        call.retry().await?;
        assert!(call
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut voter = invocation(&dispatch, &cell, 215, Duration::from_secs(15)).await?;
        assert!(voter
            .execute_remote(&fixture.transports[1], MemberId::new(1)?)
            .await
            .is_err());
        drop(voter);
        grant(&fixture, &router, 0, 1)?;
        grant(&fixture, &router, 1, 1)?;
        let follower_gateway = fixture.transports[1].install_cell_command_gateway(
            dispatch.clone(),
            controllers[1].clone(),
            fixture.cell_options_at(&cell, 1).await?,
        )?;
        let mut follower = invocation(&dispatch, &cell, 215, Duration::from_secs(15)).await?;
        let error = follower
            .execute_remote(&router.transport, MemberId::new(2)?)
            .await
            .err()
            .context("follower refuses")?;
        assert!(
            matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::NotLeader { .. }
                ))
            ),
            "{error:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(follower);
        drop(follower_gateway);
        call.retry().await?;
        assert_eq!(call.attempt().unwrap().deadline_ms(), deadline);
        let response = call
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        let original = response.record().clone();
        assert_eq!(original.cell(), &cell);
        assert_eq!(original.creation(), ProposalId::new([215; 16])?);
        assert_eq!(original.schema(), class.schema_digest());
        assert_eq!(response.served_by(), MemberId::new(1)?);
        assert_eq!(response.durability(), Durability::Quorum);
        assert_eq!(router.transport.usage().query_items, 1);
        assert!(router.transport.usage().query_bytes >= 8192);
        assert!(call
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(response);
        // Mutate through the same remote write gateway before retrying creation.
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        let mut command = dispatch
            .command(
                cell.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([216; 16])?, 1)?,
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
        command.begin().await?;
        let updated = command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert_eq!(updated.observation().version.sequence.get(), 2);
        drop(updated);
        drop(command);
        call.retry().await?;
        assert_eq!(
            call.execute_remote(&router.transport, MemberId::new(1)?)
                .await?
                .record(),
            &original
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        for new_incarnation in [false, true] {
            let mut other = cell.clone();
            if new_incarnation {
                other.incarnation = IncarnationId::new([217; 16])?;
            }
            let mut conflict = invocation(&dispatch, &other, 218, Duration::from_secs(15)).await?;
            let error = conflict
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await
                .err()
                .context("creation conflict")?;
            assert!(error.ambiguous);
            assert!(
                matches!(
                    error.cause.as_ref(),
                    CellDispatchError::Execution(ExecutionError::Refused(
                        CommandFailure::CommandConflict
                    ))
                ),
                "{error:?}"
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // A routed/possibly transmitted invocation is never fresh local work.
        assert!(controllers[0]
            .enroll(call, fixture.cell_options(&cell).await?)
            .is_err());
        let mut late = invocation(&dispatch, &cell, 215, Duration::from_secs(15)).await?;
        drop(gateway);
        assert!(late
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(late);
        assert!(router.log.cell_groups()?.is_empty());
        assert_eq!(dispatch.usage(), (0, 0));
        router.close().await?;
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}

// Disconnect/revocation hold entered initialization. Timeout holds committed
// manifest publication, so a response deadline cannot seal a committed creation.
pub(crate) async fn remote_enrollment_lifecycle(mode: u8) -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let faults =
            super::apply_faults::physical::Faults::new(root.path().join("node-1/pages"), None);
        let filesystem = Arc::new(super::apply_faults::physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Arc::new(Mutex::new(held));
        let init_entered = entered.clone();
        let init_held = held.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.initializer(
            [90; 32],
            move |_, tx| {
                if count.fetch_add(1, Ordering::SeqCst) == 0 && mode != 2 {
                    let _ = init_entered
                        .lock()
                        .unwrap()
                        .take()
                        .context("signal")?
                        .send(());
                    init_held
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(15))?;
                }
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                Ok(())
            },
        )?);
        let (mut fixture, route, _, _) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class, keys(190)?)?;
        let cell = history(&route, "remote-enrollment-lifecycle")?;
        let gateway = fixture.transports[0].install_cell_command_gateway(
            dispatch.clone(),
            controllers[0].clone(),
            fixture.cell_options(&cell).await?,
        )?;
        let router = Router::start(&fixture).await?;
        router
            .transport
            .install_cell_query_group(fixture.tcp_route(route.history)?)?;
        grant(&fixture, &router, 0, 1)?;
        if mode == 2 {
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
                    .expect("release apply");
            }));
        }
        let timeout = if mode == 2 {
            Duration::from_millis(800)
        } else {
            Duration::from_secs(15)
        };
        let mut call = invocation(&dispatch, &cell, 219, timeout).await?;
        let transport = router.transport.clone();
        let task = tokio::spawn(async move {
            call.execute_remote(&transport, MemberId::new(1).unwrap())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered_rx).await??;
        assert_eq!(controllers[0].usage().retained_commands, 1);
        if mode == 0 {
            task.abort();
            assert!(task.await.err().context("cancelled task")?.is_cancelled());
        } else {
            if mode == 1 {
                fixture.transports[0].revoke_command_origin(
                    cell.cluster,
                    cell.namespace,
                    router.peer.node,
                    2,
                )?;
            }
            let error = tokio::time::timeout(Duration::from_secs(5), task)
                .await??
                .err()
                .context("lost response")?;
            assert!(error.ambiguous);
        }
        wait_for(async || Ok(router.transport.usage().query_items == 0)).await?;
        assert_eq!(controllers[0].usage().retained_commands, 1);
        release.send(())?;
        wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
        if mode == 1 {
            grant(&fixture, &router, 0, 3)?;
        }
        let state = fixture.runtimes[0]
            .enrollment_state(route.history, cell.clone())
            .await?;
        let original = state.record().context("committed enrollment")?.clone();
        let mut retry = invocation(&dispatch, &cell, 219, Duration::from_secs(15)).await?;
        let result = retry
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert_eq!(result.record(), &original);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(result);
        drop(retry);
        drop(gateway);
        router.close().await?;
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}
