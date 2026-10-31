// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real TLS ingress never inherits write authority from read grants or membership.
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
    class: &RaftClass,
    cell: &CellHistory,
    retry_until: u64,
    input: &[u8],
) -> Result<CellCommandInvocation> {
    let mut command = dispatch
        .command(
            cell.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([211; 16])?, 1)?,
                contract: class.contract("Add").context("Add")?,
                input: input.to_vec(),
                durability: Durability::Quorum,
                retry_until_ms: retry_until,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(10),
                max_attempts: 8,
            },
            None,
        )
        .await?;
    command.begin().await?;
    Ok(command)
}

fn grant(fixture: &RuntimeCluster, router: &Router, revision: u64) -> Result<()> {
    let history = fixture.cluster.birth.route().history;
    fixture.transports[0].grant_command_origin(NamespaceCommandOrigin {
        cluster: history.cluster,
        namespace: history.namespace,
        revision,
        peer: router.peer.clone(),
    })?;
    Ok(())
}

pub(crate) async fn remote_commands() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |ctx, input| {
                count.fetch_add(1, Ordering::SeqCst);
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                let mut result = ctx.inputs().random.to_vec();
                result.extend_from_slice(&ctx.inputs().now_unix_millis.to_be_bytes());
                result.extend_from_slice(input);
                Ok(result)
            },
        )?);
        let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let options = fixture.cell_options(&a).await?;
        assert!(matches!(
            fixture.transports[0].install_cell_command_gateway(
                dispatch.clone(),
                controllers[1].clone(),
                options.clone()
            ),
            Err(TransportError::Identity)
        ));
        let gateway = fixture.transports[0].install_cell_command_gateway(
            dispatch.clone(),
            controllers[0].clone(),
            options,
        )?;
        let router = Router::start(&fixture).await?;
        router
            .transport
            .install_cell_query_group(fixture.tcp_route(route.history)?)?;
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        let mut command = invocation(&dispatch, &class, &a, retry_until, b"once").await?;
        let deadline = command.attempt().unwrap().deadline_ms();
        assert!(command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut wrong_process = router.peer.clone();
        wrong_process.process = ProcessGeneration::new([213; 16])?;
        fixture.transports[0].grant_command_origin(NamespaceCommandOrigin {
            cluster: a.cluster,
            namespace: a.namespace,
            revision: 1,
            peer: wrong_process,
        })?;
        command.retry().await?;
        assert!(command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        fixture.transports[0].revoke_command_origin(a.cluster, a.namespace, router.peer.node, 2)?;
        router.grant(&fixture, 1)?;
        command.retry().await?;
        assert!(command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Voter membership also does not confer write permission.
        let mut voter = invocation(&dispatch, &class, &a, retry_until, b"once").await?;
        assert!(voter
            .execute_remote(&fixture.transports[1], MemberId::new(1)?)
            .await
            .is_err());
        drop(voter);
        fixture.transports[0].grant_command_origin(NamespaceCommandOrigin {
            cluster: a.cluster,
            namespace: NamespaceId::new([212; 16])?,
            revision: 1,
            peer: router.peer.clone(),
        })?;
        command.retry().await?;
        assert!(command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        grant(&fixture, &router, 3)?;
        command.retry().await?;
        assert_eq!(command.attempt().unwrap().deadline_ms(), deadline);
        fixture.transports[1].grant_command_origin(NamespaceCommandOrigin {
            cluster: a.cluster,
            namespace: a.namespace,
            revision: 1,
            peer: router.peer.clone(),
        })?;
        let follower_gateway = fixture.transports[1].install_cell_command_gateway(
            dispatch.clone(),
            controllers[1].clone(),
            fixture.cell_options_at(&a, 1).await?,
        )?;
        let mut follower = invocation(&dispatch, &class, &a, retry_until, b"once").await?;
        let refusal = follower
            .execute_remote(&router.transport, MemberId::new(2)?)
            .await
            .err()
            .context("follower refuses")?;
        assert!(
            matches!(
                refusal.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::NotLeader { .. }
                ))
            ),
            "{refusal:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(follower);
        drop(follower_gateway);
        let result = command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        let bytes = result.result().to_vec();
        assert_eq!(
            result.command(),
            CommandId::new(ClientId::new([211; 16])?, 1)?
        );
        assert_eq!(result.retry_until_ms(), retry_until);
        assert_eq!(result.observation().version.history, a);
        assert_eq!(result.observation().version.sequence.get(), 2);
        assert_eq!(result.durability(), Durability::Quorum);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(router.transport.usage().query_items, 1);
        assert!(router.transport.usage().query_bytes > bytes.len());
        drop(result);
        command.retry().await?;
        assert_eq!(
            command
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await?
                .result(),
            bytes
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut conflict = invocation(&dispatch, &class, &a, retry_until, b"other").await?;
        let error = conflict
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .err()
            .context("conflicting receipt")?;
        assert!(error.ambiguous);
        assert!(matches!(
            *error.cause,
            CellDispatchError::Execution(ExecutionError::Refused(CommandFailure::CommandConflict))
        ));
        drop(conflict);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(gateway);
        command.retry().await?;
        assert!(command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(command);
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

pub(crate) async fn remote_command_lifecycle(revoke: bool) -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let entered = Arc::new(Mutex::new(Some(entered)));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Arc::new(Mutex::new(held));
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |ctx, _| {
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    let _ = entered.lock().unwrap().take().context("signal")?.send(());
                    held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                }
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(b"committed".to_vec())
            },
        )?);
        let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let gateway = fixture.transports[0].install_cell_command_gateway(
            dispatch.clone(),
            controllers[0].clone(),
            fixture.cell_options(&a).await?,
        )?;
        let router = Router::start(&fixture).await?;
        router
            .transport
            .install_cell_query_group(fixture.tcp_route(route.history)?)?;
        // No read-origin grant is installed: write permission is independent.
        grant(&fixture, &router, 1)?;
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        let mut command = invocation(&dispatch, &class, &a, retry_until, b"once").await?;
        let transport = router.transport.clone();
        let task = tokio::spawn(async move {
            command
                .execute_remote(&transport, MemberId::new(1).unwrap())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered_rx).await??;
        assert_eq!(controllers[0].usage().retained_commands, 1);
        if revoke {
            fixture.transports[0].revoke_command_origin(
                a.cluster,
                a.namespace,
                router.peer.node,
                2,
            )?;
            let error = tokio::time::timeout(Duration::from_secs(5), task)
                .await??
                .err()
                .context("revoked reply")?;
            assert!(error.ambiguous);
            assert!(grant(&fixture, &router, 1).is_err());
        } else {
            task.abort();
            assert!(task.await.err().context("cancelled task")?.is_cancelled());
        }
        wait_for(async || Ok(router.transport.usage().query_items == 0)).await?;
        assert_eq!(controllers[0].usage().retained_commands, 1);
        release.send(())?;
        wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
        if revoke {
            grant(&fixture, &router, 3)?;
        }
        let mut retry = invocation(&dispatch, &class, &a, retry_until, b"once").await?;
        let result = retry
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert_eq!(result.result(), b"committed");
        assert_eq!(result.observation().version.sequence.get(), 2);
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
