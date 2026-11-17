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
        fixture.cluster._root.disable_cleanup(true);
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let options = fixture.cell_options(&a).await?;
        assert!(matches!(
            fixture.transports[0].install_cell_command_gateway(
                dispatch.clone(),
                controllers[1].clone(),
                cellar::application::consensus::verification::cell_groups::CellGroupNamespaceStorage::new(
                    options.clone()
                )?,
            ),
            Err(TransportError::Identity)
        ));
        let gateway = fixture.transports[0].install_cell_command_gateway(
            dispatch.clone(),
            controllers[0].clone(),
            cellar::application::consensus::verification::cell_groups::CellGroupNamespaceStorage::new(options)?,
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
            cellar::application::consensus::verification::cell_groups::CellGroupNamespaceStorage::new(
                fixture.cell_options_at(&a, 1).await?,
            )?,
        )?;
        let mut follower = invocation(&dispatch, &class, &a, retry_until, b"once").await?;
        let follower_attempt = follower.attempt().unwrap().clone();
        router.transport.remove_cell_query_group(&route.history)?;
        let not_sent = follower
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .err()
            .context("missing route refuses before transmission")?;
        assert!(!not_sent.ambiguous);
        assert!(matches!(
            not_sent.cause.as_ref(),
            CellDispatchError::Transport(TransportError::CommandNotSent(
                CommandFailure::Unavailable
            ))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        router
            .transport
            .install_cell_query_group(fixture.tcp_route(route.history)?)?;
        follower.retry().await?;
        assert_eq!(
            follower.attempt().unwrap().request(),
            follower_attempt.request()
        );
        assert_eq!(
            follower.attempt().unwrap().deadline_ms(),
            follower_attempt.deadline_ms()
        );
        assert_eq!(
            follower.attempt().unwrap().ordinal(),
            follower_attempt.ordinal() + 1
        );
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
        assert!(
            !refusal.ambiguous,
            "fresh follower refusal proves no producer entered"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Use the remaining tickets of this same follower invocation: an
        // ungranted destination loses its reply, then a local proof must keep
        // that uncertainty. No new identity, deadline or bank is introduced.
        follower.retry().await?;
        let uncertain = follower
            .execute_remote(&router.transport, MemberId::new(3)?)
            .await
            .err()
            .context("ungranted third member leaves an unknown reply")?;
        assert!(uncertain.ambiguous);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        follower.retry().await?;
        router.transport.remove_cell_query_group(&route.history)?;
        let not_sent = follower
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .err()
            .context("local non-submission cannot clear earlier uncertainty")?;
        assert!(not_sent.ambiguous);
        assert!(matches!(
            not_sent.cause.as_ref(),
            CellDispatchError::Transport(TransportError::CommandNotSent(
                CommandFailure::Unavailable
            ))
        ));
        assert_eq!(
            follower.attempt().unwrap().deadline_ms(),
            follower_attempt.deadline_ms()
        );
        assert_eq!(
            follower.attempt().unwrap().request(),
            follower_attempt.request()
        );
        assert_eq!(follower.attempt().unwrap().ordinal(), 4);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        router
            .transport
            .install_cell_query_group(fixture.tcp_route(route.history)?)?;
        drop(follower);
        // This invocation already experienced unclassified transport failures.
        // The same authenticated follower rejection cannot settle that history.
        let refusal = command
            .execute_remote(&router.transport, MemberId::new(2)?)
            .await
            .err()
            .context("rejection after uncertain transmission")?;
        assert!(refusal.ambiguous);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        command.retry().await?;
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

        // Queue through the SDK's controller entry point on a node that owns
        // no group. The first peer is a live follower, so discovery must reach
        // the actual leader before sending a single mutation.
        let mut discovery = fixture.tcp_route(route.history)?;
        discovery.revision += 1;
        discovery.peers.swap(0, 1);
        router.transport.install_cell_query_group(discovery)?;
        let sql = CellGroupSqlPool::start(
            &router.log,
            fixture.appliers[&(0, route.history)].0.usage()?.limits,
        )?;
        let ingress = CellGroupController::start(
            router.runtime,
            router.events,
            router.transport.clone(),
            sql,
            vec![],
            controller::limits(),
        )
        .await?;
        let options = fixture.cell_options(&a).await?;
        let request = CellCommandRequest {
            id: CommandId::new(ClientId::new([211; 16])?, 2)?,
            contract: class.contract("Add").context("Add")?,
            input: b"routed-once".to_vec(),
            durability: Durability::Quorum,
            retry_until_ms: retry_until,
        };
        let routing = CellRoutingLimits {
            timeout: Duration::from_secs(5),
            max_attempts: 3,
        };
        let response = ingress
            .dispatch_command(
                &dispatch,
                a.clone(),
                request.clone(),
                routing,
                options.clone(),
            )?
            .response()
            .await?;
        assert!(response.local_receipt().is_none());
        assert_eq!(response.observation().version.history, a);
        assert_eq!(response.observation().version.sequence.get(), 3);
        assert!(response.result().ends_with(b"routed-once"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let saved = response.result().to_vec();
        drop(response);
        // The receiver bounds its idle header read. The sender must discard
        // the expired socket before it sends the next probe or command.
        tokio::time::sleep(Duration::from_millis(
            TransportOptions::default().io_timeout_ms + 250,
        ))
        .await;
        assert_eq!(
            ingress
                .dispatch_command(&dispatch, a.clone(), request, routing, options)?
                .response()
                .await
                .context("exact retry after query connection idle expiry")?
                .result(),
            saved
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(ingress.usage().groups, 0);
        drop(follower_gateway);
        drop(gateway);
        command.retry().await?;
        assert_eq!(command.attempt().unwrap().ordinal(), 8);
        assert!(command
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(command);
        assert!(router.log.cell_groups()?.is_empty());
        assert_eq!(dispatch.usage(), (0, 0));
        ingress.close().await?;
        router.log.close().await?;
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
        fixture.cluster._root.disable_cleanup(true);
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let gateway = fixture.transports[0].install_cell_command_gateway(
            dispatch.clone(),
            controllers[0].clone(),
            cellar::application::consensus::verification::cell_groups::CellGroupNamespaceStorage::new(
                fixture.cell_options(&a).await?,
            )?,
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

pub(crate) async fn remote_command_receipt_reconciliation(commit: bool) -> Result<()> {
    receipt_reconciliation(if commit {
        ReceiptCase::Commit
    } else {
        ReceiptCase::Rollback
    })
    .await
}

pub(crate) async fn remote_command_receipt_limits(one_ticket: bool) -> Result<()> {
    receipt_reconciliation(if one_ticket {
        ReceiptCase::OneTicket
    } else {
        ReceiptCase::OneAdmission
    })
    .await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReceiptCase {
    Commit,
    Rollback,
    OneTicket,
    OneAdmission,
}

async fn receipt_reconciliation(case: ReceiptCase) -> Result<()> {
    let commit = case != ReceiptCase::Rollback;
    let mut failures = Vec::new();
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
            move |ctx, input| {
                count.fetch_add(1, Ordering::SeqCst);
                let _ = entered
                    .lock()
                    .unwrap()
                    .take()
                    .context("one callback")?
                    .send(());
                held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                if !commit {
                    anyhow::bail!("callback rolls back before submission");
                }
                let mut output = ctx.inputs().random.to_vec();
                output.extend_from_slice(&ctx.inputs().now_unix_millis.to_be_bytes());
                output.extend_from_slice(input);
                Ok(output)
            },
        )?);
        let (mut fixture, route, cell, _) = Box::pin(registered::setup(rooted, &class)).await?;
        fixture.cluster._root.disable_cleanup(true);
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let inbound = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let dispatch = if case == ReceiptCase::OneAdmission {
            CellGroupDispatcher::new(
                &fixture.cluster.domain,
                fixture.cluster.directory.clone(),
                class.clone(),
                keys(190)?,
                CellDispatchLimits {
                    max_cached_routes: 32,
                    route_cache_bytes: 256 * 1024,
                    max_invocations: 1,
                    retained_bytes: 1024 * 1024,
                    max_input_bytes: 1024,
                },
            )?
        } else {
            inbound.clone()
        };
        let mut options = fixture.cell_options(&cell).await?;
        let base = fixture.cluster._root.path().join("receipt-gateway");
        match &mut options {
            CellSqlOptions::Materialized(options) => options.directory = base,
            CellSqlOptions::Extents(options) => options.cache_directory = base,
        }
        let gateway = fixture.transports[0].install_cell_command_gateway(
            inbound.clone(),
            controllers[0].clone(),
            cellar::application::consensus::verification::cell_groups::CellGroupNamespaceStorage::new(
                options.clone(),
            )?,
        )?;
        let Router {
            log,
            runtime,
            events,
            transport,
            peer,
        } = Router::start(&fixture).await?;
        let view = fixture.tcp_route(route.history)?;
        transport.install_cell_query_group(view.clone())?;
        fixture.transports[0].grant_command_origin(NamespaceCommandOrigin {
            cluster: cell.cluster,
            namespace: cell.namespace,
            revision: 1,
            peer,
        })?;
        let sql = CellGroupSqlPool::start(
            &log,
            fixture.appliers[&(0, route.history)].0.usage()?.limits,
        )?;
        let ingress = CellGroupController::start(
            runtime,
            events,
            transport.clone(),
            sql,
            vec![],
            controller::limits(),
        )
        .await?;
        let request = CellCommandRequest {
            id: CommandId::new(ClientId::new([221; 16])?, 1)?,
            contract: class.contract("Add").context("Add")?,
            input: b"original-input".to_vec(),
            durability: Durability::Quorum,
            retry_until_ms: fixture
                .cluster
                .domain
                .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
                .await?,
        };
        let limits = CellRoutingLimits {
            timeout: Duration::from_secs(5),
            max_attempts: if case == ReceiptCase::OneTicket { 1 } else { 3 },
        };
        let result = async {
            let call = ingress.dispatch_command(
                &dispatch,
                cell.clone(),
                request.clone(),
                limits,
                options,
            )?;
            tokio::time::timeout(Duration::from_secs(5), entered_rx).await??;
            anyhow::ensure!(
                calls.load(Ordering::SeqCst) == 1,
                "original callback entered once"
            );
            // An actual request has entered SQL. Only the sender's route view
            // changes; the peer, process, permission and committed state remain.
            let mut changed = view;
            changed.revision += 1;
            transport.install_cell_query_group(changed)?;
            release.send(())?;
            let response = call.response().await;
            wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;

            // An independent read proves the fixture's physical commit/absence.
            // It does not retry or settle the originating command invocation.
            let mut observer = inbound.lookup_receipt(
                cell.clone(),
                CellReceiptRequest {
                    id: request.id,
                    contract: request.contract.clone(),
                    input: request.input.clone(),
                    durability: request.durability,
                },
                limits,
                None,
            )?;
            observer.begin().await?;
            let found = observer
                .execute_remote(&transport, MemberId::new(1)?)
                .await?;
            anyhow::ensure!(found.command() == request.id, "original receipt ID");
            if commit && case != ReceiptCase::OneTicket {
                let actual = found
                    .result()
                    .context("committed receipt exists independently")?;
                let response =
                    response.context("original owner must reconcile its exact receipt")?;
                anyhow::ensure!(response.result() == actual, "exact original result");
                anyhow::ensure!(
                    response.result().ends_with(&request.input),
                    "original inputs"
                );
                anyhow::ensure!(
                    response.observation() == *found.observation(),
                    "exact observation"
                );
                anyhow::ensure!(
                    response.durability() == request.durability,
                    "required durability"
                );
                anyhow::ensure!(
                    response.local_receipt().is_none(),
                    "remote read grants no local producer proof"
                );
            } else {
                anyhow::ensure!(
                    found.result().is_some() == commit,
                    "physical commit/rollback outcome"
                );
                let error = response
                    .err()
                    .context("absence cannot acknowledge a command")?;
                anyhow::ensure!(
                    error.command == request.id && error.ambiguous,
                    "original identity stays unknown"
                );
            }
            anyhow::ensure!(
                calls.load(Ordering::SeqCst) == 1,
                "receipt repair never executes a callback"
            );
            drop(found);
            drop(observer);
            wait_for(async || Ok(dispatch.usage() == (0, 0) && transport.usage().query_items == 0))
                .await?;
            anyhow::ensure!(
                log.cell_groups()?.is_empty(),
                "ingress has no local replicas"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        // Always release a callback and join owners before returning a failed
        // assertion. Preserve all fixture disks, including the failing control.
        let _ = release.try_send(());
        let mut cleanup = Vec::new();
        drop(gateway);
        if let Err(error) = ingress.close().await {
            cleanup.push(format!("ingress: {error}"));
        }
        if let Err(error) = log.close().await {
            cleanup.push(format!("router log: {error}"));
        }
        for controller in controllers {
            if let Err(error) = controller.close().await {
                cleanup.push(format!("voter: {error}"));
            }
        }
        if let Err(error) = fixture.close().await {
            cleanup.push(format!("fixture: {error}"));
        }
        if let Err(error) = result {
            failures.push(format!("rooted={rooted}, commit={commit}: {error:#}"));
        }
        if !cleanup.is_empty() {
            failures.push(format!("rooted={rooted}, cleanup={cleanup:?}"));
        }
    }
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}
