// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Receipt-only inspection across real TLS and immutable SQLite images.
use super::query_origins::Router;
use super::remote_queries::{dispatcher, keys, wait_for};
use super::*;
use cellar::application::consensus::NamespaceCommandOrigin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn request(class: &RaftClass, input: &[u8]) -> Result<CellReceiptRequest> {
    Ok(CellReceiptRequest {
        id: CommandId::new(ClientId::new([221; 16])?, 1)?,
        contract: class.contract("Add").context("Add")?,
        input: input.to_vec(),
        durability: Durability::Quorum,
    })
}

fn limits() -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(15),
        max_attempts: 8,
    }
}

async fn lookup(
    dispatch: &CellGroupDispatcher,
    class: &RaftClass,
    cell: &CellHistory,
    input: &[u8],
) -> Result<CellReceiptInvocation> {
    let mut lookup =
        dispatch.lookup_receipt(cell.clone(), request(class, input)?, limits(), None)?;
    lookup.begin().await?;
    Ok(lookup)
}

async fn command(
    dispatch: &CellGroupDispatcher,
    class: &RaftClass,
    cell: &CellHistory,
    now: u64,
) -> Result<CellCommandInvocation> {
    let r = request(class, b"once")?;
    Ok(dispatch
        .command(
            cell.clone(),
            CellCommandRequest {
                id: r.id,
                contract: r.contract,
                input: r.input,
                durability: r.durability,
                retry_until_ms: now + 60_000,
            },
            limits(),
            None,
        )
        .await?)
}

fn grant(fixture: &RuntimeCluster, router: &Router) -> Result<()> {
    let g = fixture.cluster.birth.route().history;
    fixture.transports[0].grant_command_origin(NamespaceCommandOrigin {
        cluster: g.cluster,
        namespace: g.namespace,
        revision: 1,
        peer: router.peer.clone(),
    })?;
    Ok(())
}

pub(crate) async fn receipt_lookup_ingress() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |ctx, input| {
                count.fetch_add(1, Ordering::SeqCst);
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(input.to_vec())
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
        let mut read = lookup(&dispatch, &class, &a, b"once").await?;
        let deadline = read.attempt().unwrap().deadline_ms();
        assert!(read
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        router.grant(&fixture, 1)?;
        read.retry().await?;
        assert!(read
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        grant(&fixture, &router)?;
        read.retry().await?;
        assert_eq!(read.attempt().unwrap().deadline_ms(), deadline);
        let unknown = read
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert!(unknown.result().is_none());
        assert!(unknown.retry_until_ms().is_none());
        assert_eq!(unknown.observation().version.sequence.get(), 1);
        assert_eq!(unknown.command(), request(&class, b"once")?.id);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(router.transport.usage().query_items, 1);
        assert!(read
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(unknown);
        let now = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 })
            .await?;
        let committed = controllers[0]
            .command(
                command(&dispatch, &class, &a, now).await?,
                fixture.cell_options(&a).await?,
            )?
            .response()
            .await?;
        assert_eq!(committed.receipt().receipt.result(), b"once");
        drop(committed);
        read.retry().await?;
        let found = read
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert_eq!(found.result(), Some(b"once".as_slice()));
        assert_eq!(found.retry_until_ms(), Some(now + 60_000));
        assert_eq!(found.observation().version.sequence.get(), 2);
        assert_eq!(found.served_by(), MemberId::new(1)?);
        let observed = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 })
            .await?;
        assert_eq!(
            keys(190)?.verify_cell(found.token(), &a, class.schema_digest(), observed)?,
            *found.observation()
        );
        drop(found);
        let mut conflict = lookup(&dispatch, &class, &a, b"different").await?;
        assert!(matches!(
            conflict
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await
                .err(),
            Some(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::CommandConflict
            )))
        ));
        drop(conflict);
        let mut gap = request(&class, b"once")?;
        gap.id = CommandId::new(gap.id.client, 3)?;
        let mut gap = dispatch.lookup_receipt(a.clone(), gap, limits(), None)?;
        gap.begin().await?;
        assert!(matches!(
            gap.execute_remote(&router.transport, MemberId::new(1)?)
                .await
                .err(),
            Some(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::SequenceGap { next: 2 }
            )))
        ));
        drop(gap);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(gateway);
        read.retry().await?;
        assert!(read
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(read);
        assert!(router.log.cell_groups()?.is_empty());
        assert_eq!(dispatch.usage(), (0, 0));
        router.close().await?;
        for c in controllers {
            c.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}

async fn allow_two_sql_reservations(
    fixture: &mut RuntimeCluster,
    group: GroupHistory,
) -> Result<()> {
    let (old_pool, applier) = fixture.appliers.remove(&(0, group)).context("old pool")?;
    old_pool.close().await?;
    drop(old_pool);
    let pool = CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 2,
            max_pending_cells: 2,
            group_pending_cells: 2,
            node_bytes: 1024 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    fixture.appliers.insert((0, group), (pool, applier));
    Ok(())
}

pub(crate) async fn receipt_lookup_pending() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |ctx, input| {
                count.fetch_add(1, Ordering::SeqCst);
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(input.to_vec())
            },
        )?);
        let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        allow_two_sql_reservations(&mut fixture, route.history).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let now = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 })
            .await?;
        let mut mutation = command(&dispatch, &class, &a, now).await?;
        mutation.begin().await?;
        assert!(matches!(
            mutation
                .prepare(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
                .await?,
            CellCommandPreparation::Prepared
        ));
        // Inspection can coexist with a pending producer, but it shares bounded
        // SQL admission and cannot expose or retire the uncommitted receipt.
        assert_eq!(pool.usage()?.active_cells, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut read = lookup(&dispatch, &class, &a, b"once").await?;
        let empty = read
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
            .await?;
        assert!(empty.receipt().is_none());
        assert_eq!(empty.observation().version.sequence.get(), 1);
        assert_eq!(pool.usage()?.active_cells, 1);
        let mut saturated = lookup(&dispatch, &class, &a, b"once").await?;
        assert!(matches!(
            saturated
                .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
                .await,
            Err(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::Overloaded
            )))
        ));
        assert_eq!(pool.usage()?.active_cells, 1);
        drop(saturated);
        drop(empty);
        let r = request(&class, b"once")?;
        for invalid in [0, 1, 2] {
            let authority = fixture.runtimes[0]
                .read_authority(
                    route.history,
                    a.clone(),
                    if invalid == 2 {
                        CellReadConsistency::Stale
                    } else {
                        CellReadConsistency::Linearizable
                    },
                    read.attempt().unwrap().deadline_ms(),
                )
                .await?;
            let error = pool
                .lookup_receipt_registered(
                    fixture.cell_options(&a).await?,
                    authority,
                    class.clone(),
                    CommandRequest {
                        id: r.id,
                        operation: r.contract.operation.clone(),
                        contract_digest: if invalid == 0 {
                            [0; 32]
                        } else {
                            r.contract.digest
                        },
                        request_digest: if invalid == 1 {
                            [0; 32]
                        } else {
                            command_request_digest(&r.input)
                        },
                        durability: Durability::Quorum,
                        retry_until_ms: 0,
                    },
                    r.input.clone(),
                )
                .await
                .err()
                .context("invalid lookup refuses")?;
            match invalid {
                0 => assert!(matches!(error, ExecutionError::Registration(_))),
                1 => assert!(matches!(
                    error,
                    ExecutionError::Refused(CommandFailure::CommandConflict)
                )),
                _ => assert!(matches!(
                    error,
                    ExecutionError::Invalid("receipt lookup requires quorum read")
                )),
            }
        }
        assert_eq!(pool.usage()?.active_cells, 1);
        assert!(matches!(
            mutation.submit().await?,
            CellCommandSubmission::Accepted(_)
        ));
        wait_for(async || {
            match fixture.runtimes[0]
                .execution_authority(route.history, a.clone())
                .await
            {
                Ok(authority) => Ok(authority.cell().version.sequence.get() == 2),
                Err(RuntimeError::NotReady) => Ok(false),
                Err(error) => Err(error.into()),
            }
        })
        .await?;
        read.retry().await?;
        let found = read
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
            .await?;
        assert_eq!(found.receipt().context("receipt")?.result(), b"once");
        assert_eq!(pool.usage()?.active_cells, 1); // Lookup cannot finish another owner.
        drop(found);
        assert!(matches!(
            mutation.settle().await?,
            CellCommandSettlement::Committed(_)
        ));
        assert_eq!(pool.usage()?.active_cells, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(read);
        drop(mutation);
        for c in &controllers {
            c.close().await?;
        }
        // Closed controller handles cannot keep the SQL engine claim alive.
        drop(fixture.appliers.remove(&(0, route.history)));
        drop(pool);
        let replacement = apply::pool(&fixture, 0)?;
        replacement.close().await?;
        drop(replacement);
        drop(controllers);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn receipt_lookup_cancelled() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Mutex::new(held);
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |ctx, input| {
                count.fetch_add(1, Ordering::SeqCst);
                let _ = entered.lock().unwrap().take().context("entered")?.send(());
                held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(input.to_vec())
            },
        )?);
        let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        allow_two_sql_reservations(&mut fixture, route.history).await?;
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
        grant(&fixture, &router)?;
        let now = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 })
            .await?;
        let work = controllers[0].command(
            command(&dispatch, &class, &a, now).await?,
            fixture.cell_options(&a).await?,
        )?;
        tokio::time::timeout(Duration::from_secs(5), entered_rx).await??;
        let mut read = lookup(&dispatch, &class, &a, b"once").await?;
        let transport = router.transport.clone();
        let task = tokio::spawn(async move {
            read.execute_remote(&transport, MemberId::new(1).unwrap())
                .await
        });
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        wait_for(async || Ok(pool.usage()?.queued_or_running >= 2)).await?;
        task.abort();
        assert!(task.await.err().context("cancelled lookup")?.is_cancelled());
        wait_for(async || Ok(router.transport.usage().query_items == 0)).await?;
        assert_eq!(controllers[0].usage().retained_commands, 1);
        assert_eq!(pool.usage()?.active_cells, 1);
        release.send(())?;
        let committed = work.response().await?;
        drop(committed);
        wait_for(async || Ok(pool.usage()?.queued_or_running == 0)).await?;
        let mut retry = lookup(&dispatch, &class, &a, b"once").await?;
        let found = retry
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert_eq!(found.result(), Some(b"once".as_slice()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(found);
        drop(retry);
        drop(gateway);
        router.close().await?;
        for c in controllers {
            c.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}
