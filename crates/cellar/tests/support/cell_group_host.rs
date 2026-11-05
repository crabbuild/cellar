// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Namespace host over three engines/TLS peers, with production enrollment only.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

async fn idle(host: &CellGroupHost) -> Result<()> {
    controller::wait_for(async || Ok(host.usage().controller.retained_commands == 0)).await
}

fn routing() -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(15),
        max_attempts: 4,
    }
}

async fn setup(
    rooted: bool,
    class: &Arc<RaftClass>,
) -> Result<(RuntimeCluster, Vec<CellGroupController>, GroupRoute)> {
    let filesystem = Arc::new(DirectFileSystem);
    let cluster = Box::pin(Cluster::with_store_birth(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        filesystem.clone(),
        class.schema_digest(),
        Arc::new(object_store::memory::InMemory::new()),
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            minimum: Durability::Quorum,
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            log: ClusterLogLimits::default(),
        },
    ))
    .await?;
    let mut f = Box::pin(RuntimeCluster::from_cluster(cluster, 256, 256, 32)).await?;
    if rooted {
        for (node, voter) in f.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                f.cluster._root.path().join(format!("rooted-{node}")),
                filesystem.clone(),
                Default::default(),
            )?);
        }
    }
    let route = f.cluster.birth.route().clone();
    let template = history(&route, "resource-template")?;
    for node in 0..3 {
        apply::install(&mut f, &template, node).await?;
    }
    let controllers = controller::start_with_limits(
        &mut f,
        CellGroupControllerLimits {
            max_groups: 1,
            max_parallel_apply: 1,
            max_commands: 1,
            ..controller::limits()
        },
    )
    .await?;
    f.runtimes[0].campaign(route.history).await?;
    controller::wait_for(async || Ok(f.runtimes[0].status(route.history).await?.ready)).await?;
    Ok((f, controllers, route))
}

async fn registration(
    f: &RuntimeCluster,
    class: &Arc<RaftClass>,
    node: usize,
) -> Result<CellGroupHostRegistration> {
    let template = history(f.cluster.birth.route(), "namespace-options")?;
    let mut options = f.cell_options_at(&template, node).await?;
    let base = f.cluster._root.path().join(format!("namespace-sql-{node}"));
    match &mut options {
        CellSqlOptions::Materialized(o) => o.directory = base,
        CellSqlOptions::Extents(o) => o.cache_directory = base,
    }
    Ok(CellGroupHostRegistration {
        dispatcher: remote_queries::dispatcher(f, class.clone(), remote_queries::keys(190)?)?,
        storage: CellGroupNamespaceStorage::new(options)?,
    })
}

async fn bind(
    f: &RuntimeCluster,
    controllers: &[CellGroupController],
    class: &Arc<RaftClass>,
    node: usize,
) -> Result<CellGroupHost> {
    Ok(CellGroupHost::bind(
        &f.runtimes[node],
        &f.appliers[&(node, f.cluster.birth.route().history)].0,
        &f.transports[node],
        controllers[node].clone(),
        vec![registration(f, class, node).await?],
        CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 8 * 1024 * 1024,
        },
    )?)
}

fn command(class: &RaftClass, value: u8, retry_until_ms: u64) -> Result<CellCommandRequest> {
    Ok(CellCommandRequest {
        id: CommandId::new(ClientId::new([173; 16])?, 1)?,
        contract: class.contract("Add").context("Add")?,
        input: vec![value],
        durability: Durability::Quorum,
        retry_until_ms,
    })
}

fn query(class: &RaftClass, consistency: CellQueryConsistency) -> Result<CellQueryRequest> {
    Ok(CellQueryRequest {
        invocation: ProposalId::new([174; 16])?,
        contract: class.contract("Get").context("Get")?,
        input: vec![],
        consistency,
    })
}

pub(crate) async fn many_cells(rooted: bool) -> Result<()> {
    let initializers = Arc::new(AtomicUsize::new(0));
    let commands = Arc::new(AtomicUsize::new(0));
    let inits = initializers.clone();
    let calls = commands.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .initializer([83; 32], move |_, tx| {
                inits.fetch_add(1, Ordering::SeqCst);
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(0);")?;
                Ok(())
            })?
            .command("Add", [82; 32], move |ctx, input| {
                calls.fetch_add(1, Ordering::SeqCst);
                let n = *input.first().context("amount")?;
                ctx.sql()
                    .execute("UPDATE value SET n=n+?", [i64::from(n)])?;
                Ok(input.to_vec())
            })?
            .query("Get", [84; 32], |ctx, _| {
                Ok(ctx
                    .query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            })?,
    );
    let (f, controllers, route) = Box::pin(setup(rooted, &class)).await?;
    let retry_until = f
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
        .await?;
    let mut hosts = Vec::new();
    for node in 0..3 {
        hosts.push(bind(&f, &controllers, &class, node).await?);
    }
    let bytes_before = hosts[0].usage().namespace_bytes;
    let mut observations = Vec::new();
    let mut tokens = Vec::new();
    for i in 0..8u8 {
        let cell = history(&route, &format!("host-{i}"))?;
        let handle = hosts[0].cell(cell.clone())?;
        let enrollment = handle
            .enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([i + 1; 16])?,
                    durability: Durability::Quorum,
                },
                routing(),
            )
            .await?;
        assert_eq!(enrollment.record().cell(), &cell);
        drop(enrollment);
        idle(&hosts[0]).await?;
        let call = handle.command(command(&class, i + 1, retry_until)?, routing())?;
        // The response remains unpolled while the node enters the callback.
        controller::wait_for(async || Ok(commands.load(Ordering::SeqCst) == usize::from(i) + 1))
            .await?;
        let response = call.response().await?;
        assert_eq!(
            response
                .local_receipt()
                .expect("local command receipt")
                .receipt
                .result(),
            &[i + 1]
        );
        assert_eq!(
            response
                .local_receipt()
                .expect("local command receipt")
                .version
                .history,
            cell
        );
        assert_eq!(
            response
                .local_receipt()
                .expect("local command receipt")
                .version
                .sequence
                .get(),
            2
        );
        observations.push(
            response
                .local_receipt()
                .expect("local command receipt")
                .observation(),
        );
        tokens.push(response.token().clone());
        drop(response);
        idle(&hosts[0]).await?;
    }
    assert_eq!(initializers.load(Ordering::SeqCst), 8);
    assert_eq!(commands.load(Ordering::SeqCst), 8);
    for (i, observation) in observations.iter().enumerate() {
        for (node, host) in hosts.iter().enumerate() {
            let handle = host.cell(observation.version.history.clone())?;
            assert_eq!(handle.history(), &observation.version.history);
            let response = handle
                .query(
                    query(&class, CellQueryConsistency::AtLeast(tokens[i].clone()))?,
                    routing(),
                )?
                .response()
                .await?;
            assert_eq!(response.result(), (i as i64 + 1).to_be_bytes());
            assert_eq!(response.observation(), observation);
            drop(response);
            idle(host).await?;
            assert_eq!(host.usage().namespaces, 1);
            assert_eq!(host.usage().controller.groups, 1);
            assert_eq!(f.cluster.voters[node].log.cell_groups()?.len(), 1);
        }
    }
    let first = hosts[0].cell(observations[0].version.history.clone())?;
    let retry = first
        .command(command(&class, 1, retry_until)?, routing())?
        .response()
        .await?;
    assert_eq!(
        retry
            .local_receipt()
            .expect("local command receipt")
            .observation(),
        observations[0]
    );
    drop(retry);
    idle(&hosts[0]).await?;
    assert_eq!(commands.load(Ordering::SeqCst), 8);
    let receipt = first
        .receipt(
            CellReceiptRequest {
                id: command(&class, 1, retry_until)?.id,
                contract: class.contract("Add").context("Add")?,
                input: vec![1],
                durability: Durability::Quorum,
            },
            routing(),
        )
        .await?;
    assert_eq!(
        receipt.receipt().context("retained receipt")?.result(),
        &[1]
    );
    drop(receipt);
    let wrong_token = first
        .query(
            query(&class, CellQueryConsistency::AtLeast(tokens[1].clone()))?,
            routing(),
        )?
        .response()
        .await;
    assert!(wrong_token.is_err());
    let mut tenant = first.history().clone();
    tenant.namespace = NamespaceId::new([99; 16])?;
    assert!(hosts[0].cell(tenant).is_err());
    let mut wrong = first.history().clone();
    wrong.incarnation = IncarnationId::new([99; 16])?;
    let refused = hosts[0]
        .cell(wrong)?
        .query(query(&class, CellQueryConsistency::Stale)?, routing())?
        .response()
        .await;
    assert!(refused.is_err());
    for _ in 0..100 {
        drop(hosts[0].cell(first.history().clone())?);
    }
    assert_eq!(hosts[0].usage().namespace_bytes, bytes_before);
    for host in &hosts {
        host.close().await?;
        assert!(host.is_closed());
    }
    assert!(first
        .command(command(&class, 1, retry_until)?, routing())
        .is_err());
    assert!(first
        .query(query(&class, CellQueryConsistency::Stale)?, routing())
        .is_err());
    drop((first, hosts, controllers));
    f.close().await?;
    Ok(())
}

pub(crate) async fn query_shutdown(rooted: bool) -> Result<()> {
    for expired in [false, true] {
        held_query_shutdown(rooted, expired).await?;
    }
    Ok(())
}

async fn held_query_shutdown(rooted: bool, expired: bool) -> Result<()> {
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, held) = std::sync::mpsc::sync_channel(1);
    let held = Mutex::new(held);
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .initializer([83; 32], |_, tx| {
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                Ok(())
            })?
            .command("Add", [82; 32], |_, input| Ok(input.to_vec()))?
            .query("Get", [84; 32], move |ctx, _| {
                let _ = entered
                    .lock()
                    .unwrap()
                    .take()
                    .context("one held query")?
                    .send(());
                held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                Ok(ctx
                    .query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            })?,
    );
    let (f, controllers, route) = Box::pin(setup(rooted, &class)).await?;
    let retry_until = f
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
        .await?;
    let host = bind(&f, &controllers, &class, 0).await?;
    let handle = host.cell(history(&route, "held-read")?)?;
    drop(
        handle
            .enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([29; 16])?,
                    durability: Durability::Quorum,
                },
                routing(),
            )
            .await?,
    );
    idle(&host).await?;
    let pool = f.appliers[&(0, route.history)].0.clone();
    let mut limits = routing();
    if expired {
        limits.timeout = Duration::from_millis(600);
    }
    let call = handle.query(query(&class, CellQueryConsistency::Linearizable)?, limits)?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    assert_eq!(host.usage().controller.retained_commands, 1);
    if expired {
        let error = call
            .response()
            .await
            .err()
            .context("original read deadline")?;
        assert!(matches!(
            error.as_ref(),
            CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
        ));
    } else {
        drop(call);
    }
    assert_eq!(host.usage().controller.retained_commands, 1);
    assert!(
        f.cluster
            .domain
            .services()
            .application_work()
            .pending_events
            > 0
    );
    let refused = handle
        .command(command(&class, 1, retry_until)?, routing())
        .err()
        .context("query retains global request admission")?;
    assert!(matches!(
        refused.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::Overloaded)
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), host.close())
            .await
            .is_err()
    );
    assert!(host.is_closed());
    assert!(CellGroupRuntime::start(
        f.cluster.voters[0].log.clone(),
        RaftRuntimeOptions::default()
    )
    .await
    .is_err());
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), host.close()).await??;
    assert_eq!(pool.usage()?.running_workers, 0);
    assert_eq!(host.usage().controller.retained_commands, 0);
    assert_eq!(
        f.cluster
            .domain
            .services()
            .application_work()
            .pending_events,
        0
    );
    for c in &controllers {
        c.close().await?;
    }
    drop((host, handle, controllers, pool));
    f.close().await?;
    Ok(())
}

pub(crate) async fn registration_and_deadline(rooted: bool) -> Result<()> {
    let initializers = Arc::new(AtomicUsize::new(0));
    let counted = initializers.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .initializer([83; 32], move |_, tx| {
                counted.fetch_add(1, Ordering::SeqCst);
                tx.execute_batch("CREATE TABLE value(n);")?;
                Ok(())
            })?
            .command("Add", [82; 32], |_, input| Ok(input.to_vec()))?,
    );
    let (f, controllers, route) = Box::pin(setup(rooted, &class)).await?;
    let pool = &f.appliers[&(0, route.history)].0;
    let duplicate = CellGroupHost::bind(
        &f.runtimes[0],
        pool,
        &f.transports[0],
        controllers[0].clone(),
        vec![
            registration(&f, &class, 0).await?,
            registration(&f, &class, 0).await?,
        ],
        CellGroupHostLimits {
            max_namespaces: 2,
            namespace_bytes: 16 * 1024 * 1024,
        },
    );
    assert!(matches!(
        duplicate,
        Err(CellDispatchError::Invalid("duplicate host namespace"))
    ));
    let capacity = CellGroupHost::bind(
        &f.runtimes[0],
        pool,
        &f.transports[0],
        controllers[0].clone(),
        vec![registration(&f, &class, 0).await?],
        CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 8192,
        },
    );
    assert!(matches!(
        capacity,
        Err(CellDispatchError::Runtime(RuntimeError::Overloaded))
    ));
    let foreign = CellGroupHost::bind(
        &f.runtimes[0],
        pool,
        &f.transports[0],
        controllers[1].clone(),
        vec![registration(&f, &class, 0).await?],
        CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 8 * 1024 * 1024,
        },
    );
    assert!(matches!(
        foreign,
        Err(CellDispatchError::Invalid("shared host resource owner"))
    ));
    assert!(!controllers[0].usage().closing);
    let host = bind(&f, &controllers, &class, 0).await?;
    let handle = host.cell(history(&route, "expired-enrollment")?)?;
    let request = CellEnrollmentRequest {
        creation: ProposalId::new([39; 16])?,
        durability: Durability::Quorum,
    };
    let pending = handle.enroll(
        request,
        CellRoutingLimits {
            timeout: Duration::from_millis(40),
            max_attempts: 1,
        },
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    let error = pending
        .await
        .err()
        .context("original enrollment deadline")?;
    assert!(!error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::Deadline,
            ..
        })
    ));
    assert_eq!(initializers.load(Ordering::SeqCst), 0);
    assert_eq!(pool.usage()?.active_cells, 0);
    drop(handle.enroll(request, routing()).await?);
    assert_eq!(initializers.load(Ordering::SeqCst), 1);
    for c in &controllers {
        c.close().await?;
    }
    drop((host, handle, controllers));
    f.close().await?;
    Ok(())
}
