// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn limits() -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(15),
        max_attempts: 4,
    }
}

fn request(
    class: &RaftClass,
    sequence: u64,
    retry: u64,
    input: &[u8],
) -> Result<CellCommandRequest> {
    Ok(CellCommandRequest {
        id: CommandId::new(ClientId::new([211; 16])?, sequence)?,
        contract: class.contract("Add").context("Add")?,
        input: input.to_vec(),
        durability: Durability::Quorum,
        retry_until_ms: retry,
    })
}

async fn accepted(invocation: &mut CellCommandInvocation) -> Result<CellGroupProposal> {
    match invocation.submit().await? {
        CellCommandSubmission::Accepted(accepted) => Ok(accepted),
        CellCommandSubmission::Rejected => anyhow::bail!("unexpected sealed rejection"),
    }
}

async fn committed(invocation: &mut CellCommandInvocation) -> Result<CellCommandResponse> {
    match invocation.settle().await? {
        CellCommandSettlement::Committed(receipt) => Ok(receipt),
        _ => anyhow::bail!("expected committed receipt"),
    }
}

pub(crate) async fn commands(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let captured = Arc::new(Mutex::new(Vec::new()));
    let count = calls.clone();
    let inputs = captured.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let wait = Mutex::new(wait);
    let class = Arc::new(
        RaftClass::new("Counter", 1, [91; 32])?
            .command("Add", [92; 32], move |context, input| {
                count.fetch_add(1, Ordering::SeqCst);
                inputs.lock().unwrap().push((
                    context.identity().cell.clone(),
                    context.command(),
                    context.inputs().now_unix_millis,
                    context.inputs().random,
                ));
                if input == b"hold" && entered.lock().unwrap().is_some() {
                    entered
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(())
                        .map_err(|_| anyhow::anyhow!("entered"))?;
                    wait.lock().unwrap().recv_timeout(Duration::from_secs(10))?;
                }
                context.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(context.identity().cell.as_str().as_bytes().to_vec())
            })?
            .query("Get", [93; 32], |context, _| {
                let n: i64 = context.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                Ok(n.to_le_bytes().to_vec())
            })?,
    );
    let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
    let keys = VersionKeys::new(
        VersionSigningKey::new([94; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(30),
    )?;
    let bounds = CellDispatchLimits {
        max_invocations: 3,
        retained_bytes: 1024 * 1024,
        max_input_bytes: 1024,
    };
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        keys.clone(),
        bounds,
    )?;
    let now = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms() as u64 })
        .await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let runtime = fixture.runtimes[0].clone();
    let options = fixture.cell_options(&a).await?;
    let mut command = dispatcher
        .command(
            a.clone(),
            request(&class, 1, now + 60_000, b"hold")?,
            limits(),
            None,
        )
        .await?;
    command.begin().await?;
    let first = command.attempt().context("first route")?.clone();
    let mut preparation = Box::pin(command.prepare(&runtime, &pool, options.clone()));
    cellar::asyncrt::select! {
        result = &mut preparation => anyhow::bail!("held callback finished: {}", result.is_ok()),
        result = started => result?,
    }
    drop(preparation); // Cancelled response, not physical SQL ownership.
    assert_eq!(pool.usage()?.active_cells, 1);
    release.send(())?;
    command.retry().await?;
    assert_eq!(
        command.attempt().context("retry")?.request(),
        first.request()
    );
    assert_eq!(
        command.attempt().context("retry")?.deadline_ms(),
        first.deadline_ms()
    );
    let other_pool = fixture.appliers[&(1, route.history)].0.clone();
    assert!(matches!(
        command
            .prepare(
                &fixture.runtimes[1],
                &other_pool,
                fixture.cell_options_at(&a, 1).await?
            )
            .await?,
        CellCommandPreparation::Prepared
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(other_pool.usage()?.active_cells, 0);
    let first_acceptance = accepted(&mut command).await?;
    let again = accepted(&mut command).await?; // Lost acceptance response.
    assert_eq!(first_acceptance.position, again.position);
    assert_eq!(first_acceptance.digest, again.digest);
    let absent = command.settle().await.err().context("not yet applied")?;
    assert!(absent.ambiguous);
    assert!(matches!(
        absent.cause.as_ref(),
        CellDispatchError::Execution(ExecutionError::Uncertain)
    ));
    assert_eq!(pool.usage()?.active_cells, 1);
    fixture.drive(Some(again.position.index)).await?;
    let receipt = committed(&mut command).await?;
    assert_eq!(
        receipt.receipt().receipt.result(),
        a.cell.as_str().as_bytes()
    );
    assert_eq!(receipt.receipt().version.sequence.get(), 2);
    assert_eq!(receipt.durability(), Durability::Quorum);
    let now = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms() as u64 })
        .await?;
    assert_eq!(
        keys.verify_cell(receipt.token(), &a, route.schema_digest, now)?,
        receipt.receipt().observation()
    );
    drop(receipt);
    drop(command);

    let mut other = dispatcher
        .command(
            b.clone(),
            request(&class, 1, now + 60_000, b"ordinary")?,
            limits(),
            None,
        )
        .await?;
    other.begin().await?;
    assert!(matches!(
        other
            .prepare(&runtime, &pool, fixture.cell_options(&b).await?)
            .await?,
        CellCommandPreparation::Prepared
    ));
    let b_acceptance = accepted(&mut other).await?;
    fixture.drive(Some(b_acceptance.position.index)).await?;
    drop(committed(&mut other).await?);
    drop(other);
    let mut duplicate = dispatcher
        .command(
            a.clone(),
            request(&class, 1, now + 60_000, b"hold")?,
            limits(),
            Some(first.cached_route()),
        )
        .await?;
    duplicate.begin().await?;
    let CellCommandPreparation::Committed(receipt) =
        duplicate.prepare(&runtime, &pool, options.clone()).await?
    else {
        anyhow::bail!("duplicate executed")
    };
    assert_eq!(
        receipt.receipt().receipt.result(),
        a.cell.as_str().as_bytes()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    drop(receipt);
    drop(duplicate);

    // An actual higher-term applied barrier can exclude a prepared old leader
    // command. Retrying preserves host-captured time/randomness and command ID.
    let mut retry = dispatcher
        .command(
            a.clone(),
            request(&class, 2, now + 60_000, b"ordinary")?,
            limits(),
            Some(first.cached_route()),
        )
        .await?;
    retry.begin().await?;
    let retry_first = retry.attempt().context("retry origin")?.clone();
    assert!(matches!(
        retry.prepare(&runtime, &pool, options.clone()).await?,
        CellCommandPreparation::Prepared
    ));
    reconciliation::transfer_cell(&mut fixture, 0, 1, a.clone()).await?;
    let error = retry.submit().await.err().context("old leader refused")?;
    assert!(error.ambiguous);
    assert!(matches!(
        retry.settle().await?,
        CellCommandSettlement::NotCommitted
    ));
    assert_eq!(pool.usage()?.active_cells, 0);
    retry.retry().await?;
    assert_eq!(
        retry.attempt().context("retry new leader")?.request(),
        retry_first.request()
    );
    assert!(matches!(
        retry
            .prepare(
                &fixture.runtimes[1],
                &other_pool,
                fixture.cell_options_at(&a, 1).await?
            )
            .await?,
        CellCommandPreparation::Prepared
    ));
    let retry_acceptance = accepted(&mut retry).await?;
    reconciliation::apply_through(&mut fixture, retry_acceptance.position.index).await?;
    let receipt = committed(&mut retry).await?;
    assert_eq!(receipt.receipt().version.sequence.get(), 3);
    drop(receipt);
    drop(retry);
    let captured = captured.lock().unwrap().clone();
    assert_eq!(captured.len(), 4);
    assert_eq!(captured[2], captured[3]);

    let mut s3 = request(&class, 3, now + 60_000, b"ordinary")?;
    s3.durability = Durability::S3;
    let error = dispatcher
        .command(a.clone(), s3, limits(), None)
        .await
        .err()
        .context("S3 unavailable")?;
    assert!(!error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Execution(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability
        ))
    ));
    let catalog = Catalog::new(
        Arc::new(object_store::memory::InMemory::new()),
        CatalogOptions::default(),
    )?;
    let mut config = NamespaceConfig::new(
        a.cluster,
        a.namespace,
        cellar_logic::NamespaceMode::Raft,
        class.schema_digest(),
    );
    config.minimum_durability = Durability::S3;
    let namespace = fixture
        .cluster
        .domain
        .run(catalog.open_namespace(config))
        .await??;
    let required = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        RangeDirectory::new(namespace, DirectoryLimits::default())?,
        class.clone(),
        keys,
        bounds,
    )?;
    assert!(matches!(
        required
            .command(
                a.clone(),
                request(&class, 3, now + 60_000, b"ordinary")?,
                limits(),
                None
            )
            .await,
        Err(CellCommandError {
            ambiguous: false,
            cause,
            ..
        }) if matches!(cause.as_ref(), CellDispatchError::Execution(ExecutionError::Refused(
            CommandFailure::UnsupportedCapability
        )))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(dispatcher.usage(), (0, 0));
    drop(required);
    drop(dispatcher);
    drop(pool);
    drop(other_pool);
    drop(runtime);
    fixture.close().await
}
