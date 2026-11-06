// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Background apply and an application command share one physical SQL worker.
use super::*;

pub(crate) async fn run(expire: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .command("Add", [82; 32], move |ctx, _| {
                counted.fetch_add(1, Ordering::SeqCst);
                ctx.sql().execute("UPDATE value SET n=n+7", [])?;
                Ok(7_i64.to_be_bytes().to_vec())
            })?
            .query("Get", [83; 32], |ctx, _| {
                let value: i64 = ctx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })?,
    );
    let (mut fixture, route, cell, _) = Box::pin(registered::setup(false, &class)).await?;
    let faults = physical::Faults::new(
        fixture.cluster._root.path().join("extra-31-node-0/pages"),
        None,
    );
    let background = fixture
        .add_group_with_filesystem(31, |_| {
            Arc::new(physical::FaultFileSystem {
                faults: faults.clone(),
            })
        })
        .await?;
    controller::attach_group_appliers(&mut fixture, route.history, background).await?;
    let controllers = controller::start_with_limits(
        &mut fixture,
        CellGroupControllerLimits {
            max_commands: 1,
            ..controller::limits()
        },
    )
    .await?;
    let owner = &controllers[0];
    fixture.runtimes[0].campaign(background).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(background).await?.ready)).await?;
    let bg_route = fixture.cluster.voters[0]
        .log
        .cell_group(&background)?
        .group_manifest()?
        .context("background route")?
        .route()
        .clone();

    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::sync_channel(1);
    *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
        let _ = entered.send(());
        let _ = hold.recv_timeout(Duration::from_secs(15));
    }));
    let image = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(42);")?;
    let bg_cell = history(&bg_route, "background-apply")?;
    let accepted = fixture.runtimes[0]
        .propose(
            background,
            apply::enrollment_bytes(&bg_route, &bg_cell, &image, false)?,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .context("background apply entered physical page sync")??;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let held = pool.usage()?;
    assert_eq!(held.limits.workers, 1);
    assert_eq!(held.worker_capacity, 1);
    assert_eq!(held.apply_retained_operations, 1);
    assert_eq!(held.queued_or_running, 0);
    assert_eq!(owner.usage().retained_commands, 0);

    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        remote_queries::keys(89)?,
        CellDispatchLimits {
            max_cached_routes: 1,
            route_cache_bytes: 8192,
            max_invocations: 4,
            retained_bytes: 128 * 1024,
            max_input_bytes: 1024,
        },
    )?;
    let retry_until_ms = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
        .await?;
    let request = CellCommandRequest {
        id: CommandId::new(ClientId::new([188; 16])?, 1)?,
        contract: class.contract("Add").context("command contract")?,
        input: vec![],
        durability: Durability::Quorum,
        retry_until_ms,
    };
    let options = fixture.cell_options(&cell).await?;
    let routing = |timeout| CellRoutingLimits {
        timeout,
        max_attempts: 1,
    };
    let call = owner.dispatch_command(
        &dispatcher,
        cell.clone(),
        request.clone(),
        routing(Duration::from_secs(if expire { 1 } else { 10 })),
        options.clone(),
    )?;
    controller::wait_for(async || {
        Ok(pool.usage()?.queued_or_running == 1 && owner.usage().retained_commands == 1)
    })
    .await?;
    assert_eq!(calls.load(Ordering::Acquire), 0);
    let mut overflow = request.clone();
    overflow.id = CommandId::new(ClientId::new([188; 16])?, 2)?;
    let refused = owner
        .dispatch_command(
            &dispatcher,
            cell.clone(),
            overflow,
            routing(Duration::from_secs(10)),
            options.clone(),
        )
        .err()
        .context("second pending command exceeded admission")?;
    assert!(!refused.ambiguous);
    assert!(matches!(
        refused.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::Overloaded)
    ));
    let pending = if expire {
        let error = call
            .response()
            .await
            .err()
            .context("queued command deadline")?;
        assert!(matches!(
            error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
        ));
        assert_eq!(pool.usage()?.queued_or_running, 1);
        None
    } else {
        Some(call)
    };
    release.send(())?;
    controller::applied(&fixture, background, accepted.position.index).await?;
    let committed = match pending {
        Some(call) => call.response().await?,
        None => {
            controller::wait_for(async || {
                Ok(pool.usage()?.queued_or_running == 0 && owner.usage().retained_commands == 0)
            })
            .await?;
            assert_eq!(calls.load(Ordering::Acquire), 0);
            let state = fixture.runtimes[0]
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            assert_eq!(state.cell().version.sequence.get(), 1);
            owner
                .dispatch_command(
                    &dispatcher,
                    cell.clone(),
                    request,
                    routing(Duration::from_secs(10)),
                    options,
                )?
                .response()
                .await?
        }
    };
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(committed.durability(), Durability::Quorum);
    let receipt = committed.local_receipt().context("committed SQL receipt")?;
    assert_eq!(receipt.receipt.result(), 7_i64.to_be_bytes());
    controller::applied(&fixture, route.history, receipt.position.index).await?;
    let selected = fixture.runtimes[1]
        .read_authority(
            route.history,
            cell.clone(),
            CellReadConsistency::AtLeast(receipt.version.clone()),
            reads::deadline(&fixture, 5000).await?,
        )
        .await?;
    let read = fixture.appliers[&(1, route.history)]
        .0
        .query_registered(
            fixture.cell_options_at(&cell, 1).await?,
            selected,
            class.clone(),
            class.contract("Get").context("query contract")?,
            vec![],
        )
        .await?;
    assert_eq!(read.result(), 7_i64.to_be_bytes());
    assert_eq!(read.observation(), &receipt.observation());
    drop(committed);
    drop(read);
    controller::wait_for(async || Ok(owner.usage().retained_commands == 0)).await?;
    assert_eq!(pool.usage()?.active_cells, 0);
    for controller in controllers {
        controller.close().await?;
    }
    fixture.close().await
}
