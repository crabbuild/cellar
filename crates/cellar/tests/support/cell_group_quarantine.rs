// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Faults cross real manifest/engine barriers; healthy groups share the same node.
use super::apply_faults::physical;
use super::remote_queries::{keys, wait_for};
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn group_quarantine_retains_producer_and_serves_healthy_group() -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let faults = physical::Faults::new(root.path().join("node-1/pages"), None);
        let filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(
            RaftClass::new("Counter", 1, [81; 32])?
                .initializer([83; 32], |_, tx| {
                    tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                    Ok(())
                })?
                .command("Add", [82; 32], move |ctx, input| {
                    count.fetch_add(1, Ordering::SeqCst);
                    ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                    Ok(input.to_vec())
                })?,
        );
        let (mut f, route, a, _) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        // One retained producer must consume its group quota, not every node
        // slot. Keep the same one physical SQL worker and bounded queue.
        for n in 0..3 {
            let (old, applier) = f.appliers.remove(&(n, route.history)).context("old pool")?;
            old.close().await?;
            drop(old);
            let pool = CellGroupSqlPool::start(
                &f.cluster.voters[n].log,
                CellSqlLimits {
                    workers: 1,
                    queue_per_worker: 1,
                    max_pending_cells: 4,
                    group_pending_cells: 1,
                    node_bytes: 1024 * 1024 * 1024,
                    group_bytes: 512 * 1024 * 1024,
                },
            )?;
            f.appliers.insert((n, route.history), (pool, applier));
        }
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let mut second = registration::prepare(&f, 31, &a).await?;
        let other = second.birth.route().history;
        for (n, input) in std::mem::take(&mut second.inputs).into_iter().enumerate() {
            controllers[n].register(input)?.response().await?;
        }
        let healthy_dispatch = registration::dispatcher(&f, &second, class.clone())?;
        let healthy_cell = history(second.birth.route(), "healthy-after-quarantine")?;
        f.runtimes[0].campaign(other).await?;
        wait_for(async || Ok(f.runtimes[0].status(other).await?.ready)).await?;
        let before = f.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("before")?;
        let captured = f.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?;
        let dispatcher = CellGroupDispatcher::new(
            &f.cluster.domain,
            f.cluster.directory.clone(),
            class.clone(),
            keys(89)?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        let retry = f
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
            .await?;
        faults
            .mode
            .store(physical::FAIL_DIRECTORY_SYNC, Ordering::Release);
        let call = controllers[0].command(
            controller_commands::invocation(
                &dispatcher,
                &class,
                &a,
                Duration::from_secs(15),
                retry,
            )
            .await?,
            f.cell_options(&a).await?,
        )?;
        let error = call.response().await.err().context("quarantined command")?;
        assert!(error.ambiguous);
        assert!(matches!(
            error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::GroupQuarantined)
        ));
        wait_for(async || {
            Ok(controllers[0].usage().quarantined_groups == 1
                && controllers[0].usage().retained_commands == 1)
        })
        .await?;
        assert_eq!(faults.mode.load(Ordering::Acquire), 0);
        assert!(matches!(
            controllers[0].group_failure(route.history)?,
            Some(CellGroupControllerError::Apply(_))
        ));
        assert!(!controllers[0].usage().closing);
        assert_eq!(controllers[0].usage().groups, 1);
        assert!(
            captured.check().is_err(),
            "captured leader authority revoked"
        );
        assert!(matches!(
            f.runtimes[0].campaign(route.history).await,
            Err(RuntimeError::GroupQuarantined)
        ));
        assert!(matches!(
            f.runtimes[0]
                .read_authority(
                    route.history,
                    a.clone(),
                    CellReadConsistency::Stale,
                    f.cluster
                        .domain
                        .run(async { cellar::asyncrt::mono_ms() + 5000 })
                        .await?
                )
                .await,
            Err(RuntimeError::GroupQuarantined)
        ));
        let pool = f.appliers[&(0, route.history)].0.clone();
        assert_eq!(pool.usage()?.active_cells, 1, "ambiguous producer retained");
        assert!(CellGroupRuntime::start(
            f.cluster.voters[0].log.clone(),
            RaftRuntimeOptions::default()
        )
        .await
        .is_err());
        // A refused retry must not replay the callback or settle the original producer.
        let refused = controllers[0]
            .command(
                controller_commands::invocation(
                    &dispatcher,
                    &class,
                    &a,
                    Duration::from_secs(15),
                    retry,
                )
                .await?,
                f.cell_options(&a).await?,
            )?
            .response()
            .await
            .err()
            .context("quarantine refusal")?;
        assert!(!refused.ambiguous);
        assert!(matches!(
            refused.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::GroupQuarantined)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        registration::enroll(&f, &controllers[0], &healthy_dispatch, &healthy_cell).await?;
        registration::mutate(
            &f,
            &controllers[0],
            &healthy_dispatch,
            &class,
            &healthy_cell,
            retry,
        )
        .await?;
        registration::mutate(
            &f,
            &controllers[0],
            &healthy_dispatch,
            &class,
            &healthy_cell,
            retry,
        )
        .await?;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "healthy retry returns its receipt without replay"
        );
        let deadline = f
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 5000 })
            .await?;
        let authority = f.runtimes[0]
            .read_authority(
                other,
                healthy_cell.clone(),
                CellReadConsistency::Linearizable,
                deadline,
            )
            .await?;
        let result = pool
            .query(
                f.cell_options(&healthy_cell).await?,
                authority,
                |db, version| {
                    assert_eq!(version.sequence.get(), 2);
                    let value: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                    Ok(value.to_le_bytes().to_vec())
                },
            )
            .await?;
        assert_eq!(result.result(), &8_i64.to_le_bytes());
        drop(result);
        let mut third = registration::prepare(&f, 35, &a).await?;
        let refused = controllers[0]
            .register(third.inputs.remove(0))
            .err()
            .context("quarantine retains group capacity")?;
        assert!(matches!(
            refused.error,
            CellGroupControllerError::Runtime(RuntimeError::Overloaded)
        ));
        drop(refused);
        drop(third);
        assert_eq!(
            f.cluster.voters[0]
                .log
                .cell_group(&route.history)?
                .group_manifest()?
                .as_ref(),
            Some(&before)
        );
        assert_eq!(controllers[0].usage().retained_commands, 1);
        assert_eq!(pool.usage()?.active_cells, 1);
        assert_eq!(f.transports[0].usage().peers, 2);
        for controller in &controllers {
            controller.close().await?;
        }
        assert_eq!(controllers[0].usage().retained_commands, 0);
        assert_eq!(pool.usage()?.active_cells, 0);
        assert_eq!(dispatcher.usage().1, 0);
        drop(captured);
        drop(controllers);
        f.close().await?;
    }
    Ok(())
}

pub(crate) async fn shared_engine_sync_failure_still_stops_all_groups() -> Result<()> {
    for rooted in [false, true] {
        let class = RaftClass::new("Counter", 1, [81; 32])?;
        let (mut f, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let other = controller::add_second(&mut f, route.history).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        f.runtimes[0].campaign(other).await?;
        wait_for(async || Ok(f.runtimes[0].status(other).await?.ready)).await?;
        let gate = f.cluster.voters[0].gate.clone();
        let release = gate.arm(fault::FAIL_SYNC);
        let cell = history(&route, "engine-failure")?;
        let image = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(91);")?;
        let _ = f.runtimes[0]
            .propose(
                route.history,
                apply::enrollment_bytes(&route, &cell, &image, rooted)?,
            )
            .await;
        assert!(
            tokio::time::timeout(Duration::from_secs(10), controllers[0].wait())
                .await?
                .is_err()
        );
        assert_eq!(
            gate.mode.load(Ordering::Acquire),
            fault::OFF,
            "real log sync fault consumed"
        );
        assert!(controllers[0].usage().closing);
        for group in [route.history, other] {
            assert!(matches!(
                f.runtimes[0].status(group).await,
                Err(RuntimeError::Closed)
            ));
            assert!(f.runtimes[0].campaign(group).await.is_err());
        }
        assert!(CellGroupRuntime::start(
            f.cluster.voters[0].log.clone(),
            RaftRuntimeOptions::default()
        )
        .await
        .is_err());
        drop(release);
        for controller in &controllers[1..] {
            controller.close().await?;
        }
        for (pool, _) in f.appliers.values() {
            pool.close().await?;
        }
        f.appliers.clear();
        assert!(f.runtimes[0].close().await.is_err());
        for runtime in &f.runtimes[1..] {
            runtime.close().await?;
        }
        f.runtimes.clear();
        drop(controllers);
        assert!(f.cluster.voters[0].log.close().await.is_err());
        for voter in &f.cluster.voters[1..] {
            voter.log.close().await?;
        }
        f.cluster.domain.shutdown().await;
    }
    Ok(())
}
