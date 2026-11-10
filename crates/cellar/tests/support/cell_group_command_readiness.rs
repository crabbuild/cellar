// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A committed shared-group entry can temporarily precede physical apply.
use super::*;

struct Release(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

pub(crate) async fn run(expire: bool) -> Result<()> {
    for rooted in [false, true] {
        let root = tempfile::tempdir()?;
        let faults = physical::Faults::new(root.path().join("node-1/pages"), None);
        let filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |context, _| {
                counted.fetch_add(1, Ordering::AcqRel);
                context.sql().execute("UPDATE value SET n=n+7", [])?;
                Ok(7_i64.to_be_bytes().to_vec())
            },
        )?);
        let (mut fixture, route, cell, _) =
            Box::pin(registered::setup_in(rooted, &class, root, filesystem)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
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
        let limits = |timeout| CellRoutingLimits {
            timeout,
            max_attempts: 1,
        };
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        assert_eq!(pool.usage()?.active_cells, 0);

        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let release = Release(Some(release));
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            let _ = held.recv_timeout(Duration::from_secs(15));
        }));
        // The fixture's byte-only protocol entry is still open: no prepared
        // command has been submitted. This unrelated enrollment has no private
        // SQL producer/result, so the unchanged one-cell budget stays available
        // for the application command after physical apply completes.
        let other = history(&route, "committed-before-applied")?;
        let image = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(42);")?;
        let accepted = fixture.runtimes[0]
            .propose(
                route.history,
                apply::enrollment_bytes(&route, &other, &image, rooted)?,
            )
            .await?;
        let result: Result<()> =
            async {
                tokio::time::timeout(Duration::from_secs(5), started)
                    .await
                    .context("actual manifest sync entered")??;
                let status = fixture.runtimes[0].status(route.history).await?;
                assert!(
                    status.committed >= accepted.position.index
                        && status.applied.index < status.committed
                );
                assert!(matches!(
                    fixture.runtimes[0]
                        .execution_authority(route.history, cell.clone())
                        .await,
                    Err(RuntimeError::NotReady)
                ));
                let call = controllers[0].dispatch_command(
                    &dispatcher,
                    cell.clone(),
                    request.clone(),
                    limits(Duration::from_millis(if expire { 200 } else { 10_000 })),
                    options.clone(),
                )?;
                let committed = if expire {
                    let failure = call
                        .response()
                        .await
                        .err()
                        .context("original readiness deadline")?;
                    anyhow::ensure!(
                        matches!(
                            failure.cause.as_ref(),
                            CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                        ),
                        "{failure:?}"
                    );
                    wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
                    anyhow::ensure!(
                        calls.load(Ordering::Acquire) == 0,
                        "expired readiness wait entered SQL"
                    );
                    drop(release);
                    controller::applied(&fixture, route.history, accepted.position.index).await?;
                    assert_eq!(
                        calls.load(Ordering::Acquire),
                        0,
                        "expired invocation executed after apply"
                    );
                    // A new client invocation retries the same identity, payload
                    // and retry horizon. The expired invocation never resets its
                    // deadline or survives as a late SQL producer.
                    controllers[0]
                        .dispatch_command(
                            &dispatcher,
                            cell.clone(),
                            request.clone(),
                            limits(Duration::from_secs(10)),
                            options.clone(),
                        )?
                        .response()
                        .await?
                } else {
                    let mut response = Box::pin(call.response());
                    let premature =
                        tokio::time::timeout(Duration::from_millis(50), &mut response).await;
                    anyhow::ensure!(
                        premature.is_err(),
                        "readiness returned before apply: {:?}",
                        premature.map(|result| result.map(|_| ()))
                    );
                    anyhow::ensure!(calls.load(Ordering::Acquire) == 0);
                    drop(release);
                    response.await?
                };
                assert_eq!(calls.load(Ordering::Acquire), 1);
                assert_eq!(committed.durability(), Durability::Quorum);
                let receipt = committed.local_receipt().context("committed receipt")?;
                assert_eq!(receipt.receipt.result(), 7_i64.to_be_bytes());
                assert_eq!(receipt.version.history, cell);
                assert_eq!(receipt.version.sequence.get(), 2);
                let expected_version = receipt.version.clone();
                let expected_position = receipt.position;
                drop(committed);
                wait_for(async || {
                    Ok(pool.usage()?.active_cells == 0
                        && controllers[0].usage().retained_commands == 0)
                })
                .await?;
                let replay = controllers[0]
                    .dispatch_command(
                        &dispatcher,
                        cell.clone(),
                        request.clone(),
                        limits(Duration::from_secs(10)),
                        options.clone(),
                    )?
                    .response()
                    .await?;
                let receipt = replay.local_receipt().context("replayed receipt")?;
                assert_eq!(receipt.receipt.result(), 7_i64.to_be_bytes());
                assert_eq!(receipt.version, expected_version);
                assert_eq!(receipt.position, expected_position);
                assert_eq!(calls.load(Ordering::Acquire), 1, "receipt retry reran SQL");
                drop(replay);
                Ok(())
            }
            .await;
        // The async block owns the release sender. All return paths release the
        // physical apply gate before joining actual controller and SQL owners.
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
        result
            .with_context(|| format!("execution readiness, extents={rooted}, expire={expire}"))?;
    }
    Ok(())
}
