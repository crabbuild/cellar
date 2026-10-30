// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! No fixture event pumping after the production controllers start.
use super::*;
#[path = "raft_ltx_fault.rs"]
#[allow(dead_code)]
mod physical;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn limits() -> CellGroupControllerLimits {
    CellGroupControllerLimits {
        max_groups: 2,
        max_parallel_apply: 2,
        retry_interval_ms: 5,
    }
}

async fn start(fixture: &mut RuntimeCluster) -> Result<Vec<CellGroupController>> {
    fixture.connect_tcp().await?;
    let mut controllers = Vec::new();
    for (node, events) in fixture.events.drain(..).enumerate() {
        let selected: Vec<_> = fixture
            .appliers
            .iter()
            .filter(|((n, _), _)| *n == node)
            .map(|(_, (_, applier))| applier.clone())
            .collect();
        controllers.push(
            CellGroupController::start(
                fixture.runtimes[node].clone(),
                events,
                fixture.transports[node].clone(),
                fixture.appliers[&(node, fixture.cluster.birth.route().history)]
                    .0
                    .clone(),
                selected,
                limits(),
            )
            .await?,
        );
    }
    assert!(fixture.events.is_empty());
    Ok(controllers)
}

async fn wait_for(mut predicate: impl AsyncFnMut() -> Result<bool>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate().await? {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

async fn applied(fixture: &RuntimeCluster, group: GroupHistory, index: LogIndex) -> Result<()> {
    wait_for(async || {
        for runtime in &fixture.runtimes {
            if runtime.status(group).await?.applied.index < index {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
}

pub(crate) async fn controller_replication(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, hold) = std::sync::mpsc::sync_channel(1);
    let hold = Mutex::new(hold);
    let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
        "Add",
        [82; 32],
        move |context, _| {
            count.fetch_add(1, Ordering::SeqCst);
            let _ = entered
                .lock()
                .unwrap()
                .take()
                .context("one callback")?
                .send(());
            hold.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
            context.sql().execute("UPDATE value SET n=n+1", [])?;
            Ok(b"committed".to_vec())
        },
    )?);
    let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = fixture.add_group(31).await?;
    for node in 0..3 {
        let pool = fixture.appliers[&(node, route.history)].0.clone();
        let voter = &mut fixture.extra.get_mut(&second).context("extra")?[node];
        let applier = CellGroupApplier::new(
            voter.log.cell_group(&second)?,
            voter.pages.take().context("pages")?,
            CellGroupApplyImages::Materialized {
                images: voter.images.clone(),
                services: cellar_ltx::generation::ReadServices {
                    now_unix_millis: Arc::new(|| 10),
                    fill_random: Arc::new(|bytes| bytes.fill(17)),
                },
            },
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        fixture.appliers.insert((node, second), (pool, applier));
    }
    let controllers = start(&mut fixture).await?;
    fixture.runtimes[0].campaign(second).await?;
    wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let options = fixture.cell_options(&a).await?;
    let mut attempt = cell_attempt(179)?;
    attempt.request.operation = "Add".into();
    attempt.request.contract_digest = [82; 32];
    attempt.request.request_digest = command_request_digest(b"");
    let caller = pool.clone();
    let preparation = fixture.cluster.domain.spawn(async move {
        caller
            .stage_registered(options, authority, attempt, class, vec![], true)
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    assert_eq!(pool.usage()?.active_cells, 1);
    // The sole SQL worker is physically occupied. Two committed groups must
    // share its one reserved apply slot without losing either retained ticket.
    let mut accepted = Vec::new();
    for (group, rooted) in [(route.history, rooted), (second, false)] {
        let marker = fixture.cluster.voters[0]
            .log
            .cell_group(&group)?
            .group_manifest()?
            .context("group")?;
        let cell = history(marker.route(), "controller-enrolled")?;
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(23);")?;
        accepted.push((
            group,
            fixture.runtimes[0]
                .propose(
                    group,
                    apply::enrollment_bytes(marker.route(), &cell, &bytes, rooted)?,
                )
                .await?,
        ));
    }
    wait_for(async || Ok(controllers[0].usage().apply_retries > 0)).await?;
    assert!(!controllers[0].usage().closing);
    assert!(controllers[0].usage().active_apply <= 2);
    assert!(controllers[0].usage().pending_apply <= 2);
    assert_eq!(pool.usage()?.completion_reservations, 1);
    release.send(())?;
    let CellSqlOutcome::Prepared(prepared) = preparation.await?? else {
        anyhow::bail!("preparation");
    };
    for (group, receipt) in accepted {
        applied(&fixture, group, receipt.position.index).await?;
    }
    pool.mark_submitted(a.clone(), prepared.proposal()).await?;
    let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
    applied(&fixture, route.history, accepted.position.index).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let receipt = pool.finish(authority).await?;
    assert_eq!(receipt.receipt().receipt.result(), b"committed");
    assert_eq!(receipt.receipt().version.history, a);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(receipt);
    drop(prepared);
    for (n, controller) in controllers.iter().enumerate() {
        wait_for(async || Ok(controller.usage().applied >= 3)).await?;
        assert_eq!(controller.usage().groups, 2);
        assert_eq!(fixture.transports[n].usage().peers, 2);
        controller.close().await?;
        assert_eq!(controller.usage().active_apply, 0);
        assert_eq!(controller.usage().pending_apply, 0);
        assert_eq!(
            fixture.appliers[&(n, route.history)]
                .0
                .usage()?
                .running_workers,
            0
        );
    }
    drop(controllers);
    fixture.close().await
}

pub(crate) async fn controller_owners() -> Result<()> {
    for fault in 0..6 {
        let class = RaftClass::new("Counter", 1, [81; 32])?;
        let (mut fixture, route, _, _) = Box::pin(registered::setup(false, &class)).await?;
        fixture.connect_tcp().await?;
        // All three engines share an execution domain: domain equality is not
        // sufficient to authorize any of these borrowed owners.
        let events = fixture.events.remove(if fault == 0 { 1 } else { 0 });
        let transport = fixture.transports[usize::from(fault == 1)].clone();
        let pool = fixture.appliers[&(usize::from(fault == 2), route.history)]
            .0
            .clone();
        let applier = fixture.appliers[&(usize::from(fault == 3), route.history)]
            .1
            .clone();
        let appliers = if fault == 4 {
            vec![applier.clone(), applier]
        } else {
            vec![applier]
        };
        let mut bounds = limits();
        if fault == 5 {
            bounds.max_parallel_apply = 0;
        }
        assert!(matches!(
            CellGroupController::start(
                fixture.runtimes[0].clone(),
                events,
                transport,
                pool,
                appliers,
                bounds
            )
            .await,
            Err(CellGroupControllerError::Invalid(_))
        ));
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn controller_close() -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, a, _) = Box::pin(registered::setup(false, &class)).await?;
    let controllers = start(&mut fixture).await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let options = fixture.cell_options(&a).await?;
    let caller = pool.clone();
    let producer = fixture.cluster.domain.spawn(async move {
        caller
            .stage(
                options,
                authority,
                cell_attempt(179).unwrap(),
                true,
                move |tx| {
                    let _ = entered.send(());
                    wait.recv_timeout(Duration::from_secs(10))?;
                    tx.execute("UPDATE value SET n=n+1", [])?;
                    Ok(vec![])
                },
            )
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    producer.abort();
    assert!(producer.await.is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
            .await
            .is_err()
    );
    assert!(controllers[0].usage().closing);
    assert_eq!(pool.usage()?.running_workers, 1);
    assert!(apply::pool(&fixture, 0).is_err());
    assert!(CellGroupRuntime::start(
        fixture.cluster.voters[0].log.clone(),
        RaftRuntimeOptions::default()
    )
    .await
    .is_err());
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await??;
    assert_eq!(pool.usage()?.running_workers, 0);
    assert_eq!(pool.usage()?.active_cells, 0);
    for controller in &controllers {
        controller.close().await?;
    }
    drop(controllers);
    drop(pool);
    fixture.appliers.clear();
    let replacement = apply::pool(&fixture, 0)?;
    replacement.close().await?;
    drop(replacement);
    fixture.close().await
}

pub(crate) async fn controller_apply_faults(rooted: bool) -> Result<()> {
    for mode in 0..3 {
        let root = tempfile::tempdir()?;
        let faults = physical::Faults::new(root.path().join("node-1/pages"), None);
        let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        let cluster =
            Cluster::with_storage(RaftRuntimeOptions::default(), root, filesystem.clone()).await?;
        let fixture = Box::pin(RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000)).await?;
        let (mut fixture, route, a, _) =
            Box::pin(enroll_sql_cells(fixture, rooted, filesystem)).await?;
        for node in 0..3 {
            apply::install(&mut fixture, &a, node).await?;
        }
        let mut controllers = start(&mut fixture).await?;
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let before = fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("selected")?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::sync_channel(1);
        if mode == 0 {
            faults
                .mode
                .store(physical::FAIL_DIRECTORY_SYNC, Ordering::Release);
        } else {
            *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
                let _ = entered.send(());
                held.recv_timeout(Duration::from_secs(15))
                    .expect("physical apply release");
            }));
        }
        let cell = history(&route, "controller-faulted")?;
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(73);")?;
        fixture.runtimes[0]
            .propose(
                route.history,
                apply::enrollment_bytes(&route, &cell, &bytes, rooted)?,
            )
            .await?;
        if mode == 0 {
            let result =
                tokio::time::timeout(Duration::from_secs(10), controllers[0].wait()).await?;
            assert!(
                matches!(result, Err(CellGroupControllerError::Apply(ref error)) if matches!(error.as_ref(), ExecutionError::Storage(_)))
            );
            assert_eq!(
                faults.mode.load(Ordering::Acquire),
                0,
                "real manifest sync fault consumed"
            );
            assert!(controllers[0].usage().closing);
        } else {
            tokio::time::timeout(Duration::from_secs(5), started).await??;
            assert_eq!(pool.usage()?.apply_retained_operations, 1);
            assert_eq!(controllers[0].usage().active_apply, 1);
            if mode == 1 {
                assert!(
                    tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
                        .await
                        .is_err()
                );
            } else {
                // The worker cannot retain a public handle and prevent its own
                // final-handle drop from initiating physical shutdown.
                drop(controllers.remove(0));
                wait_for(async || Ok(fixture.transports[0].usage().tasks == 0)).await?;
            }
            assert!(apply::pool(&fixture, 0).is_err());
            assert!(CellGroupRuntime::start(
                fixture.cluster.voters[0].log.clone(),
                RaftRuntimeOptions::default()
            )
            .await
            .is_err());
            assert_eq!(pool.usage()?.running_workers, 1);
            assert_eq!(pool.usage()?.apply_retained_operations, 1);
            release.send(())?;
            if mode == 1 {
                tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await??;
            } else {
                wait_for(async || {
                    Ok(matches!(
                        fixture.runtimes[0].status(route.history).await,
                        Err(RuntimeError::Closed)
                    ))
                })
                .await?;
            }
        }
        assert_eq!(pool.usage()?.running_workers, 0);
        // A failed or abandoned preparation must not advance engine authority.
        assert_eq!(
            fixture.cluster.voters[0]
                .log
                .cell_group(&route.history)?
                .group_manifest()?
                .as_ref(),
            Some(&before)
        );
        if mode < 2 {
            drop(controllers.remove(0));
        }
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}
