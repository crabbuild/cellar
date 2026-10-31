// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical preparation ownership and refusal at real image/manifest barriers.
use super::*;
use std::sync::atomic::Ordering;

#[path = "raft_ltx_fault.rs"]
#[allow(dead_code)]
pub(super) mod physical;

async fn pending(
    rooted: bool,
    manifest: bool,
) -> Result<(
    RuntimeCluster,
    Arc<physical::Faults>,
    CellGroupApply,
    CellHistory,
)> {
    let root = tempfile::tempdir()?;
    let directory = if manifest {
        root.path().join("node-1/pages")
    } else if rooted {
        root.path().join("rooted-0")
    } else {
        root.path().join("node-1/images")
    };
    let faults = physical::Faults::new(directory, None);
    let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(physical::FaultFileSystem {
        faults: faults.clone(),
    });
    let cluster =
        Cluster::with_storage(RaftRuntimeOptions::default(), root, filesystem.clone()).await?;
    let fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (mut fixture, route, a, _) = enroll_sql_cells(fixture, rooted, filesystem).await?;
    apply::install(&mut fixture, &a, 0).await?;
    let cell = history(&route, "faulted-apply")?;
    // Raw replicated enrollment ensures no leader SQL preview has already
    // prepared this result, including in the content-addressed extent store.
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(11);")?;
    fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &cell, &bytes, rooted)?,
        )
        .await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture.pump(&[route.history], &mut held).await?;
            if held.contains_key(&route.history) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok((
        fixture,
        faults,
        held.remove(&route.history).context("committed work")?,
        cell,
    ))
}

pub(crate) async fn cancellation(rooted: bool) -> Result<()> {
    for manifest in [false, true] {
        let (mut fixture, faults, work, cell) = Box::pin(pending(rooted, manifest)).await?;
        let history = fixture.cluster.birth.route().history;
        let before = work.base.marker().clone();
        let position = work.ticket.position();
        let (pool, applier) = fixture.appliers.remove(&(0, history)).context("applier")?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(10))
                .expect("release physical apply barrier");
        }));
        let caller = pool.clone();
        let worker = applier.clone();
        let input = work.clone();
        let waiter = fixture
            .cluster
            .domain
            .spawn(async move { caller.prepare_apply(&worker, input).await })?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        waiter.abort();
        assert!(waiter.await.is_err());
        assert_eq!(pool.usage()?.apply_retained_operations, 1);
        assert!(pool.usage()?.apply_retained_bytes > 0);
        assert_eq!(pool.usage()?.completion_reservations, 1);
        assert_eq!(
            fixture.cluster.voters[0]
                .log
                .cell_group(&history)?
                .group_manifest()?
                .as_ref(),
            Some(&before)
        );
        // Cancel a close waiter too. Cleanup still owns the entered worker and
        // the exclusive SQL-pool lease until the physical barrier is released.
        assert!(
            tokio::time::timeout(Duration::from_millis(30), pool.close())
                .await
                .is_err()
        );
        assert!(apply::pool(&fixture, 0).is_err());
        assert_eq!(pool.usage()?.running_workers, 1);
        assert_eq!(pool.usage()?.apply_retained_operations, 1);
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(5), pool.close()).await??;
        let usage = pool.usage()?;
        assert_eq!(usage.running_workers, 0);
        assert_eq!(usage.apply_retained_operations, 0);
        assert_eq!(usage.apply_retained_bytes, 0);
        assert_eq!(usage.completion_reservations, 0);
        assert_eq!(
            fixture.cluster.voters[0]
                .log
                .cell_group(&history)?
                .group_manifest()?
                .as_ref(),
            Some(&before)
        );
        drop(pool);
        // Shutdown did not publish a result or settle this committed entry.
        // A fresh pool can repeat exact preparation, then the owner publishes it.
        let replacement = apply::pool(&fixture, 0)?;
        replacement
            .prepare_apply(&applier, work)
            .await?
            .complete(&fixture.runtimes[0])
            .await?;
        fixture.drive(Some(position.index)).await?;
        for runtime in &fixture.runtimes {
            let state = runtime
                .reconciliation_authority(history, cell.clone())
                .await?;
            assert_eq!(state.cell().version.sequence.get(), 1);
            assert_eq!(state.cell().witness.position, position);
        }
        replacement.close().await?;
        drop(replacement);
        drop(applier);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn failure(rooted: bool) -> Result<()> {
    for manifest in [false, true] {
        let (fixture, faults, work, _) = Box::pin(pending(rooted, manifest)).await?;
        let history = fixture.cluster.birth.route().history;
        let before = work.base.marker().clone();
        let (pool, applier) = &fixture.appliers[&(0, history)];
        faults
            .mode
            .store(physical::FAIL_DIRECTORY_SYNC, Ordering::Release);
        assert!(pool.prepare_apply(applier, work).await.is_err());
        assert_eq!(
            faults.mode.load(Ordering::Acquire),
            0,
            "fault must reach actual preparation I/O"
        );
        assert_eq!(pool.usage()?.apply_retained_operations, 0);
        assert_eq!(pool.usage()?.apply_retained_bytes, 0);
        assert_eq!(pool.usage()?.completion_reservations, 0);
        assert_eq!(
            fixture.cluster.voters[0]
                .log
                .cell_group(&history)?
                .group_manifest()?
                .as_ref(),
            Some(&before)
        );
        let status = fixture.runtimes[0].status(history).await?;
        assert_eq!(status.applied, before.position());
        assert!(status.committed > before.position().index);
        fixture.close().await?;
    }
    Ok(())
}
