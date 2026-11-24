// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real committed successor stays unpublished when warm reads fail or its
//! owning SQL pool closes. The original preparation is never a serving proof.

use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Clone, Copy)]
pub(crate) enum Check {
    ReadFailure,
    SyncFailure,
    Close,
    Pressure,
}

pub(crate) async fn verify(rooted: bool, check: Check) -> Result<()> {
    let root = tempfile::tempdir()?;
    let pages = root.path().join("node-1/pages");
    let faults = super::super::apply_faults::physical::Faults::new(pages.clone(), None);
    let filesystem: Arc<dyn cellar_ltx::FileSystem> =
        Arc::new(super::super::apply_faults::physical::FaultFileSystem {
            faults: faults.clone(),
        });
    let cluster =
        Cluster::with_storage(RaftRuntimeOptions::default(), root, filesystem.clone()).await?;
    let fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (mut fixture, route, _, _) = enroll_sql_cells(fixture, rooted, filesystem).await?;
    let limits = CellSqlLimits {
        workers: 1,
        queue_per_worker: 4,
        max_pending_cells: 4,
        group_pending_cells: 4,
        node_bytes: if matches!(check, Check::Pressure) {
            200
        } else {
            512
        } * 1024
            * 1024,
        group_bytes: if matches!(check, Check::Pressure) {
            200
        } else {
            512
        } * 1024
            * 1024,
    };
    let cells = [
        history(&route, "prefetch-trigger")?,
        history(&route, "prefetch-a")?,
        history(&route, "prefetch-b")?,
    ];
    for node in 0..3 {
        let pool = CellGroupSqlPool::start(&fixture.cluster.voters[node].log, limits)?;
        super::super::apply::install_with_pool(&mut fixture, &cells[0], node, pool).await?;
    }
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let applier = fixture.appliers[&(0, route.history)].1.clone();
    let mut held_sync = None;
    let checked = std::panic::AssertUnwindSafe(async {
        let mut prepared = Vec::new();
        for (i, cell) in cells[1..].iter().enumerate() {
            prepared.push(initial(&fixture, &pool, cell, 211 + i as u8, 221 + i as u8).await?);
        }
        // Queue the same finite pair behind an entered real engine sync. No
        // timers are stretched and no peer acknowledgement is fabricated.
        let gate = fixture.cluster.voters[0].gate.clone();
        held_sync = Some(gate.arm(fault::HOLD_SYNC));
        let image = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(0);")?;
        let trigger = super::super::apply::enrollment_bytes(&route, &cells[0], &image, rooted)?;
        let first = fixture.runtimes[0].propose(route.history, trigger).await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        let mut queued = Box::pin(futures_util::future::join_all(prepared.iter().map(|command| accepted_sql(&fixture.runtimes[0], command))));
        assert!(tokio::time::timeout(Duration::from_millis(30), queued.as_mut()).await.is_err());
        assert_eq!(fixture.runtimes[0].usage().ordinary.items, 2);
        drop(held_sync.take());
        let responses = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                cellar::asyncrt::select! {
                    replies = queued.as_mut() => break Ok::<_, anyhow::Error>(replies),
                    event = fixture.events[0].recv() => {
                        let event = event.context("leader events")?;
                        let CellGroupEffect::Message(message) = event.effect().map_err(|error| anyhow::anyhow!(error.to_string()))? else { anyhow::bail!("unexpected pre-delivery effect") };
                        fixture.network.push_back((route.history, message.clone()));
                    }
                }
            }
        }).await??;
        drop(queued);
        let mut accepted = vec![first];
        for response in responses { accepted.push(response?); }
        for pair in accepted.windows(2) { assert_eq!(pair[0].position.index.get() + 1, pair[1].position.index.get()); }
        let log = fixture.cluster.voters[0].log.clone();
        let group = log.cell_group(&route.history)?;
        let count = |name: &str| -> Result<u64> { Ok(log.performance_snapshot().counters.iter().find(|counter| counter.name == name).context("prefetch observation")?.value) };
        let mut held = BTreeMap::new();
        let last = accepted.last().context("last accepted")?.position.index;
        let first_work = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                fixture.pump(&[route.history], &mut held).await?;
                let status = fixture.runtimes[0].status(route.history).await?;
                if status.committed >= last && held.contains_key(&route.history) {
                    break Ok::<_, anyhow::Error>(held.remove(&route.history).context("held prefix")?);
                }
                tokio::task::yield_now().await;
            }
        }).await??;
        if matches!(check, Check::SyncFailure) {
            let before = group.group_manifest()?.context("selected root before paired barrier")?;
            let pairs_before = count("cell_apply_prepared_pairs")?;
            let snapshot = log.performance_snapshot();
            let failed_before = snapshot.operations.iter().find(|operation| operation.name == "sql_manifest_prepare").context("manifest phase")?.failed;
            faults.mode.store(super::super::apply_faults::physical::FAIL_DIRECTORY_SYNC, Ordering::Release);
            assert!(pool.prepare_apply(&applier, first_work.clone()).await.is_err());
            assert_eq!(faults.mode.load(Ordering::Acquire), 0, "pair did not reach the real manifest barrier");
            assert_eq!(count("cell_apply_prepared_pairs")?, pairs_before);
            let snapshot = log.performance_snapshot();
            assert_eq!(snapshot.operations.iter().find(|operation| operation.name == "sql_manifest_prepare").context("manifest phase")?.failed - failed_before, 1);
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            assert_eq!(pool.usage()?.apply_retained_operations, 0);
            assert_eq!(pool.usage()?.apply_retained_bytes, 0);
            // A failed persistence barrier quarantines this page owner. It
            // must not issue a preparation or publication proof on retry.
            assert!(pool.prepare_apply(&applier, first_work).await.is_err());
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            return Ok(());
        }
        let barriers = || faults.events.lock().unwrap().iter().filter(|&&event| event == "directory-sync").count();
        let before_barrier = barriers();
        let pairs_before = count("cell_apply_prepared_pairs")?;
        let ready = pool.prepare_apply(&applier, first_work).await?;
        assert_eq!(count("cell_apply_prepared_pairs")? - pairs_before, 1, "committed backlog did not prepare a pair");
        assert_eq!(barriers() - before_barrier, 1, "two committed closures must share one actual directory barrier");
        let mut retained_first = matches!(check, Check::Pressure).then(|| ready.clone());
        ready.complete(&fixture.runtimes[0]).await?;
        let work = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                fixture.pump(&[route.history], &mut held).await?;
                if let Some(work) = held.remove(&route.history) { break Ok::<_, anyhow::Error>(work); }
                tokio::task::yield_now().await;
            }
        }).await??;
        let before = group.group_manifest()?.context("selected root before successor")?;
        assert_eq!(before.position().index.get() + 1, work.ticket.position().index.get());
        assert!(pool.usage()?.apply_retained_operations > 0, "prepared successor was not charged");
        if matches!(check, Check::Close) {
            pool.close().await?;
            assert_eq!(pool.usage()?.running_workers, 0);
            assert_eq!(pool.usage()?.apply_retained_operations, 0);
            assert_eq!(pool.usage()?.apply_retained_bytes, 0);
            assert_eq!(pool.usage()?.completion_reservations, 0);
            assert!(pool.prepare_apply(&applier, work).await.is_err());
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        } else {
            if matches!(check, Check::Pressure) {
                assert!(retained_first.is_some());
                let before = count("cell_apply_prefetch_evictions")?;
                pool.prepare_apply(&applier, work).await?.complete(&fixture.runtimes[0]).await?;
                assert_eq!(count("cell_apply_prefetch_evictions")? - before, 1,
                    "prefetch must yield its charged capacity to the original apply");
                drop(retained_first.take());
            } else {
            let failed = Arc::new(AtomicBool::new(false));
            let observed = failed.clone();
            *faults.read_open.lock().unwrap() = Some(Arc::new(move |path| {
                if path.parent() == Some(pages.as_path()) && !observed.swap(true, Ordering::AcqRel) {
                    Err(std::io::Error::other("injected cached manifest read failure"))
                } else { Ok(()) }
            }));
            let phase_before = log.performance_snapshot();
            assert!(pool.prepare_apply(&applier, work.clone()).await.is_err());
            assert!(failed.load(Ordering::Acquire), "cached result bypassed actual page reads");
            let phase_after = log.performance_snapshot();
            let phase = |snapshot: &RaftPerformanceSnapshot| -> Result<u64> { Ok(snapshot.operations.iter().find(|operation| operation.name == "sql_manifest_recheck").context("cached page recheck")?.failed) };
            assert_eq!(phase(&phase_after)? - phase(&phase_before)?, 1);
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            *faults.read_open.lock().unwrap() = None;
            // Repeating physical preparation of this exact already-committed
            // entry is safe. It neither resubmits a mutation nor changes IDs.
            pool.prepare_apply(&applier, work).await?.complete(&fixture.runtimes[0]).await?;
            }
            fixture.drive(Some(accepted.last().context("last accepted")?.position.index)).await?;
            for ((cell, command), original) in cells[1..].iter().zip(&prepared).zip(&accepted[1..]) {
                for runtime in &fixture.runtimes {
                    let state = runtime.enrollment_state(route.history, cell.clone()).await?;
                    assert_eq!(state.record().context("committed original creation")?.creation(), command.proposal());
                }
                let replay = accepted_sql(&fixture.runtimes[0], command).await?;
                assert_eq!(replay.position, original.position);
                assert_eq!(replay.digest, original.digest);
            }
        }
        drop(prepared);
        Ok::<_, anyhow::Error>(())
    }).catch_unwind().await;
    drop(held_sync.take());
    *faults.read_open.lock().unwrap() = None;
    drop(pool);
    drop(applier);
    let cleanup = fixture.close().await;
    match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
}
