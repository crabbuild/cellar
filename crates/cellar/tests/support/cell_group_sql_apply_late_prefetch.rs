// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Commit a real successor after physical image work has already entered.
//! Neither a future admission hint nor an awaiting client is a publication proof.

use super::*;
use std::sync::{atomic::AtomicBool, Mutex};

pub(crate) async fn verify(rooted: bool, cancel: bool) -> Result<()> {
    let root = tempfile::tempdir()?;
    let pages = root.path().join("node-1/pages");
    let directory = if rooted {
        root.path().join("rooted-0")
    } else {
        root.path().join("node-1/images")
    };
    let faults = super::super::apply_faults::physical::Faults::new(directory, None);
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
        node_bytes: 512 * 1024 * 1024,
        group_bytes: 512 * 1024 * 1024,
    };
    let cells = [
        history(&route, "late-trigger")?,
        history(&route, "late-a")?,
        history(&route, "late-b")?,
    ];
    for node in 0..3 {
        let pool = CellGroupSqlPool::start(&fixture.cluster.voters[node].log, limits)?;
        super::super::apply::install_with_pool(&mut fixture, &cells[0], node, pool).await?;
    }
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let applier = fixture.appliers[&(0, route.history)].1.clone();
    let mut image_release = None;
    let mut page_release = None;
    let mut waiter = None;
    let checked = std::panic::AssertUnwindSafe(async {
        // Prepare both producers before arming the image barrier, but do not
        // submit either one until the first physical apply has entered.
        let mut prepared = Vec::new();
        for (index, cell) in cells[1..].iter().enumerate() {
            prepared.push(initial(&fixture, &pool, cell, 241 + index as u8, 251 + index as u8).await?);
        }
        let image = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(9876543);")?;
        let first = fixture.runtimes[0].propose(route.history,
            super::super::apply::enrollment_bytes(&route, &cells[0], &image, rooted)?).await?;
        let mut held = BTreeMap::new();
        let work = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                fixture.pump(&[route.history], &mut held).await?;
                if let Some(work) = held.remove(&route.history) { break Ok::<_, anyhow::Error>(work); }
                tokio::task::yield_now().await;
            }
        }).await??;
        assert_eq!(work.ticket.position(), first.position);
        assert_eq!(fixture.runtimes[0].status(route.history).await?.committed, first.position.index,
            "the successor must not exist at initial apply admission");
        let log = fixture.cluster.voters[0].log.clone();
        let group = log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("selected root before late work")?;
        let pairs = || -> Result<u64> { Ok(log.performance_snapshot().counters.iter().find(|counter| counter.name == "cell_apply_prepared_pairs").context("pair observation")?.value) };
        let pair_before = pairs()?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        image_release = Some(release);
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(10)).expect("release entered image barrier");
        }));
        let caller = pool.clone();
        let worker = applier.clone();
        waiter = Some(fixture.cluster.domain.spawn(async move { caller.prepare_apply(&worker, work).await })?);
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        assert_eq!(pool.usage()?.apply_retained_operations, 1);
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));

        let leader = fixture.runtimes[0].clone();
        let mut pending = Box::pin(futures_util::future::join_all(prepared.iter().map(|command| accepted_sql(&leader, command))));
        let replies = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                cellar::asyncrt::select! {
                    replies = pending.as_mut() => break Ok::<_, anyhow::Error>(replies),
                    event = fixture.events[0].recv() => {
                        let event = event.context("leader events while image is entered")?;
                        let CellGroupEffect::Message(message) = event.effect().map_err(|error| anyhow::anyhow!(error.to_string()))? else { anyhow::bail!("unexpected publication while image is held") };
                        fixture.network.push_back((route.history, message.clone()));
                    }
                }
            }
        }).await??;
        drop(pending);
        let mut accepted = vec![first];
        for reply in replies { accepted.push(reply?); }
        for pair in accepted.windows(2) { assert_eq!(pair[0].position.index.get() + 1, pair[1].position.index.get()); }
        let last = accepted.last().context("last late proposal")?.position.index;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                fixture.pump(&[route.history], &mut held).await?;
                if fixture.runtimes[0].status(route.history).await?.committed >= last { break Ok::<_, anyhow::Error>(()); }
                tokio::task::yield_now().await;
            }
        }).await??;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));

        let late_started = if cancel {
            let (entered, started) = tokio::sync::oneshot::channel();
            let entered = Mutex::new(Some(entered));
            let (release, wait) = std::sync::mpsc::sync_channel(1);
            page_release = Some(release);
            let wait = Mutex::new(wait);
            let observed = Arc::new(AtomicBool::new(false));
            let caller = pool.clone();
            *faults.read_open.lock().unwrap() = Some(Arc::new(move |path| {
                if path.parent() == Some(pages.as_path())
                    && caller.usage().map_err(|error| std::io::Error::other(error.to_string()))?.apply_retained_operations == 2
                    && !observed.swap(true, Ordering::AcqRel)
                {
                    if let Some(entered) = entered.lock().unwrap().take() { let _ = entered.send(()); }
                    wait.lock().unwrap().recv_timeout(Duration::from_secs(10))
                        .map_err(|_| std::io::Error::other("release entered late manifest read"))?;
                }
                Ok(())
            }));
            Some(started)
        } else { None };
        image_release.take().context("image release")?.send(())?;
        if let Some(started) = late_started {
            tokio::time::timeout(Duration::from_secs(5), started).await??;
            let task = waiter.take().context("entered apply waiter")?;
            task.abort();
            assert!(task.await.is_err());
            assert_eq!(pool.usage()?.apply_retained_operations, 2,
                "late reservations must survive cancellation until physical join");
            assert!(tokio::time::timeout(Duration::from_millis(30), pool.close()).await.is_err());
            assert_eq!(pool.usage()?.running_workers, 1);
            assert_eq!(pool.usage()?.apply_retained_operations, 2);
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            *faults.read_open.lock().unwrap() = None;
            page_release.take().context("late read release")?.send(())?;
            tokio::time::timeout(Duration::from_secs(5), pool.close()).await??;
            let usage = pool.usage()?;
            assert_eq!(usage.running_workers, 0);
            assert_eq!(usage.apply_retained_operations, 0);
            assert_eq!(usage.apply_retained_bytes, 0);
            assert_eq!(usage.completion_reservations, 0);
            assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
            return Ok::<_, anyhow::Error>(None);
        }
        let ready = tokio::time::timeout(Duration::from_secs(5), waiter.take().context("apply waiter")?).await???;
        let paired = pairs()? - pair_before;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        ready.complete(&fixture.runtimes[0]).await?;
        fixture.drive(Some(last)).await?;
        for ((cell, command), original) in cells[1..].iter().zip(&prepared).zip(&accepted[1..]) {
            for runtime in &fixture.runtimes {
                assert_eq!(runtime.enrollment_state(route.history, cell.clone()).await?.record().context("original late creation")?.creation(), command.proposal());
            }
            let replay = accepted_sql(&fixture.runtimes[0], command).await?;
            assert_eq!(replay.position, original.position);
            assert_eq!(replay.digest, original.digest);
        }
        Ok(Some(paired))
    }).catch_unwind().await;
    if let Some(release) = image_release.take() {
        let _ = release.send(());
    }
    if let Some(release) = page_release.take() {
        let _ = release.send(());
    }
    *faults.read_open.lock().unwrap() = None;
    *faults.before_directory_sync.lock().unwrap() = None;
    if let Some(task) = waiter.take() {
        task.abort();
        let _ = task.await;
    }
    drop(pool);
    drop(applier);
    let cleanup = fixture.close().await;
    let paired = match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    cleanup?;
    if let Some(paired) = paired {
        assert_eq!(
            paired, 1,
            "a successor committed during image preparation must share the first manifest barrier"
        );
    }
    Ok(())
}

/// Locally durable bytes are insufficient for speculative prefix preparation.
pub(crate) async fn uncommitted(rooted: bool) -> Result<()> {
    use raft::Storage as _;
    let (mut fixture, faults, work, _) =
        Box::pin(super::super::apply_faults::pending(rooted, false)).await?;
    let route = fixture.cluster.birth.route().clone();
    let first = work.ticket.position();
    let before = work.base.marker().clone();
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let applier = fixture.appliers[&(0, route.history)].1.clone();
    let log = fixture.cluster.voters[0].log.clone();
    let group = log.cell_group(&route.history)?;
    let mut image_release = None;
    let mut waiter = None;
    let checked = std::panic::AssertUnwindSafe(async {
        let pairs = || -> Result<u64> {
            Ok(log
                .performance_snapshot()
                .counters
                .iter()
                .find(|counter| counter.name == "cell_apply_prepared_pairs")
                .context("uncommitted pair observation")?
                .value)
        };
        let pair_before = pairs()?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::sync_channel(1);
        image_release = Some(release);
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            wait.recv_timeout(Duration::from_secs(10))
                .expect("release uncommitted-prefix image barrier");
        }));
        let caller = pool.clone();
        let worker = applier.clone();
        waiter = Some(
            fixture
                .cluster
                .domain
                .spawn(async move { caller.prepare_apply(&worker, work).await })?,
        );
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        let cell = history(&route, "uncommitted-successor")?;
        let image = images::image(
            None,
            "CREATE TABLE value(n); INSERT INTO value VALUES(7654321);",
        )?;
        let bytes = super::super::apply::enrollment_bytes(&route, &cell, &image, rooted)?;
        let creation = CellGroupEntry::decode(&bytes, &route, GroupEntryLimits::default())?
            .binding
            .proposal;
        let accepted = fixture.runtimes[0].propose(route.history, bytes).await?;
        assert_eq!(accepted.position.index.get(), first.index.get() + 1);
        // Drain outgoing frames to avoid output pressure, but withhold every
        // peer delivery until the first root has been selected on its own.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Ok(event) = fixture.events[0].try_recv() {
                    let CellGroupEffect::Message(message) = event
                        .effect()
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?
                    else {
                        anyhow::bail!("unexpected publication while uncommitted input is held")
                    };
                    fixture.network.push_back((route.history, message.clone()));
                }
                if group.last_index()? >= accepted.position.index.get() {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        assert_eq!(
            fixture.runtimes[0].status(route.history).await?.committed,
            first.index
        );
        image_release
            .take()
            .context("uncommitted image release")?
            .send(())?;
        let ready = tokio::time::timeout(
            Duration::from_secs(5),
            waiter.take().context("uncommitted apply waiter")?,
        )
        .await???;
        assert_eq!(
            pairs()?,
            pair_before,
            "locally durable uncommitted input must not be prefetched"
        );
        assert_eq!(pool.usage()?.apply_retained_operations, 1);
        assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
        ready.complete(&fixture.runtimes[0]).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.runtimes[0]
                .status(route.history)
                .await?
                .applied
                .index
                < first.index
            {
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        assert_eq!(
            fixture.runtimes[0].status(route.history).await?.committed,
            first.index
        );
        assert_eq!(
            group
                .group_manifest()?
                .context("selected first root")?
                .position(),
            first
        );
        // Heal the actual partition and finish the same original proposal.
        fixture.drive(Some(accepted.position.index)).await?;
        for runtime in &fixture.runtimes {
            let state = runtime
                .enrollment_state(route.history, cell.clone())
                .await?;
            assert_eq!(
                state
                    .record()
                    .context("original uncommitted creation")?
                    .creation(),
                creation
            );
            assert_eq!(
                runtime
                    .reconciliation_authority(route.history, cell.clone())
                    .await?
                    .cell()
                    .witness
                    .position,
                accepted.position
            );
        }
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    if let Some(release) = image_release.take() {
        let _ = release.send(());
    }
    *faults.before_directory_sync.lock().unwrap() = None;
    if let Some(task) = waiter.take() {
        task.abort();
        let _ = task.await;
    }
    drop(pool);
    drop(applier);
    let cleanup = fixture.close().await;
    match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    cleanup
}
