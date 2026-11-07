// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real completed images fill the runtime's reserved admission before polling
//! its worker. A separately captured checkpoint must wait without losing pins.
use super::*;
use futures_util::FutureExt;
use raft::Storage;

pub(crate) async fn checkpoint_completion_pressure(rooted: bool) -> Result<()> {
    // A current-thread owner makes the admission burst deterministic even when
    // this fixture is composed inside a multi-thread integration test. Both
    // execution domains and the nested runtime join before this call returns.
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .blocking(move || -> Result<()> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(Box::pin(run(rooted)))
        })?
        .await;
    domain.shutdown().await;
    result?
}

async fn run(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut f, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let slots = f.runtimes[0].usage().completions.max_items;
    assert_eq!(slots, 8, "qualify the actual fixed completion capacity");
    let mut groups = vec![route.history];
    for n in 0..slots {
        groups.push(f.add_group(100 + n as u8).await?);
    }
    f.connect_tcp().await?;
    for &group in &groups[1..] {
        f.runtimes[0].campaign(group).await?;
    }
    f.settle(&groups).await?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(71);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let mut targets = BTreeMap::new();
    let mut cells = Vec::new();
    for &group in &groups[1..] {
        let marker = f.cluster.voters[0]
            .log
            .cell_group(&group)?
            .group_manifest()?
            .context("completed-image group")?;
        let cell = history(marker.route(), "completed-before-checkpoint")?;
        let accepted = f.runtimes[0]
            .propose(group, request(marker.route(), cell.clone(), 1, &cut)?)
            .await?;
        targets.insert(group, accepted.position);
        cells.push((group, cell));
    }
    let successor_cell = history(&route, "after-capacity-checkpoint")?;
    let successor = f.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &successor_cell, &bytes, rooted)?,
        )
        .await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(15), async {
        while held.len() != groups.len() {
            f.pump(&groups, &mut held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let work = held
        .remove(&route.history)
        .context("checkpoint successor")?;
    let before = work.base.marker().clone();
    let group = f.cluster.voters[0].log.cell_group(&route.history)?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let source = pool.clone();
    let log = group.clone();
    let root = work.base.clone();
    let checkpoint = f
        .cluster
        .domain
        .blocking(move || source.capture(&log, &root)?.prepare_publication())?
        .await??;
    let charged = pool.usage();
    assert_eq!(charged.sources, 1);
    assert!(charged.retained_bytes > 0);
    let mut prepared = Vec::new();
    for &group in &groups[1..] {
        let work = held.remove(&group).context("completed-image ticket")?;
        let mut voters = f.extra.remove(&group).context("physical image owners")?;
        let (voters, work, next) = f
            .cluster
            .domain
            .blocking(move || -> Result<_> {
                let next = prepare(&mut voters[0], &work)?;
                Ok((voters, work, next))
            })?
            .await??;
        f.extra.insert(group, voters);
        prepared.push((group, work, next));
    }
    let runtime = f.runtimes[0].clone();
    let mut pending = prepared
        .into_iter()
        .map(|(group, work, next)| runtime.complete_apply(group, work, next).boxed())
        .collect::<Vec<_>>();
    // No owner turn occurs between these polls. All eight real completions are
    // published to the mailbox, retaining their exact fsynced image/ticket.
    for call in &mut pending {
        assert!(call.as_mut().now_or_never().is_none());
    }
    let usage = runtime.usage().completions;
    assert_eq!(usage.items, usage.max_items);
    assert!(usage.bytes > 0 && usage.bytes <= usage.max_bytes);
    pending.push(
        runtime
            .checkpoint_before_apply(route.history, work.ticket.clone(), checkpoint)
            .boxed(),
    );
    assert!(pending
        .last_mut()
        .unwrap()
        .as_mut()
        .now_or_never()
        .is_none());
    assert_eq!(runtime.usage().completions, usage);
    assert_eq!(pool.usage(), charged);
    assert!(group.checkpoint_manifest()?.is_none());

    let gate = f.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let mut admitted = futures_util::future::try_join_all(pending).boxed();
    assert!(admitted.as_mut().now_or_never().is_none());
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    // A real persistence fence is now held. Polling the ninth request lets it
    // acquire refunded completion admission while its source remains physical.
    assert!(admitted.as_mut().now_or_never().is_none());
    runtime.status(route.history).await?;
    tokio::time::timeout(Duration::from_secs(5), admitted).await??;
    assert_eq!(runtime.usage().completions.items, 0);
    assert_eq!(runtime.usage().completions.bytes, 0);
    assert_eq!(pool.usage(), charged);
    assert!(group.checkpoint_manifest()?.is_none());
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    assert_eq!(
        runtime.status(route.history).await?.applied,
        before.position()
    );
    drop(release);
    controller::wait_for(async || Ok(group.checkpoint_manifest()?.as_ref() == Some(&before)))
        .await?;
    controller::wait_for(async || Ok(pool.usage().sources == 0)).await?;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&before));
    assert!(group.last_index()? >= successor.position.index.get());
    let (sql, applier) = &f.appliers[&(0, route.history)];
    sql.prepare_apply(applier, work)
        .await?
        .complete(&runtime)
        .await?;
    f.drive(Some(successor.position.index)).await?;
    tokio::time::timeout(Duration::from_secs(15), async {
        while targets
            .iter()
            .any(|(group, position)| f.applied[group].iter().any(|index| *index < position.index))
        {
            f.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    for runtime in &f.runtimes {
        for (group, cell) in &cells {
            let authority = runtime
                .reconciliation_authority(*group, cell.clone())
                .await?;
            assert_eq!(authority.cell().version.sequence.get(), 1);
            assert_eq!(authority.cell().witness.position, targets[group]);
        }
        let authority = runtime
            .reconciliation_authority(route.history, successor_cell.clone())
            .await?;
        assert_eq!(authority.cell().version.sequence.get(), 1);
        assert_eq!(authority.cell().witness.position, successor.position);
        assert_eq!(runtime.usage().completions.items, 0);
        assert_eq!(runtime.usage().completions.bytes, 0);
    }
    assert_eq!(pool.usage().retained_bytes, 0);
    f.close().await
}
