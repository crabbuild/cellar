// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production controller owns every event after startup; no fixture pumping.
use super::*;
use raft::Storage;

pub(crate) async fn automatic_checkpoints(rooted: bool, pressure: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    fixture.connect_tcp().await?;
    let pools: Vec<_> = (0..3)
        .map(|_| CellGroupCheckpointPool::new(checkpoint_fixture::limits()))
        .collect::<std::result::Result<_, _>>()?;
    for (transport, pool) in fixture.transports.iter().zip(&pools) {
        transport.enable_group_checkpoints(pool.clone())?;
    }
    let mut held = if pressure {
        Some(pools[0].receive(
            fixture.appliers[&(0, route.history)].1.clone(),
            checkpoint_receive::scratch()?,
            fixture.cluster._root.path().into(),
        )?)
    } else {
        None
    };
    let mut bounds = controller::limits();
    bounds.checkpoints = Some(CellGroupCheckpointPolicy {
        interval_entries: 2,
        max_parallel: 1,
    });
    let controllers = controller::start_with_limits(&mut fixture, bounds).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let initial = fixture.runtimes[0]
        .status(route.history)
        .await?
        .applied
        .index
        .get();
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(23);")?;
    for round in 0..12 {
        // Interleaved writes to two groups share one checkpoint slot per node.
        // All voters must apply every successor, including under pool pressure.
        for (group, format) in [(route.history, rooted), (second, false)] {
            let log = fixture.cluster.voters[0].log.cell_group(&group)?;
            let marker = log.group_manifest()?.context("selected")?;
            let cell = history(marker.route(), &format!("auto-{round}"))?;
            let accepted = fixture.runtimes[0]
                .propose(
                    group,
                    apply::enrollment_bytes(marker.route(), &cell, &bytes, format)?,
                )
                .await?;
            controller::applied(&fixture, group, accepted.position.index).await?;
        }
        for controller in &controllers {
            assert!(!controller.usage().closing);
            assert!(controller.usage().active_checkpoints <= 1);
            assert_eq!(controller.usage().quarantined_groups, 0);
        }
        if pressure && round == 4 {
            assert!(controllers[0].usage().checkpoint_retries > 0);
            assert_eq!(controllers[0].usage().checkpoints_published, 0);
            assert_eq!(pools[0].usage().sources, 1);
            drop(held.take());
            tokio::time::sleep(Duration::from_millis(bounds.retry_interval_ms + 1)).await;
        }
    }
    for (node, controller) in controllers.iter().enumerate() {
        controller::wait_for(async || Ok(controller.usage().active_checkpoints == 0)).await?;
        assert!(controller.usage().checkpoints_published >= 4);
        for group in [route.history, second] {
            let log = fixture.cluster.voters[node].log.cell_group(&group)?;
            let current = log.group_manifest()?.context("applied root")?;
            let checkpoint = log.checkpoint_manifest()?.context("automatic checkpoint")?;
            assert!(checkpoint.position().index.get() > initial);
            assert!(checkpoint.position().index < current.position().index);
            assert!(current.position().index.get() - checkpoint.position().index.get() <= 2);
            assert_eq!(log.first_index()?, checkpoint.position().index.get() + 1);
            assert_eq!(log.last_index()?, current.position().index.get());
        }
        controller.close().await?;
        assert_eq!(controller.usage().active_checkpoints, 0);
        assert_eq!(pools[node].usage().sources, 0);
    }
    drop(controllers);
    fixture.close().await
}
