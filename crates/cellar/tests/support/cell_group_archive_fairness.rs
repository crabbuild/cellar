// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently archived histories contend for one controller and compaction lane.
use super::*;
use raft::Storage as _;

async fn reclaimed(
    fixture: &RuntimeCluster,
    controller: &CellGroupController,
    group: GroupHistory,
    cut: LogIndex,
) -> Result<()> {
    published(controller, group, cut).await?;
    let log = fixture.cluster.voters[0].log.cell_group(&group)?;
    controller::wait_for(async || Ok(log.first_index()? > cut.get())).await?;
    let checkpoint = log
        .checkpoint_manifest()?
        .context("compacted archive checkpoint")?;
    let archived = controller
        .archive_status(group)?
        .context("archive status")?
        .published
        .context("verified archive")?;
    assert!(checkpoint.position().index >= cut);
    assert!(checkpoint.position().index <= archived.position().index);
    assert_eq!(checkpoint.route().history, group);
    Ok(())
}

fn retained(
    fixture: &RuntimeCluster,
    controller: &CellGroupController,
    group: GroupHistory,
    cut: LogIndex,
) -> Result<()> {
    let log = fixture.cluster.voters[0].log.cell_group(&group)?;
    assert!(log.first_index()? <= cut.get());
    assert!(log.last_index()? >= cut.get());
    let status = controller
        .archive_status(group)?
        .context("archive status")?;
    assert!(!status.stopped);
    assert!(status
        .published
        .is_none_or(|marker| marker.position().index < cut));
    Ok(())
}

pub(crate) async fn archived_groups_reclaim_independently(
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let (mut fixture, primary) = Box::pin(setup_store(false, store.clone())).await?;
    let (base, remote) = registration(&fixture, &primary, false).await?;
    let mut archives = vec![(base.clone(), remote)];
    for number in [31, 41] {
        let (directory, birth) = fixture
            .add_group_in_store(number, store.clone(), |_| Arc::new(DirectFileSystem))
            .await?;
        controller::attach_group_appliers(&mut fixture, primary.history, birth.route().history)
            .await?;
        let (mut config, remote) = registration_for(&fixture, &directory, &birth, false).await?;
        config.io = base.io.clone();
        config.owners = base.owners.clone();
        config.tails = base.tails.clone();
        config.checkpoints = base.checkpoints.clone();
        config.scratch = base.scratch.clone();
        archives.push((config, remote));
    }
    fixture.connect_tcp().await?;
    for transport in &fixture.transports {
        transport.enable_group_checkpoints(CellGroupCheckpointPool::new(
            checkpoint_fixture::limits(),
        )?)?;
    }
    let controllers = controller::start_with_limits(
        &mut fixture,
        CellGroupControllerLimits {
            max_groups: 3,
            checkpoints: Some(CellGroupCheckpointPolicy {
                interval_entries: 1,
                max_parallel: 1,
            }),
            ..controller::limits()
        },
    )
    .await?;
    let owner = &controllers[0];
    for (config, _) in &archives {
        let group = config.birth.route().history;
        if group != primary.history {
            fixture.runtimes[0].campaign(group).await?;
            controller::wait_for(async || Ok(fixture.runtimes[0].status(group).await?.ready))
                .await?;
        }
        owner.archive(config.clone())?.response().await?;
    }

    let held = archives[0].1.hold_publication();
    let mut first = vec![add(&fixture, &primary, false, 1).await?];
    tokio::time::timeout(Duration::from_secs(10), held.entered.notified())
        .await
        .context("first group entered the held archive publication")?;
    for (config, _) in &archives[1..] {
        first.push(add(&fixture, config.birth.route(), false, 1).await?);
    }
    for ((config, _), &cut) in archives.iter().zip(&first) {
        retained(&fixture, owner, config.birth.route().history, cut)?;
    }
    held.release.notify_one();
    for ((config, _), cut) in archives.iter().zip(first) {
        reclaimed(&fixture, owner, config.birth.route().history, cut).await?;
    }

    // An unavailable history retains its local tail without starving the two
    // healthy histories that share this node's archive and checkpoint owners.
    let blocked = &archives[1].1;
    blocked.outage.store(true, Ordering::Release);
    let mut latest = Vec::new();
    for (config, _) in &archives {
        latest.push(add(&fixture, config.birth.route(), false, 2).await?);
    }
    for index in [0, 2] {
        reclaimed(
            &fixture,
            owner,
            archives[index].0.birth.route().history,
            latest[index],
        )
        .await?;
    }
    retained(
        &fixture,
        owner,
        archives[1].0.birth.route().history,
        latest[1],
    )?;
    blocked.outage.store(false, Ordering::Release);
    reclaimed(
        &fixture,
        owner,
        archives[1].0.birth.route().history,
        latest[1],
    )
    .await?;

    for ((config, _), cut) in archives.iter().zip(latest) {
        let history = config.birth.route().history;
        let coverage = verified_current(
            &fixture,
            config,
            false,
            &format!("archive-audit-{}", hex::encode(history.group.bytes())),
        )
        .await?;
        assert_eq!(coverage.marker().route().history, history);
        assert_eq!(coverage.marker().position().index, cut);
        let root = coverage.recovery().manifest();
        assert_eq!(
            root.pinned_images(),
            if history == primary.history { 4 } else { 2 }
        );
        for ordinal in 1..=2 {
            let (_, pin) = root
                .pin_cell(&CellId::new(format!("later-{ordinal}"))?)
                .context("restored cell")?;
            let CellImagePin::Materialized(pin) = pin else {
                anyhow::bail!("expected restored SQLite generation");
            };
            let bytes = fixture
                .cluster
                .domain
                .blocking(move || {
                    pin.open_reader()?
                        .read_exact_at(0, pin.state().length as usize)
                })?
                .await??;
            let expected = images::image(
                None,
                &format!("CREATE TABLE value(n); INSERT INTO value VALUES({ordinal})"),
            )?;
            assert_eq!(bytes, expected);
        }
    }
    for controller in controllers {
        assert_eq!(controller.usage().quarantined_groups, 0);
        controller.close().await?;
    }
    assert_eq!(base.tails.usage().operations, 0);
    fixture.close().await
}
