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
    let log = fixture.cluster.voters[0].log.cell_group(&group)?;
    controller::wait_for(async || Ok(log.first_index()? > cut.get()))
        .await
        .with_context(|| format!("checkpoint did not compact {group:?} through {cut:?}"))?;
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
    archives[0]
        .1
        .lose_immutable_put_response
        .store(true, Ordering::Release);
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
        let route = config.birth.route();
        published(owner, route.history, cut)
            .await
            .with_context(|| format!("first archive cut {cut:?} for {:?}", route.history))?;
        // Checkpoint scheduling retains the successor apply ticket while it
        // checkpoints the already archived base.
        add(&fixture, route, false, 2).await?;
        reclaimed(&fixture, owner, route.history, cut).await?;
    }
    assert!(!archives[0]
        .1
        .lose_immutable_put_response
        .load(Ordering::Acquire));

    // An unavailable history retains its local tail without starving the two
    // healthy histories that share this node's archive and checkpoint owners.
    let blocked = &archives[1].1;
    blocked.outage.store(true, Ordering::Release);
    let mut third = Vec::new();
    for (config, _) in &archives {
        third.push(add(&fixture, config.birth.route(), false, 3).await?);
    }
    let mut latest = Vec::new();
    for index in [0, 2, 1] {
        if index == 1 {
            retained(
                &fixture,
                owner,
                archives[1].0.birth.route().history,
                third[1],
            )?;
            blocked.outage.store(false, Ordering::Release);
        }
        let route = archives[index].0.birth.route();
        let cut = third[index];
        published(owner, route.history, cut)
            .await
            .with_context(|| format!("archive catch-up to {cut:?} for {:?}", route.history))?;
        let successor = add(&fixture, route, false, 4).await?;
        reclaimed(&fixture, owner, route.history, cut).await?;
        latest.push((index, successor));
    }

    for &(index, cut) in &latest {
        let history = archives[index].0.birth.route().history;
        published(owner, history, cut)
            .await
            .with_context(|| format!("final archive cut {cut:?} for {history:?}"))?;
    }
    for controller in controllers {
        assert_eq!(controller.usage().quarantined_groups, 0);
        controller.close().await?;
    }
    assert_eq!(base.tails.usage().operations, 0);

    let mut selected = BTreeMap::new();
    for (index, cut) in latest {
        let config = &archives[index].0;
        let history = config.birth.route().history;
        let coverage = verified_current(
            &fixture,
            config,
            false,
            &format!("archive-audit-{}", hex::encode(history.group.bytes())),
        )
        .await
        .with_context(|| format!("restore selected cut {cut:?} for {history:?}"))?;
        selected.insert(
            index,
            (
                coverage.hold().clone(),
                coverage.recovery().reference().clone(),
            ),
        );
        assert_eq!(coverage.marker().route().history, history);
        assert_eq!(coverage.marker().position().index, cut);
        let root = coverage.recovery().manifest();
        assert_eq!(
            root.pinned_images(),
            if history == primary.history { 6 } else { 4 }
        );
        for ordinal in 1..=4 {
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
    let (primary_hold, tail) = &selected[&0];
    verify_archive_isolation(
        &fixture,
        &archives[0].0,
        primary_hold,
        tail,
        &archives[1].0,
        &selected[&1].0,
    )
    .await?;
    fixture.close().await
}

async fn verify_archive_isolation(
    fixture: &RuntimeCluster,
    primary: &CellGroupArchiveRegistration,
    primary_hold: &CellGroupArchiveHold,
    tail: &CellGroupArchiveTailReference,
    sibling: &CellGroupArchiveRegistration,
    sibling_hold: &CellGroupArchiveHold,
) -> Result<()> {
    let domain = &fixture.cluster.domain;
    let registry = sibling
        .directory
        .open_group_archive(domain, &sibling.io, &sibling.birth)
        .await?;
    let destination = verifier(fixture, sibling.birth.route(), false, "foreign-hold").await?;
    let foreign = registry
        .verify_hold(
            domain,
            primary_hold,
            &sibling.tails,
            sibling.checkpoints.receive_archive(
                destination,
                sibling.scratch.clone(),
                sibling.scratch_directory.clone(),
            )?,
        )
        .await;
    assert!(matches!(foreign, Err(CellGroupArchiveError::Conflict)));

    let suffix = format!(
        "/registries/{}/generations/{}/tails/{}-{}",
        hex::encode(tail.registry()),
        tail.generation(),
        tail.length(),
        hex::encode(tail.digest())
    );
    let store = &fixture.cluster.store;
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    let mut selected = objects
        .iter()
        .filter(|object| object.location.as_ref().ends_with(&suffix));
    let path = &selected.next().context("selected immutable tail")?.location;
    assert!(selected.next().is_none());
    let original = store.get(path).await?.bytes().await?;
    let mut corrupted = original.to_vec();
    *corrupted.first_mut().context("nonempty archive tail")? ^= 1;
    store.put(path, corrupted.into()).await?;

    let checked = async {
        let failure = verified_current(fixture, primary, false, "corrupt-tail").await;
        anyhow::ensure!(
            matches!(
                failure
                    .as_ref()
                    .err()
                    .and_then(|error| error.downcast_ref()),
                Some(CellGroupArchiveError::Invalid("object digest/length"))
            ),
            "corrupt tail did not fail digest validation: {:?}",
            failure.err()
        );
        let restored = verified_current(fixture, sibling, false, "healthy-sibling").await?;
        anyhow::ensure!(
            restored.hold() == sibling_hold,
            "sibling selected cut changed"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    store.put(path, original.into()).await?;
    checked?;
    let repaired = verified_current(fixture, primary, false, "repaired-tail").await?;
    assert!(repaired.hold() == primary_hold);
    Ok(())
}
