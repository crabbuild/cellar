// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The production image adapter, exercised with actual committed three-voter work.
use super::*;

pub(super) fn pool(fixture: &RuntimeCluster, node: usize) -> Result<CellGroupSqlPool> {
    Ok(CellGroupSqlPool::start(
        &fixture.cluster.voters[node].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 1024 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?)
}

pub(super) async fn install(
    fixture: &mut RuntimeCluster,
    cell: &CellHistory,
    node: usize,
) -> Result<()> {
    let route = fixture.cluster.birth.route().clone();
    let config = match fixture.cell_options_at(cell, node).await? {
        CellSqlOptions::Materialized(o) => CellGroupApplyImages::Materialized {
            images: o.images,
            services: o.query_services,
        },
        CellSqlOptions::Extents(o) => CellGroupApplyImages::Extents {
            images: o.images,
            services: o.query_services,
            limits: Default::default(),
            cache_directory: o.cache_directory,
            cache_pool: o.cache_pool,
            cache: o.cache,
        },
    };
    let pool = pool(fixture, node)?;
    let voter = &mut fixture.cluster.voters[node];
    let applier = CellGroupApplier::new(
        voter.log.cell_group(&route.history)?,
        voter.pages.take().context("page owner")?,
        config,
        ManifestLimits::default(),
        GroupEntryLimits::default(),
        100,
    )?;
    fixture
        .appliers
        .insert((node, route.history), (pool, applier));
    Ok(())
}

pub(in crate::runtime_fixture) fn enrollment_bytes(
    route: &GroupRoute,
    cell: &CellHistory,
    bytes: &[u8],
    rooted: bool,
) -> Result<Vec<u8>> {
    let cut = images::Cut::new(&[], bytes, 0, true, false);
    let encoded = request(route, cell.clone(), 1, &cut)?;
    let mut entry = CellGroupEntry::decode(&encoded, route, GroupEntryLimits::default())?;
    if rooted {
        let manifest = ExtentManifest::new(
            ImageBinding {
                history: cell_image_history_digest(cell),
                schema: route.schema_digest,
                epoch: cut.result.epoch,
                txid: cut.result.txid.0,
                checksum: cut.result.checksum,
            },
            Geometry::new(cut.result.page_size, cut.result.length)?,
            &bytes
                .chunks(EXTENT_BYTES as usize)
                .map(ExtentId::of)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )?;
        let GroupCommand::Enroll { image, .. } = &mut entry.command else {
            anyhow::bail!("enroll")
        };
        image.commitment = ImageCommitment::Extents(ExtentRootReference::new(
            manifest.root(),
            manifest.encode()?.len().try_into()?,
        )?);
    }
    Ok(entry.encode(route, GroupEntryLimits::default())?)
}

pub(crate) async fn apply_images(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    for node in 0..3 {
        install(&mut fixture, &a, node).await?;
    }
    let (pool, applier) = fixture.appliers[&(0, route.history)].clone();
    let foreign = fixture.appliers[&(1, route.history)].0.clone();

    // Enroll through the production adapter on all voters, including scheduling
    // summaries derived from real SQLite state rather than fixture defaults.
    let cell = history(&route, "effects")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(7); CREATE TABLE __sys_raft_alarms(due); INSERT INTO __sys_raft_alarms VALUES(90),(30); CREATE TABLE __sys_raft_outbox(next_at); INSERT INTO __sys_raft_outbox VALUES(80),(40);")?;
    let encoded = enrollment_bytes(&route, &cell, &bytes, rooted)?;
    let accepted = fixture.runtimes[0].propose(route.history, encoded).await?;
    fixture.drive(Some(accepted.position.index)).await?;
    for runtime in &fixture.runtimes {
        let state = runtime
            .reconciliation_authority(route.history, cell.clone())
            .await?;
        assert_eq!(state.cell().effects.next_alarm_unix_millis, Some(30));
        assert_eq!(state.cell().effects.next_outbox_unix_millis, Some(40));
        assert_eq!(state.cell().effects.pending_outbox, 2);
    }

    let options = fixture.cell_options(&a).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let CellSqlOutcome::Prepared(sql) = pool
        .stage(options, authority, cell_attempt(231)?, true, |tx| {
            tx.execute("UPDATE value SET n=n+11", [])?;
            Ok(b"applied".to_vec())
        })
        .await?
    else {
        anyhow::bail!("SQL preparation")
    };
    // Business admission is full. Its retained producer cannot prevent apply.
    assert_eq!(pool.usage()?.active_cells, 1);
    let other = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    assert!(matches!(
        pool.stage(
            fixture.cell_options(&b).await?,
            other,
            cell_attempt(232)?,
            true,
            |_| panic!("saturated business admission")
        )
        .await,
        Err(ExecutionError::Refused(
            cellar_logic::consensus::CommandFailure::Overloaded
        ))
    ));
    pool.mark_submitted(a.clone(), sql.proposal()).await?;
    let accepted = accepted_sql(&fixture.runtimes[0], &sql).await?;
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
    let work = held.remove(&route.history).context("committed work")?;
    let before = work.base.marker().clone();
    let mut forged = work.clone();
    forged.entry.data = vec![1, 2, 3].into();
    assert!(pool.prepare_apply(&applier, forged).await.is_err());
    assert!(foreign.prepare_apply(&applier, work.clone()).await.is_err());
    assert_eq!(pool.usage()?.apply_retained_operations, 0);
    assert_eq!(foreign.usage()?.apply_retained_operations, 0);

    let prepared = pool.prepare_apply(&applier, work.clone()).await?;
    assert_eq!(prepared.manifest().position(), accepted.position);
    let duplicate = pool.prepare_apply(&applier, work.clone()).await?;
    assert_eq!(prepared.manifest(), duplicate.manifest());
    assert_eq!(pool.usage()?.apply_retained_operations, 2);
    drop(duplicate);
    let retained = prepared.clone();
    assert_eq!(pool.usage()?.apply_retained_operations, 1);
    assert!(pool.usage()?.apply_retained_bytes > 0);
    assert_eq!(
        fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .as_ref(),
        Some(&before)
    );

    // Completion only queues publication. Retention outlives the caller and a
    // blocked physical log sync; the old root remains selected until sync ends.
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    prepared.complete(&fixture.runtimes[0]).await?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    drop(retained);
    assert_eq!(pool.usage()?.apply_retained_operations, 1);
    assert_eq!(
        fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .as_ref(),
        Some(&before)
    );
    drop(release);
    fixture.drive(Some(accepted.position.index)).await?;
    assert_eq!(pool.usage()?.apply_retained_operations, 0);
    assert!(pool.prepare_apply(&applier, work).await.is_err());
    for runtime in &fixture.runtimes {
        let state = runtime
            .reconciliation_authority(route.history, a.clone())
            .await?;
        assert_eq!(state.cell().version.sequence.get(), 2);
        assert_eq!(state.cell().witness.position, accepted.position);
        assert_eq!(state.cell().witness.proposal, sql.proposal());
    }
    drop(sql);
    drop(pool);
    drop(foreign);
    drop(applier);
    fixture.close().await
}
