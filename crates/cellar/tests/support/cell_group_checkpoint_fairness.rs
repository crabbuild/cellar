// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Concurrent real enrollments compete for one shared checkpoint slot. Inspect
//! each group's durable progress at fixed cuts before either writer finishes.
use super::*;

struct Trace {
    cut: tokio::sync::Barrier,
    resume: tokio::sync::Barrier,
}

async fn write(
    runtime: CellGroupRuntime,
    route: GroupRoute,
    rooted: bool,
    trace: Arc<Trace>,
    controller: CellGroupController,
) -> Result<CellGroupProposal> {
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(83);")?;
    let mut last = None;
    for round in 0..32 {
        let cell = history(&route, &format!("concurrent-checkpoint-{round}"))?;
        let accepted = runtime
            .propose(
                route.history,
                apply::enrollment_bytes(&route, &cell, &bytes, rooted)?,
            )
            .await?;
        tokio::time::timeout(Duration::from_secs(15), async {
            while runtime.status(route.history).await?.applied.index < accepted.position.index {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        assert!(controller.usage().active_checkpoints <= 1);
        assert_eq!(controller.usage().quarantined_groups, 0);
        if round == 8 || round == 24 {
            trace.cut.wait().await;
            trace.resume.wait().await;
        }
        last = Some(accepted);
    }
    last.context("nonempty write trace")
}

pub(crate) async fn concurrent_checkpoints(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut f, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = controller::add_second(&mut f, route.history).await?;
    f.connect_tcp().await?;
    let pools: Vec<_> = (0..3)
        .map(|_| CellGroupCheckpointPool::new(checkpoint_fixture::limits()))
        .collect::<std::result::Result<_, _>>()?;
    for (transport, pool) in f.transports.iter().zip(&pools) {
        transport.enable_group_checkpoints(pool.clone())?;
    }
    let held = pools[0].receive(
        f.appliers[&(0, route.history)].1.clone(),
        checkpoint_receive::scratch()?,
        f.cluster._root.path().into(),
    )?;
    let charged = pools[0].usage();
    let mut bounds = controller::limits();
    bounds.checkpoints = Some(CellGroupCheckpointPolicy {
        interval_entries: 2,
        max_parallel: 1,
    });
    let controllers = controller::start_with_limits(&mut f, bounds).await?;
    f.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(f.runtimes[0].status(second).await?.ready)).await?;
    let second_route = f.cluster.voters[0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("second route")?
        .route()
        .clone();
    let trace = Arc::new(Trace {
        cut: tokio::sync::Barrier::new(3),
        resume: tokio::sync::Barrier::new(3),
    });
    let mut writers = Vec::new();
    for (route, rooted) in [(route.clone(), rooted), (second_route, false)] {
        let runtime = f.runtimes[0].clone();
        let control = controllers[0].clone();
        let trace = trace.clone();
        let group = route.history;
        writers.push((
            group,
            f.cluster
                .domain
                .spawn(async move { write(runtime, route, rooted, trace, control).await })?,
        ));
    }
    tokio::time::timeout(Duration::from_secs(15), trace.cut.wait()).await?;
    assert_eq!(pools[0].usage(), charged);
    assert!(controllers[0].usage().checkpoint_retries > 0);
    assert_eq!(controllers[0].usage().checkpoints_published, 0);
    let mut release_cuts = BTreeMap::new();
    for group in [route.history, second] {
        let log = f.cluster.voters[0].log.cell_group(&group)?;
        assert!(log.checkpoint_manifest()?.is_none());
        release_cuts.insert(
            group,
            log.group_manifest()?.context("held-slot cut")?.position(),
        );
    }
    drop(held);
    trace.resume.wait().await;
    // Each writer has sixteen more committed/applied entries before stopping
    // at this cut. Both must obtain a newer checkpoint while its peer is active.
    tokio::time::timeout(Duration::from_secs(15), trace.cut.wait()).await?;
    for group in [route.history, second] {
        let log = f.cluster.voters[0].log.cell_group(&group)?;
        let current = log.group_manifest()?.context("concurrent applied cut")?;
        let checkpoint = log
            .checkpoint_manifest()?
            .context("concurrent checkpoint")?;
        assert!(checkpoint.position().index > release_cuts[&group].index);
        assert!(checkpoint.position().index < current.position().index);
        assert_eq!(
            current.root().cells(),
            if group == route.history { 27 } else { 25 }
        );
    }
    trace.resume.wait().await;
    for (group, writer) in writers {
        let last = writer.await??;
        controller::applied(&f, group, last.position.index).await?;
    }
    for (node, controller) in controllers.iter().enumerate() {
        assert_eq!(controller.usage().quarantined_groups, 0);
        assert!(!controller.usage().closing);
        for group in [route.history, second] {
            let log = f.cluster.voters[node].log.cell_group(&group)?;
            assert!(log.checkpoint_manifest()?.is_some());
            let current = log.group_manifest()?.context("final applied cut")?;
            assert_eq!(
                current.root().cells(),
                if group == route.history { 34 } else { 32 }
            );
            let (sql, applier) = &f.appliers[&(node, group)];
            let applier = applier.clone();
            let recovered = f
                .cluster
                .domain
                .blocking(move || applier.recover_selected())?
                .await??;
            assert_eq!(recovered.marker(), &current);
            for round in 0..32 {
                let (leaf, _) = recovered
                    .pin_cell(&CellId::new(format!("concurrent-checkpoint-{round}"))?)
                    .context("independent concurrent cell")?;
                assert_eq!(leaf.version.sequence.get(), 1);
            }
            assert!(sql.usage()?.completion_reservations <= 1);
        }
        controller.close().await?;
        assert_eq!(controller.usage().active_checkpoints, 0);
        assert_eq!(pools[node].usage().sources, 0);
        assert_eq!(pools[node].usage().retained_bytes, 0);
    }
    f.close().await
}
