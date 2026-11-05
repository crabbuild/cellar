// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the exact rooted seal through the multiplexed three-voter owner.
use super::*;

pub(crate) async fn source_seal_blocks_delayed_work() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    let scope = route.history;
    let directory = fixture.cluster.directory.clone();
    let log = fixture.cluster.voters[0].log.clone();
    let domain = &fixture.cluster.domain;
    let view = domain.run(directory.observe()).await??;
    let (left, right) = route
        .range
        .split(cell_routing_key(scope.namespace, &CellId::new("pivot")?))?;
    let mut children = Vec::new();
    for (id, range) in [(11, left), (12, right)] {
        let child = directory.prepare_group_birth(CellGroupBirthSpec {
            history: GroupHistory {
                group: GroupId::new([id; 16])?,
                incarnation: IncarnationId::new([id + 10; 16])?,
                ..scope
            },
            range,
            epoch: RouteEpoch::new(2)?,
            creation: ProposalId::new([id; 16])?,
            voters: (1..=3)
                .map(|n| {
                    Ok(InitialMember {
                        member: MemberId::new(n)?,
                        node: NodeId::new([n as u8; 16])?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        })?;
        domain.run(directory.reserve_group_birth(&child)).await??;
        children.push(child.route().clone());
    }
    let plan = directory.prepare_replacement(
        &view,
        ProposalId::new([13; 16])?,
        std::slice::from_ref(&route),
        &children,
    )?;
    domain.run(directory.plan_transition(&plan)).await??;
    domain
        .run(directory.prepare_transition(&plan, [14; 32]))
        .await??;
    anyhow::ensure!(matches!(
        domain
            .run(directory.prepare_source_seal(&plan, scope, &log))
            .await?,
        Err(CatalogError::Fenced)
    ));
    domain
        .run(directory.authorize_transition_seal(&plan))
        .await??;
    fixture.runtimes[0].campaign(scope).await?;
    fixture.drive(None).await?;
    let cell = history(&route, "before-seal")?;
    let delayed = history(&route, "delayed-enrollment")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let accepted = fixture.runtimes[0]
        .propose(scope, request(&route, cell.clone(), 1, &cut)?)
        .await?;
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    anyhow::ensure!(matches!(
        fixture.runtimes[0].seal_source(permit).await,
        Err(RuntimeError::NotReady)
    ));
    fixture.drive(Some(accepted.position.index)).await?;
    let before = log
        .cell_group(&scope)?
        .group_manifest()?
        .context("committed source image")?;
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    let CellGroupSealOutcome::Accepted(seal) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("seal must first enter the log");
    };
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    let CellGroupSealOutcome::Accepted(retry) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("seal committed without a voter quorum");
    };
    anyhow::ensure!(retry.position == seal.position && retry.digest == seal.digest);
    anyhow::ensure!(matches!(
        fixture.runtimes[0]
            .read_index(scope, b"late-read".to_vec())
            .await,
        Err(RuntimeError::NotReady)
    ));
    anyhow::ensure!(matches!(
        fixture.runtimes[0]
            .propose(scope, request(&route, delayed.clone(), 2, &cut)?)
            .await,
        Err(RuntimeError::NotReady)
    ));
    tokio::time::timeout(Duration::from_secs(20), async {
        while !fixture.applied[&scope]
            .iter()
            .all(|index| *index >= seal.position.index)
        {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let permit = fixture
        .cluster
        .domain
        .run(directory.prepare_source_seal(&plan, scope, &log))
        .await??;
    let CellGroupSealOutcome::Applied(applied) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("committed seal was proposed a second time");
    };
    anyhow::ensure!(applied.position() == seal.position);
    let deadline = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await?
        .checked_add(5_000)
        .context("read deadline")?;
    for (voter, runtime) in fixture.cluster.voters.iter().zip(&fixture.runtimes) {
        let marker = voter
            .log
            .cell_group(&scope)?
            .group_manifest()?
            .context("sealed source marker")?;
        anyhow::ensure!(marker.source_seal() == Some(&applied) && marker.root() == before.root());
        anyhow::ensure!(marker.root().cells() == 1 && !marker.is_active());
        let restored: GroupManifest = serde_json::from_slice(&serde_json::to_vec(&marker)?)?;
        anyhow::ensure!(restored == marker);
        anyhow::ensure!(matches!(
            runtime
                .read_authority(
                    scope,
                    cell.clone(),
                    CellReadConsistency::Linearizable,
                    deadline
                )
                .await,
            Err(RuntimeError::NotReady)
        ));
        anyhow::ensure!(matches!(
            runtime.read_index(scope, b"sealed-read".to_vec()).await,
            Err(RuntimeError::NotReady)
        ));
    }
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let snapshot = fixture.runtimes[0].capture_checkpoint(scope, &pool).await?;
    anyhow::ensure!(snapshot.marker().source_seal() == Some(&applied));
    drop(snapshot);
    fixture.close().await
}
