// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise simultaneous roles and independent apply on a shared node runtime.

use super::*;
use ::raft::{GetEntriesContext, Storage};

pub(crate) async fn mixed_roles() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let first = fixture.cluster.birth.route().clone();
    let mut groups = vec![first];
    for id in 30..41 {
        let scope = fixture.add_group(id).await?;
        let marker = fixture.cluster.voters[0]
            .log
            .cell_group(&scope)?
            .group_manifest()?
            .context("new group manifest")?;
        groups.push(marker.route().clone());
    }
    for (index, route) in groups.iter().enumerate() {
        fixture.runtimes[index % 3].campaign(route.history).await?;
    }
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut held).await?;
            let mut ready = true;
            for (index, route) in groups.iter().enumerate() {
                ready &= fixture.runtimes[index % 3]
                    .status(route.history)
                    .await?
                    .ready;
            }
            if ready {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;

    for (index, route) in groups.iter().enumerate() {
        let leader = index % 3;
        let scope = route.history;
        let bytes = images::image(
            None,
            &format!("CREATE TABLE value(n); INSERT INTO value VALUES ({index});"),
        )?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        for name in ["alpha", "beta"] {
            let data = request(route, history(route, name)?, 1, &cut)?;
            let accepted = fixture.runtimes[leader]
                .propose(scope, data.clone())
                .await?;
            applied(&mut fixture, scope, accepted.position.index, &[], &mut held).await?;
            for (member, voter) in fixture.cluster.voters.iter().enumerate() {
                let status = fixture.runtimes[member].status(scope).await?;
                assert_eq!(status.leader, Some(leader as u64 + 1));
                assert_eq!(status.ready, member == leader);
                let group = voter.log.cell_group(&scope)?;
                let index = accepted.position.index.get();
                let entries =
                    group.entries(index, index + 1, None, GetEntriesContext::empty(false))?;
                assert_eq!(&entries[0].data[..], data.as_slice());
            }
        }
        for voter in &fixture.cluster.voters {
            let marker = voter
                .log
                .cell_group(&scope)?
                .group_manifest()?
                .context("applied manifest")?;
            assert_eq!(marker.root().cells(), 2);
        }
    }
    fixture.close().await
}

pub(crate) async fn hot_group_fairness() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let hot = fixture.cluster.birth.route().clone();
    let neighbor = fixture.add_group(51).await?;
    let cold = fixture.add_group(61).await?;
    let neighbor_route = fixture.cluster.voters[1]
        .log
        .cell_group(&neighbor)?
        .group_manifest()?
        .context("neighbor manifest")?
        .route()
        .clone();
    fixture.runtimes[0].campaign(hot.history).await?;
    fixture.runtimes[1].campaign(neighbor).await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut held).await?;
            if fixture.runtimes[0].status(hot.history).await?.ready
                && fixture.runtimes[1].status(neighbor).await?.ready
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;

    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let data = request(&hot, history(&hot, "hot-first")?, 1, &cut)?;
    let hot_target = fixture.runtimes[0].propose(hot.history, data).await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !held.contains_key(&hot.history) {
            fixture.pump(&[hot.history], &mut held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;

    let mut flood = tokio::task::JoinSet::new();
    for index in 0..64u8 {
        let runtime = fixture.runtimes[0].clone();
        let scope = hot.history;
        let encoded = request(&hot, history(&hot, &format!("hot-{index}"))?, 1, &cut)?;
        let mut entry = CellGroupEntry::decode(&encoded, &hot, GroupEntryLimits::default())?;
        entry.binding.proposal = ProposalId::new([index + 2; 16])?;
        let data = entry.encode(&hot, GroupEntryLimits::default())?;
        flood.spawn(async move { runtime.propose(scope, data).await });
    }
    let data = request(
        &neighbor_route,
        history(&neighbor_route, "neighbor")?,
        1,
        &cut,
    )?;
    let accepted = fixture.runtimes[1].propose(neighbor, data).await?;
    applied(
        &mut fixture,
        neighbor,
        accepted.position.index,
        &[hot.history],
        &mut held,
    )
    .await?;
    fixture.runtimes[2].campaign(cold).await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !fixture.runtimes[2].status(cold).await?.ready {
            fixture.pump(&[hot.history], &mut held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert!(held.contains_key(&hot.history));
    assert!(
        fixture.runtimes[0].status(hot.history).await?.applied.index < hot_target.position.index
    );

    flood.abort_all();
    while let Some(outcome) = flood.join_next().await {
        match outcome {
            Ok(Ok(_) | Err(RuntimeError::Overloaded)) => (),
            Err(error) if error.is_cancelled() => (),
            other => anyhow::bail!("hot submission failed: {other:?}"),
        }
    }
    let work = held.remove(&hot.history).context("held hot apply")?;
    let next = prepare(&mut fixture.cluster.voters[0], &work)?;
    fixture.runtimes[0]
        .complete_apply(hot.history, work, next)
        .await?;
    applied(
        &mut fixture,
        hot.history,
        hot_target.position.index,
        &[],
        &mut held,
    )
    .await?;
    fixture.close().await
}

async fn applied(
    fixture: &mut RuntimeCluster,
    scope: GroupHistory,
    index: LogIndex,
    hold: &[GroupHistory],
    held: &mut BTreeMap<GroupHistory, CellGroupApply>,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while fixture.applied[&scope]
            .iter()
            .any(|applied| *applied < index)
        {
            fixture.pump(hold, held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
