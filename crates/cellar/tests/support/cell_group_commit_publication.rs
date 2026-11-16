// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Commit metadata joins the exact applied-root sync, never its acknowledgement.

use super::*;
use raft::Storage as _;

pub(crate) async fn run(rooted: bool) -> Result<()> {
    let (mut fixture, route, cell, _) = enrolled_sql_cells(rooted).await?;
    let work = tokio::time::timeout(Duration::from_secs(20), async {
        let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("selected root")?;
        let prior = fixture.runtimes[0].execution_authority(route.history, cell.clone()).await?;
        let mut executor = fixture.private_cell(&cell).await?;
        let (mut executor, prepared) = fixture.cluster.domain.blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(prepared) = executor.stage(cell_attempt(249)?, true, |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"committed-result".to_vec())
            })? else { anyhow::bail!("new proposal required") };
            executor.mark_submitted(prepared.proposal())?;
            Ok((executor, prepared))
        })?.await??;
        let proposal = accepted_private(&fixture.runtimes[0], &prepared).await?;
        let mut held = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(20), async {
            while !held.contains_key(&route.history) {
                fixture.pump(&[route.history], &mut held).await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        }).await??;
        let apply = held.remove(&route.history).context("committed apply")?;
        anyhow::ensure!(apply.ticket.position() == proposal.position);
        anyhow::ensure!(group.last_index()? >= proposal.position.index.get());
        anyhow::ensure!(
            group.initial_state()?.hard_state.commit == before.position().index.get(),
            "commit metadata forced an independent durability barrier before applied-root preparation"
        );
        anyhow::ensure!(group.group_manifest()?.as_ref() == Some(&before));
        anyhow::ensure!(matches!(fixture.runtimes[0].execution_authority(route.history, cell.clone()).await,
            Err(RuntimeError::NotReady)));

        // Exercise the production applier's sealed commit ticket, not only the
        // fixture's image builder. Preparation alone cannot publish the result.
        apply::install(&mut fixture, &cell, 0).await?;
        let (pool, applier) = fixture.appliers.get(&(0, route.history)).context("SQL applier")?;
        let next = pool.prepare_apply(applier, apply).await?;
        anyhow::ensure!(next.manifest().position() == proposal.position);
        anyhow::ensure!(group.group_manifest()?.as_ref() == Some(&before));
        let gate = fixture.cluster.voters[0].gate.clone();
        let release = gate.arm(fault::HOLD_SYNC);
        // Submission is not durable application. Hold the actual engine sync,
        // then test the execution proof needed to settle the producer's receipt.
        next.complete(&fixture.runtimes[0]).await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        anyhow::ensure!(group.group_manifest()?.as_ref() == Some(&before));
        anyhow::ensure!(group.initial_state()?.hard_state.commit == before.position().index.get());
        let mut observed = Box::pin(fixture.runtimes[0].execution_authority(route.history, cell.clone()));
        anyhow::ensure!(tokio::time::timeout(Duration::from_millis(30), &mut observed).await.is_err(),
            "held root sync granted execution authority");
        executor = fixture.cluster.domain.blocking(move || -> Result<_> {
            anyhow::ensure!(matches!(executor.finish(prior),
                Err(ExecutionError::Invalid("committed cell proposal/image"))),
                "producer settled with the previous root while publication sync was held");
            Ok(executor)
        })?.await??;
        drop(release);
        let authority = tokio::time::timeout(Duration::from_secs(5), observed).await??;
        fixture.drive(Some(proposal.position.index)).await?;
        anyhow::ensure!(group.initial_state()?.hard_state.commit == proposal.position.index.get());
        anyhow::ensure!(group.group_manifest()?.context("published root")?.position() == proposal.position);
        fixture.cluster.domain.blocking(move || -> Result<_> {
            let receipt = executor.finish(authority)?;
            anyhow::ensure!(receipt.receipt.result() == b"committed-result");
            executor.close()?;
            Ok(())
        })?.await??;
        Ok::<_, anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    if work.is_err() {
        let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
        eprintln!(
            "retained commit metadata cluster disk: {}",
            root.keep().display()
        );
    }
    let cleanup = fixture.close().await;
    work?;
    cleanup
}

pub(crate) async fn foreign_ticket() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    let work = tokio::time::timeout(Duration::from_secs(20), async {
        cluster.elect().await?;
        let route = cluster.birth.route().clone();
        let cell = history(&route, "commit-ticket")?;
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(1);")?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        for voter in &mut cluster.voters {
            voter.paused = true;
        }
        cluster.voters[0]
            .owner
            .as_mut()
            .context("owner")?
            .propose(request(&route, cell, 1, &cut)?)?;
        cluster.drive(false).await?;
        let original = cluster.voters[0]
            .held
            .as_ref()
            .context("local apply")?
            .clone();
        let mut foreign = original.clone();
        foreign.ticket = cluster.voters[1]
            .held
            .as_ref()
            .context("peer apply")?
            .ticket
            .clone();
        let voter = &mut cluster.voters[0];
        let group = voter.log.cell_group(&route.history)?;
        anyhow::ensure!(
            group.initial_state()?.hard_state.commit < original.entry.index,
            "foreign ticket control did not enter deferred-commit preparation"
        );
        let pool = CellGroupSqlPool::start(
            &voter.log,
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 1,
                max_pending_cells: 1,
                group_pending_cells: 1,
                node_bytes: 1024 * 1024 * 1024,
                group_bytes: 512 * 1024 * 1024,
            },
        )?;
        let applier = CellGroupApplier::new(
            group,
            voter.pages.take().context("pages")?,
            CellGroupApplyImages::Materialized {
                images: voter.images.clone(),
                services: cellar_ltx::generation::ReadServices {
                    now_unix_millis: Arc::new(|| 10),
                    fill_random: Arc::new(|bytes| bytes.fill(17)),
                },
            },
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        let result = async {
            anyhow::ensure!(
                matches!(
                    pool.prepare_apply(&applier, foreign).await,
                    Err(ExecutionError::Invalid("group apply committed base"))
                ),
                "another engine's ticket authorized deferred-commit preparation"
            );
            let prepared = pool.prepare_apply(&applier, original).await?;
            anyhow::ensure!(
                prepared.manifest().position().index.get() > applier_group_commit(voter, &route)?
            );
            drop(prepared);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let cleanup = pool.close().await;
        drop(applier);
        result?;
        cleanup?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(std::convert::identity);
    if work.is_err() {
        let root = std::mem::replace(&mut cluster._root, tempfile::tempdir()?);
        eprintln!(
            "retained foreign commit ticket disk: {}",
            root.keep().display()
        );
    }
    let cleanup = cluster.close().await;
    work?;
    cleanup
}

fn applier_group_commit(voter: &Voter, route: &GroupRoute) -> Result<u64> {
    Ok(voter
        .log
        .cell_group(&route.history)?
        .initial_state()?
        .hard_state
        .commit)
}
