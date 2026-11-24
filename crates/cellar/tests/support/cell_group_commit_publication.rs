// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Commit metadata joins the exact applied-root sync, never its acknowledgement.

use super::*;
use raft::{GetEntriesContext, Storage as _};

pub(crate) async fn run(rooted: bool) -> Result<()> {
    publication(rooted, false).await
}

pub(crate) async fn with_checkpoint(rooted: bool) -> Result<()> {
    publication(rooted, true).await
}

async fn publication(rooted: bool, checkpoint_before_apply: bool) -> Result<()> {
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

        if checkpoint_before_apply {
            // A real retained SQL proposal has committed, but its commit-only
            // hard state still belongs to the later applied-root sync. The
            // controller may checkpoint the held successor's preceding cut.
            let hard = group.initial_state()?.hard_state;
            let last = group.last_index()?;
            let suffix = group.entries(hard.commit + 1, last + 1, None, GetEntriesContext::empty(false))?;
            let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
            let checkpoint = pool.capture(&group, &apply.base)?.prepare_publication()?;
            fixture.runtimes[0]
                .checkpoint_before_apply(route.history, apply.ticket.clone(), checkpoint)
                .await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Err(error) = fixture.runtimes[0].status(route.history).await {
                        let completion = fixture.runtimes[0].close().await;
                        anyhow::bail!("checkpoint with deferred commit stopped runtime: {error:?}; {completion:?}");
                    }
                    if group.checkpoint_manifest()?.as_ref() == Some(&before) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(())
            }).await??;
            anyhow::ensure!(group.initial_state()?.hard_state == hard,
                "local checkpoint published the successor's deferred commit");
            anyhow::ensure!(group.last_index()? == last,
                "local checkpoint changed the retained suffix");
            anyhow::ensure!(group.first_index()? == before.position().index.get() + 1);
            anyhow::ensure!(group.entries(hard.commit + 1, last + 1, None, GetEntriesContext::empty(false))? == suffix,
                "local checkpoint rewrote the retained suffix");
            anyhow::ensure!(group.group_manifest()?.as_ref() == Some(&before));
            anyhow::ensure!(matches!(fixture.runtimes[0].execution_authority(route.history, cell.clone()).await,
                Err(RuntimeError::NotReady)), "checkpoint granted successor execution authority");
        }

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
        // Ordinary enrollment retains its existing protocol barrier. The new
        // path applies to this leader's exact retained SQL preparation.
        cluster.voters[0]
            .owner
            .as_mut()
            .context("owner")?
            .propose(request(&route, cell.clone(), 1, &cut)?)?;
        cluster.drive(false).await?;
        let authority = cluster.voters[0]
            .owner
            .as_ref()
            .context("owner")?
            .execution_authority(&cell)?;
        let options = PrivateOptions {
            directory: cluster._root.path().join("private-commit-ticket"),
            host: cellar_ltx::LtxHost::new(
                || 10,
                |_| Ok(Duration::ZERO),
                |path| async move { std::fs::read(path) },
                |job| async move {
                    job();
                    Ok(())
                },
            ),
            sqlite_vfs: None,
            images: cluster.voters[0].images.clone(),
            query_services: cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 10),
                fill_random: Arc::new(|bytes| bytes.fill(17)),
            },
            application_digest: route.schema_digest,
            limits: ExecutionLimits::default(),
        };
        let (executor, prepared) = cluster
            .domain
            .blocking(move || -> Result<_> {
                let mut executor = CellPrivateExecutor::open(options, authority)?;
                let CellStageOutcome::Prepared(prepared) =
                    executor.stage(cell_attempt(250)?, true, |tx| {
                        tx.execute("UPDATE value SET n=n+1", [])?;
                        Ok(b"ticket-result".to_vec())
                    })?
                else {
                    anyhow::bail!("new SQL preparation")
                };
                executor.mark_submitted(prepared.proposal())?;
                Ok((executor, prepared))
            })?
            .await??;
        for voter in &mut cluster.voters {
            voter.paused = true;
        }
        anyhow::ensure!(matches!(
            cluster.voters[0]
                .owner
                .as_mut()
                .context("owner")?
                .submit(prepared)?,
            CellSubmissionOutcome::Accepted(_)
        ));
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
        cluster.domain.blocking(move || executor.close())?.await??;
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
