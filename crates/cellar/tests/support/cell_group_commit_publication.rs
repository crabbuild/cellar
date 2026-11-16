// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Commit metadata joins the exact applied-root sync, never its acknowledgement.

use super::*;

pub(super) async fn run(rooted: bool) -> Result<()> {
    let (mut fixture, route, cell, _) = enrolled_sql_cells(rooted).await?;
    let work = async {
        let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
        let before = group.group_manifest()?.context("selected root")?;
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
        let mut publication = Box::pin(next.complete(&fixture.runtimes[0]));
        tokio::time::timeout(Duration::from_secs(5), async {
            cellar::asyncrt::select! {
                result = &mut publication => anyhow::bail!("root publication escaped held sync: {result:?}"),
                _ = gate.entered.notified() => Ok::<_, anyhow::Error>(()),
            }
        }).await??;
        anyhow::ensure!(group.group_manifest()?.as_ref() == Some(&before));
        anyhow::ensure!(group.initial_state()?.hard_state.commit == before.position().index.get());
        anyhow::ensure!(tokio::time::timeout(Duration::from_millis(30), &mut publication).await.is_err(),
            "apply acknowledged before root/commit sync");
        drop(release);
        publication.await?;
        fixture.drive(Some(proposal.position.index)).await?;
        anyhow::ensure!(group.initial_state()?.hard_state.commit == proposal.position.index.get());
        anyhow::ensure!(group.group_manifest()?.context("published root")?.position() == proposal.position);
        let authority = fixture.runtimes[0].execution_authority(route.history, cell.clone()).await?;
        fixture.cluster.domain.blocking(move || -> Result<_> {
            let receipt = executor.finish(authority)?;
            anyhow::ensure!(receipt.receipt.result() == b"committed-result");
            executor.close()?;
            Ok(())
        })?.await??;
        Ok::<_, anyhow::Error>(())
    }.await;
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
