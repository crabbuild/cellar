// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary mutations of another cell need not stall private SQL preparation.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn unrelated_mutation(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    for node in 0..3 {
        apply::install(&mut fixture, &a, node).await?;
    }
    let (pool, applier) = fixture.appliers[&(0, route.history)].clone();
    let mut executor = fixture.private_cell(&b).await?;
    let (mut executor, prepared_b) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(prepared) =
                executor.stage(cell_attempt(214)?, true, |tx| {
                    tx.execute("UPDATE value SET n=n+11", [])?;
                    assert_eq!(
                        tx.query_row("SELECT n FROM value", [], |row| row.get::<_, i64>(0))?,
                        11
                    );
                    Ok(b"b-pipeline".to_vec())
                })?
            else {
                anyhow::bail!("new B preparation");
            };
            executor.mark_submitted(prepared.proposal())?;
            Ok((executor, prepared))
        })?
        .await??;
    let accepted_b = accepted_private(&fixture.runtimes[0], &prepared_b).await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture.pump(&[route.history], &mut held).await?;
            let status = fixture.runtimes[0].status(route.history).await?;
            if held.contains_key(&route.history)
                && status.ready
                && status.committed >= accepted_b.position.index
                && status.applied.index < accepted_b.position.index
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("committed B mutation held before physical apply")??;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    // Capture the result before releasing B so the old global readiness gate
    // fails at the intended seam. Cleanup still applies B and joins its writer.
    let prepared_a = async {
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await
            .context("unrelated ordinary mutation blocked private SQL preparation")?;
        let options = fixture.cell_options(&a).await?;
        let CellSqlOutcome::Prepared(prepared) = pool
            .stage(options, authority, cell_attempt(215)?, true, move |tx| {
                observed.fetch_add(1, Ordering::AcqRel);
                tx.execute("UPDATE value SET n=n+7", [])?;
                assert_eq!(
                    tx.query_row("SELECT n FROM value", [], |row| row.get::<_, i64>(0))?,
                    7
                );
                Ok(b"a-pipeline".to_vec())
            })
            .await?
        else {
            anyhow::bail!("new A preparation");
        };
        anyhow::ensure!(calls.load(Ordering::Acquire) == 1 && pool.usage()?.active_cells == 1);
        pool.mark_submitted(a.clone(), prepared.proposal()).await?;
        let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                fixture.pump(&[route.history], &mut held).await?;
                if fixture.runtimes[0].status(route.history).await?.committed
                    >= accepted.position.index
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("A commits behind held B")??;
        // Inspect the entire pending prefix: A's own later entry and B's entry
        // must each prevent another authority, even if the first entry differs.
        for cell in [&a, &b] {
            anyhow::ensure!(
                matches!(
                    fixture.runtimes[0]
                        .execution_authority(route.history, cell.clone())
                        .await,
                    Err(RuntimeError::NotReady)
                ),
                "pending same-cell mutation admitted execution"
            );
        }
        let untouched = history(&route, "sql-c")?;
        fixture.runtimes[0]
            .execution_authority(route.history, untouched)
            .await
            .context("two other-cell mutations blocked the untouched cell")?;
        Ok::<_, anyhow::Error>((prepared, accepted))
    }
    .await;
    let work = held
        .remove(&route.history)
        .context("held committed B work")?;
    pool.prepare_apply(&applier, work)
        .await?
        .complete(&fixture.runtimes[0])
        .await?;
    let target = prepared_a
        .as_ref()
        .map_or(accepted_b.position.index, |(_, accepted)| {
            accepted.position.index
        });
    fixture.drive(Some(target)).await?;
    let authority_b = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let receipt_b = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let receipt = executor.finish(authority_b)?;
            assert_eq!(receipt.receipt.result(), b"b-pipeline");
            executor.close()?;
            Ok(receipt)
        })?
        .await??;
    let verified = async {
        let (prepared, accepted_a) = prepared_a?;
        let authority_a = fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?;
        let receipt_a = pool.finish(authority_a).await?;
        anyhow::ensure!(
            receipt_a.receipt().receipt.result() == b"a-pipeline"
                && receipt_a.receipt().position == accepted_a.position
        );
        anyhow::ensure!(receipt_b.position == accepted_b.position);
        for runtime in &fixture.runtimes {
            for (cell, version, position) in [
                (
                    &a,
                    &receipt_a.receipt().version,
                    receipt_a.receipt().position,
                ),
                (&b, &receipt_b.version, receipt_b.position),
            ] {
                let proof = runtime
                    .reconciliation_authority(route.history, cell.clone())
                    .await?;
                anyhow::ensure!(
                    &proof.cell().version == version && proof.cell().witness.position == position
                );
            }
        }
        let version = receipt_a.receipt().version.clone();
        let position = receipt_a.receipt().position;
        drop(receipt_a);
        drop(prepared);
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?;
        let replay = pool
            .stage(
                fixture.cell_options(&a).await?,
                authority,
                cell_attempt(216)?,
                false,
                |_| panic!("exact replay must not execute A again"),
            )
            .await?;
        let CellSqlOutcome::Receipt(replay) = replay else {
            anyhow::bail!("missing A replay");
        };
        anyhow::ensure!(
            replay.receipt().receipt.result() == b"a-pipeline"
                && replay.receipt().version == version
                && replay.receipt().position == position
        );
        drop(replay);
        anyhow::ensure!(
            calls.load(Ordering::Acquire) == 1
                && pool.usage()?.active_cells == 0
                && pool.usage()?.retained_operations == 0
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(receipt_b);
    drop(prepared_b);
    drop(pool);
    drop(applier);
    let closed = fixture.close().await;
    verified.with_context(|| {
        format!("unrelated mutation pipeline extents={rooted}; close={closed:?}")
    })?;
    closed
}
