// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact producer preparation must not be replayed again by its local voter.
use super::*;
use futures_util::FutureExt;
use std::sync::Mutex;

#[path = "raft_ltx_fault.rs"]
#[allow(dead_code)]
mod files;

pub(super) async fn reuse(rooted: bool) -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    let disk = root.path().to_path_buf();
    eprintln!(
        "retained prepared-image verification disk: {}",
        disk.display()
    );
    let observed = files::Faults::new(disk.join("node-1/images"), None);
    let filesystem = Arc::new(files::FaultFileSystem {
        faults: observed.clone(),
    });
    let class = RaftClass::new("PreparedImage", 1, [41; 32])?;
    let (mut fixture, route, cell, _) = Box::pin(super::registered::setup_in(
        rooted, &class, root, filesystem,
    ))
    .await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut paths = Vec::new();
        for node in 0..3 {
            let state = fixture.runtimes[node]
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            let path = match state.image() {
                CellImagePin::Materialized(pin) => disk
                    .join(format!("node-{}/images", node + 1))
                    .join(format!("{}.sqlite", hex::encode(pin.id().bytes()))),
                CellImagePin::Extents(pin) => {
                    anyhow::ensure!(pin.manifest().geometry().extent_count() == 1);
                    let extent = pin.manifest().extent(0).context("base extent")?;
                    disk.join(format!("rooted-{node}")).join(format!(
                        "e-{:08x}-{}",
                        extent.length(),
                        hex::encode(extent.bytes())
                    ))
                }
                CellImagePin::Empty => anyhow::bail!("enrolled cell has no physical base"),
            };
            anyhow::ensure!(path.is_file(), "selected base file must exist");
            paths.push(path);
        }
        let watched = Arc::new(Mutex::new(
            paths.iter().cloned().map(|path| (path, 0)).collect(),
        ));
        observed
            .watched_reads
            .set(watched.clone())
            .map_err(|_| anyhow::anyhow!("read observation already installed"))?;
        let leader_seals = || {
            observed
                .image_seals
                .lock()
                .unwrap()
                .iter()
                .filter(|path| path.starts_with(disk.join("node-1/images")))
                .count()
        };
        let before = leader_seals();
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Prepared(command) = pool
            .stage(
                fixture.cell_options(&cell).await?,
                authority,
                cell_attempt(231)?,
                true,
                |tx| {
                    tx.execute("UPDATE value SET n=n+7", [])?;
                    Ok(b"prepared-once".to_vec())
                },
            )
            .await?
        else {
            anyhow::bail!("prepared command")
        };
        let prepared_seals = leader_seals() - before;
        for count in watched.lock().unwrap().values_mut() {
            *count = 0;
        }
        let before_apply = leader_seals();
        pool.mark_submitted(cell.clone(), command.proposal())
            .await?;
        let accepted = accepted_sql(&fixture.runtimes[0], &command).await?;
        fixture.drive(Some(accepted.position.index)).await?;
        let apply_reads: Vec<_> = {
            let reads = watched.lock().unwrap();
            paths.iter().map(|path| reads[path]).collect()
        };
        let apply_seals = leader_seals() - before_apply;
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let receipt = pool.finish(authority).await?;
        anyhow::ensure!(receipt.receipt().receipt.result() == b"prepared-once");
        anyhow::ensure!(receipt.receipt().version.sequence.get() == 2);
        let version = receipt.receipt().version.clone();
        let witness = receipt.receipt().witness;
        anyhow::ensure!(witness.position == accepted.position);
        anyhow::ensure!(witness.proposal == command.proposal());
        drop(receipt);
        // Submission clones must still be inert after exact settlement.
        let replay = accepted_sql(&fixture.runtimes[0], &command).await?;
        anyhow::ensure!(replay.position == accepted.position && replay.digest == accepted.digest);
        drop(command);
        for node in 0..3 {
            let current = fixture.runtimes[node]
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            anyhow::ensure!(current.cell().version == version && current.cell().witness == witness);
            let authority =
                super::reads::selected(&mut fixture, node, &cell, CellReadConsistency::Stale)
                    .await?;
            let output = fixture.appliers[&(node, route.history)]
                .0
                .query(
                    fixture.cell_options_at(&cell, node).await?,
                    authority,
                    |db, version| {
                        anyhow::ensure!(version.sequence.get() == 2);
                        let value: i64 =
                            db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                        Ok(value.to_le_bytes().to_vec())
                    },
                )
                .await?;
            anyhow::ensure!(output.result() == 7_i64.to_le_bytes());
        }
        anyhow::ensure!(pool.usage()?.active_cells == 0);
        anyhow::ensure!(pool.usage()?.retained_operations == 0);
        anyhow::ensure!(pool.usage()?.retained_bytes == 0);
        Ok::<_, anyhow::Error>((prepared_seals, apply_seals, apply_reads))
    })
    .catch_unwind()
    .await;
    drop(pool);
    let closed = fixture.close().await;
    let counts = match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    closed?;
    let (prepared_seals, apply_seals, apply_reads) = counts;
    eprintln!("prepared image work: rooted={rooted} preview_seals={prepared_seals} apply_seals={apply_seals} base_reads={apply_reads:?}");
    if !rooted {
        anyhow::ensure!(
            prepared_seals == 1,
            "the producer must seal its image before submission"
        );
        anyhow::ensure!(
            apply_seals == 0,
            "local apply repeated the producer's completed image seal"
        );
    }
    anyhow::ensure!(
        apply_reads[0] == 0,
        "local apply reread an already independently replayed base"
    );
    anyhow::ensure!(
        apply_reads[1] > 0 && apply_reads[2] > 0,
        "followers must independently replay the committed LTX"
    );
    Ok(())
}
