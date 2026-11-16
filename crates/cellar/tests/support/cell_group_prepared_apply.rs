// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact local preparation custody at the real committed-apply boundary.
use super::*;
use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Clone, Copy)]
pub(crate) enum PreparationCase {
    Live,
    Sealed,
    Expired,
    Transferred,
    Corrupt,
}

fn images(directory: &Path, rooted: bool) -> Result<BTreeSet<PathBuf>> {
    std::fs::read_dir(directory)?
        .filter_map(|entry| match entry {
            Ok(entry) => {
                let path = entry.path();
                let selected = if rooted {
                    entry.file_name().to_string_lossy().starts_with("e-")
                } else {
                    path.extension()
                        .is_some_and(|extension| extension == "sqlite")
                };
                selected.then_some(Ok(path))
            }
            Err(error) => Some(Err(error.into())),
        })
        .collect()
}

fn flip(path: &Path) -> Result<u8> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let mut byte = [0];
    file.read_exact(&mut byte)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&[byte[0] ^ 1])?;
    file.sync_all()?;
    Ok(byte[0])
}

pub(crate) async fn verified_preparation_handoff(
    rooted: bool,
    case: PreparationCase,
) -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root
        .path()
        .join(if rooted { "rooted-0" } else { "node-1/images" });
    let faults = Faults::new(directory.clone(), None);
    let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(FaultFileSystem {
        faults: faults.clone(),
    });
    let cluster =
        Cluster::with_storage(RaftRuntimeOptions::default(), root, filesystem.clone()).await?;
    let cluster = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (mut fixture, route, cell, _) =
        enroll_sql_cells(cluster, rooted, filesystem.clone()).await?;
    let work = async {
        for node in 0..3 {
            super::apply::install(&mut fixture, &cell, node).await?;
        }
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let base = match authority.image() {
            CellImagePin::Materialized(pin) => {
                directory.join(format!("{}.sqlite", hex::encode(pin.id().bytes())))
            }
            CellImagePin::Extents(pin) => {
                anyhow::ensure!(pin.manifest().geometry().extent_count() == 1);
                let extent = pin.manifest().extent(0).context("one base extent")?;
                directory.join(format!(
                    "e-{:08x}-{}",
                    extent.length(),
                    hex::encode(extent.bytes())
                ))
            }
            CellImagePin::Empty => anyhow::bail!("nonempty base required"),
        };
        drop(authority);
        let before = images(&directory, rooted)?;
        let mut executor = fixture.private_cell(&cell).await?;
        let (executor, prepared) = fixture
            .cluster
            .domain
            .blocking(move || -> Result<_> {
                let CellStageOutcome::Prepared(prepared) =
                    executor.stage(cell_attempt(120)?, true, |tx| {
                        tx.execute("UPDATE value SET n=n+1", [])?;
                        Ok(b"exact-preparation".to_vec())
                    })?
                else {
                    anyhow::bail!("fresh preparation required");
                };
                executor.mark_submitted(prepared.proposal())?;
                Ok((executor, prepared))
            })?
            .await??;
        let mut executor = Some(executor);
        let added: Vec<_> = images(&directory, rooted)?
            .difference(&before)
            .cloned()
            .collect();
        anyhow::ensure!(
            added.len() == 1,
            "one independently prepared result required: {added:?}"
        );
        let accepted = accepted_private(&fixture.runtimes[0], &prepared).await?;
        if matches!(case, PreparationCase::Expired) {
            let executor = executor.take().unwrap();
            fixture
                .cluster
                .domain
                .blocking(move || executor.close())?
                .await??;
        }
        let mut held = BTreeMap::new();
        // This external host harness owns the network pump, like apply_images;
        // production work remains on the fixture's execution domain.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                fixture.pump(&[route.history], &mut held).await?;
                if held.contains_key(&route.history) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("committed apply not delivered")??;
        let committed = held
            .remove(&route.history)
            .context("held committed apply")?;
        if matches!(case, PreparationCase::Transferred) {
            let executor = executor.take().unwrap();
            fixture
                .cluster
                .domain
                .blocking(move || executor.close())?
                .await??;
        }
        let prior = committed.base.marker().clone();
        if matches!(case, PreparationCase::Sealed) {
            anyhow::ensure!(!rooted, "materialized seal contract");
            faults.all_directory_files.store(true, Ordering::Release);
        }
        let image_barriers = faults.image_seals.lock().unwrap().len();
        let directory_barriers = faults.events.lock().unwrap().iter()
            .filter(|event| **event == "directory-sync").count();
        let deny = Arc::new(AtomicBool::new(true));
        let denials = Arc::new(AtomicUsize::new(0));
        let denied = denials.clone();
        let armed = deny.clone();
        *faults.read_open.lock().unwrap() = Some(Arc::new(move |path| {
            if armed.load(Ordering::Acquire) && path == base {
                denied.fetch_add(1, Ordering::AcqRel);
                return Err(std::io::Error::other("prepared apply reopened base bytes"));
            }
            Ok(())
        }));
        let (pool, applier) = fixture.appliers[&(0, route.history)].clone();
        if matches!(case, PreparationCase::Corrupt) {
            let original = flip(&added[0])?;
            let result = pool.prepare_apply(&applier, committed.clone()).await;
            anyhow::ensure!(result.is_err(), "corrupt prepared destination was accepted");
            anyhow::ensure!(
                denials.load(Ordering::Acquire) == 0,
                "corruption check replayed the base"
            );
            anyhow::ensure!(
                fixture.cluster.voters[0]
                    .log
                    .cell_group(&route.history)?
                    .group_manifest()?
                    .as_ref()
                    == Some(&prior)
            );
            anyhow::ensure!(flip(&added[0])? == (original ^ 1));
        }
        let result = pool.prepare_apply(&applier, committed.clone()).await;
        let applied = if matches!(case, PreparationCase::Expired) {
            anyhow::ensure!(
                result.is_err() && denials.load(Ordering::Acquire) != 0,
                "expired preparation retained physical authority"
            );
            anyhow::ensure!(
                fixture.cluster.voters[0]
                    .log
                    .cell_group(&route.history)?
                    .group_manifest()?
                    .as_ref()
                    == Some(&prior)
            );
            deny.store(false, Ordering::Release);
            pool.prepare_apply(&applier, committed.clone()).await?
        } else {
            result.context(
                "leader replayed a locally verified base instead of joining its preparation",
            )?
        };
        if matches!(case, PreparationCase::Sealed) {
            anyhow::ensure!(
                faults.image_seals.lock().unwrap().len() == image_barriers
                    && faults.events.lock().unwrap().iter().filter(|event| **event == "directory-sync").count() == directory_barriers,
                "committed apply repeated an already completed local image persistence barrier"
            );
            anyhow::ensure!(
                fixture.cluster.voters[0]
                    .log
                    .cell_group(&route.history)?
                    .group_manifest()?
                    .as_ref()
                    == Some(&prior),
                "physical preparation published an uncommitted root"
            );
        }
        if !matches!(case, PreparationCase::Expired) {
            anyhow::ensure!(
                denials.load(Ordering::Acquire) == 0,
                "verified preparation reread base bytes"
            );
        }
        applied.complete(&fixture.runtimes[0]).await?;
        deny.store(false, Ordering::Release);
        faults.read_open.lock().unwrap().take();
        fixture.drive(Some(accepted.position.index)).await?;
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let expected = authority.cell().version.clone();
        if let Some(mut executor) = executor {
            fixture
                .cluster
                .domain
                .blocking(move || -> Result<()> {
                    anyhow::ensure!(
                        executor.finish(authority)?.receipt.result() == b"exact-preparation"
                    );
                    executor.close()?;
                    Ok(())
                })?
                .await??;
        } else {
            drop(authority);
            let mut executor = fixture.private_cell(&cell).await?;
            fixture
                .cluster
                .domain
                .blocking(move || -> Result<()> {
                    let CellStageOutcome::Duplicate(receipt) =
                        executor.stage(cell_attempt(120)?, false, |_| {
                            panic!("exact replay executed callback");
                        })?
                    else {
                        anyhow::bail!("committed receipt missing");
                    };
                    anyhow::ensure!(receipt.receipt.result() == b"exact-preparation");
                    executor.close()?;
                    Ok(())
                })?
                .await??;
        }
        for runtime in &fixture.runtimes {
            let proof = runtime
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            anyhow::ensure!(proof.cell().version == expected && expected.sequence.get() == 2);
        }
        drop(committed);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    faults.read_open.lock().unwrap().take();
    if work.is_err() {
        let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
        eprintln!("retained prepared apply disk: {}", root.keep().display());
    }
    let cleanup = fixture.close().await;
    work?;
    cleanup
}
