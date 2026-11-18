// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact local preparation custody at the real committed-apply boundary.
use super::apply_faults::physical;
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
    Receipt,
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
        let directory_barriers = faults
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "directory-sync")
            .count();
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
                    && faults
                        .events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|event| **event == "directory-sync")
                        .count()
                        == directory_barriers,
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
            let receipt_reads = Arc::new(AtomicUsize::new(0));
            if matches!(case, PreparationCase::Receipt) {
                let prepared_image = added[0].clone();
                let reads = receipt_reads.clone();
                // Every replica has durably applied the exact proposal. Only
                // final settlement is left: its frozen writer must not reopen
                // the image that it already prepared and the owner proved.
                *faults.read_open.lock().unwrap() = Some(Arc::new(move |path| {
                    if path == prepared_image {
                        reads.fetch_add(1, Ordering::AcqRel);
                        return Err(std::io::Error::other(
                            "final receipt reopened its exact prepared image",
                        ));
                    }
                    Ok(())
                }));
            }
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
            if matches!(case, PreparationCase::Receipt) {
                anyhow::ensure!(
                    receipt_reads.load(Ordering::Acquire) == 0,
                    "final settlement reread its own prepared bytes"
                );
                faults.read_open.lock().unwrap().take();
                // Independent replay still reads the committed image and must
                // recover the exact result without invoking business code.
                let mut replay = fixture.private_cell(&cell).await?;
                fixture
                    .cluster
                    .domain
                    .blocking(move || -> Result<()> {
                        let CellStageOutcome::Duplicate(receipt) =
                            replay.stage(cell_attempt(120)?, false, |_| {
                                panic!("final receipt replay executed callback");
                            })?
                        else {
                            anyhow::bail!("independently committed final receipt missing");
                        };
                        anyhow::ensure!(receipt.receipt.result() == b"exact-preparation");
                        replay.close()?;
                        Ok(())
                    })?
                    .await??;
            }
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

pub(crate) async fn owned_image_recheck_guards() -> Result<()> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("node-1/images");
    let faults = Faults::new(directory.clone(), None);
    let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(FaultFileSystem {
        faults: faults.clone(),
    });
    let cluster =
        Cluster::with_storage(RaftRuntimeOptions::default(), root, filesystem.clone()).await?;
    let cluster = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (fixture, route, cell, _) = enroll_sql_cells(cluster, false, filesystem).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, cell)
        .await?;
    let CellImagePin::Materialized(pin) = authority.image().clone() else {
        anyhow::bail!("materialized image")
    };
    drop(authority);
    let local = fixture.cluster.voters[0].images.clone();
    let foreign = fixture.cluster.voters[1].images.clone();
    let observed = faults.clone();
    let work = fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            observed.all_directory_files.store(true, Ordering::Release);
            let barriers = observed.image_seals.lock().unwrap().len();
            observed
                .mode
                .store(physical::FAIL_IMAGE_SYNC, Ordering::Release);
            local.recheck_owned(&pin)?;
            anyhow::ensure!(observed.image_seals.lock().unwrap().len() == barriers);
            anyhow::ensure!(
                observed.mode.load(Ordering::Acquire) == physical::FAIL_IMAGE_SYNC,
                "warm owned recheck repeated its image barrier"
            );
            anyhow::ensure!(
                matches!(
                    foreign.recheck_owned(&pin),
                    Err(cellar_ltx::generation::GenerationError::Identity)
                ),
                "foreign inventory borrowed a local persistence proof"
            );
            anyhow::ensure!(
                pin.verify_and_sync().is_err(),
                "independent verification skipped its barrier"
            );
            anyhow::ensure!(observed.mode.load(Ordering::Acquire) == 0);

            // Failed verification must revoke the reusable observation, including
            // when the full file barrier succeeds but its directory barrier fails.
            observed
                .mode
                .store(physical::FAIL_DIRECTORY_SYNC, Ordering::Release);
            anyhow::ensure!(local.recheck_owned(&pin).is_err());
            anyhow::ensure!(observed.mode.load(Ordering::Acquire) == 0);
            observed
                .mode
                .store(physical::FAIL_IMAGE_SYNC, Ordering::Release);
            anyhow::ensure!(
                local.adopt(pin.id(), pin.state()).is_err(),
                "warm adoption borrowed an invalidated persistence proof"
            );
            anyhow::ensure!(observed.mode.load(Ordering::Acquire) == 0);
            local.recheck_owned(&pin)?;

            let path = directory.join(format!("{}.sqlite", hex::encode(pin.id().bytes())));
            let before = flip(&path)?;
            anyhow::ensure!(
                local.recheck_owned(&pin).is_err(),
                "warm pin hid corrupt image bytes"
            );
            anyhow::ensure!(flip(&path)? == (before ^ 1));
            observed
                .mode
                .store(physical::FAIL_IMAGE_SYNC, Ordering::Release);
            anyhow::ensure!(
                local.recheck_owned(&pin).is_err(),
                "byte corruption did not invalidate the persistence proof"
            );
            anyhow::ensure!(observed.mode.load(Ordering::Acquire) == 0);
            local.recheck_owned(&pin)?;

            let cold = tempfile::tempdir()?;
            let cold_directory = cold.path().join("images");
            std::fs::create_dir_all(&cold_directory)?;
            std::fs::copy(
                &path,
                cold_directory.join(path.file_name().context("image filename")?),
            )?;
            let cold_faults = Faults::new(cold_directory.clone(), None);
            let cold_store = cellar_ltx::generation::GenerationStore::open(
                cold_directory,
                Arc::new(FaultFileSystem {
                    faults: cold_faults.clone(),
                }),
                Default::default(),
            )?;
            cold_faults
                .all_directory_files
                .store(true, Ordering::Release);
            cold_faults
                .mode
                .store(physical::FAIL_IMAGE_SYNC, Ordering::Release);
            anyhow::ensure!(
                cold_store.adopt(pin.id(), pin.state()).is_err(),
                "cold image borrowed the previous inventory's barrier"
            );
            anyhow::ensure!(
                cold_faults.mode.load(Ordering::Acquire) == 0,
                "cold adoption did not attempt image sync"
            );
            Ok(())
        })?
        .await?;
    faults.mode.store(0, Ordering::Release);
    let cleanup = fixture.close().await;
    work?;
    cleanup
}
