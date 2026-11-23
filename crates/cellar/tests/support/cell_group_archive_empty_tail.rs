// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compare an empty tail with its ordinary independent checkpoint receiver.
use super::*;
use futures_util::FutureExt;

use super::runtime_fixture as files;

pub(super) async fn qualify(rooted: bool) -> Result<()> {
    let placeholder = tempfile::tempdir()?;
    let (mut cluster, expected) =
        checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
    let result = std::panic::AssertUnwindSafe(run(&cluster, &expected, rooted))
        .catch_unwind()
        .await;
    let result = match result {
        Ok(result) => result,
        Err(_) => Err(anyhow::anyhow!("empty-tail qualification panicked")),
    };
    let disk = std::mem::replace(&mut cluster._root, placeholder);
    let closed = cluster.close().await;
    if result.is_err() || closed.is_err() {
        eprintln!("retained failed empty-tail disk: {}", disk.keep().display());
    }
    result.and(closed)
}

async fn run(
    cluster: &Cluster,
    expected: &std::collections::BTreeMap<String, Vec<u8>>,
    rooted: bool,
) -> Result<()> {
    let remote = archive_store::Store::new();
    let archive = new_archive(
        cluster,
        remote.clone(),
        Path::from("empty-tail"),
        Default::default(),
    )
    .await?;
    let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let checkpoint = archive
        .upload_checkpoint(&cluster.domain, source(&sources, cluster)?)
        .await?;
    let group = cluster.voters[0]
        .log
        .cell_group(&cluster.birth.route().history)?;
    let current = cluster.voters[0].owner.as_ref().unwrap().manifest().clone();
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let tail = archive
        .upload_tail(&cluster.domain, &tails, &checkpoint, &group, &current)
        .await?;
    let (tail_path, index) = tail_fixture::tail_index(remote.as_ref(), &tail).await?;
    assert_eq!(
        index["entries"].as_array().context("tail entries")?.len(),
        0
    );
    let directory =
        cluster
            ._root
            .path()
            .join("archive-data")
            .join(if rooted { "extents" } else { "images" });
    let files = files::Faults::new(directory, None);
    let destination = destination_with_filesystem(
        cluster,
        Arc::new(files::FaultFileSystem {
            faults: files.clone(),
        }),
    )
    .await?;
    let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    // Warm the same destination first. Both measured paths still fetch every
    // remote object and independently verify all incoming and existing bytes.
    drop(
        archive
            .verify_checkpoint(
                &cluster.domain,
                &checkpoint,
                receiver(&receivers, &destination, cluster)?,
            )
            .await?,
    );
    files.reads.store(0, Ordering::Release);
    let received = archive
        .verify_checkpoint(
            &cluster.domain,
            &checkpoint,
            receiver(&receivers, &destination, cluster)?,
        )
        .await?;
    let checkpoint_reads = files.reads.load(Ordering::Acquire);
    assert!(
        checkpoint_reads > 0,
        "checkpoint did not inspect physical bytes"
    );
    // Tail recovery additionally inspects SQLite for external dependencies.
    // Independently establish their absence through the ordinary immutable
    // query adapter, including that required work in this fixture's control.
    let manifest = received.checkpoint().manifest().clone();
    let images = destination.apply_images.clone();
    let cells: Vec<_> = expected
        .iter()
        .map(|(cell, bytes)| (cell.clone(), bytes.is_empty()))
        .collect();
    cluster
        .domain
        .blocking(move || -> Result<()> {
            for (cell, empty) in cells {
                let (_, pin) = manifest
                    .pin_cell(&CellId::new(&cell)?)
                    .context("control image")?;
                assert_eq!(matches!(&pin, CellImagePin::Empty), empty);
                let db = match (&images, pin) {
                    (_, CellImagePin::Empty) => continue,
                    (
                        CellGroupApplyImages::Materialized { services, .. },
                        CellImagePin::Materialized(pin),
                    ) => cellar::storage::open_immutable_generation(
                        pin,
                        cellar::storage::ImmutableQueryOptions {
                            services: services.clone(),
                            sqlite_vec: false,
                        },
                    )?,
                    (
                        CellGroupApplyImages::Extents {
                            services,
                            cache_directory,
                            cache_pool,
                            cache,
                            ..
                        },
                        CellImagePin::Extents(pin),
                    ) => cellar::storage::open_immutable_extent_image(
                        pin.view().image(
                            cache_directory.clone(),
                            cache_pool.clone(),
                            *cache,
                        )?,
                        cellar::storage::ImmutableQueryOptions {
                            services: services.clone(),
                            sqlite_vec: false,
                        },
                    )?,
                    _ => anyhow::bail!("control image representation"),
                };
                let tables: i64 = db.connection().query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN('__sys_kv','__sys_raft_payload_dependencies')",
                    [],
                    |row| row.get(0),
                )?;
                assert_eq!(tables, 0, "fixture has unexpected external dependencies");
            }
            Ok(())
        })?
        .await??;
    let control_reads = files.reads.swap(0, Ordering::AcqRel);
    assert!(
        control_reads > checkpoint_reads,
        "control omitted payload inspection"
    );
    drop(received);
    let recovered = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &tail,
            receiver(&receivers, &destination, cluster)?,
        )
        .await?;
    let tail_reads = files.reads.load(Ordering::Acquire);
    assert_eq!(recovered.manifest().marker(), current.marker());
    assert_eq!(tails.usage().operations, 1);
    tail_fixture::check_images(cluster, recovered.manifest(), expected).await?;
    let (_, pin) = recovered
        .manifest()
        .pin_cell(&CellId::new("alpha")?)
        .context("received image")?;
    drop(recovered);
    assert_eq!(tails.usage().operations, 0);
    assert_eq!(receivers.usage().sources, 0);
    // An empty tail still has to agree with the exact received cut. Rehashing
    // a different, structurally valid root cannot promote that checkpoint.
    let mut forged = index.clone();
    let digest = forged["cut"]["marker"]["root"]["digest"]
        .as_array_mut()
        .context("cut root digest")?;
    digest[0] = serde_json::json!(digest[0].as_u64().context("digest byte")? ^ 1);
    let forged = tail_fixture::write_tail(remote.as_ref(), &tail_path, &forged).await?;
    assert!(matches!(
        archive
            .verify_tail(
                &cluster.domain,
                &tails,
                &forged,
                receiver(&receivers, &destination, cluster)?
            )
            .await,
        Err(CellGroupArchiveError::Invalid("replayed complete cut"))
    ));
    // Cached local pins cannot hide damaged bytes on the next independent
    // receiver. Restore only this owned fixture, then require a complete replay.
    let physical = match pin {
        CellImagePin::Materialized(pin) => cluster
            ._root
            .path()
            .join("archive-data/images")
            .join(format!("{}.sqlite", hex::encode(pin.id().bytes()))),
        CellImagePin::Extents(pin) => {
            let id = pin.manifest().extent(0).context("received extent")?;
            cluster
                ._root
                .path()
                .join("archive-data/extents")
                .join(format!("e-{:08x}-{}", id.length(), hex::encode(id.bytes())))
        }
        CellImagePin::Empty => anyhow::bail!("expected populated received image"),
    };
    let original = std::fs::read(&physical)?;
    let mut damaged = original.clone();
    damaged[100] ^= 0x80;
    std::fs::write(&physical, damaged)?;
    assert!(archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &tail,
            receiver(&receivers, &destination, cluster)?
        )
        .await
        .is_err());
    std::fs::write(physical, original)?;
    let restored = archive
        .verify_tail(
            &cluster.domain,
            &tails,
            &tail,
            receiver(&receivers, &destination, cluster)?,
        )
        .await?;
    tail_fixture::check_images(cluster, restored.manifest(), expected).await?;
    drop(restored);
    assert_eq!(tails.usage().operations, 0);
    assert_eq!(receivers.usage().sources, 0);
    eprintln!("empty-tail physical reads: rooted={rooted}, checkpoint={checkpoint_reads}, control={control_reads}, tail={tail_reads}");
    assert_eq!(
        tail_reads, control_reads,
        "empty tail repeated the fresh checkpoint's physical verification"
    );
    Ok(())
}
