// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-06 checkpoint and committed-tail data recovery. Archive-head protection,
//! S3 settlement, reclamation and fresh-incarnation recovery remain separate.
#![allow(clippy::disallowed_methods)]
// The shared engine fixture also defines fault/membership helpers selected by
// sibling integration targets and are not all selected by this archive gate.
#![allow(dead_code, unused_imports)]

#[path = "support/cell_group_archive_store.rs"]
mod archive_store;
#[path = "support/cell_group_checkpoint.rs"]
#[allow(dead_code)]
mod checkpoint_fixture;
#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;
#[path = "support/cell_group_archive_isolation.rs"]
mod isolation_fixture;
#[path = "support/cell_group_archive_publication.rs"]
mod publication_fixture;
#[path = "support/cell_group_archive_tail.rs"]
mod tail_fixture;

include!("support/cell_group_engine_fixture.rs");

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_tails_cover_enrollment_and_controls() -> Result<()> {
    for rooted in [false, true] {
        tail_fixture::enrollment_controls(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_tail_missing_and_forged_dependencies_refuse() -> Result<()> {
    for rooted in [false, true] {
        tail_fixture::dependencies(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_tail_warm_ltx_is_independently_replayed() -> Result<()> {
    for rooted in [false, true] {
        tail_fixture::warm_replay(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_tail_cancelled_replay_retains_physical_owners() -> Result<()> {
    tail_fixture::physical_cancellation().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_tail_extension_survives_local_prefix_compaction() -> Result<()> {
    for rooted in [false, true] {
        tail_fixture::extend_compacted(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_data_recovers_after_all_voter_disks_erased() -> Result<()> {
    for rooted in [false, true] {
        isolation_fixture::erased_voters(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_verification_isolated_from_serving_cut() -> Result<()> {
    for rooted in [false, true] {
        isolation_fixture::moving_cut(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_head_requires_complete_committed_source() -> Result<()> {
    for rooted in [false, true] {
        publication_fixture::complete_source_cut(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_gc_retains_holds_and_pending_parents() -> Result<()> {
    for rooted in [false, true] {
        publication_fixture::holds_and_pending_parents(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_publication_reconciles_exact_uncertain_intent() -> Result<()> {
    for rooted in [false, true] {
        publication_fixture::uncertain_publication(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_delayed_delete_cannot_erase_new_generation() -> Result<()> {
    for rooted in [false, true] {
        publication_fixture::delayed_delete(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_registry_refuses_reset_and_expired_intents() -> Result<()> {
    for rooted in [false, true] {
        publication_fixture::registry_identity_and_intents(rooted).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn archive_upload_owners_bound_source_witness_lifetimes() -> Result<()> {
    for rooted in [false, true] {
        publication_fixture::source_owner_lifetimes(rooted).await?;
    }
    Ok(())
}

use cellar_ltx::{
    generation::ReadServices,
    internal::{CacheLimits, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
};
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use std::sync::atomic::Ordering;

struct ArchiveDestination {
    store: CellGroupArchiveStore,
    images: GenerationStore,
}

async fn destination(cluster: &Cluster) -> Result<ArchiveDestination> {
    destination_with_filesystem(cluster, Arc::new(DirectFileSystem)).await
}

async fn destination_with_filesystem(
    cluster: &Cluster,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<ArchiveDestination> {
    let rooted = cluster
        .voters
        .first()
        .is_some_and(|voter| voter.extents.is_some());
    destination_mode(cluster, rooted, filesystem).await
}

async fn destination_mode(
    cluster: &Cluster,
    rooted: bool,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<ArchiveDestination> {
    let base = cluster._root.path().join("archive-data");
    let route = cluster.birth.route().clone();
    cluster
        .domain
        .blocking(move || {
            let pages = ManifestPageStore::open(
                &base.join("pages"),
                filesystem.clone(),
                1024,
                16 * 1024 * 1024,
                16,
            )?;
            let generations =
                GenerationStore::open(base.join("images"), filesystem.clone(), Default::default())?;
            let services = ReadServices {
                now_unix_millis: Arc::new(|| 10),
                fill_random: Arc::new(|bytes| bytes.fill(17)),
            };
            let images = if rooted {
                CellGroupApplyImages::Extents {
                    images: cellar_ltx::internal::NodeExtentStore::open(
                        base.join("extents"),
                        filesystem,
                        Default::default(),
                    )?,
                    services,
                    limits: Default::default(),
                    cache_directory: base.join("cache"),
                    cache_pool: ScratchPool::new(ScratchLimits {
                        max_bytes: 32 * 1024 * 1024,
                        max_files: 1,
                    })?,
                    cache: CacheLimits {
                        max_cached_bytes: EXTENT_BYTES,
                        max_cached_extents: 1,
                    },
                }
            } else {
                CellGroupApplyImages::Materialized {
                    images: generations.clone(),
                    services,
                }
            };
            let store = CellGroupArchiveStore::new(
                route,
                pages,
                images,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )?;
            Ok(ArchiveDestination {
                store,
                images: generations,
            })
        })?
        .await?
}

fn receiver(
    pool: &CellGroupCheckpointPool,
    destination: &ArchiveDestination,
    cluster: &Cluster,
) -> Result<CellGroupArchiveCheckpointReceiver> {
    Ok(pool.receive_archive(
        destination.store.clone(),
        ScratchPool::new(ScratchLimits {
            max_bytes: 32 * 1024 * 1024,
            max_files: 1,
        })?,
        cluster._root.path().into(),
    )?)
}

fn source(pool: &CellGroupCheckpointPool, cluster: &Cluster) -> Result<CellGroupCheckpoint> {
    Ok(pool.capture(
        &cluster.voters[0]
            .log
            .cell_group(&cluster.birth.route().history)?,
        cluster.voters[0].owner.as_ref().unwrap().manifest(),
    )?)
}

async fn new_archive(
    cluster: &Cluster,
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    limits: CellGroupArchiveLimits,
) -> Result<CellGroupArchiveUpload> {
    let label = prefix.as_ref().to_owned();
    let archive = CellGroupArchivePool::new(limits)?.for_group(
        store,
        prefix,
        cluster.birth.route().history,
    )?;
    start_upload(cluster, &archive, &label).await
}

async fn persist_archive_intent<T: serde::Serialize + serde::de::DeserializeOwned>(
    cluster: &Cluster,
    intent: &T,
) -> Result<T> {
    let bytes = serde_json::to_vec(intent)?;
    let name = hex::encode(Sha256::digest(&bytes));
    let directory = cluster._root.path().join("archive-intents");
    let loaded = cluster
        .domain
        .blocking(move || -> Result<Vec<u8>> {
            use std::io::Write;
            std::fs::create_dir_all(&directory)?;
            let path = directory.join(name);
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::File::open(&directory)?.sync_all()?;
            Ok(std::fs::read(path)?)
        })?
        .await??;
    Ok(serde_json::from_slice(&loaded)?)
}

async fn start_upload(
    cluster: &Cluster,
    archive: &CellGroupArchive,
    label: &str,
) -> Result<CellGroupArchiveUpload> {
    let binding = cluster._root.path().join(format!(
        "archive-binding-{}.json",
        hex::encode(Sha256::digest(label.as_bytes()))
    ));
    let existing = cluster
        .domain
        .blocking({
            let binding = binding.clone();
            move || std::fs::read(binding)
        })?
        .await?;
    let registry = match existing {
        Ok(bytes) => {
            let creation: CellGroupArchiveCreation = serde_json::from_slice(&bytes)?;
            archive.open_registry(&cluster.domain, &creation).await?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let prepared = archive
                .prepare_registry_creation(&cluster.domain, cluster.birth.route().clone())
                .await?;
            let creation = persist_archive_intent(cluster, prepared.intent()).await?;
            assert_eq!(prepared.intent(), &creation);
            let bytes = serde_json::to_vec(&creation)?;
            cluster
                .domain
                .blocking(move || -> Result<()> {
                    use std::io::Write;
                    let mut file = std::fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&binding)?;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    std::fs::File::open(binding.parent().context("binding directory")?)?
                        .sync_all()?;
                    Ok(())
                })?
                .await??;
            prepared.commit(&cluster.domain).await?
        }
        Err(error) => return Err(error.into()),
    };
    let intent = registry.prepare_upload(&cluster.domain).await?;
    let intent = persist_archive_intent(cluster, &intent).await?;
    let ticket = registry.reserve(&cluster.domain, &intent).await?;
    let owners = CellGroupArchiveUploadPool::new(Default::default())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    Ok(registry
        .open_upload(&cluster.domain, &owners, &ticket, &tails)
        .await?)
}

fn registry_id(path: &Path) -> Result<[u8; 32]> {
    let relative = path
        .as_ref()
        .split_once("/registries/")
        .context("registry path")?
        .1;
    let id = relative.split('/').next().context("registry ID")?;
    let mut registry = [0; 32];
    hex::decode_to_slice(id, &mut registry)?;
    Ok(registry)
}

fn generation_prefix(path: &Path) -> Result<(&str, u64)> {
    let (prefix, relative) = path
        .as_ref()
        .split_once("/generations/")
        .context("generation prefix")?;
    let generation = relative.split('/').next().context("generation")?.parse()?;
    Ok((prefix, generation))
}

async fn index(
    store: &dyn ObjectStore,
    reference: &CellGroupArchiveReference,
) -> Result<(Path, serde_json::Value)> {
    let suffix = format!(
        "/indexes/{}-{}",
        reference.length(),
        hex::encode(reference.digest())
    );
    let objects: Vec<_> = store.list(None).try_collect().await?;
    let path = objects
        .into_iter()
        .find(|m| {
            m.location.as_ref().ends_with(&suffix)
                && generation_prefix(&m.location)
                    .is_ok_and(|(_, generation)| generation == reference.generation())
                && registry_id(&m.location).is_ok_and(|id| id == reference.registry())
        })
        .context("checkpoint index object")?
        .location;
    let bytes = store.get(&path).await?.bytes().await?;
    Ok((path, serde_json::from_slice(&bytes)?))
}

fn cell_objects(index_path: &Path, index: &serde_json::Value, cell: &str) -> Vec<Path> {
    let (prefix, _) = generation_prefix(index_path).unwrap();
    index["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["part"]["cell"] == cell)
        .map(|chunk| {
            let digest: Vec<u8> =
                serde_json::from_value(chunk["object"]["digest"].clone()).unwrap();
            Path::from(format!(
                "{prefix}/generations/{}/chunks/{}-{}",
                chunk["object"]["generation"],
                chunk["object"]["length"],
                hex::encode(digest)
            ))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_archive_roundtrip_and_reuse() -> Result<()> {
    for rooted in [false, true] {
        let (mut cluster, mut expected) =
            checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
        let store = archive_store::Store::new();
        let archive = new_archive(
            &cluster,
            store.clone(),
            Path::from("qualification"),
            Default::default(),
        )
        .await?;
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        store.lose_put_response.store(true, Ordering::Release);
        let original = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let count = store.puts.load(Ordering::Acquire);
        let repeat = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        assert_eq!(repeat, original);
        assert_eq!(
            store.puts.load(Ordering::Acquire),
            count,
            "unchanged cut must not PUT again"
        );
        let (before_path, before_index) = index(store.as_ref(), &original).await?;
        checkpoint_fixture::advance_partition(
            &mut cluster,
            "alpha",
            &expected["alpha"],
            rooted,
            Some(2),
        )
        .await?;
        expected.insert(
            "alpha".into(),
            images::image(Some(&expected["alpha"]), "UPDATE value SET n=n+1")?,
        );
        let latest = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let (after_path, after_index) = index(store.as_ref(), &latest).await?;
        for cell in ["beta", "cell-000"] {
            let objects = cell_objects(&before_path, &before_index, cell);
            assert!(!objects.is_empty());
            assert_eq!(objects, cell_objects(&after_path, &after_index, cell));
        }
        assert_ne!(original, latest);
        let marker = cluster.voters[0]
            .owner
            .as_ref()
            .unwrap()
            .manifest()
            .marker()
            .clone();
        let applier = destination(&cluster).await?;
        let receive_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        // Prove the independent receiver uses remote data after its source engine
        // has closed; no borrowed source pin can satisfy this recovery.
        drop(cluster.voters[0].owner.take());
        cluster.voters[0].log.close().await?;
        let verified = archive
            .verify_checkpoint(
                &cluster.domain,
                &latest,
                receiver(&receive_pool, &applier, &cluster)?,
            )
            .await?;
        assert_eq!(verified.reference(), &latest);
        assert_eq!(verified.checkpoint().marker(), &marker);
        assert_eq!(archive.usage().operations, 0);
        assert_eq!(sources.usage().sources, 0);
        assert_eq!(receive_pool.usage().sources, 1);
        for (cell, wanted) in expected {
            let (_, pin) = verified
                .checkpoint()
                .manifest()
                .pin_cell(&CellId::new(cell)?)
                .context("recovered cell")?;
            let bytes = cluster
                .domain
                .blocking(move || -> Result<Vec<u8>> {
                    Ok(match pin {
                        CellImagePin::Empty => Vec::new(),
                        CellImagePin::Materialized(pin) => pin
                            .open_reader()?
                            .read_exact_at(0, pin.state().length as usize)?,
                        CellImagePin::Extents(pin) => {
                            let mut bytes = Vec::new();
                            for n in 0..pin.manifest().geometry().extent_count() {
                                bytes
                                    .extend(pin.view().read_extent(
                                        pin.manifest().extent(n).context("extent")?,
                                    )?);
                            }
                            bytes
                        }
                    })
                })?
                .await??;
            assert_eq!(bytes, wanted);
        }
        drop(verified);
        assert_eq!(receive_pool.usage().sources, 0);
        cluster.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_archive_limits_and_cancelled_receiver() -> Result<()> {
    for rooted in [false, true] {
        let (cluster, _) =
            checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
        let store = archive_store::Store::new();
        let history = cluster.birth.route().history;
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        // Chunk and encoded-index bounds are independent, and neither may leave
        // a selected index or a blocked physical producer behind on failure.
        for limits in [
            CellGroupArchiveLimits {
                max_chunks: 1,
                ..Default::default()
            },
            CellGroupArchiveLimits {
                max_index_bytes: 1024,
                ..Default::default()
            },
        ] {
            let limited =
                new_archive(&cluster, store.clone(), Path::from("limited"), limits).await?;
            assert!(matches!(
                limited
                    .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
                    .await,
                Err(CellGroupArchiveError::Limit)
            ));
            assert_eq!(limited.usage().operations, 0);
            assert_eq!(sources.usage().sources, 0);
            let objects: Vec<_> = store
                .list(Some(&Path::from("limited")))
                .try_collect()
                .await?;
            assert!(!objects
                .iter()
                .any(|m| m.location.as_ref().contains("/indexes/")));
        }
        let node_pool = CellGroupArchivePool::new(CellGroupArchiveLimits {
            max_operations: 1,
            ..Default::default()
        })?;
        let archive = node_pool.for_group(store.clone(), Path::from("qualification"), history)?;
        let other = node_pool.for_group(
            store.clone(),
            Path::from("qualification"),
            GroupHistory {
                group: GroupId::new([81; 16])?,
                ..history
            },
        )?;
        let archive = start_upload(&cluster, &archive, "qualification").await?;
        let reference = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let (path, encoded) = index(store.as_ref(), &reference).await?;
        let dependency = cell_objects(&path, &encoded, "beta")
            .into_iter()
            .last()
            .context("held beta")?;
        let applier = destination(&cluster).await?;
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let gate = store.hold(dependency);
        let mut pending = Box::pin(archive.verify_checkpoint(
            &cluster.domain,
            &reference,
            receiver(&pool, &applier, &cluster)?,
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending)
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        assert_eq!(pool.usage().sources, 1);
        assert_eq!(archive.usage().operations, 1);
        let charged = archive.usage().retained_bytes;
        assert!(charged > 0 && charged <= CellGroupArchiveLimits::default().max_retained_bytes);
        // Node admission is shared by clones and both directions. Refusal must
        // drop the rejected source without borrowing the active job's charge.
        assert!(matches!(
            archive
                .clone()
                .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
                .await,
            Err(CellGroupArchiveError::Busy)
        ));
        assert_eq!(archive.usage().retained_bytes, charged);
        assert_eq!(node_pool.usage().operations, 1);
        let other_receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        assert!(matches!(
            other
                .verify_checkpoint(
                    &cluster.domain,
                    &reference,
                    receiver(&other_receivers, &applier, &cluster)?
                )
                .await,
            Err(CellGroupArchiveError::Busy)
        ));
        assert_eq!(other_receivers.usage().sources, 0);
        assert_eq!(other.usage().retained_bytes, charged);
        assert_eq!(sources.usage().sources, 0);
        drop(pending);
        tokio::time::timeout(Duration::from_secs(5), async {
            while archive.usage().operations != 0 || pool.usage().sources != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        store.clear();
        gate.release.notify_one();
        let verified = archive
            .verify_checkpoint(
                &cluster.domain,
                &reference,
                receiver(&pool, &applier, &cluster)?,
            )
            .await?;
        drop(verified);
        // A blocked GET is bounded by the configured complete-object deadline.
        let gate = store.hold(path);
        let timed = new_archive(
            &cluster,
            store.clone(),
            Path::from("qualification"),
            CellGroupArchiveLimits {
                io_timeout_ms: 20,
                ..Default::default()
            },
        )
        .await?;
        assert!(matches!(
            timed
                .verify_checkpoint(
                    &cluster.domain,
                    &reference,
                    receiver(&pool, &applier, &cluster)?
                )
                .await,
            Err(CellGroupArchiveError::Timeout)
        ));
        assert_eq!(timed.usage().operations, 0);
        assert_eq!(pool.usage().sources, 0);
        store.clear();
        gate.release.notify_one();
        cluster.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_archive_missing_or_corrupt_cell_refuses() -> Result<()> {
    for rooted in [false, true] {
        let (mut cluster, expected) =
            checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
        checkpoint_fixture::advance_partition(
            &mut cluster,
            "alpha",
            &expected["alpha"],
            rooted,
            Some(2),
        )
        .await?;
        let store = archive_store::Store::new();
        let archive = new_archive(
            &cluster,
            store.clone(),
            Path::from("qualification"),
            Default::default(),
        )
        .await?;
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let reference = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let (path, encoded) = index(store.as_ref(), &reference).await?;
        let dependency = cell_objects(&path, &encoded, "beta")
            .into_iter()
            .last()
            .context("beta dependency")?;
        let bytes = store.get(&dependency).await?.bytes().await?;
        let applier = destination(&cluster).await?;
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        store.delete(&dependency).await?;
        assert!(matches!(
            archive
                .verify_checkpoint(
                    &cluster.domain,
                    &reference,
                    receiver(&pool, &applier, &cluster)?
                )
                .await,
            Err(CellGroupArchiveError::Invalid("missing archive object"))
        ));
        assert_eq!(pool.usage().sources, 0);
        let mut corrupt = bytes.to_vec();
        corrupt[0] ^= 0x80;
        store.inner.put(&dependency, corrupt.into()).await?;
        assert!(matches!(
            archive
                .verify_checkpoint(
                    &cluster.domain,
                    &reference,
                    receiver(&pool, &applier, &cluster)?
                )
                .await,
            Err(CellGroupArchiveError::Invalid("object digest/length"))
        ));
        assert_eq!(pool.usage().sources, 0);
        // Existing corrupt immutable payloads cannot be silently overwritten.
        assert!(archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await
            .is_err());
        store.inner.put(&dependency, bytes.into()).await?;
        let verified = archive
            .verify_checkpoint(
                &cluster.domain,
                &reference,
                receiver(&pool, &applier, &cluster)?,
            )
            .await?;
        assert_eq!(verified.checkpoint().marker().root().cells(), 4);
        drop(verified);
        assert_eq!(archive.usage().retained_bytes, 0);
        cluster.close().await?;
    }
    Ok(())
}

async fn forged_index(
    store: &dyn ObjectStore,
    original_path: &Path,
    value: &serde_json::Value,
) -> Result<CellGroupArchiveReference> {
    let bytes = serde_json::to_vec(value)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let reference = serde_json::from_value(serde_json::json!({
        "registry": registry_id(original_path)?, "generation": generation_prefix(original_path)?.1, "digest": digest, "length": bytes.len(),
    }))?;
    let prefix = original_path
        .as_ref()
        .split("/indexes/")
        .next()
        .context("index prefix")?;
    let path = Path::from(format!(
        "{prefix}/indexes/{}-{}",
        bytes.len(),
        hex::encode(digest)
    ));
    store.put(&path, bytes.into()).await?;
    Ok(reference)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn checkpoint_archive_untrusted_index_and_cut_binding() -> Result<()> {
    for rooted in [false, true] {
        let (cluster, _) =
            checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
        let store = archive_store::Store::new();
        let archive = new_archive(
            &cluster,
            store.clone(),
            Path::from("qualification"),
            Default::default(),
        )
        .await?;
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let reference = archive
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let (path, original) = index(store.as_ref(), &reference).await?;
        let group = cluster.voters[2]
            .log
            .cell_group(&cluster.birth.route().history)?;
        let base = group.group_manifest()?.context("destination base")?;
        let applier = destination(&cluster).await?;
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        for attack in 0..4 {
            let mut value = original.clone();
            match attack {
                0 => value["domain"] = "cellar-single-cell-archive-1".into(),
                1 => value["chunks"][1]["offset"] = 1.into(),
                2 => {
                    let chunks = value["chunks"].as_array_mut().unwrap();
                    chunks.retain(|chunk| chunk["part"]["cell"] != "beta");
                    value["bytes"] = chunks
                        .iter()
                        .map(|c| c["object"]["length"].as_u64().unwrap())
                        .sum::<u64>()
                        .into();
                }
                3 => value["marker"] = serde_json::to_value(&base)?,
                _ => unreachable!(),
            }
            let forged = forged_index(store.as_ref(), &path, &value).await?;
            let result = archive
                .verify_checkpoint(
                    &cluster.domain,
                    &forged,
                    receiver(&pool, &applier, &cluster)?,
                )
                .await;
            match attack {
                0 => assert!(matches!(
                    result,
                    Err(CellGroupArchiveError::Invalid("checkpoint domain/history"))
                )),
                1 => assert!(matches!(result, Err(CellGroupArchiveError::Invalid(_)))),
                2 => assert!(matches!(
                    result,
                    Err(CellGroupArchiveError::Checkpoint(_))
                        | Err(CellGroupArchiveError::Invalid("receiver refused archive"))
                )),
                3 => assert!(matches!(
                    result,
                    Err(CellGroupArchiveError::Invalid("archive checkpoint binding"))
                )),
                _ => unreachable!(),
            }
            assert_eq!(archive.usage().operations, 0);
            assert_eq!(pool.usage().sources, 0);
            assert_eq!(group.group_manifest()?.as_ref(), Some(&base));
            assert_eq!(group.checkpoint_manifest()?, None);
        }
        for length in [0, u64::MAX] {
            let invalid = serde_json::from_value(
                serde_json::json!({"registry": reference.registry(), "generation": reference.generation(), "digest": reference.digest(), "length": length}),
            )?;
            assert!(matches!(
                archive
                    .verify_checkpoint(
                        &cluster.domain,
                        &invalid,
                        receiver(&pool, &applier, &cluster)?
                    )
                    .await,
                Err(CellGroupArchiveError::Limit)
            ));
        }
        let verified = archive
            .verify_checkpoint(
                &cluster.domain,
                &reference,
                receiver(&pool, &applier, &cluster)?,
            )
            .await?;
        assert_ne!(verified.checkpoint().marker(), &base);
        assert_eq!(group.group_manifest()?.as_ref(), Some(&base));
        assert_eq!(group.checkpoint_manifest()?, None);
        drop(verified);
        cluster.close().await?;
    }
    Ok(())
}
