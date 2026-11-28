// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared archive-destination and recovery-object helpers for archive/disaster
//! integration targets. The target root provides `Cluster`, `DirectFileSystem`
//! and `persist_archive_intent`; this module owns only the archive fixtures.
#![allow(dead_code, unused_imports)]
use super::*;
use cellar_ltx::{
    generation::ReadServices,
    internal::{CacheLimits, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
};
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use std::sync::atomic::Ordering;

pub(crate) struct ArchiveDestination {
    pub(crate) store: CellGroupArchiveStore,
    pub(crate) images: GenerationStore,
    pub(crate) apply_images: CellGroupApplyImages,
}

pub(crate) async fn destination(cluster: &Cluster) -> Result<ArchiveDestination> {
    destination_with_filesystem(cluster, Arc::new(DirectFileSystem)).await
}

pub(crate) async fn destination_with_filesystem(
    cluster: &Cluster,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<ArchiveDestination> {
    let rooted = cluster
        .voters
        .first()
        .is_some_and(|voter| voter.extents.is_some());
    destination_mode(cluster, rooted, filesystem).await
}

pub(crate) async fn destination_mode(
    cluster: &Cluster,
    rooted: bool,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<ArchiveDestination> {
    let base = cluster._root.path().join("archive-data");
    let route = cluster.birth.route().clone();
    destination_at(cluster, rooted, filesystem, base, route).await
}

pub(crate) async fn destination_at(
    cluster: &Cluster,
    rooted: bool,
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
    base: std::path::PathBuf,
    route: GroupRoute,
) -> Result<ArchiveDestination> {
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
                images.clone(),
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )?;
            Ok(ArchiveDestination {
                store,
                images: generations,
                apply_images: images,
            })
        })?
        .await?
}

pub(crate) fn receiver(
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

pub(crate) fn source(
    pool: &CellGroupCheckpointPool,
    cluster: &Cluster,
) -> Result<CellGroupCheckpoint> {
    Ok(pool.capture(
        &cluster.voters[0]
            .log
            .cell_group(&cluster.birth.route().history)?,
        cluster.voters[0].owner.as_ref().unwrap().manifest(),
    )?)
}

pub(crate) async fn new_archive(
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

pub(crate) async fn start_upload(
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

pub(crate) fn registry_id(path: &Path) -> Result<[u8; 32]> {
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

pub(crate) fn generation_prefix(path: &Path) -> Result<(&str, u64)> {
    let (prefix, relative) = path
        .as_ref()
        .split_once("/generations/")
        .context("generation prefix")?;
    let generation = relative.split('/').next().context("generation")?.parse()?;
    Ok((prefix, generation))
}

pub(crate) async fn index(
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

pub(crate) fn cell_objects(index_path: &Path, index: &serde_json::Value, cell: &str) -> Vec<Path> {
    let (prefix, _) = generation_prefix(index_path).unwrap();
    index["chunks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| {
            c["parts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|part| part["part"]["cell"] == cell)
        })
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
