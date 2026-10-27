// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable object-store closure recovery. These are native snapshot data
//! tests, not public fresh-learner grant or recovery-time qualification.

use super::*;
use anyhow::{ensure, Context as _};
use cellar_ltx::LtxHost;
use futures_util::TryStreamExt as _;
use object_store::{path::Path as ObjectPath, ObjectStore};

async fn history(
    store: Arc<dyn ObjectStore>,
) -> Result<(NamespaceCatalog, GroupBirth, CellArchive)> {
    let catalog = Catalog::new(store, CatalogOptions::default())?;
    let mut config = NamespaceConfig::new(
        scope().cluster,
        scope().namespace,
        cellar_logic::NamespaceMode::Raft,
        [9; 32],
    );
    config.voters = VoterPolicy::SingleVoterDevelopment;
    let namespace = catalog.open_namespace(config).await?;
    let birth = namespace
        .resolve_birth(BirthProposal {
            scope: scope(),
            voters: vec![InitialMember {
                member: MemberId::new(1)?,
                node: NodeId::new([1; 16])?,
            }],
            learners: vec![InitialMember {
                member: MemberId::new(2)?,
                node: NodeId::new([2; 16])?,
            }],
        })
        .await?;
    let archive = namespace.cell_archive(&birth, LtxHost::default(), ArchiveLimits::default())?;
    Ok((namespace, birth, archive))
}

async fn cold_restore(store: Arc<dyn ObjectStore>) -> Result<()> {
    let root = tempfile::tempdir()?;
    let (_, birth, archive) = history(store.clone()).await?;
    let (source, captured) = snapshots::source_padding(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
        24 * 1024 * 1024,
        true,
    )
    .await?;
    let expected = captured.snapshot();
    let captured = Arc::new(captured);
    let unique = RootSnapshotSet::for_seed(
        &birth,
        [9; 32],
        &expected,
        captured.manifest(),
        SnapshotLimits::default(),
    )?
    .extents()
    .count();
    let reference = archive.store_root_snapshot(captured.clone()).await?;
    ensure!(reference.position()?.index.get() == expected.get_metadata().index);
    ensure!(archive.store_root_snapshot(captured.clone()).await? == reference);
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    ensure!(
        objects
            .iter()
            .filter(|o| o.location.as_ref().contains("/root-extents/"))
            .count()
            == unique,
        "archive transferred duplicate extent identities"
    );
    ensure!(
        objects
            .iter()
            .all(|o| !o.location.as_ref().ends_with("/head")),
        "data reference published an archive head"
    );
    drop(captured);
    source.close().await?;
    std::fs::remove_dir_all(root.path().join("source"))?;
    let path = root.path().join("target");
    let mut target = Host::open_members(
        &path,
        false,
        2,
        [9; 32],
        birth.digest(),
        2,
        vec![MemberId::new(2)?],
    )
    .await?;
    let result = async {
        ensure!(target.store.usage()?.objects == 0);
        let restored = archive
            .load_root_snapshot_at(&reference, &target.store)
            .await?;
        ensure!(restored.snapshot() == &expected);
        ensure!(target.log.group(&scope())?.applied()?.image.is_none());
        let prepared = restored.prepare(
            &target.log.group(&scope())?,
            [9; 32],
            SnapshotLimits::default(),
        )?;
        let ticket = snapshot_installs::snapshot_ticket(&mut target, expected).await?;
        target
            .runtime
            .complete_snapshot(prepared.completion(&ticket)?)
            .await?;
        snapshot_installs::installed(&mut target, &ticket).await?;
        let image = prepared.publish_root(&mut target.applier, &mut target.buffer)?;
        ensure!(read(&image, &root.path().join("read"))? == 7);
        ensure!(target.store.usage()?.objects == unique);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = target.close().await;
    result?;
    closed?;
    let resumed = Host::open_members(&path, true, 3, [9; 32], birth.digest(), 2, vec![]).await?;
    let image = resumed
        .applier
        .current()
        .context("missing restored image after restart")?;
    let result = read(&image, &root.path().join("restarted-read"));
    drop(image);
    let closed = resumed.close().await;
    ensure!(result? == 7);
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_archive_restores_without_its_source_then_installs_and_reopens() -> Result<()> {
    cold_restore(Arc::new(object_store::memory::InMemory::new())).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_archive_never_uses_a_local_cache_to_hide_remote_corruption() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let (_, birth, archive) = history(store.clone()).await?;
    let (host, captured) = snapshots::source_members(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
    )
    .await?;
    let captured = Arc::new(captured);
    let result = async {
        let reference = archive.store_root_snapshot(captured.clone()).await?;
        let target = NodeExtentStore::open(
            root.path().join("target"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let complete = archive.load_root_snapshot_at(&reference, &target).await?;
        let object = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .find(|o| o.location.as_ref().contains("/root-extents/"))
            .context("extent object")?;
        let original = store.get(&object.location).await?.bytes().await?;
        store.delete(&object.location).await?;
        ensure!(
            archive
                .load_root_snapshot_at(&reference, &target)
                .await
                .is_err(),
            "local pin hid missing remote extent"
        );
        let mut corrupt = original.to_vec();
        corrupt[0] ^= 1;
        store.put(&object.location, corrupt.into()).await?;
        ensure!(
            archive
                .load_root_snapshot_at(&reference, &target)
                .await
                .is_err(),
            "local pin hid corrupt remote extent"
        );
        ensure!(
            archive.store_root_snapshot(captured.clone()).await.is_err(),
            "conditional create repaired immutable corruption"
        );
        let foreign = NodeExtentStore::open(
            root.path().join("foreign"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let mut raw = serde_json::to_value(&reference)?;
        raw["kind"] = serde_json::json!("Snapshot");
        let wrong: RootArchiveReference = serde_json::from_value(raw)?;
        ensure!(archive
            .load_root_snapshot_at(&wrong, &foreign)
            .await
            .is_err());
        ensure!(
            foreign.usage()?.objects == 0,
            "wrong reference imported objects"
        );
        drop(complete);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(captured);
    let closed = host.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_archive_obeys_object_limits_before_upload_or_import() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let (namespace, birth, archive) = history(store.clone()).await?;
    let limited = namespace.cell_archive(
        &birth,
        LtxHost::default(),
        ArchiveLimits {
            max_manifest_bytes: 64 * 1024,
            max_capture_bytes: 64 * 1024,
            max_object_bytes: 64 * 1024,
            ..Default::default()
        },
    )?;
    let (host, captured) = snapshots::source_members(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
    )
    .await?;
    let captured = Arc::new(captured);
    let result = async {
        ensure!(
            matches!(
                limited.store_root_snapshot(captured.clone()).await,
                Err(ArchiveError::Limit)
            ),
            "publisher ignored its configured object cap"
        );
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        ensure!(
            objects
                .iter()
                .all(|o| !o.location.as_ref().contains("/root-extents/")
                    && !o.location.as_ref().contains("/root-snapshots/")),
            "object limit refusal left remote root material"
        );
        let reference = archive.store_root_snapshot(captured.clone()).await?;
        let target = NodeExtentStore::open(
            root.path().join("target"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        ensure!(
            matches!(
                limited.load_root_snapshot_at(&reference, &target).await,
                Err(ArchiveError::Limit)
            ),
            "receiver ignored its configured object cap"
        );
        ensure!(
            target.usage()?.objects == 0 && target.usage()?.roots == 0,
            "object limit refusal imported data"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(captured);
    let closed = host.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_root_source_cannot_publish_archive_material() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let (_, birth, archive) = history(store.clone()).await?;
    let (host, captured) = snapshots::source_members(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
    )
    .await?;
    host.close().await?;
    ensure!(
        archive
            .store_root_snapshot(Arc::new(captured))
            .await
            .is_err(),
        "closed source returned an archive reference"
    );
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    ensure!(
        objects
            .iter()
            .all(|o| !o.location.as_ref().contains("/root-snapshots/")
                && !o.location.as_ref().contains("/root-extents/")),
        "closed source stored archive material"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_archive_validates_complete_metadata_before_importing_objects() -> Result<()> {
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let (_, birth, archive) = history(store.clone()).await?;
    let (host, captured) = snapshots::source_members(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
    )
    .await?;
    let result = async {
        let reference = archive.store_root_snapshot(Arc::new(captured)).await?;
        let object = store
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .find(|o| o.location.as_ref().contains("/root-snapshots/"))
            .context("root metadata")?;
        let original = store.get(&object.location).await?.bytes().await?;
        let location = object.location.to_string();
        let parent = location.rsplit_once('/').context("root metadata path")?.0;
        for scenario in 0..3 {
            let target = NodeExtentStore::open(
                root.path().join(format!("target-{scenario}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?;
            let mut bytes = original.to_vec();
            match scenario {
                0 => bytes[8] ^= 1,
                1 => bytes[47] ^= 1,
                _ => bytes[40..44].copy_from_slice(&u32::MAX.to_be_bytes()),
            }
            // Give the malformed body a valid outer content identity. Refusal
            // must come from semantic binding, before any destination import.
            let digest: [u8; 32] = Sha256::digest(&bytes).into();
            let mut encoded = serde_json::to_value(&reference)?;
            encoded["digest"] = serde_json::to_value(digest)?;
            let altered: RootArchiveReference = serde_json::from_value(encoded)?;
            let path =
                ObjectPath::from(format!("{parent}/{}-{}", bytes.len(), hex::encode(digest)));
            store.put(&path, bytes.into()).await?;
            ensure!(
                archive
                    .load_root_snapshot_at(&altered, &target)
                    .await
                    .is_err(),
                "malformed root metadata accepted"
            );
            ensure!(
                target.usage()?.objects == 0 && target.usage()?.roots == 0,
                "metadata refusal imported objects"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = host.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated RustFS bucket and explicit credentials"]
async fn rustfs_root_archive_restores_without_its_source_then_installs_and_reopens() -> Result<()> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    let prefix = ObjectPath::from(format!(
        "root-archive-qualification/{:032x}",
        rand::random::<u128>()
    ));
    eprintln!("root archive qualification prefix={prefix}");
    cold_restore(Arc::new(object_store::prefix::PrefixStore::new(
        store, prefix,
    )))
    .await
}
