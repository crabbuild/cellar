// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Selected root heads use the common archive interface and real native
//! tickets. These tests do not qualify public fresh-learner authorization.

use super::*;
use anyhow::{ensure, Context as _};
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use futures_util::TryStreamExt as _;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::atomic::Ordering;

pub(super) fn verifier(path: &Path) -> Result<(ArchiveVerifier, GenerationStore)> {
    let images = GenerationStore::open(
        path.into(),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    Ok((
        ArchiveVerifier::new(images.clone(), ImageId::new([81; 16])?)?,
        images,
    ))
}

pub(super) fn cut(
    host: &Host,
    dependencies: Vec<ArchiveDependency>,
) -> Result<CommittedArchiveCut> {
    Ok(CommittedArchiveCut::capture_root(
        &host.log.group(&scope())?,
        host.applier.current().context("source image")?,
        host.store.clone(),
        dependencies,
        ArchiveLimits::default(),
    )?)
}

pub(super) fn change(
    before: &[u8],
    after: &[u8],
    txid: u64,
    history: [u8; 32],
) -> Result<CellUpdate> {
    let mut update = super::update(before, after, txid)?;
    let state = |bytes: &[u8], at| -> Result<LtxImage> {
        image_position(&ExtentManifest::new(
            ImageBinding {
                history,
                schema: [9; 32],
                epoch: 1,
                txid: at,
                checksum: 0,
            },
            Geometry::new(if bytes.is_empty() { 0 } else { 512 }, bytes.len() as u64)?,
            &bytes
                .chunks(EXTENT_BYTES as usize)
                .map(ExtentId::of)
                .collect::<Result<Vec<_>, _>>()?,
        )?)
    };
    update.base = state(before, txid)?;
    update.result = state(after, txid + 1)?;
    Ok(update)
}

async fn head_restore(store: Arc<dyn ObjectStore>) -> Result<()> {
    let root = tempfile::tempdir()?;
    let (_, birth, archive) = archive::history(store.clone()).await?;
    let (mut source, captured) = snapshots::source_members(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
    )
    .await?;
    let native = captured.snapshot();
    let set = RootSnapshotSet::for_seed(
        &birth,
        [9; 32],
        &native,
        captured.manifest(),
        SnapshotLimits::default(),
    )?;
    let manifest = ExtentManifest::decode(captured.manifest(), set.reference().root())?;
    let mut before = Vec::new();
    for index in 0..manifest.geometry().extent_count() {
        before.extend(captured.read_extent(manifest.extent(index).context("extent")?)?);
    }
    drop(captured);
    let (verifier, images) = verifier(&root.path().join("unused-flat-verifier"))?;
    let result = async {
        let published = archive
            .publish(
                cut(
                    &source,
                    vec![ArchiveDependency(b"root-dependency".to_vec())],
                )?,
                &verifier,
            )
            .await?;
        let first = published.reference();
        ensure!(published.tail_entries() == 0 && published.dependencies().count() == 1);
        ensure!(
            archive
                .recovery_metadata()
                .await?
                .context("root metadata")?
                .reference()
                == first
        );
        ensure!(
            archive.recovery_set_at(&first).await.is_err(),
            "root bytes were silently flattened"
        );
        drop(published);
        let after = images::image(Some(&before), "UPDATE counter SET value=8")?;
        source
            .commit(&change(&before, &after, 1, birth.digest())?)
            .await?;
        let latest = archive.publish(cut(&source, vec![])?, &verifier).await?;
        ensure!(latest.reference() != first && latest.position().index > first.position()?.index);
        ensure!(
            images.usage()?.generations == 0,
            "root publisher created a flat verification image"
        );
        drop(latest);
        Ok::<_, anyhow::Error>(first)
    }
    .await;
    let closed = source.close().await;
    let first = result?;
    closed?;
    std::fs::remove_dir_all(root.path().join("source"))?;
    let target_path = root.path().join("receiver");
    let mut target = Host::open_members(
        &target_path,
        false,
        2,
        [9; 32],
        birth.digest(),
        2,
        vec![MemberId::new(2)?],
    )
    .await?;
    let result = async {
        let receiving = archive.clone().with_root_store(target.store.clone());
        let restored = receiving.recovery_set_at(&first).await?;
        ensure!(restored.reference() == first && restored.dependencies().count() == 1);
        let prepared = restored.prepare_root(
            &target.log.group(&scope())?,
            [9; 32],
            SnapshotLimits::default(),
        )?;
        let ticket = snapshot_installs::snapshot_ticket(&mut target, native).await?;
        target
            .runtime
            .complete_snapshot(prepared.completion(&ticket)?)
            .await?;
        snapshot_installs::installed(&mut target, &ticket).await?;
        let image = prepared.publish_root(&mut target.applier, &mut target.buffer)?;
        ensure!(
            read(&image, &root.path().join("old-reference-read"))? == 7,
            "old reference selected newer head data"
        );
        let latest = receiving
            .recovery_set()
            .await?
            .context("current root head")?;
        ensure!(latest.position().index > first.position()?.index);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = target.close().await;
    result?;
    closed?;
    let resumed =
        Host::open_members(&target_path, true, 3, [9; 32], birth.digest(), 2, vec![]).await?;
    let result = read(
        &resumed.applier.current().context("resumed root")?,
        &root.path().join("reopened-read"),
    );
    let closed = resumed.close().await;
    ensure!(result? == 7);
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_head_advances_but_an_exact_old_reference_restores_after_source_loss() -> Result<()> {
    head_restore(Arc::new(object_store::memory::InMemory::new())).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_head_publication_never_repairs_corrupt_selected_history() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let (_, birth, archive) = archive::history(store.clone()).await?;
    let (source, captured) = snapshots::source_members(
        &root.path().join("source"),
        birth.digest(),
        vec![MemberId::new(2)?],
    )
    .await?;
    drop(captured);
    let (verifier, _) = verifier(&root.path().join("verifier"))?;
    let result = async {
        let published = archive.publish(cut(&source, vec![])?, &verifier).await?;
        let reference = published.reference();
        drop(published);
        let objects = store.list(None).try_collect::<Vec<_>>().await?;
        let head = objects
            .iter()
            .find(|o| o.location.as_ref().ends_with("/head"))
            .context("head")?;
        let before = store.get(&head.location).await?.bytes().await?;
        let extent = objects
            .iter()
            .find(|o| o.location.as_ref().contains("/root-extents/"))
            .context("extent")?;
        store.delete(&extent.location).await?;
        ensure!(
            archive
                .recovery_metadata()
                .await?
                .context("metadata")?
                .reference()
                == reference
        );
        ensure!(
            archive
                .publish(cut(&source, vec![])?, &verifier)
                .await
                .is_err(),
            "publication repaired invalid selected material"
        );
        ensure!(
            store.get(&extent.location).await.is_err(),
            "missing object was recreated"
        );
        ensure!(
            store.get(&head.location).await?.bytes().await? == before,
            "failed publication changed head"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = source.close().await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_head_reconciles_lost_write_and_confirmation_responses() -> Result<()> {
    for confirmation in [false, true] {
        let root = tempfile::tempdir()?;
        let store = Arc::new(archive_faults::Store::default());
        let (_, birth, archive) = archive::history(store.clone()).await?;
        let (source, captured) = snapshots::source_members(
            &root.path().join("source"),
            birth.digest(),
            vec![MemberId::new(2)?],
        )
        .await?;
        drop(captured);
        let (verifier, _) = verifier(&root.path().join("verifier"))?;
        let result = async {
            let cut = cut(&source, vec![])?;
            if confirmation {
                store.fail_head_confirmation.store(true, Ordering::Release);
            } else {
                store.fail_head_after.store(true, Ordering::Release);
            }
            let first = archive.publish(cut.clone(), &verifier).await;
            if confirmation {
                ensure!(first.is_err(), "lost confirmation was treated as success");
            } else {
                ensure!(
                    first.is_ok(),
                    "exact head read did not reconcile a lost PUT reply"
                );
            }
            let metadata = archive
                .recovery_metadata()
                .await?
                .context("committed head")?;
            let repeated = archive.publish(cut, &verifier).await?;
            ensure!(metadata.reference() == repeated.reference());
            Ok::<_, anyhow::Error>(())
        }
        .await;
        let closed = source.close().await;
        result?;
        closed?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an isolated RustFS bucket and explicit credentials"]
async fn rustfs_root_head_advances_and_restores_an_exact_reference_without_its_source() -> Result<()>
{
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    let prefix = ObjectPath::from(format!(
        "root-head-qualification/{:032x}",
        rand::random::<u128>()
    ));
    eprintln!("root head qualification prefix={prefix}");
    head_restore(Arc::new(object_store::prefix::PrefixStore::new(
        store, prefix,
    )))
    .await
}
