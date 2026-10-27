// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete root closure transfer; none of these values supplies native
//! installation permission or qualifies the still-disabled public root backend.
use super::*;
use anyhow::ensure;

pub(super) async fn source(root: &Path) -> Result<(Host, RootSnapshotSource)> {
    source_history(root, [7; 32]).await
}

async fn source_history(root: &Path, history: [u8; 32]) -> Result<(Host, RootSnapshotSource)> {
    source_members(root, history, vec![]).await
}

pub(super) async fn source_members(
    root: &Path,
    history: [u8; 32],
    learners: Vec<MemberId>,
) -> Result<(Host, RootSnapshotSource)> {
    source_padding(root, history, learners, 9437184, false).await
}

pub(super) async fn source_padding(
    root: &Path,
    history: [u8; 32],
    learners: Vec<MemberId>,
    padding: usize,
    recycle: bool,
) -> Result<(Host, RootSnapshotSource)> {
    let mut host = Host::open_members(root, false, 1, [9; 32], history, 1, learners).await?;
    host.elect().await?;
    let mut bytes = images::image(None, &format!("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(7); CREATE TABLE padding(value BLOB); INSERT INTO padding VALUES(zeroblob({padding})); {}", if recycle { "DELETE FROM padding" } else { "" }))?;
    if recycle {
        canonical_free_pages(&mut bytes)?;
    }
    let mut change = update(&[], &bytes, 0)?;
    let rooted = |data: &[u8], txid| -> Result<LtxImage> {
        image_position(&ExtentManifest::new(
            ImageBinding {
                history,
                schema: [9; 32],
                epoch: 1,
                txid,
                checksum: 0,
            },
            Geometry::new(if data.is_empty() { 0 } else { 512 }, data.len() as u64)?,
            &data
                .chunks(EXTENT_BYTES as usize)
                .map(ExtentId::of)
                .collect::<Result<Vec<_>, _>>()?,
        )?)
    };
    change.base = rooted(&[], 0)?;
    change.result = rooted(&bytes, 1)?;
    let image = host.commit(&change).await?;
    let source =
        RootSnapshotSource::capture(&host.log.group(&scope())?, image, SnapshotLimits::default())?;
    Ok((host, source))
}

// Pack free-list trunks at the front of the unused region and zero free leaf
// pages. SQLite never reads their payload. This keeps a valid database while
// creating repeated whole extents; zeroblob overflow pages themselves contain
// distinct next-page pointers and are not identical objects.
fn canonical_free_pages(bytes: &mut [u8]) -> Result<()> {
    let page_size = u16::from_be_bytes(bytes[16..18].try_into()?) as usize;
    ensure!(page_size == 512 && bytes.len().is_multiple_of(page_size));
    let pages = bytes.len() / page_size;
    let free = u32::from_be_bytes(bytes[36..40].try_into()?) as usize;
    ensure!(free == pages - 3, "fixture has unexpected active pages");
    let trunks = free.div_ceil(101);
    let mut leaf = 4 + trunks;
    bytes[32..36].copy_from_slice(&4_u32.to_be_bytes());
    bytes[3 * page_size..].fill(0);
    for index in 0..trunks {
        let number = 4 + index;
        let start = (number - 1) * page_size;
        let next = if index + 1 < trunks { number + 1 } else { 0 };
        let count = (pages + 1 - leaf).min(100);
        bytes[start..start + 4].copy_from_slice(&u32::try_from(next)?.to_be_bytes());
        bytes[start + 4..start + 8].copy_from_slice(&u32::try_from(count)?.to_be_bytes());
        for offset in 0..count {
            let at = start + 8 + offset * 4;
            bytes[at..at + 4].copy_from_slice(&u32::try_from(leaf + offset)?.to_be_bytes());
        }
        leaf += count;
    }
    ensure!(leaf == pages + 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seed_metadata_requires_the_catalog_birth_and_complete_root_binding() -> Result<()> {
    let root = tempfile::tempdir()?;
    let catalog = Catalog::new(
        Arc::new(object_store::memory::InMemory::new()),
        CatalogOptions::default(),
    )?;
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
            learners: vec![],
        })
        .await?;
    let (host, source) = source_history(&root.path().join("source"), birth.digest()).await?;
    let result = async {
        let limits = SnapshotLimits::default();
        let set = RootSnapshotSet::for_seed(
            &birth,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            limits,
        )?;
        ensure!(set.extents().count() >= 2 && set.marker().length > 2 * EXTENT_BYTES);
        let mut other = scope();
        other.cell.push_str("-other");
        other.group = GroupId::new([8; 16])?;
        let foreign = namespace
            .resolve_birth(BirthProposal {
                scope: other,
                voters: birth.voters().to_vec(),
                learners: vec![],
            })
            .await?;
        ensure!(RootSnapshotSet::for_seed(
            &foreign,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            limits
        )
        .is_err());
        ensure!(RootSnapshotSet::for_seed(
            &birth,
            [10; 32],
            &source.snapshot(),
            source.manifest(),
            limits
        )
        .is_err());
        let mut raw = source.manifest().to_vec();
        raw[0] ^= 1;
        ensure!(
            RootSnapshotSet::for_seed(&birth, [9; 32], &source.snapshot(), &raw, limits).is_err()
        );
        Ok(())
    }
    .await;
    result.and(host.close().await)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captured_root_rechecks_source_bytes_and_refuses_a_closed_engine_owner() -> Result<()> {
    use std::io::{Seek, Write};
    let root = tempfile::tempdir()?;
    let path = root.path().join("source");
    let (host, source) = source(&path).await?;
    let group = host.log.group(&scope())?;
    let result = (|| -> Result<()> {
        let set = RootSnapshotSet::for_group(
            &group,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            SnapshotLimits::default(),
        )?;
        let id = set
            .extents()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no dependencies"))?;
        let bytes = source.read_extent(id)?;
        let file =
            path.join("extents")
                .join(format!("e-{:08x}-{}", id.length(), hex::encode(id.bytes())));
        let mut object = std::fs::OpenOptions::new().write(true).open(file)?;
        object.seek(std::io::SeekFrom::Start(0))?;
        object.write_all(&[bytes[0] ^ 1])?;
        object.sync_all()?;
        ensure!(
            source.read_extent(id).is_err(),
            "a previously sealed root exposed corrupt source bytes"
        );
        Ok(())
    })();
    let closed = host.close().await;
    result?;
    closed?;
    ensure!(
        source.validate_owner(&group).is_err(),
        "closed owner remained a valid source"
    );
    Ok(())
}

fn read_sealed(image: &cellar_ltx::internal::SealedImageRoot, directory: &Path) -> Result<u64> {
    DirectFileSystem.create_dir_all(directory)?;
    let image = image.view().image(
        directory.into(),
        ScratchPool::new(ScratchLimits {
            max_bytes: 2 * EXTENT_BYTES,
            max_files: 2,
        })?,
        CacheLimits {
            max_cached_bytes: EXTENT_BYTES,
            max_cached_extents: 1,
        },
    )?;
    let db = open_extent_image(
        image,
        ReadServices {
            now_unix_millis: Arc::new(|| 1_700_000_000_000),
            fill_random: Arc::new(|bytes| bytes.fill(1)),
        },
    )?;
    ensure!(
        db.connection()
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?
            == "ok"
    );
    Ok(db
        .connection()
        .query_row("SELECT value FROM counter", [], |row| row.get(0))?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_root_transfer_seals_only_after_every_dependency_is_verified() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (host, source) = source(&root.path().join("source")).await?;
    let target = Host::open(&root.path().join("target"), false, 2).await?;
    let result = async {
        let group = target.log.group(&scope())?;
        let set = RootSnapshotSet::for_group(
            &group,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            SnapshotLimits::default(),
        )?;
        ensure!(
            source.manifest().len() < 1024,
            "manifest grew like database pages"
        );
        ensure!(set.marker().length > 2 * EXTENT_BYTES);
        let ids = set.extents().collect::<Vec<_>>();
        ensure!(ids.len() >= 2);
        let mut workspace = vec![0; EXTENT_BYTES as usize];
        ensure!(
            set.seal(&target.store, &mut workspace).is_err(),
            "metadata alone became a local seal"
        );
        for (index, id) in ids.iter().copied().enumerate() {
            let bytes = source.read_extent(id)?;
            set.install_extent(&target.store, id, &bytes)?;
            if index + 1 < ids.len() {
                ensure!(
                    set.seal(&target.store, &mut workspace).is_err(),
                    "partial closure became a local seal"
                );
            }
        }
        let sealed = set.seal(&target.store, &mut workspace)?;
        ensure!(sealed.reference() == set.reference());
        ensure!(read_sealed(&sealed, &root.path().join("reads"))? == 7);
        let before = target.store.usage()?;
        for id in ids {
            set.install_extent(&target.store, id, &source.read_extent(id)?)?;
        }
        let after = target.store.usage()?;
        ensure!(
            after.bytes == before.bytes
                && after.objects == before.objects
                && after.roots == before.roots,
            "exact repeat duplicated durable objects"
        );
        // The target's real runtime has not received or authorized an install.
        ensure!(target.log.group(&scope())?.applied()?.image.is_none());
        Ok(())
    }
    .await;
    let target_closed = target.close().await;
    let source_closed = host.close().await;
    result.and(target_closed).and(source_closed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_transfer_refuses_foreign_metadata_objects_and_corrupt_bytes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (host, source) = source(&root.path().join("source")).await?;
    let target = Host::open(&root.path().join("target"), false, 2).await?;
    let result = async {
        let group = target.log.group(&scope())?;
        let limits = SnapshotLimits::default();
        let mut corrupt = source.manifest().to_vec();
        corrupt[0] ^= 1;
        ensure!(
            RootSnapshotSet::for_group(&group, [9; 32], &source.snapshot(), &corrupt, limits)
                .is_err()
        );
        ensure!(RootSnapshotSet::for_group(
            &group,
            [9; 32],
            &source.snapshot(),
            &source.manifest()[1..],
            limits
        )
        .is_err());
        ensure!(RootSnapshotSet::for_group(
            &group,
            [10; 32],
            &source.snapshot(),
            source.manifest(),
            limits
        )
        .is_err());
        ensure!(
            source.validate_owner(&group).is_err(),
            "source rebound to a different open engine"
        );
        source.validate_owner(&host.log.group(&scope())?)?;
        let mut foreign = scope();
        foreign.cell.push_str("-foreign");
        foreign.group = GroupId::new([8; 16])?;
        target
            .log
            .create_group(GroupBootstrap {
                scope: foreign.clone(),
                member: MemberId::new(1)?,
                birth_digest: [7; 32],
                voters: vec![MemberId::new(1)?],
                learners: vec![],
            })
            .await?;
        ensure!(RootSnapshotSet::for_group(
            &target.log.group(&foreign)?,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            limits
        )
        .is_err());
        let set = RootSnapshotSet::for_group(
            &group,
            [9; 32],
            &source.snapshot(),
            source.manifest(),
            limits,
        )?;
        let id = set
            .extents()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no dependencies"))?;
        let mut bytes = source.read_extent(id)?;
        bytes[0] ^= 1;
        ensure!(set.install_extent(&target.store, id, &bytes).is_err());
        let foreign = ExtentId::of(&[42; 512])?;
        ensure!(source.read_extent(foreign).is_err());
        ensure!(set
            .install_extent(&target.store, foreign, &[42; 512])
            .is_err());
        ensure!(target.store.usage()?.objects == 0);
        ensure!(set
            .seal(&target.store, &mut vec![0; EXTENT_BYTES as usize])
            .is_err());
        Ok(())
    }
    .await;
    let target_closed = target.close().await;
    let source_closed = host.close().await;
    result.and(target_closed).and(source_closed)
}
