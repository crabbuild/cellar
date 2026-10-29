// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recovery qualification on actual SQLite/LTX, Raft Engine and both physical
//! image stores. These fixtures provide storage cuts, not a running Raft quorum.
#![allow(clippy::disallowed_methods)]

use super::*;
use anyhow::{bail, ensure, Context};
use cellar::application::consensus::verification::{GroupLog, LogOpenMode, ReplicaLog};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::OperationId;
use cellar_ltx::{
    apply::{ApplyLimits, ImageState},
    generation::ImageId,
    internal::{
        ExtentId, ExtentManifest, ExtentRootReference, Geometry, ImageBinding, ImageRoot,
        ImageRootUpdate, NodeExtentStore, EXTENT_BYTES,
    },
    FileSystem, TXID,
};
use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};

fn engine_bootstrap() -> cellar::application::consensus::verification::CellGroupBootstrap {
    cellar::application::consensus::verification::CellGroupBootstrap {
        route: route(),
        member: MemberId::new(1).unwrap(),
        voters: vec![MemberId::new(1).unwrap()],
        learners: vec![],
    }
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Materialized,
    Extents,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Materialized => "materialized",
            Self::Extents => "extents",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "materialized" => Ok(Self::Materialized),
            "extents" => Ok(Self::Extents),
            _ => bail!("unknown image mode"),
        }
    }
}

enum Images {
    Materialized(GenerationStore),
    Extents(NodeExtentStore),
}

fn history(cell: u8) -> CellHistory {
    CellHistory {
        cluster: route().history.cluster,
        namespace: route().history.namespace,
        cell: CellId::new(format!("cell-{cell}")).unwrap(),
        incarnation: IncarnationId::new([7; 16]).unwrap(),
    }
}

fn image_id(leaf: &CellManifestLeaf) -> Result<ImageId> {
    let cell: u8 = leaf
        .version
        .history
        .cell
        .as_str()
        .strip_prefix("cell-")
        .context("fixture cell")?
        .parse()?;
    let sequence = u8::try_from(leaf.version.sequence.get())?;
    let mut bytes = [0; 16];
    bytes[0] = cell;
    bytes[1] = sequence;
    bytes[2] = 123;
    Ok(ImageId::new(bytes)?)
}

fn extent_reference(manifest: &ExtentManifest) -> Result<ExtentRootReference> {
    Ok(ExtentRootReference::new(
        manifest.root(),
        manifest.encode()?.len() as u32,
    )?)
}

fn leaf_reference(leaf: &CellManifestLeaf) -> Result<ExtentRootReference> {
    let CellImageCommitment::Extents {
        root,
        manifest_bytes,
    } = leaf.version.image.commitment
    else {
        bail!("extent leaf required");
    };
    Ok(ExtentRootReference::new(
        ImageRoot::from_bytes(root),
        manifest_bytes,
    )?)
}

impl Images {
    fn open(path: &Path, mode: Mode, fs: Arc<dyn FileSystem>) -> Result<Self> {
        Ok(match mode {
            Mode::Materialized => Self::Materialized(GenerationStore::open(
                path.to_owned(),
                fs,
                GenerationLimits::default(),
            )?),
            Mode::Extents => Self::Extents(NodeExtentStore::open(
                path.to_owned(),
                fs,
                Default::default(),
            )?),
        })
    }

    fn prepare(
        &self,
        cell: u8,
        base: Option<(&CellManifestLeaf, &CellImagePin)>,
        cut: &images::Cut,
        after: &[u8],
        index: u64,
    ) -> Result<(CellManifestLeaf, CellImagePin)> {
        let state = cut.result;
        let mut leaf = CellManifestLeaf {
            enrollment: EnrollmentRecord::new(
                history(cell),
                ProposalId::new([cell; 16])?,
                route().schema_digest,
                CellImage {
                    epoch: state.epoch,
                    txid: state.txid.0,
                    checksum: state.checksum,
                    commitment: CellImageCommitment::Materialized(state.digest),
                },
            )?,
            version: CellVersion {
                history: history(cell),
                sequence: MutationSequence::new(state.txid.0)?,
                image: CellImage {
                    epoch: state.epoch,
                    txid: state.txid.0,
                    checksum: state.checksum,
                    commitment: CellImageCommitment::Materialized(state.digest),
                },
            },
            geometry: CellImageGeometry {
                length: state.length,
                page_size: state.page_size,
            },
            last_mutation: position(index),
            effects: EffectSummary::default(),
        };
        let previous = base.map(|(leaf, _)| leaf.enrollment.clone());
        let base = base.map(|(_, pin)| pin);
        let pin = match self {
            Self::Materialized(store) => {
                let base = match base {
                    None => None,
                    Some(CellImagePin::Materialized(pin)) => Some(pin),
                    _ => bail!("wrong physical base"),
                };
                CellImagePin::Materialized(store.prepare(image_id(&leaf)?, base, &cut.update())?)
            }
            Self::Extents(store) => {
                let binding = ImageBinding {
                    history: cell_image_history_digest(&leaf.version.history),
                    schema: route().schema_digest,
                    epoch: state.epoch,
                    txid: state.txid.0,
                    checksum: state.checksum,
                };
                let geometry = Geometry::new(state.page_size, state.length)?;
                let ids = after
                    .chunks(EXTENT_BYTES as usize)
                    .map(ExtentId::of)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let manifest = ExtentManifest::new(binding, geometry, &ids)?;
                let reference = extent_reference(&manifest)?;
                let base = match base {
                    None => None,
                    Some(CellImagePin::Extents(pin)) => Some(pin.as_ref()),
                    _ => bail!("wrong physical base"),
                };
                let empty = ExtentManifest::new(
                    ImageBinding {
                        txid: 0,
                        checksum: 0,
                        ..binding
                    },
                    Geometry::new(0, 0)?,
                    &[],
                )?;
                let update = ImageRootUpdate {
                    base: base
                        .map(|p| p.reference())
                        .unwrap_or(extent_reference(&empty)?),
                    result: reference,
                    binding,
                    geometry,
                    segments: cut.update().segments,
                };
                let prepared = store.prepare_image_root(
                    base,
                    &update,
                    &ApplyLimits::default(),
                    &mut vec![0; EXTENT_BYTES as usize],
                )?;
                leaf.version.image.commitment = CellImageCommitment::Extents {
                    root: reference.root().bytes(),
                    manifest_bytes: reference.encoded_length(),
                };
                CellImagePin::Extents(Arc::new(prepared.image))
            }
        };
        leaf.enrollment = match previous {
            Some(record) => record,
            None => EnrollmentRecord::new(
                history(cell),
                ProposalId::new([cell; 16])?,
                route().schema_digest,
                leaf.version.image,
            )?,
        };
        Ok((leaf, pin))
    }

    fn recover(&self, leaf: &CellManifestLeaf) -> Result<CellImagePin> {
        Ok(match self {
            Self::Materialized(store) => {
                let CellImageCommitment::Materialized(digest) = leaf.version.image.commitment
                else {
                    bail!("materialized leaf required");
                };
                CellImagePin::Materialized(store.adopt(
                    image_id(leaf)?,
                    ImageState {
                        epoch: leaf.version.image.epoch,
                        txid: TXID(leaf.version.image.txid),
                        checksum: leaf.version.image.checksum,
                        length: leaf.geometry.length,
                        page_size: leaf.geometry.page_size,
                        digest,
                    },
                )?)
            }
            Self::Extents(store) => {
                let view = store.open_root(leaf_reference(leaf)?)?;
                let manifest = view.manifest().clone();
                // Inventory/root metadata is never a seal after restart. Verify
                // every dependency, including extents after the SQLite header.
                for i in 0..manifest.geometry().extent_count() {
                    store.verify_extent(manifest.extent(i).context("extent closure")?)?;
                }
                CellImagePin::Extents(Arc::new(
                    store.seal_image_root(manifest, &mut vec![0; EXTENT_BYTES as usize])?,
                ))
            }
        })
    }

    fn collect(&self, leaves: &[CellManifestLeaf]) -> Result<()> {
        match self {
            Self::Materialized(store) => {
                store.retain_only(&leaves.iter().map(image_id).collect::<Result<Vec<_>>>()?)?;
                store.reclaim(4096)?;
            }
            Self::Extents(store) => {
                store.retain_only(
                    &leaves
                        .iter()
                        .map(leaf_reference)
                        .collect::<Result<Vec<_>>>()?,
                )?;
                store.reclaim(4096)?;
            }
        }
        Ok(())
    }
}

fn read_image(pin: &CellImagePin) -> Result<Vec<u8>> {
    match pin {
        CellImagePin::Materialized(pin) => images::read(pin),
        CellImagePin::Extents(pin) => {
            let manifest = pin.manifest();
            let mut bytes = Vec::new();
            for i in 0..manifest.geometry().extent_count() {
                bytes.extend(
                    pin.view()
                        .read_extent(manifest.extent(i).context("extent")?)?,
                );
            }
            ensure!(bytes.len() as u64 == manifest.geometry().length());
            Ok(bytes)
        }
        CellImagePin::Empty => Ok(Vec::new()),
    }
}

fn initial(cell: u8, large: bool) -> Result<Vec<u8>> {
    let padding = if large {
        "CREATE TABLE padding(bytes); INSERT INTO padding VALUES(zeroblob(4202496));"
    } else {
        ""
    };
    images::image(
        None,
        &format!(
            "CREATE TABLE value(n INTEGER); INSERT INTO value VALUES ({});
        CREATE TABLE receipts(client TEXT PRIMARY KEY, floor INTEGER, result BLOB);
        INSERT INTO receipts VALUES ('client-{cell}',1,X'{}'); {padding}",
            cell * 10 + 1,
            hex::encode(format!("r{cell}-1"))
        ),
    )
}

fn mutate(before: &[u8]) -> Result<Vec<u8>> {
    images::image(
        Some(before),
        "UPDATE value SET n = n + 1; UPDATE receipts SET floor = 2, result = X'72312d32';",
    )
}

fn assert_receipt(pin: &CellImagePin, cell: u8, sequence: u64) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("oracle.sqlite");
    std::fs::write(&path, read_image(pin)?)?;
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let value: u64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
    let row: (String, u64, Vec<u8>) =
        db.query_row("SELECT client, floor, result FROM receipts", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    assert_eq!(value, u64::from(cell) * 10 + sequence);
    assert_eq!(
        row,
        (
            format!("client-{cell}"),
            sequence,
            format!("r{cell}-{sequence}").into_bytes()
        )
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM receipts", [], |r| r.get::<_, u64>(0))?,
        1
    );
    Ok(())
}

fn prepare_root(
    tree: &ManifestTree,
    group: &GroupLog<GroupHistory>,
    pages: &mut ManifestPageStore,
    marker: &GroupManifest,
    images: &Images,
) -> Result<PreparedGroupManifest> {
    Ok(tree.prepare_complete(
        group,
        pages,
        marker.root(),
        marker.position(),
        marker.membership_revision(),
        |leaf| {
            images
                .recover(leaf)
                .map_err(|_| ManifestError::Invalid("incomplete physical image"))
        },
    )?)
}

async fn seed(path: &Path, mode: Mode, large: bool) -> Result<GroupManifest> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&path.join("node"), LogOpenMode::Create, 1),
            fs.clone(),
        ))
        .await??;
    let group = log.create_cell_group(engine_bootstrap()).await?;
    let images = Images::open(&path.join("images"), mode, fs.clone())?;
    let mut pages = ManifestPageStore::open(&path.join("pages"), fs, 256, 4 * 1024 * 1024, 16)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut prepared = tree.prepare_complete(
        &group,
        &mut pages,
        ManifestRoot::default(),
        position(1),
        1,
        |_| unreachable!(),
    )?;
    for cell in 1..=2 {
        let bytes = initial(cell, large && cell == 1)?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let (leaf, pin) = images.prepare(cell, None, &cut, &bytes, u64::from(cell) + 1)?;
        let entry = enrollment_entry(&leaf.enrollment, &cut)?;
        let (next, decision) = tree.prepare_enrollment(
            &mut pages,
            &prepared,
            EnrollmentApply {
                position: leaf.last_mutation,
                phase: EnrollmentPhase::Active,
                entry: &entry,
                limits: GroupEntryLimits::default(),
            },
            |record| {
                assert_eq!(record, &leaf.enrollment);
                Ok(EnrollmentImage {
                    geometry: leaf.geometry,
                    effects: leaf.effects,
                    pin,
                })
            },
        )?;
        assert!(matches!(decision, EnrollmentDecision::Propose(_)));
        prepared = next;
        let mut write = root_write(&group, prepared.clone())?;
        write.entries[0].data = entry.into();
        log.persist_cells(OperationId::new(u64::from(cell)), vec![write])
            .await?;
    }
    let marker = prepared.marker().clone();
    log.close().await?;
    domain.shutdown().await;
    Ok(marker)
}

pub async fn both_images() -> Result<()> {
    for mode in [Mode::Materialized, Mode::Extents] {
        let temp = tempfile::tempdir()?;
        seed(temp.path(), mode, true).await?;
        {
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let fs = Arc::new(DirectFileSystem);
            let log = domain
                .run(ReplicaLog::open(
                    engine_options(&temp.path().join("node"), LogOpenMode::Resume, 2),
                    fs.clone(),
                ))
                .await??;
            let group = log.cell_group(&engine_bootstrap().route.history)?;
            let images = Images::open(&temp.path().join("images"), mode, fs.clone())?;
            let mut pages =
                ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024, 16)?;
            let tree = ManifestTree::new(route(), ManifestLimits::default())?;
            let current = prepare_root(
                &tree,
                &group,
                &mut pages,
                &group.group_manifest()?.context("selected root")?,
                &images,
            )?;
            let (old, old_pin) = current.pin_cell(&history(1).cell).unwrap();
            let sibling = current.pin_cell(&history(2).cell).unwrap().0.clone();
            let before = read_image(&old_pin)?;
            let after = mutate(&before)?;
            let cut = images::Cut::new(&before, &after, 1, true, false);
            let (changed, next_pin) = images.prepare(1, Some((old, &old_pin)), &cut, &after, 4)?;
            if let (CellImagePin::Extents(old), CellImagePin::Extents(next)) = (&old_pin, &next_pin)
            {
                assert!(old.manifest().geometry().extent_count() > 1);
                assert_eq!(
                    old.manifest().extent(1),
                    next.manifest().extent(1),
                    "unchanged tail extent is shared"
                );
            }
            let next =
                tree.prepare_update(&mut pages, &current, changed, Some(&old.version), next_pin)?;
            log.persist_cells(OperationId::new(3), vec![root_write(&group, next.clone())?])
                .await?;
            assert_eq!(next.pin_cell(&history(2).cell).unwrap().0, &sibling);
            assert_receipt(&old_pin, 1, 1)?;
            assert_receipt(&next.pin_cell(&history(1).cell).unwrap().1, 1, 2)?;
            assert_receipt(&next.pin_cell(&history(2).cell).unwrap().1, 2, 1)?;
            log.close().await?;
            domain.shutdown().await;
        }
        recover_and_check(temp.path(), mode, Some(4)).await?;
        replay_enrollment(temp.path(), mode).await?;
        recover_and_check(temp.path(), mode, Some(5)).await?;
    }
    Ok(())
}

async fn recover_and_check(path: &Path, mode: Mode, expected: Option<u64>) -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&path.join("node"), LogOpenMode::Resume, 3),
            fs.clone(),
        ))
        .await??;
    let group = log.cell_group(&engine_bootstrap().route.history)?;
    let marker = group.group_manifest()?.context("selected root")?;
    let index = marker.position().index.get();
    if let Some(expected) = expected {
        assert_eq!(index, expected);
    }
    if expected != Some(5) {
        assert!([3, 4].contains(&index));
    }
    assert_eq!(marker.root().cells(), 2);
    let images = Images::open(&path.join("images"), mode, fs.clone())?;
    let mut pages = ManifestPageStore::open(&path.join("pages"), fs, 256, 4 * 1024 * 1024, 16)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let restored = prepare_root(&tree, &group, &mut pages, &marker, &images)?;
    let mut leaves = Vec::new();
    for cell in 1..=2 {
        let (leaf, pin) = restored.pin_cell(&history(cell).cell).unwrap();
        let sequence = if cell == 1 && index >= 4 { 2 } else { 1 };
        assert_eq!(leaf.version.history, history(cell));
        assert_eq!(leaf.enrollment.cell(), &history(cell));
        assert_eq!(leaf.enrollment.creation(), ProposalId::new([cell; 16])?);
        assert_eq!(leaf.enrollment.initial().txid, 1);
        assert_eq!(leaf.version.sequence.get(), sequence);
        assert_receipt(&pin, cell, sequence)?;
        leaves.push(leaf.clone());
    }
    pages.reconcile_retained(&[])?;
    collect_pages(&mut pages)?;
    images.collect(&leaves)?;
    // Collection must not turn either selected image into an empty/missing cell.
    let verified = prepare_root(&tree, &group, &mut pages, &marker, &images)?;
    assert_eq!(verified.marker(), &marker);
    assert_eq!(pages.usage().pages, 2);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

pub async fn missing_images() -> Result<()> {
    for mode in [Mode::Materialized, Mode::Extents] {
        let kinds: &[&str] = match mode {
            Mode::Materialized => &[
                "page-missing",
                "page-corrupt",
                "image-missing",
                "image-corrupt",
            ],
            Mode::Extents => &[
                "page-missing",
                "page-corrupt",
                "root-missing",
                "root-corrupt",
                "tail-missing",
                "tail-corrupt",
            ],
        };
        for kind in kinds {
            let temp = tempfile::tempdir()?;
            let marker = seed(temp.path(), mode, true).await?;
            let victim = if kind.starts_with("page-") {
                temp.path().join("pages").join(format!(
                    "{}.page",
                    hex::encode(marker.root().digest().unwrap())
                ))
            } else {
                let fs = Arc::new(DirectFileSystem);
                let pages = ManifestPageStore::open(
                    &temp.path().join("pages"),
                    fs.clone(),
                    256,
                    4 * 1024 * 1024,
                    16,
                )?;
                let tree = ManifestTree::new(route(), ManifestLimits::default())?;
                let cell = if kind.starts_with("tail-") { 1 } else { 2 };
                let leaf = tree
                    .get(&pages, marker.root(), &history(cell).cell)?
                    .unwrap();
                match mode {
                    Mode::Materialized => temp
                        .path()
                        .join("images")
                        .join(format!("{}.sqlite", hex::encode(image_id(&leaf)?.bytes()))),
                    Mode::Extents => {
                        let reference = leaf_reference(&leaf)?;
                        if kind.starts_with("root-") {
                            temp.path()
                                .join("images")
                                .join(format!("r-{}", hex::encode(reference.root().bytes())))
                        } else {
                            let store = NodeExtentStore::open(
                                temp.path().join("images"),
                                fs,
                                Default::default(),
                            )?;
                            let view = store.open_root(reference)?;
                            assert!(view.manifest().geometry().extent_count() > 1);
                            let extent = view.manifest().extent(1).unwrap();
                            temp.path().join("images").join(format!(
                                "e-{:08x}-{}",
                                extent.length(),
                                hex::encode(extent.bytes())
                            ))
                        }
                    }
                }
            };
            if kind.ends_with("missing") {
                std::fs::remove_file(&victim)?;
            } else {
                let mut bytes = std::fs::read(&victim)?;
                let last = bytes.len() - 1;
                bytes[last] ^= 0x80;
                std::fs::write(&victim, bytes)?;
            }
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let fs = Arc::new(DirectFileSystem);
            let log = domain
                .run(ReplicaLog::open(
                    engine_options(&temp.path().join("node"), LogOpenMode::Resume, 2),
                    fs.clone(),
                ))
                .await??;
            let group = log.cell_group(&engine_bootstrap().route.history)?;
            assert_eq!(group.group_manifest()?.as_ref(), Some(&marker));
            let images = Images::open(&temp.path().join("images"), mode, fs.clone())?;
            let mut pages =
                ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024, 16)?;
            let tree = ManifestTree::new(route(), ManifestLimits::default())?;
            assert!(
                prepare_root(&tree, &group, &mut pages, &marker, &images).is_err(),
                "{mode:?}/{kind}"
            );
            assert!(group.applied().is_err());
            assert!(
                pages.reclaim(16).is_err(),
                "failed recovery cannot authorize reclamation"
            );
            log.close().await?;
            domain.shutdown().await;
        }
    }
    Ok(())
}

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub async fn crash_matrix() -> Result<()> {
    for mode in [Mode::Materialized, Mode::Extents] {
        for cut in [
            "before-image",
            "image-write",
            "image-before-sync",
            "image-file-sync",
            "image-directory-sync",
            "after-image",
            "manifest-write",
            "manifest-before-sync",
            "manifest-file-sync",
            "manifest-directory-sync",
            "after-manifest",
            "engine-before-sync",
            "engine-after-sync",
            "after-publication",
            "reclaim-after-remove",
        ] {
            let temp = tempfile::tempdir()?;
            seed(temp.path(), mode, false).await?;
            let signal = temp.path().join("signal");
            let output = temp.path().join("child.log");
            let file = std::fs::File::create(&output)?;
            let mut child = Child(
                std::process::Command::new(std::env::current_exe()?)
                    .args([
                        "--ignored",
                        "--exact",
                        "manifest_process_fixture",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("CELLAR_GROUP_TEST_ROOT", temp.path())
                    .env("CELLAR_GROUP_TEST_MODE", mode.name())
                    .env("CELLAR_GROUP_TEST_CUT", cut)
                    .env("CELLAR_GROUP_TEST_SIGNAL", &signal)
                    .stdout(file.try_clone()?)
                    .stderr(file)
                    .spawn()?,
            );
            tokio::time::timeout(Duration::from_secs(30), async {
                while !signal.exists() {
                    if let Some(status) = child.0.try_wait()? {
                        bail!(
                            "{mode:?}/{cut} exited {status}: {}",
                            std::fs::read_to_string(&output)?
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            child.0.kill()?;
            let status = child.0.wait()?;
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                assert_eq!(status.signal(), Some(9));
            }
            assert!(!status.success());
            let expected = match cut {
                "engine-before-sync" => None,
                "engine-after-sync" | "after-publication" | "reclaim-after-remove" => Some(4),
                _ => Some(3),
            };
            recover_and_check(temp.path(), mode, expected)
                .await
                .with_context(|| format!("{mode:?}/{cut}"))?;
            assert_eq!(
                std::fs::read_to_string(temp.path().join("executions"))?,
                "1",
                "recovery must not rerun the callback"
            );
        }
    }
    Ok(())
}

pub async fn process_fixture() -> Result<()> {
    let path = PathBuf::from(std::env::var("CELLAR_GROUP_TEST_ROOT")?);
    let mode = Mode::parse(&std::env::var("CELLAR_GROUP_TEST_MODE")?)?;
    let cut_name = std::env::var("CELLAR_GROUP_TEST_CUT")?;
    let signal = PathBuf::from(std::env::var("CELLAR_GROUP_TEST_SIGNAL")?);
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = engine_fault::Gate::new(Some(signal.clone()));
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&path.join("node"), LogOpenMode::Resume, 2),
            Arc::new(engine_fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let group = log.cell_group(&engine_bootstrap().route.history)?;
    let image_fault = faults::Faults::new(path.join("images"), Some(signal.clone()));
    let page_fault = faults::Faults::new(path.join("pages"), Some(signal));
    image_fault
        .all_directory_files
        .store(true, Ordering::Release);
    page_fault
        .all_directory_files
        .store(true, Ordering::Release);
    let images = Images::open(
        &path.join("images"),
        mode,
        Arc::new(faults::FaultFileSystem {
            faults: image_fault.clone(),
        }),
    )?;
    let mut pages = ManifestPageStore::open(
        &path.join("pages"),
        Arc::new(faults::FaultFileSystem {
            faults: page_fault.clone(),
        }),
        256,
        4 * 1024 * 1024,
        16,
    )?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let current = prepare_root(
        &tree,
        &group,
        &mut pages,
        &group.group_manifest()?.unwrap(),
        &images,
    )?;
    let (old, old_pin) = current.pin_cell(&history(1).cell).unwrap();
    let before = read_image(&old_pin)?;
    let after = mutate(&before)?;
    // This independent callback ledger is durable before any crash gate arms.
    std::fs::write(path.join("executions"), b"1")?;
    std::fs::File::open(path.join("executions"))?.sync_all()?;
    std::fs::File::open(&path)?.sync_all()?;
    if cut_name == "before-image" {
        image_fault.pause()?;
    }
    let select = |prefix: &str| match cut_name.strip_prefix(prefix) {
        Some("write") => faults::AFTER_IMAGE_WRITE,
        Some("before-sync") => faults::BEFORE_IMAGE_SYNC,
        Some("file-sync") => faults::AFTER_IMAGE_SYNC,
        Some("directory-sync") => faults::AFTER_DIRECTORY_SYNC,
        _ => 0,
    };
    image_fault.mode.store(select("image-"), Ordering::Release);
    let cut = images::Cut::new(&before, &after, 1, true, false);
    let (changed, pin) = images.prepare(1, Some((old, &old_pin)), &cut, &after, 4)?;
    if cut_name == "after-image" {
        image_fault.pause()?;
    }
    page_fault
        .mode
        .store(select("manifest-"), Ordering::Release);
    let next = tree.prepare_update(&mut pages, &current, changed, Some(&old.version), pin)?;
    if cut_name == "after-manifest" {
        page_fault.pause()?;
    }
    let _release = gate.arm(match cut_name.as_str() {
        "engine-before-sync" => engine_fault::CRASH_BEFORE_SYNC,
        "engine-after-sync" => engine_fault::CRASH_AFTER_SYNC,
        _ => engine_fault::OFF,
    });
    log.persist_cells(OperationId::new(3), vec![root_write(&group, next)?])
        .await?;
    if cut_name == "after-publication" {
        page_fault.pause()?;
    }
    drop(current);
    drop(old_pin);
    pages.reconcile_retained(&[])?;
    if cut_name == "reclaim-after-remove" {
        page_fault
            .mode
            .store(faults::AFTER_REMOVE, Ordering::Release);
        collect_pages(&mut pages)?;
    }
    bail!("requested cut was never reached: {cut_name}")
}

fn enrollment_entry(record: &EnrollmentRecord, cut: &images::Cut) -> Result<Vec<u8>> {
    let route = route();
    Ok(CellGroupEntry {
        binding: GroupEntryBinding {
            group: route.history,
            route_epoch: route.epoch,
            birth_digest: route.birth_digest,
            schema_digest: route.schema_digest,
            proposal: record.creation(),
        },
        command: GroupCommand::Enroll {
            cell: record.cell().clone(),
            image: LtxImage::try_from(record.initial())?,
            files: vec![GroupLtx {
                min_txid: 1,
                max_txid: cut.result.txid.0,
                page_count: cut.pages.len() as u64,
                decoded_bytes: cut.pages.len() as u64 * 512,
                bytes: &cut.bytes,
            }],
        },
    }
    .encode(&route, GroupEntryLimits::default())?)
}

async fn replay_enrollment(path: &Path, mode: Mode) -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&path.join("node"), LogOpenMode::Resume, 4),
            fs.clone(),
        ))
        .await??;
    let group = log.cell_group(&route().history)?;
    let images = Images::open(&path.join("images"), mode, fs.clone())?;
    let mut pages = ManifestPageStore::open(&path.join("pages"), fs, 256, 4 * 1024 * 1024, 16)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let current = prepare_root(
        &tree,
        &group,
        &mut pages,
        &group.group_manifest()?.context("selected root")?,
        &images,
    )?;
    let (old, old_pin) = current.pin_cell(&history(1).cell).unwrap();
    assert_eq!(old.version.sequence.get(), 2);
    assert_eq!(old.enrollment.initial().txid, 1);
    assert_ne!(old.enrollment.initial(), old.version.image);
    let initial_leaf = CellManifestLeaf {
        version: CellVersion {
            sequence: MutationSequence::new(1)?,
            image: old.enrollment.initial(),
            ..old.version.clone()
        },
        ..old.clone()
    };
    assert!(
        images.recover(&initial_leaf).is_err(),
        "the original image has been reclaimed; creation identity must survive independently"
    );
    let bytes = initial(1, true)?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let entry = enrollment_entry(&old.enrollment, &cut)?;
    let before = pages.usage().written_pages;
    let apply = |phase, index| EnrollmentApply {
        position: position(index),
        phase,
        entry: &entry,
        limits: GroupEntryLimits::default(),
    };
    for phase in [EnrollmentPhase::Sealed, EnrollmentPhase::Retired] {
        assert!(tree
            .prepare_enrollment(&mut pages, &current, apply(phase, 5), |_| panic!(
                "sealed or retired retry cannot initialize"
            ))
            .is_err());
    }
    for index in [4, 6] {
        assert!(tree
            .prepare_enrollment(
                &mut pages,
                &current,
                apply(EnrollmentPhase::Active, index),
                |_| panic!("invalid prefix cannot initialize")
            )
            .is_err());
    }
    let competing = EnrollmentRecord::new(
        old.version.history.clone(),
        ProposalId::new([99; 16])?,
        route().schema_digest,
        old.enrollment.initial(),
    )?;
    let competing_entry = enrollment_entry(&competing, &cut)?;
    let error = tree.prepare_enrollment(
        &mut pages,
        &current,
        EnrollmentApply {
            entry: &competing_entry,
            ..apply(EnrollmentPhase::Active, 5)
        },
        |_| panic!("competing creation cannot initialize"),
    );
    assert!(matches!(
        error,
        Err(EnrollmentApplyError::Enrollment(EnrollmentError::Conflict))
    ));
    let (next, decision) = tree.prepare_enrollment(
        &mut pages,
        &current,
        apply(EnrollmentPhase::Active, 5),
        |_| panic!("duplicate must not rerun initialization"),
    )?;
    assert_eq!(
        decision,
        EnrollmentDecision::Existing(old.enrollment.clone())
    );
    assert_eq!(next.marker().root(), current.marker().root());
    assert_eq!(next.marker().position(), position(5));
    assert_eq!(pages.usage().written_pages, before);
    for cell in 1..=2 {
        assert_eq!(
            next.pin_cell(&history(cell).cell).unwrap().0,
            current.pin_cell(&history(cell).cell).unwrap().0
        );
    }
    let mut write = root_write(&group, next.clone())?;
    write.entries[0].data = entry.into();
    log.persist_cells(OperationId::new(7), vec![write]).await?;
    assert_eq!(group.group_manifest()?.unwrap(), *next.marker());
    assert_receipt(&next.pin_cell(&history(1).cell).unwrap().1, 1, 2)?;
    assert_receipt(&old_pin, 1, 2)?;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
