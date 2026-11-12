// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical packing, held readers and uncertain copy/deletion boundaries.
use super::*;
use cellar::application::consensus::verification::{GroupLog, LogOpenMode, ReplicaLog};
use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
};

struct Fixture {
    _temp: tempfile::TempDir,
    directory: PathBuf,
    roots: Vec<ManifestRoot>,
    tree: ManifestTree,
    _images: GenerationStore,
    pin: GenerationPin,
    domain: ExecutionDomain,
    log: ReplicaLog,
    group: GroupLog,
    trace: Arc<faults::Faults>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                engine_options(&temp.path().join("node"), LogOpenMode::Create, 1),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let group = log.create_group(engine_bootstrap()).await?;
        let images = GenerationStore::open(
            temp.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let pin = images.prepare(images::id(1), None, &cut.update())?;
        let directory = temp.path().join("pages");
        let tree = ManifestTree::new(route(), ManifestLimits::default())?;
        let mut store =
            ManifestPageStore::open(&directory, Arc::new(DirectFileSystem), 64, 1024 * 1024, 16)?;
        let mut roots = Vec::new();
        let mut pages = Vec::new();
        for id in 1..=3 {
            let root = tree.update(
                &mut store,
                ManifestRoot::default(),
                leaf(id, &pin, 1, 2),
                None,
            )?;
            pages.push(manifest_pages::read(&directory, root.digest().unwrap())?);
            roots.push(root);
        }
        drop(store);
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|s| s == "pack") {
                std::fs::remove_file(path)?;
            }
        }
        // Independently pack three real, separately authenticated single-leaf
        // roots. This fixes the physical overlap without relying on AVL ordering.
        let (name, bytes) = manifest_pages::encode(&pages)?;
        std::fs::write(directory.join(name), bytes)?;
        let trace = faults::Faults::new(directory.clone(), None);
        trace.all_directory_files.store(true, Ordering::Release);
        Ok(Self {
            _temp: temp,
            directory,
            roots,
            tree,
            _images: images,
            pin,
            domain,
            log,
            group,
            trace,
        })
    }

    fn open(&self, max_pages: usize, max_bytes: u64) -> Result<ManifestPageStore> {
        Ok(ManifestPageStore::open(
            &self.directory,
            Arc::new(faults::FaultFileSystem {
                faults: self.trace.clone(),
            }),
            max_pages,
            max_bytes,
            16,
        )?)
    }

    fn prepare(&self, store: &mut ManifestPageStore, n: usize) -> Result<PreparedGroupManifest> {
        Ok(self
            .tree
            .prepare_complete(&self.group, store, self.roots[n], position(2), 1, |_| {
                Ok(CellImagePin::Materialized(self.pin.clone()))
            })?)
    }

    async fn close(self) -> Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn physical(directory: &Path) -> Result<(usize, u64)> {
    let mut files = 0;
    let mut bytes = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|s| s == "pack") {
            files += 1;
            bytes += entry.metadata()?.len();
        }
    }
    Ok((files, bytes))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_preserves_held_roots_and_refunds_only_physical_bytes() -> Result<()> {
    let fixture = Fixture::new().await?;
    let mut pages = fixture.open(64, 1024 * 1024)?;
    let a = fixture.prepare(&mut pages, 0)?;
    let b = fixture.prepare(&mut pages, 1)?;
    pages.reconcile_retained(&[a.clone(), b.clone()])?;
    let before = pages.usage();
    assert_eq!((before.pages, before.files), (3, 1));
    let report = pages.reclaim(1)?;
    assert_eq!(
        (
            report.scanned,
            report.pages,
            report.files,
            report.compactions
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(report.relocated_pages, 2);
    assert_eq!(before.bytes - pages.usage().bytes, report.bytes);
    assert_eq!(
        (pages.usage().files, pages.usage().bytes),
        physical(&fixture.directory)?
    );
    assert_eq!((pages.usage().pages, pages.usage().files), (2, 1));
    for root in [&a, &b] {
        fixture
            .tree
            .verify(&pages, root.marker().root(), position(2), |leaf| {
                assert_eq!(root.pin_cell(&leaf.version.history.cell).unwrap().0, leaf);
                Ok(())
            })?;
    }
    assert!(fixture
        .tree
        .verify(&pages, fixture.roots[2], position(2), |_| Ok(()))
        .is_err());
    pages.reconcile_retained(std::slice::from_ref(&b))?;
    drop(a);
    collect_pages(&mut pages)?;
    assert_eq!((pages.usage().pages, pages.usage().files), (1, 1));
    fixture
        .tree
        .verify(&pages, b.marker().root(), position(2), |_| Ok(()))?;
    assert!(fixture
        .tree
        .verify(&pages, fixture.roots[0], position(2), |_| Ok(()))
        .is_err());
    drop(pages);
    assert!(
        fixture.open(64, 1024 * 1024).is_err(),
        "relocated logical pins retain the exclusive owner"
    );
    drop(b);
    let mut pages = fixture.open(64, 1024 * 1024)?;
    let b = fixture.prepare(&mut pages, 1)?;
    pages.reconcile_retained(&[])?;
    assert_eq!(
        collect_pages(&mut pages)?,
        0,
        "reader still protects the relocated page after reopen"
    );
    drop(b);
    assert_eq!(collect_pages(&mut pages)?, 1);
    assert_eq!(
        (
            pages.usage().pages,
            pages.usage().files,
            pages.usage().bytes
        ),
        (0, 0, 0)
    );
    assert_eq!(physical(&fixture.directory)?, (0, 0));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_refuses_unfunded_copy_without_credit_or_quarantine() -> Result<()> {
    for bound in ["pages", "bytes"] {
        let fixture = Fixture::new().await?;
        let initial = fixture.open(64, 1024 * 1024)?.usage();
        let (max_pages, max_bytes) = if bound == "pages" {
            (4, 1024 * 1024)
        } else {
            (64, initial.bytes)
        };
        let mut pages = fixture.open(max_pages, max_bytes)?;
        let a = fixture.prepare(&mut pages, 0)?;
        let b = fixture.prepare(&mut pages, 1)?;
        pages.reconcile_retained(&[a.clone(), b.clone()])?;
        let before = pages.usage();
        fixture.trace.image_seals.lock().unwrap().clear();
        let report = pages.reclaim(1)?;
        assert_eq!(report.blocked_compactions, 1);
        assert_eq!((report.files, report.pages, report.bytes), (0, 0, 0));
        assert_eq!(pages.usage(), before);
        assert!(fixture.trace.image_seals.lock().unwrap().is_empty());
        for root in [&a, &b] {
            fixture
                .tree
                .verify(&pages, root.marker().root(), position(2), |_| Ok(()))?;
        }
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_uncertain_cuts_retain_credit_and_recover_held_roots() -> Result<()> {
    for mode in [
        faults::FAIL_IMAGE_SYNC,
        faults::FAIL_PACK_RENAME_BEFORE,
        faults::FAIL_PACK_RENAME_AFTER,
        faults::FAIL_DIRECTORY_SYNC,
        faults::FAIL_REMOVE,
        254,
        255,
    ] {
        let fixture = Fixture::new().await?;
        let mut pages = fixture.open(64, 1024 * 1024)?;
        let a = fixture.prepare(&mut pages, 0)?;
        let b = fixture.prepare(&mut pages, 1)?;
        pages.reconcile_retained(&[a.clone(), b.clone()])?;
        let before = pages.usage();
        if mode == 254 {
            *fixture.trace.before_directory_sync.lock().unwrap() = Some(Box::new(|| {
                panic!("injected compaction directory barrier unwind");
            }));
        } else if mode == 255 {
            let first = fixture.trace.clone();
            *fixture.trace.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
                let second = first.clone();
                *first.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
                    second
                        .mode
                        .store(faults::FAIL_DIRECTORY_SYNC, Ordering::Release);
                }));
            }));
        } else {
            fixture.trace.mode.store(mode, Ordering::Release);
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| pages.reclaim(1)));
        if mode == 254 {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err(), "cut {mode}");
        }
        assert!(
            pages.usage().pages >= before.pages && pages.usage().bytes >= before.bytes,
            "cut {mode} refunded an unproved physical allocation"
        );
        assert!(matches!(pages.reclaim(1), Err(ManifestError::Quarantined)));
        assert!(fixture
            .tree
            .verify(&pages, fixture.roots[0], position(2), |_| Ok(()))
            .is_err());
        drop((a, b, pages));
        let mut pages = fixture.open(64, 1024 * 1024)?;
        assert_eq!(
            (pages.usage().files, pages.usage().bytes),
            physical(&fixture.directory)?
        );
        let a = fixture.prepare(&mut pages, 0)?;
        let b = fixture.prepare(&mut pages, 1)?;
        pages.reconcile_retained(&[a.clone(), b.clone()])?;
        collect_pages(&mut pages)?;
        assert_eq!(
            (pages.usage().pages, pages.usage().files),
            (2, 1),
            "cut {mode}"
        );
        for root in [&a, &b] {
            fixture
                .tree
                .verify(&pages, root.marker().root(), position(2), |_| Ok(()))?;
        }
        assert!(!std::fs::read_dir(&fixture.directory)?.any(|e| e
            .unwrap()
            .path()
            .extension()
            .is_some_and(|s| s == "pending")));
        fixture.close().await?;
    }
    Ok(())
}

#[test]
fn startup_rejects_malformed_pack_framing_and_legacy_files() -> Result<()> {
    let (name, original) = manifest_pages::encode(&[vec![1; 9], vec![2; 11]])?;
    for cut in [
        "header",
        "footer",
        "count",
        "length",
        "duplicate",
        "trailing",
        "legacy",
        "short",
    ] {
        let temp = tempfile::tempdir()?;
        let mut bytes = original.clone();
        let tail = bytes.len() - 52;
        let name = if cut == "legacy" {
            format!("{}.page", "01".repeat(32))
        } else {
            name.clone()
        };
        match cut {
            "header" => bytes[0] ^= 1,
            "footer" => bytes[tail] ^= 1,
            "count" => bytes[tail + 8..tail + 12].copy_from_slice(&97u32.to_be_bytes()),
            "length" => bytes[40..44].copy_from_slice(&16385u32.to_be_bytes()),
            "duplicate" => {
                let key = bytes[8..40].to_vec();
                bytes[53..85].copy_from_slice(&key);
            }
            "trailing" => bytes.push(0),
            "short" => bytes.truncate(3),
            "legacy" => (),
            _ => unreachable!(),
        }
        std::fs::write(temp.path().join(name), bytes)?;
        assert!(
            ManifestPageStore::open(temp.path(), Arc::new(DirectFileSystem), 64, 1024 * 1024, 16)
                .is_err(),
            "cut {cut}"
        );
    }
    Ok(())
}
