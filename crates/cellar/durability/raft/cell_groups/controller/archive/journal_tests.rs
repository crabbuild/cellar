// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::*, consensus::*};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn seed_journal_reopens_only_existing_exact_engine_and_purpose() -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let history = GroupHistory {
        cluster: ClusterId::new([21; 16])?,
        namespace: NamespaceId::new([22; 16])?,
        group: GroupId::new([23; 16])?,
        incarnation: IncarnationId::new([24; 16])?,
    };
    let archive = CellGroupArchivePool::new(Default::default())?.for_group(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("seed-journal"),
        history,
    )?;
    let prepared = archive
        .prepare_registry_creation(
            &domain,
            GroupRoute {
                history,
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                birth_digest: [25; 32],
                schema_digest: [26; 32],
            },
        )
        .await?;
    let creation = prepared.intent().clone();
    let registry = prepared.commit(&domain).await?;
    let intent = registry.prepare_upload(&domain).await?;
    let disk = tempfile::tempdir()?;
    let root = disk.path().to_owned();
    domain.blocking(move || -> Result<()> {
        let fs: Arc<dyn cellar_ltx::FileSystem> = Arc::new(cellar_ltx::DirectFileSystem);
        let path = root.join(seed_name(history));
        assert!(Journal::open_seed_existing(fs.clone(), path.clone(), [27; 16], creation.clone()).is_err());
        assert!(matches!(fs.symlink_metadata(&path), Err(error) if error.kind() == std::io::ErrorKind::NotFound));
        let journal = Journal::open_seed(fs.clone(), path.clone(), [27; 16], creation.clone())?;
        journal.store(&Record { pending: Pending::Reserve(intent.clone()), ..Default::default() })?;
        let identity = fs.read(&path.join("identity"))?;
        let saved = fs.read(&path.join("state"))?;
        assert_eq!(Journal::inspect_seed(fs.as_ref(), &path, [27; 16])?, creation);
        assert!(Journal::inspect_seed(fs.as_ref(), &path, [28; 16]).is_err());
        drop(journal);
        assert!(Journal::open_seed_existing(fs.clone(), path.clone(), [28; 16], creation.clone()).is_err());
        assert_eq!(fs.read(&path.join("identity"))?, identity);
        assert_eq!(fs.read(&path.join("state"))?, saved);
        let reopened = Journal::open_seed_existing(fs.clone(), path.clone(), [27; 16], creation.clone())?;
        let Pending::Reserve(recovered) = reopened.load()?.pending else { panic!("lost pending seed reserve") };
        assert_eq!(recovered, intent);
        drop(reopened);
        let ordinary = root.join("ordinary");
        let journal = Journal::open(fs.clone(), ordinary.clone(), [27; 16], creation.clone())?;
        assert!(Journal::inspect_seed(fs.as_ref(), &ordinary, [27; 16]).is_err());
        drop(journal);
        fs.remove_file(&path.join("state"))?;
        assert!(Journal::open_seed_existing(fs.clone(), path.clone(), [27; 16], creation.clone()).is_err());
        assert!(matches!(fs.symlink_metadata(&path.join("state")), Err(error) if error.kind() == std::io::ErrorKind::NotFound));
        fs.remove_file(&path.join("identity"))?;
        assert!(Journal::open_seed_existing(fs.clone(), path.clone(), [27; 16], creation).is_err());
        assert!(matches!(fs.symlink_metadata(&path.join("identity")), Err(error) if error.kind() == std::io::ErrorKind::NotFound));
        Ok(())
    })?.await??;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn archive_journal_reopens_exact_intent_and_refuses_lost_state() -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let archive = CellGroupArchivePool::new(Default::default())?.for_group(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("journal"),
        history,
    )?;
    let creation = archive
        .prepare_registry_creation(
            &domain,
            GroupRoute {
                history,
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                birth_digest: [5; 32],
                schema_digest: [6; 32],
            },
        )
        .await?;
    let expected = creation.intent().clone();
    let registry = creation.commit(&domain).await?;
    let intent = registry.prepare_upload(&domain).await?;
    let disk = tempfile::tempdir()?;
    let directory = disk.path().join("owner");
    let path = directory.clone();
    let identity = expected.clone();
    let journal = domain
        .blocking(move || {
            Journal::open(
                Arc::new(cellar_ltx::DirectFileSystem),
                path,
                [7; 16],
                identity,
            )
        })?
        .await??;
    let saved = intent.clone();
    domain
        .blocking(move || -> Result<()> {
            journal.store(&Record {
                pending: Pending::Reserve(saved),
                ..Default::default()
            })?;
            Ok(())
        })?
        .await??;
    let path = directory.clone();
    let identity = expected.clone();
    let reopened = domain
        .blocking(move || {
            Journal::open(
                Arc::new(cellar_ltx::DirectFileSystem),
                path,
                [7; 16],
                identity,
            )
        })?
        .await??;
    let recovered = domain.blocking(move || reopened.load())?.await??;
    let Pending::Reserve(recovered) = recovered.pending else {
        anyhow::bail!("lost reserve intent")
    };
    assert_eq!(recovered, intent);
    let ticket = registry.reserve(&domain, &recovered).await?;
    assert_eq!(registry.reserve(&domain, &intent).await?, ticket);
    let path = directory.clone();
    let identity = expected.clone();
    domain
        .blocking(move || -> Result<()> {
            let filesystem = cellar_ltx::DirectFileSystem;
            filesystem.remove_file(&path.join("state"))?;
            let before = filesystem.read(&path.join("identity"))?;
            assert!(Journal::open(Arc::new(filesystem), path.clone(), [7; 16], identity).is_err());
            assert_eq!(
                cellar_ltx::DirectFileSystem.read(&path.join("identity"))?,
                before
            );
            assert!(cellar_ltx::DirectFileSystem
                .metadata(&path.join("state"))
                .is_err());
            Ok(())
        })?
        .await??;
    registry.abort(&domain, &ticket).await?;
    domain.shutdown().await;
    Ok(())
}

use cellar_ltx::{DirectFileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata};
use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Counted(HostFile, Arc<AtomicUsize>);

impl HostFileIo for Counted {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.write_all(bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.read_exact_at(offset, len)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.0.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.0.file_len()
    }
}

struct ReplaceOnStat {
    state: PathBuf,
    replacement: PathBuf,
    armed: AtomicBool,
    reads: Arc<AtomicUsize>,
}

impl ReplaceOnStat {
    fn fixture(old: &[u8], replacement: Option<&[u8]>) -> (tempfile::TempDir, Self) {
        let directory = tempfile::tempdir().unwrap();
        let state = directory.path().join("state");
        let next = directory.path().join("sealed-replacement");
        DirectFileSystem.write(&state, old).unwrap();
        if let Some(bytes) = replacement {
            DirectFileSystem.write(&next, bytes).unwrap();
        }
        (
            directory,
            Self {
                state,
                replacement: next,
                armed: AtomicBool::new(replacement.is_some()),
                reads: Arc::new(AtomicUsize::new(0)),
            },
        )
    }
}

impl FileSystem for ReplaceOnStat {
    fn read(&self, p: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(p)
    }

    fn read_dir(&self, p: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(p)
    }

    fn metadata(&self, p: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(p)
    }

    fn symlink_metadata(&self, p: &Path) -> io::Result<HostMetadata> {
        let metadata = DirectFileSystem.symlink_metadata(p)?;
        if p == self.state && self.armed.swap(false, Ordering::SeqCst) {
            DirectFileSystem.rename(&self.replacement, &self.state)?;
        }
        Ok(metadata)
    }

    fn lock_file(&self, p: &Path) -> io::Result<cellar_ltx::HostFileLock> {
        DirectFileSystem.lock_file(p)
    }

    fn create_new(&self, p: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_new(p)
    }

    fn create(&self, p: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create(p)
    }

    fn open(&self, p: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(Counted(
            DirectFileSystem.open(p)?,
            self.reads.clone(),
        )))
    }

    fn write(&self, p: &Path, b: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(p, b)
    }

    fn sync_all(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.sync_all(p)
    }

    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        DirectFileSystem.rename(a, b)
    }

    fn remove_file(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(p)
    }

    fn remove_dir_all(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(p)
    }

    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(p)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn atomic_journal_reads_select_the_opened_snapshot() -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .blocking(|| -> anyhow::Result<()> {
            for (old, new) in [
                (b"old".as_slice(), b"complete-new-record".as_slice()),
                (b"complete-old-record".as_slice(), b"new".as_slice()),
            ] {
                let (directory, filesystem) = ReplaceOnStat::fixture(old, Some(new));
                assert_ne!(old.len(), new.len());
                assert_eq!(
                    read(&filesystem, directory.path(), "state")?,
                    Some(new.to_vec())
                );
                assert!(!filesystem.armed.load(Ordering::SeqCst));
                assert_eq!(filesystem.reads.load(Ordering::SeqCst), 1);
            }
            Ok(())
        })?
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn atomic_journal_reads_preserve_opened_bounds_and_type_checks() -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .blocking(|| -> anyhow::Result<()> {
            for replacement in [Vec::new(), vec![1; LIMIT + 1]] {
                let (directory, filesystem) =
                    ReplaceOnStat::fixture(b"valid-old-size", Some(&replacement));
                assert!(read(&filesystem, directory.path(), "state").is_err());
                assert!(!filesystem.armed.load(Ordering::SeqCst));
                assert_eq!(filesystem.reads.load(Ordering::SeqCst), 0);
            }
            let limit = vec![1; LIMIT];
            let (directory, filesystem) = ReplaceOnStat::fixture(&limit, None);
            assert_eq!(read(&filesystem, directory.path(), "state")?, Some(limit));
            DirectFileSystem.remove_file(&filesystem.state)?;
            DirectFileSystem.create_dir_all(&filesystem.state)?;
            let before = filesystem.reads.load(Ordering::SeqCst);
            assert!(read(&filesystem, directory.path(), "state").is_err());
            assert_eq!(filesystem.reads.load(Ordering::SeqCst), before);
            Ok(())
        })?
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn atomic_journal_load_preserves_identity_and_canonical_state() -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let history = GroupHistory {
        cluster: ClusterId::new([41; 16])?,
        namespace: NamespaceId::new([42; 16])?,
        group: GroupId::new([43; 16])?,
        incarnation: IncarnationId::new([44; 16])?,
    };
    let archive = CellGroupArchivePool::new(Default::default())?.for_group(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("atomic-journal"),
        history,
    )?;
    let prepared = archive
        .prepare_registry_creation(
            &domain,
            GroupRoute {
                history,
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                birth_digest: [45; 32],
                schema_digest: [46; 32],
            },
        )
        .await?;
    let creation = prepared.intent().clone();
    let registry = prepared.commit(&domain).await?;
    let intent = registry.prepare_upload(&domain).await?;
    let result = domain
        .blocking(move || -> anyhow::Result<()> {
            let (directory, filesystem) = ReplaceOnStat::fixture(b"unused", None);
            DirectFileSystem.remove_file(&filesystem.state)?;
            let filesystem = Arc::new(filesystem);
            let journal = Journal::open(
                filesystem.clone(),
                directory.path().to_owned(),
                [47; 16],
                creation,
            )?;
            assert!(matches!(journal.load()?.pending, Pending::None));
            let old = filesystem.read(&filesystem.state)?;
            journal.store(&Record {
                pending: Pending::Reserve(intent.clone()),
                ..Default::default()
            })?;
            let selected = filesystem.read(&filesystem.state)?;
            assert!(matches!(journal.load()?.pending, Pending::Reserve(_)));
            assert_ne!(old.len(), selected.len());
            journal.store(&Record::default())?;
            filesystem.write(&filesystem.replacement, &selected)?;
            filesystem.sync_all(&filesystem.replacement)?;
            filesystem.armed.store(true, Ordering::SeqCst);
            let Pending::Reserve(recovered) = journal.load()?.pending else {
                anyhow::bail!("lost atomic reserve")
            };
            assert_eq!(recovered, intent);
            assert!(!filesystem.armed.load(Ordering::SeqCst));
            assert_eq!(filesystem.read(&filesystem.state)?, selected);

            let foreign = encode(&Envelope {
                identity: Identity {
                    engine: [48; 16],
                    ..journal.identity.clone()
                },
                record: Record::default(),
            })?;
            let mut noncanonical = selected.clone();
            noncanonical.push(b'\n');
            for (bytes, reason) in [
                (foreign, "archive journal binding/canonical bytes"),
                (noncanonical, "archive journal binding/canonical bytes"),
                (
                    selected[..selected.len() / 2].to_vec(),
                    "archive journal encoding",
                ),
            ] {
                filesystem.write(&filesystem.replacement, &bytes)?;
                filesystem.sync_all(&filesystem.replacement)?;
                filesystem.rename(&filesystem.replacement, &filesystem.state)?;
                let error = match journal.load() {
                    Err(error) => error,
                    Ok(_) => anyhow::bail!("invalid record must refuse"),
                };
                assert!(
                    matches!(&error, Error::Invalid(message) if *message == reason),
                    "{error}"
                );
            }
            filesystem.write(&filesystem.replacement, &selected)?;
            filesystem.rename(&filesystem.replacement, &filesystem.state)?;
            let identity_path = directory.path().join("identity");
            let identity = filesystem.read(&identity_path)?;
            filesystem.write(&identity_path, b"invalid identity")?;
            assert!(matches!(
                journal.load(),
                Err(Error::Invalid("archive journal identity changed"))
            ));
            filesystem.write(&identity_path, &identity)?;
            filesystem.remove_file(&filesystem.state)?;
            assert!(matches!(
                journal.load(),
                Err(Error::Invalid("archive journal state lost"))
            ));
            Ok(())
        })?
        .await?;
    domain.shutdown().await;
    result
}
