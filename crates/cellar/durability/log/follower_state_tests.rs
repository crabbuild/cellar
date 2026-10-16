//! Check the actual cold-store read boundary, before metadata allocation.
use super::super::{AppendReq, FollowerStore, SealReq, TailReq};
use super::*;
use cellar_ltx::{DirectFileSystem, FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

const LEADER: &str = "leader/11111111111111111111111111111111/generation";

#[derive(Clone, Copy)]
enum Fault {
    None,
    LengthNotFound,
    ReadNotFound,
    ChangedLength,
    ShortRead,
}

struct Probe {
    peak: AtomicU64,
    persistence: AtomicUsize,
    fault: Fault,
}

impl Probe {
    fn request(&self, bytes: u64) {
        self.peak.fetch_max(bytes, Ordering::SeqCst);
    }

    fn mutation(&self) {
        self.persistence.fetch_add(1, Ordering::SeqCst);
    }
}

struct Filesystem(Arc<Probe>);

struct File {
    inner: HostFile,
    probe: Arc<Probe>,
    state: bool,
    lengths: usize,
}

impl HostFileIo for File {
    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if self.state {
            self.probe.request(len as u64);
            if matches!(self.probe.fault, Fault::ReadNotFound) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "injected opened-file read failure",
                ));
            }
            if matches!(self.probe.fault, Fault::ShortRead) {
                return self.inner.read_exact_at(offset, len.saturating_sub(1));
            }
        }
        self.inner.read_exact_at(offset, len)
    }

    fn file_len(&mut self) -> io::Result<u64> {
        if self.state && matches!(self.probe.fault, Fault::LengthNotFound) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "injected opened-file stat failure",
            ));
        }
        let len = self.inner.file_len()?;
        let changed =
            self.state && self.lengths > 0 && matches!(self.probe.fault, Fault::ChangedLength);
        self.lengths += 1;
        Ok(len + u64::from(changed))
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.probe.mutation();
        self.inner.write_all(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.probe.mutation();
        self.inner.sync_all()
    }
}

impl FileSystem for Filesystem {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        if path.file_name().is_some_and(|name| name == "state.json") {
            let len = DirectFileSystem.metadata(path)?.len;
            self.0.request(len);
            // The test records the unbounded request without allocating an
            // attacker-sized buffer itself. Ordinary DirectFileSystem::read
            // delegates to std::fs::read and has no such protection.
            if len > 4096 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "test prevented an oversized whole-file read",
                ));
            }
        }
        DirectFileSystem.read(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(File {
            inner: DirectFileSystem.open(path)?,
            probe: self.0.clone(),
            state: path.file_name().is_some_and(|name| name == "state.json"),
            lengths: 0,
        }))
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        self.0.mutation();
        DirectFileSystem.create(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        self.0.mutation();
        DirectFileSystem.create_new(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.0.mutation();
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.0.mutation();
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        self.0.mutation();
        DirectFileSystem.rename(a, b)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.0.mutation();
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.0.mutation();
        DirectFileSystem.remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.0.mutation();
        DirectFileSystem.create_dir_all(path)
    }
}

fn state() -> FollowerState {
    FollowerState {
        fragment_epoch: 7,
        base: 0,
        end: 0,
        sealed_to: 4,
    }
}

fn fixture(
    root: &Path,
    size: usize,
    fault: Fault,
) -> anyhow::Result<(FollowerStore, Arc<Probe>, PathBuf)> {
    let path = root.join("peerlog").join(LEADER).join("state.json");
    DirectFileSystem.create_dir_all(path.parent().unwrap())?;
    let mut bytes = encode(LEADER, "witness", state())?;
    bytes.resize(size, b' ');
    DirectFileSystem.write(&path, &bytes)?;
    let probe = Arc::new(Probe {
        peak: AtomicU64::new(0),
        persistence: AtomicUsize::new(0),
        fault,
    });
    let mut store = FollowerStore::new(root, None, "witness");
    store.filesystem = Arc::new(Filesystem(probe.clone()));
    Ok((store, probe, path))
}

#[tokio::test]
async fn oversized_state_is_refused_before_any_payload_read() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for size in [4097, 8 << 20] {
                let (store, probe, path) =
                    fixture(&directory.path().join(size.to_string()), size, Fault::None)?;
                for _ in 0..2 {
                    assert!(store.load(LEADER).is_err());
                    assert!(store
                        .tail(&TailReq {
                            leader: LEADER.into()
                        })
                        .await
                        .is_err());
                    assert!(store
                        .seal(&SealReq {
                            leader: LEADER.into(),
                            epoch: 7
                        })
                        .await
                        .is_err());
                    assert!(
                        !store
                            .append(AppendReq {
                                leader: LEADER.into(),
                                epoch: 7,
                                truncate_to: 0,
                                entries: vec![]
                            })
                            .await
                            .ok
                    );
                }
                assert_eq!(
                    probe.peak.load(Ordering::SeqCst),
                    0,
                    "oversized metadata requested payload bytes before rejection"
                );
                assert_eq!(
                    probe.persistence.load(Ordering::SeqCst),
                    0,
                    "refused metadata triggered persistence"
                );
                assert_eq!(DirectFileSystem.metadata(&path)?.len, size as u64);
            }
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn exact_state_bound_is_read_without_losing_the_seal_floor() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (store, probe, path) = fixture(directory.path(), 4096, Fault::None)?;
            assert!(store.load(LEADER)? == state());
            assert_eq!(probe.peak.load(Ordering::SeqCst), 4096);
            assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
            assert_eq!(DirectFileSystem.metadata(&path)?.len, 4096);
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn opened_state_failures_never_become_a_fresh_fragment() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for (i, fault) in [
                Fault::LengthNotFound,
                Fault::ReadNotFound,
                Fault::ChangedLength,
                Fault::ShortRead,
            ]
            .into_iter()
            .enumerate()
            {
                let root = directory.path().join(i.to_string());
                let (store, probe, path) = fixture(&root, 1024, fault)?;
                let saved = DirectFileSystem.read(&path)?;
                for _ in 0..2 {
                    assert!(
                        store.load(LEADER).is_err(),
                        "opened-file failure became empty or cached state"
                    );
                }
                assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
                assert_eq!(DirectFileSystem.read(&path)?, saved);
                let clean = FollowerStore::new(&root, None, "witness");
                assert!(
                    clean.load(LEADER)? == state(),
                    "failed reader modified the original metadata"
                );
            }
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}
