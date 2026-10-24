// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Check the actual cold-store read boundary, before metadata allocation.
use super::super::{AppendReq, FollowerStore, SealReq, TailReq};
use super::*;
use anyhow::Context as _;
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
    DirectoryLate,
    SubstituteBatch,
}

struct Probe {
    peak: AtomicU64,
    persistence: AtomicUsize,
    fault: Fault,
    batch: bool,
    cap: u64,
    rss_peak: Option<Arc<AtomicU64>>,
    reads: AtomicUsize,
}

impl Probe {
    fn target(&self, path: &Path) -> bool {
        if self.batch {
            path.extension().is_some_and(|suffix| suffix == "batch")
        } else {
            path.file_name().is_some_and(|name| name == "state.json")
        }
    }

    fn request(&self, bytes: u64) {
        self.peak.fetch_max(bytes, Ordering::SeqCst);
        if let Some(peak) = &self.rss_peak {
            peak.fetch_max(crate::memory::sample().rss_bytes, Ordering::SeqCst);
        }
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
        let mut bytes = self.inner.read_exact_at(offset, len)?;
        if self.state
            && self.probe.batch
            && matches!(self.probe.fault, Fault::SubstituteBatch)
            && self.probe.reads.fetch_add(1, Ordering::SeqCst) % 2 == 1
        {
            // Simulate a valid same-range file replacement between index and
            // payload reads, including a fresh valid source checksum.
            use sha2::Digest as _;
            let payload = bytes.len() - 32;
            bytes[payload - 1] ^= 1;
            let digest = sha2::Sha256::digest(&bytes[..payload]);
            bytes[payload..].copy_from_slice(&digest);
        }
        Ok(bytes)
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
    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        DirectFileSystem.temporary_file(directory)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<cellar_ltx::HostDirIterator> {
        let entries = DirectFileSystem.read_dir_iter(path)?;
        if matches!(self.0.fault, Fault::DirectoryLate) {
            return Ok(Box::new(entries.chain(std::iter::once(Err(
                io::Error::other("injected late directory failure"),
            )))));
        }
        Ok(entries)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        if self.0.target(path) {
            let len = DirectFileSystem.metadata(path)?.len;
            self.0.request(len);
            // The test records the unbounded request without allocating an
            // attacker-sized buffer itself. Ordinary DirectFileSystem::read
            // delegates to std::fs::read and has no such protection.
            if len > self.0.cap {
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
            state: self.0.target(path),
            lengths: 0,
        }))
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        if self.0.rss_peak.is_some() {
            return Err(io::Error::other(
                "eager directory enumeration during cold scan",
            ));
        }
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
        batch: false,
        cap: 4096,
        rss_peak: None,
        reads: AtomicUsize::new(0),
    });
    let mut store = FollowerStore::new(root, None, "witness");
    store.filesystem = Arc::new(Filesystem(probe.clone()));
    Ok((store, probe, path))
}

#[tokio::test]
async fn oversized_batch_preserves_the_required_end_without_requesting_payload(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory
        .path()
        .join("peerlog")
        .join(LEADER)
        .join("1-1.batch");
    DirectFileSystem.create_dir_all(path.parent().unwrap())?;
    // Fault fixture only: create a sparse oversized file without allocating
    // its payload. The instrumented reader also prevents the old whole-file
    // path from allocating it while measuring that unbounded request.
    let sparse = tempfile::NamedTempFile::new_in(directory.path())?;
    sparse.as_file().set_len((64 << 20) + 73)?;
    sparse.persist(&path)?;
    let seed = FollowerState {
        fragment_epoch: 7,
        base: 0,
        end: 1,
        sealed_to: 7,
    };
    let metadata = encode(LEADER, "witness", seed)?;
    DirectFileSystem.write(&path.with_file_name("state.json"), &metadata)?;
    let probe = Arc::new(Probe {
        peak: AtomicU64::new(0),
        persistence: AtomicUsize::new(0),
        fault: Fault::None,
        batch: true,
        cap: (64 << 20) + 72,
        rss_peak: None,
        reads: AtomicUsize::new(0),
    });
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let mut store = FollowerStore::new(directory.path(), None, "witness");
            store.filesystem = Arc::new(Filesystem(probe.clone()));
            for _ in 0..2 {
                assert!(store.load(LEADER)? == seed);
                let tail = store
                    .tail(&TailReq {
                        leader: LEADER.into(),
                    })
                    .await?;
                assert_eq!(tail.range.end, 1);
                assert!(!tail.range.complete);
                assert!(tail.entries.is_empty());
                let reply = store
                    .append(AppendReq {
                        leader: LEADER.into(),
                        epoch: 7,
                        truncate_to: 0,
                        entries: vec![],
                    })
                    .await;
                assert!(!reply.ok);
                assert_eq!(reply.end, 1);
            }
            assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
            assert_eq!(
                probe.peak.load(Ordering::SeqCst),
                0,
                "oversized batch requested payload bytes before rejection"
            );
            assert_eq!(DirectFileSystem.metadata(&path)?.len, (64 << 20) + 73);
            assert_eq!(
                DirectFileSystem.read(&path.with_file_name("state.json"))?,
                metadata
            );
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn invalid_first_batch_cannot_acknowledge_an_unrepresentable_entry() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (store, probe, _) = fixture(directory.path(), 1024, Fault::None)?;
            let response = store
                .append(AppendReq {
                    leader: LEADER.into(),
                    epoch: 7,
                    truncate_to: 0,
                    entries: vec![super::super::Entry {
                        seq: 1,
                        cell: "x".repeat(65536),
                        cell_epoch: 1,
                        txid: 1,
                        bytes: vec![1],
                    }],
                })
                .await;
            assert!(
                !response.ok,
                "follower acknowledged an entry whose cell length cannot fit CLE1"
            );
            assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn opened_batch_read_failures_cannot_credit_the_acknowledged_end() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for (index, fault) in [
                Fault::LengthNotFound,
                Fault::ReadNotFound,
                Fault::ChangedLength,
                Fault::ShortRead,
            ]
            .into_iter()
            .enumerate()
            {
                let root = directory.path().join(index.to_string());
                let path = root.join("peerlog").join(LEADER).join("1-1.batch");
                DirectFileSystem.create_dir_all(path.parent().unwrap())?;
                let seed = FollowerState {
                    fragment_epoch: 7,
                    base: 0,
                    end: 1,
                    sealed_to: 7,
                };
                let bytes = super::super::encode_committed_batch(
                    seed,
                    &[super::super::Entry {
                        seq: 1,
                        cell: "cell".into(),
                        cell_epoch: 1,
                        txid: 1,
                        bytes: vec![1, 2, 3],
                    }],
                );
                DirectFileSystem.write(&path, &bytes)?;
                let metadata = encode(LEADER, "witness", seed)?;
                DirectFileSystem.write(&path.with_file_name("state.json"), &metadata)?;
                let probe = Arc::new(Probe {
                    peak: AtomicU64::new(0),
                    persistence: AtomicUsize::new(0),
                    fault,
                    batch: true,
                    cap: (64 << 20) + 72,
                    rss_peak: None,
                    reads: AtomicUsize::new(0),
                });
                let mut store = FollowerStore::new(&root, None, "witness");
                store.filesystem = Arc::new(Filesystem(probe.clone()));
                for _ in 0..2 {
                    let tail = store
                        .tail(&TailReq {
                            leader: LEADER.into(),
                        })
                        .await?;
                    assert_eq!(tail.range.end, 1);
                    assert!(!tail.range.complete);
                    assert!(tail.entries.is_empty());
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
                assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
                assert_eq!(DirectFileSystem.read(&path)?, bytes);
                assert_eq!(
                    DirectFileSystem.read(&path.with_file_name("state.json"))?,
                    metadata
                );
                drop(store);
                let fresh = FollowerStore::new(&root, None, "witness");
                let recovered = fresh
                    .tail(&TailReq {
                        leader: LEADER.into(),
                    })
                    .await?;
                assert!(recovered.range.complete && recovered.range.end == 1);
                assert_eq!(recovered.entries[0].bytes, [1, 2, 3]);
            }
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
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

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn cold_scan_payload_residency_stays_bounded() -> anyhow::Result<()> {
    measured_history_residency("cold_scan_payload_residency_stays_bounded", false, false).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn seal_payload_residency_stays_bounded() -> anyhow::Result<()> {
    measured_history_residency("seal_payload_residency_stays_bounded", true, false).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn tail_transfer_payload_residency_stays_bounded() -> anyhow::Result<()> {
    measured_history_residency("tail_transfer_payload_residency_stays_bounded", false, true).await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn measured_history_residency(test_name: &str, seal: bool, tail: bool) -> anyhow::Result<()> {
    const CHILD: &str = "CELLAR_TEST_FOLLOWER_SCAN_RSS";
    if std::env::var_os(CHILD).is_none() {
        // A separate exact test process isolates the RSS observation from the
        // other SDK tests and their allocators. No production host reads this.
        let module = module_path!().split_once("::").unwrap().1;
        let name = format!("{module}::{test_name}");
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", &name, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::ensure!(
            output.status.success() && stdout.contains("1 passed; 0 failed"),
            "isolated cold scan failed: {stdout}\n{stderr}"
        );
        eprintln!("{stderr}");
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        let path = directory.path().join("peerlog").join(LEADER);
        DirectFileSystem.create_dir_all(&path)?;
        // Sixteen valid 8 MiB batches expose eager payload accumulation but
        // do not need a large file or an intentionally failed allocation.
        let seed = FollowerState { fragment_epoch: 7, base: 0, end: 16, sealed_to: 7 };
        for seq in 1..=16 {
            let entry = super::super::Entry {
                seq, cell: "cell".into(), cell_epoch: 1, txid: seq,
                bytes: vec![seq as u8; 8 << 20],
            };
            let bytes = super::super::encode_committed_batch(FollowerState { end: seq, ..seed }, &[entry]);
            DirectFileSystem.write(&path.join(format!("{seq}-{seq}.batch")), &bytes)?;
        }
        DirectFileSystem.write(&path.join("state.json"), &encode(LEADER, "witness", seed)?)?;
        let memory = Arc::new(AtomicU64::new(0));
        let probe = Arc::new(Probe {
            peak: AtomicU64::new(0), persistence: AtomicUsize::new(0), fault: Fault::None,
            batch: true, cap: (64 << 20) + 72, rss_peak: Some(memory.clone()), reads: AtomicUsize::new(0),
        });
        let mut store = FollowerStore::new(directory.path(), None, "witness");
        store.filesystem = Arc::new(Filesystem(probe));
        let store = Arc::new(store);
        if seal {
            assert!(store.load(LEADER)? == seed);
        }
        memory.store(0, Ordering::SeqCst);
        let baseline = crate::memory::sample().rss_bytes;
        anyhow::ensure!(baseline > 0, "cold-scan RSS unavailable on supported profile");
        let mut retained = None;
        if tail {
            let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut snapshot = store.snapshot_tail(TailReq { leader: LEADER.into() }, flag.clone()).await?;
            assert!(snapshot.range().complete);
            let mut count = 0;
            snapshot.visit(&flag, |entry| {
                count += 1;
                assert_eq!(entry.seq, count);
                assert_eq!(entry.bytes.len(), 8 << 20);
                assert!(entry.bytes.iter().all(|byte| *byte == count as u8));
                memory.fetch_max(crate::memory::sample().rss_bytes, Ordering::SeqCst);
                Ok(())
            })?;
            assert_eq!(count, 16);
            retained = Some(snapshot);
        } else if seal {
            let response = store.seal(&SealReq { leader: LEADER.into(), epoch: 8 }).await?;
            assert_eq!(response.fragment_epoch, 7);
            assert_eq!(response.base, Some(0));
            assert_eq!(response.end, 16);
            assert_eq!(store.load(LEADER)?.sealed_to, 8);
        } else {
            assert!(store.load(LEADER)? == seed);
        }
        let peak = memory.load(Ordering::SeqCst).max(crate::memory::sample().rss_bytes);
        let growth = peak.saturating_sub(baseline);
        std::hint::black_box(&retained);
        eprintln!("{test_name} RSS: growth={growth}, baseline={baseline}, peak={peak}");
        anyhow::ensure!(growth <= 64 << 20,
            "cold scan retained payloads across batches: growth={growth}, baseline={baseline}, peak={peak}, budget={}", 64 << 20);
        anyhow::Ok(())
    }).await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn cold_inventory_refuses_late_directory_failure_without_caching() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (mut store, probe, _) = fixture(directory.path(), 1024, Fault::DirectoryLate)?;
            for _ in 0..2 {
                let error = store
                    .load(LEADER)
                    .err()
                    .context("late enumeration must fail")?;
                assert!(error.to_string().contains("late directory failure"));
                assert!(store.logs.lock().unwrap().is_empty());
            }
            assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
            store.filesystem = Arc::new(DirectFileSystem);
            assert!(store.load(LEADER)? == state());
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn cold_inventory_spills_and_retains_missing_suffix_evidence() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let path = directory.path().join("peerlog").join(LEADER);
            DirectFileSystem.create_dir_all(&path)?;
            let seed = FollowerState {
                fragment_epoch: 7,
                base: 0,
                end: 0,
                sealed_to: 6,
            };
            DirectFileSystem.write(&path.join("state.json"), &encode(LEADER, "witness", seed)?)?;
            for seq in (1..=300).rev() {
                if seq == 151 {
                    continue;
                }
                let entry = super::super::Entry {
                    seq,
                    cell: "cell".into(),
                    cell_epoch: 1,
                    txid: seq,
                    bytes: vec![seq as u8],
                };
                let bytes = super::super::encode_committed_batch(
                    FollowerState { end: seq, ..seed },
                    &[entry],
                );
                DirectFileSystem.write(&path.join(format!("{seq}-{seq}.batch")), &bytes)?;
            }
            let store = FollowerStore::new(directory.path(), None, "witness");
            let loaded = store.load(LEADER)?;
            assert_eq!(loaded.end, 150);
            assert_eq!(loaded.sealed_to, 6);
            assert!(!store.recovered_disk_range_complete(LEADER));
            let tail = store
                .tail(&TailReq {
                    leader: LEADER.into(),
                })
                .await?;
            assert!(!tail.range.complete);
            assert_eq!(tail.entries.len(), 299);
            assert_eq!(tail.entries[149].seq, 150);
            assert_eq!(tail.entries[150].seq, 152);
            assert_eq!(
                DirectFileSystem.read_dir(&path)?.len(),
                300,
                "derived scratch left named files"
            );
            // The independent acknowledged end must not shrink to the last
            // contiguous prefix when the same inventory has a retained gap.
            DirectFileSystem.write(
                &path.join("state.json"),
                &encode(LEADER, "witness", FollowerState { end: 300, ..seed })?,
            )?;
            let acknowledged = FollowerStore::new(directory.path(), None, "witness");
            assert_eq!(acknowledged.load(LEADER)?.end, 300);
            let probe = acknowledged
                .append(AppendReq {
                    leader: LEADER.into(),
                    epoch: 7,
                    truncate_to: 0,
                    entries: Vec::new(),
                })
                .await;
            assert!(!probe.ok);
            assert_eq!(probe.end, 300);
            let seal = acknowledged
                .seal(&SealReq {
                    leader: LEADER.into(),
                    epoch: 9,
                })
                .await?;
            assert_eq!(seal.end, 300);
            assert_eq!(seal.fragment_epoch, 0);
            assert_eq!(acknowledged.load(LEADER)?.sealed_to, 9);
            // Only a verified batch's bucket base may discharge the hole. The
            // original durable seed alone cannot advance past missing sequence 151.
            let entry = super::super::Entry {
                seq: 301,
                cell: "cell".into(),
                cell_epoch: 1,
                txid: 301,
                bytes: vec![1],
            };
            let bytes = super::super::encode_committed_batch(
                FollowerState {
                    base: 151,
                    end: 301,
                    ..seed
                },
                &[entry],
            );
            DirectFileSystem.write(&path.join("301-301.batch"), &bytes)?;
            let cold = FollowerStore::new(directory.path(), None, "witness");
            let recovered = cold.load(LEADER)?;
            assert_eq!(recovered.base, 151);
            assert_eq!(recovered.end, 301);
            assert_eq!(recovered.sealed_to, 9);
            assert!(
                cold.tail(&TailReq {
                    leader: LEADER.into()
                })
                .await?
                .range
                .complete
            );
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn tail_refuses_valid_payload_substitution_after_indexing() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let path = directory.path().join("peerlog").join(LEADER);
            DirectFileSystem.create_dir_all(&path)?;
            let seed = FollowerState {
                fragment_epoch: 7,
                base: 0,
                end: 1,
                sealed_to: 8,
            };
            DirectFileSystem.write(&path.join("state.json"), &encode(LEADER, "witness", seed)?)?;
            let entry = super::super::Entry {
                seq: 1,
                cell: "cell".into(),
                cell_epoch: 1,
                txid: 1,
                bytes: vec![1],
            };
            let bytes = super::super::encode_committed_batch(seed, &[entry]);
            DirectFileSystem.write(&path.join("1-1.batch"), &bytes)?;
            let probe = Arc::new(Probe {
                peak: AtomicU64::new(0),
                persistence: AtomicUsize::new(0),
                fault: Fault::SubstituteBatch,
                batch: true,
                cap: (64 << 20) + 72,
                rss_peak: None,
                reads: AtomicUsize::new(0),
            });
            let mut store = FollowerStore::new(directory.path(), None, "witness");
            store.filesystem = Arc::new(Filesystem(probe.clone()));
            assert!(store.load(LEADER)? == seed);
            let error = store
                .tail(&TailReq {
                    leader: LEADER.into(),
                })
                .await
                .err()
                .context("changed payload cannot supply tail evidence")?;
            assert!(error.to_string().contains("indexed follower batch changed"));
            let probe_reply = store
                .append(AppendReq {
                    leader: LEADER.into(),
                    epoch: 7,
                    truncate_to: 0,
                    entries: Vec::new(),
                })
                .await;
            assert!(!probe_reply.ok);
            assert_eq!(probe_reply.end, 1);
            assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
            assert_eq!(DirectFileSystem.read(&path.join("1-1.batch"))?, bytes);
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn noncanonical_batch_names_never_supply_recovery_proof() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for (index, name) in ["01-1.batch", "1-01.batch", "+1-1.batch"]
                .into_iter()
                .enumerate()
            {
                let root = directory.path().join(index.to_string());
                let path = root.join("peerlog").join(LEADER);
                DirectFileSystem.create_dir_all(&path)?;
                let seed = FollowerState {
                    fragment_epoch: 7,
                    base: 0,
                    end: 1,
                    sealed_to: 0,
                };
                DirectFileSystem
                    .write(&path.join("state.json"), &encode(LEADER, "witness", seed)?)?;
                let entry = super::super::Entry {
                    seq: 1,
                    cell: "cell".into(),
                    cell_epoch: 1,
                    txid: 1,
                    bytes: vec![1],
                };
                let bytes = super::super::encode_committed_batch(seed, &[entry]);
                DirectFileSystem.write(&path.join(name), &bytes)?;
                let store = FollowerStore::new(&root, None, "witness");
                assert!(store.load(LEADER)? == seed);
                let tail = store
                    .tail(&TailReq {
                        leader: LEADER.into(),
                    })
                    .await?;
                assert!(!tail.range.complete);
                assert!(tail.entries.is_empty());
                let seal = store
                    .seal(&SealReq {
                        leader: LEADER.into(),
                        epoch: 8,
                    })
                    .await?;
                assert_eq!(seal.fragment_epoch, 0);
                assert_eq!(seal.end, 1);
                assert_eq!(DirectFileSystem.read(&path.join(name))?, bytes);
            }
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn warm_tail_missing_batch_refuses_cached_credit() -> anyhow::Result<()> {
    warm_observed_loss(false, false).await
}

#[tokio::test]
async fn warm_tail_corrupt_batch_refuses_cached_credit() -> anyhow::Result<()> {
    warm_observed_loss(false, true).await
}

#[tokio::test]
async fn warm_seal_missing_batch_refuses_cached_credit() -> anyhow::Result<()> {
    warm_observed_loss(true, false).await
}

#[tokio::test]
async fn warm_seal_corrupt_batch_refuses_cached_credit() -> anyhow::Result<()> {
    warm_observed_loss(true, true).await
}

#[tokio::test]
async fn warm_directory_observation_failure_does_not_invent_missing_data() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let mut store = FollowerStore::new(directory.path(), None, "witness");
            store.persist(LEADER, state())?;
            assert!(
                store
                    .append(AppendReq {
                        leader: LEADER.into(),
                        epoch: 7,
                        truncate_to: 0,
                        entries: vec![super::super::Entry {
                            seq: 1,
                            cell: "cell".into(),
                            cell_epoch: 1,
                            txid: 1,
                            bytes: vec![1]
                        }],
                    })
                    .await
                    .ok
            );
            let before = store
                .tail(&TailReq {
                    leader: LEADER.into(),
                })
                .await?;
            assert!(before.range.complete);
            let probe = Arc::new(Probe {
                peak: AtomicU64::new(0),
                persistence: AtomicUsize::new(0),
                fault: Fault::DirectoryLate,
                batch: false,
                cap: 4096,
                rss_peak: None,
                reads: AtomicUsize::new(0),
            });
            store.filesystem = Arc::new(Filesystem(probe.clone()));
            assert!(store
                .tail(&TailReq {
                    leader: LEADER.into()
                })
                .await
                .is_err());
            assert!(
                !store
                    .logs
                    .lock()
                    .unwrap()
                    .get(LEADER)
                    .unwrap()
                    .acknowledged_range_missing
            );
            assert_eq!(probe.persistence.load(Ordering::SeqCst), 0);
            store.filesystem = Arc::new(DirectFileSystem);
            let returned = store
                .tail(&TailReq {
                    leader: LEADER.into(),
                })
                .await?;
            assert_eq!(
                super::super::encode_tail_resp(&returned),
                super::super::encode_tail_resp(&before)
            );
            assert!(
                store
                    .append(AppendReq {
                        leader: LEADER.into(),
                        epoch: 7,
                        truncate_to: 0,
                        entries: Vec::new()
                    })
                    .await
                    .ok
            );
            anyhow::Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

async fn warm_observed_loss(seal: bool, corrupt: bool) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        let store = FollowerStore::new(directory.path(), None, "witness");
        let seed = FollowerState { fragment_epoch: 7, base: 0, end: 0, sealed_to: 0 };
        store.persist(LEADER, seed)?;
        let entry = |seq| super::super::Entry { seq, cell: "cell".into(), cell_epoch: 1, txid: seq, bytes: vec![seq as u8] };
        for seq in 1..=2 {
            let response = store.append(AppendReq { leader: LEADER.into(), epoch: 7, truncate_to: 0, entries: vec![entry(seq)] }).await;
            assert!(response.ok);
            assert_eq!(response.end, seq);
        }
        let before = store.tail(&TailReq { leader: LEADER.into() }).await?;
        assert!(before.range.complete);
        assert_eq!(before.entries.len(), 2);
        let path = store.dir(LEADER).join("2-2.batch");
        let saved = DirectFileSystem.read(&path)?;
        if corrupt {
            let mut damaged = saved.clone();
            *damaged.last_mut().unwrap() ^= 1;
            DirectFileSystem.write(&path, &damaged)?;
        } else {
            DirectFileSystem.remove_file(&path)?;
        }
        if seal {
            let response = store.seal(&SealReq { leader: LEADER.into(), epoch: 7 }).await?;
            assert_eq!(response.fragment_epoch, 0);
            assert_eq!(response.end, 2);
        } else {
            let response = store.tail(&TailReq { leader: LEADER.into() }).await?;
            assert!(!response.range.complete);
            assert_eq!(response.range.end, 2);
        }
        let observed_state = DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?;
        let mut credit = Vec::new();
        // Probe, exact retransmission and new suffix must all refuse after
        // observing that the acknowledged prefix is no longer readable.
        for entries in [Vec::new(), vec![entry(2)], vec![entry(3)]] {
            credit.push(store.append(AppendReq { leader: LEADER.into(), epoch: 7, truncate_to: 0, entries }).await.ok);
        }
        anyhow::ensure!(credit.iter().all(|ok| !ok), "known damaged warm witness recredited data: seal={seal}, corrupt={corrupt}, credit={credit:?}");
        assert_eq!(store.load(LEADER)?.end, 2);
        assert_eq!(DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?, observed_state);
        // Restore only the original bytes. A warm cache stays conservative;
        // a cold verification may reestablish the exact retained range.
        DirectFileSystem.write(&path, &saved)?;
        assert!(!store.append(AppendReq { leader: LEADER.into(), epoch: 7, truncate_to: 0, entries: Vec::new() }).await.ok);
        let cold = FollowerStore::new(directory.path(), None, "witness");
        let returned = cold.tail(&TailReq { leader: LEADER.into() }).await?;
        assert!(returned.range.complete);
        assert_eq!(super::super::encode_tail_resp(&returned), super::super::encode_tail_resp(&before));
        assert!(cold.append(AppendReq { leader: LEADER.into(), epoch: 7, truncate_to: 0, entries: Vec::new() }).await.ok);
        assert_eq!(DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?, observed_state);
        anyhow::Ok(())
    }).await?;
    domain.shutdown().await;
    result
}
