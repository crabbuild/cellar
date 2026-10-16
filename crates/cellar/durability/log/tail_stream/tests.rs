use super::*;
use crate::node_log::{AppendReq, FollowerState};
use cellar_ltx::DirectFileSystem;
use cellar_ltx::{HostDirEntry, HostDirIterator, HostFileIo, HostMetadata};
use std::io;
use std::path::Path;
use tokio::io::AsyncWriteExt;

const LEADER: &str = "leader/11111111111111111111111111111111/generation";

fn source(path: &Path, sequences: &[u64], size: usize) -> Result<Arc<FollowerStore>> {
    let store = Arc::new(FollowerStore::new(path, None, "witness"));
    let state = FollowerState {
        fragment_epoch: 7,
        base: 0,
        end: sequences.iter().copied().max().unwrap_or(0),
        sealed_to: 7,
    };
    store.persist(LEADER, state)?;
    for &seq in sequences {
        let entry = Entry {
            seq,
            cell: "cell".into(),
            cell_epoch: 1,
            txid: seq,
            bytes: vec![seq as u8; size],
        };
        let bytes =
            super::super::encode_committed_batch(FollowerState { end: seq, ..state }, &[entry]);
        DirectFileSystem.write(
            &store.dir(LEADER).join(format!("{seq}-{seq}.batch")),
            &bytes,
        )?;
    }
    // Persist installs a warm cache. Return a new holder so snapshot creation
    // must recover the original files and independent state barrier cold.
    Ok(Arc::new(FollowerStore::new(path, None, "witness")))
}

fn request() -> TailReq {
    TailReq {
        leader: LEADER.into(),
    }
}

async fn exchange(store: Arc<FollowerStore>, directory: PathBuf) -> Result<Snapshot> {
    let (client, server) = tokio::io::duplex(64 << 10);
    let serving = crate::asyncrt::try_spawn(serve_tail_stream(server, store))?;
    let result = receive(
        client,
        &request(),
        Arc::new(DirectFileSystem),
        directory,
        Arc::default(),
    )
    .await;
    serving.await??;
    result
}

#[tokio::test]
async fn snapshot_transfer_retains_exact_rows_and_releases_follower_guard() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = source(directory.path(), &[10, 1, 9, 2, 3, 4, 5, 6, 7, 8], 3)?;
            let before = DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?;
            let mut snapshot = store.snapshot_tail(request(), Arc::default()).await?;
            assert!(
                store.guard(LEADER).try_lock().is_ok(),
                "network snapshot retained append/GC exclusion"
            );
            assert!(snapshot.range().complete);
            let mut seq = 0;
            snapshot.visit(&AtomicBool::new(false), |entry| {
                seq += 1;
                assert_eq!(entry.seq, seq);
                assert_eq!(entry.bytes, vec![seq as u8; 3]);
                Ok(())
            })?;
            assert_eq!(seq, 10);
            let mut received = exchange(store.clone(), directory.path().join("receiver")).await?;
            assert_eq!(received.range(), snapshot.range());
            received.visit(&AtomicBool::new(false), |entry| {
                assert_eq!(entry.txid, entry.seq);
                assert_eq!(entry.cell, "cell");
                assert_eq!(entry.bytes, vec![entry.seq as u8; 3]);
                Ok(())
            })?;
            assert_eq!(
                DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?,
                before
            );
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn missing_acknowledged_rows_remain_incomplete_and_refuse_warm_credit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = source(directory.path(), &[1, 2, 3], 3)?;
            assert_eq!(store.load(LEADER)?.end, 3);
            DirectFileSystem.remove_file(&store.dir(LEADER).join("2-2.batch"))?;
            let tail = exchange(store.clone(), directory.path().join("receiver")).await?;
            assert!(!tail.range().complete);
            assert_eq!(tail.range().end, 3);
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
            assert_eq!(store.load(LEADER)?.end, 3);
            assert_eq!(store.load(LEADER)?.sealed_to, 7);
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn maximum_admitted_entry_crosses_chunk_boundaries_without_refusal() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let size = super::super::COMMITTED_BATCH_ENTRY_CAP - ENTRY_HEADER_BYTES - "cell".len();
            let store = source(directory.path(), &[1], size)?;
            let mut received = exchange(store, directory.path().join("receiver")).await?;
            assert!(received.range().complete);
            received.visit(&AtomicBool::new(false), |entry| {
                assert_eq!(entry.bytes.len(), size);
                assert!(entry.bytes.iter().all(|byte| *byte == 1));
                Ok(())
            })?;
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn active_snapshots_bound_admission_and_drop_releases_capacity() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = source(directory.path(), &[1], 1)?;
            let first = store.snapshot_tail(request(), Arc::default()).await?;
            let second = store.snapshot_tail(request(), Arc::default()).await?;
            assert!(store
                .snapshot_tail(request(), Arc::default())
                .await
                .is_err());
            drop(first);
            let third = store.snapshot_tail(request(), Arc::default()).await?;
            drop((second, third));
            assert_eq!(store.tail_transfers.available_permits(), 2);
            let flag = Arc::new(AtomicBool::new(true));
            assert!(store.snapshot_tail(request(), flag).await.is_err());
            assert_eq!(store.tail_transfers.available_permits(), 2);
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

fn wire(sequences: &[u64]) -> (Manifest, Vec<u8>) {
    let mut body = Vec::new();
    for &seq in sequences {
        super::super::encode_entry(
            &Entry {
                seq,
                cell: "cell".into(),
                cell_epoch: 1,
                txid: seq,
                bytes: vec![seq as u8],
            },
            &mut body,
        );
    }
    (
        Manifest {
            leader: Sha256::digest(LEADER.as_bytes()).into(),
            range: TailRange {
                fragment_epoch: 7,
                base: 0,
                end: sequences.len() as u64,
                complete: true,
            },
            entries: sequences.len() as u64,
            bytes: body.len() as u64,
            digest: Sha256::digest(&body).into(),
        },
        body,
    )
}

async fn fake(
    manifest: Manifest,
    body: Vec<u8>,
    terminate: bool,
    directory: PathBuf,
) -> Result<Snapshot> {
    fake_fs(
        manifest,
        body,
        terminate,
        directory,
        Arc::new(DirectFileSystem),
        Arc::default(),
    )
    .await
}

async fn fake_fs(
    manifest: Manifest,
    body: Vec<u8>,
    terminate: bool,
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    cancelled: Arc<AtomicBool>,
) -> Result<Snapshot> {
    let (client, mut server) = tokio::io::duplex(64 << 10);
    let serving = crate::asyncrt::try_spawn(async move {
        read_limited(&mut server, REQUEST_BYTES).await?;
        super::super::write_frame(&mut server, b"CTP1").await?;
        super::super::write_frame(&mut server, &manifest.encode()).await?;
        if !body.is_empty() {
            super::super::write_frame(&mut server, &body).await?;
        }
        if terminate {
            super::super::write_frame(&mut server, &[]).await?;
        }
        Ok::<_, anyhow::Error>(())
    })?;
    let result = receive(client, &request(), filesystem, directory, cancelled).await;
    let _ = serving.await?;
    result
}

#[derive(Clone, Copy)]
enum ScratchFault {
    Normal,
    Read,
    Write,
    Short,
    Length,
    Corrupt,
    Cancel,
}

struct FaultFile {
    stall: Option<Arc<Stall>>,
    file: HostFile,
    fault: ScratchFault,
    flag: Arc<AtomicBool>,
    damage_at: u64,
}

impl HostFileIo for FaultFile {
    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if matches!(self.fault, ScratchFault::Read) {
            return Err(io::Error::other("injected tail scratch read"));
        }
        let mut bytes = self.file.read_exact_at(offset, len)?;
        match self.fault {
            ScratchFault::Short => {
                bytes.pop();
            }
            ScratchFault::Corrupt if offset >= self.damage_at => {
                if let Some(first) = bytes.first_mut() {
                    *first ^= 1;
                }
            }
            ScratchFault::Cancel => self.flag.store(true, Ordering::Release),
            _ => (),
        }
        Ok(bytes)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if let Some(stall) = &self.stall {
            stall.entered.notify_one();
            let mut released = stall.released.lock().unwrap();
            while !*released {
                released = stall.wake.wait(released).unwrap();
            }
        }
        if matches!(self.fault, ScratchFault::Write) {
            return Err(io::Error::other("injected tail scratch write"));
        }
        self.file.write_all(bytes)
    }

    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.file.file_len()? + u64::from(matches!(self.fault, ScratchFault::Length)))
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

struct FaultFs {
    stall: Option<Arc<Stall>>,
    flag: Arc<AtomicBool>,
    fault: Option<ScratchFault>,
}

impl FileSystem for FaultFs {
    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        Ok(HostFile::from_io(FaultFile {
            stall: self.stall.clone(),
            file: DirectFileSystem.temporary_file(directory)?,
            fault: self.fault.unwrap_or(ScratchFault::Cancel),
            flag: self.flag.clone(),
            damage_at: 0,
        }))
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        let entries = DirectFileSystem.read_dir_iter(path)?;
        let flag = self.flag.clone();
        let cancel = self.fault.is_none();
        Ok(Box::new(entries.inspect(move |_| {
            if cancel {
                flag.store(true, Ordering::Release);
            }
        })))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
}

#[tokio::test]
async fn cancelled_or_failed_cold_index_never_caches_partial_follower_state() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for (index, fault) in [
                None,
                Some(ScratchFault::Cancel),
                Some(ScratchFault::Write),
                Some(ScratchFault::Read),
                Some(ScratchFault::Short),
                Some(ScratchFault::Length),
                Some(ScratchFault::Corrupt),
            ]
            .into_iter()
            .enumerate()
            {
                let path = directory.path().join(index.to_string());
                let mut store = source(&path, &(1..=256).collect::<Vec<_>>(), 1)?;
                let before = DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?;
                let flag = Arc::new(AtomicBool::new(false));
                Arc::get_mut(&mut store).unwrap().filesystem = Arc::new(FaultFs {
                    stall: None,
                    flag: flag.clone(),
                    fault,
                });
                assert!(store.snapshot_tail(request(), flag.clone()).await.is_err());
                assert!(
                    !store.logs.lock().unwrap().contains_key(LEADER),
                    "failed cold scan cached partial state"
                );
                assert_eq!(store.tail_transfers.available_permits(), 2);
                assert_eq!(
                    DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?,
                    before
                );
                Arc::get_mut(&mut store).unwrap().filesystem = Arc::new(DirectFileSystem);
                flag.store(false, Ordering::Release);
                let restored = store.snapshot_tail(request(), flag).await?;
                assert!(restored.range().complete);
                assert_eq!(restored.range().end, 256);
            }
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn receiver_scratch_faults_cannot_supply_a_verified_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for (index, fault) in [
                ScratchFault::Write,
                ScratchFault::Read,
                ScratchFault::Short,
                ScratchFault::Length,
                ScratchFault::Corrupt,
                ScratchFault::Cancel,
            ]
            .into_iter()
            .enumerate()
            {
                let (manifest, bytes) = wire(&[1, 2]);
                let flag = Arc::new(AtomicBool::new(false));
                let path = directory.path().join(index.to_string());
                let filesystem = Arc::new(FaultFs {
                    stall: None,
                    flag: flag.clone(),
                    fault: Some(fault),
                });
                assert!(
                    fake_fs(manifest, bytes, true, path.clone(), filesystem, flag)
                        .await
                        .is_err()
                );
                assert!(
                    DirectFileSystem.read_dir(&path)?.is_empty(),
                    "failed receiver left scratch names"
                );
            }
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn late_snapshot_damage_aborts_the_shared_gather() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = source(directory.path(), &[1, 2], 1)?;
            let mut snapshot = store.snapshot_tail(request(), Arc::default()).await?;
            snapshot.file = Some(HostFile::from_io(FaultFile {
                stall: None,
                file: snapshot.file.take().unwrap(),
                fault: ScratchFault::Corrupt,
                flag: Arc::default(),
                damage_at: 77,
            }));
            let rows = recovery_rows::Rows::new(
                Arc::new(DirectFileSystem),
                directory.path().join("gather"),
            );
            assert!(snapshot
                .gather_into(
                    rows,
                    recovery_rows::Priority::Preferred,
                    recovery_rows::Duplicates::Last,
                    None
                )
                .is_err());
            assert!(DirectFileSystem
                .read_dir(&directory.path().join("gather"))?
                .is_empty());
            assert_eq!(store.tail_transfers.available_permits(), 2);
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn completion_cannot_hide_trailing_stream_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (client, mut server) = tokio::io::duplex(1024);
            let serving = crate::asyncrt::try_spawn(async move {
                read_limited(&mut server, REQUEST_BYTES).await?;
                super::super::write_frame(&mut server, &wire(&[]).0.encode()).await?;
                super::super::write_frame(&mut server, &[]).await?;
                server.write_all(b"extra").await?;
                Ok::<_, anyhow::Error>(())
            })?;
            let result = receive(
                client,
                &request(),
                Arc::new(DirectFileSystem),
                directory.path().to_owned(),
                Arc::default(),
            )
            .await;
            assert!(result
                .err()
                .context("trailing bytes accepted")?
                .to_string()
                .contains("trailing bytes"));
            serving.await??;
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn preparation_progress_alone_never_supplies_a_witness() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (client, mut server) = tokio::io::duplex(1024);
            let serving = crate::asyncrt::try_spawn(async move {
                read_limited(&mut server, REQUEST_BYTES).await?;
                for _ in 0..3 {
                    super::super::write_frame(&mut server, b"CTP1").await?;
                }
                Ok::<_, anyhow::Error>(())
            })?;
            assert!(receive(
                client,
                &request(),
                Arc::new(DirectFileSystem),
                directory.path().to_owned(),
                Arc::default()
            )
            .await
            .is_err());
            serving.await??;
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn incomplete_or_forged_streams_never_return_a_snapshot() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let mut cases = Vec::new();
            let (manifest, body) = wire(&[1, 2]);
            cases.push((manifest, body.clone(), false)); // exact payload, lost completion
            let mut changed = manifest;
            changed.leader[0] ^= 1;
            cases.push((changed, body.clone(), true));
            let mut changed = manifest;
            changed.range.end += 1;
            cases.push((changed, body.clone(), true));
            let mut changed = manifest;
            changed.entries += 1;
            cases.push((changed, body.clone(), true));
            let mut corrupt = body.clone();
            *corrupt.last_mut().unwrap() ^= 1;
            cases.push((manifest, corrupt, true));
            cases.push((manifest, body[..body.len() - 1].to_vec(), true));
            cases.push({
                let (manifest, body) = wire(&[1, 3]);
                (manifest, body, true)
            }); // valid SHA, gap
            cases.push({
                let (manifest, body) = wire(&[1, 1]);
                (manifest, body, true)
            }); // valid SHA, duplicate
            for (index, (manifest, body, terminate)) in cases.into_iter().enumerate() {
                let path = directory.path().join(index.to_string());
                assert!(
                    fake(manifest, body, terminate, path.clone()).await.is_err(),
                    "case {index} supplied witness authority"
                );
                if DirectFileSystem.metadata(&path).is_ok() {
                    assert!(DirectFileSystem.read_dir(&path)?.is_empty());
                }
            }
            let (mut empty, _) = wire(&[]);
            empty.range.base = 9;
            empty.range.end = 9;
            let received = fake(empty, vec![], true, directory.path().join("empty")).await?;
            assert!(received.range().complete);
            assert_eq!(received.range().base, 9);
            assert!(fake(
                empty,
                vec![],
                false,
                directory.path().join("incomplete-empty")
            )
            .await
            .is_err());
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn oversized_frame_is_refused_before_its_payload_is_read() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (mut server, mut client) = tokio::io::duplex(32);
            server
                .write_all(&((CHUNK_BYTES + 1) as u32).to_le_bytes())
                .await?;
            assert!(read_limited(&mut client, CHUNK_BYTES)
                .await
                .unwrap_err()
                .to_string()
                .contains("before allocation"));
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn receiver_payload_residency_stays_bounded() -> Result<()> {
    const CHILD: &str = "CELLAR_TEST_TAIL_RECEIVER_RSS";
    if std::env::var_os(CHILD).is_none() {
        let name = format!(
            "{}::receiver_payload_residency_stays_bounded",
            module_path!().split_once("::").unwrap().1
        );
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", &name, "--nocapture", "--test-threads=1"])
            .env(CHILD, "1")
            .output()?;
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        ensure!(
            output.status.success(),
            "isolated receiver failed: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let mut digest = Sha256::new();
            let mut bytes = 0;
            for seq in 1..=16 {
                let mut entry = Vec::new();
                super::super::encode_entry(
                    &Entry {
                        seq,
                        cell: "cell".into(),
                        cell_epoch: 1,
                        txid: seq,
                        bytes: vec![seq as u8; 8 << 20],
                    },
                    &mut entry,
                );
                digest.update(&entry);
                bytes += entry.len() as u64;
            }
            let manifest = Manifest {
                leader: Sha256::digest(LEADER.as_bytes()).into(),
                range: TailRange {
                    fragment_epoch: 7,
                    base: 0,
                    end: 16,
                    complete: true,
                },
                entries: 16,
                bytes,
                digest: digest.finalize().into(),
            };
            let baseline = crate::memory::sample().rss_bytes;
            ensure!(baseline > 0, "receiver RSS unavailable");
            let (client, mut server) = tokio::io::duplex(64 << 10);
            let serving = crate::asyncrt::try_spawn(async move {
                read_limited(&mut server, REQUEST_BYTES).await?;
                super::super::write_frame(&mut server, &manifest.encode()).await?;
                for seq in 1..=16 {
                    let mut entry = Vec::new();
                    super::super::encode_entry(
                        &Entry {
                            seq,
                            cell: "cell".into(),
                            cell_epoch: 1,
                            txid: seq,
                            bytes: vec![seq as u8; 8 << 20],
                        },
                        &mut entry,
                    );
                    for chunk in entry.chunks(CHUNK_BYTES) {
                        super::super::write_frame(&mut server, chunk).await?;
                    }
                }
                super::super::write_frame(&mut server, &[]).await?;
                Ok::<_, anyhow::Error>(())
            })?;
            let mut received = receive(
                client,
                &request(),
                Arc::new(DirectFileSystem),
                directory.path().join("receiver"),
                Arc::default(),
            )
            .await?;
            serving.await??;
            let mut peak = crate::memory::sample().rss_bytes;
            let mut count = 0;
            received.visit(&AtomicBool::new(false), |entry| {
                count += 1;
                assert_eq!(entry.seq, count);
                assert!(entry.bytes.iter().all(|byte| *byte == count as u8));
                peak = peak.max(crate::memory::sample().rss_bytes);
                Ok(())
            })?;
            assert_eq!(count, 16);
            let growth = peak.saturating_sub(baseline);
            eprintln!("tail receiver RSS: growth={growth}, baseline={baseline}, peak={peak}");
            ensure!(
                growth <= 64 << 20,
                "receiver retained history: growth={growth}"
            );
            Ok(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[derive(Default)]
struct Stall {
    entered: tokio::sync::Notify,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}

impl Stall {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct ReleaseOnDrop(Arc<Stall>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
// The caller owns the final join deadline after the tested domain closes.
#[allow(clippy::disallowed_methods)]
async fn disconnect_during_preparation_cancels_and_domain_joins_entered_io() -> Result<()> {
    use futures_util::FutureExt as _;
    let directory = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut store = domain
        .run(async { source(directory.path(), &(1..=256).collect::<Vec<_>>(), 1) })
        .await??;
    let stall = Arc::new(Stall::default());
    let _release = ReleaseOnDrop(stall.clone());
    Arc::get_mut(&mut store).unwrap().filesystem = Arc::new(FaultFs {
        stall: Some(stall.clone()),
        flag: Arc::default(),
        fault: Some(ScratchFault::Normal),
    });
    let before = DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?;
    let serving_store = store.clone();
    let result = domain
        .run(async {
            let (mut client, server) = tokio::io::duplex(1024);
            let serving = crate::asyncrt::try_spawn(serve_tail_stream(server, serving_store))?;
            super::super::write_frame(
                &mut client,
                &serde_json::to_vec(&TailWireReq::ranged(request()))?,
            )
            .await?;
            crate::asyncrt::timeout(Duration::from_secs(5), stall.entered.notified())
                .await
                .context("snapshot never entered actual scratch I/O")?;
            drop(client);
            let outcome = crate::asyncrt::timeout(Duration::from_secs(5), serving)
                .await
                .context("server ignored client EOF during preparation")??;
            ensure!(
                outcome.is_err(),
                "disconnected preparation returned success"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    let mut joined = Box::pin(domain.shutdown());
    let waited_for_worker = joined.as_mut().now_or_never().is_none();
    stall.release();
    tokio::time::timeout(Duration::from_secs(5), joined).await?;
    result??;
    ensure!(
        waited_for_worker,
        "domain reported joined before entered I/O ended"
    );
    ensure!(
        !store.logs.lock().unwrap().contains_key(LEADER),
        "cancelled cold worker installed a cache"
    );
    assert_eq!(store.tail_transfers.available_permits(), 2);
    assert_eq!(
        DirectFileSystem.read(&store.dir(LEADER).join("state.json"))?,
        before
    );
    Ok(())
}
