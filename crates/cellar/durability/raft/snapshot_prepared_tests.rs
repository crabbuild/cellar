// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;
use crate::asyncrt::{ExecutionDomain, TaskHandle};
use anyhow::{ensure, Result};
use cellar_logic::consensus::*;
use cellar_ltx::{
    apply::ImageState,
    host::{FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata},
    stream_snapshot::{self, ScratchLimits, ScratchPool},
    DirectFileSystem, TXID,
};
use sha2::{Digest, Sha256};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{Condvar, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Metrics {
    files: AtomicUsize,
    builds: AtomicUsize,
    corrupt_read: AtomicBool,
    hold_close: AtomicBool,
    entered: tokio::sync::Notify,
    resumed: Mutex<bool>,
    resume: Condvar,
    close_threads: Mutex<Vec<std::thread::ThreadId>>,
}

impl Metrics {
    fn pause(&self) {
        self.entered.notify_one();
        let mut resumed = self.resumed.lock().unwrap();
        while !*resumed {
            resumed = self.resume.wait(resumed).unwrap();
        }
    }

    fn release(&self) {
        *self.resumed.lock().unwrap() = true;
        self.resume.notify_all();
    }
}

struct File {
    inner: HostFile,
    metrics: Arc<Metrics>,
}

impl Drop for File {
    fn drop(&mut self) {
        self.metrics
            .close_threads
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        if self.metrics.hold_close.load(Ordering::Acquire) {
            self.metrics.pause();
        }
        self.metrics.files.fetch_sub(1, Ordering::AcqRel);
    }
}

impl HostFileIo for File {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes)
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all_at(offset, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let mut bytes = self.inner.read_exact_at(offset, length)?;
        if self.metrics.corrupt_read.swap(false, Ordering::AcqRel) {
            if let Some(first) = bytes.first_mut() {
                *first ^= 1;
            }
        }
        Ok(bytes)
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.inner.sync_all()
    }
}

#[derive(Clone)]
struct Files {
    root: PathBuf,
    metrics: Arc<Metrics>,
}

impl FileSystem for Files {
    fn temporary_file(&self, _: Option<&Path>) -> io::Result<HostFile> {
        let inner = DirectFileSystem.temporary_file(Some(&self.root))?;
        self.metrics.files.fetch_add(1, Ordering::AcqRel);
        Ok(HostFile::from_io(File {
            inner,
            metrics: self.metrics.clone(),
        }))
    }

    fn read(&self, _: &Path) -> io::Result<Vec<u8>> {
        Err(unsupported())
    }

    fn read_dir(&self, _: &Path) -> io::Result<Vec<HostDirEntry>> {
        Err(unsupported())
    }

    fn metadata(&self, _: &Path) -> io::Result<HostMetadata> {
        Err(unsupported())
    }

    fn create(&self, _: &Path) -> io::Result<HostFile> {
        Err(unsupported())
    }

    fn open(&self, _: &Path) -> io::Result<HostFile> {
        Err(unsupported())
    }

    fn write(&self, _: &Path, _: &[u8]) -> io::Result<()> {
        Err(unsupported())
    }

    fn sync_all(&self, _: &Path) -> io::Result<()> {
        Err(unsupported())
    }

    fn rename(&self, _: &Path, _: &Path) -> io::Result<()> {
        Err(unsupported())
    }

    fn remove_file(&self, _: &Path) -> io::Result<()> {
        Err(unsupported())
    }

    fn remove_dir_all(&self, _: &Path) -> io::Result<()> {
        Err(unsupported())
    }

    fn create_dir_all(&self, _: &Path) -> io::Result<()> {
        Err(unsupported())
    }
}

fn unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "prepared fixture supports anonymous spools only",
    )
}

struct Fixture {
    _root: tempfile::TempDir,
    domain: ExecutionDomain,
    worker: Option<TaskHandle<()>>,
    cache: Cache,
    files: Files,
    scratch: ScratchPool,
    bytes: Arc<Vec<u8>>,
    key: Key,
    performance: Arc<Registry>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("source.sqlite");
        let db = rusqlite::Connection::open(&path)?;
        db.pragma_update(None, "page_size", 4096)?;
        db.execute_batch("CREATE TABLE item(value); INSERT INTO item VALUES(7)")?;
        drop(db);
        let bytes = Arc::new(std::fs::read(path)?);
        let image = ImageState {
            epoch: 1,
            txid: TXID(1),
            page_size: 4096,
            checksum: 0,
            length: bytes.len() as u64,
            digest: Sha256::digest(bytes.as_slice()).into(),
        };
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let performance = domain
            .run(async { Registry::new(crate::asyncrt::services().domain_token().unwrap()) })
            .await?;
        let (cache, owner) = channel(performance.clone());
        let worker = domain.blocking(move || owner.run())?;
        let key = Key {
            owner: Arc::new(()),
            replica: ReplicaIdentity {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Counter:prepared".into(),
                    group: GroupId::new([3; 16])?,
                    incarnation: IncarnationId::new([4; 16])?,
                },
                member: MemberId::new(1)?,
                node: NodeId::new([5; 16])?,
                disk: DiskLineage::new([6; 16])?,
                process: ProcessGeneration::new([7; 16])?,
            },
            birth: [8; 32],
            schema: [9; 32],
            limits: SnapshotLimits::default(),
            image: CommittedImage {
                name: "verified.sqlite".into(),
                commitment: image.digest.into(),
                length: image.length,
                ltx_epoch: image.epoch,
                txid: image.txid.0,
                checksum: image.checksum,
                page_size: image.page_size,
            },
        };
        let files = Files {
            root: root.path().to_path_buf(),
            metrics: Arc::default(),
        };
        Ok(Self {
            _root: root,
            domain,
            worker: Some(worker),
            cache,
            files,
            scratch: ScratchPool::new(ScratchLimits {
                max_bytes: 4 * 1024 * 1024,
                max_files: 4,
            })?,
            bytes,
            key,
            performance,
        })
    }

    fn builder(
        &self,
        key: &Key,
    ) -> Result<impl FnOnce() -> Result<SnapshotFile, SnapshotError> + Send + 'static> {
        let files = self.files.clone();
        let scratch = self.scratch.clone();
        let bytes = self.bytes.clone();
        let image = marker_state(&key.image)?;
        Ok(move || {
            files.metrics.builds.fetch_add(1, Ordering::AcqRel);
            Ok(stream_snapshot::encode_admitted_in(
                &files,
                &files.root,
                image,
                stream_snapshot::Limits::default(),
                &scratch,
                |offset, length| Ok(bytes[offset as usize..offset as usize + length].to_vec()),
            )?)
        })
    }

    async fn prepare(&self, key: Key) -> Result<Prepared> {
        let cache = self.cache.clone();
        let build = self.builder(&key)?;
        Ok(self
            .domain
            .blocking(move || cache.prepare(key, build))?
            .await??)
    }

    async fn read(&self, payload: Shared) -> Result<(Shared, Vec<u8>)> {
        Ok(self
            .domain
            .blocking(move || {
                let bytes = payload.chunk(0, payload.metadata().encoded_length as usize)?;
                Ok::<_, SnapshotError>((payload, bytes))
            })?
            .await??)
    }

    async fn trim(&self) -> Result<bool> {
        let cache = self.cache.clone();
        Ok(self.domain.blocking(move || cache.trim_idle())?.await??)
    }

    async fn stop(&mut self) -> Result<()> {
        self.cache.stop();
        if let Some(worker) = self.worker.take() {
            worker.await?;
        }
        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        self.files.metrics.release();
        self.stop().await?;
        self.domain.shutdown().await;
        ensure!(self.scratch.usage()?.reserved_files == 0);
        ensure!(self.cache.metadata.usage() == (0, 0));
        ensure!(self.cache.operations.usage() == (0, 0));
        Ok(())
    }
}

#[tokio::test]
async fn compatible_limits_reuse_one_body_and_live_consumers_prevent_eviction() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let first = fixture.prepare(fixture.key.clone()).await?;
        ensure!(!first.reused);
        let mut key = fixture.key.clone();
        key.limits.max_chunk_bytes = 512 * 1024;
        let second = fixture.prepare(key).await?;
        ensure!(second.reused);
        ensure!(fixture.files.metrics.builds.load(Ordering::Acquire) == 1);
        drop(first);
        ensure!(!fixture.trim().await?);
        let (payload, bytes) = fixture.read(second.payload).await?;
        ensure!(<[u8; 32]>::from(Sha256::digest(&bytes)) == payload.metadata().encoded_digest);
        drop(payload);
        ensure!(fixture.trim().await?);
        ensure!(fixture.scratch.usage()?.reserved_files == 0);
        ensure!(
            fixture.performance.snapshot().counters[Counter::SnapshotPreparedHits as usize].value
                == 1
        );
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test]
async fn foreign_owner_history_schema_and_generation_never_reuse() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        for field in 0..6 {
            drop(fixture.prepare(fixture.key.clone()).await?);
            let mut key = fixture.key.clone();
            match field {
                0 => key.owner = Arc::new(()),
                1 => key.schema = [99; 32],
                2 => key.birth = [99; 32],
                3 => key.replica.scope.incarnation = IncarnationId::new([99; 16])?,
                4 => key.image.name = "different-generation.sqlite".into(),
                _ => key.image.ltx_epoch += 1,
            }
            let changed = fixture.prepare(key).await?;
            ensure!(!changed.reused, "foreign field {field} reused bytes");
            drop(changed);
            ensure!(fixture.trim().await?);
        }
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test]
async fn active_body_refuses_replacement_and_narrow_limits_refuse_without_poisoning() -> Result<()>
{
    let fixture = Fixture::new().await?;
    let result = async {
        let original = fixture.prepare(fixture.key.clone()).await?;
        let mut foreign = fixture.key.clone();
        foreign.schema = [99; 32];
        ensure!(fixture.prepare(foreign).await.is_err());
        let mut narrow = fixture.key.clone();
        narrow.limits.max_image_bytes = 4096;
        ensure!(fixture.prepare(narrow).await.is_err());
        ensure!(fixture.files.metrics.builds.load(Ordering::Acquire) == 1);
        let (_payload, bytes) = fixture.read(original.payload).await?;
        ensure!(!bytes.is_empty());
        let reused = fixture.prepare(fixture.key.clone()).await?;
        ensure!(reused.reused);
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test]
async fn corrupt_cached_bytes_fail_the_stream_digest_and_are_rebuilt() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        let first = fixture.prepare(fixture.key.clone()).await?;
        fixture
            .files
            .metrics
            .corrupt_read
            .store(true, Ordering::Release);
        let (payload, bytes) = fixture.read(first.payload).await?;
        let old_id = payload.id;
        let invalid = super::super::SourcePayload::Shared(payload);
        ensure!(invalid
            .validate_digest(Sha256::digest(bytes).into())
            .is_err());
        let rebuilt = fixture.prepare(fixture.key.clone()).await?;
        ensure!(!rebuilt.reused);
        ensure!(fixture.files.metrics.builds.load(Ordering::Acquire) == 2);
        // A late old invalidation cannot discard the replacement's body.
        ensure!(fixture
            .cache
            .requests
            .send(Request::Invalidate(old_id))
            .is_ok());
        let (payload, bytes) = fixture.read(rebuilt.payload).await?;
        let valid = super::super::SourcePayload::Shared(payload);
        valid.validate_digest(Sha256::digest(bytes).into())?;
        ensure!(invalid.chunk(0, 1).is_err());
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test]
async fn idle_file_close_stays_on_the_physical_owner_and_keeps_its_charge() -> Result<()> {
    let fixture = Fixture::new().await?;
    let result = async {
        drop(fixture.prepare(fixture.key.clone()).await?);
        fixture
            .files
            .metrics
            .hold_close
            .store(true, Ordering::Release);
        let cache = fixture.cache.clone();
        let returned = Arc::new(AtomicBool::new(false));
        let finished = returned.clone();
        let trim = fixture.domain.blocking(move || {
            let result = cache.trim_idle();
            finished.store(true, Ordering::Release);
            result
        })?;
        let entered = tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.metrics.entered.notified(),
        )
        .await;
        let charged = fixture.scratch.usage()?;
        let metadata = fixture.cache.metadata.usage();
        let early = returned.load(Ordering::Acquire);
        fixture.files.metrics.release();
        ensure!(trim.await??);
        ensure!(entered.is_ok() && !early);
        ensure!(charged.reserved_files == 1 && metadata == (METADATA_BYTES, 1));
        ensure!(fixture
            .files
            .metrics
            .close_threads
            .lock()
            .unwrap()
            .iter()
            .all(|id| *id != std::thread::current().id()));
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test]
async fn stop_joins_the_file_even_when_a_source_handle_is_retained() -> Result<()> {
    let mut fixture = Fixture::new().await?;
    let result = async {
        let payload = fixture.prepare(fixture.key.clone()).await?.payload;
        fixture.stop().await?;
        ensure!(fixture.scratch.usage()?.reserved_files == 0);
        ensure!(payload.chunk(0, 1).is_err());
        ensure!(fixture.cache.leases.usage() == (METADATA_BYTES, 1));
        drop(payload);
        ensure!(fixture.cache.leases.usage() == (0, 0));
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}

#[tokio::test]
async fn cancelled_prepare_retains_its_outer_admission_until_physical_work_exits() -> Result<()> {
    let fixture = Fixture::new().await?;
    // The host's admitted blocking job owns this reservation while waiting
    // for prepare. Cancelling its async join waiter cannot refund it early.
    let admission = Budget::new(8 * 1024 * 1024, 1);
    let result = async {
        let permit = admission.acquire(8 * 1024 * 1024).unwrap();
        let cache = fixture.cache.clone();
        let key = fixture.key.clone();
        let build = fixture.builder(&key)?;
        let metrics = fixture.files.metrics.clone();
        let job = fixture.domain.blocking(move || {
            let _admission = permit;
            cache.prepare(key, move || {
                metrics.pause();
                build()
            })
        })?;
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.metrics.entered.notified(),
        )
        .await?;
        job.abort();
        drop(job);
        fixture.cache.stop();
        ensure!(admission.usage() == (8 * 1024 * 1024, 1));
        ensure!(fixture.cache.operations.usage() == (METADATA_BYTES, 1));
        fixture.files.metrics.release();
        Ok(())
    }
    .await;
    result.and(fixture.close().await)?;
    ensure!(admission.usage() == (0, 0));
    Ok(())
}

#[tokio::test]
async fn archive_scratch_pressure_releases_idle_sources_but_preserves_live_consumers() -> Result<()>
{
    use crate::raft::{
        archive::ArchiveError, host_archive::release_idle_source_on_scratch_pressure,
    };
    let mut fixture = Fixture::new().await?;
    fixture.scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 4 * 1024 * 1024,
        max_files: 2,
    })?;
    let result = async {
        let payload = fixture.prepare(fixture.key.clone()).await?.payload;
        let build = fixture.builder(&fixture.key)?;
        let failed = fixture.domain.blocking(build)?.await?;
        let error = match failed {
            Err(SnapshotError::Physical(error @ stream_snapshot::Error::ScratchFull)) => {
                Arc::new(ArchiveError::Physical(error))
            }
            _ => anyhow::bail!("the cached file left room for another two-spool encoder"),
        };
        let cache = fixture.cache.clone();
        let pressure = error.clone();
        fixture
            .domain
            .blocking(move || {
                release_idle_source_on_scratch_pressure(&cache, &pressure);
            })?
            .await?;
        ensure!(
            fixture.files.metrics.files.load(Ordering::Acquire) == 1,
            "archive pressure evicted a live source"
        );
        drop(payload);
        let cache = fixture.cache.clone();
        fixture
            .domain
            .blocking(move || {
                release_idle_source_on_scratch_pressure(&cache, &ArchiveError::Unconfirmed);
            })?
            .await?;
        ensure!(
            fixture.files.metrics.files.load(Ordering::Acquire) == 1,
            "an unrelated retry discarded reusable bytes"
        );
        let cache = fixture.cache.clone();
        fixture
            .domain
            .blocking(move || {
                release_idle_source_on_scratch_pressure(
                    &cache,
                    &ArchiveError::Images(cellar_ltx::generation::GenerationError::Snapshot(
                        stream_snapshot::Error::ScratchFull,
                    )),
                );
            })?
            .await?;
        ensure!(fixture.files.metrics.files.load(Ordering::Acquire) == 0);
        ensure!(fixture.scratch.usage()?.reserved_files == 0);
        let retry = fixture.builder(&fixture.key)?;
        fixture
            .domain
            .blocking(move || {
                let file = retry()?;
                file.verify()?;
                Ok::<_, SnapshotError>(())
            })?
            .await??;
        ensure!(
            fixture.scratch.usage()?.reserved_files == 0,
            "publication retry did not release its physical file"
        );
        drop(fixture.prepare(fixture.key.clone()).await?);
        let cache = fixture.cache.clone();
        fixture
            .domain
            .blocking(move || {
                release_idle_source_on_scratch_pressure(&cache, &error);
            })?
            .await?;
        ensure!(fixture.scratch.usage()?.reserved_files == 0);
        Ok(())
    }
    .await;
    result.and(fixture.close().await)
}
