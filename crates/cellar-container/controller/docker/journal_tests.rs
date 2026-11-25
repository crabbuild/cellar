// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Hold the actual replacement directory barrier, without blocking the test
//! driver. Production snapshot reads use this same synchronous Journal::view.
use super::*;
use cellar::sdk::ExecutionDomain;
use cellar_ltx::{DirectFileSystem, HostDirEntry, HostFile, HostMetadata};
use std::{
    io,
    sync::{atomic::AtomicUsize, Condvar},
    time::Duration,
};

#[derive(Default)]
struct Gate {
    armed: bool,
    released: bool,
    fail: bool,
}

#[derive(Default)]
struct Files {
    gate: Mutex<Gate>,
    changed: Condvar,
    entered: AtomicBool,
    barriers: AtomicUsize,
}

impl Files {
    fn arm(&self, fail: bool) {
        *self.gate.lock().unwrap() = Gate {
            armed: true,
            released: false,
            fail,
        };
        self.entered.store(false, Ordering::Release);
    }

    fn release(&self) {
        self.gate.lock().unwrap().released = true;
        self.changed.notify_all();
    }

    async fn wait_entered(&self) -> Result<()> {
        cellar::asyncrt::timeout(Duration::from_secs(5), async {
            while !self.entered.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .context("journal never entered directory barrier")
    }
}

impl FileSystem for Files {
    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
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

    fn symlink_metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.symlink_metadata(path)
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

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.barriers.fetch_add(1, Ordering::AcqRel);
        let mut gate = self.gate.lock().unwrap();
        if gate.armed {
            gate.armed = false;
            self.entered.store(true, Ordering::Release);
            while !gate.released {
                gate = self.changed.wait(gate).unwrap();
            }
            if gate.fail {
                return Err(io::Error::other("journal directory barrier refused"));
            }
        }
        drop(gate);
        DirectFileSystem.sync_all(path)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    domain: Arc<ExecutionDomain>,
    files: Arc<Files>,
    journal: Arc<Journal>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let options = DockerOptions {
            directory: root.path().join("backend"),
            socket: root.path().join("unused.sock"),
            binding: "DOCKER".into(),
            owner: "a".repeat(32),
            image: format!("sha256:{}", "b".repeat(64)),
            memory_bytes: 128 << 20,
            nano_cpus: 250_000_000,
            pids: 64,
            max_resources: 8,
            max_operations: 256,
            max_inflight: 8,
            max_body_bytes: 4096,
            max_operation_ms: 5000,
        };
        let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
        let files = Arc::new(Files::default());
        let fs = files.clone();
        let journal = domain
            .blocking(move || Journal::create(options, fs, "original".into()))?
            .await??;
        Ok(Self {
            _root: root,
            domain,
            files,
            journal,
        })
    }
}

#[tokio::test]
async fn cancelled_submission_finishes_its_unentered_reservation() -> Result<()> {
    use cellar::sdk::containers::ExecutionControllerBackend;

    let files = Arc::new(Files::default());
    let domain = Arc::new(ExecutionDomain::with_filesystem(
        tokio::runtime::Handle::current(),
        files.clone(),
        None,
    ));
    let f = super::super::receipt_tests::fixture_in_domain(domain.clone()).await?;
    let mut caller = None;
    let result = domain
        .run(async {
            // This is the real backend's reservation and original deadline. The
            // unopened Engine socket makes any unintended execution fail the oracle.
            let mut claim = serde_json::to_value(&f.current)?;
            claim["checkpoint"]["record"]["intent"] = serde_json::json!("deleted");
            claim["checkpoint"]["record"]["resource_id"] = serde_json::json!("c".repeat(64));
            let claim: ExecutionClaim = serde_json::from_value(claim)?;
            let key = key(&claim);
            let install = claim.clone();
            f.backend
                .update(move |state| {
                    let slot = state.slots.get_mut(&key).context("test fence")?;
                    slot.high = install;
                    slot.operations.clear();
                    Ok(())
                })
                .await?;
            let request =
                super::super::Request::new(crate::Operation::Destroy, serde_json::json!({}));
            let payload = serde_json::to_vec(&request)?;
            let operation = ExecutionOperation::new(
                [9; 16],
                cellar::sdk::containers::ExecutionOperationKind::Delete,
                &payload,
                u64::try_from(cellar::asyncrt::wall_ms())? + 1500,
            )?;
            let expires = operation.expires_unix_ms;
            let backend = f.backend.clone();
            let pending_claim = claim.clone();
            let pending_operation = operation.clone();
            files.arm(false);
            caller = Some(domain.spawn(async move {
                backend
                    .execute(pending_claim, pending_operation, payload)
                    .await
            })?);
            files.wait_entered().await?;
            let caller = caller.take().context("submission caller")?;
            caller.abort();
            ensure!(caller.await.is_err(), "submission caller was not cancelled");
            while u64::try_from(cellar::asyncrt::wall_ms())? <= expires {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            files.release();
            let original = ExecutionPendingOperation {
                claim: claim.clone(),
                operation,
            };
            cellar::asyncrt::timeout(Duration::from_secs(3), async {
                loop {
                    let outcome = f.backend.journal.view(|state| {
                        Ok(state
                            .original_operation(&claim, &original)?
                            .and_then(|operation| operation.outcome.clone()))
                    })?;
                    if let Some(outcome) = outcome {
                        ensure!(outcome == ExecutionOutcome::ExpiredBeforeSubmission,
                        "cancelled pre-entry submission acquired an Engine outcome: {outcome:?}");
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("cancelled reservation lost its owner before Engine entry")??;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    files.release();
    if let Some(caller) = caller {
        caller.abort();
        let _ = caller.await;
    }
    domain.shutdown().await;
    result?
}

#[tokio::test]
async fn durable_snapshot_reads_do_not_wait_for_directory_sync() -> Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            f.files.arm(false);
            let journal = f.journal.clone();
            let writer = f.domain.blocking(move || {
                journal.change(|state| {
                    state.engine = "successor".into();
                    Ok(())
                })
            })?;
            f.files.wait_entered().await?;
            let journal = f.journal.clone();
            let reader = f
                .domain
                .blocking(move || journal.view(|state| Ok(state.engine.clone())))?;
            let observed = cellar::asyncrt::timeout(Duration::from_secs(5), reader)
                .await
                .context("published snapshot blocked behind physical journal sync")???;
            ensure!(observed == "original", "unproven journal state was exposed");
            f.files.release();
            writer.await??;
            ensure!(f.journal.view(|state| Ok(state.engine.clone()))? == "successor");
            Ok::<_, anyhow::Error>(())
        })
        .await;
    f.files.release();
    f.domain.shutdown().await;
    result?
}

#[tokio::test]
async fn serialized_writers_preserve_both_durable_updates() -> Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            f.files.arm(false);
            let journal = f.journal.clone();
            let first = f.domain.blocking(move || {
                journal.change(|state| {
                    state.engine = "successor".into();
                    Ok(())
                })
            })?;
            f.files.wait_entered().await?;
            let journal = f.journal.clone();
            let second = f.domain.blocking(move || {
                journal.change(|state| {
                    ensure!(
                        state.engine == "successor",
                        "writer used an obsolete snapshot"
                    );
                    state.configuration = [7; 32];
                    Ok(())
                })
            })?;
            f.files.release();
            first.await??;
            second.await??;
            let stored = read(&*f.files, &f.journal.options.directory.join("state"))?;
            ensure!(stored.engine == "successor" && stored.configuration == [7; 32]);
            ensure!(f.journal.view(|state| Ok(state.configuration))? == stored.configuration);
            Ok::<_, anyhow::Error>(())
        })
        .await;
    f.files.release();
    f.domain.shutdown().await;
    result?
}

#[tokio::test]
async fn failed_sync_refuses_snapshot_reads_and_queued_writers() -> Result<()> {
    let f = Fixture::new().await?;
    let result = f
        .domain
        .run(async {
            f.files.arm(true);
            let before = f.files.barriers.load(Ordering::Acquire);
            let journal = f.journal.clone();
            let first = f.domain.blocking(move || {
                journal.change(|state| {
                    state.engine = "uncertain".into();
                    Ok(())
                })
            })?;
            f.files.wait_entered().await?;
            let called = Arc::new(AtomicBool::new(false));
            let entered = called.clone();
            let attempts = f.journal.attempts.load(Ordering::Acquire);
            let journal = f.journal.clone();
            let second = f.domain.blocking(move || {
                journal.change(|_| {
                    entered.store(true, Ordering::Release);
                    Ok(())
                })
            })?;
            cellar::asyncrt::timeout(Duration::from_secs(5), async {
                while f.journal.attempts.load(Ordering::Acquire) == attempts {
                    cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .context("second writer never reached its lock")?;
            f.files.release();
            ensure!(first.await?.is_err());
            ensure!(
                second.await?.is_err(),
                "queued writer passed uncertain persistence"
            );
            ensure!(
                !called.load(Ordering::Acquire),
                "queued writer entered its mutation closure"
            );
            ensure!(
                f.files.barriers.load(Ordering::Acquire) == before + 1,
                "queued writer touched disk after failure"
            );
            let error = f.journal.view(|_| Ok(())).unwrap_err();
            ensure!(matches!(
                error.downcast_ref::<ExecutionControllerFailure>(),
                Some(ExecutionControllerFailure::Unavailable)
            ));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    f.files.release();
    f.domain.shutdown().await;
    result?
}

#[tokio::test]
async fn panicked_writer_refuses_published_snapshot_admission() -> Result<()> {
    let f = Fixture::new().await?;
    let journal = f.journal.clone();
    let failed = f
        .domain
        .blocking(move || journal.change::<()>(|_| panic!("journal updater panicked")))?
        .await;
    let result = (|| {
        ensure!(failed.is_err());
        let error = f.journal.view(|_| Ok(())).unwrap_err();
        ensure!(matches!(
            error.downcast_ref::<ExecutionControllerFailure>(),
            Some(ExecutionControllerFailure::Unavailable)
        ));
        ensure!(f.journal.change(|_| Ok(())).is_err());
        Ok(())
    })();
    f.domain.shutdown().await;
    result
}
