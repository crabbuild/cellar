// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical cleanup custody, including rejected and entered domain work.
#![allow(clippy::disallowed_methods)]

use super::*;
use cellar_ltx::{
    internal::{CacheLimits, NodeExtentLimits, NodeExtentStore, StoreError, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, AtomicUsize},
        Condvar,
    },
    time::Duration,
};

const FAIL_SYNC: u8 = 1;
const PANIC_SYNC: u8 = 2;

#[derive(Default)]
struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
    outcome: AtomicU8,
    calls: AtomicUsize,
    thread: Mutex<Option<std::thread::ThreadId>>,
}

impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }

    fn hold(&self) -> io::Result<()> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        *self.thread.lock().unwrap() = Some(std::thread::current().id());
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
        drop(released);
        match self.outcome.load(Ordering::Acquire) {
            FAIL_SYNC => Err(io::Error::other("injected workspace sync failure")),
            PANIC_SYNC => panic!("injected workspace sync panic"),
            _ => Ok(()),
        }
    }
}

struct Release(Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[derive(Default)]
struct Files {
    parent: Mutex<Option<PathBuf>>,
    gate: Arc<Gate>,
}

impl FileSystem for Files {
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        DirectFileSystem.read_dir_iter(path)
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

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_new(path)
    }

    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_read_write(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open_read_write(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        let selected = self.parent.lock().unwrap().as_deref() == Some(path);
        if selected {
            self.gate.hold()?;
        }
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

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        DirectFileSystem.remove_dir_batch(path, limit)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    images: NodeExtentStore,
    files: Arc<Files>,
    path: PathBuf,
    charge: u64,
    input: Option<CellPrivateCleanup>,
}

impl Fixture {
    fn new() -> anyhow::Result<Self> {
        let mut root = tempfile::tempdir()?;
        root.disable_cleanup(true);
        eprintln!("retained cleanup custody disk: {}", root.path().display());
        let files = Arc::new(Files::default());
        let limits = ExecutionLimits::default();
        let bounds = RootPrivateOptions::workspace_bounds(limits)?;
        let charge = bounds.reservation()?;
        let parent = root.path().join("store");
        let images = NodeExtentStore::open(
            parent.clone(),
            files.clone(),
            NodeExtentLimits {
                max_bytes: charge,
                max_workspaces: 1,
                ..Default::default()
            },
        )?;
        let workspace = images.create_disposable_workspace([7; 32], bounds)?;
        let path = workspace.directory().to_path_buf();
        files.write(
            &path.join("physical-witness"),
            b"synthetic cleanup custody\n",
        )?;
        let options = RootPrivateOptions {
            workspace,
            host: cellar_ltx::LtxHost::default().with_filesystem(files.clone()),
            sqlite_vfs: None,
            images: images.clone(),
            query_services: cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 0),
                fill_random: Arc::new(|output| output.fill(0)),
            },
            application_digest: [4; 32],
            limits,
            cache_directory: root.path().join("cache"),
            cache_pool: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })?,
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        };
        let input = CellPrivateCleanup::closed_workspace_for_test(options)?;
        *files.parent.lock().unwrap() = Some(parent);
        Ok(Self {
            _root: root,
            images,
            files,
            path,
            charge,
            input: Some(input),
        })
    }

    fn held(&self) -> anyhow::Result<()> {
        let usage = self.images.usage()?;
        assert_eq!(usage.workspaces, 1);
        assert_eq!(usage.workspace_bytes, self.charge);
        assert!(!usage.quarantined);
        Ok(())
    }

    fn settled(&self, quarantined: bool) -> anyhow::Result<()> {
        let usage = self.images.usage()?;
        assert_eq!(usage.quarantined, quarantined);
        assert_eq!(usage.workspaces, usize::from(quarantined));
        assert_eq!(
            usage.workspace_bytes,
            if quarantined { self.charge } else { 0 }
        );
        assert!(!self.path.exists());
        assert_eq!(self.files.gate.calls.load(Ordering::Acquire), 1);
        Ok(())
    }
}

fn runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()?)
}

fn waker() -> Waker {
    Waker::from(Arc::new(ThreadWake(std::thread::current())))
}

#[test]
fn rejected_spawn_retains_real_workspace_for_owner_cleanup() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    runtime.block_on(domain.shutdown());
    let mut fixture = Fixture::new()?;
    fixture.files.gate.release();
    let mut task = CleanupTask::new(fixture.input.take().unwrap(), &token);
    assert!(task.task.is_none());
    fixture.held()?;
    task.join(&waker())?;
    fixture.settled(false)?;
    assert_eq!(
        *fixture.files.gate.thread.lock().unwrap(),
        Some(std::thread::current().id())
    );
    Ok(())
}

#[test]
fn cancelled_unentered_work_keeps_cleanup_on_its_owner() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let occupying = Arc::new(Gate::default());
    let held = occupying.clone();
    let blocker = domain.blocking(move || held.hold())?;
    let _release = Release(occupying.clone());
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), occupying.entered.notified()).await
    })?;
    let mut fixture = Fixture::new()?;
    fixture.files.gate.release();
    let mut task = CleanupTask::new(fixture.input.take().unwrap(), &token);
    let _release_task = Release(occupying.clone());
    task.task.as_ref().unwrap().abort();
    fixture.held()?;
    occupying.release();
    runtime.block_on(blocker)??;
    assert!(task.input.lock().unwrap().is_some());
    assert_eq!(fixture.files.gate.calls.load(Ordering::Acquire), 0);
    task.join(&waker())?;
    fixture.settled(false)?;
    assert_eq!(
        *fixture.files.gate.thread.lock().unwrap(),
        Some(std::thread::current().id())
    );
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn entered_cleanup_drop_and_domain_shutdown_join_before_refund() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let mut fixture = Fixture::new()?;
    let task = CleanupTask::new(fixture.input.take().unwrap(), &token);
    let _release = Release(fixture.files.gate.clone());
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.gate.entered.notified(),
        )
        .await
    })?;
    task.task.as_ref().unwrap().abort();
    fixture.held()?;
    let (joining, joined) = std::sync::mpsc::channel();
    let (started, starting) = std::sync::mpsc::channel();
    let owner = std::thread::spawn(move || {
        started.send(()).unwrap();
        drop(task);
        joining.send(()).unwrap();
    });
    starting.recv_timeout(Duration::from_secs(5))?;
    assert!(matches!(
        joined.recv_timeout(Duration::from_millis(100)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_millis(100), domain.shutdown()).await
        })
        .is_err());
    fixture.held()?;
    fixture.files.gate.release();
    joined.recv_timeout(Duration::from_secs(5))?;
    owner.join().unwrap();
    runtime.block_on(domain.shutdown());
    fixture.settled(false)
}

fn failed_cleanup(outcome: u8) -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let mut fixture = Fixture::new()?;
    fixture.files.gate.outcome.store(outcome, Ordering::Release);
    let mut task = CleanupTask::new(fixture.input.take().unwrap(), &token);
    let _release = Release(fixture.files.gate.clone());
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.gate.entered.notified(),
        )
        .await
    })?;
    fixture.held()?;
    fixture.files.gate.release();
    let error = task
        .join(&waker())
        .expect_err("failed physical cleanup must refuse success");
    let ExecutionError::Storage(cause) = error else {
        panic!("physical failure lost its storage classification");
    };
    if outcome == FAIL_SYNC {
        assert!(matches!(
            cause.downcast_ref::<StoreError>(),
            Some(StoreError::Quarantined)
        ));
    } else {
        assert!(cause.downcast_ref::<crate::asyncrt::TaskPanic>().is_some());
    }
    fixture.settled(true)?;
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn failed_parent_sync_keeps_full_charge_and_quarantine() -> anyhow::Result<()> {
    failed_cleanup(FAIL_SYNC)
}

#[test]
fn panicking_cleanup_joins_without_replaying_physical_work() -> anyhow::Result<()> {
    failed_cleanup(PANIC_SYNC)
}
