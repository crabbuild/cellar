// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Joined ownership of admitted workspace creation and rejected-open cleanup.
#![allow(clippy::disallowed_methods)]

use super::*;
#[path = "workspace_capacity_tests.rs"]
mod capacity;
use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};
use cellar_ltx::{
    internal::{CacheLimits, NodeExtentLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use std::{
    io,
    path::Path,
    sync::{
        atomic::{AtomicU8, AtomicUsize},
        Condvar,
    },
    time::Duration,
};

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
    calls: AtomicUsize,
    outcome: AtomicU8,
}

impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }

    fn hold(&self) -> io::Result<()> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
        drop(released);
        match self.outcome.load(Ordering::Acquire) {
            1 => Err(io::Error::other(
                "injected workspace creation or release failure",
            )),
            2 => panic!("injected workspace creation panic"),
            _ => Ok(()),
        }
    }
}

struct Release(Arc<Files>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.create.release();
        self.0.cleanup.release();
    }
}

#[derive(Default)]
struct Files {
    create: Gate,
    cleanup: Gate,
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

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        if self.cleanup.armed.load(Ordering::Acquire) {
            self.cleanup.hold()?;
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
        if self.create.armed.load(Ordering::Acquire) {
            self.create.hold()?;
        }
        DirectFileSystem.create_dir_all(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    files: Arc<Files>,
    options: CellSqlOptions,
    images: NodeExtentStore,
    lane: Arc<Semaphore>,
    charge: u64,
}

impl Fixture {
    fn new() -> anyhow::Result<Self> {
        Self::with_capacity(2, |charge| 2 * charge)
    }

    fn with_capacity(
        max_workspaces: usize,
        max_bytes: impl FnOnce(u64) -> u64,
    ) -> anyhow::Result<Self> {
        let mut root = tempfile::tempdir()?;
        root.disable_cleanup(true);
        eprintln!("retained workspace opening disk: {}", root.path().display());
        let files = Arc::new(Files::default());
        let limits = ExecutionLimits::default();
        let charge = RootPrivateOptions::workspace_bounds(limits)?.reservation()?;
        let images = NodeExtentStore::open_serialized(
            root.path().join("store"),
            files.clone(),
            NodeExtentLimits {
                max_bytes: max_bytes(charge),
                max_workspaces,
                ..Default::default()
            },
        )?;
        let options = CellSqlOptions::Extents(CellSqlRootOptions {
            host: cellar_ltx::LtxHost::default().with_filesystem(files.clone()),
            sqlite_vfs: None,
            images: images.clone(),
            query_services: cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 0),
                fill_random: Arc::new(|out| out.fill(0)),
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
        });
        Ok(Self {
            _root: root,
            files,
            options,
            images,
            lane: Arc::new(Semaphore::new(1)),
            charge,
        })
    }

    fn opening(
        &self,
        token: &crate::asyncrt::DomainToken,
        name: &str,
    ) -> anyhow::Result<SqlOpening> {
        let cell = CellHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: cellar_logic::cell_group::CellId::new(name)?,
            incarnation: IncarnationId::new([3; 16])?,
        };
        Ok(SqlOpening::new(
            &self.options,
            &cell,
            token,
            self.lane.clone(),
        ))
    }

    fn held(&self, quarantined: bool) -> anyhow::Result<()> {
        let u = self.images.usage()?;
        assert_eq!(
            (u.workspaces, u.workspace_bytes, u.quarantined),
            (1, self.charge, quarantined)
        );
        assert_eq!(self.lane.available_permits(), 0);
        Ok(())
    }

    fn empty(&self) -> anyhow::Result<()> {
        let u = self.images.usage()?;
        assert_eq!(
            (u.workspaces, u.workspace_bytes, u.quarantined),
            (0, 0, false)
        );
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

fn poll_root(
    opening: &mut SqlOpening,
    cx: &mut Context<'_>,
) -> Poll<Result<RootPrivateOptions, ExecutionError>> {
    opening.poll_with(cx, Ok(()), |root| {
        root.ok_or(ExecutionError::Invalid("rooted opening test"))
    })
}

#[test]
fn rejected_domain_enters_no_workspace_io() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    runtime.block_on(domain.shutdown());
    let fixture = Fixture::new()?;
    fixture.files.create.armed.store(true, Ordering::Release);
    fixture.files.create.release();
    let mut opening = fixture.opening(&token, "Counter:rejected")?;
    assert!(matches!(
        poll_root(&mut opening, &mut Context::from_waker(&waker())),
        Poll::Ready(Err(ExecutionError::Refused(CommandFailure::Unavailable)))
    ));
    fixture.empty()?;
    assert_eq!(fixture.files.create.calls.load(Ordering::Acquire), 0);
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    Ok(())
}

#[test]
fn expired_creation_joins_rejected_workspace_cleanup_before_refunding() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    fixture.files.create.armed.store(true, Ordering::Release);
    fixture.files.cleanup.armed.store(true, Ordering::Release);
    let mut opening = fixture.opening(&token, "Counter:expired")?;
    let _release = Release(fixture.files.clone());
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(poll_root(&mut opening, &mut cx).is_pending());
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.create.entered.notified(),
        )
        .await
    })?;
    fixture.held(false)?;
    let opened = AtomicUsize::new(0);
    let mut poll = |cx: &mut Context<'_>| {
        opening.poll_with::<RootPrivateOptions>(
            cx,
            Err(crate::raft::execution::authority_failure(
                RuntimeError::DeadlineExceeded,
            )),
            |root| {
                opened.fetch_add(1, Ordering::Relaxed);
                root.ok_or_else(unavailable)
            },
        )
    };
    assert!(poll(&mut cx).is_pending());
    fixture.held(false)?;
    fixture.files.create.release();
    runtime.block_on(async {
        let mut completed=std::future::poll_fn(&mut poll);
        crate::asyncrt::select_biased! { "reject premature completion before observing the held physical cleanup"; _ = &mut completed => panic!("reply preceded held physical cleanup"), _ = fixture.files.cleanup.entered.notified() => () }
    });
    fixture.held(false)?;
    assert!(poll(&mut cx).is_pending());
    fixture.files.cleanup.release();
    let result = runtime.block_on(std::future::poll_fn(poll));
    assert!(
        matches!(result,Err(ExecutionError::Storage(ref error)) if error.downcast_ref::<RuntimeError>().is_some_and(|e| matches!(e,RuntimeError::DeadlineExceeded)))
    );
    assert_eq!(opened.load(Ordering::Relaxed), 0);
    fixture.empty()?;
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn rejected_sql_open_keeps_workspace_pinned_until_cleanup_joins() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    fixture.files.cleanup.armed.store(true, Ordering::Release);
    let mut opening = fixture.opening(&token, "Counter:open-refusal")?;
    let _release = Release(fixture.files.clone());
    let calls = AtomicUsize::new(0);
    let mut poll = |cx: &mut Context<'_>| {
        opening.poll_with::<RootPrivateOptions>(cx, Ok(()), |root| {
            calls.fetch_add(1, Ordering::Relaxed);
            drop(root);
            Err(ExecutionError::Invalid("injected SQL open refusal"))
        })
    };
    runtime.block_on(async {
        let mut completed=std::future::poll_fn(&mut poll);
        crate::asyncrt::select_biased! { "reject premature completion before observing the held physical cleanup"; _ = &mut completed => panic!("refusal preceded held cleanup"), _ = fixture.files.cleanup.entered.notified() => () }
    });
    fixture.held(false)?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    fixture.files.cleanup.release();
    assert!(matches!(
        runtime.block_on(std::future::poll_fn(poll)),
        Err(ExecutionError::Invalid("injected SQL open refusal"))
    ));
    fixture.empty()?;
    drop(opening);
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn creation_failure_and_panic_keep_full_quarantine_charge() -> anyhow::Result<()> {
    for outcome in [1, 2] {
        let runtime = runtime()?;
        let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
        let token = domain.services().domain_token().unwrap();
        let fixture = Fixture::new()?;
        fixture
            .files
            .create
            .outcome
            .store(outcome, Ordering::Release);
        fixture.files.create.armed.store(true, Ordering::Release);
        fixture.files.create.release();
        let mut opening = fixture.opening(&token, "Counter:create-failure")?;
        let _release = Release(fixture.files.clone());
        assert!(matches!(
            runtime.block_on(std::future::poll_fn(|cx| poll_root(&mut opening, cx))),
            Err(ExecutionError::Storage(_))
        ));
        fixture.held(true)?;
        drop(opening);
        assert_eq!(fixture.lane.available_permits(), 1);
        runtime.block_on(domain.shutdown());
    }
    Ok(())
}

#[test]
fn rejected_workspace_cleanup_failure_keeps_full_quarantine_charge() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    fixture.files.cleanup.outcome.store(1, Ordering::Release);
    fixture.files.cleanup.armed.store(true, Ordering::Release);
    fixture.files.cleanup.release();
    let mut opening = fixture.opening(&token, "Counter:cleanup-failure")?;
    let _release = Release(fixture.files.clone());
    let result = runtime.block_on(std::future::poll_fn(|cx| {
        opening.poll_with::<RootPrivateOptions>(cx, Ok(()), |_| {
            Err(ExecutionError::Invalid("injected open refusal"))
        })
    }));
    assert!(matches!(result, Err(ExecutionError::Storage(_))));
    fixture.held(true)?;
    drop(opening);
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn workspace_lane_keeps_fifo_before_physical_entry() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    let initial = fixture.lane.clone().try_acquire_owned()?;
    let mut first = fixture.opening(&token, "Counter:first")?;
    let mut second = fixture.opening(&token, "Counter:second")?;
    let mut third = fixture.opening(&token, "Counter:third")?;
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    for opening in [&mut first, &mut second, &mut third] {
        assert!(poll_root(opening, &mut cx).is_pending());
        assert!(opening.task.is_none());
    }
    drop(initial);
    assert!(poll_root(&mut third, &mut cx).is_pending());
    assert!(third.task.is_none());
    assert!(poll_root(&mut second, &mut cx).is_pending());
    assert!(second.task.is_none());
    let root = runtime.block_on(std::future::poll_fn(|cx| poll_root(&mut first, cx)))?;
    fixture.held(false)?;
    assert!(poll_root(&mut second, &mut cx).is_pending());
    drop(root);
    drop(first);
    fixture.empty()?;
    assert!(poll_root(&mut third, &mut cx).is_pending());
    assert!(third.task.is_none());
    let root = runtime.block_on(std::future::poll_fn(|cx| poll_root(&mut second, cx)))?;
    fixture.held(false)?;
    drop(root);
    drop(second);
    let root = runtime.block_on(std::future::poll_fn(|cx| poll_root(&mut third, cx)))?;
    fixture.held(false)?;
    drop(root);
    drop(third);
    fixture.empty()?;
    assert_eq!(fixture.lane.available_permits(), 1);
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn shutdown_joins_entered_creation_and_retains_unadopted_workspace() -> anyhow::Result<()> {
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    fixture.files.create.armed.store(true, Ordering::Release);
    let mut opening = fixture.opening(&token, "Counter:shutdown")?;
    let _release = Release(fixture.files.clone());
    let waker = waker();
    let mut cx = Context::from_waker(&waker);
    assert!(poll_root(&mut opening, &mut cx).is_pending());
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            fixture.files.create.entered.notified(),
        )
        .await
    })?;
    fixture.held(false)?;
    runtime.block_on(async {
        let closing = domain.shutdown();
        tokio::pin!(closing);
        assert!(closing.as_mut().poll(&mut cx).is_pending());
        fixture.held(false)?;
        fixture.files.create.release();
        closing.await;
        Ok::<_, anyhow::Error>(())
    })?;
    // Completion of the domain's task cannot drop its returned workspace pin.
    fixture.held(false)?;
    assert!(opening.returned.lock().unwrap().is_some());
    opening.close(&waker)?;
    fixture.empty()?;
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    Ok(())
}

#[test]
fn cancelled_unentered_creation_never_acquires_physical_capacity() -> anyhow::Result<()> {
    struct ReleaseGate(Arc<Gate>);

    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    fixture.files.create.armed.store(true, Ordering::Release);
    fixture.files.create.release();
    let mut opening = fixture.opening(&token, "Counter:cancelled-queue")?;
    let occupied = Arc::new(Gate::default());
    let _release = ReleaseGate(occupied.clone());
    let held = occupied.clone();
    let blocking = token.blocking(move || held.hold())?;
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), occupied.entered.notified()).await
    })?;
    assert!(poll_root(&mut opening, &mut Context::from_waker(&waker())).is_pending());
    opening.task.as_ref().unwrap().abort();
    occupied.release();
    runtime.block_on(blocking)??;
    assert!(matches!(
        runtime.block_on(std::future::poll_fn(|cx| poll_root(&mut opening, cx))),
        Err(ExecutionError::Refused(CommandFailure::Unavailable))
    ));
    fixture.empty()?;
    assert_eq!(fixture.files.create.calls.load(Ordering::Acquire), 0);
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    runtime.block_on(domain.shutdown());
    Ok(())
}

#[test]
fn joined_capacity_refusal_is_not_a_shutdown_storage_failure() -> anyhow::Result<()> {
    struct ReleaseGate(Arc<Gate>);

    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }
    let runtime = runtime()?;
    let domain = crate::asyncrt::ExecutionDomain::new(runtime.handle().clone());
    let token = domain.services().domain_token().unwrap();
    let fixture = Fixture::new()?;
    let mut roots = Vec::new();
    for name in ["Counter:capacity-first", "Counter:capacity-second"] {
        let opening = fixture.opening(&token, name)?;
        let CellSqlOptions::Extents(options) = &fixture.options else {
            unreachable!("rooted fixture")
        };
        roots.push(options.clone().prepare(&opening.cell)?);
    }
    let mut opening = fixture.opening(&token, "Counter:capacity-refusal")?;
    let occupied = Arc::new(Gate::default());
    let _release = ReleaseGate(occupied.clone());
    let held = occupied.clone();
    let blocking = token.blocking(move || held.hold())?;
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), occupied.entered.notified()).await
    })?;
    let waker = waker();
    assert!(poll_root(&mut opening, &mut Context::from_waker(&waker)).is_pending());
    occupied.release();
    runtime.block_on(blocking)??;
    let sentinel = token.blocking(|| ())?;
    runtime.block_on(sentinel)?;
    assert!(opening.entered.load(Ordering::Acquire));
    // Close is the first consumer of this joined, expected quota refusal.
    opening.close(&waker)?;
    let usage = fixture.images.usage()?;
    assert_eq!(
        (usage.workspaces, usage.workspace_bytes, usage.quarantined),
        (2, 2 * fixture.charge, false)
    );
    drop(opening);
    assert_eq!(fixture.lane.available_permits(), 1);
    drop(roots);
    fixture.empty()?;
    runtime.block_on(domain.shutdown());
    Ok(())
}
