// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the real atomic-replacement barrier at the backend admission seam.
#![allow(clippy::disallowed_methods)]

use super::*;
use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use std::{io, path::Path};

#[derive(Default)]
struct Files {
    barriers: AtomicUsize,
    fail: AtomicBool,
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

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.barriers.fetch_add(1, Ordering::AcqRel);
        if self.fail.swap(false, Ordering::AcqRel) {
            return Err(io::Error::other("controller reservation barrier refused"));
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

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backend_submission_has_one_durable_reservation_and_one_result() -> Result<()> {
    let root = tempfile::tempdir()?;
    let files = Arc::new(Files::default());
    let d = Arc::new(ExecutionDomain::with_filesystem(
        tokio::runtime::Handle::current(),
        files.clone(),
        None,
    ));
    let provider = Arc::new(Provider::default());
    let controller = ExecutionController::create(
        d.clone(),
        options(root.path().join("controller")),
        provider.clone(),
        [31; 32],
    )
    .await?;
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    files.barriers.store(0, Ordering::Release);
    provider.hold.store(true, Ordering::Release);
    let expires = operation(&d, 71, b"bounded read").await?.expires_unix_ms;
    let request = ExecutionOperation::new(
        [71; 16],
        ExecutionOperationKind::Read,
        b"bounded read",
        expires,
    )?;
    let current = controller.clone();
    let owner = c.clone();
    let original = request.clone();
    let pending = d.spawn(async move {
        current
            .execute(owner, original, b"bounded read".to_vec())
            .await
    })?;
    let entered = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        provider.entered.notified(),
    )
    .await;
    let before = files.barriers.load(Ordering::Acquire);
    provider.released.notify_one();
    let result = pending.await??;
    let after = files.barriers.load(Ordering::Acquire);
    let replay = controller
        .execute(c, request, b"bounded read".to_vec())
        .await?;
    controller.shutdown().await?;
    d.shutdown().await;
    entered?;
    ensure!(
        before == 1,
        "backend entry needs one durable reservation, observed {before}"
    );
    ensure!(
        after == 2,
        "result must be independently durable, observed {after} barriers"
    );
    ensure!(result == ExecutionOutcome::Completed(b"bounded read".to_vec()) && replay == result);
    ensure!(
        provider.calls.load(Ordering::Acquire) == 1,
        "receipt replay reentered provider"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reservation_barrier_never_enters_backend() -> Result<()> {
    let root = tempfile::tempdir()?;
    let files = Arc::new(Files::default());
    let d = Arc::new(ExecutionDomain::with_filesystem(
        tokio::runtime::Handle::current(),
        files.clone(),
        None,
    ));
    let provider = Arc::new(Provider::default());
    let controller = ExecutionController::create(
        d.clone(),
        options(root.path().join("controller")),
        provider.clone(),
        [32; 32],
    )
    .await?;
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    files.fail.store(true, Ordering::Release);
    let result = controller
        .execute(c, operation(&d, 72, b"write").await?, b"write".to_vec())
        .await;
    let shutdown = controller.shutdown().await;
    d.shutdown().await;
    ensure!(
        result.is_err(),
        "failed durability barrier acknowledged an effect"
    );
    ensure!(
        provider.calls.load(Ordering::Acquire) == 0,
        "provider entered before durable reservation"
    );
    shutdown?;
    Ok(())
}
