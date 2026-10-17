use super::*;
use crate::sdk::ExecutionDomain;
use anyhow::ensure;
use cellar_ltx::{DirectFileSystem, FileSystem};
use futures_util::FutureExt as _;
use std::sync::{Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
    expected: Arc<crate::host_services::HostServices>,
}

impl Gate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct EnteredFile {
    inner: HostFile,
    gate: Arc<Gate>,
}

impl HostFileIo for EnteredFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        assert!(Arc::ptr_eq(
            &self.gate.expected,
            &crate::asyncrt::services()
        ));
        self.inner.write_all(bytes)?;
        self.gate.entered.notify_one();
        let mut released = self.gate.released.lock().unwrap();
        while !*released {
            released = self.gate.wake.wait(released).unwrap();
        }
        Ok(())
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        self.inner.read_exact_at(offset, length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.inner.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_caller_cannot_return_admission_before_entered_file_io_joins() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("entered");
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let budget = Budget::default();
    let scope = budget.acquire().await?;
    let gate = Arc::new(Gate {
        entered: Notify::new(),
        released: Mutex::new(false),
        wake: Condvar::new(),
        expected: owner.services(),
    });
    let mut file = scope.file(HostFile::from_io(EnteredFile {
        inner: DirectFileSystem.create(&path)?,
        gate: gate.clone(),
    }));
    drop(scope);
    let task = match owner.spawn(async move {
        crate::asyncrt::try_blocking(move || file.write_all(b"entered-stage"))?.await??;
        Ok::<_, anyhow::Error>(())
    }) {
        Ok(task) => task,
        Err(error) => {
            gate.release();
            owner.shutdown().await;
            return Err(error.into());
        }
    };
    // These guards belong to the caller's test driver, outside the domain.
    #[allow(clippy::disallowed_methods)]
    let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await;
    task.abort();
    let cancelled = task.await;
    let mut waiting = Box::pin(budget.acquire());
    let observed = waiting.as_mut().now_or_never();
    let reserved = observed.is_none();
    drop(observed);
    owner.close();
    let mut shutdown = Box::pin(owner.shutdown());
    let joined = shutdown.as_mut().now_or_never().is_none();
    gate.release();
    shutdown.await;
    if reserved {
        #[allow(clippy::disallowed_methods)]
        let granted = tokio::time::timeout(Duration::from_secs(2), waiting).await??;
        drop(granted);
    }
    entered?;
    ensure!(cancelled.is_err(), "caller did not cancel");
    ensure!(
        reserved,
        "cancelled caller returned admission while its file worker still ran"
    );
    ensure!(joined, "shutdown completed before entered file I/O joined");
    ensure!(DirectFileSystem.read(&path)? == b"entered-stage");
    ensure!(budget.0.available_permits() == 1);
    Ok(())
}

#[tokio::test]
async fn sorted_consumption_and_uniqueness_rewind_retain_the_reservation() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let budget = Budget::default();
    let scope = budget.acquire().await?;
    let filesystem = scope.filesystem(Arc::new(DirectFileSystem));
    let mut file = filesystem.temporary_file(Some(directory.path()))?;
    drop(filesystem);
    drop(scope);
    let mut file_waiter = Box::pin(budget.acquire());
    let observed = file_waiter.as_mut().now_or_never();
    let file_reserved = observed.is_none();
    drop(observed);
    file.write_all(b"guarded-file")?;
    ensure!(file.read_exact_at(0, 12)? == b"guarded-file");
    drop(file);
    let scope = if file_reserved {
        file_waiter.await?
    } else {
        budget.acquire().await?
    };
    let mut rows = Rows::new(Arc::new(DirectFileSystem), directory.path().to_owned());
    scope.hold_rows(&mut rows);
    rows.insert(
        ("cell".into(), 1, 2),
        vec![1, 2, 3],
        super::super::recovery_rows::Priority::Preferred,
        super::super::recovery_rows::Duplicates::First,
    )?;
    drop(scope);
    let mut sorted = rows.finish()?.unique()?;
    let mut waiting = Box::pin(budget.acquire());
    let observed = waiting.as_mut().now_or_never();
    let reserved = observed.is_none();
    drop(observed);
    let batch = sorted.next_batch()?;
    ensure!(batch.get(&("cell".into(), 1, 2)).map(AsRef::as_ref) == Some([1, 2, 3].as_slice()));
    ensure!(sorted.next_batch()?.is_empty());
    drop(sorted);
    if reserved {
        drop(waiting.await?);
    }
    ensure!(
        file_reserved,
        "returned file lost its filesystem reservation"
    );
    ensure!(
        reserved,
        "spool finish/rewind returned admission before consumption"
    );
    ensure!(budget.0.available_permits() == 1);
    Ok(())
}
