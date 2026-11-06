// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Idle/restart retirement reads one existing journal at a time. A live source
//! owns the same group lock, so a sweep cannot retire an entered transfer.
use super::*;
use cellar_ltx::{FileSystem, HostDirEntry, HostDirIterator};

struct EnteredCleanup(Arc<Jobs>);

impl Drop for EnteredCleanup {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .cleaning = false;
        self.0.finished.notify_waiters();
    }
}

impl Jobs {
    pub(in crate::raft::cell_groups::controller) fn maintain(
        self: &Arc<Self>,
        shared: &Arc<Shared>,
        now: u64,
    ) {
        {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.cleaning || now < state.next_cleanup || self.stopping.load(Ordering::Acquire) {
                return;
            }
        }
        let config = match shared.provisioning.lock() {
            Ok(slot) => slot.clone(),
            Err(_) => {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .discovery_error = Some(Arc::new(Error::Stopped));
                return;
            }
        };
        let Some(config) = config.filter(|config| config.archives.is_some()) else {
            return;
        };
        let entered = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.cleaning || self.stopping.load(Ordering::Acquire) {
                return;
            }
            state.cleaning = true;
            state.next_cleanup = now.saturating_add(shared.limits.retry_interval_ms.max(1000));
            EnteredCleanup(self.clone())
        };
        let runtime = shared.runtime.clone();
        let jobs = self.clone();
        let domain = shared.domain.clone();
        let s3 = shared.s3.clone();
        let interval = shared.limits.retry_interval_ms.max(1000);
        let result = domain.spawn_cleanup(async move {
            let _entered = entered;
            let execution = ControllerExecution::new(runtime.log().domain());
            let result =
                std::panic::AssertUnwindSafe(sweep(&config, &runtime, &jobs, &s3, &execution))
                    .catch_unwind()
                    .await;
            // A cancelled remote operation may still own physical I/O. Join it
            // before releasing either the journal sweep or its bank reservation.
            execution.join().await;
            let error = match result {
                Ok(result) => result.err(),
                Err(_) => Some(Arc::new(Error::Invalid("seed journal sweep panicked"))),
            };
            if let Some(error) = &error {
                tracing::warn!(%error, "seed journal discovery retained cleanup debt");
            }
            let next = runtime.log().domain().mono_ms();
            let mut state = jobs.state.lock().unwrap_or_else(|e| e.into_inner());
            state.discovery_error = error;
            // Rest between complete passes, including after a long outage scan.
            state.next_cleanup = match next {
                Ok(now) => now.saturating_add(interval),
                Err(_) => {
                    state
                        .discovery_error
                        .get_or_insert_with(|| Arc::new(Error::Stopped));
                    u64::MAX
                }
            };
        });
        match result {
            Ok(task) => task.detach(),
            Err(_) => {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .discovery_error = Some(Arc::new(Error::Stopped));
            }
        }
    }
}

async fn sweep(
    config: &Config,
    runtime: &CellGroupRuntime,
    jobs: &Jobs,
    s3: &Arc<crate::raft::budget::Budget>,
    execution: &ControllerExecution,
) -> std::result::Result<(), Arc<Error>> {
    let mut first_error = None;
    let result = async {
        let filesystem = execution.run(async { crate::asyncrt::fs() }).await?;
        let root = runtime
            .log()
            .limits()
            .directory
            .join("cell-group-seed-archives");
        let engine = runtime
            .log()
            .storage_instance()
            .map_err(|e| Error::Runtime(e.into()))?
            .bytes();
        let fs = filesystem.clone();
        let path = root.clone();
        let entries = execution
            .blocking(move || enumerate(fs.as_ref(), &path))?
            .await
            .map_err(Error::Worker)??;
        let Some(mut entries) = entries else {
            return Ok(());
        };
        loop {
            if jobs.stopping.load(Ordering::Acquire) {
                return Ok(());
            }
            let (next, item) = execution
                .blocking(move || {
                    let item = entries.next();
                    (entries, item)
                })?
                .await
                .map_err(Error::Worker)?;
            entries = next;
            let Some(item) = item else {
                break;
            };
            let fs = filesystem.clone();
            let directory = root.clone();
            let inspected = execution
                .blocking(move || inspect(fs.as_ref(), &directory, item?, engine))?
                .await
                .map_err(Error::Worker)?;
            let (path, creation) = match inspected {
                Ok(identity) => identity,
                Err(error) => {
                    first_error.get_or_insert_with(|| Arc::new(error));
                    continue;
                }
            };
            let history = creation.route().history;
            let group = {
                let mut state = jobs.state.lock().map_err(|_| Error::Stopped)?;
                jobs.group(&mut state, history)?
            };
            let Ok(_group) = group.try_lock_owned() else {
                // No waiting behind a live producer, and no age-based expiry.
                continue;
            };
            // The configuration owns the bounded journal workspace. Take one
            // ordinary S3 slot per journal, not for the whole disk inventory.
            // A busy bank defers this entry to the next pass without expiry.
            let Some(_permit) = s3.acquire(8192) else {
                continue;
            };
            let result = std::panic::AssertUnwindSafe(reconcile(
                config,
                filesystem.clone(),
                path,
                engine,
                creation,
                execution,
            ))
            .catch_unwind()
            .await;
            // Keep the group lock, admission and journal ownership until even
            // cancelled/timed-out child I/O is terminal, before another source
            // can reserve a new generation for this history.
            execution.join().await;
            let result = match result {
                Ok(result) => result,
                Err(_) => Err(Error::Invalid("seed journal retirement panicked")),
            };
            if !record_cleanup(jobs, history, result) {
                first_error
                    .get_or_insert_with(|| Arc::new(Error::Invalid("seed cleanup error capacity")));
            }
        }
        Ok::<_, Error>(())
    }
    .await;
    if let Err(error) = result {
        first_error.get_or_insert_with(|| Arc::new(error));
    }
    first_error.map_or(Ok(()), Err)
}

fn enumerate(
    filesystem: &dyn FileSystem,
    root: &std::path::Path,
) -> Result<Option<HostDirIterator>> {
    let metadata = match filesystem.symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_dir {
        return Err(Error::Invalid("seed journal root type"));
    }
    Ok(Some(filesystem.read_dir_iter(root)?))
}

fn inspect(
    filesystem: &dyn FileSystem,
    root: &std::path::Path,
    entry: HostDirEntry,
    engine: [u8; 16],
) -> Result<(std::path::PathBuf, CellGroupArchiveCreation)> {
    let name = entry
        .file_name
        .to_str()
        .ok_or(Error::Invalid("seed journal name encoding"))?;
    if !entry.is_dir
        || name.len() != 64
        || entry.path != root.join(name)
        || !filesystem.symlink_metadata(&entry.path)?.is_dir
    {
        return Err(Error::Invalid("seed journal entry type/path"));
    }
    let creation = journal::Journal::inspect_seed(filesystem, &entry.path, engine)?;
    if name != journal::seed_name(creation.route().history) {
        return Err(Error::Invalid("seed journal history path"));
    }
    Ok((entry.path, creation))
}

async fn reconcile(
    config: &Config,
    filesystem: Arc<dyn FileSystem>,
    path: std::path::PathBuf,
    engine: [u8; 16],
    creation: CellGroupArchiveCreation,
    execution: &ControllerExecution,
) -> Result<()> {
    let expected = creation.clone();
    let (journal, pending) = execution
        .blocking(move || {
            let journal = Arc::new(journal::Journal::open_seed_existing(
                filesystem, path, engine, expected,
            )?);
            let record = journal.load()?;
            if record.head.is_some()
                || record.retire.is_some()
                || matches!(
                    record.pending,
                    journal::Pending::Acquire(_) | journal::Pending::Publish { .. }
                )
            {
                return Err(Error::Invalid("seed cleanup journal purpose"));
            }
            let pending = !matches!(record.pending, journal::Pending::None);
            Ok::<_, Error>((journal, pending))
        })?
        .await
        .map_err(Error::Worker)??;
    if !pending {
        return Ok(());
    }
    let archives = config.archives.as_ref().ok_or(Error::Stopped)?;
    let history = creation.route().history;
    let profile = archives
        .namespaces
        .iter()
        .find(|profile| {
            let config = profile.directory.namespace_config();
            config.cluster == history.cluster && config.namespace == history.namespace
        })
        .ok_or(Error::Invalid("seed cleanup namespace unavailable"))?;
    let registry = profile
        .directory
        .open_seed_cleanup_archive(execution, &archives.io, &creation)
        .await?;
    retire(&journal, &registry, execution).await
}
