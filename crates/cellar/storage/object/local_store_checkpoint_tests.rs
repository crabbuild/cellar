// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Hold a real main-database checkpoint sync, leaving FULL WAL syncs intact.
#![allow(clippy::disallowed_methods)]

use super::*;
use anyhow::{ensure, Context as _, Result};
use object_store::multipart::MultipartStore;
use rusqlite::ffi;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CString};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, OnceLock};
use tokio::sync::Semaphore;

struct SyncGate {
    armed: AtomicBool,
    entered: Semaphore,
    released: Mutex<bool>,
    wake: Condvar,
    watchdog: AtomicBool,
}

impl SyncGate {
    fn new() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            released: Mutex::new(false),
            wake: Condvar::new(),
            watchdog: AtomicBool::new(false),
        }
    }

    fn hold_once(&self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        self.entered.add_permits(1);
        let released = self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let (released, timeout) = self
            .wake
            .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
            .unwrap_or_else(|error| error.into_inner());
        drop(released);
        if timeout.timed_out() {
            self.watchdog.store(true, Ordering::Release);
        }
    }

    fn release(&self) {
        *self
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.wake.notify_all();
    }
}

struct FileMethods {
    original: usize,
    _replacement: Box<ffi::sqlite3_io_methods>,
    gate: Arc<SyncGate>,
}

type Files = Mutex<HashMap<usize, FileMethods>>;

fn files() -> &'static Files {
    static FILES: OnceLock<Files> = OnceLock::new();
    FILES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Clone)]
struct Registration {
    base: usize,
    gate: Arc<SyncGate>,
}

fn registrations() -> &'static Mutex<HashMap<usize, Registration>> {
    static REGISTRATIONS: OnceLock<Mutex<HashMap<usize, Registration>>> = OnceLock::new();
    REGISTRATIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) struct CheckpointVfs {
    name: CString,
    vfs: *mut ffi::sqlite3_vfs,
    gate: Arc<SyncGate>,
}

impl fmt::Debug for CheckpointVfs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("checkpoint sync fixture")
    }
}

// SAFETY: the registration is immutable after SQLite publishes it. SQLite
// serializes its own VFS registry; callback bookkeeping and gates use mutexes.
// LocalStore clones retain this Arc until their owned connections have closed.
unsafe impl Send for CheckpointVfs {}

// SAFETY: see Send; sharing exposes only the immutable name and synchronized gate.
unsafe impl Sync for CheckpointVfs {}

impl CheckpointVfs {
    fn register() -> Result<Arc<Self>> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let name = CString::new(format!(
            "cellar-checkpoint-{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))?;
        let gate = Arc::new(SyncGate::new());
        // SAFETY: the built-in default VFS lives for the process. Copying its
        // callbacks and original pAppData retains its native file layout. Only
        // xOpen is wrapped, delegating to that same original VFS pointer.
        let base = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
        ensure!(!base.is_null(), "SQLite has no default VFS");
        // SAFETY: base is a live SQLite registration, checked above.
        let mut vfs = Box::new(unsafe { ptr::read(base) });
        vfs.pNext = ptr::null_mut();
        vfs.zName = name.as_ptr();
        vfs.xOpen = Some(open);
        let vfs = Box::into_raw(vfs);
        let key = vfs as usize;
        registrations().lock().unwrap().insert(
            key,
            Registration {
                base: base as usize,
                gate: gate.clone(),
            },
        );
        // SAFETY: both allocations are stable and remain owned until unregister.
        let code = unsafe { ffi::sqlite3_vfs_register(vfs, 0) };
        if code != ffi::SQLITE_OK {
            registrations().lock().unwrap().remove(&key);
            // SAFETY: failed registration published no SQLite borrowers.
            unsafe {
                drop(Box::from_raw(vfs));
            }
            anyhow::bail!("register checkpoint VFS: {code}");
        }
        Ok(Arc::new(Self { name, vfs, gate }))
    }

    pub(super) fn name(&self) -> &str {
        self.name.to_str().unwrap()
    }
}

impl Drop for CheckpointVfs {
    fn drop(&mut self) {
        // SAFETY: the last LocalStore/worker Arc has dropped. Its operation
        // connections have already closed, and no external caller knows this
        // private VFS name. No new callback can borrow the registration.
        unsafe {
            ffi::sqlite3_vfs_unregister(self.vfs);
        }
        registrations().lock().unwrap().remove(&(self.vfs as usize));
        // SAFETY: unregister removed the last possible borrower; recover the
        // allocation transferred to SQLite's registry in register().
        unsafe {
            drop(Box::from_raw(self.vfs));
        }
    }
}

unsafe extern "C" fn open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let registration = registrations()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&(vfs as usize))
        .cloned();
    let Some(registration) = registration else {
        return ffi::SQLITE_CANTOPEN;
    };
    // SAFETY: SQLite supplied these live pointers. The original VFS receives
    // its own pointer and original-size file storage. A successful open owns
    // its methods until close; replace only sync/close on main DB handles.
    unsafe {
        let base = registration.base as *mut ffi::sqlite3_vfs;
        let code = ((*base).xOpen.unwrap())(base, name, file, flags, out);
        if code != ffi::SQLITE_OK || flags & ffi::SQLITE_OPEN_MAIN_DB == 0 {
            return code;
        }
        let original = (*file).pMethods;
        let mut replacement = Box::new(ptr::read(original));
        replacement.xSync = Some(sync);
        replacement.xClose = Some(close);
        let methods = &*replacement as *const ffi::sqlite3_io_methods;
        files()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(
                file as usize,
                FileMethods {
                    original: original as usize,
                    _replacement: replacement,
                    gate: registration.gate,
                },
            );
        (*file).pMethods = methods;
        code
    }
}

unsafe extern "C" fn sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    let saved = files()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&(file as usize))
        .map(|entry| (entry.original, entry.gate.clone()));
    let Some((methods, gate)) = saved else {
        return ffi::SQLITE_IOERR_FSYNC;
    };
    gate.hold_once();
    // SAFETY: the native handle and original method table outlive this
    // synchronous callback; the registry lock is not held while I/O blocks.
    unsafe {
        ((*(methods as *const ffi::sqlite3_io_methods))
            .xSync
            .unwrap())(file, flags)
    }
}

unsafe extern "C" fn close(file: *mut ffi::sqlite3_file) -> c_int {
    let entry = files()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&(file as usize));
    let Some(entry) = entry else {
        return ffi::SQLITE_IOERR_CLOSE;
    };
    // SAFETY: this is the sole close of an opened native handle. Restore its
    // original methods before delegating, then free the copied table afterward.
    unsafe {
        let methods = entry.original as *const ffi::sqlite3_io_methods;
        (*file).pMethods = methods;
        ((*methods).xClose.unwrap())(file)
    }
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Put,
    Delete,
    Part,
    Complete,
    Abort,
    Reap,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_sync_keeps_lease_cas_progress() -> Result<()> {
    for mutation in [
        Mutation::Put,
        Mutation::Delete,
        Mutation::Part,
        Mutation::Complete,
        Mutation::Abort,
        Mutation::Reap,
    ] {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("objects.sqlite");
        let vfs = CheckpointVfs::register()?;
        let gate = vfs.gate.clone();
        let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .run(async {
                let mut store = LocalStore::open(&database)?;
                store.checkpoint_vfs = Some(vfs.clone());
                let lease = Path::from("nodes/owner.json");
                let token = store
                    .put_opts(
                        &lease,
                        Bytes::from_static(b"old").into(),
                        PutMode::Create.into(),
                    )
                    .await?;
                let token = token.e_tag.context("initial CAS has no token")?;
                store
                    .put_opts(
                        &Path::from("obsolete"),
                        Bytes::from_static(b"remove").into(),
                        PutOptions::default(),
                    )
                    .await?;
                let upload_key = Path::from("upload");
                let upload_id = store.create_multipart(&upload_key).await?;
                let receipt = MultipartStore::put_part(
                    &store,
                    &upload_key,
                    &upload_id,
                    0,
                    Bytes::from_static(b"part").into(),
                )
                .await?;
                if matches!(mutation, Mutation::Reap) {
                    store.connect()?.execute(
                        "UPDATE __sys_local_uploads SET expires_ms = 0 WHERE id = ?1",
                        [&upload_id],
                    )?;
                }
                if !matches!(mutation, Mutation::Put | Mutation::Part) {
                    // Keep an old reader while filling the WAL so its automatic
                    // checkpoint cannot complete. The next small mutation after
                    // releasing that reader must then checkpoint the pending WAL.
                    let pinned = store.connect_reader()?;
                    pinned.execute_batch("BEGIN; SELECT count(*) FROM __sys_local_objects;")?;
                    store
                        .put_opts(
                            &Path::from("large"),
                            Bytes::from(vec![0x5a; 8 << 20]).into(),
                            PutOptions::default(),
                        )
                        .await?;
                    pinned.execute_batch("ROLLBACK")?;
                }
                gate.armed.store(true, Ordering::Release);
                let slots = Arc::new(Semaphore::new(1));
                let mut write_options = PutOptions::default();
                if matches!(mutation, Mutation::Put) {
                    let permit = Arc::new(slots.clone().try_acquire_owned()?);
                    write_options
                        .extensions
                        .insert(crate::bucket::IoGuard::new(permit));
                }
                let writer_store = store.clone();
                let writer = crate::asyncrt::try_spawn(async move {
                    match mutation {
                        Mutation::Delete => writer_store.delete(&Path::from("obsolete")).await,
                        Mutation::Put => writer_store
                            .put_opts(
                                &Path::from("large"),
                                Bytes::from(vec![0x5a; 8 << 20]).into(),
                                write_options,
                            )
                            .await
                            .map(|_| ()),
                        Mutation::Part => MultipartStore::put_part(
                            &writer_store,
                            &upload_key,
                            &upload_id,
                            0,
                            Bytes::from(vec![0x5a; 8 << 20]).into(),
                        )
                        .await
                        .map(|_| ()),
                        Mutation::Complete => writer_store
                            .complete_multipart(&upload_key, &upload_id, vec![receipt])
                            .await
                            .map(|_| ()),
                        Mutation::Abort => {
                            writer_store.abort_multipart(&upload_key, &upload_id).await
                        }
                        Mutation::Reap => {
                            crate::object_multipart::ResumableMultipartStore::reap_expired(
                                &writer_store,
                                "",
                            )
                            .await
                            .map(|_| ())
                        }
                    }
                })?;
                tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire())
                    .await??
                    .forget();
                ensure!(
                    !matches!(mutation, Mutation::Put) || slots.available_permits() == 0,
                    "native checkpoint returned its I/O reservation early"
                );
                let lease_store = store.clone();
                let mut renewal = crate::asyncrt::try_spawn(async move {
                    lease_store
                        .put_opts(
                            &lease,
                            Bytes::from_static(b"renewed").into(),
                            PutMode::Update(object_store::UpdateVersion {
                                e_tag: Some(token),
                                version: None,
                            })
                            .into(),
                        )
                        .await
                })?;
                // Same first-attempt bound as a one-second lease renewed with
                // 666 ms remaining. A held main-file checkpoint is unrelated to
                // the following CAS's own FULL WAL commit.
                let observed = tokio::time::timeout(Duration::from_millis(333), &mut renewal).await;
                let timely = observed.is_ok();
                let visible_before_release = store
                    .get_opts(&Path::from("nodes/owner.json"), GetOptions::default())
                    .await?
                    .bytes()
                    .await?;
                gate.release();
                writer.await??;
                ensure!(
                    slots.available_permits() == 1,
                    "native checkpoint retained I/O capacity"
                );
                match observed {
                    Ok(result) => result??,
                    Err(_) => renewal.await??,
                };
                let saved = store
                    .get_opts(&Path::from("nodes/owner.json"), GetOptions::default())
                    .await?
                    .bytes()
                    .await?;
                ensure!(saved.as_ref() == b"renewed", "lease CAS was lost");
                ensure!(
                    !gate.watchdog.load(Ordering::Acquire),
                    "checkpoint watchdog released the fixture"
                );
                ensure!(
                    timely,
                    "lease CAS waited for unrelated {mutation:?} checkpoint sync"
                );
                ensure!(
                    visible_before_release.as_ref() == b"renewed",
                    "lease reply preceded observable WAL commit"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await;
        // Release even when setup or an assertion fails, then join entered
        // native work before the VFS registration or directory can retire.
        gate.release();
        domain.shutdown().await;
        result??;
        ensure!(
            files()
                .lock()
                .unwrap()
                .values()
                .all(|file| !Arc::ptr_eq(&file.gate, &gate)),
            "checkpoint VFS retained a file"
        );
    }
    Ok(())
}
