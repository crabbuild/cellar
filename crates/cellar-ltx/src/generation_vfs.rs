// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Process-lifetime, bounded SQLite VFS slots over injected immutable readers.
//! A descriptor is never freed under SQLite's global vfs_find callers. Slots
//! release their generation/services after both the owner and all files close.

use crate::generation::{GenerationPin, GenerationReader};
use rusqlite::{ffi, Connection, OpenFlags};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

type Clock = dyn Fn() -> i64 + Send + Sync;
type Random = dyn Fn(&mut [u8]) + Send + Sync;

#[derive(Clone)]
pub struct ReadServices {
    pub now_unix_millis: Arc<Clock>,
    pub fill_random: Arc<Random>,
}

struct ReadContext {
    name: String,
    pin: GenerationPin,
    services: ReadServices,
}

struct SlotState {
    owner: bool,
    files: usize,
    context: Option<Arc<ReadContext>>,
}

struct Slot {
    state: Mutex<SlotState>,
    name: CString,
}

static POOL: OnceLock<Mutex<Vec<&'static Slot>>> = OnceLock::new();
static NEXT_NAME: AtomicU64 = AtomicU64::new(0);
const MAX_VFS_SLOTS: usize = 4096;

struct SlotGuard(&'static Slot);

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let context = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.owner = false;
            if state.files == 0 {
                state.context.take()
            } else {
                None
            }
        };
        drop(context);
    }
}

struct FileReference(&'static Slot);

impl Drop for FileReference {
    fn drop(&mut self) {
        let context = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.files -= 1;
            if !state.owner && state.files == 0 {
                state.context.take()
            } else {
                None
            }
        };
        drop(context);
    }
}

/// Connection closes before its slot owner. Open files retain the same slot
/// context until their own close. A retired URI cannot open a reused slot.
pub struct ImmutableDatabase {
    connection: Connection,
    _slot: SlotGuard,
}

impl ImmutableDatabase {
    pub fn connection(&self) -> &Connection {
        &self.connection
    }
}

#[repr(C)]
struct ImageFile {
    methods: *const ffi::sqlite3_io_methods,
    state: *mut FileState,
}

struct FileState {
    reader: Mutex<GenerationReader>,
    _reference: FileReference,
}

pub fn open_immutable(
    pin: GenerationPin,
    services: ReadServices,
) -> rusqlite::Result<ImmutableDatabase> {
    if pin.state().length == 0 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let sequence = NEXT_NAME
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
        .map_err(|_| sqlite_error(ffi::SQLITE_FULL))?;
    let context = Arc::new(ReadContext {
        name: format!("cellar-image-{sequence}.sqlite"),
        pin,
        services,
    });
    let guard = reserve(context.clone())?;
    let name = guard
        .0
        .name
        .to_str()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let connection = Connection::open_with_flags_and_vfs(
        format!("file:{}?immutable=1&mode=ro", context.name),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
        name,
    )?;
    connection.execute_batch(
        "PRAGMA query_only=ON; PRAGMA temp_store=MEMORY; PRAGMA trusted_schema=OFF;",
    )?;
    Ok(ImmutableDatabase {
        connection,
        _slot: guard,
    })
}

fn reserve(context: Arc<ReadContext>) -> rusqlite::Result<SlotGuard> {
    static INITIALIZED: OnceLock<c_int> = OnceLock::new();
    // SAFETY: SQLite's initializer is synchronized and is called once here.
    let code = *INITIALIZED.get_or_init(|| unsafe { ffi::sqlite3_initialize() });
    if code != ffi::SQLITE_OK {
        return Err(sqlite_error(code));
    }
    let mut pool = POOL
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .map_err(|_| sqlite_error(ffi::SQLITE_IOERR))?;
    for slot in pool.iter() {
        let mut state = slot
            .state
            .lock()
            .map_err(|_| sqlite_error(ffi::SQLITE_IOERR))?;
        if !state.owner && state.files == 0 && state.context.is_none() {
            state.context = Some(context);
            state.owner = true;
            return Ok(SlotGuard(slot));
        }
    }
    if pool.len() >= MAX_VFS_SLOTS {
        return Err(sqlite_error(ffi::SQLITE_BUSY));
    }
    let name = CString::new(format!("cellar-image-vfs-{}", pool.len()))
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    let slot = Box::new(Slot {
        state: Mutex::new(SlotState {
            owner: true,
            files: 0,
            context: Some(context),
        }),
        name,
    });
    // SAFETY: SQLite's descriptor is C POD. Zero denotes absent optional
    // methods. The pointers receive stable allocations before registration.
    let mut vfs: Box<ffi::sqlite3_vfs> = Box::new(unsafe { std::mem::zeroed() });
    vfs.iVersion = 1;
    vfs.szOsFile = std::mem::size_of::<ImageFile>() as c_int;
    vfs.mxPathname = 256;
    vfs.zName = slot.name.as_ptr();
    vfs.xOpen = Some(x_open);
    vfs.xDelete = Some(x_delete);
    vfs.xAccess = Some(x_access);
    vfs.xFullPathname = Some(x_full_pathname);
    vfs.xDlOpen = Some(x_dl_open);
    vfs.xDlError = Some(x_dl_error);
    vfs.xDlClose = Some(x_dl_close);
    vfs.xRandomness = Some(x_randomness);
    vfs.xSleep = Some(x_sleep);
    vfs.xCurrentTime = Some(x_current_time);
    vfs.xGetLastError = Some(x_last_error);
    let slot = Box::into_raw(slot);
    let vfs = Box::into_raw(vfs);
    // SAFETY: one descriptor/slot allocation per peak concurrent owner is
    // retained for the process, bounded by MAX_VFS_SLOTS. Only SQLite mutates
    // descriptor registry links. No safe Rust references alias those links.
    unsafe {
        (*vfs).pAppData = slot.cast();
        if !ffi::sqlite3_vfs_find((*slot).name.as_ptr()).is_null() {
            drop(Box::from_raw(vfs));
            drop(Box::from_raw(slot));
            return Err(sqlite_error(ffi::SQLITE_CANTOPEN));
        }
        let code = ffi::sqlite3_vfs_register(vfs, 0);
        if code != ffi::SQLITE_OK {
            drop(Box::from_raw(vfs));
            drop(Box::from_raw(slot));
            return Err(sqlite_error(code));
        }
        let slot = &*slot;
        pool.push(slot);
        Ok(SlotGuard(slot))
    }
}

fn sqlite_error(code: c_int) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(ffi::Error::new(code), None)
}

unsafe fn slot(vfs: *mut ffi::sqlite3_vfs) -> &'static Slot {
    // SAFETY: every registered descriptor points to its permanent slot.
    unsafe { &*(*vfs).pAppData.cast::<Slot>() }
}

fn lookup(slot: &Slot, name: &CStr) -> Option<Arc<ReadContext>> {
    let state = slot.state.lock().ok()?;
    let context = state.context.as_ref()?;
    if state.owner && name.to_bytes() == context.name.as_bytes() {
        Some(context.clone())
    } else {
        None
    }
}

fn current(slot: &Slot) -> Option<Arc<ReadContext>> {
    slot.state.lock().ok()?.context.clone()
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: SQLite supplies a szOsFile-sized, suitably aligned output.
        let image = unsafe { &mut *file.cast::<ImageFile>() };
        image.methods = ptr::null();
        image.state = ptr::null_mut();
        if name.is_null()
            || flags & ffi::SQLITE_OPEN_MAIN_DB == 0
            || flags & (ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_READWRITE) != 0
        {
            return ffi::SQLITE_CANTOPEN;
        }
        // SAFETY: SQLite's non-null filename is NUL terminated.
        let slot = unsafe { slot(vfs) };
        let (context, reference) = {
            let Ok(mut state) = slot.state.lock() else {
                return ffi::SQLITE_CANTOPEN;
            };
            let Some(context) = state.context.as_ref().cloned() else {
                return ffi::SQLITE_CANTOPEN;
            };
            if !state.owner
                || context.name.as_bytes() != unsafe { CStr::from_ptr(name) }.to_bytes()
                || state.files >= 65_536
            {
                return ffi::SQLITE_CANTOPEN;
            }
            state.files += 1;
            (context, FileReference(slot))
        };
        let reader = match context.pin.open_reader() {
            Ok(reader) => reader,
            Err(_) => return ffi::SQLITE_CANTOPEN,
        };
        image.state = Box::into_raw(Box::new(FileState {
            reader: Mutex::new(reader),
            _reference: reference,
        }));
        image.methods = &IO_METHODS;
        if !out.is_null() {
            // SAFETY: SQLite's optional output points to one writable integer.
            unsafe { *out = ffi::SQLITE_OPEN_READONLY };
        }
        ffi::SQLITE_OK
    }))
    .unwrap_or(ffi::SQLITE_CANTOPEN)
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: successful xOpen installed one FileState in SQLite's slab.
        let image = unsafe { &mut *file.cast::<ImageFile>() };
        if !image.state.is_null() {
            // SAFETY: take ownership exactly once, clearing the slab first.
            let state = unsafe { Box::from_raw(image.state) };
            image.state = ptr::null_mut();
            image.methods = ptr::null();
            drop(state);
        }
        ffi::SQLITE_OK
    }))
    .unwrap_or(ffi::SQLITE_IOERR_CLOSE)
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buffer: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if amount < 0 || offset < 0 || amount > 0 && buffer.is_null() {
            return ffi::SQLITE_IOERR_READ;
        }
        if amount == 0 {
            return ffi::SQLITE_OK;
        }
        // SAFETY: SQLite supplies amount writable bytes for a positive xRead.
        let output =
            unsafe { std::slice::from_raw_parts_mut(buffer.cast::<u8>(), amount as usize) };
        output.fill(0);
        // SAFETY: SQLite invokes methods only after successful xOpen.
        let state = unsafe { &*(*file.cast::<ImageFile>()).state };
        let Ok(mut reader) = state.reader.lock() else {
            return ffi::SQLITE_IOERR_READ;
        };
        let available = reader
            .length()
            .saturating_sub(offset as u64)
            .min(amount as u64) as usize;
        let mut at = 0;
        while at < available {
            let length = (available - at).min(64 * 1024);
            let bytes = match reader.read_exact_at(offset as u64 + at as u64, length) {
                Ok(bytes) => bytes,
                Err(_) => return ffi::SQLITE_IOERR_READ,
            };
            output[at..at + length].copy_from_slice(&bytes);
            at += length;
        }
        if available < output.len() {
            ffi::SQLITE_IOERR_SHORT_READ
        } else {
            ffi::SQLITE_OK
        }
    }))
    .unwrap_or(ffi::SQLITE_IOERR_READ)
}

unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    // SAFETY: SQLite supplies a valid open-file slab and writable size output.
    let state = unsafe { &*(*file.cast::<ImageFile>()).state };
    let Ok(reader) = state.reader.lock() else {
        return ffi::SQLITE_IOERR_FSTAT;
    };
    // SAFETY: the output is one writable sqlite3_int64.
    unsafe { *out = reader.length() as i64 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_write(
    _: *mut ffi::sqlite3_file,
    _: *const c_void,
    _: c_int,
    _: i64,
) -> c_int {
    ffi::SQLITE_READONLY
}

unsafe extern "C" fn x_truncate(_: *mut ffi::sqlite3_file, _: i64) -> c_int {
    ffi::SQLITE_READONLY
}

unsafe extern "C" fn x_sync(_: *mut ffi::sqlite3_file, _: c_int) -> c_int {
    ffi::SQLITE_READONLY
}

unsafe extern "C" fn x_lock(_: *mut ffi::sqlite3_file, _: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_unlock(_: *mut ffi::sqlite3_file, _: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_reserved(_: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    // SAFETY: SQLite supplies one writable integer output.
    unsafe { *out = 0 };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_control(_: *mut ffi::sqlite3_file, _: c_int, _: *mut c_void) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector(_: *mut ffi::sqlite3_file) -> c_int {
    4096
}

unsafe extern "C" fn x_device(_: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_IMMUTABLE
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_reserved),
    xFileControl: Some(x_control),
    xSectorSize: Some(x_sector),
    xDeviceCharacteristics: Some(x_device),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn x_delete(_: *mut ffi::sqlite3_vfs, _: *const c_char, _: c_int) -> c_int {
    ffi::SQLITE_READONLY
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    _: c_int,
    out: *mut c_int,
) -> c_int {
    // SAFETY: SQLite supplies a terminated name and integer output. No sidecar
    // or arbitrary native path exists in this virtual namespace.
    unsafe {
        *out = i32::from(!name.is_null() && lookup(slot(vfs), CStr::from_ptr(name)).is_some())
    };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    if name.is_null() {
        return ffi::SQLITE_CANTOPEN;
    }
    // SAFETY: SQLite provides a NUL-terminated input.
    let name = unsafe { CStr::from_ptr(name) };
    // SAFETY: the descriptor's slot is permanent.
    if lookup(unsafe { slot(vfs) }, name).is_none() || size <= name.to_bytes().len() as c_int {
        return ffi::SQLITE_CANTOPEN;
    }
    // SAFETY: the checked destination fits the canonical virtual name plus NUL.
    unsafe { ptr::copy_nonoverlapping(name.as_ptr(), out, name.to_bytes_with_nul().len()) };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_dl_open(_: *mut ffi::sqlite3_vfs, _: *const c_char) -> *mut c_void {
    ptr::null_mut()
}

unsafe extern "C" fn x_dl_error(_: *mut ffi::sqlite3_vfs, size: c_int, out: *mut c_char) {
    if size > 0 {
        // SAFETY: SQLite provides size writable error-buffer bytes.
        unsafe { *out = 0 };
    }
}

unsafe extern "C" fn x_dl_close(_: *mut ffi::sqlite3_vfs, _: *mut c_void) {}

unsafe extern "C" fn x_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    amount: c_int,
    out: *mut c_char,
) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if amount <= 0 || out.is_null() {
            return 0;
        }
        // SAFETY: the descriptor's slot is permanent.
        let Some(context) = current(unsafe { slot(vfs) }) else {
            return 0;
        };
        // SAFETY: SQLite supplies amount writable bytes.
        let bytes = unsafe { std::slice::from_raw_parts_mut(out.cast::<u8>(), amount as usize) };
        bytes.fill(0);
        (context.services.fill_random)(bytes);
        amount
    }))
    .unwrap_or(0)
}

unsafe extern "C" fn x_sleep(_: *mut ffi::sqlite3_vfs, _: c_int) -> c_int {
    0
}

unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: the descriptor's slot is permanent.
        let Some(context) = current(unsafe { slot(vfs) }) else {
            return ffi::SQLITE_ERROR;
        };
        let millis = (context.services.now_unix_millis)();
        // SAFETY: SQLite supplies a writable Julian-day output.
        unsafe { *out = 2_440_587.5 + millis as f64 / 86_400_000.0 };
        ffi::SQLITE_OK
    }))
    .unwrap_or(ffi::SQLITE_ERROR)
}

unsafe extern "C" fn x_last_error(
    _: *mut ffi::sqlite3_vfs,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    if size > 0 {
        // SAFETY: SQLite supplies size writable error-buffer bytes.
        unsafe { *out = 0 };
    }
    0
}
