// Modified for Cellar from Celld.
//! Host facilities that can affect LTX scheduling or timestamps.

use std::ffi::OsString;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HostFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type BlockingJob = Box<dyn FnOnce() + Send>;
type NowUnixMillis = dyn Fn() -> i64 + Send + Sync;
type FileAge = dyn Fn(&Path) -> io::Result<Duration> + Send + Sync;
type ReadFile = dyn Fn(PathBuf) -> HostFuture<io::Result<Vec<u8>>> + Send + Sync;
type RunBlocking = dyn Fn(BlockingJob) -> HostFuture<Result<(), HostTaskError>> + Send + Sync;

/// The filesystem metadata that the LTX engine uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostMetadata {
    pub len: u64,
    pub is_dir: bool,
    pub is_file: bool,
    /// The modification time as Unix milliseconds, or zero when it is not
    /// available from the host filesystem.
    pub modified_unix_millis: i64,
}

/// One directory entry returned by the injected filesystem.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostDirEntry {
    pub path: PathBuf,
    pub file_name: OsString,
    pub is_dir: bool,
}

/// A streaming directory observation. Errors remain observable at the entry
/// that failed; implementations must not first materialize the directory.
pub type HostDirIterator = Box<dyn Iterator<Item = io::Result<HostDirEntry>> + Send>;

/// Retains an exclusive OS/virtual-filesystem lock until its owner drops it.
pub type HostFileLock = Box<dyn Send + Sync>;

/// The operations on one open injected file.
pub trait HostFileIo: Send {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>>;

    fn sync_all(&mut self) -> io::Result<()>;

    /// The file's current length, from the open handle (an `fstat`, not a
    /// path walk).
    fn file_len(&mut self) -> io::Result<u64>;

    fn write_all_at(&mut self, _offset: u64, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "random writes are unsupported",
        ))
    }

    fn set_len(&mut self, _length: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file truncation is unsupported",
        ))
    }
}

/// One open file owned by an injected filesystem.
pub struct HostFile {
    inner: Box<dyn HostFileIo>,
}

impl HostFile {
    pub fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all_at(offset, bytes)
    }

    pub fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.inner.write_all(bytes)
    }

    pub fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.inner.read_exact_at(offset, len)
    }

    pub fn sync_all(&mut self) -> io::Result<()> {
        self.inner.sync_all()
    }

    pub fn file_len(&mut self) -> io::Result<u64> {
        self.inner.file_len()
    }

    pub fn from_io(inner: impl HostFileIo + 'static) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

/// The complete local-file surface used by cellar and the LTX engine.
pub trait FileSystem: Send + Sync {
    /// Create a new read/write file without replacing existing data.
    fn create_read_write(&self, _path: &Path) -> io::Result<HostFile> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "read/write creation is unsupported",
        ))
    }

    fn open_read_write(&self, _path: &Path) -> io::Result<HostFile> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "read/write open is unsupported",
        ))
    }

    fn lock_file(&self, _path: &Path) -> io::Result<HostFileLock> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exclusive file locks are unsupported",
        ))
    }

    /// Opens an anonymous read/write scratch file. The handle owns its storage,
    /// so cancellation and process exit cannot leave a compaction scratch file.
    /// Custom filesystems must implement this to compact oversized sources.
    fn temporary_file(&self, _directory: Option<&Path>) -> io::Result<HostFile> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the filesystem does not support scratch files",
        ))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>>;

    /// Enumerate without collecting the whole directory. Retained-log recovery
    /// requires this contract; falling back to read_dir would lose its bound.
    fn read_dir_iter(&self, _path: &Path) -> io::Result<HostDirIterator> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "streaming directory enumeration is unsupported",
        ))
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata>;

    fn create(&self, path: &Path) -> io::Result<HostFile>;

    /// Create a file without replacing an existing path. Cache publication
    /// requires this atomic contract; an existence check followed by create
    /// cannot implement it safely.
    fn create_new(&self, _path: &Path) -> io::Result<HostFile> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exclusive creation is unsupported",
        ))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile>;

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    fn sync_all(&self, path: &Path) -> io::Result<()>;

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    fn remove_file(&self, path: &Path) -> io::Result<()>;

    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Remove at most `limit` files/directories from a closed subtree, without
    /// following symlinks or materializing its whole inventory. Return true
    /// only when the root is absent. Interrupted batches must be resumable.
    fn remove_dir_batch(&self, _path: &Path, _limit: usize) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded directory cleanup is unsupported",
        ))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Best-effort cache recency, in the caller's wall-clock time.
    fn set_modified_unix_millis(&self, _path: &Path, _millis: i64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "modification time is unsupported",
        ))
    }
}

/// The telemetry-only microsecond clock for the capture ledger
/// (`db::SyncTiming`). Nothing branches on these values — they exist so
/// the closed-book attribution ledger can sum — so deterministic
/// schedules are unchanged; the host owns the ambient primitive exactly
/// as it owns the filesystem and the wall clock.
#[allow(clippy::disallowed_methods)]
pub(crate) fn telemetry_us() -> u64 {
    use std::sync::OnceLock;

    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_micros() as u64
}

/// The ordinary direct-filesystem backend.
#[derive(Default)]
pub struct DirectFileSystem;

#[allow(clippy::disallowed_methods)]
fn remove_direct_leaf(path: &Path, kind: std::fs::FileType) -> io::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileTypeExt;
        if kind.is_symlink_dir() {
            return std::fs::remove_dir(path);
        }
    }
    #[cfg(not(windows))]
    let _ = kind;
    std::fs::remove_file(path)
}

struct DirectFile {
    file: std::fs::File,
}

#[allow(clippy::disallowed_methods)]
impl HostFileIo for DirectFile {
    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        use std::io::{Seek as _, SeekFrom, Write as _};
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(bytes)
    }

    fn set_len(&mut self, length: u64) -> io::Result<()> {
        self.file.set_len(length)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        std::io::Write::write_all(&mut self.file, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        use std::io::{Read as _, Seek as _, SeekFrom};
        self.file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; len];
        self.file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

#[allow(clippy::disallowed_methods)]
impl FileSystem for DirectFileSystem {
    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(DirectFile {
            file: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)?,
        }))
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(DirectFile {
            file: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)?,
        }))
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock().map_err(io::Error::from)?;
        Ok(Box::new(file))
    }

    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        let file = match directory {
            Some(directory) => tempfile::tempfile_in(directory)?,
            None => tempfile::tempfile()?,
        };
        Ok(HostFile::from_io(DirectFile { file }))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.read_dir_iter(path)?.collect()
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        Ok(Box::new(std::fs::read_dir(path)?.map(|entry| {
            let entry = entry?;
            let file_type = entry.file_type()?;
            Ok(HostDirEntry {
                path: entry.path(),
                file_name: entry.file_name(),
                is_dir: file_type.is_dir(),
            })
        })))
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        let metadata = std::fs::metadata(path)?;
        let modified_unix_millis = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0);
        Ok(HostMetadata {
            len: metadata.len(),
            is_dir: metadata.is_dir(),
            is_file: metadata.is_file(),
            modified_unix_millis,
        })
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(DirectFile {
            file: std::fs::File::create(path)?,
        }))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(DirectFile {
            file: std::fs::File::open(path)?,
        }))
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        Ok(HostFile::from_io(DirectFile {
            file: std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?,
        }))
    }

    fn set_modified_unix_millis(&self, path: &Path, millis: i64) -> io::Result<()> {
        let millis = u64::try_from(millis)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "negative file timestamp"))?;
        let modified = UNIX_EPOCH
            .checked_add(Duration::from_millis(millis))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "file timestamp exceeds host clock",
                )
            })?;
        std::fs::File::options()
            .write(true)
            .open(path)?
            .set_times(std::fs::FileTimes::new().set_modified(modified))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        std::fs::write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        if limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty cleanup budget",
            ));
        }
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        };
        if !metadata.is_dir() {
            remove_direct_leaf(path, metadata.file_type())?;
            return Ok(true);
        }
        let mut current = path.to_path_buf();
        let mut removed = 0;
        loop {
            // Keep one path and one entry, releasing each directory iterator
            // before descending. Wide trees consume no inventory-sized memory,
            // and deep trees do not keep a stack of open directory handles.
            let next = std::fs::read_dir(&current)?.next().transpose()?;
            if let Some(entry) = next {
                let kind = entry.file_type()?;
                if kind.is_dir() {
                    current = entry.path();
                    continue;
                }
                remove_direct_leaf(&entry.path(), kind)?;
            } else {
                std::fs::remove_dir(&current)?;
                if current == path {
                    return Ok(true);
                }
                current.pop();
            }
            removed += 1;
            if removed == limit {
                return Ok(false);
            }
        }
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }
}

/// A failure to dispatch or join one host blocking job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostTaskError {
    message: String,
}

impl HostTaskError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for HostTaskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for HostTaskError {}

struct Inner {
    now_unix_millis: Arc<NowUnixMillis>,
    file_age: Arc<FileAge>,
    read_file: Arc<ReadFile>,
    run_blocking: Arc<RunBlocking>,
    filesystem: Arc<dyn FileSystem>,
}

/// The clock and executor facilities used by the LTX engine.
///
/// The default uses the host clock, filesystem, and Tokio executor. A caller
/// can inject one object when it needs to own these observations and jobs.
#[derive(Clone)]
pub struct LtxHost {
    inner: Arc<Inner>,
}

impl LtxHost {
    /// Creates an injected host from four cohesive facilities.
    pub fn new<N, A, R, RF, B, BF>(now: N, age: A, read: R, blocking: B) -> Self
    where
        N: Fn() -> i64 + Send + Sync + 'static,
        A: Fn(&Path) -> io::Result<Duration> + Send + Sync + 'static,
        R: Fn(PathBuf) -> RF + Send + Sync + 'static,
        RF: Future<Output = io::Result<Vec<u8>>> + Send + 'static,
        B: Fn(BlockingJob) -> BF + Send + Sync + 'static,
        BF: Future<Output = Result<(), HostTaskError>> + Send + 'static,
    {
        Self {
            inner: Arc::new(Inner {
                now_unix_millis: Arc::new(now),
                file_age: Arc::new(age),
                read_file: Arc::new(move |path| Box::pin(read(path))),
                run_blocking: Arc::new(move |job| Box::pin(blocking(job))),
                filesystem: Arc::new(DirectFileSystem),
            }),
        }
    }

    /// Selects the filesystem used by synchronous local LTX operations.
    pub fn with_filesystem(mut self, filesystem: Arc<dyn FileSystem>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("a fresh LtxHost has one owner")
            .filesystem = filesystem;
        self
    }

    pub fn filesystem(&self) -> Arc<dyn FileSystem> {
        self.inner.filesystem.clone()
    }

    pub fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.inner.filesystem.read(path)
    }

    pub fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.inner.filesystem.read_dir(path)
    }

    pub fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.inner.filesystem.metadata(path)
    }

    pub fn create(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.filesystem.create(path)
    }

    pub fn open(&self, path: &Path) -> io::Result<HostFile> {
        self.inner.filesystem.open(path)
    }

    pub fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.inner.filesystem.write(path, bytes)
    }

    pub fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.inner.filesystem.sync_all(path)
    }

    pub fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.filesystem.rename(from, to)
    }

    pub fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.filesystem.remove_file(path)
    }

    pub fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.filesystem.remove_dir_all(path)
    }

    pub fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.filesystem.create_dir_all(path)
    }

    /// Returns the current wall time as Unix milliseconds.
    pub fn now_unix_millis(&self) -> i64 {
        (self.inner.now_unix_millis)()
    }

    /// Returns the age of one file according to the host wall-clock view.
    pub fn file_age(&self, path: &Path) -> io::Result<Duration> {
        (self.inner.file_age)(path)
    }

    /// Reads one compactor input through the host executor.
    pub async fn read_file(&self, path: impl Into<PathBuf>) -> io::Result<Vec<u8>> {
        (self.inner.read_file)(path.into()).await
    }

    /// Runs one CPU or filesystem job through the host blocking executor.
    pub async fn run_blocking<T, F>(&self, operation: F) -> Result<T, HostTaskError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let (send, receive) = tokio::sync::oneshot::channel();
        let job = Box::new(move || {
            let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
                .map_err(|_| HostTaskError::new("the host blocking job panicked"));
            let _ = send.send(output);
        });
        (self.inner.run_blocking)(job).await?;
        receive
            .await
            .map_err(|_| HostTaskError::new("the host blocking job stopped without a result"))?
    }
}

// This is the lower crate's stand-alone production backend. cellar injects an
// execution-domain-backed host on every engine path.
#[allow(clippy::disallowed_methods)]
impl Default for LtxHost {
    fn default() -> Self {
        Self::new(
            || {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_millis() as i64)
                    .unwrap_or(0)
            },
            |path| {
                let modified = std::fs::metadata(path)?.modified()?;
                Ok(SystemTime::now()
                    .duration_since(modified)
                    .unwrap_or(Duration::ZERO))
            },
            |path| async move { tokio::fs::read(path).await },
            |job| async move {
                tokio::task::spawn_blocking(job)
                    .await
                    .map_err(|error| HostTaskError::new(error.to_string()))
            },
        )
    }
}
