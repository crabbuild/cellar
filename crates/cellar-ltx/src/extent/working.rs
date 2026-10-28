// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable writable SQLite main files over an exact immutable extent base.
//! WAL, locking and file persistence stay with the injected SQLite VFS. Nothing
//! in this module selects a committed root or acknowledges an application write.

use super::{
    image::{CacheLimits, ExtentImage, ExtentSource},
    store::SealedImageRoot,
    tree::{ExtentManifest, EXTENT_BYTES},
};
use crate::{
    paged_vfs,
    stream_snapshot::{ScratchPool, MAX_IMAGE_BYTES},
    FileSystem, HostFile,
};
use rusqlite::ffi;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};

#[cfg(test)]
#[path = "working_tests.rs"]
mod tests;

pub struct WorkspaceOptions {
    pub path: PathBuf,
    pub sqlite_vfs: Option<String>,
    pub filesystem: Arc<dyn FileSystem>,
    pub max_image_bytes: u64,
    /// Bound physical WAL writes before SQLite can spill an oversized command.
    pub max_wal_bytes: u64,
    pub cache_directory: PathBuf,
    pub scratch: ScratchPool,
    pub cache: CacheLimits,
    /// Retain the node's named-file reservation through every open VFS handle.
    pub retention: Option<Arc<dyn Send + Sync>>,
}

/// Logical work, not process RSS, disk allocation or durability evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceStats {
    pub length: u64,
    pub extent_slots: usize,
    pub hydrated_extents: usize,
    pub bytes_faulted: u64,
    pub main_sequence: u64,
}

/// The caller exclusively owns this disposable path and admits its disk quota
/// separately. Every connection must use this VFS; external file writes are
/// forbidden. Drop unregisters the name; already-open SQLite files retain the
/// base, cache and registration until their own last close. The caller removes
/// the main file, WAL, SHM and capture metadata after all connections close.
pub struct ExtentWorkspace {
    name: String,
    // Close the exclusive creation handle before the main owner can release
    // its directory's last physical reservation.
    _file: HostFile,
    main: Arc<MainWorkspace>,
}

enum Base {
    Local(Arc<SealedImageRoot>),
    Source {
        manifest: ExtentManifest,
        source: Arc<dyn ExtentSource>,
    },
}

impl Base {
    fn manifest(&self) -> &ExtentManifest {
        match self {
            Self::Local(image) => image.manifest(),
            Self::Source { manifest, .. } => manifest,
        }
    }
}

impl ExtentWorkspace {
    pub fn new(base: Arc<SealedImageRoot>, options: WorkspaceOptions) -> crate::Result<Self> {
        Self::open(Base::Local(base), options)
    }

    /// Open a disposable writer without first materializing its immutable base.
    /// The caller authenticates the manifest and retains every source dependency
    /// for the lifetime of all SQLite/VFS handles. Reads use the same bounded,
    /// independently verified extent cache as read-only demand loading.
    ///
    /// Neither this workspace nor its captured LTX supplies durable root or
    /// native activation authority. Publication still needs an independent
    /// retained-base/result proof; private files are discarded after restart.
    pub fn from_source(
        manifest: ExtentManifest,
        source: Arc<dyn ExtentSource>,
        options: WorkspaceOptions,
    ) -> crate::Result<Self> {
        Self::open(Base::Source { manifest, source }, options)
    }

    fn open(base: Base, options: WorkspaceOptions) -> crate::Result<Self> {
        let geometry = base.manifest().geometry();
        if geometry.length() > options.max_image_bytes
            || !(512..=MAX_IMAGE_BYTES).contains(&options.max_image_bytes)
            || !options.max_image_bytes.is_multiple_of(512)
            || !(32..=512 * 1024 * 1024).contains(&options.max_wal_bytes)
        {
            return Err(crate::Error::Other("extent workspace image quota".into()));
        }
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = options.path.as_os_str().to_owned();
            sidecar.push(suffix);
            match options.filesystem.metadata(&PathBuf::from(sidecar)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
                Ok(_) => {
                    return Err(crate::Error::Other(
                        "extent workspace sidecar exists".into(),
                    ))
                }
            }
        }
        let image = if geometry.length() == 0 {
            None
        } else {
            let image = match &base {
                Base::Local(image) => image
                    .view()
                    .image(options.cache_directory, options.scratch, options.cache)
                    .map_err(|error| crate::Error::Other(Box::new(error)))?,
                Base::Source { manifest, source } => ExtentImage::new(
                    manifest.clone(),
                    source.clone(),
                    options.filesystem.clone(),
                    options.cache_directory,
                    options.scratch,
                    options.cache,
                )
                .map_err(|error| crate::Error::Other(Box::new(error)))?,
            };
            Some(image)
        };
        // Exclusive creation prevents a previous working file from being
        // mistaken for a zero-filled view of this base. Never replace a file.
        let file = options.filesystem.create_read_write(&options.path)?;
        let main = Arc::new(MainWorkspace {
            image,
            base,
            max_bytes: options.max_image_bytes,
            max_wal_bytes: options.max_wal_bytes,
            _retention: options.retention,
            state: Mutex::new(State {
                initialized: false,
                failed: false,
                length: geometry.length(),
                source_limit: geometry.length(),
                sequence: 0,
                bytes_faulted: 0,
                slots: vec![
                    Slot::default();
                    options.max_image_bytes.div_ceil(EXTENT_BYTES) as usize
                ],
            }),
        });
        let name = paged_vfs::next_registration_name();
        paged_vfs::register_extent_workspace(
            &name,
            options.sqlite_vfs.as_deref(),
            &options.path,
            main.clone(),
        )?;
        Ok(Self {
            name,
            main,
            _file: file,
        })
    }

    pub fn vfs_name(&self) -> &str {
        &self.name
    }

    pub fn stats(&self) -> crate::Result<WorkspaceStats> {
        self.main
            .stats()
            .map_err(|_| crate::Error::Other("extent workspace failed".into()))
    }
}

impl Drop for ExtentWorkspace {
    fn drop(&mut self) {
        // Registration and CString were created here; this name cannot contain
        // NUL. Unregistration retains allocations while any file remains open.
        let _ = paged_vfs::unregister_paged_vfs(&self.name);
    }
}

/// A narrow synchronous adapter over the existing base sqlite3_file. Calls
/// happen only on SQLite's calling thread with this main-file mutex held.
pub(crate) trait MainFile {
    fn size(&mut self) -> Result<u64, i32>;

    fn read(&mut self, offset: u64, bytes: &mut [u8]) -> Result<(), i32>;

    fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<(), i32>;

    fn truncate(&mut self, size: u64) -> Result<(), i32>;
}

#[derive(Clone, Copy, Default)]
struct Slot {
    hydrated: bool,
    written_at: u64,
}

struct State {
    initialized: bool,
    failed: bool,
    length: u64,
    // Truncation permanently removes old source bytes, even after regrowth.
    source_limit: u64,
    sequence: u64,
    bytes_faulted: u64,
    slots: Vec<Slot>,
}

pub(crate) struct MainWorkspace {
    image: Option<ExtentImage>,
    base: Base,
    max_bytes: u64,
    max_wal_bytes: u64,
    _retention: Option<Arc<dyn Send + Sync>>,
    state: Mutex<State>,
}

pub(crate) struct MainCut {
    pub sequence: u64,
    pub baseline: ExtentManifest,
    pub changed: Vec<u32>,
}

impl MainWorkspace {
    fn state(&self) -> Result<MutexGuard<'_, State>, i32> {
        let state = self.state.lock().map_err(|_| ffi::SQLITE_IOERR)?;
        if state.failed {
            return Err(ffi::SQLITE_IOERR);
        }
        Ok(state)
    }

    pub fn initialize(&self, file: &mut impl MainFile) -> Result<(), i32> {
        let mut state = self.state()?;
        if !state.initialized {
            let initialized = (|| {
                if file.size()? != 0 {
                    return Err(ffi::SQLITE_IOERR);
                }
                truncate_verified(file, state.length)
            })();
            if let Err(error) = initialized {
                state.failed = true;
                return Err(error);
            }
            state.initialized = true;
        }
        Ok(())
    }

    // Serialize faults with checkpoint writes to this same main file. Source
    // I/O holds no cache or inventory mutex; concurrent SQLite connections must
    // never overwrite a newer checkpoint with an older base extent.
    fn hydrate(
        &self,
        state: &mut State,
        index: usize,
        file: &mut impl MainFile,
    ) -> Result<(), i32> {
        if state.slots.get(index).ok_or(ffi::SQLITE_IOERR)?.hydrated {
            return Ok(());
        }
        let mut offset = index as u64 * EXTENT_BYTES;
        let start = offset;
        let end = (offset + EXTENT_BYTES).min(state.source_limit);
        let mut source_hash = Sha256::new();
        while offset < end {
            let count = (end - offset).min(65536) as usize;
            let bytes = self
                .image
                .as_ref()
                .ok_or(ffi::SQLITE_IOERR_READ)?
                .read_exact_at(offset, count)
                .map_err(|_| ffi::SQLITE_IOERR_READ)?;
            if let Err(error) = file.write(offset, &bytes) {
                state.failed = true;
                return Err(error);
            }
            source_hash.update(&bytes);
            state.bytes_faulted = state
                .bytes_faulted
                .checked_add(count as u64)
                .ok_or(ffi::SQLITE_IOERR)?;
            offset += count as u64;
        }
        // The destination is disposable, but SQL must not observe a corrupted
        // copy of its authenticated base. Check it independently before marking
        // the extent hydrated, including a surviving prefix after truncation.
        if start < end {
            let verified = (|| {
                let mut hash = Sha256::new();
                let mut offset = start;
                while offset < end {
                    let mut bytes = vec![0; (end - offset).min(65536) as usize];
                    file.read(offset, &mut bytes)?;
                    hash.update(&bytes);
                    offset += bytes.len() as u64;
                }
                if hash.finalize() != source_hash.finalize() {
                    return Err(ffi::SQLITE_IOERR_DATA);
                }
                Ok(())
            })();
            if let Err(error) = verified {
                state.failed = true;
                return Err(error);
            }
        }
        state.slots[index].hydrated = true;
        Ok(())
    }

    pub fn read(
        &self,
        offset: u64,
        output: &mut [u8],
        file: &mut impl MainFile,
    ) -> Result<(), i32> {
        let end = offset
            .checked_add(output.len() as u64)
            .ok_or(ffi::SQLITE_IOERR_READ)?;
        let mut state = self.state()?;
        if !state.initialized {
            return Err(ffi::SQLITE_IOERR_READ);
        }
        let through = end.min(state.length);
        if offset < through {
            for index in offset / EXTENT_BYTES..=(through - 1) / EXTENT_BYTES {
                self.hydrate(&mut state, index as usize, file)?;
            }
        }
        // The base VFS supplies SQLite's zero-fill / SHORT_READ at EOF.
        file.read(offset, output)
    }

    pub fn write(&self, offset: u64, bytes: &[u8], file: &mut impl MainFile) -> Result<(), i32> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= self.max_bytes)
            .ok_or(ffi::SQLITE_FULL)?;
        let mut state = self.state()?;
        if !state.initialized {
            return Err(ffi::SQLITE_IOERR_WRITE);
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let sequence = state.sequence.checked_add(1).ok_or(ffi::SQLITE_FULL)?;
        let first = (offset / EXTENT_BYTES) as usize;
        let last = ((end - 1) / EXTENT_BYTES) as usize;
        for index in first..=last {
            self.hydrate(&mut state, index, file)?;
        }
        if let Err(error) = file
            .write(offset, bytes)
            .and_then(|()| verify_write(file, offset, bytes))
        {
            state.failed = true;
            return Err(error);
        }
        state.length = state.length.max(end);
        state.sequence = sequence;
        for slot in &mut state.slots[first..=last] {
            slot.written_at = sequence;
        }
        Ok(())
    }

    pub fn truncate(&self, length: u64, file: &mut impl MainFile) -> Result<(), i32> {
        if length > self.max_bytes {
            return Err(ffi::SQLITE_FULL);
        }
        let mut state = self.state()?;
        if !state.initialized {
            return Err(ffi::SQLITE_IOERR_TRUNCATE);
        }
        if length == state.length {
            return Ok(());
        }
        let sequence = state.sequence.checked_add(1).ok_or(ffi::SQLITE_FULL)?;
        if let Err(error) = truncate_verified(file, length) {
            state.failed = true;
            return Err(error);
        }
        let first = (length.min(state.length) / EXTENT_BYTES) as usize;
        let through = length.max(state.length).div_ceil(EXTENT_BYTES) as usize;
        for (index, slot) in state.slots.iter_mut().enumerate().take(through).skip(first) {
            slot.written_at = sequence;
            if index as u64 * EXTENT_BYTES >= length {
                slot.hydrated = false;
            }
        }
        state.length = length;
        state.source_limit = state.source_limit.min(length);
        state.sequence = sequence;
        Ok(())
    }

    pub fn write_wal(
        &self,
        offset: u64,
        bytes: &[u8],
        file: &mut impl MainFile,
    ) -> Result<(), i32> {
        offset
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= self.max_wal_bytes)
            .ok_or(ffi::SQLITE_FULL)?;
        let mut state = self.state()?;
        if let Err(error) = file
            .write(offset, bytes)
            .and_then(|()| verify_write(file, offset, bytes))
        {
            state.failed = true;
            return Err(error);
        }
        Ok(())
    }

    pub fn truncate_wal(&self, length: u64, file: &mut impl MainFile) -> Result<(), i32> {
        if length > self.max_wal_bytes {
            return Err(ffi::SQLITE_FULL);
        }
        let mut state = self.state()?;
        if let Err(error) = truncate_verified(file, length) {
            state.failed = true;
            return Err(error);
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<WorkspaceStats, i32> {
        let state = self.state()?;
        Ok(WorkspaceStats {
            length: state.length,
            extent_slots: state.slots.len(),
            hydrated_extents: state.slots.iter().filter(|slot| slot.hydrated).count(),
            bytes_faulted: state.bytes_faulted,
            main_sequence: state.sequence,
        })
    }

    pub fn cut(&self, since: u64) -> Result<MainCut, i32> {
        let state = self.state()?;
        if !state.initialized || since > state.sequence {
            return Err(ffi::SQLITE_IOERR);
        }
        Ok(MainCut {
            sequence: state.sequence,
            baseline: self.base.manifest().clone(),
            changed: state
                .slots
                .iter()
                .enumerate()
                .filter_map(|(index, slot)| (slot.written_at > since).then_some(index as u32))
                .collect(),
        })
    }

    pub fn unchanged(&self, sequence: u64) -> Result<(), i32> {
        if self.state()?.sequence != sequence {
            return Err(ffi::SQLITE_IOERR);
        }
        Ok(())
    }
}

fn verify_write(file: &mut impl MainFile, offset: u64, expected: &[u8]) -> Result<(), i32> {
    for (index, bytes) in expected.chunks(65536).enumerate() {
        let mut actual = vec![0; bytes.len()];
        file.read(offset + index as u64 * 65536, &mut actual)?;
        if actual != bytes {
            return Err(ffi::SQLITE_IOERR_DATA);
        }
    }
    Ok(())
}

fn truncate_verified(file: &mut impl MainFile, length: u64) -> Result<(), i32> {
    file.truncate(length)?;
    if file.size()? != length {
        return Err(ffi::SQLITE_IOERR_TRUNCATE);
    }
    Ok(())
}
