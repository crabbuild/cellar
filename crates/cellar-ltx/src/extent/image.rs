// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Verified demand reads over an immutable extent manifest. Cached anonymous
//! files are disposable bytes, never durable inventory or admission authority.

use super::{
    fetch::{Access, Fetch},
    file::{ExtentDownload, ExtentFile},
    store::RootView,
    tree::{ExtentId, ExtentManifest, EXTENT_BYTES},
};
use crate::{
    generation_vfs::{self, ImmutableDatabase, ImmutableImage, ImmutableReader, ReadServices},
    stream_snapshot::{Error, ScratchPool, MAX_IMAGE_BYTES},
    FileSystem,
};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
};

/// Runs on the caller's owning blocking domain. The source retains every
/// dependency of the selected manifest for the image's lifetime. It must append
/// the exact requested content in <=64 KiB chunks; it cannot bypass the image's
/// scratch admission, destination verification or sync through this interface.
pub trait ExtentSource: Send + Sync {
    fn read_into(&self, id: ExtentId, download: &mut ExtentDownload) -> Result<(), Error>;
}

/// Retained cache bounds, independent of image length and the shared scratch
/// pool's physical bound. In-flight reads/downloads retain their own charges.
#[derive(Clone, Copy, Debug)]
pub struct CacheLimits {
    pub max_cached_bytes: u64,
    pub max_cached_extents: usize,
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            max_cached_bytes: 64 * 1024 * 1024,
            max_cached_extents: 16,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheUsage {
    pub cached_bytes: u64,
    pub cached_extents: usize,
}

#[derive(Default)]
struct Cache {
    files: BTreeMap<ExtentId, ExtentFile>,
    lru: VecDeque<ExtentId>,
    bytes: u64,
    fetches: BTreeMap<ExtentId, Weak<Fetch>>,
}

impl Cache {
    fn fetch(&mut self, id: ExtentId) -> Arc<Fetch> {
        // Register under the cache mutex so a completed receive cannot slip
        // between a cache miss and joining its existing physical owner.
        self.fetches.retain(|_, fetch| fetch.strong_count() != 0);
        if let Some(fetch) = self.fetches.get(&id).and_then(Weak::upgrade) {
            return fetch;
        }
        let fetch = Arc::new(Fetch::default());
        self.fetches.insert(id, Arc::downgrade(&fetch));
        fetch
    }

    fn get(&mut self, id: ExtentId) -> Option<ExtentFile> {
        let file = self.files.get(&id)?.clone();
        self.lru.retain(|candidate| *candidate != id);
        self.lru.push_back(id);
        Some(file)
    }

    /// Return owners for destruction after releasing the cache lock. Charges
    /// follow the actual files, including clones held by concurrent reads.
    fn make_room(&mut self, id: ExtentId, limits: CacheLimits) -> Result<Vec<ExtentFile>, Error> {
        let mut retired = Vec::new();
        while self.bytes + u64::from(id.length()) > limits.max_cached_bytes
            || self.files.len() >= limits.max_cached_extents
        {
            let before = self
                .lru
                .pop_front()
                .ok_or(Error::Invalid("extent cache accounting"))?;
            let file = self
                .files
                .remove(&before)
                .ok_or(Error::Invalid("extent cache inventory"))?;
            self.bytes = self
                .bytes
                .checked_sub(u64::from(before.length()))
                .ok_or(Error::Invalid("extent cache charge"))?;
            retired.push(file);
        }
        Ok(retired)
    }
}

struct Inner {
    cache: Mutex<Cache>,
    manifest: ExtentManifest,
    source: Source,
    filesystem: Arc<dyn FileSystem>,
    directory: PathBuf,
    scratch: ScratchPool,
    limits: CacheLimits,
}

enum Source {
    Download(Arc<dyn ExtentSource>),
    Local(RootView),
}

/// One immutable root and a bounded, replaceable set of verified local extents.
/// The caller must authenticate the manifest and retain its complete dependency
/// closure under the native generation/consensus gates before selecting it.
/// This value supplies data access only; it cannot publish or authorize a cell.
#[derive(Clone)]
pub struct ExtentImage {
    inner: Arc<Inner>,
}

impl ExtentImage {
    pub fn new(
        manifest: ExtentManifest,
        source: Arc<dyn ExtentSource>,
        filesystem: Arc<dyn FileSystem>,
        directory: PathBuf,
        scratch: ScratchPool,
        limits: CacheLimits,
    ) -> Result<Self, Error> {
        Self::with_source(
            manifest,
            Source::Download(source),
            filesystem,
            directory,
            scratch,
            limits,
        )
    }

    /// Only a pinned inventory view can select the local read path. Generic
    /// sources retain anonymous receives and independent destination proofs.
    pub(crate) fn from_local(
        view: RootView,
        filesystem: Arc<dyn FileSystem>,
        directory: PathBuf,
        scratch: ScratchPool,
        limits: CacheLimits,
    ) -> Result<Self, Error> {
        Self::with_source(
            view.manifest().clone(),
            Source::Local(view),
            filesystem,
            directory,
            scratch,
            limits,
        )
    }

    fn with_source(
        manifest: ExtentManifest,
        source: Source,
        filesystem: Arc<dyn FileSystem>,
        directory: PathBuf,
        scratch: ScratchPool,
        limits: CacheLimits,
    ) -> Result<Self, Error> {
        let length = manifest.geometry().length();
        if length == 0
            || limits.max_cached_bytes < length.min(EXTENT_BYTES)
            || limits.max_cached_bytes > MAX_IMAGE_BYTES
            || limits.max_cached_extents == 0
            || limits.max_cached_extents as u64 > MAX_IMAGE_BYTES.div_ceil(EXTENT_BYTES)
        {
            return Err(Error::Limit);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                cache: Mutex::default(),
                manifest,
                source,
                filesystem,
                directory,
                scratch,
                limits,
            }),
        })
    }

    pub fn manifest(&self) -> &ExtentManifest {
        &self.inner.manifest
    }

    pub fn cache_usage(&self) -> Result<CacheUsage, Error> {
        let cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| Error::Invalid("poisoned extent cache"))?;
        Ok(CacheUsage {
            cached_bytes: cache.bytes,
            cached_extents: cache.files.len(),
        })
    }

    /// Discard cache ownership without changing the selected root. Existing
    /// reads keep their files and disk charges until their own last close.
    pub fn evict_all(&self) -> Result<(), Error> {
        let before = {
            let mut cache = self
                .inner
                .cache
                .lock()
                .map_err(|_| Error::Invalid("poisoned extent cache"))?;
            // Residency eviction must not forget an active receive. Its
            // readers retain the same file and charge until their last close.
            cache.bytes = 0;
            cache.lru.clear();
            std::mem::take(&mut cache.files)
        };
        drop(before);
        Ok(())
    }

    fn extent(&self, index: u32) -> Result<ExtentFile, Error> {
        let id = self.inner.manifest.extent(index).ok_or(Error::Limit)?;
        let fetch = {
            let mut cache = self
                .inner
                .cache
                .lock()
                .map_err(|_| Error::Invalid("poisoned extent cache"))?;
            if let Some(file) = cache.get(id) {
                drop(cache);
                self.validate_at(&file, index)?;
                return Ok(file);
            }
            cache.fetch(id)
        };
        let receive = match fetch.acquire()? {
            Access::Ready(file) => {
                self.validate_at(&file, index)?;
                return Ok(file);
            }
            Access::Receive(receive) => receive,
        };
        let retired = self
            .inner
            .cache
            .lock()
            .map_err(|_| Error::Invalid("poisoned extent cache"))?
            .make_room(id, self.inner.limits)?;
        drop(retired);
        // One producer spends the physical reservation. Waiting readers share
        // its verified file even if another extent evicts its cache entry.
        // No cache/fetch mutex crosses source I/O, verification or sync.
        let file = match &self.inner.source {
            Source::Local(view) => view.cached_extent(id, &self.inner.scratch)?,
            Source::Download(source) => {
                let mut download = ExtentDownload::new(
                    self.inner.filesystem.as_ref(),
                    Some(&self.inner.directory),
                    &self.inner.scratch,
                    id,
                )?;
                source.read_into(id, &mut download)?;
                download.finish()?
            }
        };
        self.validate_at(&file, index)?;
        let (selected, retired) = {
            let mut cache = self
                .inner
                .cache
                .lock()
                .map_err(|_| Error::Invalid("poisoned extent cache"))?;
            if let Some(before) = cache.get(id) {
                (before, Vec::new())
            } else {
                let retired = cache.make_room(id, self.inner.limits)?;
                cache.bytes += u64::from(id.length());
                cache.lru.push_back(id);
                cache.files.insert(id, file.clone());
                (file.clone(), retired)
            }
        };
        drop(retired);
        receive.complete(selected.clone())?;
        Ok(selected)
    }

    fn validate_at(&self, file: &ExtentFile, index: u32) -> Result<(), Error> {
        let geometry = self.inner.manifest.geometry();
        if index == 0 {
            crate::apply::validate_sqlite_geometry(
                &file.chunk(0, 100)?,
                geometry.page_size(),
                geometry.length(),
            )
            .map_err(|_| Error::Invalid("extent SQLite geometry"))?;
        }
        let lock = (u64::from(crate::ltx::lock_pgno(geometry.page_size())) - 1)
            * u64::from(geometry.page_size());
        let start = u64::from(index) * EXTENT_BYTES;
        if lock >= start
            && lock < start + u64::from(file.identity().length())
            && file
                .chunk(lock - start, geometry.page_size() as usize)?
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(Error::Invalid("noncanonical extent lock page"));
        }
        Ok(())
    }

    pub fn read_exact_at(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        if length == 0
            || length > 65536
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > self.inner.manifest.geometry().length())
        {
            return Err(Error::Limit);
        }
        let mut output = Vec::with_capacity(length);
        while output.len() < length {
            let position = offset + output.len() as u64;
            let index = (position / EXTENT_BYTES) as u32;
            let within = position % EXTENT_BYTES;
            let count = (length - output.len()).min((EXTENT_BYTES - within) as usize);
            output.extend_from_slice(&self.extent(index)?.chunk(within, count)?);
        }
        Ok(output)
    }
}

impl ImmutableImage for ExtentImage {
    fn length(&self) -> u64 {
        self.inner.manifest.geometry().length()
    }

    fn open_reader(&self) -> io::Result<Box<dyn ImmutableReader>> {
        Ok(Box::new(self.clone()))
    }
}

impl ImmutableReader for ExtentImage {
    fn length(&self) -> u64 {
        self.inner.manifest.geometry().length()
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        ExtentImage::read_exact_at(self, offset, length).map_err(io::Error::other)
    }
}

/// Open actual SQLite over verified on-demand extents. Run the connection on
/// its owning SQL/blocking domain; this does not select a native serving root.
pub fn open_extent_image(
    image: ExtentImage,
    services: ReadServices,
) -> rusqlite::Result<ImmutableDatabase> {
    generation_vfs::open_image(Arc::new(image), services)
}
