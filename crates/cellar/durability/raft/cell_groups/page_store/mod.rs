// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exclusively owned, bounded immutable page packs. Filesystem work stays on
//! the supplied host and the owning blocking execution-domain worker.

mod collection;
mod pack;

use super::{
    manifest::ManifestError,
    page_wire::{digest, MAX_PAGE_BYTES},
    retention::RootRetentions,
    PreparedGroupManifest,
};
use cellar_ltx::{FileSystem, HostFileLock};
use pack::{Pack, Pending, Record, FOOTER_BYTES, FRAME_BYTES, HEADER_BYTES};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ManifestStoreUsage {
    /// Physical encoded records, including obsolete and duplicate copies.
    pub pages: usize,
    /// Physical containers, including a conservatively charged pending write.
    pub files: usize,
    /// Complete physical allocation, including framing and pending footer space.
    /// Live records in a partly obsolete pack never earn independent credit.
    pub bytes: u64,
    pub written_pages: u64,
}

pub struct ManifestPageStore {
    pub(super) owner: Arc<PageOwner>,
    pub(super) retention: Arc<RootRetentions>,
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    inventory: BTreeMap<[u8; 32], PageEntry>,
    packs: BTreeMap<[u8; 32], Arc<Pack>>,
    usage: ManifestStoreUsage,
    max_pages: usize,
    max_bytes: u64,
    quarantined: bool,
    collection_cursor: Option<[u8; 32]>,
    collection_scan: Option<collection::Scan>,
    directory_batch: Option<Batch>,
}

pub(super) struct PageOwner {
    _lock: HostFileLock,
}

// At most three page saves per level of the bounded (32-level) AVL update.
pub(crate) const MAX_BATCH_PAGES: usize = 3 * 32;

// A bounded writer index, its sealing overlap, and reopened packs needing a barrier.
// The already charged logical-page buffer is streamed, never retained per page.
pub(crate) const DIRECTORY_BATCH_BYTES: usize = MAX_BATCH_PAGES
    * (2 * std::mem::size_of::<Record>() + std::mem::size_of::<Arc<Pack>>())
    + std::mem::size_of::<Batch>()
    + FRAME_BYTES as usize;

struct Batch {
    pending: Option<Pending>,
    synced: Vec<Arc<Pack>>,
    touched: bool,
}

// No prepared root escapes before all required barriers. An early return or
// unwind after a physical change quarantines this owner, without quota credit.
struct DirectoryBatch<'a> {
    store: &'a mut ManifestPageStore,
}

impl Drop for DirectoryBatch<'_> {
    fn drop(&mut self) {
        if self
            .store
            .directory_batch
            .take()
            .is_some_and(|batch| batch.touched)
        {
            self.store.quarantined = true;
        }
    }
}

struct PageEntry {
    pack: Arc<Pack>,
    record: Record,
    lease: Arc<()>,
    // A scan records existing leases; later pin creation marks this page live
    // before a resumed scan may relocate/delete its containing file.
    marked: AtomicBool,
}

/// Logical leases survive physical compaction and retain the directory lock.
/// No reader owns a physical path that can outlive relocation.
pub(super) struct ManifestPagePin {
    _owner: Arc<PageOwner>,
    _lease: Arc<()>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ManifestReclaimReport {
    /// Physical page records inspected, including resumed partial packs.
    pub scanned: usize,
    /// Net physical page records removed, after any admitted replacement copy.
    pub pages: usize,
    /// Actual container unlinks, rather than logical pages or net file count.
    pub files: usize,
    /// Bounded live-page relocations, including reuse of an existing destination.
    pub compactions: usize,
    /// Records physically read/relocated, bounded across the entire request.
    pub relocated_pages: usize,
    /// Copies refused because source plus destination exceed existing quotas.
    pub blocked_compactions: usize,
    /// Net physical bytes released after copy, unlink and directory barriers.
    pub bytes: u64,
}

impl ManifestPageStore {
    /// Startup and controller provisioning lock the same page inventory.
    pub(crate) fn group_pages_path(
        base: &Path,
        history: cellar_logic::cell_group::GroupHistory,
    ) -> PathBuf {
        base.join("groups")
            .join(hex::encode(history.cluster.bytes()))
            .join(hex::encode(history.namespace.bytes()))
            .join(hex::encode(history.group.bytes()))
            .join(hex::encode(history.incarnation.bytes()))
            .join("pages")
    }

    pub(crate) fn validate_limits(
        max_pages: usize,
        max_bytes: u64,
        max_retained_roots: usize,
    ) -> Result<(), ManifestError> {
        if max_pages == 0
            || max_pages > 4_000_000
            || max_bytes == 0
            || max_bytes > 64 * 1024 * 1024 * 1024
            || !(2..=4096).contains(&max_retained_roots)
        {
            return Err(ManifestError::Limit);
        }
        Ok(())
    }

    pub(super) fn require_live(&self) -> Result<(), ManifestError> {
        if self.quarantined {
            Err(ManifestError::Quarantined)
        } else {
            Ok(())
        }
    }

    pub fn open(
        directory: &Path,
        filesystem: Arc<dyn FileSystem>,
        max_pages: usize,
        max_bytes: u64,
        max_retained_roots: usize,
    ) -> Result<Self, ManifestError> {
        Self::validate_limits(max_pages, max_bytes, max_retained_roots)?;
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), directory)?;
        let lock = filesystem.lock_file(&directory.join("LOCK"))?;
        let mut store = Self {
            owner: Arc::new(PageOwner { _lock: lock }),
            retention: Arc::new(RootRetentions::new(max_retained_roots)),
            directory: directory.to_owned(),
            filesystem,
            inventory: BTreeMap::new(),
            packs: BTreeMap::new(),
            usage: ManifestStoreUsage::default(),
            max_pages,
            max_bytes,
            quarantined: false,
            collection_cursor: None,
            collection_scan: None,
            directory_batch: None,
        };
        let mut pending = 0;
        for entry in store.filesystem.read_dir_iter(directory)? {
            let entry = entry?;
            let name = entry
                .file_name
                .to_str()
                .ok_or(ManifestError::Invalid("page pack name"))?;
            if name == "LOCK" {
                continue;
            }
            let (key, unfinished) = Self::file_key(name)?;
            let path = directory.join(name);
            let metadata = store.filesystem.symlink_metadata(&path)?;
            if !metadata.is_file || metadata.len > pack::MAX_PACK_BYTES {
                return Err(ManifestError::Invalid("page pack file"));
            }
            if unfinished {
                // A root cannot publish before final rename + directory sync.
                // Interrupted updates/copies therefore leave no selected pending
                // file. The exclusive owner may discard it, even if incomplete.
                pending += 1;
                if pending > max_pages {
                    return Err(ManifestError::Limit);
                }
                store.filesystem.remove_file(&path)?;
                continue;
            }
            let opened = Arc::new(pack::open(
                key,
                path.clone(),
                metadata.len,
                &mut store.filesystem.open(&path)?,
            )?);
            store.charge(opened.records.len(), opened.length, 1)?;
            for record in opened.records.iter() {
                // Crash between copy publication and old unlink can retain both
                // containers. Charge both; choose a deterministic logical index.
                let entry = store
                    .inventory
                    .entry(record.key)
                    .or_insert_with(|| PageEntry {
                        pack: opened.clone(),
                        record: *record,
                        lease: Arc::new(()),
                        marked: AtomicBool::new(false),
                    });
                if opened.id < entry.pack.id {
                    entry.pack = opened.clone();
                    entry.record = *record;
                }
            }
            if store.packs.insert(key, opened).is_some() {
                return Err(ManifestError::Invalid("duplicate page pack"));
            }
        }
        if pending != 0 {
            store.filesystem.sync_all(directory)?;
        }
        Ok(store)
    }

    fn file_key(name: &str) -> Result<([u8; 32], bool), ManifestError> {
        let (hex, unfinished) = if let Some(hex) = name.strip_suffix(".pack") {
            (hex, false)
        } else if let Some(hex) = name.strip_suffix(".pending") {
            (hex, true)
        } else {
            // Local marker-1 hard cutover: no legacy .page reader.
            return Err(ManifestError::Invalid("page pack name"));
        };
        if hex.len() != 64
            || !hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(ManifestError::Invalid("page pack name"));
        }
        let mut key = [0; 32];
        hex::decode_to_slice(hex, &mut key)
            .map_err(|_| ManifestError::Invalid("page pack name"))?;
        Ok((key, unfinished))
    }

    fn capacity(&self, pages: usize, bytes: u64) -> Result<(), ManifestError> {
        if self
            .usage
            .pages
            .checked_add(pages)
            .is_none_or(|n| n > self.max_pages)
            || self
                .usage
                .bytes
                .checked_add(bytes)
                .is_none_or(|n| n > self.max_bytes)
            || self.usage.written_pages.checked_add(pages as u64).is_none()
        {
            return Err(ManifestError::Limit);
        }
        Ok(())
    }

    fn charge(&mut self, pages: usize, bytes: u64, files: usize) -> Result<(), ManifestError> {
        self.capacity(pages, bytes)?;
        self.usage.pages += pages;
        self.usage.bytes += bytes;
        self.usage.files += files;
        Ok(())
    }

    /// Stream all changed AVL pages into one immutable file. The tree's logical
    /// root is returned only after the file and final directory barriers.
    pub(super) fn with_directory_batch<T>(
        &mut self,
        update: impl FnOnce(&mut Self) -> Result<T, ManifestError>,
    ) -> Result<T, ManifestError> {
        self.require_live()?;
        if self.directory_batch.is_some() {
            return Err(ManifestError::Invalid("nested page directory batch"));
        }
        self.directory_batch = Some(Batch {
            pending: None,
            synced: Vec::with_capacity(MAX_BATCH_PAGES),
            touched: false,
        });
        let batch = DirectoryBatch { store: self };
        let result = update(batch.store)?;
        batch.store.finish_directory_batch()?;
        Ok(result)
    }

    fn finish_directory_batch(&mut self) -> Result<(), ManifestError> {
        self.require_live()?;
        let batch = self
            .directory_batch
            .take()
            .ok_or(ManifestError::Invalid("missing page batch"))?;
        if !batch.touched {
            return Ok(());
        }
        self.quarantined = true;
        let opened = batch
            .pending
            .map(|pending| self.finish_pack(pending))
            .transpose()?;
        self.filesystem.sync_all(&self.directory)?;
        for pack in batch.synced {
            pack.persisted.store(true, Ordering::Release);
        }
        if let Some(opened) = opened {
            opened.persisted.store(true, Ordering::Release);
            for record in opened.records.iter() {
                self.inventory.insert(
                    record.key,
                    PageEntry {
                        pack: opened.clone(),
                        record: *record,
                        lease: Arc::new(()),
                        marked: AtomicBool::new(false),
                    },
                );
            }
            self.packs.insert(opened.id, opened);
        }
        self.quarantined = false;
        Ok(())
    }

    // The caller has already charged the complete file and quarantined the owner.
    // Publication does not overwrite even an unindexed unexpected destination.
    fn finish_pack(&self, pending: Pending) -> Result<Arc<Pack>, ManifestError> {
        let id = pack::identity(&pending.records, pending.data_end);
        let path = self.directory.join(format!("{}.pack", hex::encode(id)));
        match self.filesystem.symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
            Ok(_) => return Err(ManifestError::Invalid("page pack destination exists")),
        }
        let mut file = pending
            .file
            .into_inner()
            .map_err(|_| ManifestError::Quarantined)?;
        file.write_all(&pack::footer(&pending.records, pending.data_end))?;
        file.sync_all()?;
        drop(file);
        self.filesystem.rename(&pending.path, &path)?;
        Ok(Arc::new(Pack {
            id,
            path,
            length: pending.data_end + FOOTER_BYTES,
            records: pending.records.into_boxed_slice(),
            persisted: AtomicBool::new(false),
        }))
    }

    pub fn usage(&self) -> ManifestStoreUsage {
        self.usage
    }

    /// Reconcile complete authoritative root inventory before any collection.
    /// Engine-selected roots are retained automatically after preparation.
    /// Supply every additional snapshot/archive/transition root, including roots
    /// without readers. Missing roots must refuse recovery, never allow deletion.
    pub fn reconcile_retained(
        &mut self,
        roots: &[PreparedGroupManifest],
    ) -> Result<(), ManifestError> {
        self.require_live()?;
        if roots.len() > 4096 {
            return Err(ManifestError::Limit);
        }
        if roots
            .iter()
            .any(|root| !Arc::ptr_eq(&root.page_owner, &self.owner))
        {
            return Err(ManifestError::Invalid("retained root/page owner"));
        }
        self.retention
            .reconcile(roots.iter().map(|root| root.images.clone()).collect())
    }

    pub(super) fn pin(&self, key: [u8; 32]) -> Result<ManifestPagePin, ManifestError> {
        self.require_live()?;
        let entry = self
            .inventory
            .get(&key)
            .ok_or(ManifestError::Invalid("missing page"))?;
        entry.marked.store(true, Ordering::Release);
        Ok(ManifestPagePin {
            _owner: self.owner.clone(),
            _lease: entry.lease.clone(),
        })
    }

    pub(super) fn read(&self, key: [u8; 32]) -> Result<Vec<u8>, ManifestError> {
        self.require_live()?;
        if let Some(pending) = self
            .directory_batch
            .as_ref()
            .and_then(|b| b.pending.as_ref())
        {
            if let Some(record) = pending.records.iter().find(|record| record.key == key) {
                return pack::read(
                    &mut self.filesystem.open(&pending.path)?,
                    pending.data_end,
                    *record,
                );
            }
        }
        let expected = self
            .inventory
            .get(&key)
            .ok_or(ManifestError::Invalid("missing page"))?;
        self.read_pack(&expected.pack, expected.record)
    }

    fn read_pack(&self, pack: &Pack, record: Record) -> Result<Vec<u8>, ManifestError> {
        let result = (|| pack::read(&mut self.filesystem.open(&pack.path)?, pack.length, record))();
        if result.is_err() {
            pack.persisted.store(false, Ordering::Release);
        }
        result
    }

    pub(super) fn sync_page(&mut self, key: [u8; 32]) -> Result<(), ManifestError> {
        self.read(key)?;
        self.sync_checked_page(key)
    }

    fn sync_checked_page(&mut self, key: [u8; 32]) -> Result<(), ManifestError> {
        if self
            .directory_batch
            .as_ref()
            .and_then(|b| b.pending.as_ref())
            .is_some_and(|p| p.records.iter().any(|r| r.key == key))
        {
            return Ok(());
        }
        let pack = self
            .inventory
            .get(&key)
            .ok_or(ManifestError::Invalid("missing page"))?
            .pack
            .clone();
        if pack.persisted.load(Ordering::Acquire) {
            return Ok(());
        }
        if let Some(batch) = self.directory_batch.as_ref() {
            if batch.synced.iter().any(|p| Arc::ptr_eq(p, &pack)) {
                return Ok(());
            }
            if batch.synced.len() >= MAX_BATCH_PAGES {
                return Err(ManifestError::Limit);
            }
        }
        self.quarantined = true;
        if let Some(batch) = self.directory_batch.as_mut() {
            batch.touched = true;
        }
        self.filesystem.open(&pack.path)?.sync_all()?;
        if let Some(batch) = self.directory_batch.as_mut() {
            batch.synced.push(pack);
        } else {
            self.filesystem.sync_all(&self.directory)?;
            pack.persisted.store(true, Ordering::Release);
        }
        self.quarantined = false;
        Ok(())
    }

    pub(super) fn sync_directory(&mut self) -> Result<(), ManifestError> {
        self.require_live()?;
        self.quarantined = true;
        self.filesystem.sync_all(&self.directory)?;
        self.quarantined = false;
        Ok(())
    }

    pub(super) fn put(&mut self, key: [u8; 32], bytes: &[u8]) -> Result<(), ManifestError> {
        self.require_live()?;
        if bytes.is_empty() || bytes.len() > MAX_PAGE_BYTES || digest(bytes) != key {
            return Err(ManifestError::Invalid("page bytes"));
        }
        let staged = self
            .directory_batch
            .as_ref()
            .and_then(|b| b.pending.as_ref())
            .is_some_and(|p| p.records.iter().any(|r| r.key == key));
        if staged || self.inventory.contains_key(&key) {
            if self.read(key)? != bytes {
                return Err(ManifestError::Invalid("page collision"));
            }
            return self.sync_checked_page(key);
        }
        if self.directory_batch.is_none() {
            return self.with_directory_batch(|store| store.put(key, bytes));
        }
        let count = self
            .directory_batch
            .as_ref()
            .and_then(|b| b.pending.as_ref())
            .map_or(0, |p| p.records.len());
        if count >= MAX_BATCH_PAGES {
            return Err(ManifestError::Limit);
        }
        let extra = FRAME_BYTES
            + bytes.len() as u64
            + if count == 0 {
                HEADER_BYTES + FOOTER_BYTES
            } else {
                0
            };
        self.charge(1, extra, usize::from(count == 0))?;
        self.quarantined = true;
        self.directory_batch.as_mut().unwrap().touched = true;
        if count == 0 {
            let path = self.directory.join(format!("{}.pending", hex::encode(key)));
            let mut file = self.filesystem.create_new(&path)?;
            file.write_all(pack::MAGIC)?;
            self.directory_batch.as_mut().unwrap().pending = Some(Pending {
                path,
                file: Mutex::new(file),
                records: Vec::with_capacity(MAX_BATCH_PAGES),
                data_end: HEADER_BYTES,
            });
        }
        let pending = self
            .directory_batch
            .as_mut()
            .unwrap()
            .pending
            .as_mut()
            .unwrap();
        let record = Record {
            key,
            offset: pending.data_end,
            length: bytes.len() as u32,
        };
        let file = pending
            .file
            .get_mut()
            .map_err(|_| ManifestError::Quarantined)?;
        file.write_all(&record.header())?;
        file.write_all(bytes)?;
        pending.data_end = record.end();
        pending.records.push(record);
        self.usage.written_pages += 1;
        self.quarantined = false;
        Ok(())
    }
}
