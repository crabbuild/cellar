// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exclusively owned, bounded content-addressed page files. No filesystem work
//! escapes the supplied host. Call on a blocking execution-domain worker.

use super::{
    manifest::ManifestError,
    page_wire::{digest, MAX_PAGE_BYTES},
    retention::RootRetentions,
    PreparedGroupManifest,
};
use cellar_ltx::{FileSystem, HostFileLock};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ManifestStoreUsage {
    pub pages: usize,
    pub bytes: u64,
    pub written_pages: u64,
}

pub struct ManifestPageStore {
    pub(super) owner: Arc<PageOwner>,
    pub(super) retention: Arc<RootRetentions>,
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    inventory: BTreeMap<[u8; 32], PageEntry>,
    usage: ManifestStoreUsage,
    max_pages: usize,
    max_bytes: u64,
    quarantined: bool,
    collection_cursor: Option<[u8; 32]>,
}

pub(super) struct PageOwner {
    _lock: HostFileLock,
}

struct PageEntry {
    length: u64,
    lease: Arc<()>,
}

/// A page lease also keeps its directory locked. A replacement store cannot
/// collect pages still selected by readers from a previous owner.
pub(super) struct ManifestPagePin {
    _owner: Arc<PageOwner>,
    _lease: Arc<()>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ManifestReclaimReport {
    pub scanned: usize,
    pub pages: usize,
    pub bytes: u64,
}

impl ManifestPageStore {
    pub(super) fn require_live(&self) -> Result<(), ManifestError> {
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
        Ok(())
    }

    pub fn open(
        directory: &Path,
        filesystem: Arc<dyn FileSystem>,
        max_pages: usize,
        max_bytes: u64,
        max_retained_roots: usize,
    ) -> Result<Self, ManifestError> {
        if max_pages == 0
            || max_pages > 4_000_000
            || max_bytes == 0
            || max_bytes > 64 * 1024 * 1024 * 1024
            || !(2..=4096).contains(&max_retained_roots)
        {
            return Err(ManifestError::Limit);
        }
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), directory)?;
        let lock = filesystem.lock_file(&directory.join("LOCK"))?;
        let mut store = Self {
            owner: Arc::new(PageOwner { _lock: lock }),
            retention: Arc::new(RootRetentions::new(max_retained_roots)),
            directory: directory.to_owned(),
            filesystem,
            inventory: BTreeMap::new(),
            usage: ManifestStoreUsage::default(),
            max_pages,
            max_bytes,
            quarantined: false,
            collection_cursor: None,
        };
        for entry in store.filesystem.read_dir_iter(directory)? {
            let entry = entry?;
            let name = entry
                .file_name
                .to_str()
                .ok_or(ManifestError::Invalid("page name"))?;
            if name == "LOCK" {
                continue;
            }
            if store.inventory.len() >= max_pages {
                return Err(ManifestError::Limit);
            }
            let hex = name
                .strip_suffix(".page")
                .filter(|s| {
                    s.len() == 64
                        && s.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
                .ok_or(ManifestError::Invalid("page name"))?;
            let mut key = [0; 32];
            hex::decode_to_slice(hex, &mut key).map_err(|_| ManifestError::Invalid("page name"))?;
            // Use our own joined basename, never a provider's arbitrary path.
            let metadata = store.filesystem.symlink_metadata(&directory.join(name))?;
            if !metadata.is_file || metadata.len > MAX_PAGE_BYTES as u64 {
                return Err(ManifestError::Invalid("page file"));
            }
            store.usage.bytes = store
                .usage
                .bytes
                .checked_add(metadata.len)
                .ok_or(ManifestError::Limit)?;
            if store.usage.bytes > max_bytes {
                return Err(ManifestError::Limit);
            }
            if store
                .inventory
                .insert(
                    key,
                    PageEntry {
                        length: metadata.len,
                        lease: Arc::new(()),
                    },
                )
                .is_some()
            {
                return Err(ManifestError::Invalid("duplicate page"));
            }
        }
        store.usage.pages = store.inventory.len();
        Ok(store)
    }

    pub fn usage(&self) -> ManifestStoreUsage {
        self.usage
    }

    /// Reconcile this directory's complete authoritative root inventory before
    /// collection. First prepare every engine-selected root through
    /// `prepare_complete`; the engine retains those roots automatically. Supply
    /// all additional snapshot/archive/transition roots here, even without live
    /// readers. Calling again replaces only that additional retained set.
    /// Missing roots must refuse recovery, never be omitted to permit collection.
    pub fn reconcile_retained(
        &mut self,
        roots: &[PreparedGroupManifest],
    ) -> Result<(), ManifestError> {
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
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

    /// Scan a bounded inventory window and remove unpinned orphan pages. Root
    /// preparation requires this store's exclusive mutable borrow, so no lease
    /// can appear for a selected page during deletion. Quotas are refunded only
    /// after the directory barrier; any uncertain failure quarantines the owner.
    pub fn reclaim(&mut self, scan_limit: usize) -> Result<ManifestReclaimReport, ManifestError> {
        use std::ops::Bound::{Excluded, Unbounded};
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
        if !(1..=4096).contains(&scan_limit) {
            return Err(ManifestError::Limit);
        }
        self.retention.can_reclaim()?;
        let start = self.collection_cursor.map_or(Unbounded, Excluded);
        let mut report = ManifestReclaimReport::default();
        let mut last = None;
        let mut selected = Vec::new();
        for (key, entry) in self.inventory.range((start, Unbounded)).take(scan_limit) {
            report.scanned += 1;
            last = Some(*key);
            if Arc::strong_count(&entry.lease) == 1 {
                selected.push(*key);
                report.bytes += entry.length;
            }
        }
        if !selected.is_empty() {
            self.quarantined = true;
            for key in &selected {
                self.filesystem
                    .remove_file(&self.directory.join(format!("{}.page", hex::encode(key))))?;
            }
            self.filesystem.sync_all(&self.directory)?;
            for key in &selected {
                self.inventory.remove(key);
            }
            report.pages = selected.len();
            self.usage.pages -= report.pages;
            self.usage.bytes -= report.bytes;
            self.quarantined = false;
        }
        self.collection_cursor = if report.scanned < scan_limit {
            None
        } else {
            last
        };
        Ok(report)
    }

    pub(super) fn pin(&self, key: [u8; 32]) -> Result<ManifestPagePin, ManifestError> {
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
        let entry = self
            .inventory
            .get(&key)
            .ok_or(ManifestError::Invalid("missing page"))?;
        Ok(ManifestPagePin {
            _owner: self.owner.clone(),
            _lease: entry.lease.clone(),
        })
    }

    pub(super) fn read(&self, key: [u8; 32]) -> Result<Vec<u8>, ManifestError> {
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
        let expected = self
            .inventory
            .get(&key)
            .ok_or(ManifestError::Invalid("missing page"))?;
        let path = self.directory.join(format!("{}.page", hex::encode(key)));
        let mut file = self.filesystem.open(&path)?;
        let length = file.file_len()?;
        if length != expected.length || length > MAX_PAGE_BYTES as u64 {
            return Err(ManifestError::Invalid("page length"));
        }
        let bytes = file.read_exact_at(0, length as usize)?;
        if digest(&bytes) != key {
            return Err(ManifestError::Invalid("page digest"));
        }
        Ok(bytes)
    }

    pub(super) fn sync_page(&mut self, key: [u8; 32]) -> Result<(), ManifestError> {
        self.read(key)?;
        self.quarantined = true;
        self.filesystem
            .open(&self.directory.join(format!("{}.page", hex::encode(key))))?
            .sync_all()?;
        self.quarantined = false;
        Ok(())
    }

    pub(super) fn sync_directory(&mut self) -> Result<(), ManifestError> {
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
        self.quarantined = true;
        self.filesystem.sync_all(&self.directory)?;
        self.quarantined = false;
        Ok(())
    }

    pub(super) fn put(&mut self, key: [u8; 32], bytes: &[u8]) -> Result<(), ManifestError> {
        if self.quarantined {
            return Err(ManifestError::Quarantined);
        }
        if bytes.len() > MAX_PAGE_BYTES || digest(bytes) != key {
            return Err(ManifestError::Invalid("page bytes"));
        }
        let path = self.directory.join(format!("{}.page", hex::encode(key)));
        if self.inventory.contains_key(&key) {
            if self.read(key)? != bytes {
                return Err(ManifestError::Invalid("page collision"));
            }
            // Restart inventory is not proof of a prior completed fsync.
            self.quarantined = true;
            self.filesystem.open(&path)?.sync_all()?;
            self.filesystem.sync_all(&self.directory)?;
            self.quarantined = false;
            return Ok(());
        }
        let next_bytes = self
            .usage
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or(ManifestError::Limit)?;
        if self.inventory.len() >= self.max_pages || next_bytes > self.max_bytes {
            return Err(ManifestError::Limit);
        }
        // Any uncertain write/sync leaves the owner quarantined. No refunds or
        // deletion before a new owner reconstructs authoritative root references.
        self.quarantined = true;
        let mut file = self.filesystem.create_new(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        self.filesystem.sync_all(&self.directory)?;
        self.inventory.insert(
            key,
            PageEntry {
                length: bytes.len() as u64,
                lease: Arc::new(()),
            },
        );
        self.usage.pages += 1;
        self.usage.bytes = next_bytes;
        self.usage.written_pages = self
            .usage
            .written_pages
            .checked_add(1)
            .ok_or(ManifestError::Limit)?;
        self.quarantined = false;
        Ok(())
    }
}
