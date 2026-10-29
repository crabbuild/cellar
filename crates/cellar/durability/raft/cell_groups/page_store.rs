// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exclusively owned, bounded content-addressed page files. No filesystem work
//! escapes the supplied host. Call on a blocking execution-domain worker.

use super::{
    manifest::ManifestError,
    page_wire::{digest, MAX_PAGE_BYTES},
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
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    inventory: BTreeMap<[u8; 32], u64>,
    usage: ManifestStoreUsage,
    max_pages: usize,
    max_bytes: u64,
    quarantined: bool,
    _lock: HostFileLock,
}

impl ManifestPageStore {
    pub fn open(
        directory: &Path,
        filesystem: Arc<dyn FileSystem>,
        max_pages: usize,
        max_bytes: u64,
    ) -> Result<Self, ManifestError> {
        if max_pages == 0
            || max_pages > 4_000_000
            || max_bytes == 0
            || max_bytes > 64 * 1024 * 1024 * 1024
        {
            return Err(ManifestError::Limit);
        }
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), directory)?;
        let lock = filesystem.lock_file(&directory.join("LOCK"))?;
        let mut store = Self {
            directory: directory.to_owned(),
            filesystem,
            inventory: BTreeMap::new(),
            usage: ManifestStoreUsage::default(),
            max_pages,
            max_bytes,
            quarantined: false,
            _lock: lock,
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
            if store.inventory.insert(key, metadata.len).is_some() {
                return Err(ManifestError::Invalid("duplicate page"));
            }
        }
        store.usage.pages = store.inventory.len();
        Ok(store)
    }

    pub fn usage(&self) -> ManifestStoreUsage {
        self.usage
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
        if length != *expected || length > MAX_PAGE_BYTES as u64 {
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
        self.inventory.insert(key, bytes.len() as u64);
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
