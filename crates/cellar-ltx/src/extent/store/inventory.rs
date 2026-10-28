// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::host::create_dir_all_durable;

pub(super) fn object_name(id: ExtentId) -> String {
    format!("e-{:08x}-{}", id.length(), hex(&id.bytes()))
}

pub(super) fn root_name(root: ImageRoot) -> String {
    format!("r-{}", hex(&root.bytes()))
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn digest(text: &str) -> Result<[u8; 32], StoreError> {
    if text.len() != 64
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(StoreError::Layout);
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
            .map_err(|_| StoreError::Layout)?;
    }
    Ok(bytes)
}

impl NodeExtentStore {
    /// Inventory names and lengths only. No data extent is scanned at startup,
    /// and no filename chooses an authoritative root. Partial known files stay
    /// charged until explicit reclamation after the host supplies its references.
    pub fn open(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: NodeExtentLimits,
    ) -> Result<Self, StoreError> {
        Self::open_with_admission(directory, filesystem, limits, false)
    }

    /// Serialize physical operations from bounded blocking owners sharing one
    /// node inventory. Waiting operations retain their caller's admission and
    /// must be joined at shutdown. Never call this mode on an async executor.
    pub fn open_serialized(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: NodeExtentLimits,
    ) -> Result<Self, StoreError> {
        Self::open_with_admission(directory, filesystem, limits, true)
    }

    fn open_with_admission(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: NodeExtentLimits,
        serialized: bool,
    ) -> Result<Self, StoreError> {
        limits.validate()?;
        create_dir_all_durable(filesystem.as_ref(), &directory)?;
        let lock_path = directory.join("extents.lock");
        match filesystem.symlink_metadata(&lock_path) {
            Ok(m) if !m.is_file || m.len != 0 => return Err(StoreError::Layout),
            Ok(_) => (),
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
        let lock = filesystem.lock_file(&lock_path)?;
        filesystem.sync_all(&lock_path)?;
        filesystem.sync_all(&directory)?;
        let mut state = State::default();
        let mut workspaces = Vec::new();
        for entry in filesystem.read_dir_iter(&directory)? {
            let entry = entry?;
            if entry.file_name == "extents.lock" {
                continue;
            }
            let name = entry.file_name.to_str().ok_or(StoreError::Layout)?;
            if !name.is_ascii() {
                return Err(StoreError::Layout);
            }
            let metadata = filesystem.symlink_metadata(&directory.join(name))?;
            if name.len() == 66 && name.starts_with("w-") {
                let id = digest(&name[2..])?;
                if workspace::name(&id) != name || !entry.is_dir || !metadata.is_dir {
                    return Err(StoreError::Layout);
                }
                if workspaces.len() >= limits.max_workspaces {
                    return Err(StoreError::Limit);
                }
                workspaces.push(directory.join(name));
                continue;
            }
            if entry.is_dir || !metadata.is_file {
                return Err(StoreError::Layout);
            }
            let next_bytes = state
                .bytes
                .checked_add(metadata.len)
                .ok_or(StoreError::Limit)?;
            if next_bytes > limits.max_bytes {
                return Err(StoreError::Limit);
            }
            if name.len() == 75 && name.starts_with("e-") && &name[10..11] == "-" {
                let length =
                    u32::from_str_radix(&name[2..10], 16).map_err(|_| StoreError::Layout)?;
                let id = ExtentId::from_parts(digest(&name[11..])?, length)?;
                if object_name(id) != name || metadata.len > u64::from(id.length()) {
                    return Err(StoreError::Layout);
                }
                if state.objects.len() >= limits.max_objects {
                    return Err(StoreError::Limit);
                }
                state.objects.insert(
                    id,
                    ObjectEntry {
                        bytes: metadata.len,
                        verified: false,
                        read_proof: None,
                    },
                );
            } else if name.len() == 66 && name.starts_with("r-") {
                if metadata.len > MAX_MANIFEST_BYTES as u64 {
                    return Err(StoreError::Layout);
                }
                if state.roots.len() >= limits.max_roots {
                    return Err(StoreError::Limit);
                }
                let root = ImageRoot::from_bytes(digest(&name[2..])?);
                let slots = metadata.len.saturating_sub(112).div_ceil(32) as usize;
                state.root_extents = state
                    .root_extents
                    .checked_add(slots)
                    .ok_or(StoreError::Limit)?;
                if state.root_extents > limits.max_root_extents {
                    return Err(StoreError::Limit);
                }
                state.roots.insert(
                    root,
                    RootEntry {
                        bytes: metadata.len,
                        slots,
                        retained: true,
                        token: Arc::new(()),
                        data: None,
                    },
                );
            } else {
                return Err(StoreError::Layout);
            }
            state.bytes = next_bytes;
        }
        // Do not delete while enumerating: changing directory iteration offsets
        // could skip an immutable object and undercount the recovered inventory.
        for path in &workspaces {
            workspace::discard(filesystem.as_ref(), path)?;
        }
        if !workspaces.is_empty() {
            filesystem.sync_all(&directory)?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                directory,
                filesystem,
                limits,
                state: Mutex::new(state),
                idle: Condvar::new(),
                serialized,
                _lock: lock,
            }),
        })
    }

    pub(super) fn read_root_file(
        &self,
        reference: ExtentRootReference,
    ) -> Result<Arc<RootData>, StoreError> {
        self.read_root(reference, false)
    }

    pub(super) fn read_root_synced(
        &self,
        reference: ExtentRootReference,
    ) -> Result<Arc<RootData>, StoreError> {
        self.read_root(reference, true)
    }

    fn read_root(
        &self,
        reference: ExtentRootReference,
        persist: bool,
    ) -> Result<Arc<RootData>, StoreError> {
        let path = self.inner.directory.join(root_name(reference.root));
        let mut file = self
            .inner
            .checked_file(&path, u64::from(reference.length))?;
        let mut bytes = Vec::with_capacity(reference.length as usize);
        while bytes.len() < reference.length as usize {
            let count = (reference.length as usize - bytes.len()).min(65536);
            let next = file.read_exact_at(bytes.len() as u64, count)?;
            if next.len() != count {
                return Err(StoreError::Integrity);
            }
            bytes.extend_from_slice(&next);
        }
        if file.file_len()? != u64::from(reference.length) {
            return Err(StoreError::Integrity);
        }
        let manifest = ExtentManifest::decode(&bytes, reference.root)?;
        if persist {
            file.sync_all()?;
            self.inner.filesystem.sync_all(&self.inner.directory)?;
        }
        let mut objects = BTreeSet::new();
        for index in 0..manifest.geometry().extent_count() {
            objects.insert(manifest.extent(index).ok_or(StoreError::Layout)?);
        }
        Ok(Arc::new(RootData { manifest, objects }))
    }

    pub(super) fn load_root(
        &self,
        reference: ExtentRootReference,
    ) -> Result<Arc<RootData>, StoreError> {
        {
            let state = self.inner.state()?;
            let entry = state
                .roots
                .get(&reference.root)
                .ok_or(StoreError::Missing)?;
            if entry.bytes != u64::from(reference.length) {
                return Err(StoreError::Integrity);
            }
            if let Some(data) = &entry.data {
                return Ok(data.clone());
            }
        }
        let data = self.read_root_file(reference)?;
        self.inner
            .state()?
            .roots
            .get_mut(&reference.root)
            .ok_or(StoreError::Missing)?
            .data = Some(data.clone());
        Ok(data)
    }

    pub fn open_root(&self, reference: ExtentRootReference) -> Result<RootView, StoreError> {
        let operation = self.begin_read()?;
        let result = self
            .load_root(reference)
            .and_then(|data| self.view(reference, data));
        operation.finish()?;
        result
    }
}
