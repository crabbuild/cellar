// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::inventory::{object_name, root_name};
use super::*;
use sha2::Digest;

pub(super) fn read_object(
    inner: &Inner,
    id: ExtentId,
    mut consume: impl FnMut(u64, &[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    let result = (|| {
        let verified = {
            let state = inner.state()?;
            let entry = state.objects.get(&id).ok_or(StoreError::Missing)?;
            if entry.bytes != u64::from(id.length()) {
                return Err(StoreError::Integrity);
            }
            entry.verified
        };
        let mut file = inner.checked_file(
            &inner.directory.join(object_name(id)),
            u64::from(id.length()),
        )?;
        let mut hash = id.stream_hasher();
        let mut offset = 0;
        while offset < u64::from(id.length()) {
            let count = (u64::from(id.length()) - offset).min(65536) as usize;
            let bytes = file.read_exact_at(offset, count)?;
            if bytes.len() != count {
                return Err(StoreError::Integrity);
            }
            hash.update(&bytes);
            consume(offset, &bytes)?;
            offset += count as u64;
        }
        if <[u8; 32]>::from(hash.finalize()) != id.bytes()
            || file.file_len()? != u64::from(id.length())
        {
            return Err(StoreError::Integrity);
        }
        if !verified {
            file.sync_all()?;
            inner.filesystem.sync_all(&inner.directory)?;
        }
        inner
            .state()?
            .objects
            .get_mut(&id)
            .ok_or(StoreError::Missing)?
            .verified = true;
        Ok(())
    })();
    if result.is_err() {
        if let Some(entry) = inner.state()?.objects.get_mut(&id) {
            entry.verified = false;
        }
    }
    result
}

impl NodeExtentStore {
    /// Borrow an already admitted complete object without copying source chunks.
    /// Native providers write one at-most-4-MiB window; other providers retain
    /// bounded positional writes. Independent destination verification and both
    /// persistence barriers still precede reuse. Existing local bytes are
    /// independently verified rather than overwritten with the supplied buffer.
    pub fn put_extent_bytes(&self, id: ExtentId, bytes: &[u8]) -> Result<(), StoreError> {
        let operation = self.begin()?;
        let result = self.put_extent_bytes_inner(id, bytes);
        operation.finish()?;
        result
    }

    pub(super) fn put_extent_bytes_inner(
        &self,
        id: ExtentId,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        if bytes.len() != id.length() as usize {
            return Err(StoreError::Integrity);
        }
        self.put_extent_with(id, |file| {
            if ExtentId::of(bytes)? != id {
                return Err(StoreError::Integrity);
            }
            // The caller already owns this bounded input. Avoid a second
            // extent allocation and preserve injected providers' fallback.
            file.write_all_at_window(0, bytes)?;
            Ok(())
        })
    }

    /// Import one immutable object. Admission precedes creation/source reads;
    /// source identity, independent destination verification and both persistence
    /// barriers precede reuse. Existing names are verified, never overwritten.
    pub fn put_extent(
        &self,
        id: ExtentId,
        mut read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
    ) -> Result<(), StoreError> {
        let operation = self.begin()?;
        let result = self.put_extent_inner(id, &mut read);
        operation.finish()?;
        result
    }

    pub(super) fn put_extent_inner(
        &self,
        id: ExtentId,
        read: &mut impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
    ) -> Result<(), StoreError> {
        self.put_extent_with(id, |file| {
            let mut hash = id.stream_hasher();
            let mut offset = 0;
            while offset < u64::from(id.length()) {
                let count = (u64::from(id.length()) - offset).min(65536) as usize;
                let bytes = read(offset, count)?;
                if bytes.len() != count {
                    return Err(StoreError::Integrity);
                }
                hash.update(&bytes);
                file.write_all_at(offset, &bytes)?;
                offset += count as u64;
            }
            if <[u8; 32]>::from(hash.finalize()) != id.bytes() {
                return Err(StoreError::Integrity);
            }
            Ok(())
        })
    }

    fn put_extent_with(
        &self,
        id: ExtentId,
        write: impl FnOnce(&mut HostFile) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let exists = self.inner.state()?.objects.contains_key(&id);
        if exists {
            return read_object(&self.inner, id, |_, _| Ok(()));
        }
        {
            let mut state = self.inner.state()?;
            let bytes = state
                .bytes
                .checked_add(u64::from(id.length()))
                .ok_or(StoreError::Limit)?;
            if bytes > self.inner.limits.max_bytes
                || state.objects.len() >= self.inner.limits.max_objects
            {
                return Err(StoreError::Limit);
            }
            state.bytes = bytes;
            state.objects.insert(
                id,
                ObjectEntry {
                    bytes: u64::from(id.length()),
                    verified: false,
                },
            );
        }
        let path = self.inner.directory.join(object_name(id));
        let mut file = match self.inner.filesystem.create_read_write(&path) {
            Ok(file) => file,
            Err(error) => {
                self.inner.quarantine();
                return Err(error.into());
            }
        };
        let result = (|| {
            if file.file_len()? != 0 {
                return Err(StoreError::Integrity);
            }
            write(&mut file)?;
            file.sync_all()?;
            crate::extent::file::verify_file(&mut file, id)?;
            self.inner.filesystem.sync_all(&self.inner.directory)?;
            Ok(())
        })();
        drop(file);
        if let Err(error) = result {
            let error = self.cleanup_created(&path, error)?;
            let mut state = self.inner.state()?;
            let entry = state.objects.remove(&id).ok_or(StoreError::Missing)?;
            state.bytes -= entry.bytes;
            return Err(error);
        }
        self.inner
            .state()?
            .objects
            .get_mut(&id)
            .ok_or(StoreError::Missing)?
            .verified = true;
        Ok(())
    }

    /// Re-establish one local object's proof after reopening; no source caller
    /// can promote an inventory filename directly into a local closure seal.
    pub fn verify_extent(&self, id: ExtentId) -> Result<(), StoreError> {
        let operation = self.begin()?;
        let result = read_object(&self.inner, id, |_, _| Ok(()));
        operation.finish()?;
        result
    }

    pub(super) fn cleanup_created(
        &self,
        path: &Path,
        error: StoreError,
    ) -> Result<StoreError, StoreError> {
        if let Err(cleanup) = self
            .inner
            .filesystem
            .remove_file(path)
            .and_then(|_| self.inner.filesystem.sync_all(&self.inner.directory))
        {
            self.inner.quarantine();
            return Err(StoreError::Cleanup {
                operation: Box::new(error),
                cleanup,
            });
        }
        Ok(error)
    }

    /// Persist authenticated metadata without claiming that missing data is
    /// durable locally. Use the returned view for exact-root reads only.
    pub fn stage_root(&self, manifest: ExtentManifest) -> Result<RootView, StoreError> {
        let operation = self.begin()?;
        let result = self.stage_inner(manifest);
        operation.finish()?;
        result
    }

    /// Pin an authenticated receive closure before importing its objects. A
    /// newly created root is reclaimable after its view closes unless native
    /// marker retention subsequently selects it. Existing retention is kept.
    pub fn stage_transient_root(&self, manifest: ExtentManifest) -> Result<RootView, StoreError> {
        let operation = self.begin()?;
        let result = (|| {
            let existed = self.inner.state()?.roots.contains_key(&manifest.root());
            let view = self.stage_inner(manifest)?;
            if !existed {
                self.inner
                    .state()?
                    .roots
                    .get_mut(&view.reference.root)
                    .ok_or(StoreError::Missing)?
                    .retained = false;
            }
            Ok(view)
        })();
        operation.finish()?;
        result
    }

    pub fn seal_root(&self, manifest: ExtentManifest) -> Result<SealedRoot, StoreError> {
        let operation = self.begin()?;
        let result = self.seal_root_inner(manifest);
        operation.finish()?;
        result
    }

    pub(super) fn seal_root_inner(
        &self,
        manifest: ExtentManifest,
    ) -> Result<SealedRoot, StoreError> {
        let state = self.inner.state()?;
        for index in 0..manifest.geometry().extent_count() {
            let id = manifest.extent(index).ok_or(StoreError::Layout)?;
            if !state
                .objects
                .get(&id)
                .is_some_and(|entry| entry.verified && entry.bytes == u64::from(id.length()))
            {
                return Err(StoreError::Unverified);
            }
        }
        drop(state);
        let view = self.stage_inner(manifest)?;
        let state = self.inner.state()?;
        if view
            .data
            .objects
            .iter()
            .any(|id| !state.objects.get(id).is_some_and(|entry| entry.verified))
        {
            return Err(StoreError::Unverified);
        }
        Ok(SealedRoot(view))
    }

    fn stage_inner(&self, manifest: ExtentManifest) -> Result<RootView, StoreError> {
        let bytes = manifest.encode()?;
        let reference = ExtentRootReference::new(manifest.root(), bytes.len() as u32)?;
        let path = self.inner.directory.join(root_name(reference.root));
        let exists = self.inner.state()?.roots.contains_key(&reference.root);
        if exists {
            let data = self.read_root_synced(reference)?;
            self.inner
                .state()?
                .roots
                .get_mut(&reference.root)
                .ok_or(StoreError::Missing)?
                .data = Some(data.clone());
            return self.view(reference, data);
        }
        let slots = manifest.geometry().extent_count() as usize;
        {
            let mut state = self.inner.state()?;
            let charged = state
                .bytes
                .checked_add(bytes.len() as u64)
                .ok_or(StoreError::Limit)?;
            let refs = state
                .root_extents
                .checked_add(slots)
                .ok_or(StoreError::Limit)?;
            if charged > self.inner.limits.max_bytes
                || state.roots.len() >= self.inner.limits.max_roots
                || refs > self.inner.limits.max_root_extents
            {
                return Err(StoreError::Limit);
            }
            state.bytes = charged;
            state.root_extents = refs;
            state.roots.insert(
                reference.root,
                RootEntry {
                    bytes: bytes.len() as u64,
                    slots,
                    retained: true,
                    token: Arc::new(()),
                    data: None,
                },
            );
        }
        let mut file = match self.inner.filesystem.create_read_write(&path) {
            Ok(file) => file,
            Err(error) => {
                self.inner.quarantine();
                return Err(error.into());
            }
        };
        let result = (|| {
            if file.file_len()? != 0 {
                return Err(StoreError::Integrity);
            }
            for (index, bytes) in bytes.chunks(65536).enumerate() {
                file.write_all_at(index as u64 * 65536, bytes)?;
            }
            file.sync_all()?;
            // Read the owned destination, not a pathname which might replace it.
            let mut observed = Vec::with_capacity(bytes.len());
            while observed.len() < bytes.len() {
                let count = (bytes.len() - observed.len()).min(65536);
                let next = file.read_exact_at(observed.len() as u64, count)?;
                if next.len() != count {
                    return Err(StoreError::Integrity);
                }
                observed.extend_from_slice(&next);
            }
            if observed != bytes || file.file_len()? != bytes.len() as u64 {
                return Err(StoreError::Integrity);
            }
            ExtentManifest::decode(&observed, reference.root)?;
            self.inner.filesystem.sync_all(&self.inner.directory)?;
            Ok(())
        })();
        drop(file);
        if let Err(error) = result {
            let error = self.cleanup_created(&path, error)?;
            let mut state = self.inner.state()?;
            let entry = state
                .roots
                .remove(&reference.root)
                .ok_or(StoreError::Missing)?;
            state.bytes -= entry.bytes;
            state.root_extents -= entry.slots;
            return Err(error);
        }
        let objects = (0..manifest.geometry().extent_count())
            .map(|index| manifest.extent(index).ok_or(StoreError::Layout))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let data = Arc::new(RootData { manifest, objects });
        self.inner
            .state()?
            .roots
            .get_mut(&reference.root)
            .ok_or(StoreError::Missing)?
            .data = Some(data.clone());
        self.view(reference, data)
    }
}
