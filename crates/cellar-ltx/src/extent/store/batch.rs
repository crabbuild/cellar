// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Write an admitted cohort before persistence. Sync and independently reverify
//! each owned destination, then complete its namespace proof before eligibility.
use super::*;

// Keep destination descriptors bounded independently of image/decoded limits.
const MAX_PENDING_OBJECTS: usize = 8;

struct Object {
    proof: Arc<VerifiedBlocks>,
    epoch: u64,
    file: HostFile,
    _key: KeyGuard,
}

struct Root {
    reference: ExtentRootReference,
    epoch: u64,
    file: HostFile,
    _key: KeyGuard,
}

pub(super) struct Batch<'a> {
    pub store: &'a NodeExtentStore,
    pub enabled: bool,
    objects: BTreeMap<ExtentId, Object>,
    roots: BTreeMap<ImageRoot, Root>,
}

impl<'a> Batch<'a> {
    pub(super) fn new(store: &'a NodeExtentStore) -> Self {
        Self {
            store,
            enabled: store.inner.batch_directory_barriers,
            objects: BTreeMap::new(),
            roots: BTreeMap::new(),
        }
    }

    pub(super) fn contains(&self, id: &ExtentId) -> bool {
        self.objects.contains_key(id)
    }

    pub(super) fn read(
        &self,
        id: ExtentId,
        consume: impl FnMut(u64, &[u8]) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        match self.objects.get(&id) {
            None => publication::read_object(&self.store.inner, id, consume),
            Some(object) => {
                // This cohort owns the same-object guard and the independently
                // read-verified file, but has not yet advertised its directory proof.
                let result = publication::read_object_locked(
                    &self.store.inner,
                    id,
                    consume,
                    Some(&object.proof),
                );
                if result.is_err() {
                    self.store.inner.quarantine();
                }
                result
            }
        }
    }

    pub(super) fn put(&mut self, id: ExtentId, bytes: &[u8]) -> Result<(), StoreError> {
        if self.objects.contains_key(&id) {
            let _hash = timing::span(timing::Phase::ExtentHash);
            return if ExtentId::of(bytes)? == id {
                Ok(())
            } else {
                Err(StoreError::Integrity)
            };
        }
        if self.objects.len() >= MAX_PENDING_OBJECTS {
            self.flush()?;
        }
        let key = match self.store.inner.try_key(Key::Object(id))? {
            Some(key) => key,
            None => {
                // Never hold an unpublished dependency while waiting for a
                // competing publisher's dependency: opposite ID orders could
                // otherwise deadlock. Persist/release this cohort first.
                self.flush()?;
                self.store.inner.key(Key::Object(id), false)?
            }
        };
        let store = self.store;
        let enabled = self.enabled;
        store.put_extent_bytes_locked(id, bytes, key, enabled.then_some(self))
    }

    pub(super) fn object(
        &mut self,
        id: ExtentId,
        proof: Arc<VerifiedBlocks>,
        epoch: u64,
        file: HostFile,
        key: KeyGuard,
    ) {
        self.objects.insert(
            id,
            Object {
                proof,
                epoch,
                file,
                _key: key,
            },
        );
    }

    pub(super) fn root(
        &mut self,
        reference: ExtentRootReference,
        epoch: u64,
        file: HostFile,
        key: KeyGuard,
    ) {
        self.roots.insert(
            reference.root,
            Root {
                reference,
                epoch,
                file,
                _key: key,
            },
        );
    }

    pub(super) fn flush(&mut self) -> Result<(), StoreError> {
        let result = self.flush_inner();
        if result.is_err() {
            // A failed partial flush must not be retried by outer cleanup.
            self.store.inner.quarantine();
        }
        result
    }

    fn flush_inner(&mut self) -> Result<(), StoreError> {
        let required = self
            .objects
            .values()
            .map(|v| v.epoch)
            .chain(self.roots.values().map(|v| v.epoch))
            .max();
        let Some(required) = required else {
            return Ok(());
        };
        if self.store.inner.state()?.quarantined {
            return Err(StoreError::Quarantined);
        }
        if self.store.inner.segments.is_some() {
            let mut locations = BTreeMap::<u64, segment::Location>::new();
            {
                let state = self.store.inner.state()?;
                for location in self
                    .objects
                    .keys()
                    .map(|id| state.objects.get(id).and_then(|v| v.location))
                    .chain(
                        self.roots
                            .keys()
                            .map(|root| state.roots.get(root).and_then(|v| v.location)),
                    )
                {
                    let location = location.ok_or(StoreError::Integrity)?;
                    locations
                        .entry(location.segment)
                        .and_modify(|v| {
                            if location.end() > v.end() {
                                *v = location;
                            }
                        })
                        .or_insert(location);
                }
            }
            for location in locations.values() {
                self.store.inner.sync_segment_inner(*location)?;
            }
        }
        // All currently available writes, including root metadata, precede
        // the first persistence barrier. Never reuse preliminary read hashes
        // as a post-sync proof: sync may itself expose a destination failure.
        for (id, object) in &mut self.objects {
            if self.store.inner.segments.is_none() {
                self.store.inner.sync_file(&mut object.file)?;
            }
            object.proof = crate::extent::file::verify_file(&mut object.file, *id)?;
        }
        for root in self.roots.values_mut() {
            if self.store.inner.segments.is_none() {
                self.store.inner.sync_file(&mut root.file)?;
            }
            publication::verify_root_owned(&mut root.file, root.reference)?;
        }
        if self.store.inner.segments.is_none() {
            self.store.inner.sync_directory_through(required)?;
        }
        let mut state = self.store.inner.state()?;
        if state.quarantined {
            return Err(StoreError::Quarantined);
        }
        for id in self.objects.keys() {
            if !state
                .objects
                .get(id)
                .is_some_and(|v| v.bytes == u64::from(id.length()))
            {
                return Err(StoreError::Integrity);
            }
        }
        for root in self.roots.values() {
            if !state
                .roots
                .get(&root.reference.root)
                .is_some_and(|v| v.bytes == u64::from(root.reference.length))
            {
                return Err(StoreError::Integrity);
            }
        }
        for (id, object) in &self.objects {
            let entry = state.objects.get_mut(id).ok_or(StoreError::Missing)?;
            entry.verified = true;
            entry.publishing = false;
            entry.read_proof = Some(object.proof.clone());
        }
        for root in self.roots.keys() {
            state
                .roots
                .get_mut(root)
                .ok_or(StoreError::Missing)?
                .metadata_persisted = true;
        }
        drop(state);
        self.objects.clear();
        self.roots.clear();
        Ok(())
    }
}

impl Drop for Batch<'_> {
    fn drop(&mut self) {
        if !self.objects.is_empty() || !self.roots.is_empty() {
            // Unfinished/failed/panicked cohorts retain every disk charge.
            // Reopening and authoritative reconciliation own recovery.
            self.store.inner.quarantine();
        }
    }
}
