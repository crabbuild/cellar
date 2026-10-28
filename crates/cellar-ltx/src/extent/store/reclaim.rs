// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::inventory::{object_name, root_name};
use super::*;

impl NodeExtentStore {
    /// Retain an already sealed image without I/O or waiting for a physical
    /// mutation. The live exact-owner capability pins its root throughout GC;
    /// native control loops can therefore publish retention using metadata only.
    pub fn retain_image_reference(
        &self,
        key: [u8; 32],
        revision: u64,
        image: &SealedImageRoot,
    ) -> Result<(), StoreError> {
        if !self.owns_image(image) || key == [0; 32] || revision == 0 {
            return Err(StoreError::Layout);
        }
        let reference = image.reference();
        let mut state = self.inner.state()?;
        if state.quarantined {
            return Err(StoreError::Quarantined);
        }
        let root = state
            .roots
            .get(&reference.root)
            .ok_or(StoreError::Missing)?;
        if root.bytes != u64::from(reference.length)
            || !Arc::ptr_eq(&root.token, &image.view()._token)
        {
            return Err(StoreError::Integrity);
        }
        if let Some(before) = state.references.get(&key) {
            if revision < before.revision {
                return Ok(());
            }
            if revision == before.revision {
                return if before.reference == reference && before.active {
                    Ok(())
                } else {
                    Err(StoreError::Integrity)
                };
            }
        } else if state.references.len() >= self.inner.limits.max_references {
            return Err(StoreError::Limit);
        }
        state.references.insert(
            key,
            ReferenceEntry {
                revision,
                reference,
                active: true,
            },
        );
        Ok(())
    }

    /// The host supplies the complete authoritative marker/archive reference
    /// set. Validate every selected root before changing any retention flags.
    pub fn retain_only(&self, references: &[ExtentRootReference]) -> Result<(), StoreError> {
        let operation = self.begin()?;
        let result = (|| {
            if references.len() > self.inner.limits.max_roots {
                return Err(StoreError::Limit);
            }
            let mut selected = BTreeSet::new();
            for reference in references {
                self.load_root(*reference)?;
                selected.insert(reference.root);
            }
            let mut state = self.inner.state()?;
            for (root, entry) in &mut state.roots {
                entry.retained = selected.contains(root);
            }
            state.references_reconciled = true;
            Ok(())
        })();
        operation.finish()?;
        result
    }

    pub fn retire_root(&self, reference: ExtentRootReference) -> Result<(), StoreError> {
        let operation = self.begin()?;
        let result = (|| {
            let mut state = self.inner.state()?;
            let entry = state
                .roots
                .get_mut(&reference.root)
                .ok_or(StoreError::Missing)?;
            if entry.bytes != u64::from(reference.length) {
                return Err(StoreError::Integrity);
            }
            entry.retained = false;
            Ok(())
        })();
        operation.finish()?;
        result
    }

    /// Delete at most `limit` files. Remove retired, unpinned root metadata and
    /// sync its deletion before considering unreferenced objects. Closed roots
    /// remain in the reachability set; cache/view closure never frees disk.
    pub fn reclaim(&self, limit: usize) -> Result<usize, StoreError> {
        if limit == 0 {
            return Err(StoreError::Limit);
        }
        let operation = self.begin()?;
        let result = self.reclaim_inner(limit);
        operation.finish()?;
        result
    }

    fn reclaim_inner(&self, limit: usize) -> Result<usize, StoreError> {
        if !self.inner.state()?.references_reconciled {
            return Err(StoreError::Unreconciled);
        }
        let roots = {
            let state = self.inner.state()?;
            state
                .roots
                .iter()
                .filter(|(root, entry)| {
                    !entry.retained
                        && Arc::strong_count(&entry.token) == 1
                        && !state
                            .references
                            .values()
                            .any(|reference| reference.active && reference.reference.root == **root)
                })
                .map(|(root, _)| *root)
                .take(limit)
                .collect::<Vec<_>>()
        };
        if !roots.is_empty() {
            let result = (|| {
                for root in &roots {
                    self.inner
                        .filesystem
                        .remove_file(&self.inner.directory.join(root_name(*root)))?;
                }
                self.inner.filesystem.sync_all(&self.inner.directory)
            })();
            if let Err(error) = result {
                self.inner.quarantine();
                return Err(error.into());
            }
            let mut state = self.inner.state()?;
            for root in &roots {
                let entry = state.roots.remove(root).ok_or(StoreError::Missing)?;
                state.bytes -= entry.bytes;
                state.root_extents -= entry.slots;
            }
        }
        if roots.len() == limit {
            return Ok(roots.len());
        }
        let references = self
            .inner
            .state()?
            .roots
            .iter()
            .map(|(root, entry)| ExtentRootReference::new(*root, entry.bytes as u32))
            .collect::<Result<Vec<_>, _>>()?;
        let mut live = BTreeSet::new();
        for reference in references {
            // Metadata-only traversal, one bounded manifest at a time. Do not
            // load every root tree into the persistent in-memory cache for GC.
            let data = self.read_root_file(reference)?;
            live.extend(data.objects.iter().copied());
            if live.len() > self.inner.limits.max_root_extents {
                return Err(StoreError::Limit);
            }
        }
        let objects = self
            .inner
            .state()?
            .objects
            .keys()
            .filter(|id| !live.contains(id))
            .copied()
            .take(limit - roots.len())
            .collect::<Vec<_>>();
        if !objects.is_empty() {
            let result = (|| {
                for id in &objects {
                    self.inner
                        .filesystem
                        .remove_file(&self.inner.directory.join(object_name(*id)))?;
                }
                self.inner.filesystem.sync_all(&self.inner.directory)
            })();
            if let Err(error) = result {
                self.inner.quarantine();
                return Err(error.into());
            }
            let mut state = self.inner.state()?;
            for id in &objects {
                let entry = state.objects.remove(id).ok_or(StoreError::Missing)?;
                state.bytes -= entry.bytes;
            }
        }
        Ok(roots.len() + objects.len())
    }

    /// Rebuilt from authenticated native markers after opening the node owner.
    /// Scope-derived keys must be unique across groups and reference roles.
    /// Stale/equal revisions cannot reinterpret or resurrect a released key.
    pub fn retain_reference(
        &self,
        key: [u8; 32],
        revision: u64,
        reference: ExtentRootReference,
    ) -> Result<(), StoreError> {
        let operation = self.begin()?;
        let result = (|| {
            if key == [0; 32] || revision == 0 {
                return Err(StoreError::Layout);
            }
            {
                let state = self.inner.state()?;
                if let Some(before) = state.references.get(&key) {
                    if revision < before.revision {
                        return Ok(());
                    }
                    if revision == before.revision {
                        return if before.reference == reference && before.active {
                            Ok(())
                        } else {
                            Err(StoreError::Integrity)
                        };
                    }
                } else if state.references.len() >= self.inner.limits.max_references {
                    return Err(StoreError::Limit);
                }
            }
            self.load_root(reference)?;
            self.inner.state()?.references.insert(
                key,
                ReferenceEntry {
                    revision,
                    reference,
                    active: true,
                },
            );
            Ok(())
        })();
        operation.finish()?;
        result
    }

    pub fn release_reference(&self, key: [u8; 32], revision: u64) -> Result<(), StoreError> {
        if key == [0; 32] || revision == 0 {
            return Err(StoreError::Layout);
        }
        // Releasing metadata cannot add an unpinned root to GC's live set.
        // It needs no physical-operation admission or filesystem work.
        let mut state = self.inner.state()?;
        let entry = state.references.get_mut(&key).ok_or(StoreError::Missing)?;
        if revision < entry.revision {
            return Ok(());
        }
        if revision != entry.revision {
            return Err(StoreError::Integrity);
        }
        entry.active = false;
        Ok(())
    }
}
