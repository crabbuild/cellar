// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inventory exact framed locations without scanning extent bodies. A torn tail
//! stays charged and is never reused. Corrupt complete framing refuses opening.
use super::*;

impl Segments {
    pub(in super::super) fn recover(
        &self,
        directory: &Path,
        filesystem: &dyn FileSystem,
        inventory: &mut super::super::State,
        limits: NodeExtentLimits,
        physical: (u64, u64),
    ) -> Result<(), StoreError> {
        let (id, length) = physical;
        let mut state = self.state()?;
        if length > 64 * 1024 * 1024 {
            return Err(StoreError::Layout);
        }
        if id == 0 || state.files.len() >= limits.max_objects.saturating_add(limits.max_roots) {
            return Err(StoreError::Limit);
        }
        let next_bytes = inventory
            .bytes
            .checked_add(length)
            .ok_or(StoreError::Limit)?;
        if next_bytes > limits.max_bytes {
            return Err(StoreError::Limit);
        }
        let mut file = filesystem.open(&directory.join(name(id)))?;
        if file.file_len()? != length {
            return Err(StoreError::Integrity);
        }
        let mut offset = HEADER;
        if length >= HEADER {
            if file.read_exact_at(0, HEADER as usize)? != codec::segment_header(id) {
                return Err(StoreError::Integrity);
            }
            while offset < length {
                if length - offset < FRAME {
                    break;
                }
                let header = file.read_exact_at(offset, FRAME as usize)?;
                let (key, payload) = codec::decode(&header, offset)?;
                let location = Location {
                    segment: id,
                    offset,
                    length: payload,
                    key,
                };
                if location.end() > length {
                    break;
                }
                range::validate(&mut file, location)?;
                match key {
                    Key::Object(object) => {
                        if inventory.objects.len() >= limits.max_objects
                            || inventory.objects.contains_key(&object)
                        {
                            return Err(StoreError::Limit);
                        }
                        inventory.objects.insert(
                            object,
                            ObjectEntry {
                                bytes: payload,
                                location: Some(location),
                                publishing: false,
                                verified: false,
                                read_proof: None,
                            },
                        );
                    }
                    Key::Root(root) => {
                        let slots = (payload - 112) as usize / 32;
                        inventory.root_extents = inventory
                            .root_extents
                            .checked_add(slots)
                            .ok_or(StoreError::Limit)?;
                        if inventory.roots.len() >= limits.max_roots
                            || inventory.root_extents > limits.max_root_extents
                            || inventory.roots.contains_key(&root)
                        {
                            return Err(StoreError::Limit);
                        }
                        inventory.roots.insert(
                            root,
                            RootEntry {
                                bytes: payload,
                                location: Some(location),
                                slots,
                                retained: true,
                                metadata_persisted: false,
                                token: Arc::new(()),
                                data: None,
                            },
                        );
                    }
                }
                offset = location.end();
            }
        }
        let appendable = length >= HEADER && offset == length;
        state.files.insert(
            id,
            Segment {
                length,
                durable: 0,
                namespace_persisted: false,
                syncing: false,
                appendable,
            },
        );
        state.next = state.next.max(id);
        inventory.bytes = next_bytes;
        Ok(())
    }

    pub(in super::super) fn finish_recovery(&self) -> Result<(), StoreError> {
        let mut state = self.state()?;
        // Only the highest sequence can be the append owner; an older segment
        // must never acquire new records after a later segment was created.
        state.current = state
            .files
            .last_key_value()
            .and_then(|(id, v)| v.appendable.then_some(*id));
        Ok(())
    }
}
