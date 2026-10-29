// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Canonical manifest plus sorted unique extent bodies. No full-image file,
//! aggregate SQLite hash, per-page index or partially durable root is created.

use super::*;
use std::sync::Mutex;

fn dependencies(manifest: &ExtentManifest) -> Result<BTreeSet<ExtentId>, SnapshotError> {
    (0..manifest.geometry().extent_count())
        .map(|index| {
            manifest
                .extent(index)
                .ok_or(SnapshotError::Invalid("extent coverage"))
        })
        .collect()
}

pub(in crate::raft::snapshot) fn transfer_length(
    manifest: &ExtentManifest,
    metadata: usize,
) -> Result<usize, SnapshotError> {
    dependencies(manifest)?
        .iter()
        .try_fold(metadata, |total, id| {
            total
                .checked_add(id.length() as usize)
                .ok_or(SnapshotError::Limit)
        })
}

struct Reader {
    next: usize,
    object: usize,
    object_offset: usize,
    bytes: Vec<u8>,
    failed: bool,
}

pub(in crate::raft::snapshot) struct RootPayload {
    source: RootSnapshotSource,
    objects: Vec<ExtentId>,
    length: usize,
    reader: Mutex<Reader>,
}

impl RootPayload {
    pub(super) fn new(source: RootSnapshotSource) -> Result<Self, SnapshotError> {
        let objects = dependencies(source.image.manifest())?.into_iter().collect();
        let length = transfer_length(source.image.manifest(), source.manifest.len())?;
        Ok(Self {
            source,
            objects,
            length,
            reader: Mutex::new(Reader {
                next: 0,
                object: 0,
                object_offset: 0,
                bytes: Vec::new(),
                failed: false,
            }),
        })
    }

    pub(in crate::raft::snapshot) fn length(&self) -> usize {
        self.length
    }

    /// Called only by the admitted blocking source owner. One verified extent
    /// is cached across wire windows; a second connection cannot rewind it.
    pub(in crate::raft::snapshot) fn chunk(
        &self,
        offset: usize,
        length: usize,
    ) -> Result<Vec<u8>, SnapshotError> {
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| SnapshotError::Invalid("root source owner"))?;
        if reader.failed
            || reader.next != offset
            || length == 0
            || offset
                .checked_add(length)
                .is_none_or(|end| end > self.length)
        {
            return Err(SnapshotError::Invalid("root source order or bound"));
        }
        let result = (|| {
            self.source.validate_owner(&self.source.group)?;
            let mut output = Vec::with_capacity(length);
            if reader.next < self.source.manifest.len() {
                let count = length.min(self.source.manifest.len() - reader.next);
                output.extend_from_slice(&self.source.manifest[reader.next..reader.next + count]);
                reader.next += count;
            }
            while output.len() < length {
                let id = *self
                    .objects
                    .get(reader.object)
                    .ok_or(SnapshotError::Limit)?;
                if reader.bytes.is_empty() {
                    // read_extent does not expose a prefix before the complete
                    // domain-separated identity has matched the root member.
                    reader.bytes = self.source.read_extent(id)?;
                }
                let count = (length - output.len()).min(reader.bytes.len() - reader.object_offset);
                output.extend_from_slice(
                    &reader.bytes[reader.object_offset..reader.object_offset + count],
                );
                reader.next += count;
                reader.object_offset += count;
                if reader.object_offset == id.length() as usize {
                    reader.bytes = Vec::new();
                    reader.object_offset = 0;
                    reader.object += 1;
                }
            }
            Ok(output)
        })();
        if result.is_err() {
            reader.failed = true;
        }
        result
    }

    pub(in crate::raft::snapshot) fn complete(&self) -> Result<(), SnapshotError> {
        let reader = self
            .reader
            .lock()
            .map_err(|_| SnapshotError::Invalid("root source owner"))?;
        if reader.failed
            || reader.next != self.length
            || reader.object != self.objects.len()
            || !reader.bytes.is_empty()
        {
            return Err(SnapshotError::Invalid("incomplete root source"));
        }
        Ok(())
    }
}

/// Registered receive workers own all imports and their admission. Only one
/// extent body is resident; canonical metadata is bounded by the 10 GB quota.
pub(in crate::raft::snapshot) struct RootDownload {
    descriptor: Descriptor,
    store: NodeExtentStore,
    received: usize,
    metadata: Vec<u8>,
    set: Option<RootSnapshotSet>,
    receive_pin: Option<cellar_ltx::internal::RootView>,
    objects: Vec<ExtentId>,
    object: usize,
    bytes: Vec<u8>,
    failed: bool,
    chunk_bytes: usize,
}

impl RootDownload {
    pub(in crate::raft::snapshot) fn new(
        descriptor: Descriptor,
        store: NodeExtentStore,
        chunk_bytes: usize,
    ) -> Self {
        Self {
            metadata: Vec::with_capacity(descriptor.encoded_length),
            descriptor,
            store,
            received: 0,
            set: None,
            receive_pin: None,
            objects: vec![],
            object: 0,
            bytes: vec![],
            failed: false,
            chunk_bytes,
        }
    }

    pub(in crate::raft::snapshot) fn append(
        &mut self,
        offset: usize,
        bytes: &[u8],
    ) -> Result<(), SnapshotError> {
        if self.failed
            || offset != self.received
            || bytes.is_empty()
            || bytes.len() > self.chunk_bytes
            || offset
                .checked_add(bytes.len())
                .is_none_or(|end| end > self.descriptor.transfer_length)
        {
            self.failed = true;
            return Err(SnapshotError::Invalid("root receive order or bound"));
        }
        let result = self.append_inner(bytes);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_inner(&mut self, mut bytes: &[u8]) -> Result<(), SnapshotError> {
        if self.received < self.descriptor.encoded_length {
            let count = bytes
                .len()
                .min(self.descriptor.encoded_length - self.received);
            self.metadata.extend_from_slice(&bytes[..count]);
            self.received += count;
            bytes = &bytes[count..];
            if self.received == self.descriptor.encoded_length {
                let set = RootSnapshotSet::bound(
                    &self.descriptor.snapshot()?,
                    self.descriptor.clone(),
                    &self.metadata,
                )?;
                self.objects = set.extents().collect();
                self.receive_pin = Some(set.pin_receive(&self.store)?);
                self.set = Some(set);
                self.metadata = Vec::new();
            }
        }
        while !bytes.is_empty() {
            let id = *self.objects.get(self.object).ok_or(SnapshotError::Limit)?;
            if self.bytes.capacity() < id.length() as usize {
                self.bytes = Vec::with_capacity(id.length() as usize);
            }
            let count = bytes.len().min(id.length() as usize - self.bytes.len());
            self.bytes.extend_from_slice(&bytes[..count]);
            self.received += count;
            bytes = &bytes[count..];
            if self.bytes.len() == id.length() as usize {
                self.set
                    .as_ref()
                    .ok_or(SnapshotError::Invalid("missing root manifest"))?
                    .install_extent(&self.store, id, &self.bytes)?;
                self.bytes.clear();
                self.object += 1;
            }
        }
        if self.received == self.descriptor.transfer_length {
            // Return the whole-object allocation before sealing allocates its
            // separate admitted workspace on this same physical owner.
            self.bytes = Vec::new();
        }
        Ok(())
    }

    pub(in crate::raft::snapshot) fn finish(
        self,
        expected: &NodeExtentStore,
    ) -> Result<SnapshotPin, SnapshotError> {
        if self.failed
            || self.received != self.descriptor.transfer_length
            || self.object != self.objects.len()
            || !self.bytes.is_empty()
            || !self.store.same_inventory(expected)
        {
            return Err(SnapshotError::Invalid("incomplete or foreign root receive"));
        }
        let set = self
            .set
            .ok_or(SnapshotError::Invalid("missing root manifest"))?;
        let image = set.seal(
            &self.store,
            &mut vec![0; cellar_ltx::internal::EXTENT_BYTES as usize],
        )?;
        Ok(SnapshotPin::Rooted {
            image: Arc::new(image),
            store: self.store,
        })
    }
}
