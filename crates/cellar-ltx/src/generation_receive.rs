// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded raw-image receive material. A finished image is still private data;
//! only the owning host can publish its complete durable state marker.

use super::*;
use crate::stream_snapshot::ScratchReservation;

pub struct GenerationReceive {
    store: GenerationStore,
    file: HostFile,
    id: ImageId,
    expected: ImageState,
    received: u64,
    failed: bool,
    _reservation: ScratchReservation,
}

impl GenerationStore {
    /// Reserve the complete anonymous spool before opening it. Filesystem work
    /// belongs on the caller's joined blocking owner. Only one image is buffered
    /// on disk; append never holds more than the supplied <=64 KiB chunk.
    pub fn receive_image(
        &self,
        id: ImageId,
        expected: ImageState,
        scratch: &crate::stream_snapshot::ScratchPool,
        directory: &Path,
    ) -> Result<GenerationReceive, GenerationError> {
        expected.validate(&self.inner.limits.apply)?;
        if expected.length == 0 || !directory.is_absolute() {
            return Err(GenerationError::Identity);
        }
        let reservation = scratch.acquire(expected.length, 1)?;
        let file = self.inner.filesystem.temporary_file(Some(directory))?;
        Ok(GenerationReceive {
            store: self.clone(),
            file,
            id,
            expected,
            received: 0,
            failed: false,
            _reservation: reservation,
        })
    }
}

impl GenerationReceive {
    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), GenerationError> {
        if self.failed {
            return Err(GenerationError::Quarantined);
        }
        self.failed = true;
        let next = offset
            .checked_add(bytes.len() as u64)
            .ok_or(GenerationError::Limit)?;
        if offset != self.received
            || bytes.is_empty()
            || bytes.len() > 64 * 1024
            || next > self.expected.length
        {
            return Err(GenerationError::Identity);
        }
        self.file.write_all_at(offset, bytes)?;
        self.received = next;
        self.failed = false;
        Ok(())
    }

    /// Independently inspect incoming bytes even if the inventory already has
    /// this image. The final immutable destination is separately verified/synced.
    /// New unselected images are reclaimable after the returned pin is released;
    /// existing host retention is preserved.
    pub fn finish(mut self) -> Result<GenerationPin, GenerationError> {
        if self.failed || self.received != self.expected.length {
            return Err(GenerationError::Identity);
        }
        apply::inspect(&mut self.file, &self.expected, None)?;
        self.file.sync_all()?;
        match self.store.pin_prepared(self.id, self.expected) {
            Ok(pin) => {
                // pin_prepared may reuse an in-memory seal. A transfer must
                // independently inspect the existing destination as well as
                // the incoming spool, while the pin prevents reclamation.
                pin.verify_and_sync()?;
                return Ok(pin);
            }
            Err(GenerationError::Missing) => {}
            Err(error) => return Err(error),
        }
        let pin = self.store.prepare_private(
            self.id,
            self.expected.length,
            None,
            None,
            |destination, _| {
                apply::inspect(&mut self.file, &self.expected, Some(destination))?;
                apply::inspect(destination, &self.expected, None)?;
                Ok(self.expected)
            },
        )?;
        self.store.retire(pin.id())?;
        Ok(pin)
    }
}
