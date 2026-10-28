// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only, pinned local cache handles. The durable inventory owns the data;
//! the image cache owns bounded descriptors and verification metadata only.

use super::{inventory::object_name, *};
use crate::extent::file::ExtentFile;
use crate::stream_snapshot::ScratchReservation;

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;

impl RootView {
    pub(crate) fn cached_extent(
        &self,
        id: ExtentId,
        scratch: &ScratchPool,
    ) -> Result<ExtentFile, stream_snapshot::Error> {
        if !self.data.objects.contains(&id) {
            return Err(stream_snapshot::Error::Invalid(
                "extent outside pinned root",
            ));
        }
        // Keep the same conservative byte/file admission as an anonymous
        // download. A refused reservation performs no file open or sync.
        let reservation = scratch.acquire(u64::from(id.length()), 1)?;
        self.open_cached_extent(id, reservation)
            .map_err(|error| stream_snapshot::Error::Io(io::Error::other(error)))
    }

    fn open_cached_extent(
        &self,
        id: ExtentId,
        reservation: ScratchReservation,
    ) -> Result<ExtentFile, StoreError> {
        let result = (|| {
            let (verified, proof) = {
                let state = self.inner.state()?;
                let entry = state.objects.get(&id).ok_or(StoreError::Missing)?;
                if entry.bytes != u64::from(id.length()) {
                    return Err(StoreError::Integrity);
                }
                (entry.verified, entry.read_proof.clone())
            };
            let mut file = self.inner.checked_file(
                &self.inner.directory.join(object_name(id)),
                u64::from(id.length()),
            )?;
            // Restart clears persistence observations. Re-establish both
            // barriers before deriving a read capability from reopened bytes.
            if !verified {
                file.sync_all()?;
                self.inner.filesystem.sync_all(&self.inner.directory)?;
            }
            let file = ExtentFile::from_local(
                file,
                reservation,
                self.clone(),
                id,
                if verified { proof } else { None },
            )?;
            let mut state = self.inner.state()?;
            let entry = state.objects.get_mut(&id).ok_or(StoreError::Missing)?;
            entry.verified = true;
            // Keep proof metadata after descriptor eviction. Object count
            // admission bounds it; reclamation drops it with the inventory.
            entry.read_proof = Some(file.block_proof());
            Ok(file)
        })();
        if result.is_err() {
            if let Some(entry) = self.inner.state()?.objects.get_mut(&id) {
                entry.verified = false;
                entry.read_proof = None;
            }
        }
        result
    }
}
