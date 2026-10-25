// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

/// Large sequential spools should not compete with hot SQLite pages for cache
/// residency. Logical size, rather than compression, selects this policy.
const STREAMING_IMAGE_BYTES: u64 = 256 * 1024 * 1024;

pub(super) fn open_spool(
    filesystem: &dyn FileSystem,
    directory: Option<&std::path::Path>,
    image_length: u64,
) -> Result<HostFile, Error> {
    let mut file = filesystem.temporary_file(directory)?;
    if image_length >= STREAMING_IMAGE_BYTES {
        if let Err(error) = file.advise_streaming() {
            if error.kind() != io::ErrorKind::Unsupported {
                return Err(error.into());
            }
        }
    }
    Ok(file)
}

/// Immutable encoded bytes, owned by an anonymous file and shared through a
/// bounded reader. This is transport material, never a synced generation pin.
#[derive(Clone)]
pub struct SnapshotFile {
    storage: Arc<Storage>,
    metadata: Metadata,
    limits: Limits,
}

// Fields drop in declaration order: close the file before refunding its charge.
struct Storage {
    file: Mutex<HostFile>,
    _reservation: Option<ScratchReservation>,
}

impl SnapshotFile {
    pub(super) fn new(
        file: HostFile,
        metadata: Metadata,
        limits: Limits,
        reservation: Option<ScratchReservation>,
    ) -> Self {
        Self {
            storage: Arc::new(Storage {
                file: Mutex::new(file),
                _reservation: reservation,
            }),
            metadata,
            limits,
        }
    }

    pub fn metadata(&self) -> Metadata {
        self.metadata
    }

    /// Read on the owning blocking domain; no read allocates over 64 KiB.
    pub fn chunk(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        if length == 0
            || length > BUFFER_BYTES
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > self.metadata.encoded_length)
        {
            return Err(Error::Limit);
        }
        let bytes = self
            .storage
            .file
            .lock()
            .map_err(|_| Error::Invalid("poisoned snapshot owner"))?
            .read_exact_at(offset, length)?;
        if bytes.len() != length {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok(bytes)
    }

    /// Verify every frame, canonical index entry, image SHA-256 and encoded
    /// SHA-256 before returning. Caller keeps the destination private and owns
    /// persistence/publication barriers. Failure never returns a partial proof.
    pub fn write_image(&self, destination: &mut HostFile) -> Result<ImageState, Error> {
        if destination.file_len()? != 0 {
            return Err(Error::Invalid("nonempty snapshot destination"));
        }
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| Error::Invalid("poisoned snapshot owner"))?;
        super::decode::verify(&mut file, self.metadata, self.limits, Some(destination))?;
        Ok(self.metadata.image)
    }

    pub fn verify(&self) -> Result<(), Error> {
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| Error::Invalid("poisoned snapshot owner"))?;
        super::decode::verify(&mut file, self.metadata, self.limits, None)
    }
}

/// Ordered receive state with bounded append buffers. A failed write poisons
/// this owner; a caller cannot retry over an uncertain partial append.
pub struct Download {
    file: HostFile,
    reservation: Option<ScratchReservation>,
    metadata: Metadata,
    limits: Limits,
    received: u64,
    hash: Sha256,
    failed: bool,
}

impl Download {
    pub fn new(
        filesystem: &dyn FileSystem,
        metadata: Metadata,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::new_in(filesystem, None, metadata, limits)
    }

    /// Own the receive spool on the caller's explicit scratch volume.
    pub fn new_in(
        filesystem: &dyn FileSystem,
        directory: Option<&std::path::Path>,
        metadata: Metadata,
        limits: Limits,
    ) -> Result<Self, Error> {
        Self::new_bounded(filesystem, directory, metadata, limits, None)
    }

    /// Reserve the complete receive file against a shared node disk budget.
    /// Cancellation cannot refund storage while blocking work owns this value.
    pub fn new_admitted_in(
        filesystem: &dyn FileSystem,
        directory: Option<&std::path::Path>,
        metadata: Metadata,
        limits: Limits,
        scratch: &ScratchPool,
    ) -> Result<Self, Error> {
        Self::new_bounded(filesystem, directory, metadata, limits, Some(scratch))
    }

    fn new_bounded(
        filesystem: &dyn FileSystem,
        directory: Option<&std::path::Path>,
        metadata: Metadata,
        limits: Limits,
        scratch: Option<&ScratchPool>,
    ) -> Result<Self, Error> {
        metadata.validate(limits)?;
        let reservation = scratch
            .map(|s| s.acquire(metadata.encoded_length, 1))
            .transpose()?;
        Ok(Self {
            file: open_spool(filesystem, directory, metadata.image.length)?,
            reservation,
            metadata,
            limits,
            received: 0,
            hash: Sha256::new(),
            failed: false,
        })
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), Error> {
        if self.failed
            || offset != self.received
            || bytes.is_empty()
            || bytes.len() > BUFFER_BYTES
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|n| n > self.metadata.encoded_length)
        {
            return Err(Error::Invalid("download order or bound"));
        }
        if let Err(error) = self.file.write_all(bytes) {
            self.failed = true;
            return Err(error.into());
        }
        self.hash.update(bytes);
        self.received += bytes.len() as u64;
        Ok(())
    }

    /// Confirms the complete encoded identity only. Physical decoding and image
    /// verification are mandatory in `SnapshotFile::write_image` before sealing.
    pub fn finish(mut self) -> Result<SnapshotFile, Error> {
        if self.failed
            || self.received != self.metadata.encoded_length
            || self.file.file_len()? != self.received
            || <[u8; 32]>::from(self.hash.finalize()) != self.metadata.encoded_digest
        {
            return Err(Error::Integrity);
        }
        Ok(SnapshotFile::new(
            self.file,
            self.metadata,
            self.limits,
            self.reservation,
        ))
    }
}
