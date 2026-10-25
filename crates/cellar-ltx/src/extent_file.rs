// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private extent receive ownership. An extent proves bytes, never a published
//! generation or permission to serve a cell. Callers run I/O on their own domain.

use crate::extent_tree::ExtentId;
use crate::host::{FileSystem, HostFile};
use crate::stream_snapshot::{Error, ScratchPool, ScratchReservation, BUFFER_BYTES};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

/// An incomplete extent cannot expose bytes. Reserve its complete disk charge
/// before creation, and poison every refused/uncertain append until destruction.
pub struct ExtentDownload {
    file: HostFile,
    reservation: ScratchReservation,
    id: ExtentId,
    received: u64,
    hash: Sha256,
    failed: bool,
}

impl ExtentDownload {
    pub fn new(
        filesystem: &dyn FileSystem,
        directory: Option<&Path>,
        scratch: &ScratchPool,
        id: ExtentId,
    ) -> Result<Self, Error> {
        let reservation = scratch.acquire(u64::from(id.length()), 1)?;
        let mut file = filesystem.temporary_file(directory)?;
        if file.file_len()? != 0 {
            return Err(Error::Invalid("nonempty extent receive file"));
        }
        Ok(Self {
            file,
            reservation,
            id,
            received: 0,
            hash: id.stream_hasher(),
            failed: false,
        })
    }

    pub fn append(&mut self, offset: u64, bytes: &[u8]) -> Result<(), Error> {
        let already_failed = self.failed;
        self.failed = true;
        if already_failed
            || bytes.is_empty()
            || bytes.len() > BUFFER_BYTES
            || offset != self.received
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|end| end > u64::from(self.id.length()))
        {
            return Err(Error::Invalid(
                "extent receive order, length or poisoned owner",
            ));
        }
        self.file.write_all_at(offset, bytes)?;
        self.hash.update(bytes);
        self.received += bytes.len() as u64;
        self.failed = false;
        Ok(())
    }

    /// Sync and independently hash the destination before returning an immutable
    /// owner. Source hashes and successful writes alone cannot seal this file.
    pub fn finish(mut self) -> Result<ExtentFile, Error> {
        if self.failed || self.received != u64::from(self.id.length()) {
            return Err(Error::Invalid("incomplete or poisoned extent"));
        }
        if <[u8; 32]>::from(self.hash.finalize()) != self.id.bytes() {
            return Err(Error::Integrity);
        }
        self.file.sync_all()?;
        let blocks = verify_file(&mut self.file, self.id)?;
        Ok(ExtentFile {
            storage: Arc::new(Storage {
                file: Mutex::new(self.file),
                _reservation: self.reservation,
                blocks,
            }),
            id: self.id,
        })
    }
}

// Fields close the anonymous file before refunding disk/file admission.
struct Storage {
    file: Mutex<HostFile>,
    _reservation: ScratchReservation,
    // Derived from independently verified destination bytes only after their
    // complete extent identity matched. At most 64 digests (2 KiB) per file.
    blocks: Vec<[u8; 32]>,
}

/// Shared immutable data material verified against an authenticated manifest's
/// extent identity. Residency/eviction is a separate owner; no path is exposed.
#[derive(Clone)]
pub struct ExtentFile {
    storage: Arc<Storage>,
    id: ExtentId,
}

impl ExtentFile {
    pub fn identity(&self) -> ExtentId {
        self.id
    }

    /// Recheck the containing 64 KiB blocks before exposing any requested byte.
    /// An unaligned request needs at most two bounded reads; no extent rescan.
    pub fn chunk(&self, offset: u64, length: usize) -> Result<Vec<u8>, Error> {
        if length == 0
            || length > BUFFER_BYTES
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > u64::from(self.id.length()))
        {
            return Err(Error::Limit);
        }
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| Error::Invalid("poisoned extent owner"))?;
        let end = offset + length as u64;
        let mut position = offset / BUFFER_BYTES as u64 * BUFFER_BYTES as u64;
        let mut output = Vec::with_capacity(length);
        while position < end {
            let count = (u64::from(self.id.length()) - position).min(BUFFER_BYTES as u64) as usize;
            let bytes = file.read_exact_at(position, count)?;
            if bytes.len() != count {
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            let expected = self
                .storage
                .blocks
                .get((position / BUFFER_BYTES as u64) as usize)
                .ok_or(Error::Invalid("extent read proof coverage"))?;
            if <[u8; 32]>::from(Sha256::digest(&bytes)) != *expected {
                return Err(Error::Integrity);
            }
            let first = offset.saturating_sub(position) as usize;
            let last = (end - position).min(count as u64) as usize;
            output.extend_from_slice(&bytes[first..last]);
            position += count as u64;
        }
        Ok(output)
    }

    pub fn verify(&self) -> Result<(), Error> {
        let mut file = self
            .storage
            .file
            .lock()
            .map_err(|_| Error::Invalid("poisoned extent owner"))?;
        verify_file(&mut file, self.id).map(|_| ())
    }
}

fn verify_file(file: &mut HostFile, id: ExtentId) -> Result<Vec<[u8; 32]>, Error> {
    if file.file_len()? != u64::from(id.length()) {
        return Err(Error::Integrity);
    }
    let mut hash = id.stream_hasher();
    let mut blocks = Vec::with_capacity((id.length() as usize).div_ceil(BUFFER_BYTES));
    let mut offset = 0;
    while offset < u64::from(id.length()) {
        let length = (u64::from(id.length()) - offset).min(BUFFER_BYTES as u64) as usize;
        let bytes = file.read_exact_at(offset, length)?;
        if bytes.len() != length {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        hash.update(&bytes);
        blocks.push(Sha256::digest(&bytes).into());
        offset += length as u64;
    }
    if <[u8; 32]>::from(hash.finalize()) != id.bytes() {
        return Err(Error::Integrity);
    }
    Ok(blocks)
}
