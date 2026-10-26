// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inspect the committed SQLite image independently of captured LTX bytes.
//! The exclusive private database owner supplies the SQL-visible geometry and
//! keeps writers/checkpoint work quiescent for this synchronous observation.

use super::{overlay::CommittedOverlay, Db, Result};
use sha2::{Digest, Sha256};

impl Db {
    /// Hash the main-file/WAL overlay without serializing the whole database.
    /// Main-file reads go through SQLite's VFS so demand-loaded images work.
    /// WAL offsets are bounded by an independent scratch/index budget, and only
    /// complete checksum-verified transactions contribute to the image.
    ///
    /// The caller must exclusively own all database writers during inspection.
    /// This is a physical observation, never a durability or publication proof.
    pub fn committed_image_digest(
        &self,
        length: u64,
        page_size: u32,
        max_wal_bytes: u64,
    ) -> Result<[u8; 32]> {
        let mut overlay =
            CommittedOverlay::open(self, length, page_size, max_wal_bytes, None, false)?;
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < length {
            let count = (length - offset).min(65536) as usize;
            hash.update(overlay.read(self, offset, count)?);
            offset += count as u64;
        }
        overlay.unchanged(self)?;
        Ok(hash.finalize().into())
    }
}
