// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inspect the committed SQLite image independently of captured LTX bytes.
//! The exclusive private database owner supplies the SQL-visible geometry and
//! keeps writers/checkpoint work quiescent for this synchronous observation.

use super::wal_index::WalIndexCut;
use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

impl Db {
    /// Hash the main-file/WAL overlay without serializing the whole database.
    /// Main-file reads go through SQLite's VFS so demand-loaded images work.
    /// WAL offsets are bounded by an independent scratch/index budget, and only
    /// complete checksum-verified transactions contribute to the image.
    ///
    /// The caller must exclude all database writers and checkpoints during inspection.
    /// The live SQLite WAL index fixes the exact committed boundary: corrupt or
    /// missing committed frames cannot fall back to an older same-size image.
    /// This is a physical observation, never a durability or publication proof.
    pub fn committed_image_digest(
        &self,
        length: u64,
        page_size: u32,
        max_wal_bytes: u64,
    ) -> Result<[u8; 32]> {
        if page_size != self.page_size
            || length == 0
            || length > crate::stream_snapshot::MAX_IMAGE_BYTES
            || !length.is_multiple_of(u64::from(page_size))
            || !(WAL_HEADER_SIZE as u64..=512 * 1024 * 1024).contains(&max_wal_bytes)
        {
            return Err(Error::Other("invalid image inspection bounds".into()));
        }
        let mut wal = self.host.open(&self.wal_path())?;
        let wal_length = wal.file_len()?;
        if wal_length < WAL_HEADER_SIZE as u64 || wal_length > max_wal_bytes {
            return Err(Error::Other("image inspection WAL budget exceeded".into()));
        }
        let header = wal.read_exact_at(0, WAL_HEADER_SIZE)?;
        let mut reader = WalReader::new(&header).map_err(Error::from)?;
        if reader.page_size() != page_size {
            return Err(Error::Other("image inspection WAL page size".into()));
        }
        let frame_length = u64::from(page_size) + WAL_FRAME_HEADER_SIZE as u64;
        let cut = WalIndexCut::capture(self)?;
        let committed_end = WAL_HEADER_SIZE as u64 + u64::from(cut.frames()) * frame_length;
        if !cut.matches_wal(&header)
            || u64::from(cut.pages()) * u64::from(page_size) != length
            || committed_end > wal_length
        {
            return Err(Error::Other(
                "committed WAL boundary differs from physical bytes".into(),
            ));
        }
        let mut offset = WAL_HEADER_SIZE as u64;
        let mut pages = BTreeMap::new();
        let mut pending = BTreeMap::new();
        let mut commit = 0;
        while offset < committed_end {
            let frame = wal.read_exact_at(offset, WAL_FRAME_HEADER_SIZE)?;
            let data =
                wal.read_exact_at(offset + WAL_FRAME_HEADER_SIZE as u64, page_size as usize)?;
            let (pgno, size) = reader
                .consume_frame(&frame, &data, true)
                .map_err(Error::from)?;
            if offset + frame_length == committed_end
                && (size == 0 || !cut.matches_last_frame(&frame))
            {
                return Err(Error::Other(
                    "committed WAL frame differs from SQLite's index".into(),
                ));
            }
            if pgno == 0 || pgno == lock_pgno(page_size) {
                return Err(Error::Other("invalid image inspection WAL page".into()));
            }
            pending.insert(pgno, offset + WAL_FRAME_HEADER_SIZE as u64);
            if size != 0 {
                while let Some((page, position)) = pending.pop_first() {
                    pages.insert(page, position);
                }
                while pages.last_key_value().is_some_and(|(page, _)| *page > size) {
                    pages.pop_last();
                }
                commit = size;
            }
            offset += frame_length;
        }
        if u64::from(commit) * u64::from(page_size) != length {
            return Err(Error::Other("SQL/WAL image geometry differs".into()));
        }
        pages.retain(|pgno, _| *pgno <= commit);
        let mut hash = Sha256::new();
        let mut pgno = 1;
        while pgno <= commit {
            if pgno == lock_pgno(page_size) {
                hash.update(vec![0; page_size as usize]);
                pgno += 1;
            } else if let Some(offset) = pages.get(&pgno) {
                let data = wal.read_exact_at(*offset, page_size as usize)?;
                if data.len() != page_size as usize {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                }
                hash.update(&data);
                pgno += 1;
            } else {
                let next_changed = pages.range(pgno..).next().map_or(commit + 1, |(p, _)| *p);
                let stop = next_changed.min(commit + 1);
                let stop = if pgno < lock_pgno(page_size) {
                    stop.min(lock_pgno(page_size))
                } else {
                    stop
                };
                let count = (stop - pgno).min(65536 / page_size);
                let data = self.read_db_range(
                    (u64::from(pgno) - 1) * u64::from(page_size),
                    (count * page_size) as usize,
                )?;
                hash.update(&data);
                pgno += count;
            }
        }
        if wal.file_len()? != wal_length
            || wal.read_exact_at(0, WAL_HEADER_SIZE)? != header
            || WalIndexCut::capture(self)? != cut
        {
            return Err(Error::Other("WAL changed during image inspection".into()));
        }
        Ok(hash.finalize().into())
    }
}
