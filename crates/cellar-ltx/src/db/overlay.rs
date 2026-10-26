// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently inspect complete committed WAL transactions. The caller owns
//! the private writer and excludes writes/checkpoints for the entire view.

use super::wal_index::WalIndexCut;
use super::*;
use crate::HostFile;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(super) struct CommittedOverlay {
    wal: HostFile,
    cut: WalIndexCut,
    pub header: Vec<u8>,
    wal_length: u64,
    pages: BTreeMap<u32, u64>,
    pub page_size: u32,
    pub length: u64,
    pub commit_end: u64,
    pub prefix: [u8; 32],
    pub previous_prefix: Option<[u8; 32]>,
}

fn exact(file: &mut HostFile, offset: u64, length: usize) -> Result<Vec<u8>> {
    let bytes = file.read_exact_at(offset, length)?;
    if bytes.len() != length {
        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
    }
    Ok(bytes)
}

impl CommittedOverlay {
    pub fn open(
        db: &Db,
        length: u64,
        page_size: u32,
        max_wal_bytes: u64,
        previous_end: Option<u64>,
        track_prefix: bool,
    ) -> Result<Self> {
        if page_size != db.page_size
            || length == 0
            || length > crate::stream_snapshot::MAX_IMAGE_BYTES
            || !length.is_multiple_of(u64::from(page_size))
            || !(WAL_HEADER_SIZE as u64..=512 * 1024 * 1024).contains(&max_wal_bytes)
        {
            return Err(Error::Other("invalid image inspection bounds".into()));
        }
        let mut wal = db.host.open(&db.wal_path())?;
        let wal_length = wal.file_len()?;
        if wal_length < WAL_HEADER_SIZE as u64 || wal_length > max_wal_bytes {
            return Err(Error::Other("image inspection WAL budget exceeded".into()));
        }
        let header = exact(&mut wal, 0, WAL_HEADER_SIZE)?;
        let cut = WalIndexCut::capture(db)?;
        let mut reader = WalReader::new(&header).map_err(Error::from)?;
        if reader.page_size() != page_size {
            return Err(Error::Other("image inspection WAL page size".into()));
        }
        let frame_length = u64::from(page_size) + WAL_FRAME_HEADER_SIZE as u64;
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
        let mut commit_end = 0;
        let mut hash = track_prefix.then(Sha256::new);
        if let Some(hash) = &mut hash {
            hash.update(&header);
        }
        let mut prefix = [0; 32];
        let mut previous_prefix = None;
        while offset < committed_end {
            let frame = exact(&mut wal, offset, WAL_FRAME_HEADER_SIZE)?;
            let data = exact(
                &mut wal,
                offset + WAL_FRAME_HEADER_SIZE as u64,
                page_size as usize,
            )?;
            let (pgno, size) = reader
                .consume_frame(&frame, &data, true)
                .map_err(Error::from)?;
            if pgno == 0 || pgno == lock_pgno(page_size) {
                return Err(Error::Other("invalid image inspection WAL page".into()));
            }
            if let Some(hash) = &mut hash {
                hash.update(&frame);
                hash.update(&data);
            }
            pending.insert(pgno, offset + WAL_FRAME_HEADER_SIZE as u64);
            offset += frame_length;
            if offset == committed_end && (size == 0 || !cut.matches_last_frame(&frame)) {
                return Err(Error::Other(
                    "committed WAL frame differs from SQLite's index".into(),
                ));
            }
            if size != 0 {
                while let Some((page, position)) = pending.pop_first() {
                    pages.insert(page, position);
                }
                // A truncated page must not reappear from an older transaction
                // if a later commit grows the database again.
                while pages.last_key_value().is_some_and(|(page, _)| *page > size) {
                    pages.pop_last();
                }
                commit = size;
                commit_end = offset;
                if let Some(hash) = &hash {
                    prefix = hash.clone().finalize().into();
                    if previous_end == Some(offset) {
                        previous_prefix = Some(prefix);
                    }
                }
            }
        }
        if u64::from(commit) * u64::from(page_size) != length || commit_end != committed_end {
            return Err(Error::Other("SQL/WAL image geometry differs".into()));
        }
        Ok(Self {
            wal,
            cut,
            header,
            wal_length,
            pages,
            page_size,
            length,
            commit_end,
            prefix,
            previous_prefix,
        })
    }

    pub fn changed_extents(&self, since: u64) -> impl Iterator<Item = u32> + '_ {
        self.pages.iter().filter_map(move |(page, offset)| {
            (*offset >= since).then_some(
                ((u64::from(*page) - 1) * u64::from(self.page_size)
                    / crate::extent::tree::EXTENT_BYTES) as u32,
            )
        })
    }

    /// At most one 64 KiB output and one 64 KiB source buffer; no image buffer.
    pub fn read(&mut self, db: &Db, offset: u64, length: usize) -> Result<Vec<u8>> {
        let size = u64::from(self.page_size);
        if length == 0
            || length > 65536
            || !offset.is_multiple_of(size)
            || !(length as u64).is_multiple_of(size)
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > self.length)
        {
            return Err(Error::Other("invalid committed overlay range".into()));
        }
        let mut output = Vec::with_capacity(length);
        let mut page = (offset / size + 1) as u32;
        let end = page + (length as u32 / self.page_size);
        let lock = lock_pgno(self.page_size);
        while page < end {
            if page == lock {
                output.resize(output.len() + self.page_size as usize, 0);
                page += 1;
            } else if let Some(offset) = self.pages.get(&page) {
                output.extend_from_slice(&exact(&mut self.wal, *offset, self.page_size as usize)?);
                page += 1;
            } else {
                let next = self
                    .pages
                    .range(page..)
                    .next()
                    .map_or(end, |(page, _)| *page);
                let stop = if page < lock {
                    next.min(end).min(lock)
                } else {
                    next.min(end)
                };
                let count = (stop - page) * self.page_size;
                output.extend_from_slice(
                    &db.read_db_range((u64::from(page) - 1) * size, count as usize)?,
                );
                page = stop;
            }
        }
        Ok(output)
    }

    pub fn unchanged(&mut self, db: &Db) -> Result<()> {
        if self.wal.file_len()? != self.wal_length
            || exact(&mut self.wal, 0, WAL_HEADER_SIZE)? != self.header
            || WalIndexCut::capture(db)? != self.cut
        {
            return Err(Error::Other("WAL changed during image inspection".into()));
        }
        Ok(())
    }
}
