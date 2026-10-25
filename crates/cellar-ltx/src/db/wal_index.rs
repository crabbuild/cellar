// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQLite's live WAL-index boundary distinguishes committed corruption from
//! unused/uncommitted trailing frames. Quiescent writer ownership is mandatory.

use super::{Db, Error, Result};
use rusqlite::ffi;
use std::{ffi::c_void, ptr};

#[derive(Clone, PartialEq, Eq)]
pub(super) struct WalIndexCut([u8; 48]);

impl WalIndexCut {
    pub fn capture(db: &Db) -> Result<Self> {
        let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
        // SAFETY: the live managed connection owns this main VFS file for the
        // entire synchronous call; the output pointer is initialized storage.
        let rc = unsafe {
            ffi::sqlite3_file_control(
                db.conn.handle(),
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                (&mut file as *mut *mut ffi::sqlite3_file).cast(),
            )
        };
        if rc != ffi::SQLITE_OK || file.is_null() {
            return Err(Error::Other(
                "no main file for committed WAL observation".into(),
            ));
        }
        let mut mapping: *mut c_void = ptr::null_mut();
        // SAFETY: SQLite supplied a live method table. xShmMap's 32 KiB region
        // stays mapped while the connection owns the file. This does not create
        // a region, take a checkpoint lock, unmap memory or extend any file.
        let rc = unsafe {
            let methods = (*file).pMethods;
            if methods.is_null() || (*methods).iVersion < 2 {
                return Err(Error::Other("main VFS has no WAL-index methods".into()));
            }
            let map = (*methods)
                .xShmMap
                .ok_or_else(|| Error::Other("main VFS has no WAL-index mapping".into()))?;
            map(file, 0, 32768, 0, &mut mapping)
        };
        if rc != ffi::SQLITE_OK || mapping.is_null() {
            return Err(Error::Other("committed WAL-index is unavailable".into()));
        }
        let mut copies = [0u8; 96];
        // SAFETY: xShmMap returned a live 32 KiB region, and copies owns 96
        // initialized bytes. The WAL write lock protects header mutations even
        // if another connection violates the caller's quiescence contract.
        // Between acquisition and release there is only an infallible bounded
        // copy: no fallible work, callbacks or early return can leak the lock.
        unsafe {
            let lock = (*(*file).pMethods)
                .xShmLock
                .ok_or_else(|| Error::Other("main VFS has no WAL-index lock".into()))?;
            let rc = lock(file, 0, 1, ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_EXCLUSIVE);
            if rc != ffi::SQLITE_OK {
                return Err(Error::Other(
                    "committed WAL-index has an active writer".into(),
                ));
            }
            ptr::copy_nonoverlapping(mapping.cast::<u8>(), copies.as_mut_ptr(), copies.len());
            let rc = lock(
                file,
                0,
                1,
                ffi::SQLITE_SHM_UNLOCK | ffi::SQLITE_SHM_EXCLUSIVE,
            );
            if rc != ffi::SQLITE_OK {
                return Err(Error::Other("committed WAL-index unlock failed".into()));
            }
        }
        Self::decode(&copies, db.page_size)
    }

    fn decode(copies: &[u8; 96], expected_page_size: u32) -> Result<Self> {
        let header: [u8; 48] = copies[..48]
            .try_into()
            .map_err(|_| Error::Other("WAL-index header length".into()))?;
        let cut = Self(header);
        let checksum = crate::wal_checksum(cfg!(target_endian = "big"), 0, 0, &header[..40]);
        // WalIndexHdr is SQLite's format 3007000: two identical native-endian
        // headers, with salts copied verbatim from the big-endian WAL header.
        // Refuse an unsupported layout instead of interpreting it optimistically.
        let encoded_page_size = u16::from_ne_bytes([header[14], header[15]]);
        let page_size = if encoded_page_size == 1 {
            65536
        } else {
            u32::from(encoded_page_size)
        };
        if copies[..48] != copies[48..]
            || cut.word(0) != 3_007_000
            || header[12] != 1
            || header[13] > 1
            || page_size != expected_page_size
            || checksum != (cut.word(40), cut.word(44))
            || cut.frames() == 0
            || cut.pages() == 0
        {
            return Err(Error::Other("invalid committed WAL-index header".into()));
        }
        Ok(cut)
    }

    fn word(&self, offset: usize) -> u32 {
        u32::from_ne_bytes([
            self.0[offset],
            self.0[offset + 1],
            self.0[offset + 2],
            self.0[offset + 3],
        ])
    }

    pub fn frames(&self) -> u32 {
        self.word(16)
    }

    pub fn pages(&self) -> u32 {
        self.word(20)
    }

    pub fn matches_wal(&self, header: &[u8]) -> bool {
        header.len() == 32 && self.0[32..40] == header[16..24] && self.0[13] == header[3] & 1
    }

    pub fn matches_last_frame(&self, header: &[u8]) -> bool {
        header.len() == 24
            && u32::from_be_bytes([header[16], header[17], header[18], header[19]]) == self.word(24)
            && u32::from_be_bytes([header[20], header[21], header[22], header[23]]) == self.word(28)
    }
}

#[cfg(test)]
#[path = "wal_index_tests.rs"]
mod tests;
