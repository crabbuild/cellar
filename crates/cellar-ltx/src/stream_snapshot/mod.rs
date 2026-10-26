// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! File-backed physical snapshots with bounded codec buffers and a disk-spooled
//! page index. Bytes never provide cell ownership or Raft installation authority.

mod decode;
mod encode;
mod file;
mod io_profile;
mod scratch;

use crate::apply::ImageState;
use crate::host::{FileSystem, HostFile};
use crate::ltx::{self, Header, Trailer};
use std::io;

pub use file::{Download, SnapshotFile};
pub(crate) use io_profile::IoOptions;
pub use io_profile::IoProfile;
pub(crate) use scratch::Reservation as ScratchReservation;
pub use scratch::{ScratchLimits, ScratchPool, ScratchUsage};

/// Decimal per-cell SQLite quota. Transfer and scratch budgets are independent.
pub const MAX_IMAGE_BYTES: u64 = 10_000_000_000;
/// v0.5.2 frames, index and trailer fit below this conservative encoded bound.
pub const MAX_ENCODED_BYTES: u64 = 12_000_000_000;
pub(crate) const BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_image_bytes: u64,
    pub max_encoded_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_image_bytes: MAX_IMAGE_BYTES,
            max_encoded_bytes: MAX_ENCODED_BYTES,
        }
    }
}

impl Limits {
    pub fn validate(self) -> Result<(), Error> {
        if !(512..=MAX_IMAGE_BYTES).contains(&self.max_image_bytes)
            || self.max_encoded_bytes == 0
            || self.max_encoded_bytes > MAX_ENCODED_BYTES
        {
            return Err(Error::Limit);
        }
        Ok(())
    }

    pub(crate) fn image(self, state: ImageState) -> Result<(), Error> {
        self.validate()?;
        if state.epoch == 0
            || state.txid.0 == 0
            || state.length == 0
            || state.length > self.max_image_bytes
            || !ltx::is_valid_page_size(state.page_size)
            || !state.length.is_multiple_of(u64::from(state.page_size))
            || (state.checksum != 0 && state.checksum & crate::CHECKSUM_FLAG == 0)
        {
            return Err(Error::Invalid("snapshot image geometry or quota"));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("streamed snapshot exceeds its image, encoded or buffer bound")]
    Limit,
    #[error("node snapshot scratch capacity is exhausted")]
    ScratchFull,
    #[error("invalid streamed snapshot: {0}")]
    Invalid(&'static str),
    #[error("streamed snapshot integrity mismatch")]
    Integrity,
    #[error("streamed snapshot I/O: {0}")]
    Io(#[from] io::Error),
    #[error("streamed snapshot codec: {0}")]
    Codec(#[from] crate::Error),
}

/// Data identity authenticated by the caller's history/position descriptor.
/// A declaration is untrusted until physical verification completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Metadata {
    pub image: ImageState,
    pub encoded_length: u64,
    pub encoded_digest: [u8; 32],
}

impl Metadata {
    pub(crate) fn validate(self, limits: Limits) -> Result<(), Error> {
        limits.image(self.image)?;
        if self.encoded_length == 0 || self.encoded_length > limits.max_encoded_bytes {
            return Err(Error::Limit);
        }
        Ok(())
    }
}

/// Encode a pinned, immutable physical image. `read` must return the exact
/// requested bytes and is never asked for more than 64 KiB. Work belongs on
/// the owning blocking domain. Anonymous scratch handles own their disk space.
pub fn encode(
    filesystem: &dyn FileSystem,
    image: ImageState,
    limits: Limits,
    read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    encode_with_io_profile(filesystem, image, limits, IoProfile::Compact, read)
}

/// Encode with explicit codec memory admission supplied by the caller.
/// The image source still receives at most 64 KiB per request. Providers without
/// native window operations retain the same 64 KiB physical fallback.
pub fn encode_with_io_profile(
    filesystem: &dyn FileSystem,
    image: ImageState,
    limits: Limits,
    profile: IoProfile,
    read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    encode::encode(
        filesystem,
        None,
        image,
        limits,
        None,
        IoOptions {
            profile,
            activity: None,
        },
        read,
    )
}

/// Encode using anonymous scratch files on an explicitly owned filesystem
/// volume. The directory must already exist; no path becomes image authority.
pub fn encode_in(
    filesystem: &dyn FileSystem,
    directory: &std::path::Path,
    image: ImageState,
    limits: Limits,
    read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    encode::encode(
        filesystem,
        Some(directory),
        image,
        limits,
        None,
        IoOptions::default(),
        read,
    )
}

/// Encode under a shared node disk reservation acquired before file creation.
/// The directory and pool must describe the same owned scratch volume policy.
pub fn encode_admitted_in(
    filesystem: &dyn FileSystem,
    directory: &std::path::Path,
    image: ImageState,
    limits: Limits,
    scratch: &ScratchPool,
    read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    encode_admitted_with_io_profile_in(
        filesystem,
        directory,
        image,
        limits,
        scratch,
        IoProfile::Compact,
        read,
    )
}

/// Encode under shared disk admission and an explicit caller-admitted codec
/// memory profile. Reservation precedes file creation and source I/O.
pub fn encode_admitted_with_io_profile_in(
    filesystem: &dyn FileSystem,
    directory: &std::path::Path,
    image: ImageState,
    limits: Limits,
    scratch: &ScratchPool,
    profile: IoProfile,
    read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    encode_observed_in(
        filesystem,
        directory,
        image,
        limits,
        scratch,
        IoOptions {
            profile,
            activity: None,
        },
        read,
    )
}

pub(crate) fn encode_observed_in(
    filesystem: &dyn FileSystem,
    directory: &std::path::Path,
    image: ImageState,
    limits: Limits,
    scratch: &ScratchPool,
    io: IoOptions,
    read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    encode::encode(
        filesystem,
        Some(directory),
        image,
        limits,
        Some(scratch),
        io,
        read,
    )
}

fn header(image: ImageState) -> Header {
    Header {
        version: ltx::VERSION,
        flags: if image.checksum == 0 {
            ltx::HEADER_FLAG_NO_CHECKSUM
        } else {
            0
        },
        page_size: image.page_size,
        commit: (image.length / u64::from(image.page_size)) as u32,
        min_txid: crate::TXID(1),
        max_txid: image.txid,
        ..Default::default()
    }
}

fn trailer(image: ImageState, checksum: u64) -> Trailer {
    Trailer {
        post_apply_checksum: image.checksum,
        file_checksum: checksum,
    }
}

/// Bounded sequential access to one region of an injected file. All positions
/// stay u64, including files crossing the 4 GiB byte-offset boundary.
struct Cursor {
    position: u64,
    end: u64,
    buffer: Vec<u8>,
    at: usize,
    window: usize,
}

impl Cursor {
    fn new(position: u64, end: u64, profile: IoProfile) -> Result<Self, Error> {
        if position > end {
            return Err(Error::Invalid("file region"));
        }
        Ok(Self {
            position,
            end,
            buffer: Vec::new(),
            at: 0,
            window: profile.buffer_bytes(),
        })
    }

    fn exact(&mut self, file: &mut HostFile, count: usize) -> Result<Vec<u8>, Error> {
        if count > 2 * BUFFER_BYTES
            || self
                .position
                .checked_add(count as u64)
                .is_none_or(|n| n > self.end)
        {
            return Err(Error::Invalid("short file region"));
        }
        let mut output = Vec::with_capacity(count);
        while output.len() < count {
            if self.at == self.buffer.len() {
                let amount = (self.end - self.position).min(self.window as u64) as usize;
                self.buffer.resize(amount, 0);
                file.read_exact_into_at(self.position, &mut self.buffer)?;
                self.at = 0;
            }
            let amount = (count - output.len()).min(self.buffer.len() - self.at);
            output.extend_from_slice(&self.buffer[self.at..self.at + amount]);
            self.at += amount;
            self.position += amount as u64;
        }
        Ok(output)
    }

    fn byte(&mut self, file: &mut HostFile) -> Result<u8, Error> {
        if self.position == self.end {
            return Err(Error::Invalid("short index varint"));
        }
        if self.at == self.buffer.len() {
            let amount = (self.end - self.position).min(self.window as u64) as usize;
            self.buffer.resize(amount, 0);
            file.read_exact_into_at(self.position, &mut self.buffer)?;
            self.at = 0;
        }
        let byte = self.buffer[self.at];
        self.at += 1;
        self.position += 1;
        Ok(byte)
    }

    fn varint(&mut self, file: &mut HostFile) -> Result<u64, Error> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            let byte = self.byte(file)?;
            if shift == 63 && byte > 1 {
                return Err(Error::Invalid("index varint overflow"));
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                if shift != 0 && byte == 0 {
                    return Err(Error::Invalid("noncanonical index varint"));
                }
                return Ok(value);
            }
        }
        Err(Error::Invalid("index varint overflow"))
    }
}

impl From<io::ErrorKind> for Error {
    fn from(kind: io::ErrorKind) -> Self {
        Self::Io(io::Error::from(kind))
    }
}

fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        bytes.push(value as u8 | 128);
        value >>= 7;
    }
    bytes.push(value as u8);
}

// Unix fixture files support the sparse prefix without allocating four GiB.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn index_varints_cross_io_windows_above_four_gib_and_refuse_invalid_suffixes() {
        let mut maximum = vec![255; 9];
        maximum.push(1);
        for profile in [IoProfile::Compact, IoProfile::Buffered] {
            for (suffix, expected) in [
                (maximum.clone(), Some(u64::MAX)),
                (vec![128], None),
                (vec![128, 0], None),
                (vec![255; 10], None),
            ] {
                let mut bytes = vec![85; profile.buffer_bytes() - 1];
                let prefix_length = bytes.len();
                bytes.extend_from_slice(&suffix);
                let offset = (1_u64 << 32) + 8192;
                let mut file = crate::DirectFileSystem.temporary_file(None).unwrap();
                for (index, chunk) in bytes.chunks(BUFFER_BYTES).enumerate() {
                    file.write_all_at(offset + (index * BUFFER_BYTES) as u64, chunk)
                        .unwrap();
                }
                let end = offset + bytes.len() as u64;
                let mut cursor = Cursor::new(offset, end, profile).unwrap();
                let mut remaining = prefix_length;
                while remaining != 0 {
                    let count = remaining.min(BUFFER_BYTES);
                    assert_eq!(cursor.exact(&mut file, count).unwrap(), vec![85; count]);
                    remaining -= count;
                }
                // The first varint byte is the last byte in the first window.
                // Continuation bytes must refill without changing the u64 cut.
                match expected {
                    Some(value) => {
                        assert_eq!(cursor.varint(&mut file).unwrap(), value);
                        assert_eq!(cursor.position, end);
                    }
                    None => assert!(cursor.varint(&mut file).is_err()),
                }
                assert!(cursor.position <= end);
            }
        }
    }
}
