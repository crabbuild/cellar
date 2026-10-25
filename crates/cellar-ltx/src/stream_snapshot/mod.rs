// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! File-backed physical snapshots with bounded codec buffers and a disk-spooled
//! page index. Bytes never provide cell ownership or Raft installation authority.

mod decode;
mod encode;
mod file;
mod scratch;

use crate::apply::ImageState;
use crate::host::{FileSystem, HostFile};
use crate::ltx::{self, Header, Trailer};
use std::io;

pub use file::{Download, SnapshotFile};
use scratch::Reservation as ScratchReservation;
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
    encode::encode(filesystem, None, image, limits, None, read)
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
    encode::encode(filesystem, Some(directory), image, limits, None, read)
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
    encode::encode(
        filesystem,
        Some(directory),
        image,
        limits,
        Some(scratch),
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
}

impl Cursor {
    fn new(position: u64, end: u64) -> Result<Self, Error> {
        if position > end {
            return Err(Error::Invalid("file region"));
        }
        Ok(Self {
            position,
            end,
            buffer: Vec::new(),
            at: 0,
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
                let amount = (self.end - self.position).min(BUFFER_BYTES as u64) as usize;
                self.buffer = file.read_exact_at(self.position, amount)?;
                if self.buffer.len() != amount {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
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
            let amount = (self.end - self.position).min(BUFFER_BYTES as u64) as usize;
            self.buffer = file.read_exact_at(self.position, amount)?;
            if self.buffer.len() != amount {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
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
