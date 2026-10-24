// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Strict, allocation-bounded physical LTX verification for consensus inputs.

use crate::ltx::{
    checksum_page, is_valid_page_size, lock_pgno, Crc64, Header, PageHeader, Trailer, HEADER_SIZE,
    PAGE_HEADER_FLAG_SIZE, PAGE_HEADER_SIZE, TRAILER_SIZE,
};
use crate::CHECKSUM_FLAG;
use std::io::{Cursor, Read};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_encoded_bytes: usize,
    pub max_decoded_bytes: u64,
    pub max_pages: usize,
    pub max_image_bytes: u64,
    /// Scratch allocation advertised by a pre-v0.5.2 LZ4 frame. Independent
    /// 64-KiB blocks match Cellar's writer; larger formats require explicit bounds.
    pub max_frame_block_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 16 * 1024 * 1024,
            max_decoded_bytes: 64 * 1024 * 1024,
            max_pages: 131_072,
            max_image_bytes: 64 * 1024 * 1024,
            max_frame_block_bytes: 64 * 1024,
        }
    }
}

impl DecodeLimits {
    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.max_encoded_bytes == 0
            || self.max_encoded_bytes > 128 * 1024 * 1024
            || self.max_decoded_bytes == 0
            || self.max_decoded_bytes > 256 * 1024 * 1024
            || self.max_pages == 0
            || self.max_pages > 524_288
            || self.max_image_bytes == 0
            || self.max_image_bytes > 256 * 1024 * 1024
            || !matches!(
                self.max_frame_block_bytes,
                65_536 | 262_144 | 1_048_576 | 4_194_304
            )
        {
            return Err(DecodeError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid bounded LTX decoder limits")]
    InvalidLimits,
    #[error("LTX input exceeds its encoded, decoded, page, image or frame bound")]
    Limit,
    #[error("invalid LTX structure: {0}")]
    Format(&'static str),
    #[error("LTX checksum mismatch")]
    Checksum,
    #[error("LTX decoding failed: {0}")]
    Codec(#[from] crate::Error),
}

/// Successful complete verification. Page bytes and metadata cannot be changed
/// independently after this proof has been constructed.
#[derive(Debug)]
pub struct BoundedLtx {
    header: Header,
    trailer: Trailer,
    pages: Vec<(u32, Vec<u8>)>,
}

impl BoundedLtx {
    pub fn header(&self) -> Header {
        self.header
    }

    pub fn trailer(&self) -> Trailer {
        self.trailer
    }

    pub fn pages(&self) -> &[(u32, Vec<u8>)] {
        &self.pages
    }

    pub fn decoded_bytes(&self) -> u64 {
        self.pages.len() as u64 * self.header.page_size as u64
    }
}

/// Verify bounds before retaining pages or constructing a compressed-frame
/// decoder. Every index entry must locate exactly one contiguous physical frame;
/// page numbers are strictly increasing and snapshots cover their entire image.
/// File CRCs always apply, including HeaderFlagNoChecksum files.
pub fn decode_file_bounded(bytes: &[u8], limits: DecodeLimits) -> Result<BoundedLtx, DecodeError> {
    limits.validate()?;
    if bytes.len() > limits.max_encoded_bytes {
        return Err(DecodeError::Limit);
    }
    if bytes.len() < HEADER_SIZE + PAGE_HEADER_SIZE + 1 + 8 + TRAILER_SIZE {
        return Err(DecodeError::Format("short file"));
    }
    let header = Header::parse(&bytes[..HEADER_SIZE])?;
    header.validate()?;
    if bytes[80..HEADER_SIZE].iter().any(|b| *b != 0) {
        return Err(DecodeError::Format("reserved header bytes"));
    }
    if !is_valid_page_size(header.page_size) {
        return Err(DecodeError::Format("page size"));
    }
    let image_bytes = header.commit as u64 * header.page_size as u64;
    if image_bytes > limits.max_image_bytes {
        return Err(DecodeError::Limit);
    }
    let trailer_at = bytes.len() - TRAILER_SIZE;
    let size_at = trailer_at - 8;
    let index_size = u64::from_be_bytes(
        bytes[size_at..trailer_at]
            .try_into()
            .map_err(|_| DecodeError::Format("index size"))?,
    );
    if index_size > size_at as u64 {
        return Err(DecodeError::Format("index range"));
    }
    let index_at = size_at - index_size as usize;
    if index_at < HEADER_SIZE + PAGE_HEADER_SIZE {
        return Err(DecodeError::Format("page region"));
    }
    let terminator_at = index_at - PAGE_HEADER_SIZE;
    if bytes[terminator_at..index_at] != [0; PAGE_HEADER_SIZE] {
        return Err(DecodeError::Format("page terminator"));
    }
    let index = parse_index(&bytes[index_at..size_at], header, limits, terminator_at)?;
    let trailer = Trailer::parse(&bytes[trailer_at..])?;
    trailer.validate(header)?;
    let mut hash = Crc64::new();
    hash.update(&bytes[..HEADER_SIZE]);
    let mut rolling = CHECKSUM_FLAG;
    let mut pages = Vec::with_capacity(index.len());
    for span in index {
        let frame = &bytes[span.offset..span.end];
        let page = PageHeader::parse(&frame[..PAGE_HEADER_SIZE])?;
        page.validate()?;
        if page.pgno != span.pgno {
            return Err(DecodeError::Format("index page identity"));
        }
        hash.update(&frame[..PAGE_HEADER_SIZE]);
        let mut data = vec![0; header.page_size as usize];
        if page.flags & PAGE_HEADER_FLAG_SIZE != 0 {
            let size_bytes = frame
                .get(PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4)
                .ok_or(DecodeError::Format("block size"))?;
            let size = u32::from_be_bytes(
                size_bytes
                    .try_into()
                    .map_err(|_| DecodeError::Format("block size"))?,
            ) as usize;
            if size == 0
                || size > crate::lz4_block::compress_bound(data.len())
                || size != frame.len() - PAGE_HEADER_SIZE - 4
            {
                return Err(DecodeError::Format("block range"));
            }
            hash.update(size_bytes);
            let decoded =
                lz4_flex::block::decompress_into(&frame[PAGE_HEADER_SIZE + 4..], &mut data)
                    .map_err(|_| DecodeError::Format("compressed block"))?;
            if decoded != data.len() {
                return Err(DecodeError::Format("decoded page length"));
            }
        } else {
            let compressed = &frame[PAGE_HEADER_SIZE..];
            preflight_frame(compressed, data.len(), limits.max_frame_block_bytes)?;
            let mut decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(compressed));
            decoder
                .read_exact(&mut data)
                .map_err(|_| DecodeError::Format("compressed frame"))?;
            let mut extra = [0; 1];
            if decoder
                .read(&mut extra)
                .map_err(|_| DecodeError::Format("frame end"))?
                != 0
                || decoder.into_inner().position() != compressed.len() as u64
            {
                return Err(DecodeError::Format("decoded frame length"));
            }
        }
        hash.update(&data);
        rolling = CHECKSUM_FLAG | (rolling ^ checksum_page(page.pgno, &data));
        pages.push((page.pgno, data));
    }
    hash.update(&bytes[terminator_at..bytes.len() - 8]);
    if CHECKSUM_FLAG | hash.sum64() != trailer.file_checksum {
        return Err(DecodeError::Checksum);
    }
    if header.is_snapshot() && !header.no_checksum() && rolling != trailer.post_apply_checksum {
        return Err(DecodeError::Checksum);
    }
    Ok(BoundedLtx {
        header,
        trailer,
        pages,
    })
}

struct Span {
    pgno: u32,
    offset: usize,
    end: usize,
}

fn parse_index(
    mut bytes: &[u8],
    header: Header,
    limits: DecodeLimits,
    end: usize,
) -> Result<Vec<Span>, DecodeError> {
    let mut spans = Vec::new();
    let mut offset = HEADER_SIZE;
    let mut previous = 0_u32;
    let lock = lock_pgno(header.page_size);
    loop {
        let pgno = varint(&mut bytes)?;
        if pgno == 0 {
            break;
        }
        if spans.len() >= limits.max_pages
            || (spans.len() as u64 + 1) * header.page_size as u64 > limits.max_decoded_bytes
        {
            return Err(DecodeError::Limit);
        }
        let pgno = u32::try_from(pgno).map_err(|_| DecodeError::Format("page number overflow"))?;
        let start = varint(&mut bytes)?;
        let size = varint(&mut bytes)?;
        if pgno <= previous
            || pgno > header.commit
            || pgno == lock
            || start != offset as u64
            || size < PAGE_HEADER_SIZE as u64
            || size > end.saturating_sub(offset) as u64
        {
            return Err(DecodeError::Format("page index coverage"));
        }
        if header.is_snapshot() {
            let expected = if previous == lock - 1 {
                previous + 2
            } else {
                previous + 1
            };
            if pgno != expected {
                return Err(DecodeError::Format("snapshot page gap"));
            }
        }
        let next = offset + size as usize;
        spans.push(Span {
            pgno,
            offset,
            end: next,
        });
        offset = next;
        previous = pgno;
    }
    if !bytes.is_empty() || offset != end {
        return Err(DecodeError::Format("index terminator or page span"));
    }
    if header.is_snapshot() {
        let last = if header.commit == lock {
            lock - 1
        } else {
            header.commit
        };
        if previous != last {
            return Err(DecodeError::Format("incomplete snapshot"));
        }
    }
    Ok(spans)
}

fn varint(bytes: &mut &[u8]) -> Result<u64, DecodeError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = bytes
            .split_first()
            .ok_or(DecodeError::Format("short index varint"))?;
        *bytes = rest;
        if shift == 63 && byte > 1 {
            return Err(DecodeError::Format("index varint overflow"));
        }
        value |= ((byte & 127) as u64) << shift;
        if byte & 128 == 0 {
            if shift != 0 && byte == 0 {
                return Err(DecodeError::Format("noncanonical index varint"));
            }
            return Ok(value);
        }
    }
    Err(DecodeError::Format("index varint overflow"))
}

fn preflight_frame(bytes: &[u8], page_size: usize, max_block: usize) -> Result<(), DecodeError> {
    if bytes.len() < 7 || bytes[..4] != [4, 34, 77, 24] {
        return Err(DecodeError::Format("LZ4 frame magic"));
    }
    let flags = bytes[4];
    let descriptor = bytes[5];
    if flags & 0xc0 != 0x40 || flags & 0x23 != 0x20 || descriptor & 0x8f != 0 {
        return Err(DecodeError::Format("LZ4 frame flags"));
    }
    let block = match (descriptor >> 4) & 7 {
        4 => 65_536,
        5 => 262_144,
        6 => 1_048_576,
        7 => 4_194_304,
        _ => return Err(DecodeError::Format("LZ4 block size")),
    };
    if block > max_block {
        return Err(DecodeError::Limit);
    }
    let mut at = 6_usize;
    if flags & 8 != 0 {
        let size_bytes = bytes
            .get(at..at + 8)
            .ok_or(DecodeError::Format("LZ4 content size"))?;
        let size = u64::from_le_bytes(
            size_bytes
                .try_into()
                .map_err(|_| DecodeError::Format("LZ4 content size"))?,
        );
        if size != 0 && size != page_size as u64 {
            return Err(DecodeError::Format("LZ4 advertised page size"));
        }
        at += 8;
    }
    at += 1; // Header checksum is verified by FrameDecoder, after these bounds.
    loop {
        let size_bytes = bytes
            .get(at..at + 4)
            .ok_or(DecodeError::Format("LZ4 block header"))?;
        let size = u32::from_le_bytes(
            size_bytes
                .try_into()
                .map_err(|_| DecodeError::Format("LZ4 block header"))?,
        );
        at += 4;
        if size == 0 {
            break;
        }
        let length = (size & 0x7fff_ffff) as usize;
        if length == 0 || length > block {
            return Err(DecodeError::Format("LZ4 block length"));
        }
        at = at
            .checked_add(length)
            .and_then(|n| n.checked_add(if flags & 16 != 0 { 4 } else { 0 }))
            .ok_or(DecodeError::Limit)?;
        if at > bytes.len() {
            return Err(DecodeError::Format("short LZ4 block"));
        }
    }
    if flags & 4 != 0 {
        at = at.checked_add(4).ok_or(DecodeError::Limit)?;
    }
    if at != bytes.len() {
        return Err(DecodeError::Format("LZ4 trailing bytes"));
    }
    Ok(())
}
