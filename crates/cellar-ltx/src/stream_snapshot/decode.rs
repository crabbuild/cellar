// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::ltx::{
    checksum_page, Crc64, PageHeader, HEADER_SIZE, PAGE_HEADER_FLAG_SIZE, PAGE_HEADER_SIZE,
    TRAILER_SIZE,
};
use crate::CHECKSUM_FLAG;
use sha2::{Digest, Sha256};

pub(super) fn verify(
    file: &mut HostFile,
    metadata: Metadata,
    limits: Limits,
    destination: Option<&mut HostFile>,
    profile: IoProfile,
) -> Result<(), Error> {
    metadata.validate(limits)?;
    let length = metadata.encoded_length;
    if file.file_len()? != length
        || length < (HEADER_SIZE + PAGE_HEADER_SIZE + 1 + 8 + TRAILER_SIZE) as u64
    {
        return Err(Error::Invalid("snapshot file length"));
    }
    let mut destination = ImageWriter {
        file: destination,
        buffer: Vec::with_capacity(BUFFER_BYTES),
        offset: 0,
    };
    let image = metadata.image;
    let page_size = image.page_size as usize;
    let header_bytes = file.read_exact_at(0, HEADER_SIZE)?;
    let actual_header = Header::parse(&header_bytes)?;
    actual_header.validate()?;
    if actual_header != header(image) || header_bytes != actual_header.marshal() {
        return Err(Error::Invalid("snapshot header binding"));
    }
    let size_at = length - TRAILER_SIZE as u64 - 8;
    let footer = file.read_exact_at(size_at, 8 + TRAILER_SIZE)?;
    if footer.len() != 8 + TRAILER_SIZE {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let index_length = u64::from_be_bytes(
        footer[..8]
            .try_into()
            .map_err(|_| Error::Invalid("index length"))?,
    );
    let index_at = size_at
        .checked_sub(index_length)
        .ok_or(Error::Invalid("index region"))?;
    let terminator_at = index_at
        .checked_sub(PAGE_HEADER_SIZE as u64)
        .ok_or(Error::Invalid("page region"))?;
    if terminator_at < HEADER_SIZE as u64 {
        return Err(Error::Invalid("short page region"));
    }
    let actual_trailer = Trailer::parse(&footer[8..])?;
    actual_trailer.validate(actual_header)?;
    if actual_trailer.post_apply_checksum != image.checksum {
        return Err(Error::Invalid("snapshot checksum binding"));
    }
    let mut index = Cursor::new(index_at, size_at, profile)?;
    let mut frames = Cursor::new(HEADER_SIZE as u64, index_at, profile)?;
    let mut file_crc = Crc64::new();
    file_crc.update(&header_bytes);
    let mut encoded_hash = Sha256::new();
    encoded_hash.update(&header_bytes);
    let mut image_hash = Sha256::new();
    let mut index_hash = Sha256::new();
    let mut rolling = CHECKSUM_FLAG;
    let mut triple = Vec::with_capacity(30);
    let mut data = vec![0; page_size];
    for pgno in 1..=actual_header.commit {
        if pgno == ltx::lock_pgno(image.page_size) {
            let zeros = vec![0; page_size];
            image_hash.update(&zeros);
            destination.write(&zeros)?;
            continue;
        }
        let indexed_page = index.varint(file)?;
        let start = index.varint(file)?;
        let size = index.varint(file)?;
        if indexed_page != u64::from(pgno)
            || start != frames.position
            || size < (PAGE_HEADER_SIZE + 4) as u64
            || size > (PAGE_HEADER_SIZE + 4 + crate::lz4_block::compress_bound(page_size)) as u64
            || start
                .checked_add(size)
                .is_none_or(|end| end > terminator_at)
        {
            return Err(Error::Invalid("snapshot index coverage"));
        }
        triple.clear();
        put_varint(&mut triple, indexed_page);
        put_varint(&mut triple, start);
        put_varint(&mut triple, size);
        index_hash.update(&triple);
        let frame = frames.exact(file, size as usize)?;
        let page = PageHeader::parse(&frame[..PAGE_HEADER_SIZE])?;
        page.validate()?;
        if page.pgno != pgno || page.flags != PAGE_HEADER_FLAG_SIZE {
            return Err(Error::Invalid("snapshot frame identity"));
        }
        let compressed_size = u32::from_be_bytes(
            frame[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 4]
                .try_into()
                .map_err(|_| Error::Invalid("snapshot block length"))?,
        ) as usize;
        if compressed_size == 0 || compressed_size != frame.len() - PAGE_HEADER_SIZE - 4 {
            return Err(Error::Invalid("snapshot block range"));
        }
        let decoded = lz4_flex::block::decompress_into(&frame[PAGE_HEADER_SIZE + 4..], &mut data)
            .map_err(|_| Error::Invalid("snapshot compressed page"))?;
        if decoded != page_size {
            return Err(Error::Invalid("snapshot decoded page length"));
        }
        if pgno == 1 {
            crate::apply::validate_sqlite_header(&data, &image)
                .map_err(|_| Error::Invalid("decoded SQLite header"))?;
        }
        file_crc.update(&frame[..PAGE_HEADER_SIZE + 4]);
        file_crc.update(&data);
        encoded_hash.update(&frame);
        image_hash.update(&data);
        if image.checksum != 0 {
            rolling = CHECKSUM_FLAG | (rolling ^ checksum_page(pgno, &data));
        }
        destination.write(&data)?;
    }
    if index.varint(file)? != 0 || index.position != size_at || frames.position != terminator_at {
        return Err(Error::Invalid("snapshot index terminator"));
    }
    index_hash.update([0]);
    let terminator = frames.exact(file, PAGE_HEADER_SIZE)?;
    if terminator != [0; PAGE_HEADER_SIZE] {
        return Err(Error::Invalid("snapshot page terminator"));
    }
    file_crc.update(&terminator);
    encoded_hash.update(&terminator);
    let mut index_bytes_hash = Sha256::new();
    let mut tail = Cursor::new(index_at, size_at, profile)?;
    while tail.position < tail.end {
        let n = (tail.end - tail.position).min(BUFFER_BYTES as u64) as usize;
        let bytes = tail.exact(file, n)?;
        file_crc.update(&bytes);
        encoded_hash.update(&bytes);
        index_bytes_hash.update(&bytes);
    }
    file_crc.update(&footer[..16]);
    encoded_hash.update(&footer);
    if <[u8; 32]>::from(index_hash.finalize()) != <[u8; 32]>::from(index_bytes_hash.finalize())
        || CHECKSUM_FLAG | file_crc.sum64() != actual_trailer.file_checksum
        || <[u8; 32]>::from(encoded_hash.finalize()) != metadata.encoded_digest
        || <[u8; 32]>::from(image_hash.finalize()) != image.digest
        || image.checksum != 0 && rolling != image.checksum
    {
        return Err(Error::Integrity);
    }
    destination.finish(image.length)?;
    Ok(())
}

/// Batch sequential decoded pages into 64 KiB writes, including the canonical
/// reserved lock-page hole. Destination ownership remains with the generation
/// publisher, and no success proof escapes before all integrity checks pass.
struct ImageWriter<'a> {
    file: Option<&'a mut HostFile>,
    buffer: Vec<u8>,
    offset: u64,
}

impl ImageWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if self.file.is_none() {
            return Ok(());
        }
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() == BUFFER_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        if let Some(target) = &mut self.file {
            if !self.buffer.is_empty() {
                target.write_all_at(self.offset, &self.buffer)?;
                self.offset += self.buffer.len() as u64;
                self.buffer.clear();
            }
        }
        Ok(())
    }

    fn finish(mut self, length: u64) -> Result<(), Error> {
        self.flush()?;
        if let Some(target) = self.file {
            if self.offset != length {
                return Err(Error::Invalid("decoded image length"));
            }
            target.set_len(length)?;
        }
        Ok(())
    }
}
