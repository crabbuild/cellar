// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::ltx::{checksum_page, Crc64, PageHeader, PAGE_HEADER_FLAG_SIZE, PAGE_HEADER_SIZE};
use crate::CHECKSUM_FLAG;
use sha2::{Digest, Sha256};

struct Writer<'a> {
    file: &'a mut HostFile,
    buffer: Vec<u8>,
    length: u64,
    maximum: u64,
    hash: Sha256,
    window: usize,
}

impl<'a> Writer<'a> {
    fn new(file: &'a mut HostFile, maximum: u64, profile: IoProfile) -> Result<Self, Error> {
        if file.file_len()? != 0 {
            return Err(Error::Invalid("nonempty encoder destination"));
        }
        Ok(Self {
            file,
            buffer: Vec::with_capacity(profile.buffer_bytes()),
            length: 0,
            maximum,
            hash: Sha256::new(),
            window: profile.buffer_bytes(),
        })
    }

    fn write(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        let length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or(Error::Limit)?;
        if length > self.maximum {
            return Err(Error::Limit);
        }
        self.hash.update(bytes);
        self.length = length;
        while !bytes.is_empty() {
            let n = bytes.len().min(self.window - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..n]);
            bytes = &bytes[n..];
            if self.buffer.len() == self.window {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        if !self.buffer.is_empty() {
            self.file.write_all_window(&self.buffer)?;
            self.buffer.clear();
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(u64, [u8; 32]), Error> {
        self.flush()?;
        Ok((self.length, self.hash.finalize().into()))
    }
}

pub(super) fn encode(
    filesystem: &dyn FileSystem,
    directory: Option<&std::path::Path>,
    image: ImageState,
    limits: Limits,
    scratch: Option<&ScratchPool>,
    io: IoOptions,
    mut read: impl FnMut(u64, usize) -> io::Result<Vec<u8>>,
) -> Result<SnapshotFile, Error> {
    limits.image(image)?;
    let mut reservation = scratch
        .map(|scratch| {
            let body =
                ltx::full_snapshot_bound(image.length, image.page_size).ok_or(Error::Limit)? as u64;
            let index = (image.length / u64::from(image.page_size))
                .checked_mul(30)
                .and_then(|n| n.checked_add(1))
                .ok_or(Error::Limit)?;
            scratch.acquire(
                body.min(limits.max_encoded_bytes)
                    .checked_add(index.min(limits.max_encoded_bytes))
                    .ok_or(Error::Limit)?,
                2,
            )
        })
        .transpose()?;
    let mut body_file = super::file::open_spool(filesystem, directory, image.length)?;
    let mut index_file = super::file::open_spool(filesystem, directory, image.length)?;
    body_file.set_io_activity(io.activity.clone());
    index_file.set_io_activity(io.activity);
    let metadata = {
        let mut output = Writer::new(&mut body_file, limits.max_encoded_bytes, io.profile)?;
        let mut index = Writer::new(&mut index_file, limits.max_encoded_bytes, io.profile)?;
        let mut file_crc = Crc64::new();
        let header_bytes = header(image).marshal();
        output.write(&header_bytes)?;
        file_crc.update(&header_bytes);
        let mut image_hash = Sha256::new();
        let mut rolling = CHECKSUM_FLAG;
        let mut compressor = crate::lz4_block::Compressor::default();
        let mut offset = 0;
        let mut triple = Vec::with_capacity(30);
        while offset < image.length {
            let n = (image.length - offset).min(BUFFER_BYTES as u64) as usize;
            let bytes = read(offset, n)?;
            if bytes.len() != n {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            if offset == 0 {
                crate::apply::validate_sqlite_header(&bytes, &image)
                    .map_err(|_| Error::Invalid("source SQLite header"))?;
            }
            image_hash.update(&bytes);
            for (i, page) in bytes.chunks_exact(image.page_size as usize).enumerate() {
                let pgno = (offset / u64::from(image.page_size)) as u32 + i as u32 + 1;
                if pgno == ltx::lock_pgno(image.page_size) {
                    // SQLite never writes this page. Canonical zero bytes are
                    // still part of the physical image SHA-256 and length.
                    if page.iter().any(|byte| *byte != 0) {
                        return Err(Error::Invalid("nonzero SQLite lock page"));
                    }
                    continue;
                }
                let compressed = compressor.compress(page);
                let frame = PageHeader {
                    pgno,
                    flags: PAGE_HEADER_FLAG_SIZE,
                }
                .marshal();
                let size = (compressed.len() as u32).to_be_bytes();
                let start = output.length;
                output.write(&frame)?;
                output.write(&size)?;
                output.write(&compressed)?;
                file_crc.update(&frame);
                file_crc.update(&size);
                file_crc.update(page);
                if image.checksum != 0 {
                    rolling = CHECKSUM_FLAG | (rolling ^ checksum_page(pgno, page));
                }
                triple.clear();
                put_varint(&mut triple, u64::from(pgno));
                put_varint(&mut triple, start);
                put_varint(&mut triple, output.length - start);
                index.write(&triple)?;
            }
            offset += n as u64;
        }
        if <[u8; 32]>::from(image_hash.finalize()) != image.digest
            || image.checksum != 0 && rolling != image.checksum
        {
            return Err(Error::Integrity);
        }
        let terminator = [0; PAGE_HEADER_SIZE];
        output.write(&terminator)?;
        file_crc.update(&terminator);
        index.write(&[0])?;
        let (index_length, _) = index.finish()?;
        let mut cursor = Cursor::new(0, index_length, io.profile)?;
        while cursor.position < cursor.end {
            let n = (cursor.end - cursor.position).min(BUFFER_BYTES as u64) as usize;
            let bytes = cursor.exact(&mut index_file, n)?;
            output.write(&bytes)?;
            file_crc.update(&bytes);
        }
        output.write(&index_length.to_be_bytes())?;
        file_crc.update(&index_length.to_be_bytes());
        file_crc.update(&image.checksum.to_be_bytes());
        let trailer = trailer(image, CHECKSUM_FLAG | file_crc.sum64());
        trailer.validate(header(image))?;
        output.write(&trailer.marshal())?;
        let (encoded_length, encoded_digest) = output.finish()?;
        Metadata {
            image,
            encoded_length,
            encoded_digest,
        }
    };
    // Anonymous files are disposable transport material. Generation publication
    // separately syncs the independently verified destination and its directory.
    drop(index_file);
    if let Some(reservation) = &mut reservation {
        reservation.shrink(metadata.encoded_length, 1)?;
    }
    Ok(SnapshotFile::new(
        body_file,
        metadata,
        limits,
        reservation,
        io.profile,
    ))
}
