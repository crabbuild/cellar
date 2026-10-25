// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical image contracts. Only the generation owner can patch a private file;
//! a verified image is not itself a Raft commit or query-publication proof.

use crate::host::{FileSystem, HostFile};
use crate::ltx::{self, DecodeError, DecodeLimits, Header};
use crate::{CHECKSUM_FLAG, TXID};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImageState {
    pub epoch: u64,
    pub txid: TXID,
    /// Zero means that LTX rolling database checksums are not tracked. The
    /// mandatory image SHA-256 is independent of this optional checksum.
    pub checksum: u64,
    pub page_size: u32,
    pub length: u64,
    pub digest: [u8; 32],
}

impl ImageState {
    pub fn empty(epoch: u64) -> Result<Self, ApplyError> {
        if epoch == 0 {
            return Err(ApplyError::Identity);
        }
        Ok(Self {
            epoch,
            txid: TXID(0),
            checksum: 0,
            page_size: 0,
            length: 0,
            digest: Sha256::digest([]).into(),
        })
    }

    pub fn validate(&self, limits: &ApplyLimits) -> Result<(), ApplyError> {
        if self.epoch == 0 || self.checksum != 0 && self.checksum & CHECKSUM_FLAG == 0 {
            return Err(ApplyError::Identity);
        }
        if self.length > limits.decode.max_image_bytes {
            return Err(ApplyError::Limit);
        }
        if self.txid == TXID(0) {
            if *self != Self::empty(self.epoch)? {
                return Err(ApplyError::Identity);
            }
        } else if !ltx::is_valid_page_size(self.page_size)
            || !self.length.is_multiple_of(self.page_size as u64)
        {
            return Err(ApplyError::Geometry);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LtxSegment<'a> {
    pub bytes: &'a [u8],
    pub min_txid: TXID,
    pub max_txid: TXID,
    pub page_count: usize,
    pub decoded_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct ImageUpdate<'a> {
    pub base: ImageState,
    pub result: ImageState,
    pub segments: Vec<LtxSegment<'a>>,
}

#[derive(Clone, Copy, Debug)]
pub struct ApplyLimits {
    pub decode: DecodeLimits,
    pub max_files: usize,
    pub max_total_encoded_bytes: usize,
    pub max_total_decoded_bytes: u64,
    pub max_total_pages: usize,
}

impl Default for ApplyLimits {
    fn default() -> Self {
        Self {
            decode: DecodeLimits::default(),
            max_files: 64,
            max_total_encoded_bytes: 16 * 1024 * 1024,
            max_total_decoded_bytes: 64 * 1024 * 1024,
            max_total_pages: 131_072,
        }
    }
}

impl ApplyLimits {
    pub fn validate(&self) -> Result<(), ApplyError> {
        self.decode.validate()?;
        if self.max_files == 0
            || self.max_files > 1024
            || self.max_total_encoded_bytes == 0
            || self.max_total_encoded_bytes > 512 * 1024 * 1024
            || self.max_total_decoded_bytes == 0
            || self.max_total_decoded_bytes > 256 * 1024 * 1024
            || self.max_total_pages == 0
            || self.max_total_pages > 524_288
        {
            return Err(ApplyError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("invalid image apply limits")]
    InvalidLimits,
    #[error("invalid image epoch, position or checksum identity")]
    Identity,
    #[error("image or LTX apply exceeds its configured bound")]
    Limit,
    #[error("image length, page size or SQLite header is invalid")]
    Geometry,
    #[error("LTX transaction chain does not cover the exact base/result")]
    Chain,
    #[error("LTX envelope declarations differ from verified file contents")]
    Declaration,
    #[error("the required base image is absent")]
    MissingBase,
    #[error("base image SHA-256 does not match")]
    BaseDigest,
    #[error("result image SHA-256 does not match")]
    ResultDigest,
    #[error("image SHA-256 does not match")]
    Digest,
    #[error("rolling database checksum does not match")]
    Checksum,
    #[error("database growth leaves an unspecified page")]
    MissingPage,
    #[error("LTX decode: {0}")]
    Decode(#[from] DecodeError),
    #[error("image I/O: {0}")]
    Io(#[from] std::io::Error),
}

/// The largest temporary image in the entire declared chain. The publisher
/// reserves disk capacity for this size before copying or growing any file.
pub(crate) fn peak_image_bytes(
    update: &ImageUpdate<'_>,
    limits: &ApplyLimits,
) -> Result<u64, ApplyError> {
    limits.validate()?;
    update.base.validate(limits)?;
    update.result.validate(limits)?;
    if update.base.epoch != update.result.epoch || update.result.txid <= update.base.txid {
        return Err(ApplyError::Chain);
    }
    if update.segments.is_empty() || update.segments.len() > limits.max_files {
        return Err(ApplyError::Limit);
    }
    let mut peak = update.base.length;
    let mut txid = update.base.txid.0;
    let mut encoded = 0_usize;
    let mut decoded = 0_u64;
    let mut pages = 0_usize;
    let mut page_size = update.base.page_size;
    let mut last = None;
    for segment in &update.segments {
        if txid.checked_add(1) != Some(segment.min_txid.0) || segment.max_txid < segment.min_txid {
            return Err(ApplyError::Chain);
        }
        txid = segment.max_txid.0;
        encoded = encoded
            .checked_add(segment.bytes.len())
            .ok_or(ApplyError::Limit)?;
        decoded = decoded
            .checked_add(segment.decoded_bytes)
            .ok_or(ApplyError::Limit)?;
        pages = pages
            .checked_add(segment.page_count)
            .ok_or(ApplyError::Limit)?;
        if segment.bytes.len() > limits.decode.max_encoded_bytes
            || segment.decoded_bytes > limits.decode.max_decoded_bytes
            || segment.page_count > limits.decode.max_pages
            || encoded > limits.max_total_encoded_bytes
            || decoded > limits.max_total_decoded_bytes
            || pages > limits.max_total_pages
        {
            return Err(ApplyError::Limit);
        }
        let header = Header::parse(segment.bytes).map_err(DecodeError::from)?;
        header.validate().map_err(DecodeError::from)?;
        if header.min_txid != segment.min_txid || header.max_txid != segment.max_txid {
            return Err(ApplyError::Declaration);
        }
        if page_size == 0 {
            page_size = header.page_size;
        }
        if header.page_size != page_size {
            return Err(ApplyError::Geometry);
        }
        let length = header.commit as u64 * header.page_size as u64;
        if length > limits.decode.max_image_bytes {
            return Err(ApplyError::Limit);
        }
        peak = peak.max(length);
        last = Some(length);
    }
    if txid != update.result.txid.0
        || last != Some(update.result.length)
        || page_size != update.result.page_size
    {
        return Err(ApplyError::Chain);
    }
    Ok(peak)
}

/// Bound the whole private chain before reserving disk or starting I/O. Each
/// intermediate image retains the same geometry/checksum/digest contract.
pub(crate) fn peak_chain_bytes(
    updates: &[ImageUpdate<'_>],
    limits: &ApplyLimits,
) -> Result<u64, ApplyError> {
    limits.validate()?;
    if updates.is_empty() || updates.len() > limits.max_files {
        return Err(ApplyError::Limit);
    }
    let mut previous = updates[0].base;
    let (mut peak, mut files, mut encoded, mut decoded, mut pages) =
        (0u64, 0usize, 0usize, 0u64, 0usize);
    for update in updates {
        if update.base != previous {
            return Err(ApplyError::Chain);
        }
        peak = peak.max(peak_image_bytes(update, limits)?);
        for segment in &update.segments {
            files = files.checked_add(1).ok_or(ApplyError::Limit)?;
            encoded = encoded
                .checked_add(segment.bytes.len())
                .ok_or(ApplyError::Limit)?;
            decoded = decoded
                .checked_add(segment.decoded_bytes)
                .ok_or(ApplyError::Limit)?;
            pages = pages
                .checked_add(segment.page_count)
                .ok_or(ApplyError::Limit)?;
        }
        if files > limits.max_files
            || encoded > limits.max_total_encoded_bytes
            || decoded > limits.max_total_decoded_bytes
            || pages > limits.max_total_pages
        {
            return Err(ApplyError::Limit);
        }
        previous = update.result;
    }
    Ok(peak)
}

/// Only called with an exclusively created private destination. No public API
/// can ask this helper to mutate a published/pinned generation in place.
pub(crate) fn apply_private_chain(
    fs: &dyn FileSystem,
    base: Option<&Path>,
    target: &mut HostFile,
    updates: &[ImageUpdate<'_>],
    limits: &ApplyLimits,
) -> Result<ImageState, ApplyError> {
    peak_chain_bytes(updates, limits)?;
    let update = updates.first().ok_or(ApplyError::Limit)?;
    if target.file_len()? != 0 {
        return Err(ApplyError::Geometry);
    }
    let mut rolling = if let Some(path) = base {
        let mut source = fs.open(path)?;
        inspect(&mut source, &update.base, Some(&mut *target)).map_err(|e| match e {
            ApplyError::Digest => ApplyError::BaseDigest,
            e => e,
        })?
    } else {
        if update.base != ImageState::empty(update.base.epoch)? {
            return Err(ApplyError::MissingBase);
        }
        CHECKSUM_FLAG
    };
    let mut state = update.base;
    for update in updates {
        rolling = apply_private_update(target, update, limits, rolling)?;
        state = update.result;
    }
    Ok(state)
}

// This target belongs exclusively to apply_private_chain. No intermediate
// image is exposed, but every result is physically inspected before proceeding.
fn apply_private_update(
    target: &mut HostFile,
    update: &ImageUpdate<'_>,
    limits: &ApplyLimits,
    mut rolling: u64,
) -> Result<u64, ApplyError> {
    let mut count = if update.base.page_size == 0 {
        0
    } else {
        (update.base.length / update.base.page_size as u64) as u32
    };
    let mut actual_pages = 0_usize;
    let mut actual_decoded = 0_u64;
    let mut last_checksum = 0;
    for segment in &update.segments {
        let mut decode = limits.decode;
        decode.max_pages = decode.max_pages.min(segment.page_count).min(
            limits
                .max_total_pages
                .checked_sub(actual_pages)
                .ok_or(ApplyError::Limit)?,
        );
        decode.max_decoded_bytes = decode.max_decoded_bytes.min(segment.decoded_bytes).min(
            limits
                .max_total_decoded_bytes
                .checked_sub(actual_decoded)
                .ok_or(ApplyError::Limit)?,
        );
        // A zero remaining budget can still admit an empty file; DecodeLimits
        // itself stays a positive configuration and the counts are checked below.
        decode.max_pages = decode.max_pages.max(1);
        decode.max_decoded_bytes = decode.max_decoded_bytes.max(1);
        let file = ltx::decode_file_bounded(segment.bytes, decode)?;
        let header = file.header();
        if file.pages().len() != segment.page_count || file.decoded_bytes() != segment.decoded_bytes
        {
            return Err(ApplyError::Declaration);
        }
        actual_pages = actual_pages
            .checked_add(file.pages().len())
            .ok_or(ApplyError::Limit)?;
        actual_decoded = actual_decoded
            .checked_add(file.decoded_bytes())
            .ok_or(ApplyError::Limit)?;
        if actual_pages > limits.max_total_pages || actual_decoded > limits.max_total_decoded_bytes
        {
            return Err(ApplyError::Limit);
        }
        if !header.no_checksum() && !header.is_snapshot() && header.pre_apply_checksum != rolling {
            return Err(ApplyError::Checksum);
        }
        let page_size = header.page_size as u64;
        let lock = ltx::lock_pgno(header.page_size);
        let mut next = count + 1;
        for (pgno, _) in file.pages().iter().filter(|(pgno, _)| *pgno > count) {
            if next == lock {
                next += 1;
            }
            if *pgno != next {
                return Err(ApplyError::MissingPage);
            }
            next += 1;
        }
        if next == lock {
            next += 1;
        }
        if next <= header.commit {
            return Err(ApplyError::MissingPage);
        }
        for pgno in header.commit + 1..=count {
            if pgno != lock {
                let old = read_page(target, pgno, page_size)?;
                rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(pgno, &old));
            }
        }
        for (pgno, data) in file.pages() {
            if *pgno <= count && *pgno != lock {
                let old = read_page(target, *pgno, page_size)?;
                rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(*pgno, &old));
            }
            target.write_all_at((*pgno as u64 - 1) * page_size, data)?;
            rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(*pgno, data));
        }
        target.set_len(header.commit as u64 * page_size)?;
        count = header.commit;
        last_checksum = file.trailer().post_apply_checksum;
        if !header.no_checksum() && last_checksum != rolling {
            return Err(ApplyError::Checksum);
        }
    }
    if update.result.checksum != last_checksum {
        return Err(ApplyError::Checksum);
    }
    let observed = inspect(target, &update.result, None).map_err(|e| match e {
        ApplyError::Digest => ApplyError::ResultDigest,
        e => e,
    })?;
    if observed != rolling {
        return Err(ApplyError::Checksum);
    }
    Ok(observed)
}

fn read_page(file: &mut HostFile, pgno: u32, page_size: u64) -> Result<Vec<u8>, ApplyError> {
    let bytes = file.read_exact_at((pgno as u64 - 1) * page_size, page_size as usize)?;
    if bytes.len() != page_size as usize {
        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
    }
    Ok(bytes)
}

/// Recompute the full SHA-256 and rolling page checksum from the same bytes
/// optionally copied to a private destination. The caller validates limits.
pub(crate) fn inspect(
    file: &mut HostFile,
    state: &ImageState,
    mut copy: Option<&mut HostFile>,
) -> Result<u64, ApplyError> {
    if file.file_len()? != state.length {
        return Err(ApplyError::Geometry);
    }
    let mut hash = Sha256::new();
    let mut rolling = CHECKSUM_FLAG;
    let mut offset = 0;
    while offset < state.length {
        let amount = (state.length - offset).min(64 * 1024) as usize;
        let bytes = file.read_exact_at(offset, amount)?;
        if bytes.len() != amount {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        if offset == 0 {
            validate_sqlite_header(&bytes, state)?;
        }
        hash.update(&bytes);
        for (i, page) in bytes.chunks_exact(state.page_size as usize).enumerate() {
            let pgno = (offset / state.page_size as u64) as u32 + i as u32 + 1;
            if pgno != ltx::lock_pgno(state.page_size) {
                rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(pgno, page));
            }
        }
        if let Some(target) = copy.as_mut() {
            target.write_all_at(offset, &bytes)?;
        }
        offset += amount as u64;
    }
    if file.file_len()? != state.length {
        return Err(ApplyError::Geometry);
    }
    if <[u8; 32]>::from(hash.finalize()) != state.digest {
        return Err(ApplyError::Digest);
    }
    if state.checksum != 0 && state.checksum != rolling {
        return Err(ApplyError::Checksum);
    }
    Ok(rolling)
}

fn validate_sqlite_header(bytes: &[u8], state: &ImageState) -> Result<(), ApplyError> {
    if bytes.len() < 100 || &bytes[..16] != b"SQLite format 3\0" {
        return Err(ApplyError::Geometry);
    }
    let encoded = u16::from_be_bytes([bytes[16], bytes[17]]);
    let size = if encoded == 1 { 65_536 } else { encoded as u32 };
    if size != state.page_size {
        return Err(ApplyError::Geometry);
    }
    if bytes[24..28] == bytes[92..96] {
        let pages = u32::from_be_bytes(bytes[28..32].try_into().map_err(|_| ApplyError::Geometry)?);
        if pages as u64 * state.page_size as u64 != state.length {
            return Err(ApplyError::Geometry);
        }
    }
    Ok(())
}
