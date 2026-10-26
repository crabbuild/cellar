// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQLite image preparation over immutable extents. A raw closure seal is not
//! sufficient: image geometry, the lock-page hole and optional LTX checksum
//! receive a separate capability before a native owner can publish a marker.

use super::super::tree::{Geometry, ImageBinding, EXTENT_BYTES};
use super::*;
use crate::{
    apply::{ApplyError, ApplyLimits, LtxSegment},
    ltx, CHECKSUM_FLAG,
};

/// A locally sealed SQLite image, distinct from a generic sealed byte closure.
/// This establishes neither a committed Raft cut nor permission to serve it.
pub struct SealedImageRoot(SealedRoot);

impl SealedImageRoot {
    pub fn view(&self) -> &RootView {
        self.0.view()
    }

    pub fn reference(&self) -> ExtentRootReference {
        self.view().reference()
    }

    pub fn manifest(&self) -> &ExtentManifest {
        self.view().manifest()
    }
}

/// Expected exact transition, independently supplied by the command/consensus
/// owner. Result extents are derived from verified LTX and local base bytes.
pub struct ImageRootUpdate<'a> {
    pub base: ExtentRootReference,
    pub result: ExtentRootReference,
    pub binding: ImageBinding,
    pub geometry: Geometry,
    pub segments: Vec<LtxSegment<'a>>,
}

/// Logical work counters, not OS I/O or transferred-byte measurements.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RootApplyWork {
    pub base_bytes_read: u64,
    pub extents_prepared: u32,
    pub introduced_extent_bytes: u64,
}

pub struct PreparedImageRoot {
    pub image: SealedImageRoot,
    pub work: RootApplyWork,
}

fn reference(manifest: &ExtentManifest) -> Result<ExtentRootReference, StoreError> {
    ExtentRootReference::new(manifest.root(), manifest.encode()?.len() as u32)
}

fn check_buffer(buffer: &[u8]) -> Result<(), StoreError> {
    if buffer.len() != EXTENT_BYTES as usize {
        return Err(StoreError::Limit);
    }
    Ok(())
}

fn page_count(geometry: Geometry) -> u32 {
    if geometry.length() == 0 {
        0
    } else {
        (geometry.length() / u64::from(geometry.page_size())) as u32
    }
}

fn extent_index(page: u32, page_size: u32) -> u32 {
    ((u64::from(page) - 1) * u64::from(page_size) / EXTENT_BYTES) as u32
}

impl NodeExtentStore {
    fn read_image_extent(
        &self,
        id: ExtentId,
        buffer: &mut [u8],
        work: &mut RootApplyWork,
    ) -> Result<(), StoreError> {
        super::publication::read_object(&self.inner, id, |offset, bytes| {
            let start = offset as usize;
            buffer
                .get_mut(start..start + bytes.len())
                .ok_or(StoreError::Integrity)?
                .copy_from_slice(bytes);
            Ok(())
        })?;
        work.base_bytes_read = work
            .base_bytes_read
            .checked_add(u64::from(id.length()))
            .ok_or(StoreError::Limit)?;
        Ok(())
    }

    /// The caller admits the 4 MiB buffer separately from shared disk capacity.
    /// Every extent must already have its own verified and synced local closure.
    pub fn seal_image_root(
        &self,
        manifest: ExtentManifest,
        buffer: &mut [u8],
    ) -> Result<SealedImageRoot, StoreError> {
        check_buffer(buffer)?;
        let operation = self.begin()?;
        let result = (|| {
            self.check_image(&manifest, buffer, &mut RootApplyWork::default())?;
            Ok(SealedImageRoot(self.seal_root_inner(manifest)?))
        })();
        operation.finish()?;
        result
    }

    fn check_image(
        &self,
        manifest: &ExtentManifest,
        buffer: &mut [u8],
        work: &mut RootApplyWork,
    ) -> Result<(), StoreError> {
        let geometry = manifest.geometry();
        if geometry.length() == 0 {
            return Ok(());
        }
        let checksums = manifest.binding().checksum != 0;
        let lock = ltx::lock_pgno(geometry.page_size());
        let lock_index = extent_index(lock, geometry.page_size());
        let mut rolling = CHECKSUM_FLAG;
        for index in 0..geometry.extent_count() {
            if !checksums && index != 0 && index != lock_index {
                continue;
            }
            let id = manifest.extent(index).ok_or(StoreError::Missing)?;
            self.read_image_extent(id, buffer, work)?;
            if index == 0 {
                crate::apply::validate_sqlite_geometry(
                    &buffer[..id.length() as usize],
                    geometry.page_size(),
                    geometry.length(),
                )?;
            }
            for (within, page) in buffer[..id.length() as usize]
                .chunks_exact(geometry.page_size() as usize)
                .enumerate()
            {
                let pgno = (u64::from(index) * EXTENT_BYTES / u64::from(geometry.page_size()))
                    as u32
                    + within as u32
                    + 1;
                if pgno == lock {
                    if page.iter().any(|byte| *byte != 0) {
                        return Err(ApplyError::Geometry.into());
                    }
                } else if checksums {
                    rolling = CHECKSUM_FLAG | (rolling ^ ltx::checksum_page(pgno, page));
                }
            }
        }
        if checksums && rolling != manifest.binding().checksum {
            return Err(ApplyError::Checksum.into());
        }
        Ok(())
    }

    /// Replay bounded command files into new immutable extents. The base image
    /// capability belongs to this exact node owner. Unchanged extents are shared;
    /// neither root bytes nor an image filename can substitute for that base.
    /// The caller admits the 4 MiB workspace plus the configured decoded-file
    /// budget. Failed preparation can leave charged, unreferenced extent files;
    /// ordinary reconciled GC owns their reclamation, never a speculative refund.
    pub fn prepare_image_root(
        &self,
        base: Option<&SealedImageRoot>,
        update: &ImageRootUpdate<'_>,
        limits: &ApplyLimits,
        buffer: &mut [u8],
    ) -> Result<PreparedImageRoot, StoreError> {
        check_buffer(buffer)?;
        let mut current = match base {
            Some(base) => {
                if !Arc::ptr_eq(&self.inner, &base.view().inner) {
                    return Err(ApplyError::MissingBase.into());
                }
                base.manifest().clone()
            }
            None => ExtentManifest::new(
                ImageBinding {
                    txid: 0,
                    checksum: 0,
                    ..update.binding
                },
                Geometry::new(0, 0)?,
                &[],
            )?,
        };
        preflight(&current, update, limits)?;
        let operation = self.begin()?;
        let result = (|| {
            let mut work = RootApplyWork::default();
            let mut rolling =
                (current.binding().checksum != 0).then_some(current.binding().checksum);
            let mut actual_pages = 0usize;
            let mut actual_decoded = 0u64;
            for segment in &update.segments {
                let mut decode = limits.decode;
                decode.max_pages = decode
                    .max_pages
                    .min(segment.page_count)
                    .min(
                        limits
                            .max_total_pages
                            .checked_sub(actual_pages)
                            .ok_or(ApplyError::Limit)?,
                    )
                    .max(1);
                decode.max_decoded_bytes = decode
                    .max_decoded_bytes
                    .min(segment.decoded_bytes)
                    .min(
                        limits
                            .max_total_decoded_bytes
                            .checked_sub(actual_decoded)
                            .ok_or(ApplyError::Limit)?,
                    )
                    .max(1);
                let file =
                    ltx::decode_file_bounded(segment.bytes, decode).map_err(ApplyError::from)?;
                if file.pages().len() != segment.page_count
                    || file.decoded_bytes() != segment.decoded_bytes
                {
                    return Err(ApplyError::Declaration.into());
                }
                actual_pages = actual_pages
                    .checked_add(file.pages().len())
                    .ok_or(ApplyError::Limit)?;
                actual_decoded = actual_decoded
                    .checked_add(file.decoded_bytes())
                    .ok_or(ApplyError::Limit)?;
                current =
                    self.apply_image_file(&current, &file, buffer, &mut rolling, &mut work)?;
            }
            if current.binding() != update.binding
                || current.geometry() != update.geometry
                || reference(&current)? != update.result
            {
                return Err(StoreError::Integrity);
            }
            // Header/hole checks are independent of the proposed commitment.
            // A checksum-enabled result has already been checked at each file.
            self.check_image_geometry(&current, buffer, &mut work)?;
            Ok(PreparedImageRoot {
                image: SealedImageRoot(self.seal_root_inner(current)?),
                work,
            })
        })();
        operation.finish()?;
        result
    }

    fn check_image_geometry(
        &self,
        manifest: &ExtentManifest,
        buffer: &mut [u8],
        work: &mut RootApplyWork,
    ) -> Result<(), StoreError> {
        let binding = ImageBinding {
            checksum: 0,
            ..manifest.binding()
        };
        let geometry_only = manifest.with_updates(binding, manifest.geometry(), &[])?;
        self.check_image(&geometry_only, buffer, work)
    }

    fn apply_image_file(
        &self,
        before: &ExtentManifest,
        file: &ltx::BoundedLtx,
        buffer: &mut [u8],
        rolling: &mut Option<u64>,
        work: &mut RootApplyWork,
    ) -> Result<ExtentManifest, StoreError> {
        let header = file.header();
        let geometry = Geometry::new(
            header.page_size,
            u64::from(header.commit) * u64::from(header.page_size),
        )?;
        let old = before.geometry();
        let old_count = page_count(old);
        let lock = ltx::lock_pgno(header.page_size);
        if header.is_snapshot() {
            *rolling = (!header.no_checksum()).then_some(CHECKSUM_FLAG);
        }
        if !header.no_checksum() && rolling.is_none() {
            let mut value = CHECKSUM_FLAG;
            for index in 0..old.extent_count() {
                let id = before.extent(index).ok_or(StoreError::Missing)?;
                self.read_image_extent(id, buffer, work)?;
                for (within, page) in buffer[..id.length() as usize]
                    .chunks_exact(header.page_size as usize)
                    .enumerate()
                {
                    let pgno = (u64::from(index) * EXTENT_BYTES / u64::from(header.page_size))
                        as u32
                        + within as u32
                        + 1;
                    if pgno != lock {
                        value = CHECKSUM_FLAG | (value ^ ltx::checksum_page(pgno, page));
                    }
                }
            }
            *rolling = Some(value);
        }
        if !header.no_checksum()
            && !header.is_snapshot()
            && *rolling != Some(header.pre_apply_checksum)
        {
            return Err(ApplyError::Checksum.into());
        }
        let mut next = old_count + 1;
        for (page, _) in file.pages().iter().filter(|(page, _)| *page > old_count) {
            if next == lock {
                next += 1;
            }
            if *page != next {
                return Err(ApplyError::MissingPage.into());
            }
            next += 1;
        }
        if next == lock {
            next += 1;
        }
        if next <= header.commit {
            return Err(ApplyError::MissingPage.into());
        }
        let mut touched: BTreeSet<_> = file
            .pages()
            .iter()
            .map(|(page, _)| extent_index(*page, header.page_size))
            .collect();
        if let Some(boundary) = old
            .extent_count()
            .min(geometry.extent_count())
            .checked_sub(1)
        {
            if old.extent_length(boundary) != geometry.extent_length(boundary) {
                touched.insert(boundary);
            }
        }
        if rolling.is_some() && !header.is_snapshot() {
            touched.extend(geometry.extent_count()..old.extent_count());
        }
        let mut updates = Vec::with_capacity(touched.len());
        for index in touched {
            let old_length = old.extent_length(index).unwrap_or(0) as usize;
            let length = geometry.extent_length(index).unwrap_or(0) as usize;
            if old_length != 0 && !header.is_snapshot() {
                self.read_image_extent(
                    before.extent(index).ok_or(StoreError::Missing)?,
                    buffer,
                    work,
                )?;
            } else {
                buffer.fill(0);
            }
            if length > old_length {
                buffer[old_length..length].fill(0);
            }
            let pages = file.pages();
            let start =
                pages.partition_point(|(page, _)| extent_index(*page, header.page_size) < index);
            let end =
                pages.partition_point(|(page, _)| extent_index(*page, header.page_size) <= index);
            let first_page =
                (u64::from(index) * EXTENT_BYTES / u64::from(header.page_size)) as u32 + 1;
            if let Some(value) = rolling.as_mut() {
                if !header.is_snapshot() {
                    let last_old = first_page + old_length as u32 / header.page_size;
                    for pgno in header.commit.saturating_add(1).max(first_page)..last_old {
                        if pgno != lock {
                            let at = (pgno - first_page) as usize * header.page_size as usize;
                            *value = CHECKSUM_FLAG
                                | (*value
                                    ^ ltx::checksum_page(
                                        pgno,
                                        &buffer[at..at + header.page_size as usize],
                                    ));
                        }
                    }
                    for (pgno, _) in &pages[start..end] {
                        if *pgno <= old_count {
                            let at = (*pgno - first_page) as usize * header.page_size as usize;
                            *value = CHECKSUM_FLAG
                                | (*value
                                    ^ ltx::checksum_page(
                                        *pgno,
                                        &buffer[at..at + header.page_size as usize],
                                    ));
                        }
                    }
                }
            }
            for (pgno, page) in &pages[start..end] {
                let at = (*pgno - first_page) as usize * header.page_size as usize;
                buffer
                    .get_mut(at..at + page.len())
                    .filter(|_| at + page.len() <= length)
                    .ok_or(ApplyError::Geometry)?
                    .copy_from_slice(page);
                if let Some(value) = rolling.as_mut() {
                    *value = CHECKSUM_FLAG | (*value ^ ltx::checksum_page(*pgno, page));
                }
            }
            if length == 0 {
                continue;
            }
            let id = ExtentId::of(&buffer[..length])?;
            let exists = self.inner.state()?.objects.contains_key(&id);
            self.put_extent_inner(id, &mut |offset, count| {
                Ok(buffer[offset as usize..offset as usize + count].to_vec())
            })?;
            work.extents_prepared = work
                .extents_prepared
                .checked_add(1)
                .ok_or(StoreError::Limit)?;
            if !exists {
                work.introduced_extent_bytes = work
                    .introduced_extent_bytes
                    .checked_add(length as u64)
                    .ok_or(StoreError::Limit)?;
            }
            updates.push((index, id));
        }
        let checksum = file.trailer().post_apply_checksum;
        if header.no_checksum() && checksum != 0
            || !header.no_checksum() && *rolling != Some(checksum)
        {
            return Err(ApplyError::Checksum.into());
        }
        let manifest = before.with_updates(
            ImageBinding {
                txid: header.max_txid.0,
                checksum,
                ..before.binding()
            },
            geometry,
            &updates,
        )?;
        self.check_image_geometry(&manifest, buffer, work)?;
        Ok(manifest)
    }
}

fn preflight(
    base: &ExtentManifest,
    update: &ImageRootUpdate<'_>,
    limits: &ApplyLimits,
) -> Result<(), StoreError> {
    limits.validate()?;
    if base.geometry().length() > limits.decode.max_image_bytes {
        return Err(ApplyError::Limit.into());
    }
    update.binding.validate(update.geometry)?;
    if reference(base)? != update.base
        || base.binding().history != update.binding.history
        || base.binding().schema != update.binding.schema
        || base.binding().epoch != update.binding.epoch
    {
        return Err(ApplyError::Identity.into());
    }
    if update.segments.is_empty() || update.segments.len() > limits.max_files {
        return Err(ApplyError::Limit.into());
    }
    let mut txid = base.binding().txid;
    let mut page_size = base.geometry().page_size();
    let mut final_geometry = base.geometry();
    let (mut encoded, mut decoded, mut pages) = (0usize, 0u64, 0usize);
    for segment in &update.segments {
        if txid.checked_add(1) != Some(segment.min_txid.0) || segment.max_txid < segment.min_txid {
            return Err(ApplyError::Chain.into());
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
            return Err(ApplyError::Limit.into());
        }
        let header = ltx::Header::parse(segment.bytes)
            .map_err(ltx::DecodeError::from)
            .map_err(ApplyError::from)?;
        header
            .validate()
            .map_err(ltx::DecodeError::from)
            .map_err(ApplyError::from)?;
        if header.min_txid != segment.min_txid || header.max_txid != segment.max_txid {
            return Err(ApplyError::Declaration.into());
        }
        if page_size == 0 {
            page_size = header.page_size;
        }
        if page_size != header.page_size {
            return Err(ApplyError::Geometry.into());
        }
        final_geometry = Geometry::new(
            header.page_size,
            u64::from(header.commit) * u64::from(header.page_size),
        )?;
        if final_geometry.length() > limits.decode.max_image_bytes {
            return Err(ApplyError::Limit.into());
        }
    }
    if txid != update.binding.txid || final_geometry != update.geometry {
        return Err(ApplyError::Chain.into());
    }
    Ok(())
}
