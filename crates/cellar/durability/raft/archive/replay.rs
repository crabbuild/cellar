// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replay every entry proof while sealing bounded batches of private images.
use super::{result_state, ArchiveError, ArchiveRecoverySet};
use crate::application::consensus::CellUpdate;
use cellar_ltx::apply::{ApplyLimits, ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationPin, GenerationStore, ImageId};
use cellar_ltx::TXID;
use sha2::{Digest, Sha256};

// Decoded envelopes have their own existing per-entry bound. Small entries
// share this window; a larger valid entry is replayed alone. The snapshot is
// borrowed from the recovery set and does not consume this additional buffer.
const MAX_REPLAY_BYTES: usize = 4 * 1024 * 1024;
const MAX_REPLAY_ENTRIES: usize = 64;

#[derive(Default, Clone, Copy)]
struct Usage {
    files: usize,
    encoded: usize,
    decoded: u64,
    pages: usize,
}

impl Usage {
    fn add(
        self,
        segments: impl IntoIterator<Item = (usize, u64, usize)>,
    ) -> Result<Self, ArchiveError> {
        let mut next = self;
        for (encoded, decoded, pages) in segments {
            next.files = next.files.checked_add(1).ok_or(ArchiveError::Limit)?;
            next.encoded = next
                .encoded
                .checked_add(encoded)
                .ok_or(ArchiveError::Limit)?;
            next.decoded = next
                .decoded
                .checked_add(decoded)
                .ok_or(ArchiveError::Limit)?;
            next.pages = next.pages.checked_add(pages).ok_or(ArchiveError::Limit)?;
        }
        Ok(next)
    }

    fn fits(self, limits: ApplyLimits) -> bool {
        self.files <= limits.max_files
            && self.encoded <= limits.max_total_encoded_bytes
            && self.decoded <= limits.max_total_decoded_bytes
            && self.pages <= limits.max_total_pages
    }
}

struct Update {
    envelope: CellUpdate,
    base: ImageState,
    result: ImageState,
}

#[derive(Default)]
struct Batch<'a> {
    snapshot: Option<&'a cellar_ltx::stream_snapshot::SnapshotFile>,
    entries: Vec<Update>,
    retained: usize,
    usage: Usage,
}

impl Batch<'_> {
    fn is_empty(&self) -> bool {
        self.snapshot.is_none() && self.entries.is_empty()
    }

    fn flush(
        &mut self,
        images: &GenerationStore,
        nonce: ImageId,
        sequence: &mut u64,
        pin: &mut Option<GenerationPin>,
        activity: Option<cellar_ltx::host::IoActivity>,
    ) -> Result<(), ArchiveError> {
        if self.is_empty() {
            return Ok(());
        }
        let id = image_id(nonce, sequence)?;
        let mut updates = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            updates.push(ImageUpdate {
                base: entry.base,
                result: entry.result,
                segments: entry
                    .envelope
                    .files
                    .iter()
                    .map(|file| LtxSegment {
                        bytes: &file.bytes,
                        min_txid: TXID(file.min_txid),
                        max_txid: TXID(file.max_txid),
                        page_count: file.page_count as usize,
                        decoded_bytes: file.decoded_bytes,
                    })
                    .collect(),
            });
        }
        let next = match self.snapshot {
            Some(snapshot) => {
                images.prepare_snapshot_chain_with_activity(id, snapshot, &updates, activity)
            }
            None => images.prepare_chain_with_activity(id, pin.as_ref(), &updates, activity),
        }
        .map_err(ArchiveError::Images)?;
        if let Some(previous) = pin.take() {
            images.retire(previous.id()).map_err(ArchiveError::Images)?;
            drop(previous);
            images.reclaim(8).map_err(ArchiveError::Images)?;
        }
        *pin = Some(next);
        drop(updates);
        self.snapshot = None;
        self.entries.clear();
        self.retained = 0;
        self.usage = Usage::default();
        Ok(())
    }
}

impl ArchiveRecoverySet {
    /// Run on the owning blocking domain with an exclusively opened destination.
    /// A failed restore leaves no activation authority. Its retained temporary
    /// images may be inspected or removed by that directory's recovery owner.
    pub fn restore(
        &self,
        images: &GenerationStore,
        nonce: ImageId,
    ) -> Result<GenerationPin, ArchiveError> {
        self.restore_state(images, nonce)?
            .ok_or(ArchiveError::Invalid(
                "canonical empty archive has no SQL image",
            ))
    }

    /// Restore a verified selected state. Canonical empty has no SQL image.
    pub fn restore_state(
        &self,
        images: &GenerationStore,
        nonce: ImageId,
    ) -> Result<Option<GenerationPin>, ArchiveError> {
        let Some(snapshot_image) = self.manifest.snapshot.image else {
            self.check_payloads()?;
            return Ok(None);
        };
        let mut state = snapshot_image.decode()?;
        let mut sequence = 0;
        let mut pin = None;
        let mut batch = Batch {
            snapshot: Some(
                self.snapshot
                    .as_ref()
                    .ok_or(ArchiveError::Corrupt("missing streamed snapshot"))?,
            ),
            usage: Usage {
                files: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let limits = images.apply_limits();
        for entry in &self.entries {
            // The recovery-set constructor already replayed and matched every
            // configuration and member binding. They change no SQLite pages.
            if entry.get_entry_type() != ::raft::prelude::EntryType::EntryNormal
                || entry.data.is_empty()
            {
                continue;
            }
            // Flush before decoding the next envelope, so a large entry cannot
            // retain another batch's copied payloads alongside its own bound.
            if !batch.is_empty()
                && (batch.entries.len() >= MAX_REPLAY_ENTRIES
                    || batch
                        .retained
                        .checked_add(entry.data.len())
                        .is_none_or(|bytes| bytes > MAX_REPLAY_BYTES))
            {
                batch.flush(
                    images,
                    nonce,
                    &mut sequence,
                    &mut pin,
                    self.activity.clone(),
                )?;
            }
            let update = self.update(entry, state)?;
            let result = result_state(&update)?;
            let usage = batch.usage.add(
                update
                    .files
                    .iter()
                    .map(|f| (f.bytes.len(), f.decoded_bytes, f.page_count as usize)),
            )?;
            if !batch.is_empty() && !usage.fits(limits) {
                batch.flush(
                    images,
                    nonce,
                    &mut sequence,
                    &mut pin,
                    self.activity.clone(),
                )?;
            }
            batch.usage = batch.usage.add(
                update
                    .files
                    .iter()
                    .map(|f| (f.bytes.len(), f.decoded_bytes, f.page_count as usize)),
            )?;
            batch.retained = batch
                .retained
                .checked_add(entry.data.len())
                .ok_or(ArchiveError::Limit)?;
            batch.entries.push(Update {
                envelope: update,
                base: state,
                result,
            });
            // A later growth must reserve against the smaller pinned base,
            // keeping the disk budget no larger than entry-by-entry replay.
            if result.length < state.length {
                batch.flush(
                    images,
                    nonce,
                    &mut sequence,
                    &mut pin,
                    self.activity.clone(),
                )?;
            }
            state = result;
        }
        batch.flush(
            images,
            nonce,
            &mut sequence,
            &mut pin,
            self.activity.clone(),
        )?;
        let pin = pin.ok_or(ArchiveError::Corrupt("missing restored archive image"))?;
        if Some(super::super::manifest::Image::from(pin.state())) != self.manifest.image {
            return Err(ArchiveError::Corrupt("restored archive image digest"));
        }
        Ok(Some(pin))
    }
}

fn image_id(nonce: ImageId, sequence: &mut u64) -> Result<ImageId, ArchiveError> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-archive-image-1");
    hash.update(nonce.bytes());
    hash.update(sequence.to_be_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    *sequence = sequence.checked_add(1).ok_or(ArchiveError::Limit)?;
    ImageId::new(
        digest[..16]
            .try_into()
            .map_err(|_| ArchiveError::Corrupt("archive image identity"))?,
    )
    .map_err(ArchiveError::Images)
}
