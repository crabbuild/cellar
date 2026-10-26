// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical snapshots use immutable bounded parts and a small authenticated
//! index. Conditional create and read-back proofs cover every part before the
//! index can be referenced by a conditionally published cell head.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::stream::FuturesOrdered;

pub(super) const INLINE_BYTES: usize = 64 * 1024;
const PART_BYTES: usize = 4 * 1024 * 1024;

// Keep source/hash reads ordered, while overlapping conditional PUT and exact
// confirmation of two independent parts. Producer admission bounds the number
// of these windows across the node; no task is spawned per part.
const UPLOAD_WINDOW: usize = 2;
const MAX_PARTS: usize = 4096;
const MAX_INDEX_BYTES: usize = 1024 * 1024;
pub(super) const READ_BYTES: usize = READ_WINDOW * PART_BYTES + 2 * 64 * 1024;

// Each upload holds either its owned PUT buffer or a bounded GET buffer plus
// one response chunk. Reuse the existing physical working-memory allowance.
const _: () = assert!(2 * UPLOAD_WINDOW <= READ_WINDOW);
const _: () = assert!(stream_snapshot::IoProfile::Buffered.max_working_bytes() <= READ_BYTES);
const _: () = assert!(PART_BYTES <= cellar_ltx::host::MAX_IO_WINDOW_BYTES);

pub(super) fn io_profile(image_bytes: u64) -> stream_snapshot::IoProfile {
    if image_bytes >= 256 * 1024 * 1024 {
        stream_snapshot::IoProfile::Buffered
    } else {
        stream_snapshot::IoProfile::Compact
    }
}

/// Exact read-back confirmations bind every stored part/index to this local
/// encoded stream. Retaining it avoids another download of the same bytes;
/// ordinary physical decoding and independent destination replay still run.
pub(super) struct ConfirmedSnapshot {
    object: ObjectRef,
    source: stream_snapshot::SnapshotFile,
}

impl ConfirmedSnapshot {
    pub fn reference(&self) -> &ObjectRef {
        &self.object
    }

    pub fn bind(self, expected: &Snapshot) -> Result<stream_snapshot::SnapshotFile, ArchiveError> {
        if Some(&self.object) != expected.object.as_ref()
            || Some(self.source.metadata().image)
                != expected.image.map(Image::decode).transpose()?
        {
            return Err(ArchiveError::Corrupt("confirmed snapshot binding"));
        }
        Ok(self.source)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Index {
    length: u64,
    digest: [u8; 32],
    parts: Vec<Part>,
    inline: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Part {
    length: u32,
    digest: [u8; 32],
}

impl Part {
    fn object(&self) -> ObjectRef {
        ObjectRef {
            kind: ObjectKind::SnapshotPart,
            digest: self.digest,
            length: u64::from(self.length),
            range: None,
            bundle: None,
        }
    }
}

impl CellArchive {
    pub(super) async fn put_snapshot(
        &self,
        snapshot: stream_snapshot::SnapshotFile,
        inline: Option<Vec<u8>>,
        position: Position,
    ) -> Result<ConfirmedSnapshot, ArchiveError> {
        let metadata = snapshot.metadata();
        let part_bytes = PART_BYTES.min(self.limits.max_object_bytes);
        if metadata.encoded_length > self.limits.max_snapshot_bytes
            || metadata.encoded_length.div_ceil(part_bytes as u64) > MAX_PARTS as u64
        {
            return Err(ArchiveError::Limit);
        }
        let mut index = Index {
            length: metadata.encoded_length,
            digest: metadata.encoded_digest,
            parts: Vec::new(),
            inline: String::new(),
        };
        if let Some(bytes) = inline {
            if bytes.len() > INLINE_BYTES
                || bytes.len() as u64 != metadata.encoded_length
                || manifest::digest(&bytes) != metadata.encoded_digest
            {
                return Err(ArchiveError::Corrupt("inline snapshot identity"));
            }
            index.inline = STANDARD.encode(bytes);
            let body = manifest::encode(4, &index, MAX_INDEX_BYTES)?;
            let object = self
                .put_object(
                    ObjectKind::Snapshot,
                    &body,
                    Some(PositionRange {
                        first: position,
                        last: position,
                    }),
                )
                .await?;
            return Ok(ConfirmedSnapshot {
                object,
                source: snapshot,
            });
        }
        let mut offset = 0;
        let mut uploaded_hash = Sha256::new();
        let mut pending = FuturesOrdered::new();
        let mut failure = None;
        loop {
            while failure.is_none()
                && pending.len() < UPLOAD_WINDOW
                && offset < metadata.encoded_length
            {
                let source = snapshot.clone();
                let length = (metadata.encoded_length - offset).min(part_bytes as u64) as usize;
                // The upload owns this buffer. Hash it on the same blocking
                // job that reads it, avoiding object_reference/put_object's
                // additional complete-part copies.
                let hash = uploaded_hash.clone();
                let read = self
                    .host
                    .run_blocking(move || {
                        let mut bytes = vec![0; length];
                        source.read_into(offset, &mut bytes)?;
                        let mut hash = hash;
                        hash.update(&bytes);
                        let object = ObjectRef::new(ObjectKind::SnapshotPart, &bytes, None);
                        Ok::<_, stream_snapshot::Error>((bytes, object, hash))
                    })
                    .await
                    .map_err(ArchiveError::Worker)
                    .and_then(|result| result.map_err(ArchiveError::Physical));
                match read {
                    Ok((bytes, object, hash)) => {
                        uploaded_hash = hash;
                        pending.push_back(self.put_direct(object, bytes));
                        offset += length as u64;
                    }
                    Err(error) => failure = Some(error),
                }
            }
            let Some(result) = pending.next().await else {
                break;
            };
            match result {
                Ok(object) => index.parts.push(Part {
                    length: object.length as u32,
                    digest: object.digest,
                }),
                Err(error) if failure.is_none() => failure = Some(error),
                Err(_) => (),
            }
            // On failure, stop admitting parts but drain the entire entered
            // window. Its exact confirmations and owned blocking jobs retain
            // their buffers/source until completion; no index/head is exposed.
        }
        if let Some(error) = failure {
            return Err(error);
        }
        // The actual uploaded stream must match the verified local source.
        // Per-part confirmations alone cannot detect a transient source read
        // fault that would otherwise make a different, valid part identity.
        if <[u8; 32]>::from(uploaded_hash.finalize()) != metadata.encoded_digest {
            return Err(ArchiveError::Corrupt("uploaded snapshot stream digest"));
        }
        let body = manifest::encode(4, &index, MAX_INDEX_BYTES)?;
        let object = self
            .put_object(
                ObjectKind::Snapshot,
                &body,
                Some(PositionRange {
                    first: position,
                    last: position,
                }),
            )
            .await?;
        Ok(ConfirmedSnapshot {
            object,
            source: snapshot,
        })
    }

    /// A protocol-only cut can name the same complete image under a new exact
    /// position/configuration. Reuse only a fully downloaded/validated prior
    /// snapshot whose complete image state equals the new durable cut.
    pub(super) async fn rebind_snapshot(
        &self,
        previous: ArchiveRecoverySet,
        image: ImageState,
        position: Position,
    ) -> Result<ConfirmedSnapshot, ArchiveError> {
        let source = previous
            .snapshot
            .ok_or(ArchiveError::Invalid("snapshot rebind needs a stream"))?;
        if previous.manifest.snapshot.image != Some(Image::from(image))
            || previous.manifest.image != Some(Image::from(image))
            || source.metadata().image != image
            || previous.position.index.get() >= position.index
            || previous.position.term.get() > position.term
        {
            return Err(ArchiveError::Invalid("snapshot rebind image/position"));
        }
        let bytes = self
            .get_object(
                previous
                    .manifest
                    .snapshot
                    .object
                    .as_ref()
                    .ok_or(ArchiveError::Corrupt("snapshot rebind index"))?,
            )
            .await?;
        let index: Index = manifest::decode(4, &bytes, MAX_INDEX_BYTES)?;
        let metadata = source.metadata();
        if index.length != metadata.encoded_length || index.digest != metadata.encoded_digest {
            return Err(ArchiveError::Corrupt("snapshot rebind stream binding"));
        }
        let object = self
            .put_object(
                ObjectKind::Snapshot,
                &bytes,
                Some(PositionRange {
                    first: position,
                    last: position,
                }),
            )
            .await?;
        Ok(ConfirmedSnapshot { object, source })
    }

    pub(super) async fn get_snapshot(
        &self,
        snapshot: &Snapshot,
        bytes: Vec<u8>,
    ) -> Result<stream_snapshot::SnapshotFile, ArchiveError> {
        let index: Index = manifest::decode(4, &bytes, MAX_INDEX_BYTES)?;
        if index.length == 0
            || index.length > self.limits.max_snapshot_bytes
            || (index.parts.is_empty() == index.inline.is_empty())
            || index.parts.len() > MAX_PARTS
        {
            return Err(ArchiveError::Limit);
        }
        let inline = if index.inline.is_empty() {
            Vec::new()
        } else {
            if index.inline.len() > INLINE_BYTES.div_ceil(3) * 4 {
                return Err(ArchiveError::Limit);
            }
            let bytes = STANDARD
                .decode(&index.inline)
                .map_err(|_| ArchiveError::Corrupt("snapshot inline encoding"))?;
            if bytes.len() > INLINE_BYTES || STANDARD.encode(&bytes) != index.inline {
                return Err(ArchiveError::Corrupt("snapshot inline canonical encoding"));
            }
            bytes
        };
        let mut total = inline.len() as u64;
        for part in &index.parts {
            if part.length == 0
                || part.length as usize > PART_BYTES.min(self.limits.max_object_bytes)
            {
                return Err(ArchiveError::Limit);
            }
            total = total
                .checked_add(u64::from(part.length))
                .ok_or(ArchiveError::Limit)?;
        }
        if total != index.length {
            return Err(ArchiveError::Corrupt("snapshot part coverage"));
        }
        let metadata = stream_snapshot::Metadata {
            image: snapshot
                .image
                .ok_or(ArchiveError::Corrupt("snapshot index needs an image"))?
                .decode()?,
            encoded_length: index.length,
            encoded_digest: index.digest,
        };
        let limits = stream_snapshot::Limits {
            max_image_bytes: self.limits.max_image_bytes,
            max_encoded_bytes: self.limits.max_snapshot_bytes,
        };
        let filesystem = self.host.filesystem();
        let scratch_directory = self.scratch_directory.clone();
        let scratch = self.snapshot_scratch.clone();
        let mut download = self
            .host
            .run_blocking(move || {
                stream_snapshot::Download::new_admitted_in(
                    filesystem.as_ref(),
                    scratch_directory.as_deref(),
                    metadata,
                    limits,
                    &scratch,
                )
                .map(|download| download.with_io_profile(io_profile(metadata.image.length)))
            })
            .await
            .map_err(ArchiveError::Worker)?
            .map_err(ArchiveError::Physical)?;
        if !inline.is_empty() {
            return self
                .host
                .run_blocking(move || {
                    download.append(0, &inline)?;
                    download.finish()
                })
                .await
                .map_err(ArchiveError::Worker)?
                .map_err(ArchiveError::Physical);
        }
        let objects: Vec<_> = index.parts.iter().map(Part::object).collect();
        let pending: Vec<_> = objects
            .iter()
            .map(|object| self.get_object(object))
            .collect();
        let mut reads = futures_util::stream::iter(pending).buffered(READ_WINDOW);
        let mut offset = 0_u64;
        while let Some(bytes) = reads.try_next().await? {
            let length = bytes.len() as u64;
            let retention = self.read_retention.clone();
            download = self
                .host
                .run_blocking(move || {
                    let _retention = retention;
                    download.append_window(offset, &bytes)?;
                    Ok::<_, stream_snapshot::Error>(download)
                })
                .await
                .map_err(ArchiveError::Worker)?
                .map_err(ArchiveError::Physical)?;
            offset += length;
        }
        self.host
            .run_blocking(move || download.finish())
            .await
            .map_err(ArchiveError::Worker)?
            .map_err(ArchiveError::Physical)
    }
}
