// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{checkpoint_packing as packing, objects::Object, *};
use futures_util::{stream::FuturesOrdered, StreamExt};
use std::io::Write;

const CHECKPOINT_READ_WINDOW: usize = 2;
const CHECKPOINT_WRITE_WINDOW: usize = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Index {
    domain: String,
    pub marker: GroupManifest,
    bytes: u64,
    pub chunks: Vec<Chunk>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Chunk {
    parts: Vec<packing::Fragment>,
    pub object: Object,
}

impl Index {
    fn validate(&self, archive: &CellGroupArchive) -> Result<()> {
        self.marker
            .check()
            .map_err(CellGroupCheckpointError::from)?;
        if self.domain != DOMAIN || self.marker.route().history != archive.history {
            return Err(CellGroupArchiveError::Invalid("checkpoint domain/history"));
        }
        if self.chunks.is_empty()
            || self.chunks.len() > archive.limits.max_chunks
            || self.bytes > archive.limits.max_checkpoint_bytes
        {
            return Err(CellGroupArchiveError::Limit);
        }
        let mut total = 0u64;
        let mut count = 0usize;
        let mut previous: Option<&packing::Fragment> = None;
        for chunk in &self.chunks {
            if chunk.object.length == 0
                || chunk.object.length > CHUNK_BYTES as u64
                || chunk.parts.is_empty()
            {
                return Err(CellGroupArchiveError::Limit);
            }
            count = count
                .checked_add(chunk.parts.len())
                .filter(|n| *n <= archive.limits.max_chunks)
                .ok_or(CellGroupArchiveError::Limit)?;
            let mut object_bytes = 0u64;
            for part in &chunk.parts {
                if part.bytes == 0
                    || part.bytes > CHUNK_BYTES as u64
                    || part.length == 0
                    || part.length > archive.limits.max_checkpoint_bytes
                {
                    return Err(CellGroupArchiveError::Limit);
                }
                part.offset
                    .checked_add(part.bytes)
                    .filter(|n| *n <= part.length)
                    .ok_or(CellGroupArchiveError::Invalid("chunk bounds"))?;
                if let Some(before) = previous {
                    let before_end = before.offset + before.bytes;
                    if before_end < before.length {
                        if part.part != before.part
                            || part.length != before.length
                            || part.offset != before_end
                        {
                            return Err(CellGroupArchiveError::Invalid("chunk continuity"));
                        }
                    } else if part.offset != 0 {
                        return Err(CellGroupArchiveError::Invalid("part offset"));
                    }
                } else if part.part != wire::Part::Header || part.offset != 0 {
                    return Err(CellGroupArchiveError::Invalid("first archive part"));
                }
                object_bytes = object_bytes
                    .checked_add(part.bytes)
                    .filter(|n| *n <= chunk.object.length)
                    .ok_or(CellGroupArchiveError::Invalid("packed object bounds"))?;
                previous = Some(part);
            }
            if object_bytes != chunk.object.length {
                return Err(CellGroupArchiveError::Invalid("packed object length"));
            }
            total = total
                .checked_add(object_bytes)
                .ok_or(CellGroupArchiveError::Limit)?;
        }
        let last = previous.ok_or(CellGroupArchiveError::Invalid("empty archive"))?;
        if total != self.bytes || last.offset + last.bytes != last.length {
            return Err(CellGroupArchiveError::Invalid("incomplete archive index"));
        }
        Ok(())
    }
}

pub(super) struct Bounded {
    pub(super) bytes: Vec<u8>,
    pub(super) limit: usize,
}

impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(std::io::Error::other("archive index limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl CellGroupArchiveUpload {
    /// Fetch the exact checkpoint protected by this still-pending upload and
    /// its inherited parent graph. This streams data only: the caller must use
    /// a physical receiver and a separately authenticated live install grant.
    pub(crate) async fn download_seed_checkpoint<F, Fut>(
        &self,
        domain: &impl CellGroupArchiveExecution,
        reference: &CellGroupArchiveReference,
        mut sink: F,
    ) -> Result<()>
    where
        F: FnMut(wire::Chunk) -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        domain
            .run(async {
                let permit = self.admit()?;
                self.registry().pending(&self.ticket()).await?;
                self.allows_object(super::graph::Kind::Indexes, &reference.object())?;
                let index = self
                    .checkpoint_index(domain, reference, permit.clone())
                    .await?;
                for chunk in index.chunks {
                    self.registry().pending(&self.ticket()).await?;
                    self.allows_object(super::graph::Kind::Chunks, &chunk.object)?;
                    let bytes = self.get("chunks", &chunk.object, CHUNK_BYTES).await?;
                    for part in packing::unpack(chunk.parts, bytes)? {
                        // A packed object never substitutes for the live upload
                        // hold. Preserve the original per-fragment fence.
                        self.registry().pending(&self.ticket()).await?;
                        sink(part).await?;
                    }
                }
                self.registry().pending(&self.ticket()).await?;
                Ok(())
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Capture is supplied by the caller's node checkpoint pool. The bounded
    /// producer keeps the full source pinned through physical reads. Losing the
    /// async caller closes its channel; domain shutdown joins the physical owner.
    /// Success confirms immutable bytes, without granting group coverage.
    pub async fn upload_checkpoint(
        &self,
        domain: &impl CellGroupArchiveExecution,
        source: CellGroupCheckpoint,
    ) -> Result<CellGroupArchiveReference> {
        domain
            .run(self.upload(domain, source))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn upload(
        &self,
        domain: &impl CellGroupArchiveExecution,
        mut source: CellGroupCheckpoint,
    ) -> Result<CellGroupArchiveReference> {
        if source.marker().route().history != self.history {
            return Err(CellGroupArchiveError::Invalid("source history"));
        }
        let permit = self.admit()?;
        self.registry().pending(&self.ticket()).await?;
        let retained = permit.clone();
        let mut index = Index {
            domain: DOMAIN.into(),
            marker: source.marker().clone(),
            bytes: 0,
            chunks: Vec::new(),
        };
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let limits = self.limits;
        let producer = domain
            .blocking(move || -> Result<()> {
                let _retained = retained;
                let mut packer =
                    packing::Packer::new(limits.max_chunks, limits.max_checkpoint_bytes);
                let mut emit = |packed| {
                    sender.blocking_send(packed).map_err(|_| {
                        CellGroupCheckpointError::Sink(std::io::Error::other("archive stopped"))
                    })
                };
                let mut packing_limit = false;
                let produced = source.stream(|chunk| {
                    let result = packer.push(wire::Chunk::copy(chunk), &mut emit);
                    packing_limit |= matches!(&result, Err(CellGroupCheckpointError::Limit));
                    result
                });
                if packing_limit {
                    return Err(CellGroupArchiveError::Limit);
                }
                produced?;
                packer.finish(&mut emit)?;
                Ok(())
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?;
        let upload: Result<()> = async {
            enum UploadEvent {
                Source(Option<packing::Packed>),
                Confirmed(Option<Result<Chunk>>),
            }

            // Two creates, their exact GET confirmations, transport copies,
            // producer packer and one-slot channel fit the ten-chunk I/O
            // workspace. Poll writes while waiting for more physical bytes;
            // a slow producer must not stall an already admitted upload.
            let mut writes = FuturesOrdered::new();
            let mut source_done = false;
            let mut failure = None;
            loop {
                let event = if writes.is_empty() {
                    if source_done || failure.is_some() {
                        break;
                    }
                    UploadEvent::Source(receiver.recv().await)
                } else if source_done
                    || failure.is_some()
                    || writes.len() >= CHECKPOINT_WRITE_WINDOW
                {
                    UploadEvent::Confirmed(writes.next().await)
                } else {
                    crate::asyncrt::select_biased! { "completed confirmations retire buffers before more source admission";
                        result = writes.next() => UploadEvent::Confirmed(result),
                        chunk = receiver.recv() => UploadEvent::Source(chunk),
                    }
                };
                match event {
                    UploadEvent::Source(Some(chunk)) => {
                        let bytes = index
                            .bytes
                            .checked_add(chunk.bytes.len() as u64)
                            .filter(|n| *n <= self.limits.max_checkpoint_bytes);
                        if bytes.is_none()
                            || index.chunks.len() + writes.len() >= self.limits.max_chunks
                        {
                            failure = Some(CellGroupArchiveError::Limit);
                            continue;
                        }
                        index.bytes = bytes.ok_or(CellGroupArchiveError::Limit)?;
                        writes.push_back(async move {
                            let object = self.put("chunks", chunk.bytes, CHUNK_BYTES).await?;
                            Ok(Chunk {
                                parts: chunk.parts,
                                object,
                            })
                        });
                    }
                    UploadEvent::Source(None) => source_done = true,
                    UploadEvent::Confirmed(Some(Ok(chunk))) if failure.is_none() => {
                        index.chunks.push(chunk);
                    }
                    UploadEvent::Confirmed(Some(Err(error))) if failure.is_none() => {
                        failure = Some(error);
                    }
                    UploadEvent::Confirmed(Some(_)) => (),
                    UploadEvent::Confirmed(None) => break,
                }
            }
            // Keep the producer and archive admission until entered writes
            // finish, including ambiguous PUTs and their confirmation reads.
            drop(writes);
            failure.map_or(Ok(()), Err)
        }
        .await;
        drop(receiver); // Release a blocked producer before joining any error.
        let produced = producer.await.map_err(CellGroupArchiveError::Worker)?;
        upload?;
        produced?;
        let archive = self.clone();
        let retained = permit.clone();
        let bytes = domain
            .blocking(move || {
                let _retained = retained;
                index.validate(&archive)?;
                let mut output = Bounded {
                    bytes: Vec::new(),
                    limit: archive.limits.max_index_bytes,
                };
                serde_json::to_writer(&mut output, &index)
                    .map_err(|_| CellGroupArchiveError::Limit)?;
                Ok::<_, CellGroupArchiveError>(output.bytes)
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?
            .await
            .map_err(CellGroupArchiveError::Worker)??;
        let object = self
            .put("indexes", bytes, self.limits.max_index_bytes)
            .await?;
        drop(permit);
        Ok(CellGroupArchiveReference {
            registry: object.registry,
            generation: object.generation,
            digest: object.digest,
            length: object.length,
        })
    }
}

impl CellGroupArchive {
    /// Independently GET every object into the ordinary physical receiver. No
    /// source pin or upload receipt substitutes for dependency verification.
    /// A missing/corrupt cell refuses the whole cut. Ordinary errors join the
    /// receiver; cancellation retains its permits until physical work finishes.
    pub async fn verify_checkpoint(
        &self,
        domain: &impl CellGroupArchiveExecution,
        reference: &CellGroupArchiveReference,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<VerifiedCellGroupArchiveCheckpoint> {
        domain
            .run(self.verify(domain, reference, receiver))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn verify(
        &self,
        domain: &impl CellGroupArchiveExecution,
        reference: &CellGroupArchiveReference,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<VerifiedCellGroupArchiveCheckpoint> {
        let permit = self.admit()?;
        self.verify_admitted(domain, reference, receiver, permit)
            .await
    }

    pub(super) async fn checkpoint_marker(
        &self,
        domain: &impl CellGroupArchiveExecution,
        reference: &CellGroupArchiveReference,
        permit: Arc<Permit>,
    ) -> Result<GroupManifest> {
        Ok(self
            .checkpoint_index(domain, reference, permit)
            .await?
            .marker)
    }

    pub(super) async fn checkpoint_index(
        &self,
        domain: &impl CellGroupArchiveExecution,
        reference: &CellGroupArchiveReference,
        permit: Arc<Permit>,
    ) -> Result<Index> {
        let bytes = self
            .get(
                "indexes",
                &Object {
                    registry: reference.registry,
                    generation: reference.generation,
                    digest: reference.digest,
                    length: reference.length,
                },
                self.limits.max_index_bytes,
            )
            .await?;
        let archive = self.clone();
        domain
            .blocking(move || {
                let _permit = permit;
                let index: Index = serde_json::from_slice(&bytes)
                    .map_err(|_| CellGroupArchiveError::Invalid("checkpoint index"))?;
                index.validate(&archive)?;
                Ok::<_, CellGroupArchiveError>(index)
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?
            .await
            .map_err(CellGroupArchiveError::Worker)?
    }

    pub(super) async fn verify_admitted(
        &self,
        domain: &impl CellGroupArchiveExecution,
        reference: &CellGroupArchiveReference,
        mut receiver: CellGroupArchiveCheckpointReceiver,
        permit: Arc<Permit>,
    ) -> Result<VerifiedCellGroupArchiveCheckpoint> {
        let index = self
            .checkpoint_index(domain, reference, permit.clone())
            .await?;
        let marker = index.marker;
        let (sender, mut incoming) = tokio::sync::mpsc::channel::<Vec<wire::Chunk>>(1);
        let retained = permit.clone();
        let consumer = domain
            .blocking(move || {
                let _retained = retained;
                while let Some(chunks) = incoming.blocking_recv() {
                    for chunk in chunks {
                        chunk.append(&mut receiver)?;
                    }
                }
                receiver.finish()
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?;
        let download: Result<()> = async {
            // The ten-chunk workspace covers two GETs (including
            // transport copies), an ordered result, the one-slot channel and
            // the physical consumer and unpacking copies. FuturesOrdered bounds both in-flight and
            // completed reads while preserving the validated descriptor order.
            let mut remaining = index.chunks.into_iter();
            let mut reads = FuturesOrdered::new();
            let mut failure = None;
            let mut receiving = true;
            loop {
                while failure.is_none() && receiving && reads.len() < CHECKPOINT_READ_WINDOW {
                    let Some(chunk) = remaining.next() else {
                        break;
                    };
                    reads.push_back(async move {
                        let bytes = self.get("chunks", &chunk.object, CHUNK_BYTES).await?;
                        packing::unpack(chunk.parts, bytes).map_err(CellGroupArchiveError::from)
                    });
                }
                let Some(result) = reads.next().await else {
                    break;
                };
                match result {
                    Ok(chunk) if failure.is_none() && receiving => {
                        // Preserve the consumer's typed checkpoint failure.
                        receiving = sender.send(chunk).await.is_ok();
                    }
                    Err(error) if failure.is_none() && receiving => failure = Some(error),
                    _ => (),
                }
            }
            // A download or consumer failure stops admission, but already
            // entered reads must finish before their buffers/permits are freed.
            drop(reads);
            failure.map_or(Ok(()), Err)
        }
        .await;
        drop(sender);
        let received = consumer.await.map_err(CellGroupArchiveError::Worker)?;
        download?;
        let checkpoint = received?;
        if checkpoint.marker() != &marker {
            return Err(CellGroupArchiveError::Invalid("archive checkpoint binding"));
        }
        Ok(VerifiedCellGroupArchiveCheckpoint {
            reference: reference.clone(),
            checkpoint,
        })
    }
}
