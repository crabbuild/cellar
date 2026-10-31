// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{objects::Object, *};
use std::io::Write;

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
    part: wire::Part,
    offset: u64,
    length: u64,
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
        let mut previous: Option<&Chunk> = None;
        for chunk in &self.chunks {
            if chunk.object.length == 0
                || chunk.object.length > CHUNK_BYTES as u64
                || chunk.length == 0
                || chunk.length > archive.limits.max_checkpoint_bytes
            {
                return Err(CellGroupArchiveError::Limit);
            }
            chunk
                .offset
                .checked_add(chunk.object.length)
                .filter(|n| *n <= chunk.length)
                .ok_or(CellGroupArchiveError::Invalid("chunk bounds"))?;
            if let Some(before) = previous {
                let before_end = before.offset + before.object.length;
                if before_end < before.length {
                    if chunk.part != before.part
                        || chunk.length != before.length
                        || chunk.offset != before_end
                    {
                        return Err(CellGroupArchiveError::Invalid("chunk continuity"));
                    }
                } else if chunk.offset != 0 {
                    return Err(CellGroupArchiveError::Invalid("part offset"));
                }
            } else if chunk.part != wire::Part::Header || chunk.offset != 0 {
                return Err(CellGroupArchiveError::Invalid("first archive part"));
            }
            total = total
                .checked_add(chunk.object.length)
                .ok_or(CellGroupArchiveError::Limit)?;
            previous = Some(chunk);
        }
        let last = previous.ok_or(CellGroupArchiveError::Invalid("empty archive"))?;
        if total != self.bytes || last.offset + last.object.length != last.length {
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
    /// Capture is supplied by the caller's node checkpoint pool. The bounded
    /// producer keeps the full source pinned through physical reads. Losing the
    /// async caller closes its channel; domain shutdown joins the physical owner.
    /// Success confirms immutable bytes, without granting group coverage.
    pub async fn upload_checkpoint(
        &self,
        domain: &ExecutionDomain,
        source: CellGroupCheckpoint,
    ) -> Result<CellGroupArchiveReference> {
        domain
            .run(self.upload(domain, source))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn upload(
        &self,
        domain: &ExecutionDomain,
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
        let producer = domain
            .blocking(move || {
                let _retained = retained;
                source.stream(|chunk| {
                    if chunk.bytes.len() > CHUNK_BYTES {
                        return Err(CellGroupCheckpointError::Limit);
                    }
                    sender.blocking_send(wire::Chunk::copy(chunk)).map_err(|_| {
                        CellGroupCheckpointError::Sink(std::io::Error::other("archive stopped"))
                    })
                })
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?;
        let upload = async {
            while let Some(chunk) = receiver.recv().await {
                index.bytes = index
                    .bytes
                    .checked_add(chunk.bytes.len() as u64)
                    .filter(|n| *n <= self.limits.max_checkpoint_bytes)
                    .ok_or(CellGroupArchiveError::Limit)?;
                if index.chunks.len() >= self.limits.max_chunks {
                    return Err(CellGroupArchiveError::Limit);
                }
                let object = self.put("chunks", chunk.bytes, CHUNK_BYTES).await?;
                index.chunks.push(Chunk {
                    part: chunk.part,
                    offset: chunk.offset,
                    length: chunk.length,
                    object,
                });
            }
            Ok(())
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
        domain: &ExecutionDomain,
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
        domain: &ExecutionDomain,
        reference: &CellGroupArchiveReference,
        receiver: CellGroupArchiveCheckpointReceiver,
    ) -> Result<VerifiedCellGroupArchiveCheckpoint> {
        let permit = self.admit()?;
        self.verify_admitted(domain, reference, receiver, permit)
            .await
    }

    pub(super) async fn checkpoint_marker(
        &self,
        domain: &ExecutionDomain,
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
        domain: &ExecutionDomain,
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
        domain: &ExecutionDomain,
        reference: &CellGroupArchiveReference,
        mut receiver: CellGroupArchiveCheckpointReceiver,
        permit: Arc<Permit>,
    ) -> Result<VerifiedCellGroupArchiveCheckpoint> {
        let index = self
            .checkpoint_index(domain, reference, permit.clone())
            .await?;
        let marker = index.marker;
        let (sender, mut incoming) = tokio::sync::mpsc::channel::<wire::Chunk>(1);
        let retained = permit.clone();
        let consumer = domain
            .blocking(move || {
                let _retained = retained;
                while let Some(chunk) = incoming.blocking_recv() {
                    chunk.append(&mut receiver)?;
                }
                receiver.finish()
            })
            .map_err(|_| CellGroupArchiveError::Stopped)?;
        let download: Result<()> = async {
            for chunk in index.chunks {
                let bytes = self.get("chunks", &chunk.object, CHUNK_BYTES).await?;
                sender
                    .send(wire::Chunk {
                        part: chunk.part,
                        offset: chunk.offset,
                        length: chunk.length,
                        bytes,
                    })
                    .await
                    .map_err(|_| CellGroupArchiveError::Invalid("receiver refused archive"))?;
            }
            Ok(())
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
