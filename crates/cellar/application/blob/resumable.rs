// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reconnectable backend upload identities, independent of process registries.

use super::{store, Bucket, Context, Object, UploadedPart};
use anyhow::{ensure, Result};
use object_store::multipart::PartId;
use std::sync::Arc;

/// A persistent upload route. Dropping it does not abort the remote resource.
/// Its one-based parts may be replaced; completion names consecutive parts
/// starting at one and must supply their current, nonempty backend receipts.
#[derive(Clone)]
pub struct ResumableMultipart {
    bucket: Bucket,
    key: Arc<str>,
    id: Arc<str>,
}

impl ResumableMultipart {
    pub(super) fn new(bucket: Bucket, key: &str, id: &str) -> Self {
        Self {
            bucket,
            key: key.into(),
            id: id.into(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn upload_id(&self) -> &str {
        &self.id
    }

    pub async fn upload_part(
        &self,
        context: &Context,
        number: u32,
        bytes: impl Into<Vec<u8>>,
    ) -> Result<UploadedPart> {
        context.check_turn()?;
        ensure!(
            (1..=10_000).contains(&number),
            "multipart part number must be in 1..=10000"
        );
        let bytes = bytes.into();
        ensure!(
            bytes.len()
                <= crate::application::limits::current()
                    .blob
                    .max_multipart_backlog_bytes,
            "multipart part exceeds the native Blob buffer limit"
        );
        let backend = store::store().map_err(anyhow::Error::msg)?;
        crate::native_worker::blob_gate(context).await?;
        let receipt = backend
            .upload_resumable_part(
                &store::blob_key(&self.bucket.name, &self.key),
                &self.id,
                number as usize - 1,
                bytes.into(),
            )
            .await?;
        ensure!(
            !receipt.content_id.is_empty() && receipt.content_id.len() <= 16_384,
            "invalid uploaded part receipt"
        );
        Ok(UploadedPart {
            part_number: number,
            etag: receipt.content_id,
        })
    }

    pub async fn complete(&self, context: &Context, parts: &[UploadedPart]) -> Result<Object> {
        context.check_turn()?;
        ensure!(
            !parts.is_empty() && parts.len() <= 10_000,
            "multipart completion requires 1 to 10000 parts"
        );
        for (index, part) in parts.iter().enumerate() {
            ensure!(
                part.part_number as usize == index + 1
                    && !part.etag.is_empty()
                    && part.etag.len() <= 16_384,
                "multipart completion requires consecutive current part receipts"
            );
        }
        let backend = store::store().map_err(anyhow::Error::msg)?;
        let key = store::blob_key(&self.bucket.name, &self.key);
        crate::native_worker::blob_gate(context).await?;
        let published = backend
            .complete_resumable_multipart(
                &key,
                &self.id,
                parts
                    .iter()
                    .map(|part| PartId {
                        content_id: part.etag.clone(),
                    })
                    .collect(),
            )
            .await?;
        // Completion can already have committed. A raced/missing observation
        // cannot turn this into a rollback proof or authorize mutation replay.
        let meta = backend
            .head_blob(&key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("completed object could not be observed"))?;
        let same = match published.version {
            Some(version) => meta.version.as_deref() == Some(&version),
            None => published
                .e_tag
                .as_ref()
                .is_some_and(|etag| meta.etag.as_ref() == Some(etag)),
        };
        ensure!(
            same,
            "completed object was replaced before metadata observation"
        );
        Object::from_record(store::object_json(&self.key, &meta, None))
    }

    pub async fn abort(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        let backend = store::store().map_err(anyhow::Error::msg)?;
        crate::native_worker::blob_gate(context).await?;
        backend
            .abort_resumable_multipart(&store::blob_key(&self.bucket.name, &self.key), &self.id)
            .await
    }
}
