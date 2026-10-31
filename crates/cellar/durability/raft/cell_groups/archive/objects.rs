// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::TryStreamExt;
use object_store::{PutMode, PutOptions};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Object {
    pub registry: [u8; 32],
    pub generation: u64,
    pub digest: [u8; 32],
    pub length: u64,
}

impl Object {
    pub fn of(registry: [u8; 32], generation: u64, bytes: &[u8]) -> Self {
        Self {
            registry,
            generation,
            digest: Sha256::digest(bytes).into(),
            length: bytes.len() as u64,
        }
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 != self.length
            || <[u8; 32]>::from(Sha256::digest(bytes)) != self.digest
        {
            return Err(CellGroupArchiveError::Invalid("object digest/length"));
        }
        Ok(())
    }
}

impl CellGroupArchive {
    pub(super) fn object_path(&self, kind: &str, object: &Object) -> Path {
        self.prefix
            .child("registries")
            .child(hex::encode(object.registry))
            .child("generations")
            .child(object.generation.to_string())
            .child(kind)
            .child(format!("{}-{}", object.length, hex::encode(object.digest)))
    }

    pub(super) async fn io<T>(
        &self,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        crate::asyncrt::timeout(Duration::from_millis(self.limits.io_timeout_ms), future)
            .await
            .map_err(|_| CellGroupArchiveError::Timeout)?
    }

    // One deadline includes headers, every body chunk and EOF. Never allocate
    // from unbounded remote metadata or accept a partial/ranged object response.
    async fn read(&self, kind: &str, object: &Object, limit: usize) -> Result<Option<Vec<u8>>> {
        if object.registry == [0; 32]
            || object.generation < 2
            || object.generation == u64::MAX
            || object.length == 0
            || object.length > limit as u64
        {
            return Err(CellGroupArchiveError::Limit);
        }
        let path = self.object_path(kind, object);
        self.io(async {
            let result = match self.store.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(CellGroupArchiveError::Storage(error)),
            };
            if result.meta.size != object.length
                || result.range.start != 0
                || result.range.end != object.length
            {
                return Err(CellGroupArchiveError::Invalid(
                    "object response length/range",
                ));
            }
            let mut bytes = Vec::with_capacity(object.length as usize);
            let mut stream = result.into_stream();
            let mut chunks = 0;
            while let Some(chunk) = stream
                .try_next()
                .await
                .map_err(CellGroupArchiveError::Storage)?
            {
                chunks += 1;
                if chunks > 16_384
                    || bytes
                        .len()
                        .checked_add(chunk.len())
                        .is_none_or(|n| n > object.length as usize)
                {
                    return Err(CellGroupArchiveError::Limit);
                }
                bytes.extend_from_slice(&chunk);
            }
            object.verify(&bytes)?;
            Ok(Some(bytes))
        })
        .await
    }

    pub(super) async fn get(&self, kind: &str, object: &Object, limit: usize) -> Result<Vec<u8>> {
        self.read(kind, object, limit)
            .await?
            .ok_or(CellGroupArchiveError::Invalid("missing archive object"))
    }

    pub(super) async fn put_generation(
        &self,
        registry: [u8; 32],
        generation: u64,
        kind: &str,
        bytes: Vec<u8>,
        limit: usize,
    ) -> Result<Object> {
        if bytes.is_empty() || bytes.len() > limit {
            return Err(CellGroupArchiveError::Limit);
        }
        let object = Object::of(registry, generation, &bytes);
        // Reuse verified immutable objects without another PUT. A corrupt
        // existing object refuses; overwriting could erase evidence of damage.
        if self.read(kind, &object, limit).await?.is_some() {
            return Ok(object);
        }
        let path = self.object_path(kind, &object);
        let put = self
            .io(async {
                self.store
                    .put_opts(&path, bytes.into(), PutOptions::from(PutMode::Create))
                    .await
                    .map_err(CellGroupArchiveError::Storage)
            })
            .await;
        // Lost/failed PUT responses are ambiguous. Only an exact GET confirms
        // publication, including a concurrent conditional-create winner.
        match self.read(kind, &object, limit).await? {
            Some(_) => Ok(object),
            None => match put {
                Err(error) => Err(error),
                Ok(_) => Err(CellGroupArchiveError::Invalid("unconfirmed archive object")),
            },
        }
    }

    pub(super) async fn read_versioned(
        &self,
        path: &Path,
        limit: usize,
    ) -> Result<Option<(Vec<u8>, object_store::UpdateVersion)>> {
        self.io(async {
            let result = match self.store.get(path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(CellGroupArchiveError::Storage(error)),
            };
            let length = result.meta.size;
            if length == 0
                || length > limit as u64
                || result.range.start != 0
                || result.range.end != length
            {
                return Err(CellGroupArchiveError::Invalid(
                    "control response length/range",
                ));
            }
            let version = object_store::UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            let mut bytes = Vec::with_capacity(length as usize);
            let mut stream = result.into_stream();
            let mut chunks = 0;
            while let Some(chunk) = stream
                .try_next()
                .await
                .map_err(CellGroupArchiveError::Storage)?
            {
                chunks += 1;
                if chunks > 16_384
                    || bytes
                        .len()
                        .checked_add(chunk.len())
                        .is_none_or(|n| n > length as usize)
                {
                    return Err(CellGroupArchiveError::Limit);
                }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() != length as usize {
                return Err(CellGroupArchiveError::Invalid("control body length"));
            }
            Ok(Some((bytes, version)))
        })
        .await
    }
}
