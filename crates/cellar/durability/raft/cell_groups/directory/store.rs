// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::coordination_index::PageStore;
use bytes::Bytes;
use futures_util::future::BoxFuture;

#[derive(Default)]
pub(super) struct Cache {
    pages: std::collections::BTreeMap<[u8; 32], Bytes>,
    order: std::collections::VecDeque<[u8; 32]>,
    bytes: usize,
}

impl RangeDirectory {
    pub(super) fn key(&self, suffix: &str) -> Path {
        Path::from(format!(
            "{}cell-groups/{suffix}",
            self.namespace.namespace_key()
        ))
    }

    pub(super) async fn read(&self, suffix: &str) -> Result<Option<Vec<u8>>, CatalogError> {
        Ok(self
            .namespace
            .inner
            .read_versioned_bounded(&self.key(suffix), wire::LIMIT)
            .await?
            .map(|r| r.0))
    }

    pub(super) async fn immutable(
        &self,
        category: &str,
        id: [u8; 32],
        bytes: &[u8],
    ) -> Result<(), CatalogError> {
        if record::digest(bytes) != id {
            return Err(CatalogError::Invalid("immutable range object digest"));
        }
        let key = self.key(&format!("{category}/{}", hex::encode(id)));
        let observed = self
            .namespace
            .inner
            .create_and_observe_bounded(&key, bytes, wire::LIMIT)
            .await?;
        if observed != bytes {
            return Err(CatalogError::Corrupt("immutable range object collision"));
        }
        Ok(())
    }

    pub(super) async fn range_bytes(&self, id: [u8; 32]) -> Result<Bytes, CatalogError> {
        if let Some(bytes) = self
            .cache
            .lock()
            .map_err(|_| CatalogError::Corrupt("range cache"))?
            .pages
            .get(&id)
            .cloned()
        {
            return Ok(bytes);
        }
        let bytes = Bytes::from(
            self.read(&format!("ranges/{}", hex::encode(id)))
                .await?
                .ok_or(CatalogError::Corrupt("missing range page"))?,
        );
        if record::digest(&bytes) != id {
            return Err(CatalogError::Corrupt("range page digest"));
        }
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| CatalogError::Corrupt("range cache"))?;
        if self.limits.max_cached_pages > 0
            && bytes.len() <= self.limits.max_cache_bytes
            && !cache.pages.contains_key(&id)
        {
            while cache.pages.len() >= self.limits.max_cached_pages
                || cache.bytes + bytes.len() > self.limits.max_cache_bytes
            {
                let oldest = cache
                    .order
                    .pop_front()
                    .ok_or(CatalogError::Corrupt("range cache accounting"))?;
                if let Some(removed) = cache.pages.remove(&oldest) {
                    cache.bytes -= removed.len();
                }
            }
            cache.bytes += bytes.len();
            cache.order.push_back(id);
            cache.pages.insert(id, bytes.clone());
        }
        Ok(bytes)
    }

    /// Cached immutable bytes only. Returned routes remain hints and every
    /// destination must verify the current route and its own quorum authority.
    pub fn cache_usage(&self) -> Result<(usize, usize), CatalogError> {
        let cache = self
            .cache
            .lock()
            .map_err(|_| CatalogError::Corrupt("range cache"))?;
        Ok((cache.pages.len(), cache.bytes))
    }
}

impl PageStore for RangeDirectory {
    type Error = CatalogError;

    fn page_limit(&self) -> usize {
        wire::LIMIT
    }

    fn read_page(&self, id: [u8; 32]) -> BoxFuture<'_, Result<Bytes, CatalogError>> {
        Box::pin(async move {
            self.read(&format!("index/{}", hex::encode(id)))
                .await?
                .map(Bytes::from)
                .ok_or(CatalogError::Corrupt("missing range index page"))
        })
    }

    fn create_page(&self, id: [u8; 32], bytes: Vec<u8>) -> BoxFuture<'_, Result<(), CatalogError>> {
        Box::pin(async move { self.immutable("index", id, &bytes).await })
    }
}
