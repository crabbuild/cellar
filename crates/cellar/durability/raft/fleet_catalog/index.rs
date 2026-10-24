//! Fleet-owned storage adapter for the shared bounded coordination index.
use super::{FleetCatalogError, FleetCursor, FleetPage, Inner, Result, PAGE_LIMIT};
use crate::raft::coordination_index::{Index, PageStore};
use bytes::Bytes;
use futures_util::future::BoxFuture;

impl PageStore for Inner {
    type Error = FleetCatalogError;

    fn page_limit(&self) -> usize {
        PAGE_LIMIT
    }

    fn read_page(&self, id: [u8; 32]) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            self.bucket
                .get_bounded(
                    &self.key(&format!("objects/{}", hex::encode(id))),
                    PAGE_LIMIT,
                )
                .await
                .map_err(FleetCatalogError::Storage)?
                .map(|(bytes, _)| bytes)
                .ok_or(FleetCatalogError::Corrupt("missing index page"))
        })
    }

    fn create_page(&self, id: [u8; 32], bytes: Vec<u8>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.create_exact(&format!("objects/{}", hex::encode(id)), &bytes, PAGE_LIMIT)
                .await
        })
    }
}

impl Inner {
    pub(super) async fn lookup(
        &self,
        root: Option<[u8; 32]>,
        key: &str,
    ) -> Result<Option<Vec<u8>>> {
        Index::new(self).lookup(root, key).await
    }

    pub(super) fn update<'a>(
        &'a self,
        root: Option<[u8; 32]>,
        key: &'a str,
        value: Option<Vec<u8>>,
        depth: usize,
    ) -> BoxFuture<'a, Result<Option<[u8; 32]>>> {
        Box::pin(async move { Index::new(self).update(root, key, value, depth).await })
    }

    pub(super) async fn scan(
        &self,
        root: Option<[u8; 32]>,
        after: Option<&FleetCursor>,
        limit: usize,
    ) -> Result<FleetPage> {
        Index::new(self).scan(root, after, limit).await
    }
}
