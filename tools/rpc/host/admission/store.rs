// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe real metadata reads and exclude the witness from ownership claims.
//! Data reads, follower service and the witness's own node lease remain real.
use futures_util::{stream::BoxStream, StreamExt};
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub(super) struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub root: String,
    pub owner: String,
    pub exclude: bool,
    pub replacement: String,
    pub observing: AtomicBool,
    pub missing_reads: AtomicUsize,
    pub replaced_reads: AtomicUsize,
    pub denied_claims: AtomicUsize,
}

impl Store {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        prefix: &str,
        scope: &str,
        exclude: bool,
        replacement: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            root: format!("{prefix}nodes/recovery-owner.json"),
            owner: format!("{prefix}cells/{scope}/own.json"),
            exclude,
            replacement,
            observing: AtomicBool::new(true),
            missing_reads: AtomicUsize::new(0),
            replaced_reads: AtomicUsize::new(0),
            denied_claims: AtomicUsize::new(0),
        })
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("recovery admission observer")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        if self.exclude && (path.as_ref() == self.root || path.as_ref() == self.owner) {
            self.denied_claims.fetch_add(1, Ordering::AcqRel);
            return Err(Error::Generic {
                store: "recovery admission",
                source: "witness is excluded from the selected recovery claim".into(),
            });
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        let result = self.inner.get_opts(path, options).await;
        if self.exclude || path.as_ref() != self.root || !self.observing.load(Ordering::Acquire) {
            return result;
        }
        let result = match result {
            Err(error @ Error::NotFound { .. }) => {
                self.missing_reads.fetch_add(1, Ordering::AcqRel);
                return Err(error);
            }
            other => other?,
        };
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let bytes = result.bytes().await?;
        if serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .is_some_and(|root| root["generation"].as_str() == Some(self.replacement.as_str()))
        {
            self.replaced_reads.fetch_add(1, Ordering::AcqRel);
        }
        // Preserve the exact bytes, token metadata, range and attributes read
        // from the real bucket. The observer never supplies a proof verdict.
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures_util::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.inner.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
