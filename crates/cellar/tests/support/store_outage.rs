// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refuse real object-store operations for one native owner's client handles.
//! The underlying local store remains available to independent successors.
#![allow(clippy::disallowed_methods)]
use anyhow::Result;
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{list::*, path::Path, *};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
pub struct Outage {
    store: Arc<dyn ObjectStore>,
    cas_store: Arc<dyn ObjectStore>,
    isolated: AtomicBool,
    refused: AtomicUsize,
    lease_prefix: String,
    renewal_delay_ms: AtomicU64,
    delayed_renewals: AtomicUsize,
}

impl Outage {
    pub fn wrap(bucket: &cellar::bucket::Bucket) -> Result<(cellar::bucket::Bucket, Arc<Self>)> {
        let fault = Arc::new(Self {
            store: bucket.store.clone(),
            cas_store: bucket.cas_store.clone(),
            isolated: AtomicBool::new(false),
            refused: AtomicUsize::new(0),
            lease_prefix: format!("{}nodes/", bucket.prefix),
            renewal_delay_ms: AtomicU64::new(0),
            delayed_renewals: AtomicUsize::new(0),
        });
        let isolated = cellar::bucket::Bucket::from_clients(
            fault.clone(),
            fault.clone(),
            fault.clone(),
            bucket.backend,
            bucket.name.clone(),
            bucket.prefix.clone(),
        )?;
        Ok((isolated, fault))
    }

    pub fn isolate(&self) {
        self.isolated.store(true, Ordering::Release);
    }

    pub fn refused(&self) -> usize {
        self.refused.load(Ordering::Acquire)
    }

    pub fn is_isolated(&self) -> bool {
        self.isolated.load(Ordering::Acquire)
    }

    pub fn delay_node_renewals(&self, millis: u64) {
        self.renewal_delay_ms.store(millis, Ordering::Release);
    }

    pub fn delayed_renewals(&self) -> usize {
        self.delayed_renewals.load(Ordering::Acquire)
    }

    fn check(&self) -> object_store::Result<()> {
        if self.isolated.load(Ordering::Acquire) {
            self.refused.fetch_add(1, Ordering::AcqRel);
            return Err(object_store::Error::Generic {
                store: "event-owner-outage",
                source: "injected node-specific storage outage".into(),
            });
        }
        Ok(())
    }
}

impl std::fmt::Display for Outage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("event owner outage")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Outage {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check()?;
        let delay = self.renewal_delay_ms.load(Ordering::Acquire);
        let delayed = delay != 0
            && path.as_ref().starts_with(&self.lease_prefix)
            && matches!(&opts.mode, PutMode::Update(_));
        if delayed {
            cellar::asyncrt::sleep(std::time::Duration::from_millis(delay)).await;
            self.check()?;
        }
        // Preserve the bucket's no-transport-retry CAS client while wrapping
        // real S3 requests; a lost conditional response remains ambiguous.
        let store = if matches!(&opts.mode, PutMode::Create | PutMode::Update(_)) {
            &self.cas_store
        } else {
            &self.store
        };
        let result = store.put_opts(path, payload, opts).await;
        if delayed && result.is_ok() {
            self.delayed_renewals.fetch_add(1, Ordering::AcqRel);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check()?;
        self.store.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.check()?;
        self.store.get_opts(path, opts).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.check()?;
        self.store.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        match self.check() {
            Ok(()) => self.store.list(prefix),
            Err(failure) => futures_util::stream::once(async { Err(failure) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.check()?;
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.check()?;
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.check()?;
        self.store.copy_if_not_exists(from, to).await
    }
}

// The fixture has a fixed small object inventory. This adapter materializes
// that inventory solely to emulate pagination; production listing is unchanged.
#[async_trait::async_trait]
impl PaginatedListStore for Outage {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        self.check()?;
        let prefix = prefix.unwrap_or_default();
        let mut objects: Vec<ObjectMeta> = self.store.list(None).try_collect().await?;
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        let after = opts.page_token.as_deref().or(opts.offset.as_deref());
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for object in objects {
            let key = object.location.as_ref();
            let Some(tail) = key.strip_prefix(prefix) else {
                continue;
            };
            if after.is_some_and(|after| key <= after) {
                continue;
            }
            let common = opts.delimiter.as_deref().and_then(|delimiter| {
                tail.find(delimiter)
                    .map(|i| Path::from(format!("{prefix}{}{delimiter}", &tail[..i])))
            });
            if let Some(common) = common {
                if let Some((last, Some(prior), _)) = entries.last_mut() {
                    if *prior == common {
                        *last = key.into();
                        continue;
                    }
                }
                entries.push((key.into(), Some(common), None));
            } else {
                entries.push((key.into(), None, Some(object)));
            }
        }
        let limit = opts.max_keys.unwrap_or(usize::MAX);
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated
            .then(|| entries.last().map(|entry| entry.0.clone()))
            .flatten();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for (_, common, object) in entries {
            if let Some(common) = common {
                result.common_prefixes.push(common);
            }
            if let Some(object) = object {
                result.objects.push(object);
            }
        }
        Ok(PaginatedListResult { result, page_token })
    }
}
