// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent durable object keys behind one coherent qualification namespace.
use cellar::bucket::Bucket;
use futures_util::{future, stream, stream::BoxStream, StreamExt as _, TryStreamExt as _};
use object_store::{
    list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore},
    path::Path,
    *,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub(super) fn open(root: &std::path::Path, names: &[&str]) -> anyhow::Result<Bucket> {
    let bucket = Bucket::open_dev(&root.join("fleet.sqlite"))?;
    let mut nodes = BTreeMap::new();
    for name in names {
        let key = Path::from(format!("nodes/{name}.json"));
        anyhow::ensure!(!nodes.contains_key(&key), "duplicate fixture node key");
        let encoded = name
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let store = Bucket::open_dev(&root.join(format!("node-{encoded}.sqlite")))?;
        nodes.insert(key, store.store);
    }
    // Ordinary readers and the lease client share this exact logical store.
    // Every shard is the same FULL-synchronous SQLite backend. Independent
    // node-key CAS writes do not queue behind application-data transactions.
    let store = Arc::new(Store {
        data: bucket.store,
        nodes,
    });
    Bucket::from_clients(
        store.clone(),
        store.clone(),
        store,
        bucket.backend,
        bucket.name,
        bucket.prefix,
    )
}

#[derive(Debug)]
struct Store {
    data: Arc<dyn ObjectStore>,
    nodes: BTreeMap<Path, Arc<dyn ObjectStore>>,
}

impl std::fmt::Display for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("durable signed-peer fixture namespace")
    }
}

impl Store {
    fn target(&self, key: &Path) -> &Arc<dyn ObjectStore> {
        self.nodes.get(key).unwrap_or(&self.data)
    }

    async fn copy_between(&self, from: &Path, to: &Path, create: bool) -> Result<()> {
        let source = self.target(from);
        let target = self.target(to);
        if Arc::ptr_eq(source, target) {
            return if create {
                source.copy_if_not_exists(from, to).await
            } else {
                source.copy(from, to).await
            };
        }
        let response = source.get(from).await?;
        let attributes = response.attributes.clone();
        target
            .put_opts(
                to,
                response.bytes().await?.into(),
                PutOptions {
                    mode: if create {
                        PutMode::Create
                    } else {
                        PutMode::Overwrite
                    },
                    attributes,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
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
        self.target(path).put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.target(path).put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        // A missing or unavailable canonical shard is an error, never a
        // fallback to a stale copy in another physical file.
        self.target(path).get_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> Result<()> {
        self.target(path).delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let keys = self.nodes.keys().cloned().collect::<BTreeSet<_>>();
        let mut streams = vec![self
            .data
            .list(prefix)
            .try_filter(move |meta| future::ready(!keys.contains(&meta.location)))
            .boxed()];
        for (key, store) in &self.nodes {
            let key = key.clone();
            streams.push(
                store
                    .list(prefix)
                    .try_filter(move |meta| future::ready(meta.location == key))
                    .boxed(),
            );
        }
        stream::iter(streams).flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut rows = self.list(Some(&prefix));
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        while let Some(meta) = rows.try_next().await? {
            let common = {
                let Some(mut remainder) = meta.location.prefix_match(&prefix) else {
                    continue;
                };
                let Some(child) = remainder.next() else {
                    continue;
                };
                remainder.next().map(|_| prefix.child(child))
            };
            if let Some(common) = common {
                common_prefixes.insert(common);
            } else {
                objects.push(meta);
            }
        }
        objects.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_between(from, to, false).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_between(from, to, true).await
    }
}

// This small qualification namespace uses the same snapshot-style pagination
// as LocalStore. Every interface observes canonical node records.
#[async_trait::async_trait]
impl PaginatedListStore for Store {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let mut rows = self.list(None).try_collect::<Vec<_>>().await?;
        rows.sort_by(|a, b| a.location.cmp(&b.location));
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for meta in rows {
            let key = meta.location.to_string();
            let Some(remainder) = key.strip_prefix(prefix) else {
                continue;
            };
            if after.is_some_and(|after| key.as_str() <= after) {
                continue;
            }
            let common = options.delimiter.as_deref().and_then(|delimiter| {
                remainder.find(delimiter).map(|index| {
                    Path::from(format!("{prefix}{}{}", &remainder[..index], delimiter))
                })
            });
            if let Some(common) = common {
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = key;
                        continue;
                    }
                }
                entries.push((key, Some(common), None));
            } else {
                entries.push((key, None, Some(meta)));
            }
        }
        let limit = options.max_keys.unwrap_or(usize::MAX);
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
