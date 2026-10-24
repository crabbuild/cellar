// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in private process fixture. Refuse actual node store operations and
//! hold a source callback inside its poll; never provide durability credit.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{list::*, path::Path, *};
use serde_json::{json, Value};
use std::{
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct Control {
    directory: PathBuf,
    journal: Mutex<()>,
    blocked_claim: Option<String>,
}

impl Control {
    pub fn new(directory: &std::path::Path) -> Arc<Self> {
        Arc::new(Self {
            directory: directory.into(),
            journal: Mutex::new(()),
            blocked_claim: None,
        })
    }

    pub fn for_claim(directory: &std::path::Path, scope: &str) -> Result<Arc<Self>> {
        let identity = scope.strip_prefix("__EventStream:").unwrap_or_default();
        ensure!(
            identity.len() == 64
                && identity
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "invalid private stream claim scope"
        );
        Ok(Arc::new(Self {
            directory: directory.into(),
            journal: Mutex::new(()),
            blocked_claim: Some(format!("cells/{scope}/own.json")),
        }))
    }

    fn publish(&self, value: Value) -> Result<()> {
        let temporary = self.directory.join("owner-phase.tmp");
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&serde_json::to_vec(&value)?)?;
        file.sync_all()?;
        std::fs::rename(temporary, self.directory.join("owner-phase.json"))?;
        Ok(())
    }

    pub fn hold(&self, scope: &str, cut: &str) -> Result<()> {
        self.publish(json!({"boundary":"source_poll_entered","scope":scope,"cut":cut,"pid":std::process::id()}))?;
        self.wait_release()
    }

    fn wait_release(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match std::fs::read(self.directory.join("owner-release")) {
                Ok(value) => {
                    ensure!(value == b"release", "invalid private callback release");
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            ensure!(
                Instant::now() < deadline,
                "private callback release deadline elapsed"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn attempted(&self, scope: &str, cut: &str, result: &Result<()>) -> Result<()> {
        self.publish(json!({"boundary":"source_poll_attempted","scope":scope,"cut":cut,"pid":std::process::id(),
            "refused":result.is_err(),"closed_domain":result.as_ref().err().is_some_and(|error| format!("{error:#}").contains("execution domain is closed"))}))
    }

    pub fn hold_consumer(
        &self,
        scope: &str,
        cut: &str,
        delivery: &Value,
        accepted: Option<bool>,
    ) -> Result<()> {
        self.publish(json!({"boundary":"consumer_poll_entered","scope":scope,"cut":cut,"pid":std::process::id(),"delivery":delivery,"accepted":accepted}))?;
        self.wait_release()
    }

    pub fn attempted_consumer(
        &self,
        scope: &str,
        cut: &str,
        acceptance: &std::result::Result<bool, String>,
        result: &Result<()>,
    ) -> Result<()> {
        self.publish(json!({"boundary":"consumer_poll_attempted","scope":scope,"cut":cut,"pid":std::process::id(),
            "accepted":acceptance.as_ref().ok(),"acceptance_closed_domain":acceptance.as_ref().err().is_some_and(|error|error.contains("execution domain is closed")),
            "refused":result.is_err(),"closed_domain":result.as_ref().err().is_some_and(|error|format!("{error:#}").contains("execution domain is closed"))}))
    }

    fn check(&self, operation: &str, path: &str) -> std::io::Result<bool> {
        let marker = if let Some(claim) = &self.blocked_claim {
            if operation != "put" || path != claim {
                return Ok(false);
            }
            "claim-block"
        } else {
            "owner-isolate"
        };
        match std::fs::metadata(self.directory.join(marker)) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        }
        let _lock = self
            .journal
            .lock()
            .map_err(|_| std::io::Error::other("fixture journal poisoned"))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join("owner-refused.jsonl"))?;
        writeln!(file, "{}", json!({"operation":operation,"path":path}))?;
        file.sync_all()?;
        Ok(true)
    }
}

#[derive(Clone, Debug)]
struct Fault {
    store: Arc<dyn ObjectStore>,
    control: Arc<Control>,
}

fn failure(error: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "stream-owner-fault",
        source: Box::new(error),
    }
}

impl Fault {
    async fn check(&self, operation: &str, path: &str) -> object_store::Result<()> {
        let (control, operation, path) =
            (self.control.clone(), operation.to_owned(), path.to_owned());
        let refused = cellar::asyncrt::try_blocking(move || control.check(&operation, &path))
            .map_err(failure)?
            .await
            .map_err(failure)?
            .map_err(failure)?;
        if refused {
            return Err(failure(std::io::Error::other(
                "injected node-specific store outage",
            )));
        }
        Ok(())
    }

    async fn objects(&self, prefix: Option<&Path>) -> object_store::Result<Vec<ObjectMeta>> {
        let rows: Vec<_> = self.store.list(prefix).take(1001).try_collect().await?;
        if rows.len() > 1000 {
            return Err(failure(std::io::Error::other(
                "private fixture inventory exceeds 1000 objects",
            )));
        }
        Ok(rows)
    }
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("private node store outage")
    }
}

pub fn wrap(
    bucket: cellar::bucket::Bucket,
    control: Arc<Control>,
) -> Result<cellar::bucket::Bucket> {
    let normal = Arc::new(Fault {
        store: bucket.store.clone(),
        control: control.clone(),
    });
    let conditional = Arc::new(Fault {
        store: bucket.cas_store.clone(),
        control,
    });
    // Only the small private fixture listing is materialized. Conditional
    // writes retain the original no-retry client; no success is synthesized.
    cellar::bucket::Bucket::from_clients(
        normal.clone(),
        conditional,
        normal,
        bucket.backend,
        bucket.name,
        bucket.prefix,
    )
}

#[async_trait::async_trait]
impl ObjectStore for Fault {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check("put", path.as_ref()).await?;
        self.store.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check("multipart", path.as_ref()).await?;
        self.store.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.check("get", path.as_ref()).await?;
        self.store.get_opts(path, opts).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.check("delete", path.as_ref()).await?;
        self.store.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let fault = self.clone();
        let prefix = prefix.cloned();
        futures_util::stream::once(async move {
            fault
                .check("list", prefix.as_ref().map_or("", |value| value.as_ref()))
                .await?;
            Ok::<_, object_store::Error>(futures_util::stream::iter(
                fault.objects(prefix.as_ref()).await?.into_iter().map(Ok),
            ))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.check(
            "list_with_delimiter",
            prefix.map_or("", |value| value.as_ref()),
        )
        .await?;
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.check("copy", from.as_ref()).await?;
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.check("copy_if_not_exists", from.as_ref()).await?;
        self.store.copy_if_not_exists(from, to).await
    }
}

#[async_trait::async_trait]
impl PaginatedListStore for Fault {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        self.check("list_paginated", prefix.unwrap_or_default())
            .await?;
        let prefix = prefix.unwrap_or_default();
        let mut objects: Vec<ObjectMeta> = self.objects(None).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn claim_fault_preserves_other_writes_reads_and_original_cas() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let scope = format!("__EventStream:{}", "a".repeat(64));
        for invalid in ["", "__EventStream:../scope", "Order:abc"] {
            ensure!(Control::for_claim(directory.path(), invalid).is_err());
        }
        let base = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
        let wrapped = wrap(base.clone(), Control::for_claim(directory.path(), &scope)?)?;
        let marker = directory.path().join("claim-block");
        std::fs::File::create(&marker)?.sync_all()?;
        let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let key = format!("cells/{scope}/own.json");
        let journal = directory.path().join("owner-refused.jsonl");
        let result = domain
            .run(async move {
                let token = base
                    .put_cas(&key, b"original".to_vec(), None)
                    .await?
                    .unwrap();
                ensure!(wrapped
                    .put_cas(&key, b"refused".to_vec(), Some(&token))
                    .await
                    .is_err());
                ensure!(base.get(&key).await?.unwrap().0.as_ref() == b"original");
                ensure!(wrapped.get(&key).await?.unwrap().0.as_ref() == b"original");
                ensure!(wrapped
                    .put_cas("nodes/producer.json", b"lease".to_vec(), None)
                    .await?
                    .is_some());
                ensure!(wrapped
                    .put_cas("cells/Order:other/own.json", b"owner".to_vec(), None)
                    .await?
                    .is_some());
                let rows: Vec<Value> = std::fs::read_to_string(journal)?
                    .lines()
                    .map(serde_json::from_str)
                    .collect::<std::result::Result<_, _>>()?;
                ensure!(rows == [json!({"operation":"put","path":key})]);
                std::fs::remove_file(marker)?;
                ensure!(wrapped
                    .put_cas(&key, b"released".to_vec(), Some(&token))
                    .await?
                    .is_some());
                ensure!(wrapped
                    .put_cas(&key, b"stale".to_vec(), Some(&token))
                    .await?
                    .is_none());
                ensure!(base.get(&key).await?.unwrap().0.as_ref() == b"released");
                Ok::<_, anyhow::Error>(())
            })
            .await?;
        domain.shutdown().await;
        result
    }
}
