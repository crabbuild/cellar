//! Hold one actual root response after reading its exact bytes and token.
//! This gate supplies no replacement metadata or durability verdict.

use anyhow::{Context as _, Result};
use bytes::Bytes;
use futures_util::{stream::BoxStream, StreamExt as _};
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::Semaphore;

#[derive(Debug)]
pub(super) struct RootReadCut {
    store: Arc<dyn ObjectStore>,
    key: String,
    captured: AtomicBool,
    bytes: Mutex<Option<Bytes>>,
    pub entered: Semaphore,
    pub release: Semaphore,
}

impl RootReadCut {
    pub fn new(store: Arc<dyn ObjectStore>, key: String) -> Arc<Self> {
        Arc::new(Self {
            store,
            key,
            captured: AtomicBool::new(false),
            bytes: Mutex::new(None),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }

    pub fn snapshot(&self) -> Result<serde_json::Value> {
        let bytes = self
            .bytes
            .lock()
            .unwrap()
            .clone()
            .context("root read was not captured")?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl std::fmt::Display for RootReadCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("root read cut")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RootReadCut {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let result = self.store.get_opts(path, opts).await?;
        if path.as_ref() != self.key || self.captured.swap(true, Ordering::AcqRel) {
            return Ok(result);
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let bytes = result.bytes().await?;
        *self.bytes.lock().unwrap() = Some(bytes.clone());
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "root-read-cut",
                source: Box::new(error),
            })?
            .forget();
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures_util::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        self.store.delete(path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}

pub(super) fn follower_proof(
    root: &std::path::Path,
    lease: &serde_json::Value,
) -> Result<Vec<(String, Vec<u8>)>> {
    let lineage = lease["lineage"].as_str().context("proof lineage missing")?;
    let generation = lease["generation"]
        .as_str()
        .context("proof generation missing")?;
    anyhow::ensure!(
        lineage.len() == 32
            && generation.len() == 64
            && lineage
                .bytes()
                .chain(generation.bytes())
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
    let mut rows = Vec::new();
    let mut total = 0usize;
    for member in lease["log"]["ensemble"]
        .as_array()
        .context("proof ensemble missing")?
    {
        let member = member.as_str().context("non-string follower")?;
        let label = match member {
            "refusal-source" => "source",
            "refusal-witness" => "witness",
            _ => anyhow::bail!("unexpected proof member"),
        };
        let leaf = root
            .join(label)
            .join("peerlog/refusal-destination")
            .join(lineage)
            .join(generation);
        let entries = match std::fs::read_dir(&leaf) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            anyhow::ensure!(
                rows.len() < 100 && entry.file_type()?.is_file(),
                "unexpected or excessive follower files"
            );
            let bytes = usize::try_from(entry.metadata()?.len())?;
            total = total
                .checked_add(bytes)
                .context("follower proof size overflow")?;
            anyhow::ensure!(
                total <= 32 << 20,
                "fixture proof inventory exceeded its bound"
            );
            rows.push((
                format!(
                    "{label}/{}",
                    entry
                        .file_name()
                        .to_str()
                        .context("invalid proof filename")?
                ),
                std::fs::read(entry.path())?,
            ));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    anyhow::ensure!(
        rows.iter().any(|(name, _)| name.ends_with(".batch")),
        "no retained committed follower batch"
    );
    Ok(rows)
}
