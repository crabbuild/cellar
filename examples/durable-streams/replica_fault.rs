//! Opt-in process fixture: refuse real replica publication, never supply proof.

use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use std::{
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub struct ReplicaFault {
    store: Arc<dyn ObjectStore>,
    directory: PathBuf,
    journal_lock: Arc<Mutex<()>>,
}

impl ReplicaFault {
    pub fn new(store: Arc<dyn ObjectStore>, directory: &std::path::Path) -> Arc<Self> {
        Arc::new(Self {
            store,
            directory: directory.into(),
            journal_lock: Arc::new(Mutex::new(())),
        })
    }

    async fn check(&self, path: &Path) -> object_store::Result<()> {
        let directory = self.directory.clone();
        let path = path.to_string();
        let journal_lock = self.journal_lock.clone();
        let refused = cellar::asyncrt::try_blocking(move || -> std::io::Result<bool> {
            match std::fs::metadata(directory.join("replica-block")) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(error) => return Err(error),
            }
            // Distinct source upload tasks may fail concurrently. Serialize
            // complete evidence lines inside owned blocking work, never across await.
            let _guard = journal_lock
                .lock()
                .map_err(|_| std::io::Error::other("fixture journal lock poisoned"))?;
            let mut journal = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(directory.join("replica-refused.jsonl"))?;
            writeln!(journal, "{}", serde_json::json!({"path": path}))?;
            journal.sync_all()?;
            Ok(true)
        })
        .map_err(failure)?
        .await
        .map_err(failure)?
        .map_err(failure)?;
        if refused {
            return Err(failure(std::io::Error::other(
                "injected replica publication refusal",
            )));
        }
        Ok(())
    }
}

fn failure(error: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "stream-replica-fault",
        source: Box::new(error),
    }
}

impl std::fmt::Display for ReplicaFault {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("stream fixture replica publication fault")
    }
}

#[async_trait::async_trait]
impl ObjectStore for ReplicaFault {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check(path).await?;
        self.store.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check(path).await?;
        self.store.put_multipart_opts(path, opts).await
    }

    async fn get_opts(&self, path: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        self.store.get_opts(path, opts).await
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
