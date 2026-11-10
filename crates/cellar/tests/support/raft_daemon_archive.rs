// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refuse archive writes at each voter while the real backend remains available.
use super::*;
use futures_util::stream::BoxStream;
use object_store::{
    path::Path as Key, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

const BLOCK: &str = "block-archive-writes";

#[derive(Debug)]
pub(super) struct Gate {
    store: Arc<dyn ObjectStore>,
    block: PathBuf,
    denied: PathBuf,
}

impl Gate {
    pub(super) fn new(store: Arc<dyn ObjectStore>, root: &Path, voter: u8) -> Self {
        Self {
            store,
            block: root.join(BLOCK),
            denied: denied_path(root, voter),
        }
    }

    async fn permit_write(&self, key: &Key) -> object_store::Result<()> {
        let error = |source: std::io::Error| object_store::Error::Generic {
            store: "daemon archive fixture",
            source: Box::new(source),
        };
        if key.as_ref().contains("/cell-group-archives/")
            && tokio::fs::try_exists(&self.block).await.map_err(error)?
        {
            tokio::fs::write(&self.denied, []).await.map_err(error)?;
            return Err(error(std::io::Error::other(
                "fixture refused an archive write",
            )));
        }
        Ok(())
    }
}

impl std::fmt::Display for Gate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("daemon archive fixture")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Gate {
    async fn put_opts(
        &self,
        key: &Key,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.permit_write(key).await?;
        self.store.put_opts(key, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        key: &Key,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.permit_write(key).await?;
        self.store.put_multipart_opts(key, options).await
    }

    async fn get_opts(&self, key: &Key, options: GetOptions) -> object_store::Result<GetResult> {
        self.store.get_opts(key, options).await
    }

    async fn delete(&self, key: &Key) -> object_store::Result<()> {
        self.store.delete(key).await
    }

    fn list(&self, prefix: Option<&Key>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Key>) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Key, to: &Key) -> object_store::Result<()> {
        self.permit_write(to).await?;
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Key, to: &Key) -> object_store::Result<()> {
        self.permit_write(to).await?;
        self.store.copy_if_not_exists(from, to).await
    }
}

fn denied_path(root: &Path, voter: u8) -> PathBuf {
    root.join(format!("archive-write-denied-{voter}"))
}

pub(super) async fn verify(
    root: &Path,
    client: &reqwest::Client,
    address: &ManagedIngress,
    before: &Value,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    let id = CommandId::new(ClientId::new([91; 16])?, 1)?;
    let command = format!("{}:1", hex::encode([91; 16]));
    let retry_until = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        + 60_000)
        .to_string();
    let submit = |ingress: Ingress, durability: &str| {
        client
            .post(format!(
                "{}/counters/one?delta=0",
                ingress.endpoint(address)
            ))
            .bearer_auth(SECRET)
            .header("x-command-id", &command)
            .header("x-retry-until-ms", &retry_until)
            .header("x-write-durability", durability)
    };

    let block = root.join(BLOCK);
    File::options().write(true).create_new(true).open(&block)?;
    let expected = format!("Ambiguous {{ command: {id:?} }}; cause=");
    let outage = async {
        ledger.record(serde_json::json!({
            "phase": "archive_writes_blocked", "cell": "one", "command": command,
            "retry_until": retry_until, "delta": 0,
        }))?;
        let receipt = submit(Ingress::Native, "quorum")
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let committed = receipt["index"].as_u64().context("outage receipt index")?;
        ensure!(
            receipt["value"] == before["value"]
                && receipt["group"] == before["group"]
                && receipt["durability"] == "quorum"
                && committed > before["index"].as_u64().context("initial receipt index")?
        );
        ledger.record(serde_json::json!({
            "phase": "archive_outage_quorum", "ingress": "native",
            "command": command, "receipt": receipt,
        }))?;
        let read = client
            .get(format!("{}/counters/one", Ingress::Axum.endpoint(address)))
            .bearer_auth(SECRET)
            .header("x-read-consistency", "linearizable")
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        recovery::matching_receipt(&read, &receipt)?;
        ledger.record(serde_json::json!({
            "phase": "archive_outage_linearizable", "ingress": "axum",
            "command": command, "receipt": read,
        }))?;
        for ingress in Ingress::ALL {
            let response = submit(ingress, "s3").send().await?;
            let status = response.status().as_u16();
            let body = response.text().await?;
            ledger.record(serde_json::json!({
                "phase": "archive_outage_s3_refused", "ingress": ingress.label(),
                "command": command, "status": status, "body": body,
            }))?;
            ensure!(
                status == 503 && body.starts_with(&expected),
                "explicit S3 acknowledgement during archive outage: {status} {body}"
            );
        }
        let denied: Vec<_> = (0..3)
            .filter(|&voter| denied_path(root, voter).is_file())
            .map(|voter| voter + 1)
            .collect();
        ensure!(!denied.is_empty(), "no voter attempted an archive write");
        ledger.record(serde_json::json!({
            "phase": "archive_outage_denied_writes", "members": denied,
        }))?;
        Ok::<_, anyhow::Error>(receipt)
    }
    .await;
    std::fs::remove_file(block)?;
    let quorum = outage?;
    ledger.record(serde_json::json!({"phase": "archive_writes_restored"}))?;

    let deadline = Instant::now() + Duration::from_secs(15);
    for ingress in Ingress::ALL {
        let receipt = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "archive receipt reconciliation deadline expired"
            );
            let response = submit(ingress, "s3").timeout(remaining).send().await?;
            let status = response.status();
            let body = response.text().await?;
            if status.is_success() {
                break serde_json::from_str::<Value>(&body)?;
            }
            ledger.record(serde_json::json!({
                "phase": "archive_outage_replay_waiting", "ingress": ingress.label(),
                "command": command, "status": status.as_u16(), "body": body,
            }))?;
            ensure!(
                status == 503 && body.starts_with(&expected) && Instant::now() < deadline,
                "restored archive did not cover the original receipt: {status} {body}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        recovery::matching_receipt(&receipt, &quorum)?;
        ensure!(receipt["sequence"] == quorum["sequence"] && receipt["durability"] == "s3");
        ledger.record(serde_json::json!({
            "phase": "archive_outage_s3_replayed", "ingress": ingress.label(),
            "command": command, "receipt": receipt,
        }))?;
    }
    Ok(())
}
