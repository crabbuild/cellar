//! Real CAS/readback under two distinct lease-renewal response losses.
use super::*;
use anyhow::Result;
use futures_util::stream::BoxStream;
use object_store::{path::Path as ObjectPath, *};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

struct RenewalDelay {
    store: Arc<dyn ObjectStore>,
    key: String,
    armed: AtomicBool,
    maximum_expiry: Arc<AtomicU64>,
    attempts: AtomicUsize,
    committed: Arc<Semaphore>,
    readbacks: AtomicUsize,
    held_response: Mutex<Option<cellar::asyncrt::TaskHandle<()>>>,
}

impl std::fmt::Debug for RenewalDelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenewalDelay")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for RenewalDelay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native lease renewal response delay")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RenewalDelay {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let expiry = (path.as_ref() == self.key)
            .then(|| {
                let bytes: Vec<u8> = payload
                    .iter()
                    .flat_map(|chunk| chunk.iter().copied())
                    .collect();
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .ok()
                    .and_then(|value| value["expires_ms"].as_u64())
            })
            .flatten();
        let attempt = expiry
            .filter(|expiry| {
                self.armed.load(Ordering::Acquire)
                    && *expiry > self.maximum_expiry.load(Ordering::Acquire)
            })
            .map(|_| self.attempts.fetch_add(1, Ordering::AcqRel));
        if attempt == Some(0) {
            // The native attempt deadline drops this first request before it
            // reaches the store. No new root exists for the first readback.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if attempt == Some(1) {
            // Physical CAS can finish after its caller is cancelled. Keep the
            // commit observation with that work, and join it before teardown.
            let store = self.store.clone();
            let path = path.clone();
            let maximum_expiry = self.maximum_expiry.clone();
            let committed = self.committed.clone();
            let (send, receive) = tokio::sync::oneshot::channel();
            let task = cellar::asyncrt::try_spawn(async move {
                let result = store.put_opts(&path, payload, options).await;
                if result.is_ok() {
                    if let Some(expiry) = expiry {
                        maximum_expiry.fetch_max(expiry, Ordering::AcqRel);
                    }
                    // Lose the actual store response beyond the old authority.
                    committed.add_permits(1);
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                let _ = send.send(result);
            })
            .map_err(|error| object_store::Error::Generic {
                store: "lease-renewal-fixture",
                source: Box::new(error),
            })?;
            let prior = self.held_response.lock().unwrap().replace(task);
            assert!(prior.is_none(), "second renewal task was already owned");
            return receive
                .await
                .map_err(|error| object_store::Error::Generic {
                    store: "lease-renewal-fixture",
                    source: Box::new(error),
                })?;
        }
        let result = self.store.put_opts(path, payload, options).await?;
        if let Some(expiry) = expiry {
            self.maximum_expiry.fetch_max(expiry, Ordering::AcqRel);
        }
        Ok(result)
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.store.get_opts(path, options).await?;
        if path.as_ref() == self.key && self.attempts.load(Ordering::Acquire) >= 2 {
            self.readbacks.fetch_add(1, Ordering::AcqRel);
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.store.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_committed_renewal_keeps_time_for_actual_readback() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("lease-readback-budget")?;
    let mut bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let fault = Arc::new(RenewalDelay {
        store: bucket.cas_store.clone(),
        key: format!("{}nodes/lease-budget.json", bucket.prefix),
        armed: AtomicBool::new(false),
        maximum_expiry: Arc::new(AtomicU64::new(0)),
        attempts: AtomicUsize::new(0),
        committed: Arc::new(Semaphore::new(0)),
        readbacks: AtomicUsize::new(0),
        held_response: Mutex::new(None),
    });
    bucket.store = fault.clone();
    bucket.cas_store = fault.clone();
    let (mut config, mut services) = inputs(
        "lease-budget",
        &directory.path().join("node"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    // Keep both remaining-authority fractions deterministic and below the old
    // 250ms minimum after the first cancelled attempt. This is a short-lease
    // fault fixture, not an increased timeout to make a failed check pass.
    config.actor.lease_ttl_ms = 900;
    let fleet = services.fleet.as_mut().context("lease fixture fleet")?;
    fleet.fleet_durability = false;
    let node = Node::start(config, services, graph(), applications("budget", None)?).await?;
    ready(&node).await?;
    let client = client()?;
    ensure!(call(&client, &node, reqwest::Method::POST, "/").await? == "budget:1");
    fault.armed.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(3), fault.committed.acquire())
        .await
        .context("second renewal never committed")??
        .forget();
    tokio::time::timeout(Duration::from_secs(2), async {
        while fault.readbacks.load(Ordering::Acquire) == 0 {
            ensure!(
                !node.is_closed(),
                "node fenced before reading its committed renewal"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("no actual readback after committed renewal")??;
    // The response remains held; staying live across a full TTL requires proof
    // from the readback and subsequent ordinary renewals, not a client retry.
    tokio::time::sleep(Duration::from_millis(950)).await;
    ensure!(
        !node.is_closed(),
        "node discarded a provable committed renewal"
    );
    ensure!(
        fault.attempts.load(Ordering::Acquire) >= 3,
        "no later renewal progressed"
    );
    ensure!(call(&client, &node, reqwest::Method::GET, "/").await? == "budget:1");
    ensure!(call(&client, &node, reqwest::Method::POST, "/").await? == "budget:2");
    let held_response = { fault.held_response.lock().unwrap().take() }
        .context("second renewal task was not retained")?;
    held_response.await.context("join held renewal response")?;
    stopped(node).await?;
    ensure!(
        core.handle().spawn(async { 7 }).await? == 7,
        "caller executor was closed"
    );
    Ok(())
}
