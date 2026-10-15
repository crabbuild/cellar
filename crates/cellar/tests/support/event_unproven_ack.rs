//! Hold real bucket publication after local ack commit, then force owner closure.
//! No synthetic gate or success adapter supplies durability to this fixture.

use super::*;
use anyhow::Result;
use futures_util::stream::BoxStream;
use object_store::{path::Path, *};
use serde_json::{json, Value};
use std::sync::Mutex;
use tokio::sync::{watch, Semaphore};

#[derive(Debug)]
pub(super) struct PublicationFault {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    extra_prefixes: Vec<String>,
    phase: watch::Sender<u8>,
    entered: Semaphore,
    attempts: AtomicUsize,
    held_path: Mutex<Option<String>>,
}

impl PublicationFault {
    fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Arc<Self> {
        Self::with_prefixes(store, prefix, Vec::new())
    }

    pub(super) fn with_prefixes(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        extra_prefixes: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            prefix,
            extra_prefixes,
            phase: watch::channel(0).0,
            entered: Semaphore::new(0),
            attempts: AtomicUsize::new(0),
            held_path: Mutex::new(None),
        })
    }

    pub(super) fn block(&self) {
        self.phase.send_replace(2);
    }

    pub(super) fn hold(&self) {
        self.phase.send_replace(1);
    }

    pub(super) async fn wait_for_publication(&self) -> Result<()> {
        self.entered.acquire().await?.forget();
        Ok(())
    }

    pub(super) fn refused(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }

    async fn check(&self, path: &Path) -> object_store::Result<()> {
        let mut phase = self.phase.subscribe();
        let current = *phase.borrow_and_update();
        if (!path.as_ref().starts_with(&self.prefix)
            && !self
                .extra_prefixes
                .iter()
                .any(|prefix| path.as_ref().starts_with(prefix)))
            || current == 0
        {
            return Ok(());
        }
        self.attempts.fetch_add(1, Ordering::AcqRel);
        self.held_path
            .lock()
            .unwrap()
            .get_or_insert_with(|| path.to_string());
        if current == 1 {
            self.entered.add_permits(1);
            while *phase.borrow_and_update() == 1 {
                phase
                    .changed()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "unproven-ack",
                        source: Box::new(error),
                    })?;
            }
        }
        Err(object_store::Error::Generic {
            store: "unproven-ack",
            source: "injected replica publication failure".into(),
        })
    }
}

impl std::fmt::Display for PublicationFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unproven ack replica publication fault")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PublicationFault {
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

async fn status(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "unproven-ack-audit",
    )
    .await?["result"]
        .clone())
}

async fn owner(domain: &ExecutionDomain, bucket: &cellar::bucket::Bucket) -> Result<Value> {
    domain
        .run(async {
            let (bytes, _) = bucket
                .get(&format!("cells/{}/own.json", stream_scope()?))
                .await?
                .context("partition owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unproven_ack_cannot_escape_its_gate_and_recovers_the_original_lease() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("unproven-ack-destination-core")?;
    let consumer_core = Executor::new("unproven-ack-consumer-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let data = directory.path().join("destination");
    let (config, mut services) = inputs(
        "unproven-destination",
        &data,
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let scope = stream_scope()?;
    let fault = PublicationFault::new(bucket.store.clone(), format!("cells/{scope}/ltx/"));
    let fleet = services.fleet.as_mut().context("fleet missing")?;
    // No follower proof may bypass the held bucket write. The real runtime
    // owns replication and gates; only its supplied replica client is decorated.
    fleet.fleet_durability = false;
    fleet.replica_storage = cellar::replica_storage::ReplicaStorage::new(
        fault.clone(),
        cellar::replica_storage::ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let destination = Node::start(
        config,
        services,
        graph(1, false),
        applications("destination")?,
    )
    .await?;
    super::super::ready(&destination).await?;
    let old_owner = owner(&observer, &bucket).await?;
    ensure!(old_owner["node"] == "unproven-destination");
    let epoch = old_owner["epoch"].as_u64().context("old epoch missing")?;
    let (config, services) = inputs(
        "unproven-consumer",
        &directory.path().join("consumer"),
        &consumer_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let consumer = Node::start(config, services, external, applications("consumer")?).await?;
    super::super::ready(&consumer).await?;
    let client = client()?;
    ensure!(call(&client, &consumer, reqwest::Method::POST, "/write").await? == "consumer:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while call(&client, &consumer, reqwest::Method::GET, "/outbox-pending").await? != "0" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("settle original proven append before ack cut")??;
    let batch: Value = serde_json::from_str(
        &call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/fetch-save?source=projection&id=000000000000000000000000000000b1",
        )
        .await?,
    )?;
    ensure!(batch["processed"] == 1);
    let original = batch["batch"]["receipt"]
        .as_str()
        .context("saved receipt missing")?
        .to_owned();
    let committed = status(&client, &destination)
        .await
        .context("audit original proven lease before ack cut")?;
    ensure!(committed["checkpoint"] == "0" && committed["observation"]["lease_active"] == true);

    fault.phase.send_replace(1);
    let mut acknowledging = Box::pin(call(
        &client,
        &consumer,
        reqwest::Method::POST,
        "/ack-original?source=projection",
    ));
    cellar::asyncrt::select! {
        result = &mut acknowledging => anyhow::bail!("ack escaped before held publication: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(5), fault.entered.acquire()) =>
            entered.context("hold actual stream replica publication")??.forget(),
    }
    let local = data
        .join(&scope)
        .join("ltx")
        .join(format!("e{epoch}"))
        .join("db.sqlite");
    let local_commit = tokio::task::spawn_blocking(move || -> Result<(i64, i64, String)> {
        // Test-only forensic observation of a local commit. Public reads still
        // require the real output gate; this connection never mutates storage.
        let db = rusqlite::Connection::open_with_flags(
            local,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        Ok((
            db.query_row(
                "SELECT checkpoint FROM __sys_stream_subscriptions WHERE name='projection'",
                [],
                |row| row.get(0),
            )?,
            db.query_row("SELECT count(*) FROM __sys_stream_leases", [], |row| {
                row.get(0)
            })?,
            db.query_row("SELECT receipt FROM __sys_stream_ack_receipts", [], |row| {
                row.get(0)
            })?,
        ))
    })
    .await??;
    ensure!(
        local_commit == (1, 0, original.clone()),
        "held upload did not follow the expected local ack commit"
    );
    ensure!(fault.attempts.load(Ordering::Acquire) > 0);
    ensure!(fault
        .held_path
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|path| path.starts_with(&fault.prefix)));
    ensure!(
        futures_util::FutureExt::now_or_never(acknowledging.as_mut()).is_none(),
        "ack became visible while its data proof was withheld"
    );
    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut acknowledging)
        .await
        .context("unproven ack caller outcome")??;
    drop(acknowledging);
    ensure!(
        outcome == "Ambiguous",
        "unproven submitted ack returned {outcome}"
    );
    let old_services = destination.services();
    let mut closing = Box::pin(destination.cancel());
    // Close admission before the held publisher is released to fail. This cut
    // cannot race a publication error against the requested forced closure.
    let completed = futures_util::FutureExt::now_or_never(closing.as_mut());
    fault.phase.send_replace(2);
    let outcome = match completed {
        Some(result) => result?,
        None => tokio::time::timeout(Duration::from_secs(10), closing)
            .await
            .context("join forcibly closed unproven destination")??,
    };
    ensure!(
        outcome == NodeOutcome::Cancelled,
        "unexpected forced closure {outcome:?}"
    );
    ensure!(old_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );

    // Forced closure joins local work but does not revoke a still-live node
    // lease. Observe actual authority expiry before asking a successor to claim.
    tokio::time::timeout(
        Duration::from_secs(5),
        observer.run(async {
            loop {
                let (bytes, _) = bucket
                    .get("nodes/unproven-destination.json")
                    .await?
                    .context("closed node lease missing")?;
                let lease: Value = serde_json::from_slice(&bytes)?;
                let expires = lease["expires_ms"]
                    .as_u64()
                    .context("node lease expiry missing")?;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis();
                if u128::from(expires) <= now {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }),
    )
    .await
    .context("observe forced owner's node lease expiry")???;
    let recovered = status(&client, &consumer)
        .await
        .context("audit restored stream after forced owner closure")?;
    let next_owner = owner(&observer, &bucket).await?;
    ensure!(
        next_owner["node"] != old_owner["node"]
            && next_owner["epoch"].as_u64().context("new epoch missing")? > epoch
    );
    // With bucket-only proof and all post-arm stream uploads refused, recovery
    // must use the previously proven checkpoint and original live batch.
    ensure!(
        recovered["identity"] == committed["identity"]
            && recovered["checkpoint"] == "0"
            && recovered["lease"] == committed["lease"]
            && recovered["observation"]["lease_active"] == true,
        "unproven checkpoint leaked into recovered state: {recovered}"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "Acknowledged"
    );
    ensure!(
        status(&client, &consumer)
            .await
            .context("audit recovered ack checkpoint")?["checkpoint"]
            == "1"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "AlreadyAcknowledged"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );
    ensure!(call(&client, &consumer, reqwest::Method::POST, "/write").await? == "consumer:2");
    tokio::time::timeout(Duration::from_secs(5), async {
        while status(&client, &consumer).await?["end"] != "3" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("wait for second event after unproven ack recovery")??;
    let next: Value = serde_json::from_str(
        &call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/fetch-save?source=projection&id=000000000000000000000000000000b2",
        )
        .await?,
    )?;
    ensure!(next["processed"] == 3 && next["batch"]["receipt"] != original);
    let second = status(&client, &consumer).await?;
    ensure!(
        second["checkpoint"] == "1"
            && second["lease"]["first"] == "2"
            && second["lease"]["last"] == "2"
            && second["observation"]["lease_active"] == true
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "AlreadyAcknowledged"
    );
    let after_old_ack = status(&client, &consumer).await?;
    ensure!(
        after_old_ack["checkpoint"] == "1" && after_old_ack["lease"] == second["lease"],
        "original receipt consumed the later lease: {after_old_ack}"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-stored?source=projection"
        )
        .await?
            == "Acknowledged"
    );
    let finished = status(&client, &consumer).await?;
    ensure!(finished["checkpoint"] == "2" && finished["lease"].is_null());
    stopped(consumer)
        .await
        .context("complete unproven-ack consumer shutdown")?;
    observer.shutdown().await;
    Ok(())
}
