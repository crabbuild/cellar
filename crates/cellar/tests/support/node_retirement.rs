// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retirement checks are activation-scoped, never time-based negative caching.
use super::*;
use cellar::sdk::containers::SandboxCatalog;
use futures_util::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Debug, Default)]
struct Reads {
    count: AtomicUsize,
    refuse: AtomicBool,
    marker_mode: AtomicU8,
    marker_entered: Notify,
    marker_release: Notify,
}

#[derive(Debug)]
struct RetirementStore {
    inner: Arc<dyn ObjectStore>,
    reads: Arc<Reads>,
}

impl std::fmt::Display for RetirementStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("retirement admission fixture")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RetirementStore {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if path.as_ref().contains("cell-retirement/records/") {
            if self.reads.marker_mode.load(Ordering::SeqCst) != 0 {
                self.reads.marker_entered.notify_one();
            }
            loop {
                let released = self.reads.marker_release.notified();
                if self.reads.marker_mode.load(Ordering::SeqCst) != 1 {
                    break;
                }
                released.await;
            }
            if self.reads.marker_mode.load(Ordering::SeqCst) == 2 {
                return Err(object_store::Error::Generic {
                    store: "retirement publication fixture",
                    source: "marker publication unavailable".into(),
                });
            }
        }
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if path.as_ref().contains("cell-retirement/records/") {
            self.reads.count.fetch_add(1, Ordering::SeqCst);
            if self.reads.refuse.load(Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "retirement admission fixture",
                    source: "retirement backend unavailable".into(),
                });
            }
        }
        self.inner.get_opts(path, options).await
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

struct ReleaseMarker(Arc<Reads>);

impl Drop for ReleaseMarker {
    fn drop(&mut self) {
        self.0.marker_mode.store(0, Ordering::SeqCst);
        self.0.marker_release.notify_waiters();
    }
}

#[derive(Default)]
struct ErasureSignals {
    entered: Notify,
    release: Notify,
    handlers: AtomicUsize,
    held_completed: AtomicUsize,
    scope: Mutex<Option<String>>,
}

struct Erasable(Arc<ErasureSignals>);

impl Cell for Erasable {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.0.handlers.fetch_add(1, Ordering::SeqCst);
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("business cleanup callback must not run");
            };
            let storage = context.storage()?;
            if request.url.ends_with("/seed") {
                storage.put_json("value", &serde_json::json!(1))?;
                *self.0.scope.lock().unwrap() = Some(context.identity().unwrap().scope.clone());
            }
            if request.url.ends_with("/erase") {
                let catalog = SandboxCatalog::open(&context)?;
                catalog.begin_retirement(&context).await?;
                catalog.finish_retirement(&context).await?;
                catalog.request_erasure(&context).await?;
                return Ok(response("erasure requested"));
            }
            let value = storage
                .get_json("value")?
                .context("missing fixture state")?;
            if request.url.ends_with("/hold") {
                self.0.entered.notify_one();
                self.0.release.notified().await;
                self.0.held_completed.fetch_add(1, Ordering::SeqCst);
            }
            Ok(response(value))
        })
    }
}

fn erasing_app(signals: Arc<ErasureSignals>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let signals = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        anyhow::bail!("HTTP request required")
                    };
                    Ok(Output::Http(
                        context
                            .namespace("ACCOUNTS")?
                            .get_by_name("same-business-id")
                            .fetch(&context, request)
                            .await?,
                    ))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(Erasable(signals.clone())))
            })?,
        ))
    })
}

async fn wait_erased(node: &Node, scope: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while node.sandbox_erasure_status(scope).await? != Some("erased") {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

async fn inspect_source(
    client: &reqwest::Client,
    node: &Node,
    scope: &str,
) -> Result<reqwest::Response> {
    let path = format!("/outbox/{scope}");
    let body = serde_json::to_vec(&serde_json::json!({"op":"inspect"}))?;
    let auth = cellar::peer_auth::PeerAuth::new([11; 32], "erasure-operator")?;
    Ok(auth
        .sign(
            client
                .post(format!("http://{}{path}", node.info().internal_address))
                .body(body.clone()),
            "POST",
            &path,
            &body,
            &node.info().node,
        )?
        .send()
        .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resident_erasure_closes_new_and_suspended_reads_before_marker_publication() -> Result<()> {
    erasure_case(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_erasure_intent_stays_closed_when_marker_publication_failed() -> Result<()> {
    erasure_case(true).await
}

async fn erasure_case(fail_publication: bool) -> Result<()> {
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = observer
        .run(async {
            let directory = tempfile::tempdir()?;
            let mut bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            let reads = Arc::new(Reads::default());
            bucket.store = Arc::new(RetirementStore {
                inner: bucket.store.clone(),
                reads: reads.clone(),
            });
            let core = Executor::new("erasure-admission-core")?;
            let signals = Arc::new(ErasureSignals::default());
            let (config, services) = inputs(
                "erasure-owner",
                &directory.path().join("owner"),
                &core,
                Some(bucket.clone()),
                0,
            )
            .await?;
            let node =
                Node::start(config, services, graph(), erasing_app(signals.clone())?).await?;
            let client = client()?;
            let mut requests = tokio::task::JoinSet::new();
            let release = ReleaseMarker(reads.clone());
            let scope = async {
                ready(&node).await?;
                ensure!(call(&client, &node, reqwest::Method::GET, "/seed").await? == "1");
                let scope = signals
                    .scope
                    .lock()
                    .unwrap()
                    .clone()
                    .context("fixture scope missing")?;
                let url = format!("http://{}", node.info().public_address);
                let inspected = inspect_source(&client, &node, &scope).await?;
                ensure!(
                    inspected.status().is_success(),
                    "source operator fixture failed: {}",
                    inspected.text().await?
                );
                let held_client = client.clone();
                let held_url = url.clone();
                requests.spawn(async move {
                    let response = held_client.get(format!("{held_url}/hold")).send().await?;
                    Ok::<_, anyhow::Error>((true, response.status(), response.text().await?))
                });
                tokio::time::timeout(Duration::from_secs(5), signals.entered.notified()).await?;
                // Hold every publisher (including repair) while measuring the
                // foreground admission path; background readback is not a read.
                reads.marker_mode.store(1, Ordering::SeqCst);
                let erase_client = client.clone();
                let erase_url = url.clone();
                // The erasure reply may race irreversible admission closure. The
                // actual marker publication, not that potentially ambiguous reply,
                // establishes that framework cleanup reached its sealed request.
                requests.spawn(async move {
                    let response = erase_client
                        .get(format!("{erase_url}/erase"))
                        .send()
                        .await?;
                    Ok::<_, anyhow::Error>((false, response.status(), response.text().await?))
                });
                tokio::time::timeout(Duration::from_secs(5), reads.marker_entered.notified())
                    .await?;
                let before = reads.count.load(Ordering::SeqCst);
                let handlers = signals.handlers.load(Ordering::SeqCst);
                let inspected = inspect_source(&client, &node, &scope).await?;
                ensure!(
                    inspected.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    "privileged source operator bypassed erasure closure"
                );
                // The RPC boundary deliberately reports an unproven result as
                // ambiguous; it does not expose the executor's admission error.
                let inspected: serde_json::Value = inspected.json().await?;
                ensure!(
                    inspected["error"] == "Ambiguous: event RPC did not return a proven result",
                    "source operator returned an unexpected failure: {inspected}"
                );
                for _ in 0..3 {
                    let response = client.get(format!("{url}/read")).send().await?;
                    ensure!(
                        !response.status().is_success(),
                        "erasing resident served a business read"
                    );
                    let body = response.text().await?;
                    ensure!(
                        body.contains("source business admission"),
                        "read was not refused by local admission: {body}"
                    );
                }
                ensure!(
                    signals.handlers.load(Ordering::SeqCst) == handlers,
                    "closed admission entered application code"
                );
                ensure!(
                    reads.count.load(Ordering::SeqCst) == before,
                    "closed resident queried remote admission"
                );
                signals.release.notify_one();
                if fail_publication {
                    reads.marker_mode.store(2, Ordering::SeqCst);
                    reads.marker_release.notify_waiters();
                }
                while let Some(joined) = requests.join_next().await {
                    let (held, status, body) = joined??;
                    if held {
                        ensure!(
                            !status.is_success() && body.contains("source business admission"),
                            "suspended read escaped erasure closure: {status} {body}"
                        );
                    }
                }
                ensure!(
                    signals.held_completed.load(Ordering::SeqCst) == 0,
                    "suspended handler resumed after closure"
                );
                if !fail_publication {
                    reads.marker_mode.store(0, Ordering::SeqCst);
                    reads.marker_release.notify_waiters();
                    wait_erased(&node, &scope).await?;
                }
                Ok::<_, anyhow::Error>(scope)
            }
            .await;
            requests.abort_all();
            while requests.join_next().await.is_some() {}
            // Failed publication remains failed through the first shutdown and
            // the replacement read; this proves recovery uses the SQLite intent.
            if scope.is_err() {
                drop(release);
                let _ = node.shutdown(ShutdownMode::Preserve).await;
                return scope.map(|_| ());
            }
            let scope = scope?;
            node.shutdown(ShutdownMode::Preserve).await?;
            // Preserve leaves the old owner lease in shared storage. Let this
            // fixture's 1 s authority expire before a differently named owner.
            tokio::time::sleep(Duration::from_millis(1_100)).await;
            let (config, services) = inputs(
                "erasure-replacement",
                &directory.path().join("replacement"),
                &core,
                Some(bucket),
                0,
            )
            .await?;
            let node =
                Node::start(config, services, graph(), erasing_app(signals.clone())?).await?;
            let restarted = async {
                ready(&node).await?;
                let handlers = signals.handlers.load(Ordering::SeqCst);
                let response = client
                    .get(format!("http://{}/read", node.info().public_address))
                    .send()
                    .await?;
                ensure!(
                    !response.status().is_success(),
                    "replacement served erased state"
                );
                ensure!(
                    signals.handlers.load(Ordering::SeqCst) == handlers,
                    "replacement entered erased business code"
                );
                if fail_publication {
                    ensure!(
                        response.text().await?.contains("source business admission"),
                        "replacement did not restore closed SQLite intent"
                    );
                }
                drop(release);
                wait_erased(&node, &scope).await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            restarted?;
            stopped?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    observer.shutdown().await;
    result?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn warm_retirement_admission_reuses_residency_but_restart_rechecks_storage() -> Result<()> {
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = observer
        .run(async {
            let directory = tempfile::tempdir()?;
            let mut bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            let reads = Arc::new(Reads::default());
            bucket.store = Arc::new(RetirementStore {
                inner: bucket.store.clone(),
                reads: reads.clone(),
            });
            let core = Executor::new("retirement-admission-core")?;
            let path = directory.path().join("owner");
            let (config, services) =
                inputs("retirement-owner", &path, &core, Some(bucket.clone()), 0).await?;
            let node =
                Node::start(config, services, graph(), applications("admission", None)?).await?;
            let client = client()?;
            let warm = async {
                ready(&node).await?;
                ensure!(
                    call(&client, &node, reqwest::Method::POST, "/write").await? == "admission:1"
                );
                let before = reads.count.load(Ordering::SeqCst);
                ensure!(
                    before > 0,
                    "cold activation never checked retirement storage"
                );
                reads.refuse.store(true, Ordering::SeqCst);
                for _ in 0..8 {
                    ensure!(
                        call(&client, &node, reqwest::Method::GET, "/read").await? == "admission:1"
                    );
                }
                ensure!(
                    reads.count.load(Ordering::SeqCst) == before,
                    "resident reads consulted remote retirement storage"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            let stop = node.shutdown(ShutdownMode::Preserve).await;
            warm?;
            stop?;

            // Preserve retains the predecessor's 1 s ownership lease.
            tokio::time::sleep(Duration::from_millis(1_100)).await;

            // The same durable state on a fresh host has no resident admission.
            // A failed lookup must refuse business execution, never reuse a prior
            // host's successful absence observation.
            let (config, services) = inputs(
                "retirement-replacement",
                &directory.path().join("replacement"),
                &core,
                Some(bucket),
                0,
            )
            .await?;
            let node =
                Node::start(config, services, graph(), applications("admission", None)?).await?;
            let cold = async {
                ready(&node).await?;
                let before = reads.count.load(Ordering::SeqCst);
                let response = client
                    .get(format!("http://{}/read", node.info().public_address))
                    .send()
                    .await?;
                ensure!(
                    !response.status().is_success(),
                    "cold request ignored unavailable retirement storage"
                );
                ensure!(
                    reads.count.load(Ordering::SeqCst) > before,
                    "cold request reused old residency"
                );
                reads.refuse.store(false, Ordering::SeqCst);
                ensure!(
                    call(&client, &node, reqwest::Method::GET, "/read").await? == "admission:1"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            reads.refuse.store(false, Ordering::SeqCst);
            let stop = node.shutdown(ShutdownMode::Preserve).await;
            cold?;
            stop?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    observer.shutdown().await;
    result?
}
