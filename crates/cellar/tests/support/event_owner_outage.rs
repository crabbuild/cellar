//! Node-specific storage outage with independent pre-replay event audits.
//! This is an injected local-store fault, not physical network isolation.

use super::*;
use anyhow::Result;
use futures_util::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{list::*, path::Path, *};
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;

#[derive(Debug)]
struct Outage {
    store: Arc<dyn ObjectStore>,
    isolated: AtomicBool,
    refused: AtomicUsize,
}

impl Outage {
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
        self.store.put_opts(path, payload, opts).await
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

fn source_scope() -> String {
    let script = "embedded-node";
    let namespace = format!("cells:v1:{}:{script}:Account", script.len());
    let id = cellar_native::cell_id::for_name(&namespace, "events");
    format!("Account:{}", cellar_native::cell_id::encode_hex(&id))
}

async fn inspect(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(operator(
        client,
        node,
        &source_scope(),
        &json!({"op":"inspect"}),
        "outage-audit",
    )
    .await?["result"]
        .clone())
}

async fn subscription(client: &reqwest::Client, node: &Node) -> Result<Value> {
    Ok(stream_operator(
        client,
        node,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "outage-audit",
    )
    .await?["result"]
        .clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_isolated_event_owner_expires_and_recovers_before_replay() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let old_core = Executor::new("isolated-event-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let outage = Arc::new(Outage {
        store: bucket.store.clone(),
        isolated: AtomicBool::new(false),
        refused: AtomicUsize::new(0),
    });
    let isolated = cellar::bucket::Bucket::from_clients(
        outage.clone(),
        outage.clone(),
        outage.clone(),
        bucket.backend,
        bucket.name.clone(),
        bucket.prefix.clone(),
    )?;
    let (mut config, services) = inputs(
        "isolated-event-owner",
        &directory.path().join("old"),
        &old_core,
        Some(isolated),
        0,
    )
    .await?;
    config.cell_limits.events.lease_ms = 10_000;
    let owner = Node::start(config, services, graph(1, false), applications("old")?).await?;
    super::super::ready(&owner).await?;
    let client = client()?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "old:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while inspect(&client, &owner).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        call(
            &client,
            &owner,
            reqwest::Method::POST,
            "/process-save?source=projection"
        )
        .await?
            == "1"
    );
    call(&client, &owner, reqwest::Method::POST, "/revoke-publish").await?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "old:2");
    let pending = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = inspect(&client, &owner).await?;
            if state["lanes"][0]["blocked_reason"] == "Forbidden" {
                return Ok::<_, anyhow::Error>(state);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    ensure!(
        pending["pending_rows"] == "1"
            && pending["lanes"][0]["revision"]["settled_through"] == "1"
            && pending["lanes"][0]["revision"]["next_sequence"] == "3"
    );
    let history = stream_operator(
        &client,
        &owner,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "outage-audit",
    )
    .await?["result"]["records"]
        .clone();
    ensure!(history.as_array().is_some_and(|records| records.len() == 1));
    ensure!(
        history[0]["event"]["payload"] == json!({"value":1})
            && history[0]["event"]["sequence"] == "1"
            && history[0]["event"]["source"] == source_scope()
    );
    let leased = subscription(&client, &owner).await?;
    ensure!(leased["checkpoint"] == "0" && leased["observation"]["lease_active"] == true);
    let old_services = owner.services();
    outage.isolated.store(true, Ordering::Release);
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        owner.run_until(std::future::pending()),
    )
    .await??;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseExpired
            },
        "outage returned {outcome:?}"
    );
    ensure!(
        outage.refused.load(Ordering::Acquire) > 0,
        "outage did not refuse real storage calls"
    );
    ensure!(
        old_services
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err(),
        "fenced domain remained open"
    );
    ensure!(
        old_core.handle().spawn(async { 7 }).await? == 7,
        "fence stopped caller executor"
    );

    // The successor has an independent healthy client and executor. The old
    // client remains isolated throughout recovery and the final audit.
    let new_core = Executor::new("successor-event-core")?;
    let (config, services) = inputs(
        "successor-event-owner",
        &directory.path().join("new"),
        &new_core,
        Some(bucket),
        0,
    )
    .await?;
    let mut declaration = graph(2, false);
    declaration.primary.event_streams[0]
        .grants
        .iter_mut()
        .find(|grant| grant.id == "application")
        .unwrap()
        .publish = false;
    declaration.primary.options.stream_bindings[0].publish = false;
    // This successor binds to the already configured resource. Reissuing its
    // administrative revision under a new deployment operation is a conflict;
    // recovery must not create a new configuration to make the audit pass.
    declaration.primary.event_streams.clear();
    let successor = Node::start(config, services, declaration, applications("new")?).await?;
    super::super::ready(&successor).await?;
    let recovered = inspect(&client, &successor).await?;
    ensure!(
        recovered["incarnation"] == pending["incarnation"]
            && recovered["pending_rows"] == "1"
            && recovered["lanes"][0]["revision"] == pending["lanes"][0]["revision"],
        "source obligation changed before replay: {recovered}"
    );
    ensure!(call(&client, &successor, reqwest::Method::GET, "/read").await? == "new:2");
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );
    let recovered_history = stream_operator(
        &client,
        &successor,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "outage-audit",
    )
    .await?["result"]["records"]
        .clone();
    ensure!(
        recovered_history == history,
        "accepted envelope changed before replay"
    );
    let recovered_lease = subscription(&client, &successor).await?;
    ensure!(
        recovered_lease["identity"] == leased["identity"] && recovered_lease["checkpoint"] == "0",
        "checkpoint advanced during outage"
    );
    if recovered_lease["observation"]["lease_active"] == true {
        ensure!(
            recovered_lease["lease"] == leased["lease"],
            "recovery changed the live lease"
        );
    }
    tokio::time::timeout(Duration::from_secs(12), async {
        while subscription(&client, &successor).await?["observation"]["lease_active"] == true {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/process-save?source=projection"
        )
        .await?
            == "1",
        "redelivery repeated inbox effect"
    );
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/ack-original?source=projection"
        )
        .await?
            == "StaleReceipt"
    );
    ensure!(
        subscription(&client, &successor).await?["checkpoint"] == "0",
        "obsolete receipt advanced checkpoint"
    );
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/ack-stored?source=projection"
        )
        .await?
            == "Acknowledged"
    );
    call(
        &client,
        &successor,
        reqwest::Method::POST,
        "/restore-publish",
    )
    .await?;
    call(&client, &successor, reqwest::Method::POST, "/retry-outbox").await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while inspect(&client, &successor).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    ensure!(
        call(
            &client,
            &successor,
            reqwest::Method::POST,
            "/process-ack?source=projection"
        )
        .await?
            == "3"
    );
    ensure!(subscription(&client, &successor).await?["checkpoint"] == "2");
    ensure!(
        outage.isolated.load(Ordering::Acquire),
        "old client was reconnected to repair recovery"
    );
    stopped(successor).await?;
    Ok(())
}
