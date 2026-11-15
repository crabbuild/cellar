// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable payload protocol, independently of ordinary KV service admission.

use anyhow::{ensure, Context as _, Result};
use bytes::Bytes;
use cell_groups::{CellGroupPayloadError as Error, CellGroupPayloadLimits, CellGroupPayloadPool};
use cellar::sdk::consensus::{verification::*, *};
use futures_util::{stream::BoxStream, TryStreamExt};
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[path = "support/cell_group_rustfs.rs"]
#[allow(dead_code)]
mod rustfs;

const TIMEOUT: Duration = Duration::from_secs(3);

fn payload_path(path: &Path) -> bool {
    path.as_ref().contains("/cell-payloads-1/")
        && !path.as_ref().contains("/cell-payloads-1/ownership/")
}

#[derive(Debug)]
struct Faults {
    store: Arc<dyn ObjectStore>,
    lost_put: AtomicBool,
    lost_object: AtomicBool,
    denied_get: AtomicBool,
    corrupt_body: AtomicBool,
    ranged_body: AtomicBool,
    pending_body: AtomicBool,
    hold_body: AtomicBool,
    held_digest: std::sync::Mutex<Option<[u8; 32]>>,
    body_resume: Arc<tokio::sync::Notify>,
    body_entered: tokio::sync::Notify,
    lost_control_put: AtomicBool,
    deny_delete: AtomicBool,
    puts: AtomicUsize,
}

impl Faults {
    fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            lost_put: AtomicBool::new(false),
            lost_object: AtomicBool::new(false),
            denied_get: AtomicBool::new(false),
            corrupt_body: AtomicBool::new(false),
            ranged_body: AtomicBool::new(false),
            pending_body: AtomicBool::new(false),
            hold_body: AtomicBool::new(false),
            held_digest: std::sync::Mutex::new(None),
            body_resume: Arc::new(tokio::sync::Notify::new()),
            body_entered: tokio::sync::Notify::new(),
            lost_control_put: AtomicBool::new(false),
            deny_delete: AtomicBool::new(false),
            puts: AtomicUsize::new(0),
        }
    }
}

impl std::fmt::Display for Faults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cell-payload-protocol-faults")
    }
}

#[async_trait::async_trait]
impl ObjectStore for Faults {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if payload_path(path) && self.lost_object.swap(false, Ordering::SeqCst) {
            return Ok(PutResult {
                e_tag: Some("unconfirmed".into()),
                version: None,
            });
        }
        let result = self.store.put_opts(path, payload, options).await?;
        if path.as_ref().contains("/cell-payloads-1/ownership/")
            && self.lost_control_put.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "lost-payload-control-reply",
                source: std::io::Error::other("response lost after ownership CAS").into(),
            });
        }
        if payload_path(path) {
            self.puts.fetch_add(1, Ordering::SeqCst);
            if self.lost_put.swap(false, Ordering::SeqCst) {
                return Err(object_store::Error::Generic {
                    store: "lost-payload-reply",
                    source: std::io::Error::other("response lost after conditional create").into(),
                });
            }
        }
        Ok(result)
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if payload_path(path) && self.denied_get.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "payload-get-denied",
                source: std::io::Error::other("payload GET unavailable").into(),
            });
        }
        let mut result = self.store.get_opts(path, options).await?;
        if payload_path(path) {
            if self.ranged_body.load(Ordering::SeqCst) {
                result.range.start = 1;
            }
            if self.pending_body.load(Ordering::SeqCst) {
                result.payload =
                    GetResultPayload::Stream(Box::pin(futures_util::stream::pending()));
            } else if self.hold_body.load(Ordering::SeqCst)
                && self
                    .held_digest
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_none_or(|digest| {
                        path.filename()
                            .is_some_and(|name| name.ends_with(&hex::encode(digest)))
                    })
            {
                use futures_util::StreamExt;
                self.body_entered.notify_one();
                let resume = self.body_resume.clone();
                let original = std::mem::replace(
                    &mut result.payload,
                    GetResultPayload::Stream(Box::pin(futures_util::stream::empty())),
                );
                let mut held = GetResult {
                    payload: original,
                    meta: result.meta.clone(),
                    range: result.range.clone(),
                    attributes: result.attributes.clone(),
                }
                .into_stream();
                result.payload = GetResultPayload::Stream(Box::pin(
                    futures_util::stream::once(async move {
                        resume.notified().await;
                        Ok(Bytes::new())
                    })
                    .chain(futures_util::stream::poll_fn(move |cx| {
                        held.as_mut().poll_next(cx)
                    })),
                ));
            } else if self.corrupt_body.load(Ordering::SeqCst) {
                let bytes = vec![0; result.meta.size as usize];
                result.payload =
                    GetResultPayload::Stream(Box::pin(futures_util::stream::once(async {
                        Ok(bytes.into())
                    })));
            }
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, options).await
    }

    async fn delete(&self, path: &Path) -> object_store::Result<()> {
        if payload_path(path) && self.deny_delete.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "payload-delete-denied",
                source: std::io::Error::other("DELETE unavailable").into(),
            });
        }
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

async fn qualify_ownership(store: Arc<dyn ObjectStore>) -> Result<()> {
    let faults = Arc::new(Faults::new(store));
    let cluster = ClusterId::new([21; 16])?;
    let namespace = NamespaceId::new([22; 16])?;
    let config = NamespaceConfig::new(cluster, namespace, NamespaceMode::Raft, [23; 32]);
    let catalog = Catalog::new(faults.clone(), CatalogOptions::default())?;
    let ns = catalog.open_namespace(config.clone()).await?;
    let pool = CellGroupPayloadPool::new(Default::default())?;
    let payloads = ns.cell_payloads(&pool)?;
    let history = CellHistory {
        cluster,
        namespace,
        cell: CellId::new("__KvNamespace:retained-roots")?,
        incarnation: IncarnationId::new([24; 16])?,
    };
    let old = payloads
        .publish(
            &history,
            Bytes::from_static(b"retained recovery cut"),
            TIMEOUT,
        )
        .await?;
    let cut = payloads.retain(&old, [25; 32], TIMEOUT).await?;
    let transfer = payloads.retain(&old, [26; 32], TIMEOUT).await?;
    let current = payloads
        .publish(
            &history,
            Bytes::from_static(b"replacement current value"),
            TIMEOUT,
        )
        .await?;
    faults.lost_control_put.store(true, Ordering::SeqCst);
    ensure!(
        payloads.retain(&current, [27; 32], TIMEOUT).await.is_err(),
        "lost CAS reply must remain uncertain"
    );
    let current_hold = payloads.retain(&current, [27; 32], TIMEOUT).await?;
    let pending = payloads
        .publish(&history, Bytes::from_static(b"interrupted upload"), TIMEOUT)
        .await?;
    let orphan = payloads
        .publish(
            &history,
            Bytes::from_static(b"joined unreferenced upload"),
            TIMEOUT,
        )
        .await?;
    payloads.abort_upload(&orphan, TIMEOUT).await?;
    let first = payloads.collect(Default::default(), TIMEOUT).await?;
    ensure!(first.deleted_objects == 1 && first.protected_objects == 3);
    ensure!(matches!(
        payloads.get(&history, orphan.reference(), TIMEOUT).await,
        Err(Error::Retired)
    ));
    let reopened = catalog
        .existing_namespace(config)
        .await?
        .cell_payloads(&pool)?;
    let cut: cell_groups::CellGroupPayloadHold =
        serde_json::from_slice(&serde_json::to_vec(&cut)?)?;
    reopened.release(&transfer, TIMEOUT).await?;
    ensure!(
        reopened
            .get(&history, old.reference(), TIMEOUT)
            .await?
            .as_ref()
            == b"retained recovery cut"
    );
    ensure!(
        reopened
            .collect(Default::default(), TIMEOUT)
            .await?
            .deleted_objects
            == 0,
        "older retained cut lost its payload"
    );

    // Retire the last recovery root while an actual provider body is suspended.
    // The remote reader pin must survive root retirement and a second node pool.
    faults.hold_body.store(true, Ordering::SeqCst);
    *faults.held_digest.lock().unwrap() = Some(old.reference().digest());
    let mut reader = Box::pin(reopened.get(&history, old.reference(), TIMEOUT));
    match futures_util::future::select(Box::pin(faults.body_entered.notified()), reader.as_mut())
        .await
    {
        futures_util::future::Either::Left(_) => (),
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("reader never entered its held provider body")
        }
    }
    reopened.release(&cut, TIMEOUT).await?;
    let other_pool = CellGroupPayloadPool::new(Default::default())?;
    let other = ns.cell_payloads(&other_pool)?;
    ensure!(
        other
            .collect(Default::default(), TIMEOUT)
            .await?
            .deleted_objects
            == 0,
        "collector deleted an entered reader's bytes"
    );
    faults.hold_body.store(false, Ordering::SeqCst);
    faults.body_resume.notify_one();
    let body = reader.await?;
    ensure!(body.as_ref() == b"retained recovery cut");
    ensure!(
        other
            .collect(Default::default(), TIMEOUT)
            .await?
            .deleted_objects
            == 1
    );
    ensure!(
        body.as_ref() == b"retained recovery cut",
        "retained local body changed after remote reclamation"
    );
    drop(body);
    ensure!(
        matches!(
            reopened
                .publish_prepared(
                    &history,
                    old.reference().clone(),
                    Bytes::from_static(b"retained recovery cut"),
                    TIMEOUT
                )
                .await,
            Err(Error::Retired)
        ),
        "retired allocation resurrected under a delayed publication"
    );
    let fresh = reopened
        .publish(
            &history,
            Bytes::from_static(b"retained recovery cut"),
            TIMEOUT,
        )
        .await?;
    ensure!(
        fresh.reference() != old.reference(),
        "republication reused a collected allocation"
    );
    reopened.abort_upload(&fresh, TIMEOUT).await?;
    ensure!(
        reopened
            .get(&history, pending.reference(), TIMEOUT)
            .await?
            .as_ref()
            == b"interrupted upload"
    );
    ensure!(
        reopened
            .get(&history, current.reference(), TIMEOUT)
            .await?
            .as_ref()
            == b"replacement current value"
    );
    reopened.release(&current_hold, TIMEOUT).await?;
    faults.deny_delete.store(true, Ordering::SeqCst);
    ensure!(other.collect(Default::default(), TIMEOUT).await.is_err());
    faults.deny_delete.store(false, Ordering::SeqCst);
    other.collect(Default::default(), TIMEOUT).await?;
    ensure!(matches!(
        other.get(&history, current.reference(), TIMEOUT).await,
        Err(Error::Retired)
    ));
    ensure!(
        other
            .get(&history, pending.reference(), TIMEOUT)
            .await?
            .as_ref()
            == b"interrupted upload"
    );
    ensure!(pool.usage().operations == 0 && pool.usage().retained_bytes == 0);
    eprintln!("payload GC retained recovery cuts, transfer roots, pending uploads and actual readers; lost CAS and DELETE interruption reconciled");
    Ok(())
}

async fn joined_payload_cleanup(pool: &CellGroupPayloadPool) -> Result<()> {
    cellar::asyncrt::timeout(TIMEOUT, async {
        while pool.usage().operations != 0 {
            cellar::asyncrt::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("payload cleanup retained its operation admission")?;
    ensure!(pool.usage().retained_bytes == 0);
    Ok(())
}

async fn qualify(store: Arc<dyn ObjectStore>) -> Result<()> {
    qualify_ownership(store.clone()).await?;
    let faults = Arc::new(Faults::new(store));
    let cluster = ClusterId::new([1; 16])?;
    let namespace = NamespaceId::new([2; 16])?;
    let catalog = Catalog::new(faults.clone(), CatalogOptions::default())?;
    let config = NamespaceConfig::new(cluster, namespace, NamespaceMode::Raft, [3; 32]);
    let ns = catalog.open_namespace(config.clone()).await?;
    let fleet = catalog
        .open_namespace(NamespaceConfig::new(
            cluster,
            NamespaceId::new([6; 16])?,
            NamespaceMode::Fleet,
            [3; 32],
        ))
        .await?;
    let pool = CellGroupPayloadPool::new(CellGroupPayloadLimits {
        max_operations: 1,
        max_retained_bytes: 80 * 1024 * 1024,
        ..Default::default()
    })?;
    ensure!(matches!(
        fleet.cell_payloads(&pool),
        Err(CatalogError::Invalid(_))
    ));
    let payloads = ns.cell_payloads(&pool)?;
    let history = CellHistory {
        cluster,
        namespace,
        cell: CellId::new("__KvNamespace:payload-protocol")?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let bytes = Bytes::from(vec![117; cellar_logic::kv::MAX_INLINE_VALUE_BYTES + 1]);
    faults.lost_put.store(true, Ordering::SeqCst);
    let published = payloads.publish(&history, bytes.clone(), TIMEOUT).await?;
    ensure!(
        !faults.lost_put.load(Ordering::SeqCst),
        "fault reached a real conditional create"
    );
    let reference = published.reference();
    ensure!(reference.length() == bytes.len() as u64);
    ensure!(
        payloads
            .publish_prepared(&history, reference.clone(), bytes.clone(), TIMEOUT)
            .await?
            .reference()
            == reference
    );
    ensure!(
        faults.puts.load(Ordering::SeqCst) == 1,
        "verified objects are reused without overwrite"
    );
    let got = payloads.get(&history, reference, TIMEOUT).await?;
    ensure!(got.as_ref() == bytes.as_ref());
    ensure!(pool.usage().operations == 0 && pool.usage().retained_bytes == bytes.len());
    drop(got);
    ensure!(pool.usage().retained_bytes == 0);
    faults.denied_get.store(true, Ordering::SeqCst);
    ensure!(matches!(
        payloads
            .publish_prepared(&history, reference.clone(), bytes.clone(), TIMEOUT)
            .await,
        Err(Error::Storage(_))
    ));
    ensure!(
        faults.puts.load(Ordering::SeqCst) == 1,
        "GET refusal cannot authorize an overwrite"
    );
    faults.denied_get.store(false, Ordering::SeqCst);
    faults.lost_object.store(true, Ordering::SeqCst);
    ensure!(matches!(
        payloads
            .publish(&history, Bytes::from_static(b"unconfirmed"), TIMEOUT)
            .await,
        Err(Error::Unconfirmed)
    ));

    for field in ["cell", "namespace", "cluster"] {
        let mut other = history.clone();
        match field {
            "cell" => other.cell = CellId::new("__KvNamespace:other")?,
            "namespace" => other.namespace = NamespaceId::new([5; 16])?,
            "cluster" => other.cluster = ClusterId::new([5; 16])?,
            _ => unreachable!(),
        }
        ensure!(matches!(
            payloads.get(&other, reference, TIMEOUT).await,
            Err(Error::Invalid(_))
        ));
    }
    let serialized = serde_json::to_value(reference)?;
    for (field, value) in [
        ("format", serde_json::json!(2)),
        ("length", serde_json::json!(u64::MAX)),
        ("writer", serde_json::json!(vec![0u8; 16])),
    ] {
        let mut invalid = serialized.clone();
        invalid[field] = value;
        let invalid = serde_json::from_value(invalid)?;
        ensure!(matches!(
            payloads.get(&history, &invalid, TIMEOUT).await,
            Err(Error::Invalid(_))
        ));
    }
    let mut unknown = serialized.clone();
    unknown["unexpected"] = serde_json::json!(true);
    ensure!(serde_json::from_value::<cell_groups::CellGroupPayloadReference>(unknown).is_err());
    let mut missing = serialized.clone();
    missing["digest"] = serde_json::json!(vec![255u8; 32]);
    ensure!(matches!(
        payloads
            .get(&history, &serde_json::from_value(missing)?, TIMEOUT)
            .await,
        Err(Error::Invalid(_))
    ));
    ensure!(matches!(
        payloads.publish(&history, Bytes::new(), TIMEOUT).await,
        Err(Error::Invalid(_))
    ));
    ensure!(matches!(
        payloads.get(&history, reference, Duration::ZERO).await,
        Err(Error::DeadlineExceeded)
    ));

    // No group or term participates in the key. A new cell incarnation can
    // read a separately restored row, but writes a distinct immutable subtree.
    let mut recovered = history.clone();
    recovered.incarnation = IncarnationId::new([9; 16])?;
    ensure!(payloads.get(&recovered, reference, TIMEOUT).await?.as_ref() == bytes.as_ref());
    let fresh = payloads.publish(&recovered, bytes.clone(), TIMEOUT).await?;
    ensure!(fresh.reference() != reference);
    ensure!(faults.puts.load(Ordering::SeqCst) == 2);
    let reopened = catalog
        .existing_namespace(config)
        .await?
        .cell_payloads(&pool)?;
    ensure!(reopened.get(&recovered, reference, TIMEOUT).await?.as_ref() == bytes.as_ref());

    faults.corrupt_body.store(true, Ordering::SeqCst);
    ensure!(matches!(
        payloads.get(&history, reference, TIMEOUT).await,
        Err(Error::Invalid(_))
    ));
    ensure!(matches!(
        payloads
            .publish_prepared(&history, reference.clone(), bytes.clone(), TIMEOUT)
            .await,
        Err(Error::Invalid(_))
    ));
    ensure!(
        faults.puts.load(Ordering::SeqCst) == 2,
        "corruption must never trigger overwrite"
    );
    faults.corrupt_body.store(false, Ordering::SeqCst);
    faults.ranged_body.store(true, Ordering::SeqCst);
    ensure!(matches!(
        payloads.get(&history, reference, TIMEOUT).await,
        Err(Error::Invalid(_))
    ));
    faults.ranged_body.store(false, Ordering::SeqCst);

    faults.pending_body.store(true, Ordering::SeqCst);
    ensure!(matches!(
        payloads
            .get(&history, reference, Duration::from_millis(30))
            .await,
        Err(Error::DeadlineExceeded)
    ));
    joined_payload_cleanup(&pool).await?;
    ensure!(pool.usage().retained_bytes == 0 && pool.usage().operations == 0);
    // Cancel a live body, then prove the same node-wide operation bank is free.
    let mut pending = Box::pin(payloads.get(&history, reference, TIMEOUT));
    ensure!(matches!(
        futures_util::poll!(pending.as_mut()),
        std::task::Poll::Pending
    ));
    ensure!(pool.usage().operations == 1);
    ensure!(matches!(
        reopened.get(&history, reference, TIMEOUT).await,
        Err(Error::Overloaded)
    ));
    drop(pending);
    faults.pending_body.store(false, Ordering::SeqCst);
    joined_payload_cleanup(&pool).await?;
    ensure!(payloads.get(&history, reference, TIMEOUT).await?.as_ref() == bytes.as_ref());
    ensure!(pool.usage().operations == 0 && pool.usage().retained_bytes == 0);

    // Separate node pools race the same immutable key. Either conditional
    // create winner is acceptable; both callers must confirm identical bytes.
    let other_pool = CellGroupPayloadPool::new(Default::default())?;
    let other_node = ns.cell_payloads(&other_pool)?;
    let raced = Bytes::from_static(b"two independent payload publishers");
    use sha2::{Digest, Sha256};
    let prepared =
        payloads.prepare_reference(&history, Sha256::digest(&raced).into(), raced.len() as u64)?;
    let (left, right) = futures_util::join!(
        payloads.publish_prepared(&history, prepared.clone(), raced.clone(), TIMEOUT),
        other_node.publish_prepared(&history, prepared, raced, TIMEOUT)
    );
    ensure!(left?.reference() == right?.reference());
    ensure!(faults.puts.load(Ordering::SeqCst) == 3);

    // A retained body still consumes bytes after freeing its I/O slot.
    let tight_pool = CellGroupPayloadPool::new(CellGroupPayloadLimits {
        max_payload_bytes: bytes.len(),
        max_retained_bytes: bytes.len() * 3 + 4 * 64 * 1024,
        ..Default::default()
    })?;
    let tight = ns.cell_payloads(&tight_pool)?;
    let held = tight.get(&history, reference, TIMEOUT).await?;
    ensure!(tight_pool.usage().operations == 0);
    ensure!(matches!(
        tight.get(&history, reference, TIMEOUT).await,
        Err(Error::Overloaded)
    ));
    drop(held);
    ensure!(tight.get(&history, reference, TIMEOUT).await?.as_ref() == bytes.as_ref());

    let objects: Vec<_> = faults
        .store
        .list(Some(&Path::from(ns.data_prefix())))
        .try_collect()
        .await?;
    ensure!(
        objects
            .iter()
            .filter(|object| payload_path(&object.location))
            .count()
            == 3,
        "retained old and new incarnations survive; there is no unsafe current-row collector"
    );
    let original = objects
        .iter()
        .find(|object| {
            object.location.as_ref().contains(&hex::encode([4u8; 16]))
                && object
                    .location
                    .filename()
                    .is_some_and(|name| name.ends_with(&hex::encode(reference.digest())))
        })
        .context("original immutable payload object")?;
    faults
        .store
        .put(&original.location, Bytes::from(vec![0; bytes.len()]).into())
        .await?;
    ensure!(
        matches!(
            payloads
                .publish_prepared(&history, reference.clone(), bytes.clone(), TIMEOUT)
                .await,
            Err(Error::Invalid(_))
        ),
        "actual stored corruption must remain refused"
    );
    ensure!(
        faults
            .store
            .get(&original.location)
            .await?
            .bytes()
            .await?
            .iter()
            .all(|byte| *byte == 0),
        "publisher must leave corrupted evidence intact"
    );
    ensure!(
        payloads
            .get(&recovered, fresh.reference(), TIMEOUT)
            .await?
            .as_ref()
            == bytes.as_ref()
    );
    eprintln!("payload publication/read-back, cold reopen, incarnation reuse, corruption refusal, body deadline and cancellation passed");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn immutable_payloads_confirm_bytes_and_preserve_old_recovery_cuts() -> Result<()> {
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(qualify(Arc::new(object_store::memory::InMemory::new())))
        .await;
    domain.shutdown().await;
    result.context("native immutable payload protocol")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires explicit isolated real RustFS credentials and prefix"]
#[allow(clippy::disallowed_methods)]
async fn rustfs_immutable_payloads_confirm_bytes_and_preserve_old_recovery_cuts() -> Result<()> {
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async { qualify(rustfs::store("immutable-cell-payloads", false)?).await })
        .await;
    domain.shutdown().await;
    result.context("RustFS immutable payload protocol")?
}
