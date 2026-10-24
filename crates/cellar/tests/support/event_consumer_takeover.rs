// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real inbox acceptance and effect commit from a poll spanning owner fencing.

use super::event_clock_retention::{history, pending, wall};
use super::event_settlement_race::status;
use super::*;
use base64::Engine;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};
use tokio::sync::{mpsc, Semaphore};

#[derive(Clone)]
struct Delivery {
    record: Record,
    receipt: Receipt,
    expires: i64,
}

struct Controls {
    before_commit: bool,
    deliveries: Mutex<BTreeMap<String, Delivery>>,
    acceptance: Mutex<Option<Result<bool, String>>>,
    entered: Semaphore,
    release: (Mutex<bool>, Condvar),
    attempted: mpsc::UnboundedSender<Result<(), String>>,
}

impl Controls {
    fn wait(&self) -> Result<()> {
        self.entered.add_permits(1);
        let (released, _) = self
            .release
            .1
            .wait_timeout_while(
                self.release.0.lock().unwrap(),
                Duration::from_secs(15),
                |released| !*released,
            )
            .unwrap();
        ensure!(*released, "test did not release the running consumer");
        Ok(())
    }

    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }

    fn delivery(&self, slot: &str) -> Result<Delivery> {
        self.deliveries
            .lock()
            .unwrap()
            .get(slot)
            .cloned()
            .context("actual SDK delivery missing")
    }
}

struct Release(Arc<Controls>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Consumer(Arc<Controls>);

fn state(db: &Storage) -> Result<Value> {
    Ok(json!({
        "processed": db.get_json("processed")?.unwrap_or(json!(0)),
        "effects": db.get_json("effects")?.unwrap_or(json!(0)),
        "last_event": db.get_json("last_event")?,
    }))
}

fn effect(tx: &Storage, record: &Record) -> Result<()> {
    let prior = tx
        .get_json("processed")?
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let count = tx
        .get_json("effects")?
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    tx.put_json(
        "processed",
        &json!(prior + record.event.decode::<Changed>()?.value),
    )?;
    tx.put_json("effects", &json!(count + 1))?;
    tx.put_json("last_event", &json!(record.event.id))?;
    Ok(())
}

impl Cell for Consumer {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let url = url::Url::parse(&request.url)?;
            let db = context.storage()?;
            if url.path() == "/takeover-audit" {
                return Ok(super::super::response(serde_json::to_string(&state(&db)?)?));
            }
            let subscription = context
                .stream::<Changed>("EVENTS")?
                .subscription("projection")?;
            let value = if url.path() == "/takeover-first" || url.path() == "/takeover-running" {
                let first = url.path() == "/takeover-first";
                let batch = subscription
                    .next_with_id(
                        &context,
                        if first {
                            "000000000000000000000000000000d1"
                        } else {
                            "000000000000000000000000000000d2"
                        },
                        BatchOptions {
                            max_records: 1,
                            max_bytes: 4096,
                            wait_ms: 0,
                        },
                    )
                    .await?
                    .context("expected one real event")?;
                ensure!(
                    batch.records.len() == 1 && batch.expires_at_ms - batch.issued_at_ms == 30_000
                );
                let delivery = Delivery {
                    record: batch.records[0].clone(),
                    receipt: batch.receipt().clone(),
                    expires: batch.expires_at_ms,
                };
                {
                    let mut deliveries = self.0.deliveries.lock().unwrap();
                    ensure!(deliveries.len() < 2);
                    ensure!(deliveries
                        .insert(
                            if first { "first" } else { "second" }.into(),
                            delivery.clone()
                        )
                        .is_none());
                }
                // Retain the actual SDK handle before fencing. The first cut
                // enters the transaction before closure, then calls accept.
                let inbox = db.inbox("projection")?;
                if first {
                    db.tx(|tx| {
                        ensure!(inbox.accept(&delivery.record)?);
                        effect(tx, &delivery.record)
                    })?;
                    db.sync().await?;
                    ensure!(
                        subscription.ack(&context, &delivery.receipt).await?
                            == Acknowledgment::Acknowledged
                    );
                    state(&db)?
                } else {
                    let result = db.tx(|tx| {
                        if !self.0.before_commit {
                            self.0.wait()?;
                        }
                        let acceptance = inbox.accept(&delivery.record);
                        *self.0.acceptance.lock().unwrap() = Some(
                            acceptance
                                .as_ref()
                                .copied()
                                .map_err(|error| format!("{error:#}")),
                        );
                        ensure!(acceptance?, "second delivery already accepted");
                        effect(tx, &delivery.record)?;
                        if self.0.before_commit {
                            self.0.wait()?;
                        }
                        Ok(())
                    });
                    self.0
                        .attempted
                        .send(result.map_err(|e| format!("{e:#}")))?;
                    // Swallowing a local refusal cannot create HTTP success.
                    return Ok(super::super::response("stale-success"));
                }
            } else if url.path() == "/takeover-apply" {
                let slot = url
                    .query_pairs()
                    .find(|(k, _)| k == "slot")
                    .map(|(_, v)| v.into_owned())
                    .context("delivery slot missing")?;
                ensure!(["first", "second"].contains(&slot.as_str()));
                let delivery = self.0.delivery(&slot)?;
                let accepted = db.tx(|tx| {
                    if !tx.inbox("projection")?.accept(&delivery.record)? {
                        return Ok(false);
                    }
                    effect(tx, &delivery.record)?;
                    Ok(true)
                })?;
                db.sync().await?;
                json!({"accepted":accepted,"state":state(&db)?})
            } else {
                anyhow::bail!("unknown consumer takeover route")
            };
            Ok(super::super::response(serde_json::to_string(&value)?))
        })
    }
}

fn applications(controls: Arc<Controls>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let controls = controls.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let cell = context.namespace("ACCOUNTS")?.get_by_name("projection");
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(Consumer(controls.clone())))
            })?,
        ))
    })
}

async fn json_call(client: &reqwest::Client, node: &Node, path: &str) -> Result<Value> {
    Ok(serde_json::from_str(
        &call(client, node, reqwest::Method::POST, path).await?,
    )?)
}

fn scope() -> String {
    let id = cellar_native::cell_id::for_name("cells:v1:13:embedded-node:Account", "projection");
    format!("Account:{}", cellar_native::cell_id::encode_hex(&id))
}

async fn owner(observer: &ExecutionDomain, bucket: &cellar::bucket::Bucket) -> Result<Value> {
    observer
        .run(async {
            let (bytes, _) = bucket
                .get(&format!("cells/{}/own.json", scope()))
                .await?
                .context("consumer owner missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

// The original domain has fully joined before this runs. Inspect a copied
// SQLite/WAL family, never an active connection or a repaired/replayed source.
fn audit_joined_old(
    directory: &std::path::Path,
    epoch: u64,
    first: &Delivery,
    original: &Value,
) -> Result<()> {
    use sha2::{Digest, Sha256};
    let family = directory
        .join("old")
        .join(scope())
        .join("ltx")
        .join(format!("e{epoch}"));
    let copy = directory.join("old-audit");
    std::fs::create_dir(&copy)?;
    let mut captured = Vec::new();
    for name in ["db.sqlite", "db.sqlite-wal", "db.sqlite-shm"] {
        let path = family.join(name);
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            std::fs::write(copy.join(name), &bytes)?;
            captured.push((path, bytes));
        }
    }
    ensure!(
        copy.join("db.sqlite").is_file(),
        "joined consumer database missing"
    );
    let db = rusqlite::Connection::open_with_flags(
        copy.join("db.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut observed = serde_json::Map::new();
    for key in ["processed", "effects", "last_event"] {
        let value: String = db.query_row(
            "SELECT v FROM __sys_cell_kv WHERE scope=?1 AND k=?2",
            rusqlite::params![scope(), key],
            |row| row.get(0),
        )?;
        observed.insert(key.into(), serde_json::from_str(&value)?);
    }
    ensure!(
        Value::Object(observed) == *original,
        "joined old database retained stale business writes"
    );
    let (rows, counter): (i64, i64) = db.query_row(
        "SELECT (SELECT count(*) FROM __sys_inbox), rows FROM __sys_inbox_stats WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(
        (rows, counter) == (1, 1),
        "joined old database retained an uncommitted inbox receipt"
    );
    let (id, digest): (String, Vec<u8>) = db.query_row(
        "SELECT event_id,digest FROM __sys_inbox WHERE processing_scope='projection'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    ensure!(
        id == first.record.event.id && digest == Sha256::digest(first.record.encoded()).as_slice()
    );
    drop(db);
    for (path, bytes) in captured {
        ensure!(
            std::fs::read(path)? == bytes,
            "old audit changed original SQLite family"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_consumer_cannot_accept_after_fenced_takeover() -> Result<()> {
    run(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_consumer_receipt_and_effect_cannot_commit_after_fenced_takeover() -> Result<()> {
    run(true).await
}

async fn run(before_commit: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let result = run_in(&directory, before_commit).await;
    if let Err(error) = &result {
        let path = directory.keep();
        eprintln!(
            "CONSUMER_TAKEOVER_FAILURE before_commit={before_commit} retained={} error={error:#}",
            path.display()
        );
    }
    result
}

async fn run_in(directory: &tempfile::TempDir, before_commit: bool) -> Result<()> {
    let destination_core = Executor::new("consumer-takeover-destination")?;
    let old_core = Executor::new("consumer-takeover-old")?;
    let new_core = Executor::new("consumer-takeover-new")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (isolated, fault) = event_owner_outage::Outage::wrap(&bucket)?;
    let (send, mut attempted) = mpsc::unbounded_channel();
    let controls = Arc::new(Controls {
        before_commit,
        deliveries: Mutex::new(BTreeMap::new()),
        acceptance: Mutex::new(None),
        entered: Semaphore::new(0),
        release: (Mutex::new(false), Condvar::new()),
        attempted: send,
    });
    let _release = Release(controls.clone());
    let (config, mut services) = inputs(
        "consumer-destination",
        &directory.path().join("destination"),
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    services.fleet.as_mut().unwrap().fleet_durability = false;
    let mut declaration = graph(1, false);
    declaration.primary.event_streams[0].retention = Retention::Required;
    declaration.primary.event_streams[0].subscriptions[0].required = true;
    let destination = Node::start(
        config,
        services,
        declaration,
        super::applications("destination")?,
    )
    .await
    .with_context(|| format!("start consumer destination before_commit={before_commit}"))?;
    super::super::ready(&destination).await?;
    let (config, mut services) = inputs(
        "consumer-old",
        &directory.path().join("old"),
        &old_core,
        Some(isolated),
        0,
    )
    .await?;
    ensure!(config.actor.core.operation_deadline_ms == Some(3000));
    services.fleet.as_mut().unwrap().fleet_durability = false;
    let mut declaration = graph(1, false);
    declaration.primary.event_streams.clear();
    let old = Node::start(
        config,
        services,
        declaration,
        applications(controls.clone())?,
    )
    .await
    .with_context(|| format!("start consumer old before_commit={before_commit}"))?;
    super::super::ready(&old).await?;
    let client = client()?;
    for value in 1..=2 {
        ensure!(
            call(
                &client,
                &destination,
                reqwest::Method::POST,
                "/write?source=producer"
            )
            .await?
                == format!("destination:{value}")
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while pending(&client, &destination).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let original_history = history(&client, &destination).await?;
    ensure!(original_history
        .as_array()
        .is_some_and(|rows| rows.len() == 2));
    let original = json_call(&client, &old, "/takeover-first").await?;
    let first = controls.delivery("first")?;
    ensure!(
        first.record.offset == 1
            && original == json!({"processed":1,"effects":1,"last_event":first.record.event.id})
    );
    let old_owner = owner(&observer, &bucket).await?;
    ensure!(old_owner["node"] == "consumer-old");
    let old_epoch = old_owner["epoch"].as_u64().context("old epoch missing")?;
    let request = tokio::spawn(
        client
            .post(format!(
                "http://{}/takeover-running",
                old.info().public_address
            ))
            .send(),
    );
    tokio::time::timeout(Duration::from_secs(5), controls.entered.acquire())
        .await??
        .forget();
    let second = controls.delivery("second")?;
    ensure!(second.record.offset == 2 && second.record.event.decode::<Changed>()?.value == 2);
    let before = status(&client, &destination).await?;
    ensure!(
        before["checkpoint"] == "1"
            && before["observation"]["lease_active"] == true
            && wall() < second.expires
    );
    fault.isolate();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !old.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("old consumer did not fence while its poll was still executing")?;
    ensure!(fault.refused() > 0 && attempted.try_recv().is_err());
    let (config, mut services) = inputs(
        "consumer-new",
        &directory.path().join("new"),
        &new_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    services.fleet.as_mut().unwrap().fleet_durability = false;
    let mut declaration = graph(1, false);
    declaration.primary.event_streams.clear();
    let successor = Node::start(
        config,
        services,
        declaration,
        applications(controls.clone())?,
    )
    .await
    .with_context(|| format!("start consumer successor before_commit={before_commit}"))?;
    super::super::ready(&successor).await?;
    // This first successor call is read-only. No replay can repair its prefix.
    ensure!(json_call(&client, &successor, "/takeover-audit").await? == original);
    let new_owner = owner(&observer, &bucket).await?;
    ensure!(
        new_owner["node"] == "consumer-new"
            && new_owner["epoch"].as_u64().context("new epoch missing")? > old_epoch
    );
    ensure!(history(&client, &destination).await? == original_history);
    let at_cut = status(&client, &destination).await?;
    for field in ["identity", "checkpoint", "lease", "end", "earliest"] {
        ensure!(
            at_cut[field] == before[field],
            "takeover changed subscription {field}"
        );
    }
    ensure!(
        wall() < second.expires && attempted.try_recv().is_err(),
        "old poll ended or delivery expired before successor audit"
    );
    ensure!(
        *controls.acceptance.lock().unwrap() == if before_commit { Some(Ok(true)) } else { None },
        "acceptance cut not established before release"
    );
    controls.release();
    let refusal = tokio::time::timeout(Duration::from_secs(5), attempted.recv())
        .await?
        .context("old consumer never attempted storage")?
        .expect_err("stale inbox/effect transaction committed");
    ensure!(
        refusal.contains("execution domain is closed"),
        "unexpected consumer refusal: {refusal}"
    );
    let acceptance = controls
        .acceptance
        .lock()
        .unwrap()
        .clone()
        .context("old callback never attempted inbox acceptance")?;
    if before_commit {
        ensure!(
            acceptance == Ok(true),
            "receipt was not accepted before the commit cut"
        );
    } else {
        let refusal = acceptance.expect_err("closed consumer accepted an inbox receipt");
        ensure!(
            refusal.contains("execution domain is closed"),
            "unexpected inbox refusal: {refusal}"
        );
    }
    let result = tokio::time::timeout(Duration::from_secs(5), request).await??;
    ensure!(
        result.is_err() || !result.as_ref().unwrap().status().is_success(),
        "fenced consumer returned success"
    );
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        old.run_until(std::future::pending()),
    )
    .await??;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseExpired
            }
    );
    audit_joined_old(directory.path(), old_epoch, &first, &original)?;
    ensure!(json_call(&client, &successor, "/takeover-audit").await? == original);
    ensure!(history(&client, &destination).await? == original_history);
    let after = status(&client, &destination).await?;
    for field in ["identity", "checkpoint", "lease", "end", "earliest"] {
        ensure!(
            after[field] == before[field],
            "stale consumer changed subscription {field}"
        );
    }
    // Only after independent prefix audits and the old domain joins do we
    // probe retained inbox identity or apply the pending original event.
    ensure!(
        json_call(&client, &successor, "/takeover-apply?slot=first").await?
            == json!({"accepted":false,"state":original})
    );
    let applied = json_call(&client, &successor, "/takeover-apply?slot=second").await?;
    ensure!(
        applied
            == json!({"accepted":true,"state":{"processed":3,"effects":2,"last_event":second.record.event.id}})
    );
    ensure!(
        json_call(&client, &successor, "/takeover-apply?slot=second").await?
            == json!({"accepted":false,"state":applied["state"]})
    );
    ensure!(
        super::event_settlement_race::settle(
            &client,
            &destination,
            "producer",
            "ack",
            &second.receipt,
            0
        )
        .await?
            == json!({"status":"Acknowledged"})
    );
    ensure!(status(&client, &destination).await?["checkpoint"] == "2");
    eprintln!(
        "CONSUMER_TAKEOVER_EVIDENCE {}",
        json!({"before_commit":before_commit,"old_epoch":old_epoch,"new_epoch":new_owner["epoch"],"refusal":refusal,"original":original,"final":applied,"envelope":base64::engine::general_purpose::STANDARD.encode(second.record.encoded()),"checkpoint":"2"})
    );
    stopped(successor).await?;
    stopped(destination).await?;
    ensure!(
        old_core.handle().spawn(async { 7 }).await? == 7
            && new_core.handle().spawn(async { 7 }).await? == 7
            && destination_core.handle().spawn(async { 7 }).await? == 7
    );
    observer.shutdown().await;
    Ok(())
}
