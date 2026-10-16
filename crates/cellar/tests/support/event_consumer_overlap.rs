//! Expired and current workers share one durable inbox. The paused callback
//! retains only a real SDK delivery, not fabricated records or proof.

use super::event_clock_retention::{history, number, pending, wall};
use super::event_settlement_race::{settle, status};
use super::*;
use base64::Engine;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;
use tokio::sync::Semaphore;

#[derive(Clone)]
struct Delivery {
    record: Record,
    receipt: Receipt,
    issued: i64,
    expires: i64,
}

impl Delivery {
    fn describe(&self) -> Result<Value> {
        Ok(json!({
            "receipt":self.receipt.encode()?,
            "issued_ms":self.issued.to_string(), "expires_ms":self.expires.to_string(),
            "offset":self.record.offset.to_string(),
            "envelope":base64::engine::general_purpose::STANDARD.encode(self.record.encoded()),
        }))
    }
}

struct Controls {
    deliveries: Mutex<BTreeMap<String, Delivery>>,
    entered: Semaphore,
    release: Semaphore,
}

struct Release(Arc<Controls>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release.add_permits(1);
    }
}

struct Consumer {
    label: &'static str,
    controls: Arc<Controls>,
}

fn effects(db: &Storage) -> Result<Value> {
    Ok(json!({
        "processed":db.get_json("processed")?.unwrap_or(json!(0)),
        "effects":db.get_json("effects")?.unwrap_or(json!(0)),
        "winner":db.get_json("winner")?,
    }))
}

impl Cell for Consumer {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let url = url::Url::parse(&request.url)?;
            if !url.path().starts_with("/overlap-") {
                return Arc::new(Counter(self.label))
                    .invoke(context, Invocation::Fetch(request))
                    .await;
            }
            let fields = url.query_pairs().collect::<BTreeMap<_, _>>();
            let db = context.storage()?;
            if url.path() == "/overlap-audit" {
                return Ok(super::super::response(serde_json::to_string(&effects(
                    &db,
                )?)?));
            }
            let slot = fields
                .get("slot")
                .context("delivery slot missing")?
                .as_ref();
            ensure!(
                ["old", "new", "next"].contains(&slot),
                "invalid delivery slot"
            );
            let subscription = context
                .stream::<Changed>("EVENTS")?
                .subscription("projection")?;
            let value = if url.path() == "/overlap-fetch" {
                let id = fields.get("id").context("fetch ID missing")?;
                let batch = subscription
                    .next_with_id(
                        &context,
                        id,
                        BatchOptions {
                            max_records: 1,
                            max_bytes: 4096,
                            wait_ms: 0,
                        },
                    )
                    .await?
                    .context("expected a delivery")?;
                ensure!(batch.records.len() == 1);
                let delivery = Delivery {
                    record: batch.records[0].clone(),
                    receipt: batch.receipt().clone(),
                    issued: batch.issued_at_ms,
                    expires: batch.expires_at_ms,
                };
                let value = delivery.describe()?;
                let mut deliveries = self.controls.deliveries.lock().unwrap();
                ensure!(deliveries.len() < 3 && !deliveries.contains_key(slot));
                deliveries.insert(slot.to_owned(), delivery);
                value
            } else if url.path() == "/overlap-apply" {
                let delivery = self
                    .controls
                    .deliveries
                    .lock()
                    .unwrap()
                    .get(slot)
                    .cloned()
                    .context("actual delivery missing")?;
                if fields.get("hold").is_some_and(|value| value == "1") {
                    self.controls.entered.add_permits(1);
                    self.controls.release.acquire().await?.forget();
                }
                let accepted = db.tx(|tx| {
                    if !tx.inbox("projection")?.accept(&delivery.record)? {
                        return Ok(false);
                    }
                    let prior = tx
                        .get_json("processed")?
                        .and_then(|value| value.as_i64())
                        .unwrap_or(0);
                    let count = tx
                        .get_json("effects")?
                        .and_then(|value| value.as_i64())
                        .unwrap_or(0);
                    tx.put_json(
                        "processed",
                        &json!(prior + delivery.record.event.decode::<Changed>()?.value),
                    )?;
                    tx.put_json("effects", &json!(count + 1))?;
                    tx.put_json("winner", &json!(slot))?;
                    Ok(true)
                })?;
                db.sync().await?;
                let ack = if fields.get("ack").is_some_and(|value| value == "1") {
                    match subscription.ack(&context, &delivery.receipt).await {
                        Ok(status) => json!({"status":format!("{status:?}")}),
                        Err(error) => {
                            json!({"error":error.downcast_ref::<DeliveryError>().context("untyped stale acknowledgment")?.code.as_str()})
                        }
                    }
                } else {
                    Value::Null
                };
                json!({"accepted":accepted,"state":effects(&db)?,"ack":ack})
            } else {
                anyhow::bail!("unknown overlap route");
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
                    let url = url::Url::parse(&request.url)?;
                    let source = url
                        .query_pairs()
                        .find(|(name, _)| name == "source")
                        .map(|(_, value)| value.into_owned())
                        .unwrap_or_else(|| "events".into());
                    let cell = context.namespace("ACCOUNTS")?.get_by_name(&source);
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(Consumer {
                    label: "consumer",
                    controls: controls.clone(),
                }))
            })?,
        ))
    })
}

async fn json_call(client: &reqwest::Client, node: &Node, path: &str) -> Result<Value> {
    Ok(serde_json::from_str(
        &call(client, node, reqwest::Method::POST, path).await?,
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_consumers_deduplicate_before_current_ack() -> Result<()> {
    run(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_consumers_preserve_next_lease_after_current_ack() -> Result<()> {
    run(true).await
}

async fn run(after_ack: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let destination_core = Executor::new("overlap-destination")?;
    let consumer_core = Executor::new("overlap-consumer")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "overlap-destination",
        &directory.path().join("destination"),
        &destination_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut definition = graph(1, false);
    definition.primary.event_streams[0].retention = Retention::Required;
    definition.primary.event_streams[0].subscriptions[0].required = true;
    let destination = Node::start(
        config,
        services,
        definition,
        super::applications("destination")?,
    )
    .await?;
    super::super::ready(&destination).await?;
    let (config, services) = inputs(
        "overlap-consumer",
        &directory.path().join("consumer"),
        &consumer_core,
        Some(bucket),
        0,
    )
    .await?;
    let budget = Duration::from_millis(
        config
            .actor
            .core
            .operation_deadline_ms
            .context("operation budget missing")?,
    );
    ensure!(budget == Duration::from_secs(3));
    let controls = Arc::new(Controls {
        deliveries: Mutex::new(BTreeMap::new()),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let _release = Release(controls.clone());
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let consumer = Node::start(config, services, external, applications(controls.clone())?).await?;
    super::super::ready(&consumer).await?;
    let client = client()?;
    for name in ["producer", "projection", "old-fetch", "new-fetch"] {
        ensure!(
            call(
                &client,
                &consumer,
                reqwest::Method::GET,
                &format!("/?source={name}")
            )
            .await?
                == "consumer:0"
        );
    }
    for value in 1..=2 {
        ensure!(
            call(
                &client,
                &consumer,
                reqwest::Method::POST,
                "/write?source=producer"
            )
            .await?
                == format!("consumer:{value}")
        );
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while pending(&client, &consumer).await?["pending_rows"] != "0" {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("settle both original events")??;
    let original_history = history(&client, &destination).await?;
    ensure!(original_history
        .as_array()
        .is_some_and(|rows| rows.len() == 2));
    let original = json_call(
        &client,
        &consumer,
        "/overlap-fetch?source=old-fetch&slot=old&id=000000000000000000000000000000f1",
    )
    .await?;
    ensure!(
        number(&original["expires_ms"])? - number(&original["issued_ms"])? == 30_000
            && original["offset"] == "1"
    );
    ensure!(
        json_call(&client, &consumer, "/overlap-audit?source=projection").await?
            == json!({"processed":0,"effects":0,"winner":null})
    );
    // Wait outside invocation budgets until just before the real default lease
    // expires. The old callback then spans actual expiry within its usual 3s.
    let expires = number(&original["expires_ms"])?;
    let wait = expires - wall() - 750;
    if wait > 0 {
        tokio::time::sleep(Duration::from_millis(wait as u64)).await;
    }
    let submitted = Instant::now();
    let mut old = Box::pin(json_call(
        &client,
        &consumer,
        "/overlap-apply?source=projection&slot=old&hold=1&ack=1",
    ));
    cellar::asyncrt::select! {
        result=&mut old=>anyhow::bail!("old worker did not reach the overlap barrier: {result:?}"),
        entered=tokio::time::timeout(budget,controls.entered.acquire())=>entered??.forget(),
    }
    let at_cut = status(&client, &destination).await?;
    ensure!(
        wall() < expires
            && at_cut["observation"]["lease_active"] == true
            && at_cut["checkpoint"] == "0"
    );
    while wall() <= expires {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let replacement = json_call(
        &client,
        &consumer,
        "/overlap-fetch?source=new-fetch&slot=new&id=000000000000000000000000000000f2",
    )
    .await?;
    ensure!(
        replacement["envelope"] == original["envelope"]
            && replacement["offset"] == original["offset"]
            && replacement["receipt"] != original["receipt"]
    );
    let current = Receipt::decode(
        replacement["receipt"]
            .as_str()
            .context("new receipt missing")?,
    )?;
    let applied = json_call(
        &client,
        &consumer,
        "/overlap-apply?source=projection&slot=new",
    )
    .await?;
    let once = json!({"processed":1,"effects":1,"winner":"new"});
    ensure!(
        applied["accepted"] == true && applied["state"] == once,
        "replacement effect was not proven once: {applied}"
    );
    let next = if after_ack {
        ensure!(
            settle(&client, &consumer, "new-fetch", "ack", &current, 0).await?
                == json!({"status":"Acknowledged"})
        );
        let next = json_call(
            &client,
            &consumer,
            "/overlap-fetch?source=new-fetch&slot=next&id=000000000000000000000000000000f3",
        )
        .await?;
        ensure!(next["offset"] == "2" && next["envelope"] != original["envelope"]);
        Some(next)
    } else {
        None
    };
    let before = status(&client, &destination).await?;
    let before_history = history(&client, &destination).await?;
    ensure!(
        before["observation"]["lease_active"] == true
            && before["checkpoint"] == if after_ack { "1" } else { "0" }
    );
    controls.release.add_permits(1);
    let result = tokio::time::timeout(budget, &mut old)
        .await
        .context("join expired worker callback")??;
    drop(old);
    ensure!(
        submitted.elapsed() < budget && !consumer.is_closed() && !destination.is_closed(),
        "closure or timeout cannot qualify inbox overlap"
    );
    let expected_ack = if after_ack {
        json!({"status":"Obsolete"})
    } else {
        json!({"error":"StaleReceipt"})
    };
    ensure!(
        result["accepted"] == false && result["state"] == once && result["ack"] == expected_ack,
        "expired worker duplicated an effect or settled the wrong lease: {result}"
    );
    ensure!(json_call(&client, &consumer, "/overlap-audit?source=projection").await? == once);
    let after = status(&client, &destination).await?;
    for field in ["checkpoint", "identity", "lease", "end", "earliest"] {
        ensure!(
            after[field] == before[field],
            "old callback changed {field}: {before} -> {after}"
        );
    }
    ensure!(history(&client, &destination).await? == before_history);
    let next = if let Some(next) = next {
        next
    } else {
        ensure!(
            settle(&client, &consumer, "new-fetch", "ack", &current, 0).await?
                == json!({"status":"Acknowledged"})
        );
        json_call(
            &client,
            &consumer,
            "/overlap-fetch?source=new-fetch&slot=next&id=000000000000000000000000000000f3",
        )
        .await?
    };
    let next_receipt = Receipt::decode(next["receipt"].as_str().context("next receipt missing")?)?;
    let final_effect = json_call(
        &client,
        &consumer,
        "/overlap-apply?source=projection&slot=next",
    )
    .await?;
    ensure!(
        final_effect["accepted"] == true
            && final_effect["state"] == json!({"processed":3,"effects":2,"winner":"next"})
    );
    ensure!(
        settle(&client, &consumer, "new-fetch", "ack", &next_receipt, 0).await?
            == json!({"status":"Acknowledged"})
    );
    ensure!(status(&client, &destination).await?["checkpoint"] == "2");
    eprintln!(
        "CONSUMER_OVERLAP_EVIDENCE {}",
        json!({"after_ack":after_ack,"original":original,"replacement":replacement,"old_result":result,"held_ms":submitted.elapsed().as_millis(),"checkpoint":"2","effects":2,"processed":3})
    );
    stopped(consumer).await?;
    stopped(destination).await?;
    ensure!(
        consumer_core.handle().spawn(async { 7 }).await? == 7
            && destination_core.handle().spawn(async { 7 }).await? == 7
    );
    Ok(())
}
