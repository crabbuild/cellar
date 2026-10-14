//! Business state and stream obligations share one source transaction.

use anyhow::Result;
use cellar::sdk::{event_stream::*, *};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

pub const SCRIPT: &str = "stream-example";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrderConfirmed {
    order_id: String,
    amount_cents: i64,
}

impl Event for OrderConfirmed {
    const FAMILY: &'static str = "order-confirmed";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        &self.order_id
    }
}

pub fn resource() -> Resource {
    Resource {
        tenant: "example".into(),
        application: SCRIPT.into(),
        id: "00000000000000000000000000000100".into(),
        name: "orders".into(),
        family: OrderConfirmed::FAMILY.into(),
    }
}

pub fn binding(admin: bool) -> Binding {
    Binding {
        environment: if admin { "ADMIN_EVENTS" } else { "EVENTS" }.into(),
        resource: resource(),
        principal: SCRIPT.into(),
        grant: if admin { "management" } else { "business" }.into(),
        publish: !admin,
        read: !admin,
        subscriptions: if admin {
            Vec::new()
        } else {
            vec!["projection".into()]
        },
        administer: admin,
    }
}

pub fn definition() -> Definition {
    Definition {
        resource: resource(),
        revision: 1,
        retention: Retention::Required,
        max_bytes: 1 << 20,
        max_records: 1000,
        max_age_ms: 86_400_000,
        grants: vec![
            Grant::from_binding(&binding(true)),
            Grant::from_binding(&binding(false)),
        ],
        subscriptions: vec![SubscriptionDefinition {
            name: "projection".into(),
            start: Start::Earliest,
            required: true,
        }],
    }
}

/// Explicit local fixture controls; absent from the ordinary walkthrough.
#[derive(Default)]
pub struct FaultControls {
    phase: Mutex<Option<Value>>,
}

fn payload_digest(event: &OrderConfirmed) -> Result<String> {
    // Fixed, canonical key order in both measuring client and application.
    let payload = json!({"amount_cents":event.amount_cents,"order_id":event.order_id});
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&payload)?)))
}

fn event_receipt(record: &Record) -> Result<Value> {
    let event = record.event.decode::<OrderConfirmed>()?;
    Ok(
        json!({"order_id":event.order_id,"payload_sha256":payload_digest(&event)?,
        "envelope_sha256":hex::encode(Sha256::digest(record.encoded())),"event_id":record.event.id,
        "offset":record.offset.to_string(),"source":record.event.source,"incarnation":record.event.source_incarnation,
        "lane":record.event.lane,"sequence":record.event.sequence.to_string()}),
    )
}

fn reply(context: &Context, status: u16, body: Value) -> Result<Output> {
    Ok(Output::Http(
        http::Response::new(
            status,
            serde_json::to_vec(&body)?,
            vec![("content-type".into(), "application/json".into())],
        )
        .into_http(context)?,
    ))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
}

struct Order {
    faults: bool,
}

impl Cell for Order {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let url = url::Url::parse(&request.url)?;
            let order_id = url.path().strip_prefix("/orders/").unwrap_or_default();
            if !valid_name(order_id) {
                return reply(&context, 400, json!({"error":"invalid order id"}));
            }
            let db = context.storage()?;
            if request.method == "GET" {
                return reply(
                    &context,
                    200,
                    json!({"order":db.get_json("order")?,"receipt":db.get_json("command_receipt")?,"outbox":db.outbox(&OutboxOptions::default())?}),
                );
            }
            if request.method != "POST" {
                return reply(&context, 405, json!({"error":"use GET or POST"}));
            }
            let amount = url
                .query_pairs()
                .find(|(name, _)| name == "amount_cents")
                .and_then(|(_, value)| value.parse::<i64>().ok())
                .filter(|amount| *amount > 0 && *amount <= 1_000_000_000);
            let Some(amount_cents) = amount else {
                return reply(
                    &context,
                    400,
                    json!({"error":"amount_cents must be 1..1000000000"}),
                );
            };
            let event = OrderConfirmed {
                order_id: order_id.into(),
                amount_cents,
            };
            let encoded = serde_json::to_value(&event)?;
            let events = context.stream::<OrderConfirmed>("EVENTS")?;
            let outcome = db.tx(|tx| {
                if let Some(existing) = tx.get_json("order")? {
                    return Ok(if existing == encoded {
                        "duplicate"
                    } else {
                        "conflict"
                    });
                }
                tx.put_json("order", &encoded)?;
                let digest = payload_digest(&event)?;
                tx.emit(&events, event)?;
                if self.faults {
                    let page = tx.outbox(&OutboxOptions {
                        limit: 1,
                        cursor: None,
                    })?;
                    let producer = page
                        .lanes
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("emitted lane missing"))?;
                    tx.put_json(
                        "command_receipt",
                        &json!({"payload_sha256":digest,"source":producer.revision.source,
                        "incarnation":producer.revision.incarnation,"lane":producer.revision.lane,
                        "sequence":(producer.revision.next_sequence-1).to_string()}),
                    )?;
                }
                Ok("created")
            })?;
            reply(
                &context,
                match outcome {
                    "created" => 201,
                    "conflict" => 409,
                    _ => 200,
                },
                json!({"status":outcome,"order":db.get_json("order")?,"receipt":db.get_json("command_receipt")?}),
            )
        })
    }
}

fn projection(db: &Storage) -> Result<Value> {
    Ok(db
        .get_json("projection")?
        .unwrap_or_else(|| json!({"orders":0,"total_cents":0})))
}

struct Projection {
    faults: Option<Arc<FaultControls>>,
}

impl Cell for Projection {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let url = url::Url::parse(&request.url)?;
            let path = url.path().to_owned();
            let db = context.storage()?;
            if request.method == "GET"
                && path == "/__test/projection-receipts"
                && self.faults.is_some()
            {
                return reply(
                    &context,
                    200,
                    json!({"projection":projection(&db)?,"receipts":db.get_json("effect_receipts")?.unwrap_or_else(||json!({}))}),
                );
            }
            let hold = path == "/projection/hold" && self.faults.is_some();
            let token = if hold {
                let token = url
                    .query_pairs()
                    .find(|(name, _)| name == "token")
                    .map(|(_, value)| value.into_owned())
                    .filter(|token| valid_name(token));
                let Some(token) = token else {
                    return reply(
                        &context,
                        400,
                        json!({"error":"bounded hold token required"}),
                    );
                };
                Some(token)
            } else {
                None
            };
            if request.method == "GET" && path == "/projection" {
                return reply(&context, 200, projection(&db)?);
            }
            if request.method != "POST"
                || !(hold || matches!(path.as_str(), "/projection/process" | "/projection/retry"))
            {
                return reply(
                    &context,
                    405,
                    json!({"error":"use POST /projection/process or /projection/retry"}),
                );
            }
            let events = context.stream::<OrderConfirmed>("EVENTS")?;
            let subscription = events.subscription("projection")?;
            let Some(batch) = subscription
                .next(
                    &context,
                    BatchOptions {
                        max_records: 20,
                        max_bytes: 64 << 10,
                        wait_ms: 0,
                    },
                )
                .await?
            else {
                return reply(
                    &context,
                    200,
                    json!({"status":"empty","fresh_effects":0,"projection":projection(&db)?}),
                );
            };
            let mut fresh_effects = 0;
            for record in &batch.records {
                let fresh = db.tx(|tx| {
                    if !tx.inbox("order-projection-v1")?.accept(record)? {
                        return Ok(false);
                    }
                    let event = record.event.decode::<OrderConfirmed>()?;
                    let prior = projection(tx)?;
                    let orders = prior["orders"]
                        .as_i64()
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("order count overflow"))?;
                    let total = prior["total_cents"]
                        .as_i64()
                        .unwrap_or(0)
                        .checked_add(event.amount_cents)
                        .ok_or_else(|| anyhow::anyhow!("projection total overflow"))?;
                    tx.put_json("projection", &json!({"orders":orders,"total_cents":total}))?;
                    if self.faults.is_some() {
                        let mut receipts =
                            tx.get_json("effect_receipts")?.unwrap_or_else(|| json!({}));
                        anyhow::ensure!(
                            receipts.get(&event.order_id).is_none(),
                            "second effect for the same business command"
                        );
                        receipts
                            .as_object_mut()
                            .ok_or_else(|| anyhow::anyhow!("invalid effect receipt map"))?
                            .insert(event.order_id.clone(), event_receipt(record)?);
                        tx.put_json("effect_receipts", &receipts)?;
                    }
                    Ok(true)
                })?;
                fresh_effects += usize::from(fresh);
            }
            // Prove local receipts and effects before touching the remote
            // checkpoint. Redelivery reuses those receipts after recovery.
            db.sync().await?;
            if let Some(token) = token {
                let phase = json!({"token":token,"boundary":"consumer_proven_before_ack","receipt":batch.receipt().encode()?,
                    "records":batch.records.iter().map(event_receipt).collect::<Result<Vec<_>>>()?,"projection":projection(&db)?});
                *self.faults.as_ref().unwrap().phase.lock().unwrap() = Some(phase);
                // The measuring client observes the proven boundary and kills
                // this process. No retry or acknowledgment is issued here.
                std::future::pending::<()>().await;
            }
            let retried = path == "/projection/retry";
            if retried {
                batch.retry(&context, 0).await?;
            } else {
                batch.ack(&context).await?;
            }
            reply(
                &context,
                200,
                json!({"status":if retried {"retried"} else {"acknowledged"},"fresh_effects":fresh_effects,"projection":projection(&db)?}),
            )
        })
    }
}

pub fn applications(faults: Option<Arc<FaultControls>>) -> Result<CompiledApplications> {
    CompiledApplications::default().register(SCRIPT,"v1",move || {
        let endpoints=faults.clone();
        let projections=faults.clone();
        let enabled=faults.is_some();
        Ok(Arc::new(Registry::new(move |context,invocation| {
            let endpoints=endpoints.clone();
            Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {return Ok(Output::Done)};
            let url = url::Url::parse(&request.url)?;
            let path = url.path();
            if let Some(controls)=endpoints.as_ref().filter(|_|path=="/__test/phase" && request.method=="GET") {
                let phase=controls.phase.lock().unwrap().clone();
                return reply(&context,200,json!({"phase":phase}));
            }
            if endpoints.is_some() && path=="/__test/projection-receipts" {
                return Ok(Output::Http(context.namespace("PROJECTIONS")?.get_by_name("orders").fetch(&context,request).await?));
            }
            if let Some(name) = path.strip_prefix("/orders/") {
                if !valid_name(name) {return reply(&context,400,json!({"error":"invalid order id"}));}
                return Ok(Output::Http(context.namespace("ORDERS")?.get_by_name(name).fetch(&context,request).await?));
            }
            if path == "/projection" || path.starts_with("/projection/") {
                return Ok(Output::Http(context.namespace("PROJECTIONS")?.get_by_name("orders").fetch(&context,request).await?));
            }
            if (path == "/events" || (path=="/__test/events" && endpoints.is_some())) && request.method == "GET" {
                let events = context.stream::<OrderConfirmed>("EVENTS")?;
                let cursor = url.query_pairs().find(|(name,_)|name == "cursor").map(|(_,value)|Cursor::decode(&value)).transpose()?;
                let page = events.read(&context,cursor.as_ref(),ReadOptions{max_records:20,max_bytes:64<<10}).await?;
                let records = page.records.iter().map(|record| {
                    if path=="/__test/events" { let mut receipt=event_receipt(record)?; receipt["event"]=serde_json::to_value(record.event.decode::<OrderConfirmed>()?)?; Ok(receipt) }
                    else {Ok(json!({"offset":record.offset.to_string(),"event":record.event.decode::<OrderConfirmed>()?}))}
                }).collect::<Result<Vec<_>>>()?;
                return reply(&context,200,json!({"records":records,"cursor":page.next.encode()?}));
            }
            if path == "/subscription" && request.method == "GET" {
                let status = context.stream::<OrderConfirmed>("EVENTS")?.subscription("projection")?.status(&context).await?;
                return reply(&context,200,serde_json::to_value(status)?);
            }
            reply(&context,404,json!({"routes":["POST /orders/ID?amount_cents=N","GET /orders/ID","GET /events","GET /subscription","GET /projection","POST /projection/retry","POST /projection/process"]}))
        })}).cell("Order",move |_,_|Ok(Arc::new(Order{faults:enabled})))?.cell("Projection",move |_,_|Ok(Arc::new(Projection{faults:projections.clone()})))?))
    })
}
