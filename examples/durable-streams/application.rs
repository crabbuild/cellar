// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Business state and stream obligations share one source transaction.

use anyhow::Result;
use cellar::sdk::{event_stream::*, *};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

#[path = "backlog.rs"]
mod backlog;

#[path = "consumer_owner.rs"]
mod consumer_owner;

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
    node: String,
    phase: Mutex<Option<Value>>,
    owner: Option<Arc<super::owner_fault::Control>>,
}

impl FaultControls {
    pub fn for_node(node: &str, owner: Option<Arc<super::owner_fault::Control>>) -> Self {
        Self {
            node: node.into(),
            owner,
            ..Default::default()
        }
    }
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
    faults: Option<Arc<FaultControls>>,
}

impl Order {
    fn confirm(&self, context: &Context, event: &OrderConfirmed) -> Result<Value> {
        let encoded = serde_json::to_value(event)?;
        let db = context.storage()?;
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
            let digest = payload_digest(event)?;
            tx.emit(&events, event.clone())?;
            if self.faults.is_some() {
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
        Ok(
            json!({"status":outcome,"order":db.get_json("order")?,"receipt":db.get_json("command_receipt")?}),
        )
    }
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
                let mut state = json!({"order":db.get_json("order")?,"receipt":db.get_json("command_receipt")?,"outbox":db.outbox(&OutboxOptions::default())?});
                if let Some(controls) = &self.faults {
                    state["host"] = json!(controls.node);
                }
                return reply(&context, 200, state);
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
            if let Some(cut) = url
                .query_pairs()
                .find(|(name, _)| name == "running_cut")
                .map(|(_, value)| value.into_owned())
            {
                let control = self
                    .faults
                    .as_ref()
                    .and_then(|controls| controls.owner.as_ref())
                    .ok_or_else(|| anyhow::anyhow!("private owner fault is not configured"))?;
                anyhow::ensure!(
                    matches!(cut.as_str(), "before_tx" | "before_commit"),
                    "invalid private callback cut"
                );
                let events = context.stream::<OrderConfirmed>("EVENTS")?;
                let scope = context.identity().unwrap().scope.clone();
                let result = if cut == "before_commit" {
                    db.tx(|tx| {
                        tx.put_json("order", &serde_json::to_value(&event)?)?;
                        tx.emit(&events, event.clone())?;
                        control.hold(&scope, &cut)?;
                        Ok(())
                    })
                } else {
                    control.hold(&scope, &cut)?;
                    db.tx(|tx| {
                        tx.put_json("order", &serde_json::to_value(&event)?)?;
                        tx.emit(&events, event.clone())
                    })
                };
                control.attempted(&scope, &cut, &result)?;
                // Deliberately swallow the refusal: normal output gates still
                // must prevent a fenced callback from returning success.
                return Ok(Output::Http(cellar::engine_api::HttpResponse {
                    status: 200,
                    body: b"stale_success".to_vec(),
                    headers: Vec::new(),
                    stream: None,
                    websocket: None,
                    write_position: None,
                    observed_position: None,
                }));
            }
            let hold = self.faults.as_ref().and_then(|controls| {
                url.query_pairs()
                    .find(|(name, _)| name == "hold_before_send")
                    .map(|(_, token)| (controls.clone(), token.into_owned()))
            });
            if let Some((controls, token)) = hold {
                if !valid_name(&token) {
                    return reply(
                        &context,
                        400,
                        json!({"error":"bounded source hold token required"}),
                    );
                }
                // Acquire the real cell input gate before the transaction.
                // Internal outbox work cannot enter while this callback holds it.
                return context
                    .block_concurrency_while(move |context| async move {
                        let result = self.confirm(&context, &event)?;
                        anyhow::ensure!(
                            result["status"] == "created",
                            "source fault requires a fresh command"
                        );
                        let db = context.storage()?;
                        db.sync().await?;
                        let outbox = db.outbox(&OutboxOptions {
                            limit: 1,
                            cursor: None,
                        })?;
                        anyhow::ensure!(
                            outbox.pending_rows == 1
                                && outbox.lanes.len() == 1
                                && outbox.lanes[0].attempts == 0
                                && outbox.lanes[0].revision.dispatch_generation == 0,
                            "source dispatched before its gated proof boundary"
                        );
                        *controls.phase.lock().unwrap() =
                            Some(json!({"boundary":"source_proven_before_send",
                        "token":token,"command":result,"outbox":outbox}));
                        // The independent observer records this evidence and SIGKILLs
                        // the process before the ordinary gate/deadline can release it.
                        std::future::pending::<Result<Output>>().await
                    })
                    .await;
            }
            let result = self.confirm(&context, &event)?;
            reply(
                &context,
                match result["status"].as_str() {
                    Some("created") => 201,
                    Some("conflict") => 409,
                    _ => 200,
                },
                result,
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
            if let Some(controls) = self
                .faults
                .as_ref()
                .filter(|_| request.method == "GET" && path == "/__test/projection-receipts")
            {
                return reply(
                    &context,
                    200,
                    json!({"projection":projection(&db)?,"receipts":db.get_json("effect_receipts")?.unwrap_or_else(||json!({})), "host":controls.node,
                        "ack_receipt_sha256":db.get_json("ack_receipt")?.and_then(|value|value.as_str().map(|value|hex::encode(Sha256::digest(value.as_bytes()))))}),
                );
            }
            if let Some(controls) = &self.faults {
                if path == "/projection/owner-audit" && request.method == "GET" {
                    return consumer_owner::audit(&context, &db, controls);
                }
                if path == "/projection/inbox-audit" && request.method == "GET" {
                    return consumer_owner::inbox_audit(&context, &db).await;
                }
                if path == "/projection/running" && request.method == "POST" {
                    return consumer_owner::running(&context, &db, &url, controls).await;
                }
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
            let lost_ack = self.faults.is_some() && path == "/projection/lost-ack";
            let ack_stored = self.faults.is_some() && path == "/projection/ack-stored";
            let fetch_saved = self.faults.is_some() && path == "/projection/fetch";
            if request.method != "POST"
                || !(hold
                    || lost_ack
                    || ack_stored
                    || fetch_saved
                    || matches!(path.as_str(), "/projection/process" | "/projection/retry"))
            {
                return reply(
                    &context,
                    405,
                    json!({"error":"use POST /projection/process or /projection/retry"}),
                );
            }
            let events = context.stream::<OrderConfirmed>("EVENTS")?;
            let subscription = events.subscription("projection")?;
            if ack_stored {
                let encoded = db
                    .get_json("ack_receipt")?
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .ok_or_else(|| anyhow::anyhow!("stored acknowledgment receipt missing"))?;
                let receipt = Receipt::decode(&encoded)?;
                let result = subscription.ack(&context, &receipt).await?;
                return reply(
                    &context,
                    200,
                    json!({"status":format!("{result:?}"),"projection":projection(&db)?}),
                );
            }
            let options = BatchOptions {
                max_records: 20,
                max_bytes: 64 << 10,
                wait_ms: 0,
            };
            let fetched = if fetch_saved {
                let id = url
                    .query_pairs()
                    .find(|(name, _)| name == "fetch_id")
                    .map(|(_, value)| value.into_owned())
                    .ok_or_else(|| anyhow::anyhow!("fetch ID missing"))?;
                subscription.next_with_id(&context, &id, options).await
            } else {
                subscription.next(&context, options).await
            };
            let batch = match fetched {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    return reply(
                        &context,
                        200,
                        json!({"status":"empty","fresh_effects":0,"projection":projection(&db)?}),
                    )
                }
                Err(error) if fetch_saved => {
                    let error = error
                        .downcast_ref::<DeliveryError>()
                        .ok_or_else(|| anyhow::anyhow!("untyped fetch refusal"))?;
                    return reply(
                        &context,
                        200,
                        json!({"status":error.code.as_str(),"fresh_effects":0,"projection":projection(&db)?}),
                    );
                }
                Err(error) => return Err(error),
            };
            let mut fresh_effects = 0;
            for record in &batch.records {
                let fresh = db.tx(|tx| {
                    if lost_ack || fetch_saved {
                        tx.put_json("ack_receipt", &json!(batch.receipt().encode()?))?;
                    }
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
            if fetch_saved {
                return reply(
                    &context,
                    200,
                    json!({
                        "status":"fetched", "fresh_effects":fresh_effects, "projection":projection(&db)?,
                        "batch":{"receipt":batch.receipt().encode()?,
                            "issued_ms":batch.issued_at_ms.to_string(), "expires_ms":batch.expires_at_ms.to_string(),
                            "records":batch.records.iter().map(event_receipt).collect::<Result<Vec<_>>>()?}
                    }),
                );
            }
            if lost_ack {
                let status = match batch.ack(&context).await {
                    Ok(value) => format!("{value:?}"),
                    Err(_) => "reply_lost".into(),
                };
                return reply(
                    &context,
                    200,
                    json!({"status":status,"fresh_effects":fresh_effects,"projection":projection(&db)?}),
                );
            }
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
        let sources=faults.clone();
        let backlog=faults.clone();
        Ok(Arc::new(Registry::new(move |context,invocation| {
            let endpoints=endpoints.clone();
            Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {return Ok(Output::Done)};
            let url = url::Url::parse(&request.url)?;
            let path = url.path();
            if endpoints.is_some() && path=="/__test/backlog" {
                return Ok(Output::Http(context.namespace("BACKLOG")?.get_by_name("pending").fetch(&context,request).await?));
            }
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
        })}).cell("Order",move |_,_|Ok(Arc::new(Order{faults:sources.clone()})))?.cell("Projection",move |_,_|Ok(Arc::new(Projection{faults:projections.clone()})))?
        .cell("Backlog",move |_,_|Ok(Arc::new(backlog::Backlog{faults:backlog.clone()})))?))
    })
}
