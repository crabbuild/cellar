// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::application::{response, stub};
use crate::config::*;
use crate::contracts::*;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    clone::StructuredValue,
    http::{OutboundRequest, Response},
    queue::{self, Body},
    workflow::{self, Steps},
    Context, HandlerFuture, Output, Request,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const PROJECTION_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS projections(
 source_scope TEXT PRIMARY KEY, version INTEGER NOT NULL, snapshot_json TEXT NOT NULL, applied_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS consumer_receipts(effect_id TEXT PRIMARY KEY, digest TEXT NOT NULL);
CREATE TRIGGER IF NOT EXISTS reject_changed_effect BEFORE INSERT ON consumer_receipts
 WHEN EXISTS(SELECT 1 FROM consumer_receipts WHERE effect_id=NEW.effect_id AND digest!=NEW.digest)
 BEGIN SELECT RAISE(ABORT,'effect ID reused with another payload'); END;
CREATE TABLE IF NOT EXISTS quarantine(effect_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL);
";

pub async fn flush(context: &Context, config: &AppConfig, tenant: usize) -> Result<()> {
    let storage = context.storage()?;
    let now = cellar::asyncrt::wall_ms();
    let (_, rows, _) = storage.sql(
        "SELECT effect_id,payload_json FROM outbox WHERE delivered=0 AND next_attempt_ms<=?1 ORDER BY next_attempt_ms LIMIT 20",
        &[json!(now)])?;
    for row in rows {
        let id = row[0].as_str().context("invalid effect ID")?;
        let effect: Effect = serde_json::from_str(row[1].as_str().context("invalid effect")?)?;
        ensure!(effect.tenant == tenant, "outbox tenant mismatch");
        let result = if effect.kind == "dispatch" {
            let book = effect.snapshot["book"]
                .as_u64()
                .context("invalid dispatch book")? as usize;
            let counts = config.profile.counts();
            let slot = book % (counts.workflows / counts.tenants);
            let instance = context
                .workflow("DISPATCH")?
                .get(context, &workflow_id(tenant, slot))
                .await?;
            instance
                .send_event(
                    context,
                    workflow::SendEvent {
                        event_type: "assignment".into(),
                        payload: Some(StructuredValue::from_json(effect.snapshot.clone())?),
                    },
                )
                .await
        } else {
            context
                .queue(&binding("Q", tenant / 2, 0))?
                .send(
                    context,
                    Body::Json(serde_json::to_value(&effect)?),
                    Default::default(),
                )
                .await
                .map(|_| ())
        };
        match result {
            Ok(()) => {
                storage.sql(
                    "UPDATE outbox SET delivered=1 WHERE effect_id=?1",
                    &[json!(id)],
                )?;
            }
            Err(error) => {
                storage.sql(
                    "UPDATE outbox SET attempts=attempts+1,next_attempt_ms=?2 WHERE effect_id=?1",
                    &[json!(id), json!(now + 1000)],
                )?;
                return Err(error);
            }
        }
    }
    Ok(())
}

pub fn consume(
    context: Context,
    config: Arc<AppConfig>,
    batch: cellar::engine_api::QueueBatch,
) -> HandlerFuture<Output> {
    queue::dispatch(batch, move |event| {
        let context = context.clone();
        let config = config.clone();
        async move {
            let dead = event.queue().ends_with("-dead");
            for message in event.messages() {
                let value = match &message.body {
                    Body::Json(value) => value.clone(),
                    Body::Structured(value) | Body::JsonValue(value) => value.to_json()?,
                    Body::Text(text) => serde_json::from_str(text)?,
                    Body::Bytes(bytes) => serde_json::from_slice(bytes)?,
                };
                let effect: Effect = serde_json::from_value(value)?;
                ensure!(
                    effect.tenant < config.profile.counts().tenants,
                    "invalid queue tenant"
                );
                ensure!(
                    event.queue() == queue_identity(effect.tenant / 2, dead),
                    "queue tenant mismatch"
                );
                let projection = effect.kind == "projection"
                    && effect.snapshot["state"]["version"].as_u64() == Some(effect.version);
                if !projection && !dead {
                    message.retry(Some(0))?;
                    continue;
                }
                let partition = usize::from(Sha256::digest(effect.source_scope.as_bytes())[0]) % 3;
                let db = context.sql(&binding("SQL", effect.tenant, partition))?;
                let encoded = serde_json::to_string(&effect)?;
                let digest = format!("{:x}", Sha256::digest(encoded.as_bytes()));
                let receipt = db
                    .prepare("INSERT OR IGNORE INTO consumer_receipts VALUES(?1,?2)")
                    .bind_json([json!(effect.effect_id), json!(digest)])?;
                // Exhausted delivery attempts can mean a transient outage.
                // A valid projection remains an idempotent repair in the DLQ;
                // poison and malformed snapshots are terminal quarantine.
                let mutation = if !projection {
                    db.prepare("INSERT OR IGNORE INTO quarantine VALUES(?1,?2)")
                        .bind_json([json!(effect.effect_id), json!(encoded)])?
                } else {
                    db.prepare(
                        "INSERT INTO projections VALUES(?1,?2,?3,?4) ON CONFLICT(source_scope)
                        DO UPDATE SET version=excluded.version,snapshot_json=excluded.snapshot_json,
                        applied_ms=excluded.applied_ms WHERE excluded.version>projections.version",
                    )
                    .bind_json([
                        json!(effect.source_scope),
                        json!(effect.version),
                        json!(serde_json::to_string(&effect.snapshot)?),
                        json!(cellar::asyncrt::wall_ms()),
                    ])?
                };
                db.batch(&context, &[receipt, mutation]).await?;
                message.ack()?;
            }
            Ok(())
        }
    })
}

async fn cell_command(
    context: &Context,
    tenant: usize,
    kind: &str,
    id: usize,
    command: Command,
) -> Result<Receipt> {
    let data =
        cellar::sdk::encode_rpc(&StructuredValue::from_json(serde_json::to_value(command)?)?)?;
    let reply = stub(context, tenant, kind, id)?
        .rpc(context, "command", data)
        .await?;
    let value = cellar::sdk::decode_rpc_result(&reply)?.to_json()?;
    let receipt: Receipt = serde_json::from_value(value["receipt"].clone())?;
    ensure!(
        receipt.accepted,
        "participant rejected command: {}",
        receipt.result
    );
    Ok(receipt)
}

fn command(id: &str, suffix: &str, action: &str, payload: Value) -> Command {
    Command {
        command_id: format!("saga-{id}-{suffix}"),
        action: action.into(),
        payload,
        expected_version: None,
    }
}

async fn transition(
    context: &Context,
    tenant: usize,
    book: usize,
    order: &str,
    status: &str,
) -> Result<Receipt> {
    cell_command(
        context,
        tenant,
        "books",
        book,
        command(
            &crate::contracts::assignment_id(tenant, book, order),
            status,
            "transition",
            json!({"order_id": order, "status": status}),
        ),
    )
    .await
}

fn step_policy() -> workflow::StepConfig {
    workflow::StepConfig {
        retries: Some(workflow::Retries {
            limit: 2.0,
            delay: workflow::Delay::Fixed(workflow::Duration::Milliseconds(100.0)),
            backoff: Some(workflow::Backoff::Constant),
        }),
        timeout: Some(workflow::Duration::Milliseconds(5000.0)),
        ..Default::default()
    }
}

async fn participant(
    context: &Context,
    tenant: usize,
    kind: &str,
    id: usize,
    path: &str,
) -> Result<Value> {
    let reply = stub(context, tenant, kind, id)?
        .fetch(
            context,
            Request {
                url: format!("http://participant/api/{tenant}/{kind}/{id}/{path}"),
                method: "GET".into(),
                headers: Vec::new(),
                body: cellar::engine_api::RequestBody::Bytes(Vec::new().into()),
                entrypoint: None,
            },
        )
        .await?;
    Response::from_http(reply).json(context).await?.to_json()
}

fn compensation_status(reservation: Option<ReservationStatus>) -> &'static str {
    if reservation == Some(ReservationStatus::Consumed) {
        "needs_review"
    } else {
        "cancelled"
    }
}

async fn compensate(
    context: &Context,
    tenant: usize,
    book: usize,
    inventory: usize,
    order: &str,
) -> Result<StructuredValue> {
    let current = participant(context, tenant, "books", book, &format!("orders/{order}")).await?;
    if current["order"]["status"] == "completed" {
        return StructuredValue::from_json(json!({"status":"completed"}));
    }
    transition(context, tenant, book, order, "compensating").await?;
    let reservation = crate::contracts::reservation_id(tenant, book, order);
    let value = participant(
        context,
        tenant,
        "inventory",
        inventory,
        &format!("reservations/{reservation}"),
    )
    .await?;
    let status: Option<ReservationStatus> = value["reservation"]
        .get("status")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?;
    if status == Some(ReservationStatus::Held) {
        cell_command(
            context,
            tenant,
            "inventory",
            inventory,
            command(
                &crate::contracts::assignment_id(tenant, book, order),
                "release",
                "release",
                json!({"reservation_id":reservation}),
            ),
        )
        .await?;
    }
    let status = compensation_status(status);
    transition(context, tenant, book, order, status).await?;
    StructuredValue::from_json(json!({"status":status}))
}

pub fn dispatch(
    context: Context,
    config: Arc<AppConfig>,
    event: workflow::Event,
    steps: Steps,
) -> HandlerFuture<StructuredValue> {
    Box::pin(async move {
        let params = event.payload.to_json()?;
        let tenant = params["tenant"]
            .as_u64()
            .context("dispatcher tenant absent")? as usize;
        loop {
            let event = steps
                .wait_for_event_with_timeout("v1-assignment", "assignment".into(), None)
                .await?;
            let job = event
                .property(&"payload".encode_utf16().collect::<Vec<_>>())?
                .context("assignment payload absent")?
                .to_json()?;
            let book = job["book"].as_u64().context("assignment book absent")? as usize;
            let inventory = job["inventory_id"]
                .as_u64()
                .context("assignment inventory absent")? as usize;
            let order = job["order_id"]
                .as_str()
                .context("assignment order absent")?
                .to_string();
            let identity = crate::contracts::assignment_id(tenant, book, &order);
            let reservation = crate::contracts::reservation_id(tenant, book, &order);
            let ctx = context.clone();
            let oid = order.clone();
            let rid = reservation.clone();
            let reserved = steps
                .do_with_config(
                    format!("v1-reserve-{identity}"),
                    step_policy(),
                    move |_| async move {
                        transition(&ctx, tenant, book, &oid, "reserving").await?;
                        cell_command(
                            &ctx,
                            tenant,
                            "inventory",
                            inventory,
                            command(
                                &crate::contracts::assignment_id(tenant, book, &oid),
                                "reserve",
                                "reserve",
                                json!({"reservation_id": rid, "qty": 1}),
                            ),
                        )
                        .await?;
                        transition(&ctx, tenant, book, &oid, "reserved").await?;
                        StructuredValue::from_json(json!({"reserved": true}))
                    },
                )
                .await;
            if reserved.is_err() {
                let ctx = context.clone();
                let oid = order.clone();
                steps.do_with_config(format!("v1-compensate-{identity}"),step_policy(),move |_| async move {
                    compensate(&ctx,tenant,book,inventory,&oid).await
                }).await?;
                continue;
            }
            let ctx = context.clone();
            let oid = order.clone();
            let simulator = config.simulator.clone();
            let notified = steps
                .do_with_config(
                    format!("v1-notify-{identity}"),
                    step_policy(),
                    move |_| async move {
                        if let Some(url) = simulator {
                            let request = OutboundRequest {
                            url: format!("{url}/notify"),
                            method: "POST".into(),
                            headers: vec![("content-type".into(), "application/json".into())],
                            body: Some(
                                serde_json::to_vec(
                                    &json!({"effect_id": format!("notify-{}", crate::contracts::assignment_id(tenant, book, &oid)), "tenant":tenant, "book":book, "order_id": oid}),
                                )?
                                .into(),
                            ),
                            redirect: Default::default(),
                        };
                            let mut response = Response::from_http(ctx.fetch(request).await?);
                            ensure!(response.status() == 200, "technician unavailable");
                            response.bytes(&ctx).await?;
                        }
                        transition(&ctx, tenant, book, &oid, "dispatched").await?;
                        StructuredValue::from_json(json!({"notified": true}))
                    },
                )
                .await;
            if notified.is_err() {
                let ctx = context.clone();
                let oid = order.clone();
                steps.do_with_config(format!("v1-compensate-{identity}"),step_policy(),move |_| async move {
                    compensate(&ctx,tenant,book,inventory,&oid).await
                }).await?;
                continue;
            }
            // This durable wait models an automated technician's bounded delay.
            // Operator-driven event waits are exercised by assignment and controls.
            steps
                .sleep(
                    format!("v1-technician-{identity}"),
                    workflow::Duration::Milliseconds(10.0),
                )
                .await?;
            let ctx = context.clone();
            let oid = order.clone();
            let rid = reservation.clone();
            let finalized = steps
                .do_with_config(
                    format!("v1-finalize-{identity}"),
                    step_policy(),
                    move |_| async move {
                        transition(&ctx, tenant, book, &oid, "in_progress").await?;
                        cell_command(
                            &ctx,
                            tenant,
                            "inventory",
                            inventory,
                            command(
                                &crate::contracts::assignment_id(tenant, book, &oid),
                                "consume",
                                "consume",
                                json!({"reservation_id": rid}),
                            ),
                        )
                        .await?;
                        let receipt = transition(&ctx, tenant, book, &oid, "completed").await?;
                        StructuredValue::from_json(serde_json::to_value(receipt)?)
                    },
                )
                .await;
            if finalized.is_err() {
                let ctx = context.clone();
                let oid = order.clone();
                steps.do_with_config(format!("v1-compensate-{identity}"),step_policy(),move |_| async move {
                    compensate(&ctx,tenant,book,inventory,&oid).await
                }).await?;
            }
        }
    })
}

pub async fn reconcile(context: &Context, config: &AppConfig) -> Result<()> {
    // Durable alarms retain individual outboxes. Cron observes their broker
    // backlog without performing an unbounded fleet-wide scan every minute.
    for pair in 0..config.profile.counts().queues / 2 {
        context
            .queue(&binding("Q", pair, 0))?
            .metrics(context)
            .await?;
    }
    Ok(())
}

pub async fn system(context: &Context, config: &AppConfig, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let parts: Vec<_> = url.path().trim_matches('/').split('/').collect();
    ensure!(parts.len() == 4, "invalid system route");
    let kind = parts[1];
    let tenant = parts[2].parse::<usize>()?;
    let id = parts[3].parse::<usize>()?;
    let counts = config.profile.counts();
    let after = url
        .query_pairs()
        .find(|(k, _)| k == "after")
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default();
    let quarantine = url
        .query_pairs()
        .any(|(k, v)| k == "quarantine" && v == "1");
    ensure!(tenant < counts.tenants && id < 128, "invalid system target");
    let method = request.method.clone();
    let input: Value = if method == "POST" {
        let bytes = request.into_body_reader(context)?.bytes(context).await?;
        if bytes.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&bytes)?
        }
    } else {
        json!({})
    };
    let value = match kind {
        "cron" => {
            ensure!(tenant == 0 && id == 0, "invalid cron target");
            reconcile(context, config).await?;
            json!({"scope": cellar_logic::cron::reserved_cell(SCRIPT), "reconciled": true})
        }
        "sql" => {
            ensure!(id < 3, "unknown reporting partition");
            let db = context.sql(&binding("SQL", tenant, id))?;
            if method == "POST" {
                db.migrate(
                    context,
                    "001-projections",
                    PROJECTION_SCHEMA,
                    "__sys_sql_migrations_fleetcare",
                )
                .await?;
            }
            let results = if quarantine {
                db.prepare(
                    "SELECT effect_id,payload_json FROM quarantine ORDER BY effect_id LIMIT 100",
                )
                .all(context)
                .await?
            } else {
                db.prepare("SELECT source_scope,version,snapshot_json,applied_ms FROM projections WHERE source_scope>?1 ORDER BY source_scope LIMIT 100")
                    .bind_json([json!(after)])?.all(context).await?
            };
            json!({"rows": results.results})
        }
        "kv" => {
            ensure!(id == 0, "unknown KV partition");
            let kv = context.kv(&binding("KV", tenant, 0))?;
            if method == "POST" {
                kv.put_text(
                    context,
                    "policy",
                    &serde_json::to_string(&input)?,
                    cellar::sdk::kv::PutOptions {
                        metadata: Some(json!({"version": 1})),
                        ..Default::default()
                    },
                )
                .await?;
            }
            let value = kv
                .get_with_metadata(context, "policy", cellar::sdk::kv::ReadType::Json)
                .await?;
            let payload = match value.value {
                Some(cellar::sdk::kv::ReadValue::Json(v)) => v,
                None => Value::Null,
                _ => anyhow::bail!("unexpected KV encoding"),
            };
            json!({"policy": payload, "metadata": value.metadata})
        }
        "queue" | "dlq" => {
            ensure!(tenant < counts.queues / 2 && id == 0, "unknown queue");
            let producer =
                context.queue(&binding(if kind == "dlq" { "DLQ" } else { "Q" }, tenant, 0))?;
            if method == "POST" {
                let source_scope = format!("seed-{kind}-{tenant}");
                let version = input["version"].as_u64().unwrap_or(0);
                let mut snapshot = input.clone();
                if snapshot.get("state").is_none() {
                    snapshot["state"] = json!({"version":version});
                }
                let effect = Effect {
                    effect_id: input["effect_id"].as_str().unwrap_or(&source_scope).into(),
                    tenant: tenant * 2,
                    source_scope,
                    kind: input["kind"].as_str().unwrap_or("projection").into(),
                    version,
                    snapshot,
                };
                producer
                    .send(
                        context,
                        Body::Json(serde_json::to_value(effect)?),
                        Default::default(),
                    )
                    .await?;
            }
            let metrics = producer.metrics(context).await?;
            json!({"backlog_count": metrics.backlog_count, "backlog_bytes": metrics.backlog_bytes,
                "oldest_message_timestamp_ms": metrics.oldest_message_timestamp_ms})
        }
        "workflow" => {
            ensure!(id < counts.workflows / counts.tenants, "unknown dispatcher");
            let workflow = context.workflow("DISPATCH")?;
            let wid = workflow_id(tenant, id);
            let instance =
                if method == "POST" && input["action"].as_str().unwrap_or("seed") == "seed" {
                    workflow
                        .create(
                            context,
                            workflow::CreateOptions {
                                id: Some(wid),
                                params: Some(StructuredValue::from_json(
                                    json!({"tenant": tenant, "slot": id}),
                                )?),
                                ..Default::default()
                            },
                        )
                        .await?
                } else {
                    workflow.get(context, &wid).await?
                };
            if method == "POST" {
                match input["action"].as_str().unwrap_or("seed") {
                    "event" => {
                        instance
                            .send_event(
                                context,
                                workflow::SendEvent {
                                    event_type: input["type"]
                                        .as_str()
                                        .context("event type absent")?
                                        .into(),
                                    payload: Some(StructuredValue::from_json(
                                        input["payload"].clone(),
                                    )?),
                                },
                            )
                            .await?
                    }
                    "pause" => instance.pause(context).await?,
                    "resume" => instance.resume(context).await?,
                    "restart" => instance.restart(context, Default::default()).await?,
                    "terminate" => instance.terminate(context).await?,
                    "delete" => {
                        instance.delete(context).await?;
                        return response(context, json!({"deleted": true}));
                    }
                    "seed" => {}
                    _ => anyhow::bail!("unknown workflow action"),
                }
            }
            let status = instance.status(context).await?;
            json!({"id": instance.id(), "state": format!("{:?}", status.state)})
        }
        _ => anyhow::bail!("unknown system operation"),
    };
    response(context, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compensation_distinguishes_no_stock_effect_from_irreversible_consumption() {
        for status in [
            None,
            Some(ReservationStatus::Held),
            Some(ReservationStatus::Released),
            Some(ReservationStatus::Expired),
        ] {
            assert_eq!(compensation_status(status), "cancelled");
        }
        assert_eq!(
            compensation_status(Some(ReservationStatus::Consumed)),
            "needs_review"
        );
    }
}
