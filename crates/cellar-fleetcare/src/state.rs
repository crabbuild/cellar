use crate::config::AppConfig;
use crate::contracts::*;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::Storage;
use serde_json::{json, Value};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS command_receipts(
 command_id TEXT PRIMARY KEY, digest TEXT NOT NULL, ordinal INTEGER UNIQUE NOT NULL,
 version INTEGER NOT NULL, accepted INTEGER NOT NULL, receipt_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS changes(
 version INTEGER PRIMARY KEY, command_id TEXT NOT NULL, snapshot_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS outbox(
 effect_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
 attempts INTEGER NOT NULL DEFAULT 0, next_attempt_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(delivered,next_attempt_ms);
CREATE TABLE IF NOT EXISTS reservations(
 id TEXT PRIMARY KEY, qty INTEGER NOT NULL CHECK(qty>0), state TEXT NOT NULL, expires_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS reservation_expiry ON reservations(state,expires_ms);
CREATE TABLE IF NOT EXISTS orders(
 id TEXT PRIMARY KEY, order_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS attachments(
 key TEXT PRIMARY KEY, order_id TEXT NOT NULL, reference_json TEXT NOT NULL);
";

pub fn initialize(storage: &Storage, class: &str) -> Result<()> {
    if storage.get_json("schema_version")?.is_none() {
        storage.transaction_sync(|tx| {
            tx.sql(SCHEMA, &[])?;
            tx.put_json(
                "state",
                &json!({
                    "class": class, "version": 0, "commands": 0,
                    "last_seq": 0, "health": "healthy", "last_seen_ms": 0,
                    "on_hand": 1_000_000, "reserved": 0, "replenished": 0, "consumed": 0
                }),
            )?;
            tx.put_json("schema_version", &json!(1))
        })?;
    }
    ensure!(
        storage.get_json("schema_version")? == Some(json!(1)),
        "unsupported schema version"
    );
    Ok(())
}

pub fn receipt(storage: &Storage, command_id: &str) -> Result<Option<Receipt>> {
    let (_, rows, _) = storage.sql(
        "SELECT receipt_json FROM command_receipts WHERE command_id=?1",
        &[json!(command_id)],
    )?;
    rows.first()
        .map(|r| {
            serde_json::from_str(r[0].as_str().context("invalid receipt")?).map_err(Into::into)
        })
        .transpose()
}

pub fn state(storage: &Storage) -> Result<Value> {
    storage
        .get_json("state")?
        .context("cell has no seeded state")
}

fn number(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .with_context(|| format!("invalid {field}"))
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    let value = value[field]
        .as_str()
        .with_context(|| format!("missing {field}"))?;
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
        "invalid {field}"
    );
    Ok(value)
}

#[derive(Clone, Copy)]
pub struct EntityKey<'a> {
    pub class: &'a str,
    pub tenant: usize,
    pub entity: usize,
    pub scope: &'a str,
}

pub fn apply(
    storage: &Storage,
    config: &AppConfig,
    key: EntityKey<'_>,
    command: &Command,
    now: i64,
) -> Result<(Receipt, bool)> {
    let EntityKey {
        class,
        tenant,
        entity,
        scope,
    } = key;
    let digest = command.digest(scope)?;
    storage.transaction_sync(|tx| {
        if let Some(receipt) = receipt(tx, &command.command_id)? {
            ensure!(
                receipt.digest == digest,
                "command ID reused with another request"
            );
            return Ok((receipt, true));
        }
        let mut current = state(tx)?;
        let version = number(&current, "version")?;
        let ordinal = number(&current, "commands")?
            .checked_add(1)
            .context("command overflow")?;
        // A rejected mutation rolls back its nested transaction, while the
        // outer transaction retains a deterministic idempotency receipt.
        let mutation = tx.transaction_sync(|view| {
            if let Some(expected) = command.expected_version {
                ensure!(expected == version, "version conflict");
            }
            mutate(view, config, key, &mut current, command, now)
        });
        let accepted = mutation.is_ok();
        let result = match mutation {
            Ok(value) => value,
            Err(error) => {
                current = state(tx)?;
                json!({"error": error.to_string()})
            }
        };
        let next = if accepted {
            version.checked_add(1).context("version overflow")?
        } else {
            version
        };
        current["version"] = json!(next);
        current["commands"] = json!(ordinal);
        tx.put_json("state", &current)?;
        let receipt = Receipt {
            command_id: command.command_id.clone(),
            digest: digest.clone(),
            ordinal,
            version: next,
            accepted,
            result,
        };
        tx.sql(
            "INSERT INTO command_receipts VALUES(?1,?2,?3,?4,?5,?6)",
            &[
                json!(receipt.command_id),
                json!(receipt.digest),
                json!(ordinal),
                json!(next),
                json!(accepted),
                json!(serde_json::to_string(&receipt)?),
            ],
        )?;
        if accepted {
            let snapshot = json!({"state": current, "result": receipt.result});
            tx.sql(
                "INSERT INTO changes VALUES(?1,?2,?3)",
                &[
                    json!(next),
                    json!(command.command_id),
                    json!(serde_json::to_string(&snapshot)?),
                ],
            )?;
            if config.asynchronous {
                let effect = Effect {
                    effect_id: format!("{scope}-{}", command.command_id),
                    tenant,
                    source_scope: scope.into(),
                    kind: "projection".into(),
                    version: next,
                    snapshot,
                };
                insert_outbox(tx, &effect, now)?;
                if class == "WorkOrderBook"
                    && command.action == "create"
                    && command.payload["saga"] == true
                {
                    insert_outbox(
                        tx,
                        &Effect {
                            effect_id: format!("dispatch-{scope}-{}", command.command_id),
                            tenant,
                            source_scope: scope.into(),
                            kind: "dispatch".into(),
                            version: next,
                            snapshot: json!({"book": entity, "order_id": receipt.result["order_id"],
                            "inventory_id": receipt.result["inventory_id"]}),
                        },
                        now,
                    )?;
                }
            }
        }
        schedule(tx, config, class, now)?;
        Ok((receipt, false))
    })
}

pub fn insert_outbox(storage: &Storage, effect: &Effect, now: i64) -> Result<()> {
    storage.sql(
        "INSERT OR IGNORE INTO outbox(effect_id,payload_json,next_attempt_ms) VALUES(?1,?2,?3)",
        &[
            json!(effect.effect_id),
            json!(serde_json::to_string(effect)?),
            json!(now),
        ],
    )?;
    Ok(())
}

fn mutate(
    tx: &Storage,
    config: &AppConfig,
    key: EntityKey<'_>,
    current: &mut Value,
    command: &Command,
    now: i64,
) -> Result<Value> {
    let EntityKey {
        class,
        tenant,
        entity,
        ..
    } = key;
    let payload = &command.payload;
    match (class, command.action.as_str()) {
        (_, "seed") => Ok(json!({"seeded": true})),
        ("Device", "telemetry") => {
            let last = number(current, "last_seq")?;
            let seq = match payload["seq"].as_u64() {
                Some(seq) => seq,
                None => last.checked_add(1).context("device sequence overflow")?,
            };
            ensure!(seq > last, "device sequence regression");
            current["last_seq"] = json!(seq);
            current["latest"] = payload.clone();
            current["last_seen_ms"] = json!(now);
            let ttl = payload["offline_after_ms"]
                .as_i64()
                .unwrap_or(config.offline_ms);
            ensure!(
                ttl >= 100 && ttl <= config.offline_ms,
                "invalid offline deadline"
            );
            current["deadline_ms"] = json!(now + ttl);
            current["health"] = json!(if payload["temperature"].as_f64().unwrap_or(20.0) > 80.0 {
                "alerting"
            } else {
                "healthy"
            });
            Ok(json!({"seq": seq, "health": current["health"]}))
        }
        ("Device", "maintenance") => {
            current["health"] = json!(if payload["enabled"].as_bool().unwrap_or(false) {
                "maintenance"
            } else {
                "healthy"
            });
            Ok(json!({"health": current["health"]}))
        }
        ("WorkOrderBook", "create") => {
            let id = text(payload, "order_id")?;
            let order = Order {
                order_id: id.into(),
                status: OrderStatus::Open,
                inventory_id: payload["inventory_id"].as_u64().unwrap_or(0) as usize,
                reservation_id: reservation_id(tenant, entity, id),
                version: 1,
            };
            ensure!(
                order.inventory_id
                    < config.profile.counts().inventory / config.profile.counts().tenants,
                "unknown inventory target"
            );
            tx.sql(
                "INSERT INTO orders VALUES(?1,?2)",
                &[json!(id), json!(serde_json::to_string(&order)?)],
            )?;
            Ok(serde_json::to_value(order)?)
        }
        ("WorkOrderBook", "transition") => {
            let id = text(payload, "order_id")?;
            let mut order = order(tx, id)?.context("order absent")?;
            let next: OrderStatus = serde_json::from_value(payload["status"].clone())?;
            order.status = order.status.transition(next)?;
            order.version = order
                .version
                .checked_add(1)
                .context("order version overflow")?;
            tx.sql(
                "UPDATE orders SET order_json=?2 WHERE id=?1",
                &[json!(id), json!(serde_json::to_string(&order)?)],
            )?;
            Ok(serde_json::to_value(order)?)
        }
        ("WorkOrderBook", "attach") => {
            let id = text(payload, "order_id")?;
            ensure!(order(tx, id)?.is_some(), "attachment order absent");
            let key = payload["key"].as_str().context("attachment key absent")?;
            ensure!(
                key.len() <= 512
                    && payload["sha256"].as_str().is_some_and(|s| s.len() == 64)
                    && payload["bytes"]
                        .as_u64()
                        .is_some_and(|n| n <= 32 * 1024 * 1024),
                "invalid attachment reference"
            );
            tx.sql("INSERT INTO attachments VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET reference_json=excluded.reference_json",
                &[json!(key), json!(id), json!(serde_json::to_string(payload)?)])?;
            Ok(payload.clone())
        }
        ("Inventory", "reserve") => {
            let id = text(payload, "reservation_id")?;
            let qty = payload["qty"].as_u64().context("missing qty")?;
            ensure!(qty > 0 && qty <= 1_000_000, "invalid quantity");
            let available = number(current, "on_hand")?
                .checked_sub(number(current, "reserved")?)
                .context("invalid inventory balance")?;
            ensure!(qty <= available, "insufficient inventory");
            let ttl = payload["ttl_ms"].as_i64().unwrap_or(config.reservation_ms);
            ensure!(
                ttl >= 100 && ttl <= config.reservation_ms,
                "invalid reservation deadline"
            );
            let reservation = Reservation {
                id: id.into(),
                qty,
                status: ReservationStatus::Held,
                expires_ms: now + ttl,
            };
            tx.sql(
                "INSERT INTO reservations VALUES(?1,?2,'held',?3)",
                &[json!(id), json!(qty), json!(reservation.expires_ms)],
            )?;
            current["reserved"] = json!(number(current, "reserved")? + qty);
            Ok(serde_json::to_value(reservation)?)
        }
        ("Inventory", action @ ("consume" | "release")) => {
            let id = text(payload, "reservation_id")?;
            let reservation = reservation(tx, id)?.context("reservation absent")?;
            ensure!(
                reservation.status == ReservationStatus::Held,
                "reservation is terminal"
            );
            ensure!(reservation.expires_ms > now, "reservation expired");
            current["reserved"] = json!(number(current, "reserved")? - reservation.qty);
            let next = if action == "consume" {
                current["on_hand"] = json!(number(current, "on_hand")? - reservation.qty);
                current["consumed"] = json!(number(current, "consumed")? + reservation.qty);
                "consumed"
            } else {
                "released"
            };
            tx.sql(
                "UPDATE reservations SET state=?2 WHERE id=?1",
                &[json!(id), json!(next)],
            )?;
            Ok(json!({"reservation_id": id, "state": next, "qty": reservation.qty}))
        }
        ("Inventory", "replenish") => {
            let qty = payload["qty"].as_u64().context("missing qty")?;
            ensure!(qty > 0 && qty <= 1_000_000, "invalid quantity");
            current["on_hand"] = json!(number(current, "on_hand")?
                .checked_add(qty)
                .context("stock overflow")?);
            current["replenished"] = json!(number(current, "replenished")?
                .checked_add(qty)
                .context("stock overflow")?);
            Ok(json!({"on_hand": current["on_hand"]}))
        }
        _ => anyhow::bail!("unsupported command for {class}"),
    }
}

pub fn order(storage: &Storage, id: &str) -> Result<Option<Order>> {
    let (_, rows, _) = storage.sql("SELECT order_json FROM orders WHERE id=?1", &[json!(id)])?;
    rows.first()
        .map(|r| serde_json::from_str(r[0].as_str().context("invalid order")?).map_err(Into::into))
        .transpose()
}

pub fn reservation(storage: &Storage, id: &str) -> Result<Option<Reservation>> {
    let (_, rows, _) = storage.sql(
        "SELECT id,qty,state,expires_ms FROM reservations WHERE id=?1",
        &[json!(id)],
    )?;
    rows.first()
        .map(|r| {
            Ok(Reservation {
                id: r[0].as_str().context("invalid reservation")?.into(),
                qty: r[1].as_u64().context("invalid reservation quantity")?,
                status: serde_json::from_value(r[2].clone())?,
                expires_ms: r[3].as_i64().context("invalid expiration")?,
            })
        })
        .transpose()
}

pub fn schedule(storage: &Storage, config: &AppConfig, class: &str, now: i64) -> Result<()> {
    let (_, rows, _) = storage.sql(
        "SELECT min(next_attempt_ms) FROM outbox WHERE delivered=0",
        &[],
    )?;
    let mut next = rows[0][0].as_i64();
    if class == "Device" {
        let state = state(storage)?;
        let seen = state["last_seen_ms"].as_i64().unwrap_or(0);
        if seen > 0 && state["health"] != "offline" && state["health"] != "maintenance" {
            let deadline = state["deadline_ms"]
                .as_i64()
                .unwrap_or(seen + config.offline_ms);
            next = Some(next.map_or(deadline, |n| n.min(deadline)));
        }
    }
    if class == "Inventory" {
        let (_, rows, _) = storage.sql(
            "SELECT min(expires_ms) FROM reservations WHERE state='held'",
            &[],
        )?;
        if let Some(expiry) = rows[0][0].as_i64() {
            next = Some(next.map_or(expiry, |n| n.min(expiry)));
        }
    }
    match next {
        Some(next) => storage.set_alarm(next.max(now + 1))?,
        None => {
            storage.delete_alarm()?;
        }
    }
    Ok(())
}

pub fn process_deadlines(
    storage: &Storage,
    config: &AppConfig,
    class: &str,
    tenant: usize,
    scope: &str,
    now: i64,
) -> Result<()> {
    storage.transaction_sync(|tx| {
        let mut current = state(tx)?;
        let original = current.clone();
        if class == "Device" && current["health"] != "maintenance" && current["health"] != "offline"
            && current["last_seen_ms"].as_i64().unwrap_or(0) > 0
            && current["deadline_ms"].as_i64().unwrap_or(current["last_seen_ms"].as_i64().unwrap_or(0) + config.offline_ms) <= now {
            current["health"] = json!("offline");
        }
        if class == "Inventory" {
            let (_, rows, _) = tx.sql("SELECT coalesce(sum(qty),0) FROM reservations WHERE state='held' AND expires_ms<=?1", &[json!(now)])?;
            let expired = rows[0][0].as_u64().context("invalid expiry sum")?;
            if expired > 0 {
                current["reserved"] = json!(number(&current, "reserved")?.checked_sub(expired).context("expiry balance failed")?);
                tx.sql("UPDATE reservations SET state='expired' WHERE state='held' AND expires_ms<=?1", &[json!(now)])?;
            }
        }
        if current != original {
            let version = number(&current, "version")?.checked_add(1).context("version overflow")?;
            current["version"] = json!(version);
            tx.put_json("state", &current)?;
            let id = format!("alarm-v{version}");
            let snapshot = json!({"state": current, "result": {"deadline_ms": now}});
            tx.sql("INSERT INTO changes VALUES(?1,?2,?3)",
                &[json!(version), json!(id), json!(serde_json::to_string(&snapshot)?)])?;
            if config.asynchronous {
                insert_outbox(tx, &Effect { effect_id: format!("{scope}-{id}"), tenant,
                    source_scope: scope.into(), kind: "projection".into(), version, snapshot }, now)?;
            }
        }
        Ok(())
    })
}

pub fn audit(storage: &Storage) -> Result<Value> {
    let current = state(storage)?;
    let (_, receipts, _) = storage.sql(
        "SELECT count(*),coalesce(max(ordinal),0),count(DISTINCT ordinal) FROM command_receipts",
        &[],
    )?;
    let (_, held, _) = storage.sql(
        "SELECT coalesce(sum(qty),0) FROM reservations WHERE state='held'",
        &[],
    )?;
    ensure!(
        receipts[0][0] == current["commands"]
            && receipts[0][1] == current["commands"]
            && receipts[0][2] == current["commands"],
        "receipt ordinal audit failed"
    );
    if current["class"] == "Inventory" {
        let hand = number(&current, "on_hand")?;
        let reserved = number(&current, "reserved")?;
        ensure!(
            reserved <= hand && held[0][0] == json!(reserved),
            "reservation balance failed"
        );
        ensure!(
            hand == 1_000_000 + number(&current, "replenished")? - number(&current, "consumed")?,
            "inventory conservation failed"
        );
    }
    let (_, pending, _) = storage.sql("SELECT count(*) FROM outbox WHERE delivered=0", &[])?;
    let (_, history, _) =
        storage.sql("SELECT count(*),coalesce(max(version),0) FROM changes", &[])?;
    ensure!(
        history[0][0] == current["version"] && history[0][1] == current["version"],
        "version history audit failed"
    );
    Ok(
        json!({"state": current, "receipts": receipts[0][0], "pending_outbox": pending[0][0],
        "database_bytes": storage.sql_database_size()?}),
    )
}
