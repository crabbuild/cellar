// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Queue observations share one implementation for commands and pinned images.

use super::{error, now, sql::*};
use crate::application::{decode_rpc, encode_rpc, Storage};
use crate::engine_api::RpcData;
use anyhow::{ensure, Result};
use base64::Engine;
use cellar_native::clone::StructuredValue;
use serde_json::{json, Value};

pub(crate) const OPERATION: &str = "__cellar_queue_operator_read";

/// None means that the selected image has not installed the Queue schema yet.
/// Only the authenticated operator may then enter the ordinary initialization
/// command, retaining its original admission, arguments and deadline.
pub(crate) fn operator(storage: Storage, args: RpcData) -> Result<Option<RpcData>> {
    let input = decode_rpc(&args)?.to_json()?;
    let body = input.get(0).ok_or_else(|| error("Queue operator input"))?;
    let op = body["op"].as_str();
    ensure!(
        matches!(op, Some("info" | "peek")),
        "Queue observation requires info or peek"
    );
    let limits = crate::application::limits::current().queue;
    let limit = if op == Some("peek") {
        let limit = match body.get("limit").filter(|value| !value.is_null()) {
            None => 10,
            Some(value) => value
                .as_i64()
                .ok_or_else(|| error("limit must be a positive integer"))?,
        };
        ensure!(
            limit > 0 && limit as u64 <= limits.max_admin_batch_size as u64,
            "QUEUE_ERROR: limit must be from 1 through {}",
            limits.max_admin_batch_size
        );
        if body["console"].as_bool().unwrap_or(false) {
            ensure!(limit <= 100, "console peek limit cannot exceed 100");
        }
        limit
    } else {
        0
    };
    let schema = storage.system_sql_read(
        "SELECT COUNT(*) AS count FROM sqlite_schema WHERE type='table' AND name IN ('__sys_queue_stats','__sys_queue_messages','__sys_queue_meta')", &[])?;
    if number(
        schema
            .first()
            .ok_or_else(|| error("missing Queue schema count"))?,
        "count",
    )? != 3
    {
        return Ok(None);
    }
    if storage
        .system_sql_read("SELECT id FROM __sys_queue_stats WHERE id=1", &[])?
        .is_empty()
    {
        return Ok(None);
    }
    let at = now();
    let cutoff = limits
        .retention_ms
        .map_or(i64::MIN, |ms| at.saturating_sub(ms));
    let result = if op == Some("info") {
        info(&storage, cutoff, at)?
    } else {
        peek(
            &storage,
            cutoff,
            at,
            limit,
            body["console"].as_bool().unwrap_or(false),
        )?
    };
    Ok(Some(encode_rpc(&StructuredValue::from_json(result)?)?))
}

pub(super) fn info(storage: &Storage, cutoff: i64, at: i64) -> Result<Value> {
    let mut value = metrics(storage, cutoff, at, false)?;
    let rows =
        storage.system_sql_read("SELECT stored_count FROM __sys_queue_stats WHERE id=1", &[])?;
    value["stored"] = json!(number(
        rows.first()
            .ok_or_else(|| error("missing Queue statistics"))?,
        "stored_count"
    )?);
    let paused = storage.system_sql_read(
        "SELECT value FROM __sys_queue_meta WHERE name='paused'",
        &[],
    )?;
    value["paused"] = json!(
        paused
            .first()
            .map(|row| text(row, "value"))
            .transpose()?
            .as_deref()
            == Some("1")
    );
    Ok(value)
}

pub(super) fn metrics(storage: &Storage, cutoff: i64, at: i64, waiting: bool) -> Result<Value> {
    let stats=storage.system_sql_read("SELECT backlog_count,backlog_bytes,oldest_message_timestamp FROM __sys_queue_stats WHERE id=1",&[])?.into_iter().next().ok_or_else(|| error("missing Queue statistics"))?;
    let oldest = optional_number(&stats, "oldest_message_timestamp")?;
    let (count, bytes, oldest) = if oldest.is_some_and(|oldest| oldest <= cutoff) {
        let row=storage.system_sql_read("SELECT COUNT(*) AS backlog_count,COALESCE(SUM(size),0) AS backlog_bytes,MIN(enqueued_at) AS oldest_message_timestamp FROM __sys_queue_messages WHERE purge_on_settle=0 AND enqueued_at>? AND (?=0 OR (lease_id IS NULL AND dlq_target IS NULL AND visible_at<=?))",&[cutoff.into(),i64::from(waiting).into(),at.into()])?.remove(0);
        (
            number(&row, "backlog_count")?,
            number(&row, "backlog_bytes")?,
            optional_number(&row, "oldest_message_timestamp")?,
        )
    } else if waiting {
        let blocked=storage.system_sql_read("SELECT COALESCE(SUM(blocked_count),0) AS blocked_count,COALESCE(SUM(blocked_bytes),0) AS blocked_bytes FROM (SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_lease WHERE purge_on_settle=0 AND lease_id IS NOT NULL UNION ALL SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_moving WHERE purge_on_settle=0 AND dlq_target IS NOT NULL UNION ALL SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_ready WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at>?)",&[at.into()])?.remove(0);
        let oldest=storage.system_sql_read("SELECT enqueued_at FROM __sys_queue_messages INDEXED BY __sys_queue_messages_retained WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at<=? ORDER BY enqueued_at,seq LIMIT 1",&[at.into()])?.first().map(|row|number(row,"enqueued_at")).transpose()?;
        (
            number(&stats, "backlog_count")? - number(&blocked, "blocked_count")?,
            number(&stats, "backlog_bytes")? - number(&blocked, "blocked_bytes")?,
            oldest,
        )
    } else {
        (
            number(&stats, "backlog_count")?,
            number(&stats, "backlog_bytes")?,
            oldest,
        )
    };
    Ok(json!({"backlogCount":count,"backlogBytes":bytes,"oldestMessageTimestamp":oldest}))
}

pub(super) fn peek(
    storage: &Storage,
    cutoff: i64,
    at: i64,
    requested: i64,
    console: bool,
) -> Result<Value> {
    let mut limit = requested;
    let mut truncated = false;
    if console {
        ensure!(requested <= 100, "console peek limit cannot exceed 100");
        let sizes = storage.system_sql_read("SELECT length(body) AS bytes FROM __sys_queue_messages WHERE enqueued_at>? ORDER BY visible_at,seq LIMIT ?", &[cutoff.into(),requested.into()])?;
        let mut budget = 512 * 1024i64;
        limit = 0;
        for row in sizes {
            let bytes = number(&row, "bytes")?.saturating_add(4096);
            if bytes > budget {
                truncated = true;
                break;
            }
            budget -= bytes;
            limit += 1;
        }
    }
    let rows=storage.system_sql_read("SELECT id,body,content_type,size,enqueued_at,visible_at,attempts,lease_id,lease_generation,leased_until,purge_on_settle,dlq_target,dlq_transfer_id,transfer_kind,dead_letter_source FROM __sys_queue_messages WHERE enqueued_at>? ORDER BY visible_at,seq LIMIT ?",&[cutoff.into(),limit.into()])?;
    let messages=rows.iter().map(|row| {let lease=optional_text(row,"lease_id")?.is_some() && optional_number(row,"leased_until")?.is_some_and(|until|until>at);let target=optional_text(row,"dlq_target")?;
            Ok(json!({"id":text(row,"id")?,"bodyBase64":base64::engine::general_purpose::STANDARD.encode(blob(row,"body")?),"contentType":text(row,"content_type")?,"size":number(row,"size")?,"enqueuedAt":number(row,"enqueued_at")?,"visibleAt":number(row,"visible_at")?,"attempts":number(row,"attempts")?,"state":if target.is_some(){"moving"}else if number(row,"purge_on_settle")?!=0{"purging"}else if lease{"leased"}else if number(row,"visible_at")?<=at{"visible"}else{"delayed"},"leasedUntil":if lease{optional_number(row,"leased_until")?}else{None},"transferTarget":target,"deadLetterSource":optional_text(row,"dead_letter_source")?,"deliveryAttempts":number(row,"attempts")?.saturating_add(i64::from(optional_text(row,"lease_id")?.is_some())),"lease":{"active":lease,"expiresAt":optional_number(row,"leased_until")?,"generation":number(row,"lease_generation")?},"provenance":{"sourceQueue":optional_text(row,"dead_letter_source")?,"transferId":optional_text(row,"dlq_transfer_id")?,"kind":optional_text(row,"transfer_kind")?,"targetQueue":target}}))
        }).collect::<Result<Vec<_>>>()?;
    Ok(if console {
        json!({"messages":messages,"truncated":truncated,"byte_budget":512*1024,"observedAt":at})
    } else {
        json!({"messages":messages})
    })
}
