// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Queue inspection over a caller's already selected committed image.
//! This view never installs schema, advances clocks, expires leases or rearms work.

use super::broker::{blob, number, optional_number, optional_text, text};
use crate::application::{
    storage::{SqlRow, SqlValue},
    Storage,
};
use anyhow::{ensure, Context as _, Result};
use base64::Engine as _;
use serde_json::{json, Value};

pub(super) struct Metadata<'a> {
    storage: &'a Storage,
    retention_ms: Option<i64>,
    max_admin_batch_size: usize,
}

impl<'a> Metadata<'a> {
    pub(super) fn new(
        storage: &'a Storage,
        retention_ms: Option<i64>,
        max_admin_batch_size: usize,
    ) -> Self {
        Self {
            storage,
            retention_ms,
            max_admin_batch_size,
        }
    }

    fn exec(&self, query: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        self.storage.system_sql_read(query, params)
    }

    fn retention_cutoff(&self, at: i64) -> i64 {
        self.retention_ms
            .map_or(i64::MIN, |ms| at.saturating_sub(ms))
    }

    pub(super) fn info(&self, at: i64) -> Result<Value> {
        let mut value = self.metrics(at, false)?;
        let row = self
            .exec("SELECT stored_count FROM __sys_queue_stats WHERE id=1", &[])?
            .into_iter()
            .next()
            .context("Queue statistics are missing")?;
        value["stored"] = json!(number(&row, "stored_count")?);
        let paused = self
            .exec(
                "SELECT value FROM __sys_queue_meta WHERE name='paused'",
                &[],
            )?
            .first()
            .map(|row| text(row, "value"))
            .transpose()?
            .as_deref()
            == Some("1");
        value["paused"] = json!(paused);
        Ok(value)
    }

    pub(super) fn read(&self, body: &Value, at: i64) -> Result<Value> {
        match body["op"].as_str() {
            Some("info") => self.info(at),
            Some("peek") => self.peek(
                at,
                limit(body, 10, self.max_admin_batch_size)?,
                body["console"].as_bool().unwrap_or(false),
            ),
            _ => {
                anyhow::bail!(crate::application::consensus::CommandFailure::UnsupportedCapability)
            }
        }
    }

    pub(super) fn metrics(&self, at: i64, waiting: bool) -> Result<Value> {
        let cutoff = self.retention_cutoff(at);
        let stats=self.exec("SELECT backlog_count,backlog_bytes,oldest_message_timestamp FROM __sys_queue_stats WHERE id=1",&[])?.remove(0);
        let oldest = optional_number(&stats, "oldest_message_timestamp")?;
        let (count, bytes, oldest) = if oldest.is_some_and(|oldest| oldest <= cutoff) {
            let row=self.exec("SELECT COUNT(*) AS backlog_count,COALESCE(SUM(size),0) AS backlog_bytes,MIN(enqueued_at) AS oldest_message_timestamp FROM __sys_queue_messages WHERE purge_on_settle=0 AND enqueued_at>? AND (?=0 OR (lease_id IS NULL AND dlq_target IS NULL AND visible_at<=?))",&[cutoff.into(),i64::from(waiting).into(),at.into()])?.remove(0);
            (
                number(&row, "backlog_count")?,
                number(&row, "backlog_bytes")?,
                optional_number(&row, "oldest_message_timestamp")?,
            )
        } else if waiting {
            let blocked=self.exec("SELECT COALESCE(SUM(blocked_count),0) AS blocked_count,COALESCE(SUM(blocked_bytes),0) AS blocked_bytes FROM (SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_lease WHERE purge_on_settle=0 AND lease_id IS NOT NULL UNION ALL SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_moving WHERE purge_on_settle=0 AND dlq_target IS NOT NULL UNION ALL SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_ready WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at>?)",&[at.into()])?.remove(0);
            let oldest=self.exec("SELECT enqueued_at FROM __sys_queue_messages INDEXED BY __sys_queue_messages_retained WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at<=? ORDER BY enqueued_at,seq LIMIT 1",&[at.into()])?.first().map(|row|number(row,"enqueued_at")).transpose()?;
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

    pub(super) fn peek(&self, at: i64, requested: i64, console: bool) -> Result<Value> {
        let mut limit = requested;
        let mut truncated = false;
        if console {
            ensure!(requested <= 100, "console peek limit cannot exceed 100");
            let sizes = self.exec("SELECT length(body) AS bytes FROM __sys_queue_messages WHERE enqueued_at>? ORDER BY visible_at,seq LIMIT ?", &[self.retention_cutoff(at).into(),requested.into()])?;
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
        let rows=self.exec("SELECT id,body,content_type,size,enqueued_at,visible_at,attempts,lease_id,lease_generation,leased_until,purge_on_settle,dlq_target,dlq_transfer_id,transfer_kind,dead_letter_source FROM __sys_queue_messages WHERE enqueued_at>? ORDER BY visible_at,seq LIMIT ?",&[self.retention_cutoff(at).into(),limit.into()])?;
        let messages=rows.iter().map(|row| {let lease=optional_text(row,"lease_id")?.is_some() && optional_number(row,"leased_until")?.is_some_and(|until|until>at);let target=optional_text(row,"dlq_target")?;
            Ok(json!({"id":text(row,"id")?,"bodyBase64":base64::engine::general_purpose::STANDARD.encode(blob(row,"body")?),"contentType":text(row,"content_type")?,"size":number(row,"size")?,"enqueuedAt":number(row,"enqueued_at")?,"visibleAt":number(row,"visible_at")?,"attempts":number(row,"attempts")?,"state":if target.is_some(){"moving"}else if number(row,"purge_on_settle")?!=0{"purging"}else if lease{"leased"}else if number(row,"visible_at")?<=at{"visible"}else{"delayed"},"leasedUntil":if lease{optional_number(row,"leased_until")?}else{None},"transferTarget":target,"deadLetterSource":optional_text(row,"dead_letter_source")?,"deliveryAttempts":number(row,"attempts")?.saturating_add(i64::from(optional_text(row,"lease_id")?.is_some())),"lease":{"active":lease,"expiresAt":optional_number(row,"leased_until")?,"generation":number(row,"lease_generation")?},"provenance":{"sourceQueue":optional_text(row,"dead_letter_source")?,"transferId":optional_text(row,"dlq_transfer_id")?,"kind":optional_text(row,"transfer_kind")?,"targetQueue":target}}))
        }).collect::<Result<Vec<_>>>()?;
        Ok(if console {
            json!({"messages":messages,"truncated":truncated,"byte_budget":512*1024,"observedAt":at})
        } else {
            json!({"messages":messages})
        })
    }
}

pub(super) fn limit(body: &Value, default: i64, maximum: usize) -> Result<i64> {
    let limit = match body.get("limit").filter(|value| !value.is_null()) {
        None => default,
        Some(value) => value.as_i64().context("limit must be a positive integer")?,
    };
    ensure!(
        limit > 0 && limit as u64 <= maximum as u64,
        "QUEUE_ERROR: limit must be from 1 through {}",
        maximum
    );
    Ok(limit)
}

pub(crate) fn read(storage: &Storage, body: &Value) -> Result<Value> {
    let (retention, maximum) = crate::queue_policy::inspection_limits();
    Metadata::new(storage, retention, maximum).read(body, super::now())
}

pub(crate) fn metrics(storage: &Storage) -> Result<Value> {
    let (retention, maximum) = crate::queue_policy::inspection_limits();
    Metadata::new(storage, retention, maximum).metrics(super::now(), false)
}
