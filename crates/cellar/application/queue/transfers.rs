// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A persisted retry boundary prevents a parked transfer from immediately
//! dispatching again through due work. Target receipts still own deduplication.

use super::sql::*;
use crate::application::{
    storage::{SqlRow, SqlValue},
    Storage,
};
use anyhow::Result;

const PENDING_RAFT: &str =
    "SELECT seq,id,body,content_type,size,dlq_target,dlq_transfer_id,transfer_kind
     FROM __sys_queue_messages INDEXED BY __sys_queue_messages_moving_due
     WHERE dlq_target IS NOT NULL AND enqueued_at>?
       AND COALESCE(leased_until, -9223372036854775808)<=?
     ORDER BY COALESCE(leased_until, -9223372036854775808),seq LIMIT ?";

const NEXT_RETRY_RAFT: &str = "SELECT leased_until FROM __sys_queue_messages
     INDEXED BY __sys_queue_messages_moving_due
     WHERE dlq_target IS NOT NULL
     ORDER BY COALESCE(leased_until, -9223372036854775808),seq LIMIT 1";

/// Stamp new native work with its ready time so continual arrivals cannot
/// displace an older expired retry. Consumer lease IDs remain absent. Fleet
/// retains its existing immediate-dispatch representation and ordering.
pub(super) fn ready_at(storage: &Storage, at: i64) -> SqlValue {
    if storage.is_raft() {
        at.into()
    } else {
        SqlValue::Null
    }
}

pub(super) fn pending(
    storage: &Storage,
    cutoff: i64,
    at: i64,
    limit: usize,
) -> Result<Vec<SqlRow>> {
    let limit = i64::try_from(limit)?;
    if storage.is_raft() {
        storage.system_sql(PENDING_RAFT, &[cutoff.into(), at.into(), limit.into()])
    } else {
        storage.system_sql(
            "SELECT seq,id,body,content_type,size,dlq_target,dlq_transfer_id,transfer_kind
             FROM __sys_queue_messages WHERE dlq_target IS NOT NULL AND enqueued_at>?
             ORDER BY seq LIMIT ?",
            &[cutoff.into(), limit.into()],
        )
    }
}

/// The caller must rearm committed due work before the binding's source proof.
/// Only this invocation's remaining deadline bounds the claim; it grants no
/// target acknowledgement and never changes a transfer ID or consumer attempt.
pub(super) fn claim<'a>(
    storage: &Storage,
    rows: &[&'a SqlRow],
    at: i64,
    until: i64,
) -> Result<Vec<&'a SqlRow>> {
    anyhow::ensure!(until > at, "Queue transfer deadline has elapsed");
    storage.transaction_sync(|_| {
        let mut claimed = Vec::new();
        for row in rows {
            // An earlier target group can have parked this invocation. A
            // successor writer may have deleted, replaced or claimed this row.
            let changed = storage.system_sql(
                "UPDATE __sys_queue_messages SET leased_until=?
                 WHERE seq=? AND dlq_target=? AND dlq_transfer_id=? AND transfer_kind=?
                   AND lease_id IS NULL AND (leased_until IS NULL OR leased_until<=?)
                 RETURNING seq",
                &[
                    until.into(),
                    number(row, "seq")?.into(),
                    text(row, "dlq_target")?.into(),
                    text(row, "dlq_transfer_id")?.into(),
                    text(row, "transfer_kind")?.into(),
                    at.into(),
                ],
            )?;
            if !changed.is_empty() {
                claimed.push(*row);
            }
        }
        Ok(claimed)
    })
}

pub(super) fn next_retry(storage: &Storage, at: i64) -> Result<Option<i64>> {
    if storage.is_raft() {
        storage
            .system_sql(NEXT_RETRY_RAFT, &[])?
            .first()
            .map(|row| Ok(optional_number(row, "leased_until")?.unwrap_or(at)))
            .transpose()
    } else {
        Ok((!storage
            .system_sql(
                "SELECT 1 FROM __sys_queue_messages INDEXED BY __sys_queue_messages_moving
             WHERE dlq_target IS NOT NULL LIMIT 1",
                &[],
            )?
            .is_empty())
        .then_some(at))
    }
}

pub(super) fn delivered(storage: &Storage, rows: &[&SqlRow]) -> Result<()> {
    storage.transaction_sync(|_| {
        for row in rows {
            storage.system_sql(
                "DELETE FROM __sys_queue_messages
                 WHERE seq=? AND dlq_target=? AND dlq_transfer_id=? AND transfer_kind=?",
                &[
                    number(row, "seq")?.into(),
                    text(row, "dlq_target")?.into(),
                    text(row, "dlq_transfer_id")?.into(),
                    text(row, "transfer_kind")?.into(),
                ],
            )?;
        }
        Ok(())
    })
}

#[cfg(test)]
#[path = "transfers_tests.rs"]
mod tests;
