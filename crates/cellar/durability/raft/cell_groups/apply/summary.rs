// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use rusqlite::OptionalExtension;

pub(super) fn read(
    connection: &rusqlite::Connection,
    max: u32,
) -> Result<EffectSummary, ExecutionError> {
    let scan = |table: &str, column: &str| -> Result<(u64, Option<u64>), ExecutionError> {
        let kind: Option<String> = connection
            .query_row(
                "SELECT type FROM sqlite_schema WHERE name=?1",
                [table],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage)?;
        match kind.as_deref() {
            None => return Ok((0, None)),
            Some("table") => (),
            _ => return Err(ExecutionError::Invalid("effect summary schema")),
        }
        // Only constant internal identifiers reach this function. Limit the scan
        // before aggregation; corrupt/unbounded effect state cannot monopolize a worker.
        let sql = format!("SELECT COUNT(*), MIN(value), COALESCE(SUM(invalid),0) FROM (SELECT {column} AS value, CASE WHEN typeof({column})='integer' AND {column}>=0 THEN 0 ELSE 1 END AS invalid FROM {table} LIMIT ?1)");
        let (count, earliest, invalid): (u64, Option<u64>, u64) = connection
            .query_row(&sql, [u64::from(max) + 1], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(storage)?;
        if count > u64::from(max) || invalid != 0 {
            return Err(ExecutionError::Invalid("effect summary bounds/state"));
        }
        Ok((count, earliest))
    };
    let (_, next_alarm_unix_millis) = scan("__sys_raft_alarms", "due")?;
    let (pending_outbox, _) = scan("__sys_raft_outbox", "next_at")?;
    let (calls, _) = scan("__sys_raft_binding_intents", "phase")?;
    let pending_calls = if calls == 0 {
        0
    } else {
        let (pending, invalid): (u64, u64) = connection.query_row(
            "SELECT COALESCE(SUM(phase<3),0),COALESCE(SUM(CASE WHEN typeof(format)='integer' AND format=1 AND phase<=3 THEN 0 ELSE 1 END),0) FROM __sys_raft_binding_intents",
            [], |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(storage)?;
        if invalid != 0 {
            return Err(ExecutionError::Invalid("binding intent summary state"));
        }
        pending
    };
    let next_outbox_unix_millis = outbox_front(connection, pending_outbox)?;
    Ok(EffectSummary {
        next_alarm_unix_millis,
        next_outbox_unix_millis,
        pending_outbox,
        // The current inbox is synchronous receipt/deduplication state, not an
        // asynchronous work queue. Retained inbox receipts cannot prevent parking.
        pending_inbox: 0,
        pending_calls,
    })
}

fn outbox_front(
    connection: &rusqlite::Connection,
    count: u64,
) -> Result<Option<u64>, ExecutionError> {
    if count == 0 {
        return Ok(None);
    }
    // The preceding scan has already bounded every row and validated its time.
    // Read fixed-size metadata only; no payload allocation or unbounded sort.
    // Later deliveries cannot run before their lane's oldest sequence, even
    // when those later rows have next_at=0. Scheduling that zero spins on an
    // otherwise sleeping cell until its dispatched head becomes retryable.
    let mut statement = connection.prepare(
        "SELECT CASE WHEN typeof(lane)='text' AND length(CAST(lane AS BLOB)) BETWEEN 1 AND 64 THEN lane ELSE NULL END,CASE WHEN typeof(sequence)='blob' AND length(sequence)=8 THEN sequence ELSE NULL END,next_at FROM __sys_raft_outbox LIMIT ?1",
    ).map_err(storage)?;
    let mut rows = statement.query([count]).map_err(storage)?;
    let mut fronts = std::collections::BTreeMap::<String, (u64, u64)>::new();
    while let Some(row) = rows.next().map_err(storage)? {
        let lane: String = row.get(0).map_err(storage)?;
        let sequence: Vec<u8> = row.get(1).map_err(storage)?;
        let sequence = u64::from_be_bytes(
            sequence
                .try_into()
                .map_err(|_| ExecutionError::Invalid("effect outbox sequence"))?,
        );
        let next: u64 = row.get(2).map_err(storage)?;
        if sequence == 0 || !crate::raft::effects::delivery::valid_name(&lane) {
            return Err(ExecutionError::Invalid("effect outbox identity"));
        }
        if !fronts.contains_key(&lane) && fronts.len() == crate::raft::effects::outbox::MAX_LANES {
            return Err(ExecutionError::Invalid("effect outbox lane bounds"));
        }
        let front = fronts.entry(lane).or_insert((sequence, next));
        if sequence < front.0 {
            *front = (sequence, next);
        }
    }
    let next = fronts
        .values()
        .map(|(_, next)| *next)
        .min()
        .ok_or(ExecutionError::Invalid("effect outbox row count changed"))?;
    Ok(Some(next))
}

#[cfg(test)]
#[path = "summary_tests.rs"]
mod tests;
