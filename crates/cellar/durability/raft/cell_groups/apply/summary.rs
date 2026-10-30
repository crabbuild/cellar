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
    let (pending_outbox, next_outbox_unix_millis) = scan("__sys_raft_outbox", "next_at")?;
    Ok(EffectSummary {
        next_alarm_unix_millis,
        next_outbox_unix_millis,
        pending_outbox,
        // The current inbox is synchronous receipt/deduplication state, not an
        // asynchronous work queue. Retained inbox receipts cannot prevent parking.
        pending_inbox: 0,
    })
}

#[cfg(test)]
#[path = "summary_tests.rs"]
mod tests;
