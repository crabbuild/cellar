// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Wake installation identity, committed atomically with alarms and internal
//! work. The application alarm remains independent of the effective deadline.
use cellar_logic::wake::{AlarmSnapshot, AlarmSource, PublicationId};
use rusqlite::{params, Connection, OptionalExtension as _};

/// Development layouts cut over without replacing a retained wake projection.
/// Run before base-schema DDL, pragmas or ownership-epoch changes.
pub(super) fn check_schema(c: &Connection) -> anyhow::Result<()> {
    let previous: bool = c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='trigger' AND name IN (
           '__sys_cell_wake_insert', '__sys_cell_wake_update', '__sys_cell_wake_delete'
         ))",
        [],
        |row| row.get(0),
    )?;
    super::event_schema::compatible(
        !previous,
        "previous alarm-only wake projection; recreate development state",
    )
}

/// The ownership protocol advances the epoch before a restored image can
/// write. A same-epoch reopen retains this row, including consumed tombstones.
/// Resetting its sequence would let an old DELETE name a new installation.
/// A new epoch also preserves the sequence: zero then means this image has no
/// alarm history, so an empty cell needs retirement only when a scan finds it.
pub(crate) fn initialize(c: &Connection, scope: &str, epoch: u64) -> anyhow::Result<()> {
    anyhow::ensure!(epoch > 0, "wake writer epoch must be positive");
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS __sys_cell_wake (
           scope TEXT PRIMARY KEY, epoch TEXT NOT NULL CHECK(length(epoch)=16),
           sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>=0),
           at_ms INTEGER
         );",
    )?;
    let epoch = format!("{epoch:016x}");
    let prior: Option<String> = c
        .query_row(
            "SELECT epoch FROM __sys_cell_wake WHERE scope=?1",
            [scope],
            |row| row.get(0),
        )
        .optional()?;
    anyhow::ensure!(
        prior.as_ref().is_none_or(|prior| prior <= &epoch),
        "wake writer epoch cannot move backwards"
    );
    // Initialize a new scope without changing its alarm deadline or retries.
    // Preserve every existing publication sequence, including tombstones;
    // rebuilding history would reuse keys that readers already retired.
    c.execute_batch("SAVEPOINT cellar_wake_open;")?;
    let result = (|| -> anyhow::Result<()> {
        c.execute(
            "INSERT INTO __sys_cell_wake(scope,epoch,sequence,at_ms)
             VALUES(?1,?2,0,(SELECT min(at_ms) FROM (
                 SELECT at_ms FROM __sys_cell_alarm WHERE scope=?1
                 UNION ALL SELECT at_ms FROM __sys_work WHERE scope=?1
             )))
             ON CONFLICT(scope) DO UPDATE SET epoch=excluded.epoch
               WHERE excluded.epoch>__sys_cell_wake.epoch",
            params![scope, epoch],
        )?;
        // Triggers also cover handler consumption, retry, and transaction
        // rollback. Callback bookkeeping cannot make identity atomic with SQL.
        // Work tombstones preserve generations and all changes advance
        // publication identity, even when another reason keeps the deadline.
        for (table, prefix, update_columns) in [
            ("__sys_cell_alarm", "alarm", "at_ms,generation"),
            ("__sys_work", "work", "at_ms,generation"),
        ] {
            for (event, source) in [("insert", "NEW"), ("update", "NEW"), ("delete", "OLD")] {
                let operation = if event == "update" {
                    format!("UPDATE OF {update_columns}")
                } else {
                    event.to_uppercase()
                };
                c.execute_batch(&format!(
                    "CREATE TRIGGER IF NOT EXISTS __sys_wake_{prefix}_{event}
                     AFTER {operation} ON {table} BEGIN
                       UPDATE __sys_cell_wake SET sequence=sequence+1,
                         at_ms=(SELECT min(at_ms) FROM (
                           SELECT at_ms FROM __sys_cell_alarm WHERE scope={source}.scope
                           UNION ALL
                           SELECT at_ms FROM __sys_work WHERE scope={source}.scope
                         ))
                       WHERE scope={source}.scope;
                     END;"
                ))?;
            }
        }
        let consistent: bool = c.query_row(
            "SELECT at_ms IS (SELECT min(at_ms) FROM (
               SELECT at_ms FROM __sys_cell_alarm WHERE scope=?1
               UNION ALL SELECT at_ms FROM __sys_work WHERE scope=?1
             ))
             FROM __sys_cell_wake WHERE scope=?1",
            [scope],
            |row| row.get(0),
        )?;
        anyhow::ensure!(
            consistent,
            "durable deadlines and wake installation disagree"
        );
        Ok(())
    })();
    if result.is_err() {
        c.execute_batch("ROLLBACK TO cellar_wake_open;")?;
    }
    c.execute_batch("RELEASE cellar_wake_open;")?;
    result
}

/// Call while holding the connection's source lock. An open transaction has
/// no publication identity yet: its sequence can still be rolled back.
pub(crate) fn snapshot(
    c: &Connection,
    scope: &str,
    position: u64,
) -> anyhow::Result<AlarmSnapshot> {
    anyhow::ensure!(
        c.is_autocommit(),
        "wake snapshot requires a committed transaction"
    );
    // SQLite keeps autocommit enabled while a RETURNING cursor holds an
    // implicit write open. Publishing that sequence before rollback would
    // let the next installation reuse an already published key.
    super::ensure_no_unfinished_write_cursor(c)?;
    let (epoch, sequence, at_ms): (String, u64, Option<i64>) = c
        .prepare_cached("SELECT epoch,sequence,at_ms FROM __sys_cell_wake WHERE scope=?1")?
        .query_row([scope], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    let epoch = u64::from_str_radix(&epoch, 16)?;
    anyhow::ensure!(epoch > 0, "invalid wake writer epoch");
    Ok(AlarmSnapshot::committed(
        at_ms,
        AlarmSource {
            id: PublicationId { epoch, sequence },
            position,
        },
    ))
}
