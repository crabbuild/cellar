// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Erase business state inside the current private Raft transaction.

use super::*;
use rusqlite::config::DbConfig;

pub(super) fn raft_delete_all(scope: &str, delete_alarm: bool) -> anyhow::Result<()> {
    with_application_storage_write(scope, |connection| {
        as_framework_sql(|| erase(connection, scope, delete_alarm))
    })
}

fn retained(table: &str) -> bool {
    matches!(
        table,
        "__sys_cell_metadata"
            | "__sys_cell_facet_names"
            | "__sys_cell_wake"
            | "__sys_cell_alarm"
            | "__sys_work"
            | "__sys_event_source"
    ) || [
        "__sys_fs_",
        "__sys_outbox",
        "__sys_inbox",
        "__sys_container_bind",
        "__sys_sandbox_",
        "__sys_stream",
        "__sys_event_catalog",
    ]
    .iter()
    .any(|prefix| table.starts_with(prefix))
        || cellar_ltx::db::is_control_table(table)
        // Raft owns this reserved namespace, including client floors, inbox
        // deduplication, named alarms, outbox frontiers and retained payloads.
        // A new protocol table must be retained even before erasure knows it.
        || table.starts_with("__sys_raft_")
}

fn erase(connection: &Connection, scope: &str, delete_alarm: bool) -> anyhow::Result<()> {
    filesystem::check_clear(connection)?;
    crate::raft::effects::outbox::check_clear(connection)?;
    let has_outbox: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_outbox')",
        [],
        |row| row.get(0),
    )?;
    if has_outbox {
        let pending: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_outbox LIMIT 1)",
            [],
            |row| row.get(0),
        )?;
        anyhow::ensure!(
            !pending,
            "PendingDelivery: cannot clear business state with pending outbox events"
        );
    }
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);

    // PRAGMA foreign_keys is a no-op inside the private transaction. Disable
    // enforcement only for framework DROP statements, restoring the original
    // policy before returning. Do not clear deferred constraint counters: an
    // already-invalid transaction must still refuse publication.
    let foreign_keys = ForeignKeys::suspend(connection)?;
    let result = as_storage_transaction(|| -> anyhow::Result<()> {
        connection.execute_batch("SAVEPOINT __sys_raft_delete_all")?;
        let result = (|| -> anyhow::Result<()> {
            for table in &tables {
                if !retained(table) {
                    let quoted = table.replace('"', "\"\"");
                    connection.execute_batch(&format!("DROP TABLE IF EXISTS \"{quoted}\""))?;
                }
            }
            if delete_alarm && tables.iter().any(|table| table == "__sys_cell_alarm") {
                connection.execute("DELETE FROM __sys_cell_alarm WHERE scope=?1", [scope])?;
            }
            connection.execute_batch(KV_SCHEMA)?;
            connection.execute_batch("RELEASE __sys_raft_delete_all")?;
            Ok(())
        })();
        if result.is_err() {
            if let Err(rollback) = connection
                .execute_batch("ROLLBACK TO __sys_raft_delete_all; RELEASE __sys_raft_delete_all")
            {
                let error = format!("Storage erasure rollback failed: {rollback}");
                command::poison(error.clone());
                return Err(anyhow::anyhow!(error));
            }
            return result;
        }
        Ok(())
    });
    foreign_keys.restore()?;
    result
}

struct ForeignKeys<'a> {
    connection: &'a Connection,
    enabled: bool,
    restored: bool,
}

impl<'a> ForeignKeys<'a> {
    fn suspend(connection: &'a Connection) -> anyhow::Result<Self> {
        let enabled = connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false)?;
        Ok(Self {
            connection,
            enabled,
            restored: false,
        })
    }

    fn restore(mut self) -> anyhow::Result<()> {
        self.restore_policy()
    }

    fn restore_policy(&mut self) -> anyhow::Result<()> {
        if !self.restored {
            if let Err(error) = self
                .connection
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.enabled)
            {
                command::poison(format!(
                    "Storage erasure foreign-key restoration failed: {error}"
                ));
                return Err(error.into());
            }
            self.restored = true;
        }
        Ok(())
    }
}

impl Drop for ForeignKeys<'_> {
    fn drop(&mut self) {
        // Also restore during unwinding; a restoration failure poisons the
        // private frame even when the application catches the original error.
        let _ = self.restore_policy();
    }
}

#[cfg(test)]
#[path = "erase_tests.rs"]
mod tests;
