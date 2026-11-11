// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The same lazy SQLite rollback/deferred-error oracle for native and Raft turns.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{storage::SqlValue, Storage};

pub(super) fn check(storage: &Storage) -> Result<()> {
    storage.sql(
        "CREATE TABLE IF NOT EXISTS cursor_rollback(n INTEGER PRIMARY KEY);
         INSERT OR IGNORE INTO cursor_rollback VALUES(1),(2),(3);",
        &[],
    )?;
    let mut cursor = storage.sql_cursor("SELECT n FROM cursor_rollback ORDER BY n", &[])?;
    ensure!(cursor.next_raw()? == Some(vec![SqlValue::Integer(1)]));
    let rolled_back: Result<()> = storage.transaction_sync(|tx| {
        tx.sql("CREATE TABLE cursor_rollback_schema(n INTEGER)", &[])?;
        tx.sql("UPDATE cursor_rollback SET n=30 WHERE n=3", &[])?;
        anyhow::bail!("deliberate cursor rollback");
    });
    ensure!(rolled_back.is_err());
    // Keep the already-prefetched row. SQLite invalidates the following step
    // after schema rollback, and the public iterator defers that error by one call.
    ensure!(cursor.next_raw()? == Some(vec![SqlValue::Integer(2)]));
    let error = cursor
        .next_raw()
        .err()
        .context("rollback must invalidate the live VM")?;
    ensure!(error.to_string().contains("abort due to ROLLBACK"));
    ensure!(cursor.next_raw()?.is_none());
    ensure!(cursor.rows_read() == 2 && cursor.rows_written() == 0);
    ensure!(
        storage
            .sql("SELECT n FROM cursor_rollback ORDER BY n", &[])?
            .1
            == vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(3)]
            ]
    );
    Ok(())
}
