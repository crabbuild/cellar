// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar::sdk::storage::{SqlCursor, SqlValue};

pub(super) fn exercise(storage: &Storage, mutate: bool) -> Result<()> {
    let query = if mutate {
        "CREATE TABLE IF NOT EXISTS cursor_items(n INTEGER PRIMARY KEY,b BLOB);
         INSERT INTO cursor_items VALUES(1,x'ff'),(2,x'00') ON CONFLICT(n) DO UPDATE SET b=excluded.b;
         SELECT n,b,9e999 AS inf FROM cursor_items WHERE n IN (1,2) ORDER BY n"
    } else {
        "SELECT n,b,9e999 AS inf FROM cursor_items WHERE n IN (1,2) ORDER BY n"
    };
    let mut cursor = storage.sql_cursor(query, &[])?;
    ensure!(cursor.column_names() == ["n", "b", "inf"]);
    ensure!(cursor.rows_read() == 1 && cursor.rows_written() == 0);
    let row = cursor.next().context("first SQL cursor row")??;
    ensure!(row["n"] == SqlValue::Integer(1) && row["b"] == SqlValue::Blob(vec![255]));
    ensure!(matches!(row["inf"], SqlValue::Real(value) if value.is_infinite()));
    ensure!(cursor.rows_read() == 2);
    ensure!(cursor.next().context("second SQL cursor row")??["n"] == SqlValue::Integer(2));
    ensure!(cursor.next().is_none());
    ensure!(cursor.rows_written() == if mutate { 2 } else { 0 });
    let blob = storage
        .sql_cursor(
            "SELECT ?1 AS b,?2 AS n",
            &[SqlValue::Blob(vec![0, 255]), SqlValue::Integer(7)],
        )?
        .one()?;
    ensure!(blob["b"] == SqlValue::Blob(vec![0, 255]) && blob["n"] == SqlValue::Integer(7));
    let duplicate = storage
        .sql_cursor("SELECT 1 AS duplicate,2 AS duplicate", &[])?
        .one()?;
    ensure!(duplicate["duplicate"] == SqlValue::Integer(2));
    ensure!(storage.sql_cursor("SELECT 1 WHERE 0", &[])?.one().is_err());
    ensure!(storage
        .sql_cursor("SELECT n FROM cursor_items", &[])?
        .one()
        .is_err());
    ensure!(storage.sql_cursor("SELECT ?", &[]).is_err());
    ensure!(storage
        .sql_cursor("SELECT * FROM __sys_cell_kv", &[])
        .is_err());
    ensure!(storage.sql_cursor("COMMIT", &[]).is_err());
    let mut closed = storage.sql_cursor("SELECT n FROM cursor_items", &[])?;
    ensure!(closed.buffered_bytes() > 0);
    closed.close()?;
    ensure!(closed.next_raw()?.is_none());

    if mutate {
        let cursor = storage
            .transaction_sync(|tx| tx.sql_cursor("SELECT n FROM cursor_items ORDER BY n", &[]))?;
        ensure!(
            cursor.to_raw_array()? == vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
        );
        let returning = storage.sql_cursor(
            "INSERT INTO cursor_items VALUES(3,x'01'),(4,x'02') RETURNING n",
            &[],
        )?;
        ensure!(returning.rows_written() == 0);
        ensure!(returning.to_raw_array()?.len() == 2);
        storage.sql("DELETE FROM cursor_items WHERE n>2", &[])?;
    } else {
        ensure!(storage
            .sql_cursor("INSERT INTO cursor_items VALUES(3,x'01') RETURNING n", &[])
            .is_err());
    }

    // Capacity refuses before entering a mutating statement. Closing tokens
    // releases admission rather than retaining a per-cell worker or result set.
    let mut held = Vec::new();
    for _ in 0..cellar::sdk::SqlLimits::default().max_open_cursors {
        held.push(storage.sql_cursor("SELECT n FROM cursor_items ORDER BY n", &[])?);
    }
    ensure!(storage
        .sql_cursor("SELECT n FROM cursor_items", &[])
        .err()
        .context("cursor count is bounded")?
        .to_string()
        .contains("cursor count"));
    if mutate {
        ensure!(storage
            .sql_cursor("INSERT INTO cursor_items VALUES(99,x'ff') RETURNING n", &[])
            .err()
            .context("capacity must refuse before mutation")?
            .to_string()
            .contains("cursor count"));
    }
    drop(held);
    ensure!(
        storage
            .sql_cursor("SELECT n FROM cursor_items", &[])?
            .to_raw_array()?
            .len()
            == 2
    );
    ensure!(storage
        .sql_cursor("SELECT ?1", &[SqlValue::Blob(vec![0; 2 * 1024 * 1024])])
        .err()
        .context("bound cursor VM/bind memory")?
        .to_string()
        .contains("byte limit"));
    Ok(())
}

pub(super) fn escaped(slot: &Mutex<Option<SqlCursor>>, storage: &Storage) -> Result<()> {
    if let Some(mut old) = slot
        .lock()
        .unwrap()
        .replace(storage.sql_cursor("SELECT value FROM counter", &[])?)
    {
        ensure!(
            old.next_raw().is_err(),
            "SQL cursor entered a different invocation/query"
        );
        ensure!(old.close().is_err());
        // Its destructor runs outside the original frame. Only its cancellation
        // flag can remain; that frame has already finalized its raw statement.
    }
    Ok(())
}
