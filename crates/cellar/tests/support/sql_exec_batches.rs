// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The ordinary Storage SQL batch contract in both native and Raft turns.

use anyhow::{ensure, Result};
use cellar::sdk::Storage;
use serde_json::json;

pub(super) fn check(storage: &Storage) -> Result<()> {
    let (columns, rows, written) = storage.sql(
        "CREATE TABLE batch_probe(n INTEGER); INSERT INTO batch_probe VALUES(7)",
        &[],
    )?;
    ensure!(columns.is_empty() && rows.is_empty() && written == 1);
    ensure!(storage.sql("SELECT n FROM batch_probe", &[])?.1 == vec![vec![json!(7)]]);
    for sql in [
        "SELECT ';' AS token",
        "SELECT ';' AS token; -- trailing ; trivia ;",
    ] {
        let (columns, rows, written) = storage.sql(sql, &[])?;
        ensure!(columns == ["token"] && rows == vec![vec![json!(";")]] && written == 0);
    }
    let (columns, rows, _) = storage.sql("SELECT ?1 AS n; -- parameter ;", &[json!(9)])?;
    ensure!(columns == ["n"] && rows == vec![vec![json!(9)]]);
    ensure!(storage.sql("SELECT ?1; SELECT 2", &[json!(9)]).is_err());
    storage.sql("DROP TABLE batch_probe", &[])?;
    Ok(())
}
