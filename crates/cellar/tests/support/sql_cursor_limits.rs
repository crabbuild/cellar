// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The same cursor admission/provenance oracle for native and Raft storage.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    storage::{SqlCursorRefusal, SqlValue},
    SqlLimits, Storage,
};

pub(super) fn check(storage: &Storage, mutate: bool) -> Result<()> {
    if mutate {
        storage.sql(
            "CREATE TABLE IF NOT EXISTS cursor_limit_probe(n INTEGER PRIMARY KEY);
             INSERT OR IGNORE INTO cursor_limit_probe VALUES(1),(2)",
            &[],
        )?;
    }
    let limits = SqlLimits::default();
    let mut held = Vec::new();
    for _ in 0..limits.max_open_cursors {
        held.push(storage.sql_cursor("SELECT n FROM cursor_limit_probe ORDER BY n", &[])?);
    }
    let error = storage
        .sql_cursor("SELECT n FROM cursor_limit_probe", &[])
        .err()
        .context("cursor count is bounded")?;
    ensure!(error.downcast_ref::<SqlCursorRefusal>() == Some(&SqlCursorRefusal::CountLimit));
    ensure!(error.to_string().contains("cursor count"));
    if mutate {
        let error = storage
            .sql_cursor(
                "INSERT INTO cursor_limit_probe VALUES(99); SELECT n FROM cursor_limit_probe",
                &[],
            )
            .err()
            .context("capacity must refuse before a mutating prefix")?;
        ensure!(error.downcast_ref::<SqlCursorRefusal>() == Some(&SqlCursorRefusal::CountLimit));
        ensure!(error.to_string().contains("cursor count"));
    }
    drop(held);
    ensure!(
        storage
            .sql_cursor("SELECT n FROM cursor_limit_probe ORDER BY n", &[])?
            .to_raw_array()?
            == vec![vec![SqlValue::Integer(1)], vec![SqlValue::Integer(2)]]
    );

    let prefix = if mutate {
        "INSERT INTO cursor_limit_probe VALUES(99); SELECT 1"
    } else {
        "SELECT 1"
    };
    let oversized = format!("{prefix}{}", " ".repeat(limits.max_cursor_bytes));
    let error = storage
        .sql_cursor(&oversized, &[])
        .err()
        .context("SQL text must refuse before preparing its prefix")?;
    ensure!(
        error.downcast_ref::<SqlCursorRefusal>() == Some(&SqlCursorRefusal::StatementByteLimit)
    );
    ensure!(error.to_string().contains("byte limit"));
    ensure!(
        storage
            .sql("SELECT n FROM cursor_limit_probe ORDER BY n", &[])?
            .1
            == vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]]
    );

    // Binding/statement accounting can fail after a preceding statement ran.
    // Identical quota text is not proof that the complete SQL request refused.
    let error = storage
        .sql_cursor(
            "SELECT ?1",
            &[SqlValue::Blob(vec![0; limits.max_cursor_bytes])],
        )
        .err()
        .context("bound cursor VM/bind memory")?;
    ensure!(error.to_string().contains("byte limit"));
    ensure!(!error.is::<SqlCursorRefusal>());
    if mutate {
        let error = storage
            .sql_cursor(
                "INSERT INTO cursor_limit_probe VALUES(99); SELECT ?1",
                &[SqlValue::Blob(vec![0; limits.max_cursor_bytes])],
            )
            .err()
            .context("bound statement/bind memory after an executed prefix")?;
        ensure!(error.to_string().contains("byte limit"));
        ensure!(!error.is::<SqlCursorRefusal>());
        ensure!(
            storage
                .sql("SELECT n FROM cursor_limit_probe WHERE n=99", &[])?
                .1
                == vec![vec![serde_json::json!(99)]]
        );
        storage.sql("DELETE FROM cursor_limit_probe WHERE n=99", &[])?;
    }
    Ok(())
}
