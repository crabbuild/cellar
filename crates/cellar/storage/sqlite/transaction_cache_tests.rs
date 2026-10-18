use super::*;

#[test]
fn storage_transactions_do_not_expire_prepared_queries() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:transactions";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    put_json(scope, "count", &serde_json::json!(0))?;
    with(scope, |connection| -> anyhow::Result<()> {
        let mut statement = connection.prepare_cached(KV_GET_SQL)?;
        for value in 0..10 {
            transaction_control(scope, "start", false, "").map_err(anyhow::Error::msg)?;
            assert_eq!(
                statement.query_row([scope, "count"], |row| row.get::<_, String>(0))?,
                value.to_string()
            );
            connection
                .prepare_cached(KV_PUT_SQL)?
                .execute(rusqlite::params![scope, "count", (value + 1).to_string()])?;
            transaction_control(scope, "commit", false, "").map_err(anyhow::Error::msg)?;
        }
        assert_eq!(
            statement.get_status(rusqlite::StatementStatus::RePrepare),
            0,
            "transaction boundaries expired a warm query"
        );
        Ok(())
    })
    .unwrap()?;
    close(scope);
    Ok(())
}

#[test]
fn transaction_privilege_never_authorizes_application_sql() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:authority";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    // Prime the host's cached BEGIN/COMMIT before trying the same text through
    // the application's separate SQL statement cache.
    transaction_control(scope, "start", false, "").map_err(anyhow::Error::msg)?;
    transaction_control(scope, "commit", false, "").map_err(anyhow::Error::msg)?;
    for sql in [
        "BEGIN IMMEDIATE",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT cells_tx_1",
        "RELEASE cells_tx_1",
        "ATTACH ':memory:' AS other",
        "PRAGMA synchronous=OFF",
        "SELECT * FROM __sys_cell_metadata",
    ] {
        for _ in 0..2 {
            assert!(sql_exec(scope, sql, &[]).is_err(), "accepted {sql}");
            assert!(sql_cursor_start(scope, sql, &[]).is_err(), "cached {sql}");
            as_storage_transaction(|| {
                assert!(sql_exec(scope, sql, &[]).is_err(), "privileged {sql}");
                assert!(
                    sql_cursor_start(scope, sql, &[]).is_err(),
                    "privileged cached {sql}"
                );
            });
        }
    }
    let panic = std::panic::catch_unwind(|| {
        as_storage_transaction(|| panic!("transaction control interrupted"));
    });
    assert!(panic.is_err());
    assert!(!STORAGE_TRANSACTION.with(std::cell::Cell::get));
    assert!(with(scope, |connection| connection.execute_batch("BEGIN"))
        .unwrap()
        .is_err());
    close(scope);
    Ok(())
}
