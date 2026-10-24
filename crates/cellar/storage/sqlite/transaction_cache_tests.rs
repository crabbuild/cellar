// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn bulk_storage_writes_preserve_warm_statements() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:bulk-cache";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    put_json(scope, "count", &serde_json::json!(0))?;
    let read = || -> anyhow::Result<(String, i32)> {
        with(scope, |connection| -> anyhow::Result<_> {
            let mut query = connection.prepare_cached(KV_GET_SQL)?;
            let value = query.query_row([scope, "count"], |row| row.get::<_, String>(0))?;
            Ok((
                value,
                query.get_status(rusqlite::StatementStatus::RePrepare),
            ))
        })
        .unwrap()
    };
    assert_eq!(read()?.1, 0);
    for nested in [false, true] {
        if nested {
            transaction_control(scope, "start", false, "").map_err(anyhow::Error::msg)?;
        }
        for value in 1..=10 {
            put_many_json(scope, &[("count".into(), serde_json::json!(value))])?;
            put_many_serialized(scope, &[("clone".into(), vec![value])])?;
            assert_eq!(delete_many(scope, &["clone".into()])?, 1);
            let (stored, reprepares) = read()?;
            assert_eq!(stored, value.to_string());
            assert_eq!(
                reprepares, 0,
                "bulk operation expired a warm query (nested={nested})"
            );
        }
        if nested {
            transaction_control(scope, "rollback", false, "").map_err(anyhow::Error::msg)?;
        }
    }
    close(scope);
    Ok(())
}

#[test]
fn allocation_observations_preserve_warm_statements_and_sql_restrictions() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:allocation-cache";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    put_json(scope, "count", &serde_json::json!(42))?;
    with(scope, |connection| -> anyhow::Result<()> {
        let mut query = connection.prepare_cached(KV_GET_SQL)?;
        for _ in 0..10 {
            let (allocated, used) = sql_page_usage(scope).map_err(anyhow::Error::msg)?;
            assert!(allocated >= used && used > 0);
            assert_eq!(sql_database_size(scope).map_err(anyhow::Error::msg)?, used);
            assert_eq!(connection_database_size(connection), used);
            assert!(sql_schema_version(connection) > 0);
            assert_eq!(
                query.query_row([scope, "count"], |row| row.get::<_, String>(0))?,
                "42"
            );
        }
        assert_eq!(query.get_status(rusqlite::StatementStatus::RePrepare), 0);
        Ok(())
    })
    .unwrap()?;
    for query in [
        "PRAGMA page_count",
        "PRAGMA freelist_count",
        "PRAGMA page_size",
        "PRAGMA page_size=8192",
        "SELECT * FROM pragma_page_count()",
        "SELECT * FROM pragma_freelist_count()",
        "SELECT * FROM pragma_page_size()",
    ] {
        assert!(sql_exec(scope, query, &[]).is_err(), "allowed {query}");
        assert!(
            sql_cursor_start(scope, query, &[]).is_err(),
            "cached {query}"
        );
        assert!(
            sql_run(
                scope,
                &SqlRequest::Prepared {
                    sql: query.into(),
                    params: Vec::new(),
                    first: false,
                }
            )
            .is_err(),
            "SQL binding allowed {query}"
        );
    }
    let request = SqlRequest::Batch {
        statements: vec![
            SqlPreparedRequest {
                sql: "SELECT 42".into(),
                params: Vec::new(),
            },
            SqlPreparedRequest {
                sql: "SELECT * FROM __sys_cell_metadata".into(),
                params: Vec::new(),
            },
        ],
    };
    assert!(
        sql_run(scope, &request).is_err(),
        "metadata accounting leaked host authority to the next statement"
    );
    assert!(with(scope, Connection::is_autocommit).unwrap());
    assert!(!USER_SQL.with(std::cell::Cell::get));
    close(scope);
    Ok(())
}

#[test]
fn bulk_failures_rollback_only_their_atomic_boundary() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:bulk-rollback";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    with(scope, |connection| {
        connection.execute_batch(
            "CREATE TRIGGER reject_bulk_insert BEFORE INSERT ON __sys_cell_kv
         WHEN NEW.k='reject' BEGIN SELECT RAISE(ABORT,'rejected bulk insert'); END;
         CREATE TRIGGER reject_bulk_delete BEFORE DELETE ON __sys_cell_kv
         WHEN OLD.k='delete-reject' BEGIN SELECT RAISE(ABORT,'rejected bulk delete'); END;",
        )
    })
    .unwrap()?;
    for nested in [false, true] {
        if nested {
            transaction_control(scope, "start", false, "").map_err(anyhow::Error::msg)?;
            put_json(scope, "outer", &serde_json::json!(42))?;
        }
        assert!(put_many_json(
            scope,
            &[
                ("partial-json".into(), serde_json::json!(1)),
                ("reject".into(), serde_json::json!(2))
            ]
        )
        .is_err());
        assert!(get_stored(scope, "partial-json")?.is_none());
        assert!(put_many_serialized(
            scope,
            &[
                ("partial-clone".into(), vec![1]),
                ("reject".into(), vec![2])
            ]
        )
        .is_err());
        assert!(get_stored(scope, "partial-clone")?.is_none());
        put_many_json(
            scope,
            &[
                ("delete-first".into(), serde_json::json!(1)),
                ("delete-reject".into(), serde_json::json!(2)),
            ],
        )?;
        assert!(delete_many(scope, &["delete-first".into(), "delete-reject".into()]).is_err());
        assert!(get_stored(scope, "delete-first")?.is_some());
        assert!(get_stored(scope, "delete-reject")?.is_some());
        if nested {
            assert!(get_stored(scope, "outer")?.is_some());
            transaction_control(scope, "commit", false, "").map_err(anyhow::Error::msg)?;
        }
        assert!(with(scope, Connection::is_autocommit).unwrap());
        assert!(sql_critical_error(scope).is_none());
    }
    assert!(get_stored(scope, "outer")?.is_some());
    close(scope);
    Ok(())
}

#[test]
fn storage_observations_never_reserve_the_writer() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cell.sqlite");
    let scope = "Counter:reader";
    open(scope, path.to_str().unwrap())?;
    put_many_json(
        scope,
        &[
            ("a".into(), serde_json::json!(1)),
            ("b".into(), serde_json::json!(2)),
        ],
    )?;
    let position = write_position(scope)?;
    let writer = Connection::open(&path)?;
    writer.execute_batch("BEGIN IMMEDIATE")?;
    with(scope, |connection| {
        connection.busy_timeout(std::time::Duration::ZERO)
    })
    .unwrap()?;
    let read = (|| -> anyhow::Result<()> {
        assert!(get_stored(scope, "a")?.is_some());
        assert_eq!(get_many_stored(scope, &["a".into(), "b".into()])?.len(), 2);
        assert!(sql_database_size(scope).map_err(anyhow::Error::msg)? > 0);
        assert!(sql_page_usage(scope).map_err(anyhow::Error::msg)?.0 > 0);
        assert_eq!(write_position(scope)?, position);
        assert!(with(scope, Connection::is_autocommit).unwrap());
        Ok(())
    })();
    writer.execute_batch("ROLLBACK")?;
    read?;
    close(scope);
    Ok(())
}

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

#[test]
fn batch_unwinding_rolls_back_without_leaking_authority() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let directory = tempfile::tempdir()?;
    let scope = "Counter:batch-panic";
    open(
        scope,
        directory.path().join("cell.sqlite").to_str().unwrap(),
    )?;
    for nested in [false, true] {
        if nested {
            transaction_control(scope, "start", false, "").map_err(anyhow::Error::msg)?;
            put_json(scope, "outer", &serde_json::json!(42))?;
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_mut(scope, |connection| {
                with_storage_batch(connection, |connection| -> anyhow::Result<()> {
                    connection
                        .prepare_cached(KV_PUT_SQL)?
                        .execute([scope, "partial", "1"])?;
                    panic!("interrupt framework batch");
                })
            })
        }));
        assert!(result.is_err());
        assert!(!STORAGE_TRANSACTION.with(std::cell::Cell::get));
        assert!(get_stored(scope, "partial")?.is_none());
        assert!(sql_exec(scope, "SAVEPOINT unauthorized", &[]).is_err());
        if nested {
            assert!(get_stored(scope, "outer")?.is_some());
            transaction_control(scope, "commit", false, "").map_err(anyhow::Error::msg)?;
        }
        assert!(with(scope, Connection::is_autocommit).unwrap());
    }
    as_user_sql(|| {
        let interrupted =
            std::panic::catch_unwind(|| as_framework_sql(|| panic!("metadata interruption")));
        assert!(interrupted.is_err());
        assert!(USER_SQL.with(std::cell::Cell::get));
    });
    assert!(!USER_SQL.with(std::cell::Cell::get));
    close(scope);
    Ok(())
}
