// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn exists(connection: &Connection, name: &str) -> bool {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
            [name],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn erasure_retains_protocol_state_and_restores_both_foreign_key_policies() -> anyhow::Result<()> {
    for enabled in [false, true] {
        let connection = Connection::open_in_memory()?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, enabled)?;
        connection.execute_batch(
            "CREATE TABLE __sys_raft_clients(v); INSERT INTO __sys_raft_clients VALUES(7);
             CREATE TABLE __sys_raft_receipts(v); INSERT INTO __sys_raft_receipts VALUES(9);
             CREATE TABLE __sys_cell_alarm(scope PRIMARY KEY,at_ms);
             INSERT INTO __sys_cell_alarm VALUES('cell',123);
             CREATE TABLE \"quoted\"\"parent\"(v PRIMARY KEY,child REFERENCES child(v) DEFERRABLE INITIALLY DEFERRED);
             CREATE TABLE child(v PRIMARY KEY,parent REFERENCES \"quoted\"\"parent\"(v));
             BEGIN IMMEDIATE;
             INSERT INTO \"quoted\"\"parent\" VALUES(1,1); INSERT INTO child VALUES(1,1);"
        )?;
        erase(&connection, "cell", false)?;
        assert!(!exists(&connection, "quoted\"parent"));
        assert!(!exists(&connection, "child"));
        assert_eq!(
            connection.query_row("SELECT v FROM __sys_raft_clients", [], |row| row
                .get::<_, i64>(0))?,
            7
        );
        assert_eq!(
            connection.query_row("SELECT v FROM __sys_raft_receipts", [], |row| row
                .get::<_, i64>(0))?,
            9
        );
        assert_eq!(
            connection.query_row("SELECT at_ms FROM __sys_cell_alarm", [], |row| row
                .get::<_, i64>(0))?,
            123
        );
        assert!(exists(&connection, "__sys_cell_kv"));
        assert_eq!(
            connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?,
            enabled
        );
        assert!(!connection.is_autocommit());
        erase(&connection, "cell", true)?;
        assert_eq!(
            connection.query_row("SELECT COUNT(*) FROM __sys_cell_alarm", [], |row| row
                .get::<_, i64>(0))?,
            0
        );
        connection.execute_batch("COMMIT")?;
        connection.execute_batch(
            "CREATE TABLE parent(v PRIMARY KEY); CREATE TABLE child(v REFERENCES parent(v))",
        )?;
        assert_eq!(
            connection
                .execute("INSERT INTO child VALUES(1)", [])
                .is_err(),
            enabled
        );
    }
    Ok(())
}

#[test]
fn failed_drop_rolls_back_prior_drops_and_restores_foreign_keys() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)?;
    connection.execute_batch(
        "CREATE TABLE first(v); INSERT INTO first VALUES(7);
         CREATE TABLE held(v); INSERT INTO held VALUES(1),(2),(3); BEGIN IMMEDIATE",
    )?;
    let mut statement = connection.prepare("SELECT v FROM held")?;
    let mut rows = statement.query([])?;
    assert!(rows.next()?.is_some());
    assert!(erase(&connection, "cell", true).is_err());
    assert!(exists(&connection, "first"));
    assert!(exists(&connection, "held"));
    assert!(connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?);
    drop(rows);
    drop(statement);
    assert_eq!(
        connection.query_row("SELECT v FROM first", [], |row| row.get::<_, i64>(0))?,
        7
    );
    erase(&connection, "cell", true)?;
    connection.execute_batch("COMMIT")?;
    Ok(())
}

#[test]
fn erasure_refuses_pending_delivery_and_does_not_clear_deferred_constraint_failures(
) -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)?;
    connection.execute_batch(
        "CREATE TABLE business(v); INSERT INTO business VALUES(7);
         CREATE TABLE __sys_outbox(v); INSERT INTO __sys_outbox VALUES(1);
         CREATE TABLE parent(v PRIMARY KEY);
         CREATE TABLE child(v REFERENCES parent(v) DEFERRABLE INITIALLY DEFERRED);
         BEGIN IMMEDIATE; INSERT INTO child VALUES(1)",
    )?;
    assert!(erase(&connection, "cell", true)
        .unwrap_err()
        .to_string()
        .contains("PendingDelivery"));
    assert!(exists(&connection, "business"));
    connection.execute("DELETE FROM __sys_outbox", [])?;
    erase(&connection, "cell", true)?;
    assert!(connection.execute_batch("COMMIT").is_err());
    connection.execute_batch("ROLLBACK")?;
    assert!(exists(&connection, "business"));
    Ok(())
}
