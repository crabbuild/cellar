// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the real SQLite callback ABI without pausing the unit-test process.
use super::*;
use rusqlite::{params, Connection, OptionalExtension};

const SCOPE: &str = "PythonCounter:trace'🙂";
const COUNT: &str = "9007199254740993";

fn fixture() -> Result<(Connection, Arc<Gate>, Arc<Watch>)> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(
        "CREATE TABLE __sys_cell_kv(scope TEXT,k TEXT,v TEXT,PRIMARY KEY(scope,k))",
    )?;
    // SAFETY: the connection owns this live handle and outlives the callbacks.
    ensure!(
        unsafe { attach(connection.handle(), std::ptr::null_mut(), std::ptr::null()) }
            == ffi::SQLITE_OK
    );
    let gate = Arc::new(Gate::default());
    let evidence = json!({"ready":true,"count":COUNT,"receipt":null,"transaction_open":true});
    let mut watch = Watch::build(gate.clone(), evidence, SCOPE, "op", COUNT);
    watch.observe_only = true;
    Ok((connection, gate, Arc::new(watch)))
}

fn receipt(connection: &Connection) -> Result<Option<String>> {
    Ok(connection
        .query_row(READ, params![SCOPE, "receipt:op"], |row| row.get(0))
        .optional()?)
}

fn count(connection: &Connection) -> Result<()> {
    connection.execute(PUT, params![SCOPE, "count", json!(COUNT).to_string()])?;
    Ok(())
}

#[test]
fn sqlite_checkpoint_observes_the_completed_write_and_follows_original_turns() -> Result<()> {
    let (connection, gate, watch) = fixture()?;
    // A statement outside this invocation's poll must not arm its observation.
    assert!(receipt(&connection)?.is_none());
    assert_eq!(gate.snapshot(), json!({"ready":false}));
    connection.execute_batch("BEGIN")?;
    {
        let _turn = Turn::enter(Some(watch.clone()));
        assert!(receipt(&connection)?.is_none());
    }
    assert!(CURRENT.with(|current| current.borrow().is_none()));
    // Native polls can migrate to another worker after an await. The state
    // follows the invocation, while the SQLite connection still has one owner.
    let evidence = std::thread::spawn(move || -> Result<Value> {
        let _turn = Turn::enter(Some(watch));
        count(&connection)?;
        let evidence = gate.snapshot();
        assert_eq!(evidence["ready"], true);
        assert!(!connection.is_autocommit());
        assert!(receipt(&connection)?.is_none());
        let actual: String =
            connection.query_row(READ, params![SCOPE, "count"], |row| row.get(0))?;
        assert_eq!(serde_json::from_str::<Value>(&actual)?, json!(COUNT));
        connection.execute_batch("ROLLBACK")?;
        assert!(connection
            .query_row(READ, params![SCOPE, "count"], |row| row.get::<_, String>(0))
            .optional()?
            .is_none());
        Ok(evidence)
    })
    .join()
    .expect("SQLite observer worker panicked")?;
    assert_eq!(
        evidence["sqlite_checkpoint"],
        json!({"statement_finished":true,
        "receipt_read_rows":0,"count_changes":1,"total_changes_before":0,"total_changes_after":1})
    );
    Ok(())
}

#[test]
fn sqlite_checkpoint_refuses_a_present_or_unobserved_receipt() -> Result<()> {
    for existing in [false, true] {
        let (connection, gate, watch) = fixture()?;
        if existing {
            connection.execute(PUT, params![SCOPE, "receipt:op", "{}"])?;
        }
        connection.execute_batch("BEGIN")?;
        let _turn = Turn::enter(Some(watch));
        if existing {
            assert!(receipt(&connection)?.is_some());
        }
        count(&connection)?;
        let evidence = gate.snapshot();
        assert_eq!(evidence["ready"], false);
        assert_eq!(evidence["observer_failed"], true);
        connection.execute_batch("ROLLBACK")?;
    }
    Ok(())
}

#[test]
fn sqlite_checkpoint_refuses_an_autocommit_write() -> Result<()> {
    let (connection, gate, watch) = fixture()?;
    let _turn = Turn::enter(Some(watch));
    assert!(receipt(&connection)?.is_none());
    count(&connection)?;
    assert_eq!(gate.snapshot()["ready"], false);
    assert_eq!(gate.snapshot()["observer_failed"], true);
    Ok(())
}
