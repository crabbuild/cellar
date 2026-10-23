//! Real SQLite failure observations, independent of the embedded interpreter.
// This disposable fixture observes files outside the execution boundary.
#![allow(clippy::disallowed_methods)]

#[path = "support/queue_observation.rs"]
mod observation;

use anyhow::Result;
use rusqlite::Connection;
use serde_json::json;
use std::path::Path;

fn fixture(root: &Path) -> Result<Connection> {
    let epoch = root.join("__Queue:fixture/ltx/e2");
    std::fs::create_dir_all(&epoch)?;
    let connection = Connection::open(epoch.join("db.sqlite"))?;
    connection.execute_batch(include_str!("../../cellar/application/queue/schema.sql"))?;
    connection.execute_batch("CREATE TABLE __sys_cell_alarm(scope TEXT PRIMARY KEY,at_ms INTEGER,retry INTEGER DEFAULT 0,counted_retry INTEGER NOT NULL DEFAULT 0,generation INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE __sys_cell_wake(scope TEXT PRIMARY KEY,epoch TEXT,sequence INTEGER,at_ms INTEGER);
        INSERT INTO __sys_cell_alarm VALUES ('fixture',21000,0,0,7);
        INSERT INTO __sys_cell_wake VALUES ('fixture','2',8,21000);")?;
    Ok(connection)
}

fn message(connection: &Connection, id: i64) -> Result<()> {
    connection.execute("INSERT INTO __sys_queue_messages(id,body,content_type,size,enqueued_at,visible_at,lease_id,lease_generation,leased_until) VALUES (?,?,'json',27,1000,1000,'lease',3,21000)",
        rusqlite::params![id.to_string(), b"PRIVATE_MESSAGE_BODY".as_slice()])?;
    Ok(())
}

#[test]
fn observes_committed_wal_lease_and_alarm_without_message_contents() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let connection = fixture(directory.path())?;
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")?;
    message(&connection, 1)?;
    let snapshot = observation::capture(directory.path());
    assert_eq!(snapshot["databases"].as_array().unwrap().len(), 1);
    let state = &snapshot["databases"][0]["state"];
    assert_eq!(state["messages"]["rows"][0]["lease_id"], "lease");
    assert_eq!(state["messages"]["rows"][0]["lease_generation"], 3);
    assert_eq!(state["messages"]["rows"][0]["leased_until"], 21000);
    assert_eq!(state["alarms"]["rows"][0]["at_ms"], 21000);
    assert_eq!(
        state["wakes"]["rows"][0],
        json!({"epoch":"2","sequence":8,"at_ms":21000})
    );
    assert_eq!(snapshot["discovery_errors"], json!([]));
    assert!(!snapshot.to_string().contains("PRIVATE_MESSAGE_BODY"));
    assert!(!snapshot
        .to_string()
        .contains(directory.path().to_str().unwrap()));
    assert_eq!(
        connection.query_row("SELECT COUNT(*) FROM __sys_queue_messages", [], |row| row
            .get::<_, i64>(
            0
        ))?,
        1
    );
    Ok(())
}

#[test]
fn missing_or_corrupt_databases_remain_errors_and_are_not_created() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let absent = directory.path().join("absent");
    assert!(observation::capture(&absent)["discovery_error"].is_string());
    assert!(!absent.exists());
    let epoch = directory.path().join("__Queue:fixture/ltx/e1");
    std::fs::create_dir_all(&epoch)?;
    assert!(observation::capture(directory.path())["databases"][0]["state"]["error"].is_string());
    assert!(!epoch.join("db.sqlite").exists());
    std::fs::write(epoch.join("db.sqlite"), b"damaged fixture")?;
    assert!(observation::capture(directory.path())["databases"][0]["state"]["error"].is_string());
    assert_eq!(std::fs::read(epoch.join("db.sqlite"))?, b"damaged fixture");
    Ok(())
}

#[test]
fn locked_database_does_not_hide_observation_failure() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let connection = fixture(directory.path())?;
    connection.execute_batch("BEGIN EXCLUSIVE")?;
    let snapshot = observation::capture(directory.path());
    assert!(snapshot["databases"][0]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("locked"));
    connection.execute_batch("ROLLBACK")?;
    assert!(
        observation::capture(directory.path())["databases"][0]["state"]
            .get("error")
            .is_none()
    );
    Ok(())
}

#[test]
fn bounds_message_rows_and_reports_omission() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let connection = fixture(directory.path())?;
    for id in 0..40 {
        message(&connection, id)?;
    }
    let snapshot = observation::capture(directory.path());
    let messages = &snapshot["databases"][0]["state"]["messages"];
    assert_eq!(messages["rows"].as_array().unwrap().len(), 32);
    assert_eq!(messages["truncated"], true);
    Ok(())
}
