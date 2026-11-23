// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Raft reopens this exact schema on every private Queue command image.

use rusqlite::Connection;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SCHEMA: &str = include_str!("schema.sql");

fn reopen_steps(connection: &Connection) -> rusqlite::Result<usize> {
    let steps = Arc::new(AtomicUsize::new(0));
    let observed = steps.clone();
    connection.progress_handler(
        1,
        Some(move || {
            observed.fetch_add(1, Ordering::Relaxed);
            false
        }),
    )?;
    let result = connection.execute_batch(SCHEMA);
    connection.progress_handler(0, None::<fn() -> bool>)?;
    result?;
    Ok(steps.load(Ordering::Relaxed))
}

fn stats(connection: &Connection) -> rusqlite::Result<(i64, i64, i64, Option<i64>)> {
    connection.query_row(
        "SELECT stored_count, backlog_count, backlog_bytes, oldest_message_timestamp
         FROM __sys_queue_stats WHERE id = 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )
}

#[test]
fn repeated_schema_open_does_not_scan_existing_backlog() -> rusqlite::Result<()> {
    let mut connection = Connection::open_in_memory()?;
    connection.execute_batch(SCHEMA)?;
    let empty_steps = reopen_steps(&connection)?;
    for (previous, count) in [(0, 512), (512, 4096)] {
        let transaction = connection.transaction()?;
        for id in previous..count {
            transaction.execute(
                "INSERT INTO __sys_queue_messages
                 (id, body, content_type, size, enqueued_at, visible_at)
                 VALUES (?1, X'00', 'application/octet-stream', 1, ?2, ?2)",
                rusqlite::params![format!("schema-message-{id}"), id],
            )?;
        }
        transaction.commit()?;
        let before = stats(&connection)?;
        assert_eq!(before, (count, count, count, Some(0)));
        let populated_steps = reopen_steps(&connection)?;
        assert_eq!(stats(&connection)?, before);
        assert!(
            populated_steps <= empty_steps + 256,
            "schema reopen scanned {count} existing messages: empty={empty_steps}, populated={populated_steps}"
        );
    }
    Ok(())
}

#[test]
fn missing_stats_rebuild_and_subsequent_triggers_preserve_metrics() -> rusqlite::Result<()> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch(SCHEMA)?;
    connection.execute_batch(
        "INSERT INTO __sys_queue_messages
         (id, body, content_type, size, enqueued_at, visible_at, purge_on_settle)
         VALUES ('ready', X'010203', 'application/octet-stream', 3, 100, 100, 0),
                ('leased', X'0102030405', 'application/octet-stream', 5, 200, 200, 0),
                ('purged', X'01020304050607', 'application/octet-stream', 7, 50, 50, 1);
         UPDATE __sys_queue_messages SET lease_id='active', leased_until=1000
         WHERE id='leased';
         DELETE FROM __sys_queue_stats;",
    )?;
    connection.execute_batch(SCHEMA)?;
    assert_eq!(stats(&connection)?, (3, 2, 8, Some(100)));
    connection.execute_batch(
        "DELETE FROM __sys_queue_messages WHERE id='ready';
         UPDATE __sys_queue_messages SET purge_on_settle=0 WHERE id='purged';",
    )?;
    connection.execute_batch(SCHEMA)?;
    assert_eq!(stats(&connection)?, (2, 2, 12, Some(50)));
    connection.execute_batch("UPDATE __sys_queue_messages SET purge_on_settle=1")?;
    connection.execute_batch(SCHEMA)?;
    assert_eq!(stats(&connection)?, (2, 0, 0, None));
    connection.execute_batch("DELETE FROM __sys_queue_messages")?;
    connection.execute_batch(SCHEMA)?;
    assert_eq!(stats(&connection)?, (0, 0, 0, None));
    Ok(())
}
