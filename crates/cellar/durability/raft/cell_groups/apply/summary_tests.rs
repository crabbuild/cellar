// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn effects_are_bounded_and_inbox_receipts_do_not_schedule_work() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    assert_eq!(read(&db, 2).unwrap(), EffectSummary::default());
    db.execute_batch("CREATE TABLE __sys_raft_inbox(sequence); INSERT INTO __sys_raft_inbox VALUES(1); CREATE TABLE __sys_raft_alarms(due); INSERT INTO __sys_raft_alarms VALUES(20),(0); CREATE TABLE __sys_raft_outbox(next_at); INSERT INTO __sys_raft_outbox VALUES(30),(10);").unwrap();
    assert_eq!(
        read(&db, 2).unwrap(),
        EffectSummary {
            next_alarm_unix_millis: Some(0),
            next_outbox_unix_millis: Some(10),
            pending_outbox: 2,
            pending_inbox: 0,
        }
    );
    assert!(read(&db, 1).is_err());
}

#[test]
fn malformed_effect_state_refuses_instead_of_reporting_idle() {
    for ddl in [
        "CREATE VIEW __sys_raft_alarms AS SELECT 10 AS due",
        "CREATE TABLE __sys_raft_alarms(other)",
        "CREATE TABLE __sys_raft_alarms(due); INSERT INTO __sys_raft_alarms VALUES(-1)",
        "CREATE TABLE __sys_raft_outbox(next_at); INSERT INTO __sys_raft_outbox VALUES(NULL)",
        "CREATE TABLE __sys_raft_outbox(next_at); INSERT INTO __sys_raft_outbox VALUES('10')",
        "CREATE TABLE __sys_raft_outbox(next_at); INSERT INTO __sys_raft_outbox VALUES(1.5)",
    ] {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(ddl).unwrap();
        assert!(read(&db, 100).is_err(), "{ddl}");
    }
}
