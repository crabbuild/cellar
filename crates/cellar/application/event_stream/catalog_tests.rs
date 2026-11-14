// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use rusqlite::{params, Connection};

#[test]
fn coalesced_catalog_work_keeps_monotonic_retry_and_operation_fences() -> Result<()> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(include_str!("catalog.sql"))?;
    let id = "1".repeat(32);
    let first = "a".repeat(32);
    let second = "b".repeat(32);
    db.execute("INSERT INTO __sys_event_catalog(id,name,lifecycle,tenant,application,family,operation,digest,operation_authority,pending_definition,pending_authority,desired_revision,retry_at_ms) VALUES(?,'events','creating','team','app','events',?,'digest','authority','definition','authority',1,1000)", params![id, first])?;
    let retry = || {
        db.query_row(
            "SELECT retry_at_ms FROM __sys_event_catalog WHERE id=?",
            [&id],
            |row| row.get::<_, i64>(0),
        )
    };
    assert_eq!(
        db.execute(REARM_RECONCILIATION, params![500, id, first])?,
        1
    );
    assert_eq!(retry()?, 1000);
    assert_eq!(
        db.execute(REARM_RECONCILIATION, params![2500, id, first])?,
        1
    );
    assert_eq!(retry()?, 2500);
    db.execute(
        "UPDATE __sys_event_catalog SET operation=?,retry_at_ms=4000 WHERE id=?",
        params![second, id],
    )?;
    assert_eq!(
        db.execute(REARM_RECONCILIATION, params![9000, id, first])?,
        0
    );
    assert_eq!(retry()?, 4000);
    db.execute(
        "UPDATE __sys_event_catalog SET pending_definition=NULL,lifecycle='active' WHERE id=?",
        [&id],
    )?;
    assert_eq!(
        db.execute(REARM_RECONCILIATION, params![9000, id, second])?,
        0
    );
    assert_eq!(retry()?, 4000);
    db.execute("UPDATE __sys_event_catalog SET pending_retirement='request',lifecycle='retiring' WHERE id=?", [&id])?;
    assert_eq!(
        db.execute(REARM_RECONCILIATION, params![5500, id, second])?,
        1
    );
    assert_eq!(retry()?, 5500);
    Ok(())
}

#[test]
fn duplicate_and_stale_catalog_completions_preserve_accepted_metadata() -> Result<()> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(include_str!("catalog.sql"))?;
    let id = "1".repeat(32);
    let first = "a".repeat(32);
    let second = "b".repeat(32);
    db.execute(
            "INSERT INTO __sys_event_catalog(id,name,lifecycle,tenant,application,family,operation,digest,operation_authority,pending_definition,pending_authority,desired_revision,retry_at_ms) VALUES(?,'events','creating','team','app','changed',?,'digest','authority','definition-v1','authority',1,0)",
            params![id, first],
        )?;
    assert_eq!(db.execute(ACCEPT_CONFIGURATION, params![id, first])?, 1);
    assert_eq!(db.execute(ACCEPT_CONFIGURATION, params![id, first])?, 0);
    assert_eq!(
        db.execute(RECORD_FAILURE, params!["late failure", id, first])?,
        0
    );
    let state = || {
        db.query_row(
                "SELECT definition,accepted_revision,pending_definition,desired_revision,last_error FROM __sys_event_catalog WHERE id=?",
                [&id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
    };
    assert_eq!(
        state()?,
        (Some("definition-v1".into()), Some(1), None, None, None)
    );
    db.execute(
            "UPDATE __sys_event_catalog SET lifecycle='updating',operation=?,pending_definition='definition-v2',pending_authority='authority',desired_revision=2 WHERE id=?",
            params![second, id],
        )?;
    assert_eq!(db.execute(ACCEPT_CONFIGURATION, params![id, first])?, 0);
    assert_eq!(
        db.execute(RECORD_FAILURE, params!["old operation", id, first])?,
        0
    );
    assert_eq!(
        state()?,
        (
            Some("definition-v1".into()),
            Some(1),
            Some("definition-v2".into()),
            Some(2),
            None
        )
    );
    assert_eq!(
        db.execute(RECORD_FAILURE, params!["pending retry", id, second])?,
        1
    );
    assert_eq!(db.execute(ACCEPT_CONFIGURATION, params![id, second])?, 1);
    assert_eq!(db.execute(ACCEPT_CONFIGURATION, params![id, second])?, 0);
    assert_eq!(
        db.execute(RECORD_FAILURE, params!["late retry", id, second])?,
        0
    );
    assert_eq!(
        state()?,
        (Some("definition-v2".into()), Some(2), None, None, None)
    );
    Ok(())
}
