// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn callback_claims_recover_without_consuming_other_generations() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    arm(&db, "queue", 20)?;
    let alarm = due(&db, 10)?.unwrap();
    assert!(claim(&db, &alarm, 10)?);
    assert!(!claim(&db, &alarm, 10)?);
    assert!(due(&db, 19)?.is_none());
    let queue = due(&db, 20)?.unwrap();
    assert_eq!(queue.reason, "queue");
    assert!(claim(&db, &queue, 20)?);
    arm(&db, "queue", 30)?;
    complete(&db, &queue)?;
    assert_eq!(get(&db, "queue")?, Some(30));
    cancel(&db, "queue")?;
    let retry = due(&db, 10 + LEASE_MS)?.unwrap();
    assert_eq!(retry.generation, alarm.generation);
    assert_eq!(retry.attempts, 1);
    assert!(claim(&db, &retry, 10 + LEASE_MS)?);
    complete(&db, &alarm)?;
    assert!(get(&db, "alarm")?.is_some());
    complete(&db, &retry)?;
    assert!(due(&db, u32::MAX.into())?.is_none());
    Ok(())
}

#[test]
fn callback_installation_and_claim_follow_transaction_rollback() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    let original = due(&db, 10)?.unwrap();
    db.execute_batch("BEGIN")?;
    assert!(claim(&db, &original, 10)?);
    arm(&db, "alarm", 50)?;
    db.execute_batch("ROLLBACK")?;
    assert_eq!(due(&db, 10)?.unwrap().generation, original.generation);
    assert!(claim(&db, &original, 10)?);
    assert!(arm(&db, "unknown", 10).is_err());
    assert!(arm(&db, "queue", -1).is_err());
    Ok(())
}

#[test]
fn callback_claim_uses_its_bounded_execution_lease() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "queue", 10)?;
    let mut work = due(&db, 10)?.unwrap();
    for invalid in [0, 1000, 301_001, u64::MAX] {
        work.lease_ms = invalid;
        assert!(claim(&db, &work, 10).is_err());
        assert_eq!(get(&db, "queue")?, Some(10));
    }
    work.lease_ms = 91_000;
    assert!(claim(&db, &work, 10)?);
    assert!(due(&db, 91_009)?.is_none());
    let retry = due(&db, 91_010)?.unwrap();
    assert_eq!(retry.generation, work.generation);
    assert_eq!(retry.attempts, 1);
    complete(&db, &work)?;
    assert_eq!(get(&db, "queue")?, None);
    Ok(())
}

#[test]
fn queue_wakes_coalesce_only_before_claim_and_preserve_new_obligations() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm_queue(&db, 10)?;
    let original = due(&db, 10)?.unwrap();
    arm_queue(&db, 20)?;
    assert_eq!(due(&db, 10)?.unwrap().generation, original.generation);
    assert!(claim(&db, &original, 10)?);
    let later = 10 + LEASE_MS as i64 + 100;
    arm_queue(&db, later)?;
    complete(&db, &original)?;
    let replacement = due(&db, later as u64)?.unwrap();
    assert_ne!(replacement.generation, original.generation);
    assert_eq!(replacement.attempts, 0);
    assert_eq!(replacement.due, later as u64);
    Ok(())
}

#[test]
fn queue_wakes_keep_earlier_installs_and_transaction_rollback() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm_queue(&db, 20)?;
    let original = due(&db, 20)?.unwrap();
    db.execute_batch("BEGIN")?;
    arm_queue(&db, 10)?;
    assert_ne!(due(&db, 10)?.unwrap().generation, original.generation);
    db.execute_batch("ROLLBACK")?;
    assert_eq!(due(&db, 20)?.unwrap().generation, original.generation);
    assert!(arm_queue(&db, -1).is_err());
    arm(&db, "alarm", 30)?;
    let alarm = db.query_row(
        "SELECT generation FROM __sys_raft_native_work WHERE reason='alarm'",
        [],
        |row| row.get::<_, Vec<u8>>(0),
    )?;
    arm(&db, "alarm", 30)?;
    assert_ne!(
        db.query_row(
            "SELECT generation FROM __sys_raft_native_work WHERE reason='alarm'",
            [],
            |row| row.get::<_, Vec<u8>>(0)
        )?,
        alarm
    );
    Ok(())
}
