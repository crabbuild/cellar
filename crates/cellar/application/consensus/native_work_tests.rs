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
    complete(&db, &queue, 20 + queue.lease_ms)?;
    assert_eq!(get(&db, "queue")?, Some(30));
    cancel(&db, "queue")?;
    let retry = due(&db, 10 + LEASE_MS)?.unwrap();
    assert_eq!(retry.generation, alarm.generation);
    assert_eq!(retry.attempts, 1);
    assert!(claim(&db, &retry, 10 + LEASE_MS)?);
    complete(&db, &alarm, 10 + alarm.lease_ms)?;
    assert!(get(&db, "alarm")?.is_some());
    complete(&db, &retry, 10 + LEASE_MS + retry.lease_ms)?;
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
    complete(&db, &work, 10 + work.lease_ms)?;
    assert_eq!(get(&db, "queue")?, None);
    Ok(())
}

#[test]
fn queue_wakes_coalesce_only_before_claim_and_preserve_new_obligations() -> Result<()> {
    for reason in ["queue", "filesystem"] {
        let db = Connection::open_in_memory()?;
        arm_maintenance(&db, reason, 10)?;
        let original = due(&db, 10)?.unwrap();
        arm_maintenance(&db, reason, 20)?;
        assert_eq!(due(&db, 10)?.unwrap().generation, original.generation);
        assert!(claim(&db, &original, 10)?);
        let leased_until = 10 + original.lease_ms;
        complete(&db, &original, leased_until + 1)?;
        assert_eq!(get(&db, reason)?, Some(i64::try_from(leased_until)?));
        let later = 10 + LEASE_MS as i64 + 100;
        arm_maintenance(&db, reason, later)?;
        complete(&db, &original, 10 + original.lease_ms)?;
        let replacement = due(&db, later as u64)?.unwrap();
        assert_ne!(replacement.generation, original.generation);
        assert_eq!(replacement.attempts, 0);
        assert_eq!(replacement.due, later as u64);
    }
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

#[test]
fn alarm_failures_commit_backoff_and_exhaust_only_counted_failures() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    let mut now = 10;
    for (retry, delay) in [2_000, 4_000, 8_000, 16_000, 32_000, 64_000]
        .into_iter()
        .enumerate()
    {
        let work = due(&db, now)?.unwrap();
        assert!(claim(&db, &work, now)?);
        let leased_until = now + work.lease_ms;
        assert!(retry_failed(&db, &work, leased_until, now as i64, true)?);
        assert!(!retry_failed(&db, &work, leased_until, now as i64, true)?);
        complete(&db, &work, leased_until)?;
        now += delay;
        assert_eq!(get(&db, "alarm")?, Some(now as i64));
        assert_eq!(
            db.query_row(
                "SELECT retry,counted_retry FROM __sys_raft_native_work",
                [],
                |row| Ok((row.get::<_, usize>(0)?, row.get::<_, usize>(1)?))
            )?,
            (retry + 1, retry + 1)
        );
        assert!(due(&db, now - 1)?.is_none());
    }
    let work = due(&db, now)?.unwrap();
    assert!(claim(&db, &work, now)?);
    assert!(retry_failed(
        &db,
        &work,
        now + work.lease_ms,
        now as i64,
        true
    )?);
    assert!(get(&db, "alarm")?.is_none());
    Ok(())
}

#[test]
fn alarm_failure_phase_survives_equal_lease_and_backoff_deadlines() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    let work = due(&db, 10)?.unwrap();
    assert!(claim(&db, &work, 10)?);
    let leased_until = 10 + work.lease_ms;
    let failed_at = i64::try_from(leased_until)? - 2_000;
    assert!(retry_failed(&db, &work, leased_until, failed_at, true)?);
    assert_eq!(get(&db, "alarm")?, Some(leased_until as i64));
    assert!(!retry_failed(&db, &work, leased_until, failed_at, true)?);
    complete(&db, &work, leased_until)?;
    assert!(get(&db, "alarm")?.is_some());
    let next = due(&db, leased_until)?.unwrap();
    assert!(claim(&db, &next, leased_until)?);
    complete(&db, &work, leased_until)?;
    assert!(!retry_failed(&db, &work, leased_until, failed_at, true)?);
    assert!(get(&db, "alarm")?.is_some());
    Ok(())
}

#[test]
fn alarm_failures_rollback_and_cannot_edit_replacements() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    let work = due(&db, 10)?.unwrap();
    assert!(claim(&db, &work, 10)?);
    let leased_until = 10 + work.lease_ms;
    db.execute_batch("BEGIN")?;
    assert!(retry_failed(&db, &work, leased_until, 11, true)?);
    db.execute_batch("ROLLBACK")?;
    assert_eq!(get(&db, "alarm")?, Some(leased_until as i64));
    assert_eq!(
        db.query_row(
            "SELECT counted_retry FROM __sys_raft_native_work",
            [],
            |row| row.get::<_, u32>(0)
        )?,
        0
    );
    arm(&db, "alarm", 10)?;
    assert!(!retry_failed(&db, &work, leased_until, 11, true)?);
    complete(&db, &work, leased_until)?;
    assert_eq!(get(&db, "alarm")?, Some(10));
    Ok(())
}

#[test]
fn alarm_lease_recovery_does_not_count_as_application_failure() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    let first = due(&db, 10)?.unwrap();
    assert!(claim(&db, &first, 10)?);
    let now = 10 + first.lease_ms;
    let recovered = due(&db, now)?.unwrap();
    assert!(claim(&db, &recovered, now)?);
    assert!(!retry_failed(&db, &first, now, now as i64, true)?);
    assert_eq!(
        db.query_row(
            "SELECT retry,counted_retry FROM __sys_raft_native_work",
            [],
            |row| Ok((row.get::<_, u32>(0)?, row.get::<_, u32>(1)?))
        )?,
        (0, 0)
    );
    assert!(retry_failed(
        &db,
        &recovered,
        now + recovered.lease_ms,
        now as i64,
        true
    )?);
    assert_eq!(get(&db, "alarm")?, Some(now as i64 + 2_000));
    Ok(())
}

#[test]
fn excused_alarm_failures_do_not_exhaust_counted_retries() -> Result<()> {
    let db = Connection::open_in_memory()?;
    arm(&db, "alarm", 10)?;
    let mut now = 10;
    for delay in [
        2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 128_000,
    ] {
        let work = due(&db, now)?.unwrap();
        assert!(claim(&db, &work, now)?);
        assert!(retry_failed(
            &db,
            &work,
            now + work.lease_ms,
            now as i64,
            false
        )?);
        now += delay;
        assert_eq!(get(&db, "alarm")?, Some(now as i64));
        assert_eq!(
            db.query_row(
                "SELECT counted_retry FROM __sys_raft_native_work",
                [],
                |row| row.get::<_, u32>(0)
            )?,
            0
        );
    }
    for counted in 0..6 {
        let work = due(&db, now)?.unwrap();
        assert!(claim(&db, &work, now)?);
        assert!(retry_failed(
            &db,
            &work,
            now + work.lease_ms,
            now as i64,
            true
        )?);
        now += 128_000;
        assert_eq!(get(&db, "alarm")?, Some(now as i64));
        assert_eq!(
            db.query_row(
                "SELECT counted_retry FROM __sys_raft_native_work",
                [],
                |row| row.get::<_, u32>(0)
            )?,
            counted + 1
        );
    }
    let work = due(&db, now)?.unwrap();
    assert!(claim(&db, &work, now)?);
    assert!(retry_failed(
        &db,
        &work,
        now + work.lease_ms,
        now as i64,
        true
    )?);
    assert!(get(&db, "alarm")?.is_none());
    Ok(())
}
