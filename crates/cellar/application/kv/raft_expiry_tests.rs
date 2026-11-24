// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::native_work;
use crate::storage::{CommandDatabase, CommandInputs, CommandTransaction};

const AT: u64 = 1_000;

fn inputs() -> CommandInputs {
    CommandInputs {
        now_unix_millis: AT,
        random: [91; 32],
    }
}

fn cell(tx: &CommandTransaction<'_>) -> Result<Arc<NamespaceCell>> {
    let storage = tx.storage()?;
    storage
        .resources()
        .durable_callbacks
        .store(true, Ordering::Release);
    let cell = namespace_cell(storage);
    cell.open()?;
    Ok(cell)
}

fn seed(database: &mut CommandDatabase, expired: usize, future: bool) -> Result<native_work::Work> {
    database.transaction(inputs(), |tx| {
        let cell = cell(tx)?;
        for key in 0..expired {
            cell.exec(
                "INSERT INTO __sys_kv(name,value,size,tag,expires_at) VALUES(?,x'01',1,'bytes',?)",
                &[format!("expired-{key}").into(), (AT as i64).into()],
            )?;
        }
        if future {
            cell.exec(
                "INSERT INTO __sys_kv(name,value,size,tag,expires_at) VALUES('future',x'02',1,'bytes',2000)",
                &[],
            )?;
        }
        cell.storage.set_alarm(AT as i64)
    })?;
    native_work::due(database.framework(), AT)?.ok_or_else(|| anyhow!("missing seeded work"))
}

fn run(tx: &CommandTransaction<'_>, work: &native_work::Work, at_ms: u64) -> Result<bool> {
    assert!(native_work::claim(&*tx.framework()?, work, at_ms)?);
    tx.begin_native_alarm(work.generation)?;
    let performed = cell(tx)?.try_expire(i64::try_from(at_ms)?)?;
    if performed {
        native_work::complete(&*tx.framework()?, work, at_ms + work.lease_ms)?;
    }
    Ok(performed)
}

#[test]
fn expiry_and_work_completion_rollback_together() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = CommandDatabase::open(&root.path().join("kv.sqlite"), None, 16 << 20)?;
    let work = seed(&mut database, 1, false)?;
    let failed: Result<()> = database.transaction(inputs(), |tx| {
        assert!(run(tx, &work, AT)?);
        anyhow::bail!("final expiry transaction failed")
    });
    assert!(failed.is_err());
    assert_eq!(
        database
            .framework()
            .query_row("SELECT count(*) FROM __sys_kv", [], |r| r.get::<_, u64>(0))?,
        1
    );
    assert_eq!(
        serde_json::to_value(native_work::due(database.framework(), AT)?)?,
        serde_json::to_value(Some(&work))?
    );
    database.transaction(inputs(), |tx| {
        assert!(run(tx, &work, AT)?);
        Ok(())
    })?;
    assert_eq!(
        database
            .framework()
            .query_row("SELECT count(*) FROM __sys_kv", [], |r| r.get::<_, u64>(0))?,
        0
    );
    assert_eq!(native_work::get(database.framework(), "alarm")?, None);
    Ok(())
}

#[test]
fn failed_sql_expiry_rolls_back_its_unproven_claim() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = CommandDatabase::open(&root.path().join("kv.sqlite"), None, 16 << 20)?;
    let work = seed(&mut database, 1, false)?;
    database.framework().execute_batch(
        "CREATE TRIGGER reject_expiry BEFORE DELETE ON __sys_kv BEGIN SELECT RAISE(ABORT,'expiry refusal'); END;",
    )?;
    let failed = database.transaction(inputs(), |tx| run(tx, &work, AT));
    assert!(format!("{:#}", failed.unwrap_err()).contains("expiry refusal"));
    assert_eq!(
        serde_json::to_value(native_work::due(database.framework(), AT)?)?,
        serde_json::to_value(Some(&work))?
    );
    assert_eq!(
        database
            .framework()
            .query_row("SELECT count(*) FROM __sys_kv", [], |r| r.get::<_, u64>(0))?,
        1
    );
    database
        .framework()
        .execute_batch("DROP TRIGGER reject_expiry")?;
    database.transaction(inputs(), |tx| {
        assert!(run(tx, &work, AT)?);
        Ok(())
    })?;
    assert!(native_work::due(database.framework(), AT)?.is_none());
    Ok(())
}

#[test]
fn expiry_uses_captured_time_and_preserves_the_next_generation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = CommandDatabase::open(&root.path().join("kv.sqlite"), None, 16 << 20)?;
    let work = seed(&mut database, 1, true)?;
    database.transaction(inputs(), |tx| {
        assert!(run(tx, &work, AT)?);
        native_work::complete(&*tx.framework()?, &work, AT + work.lease_ms)?;
        Ok(())
    })?;
    assert_eq!(
        native_work::get(database.framework(), "alarm")?,
        Some(2_000)
    );
    assert!(native_work::due(database.framework(), AT)?.is_none());
    let next = native_work::due(database.framework(), 2_000)?.unwrap();
    assert_ne!(next.generation, work.generation);
    assert_eq!(next.attempts, 0);
    assert_eq!(
        database
            .framework()
            .query_row("SELECT name FROM __sys_kv", [], |r| r.get::<_, String>(0))?,
        "future"
    );
    Ok(())
}

#[test]
fn expiry_is_bounded_and_old_completion_cannot_erase_its_continuation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = CommandDatabase::open(&root.path().join("kv.sqlite"), None, 16 << 20)?;
    let work = seed(&mut database, cellar_logic::sweep::BATCH_ROWS + 1, false)?;
    database.transaction(inputs(), |tx| {
        assert!(run(tx, &work, AT)?);
        Ok(())
    })?;
    assert_eq!(
        database
            .framework()
            .query_row("SELECT count(*) FROM __sys_kv", [], |r| r.get::<_, u64>(0))?,
        1
    );
    assert_eq!(
        native_work::get(database.framework(), "alarm")?,
        Some(1_001)
    );
    database.transaction(inputs(), |tx| {
        native_work::complete(&*tx.framework()?, &work, AT + work.lease_ms)
    })?;
    assert_eq!(
        native_work::get(database.framework(), "alarm")?,
        Some(1_001)
    );
    Ok(())
}

#[test]
fn busy_namespace_and_provider_sweep_require_the_ordinary_proven_path() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut database = CommandDatabase::open(&root.path().join("kv.sqlite"), None, 16 << 20)?;
    seed(&mut database, 1, false)?;
    database.transaction(inputs(), |tx| {
        let cell = cell(tx)?;
        let _protocol = cell.protocol.try_lock().unwrap();
        assert!(!cell.try_expire(AT as i64)?);
        Ok(())
    })?;
    database.transaction(inputs(), |tx| {
        let cell = cell(tx)?;
        cell.exec(
            "INSERT INTO __sys_kv_meta(name,value) VALUES('blob-sweep-due',1)",
            &[],
        )?;
        assert!(!cell.try_expire(AT as i64)?);
        Ok(())
    })?;
    assert_eq!(
        database
            .framework()
            .query_row("SELECT count(*) FROM __sys_kv", [], |r| r.get::<_, u64>(0))?,
        1
    );
    assert!(native_work::due(database.framework(), AT)?.is_some());
    Ok(())
}
