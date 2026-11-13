// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Committed, generation-fenced ordinary callbacks. The manifest projects the
//! earliest deadline; only a command against the selected image claims work.

use anyhow::{ensure, Result};
use cellar_logic::consensus::CommandFailure;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

pub(crate) const OPERATION: &str = "__sys_raft_native_work";

// Each claim carries the admitted callback budget. The extra second prevents a
// fresh claim from racing the former owner before its execution deadline.
const LEASE_MS: u64 = 61_000;
const MAX_REASONS: usize = 16;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Work {
    pub reason: String,
    pub generation: [u8; 16],
    pub due: u64,
    pub attempts: u32,
    pub lease_ms: u64,
}

fn exists(db: &Connection) -> Result<bool> {
    Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_raft_native_work')", [], |r| r.get(0))?)
}

fn install(db: &Connection) -> Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS __sys_raft_native_work (
        reason TEXT PRIMARY KEY CHECK(reason IN ('alarm','queue')),
        generation BLOB NOT NULL CHECK(typeof(generation)='blob' AND length(generation)=16),
        due INTEGER NOT NULL CHECK(typeof(due)='integer' AND due>=0),
        attempts INTEGER NOT NULL CHECK(typeof(attempts)='integer' AND attempts BETWEEN 0 AND 4294967295)
    ) WITHOUT ROWID; CREATE INDEX IF NOT EXISTS __sys_raft_native_work_due ON __sys_raft_native_work(due,reason);")?;
    Ok(())
}

fn validate(reason: &str) -> Result<()> {
    ensure!(
        matches!(reason, "alarm" | "queue"),
        CommandFailure::UnsupportedCapability
    );
    Ok(())
}

pub(crate) fn arm(db: &Connection, reason: &str, due: i64) -> Result<()> {
    validate(reason)?;
    ensure!(due >= 0, CommandFailure::UnsupportedCapability);
    install(db)?;
    let mut generation = [0; 16];
    cellar_native::crypto::random_values(&mut generation)?;
    db.execute("INSERT INTO __sys_raft_native_work(reason,generation,due,attempts) VALUES(?1,?2,?3,0) ON CONFLICT(reason) DO UPDATE SET generation=excluded.generation,due=excluded.due,attempts=0",params![reason,generation.as_slice(),due])?;
    Ok(())
}

/// Queue deadlines are discovery hints over the current broker image. Keep an
/// earlier pending wake valid across producer commits. A claimed installation
/// must always get a replacement, even at the same/later deadline, so its old
/// completion cannot remove a newly installed retry or delayed delivery.
pub(crate) fn arm_queue(db: &Connection, due: i64) -> Result<()> {
    ensure!(due >= 0, CommandFailure::UnsupportedCapability);
    if exists(db)? {
        let pending: Option<(i64, u32)> = db
            .query_row(
                "SELECT due,attempts FROM __sys_raft_native_work WHERE reason='queue'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if pending.is_some_and(|(installed, attempts)| attempts == 0 && installed <= due) {
            return Ok(());
        }
    }
    arm(db, "queue", due)
}

pub(crate) fn get(db: &Connection, reason: &str) -> Result<Option<i64>> {
    validate(reason)?;
    if !exists(db)? {
        return Ok(None);
    }
    Ok(db
        .query_row(
            "SELECT due FROM __sys_raft_native_work WHERE reason=?1",
            [reason],
            |r| r.get(0),
        )
        .optional()?)
}

pub(crate) fn cancel(db: &Connection, reason: &str) -> Result<()> {
    validate(reason)?;
    if exists(db)? {
        db.execute(
            "DELETE FROM __sys_raft_native_work WHERE reason=?1",
            [reason],
        )?;
    }
    Ok(())
}

pub(crate) fn due(db: &Connection, now: u64) -> Result<Option<Work>> {
    if !exists(db)? {
        return Ok(None);
    }
    let count: usize = db.query_row("SELECT count(*) FROM __sys_raft_native_work", [], |r| {
        r.get(0)
    })?;
    ensure!(count <= MAX_REASONS, CommandFailure::CorruptState);
    let row: Option<(String,Vec<u8>,u64,u32)> = db.query_row("SELECT reason,generation,due,attempts FROM __sys_raft_native_work WHERE due<=?1 ORDER BY due,reason LIMIT 1",[i64::try_from(now)?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    row.map(|(reason, generation, due, attempts)| {
        validate(&reason)?;
        Ok(Work {
            reason,
            generation: generation
                .try_into()
                .map_err(|_| CommandFailure::CorruptState)?,
            due,
            attempts,
            lease_ms: LEASE_MS,
        })
    })
    .transpose()
}

/// Compare the full observed installation. A delayed command cannot claim a
/// replacement, nor a lease already renewed by another callback attempt.
pub(crate) fn claim(db: &Connection, work: &Work, now: u64) -> Result<bool> {
    validate(&work.reason)?;
    ensure!(
        (1001..=301_000).contains(&work.lease_ms),
        CommandFailure::UnsupportedCapability
    );
    if !exists(db)? || work.due > now {
        return Ok(false);
    }
    let next = i64::try_from(
        now.checked_add(work.lease_ms)
            .ok_or(CommandFailure::Overloaded)?,
    )?;
    let attempts = work
        .attempts
        .checked_add(1)
        .ok_or(CommandFailure::Overloaded)?;
    Ok(db.execute("UPDATE __sys_raft_native_work SET due=?5,attempts=?6 WHERE reason=?1 AND generation=?2 AND due=?3 AND attempts=?4",params![work.reason,work.generation.as_slice(),work.due,work.attempts,next,attempts])? == 1)
}

pub(crate) fn complete(db: &Connection, work: &Work) -> Result<()> {
    db.execute(
        "DELETE FROM __sys_raft_native_work WHERE reason=?1 AND generation=?2 AND attempts=?3",
        params![
            work.reason,
            work.generation.as_slice(),
            u64::from(work.attempts) + 1
        ],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "native_work_tests.rs"]
mod tests;
