// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Sandbox decisions use the current private Raft image, never a Fleet database.
//! Retained identities pin both the execution history and authenticated parent.
use super::*;
use crate::application::consensus::{CellHistory, Durability};
use crate::application::containers::*;
use anyhow::{ensure, Result};

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_raft_sandbox_schema",
    ddl: "CREATE TABLE __sys_raft_sandbox_owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1),history BLOB NOT NULL CHECK(typeof(history)='blob' AND length(history)=32),parent BLOB CHECK(parent IS NULL OR (typeof(parent)='blob' AND length(parent)=32)),confirmed_revision INTEGER NOT NULL CHECK(typeof(confirmed_revision)='integer' AND confirmed_revision>=0),minimum_durability INTEGER NOT NULL CHECK(minimum_durability IN (1,2)));",
    required: &[("__sys_raft_sandbox_owner", "singleton,history,parent,confirmed_revision,minimum_durability")],
};

pub(super) fn check_schema(db: &Connection) -> Result<()> {
    if SCHEMA.check(db)? {
        let count: u64 =
            db.query_row("SELECT count(*) FROM __sys_raft_sandbox_owner", [], |r| {
                r.get(0)
            })?;
        ensure!(count == 1, "invalid shared sandbox owner inventory");
        let (history, parent): (Vec<u8>, Option<Vec<u8>>) = db.query_row(
            "SELECT history,parent FROM __sys_raft_sandbox_owner WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            history.len() == 32 && parent.as_ref().is_none_or(|p| p.len() == 32),
            "invalid shared sandbox owner"
        );
        let work: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='__sys_work')",
            [],
            |row| row.get(0),
        )?;
        if work {
            let wrong_scope: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM __sys_work WHERE reason='sandbox-cleanup' AND scope!=?1)",
                [RAFT_STORAGE_SCOPE], |row| row.get(0),
            )?;
            ensure!(!wrong_scope, "shared sandbox work has a different storage scope; recreate the development history");
        }
    }
    Ok(())
}

pub(super) fn installed(db: &Connection) -> Result<bool> {
    SCHEMA.check(db)
}

fn owner(
    db: &Connection,
    history: &CellHistory,
    parent: Option<[u8; 32]>,
    create: bool,
) -> Result<()> {
    let digest = crate::raft::cell_groups::cell_image_history_digest(history);
    if SCHEMA.check(db)? {
        let (old, source): (Vec<u8>, Option<Vec<u8>>) = db.query_row(
            "SELECT history,parent FROM __sys_raft_sandbox_owner WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            old == digest && source.as_deref() == parent.as_ref().map(|p| p.as_slice()),
            "shared sandbox history changed; explicit fenced recovery is required"
        );
        if let Some(retained) = super::sandbox_catalog::read_parent(db)? {
            ensure!(
                parent.is_none() && retained.scope == history.cell.as_str(),
                "sandbox catalog has another logical owner"
            );
            confirmation_pending(db, &retained)?;
        }
        if let Some(retained) = super::sandbox_children::read(db)? {
            ensure!(
                parent.is_some() && retained.identity.execution_scope == history.cell.as_str(),
                "sandbox child has another logical owner"
            );
        }
    } else if create {
        ensure!(
            super::sandbox_catalog::read_parent(db)?.is_none()
                && super::sandbox_children::read(db)?.is_none(),
            "retained sandbox owner is missing; refuse a replacement authority"
        );
        if parent.is_some() && super::container_bindings::check_schema(db)? {
            let retained: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM __sys_container_bindings LIMIT 1)",
                [],
                |r| r.get(0),
            )?;
            ensure!(
                !retained,
                "retained execution lacks its original sandbox authority"
            );
        }
        SCHEMA.open(db)?;
        db.execute(
            "INSERT INTO __sys_raft_sandbox_owner VALUES(1,?1,?2,0,1)",
            rusqlite::params![digest.as_slice(), parent.as_ref().map(|p| p.as_slice())],
        )?;
    } else {
        ensure!(
            super::sandbox_catalog::read_parent(db)?.is_none()
                && super::sandbox_children::read(db)?.is_none(),
            "shared sandbox owner is missing"
        );
    }
    Ok(())
}

pub(crate) fn query_parent(
    db: &Connection,
    history: &CellHistory,
) -> Result<Option<SandboxParent>> {
    as_framework_sql(|| {
        owner(db, history, None, false)?;
        super::sandbox_catalog::read_parent(db)
    })
}

pub(crate) fn query_allocation(
    db: &Connection,
    history: &CellHistory,
    name: &str,
) -> Result<Option<SandboxAllocation>> {
    crate::application::sandbox_catalog::values::name(name)?;
    as_framework_sql(|| {
        owner(db, history, None, false)?;
        match super::sandbox_catalog::read_parent(db)? {
            Some(p) => super::sandbox_catalog::read(db, &p, name),
            None => Ok(None),
        }
    })
}

fn access<T>(
    history: &CellHistory,
    parent: Option<[u8; 32]>,
    read: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    require_sql_healthy(RAFT_STORAGE_SCOPE).map_err(anyhow::Error::msg)?;
    with(RAFT_STORAGE_SCOPE, |db| {
        as_framework_sql(|| {
            owner(db, history, parent, false)?;
            read(db)
        })
    })
    .ok_or_else(|| anyhow::anyhow!("shared sandbox image is not active"))?
}

fn change<T>(
    history: &CellHistory,
    parent: Option<[u8; 32]>,
    write: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let minimum = super::raft_invocation_minimum()?;
    with_application_storage_write(RAFT_STORAGE_SCOPE, |db| {
        super::sandbox_catalog::transaction(db, |db| {
            owner(db, history, parent, true)?;
            let strengthened = strengthen_minimum(db, minimum)?;
            db.execute_batch(super::work::SCHEMA)?;
            let result = write(db)?;
            if strengthened && parent.is_none() {
                super::sandbox_work::rearm(db, history.cell.as_str())?;
            }
            Ok(result)
        })
    })
}

pub(crate) fn check(history: &CellHistory) -> Result<()> {
    access(history, None, |_| Ok(()))
}

pub(crate) fn parent(history: &CellHistory) -> Result<Option<SandboxParent>> {
    access(history, None, super::sandbox_catalog::read_parent)
}

pub(crate) fn load(history: &CellHistory, name: &str) -> Result<Option<SandboxAllocation>> {
    access(
        history,
        None,
        |db| match super::sandbox_catalog::read_parent(db)? {
            Some(p) => super::sandbox_catalog::read(db, &p, name),
            None => Ok(None),
        },
    )
}

pub(crate) fn list(
    history: &CellHistory,
    after: Option<&str>,
    limit: usize,
    cleanup: bool,
) -> Result<SandboxAllocationPage> {
    access(
        history,
        None,
        |db| match super::sandbox_catalog::read_parent(db)? {
            Some(p) => super::sandbox_catalog::page_at(db, &p, after, limit, cleanup),
            None => Ok(SandboxAllocationPage {
                allocations: Vec::new(),
                next: None,
            }),
        },
    )
}

pub(crate) struct Reservation<'a> {
    pub incarnation: &'a str,
    pub name: &'a str,
    pub binding: &'a str,
    pub parameters: &'a str,
    pub profile: Option<&'a str>,
}

pub(crate) fn reserve(
    history: &CellHistory,
    input: Reservation<'_>,
    execution_scope: &impl Fn(&str) -> String,
) -> Result<(SandboxParent, SandboxReservation)> {
    change(history, None, |db| {
        let result = super::sandbox_catalog::reserve_at(
            db,
            history.cell.as_str(),
            input.incarnation,
            input.name,
            input.binding,
            input.parameters,
            execution_scope,
        )?;
        if let Some(profile) = input.profile {
            super::sandbox_work::register_in(
                db,
                history.cell.as_str(),
                &result.1.allocation,
                profile,
            )?;
        }
        Ok(result)
    })
}

pub(crate) fn transition(
    history: &CellHistory,
    expected: &SandboxAllocation,
    target: SandboxAllocationState,
) -> Result<(SandboxParent, SandboxAllocation)> {
    change(history, None, |db| {
        super::sandbox_catalog::transition_at(db, history.cell.as_str(), expected, target)
    })
}

pub(crate) fn retire(
    history: &CellHistory,
    incarnation: &str,
    finish: bool,
) -> Result<SandboxParent> {
    change(history, None, |db| {
        super::sandbox_catalog::retire_at(db, history.cell.as_str(), incarnation, finish)
    })
}

pub(crate) fn claim(history: &CellHistory, now: i64) -> Result<Option<SandboxAllocation>> {
    let minimum = super::raft_invocation_minimum()?;
    change(history, None, |db| {
        check_work_minimum(db, minimum)?;
        super::sandbox_work::claim_at(db, history.cell.as_str(), now)
    })
}

pub(crate) fn refresh(history: &CellHistory) -> Result<()> {
    change(history, None, |db| {
        super::sandbox_work::rearm(db, history.cell.as_str())
    })
}

pub(crate) fn attach_child(history: &CellHistory, parent: [u8; 32]) -> Result<()> {
    change(history, Some(parent), |_| Ok(()))
}

pub(crate) fn child(
    history: &CellHistory,
    parent: [u8; 32],
) -> Result<Option<crate::runtime::sandbox_execution::Record>> {
    access(history, Some(parent), super::sandbox_children::read)
}

pub(crate) fn save_child(
    history: &CellHistory,
    parent: [u8; 32],
    record: &crate::runtime::sandbox_execution::Record,
) -> Result<()> {
    record.identity.validate()?;
    ensure!(
        record.identity.execution_scope == history.cell.as_str(),
        "shared sandbox child scope differs"
    );
    let encoded = serde_json::to_string(record)?;
    ensure!(
        encoded.len() <= 4096,
        "sandbox child record exceeds byte limit"
    );
    change(history, Some(parent), |db| {
        super::sandbox_children::save_in(db, record, &encoded)
    })
}

/// Applied terminal metadata remains discoverable until a mandatory source
/// proof has covered that exact revision. A missing response proves nothing.
pub(super) fn confirmation_pending(db: &Connection, parent: &SandboxParent) -> Result<bool> {
    if !installed(db)? {
        return Ok(false);
    }
    let confirmed: u64 = db.query_row(
        "SELECT confirmed_revision FROM __sys_raft_sandbox_owner WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        confirmed == 0
            || (parent.state == SandboxParentState::Deleted && confirmed == parent.revision),
        "sandbox confirmation revision differs"
    );
    Ok(parent.state == SandboxParentState::Deleted && confirmed == 0)
}

fn confirm_in(
    db: &Connection,
    history: &CellHistory,
    expected: &SandboxParent,
    minimum: Durability,
) -> Result<()> {
    owner(db, history, None, false)?;
    check_work_minimum(db, minimum)?;
    ensure!(
        expected.state == SandboxParentState::Deleted
            && super::sandbox_catalog::read_parent(db)?.as_ref() == Some(expected),
        "sandbox retirement confirmation changed"
    );
    db.execute(
        "UPDATE __sys_raft_sandbox_owner SET confirmed_revision=?1 WHERE singleton=1",
        [i64::try_from(expected.revision)?],
    )?;
    super::sandbox_work::rearm(db, history.cell.as_str())
}

pub(crate) fn confirm_retirement(history: &CellHistory, expected: &SandboxParent) -> Result<()> {
    let minimum = super::raft_invocation_minimum()?;
    change(history, None, |db| {
        confirm_in(db, history, expected, minimum)
    })
}

pub(crate) fn query_cleanup_pending(db: &Connection, history: &CellHistory) -> Result<bool> {
    as_framework_sql(|| {
        owner(db, history, None, false)?;
        let Some(parent) = super::sandbox_catalog::read_parent(db)? else {
            return Ok(false);
        };
        if parent.state == SandboxParentState::Deleted {
            return confirmation_pending(db, &parent);
        }
        if parent.state == SandboxParentState::Deleting {
            return Ok(true);
        }
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_sandbox_allocations WHERE state='deleting' LIMIT 1)",
            [],
            |r| r.get(0),
        )?)
    })
}

/// Source policy lives with the logical parent, independently of the namespace
/// default and a discovery task's reusable wake generation.
pub(crate) fn next_minimum(db: &Connection) -> Result<Durability> {
    ensure!(
        installed(db)?,
        crate::application::consensus::CommandFailure::CorruptState
    );
    let value: u8 = db.query_row(
        "SELECT minimum_durability FROM __sys_raft_sandbox_owner WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    match value {
        1 => Ok(Durability::Quorum),
        2 => Ok(Durability::S3),
        _ => anyhow::bail!(crate::application::consensus::CommandFailure::CorruptState),
    }
}

fn strengthen_minimum(db: &Connection, minimum: Durability) -> Result<bool> {
    let previous = next_minimum(db)?;
    if minimum > previous {
        let encoded = match minimum {
            Durability::Quorum => 1,
            Durability::S3 => 2,
        };
        db.execute("UPDATE __sys_raft_sandbox_owner SET minimum_durability=?1,confirmed_revision=0 WHERE singleton=1",[encoded])?;
    }
    Ok(minimum > previous)
}

pub(crate) fn check_work_minimum(db: &Connection, admitted: Durability) -> Result<()> {
    ensure!(
        admitted >= next_minimum(db)?,
        crate::application::consensus::CommandFailure::UnsupportedCapability
    );
    Ok(())
}

#[cfg(test)]
#[path = "sandbox_shared_tests.rs"]
mod tests;
