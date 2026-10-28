// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable cleanup registration and retry claims for managed named children.
use super::*;
use crate::application::containers::{
    SandboxAllocation, SandboxAllocationState, SandboxParentState,
};
use anyhow::{ensure, Result};
use cellar_logic::work::Reason;

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_sandbox_managed_schema",
    ddl: "CREATE TABLE __sys_sandbox_managed(name TEXT PRIMARY KEY NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),profile TEXT NOT NULL,due_ms INTEGER NOT NULL CHECK(due_ms>=0)) WITHOUT ROWID; CREATE INDEX __sys_sandbox_managed_due ON __sys_sandbox_managed(due_ms,name);",
    required: &[("__sys_sandbox_managed","name,generation,profile,due_ms")],
};

fn supported_work(connection: &Connection) -> Result<()> {
    let schema: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND name='__sys_work'",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        schema.contains("'sandbox-cleanup'"),
        "sandbox cleanup requires recreated development work state"
    );
    Ok(())
}

fn next(connection: &Connection) -> Result<Option<(String, u64, String, i64)>> {
    if !SCHEMA.check(connection)? {
        return Ok(None);
    }
    let parent = super::sandbox_catalog::read_parent(connection)?
        .ok_or_else(|| anyhow::anyhow!("managed sandbox parent missing"))?;
    let query = if parent.state == SandboxParentState::Active {
        "SELECT m.name,m.generation,m.profile,m.due_ms FROM __sys_sandbox_allocations AS a INDEXED BY __sys_sandbox_deleting JOIN __sys_sandbox_managed AS m ON m.name=a.name WHERE a.state='deleting' ORDER BY m.due_ms,m.name LIMIT 1"
    } else {
        "SELECT m.name,m.generation,m.profile,m.due_ms FROM __sys_sandbox_managed AS m INDEXED BY __sys_sandbox_managed_due JOIN __sys_sandbox_allocations AS a ON a.name=m.name WHERE a.state!='deleted' ORDER BY m.due_ms,m.name LIMIT 1"
    };
    Ok(connection
        .query_row(query, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .optional()?)
}

pub(super) fn rearm(connection: &Connection, scope: &str) -> Result<()> {
    if super::sandbox_erasure::read(connection)?.is_some() {
        return Ok(());
    }
    if !SCHEMA.check(connection)? {
        return Ok(());
    }
    let mut due = next(connection)?.map(|(_, _, _, due)| due);
    if due.is_none()
        && super::sandbox_catalog::read_parent(connection)?
            .is_some_and(|parent| parent.state == SandboxParentState::Deleting)
    {
        let outstanding: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_sandbox_allocations WHERE state!='deleted' LIMIT 1)",
            [],
            |row| row.get(0),
        )?;
        if !outstanding {
            due = Some(0);
        }
    }
    let current: Option<Option<i64>> = connection
        .query_row(
            "SELECT at_ms FROM __sys_work WHERE scope=?1 AND reason='sandbox-cleanup'",
            [scope],
            |row| row.get(0),
        )
        .optional()?;
    if due.is_none()
        && super::sandbox_catalog::read_parent(connection)?
            .is_some_and(|parent| parent.state == SandboxParentState::Active)
        && connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_sandbox_managed LIMIT 1)",
            [],
            |row| row.get::<_, bool>(0),
        )?
    {
        // Persist discovery even while the application is idle. A future
        // deployment can remove this parent without erasing its obligations.
        let now = crate::asyncrt::wall_ms().max(0);
        due = Some(
            current
                .flatten()
                .filter(|at| *at > now)
                .unwrap_or(now.saturating_add(30_000)),
        );
    }
    if due != current.flatten() {
        if let Some(due) = due {
            super::work::set_in(connection, scope, Reason::SandboxCleanup, due)?;
        } else {
            connection.execute(
                "UPDATE __sys_work SET at_ms=NULL WHERE scope=?1 AND reason='sandbox-cleanup'",
                [scope],
            )?;
        }
    }
    Ok(())
}

pub(super) fn changed(
    connection: &Connection,
    scope: &str,
    allocation: &SandboxAllocation,
) -> Result<()> {
    if !SCHEMA.check(connection)? {
        return Ok(());
    }
    if allocation.state == SandboxAllocationState::Deleted {
        connection.execute(
            "DELETE FROM __sys_sandbox_managed WHERE name=?1 AND generation=?2",
            rusqlite::params![allocation.name, allocation.generation],
        )?;
    }
    rearm(connection, scope)
}

pub(super) fn check_schema(connection: &Connection) -> Result<()> {
    if !SCHEMA.check(connection)? {
        return Ok(());
    }
    let index: String = connection.query_row(
        "SELECT sql FROM sqlite_schema WHERE type='index' AND name='__sys_sandbox_managed_due'",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        index == "CREATE INDEX __sys_sandbox_managed_due ON __sys_sandbox_managed(due_ms,name)",
        "sandbox cleanup deadline index differs"
    );
    let invalid: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_sandbox_managed WHERE typeof(name)!='text' OR length(CAST(name AS BLOB)) NOT BETWEEN 1 AND 128 OR typeof(profile)!='text' OR length(CAST(profile AS BLOB))!=64 OR typeof(generation)!='integer' OR generation<1 OR typeof(due_ms)!='integer' OR due_ms<0)",[],|row|row.get(0))?;
    ensure!(!invalid, "invalid sandbox cleanup storage");
    supported_work(connection)?;
    let parent = super::sandbox_catalog::read_parent(connection)?
        .ok_or_else(|| anyhow::anyhow!("managed sandbox parent missing"))?;
    let mut statement =
        connection.prepare("SELECT name,generation,profile,due_ms FROM __sys_sandbox_managed")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        let generation: u64 = row.get(1)?;
        let profile: String = row.get(2)?;
        let due: i64 = row.get(3)?;
        ensure!(
            crate::application::sandbox_catalog::values::hex(&profile, 64) && due >= 0,
            "invalid sandbox cleanup registration"
        );
        let allocation = super::sandbox_catalog::read(connection, &parent, &name)?
            .ok_or_else(|| anyhow::anyhow!("managed sandbox allocation missing"))?;
        ensure!(
            allocation.generation == generation
                && allocation.state != SandboxAllocationState::Deleted,
            "sandbox cleanup generation differs"
        );
    }
    Ok(())
}

pub(super) fn register_in(
    connection: &Connection,
    scope: &str,
    allocation: &SandboxAllocation,
    profile: &str,
) -> Result<()> {
    ensure!(
        crate::application::sandbox_catalog::values::hex(profile, 64),
        "invalid sandbox executor configuration digest"
    );
    supported_work(connection)?;
    SCHEMA.open(connection)?;
    let prior: Option<(u64, String)> = connection
        .query_row(
            "SELECT generation,profile FROM __sys_sandbox_managed WHERE name=?1",
            [&allocation.name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((generation, configured)) = prior {
        ensure!(
            generation == allocation.generation && configured == profile,
            "managed sandbox generation or executor configuration changed"
        );
    } else {
        connection.execute(
            "INSERT INTO __sys_sandbox_managed VALUES(?1,?2,?3,0)",
            rusqlite::params![allocation.name, allocation.generation, profile],
        )?;
    }
    rearm(connection, scope)
}

pub(crate) fn check_registration(
    scope: &str,
    allocation: &SandboxAllocation,
    profile: &str,
) -> Result<()> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |connection| {
        as_framework_sql(|| {
            ensure!(
                SCHEMA.check(connection)?,
                "sandbox is not managed by a registered executor"
            );
            let prior: (u64, String) = connection.query_row(
                "SELECT generation,profile FROM __sys_sandbox_managed WHERE name=?1",
                [&allocation.name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            ensure!(
                prior == (allocation.generation, profile.into()),
                "sandbox executor configuration changed"
            );
            Ok(())
        })
    })
    .ok_or_else(|| anyhow::anyhow!("sandbox parent storage is not local"))?
}

pub(super) fn claim_at(
    connection: &Connection,
    scope: &str,
    now: i64,
) -> Result<Option<SandboxAllocation>> {
    ensure!(now >= 0, "sandbox cleanup clock must be nonnegative");
    let Some((name, generation, _, due)) = next(connection)? else {
        return Ok(None);
    };
    if due > now {
        return Ok(None);
    }
    let parent = super::sandbox_catalog::read_parent(connection)?
        .ok_or_else(|| anyhow::anyhow!("sandbox parent missing"))?;
    let allocation = super::sandbox_catalog::read(connection, &parent, &name)?
        .ok_or_else(|| anyhow::anyhow!("sandbox allocation missing"))?;
    ensure!(
        allocation.generation == generation,
        "sandbox cleanup generation changed"
    );
    let retry = now
        .checked_add(30_000)
        .ok_or_else(|| anyhow::anyhow!("sandbox cleanup deadline exhausted"))?;
    connection.execute(
        "UPDATE __sys_sandbox_managed SET due_ms=?1 WHERE name=?2 AND generation=?3",
        rusqlite::params![retry, name, generation],
    )?;
    rearm(connection, scope)?;
    Ok(Some(allocation))
}

pub(crate) fn claim(scope: &str, now: i64) -> Result<Option<SandboxAllocation>> {
    with_application_storage_write(scope, |connection| {
        super::sandbox_catalog::transaction(connection, |connection| {
            claim_at(connection, scope, now)
        })
    })
}

pub(crate) fn refresh(scope: &str) -> Result<()> {
    with_application_storage_write(scope, |connection| {
        super::sandbox_catalog::transaction(connection, |connection| rearm(connection, scope))
    })
}

pub(crate) fn publish(scope: &str) -> Option<i64> {
    super::publish_alarm(scope)
}
