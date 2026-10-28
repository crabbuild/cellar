// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Parent-owned named sandbox identities. No fixed allocation count or resident map.
use super::*;
use crate::application::sandbox_catalog::values::*;
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_sandbox_schema",
    ddl: include_str!("sandbox_catalog.sql"),
    required: &[
        ("__sys_sandbox_parent", "singleton,record"),
        (
            "__sys_sandbox_allocations",
            "name,state,execution_scope,record",
        ),
    ],
};

fn state(value: SandboxAllocationState) -> &'static str {
    match value {
        SandboxAllocationState::Reserved => "reserved",
        SandboxAllocationState::Ready => "ready",
        SandboxAllocationState::Deleting => "deleting",
        SandboxAllocationState::Deleted => "deleted",
    }
}

fn next(value: u64) -> Result<u64> {
    value
        .checked_add(1)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("sandbox revision exhausted"))
}

fn execution_name(parent: &SandboxParent, name: &str, generation: u64) -> Result<String> {
    let identity = serde_json::to_vec(&(
        "cellar-sandbox-v1",
        &parent.scope,
        &parent.incarnation,
        name,
        generation,
    ))?;
    Ok(format!("sandbox-{}", hex::encode(Sha256::digest(identity))))
}

fn encode(value: &impl serde::Serialize) -> Result<String> {
    let encoded = serde_json::to_string(value)?;
    ensure!(encoded.len() <= 4096, "sandbox record exceeds byte limit");
    Ok(encoded)
}

fn read_parent(connection: &Connection) -> Result<Option<SandboxParent>> {
    if !SCHEMA.check(connection)? {
        return Ok(None);
    }
    let encoded: Option<Option<String>> = connection
        .query_row(
            "SELECT CASE WHEN typeof(record)='text' AND length(CAST(record AS BLOB))<=4096 THEN record END FROM __sys_sandbox_parent WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let encoded = encoded
        .flatten()
        .ok_or_else(|| anyhow::anyhow!("sandbox parent record missing or invalid"))?;
    ensure!(encoded.len() <= 4096, "sandbox parent exceeds byte limit");
    let parent: SandboxParent = serde_json::from_str(&encoded)?;
    parent.validate()?;
    Ok(Some(parent))
}

fn write_parent(connection: &Connection, parent: &SandboxParent) -> Result<()> {
    parent.validate()?;
    connection.execute("INSERT INTO __sys_sandbox_parent(singleton,record) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET record=excluded.record", [encode(parent)?])?;
    Ok(())
}

fn initialize(connection: &Connection, scope: &str, incarnation: &str) -> Result<SandboxParent> {
    if let Some(parent) = read_parent(connection)? {
        ensure!(
            parent.scope == scope,
            "sandbox catalog belongs to another parent"
        );
        return Ok(parent);
    }
    SCHEMA.open(connection)?;
    let parent = SandboxParent {
        scope: scope.into(),
        incarnation: incarnation.into(),
        revision: 1,
        state: SandboxParentState::Active,
    };
    write_parent(connection, &parent)?;
    Ok(parent)
}

fn decode(
    parent: &SandboxParent,
    name: &str,
    column_state: &str,
    scope: &str,
    encoded: &str,
) -> Result<SandboxAllocation> {
    ensure!(
        encoded.len() <= 4096,
        "sandbox allocation exceeds byte limit"
    );
    let allocation: SandboxAllocation = serde_json::from_str(encoded)?;
    allocation.validate()?;
    ensure!(
        allocation.parent_incarnation == parent.incarnation
            && allocation.name == name
            && state(allocation.state) == column_state
            && allocation.execution_scope == scope
            && allocation.execution_name == execution_name(parent, name, allocation.generation)?,
        "sandbox allocation identity or index differs from retained record"
    );
    ensure!(
        parent.state != SandboxParentState::Deleted
            || allocation.state == SandboxAllocationState::Deleted,
        "retired sandbox parent retains a live child"
    );
    Ok(allocation)
}

fn read(
    connection: &Connection,
    parent: &SandboxParent,
    name: &str,
) -> Result<Option<SandboxAllocation>> {
    let row: Option<(String, String, String)> = connection
        .query_row(
            "SELECT state,execution_scope,record FROM __sys_sandbox_allocations WHERE name=?1",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    row.map(|(state, scope, record)| decode(parent, name, &state, &scope, &record))
        .transpose()
}

fn write(connection: &Connection, allocation: &SandboxAllocation) -> Result<()> {
    allocation.validate()?;
    connection.execute("INSERT INTO __sys_sandbox_allocations(name,state,execution_scope,record) VALUES(?1,?2,?3,?4) ON CONFLICT(name) DO UPDATE SET state=excluded.state,execution_scope=excluded.execution_scope,record=excluded.record", rusqlite::params![allocation.name,state(allocation.state),allocation.execution_scope,encode(allocation)?])?;
    Ok(())
}

pub(super) fn check_schema(connection: &Connection) -> Result<bool> {
    let Some(parent) = read_parent(connection)? else {
        return Ok(false);
    };
    let count: u64 =
        connection.query_row("SELECT count(*) FROM __sys_sandbox_parent", [], |row| {
            row.get(0)
        })?;
    ensure!(count == 1, "sandbox catalog must have exactly one parent");
    for (index, predicate) in [
        ("__sys_sandbox_cleanup", "state!='deleted'"),
        ("__sys_sandbox_deleting", "state='deleting'"),
    ] {
        let sql: String = connection.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?1",
            [index],
            |row| row.get(0),
        )?;
        ensure!(
            sql == format!(
                "CREATE INDEX {index} ON __sys_sandbox_allocations(name) WHERE {predicate}"
            ),
            "sandbox cleanup index differs from required layout"
        );
    }
    let invalid: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_sandbox_allocations WHERE typeof(name)!='text' OR length(CAST(name AS BLOB)) NOT BETWEEN 1 AND 128 OR typeof(state)!='text' OR length(state)>8 OR typeof(execution_scope)!='text' OR length(CAST(execution_scope AS BLOB))>255 OR typeof(record)!='text' OR length(CAST(record AS BLOB))>4096)", [], |row| row.get(0))?;
    ensure!(!invalid, "sandbox retained fields exceed format bounds");
    // Streaming validation, not an unbounded in-memory inventory.
    let mut statement = connection
        .prepare("SELECT name,state,execution_scope,record FROM __sys_sandbox_allocations")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        decode(
            &parent,
            &row.get::<_, String>(0)?,
            &row.get::<_, String>(1)?,
            &row.get::<_, String>(2)?,
            &row.get::<_, String>(3)?,
        )?;
    }
    Ok(true)
}

fn transaction<T>(
    connection: &Connection,
    effect: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    as_framework_sql(|| {
        as_storage_transaction(|| connection.execute_batch("SAVEPOINT cellar_sandbox_catalog;"))?;
        let result = effect(connection);
        let cleanup = if result.is_err() && connection.is_autocommit() {
            Ok(())
        } else {
            as_storage_transaction(|| {
                if result.is_err() {
                    connection.execute_batch("ROLLBACK TO cellar_sandbox_catalog;")?;
                }
                connection.execute_batch("RELEASE cellar_sandbox_catalog;")
            })
        };
        match (result, cleanup) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(cleanup)) => Err(cleanup.into()),
            (Err(original), Ok(())) => Err(original),
            (Err(original), Err(cleanup)) => {
                Err(original.context(format!("sandbox savepoint cleanup failed: {cleanup}")))
            }
        }
    })
}

fn access<T>(
    scope: &str,
    read: impl FnOnce(&Connection, Option<&SandboxParent>) -> Result<T>,
) -> Result<T> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |connection| {
        as_framework_sql(|| {
            let parent = read_parent(connection)?;
            ensure!(
                parent.as_ref().is_none_or(|parent| parent.scope == scope),
                "sandbox catalog belongs to another parent"
            );
            read(connection, parent.as_ref())
        })
    })
    .ok_or_else(|| anyhow::anyhow!("cell storage is not local"))?
}

pub(crate) fn parent(scope: &str) -> Result<Option<SandboxParent>> {
    access(scope, |_, parent| Ok(parent.cloned()))
}

pub(crate) fn load(scope: &str, name: &str) -> Result<Option<SandboxAllocation>> {
    access(scope, |connection, parent| match parent {
        Some(parent) => read(connection, parent, name),
        None => Ok(None),
    })
}

fn reserve_at(
    connection: &Connection,
    scope: &str,
    incarnation: &str,
    name: &str,
    binding: &str,
    digest: &str,
    execution_scope: &impl Fn(&str) -> String,
) -> Result<(SandboxParent, SandboxReservation)> {
    let parent = initialize(connection, scope, incarnation)?;
    ensure!(
        parent.state == SandboxParentState::Active,
        "sandbox parent is retiring; allocation admission is closed"
    );
    let prior = read(connection, &parent, name)?;
    if let Some(prior) = &prior {
        if prior.state != SandboxAllocationState::Deleted {
            ensure!(
                matches!(
                    prior.state,
                    SandboxAllocationState::Reserved | SandboxAllocationState::Ready
                ),
                "sandbox deletion is pending"
            );
            ensure!(
                prior.binding == binding
                    && prior.parameters_digest == digest
                    && execution_scope(&prior.execution_name) == prior.execution_scope,
                "sandbox name has different parameters or namespace configuration"
            );
            return Ok((
                parent,
                SandboxReservation {
                    allocation: prior.clone(),
                    fresh: false,
                },
            ));
        }
    }
    let generation = prior
        .as_ref()
        .map_or(Ok(1), |prior| next(prior.generation))?;
    let execution_name = execution_name(&parent, name, generation)?;
    let allocation = SandboxAllocation {
        parent_incarnation: parent.incarnation.clone(),
        name: name.into(),
        binding: binding.into(),
        parameters_digest: digest.into(),
        generation,
        revision: prior.as_ref().map_or(Ok(1), |prior| next(prior.revision))?,
        execution_scope: execution_scope(&execution_name),
        execution_name,
        state: SandboxAllocationState::Reserved,
    };
    write(connection, &allocation)?;
    Ok((
        parent,
        SandboxReservation {
            allocation,
            fresh: true,
        },
    ))
}

pub(crate) fn reserve(
    scope: &str,
    incarnation: &str,
    name: &str,
    binding: &str,
    digest: &str,
    execution_scope: &impl Fn(&str) -> String,
) -> Result<(SandboxParent, SandboxReservation)> {
    with_application_storage_write(scope, |connection| {
        transaction(connection, |connection| {
            reserve_at(
                connection,
                scope,
                incarnation,
                name,
                binding,
                digest,
                execution_scope,
            )
        })
    })
}

fn transition_at(
    connection: &Connection,
    scope: &str,
    expected: &SandboxAllocation,
    target: SandboxAllocationState,
) -> Result<(SandboxParent, SandboxAllocation)> {
    let parent =
        read_parent(connection)?.ok_or_else(|| anyhow::anyhow!("sandbox parent missing"))?;
    ensure!(
        parent.scope == scope,
        "sandbox catalog belongs to another parent"
    );
    ensure!(
        read(connection, &parent, &expected.name)?.as_ref() == Some(expected),
        "sandbox allocation revision or identity conflict"
    );
    ensure!(
        target != SandboxAllocationState::Ready || parent.state == SandboxParentState::Active,
        "sandbox parent admission is closed"
    );
    let allowed = match (expected.state, target) {
        (SandboxAllocationState::Reserved, SandboxAllocationState::Ready) => {
            parent.state == SandboxParentState::Active
        }
        (
            SandboxAllocationState::Reserved | SandboxAllocationState::Ready,
            SandboxAllocationState::Deleting,
        ) => true,
        (SandboxAllocationState::Deleting, SandboxAllocationState::Deleted) => true,
        (current, target) => current == target,
    };
    ensure!(allowed, "invalid sandbox allocation transition");
    if expected.state == target {
        return Ok((parent, expected.clone()));
    }
    let allocation = SandboxAllocation {
        revision: next(expected.revision)?,
        state: target,
        ..expected.clone()
    };
    write(connection, &allocation)?;
    Ok((parent, allocation))
}

pub(crate) fn transition(
    scope: &str,
    expected: &SandboxAllocation,
    target: SandboxAllocationState,
) -> Result<(SandboxParent, SandboxAllocation)> {
    with_application_storage_write(scope, |connection| {
        transaction(connection, |connection| {
            transition_at(connection, scope, expected, target)
        })
    })
}

fn retire_at(
    connection: &Connection,
    scope: &str,
    incarnation: &str,
    finish: bool,
) -> Result<SandboxParent> {
    let mut parent = initialize(connection, scope, incarnation)?;
    let target = if finish {
        ensure!(
            parent.state != SandboxParentState::Active,
            "sandbox parent admission must close before retirement"
        );
        let outstanding: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM __sys_sandbox_allocations INDEXED BY __sys_sandbox_cleanup WHERE state!='deleted' LIMIT 1)", [], |row| row.get(0))?;
        ensure!(!outstanding, "sandbox children still require cleanup");
        SandboxParentState::Deleted
    } else if parent.state == SandboxParentState::Deleted {
        SandboxParentState::Deleted
    } else {
        SandboxParentState::Deleting
    };
    if parent.state != target {
        parent.state = target;
        parent.revision = next(parent.revision)?;
        write_parent(connection, &parent)?;
    }
    Ok(parent)
}

pub(crate) fn retire(scope: &str, incarnation: &str, finish: bool) -> Result<SandboxParent> {
    with_application_storage_write(scope, |connection| {
        transaction(connection, |connection| {
            retire_at(connection, scope, incarnation, finish)
        })
    })
}

fn page_at(
    connection: &Connection,
    parent: &SandboxParent,
    after: Option<&str>,
    limit: usize,
    cleanup: bool,
) -> Result<SandboxAllocationPage> {
    let selection = if !cleanup {
        ""
    } else if parent.state == SandboxParentState::Active {
        " AND state='deleting'"
    } else {
        " AND state!='deleted'"
    };
    let query = format!("SELECT name,state,execution_scope,record FROM __sys_sandbox_allocations WHERE name>?1{selection} ORDER BY name LIMIT ?2");
    let mut statement = connection.prepare(&query)?;
    let mut rows = statement.query(rusqlite::params![after.unwrap_or(""), limit + 1])?;
    let mut allocations = Vec::with_capacity(limit + 1);
    while let Some(row) = rows.next()? {
        allocations.push(decode(
            parent,
            &row.get::<_, String>(0)?,
            &row.get::<_, String>(1)?,
            &row.get::<_, String>(2)?,
            &row.get::<_, String>(3)?,
        )?);
    }
    let next = if allocations.len() > limit {
        allocations.pop();
        allocations.last().map(|allocation| allocation.name.clone())
    } else {
        None
    };
    Ok(SandboxAllocationPage { allocations, next })
}

pub(crate) fn list(
    scope: &str,
    after: Option<&str>,
    limit: usize,
    cleanup: bool,
) -> Result<SandboxAllocationPage> {
    access(scope, |connection, parent| match parent {
        Some(parent) => page_at(connection, parent, after, limit, cleanup),
        None => Ok(SandboxAllocationPage {
            allocations: Vec::new(),
            next: None,
        }),
    })
}

#[cfg(test)]
#[path = "sandbox_catalog_tests.rs"]
mod tests;
