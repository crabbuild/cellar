// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Monotonic provider execution claims, copied with the owning cell image.
use super::*;
use crate::application::containers::{BindingCheckpoint, MAX_BINDINGS};

#[derive(Clone, Copy)]
pub(crate) enum ClaimPurpose {
    Execute,
    ConfirmRetired,
}

const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_container_execution_schema",
    ddl: include_str!("container_execution.sql"),
    required: &[(
        "__sys_container_execution",
        "binding,sequence,generation,binding_revision,owner",
    )],
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Claim {
    pub sequence: u64,
    pub generation: u64,
    pub binding_revision: u64,
    pub owner: String,
}

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<()> {
    if !SCHEMA.check(connection)? {
        return Ok(());
    }
    let count: usize = connection.query_row(
        "SELECT count(*) FROM __sys_container_execution",
        [],
        |row| row.get(0),
    )?;
    event_schema::compatible(
        count <= MAX_BINDINGS,
        "provider execution claim count exceeds limit",
    )?;
    let mut statement = connection.prepare(
        "SELECT binding,sequence,generation,binding_revision,owner FROM __sys_container_execution",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let binding: String = row.get(0)?;
        super::container_bindings::validate_name(&binding)?;
        let sequence: u64 = row.get(1)?;
        let generation: u64 = row.get(2)?;
        let binding_revision: u64 = row.get(3)?;
        let owner: String = row.get(4)?;
        validate_owner(&owner)?;
        event_schema::compatible(
            sequence > 0 && generation > 0 && binding_revision > 0,
            "invalid provider execution claim",
        )?;
        let record = super::container_bindings::read(connection, &binding)?;
        event_schema::compatible(
            record.is_some_and(|record| {
                record.record.generation >= generation && record.revision >= binding_revision
            }),
            "provider execution claim has no matching binding generation",
        )?;
    }
    Ok(())
}

fn validate_owner(owner: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        owner.len() == 64
            && owner
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid provider execution owner"
    );
    Ok(())
}

fn read(connection: &Connection, binding: &str) -> anyhow::Result<Option<Claim>> {
    if !SCHEMA.check(connection)? {
        return Ok(None);
    }
    Ok(connection
        .query_row(
            "SELECT sequence,generation,binding_revision,owner FROM __sys_container_execution WHERE binding=?1",
            [binding],
            |row| {
                Ok(Claim {
                    sequence: row.get(0)?,
                    generation: row.get(1)?,
                    binding_revision: row.get(2)?,
                    owner: row.get(3)?,
                })
            },
        )
        .optional()?)
}

fn reserve(
    connection: &Connection,
    binding: &str,
    expected: &BindingCheckpoint,
    owner: &str,
) -> anyhow::Result<Claim> {
    reserve_for(connection, binding, expected, owner, ClaimPurpose::Execute)
}

fn reserve_for(
    connection: &Connection,
    binding: &str,
    expected: &BindingCheckpoint,
    owner: &str,
    purpose: ClaimPurpose,
) -> anyhow::Result<Claim> {
    super::container_bindings::validate_name(binding)?;
    validate_owner(owner)?;
    anyhow::ensure!(
        super::container_bindings::read(connection, binding)?.as_ref() == Some(expected)
            && expected.record.is_retired() == matches!(purpose, ClaimPurpose::ConfirmRetired),
        "provider binding changed before execution claim"
    );
    let previous = read(connection, binding)?;
    if let Some(previous) = previous.as_ref().filter(|previous| {
        previous.owner == owner
            && previous.generation == expected.record.generation
            && previous.binding_revision == expected.revision
    }) {
        return Ok(previous.clone());
    }
    let claim = Claim {
        sequence: previous
            .map_or(0, |claim| claim.sequence)
            .checked_add(1)
            .filter(|sequence| *sequence <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("provider execution sequence exhausted"))?,
        generation: expected.record.generation,
        binding_revision: expected.revision,
        owner: owner.into(),
    };
    SCHEMA.open(connection)?;
    connection.execute(
        "INSERT INTO __sys_container_execution(binding,sequence,generation,binding_revision,owner) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(binding) DO UPDATE SET sequence=excluded.sequence,generation=excluded.generation,binding_revision=excluded.binding_revision,owner=excluded.owner",
        rusqlite::params![binding, claim.sequence, claim.generation, claim.binding_revision, claim.owner],
    )?;
    Ok(claim)
}

pub(crate) fn claim(
    scope: &str,
    binding: &str,
    expected: &BindingCheckpoint,
    owner: &str,
) -> anyhow::Result<Claim> {
    with_application_storage_write(scope, |connection| {
        as_framework_sql(|| reserve(connection, binding, expected, owner))
    })
}

pub(crate) fn claim_retirement(
    scope: &str,
    binding: &str,
    expected: &BindingCheckpoint,
    owner: &str,
) -> anyhow::Result<Claim> {
    anyhow::ensure!(
        scope == RAFT_STORAGE_SCOPE,
        "retirement fencing requires shared storage"
    );
    with_application_storage_write(scope, |connection| {
        as_framework_sql(|| {
            reserve_for(
                connection,
                binding,
                expected,
                owner,
                ClaimPurpose::ConfirmRetired,
            )
        })
    })
}

pub(crate) fn check(scope: &str, binding: &str, claim: &Claim) -> anyhow::Result<()> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |connection| {
        as_framework_sql(|| {
            anyhow::ensure!(
                read(connection, binding)?.as_ref() == Some(claim),
                "provider execution claim has been superseded"
            );
            Ok(())
        })
    })
    .ok_or_else(|| anyhow::anyhow!("cell storage is not local"))?
}

#[cfg(test)]
#[path = "container_execution_tests.rs"]
mod tests;
