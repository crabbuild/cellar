// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::runtime::sandbox_execution::{Identity, Phase};

fn record() -> Record {
    Record {
        identity: Identity {
            parent_scope: "Parent:one".into(),
            parent_incarnation: "a".repeat(32),
            name: "build".into(),
            generation: 1,
            execution_scope: "Child:one".into(),
            parameters_digest: "b".repeat(64),
            configuration_digest: "c".repeat(64),
        },
        phase: Phase::Unstarted,
    }
}

fn write(connection: &Connection, record: &Record) -> anyhow::Result<()> {
    super::super::sandbox_catalog::transaction(connection, |connection| {
        save_in(connection, record, &serde_json::to_string(record)?)
    })
}

#[test]
fn deletion_before_start_tombstones_identity_and_rejects_delayed_commands() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let mut original = record();
    write(&connection, &original)?;
    original.phase = Phase::Deleting;
    write(&connection, &original)?;
    original.phase = Phase::Deleted;
    write(&connection, &original)?;
    for phase in [
        Phase::Unstarted,
        Phase::Starting,
        Phase::Ready,
        Phase::Deleting,
    ] {
        assert!(write(
            &connection,
            &Record {
                phase,
                ..original.clone()
            }
        )
        .is_err());
        assert_eq!(read(&connection)?, Some(original.clone()));
    }
    let mut foreign = original.clone();
    foreign.identity.generation += 1;
    assert!(write(&connection, &foreign).is_err());
    foreign = original.clone();
    foreign.identity.parent_incarnation = "d".repeat(32);
    assert!(write(&connection, &foreign).is_err());
    foreign = original.clone();
    foreign.identity.configuration_digest = "d".repeat(64);
    assert!(write(&connection, &foreign).is_err());
    write(&connection, &original)?;
    Ok(())
}

#[test]
fn creation_intent_precedes_ready_and_corrupt_child_prevents_recovery() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let mut original = record();
    write(&connection, &original)?;
    assert!(write(
        &connection,
        &Record {
            phase: Phase::Ready,
            ..original.clone()
        }
    )
    .is_err());
    original.phase = Phase::Starting;
    write(&connection, &original)?;
    // Only the executor can assert this transition after proven pre-effect refusal.
    original.phase = Phase::Unstarted;
    write(&connection, &original)?;
    original.phase = Phase::Starting;
    write(&connection, &original)?;
    original.phase = Phase::Ready;
    write(&connection, &original)?;
    assert!(write(&connection, &record()).is_err());
    connection.execute("UPDATE __sys_sandbox_child SET record='{}'", [])?;
    assert!(check_schema(&connection).is_err());
    Ok(())
}
