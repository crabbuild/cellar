// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

const PARENT: &str = "Business:alice";

fn target(name: &str) -> String {
    format!("Sandbox:{}", hex::encode(Sha256::digest(name.as_bytes())))
}

fn reserve_child(connection: &Connection, name: &str) -> Result<SandboxReservation> {
    transaction(connection, |connection| {
        Ok(reserve_at(
            connection,
            PARENT,
            &"a".repeat(32),
            name,
            "SANDBOXES",
            &"b".repeat(64),
            &target,
        )?
        .1)
    })
}

fn transition_child(
    connection: &Connection,
    expected: &SandboxAllocation,
    target: SandboxAllocationState,
) -> Result<SandboxAllocation> {
    transaction(connection, |connection| {
        Ok(transition_at(connection, PARENT, expected, target)?.1)
    })
}

#[test]
fn names_retain_identity_and_uncertain_reservations_until_exact_retirement() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let first = reserve_child(&connection, "build")?;
    assert!(first.fresh);
    let repeat = reserve_child(&connection, "build")?;
    assert!(!repeat.fresh);
    assert_eq!(repeat.allocation, first.allocation);
    assert!(transaction(&connection, |connection| reserve_at(
        connection,
        PARENT,
        &"a".repeat(32),
        "build",
        "OTHER",
        &"b".repeat(64),
        &target
    ))
    .is_err());
    assert!(transaction(&connection, |connection| reserve_at(
        connection,
        PARENT,
        &"a".repeat(32),
        "build",
        "SANDBOXES",
        &"c".repeat(64),
        &target
    ))
    .is_err());
    assert!(transaction(&connection, |connection| reserve_at(
        connection,
        PARENT,
        &"a".repeat(32),
        "build",
        "SANDBOXES",
        &"b".repeat(64),
        &|_| "Other:changed".into()
    ))
    .is_err());
    assert!(transition_child(
        &connection,
        &first.allocation,
        SandboxAllocationState::Deleted
    )
    .is_err());
    let deleting = transition_child(
        &connection,
        &first.allocation,
        SandboxAllocationState::Deleting,
    )?;
    assert!(reserve_child(&connection, "build").is_err());
    assert!(transition_child(
        &connection,
        &first.allocation,
        SandboxAllocationState::Ready
    )
    .is_err());
    let deleted = transition_child(&connection, &deleting, SandboxAllocationState::Deleted)?;
    let replacement = reserve_child(&connection, "build")?;
    assert!(replacement.fresh);
    assert_eq!(replacement.allocation.generation, 2);
    assert_eq!(replacement.allocation.revision, deleted.revision + 1);
    assert_ne!(
        first.allocation.execution_scope,
        replacement.allocation.execution_scope
    );
    assert!(transition_child(&connection, &deleting, SandboxAllocationState::Deleted).is_err());
    Ok(())
}

#[test]
fn retirement_closes_admission_and_pages_every_unconfirmed_child() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    // Exceeds the static provider binding limit; no per-parent allocation cap.
    for n in 0..1025 {
        reserve_child(&connection, &format!("sandbox-{n:04}"))?;
    }
    let parent = read_parent(&connection)?.unwrap();
    assert!(page_at(&connection, &parent, None, 17, true)?
        .allocations
        .is_empty());
    let initial = read(&connection, &parent, "sandbox-0000")?.unwrap();
    let ready = transition_child(&connection, &initial, SandboxAllocationState::Ready)?;
    let deleting = transition_child(&connection, &ready, SandboxAllocationState::Deleting)?;
    assert_eq!(
        page_at(&connection, &parent, None, 17, true)?.allocations,
        vec![deleting]
    );
    let second = read(&connection, &parent, "sandbox-0001")?.unwrap();
    let second = transition_child(&connection, &second, SandboxAllocationState::Ready)?;
    let retired = transaction(&connection, |connection| {
        retire_at(connection, PARENT, &"a".repeat(32), false)
    })?;
    assert_eq!(retired.state, SandboxParentState::Deleting);
    assert!(transition_child(&connection, &second, SandboxAllocationState::Ready).is_err());
    assert!(reserve_child(&connection, "late").is_err());
    assert!(transition_child(&connection, &ready, SandboxAllocationState::Ready).is_err());
    assert!(transaction(&connection, |connection| retire_at(
        connection,
        PARENT,
        &"a".repeat(32),
        true
    ))
    .is_err());
    let mut cursor = None;
    let mut count = 0;
    loop {
        let page = page_at(&connection, &retired, cursor.as_deref(), 17, true)?;
        assert!(page.allocations.len() <= 17);
        for allocation in page.allocations {
            let deleting =
                transition_child(&connection, &allocation, SandboxAllocationState::Deleting)?;
            transition_child(&connection, &deleting, SandboxAllocationState::Deleted)?;
            count += 1;
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(count, 1025);
    assert!(page_at(&connection, &retired, None, 17, true)?
        .allocations
        .is_empty());
    let deleted = transaction(&connection, |connection| {
        retire_at(connection, PARENT, &"a".repeat(32), true)
    })?;
    assert_eq!(deleted.state, SandboxParentState::Deleted);
    assert_eq!(
        transaction(&connection, |connection| retire_at(
            connection,
            PARENT,
            &"a".repeat(32),
            false
        ))?,
        deleted
    );
    assert!(reserve_child(&connection, "build").is_err());
    check_schema(&connection)?;
    Ok(())
}

#[test]
fn parent_incarnations_and_full_checkpoints_refuse_foreign_or_forged_cleanup() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let first = reserve_child(&connection, "same")?.allocation;
    let mut forged = first.clone();
    forged.execution_scope = "Sandbox:foreign".into();
    assert!(transition_child(&connection, &forged, SandboxAllocationState::Deleting).is_err());
    let mut forged = first.clone();
    forged.parent_incarnation = "c".repeat(32);
    assert!(transition_child(&connection, &forged, SandboxAllocationState::Deleting).is_err());
    assert!(transaction(&connection, |connection| transition_at(
        connection,
        "Business:bob",
        &first,
        SandboxAllocationState::Deleting
    ))
    .is_err());
    for (scope, incarnation) in [("Business:bob", "a".repeat(32)), (PARENT, "c".repeat(32))] {
        let other = Connection::open_in_memory()?;
        let (_, reservation) = transaction(&other, |connection| {
            reserve_at(
                connection,
                scope,
                &incarnation,
                "same",
                "SANDBOXES",
                &"b".repeat(64),
                &target,
            )
        })?;
        assert_ne!(
            reservation.allocation.execution_scope,
            first.execution_scope
        );
    }
    Ok(())
}

#[test]
fn corrupt_or_incomplete_catalog_refuses_activation_without_schema_mutation() -> Result<()> {
    for mutation in [
        "DROP TABLE __sys_sandbox_schema",
        "UPDATE __sys_sandbox_schema SET version=2",
        "DROP INDEX __sys_sandbox_cleanup",
        "UPDATE __sys_sandbox_allocations SET state='deleted'",
        "UPDATE __sys_sandbox_allocations SET record='{}'",
        "DELETE FROM __sys_sandbox_parent",
        "ALTER TABLE __sys_sandbox_allocations ADD COLUMN legacy TEXT",
    ] {
        let connection = Connection::open_in_memory()?;
        reserve_child(&connection, "build")?;
        connection.execute_batch(mutation)?;
        let before: i64 = connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        assert!(super::super::schema(&connection).is_err(), "{mutation}");
        assert_eq!(
            connection.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))?,
            before
        );
    }
    Ok(())
}

#[test]
fn reserved_sql_is_protected_and_failed_writes_roll_back_initialization() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    assert!(transaction(&connection, |connection| retire_at(
        connection,
        PARENT,
        &"a".repeat(32),
        true
    ))
    .is_err());
    assert!(!SCHEMA.check(&connection)?);
    let first = reserve_child(&connection, "build")?.allocation;
    connection.authorizer(Some(authorize_sql))?;
    for query in [
        "SELECT * FROM __sys_sandbox_parent",
        "DELETE FROM __sys_sandbox_allocations",
        "DROP TABLE __sys_sandbox_allocations",
        "UPDATE __sys_sandbox_parent SET record='{}'",
    ] {
        assert!(as_user_sql(|| connection.execute_batch(query)).is_err());
    }
    as_storage_transaction(|| connection.execute_batch("BEGIN;"))?;
    reserve_child(&connection, "rolled-back")?;
    as_storage_transaction(|| connection.execute_batch("ROLLBACK;"))?;
    let parent = read_parent(&connection)?.unwrap();
    assert!(read(&connection, &parent, "rolled-back")?.is_none());
    connection.execute_batch("CREATE TRIGGER reject_sandbox BEFORE INSERT ON __sys_sandbox_allocations BEGIN SELECT RAISE(ROLLBACK,'allocation interrupted'); END;")?;
    let error = reserve_child(&connection, "rejected").unwrap_err();
    assert!(error.to_string().contains("allocation interrupted"));
    assert!(error.downcast_ref::<rusqlite::Error>().is_some());
    assert!(connection.is_autocommit());
    assert_eq!(read(&connection, &parent, "build")?, Some(first));
    Ok(())
}

#[test]
fn bounded_pages_use_cleanup_indexes_and_validate_limits() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    reserve_child(&connection, "build")?;
    for (condition, index) in [
        ("state!='deleted'", "__sys_sandbox_cleanup"),
        ("state='deleting'", "__sys_sandbox_deleting"),
    ] {
        let query = format!("EXPLAIN QUERY PLAN SELECT name,state,execution_scope,record FROM __sys_sandbox_allocations WHERE name>?1 AND {condition} ORDER BY name LIMIT ?2");
        let mut statement = connection.prepare(&query)?;
        let details = statement
            .query_map(rusqlite::params!["", 17], |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .join(" ");
        assert!(details.contains(index), "{details}");
    }
    assert!(page(None, 0).is_err());
    assert!(page(None, 257).is_err());
    assert!(page(Some("../escape"), 17).is_err());
    assert!(page(None, 256).is_ok());
    Ok(())
}
