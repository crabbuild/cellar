// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::budget::Budget;
use cellar_logic::{cell_group::*, consensus::*};
use rusqlite::types::Value;

fn scan_operation(operation: Value) -> Result<()> {
    let database = rusqlite::Connection::open_in_memory()?;
    database.execute_batch(
        "CREATE TABLE __sys_raft_clients(client_id BLOB, last_sequence BLOB);
         CREATE TABLE __sys_raft_receipts(client_id BLOB, sequence BLOB, operation);",
    )?;
    let history = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:one")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let client = super::super::system_client(&history)?;
    let sequence = 1_u64.to_be_bytes();
    database.execute(
        "INSERT INTO __sys_raft_clients VALUES(?1, ?2)",
        rusqlite::params![client.bytes().as_slice(), sequence.as_slice()],
    )?;
    database.execute(
        "INSERT INTO __sys_raft_receipts VALUES(?1, ?2, ?3)",
        rusqlite::params![client.bytes().as_slice(), sequence.as_slice(), operation],
    )?;
    let memory = Budget::new(64 * 1024, 1);
    let result = scan_connection_fair(
        &database,
        &history,
        &RaftClass::new("Counter", 1, [4; 32])?,
        1,
        memory.acquire(64 * 1024).unwrap(),
    );
    match result {
        Ok(scan) => {
            assert_eq!(scan.next, CommandId::new(client, 2)?);
            assert!(scan.due.is_none());
            drop(scan);
            assert_eq!(memory.usage(), (0, 0));
            Ok(())
        }
        Err(error) => {
            assert_eq!(memory.usage(), (0, 0));
            Err(error)
        }
    }
}

#[test]
fn previous_private_operation_is_bounded_before_allocation() -> Result<()> {
    scan_operation(Value::Text("__sys_raft_alarm_Wake".into()))?;
    scan_operation(Value::Text("x".repeat(128)))?;
    for invalid in [
        Value::Null,
        Value::Integer(1),
        Value::Blob(vec![1; 128]),
        Value::Text(String::new()),
        Value::Text("x".repeat(129)),
    ] {
        assert!(scan_operation(invalid).is_err());
    }
    Ok(())
}

fn allocator_scope() -> Result<CellHistory> {
    Ok(CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:allocator")?,
        incarnation: IncarnationId::new([3; 16])?,
    })
}

#[test]
fn system_allocator_reads_no_pending_payload_or_receipt() -> Result<()> {
    let database = rusqlite::Connection::open_in_memory()?;
    database.execute_batch(
        "CREATE TABLE __sys_raft_clients(client_id BLOB PRIMARY KEY,last_sequence BLOB);
        CREATE TABLE __sys_raft_receipts(body BLOB);
        CREATE TABLE __sys_raft_binding_intents(body BLOB);
        INSERT INTO __sys_raft_receipts VALUES(zeroblob(1048576));
        INSERT INTO __sys_raft_binding_intents VALUES(zeroblob(1048576));",
    )?;
    let scope = allocator_scope()?;
    let client = super::super::system_client(&scope)?;
    assert_eq!(next_command(&database, &scope)?, CommandId::new(client, 1)?);
    database.execute(
        "INSERT INTO __sys_raft_clients VALUES(?1,?2)",
        rusqlite::params![client.bytes().as_slice(), 9_u64.to_be_bytes().as_slice()],
    )?;
    database.authorizer(Some(|context: rusqlite::hooks::AuthContext<'_>| {
        if matches!(
            context.action,
            rusqlite::hooks::AuthAction::Read {
                table_name: "__sys_raft_receipts" | "__sys_raft_binding_intents",
                ..
            }
        ) {
            rusqlite::hooks::Authorization::Deny
        } else {
            rusqlite::hooks::Authorization::Allow
        }
    }));
    assert_eq!(
        next_command(&database, &scope)?,
        CommandId::new(client, 10)?
    );
    let fresh = CellHistory {
        incarnation: IncarnationId::new([4; 16])?,
        ..scope
    };
    assert_eq!(
        next_command(&database, &fresh)?,
        CommandId::new(super::super::system_client(&fresh)?, 1)?
    );
    assert!(database
        .query_row("SELECT body FROM __sys_raft_binding_intents", [], |row| row
            .get::<_, Vec<u8>>(0))
        .is_err());
    Ok(())
}

#[test]
fn system_allocator_rejects_malformed_or_exhausted_counter_without_reset() -> Result<()> {
    let database = rusqlite::Connection::open_in_memory()?;
    database.execute_batch(
        "CREATE TABLE __sys_raft_clients(client_id BLOB PRIMARY KEY,last_sequence)",
    )?;
    let scope = allocator_scope()?;
    let client = super::super::system_client(&scope)?;
    for value in [
        Value::Null,
        Value::Integer(1),
        Value::Text("12345678".into()),
        Value::Blob(vec![1; 7]),
        Value::Blob(vec![1; 1024 * 1024]),
    ] {
        database.execute(
            "INSERT OR REPLACE INTO __sys_raft_clients VALUES(?1,?2)",
            rusqlite::params![client.bytes().as_slice(), value],
        )?;
        assert!(next_command(&database, &scope).is_err());
    }
    database.execute(
        "UPDATE __sys_raft_clients SET last_sequence=?1",
        [u64::MAX.to_be_bytes().as_slice()],
    )?;
    assert!(matches!(
        next_command(&database, &scope)
            .unwrap_err()
            .downcast_ref::<CommandFailure>(),
        Some(CommandFailure::Overloaded)
    ));
    assert_eq!(
        database.query_row("SELECT last_sequence FROM __sys_raft_clients", [], |row| {
            row.get::<_, Vec<u8>>(0)
        })?,
        u64::MAX.to_be_bytes()
    );
    Ok(())
}
