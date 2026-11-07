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
