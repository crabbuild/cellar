//! Storage invariants for bounded automatic selection, including rollback and
//! retained latest acknowledgements. Uses real SQLite transactions.
#![allow(clippy::disallowed_methods)]
use super::super::execution::{CommandRequest, ExecutionLimits};
use super::*;
use cellar_logic::consensus::{CommandFailure, CommandId, Durability};

fn id(client: u8) -> ClientId {
    ClientId::new([client; 16]).unwrap()
}

fn request(client: u8, sequence: u64, deadline: u64) -> CommandRequest {
    CommandRequest {
        id: CommandId::new(id(client), sequence).unwrap(),
        operation: "Deliver".into(),
        contract_digest: [2; 32],
        request_digest: [3; 32],
        durability: Durability::Quorum,
        retry_until_ms: deadline,
    }
}

fn database(clients: &[u8]) -> anyhow::Result<Connection> {
    let connection = Connection::open_in_memory()?;
    receipts::install(&connection)?;
    for client in clients {
        for sequence in 1..=3 {
            receipts::record(
                &connection,
                &request(*client, sequence, 10),
                vec![sequence as u8],
            )?;
        }
    }
    Ok(connection)
}

fn apply(connection: &mut Connection, plan: &Plan, now: u64) -> anyhow::Result<()> {
    let transaction = connection.transaction()?;
    plan.apply(&transaction, now)?;
    transaction.commit()?;
    Ok(())
}

#[test]
fn contiguous_prefix_preserves_later_expiry_and_latest_receipt() -> anyhow::Result<()> {
    let mut connection = database(&[1])?;
    connection.execute(
        "UPDATE __sys_raft_receipts SET retry_until=?1 WHERE sequence=?2",
        params![
            30_u64.to_be_bytes().as_slice(),
            2_u64.to_be_bytes().as_slice()
        ],
    )?;
    let selected = plan(&connection, 20, ReceiptMaintenanceLimits::default(), None)?;
    assert_eq!(selected.rows_removed, 1);
    apply(&mut connection, &selected, 20)?;
    assert_eq!(receipts::client(&connection, id(1))?, Some((3, 1)));
    let selected = plan(&connection, 30, ReceiptMaintenanceLimits::default(), None)?;
    assert_eq!(selected.rows_removed, 0, "deadline is inclusive");
    let selected = plan(&connection, 31, ReceiptMaintenanceLimits::default(), None)?;
    assert_eq!(selected.rows_removed, 1);
    apply(&mut connection, &selected, 31)?;
    assert_eq!(receipts::client(&connection, id(1))?, Some((3, 2)));
    let selected = plan(
        &connection,
        u64::MAX,
        ReceiptMaintenanceLimits::default(),
        None,
    )?;
    assert_eq!(
        selected.rows_removed, 0,
        "latest acknowledgement must remain"
    );
    let receipt =
        receipts::preflight(&connection, &request(1, 3, 10), &ExecutionLimits::default())?.unwrap();
    assert_eq!(receipt.result(), &[3]);
    assert!(matches!(
        receipts::preflight(&connection, &request(1, 2, 10), &ExecutionLimits::default()),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    Ok(())
}

#[test]
fn bounded_cursor_reaches_cold_clients_behind_unexpired_client() -> anyhow::Result<()> {
    let mut connection = database(&[1, 2, 3, 4])?;
    connection.execute(
        "UPDATE __sys_raft_receipts SET retry_until=?1 WHERE client_id=?2",
        params![100_u64.to_be_bytes().as_slice(), id(1).bytes().as_slice()],
    )?;
    let limits = ReceiptMaintenanceLimits {
        max_clients_per_command: 1,
        max_rows_per_command: 1,
    };
    let mut cursor = None;
    for client in 1..=4 {
        let selected = plan(&connection, 20, limits, cursor)?;
        assert_eq!(selected.clients_scanned, 1);
        assert_eq!(selected.rows_scanned, 1);
        assert_eq!(selected.rows_removed, usize::from(client != 1));
        assert_eq!(selected.next_cursor, Some(id(client)));
        cursor = selected.next_cursor;
        apply(&mut connection, &selected, 20)?;
    }
    assert_eq!(receipts::client(&connection, id(1))?, Some((3, 0)));
    assert_eq!(receipts::client(&connection, id(4))?, Some((3, 1)));
    let selected = plan(&connection, 20, limits, cursor)?;
    assert_eq!(selected.next_cursor, Some(id(1)), "cursor wraps once");
    Ok(())
}

#[test]
fn stale_client_state_rolls_back_all_selected_prefixes() -> anyhow::Result<()> {
    let mut connection = database(&[1, 2])?;
    let selected = plan(&connection, 20, ReceiptMaintenanceLimits::default(), None)?;
    assert_eq!(selected.rows_removed, 4);
    receipts::record(&connection, &request(2, 4, 10), vec![4])?;
    {
        let transaction = connection.transaction()?;
        assert!(matches!(
            selected.apply(&transaction, 20),
            Err(ExecutionError::Refused(CommandFailure::CorruptState))
        ));
        // Drop rolls back the first prefix even though the second client failed.
    }
    assert_eq!(receipts::client(&connection, id(1))?, Some((3, 0)));
    assert_eq!(receipts::client(&connection, id(2))?, Some((4, 0)));
    assert_eq!(receipts::retained_count(&connection)?, 7);
    Ok(())
}

#[test]
fn maintenance_requires_same_clock_and_real_transaction() -> anyhow::Result<()> {
    let mut connection = database(&[1])?;
    let selected = plan(&connection, 20, ReceiptMaintenanceLimits::default(), None)?;
    assert!(matches!(
        selected.apply(&connection, 20),
        Err(ExecutionError::Invalid(_))
    ));
    let transaction = connection.transaction()?;
    assert!(matches!(
        selected.apply(&transaction, 10),
        Err(ExecutionError::Invalid(_))
    ));
    transaction.rollback()?;
    assert_eq!(receipts::retained_count(&connection)?, 3);
    Ok(())
}

#[test]
fn missing_contiguous_receipt_refuses_cleanup() -> anyhow::Result<()> {
    let connection = database(&[1])?;
    connection.execute(
        "DELETE FROM __sys_raft_receipts WHERE sequence=?1",
        [1_u64.to_be_bytes().as_slice()],
    )?;
    assert!(matches!(
        plan(&connection, 20, ReceiptMaintenanceLimits::default(), None),
        Err(ExecutionError::Refused(CommandFailure::CorruptState))
    ));
    assert_eq!(receipts::client(&connection, id(1))?, Some((3, 0)));
    Ok(())
}

#[test]
fn unsigned_sequence_floor_and_expiry_keep_full_precision() -> anyhow::Result<()> {
    let mut connection = database(&[])?;
    receipts::record(
        &connection,
        &request(1, u64::MAX - 1, u64::MAX - 1),
        vec![8],
    )?;
    receipts::record(&connection, &request(1, u64::MAX, u64::MAX), vec![9])?;
    connection.execute(
        "UPDATE __sys_raft_clients SET expired_through=?1",
        [(u64::MAX - 2).to_be_bytes().as_slice()],
    )?;
    let selected = plan(
        &connection,
        u64::MAX - 1,
        ReceiptMaintenanceLimits::default(),
        None,
    )?;
    assert_eq!(selected.rows_removed, 0);
    let selected = plan(
        &connection,
        u64::MAX,
        ReceiptMaintenanceLimits::default(),
        None,
    )?;
    assert_eq!(selected.rows_removed, 1);
    apply(&mut connection, &selected, u64::MAX)?;
    assert_eq!(
        receipts::client(&connection, id(1))?,
        Some((u64::MAX, u64::MAX - 1))
    );
    assert_eq!(receipts::retained_count(&connection)?, 1);
    Ok(())
}
