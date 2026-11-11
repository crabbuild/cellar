// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private receipt storage contracts exercised with real SQLite rows.
#![allow(clippy::disallowed_methods)]
use super::*;
use cellar_logic::consensus::CommandId;

fn request(sequence: u64, retry_until_ms: u64) -> CommandRequest {
    CommandRequest {
        id: CommandId::new(ClientId::new([1; 16]).unwrap(), sequence).unwrap(),
        operation: "Increment".into(),
        contract_digest: [2; 32],
        request_digest: [3; 32],
        durability: Durability::Quorum,
        retry_until_ms,
    }
}

#[test]
fn retry_deadline_is_inclusive() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    install(&connection)?;
    let command = request(1, 1000);
    record(&connection, &command, vec![7])?;
    assert_eq!(client(&connection, command.id.client)?, Some((1, 0)));
    assert_eq!(
        preflight(&connection, &command, &ExecutionLimits::default())?
            .expect("new receipt exists")
            .retry_until_ms(),
        1000
    );
    // New command admission accepts retry_until == now. Maintenance must not
    // invalidate the promised retry at that same instant.
    assert!(matches!(
        prune(&connection, command.id.client, 1, 1000),
        Err(ExecutionError::RetentionNotExpired)
    ));
    assert_eq!(
        preflight(&connection, &command, &ExecutionLimits::default())?
            .expect("receipt retained through its deadline")
            .result(),
        &[7]
    );
    prune(&connection, command.id.client, 1, 1001)?;
    assert!(matches!(
        preflight(&connection, &command, &ExecutionLimits::default()),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    Ok(())
}

#[test]
fn maximum_retry_deadline_is_never_expired() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    install(&connection)?;
    let command = request(1, u64::MAX);
    record(&connection, &command, vec![9])?;
    assert!(matches!(
        prune(&connection, command.id.client, 1, u64::MAX),
        Err(ExecutionError::RetentionNotExpired)
    ));
    assert_eq!(client(&connection, command.id.client)?, Some((1, 0)));
    Ok(())
}

#[test]
fn committed_lookup_does_not_admit_new_client_at_capacity() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    install(&connection)?;
    let limits = ExecutionLimits {
        max_client_identities: 1,
        max_retained_receipts: 1,
        ..Default::default()
    };
    let retained = request(1, 1000);
    record(&connection, &retained, vec![7])?;
    let mut absent = request(1, 1000);
    absent.id = CommandId::new(ClientId::new([9; 16])?, 1)?;
    assert!(lookup_identity(&connection, &absent, &limits)?.is_none());
    assert!(matches!(
        preflight_identity(&connection, &absent, &limits),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(
        lookup_identity(&connection, &retained, &limits)?
            .unwrap()
            .result(),
        &[7]
    );
    // Existing client admission remains independent from the new-client bound;
    // the separate retained-receipt ceiling still refuses a new command.
    assert!(preflight_identity(&connection, &request(2, 1000), &limits)?.is_none());
    assert!(matches!(
        preflight(&connection, &request(2, 1000), &limits),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(retained_count(&connection)?, 1);
    assert!(client(&connection, absent.id.client)?.is_none());
    Ok(())
}

#[test]
fn committed_lookup_preserves_identity_floor_and_allocation_checks() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    install(&connection)?;
    let limits = ExecutionLimits::default();
    let original = request(1, 1000);
    record(&connection, &original, vec![7, 8])?;
    let mut conflict = request(1, 1000);
    conflict.request_digest = [99; 32];
    assert!(matches!(
        lookup_identity(&connection, &conflict, &limits),
        Err(ExecutionError::Refused(CommandFailure::CommandConflict))
    ));
    assert!(matches!(
        lookup_identity(&connection, &request(3, 1000), &limits),
        Err(ExecutionError::Refused(CommandFailure::SequenceGap {
            next: 2
        }))
    ));
    let bounded = ExecutionLimits {
        max_result_bytes: 1,
        ..limits
    };
    assert!(matches!(
        lookup_identity(&connection, &original, &bounded),
        Err(ExecutionError::Refused(CommandFailure::CorruptState))
    ));
    prune(&connection, original.id.client, 1, 1001)?;
    assert!(matches!(
        lookup_identity(&connection, &original, &limits),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    assert!(lookup_identity(&connection, &request(2, 2000), &limits)?.is_none());
    assert_eq!(client(&connection, original.id.client)?, Some((1, 1)));
    Ok(())
}

#[test]
fn cell_lookup_distinguishes_initial_absence_from_lost_schema() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    let command = request(1, 1000);
    let limits = ExecutionLimits::default();
    assert!(lookup_cell_identity(&connection, &command, &limits, 1)?.is_none());
    assert!(matches!(
        lookup_cell_identity(&connection, &request(2, 1000), &limits, 1),
        Err(ExecutionError::Refused(CommandFailure::SequenceGap {
            next: 1
        }))
    ));
    for sequence in [0, 2] {
        assert!(matches!(
            lookup_cell_identity(&connection, &command, &limits, sequence),
            Err(ExecutionError::Refused(CommandFailure::CorruptState))
        ));
    }
    install(&connection)?;
    // Inspection does not consume write capacity or invoke receipt maintenance.
    let saturated = ExecutionLimits {
        max_retained_receipts: 1,
        max_client_identities: 1,
        ..limits
    };
    record(&connection, &command, vec![7])?;
    assert_eq!(
        lookup_cell_identity(&connection, &command, &saturated, 2)?
            .unwrap()
            .result(),
        &[7]
    );
    let mut other = command.clone();
    other.id = CommandId::new(ClientId::new([9; 16])?, 1)?;
    assert!(lookup_cell_identity(&connection, &other, &saturated, 2)?.is_none());
    prune(&connection, command.id.client, 1, 1001)?;
    assert!(matches!(
        lookup_cell_identity(&connection, &command, &limits, 3),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    connection.execute_batch("DROP TABLE __sys_raft_receipts")?;
    for sequence in [1, 3] {
        assert!(matches!(
            lookup_cell_identity(&connection, &command, &limits, sequence),
            Err(ExecutionError::Refused(CommandFailure::CorruptState))
        ));
    }
    connection.execute_batch("DROP TABLE __sys_raft_clients")?;
    assert!(matches!(
        lookup_cell_identity(&connection, &command, &limits, 3),
        Err(ExecutionError::Refused(CommandFailure::CorruptState))
    ));
    Ok(())
}

#[test]
fn rpc_checkpoints_never_become_completed_or_absent_receipts() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    install(&connection)?;
    let request = request(1, 1000);
    let limits = ExecutionLimits::default();
    connection.execute_batch("BEGIN")?;
    record_rpc_step(&connection, &request, &limits, 1)?;
    connection.execute_batch("COMMIT")?;
    assert_eq!(retained_count(&connection)?, 1);
    assert_eq!(client(&connection, request.id.client)?, Some((1, 0)));
    assert!(matches!(
        preflight(&connection, &request, &limits),
        Err(ExecutionError::Uncertain)
    ));
    assert!(matches!(
        lookup_cell_identity(&connection, &request, &limits, 2),
        Err(ExecutionError::Uncertain)
    ));
    assert_eq!(
        rpc_receipt(&connection, &request, &limits, 1)?.rpc_step,
        Some(1)
    );
    let mut conflict = request.clone();
    conflict.request_digest[0] ^= 1;
    assert!(matches!(
        lookup_identity(&connection, &conflict, &limits),
        Err(ExecutionError::Refused(CommandFailure::CommandConflict))
    ));
    assert!(record_rpc_step(&connection, &request, &limits, 3).is_err());
    assert!(complete_rpc(&connection, &request, &limits, 2, vec![4]).is_err());
    assert_eq!(
        rpc_receipt(&connection, &request, &limits, 1)?.rpc_step,
        Some(1)
    );
    record_rpc_step(&connection, &request, &limits, 2)?;
    assert!(rpc_receipt(&connection, &request, &limits, 1).is_err());
    complete_rpc(&connection, &request, &limits, 2, vec![4])?;
    let complete = lookup_identity(&connection, &request, &limits)?.unwrap();
    assert_eq!(complete.result(), &[4]);
    assert_eq!(complete.rpc_step, None);
    assert_eq!(retained_count(&connection)?, 1);
    Ok(())
}

#[test]
fn interrupted_rpc_retains_the_original_expiry_floor() -> anyhow::Result<()> {
    let connection = Connection::open_in_memory()?;
    install(&connection)?;
    let command = request(1, 1000);
    record_rpc_step(&connection, &command, &ExecutionLimits::default(), 1)?;
    assert!(matches!(
        prune(&connection, command.id.client, 1, 1000),
        Err(ExecutionError::RetentionNotExpired)
    ));
    prune(&connection, command.id.client, 1, 1001)?;
    assert!(matches!(
        lookup_identity(&connection, &command, &ExecutionLimits::default()),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    assert_eq!(client(&connection, command.id.client)?, Some((1, 1)));
    assert_eq!(retained_count(&connection)?, 0);
    Ok(())
}
