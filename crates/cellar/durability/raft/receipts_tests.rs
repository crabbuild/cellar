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
