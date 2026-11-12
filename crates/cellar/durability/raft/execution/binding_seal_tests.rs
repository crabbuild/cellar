// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::storage::CommandDatabase;

#[test]
#[allow(clippy::disallowed_methods)]
fn binding_seal_checks_policy_and_rolls_back_with_outer_transaction() -> anyhow::Result<()> {
    let class = RaftClass::new("Counter", 1, [21; 32])?.json_cell_rpc("Add", [22; 32])?;
    let target = class.contract("Add").unwrap();
    let maintenance = CommandId::new(ClientId::new([23; 16])?, 1)?;
    let mut client = [26; 16];
    client[..4].copy_from_slice(b"RCB1");
    let original = CommandRequest {
        id: CommandId::new(ClientId::new(client)?, 1)?,
        operation: target.operation.clone(),
        contract_digest: target.digest,
        request_digest: [27; 32],
        durability: Durability::S3,
        retry_until_ms: 10,
    };
    let input = serde_json::to_vec(&Request {
        format: 1,
        client,
        sequence: 1,
        operation: target.operation,
        contract: target.digest,
        request_digest: original.request_digest,
        durability: 2,
        retry_until_ms: 10,
    })?;
    let root = tempfile::tempdir()?;
    let limits = ExecutionLimits::default();
    let mut database = CommandDatabase::open(
        &root.path().join("target.sqlite"),
        None,
        limits.max_image_bytes,
    )?;
    crate::storage::as_framework_sql(|| receipts::install(database.framework()))?;
    // The original business expiry is in the past. Sealing cannot dispatch it;
    // maintenance has its own admission clock and must preserve its S3 minimum.
    let inputs = CommandInputs {
        now_unix_millis: 1000,
        random: [28; 32],
    };
    assert!(database
        .transaction(inputs, |tx| execute(
            &class,
            maintenance,
            Durability::Quorum,
            tx,
            &input,
            limits
        ))
        .is_err());
    assert!(receipts::client(database.framework(), original.id.client)?.is_none());
    let rolled_back: anyhow::Result<()> = database.transaction(inputs, |tx| {
        assert_eq!(
            execute(&class, maintenance, Durability::S3, tx, &input, limits)?,
            [0]
        );
        anyhow::bail!("outer maintenance failed")
    });
    assert!(rolled_back.is_err());
    assert!(receipts::client(database.framework(), original.id.client)?.is_none());
    let result = database.transaction(inputs, |tx| {
        execute(&class, maintenance, Durability::S3, tx, &input, limits)
    })?;
    assert_eq!(result, [0]);
    assert!(matches!(
        receipts::lookup_identity(database.framework(), &original, &limits),
        Err(ExecutionError::Uncertain)
    ));
    let result = database.transaction(inputs, |tx| {
        execute(&class, maintenance, Durability::S3, tx, &input, limits)
    })?;
    assert_eq!(result, [1]);
    for invalid in [vec![b' '; MAX_INPUT + 1], b"{}".to_vec()] {
        assert!(database
            .transaction(inputs, |tx| execute(
                &class,
                maintenance,
                Durability::S3,
                tx,
                &invalid,
                limits
            ))
            .is_err());
    }
    Ok(())
}
