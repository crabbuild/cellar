// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClientId, ClusterId, IncarnationId, NamespaceId},
};

fn fixture() -> Result<(
    Connection,
    CellHistory,
    CellGroupPayloadReference,
    CommandId,
)> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("CREATE TABLE __sys_raft_receipts(client_id BLOB,sequence BLOB,rpc_step BLOB);
        CREATE TABLE __sys_raft_binding_intents(parent_client BLOB,parent_sequence BLOB,phase INTEGER)")?;
    let history = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:payload")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let reference = serde_json::from_value(
        serde_json::json!({"format":1,"owner":vec![4;32],"writer":vec![3;16],"allocation":vec![5;16],"digest":vec![6;32],"length":1024}),
    )?;
    let parent = CommandId::new(ClientId::new([7; 16])?, 1)?;
    Ok((connection, history, reference, parent))
}

#[test]
fn partial_turn_and_unknown_child_keep_dependencies_until_retirement() -> Result<()> {
    let (connection, history, reference, parent) = fixture()?;
    record_in(
        &connection,
        parent,
        history.clone(),
        reference.clone(),
        false,
        [8; 32],
    )?;
    record_in(
        &connection,
        parent,
        history.clone(),
        reference.clone(),
        true,
        [8; 32],
    )?;
    record_in(
        &connection,
        parent,
        history.clone(),
        reference.clone(),
        false,
        [8; 32],
    )?;
    let mut confirmed = Vec::new();
    visit(&connection, &history, &mut |dependency| {
        confirmed.push(dependency.confirmed);
        Ok(())
    })?;
    assert_eq!(confirmed, [true]);
    connection.execute(
        "INSERT INTO __sys_raft_receipts VALUES(?1,?2,X'01')",
        rusqlite::params![
            parent.client.bytes().as_slice(),
            1_u64.to_be_bytes().as_slice()
        ],
    )?;
    assert!(!prunable(&connection, parent.client, 1)?);
    let key = due(&connection)?.expect("abandoned partial turn");
    assert!(
        retire(&connection, &key).is_err(),
        "retirement requires a writer transaction"
    );
    for phase in [0, 2, 4] {
        connection.execute(
            "INSERT INTO __sys_raft_binding_intents VALUES(?1,?2,?3)",
            rusqlite::params![key.client.as_slice(), 1_u64.to_be_bytes().as_slice(), phase],
        )?;
        assert!(due(&connection)?.is_none());
        assert!(!prunable(&connection, parent.client, 1)?);
        connection.execute_batch("BEGIN")?;
        assert!(
            retire(&connection, &key).is_err(),
            "unknown child input was retired"
        );
        connection.execute_batch("ROLLBACK; DELETE FROM __sys_raft_binding_intents")?;
    }
    connection.execute_batch("BEGIN")?;
    retire(&connection, &key)?;
    connection.execute_batch("COMMIT")?;
    assert!(due(&connection)?.is_none());
    assert!(prunable(&connection, parent.client, 1)?);
    assert!(
        record_in(
            &connection,
            parent,
            history.clone(),
            reference,
            true,
            [8; 32]
        )
        .is_err(),
        "retired native frame resumed"
    );
    prune(&connection, parent.client, 1)?;
    visit(&connection, &history, &mut |_| {
        anyhow::bail!("retired command dependency survived its retry floor")
    })?;
    Ok(())
}

#[test]
fn value_rows_outlive_receipts_and_corrupt_descriptors_refuse_recovery() -> Result<()> {
    let (connection, history, reference, parent) = fixture()?;
    record_in(
        &connection,
        parent,
        history.clone(),
        reference.clone(),
        true,
        [8; 32],
    )?;
    connection.execute_batch("CREATE TABLE __sys_kv(blob_id TEXT,size INTEGER)")?;
    connection.execute(
        "INSERT INTO __sys_kv VALUES(?1,1024)",
        [serde_json::to_string(&reference)?],
    )?;
    prune(&connection, parent.client, 1)?;
    let mut observed = Vec::new();
    visit(&connection, &history, &mut |dependency| {
        observed.push(dependency);
        Ok(())
    })?;
    assert_eq!(
        observed,
        [Dependency {
            history: history.clone(),
            reference: reference.clone(),
            confirmed: true,
            guard: [0; 32]
        }]
    );
    record_in(
        &connection,
        parent,
        history.clone(),
        reference,
        false,
        [8; 32],
    )?;
    connection.execute_batch("UPDATE __sys_raft_payload_dependencies SET body=X'00'")?;
    assert!(visit(&connection, &history, &mut |_| Ok(())).is_err());
    connection.execute_batch(
        "DELETE FROM __sys_raft_payload_dependencies; UPDATE __sys_kv SET size=1023",
    )?;
    assert!(visit(&connection, &history, &mut |_| Ok(())).is_err());
    Ok(())
}
