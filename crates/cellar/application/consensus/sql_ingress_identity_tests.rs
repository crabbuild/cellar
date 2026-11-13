// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::consensus::{command_request_digest, native_rpc};
use crate::engine_api::RpcData;

#[test]
fn identical_business_requests_have_distinct_conflict_fingerprints() -> Result<()> {
    let args = RpcData::Json("[\"INSERT INTO data VALUES(1)\"]".into());
    let first = native_rpc::encode_ingress_request(Some("db"), &args, [1; 16], 1024)?;
    let second = native_rpc::encode_ingress_request(Some("db"), &args, [2; 16], 1024)?;
    assert_ne!(
        command_request_digest(&first),
        command_request_digest(&second)
    );
    for request in [&first, &second] {
        let decoded = native_rpc::decode_request(request)?;
        assert_eq!(decoded.name.as_deref(), Some("db"));
        assert!(
            matches!(decoded.args, RpcData::Json(value) if value == "[\"INSERT INTO data VALUES(1)\"]")
        );
    }
    assert!(
        native_rpc::encode_ingress_request(Some("db"), &args, [1; 16], first.len() - 1).is_err()
    );
    for end in 0..22 {
        assert!(native_rpc::decode_request(&first[..end]).is_err());
    }
    Ok(())
}

#[test]
fn absence_and_matching_receipts_never_authorize_new_mutations() -> Result<()> {
    let mut bytes = 8u64.to_be_bytes().to_vec();
    bytes.push(0);
    let missing = Observation::decode(&bytes)?;
    assert!(!missing.consumed_other(7, [1; 32]));
    bytes[8] = 1;
    bytes.extend([1; 32]);
    let present = Observation::decode(&bytes)?;
    assert!(!present.consumed_other(7, [1; 32]));
    assert!(present.consumed_other(7, [2; 32]));
    assert!(!present.consumed_other(8, [2; 32]));
    bytes.push(0);
    assert!(Observation::decode(&bytes).is_err());
    Ok(())
}

#[test]
fn fixed_stream_requires_reserved_sql_contract_and_nonce() -> Result<()> {
    use crate::application::consensus::{CommandId, Durability, RaftClass};
    use cellar_logic::cell_group::{CellHistory, CellId};
    use cellar_logic::consensus::{ClusterId, IncarnationId, NamespaceId};
    let sql = crate::application::consensus::sql_service()?;
    let scope = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("__SQLDatabase:test")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let contract = sql.binding_contract("__sqlExec").unwrap();
    assert!(sql
        .binding_contract(crate::application::sql::operator::METHOD)
        .is_none());
    let operator = sql.sql_operator_contract().unwrap();
    let id = CommandId::new(client(), 1)?;
    let args = RpcData::Json("[\"SELECT 1\"]".into());
    let input = native_rpc::encode_ingress_request(Some("db"), &args, [1; 16], 1024)?;
    sql.validate_command_identity(&scope, &contract, id, Durability::Quorum, &input)?;
    sql.validate_command_identity(&scope, &operator, id, Durability::Quorum, &input)?;
    assert!(sql
        .validate_command_identity(
            &scope,
            &contract,
            id,
            Durability::Quorum,
            &native_rpc::encode_request(Some("db"), &args, 1024)?
        )
        .is_err());
    let ordinary = CommandId::new(ClientId::new([99; 16])?, 1)?;
    assert!(sql
        .validate_command_identity(&scope, &contract, ordinary, Durability::Quorum, &input)
        .is_err());
    let other = RaftClass::new("Counter", 1, [1; 32])?.cell_rpc("Add", [2; 32])?;
    assert!(other
        .validate_command_identity(
            &scope,
            &other.contract("Add").unwrap(),
            id,
            Durability::Quorum,
            &input
        )
        .is_err());
    // Registered commands accept opaque application bytes. The nonce marker
    // must not reserve an unrelated raw command's input or client namespace.
    let opaque = RaftClass::new("Opaque", 1, [3; 32])?
        .command("Bytes", [4; 32], |_, input| Ok(input.to_vec()))?;
    opaque.validate_command_identity(
        &scope,
        &opaque.contract("Bytes").unwrap(),
        id,
        Durability::Quorum,
        &input,
    )?;
    Ok(())
}
