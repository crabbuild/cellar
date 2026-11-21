// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

#[test]
fn queue_inspection_never_accepts_mutating_operations_or_another_resource() -> Result<()> {
    let scope = crate::engine_api::queue_cell_scope("jobs");
    for operation in [
        "info", "peek", "pause", "resume", "purge", "redrive", "unknown",
    ] {
        let args = RpcData::Json(json!([{"op":operation,"limit":10}]).to_string());
        let read = matches!(operation, "info" | "peek");
        assert_eq!(is_metadata(&args)?, read);
        let encoded = encode(&scope, Some("jobs"), &args, 4096);
        if read {
            let encoded = encoded?;
            assert_eq!(decode(&scope, &encoded)?["op"], operation);
            assert!(decode(&crate::engine_api::queue_cell_scope("other"), &encoded).is_err());
            for end in 0..encoded.len() {
                assert!(decode(&scope, &encoded[..end]).is_err());
            }
        } else {
            assert!(encoded.is_err());
            // A forged private prefix cannot turn a stateful RPC into a read.
            let mut forged = MAGIC.to_vec();
            forged.extend(native_rpc::encode_request(Some("jobs"), &args, 4096)?);
            assert!(decode(&scope, &forged).is_err());
        }
    }
    assert!(is_metadata(&RpcData::Json("not-json".into())).is_err());
    assert!(is_metadata(&RpcData::Json("{}".into())).is_err());
    Ok(())
}

#[test]
fn queue_inspection_keeps_wire_and_identity_bounds() -> Result<()> {
    let scope = crate::engine_api::queue_cell_scope("jobs");
    let args = RpcData::Json(json!([{"op":"info"}]).to_string());
    let wire = encode(&scope, Some("jobs"), &args, 4096)?;
    assert!(encode(&scope, None, &args, 4096).is_err());
    assert!(encode(&scope, Some("other"), &args, 4096).is_err());
    assert!(encode(&scope, Some("jobs"), &args, wire.len() - 1).is_err());
    assert_eq!(encode(&scope, Some("jobs"), &args, wire.len())?, wire);
    assert!(native_rpc::decode_request(&wire).is_err());
    Ok(())
}

#[test]
fn queue_inspection_requires_its_private_class_admission() -> Result<()> {
    let class = super::super::RaftClass::queue_service()?
        .cell_rpc(crate::application::queue::OPERATOR, [1; 32])?;
    let contract = contract();
    assert!(class.contract(&contract.operation).is_none());
    assert!(class
        .validate_query_admission(&contract, super::super::QueryAdmission::Public)
        .is_err());
    assert!(class
        .validate_query_admission(
            &contract,
            super::super::QueryAdmission::Event(super::super::Durability::Quorum)
        )
        .is_err());
    for minimum in [
        super::super::Durability::Quorum,
        super::super::Durability::S3,
    ] {
        class.validate_query_admission(
            &contract,
            super::super::QueryAdmission::QueueOperator(minimum),
        )?;
    }
    Ok(())
}
