// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn metrics_wire_confines_resource_identity_arguments_and_purpose() -> Result<()> {
    let scope = crate::engine_api::queue_cell_scope("jobs");
    let args = RpcData::Json("[]".into());
    let bytes = encode(&scope, Some("jobs"), &args, 4096)?;
    validate(&scope, &bytes)?;
    assert!(is_method(&scope, "__queueMetrics"));
    assert!(!is_method(&scope, crate::application::queue::OPERATOR));
    assert!(!is_method("Counter:jobs", "__queueMetrics"));
    assert!(encode(&scope, None, &args, 4096).is_err());
    assert!(encode(&scope, Some("other"), &args, 4096).is_err());
    for args in [
        RpcData::Json("[{}]".into()),
        RpcData::Json("{}".into()),
        RpcData::Json("bad".into()),
    ] {
        assert!(encode(&scope, Some("jobs"), &args, 4096).is_err());
    }
    assert!(encode(&scope, Some("jobs"), &args, bytes.len() - 1).is_err());
    assert_eq!(encode(&scope, Some("jobs"), &args, bytes.len())?, bytes);
    assert!(validate(&crate::engine_api::queue_cell_scope("other"), &bytes).is_err());
    assert!(native_rpc::decode_request(&bytes).is_err());
    for end in 0..bytes.len() {
        assert!(validate(&scope, &bytes[..end]).is_err());
    }
    let operator = super::super::native_queue_query::encode(
        &scope,
        Some("jobs"),
        &RpcData::Json("[{\"op\":\"peek\"}]".into()),
        4096,
    )?;
    assert!(validate(&scope, &operator).is_err());
    Ok(())
}

#[test]
fn metrics_class_admission_cannot_substitute_operator_event_or_public_grants() -> Result<()> {
    use super::super::{Durability, QueryAdmission, RaftClass};
    let class = RaftClass::queue_service()?
        .cell_rpc("__queueMetrics", [1; 32])?
        .cell_rpc(crate::application::queue::OPERATOR, [2; 32])?;
    let metrics = contract();
    let operator = super::super::native_queue_query::contract();
    assert!(class.contract(&metrics.operation).is_none());
    assert!(class
        .validate_query_admission(&metrics, QueryAdmission::Public)
        .is_err());
    for minimum in [Durability::Quorum, Durability::S3] {
        let admission = QueryAdmission::QueueMetrics(minimum);
        assert!(admission.minimum() == Some(minimum));
        class.validate_query_admission(&metrics, admission)?;
        assert!(class
            .validate_query_admission(&operator, admission)
            .is_err());
        assert!(class
            .validate_query_admission(&metrics, QueryAdmission::QueueOperator(minimum))
            .is_err());
        assert!(class
            .validate_query_admission(&metrics, QueryAdmission::Event(minimum))
            .is_err());
        let foreign = RaftClass::new("Counter", 1, [3; 32])?.cell_rpc("__queueMetrics", [1; 32])?;
        assert!(foreign
            .validate_query_admission(&metrics, admission)
            .is_err());
    }
    Ok(())
}
