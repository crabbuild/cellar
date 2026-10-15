//! A malformed submitted reply cannot establish non-execution or permanent refusal.
use super::*;
use serde_json::json;

fn code(failure: &anyhow::Error) -> Option<ErrorCode> {
    failure.downcast_ref::<DeliveryError>().map(|e| e.code)
}

#[test]
fn successful_reply_moves_its_payload_without_cloning() -> anyhow::Result<()> {
    let value = json!({"ok":{"payload":"x".repeat(4096)}});
    let allocation = value["ok"]["payload"].as_str().unwrap().as_ptr();
    let accepted = unwrap_reply(value)?;
    assert_eq!(accepted["payload"].as_str().unwrap().as_ptr(), allocation);
    assert!(unwrap_reply(json!({"ok":null}))?.is_null());
    Ok(())
}

#[test]
fn contradictory_and_invalid_error_envelopes_are_ambiguous() {
    for value in [
        json!({"ok":null,"error":{"code":"Forbidden","message":"untrusted"}}),
        json!({"ok":null,"other":1}),
        json!({"error":{"code":"new-code","message":"untrusted"}}),
        json!({"error":"untrusted"}),
        json!({}),
        json!([]),
    ] {
        let failure = unwrap_reply(value).unwrap_err();
        assert_eq!(code(&failure), Some(ErrorCode::Ambiguous));
        assert!(!failure.to_string().contains("untrusted"));
    }
}

#[test]
fn valid_protocol_errors_preserve_codes_and_exact_metadata() -> anyhow::Result<()> {
    for expected in [
        ErrorCode::Unavailable,
        ErrorCode::OwnershipLost,
        ErrorCode::StreamFull,
        ErrorCode::CursorExpired,
        ErrorCode::Forbidden,
        ErrorCode::SequenceGap,
    ] {
        let data = json!({"earliest":i64::MAX.to_string(),"batch_records":"100"});
        let failure = unwrap_reply(
            json!({"error":{"code":expected,"message":"bounded refusal","data":data}}),
        )
        .unwrap_err();
        let accepted = failure.downcast_ref::<DeliveryError>().unwrap();
        assert_eq!(accepted.code, expected);
        assert_eq!(accepted.data, data);
        assert_eq!(
            accepted.is_permanent(),
            matches!(expected, ErrorCode::Forbidden | ErrorCode::SequenceGap)
        );
    }
    Ok(())
}

#[test]
fn error_metadata_is_bounded_before_typed_acceptance() {
    for value in [
        json!({"error":{"code":"Forbidden","message":"x".repeat(MAX_ERROR_MESSAGE_BYTES+1)}}),
        json!({"error":{"code":"Forbidden","message":"bounded","data":"x".repeat(MAX_ERROR_BYTES)}}),
        json!({"error":{"code":"Forbidden","message":null}}),
    ] {
        let failure = unwrap_reply(value).unwrap_err();
        assert_eq!(code(&failure), Some(ErrorCode::Ambiguous));
        assert!(failure.to_string().len() < 128);
    }
}

#[test]
fn wire_reply_decodes_broker_envelopes_and_preserves_known_refusals() -> anyhow::Result<()> {
    use cellar_native::clone::StructuredValue;
    let value = json!({"ok":{"offset":i64::MAX.to_string()}});
    let encoded = crate::application::encode_rpc(&StructuredValue::from_json(value.clone())?)?;
    assert_eq!(decode_reply(&encoded)?, value["ok"]);
    assert_eq!(
        decode_reply(&crate::engine_api::RpcData::Json(value.to_string()))?,
        value["ok"]
    );
    let value = json!({"error":{"code":"StreamFull","message":"capacity","data":{"batch_bytes":"1000","batch_records":"2"}}});
    let encoded = crate::application::encode_rpc(&StructuredValue::from_json(value)?)?;
    let failure = decode_reply(&encoded).unwrap_err();
    assert_eq!(code(&failure), Some(ErrorCode::StreamFull));
    assert_eq!(
        failure.downcast_ref::<DeliveryError>().unwrap().data["batch_bytes"],
        "1000"
    );
    Ok(())
}

#[test]
fn malformed_or_unprojectable_wire_results_are_ambiguous_without_payload_text() -> anyhow::Result<()>
{
    use cellar_native::clone::{Object, StructuredValue, Value};
    let cycle = StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::DenseArray {
            elements: vec![Value::Reference(0)],
            properties: vec![],
        }],
    };
    let replies = [
        crate::engine_api::RpcData::Json("untrusted".into()),
        crate::engine_api::RpcData::Structured(vec![0xfe].into()),
        crate::engine_api::RpcData::Structured(vec![1].into()),
        crate::engine_api::RpcData::Structured(
            cellar_native::rpc::encode_error("untrusted", "Error", None)?.into(),
        ),
        crate::application::encode_rpc(&cycle)?,
    ];
    for reply in replies {
        let failure = decode_reply(&reply).unwrap_err();
        assert_eq!(code(&failure), Some(ErrorCode::Ambiguous));
        assert!(!format!("{failure:#}").contains("untrusted"));
    }
    Ok(())
}

#[test]
fn oversized_json_wire_is_refused_before_successful_decoding() {
    // Valid JSON would otherwise return a successful null result. The byte
    // ceiling applies before JSON parsing and structured graph allocation.
    let mut text = " ".repeat(MAX_REPLY_BYTES);
    text.push_str("{\"ok\":null}");
    let failure = decode_reply(&crate::engine_api::RpcData::Json(text)).unwrap_err();
    assert_eq!(code(&failure), Some(ErrorCode::Ambiguous));
}
