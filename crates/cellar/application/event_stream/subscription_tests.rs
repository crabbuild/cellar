//! Untrusted fetch responses cannot mint processing authority for another batch.
use super::super::{Envelope, Resource, SubscriptionIdentity};
use super::*;

fn fixture() -> (Resource, SubscriptionIdentity, Value, usize) {
    let resource = Resource {
        tenant: "tenant".into(),
        application: "app".into(),
        id: "a".repeat(32),
        name: "orders".into(),
        family: "orders".into(),
    };
    let identity = SubscriptionIdentity {
        resource: resource.id.clone(),
        name: "worker".into(),
        id: "b".repeat(32),
        generation: "c".repeat(32),
    };
    let source = "Order:source".to_owned();
    let incarnation = "d".repeat(32);
    let lane = super::super::lane_identity(&resource, &source, &incarnation).unwrap();
    let event = Envelope {
        id: format!("v1:{lane}:1"),
        lane,
        sequence: 1,
        source,
        source_incarnation: incarnation,
        family: resource.family.clone(),
        version: 7,
        key: "order".into(),
        created_at_ms: 1,
        payload: json!({"n":u64::MAX}),
    };
    let bytes = serde_json::to_vec(&event).unwrap();
    let receipt = Receipt {
        resource: resource.id.clone(),
        partition: 0,
        name: identity.name.clone(),
        subscription: identity.id.clone(),
        incarnation: identity.generation.clone(),
        generation: 1,
        token: "e".repeat(64),
        first: 1,
        last: 1,
    };
    let value = json!({"receipt":receipt.encode().unwrap(),"issued_ms":"100","expires_ms":"200","records":[{"offset":"1","envelope":base64::engine::general_purpose::STANDARD.encode(&bytes)}]});
    (resource, identity, value, bytes.len())
}

fn accept(value: Value) -> Result<(Receipt, Vec<Record>, i64, i64)> {
    let (resource, identity, _, _) = fixture();
    accept_batch(
        &resource,
        &identity,
        BatchOptions::default(),
        cellar_logic::event_stream::Limits::default(),
        value,
    )
}

fn code(value: Value, expected: ErrorCode) {
    let error = accept(value).expect_err("accepted invalid batch");
    assert_eq!(
        error
            .downcast_ref::<super::super::DeliveryError>()
            .expect("untyped acceptance failure")
            .code,
        expected
    );
}

#[test]
fn fetch_acceptance_preserves_exact_receipt_unknown_schema_and_integer_payload() -> Result<()> {
    let (_, _, value, _) = fixture();
    let encoded = value["receipt"].as_str().unwrap().to_owned();
    let (receipt, records, issued, expires) = accept(value)?;
    assert_eq!(receipt.encode()?, encoded);
    assert_eq!((issued, expires), (100, 200));
    assert_eq!(records[0].event.version, 7);
    assert_eq!(records[0].event.payload["n"].as_u64(), Some(u64::MAX));
    Ok(())
}

#[test]
fn fetch_rejects_wrong_resolved_subscription_and_recreation() {
    for field in ["resource", "name", "subscription", "incarnation"] {
        let (_, _, mut value, _) = fixture();
        let mut receipt = Receipt::decode(value["receipt"].as_str().unwrap()).unwrap();
        match field {
            "resource" => receipt.resource = "f".repeat(32),
            "name" => receipt.name = "other".into(),
            "subscription" => receipt.subscription = "f".repeat(32),
            _ => receipt.incarnation = "f".repeat(32),
        }
        value["receipt"] = json!(receipt.encode().unwrap());
        code(value, ErrorCode::SubscriptionReset);
    }
}

#[test]
fn fetch_rejects_untrusted_envelope_identity_and_malformed_codecs() {
    for field in [
        "id",
        "lane",
        "source",
        "source_incarnation",
        "family",
        "sequence",
        "version",
        "created_at_ms",
    ] {
        let (_, _, mut value, _) = fixture();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(value["records"][0]["envelope"].as_str().unwrap())
            .unwrap();
        let mut event: Value = serde_json::from_slice(&bytes).unwrap();
        event[field] = match field {
            "sequence" => json!("0"),
            "version" => json!(0),
            "created_at_ms" => json!("-1"),
            _ => json!("foreign"),
        };
        value["records"][0]["envelope"] =
            json!(base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&event).unwrap()));
        code(value, ErrorCode::InvalidRequest);
    }
    for encoded in ["%%%", "e30="] {
        let (_, _, mut value, _) = fixture();
        value["records"][0]["envelope"] = json!(encoded);
        code(value, ErrorCode::InvalidRequest);
    }
}

#[test]
fn fetch_rejects_invalid_lease_intervals_and_noncanonical_times() {
    for (issued, expires) in [
        ("-1", "100"),
        ("100", "100"),
        ("200", "100"),
        ("0", "300001"),
    ] {
        let (_, _, mut value, _) = fixture();
        value["issued_ms"] = json!(issued);
        value["expires_ms"] = json!(expires);
        code(value, ErrorCode::StaleReceipt);
    }
    for field in ["issued_ms", "expires_ms"] {
        let (_, _, mut value, _) = fixture();
        value[field] = json!("0100");
        code(value, ErrorCode::InvalidRequest);
    }
}

#[test]
fn fetch_enforces_caller_and_host_budgets_before_envelope_allocation() {
    let (resource, identity, value, size) = fixture();
    for (options, limits) in [
        (
            BatchOptions {
                max_bytes: size - 1,
                ..Default::default()
            },
            cellar_logic::event_stream::Limits::default(),
        ),
        (
            BatchOptions::default(),
            cellar_logic::event_stream::Limits {
                max_event_bytes: size - 1,
                ..Default::default()
            },
        ),
        (
            BatchOptions {
                max_records: 0,
                ..Default::default()
            },
            cellar_logic::event_stream::Limits::default(),
        ),
    ] {
        let failure = accept_batch(&resource, &identity, options, limits, value.clone())
            .err()
            .unwrap();
        assert_eq!(
            failure
                .downcast_ref::<super::super::DeliveryError>()
                .unwrap()
                .code,
            ErrorCode::PayloadTooLarge
        );
    }
    let mut huge = value;
    huge["records"][0]["envelope"] = json!("A".repeat(
        cellar_logic::event_stream::Limits::default()
            .event_bytes()
            .div_ceil(3)
            * 4
            + 4
    ));
    code(huge, ErrorCode::PayloadTooLarge);
}

#[test]
fn fetch_rejects_missing_repeated_skipped_and_noncanonical_offsets() {
    for records in [
        json!([]),
        json!([{"offset":"2","envelope":fixture().2["records"][0]["envelope"]}]),
        json!([{"offset":"01","envelope":fixture().2["records"][0]["envelope"]}]),
    ] {
        let (_, _, mut value, _) = fixture();
        value["records"] = records;
        assert!(accept(value).is_err());
    }
    let (_, _, mut value, _) = fixture();
    let row = value["records"][0].clone();
    value["records"].as_array_mut().unwrap().push(row);
    let mut receipt = Receipt::decode(value["receipt"].as_str().unwrap()).unwrap();
    receipt.last = 2;
    value["receipt"] = json!(receipt.encode().unwrap());
    code(value, ErrorCode::StaleReceipt);
}

#[test]
fn fetch_preserves_offsets_and_deadlines_near_i64_end() -> Result<()> {
    let (_, _, mut value, _) = fixture();
    let mut receipt = Receipt::decode(value["receipt"].as_str().unwrap())?;
    receipt.first = i64::MAX - 1;
    receipt.last = receipt.first;
    value["receipt"] = json!(receipt.encode()?);
    value["records"][0]["offset"] = json!(receipt.first.to_string());
    value["issued_ms"] = json!((i64::MAX - 10).to_string());
    value["expires_ms"] = json!(i64::MAX.to_string());
    let (_, records, issued, expires) = accept(value)?;
    assert_eq!(records[0].offset, i64::MAX - 1);
    assert_eq!((issued, expires), (i64::MAX - 10, i64::MAX));
    Ok(())
}

#[test]
fn extension_response_preserves_absolute_target_and_rejects_shortening() -> Result<()> {
    assert_eq!(accept_extension(100, &json!({"expires_ms":"100"}))?, 100);
    assert_eq!(accept_extension(100, &json!({"expires_ms":"200"}))?, 200);
    assert_eq!(
        accept_extension(i64::MAX, &json!({"expires_ms":i64::MAX.to_string()}))?,
        i64::MAX
    );
    for value in [
        json!({"expires_ms":"99"}),
        json!({"expires_ms":"-1"}),
        json!({"expires_ms":"0100"}),
        json!({}),
    ] {
        assert!(accept_extension(100, &value).is_err());
    }
    assert!(accept_extension(0, &json!({"expires_ms":"0"})).is_err());
    Ok(())
}

#[test]
fn fetch_bounds_second_envelope_by_remaining_batch_bytes() {
    let (resource, identity, mut value, size) = fixture();
    let mut receipt = Receipt::decode(value["receipt"].as_str().unwrap()).unwrap();
    receipt.last = 2;
    value["receipt"] = json!(receipt.encode().unwrap());
    let mut second = value["records"][0].clone();
    second["offset"] = json!("2");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(second["envelope"].as_str().unwrap())
        .unwrap();
    let mut event: Envelope = serde_json::from_slice(&bytes).unwrap();
    event.sequence = 2;
    event.id = format!("v1:{}:2", event.lane);
    second["envelope"] = json!(
        base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&event).unwrap())
    );
    value["records"].as_array_mut().unwrap().push(second);
    let options = BatchOptions {
        max_bytes: size * 2 - 1,
        ..Default::default()
    };
    let failure = accept_batch(
        &resource,
        &identity,
        options,
        cellar_logic::event_stream::Limits::default(),
        value,
    )
    .err()
    .unwrap();
    assert_eq!(
        failure
            .downcast_ref::<super::super::DeliveryError>()
            .unwrap()
            .code,
        ErrorCode::PayloadTooLarge
    );
}
