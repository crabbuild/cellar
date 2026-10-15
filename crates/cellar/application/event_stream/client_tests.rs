//! Client acceptance must independently enforce the bounded replay contract.
use super::super::{Envelope, Resource};
use super::*;

fn fixture() -> (Resource, serde_json::Value, usize) {
    let resource = Resource {
        tenant: "tenant".into(),
        application: "application".into(),
        id: "a".repeat(32),
        name: "orders".into(),
        family: "orders".into(),
    };
    let source = format!("Order:{}", "b".repeat(64));
    let incarnation = "c".repeat(32);
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
        payload: json!({"unsigned":u64::MAX,"signed":i64::MIN}),
    };
    let bytes = serde_json::to_vec(&event).unwrap();
    let page = json!({"resource":resource.id,"next":"2","earliest":"1","end":"2",
        "records":[{"offset":"1","envelope":base64::engine::general_purpose::STANDARD.encode(&bytes)}]});
    (resource, page, bytes.len())
}

fn accept(value: serde_json::Value) -> Result<Page> {
    accept_page(
        &fixture().0,
        None,
        ReadOptions::default(),
        cellar_logic::event_stream::Limits::default(),
        value,
    )
}

#[test]
fn raw_read_preserves_unknown_schema_and_exact_integer_payloads() -> Result<()> {
    let (_, value, _) = fixture();
    let page = accept(value)?;
    assert_eq!(page.next.next, 2);
    assert_eq!(page.records[0].event.version, 7);
    assert_eq!(
        page.records[0].event.payload["unsigned"].as_u64(),
        Some(u64::MAX)
    );
    assert_eq!(
        page.records[0].event.payload["signed"].as_i64(),
        Some(i64::MIN)
    );
    assert_eq!(
        serde_json::from_slice::<Envelope>(page.records[0].encoded())?.id,
        page.records[0].event.id
    );
    Ok(())
}

#[test]
fn replay_rejects_noncanonical_metadata_and_record_offsets() {
    for field in ["next", "earliest", "end"] {
        let (_, mut value, _) = fixture();
        value[field] = json!(format!("0{}", value[field].as_str().unwrap()));
        assert!(accept(value).is_err(), "accepted noncanonical {field}");
    }
    let (_, mut value, _) = fixture();
    value["records"][0]["offset"] = json!("01");
    assert!(
        accept(value).is_err(),
        "accepted noncanonical record offset"
    );
}

#[test]
fn replay_rejects_skipped_offsets_and_unmatched_next_cursor() {
    let (_, mut value, _) = fixture();
    value["records"][0]["offset"] = json!("2");
    value["next"] = json!("3");
    value["end"] = json!("3");
    assert!(accept(value).is_err(), "accepted skipped first offset");
    let (_, mut value, _) = fixture();
    value["next"] = json!("1");
    assert!(
        accept(value).is_err(),
        "accepted a cursor that repeated delivered history"
    );
    let (_, mut value, _) = fixture();
    value["records"] = json!([]);
    assert!(
        accept(value).is_err(),
        "accepted missing history before the reported end"
    );
}

#[test]
fn replay_rejects_batches_over_caller_or_host_budgets() {
    let (resource, mut value, size) = fixture();
    let extra = value["records"][0].clone();
    value["records"].as_array_mut().unwrap().push(extra);
    assert!(
        accept_page(
            &resource,
            None,
            ReadOptions {
                max_records: 1,
                max_bytes: usize::MAX
            },
            cellar_logic::event_stream::Limits::default(),
            value
        )
        .is_err(),
        "ignored caller record budget"
    );
    let (_, value, _) = fixture();
    assert!(
        accept_page(
            &resource,
            None,
            ReadOptions {
                max_records: 100,
                max_bytes: size - 1
            },
            cellar_logic::event_stream::Limits::default(),
            value.clone()
        )
        .is_err(),
        "ignored caller byte budget"
    );
    let limits = cellar_logic::event_stream::Limits {
        max_event_bytes: size - 1,
        ..Default::default()
    };
    assert!(
        accept_page(&resource, None, ReadOptions::default(), limits, value).is_err(),
        "ignored host event budget"
    );
}

#[test]
fn replay_rejects_wrong_envelope_family_lane_or_event_identity() {
    for field in ["family", "lane", "id"] {
        let (_, mut value, _) = fixture();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(value["records"][0]["envelope"].as_str().unwrap())
            .unwrap();
        let mut event: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        event[field] = json!("foreign");
        value["records"][0]["envelope"] =
            json!(base64::engine::general_purpose::STANDARD
                .encode(serde_json::to_vec(&event).unwrap()));
        assert!(accept(value).is_err(), "accepted foreign envelope {field}");
    }
}

#[test]
fn replay_respects_the_requested_position_and_empty_end_cursor() -> Result<()> {
    let (resource, value, _) = fixture();
    let cursor = Cursor {
        resource: resource.id.clone(),
        partition: 0,
        next: 2,
    };
    assert!(
        accept_page(
            &resource,
            Some(&cursor),
            ReadOptions::default(),
            cellar_logic::event_stream::Limits::default(),
            value.clone()
        )
        .is_err(),
        "ignored requested position"
    );
    let mut empty = value;
    empty["records"] = json!([]);
    assert_eq!(
        accept_page(
            &resource,
            Some(&cursor),
            ReadOptions::default(),
            cellar_logic::event_stream::Limits::default(),
            empty
        )?
        .next,
        cursor
    );
    Ok(())
}

#[test]
fn replay_preserves_positions_above_binary64_and_at_i64_end() -> Result<()> {
    let (resource, mut value, _) = fixture();
    let first = i64::MAX - 1;
    value["earliest"] = json!(first.to_string());
    value["end"] = json!(i64::MAX.to_string());
    value["next"] = json!(i64::MAX.to_string());
    value["records"][0]["offset"] = json!(first.to_string());
    let cursor = Cursor {
        resource: resource.id.clone(),
        partition: 0,
        next: first,
    };
    let page = accept_page(
        &resource,
        Some(&cursor),
        ReadOptions::default(),
        cellar_logic::event_stream::Limits::default(),
        value,
    )?;
    assert_eq!(page.records[0].offset, first);
    assert_eq!(page.next.next, i64::MAX);
    Ok(())
}

#[test]
fn unrestricted_read_still_rejects_protocol_record_overflow_before_decoding() {
    let (resource, mut value, _) = fixture();
    value["records"] = json!(vec![
        serde_json::Value::Null;
        cellar_logic::event_stream::MAX_PROTOCOL_BATCH_RECORDS
            + 1
    ]);
    value["end"] = json!((cellar_logic::event_stream::MAX_PROTOCOL_BATCH_RECORDS + 2).to_string());
    let failure = accept_page(
        &resource,
        None,
        ReadOptions {
            max_records: usize::MAX,
            max_bytes: usize::MAX,
        },
        cellar_logic::event_stream::Limits::unrestricted(),
        value,
    )
    .unwrap_err();
    assert_eq!(
        failure
            .downcast_ref::<super::super::DeliveryError>()
            .unwrap()
            .code,
        ErrorCode::PayloadTooLarge
    );
}

#[test]
fn malformed_read_encoding_and_zero_event_version_are_typed_failures() {
    let (_, mut value, _) = fixture();
    value["records"][0]["envelope"] = json!("!");
    assert_eq!(
        accept(value)
            .unwrap_err()
            .downcast_ref::<super::super::DeliveryError>()
            .unwrap()
            .code,
        ErrorCode::InvalidRequest
    );
    let (_, mut value, _) = fixture();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(value["records"][0]["envelope"].as_str().unwrap())
        .unwrap();
    let mut event: Envelope = serde_json::from_slice(&raw).unwrap();
    event.version = 0;
    value["records"][0]["envelope"] = json!(
        base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&event).unwrap())
    );
    assert_eq!(
        accept(value)
            .unwrap_err()
            .downcast_ref::<super::super::DeliveryError>()
            .unwrap()
            .code,
        ErrorCode::InvalidRequest
    );
}

fn configuration_fixture() -> (Definition, String, serde_json::Value) {
    use super::super::{Grant, Retention};
    use sha2::{Digest, Sha256};
    let definition = Definition {
        resource: fixture().0,
        revision: 7,
        retention: Retention::Bounded,
        max_bytes: 10000,
        max_records: 100,
        max_age_ms: 1000,
        grants: vec![Grant {
            id: "admin".into(),
            principal: "app".into(),
            publish: true,
            read: true,
            subscriptions: vec![],
            administer: true,
        }],
        subscriptions: vec![],
    };
    let operation = "b".repeat(32);
    let value = json!({"state":"active","operation":operation,"resource":definition.resource.id,"last_error":null,"accepted_revision":"7","accepted_digest":hex::encode(Sha256::digest(serde_json::to_vec(&definition).unwrap()))});
    (definition, operation, value)
}

#[test]
fn configuration_acceptance_binds_exact_definition_and_historical_operation() -> Result<()> {
    let (definition, operation, value) = configuration_fixture();
    assert_eq!(
        accept_configuration(
            &definition.resource,
            Some((&definition, &operation)),
            value.clone()
        )?
        .state,
        "active"
    );
    // Current status can describe a later operation; operation replay must still match its original request.
    let mut later = value.clone();
    later["operation"] = json!("c".repeat(32));
    later["accepted_revision"] = json!("9");
    accept_configuration(&definition.resource, None, later.clone())?;
    assert!(
        accept_configuration(&definition.resource, Some((&definition, &operation)), later).is_err()
    );
    accept_configuration(&definition.resource, Some((&definition, &operation)), value)?;
    Ok(())
}

#[test]
fn configuration_rejects_wrong_identity_revision_digest_and_state() {
    let (definition, operation, value) = configuration_fixture();
    for (field, replacement) in [
        ("resource", json!("c".repeat(32))),
        ("operation", json!("c".repeat(32))),
        ("state", json!("unknown")),
        ("accepted_revision", json!("8")),
        ("accepted_revision", json!("07")),
        ("accepted_digest", json!("0".repeat(64))),
        ("accepted_digest", serde_json::Value::Null),
        ("last_error", json!("x".repeat(4097))),
    ] {
        let mut bad = value.clone();
        bad[field] = replacement;
        let failure =
            accept_configuration(&definition.resource, Some((&definition, &operation)), bad)
                .err()
                .unwrap();
        assert!(
            failure
                .downcast_ref::<super::super::DeliveryError>()
                .is_some(),
            "untyped {field} failure"
        );
    }
}

#[test]
fn pending_configuration_allows_only_consistent_accepted_metadata() -> Result<()> {
    let (definition, operation, mut value) = configuration_fixture();
    value["state"] = json!("creating");
    value["accepted_revision"] = serde_json::Value::Null;
    value["accepted_digest"] = serde_json::Value::Null;
    assert!(accept_configuration(
        &definition.resource,
        Some((&definition, &operation)),
        value.clone()
    )
    .is_err());
    let initial = Definition {
        revision: 1,
        ..definition.clone()
    };
    accept_configuration(
        &initial.resource,
        Some((&initial, &operation)),
        value.clone(),
    )?;
    value["state"] = json!("updating");
    assert!(accept_configuration(&definition.resource, None, value.clone()).is_err());
    value["accepted_revision"] = json!("6");
    value["accepted_digest"] = json!("d".repeat(64));
    accept_configuration(
        &definition.resource,
        Some((&definition, &operation)),
        value.clone(),
    )?;
    value["accepted_revision"] = json!("7");
    assert!(
        accept_configuration(&definition.resource, Some((&definition, &operation)), value).is_err()
    );
    Ok(())
}

#[test]
fn configuration_error_metadata_retains_bounded_unicode_and_exact_revisions() -> Result<()> {
    let (definition, _, mut value) = configuration_fixture();
    value["last_error"] = json!("🌟".repeat(1024));
    value["accepted_revision"] = json!(i64::MAX.to_string());
    accept_configuration(&definition.resource, None, value.clone())?;
    value["last_error"] = json!("🌟".repeat(1025));
    assert!(accept_configuration(&definition.resource, None, value).is_err());
    Ok(())
}
