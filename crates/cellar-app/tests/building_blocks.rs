use cellar_app::{blob, cron, kv, limits, queue, sql, workflow};
use serde_json::json;

#[test]
fn sql_request_normalization_preserves_typed_refusals() -> anyhow::Result<()> {
    assert_eq!(
        sql::wire::params(Some(&json!([1, null])))?,
        vec![json!(1), json!(null)]
    );
    let error = sql::wire::params(Some(&json!({}))).unwrap_err();
    assert_eq!(
        error.downcast_ref::<sql::Error>().unwrap().family,
        "SQL_ERROR"
    );
    let mut limits = limits::CellLimits::default();
    limits.sql.connection.as_mut().unwrap().max_bind_parameters = 0;
    assert!(limits.validate().is_err());
    Ok(())
}

#[test]
fn queue_codec_enforces_the_supplied_budget_without_a_host() -> anyhow::Result<()> {
    let mut limits = limits::CellLimits::default().queue;
    limits.max_message_bytes = 3;
    limits.max_delay_seconds = 2;
    let encoded = queue::encode(queue::Body::Bytes(vec![1, 2, 3]), 2, limits)?;
    assert_eq!(encoded.body, [1, 2, 3]);
    assert!(queue::encode(queue::Body::Bytes(vec![0; 4]), 2, limits).is_err());
    assert!(queue::encode(queue::Body::Bytes(vec![]), 3, limits).is_err());
    let queue::Body::JsonValue(value) =
        queue::decode(b"-0".to_vec(), queue::QueueContentType::Json)?
    else {
        panic!("negative zero must retain its structured representation");
    };
    assert!(
        matches!(value.root, cellar_native::clone::Value::Number(number) if number == 0.0 && number.is_sign_negative())
    );
    Ok(())
}

#[test]
fn kv_validation_uses_the_supplied_clock_and_expiry_floor() -> anyhow::Result<()> {
    let limits = limits::KvLimits {
        min_expiration_ttl_ms: 2_000,
        ..Default::default()
    };
    assert!(kv::validation::expiry(10_000, None, Some(1.0), limits.min_expiration_ttl_ms).is_err());
    assert_eq!(
        kv::validation::expiry(10_000, None, Some(2.0), limits.min_expiration_ttl_ms)?,
        Some(12_000)
    );
    assert!(
        kv::validation::expiry(i64::MAX - 1, None, Some(2.0), limits.min_expiration_ttl_ms)
            .is_err()
    );
    assert!(kv::RevisionCondition {
        revision: Some("01".into())
    }
    .validate()
    .is_err());
    Ok(())
}

#[test]
fn retention_uses_the_supplied_clock_and_budget() -> anyhow::Result<()> {
    let mut limits = limits::WorkflowLimits {
        max_retention_ms: Some(2_000),
        ..Default::default()
    };
    let retention = workflow::Retention {
        success: Some(workflow::Duration::from("1.5 seconds")),
        error: Some(workflow::Duration::from(2_000.0)),
    };
    assert_eq!(
        retention.wire(limits, 10_000)?,
        json!({"successMs":1500.0,"errorMs":2000.0})
    );
    assert!(retention.wire(limits, i64::MAX - 1).is_err());
    limits.max_retention_ms = Some(1_000);
    assert!(retention.wire(limits, 10_000).is_err());
    Ok(())
}

#[test]
fn cron_retry_control_is_shared_only_within_one_occurrence() {
    let first = cron::Scheduled::new(1_000, "* * * * *".into());
    let retained = first.clone();
    let next = cron::Scheduled::new(2_000, first.cron.clone());
    retained.no_retry();
    assert!(first.retry_declined());
    assert!(!next.retry_declined());
}

#[test]
fn blob_range_and_condition_parsing_need_no_store() -> anyhow::Result<()> {
    assert!(blob::Range::from_header("bytes=10-8").is_err());
    assert_eq!(blob::Range::from_header("bytes=10-9")?.length, Some(0));
    assert!(blob::Range::from_header("bytes=0-18446744073709551615").is_err());
    assert_eq!(blob::Range::from_header("bytes=-5")?.suffix, Some(5));
    let condition =
        blob::Conditional::from_headers(&[("If-Match".into(), "\"version-1\"".into())]).unwrap();
    assert_eq!(condition.etag_matches.as_deref(), Some("\"version-1\""));
    Ok(())
}
