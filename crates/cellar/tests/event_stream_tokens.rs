// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public resume inputs must preserve typed errors and full-range positions.

use base64::Engine;
use cellar::sdk::event_stream::{Cursor, DeliveryError, ErrorCode, Receipt};
use serde_json::{json, Value};

fn token(bytes: impl AsRef<[u8]>) -> String {
    format!(
        "v1.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    )
}

fn fields(value: &Value) -> String {
    token(serde_json::to_vec(value).unwrap())
}

fn assert_code(error: anyhow::Error, expected: ErrorCode) {
    assert_eq!(
        error
            .downcast_ref::<DeliveryError>()
            .map(|error| error.code),
        Some(expected),
        "public token error must remain actionable: {error}"
    );
}

fn receipt() -> Value {
    json!({
        "resource": "00000000000000000000000000000001",
        "partition": 0,
        "name": "projection",
        "subscription": "00000000000000000000000000000002",
        "incarnation": "00000000000000000000000000000003",
        "generation": i64::MAX.to_string(),
        "token": "ab".repeat(32),
        "first": "9007199254740993",
        "last": i64::MAX.to_string()
    })
}

#[test]
fn invalid_resume_cursors_always_return_invalid_cursor() {
    let cursor = json!({
        "resource": "00000000000000000000000000000001",
        "partition": 0,
        "next": "1"
    });
    let mut cases = vec![
        String::new(),
        "v2.abc".into(),
        "v1.%%%".into(),
        "x".repeat(513),
        token(b"\xff"),
        token(b"{"),
        fields(&json!({})),
    ];
    for (key, value) in [
        ("resource", json!("invalid")),
        ("partition", json!(1)),
        ("next", json!("0")),
        ("next", json!("01")),
        ("next", json!(1)),
        ("next", json!("9223372036854775808")),
        ("unexpected", json!(true)),
    ] {
        let mut invalid = cursor.clone();
        invalid[key] = value;
        cases.push(fields(&invalid));
    }
    for invalid in cases {
        assert_code(
            Cursor::decode(&invalid).unwrap_err(),
            ErrorCode::InvalidCursor,
        );
    }
}

#[test]
fn invalid_delivery_receipts_always_return_stale_receipt() {
    let mut cases = vec![
        String::new(),
        "v2.abc".into(),
        "v1.%%%".into(),
        "x".repeat(2049),
        token(b"\xff"),
        token(b"{"),
        fields(&json!({})),
    ];
    for (key, value) in [
        ("resource", json!("invalid")),
        ("subscription", json!("invalid")),
        ("incarnation", json!("invalid")),
        ("partition", json!(1)),
        ("name", json!("")),
        ("token", json!("AB".repeat(32))),
        ("generation", json!("0")),
        ("first", json!("01")),
        ("last", json!(1)),
        ("last", json!("1")),
        ("last", json!("9223372036854775808")),
        ("unexpected", json!(true)),
    ] {
        let mut invalid = receipt();
        invalid[key] = value;
        cases.push(fields(&invalid));
    }
    for invalid in cases {
        assert_code(
            Receipt::decode(&invalid).unwrap_err(),
            ErrorCode::StaleReceipt,
        );
    }
}

#[test]
fn resume_tokens_preserve_positions_above_binary64_precision() {
    for next in [9_007_199_254_740_993, i64::MAX] {
        let original = Cursor {
            resource: "00000000000000000000000000000001".into(),
            partition: 0,
            next,
        };
        assert_eq!(
            Cursor::decode(&original.encode().unwrap()).unwrap(),
            original
        );
    }
    let original = fields(&receipt());
    let decoded = Receipt::decode(&original).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), receipt());
    assert_eq!(
        Receipt::decode(&decoded.encode().unwrap()).unwrap(),
        decoded
    );
}
