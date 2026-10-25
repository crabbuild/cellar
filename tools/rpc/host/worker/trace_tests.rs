// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn checkpoint_requires_a_closed_transaction_and_matching_receipt() {
    let delta = 9_007_199_254_740_993;
    let valid = json!({"count":delta.to_string(),"before_receipt":null,
        "after_receipt":{"delta":delta.to_string(),"value":delta.to_string()},
        "transaction_open":false,"write_position":19});
    assert!(checkpoint_matches(&valid, true, delta));
    assert!(!checkpoint_matches(&valid, false, delta));
    for (field, value) in [
        ("count", json!(delta)),
        ("count", json!("0")),
        ("after_receipt", Value::Null),
        (
            "after_receipt",
            json!({"delta":"1","value":delta.to_string()}),
        ),
        ("before_receipt", json!({"delta":"1","value":"1"})),
        ("transaction_open", json!(true)),
        ("write_position", json!(0)),
    ] {
        let mut changed = valid.clone();
        changed[field] = value;
        assert!(
            !checkpoint_matches(&changed, true, delta),
            "accepted wrong checkpoint field {field}"
        );
    }
}

#[test]
fn before_write_checkpoint_cannot_contain_either_receipt() {
    let mut value = json!({"count":null,"before_receipt":null,"after_receipt":null,
        "transaction_open":false,"write_position":1});
    assert!(checkpoint_matches(&value, false, 1));
    assert!(!checkpoint_matches(
        &json!({"transaction_open":false,"write_position":1}),
        false,
        1
    ));
    value["before_receipt"] = json!({"delta":"1","value":"1"});
    assert!(!checkpoint_matches(&value, false, 1));
}
