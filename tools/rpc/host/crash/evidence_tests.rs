// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

fn complete() -> Vec<Value> {
    let mut cases = Vec::new();
    for point in [
        "before_handler",
        "after_synced_handler",
        "after_count_before_receipt",
    ] {
        for caller in ["rust", "python", "v8", "node"] {
            for provider in ["rust", "python", "v8", "node"] {
                let after = point == "after_synced_handler";
                let partial = point == "after_count_before_receipt";
                cases.push(json!({"point":point,"caller":caller,"provider":provider,
                    "owner_signal":"SIGKILL","owner_process_group_exited":true,
                    "first_failure":{"code":"Unavailable","outcome":"unknown"},
                    "count_at_crash":if after || partial {"9007199254740993"} else {"0"},
                    "count_after_recovery":if after {"9007199254740993"} else {"0"},
                    "transaction_open":partial,
                    "receipt_at_crash":if after {json!({"delta":"9007199254740993","value":"9007199254740993"})} else {Value::Null},
                    "typed_reply_held":after,"final_count":"9007199254740993",
                    "duplicate_mutations":0,"receipt_replayed":true,"scope_preserved":true,
                    "epoch_before":1,"epoch_after":2}));
                if partial {
                    let case = cases.last_mut().unwrap();
                    case["transaction_observer"] = json!(if provider == "rust" {
                        "native_callback"
                    } else {
                        "sqlite_trace_profile"
                    });
                    if provider != "rust" {
                        case["sqlite_checkpoint"] = json!({"statement_finished":true,
                            "receipt_read_rows":0,"count_changes":1,
                            "total_changes_before":7,"total_changes_after":8});
                    }
                }
            }
        }
    }
    cases
}

#[test]
fn crash_evidence_requires_all_three_points_for_every_backend_pair() {
    let valid = complete();
    validate(&valid).unwrap();
    let mut missing = valid.clone();
    missing.pop();
    assert!(validate(&missing).is_err());
    let mut duplicate = valid.clone();
    duplicate[31] = duplicate[0].clone();
    assert!(validate(&duplicate).is_err());
    let mut unknown = valid;
    unknown[0]["provider"] = json!("unregistered");
    assert!(validate(&unknown).is_err());
}

#[test]
fn crash_evidence_refuses_unproven_kills_replies_receipts_or_rollback_claims() {
    for (index, key, value) in [
        (0, "owner_signal", json!("SIGTERM")),
        (0, "owner_process_group_exited", json!(false)),
        (0, "final_count", json!("18014398509481986")),
        (0, "duplicate_mutations", json!(1)),
        (0, "receipt_replayed", json!(false)),
        (0, "epoch_after", json!(1)),
        (0, "scope_preserved", json!(false)),
        (0, "count_at_crash", json!("9007199254740993")),
        (16, "typed_reply_held", json!(false)),
        (16, "count_at_crash", json!("0")),
        (32, "transaction_open", json!(false)),
        (32, "count_after_recovery", json!("9007199254740993")),
        (
            32,
            "receipt_at_crash",
            json!({"delta":"9007199254740993","value":"9007199254740993"}),
        ),
        (32, "count_at_crash", json!("0")),
        (32, "typed_reply_held", json!(true)),
        (32, "transaction_observer", Value::Null),
        (33, "transaction_observer", json!("native_callback")),
        (33, "sqlite_checkpoint", Value::Null),
    ] {
        let mut invalid = complete();
        invalid[index][key] = value;
        assert!(validate(&invalid).is_err(), "accepted invalid {key}");
    }
    let mut rollback = complete();
    rollback[0]["first_failure"]["outcome"] = json!("not_committed");
    assert!(validate(&rollback).is_err());
}

#[test]
fn guest_crash_evidence_requires_a_finished_write_and_an_absent_receipt() {
    for (key, value) in [
        ("statement_finished", json!(false)),
        ("receipt_read_rows", json!(1)),
        ("count_changes", json!(0)),
        ("total_changes_before", json!(-1)),
        ("total_changes_after", json!(7)),
    ] {
        let mut invalid = complete();
        invalid[33]["sqlite_checkpoint"][key] = value;
        assert!(validate(&invalid).is_err(), "accepted invalid SQLite {key}");
    }
    let missing_guest_midpoints: Vec<_> = complete()
        .into_iter()
        .filter(|case| case["point"] != "after_count_before_receipt" || case["provider"] == "rust")
        .collect();
    assert_eq!(missing_guest_midpoints.len(), 36);
    assert!(validate(&missing_guest_midpoints).is_err());
}
