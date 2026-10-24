use super::*;
use serde_json::json;

fn complete() -> Vec<Value> {
    let mut cases = Vec::new();
    for point in ["release", "fail", "fence_output", "fence_sync"] {
        for (caller, _, _) in super::super::fleet::CLASSES {
            for (provider, _, class) in super::super::fleet::CLASSES {
                cases.push(json!({"point":point,"caller":caller,"provider":provider,
                    "node":"proof-owner","class":class,"scope":format!("{class}:named-cell"),"epoch":1,
                    "handler_returned":true,"transaction_open":false,"write_position":2,
                    "peer_proof_held":1,"bucket_proof_held":1,"response_escaped_while_held":false,
                    "initial_replica_members":["proof-ingress"],"replica_members":["proof-ingress","proof-replica"],
                    "baseline_count":"1","local_count":"9007199254740994","expected_count":"9007199254740994",
                    "result":if point == "release" {json!({"value":"9007199254740994"})} else {json!({"code":"Unavailable","outcome":"unknown"})},
                    "count_after_recovery":"9007199254740994","final_count":"9007199254740994",
                    "automatic_replays":0,"receipt_replayed":true,"scope_preserved":true}));
                if matches!(point, "fence_output" | "fence_sync") {
                    let case = cases.last_mut().unwrap();
                    case["fence"] =
                        json!({"code":3,"reason":"NodeLeaseMismatch","domain_joined":true});
                    case["fenced_new_request_refused"] = json!(true);
                    case["acknowledged_receipt_replayed"] = json!(true);
                    case["recovered_node"] = json!("proof-ingress");
                    case["recovered_epoch"] = json!(2);
                }
                if point == "fence_sync" {
                    let case = cases.last_mut().unwrap();
                    case["handler_returned"] = json!(false);
                    case["transaction_open"] = Value::Null;
                    case["write_position"] = Value::Null;
                    let before = json!({"ready":true,"id":format!("proof-fence-sync-{caller}-{provider}"),
                        "node":"proof-owner","class":class,"scope":format!("{class}:named-cell"),"epoch":"1",
                        "active_before":true,"committed":true,"waiting":true,"cancelled":false,"late_done":false,
                        "count":"9007199254740994","receipt":{"delta":"9007199254740993","value":"9007199254740994"}});
                    let mut after = before.clone();
                    for key in [
                        "cancelled",
                        "late_done",
                        "storage_refused",
                        "read_refused",
                        "call_refused",
                        "raw_refused",
                        "bridge_inactive",
                    ] {
                        after[key] = json!(true);
                    }
                    after["waiting"] = json!(false);
                    case["live_before"] = before;
                    case["live_after"] = after;
                }
            }
        }
    }
    cases
}

#[test]
fn proof_evidence_requires_both_fault_outcomes_for_every_pair() {
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
fn proof_evidence_refuses_missing_proof_early_delivery_and_forged_rollback() {
    for (index, key, value) in [
        (0, "peer_proof_held", json!(0)),
        (0, "bucket_proof_held", json!(0)),
        (0, "response_escaped_while_held", json!(true)),
        (0, "handler_returned", json!(false)),
        (0, "transaction_open", json!(true)),
        (0, "write_position", Value::Null),
        (0, "node", json!("proof-ingress")),
        (0, "scope", json!("Other:scope")),
        (0, "replica_members", json!(["proof-ingress"])),
        (
            0,
            "initial_replica_members",
            json!(["proof-ingress", "proof-replica"]),
        ),
        (0, "local_count", json!("1")),
        (0, "final_count", json!("18014398509481987")),
        (0, "receipt_replayed", json!(false)),
        (0, "scope_preserved", json!(false)),
        (0, "automatic_replays", json!(1)),
        (16, "count_after_recovery", json!("2")),
        (16, "result", json!({"value":"9007199254740994"})),
        (
            32,
            "fence",
            json!({"code":3,"reason":"NodeLeaseMismatch","domain_joined":false}),
        ),
        (32, "fenced_new_request_refused", json!(false)),
        (32, "acknowledged_receipt_replayed", json!(false)),
        (32, "recovered_epoch", json!(1)),
        (32, "recovered_node", json!("proof-owner")),
        (
            16,
            "result",
            json!({"code":"Unavailable","outcome":"not_committed"}),
        ),
    ] {
        let mut invalid = complete();
        invalid[index][key] = value;
        assert!(validate(&invalid).is_err(), "accepted invalid {key}");
    }
    let mut rolled_back = complete();
    rolled_back[16]["count_after_recovery"] = json!("1");
    validate(&rolled_back).unwrap(); // Unknown work may be absent after recovery.
}

#[test]
fn fence_only_evidence_cannot_substitute_an_unfenced_or_partial_matrix() {
    let cases: Vec<_> = complete()
        .into_iter()
        .filter(|case| matches!(case["point"].as_str(), Some("fence_output" | "fence_sync")))
        .collect();
    validate_fences(&cases).unwrap();
    assert!(validate(&cases).is_err());
    assert!(validate_fences(&cases[..15]).is_err());
    let released: Vec<_> = complete()
        .into_iter()
        .filter(|case| case["point"] == "release")
        .collect();
    assert!(validate_fences(&released).is_err());
}

#[test]
fn live_fence_requires_the_original_active_handler_and_late_sdk_refusals() {
    for (section, key, value) in [
        ("live_before", "active_before", json!(false)),
        ("live_before", "committed", json!(false)),
        ("live_before", "waiting", json!(false)),
        ("live_before", "cancelled", json!(true)),
        ("live_before", "receipt", Value::Null),
        ("live_before", "scope", json!("Other:scope")),
        ("live_before", "epoch", json!(1)),
        ("live_before", "count", json!(9_007_199_254_740_994_i64)),
        ("live_after", "storage_refused", json!(false)),
        ("live_after", "read_refused", json!(false)),
        ("live_after", "call_refused", json!(false)),
        ("live_after", "late_done", json!(false)),
    ] {
        let mut invalid = complete();
        invalid[48][section][key] = value;
        assert!(
            validate(&invalid).is_err(),
            "accepted invalid {section}.{key}"
        );
    }
    let mut raw = complete();
    raw[49]["live_after"]["raw_refused"] = json!(false); // Rust caller -> Python provider.
    assert!(validate(&raw).is_err());
    let mut returned = complete();
    returned[48]["handler_returned"] = json!(true);
    assert!(validate(&returned).is_err());
}
