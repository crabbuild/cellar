// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn complete() -> Vec<Value> {
    let mut rows = Vec::new();
    for trigger in ["signal", "deadline"] {
        for topology in ["local", "remote"] {
            for phase in ["output", "sync"] {
                for (caller, _, _) in super::super::fleet::CLASSES {
                    for (provider, _, class) in super::super::fleet::CLASSES {
                        let code = if trigger == "signal" {
                            "Cancelled"
                        } else {
                            "DeadlineExceeded"
                        };
                        let result = if caller == "python" && trigger == "signal" {
                            json!({"cancelled":true})
                        } else {
                            json!({"code":code,"outcome":"unknown"})
                        };
                        let mut row = json!({"caller":caller,"provider":provider,"class":class,"phase":phase,"trigger":trigger,"topology":topology,
                            "ingress":if topology == "local" {"proof-owner"} else {"proof-ingress"},"owner":"proof-owner","scope":format!("{class}:test"),"epoch":1,
                            "result":result,"settled_while_held":{"settled":true,"followup_writes":0,"result":result},"final_followup_writes":0,
                            "parent_live_during_cleanup":true,"proof_held_through_cleanup":true,"peer_proof_held":1,"bucket_proof_held":1,
                            "initial_replica_members":["proof-ingress"],"replica_members":["proof-ingress","proof-replica"],
                            "baseline_count":"1","expected_count":"9007199254740994","local_count":"9007199254740994","final_count":"9007199254740994",
                            "receipt_replayed":true,"automatic_replays":0,"owner_scope_epoch_verified":true,"handler_returned":phase == "output",
                            "transaction_open":false,"write_position":1,
                            "live_before":{"ready":true,"committed":true,"active_before":true,"waiting":true,"cancelled":false,"late_done":false},
                            "live_after":{"cancelled":true,"late_done":true,"storage_refused":true,"read_refused":true,"call_refused":true,"authority_revoked":true,"raw_refused":true,"bridge_inactive":true}});
                        row["response_usage_while_proof_held"] =
                            json!({"active_buffers":1,"reserved_response_bytes":66000});
                        row["response_usage_after_caller_settled"] =
                            json!({"active_buffers":0,"reserved_response_bytes":0});
                        row["response_usage_after_case"] =
                            json!({"active_buffers":0,"reserved_response_bytes":0});
                        rows.push(row);
                    }
                }
            }
        }
    }
    rows
}

#[test]
fn requires_every_pair_trigger_wait_and_route_once() {
    let rows = complete();
    validate(&rows).unwrap();
    assert!(validate(&rows[..rows.len() - 1]).is_err());
    let mut duplicate = rows.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(validate(&duplicate).is_err());
}

#[test]
fn refuses_optimistic_outcomes_and_missing_cleanup_or_proof() {
    for (key, value) in [
        ("proof_held_through_cleanup", json!(false)),
        ("final_followup_writes", Value::Null),
        (
            "response_usage_while_proof_held",
            json!({"active_buffers":0,"reserved_response_bytes":0}),
        ),
        (
            "response_usage_after_caller_settled",
            json!({"active_buffers":1,"reserved_response_bytes":66000}),
        ),
        ("response_usage_after_case", Value::Null),
        ("automatic_replays", json!(1)),
        ("peer_proof_held", json!(0)),
        ("local_count", json!("01")),
        (
            "result",
            json!({"code":"Cancelled","outcome":"not_committed"}),
        ),
    ] {
        let mut rows = complete();
        rows[0][key] = value;
        assert!(validate(&rows).is_err(), "accepted weakened {key}");
    }
    let mut rows = complete();
    let native_sync = rows
        .iter_mut()
        .find(|row| row["provider"] == "rust" && row["phase"] == "sync")
        .unwrap();
    native_sync["live_after"]["authority_revoked"] = json!(false);
    assert!(validate(&rows).is_err());
}
