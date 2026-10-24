use super::*;
use serde_json::json;

fn complete() -> Vec<Value> {
    let mut cases = Vec::new();
    for point in ["missing_root", "replaced_root"] {
        for (caller, _, _) in super::super::fleet::CLASSES {
            for (provider, _, class) in super::super::fleet::CLASSES {
                cases.push(json!({"point":point,"caller":caller,"provider":provider,"scope":format!("{class}:named-cell"),
                    "epoch_before":1,"epoch_after":2,"baseline_count":"9007199254740994","final_count":"9007199254740995",
                    "failure_count":2,"failures":[{"code":"Unavailable","outcome":"unknown"},{"code":"Unavailable","outcome":"unknown"}],
                    "metadata_fault_reads":1,"factories_before_restore":0,"handlers_before_restore":0,
                    "peer_append_acks":1,"bucket_publications_held":1,"retained_follower_batches":1,
                    "live_successor_preserved":true,"original_root_restored":true,"bucket_proof_preserved":true,
                    "follower_proof_preserved":true,"log_only_acknowledgement_verified":true,
                    "acknowledged_receipt_replayed":true,"pending_receipt_replayed":true,"receipt_conflict_verified":true,
                    "owner_process_group_exited":true,"node_worker_group_exited":true,"automatic_replays":0}));
            }
        }
    }
    cases
}

#[test]
fn recovery_requires_both_faults_for_every_pair() {
    let valid = complete();
    validate(&valid).unwrap();
    assert!(validate(&valid[..16]).is_err());
    let mut duplicate = valid.clone();
    duplicate[31] = duplicate[0].clone();
    assert!(validate(&duplicate).is_err());
    let mut unknown = valid;
    unknown[0]["provider"] = json!("unknown");
    assert!(validate(&unknown).is_err());
}

#[test]
fn recovery_refuses_unobserved_faults_provider_entry_and_forged_rollback() {
    for (key, value) in [
        ("metadata_fault_reads", json!(0)),
        ("peer_append_acks", json!(0)),
        ("bucket_publications_held", json!(0)),
        ("retained_follower_batches", json!(0)),
        ("factories_before_restore", json!(1)),
        ("handlers_before_restore", json!(1)),
        ("live_successor_preserved", json!(false)),
        ("original_root_restored", json!(false)),
        ("bucket_proof_preserved", json!(false)),
        ("follower_proof_preserved", json!(false)),
        ("log_only_acknowledgement_verified", json!(false)),
        ("pending_receipt_replayed", json!(false)),
        ("acknowledged_receipt_replayed", json!(false)),
        ("receipt_conflict_verified", json!(false)),
        ("automatic_replays", json!(1)),
        ("owner_process_group_exited", json!(false)),
        ("node_worker_group_exited", json!(false)),
        ("epoch_after", json!(1)),
        ("final_count", json!("9007199254740996")),
        ("scope", json!("Other:scope")),
        ("failure_count", json!(1)),
        (
            "failures",
            json!([{"code":"Unavailable","outcome":"not_committed"},{"code":"Unavailable","outcome":"unknown"}]),
        ),
    ] {
        let mut invalid = complete();
        invalid[0][key] = value;
        assert!(
            validate(&invalid).is_err(),
            "accepted invalid recovery {key}"
        );
    }
}
