// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn closed() -> RaftStopProof {
    RaftStopProof::Closed {
        startup_incomplete: false,
        owners: RaftStopOwners {
            node: [1; 16],
            disk: [2; 16],
            process: [3; 16],
            engine: [4; 16],
            groups: 1_000_000,
            omitted_group_details: 999_999,
            groups_with_archive_lag: 800_000,
            groups_with_unknown_archive: 100_000,
            unfinished_commands: 0,
        },
        handoff: RaftStopHandoff::Observed {
            groups_examined: 10,
            attempts: 2,
            accepted: 1,
            leadership_released: 0,
            unavailable_groups: 9,
            deadline_elapsed: false,
        },
    }
}

#[test]
fn raft_stop_receipt_has_constant_aggregate_size() {
    let proof = closed();
    assert!(proof.complete());
    // Large counts here exercise wire bounds, not supported group capacity.
    let bytes = serde_json::to_vec(&proof).unwrap();
    assert!(bytes.len() < 2048);
    let decoded: RaftStopProof = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(decoded, proof);
    assert!(decoded.complete());
}

#[test]
fn raft_stop_refuses_incomplete_work_and_unknown_handoff() {
    for handoff in [
        RaftStopHandoff::Missing,
        RaftStopHandoff::Failed,
        RaftStopHandoff::Observed {
            groups_examined: 1,
            attempts: 1,
            accepted: 1,
            leadership_released: 0,
            unavailable_groups: 0,
            deadline_elapsed: true,
        },
    ] {
        let mut proof = closed();
        if let RaftStopProof::Closed { handoff: value, .. } = &mut proof {
            *value = handoff;
        }
        assert!(!proof.complete());
    }
    let mut proof = closed();
    if let RaftStopProof::Closed { owners, .. } = &mut proof {
        owners.unfinished_commands = 1;
    }
    assert!(!proof.complete());
    let mut proof = closed();
    if let RaftStopProof::Closed {
        startup_incomplete, ..
    } = &mut proof
    {
        *startup_incomplete = true;
    }
    assert!(!proof.complete());
    assert!(!RaftStopProof::Failed {
        startup_incomplete: false
    }
    .complete());
    assert!(!RaftStopProof::Failed {
        startup_incomplete: true
    }
    .complete());
    assert!(RaftStopProof::NotRegistered.complete());
}

#[test]
fn raft_stop_refuses_fabricated_identity_counts_and_fields() {
    let valid = serde_json::to_value(closed()).unwrap();
    for key in ["node", "disk", "process", "engine"] {
        let mut invalid = valid.clone();
        invalid["owners"][key] = serde_json::json!(vec![0; 16]);
        let proof: RaftStopProof = serde_json::from_value(invalid).unwrap();
        assert!(proof.validate().is_err());
        assert!(!proof.complete());
    }
    for (path, value) in [
        ("omitted_group_details", usize::MAX),
        ("groups_with_archive_lag", usize::MAX),
        ("groups_with_unknown_archive", usize::MAX),
    ] {
        let mut invalid = valid.clone();
        invalid["owners"][path] = serde_json::json!(value);
        assert!(serde_json::from_value::<RaftStopProof>(invalid)
            .unwrap()
            .validate()
            .is_err());
    }
    let mut invalid = valid.clone();
    invalid["handoff"]["leadership_released"] = serde_json::json!(2);
    assert!(serde_json::from_value::<RaftStopProof>(invalid)
        .unwrap()
        .validate()
        .is_err());
    let mut invalid = valid;
    invalid["retain_replica_disks"] = serde_json::json!(false);
    assert!(serde_json::from_value::<RaftStopProof>(invalid).is_err());
    assert!(serde_json::from_str::<RaftStopProof>("{}").is_err());
}
