// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Outcome proof is explicit; identical failure codes need not mean rejection.
use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn advice() -> LeaderAdvice {
    LeaderAdvice {
        history: cellar_logic::cell_group::GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        membership_revision: 17,
        configuration_digest: [5; 32],
        term: 19,
        leader: MemberId::new(7).unwrap(),
    }
}

fn decode(bytes: &[u8], limit: usize) -> Result<CellCommandReply, TransportError> {
    let advice = advice();
    let cell = CellHistory {
        cluster: advice.history.cluster,
        namespace: advice.history.namespace,
        cell: cellar_logic::cell_group::CellId::new("Counter:test").unwrap(),
        incarnation: advice.history.incarnation,
    };
    let id = CommandId::new(ClientId::new([8; 16]).unwrap(), 1).unwrap();
    let keys = VersionKeys::new(
        crate::raft::version::VersionSigningKey::new([9; 32], u64::MAX).unwrap(),
        vec![],
        std::time::Duration::from_secs(60),
    )
    .unwrap();
    decode_reply(
        bytes,
        (id, &cell, Durability::Quorum),
        [10; 32],
        &keys,
        1000,
        limit,
        advice.leader,
    )
}

#[test]
fn not_leader_and_timeout_are_not_implicit_rejection_proofs() {
    for failure in [
        CommandFailure::NotLeader {
            leader: Some(advice().leader),
        },
        CommandFailure::Unavailable,
        CommandFailure::Overloaded,
    ] {
        for disposition in [
            MutationDisposition::RejectedBeforeAdmission,
            MutationDisposition::AdmittedOrUnknown,
        ] {
            let bytes =
                encode_reply(&Err(failure.clone()), disposition, Some(&advice()), 4096).unwrap();
            let reply = decode(&bytes, 4096).unwrap();
            assert_eq!(reply.disposition, disposition);
            assert_eq!(reply.result.err(), Some(failure.clone()));
            assert_eq!(reply.advice, Some(advice()));
        }
    }
}

#[test]
fn command_advice_and_disposition_are_mandatory_bounded_and_canonical() {
    let bytes = encode_reply(
        &Err(CommandFailure::Unavailable),
        MutationDisposition::AdmittedOrUnknown,
        Some(&advice()),
        4096,
    )
    .unwrap();
    assert!(matches!(
        decode(&bytes, bytes.len() - 1),
        Err(TransportError::Limit)
    ));
    assert!(matches!(
        encode_reply(
            &Err(CommandFailure::Unavailable),
            MutationDisposition::Applied,
            None,
            4096
        ),
        Err(TransportError::Protocol)
    ));
    for end in 0..bytes.len() {
        assert!(decode(&bytes[..end], 4096).is_err(), "truncation {end}");
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode(&trailing, 4096).is_err());
    let mut invalid = bytes;
    invalid[8] = 0; // An error cannot claim an applied receipt.
    assert!(decode(&invalid, 4096).is_err());
    invalid[8] = 255;
    assert!(decode(&invalid, 4096).is_err());
    invalid[8] = 2;
    invalid[9] = 255;
    assert!(decode(&invalid, 4096).is_err());
    let mut old = b"CLGCCS01".to_vec();
    old.push(7); // The predecessor's unclassified error format is refused.
    assert!(decode(&old, 4096).is_err());
}

#[test]
fn probe_carries_actual_term_and_configuration_without_inventing_a_lease() {
    for leader in [false, true] {
        for observation in [None, Some(advice())] {
            let bytes = encode_probe_result(leader, observation.as_ref(), 4096).unwrap();
            assert_eq!(
                decode_probe_result(&bytes, 4096).unwrap(),
                (leader, observation)
            );
            assert!(decode_probe_result(&bytes, bytes.len() - 1).is_err());
            for end in 0..bytes.len() {
                assert!(decode_probe_result(&bytes[..end], 4096).is_err());
            }
            let mut trailing = bytes;
            trailing.push(0);
            assert!(decode_probe_result(&trailing, 4096).is_err());
        }
    }
    assert!(decode_probe_result(b"CLGCPS01\x01", 4096).is_err());
    for field in 0..3 {
        let mut invalid = advice();
        match field {
            0 => invalid.term = 0,
            1 => invalid.membership_revision = 0,
            _ => invalid.configuration_digest = [0; 32],
        }
        assert!(encode_probe_result(true, Some(&invalid), 4096).is_err());
    }
}
