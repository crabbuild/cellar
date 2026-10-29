// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-group protocol boundary tests. These do not qualify a running group.

use cellar::application::consensus::verification::cell_groups::*;
use cellar::application::consensus::NamespaceId;
use cellar::application::consensus::{
    ClusterId, GroupId, IncarnationId, LogIndex, LogPosition, ProposalId, Term, VersionError,
    VersionKeys, VersionSigningKey, VersionToken,
};
use std::time::Duration;

fn observation() -> CellVersionObservation {
    let history = CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new("alpha").unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    };
    let group = GroupHistory {
        cluster: history.cluster,
        namespace: history.namespace,
        group: GroupId::new([4; 16]).unwrap(),
        incarnation: IncarnationId::new([5; 16]).unwrap(),
    };
    CellVersionObservation {
        version: CellVersion {
            history,
            sequence: MutationSequence::new(7).unwrap(),
            image: CellImage {
                epoch: 1,
                txid: 10,
                checksum: 17,
                commitment: CellImageCommitment::Materialized([8; 32]),
            },
        },
        witness: CommitWitness {
            group,
            position: LogPosition {
                index: LogIndex::new(100),
                term: Term::new(9),
            },
            proposal: ProposalId::new([6; 16]).unwrap(),
        },
    }
}

fn keys() -> VersionKeys {
    VersionKeys::new(
        VersionSigningKey::new([7; 32], 100_000).unwrap(),
        vec![],
        Duration::from_secs(10),
    )
    .unwrap()
}

#[test]
fn version_survives_transfer_not_disaster() {
    let keys = keys();
    let original = observation();
    let schema = [42; 32];
    let token = keys.issue_cell(&original, schema, 100).unwrap();
    assert!(VersionToken::from_encoded(token.as_str()).is_err());
    let decoded = keys
        .verify_cell(&token, &original.version.history, schema, 101)
        .unwrap();
    assert_eq!(decoded, original);
    let mut moved = original.clone();
    moved.witness.group.group = GroupId::new([10; 16]).unwrap();
    moved.witness.group.incarnation = IncarnationId::new([11; 16]).unwrap();
    moved.witness.position = LogPosition {
        index: LogIndex::new(2),
        term: Term::new(1),
    };
    moved.version.sequence = moved.version.sequence.checked_next().unwrap();
    moved.version.image.txid += 1;
    let successor = keys.issue_cell(&moved, schema, 102).unwrap();
    let successor = keys
        .verify_cell(&successor, &original.version.history, schema, 103)
        .unwrap();
    assert!(successor.version.covers(&decoded.version).unwrap());
    assert!(!decoded.version.covers(&successor.version).unwrap());
    for field in 0..4 {
        let mut other = original.version.history.clone();
        match field {
            0 => other.cluster = ClusterId::new([12; 16]).unwrap(),
            1 => other.namespace = NamespaceId::new([12; 16]).unwrap(),
            2 => other.cell = CellId::new("beta").unwrap(),
            _ => other.incarnation = IncarnationId::new([12; 16]).unwrap(),
        }
        assert_eq!(
            keys.verify_cell(&token, &other, schema, 103),
            Err(VersionError::WrongHistory)
        );
    }
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, [43; 32], 103),
        Err(VersionError::WrongSchema)
    );
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, schema, 10_100),
        Err(VersionError::Expired)
    );
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, schema, 99),
        Err(VersionError::Expired)
    );
}

#[test]
fn cell_token_bounds_tampering_and_commitment_domains() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let keys = keys();
    let mut original = observation();
    original.version.history.cell = CellId::new("a").unwrap();
    let shortest = keys.issue_cell(&original, [42; 32], 100).unwrap();
    assert_eq!(
        URL_SAFE_NO_PAD.decode(shortest.as_str()).unwrap().len(),
        368
    );
    assert_eq!(
        keys.verify_cell(&shortest, &original.version.history, [42; 32], 101)
            .unwrap(),
        original
    );
    original.version.history.cell = CellId::new("é".repeat(512)).unwrap();
    original.version.image.commitment = CellImageCommitment::Extents {
        root: [9; 32],
        manifest_bytes: 112,
    };
    let token = keys.issue_cell(&original, [42; 32], 100).unwrap();
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, [42; 32], 101)
            .unwrap(),
        original
    );
    let bytes = URL_SAFE_NO_PAD.decode(token.as_str()).unwrap();
    for offset in [0, 8, 40, 120, bytes.len() - 65, bytes.len() - 1] {
        let mut changed = bytes.clone();
        changed[offset] ^= 1;
        if let Ok(tampered) = CellVersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(changed)) {
            assert!(keys
                .verify_cell(&tampered, &original.version.history, [42; 32], 101)
                .is_err());
        }
    }
    for end in 0..bytes.len() {
        if let Ok(truncated) =
            CellVersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(&bytes[..end]))
        {
            assert!(keys
                .verify_cell(&truncated, &original.version.history, [42; 32], 101)
                .is_err());
        }
    }
    assert_eq!(
        CellVersionToken::from_encoded(&"a".repeat(2049)),
        Err(VersionError::Limit)
    );
    original.version.image.commitment = CellImageCommitment::Extents {
        root: [9; 32],
        manifest_bytes: 113,
    };
    assert_eq!(
        keys.issue_cell(&original, [42; 32], 100),
        Err(VersionError::Invalid)
    );
    original.version.image.commitment = CellImageCommitment::Materialized([9; 32]);
    original.witness.group.namespace = NamespaceId::new([15; 16]).unwrap();
    assert_eq!(
        keys.issue_cell(&original, [42; 32], 100),
        Err(VersionError::Invalid)
    );
}

#[test]
fn identity_and_hash_boundaries() {
    let namespace = NamespaceId::new([2; 16]).unwrap();
    let alpha = CellId::new("alpha").unwrap();
    let key = cell_routing_key(namespace, &alpha);
    // Independent SHA-256 golden vector for the specified preimage, including NUL.
    assert_eq!(
        hex::encode(key.digest()),
        "9125bede939b7512fcd2b4567a7c60ee066a3baf5847a265088e48449e408f1b"
    );
    assert_ne!(
        key,
        cell_routing_key(NamespaceId::new([3; 16]).unwrap(), &alpha)
    );
    let tied = CellKey::from_digest(key.digest(), CellId::new("beta").unwrap());
    assert!(key < tied);
    let (left, right) = CellRange::all().split(tied.clone()).unwrap();
    assert!(left.contains(&key) && !right.contains(&key));
    assert!(!left.contains(&tied) && right.contains(&tied));
    assert_eq!(left.merge(&right).unwrap(), CellRange::all());
    assert!(right.merge(&left).is_err());
    assert!(CellRange::new(CellBound::PositiveInfinity, CellBound::NegativeInfinity).is_err());
    assert!(CellId::new("").is_err());
    assert!(CellId::new("a\nb").is_err());
    assert!(CellId::new("a".repeat(1025)).is_err());
    assert!(MutationSequence::new(u64::MAX)
        .unwrap()
        .checked_next()
        .is_err());
}
