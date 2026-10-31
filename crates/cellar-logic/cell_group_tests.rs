// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn key(digest: u8, id: &str) -> CellKey {
    CellKey::from_digest([digest; 32], CellId::new(id).unwrap())
}

#[test]
fn routing_rejects_foreign_namespace_identity_and_range() {
    let cell = CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new("alpha").unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    };
    let route = GroupRoute {
        history: GroupHistory {
            cluster: cell.cluster,
            namespace: cell.namespace,
            group: GroupId::new([4; 16]).unwrap(),
            incarnation: IncarnationId::new([5; 16]).unwrap(),
        },
        range: CellRange::all().split(key(3, "boundary")).unwrap().0,
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [7; 32],
        schema_digest: [8; 32],
    };
    assert_eq!(route.check_cell(&cell, &key(2, "alpha")), Ok(()));
    assert_eq!(
        route.check_cell(&cell, &key(2, "beta")),
        Err(Refusal::WrongHistory)
    );
    assert_eq!(
        route.check_cell(&cell, &key(4, "alpha")),
        Err(Refusal::WrongHistory)
    );
    let mut foreign = cell.clone();
    foreign.namespace = NamespaceId::new([9; 16]).unwrap();
    assert_eq!(
        route.check_cell(&foreign, &key(2, "alpha")),
        Err(Refusal::WrongHistory)
    );
    foreign.namespace = cell.namespace;
    foreign.cluster = ClusterId::new([9; 16]).unwrap();
    assert_eq!(
        route.check_cell(&foreign, &key(2, "alpha")),
        Err(Refusal::WrongHistory)
    );
}

#[test]
fn ranges_partition_digest_collisions_without_aliases() {
    let first = key(2, "alpha");
    let second = key(2, "beta");
    let third = key(3, "alpha");
    assert!(first < second && second < third);
    let all = CellRange::all();
    let (left, right) = all.split(second.clone()).unwrap();
    for value in [key(0, "a"), first, second, third, key(255, "z")] {
        assert_ne!(left.contains(&value), right.contains(&value));
    }
    assert_eq!(left.merge(&right), Ok(all));
    assert_eq!(right.merge(&left), Err(Refusal::NonContiguous));
    assert!(left.split(key(2, "beta")).is_err());
    assert!(right.split(key(2, "beta")).is_err());
    assert!(left.split(key(3, "outside")).is_err());
    for (lower, upper) in [
        (CellBound::PositiveInfinity, CellBound::PositiveInfinity),
        (CellBound::PositiveInfinity, CellBound::NegativeInfinity),
        (CellBound::NegativeInfinity, CellBound::NegativeInfinity),
    ] {
        assert!(CellRange::new(lower, upper).is_err());
    }
}

#[test]
fn identifiers_and_counters_have_no_silent_normalization_or_wraparound() {
    for value in ["", "\n", "a\0b", "\u{85}", "a\u{7f}"] {
        assert_eq!(CellId::new(value), Err(Refusal::InvalidIdentity));
    }
    assert!(CellId::new("é".repeat(512)).is_ok());
    assert!(CellId::new("é".repeat(513)).is_err());
    assert_ne!(CellId::new("é").unwrap(), CellId::new("e\u{301}").unwrap());
    assert!(MutationSequence::new(0).is_err());
    assert_eq!(
        MutationSequence::new(1)
            .unwrap()
            .checked_next()
            .unwrap()
            .get(),
        2
    );
    assert_eq!(
        MutationSequence::new(u64::MAX).unwrap().checked_next(),
        Err(Refusal::InvalidPosition)
    );
    assert!(RouteEpoch::new(0).is_err());
    assert!(RouteEpoch::new(u64::MAX).unwrap().checked_next().is_err());
    assert!(MembershipRevision::new(0).is_err());
    assert!(MembershipRevision::new(u64::MAX)
        .unwrap()
        .checked_next()
        .is_err());
}

#[test]
fn cell_versions_require_exact_history_and_same_sequence_image() {
    let version = CellVersion {
        history: CellHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: CellId::new("alpha").unwrap(),
            incarnation: IncarnationId::new([3; 16]).unwrap(),
        },
        sequence: MutationSequence::new(2).unwrap(),
        image: CellImage {
            epoch: 1,
            txid: 2,
            checksum: 123,
            commitment: CellImageCommitment::Materialized([7; 32]),
        },
    };
    assert_eq!(version.covers(&version), Ok(true));
    let mut later = version.clone();
    later.sequence = later.sequence.checked_next().unwrap();
    later.image.txid += 1;
    assert_eq!(later.covers(&version), Ok(true));
    assert_eq!(version.covers(&later), Ok(false));
    for field in 0..4 {
        let mut other = later.clone();
        match field {
            0 => other.history.cluster = ClusterId::new([9; 16]).unwrap(),
            1 => other.history.namespace = NamespaceId::new([9; 16]).unwrap(),
            2 => other.history.cell = CellId::new("beta").unwrap(),
            _ => other.history.incarnation = IncarnationId::new([9; 16]).unwrap(),
        }
        assert_eq!(other.covers(&version), Err(Refusal::WrongHistory));
    }
    for field in 0..4 {
        let mut conflicting = version.clone();
        match field {
            0 => conflicting.image.epoch += 1,
            1 => conflicting.image.txid += 1,
            2 => conflicting.image.checksum += 1,
            _ => {
                conflicting.image.commitment = CellImageCommitment::Extents {
                    root: [7; 32],
                    manifest_bytes: 112,
                }
            }
        }
        assert_eq!(conflicting.covers(&version), Err(Refusal::WrongHistory));
    }
}

#[test]
fn physical_history_projection_preserves_every_group_identity_field() {
    let original = crate::consensus::GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
        cell: "alpha".into(),
    };
    let history = GroupHistory::from(&original);
    let mut other = original.clone();
    other.cell = "beta".into();
    assert_eq!(GroupHistory::from(&other), history);
    for field in 0..4 {
        let mut other = original.clone();
        match field {
            0 => other.cluster = ClusterId::new([9; 16]).unwrap(),
            1 => other.namespace = NamespaceId::new([9; 16]).unwrap(),
            2 => other.group = GroupId::new([9; 16]).unwrap(),
            _ => other.incarnation = IncarnationId::new([9; 16]).unwrap(),
        }
        assert_ne!(GroupHistory::from(&other), history);
    }
}
