// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::GroupManifest;
use cellar_logic::{
    cell_group::{CellRange, GroupHistory, GroupRoute, RouteEpoch},
    consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
};

fn marker(index: u64) -> GroupManifest {
    let mut marker = GroupManifest::genesis(GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    })
    .unwrap();
    marker.position = LogPosition {
        index: LogIndex::new(index),
        term: Term::new(2),
    };
    marker
}

#[test]
fn park_prepare_binds_committed_position_and_membership() {
    let mut marker = marker(3);
    let generation = ParkGeneration::new(3).unwrap();
    let term = Term::new(2);
    let membership = MembershipRevision::new(1).unwrap();
    for (generation, term, membership) in [
        (ParkGeneration::new(2).unwrap(), term, membership),
        (generation, Term::new(3), membership),
        (generation, term, MembershipRevision::new(2).unwrap()),
    ] {
        assert!(marker.prepare_park(generation, term, membership).is_err());
        assert!(marker.park_cut().is_none());
    }
    marker.prepare_park(generation, term, membership).unwrap();
    let cut = marker.park_cut().unwrap();
    assert_eq!(cut.generation(), generation.get());
    assert_eq!(cut.position(), marker.position());
    assert_eq!(cut.membership_revision(), membership.get());
    assert!(marker.is_active());

    let bytes = serde_json::to_vec(&marker).unwrap();
    assert_eq!(
        serde_json::from_slice::<GroupManifest>(&bytes).unwrap(),
        marker
    );
    let mut value = serde_json::to_value(&marker).unwrap();
    value["park"]["term"] = 1.into();
    assert!(serde_json::from_value::<GroupManifest>(value).is_err());
}

#[test]
fn park_cut_survives_advance_and_refuses_checkpoint_regression() {
    let mut prepared = marker(3);
    prepared
        .prepare_park(
            ParkGeneration::new(3).unwrap(),
            Term::new(2),
            MembershipRevision::new(1).unwrap(),
        )
        .unwrap();
    let mut advanced = prepared.clone();
    advanced.position.index = LogIndex::new(4);
    advanced.membership_revision = 4;
    advanced.check().unwrap();
    assert!(advanced.preserves_park_cut(&prepared));
    assert!(!marker(4).preserves_park_cut(&prepared));
    assert!(advanced
        .prepare_park(
            ParkGeneration::new(3).unwrap(),
            Term::new(2),
            MembershipRevision::new(1).unwrap(),
        )
        .is_err());

    let mut forged = marker(5);
    forged
        .prepare_park(
            ParkGeneration::new(5).unwrap(),
            Term::new(2),
            MembershipRevision::new(1).unwrap(),
        )
        .unwrap();
    assert!(!forged.preserves_park_cut(&advanced));

    let mut next = advanced.clone();
    next.position.index = LogIndex::new(5);
    next.prepare_park(
        ParkGeneration::new(5).unwrap(),
        Term::new(2),
        MembershipRevision::new(4).unwrap(),
    )
    .unwrap();
    assert!(next.preserves_park_cut(&advanced));
    assert!(!advanced.preserves_park_cut(&next));
}
