// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    cell_group::{CellRange, GroupRoute, RouteEpoch},
    consensus::*,
};

fn observation(cells: bool) -> LocalObservation {
    let mut node = super::super::node_inspection_wire::tests::view();
    node.node.groups = 1;
    node.node.reserved_bytes = node.node.profile.bytes_per_group;
    node.registered_histories = 1;
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let route = GroupRoute {
        history,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [6; 32],
        schema_digest: [5; 32],
    };
    let mut root = serde_json::to_value(GroupManifest::genesis(route).unwrap()).unwrap();
    root["index"] = serde_json::json!(5);
    root["term"] = serde_json::json!(2);
    if cells {
        root["root"] = serde_json::json!({"digest":vec![7;32],"height":2,"cells":2,"last_mutation_index":4,"last_mutation_term":2,"next_due_unix_millis":null});
    }
    let root: GroupManifest = serde_json::from_value(root).unwrap();
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let binding =
        FleetGroupBinding::new(IncarnationId::new([80; 16]).unwrap(), &history, [81; 32]).unwrap();
    let membership = GroupMembership::managed_initial(&conf, binding).unwrap();
    let snapshot = GroupSnapshot {
        partition: None,
        replica: ReplicaIdentity {
            scope: history,
            member: MemberId::new(1).unwrap(),
            node: node.node.identity.node,
            disk: node.node.identity.disk,
            process: node.node.identity.process,
        },
        engine: node.node.storage_instance,
        barrier: LogPosition {
            index: LogIndex::new(5),
            term: Term::new(2),
        },
        status: CellGroupStatus {
            term: Term::new(2),
            leader: Some(1),
            applied: root.position(),
            committed: LogIndex::new(5),
            ready: true,
        },
        root,
        membership,
        configuration: conf,
        load: None,
    };
    LocalObservation {
        node: Box::new(node),
        snapshot,
    }
}

#[test]
fn fleet_quorum_body_roundtrips_complete_group_roots_and_explicit_empty() {
    for cells in [false, true] {
        let observed = observation(cells);
        let bytes = encode(&observed).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(*decoded.node, *observed.node);
        assert_eq!(decoded.snapshot.root, observed.snapshot.root);
        assert_eq!(
            decoded.snapshot.root.root().cells(),
            if cells { 2 } else { 0 }
        );
        assert_eq!(decoded.snapshot.membership, observed.snapshot.membership);
        assert_eq!(
            decoded.snapshot.configuration,
            observed.snapshot.configuration
        );
        assert_eq!(decoded.snapshot.barrier, observed.snapshot.barrier);
        assert!(bytes.len() <= MAX_BODY);
        assert!(decoded.snapshot.load.is_none());
    }
}

#[test]
fn fleet_configuration_commitment_is_order_independent_but_role_exact() {
    let observed = observation(false);
    let original = &observed.snapshot.configuration;
    let mut reordered = original.clone();
    reordered.voters = vec![3, 1, 2];
    assert_eq!(
        crate::raft::host::group_membership_digest(&observed.snapshot.membership, original)
            .unwrap(),
        crate::raft::host::group_membership_digest(&observed.snapshot.membership, &reordered)
            .unwrap()
    );
    assert_eq!(
        crate::raft::host::FleetMembershipConfiguration::capture(original).unwrap(),
        crate::raft::host::FleetMembershipConfiguration::capture(&reordered).unwrap()
    );
    reordered.voters = vec![1, 2];
    reordered.learners = vec![3];
    assert_ne!(
        crate::raft::host::group_membership_digest(&observed.snapshot.membership, original)
            .unwrap(),
        crate::raft::host::group_membership_digest(&observed.snapshot.membership, &reordered)
            .unwrap()
    );
}

#[test]
fn fleet_quorum_body_rejects_mixed_boot_root_barrier_and_configuration() {
    let mut observed = observation(true);
    observed.snapshot.replica.process = ProcessGeneration::new([99; 16]).unwrap();
    assert!(encode(&observed).is_err());
    observed = observation(true);
    observed.snapshot.barrier.index = LogIndex::new(6);
    assert!(encode(&observed).is_err());
    observed = observation(true);
    observed.snapshot.barrier.term = Term::new(1);
    assert!(encode(&observed).is_err());
    observed = observation(true);
    observed.snapshot.configuration.voters = vec![1, 4];
    assert!(encode(&observed).is_err());
    observed = observation(true);
    observed.snapshot.status.leader = Some(4);
    assert!(encode(&observed).is_err());
    observed = observation(true);
    observed.snapshot.status.applied.index = LogIndex::new(4);
    assert!(encode(&observed).is_err());
    observed = observation(true);
    observed.snapshot.engine = EngineInstanceId::new([98; 16]).unwrap();
    assert!(encode(&observed).is_err());
}

#[test]
fn fleet_quorum_body_refuses_legacy_single_image_shapes_missing_fields_and_lengths() {
    let original = encode(&observation(true)).unwrap();
    for len in [0, 11, 12, original.len() - 1] {
        assert!(decode(&original[..len]).is_err());
    }
    let mut extra = original.clone();
    extra.push(0);
    assert!(decode(&extra).is_err());
    assert!(decode(&vec![0; MAX_BODY + 1]).is_err());
    let node_len = u32::from_be_bytes(original[4..8].try_into().unwrap()) as usize;
    let body: serde_json::Value = serde_json::from_slice(&original[12 + node_len..]).unwrap();
    for field in [
        "root",
        "membership",
        "configuration",
        "barrier_index",
        "barrier_term",
        "leader",
        "load",
    ] {
        let mut changed = body.clone();
        changed.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<Body>(changed).is_err());
    }
    let mut old = body.clone();
    old["image"] = serde_json::json!(null);
    assert!(serde_json::from_value::<Body>(old).is_err());
    let mut old_root = body;
    old_root["root"]["root"]
        .as_object_mut()
        .unwrap()
        .remove("last_mutation_index");
    assert!(serde_json::from_value::<Body>(old_root).is_err());
}
