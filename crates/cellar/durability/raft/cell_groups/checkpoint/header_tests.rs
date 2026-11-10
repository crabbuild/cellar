// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::member_state::FleetGroupBinding;
use cellar_logic::{
    cell_group::{CellRange, GroupHistory, GroupRoute, RouteEpoch},
    consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
};

#[test]
fn checkpoint_refuses_a_foreign_managed_group_guard() {
    let route = GroupRoute {
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
    };
    let root = GroupManifest::genesis(route.clone()).unwrap();
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let binding = |history: &GroupHistory| {
        FleetGroupBinding::new(IncarnationId::new([7; 16]).unwrap(), history, [8; 32]).unwrap()
    };
    let mut foreign = route.history;
    foreign.group = GroupId::new([9; 16]).unwrap();
    let foreign_membership = GroupMembership::managed_initial(&conf, binding(&foreign)).unwrap();
    assert!(Header::capture(root.clone(), &conf, foreign_membership.clone()).is_err());
    let membership = GroupMembership::managed_initial(&conf, binding(&route.history)).unwrap();
    let header = Header::capture(root, &conf, membership).unwrap();
    Header::decode(&header.encode().unwrap()).unwrap();
    let malformed = Header {
        membership: foreign_membership,
        ..header
    };
    assert!(Header::decode(&malformed.encode().unwrap()).is_err());
}
