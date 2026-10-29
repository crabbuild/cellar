// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::node_inspection_wire::tests::view;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
        cell: "Counter:physical".into(),
    }
}

fn occupied(count: usize) -> (RaftHostObservation, RaftPhysicalInventory) {
    let mut node = view();
    node.node.groups = count as u32;
    node.node.reserved_bytes = count as u64 * node.node.profile.bytes_per_group;
    node.unattached_engine_histories = count as u32;
    let histories = (1..=count)
        .map(|i| {
            let mut digest = [0; 32];
            digest[24..].copy_from_slice(&(i as u64).to_be_bytes());
            RaftPhysicalHistory {
                scope: digest,
                birth: [8; 32],
                member: (i as u64).to_be_bytes(),
                completed_sequence: (i as u64).to_be_bytes(),
                in_configuration: true,
            }
        })
        .collect();
    (
        node,
        RaftPhysicalInventory {
            histories,
            initializing: None,
        },
    )
}

#[test]
fn physical_inventory_maximum_is_bounded_and_roundtrips() {
    for count in [0, 1, 1500, MAX_GROUPS] {
        let (node, inventory) = occupied(count);
        let bytes = encode(&node, &inventory).unwrap();
        assert!(bytes.len() <= MAX_BODY);
        let (decoded_node, decoded) = decode(&bytes).unwrap();
        assert_eq!(*decoded_node, node);
        assert_eq!(*decoded, inventory);
        assert_eq!(decoded.digest(), inventory.digest());
    }

    // Two bounded wire copies plus decoded identities and node metadata fit
    // the existing per-node/per-peer and new host-response reservations.
    // Valid decoded node data has fixed fields and three profile strings of
    // at most 64 bytes. Invalid node bodies refuse before allocating histories.
    // Account for that bound while retaining the original 1 MiB reservation.
    const NODE_DECODED_BOUND: usize = 16 * 1024;
    assert!(std::mem::size_of::<RaftHostObservation>() + 3 * 64 < NODE_DECODED_BOUND);
    assert!(
        2 * MAX_BODY + std::mem::size_of::<RaftPhysicalHistory>() * MAX_GROUPS + NODE_DECODED_BOUND
            < RESPONSE_BYTES
    );
}

#[test]
fn physical_inventory_refuses_truncation_duplicates_and_invalid_counts() {
    let (node, inventory) = occupied(2);
    let original = encode(&node, &inventory).unwrap();
    for len in [0, PREFIX - 1, PREFIX, original.len() - 1] {
        assert!(decode(&original[..len]).is_err());
    }
    let mut extra = original.clone();
    extra.push(0);
    assert!(decode(&extra).is_err());
    let mut oversized = original.clone();
    oversized[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(decode(&oversized).is_err());
    let mut unknown = original.clone();
    unknown[..4].copy_from_slice(&2u32.to_be_bytes());
    assert!(decode(&unknown).is_err());
    let mut duplicate = inventory.clone();
    duplicate.histories[1] = duplicate.histories[0];
    assert!(encode(&node, &duplicate).is_err());
    let mut reverse = inventory.clone();
    reverse.histories.reverse();
    assert!(encode(&node, &reverse).is_err());
    let mut zero = original.clone();
    let n = zero.len();
    zero[n - 17..n - 9].fill(0);
    assert!(decode(&zero).is_err());
    assert!(decode(&vec![0; MAX_BODY + 1]).is_err());
    let mut missing = node;
    missing.node.groups += 1;
    missing.node.reserved_bytes += missing.node.profile.bytes_per_group;
    assert!(encode(&missing, &inventory).is_err());
}

#[test]
fn physical_inventory_pending_slot_is_counted_once_and_never_hidden() {
    let (mut node, mut inventory) = occupied(1);
    inventory.initializing = Some(inventory.histories[0].scope);
    assert!(encode(&node, &inventory).is_ok());
    inventory.initializing = Some([9; 32]);
    assert!(encode(&node, &inventory).is_err());
    node.node.groups += 1;
    node.node.reserved_bytes += node.node.profile.bytes_per_group;
    let bytes = encode(&node, &inventory).unwrap();
    assert_eq!(decode(&bytes).unwrap().1.initializing(), Some([9; 32]));
    let mut bad_flag = bytes.clone();
    bad_flag[12] = 2;
    assert!(decode(&bad_flag).is_err());
    let mut absent = bytes;
    absent[12] = 0;
    assert!(decode(&absent).is_err());
}

#[test]
fn physical_inventory_scope_matching_binds_the_full_history() {
    let first = scope();
    let digest = scope_digest(&first).unwrap();
    let inventory = RaftPhysicalInventory {
        histories: vec![RaftPhysicalHistory {
            scope: digest,
            birth: [8; 32],
            member: 3u64.to_be_bytes(),
            completed_sequence: 0u64.to_be_bytes(),
            in_configuration: false,
        }],
        initializing: None,
    };
    assert_eq!(
        inventory.history(&first).unwrap().unwrap().birth_digest(),
        [8; 32]
    );
    let mut changed = first.clone();
    changed.cell.push('x');
    assert!(inventory.history(&changed).unwrap().is_none());
    let mut changed = first.clone();
    changed.incarnation = IncarnationId::new([5; 16]).unwrap();
    assert!(inventory.history(&changed).unwrap().is_none());
    let mut changed = first;
    changed.cell.clear();
    assert!(inventory.history(&changed).is_err());
}

#[test]
fn physical_inventory_configuration_flag_is_strict_and_digest_bound() {
    let (node, mut inventory) = occupied(1);
    let active = inventory.digest();
    inventory.histories[0].in_configuration = false;
    assert_ne!(active, inventory.digest());
    let mut bytes = encode(&node, &inventory).unwrap();
    assert!(!decode(&bytes).unwrap().1.histories()[0].in_configuration());
    let last = bytes.len() - 1;
    bytes[last] = 2;
    assert!(decode(&bytes).is_err());
    // The internal format marker remains 1; old shorter rows cannot silently
    // default the new role observation to false during a mixed deployment.
    bytes.pop();
    assert!(decode(&bytes).is_err());
}

#[test]
fn physical_inventory_role_includes_every_joint_configuration_set() {
    let member = MemberId::new(4).unwrap();
    for field in 0..4 {
        let mut configuration = ::raft::prelude::ConfState::default();
        match field {
            0 => configuration.voters.push(4),
            1 => configuration.voters_outgoing.push(4),
            2 => configuration.learners.push(4),
            _ => configuration.learners_next.push(4),
        }
        assert!(contains_member(&configuration, member));
        assert!(!contains_member(&configuration, MemberId::new(5).unwrap()));
    }
    assert!(!contains_member(
        &::raft::prelude::ConfState::default(),
        member
    ));
}
