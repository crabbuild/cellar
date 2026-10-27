// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::member_state::JoiningMember;
use ::raft::prelude::{ConfChangeTransition, ConfChangeType};
use cellar_logic::consensus::{DiskLineage, MemberId, NodeId};

fn base() -> Configuration {
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    Configuration::new(
        Position { index: 10, term: 2 },
        &manifest::encode_conf(&conf).unwrap(),
        GroupMembership::initial(&conf),
    )
    .unwrap()
}

fn joining() -> JoiningMember {
    JoiningMember {
        member: MemberId::new(4).unwrap(),
        node: NodeId::new([4; 16]).unwrap(),
        disk: DiskLineage::new([14; 16]).unwrap(),
    }
}

fn change(replay: &Configuration, kind: ConfChangeType) -> ConfChangeV2 {
    let mut change = ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            node_id: 4,
            change_type: kind,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    let joins = if kind == ConfChangeType::AddLearnerNode {
        vec![joining()]
    } else {
        vec![]
    };
    replay.membership.bind_change(&mut change, &joins).unwrap();
    change
}

fn entry(replay: &Configuration, change: &ConfChangeV2) -> Entry {
    Entry {
        entry_type: EntryType::EntryConfChangeV2,
        term: replay.position.term,
        index: replay.position.index + 1,
        data: change.write_to_bytes().unwrap().into(),
        ..Default::default()
    }
}

fn refused_without_advancing(replay: &mut Configuration, entry: &Entry) {
    let position = replay.position;
    let conf = manifest::encode_conf(&replay.conf).unwrap();
    let membership = replay.membership.clone();
    assert!(replay.step(entry).is_err());
    assert!(replay.matches(position, &conf, &membership).unwrap());
}

#[test]
fn learner_promotion_and_joint_leave_retain_exact_physical_identity() {
    // Both explicit and implicit joint consensus must replay without a live
    // node, and outgoing bindings survive until the joint leave commits.
    for transition in [
        ConfChangeTransition::Explicit,
        ConfChangeTransition::Implicit,
    ] {
        let mut replay = base();
        let learner = change(&replay, ConfChangeType::AddLearnerNode);
        replay.step(&entry(&replay, &learner)).unwrap();
        assert_eq!(replay.conf.learners, vec![4]);
        let bound = (joining(), LogIndex::new(11));
        assert_eq!(
            replay.membership.joined(bound.0.member).unwrap(),
            Some(bound.clone())
        );

        // An ordinary entry advances coverage without changing configuration
        // revision. Its SQL bytes are verified by the recovery set separately.
        replay
            .step(&Entry {
                index: 12,
                term: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(replay.membership.revision(), LogIndex::new(11));
        let promote = change(&replay, ConfChangeType::AddNode);
        replay.step(&entry(&replay, &promote)).unwrap();
        assert!(replay.conf.voters.contains(&4));
        assert!(replay.conf.learners.is_empty());

        let mut remove = change(&replay, ConfChangeType::RemoveNode);
        remove.set_transition(transition);
        replay.step(&entry(&replay, &remove)).unwrap();
        assert!(!replay.conf.voters.contains(&4));
        assert!(replay.conf.voters_outgoing.contains(&4));
        assert_eq!(
            replay.conf.auto_leave,
            transition == ConfChangeTransition::Implicit
        );
        assert_eq!(
            replay.membership.joined(bound.0.member).unwrap(),
            Some(bound)
        );

        replay
            .step(&entry(&replay, &ConfChangeV2::default()))
            .unwrap();
        assert!(replay.conf.voters_outgoing.is_empty());
        assert!(replay
            .membership
            .joined(joining().member)
            .unwrap()
            .is_none());
        assert_eq!(replay.membership.high_water(), 4);
        assert_eq!(replay.membership.revision(), LogIndex::new(15));
    }
}

#[test]
fn stale_unbound_invalid_joint_and_wrong_coverage_are_refused_atomically() {
    let mut replay = base();
    let learner = change(&replay, ConfChangeType::AddLearnerNode);
    let valid = entry(&replay, &learner);
    let mut gap = valid.clone();
    gap.index += 1;
    refused_without_advancing(&mut replay, &gap);
    let mut old_term = valid.clone();
    old_term.term -= 1;
    refused_without_advancing(&mut replay, &old_term);

    let voter = change(&replay, ConfChangeType::AddNode);
    refused_without_advancing(&mut replay, &entry(&base(), &voter));
    let mut unbound = learner.clone();
    unbound.context.clear();
    refused_without_advancing(&mut replay, &entry(&base(), &unbound));
    refused_without_advancing(&mut replay, &entry(&base(), &ConfChangeV2::default()));

    replay.step(&valid).unwrap();
    let stale = entry(&replay, &learner);
    refused_without_advancing(&mut replay, &stale);
    // Failed replay does not poison the next correctly bound transition.
    let promote = change(&replay, ConfChangeType::AddNode);
    replay.step(&entry(&replay, &promote)).unwrap();
}

#[test]
fn unknown_protobuf_fields_and_oversized_changes_are_refused() {
    let mut replay = base();
    let learner = change(&replay, ConfChangeType::AddLearnerNode);
    let valid = entry(&replay, &learner);
    let mut unknown_entry = valid.clone();
    unknown_entry.mut_unknown_fields().add_varint(99, 1);
    refused_without_advancing(&mut replay, &unknown_entry);
    let mut unknown_change = learner.clone();
    unknown_change.mut_unknown_fields().add_varint(99, 1);
    refused_without_advancing(&mut replay, &entry(&base(), &unknown_change));
    let mut unknown_item = learner.clone();
    unknown_item.changes[0]
        .mut_unknown_fields()
        .add_varint(99, 1);
    refused_without_advancing(&mut replay, &entry(&base(), &unknown_item));
    let mut oversized = valid;
    oversized.data = vec![0; MAX_CHANGE_BYTES + 1].into();
    refused_without_advancing(&mut replay, &oversized);
}

#[test]
fn v1_normalizes_like_live_apply_and_tip_requires_exact_replayed_bindings() {
    let mut replay = base();
    let learner = change(&replay, ConfChangeType::AddLearnerNode);
    let v1_entry = Entry {
        entry_type: EntryType::EntryConfChange,
        index: 11,
        term: 2,
        data: ConfChange {
            node_id: 4,
            change_type: ConfChangeType::AddLearnerNode,
            context: learner.context.clone(),
            ..Default::default()
        }
        .write_to_bytes()
        .unwrap()
        .into(),
        ..Default::default()
    };
    replay.step(&v1_entry).unwrap();
    let conf = manifest::encode_conf(&replay.conf).unwrap();
    assert!(replay
        .matches(replay.position, &conf, &replay.membership)
        .unwrap());

    // A separately self-consistent final member map cannot override the exact
    // node/disk binding that the archived configuration entry committed.
    let mut rebound = serde_json::to_value(&replay.membership).unwrap();
    rebound["bindings"][0]["disk"] = serde_json::json!(vec![99; 16]);
    let rebound: GroupMembership = serde_json::from_value(rebound).unwrap();
    rebound
        .validate(&replay.conf, replay.position.index)
        .unwrap();
    assert!(!replay.matches(replay.position, &conf, &rebound).unwrap());
    assert!(!replay
        .matches(Position { index: 12, term: 2 }, &conf, &replay.membership)
        .unwrap());
    assert!(!replay
        .matches(
            replay.position,
            &manifest::encode_conf(&base().conf).unwrap(),
            &replay.membership
        )
        .unwrap());
}
