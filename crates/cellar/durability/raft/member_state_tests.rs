// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use ::raft::prelude::{ConfChangeSingle, ConfChangeTransition};

fn conf() -> ConfState {
    ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    }
}

fn member(id: u64) -> JoiningMember {
    JoiningMember {
        member: MemberId::new(id).unwrap(),
        node: NodeId::new([id as u8; 16]).unwrap(),
        disk: DiskLineage::new([id as u8 + 10; 16]).unwrap(),
    }
}

fn change(id: u64, kind: ConfChangeType) -> ConfChangeV2 {
    ConfChangeV2 {
        changes: vec![ConfChangeSingle {
            node_id: id,
            change_type: kind,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    }
}

fn joined() -> (GroupMembership, ConfState) {
    let before = conf();
    let mut after = before.clone();
    after.learners.push(4);
    let state = GroupMembership::initial(&before);
    let mut command = change(4, ConfChangeType::AddLearnerNode);
    state.bind_change(&mut command, &[member(4)]).unwrap();
    (
        state
            .applied(&command, &before, &after, LogIndex::new(2))
            .unwrap(),
        after,
    )
}

#[test]
fn fresh_bindings_require_learner_first() {
    let conf = conf();
    let state = GroupMembership::initial(&conf);
    let mut command = change(4, ConfChangeType::AddNode);
    state.bind_change(&mut command, &[member(4)]).unwrap();
    assert!(state.admit(&command, &conf).is_err());
    let mut command = change(4, ConfChangeType::AddLearnerNode);
    assert!(
        state.admit(&command, &conf).is_err(),
        "an unbound new ID is refused"
    );
    state.bind_change(&mut command, &[member(4)]).unwrap();
    state.admit(&command, &conf).unwrap();
    command.changes.push(command.changes[0].clone());
    assert!(state.admit(&command, &conf).is_err());
}

#[test]
fn retired_ids_and_watermark_survive_serialization() {
    let (state, before) = joined();
    let after = conf();
    let mut remove = change(4, ConfChangeType::RemoveNode);
    state.bind_change(&mut remove, &[]).unwrap();
    let retired = state
        .applied(&remove, &before, &after, LogIndex::new(3))
        .unwrap();
    let restored: GroupMembership =
        serde_json::from_slice(&serde_json::to_vec(&retired).unwrap()).unwrap();
    restored.validate(&after, 3).unwrap();
    assert_eq!(restored.high_water(), 4);
    assert!(restored.joined(member(4).member).unwrap().is_none());
    let mut retry = change(4, ConfChangeType::AddLearnerNode);
    restored.bind_change(&mut retry, &[member(4)]).unwrap();
    assert!(restored.admit(&retry, &after).is_err());
    let mut fresh = change(5, ConfChangeType::AddLearnerNode);
    restored.bind_change(&mut fresh, &[member(5)]).unwrap();
    restored.admit(&fresh, &after).unwrap();
}

#[test]
fn snapshot_successor_refuses_rebinding_resurrection_and_lost_watermark() {
    let (state, joined_conf) = joined();
    let mut rebound = state.clone();
    rebound.revision = 3;
    rebound.bindings[0].disk = [99; 16];
    rebound.validate(&joined_conf, 3).unwrap();
    assert!(rebound.succeeds(&state).is_err());
    let mut retired = state.clone();
    retired.revision = 3;
    retired.bindings.clear();
    let mut resurrected = state.clone();
    resurrected.revision = 4;
    assert!(resurrected.succeeds(&retired).is_err());
    let mut lost = retired.clone();
    lost.revision = 4;
    lost.high_water = 3;
    lost.validate(&conf(), 4).unwrap();
    assert!(lost.succeeds(&retired).is_err());
}

#[test]
fn joint_leave_keeps_bindings_until_outgoing_member_retires() {
    let (state, before) = joined();
    let mut voters = conf();
    voters.voters.push(4);
    let mut promote = change(4, ConfChangeType::AddNode);
    state.bind_change(&mut promote, &[]).unwrap();
    let state = state
        .applied(&promote, &before, &voters, LogIndex::new(3))
        .unwrap();
    let mut joint = conf();
    joint.voters_outgoing = vec![1, 2, 3, 4];
    joint.auto_leave = true;
    let mut remove = change(4, ConfChangeType::RemoveNode);
    remove.set_transition(ConfChangeTransition::Implicit);
    state.bind_change(&mut remove, &[]).unwrap();
    let state = state
        .applied(&remove, &voters, &joint, LogIndex::new(4))
        .unwrap();
    assert!(state.joined(member(4).member).unwrap().is_some());
    let state = state
        .applied(&ConfChangeV2::default(), &joint, &conf(), LogIndex::new(5))
        .unwrap();
    assert!(state.joined(member(4).member).unwrap().is_none());
    assert_eq!(state.high_water(), 4);
}

#[test]
fn stale_noncanonical_unknown_and_oversized_intents_refused() {
    let state = GroupMembership::initial(&conf());
    let mut command = change(4, ConfChangeType::AddLearnerNode);
    state.bind_change(&mut command, &[member(4)]).unwrap();
    let valid = command.context.clone();
    command.context = [b" ".as_slice(), valid.as_ref()].concat().into();
    assert!(state.admit(&command, &conf()).is_err());
    command.context = vec![b' '; MAX_BYTES + 1].into();
    assert!(state.admit(&command, &conf()).is_err());
    let mut text = String::from_utf8(valid.to_vec()).unwrap();
    text.pop();
    text.push_str(",\"unknown\":1}");
    command.context = text.into_bytes().into();
    assert!(state.admit(&command, &conf()).is_err());
    command.context = valid;
    let (applied, conf) = joined();
    assert!(applied.admit(&command, &conf).is_err());
}
