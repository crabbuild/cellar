// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn configuration() -> ConfState {
    ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    }
}

fn plan(state: &GroupMembership, id: u8, member: u64) -> RepairPlan {
    RepairPlan {
        id: ProposalId::new([id; 16]).unwrap(),
        policy_revision: 1,
        expected_revision: state.revision(),
        obsolete: MemberId::new(3).unwrap(),
        replacement: JoiningMember {
            member: MemberId::new(member).unwrap(),
            node: NodeId::new([id; 16]).unwrap(),
            disk: DiskLineage::new([id + 10; 16]).unwrap(),
        },
    }
}

fn start() -> (GroupMembership, ConfState, RepairPlan) {
    let before = configuration();
    let state = GroupMembership::initial(&before);
    let plan = plan(&state, 4, 4);
    let mut change = ConfChangeV2::default();
    state
        .bind_repair_start(&mut change, &before, &plan)
        .unwrap();
    let mut after = before.clone();
    after.learners.push(4);
    let next = state
        .applied(&change, &before, &after, LogIndex::new(2))
        .unwrap();
    (next, after, plan)
}

fn advance(
    state: &GroupMembership,
    conf: &ConfState,
    plan: &RepairPlan,
    step: RepairStep,
    at: u64,
) -> (GroupMembership, ConfState) {
    let mut change = ConfChangeV2::default();
    state
        .bind_repair_step(&mut change, conf, plan.id, step)
        .unwrap();
    let phase = match step {
        RepairStep::PromoteAndRemove => RepairPhase::Joint,
        RepairStep::FinishJoint => RepairPhase::Complete,
        RepairStep::AbortLearner => RepairPhase::Aborted,
    };
    let after = state.repair.as_ref().unwrap().plan.configuration(phase);
    let next = state
        .applied(&change, conf, &after, LogIndex::new(at))
        .unwrap();
    (next, after)
}

#[test]
fn repair_plan_serializes_and_requires_the_original_phase() {
    let (state, conf, plan) = start();
    let restored: GroupMembership =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    restored.validate(&conf, 2).unwrap();
    assert_eq!(restored.repair_status().unwrap().unwrap().plan, plan);
    assert_eq!(
        restored.repair_status().unwrap().unwrap().phase,
        RepairPhase::Seeding
    );
    let mut generic = ConfChangeV2 {
        changes: vec![item(1, ConfChangeType::RemoveNode)].into(),
        ..Default::default()
    };
    restored.bind_change(&mut generic, &[]).unwrap();
    assert!(restored.admit(&generic, &conf).is_err());
    assert!(restored
        .bind_repair_step(
            &mut generic,
            &conf,
            ProposalId::new([9; 16]).unwrap(),
            RepairStep::PromoteAndRemove
        )
        .is_err());
    assert!(restored
        .bind_repair_step(&mut generic, &conf, plan.id, RepairStep::FinishJoint)
        .is_err());
}

#[test]
fn explicit_joint_plan_retains_outgoing_members_until_finish() {
    let (state, conf, plan) = start();
    let (joint, joint_conf) = advance(&state, &conf, &plan, RepairStep::PromoteAndRemove, 3);
    assert_eq!(joint_conf.voters, vec![1, 2, 4]);
    assert_eq!(joint_conf.voters_outgoing, vec![1, 2, 3]);
    assert!(!joint_conf.auto_leave);
    let restored: GroupMembership =
        serde_json::from_slice(&serde_json::to_vec(&joint).unwrap()).unwrap();
    restored.validate(&joint_conf, 3).unwrap();
    restored.succeeds(&state).unwrap();
    let mut change = ConfChangeV2::default();
    assert!(restored
        .bind_repair_step(&mut change, &joint_conf, plan.id, RepairStep::AbortLearner)
        .is_err());
    let (complete, final_conf) = advance(&restored, &joint_conf, &plan, RepairStep::FinishJoint, 4);
    complete.validate(&final_conf, 4).unwrap();
    complete.succeeds(&state).unwrap();
    complete.succeeds(&joint).unwrap();
    assert_eq!(
        complete.repair_status().unwrap().unwrap().phase,
        RepairPhase::Complete
    );
    assert_eq!(complete.high_water(), 4);
    assert!(final_conf.voters_outgoing.is_empty());
    let mut stale = state.clone();
    stale.revision = 5;
    stale.repair.as_mut().unwrap().changed_at = 5;
    assert!(stale.succeeds(&joint).is_err());
}

#[test]
fn aborted_learner_consumes_identity_before_another_plan() {
    let (state, conf, original) = start();
    let (aborted, conf) = advance(&state, &conf, &original, RepairStep::AbortLearner, 3);
    assert_eq!(conf, configuration());
    assert!(aborted
        .joined(original.replacement.member)
        .unwrap()
        .is_none());
    assert_eq!(aborted.high_water(), 4);
    let mut change = ConfChangeV2::default();
    let mut reused = plan(&aborted, 5, 4);
    assert!(aborted
        .bind_repair_start(&mut change, &conf, &reused)
        .is_err());
    reused.replacement.member = MemberId::new(5).unwrap();
    aborted
        .bind_repair_start(&mut change, &conf, &reused)
        .unwrap();
    let mut after = conf.clone();
    after.learners.push(5);
    let next = aborted
        .applied(&change, &conf, &after, LogIndex::new(4))
        .unwrap();
    next.succeeds(&state).unwrap();
    next.succeeds(&aborted).unwrap();
    assert_eq!(next.high_water(), 5);
}

#[test]
fn repair_intent_refuses_payload_rebinding_and_configuration_substitution() {
    let (state, conf, original) = start();
    let mut change = ConfChangeV2::default();
    state
        .bind_repair_step(
            &mut change,
            &conf,
            original.id,
            RepairStep::PromoteAndRemove,
        )
        .unwrap();
    change
        .changes
        .retain(|c| c.node_id != original.obsolete.get());
    assert!(state.admit(&change, &conf).is_err());
    let mut wrong = conf.clone();
    wrong.learners.clear();
    assert!(state
        .bind_repair_step(
            &mut change,
            &wrong,
            original.id,
            RepairStep::PromoteAndRemove
        )
        .is_err());
    let mut rebound = state.clone();
    rebound.repair.as_mut().unwrap().plan.replacement.disk = [99; 16];
    assert!(rebound.validate(&conf, 2).is_err());
    assert!(rebound.succeeds(&state).is_err());
    let mut erased = state.clone();
    erased.repair = None;
    erased.revision = 3;
    assert!(erased.succeeds(&state).is_err());
}

#[test]
fn completed_plan_retains_receipt_without_blocking_later_manual_configuration() {
    let (state, conf, original) = start();
    let (state, conf) = advance(&state, &conf, &original, RepairStep::PromoteAndRemove, 3);
    let (complete, conf) = advance(&state, &conf, &original, RepairStep::FinishJoint, 4);
    let fresh = JoiningMember {
        member: MemberId::new(5).unwrap(),
        node: NodeId::new([5; 16]).unwrap(),
        disk: DiskLineage::new([15; 16]).unwrap(),
    };
    let mut change = ConfChangeV2 {
        changes: vec![item(5, ConfChangeType::AddLearnerNode)].into(),
        ..Default::default()
    };
    complete.bind_change(&mut change, &[fresh]).unwrap();
    let mut after = conf.clone();
    after.learners.push(5);
    let next = complete
        .applied(&change, &conf, &after, LogIndex::new(5))
        .unwrap();
    next.succeeds(&complete).unwrap();
    assert_eq!(
        next.repair_status().unwrap(),
        complete.repair_status().unwrap()
    );
    let mut replay = plan(&next, 4, 6);
    assert!(next
        .bind_repair_start(&mut change, &after, &replay)
        .is_err());
    replay.id = ProposalId::new([6; 16]).unwrap();
    replay.policy_revision = 0;
    assert!(next
        .bind_repair_start(&mut change, &after, &replay)
        .is_err());
}

#[test]
fn maximum_repair_membership_fits_completion_metadata_bound() {
    let conf = ConfState {
        voters: vec![1, 2, 3],
        learners: (u64::MAX - 33..=u64::MAX - 6).collect(),
        ..Default::default()
    };
    let mut state = GroupMembership::initial(&configuration());
    state.revision = 2;
    state.high_water = u64::MAX - 4;
    state.bindings = conf
        .learners
        .iter()
        .enumerate()
        .map(|(i, id)| Binding {
            member: *id,
            node: [i as u8 + 10; 16],
            disk: [i as u8 + 80; 16],
            joined_at: 2,
        })
        .collect();
    state.validate(&conf, 2).unwrap();
    let request = plan(&state, 99, u64::MAX - 3);
    let mut change = ConfChangeV2::default();
    state
        .bind_repair_start(&mut change, &conf, &request)
        .unwrap();
    let mut after = conf.clone();
    after.learners.push(u64::MAX - 3);
    let next = state
        .applied(&change, &conf, &after, LogIndex::new(3))
        .unwrap();
    let bytes = serde_json::to_vec(&next).unwrap();
    assert!(
        bytes.len() <= 12 * 1024,
        "membership needs metadata reservation review: {}",
        bytes.len()
    );
    let restored: GroupMembership = serde_json::from_slice(&bytes).unwrap();
    restored.validate(&after, 3).unwrap();
    assert_eq!(restored, next);
}

#[test]
fn omitted_nullable_repair_fields_are_not_a_current_record() {
    let state = GroupMembership::initial(&configuration());
    let mut value = serde_json::to_value(&state).unwrap();
    value.as_object_mut().unwrap().remove("repair");
    assert!(serde_json::from_value::<GroupMembership>(value).is_err());
    let mut change = ConfChangeV2 {
        changes: vec![item(4, ConfChangeType::AddLearnerNode)].into(),
        ..Default::default()
    };
    let member = plan(&state, 4, 4).replacement;
    state.bind_change(&mut change, &[member]).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&change.context).unwrap();
    value.as_object_mut().unwrap().remove("repair");
    assert!(serde_json::from_value::<Intent>(value).is_err());
}
