// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Descriptive membership fixtures exercise profile checks. These values grant
//! no catalog, network, physical-install, startup or serving authority.
use super::*;
use crate::raft::member_state::{FleetGroupBinding, JoiningMember, RepairPlan, RepairStep};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{
        ClusterId, DiskLineage, GroupId, IncarnationId, LogIndex, MemberId, NamespaceId, NodeId,
        ProposalId,
    },
};
use raft::prelude::{ConfChangeV2, ConfState};

fn fixture(genesis: bool) -> (GroupMembership, ConfState, FleetMembershipPermit) {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let binding =
        FleetGroupBinding::new(IncarnationId::new([5; 16]).unwrap(), &history, [6; 32]).unwrap();
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let mut state = if genesis {
        GroupMembership::managed_initial(&conf, binding.clone()).unwrap()
    } else {
        GroupMembership::initial(&conf)
    };
    if !genesis {
        let mut change = ConfChangeV2::default();
        state
            .bind_fleet_enrollment(&mut change, &conf, &binding)
            .unwrap();
        state = state
            .applied(&change, &conf, &conf, LogIndex::new(2))
            .unwrap();
    }
    let plan = RepairPlan {
        id: ProposalId::new([7; 16]).unwrap(),
        policy_revision: 1,
        expected_revision: state.revision(),
        obsolete: MemberId::new(3).unwrap(),
        replacement: JoiningMember {
            member: MemberId::new(4).unwrap(),
            node: NodeId::new([8; 16]).unwrap(),
            disk: DiskLineage::new([9; 16]).unwrap(),
        },
    };
    let permit = FleetMembershipPermit::new(
        binding,
        1,
        ProposalId::new([10; 16]).unwrap(),
        ProposalId::new([11; 16]).unwrap(),
        &plan,
    )
    .unwrap();
    let mut change = ConfChangeV2::default();
    state
        .bind_managed_repair_start(&mut change, &conf, &plan, &permit)
        .unwrap();
    let mut next = conf.clone();
    next.learners.push(4);
    let state = state
        .applied(
            &change,
            &conf,
            &next,
            LogIndex::new(state.revision().get() + 1),
        )
        .unwrap();
    state.validate(&next, state.revision().get()).unwrap();
    (state, next, permit)
}

#[test]
fn managed_seed_profile_requires_exact_consumed_permit_and_profile() {
    for genesis in [false, true] {
        let (state, conf, permit) = fixture(genesis);
        check_membership(&state, Some(&permit)).unwrap();
        assert!(check_membership(&state, None).is_err());
        assert!(check_membership(&GroupMembership::initial(&conf), Some(&permit)).is_err());
        let plan = state.repair_status().unwrap().unwrap().plan;
        let other = FleetMembershipPermit::new(
            permit.binding().clone(),
            2,
            ProposalId::new([12; 16]).unwrap(),
            ProposalId::new([13; 16]).unwrap(),
            &plan,
        )
        .unwrap();
        assert!(check_membership(&state, Some(&other)).is_err());
    }
}

#[test]
fn managed_seed_profile_refuses_aborted_repair() {
    let (state, conf, permit) = fixture(true);
    let mut change = ConfChangeV2::default();
    state
        .bind_managed_repair_step(
            &mut change,
            &conf,
            permit.repair_id().unwrap(),
            RepairStep::AbortLearner,
            &permit,
        )
        .unwrap();
    let mut next = conf.clone();
    next.learners.clear();
    let aborted = state
        .applied(
            &change,
            &conf,
            &next,
            LogIndex::new(state.revision().get() + 1),
        )
        .unwrap();
    aborted.validate(&next, aborted.revision().get()).unwrap();
    assert!(check_membership(&aborted, Some(&permit)).is_err());
}

#[test]
fn managed_seed_base_preserves_genesis_and_committed_enrollment_origins() {
    for genesis in [false, true] {
        let (source, _, _) = fixture(genesis);
        let birth = ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let staged = source.seed_initial(&birth).unwrap();
        staged.validate(&birth, 1).unwrap();
        assert_eq!(staged.revision(), LogIndex::new(1));
        assert!(staged.joined(MemberId::new(4).unwrap()).unwrap().is_none());
        assert_eq!(staged.fleet_status().is_some(), genesis);
        source.succeeds(&staged).unwrap();
        assert!(staged.succeeds(&source).is_err());
        let wrong = ConfState {
            voters: vec![1, 2, 5],
            ..Default::default()
        };
        assert!(source.seed_initial(&wrong).is_err());
    }
}
