use super::*;

#[test]
fn fleet_genesis_origin_is_explicit_and_cannot_be_retrofitted() {
    let (_, conf, scope, binding) = fixture();
    let genesis = GroupMembership::managed_initial(&conf, binding.clone()).unwrap();
    genesis.validate(&conf, 1).unwrap();
    genesis.validate_scope(&scope).unwrap();
    assert_eq!(genesis.fleet_status().unwrap().changed_at, LogIndex::new(1));
    assert!(genesis.succeeds(&GroupMembership::initial(&conf)).is_err());
    let mut wire = serde_json::to_value(&genesis).unwrap();
    wire["fleet"].as_object_mut().unwrap().remove("origin");
    assert!(serde_json::from_value::<GroupMembership>(wire).is_err());
    let mut committed = genesis.clone();
    committed.fleet.as_mut().unwrap().origin = EnrollmentOrigin::Committed;
    assert!(committed.validate(&conf, 1).is_err());
    committed.revision = 2;
    committed.fleet.as_mut().unwrap().enrolled_at = 2;
    committed.validate(&conf, 2).unwrap();
    assert!(committed.succeeds(&genesis).is_err());
    let mut single = conf.clone();
    single.voters = vec![1];
    assert!(GroupMembership::managed_initial(&single, binding).is_err());
}

#[test]
fn fleet_genesis_fences_first_permit_before_any_business_write() {
    let (_, conf, scope, binding) = fixture();
    let genesis = GroupMembership::managed_initial(&conf, binding.clone()).unwrap();
    let (unused, mut plan) = permit(binding.clone());
    plan.expected_revision = LogIndex::new(1);
    let permit = FleetMembershipPermit::new(
        binding,
        1,
        ProposalId::new([7; 16]).unwrap(),
        ProposalId::new([8; 16]).unwrap(),
        &plan,
    )
    .unwrap();
    assert!(genesis
        .bind_repair_start(&mut ConfChangeV2::default(), &conf, &plan)
        .is_err());
    let mut change = ConfChangeV2::default();
    genesis
        .bind_fleet_fence(&mut change, &conf, &permit)
        .unwrap();
    let fenced = genesis
        .applied(&change, &conf, &conf, LogIndex::new(2))
        .unwrap();
    let restored: GroupMembership =
        serde_json::from_slice(&serde_json::to_vec(&fenced).unwrap()).unwrap();
    restored.validate(&conf, 2).unwrap();
    restored.validate_scope(&scope).unwrap();
    restored.succeeds(&genesis).unwrap();
    assert_eq!(restored.fleet_status().unwrap().consumed_sequence, 1);
    assert!(restored
        .bind_managed_repair_start(&mut ConfChangeV2::default(), &conf, &plan, &permit)
        .is_err());
    assert_ne!(unused.expected_revision(), permit.expected_revision());
}

fn fixture() -> (GroupMembership, ConfState, GroupScope, FleetGroupBinding) {
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:fleet".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let binding = FleetGroupBinding::new(IncarnationId::new([5; 16]).unwrap(), &scope).unwrap();
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let initial = GroupMembership::initial(&conf);
    let mut change = ConfChangeV2::default();
    initial
        .bind_fleet_enrollment(&mut change, &conf, &binding)
        .unwrap();
    initial.admit_scoped(&change, &conf, &scope).unwrap();
    let state = initial
        .applied(&change, &conf, &conf, LogIndex::new(2))
        .unwrap();
    (state, conf, scope, binding)
}

fn permit(binding: FleetGroupBinding) -> (FleetMembershipPermit, RepairPlan) {
    let plan = RepairPlan {
        id: ProposalId::new([6; 16]).unwrap(),
        policy_revision: 1,
        expected_revision: LogIndex::new(2),
        obsolete: MemberId::new(3).unwrap(),
        replacement: JoiningMember {
            member: MemberId::new(4).unwrap(),
            node: NodeId::new([4; 16]).unwrap(),
            disk: DiskLineage::new([14; 16]).unwrap(),
        },
    };
    (
        FleetMembershipPermit::new(
            binding,
            1,
            ProposalId::new([7; 16]).unwrap(),
            ProposalId::new([8; 16]).unwrap(),
            &plan,
        )
        .unwrap(),
        plan,
    )
}

#[test]
fn fleet_scope_and_unguarded_configuration_are_refused() {
    let (state, conf, mut scope, binding) = fixture();
    let (permit, plan) = permit(binding);
    let mut change = ConfChangeV2::default();
    state
        .bind_managed_repair_start(&mut change, &conf, &plan, &permit)
        .unwrap();
    state.admit_scoped(&change, &conf, &scope).unwrap();
    scope.incarnation = IncarnationId::new([99; 16]).unwrap();
    assert!(state.admit_scoped(&change, &conf, &scope).is_err());
    assert!(state.validate_scope(&scope).is_err());
    assert!(state
        .bind_repair_start(&mut ConfChangeV2::default(), &conf, &plan)
        .is_err());
    assert!(state
        .bind_change(&mut ConfChangeV2::default(), &[])
        .is_err());
    assert!(state.admit(&ConfChangeV2::default(), &conf).is_err());
}

#[test]
fn fleet_snapshot_successor_cannot_drop_or_regress_consumption() {
    let (state, conf, scope, binding) = fixture();
    let (permit, plan) = permit(binding);
    let mut change = ConfChangeV2::default();
    state.bind_fleet_fence(&mut change, &conf, &permit).unwrap();
    let fenced = state
        .applied(&change, &conf, &conf, LogIndex::new(3))
        .unwrap();
    let restored: GroupMembership =
        serde_json::from_slice(&serde_json::to_vec(&fenced).unwrap()).unwrap();
    restored.validate(&conf, 3).unwrap();
    restored.validate_scope(&scope).unwrap();
    restored.succeeds(&state).unwrap();
    assert_eq!(
        restored.fleet_status().unwrap().phase,
        Some(FleetPermitPhase::Fenced)
    );
    assert!(restored
        .bind_managed_repair_start(&mut ConfChangeV2::default(), &conf, &plan, &permit)
        .is_err());
    let mut dropped = restored.clone();
    dropped.revision += 1;
    dropped.fleet = None;
    dropped.validate(&conf, 4).unwrap();
    assert!(dropped.succeeds(&restored).is_err());
    let mut regressed = state.clone();
    regressed.revision = 4;
    regressed.validate(&conf, 4).unwrap();
    assert!(regressed.succeeds(&restored).is_err());
    let mut rebound = restored.clone();
    rebound.revision = 4;
    let fleet = rebound.fleet.as_mut().unwrap();
    fleet.binding.fleet = [88; 16];
    fleet.last.as_mut().unwrap().permit.binding.fleet = [88; 16];
    rebound.validate(&conf, 4).unwrap();
    assert!(rebound.succeeds(&restored).is_err());
}

#[test]
fn fleet_wire_requires_explicit_guard_and_exact_permit_payload() {
    let (state, conf, _, binding) = fixture();
    let (permit, mut plan) = permit(binding);
    let mut encoded = serde_json::to_value(&state).unwrap();
    encoded.as_object_mut().unwrap().remove("fleet");
    assert!(serde_json::from_value::<GroupMembership>(encoded).is_err());
    assert!(FleetMembershipPermit::decode(&permit.encode().unwrap()).is_ok());
    plan.policy_revision += 1;
    assert!(state
        .bind_managed_repair_start(&mut ConfChangeV2::default(), &conf, &plan, &permit)
        .is_err());
    let mut corrupt = permit.encode().unwrap();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(FleetMembershipPermit::decode(&corrupt).is_err());
}
