use super::*;
use crate::consensus::{ClusterId, DiskLineage, GroupId, IncarnationId, NamespaceId, NodeId};

fn fixture() -> (LeaderView, Vec<LeaderSample>, LeaderPolicy) {
    let voters = (1..=3)
        .map(|id| PlacedMember {
            member: MemberId::new(id).unwrap(),
            node: NodeId::new([id as u8; 16]).unwrap(),
            disk: DiskLineage::new([id as u8 + 10; 16]).unwrap(),
        })
        .collect::<Vec<_>>();
    let samples = voters
        .iter()
        .enumerate()
        .map(|(i, binding)| LeaderSample {
            binding: binding.clone(),
            sampled_ms: 1000,
            health: NodeHealth::Healthy,
            compatible: true,
            draining: false,
            leaders: if i == 0 { 6 } else { 0 },
            leader_capacity: 10,
            matched: LogIndex::new(50),
            applied: LogIndex::new(50),
        })
        .collect();
    (
        LeaderView {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                cell: "Counter:balance".into(),
                group: GroupId::new([3; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            term: Term::new(2),
            revision: LogIndex::new(1),
            current: voters[0].member,
            committed: LogIndex::new(50),
            voters,
            joint: false,
            repairing: false,
            last_transfer_ms: 0,
        },
        samples,
        LeaderPolicy {
            max_sample_age_ms: 100,
            min_transfer_interval_ms: 500,
            deadband_basis_points: 1000,
        },
    )
}

#[test]
fn deterministic_balancing_binds_history_term_and_revision() {
    let (view, mut samples, policy) = fixture();
    let chosen = select_leader(&view, &samples, policy, 1000)
        .unwrap()
        .unwrap();
    assert_eq!(chosen.scope, view.scope);
    assert_eq!(chosen.expected_term, view.term);
    assert_eq!(chosen.expected_revision, view.revision);
    assert_ne!(chosen.target.member, view.current);
    samples.reverse();
    assert_eq!(
        select_leader(&view, &samples, policy, 1000),
        Ok(Some(chosen))
    );
}

#[test]
fn lagging_or_unapplied_voters_are_ineligible() {
    let (view, mut samples, policy) = fixture();
    samples[1].matched = LogIndex::new(49);
    samples[2].applied = LogIndex::new(49);
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    samples[1].matched = view.committed;
    assert_eq!(
        select_leader(&view, &samples, policy, 1000)
            .unwrap()
            .unwrap()
            .target,
        samples[1].binding
    );
}

#[test]
fn stale_unavailable_incompatible_draining_and_full_samples_are_refused() {
    let (view, samples, policy) = fixture();
    for refusal in 0..5 {
        let mut samples = samples.clone();
        for target in &mut samples[1..] {
            match refusal {
                0 => target.sampled_ms = 0,
                1 => target.health = NodeHealth::Unavailable,
                2 => target.compatible = false,
                3 => target.draining = true,
                _ => target.leaders = target.leader_capacity,
            }
        }
        assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    }
    let mut stale_source = samples;
    stale_source[0].sampled_ms = 0;
    assert_eq!(select_leader(&view, &stale_source, policy, 1000), Ok(None));
}

#[test]
fn cooldown_joint_repair_and_deadband_suppress_moves() {
    let (mut view, samples, policy) = fixture();
    view.last_transfer_ms = 501;
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    view.last_transfer_ms = 500;
    view.joint = true;
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    view.joint = false;
    view.repairing = true;
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    view.repairing = false;
    assert_eq!(
        select_leader(
            &view,
            &samples,
            LeaderPolicy {
                deadband_basis_points: 6001,
                ..policy
            },
            1000
        ),
        Ok(None)
    );
}

#[test]
fn projected_utilization_does_not_oscillate_or_ignore_capacity_weights() {
    let (view, mut samples, policy) = fixture();
    samples[0].leaders = 2;
    samples[1].leader_capacity = 1;
    samples[2].leader_capacity = 10;
    assert_eq!(
        select_leader(&view, &samples, policy, 1000)
            .unwrap()
            .unwrap()
            .target,
        samples[2].binding
    );
    samples[0].leaders -= 1;
    samples[2].leaders += 1;
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    let mut after = view;
    after.current = samples[2].binding.member;
    assert_eq!(select_leader(&after, &samples, policy, 1000), Ok(None));
    // Counts at their type limit still use exact rational arithmetic.
    samples[2].leaders = u32::MAX;
    samples[2].leader_capacity = u32::MAX;
    samples[0].leaders = u32::MAX - 1;
    samples[0].leader_capacity = u32::MAX;
    assert_eq!(select_leader(&after, &samples, policy, 1000), Ok(None));
}

#[test]
fn drain_bypasses_pressure_but_preserves_target_eligibility() {
    let (view, mut samples, policy) = fixture();
    samples[0].leaders = 1;
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
    samples[0].draining = true;
    assert!(select_leader(&view, &samples, policy, 1000)
        .unwrap()
        .is_some());
    samples[1].matched = LogIndex::new(49);
    samples[2].applied = LogIndex::new(49);
    assert_eq!(select_leader(&view, &samples, policy, 1000), Ok(None));
}

#[test]
fn malformed_views_and_foreign_or_duplicate_samples_are_refused() {
    let (mut view, mut samples, mut policy) = fixture();
    samples[1].binding.disk = DiskLineage::new([99; 16]).unwrap();
    assert_eq!(
        select_leader(&view, &samples, policy, 1000),
        Err(LeaderError::InvalidSample)
    );
    samples[1].binding = samples[0].binding.clone();
    assert_eq!(
        select_leader(&view, &samples, policy, 1000),
        Err(LeaderError::InvalidSample)
    );
    samples[1].binding = view.voters[1].clone();
    samples[1].sampled_ms = 1001;
    assert_eq!(
        select_leader(&view, &samples, policy, 1000),
        Err(LeaderError::InvalidSample)
    );
    samples[1].sampled_ms = 1000;
    view.voters[1].node = view.voters[0].node;
    assert_eq!(
        select_leader(&view, &samples, policy, 1000),
        Err(LeaderError::InvalidView)
    );
    policy.min_transfer_interval_ms = 0;
    assert_eq!(
        select_leader(&view, &samples, policy, 1000),
        Err(LeaderError::InvalidPolicy)
    );
}
