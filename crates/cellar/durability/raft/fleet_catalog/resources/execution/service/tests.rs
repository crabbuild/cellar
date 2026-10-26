// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::member_state::{JoiningMember, RepairPlan};
use cellar_logic::consensus::{
    ClusterId, DiskLineage, GroupId, IncarnationId, LogIndex, MemberId, NamespaceId, NodeId,
    ProposalId,
};

fn pending(id: u8) -> FleetPendingMovement {
    FleetPendingMovement {
        reservation: [id; 16],
        group: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: format!("Counter:{id}"),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([3; 16]).unwrap(),
        },
        plan: RepairPlan {
            id: ProposalId::new([id; 16]).unwrap(),
            policy_revision: 1,
            expected_revision: LogIndex::new(1),
            obsolete: MemberId::new(1).unwrap(),
            replacement: JoiningMember {
                member: MemberId::new(4).unwrap(),
                node: NodeId::new([4; 16]).unwrap(),
                disk: DiskLineage::new([5; 16]).unwrap(),
            },
        },
    }
}

#[test]
fn fleet_service_bounds_refuse_busy_polling_and_unbounded_turns() {
    let mut options = FleetMovementServiceOptions::default();
    assert!(options.validate().is_ok());
    options.interval = Duration::from_millis(49);
    assert!(options.validate().is_err());
    options.interval = Duration::from_millis(100);
    options.retry_max_interval = Duration::from_millis(99);
    assert!(options.validate().is_err());
    options.retry_max_interval = Duration::from_secs(1);
    options.turn_timeout = Duration::from_secs(61);
    assert!(options.validate().is_err());
}

#[test]
fn fleet_service_retains_missing_work_until_proven_settlement() {
    let mut schedule = schedule::Schedule::default();
    schedule.refresh(vec![pending(10), pending(11)]);
    schedule.refresh(vec![pending(12)]);
    assert_eq!(schedule.entries.len(), 2);
    assert_eq!(schedule.entries[0].pending.reservation, [10; 16]);
    schedule.entries.remove(0); // Only a verified terminal result removes it.
    schedule.refresh(vec![pending(11), pending(12)]);
    assert_eq!(schedule.entries.len(), 2);
    assert_eq!(schedule.entries[1].pending.reservation, [12; 16]);
}

#[test]
fn fleet_service_joint_priority_preserves_other_work_fairness() {
    for phase in [
        FleetMovementPhase::Joint,
        FleetMovementPhase::Complete,
        FleetMovementPhase::Fenced,
        FleetMovementPhase::Aborted,
    ] {
        let mut schedule = schedule::Schedule::default();
        schedule.refresh(vec![pending(10), pending(11)]);
        schedule.entries[1].status.phase = Some(phase);
        assert_eq!(
            (0..4)
                .map(|_| schedule.select(0).unwrap())
                .collect::<Vec<_>>(),
            vec![1, 1, 1, 0]
        );
        schedule.entries[1].next_at = 100;
        assert_eq!(schedule.select(99), Some(0));
        schedule.entries[0].next_at = 200;
        assert_eq!(schedule.select(99), None);
        assert_eq!(schedule.select(100), Some(1));
    }
}

#[test]
fn fleet_service_backoff_saturates_and_bounds_jitter() {
    let options = FleetMovementServiceOptions::default();
    for failures in [0, 1, 2, 16, u32::MAX] {
        for jitter in [0, 1, u32::MAX] {
            let delay = schedule::retry_delay(&options, failures, jitter);
            assert!(delay >= options.interval.as_millis() as u64);
            assert!(delay <= options.retry_max_interval.as_millis() as u64);
        }
    }
    assert_eq!(schedule::retry_delay(&options, u32::MAX, 0), 10_000);
    assert_eq!(
        schedule::classify(&FleetCatalogError::Unconfirmed),
        FleetMovementServiceFailure::Unconfirmed
    );
    assert_eq!(
        schedule::classify(&FleetCatalogError::Corrupt("fixture")),
        FleetMovementServiceFailure::Rejected
    );
}
