use super::*;

fn status(cache: &super::super::super::cache::CachedGroup, member: u64) -> GroupStatus {
    GroupStatus {
        membership_revision: cache.membership.revision(),
        member_high_water: cache.membership.high_water(),
        replica: ReplicaIdentity {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                cell: "Counter:readiness".into(),
                group: GroupId::new([3; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            node: NodeId::new([5; 16]).unwrap(),
            member: MemberId::new(member).unwrap(),
            disk: DiskLineage::new([6; 16]).unwrap(),
            process: ProcessGeneration::new([7; 16]).unwrap(),
        },
        term: Term::new(1),
        leader: Some(1),
        is_leader: member == 1,
        protocol_ready: member == 1,
        persisted: LogIndex::new(1),
        committed: LogIndex::new(1),
        applied: cache.applied.position,
        checkpoint: cache.applied.position,
        awaiting_ready: false,
        awaiting_apply: false,
        election_timeout: 10,
        source_observed_ms: Some(10),
    }
}

#[test]
fn readiness_role_uses_exact_configuration_and_term() {
    let mut conf = ::raft::prelude::ConfState::default();
    conf.set_voters(vec![1, 2, 3]);
    conf.set_learners(vec![4]);
    let cache = crate::raft::cache::genesis(conf);
    assert_eq!(role(&cache, &status(&cache, 1)), RaftReplicaRole::Leader);
    assert_eq!(role(&cache, &status(&cache, 2)), RaftReplicaRole::Voter);
    let mut learner = status(&cache, 4);
    learner.is_leader = true;
    assert_eq!(role(&cache, &learner), RaftReplicaRole::Learner);
    assert_eq!(role(&cache, &status(&cache, 5)), RaftReplicaRole::Removed);
    let mut changed = status(&cache, 1);
    changed.term = Term::new(2);
    assert_eq!(role(&cache, &changed), RaftReplicaRole::Unknown);
    let mut changed = status(&cache, 1);
    changed.membership_revision = LogIndex::new(2);
    assert_eq!(role(&cache, &changed), RaftReplicaRole::Unknown);
}

#[test]
fn readiness_joint_demotion_remains_a_voter_until_leave() {
    let mut conf = ::raft::prelude::ConfState::default();
    conf.set_voters(vec![2, 3]);
    conf.set_voters_outgoing(vec![1, 2, 3]);
    conf.set_learners_next(vec![1]);
    let mut cache = crate::raft::cache::genesis(conf);
    let mut observed = status(&cache, 1);
    observed.is_leader = false;
    assert_eq!(role(&cache, &observed), RaftReplicaRole::Voter);
    cache.conf_state.voters_outgoing.clear();
    cache.conf_state.learners_next.clear();
    cache.conf_state.set_learners(vec![1]);
    assert_eq!(role(&cache, &observed), RaftReplicaRole::Learner);
}

#[test]
fn readiness_source_deadline_refuses_unknown_future_expired_and_overflow() {
    use crate::raft::query::recent_source_deadline as deadline;
    assert_eq!(deadline(None, 10, 5), None);
    assert_eq!(deadline(Some(11), 10, 5), None);
    assert_eq!(deadline(Some(10), 10, 5), Some(15));
    assert_eq!(deadline(Some(10), 14, 5), Some(15));
    assert_eq!(deadline(Some(10), 15, 5), None);
    assert_eq!(deadline(Some(10), 10, 0), None);
    assert_eq!(deadline(Some(u64::MAX - 1), u64::MAX - 1, 2), None);
}
