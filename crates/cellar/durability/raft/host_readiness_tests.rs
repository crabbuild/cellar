// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn image_fixture() -> (crate::raft::cache::CachedGroup, ImageView) {
    let mut conf = ::raft::prelude::ConfState::default();
    conf.set_voters(vec![1, 2, 3]);
    let mut cache = crate::raft::cache::genesis(conf);
    let image = ImageView {
        position: cache.applied.position,
        image: crate::raft::cache::CommittedImage {
            name: "verified.sqlite".into(),
            commitment: [9; 32].into(),
            length: 8192,
            ltx_epoch: 1,
            txid: 1,
            checksum: 0,
            page_size: 4096,
        },
    };
    cache.applied.image = Some(image.image.clone());
    cache.validate(128).unwrap();
    (cache, image)
}

fn apply_protocol(cache: &mut crate::raft::cache::CachedGroup, index: u64, term: u64) {
    cache.entries.insert(
        index,
        ::raft::prelude::Entry {
            index,
            term,
            ..Default::default()
        },
    );
    cache.applied.position = LogPosition {
        index: LogIndex::new(index),
        term: Term::new(term),
    };
    cache.hard_state.commit = index;
    cache.hard_state.term = term;
    cache.validate(128).unwrap();
}

#[test]
fn readiness_preserves_verified_image_across_protocol_only_positions() {
    let (mut cache, image) = image_fixture();
    assert!(coherent_image(&cache, Some(&image), cache.applied.position));
    for (index, term) in [(2, 1), (3, 2), (4, 3)] {
        apply_protocol(&mut cache, index, term);
        assert!(
            coherent_image(&cache, Some(&image), cache.applied.position),
            "protocol-only apply must preserve the exact verified image at {index}/{term}"
        );
    }
}

#[test]
fn readiness_refuses_a_missing_or_different_complete_image_marker() {
    let (mut cache, image) = image_fixture();
    apply_protocol(&mut cache, 2, 2);
    assert!(!coherent_image(&cache, None, cache.applied.position));
    let mut absent = cache.clone();
    absent.applied.image = None;
    assert!(!coherent_image(
        &absent,
        Some(&image),
        absent.applied.position
    ));
    for field in 0..7 {
        let mut changed = cache.clone();
        let marker = changed.applied.image.as_mut().unwrap();
        match field {
            0 => marker.name = "other.sqlite".into(),
            1 => {
                let crate::application::consensus::ImageCommitment::Sha256(digest) =
                    &mut marker.commitment
                else {
                    panic!("materialized fixture")
                };
                digest[0] ^= 1;
            }
            2 => marker.length += 4096,
            3 => marker.ltx_epoch += 1,
            4 => marker.txid += 1,
            5 => marker.checksum = cellar_ltx::CHECKSUM_FLAG | 1,
            _ => marker.page_size = 8192,
        }
        assert!(!coherent_image(
            &changed,
            Some(&image),
            changed.applied.position
        ));
    }
}

#[test]
fn readiness_refuses_regressing_mismatched_or_uncommitted_positions() {
    let (mut cache, image) = image_fixture();
    apply_protocol(&mut cache, 2, 2);
    assert!(!coherent_image(&cache, Some(&image), image.position));
    let mut future = image.clone();
    future.position = LogPosition {
        index: LogIndex::new(3),
        term: Term::new(2),
    };
    assert!(!coherent_image(
        &cache,
        Some(&future),
        cache.applied.position
    ));
    let mut newer_term = image.clone();
    newer_term.position.term = Term::new(3);
    assert!(!coherent_image(
        &cache,
        Some(&newer_term),
        cache.applied.position
    ));
    let mut same_index_wrong_term = image.clone();
    same_index_wrong_term.position.index = cache.applied.position.index;
    assert!(!coherent_image(
        &cache,
        Some(&same_index_wrong_term),
        cache.applied.position
    ));
    cache.hard_state.commit = 1;
    assert!(!coherent_image(
        &cache,
        Some(&image),
        cache.applied.position
    ));
}

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
