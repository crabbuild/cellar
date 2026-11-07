// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conservative local observations, separate from per-invocation proof gates.
use super::*;
use crate::application::consensus::RaftReplicaRole;

fn role(cache: &super::super::cache::CachedGroup, status: &GroupStatus) -> RaftReplicaRole {
    if cache.membership.revision() != status.membership_revision
        || cache.hard_state.term != status.term.get()
    {
        return RaftReplicaRole::Unknown;
    }
    let member = status.replica.member.get();
    let conf = &cache.conf_state;
    if conf.voters.contains(&member) || conf.voters_outgoing.contains(&member) {
        if status.is_leader {
            RaftReplicaRole::Leader
        } else {
            RaftReplicaRole::Voter
        }
    } else if conf.learners.contains(&member) || conf.learners_next.contains(&member) {
        RaftReplicaRole::Learner
    } else {
        RaftReplicaRole::Removed
    }
}

#[cfg(test)]
#[path = "host_readiness_tests.rs"]
mod tests;

fn coherent_image(
    cache: &super::super::cache::CachedGroup,
    image: Option<&ImageView>,
    applied: cellar_logic::consensus::LogPosition,
) -> bool {
    image.is_some_and(|image| {
        cache.coherent_image_position(&image.image, image.position) == Some(applied)
    })
}
