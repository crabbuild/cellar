// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conservative local observations, separate from per-invocation proof gates.
use super::*;
use crate::application::consensus::{RaftQueryReadiness, RaftReplicaRole};

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

pub(super) fn capture(
    cell: &Cell,
    status: &GroupStatus,
    now: u64,
    source_timeout_ms: u64,
    draining: bool,
    registered_queries: bool,
) -> Result<(RaftReplicaRole, RaftQueryReadiness), RuntimeError> {
    let cache = cell.group.read()?;
    let role = role(&cache, status);
    let coherent_image = coherent_image(&cache, cell.image.as_ref(), status.applied);
    let historical = registered_queries
        && coherent_image
        && !matches!(role, RaftReplicaRole::Removed | RaftReplicaRole::Unknown)
        && !draining
        && !cell.failed
        && !cell.retired
        && cell.incoming.is_none()
        && !cell.sql_busy
        && cell.apply.is_none()
        && cell.publication.is_none();
    let source_age_ms = status.source_observed_ms.and_then(|at| now.checked_sub(at));
    let recent_source = historical
        && super::super::query::recent_source_deadline(
            status.source_observed_ms,
            now,
            source_timeout_ms,
        )
        .is_some();
    Ok((
        role,
        RaftQueryReadiness {
            registered_queries,
            coherent_image,
            historical,
            recent_source,
            source_age_ms,
            source_timeout_ms,
        },
    ))
}
