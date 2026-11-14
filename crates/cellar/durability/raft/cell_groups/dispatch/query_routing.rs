// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable replica preferences are observations, never destination authority.
use super::*;
use cellar_logic::consensus::MemberId;
use std::time::Duration;

/// Replica preferences for one logical cell query. Candidates and an optional
/// untried observed leader share the original invocation deadline and attempt
/// bank. Every destination independently authorizes the request. Incoming RPCs
/// always execute locally, regardless of the origin's preferences.
#[derive(Clone, Debug, Default)]
pub enum CellQueryRouting {
    #[default]
    Local,
    /// Use the local replica when present. On an ingress-only node, try
    /// the installed trusted group route within the original attempt budget.
    Automatic,
    /// One to eight distinct members; attempt_timeout is positive and no larger
    /// than the original timeout. Missing leader advice never grants authority.
    Replicas {
        members: Vec<MemberId>,
        attempt_timeout: Duration,
        leader_fallback: bool,
    },
}

impl CellQueryRouting {
    pub(crate) fn validate(&mut self, limits: CellRoutingLimits) -> Result<(), CellDispatchError> {
        if let Self::Replicas {
            members,
            attempt_timeout,
            ..
        } = self
        {
            if members.is_empty()
                || members.len() > 8
                || members
                    .iter()
                    .enumerate()
                    .any(|(n, member)| members[..n].contains(member))
                || attempt_timeout.as_millis() == 0
                || *attempt_timeout > limits.timeout
            {
                return Err(CellDispatchError::Invalid("query replica routing bounds"));
            }
            *members = std::mem::take(members).into_boxed_slice().into_vec();
        }
        Ok(())
    }

    pub(super) fn fingerprint(&self, hash: &mut Sha256) {
        match self {
            Self::Local => hash.update([0]),
            Self::Automatic => hash.update([2]),
            Self::Replicas {
                members,
                attempt_timeout,
                leader_fallback,
            } => {
                hash.update([1, members.len() as u8, u8::from(*leader_fallback)]);
                for member in members {
                    hash.update(member.get().to_be_bytes());
                }
                hash.update(attempt_timeout.as_nanos().to_be_bytes());
            }
        }
    }
}

impl CellQueryInvocation {
    /// Reject invalid minima before any replica attempt can reinterpret a
    /// destination refusal as transient unavailability.
    pub(crate) async fn validate_minimum(&self) -> Result<(), CellDispatchError> {
        let attempt = self
            .attempt
            .as_ref()
            .ok_or(CellDispatchError::Invalid("missing route"))?;
        if let CellQueryConsistency::AtLeast(token) = &self.request.consistency {
            self.dispatcher
                .keys
                .verify_cell(
                    token,
                    &attempt.request().cell,
                    self.dispatcher.class.schema_digest(),
                    self.now().await?,
                )
                .map_err(CellDispatchError::Version)?;
        }
        self.session
            .validate_attempt(attempt)
            .map_err(CellDispatchError::Routing)
    }

    pub(crate) fn deadline_ms(&self) -> u64 {
        self.session.deadline_ms()
    }

    pub(crate) fn maximum_attempts(&self) -> usize {
        self.session.maximum_attempts()
    }

    pub(crate) fn routing(&self) -> CellQueryRouting {
        self.routing.clone()
    }

    pub(crate) fn leader_hint(&self) -> Option<MemberId> {
        self.leader_hint
    }

    /// Shortening one ticket never renews the session or any earlier physical read.
    pub(crate) fn limit_attempt(&mut self, timeout: Duration) -> Result<(), CellDispatchError> {
        let now = self.dispatcher.domain.mono_ms().map_err(|_| closed())?;
        let attempt = self
            .attempt
            .as_mut()
            .ok_or(CellDispatchError::Invalid("missing route"))?;
        let deadline = now
            .checked_add(timeout.as_millis() as u64)
            .ok_or(CellDispatchError::Invalid("query attempt deadline"))?
            .min(attempt.deadline_ms());
        attempt
            .limit_query_deadline(deadline)
            .map_err(CellDispatchError::Routing)
    }
}
