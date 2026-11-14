// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-bounded catalog confirmation of untrusted relocation hints.
//! Attempts are routing observations, never admission or quorum capabilities.

use super::*;
use crate::asyncrt::{DomainToken, ExecutionDomain};
use cellar_logic::consensus::CommandId;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellRouteOperation {
    Command(CommandId),
    Receipt(CommandId),
    BindingRetirement(CommandId),
    Enrollment(ProposalId),
    Query(ProposalId),
}

/// The dispatcher must compute the fingerprint over its complete canonical
/// request (contract, input, durability or consistency, and retry metadata).
/// The session retains this exact binding through every attempt; destination
/// admission still validates the actual request and logical cell incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellRouteRequest {
    pub cell: CellHistory,
    pub operation: CellRouteOperation,
    pub fingerprint: [u8; 32],
}

#[derive(Clone, Copy, Debug)]
pub struct CellRoutingLimits {
    pub timeout: Duration,
    /// Includes the initial attempt, same-group retries and relocations.
    pub max_attempts: u8,
}

impl CellRoutingLimits {
    pub(crate) fn deadline_at(self, started_at: u64) -> Option<u64> {
        self.deadline_at_bounded(started_at, 60_000)
    }

    pub(crate) fn deadline_at_bounded(self, started_at: u64, maximum_ms: u64) -> Option<u64> {
        if self.timeout.as_millis() == 0
            || self.timeout > Duration::from_millis(maximum_ms)
            || !(1..=16).contains(&self.max_attempts)
        {
            return None;
        }
        started_at.checked_add(self.timeout.as_millis() as u64)
    }
}

#[derive(Debug)]
pub enum CellRoutingFailure {
    InvalidRequest,
    Closed,
    Deadline,
    AttemptsExhausted,
    StaleAttempt,
    InvalidHint,
    Catalog(CatalogError),
}

#[derive(Debug)]
pub struct CellRoutingError {
    pub reason: CellRoutingFailure,
    /// Once dispatch may have submitted a command, neither a redirect nor a
    /// failed catalog read proves it uncommitted. Retry this same command ID.
    pub ambiguous_command: Option<CommandId>,
}

impl fmt::Display for CellRoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cell-group routing: {:?}", self.reason)
    }
}

impl std::error::Error for CellRoutingError {}

/// A routing observation from the trusted catalog or an entered destination's
/// applied group manifest. Neither grants serving authority; the actual owner
/// must check the full route and current execution admission independently.
#[derive(Clone)]
pub struct ResolvedCellRoute {
    cell: CellHistory,
    route: GroupRoute,
    revision: u64,
    root: [u8; 32],
    owner: Arc<()>,
}

impl ResolvedCellRoute {
    pub fn route(&self) -> &GroupRoute {
        &self.route
    }
}

#[derive(Clone)]
pub struct CellRouteAttempt {
    resolved: ResolvedCellRoute,
    request: Arc<CellRouteRequest>,
    deadline_ms: u64,
    ordinal: u8,
    owner: Arc<()>,
}

/// Private attempt-scoped bookkeeping, consumed only by an authenticated
/// pre-admission refusal. Dropping it preserves submission uncertainty.
pub(crate) struct RemoteSubmission {
    previous: bool,
    ordinal: u8,
    owner: Arc<()>,
}

impl CellRouteAttempt {
    pub fn route(&self) -> &GroupRoute {
        self.resolved.route()
    }

    pub fn cached_route(&self) -> ResolvedCellRoute {
        self.resolved.clone()
    }

    pub fn request(&self) -> &CellRouteRequest {
        &self.request
    }

    /// Absolute monotonic deadline in the session's captured execution domain.
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    pub fn ordinal(&self) -> u8 {
        self.ordinal
    }

    pub(crate) fn limit_query_deadline(&mut self, deadline: u64) -> Result<(), CellRoutingError> {
        if !matches!(self.request.operation, CellRouteOperation::Query(_))
            || deadline > self.deadline_ms
        {
            return Err(CellRoutingError {
                reason: CellRoutingFailure::InvalidRequest,
                ambiguous_command: None,
            });
        }
        self.deadline_ms = deadline;
        Ok(())
    }
}

/// One invocation, with no automatic command submission or producer ownership.
/// Dispatchers retain physical producers independently of these routing waiters.
/// This owner is not cloneable: retries cannot copy or replenish its budget.
pub struct CellRoutingSession {
    directory: RangeDirectory,
    domain: DomainToken,
    request: Arc<CellRouteRequest>,
    deadline: u64,
    maximum: u8,
    attempts: u8,
    active: bool,
    submitted: bool,
    cached: Option<ResolvedCellRoute>,
    minimum_revision: u64,
    route_cache: Option<Arc<RouteCache>>,
    command_performance: Option<Arc<crate::raft::performance::Registry>>,
    owner: Arc<()>,
}

impl RangeDirectory {
    /// Capture the deadline before initial discovery. A matching cached route
    /// permits the first attempt without any object-store access.
    pub fn routing_session(
        &self,
        domain: &ExecutionDomain,
        request: CellRouteRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
    ) -> Result<CellRoutingSession, CellRoutingError> {
        let domain = domain.services().domain_token().ok_or(CellRoutingError {
            reason: CellRoutingFailure::Closed,
            ambiguous_command: None,
        })?;
        let started_at = domain.mono_ms().map_err(|_| CellRoutingError {
            reason: CellRoutingFailure::Closed,
            ambiguous_command: None,
        })?;
        self.routing_session_in(domain, request, limits, cached, started_at)
    }

    pub(crate) fn routing_session_in(
        &self,
        domain: DomainToken,
        request: CellRouteRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        started_at: u64,
    ) -> Result<CellRoutingSession, CellRoutingError> {
        self.routing_session_bounded_in(domain, request, limits, cached, started_at, 60_000)
    }

    pub(crate) fn routing_session_bounded_in(
        &self,
        domain: DomainToken,
        request: CellRouteRequest,
        limits: CellRoutingLimits,
        cached: Option<ResolvedCellRoute>,
        started_at: u64,
        maximum_ms: u64,
    ) -> Result<CellRoutingSession, CellRoutingError> {
        let error = |reason| CellRoutingError {
            reason,
            ambiguous_command: None,
        };
        let config = self.namespace.config();
        if request.cell.cluster != config.cluster
            || request.cell.namespace != config.namespace
            || request.fingerprint == [0; 32]
            || cached.as_ref().is_some_and(|cached| {
                cached.cell != request.cell
                    || !Arc::ptr_eq(&cached.owner, &self.namespace.inner.owner)
            })
        {
            return Err(error(CellRoutingFailure::InvalidRequest));
        }
        let now = domain
            .mono_ms()
            .map_err(|_| error(CellRoutingFailure::Closed))?;
        if started_at > now {
            return Err(error(CellRoutingFailure::InvalidRequest));
        }
        let deadline = limits
            .deadline_at_bounded(started_at, maximum_ms)
            .ok_or_else(|| error(CellRoutingFailure::InvalidRequest))?;
        if now >= deadline {
            return Err(error(CellRoutingFailure::Deadline));
        }
        Ok(CellRoutingSession {
            directory: self.clone(),
            domain,
            request: Arc::new(request),
            deadline,
            maximum: limits.max_attempts,
            attempts: 0,
            active: false,
            submitted: false,
            minimum_revision: cached.as_ref().map_or(0, |route| route.revision),
            cached,
            route_cache: None,
            command_performance: None,
            owner: Arc::new(()),
        })
    }
}

impl CellRoutingSession {
    pub(crate) fn track_command(&mut self, performance: Arc<crate::raft::performance::Registry>) {
        self.command_performance = Some(performance);
    }

    /// An incoming command already selected its destination. Its actual local
    /// manifest supplies the range observation without another catalog round
    /// trip. This never populates the directory cache or authorizes SQL.
    pub(crate) fn begin_destination(
        &mut self,
        route: GroupRoute,
    ) -> Result<CellRouteAttempt, CellRoutingError> {
        if self.attempts != 0 {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        self.active = false;
        self.live()?;
        self.attempts = 1;
        wire::Route::encode(&route, &self.directory)
            .map_err(|error| self.error(CellRoutingFailure::Catalog(error)))?;
        route
            .check_cell(
                &self.request.cell,
                &cell_routing_key(self.request.cell.namespace, &self.request.cell.cell),
            )
            .map_err(|_| self.error(CellRoutingFailure::InvalidHint))?;
        let resolved = ResolvedCellRoute {
            cell: self.request.cell.clone(),
            route,
            revision: self.cached.as_ref().map_or(0, |previous| previous.revision),
            root: [0; 32],
            owner: self.directory.namespace.inner.owner.clone(),
        };
        self.cached = Some(resolved.clone());
        self.active = true;
        Ok(CellRouteAttempt {
            resolved,
            request: self.request.clone(),
            deadline_ms: self.deadline,
            ordinal: 1,
            owner: self.owner.clone(),
        })
    }

    /// Incoming RPC construction may consume time before its local session is
    /// captured. Preserve the transport's original absolute deadline exactly.
    pub(crate) fn constrain_deadline(&mut self, deadline: u64) -> Result<(), CellRoutingError> {
        if self.attempts != 0 {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        self.deadline = self.deadline.min(deadline);
        self.live()
    }

    pub(crate) fn using_cache(mut self, cache: Arc<RouteCache>) -> Self {
        self.route_cache = Some(cache);
        self
    }

    pub(crate) async fn begin_current(&mut self) -> Result<CellRouteAttempt, CellRoutingError> {
        if self.attempts != 0 {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        self.cached = None;
        self.next(None, false).await
    }

    pub(crate) fn maximum_attempts(&self) -> usize {
        usize::from(self.maximum)
    }

    pub(crate) fn deadline_ms(&self) -> u64 {
        self.deadline
    }

    pub(crate) fn validate_attempt(
        &self,
        attempt: &CellRouteAttempt,
    ) -> Result<(), CellRoutingError> {
        self.check_attempt(attempt)?;
        self.live()
    }

    fn error(&self, reason: CellRoutingFailure) -> CellRoutingError {
        CellRoutingError {
            reason,
            ambiguous_command: match self.request.operation {
                CellRouteOperation::Command(id) if self.submitted => Some(id),
                _ => None,
            },
        }
    }

    fn live(&self) -> Result<(), CellRoutingError> {
        let now = self
            .domain
            .mono_ms()
            .map_err(|_| self.error(CellRoutingFailure::Closed))?;
        if now >= self.deadline {
            return Err(self.error(CellRoutingFailure::Deadline));
        }
        Ok(())
    }

    fn check_attempt(&self, attempt: &CellRouteAttempt) -> Result<(), CellRoutingError> {
        if !self.active
            || !Arc::ptr_eq(&self.owner, &attempt.owner)
            || attempt.ordinal != self.attempts
        {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        Ok(())
    }

    /// Mark before handing a mutation to a producer that may submit it. Only
    /// explicit request reconciliation can later establish its actual outcome.
    pub fn mark_submitted(&mut self, attempt: &CellRouteAttempt) -> Result<(), CellRoutingError> {
        self.check_attempt(attempt)?;
        self.live()?;
        self.submitted = true;
        Ok(())
    }

    pub(crate) fn mark_remote_submitted(
        &mut self,
        attempt: &CellRouteAttempt,
    ) -> Result<RemoteSubmission, CellRoutingError> {
        let previous = self.submitted;
        self.mark_submitted(attempt)?;
        Ok(RemoteSubmission {
            previous,
            ordinal: attempt.ordinal,
            owner: attempt.owner.clone(),
        })
    }

    pub(crate) fn reject_remote_submission(
        &mut self,
        proof: RemoteSubmission,
    ) -> Result<(), CellRoutingError> {
        if !self.active || proof.ordinal != self.attempts || !Arc::ptr_eq(&proof.owner, &self.owner)
        {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        self.submitted = proof.previous;
        Ok(())
    }

    pub async fn begin(&mut self) -> Result<CellRouteAttempt, CellRoutingError> {
        if self.attempts != 0 {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        self.next(None, true).await
    }

    /// Same-group retry, including a different member, under the same budget.
    pub async fn retry(
        &mut self,
        previous: &CellRouteAttempt,
    ) -> Result<CellRouteAttempt, CellRoutingError> {
        self.check_attempt(previous)?;
        self.next(None, true).await
    }

    /// Refresh an advisory route after a read-only readiness or transport
    /// failure. Keep the previous catalog floor, identity, deadline and bank.
    pub(crate) async fn refresh(
        &mut self,
        previous: &CellRouteAttempt,
    ) -> Result<CellRouteAttempt, CellRoutingError> {
        self.check_attempt(previous)?;
        self.next(None, false).await
    }

    /// A relocation hint is untrusted until an exact lookup in the configured
    /// catalog confirms its namespace, range, epoch, schema, birth and history.
    /// No arbitrary URL or caller-supplied catalog is followed.
    pub async fn redirect(
        &mut self,
        previous: &CellRouteAttempt,
        hint: &GroupRoute,
    ) -> Result<CellRouteAttempt, CellRoutingError> {
        self.check_attempt(previous)?;
        self.next(Some(hint), true).await
    }

    async fn next(
        &mut self,
        hint: Option<&GroupRoute>,
        use_cache: bool,
    ) -> Result<CellRouteAttempt, CellRoutingError> {
        // Consume the previous ticket and attempt before any await. Dropping a
        // discovery future cannot refund attempts or authorize a late response.
        self.active = false;
        self.live()?;
        if self.attempts >= self.maximum {
            return Err(self.error(CellRoutingFailure::AttemptsExhausted));
        }
        self.attempts += 1;
        if let Some(hint) = hint {
            let previous = self
                .cached
                .as_ref()
                .ok_or_else(|| self.error(CellRoutingFailure::InvalidHint))?;
            if wire::Route::encode(hint, &self.directory).is_err()
                || hint.epoch <= previous.route.epoch
                || !hint.range.contains(&cell_routing_key(
                    hint.history.namespace,
                    &self.request.cell.cell,
                ))
            {
                return Err(self.error(CellRoutingFailure::InvalidHint));
            }
        }
        if let (Some(cache), Some(cached)) = (&self.route_cache, &self.cached) {
            if !cache
                .can_reuse(&cached.route, cached.revision, cached.root)
                .map_err(|e| self.error(CellRoutingFailure::Catalog(e)))?
            {
                // Keep the invocation's catalog floor even when its prior
                // observation has been invalidated by a publication or fence.
                self.cached = None;
            }
        }
        if use_cache && hint.is_none() && self.cached.is_none() {
            if let Some(cache) = &self.route_cache {
                if let Some((route, revision, root)) = cache
                    .get(&self.request.cell, self.minimum_revision)
                    .map_err(|e| self.error(CellRoutingFailure::Catalog(e)))?
                {
                    self.cached = Some(ResolvedCellRoute {
                        cell: self.request.cell.clone(),
                        route,
                        revision,
                        root,
                        owner: self.directory.namespace.inner.owner.clone(),
                    });
                }
            }
        }
        if !use_cache || self.cached.is_none() || hint.is_some() {
            if let Some(performance) = &self.command_performance {
                performance.increment(
                    crate::raft::performance::Counter::CellCommandDirectoryLookups,
                    1,
                );
            }
            let minimum = self.minimum_revision;
            let generation = self
                .route_cache
                .as_ref()
                .map(|cache| cache.generation())
                .transpose()
                .map_err(|e| self.error(CellRoutingFailure::Catalog(e)))?;
            let work = async {
                let view = self.directory.observe().await?;
                // Record a verified publication/fence before the target path
                // can fail or expire. The returned token names that exact
                // observation, never a later generation sampled after I/O.
                let observed_generation =
                    if let (Some(cache), Some(generation)) = (&self.route_cache, &generation) {
                        Some(cache.observe_lookup(&view, generation).await?)
                    } else {
                        None
                    };
                let route = self
                    .directory
                    .lookup(&view, &self.request.cell.cell)
                    .await?;
                if let Some(recovery) = &view.head.recovery {
                    // A published recovery range can vote before activation.
                    // Never cache it as an application route during that window.
                    if recovery.blocks_application_route(&self.directory, &route)? {
                        return Err(CatalogError::Fenced);
                    }
                }
                Ok::<_, CatalogError>((view, route, observed_generation))
            };
            let result = self
                .domain
                .run(crate::asyncrt::timeout_at(self.deadline, work))
                .await
                .map_err(|_| self.error(CellRoutingFailure::Closed))?;
            self.live()?;
            let (view, route, generation) = result
                .map_err(|_| self.error(CellRoutingFailure::Deadline))?
                .map_err(|error| self.error(CellRoutingFailure::Catalog(error)))?;
            if view.revision() < minimum || hint.is_some_and(|hint| &route != hint) {
                return Err(self.error(CellRoutingFailure::InvalidHint));
            }
            if let (Some(cache), Some(generation)) = (&self.route_cache, &generation) {
                self.domain
                    .run(crate::asyncrt::timeout_at(
                        self.deadline,
                        cache.remember(&view, &route, generation),
                    ))
                    .await
                    .map_err(|_| self.error(CellRoutingFailure::Closed))?
                    .map_err(|_| self.error(CellRoutingFailure::Deadline))?
                    .map_err(|e| self.error(CellRoutingFailure::Catalog(e)))?;
            }
            self.minimum_revision = self.minimum_revision.max(view.revision());
            self.cached = Some(ResolvedCellRoute {
                cell: self.request.cell.clone(),
                route,
                revision: view.revision(),
                root: view.digest(),
                owner: self.directory.namespace.inner.owner.clone(),
            });
        }
        self.live()?;
        let resolved = self
            .cached
            .clone()
            .ok_or_else(|| self.error(CellRoutingFailure::InvalidHint))?;
        self.minimum_revision = self.minimum_revision.max(resolved.revision);
        self.active = true;
        Ok(CellRouteAttempt {
            resolved,
            request: self.request.clone(),
            deadline_ms: self.deadline,
            ordinal: self.attempts,
            owner: self.owner.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_budget_does_not_extend_public_routing() {
        let public = CellRoutingLimits {
            timeout: Duration::from_secs(60),
            max_attempts: 4,
        };
        assert_eq!(public.deadline_at(10), Some(60_010));
        let callback = CellRoutingLimits {
            timeout: Duration::from_secs(90),
            ..public
        };
        assert_eq!(callback.deadline_at(10), None);
        assert_eq!(callback.deadline_at_bounded(10, 300_000), Some(90_010));
        assert_eq!(callback.deadline_at_bounded(u64::MAX, 300_000), None);
        assert_eq!(
            CellRoutingLimits {
                timeout: Duration::from_secs(301),
                ..public
            }
            .deadline_at_bounded(10, 300_000),
            None
        );
    }
}
