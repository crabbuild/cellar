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

/// A cached observation obtained from the configured trusted catalog. It can
/// become stale; a serving destination must check the full route binding.
#[derive(Clone)]
pub struct ResolvedCellRoute {
    cell: CellHistory,
    route: GroupRoute,
    revision: u64,
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
        let error = |reason| CellRoutingError {
            reason,
            ambiguous_command: None,
        };
        let config = self.namespace.config();
        if request.cell.cluster != config.cluster
            || request.cell.namespace != config.namespace
            || request.fingerprint == [0; 32]
            || limits.timeout.as_millis() == 0
            || limits.timeout > Duration::from_secs(60)
            || !(1..=16).contains(&limits.max_attempts)
            || cached.as_ref().is_some_and(|cached| {
                cached.cell != request.cell
                    || !Arc::ptr_eq(&cached.owner, &self.namespace.inner.owner)
            })
        {
            return Err(error(CellRoutingFailure::InvalidRequest));
        }
        let domain = domain
            .services()
            .domain_token()
            .ok_or_else(|| error(CellRoutingFailure::Closed))?;
        let now = domain
            .mono_ms()
            .map_err(|_| error(CellRoutingFailure::Closed))?;
        let deadline = now
            .checked_add(limits.timeout.as_millis() as u64)
            .ok_or_else(|| error(CellRoutingFailure::InvalidRequest))?;
        Ok(CellRoutingSession {
            directory: self.clone(),
            domain,
            request: Arc::new(request),
            deadline,
            maximum: limits.max_attempts,
            attempts: 0,
            active: false,
            submitted: false,
            cached,
            owner: Arc::new(()),
        })
    }
}

impl CellRoutingSession {
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

    pub async fn begin(&mut self) -> Result<CellRouteAttempt, CellRoutingError> {
        if self.attempts != 0 {
            return Err(self.error(CellRoutingFailure::StaleAttempt));
        }
        self.next(None).await
    }

    /// Same-group retry, including a different member, under the same budget.
    pub async fn retry(
        &mut self,
        previous: &CellRouteAttempt,
    ) -> Result<CellRouteAttempt, CellRoutingError> {
        self.check_attempt(previous)?;
        self.next(None).await
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
        self.next(Some(hint)).await
    }

    async fn next(
        &mut self,
        hint: Option<&GroupRoute>,
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
        if self.cached.is_none() || hint.is_some() {
            let minimum = self.cached.as_ref().map_or(0, |route| route.revision);
            let work = async {
                let view = self.directory.observe().await?;
                let route = self
                    .directory
                    .lookup(&view, &self.request.cell.cell)
                    .await?;
                Ok::<_, CatalogError>((view, route))
            };
            let result = self
                .domain
                .run(crate::asyncrt::timeout_at(self.deadline, work))
                .await
                .map_err(|_| self.error(CellRoutingFailure::Closed))?;
            self.live()?;
            let (view, route) = result
                .map_err(|_| self.error(CellRoutingFailure::Deadline))?
                .map_err(|error| self.error(CellRoutingFailure::Catalog(error)))?;
            if view.revision() < minimum || hint.is_some_and(|hint| &route != hint) {
                return Err(self.error(CellRoutingFailure::InvalidHint));
            }
            self.cached = Some(ResolvedCellRoute {
                cell: self.request.cell.clone(),
                route,
                revision: view.revision(),
                owner: self.directory.namespace.inner.owner.clone(),
            });
        }
        self.live()?;
        let resolved = self
            .cached
            .clone()
            .ok_or_else(|| self.error(CellRoutingFailure::InvalidHint))?;
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
