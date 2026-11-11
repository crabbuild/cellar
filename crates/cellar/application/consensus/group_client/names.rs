// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Name discovery precedes exact-history handles and never enrolls a cell.

use super::*;
use crate::raft::{budget::Permit, completion::RuntimeError, transport::CellRemoteNameResponse};
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, IncarnationId, NamespaceId, ProposalId},
};
use futures_util::future::BoxFuture;
use sha2::{Digest, Sha256};

type Outcome = Result<Option<CellHistory>, CellDispatchError>;

struct Resolution {
    owner: RaftCells,
    directory: RangeDirectory,
    candidate: CellHistory,
    options: QueryOptions,
    started: u64,
    deadline: u64,
    _memory: Permit,
}

impl RaftCells {
    /// Discover an existing logical name's current incarnation without creating
    /// or invoking it. Absence at this cut is never non-commit reconciliation.
    /// Only linearizable consistency is supported. Discovery and replica
    /// attempts share the original deadline. Inside an ordinary Raft RPC,
    /// dispatch first waits for its source checkpoint; atomic callbacks refuse.
    pub fn resolve_cell_history(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        cell: CellId,
        mut options: QueryOptions,
    ) -> impl Future<Output = Result<Option<CellHistory>, Arc<CellDispatchError>>> + Send + 'static
    {
        let prepared = (|| {
            if !matches!(options.consistency, QueryConsistency::Linearizable) {
                return Err(CellDispatchError::Execution(
                    crate::raft::execution::ExecutionError::Refused(
                        cellar_logic::consensus::CommandFailure::UnsupportedConsistency,
                    ),
                ));
            }
            let limits = CellRoutingLimits {
                timeout: options.timeout,
                max_attempts: options.max_attempts,
            };
            let now = self
                .handle
                .domain
                .mono_ms()
                .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
            if limits.deadline_at(now).is_none() {
                return Err(CellDispatchError::Runtime(RuntimeError::Invalid(
                    "cell name lookup limits".into(),
                )));
            }
            options.routing.validate(limits)?;
            let source = crate::storage::capture_raft_dispatch()
                .and_then(|source| {
                    source
                        .map(|s| s.limit(self.handle.domain.clone(), options.timeout))
                        .transpose()
                })
                .map_err(scoped_dispatch::failure)?;
            let directory = self.handle.core.host.name_directory(cluster, namespace)?;
            let bytes = cell
                .as_str()
                .len()
                .checked_mul(2)
                .and_then(|n| n.checked_add(8192))
                .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
            let admitted = if source.is_some() {
                self.capture_metadata_admission(options.timeout, bytes)
            } else {
                self.metadata_admission(options.timeout, bytes)
            };
            let (deadline, memory) = admitted.map_err(|error| {
                CellDispatchError::Execution(crate::raft::execution::ExecutionError::Refused(error))
            })?;
            let resolution = Resolution {
                owner: self.internal_observer(),
                directory,
                candidate: CellHistory {
                    cluster,
                    namespace,
                    cell,
                    // Authenticated name placeholder, never an enrollment identity.
                    incarnation: IncarnationId::new([1; 16])
                        .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
                },
                started: deadline
                    .checked_sub(options.timeout.as_millis() as u64)
                    .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?,
                deadline,
                options,
                _memory: memory,
            };
            let domain = self.handle.domain.clone();
            let future: BoxFuture<'static, Result<_, Arc<CellDispatchError>>> = Box::pin(
                async move {
                    let timer = domain
                        .sleep_until(deadline)
                        .map_err(|_| Arc::new(CellDispatchError::Runtime(RuntimeError::Closed)))?;
                    domain
                        .run(async move {
                            crate::asyncrt::select! {
                                result = resolution.run() => result,
                                _ = timer => Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
                            }
                        })
                        .await
                        .map_err(|_| Arc::new(CellDispatchError::Runtime(RuntimeError::Closed)))?
                        .map_err(Arc::new)
                },
            );
            Ok::<_, CellDispatchError>((source, future))
        })();
        let future: BoxFuture<'static, Result<_, Arc<CellDispatchError>>> = match prepared {
            Err(error) => Box::pin(async move { Err(Arc::new(error)) }),
            Ok((None, future)) => future,
            Ok((Some(source), future)) => {
                let call = source.schedule(move |_| future);
                Box::pin(async move {
                    call.future
                        .await
                        .map_err(|e| Arc::new(scoped_dispatch::failure(e)))?
                })
            }
        };
        future
    }
}

impl Resolution {
    async fn run(self) -> Outcome {
        use rand::RngCore as _;
        let mut nonce = [0; 16];
        crate::asyncrt::rng("cell-name-discovery").fill_bytes(&mut nonce);
        let invocation =
            ProposalId::new(nonce).map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        let mut hash = Sha256::new();
        hash.update(b"cellar-name-discovery-1");
        hash.update(invocation.bytes());
        hash.update(self.candidate.cell.as_str().as_bytes());
        let mut session = self
            .directory
            .routing_session_in(
                self.owner.handle.domain.clone(),
                CellRouteRequest {
                    cell: self.candidate.clone(),
                    operation: CellRouteOperation::Query(invocation),
                    fingerprint: hash.finalize().into(),
                },
                CellRoutingLimits {
                    timeout: self.options.timeout,
                    max_attempts: self.options.max_attempts,
                },
                None,
                self.started,
            )
            .map_err(CellDispatchError::Routing)?;
        let mut attempt = session.begin().await.map_err(CellDispatchError::Routing)?;
        let mut previous = None;
        let mut next = 0;
        loop {
            self.live()?;
            let route = attempt.route().clone();
            if previous != Some(route.history) {
                previous = Some(route.history);
                next = 0;
            }
            let local = match self.owner.handle.core.log.cell_group(&route.history) {
                Ok(group) => Some(group.identity().member),
                Err(LogError::NotFound) => None,
                Err(error) => return Err(CellDispatchError::Runtime(error.into())),
            };
            let mut members = self.members(&route, local).await?;
            if let QueryRouting::Replicas {
                leader_fallback: true,
                ..
            } = self.options.routing
            {
                if let Ok(status) = self.owner.handle.core.runtime.status(route.history).await {
                    if let Some(leader) = status
                        .leader
                        .and_then(|id| MemberId::new(id).ok())
                        .filter(|id| !members.contains(id))
                    {
                        members.push(leader);
                    }
                }
            }
            let member = *members
                .get(next)
                .ok_or(CellDispatchError::Runtime(RuntimeError::NotReady))?;
            next += 1;
            let now = self
                .owner
                .handle
                .domain
                .mono_ms()
                .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
            let remaining = self
                .deadline
                .checked_sub(now)
                .filter(|n| *n != 0)
                .ok_or(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))?;
            let slice = match self.options.routing {
                QueryRouting::Local => remaining,
                QueryRouting::Replicas {
                    attempt_timeout, ..
                } => (attempt_timeout.as_millis() as u64).min(remaining),
                QueryRouting::Automatic => {
                    remaining
                        / u64::from(
                            self.options
                                .max_attempts
                                .saturating_sub(attempt.ordinal())
                                .saturating_add(1),
                        )
                        .max(1)
                }
            }
            .max(1);
            let deadline = now.saturating_add(slice).min(self.deadline);
            attempt
                .limit_query_deadline(deadline)
                .map_err(CellDispatchError::Routing)?;
            let selected = if local == Some(member) {
                self.owner
                    .handle
                    .core
                    .runtime
                    .resolve_cell_name(route.clone(), self.candidate.clone(), deadline)
                    .await
                    .map_err(CellDispatchError::Runtime)
            } else {
                let response: Result<CellRemoteNameResponse, _> = self
                    .owner
                    .handle
                    .core
                    .transport
                    .lookup_cell_name(&route, &self.candidate, member, deadline)
                    .await;
                response
                    .map(|response| response.record().cloned())
                    .map_err(CellDispatchError::Transport)
            };
            match selected {
                Ok(record) => {
                    self.live()?;
                    session
                        .validate_attempt(&attempt)
                        .map_err(CellDispatchError::Routing)?;
                    return Ok(record.map(|record| record.cell().clone()));
                }
                Err(error)
                    if name_retryable(&error) && attempt.ordinal() < self.options.max_attempts =>
                {
                    attempt = session
                        .refresh(&attempt)
                        .await
                        .map_err(CellDispatchError::Routing)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn members(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        local: Option<MemberId>,
    ) -> Result<Vec<MemberId>, CellDispatchError> {
        match &self.options.routing {
            QueryRouting::Local => Ok(local.into_iter().collect()),
            QueryRouting::Replicas { members, .. } => Ok(members.clone()),
            QueryRouting::Automatic => {
                let transport = &self.owner.handle.core.transport;
                let remote = match transport.cell_query_router_members(route, 8) {
                    Err(crate::raft::transport::TransportError::UnknownGroup)
                        if local.is_none() =>
                    {
                        transport
                            .refresh_managed_router(route.history, self.deadline)
                            .await
                            .map_err(CellDispatchError::Runtime)?;
                        transport
                            .cell_query_router_members(route, 8)
                            .map_err(CellDispatchError::Transport)?
                    }
                    Err(crate::raft::transport::TransportError::UnknownGroup) => Vec::new(),
                    result => result.map_err(CellDispatchError::Transport)?,
                };
                let mut members: Vec<_> = local.into_iter().collect();
                for member in remote {
                    if !members.contains(&member) {
                        members.push(member);
                    }
                }
                Ok(members)
            }
        }
    }

    fn live(&self) -> Result<(), CellDispatchError> {
        self.owner.check_open()?;
        if self
            .owner
            .handle
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            >= self.deadline
        {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        Ok(())
    }
}

fn name_retryable(error: &CellDispatchError) -> bool {
    matches!(
        error,
        CellDispatchError::Runtime(
            RuntimeError::NotReady | RuntimeError::NotFound | RuntimeError::DeadlineExceeded
        ) | CellDispatchError::Transport(
            crate::raft::transport::TransportError::Timeout
                | crate::raft::transport::TransportError::UnknownGroup
                | crate::raft::transport::TransportError::StalePeer
                | crate::raft::transport::TransportError::Query(
                    cellar_logic::consensus::CommandFailure::Unavailable
                )
        )
    )
}
