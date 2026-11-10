// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared cell reads reuse the peer query lane; namespace grants remain read-only.
use super::*;
use crate::application::consensus::CommandFailure;
use crate::raft::cell_groups::{
    CellDispatchError, CellGroupController, CellGroupDispatcher, CellGroupNamespaceStorage,
    CellRoutingLimits,
};
use crate::raft::query_identity::QueryRoute;
use crate::raft::query_wire::{
    self as wire,
    cell::{CellQueryCall, CellQueryValue},
};
use crate::raft::version::{CellVersionObservation, CellVersionToken, VersionKeys};
use cellar_logic::{
    cell_group::CellHistory,
    consensus::{MemberId, NodeIdentity},
};
use queries::{QueryConnection, SendQuery};
use std::time::Duration;

/// Host-owned registration. The transport retains only a weak reference, so it
/// cannot extend the host's lifetime or create a runtime/transport ownership cycle.
#[derive(Clone)]
pub struct CellQueryGateway {
    inner: Arc<Registration>,
}

// Accepted requests retain Gateway, never this admission owner. In-flight work
// cannot keep accepting new reads after the final host registration is dropped.
struct Registration {
    gateway: Arc<Gateway>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.gateway.accepting.store(false, Ordering::Release);
    }
}

pub(super) struct Gateway {
    accepting: AtomicBool,
    controller: CellGroupController,
    dispatcher: CellGroupDispatcher,
    storage: CellGroupNamespaceStorage,
}

/// Retains the same node/peer query admission through consumption of the result.
pub struct CellRemoteQueryResponse {
    value: CellQueryValue,
    replica: cellar_logic::consensus::ReplicaIdentity<GroupHistory>,
    _node: Permit,
    _peer: Permit,
}

impl CellRemoteQueryResponse {
    pub fn result(&self) -> &[u8] {
        &self.value.result
    }

    pub fn observation(&self) -> &CellVersionObservation {
        &self.value.observation
    }

    pub fn token(&self) -> &CellVersionToken {
        &self.value.token
    }

    pub fn served_by(&self) -> MemberId {
        self.value.served_by
    }

    pub fn replica(&self) -> &cellar_logic::consensus::ReplicaIdentity<GroupHistory> {
        &self.replica
    }
}

pub(in crate::raft) struct CellRemoteQueryReply {
    pub result: Result<CellRemoteQueryResponse, CommandFailure>,
    pub leader_hint: Option<MemberId>,
}

pub(crate) struct CellQueryContext {
    pub keys: VersionKeys,
    pub admission: Arc<Permit>,
}

pub(super) struct SendCellQuery {
    route: Arc<GroupRoute<GroupHistory>>,
    peer: RaftPeer<GroupHistory>,
    cell: CellHistory,
    call: CellQueryCall,
    started_at: u64,
    leader_generation: Option<std::sync::Weak<()>>,
    keys: VersionKeys,
    _admission: Arc<Permit>,
    deadline: u64,
    cancel: watch::Receiver<bool>,
    reply: oneshot::Sender<Result<CellRemoteQueryReply, TransportError>>,
    node: Permit,
    peer_bytes: Permit,
}

struct Cancel(watch::Sender<bool>);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

impl RaftTransport {
    /// Register one namespace's compiled reads on this shared runtime and SQL
    /// pool. Options provide the namespace's immutable-query services and cache;
    /// cell images are selected only by fresh runtime authority. This grants no
    /// origin permission. Retain the returned owner while serving requests.
    /// Dropping its final clone refuses new reads; already admitted reads retain
    /// their physical owners until completion or joined shutdown.
    pub fn install_cell_query_gateway(
        &self,
        dispatcher: CellGroupDispatcher,
        controller: CellGroupController,
        storage: CellGroupNamespaceStorage,
    ) -> Result<CellQueryGateway, TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        if !dispatcher.uses_domain(&state.domain) || !controller.belongs_to(runtime) {
            return Err(TransportError::Identity);
        }
        let (namespace, _, _) = dispatcher
            .host_binding()
            .map_err(|_| TransportError::Identity)?;
        storage
            .validate_namespace(&namespace)
            .map_err(|_| TransportError::Identity)?;
        let key = dispatcher.namespace();
        let mut gateways = state
            .cell_query_gateways
            .lock()
            .map_err(|_| TransportError::Closed)?;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        gateways.retain(|_, value| value.strong_count() > 0);
        if gateways.contains_key(&key) {
            return Err(TransportError::Refused);
        }
        if gateways.len() >= state.options.max_groups {
            return Err(TransportError::Overloaded);
        }
        let inner = Arc::new(Gateway {
            accepting: AtomicBool::new(true),
            controller,
            dispatcher,
            storage,
        });
        gateways.insert(key, Arc::downgrade(&inner));
        Ok(CellQueryGateway {
            inner: Arc::new(Registration { gateway: inner }),
        })
    }

    pub(crate) fn uses_query_domain(&self, domain: &DomainToken) -> bool {
        self.handle.0.domain.same_owner(domain)
    }

    /// Called only by a consumed catalog attempt. No forwarding or retry occurs
    /// here; the caller keeps its original deadline and attempt budget.
    pub(in crate::raft) async fn query_cell(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        cell: &CellHistory,
        member: MemberId,
        call: CellQueryCall,
        context: CellQueryContext,
        deadline: u64,
    ) -> Result<CellRemoteQueryReply, TransportError> {
        let state = &self.handle.0;
        // Discovery never creates a local replica or grants voting authority.
        state.runtime.shared()?;
        let (view, peer, pool, leader_generation) = {
            let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
            state.admit(&registry)?;
            let view = registry
                .physical_groups
                .get(&route.history)
                .or_else(|| registry.cell_query_groups.get(&route.history))
                .ok_or(TransportError::UnknownGroup)?
                .clone();
            if view.schema_digest != route.schema_digest || !route.history.same_namespace(cell) {
                return Err(TransportError::Identity);
            }
            let peer = view
                .peers
                .iter()
                .find(|p| {
                    p.replica.member == member
                        && p.replica.node != state.runtime.log().limits().node
                })
                .ok_or(TransportError::Identity)?
                .clone();
            let pool = registry
                .pools
                .get(&peer.replica.node)
                .ok_or(TransportError::StalePeer)?
                .clone();
            let leader_generation = registry.leaders.generation(&view);
            (view, peer, pool, leader_generation)
        };
        let started_at = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if started_at >= deadline {
            return Err(TransportError::Timeout);
        }
        let charge = queries::reservation(
            call.request
                .input
                .len()
                .checked_add(8192)
                .ok_or(TransportError::Limit)?,
            &state.options,
        )?;
        let node = state
            .outgoing
            .query
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let peer_bytes = pool
            .outgoing
            .query
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        drop(call.encode(state.options.max_query_request_bytes)?);
        if !state.current_cell_query_route(&view, &peer) {
            return Err(TransportError::StalePeer);
        }
        let (reply, result) = oneshot::channel();
        let (cancel, cancelled) = watch::channel(false);
        let _cancel = Cancel(cancel);
        pool.query
            .try_send(SendQuery::Shared(Box::new(SendCellQuery {
                route: view,
                peer,
                cell: cell.clone(),
                call,
                started_at,
                leader_generation,
                keys: context.keys,
                _admission: context.admission,
                deadline,
                cancel: cancelled,
                reply,
                node,
                peer_bytes,
            })))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TransportError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
            })?;
        result.await.map_err(|_| TransportError::Closed)?
    }
}

impl CellQueryGateway {
    /// The cluster and namespace bound to this host-owned registration.
    pub fn namespace(&self) -> (ClusterId, NamespaceId) {
        self.inner.gateway.dispatcher.namespace()
    }
}

impl State {
    pub(super) fn authorize_cell_query(
        &self,
        wire: &QueryRoute,
    ) -> Result<(Arc<Pool>, Option<watch::Receiver<bool>>), TransportError> {
        let runtime = self.runtime.shared()?;
        let (source, target, _) = wire.decode_shared()?;
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let route = registry
            .physical_groups
            .get(&target.scope)
            .ok_or(TransportError::UnknownGroup)?;
        let group = runtime
            .log()
            .cell_group(&target.scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        if *group.identity() != target
            || wire.target_label() != self.auth.source()
            || wire.schema() != route.schema_digest
        {
            return Err(TransportError::StalePeer);
        }
        shared::permitted(&group, &[&target])?;
        let implicit = route.peers.iter().find(|p| {
            p.replica.node_identity() == source
                && p.label == wire.source_label()
                && shared::permitted(&group, &[&p.replica]).is_ok()
        });
        self.authorize_query_origin(
            &registry,
            target.scope.cluster,
            target.scope.namespace,
            source,
            wire.source_label(),
            implicit,
        )
    }

    pub(super) async fn send_cell_query(
        &self,
        pool: &Pool,
        mut request: SendCellQuery,
        connection: &mut Option<QueryConnection>,
    ) -> bool {
        let result = self
            .query_until(
                pool,
                request.deadline,
                request.cancel.clone(),
                None,
                async {
                    if !self.current_cell_query_route(&request.route, &request.peer) {
                        return Err(TransportError::StalePeer);
                    }
                    let remaining = request
                        .deadline
                        .checked_sub(self.domain.mono_ms().map_err(|_| TransportError::Closed)?)
                        .filter(|n| *n > 0)
                        .ok_or(TransportError::Timeout)?;
                    request.call.timeout_ms = request.call.timeout_ms.min(remaining);
                    let body = request.call.encode(self.options.max_query_request_bytes)?;
                    let local = self.runtime.log().limits();
                    let route = QueryRoute::new_shared(
                        NodeIdentity {
                            node: local.node,
                            disk: local.disk,
                            process: local.process,
                        },
                        self.auth.source(),
                        &request.peer.replica,
                        &request.peer.label,
                        &request.cell,
                        request.route.schema_digest,
                    )?;
                    let body = self
                        .exchange_query(connection, &RaftPeerNode::from(&request.peer), route, body)
                        .await?;
                    if !self.current_cell_query_route(&request.route, &request.peer) {
                        return Err(TransportError::StalePeer);
                    }
                    let now = u64::try_from(crate::asyncrt::wall_ms())
                        .map_err(|_| TransportError::Authentication)?;
                    let reply = wire::cell::decode_cell_reply(
                        &body,
                        &request.cell,
                        request.route.schema_digest,
                        &request.keys,
                        now,
                        self.options.max_query_response_bytes,
                        request.peer.replica.member,
                    )?;
                    if reply
                        .1
                        .as_ref()
                        .is_some_and(|advice| advice.history != request.route.scope)
                    {
                        return Err(TransportError::Authentication);
                    }
                    self.remember_cell_leader(
                        &request.route,
                        request.leader_generation.as_ref(),
                        &request.peer,
                        reply.1,
                        request.started_at,
                    );
                    Ok(reply)
                },
            )
            .await;
        // A fully authenticated application refusal consumed a complete reply;
        // retain the peer connection for the next authenticated request.
        let failed = result.is_err();
        let _ = request
            .reply
            .send(result.map(|(value, leader_hint)| CellRemoteQueryReply {
                result: value.map(|value| CellRemoteQueryResponse {
                    value,
                    replica: request.peer.replica.clone(),
                    _node: request.node,
                    _peer: request.peer_bytes,
                }),
                leader_hint: leader_hint.map(|advice| advice.leader),
            }));
        failed
    }
}

pub(super) enum IncomingCall {
    Shared(CellQueryCall),
    Command(crate::raft::query_wire::command::CellCommandCall),
    CommandProbe(crate::raft::query_wire::command::CellCommandProbe),
    Enrollment(crate::raft::query_wire::enrollment::CellEnrollmentCall),
    Receipt(crate::raft::query_wire::receipt::CellReceiptCall),
}

impl IncomingCall {
    pub(super) fn decode(
        route: &QueryRoute,
        bytes: &[u8],
        limit: usize,
    ) -> Result<Self, TransportError> {
        if route.is_receipt() {
            return Ok(Self::Receipt(
                crate::raft::query_wire::receipt::CellReceiptCall::decode(bytes, limit)?,
            ));
        }
        if route.is_enrollment() {
            return Ok(Self::Enrollment(
                crate::raft::query_wire::enrollment::CellEnrollmentCall::decode(bytes, limit)?,
            ));
        }
        if route.is_command_probe() {
            return Ok(Self::CommandProbe(
                crate::raft::query_wire::command::CellCommandProbe::decode(bytes, limit)?,
            ));
        }
        if route.is_command() {
            return Ok(Self::Command(
                crate::raft::query_wire::command::CellCommandCall::decode(bytes, limit)?,
            ));
        }
        Ok(Self::Shared(CellQueryCall::decode(bytes, limit)?))
    }

    pub(super) fn timeout_ms(&self) -> u64 {
        match self {
            Self::Shared(call) => call.timeout_ms,
            Self::Command(call) => call.timeout_ms,
            Self::CommandProbe(call) => call.timeout_ms,
            Self::Enrollment(call) => call.timeout_ms,
            Self::Receipt(call) => call.timeout_ms,
        }
    }

    pub(super) async fn execute(
        self,
        state: &State,
        route: &QueryRoute,
        deadline: u64,
    ) -> Result<super::query_replies::Reply, TransportError> {
        match self {
            Self::Command(call) => state.execute_cell_command(route, call, deadline).await,
            Self::CommandProbe(call) => {
                state
                    .execute_cell_command_probe(route, call, deadline)
                    .await
            }
            Self::Enrollment(call) => state.execute_cell_enrollment(route, call).await,
            Self::Receipt(call) => state.execute_cell_receipt(route, call).await,
            Self::Shared(call) => {
                let (_, target, cell) = route.decode_shared()?;
                let gateway = state
                    .cell_query_gateways
                    .lock()
                    .map_err(|_| TransportError::Closed)?
                    .get(&(cell.cluster, cell.namespace))
                    .and_then(|h| h.upgrade())
                    .ok_or(TransportError::Refused)?;
                if !gateway.accepting.load(Ordering::Acquire) {
                    return Err(TransportError::Refused);
                }
                // Queue first: metadata observation cannot renew the query's
                // captured deadline or delay ownership transfer to its producer.
                let queued = gateway.enqueue(call, target.scope, cell, route.schema());
                let leader_hint = if queued.is_ok() {
                    state
                        .runtime
                        .shared()?
                        .leader_advice(target.scope, deadline)
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };
                let result = match queued {
                    Ok(call) => call.response().await,
                    Err(error) => Err(error),
                }
                .map_err(|e| query_failure(&e));
                wire::cell::encode_cell_result(
                    &result,
                    leader_hint.as_ref(),
                    state.options.max_query_response_bytes,
                )
                .map(super::query_replies::Reply::plain)
            }
        }
    }
}

impl Gateway {
    fn enqueue(
        &self,
        call: CellQueryCall,
        group: GroupHistory,
        cell: CellHistory,
        schema: [u8; 32],
    ) -> Result<crate::raft::cell_groups::CellQueryCall, Arc<CellDispatchError>> {
        let options = self.storage.for_cell(&cell);
        let queued = (|| {
            let mut query = self.dispatcher.query(
                cell,
                call.request,
                CellRoutingLimits {
                    timeout: Duration::from_millis(call.timeout_ms),
                    max_attempts: 1,
                },
                None,
            )?;
            query.bind_remote_route(crate::raft::cell_groups::RemoteMutationRoute {
                group,
                schema,
                epoch: call.route_epoch,
                birth: call.birth_digest,
            });
            self.controller.query(query, options)
        })();
        queued.map_err(Arc::new)
    }
}

fn query_failure(error: &CellDispatchError) -> CommandFailure {
    use crate::raft::{execution::ExecutionError, version::VersionError};
    match error {
        CellDispatchError::Execution(ExecutionError::Refused(failure)) => failure.clone(),
        CellDispatchError::Runtime(RuntimeError::Overloaded) => CommandFailure::Overloaded,
        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded | RuntimeError::NotReady) => {
            CommandFailure::VersionUnavailable
        }
        CellDispatchError::Version(VersionError::WrongHistory) => CommandFailure::WrongIncarnation,
        CellDispatchError::Version(_) => CommandFailure::VersionUnavailable,
        CellDispatchError::Registration(_) => CommandFailure::UnsupportedCapability,
        _ => CommandFailure::Unavailable,
    }
}
