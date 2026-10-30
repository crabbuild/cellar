// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Commands and enrollment share bounded RPC admission and explicit write grants.
use super::*;
use crate::application::consensus::CommandFailure;
use crate::raft::cell_groups::{
    CellDispatchError, CellGroupController, CellGroupDispatcher, CellRoutingLimits, CellSqlOptions,
    EnrollmentRecord,
};
use crate::raft::query_identity::QueryRoute;
use crate::raft::query_wire::{
    self as wire,
    command::{CellCommandCall, CellCommandValue},
    enrollment::CellEnrollmentCall,
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
pub struct CellCommandGateway {
    inner: Arc<Registration>,
}

// Accepted requests retain Gateway, never this admission owner. In-flight work
// cannot keep accepting new commands after the final host registration is dropped.
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
    options: CellSqlOptions,
}

/// Retains the same node/peer query admission through consumption of the result.
pub struct CellRemoteCommandResponse {
    value: CellCommandValue,
    _node: Permit,
    _peer: Permit,
}

impl CellRemoteCommandResponse {
    pub fn command(&self) -> cellar_logic::consensus::CommandId {
        self.value.command
    }

    pub fn retry_until_ms(&self) -> u64 {
        self.value.retry_until_ms
    }

    pub fn durability(&self) -> cellar_logic::consensus::Durability {
        cellar_logic::consensus::Durability::Quorum
    }

    pub fn result(&self) -> &[u8] {
        &self.value.value.result
    }

    pub fn observation(&self) -> &CellVersionObservation {
        &self.value.value.observation
    }

    pub fn token(&self) -> &CellVersionToken {
        &self.value.value.token
    }

    pub fn served_by(&self) -> MemberId {
        self.value.value.served_by
    }
}

/// An authenticated creation receipt, retaining transport admission until dropped.
/// The original enrollment image is not a claim about the latest cell version.
pub struct CellRemoteEnrollmentResponse {
    record: EnrollmentRecord,
    served_by: MemberId,
    _node: Permit,
    _peer: Permit,
}

impl CellRemoteEnrollmentResponse {
    pub fn record(&self) -> &EnrollmentRecord {
        &self.record
    }

    pub fn served_by(&self) -> MemberId {
        self.served_by
    }

    pub fn durability(&self) -> cellar_logic::consensus::Durability {
        cellar_logic::consensus::Durability::Quorum
    }
}

enum WriteCall {
    Command(CellCommandCall),
    Enrollment(CellEnrollmentCall),
}

impl WriteCall {
    fn encode(&self, limit: usize) -> Result<Vec<u8>, TransportError> {
        match self {
            Self::Command(c) => c.encode(limit),
            Self::Enrollment(c) => c.encode(limit),
        }
    }

    fn shorten_timeout(&mut self, remaining: u64) {
        let timeout = match self {
            Self::Command(c) => &mut c.timeout_ms,
            Self::Enrollment(c) => &mut c.timeout_ms,
        };
        *timeout = (*timeout).min(remaining);
    }

    fn input_bytes(&self) -> usize {
        match self {
            Self::Command(c) => c.request.input.len(),
            Self::Enrollment(_) => 0,
        }
    }
}

enum WriteValue {
    Command(CellCommandValue),
    Enrollment(EnrollmentRecord),
}

struct WriteResponse {
    value: WriteValue,
    served_by: MemberId,
    node: Permit,
    peer: Permit,
}

pub(super) struct SendCellWrite {
    route: Arc<GroupRoute<GroupHistory>>,
    peer: RaftPeer<GroupHistory>,
    cell: CellHistory,
    call: WriteCall,
    keys: VersionKeys,
    deadline: u64,
    cancel: watch::Receiver<bool>,
    reply: oneshot::Sender<Result<WriteResponse, TransportError>>,
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
    /// Register compiled commands and initialization on the exact controller/runtime owner. Retain
    /// this host-owned handle while accepting commands. Dropping it refuses new
    /// admission; commands already transferred to the controller remain owned.
    /// Namespace write grants are independently required even for voter peers.
    pub fn install_cell_command_gateway(
        &self,
        dispatcher: CellGroupDispatcher,
        controller: CellGroupController,
        options: CellSqlOptions,
    ) -> Result<CellCommandGateway, TransportError> {
        let state = &self.handle.0;
        let runtime = state.runtime.shared()?;
        if !dispatcher.uses_domain(&state.domain) || !controller.belongs_to(runtime) {
            return Err(TransportError::Identity);
        }
        let key = dispatcher.namespace();
        let mut gateways = state
            .cell_command_gateways
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
            options,
        });
        gateways.insert(key, Arc::downgrade(&inner));
        Ok(CellCommandGateway {
            inner: Arc::new(Registration { gateway: inner }),
        })
    }

    pub(crate) fn uses_command_domain(&self, domain: &DomainToken) -> bool {
        self.handle.0.domain.same_owner(domain)
    }

    /// Called only by a consumed catalog attempt. No forwarding or retry occurs
    /// here; the caller keeps its original deadline and attempt budget.
    pub(in crate::raft) async fn command_cell(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        cell: &CellHistory,
        member: MemberId,
        call: CellCommandCall,
        keys: VersionKeys,
        deadline: u64,
    ) -> Result<CellRemoteCommandResponse, TransportError> {
        let response = self
            .write_cell(
                route,
                cell,
                member,
                WriteCall::Command(call),
                keys,
                deadline,
            )
            .await?;
        let WriteValue::Command(value) = response.value else {
            return Err(TransportError::Protocol);
        };
        Ok(CellRemoteCommandResponse {
            value,
            _node: response.node,
            _peer: response.peer,
        })
    }

    pub(in crate::raft) async fn enroll_cell(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        cell: &CellHistory,
        member: MemberId,
        call: CellEnrollmentCall,
        keys: VersionKeys,
        deadline: u64,
    ) -> Result<CellRemoteEnrollmentResponse, TransportError> {
        let response = self
            .write_cell(
                route,
                cell,
                member,
                WriteCall::Enrollment(call),
                keys,
                deadline,
            )
            .await?;
        let WriteValue::Enrollment(record) = response.value else {
            return Err(TransportError::Protocol);
        };
        Ok(CellRemoteEnrollmentResponse {
            record,
            served_by: response.served_by,
            _node: response.node,
            _peer: response.peer,
        })
    }

    async fn write_cell(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        cell: &CellHistory,
        member: MemberId,
        call: WriteCall,
        keys: VersionKeys,
        deadline: u64,
    ) -> Result<WriteResponse, TransportError> {
        let state = &self.handle.0;
        // Discovery never creates a local replica or grants voting authority.
        state.runtime.shared()?;
        let (view, peer, pool) = {
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
            (view, peer, pool)
        };
        if state.domain.mono_ms().map_err(|_| TransportError::Closed)? >= deadline {
            return Err(TransportError::Timeout);
        }
        let charge = queries::reservation(
            call.input_bytes()
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
            .try_send(SendQuery::Write(Box::new(SendCellWrite {
                route: view,
                peer,
                cell: cell.clone(),
                call,
                keys,
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

impl CellCommandGateway {
    /// The cluster and namespace bound to this host-owned registration.
    pub fn namespace(&self) -> (ClusterId, NamespaceId) {
        self.inner.gateway.dispatcher.namespace()
    }
}

impl State {
    pub(super) fn authorize_cell_command(
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
        let grant = registry
            .command_origins
            .get(&(target.scope.cluster, target.scope.namespace, source.node))
            .filter(|grant| !*grant.stop.borrow())
            .filter(|grant| {
                grant.peer.as_ref().is_some_and(|peer| {
                    peer.node == source.node
                        && peer.disk == source.disk
                        && peer.process == source.process
                        && peer.label == wire.source_label()
                })
            })
            .ok_or(TransportError::Refused)?;
        let pool = registry
            .pools
            .get(&source.node)
            .filter(|pool| grant.peer.as_ref() == Some(&pool.peer))
            .cloned()
            .ok_or(TransportError::Refused)?;
        Ok((pool, Some(grant.stop.subscribe())))
    }

    pub(super) async fn send_cell_write(
        &self,
        pool: &Pool,
        mut request: SendCellWrite,
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
                    request.call.shorten_timeout(remaining);
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
                    let route = match &request.call {
                        WriteCall::Command(_) => route.for_command()?,
                        WriteCall::Enrollment(_) => route.for_enrollment()?,
                    };
                    let body = self
                        .exchange_query(connection, &RaftPeerNode::from(&request.peer), route, body)
                        .await?;
                    if !self.current_cell_query_route(&request.route, &request.peer) {
                        return Err(TransportError::StalePeer);
                    }
                    match &request.call {
                        WriteCall::Command(call) => {
                            let now = u64::try_from(crate::asyncrt::wall_ms())
                                .map_err(|_| TransportError::Authentication)?;
                            wire::command::decode_result(
                                &body,
                                (call.request.id, &request.cell),
                                request.route.schema_digest,
                                &request.keys,
                                now,
                                self.options.max_query_response_bytes,
                                request.peer.replica.member,
                            )?
                            .map(WriteValue::Command)
                        }
                        WriteCall::Enrollment(call) => wire::enrollment::decode_result(
                            &body,
                            call.request.creation,
                            &request.cell,
                            request.route.schema_digest,
                            self.options.max_query_response_bytes,
                        )?
                        .map(WriteValue::Enrollment),
                    }
                    .map_err(TransportError::Command)
                },
            )
            .await;
        // A fully authenticated application refusal consumed a complete reply;
        // retain the peer connection just as the legacy query lane does.
        let failed = matches!(&result, Err(error) if !matches!(error, TransportError::Command(_)));
        let _ = request.reply.send(result.map(|value| WriteResponse {
            value,
            served_by: request.peer.replica.member,
            node: request.node,
            peer: request.peer_bytes,
        }));
        failed
    }
}

impl State {
    pub(super) async fn execute_cell_enrollment(
        &self,
        route: &QueryRoute,
        call: CellEnrollmentCall,
    ) -> Result<Vec<u8>, TransportError> {
        let (_, target, cell) = route.decode_shared()?;
        let gateway = self
            .cell_command_gateways
            .lock()
            .map_err(|_| TransportError::Closed)?
            .get(&(cell.cluster, cell.namespace))
            .and_then(|g| g.upgrade())
            .ok_or(TransportError::Refused)?;
        if !gateway.accepting.load(Ordering::Acquire) {
            return Err(TransportError::Refused);
        }
        let binding = crate::raft::cell_groups::RemoteMutationRoute {
            group: target.scope,
            schema: route.schema(),
            epoch: call.route_epoch,
            birth: call.birth_digest,
        };
        let result = async {
            let mut invocation = gateway
                .dispatcher
                .enroll_with_inputs(
                    cell,
                    call.request,
                    CellRoutingLimits {
                        timeout: Duration::from_millis(call.timeout_ms),
                        max_attempts: 1,
                    },
                    None,
                    Some((call.inputs, call.preview)),
                )
                .await
                .map_err(|error| command_failure(&error.cause))?;
            invocation.bind_remote_route(binding);
            if !gateway.accepting.load(Ordering::Acquire) {
                return Err(CommandFailure::Unavailable);
            }
            self.authorize_cell_command(route)
                .map_err(|_| CommandFailure::Unavailable)?;
            let call = gateway
                .controller
                .enroll(invocation, gateway.options.clone())
                .map_err(|error| command_failure(&error.error.cause))?;
            let response = call
                .response()
                .await
                .map_err(|error| command_failure(&error.cause))?;
            Ok(response.record().clone())
        }
        .await;
        wire::enrollment::encode_result(&result, self.options.max_query_response_bytes)
    }

    pub(super) async fn execute_cell_command(
        &self,
        route: &QueryRoute,
        call: CellCommandCall,
    ) -> Result<Vec<u8>, TransportError> {
        let (_, target, cell) = route.decode_shared()?;
        let gateway = self
            .cell_command_gateways
            .lock()
            .map_err(|_| TransportError::Closed)?
            .get(&(cell.cluster, cell.namespace))
            .and_then(|g| g.upgrade())
            .ok_or(TransportError::Refused)?;
        if !gateway.accepting.load(Ordering::Acquire) {
            return Err(TransportError::Refused);
        }
        let binding = crate::raft::cell_groups::RemoteMutationRoute {
            group: target.scope,
            schema: route.schema(),
            epoch: call.route_epoch,
            birth: call.birth_digest,
        };
        let result = async {
            let mut invocation = gateway
                .dispatcher
                .command_with_inputs(
                    cell,
                    call.request,
                    CellRoutingLimits {
                        timeout: Duration::from_millis(call.timeout_ms),
                        max_attempts: 1,
                    },
                    None,
                    Some(call.inputs),
                )
                .await
                .map_err(|error| command_failure(&error.cause))?;
            invocation.bind_remote_route(binding);
            if !gateway.accepting.load(Ordering::Acquire) {
                return Err(CommandFailure::Unavailable);
            }
            // Recheck after asynchronous request construction, before ownership
            // transfers. Later revocation cannot undo an admitted mutation.
            self.authorize_cell_command(route)
                .map_err(|_| CommandFailure::Unavailable)?;
            let call = gateway
                .controller
                .command(invocation, gateway.options.clone())
                .map_err(|error| command_failure(&error.error.cause))?;
            call.response()
                .await
                .map_err(|error| command_failure(&error.cause))
        }
        .await;
        wire::command::encode_result(&result, self.options.max_query_response_bytes)
    }
}

fn command_failure(error: &CellDispatchError) -> CommandFailure {
    use crate::raft::execution::ExecutionError;
    match error {
        CellDispatchError::Execution(ExecutionError::Refused(failure)) => failure.clone(),
        CellDispatchError::Runtime(RuntimeError::NotLeader) => {
            CommandFailure::NotLeader { leader: None }
        }
        CellDispatchError::Runtime(RuntimeError::Overloaded) => CommandFailure::Overloaded,
        CellDispatchError::Registration(_) => CommandFailure::UnsupportedCapability,
        _ => CommandFailure::Unavailable,
    }
}
