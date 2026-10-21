//! Node-owned TLS connections multiplex groups. Independent control, data and
//! query listeners/workers/budgets preserve progress while queries await LTX.

use super::budget::{Budget, Permit};
use super::completion::RuntimeError;
use super::runtime::RaftRuntime;
use super::transport_frame::{self as frame, Disposition, FrameHead, RequestHeader, Route};
use super::transport_options::{NamespaceQueryOrigin, RaftPeerNode, RaftTls, TransportOptions};
use crate::asyncrt::DomainToken;
use crate::peer_auth::PeerAuth;
use ::raft::prelude::{Message, MessageType};
use cellar_logic::consensus::{ClusterId, GroupScope, NamespaceId, NodeId, ReplicaIdentity};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, Notify, Semaphore};

#[path = "transport_directory.rs"]
mod directory;
#[path = "query_transport.rs"]
mod queries;
#[path = "seed_transport.rs"]
pub(crate) mod seeds;
#[path = "snapshot_transport.rs"]
pub(crate) mod snapshots;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lane {
    Control,
    Data,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    Invalid(String),
    Io(String),
    Tls(String),
    Runtime(RuntimeError),
    Authentication,
    Protocol,
    Identity,
    Replay,
    UnknownGroup,
    StalePeer,
    Refused,
    Limit,
    Overloaded,
    Timeout,
    Closed,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft transport: {self:?}")
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

impl From<RuntimeError> for TransportError {
    fn from(e: RuntimeError) -> Self {
        Self::Runtime(e)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftPeer {
    pub replica: ReplicaIdentity,
    pub label: String,
    pub server_name: String,
    pub control_address: SocketAddr,
    pub data_address: SocketAddr,
    pub query_address: SocketAddr,
}

impl RaftPeer {
    fn same_host(&self, other: &Self) -> bool {
        self.replica.node == other.replica.node
            && self.replica.disk == other.replica.disk
            && self.replica.process == other.replica.process
            && self.label == other.label
            && self.server_name == other.server_name
            && self.control_address == other.control_address
            && self.data_address == other.data_address
            && self.query_address == other.query_address
    }
}

impl From<&RaftPeer> for RaftPeerNode {
    fn from(peer: &RaftPeer) -> Self {
        Self {
            node: peer.replica.node,
            disk: peer.replica.disk,
            process: peer.replica.process,
            label: peer.label.clone(),
            server_name: peer.server_name.clone(),
            control_address: peer.control_address,
            data_address: peer.data_address,
            query_address: peer.query_address,
        }
    }
}

impl RaftPeerNode {
    pub(crate) fn validate_endpoint(&self) -> Result<(), TransportError> {
        if crate::machine::validate_node_name(&self.label).is_err()
            || rustls::pki_types::ServerName::try_from(self.server_name.clone()).is_err()
            || self.control_address.port() == 0
            || self.data_address.port() == 0
            || self.query_address.port() == 0
            || self.control_address == self.data_address
            || self.control_address == self.query_address
            || self.data_address == self.query_address
        {
            return Err(TransportError::Identity);
        }
        Ok(())
    }

    fn address(&self, lane: Lane) -> SocketAddr {
        match lane {
            Lane::Control => self.control_address,
            Lane::Data => self.data_address,
        }
    }

    fn same_host(&self, peer: &RaftPeer) -> bool {
        self.node == peer.replica.node
            && self.disk == peer.replica.disk
            && self.process == peer.replica.process
            && self.label == peer.label
            && self.server_name == peer.server_name
            && self.control_address == peer.control_address
            && self.data_address == peer.data_address
            && self.query_address == peer.query_address
    }
}

/// An already authorized routing view, installed by the membership controller.
/// Installing it never creates storage or changes Raft's configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupRoute {
    pub scope: GroupScope,
    pub schema_digest: [u8; 32],
    pub revision: u64,
    pub peers: Vec<RaftPeer>,
}

/// Message admission only. No durability, commit or applied position is implied.
pub struct Delivery {
    result: oneshot::Receiver<Result<(), TransportError>>,
}

impl Delivery {
    pub async fn accepted(self) -> Result<(), TransportError> {
        self.result.await.map_err(|_| TransportError::Closed)?
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportUsage {
    pub control_bytes: usize,
    pub data_bytes: usize,
    pub control_items: usize,
    pub data_items: usize,
    pub query_bytes: usize,
    pub query_items: usize,
    pub tasks: usize,
    pub peers: usize,
    pub groups: usize,
    pub accepted: u64,
    pub refused: u64,
    pub failed: u64,
}

struct Banks {
    control: Arc<Budget>,
    data: Arc<Budget>,
    query: Arc<Budget>,
}

impl Banks {
    fn new(control: usize, data: usize, query: usize, items: usize) -> Self {
        Self {
            control: Budget::new(control, items),
            data: Budget::new(data, items),
            query: Budget::new(query, items),
        }
    }

    fn lane(&self, lane: Lane) -> &Arc<Budget> {
        match lane {
            Lane::Control => &self.control,
            Lane::Data => &self.data,
        }
    }
}

struct SendRequest {
    scope: GroupScope,
    route: Arc<GroupRoute>,
    peer: RaftPeer,
    header: RequestHeader,
    body: Vec<u8>,
    snapshot: bool,
    result: Option<oneshot::Sender<Result<(), TransportError>>>,
    _node: Permit,
    _peer: Permit,
}

struct Pool {
    peer: RaftPeerNode,
    tx: [mpsc::Sender<SendRequest>; 2],
    query: mpsc::Sender<queries::SendQuery>,
    outgoing: Banks,
    incoming: Banks,
    stop: watch::Sender<bool>,
}

struct PoolStart {
    pool: Arc<Pool>,
    control: mpsc::Receiver<SendRequest>,
    data: mpsc::Receiver<SendRequest>,
    query: mpsc::Receiver<queries::SendQuery>,
    guards: [TaskGuard; 3],
}

struct Registry {
    closed: bool,
    tasks: usize,
    groups: BTreeMap<GroupScope, Arc<GroupRoute>>,
    query_groups: BTreeMap<GroupScope, Arc<GroupRoute>>,
    origins: BTreeMap<(ClusterId, NamespaceId, NodeId), Arc<queries::OriginGrant>>,
    pools: BTreeMap<NodeId, Arc<Pool>>,
    node_views: BTreeMap<NodeId, crate::raft::catalog::NodeView>,
    seed_peers: BTreeMap<NodeId, directory::SeedPeer>,
}

impl Registry {
    fn prune_pools(&mut self) {
        let used: BTreeSet<_> = self
            .groups
            .values()
            .chain(self.query_groups.values())
            .flat_map(|route| route.peers.iter().map(|peer| peer.replica.node))
            .chain(
                self.origins
                    .values()
                    .filter_map(|grant| grant.peer.as_ref().map(|peer| peer.node)),
            )
            .collect();
        self.pools.retain(|node, pool| {
            if used.contains(node) {
                true
            } else {
                pool.stop.send_replace(true);
                false
            }
        });
    }
}

struct State {
    domain: DomainToken,
    runtime: RaftRuntime,
    auth: Arc<PeerAuth>,
    tls: RaftTls,
    options: TransportOptions,
    local_control: SocketAddr,
    local_data: SocketAddr,
    local_query: SocketAddr,
    query_gateway: Mutex<Option<std::sync::Weak<crate::application::consensus::client::Handle>>>,
    registry: Mutex<Registry>,
    outgoing: Banks,
    incoming: Banks,
    stop: watch::Sender<bool>,
    done: Notify,
    sequence: AtomicU64,
    accepted: AtomicU64,
    refused: AtomicU64,
    failed: AtomicU64,
    claim: Mutex<Option<super::runtime::TransportClaim>>,
    snapshot_source: Mutex<Option<std::sync::Weak<snapshots::Source>>>,
    snapshot_source_budget: Arc<Budget>,
    snapshot_receive_budget: Arc<Budget>,
    snapshot_connections: Arc<Semaphore>,
    seed_metadata: Arc<Budget>,
}

struct Handle(Arc<State>);

impl Drop for Handle {
    fn drop(&mut self) {
        self.0.stop();
    }
}

#[derive(Clone)]
pub struct RaftTransport {
    handle: Arc<Handle>,
}

struct TaskGuard(Arc<State>);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let finished = {
            let mut registry = self.0.lifecycle_registry();
            registry.tasks -= 1;
            registry.closed && registry.tasks == 0
        };
        if finished {
            if let Ok(mut claim) = self.0.claim.lock() {
                claim.take();
            }
        }
        self.0.done.notify_waiters();
    }
}

// Field drop order releases sockets/queued permits before decrementing the task
// count, including cancellation before the task's first poll.
struct OwnedTask {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    _guard: TaskGuard,
}

impl Future for OwnedTask {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

impl RaftTransport {
    pub(crate) fn peer_limit(&self) -> usize {
        self.handle.0.options.max_peers
    }

    /// All three listeners must be caller-bound. Accepted sockets, deadlines and
    /// workers belong to the runtime's execution domain and join on its shutdown.
    pub async fn start(
        runtime: RaftRuntime,
        auth: Arc<PeerAuth>,
        tls: RaftTls,
        control: TcpListener,
        data: TcpListener,
        query: TcpListener,
        options: TransportOptions,
    ) -> Result<Self, TransportError> {
        options.validate()?;
        let local_control = control.local_addr()?;
        let local_data = data.local_addr()?;
        let local_query = query.local_addr()?;
        if local_control == local_data || local_query == local_control || local_query == local_data
        {
            return Err(TransportError::Invalid(
                "separate control/data/query listeners required".into(),
            ));
        }
        let domain = runtime.log().domain();
        domain.mono_ms().map_err(|_| TransportError::Closed)?;
        let claim = runtime.claim_transport()?;
        let (stop, _) = watch::channel(false);
        let snapshot_memory = snapshots::memory_bound(&options);
        let state = Arc::new(State {
            domain,
            runtime,
            auth,
            tls,
            local_control,
            local_data,
            local_query,
            query_gateway: Mutex::new(None),
            registry: Mutex::new(Registry {
                closed: false,
                tasks: 0,
                groups: BTreeMap::new(),
                query_groups: BTreeMap::new(),
                origins: BTreeMap::new(),
                pools: BTreeMap::new(),
                node_views: BTreeMap::new(),
                seed_peers: BTreeMap::new(),
            }),
            outgoing: Banks::new(
                options.node_control_bytes,
                options.node_data_bytes,
                options.node_query_bytes,
                options.queue_items * options.max_peers,
            ),
            incoming: Banks::new(
                options.node_control_bytes,
                options.node_data_bytes,
                options.node_query_bytes,
                options.max_connections_per_lane,
            ),
            stop,
            done: Notify::new(),
            sequence: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            options,
            claim: Mutex::new(Some(claim)),
            snapshot_source: Mutex::new(None),
            snapshot_source_budget: Budget::new(snapshot_memory, 1),
            snapshot_receive_budget: Budget::new(snapshot_memory + 256 * 1024 * 1024, 1),
            snapshot_connections: Arc::new(Semaphore::new(1)),
            seed_metadata: Budget::new(seeds::META_BYTES, 1),
        });
        let service = Self {
            handle: Arc::new(Handle(state.clone())),
        };
        for (listener, lane) in [(control, Lane::Control), (data, Lane::Data)] {
            let worker = state.clone();
            state.spawn(async move {
                worker.listen(listener, lane).await;
            })?;
        }
        let worker = state.clone();
        state.spawn(async move {
            worker.listen_queries(query).await;
        })?;
        Ok(service)
    }

    pub fn control_address(&self) -> SocketAddr {
        self.handle.0.local_control
    }

    pub fn data_address(&self) -> SocketAddr {
        self.handle.0.local_data
    }

    pub fn query_address(&self) -> SocketAddr {
        self.handle.0.local_query
    }

    pub fn install_group(&self, route: GroupRoute) -> Result<(), TransportError> {
        let state = &self.handle.0;
        route
            .scope
            .validate()
            .map_err(|_| TransportError::Identity)?;
        let group = state
            .runtime
            .log()
            .group(&route.scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        let local = group.identity();
        if route.revision == 0 || route.peers.is_empty() || route.peers.len() > 128 {
            return Err(TransportError::Invalid("invalid group route".into()));
        }
        let cache = group
            .read()
            .map_err(|e| TransportError::Runtime(e.into()))?;
        let conf = cache.conf_state.clone();
        let membership = cache.membership.clone();
        drop(cache);
        let members: BTreeSet<_> = conf
            .voters
            .iter()
            .chain(&conf.voters_outgoing)
            .chain(&conf.learners)
            .chain(&conf.learners_next)
            .copied()
            .collect();
        let mut ids = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        for peer in &route.peers {
            if peer.replica.scope != route.scope
                || !members.contains(&peer.replica.member.get())
                || !membership.permits(&peer.replica)
                || !ids.insert(peer.replica.member)
                || !nodes.insert(peer.replica.node)
                || crate::machine::validate_node_name(&peer.label).is_err()
                || rustls::pki_types::ServerName::try_from(peer.server_name.clone()).is_err()
                || peer.control_address == peer.data_address
                || peer.query_address == peer.control_address
                || peer.query_address == peer.data_address
            {
                return Err(TransportError::Identity);
            }
        }
        let own = route
            .peers
            .iter()
            .find(|p| p.replica.member == local.member)
            .ok_or(TransportError::Identity)?;
        if own.replica != *local || own.label != state.auth.source() {
            return Err(TransportError::Identity);
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        for peer in &route.peers {
            registry.check_node_view(&RaftPeerNode::from(peer))?;
        }
        if registry.query_groups.contains_key(&route.scope) {
            return Err(TransportError::Identity);
        }
        if let Some(old) = registry.groups.get(&route.scope) {
            if **old == route {
                return Ok(());
            }
            if old.revision >= route.revision || old.schema_digest != route.schema_digest {
                return Err(TransportError::StalePeer);
            }
            for peer in &route.peers {
                if let Some(previous) = old
                    .peers
                    .iter()
                    .find(|p| p.replica.member == peer.replica.member)
                {
                    if previous.replica.node != peer.replica.node
                        || previous.replica.disk != peer.replica.disk
                    {
                        return Err(TransportError::Identity);
                    }
                }
            }
        } else if registry.groups.len() + registry.query_groups.len() >= state.options.max_groups {
            return Err(TransportError::Overloaded);
        }
        // Existing peers' physical addresses/process must agree across groups.
        // A controller removes conflicting old routing views before replacement.
        for peer in route.peers.iter().filter(|p| p.replica.node != local.node) {
            if registry
                .groups
                .values()
                .filter(|g| g.scope != route.scope)
                .flat_map(|g| &g.peers)
                .any(|p| p.replica.node == peer.replica.node && !p.same_host(peer))
            {
                return Err(TransportError::StalePeer);
            }
        }
        let physical: Vec<_> = route
            .peers
            .iter()
            .filter(|peer| peer.replica.node != local.node)
            .map(RaftPeerNode::from)
            .collect();
        for peer in &physical {
            if registry
                .query_groups
                .values()
                .flat_map(|view| &view.peers)
                .any(|other| other.replica.node == peer.node && !peer.same_host(other))
                || registry
                    .origins
                    .values()
                    .filter_map(|grant| grant.peer.as_ref())
                    .any(|other| other.node == peer.node && other != peer)
            {
                return Err(TransportError::StalePeer);
            }
        }
        let starts = state.prepare_pools(&mut registry, &physical)?;
        registry.groups.insert(route.scope.clone(), Arc::new(route));
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)?;
        Ok(())
    }

    pub fn remove_group(&self, scope: &GroupScope) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.groups.remove(scope);
        registry.prune_pools();
        Ok(())
    }

    /// Queue without waiting on a slow peer. The returned delivery owns the
    /// completion only; queued/in-flight bytes remain charged if it is dropped.
    /// Immediate refusal has not delivered a message; the dispatcher must retain
    /// or report it as unreachable. Accepted queue failures are reported here.
    pub fn submit(&self, scope: &GroupScope, message: Message) -> Result<Delivery, TransportError> {
        let state = &self.handle.0;
        let lane = frame::classify(&message)?;
        let size = message.compute_size() as usize;
        if size > frame::payload_limit(lane, &state.options) {
            return Err(TransportError::Limit);
        }
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        let route = registry
            .groups
            .get(scope)
            .ok_or(TransportError::UnknownGroup)?
            .clone();
        let local = state
            .runtime
            .log()
            .group(scope)
            .map_err(|_| TransportError::UnknownGroup)?
            .identity()
            .clone();
        if message.from != local.member.get() {
            return Err(TransportError::Identity);
        }
        let peer = route
            .peers
            .iter()
            .find(|p| p.replica.member.get() == message.to)
            .ok_or(TransportError::Identity)?
            .clone();
        if peer.replica.node == local.node {
            return Err(TransportError::Identity);
        }
        let pool = registry
            .pools
            .get(&peer.replica.node)
            .ok_or(TransportError::StalePeer)?
            .clone();
        drop(registry);
        let charge = size + state.options.max_header_bytes + frame::PREFIX + frame::TAG;
        let node_permit = state
            .outgoing
            .lane(lane)
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let peer_permit = pool
            .outgoing
            .lane(lane)
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        // Encoding and hashing bulk data must not hold the shared registry
        // mutex or allocate decoded Entry copies on the sender.
        let body = message
            .write_to_bytes()
            .map_err(|_| TransportError::Protocol)?;
        let wire_route = Route::new(
            &local,
            state.auth.source(),
            &peer.replica,
            &peer.label,
            route.schema_digest,
        )?;
        frame::scan_message(&body, state.options.max_entries)?;
        let mut header = RequestHeader::new(wire_route, 1, lane, &body);
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if !registry
            .groups
            .get(scope)
            .is_some_and(|r| Arc::ptr_eq(r, &route))
            || !registry
                .pools
                .get(&peer.replica.node)
                .is_some_and(|p| Arc::ptr_eq(p, &pool))
        {
            return Err(TransportError::StalePeer);
        }
        // Assign and enqueue under one short lock so concurrent senders cannot
        // invert the authenticated sequence on a shared connection.
        let sequence = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            + 1;
        header.set_sequence(sequence);
        let (result, wait) = oneshot::channel();
        let request = SendRequest {
            scope: scope.clone(),
            route,
            peer,
            header,
            body,
            snapshot: message.get_msg_type() == MessageType::MsgSnapshot,
            result: Some(result),
            _node: node_permit,
            _peer: peer_permit,
        };
        pool.tx[lane_index(lane)]
            .try_send(request)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TransportError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
            })?;
        Ok(Delivery { result: wait })
    }

    pub fn usage(&self) -> TransportUsage {
        self.handle.0.usage()
    }

    pub fn peer_usage(&self, node: NodeId) -> Result<TransportUsage, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        let pool = registry.pools.get(&node).ok_or(TransportError::StalePeer)?;
        let (cb, ci) = pool.outgoing.control.usage();
        let (db, di) = pool.outgoing.data.usage();
        let (rcb, rci) = pool.incoming.control.usage();
        let (rdb, rdi) = pool.incoming.data.usage();
        let (qb, qi) = pool.outgoing.query.usage();
        let (rqb, rqi) = pool.incoming.query.usage();
        Ok(TransportUsage {
            control_bytes: cb + rcb,
            data_bytes: db + rdb,
            control_items: ci + rci,
            data_items: di + rdi,
            query_bytes: qb + rqb,
            query_items: qi + rqi,
            ..Default::default()
        })
    }

    /// Release sockets, queued permits and worker resources. Domain shutdown
    /// additionally joins the owning execution-domain task registry.
    pub async fn close(&self) {
        let state = &self.handle.0;
        state.stop();
        loop {
            let done = state.done.notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if state.lifecycle_registry().tasks == 0 {
                return;
            }
            done.await;
        }
    }
}

fn lane_index(lane: Lane) -> usize {
    match lane {
        Lane::Control => 0,
        Lane::Data => 1,
    }
}

impl State {
    /// Reserve every worker before mutating a view. Callers perform all other
    /// fallible validation first, release the registry lock, then start workers.
    fn prepare_pools(
        self: &Arc<Self>,
        registry: &mut Registry,
        peers: &[RaftPeerNode],
    ) -> Result<Vec<PoolStart>, TransportError> {
        let new_nodes = peers
            .iter()
            .filter(|peer| !registry.pools.contains_key(&peer.node))
            .count();
        let replacing = peers
            .iter()
            .filter(|peer| {
                registry
                    .pools
                    .get(&peer.node)
                    .is_none_or(|pool| pool.peer != **peer)
            })
            .count();
        if registry.pools.len() + new_nodes > self.options.max_peers
            || registry.tasks + 3 * replacing > self.task_limit()
        {
            return Err(TransportError::Overloaded);
        }
        let mut starts = Vec::new();
        for peer in peers {
            if registry
                .pools
                .get(&peer.node)
                .is_some_and(|pool| pool.peer == *peer)
            {
                continue;
            }
            if let Some(old) = registry.pools.remove(&peer.node) {
                old.stop.send_replace(true);
            }
            let (control, crx) = mpsc::channel(self.options.queue_items);
            let (data, drx) = mpsc::channel(self.options.queue_items);
            let (query, qrx) = mpsc::channel(self.options.queue_items);
            let (stop, _) = watch::channel(false);
            let pool = Arc::new(Pool {
                peer: peer.clone(),
                tx: [control, data],
                query,
                stop,
                outgoing: Banks::new(
                    self.options.peer_control_bytes,
                    self.options.peer_data_bytes,
                    self.options.peer_query_bytes,
                    self.options.queue_items,
                ),
                incoming: Banks::new(
                    self.options.peer_control_bytes,
                    self.options.peer_data_bytes,
                    self.options.peer_query_bytes,
                    self.options.max_connections_per_lane,
                ),
            });
            registry.tasks += 3;
            starts.push(PoolStart {
                pool: pool.clone(),
                control: crx,
                data: drx,
                query: qrx,
                guards: [
                    TaskGuard(self.clone()),
                    TaskGuard(self.clone()),
                    TaskGuard(self.clone()),
                ],
            });
            registry.pools.insert(peer.node, pool);
        }
        Ok(starts)
    }

    fn start_pools(self: &Arc<Self>, starts: Vec<PoolStart>) -> Result<(), TransportError> {
        for start in starts {
            let PoolStart {
                pool,
                control,
                data,
                query,
                guards: [cg, dg, qg],
            } = start;
            for (lane, rx, guard) in [(Lane::Control, control, cg), (Lane::Data, data, dg)] {
                let worker = self.clone();
                let pool = pool.clone();
                self.spawn_reserved(guard, async move {
                    worker.send_loop(pool, lane, rx).await;
                })?;
            }
            let worker = self.clone();
            self.spawn_reserved(qg, async move {
                worker.send_queries(pool, query).await;
            })?;
        }
        Ok(())
    }

    /// Poison closes ordinary admission. Cleanup still reads the real owner
    /// count; a poisoned lock must never stand in for successful resource join.
    fn lifecycle_registry(&self) -> std::sync::MutexGuard<'_, Registry> {
        match self.registry.lock() {
            Ok(registry) => registry,
            Err(poisoned) => {
                let mut registry = poisoned.into_inner();
                registry.closed = true;
                self.stop.send_replace(true);
                registry
            }
        }
    }

    fn admit(&self, registry: &Registry) -> Result<(), TransportError> {
        if registry.closed || self.domain.mono_ms().is_err() || self.runtime.admit().is_err() {
            Err(TransportError::Closed)
        } else {
            Ok(())
        }
    }

    fn task_limit(&self) -> usize {
        3 + 6 * self.options.max_peers + 3 * self.options.max_connections_per_lane
    }

    fn stop(&self) {
        let finished = {
            let mut registry = self.lifecycle_registry();
            registry.closed = true;
            registry.tasks == 0
        };
        self.stop.send_replace(true);
        if let Ok(mut gateway) = self.query_gateway.lock() {
            gateway.take();
        }
        if finished {
            if let Ok(mut claim) = self.claim.lock() {
                claim.take();
            }
        }
    }

    fn spawn(
        self: &Arc<Self>,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), TransportError> {
        let mut registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        if registry.tasks >= self.task_limit() {
            return Err(TransportError::Overloaded);
        }
        registry.tasks += 1;
        drop(registry);
        self.spawn_reserved(TaskGuard(self.clone()), future)
    }

    fn spawn_reserved(
        self: &Arc<Self>,
        guard: TaskGuard,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), TransportError> {
        self.domain
            .spawn_detached(
                "raft-transport",
                OwnedTask {
                    future: Box::pin(future),
                    _guard: guard,
                },
            )
            .map_err(|_| TransportError::Closed)?
            .detach();
        Ok(())
    }

    fn usage(&self) -> TransportUsage {
        let (cb, ci) = self.outgoing.control.usage();
        let (db, di) = self.outgoing.data.usage();
        let (rcb, rci) = self.incoming.control.usage();
        let (rdb, rdi) = self.incoming.data.usage();
        let (qb, qi) = self.outgoing.query.usage();
        let (rqb, rqi) = self.incoming.query.usage();
        let registry = self.lifecycle_registry();
        TransportUsage {
            control_bytes: cb + rcb,
            data_bytes: db + rdb,
            control_items: ci + rci,
            data_items: di + rdi,
            query_bytes: qb + rqb,
            query_items: qi + rqi,
            tasks: registry.tasks,
            peers: registry.pools.len(),
            groups: registry.groups.len(),
            accepted: self.accepted.load(Ordering::Acquire),
            refused: self.refused.load(Ordering::Acquire),
            failed: self.failed.load(Ordering::Acquire),
        }
    }

    fn current(&self, request: &SendRequest) -> bool {
        self.current_route(&request.scope, &request.route, &request.peer)
    }

    fn current_route(&self, scope: &GroupScope, route: &Arc<GroupRoute>, peer: &RaftPeer) -> bool {
        self.registry.lock().is_ok_and(|r| {
            self.admit(&r).is_ok()
                && r.groups.get(scope).is_some_and(|g| Arc::ptr_eq(g, route))
                && r.pools
                    .get(&peer.replica.node)
                    .is_some_and(|p| p.peer.same_host(peer))
                && self.runtime.log().group(scope).is_ok_and(|group| {
                    group.read().is_ok_and(|g| {
                        let conf = &g.conf_state;
                        if !g.membership.permits(group.identity())
                            || !g.membership.permits(&peer.replica)
                        {
                            return false;
                        }
                        [group.identity().member.get(), peer.replica.member.get()]
                            .into_iter()
                            .all(|member| {
                                conf.voters
                                    .iter()
                                    .chain(&conf.voters_outgoing)
                                    .chain(&conf.learners)
                                    .chain(&conf.learners_next)
                                    .any(|id| *id == member)
                            })
                    })
                })
        })
    }

    fn authorize(&self, header: &RequestHeader) -> Result<Arc<Pool>, TransportError> {
        self.authorize_route(&header.route)
    }

    fn authorize_route(&self, wire: &Route) -> Result<Arc<Pool>, TransportError> {
        let (source, target) = wire.decode()?;
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let route = registry
            .groups
            .get(&target.scope)
            .ok_or(TransportError::UnknownGroup)?;
        let group = self
            .runtime
            .log()
            .group(&target.scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        if *group.identity() != target
            || wire.target_label() != self.auth.source()
            || wire.schema() != route.schema_digest
        {
            return Err(TransportError::StalePeer);
        }
        let peer = route
            .peers
            .iter()
            .find(|p| p.replica == source && p.label == wire.source_label())
            .ok_or(TransportError::StalePeer)?;
        let cache = group
            .read()
            .map_err(|e| TransportError::Runtime(e.into()))?;
        if !cache.membership.permits(&source) || !cache.membership.permits(&target) {
            return Err(TransportError::StalePeer);
        }
        let conf = &cache.conf_state;
        for member in [source.member.get(), target.member.get()] {
            if !conf
                .voters
                .iter()
                .chain(&conf.voters_outgoing)
                .chain(&conf.learners)
                .chain(&conf.learners_next)
                .any(|id| *id == member)
            {
                return Err(TransportError::StalePeer);
            }
        }
        registry
            .pools
            .get(&source.node)
            .filter(|p| p.peer.same_host(peer))
            .cloned()
            .ok_or(TransportError::StalePeer)
    }

    async fn bounded<T>(
        &self,
        pool: Option<&Pool>,
        future: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.map(|p| p.stop.subscribe());
        if *stop.borrow() || peer_stop.as_ref().is_some_and(|s| *s.borrow()) {
            return Err(TransportError::Closed);
        }
        let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        let mut deadline = self
            .domain
            .sleep_until(now.saturating_add(self.options.io_timeout_ms))
            .map_err(|_| TransportError::Closed)?;
        crate::asyncrt::select! {
            _ = stop.changed() => Err(TransportError::Closed),
            _ = async { match peer_stop.as_mut() { Some(s) => { let _ = s.changed().await; }, None => std::future::pending::<()>().await } } => Err(TransportError::StalePeer),
            _ = &mut deadline => Err(TransportError::Timeout),
            result = future => result,
        }
    }

    async fn listen(self: Arc<Self>, listener: TcpListener, lane: Lane) {
        let slots = Arc::new(Semaphore::new(self.options.max_connections_per_lane));
        let mut stop = self.stop.subscribe();
        loop {
            if *stop.borrow() {
                return;
            }
            let accepted = crate::asyncrt::select! {
                _ = stop.changed() => return,
                accepted = listener.accept() => accepted,
            };
            let Ok((stream, _)) = accepted else {
                return;
            };
            let Ok(slot) = slots.clone().try_acquire_owned() else {
                drop(stream);
                continue;
            };
            let state = self.clone();
            match self.spawn(async move {
                let _slot = slot;
                let _ = state.clone().receive_connection(stream, lane).await;
            }) {
                Ok(()) | Err(TransportError::Overloaded) => (),
                Err(_) => return,
            }
        }
    }

    async fn receive_connection(
        self: Arc<Self>,
        stream: TcpStream,
        lane: Lane,
    ) -> Result<(), TransportError> {
        stream.set_nodelay(true)?;
        let acceptor = tokio_rustls::TlsAcceptor::from(self.tls.server.clone());
        let mut stream = self
            .bounded(None, async {
                acceptor
                    .accept(stream)
                    .await
                    .map_err(|e| TransportError::Tls(e.to_string()))
            })
            .await?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let mut sequence = 0;
        let mut source_host = None;
        loop {
            let mut prefix = [0; frame::PREFIX];
            self.bounded(None, async {
                stream.read_exact(&mut prefix).await?;
                Ok(())
            })
            .await?;
            if &prefix[..4] == seeds::REQUEST_MAGIC {
                if lane != Lane::Data || sequence != 0 {
                    return Err(TransportError::Protocol);
                }
                let exporter = stream
                    .get_ref()
                    .1
                    .export_keying_material([0; 32], seeds::EXPORT_LABEL, None)
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                return self.receive_seed(&mut stream, &exporter, prefix).await;
            }
            if &prefix[..4] == snapshots::REQUEST_MAGIC {
                if lane != Lane::Data || sequence != 0 {
                    return Err(TransportError::Protocol);
                }
                return self.receive_snapshot(&mut stream, &exporter, prefix).await;
            }
            let head = self
                .bounded(
                    None,
                    read_head_after_prefix(&mut stream, &self.options, lane, false, prefix),
                )
                .await?;
            let header = head.request(&self.auth, &exporter)?;
            if header.lane != lane || header.sequence()? <= sequence {
                return Err(TransportError::Replay);
            }
            let (source, _) = header.route.decode()?;
            let host = (
                source.node,
                source.disk,
                source.process,
                header.route.source_label().to_owned(),
            );
            if source_host.as_ref().is_some_and(|h| *h != host) {
                return Err(TransportError::StalePeer);
            }
            source_host = Some(host);
            sequence = header.sequence()?;
            let pool = match self.authorize(&header) {
                Ok(pool) => pool,
                Err(error) => {
                    self.refused.fetch_add(1, Ordering::Relaxed);
                    let reply = frame::response_head(
                        &self.auth,
                        &exporter,
                        &header,
                        &head.tag,
                        disposition(&error),
                        &self.options,
                    )?;
                    let _ = self.bounded(None, write_head(&mut stream, &reply)).await;
                    // The body remains unread. Close rather than resynchronizing
                    // on attacker-chosen bytes or creating an unknown group.
                    return Err(error);
                }
            };
            let charge = head.payload_len + head.header.len() + frame::PREFIX + frame::TAG;
            let _node = self
                .incoming
                .lane(lane)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?;
            let _peer = pool
                .incoming
                .lane(lane)
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?;
            let mut body = vec![0; head.payload_len];
            self.bounded(Some(&pool), async {
                stream.read_exact(&mut body).await?;
                Ok(())
            })
            .await?;
            let message = header.verify_body(&body, &self.options)?;
            // Routing/configuration may have changed while reading the body.
            self.authorize(&header)?;
            let scope = source.scope.clone();
            let result = self
                .bounded(Some(&pool), async {
                    self.runtime
                        .admit_step(&scope, message)
                        .await
                        .map_err(TransportError::from)
                })
                .await;
            let accepted = result.is_ok();
            let status = result
                .as_ref()
                .err()
                .map_or(Disposition::Accepted, disposition);
            let reply = frame::response_head(
                &self.auth,
                &exporter,
                &header,
                &head.tag,
                status,
                &self.options,
            )?;
            self.bounded(Some(&pool), write_head(&mut stream, &reply))
                .await?;
            if accepted {
                self.accepted.fetch_add(1, Ordering::Relaxed);
            } else {
                self.refused.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn send_loop(
        self: Arc<Self>,
        pool: Arc<Pool>,
        lane: Lane,
        mut requests: mpsc::Receiver<SendRequest>,
    ) {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        let mut connection = None;
        loop {
            if *stop.borrow() || *peer_stop.borrow() {
                return;
            }
            let mut request = crate::asyncrt::select! {
                _ = stop.changed() => return,
                _ = peer_stop.changed() => return,
                request = requests.recv() => match request { Some(request) => request, None => return },
            };
            let result = self
                .bounded(Some(&pool), async {
                    if !self.current(&request) {
                        return Err(TransportError::StalePeer);
                    }
                    if connection.is_none() {
                        let tcp = TcpStream::connect(pool.peer.address(lane)).await?;
                        tcp.set_nodelay(true)?;
                        let name =
                            rustls::pki_types::ServerName::try_from(pool.peer.server_name.clone())
                                .map_err(|_| TransportError::Identity)?;
                        let tls = tokio_rustls::TlsConnector::from(self.tls.client.clone())
                            .connect(name, tcp)
                            .await
                            .map_err(|e| TransportError::Tls(e.to_string()))?;
                        let exporter = tls
                            .get_ref()
                            .1
                            .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
                            .map_err(|e| TransportError::Tls(e.to_string()))?;
                        connection = Some((tls, exporter));
                    }
                    let (stream, exporter) = connection.as_mut().ok_or(TransportError::Closed)?;
                    let head = frame::request_head(
                        &self.auth,
                        exporter,
                        &request.header,
                        request.body.len(),
                        &self.options,
                    )?;
                    write_head(stream, &head).await?;
                    stream.write_all(&request.body).await?;
                    stream.flush().await?;
                    let response = read_head(stream, &self.options, lane, true).await?;
                    match response.response(&self.auth, exporter, &request.header, &head.tag)? {
                        Disposition::Accepted => (),
                        Disposition::UnknownGroup => return Err(TransportError::UnknownGroup),
                        Disposition::StalePeer => return Err(TransportError::StalePeer),
                        Disposition::Overloaded => return Err(TransportError::Overloaded),
                        Disposition::Refused => return Err(TransportError::Refused),
                    }
                    if !self.current(&request) {
                        return Err(TransportError::StalePeer);
                    }
                    Ok(())
                })
                .await;
            if result.is_err() {
                connection = None;
                self.failed.fetch_add(1, Ordering::Relaxed);
                if self.current(&request) {
                    // These are failure hints only. A timeout never constructs
                    // MsgAppendResponse or advances a durable/committed index.
                    let _ = self
                        .bounded(Some(&pool), async {
                            self.runtime
                                .admit_unreachable(
                                    &request.scope,
                                    request.peer.replica.member.get(),
                                )
                                .await?;
                            if request.snapshot {
                                self.runtime
                                    .admit_snapshot_status(
                                        &request.scope,
                                        request.peer.replica.member.get(),
                                        false,
                                    )
                                    .await?;
                            }
                            Ok(())
                        })
                        .await;
                }
            }
            if result.is_ok() && request.snapshot && self.current(&request) {
                // raft-rs defines Finish as the transport status of the sent
                // snapshot. Actual installation still needs the receiver's
                // persisted Raft response; this hint never proves installation.
                let _ = self
                    .bounded(Some(&pool), async {
                        self.runtime
                            .admit_snapshot_status(
                                &request.scope,
                                request.peer.replica.member.get(),
                                true,
                            )
                            .await?;
                        Ok(())
                    })
                    .await;
            }
            // Completion observers may immediately admit another frame. Drop
            // the body and both permits before waking that observer.
            let reply = request.result.take();
            drop(request);
            if let Some(reply) = reply {
                let _ = reply.send(result);
            }
        }
    }
}

fn disposition(error: &TransportError) -> Disposition {
    match error {
        TransportError::UnknownGroup | TransportError::Runtime(RuntimeError::NotFound) => {
            Disposition::UnknownGroup
        }
        TransportError::StalePeer | TransportError::Identity => Disposition::StalePeer,
        TransportError::Overloaded | TransportError::Runtime(RuntimeError::Overloaded) => {
            Disposition::Overloaded
        }
        _ => Disposition::Refused,
    }
}

// Header memory is bounded by the independently limited connection slots. Body
// memory is acquired separately only after header authentication/authorization.
async fn read_head<S: AsyncRead + Unpin>(
    stream: &mut S,
    options: &TransportOptions,
    lane: Lane,
    response: bool,
) -> Result<FrameHead, TransportError> {
    let mut prefix = [0; frame::PREFIX];
    stream.read_exact(&mut prefix).await?;
    read_head_after_prefix(stream, options, lane, response, prefix).await
}

async fn read_head_after_prefix<S: AsyncRead + Unpin>(
    stream: &mut S,
    options: &TransportOptions,
    lane: Lane,
    response: bool,
    prefix: [u8; frame::PREFIX],
) -> Result<FrameHead, TransportError> {
    let (header_len, payload_len) = frame::parse_prefix(&prefix, options, lane, response)?;
    let mut header = vec![0; header_len];
    stream.read_exact(&mut header).await?;
    let mut tag = [0; frame::TAG];
    stream.read_exact(&mut tag).await?;
    Ok(FrameHead {
        prefix,
        header,
        tag,
        payload_len,
    })
}

async fn write_head<S: AsyncWrite + Unpin>(
    stream: &mut S,
    head: &FrameHead,
) -> Result<(), TransportError> {
    stream.write_all(&head.prefix).await?;
    stream.write_all(&head.header).await?;
    stream.write_all(&head.tag).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "transport_tests.rs"]
mod tests;
