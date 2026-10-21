//! A separately owned query connection per peer. Waiting for a read cannot
//! occupy the control or LTX connection required to establish its barrier.

use super::*;
use crate::application::consensus::{client, CommandFailure, QueryResult, RaftCells, VersionKeys};
use crate::raft::query_identity::QueryRoute;
use crate::raft::query_wire::{self as wire, QueryCall};
use cellar_logic::consensus::{MemberId, NodeIdentity};
use sha2::{Digest, Sha256};

pub(crate) struct QueryResponse {
    pub value: Result<QueryResult, CommandFailure>,
    _node: Permit,
    _peer: Permit,
}

pub(crate) struct QueryDelivery {
    result: Option<oneshot::Receiver<Result<QueryResponse, TransportError>>>,
    cancel: watch::Sender<bool>,
}

impl QueryDelivery {
    pub async fn receive(mut self) -> Result<QueryResponse, TransportError> {
        self.result
            .take()
            .ok_or(TransportError::Closed)?
            .await
            .map_err(|_| TransportError::Closed)?
    }
}

impl Drop for QueryDelivery {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

pub(super) struct SendQuery {
    scope: GroupScope,
    route: Arc<GroupRoute>,
    peer: RaftPeer,
    call: QueryCall,
    sequence: u64,
    deadline: u64,
    keys: VersionKeys,
    cancel: watch::Receiver<bool>,
    reply: oneshot::Sender<Result<QueryResponse, TransportError>>,
    node: Permit,
    peer_bytes: Permit,
}

pub(super) struct OriginGrant {
    pub(super) revision: u64,
    pub(super) peer: Option<RaftPeerNode>,
    pub(super) stop: watch::Sender<bool>,
}

impl RaftTransport {
    /// Install trusted namespace read permission. It cannot create a group or
    /// authorize a consensus message. Revisions include revocation tombstones.
    pub fn grant_query_origin(&self, grant: NamespaceQueryOrigin) -> Result<(), TransportError> {
        let state = &self.handle.0;
        validate_peer(&grant.peer)?;
        if grant.revision == 0 || grant.peer.node == state.runtime.log().limits().node {
            return Err(TransportError::Identity);
        }
        let key = (grant.cluster, grant.namespace, grant.peer.node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.check_node_view(&grant.peer)?;
        if let Some(old) = registry.origins.get(&key) {
            if old.revision == grant.revision && old.peer.as_ref() == Some(&grant.peer) {
                return Ok(());
            }
            if old.revision >= grant.revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.origins.len() >= state.options.max_query_origins {
            return Err(TransportError::Overloaded);
        }
        if registry
            .groups
            .values()
            .chain(registry.query_groups.values())
            .flat_map(|route| &route.peers)
            .any(|peer| peer.replica.node == grant.peer.node && !grant.peer.same_host(peer))
            || registry
                .origins
                .iter()
                .filter(|(other, _)| **other != key)
                .filter_map(|(_, old)| old.peer.as_ref())
                .any(|peer| peer.node == grant.peer.node && *peer != grant.peer)
        {
            return Err(TransportError::StalePeer);
        }
        let starts = state.prepare_pools(&mut registry, std::slice::from_ref(&grant.peer))?;
        let (stop, _) = watch::channel(false);
        if let Some(old) = registry.origins.insert(
            key,
            Arc::new(OriginGrant {
                revision: grant.revision,
                peer: Some(grant.peer),
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)
    }

    pub fn revoke_query_origin(
        &self,
        cluster: ClusterId,
        namespace: NamespaceId,
        node: NodeId,
        revision: u64,
    ) -> Result<(), TransportError> {
        if revision == 0 {
            return Err(TransportError::Identity);
        }
        let state = &self.handle.0;
        let key = (cluster, namespace, node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if let Some(old) = registry.origins.get(&key) {
            if old.revision == revision && old.peer.is_none() {
                return Ok(());
            }
            if old.revision >= revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.origins.len() >= state.options.max_query_origins {
            return Err(TransportError::Overloaded);
        }
        let (stop, _) = watch::channel(true);
        if let Some(old) = registry.origins.insert(
            key,
            Arc::new(OriginGrant {
                revision,
                peer: None,
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        registry.prune_pools();
        Ok(())
    }

    /// Discovery for a query-only origin. No local group or member is created.
    /// Every destination still checks its own live membership and read proof.
    pub fn install_query_group(&self, route: GroupRoute) -> Result<(), TransportError> {
        let state = &self.handle.0;
        route
            .scope
            .validate()
            .map_err(|_| TransportError::Identity)?;
        if route.revision == 0
            || route.schema_digest == [0; 32]
            || route.peers.is_empty()
            || route.peers.len() > 128
        {
            return Err(TransportError::Identity);
        }
        if !matches!(
            state.runtime.log().group(&route.scope),
            Err(crate::raft::storage::LogError::NotFound)
        ) {
            return Err(TransportError::Identity);
        }
        let local = state.runtime.log().limits().node;
        let mut members = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        let physical: Vec<_> = route.peers.iter().map(RaftPeerNode::from).collect();
        for (peer, physical) in route.peers.iter().zip(&physical) {
            validate_peer(physical)?;
            if peer.replica.scope != route.scope
                || peer.replica.node == local
                || !members.insert(peer.replica.member)
                || !nodes.insert(peer.replica.node)
            {
                return Err(TransportError::Identity);
            }
        }
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        for peer in &physical {
            registry.check_node_view(peer)?;
        }
        if let Some(old) = registry.query_groups.get(&route.scope) {
            if **old == route {
                return Ok(());
            }
            if old.revision >= route.revision || old.schema_digest != route.schema_digest {
                return Err(TransportError::StalePeer);
            }
        } else if registry.query_groups.len() + registry.groups.len() >= state.options.max_groups {
            return Err(TransportError::Overloaded);
        }
        for peer in &physical {
            if registry
                .groups
                .values()
                .chain(
                    registry
                        .query_groups
                        .iter()
                        .filter(|(scope, _)| **scope != route.scope)
                        .map(|(_, value)| value),
                )
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
        registry
            .query_groups
            .insert(route.scope.clone(), Arc::new(route));
        registry.prune_pools();
        drop(registry);
        state.start_pools(starts)
    }

    pub(crate) fn install_query_gateway(
        &self,
        handle: Arc<client::Handle>,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        let mut gateway = state
            .query_gateway
            .lock()
            .map_err(|_| TransportError::Closed)?;
        if gateway.is_some()
            || handle.closed.load(Ordering::Acquire)
            || !handle.domain.same_owner(&state.domain)
        {
            return Err(TransportError::Refused);
        }
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        *gateway = Some(Arc::downgrade(&handle));
        Ok(())
    }

    pub(in crate::raft) fn query(
        &self,
        scope: &GroupScope,
        member: MemberId,
        call: QueryCall,
        keys: VersionKeys,
        deadline: u64,
    ) -> Result<QueryDelivery, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if state.domain.mono_ms().map_err(|_| TransportError::Closed)? >= deadline {
            return Err(TransportError::Timeout);
        }
        let route = registry
            .groups
            .get(scope)
            .or_else(|| registry.query_groups.get(scope))
            .ok_or(TransportError::UnknownGroup)?
            .clone();
        let local_node = state.runtime.log().limits().node;
        let peer = route
            .peers
            .iter()
            .find(|p| p.replica.member == member && p.replica.node != local_node)
            .ok_or(TransportError::Identity)?
            .clone();
        let pool = registry
            .pools
            .get(&peer.replica.node)
            .ok_or(TransportError::StalePeer)?
            .clone();
        drop(registry);
        let charge = reservation(
            call.input
                .len()
                .checked_add(4096)
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
        // Validate/size the bounded representation before queueing, but retain
        // only captured inputs. The sender encodes again with remaining time.
        drop(call.encode(state.options.max_query_request_bytes)?);
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        if !registry
            .groups
            .get(scope)
            .or_else(|| registry.query_groups.get(scope))
            .is_some_and(|r| Arc::ptr_eq(r, &route))
            || !registry
                .pools
                .get(&peer.replica.node)
                .is_some_and(|p| Arc::ptr_eq(p, &pool))
        {
            return Err(TransportError::StalePeer);
        }
        let sequence = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            + 1;
        let (reply, result) = oneshot::channel();
        let (cancel, cancelled) = watch::channel(false);
        pool.query
            .try_send(SendQuery {
                scope: scope.clone(),
                route,
                peer,
                call,
                sequence,
                deadline,
                keys,
                cancel: cancelled,
                reply,
                node,
                peer_bytes,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TransportError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
            })?;
        Ok(QueryDelivery {
            result: Some(result),
            cancel,
        })
    }
}

fn reservation(request: usize, options: &TransportOptions) -> Result<usize, TransportError> {
    request
        .checked_mul(2)
        .and_then(|n| {
            n.checked_add(
                2 * options.max_query_response_bytes
                    + 2 * (options.max_header_bytes + frame::PREFIX + frame::TAG),
            )
        })
        .ok_or(TransportError::Limit)
}

fn validate_peer(peer: &RaftPeerNode) -> Result<(), TransportError> {
    if crate::machine::validate_node_name(&peer.label).is_err()
        || rustls::pki_types::ServerName::try_from(peer.server_name.clone()).is_err()
        || peer.control_address == peer.data_address
        || peer.query_address == peer.control_address
        || peer.query_address == peer.data_address
    {
        return Err(TransportError::Identity);
    }
    Ok(())
}

impl State {
    /// Permission is checked independently of discovery and voting. Consensus
    /// messages keep their separate member-bearing route validator.
    pub(super) fn authorize_query_route(
        &self,
        wire: &QueryRoute,
    ) -> Result<(Arc<Pool>, Option<watch::Receiver<bool>>), TransportError> {
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
        let member_peer = route.peers.iter().find(|peer| {
            peer.replica.node_identity() == source && peer.label == wire.source_label()
        });
        let cache = group
            .read()
            .map_err(|error| TransportError::Runtime(error.into()))?;
        if !cache.membership.permits(&target) {
            return Err(TransportError::StalePeer);
        }
        let conf = &cache.conf_state;
        let present = |member| {
            conf.voters
                .iter()
                .chain(&conf.voters_outgoing)
                .chain(&conf.learners)
                .chain(&conf.learners_next)
                .any(|id| *id == member)
        };
        if !present(target.member.get()) {
            return Err(TransportError::StalePeer);
        }
        let implicit = member_peer.filter(|peer| {
            present(peer.replica.member.get()) && cache.membership.permits(&peer.replica)
        });
        let grant = registry
            .origins
            .get(&(target.scope.cluster, target.scope.namespace, source.node))
            .filter(|grant| !*grant.stop.borrow())
            .filter(|grant| {
                grant.peer.as_ref().is_some_and(|peer| {
                    peer.node == source.node
                        && peer.disk == source.disk
                        && peer.process == source.process
                        && peer.label == wire.source_label()
                })
            });
        let permission = if implicit.is_some() {
            None
        } else {
            grant.map(|grant| grant.stop.subscribe())
        };
        let pool = registry
            .pools
            .get(&source.node)
            .filter(|pool| {
                implicit.is_some_and(|peer| pool.peer.same_host(peer))
                    || grant
                        .and_then(|grant| grant.peer.as_ref())
                        .is_some_and(|peer| pool.peer == *peer)
            })
            .cloned()
            .ok_or(TransportError::Refused)?;
        Ok((pool, permission))
    }

    fn current_query_route(
        &self,
        scope: &GroupScope,
        route: &Arc<GroupRoute>,
        peer: &RaftPeer,
    ) -> bool {
        if self.runtime.log().group(scope).is_ok() {
            return self.current_route(scope, route, peer);
        }
        self.registry.lock().is_ok_and(|registry| {
            self.admit(&registry).is_ok()
                && registry
                    .query_groups
                    .get(scope)
                    .is_some_and(|view| Arc::ptr_eq(view, route))
                && registry
                    .pools
                    .get(&peer.replica.node)
                    .is_some_and(|pool| pool.peer.same_host(peer))
        })
    }

    pub(super) async fn query_until<T>(
        &self,
        pool: &Pool,
        deadline: u64,
        mut cancel: watch::Receiver<bool>,
        mut permission: Option<watch::Receiver<bool>>,
        future: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        if *stop.borrow() || *peer_stop.borrow() || *cancel.borrow() {
            return Err(TransportError::Closed);
        }
        if permission
            .as_ref()
            .is_some_and(|permission| *permission.borrow())
        {
            return Err(TransportError::Refused);
        }
        let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        let until = deadline.min(now.saturating_add(self.options.io_timeout_ms));
        if now >= until {
            return Err(TransportError::Timeout);
        }
        let timer = self
            .domain
            .sleep_until(until)
            .map_err(|_| TransportError::Closed)?;
        crate::asyncrt::select_biased! { "revocation and cancellation take precedence over a ready query result";
            _ = stop.changed() => Err(TransportError::Closed),
            _ = peer_stop.changed() => Err(TransportError::StalePeer),
            _ = cancel.changed() => Err(TransportError::Closed),
            _ = async { match permission.as_mut() { Some(permission) => { let _ = permission.changed().await; }, None => std::future::pending::<()>().await } } => Err(TransportError::Refused),
            _ = timer => Err(TransportError::Timeout),
            value = future => value,
        }
    }

    pub(super) async fn listen_queries(self: Arc<Self>, listener: TcpListener) {
        let slots = Arc::new(Semaphore::new(self.options.max_connections_per_lane));
        let mut stop = self.stop.subscribe();
        loop {
            if *stop.borrow() {
                return;
            }
            let accepted = crate::asyncrt::select! { _ = stop.changed() => return, value = listener.accept() => value };
            let Ok((stream, _)) = accepted else {
                return;
            };
            let Ok(slot) = slots.clone().try_acquire_owned() else {
                continue;
            };
            let state = self.clone();
            match self.spawn(async move {
                let _slot = slot;
                let _ = state.receive_queries(stream).await;
            }) {
                Ok(()) | Err(TransportError::Overloaded) => (),
                Err(_) => return,
            }
        }
    }

    async fn receive_queries(self: Arc<Self>, tcp: TcpStream) -> Result<(), TransportError> {
        tcp.set_nodelay(true)?;
        let mut stream = self
            .bounded(None, async {
                tokio_rustls::TlsAcceptor::from(self.tls.server.clone())
                    .accept(tcp)
                    .await
                    .map_err(|e| TransportError::Tls(e.to_string()))
            })
            .await?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], wire::EXPORT_LABEL, None)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let mut last_sequence = 0;
        let mut source_host = None;
        loop {
            let head = self
                .bounded(None, read_query_head(&mut stream, &self.options, false))
                .await?;
            let header = wire::read_request(&head, &self.auth, &exporter, &self.options)?;
            let (source, target) = header.route.decode()?;
            let host = (
                source.node,
                source.disk,
                source.process,
                header.route.source_label().to_owned(),
            );
            if source_host
                .as_ref()
                .is_some_and(|previous| *previous != host)
            {
                return Err(TransportError::StalePeer);
            }
            if header.sequence()? <= last_sequence {
                return Err(TransportError::Replay);
            }
            source_host = Some(host);
            last_sequence = header.sequence()?;
            let (pool, permission) = self.authorize_query_route(&header.route)?;
            let charge = reservation(head.payload_len, &self.options)?;
            let _node = self
                .incoming
                .query
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?;
            let _peer = pool
                .incoming
                .query
                .acquire(charge)
                .ok_or(TransportError::Overloaded)?;
            let mut body = vec![0; head.payload_len];
            let (_cancel, cancel) = watch::channel(false);
            let io_until = self
                .domain
                .mono_ms()
                .map_err(|_| TransportError::Closed)?
                .saturating_add(self.options.io_timeout_ms);
            self.query_until(&pool, io_until, cancel.clone(), permission.clone(), async {
                stream.read_exact(&mut body).await?;
                Ok(())
            })
            .await?;
            header.verify_body(&body)?;
            let call = QueryCall::decode(&body, self.options.max_query_request_bytes)?;
            drop(body);
            self.authorize_query_route(&header.route)?;
            let handle = self
                .query_gateway
                .lock()
                .map_err(|_| TransportError::Closed)?
                .as_ref()
                .and_then(|handle| handle.upgrade())
                .ok_or(TransportError::Refused)?;
            let gateway = RaftCells { handle };
            let cell = gateway
                .cell(target.scope.namespace, &target.scope.cell)
                .map_err(|_| TransportError::Refused)?;
            if cell.scope() != &target.scope
                || cell.binding.class.schema_digest() != header.route.schema()
            {
                return Err(TransportError::StalePeer);
            }
            let timeout = u64::try_from(call.options.timeout.as_millis())
                .map_err(|_| TransportError::Limit)?;
            let until = self
                .domain
                .mono_ms()
                .map_err(|_| TransportError::Closed)?
                .saturating_add(timeout);
            let query = cell.query(call.contract, call.input, call.options);
            let mut extra = [0; 1];
            // Query connections do not pipeline. EOF or unexpected input while
            // waiting cancels the SDK waiter and its bounded SQL work.
            let result = self.query_until(&pool, until, cancel.clone(), permission.clone(), async {
                crate::asyncrt::select! {
                    value = query => Ok(value),
                    read = stream.read(&mut extra) => { let _ = read?; Err(TransportError::Closed) },
                }
            }).await?;
            if result
                .as_ref()
                .is_ok_and(|value| value.served_by != target.member)
            {
                return Err(TransportError::Identity);
            }
            self.authorize_query_route(&header.route)?;
            let body = wire::encode_result(&result, self.options.max_query_response_bytes)?;
            let response = wire::response_head(
                &self.auth,
                &exporter,
                &header,
                &head.tag,
                &body,
                &self.options,
            )?;
            self.query_until(&pool, until, cancel, permission, async {
                write_head(&mut stream, &response).await?;
                stream.write_all(&body).await?;
                stream.flush().await?;
                Ok(())
            })
            .await?;
            self.accepted.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) async fn send_queries(
        self: Arc<Self>,
        pool: Arc<Pool>,
        mut requests: mailbox::Receiver<SendQuery>,
    ) {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        let mut connection = None;
        loop {
            if *stop.borrow() || *peer_stop.borrow() {
                return;
            }
            let mut request = crate::asyncrt::select! { _ = stop.changed() => return, _ = peer_stop.changed() => return, request = requests.recv() => match request { Some(request) => request, None => return } };
            let result = self
                .query_until(
                    &pool,
                    request.deadline,
                    request.cancel.clone(),
                    None,
                    async {
                        if !self.current_query_route(&request.scope, &request.route, &request.peer)
                        {
                            return Err(TransportError::StalePeer);
                        }
                        if connection.is_none() {
                            let tcp = TcpStream::connect(request.peer.query_address).await?;
                            tcp.set_nodelay(true)?;
                            let name = rustls::pki_types::ServerName::try_from(
                                request.peer.server_name.clone(),
                            )
                            .map_err(|_| TransportError::Identity)?;
                            let tls = tokio_rustls::TlsConnector::from(self.tls.client.clone())
                                .connect(name, tcp)
                                .await
                                .map_err(|e| TransportError::Tls(e.to_string()))?;
                            let exporter = tls
                                .get_ref()
                                .1
                                .export_keying_material([0; 32], wire::EXPORT_LABEL, None)
                                .map_err(|e| TransportError::Tls(e.to_string()))?;
                            connection = Some((tls, exporter));
                        }
                        let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
                        let remaining = request
                            .deadline
                            .checked_sub(now)
                            .filter(|ms| *ms > 0)
                            .ok_or(TransportError::Timeout)?;
                        request.call.options.timeout = request
                            .call
                            .options
                            .timeout
                            .min(std::time::Duration::from_millis(remaining));
                        let body = request.call.encode(self.options.max_query_request_bytes)?;
                        let local = self.runtime.log().limits();
                        let route = QueryRoute::new(
                            NodeIdentity {
                                node: local.node,
                                disk: local.disk,
                                process: local.process,
                            },
                            self.auth.source(),
                            &request.peer.replica,
                            &request.peer.label,
                            request.route.schema_digest,
                        )?;
                        let header = wire::RequestHead::new(route, request.sequence, &body);
                        let (stream, exporter) =
                            connection.as_mut().ok_or(TransportError::Closed)?;
                        let head = wire::request_head(
                            &self.auth,
                            exporter,
                            &header,
                            &body,
                            &self.options,
                        )?;
                        write_head(stream, &head).await?;
                        stream.write_all(&body).await?;
                        stream.flush().await?;
                        drop(body);
                        let response = read_query_head(stream, &self.options, true).await?;
                        let digest = wire::read_response(
                            &response,
                            &self.auth,
                            exporter,
                            &header,
                            &head.tag,
                            &self.options,
                        )?;
                        let mut body = vec![0; response.payload_len];
                        stream.read_exact(&mut body).await?;
                        if digest != <[u8; 32]>::from(Sha256::digest(&body)) {
                            return Err(TransportError::Authentication);
                        }
                        if !self.current_query_route(&request.scope, &request.route, &request.peer)
                        {
                            return Err(TransportError::StalePeer);
                        }
                        let now = u64::try_from(crate::asyncrt::wall_ms())
                            .map_err(|_| TransportError::Authentication)?;
                        wire::decode_result(
                            &body,
                            &request.scope,
                            request.route.schema_digest,
                            &request.keys,
                            now,
                            self.options.max_query_response_bytes,
                            request.peer.replica.member,
                        )
                    },
                )
                .await;
            if result.is_err() {
                connection = None;
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
            let SendQuery {
                reply,
                node,
                peer_bytes,
                ..
            } = request;
            let result = result.map(|value| QueryResponse {
                value,
                _node: node,
                _peer: peer_bytes,
            });
            let _ = reply.send(result);
        }
    }
}

async fn read_query_head<S: AsyncRead + Unpin>(
    stream: &mut S,
    options: &TransportOptions,
    response: bool,
) -> Result<FrameHead, TransportError> {
    let mut prefix = [0; frame::PREFIX];
    stream.read_exact(&mut prefix).await?;
    let (header, payload_len) = wire::parse_prefix(&prefix, options, response)?;
    let mut header = vec![0; header];
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
