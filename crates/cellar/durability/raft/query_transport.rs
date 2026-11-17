// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Concurrent query exchanges share each peer's existing sender worker and
//! retain the node's physical socket ceiling and original admission bounds.

use super::*;
use crate::raft::query_identity::QueryRoute;
use crate::raft::query_wire as wire;
use cellar_logic::consensus::NodeIdentity;
use futures_util::stream::{FuturesUnordered, StreamExt};

pub(super) enum SendQuery {
    Shared(Box<super::cell_queries::SendCellQuery>),
    Write(Box<super::cell_commands::SendCellWrite>),
}

enum QueryEvent {
    Stop,
    IdleClosed,
    Request(SendQuery),
    Finished(Option<QueryConnection>),
    Socket(tokio::sync::OwnedSemaphorePermit),
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
        registry.check_physical_endpoint(&grant.peer)?;
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
        if registry
            .inspection_origins
            .values()
            .filter_map(|origin| origin.peer.as_ref())
            .any(|peer| peer.node == grant.peer.node && peer != &grant.peer)
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
            registry.check_physical_endpoint(peer)?;
        }
        if let Some(old) = registry.query_groups.get(&route.scope) {
            if **old == route {
                return Ok(());
            }
            if old.revision >= route.revision || old.schema_digest != route.schema_digest {
                return Err(TransportError::StalePeer);
            }
        } else if registry.route_count() >= state.options.max_groups {
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
}

pub(super) fn reservation(
    request: usize,
    options: &TransportOptions,
) -> Result<usize, TransportError> {
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
        if wire.requires_command_grant() {
            return self.authorize_cell_command(wire);
        }
        self.authorize_cell_query(wire)
    }

    /// Every cell read uses the same read-only origin grant, process binding,
    /// revocation and peer-pool permission path.
    pub(super) fn authorize_query_origin<S>(
        &self,
        registry: &Registry,
        cluster: ClusterId,
        namespace: NamespaceId,
        source: NodeIdentity,
        source_label: &str,
        implicit: Option<&RaftPeer<S>>,
    ) -> Result<(Arc<Pool>, Option<watch::Receiver<bool>>), TransportError> {
        let grant = registry
            .origins
            .get(&(cluster, namespace, source.node))
            .filter(|grant| !*grant.stop.borrow())
            .filter(|grant| {
                grant.peer.as_ref().is_some_and(|peer| {
                    peer.node == source.node
                        && peer.disk == source.disk
                        && peer.process == source.process
                        && peer.label == source_label
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
        // The invocation owns execution time, including provider work. Socket
        // phases have their own I/O bound; applying it here truncates a valid
        // caller budget on both sides of the connection.
        let until = deadline;
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
        let slots = self.query_connections.clone();
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
                let endpoint = state.local_query;
                if let Err(error) = state.receive_queries(stream).await {
                    tracing::debug!(target: "cellar::raft::query_connection", %endpoint, %error, "shared request connection ended");
                }
            }) {
                Ok(()) | Err(TransportError::Overloaded) => (),
                Err(_) => return,
            }
        }
    }

    async fn receive_queries(self: Arc<Self>, tcp: TcpStream) -> Result<(), TransportError> {
        tcp.set_nodelay(true)?;
        let tcp = self.observed_socket(tcp, metrics::Lane::Query);
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
            let source = header.route.source()?;
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
            let (pool, permission) = match self.authorize_query_route(&header.route) {
                Ok(authorization) => authorization,
                Err(TransportError::Refused) if header.route.is_query() => {
                    self.refuse_read(&mut stream, &exporter, &header, &head)
                        .await?;
                    return Err(TransportError::Refused);
                }
                Err(error) => return Err(error),
            };
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
            let call = super::cell_queries::IncomingCall::decode(
                &header.route,
                &body,
                self.options.max_query_request_bytes,
            )?;
            drop(body);
            self.authorize_query_route(&header.route)?;
            let until = self
                .domain
                .mono_ms()
                .map_err(|_| TransportError::Closed)?
                .checked_add(call.timeout_ms())
                .ok_or(TransportError::Limit)?;
            let query = async {
                call.execute(&self, &header.route, until).await.inspect_err(|error| {
                    tracing::debug!(target: "cellar::raft::query_connection", endpoint=%self.local_query, %error, "shared request execution refused");
                })
            };
            let mut extra = [0; 1];
            // No pipelining: EOF/unexpected input cancels the SDK waiter while
            // the SQL owner retains any entered physical work through cleanup.
            let body = self.query_until(&pool, until, cancel.clone(), permission.clone(), async {
                crate::asyncrt::select! {
                    value = query => value,
                    read = stream.read(&mut extra) => { let _ = read?; Err(TransportError::Closed) },
                }
            }).await?;
            self.authorize_query_route(&header.route)?;
            let response = wire::response_head(
                &self.auth,
                &exporter,
                &header,
                &head.tag,
                body.bytes(),
                &self.options,
            )?;
            // This aggregate closes the stream before dropping the output proof
            // on every exit, including destruction of the entire serving task.
            let mut writing = super::query_replies::Write {
                stream,
                reply: body,
            };
            let observation = self
                .performance
                .timer(crate::raft::performance::Operation::QueryReplyWrite);
            let result = self
                .query_until(&pool, until, cancel, permission, async {
                    self.bounded(Some(&pool), async {
                        write_head(&mut writing.stream, &response).await?;
                        writing.stream.write_all(writing.reply.bytes()).await?;
                        writing.stream.flush().await?;
                        Ok(())
                    })
                    .await
                })
                .await;
            if let Err(error) = result {
                drop(writing);
                observation.finish(false);
                return Err(error);
            }
            let super::query_replies::Write {
                stream: resumed,
                reply,
            } = writing;
            stream = resumed;
            drop(reply);
            observation.finish(true);
            self.accepted.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn refuse_read<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        header: &wire::RequestHead,
        head: &FrameHead,
    ) -> Result<(), TransportError> {
        // A metadata-only physical process may lack both a query pool and this
        // namespace's grant. Its existing process owner and the same peer bank
        // bound the denial without creating workers or granting data admission.
        let source = header.route.source()?;
        let (incoming, epoch) = {
            let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
            self.admit(&registry)?;
            let matches = |peer: &RaftPeerNode| {
                peer.disk == source.disk
                    && peer.process == source.process
                    && peer.label == header.route.source_label()
            };
            if let Some(pool) = registry
                .pools
                .get(&source.node)
                .filter(|pool| matches(&pool.peer))
            {
                (pool.incoming.query.clone(), pool.stop.subscribe())
            } else {
                let seed = registry
                    .seed_peers
                    .get(&source.node)
                    .filter(|seed| matches(&seed.peer))
                    .ok_or(TransportError::Refused)?;
                (seed.incoming_query.clone(), seed.stop.subscribe())
            }
        };
        let charge = reservation(head.payload_len, &self.options)?;
        let _node = self
            .incoming
            .query
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let _peer = incoming.acquire(charge).ok_or(TransportError::Overloaded)?;
        let deadline = self
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .saturating_add(self.options.io_timeout_ms);
        self.seed_process_io(&epoch, deadline, async {
            // Consume and authenticate the bounded request before closing the
            // stream, so unread request bytes cannot reset the signed refusal.
            let mut body = vec![0; head.payload_len];
            stream.read_exact(&mut body).await?;
            header.verify_body(&body)?;
            drop(body);
            let refusal = wire::read_refusal_body(header)?;
            let reply = wire::response_head(
                &self.auth,
                exporter,
                header,
                &head.tag,
                refusal,
                &self.options,
            )?;
            write_head(stream, &reply).await?;
            stream.write_all(refusal).await?;
            stream.flush().await?;
            Ok(())
        })
        .await
    }

    pub(super) async fn send_queries(
        self: Arc<Self>,
        pool: Arc<Pool>,
        mut requests: mailbox::Receiver<SendQuery>,
    ) {
        let mut stop = self.stop.subscribe();
        let mut peer_stop = pool.stop.subscribe();
        let mut connection: Option<QueryConnection> = None;
        let mut active = FuturesUnordered::new();
        let mut pending = std::collections::VecDeque::new();
        loop {
            if *stop.borrow() || *peer_stop.borrow() {
                return;
            }
            // Keep socket assignment in the sender. A request waiting for a
            // new socket must be able to reuse an exchange that just finished,
            // even when the physical socket ceiling is already full.
            while let Some(request) = pending.pop_front() {
                let assigned = match connection.take() {
                    Some(connection) => connection,
                    None => match self.outgoing_query_slots.clone().try_acquire_owned() {
                        Ok(slot) => QueryConnection::reserved(slot),
                        Err(tokio::sync::TryAcquireError::NoPermits) => {
                            pending.push_front(request);
                            break;
                        }
                        Err(tokio::sync::TryAcquireError::Closed) => return,
                    },
                };
                let state = self.clone();
                let pool = pool.clone();
                let mut connection = Some(assigned);
                // Requests already own node/peer item and byte admission.
                // Poll their exchanges here, without spawning more workers.
                active.push(async move {
                    let failed = match request {
                        SendQuery::Write(request) => {
                            state
                                .send_cell_write(&pool, *request, &mut connection)
                                .await
                        }
                        SendQuery::Shared(request) => {
                            state
                                .send_cell_query(&pool, *request, &mut connection)
                                .await
                        }
                    };
                    if failed {
                        state.failed.fetch_add(1, Ordering::Relaxed);
                        None
                    } else {
                        connection
                    }
                });
            }
            // The server bounds its idle header read. Observe peer closure
            // while the lane is idle so the next request can connect afresh.
            // No response bytes are valid before the next request is sent.
            let idle_read = async {
                if let Some(socket) = connection.as_mut().and_then(|c| c.socket.as_mut()) {
                    let mut unexpected = [0];
                    let _ = socket.stream.read(&mut unexpected).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let work = async {
                crate::asyncrt::select! {
                    finished = active.next(), if !active.is_empty() => {
                        QueryEvent::Finished(finished.flatten())
                    },
                    request = requests.recv() => match request {
                        Some(request) => QueryEvent::Request(request),
                        None => QueryEvent::Stop,
                    },
                    slot = self.outgoing_query_slots.clone().acquire_owned(), if !pending.is_empty() => match slot {
                        Ok(slot) => QueryEvent::Socket(slot),
                        Err(_) => QueryEvent::Stop,
                    },
                }
            };
            let next = crate::asyncrt::select_biased! {
                "retire a closed idle connection before sending a queued request";
                _ = stop.changed() => QueryEvent::Stop,
                _ = peer_stop.changed() => QueryEvent::Stop,
                _ = idle_read => QueryEvent::IdleClosed,
                event = work => event,
            };
            match next {
                QueryEvent::Stop => return,
                QueryEvent::IdleClosed => connection = None,
                QueryEvent::Finished(completed) => {
                    // Retain the existing one idle socket per peer. Additional
                    // completed sockets close before refunding physical slots.
                    if connection.is_none() {
                        connection = completed;
                    }
                }
                QueryEvent::Request(request) => pending.push_back(request),
                QueryEvent::Socket(slot) => connection = Some(QueryConnection::reserved(slot)),
            }
        }
    }

    pub(super) async fn exchange_query(
        &self,
        connection: &mut Option<QueryConnection>,
        peer: &RaftPeerNode,
        route: QueryRoute,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, TransportError> {
        let connection = connection.as_mut().ok_or(TransportError::Closed)?;
        if connection.socket.is_none() {
            let owner = self.outgoing_connections.query();
            let tls = self
                .bounded(None, async {
                    let tcp = TcpStream::connect(peer.query_address).await?;
                    tcp.set_nodelay(true)?;
                    let tcp = self.observed_socket(tcp, metrics::Lane::Query);
                    let name = rustls::pki_types::ServerName::try_from(peer.server_name.clone())
                        .map_err(|_| TransportError::Identity)?;
                    tokio_rustls::TlsConnector::from(self.tls.client.clone())
                        .connect(name, tcp)
                        .await
                        .map_err(|e| TransportError::Tls(e.to_string()))
                })
                .await?;
            let exporter = tls
                .get_ref()
                .1
                .export_keying_material([0; 32], wire::EXPORT_LABEL, None)
                .map_err(|e| TransportError::Tls(e.to_string()))?;
            connection.socket = Some(Box::new(QuerySocket {
                stream: tls,
                exporter,
                _owner: owner,
            }));
        }
        let socket = connection.socket.as_mut().ok_or(TransportError::Closed)?;
        let QuerySocket {
            stream, exporter, ..
        } = socket.as_mut();
        // Each stream has one exclusive exchange owner. Assign after acquiring
        // that stream, so its reused requests remain monotone. Authentication
        // binds replay protection to the exact TLS exporter on each connection.
        let sequence = self
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            + 1;
        let header = wire::RequestHead::new(route, sequence, &body);
        let head = wire::request_head(&self.auth, exporter, &header, &body, &self.options)?;
        self.bounded(None, async {
            write_head(stream, &head).await?;
            stream.write_all(&body).await?;
            stream.flush().await?;
            Ok(())
        })
        .await?;
        drop(body);
        // A quiet socket while the receiver executes is not stalled framing.
        // The enclosing query_until retains the original request deadline.
        let mut prefix = [0; frame::PREFIX];
        stream.read_exact(&mut prefix[..1]).await?;
        let response = self
            .bounded(None, async {
                stream.read_exact(&mut prefix[1..]).await?;
                read_query_tail(stream, prefix, &self.options, true).await
            })
            .await?;
        let verified = wire::read_response(
            &response,
            &self.auth,
            exporter,
            &header,
            &head.tag,
            &self.options,
        )?;
        let mut body = vec![0; response.payload_len];
        self.bounded(None, async {
            stream.read_exact(&mut body).await?;
            Ok(())
        })
        .await?;
        verified.verify(&body)?;
        Ok(body)
    }
}

pub(super) struct QueryConnection {
    // Drop the physical socket and its observer before refunding its slot.
    socket: Option<Box<QuerySocket>>,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

struct QuerySocket {
    stream: tokio_rustls::client::TlsStream<metrics::Socket<TcpStream>>,
    exporter: [u8; 32],
    _owner: resources::ConnectionOwner,
}

impl QueryConnection {
    fn reserved(slot: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self {
            socket: None,
            _slot: slot,
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
    read_query_tail(stream, prefix, options, response).await
}

async fn read_query_tail<S: AsyncRead + Unpin>(
    stream: &mut S,
    prefix: [u8; frame::PREFIX],
    options: &TransportOptions,
    response: bool,
) -> Result<FrameHead, TransportError> {
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
