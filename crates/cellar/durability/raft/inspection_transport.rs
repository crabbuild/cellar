// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Purpose-specific physical-node inspection. The operation belongs to the
//! transport task registry; dropping a caller cancels its read-only RPC.
use super::snapshots::{read_bulk_head, Wire};
use super::*;
use crate::application::consensus::{CommandFailure, RaftCells};
use crate::raft::inspection::{InspectionError, InspectionSnapshot};
use crate::raft::inspection_identity::{InspectionRoute, PhysicalNode};
use crate::raft::inspection_wire as body;
use crate::raft::transport_options::NamespaceInspectionOrigin;
use cellar_logic::consensus::NodeIdentity;
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicU8;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLIQ";
const RESPONSE_MAGIC: &[u8; 4] = b"CLIP";
pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-inspection-1";
const REQUEST_PURPOSE: &[u8] = b"cellar-inspection-request-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-inspection-response-1";
pub(super) const SLOTS: usize = 16;
pub(super) const META_BYTES: usize = 1024 * 1024;

#[cfg(test)]
#[path = "inspection_connect_tests.rs"]
mod connect_tests;

pub(super) async fn dial<T>(
    domain: &crate::asyncrt::DomainToken,
    deadline: u64,
    stage: &AtomicU8,
    connect: impl Future<Output = std::io::Result<T>>,
) -> Result<T, TransportError> {
    let now = domain.mono_ms().map_err(|_| TransportError::Closed)?;
    let remaining = deadline
        .checked_sub(now)
        .filter(|ms| *ms >= 2)
        .ok_or(TransportError::Timeout)?;
    // A dial that consumes the outer deadline cannot report Unreachable: the
    // host has already cancelled its waiter. Reserve half the remaining call
    // budget for classification and delivery. Local expiry before an attempt
    // keeps stage zero, and TLS/metadata retain stage two and the full deadline.
    let timer = domain
        .sleep_until(now + remaining / 2)
        .map_err(|_| TransportError::Closed)?;
    stage.store(1, Ordering::Release);
    let socket = crate::asyncrt::select_biased! { "the connection phase expires before a late socket is accepted";
        _ = timer => Err(TransportError::Timeout),
        result = connect => result.map_err(TransportError::from),
    }?;
    stage.store(2, Ordering::Release);
    Ok(socket)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestHead {
    route: InspectionRoute,
    nonce: String,
    timeout_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHead {
    route: InspectionRoute,
    nonce: String,
    status: u8,
    digest: [u8; 32],
}

pub(crate) struct Received {
    pub snapshot: Box<InspectionSnapshot>,
    _node: Permit,
    _peer: Permit,
}

pub(crate) struct Delivery<T = Received> {
    pub(super) reply: Option<oneshot::Receiver<Result<T, InspectionError>>>,
    pub(super) cancel: watch::Sender<bool>,
}

impl<T> Delivery<T> {
    pub(crate) async fn receive(mut self) -> Result<T, InspectionError> {
        self.reply
            .take()
            .ok_or(InspectionError::Unavailable)?
            .await
            .map_err(|_| InspectionError::Unavailable)?
    }
}

impl<T> Drop for Delivery<T> {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

pub(super) fn classify(error: TransportError, stage: u8) -> InspectionError {
    match error {
        TransportError::Overloaded => InspectionError::Overloaded,
        TransportError::Refused | TransportError::UnknownGroup | TransportError::Identity => {
            InspectionError::Refused
        }
        TransportError::StalePeer => InspectionError::StalePeer,
        TransportError::Io(_) | TransportError::Timeout if stage == 1 => {
            InspectionError::Unreachable
        }
        _ => InspectionError::Unavailable,
    }
}

impl Registry {
    pub(super) fn inspection_peer_budget(
        &mut self,
        node: NodeId,
        limit: usize,
    ) -> Result<Arc<Budget>, TransportError> {
        if let Some(budget) = self.inspection_peers.get(&node) {
            return Ok(budget.clone());
        }
        if self.inspection_peers.len() >= limit {
            return Err(TransportError::Overloaded);
        }
        let budget = Budget::new(META_BYTES * 2, 2);
        self.inspection_peers.insert(node, budget.clone());
        Ok(budget)
    }
}

impl RaftTransport {
    pub fn grant_inspection_origin(
        &self,
        grant: NamespaceInspectionOrigin,
    ) -> Result<(), TransportError> {
        let state = &self.handle.0;
        grant.peer.validate_endpoint()?;
        if grant.revision == 0 || grant.peer.node == state.runtime.log().limits().node {
            return Err(TransportError::Identity);
        }
        let key = (grant.cluster, grant.namespace, grant.peer.node);
        let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        registry.check_node_view(&grant.peer)?;
        if let Some(old) = registry.inspection_origins.get(&key) {
            if old.revision == grant.revision && old.peer.as_ref() == Some(&grant.peer) {
                return Ok(());
            }
            if old.revision >= grant.revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.inspection_origins.len() >= state.options.max_query_origins {
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
                .values()
                .filter_map(|old| old.peer.as_ref())
                .chain(
                    registry
                        .inspection_origins
                        .iter()
                        .filter(|(other, _)| **other != key)
                        .filter_map(|(_, old)| old.peer.as_ref()),
                )
                .any(|peer| peer.node == grant.peer.node && peer != &grant.peer)
        {
            return Err(TransportError::Identity);
        }
        let (stop, _) = watch::channel(false);
        if let Some(old) = registry.inspection_origins.insert(
            key,
            Arc::new(queries::OriginGrant {
                revision: grant.revision,
                peer: Some(grant.peer),
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        if !state.inspection_enabled.swap(true, Ordering::AcqRel) {
            state.control_connections.add_permits(SLOTS);
        }
        drop(registry);
        Ok(())
    }

    pub fn revoke_inspection_origin(
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
        if let Some(old) = registry.inspection_origins.get(&key) {
            if old.revision == revision && old.peer.is_none() {
                return Ok(());
            }
            if old.revision >= revision {
                return Err(TransportError::StalePeer);
            }
        } else if registry.inspection_origins.len() >= state.options.max_query_origins {
            return Err(TransportError::Overloaded);
        }
        let (stop, _) = watch::channel(false);
        if let Some(old) = registry.inspection_origins.insert(
            key,
            Arc::new(queries::OriginGrant {
                revision,
                peer: None,
                stop,
            }),
        ) {
            old.stop.send_replace(true);
        }
        Ok(())
    }

    pub(crate) fn inspect_peer(
        &self,
        scope: GroupScope,
        schema: [u8; 32],
        birth: [u8; 32],
        peer: RaftPeerNode,
        deadline: u64,
    ) -> Result<Delivery, InspectionError> {
        self.begin_inspection(scope, schema, birth, peer, deadline)
            .map_err(|error| classify(error, 0))
    }

    fn begin_inspection(
        &self,
        scope: GroupScope,
        schema: [u8; 32],
        birth: [u8; 32],
        peer: RaftPeerNode,
        deadline: u64,
    ) -> Result<Delivery, TransportError> {
        let state = self.handle.0.clone();
        peer.validate_endpoint()?;
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if deadline <= now || deadline - now > 60_000 {
            return Err(TransportError::Timeout);
        }
        let (target, peer_budget) = {
            let mut registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
            state.admit(&registry)?;
            registry.check_node_view(&peer)?;
            let view = registry
                .node_views
                .get(&peer.node)
                .ok_or(TransportError::Refused)?;
            let target = PhysicalNode {
                identity: NodeIdentity {
                    node: peer.node,
                    disk: peer.disk,
                    process: peer.process,
                },
                storage_instance: view.storage_instance(),
            };
            let budget = registry.inspection_peer_budget(peer.node, state.options.max_peers)?;
            (target, budget)
        };
        let epoch = state.seed_peer_epoch(&peer)?;
        let local = state.runtime.log().limits();
        let source = PhysicalNode {
            identity: NodeIdentity {
                node: local.node,
                disk: local.disk,
                process: local.process,
            },
            storage_instance: state
                .runtime
                .log()
                .storage_instance()
                .map_err(|_| TransportError::Closed)?,
        };
        let route = InspectionRoute::new(
            &scope,
            schema,
            birth,
            &source,
            state.auth.source(),
            &target,
            &peer.label,
        )?;
        let node = state
            .inspection_metadata
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let peer_bytes = peer_budget
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let nonce = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            + 1;
        let (cancel, mut cancelled) = watch::channel(false);
        let (reply, wait) = oneshot::channel();
        let operation = state.clone();
        state.spawn(async move {
            let stage = Arc::new(AtomicU8::new(0));
            let io = Box::pin(operation.inspection_exchange(
                &peer, &route, nonce, deadline, stage.clone(),
            ));
            let result = if *cancelled.borrow() {
                Err(TransportError::Closed)
            } else {
                operation.seed_process_until(&epoch, deadline, async {
                    crate::asyncrt::select_biased! { "caller cancellation precedes an inspection response";
                        _ = cancelled.changed() => Err(TransportError::Closed),
                        result = io => result,
                    }
                }).await
            };
            let result = result.and_then(|snapshot| {
                let registry = operation.registry.lock().map_err(|_| TransportError::Closed)?;
                operation.admit(&registry)?;
                registry.check_node_view(&peer)?;
                if *epoch.borrow() || *cancelled.borrow() {
                    return Err(TransportError::StalePeer);
                }
                Ok(snapshot)
            })
            .map(|snapshot| Received { snapshot, _node: node, _peer: peer_bytes })
            .map_err(|error| classify(error, stage.load(Ordering::Acquire)));
            let _ = reply.send(result);
        })?;
        Ok(Delivery {
            reply: Some(wait),
            cancel,
        })
    }
}

fn validate_response(
    route: &InspectionRoute,
    snapshot: &InspectionSnapshot,
) -> Result<(), TransportError> {
    let (scope, _, target) = route.decode()?;
    if snapshot.view.scope != scope
        || snapshot.view.schema_digest != route.schema()
        || snapshot.view.birth_digest != route.birth()
        || snapshot.view.node.identity != target.identity
        || snapshot.view.node.storage_instance != target.storage_instance
    {
        return Err(TransportError::Identity);
    }
    Ok(())
}

impl State {
    async fn inspection_exchange(
        &self,
        peer: &RaftPeerNode,
        route: &InspectionRoute,
        nonce: u64,
        deadline: u64,
        stage: Arc<AtomicU8>,
    ) -> Result<Box<InspectionSnapshot>, TransportError> {
        let _connection = self.outgoing_connections.acquire(Lane::Control);
        let tcp = dial(
            &self.domain,
            deadline,
            &stage,
            TcpStream::connect(peer.control_address),
        )
        .await?;
        tcp.set_nodelay(true)?;
        let tcp = self.observed_socket(tcp, metrics::Lane::Control);
        let name = rustls::pki_types::ServerName::try_from(peer.server_name.clone())
            .map_err(|_| TransportError::Identity)?;
        let mut stream = tokio_rustls::TlsConnector::from(self.tls.client.clone())
            .connect(name, tcp)
            .await
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], EXPORT_LABEL, None)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let remaining = deadline
            .checked_sub(self.domain.mono_ms().map_err(|_| TransportError::Closed)?)
            .filter(|ms| *ms > 0)
            .ok_or(TransportError::Timeout)?;
        let wire = Wire {
            auth: &self.auth,
            exporter: &exporter,
            options: &self.options,
            max_body: body::MAX_BODY,
        };
        let request = RequestHead {
            route: route.clone(),
            nonce: nonce.to_string(),
            timeout_ms: remaining,
        };
        let head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
        write_head(&mut stream, &head).await?;
        let response = read_bulk_head(
            &mut stream,
            None,
            RESPONSE_MAGIC,
            body::MAX_BODY,
            &self.options,
        )
        .await?;
        let received: ReplyHead =
            wire.decode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&head.tag))?;
        if received.route != *route || received.nonce != request.nonce {
            return Err(TransportError::Replay);
        }
        let mut bytes = vec![0; response.payload_len];
        stream.read_exact(&mut bytes).await?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if received.digest != digest {
            return Err(TransportError::Protocol);
        }
        match received.status {
            0 => (),
            1 if bytes.is_empty() => return Err(TransportError::Overloaded),
            2 if bytes.is_empty() => return Err(TransportError::Refused),
            _ => return Err(TransportError::Protocol),
        }
        let snapshot = body::decode(&bytes)?;
        validate_response(route, &snapshot)?;
        Ok(snapshot)
    }

    pub(super) async fn receive_inspection<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
    ) -> Result<(), TransportError> {
        let _node = self
            .inspection_metadata
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let head = self
            .bounded(
                None,
                read_bulk_head(stream, Some(prefix), REQUEST_MAGIC, 0, &self.options),
            )
            .await?;
        let wire = Wire {
            auth: &self.auth,
            exporter,
            options: &self.options,
            max_body: body::MAX_BODY,
        };
        let request: RequestHead = wire.decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)?;
        if frame::decimal(&request.nonce)? == 0 || !(1..=60_000).contains(&request.timeout_ms) {
            return Err(TransportError::Protocol);
        }
        let (scope, source, target) = request.route.decode()?;
        let local = self.runtime.log().limits();
        if target.identity
            != (NodeIdentity {
                node: local.node,
                disk: local.disk,
                process: local.process,
            })
            || target.storage_instance
                != self
                    .runtime
                    .log()
                    .storage_instance()
                    .map_err(|_| TransportError::Closed)?
            || request.route.target_label() != self.auth.source()
        {
            return Err(TransportError::StalePeer);
        }
        let authorized = (|| {
            let mut registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
            self.admit(&registry)?;
            let grant = registry
                .inspection_origins
                .get(&(scope.cluster, scope.namespace, source.identity.node))
                .cloned()
                .ok_or(TransportError::Refused)?;
            let peer = grant.peer.as_ref().ok_or(TransportError::Refused)?;
            registry.check_node_view(peer)?;
            let view = registry
                .node_views
                .get(&peer.node)
                .ok_or(TransportError::Refused)?;
            if source.identity
                != (NodeIdentity {
                    node: peer.node,
                    disk: peer.disk,
                    process: peer.process,
                })
                || source.storage_instance != view.storage_instance()
                || request.route.source_label() != peer.label
            {
                return Err(TransportError::StalePeer);
            }
            let budget = registry.inspection_peer_budget(peer.node, self.options.max_peers)?;
            Ok::<_, TransportError>((grant, budget))
        })();
        let (grant, peer_budget) = match authorized {
            Ok(authorized) => authorized,
            Err(TransportError::Refused | TransportError::UnknownGroup) => {
                let reply = ReplyHead {
                    route: request.route,
                    nonce: request.nonce,
                    status: 2,
                    digest: Sha256::digest([]).into(),
                };
                let response =
                    wire.encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &[])?;
                return self.bounded(None, write_head(stream, &response)).await;
            }
            Err(error) => return Err(error),
        };
        let _peer = peer_budget
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let deadline = self
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .checked_add(request.timeout_ms)
            .ok_or(TransportError::Limit)?;
        let handle = self
            .query_gateway
            .lock()
            .map_err(|_| TransportError::Closed)?
            .as_ref()
            .and_then(|h| h.upgrade())
            .ok_or(TransportError::Refused)?;
        let binding = handle
            .bindings
            .get(&(scope.namespace, scope.cell.clone()))
            .map_err(|_| TransportError::UnknownGroup)?;
        if binding.scope != scope || binding.class.schema_digest() != request.route.schema() {
            return Err(TransportError::Refused);
        }
        let cell = RaftCells { handle }
            .cell(scope.namespace, &scope.cell)
            .map_err(|_| TransportError::UnknownGroup)?;
        let epoch = grant.stop.subscribe();
        self.seed_process_until(&epoch, deadline, async {
            let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
            let duration = std::time::Duration::from_millis(
                deadline
                    .checked_sub(now)
                    .filter(|ms| *ms > 0)
                    .ok_or(TransportError::Timeout)?,
            );
            let result = cell.inspect_snapshot(duration).await;
            if *epoch.borrow() {
                return Err(TransportError::StalePeer);
            }
            if *self.stop.borrow() {
                return Err(TransportError::Closed);
            }
            let (status, bytes) = match result {
                Ok(snapshot) if snapshot.view.birth_digest == request.route.birth() => {
                    (0, body::encode(&snapshot)?)
                }
                Err(CommandFailure::Overloaded) => (1, vec![]),
                _ => (2, vec![]),
            };
            let reply = ReplyHead {
                route: request.route,
                nonce: request.nonce,
                status,
                digest: Sha256::digest(&bytes).into(),
            };
            let response =
                wire.encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &bytes)?;
            write_head(stream, &response).await?;
            stream.write_all(&bytes).await?;
            stream.flush().await?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
#[path = "inspection_auth_tests.rs"]
mod tests;
