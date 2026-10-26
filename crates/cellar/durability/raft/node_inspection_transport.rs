//! Owned, bounded read-only fleet metadata RPC; no group or member is invented.
use super::inspections::{classify, Delivery, META_BYTES};
use super::snapshots::{read_bulk_head, Wire};
use super::*;
use crate::application::consensus::{CommandFailure, RaftCells};
use crate::raft::host::RaftHostObservation;
use crate::raft::inspection::InspectionError;
use crate::raft::inspection_identity::PhysicalNode;
use crate::raft::node_inspection_wire::{self as body, NodeRoute};
use cellar_logic::consensus::NodeIdentity;
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicU8;

#[cfg(test)]
#[path = "node_inspection_auth_tests.rs"]
mod tests;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLNQ";
const RESPONSE_MAGIC: &[u8; 4] = b"CLNR";
pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-node-inspection-1";
const REQUEST_PURPOSE: &[u8] = b"cellar-node-inspection-request-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-node-inspection-response-1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestHead {
    route: NodeRoute,
    nonce: String,
    timeout_ms: u64,
    physical: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHead {
    route: NodeRoute,
    nonce: String,
    status: u8,
    digest: [u8; 32],
}

pub(crate) struct Received {
    pub view: Box<RaftHostObservation>,
    pub inventory: Option<Box<crate::raft::physical_inventory::RaftPhysicalInventory>>,
    _node: Permit,
    _peer: Permit,
}

impl RaftTransport {
    pub(crate) fn inspect_fleet_node(
        &self,
        node: NodeId,
        minimum_revision: u64,
        deadline: u64,
        physical: bool,
    ) -> Result<Delivery<Received>, InspectionError> {
        self.begin_node_inspection(node, minimum_revision, deadline, physical)
            .map_err(|error| classify(error, 0))
    }

    pub(crate) fn validate_fleet_node_observation(
        &self,
        view: &RaftHostObservation,
        minimum_revision: u64,
    ) -> Result<(), InspectionError> {
        self.handle
            .0
            .validate_node_observation(view, minimum_revision)
            .map_err(|error| classify(error, 0))
    }

    fn begin_node_inspection(
        &self,
        node: NodeId,
        minimum_revision: u64,
        deadline: u64,
        physical: bool,
    ) -> Result<Delivery<Received>, TransportError> {
        let state = self.handle.0.clone();
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if deadline <= now || deadline - now > 60_000 {
            return Err(TransportError::Timeout);
        }
        let (peer, route, peer_budget) = state.node_metadata_route(node, minimum_revision)?;
        let epoch = state.seed_peer_epoch(&peer)?;
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
            let io = Box::pin(operation.node_inspection_exchange(
                &peer,
                &route,
                nonce,
                deadline,
                stage.clone(),
                physical,
            ));
            let result = if *cancelled.borrow() {
                Err(TransportError::Closed)
            } else {
                operation.seed_process_until(&epoch, deadline, async {
                    crate::asyncrt::select_biased! { "caller cancellation precedes node metadata";
                        _ = cancelled.changed() => Err(TransportError::Closed),
                        result = io => result,
                    }
                }).await
            };
            let result = result
                .and_then(|(view, inventory)| {
                    operation.validate_node_observation(&view, minimum_revision)?;
                    if *epoch.borrow() || *cancelled.borrow() {
                        return Err(TransportError::StalePeer);
                    }
                    Ok(Received {
                        view,
                        inventory,
                        _node: node,
                        _peer: peer_bytes,
                    })
                })
                .map_err(|error| classify(error, stage.load(Ordering::Acquire)));
            let _ = reply.send(result);
        })?;
        Ok(Delivery {
            reply: Some(wait),
            cancel,
        })
    }
}

impl State {
    pub(super) fn node_metadata_route(
        &self,
        node: NodeId,
        minimum_revision: u64,
    ) -> Result<(RaftPeerNode, NodeRoute, Arc<Budget>), TransportError> {
        let mut registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let trust = registry
            .fleet_trust
            .as_ref()
            .ok_or(TransportError::Refused)?;
        if minimum_revision == 0 || trust.revision() < minimum_revision {
            return Err(TransportError::StalePeer);
        }
        let view = registry
            .node_views
            .get(&node)
            .ok_or(TransportError::Refused)?;
        let peer = view.peer().clone();
        registry.check_node_view(&peer)?;
        if trust
            .node(node)
            .is_none_or(|n| n.storage_instance() != Some(view.storage_instance()))
        {
            return Err(TransportError::Identity);
        }
        let route = NodeRoute::new(
            trust,
            minimum_revision,
            &self.local_physical()?,
            self.auth.source(),
            &PhysicalNode {
                identity: NodeIdentity {
                    node: peer.node,
                    disk: peer.disk,
                    process: peer.process,
                },
                storage_instance: view.storage_instance(),
            },
            &peer.label,
        )?;
        let budget = registry.inspection_peer_budget(node, self.options.max_peers)?;
        Ok((peer, route, budget))
    }

    pub(super) fn local_physical(&self) -> Result<PhysicalNode, TransportError> {
        let local = self.runtime.log().limits();
        Ok(PhysicalNode {
            identity: NodeIdentity {
                node: local.node,
                disk: local.disk,
                process: local.process,
            },
            storage_instance: self
                .runtime
                .log()
                .storage_instance()
                .map_err(|_| TransportError::Closed)?,
        })
    }

    pub(super) fn validate_node_observation(
        &self,
        view: &RaftHostObservation,
        minimum_revision: u64,
    ) -> Result<(), TransportError> {
        body::validate(view)?;
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let trust = registry
            .fleet_trust
            .as_ref()
            .ok_or(TransportError::Refused)?;
        if trust.revision() < minimum_revision {
            return Err(TransportError::StalePeer);
        }
        let node = trust
            .node(view.node.identity.node)
            .ok_or(TransportError::Refused)?;
        let process = registry
            .node_views
            .get(&node.node())
            .ok_or(TransportError::Refused)?;
        registry.check_node_view(process.peer())?;
        let current = view
            .current_enrollment
            .as_ref()
            .ok_or(TransportError::Identity)?;
        if view.node.identity
            != (NodeIdentity {
                node: process.peer().node,
                disk: process.peer().disk,
                process: process.peer().process,
            })
            || view.node.storage_instance != process.storage_instance()
            || node.storage_instance() != Some(view.node.storage_instance)
            || current.revision < minimum_revision
            || current.fleet != trust.fleet()
            || current.epoch != node.epoch()
            || current.physical_host != node.physical_host()
            || current.state != node.state()
            || current.capability_digest != node.capability_digest()
        {
            return Err(TransportError::StalePeer);
        }
        Ok(())
    }

    async fn node_inspection_exchange(
        &self,
        peer: &RaftPeerNode,
        route: &NodeRoute,
        nonce: u64,
        deadline: u64,
        stage: Arc<AtomicU8>,
        physical: bool,
    ) -> Result<
        (
            Box<RaftHostObservation>,
            Option<Box<crate::raft::physical_inventory::RaftPhysicalInventory>>,
        ),
        TransportError,
    > {
        stage.store(1, Ordering::Release);
        let _connection = self.outgoing_connections.acquire(Lane::Control);
        let tcp = TcpStream::connect(peer.control_address).await?;
        stage.store(2, Ordering::Release);
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
            max_body: if physical {
                crate::raft::physical_inventory::MAX_BODY
            } else {
                body::MAX_BODY
            },
        };
        let request = RequestHead {
            route: route.clone(),
            nonce: nonce.to_string(),
            timeout_ms: remaining,
            physical,
        };
        let head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
        write_head(&mut stream, &head).await?;
        let response = read_bulk_head(
            &mut stream,
            None,
            RESPONSE_MAGIC,
            if physical {
                crate::raft::physical_inventory::MAX_BODY
            } else {
                body::MAX_BODY
            },
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
        if received.digest != <[u8; 32]>::from(Sha256::digest(&bytes)) {
            return Err(TransportError::Protocol);
        }
        match received.status {
            0 => (),
            1 if bytes.is_empty() => return Err(TransportError::Overloaded),
            2 if bytes.is_empty() => return Err(TransportError::Refused),
            3 if bytes.is_empty() => return Err(TransportError::StalePeer),
            _ => return Err(TransportError::Protocol),
        }
        let (view, inventory) = if physical {
            let (view, inventory) = crate::raft::physical_inventory::decode(&bytes)?;
            (view, Some(inventory))
        } else {
            (body::decode(&bytes)?, None)
        };
        let (_, target) = route.identities()?;
        if view.node.identity != target.identity
            || view.node.storage_instance != target.storage_instance
        {
            return Err(TransportError::Identity);
        }
        self.validate_node_observation(&view, route.minimum_revision()?)?;
        Ok((view, inventory))
    }

    pub(super) fn authorize_node_inspection(
        &self,
        route: &NodeRoute,
    ) -> Result<(RaftPeerNode, Arc<Budget>), TransportError> {
        let (source, target) = route.identities()?;
        if target != self.local_physical()? || route.target_label() != self.auth.source() {
            return Err(TransportError::StalePeer);
        }
        let mut registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let trust = registry
            .fleet_trust
            .as_ref()
            .ok_or(TransportError::Refused)?;
        if trust.fleet() != route.fleet()
            || trust.catalog() != route.catalog()
            || trust.cluster().bytes() != route.cluster()
        {
            return Err(TransportError::Refused);
        }
        if trust.revision() < route.minimum_revision()? {
            return Err(TransportError::StalePeer);
        }
        let view = registry
            .node_views
            .get(&source.identity.node)
            .ok_or(TransportError::Refused)?;
        let peer = view.peer().clone();
        registry.check_node_view(&peer)?;
        if source.identity
            != (NodeIdentity {
                node: peer.node,
                disk: peer.disk,
                process: peer.process,
            })
            || source.storage_instance != view.storage_instance()
            || route.source_label() != peer.label
            || trust
                .node(peer.node)
                .is_none_or(|n| n.storage_instance() != Some(source.storage_instance))
        {
            return Err(TransportError::StalePeer);
        }
        let budget = registry.inspection_peer_budget(peer.node, self.options.max_peers)?;
        Ok((peer, budget))
    }

    pub(super) async fn receive_node_inspection<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
    ) -> Result<(), TransportError> {
        let admitted = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
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
            max_body: crate::raft::physical_inventory::MAX_BODY,
        };
        let request: RequestHead = wire.decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)?;
        if frame::decimal(&request.nonce)? == 0 || !(1..=60_000).contains(&request.timeout_ms) {
            return Err(TransportError::Protocol);
        }
        let authorized = self.authorize_node_inspection(&request.route);
        let (peer, peer_budget) = match authorized {
            Ok(result) => result,
            Err(error) => {
                let status = match error {
                    TransportError::Overloaded => 1,
                    TransportError::StalePeer => 3,
                    _ => 2,
                };
                let response = wire.encode(
                    RESPONSE_MAGIC,
                    RESPONSE_PURPOSE,
                    &head.tag,
                    &ReplyHead {
                        route: request.route,
                        nonce: request.nonce,
                        status,
                        digest: Sha256::digest([]).into(),
                    },
                    &[],
                )?;
                return self.bounded(None, write_head(stream, &response)).await;
            }
        };
        let _peer = peer_budget
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let epoch = self.seed_peer_epoch(&peer)?;
        let deadline = admitted
            .checked_add(request.timeout_ms)
            .ok_or(TransportError::Limit)?;
        let handle = self
            .query_gateway
            .lock()
            .map_err(|_| TransportError::Closed)?
            .as_ref()
            .and_then(|h| h.upgrade())
            .ok_or(TransportError::Refused)?;
        self.seed_process_until(&epoch, deadline, async {
            let remaining = deadline
                .checked_sub(self.domain.mono_ms().map_err(|_| TransportError::Closed)?)
                .filter(|ms| *ms > 0)
                .ok_or(TransportError::Timeout)?;
            let result = if request.physical {
                match (RaftCells { handle })
                    .observe_physical_node(std::time::Duration::from_millis(remaining))
                    .await
                {
                    Ok((node, inventory)) => {
                        self.validate_node_observation(&node, request.route.minimum_revision()?)?;
                        Ok(crate::raft::physical_inventory::encode(&node, &inventory)?)
                    }
                    Err(error) => Err(error),
                }
            } else {
                match (RaftCells { handle })
                    .observe_node(std::time::Duration::from_millis(remaining))
                    .await
                {
                    Ok(node) => {
                        self.validate_node_observation(&node, request.route.minimum_revision()?)?;
                        Ok(body::encode(&node)?)
                    }
                    Err(error) => Err(error),
                }
            };
            let (status, bytes) = match result {
                Ok(bytes) => (0, bytes),
                Err(CommandFailure::Overloaded) => (1, vec![]),
                _ => (2, vec![]),
            };
            self.authorize_node_inspection(&request.route)?;
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
