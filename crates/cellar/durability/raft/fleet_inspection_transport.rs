//! Purpose-specific remote safe-ReadIndex RPC. Node trust alone never creates
//! group authority; the receiver independently authorizes the rooted history.
use super::inspections::{classify, Delivery, META_BYTES};
use super::snapshots::{read_bulk_head, Wire};
use super::*;
use crate::application::consensus::{CommandFailure, RaftCells};
use crate::raft::fleet_catalog::FleetGroupAuthorization;
use crate::raft::fleet_inspection_wire::{self as body, History, LocalObservation};
use crate::raft::inspection::InspectionError;
use crate::raft::node_inspection_wire::NodeRoute;
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicU8;

#[cfg(test)]
#[path = "fleet_inspection_auth_tests.rs"]
mod tests;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLFQ";
const RESPONSE_MAGIC: &[u8; 4] = b"CLFR";
pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-fleet-quorum-1";
const REQUEST_PURPOSE: &[u8] = b"cellar-fleet-quorum-request-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-fleet-quorum-response-1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestHead {
    route: NodeRoute,
    history: History,
    nonce: String,
    timeout_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHead {
    route: NodeRoute,
    history: History,
    nonce: String,
    status: u8,
    digest: [u8; 32],
}

pub(crate) struct Received {
    pub observation: LocalObservation,
    _node: Permit,
    _peer: Permit,
}

impl RaftTransport {
    pub(crate) fn inspect_fleet_membership(
        &self,
        node: NodeId,
        authorization: &FleetGroupAuthorization,
        minimum_revision: u64,
        deadline: u64,
    ) -> Result<Delivery<Received>, InspectionError> {
        self.begin_fleet_inspection(node, authorization, minimum_revision, deadline)
            .map_err(|error| classify(error, 0))
    }

    fn begin_fleet_inspection(
        &self,
        node: NodeId,
        authorization: &FleetGroupAuthorization,
        minimum_revision: u64,
        deadline: u64,
    ) -> Result<Delivery<Received>, TransportError> {
        let state = self.handle.0.clone();
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if deadline <= now || deadline - now > 60_000 {
            return Err(TransportError::Timeout);
        }
        let (peer, route, peer_budget) = state.node_metadata_route(node, minimum_revision)?;
        let history = History::new(authorization);
        validate_history_route(&route, &history)?;
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
            let io = Box::pin(operation.fleet_inspection_exchange(
                &peer,
                &route,
                &history,
                nonce,
                deadline,
                stage.clone(),
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
                .and_then(|view| {
                    operation.validate_node_observation(&view.node, minimum_revision)?;
                    if *epoch.borrow() || *cancelled.borrow() {
                        return Err(TransportError::StalePeer);
                    }
                    Ok(Received {
                        observation: view,
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
    async fn fleet_inspection_exchange(
        &self,
        peer: &RaftPeerNode,
        route: &NodeRoute,
        history: &History,
        nonce: u64,
        deadline: u64,
        stage: Arc<AtomicU8>,
    ) -> Result<LocalObservation, TransportError> {
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
            max_body: body::MAX_BODY,
        };
        let request = RequestHead {
            route: route.clone(),
            history: history.clone(),
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
        if received.route != *route
            || received.history != *history
            || received.nonce != request.nonce
        {
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
            4 if bytes.is_empty() => return Err(TransportError::Closed),
            _ => return Err(TransportError::Protocol),
        }
        let view = body::decode(&bytes)?;
        let (_, target) = route.identities()?;
        if view.node.node.identity != target.identity
            || view.node.node.storage_instance != target.storage_instance
        {
            return Err(TransportError::Identity);
        }
        self.validate_node_observation(&view.node, route.minimum_revision()?)?;
        Ok(view)
    }

    pub(super) async fn receive_fleet_inspection<S: AsyncRead + AsyncWrite + Unpin>(
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
        let authorized = validate_history_route(&request.route, &request.history)
            .and_then(|()| self.authorize_node_inspection(&request.route));
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
                        history: request.history,
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
        self.seed_process_until(&epoch, deadline, async {
            let remaining = deadline
                .checked_sub(self.domain.mono_ms().map_err(|_| TransportError::Closed)?)
                .filter(|ms| *ms > 0)
                .ok_or(TransportError::Timeout)?;
            let work = RaftCells { handle }.observe_local_fleet_membership(
                request.history.clone(), request.route.minimum_revision()?, std::time::Duration::from_millis(remaining),
            );
            let mut extra = [0; 1];
            // The connection cannot pipeline. EOF/unexpected input drops the
            // nested SDK waiter, including a blocked quorum read or catalog IO.
            let result = crate::asyncrt::select_biased! { "cancel remote quorum work on disconnect";
                read = stream.read(&mut extra) => { let _ = read?; return Err(TransportError::Closed); },
                result = work => result,
            };
            let (status, bytes) = match result {
                Ok(view) => {
                    self.validate_node_observation(&view.node, request.route.minimum_revision()?)?;
                    (0, body::encode(&view)?)
                }
                Err(CommandFailure::Overloaded) => (1, vec![]),
                Err(CommandFailure::UnsupportedCapability | CommandFailure::CommandConflict
                    | CommandFailure::CorruptState) => (2, vec![]),
                _ => (4, vec![]),
            };
            self.authorize_node_inspection(&request.route)?;
            let reply = ReplyHead {
                route: request.route,
                history: request.history,
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

fn validate_history_route(route: &NodeRoute, history: &History) -> Result<(), TransportError> {
    let scope = history.scope()?;
    if history.binding.fleet() != route.fleet()
        || history.binding.catalog() != route.catalog()
        || scope.cluster.bytes() != route.cluster()
    {
        return Err(TransportError::Identity);
    }
    Ok(())
}
