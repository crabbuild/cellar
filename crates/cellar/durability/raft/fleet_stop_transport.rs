// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact-boot stop/fence RPC on the existing rooted, authenticated control lane.
//! Transport cancellation never cancels work admitted by the Node-owned worker.
use super::inspections::META_BYTES;
use super::snapshots::{read_bulk_head, Wire};
use super::*;
use crate::raft::fleet_catalog::FleetCatalogError;
use crate::raft::fleet_stop::{BootBinding, FleetStopAcceptance, FleetStopRequest};
use crate::raft::node_inspection_wire::NodeRoute;
use sha2::{Digest, Sha256};

#[cfg(test)]
#[path = "fleet_stop_transport_tests.rs"]
mod tests;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLTQ";
const RESPONSE_MAGIC: &[u8; 4] = b"CLTR";
pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-fleet-stop-1";
const REQUEST_PURPOSE: &[u8] = b"cellar-fleet-stop-request-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-fleet-stop-response-1";
const MAX_BODY: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestHead {
    route: NodeRoute,
    origin: BootBinding,
    nonce: String,
    timeout_ms: u64,
    digest: [u8; 32],
}

impl RequestHead {
    fn deadline(&self, connected_at: u64, now: u64) -> Result<u64, TransportError> {
        let deadline = connected_at
            .checked_add(self.timeout_ms)
            .ok_or(TransportError::Limit)?;
        if !(1..=60_000).contains(&self.timeout_ms) || now < connected_at || now >= deadline {
            return Err(TransportError::Timeout);
        }
        Ok(deadline)
    }

    fn decode_body(&self, bytes: &[u8]) -> Result<FleetStopRequest, TransportError> {
        if frame::decimal(&self.nonce)? == 0
            || !(1..=60_000).contains(&self.timeout_ms)
            || bytes.len() > MAX_BODY
            || self.digest != <[u8; 32]>::from(Sha256::digest(bytes))
        {
            return Err(TransportError::Protocol);
        }
        let request: FleetStopRequest =
            serde_json::from_slice(bytes).map_err(|_| TransportError::Protocol)?;
        check_request(&self.route, &request)?;
        self.check_origin()?;
        Ok(request)
    }

    fn check_origin(&self) -> Result<(), TransportError> {
        self.origin
            .validate()
            .map_err(|_| TransportError::Identity)?;
        let (source, _) = self.route.identities()?;
        if self.origin.catalog != self.route.catalog()
            || self.origin.fleet != self.route.fleet()
            || self.origin.node != source.identity.node.bytes()
            || self.origin.disk != source.identity.disk.bytes()
            || self.origin.process != source.identity.process.bytes()
            || self.origin.engine != source.storage_instance.bytes()
        {
            return Err(TransportError::Identity);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Accepted,
    Conflict,
    PayloadConflict,
    Refused,
    Overloaded,
    Unconfirmed,
}

impl Status {
    fn result(self) -> Result<(), FleetCatalogError> {
        match self {
            Self::Accepted => Ok(()),
            Self::Conflict => Err(FleetCatalogError::Conflict),
            Self::PayloadConflict => Err(FleetCatalogError::PayloadConflict),
            Self::Refused => Err(FleetCatalogError::Invalid("peer refused fleet stop")),
            Self::Overloaded => Err(FleetCatalogError::Overloaded),
            Self::Unconfirmed => Err(FleetCatalogError::Unconfirmed),
        }
    }

    fn from_result(result: &Result<FleetStopAcceptance, FleetCatalogError>) -> Self {
        match result {
            Ok(_) => Self::Accepted,
            Err(FleetCatalogError::Conflict) => Self::Conflict,
            Err(FleetCatalogError::PayloadConflict) => Self::PayloadConflict,
            Err(FleetCatalogError::Overloaded) => Self::Overloaded,
            Err(FleetCatalogError::Invalid(_)) => Self::Refused,
            _ => Self::Unconfirmed,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyHead {
    route: NodeRoute,
    nonce: String,
    digest: [u8; 32],
    status: Status,
}

impl ReplyHead {
    fn check(&self, request: &RequestHead) -> Result<Status, TransportError> {
        if self.route != request.route
            || self.nonce != request.nonce
            || self.digest != request.digest
        {
            return Err(TransportError::Replay);
        }
        Ok(self.status)
    }
}

fn check_request(route: &NodeRoute, request: &FleetStopRequest) -> Result<(), TransportError> {
    request
        .selector
        .validate()
        .map_err(|_| TransportError::Identity)?;
    let (_, target) = route.identities()?;
    let boot = &request.selector.boot;
    if boot.catalog != route.catalog()
        || boot.fleet != route.fleet()
        || boot.node != target.identity.node.bytes()
        || boot.disk != target.identity.disk.bytes()
        || boot.process != target.identity.process.bytes()
        || boot.engine != target.storage_instance.bytes()
    {
        return Err(TransportError::Identity);
    }
    Ok(())
}

pub(crate) struct Received {
    pub acceptance: Box<FleetStopAcceptance>,
    _node: Permit,
    _peer: Permit,
}

pub(crate) struct Delivery {
    reply: Option<oneshot::Receiver<Result<Received, FleetCatalogError>>>,
    cancel: watch::Sender<bool>,
}

impl Delivery {
    pub(crate) async fn receive(mut self) -> Result<Received, FleetCatalogError> {
        self.reply
            .take()
            .ok_or(FleetCatalogError::Unconfirmed)?
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?
    }
}

impl Drop for Delivery {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

fn preflight_error(error: TransportError) -> FleetCatalogError {
    match error {
        TransportError::Overloaded => FleetCatalogError::Overloaded,
        TransportError::Closed => FleetCatalogError::Closed,
        TransportError::Limit => FleetCatalogError::Limit,
        // Discovery lag has not invalidated the admitted lifecycle scope.
        // Keep its exact child instead of forcing a new Ready/consent cycle.
        TransportError::StalePeer => {
            FleetCatalogError::Observation(crate::raft::inspection::InspectionError::StalePeer)
        }
        TransportError::Refused | TransportError::Identity => FleetCatalogError::Conflict,
        _ => FleetCatalogError::Unconfirmed,
    }
}

impl RaftTransport {
    pub(crate) fn request_fleet_stop(
        &self,
        request: FleetStopRequest,
        minimum_revision: u64,
        deadline: u64,
    ) -> Result<Delivery, FleetCatalogError> {
        let state = self.handle.0.clone();
        let now = state
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        if deadline <= now || deadline - now > 60_000 {
            return Err(FleetCatalogError::Unconfirmed);
        }
        let target = NodeId::new(request.selector.boot.node)
            .map_err(|_| FleetCatalogError::Invalid("stop target"))?;
        let (peer, route, budget) = state
            .node_metadata_route(target, minimum_revision)
            .map_err(preflight_error)?;
        check_request(&route, &request).map_err(preflight_error)?;
        let origin = state
            .runtime
            .log()
            .lifecycle_guard()
            .map_err(|_| FleetCatalogError::Closed)?
            .ok_or(FleetCatalogError::Invalid("no synced fleet origin boot"))?
            .boot;
        let bytes = serde_json::to_vec(&request)
            .map_err(|_| FleetCatalogError::Invalid("stop encoding"))?;
        if bytes.len() > MAX_BODY {
            return Err(FleetCatalogError::Limit);
        }
        let epoch = state.seed_peer_epoch(&peer).map_err(preflight_error)?;
        let node = state
            .inspection_metadata
            .acquire(META_BYTES)
            .ok_or(FleetCatalogError::Overloaded)?;
        let peer_bytes = budget
            .acquire(META_BYTES)
            .ok_or(FleetCatalogError::Overloaded)?;
        let nonce = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| FleetCatalogError::Closed)?
            + 1;
        let (cancel, mut cancelled) = watch::channel(false);
        let (reply, wait) = oneshot::channel();
        let operation = state.clone();
        state.spawn(async move {
            let result = operation.seed_process_until(&epoch, deadline, async {
                if *cancelled.borrow() { return Err(TransportError::Closed); }
                crate::asyncrt::select_biased! { "stop caller cancellation leaves receiver-owned work intact";
                    _ = cancelled.changed() => Err(TransportError::Closed),
                    result = operation.fleet_stop_exchange(&peer, &route, origin, nonce, deadline, &bytes) => result,
                }
            }).await;
            // After transmission, even a malformed or lost reply cannot prove
            // that the receiver failed to sync its guard. Never retry here.
            let result = match result {
                Ok(status) => status.result().map(|()| Received { acceptance:Box::new(request.acceptance()), _node:node, _peer:peer_bytes }),
                Err(_) => Err(FleetCatalogError::Unconfirmed),
            };
            let _ = reply.send(result);
        }).map_err(preflight_error)?;
        Ok(Delivery {
            reply: Some(wait),
            cancel,
        })
    }
}

impl State {
    async fn fleet_stop_exchange(
        &self,
        peer: &RaftPeerNode,
        route: &NodeRoute,
        origin: BootBinding,
        nonce: u64,
        deadline: u64,
        bytes: &[u8],
    ) -> Result<Status, TransportError> {
        let _connection = self.outgoing_connections.acquire(Lane::Control);
        let tcp = TcpStream::connect(peer.control_address).await?;
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
            max_body: MAX_BODY,
        };
        let request = RequestHead {
            route: route.clone(),
            origin,
            nonce: nonce.to_string(),
            timeout_ms: remaining,
            digest: Sha256::digest(bytes).into(),
        };
        request.check_origin()?;
        let head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, bytes)?;
        write_head(&mut stream, &head).await?;
        stream.write_all(bytes).await?;
        stream.flush().await?;
        let response = read_bulk_head(&mut stream, None, RESPONSE_MAGIC, 0, &self.options).await?;
        let reply: ReplyHead =
            wire.decode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&head.tag))?;
        reply.check(&request)
    }

    pub(super) async fn receive_fleet_stop<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
        connected_at: u64,
    ) -> Result<(), TransportError> {
        let _node = self
            .inspection_metadata
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let head = self
            .bounded(
                None,
                read_bulk_head(stream, Some(prefix), REQUEST_MAGIC, MAX_BODY, &self.options),
            )
            .await?;
        let wire = Wire {
            auth: &self.auth,
            exporter,
            options: &self.options,
            max_body: MAX_BODY,
        };
        let request: RequestHead = wire.decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)?;
        if frame::decimal(&request.nonce)? == 0 || !(1..=60_000).contains(&request.timeout_ms) {
            return Err(TransportError::Protocol);
        }
        let deadline = request.deadline(
            connected_at,
            self.domain.mono_ms().map_err(|_| TransportError::Closed)?,
        )?;
        let authorized =
            self.authorize_node_inspection(&request.route)
                .and_then(|(peer, budget)| {
                    let permit = budget
                        .acquire(META_BYTES)
                        .ok_or(TransportError::Overloaded)?;
                    Ok((peer, permit))
                });
        let (peer, _peer) = match authorized {
            Ok(value) => value,
            Err(error) => {
                let status = if error == TransportError::Overloaded {
                    Status::Overloaded
                } else {
                    Status::Refused
                };
                let reply = ReplyHead {
                    route: request.route,
                    nonce: request.nonce,
                    digest: request.digest,
                    status,
                };
                let response =
                    wire.encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &[])?;
                return self
                    .seed_until(deadline, async {
                        write_head(stream, &response).await?;
                        stream.flush().await?;
                        Ok(())
                    })
                    .await;
            }
        };
        let epoch = self.seed_peer_epoch(&peer)?;
        self.seed_process_until(&epoch, deadline, async {
            let mut bytes = vec![0; head.payload_len];
            stream.read_exact(&mut bytes).await?;
            let stop = request.decode_body(&bytes)?;
            self.authorize_node_inspection(&request.route)?;
            let reporter = crate::asyncrt::services()
                .raft_lifecycle()
                .stop_reporter()
                .map_err(|_| TransportError::Closed)?;
            let status = match reporter {
                Some(reporter) => {
                    Status::from_result(&reporter.submit_peer(stop, request.origin, deadline).await)
                }
                None => Status::Refused,
            };
            let reply = ReplyHead {
                route: request.route,
                nonce: request.nonce,
                digest: request.digest,
                status,
            };
            let response = wire.encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &[])?;
            write_head(stream, &response).await?;
            stream.flush().await?;
            Ok(())
        })
        .await
    }
}
