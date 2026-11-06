// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Park acknowledgements use the physical group registry and control lane.
//! A response is bound to one request and its TLS session.

use super::snapshots::{read_bulk_head, Wire};
use super::*;
use crate::raft::cell_groups::{
    GroupParkCut, ParkAcknowledgement, ParkCertificate, SelectedParkCertificate,
    MAX_PARK_NEGOTIATION_MS,
};
use cellar_logic::cell_group::GroupRoute as CellRoute;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLPQ";
const RESPONSE_MAGIC: &[u8; 4] = b"CLPR";
pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-park-1";
const REQUEST_PURPOSE: &[u8] = b"cellar-park-ack-request-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-park-ack-response-1";
const RETRY_MS: u64 = 50;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    route: Route,
    cut: GroupParkCut,
    timeout_ms: u64,
}

impl Request {
    fn deadline(&self, connected_at: u64, now: u64) -> Result<u64, TransportError> {
        if !(1..=MAX_PARK_NEGOTIATION_MS).contains(&self.timeout_ms) || now < connected_at {
            return Err(TransportError::Protocol);
        }
        connected_at
            .checked_add(self.timeout_ms)
            .filter(|deadline| now < *deadline)
            .ok_or(TransportError::Timeout)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status", deny_unknown_fields)]
enum Reply {
    Acknowledged { sequence: u64 },
    Retry,
    Refused,
}

impl Reply {
    fn acknowledgement(self, request: Request) -> Result<ParkAcknowledgement, TransportError> {
        let (leader, voter) = request.route.decode_group()?;
        match self {
            Self::Acknowledged { sequence } if sequence != 0 => Ok(ParkAcknowledgement {
                voter,
                leader: leader.member,
                cut: request.cut,
                sync_sequence: sequence,
            }),
            Self::Acknowledged { .. } => Err(TransportError::Protocol),
            Self::Retry => Err(RuntimeError::NotReady.into()),
            Self::Refused => Err(TransportError::Refused),
        }
    }
}

struct Peer {
    route: RaftPeer<GroupHistory>,
    pool: Arc<Pool>,
}

struct Roster {
    application: CellRoute,
    registration: Arc<GroupRoute<GroupHistory>>,
    local: ReplicaIdentity<GroupHistory>,
    remote: Vec<Peer>,
}

impl RaftTransport {
    /// Collect every voter in the exact current registration. A partial result
    /// cannot reach certificate selection. Cancellation drops the exchange;
    /// already-submitted local journal work settles through the Raft worker.
    pub(crate) async fn collect_park_certificate(
        &self,
        history: GroupHistory,
        cut: GroupParkCut,
        deadline: u64,
    ) -> Result<SelectedParkCertificate, TransportError> {
        let state = self.handle.0.clone();
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if deadline <= now || deadline - now > MAX_PARK_NEGOTIATION_MS {
            return Err(TransportError::Timeout);
        }
        self.reconcile_managed_physical(&history)?;
        let roster = state.park_roster(history, &cut)?;
        let (mut reply, wait) = oneshot::channel();
        let worker = state.clone();
        state.spawn(async move {
            let result = crate::asyncrt::select! {
                _ = reply.closed() => return,
                result = worker.seed_until(deadline, worker.collect_park(roster, cut, deadline)) => result,
            };
            let _ = reply.send(result);
        })?;
        wait.await.map_err(|_| TransportError::Closed)?
    }
}

impl State {
    fn park_roster(
        &self,
        history: GroupHistory,
        cut: &GroupParkCut,
    ) -> Result<Roster, TransportError> {
        let group = self
            .runtime
            .shared()?
            .log()
            .cell_group(&history)
            .map_err(|_| TransportError::UnknownGroup)?;
        let (application, voters) = {
            let cache = group.read().map_err(RuntimeError::from)?;
            if !cache.quiescent_park_cut(cut) {
                return Err(RuntimeError::NotReady.into());
            }
            let marker = cache
                .group_manifest
                .as_ref()
                .ok_or(TransportError::UnknownGroup)?;
            (marker.route().clone(), cache.conf_state.voters.clone())
        };
        let local = group.identity().clone();
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let registration = registry
            .physical_groups
            .get(&history)
            .ok_or(TransportError::UnknownGroup)?
            .clone();
        if registration.schema_digest != application.schema_digest
            || registration.peers.len() != voters.len()
            || !registration.peers.iter().any(|peer| peer.replica == local)
            || !registration
                .peers
                .iter()
                .all(|peer| voters.contains(&peer.replica.member.get()))
        {
            return Err(TransportError::StalePeer);
        }
        let remote = registration
            .peers
            .iter()
            .filter(|peer| peer.replica != local)
            .map(|peer| {
                let pool = registry
                    .pools
                    .get(&peer.replica.node)
                    .filter(|pool| pool.peer.same_host(peer))
                    .ok_or(TransportError::StalePeer)?;
                Ok(Peer {
                    route: peer.clone(),
                    pool: pool.clone(),
                })
            })
            .collect::<Result<_, TransportError>>()?;
        Ok(Roster {
            application,
            registration,
            local,
            remote,
        })
    }

    async fn collect_park(
        &self,
        roster: Roster,
        cut: GroupParkCut,
        deadline: u64,
    ) -> Result<SelectedParkCertificate, TransportError> {
        let runtime = self.runtime.shared()?;
        let history = roster.local.scope;
        let voters: Vec<_> = roster
            .registration
            .peers
            .iter()
            .map(|peer| peer.replica.clone())
            .collect();
        let group = runtime
            .log()
            .cell_group(&history)
            .map_err(RuntimeError::from)?;
        let prior = group
            .read()
            .map_err(RuntimeError::from)?
            .local_park
            .as_ref()
            .and_then(|park| park.certificate())
            .cloned();
        let certificate = if let Some(prior) = prior {
            // A lost reply can be retried without substituting new sync
            // sequences for an already durable certificate.
            if prior.cut() != &cut
                || !prior.matches_roster(&roster.application, &roster.local, &voters)
            {
                return Err(TransportError::StalePeer);
            }
            prior
        } else {
            let local = self
                .retry_park(deadline, || async {
                    Ok(runtime
                        .acknowledge_park(history, roster.local.member, cut.clone())
                        .await?)
                })
                .await?;
            let mut acknowledgements = futures_util::future::try_join_all(
                roster
                    .remote
                    .iter()
                    .map(|peer| self.request_park_ack(&roster, peer, &cut, deadline)),
            )
            .await?;
            acknowledgements.push(local);
            ParkCertificate::assemble(
                &roster.application,
                cut,
                &roster.local,
                &voters,
                acknowledgements,
            )
            .map_err(RuntimeError::from)?
        };
        // Resolve the roster again after the exchanges. A replaced transport
        // registration or physical process invalidates the entire selection.
        let current = self.park_roster(history, certificate.cut())?;
        if !Arc::ptr_eq(&current.registration, &roster.registration)
            || current.local != roster.local
            || current.application != roster.application
        {
            return Err(TransportError::StalePeer);
        }
        let selected = self
            .retry_park(deadline, || async {
                Ok(runtime.certify_park(history, certificate.clone()).await?)
            })
            .await?;
        // The leader may have synced while membership discovery was refreshed.
        // A lost selection reply is safe and the negotiation can expire.
        if !Arc::ptr_eq(
            &self
                .park_roster(history, selected.certificate().cut())?
                .registration,
            &roster.registration,
        ) {
            return Err(TransportError::StalePeer);
        }
        Ok(selected)
    }

    fn park_charge(&self) -> usize {
        2 * (self.options.max_header_bytes + frame::PREFIX + frame::TAG) + 1024
    }

    async fn request_park_ack(
        &self,
        roster: &Roster,
        peer: &Peer,
        cut: &GroupParkCut,
        deadline: u64,
    ) -> Result<ParkAcknowledgement, TransportError> {
        let wire_route = Route::new_group(
            &roster.local,
            self.auth.source(),
            &peer.route.replica,
            &peer.route.label,
            roster.application.schema_digest,
        )?;
        self.retry_park(deadline, || async {
            if !self.current_physical(&roster.registration, &peer.route) {
                return Err(TransportError::StalePeer);
            }
            let result = self
                .bounded(
                    Some(&peer.pool),
                    self.park_exchange(peer, &wire_route, cut, deadline),
                )
                .await?;
            if !self.current_physical(&roster.registration, &peer.route) {
                return Err(TransportError::StalePeer);
            }
            Ok(result)
        })
        .await
    }

    async fn retry_park<T, F: Future<Output = Result<T, TransportError>>>(
        &self,
        deadline: u64,
        mut attempt: impl FnMut() -> F,
    ) -> Result<T, TransportError> {
        loop {
            let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
            if now >= deadline {
                return Err(TransportError::Timeout);
            }
            match attempt().await {
                Err(
                    TransportError::Runtime(RuntimeError::NotReady | RuntimeError::Overloaded)
                    | TransportError::Overloaded,
                ) => {
                    let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
                    self.domain
                        .sleep_until(now.saturating_add(RETRY_MS).min(deadline))
                        .map_err(|_| TransportError::Closed)?
                        .await;
                }
                result => return result,
            }
        }
    }

    async fn park_exchange(
        &self,
        peer: &Peer,
        route: &Route,
        cut: &GroupParkCut,
        deadline: u64,
    ) -> Result<ParkAcknowledgement, TransportError> {
        let charge = self.park_charge();
        let _node = self
            .outgoing
            .control
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let _peer = peer
            .pool
            .outgoing
            .control
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        let _connection = self.outgoing_connections.acquire(Lane::Control);
        let tcp = TcpStream::connect(peer.route.control_address).await?;
        tcp.set_nodelay(true)?;
        let tcp = self.observed_socket(tcp, metrics::Lane::Control);
        let name = rustls::pki_types::ServerName::try_from(peer.route.server_name.clone())
            .map_err(|_| TransportError::Identity)?;
        let mut stream = tokio_rustls::TlsConnector::from(self.tls.client.clone())
            .connect(name, tcp)
            .await
            .map_err(|error| TransportError::Tls(error.to_string()))?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], EXPORT_LABEL, None)
            .map_err(|error| TransportError::Tls(error.to_string()))?;
        let timeout_ms = deadline
            .checked_sub(self.domain.mono_ms().map_err(|_| TransportError::Closed)?)
            .filter(|remaining| *remaining > 0)
            .ok_or(TransportError::Timeout)?;
        let request = Request {
            route: route.clone(),
            cut: cut.clone(),
            timeout_ms,
        };
        let wire = Wire {
            auth: &self.auth,
            exporter: &exporter,
            options: &self.options,
            max_body: 0,
        };
        let head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
        write_head(&mut stream, &head).await?;
        stream.flush().await?;
        let response = read_bulk_head(&mut stream, None, RESPONSE_MAGIC, 0, &self.options).await?;
        let reply: Reply =
            wire.decode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&head.tag))?;
        reply.acknowledgement(request)
    }

    pub(super) async fn receive_park_ack<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
        connected_at: u64,
    ) -> Result<(), TransportError> {
        let charge = self.park_charge();
        let _node = self
            .incoming
            .control
            .acquire(charge)
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
            max_body: 0,
        };
        let request: Request = wire.decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)?;
        let deadline = request.deadline(
            connected_at,
            self.domain.mono_ms().map_err(|_| TransportError::Closed)?,
        )?;
        let (leader, voter) = request.route.decode_group()?;
        let pool = self.authorize_physical(&request.route)?;
        let _peer = pool
            .incoming
            .control
            .acquire(charge)
            .ok_or(TransportError::Overloaded)?;
        self.seed_until(
            deadline,
            self.bounded(Some(&pool), async {
                let response = match self
                    .runtime
                    .shared()?
                    .acknowledge_park(voter.scope, leader.member, request.cut.clone())
                    .await
                {
                    Ok(receipt)
                        if receipt.voter == voter
                            && receipt.leader == leader.member
                            && receipt.cut == request.cut
                            && receipt.sync_sequence != 0 =>
                    {
                        Reply::Acknowledged {
                            sequence: receipt.sync_sequence,
                        }
                    }
                    Ok(_) => return Err(TransportError::Protocol),
                    Err(RuntimeError::NotReady | RuntimeError::Overloaded) => Reply::Retry,
                    Err(_) => Reply::Refused,
                };
                if !Arc::ptr_eq(&self.authorize_physical(&request.route)?, &pool) {
                    return Err(TransportError::StalePeer);
                }
                let response =
                    wire.encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &response, &[])?;
                write_head(stream, &response).await?;
                stream.flush().await?;
                Ok(())
            }),
        )
        .await
    }
}

#[cfg(test)]
#[path = "park_transport_tests.rs"]
mod tests;
