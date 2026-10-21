//! Purpose-separated native seed authorization and transfer. Membership values
//! are data; only a fresh authenticated response can form a local seed grant.

use super::*;
use cellar_logic::consensus::LogIndex;

pub(crate) struct SourceSeed {
    pub snapshot: ::raft::prelude::Snapshot,
    pub barrier: LogIndex,
    pub lease: u64,
}

use super::snapshots::{read_bulk_head, Wire};
use crate::application::consensus::RaftCells;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLJG";
const RESPONSE_MAGIC: &[u8; 4] = b"CLJR";
pub(super) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-raft-seed-1";
const REQUEST_PURPOSE: &[u8] = b"cellar-seed-grant-request-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-seed-grant-response-1";
pub(super) const META_BYTES: usize = 256 * 1024;
const SNAPSHOT_META_BYTES: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedRequest {
    route: Route,
    nonce: String,
    timeout_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedReply {
    route: Route,
    request: [u8; 32],
    nonce: String,
    status: u8,
    barrier: String,
    lease: String,
    digest: [u8; 32],
}

/// Only this module creates the capability, after verifying a fresh response
/// against the current TLS exporter, request tag and exact local engine owner.
pub(crate) struct SeedGrant {
    local: ReplicaIdentity,
    source: RaftPeer,
    snapshot: ::raft::prelude::Snapshot,
    barrier: LogIndex,
    lease: u64,
    deadline: u64,
    owner: Arc<()>,
    domain: DomainToken,
    _metadata: Permit,
}

impl SeedGrant {
    pub(crate) fn snapshot(&self) -> &::raft::prelude::Snapshot {
        &self.snapshot
    }

    pub(crate) fn local(&self) -> &ReplicaIdentity {
        &self.local
    }

    pub(crate) fn source(&self) -> &RaftPeer {
        &self.source
    }

    pub(crate) fn barrier(&self) -> LogIndex {
        self.barrier
    }

    pub(crate) fn lease(&self) -> u64 {
        self.lease
    }

    pub(crate) fn deadline(&self) -> u64 {
        self.deadline
    }

    pub(crate) fn authorized(&self, owner: &Arc<()>, identity: &ReplicaIdentity) -> bool {
        Arc::ptr_eq(&self.owner, owner)
            && self.local == *identity
            && self.domain.mono_ms().is_ok_and(|now| now < self.deadline)
    }
}

impl RaftTransport {
    pub(crate) fn seed_deadline(&self) -> Result<u64, TransportError> {
        let state = &self.handle.0;
        Ok(state
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .saturating_add(state.options.snapshot_timeout_ms))
    }

    /// Retry transport loss with a fresh quorum request each time. The original
    /// deadline includes all network attempts and physical preparation.
    pub(crate) async fn seed_from_peer(
        &self,
        local: ReplicaIdentity,
        source: RaftPeer,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: crate::raft::snapshot::SnapshotLimits,
    ) -> Result<super::snapshots::ReceivedSeed, TransportError> {
        let deadline = self.seed_deadline()?;
        loop {
            let result = async {
                let grant = self
                    .request_seed(local.clone(), source.clone(), schema, deadline)
                    .await?;
                self.fetch_seed(grant, birth, schema, limits).await
            }
            .await;
            match result {
                Err(
                    TransportError::Overloaded
                    | TransportError::Timeout
                    | TransportError::Io(_)
                    | TransportError::StalePeer,
                ) => {
                    let state = &self.handle.0;
                    let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
                    if now >= deadline {
                        return Err(TransportError::Timeout);
                    }
                    state
                        .seed_io(deadline, async {
                            state
                                .domain
                                .sleep_until(now.saturating_add(50).min(deadline))
                                .map_err(|_| TransportError::Closed)?
                                .await;
                            Ok(())
                        })
                        .await?;
                }
                result => return result,
            }
        }
    }

    pub(crate) async fn request_seed(
        &self,
        local: ReplicaIdentity,
        source: RaftPeer,
        schema: [u8; 32],
        deadline: u64,
    ) -> Result<SeedGrant, TransportError> {
        let state = &self.handle.0;
        let options = state.runtime.log().limits();
        if local.node != options.node
            || local.disk != options.disk
            || local.process != options.process
            || local.scope != source.replica.scope
            || local.node == source.replica.node
            || local.member == source.replica.member
        {
            return Err(TransportError::Identity);
        }
        let metadata = state
            .seed_metadata
            .acquire(META_BYTES)
            .ok_or(TransportError::Overloaded)?;
        let now = state.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        let remaining = deadline
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .ok_or(TransportError::Timeout)?;
        let nonce = state
            .sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| TransportError::Closed)?
            .checked_add(1)
            .ok_or(TransportError::Closed)?
            .to_string();
        let request = SeedRequest {
            route: Route::new(
                &local,
                state.auth.source(),
                &source.replica,
                &source.label,
                schema,
            )?,
            nonce,
            timeout_ms: remaining.min(state.options.snapshot_timeout_ms),
        };
        let mut stream = state
            .seed_io(deadline, async {
                let tcp = TcpStream::connect(source.data_address).await?;
                tcp.set_nodelay(true)?;
                let name = rustls::pki_types::ServerName::try_from(source.server_name.clone())
                    .map_err(|_| TransportError::Identity)?;
                tokio_rustls::TlsConnector::from(state.tls.client.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|e| TransportError::Tls(e.to_string()))
            })
            .await?;
        let exporter = stream
            .get_ref()
            .1
            .export_keying_material([0; 32], EXPORT_LABEL, None)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let wire = Wire {
            auth: &state.auth,
            exporter: &exporter,
            options: &state.options,
            max_body: SNAPSHOT_META_BYTES,
        };
        let request_head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
        let (reply, body) = state
            .seed_io(deadline, async {
                write_head(&mut stream, &request_head).await?;
                let head = read_bulk_head(
                    &mut stream,
                    None,
                    RESPONSE_MAGIC,
                    SNAPSHOT_META_BYTES,
                    &state.options,
                )
                .await?;
                let reply: SeedReply = wire.decode(
                    RESPONSE_MAGIC,
                    RESPONSE_PURPOSE,
                    &head,
                    Some(&request_head.tag),
                )?;
                if reply.route != request.route.reversed()
                    || reply.request != request_head.tag
                    || reply.nonce != request.nonce
                {
                    return Err(TransportError::Authentication);
                }
                let mut body = vec![0; head.payload_len];
                stream.read_exact(&mut body).await?;
                if reply.digest != <[u8; 32]>::from(Sha256::digest(&body)) {
                    return Err(TransportError::Authentication);
                }
                Ok((reply, body))
            })
            .await?;
        if reply.status != 0 {
            return Err(if reply.status == 1 {
                TransportError::Overloaded
            } else {
                TransportError::Refused
            });
        }
        let barrier = frame::decimal(&reply.barrier)?;
        let lease = frame::decimal(&reply.lease)?;
        let snapshot = ::raft::prelude::Snapshot::parse_from_bytes(&body)
            .map_err(|_| TransportError::Protocol)?;
        if barrier == 0
            || snapshot.get_metadata().index < barrier
            || snapshot
                .write_to_bytes()
                .map_err(|_| TransportError::Protocol)?
                != body
        {
            return Err(TransportError::Authentication);
        }
        Ok(SeedGrant {
            local,
            source,
            snapshot,
            barrier: LogIndex::new(barrier),
            lease,
            deadline,
            owner: state.runtime.log().owner(),
            domain: state.domain.clone(),
            _metadata: metadata,
        })
    }
}

impl State {
    pub(super) async fn seed_io<T>(
        &self,
        deadline: u64,
        future: impl Future<Output = Result<T, TransportError>>,
    ) -> Result<T, TransportError> {
        let now = self.domain.mono_ms().map_err(|_| TransportError::Closed)?;
        if now >= deadline {
            return Err(TransportError::Timeout);
        }
        let timer = self
            .domain
            .sleep_until(deadline)
            .map_err(|_| TransportError::Closed)?;
        crate::asyncrt::select! {_=timer=>Err(TransportError::Timeout),result=self.bounded(None,future)=>result}
    }

    pub(super) async fn receive_seed<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
    ) -> Result<(), TransportError> {
        let _metadata = self
            .seed_metadata
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
            max_body: SNAPSHOT_META_BYTES,
        };
        let request: SeedRequest = wire.decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)?;
        if frame::decimal(&request.nonce)? == 0
            || request.timeout_ms == 0
            || request.timeout_ms > 300_000
        {
            return Err(TransportError::Protocol);
        }
        self.authorize_route(&request.route)?;
        let (joining, source) = request.route.decode()?;
        let group = self
            .runtime
            .log()
            .group(&source.scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        if !group
            .read()
            .map_err(|e| TransportError::Runtime(e.into()))?
            .conf_state
            .learners
            .contains(&joining.member.get())
        {
            return Err(TransportError::Refused);
        }
        let handle = self
            .query_gateway
            .lock()
            .map_err(|_| TransportError::Closed)?
            .as_ref()
            .and_then(|h| h.upgrade())
            .ok_or(TransportError::Refused)?;
        let gateway = RaftCells { handle };
        let cell = gateway
            .cell(source.scope.namespace, &source.scope.cell)
            .map_err(|_| TransportError::Refused)?;
        if cell.scope() != &source.scope
            || cell.binding.class.schema_digest() != request.route.schema()
        {
            return Err(TransportError::Identity);
        }
        let deadline = self
            .domain
            .mono_ms()
            .map_err(|_| TransportError::Closed)?
            .saturating_add(request.timeout_ms);
        let seed = cell.prepare_seed(
            joining,
            std::time::Duration::from_millis(request.timeout_ms),
        );
        let mut extra = [0; 1];
        let result=self.seed_io(deadline,async {
            crate::asyncrt::select! {result=seed=>Ok(result),read=stream.read(&mut extra)=>{let _=read?;Err(TransportError::Closed)}}
        }).await?;
        self.authorize_route(&request.route)?;
        let (status, barrier, lease, body) = match result {
            Ok(seed) => (
                0,
                seed.barrier.get(),
                seed.lease,
                seed.snapshot
                    .write_to_bytes()
                    .map_err(|_| TransportError::Protocol)?,
            ),
            Err(crate::application::consensus::CommandFailure::Overloaded) => (1, 0, 0, vec![]),
            Err(_) => (2, 0, 0, vec![]),
        };
        let reply = SeedReply {
            route: request.route.reversed(),
            request: head.tag,
            nonce: request.nonce,
            status,
            barrier: barrier.to_string(),
            lease: lease.to_string(),
            digest: Sha256::digest(&body).into(),
        };
        let response = wire.encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &body)?;
        let result = self
            .seed_io(deadline, async {
                write_head(stream, &response).await?;
                stream.write_all(&body).await?;
                stream.flush().await?;
                Ok(())
            })
            .await;
        if result.is_err() && lease != 0 {
            self.finish_seed_source(lease);
        }
        result
    }
}
