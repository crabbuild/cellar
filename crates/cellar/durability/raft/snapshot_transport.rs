// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Member-only snapshot pulls on dedicated data connections. A lease pins one
//! exact immutable cut; no transfer response grants install or voting authority.

use super::*;
use crate::raft::completion::ApplyTicket;
use crate::raft::snapshot::{PreparedSnapshot, SnapshotDownload, SnapshotLimits, SnapshotPackage};
use cellar_logic::consensus::MemberId;
use sha2::{Digest, Sha256};
use std::sync::{atomic::AtomicBool, Weak};

#[path = "snapshot_receive.rs"]
mod receive;

#[cfg(test)]
#[path = "snapshot_codec_tests.rs"]
mod codec_tests;

pub(super) const REQUEST_MAGIC: &[u8; 4] = b"CLSP";
const RESPONSE_MAGIC: &[u8; 4] = b"CLSA";
const REQUEST_PURPOSE: &[u8] = b"cellar-snapshot-pull-1";
const RESPONSE_PURPOSE: &[u8] = b"cellar-snapshot-chunk-1";

pub(super) fn snapshot_error(
    error: impl Into<crate::raft::snapshot::SnapshotError>,
) -> TransportError {
    let error = error.into();
    if error.scratch_full() {
        TransportError::Overloaded
    } else {
        TransportError::Invalid(error.to_string())
    }
}

pub(in crate::raft) fn memory_bound(options: &TransportOptions) -> usize {
    // File-backed bodies and indexes do not reserve image-sized memory. Cover
    // wire framing/copies plus codec/hash/compression and independent sealing
    // buffers. One transfer retains this reservation through blocking work.
    4 * options.snapshot_chunk_bytes + io_profile(options).max_working_bytes().max(4 * 1024 * 1024)
}

fn io_profile(options: &TransportOptions) -> cellar_ltx::stream_snapshot::IoProfile {
    // Large-cell transports admit the larger codec allowance before capture
    // or download. This changes physical batching, never the encoded format.
    if options.max_snapshot_image_bytes >= 256 * 1024 * 1024 {
        cellar_ltx::stream_snapshot::IoProfile::Buffered
    } else {
        cellar_ltx::stream_snapshot::IoProfile::Compact
    }
}

struct StopPrepared(crate::raft::snapshot::prepared::Cache);

impl Drop for StopPrepared {
    fn drop(&mut self) {
        self.0.stop();
    }
}

pub(super) fn start_prepared(
    state: &Arc<State>,
    owner: crate::raft::snapshot::prepared::Owner,
) -> Result<(), TransportError> {
    let physical = state.reserve_task()?;
    let observer = state.reserve_task()?;
    let mut stop = state.stop.subscribe();
    let abort = StopPrepared(state.prepared_snapshot.clone());
    let (done, stopped) = tokio::sync::oneshot::channel();
    state
        .domain
        .blocking(move || {
            let _guard = physical;
            owner.run();
            let _ = done.send(());
        })
        .map_err(|_| TransportError::Closed)?
        .detach();
    state.spawn_reserved(observer, async move {
        // Cancellation before the first poll must still wake the physical
        // owner. Files close there even when source handles remain unpolled.
        let _abort = abort;
        if *stop.borrow() {
            return;
        }
        crate::asyncrt::select! { _ = stop.changed() => (), _ = stopped => (), }
    })
}

pub(crate) struct SourceReservation {
    _permit: Permit,
    owner: Arc<Budget>,
    io_profile: cellar_ltx::stream_snapshot::IoProfile,
}

impl SourceReservation {
    pub(crate) fn io_profile(&self) -> cellar_ltx::stream_snapshot::IoProfile {
        self.io_profile
    }
}

pub(super) struct Source {
    id: u64,
    route: Arc<GroupRoute>,
    peer: RaftPeer,
    package: SnapshotPackage,
    digest: [u8; 32],
    expires: u64,
    finished: AtomicBool,
    _reservation: SourceReservation,
}

pub(crate) struct SnapshotSource(Arc<Source>);

struct ActiveSource {
    source: Arc<Source>,
    pool: Arc<Pool>,
}

impl Drop for ActiveSource {
    fn drop(&mut self) {
        self.source.finished.store(true, Ordering::Release);
    }
}

impl SnapshotSource {
    pub(crate) fn targets(&self, scope: &GroupScope, member: MemberId) -> bool {
        self.0.peer.replica.scope == *scope && self.0.peer.replica.member == member
    }

    pub(crate) fn cancel(self) {
        self.0.finished.store(true, Ordering::Release);
    }

    pub fn id(&self) -> u64 {
        self.0.id
    }

    pub fn finished(&self, now: u64) -> bool {
        self.0.finished.load(Ordering::Acquire) || now >= self.0.expires
    }

    pub fn matches(
        &self,
        scope: &GroupScope,
        member: u64,
        snapshot: &::raft::prelude::Snapshot,
    ) -> bool {
        self.0.peer.replica.scope == *scope
            && self.0.peer.replica.member.get() == member
            && self.0.package.snapshot() == *snapshot
    }
}

/// The reservation follows the buffer onto the blocking SQL owner, through
/// verification/sealing and durable publication or pre-authorization discard.
pub(crate) struct ReceivedSnapshot {
    receiver: receive::Received<(PreparedSnapshot, Permit)>,
}

impl ReceivedSnapshot {
    pub fn finish(
        self,
        images: &cellar_ltx::generation::GenerationStore,
    ) -> Result<(PreparedSnapshot, Permit), crate::raft::snapshot::SnapshotError> {
        self.receiver.seal(images)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pull {
    route: Route,
    snapshot: [u8; 32],
    transfer: String,
    lease: String,
    offset: String,
    length: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Chunk {
    route: Route,
    request: [u8; 32],
    snapshot: [u8; 32],
    transfer: String,
    offset: String,
    digest: [u8; 32],
}

impl RaftTransport {
    pub(crate) fn prepared_snapshot_cache(&self) -> crate::raft::snapshot::prepared::Cache {
        self.handle.0.prepared_snapshot.clone()
    }

    pub(crate) fn snapshot_limits(
        &self,
        image_bytes: u64,
    ) -> Result<SnapshotLimits, TransportError> {
        self.handle.0.options.snapshot_limits(image_bytes)
    }

    /// Reserve peak memory before source capture/encoding on the blocking owner.
    pub(crate) fn reserve_snapshot_source(&self) -> Result<SourceReservation, TransportError> {
        let state = &self.handle.0;
        let registry = state.registry.lock().map_err(|_| TransportError::Closed)?;
        state.admit(&registry)?;
        drop(registry);
        state
            .snapshot_source_budget
            .acquire(memory_bound(&state.options))
            .map(|permit| SourceReservation {
                _permit: permit,
                owner: state.snapshot_source_budget.clone(),
                io_profile: io_profile(&state.options),
            })
            .ok_or(TransportError::Overloaded)
    }

    pub(crate) fn serve_snapshot(
        &self,
        scope: &GroupScope,
        member: u64,
        package: SnapshotPackage,
        reservation: SourceReservation,
    ) -> Result<SnapshotSource, TransportError> {
        let state = &self.handle.0;
        if !Arc::ptr_eq(&reservation.owner, &state.snapshot_source_budget) {
            return Err(TransportError::Identity);
        }
        let (route, peer, _) = state.snapshot_peer(scope, member)?;
        if package.checkpoint_material().scope() != scope
            || package.encoded_length() > state.options.max_snapshot_bytes
        {
            return Err(TransportError::Identity);
        }
        let source = Arc::new(Source {
            id: state
                .sequence
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
                .map_err(|_| TransportError::Closed)?
                + 1,
            route,
            peer,
            digest: snapshot_digest(&package.snapshot())?,
            package,
            expires: state
                .domain
                .mono_ms()
                .map_err(|_| TransportError::Closed)?
                .saturating_add(state.options.snapshot_timeout_ms),
            finished: AtomicBool::new(false),
            _reservation: reservation,
        });
        // The host owns the body and expiry. The registry cannot extend either
        // lifetime, including after its host's close/drop.
        *state
            .snapshot_source
            .lock()
            .map_err(|_| TransportError::Closed)? = Some(Arc::downgrade(&source));
        Ok(SnapshotSource(source))
    }

    pub(crate) async fn fetch_snapshot(
        &self,
        ticket: &ApplyTicket,
        source: u64,
        snapshot: ::raft::prelude::Snapshot,
        limits: SnapshotLimits,
    ) -> Result<ReceivedSnapshot, TransportError> {
        let state = &self.handle.0;
        let scope = &ticket.replica().scope;
        let group = state
            .runtime
            .log()
            .group(scope)
            .map_err(|_| TransportError::UnknownGroup)?;
        if ticket.replica() != group.identity()
            || snapshot_digest(&snapshot)? != ticket.entry_digest()
        {
            return Err(TransportError::Identity);
        }
        let (route, peer, pool) = state.snapshot_peer(scope, source)?;
        let schema = route.schema_digest;
        let download_group = group.clone();
        let reservation = state
            .snapshot_receive_budget
            .acquire(memory_bound(&state.options))
            .ok_or(TransportError::Overloaded)?;
        let profile = io_profile(&state.options);
        let prepared = state.prepared_snapshot.clone();
        let (download, reservation) = state
            .domain
            .blocking(move || {
                prepared.trim_idle()?;
                let download = SnapshotDownload::new(&download_group, schema, snapshot, limits)?
                    .with_io_profile(profile);
                Ok::<_, crate::raft::snapshot::SnapshotError>((download, reservation))
            })
            .map_err(|_| TransportError::Closed)?
            .await
            .map_err(|_| TransportError::Closed)?
            .map_err(snapshot_error)?;
        if download.encoded_length() > state.options.max_snapshot_bytes {
            return Err(TransportError::Limit);
        }
        let total = download.encoded_length();
        let receiver = receive::collect(state, download, reservation, |send| async move {
            // Configuration-only snapshots contain no bytes and need no socket.
            if total == 0 {
                return Ok(());
            }
            let deadline = state
                .domain
                .mono_ms()
                .map_err(|_| TransportError::Closed)?
                .saturating_add(state.options.snapshot_timeout_ms);
            let wire_route = Route::new(
                group.identity(),
                state.auth.source(),
                &peer.replica,
                &peer.label,
                route.schema_digest,
            )?;
            let _connection = state.outgoing_connections.acquire(Lane::Data);
            let mut stream = state
                .snapshot_io(&pool, deadline, async {
                    let tcp = TcpStream::connect(peer.data_address).await?;
                    tcp.set_nodelay(true)?;
                    let tcp = state.observed_socket(tcp, metrics::Lane::Data);
                    let name = rustls::pki_types::ServerName::try_from(peer.server_name.clone())
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
                .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
                .map_err(|e| TransportError::Tls(e.to_string()))?;
            let transfer = state
                .sequence
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
                .map_err(|_| TransportError::Closed)?
                .checked_add(1)
                .ok_or(TransportError::Closed)?
                .to_string();
            let mut offset = 0;
            let wire = Wire {
                auth: &state.auth,
                exporter: &exporter,
                options: &state.options,
                max_body: state.options.snapshot_chunk_bytes,
            };
            while offset < total {
                let slot = send.reserve().await.map_err(|_| TransportError::Closed)?;
                if !state.current_route(scope, &route, &peer) {
                    return Err(TransportError::StalePeer);
                }
                let length = (total - offset)
                    .min(state.options.snapshot_chunk_bytes)
                    .min(limits.max_chunk_bytes);
                let request = Pull {
                    route: wire_route.clone(),
                    snapshot: ticket.entry_digest(),
                    transfer: transfer.clone(),
                    lease: "0".into(),
                    offset: offset.to_string(),
                    length,
                };
                let received = state
                    .snapshot_io(&pool, deadline, async {
                        let request_head =
                            wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
                        write_head(&mut stream, &request_head).await?;
                        let head = read_bulk_head(
                            &mut stream,
                            None,
                            RESPONSE_MAGIC,
                            length,
                            &state.options,
                        )
                        .await?;
                        let response: Chunk = wire.decode(
                            RESPONSE_MAGIC,
                            RESPONSE_PURPOSE,
                            &head,
                            Some(&request_head.tag),
                        )?;
                        if head.payload_len == 0
                            || response.request != request_head.tag
                            || response.route != request.route.reversed()
                            || response.snapshot != request.snapshot
                            || response.transfer != request.transfer
                            || response.offset != request.offset
                        {
                            return Err(TransportError::Authentication);
                        }
                        let mut body = vec![0; head.payload_len];
                        stream.read_exact(&mut body).await?;
                        if response.digest != <[u8; 32]>::from(Sha256::digest(&body)) {
                            return Err(TransportError::Authentication);
                        }
                        Ok(body)
                    })
                    .await?;
                let received_length = received.len();
                state.performance.increment(
                    crate::raft::performance::Counter::RecoveryPeerReceivedBytes,
                    received_length as u64,
                );
                slot.send(receive::Chunk {
                    offset,
                    bytes: received,
                    queued: state
                        .performance
                        .timer(crate::raft::performance::Operation::RecoveryAppendQueue),
                })?;
                offset += received_length;
            }
            if !state.current_route(scope, &route, &peer) {
                return Err(TransportError::StalePeer);
            }
            Ok(())
        })
        .await?;
        Ok(ReceivedSnapshot { receiver })
    }
}

impl State {
    pub(super) fn invalidate_snapshot_routes(&self) {
        let source = self
            .snapshot_source
            .lock()
            .ok()
            .and_then(|source| source.as_ref().and_then(Weak::upgrade));
        if let Some(source) = source {
            if !self.current_route(&source.peer.replica.scope, &source.route, &source.peer) {
                source.finished.store(true, Ordering::Release);
            }
        }
    }

    pub(super) fn finish_seed_source(&self, id: u64) {
        if let Ok(source) = self.snapshot_source.lock() {
            if let Some(source) = source
                .as_ref()
                .and_then(Weak::upgrade)
                .filter(|source| source.id == id)
            {
                source.finished.store(true, Ordering::Release);
            }
        }
    }

    fn snapshot_peer(
        &self,
        scope: &GroupScope,
        member: u64,
    ) -> Result<(Arc<GroupRoute>, RaftPeer, Arc<Pool>), TransportError> {
        let registry = self.registry.lock().map_err(|_| TransportError::Closed)?;
        self.admit(&registry)?;
        let route = registry
            .groups
            .get(scope)
            .ok_or(TransportError::UnknownGroup)?
            .clone();
        let peer = route
            .peers
            .iter()
            .find(|peer| peer.replica.member.get() == member)
            .ok_or(TransportError::StalePeer)?
            .clone();
        let pool = registry
            .pools
            .get(&peer.replica.node)
            .ok_or(TransportError::StalePeer)?
            .clone();
        drop(registry);
        if !self.current_route(scope, &route, &peer) {
            return Err(TransportError::StalePeer);
        }
        Ok((route, peer, pool))
    }

    async fn snapshot_io<T>(
        &self,
        pool: &Pool,
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
        crate::asyncrt::select! {
            _ = timer => Err(TransportError::Timeout),
            result = self.bounded(Some(pool), future) => result,
        }
    }

    pub(super) async fn receive_snapshot<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        exporter: &[u8; 32],
        prefix: [u8; frame::PREFIX],
    ) -> Result<(), TransportError> {
        let _slot = self
            .snapshot_connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportError::Overloaded)?;
        let mut first = Some(prefix);
        let mut offset = 0;
        let mut encoded_hash = Sha256::new();
        let mut transfer = None;
        let mut selected: Option<ActiveSource> = None;
        loop {
            let reading = read_bulk_head(stream, first.take(), REQUEST_MAGIC, 0, &self.options);
            let head = match &selected {
                Some(selected) => {
                    self.snapshot_io(&selected.pool, selected.source.expires, reading)
                        .await?
                }
                None => self.bounded(None, reading).await?,
            };
            let wire = Wire {
                auth: &self.auth,
                exporter,
                options: &self.options,
                max_body: self.options.snapshot_chunk_bytes,
            };
            let request: Pull = wire.decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)?;
            let pool = self.authorize_route(&request.route)?;
            let lease = self
                .snapshot_source
                .lock()
                .map_err(|_| TransportError::Closed)?
                .as_ref()
                .and_then(Weak::upgrade)
                .ok_or(TransportError::Refused)?;
            let (recipient, source) = request.route.decode()?;
            if selected
                .as_ref()
                .is_some_and(|old| !Arc::ptr_eq(&old.source, &lease))
                || lease.finished.load(Ordering::Acquire)
                || (frame::decimal(&request.lease)? != 0
                    && frame::decimal(&request.lease)? != lease.id)
                || lease.peer.replica != recipient
                || source
                    != *self
                        .runtime
                        .log()
                        .group(&source.scope)
                        .map_err(|_| TransportError::UnknownGroup)?
                        .identity()
                || request.snapshot != lease.digest
                || frame::decimal(&request.offset)? != offset as u64
                || request.length == 0
                || request.length > 1024 * 1024
                || frame::decimal(&request.transfer)? == 0
                || transfer
                    .as_ref()
                    .is_some_and(|old| *old != request.transfer)
                || !self.current_route(&source.scope, &lease.route, &lease.peer)
            {
                return Err(TransportError::Identity);
            }
            transfer.get_or_insert(request.transfer.clone());
            selected.get_or_insert_with(|| ActiveSource {
                source: lease.clone(),
                pool: pool.clone(),
            });
            let chunk_source = lease.clone();
            let length = request.length.min(self.options.snapshot_chunk_bytes);
            let body = self
                .domain
                .blocking(move || chunk_source.package.chunk(offset, length))
                .map_err(|_| TransportError::Closed)?
                .await
                .map_err(|_| TransportError::Closed)?
                .map_err(snapshot_error)?;
            encoded_hash.update(&body);
            if offset + body.len() == lease.package.encoded_length() {
                lease
                    .package
                    .validate_encoded_hash(encoded_hash.clone().finalize().into())
                    .map_err(snapshot_error)?;
            }
            let response = Chunk {
                route: request.route.reversed(),
                request: head.tag,
                snapshot: request.snapshot,
                transfer: request.transfer,
                offset: request.offset,
                digest: Sha256::digest(&body).into(),
            };
            let response_head = wire.encode(
                RESPONSE_MAGIC,
                RESPONSE_PURPOSE,
                &head.tag,
                &response,
                &body,
            )?;
            self.snapshot_io(&pool, lease.expires, async {
                write_head(stream, &response_head).await?;
                stream.write_all(&body).await?;
                stream.flush().await?;
                Ok(())
            })
            .await?;
            offset += body.len();
            if offset == lease.package.encoded_length() {
                lease.finished.store(true, Ordering::Release);
                return Ok(());
            }
        }
    }
}

fn snapshot_digest(snapshot: &::raft::prelude::Snapshot) -> Result<[u8; 32], TransportError> {
    Ok(Sha256::digest(
        snapshot
            .write_to_bytes()
            .map_err(|_| TransportError::Protocol)?,
    )
    .into())
}

pub(super) struct Wire<'a> {
    pub(super) auth: &'a PeerAuth,
    pub(super) exporter: &'a [u8; 32],
    pub(super) options: &'a TransportOptions,
    pub(super) max_body: usize,
}

impl Wire<'_> {
    pub(super) fn encode<T: Serialize>(
        &self,
        magic: &[u8; 4],
        purpose: &[u8],
        request: &[u8],
        value: &T,
        body: &[u8],
    ) -> Result<FrameHead, TransportError> {
        let header = serde_json::to_vec(value).map_err(|_| TransportError::Protocol)?;
        if header.is_empty()
            || header.len() > self.options.max_header_bytes
            || body.len() > self.max_body
        {
            return Err(TransportError::Limit);
        }
        let mut prefix = [0; frame::PREFIX];
        prefix[..4].copy_from_slice(magic);
        prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
        prefix[8..12].copy_from_slice(&(header.len() as u32).to_be_bytes());
        prefix[12..].copy_from_slice(&(body.len() as u64).to_be_bytes());
        let tag = self
            .auth
            .raft_tag(purpose, &[self.exporter, request, &prefix, &header]);
        Ok(FrameHead {
            prefix,
            header,
            tag,
            payload_len: body.len(),
        })
    }

    pub(super) fn decode<T: for<'de> Deserialize<'de>>(
        &self,
        magic: &[u8; 4],
        purpose: &[u8],
        head: &FrameHead,
        request: Option<&[u8; 32]>,
    ) -> Result<T, TransportError> {
        let request = request.map_or(&[][..], |request| &request[..]);
        if &head.prefix[..4] != magic
            || !self.auth.verify_raft_tag(
                purpose,
                &[self.exporter, request, &head.prefix, &head.header],
                &head.tag,
            )
        {
            return Err(TransportError::Authentication);
        }
        serde_json::from_slice(&head.header).map_err(|_| TransportError::Protocol)
    }
}

pub(super) async fn read_bulk_head<S: AsyncRead + Unpin>(
    stream: &mut S,
    prefix: Option<[u8; frame::PREFIX]>,
    magic: &[u8; 4],
    max_payload: usize,
    options: &TransportOptions,
) -> Result<FrameHead, TransportError> {
    let prefix = match prefix {
        Some(prefix) => prefix,
        None => {
            let mut prefix = [0; frame::PREFIX];
            stream.read_exact(&mut prefix).await?;
            prefix
        }
    };
    if &prefix[..4] != magic || prefix[4..8] != 1u32.to_be_bytes() {
        return Err(TransportError::Protocol);
    }
    let header_len = u32::from_be_bytes(
        prefix[8..12]
            .try_into()
            .map_err(|_| TransportError::Protocol)?,
    ) as usize;
    let payload_len = u64::from_be_bytes(
        prefix[12..]
            .try_into()
            .map_err(|_| TransportError::Protocol)?,
    );
    if header_len == 0 || header_len > options.max_header_bytes || payload_len > max_payload as u64
    {
        return Err(TransportError::Limit);
    }
    let mut header = vec![0; header_len];
    stream.read_exact(&mut header).await?;
    let mut tag = [0; frame::TAG];
    stream.read_exact(&mut tag).await?;
    Ok(FrameHead {
        prefix,
        header,
        tag,
        payload_len: payload_len as usize,
    })
}

pub(crate) struct ReceivedSeed {
    scope: GroupScope,
    receiver: receive::Received<crate::raft::snapshot::PreparedSeed>,
}

impl ReceivedSeed {
    pub(crate) fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub(crate) fn finish(
        self,
        images: &cellar_ltx::generation::GenerationStore,
    ) -> Result<crate::raft::snapshot::PreparedSeed, crate::raft::snapshot::SnapshotError> {
        self.receiver.seal(images)
    }
}

impl RaftTransport {
    pub(crate) async fn fetch_seed(
        &self,
        grant: super::seeds::SeedGrant,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<ReceivedSeed, TransportError> {
        self.handle
            .0
            .performance
            .measure(
                crate::raft::performance::Operation::RecoveryPeerFetch,
                self.fetch_seed_inner(grant, birth, schema, limits),
            )
            .await
    }

    async fn fetch_seed_inner(
        &self,
        grant: super::seeds::SeedGrant,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<ReceivedSeed, TransportError> {
        let state = &self.handle.0;
        if !grant.authorized(&state.runtime.log().owner(), grant.local()) {
            return Err(TransportError::Identity);
        }
        let deadline = grant.deadline();
        let process = grant.process_epoch().clone();
        let peer = grant.source().clone();
        let lease = grant.lease();
        let digest = snapshot_digest(grant.snapshot())?;
        let wire_route = Route::new(
            grant.local(),
            state.auth.source(),
            &peer.replica,
            &peer.label,
            schema,
        )?;
        let scratch_directory = state.runtime.log().limits().directory.clone();
        let snapshot_scratch = state.runtime.log().snapshot_scratch();
        let prepared = state.prepared_snapshot.clone();
        let birth = birth.clone();
        let reservation = state
            .snapshot_receive_budget
            .acquire(memory_bound(&state.options))
            .ok_or(TransportError::Overloaded)?;
        let profile = io_profile(&state.options);
        let (download, reservation) = state
            .domain
            .blocking(move || {
                prepared.trim_idle()?;
                let download = crate::raft::snapshot::SeedDownload::new(
                    grant,
                    &birth,
                    schema,
                    limits,
                    crate::asyncrt::fs().as_ref(),
                    &scratch_directory,
                    &snapshot_scratch,
                )?
                .with_io_profile(profile);
                Ok::<_, crate::raft::snapshot::SnapshotError>((download, reservation))
            })
            .map_err(|_| TransportError::Closed)?
            .await
            .map_err(|_| TransportError::Closed)?
            .map_err(snapshot_error)?;
        let total = download.encoded_length();
        let scope = download.scope().clone();
        if total > state.options.max_snapshot_bytes || (total == 0) != (lease == 0) {
            return Err(TransportError::Limit);
        }
        let receiver = receive::collect(state, download, reservation, |send| async move {
            if total == 0 {
                return Ok(());
            }
            let _connection = state.outgoing_connections.acquire(Lane::Data);
            let mut stream = state
                .seed_process_io(&process, deadline, async {
                    let tcp = TcpStream::connect(peer.data_address).await?;
                    tcp.set_nodelay(true)?;
                    let tcp = state.observed_socket(tcp, metrics::Lane::Data);
                    let name = rustls::pki_types::ServerName::try_from(peer.server_name.clone())
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
                .export_keying_material([0; 32], frame::EXPORT_LABEL, None)
                .map_err(|e| TransportError::Tls(e.to_string()))?;
            let wire = Wire {
                auth: &state.auth,
                exporter: &exporter,
                options: &state.options,
                max_body: state.options.snapshot_chunk_bytes,
            };
            let transfer = state
                .sequence
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
                .map_err(|_| TransportError::Closed)?
                .checked_add(1)
                .ok_or(TransportError::Closed)?
                .to_string();
            let mut offset = 0;
            while offset < total {
                let slot = send.reserve().await.map_err(|_| TransportError::Closed)?;
                let length = (total - offset)
                    .min(state.options.snapshot_chunk_bytes)
                    .min(limits.max_chunk_bytes);
                let request = Pull {
                    route: wire_route.clone(),
                    snapshot: digest,
                    transfer: transfer.clone(),
                    lease: lease.to_string(),
                    offset: offset.to_string(),
                    length,
                };
                let received = state
                    .performance
                    .measure(
                        crate::raft::performance::Operation::RecoveryPeerReceive,
                        state.seed_process_io(&process, deadline, async {
                            let request_head =
                                wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
                            write_head(&mut stream, &request_head).await?;
                            let head = read_bulk_head(
                                &mut stream,
                                None,
                                RESPONSE_MAGIC,
                                length,
                                &state.options,
                            )
                            .await?;
                            let response: Chunk = wire.decode(
                                RESPONSE_MAGIC,
                                RESPONSE_PURPOSE,
                                &head,
                                Some(&request_head.tag),
                            )?;
                            if head.payload_len == 0
                                || response.request != request_head.tag
                                || response.route != request.route.reversed()
                                || response.snapshot != request.snapshot
                                || response.transfer != request.transfer
                                || response.offset != request.offset
                            {
                                return Err(TransportError::Authentication);
                            }
                            let mut body = vec![0; head.payload_len];
                            stream.read_exact(&mut body).await?;
                            if response.digest != <[u8; 32]>::from(Sha256::digest(&body)) {
                                return Err(TransportError::Authentication);
                            }
                            Ok(body)
                        }),
                    )
                    .await?;
                let received_length = received.len();
                state.performance.increment(
                    crate::raft::performance::Counter::RecoveryPeerReceivedBytes,
                    received_length as u64,
                );
                slot.send(receive::Chunk {
                    offset,
                    bytes: received,
                    queued: state
                        .performance
                        .timer(crate::raft::performance::Operation::RecoveryAppendQueue),
                })?;
                offset += received_length;
            }
            Ok(())
        })
        .await?;
        Ok(ReceivedSeed { scope, receiver })
    }
}
