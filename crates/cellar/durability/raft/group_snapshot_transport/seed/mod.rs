// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pull one authenticated complete-group seed into a non-serving staged member.
use super::*;
use crate::raft::{
    cache::{GroupLog, GroupWrite, SnapshotChange},
    catalog::{CatalogError, CellGroupBirth, CellGroupBootstrapPermit, RangeDirectory},
    cell_groups::{checkpoint, CellGroupSeedAuthority, GroupManifest, PreparedGroupManifest},
    storage::{CellGroupBootstrap, LogError, ReplicaLog},
};
use cellar_logic::consensus::{LogIndex, OperationId};
mod client;
mod grant;
mod install;
mod server;
mod work;
pub(crate) use grant::Grant;

pub(in crate::raft::transport) const REQUEST_MAGIC: &[u8; 4] = b"CLGJ";
pub(in crate::raft::transport) const EXPORT_LABEL: &[u8] = b"EXPORTER-cellar-group-seed-1";
const REPLY_MAGIC: &[u8; 4] = b"CLGK";
const READY_MAGIC: &[u8; 4] = b"CLGL";
const CHUNK_MAGIC: &[u8; 4] = b"CLGM";
const REQUEST: &[u8] = b"cellar-group-seed-request-1";
const REPLY: &[u8] = b"cellar-group-seed-grant-1";
const READY: &[u8] = b"cellar-group-seed-ready-1";
const CHUNK: &[u8] = b"cellar-group-seed-chunk-1";
const META_BYTES: usize = 64 * 1024;

type Stream = tokio_rustls::client::TlsStream<metrics::Socket<TcpStream>>;
type Installed = (CellGroupBootstrapPermit, PreparedGroupManifest);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    route: Route,
    nonce: String,
    timeout_ms: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    route: Route,
    request: [u8; 32],
    nonce: String,
    barrier: String,
    digest: [u8; 32],
}

#[derive(Debug)]
pub enum CellGroupSeedError {
    Transport(TransportError),
    Catalog(CatalogError),
    Log(LogError),
    Checkpoint(CellGroupCheckpointError),
}

impl std::fmt::Display for CellGroupSeedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cell-group seed: {self:?}")
    }
}

impl std::error::Error for CellGroupSeedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Transport(e) => e,
            Self::Catalog(e) => e,
            Self::Log(e) => e,
            Self::Checkpoint(e) => e,
        })
    }
}

impl From<TransportError> for CellGroupSeedError {
    fn from(e: TransportError) -> Self {
        Self::Transport(e)
    }
}

impl From<CatalogError> for CellGroupSeedError {
    fn from(e: CatalogError) -> Self {
        Self::Catalog(e)
    }
}

impl From<LogError> for CellGroupSeedError {
    fn from(e: LogError) -> Self {
        Self::Log(e)
    }
}

impl From<CellGroupCheckpointError> for CellGroupSeedError {
    fn from(e: CellGroupCheckpointError) -> Self {
        Self::Checkpoint(e)
    }
}

/// A staged, non-serving destination. Dropping it before install closes the
/// exchange; retry with a fresh grant and the same permanent engine/member IDs.
/// Recover the staged group with `CellGroupApplier::recover` before install so
/// the selected base and its physical retention owner are established, even for
/// an empty genesis. Once install is accepted, cancellation does not abandon
/// queued engine work.
/// An unknown result must be reconciled through bootstrap_joined_group_member.
pub struct CellGroupSeedSession {
    group: GroupLog<GroupHistory>,
    command: oneshot::Sender<CellGroupApplier>,
    result: oneshot::Receiver<Result<Installed, CellGroupSeedError>>,
}

impl CellGroupSeedSession {
    pub fn group(&self) -> &GroupLog<GroupHistory> {
        &self.group
    }

    /// Use the same stores on retry. Only a complete authenticated End, verified
    /// physical closure and actual engine fence can return the startup permit.
    pub async fn install(self, applier: CellGroupApplier) -> Result<Installed, CellGroupSeedError> {
        self.command
            .send(applier)
            .map_err(|_| TransportError::Closed)?;
        self.result.await.map_err(|_| TransportError::Closed)?
    }
}

struct Lease(Arc<AtomicBool>);

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct ReceiveAdmission {
    _wire: Permit,
    _transfer: Permit,
}

struct Exchange {
    state: Arc<State>,
    stream: Stream,
    exporter: [u8; 32],
    binding: [u8; 32],
    grant: Arc<Grant>,
    group: GroupLog<GroupHistory>,
    lease: Arc<Lease>,
    directory: RangeDirectory,
    birth: CellGroupBirth,
    pool: CellGroupCheckpointPool,
    admission: Arc<ReceiveAdmission>,
    _connection: resources::ConnectionOwner,
    operation: OperationId,
}
