// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit wire, queue and connection bounds; control has independent capacity.

use super::transport::TransportError;
use cellar_logic::consensus::{ClusterId, DiskLineage, NamespaceId, NodeId, ProcessGeneration};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;

/// A physical peer endpoint shared by several groups. It identifies no voting
/// or learner member; each consensus message retains its own group identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftPeerNode {
    pub node: NodeId,
    pub disk: DiskLineage,
    pub process: ProcessGeneration,
    pub label: String,
    pub server_name: String,
    pub control_address: std::net::SocketAddr,
    pub data_address: std::net::SocketAddr,
    pub query_address: std::net::SocketAddr,
}

/// Trusted namespace read permission for an exact node/disk/process endpoint.
/// Discovery does not create this grant; it never authorizes consensus messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceQueryOrigin {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub revision: u64,
    pub peer: RaftPeerNode,
}

/// Explicit command, enrollment and receipt-inspection permission for an exact namespace/process.
/// Read grants, discovery and Raft membership never create this permission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceCommandOrigin {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub revision: u64,
    pub peer: RaftPeerNode,
}

/// Explicit namespace inspection permission for a physical node process.
/// Query grants and directory observations cannot create this permission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceInspectionOrigin {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub revision: u64,
    pub peer: RaftPeerNode,
}

#[derive(Clone, Debug)]
pub struct TransportOptions {
    pub max_peers: usize,
    pub max_groups: usize,
    pub max_query_origins: usize,
    pub max_command_origins: usize,
    pub max_connections_per_lane: usize,
    pub max_header_bytes: usize,
    pub max_message_bytes: usize,
    pub max_control_bytes: usize,
    pub max_query_request_bytes: usize,
    pub max_query_response_bytes: usize,
    pub peer_query_bytes: usize,
    pub node_query_bytes: usize,
    pub max_entries: usize,
    pub peer_control_bytes: usize,
    pub peer_data_bytes: usize,
    pub node_control_bytes: usize,
    pub node_data_bytes: usize,
    pub queue_items: usize,
    pub io_timeout_ms: u64,
    /// Whole-transfer deadline, independent of the per-I/O idle deadline.
    pub snapshot_timeout_ms: u64,
    pub max_snapshot_bytes: usize,
    /// Logical image quota; memory admission covers only bounded codec/wire buffers.
    pub max_snapshot_image_bytes: u64,
    /// Complete S3 recovery set plus containing-bundle read reservations.
    pub max_seed_recovery_bytes: usize,
    pub snapshot_chunk_bytes: usize,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            max_peers: 128,
            max_groups: 4096,
            max_query_origins: 4096,
            max_command_origins: 4096,
            max_connections_per_lane: 128,
            max_header_bytes: 8192,
            max_message_bytes: 20 * 1024 * 1024,
            max_control_bytes: 64 * 1024,
            max_query_request_bytes: 2 * 1024 * 1024,
            max_query_response_bytes: 128 * 1024,
            peer_query_bytes: 8 * 1024 * 1024,
            node_query_bytes: 64 * 1024 * 1024,
            max_entries: 16_384,
            peer_control_bytes: 1024 * 1024,
            peer_data_bytes: 32 * 1024 * 1024,
            node_control_bytes: 16 * 1024 * 1024,
            node_data_bytes: 128 * 1024 * 1024,
            queue_items: 128,
            io_timeout_ms: 5000,
            snapshot_timeout_ms: 30_000,
            max_snapshot_bytes: 128 * 1024 * 1024,
            max_snapshot_image_bytes: 64 * 1024 * 1024,
            max_seed_recovery_bytes: 256 * 1024 * 1024
                + super::archive::RECOVERY_BUNDLE_READ_BYTES
                + super::archive::physical_read_bytes(),
            snapshot_chunk_bytes: 1024 * 1024,
        }
    }
}

impl TransportOptions {
    pub(crate) fn snapshot_limits(
        &self,
        image_bytes: u64,
    ) -> Result<super::snapshot::SnapshotLimits, TransportError> {
        if image_bytes > self.max_snapshot_image_bytes {
            return Err(TransportError::Invalid(
                "snapshot image exceeds configured quota".into(),
            ));
        }
        let limits = super::snapshot::SnapshotLimits {
            max_image_bytes: image_bytes,
            max_encoded_bytes: self.max_snapshot_bytes,
            max_chunk_bytes: self.snapshot_chunk_bytes,
        };
        limits
            .validate()
            .map_err(|e| TransportError::Invalid(e.to_string()))?;
        Ok(limits)
    }

    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        if self.max_peers == 0
            || self.max_peers > 4096
            || self.max_groups == 0
            || self.max_groups > 32_768
            || self.max_command_origins == 0
            || self.max_command_origins > 65_536
            || self.max_query_origins == 0
            || self.max_query_origins > 65_536
            || self.max_connections_per_lane == 0
            || self.max_connections_per_lane > 4096
            || self.max_header_bytes < 1024
            || self.max_header_bytes > 64 * 1024
            || self.max_message_bytes == 0
            || self.max_message_bytes > 64 * 1024 * 1024
            || self.max_control_bytes == 0
            || self.max_control_bytes > 64 * 1024
            || self.max_control_bytes > self.max_message_bytes
            || !(4096..=64 * 1024 * 1024).contains(&self.max_query_request_bytes)
            || !(4096..=8 * 1024 * 1024).contains(&self.max_query_response_bytes)
            || self.max_entries == 0
            || self.max_entries > 65_536
            || self.queue_items == 0
            || self.queue_items > 4096
            || self.io_timeout_ms == 0
            || self.io_timeout_ms > 60_000
            || !(1..=3_600_000).contains(&self.snapshot_timeout_ms)
            || !(512..=cellar_ltx::stream_snapshot::MAX_ENCODED_BYTES as usize)
                .contains(&self.max_snapshot_bytes)
            || !(512..=cellar_ltx::stream_snapshot::MAX_IMAGE_BYTES)
                .contains(&self.max_snapshot_image_bytes)
            || !(1..=super::archive::MAX_RECOVERY_READ_BYTES)
                .contains(&self.max_seed_recovery_bytes)
            || !(1..=1024 * 1024).contains(&self.snapshot_chunk_bytes)
            || self.snapshot_chunk_bytes > self.max_snapshot_bytes
        {
            return Err(TransportError::Invalid("invalid transport limits".into()));
        }
        let overhead = self.max_header_bytes + 52;
        for (peer, node, frame) in [
            (
                self.peer_control_bytes,
                self.node_control_bytes,
                self.max_control_bytes,
            ),
            (
                self.peer_data_bytes,
                self.node_data_bytes,
                self.max_message_bytes,
            ),
        ] {
            if peer < frame + overhead || node < peer || node > 1024 * 1024 * 1024 {
                return Err(TransportError::Invalid(
                    "invalid transport byte budgets".into(),
                ));
            }
        }
        let query_reservation = 2 * self.max_query_request_bytes
            + 2 * self.max_query_response_bytes
            + 2 * (self.max_header_bytes + 52);
        if self.peer_query_bytes < query_reservation
            || self.node_query_bytes < self.peer_query_bytes
            || self.node_query_bytes > 1024 * 1024 * 1024
        {
            return Err(TransportError::Invalid("invalid query byte budgets".into()));
        }
        Ok(())
    }
}

/// Explicit trust roots and server credentials. There is no plaintext or
/// certificate-verification bypass. Raft identities are authenticated separately
/// with channel-bound MACs; possession of any trusted server certificate alone
/// does not authorize a member, group, process or response.
#[derive(Clone)]
pub struct RaftTls {
    pub(crate) client: Arc<rustls::ClientConfig>,
    pub(crate) server: Arc<rustls::ServerConfig>,
}

impl RaftTls {
    pub fn new(
        trust: Vec<CertificateDer<'static>>,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self, TransportError> {
        if trust.is_empty() || trust.len() > 128 || chain.is_empty() || chain.len() > 16 {
            return Err(TransportError::Invalid(
                "invalid TLS certificate count".into(),
            ));
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        for cert in trust {
            roots
                .add(cert)
                .map_err(|e| TransportError::Tls(e.to_string()))?;
        }
        // TLS 1.3 and no early data: every frame uses a completed handshake's
        // normal exporter, including on resumed connections.
        let mut client = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.enable_early_data = false;
        client.alpn_protocols = vec![b"cellar-raft-1".to_vec()];
        let mut server = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        server.max_early_data_size = 0;
        server.alpn_protocols = client.alpn_protocols.clone();
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}
