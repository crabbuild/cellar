//! Explicit wire, queue and connection bounds; control has independent capacity.

use super::transport::TransportError;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct TransportOptions {
    pub max_peers: usize,
    pub max_groups: usize,
    pub max_connections_per_lane: usize,
    pub max_header_bytes: usize,
    pub max_message_bytes: usize,
    pub max_control_bytes: usize,
    pub max_entries: usize,
    pub peer_control_bytes: usize,
    pub peer_data_bytes: usize,
    pub node_control_bytes: usize,
    pub node_data_bytes: usize,
    pub queue_items: usize,
    pub io_timeout_ms: u64,
}

impl Default for TransportOptions {
    fn default() -> Self {
        Self {
            max_peers: 128,
            max_groups: 4096,
            max_connections_per_lane: 128,
            max_header_bytes: 8192,
            max_message_bytes: 20 * 1024 * 1024,
            max_control_bytes: 64 * 1024,
            max_entries: 16_384,
            peer_control_bytes: 1024 * 1024,
            peer_data_bytes: 32 * 1024 * 1024,
            node_control_bytes: 16 * 1024 * 1024,
            node_data_bytes: 128 * 1024 * 1024,
            queue_items: 128,
            io_timeout_ms: 5000,
        }
    }
}

impl TransportOptions {
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        if self.max_peers == 0
            || self.max_peers > 4096
            || self.max_groups == 0
            || self.max_groups > 32_768
            || self.max_connections_per_lane == 0
            || self.max_connections_per_lane > 4096
            || self.max_header_bytes < 1024
            || self.max_header_bytes > 64 * 1024
            || self.max_message_bytes == 0
            || self.max_message_bytes > 64 * 1024 * 1024
            || self.max_control_bytes == 0
            || self.max_control_bytes > 64 * 1024
            || self.max_control_bytes > self.max_message_bytes
            || self.max_entries == 0
            || self.max_entries > 65_536
            || self.queue_items == 0
            || self.queue_items > 4096
            || self.io_timeout_ms == 0
            || self.io_timeout_ms > 60_000
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
