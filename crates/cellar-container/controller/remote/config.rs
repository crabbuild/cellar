// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    RootCertStore,
};
use std::{net::SocketAddr, sync::Arc};

/// Explicit transport budgets, independent of the immutable effect deadline.
#[derive(Clone, Copy, Debug)]
pub struct RemoteOptions {
    pub address: SocketAddr,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub timeout_ms: u64,
}

impl RemoteOptions {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            (1024..=16 * 1024 * 1024).contains(&self.max_frame_bytes)
                && (2..=1024).contains(&self.max_connections)
                && (1..=60_000).contains(&self.timeout_ms),
            "invalid controller transport bounds"
        );
        Ok(())
    }
}

/// A CA-authenticated leaf certificate is additionally authorized for exactly
/// one cluster and namespace on this controller's binding. Claims are metadata,
/// not credentials. Configure separate credentials for other trust scopes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Permission {
    pub certificate_sha256: [u8; 32],
    pub cluster: [u8; 16],
    pub namespace: [u8; 16],
}

/// DER credentials supplied by deployment configuration; never serialized or
/// included in Debug output. Recovery credentials are not part of this API.
pub struct ServerTls {
    pub certificates: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub client_roots: RootCertStore,
    pub permissions: Vec<Permission>,
}

impl ServerTls {
    pub(super) fn build(self) -> Result<(tokio_rustls::TlsAcceptor, Vec<Permission>)> {
        ensure!(
            !self.permissions.is_empty() && self.permissions.len() <= 1024,
            "missing or excessive controller permissions"
        );
        for permission in &self.permissions {
            ensure!(
                permission.certificate_sha256 != [0; 32]
                    && permission.cluster != [0; 16]
                    && permission.namespace != [0; 16],
                "invalid controller permission"
            );
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(self.client_roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(self.certificates, self.key)?;
        Ok((Arc::new(config).into(), self.permissions))
    }
}

/// Both the normal CA/name verification and this exact server certificate pin
/// must pass. Server state identity is separately pinned by RemoteController.
pub struct ClientTls {
    pub certificates: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub server_roots: RootCertStore,
    pub server_name: ServerName<'static>,
    pub server_certificate_sha256: [u8; 32],
}

impl ClientTls {
    pub(super) fn build(
        self,
    ) -> Result<(tokio_rustls::TlsConnector, ServerName<'static>, [u8; 32])> {
        ensure!(
            self.server_certificate_sha256 != [0; 32],
            "missing controller certificate pin"
        );
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(self.server_roots)
        .with_client_auth_cert(self.certificates, self.key)?;
        Ok((
            Arc::new(config).into(),
            self.server_name,
            self.server_certificate_sha256,
        ))
    }
}
