// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pinned infrastructure signatures for permanently fenced group histories.
use crate::raft::{
    catalog::DisasterFenceAuthority, fleet_catalog::FleetTransitionRecoveryFenceRequest,
};
use anyhow::{ensure, Context as _, Result};
use futures_util::future::BoxFuture;

use super::CellGroupDisasterFenceRequest;

const BODY_LEN: usize = 1 + 32 + 32;
const CERTIFICATE_LEN: usize = BODY_LEN + 64;

/// Accept an already-issued permanent fence receipt from a trusted controller.
/// Supports both one shared group and an interrupted managed component.
///
/// The controller must permanently revoke *all* old launch/restart authority,
/// stop every possible old writer (including obsolete hosts and effect workers),
/// and confirm completion before signing. Its authoritative inventory may exceed
/// the request's minimum voter roster. A signature cannot establish those facts
/// by itself: pin the controller's public key independently of S3 and voters.
/// This adapter performs no fencing I/O and supplies no permissive default.
///
/// Receipts are exactly 129 bytes: format `1`, request digest (32 bytes), SHA-256
/// commitment to the controller's complete fenced inventory (32 bytes), and an
/// Ed25519 signature (64 bytes) over [`Self::PURPOSE`] followed by the first 65
/// receipt bytes. The inventory commitment is opaque to Cellar; the controller
/// retains its inventory for audit. No previous receipt format is accepted.
#[derive(Clone)]
pub struct CellGroupPermanentFenceAuthority {
    public: ed25519_dalek::VerifyingKey,
    certificate: Option<Vec<u8>>,
}

impl CellGroupPermanentFenceAuthority {
    pub const PURPOSE: &'static [u8] = b"cellar-cell-group-permanent-fence-1\0";

    /// Pin a controller key for independent receipt verification. Without an
    /// explicitly supplied certificate, `fence` refuses to attest completion.
    pub fn new(public_key: [u8; 32]) -> Result<Self> {
        let public = ed25519_dalek::VerifyingKey::from_bytes(&public_key)
            .context("invalid permanent fence controller key")?;
        ensure!(!public.is_weak(), "weak permanent fence controller key");
        Ok(Self {
            public,
            certificate: None,
        })
    }

    /// Supply a pre-issued receipt after the controller has completed fencing.
    pub fn with_certificate(mut self, certificate: Vec<u8>) -> Result<Self> {
        self.check(None, &certificate)?;
        self.certificate = Some(certificate);
        Ok(self)
    }

    /// Independently verify winning receipt bytes, including exact request scope.
    /// A concurrent controller receipt need not equal the locally supplied one.
    pub fn verify_receipt(&self, request_digest: [u8; 32], evidence: &[u8]) -> Result<()> {
        self.check(Some(request_digest), evidence)
    }

    fn check(&self, request_digest: Option<[u8; 32]>, evidence: &[u8]) -> Result<()> {
        ensure!(
            evidence.len() == CERTIFICATE_LEN && evidence[0] == 1,
            "missing or invalid permanent fence certificate"
        );
        ensure!(
            evidence[1..33] != [0; 32]
                && evidence[33..BODY_LEN] != [0; 32]
                && request_digest.is_none_or(|digest| evidence[1..33] == digest),
            "foreign or incomplete permanent fence certificate"
        );
        let mut message = Vec::with_capacity(Self::PURPOSE.len() + BODY_LEN);
        message.extend_from_slice(Self::PURPOSE);
        message.extend_from_slice(&evidence[..BODY_LEN]);
        self.public
            .verify_strict(
                &message,
                &ed25519_dalek::Signature::from_slice(&evidence[BODY_LEN..])?,
            )
            .context("untrusted permanent fence certificate")
    }

    fn evidence(&self, digest: [u8; 32]) -> Result<Vec<u8>> {
        let certificate = self
            .certificate
            .as_ref()
            .context("missing permanent fence certificate")?;
        self.verify_receipt(digest, certificate)?;
        Ok(certificate.clone())
    }
}

impl DisasterFenceAuthority<CellGroupDisasterFenceRequest> for CellGroupPermanentFenceAuthority {
    fn fence<'a>(
        &'a self,
        request: &'a CellGroupDisasterFenceRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.evidence(request.digest()) })
    }

    fn verify<'a>(
        &'a self,
        request: &'a CellGroupDisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_receipt(request.digest(), evidence) })
    }
}

impl DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest>
    for CellGroupPermanentFenceAuthority
{
    fn fence<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.evidence(request.digest()) })
    }

    fn verify<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.verify_receipt(request.digest(), evidence) })
    }
}
