// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A run owns its CA, bounded leaf cache and TLS sessions. Only the public CA
//! is copied into the container; key generation uses the execution domain RNG.

use anyhow::{ensure, Context as _, Result};
use p256::elliptic_curve::zeroize::Zeroizing;
use p256::pkcs8::EncodePrivateKey as _;
use rand::RngCore as _;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::{
    pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    ServerConfig,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

const MAX_IDENTITIES: usize = 64;

pub(super) struct Authority {
    // A weak origin check avoids a host -> container -> authority -> host cycle.
    origin: Box<dyn Fn() -> bool + Send + Sync>,
    issuer: Issuer<'static, KeyPair>,
    certificate: CertificateDer<'static>,
    pem: String,
    not_before: time::OffsetDateTime,
    not_after: time::OffsetDateTime,
    cache: Mutex<VecDeque<(String, Arc<ServerConfig>)>>,
}

impl Authority {
    pub async fn new() -> Result<Arc<Self>> {
        crate::asyncrt::try_blocking(|| {
            let now = now()?;
            let not_before = now
                .checked_sub(time::Duration::days(1))
                .context("CA validity underflow")?;
            let not_after = now
                .checked_add(time::Duration::days(365))
                .context("CA validity overflow")?;
            let mut params = CertificateParams::new(Vec::<String>::new())?;
            params.not_before = not_before;
            params.not_after = not_after;
            params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
            params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            params.distinguished_name = DistinguishedName::new();
            params
                .distinguished_name
                .push(DnType::CommonName, "Cellar ephemeral container CA");
            let key = key_pair("container-https-authority")?;
            let certificate = params.self_signed(&key)?;
            let origin = Arc::downgrade(&crate::asyncrt::services());
            Ok(Arc::new(Self {
                origin: Box::new(move || {
                    origin.upgrade().is_some_and(|origin| {
                        Arc::ptr_eq(&origin, &crate::asyncrt::services()) && !origin.is_closed()
                    })
                }),
                certificate: certificate.der().clone(),
                pem: certificate.pem(),
                issuer: Issuer::new(params, key),
                not_before,
                not_after,
                cache: Mutex::new(VecDeque::new()),
            }))
        })?
        .await?
    }

    /// Identity is the canonical SNI, or the destination address for a client
    /// without SNI. The HTTP Host is subsequently required to match it.
    pub async fn server(self: &Arc<Self>, identity: String) -> Result<Arc<ServerConfig>> {
        ensure!(
            (self.origin)(),
            "TLS authority belongs to another or closed host"
        );
        let authority = self.clone();
        crate::asyncrt::try_blocking(move || {
            ensure!(
                identity.len() <= 253,
                "TLS identity exceeds the routing limit"
            );
            let now = now()?;
            ensure!(
                now >= authority.not_before && now < authority.not_after,
                "container CA has expired; start a new run"
            );
            {
                let mut cache = authority.cache.lock().unwrap();
                if let Some(index) = cache.iter().position(|(name, _)| *name == identity) {
                    let entry = cache.remove(index).unwrap();
                    let config = entry.1.clone();
                    cache.push_back(entry);
                    return Ok(config);
                }
            }
            let key = key_pair("container-https-endpoint")?;
            let mut params = CertificateParams::new(vec![identity.clone()])?;
            params.not_before = authority.not_before;
            params.not_after = authority.not_after;
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
            params.use_authority_key_identifier_extension = true;
            let certificate = params.signed_by(&key, &authority.issuer)?;
            let mut config = ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.der().clone(), authority.certificate.clone()],
                PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )?;
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let config = Arc::new(config);
            let mut cache = authority.cache.lock().unwrap();
            // Concurrent misses may finish together. Keep a single bounded
            // entry; accepted TLS sessions retain their own configuration.
            if let Some((_, existing)) = cache.iter().find(|(name, _)| *name == identity) {
                return Ok(existing.clone());
            }
            if cache.len() == MAX_IDENTITIES {
                cache.pop_front();
            }
            cache.push_back((identity, config.clone()));
            Ok(config)
        })?
        .await?
    }

    #[cfg(test)]
    pub(super) fn certificate(&self) -> CertificateDer<'static> {
        self.certificate.clone()
    }

    pub fn archive(&self) -> Result<Vec<u8>> {
        let mut archive = tar::Builder::new(Vec::new());
        for path in ["etc/", "etc/cloudflare/", "etc/cloudflare/certs/"] {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_cksum();
            archive.append_data(&mut header, path, std::io::empty())?;
        }
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o444);
        header.set_size(self.pem.len() as u64);
        header.set_cksum();
        archive.append_data(
            &mut header,
            super::super::HTTPS_CA_PATH.trim_start_matches('/'),
            self.pem.as_bytes(),
        )?;
        Ok(archive.into_inner()?)
    }
}

#[cfg(test)]
#[path = "https_tests.rs"]
mod tests;

fn now() -> Result<time::OffsetDateTime> {
    Ok(time::OffsetDateTime::from_unix_timestamp(
        crate::asyncrt::wall_ms().div_euclid(1000),
    )?)
}

fn key_pair(consumer: &'static str) -> Result<KeyPair> {
    // Rejection sampling is bounded and fallible, including domain closure.
    // SecretKey/PKCS8 documents own zeroizing storage after construction.
    let mut rng = crate::asyncrt::rng(consumer);
    for _ in 0..16 {
        let mut seed = Zeroizing::new([0; 32]);
        rng.try_fill_bytes(&mut seed[..])?;
        let key = p256::SecretKey::from_slice(&seed[..]);
        if let Ok(key) = key {
            let document = key.to_pkcs8_der()?;
            return Ok(KeyPair::from_pkcs8_der_and_sign_algo(
                &PrivatePkcs8KeyDer::from(document.as_bytes()),
                &rcgen::PKCS_ECDSA_P256_SHA256,
            )?);
        }
    }
    anyhow::bail!("TLS key generation exceeded its retry bound")
}
