// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]

use super::*;
use crate::sdk::ExecutionDomain;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::test]
async fn run_authorities_are_distinct_public_archives_and_leaf_cache_are_bounded() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let first = Authority::new().await?;
            let other = Authority::new().await?;
            assert_ne!(first.certificate, other.certificate);
            let bytes = first.archive()?;
            assert!(bytes.len() <= 16384);
            assert!(!String::from_utf8_lossy(&bytes).contains("PRIVATE KEY"));
            let mut archive = tar::Archive::new(std::io::Cursor::new(bytes));
            let mut files = 0;
            for entry in archive.entries()? {
                let mut entry = entry?;
                if entry.header().entry_type().is_file() {
                    use std::io::Read as _;
                    files += 1;
                    assert_eq!(
                        entry.path()?.to_str().unwrap(),
                        super::super::super::HTTPS_CA_PATH.trim_start_matches('/')
                    );
                    assert_eq!(entry.header().mode()?, 0o444);
                    let mut pem = String::new();
                    entry.read_to_string(&mut pem)?;
                    assert_eq!(pem, first.pem);
                }
            }
            assert_eq!(files, 1);
            let accepted = first.server("first.test".into()).await?;
            assert!(Arc::ptr_eq(
                &accepted,
                &first.server("first.test".into()).await?
            ));
            for index in 0..MAX_IDENTITIES {
                first.server(format!("host-{index}.test")).await?;
            }
            assert_eq!(first.cache.lock().unwrap().len(), MAX_IDENTITIES);
            assert!(!first
                .cache
                .lock()
                .unwrap()
                .iter()
                .any(|(name, _)| name == "first.test"));
            Ok::<_, anyhow::Error>(first)
        })
        .await;
    domain.shutdown().await;
    let authority = result??;
    let replacement = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = replacement
        .run(async {
            assert!(authority.server("first.test".into()).await.is_err());
        })
        .await;
    replacement.shutdown().await;
    result?;
    Ok(())
}

#[tokio::test]
async fn trusted_tls_round_trip_validates_name_and_refuses_another_runs_ca() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let authority = Authority::new().await?;
            let other = Authority::new().await?;
            let server = authority.server("route.test".into()).await?;
            // The accepted configuration must continue to authenticate correctly
            // even after newer identities evict it from the bounded cache.
            for index in 0..MAX_IDENTITIES {
                authority.server(format!("other-{index}.test")).await?;
            }
            assert_eq!(authority.cache.lock().unwrap().len(), MAX_IDENTITIES);
            assert!(!authority
                .cache
                .lock()
                .unwrap()
                .iter()
                .any(|(name, _)| name == "route.test"));
            for (certificate, name, success) in [
                (authority.certificate.clone(), "route.test", true),
                (authority.certificate.clone(), "wrong.test", false),
                (other.certificate.clone(), "route.test", false),
            ] {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(certificate)?;
                let config = super::super::super::HttpsInterceptionOptions::new(roots)?.upstream;
                let (client, peer) = tokio::io::duplex(16384);
                let acceptor = tokio_rustls::TlsAcceptor::from(server.clone());
                let serving = crate::asyncrt::spawn(async move {
                    let mut stream = acceptor.accept(peer).await?;
                    let mut bytes = [0; 4];
                    stream.read_exact(&mut bytes).await?;
                    stream.write_all(&bytes).await?;
                    Ok::<_, anyhow::Error>(())
                });
                let connected = tokio_rustls::TlsConnector::from(config)
                    .connect(
                        rustls::pki_types::ServerName::try_from(name.to_owned())?,
                        client,
                    )
                    .await;
                assert_eq!(connected.is_ok(), success);
                if let Ok(mut stream) = connected {
                    stream.write_all(&[0, 255, 1, 42]).await?;
                    let mut bytes = [0; 4];
                    stream.read_exact(&mut bytes).await?;
                    assert_eq!(bytes, [0, 255, 1, 42]);
                }
                assert_eq!(serving.await?.is_ok(), success);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}
