// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real authenticated socket to the independent protocol controller. The
//! controlled provider remains in-process and supplies no native compute proof.
use super::*;
use cellar_container::controller::remote::*;
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
use rustls::{pki_types::PrivatePkcs8KeyDer, RootCertStore};

pub(super) async fn start(
    domain: Arc<cellar::sdk::ExecutionDomain>,
    controller: Arc<ExecutionController>,
    history: &GroupHistory,
) -> Result<(ControllerServer, ControllerProvider)> {
    let mut ca = CertificateParams::new(Vec::<String>::new())?;
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate()?)?;
    let server_key = KeyPair::generate()?;
    let server_certificate = CertificateParams::new(vec!["localhost".into()])?
        .signed_by(&server_key, &ca)?
        .der()
        .clone();
    let client_key = KeyPair::generate()?;
    let client_certificate = CertificateParams::new(vec!["cellar-host".into()])?
        .signed_by(&client_key, &ca)?
        .der()
        .clone();
    let mut roots = RootCertStore::empty();
    roots.add(ca.der().clone())?;
    let options = RemoteOptions {
        address: "127.0.0.1:0".parse()?,
        max_connections: 16,
        max_frame_bytes: 1024 * 1024,
        timeout_ms: 5000,
    };
    let server = ControllerServer::bind(
        domain.clone(),
        controller.clone(),
        options,
        ServerTls {
            certificates: vec![server_certificate.clone()],
            key: PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
            client_roots: roots.clone(),
            permissions: vec![Permission {
                certificate_sha256: sha2::Sha256::digest(client_certificate.as_ref()).into(),
                cluster: history.cluster.bytes(),
                namespace: history.namespace.bytes(),
            }],
        },
    )
    .await?;
    let client = RemoteController::new(
        domain,
        RemoteOptions {
            address: server.address(),
            ..options
        },
        controller.identity(),
        controller.options().limits,
        ClientTls {
            certificates: vec![client_certificate],
            key: PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
            server_roots: roots,
            server_name: "localhost".try_into()?,
            server_certificate_sha256: sha2::Sha256::digest(server_certificate.as_ref()).into(),
        },
    )?;
    Ok((server, ControllerProvider::from_endpoint(client)?))
}
