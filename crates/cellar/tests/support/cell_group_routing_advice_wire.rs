// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent query-lane framing for a deliberately faulty, owned TLS peer.
use super::*;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

const REQUEST: &[u8] = b"cellar-raft-query-advice-request-1";
const RESPONSE: &[u8] = b"cellar-raft-query-advice-response-1";

#[derive(Clone, Serialize, Deserialize)]
struct Endpoint {
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    label: String,
    member: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity")]
enum Scope {
    Shared {
        group: [[u8; 16]; 4],
        cell: String,
        incarnation: [u8; 16],
    },
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Route {
    pub purpose: String,
    scope: Scope,
    schema: [u8; 32],
    source: Endpoint,
    target: Endpoint,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Request {
    pub route: Route,
    sequence: String,
    digest: [u8; 32],
}

#[derive(Serialize)]
struct Response {
    route: Route,
    sequence: String,
    request: [u8; 32],
    digest: [u8; 32],
}

fn tag(
    exporter: &[u8; 32],
    binding: Option<&[u8; 32]>,
    prefix: &[u8],
    header: &[u8],
) -> Result<[u8; 32]> {
    let mut mac = Hmac::<Sha256>::new_from_slice(&[17; 32])?;
    for part in std::iter::once(if binding.is_some() { RESPONSE } else { REQUEST })
        .chain(std::iter::once(exporter.as_slice()))
        .chain(binding.map(|value| value.as_slice()))
        .chain([prefix, header])
    {
        mac.update(&(part.len() as u64).to_be_bytes());
        mac.update(part);
    }
    Ok(mac.finalize().into_bytes().into())
}

pub(super) fn acceptor() -> Result<tokio_rustls::TlsAcceptor> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_single_cert(
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?;
    config.alpn_protocols = vec![b"cellar-raft-1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

pub(super) struct Wire {
    stream: TlsStream<TcpStream>,
    exporter: [u8; 32],
}

impl Wire {
    pub fn new(stream: TlsStream<TcpStream>) -> Result<Self> {
        let exporter = stream.get_ref().1.export_keying_material(
            [0; 32],
            b"EXPORTER-cellar-raft-query-advice-1",
            None,
        )?;
        Ok(Self { stream, exporter })
    }

    pub async fn receive(&mut self) -> Result<Option<(Request, Vec<u8>, [u8; 32])>> {
        let mut prefix = [0; 20];
        match self.stream.read_exact(&mut prefix).await {
            Ok(_) => (),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        ensure!(&prefix[..4] == b"CLQP" && prefix[4..8] == 1u32.to_be_bytes());
        let header_len = usize::try_from(u32::from_be_bytes(prefix[8..12].try_into()?))?;
        let length = usize::try_from(u64::from_be_bytes(prefix[12..].try_into()?))?;
        ensure!((1..=16 * 1024).contains(&header_len) && length <= 64 * 1024);
        let mut header = vec![0; header_len];
        self.stream.read_exact(&mut header).await?;
        let mut actual = [0; 32];
        self.stream.read_exact(&mut actual).await?;
        let expected = tag(&self.exporter, None, &prefix, &header)?;
        // The fixture verifies an owned process's real request before emitting
        // faulty advice; it does not invent an application invocation.
        ensure!(actual == expected, "query request MAC");
        let request: Request = serde_json::from_slice(&header)?;
        ensure!(
            serde_json::to_vec(&request)? == header,
            "canonical query header"
        );
        let mut body = vec![0; length];
        self.stream.read_exact(&mut body).await?;
        ensure!(request.digest == <[u8; 32]>::from(Sha256::digest(&body)));
        Ok(Some((request, body, actual)))
    }

    pub async fn reply(
        &mut self,
        request: Request,
        binding: [u8; 32],
        body: Vec<u8>,
        fault: Fault,
    ) -> Result<()> {
        let Route {
            purpose,
            scope,
            schema,
            source,
            mut target,
        } = request.route;
        if matches!(fault, Fault::WrongProcess) {
            target.process = [251; 16];
        }
        let header = serde_json::to_vec(&Response {
            route: Route {
                purpose,
                scope,
                schema,
                source: target,
                target: source,
            },
            sequence: request.sequence,
            request: binding,
            digest: Sha256::digest(&body).into(),
        })?;
        let mut prefix = [0; 20];
        prefix[..4].copy_from_slice(b"CLQA");
        prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
        prefix[8..12].copy_from_slice(&u32::try_from(header.len())?.to_be_bytes());
        prefix[12..].copy_from_slice(&u64::try_from(body.len())?.to_be_bytes());
        let mut exporter = self.exporter;
        if matches!(fault, Fault::WrongExporter) {
            exporter[0] ^= 1;
        }
        let mac = tag(&exporter, Some(&binding), &prefix, &header)?;
        for bytes in [prefix.as_slice(), header.as_slice(), &mac, body.as_slice()] {
            self.stream.write_all(bytes).await?;
        }
        self.stream.flush().await?;
        Ok(())
    }
}
