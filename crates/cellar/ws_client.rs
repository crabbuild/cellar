// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

// The real peer WebSocket transport is outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Outbound WebSocket client.
//!
//! cellar serves WebSockets with fastwebsockets, and connects with it too. That
//! matters most on the peer hop: a frame relayed from a client to the cell's
//! owner is framed, masked, and closed by one implementation on both sides
//! rather than translated between two.
//!
//! What this owns that a client library would have owned: the TLS setup, the
//! handshake headers, and the non-101 response. The last one is why
//! `fastwebsockets::handshake::client` is not called directly -- it discards
//! the response body, and `new WebSocket()` reports a declined upgrade with it.

use anyhow::anyhow;
use anyhow::Context;
use base64::Engine;
use fastwebsockets::Role;
use fastwebsockets::WebSocket;
use http_body_util::BodyExt;
use http_body_util::Empty;
use hyper::header::HeaderMap;
use hyper::header::HeaderValue;
use hyper::header::CONNECTION;
use hyper::header::HOST;
use hyper::header::SEC_WEBSOCKET_ACCEPT;
use hyper::header::SEC_WEBSOCKET_KEY;
use hyper::header::SEC_WEBSOCKET_VERSION;
use hyper::header::UPGRADE;
use hyper::upgrade::Upgraded;
use hyper::Request;
use hyper::StatusCode;
use hyper_util::rt::TokioIo;
use sha1::Digest;
use sha1::Sha1;
use std::sync::OnceLock;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

/// A server response that was not a 101.
pub struct Declined {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

pub enum Error {
    /// The server answered and refused the upgrade. Callers read this: a 401
    /// revokes managed credentials, a stale-route header triggers a
    /// redispatch, and the isolate surfaces the whole thing to `onerror`.
    Declined(Box<Declined>),
    /// No answer to read -- DNS, TCP, TLS, or a malformed handshake.
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::Failed(error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Declined(declined) => {
                write!(f, "server refused the upgrade: {}", declined.status)
            }
            Error::Failed(error) => write!(f, "{error}"),
        }
    }
}

pub struct Connection {
    pub socket: WebSocket<TokioIo<Upgraded>>,
    /// The 101's headers: the negotiated subprotocol, and on the peer hop the
    /// signature the caller verifies before trusting the tunnel.
    pub headers: HeaderMap,
}

struct HandshakeDriver(tokio::task::JoinHandle<()>);
impl Drop for HandshakeDriver {
    fn drop(&mut self) {
        // Until hyper hands us the upgraded transport, this task owns the
        // peer. A cancelled/failed opening must not leave that peer alive.
        self.0.abort();
    }
}

/// Mozilla's roots, the same set the previous client compiled in. Deliberately
/// not the platform store: a downloaded cellar should reach `wss://` on a host
/// that has no `/etc/ssl/certs`.
fn tls_config() -> &'static tokio_rustls::TlsConnector {
    static CONFIG: OnceLock<tokio_rustls::TlsConnector> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
    })
}

/// RFC 6455's proof the server read the key rather than blindly answering 101.
fn accept_key(key: &str) -> String {
    let mut digest = Sha1::new();
    digest.update(key.as_bytes());
    digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(digest.finalize())
}

/// Open a WebSocket to `url`, sending `extra` alongside the handshake headers.
///
/// The caller owns the timeout: nothing here bounds how long a server may take
/// to answer.
pub async fn connect(url: &str, extra: HeaderMap) -> Result<Connection, Error> {
    connect_with_tls(url, extra, || tls_config().clone()).await
}

// A connector factory also lets the host tests verify TLS with a local CA;
// the public/application path always uses Mozilla's roots above.
async fn connect_with_tls(
    url: &str,
    extra: HeaderMap,
    connector: impl FnOnce() -> tokio_rustls::TlsConnector,
) -> Result<Connection, Error> {
    let url = url::Url::parse(url).context("parse WebSocket URL")?;
    // `fetch("http://…", { headers: { Upgrade: "websocket" } })` is the
    // Workers idiom for an outbound socket, and the only form a container
    // port offers, so the HTTP schemes count as their WebSocket twins.
    let tls = match url.scheme() {
        "ws" | "http" => false,
        "wss" | "https" => true,
        scheme => return Err(anyhow!("not a WebSocket scheme: {scheme}").into()),
    };
    let host = url
        .host_str()
        .context("WebSocket URL has no host")?
        .to_string();
    let network_host = host.trim_start_matches('[').trim_end_matches(']');
    let port = url.port_or_known_default().context("no port")?;
    let authority = match (tls, port) {
        (false, 80) | (true, 443) => host.clone(),
        _ => format!("{host}:{port}"),
    };
    let target = match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    };

    let tcp = tokio::net::TcpStream::connect((network_host, port))
        .await
        .with_context(|| format!("connect {authority}"))?;
    let stream: Box<dyn Stream> = if tls {
        let name = rustls::pki_types::ServerName::try_from(network_host.to_owned())
            .map_err(|_| anyhow!("invalid TLS server name: {host}"))?;
        Box::new(
            connector()
                .connect(name, tcp)
                .await
                .with_context(|| format!("TLS handshake with {authority}"))?,
        )
    } else {
        Box::new(tcp)
    };

    let key = fastwebsockets::handshake::generate_key();
    let mut request = Request::builder()
        .method("GET")
        .uri(&target)
        .header(HOST, &authority)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "Upgrade")
        .header(SEC_WEBSOCKET_KEY, &key)
        .header(SEC_WEBSOCKET_VERSION, "13")
        .body(Empty::<bytes::Bytes>::new())
        .context("build WebSocket handshake")?;
    request.headers_mut().extend(extra);

    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .with_context(|| format!("HTTP handshake with {authority}"))?;
    // The opening owns this driver until hyper transfers the transport into
    // Upgraded. Afterwards the WebSocket itself owns all transport I/O.
    let _driver = HandshakeDriver(tokio::spawn(async move {
        let _ = connection.with_upgrades().await;
    }));

    let mut response = sender
        .send_request(request)
        .await
        .with_context(|| format!("WebSocket handshake with {authority}"))?;
    if response.status() != StatusCode::SWITCHING_PROTOCOLS {
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .map(|body| body.to_bytes().to_vec())
            .unwrap_or_default();
        return Err(Error::Declined(Box::new(Declined {
            status,
            headers,
            body,
        })));
    }
    if response
        .headers()
        .get(SEC_WEBSOCKET_ACCEPT)
        .map(HeaderValue::as_bytes)
        != Some(accept_key(&key).as_bytes())
    {
        return Err(anyhow!("server returned an invalid Sec-WebSocket-Accept").into());
    }

    let headers = response.headers().clone();
    let upgraded = hyper::upgrade::on(&mut response)
        .await
        .with_context(|| format!("upgrade the connection to {authority}"))?;
    Ok(Connection {
        socket: WebSocket::after_handshake(TokioIo::new(upgraded), Role::Client),
        headers,
    })
}

trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<S: AsyncRead + AsyncWrite + Send + Unpin> Stream for S {}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // Transport tests run outside application turns.
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn echo(mut stream: impl Stream) -> anyhow::Result<()> {
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(stream.read_u8().await?);
            anyhow::ensure!(headers.len() <= 16_384, "oversized handshake");
        }
        let headers = String::from_utf8(headers)?;
        let key = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("sec-websocket-key")
                    .then(|| value.trim())
            })
            .context("handshake key")?;
        stream.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {}\r\n\r\n", accept_key(key)).as_bytes()).await?;
        let mut socket = WebSocket::after_handshake(stream, Role::Server);
        let frame = socket.read_frame().await?;
        socket
            .write_frame(fastwebsockets::Frame::binary(frame.payload.to_vec().into()))
            .await?;
        Ok(())
    }

    async fn exchange(mut connection: Connection) -> anyhow::Result<()> {
        connection
            .socket
            .write_frame(fastwebsockets::Frame::binary(vec![0, 128, 255].into()))
            .await?;
        assert_eq!(
            &*connection.socket.read_frame().await?.payload,
            &[0, 128, 255]
        );
        Ok(())
    }

    #[tokio::test]
    async fn verified_tls_and_ipv6_websocket_transport() -> anyhow::Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer};
            let ca = CertificateDer::from(include_bytes!("host_tcp/fixtures/ca.der").to_vec());
            let certificate =
                CertificateDer::from(include_bytes!("host_tcp/fixtures/server.der").to_vec());
            let key = PrivateKeyDer::try_from(include_bytes!("host_tcp/fixtures/key.der").to_vec())
                .map_err(anyhow::Error::msg)?;
            let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(
                rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(vec![certificate], key)?,
            ));
            let mut roots = rustls::RootCertStore::empty();
            roots.add(ca)?;
            let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            ));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let url = format!("wss://localhost:{}/echo", listener.local_addr()?.port());
            let peer = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await?;
                echo(acceptor.accept(tcp).await?).await?;
                let (tcp, _) = listener.accept().await?;
                anyhow::ensure!(
                    acceptor.accept(tcp).await.is_err(),
                    "default roots accepted the local test CA"
                );
                Ok::<_, anyhow::Error>(())
            });
            exchange(
                connect_with_tls(&url, HeaderMap::new(), || connector)
                    .await
                    .map_err(|e| anyhow!("{e}"))?,
            )
            .await?;
            assert!(matches!(
                connect(&url, HeaderMap::new()).await,
                Err(Error::Failed(_))
            ));
            peer.await??;
            if let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await {
                let url = format!("ws://{}/echo", listener.local_addr()?);
                let peer = tokio::spawn(async move { echo(listener.accept().await?.0).await });
                exchange(
                    connect(&url, HeaderMap::new())
                        .await
                        .map_err(|e| anyhow!("{e}"))?,
                )
                .await?;
                peer.await??;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
