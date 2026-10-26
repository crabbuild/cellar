// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! A minimal Docker Engine API client over the daemon's unix socket.
//!
//! One HTTP/1.1 connection per call: the daemon is local, the calls are
//! rare (a container starts once), and a pooled client would have to be
//! taught about hijacked exec streams and long-polled waits anyway. Podman
//! serves the same API on its own socket, so nothing here names Docker
//! beyond the default socket path.

use anyhow::{anyhow, Context};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, CONTENT_TYPE, HOST, UPGRADE};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixStream;

/// Unversioned paths: the daemon answers with its newest API, and every
/// call here uses fields that have been stable since API 1.24. A pinned
/// version is refused by a daemon whose minimum moved past it, which Docker
/// 29 did for 1.41.
const API: &str = "";

#[derive(Clone, Debug)]
pub struct Docker {
    socket: PathBuf,
}

/// A raw bidirectional stream the daemon hijacked for an exec or attach.
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

pub struct Reply {
    pub status: StatusCode,
    pub body: Bytes,
}

impl Reply {
    pub fn json(&self) -> anyhow::Result<serde_json::Value> {
        serde_json::from_slice(&self.body).context("decode Docker response")
    }

    /// The daemon's `message` field, or the raw body.
    pub fn message(&self) -> String {
        self.json()
            .ok()
            .and_then(|value| value.get("message")?.as_str().map(str::to_string))
            .unwrap_or_else(|| String::from_utf8_lossy(&self.body).into_owned())
    }
}

impl Docker {
    /// Bounded archive traffic. The request owns its connection, so dropping
    /// it cancels unfinished upload/download work. Archives never touch host FS.
    pub(crate) async fn archive(
        &self,
        method: &str,
        path: &str,
        body: Bytes,
        maximum: usize,
    ) -> anyhow::Result<(Reply, Option<String>)> {
        let mut request = self.request(method, path, body)?;
        request
            .headers_mut()
            .insert(CONTENT_TYPE, "application/x-tar".parse()?);
        self.exchange(request, |response| async move {
            let status = response.status();
            let stat = response
                .headers()
                .get("x-docker-container-path-stat")
                .map(|header| {
                    anyhow::ensure!(
                        header.as_bytes().len() <= 16384,
                        "container stat header exceeds limit"
                    );
                    Ok::<_, anyhow::Error>(header.to_str()?.to_owned())
                })
                .transpose()?;
            let body = Limited::new(response.into_body(), maximum)
                .collect()
                .await
                .map_err(|error| {
                    anyhow!("container archive exceeds byte budget or could not be read: {error}")
                })?
                .to_bytes();
            Ok((Reply { status, body }, stat))
        })
        .await
    }

    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    /// The daemon socket: `DOCKER_HOST` when it names a unix socket, else
    /// the first of the well-known paths that exists. Docker Desktop and
    /// OrbStack put theirs under the home directory; Linux under `/var/run`.
    pub fn discover() -> Option<Self> {
        if let Ok(host) = std::env::var("DOCKER_HOST") {
            let path = host.strip_prefix("unix://")?;
            return Some(Self::new(path));
        }
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let candidates = [
            Some(PathBuf::from("/var/run/docker.sock")),
            home.as_ref()
                .map(|home| home.join(".orbstack/run/docker.sock")),
            home.as_ref()
                .map(|home| home.join(".docker/run/docker.sock")),
            Some(PathBuf::from("/run/podman/podman.sock")),
        ];
        // A connect, not a stat: the socket is a daemon's, not node
        // storage, and a stale socket file without a daemon behind it must
        // not win over the next candidate.
        candidates
            .into_iter()
            .flatten()
            .find(|path| std::os::unix::net::UnixStream::connect(path).is_ok())
            .map(Self::new)
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    async fn connect(&self) -> anyhow::Result<UnixStream> {
        UnixStream::connect(&self.socket).await.with_context(|| {
            format!(
                "connect to the container engine at {}",
                self.socket.display()
            )
        })
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Bytes,
    ) -> anyhow::Result<Request<Full<Bytes>>> {
        Request::builder()
            .method(method)
            .uri(format!("{API}{path}"))
            .header(HOST, "docker")
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(body))
            .context("build Docker request")
    }

    // Drive each connection inside its request future, so cancelling a
    // request also drops long-polled waits and unfinished upgrade transports.
    async fn exchange<T, F, Fut>(
        &self,
        request: Request<Full<Bytes>>,
        receive: F,
    ) -> anyhow::Result<T>
    where
        F: FnOnce(Response<Incoming>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let stream = self.connect().await?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .context("HTTP handshake with the container engine")?;
        let operation = async move {
            let response = sender
                .send_request(request)
                .await
                .context("send request to the container engine")?;
            let result = receive(response).await;
            drop(sender);
            result
        };
        tokio::pin!(operation);
        let connection = connection.with_upgrades();
        tokio::pin!(connection);
        crate::asyncrt::select! {
            result = &mut operation => result,
            result = &mut connection => {
                result.context("drive the container engine connection")?;
                operation.await
            }
        }
    }

    async fn reply(&self, request: Request<Full<Bytes>>) -> anyhow::Result<Reply> {
        self.exchange(request, |response| async move {
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .context("read Docker response")?
                .to_bytes();
            Ok(Reply { status, body })
        })
        .await
    }

    /// One call, whole body. A long-polled `wait` is fine here: the body
    /// arrives only when the container exits.
    pub async fn call(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> anyhow::Result<Reply> {
        let body = match body {
            Some(value) => Bytes::from(serde_json::to_vec(&value)?),
            None => Bytes::new(),
        };
        self.reply(self.request(method, path, body)?).await
    }

    /// `call`, failing on any non-success status with the daemon's message.
    pub async fn expect(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
        what: &str,
    ) -> anyhow::Result<Reply> {
        let reply = self.call(method, path, body).await?;
        if !reply.status.is_success() {
            return Err(anyhow!(
                "{what} failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        Ok(reply)
    }

    /// Upload a request body that is not JSON: `POST /images/load` takes a
    /// tar. The whole body is in memory, which bounds an image at what a
    /// node can hold; a streaming upload is the next step when an image
    /// outgrows that.
    pub async fn post_octets(&self, path: &str, body: Bytes, what: &str) -> anyhow::Result<Reply> {
        let request = Request::builder()
            .method("POST")
            .uri(format!("{API}{path}"))
            .header(HOST, "docker")
            .header(CONTENT_TYPE, "application/x-tar")
            .body(Full::new(body))
            .context("build Docker request")?;
        let reply = self.reply(request).await?;
        if !reply.status.is_success() {
            return Err(anyhow!(
                "{what} failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        Ok(reply)
    }

    /// `POST /exec/{id}/start` with `Upgrade: tcp`: the daemon answers 101
    /// and the connection becomes the process's stdin and its multiplexed
    /// stdout/stderr. The returned stream is that connection.
    pub async fn hijack(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<Box<dyn Stream>> {
        let request = Request::builder()
            .method("POST")
            .uri(format!("{API}{path}"))
            .header(HOST, "docker")
            .header(CONTENT_TYPE, "application/json")
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "tcp")
            .body(Full::new(Bytes::from(serde_json::to_vec(&body)?)))
            .context("build Docker request")?;
        self.exchange(request, |mut response| async move {
            if response.status() != StatusCode::SWITCHING_PROTOCOLS {
                let status = response.status();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map(|body| body.to_bytes())
                    .unwrap_or_default();
                let reply = Reply { status, body };
                return Err(anyhow!(
                    "exec start failed with [{}] {}",
                    status.as_u16(),
                    reply.message()
                ));
            }
            let upgraded = hyper::upgrade::on(&mut response)
                .await
                .context("upgrade the exec connection")?;
            Ok(Box::new(TokioIo::new(upgraded)) as Box<dyn Stream>)
        })
        .await
    }
}

/// Parse one frame header of Docker's multiplexed stream: one byte of
/// stream id (1 stdout, 2 stderr), three reserved, four of big-endian
/// length.
pub fn frame_header(header: &[u8; 8]) -> (u8, usize) {
    let length = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
    (header[0], length)
}
