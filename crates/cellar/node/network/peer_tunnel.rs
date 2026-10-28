// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. All rights reserved.
// Modified for Cellar.

//! The peer tunnel: an upgraded stream per peer, plain HTTP inside.
//!
//! Establishment is an h1 upgrade on `/peer/tunnel` — CONNECT semantics with Upgrade
//! mechanics, because CONNECT's authority-form target fits this use badly.
//! After the 101 the connection is an opaque duplex stream, and app requests
//! cross as literal HTTP driven by hyper on both ends: the hop never
//! interprets the inner bytes, so the headers the cell must receive
//! byte-faithful (`Host`, `Content-Length`, `Upgrade`) are data here, not
//! metadata. Per-call control (scope, cell name, request id, capacity
//! handoff) rides `x-cells-*` headers on the inner request; node A overwrites
//! and node B strips them, so the reserved names cannot be smuggled by an
//! application in either direction.
//!
//! Idle tunnels are pooled per peer and reused for sequential calls — plain
//! h1 keep-alive inside the tunnel, never multiplexing. Without reuse every
//! call is a fresh TCP connection, and a loopback-speed caller saturates the
//! whole ephemeral-port range into TIME_WAIT within seconds (~1k calls/s is
//! the hard OS ceiling), which the pooled transport this replaces never hit.

use std::collections::VecDeque;
use std::convert::Infallible;

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use hyper_util::rt::TokioIo;

use super::*;
use crate::engine_api::HttpChunkStream;

pub(crate) const TUNNEL_PROTOCOL: &str = "cellar-tunnel";
pub(crate) const TUNNEL_VERSION: &str = peer_auth::PROTOCOL_VERSION_TEXT;
pub(crate) const VERSION_HEADER: &str = "x-cells-tunnel-version";
pub(crate) const KIND_HEADER: &str = "x-cells-tunnel-kind";
pub(crate) const SCOPE_HEADER: &str = "x-cells-scope";
pub(crate) const NAME_HEADER: &str = "x-cells-do-name";
pub(crate) const REQUEST_ID_HEADER: &str = "x-cells-request-id";
pub(crate) const HANDOFF_HEADER: &str = "x-cells-capacity-handoff";
const RPC_IDLE_HEADER: &str = "x-cells-rpc-idle-ms";
const RPC_REUSE_HEADER: &str = "x-cells-rpc-reuse-version";

/// The inner headers node A owns. A overwrites them on the way in and B
/// strips them on the way out, so an application header with a reserved name
/// is dropped rather than obeyed.
const CONTROL_HEADERS: [&str; 5] = [
    SCOPE_HEADER,
    NAME_HEADER,
    REQUEST_ID_HEADER,
    HANDOFF_HEADER,
    rpc_deadline::BUDGET_HEADER,
];

enum TunnelKind {
    Do,
    Rpc,
}

/// The control fields of one tunneled call, carried as inner-request headers
/// so one tunnel can serve calls for any cell.
pub(crate) struct TunnelControl {
    pub(crate) scope: String,
    pub(crate) name: Option<String>,
    pub(crate) request_id: Option<crate::engine_api::RequestId>,
    pub(crate) capacity_handoff: bool,
}

pub(crate) fn is_tunnel_request(request: &Request<Incoming>) -> bool {
    request
        .headers()
        .get(hyper::header::UPGRADE)
        .is_some_and(|value| value.as_bytes() == TUNNEL_PROTOCOL.as_bytes())
}

/// Node B: answer a tunnel establishment on `/peer/tunnel` with a 101 and serve the
/// inner connection until the peer closes it.
///
/// The establishment carries the fleet HMAC and nothing after it does. The
/// signature gates who can open a tunnel — the inner calls it then carries
/// are cell dispatches, and a reserved-class dispatch must demand the fleet
/// secret on every path the way `/runtime/` does. The signature does not
/// authenticate the bytes after the 101 and does not encrypt anything; the
/// private network remains the boundary for both.
pub(crate) fn accept(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        request.method(),
        &path_and_query,
        request.headers(),
        b"",
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        // A wrong target means the dialer holds a stale lease for this
        // address; the stale marker lets its redispatch invalidate the
        // route instead of retrying the same dead name.
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    let headers = request.headers();
    if headers
        .get(VERSION_HEADER)
        .is_none_or(|value| value.as_bytes() != TUNNEL_VERSION.as_bytes())
    {
        return peer_response(response(
            StatusCode::UPGRADE_REQUIRED,
            "peer tunnel version is incompatible",
        ));
    }
    let kind = match headers
        .get(KIND_HEADER)
        .map(hyper::header::HeaderValue::as_bytes)
    {
        Some(b"do") => TunnelKind::Do,
        Some(b"rpc") => TunnelKind::Rpc,
        _ => {
            return peer_response(response(StatusCode::BAD_REQUEST, "unknown tunnel kind"));
        }
    };
    let rpc_connection = if matches!(&kind, TunnelKind::Rpc) {
        if headers.get_all(RPC_REUSE_HEADER).iter().count() != 1
            || headers
                .get(RPC_REUSE_HEADER)
                .is_none_or(|value| value.as_bytes() != TUNNEL_VERSION.as_bytes())
        {
            return peer_response(response(
                StatusCode::UPGRADE_REQUIRED,
                "RPC tunnel reuse contract is incompatible",
            ));
        }
        let source = match peer_auth::verified_source(request.headers()) {
            Ok(source) => source,
            Err(_) => return peer_response(response(StatusCode::UNAUTHORIZED, "invalid RPC peer")),
        };
        match crate::asyncrt::services().admit_rpc_tunnel(source) {
            Some(connection) => Some(connection),
            None => return rpc_failure::exhausted(),
        }
    } else {
        None
    };
    let rpc_idle_ms = rpc_connection
        .as_ref()
        .map(|connection| connection.idle_timeout().as_millis() as u64);
    let upgrade = hyper::upgrade::on(&mut request);
    crate::asyncrt::spawn(async move {
        let upgraded = match rpc_connection.as_ref() {
            Some(connection) => {
                match crate::asyncrt::timeout(connection.idle_timeout(), upgrade).await {
                    Ok(Ok(upgraded)) => upgraded,
                    _ => return,
                }
            }
            None => match upgrade.await {
                Ok(upgraded) => upgraded,
                Err(_) => return,
            },
        };
        match (kind, rpc_connection) {
            (TunnelKind::Do, _) => serve_do(upgraded, app).await,
            (TunnelKind::Rpc, Some(connection)) => serve_rpc(upgraded, app, connection).await,
            (TunnelKind::Rpc, None) => (),
        }
    })
    .detach();
    let mut reply = response(StatusCode::SWITCHING_PROTOCOLS, "");
    reply.headers_mut().insert(
        hyper::header::UPGRADE,
        hyper::header::HeaderValue::from_static(TUNNEL_PROTOCOL),
    );
    reply.headers_mut().insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("upgrade"),
    );
    reply.headers_mut().insert(
        hyper::header::HeaderName::from_static(VERSION_HEADER),
        hyper::header::HeaderValue::from_static(TUNNEL_VERSION),
    );
    if let Some(idle_ms) = rpc_idle_ms {
        reply.headers_mut().insert(
            hyper::header::HeaderName::from_static(RPC_IDLE_HEADER),
            hyper::header::HeaderValue::from(idle_ms),
        );
    }
    peer_response(reply)
}

/// One inner request, one call: parse the app request hyper already framed,
/// dispatch it exactly as the enveloped path did, and let the reply stream
/// back as an ordinary HTTP response. The connection serves calls until the
/// peer closes it or an inner upgrade consumes it.
async fn serve_do(upgraded: hyper::upgrade::Upgraded, app: AppHandle) {
    let service = service_fn(move |mut inner: Request<Incoming>| {
        let app = app.clone();
        async move {
            let Some(scope) = inner
                .headers()
                .get(SCOPE_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .filter(|scope| cellar_logic::cell::valid_cell_scope(scope))
            else {
                return Ok::<_, Infallible>(peer_response(malformed_scope()));
            };
            let control = TunnelControl {
                scope,
                name: inner
                    .headers()
                    .get(NAME_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string),
                request_id: inner
                    .headers()
                    .get(REQUEST_ID_HEADER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(crate::engine_api::parse_request_id),
                capacity_handoff: inner.headers().get(HANDOFF_HEADER).is_some(),
            };
            // A tunneled upgrade is the client's own handshake, so the
            // owner answers it directly: fastwebsockets computes the 101
            // for the client's key, and the inner connection becomes the
            // socket the cell speaks — the same machinery a locally
            // connected client gets.
            let client_upgrade = if fastwebsockets::upgrade::is_upgrade_request(&inner) {
                match fastwebsockets::upgrade::upgrade(&mut inner) {
                    Ok(pair) => Some(pair),
                    Err(error) => {
                        return Ok(peer_response(response(
                            StatusCode::BAD_REQUEST,
                            format!("tunneled upgrade: {error}"),
                        )));
                    }
                }
            } else {
                None
            };
            let (parts, body) = inner.into_parts();
            let headers = parts
                .headers
                .iter()
                .filter(|(name, _)| !CONTROL_HEADERS.contains(&name.as_str()))
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_string(), value.to_string()))
                })
                .collect();
            // Streaming by construction: the inner body is handed to the
            // isolate as a stream whatever its size, bounded the way the
            // enveloped path bounded its frame.
            let limited = http_body_util::Limited::new(body, MAX_PEER_FORWARD_BODY_BYTES);
            let chunks: HttpChunkStream = Box::pin(
                http_body_util::BodyStream::new(limited)
                    .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) })
                    .map_ok(|data: Bytes| data.to_vec())
                    .map_err(|error| error.to_string()),
            );
            let stream_id = match crate::http_streams::register_body_stream(chunks) {
                Ok(stream_id) => stream_id,
                Err(error) => {
                    return Ok(peer_response(response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("request body stream: {error}"),
                    )));
                }
            };
            let body = crate::engine_api::RequestBody::Stream(stream_id);
            let fetch = ForwardedFetch {
                name: control.name,
                url: parts.uri.to_string(),
                method: parts.method.to_string(),
                headers,
                request_id: control.request_id,
                capacity_handoff: control.capacity_handoff,
            };
            let outcome = dispatch_forwarded_fetch(app.clone(), control.scope, fetch, body).await;
            let (target, worker_headers) = match outcome {
                ForwardedFetchOutcome::Reply(reply) => return Ok(reply),
                ForwardedFetchOutcome::WebSocket { target, headers } => (target, headers),
            };
            let Some((accept, upgrade)) = client_upgrade else {
                // `dispatch_forwarded_fetch` refuses a request without
                // `Upgrade: websocket`, so this branch sees only a request
                // that names the upgrade but is not a valid handshake. The
                // socket is registered with the core by now, and the refusal
                // must release it.
                let refusal = super::websocket::refuse_websocket_without_upgrade(
                    &app,
                    HttpResponseWebSocket::Cell(target),
                )
                .await;
                return Ok(peer_response(refusal));
            };
            // The cell accepted: hand the inner stream to the same socket
            // task a local client gets, and answer with the real 101. The
            // Worker's application headers ride along; the sidecars and
            // the hop framing die here.
            crate::asyncrt::spawn(async move {
                match upgrade.await {
                    Ok(socket) => super::websocket::websocket_task(app, target, socket).await,
                    Err(error) => {
                        eprintln!("cellar tunneled upgrade failed: {error}");
                    }
                }
            })
            .detach();
            let (accept_parts, _) = accept.into_parts();
            let mut answer = Response::from_parts(
                accept_parts,
                BodyExt::boxed_unsync(http_body_util::Empty::new().map_err(|never| match never {})),
            );
            for (name, value) in &worker_headers {
                let lowered = name.to_ascii_lowercase();
                if lowered == "content-length"
                    || lowered == "upgrade"
                    || lowered == "connection"
                    || lowered == "sec-websocket-accept"
                {
                    continue;
                }
                let Ok(name) = hyper::header::HeaderName::from_bytes(name.as_bytes()) else {
                    continue;
                };
                let Ok(value) = hyper::header::HeaderValue::from_str(value) else {
                    continue;
                };
                answer.headers_mut().append(name, value);
            }
            Ok(peer_response(answer))
        }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(upgraded, service)
        .with_upgrades()
        .await;
}

struct TunnelSender {
    inner: hyper::client::conn::http1::SendRequest<TunnelBody>,
    reuse: Option<ReuseWindow>,
}

/// Cache eligibility is shorter than receiving idle admission. It starts at
/// dial, not reply retirement: slow reads cannot renew an older peer socket.
/// A weak clock token also prevents reuse after the original driver stops.
struct ReuseWindow {
    domain: crate::asyncrt::DomainToken,
    expires_ms: u64,
}

impl TunnelSender {
    fn reusable(&self) -> bool {
        !self.inner.is_closed()
            && self.reuse.as_ref().is_none_or(|window| {
                window
                    .domain
                    .mono_ms()
                    .is_ok_and(|now| now < window.expires_ms)
            })
    }
}

fn rpc_idle_interval(headers: &hyper::HeaderMap) -> anyhow::Result<u32> {
    anyhow::ensure!(
        headers.get_all(RPC_IDLE_HEADER).iter().count() == 1,
        "RPC tunnel idle interval is missing or repeated"
    );
    let value = headers[RPC_IDLE_HEADER]
        .to_str()
        .context("RPC tunnel idle interval is not text")?;
    anyhow::ensure!(
        !value.is_empty()
            && !value.starts_with('0')
            && value.bytes().all(|byte| byte.is_ascii_digit()),
        "RPC tunnel idle interval is not canonical positive decimal"
    );
    value
        .parse()
        .context("RPC tunnel idle interval is out of range")
}

/// The peer rejected the outer tunnel before an application request crossed
/// it. This is a definite stale route, so the caller can refresh ownership
/// without risking a duplicate handler invocation.
#[derive(Debug)]
pub(crate) struct StaleTunnelRoute;

impl std::fmt::Display for StaleTunnelRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("peer rejected the stale tunnel route")
    }
}

impl std::error::Error for StaleTunnelRoute {}

/// Who node A dials and how it proves itself. One borrow carries the
/// client, the fleet key, and the routed peer's address and name through
/// an attempt.
pub(crate) struct Peer<'a> {
    pub(crate) http: &'a reqwest::Client,
    pub(crate) auth: &'a PeerAuth,
    pub(crate) addr: &'a str,
    pub(crate) node: &'a str,
}

/// One bounded idle cache across all peers and tunnel kinds. A sender goes back
/// only after its response body reaches a clean end; active calls own senders
/// outside the cache. No empty per-peer entries survive checkout or eviction.
static TUNNELS: std::sync::OnceLock<std::sync::Mutex<IdleTunnels>> = std::sync::OnceLock::new();
/// Completed idle tunnels count against this bound. Active calls own their
/// senders outside the pool; filling the cache cannot interrupt an active reply.
const TUNNEL_POOL_CAP: usize = 64;

#[derive(Clone, PartialEq, Eq)]
struct TunnelKey {
    address: String,
    kind: &'static str,
    target: String,
    scope: crate::peer_auth::TunnelScope,
}

#[derive(Default)]
struct IdleTunnels(VecDeque<(TunnelKey, TunnelSender)>);

impl IdleTunnels {
    fn checkout(&mut self, key: &TunnelKey) -> Option<TunnelSender> {
        self.0.retain(|(_, sender)| sender.reusable());
        let index = self
            .0
            .iter()
            .position(|(peer, sender)| peer == key && sender.inner.is_ready())?;
        self.0.remove(index).map(|(_, sender)| sender)
    }

    fn park(&mut self, key: &TunnelKey, sender: TunnelSender) {
        self.0.retain(|(_, sender)| sender.reusable());
        if !sender.reusable() {
            return;
        }
        // Evict the least recently returned idle tunnel. Reusing a live peer
        // refreshes its position, while departed peers cannot reserve slots
        // forever. Eviction never touches an active response or replays work.
        if self.0.len() == TUNNEL_POOL_CAP {
            self.0.pop_front();
        }
        self.0.push_back((key.clone(), sender));
    }
}

fn checkout(key: &TunnelKey) -> Option<TunnelSender> {
    TUNNELS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .checkout(key)
}

fn park(key: &TunnelKey, sender: TunnelSender) {
    TUNNELS
        .get_or_init(Default::default)
        .lock()
        .unwrap()
        .park(key, sender);
}

fn tunnel_key(peer: &Peer<'_>, kind: &'static str) -> TunnelKey {
    // The signed upgrade binds both authentication owner and target session.
    // Neither may be bypassed just because another connection shares an address.
    TunnelKey {
        address: peer.addr.to_string(),
        kind,
        target: peer.node.to_string(),
        scope: peer.auth.tunnel_scope(),
    }
}

/// Establish a fresh tunnel to `addr` and hand back its inner h1 client.
/// The establishment is a bodiless GET, so the fleet HMAC signs it whole —
/// the one signature every call on this tunnel then rides.
async fn establish(peer: &Peer<'_>, kind: &'static str) -> anyhow::Result<TunnelSender> {
    let reuse_clock = if kind == "rpc" {
        let domain = crate::asyncrt::services()
            .domain_token()
            .context("RPC tunnel source domain is unavailable")?;
        let started_ms = domain.mono_ms()?;
        Some((domain, started_ms))
    } else {
        None
    };
    let mut request = peer
        .http
        .get(format!("http://{}/peer/tunnel", peer.addr))
        .header(reqwest::header::UPGRADE, TUNNEL_PROTOCOL)
        .header(reqwest::header::CONNECTION, "upgrade")
        .header(VERSION_HEADER, TUNNEL_VERSION)
        .header(KIND_HEADER, kind)
        .header(
            peer_auth::RESPONSE_VERSION_HEADER,
            peer_auth::PROTOCOL_VERSION_TEXT,
        );
    if kind == "rpc" {
        request = request.header(RPC_REUSE_HEADER, TUNNEL_VERSION);
    }
    let response = peer
        .auth
        .sign(request, "GET", "/peer/tunnel", b"", peer.node)?
        .send()
        .await?;
    peer_auth::validate_response(response.headers())?;
    if let Some(failure) = rpc_failure::remote(response.status(), response.headers()) {
        return Err(failure.into());
    }
    if response
        .headers()
        .get(STALE_ROUTE_HEADER)
        .is_some_and(|value| value == STALE_ROUTE_VALUE)
    {
        return Err(StaleTunnelRoute.into());
    }
    anyhow::ensure!(
        response.status() == reqwest::StatusCode::SWITCHING_PROTOCOLS,
        "tunnel establishment answered {}",
        response.status()
    );
    let reuse = match reuse_clock {
        Some((domain, started_ms)) => Some(ReuseWindow {
            domain,
            // Leave half the receiving interval for a queued or delayed next
            // submission. This is a cache hint, never authority to replay work.
            expires_ms: started_ms
                .saturating_add(u64::from(rpc_idle_interval(response.headers())?) / 2),
        }),
        None => None,
    };
    let upgraded = response.upgrade().await?;
    let (sender, connection) = hyper::client::conn::http1::Builder::new()
        .handshake(TokioIo::new(upgraded))
        .await?;
    crate::asyncrt::spawn(async move {
        let _ = connection.with_upgrades().await;
    })
    .detach();
    Ok(TunnelSender {
        inner: sender,
        reuse,
    })
}

fn inner_request(
    control: &TunnelControl,
    url: &str,
    method: &str,
    headers: &[(String, String)],
    body: TunnelBody,
) -> anyhow::Result<Request<TunnelBody>> {
    let mut inner = Request::builder().method(method).uri(url);
    for (name, value) in headers {
        if CONTROL_HEADERS
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(name))
        {
            continue;
        }
        inner = inner.header(name.as_str(), value.as_str());
    }
    inner = inner.header(SCOPE_HEADER, control.scope.as_str());
    if let Some(name) = &control.name {
        inner = inner.header(NAME_HEADER, name);
    }
    if let Some(request_id) = control.request_id {
        inner = inner.header(
            REQUEST_ID_HEADER,
            crate::engine_api::request_id_string(request_id),
        );
    }
    if control.capacity_handoff {
        inner = inner.header(HANDOFF_HEADER, "1");
    }
    let mut inner = inner.body(body)?;
    // Stub/service fetches request an upgrade with only `Upgrade: websocket`.
    // The peer hop needs its own valid HTTP handshake before the returned
    // capability can be consumed or forwarded. Keep a public client's key.
    if crate::cell_dispatch::requests_websocket_upgrade(headers) {
        inner.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("upgrade"),
        );
        if !inner
            .headers()
            .contains_key(hyper::header::SEC_WEBSOCKET_VERSION)
        {
            inner.headers_mut().insert(
                hyper::header::SEC_WEBSOCKET_VERSION,
                hyper::header::HeaderValue::from_static("13"),
            );
        }
        if !inner
            .headers()
            .contains_key(hyper::header::SEC_WEBSOCKET_KEY)
        {
            use base64::Engine;
            let mut key = [0_u8; 16];
            getrandom::fill(&mut key)?;
            let key = base64::engine::general_purpose::STANDARD.encode(key);
            inner
                .headers_mut()
                .insert(hyper::header::SEC_WEBSOCKET_KEY, key.parse()?);
        }
    }
    Ok(inner)
}

/// Retry a pooled tunnel only when Hyper returns the request as unsent.
/// A buffered/replayable body alone cannot establish non-execution: the owner
/// may have committed before losing its response. Reuse the exact recovered
/// request (including a streamed body), without rebuilding protocol identity.
async fn send_inner(
    peer: &Peer<'_>,
    kind: &'static str,
    build: impl FnOnce(TunnelBody) -> anyhow::Result<Request<TunnelBody>>,
    body: crate::engine_api::RequestBody,
    deadline_ms: Option<u64>,
) -> anyhow::Result<Response<PooledResponseBody>> {
    let key = tunnel_key(peer, kind);
    let body = match body {
        crate::engine_api::RequestBody::Bytes(bytes) => full_body(bytes),
        crate::engine_api::RequestBody::Stream(stream_id) => {
            let payload = crate::http_streams::take_body_stream(stream_id)
                .map_err(|error| anyhow::anyhow!("take tunnel body: {error}"))?;
            stream_body(payload)
        }
    };
    let mut request = build(body)?;
    if let Some(sender) = checkout(&key) {
        rpc_deadline::write(&mut request, deadline_ms)?;
        match send_on_tunnel(&key, sender, request).await {
            Ok(response) => return Ok(response),
            Err(mut error) => {
                let Some(unsent) = error.take_message() else {
                    return Err((*error).into_error().into());
                };
                request = unsent;
            }
        }
    }
    let sender = establish(peer, kind).await?;
    rpc_deadline::write(&mut request, deadline_ms)?;
    send_on_tunnel(&key, sender, request)
        .await
        .map_err(|error| (*error).into_error().into())
}

async fn send_on_tunnel(
    key: &TunnelKey,
    mut sender: TunnelSender,
    request: Request<TunnelBody>,
) -> Result<Response<PooledResponseBody>, Box<hyper::client::conn::TrySendError<Request<TunnelBody>>>>
{
    let response = sender
        .inner
        .try_send_request(request)
        .await
        .map_err(Box::new)?;
    let reusable =
        (response.status() != StatusCode::SWITCHING_PROTOCOLS).then(|| (key.clone(), sender));
    Ok(response.map(|body| PooledResponseBody { body, reusable }))
}

/// An inner response owns its tunnel until the body finishes successfully.
/// Returning the sender at the response head makes active replies consume the
/// idle-pool bound. Sustained RPC concurrency then creates and discards one
/// upgraded TCP connection for every overflow call.
pub(crate) struct PooledResponseBody {
    body: Incoming,
    reusable: Option<(TunnelKey, TunnelSender)>,
}

impl Body for PooledResponseBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match std::pin::Pin::new(&mut self.body).poll_frame(cx) {
            std::task::Poll::Ready(None) => {
                if let Some((key, sender)) = self.reusable.take() {
                    park(&key, sender);
                }
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Ready(Some(Err(error))) => {
                // The unread or failed stream cannot carry another request.
                self.reusable.take();
                std::task::Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        // Force one final poll while the sender is still attached. A zero-byte
        // response can otherwise skip `poll_frame` and close a reusable tunnel.
        self.reusable.is_none() && self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.body.size_hint()
    }
}

pub(crate) async fn fetch(
    peer: &Peer<'_>,
    control: &TunnelControl,
    url: String,
    method: String,
    headers: Vec<(String, String)>,
    body: crate::engine_api::RequestBody,
) -> anyhow::Result<Response<PooledResponseBody>> {
    send_inner(
        peer,
        "do",
        |body| inner_request(control, &url, &method, &headers, body),
        body,
        None,
    )
    .await
}

pub(crate) const METHOD_HEADER: &str = "x-cells-rpc-method";
const AUTHORITY_HEADER: &str = "x-cellar-event-authority";

/// One inner request, one RPC: the method name rides a header, the
/// arguments ride the body, and the content type says which encoding the
/// caller used.
async fn serve_rpc(
    upgraded: hyper::upgrade::Upgraded,
    app: AppHandle,
    connection: crate::inbound_rpc::Connection,
) {
    let idle = connection.clone();
    let service = service_fn(move |inner: Request<Incoming>| {
        let app = app.clone();
        let connection = connection.clone();
        async move {
            connection.touch();
            if !crate::asyncrt::services().accepts_work() {
                return Ok::<_, Infallible>(close_rpc_reply(peer_response(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "RPC host is stopping",
                ))));
            }
            let deadline_ms = match rpc_deadline::from_headers(
                inner.headers(),
                crate::asyncrt::mono_ms(),
                app.operation_deadline_ms,
            ) {
                Ok(deadline) => deadline,
                Err(_) => {
                    return Ok::<_, Infallible>(peer_response(response(
                        StatusCode::BAD_REQUEST,
                        "invalid RPC budget",
                    )))
                }
            };
            if deadline_ms.is_some_and(|deadline| deadline <= crate::asyncrt::mono_ms()) {
                return Ok(rpc_deadline::response());
            }
            let Some(reservation) = connection.request() else {
                return Ok(rpc_failure::exhausted());
            };
            let authority = match inner.headers().get(AUTHORITY_HEADER) {
                None => None,
                Some(header) => {
                    use base64::Engine;
                    let decoded = (|| -> anyhow::Result<crate::runtime::RpcAuthority> {
                        anyhow::ensure!(
                            header.as_bytes().len() <= 16_384,
                            "event authority header is too large"
                        );
                        let bytes =
                            base64::engine::general_purpose::STANDARD.decode(header.as_bytes())?;
                        let authority: crate::runtime::RpcAuthority =
                            serde_json::from_slice(&bytes)?;
                        authority.validate()?;
                        Ok(authority)
                    })();
                    match decoded {
                        Ok(authority) => Some(authority),
                        Err(_) => {
                            return Ok::<_, Infallible>(peer_response(response(
                                StatusCode::BAD_REQUEST,
                                "invalid event RPC authority",
                            )))
                        }
                    }
                }
            };
            if authority.is_some() && deadline_ms.is_some() {
                return Ok::<_, Infallible>(peer_response(response(
                    StatusCode::BAD_REQUEST,
                    "invalid RPC budget",
                )));
            }
            let Some(scope) = inner
                .headers()
                .get(SCOPE_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .filter(|scope| cellar_logic::cell::valid_cell_scope(scope))
            else {
                return Ok::<_, Infallible>(peer_response(malformed_scope()));
            };
            let name = inner
                .headers()
                .get(NAME_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let structured = inner
                .headers()
                .get(hyper::header::CONTENT_TYPE)
                .is_some_and(|value| value.as_bytes() == b"application/octet-stream");
            let request_id = inner
                .headers()
                .get(REQUEST_ID_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(crate::engine_api::parse_request_id);
            let Some(method) = inner
                .headers()
                .get(METHOD_HEADER)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
            else {
                return Ok::<_, Infallible>(peer_response(response(
                    StatusCode::BAD_REQUEST,
                    "RPC method header is missing",
                )));
            };
            let body_limit = if matches!(&authority, Some(crate::runtime::RpcAuthority::Name(_))) {
                crate::application::event_stream::name_resolution::MAX_REQUEST_BYTES
            } else if matches!(&authority, Some(crate::runtime::RpcAuthority::Catalog(_))) {
                crate::runtime::catalog_control::MAX_RPC_BYTES
            } else if matches!(&authority, Some(crate::runtime::RpcAuthority::Sandbox(_))) {
                crate::application::containers::MAX_SANDBOX_MESSAGE_BYTES
            } else if method == crate::runtime::source_control::METHOD
                || matches!(&authority, Some(crate::runtime::RpcAuthority::Source(_)))
            {
                crate::runtime::source_control::MAX_RPC_BYTES
            } else {
                MAX_PEER_FORWARD_BODY_BYTES
            };
            let upload_deadline = deadline_ms.unwrap_or_else(|| {
                crate::asyncrt::mono_ms().saturating_add(app.operation_deadline_ms)
            });
            let body = match rpc_deadline::receive(Some(upload_deadline), async {
                Ok(rpc_upload::receive(inner.into_body(), body_limit, &reservation).await)
            })
            .await
            {
                Ok(Ok(body)) => body,
                Ok(Err(error)) => {
                    let reply = match error {
                        rpc_upload::Error::Exhausted => rpc_failure::exhausted(),
                        rpc_upload::Error::TooLarge => peer_response(response(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "RPC request body too large",
                        )),
                        rpc_upload::Error::Invalid => peer_response(response(
                            StatusCode::BAD_REQUEST,
                            "invalid RPC request body",
                        )),
                    };
                    return Ok(close_rpc_reply(reply));
                }
                Err(_) => return Ok(close_rpc_reply(rpc_deadline::response())),
            };
            let args = if structured {
                crate::engine_api::RpcData::Structured(body)
            } else {
                match String::from_utf8(body.to_vec()) {
                    Ok(json) => crate::engine_api::RpcData::Json(json),
                    Err(_) => {
                        return Ok(peer_response(response(
                            StatusCode::BAD_REQUEST,
                            "JSON RPC arguments are not UTF-8",
                        )));
                    }
                }
            };
            let reply = dispatch_forwarded_rpc(
                app,
                crate::runtime::RpcInvocation {
                    reservation: Some(reservation.ticket()),
                    parent_cancellation: None,
                    signal: None,
                    deadline_ms,
                    cell: scope,
                    name,
                    method,
                    args,
                    request_id,
                    authority,
                },
            )
            .await;
            let (parts, body) = reply.into_parts();
            let body = match RetainedRpcBody::new(body, reservation, deadline_ms) {
                Ok(body) => body,
                Err(crate::application::rpc::Code::DeadlineExceeded) => {
                    return Ok(close_rpc_reply(rpc_deadline::response()));
                }
                Err(crate::application::rpc::Code::ResourceExhausted) => {
                    return Ok(close_rpc_reply(rpc_failure::exhausted()));
                }
                Err(_) => {
                    return Ok(close_rpc_reply(peer_response(response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "RPC receiver is stopping",
                    ))))
                }
            };
            Ok(Response::from_parts(parts, body.boxed_unsync()))
        }
    });
    let serving = rpc_server_builder().serve_connection(upgraded, service);
    crate::asyncrt::select_biased! { "idle receiving RPC tunnels release owned slots";
        _ = idle.idle() => {},
        _ = serving => {},
    }
}

fn rpc_server_builder() -> hyper::server::conn::http1::Builder {
    let mut builder = hyper::server::conn::http1::Builder::new();
    // Existing framework authority may itself occupy 16 KiB. Bound the whole
    // parser while leaving room for its scope, budget and ordinary HTTP fields.
    builder.max_buf_size(64 * 1024).max_headers(128);
    // Keep body-frame owners in Hyper's write queue even if an I/O wrapper only
    // implements scalar writes. Flattening copies bytes and drops their leases
    // before the pending socket write has consumed that copy.
    builder.writev(true);
    builder
}

fn close_rpc_reply(mut reply: HttpReply) -> HttpReply {
    reply.headers_mut().insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("close"),
    );
    reply
}

struct RetainedRpcBody {
    body: UnsyncBoxBody<Bytes, std::io::Error>,
    _reservation: crate::inbound_rpc::Reply,
    yielded: usize,
    failed: bool,
}

impl RetainedRpcBody {
    fn new(
        body: UnsyncBoxBody<Bytes, std::io::Error>,
        reservation: crate::inbound_rpc::Request,
        deadline_ms: Option<u64>,
    ) -> Result<Self, crate::application::rpc::Code> {
        let reservation = reservation.begin_reply(deadline_ms)?;
        if let Some(upper) = body.size_hint().upper() {
            let capacity = usize::try_from(upper)
                .map_err(|_| crate::application::rpc::Code::ResourceExhausted)?;
            // Ordinary RPC results have an exact length. Refuse their payload
            // before response headers escape, even when business work committed.
            reservation.buffer(capacity)?;
        }
        Ok(Self {
            body,
            _reservation: reservation,
            yielded: 0,
            failed: false,
        })
    }
}

/// Hyper may retain a yielded frame after dropping its body. The shared ticket
/// follows every slice/clone of the bytes until the last transport buffer drops.
struct RetainedRpcData {
    data: Bytes,
    _reservation: crate::inbound_rpc::Reply,
}

impl AsRef<[u8]> for RetainedRpcData {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl Body for RetainedRpcBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        use std::task::Poll;
        if self.failed {
            return Poll::Ready(None);
        }
        let frame = match std::pin::Pin::new(&mut self.body).poll_frame(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => {
                self.failed = true;
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(Some(Ok(frame))) => frame,
        };
        let data = match frame.into_data() {
            Ok(data) => data,
            Err(frame) => return Poll::Ready(Some(Ok(frame))),
        };
        let charged = self
            .yielded
            .checked_add(data.len())
            .ok_or(crate::application::rpc::Code::ResourceExhausted)
            .and_then(|total| {
                self._reservation.buffer(total)?;
                self.yielded = total;
                Ok(())
            });
        if let Err(code) = charged {
            // A bad hint or an unknown-length body cannot bypass admission.
            // After headers this is a broken reply, never successful empty EOF.
            self.failed = true;
            return Poll::Ready(Some(Err(std::io::Error::other(format!(
                "RPC reply refused: {code:?}"
            )))));
        }
        Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_owner(
            RetainedRpcData {
                data,
                _reservation: self._reservation.clone(),
            },
        )))))
    }

    fn is_end_stream(&self) -> bool {
        self.failed || self.body.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        if self.failed {
            hyper::body::SizeHint::with_exact(0)
        } else {
            self.body.size_hint()
        }
    }
}

/// Node A: one RPC through the pooled rpc tunnel.
pub(crate) struct RpcControl<'a> {
    pub(crate) reservation: Option<&'a crate::host_channels::RpcReservation>,
    pub(crate) request_id: Option<crate::engine_api::RequestId>,
    pub(crate) authority: Option<&'a crate::runtime::RpcAuthority>,
    pub(crate) deadline_ms: Option<u64>,
}

pub(crate) async fn rpc(
    peer: &Peer<'_>,
    scope: &str,
    name: Option<&str>,
    method: &str,
    args: &crate::engine_api::RpcData,
    control: RpcControl<'_>,
) -> anyhow::Result<Response<PooledResponseBody>> {
    let (content_type, payload) = rpc_payload(args, control.reservation)?;
    send_inner(
        peer,
        "rpc",
        |body| {
            let mut inner = Request::builder()
                .method("POST")
                .uri("/")
                .header(SCOPE_HEADER, scope)
                .header(METHOD_HEADER, method)
                .header(hyper::header::CONTENT_TYPE, content_type);
            if let Some(name) = name {
                inner = inner.header(NAME_HEADER, name);
            }
            if let Some(authority) = control.authority {
                use base64::Engine;
                authority.validate()?;
                inner = inner.header(
                    AUTHORITY_HEADER,
                    base64::engine::general_purpose::STANDARD
                        .encode(serde_json::to_vec(authority)?),
                );
            }
            if let Some(request_id) = control.request_id {
                inner = inner.header(
                    REQUEST_ID_HEADER,
                    crate::engine_api::request_id_string(request_id),
                );
            }
            Ok(inner.body(body)?)
        },
        crate::engine_api::RequestBody::Bytes(payload),
        control.deadline_ms,
    )
    .await
}

fn rpc_payload(
    args: &crate::engine_api::RpcData,
    reservation: Option<&crate::host_channels::RpcReservation>,
) -> anyhow::Result<(&'static str, Bytes)> {
    Ok(match args {
        crate::engine_api::RpcData::Leased { .. } => {
            anyhow::bail!("leased RPC arguments require a capability session")
        }
        crate::engine_api::RpcData::Json(json) => {
            let mut dense = Vec::new();
            dense.try_reserve_exact(json.len()).map_err(|_| {
                crate::application::rpc::Failure::new(
                    crate::application::rpc::Code::ResourceExhausted,
                )
            })?;
            dense.extend_from_slice(json.as_bytes());
            let bytes = Bytes::from(dense);
            // JSON's String cannot carry a ticket, but its peer body can.
            // Hyper may retain a cloned body after its dispatch is cancelled.
            let bytes = match reservation {
                Some(reservation) => reservation.retain_bytes(bytes),
                None => bytes,
            };
            ("application/json", bytes)
        }
        crate::engine_api::RpcData::Structured(bytes) => {
            ("application/octet-stream", bytes.clone())
        }
    })
}

pub(crate) use crate::ws_tunnel::{claim_upgrade, park_upgrade, TunnelUpgradeClaim};

/// Splice the upgraded client socket to the tunneled owner: the hop stops
/// dispatching WebSocket frames and copies bytes until either side closes.
pub(crate) async fn splice<S>(client: S, parked: TunnelUpgradeClaim) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let upgrade = parked.into_upgrade();
    let inner = upgrade.await?;
    let inner = TokioIo::new(inner);
    let (mut client_read, mut client_write) = tokio::io::split(client);
    let (mut inner_read, mut inner_write) = tokio::io::split(inner);
    crate::asyncrt::select! {
        _ = tokio::io::copy(&mut client_read, &mut inner_write) => {
            // The client went away; the owner sees EOF through the tunnel
            // and runs its own close dispatch.
        }
        can_append_close = copy_owner_to_client(&mut inner_read, &mut client_write) => {
            // A second Close fails browser channels. An incomplete frame
            // would consume a synthetic Close as payload, so only append at
            // a frame boundary before the owner starts closing.
            if can_append_close {
                let client = client_read.unsplit(client_write);
                let mut ws = fastwebsockets::WebSocket::after_handshake(
                    client,
                    fastwebsockets::Role::Server,
                );
                let _ = ws
                    .write_frame(fastwebsockets::Frame::close(1012, b"owner unavailable"))
                    .await;
            }
        }
    }
    Ok(())
}

/// Return whether owner EOF or a read error leaves room for a failure Close.
/// A client write failure ends the splice without any further writes.
async fn copy_owner_to_client<R, W>(owner: &mut R, client: &mut W) -> bool
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut scanner = cellar_logic::schedule::WebSocketCloseScanner::default();
    let mut buffer = [0u8; 8192];
    loop {
        let read = match owner.read(&mut buffer).await {
            Ok(0) | Err(_) => return scanner.can_append_close(),
            Ok(read) => read,
        };
        if client.write_all(&buffer[..read]).await.is_err() {
            return false;
        }
        scanner.observe(&buffer[..read]);
        // Flush before waiting for more owner bytes, including a peer's
        // closing-handshake response. Buffered writers must make progress.
        if client.flush().await.is_err() {
            return false;
        }
    }
}

type TunnelBody = http_body_util::combinators::UnsyncBoxBody<Bytes, std::io::Error>;

fn full_body(bytes: Bytes) -> TunnelBody {
    BodyExt::boxed_unsync(http_body_util::Full::new(bytes).map_err(|never| match never {}))
}

fn stream_body(payload: HttpChunkStream) -> TunnelBody {
    let frames = payload.map(|chunk| {
        chunk
            .map(|data| hyper::body::Frame::data(Bytes::from(data)))
            .map_err(std::io::Error::other)
    });
    BodyExt::boxed_unsync(http_body_util::StreamBody::new(frames))
}

/// The inner response body as the chunk stream the reply path already
/// speaks.
pub(crate) fn response_stream(body: PooledResponseBody) -> HttpChunkStream {
    Box::pin(
        http_body_util::BodyStream::new(body)
            .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) })
            .map_ok(|data: Bytes| data.to_vec())
            .map_err(|error| error.to_string()),
    )
}

#[cfg(all(test, cellar_internal_tests))]
mod private_tests {
    include!(env!("CELLAR_CONFORMANCE_PEER_TUNNEL_TESTS"));
}

#[cfg(test)]
#[path = "peer_tunnel_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "peer_tunnel_reply_tests.rs"]
mod reply_tests;

#[cfg(test)]
#[path = "peer_tunnel_reuse_tests.rs"]
mod reuse_tests;

#[cfg(test)]
#[path = "peer_tunnel_reply_budget_tests.rs"]
mod reply_budget_tests;
