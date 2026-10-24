// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/main.rs.

// Production node bootstrap, connections and shutdown shared by the Rust
// library and compiled application's command-line runner.
#![allow(clippy::disallowed_methods)]

//! Runnable cellar vertical slice.
//!
//! One actor serializes every event through `cellar-logic`; the actor polls its
//! mailbox, timers, lease futures, and task completions together. Other effect
//! work runs on the host runtime so synchronous work cannot starve the core's
//! monotonic lease timers.

use crate::actor::*;
use crate::bucket::Bucket;
use crate::cell_dispatch::{
    dispatch_call_attempt, dispatch_gate, local_dispatch_request_id, local_request_error,
    local_response_stream, requests_websocket_upgrade, split_do_call, CallAttempt, CallPayload,
    RoutedRequestError, WEBSOCKET_WITHOUT_UPGRADE,
};
use crate::engine_api::{
    Compat, HttpResponse, HttpResponseWebSocket, QueueBinding, WorkerConfigOptions, WorkflowBinding,
};
use crate::fleet;
use crate::generation::{
    DeploymentGraph, Generation, GenerationOptions, ReloadOutcome, ReloadRequest, FIRST_GENERATION,
};
use crate::host_channels::DoCallReq;
use crate::host_channels::{AssetCallReq, QueueDispatchReq, RpcCallReq, SvcCallReq, SvcRpcReq};
use crate::ownership_store::{now_ms, BucketOwnership};
use crate::peer_auth::{self, PeerAuth};
use crate::protocol::DeployPointer;
use crate::runtime::{Replication, RuntimeFetch, RuntimeManager, RuntimeOptions};
use crate::wake_entry::ArmGate;
use anyhow::Context as _;
use cellar_logic::{RequestError, Route, WebSocketKind};
use futures_util::stream::{FuturesUnordered, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

#[cfg(test)]
mod capacity_tests;
mod catalog_operator;
mod driver;
#[cfg(not(cellar_internal_tests))]
mod embedded;
mod event_failure;
mod event_inventory;
mod event_metrics;
mod event_reply;
mod event_resources;
mod event_state;
mod event_wait;
mod ingress;
pub(crate) mod operations;
mod raft_metrics;
mod raft_startup;
mod rpc_deadline;
mod source_operator;
mod stream_operator;
mod wake_discovery;
#[cfg(not(cellar_internal_tests))]
pub use embedded::*;
#[cfg(not(cellar_internal_tests))]
pub use ingress::{IngressBody, IngressHandle, IngressResponse};
mod native;
mod native_raft;
use native::Backend;
pub use native::{CompiledApplications, NativeDaemon};
pub use native_raft::{NativeRaftCells, NativeRaftNode};

/// Explicit configuration for the node's shared Raft infrastructure. Cell
/// registration and namespace routing remain separate application contracts.
pub struct NodeRaftConfig {
    pub log: crate::application::consensus::ReplicaLogOptions,
    pub consensus: crate::application::consensus::RaftRuntimeOptions,
    pub transport: crate::application::consensus::TransportOptions,
}

/// Caller-bound peer endpoints with independent control, data and query capacity.
pub struct NodeRaftListeners {
    pub control: tokio::net::TcpListener,
    pub data: tokio::net::TcpListener,
    pub query: tokio::net::TcpListener,
}

/// Infrastructure handles owned by the embedded node's execution domain. The
/// host dispatcher consumes events, routes messages and supplies verified image
/// completions. Transport admission itself is never a write acknowledgement.
/// Recovered groups remain parked until catalog/bootstrap admission attaches them.
#[doc(hidden)]
pub struct NodeRaftService {
    pub log: crate::application::consensus::verification::ReplicaLog,
    pub runtime: crate::application::consensus::verification::RaftRuntime,
    pub events: crate::application::consensus::verification::RuntimeEvents,
    pub transport: crate::application::consensus::verification::RaftTransport,
}

/// Let an admitted HTTP request finish, then close the transport even when a
/// response stream or a client keep-alive does not settle. The semantic drain
/// continues after this bound, so durability and resident activity still use
/// the complete shutdown grace.
const CONNECTION_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Retry a failed shutdown accept quickly enough to keep peer and health
/// traffic available, then reduce repeated error work while the listener is
/// unavailable. The complete shutdown deadline still bounds every retry.
const SHUTDOWN_ACCEPT_RETRY_INITIAL: std::time::Duration = std::time::Duration::from_millis(10);
const SHUTDOWN_ACCEPT_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(1);

/// The lossy stdout writer's flush handle. Every exit path uses
/// `std::process::exit`, which skips destructors — but the last lines before
/// an exit are the fence forensics, exactly the lines that must survive.
static LOG_GUARD: std::sync::Mutex<Option<tracing_appender::non_blocking::WorkerGuard>> =
    std::sync::Mutex::new(None);

#[cfg(all(test, cellar_internal_tests))]
// Keep the client open until the child exits, so the accepted connection
// reaches the real drain-time HTTP path instead of closing immediately.
static SHUTDOWN_ACCEPT_FAILURE_CLIENT: std::sync::Mutex<Option<std::net::TcpStream>> =
    std::sync::Mutex::new(None);

#[cfg(all(test, cellar_internal_tests))]
fn shutdown_accept_failure_test_checkpoint(active: bool, step: &'static str) {
    if active {
        // The tracing writer is deliberately lossy and asynchronous. This
        // process regression needs every cleanup checkpoint to remain
        // visible and ordered when the child exits through process::exit.
        crate::cli_output::Output::new(crate::cli_output::Format::Text)
            .line(format_args!(
                "shutdown accept failure test checkpoint: {step}"
            ))
            .expect("write the shutdown accept failure test checkpoint");
    }
}

static CONNECTION_ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_REPORTED: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERROR_LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_INCOMPLETE: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_TIMEOUT: AtomicU64 = AtomicU64::new(0);
static CONNECTION_ERRORS_OTHER: AtomicU64 = AtomicU64::new(0);

fn record_connection_error(
    error: &hyper::Error,
    surface: HttpSurface,
    peer: Option<std::net::SocketAddr>,
) {
    static STARTED: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let total = CONNECTION_ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed) + 1;
    let (cause, cause_total) = if error.is_incomplete_message() {
        (
            "incomplete_message",
            CONNECTION_ERRORS_INCOMPLETE.fetch_add(1, Ordering::Relaxed) + 1,
        )
    } else if error.is_timeout() {
        (
            "timeout",
            CONNECTION_ERRORS_TIMEOUT.fetch_add(1, Ordering::Relaxed) + 1,
        )
    } else {
        (
            "other",
            CONNECTION_ERRORS_OTHER.fetch_add(1, Ordering::Relaxed) + 1,
        )
    };
    let elapsed_ms = STARTED.get_or_init(Instant::now).elapsed().as_millis() as u64;
    let last = CONNECTION_ERROR_LAST_LOG_MS.load(Ordering::Relaxed);
    let observed = elapsed_ms.max(1);
    if (last != 0 && elapsed_ms.saturating_sub(last) < 1_000)
        || CONNECTION_ERROR_LAST_LOG_MS
            .compare_exchange(last, observed, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    let reported = CONNECTION_ERRORS_REPORTED.swap(total, Ordering::Relaxed);
    tracing::warn!(
        event = "http_connection_failures",
        total,
        since_last = total.saturating_sub(reported),
        cause,
        cause_total,
        surface = surface.name(),
        ?peer,
        %error,
        "HTTP connections failed"
    );
}

fn exit_flushed(code: i32) -> ! {
    drop(LOG_GUARD.lock().unwrap().take());
    std::process::exit(code);
}

/// Await shutdown work only while the orchestrator-compatible process budget
/// remains. A zero budget does not poll the future, so best-effort store and
/// diagnostic work cannot begin after the absolute deadline.
async fn before_process_deadline<F>(deadline: tokio::time::Instant, future: F) -> Option<F::Output>
where
    F: std::future::Future,
{
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return None;
    }
    tokio::time::timeout(remaining, future).await.ok()
}

/// Finish process-wide WebSocket frame work before connection tasks can
/// unregister the sockets that receive those frames.
async fn flush_shutdown_connections<Frames, Connections>(
    process_deadline: tokio::time::Instant,
    flush_frames: Frames,
    flush_connections: Connections,
) where
    Frames: std::future::Future<Output = ()>,
    Connections: std::future::Future<Output = ()>,
{
    // The frames a flush still holds are behind a durability gate, and a
    // socket whose peer is gone can never deliver them: a `cellar dev` stop
    // on OrbStack sat here for its whole process deadline once the
    // container behind an outbound socket had been destroyed. The grace is
    // for frames that can still land; the durability of the writes behind
    // them is the quiesce that follows, not this wait.
    match before_process_deadline(
        process_deadline,
        tokio::time::timeout(CONNECTION_DRAIN_GRACE, flush_frames),
    )
    .await
    {
        None => return,
        Some(Err(_)) => tracing::warn!(
            event = "shutdown_frame_flush_cut",
            grace_ms = CONNECTION_DRAIN_GRACE.as_millis() as u64,
            "WebSocket frames still held at the flush grace were dropped"
        ),
        Some(Ok(())) => {}
    }
    let _ = before_process_deadline(
        process_deadline,
        tokio::time::timeout(CONNECTION_DRAIN_GRACE, flush_connections),
    )
    .await;
}

// Keep the runtime owner and the follower fallback in one consumed selection.
// The process must finalize this value before it starts application services,
// so a caller cannot forget the follower-only owner on a non-runtime path.
struct DurabilityOwnerSelection {
    runtime_owner: Option<crate::node_log::DurabilityOwner>,
    follower: Option<Arc<crate::node_log::FollowerStore>>,
}

impl DurabilityOwnerSelection {
    fn new(follower: Option<Arc<crate::node_log::FollowerStore>>) -> Self {
        Self {
            runtime_owner: None,
            follower,
        }
    }

    fn install_runtime(&mut self, owner: crate::node_log::DurabilityOwner) {
        assert!(
            self.runtime_owner.is_none(),
            "the runtime durability owner was already installed"
        );
        self.runtime_owner = Some(owner);
    }

    fn select(self) -> Option<crate::node_log::DurabilityOwner> {
        self.runtime_owner.or_else(|| {
            self.follower
                .map(crate::node_log::DurabilityOwner::new_follower)
        })
    }
}

// Keep the phase-one durability result inside the complete preparation
// decision. A timeout runs fallback, so it must override a successful drain.
fn clean_reload_is_eligible(
    durability_quiesced: bool,
    drained: bool,
    shutdown_mode: ShutdownMode,
) -> bool {
    durability_quiesced && drained && shutdown_mode == ShutdownMode::Preserve
}

type HttpReply = Response<UnsyncBoxBody<Bytes, std::io::Error>>;

const STALE_ROUTE_HEADER: &str = "x-cells-route-error";
const STALE_ROUTE_VALUE: &str = "stale-owner";
/// Marks a forwarded fetch whose handler failed on the owner, so the caller's
/// `stub.fetch()` rejects as it does for a local owner. Without the marker a
/// remote handler failure looks like an application 500 response.
const HANDLER_FAILURE_HEADER: &str = "x-cells-handler-failure";
const HANDLER_FAILURE_PREFIX: &str = "cell Worker failed: ";

/// A handler failure that the owner reported for a forwarded fetch. The owner
/// ran the handler, so a retry would run it a second time.
#[derive(Debug)]
struct RemoteHandlerFailure(String);

impl std::fmt::Display for RemoteHandlerFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RemoteHandlerFailure {}

const DURABLE_OBJECT_ROUTING_ERROR_MARKER: &str = "__CELLAR_DO_ROUTING_ERROR__:";

fn owner_unreachable(scope: &str, owner: &str, source: anyhow::Error) -> anyhow::Error {
    // Record how the attempt failed, not just that it did. `connect` is the
    // one that decides whether a retry is safe -- a request that never left
    // this node may be re-sent, a truncated read may not, because the owner
    // already ran it. Without these an operator cannot tell an unreachable
    // peer from one that answered badly, and neither can a bug report.
    let transport = source.downcast_ref::<reqwest::Error>();
    // The full chain, because the root cause is the actionable part: a
    // "connect error" one level down still hides refused vs. out-of-fds.
    let cause = format!("{source:#}");
    tracing::warn!(
        %scope,
        %owner,
        error = %source,
        %cause,
        connect = transport.is_some_and(reqwest::Error::is_connect),
        timeout = transport.is_some_and(reqwest::Error::is_timeout),
        request = transport.is_some_and(reqwest::Error::is_request),
        body = transport.is_some_and(reqwest::Error::is_body),
        decode = transport.is_some_and(reqwest::Error::is_decode),
        "peer owner unreachable"
    );
    let detail = serde_json::json!({
        "scope": scope,
        "owner": owner,
    });
    source.context(format!("{DURABLE_OBJECT_ROUTING_ERROR_MARKER}{detail}"))
}

fn response(status: StatusCode, body: impl Into<Bytes>) -> HttpReply {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            Full::new(body.into())
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("static HTTP response")
}

fn asset_response(response: axum::response::Response) -> HttpReply {
    response.map(|body| body.map_err(std::io::Error::other).boxed_unsync())
}

fn peer_response(mut response: HttpReply) -> HttpReply {
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static(peer_auth::RESPONSE_VERSION_HEADER),
        hyper::header::HeaderValue::from_static(peer_auth::PROTOCOL_VERSION_TEXT),
    );
    response
}

/// Decides whether a Worker-set response header reaches the caller. The
/// `connection` and `transfer-encoding` headers describe the Worker-side hop,
/// and the host frames every body it sends, so a Worker `content-length` is
/// stale framing it must not repeat — except on a HEAD response, where the
/// body the host measures is empty and the Worker's value is the only
/// representation length the client can get. RFC 9110 forbids the header on a
/// 1xx or 204 status, so those never carry one even on a HEAD path.
fn forwards_worker_header(
    name: &str,
    preserve_representation_length: bool,
    status: StatusCode,
) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "connection" | "transfer-encoding"
    ) && (!name.eq_ignore_ascii_case("content-length")
        || (preserve_representation_length
            && !status.is_informational()
            && status != StatusCode::NO_CONTENT))
}

fn runtime_response(
    worker_response: crate::engine_api::HttpResponse,
    preserve_representation_length: bool,
) -> HttpReply {
    let Ok(status) = StatusCode::from_u16(worker_response.status) else {
        return response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker status");
    };
    let mut builder = Response::builder().status(status);
    for (name, value) in worker_response.headers {
        if !forwards_worker_header(&name, preserve_representation_length, status) {
            continue;
        }
        builder = builder.header(name, value);
    }
    let body = match worker_response.stream {
        Some(stream) => {
            let chunks = stream.map(|chunk| {
                chunk
                    .map(|bytes| Frame::data(Bytes::from(bytes)))
                    .map_err(std::io::Error::other)
            });
            StreamBody::new(chunks).boxed_unsync()
        }
        None => Full::new(Bytes::from(worker_response.body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    };
    builder
        .body(body)
        .unwrap_or_else(|_| response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker headers"))
}

#[derive(Debug)]
struct StalePeerRoute {
    scope: String,
}

impl std::fmt::Display for StalePeerRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "peer no longer owns {}", self.scope)
    }
}

impl std::error::Error for StalePeerRoute {}

/// Identifies a failed remote RPC exchange without authorizing general replay.
#[derive(Debug)]
struct RemoteRpcTransportFailure;

impl std::fmt::Display for RemoteRpcTransportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("remote RPC transport failed")
    }
}

impl std::error::Error for RemoteRpcTransportFailure {}

fn remote_rpc_failure(error: anyhow::Error) -> anyhow::Error {
    if error.downcast_ref::<reqwest::Error>().is_some()
        || error.downcast_ref::<hyper::Error>().is_some()
        || error.downcast_ref::<std::io::Error>().is_some()
    {
        error.context(RemoteRpcTransportFailure)
    } else {
        // Header/protocol/authority refusals are not transport failures and
        // must not enter the activation-only replay loop.
        error.context("remote RPC transport failed")
    }
}

/// Preserve the owner's durability verdict and handler failure in a peer reply.
fn peer_gate_failure(verdict: RequestError, handler: Option<&anyhow::Error>) -> HttpReply {
    let detail = match handler {
        Some(handler) => format!("; the handler failed: {handler:#}"),
        None => String::new(),
    };
    let mut reply = peer_response(response(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("durability unproven: {verdict:?}{detail}"),
    ));
    reply.headers_mut().insert(
        hyper::header::HeaderName::from_static(event_failure::HEADER),
        hyper::header::HeaderValue::from_static(event_failure::header(event_failure::gate_code(
            verdict,
        ))),
    );
    reply
}

fn classify_remote_attempt(error: &anyhow::Error) -> cellar_logic::routing::Attempt {
    if error.downcast_ref::<StalePeerRoute>().is_some()
        || error
            .downcast_ref::<peer_tunnel::StaleTunnelRoute>()
            .is_some()
    {
        cellar_logic::routing::Attempt::NotOwner
    } else if error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_connect)
    {
        cellar_logic::routing::Attempt::NeverConnected
    } else {
        cellar_logic::routing::Attempt::Ambiguous
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteRetryAction {
    Stop,
    Redispatch,
    WaitForOwner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RemoteRetryOutcome {
    Ready,
    Stop,
    Cancelled,
}

/// Keep the one-retry transport policy, but do not spend that retry on an
/// ownership generation which a reachable peer already rejected.
///
/// A replacement can bind the prior address while the prior session's lease
/// is still live. The new process then gives a definitive `NotOwner` answer
/// for the old `(node, epoch)`. An immediate redispatch resolves that same
/// generation and fails a safe application retry before ownership can move.
struct RemoteRouteRetry {
    dispatcher: cellar_logic::routing::Dispatcher,
    generation: Option<(String, u64, Option<u64>)>,
    blocked: bool,
    deadline_ms: u64,
    next_wait_ms: u64,
}

impl RemoteRouteRetry {
    const INITIAL_WAIT_MS: u64 = 10;
    const MAX_WAIT_MS: u64 = 250;

    fn new(operation_deadline_ms: u64) -> Self {
        Self::until(crate::asyncrt::mono_ms().saturating_add(operation_deadline_ms))
    }

    fn until(deadline_ms: u64) -> Self {
        Self {
            dispatcher: cellar_logic::routing::Dispatcher::default(),
            generation: None,
            blocked: false,
            deadline_ms,
            next_wait_ms: Self::INITIAL_WAIT_MS,
        }
    }

    /// Return true when this route is known not to be usable yet.
    fn observe(&mut self, node: &str, epoch: u64, capacity_sample_ms: Option<u64>) -> bool {
        let changed =
            self.generation
                .as_ref()
                .is_none_or(|(current_node, current_epoch, current_sample)| {
                    current_node != node
                        || *current_epoch != epoch
                        || *current_sample != capacity_sample_ms
                });
        if changed {
            self.generation = Some((node.to_string(), epoch, capacity_sample_ms));
            self.blocked = false;
            self.dispatcher = cellar_logic::routing::Dispatcher::default();
            self.next_wait_ms = Self::INITIAL_WAIT_MS;
        }
        self.blocked
    }

    fn failed(&mut self, attempt: cellar_logic::routing::Attempt) -> RemoteRetryAction {
        match attempt {
            cellar_logic::routing::Attempt::Ambiguous => RemoteRetryAction::Stop,
            cellar_logic::routing::Attempt::NotOwner => {
                if !self.dispatcher.redispatch(attempt) {
                    return RemoteRetryAction::Stop;
                }
                self.blocked = true;
                RemoteRetryAction::WaitForOwner
            }
            cellar_logic::routing::Attempt::NeverConnected
                if self.dispatcher.redispatch(attempt) =>
            {
                RemoteRetryAction::Redispatch
            }
            cellar_logic::routing::Attempt::NeverConnected => RemoteRetryAction::Stop,
        }
    }

    /// Apply one failed attempt's policy and discard its observed route. A
    /// stopped caller queues cleanup without waiting or authorizing replay.
    async fn retry(
        &mut self,
        app: &AppHandle,
        scope: &str,
        attempt: cellar_logic::routing::Attempt,
        cancel: Option<&mut oneshot::Receiver<()>>,
        surface: Option<&'static str>,
    ) -> RemoteRetryOutcome {
        let action = self.failed(attempt);
        let (node, epoch, _) = self.generation.as_ref().expect("an observed remote route");
        tracing::warn!(
            event = "remote_route_retry",
            %scope,
            %node,
            epoch,
            ?attempt,
            ?action,
            surface,
            "a peer attempt changed the remote route retry state"
        );
        let outcome = match action {
            RemoteRetryAction::Stop => RemoteRetryOutcome::Stop,
            RemoteRetryAction::Redispatch => RemoteRetryOutcome::Ready,
            RemoteRetryAction::WaitForOwner => self.wait(cancel).await,
        };
        if outcome == RemoteRetryOutcome::Ready {
            self.invalidate(app, scope).await;
        } else {
            let (node, epoch, sample) = self.generation.as_ref().expect("an observed remote route");
            drop(app.queue_remote_invalidation(scope.to_string(), node.clone(), *epoch, *sample));
        }
        outcome
    }

    async fn invalidate(&self, app: &AppHandle, scope: &str) {
        let (node, epoch, sample) = self.generation.as_ref().expect("an observed remote route");
        let _ = app
            .queue_remote_invalidation(scope.to_string(), node.clone(), *epoch, *sample)
            .await;
    }

    async fn wait(&mut self, cancel: Option<&mut oneshot::Receiver<()>>) -> RemoteRetryOutcome {
        let remaining_ms = self.deadline_ms.saturating_sub(crate::asyncrt::mono_ms());
        if remaining_ms == 0 {
            return RemoteRetryOutcome::Stop;
        }
        let wait = std::time::Duration::from_millis(remaining_ms.min(self.next_wait_ms));
        self.next_wait_ms = self.next_wait_ms.saturating_mul(2).min(Self::MAX_WAIT_MS);
        match cancel {
            Some(cancel) => crate::asyncrt::select_biased! {
                "a lifecycle cancellation wins a tie with route retry backoff";
                _ = cancel => RemoteRetryOutcome::Cancelled,
                _ = crate::asyncrt::sleep(wait) => RemoteRetryOutcome::Ready,
            },
            None => {
                crate::asyncrt::sleep(wait).await;
                RemoteRetryOutcome::Ready
            }
        }
    }

    async fn wait_capacity(
        &mut self,
        cancel: Option<&mut oneshot::Receiver<()>>,
    ) -> RemoteRetryOutcome {
        // Candidate changes reset the owner backoff, but must not reset the
        // original operation deadline. Keep the capacity loop's minimum pace
        // to avoid exhausting ephemeral ports on repeated tunnel refusals.
        self.next_wait_ms = self.next_wait_ms.max(25);
        match self.wait(cancel).await {
            RemoteRetryOutcome::Ready if crate::asyncrt::mono_ms() >= self.deadline_ms => {
                RemoteRetryOutcome::Stop
            }
            outcome => outcome,
        }
    }
}

struct WebSocketRouteTiming {
    started: Instant,
    route_resolution_us: u64,
    dispatch_us: u64,
    attempts: u8,
}

impl WebSocketRouteTiming {
    fn emit(
        &self,
        app: &AppHandle,
        scope: &str,
        request_id: Option<crate::engine_api::RequestId>,
        outcome: &str,
        route: &str,
        peer_node: &str,
    ) {
        let request_id = request_id
            .map(crate::engine_api::request_id_string)
            .unwrap_or_default();
        let (node, region) = app
            .runtime
            .as_ref()
            .map_or(("", ""), |runtime| (runtime.node(), runtime.region()));
        tracing::debug!(
            target: "timing",
            event = "websocket_route_timing",
            outcome,
            route,
            peer_node,
            scope,
            request_id,
            node,
            region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            attempts = self.attempts,
            total_us = self.started.elapsed().as_micros() as u64,
            route_resolution_us = self.route_resolution_us,
            dispatch_us = self.dispatch_us,
            "WebSocket cell request resolved"
        );
    }
}

/// A facet stream request from the isolate that runs the facet.
async fn dispatch_facet(app: AppHandle, req: crate::host_channels::FacetReq) {
    use crate::host_channels::FacetReq;
    let Some(runtime) = app.runtime.as_ref() else {
        let error = || anyhow::anyhow!("no cell runtime");
        match req {
            FacetReq::Open { reply, .. } => drop(reply.send(Err(error()))),
            FacetReq::Delete { reply, .. } => drop(reply.send(Err(error()))),
            FacetReq::DeleteBatch { reply, .. } => drop(reply.send(Err(error()))),
            FacetReq::Prove { reply, .. } => drop(reply.send(Err(error()))),
        }
        return;
    };
    match req {
        FacetReq::Open {
            authority,
            root,
            epoch,
            names,
            reply,
        } => drop(
            reply.send(
                runtime
                    .open_facet_authorized(&root, epoch, &names, authority.as_ref())
                    .await,
            ),
        ),
        FacetReq::Delete {
            root,
            epoch,
            names,
            reply,
        } => drop(reply.send(runtime.delete_facet(&root, epoch, &names).await)),
        FacetReq::DeleteBatch {
            root,
            epoch,
            names,
            reply,
        } => drop(reply.send(runtime.delete_facet_batch(&root, epoch, &names).await)),
        FacetReq::Prove {
            stream,
            epoch,
            reply,
        } => drop(reply.send(runtime.prove_facet(&stream, epoch).await)),
    }
}

/// Propagates a forwarding-side cancellation over the authenticated peer
/// protocol. Dropping a Reqwest future does not guarantee that its pooled
/// HTTP/1 transport closes, so the owner cannot use transport EOF alone.
struct RemoteFetchAbortGuard {
    domain: Option<crate::asyncrt::DomainToken>,
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
    node: String,
    addr: String,
    scope: String,
    request: Option<crate::engine_api::RequestId>,
    phase: &'static str,
}

impl RemoteFetchAbortGuard {
    fn new(
        app: &AppHandle,
        node: String,
        addr: String,
        scope: String,
        request: Option<crate::engine_api::RequestId>,
    ) -> Self {
        Self {
            domain: crate::asyncrt::services().domain_token(),
            http: app.peer_http.clone(),
            auth: app.peer_auth.clone(),
            node,
            addr,
            scope,
            request,
            phase: "response_head",
        }
    }

    fn body_active(&mut self) {
        self.phase = "response_body";
    }

    fn disarm(&mut self) {
        self.request = None;
    }
}

impl Drop for RemoteFetchAbortGuard {
    fn drop(&mut self) {
        let Some(request) = self.request.take() else {
            return;
        };
        let http = self.http.clone();
        let auth = self.auth.clone();
        let node = self.node.clone();
        let addr = self.addr.clone();
        let scope = self.scope.clone();
        let phase = self.phase;
        let Some(domain) = &self.domain else {
            return;
        };
        let _ = domain.spawn_cleanup(async move {
            let abort = send_peer_abort(http, auth, node.clone(), addr, scope.clone(), request);
            match tokio::time::timeout(std::time::Duration::from_secs(2), abort).await {
                Ok(Ok(())) => tracing::debug!(
                    event = "peer_fetch_abort_sent",
                    %node,
                    %scope,
                    request_id = %crate::engine_api::request_id_string(request),
                    phase,
                    "propagated a disconnected caller to the cell owner"
                ),
                Ok(Err(error)) => tracing::debug!(
                    event = "peer_fetch_abort_failed",
                    %node,
                    %scope,
                    %error,
                    "could not propagate a disconnected caller to the cell owner"
                ),
                Err(_) => tracing::debug!(
                    event = "peer_fetch_abort_timed_out",
                    %node,
                    %scope,
                    "timed out propagating a disconnected caller to the cell owner"
                ),
            }
        });
    }
}

async fn send_peer_abort(
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
    node: String,
    addr: String,
    scope: String,
    request: crate::engine_api::RequestId,
) -> anyhow::Result<()> {
    let encoded_scope =
        percent_encoding::utf8_percent_encode(&scope, percent_encoding::NON_ALPHANUMERIC);
    let path = format!(
        "/peer/abort/{encoded_scope}/{}",
        crate::engine_api::request_id_string(request)
    );
    let request = auth.sign(
        http.post(format!("http://{addr}{path}")),
        "POST",
        &path,
        &[],
        &node,
    )?;
    let response = request.body(Vec::new()).send().await?;
    peer_auth::validate_response(response.headers())?;
    anyhow::ensure!(
        response.status().is_success(),
        "peer abort failed with {}",
        response.status()
    );
    Ok(())
}

/// Call the reserved cron cell's arm endpoint, through the ordinary Durable
/// Object routing path so ownership resolution and remote forwarding are the
/// ones every other call gets.
async fn arm_cron_schedule(app: AppHandle, cell: String) -> anyhow::Result<()> {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let body = crate::engine_api::RequestBody::Bytes(Bytes::new());
    dispatch_do_call(
        app,
        DoCallReq {
            request_id: None,
            cancel: None,
            deliver_abort_to_handler: false,
            scope: cell,
            name: None,
            url: "https://cron.cellar.internal/arm".to_string(),
            method: "POST".to_string(),
            body_guard: crate::http_streams::RequestBodyGuard::of(&body),
            body,
            headers: Vec::new(),
            reply,
            order: None,
            parent: None,
        },
    )
    .await;
    let response = receive.await.context("cron arm dispatch dropped")??;
    if !(200..300).contains(&response.status) {
        anyhow::bail!("cron arm returned status {}", response.status);
    }
    Ok(())
}

/// Arm the current deployment's cron schedule, if it declares one.
///
/// A cron cell has no client to wake it, so somebody has to make the first
/// call; every node makes it, and ownership CAS decides which one keeps the
/// cell while the others route to that owner. No new election, no reserved
/// node -- the same arbiter that makes an alarm fire once per fleet makes a
/// cron trigger fire once too. Boot and adoption both come through here, so
/// a schedule change arrives with the deployment that carries it.
fn spawn_cron_arm(app: AppHandle) {
    let Some(cell) = app.runtime.as_ref().and_then(|runtime| runtime.cron_cell()) else {
        return;
    };
    crate::asyncrt::spawn(async move {
        // Routing needs node authority, exactly as the wake scan does.
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        while !app.healthy().await {
            if Instant::now() >= deadline {
                tracing::warn!(cell, "cron schedule not armed: no node authority");
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // A failed arm leaves the schedule silent until the next adoption,
        // so retry rather than log once. The backoff is bounded because a
        // fleet that cannot route for a minute has a larger problem.
        for attempt in 0..5 {
            match arm_cron_schedule(app.clone(), cell.clone()).await {
                Ok(()) => return,
                Err(error) if attempt == 4 => {
                    tracing::error!(cell, %error, "cron schedule not armed");
                }
                Err(error) => {
                    tracing::warn!(cell, %error, attempt, "cron arm failed, retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(500 << attempt.min(5)))
                        .await;
                }
            }
        }
    })
    .detach();
}

/// Pointer transport and asset capability captured once by the CLI host.
struct DeploymentResources {
    bucket: Bucket,
    assets: crate::assets::AssetCache,
}

/// Adopt the deployment `pointer` names.
///
/// The same two calls boot makes -- `DeploymentGraph::load` and
/// `Generation::build` -- then the flip and the cron arm. A build that fails
/// leaves the current generation serving; nothing has changed until `adopt`.
async fn adopt_deployment(
    app: &AppHandle,
    deployment: &DeploymentResources,
    node: &str,
    region: &str,
    pointer: DeployPointer,
    force: bool,
    backend: &Backend,
) -> ReloadOutcome {
    let Some(runtime) = app.runtime.as_ref() else {
        return ReloadOutcome::Failed {
            version: pointer.version,
            prefix: pointer.prefix,
            error: "this node has no Worker runtime".to_string(),
        };
    };
    let current = runtime.generation();
    let (expressions, schedule_revision) =
        match crate::deployment_cron::desired(&deployment.bucket, &pointer).await {
            Ok(desired) => desired,
            Err(error) => {
                return ReloadOutcome::Failed {
                    version: pointer.version,
                    prefix: pointer.prefix,
                    error: format!("read cron configuration: {error:#}"),
                }
            }
        };
    if !force
        && pointer.version == current.version()
        && pointer.prefix == current.prefix()
        && schedule_revision.as_deref() == current.cron_revision()
        && expressions == current.primary_crons
    {
        return ReloadOutcome::Unchanged {
            generation: current.id(),
            version: current.version().to_string(),
        };
    }
    let failed = |error: String| ReloadOutcome::Failed {
        version: pointer.version.clone(),
        prefix: pointer.prefix.clone(),
        error,
    };
    let graph =
        match DeploymentGraph::load(&deployment.bucket, node.to_string(), &deployment.assets).await
        {
            Ok(graph) => graph,
            Err(error) => return failed(format!("{error:#}")),
        };
    let id = runtime.next_generation_id();
    let options = GenerationOptions {
        node: node.to_string(),
        region: region.to_string(),
    };
    // Source compilation and compiled application factories both run away
    // from the routing and lease timer threads.
    let backend = backend.clone();
    let build = match crate::asyncrt::try_blocking(move || backend.build(id, graph, options)) {
        Ok(build) => build,
        Err(error) => return failed(format!("generation build refused: {error}")),
    };
    let built = build.await;
    let generation = match built {
        Ok(Ok(generation)) => generation,
        Ok(Err(error)) => return failed(format!("{error:#}")),
        Err(error) => return failed(format!("generation build panicked: {error}")),
    };
    if let Err(error) = event_resources::activate(app.clone(), &generation).await {
        return failed(format!("{error:#}"));
    }
    let adopted = runtime.adopt(generation);
    // Tell the core, so resident cells move to the new generation at their
    // safe points. The reserved cells move at once, ahead of the cron arm
    // below, which must reach a cron cell already running the new schedule.
    let _ = app.tx.send(Message::GenerationChanged {
        generation: adopted.id(),
        max_age_ms: crate::generation::max_age().as_millis() as u64,
        eager_classes: adopted.reserved_classes(),
    });
    spawn_cron_arm(app.clone());
    ReloadOutcome::Adopted {
        generation: adopted.id(),
        version: adopted.version().to_string(),
        prefix: adopted.prefix().to_string(),
    }
}

/// The pointer watcher: the node's one reader of `deploy/current.json`
/// after boot.
///
/// It polls on a slow interval and adopts what the pointer names through
/// `adopt_deployment`; a `POST /reload` or a managed notification only makes
/// it look now. The pointer is the authority, so a missed nudge degrades to
/// "adopts one interval later". A version that failed to build is remembered
/// and not retried until the pointer names something else or a reload is
/// forced: a broken bundle must not recompile every interval on every node.
fn start_pointer_watcher(
    app: AppHandle,
    deployment: DeploymentResources,
    node: String,
    region: String,
    mut requests: crate::generation::ReloadReceiver,
    backend: Backend,
) {
    crate::asyncrt::spawn(async move {
        let period = crate::generation::poll_interval();
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut failed: Option<ReloadOutcome> = None;
        let mut last_schedule: Option<(Vec<String>, Option<String>)> = None;
        loop {
            let request = crate::asyncrt::select! {
                _ = tick.tick() => ReloadRequest { force: false, reply: None },
                request = requests.recv() => match request {
                    Some(request) => request,
                    None => return,
                },
            };
            let desired = async {
                let pointer = fleet::read_current_pointer(&deployment.bucket).await?;
                let schedule =
                    crate::deployment_cron::desired(&deployment.bucket, &pointer).await?;
                Ok::<_, anyhow::Error>((pointer, schedule))
            }
            .await;
            let outcome = match desired {
                Err(error) => {
                    tracing::warn!(event = "deployment_pointer_unreadable", %error);
                    ReloadOutcome::Failed {
                        version: String::new(),
                        prefix: String::new(),
                        error: format!("{error:#}"),
                    }
                }
                Ok((pointer, schedule)) => {
                    if Some(&schedule) != last_schedule.as_ref() {
                        failed = None;
                        last_schedule = Some(schedule);
                    }
                    match &failed {
                        Some(ReloadOutcome::Failed {
                            version, prefix, ..
                        }) if !request.force
                            && version == &pointer.version
                            && prefix == &pointer.prefix =>
                        {
                            failed.clone().expect("matched above")
                        }
                        _ => {
                            adopt_deployment(
                                &app,
                                &deployment,
                                &node,
                                &region,
                                pointer,
                                request.force,
                                &backend,
                            )
                            .await
                        }
                    }
                }
            };
            match &outcome {
                ReloadOutcome::Adopted {
                    generation,
                    version,
                    prefix,
                } => {
                    failed = None;
                    tracing::info!(
                        event = "deployment_adopted",
                        generation,
                        version = %version,
                        prefix = %prefix,
                        "deployment adopted in place"
                    );
                }
                ReloadOutcome::Unchanged { .. } => {}
                ReloadOutcome::Failed {
                    version,
                    prefix,
                    error,
                } => {
                    if failed.as_ref() != Some(&outcome) {
                        tracing::error!(
                            event = "deployment_adoption_failed",
                            version = %version,
                            prefix = %prefix,
                            %error,
                            "deployment did not build; the current generation keeps serving"
                        );
                    }
                    if !version.is_empty() {
                        failed = Some(outcome.clone());
                    }
                }
            }
            if let Some(reply) = request.reply {
                let _ = reply.send(outcome);
            }
        }
    })
    .detach();
}

/// `POST /reload`: adopt the pointer's deployment now and report the outcome.
/// Forced, so an unchanged pointer still builds a new generation and a vars
/// file edit takes effect.
/// Pause or resume balancing. The node publishes the flag in its lease, and
/// one paused lease stops every donor in the fleet, so an operator pauses
/// the fleet from any node and resumes it from the same one.
fn internal_rebalance_switch(paused: bool) -> HttpReply {
    if !crate::ownership_store::set_rebalance_paused(paused) {
        return response(StatusCode::CONFLICT, "this node publishes no lease");
    }
    response(StatusCode::OK, format!("{{\"rebalance_paused\":{paused}}}"))
}

async fn internal_reload(app: AppHandle) -> HttpReply {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let sent = app
        .reload
        .send(ReloadRequest {
            force: true,
            reply: Some(reply),
        })
        .is_ok();
    let outcome = if sent { receive.await.ok() } else { None };
    let (status, body) = match outcome {
        Some(ReloadOutcome::Adopted {
            generation,
            version,
            prefix,
        }) => (
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "outcome": "adopted",
                "generation": generation,
                "version": version,
                "prefix": prefix,
            }),
        ),
        Some(ReloadOutcome::Unchanged {
            generation,
            version,
        }) => (
            StatusCode::OK,
            serde_json::json!({
                "ok": true,
                "outcome": "unchanged",
                "generation": generation,
                "version": version,
            }),
        ),
        Some(ReloadOutcome::Failed {
            version,
            prefix,
            error,
        }) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "ok": false,
                "outcome": "failed",
                "version": version,
                "prefix": prefix,
                "error": error,
            }),
        ),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({
                "ok": false,
                "outcome": "unavailable",
                "error": "this node has no deployment pointer to reload from",
            }),
        ),
    };
    response(status, body.to_string())
}

async fn dispatch_do_call(app: AppHandle, call: DoCallReq) {
    let (mut call, mut payload, reply) = split_do_call(call);
    let mut websocket_timing =
        requests_websocket_upgrade(&payload.headers).then(|| WebSocketRouteTiming {
            started: Instant::now(),
            route_resolution_us: 0,
            dispatch_us: 0,
            attempts: 0,
        });
    let operation = async {
        let mut dispatcher = RemoteRouteRetry::new(app.operation_deadline_ms);
        loop {
            if let Some(timing) = websocket_timing.as_mut() {
                timing.attempts = timing.attempts.saturating_add(1);
            }
            let route_started = Instant::now();
            // The routed hook stamps the boundary between route resolution and
            // local dispatch; the library owns that moment and cannot read the
            // clock. A remote route leaves the stamp unused, because remote
            // dispatch time starts after the stale-route pacing below.
            let mut dispatch_started = route_started;
            let attempt = dispatch_call_attempt(&app, &mut call, payload, || {
                if let Some(timing) = websocket_timing.as_mut() {
                    timing.route_resolution_us = timing
                        .route_resolution_us
                        .saturating_add(route_started.elapsed().as_micros() as u64);
                }
                dispatch_started = Instant::now();
            })
            .await;
            let (node, addr, epoch, peer_protocol, capacity_sample_ms) = match attempt {
                CallAttempt::Unstarted(error) => break Err(error),
                CallAttempt::RouteError(error) => {
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.route_resolution_us = timing
                            .route_resolution_us
                            .saturating_add(route_started.elapsed().as_micros() as u64);
                        timing.emit(&app, &call.scope, call.request_id, "route_error", "", "");
                    }
                    break Err(error);
                }
                CallAttempt::Local(result) => {
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.dispatch_us = timing
                            .dispatch_us
                            .saturating_add(dispatch_started.elapsed().as_micros() as u64);
                        timing.emit(
                            &app,
                            &call.scope,
                            call.request_id,
                            if result.is_ok() { "ok" } else { "error" },
                            "local",
                            app.runtime.as_ref().map_or("", |runtime| runtime.node()),
                        );
                    }
                    break result;
                }
                CallAttempt::RefusedWebSocket { error, websocket } => {
                    websocket::reject_accepted_websocket(&app, &websocket, &error.to_string())
                        .await;
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.dispatch_us = timing
                            .dispatch_us
                            .saturating_add(dispatch_started.elapsed().as_micros() as u64);
                        timing.emit(
                            &app,
                            &call.scope,
                            call.request_id,
                            "error",
                            "local",
                            app.runtime.as_ref().map_or("", |runtime| runtime.node()),
                        );
                    }
                    break Err(error);
                }
                CallAttempt::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                    capacity_sample_ms,
                    payload: returned,
                } => {
                    // The attempt hands the payload back with the route, so a
                    // retry re-enters routing with the body it never lost.
                    payload = returned;
                    (node, addr, epoch, peer_protocol, capacity_sample_ms)
                }
            };
            let scope = &call.scope;
            let request_id = call.request_id;
            let deliver_abort_to_handler = call.deliver_abort_to_handler;
            let cancel = &mut call.cancel;
            let body_guard = &mut call.body_guard;
            let CallPayload {
                name,
                url,
                method,
                body,
                headers,
            } = &payload;
            if dispatcher.observe(&node, epoch, capacity_sample_ms) {
                let cancel = if deliver_abort_to_handler {
                    None
                } else {
                    cancel.as_mut()
                };
                match dispatcher.wait(cancel).await {
                    RemoteRetryOutcome::Ready => {
                        dispatcher.invalidate(&app, scope).await;
                        continue;
                    }
                    RemoteRetryOutcome::Cancelled => {
                        break Err(anyhow::anyhow!("Durable Object call cancelled"));
                    }
                    RemoteRetryOutcome::Stop => {
                        break Err(anyhow::anyhow!(
                            "remote owner remained stale until the retry deadline"
                        ));
                    }
                }
            }
            let dispatch_started = Instant::now();
            let streamed_attempt = matches!(body, crate::engine_api::RequestBody::Stream(_));
            let remote_call = async {
                anyhow::ensure!(
                    peer_protocol == peer_auth::PROTOCOL_VERSION,
                    "peer {node} speaks incompatible protocol {peer_protocol}"
                );
                {
                    let control = peer_tunnel::TunnelControl {
                        scope: scope.clone(),
                        name: name.clone(),
                        request_id,
                        capacity_handoff: epoch == 0,
                    };
                    let attempt_body = match body {
                        crate::engine_api::RequestBody::Bytes(bytes) => {
                            crate::engine_api::RequestBody::Bytes(bytes.clone())
                        }
                        crate::engine_api::RequestBody::Stream(stream_id) => {
                            crate::engine_api::RequestBody::Stream(*stream_id)
                        }
                    };
                    let mut abort = RemoteFetchAbortGuard::new(
                        &app,
                        node.clone(),
                        addr.clone(),
                        scope.clone(),
                        request_id,
                    );
                    let response = peer_tunnel::fetch(
                        &peer_tunnel::Peer {
                            http: &app.peer_http,
                            auth: &app.peer_auth,
                            addr: &addr,
                            node: &node,
                        },
                        &control,
                        url.clone(),
                        method.clone(),
                        headers.clone(),
                        attempt_body,
                    )
                    .await
                    .map_err(|error| owner_unreachable(scope, &addr, error))?;
                    // Only after the tunnel has taken the stream: `disarm`
                    // releases the guard's registry claim, and a claim that
                    // drops to zero deletes the entry, so disarming before
                    // the take deletes the body out from under the attempt.
                    // An error above keeps the guard armed so an untaken
                    // stream is still cleaned up.
                    body_guard.disarm();
                    if response
                        .headers()
                        .get(STALE_ROUTE_HEADER)
                        .is_some_and(|value| value == STALE_ROUTE_VALUE)
                    {
                        abort.disarm();
                        return Err(owner_unreachable(
                            scope,
                            &addr,
                            anyhow::Error::new(StalePeerRoute {
                                scope: scope.clone(),
                            }),
                        ));
                    }
                    let status = response.status().as_u16();
                    if status == 101 {
                        // A tunneled upgrade: the inner 101 is the cell's
                        // own handshake for this client's key. Park the
                        // inner upgrade; once this node's client socket
                        // upgrades, the two splice and the hop copies
                        // bytes. The handshake fields are dropped because
                        // the client-facing 101 regenerates them.
                        let response_headers = response
                            .headers()
                            .iter()
                            .filter(|(header, _)| {
                                let header = header.as_str();
                                header != "upgrade"
                                    && header != "connection"
                                    && header != "sec-websocket-accept"
                            })
                            .filter_map(|(header, value)| {
                                value
                                    .to_str()
                                    .ok()
                                    .map(|value| (header.to_string(), value.to_string()))
                            })
                            .collect();
                        abort.disarm();
                        let mut response = response;
                        let parked = peer_tunnel::park_upgrade(hyper::upgrade::on(&mut response));
                        return Ok(HttpResponse {
                            status,
                            headers: response_headers,
                            body: Vec::new(),
                            websocket: Some(HttpResponseWebSocket::Cell(
                                crate::engine_api::WsTarget {
                                    id: 0,
                                    scope: scope.clone(),
                                    tunnel: Some(parked),
                                },
                            )),
                            stream: None,
                            write_position: None,
                            observed_position: None,
                        });
                    }
                    if response.headers().contains_key(HANDLER_FAILURE_HEADER) {
                        // The owner ran the handler, so the attempt is
                        // complete: the owner needs no abort, and the error
                        // must not reach the redispatch policy below.
                        abort.disarm();
                        let body = Limited::new(response.into_body(), MAX_PEER_CONTROL_BODY_BYTES)
                            .collect()
                            .await
                            .map(|body| body.to_bytes())
                            .unwrap_or_default();
                        let body = String::from_utf8_lossy(&body);
                        let message = body.strip_prefix(HANDLER_FAILURE_PREFIX).unwrap_or(&body);
                        return Err(anyhow::Error::new(RemoteHandlerFailure(
                            message.to_string(),
                        )));
                    }
                    let response_headers = response
                        .headers()
                        .iter()
                        .filter_map(|(header, value)| {
                            value
                                .to_str()
                                .ok()
                                .map(|value| (header.to_string(), value.to_string()))
                        })
                        .collect();
                    let stream = forwarder_response_stream(
                        peer_tunnel::response_stream(response.into_body()),
                        abort,
                    );
                    Ok(HttpResponse {
                        status,
                        headers: response_headers,
                        body: Vec::new(),
                        websocket: None,
                        stream: Some(stream),
                        // A proxied remote response wrote on the owner, not
                        // here.
                        write_position: None,
                        observed_position: None,
                    })
                }
            };
            let remote = match cancel.as_mut() {
                Some(cancel) => crate::asyncrt::select_biased! {
                    "a cancellation that ties the remote response prevents returning abandoned work";
                    _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                    remote = remote_call => remote,
                },
                None => remote_call.await,
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.dispatch_us = timing
                    .dispatch_us
                    .saturating_add(dispatch_started.elapsed().as_micros() as u64);
            }
            match remote {
                Ok(response) => {
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, scope, request_id, "ok", "remote", &node);
                    }
                    break Ok(response);
                }
                Err(error) if error.downcast_ref::<RemoteHandlerFailure>().is_some() => {
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, scope, request_id, "error", "remote", &node);
                    }
                    break Err(error);
                }
                Err(error) => {
                    // A streamed upload is not replayable after the HTTP
                    // client starts pulling it. Retrying would send only the
                    // unread suffix as a new request, so fail this attempt
                    // instead of manufacturing a truncated body.
                    if streamed_attempt {
                        break Err(error);
                    }
                    // Epoch zero is a candidate, not an owner. A peer refusal
                    // proves this attempt did not execute and must not consume
                    // the ordinary one-owner stale-route budget;
                    // the core excludes that exact load sample before the
                    // next deterministic placement decision.
                    let capacity_refused = epoch == 0
                        && error
                            .chain()
                            .any(|cause| cause.downcast_ref::<StalePeerRoute>().is_some());
                    if capacity_refused {
                        dispatcher.invalidate(&app, scope).await;
                        let cancel = if deliver_abort_to_handler {
                            None
                        } else {
                            cancel.as_mut()
                        };
                        match dispatcher.wait_capacity(cancel).await {
                            RemoteRetryOutcome::Ready => continue,
                            RemoteRetryOutcome::Cancelled => {
                                break Err(anyhow::anyhow!("Durable Object call cancelled"));
                            }
                            RemoteRetryOutcome::Stop => {
                                // Every attempt was explicitly refused before
                                // execution. Preserve typed admission failure;
                                // an ambiguous transport result never enters
                                // this branch and is never replayed here.
                                break Err(anyhow::Error::new(crate::engine_api::CellOverloaded));
                            }
                        }
                    }
                    let cancel = if deliver_abort_to_handler {
                        None
                    } else {
                        cancel.as_mut()
                    };
                    match dispatcher
                        .retry(&app, scope, classify_remote_attempt(&error), cancel, None)
                        .await
                    {
                        RemoteRetryOutcome::Ready => continue,
                        RemoteRetryOutcome::Cancelled => {
                            break Err(anyhow::anyhow!("Durable Object call cancelled"));
                        }
                        RemoteRetryOutcome::Stop => {}
                    }
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, scope, request_id, "error", "remote", &node);
                    }
                    break Err(error);
                }
            }
        }
    };
    let result = operation.await;
    let _ = reply.send(result);
}

async fn dispatch_rpc_call(app: AppHandle, call: RpcCallReq) {
    let RpcCallReq {
        parent_cancellation,
        signal,
        deadline_ms,
        authority,
        request_id,
        cancel,
        scope,
        name,
        method,
        args,
        reply,
    } = call;
    let event_call = authority.is_some();
    let deadline_ms = deadline_ms.map(|deadline| {
        deadline.min(crate::asyncrt::mono_ms().saturating_add(app.operation_deadline_ms))
    });
    let request_id = Some(request_id.unwrap_or_else(crate::engine_api::next_request_id));
    let budget = event_call.then(|| {
        event_wait::Budget::new(crate::host_rpc::event_budget_ms(
            &method,
            &args,
            app.operation_deadline_ms,
        ))
    });
    // Poll the routed operation through one indirection instead of embedding
    // its entire state in every enclosing deadline/cancellation wrapper.
    let operation = Box::pin(async {
        crate::runtime::rpc_authority::validate_target(authority.as_ref(), &scope, &method)
            .map_err(|failure| {
                if event_call {
                    event_failure::annotate(
                        failure,
                        crate::application::event_stream::ErrorCode::Forbidden,
                    )
                } else {
                    failure
                }
            })?;
        anyhow::ensure!(
            cellar_logic::cell::valid_cell_scope(&scope),
            "cell scope is malformed or exceeds the fleet storage limit"
        );
        let mut dispatcher = match budget.as_ref() {
            Some(budget) => RemoteRouteRetry::until(budget.deadline_ms()),
            None => match deadline_ms {
                Some(deadline) => RemoteRouteRetry::until(deadline),
                None => RemoteRouteRetry::new(app.operation_deadline_ms),
            },
        };
        loop {
            let resolving = app.request(scope.clone());
            let routed = resolving.await;
            let Routed { request, route } = routed.map_err(|error| {
                let failure = anyhow::Error::new(RoutedRequestError(error));
                if event_call {
                    event_failure::annotate(failure, event_failure::route_code(error))
                } else {
                    failure
                }
            })?;
            let (node, addr, epoch, peer_protocol, capacity_sample_ms) = match route {
                Route::Local => {
                    let local_request_id = local_dispatch_request_id(request_id);
                    let local =
                        app.local_request(request, scope.clone(), local_request_id, "local");
                    if let Some(budget) = &budget {
                        budget.submitted();
                    }
                    let dispatch = async {
                        let completed = local
                            .run(async {
                                app.runtime
                                    .as_ref()
                                    .context("no cell runtime")?
                                    .rpc_with_authority(crate::runtime::RpcInvocation {
                                        parent_cancellation: parent_cancellation.clone(),
                                        signal: signal.clone(),
                                        deadline_ms,
                                        cell: scope,
                                        name,
                                        method,
                                        args,
                                        request_id: local_request_id,
                                        authority,
                                    })
                                    .await
                            })
                            .await;
                        Ok(completed.result.map_err(local_request_error)?.data)
                    };
                    return dispatch.await;
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                    capacity_sample_ms,
                } => (node, addr, epoch, peer_protocol, capacity_sample_ms),
            };
            if dispatcher.observe(&node, epoch, capacity_sample_ms) {
                match dispatcher.wait(None).await {
                    RemoteRetryOutcome::Ready => {
                        dispatcher.invalidate(&app, &scope).await;
                        continue;
                    }
                    RemoteRetryOutcome::Stop => {
                        return Err(if event_call {
                            event_failure::annotate(
                                anyhow::anyhow!(
                                    "remote RPC owner remained stale until the retry deadline"
                                ),
                                crate::application::event_stream::ErrorCode::OwnershipLost,
                            )
                        } else {
                            anyhow::anyhow!(
                                "remote RPC owner remained stale until the retry deadline"
                            )
                        })
                    }
                    RemoteRetryOutcome::Cancelled => anyhow::bail!("Durable Object RPC cancelled"),
                }
            }
            if peer_protocol != peer_auth::PROTOCOL_VERSION {
                let failure =
                    anyhow::anyhow!("peer {node} speaks incompatible protocol {peer_protocol}");
                return Err(if event_call {
                    event_failure::annotate(
                        failure,
                        crate::application::event_stream::ErrorCode::UnsupportedProtocol,
                    )
                } else {
                    failure
                });
            }
            let expects_structured_response =
                matches!(args, crate::engine_api::RpcData::Structured(_));
            {
                let mut abort = RemoteFetchAbortGuard::new(
                    &app,
                    node.clone(),
                    addr.clone(),
                    scope.clone(),
                    request_id,
                );
                let peer = peer_tunnel::Peer {
                    http: &app.peer_http,
                    auth: &app.peer_auth,
                    addr: &addr,
                    node: &node,
                };
                let sending = peer_tunnel::rpc(
                    &peer,
                    &scope,
                    name.as_deref(),
                    &method,
                    &args,
                    peer_tunnel::RpcControl {
                        request_id,
                        authority: authority.as_ref(),
                        deadline_ms,
                    },
                );
                if let Some(budget) = &budget {
                    budget.submitted();
                }
                let response = sending.await;
                // Classify failures before response decoding. An explicit stale
                // owner ran nothing; a failed or truncated reply after execution
                // remains ambiguous and cannot justify another RPC invocation.
                let attempt = match &response {
                    Err(error) => Some(classify_remote_attempt(error)),
                    Ok(response)
                        if response
                            .headers()
                            .get(STALE_ROUTE_HEADER)
                            .is_some_and(|value| value == STALE_ROUTE_VALUE) =>
                    {
                        Some(cellar_logic::routing::Attempt::NotOwner)
                    }
                    Ok(_) => None,
                };
                if let Some(attempt) = attempt {
                    if let Some(budget) = &budget {
                        budget.refused(attempt);
                    }
                    if attempt != cellar_logic::routing::Attempt::Ambiguous {
                        // Nothing executed. An abort tombstone must not cancel
                        // the safe redispatch of this same request identity.
                        abort.disarm();
                    }
                    match dispatcher
                        .retry(&app, &scope, attempt, None, Some("rpc"))
                        .await
                    {
                        RemoteRetryOutcome::Ready => continue,
                        RemoteRetryOutcome::Cancelled => {
                            anyhow::bail!("Durable Object RPC cancelled")
                        }
                        RemoteRetryOutcome::Stop => {
                            match response {
                                Err(error) => {
                                    let failure = remote_rpc_failure(error);
                                    return Err(if event_call {
                                        event_failure::transport(failure, attempt)
                                    } else {
                                        failure
                                    });
                                }
                                Ok(_) => {
                                    return Err(if event_call {
                                        event_failure::annotate(anyhow::anyhow!("remote RPC owner was stale"),crate::application::event_stream::ErrorCode::OwnershipLost)
                                    } else {
                                        anyhow::anyhow!("remote RPC owner was stale")
                                    })
                                }
                            }
                        }
                    }
                }
                let response = response?;
                if let Some(failure) =
                    rpc_deadline::remote_failure(response.status(), response.headers())
                {
                    return Err(failure.into());
                }
                // An RPC response head confirms that execution and its output
                // gate finished. Caller cancellation must not kill later
                // waitUntil work while the finite result body is collected.
                abort.disarm();
                if response.status() == StatusCode::SERVICE_UNAVAILABLE
                    && response
                        .headers()
                        .get("x-cellar-overload")
                        .is_some_and(|value| value == "cell")
                {
                    return Err(anyhow::Error::new(crate::engine_api::CellOverloaded));
                }
                if !event_call
                    && !response.status().is_success()
                    && response
                        .headers()
                        .get(HANDLER_FAILURE_HEADER)
                        .is_some_and(|v| v == "1")
                {
                    // Only a gated handler failure carries this marker. Gate
                    // rejection remains opaque below, so an SDK control must
                    // never interpret an unproven mutation as its completion.
                    let collecting =
                        Limited::new(response.into_body(), MAX_PEER_CONTROL_BODY_BYTES).collect();
                    let collected = collecting.await;
                    let body = collected
                        .map_err(|error| anyhow::anyhow!("remote RPC error body: {error}"))?
                        .to_bytes();
                    let body = String::from_utf8_lossy(&body);
                    return Err(anyhow::Error::new(RemoteHandlerFailure(
                        body.strip_prefix(HANDLER_FAILURE_PREFIX)
                            .unwrap_or(&body)
                            .to_owned(),
                    )));
                }
                if !response.status().is_success() {
                    let failure = anyhow::anyhow!("remote RPC failed with {}", response.status());
                    return Err(if event_call {
                        event_failure::annotate(
                            failure,
                            event_failure::response_code(response.headers(), response.status()),
                        )
                    } else {
                        failure
                    });
                }
                let body = event_reply::receive(
                    response.into_body(),
                    event_call,
                    None,
                    event_call.then_some(dispatcher.deadline_ms),
                )
                .await?;
                return Ok(if expects_structured_response {
                    crate::engine_api::RpcData::Structured(body)
                } else {
                    crate::engine_api::RpcData::Json(String::from_utf8_lossy(&body).into_owned())
                });
            }
        }
    });
    let result = crate::runtime::cancellation::receive(
        parent_cancellation.as_ref(),
        signal.as_ref(),
        rpc_deadline::receive(
            deadline_ms,
            event_wait::receive(operation, cancel, budget.as_ref()),
        ),
    )
    .await;
    let result = if event_call {
        result.map_err(event_failure::dispatched)
    } else {
        result
    };
    let _ = reply.send(result);
}

async fn request_payload<B>(
    request: Request<B>,
    trust_forwarded_headers: bool,
    max_body_bytes: usize,
) -> Result<(String, String, Vec<u8>, Vec<(String, String)>), Box<HttpReply>>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let body = collect_limited_body(body, max_body_bytes)
        .await
        .map_err(|error| Box::new(body_read_error("request", error)))?;
    let headers = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    Ok((
        request_url(&parts, trust_forwarded_headers),
        parts.method.to_string(),
        body.to_vec(),
        headers,
    ))
}

/// A body at or above this many bytes reaches the Worker as a stream.
///
/// A small body crosses as bytes. The host makes one copy, and the handler
/// reads that copy with no asynchronous operation. A large body crosses as
/// a stream, because one copy of a large body costs more than the
/// operations that read it in parts. The host also streams a body that
/// declares no length, because the host cannot know the size of that body.
const INGRESS_STREAM_THRESHOLD: u64 = 1 << 20;
/// Peer fetches and RPC calls can carry the same application data as public
/// ingress. The frame needs room for its bounded metadata in addition to the
/// application body.
const MAX_PEER_FORWARD_BODY_BYTES: usize = DEFAULT_MAX_REQUEST_BODY_BYTES + (1 << 20);
/// Operator requests carry JSON commands, migration SQL, or workflow event
/// arguments. The 64 MiB ceiling leaves headroom above the 32 MiB SQL result
/// contract without giving an operator route the 1 GiB forwarding allowance.
const MAX_PEER_PROTOCOL_BODY_BYTES: usize = 64 << 20;
/// Control messages contain identifiers and counters, not application data.
const MAX_PEER_CONTROL_BODY_BYTES: usize = 64 << 10;
const INTERNAL_LOG_APPEND_PATH: &str = "/peer/log/append";
/// A streamed body crosses the isolate boundary through a string-only error
/// channel. Keep one private marker in that string so the HTTP entry point can
/// restore status 413; a generic stream error would turn the refusal into 500.
const BODY_LIMIT_STREAM_ERROR: &str = "cellar request body limit exceeded";

enum BodyReadError {
    TooLarge,
    Invalid(String),
}

async fn collect_limited_body<B>(body: B, limit: usize) -> Result<Bytes, BodyReadError>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if body.size_hint().lower() > limit as u64 {
        return Err(BodyReadError::TooLarge);
    }
    Limited::new(body, limit)
        .collect()
        .await
        .map(|body| body.to_bytes())
        .map_err(|error| {
            if error
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some()
            {
                BodyReadError::TooLarge
            } else {
                BodyReadError::Invalid(error.to_string())
            }
        })
}

fn body_read_error(context: &str, error: BodyReadError) -> HttpReply {
    match error {
        BodyReadError::TooLarge => {
            response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        BodyReadError::Invalid(error) => response(
            StatusCode::BAD_REQUEST,
            format!("invalid {context} body: {error}"),
        ),
    }
}

fn request_body_limit_error(error: &anyhow::Error) -> bool {
    // The Worker runtime adds context around a host-stream failure, so inspect
    // the chain instead of requiring the marker to remain the top-level error.
    error
        .chain()
        .any(|cause| cause.to_string().contains(BODY_LIMIT_STREAM_ERROR))
}

fn cell_overload_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<crate::engine_api::CellOverloaded>().is_some()
            // A Queue producer call crosses a service error boundary before the
            // public Worker response fails. The opaque text marker prevents an
            // application error from accidentally receiving the 503 contract.
            || cause
                .to_string()
                .contains(crate::engine_api::CELL_OVERLOAD_ERROR_MARKER)
    })
}

fn cell_overload_response() -> HttpReply {
    let mut refused = response(
        StatusCode::SERVICE_UNAVAILABLE,
        "{\"error\":\"cell admission refused\"}",
    );
    refused.headers_mut().insert(
        hyper::header::RETRY_AFTER,
        hyper::header::HeaderValue::from_static("1"),
    );
    refused.headers_mut().insert(
        hyper::header::HeaderName::from_static("x-cellar-overload"),
        hyper::header::HeaderValue::from_static("cell"),
    );
    refused
}

fn application_overload_response() -> HttpReply {
    let mut refused = response(
        StatusCode::SERVICE_UNAVAILABLE,
        "{\"error\":\"application admission refused\",\"code\":\"Overloaded\",\"outcome\":\"unknown\"}",
    );
    refused.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    refused.headers_mut().insert(
        hyper::header::HeaderName::from_static("x-cellar-overload"),
        hyper::header::HeaderValue::from_static("application"),
    );
    refused
}

// The peer parser returns before a streamed body can cross its limit, so this
// error arrives from the Worker and must retain the ingress 413 contract.
fn internal_do_worker_error(error: anyhow::Error) -> HttpReply {
    if request_body_limit_error(&error) {
        return response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    }
    if cell_overload_error(&error) {
        return cell_overload_response();
    }
    let mut failed = response(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("{HANDLER_FAILURE_PREFIX}{error:#}"),
    );
    failed.headers_mut().insert(
        hyper::header::HeaderName::from_static(HANDLER_FAILURE_HEADER),
        hyper::header::HeaderValue::from_static("1"),
    );
    failed
}

/// Divide an ingress request into its metadata and a body that the Worker
/// can read. The function streams the body if one copy of the body costs
/// more than the operations that read it in parts.
async fn ingress_payload<B>(
    request: Request<B>,
    trust_forwarded_headers: bool,
    max_body_bytes: usize,
) -> Result<
    (
        String,
        String,
        crate::engine_api::RequestBody,
        Vec<(String, String)>,
    ),
    Box<HttpReply>,
>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let declared = request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|length| length > max_body_bytes as u64) {
        return Err(Box::new(response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body too large",
        )));
    }
    let (url, method, body, headers, held) =
        ingress_payload_parts(request, trust_forwarded_headers, max_body_bytes).map_err(
            |error| {
                Box::new(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("request body stream: {error}"),
                ))
            },
        )?;
    // The host collects a small body here. A read failure at this point
    // can still return status 400. A streamed body has no such point,
    // because that body fails while the handler reads it. The failure
    // surfaces there instead.
    let body = match held {
        None => body,
        Some(held) => match collect_limited_body(held, max_body_bytes).await {
            Ok(collected) => crate::engine_api::RequestBody::Bytes(collected),
            Err(error) => return Err(Box::new(body_read_error("request", error))),
        },
    };
    Ok((url, method, body, headers))
}

type IngressPayloadParts<B> = (
    String,
    String,
    crate::engine_api::RequestBody,
    Vec<(String, String)>,
    Option<B>,
);

fn ingress_payload_parts<B>(
    request: Request<B>,
    trust_forwarded_headers: bool,
    max_body_bytes: usize,
) -> Result<IngressPayloadParts<B>, String>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    let url = request_url(&parts, trust_forwarded_headers);
    let method = parts.method.to_string();
    let declared = parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    // A method that carries no body does not stream. There is nothing to
    // pull, and a stream costs the handler one operation to learn this.
    let bodyless =
        matches!(parts.method, hyper::Method::GET | hyper::Method::HEAD) || declared == Some(0);
    if bodyless || declared.is_some_and(|length| length < INGRESS_STREAM_THRESHOLD) {
        return Ok((
            url,
            method,
            crate::engine_api::RequestBody::Bytes(Bytes::new()),
            headers,
            Some(body),
        ));
    }
    let chunks = Limited::new(body, max_body_bytes)
        .into_data_stream()
        .map(|chunk| {
            chunk.map(|bytes| bytes.to_vec()).map_err(|error| {
                if error
                    .downcast_ref::<http_body_util::LengthLimitError>()
                    .is_some()
                {
                    BODY_LIMIT_STREAM_ERROR.to_string()
                } else {
                    error.to_string()
                }
            })
        });
    let stream_id = crate::http_streams::register_body_stream(Box::pin(chunks))?;
    Ok((
        url,
        method,
        crate::engine_api::RequestBody::Stream(stream_id),
        headers,
        None,
    ))
}

/// The refusal every path arm gives a cell scope that fails the charset gate.
///
/// A scope taken from a URL segment reaches `db_path`, which joins it under the
/// data directory, and the replication client, which builds a bucket key from
/// it, so a scope carrying its own path segments walks out of both. The gate
/// itself is reified in `cellar_logic::cell`, next to the peer-identity gate it
/// mirrors.
fn malformed_scope() -> HttpReply {
    response(
        StatusCode::BAD_REQUEST,
        "{\"error\":\"malformed_cell_scope\"}",
    )
}

/// Can cellar put this host in `request.url`?
///
/// `cellar_logic::http::authority_decision` returns the complete sans-IO
/// decision. A Punycode label is the one part that decision cannot settle,
/// because deciding it is the whole of IDNA: `xn--a` passes every other rule
/// and still makes `new URL(request.url)` throw inside a Worker.
///
/// So cellar runs a parser only when the decision requires one. The parser is
/// the same `url::Url::parse` that backs `URL` in the isolate, therefore this
/// check agrees with the Worker by construction rather than by a rule cellar
/// keeps in step by hand.
fn well_formed_host(host: &str) -> Option<&str> {
    match cellar_logic::http::authority_decision(host) {
        cellar_logic::http::AuthorityDecision::Reject => None,
        cellar_logic::http::AuthorityDecision::Use(host) => Some(host),
        cellar_logic::http::AuthorityDecision::NeedsUrlParser(host) => {
            url::Url::parse(&format!("http://{host}/"))
                .is_ok()
                .then_some(host)
        }
    }
}

/// `request.url` controls application routing and absolute links, so cellar
/// does not let an untrusted forwarding header or request-target authority set
/// its scheme or host. The path and query always come from the request target.
/// The host comes from `Host`, and the scheme is `http` because cellar does not
/// terminate TLS.
///
/// An operator can set `--trust-forwarded-headers` when a trusted proxy
/// replaces both forwarded headers. The trusted read takes the last value
/// because a proxy can append its value after a client-supplied value.
///
/// The host and the scheme are validated whatever their source. `Host` is
/// client-controlled whenever no proxy sits in front, which is the default, so
/// trusting its source is not the same as trusting its shape: an unchecked
/// value moves the path into a fragment or a query. `well_formed_host` is the
/// gate. A value that fails it is dropped rather than repaired, and the next
/// source in the chain applies.
/// A malformed `X-Forwarded-Host` therefore falls through to `Host`, and only
/// an empty chain reaches the fallback host. `Host` can still carry a client
/// value in that case, because a proxy often forwards it rather than replacing
/// it, so the fall-through trades a known-safe host for a well-formed one. The
/// gate is what makes that trade safe: neither value can reshape the URL, and
/// no hostname in `request.url` is trustworthy anyway.
fn request_url(parts: &hyper::http::request::Parts, trust_forwarded_headers: bool) -> String {
    // A header can arrive as several lines as well as one comma-joined line,
    // and `HeaderMap::get` returns only the first line. Select the actual final
    // field before validating it. Skipping an unusable final field would expose
    // an earlier client value again, which is the defect this gate prevents.
    let last_value = |name: &str| {
        parts
            .headers
            .get_all(name)
            .into_iter()
            .next_back()
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next_back())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let first_value = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let forwarded = |name: &str| trust_forwarded_headers.then(|| last_value(name)).flatten();

    // The gate applies per source, not once at the end of the chain, so a
    // malformed forwarded value falls through to `Host` instead of discarding
    // the whole chain.
    let host = forwarded("x-forwarded-host")
        .and_then(well_formed_host)
        .or_else(|| first_value("host").and_then(well_formed_host))
        .unwrap_or("cellar.local");
    let canonical = |scheme: &str| cellar_logic::http::canonical_scheme(scheme);
    let scheme = forwarded("x-forwarded-proto")
        .and_then(canonical)
        .unwrap_or("http");
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", hyper::http::uri::PathAndQuery::as_str);
    format!("{scheme}://{host}{path_and_query}")
}

/// Send a fetch to a cell through the dispatcher a Durable Object call from
/// inside a Worker goes through.
///
/// Public ingress used to resolve the route itself and, on finding another
/// owner, answer with a 307 and a JSON description of where the cell lived --
/// with no Location header, so nothing could follow it. A fleet behind a load
/// balancer serves a cell only from the node that happens to own it, which is
/// to say it does not serve a fleet at all.
///
/// Going through `dispatch_do_call` also inherits the redispatch policy and
/// the cancellation channel, so a client that hangs up reaches the owner
/// rather than only the node it connected to. `/do/` and `/runtime/` share this:
/// they differ in what they authenticate, not in how they reach a cell.
async fn dispatch_cell_fetch(
    app: &AppHandle,
    cell: String,
    name: Option<String>,
    url: String,
    method: String,
    body: crate::engine_api::RequestBody,
    headers: Vec<(String, String)>,
) -> HttpReply {
    let preserve_representation_length = method.eq_ignore_ascii_case("HEAD");
    let (reply, receive) = oneshot::channel();
    let (cancel_tx, cancel) = oneshot::channel();
    let accepted = crate::host_channels::submit_do_call(crate::host_channels::DoCallReq {
        // Named, and named here: the abort fires only for a call that carries
        // both an id and a cancel signal, so leaving this None silently costs
        // the cancellation rather than failing.
        request_id: Some(crate::engine_api::next_request_id()),
        cancel: Some(cancel),
        deliver_abort_to_handler: false,
        scope: cell,
        name,
        url,
        method,
        body_guard: crate::http_streams::RequestBodyGuard::of(&body),
        body,
        headers,
        reply,
        // An ingress call has no caller in this process to be ordered against.
        order: None,
        // Cross-node trace propagation is not available here, so a direct-DO
        // ingress starts without a remote parent.
        parent: None,
    });
    if !accepted {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"error\":\"dispatcher unavailable\"}",
        );
    }
    let _hangup = HangUp(Some(cancel_tx));
    match receive.await {
        Ok(Ok(worker_response)) => {
            reply_without_upgrade(app, worker_response, preserve_representation_length).await
        }
        Ok(Err(error)) if request_body_limit_error(&error) => {
            response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        Ok(Err(error)) if cell_overload_error(&error) => cell_overload_response(),
        Ok(Err(error)) if crate::application::ApplicationOverloaded::matches(&error) => {
            application_overload_response()
        }
        Ok(Err(error)) => match error.downcast_ref::<RoutedRequestError>() {
            Some(error) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{{\"error\":\"{:?}\"}}", error.0),
            ),
            None => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("cell Worker failed: {error:#}"),
            ),
        },
        Err(_) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"error\":\"dispatcher dropped the call\"}",
        ),
    }
}

/// One forwarded cell fetch, after transport decode.
pub(crate) struct ForwardedFetch {
    pub(crate) name: Option<String>,
    pub(crate) url: String,
    pub(crate) method: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) request_id: Option<crate::engine_api::RequestId>,
    pub(crate) capacity_handoff: bool,
}

pub(crate) enum ForwardedFetchOutcome {
    /// A plain application response, headers truthful — no sidecars.
    Reply(HttpReply),
    /// The cell accepted a WebSocket, already registered with the core.
    /// The transport decides how the 101 reaches the caller.
    WebSocket {
        target: crate::engine_api::WsTarget,
        headers: Vec<(String, String)>,
    },
}

pub(crate) async fn dispatch_forwarded_fetch(
    app: AppHandle,
    scope: String,
    fetch: ForwardedFetch,
    request_body: crate::engine_api::RequestBody,
) -> ForwardedFetchOutcome {
    let ForwardedFetch {
        name,
        url,
        method,
        headers,
        request_id,
        capacity_handoff,
    } = fetch;
    let _request_body_guard = crate::http_streams::RequestBodyGuard::of(&request_body);
    let preserve_representation_length = method.eq_ignore_ascii_case("HEAD");
    let routed = if capacity_handoff {
        app.capacity_request(scope.clone()).await
    } else {
        app.request(scope.clone()).await
    };
    let result = match routed {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let local = app.local_request(request, scope.clone(), request_id, "forwarded");
            let Some(runtime) = &app.runtime else {
                return ForwardedFetchOutcome::Reply(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no cell runtime",
                ));
            };
            let abort_scope = scope.clone();
            // The forwarding node hangs up when its own client does, so this
            // connection going away is the cancellation signal reaching the
            // owner -- and it arrives as a drop, which is why it is a guard
            // rather than the channel `fetch_cell` takes. Without it a handler
            // keeps running on the owner for a client that left the node it
            // dialled.
            let mut abort = AbortPeerFetchOnHangUp {
                runtime: runtime.clone(),
                scope: abort_scope,
                request_id,
                drain_pins: app.drain_pins.clone(),
                handler_active: true,
            };
            let upgrade = requests_websocket_upgrade(&headers);
            let mut accepted = None;
            let completed = local
                .run(async {
                    let result = runtime
                        .fetch_cell(
                            scope,
                            name,
                            RuntimeFetch {
                                url,
                                method,
                                body: request_body,
                                headers,
                                request_id,
                                // A peer's call has no caller in this process, and the
                                // peer protocol does not carry trace context.
                                order: None,
                                parent: None,
                            },
                            None,
                        )
                        .await;
                    // The handler has answered in both arms; a hang-up during the
                    // gate wait below cancels the request's remaining work, not a
                    // handler that is still running.
                    abort.handler_answered();
                    let response = result?;
                    if let Some(HttpResponseWebSocket::Cell(target)) = &response.websocket {
                        accepted = Some(target.clone());
                        // The same object-level check as a local call, so a
                        // caller sees one answer whichever node owns the cell.
                        anyhow::ensure!(upgrade, "TypeError: {WEBSOCKET_WITHOUT_UPGRADE}");
                    }
                    Ok(response)
                })
                .await;
            let activity = completed.activity;
            // A failure after the cell returned a socket discards the response,
            // which is the socket's only owner.
            if let (Err(failure), Some(target)) = (&completed.result, accepted.as_ref()) {
                let reason = match failure {
                    LocalRequestFailure::Handler(error) => error.to_string(),
                    LocalRequestFailure::OutputGate { verdict, .. } => {
                        RoutedRequestError(*verdict).to_string()
                    }
                };
                websocket::reject_accepted_websocket(&app, target, &reason).await;
            }
            let result = match completed.result {
                Ok(answer) => Ok(answer),
                Err(LocalRequestFailure::Handler(error)) => Err(error),
                Err(LocalRequestFailure::OutputGate { verdict, handler }) => {
                    return ForwardedFetchOutcome::Reply(peer_gate_failure(
                        verdict,
                        handler.as_ref(),
                    ));
                }
            };
            match result {
                Ok(mut worker_response) => {
                    if let Some(HttpResponseWebSocket::Cell(target)) = &worker_response.websocket {
                        let (cell, hibernatable) = crate::ws_registry::ws_core_registration(target);
                        let kind = if hibernatable {
                            WebSocketKind::Hibernatable
                        } else {
                            WebSocketKind::Regular
                        };
                        if let Err(error) = app.websocket_opened(cell, target.id, kind).await {
                            let reason = format!("WebSocket core registration failed: {error:#}");
                            websocket::reject_accepted_websocket(&app, target, &reason).await;
                            return ForwardedFetchOutcome::Reply(peer_response(response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                reason,
                            )));
                        }
                    }
                    if let Some(HttpResponseWebSocket::Cell(target)) =
                        worker_response.websocket.take()
                    {
                        abort.disarm();
                        drop(activity);
                        return ForwardedFetchOutcome::WebSocket {
                            target,
                            headers: worker_response.headers,
                        };
                    }
                    let body_active = worker_response.stream.is_some();
                    activity.set_phase("response_body", true, body_active);
                    if let Some(stream) = worker_response.stream.take() {
                        worker_response.stream =
                            Some(local_response_stream(stream, activity, move || {
                                abort.disarm()
                            }));
                    } else {
                        abort.disarm();
                        drop(activity);
                    }
                    reply_without_upgrade(&app, worker_response, preserve_representation_length)
                        .await
                }
                Err(error) => internal_do_worker_error(error),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(RequestError::CapacityExhausted) => {
            let mut stale = response(StatusCode::CONFLICT, "capacity exhausted");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("route failed: {error:?}"),
        ),
    };
    ForwardedFetchOutcome::Reply(peer_response(result))
}

async fn internal_abort(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("abort", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some((encoded_scope, encoded_request)) = path
        .strip_prefix("/peer/abort/")
        .and_then(|rest| rest.rsplit_once('/'))
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort target"));
    };
    let scope = match percent_encoding::percent_decode_str(encoded_scope).decode_utf8() {
        Ok(scope) => scope.into_owned(),
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort scope")),
    };
    if !cellar_logic::cell::valid_cell_scope(&scope) {
        return peer_response(malformed_scope());
    }
    let Some(request_id) = crate::engine_api::parse_request_id(encoded_request) else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid request id"));
    };
    let result = match &app.runtime {
        Some(runtime) => {
            // The signed target is the exact node session that received the
            // forwarded fetch. A cancellation must not route the cell again:
            // routing can activate work solely to cancel a request that never
            // arrived, and the request id is already process-wide.
            let phase = app
                .drain_pins
                .cancel_request(request_id, "peer_abort_received");
            if phase.is_none() || phase == Some("handler") {
                runtime.abort_fetch(&scope, request_id);
            }
            response(StatusCode::NO_CONTENT, Bytes::new())
        }
        None => response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"),
    };
    peer_response(result)
}

pub(crate) async fn dispatch_forwarded_rpc(
    app: AppHandle,
    mut invocation: crate::runtime::RpcInvocation,
) -> HttpReply {
    let scope = invocation.cell.clone();
    let deadline_ms = invocation.deadline_ms;
    if crate::runtime::rpc_authority::validate_target(
        invocation.authority.as_ref(),
        &scope,
        &invocation.method,
    )
    .is_err()
    {
        return peer_response(response(
            StatusCode::FORBIDDEN,
            "framework RPC requires valid host authority",
        ));
    }
    let Some(runtime) = &app.runtime else {
        let mut refusal =
            peer_response(response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"));
        refusal.headers_mut().insert(
            hyper::header::HeaderName::from_static(event_failure::HEADER),
            hyper::header::HeaderValue::from_static("Unavailable"),
        );
        return refusal;
    };
    let request_id =
        local_dispatch_request_id(invocation.request_id).expect("RPC dispatch has an id");
    invocation.request_id = Some(request_id);
    let (cancel, _reply_wait) = runtime.begin_rpc_reply_wait(request_id);
    let dispatch = async {
        let result = match app.request(scope.clone()).await {
            Ok(Routed {
                request,
                route: Route::Local,
            }) => {
                let local_request_id = Some(request_id);
                let local = app.local_request(request, scope.clone(), local_request_id, "local");
                let Some(runtime) = &app.runtime else {
                    return peer_response(response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no cell runtime",
                    ));
                };
                let completed = local.run(runtime.rpc_with_authority(invocation)).await;
                match completed.result {
                    Ok(outcome) => Ok(outcome.data),
                    Err(LocalRequestFailure::Handler(error)) => {
                        return peer_response(internal_do_worker_error(error));
                    }
                    Err(LocalRequestFailure::OutputGate { verdict, handler }) => {
                        return peer_gate_failure(verdict, handler.as_ref());
                    }
                }
            }
            Ok(Routed {
                route: Route::Remote { .. },
                ..
            }) => {
                let mut stale = response(StatusCode::CONFLICT, "stale route");
                stale.headers_mut().insert(
                    hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                    hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
                );
                return peer_response(stale);
            }
            Err(error) => {
                let mut refusal = peer_response(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "RPC route refused before execution",
                ));
                refusal.headers_mut().insert(
                    hyper::header::HeaderName::from_static(event_failure::HEADER),
                    hyper::header::HeaderValue::from_static(event_failure::header(
                        event_failure::route_code(error),
                    )),
                );
                return refusal;
            }
        };
        match result {
            Ok(crate::engine_api::RpcData::Json(json)) => peer_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .body(
                        Full::new(Bytes::from(json))
                            .map_err(|never| match never {})
                            .boxed_unsync(),
                    )
                    .expect("RPC JSON response"),
            ),
            Ok(crate::engine_api::RpcData::Structured(bytes)) => peer_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .body(
                        Full::new(bytes)
                            .map_err(|never| match never {})
                            .boxed_unsync(),
                    )
                    .expect("RPC clone response"),
            ),
            Err(error) if cell_overload_error(&error) => peer_response(cell_overload_response()),
            Err(error) => peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("RPC failed: {error:#}"),
            )),
        }
    };
    // The cancellation endpoint must also release an actor output-gate wait
    // after the runtime already returned its handler's committed result.
    match rpc_deadline::receive(
        deadline_ms,
        crate::cell_dispatch::receive_rpc(async { Ok(dispatch.await) }, Some(cancel)),
    )
    .await
    {
        Ok(response) => response,
        Err(error)
            if error
                .downcast_ref::<crate::application::rpc::Failure>()
                .is_some_and(|failure| {
                    failure.code() == crate::application::rpc::Code::DeadlineExceeded
                }) =>
        {
            rpc_deadline::response()
        }
        Err(error) => peer_response(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )),
    }
}

#[path = "network/management.rs"]
mod management;
#[path = "network/peer_tunnel.rs"]
mod peer_tunnel;
#[path = "network/websocket.rs"]
mod websocket;
use websocket::{handle_websocket, outbound_websocket_task, reply_without_upgrade};

async fn handle_ingress<B>(
    request: Request<B>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> HttpReply
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    if matches!(*request.method(), hyper::Method::GET | hyper::Method::HEAD) {
        // One snapshot for the asset decision and the Worker it may fall
        // into, so a deployment adopted mid-request cannot serve the new
        // generation's index with the old generation's Worker.
        let generation = app.runtime.as_ref().map(|runtime| runtime.generation());
        if let Some(resolver) = generation.as_deref().and_then(Generation::ingress_assets) {
            let path = request.uri().path();
            if !resolver.should_run_worker_first(path) {
                let head = request.method() == hyper::Method::HEAD;
                match resolver
                    .ingress_response(path, request.uri().query(), head, request.headers())
                    .await
                {
                    Ok(Some(response)) => return asset_response(response),
                    Ok(None) if resolver.asset_only() => {
                        return response(StatusCode::NOT_FOUND, "Not found");
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("cellar asset response failed for {path}: {error:#}");
                        return response(
                            StatusCode::BAD_GATEWAY,
                            "Active deployment asset is unavailable",
                        );
                    }
                }
            }
        }
    }

    let (url, method, body, headers) = match ingress_payload(
        request,
        app.trust_forwarded_headers,
        app.max_request_body_bytes,
    )
    .await
    {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    let preserve_representation_length = method.eq_ignore_ascii_case("HEAD");
    match app
        .fetch_worker(url, method, body, headers, connection)
        .await
    {
        Ok(worker_response) => {
            reply_without_upgrade(&app, worker_response, preserve_representation_length).await
        }
        Err(error) if request_body_limit_error(&error) => {
            response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        Err(error) if cell_overload_error(&error) => cell_overload_response(),
        Err(error) if crate::application::ApplicationOverloaded::matches(&error) => {
            application_overload_response()
        }
        Err(error) => match error.downcast_ref::<crate::pool::AdmitError>() {
            // Saturation is not a failure of the request. Answering it now
            // lets the caller retry or shed; holding the connection until its
            // own deadline is what a node with no capacity used to do.
            Some(refused @ crate::pool::AdmitError::Refused(_)) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Worker refused: {refused}"),
            ),
            // A build failure is a fault, not saturation.
            Some(crate::pool::AdmitError::Build(_)) | None => response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            ),
        },
    }
}

async fn dispatch_asset_call(app: AppHandle, call: AssetCallReq) {
    let generation = app
        .runtime
        .as_ref()
        .map(|runtime| runtime.generation_by_id(call.generation));
    let response = match generation.as_deref().and_then(|g| g.assets(&call.script)) {
        Some(resolver) => {
            resolver
                .binding_response(&call.url, &call.method, &call.headers)
                .await
        }
        None => Err(anyhow::anyhow!(
            "no asset resolver for script {}",
            call.script
        )),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_service_call(app: AppHandle, call: SvcCallReq) {
    crate::host_service::dispatch_fetch(app.runtime.as_ref(), call).await;
}

async fn dispatch_service_rpc(app: AppHandle, call: SvcRpcReq) {
    crate::host_service::dispatch_rpc(app.runtime.as_ref(), call).await;
}

async fn dispatch_queue_batch(app: AppHandle, call: QueueDispatchReq) {
    let QueueDispatchReq {
        scope,
        generation,
        script,
        lease_id,
        leases,
        batch,
    } = call;
    let queue = batch.queue.clone();
    let result = match &app.runtime {
        Some(runtime) => runtime.queue_service(generation, &script, batch).await,
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            tracing::warn!(%scope, %queue, %script, %error, "Queue dispatch failed; its lease will expire");
            return;
        }
    };
    let outcome = match result.outcome {
        crate::engine_api::QueueOutcome::Ok => "ok",
        crate::engine_api::QueueOutcome::Exception => "exception",
    };
    let body = serde_json::json!({
        "leaseId": lease_id,
        "leases": leases,
        "outcome": outcome,
        "error": result.error,
        "ackAll": result.ack_all,
        "retryBatch": {
            "retry": result.retry_batch.retry,
            "delaySeconds": result.retry_batch.delay_seconds,
        },
        "explicitAcks": result.explicit_acks,
        "retryMessages": result.retry_messages.into_iter().map(|retry| serde_json::json!({
            "msgId": retry.msg_id,
            "delaySeconds": retry.delay_seconds,
        })).collect::<Vec<_>>(),
    });
    let body = crate::engine_api::RequestBody::Bytes(body.to_string().into_bytes().into());
    let (reply, receive) = oneshot::channel();
    dispatch_do_call(
        app,
        DoCallReq {
            request_id: None,
            cancel: None,
            deliver_abort_to_handler: false,
            scope: scope.clone(),
            name: None,
            url: "https://queue.cellar.internal/__qSettle".to_string(),
            method: "POST".to_string(),
            body_guard: crate::http_streams::RequestBodyGuard::of(&body),
            body,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            reply,
            order: None,
            parent: None,
        },
    )
    .await;
    match receive.await {
        Ok(Ok(response)) if (200..300).contains(&response.status) => {}
        Ok(Ok(response)) => tracing::warn!(
            %scope,
            %queue,
            status = response.status,
            "Queue settlement was refused; its lease will expire"
        ),
        Ok(Err(error)) => tracing::warn!(
            %scope,
            %queue,
            %error,
            "Queue settlement routing failed; its lease will expire"
        ),
        Err(error) => tracing::warn!(
            %scope,
            %queue,
            %error,
            "Queue settlement dispatcher dropped; its lease will expire"
        ),
    }
}

async fn internal_probe(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("probe", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::GET {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some(challenge) = parts
        .headers
        .get("x-cells-probe-challenge")
        .and_then(|value| value.to_str().ok())
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "missing probe challenge"));
    };
    match crate::peer_probe::respond(app.peer_auth.source(), &app.advertise, challenge) {
        Ok(probe) => match serde_json::to_vec(&probe) {
            Ok(body) => peer_response(response(StatusCode::OK, body)),
            Err(_) => peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "encode probe response",
            )),
        },
        Err(_) => peer_response(response(StatusCode::BAD_REQUEST, "invalid probe challenge")),
    }
}

/// Adopt one released cell for a terminating peer. A successful response
/// proves that this node acquired the next ownership epoch. The restored
/// runtime remains dormant until real traffic needs it. If another peer won
/// the race, the response identifies that current owner.
async fn internal_handoff(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    if app.is_draining() {
        return peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"ok\":false,\"draining\":true}",
        ));
    }
    let (parts, body) = request.into_parts();
    let body = match collect_limited_body(body, MAX_PEER_CONTROL_BODY_BYTES).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("handoff", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let request: HandoffRequest = match serde_json::from_slice::<HandoffRequest>(&body) {
        Ok(request) if !request.cell.is_empty() && request.released_epoch > 0 => request,
        Ok(_) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid handoff cell or epoch",
            ));
        }
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid handoff JSON: {error}"),
            ));
        }
    };
    let mut attempt = app
        .accept_handoff(request.cell.clone(), request.released_epoch)
        .await;
    // Concurrent traffic commonly leaves this successor with a cached route
    // to the donor. The signed release says that exact generation is stale.
    // Retire it and resolve the owner record again; otherwise the successor
    // redirects the donor back to itself forever and no cell is adopted.
    if let Ok(HandoffAttempt::CurrentOwner(owner)) = &attempt {
        if owner.epoch <= request.released_epoch {
            app.invalidate_remote(request.cell.clone(), owner.node.clone(), owner.epoch)
                .await;
            attempt = app
                .accept_handoff(request.cell.clone(), request.released_epoch)
                .await;
        }
    }
    let (status, result) = match attempt {
        Ok(HandoffAttempt::Accepted(owner)) if owner.epoch > request.released_epoch => (
            StatusCode::OK,
            HandoffResponse {
                node: owner.node,
                addr: owner.addr,
                epoch: owner.epoch,
                peer_protocol: owner.peer_protocol,
                published: true,
            },
        ),
        Ok(HandoffAttempt::CurrentOwner(owner)) => (
            StatusCode::CONFLICT,
            HandoffResponse {
                node: owner.node,
                addr: owner.addr,
                epoch: owner.epoch,
                peer_protocol: owner.peer_protocol,
                published: false,
            },
        ),
        Ok(HandoffAttempt::Accepted(_)) | Ok(HandoffAttempt::Refused) | Err(_) => {
            return peer_response(response(
                StatusCode::SERVICE_UNAVAILABLE,
                "handoff capacity unavailable",
            ));
        }
    };
    match serde_json::to_vec(&result) {
        Ok(body) => peer_response(response(status, body)),
        Err(_) => peer_response(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "encode handoff response",
        )),
    }
}

/// The log tier's follower endpoints (crate::node_log): append fsyncs
/// entries and answers the ack-all vote, seal persists the fence mark
/// before replying, tail hands recovery the held fragment. Fleet-HMAC
/// verified like every internal peer surface.
async fn internal_log(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let Some(follower) = app.follower.clone() else {
        return peer_response(response(StatusCode::NOT_FOUND, "no follower store"));
    };
    let entered = crate::asyncrt::mono_us();
    let (mut parts, body) = request.into_parts();
    let body_limit = if path == INTERNAL_LOG_APPEND_PATH {
        // An append includes the LTX bytes from an application transaction,
        // so it needs the same allowance as other application-data routes.
        MAX_PEER_FORWARD_BODY_BYTES
    } else {
        MAX_PEER_CONTROL_BODY_BYTES
    };
    let body = match collect_limited_body(body, body_limit).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("log", error)),
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    if matches!(path.as_str(), "/peer/log/stream" | "/peer/log/tail") {
        // Ordered appends and immutable recovery snapshots share the same
        // authenticated upgrade. The 101 carries response auth before the
        // caller takes the duplex; each owned task serves its selected protocol.
        if !body.is_empty()
            || parts
                .headers
                .get(hyper::header::UPGRADE)
                .is_none_or(|header| header != "cellar-log-stream")
        {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "log streams require an empty authenticated upgrade",
            ));
        }
        let Some(upgrade) = parts.extensions.remove::<hyper::upgrade::OnUpgrade>() else {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "stream requires an upgradable connection",
            ));
        };
        let store = follower.clone();
        let tail = path == "/peer/log/tail";
        let task = crate::asyncrt::try_spawn(async move {
            match upgrade.await {
                Ok(upgraded) => {
                    let io = TokioIo::new(upgraded);
                    let result = if tail {
                        crate::node_log::serve_tail_stream(io, store).await
                    } else {
                        crate::node_log::serve_log_stream(io, store).await
                    };
                    if let Err(error) = result {
                        tracing::debug!(%error, "log stream ended");
                    }
                }
                Err(error) => tracing::debug!(%error, "log stream upgrade failed"),
            }
        });
        match task {
            Ok(task) => task.detach(),
            Err(_) => {
                return peer_response(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "log stream execution domain closed",
                ))
            }
        }
        let mut reply = response(StatusCode::SWITCHING_PROTOCOLS, "");
        reply.headers_mut().insert(
            hyper::header::UPGRADE,
            hyper::header::HeaderValue::from_static("cellar-log-stream"),
        );
        reply.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("upgrade"),
        );
        return peer_response(reply);
    }
    let result: anyhow::Result<Vec<u8>> = match path.as_str() {
        // Append bodies are binary; these ordinary control replies stay JSON.
        // Recovery tails use the bounded upgrade above.
        INTERNAL_LOG_APPEND_PATH => match crate::node_log::decode_append(&body) {
            Ok(req) => {
                let collected = crate::asyncrt::mono_us();
                let resp = follower.append(req).await;
                tracing::info!(
                    event = "log_append_router",
                    collect_ms = collected.saturating_sub(entered) / 1000,
                    append_ms = (crate::asyncrt::mono_us() - collected) / 1000,
                    body_len = body.len(),
                    "append routed"
                );
                serde_json::to_vec(&resp).map_err(Into::into)
            }
            Err(error) => Err(error),
        },
        "/peer/log/seal" => match serde_json::from_slice::<crate::node_log::SealReq>(&body) {
            Ok(req) => match follower.seal(&req).await {
                Ok(resp) => serde_json::to_vec(&resp).map_err(Into::into),
                Err(error) => Err(error),
            },
            Err(error) => Err(error.into()),
        },
        _ => Err(anyhow::anyhow!("unknown log endpoint")),
    };
    match result {
        Ok(body) => peer_response(response(StatusCode::OK, body)),
        Err(error) => peer_response(response(StatusCode::BAD_REQUEST, format!("{error:#}"))),
    }
}

async fn handle_public<B>(
    request: Request<B>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> Result<HttpReply, Infallible>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let path = request.uri().path().to_string();
    let _public_admission = if path == "/.well-known/cellar/health" {
        None
    } else {
        let Some(admission) = app.admit_public() else {
            let mut refused = response(
                StatusCode::SERVICE_UNAVAILABLE,
                "{\"ok\":false,\"draining\":true}",
            );
            refused.headers_mut().insert(
                hyper::header::RETRY_AFTER,
                hyper::header::HeaderValue::from_static("1"),
            );
            refused.headers_mut().insert(
                hyper::header::CONNECTION,
                hyper::header::HeaderValue::from_static("close"),
            );
            return Ok(refused);
        };
        Some(admission)
    };
    let result = handle_admitted_public(request, app, connection).await;
    drop(_public_admission);
    result
}

async fn handle_admitted_public<B>(
    request: Request<B>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> Result<HttpReply, Infallible>
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let path = request.uri().path().to_string();
    let draining = app.is_draining();
    if path != "/.well-known/cellar/health"
        && app.runtime.is_some()
        && fastwebsockets::upgrade::is_upgrade_request(&request)
    {
        return Ok(handle_websocket(request, app).await);
    }
    let mut result = match path.as_str() {
        "/.well-known/cellar/health"
            if !app.is_draining() && app.fleet_ready() && app.healthy().await =>
        {
            response(StatusCode::OK, "{\"ok\":true}")
        }
        "/.well-known/cellar/health" => response(StatusCode::SERVICE_UNAVAILABLE, "{\"ok\":false}"),
        _ if app.runtime.is_some() => handle_ingress(request, app, connection).await,
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    if draining {
        result.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
    }
    Ok(result)
}

async fn handle_internal(
    request: Request<Incoming>,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
) -> Result<HttpReply, Infallible> {
    let path = request.uri().path().to_string();
    let draining = app.is_draining();
    let do_scope = path.strip_prefix("/do/");
    let cell_scope = path.strip_prefix("/cell/");
    let evict_scope = path.strip_prefix("/evict/");
    let result = match path.as_str() {
        "/event-metrics" if app.runtime.is_some() => event_metrics::handle(&app, request).await,
        "/peer/raft/metrics" => raft_metrics::handle(&app, request).await,
        "/peer/probe" => internal_probe(request, app).await,
        "/peer/management" => management::handle(request, app, shutdown).await,
        "/peer/handoff" => internal_handoff(request, app).await,
        _ if path.starts_with("/peer/log/") => internal_log(request, app, path.clone()).await,
        "/state" => response(StatusCode::OK, app.snapshot().await),
        "/reload" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/reload" => internal_reload(app).await,
        "/rebalance/pause" | "/rebalance/resume" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/rebalance/pause" | "/rebalance/resume" => {
            internal_rebalance_switch(path == "/rebalance/pause")
        }
        "/shutdown" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/shutdown" => {
            let preserve_ownership = request
                .uri()
                .query()
                .is_some_and(|query| query.split('&').any(|part| part == "handoff=preserve"));
            let mode = if preserve_ownership {
                ShutdownMode::Preserve
            } else {
                ShutdownMode::Handoff
            };
            let _ = shutdown.send(mode);
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ if path.starts_with("/peer/abort/") && app.runtime.is_some() => {
            internal_abort(request, app, path).await
        }
        "/peer/tunnel" if app.runtime.is_some() => {
            if peer_tunnel::is_tunnel_request(&request) {
                peer_tunnel::accept(request, app)
            } else {
                peer_response(response(
                    StatusCode::BAD_REQUEST,
                    "tunnel establishment must upgrade",
                ))
            }
        }
        // The operator CLIs' way in (`cellar sql`, queues, workflows): the same
        // forwarding dispatch `/do/` uses, with the fleet authentication
        // `/do/` does not have. A runtime class holds application data and
        // answers an operator protocol — arbitrary SQL for SQL — so it must
        // not be reachable from an unauthenticated route; `/do/` refuses a
        // reserved scope below and sends the caller here.
        //
        // It is one route rather than one per class on purpose. SQL's design
        // page records what writing the second by hand would cost: the
        // refusal on the *other* route has to be widened in the same change,
        // and gating only the new route "would have left the hole open".
        // With one entrance and one refusal, a class added to
        // `RESERVED_CLASSES` inherits both. (The pre-v5 `/__sql/` alias died
        // with the v0.4.0 stop-the-world upgrade; the CLI in this tree sends
        // `/runtime/`.)
        _ if path.starts_with("/event-inventory/") && app.runtime.is_some() => {
            event_inventory::handle(&app, request).await
        }
        _ if path.starts_with("/event-state/") && app.runtime.is_some() => {
            event_state::handle(&app, request).await
        }
        _ if path.starts_with("/outbox/") && app.runtime.is_some() => {
            source_operator::handle(&app, request).await
        }
        _ if path.starts_with("/streams/") && app.runtime.is_some() => {
            stream_operator::handle(&app, request).await
        }
        _ if path.starts_with("/catalogs/") && app.runtime.is_some() => {
            catalog_operator::handle(&app, request).await
        }
        _ if path.starts_with("/runtime/") && app.runtime.is_some() => {
            let scope = path
                .strip_prefix("/runtime/")
                .expect("matched the prefix")
                .to_string();
            if !cellar_logic::cell::valid_cell_scope(&scope) {
                return Ok(peer_response(malformed_scope()));
            }
            // The mirror of `/do/`'s refusal: that route serves every ordinary
            // Durable Object and no reserved one, this route serves the
            // reserved ones and nothing else. Without the check, a signed
            // request could drive a user's Durable Object through a route
            // whose only contract is an operator protocol.
            if !crate::deploy::is_reserved_scope(&scope) {
                return Ok(peer_response(response(
                    StatusCode::FORBIDDEN,
                    "{\"error\":\"only a runtime class is served on /runtime/; use /do/ for a Durable Object\"}",
                )));
            }
            let operator_query = request.uri().query().map(str::to_owned);
            let method = request.method().clone();
            let path_and_query = request
                .uri()
                .path_and_query()
                .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
            let request_headers = request.headers().clone();
            let body = match collect_limited_body(request.into_body(), MAX_PEER_PROTOCOL_BODY_BYTES)
                .await
            {
                Ok(body) => body,
                Err(error) => return Ok(peer_response(body_read_error("operator cell", error))),
            };
            if let Err(error) = app.peer_auth.verify(
                &method,
                &path_and_query,
                &request_headers,
                &body,
                app.peer_auth.source(),
            ) {
                return Ok(peer_response(response(error.status(), error.message())));
            }
            let mut name = None;
            let mut cron_history = false;
            for (key, value) in url::form_urlencoded::parse(
                operator_query.as_deref().unwrap_or_default().as_bytes(),
            ) {
                if key == "inspection"
                    && value == "cron_history_v1"
                    && !cron_history
                    && scope
                        .split_once(':')
                        .is_some_and(|(class, _)| class == cellar_logic::cron::RESERVED_CLASS)
                {
                    cron_history = true;
                } else if key == "name" && name.is_none() && !value.is_empty() {
                    name = Some(value.into_owned());
                } else {
                    return Ok(peer_response(response(
                        StatusCode::BAD_REQUEST,
                        "{\"error\":\"invalid operator query\"}",
                    )));
                }
            }
            if cron_history {
                if name.is_some() {
                    return Ok(peer_response(response(
                        StatusCode::BAD_REQUEST,
                        "{\"error\":\"cron inspection does not accept a cell name\"}",
                    )));
                }
                return Ok(peer_response(
                    management::cron_history(app, scope, body.to_vec()).await,
                ));
            }
            // A Queue needs its name before its constructor chooses a
            // consumer. The runtime also proves that this name hashes to the
            // requested scope before it exposes the identity to JavaScript.
            dispatch_cell_fetch(
                &app,
                scope,
                name,
                "http://cell/".to_string(),
                "POST".to_string(),
                crate::engine_api::RequestBody::Bytes(body),
                vec![("content-type".to_string(), "application/json".to_string())],
            )
            .await
        }
        _ if do_scope.is_some() && app.runtime.is_some() => {
            let runtime = app.runtime.as_ref().expect("checked runtime");
            let scope = do_scope.expect("checked prefix");
            let cell = match runtime.cell_scope(scope) {
                Ok(cell) => cell,
                Err(error) => {
                    return Ok(response(StatusCode::BAD_REQUEST, format!("{error:#}")));
                }
            };
            // This route has no authentication, so it must reach no reserved
            // class at all. A reserved cell's whole fetch surface is an
            // operator protocol — arbitrary SQL for SQL, create/terminate/
            // events for a workflow — and its scope is an HMAC over names that
            // sit in the project's config rather than a secret.
            //
            // One question, not one per class. Two hand-written refusals stood
            // here before, and a third reserved class would have needed a third
            // that nothing forces anyone to write. `is_reserved_scope` closes
            // the door for every class in `RESERVED_CLASSES`; the hint below
            // is the only part that is per-class, and a class without one still
            // gets refused.
            if let Some(class) = crate::deploy::reserved_class_of(&cell) {
                let hint = crate::deploy::operator_hint(class);
                return Ok(response(
                    StatusCode::FORBIDDEN,
                    format!(
                        "{{\"error\":\"{class} is a runtime class and is not reachable over /do/; {hint}\"}}"
                    ),
                ));
            }
            let (url, method, body, headers) = match ingress_payload(
                request,
                app.trust_forwarded_headers,
                app.max_request_body_bytes,
            )
            .await
            {
                Ok(payload) => payload,
                Err(response) => return Ok(*response),
            };
            dispatch_cell_fetch(&app, cell, None, url, method, body, headers).await
        }
        _ if cell_scope.is_some() => {
            let cell = cell_scope.expect("checked prefix");
            if !cellar_logic::cell::valid_cell_scope(cell) {
                malformed_scope()
            } else {
                let cell = cell.to_string();
                match app.request(cell.clone()).await {
                Ok(Routed {
                    request,
                    route: Route::Local,
                }) => {
                    let _activity = app.activity(request, cell.clone());
                    response(
                        StatusCode::OK,
                        format!("{{\"route\":\"local\",\"cell\":{cell:?}}}"),
                    )
                }
                Ok(Routed {
                    route:
                        Route::Remote {
                            node,
                            addr,
                            epoch,
                            peer_protocol,
                            ..
                        },
                    ..
                }) => response(
                    StatusCode::TEMPORARY_REDIRECT,
                    format!(
                        "{{\"route\":\"remote\",\"node\":{node:?},\"addr\":{addr:?},\"epoch\":{epoch},\"peer_protocol\":{peer_protocol}}}"
                    ),
                ),
                    Err(error) => response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("{{\"error\":\"{error:?}\"}}"),
                    ),
                }
            }
        }
        _ if evict_scope.is_some() => {
            let cell = evict_scope.expect("checked prefix");
            if !cellar_logic::cell::valid_cell_scope(cell) {
                malformed_scope()
            } else {
                eviction_response(app.evict(cell.to_string()).await)
            }
        }
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    // Close the connection behind any response sent while draining, so
    // keep-alive clients reconnect to a healthy node and the drain loop
    // can finish. A request that raced the drain flag converges on its
    // next request, which hits the gate above.
    let mut result = result;
    // A 101 is exempt: `Connection: close` on it breaks the upgrade, and a
    // tunnel establishment during a drain is exactly how a draining owner
    // keeps serving the calls its handoff has not yet moved. The upgraded
    // connection leaves keep-alive rotation on its own when the call ends.
    if draining && result.status() != StatusCode::SWITCHING_PROTOCOLS {
        result.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
    }
    Ok(result)
}

fn eviction_response(result: Result<EvictSuccess, EvictError>) -> HttpReply {
    let Err(error) = result else {
        return response(StatusCode::OK, r#"{"ok":true}"#);
    };
    let status = match error {
        EvictError::Refused(EvictRefusal::NodeUnavailable | EvictRefusal::ConcurrencyLimit)
        | EvictError::Failed(EvictFailure::ActorUnavailable) => StatusCode::SERVICE_UNAVAILABLE,
        EvictError::Refused(_) | EvictError::Cancelled(_) => StatusCode::CONFLICT,
        EvictError::Failed(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    response(
        status,
        serde_json::json!({
            "ok": false,
            "error": { "kind": error.kind(), "reason": error.reason() }
        })
        .to_string(),
    )
}

#[derive(Clone, Copy)]
enum HttpSurface {
    Public,
    Internal,
    Recovery,
}

impl HttpSurface {
    fn name(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Internal => "internal",
            Self::Recovery => "recovery",
        }
    }
}

struct ShutdownConnection {
    stream: tokio::net::TcpStream,
    surface: HttpSurface,
    received: oneshot::Sender<()>,
    #[cfg(all(test, cellar_internal_tests))]
    recovered_after_error: bool,
}

fn spawn_shutdown_acceptor(
    listener: tokio::net::TcpListener,
    surface: HttpSurface,
    connections: mpsc::Sender<ShutdownConnection>,
    #[cfg(all(test, cellar_internal_tests))] mut fail_next_for_test: bool,
) {
    // Shutdown selects finish whenever a timer, request, or actor message
    // wins. Keep each listener's accept and retry waits in one task so those
    // unrelated events cannot cancel a wait or reset its retry delay.
    crate::asyncrt::spawn(async move {
        let mut retry_delay = SHUTDOWN_ACCEPT_RETRY_INITIAL;
        let mut recovering = false;
        #[cfg(all(test, cellar_internal_tests))]
        let mut connect_after_failure_for_test = false;
        loop {
            #[cfg(all(test, cellar_internal_tests))]
            let inject_failure = std::mem::take(&mut fail_next_for_test);
            #[cfg(all(test, cellar_internal_tests))]
            let accepted = if inject_failure {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "injected listener accept failure during shutdown",
                ))
            } else if std::mem::take(&mut connect_after_failure_for_test) {
                let retry_address = listener
                    .local_addr()
                    .expect("read the shutdown accept failure test listener address");
                // Connect before polling accept, so the injected transition
                // does not depend on an operating-system readiness-
                // notification race after the error.
                let connection = std::net::TcpStream::connect(retry_address)
                    .expect("connect after the injected shutdown accept failure");
                *SHUTDOWN_ACCEPT_FAILURE_CLIENT
                    .lock()
                    .expect("lock the shutdown accept failure test connection") = Some(connection);
                listener.accept().await
            } else {
                listener.accept().await
            };
            #[cfg(not(all(test, cellar_internal_tests)))]
            let accepted = listener.accept().await;
            match accepted {
                Ok((stream, _)) => {
                    if recovering {
                        tracing::info!(
                            event = "shutdown_accept_recovered",
                            surface = surface.name(),
                            "accepted {} connection after a shutdown accept error",
                            surface.name()
                        );
                    }
                    let (received, wait_until_received) = oneshot::channel();
                    let connection = ShutdownConnection {
                        stream,
                        surface,
                        received,
                        #[cfg(all(test, cellar_internal_tests))]
                        recovered_after_error: recovering,
                    };
                    if connections.send(connection).await.is_err() {
                        return;
                    }
                    let _ = wait_until_received.await;
                    retry_delay = SHUTDOWN_ACCEPT_RETRY_INITIAL;
                    recovering = false;
                }
                Err(error) => {
                    tracing::warn!(
                        event = "shutdown_accept_failed",
                        surface = surface.name(),
                        retry_ms = retry_delay.as_millis(),
                        %error,
                        "could not accept {} connection during shutdown; retrying",
                        surface.name()
                    );
                    #[cfg(all(test, cellar_internal_tests))]
                    if inject_failure {
                        shutdown_accept_failure_test_checkpoint(true, "listener failure observed");
                        connect_after_failure_for_test = true;
                    }
                    recovering = true;
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(SHUTDOWN_ACCEPT_RETRY_MAX);
                }
            }
        }
    })
    .detach();
}

fn serve_http_connection(
    stream: tokio::net::TcpStream,
    surface: HttpSurface,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
    mut connection_drain: watch::Receiver<bool>,
    connection_grace: std::time::Duration,
) -> ConnectionFuture {
    // Nagle + delayed-ACK stalls small request/response exchanges by tens
    // of milliseconds; the log tier's append acks measured it directly (a
    // ~70 ms floor on a sub-millisecond VPC path). Responses must leave
    // when written, on every surface.
    let _ = stream.set_nodelay(true);
    Box::pin(async move {
        let peer = stream.peer_addr().ok();
        // Serve on the runtime, not on this task. `main` drives its loop with
        // `block_on`, so serving there put every connection on one core.
        // Awaiting the spawned task keeps shutdown tracking unchanged.
        let served = crate::asyncrt::spawn(async move {
            let connection_requests = ConnectionWorkerRequests::default();
            let service_requests = connection_requests.clone();
            let service = service_fn(move |request| {
                let app = app.clone();
                let shutdown = shutdown.clone();
                let service_requests = service_requests.clone();
                async move {
                    match surface {
                        HttpSurface::Public => handle_public(request, app, service_requests).await,
                        HttpSurface::Internal => handle_internal(request, app, shutdown).await,
                        HttpSurface::Recovery => {
                            let path = request.uri().path().to_string();
                            Ok(match path.as_str() {
                                "/peer/log/seal" | "/peer/log/tail" => {
                                    internal_log(request, app, path).await
                                }
                                _ => response(
                                    StatusCode::SERVICE_UNAVAILABLE,
                                    "predecessor recovery in progress",
                                ),
                            })
                        }
                    }
                }
            });
            let connection = http1::Builder::new()
                // Reclaim a connection that never sends a complete request
                // head. The timeout also bounds an idle keep-alive waiting
                // for its next request.
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(std::time::Duration::from_secs(30))
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades();
            tokio::pin!(connection);
            let result = crate::asyncrt::select! {
                result = &mut connection => Some(result),
                _ = connection_drain.changed() => {
                    connection.as_mut().graceful_shutdown();
                    tokio::time::timeout(connection_grace, &mut connection)
                        .await
                        .ok()
                }
            };
            connection_requests.abort_all();
            match result {
                Some(Err(error)) => record_connection_error(&error, surface, peer),
                None => tracing::warn!(
                    event = "connection_drain_forced",
                    grace_ms = connection_grace.as_millis(),
                    "forced an HTTP connection closed after its graceful drain"
                ),
                Some(Ok(())) => {}
            }
        });
        let _ = served.await;
    })
}

/// Serve the surviving follower disks before recovering this node's own
/// predecessor. After a correlated restart, every predecessor can need a
/// fragment on another restarting node. Waiting to accept until recovery
/// finishes makes those intact disks look lost and seals out acked writes.
/// Only authenticated seal and tail requests run here: the actor has no
/// lease yet, so neither application work nor new log appends can run.
async fn recover_with_follower_listener(
    listener: &tokio::net::TcpListener,
    app: AppHandle,
    recovery: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let (drain_tx, drain_rx) = watch::channel(false);
    let (shutdown_tx, _shutdown_rx) = mpsc::unbounded_channel();
    let mut connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    tokio::pin!(recovery);
    let result = loop {
        crate::asyncrt::select! {
            result = &mut recovery => break result,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => connections.push(serve_http_connection(
                        stream,
                        HttpSurface::Recovery,
                        app.clone(),
                        shutdown_tx.clone(),
                        drain_rx.clone(),
                        CONNECTION_DRAIN_GRACE,
                    )),
                    Err(error) => break Err(error.into()),
                }
            }
            Some(()) = connections.next(), if !connections.is_empty() => {}
        }
    };
    // Retire the recovery-only keep-alives on either outcome, so no stale
    // service survives into normal operation or an unsuccessful boot.
    let _ = drain_tx.send(true);
    while connections.next().await.is_some() {}
    result
}
#[path = "../cli/daemon.rs"]
mod cli;
use cli::{action_from_process, print_help, Action};

#[cfg(all(test, cellar_internal_tests))]
fn shutdown_accept_failure_test_action() -> Action {
    let listen = std::env::var("CELLAR_DRAIN_ACCEPT_FAILURE_LISTEN")
        .map(crate::startup::Listen::Explicit)
        .unwrap_or(crate::startup::Listen::LoopbackEphemeral);
    Action::Run(cli::Settings {
        control_plane: false,
        bucket: None,
        load_deployment: false,
        endpoint: None,
        region: "test".to_string(),
        listen,
        internal_listen: crate::startup::Listen::LoopbackEphemeral,
        advertise: None,
        unsafe_public_advertise: false,
        trust_forwarded_headers: false,
        dev_store: None,
        data_dir: None,
    })
}

fn node_bucket(
    settings: &cli::Settings,
    managed: Option<&crate::control_plane::ManagedStorageConfig>,
    lease: bool,
) -> anyhow::Result<crate::bucket::Bucket> {
    if let Some(database) = &settings.dev_store {
        anyhow::ensure!(
            managed.is_none(),
            "a development node cannot use managed storage"
        );
        return crate::dev::open_local_bucket(database);
    }
    let bucket = settings
        .bucket
        .as_deref()
        .context("the node has no fleet bucket")?;
    if lease {
        fleet::lease_bucket_client_with_credentials(
            bucket,
            settings.endpoint.as_deref(),
            &settings.region,
            managed,
        )
    } else {
        fleet::bucket_client_with_credentials(
            bucket,
            settings.endpoint.as_deref(),
            &settings.region,
            managed,
        )
    }
}

/// The shutdown handoff parks the successor dormant, so the batch does not
/// start restore work there. The fleet drain token also admits one donor at a
/// time. Keep enough ownership operations in flight to overlap object-store
/// latency: a width of 8 evacuated only about 110 cells inside the default
/// 40-second process deadline.
const DEFAULT_MAX_CONCURRENT_RELEASES: usize = 128;
/// The actor refreshes the counts a node lease publishes once per sample.
const LOAD_SAMPLE_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);
/// How often a node with balancing off samples the fleet leases for the
/// bucket-format gate: the balancing default, one shared fleet sample.
const FORMAT_GATE_SAMPLE_MS: u64 = 5_000;
/// One lease renewal plus one load sample bound how stale a peer's count is,
/// so a batch every five seconds sees its predecessor before it plans.
const DEFAULT_REBALANCE_INTERVAL_MS: u64 = 5_000;
const DEFAULT_REBALANCE_BATCH_CELLS: usize = 32;
/// Preserved SQLite snapshots make a same-node wake a rename instead of a
/// remote restore, but must not grow with the lifetime population of a node.
/// The walk is O(cached cells), so keep it off the hot maintenance cadence.
const LOCAL_CACHE_PRUNE_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

type HostStop = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

type HostObserver = Box<dyn FnOnce(crate::node_lifecycle::NodeOutcome) + Send>;

fn run_host(
    backend: Backend,
    local_graph: Option<DeploymentGraph>,
    stop: Option<HostStop>,
    observer: Option<HostObserver>,
    raft: Option<native_raft::Startup>,
) -> anyhow::Result<()> {
    crate::env_vars::validate()?;
    // Parse the telemetry group once, before any command or runtime work.
    // Its specialized values share the strict scalar parsers in env_vars.
    let telemetry_config = crate::telemetry::Config::from_env()?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(workers) = crate::env_vars::positive::<usize>("CELLAR_TOKIO_THREADS")? {
        builder.worker_threads(workers);
    }
    builder.build()?.block_on(async_main(
        telemetry_config,
        backend,
        local_graph,
        stop,
        observer,
        raft,
    ))
}

async fn async_main(
    telemetry_config: Option<crate::telemetry::Config>,
    backend: Backend,
    mut local_graph: Option<DeploymentGraph>,
    stop: Option<HostStop>,
    observer: Option<HostObserver>,
    raft: Option<native_raft::Startup>,
) -> anyhow::Result<()> {
    let mut stop = stop;
    let return_to_caller = stop.is_some();
    let raft_enabled = raft.is_some();
    #[cfg(all(test, cellar_internal_tests))]
    let shutdown_accept_failure_test_active =
        std::env::var_os("CELLAR_DRAIN_ACCEPT_FAILURE_CHILD").is_some();
    #[cfg(all(test, cellar_internal_tests))]
    let action = if shutdown_accept_failure_test_active {
        shutdown_accept_failure_test_action()
    } else {
        if backend.is_native() {
            cli::action_from_process_with_application(true)?
        } else {
            action_from_process()?
        }
    };
    #[cfg(not(all(test, cellar_internal_tests)))]
    let action = if backend.is_native() {
        cli::action_from_process_with_application(true)?
    } else {
        action_from_process()?
    };
    if backend.is_native() {
        anyhow::ensure!(
            !matches!(&action, Action::Deploy(_) | Action::Dev(_)),
            "native application build/deploy/dev must use the compiled application's delivery workflow; the source bundler cannot publish Rust code"
        );
        anyhow::ensure!(
            std::env::var_os("CELLAR_TEST_SCRIPT_PATH").is_none(),
            "a native daemon cannot load CELLAR_TEST_SCRIPT_PATH; supply an embedded deployment graph or use the fleet pointer"
        );
    }
    let host_runtime = tokio::runtime::Handle::current();
    crate::asyncrt::set_host_handle(host_runtime.clone());
    crate::asyncrt::services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy::from_env()?)?;
    crate::asyncrt::services().configure_loader(crate::loader_policy::LoaderPolicy::from_env()?)?;
    crate::asyncrt::services().configure_http(
        crate::http_policy::HttpPolicy::from_env()?,
        Some(crate::http_policy::HttpClientFactory::new(
            reqwest::Client::builder,
        )),
    )?;
    // Docker and journald can stop consuming the process pipe during a log
    // burst. Logging must lose diagnostics under that backpressure rather
    // than block the Tokio workers that route requests and renew authority.
    // Parsed before the subscriber exists, because where the log goes depends
    // on which invocation this is: a node logs to stdout, and a CLI subcommand
    // must keep stdout for its answer.
    let log_filter = if matches!(
        &action,
        Action::Dev(arguments) if !arguments.iter().any(|argument| argument == "--logs")
    ) {
        // `cellar dev` is an application-facing supervisor. Its default view
        // must stay concise even when the parent inherited a broad RUST_LOG;
        // --logs is the explicit opt-in to that diagnostic stream.
        tracing_subscriber::EnvFilter::new("error")
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())
    };
    let (log_sink, log_is_terminal): (Box<dyn std::io::Write + Send>, bool) =
        if action.stdout_is_data() {
            (
                Box::new(std::io::stderr()),
                std::io::IsTerminal::is_terminal(&std::io::stderr()),
            )
        } else {
            (
                Box::new(std::io::stdout()),
                std::io::IsTerminal::is_terminal(&std::io::stdout()),
            )
        };
    let (log_writer, log_guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(8_192)
        .lossy(true)
        .finish(log_sink);
    *LOG_GUARD.lock().unwrap() = Some(log_guard);
    tracing_subscriber::fmt()
        .with_writer(log_writer)
        // The custom writer defeats fmt's own TTY detection, and journald
        // must not receive ANSI escapes.
        .with_ansi(log_is_terminal)
        .with_env_filter(log_filter)
        .init();
    // After the subscriber, because this reports whether the allocator agreed
    // to return freed pages on a timer. A node without that thread holds
    // retention until a thread allocates again, so the operator has to be
    // able to read whether that cleanup thread started.
    crate::memory::tune_allocator();
    let mut settings = match action {
        Action::Deploy(arguments) => return fleet::run_deploy(arguments).await,
        Action::Dev(arguments) => return crate::dev::run(arguments).await,
        Action::Cell(arguments) => return crate::cell_cli::run(arguments).await,
        Action::Sql(arguments) => return crate::sql_cli::run(arguments).await,
        Action::Kv(arguments) => return crate::kv_cli::run(arguments).await,
        Action::Queue(arguments) => return crate::queue_cli::run(arguments).await,
        Action::Event(arguments) => return crate::event_cli::run(arguments).await,
        Action::Outbox(arguments) => return crate::outbox_cli::run(arguments).await,
        Action::Stream(arguments) => return crate::stream_cli::run(arguments, false).await,
        Action::Subscription(arguments) => return crate::stream_cli::run(arguments, true).await,
        Action::Blob(arguments) => return crate::blob_cli::run(arguments).await,
        Action::Connect(arguments) => {
            return crate::control_plane::handle_connect_command(arguments).await
        }
        Action::Credentials(arguments) => {
            return crate::control_plane::handle_credentials_command(arguments).await
        }
        Action::Token(arguments) => {
            return crate::control_plane::handle_token_command(arguments).await
        }
        Action::Disconnect(arguments) => {
            return crate::control_plane::handle_disconnect_command(arguments).await
        }
        Action::Help => {
            if backend.is_native() {
                native::print_help()?;
            } else {
                print_help()?;
            }
            return Ok(());
        }
        Action::Version => {
            let profile = if cfg!(debug_assertions) {
                " (debug)"
            } else {
                ""
            };
            crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
                "cellar {}{profile}",
                env!("CARGO_PKG_VERSION")
            ))?;
            return Ok(());
        }
        Action::Diagnose {
            mut settings,
            peers,
            read_only,
            json,
        } => {
            let ingress = crate::startup::bind_ingress_listener(&settings.listen).await?;
            let internal =
                crate::startup::bind_internal_listener(&crate::startup::InternalListenerSettings {
                    listen: settings.internal_listen.clone(),
                    advertise: settings.advertise.clone(),
                    unsafe_public_advertise: settings.unsafe_public_advertise,
                })
                .await?;
            // The bind proves the address is free; diagnose never serves on it,
            // and an operator should not read this line as a running listener.
            // These reach the terminal through `fleet::diagnose`, so that one
            // `Output` renders every check and `--json` stays parseable.
            let preamble = vec![
                crate::fleet::Check::ok(
                    format!("listen {}", ingress.listen),
                    "bind check; diagnose does not serve",
                ),
                crate::fleet::Check::ok(
                    format!("internal listen {}", internal.listen),
                    "bind check; diagnose does not serve",
                ),
                crate::fleet::Check::ok(
                    format!("advertise {}", internal.advertise),
                    format!(
                        "{}; direct reachability is not inferred",
                        internal.advertise.scope()
                    ),
                ),
            ];
            let managed_storage = if settings.control_plane {
                match crate::control_plane::installation_storage().context(
                    "managed diagnostics require an existing enrollment; run `cellar --control-plane` first",
                )? {
                    crate::control_plane::InstallationStorageConfig::Managed(storage) => {
                        settings.bucket = Some(storage.bucket.clone());
                        settings.endpoint = Some(storage.endpoint.clone());
                        settings.region = storage.region.clone();
                        Some(storage)
                    }
                    crate::control_plane::InstallationStorageConfig::Byo(storage) => {
                        settings.bucket = Some(storage.bucket);
                        settings.endpoint = storage.endpoint;
                        settings.region = storage.region;
                        None
                    }
                }
            } else {
                None
            };
            let bucket = settings
                .bucket
                .ok_or_else(|| anyhow::anyhow!("cellar diagnose requires --bucket"))?;
            let client = fleet::bucket_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            return fleet::diagnose(
                &client,
                peers,
                settings.unsafe_public_advertise,
                read_only,
                json,
                preamble,
            )
            .await;
        }
        Action::Run(settings) => settings,
    };
    anyhow::ensure!(
        local_graph.is_none() || !settings.control_plane,
        "an embedded native graph cannot follow managed deployments; use NativeDaemon::fleet and register the deployed versions"
    );
    if backend.is_native() && local_graph.is_none() {
        anyhow::ensure!(
            settings.bucket.is_some() || settings.control_plane,
            "a native fleet daemon requires a fleet bucket; use NativeDaemon::local for an embedded deployment"
        );
        settings.load_deployment = true;
    }
    // Operators need the applied worker count to verify CELLAR_TOKIO_THREADS;
    // a count of process threads also includes unrelated, lazily started
    // thread pools, so it cannot stand in for this value.
    tracing::info!(
        event = "host_runtime",
        worker_count = host_runtime.metrics().num_workers(),
        "host runtime initialized"
    );
    crate::startup::raise_file_limit();
    // Queue generations sample this budget before the actor is constructed.
    // Capture the CLI override now; library hosts use explicit actor policy
    // or deterministic defaults without reading process environment.
    let operation_deadline_ms = crate::actor::capture_operation_deadline_from_environment()?;
    let max_resident = crate::env_vars::optional("CELLAR_MAX_RESIDENT_CELLS")?
        // cellar has no resident ceiling unless the operator configures one.
        // The clean-sheet prototype originally defaulted to eight, which
        // introduced eviction churn in otherwise unconstrained workloads and
        // made cancellation semantics depend on cold-reactivation latency.
        .unwrap_or(usize::MAX);
    let max_request_body_bytes = crate::env_vars::positive_or(
        "CELLAR_MAX_REQUEST_BODY_BYTES",
        DEFAULT_MAX_REQUEST_BODY_BYTES,
    )?;
    anyhow::ensure!(
        max_request_body_bytes <= DEFAULT_MAX_REQUEST_BODY_BYTES,
        "CELLAR_MAX_REQUEST_BODY_BYTES cannot exceed {DEFAULT_MAX_REQUEST_BODY_BYTES}"
    );
    let local_cache_max_bytes = local_cache_max_bytes_from_environment()?;
    let shutdown_policy = crate::node_lifecycle::ShutdownPolicy::from_env()?;
    let shutdown_report_json = crate::env_vars::flag("CELLAR_SHUTDOWN_REPORT_JSON", false)?;
    let fail_publish_once = std::env::var_os("CELLAR_TEST_FAIL_PUBLISH_ONCE").is_some();
    let ingress = crate::startup::bind_ingress_listener(&settings.listen).await?;
    let internal =
        crate::startup::bind_internal_listener(&crate::startup::InternalListenerSettings {
            listen: settings.internal_listen.clone(),
            advertise: settings.advertise.clone(),
            unsafe_public_advertise: settings.unsafe_public_advertise,
        })
        .await?;
    let advertise = internal.advertise.to_string();
    let listen = ingress.listen.to_string();
    let listener = ingress.listener;
    let internal_listener = internal.listener;
    let mut adapter_credential_version = None;
    let managed_storage = if settings.control_plane {
        if let Some(spec) = settings.bucket.take() {
            let (backend, name, prefix) = crate::bucket::split_spec(&spec);
            // The control plane issues and validates S3-compatible storage
            // only. Parse through the same seam as Bucket::open so scheme
            // case cannot change this decision or the eventual backend.
            anyhow::ensure!(
                backend == crate::bucket::StorageBackend::S3,
                "--control-plane storage is S3-compatible; a {}:// bucket runs without it",
                match backend {
                    crate::bucket::StorageBackend::Gcs => "gs",
                    crate::bucket::StorageBackend::Azure => "az",
                    crate::bucket::StorageBackend::S3 => "s3",
                    crate::bucket::StorageBackend::Local => "dev",
                }
            );
            // The control plane issues one bucket per fleet and its enrollment
            // API rejects a prefix. Install the parsed S3 name because the API
            // accepts a name, while every engine bucket path accepts a spec.
            anyhow::ensure!(
                prefix.is_empty(),
                "--control-plane does not accept a --bucket prefix"
            );
            settings.bucket = Some(name.to_string());
        }
        let requested_byo =
            settings
                .bucket
                .as_ref()
                .map(|bucket| crate::control_plane::ByoStorageConfig {
                    bucket: bucket.clone(),
                    endpoint: settings.endpoint.clone(),
                    region: settings.region.clone(),
                });
        crate::control_plane::connect_on_startup_with_storage(requested_byo).await?;
        settings.load_deployment = true;
        let (storage, credential_version) =
            crate::control_plane::installation_storage_with_version()?;
        adapter_credential_version = Some(credential_version);
        match storage {
            crate::control_plane::InstallationStorageConfig::Managed(storage) => {
                settings.bucket = Some(storage.bucket.clone());
                settings.endpoint = Some(storage.endpoint.clone());
                settings.region = storage.region.clone();
                Some(storage)
            }
            crate::control_plane::InstallationStorageConfig::Byo(storage) => {
                settings.bucket = Some(storage.bucket);
                settings.endpoint = storage.endpoint;
                settings.region = storage.region;
                None
            }
        }
    } else {
        None
    };
    let storage_credentials =
        managed_storage
            .as_ref()
            .map(|storage| crate::replication::StorageCredentials {
                access_key_id: storage.access_key_id.clone(),
                secret_access_key: storage.secret_access_key.clone(),
                session_token: storage.session_token.clone(),
            });
    let (tx, rx) = mpsc::unbounded_channel();
    let sample_tx = tx.clone();
    let alarm_tx = tx.clone();
    let alarm_observer: crate::runtime::AlarmObserver = Arc::new(move |cell, alarm| {
        let _ = alarm_tx.send(Message::AlarmObserved {
            cell,
            alarm,
            covered: false,
        });
    });
    let (fence_tx, fence_rx) = mpsc::unbounded_channel();
    let node = std::env::var("CELLAR_NODE").unwrap_or_else(|_| random_node_session_id());
    let clean_reload_node = node.clone();
    crate::control_plane::install_reexec_node_session_id(&node)?;
    let probe_public_key = crate::peer_probe::install_signer()?;
    // Cold routes are object-store latency, not compute: the default is
    // derived from the thread count but far above it. See
    // `machine::default_max_activations` for the measurements.
    let max_activations =
        crate::env_vars::positive::<usize>("CELLAR_ACTIVATIONS")?.unwrap_or_else(|| {
            crate::machine::default_max_activations(
                std::thread::available_parallelism()
                    .map(usize::from)
                    .unwrap_or(1),
            )
        });
    let placement_weight = crate::env_vars::positive::<u64>("CELLAR_PLACEMENT_WEIGHT")?
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|cpus| cpus.get() as u64)
                .unwrap_or(1)
        });
    let rebalance_interval_ms = crate::env_vars::optional::<u64>("CELLAR_REBALANCE_INTERVAL_MS")?
        .unwrap_or(DEFAULT_REBALANCE_INTERVAL_MS);
    // The shared fleet sample's interval: the balancing interval, or the
    // format gate's when balancing is off. Every reader of the sample, the
    // balancing tick, placement and recruitment, judges freshness by it.
    let fleet_sample_ms = if rebalance_interval_ms > 0 {
        rebalance_interval_ms
    } else {
        FORMAT_GATE_SAMPLE_MS
    };
    let max_releases = crate::env_vars::positive::<usize>("CELLAR_RELEASES")?
        .unwrap_or(DEFAULT_MAX_CONCURRENT_RELEASES);
    if raft_enabled {
        anyhow::ensure!(
            settings
                .data_dir
                .as_ref()
                .is_some_and(|path| path.is_absolute()),
            "Raft CLI startup requires --data-dir with an absolute persistent directory"
        );
        anyhow::ensure!(
            std::env::var_os("CELLAR_TEST_DATA_DIR").is_none()
                && std::env::var_os("CELLAR_WATCH").is_none(),
            "Raft CLI startup refuses test/dev directory overrides; use --data-dir"
        );
    }
    let data_dir = settings
        .data_dir
        .clone()
        .or_else(|| {
            std::env::var_os("CELLAR_TEST_DATA_DIR")
                .or_else(|| std::env::var_os("CELLAR_WATCH"))
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| std::env::temp_dir().join(format!("cellar-{}", std::process::id())));
    let asset_cache = crate::assets::AssetCache::new(crate::assets::AssetCacheOptions::from_env(
        data_dir.join("asset-cache"),
    )?)?;
    let mut deploy_agent = None;
    // The channel `POST /reload` and a managed notification nudge the pointer
    // watcher through. Without a deployment bucket the receiver is dropped
    // and a reload reports that there is no pointer to reload from.
    let (reload_tx, reload_rx) = crate::generation::reload_channel();
    let (runtime, ownership, peer_key, wake_scan, deploy_bucket) =
        if settings.bucket.is_some() && (settings.load_deployment || local_graph.is_some()) {
            let client = node_bucket(&settings, managed_storage.as_ref(), false)?;
            if settings.control_plane {
                fleet::validate_managed_bucket(&client).await?;
            } else {
                fleet::validate_bucket(&client).await?;
            }
            crate::wake_format::ensure_ready(&client).await?;
            // The list above proves the bucket answers; it does not prove the
            // store enforces the conditional writes or ranged reads that a
            // cell needs. Test both contracts here before the node serves.
            fleet::probe_storage_before_serving(&client, settings.control_plane).await?;
            let lease_client = node_bucket(&settings, managed_storage.as_ref(), true)?;
            if settings.control_plane && local_graph.is_none() {
                crate::control_plane::wait_for_initial_deployment(&client).await?;
                deploy_agent = Some(client.clone());
            } else if local_graph.is_none() {
                // A self-hosted node can start before its first `cellar
                // deploy`; wait for the pointer instead of exiting, or
                // systemd restarts the node in a loop until a deployment
                // exists. Managed mode already waited above.
                fleet::wait_for_deployment_pointer(&client).await?;
            }
            let peer_key = peer_auth::load_or_create(&client).await?;
            let watches_pointer = local_graph.is_none()
                && !(backend.is_native() && crate::env_vars::flag("CELLAR_NATIVE_DEV", false)?);
            let graph = match local_graph.take() {
                Some(graph) => graph,
                None => DeploymentGraph::load(&client, node.clone(), &asset_cache).await?,
            };
            let wake = Arc::new(crate::wake::WakeFlusher::new());
            crate::wake_entry::set_arm_gate(ArmGate {
                bucket: client.clone(),
                flusher: wake.clone(),
            });
            // A `blob_buckets` binding lives in the fleet bucket, under the
            // reserved binding prefix. cellar runs on blob storage
            // rather than providing it, so a binding gets the store the node
            // already holds credentials for instead of a second one.
            crate::host_channels::set_blob_store(client.clone());
            // cellar treats replication as a node service, not as a property
            // of today's manifest. Start it even for a stateless deployment
            // so a later deployment can introduce cells without changing the
            // durability contract underneath the node.
            let replication = Some(Replication::from_environment(
                client.clone(),
                &data_dir,
                settings.endpoint.clone(),
                settings.region.clone(),
                storage_credentials.clone(),
            )?);
            // The same two calls a reload makes: there is no boot-only way from
            // a deployment to a serving generation.
            let generation = backend.build(
                FIRST_GENERATION,
                graph,
                GenerationOptions {
                    node: node.clone(),
                    region: settings.region.clone(),
                },
            )?;
            let runtime = RuntimeManager::start(
                generation,
                RuntimeOptions {
                    data_dir: data_dir.clone(),
                    replication,
                    wake: Some(wake.clone()),
                    alarm_observer: alarm_observer.clone(),
                    node: node.clone(),
                    region: settings.region.clone(),
                    bucket: Some(client.clone()),
                },
            )?;
            let deploy_bucket = watches_pointer.then(|| client.clone());
            let bucket_ownership = Arc::new(
                BucketOwnership::new(
                    client.clone(),
                    lease_client,
                    node.clone(),
                    probe_public_key.clone(),
                )
                .with_lease_ttl_ms(lease_ttl_ms_from_environment())
                .with_fleet_sample_ms(fleet_sample_ms),
            );
            let wake_scan = Some((client, bucket_ownership.clone()));
            let ownership = Ownership::Bucket(bucket_ownership);
            (
                Some(runtime),
                Some(ownership),
                peer_key,
                wake_scan,
                deploy_bucket,
            )
        } else if let Some(graph) = local_graph.take() {
            let generation = backend.build(
                FIRST_GENERATION,
                graph,
                GenerationOptions {
                    node: node.clone(),
                    region: settings.region.clone(),
                },
            )?;
            (
                Some(RuntimeManager::start(
                    generation,
                    RuntimeOptions {
                        data_dir: data_dir.clone(),
                        replication: None,
                        wake: None,
                        alarm_observer: alarm_observer.clone(),
                        node: node.clone(),
                        region: settings.region.clone(),
                        bucket: None,
                    },
                )?),
                None,
                random_peer_key(),
                None,
                None,
            )
        } else if let Ok(script_path) = std::env::var("CELLAR_TEST_SCRIPT_PATH") {
            let source = std::fs::read_to_string(&script_path)?;
            let do_classes = std::env::var("CELLAR_TEST_DO_CLASSES")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            let bindings = std::env::var("CELLAR_TEST_DO_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, class)| (name.trim().to_string(), class.trim().to_string()))
                .filter(|(name, class)| !name.is_empty() && !class.is_empty())
                .collect();
            // `BINDING=database` pairs, the local-script equivalent of
            // `sql_databases` in a deployed project.
            let sql_bindings: Vec<(String, String)> = std::env::var("CELLAR_TEST_SQL_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, database)| (name.trim().to_string(), database.trim().to_string()))
                .filter(|(name, database)| !name.is_empty() && !database.is_empty())
                .collect();
            // `BINDING=namespace-id`, the `CELLAR_TEST_SQL_BINDINGS` shape. Day one,
            // for the reason SQL's and Workflows' equivalents landed on day one:
            // every runtime test needs it.
            let kv_bindings: Vec<(String, String)> = std::env::var("CELLAR_LOCAL_KV_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, id)| (name.trim().to_string(), id.trim().to_string()))
                .filter(|(name, id)| !name.is_empty() && !id.is_empty())
                .collect();
            // `BINDING=queue`, the local-script equivalent of one Wrangler Queue
            // producer. A local binding has no configuration object for a default
            // delay, so it uses the Queue default of zero.
            let queue_bindings: Vec<QueueBinding> = std::env::var("CELLAR_LOCAL_QUEUE_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(environment, queue)| QueueBinding {
                    environment: environment.trim().to_string(),
                    queue: queue.trim().to_string(),
                    delivery_delay: 0,
                })
                .filter(|binding| !binding.environment.is_empty() && !binding.queue.is_empty())
                .collect();
            for binding in &queue_bindings {
                anyhow::ensure!(
                    cellar_logic::cell::valid_cell_scope(&binding.queue),
                    "local Queue binding {} has a queue name that cannot name a cell: {:?}",
                    binding.environment,
                    binding.queue,
                );
            }
            // `BINDING=bucket` pairs, the local-script equivalent of
            // `blob_buckets` in a deployed project.
            let blob_bindings: Vec<(String, String)> = std::env::var("CELLAR_TEST_BLOB_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, bucket)| (name.trim().to_string(), bucket.trim().to_string()))
                .filter(|(name, bucket)| !name.is_empty() && !bucket.is_empty())
                .collect();
            let mut do_classes: Vec<String> = do_classes;
            if !sql_bindings.is_empty() {
                do_classes.push(crate::deploy::SQL_CLASS.to_string());
            }
            if !kv_bindings.is_empty() {
                do_classes.push(crate::deploy::KV_CLASS.to_string());
            }
            if !queue_bindings.is_empty() {
                do_classes.push(crate::deploy::QUEUE_CLASS.to_string());
            }
            let workflow_bindings: Vec<WorkflowBinding> =
                std::env::var("CELLAR_LOCAL_WORKFLOW_BINDINGS")
                    .unwrap_or_default()
                    .split(',')
                    .filter_map(|value| {
                        let (binding, rest) = value.split_once('=')?;
                        let (name, class) = rest.split_once('=')?;
                        Some(WorkflowBinding {
                            environment: binding.trim().to_string(),
                            workflow: name.trim().to_string(),
                            class: class.trim().to_string(),
                            settings: Default::default(),
                        })
                    })
                    .filter(|binding| {
                        !binding.environment.is_empty()
                            && !binding.workflow.is_empty()
                            && !binding.class.is_empty()
                    })
                    .collect();
            // Resolved before the config is built, because the reserved workflow
            // class is script-scoped and `do_classes` has to carry the scoped name.
            let script_name = std::env::var("CELLAR_TEST_SCRIPT_NAME")
                .unwrap_or_else(|_| "cellar-local".to_string());
            if !workflow_bindings.is_empty() {
                do_classes.push(crate::deploy::workflow_class(&script_name));
            }
            let crons: Vec<String> = std::env::var("CELLAR_TEST_CRONS")
                .unwrap_or_default()
                .split(';')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            let options = WorkerConfigOptions {
                stream_bindings: Vec::new(),
                src: source,
                script_name,
                do_classes,
                bindings,
                blob_bindings,
                sql_bindings,
                kv_bindings,
                queue_bindings,
                queue_consumers: Vec::new(),
                workflow_bindings,
                vars: Vec::new(),
                node: node.clone(),
                modules: Vec::new(),
                compat: Compat::default(),
            };
            let (ownership, peer_key, wake, wake_scan) = if settings.bucket.is_some() {
                let client = node_bucket(&settings, managed_storage.as_ref(), false)?;
                crate::wake_format::ensure_ready(&client).await?;
                let lease_client = node_bucket(&settings, managed_storage.as_ref(), true)?;
                let peer_key = peer_auth::load_or_create(&client).await?;
                let wake = Arc::new(crate::wake::WakeFlusher::new());
                crate::wake_entry::set_arm_gate(ArmGate {
                    bucket: client.clone(),
                    flusher: wake.clone(),
                });
                crate::host_channels::set_blob_store(client.clone());
                let bucket_ownership = Arc::new(
                    BucketOwnership::new(
                        client.clone(),
                        lease_client,
                        node.clone(),
                        probe_public_key.clone(),
                    )
                    .with_lease_ttl_ms(lease_ttl_ms_from_environment())
                    .with_fleet_sample_ms(fleet_sample_ms),
                );
                let wake_scan = Some((client, bucket_ownership.clone()));
                (
                    Some(Ownership::Bucket(bucket_ownership)),
                    peer_key,
                    Some(wake),
                    wake_scan,
                )
            } else {
                (None, random_peer_key(), None, None)
            };
            // A local script is a one-script deployment with no pointer behind
            // it, built through the same function as a bucket deployment.
            let script_name = options.script_name.clone();
            let generation = backend.build(
                FIRST_GENERATION,
                DeploymentGraph::single(fleet::LoadedDeployment {
                    event_streams: Vec::new(),
                    native: None,
                    options,
                    script_name,
                    version: "local".to_string(),
                    prefix: script_path.clone(),
                    asset_binding: None,
                    // A bare script has no wrangler config, so this names the
                    // loaders the way `CELLAR_TEST_DO_CLASSES` names a class:
                    // a comma-separated list, because a config can declare
                    // more than one loader. It is the local-script equivalent
                    // of `worker_loaders`, and not a production switch: a
                    // deployed project reaches a loader only through its own
                    // config.
                    loader_bindings: std::env::var("CELLAR_TEST_WORKER_LOADER")
                        .unwrap_or_default()
                        .split(',')
                        .map(str::trim)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string)
                        .collect(),
                    assets: None,
                    services: Vec::new(),
                    crons,
                    cron_revision: None,
                    containers: Vec::new(),
                    fence_image: None,
                    container_images: Vec::new(),
                }),
                GenerationOptions {
                    node: node.clone(),
                    region: settings.region.clone(),
                },
            )?;
            (
                Some(RuntimeManager::start(
                    generation,
                    RuntimeOptions {
                        data_dir: data_dir.clone(),
                        replication: None,
                        wake: wake.clone(),
                        alarm_observer: alarm_observer.clone(),
                        node: node.clone(),
                        region: settings.region.clone(),
                        bucket: None,
                    },
                )?),
                ownership,
                peer_key,
                wake_scan,
                None,
            )
        } else {
            let (ownership, peer_key) = if settings.bucket.is_some() {
                let client = node_bucket(&settings, managed_storage.as_ref(), false)?;
                crate::wake_format::ensure_ready(&client).await?;
                let lease_client = node_bucket(&settings, managed_storage.as_ref(), true)?;
                let peer_key = peer_auth::load_or_create(&client).await?;
                (
                    Some(Ownership::Bucket(Arc::new(
                        BucketOwnership::new(
                            client,
                            lease_client,
                            node.clone(),
                            probe_public_key.clone(),
                        )
                        .with_lease_ttl_ms(lease_ttl_ms_from_environment())
                        .with_fleet_sample_ms(fleet_sample_ms),
                    ))),
                    peer_key,
                )
            } else {
                (None, random_peer_key())
            };
            (None, ownership, peer_key, None, None)
        };
    if let Some(runtime) = &runtime {
        runtime.generation().begin_event_activation();
    }
    if let Some(config) = &telemetry_config {
        let sink_bucket = match &config.sink {
            crate::telemetry::SinkChoice::Bucket => {
                if settings.bucket.is_none() {
                    anyhow::bail!(
                        "CELLAR_OTEL=1 but this node has no bucket; the \
                         bucket sink needs one (CELLAR_BUCKET), or choose \
                         CELLAR_OTEL=http://collector:4318"
                    );
                }
                // Its own client even for the fleet bucket: each open is its
                // own transport (bucket.rs), so telemetry PUT bursts never
                // share a connection pool with ownership traffic.
                Some(if let Some(bucket) = config.bucket_override.as_deref() {
                    fleet::bucket_client_with_credentials(
                        bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?
                } else {
                    node_bucket(&settings, managed_storage.as_ref(), false)?
                })
            }
            // The collector path needs no bucket at all.
            crate::telemetry::SinkChoice::Otlp { .. } => None,
        };
        crate::telemetry::init(config, sink_bucket, node.clone(), settings.region.clone())?;
    }
    let peer_auth = Arc::new(PeerAuth::new(peer_key, node.clone())?);
    // Connect-only timeout: a peer request may legitimately run through a
    // restore, but a handshake that never completes provably ran nothing.
    let peer_http = reqwest::Client::builder()
        .connect_timeout(PEER_CONNECT_TIMEOUT)
        // Nagle against delayed ACKs costs tens of milliseconds on every
        // multi-segment peer body. Peer requests are latency-bound, so the
        // socket must write eagerly.
        .tcp_nodelay(true)
        .build()
        .unwrap();
    let resume_generation = crate::runtime::take_clean_reload_generation(&data_dir, &node);
    let clean_reload_candidate = resume_generation.is_some();
    let drain_pins = DrainPinRegistry::default();
    let actor = Actor::from_environment_with_services(
        AdmissionLimits {
            resident: max_resident,
            activations: max_activations,
            releases: max_releases,
            placement_weight,
        },
        fail_publish_once,
        fence_tx,
        ActorServices {
            runtime: runtime.clone(),
            drain_pins: drain_pins.clone(),
            ownership,
            peer_http: peer_http.clone(),
            peer_auth: peer_auth.clone(),
        },
        ActorIdentity {
            node: node.clone(),
            advertise: advertise.clone(),
            region: settings.region.clone(),
        },
        resume_generation,
    )
    .await?;
    let process_generation = actor.lease_spec.generation.clone();
    let ownership_name = actor.ownership.name();
    // Keep the fleet reader beside the readiness task before the actor moves
    // to its isolated thread. Both handles share the same lease adapter, so
    // readiness evaluates exactly the records that placement publishes.
    let ready_ownership = match &actor.ownership {
        Ownership::Bucket(ownership) => Some(ownership.clone()),
        Ownership::Memory(_) => None,
    };
    let fleet_bucket = ready_ownership
        .as_ref()
        .map(|ownership| ownership.bucket_client());
    let explorer_replication = runtime.as_ref().and_then(|runtime| runtime.replication());
    let (websocket_tx, websocket_rx) = mpsc::unbounded_channel();
    // The log tier's follower store (crate::node_log): fragments other
    // leaders replicate here live under the node's data dir beside the cell
    // databases.
    let follower = match &actor.ownership {
        Ownership::Bucket(bucket_ownership) if settings.bucket.is_some() => {
            Some(Arc::new(crate::node_log::FollowerStore::new(
                &data_dir,
                Some(Arc::new(bucket_ownership.bucket_client())),
                &node,
            )))
        }
        _ => None,
    };
    #[cfg(all(test, cellar_internal_tests))]
    let follower = follower.or_else(|| {
        shutdown_accept_failure_test_active
            .then(|| Arc::new(crate::node_log::FollowerStore::new(&data_dir, None, &node)))
    });
    let app = AppHandle {
        tx,
        runtime,
        #[cfg(all(test, cellar_internal_tests))]
        scripted_activity_host: None,
        reload: reload_tx.clone(),
        peer_http,
        peer_auth,
        advertise: advertise.clone(),
        websockets: websocket_tx,
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        public_in_flight: Arc::new(AtomicUsize::new(0)),
        drain_pins,
        trust_forwarded_headers: settings.trust_forwarded_headers,
        max_outbound_websockets: crate::env_vars::positive_or(
            "CELLAR_MAX_OUTBOUND_WEBSOCKETS",
            DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
        )?,
        max_request_body_bytes,
        operation_deadline_ms,
        follower: follower.clone(),
    };

    // The in-fleet log tier, v0. The takeover interlock is installed in
    // every posture — a bucket-posture node can take over from a
    // fleet-posture one and must find a complete bucket — while shipping
    // requires the explicit fleet posture.
    // Fleet is the default: without a healthy follower, writes need bucket
    // proof. Maintenance recruits up to two followers; one healthy follower
    // is sufficient for fleet proof. CELLAR_DURABILITY=bucket always requires
    // bucket proof, including when peers are available.
    let durability = std::env::var("CELLAR_DURABILITY").unwrap_or_else(|_| "fleet".into());
    anyhow::ensure!(
        matches!(durability.as_str(), "bucket" | "fleet"),
        "CELLAR_DURABILITY must be `bucket` or `fleet`"
    );
    let mut durability_owner = DurabilityOwnerSelection::new(follower.clone());
    if let Ownership::Bucket(bucket_ownership) = &actor.ownership {
        if let (Some(replication), Some(spec)) = (
            app.runtime
                .as_ref()
                .and_then(|runtime| runtime.replication()),
            settings.bucket.clone(),
        ) {
            let _ = spec;
            let log_bucket = Arc::new(bucket_ownership.bucket_client());
            let ltx = replication.ltx();
            // Bundle the paced tiering: one PUT per node-flush instead of
            // one per cell-transaction — the Class A collapse, measured at
            // 208x against per-transaction PUTs. The shipper and the bundle
            // sink are installed as one durability registration.
            let nudge_tx = app.tx.clone();
            let own_log = Arc::new(crate::node_log::OwnLog {
                ownership: bucket_ownership.clone(),
                nudge: Box::new(move || {
                    let _ = nudge_tx.send(Message::NudgeNodeLease);
                }),
                write_lock: tokio::sync::Mutex::new(()),
            });
            let manager = Arc::new(crate::node_log::NodeLogManager::from_environment(
                &bucket_ownership.log_session()?,
                log_bucket,
                own_log,
                ltx.clone(),
                app.peer_auth.clone(),
            )?);
            *actor.node_log.lock().unwrap() = Some(manager.clone());
            // Recovery-before-install is fatal for every posture: the
            // predecessor's folded state lives in the lease record this
            // session is about to replace, and the install writes a fresh
            // log — so an unrecovered predecessor must stop the boot, or
            // the install erases the only evidence recovery was needed.
            // The ladder waits out the predecessor's own lease because the
            // fence recheck refuses a live lease, retries with backoff,
            // and exits on exhaustion. Serve the retained follower disks
            // throughout the ladder, including retry backoffs, so a fleet
            // restart can recover without waiting for another node's lease.
            recover_with_follower_listener(&internal_listener, app.clone(), async {
                let ttl_backoff =
                    std::time::Duration::from_millis(actor.lease_spec.ttl_ms.max(1_000));
                let mut recovered = Ok(());
                // A boot waits behind a peer's live recovery claim and takes
                // it over only once its heartbeat is stale, so one attempt
                // can legitimately last as long as the peer's gather. The
                // bound is a backstop against a wedge, not a pace.
                for attempt in 1..=4u32 {
                    recovered = match tokio::time::timeout(
                        std::time::Duration::from_secs(15 * 60),
                        manager.recover_self(),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!("predecessor recovery timed out")),
                    };
                    if recovered.is_ok() || attempt == 4 {
                        break;
                    }
                    if let Err(error) = recovered.as_ref() {
                        eprintln!(
                            "cellar predecessor recovery attempt {attempt} failed: {error:#}"
                        );
                        // Retain the TTL backoff for a live predecessor or an
                        // unchanged failure. A returning witness is a reason
                        // to retry proof before its own activation expires.
                        manager
                            .wait_for_startup_retry(error, ttl_backoff.saturating_mul(attempt))
                            .await;
                    }
                }
                recovered.map_err(|error| {
                    anyhow::anyhow!(
                        "refusing to install a lease over an unrecovered \
                         predecessor log: {error:#}"
                    )
                })
            })
            .await?;
            let mut owner = crate::node_log::DurabilityOwner::new(manager, durability == "fleet");
            owner.start_background(follower.clone());
            durability_owner.install_runtime(owner);
        }
    }
    let durability_owner = durability_owner.select();
    let channels = driver::Channels::install(websocket_rx);
    // KV large values live in the fleet bucket. The cloneable handle follows
    // the Blob binding's startup pattern, so independent cells issue bucket I/O
    // independently instead of queueing behind one task. Blob I/O takes no
    // part in the drain ordering: a blob is written before its row, so an
    // interrupted write leaves collectable bytes and never a dangling row.
    if let Ownership::Bucket(bucket_ownership) = &actor.ownership {
        crate::host_channels::set_kv_blob_store(bucket_ownership.bucket_client());
    }
    // Seed `/state` and the first lease before the core starts or the ticker posts a sample.
    let services = crate::asyncrt::services();
    let initial_sample = crate::asyncrt::try_blocking(move || services.sample_metrics())?.await?;
    actor.store_load_sample(initial_sample);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    if let Some(raft) = raft {
        let result = {
            let supplied_stop = async {
                match stop.as_mut() {
                    Some(stop) => stop.await,
                    None => std::future::pending::<()>().await,
                }
            };
            crate::asyncrt::select! {
                result = raft.start(&app, data_dir.clone(), node.clone(), fleet_bucket.is_some()) => result,
                _ = supplied_stop => Err(anyhow::anyhow!("Raft startup cancelled by host")),
                _ = sigterm.recv() => Err(anyhow::anyhow!("Raft startup interrupted by SIGTERM")),
                _ = sigint.recv() => Err(anyhow::anyhow!("Raft startup interrupted by SIGINT")),
            }
        };
        if let Err(error) = result {
            let _ = crate::asyncrt::services().raft_lifecycle().close().await;
            #[cfg(not(cellar_internal_tests))]
            crate::asyncrt::shutdown_process_domain().await;
            return Err(error.context("production Raft startup refused; retain replica disks"));
        }
    }
    // The core is a serial ownership actor, not a Worker executor. It owns the
    // node lease timer, so ingress, proxy retries, and restore completions must
    // not consume every scheduler turn it needs. Its isolated single-thread
    // runtime also keeps state transitions ordered exactly as the deterministic
    // executor models them. Request work, restores, and blocking scans stay on
    // the shared runtime and report their results back as messages.
    let (actor_exit_tx, actor_exit_rx) = mpsc::unbounded_channel();
    let core_thread = std::thread::Builder::new()
        .name("cellar-core".into())
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .and_then(|runtime| {
                    #[cfg(not(cellar_internal_tests))]
                    let result = runtime
                        .block_on(crate::asyncrt::run_in_process_domain(actor.run(rx)))
                        .map_err(|error| error.to_string())?;
                    #[cfg(cellar_internal_tests)]
                    let result = runtime.block_on(actor.run(rx));
                    result.map_err(|error| error.to_string())
                });
            let _ = actor_exit_tx.send(result);
        })?;

    // The sampler is a plain ticker: it measures and posts, and decides
    // nothing. Everything downstream of the numbers -- the latch, the target,
    // which cell goes -- is in the core, so a sample sequence replays.
    {
        crate::asyncrt::spawn(async move {
            let mut tick = crate::asyncrt::interval(LOAD_SAMPLE_PERIOD);
            tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
            // The seed sample above takes the CPU baseline. Sampling again on
            // the immediate tick divides CPU clock ticks by too little time and
            // spikes the load.
            tick.tick().await;
            loop {
                tick.tick().await;
                // What SQLite freed when a cell stopped stays on the C
                // allocator's free lists, and the sample would count it as
                // the node's: 5,000 hibernated cells kept RSS at 1.6 GB with
                // 1.5 GB of it free (GCE, 2026-09-04). Trimmed here,
                // ahead of the sample, on a blocking thread because a trim
                // walks every free chunk.
                let Ok(trim) = crate::asyncrt::try_blocking(crate::memory::trim_c_heap_if_retained)
                else {
                    return;
                };
                let _ = trim.await;
                let services = crate::asyncrt::services();
                let Ok(sample_task) =
                    crate::asyncrt::try_blocking(move || services.sample_metrics())
                else {
                    return;
                };
                let sample = match sample_task.await {
                    Ok(sample) => sample,
                    Err(error) => {
                        tracing::warn!(
                            event = "load_sample_failed",
                            %error,
                            "process load sample failed"
                        );
                        continue;
                    }
                };
                if sample_tx.send(Message::SampleLoad(sample)).is_err() {
                    return;
                }
            }
        })
        .detach();
    }

    // The fleet-aware first-readiness gate: withhold the first health 200
    // until no donor is mid-handoff and every live peer publishes successor
    // capacity. CELLAR_READY_FLEET_GATE_MS bounds the silent wait before cellar
    // reports the blocking condition, but it cannot make an unsafe fleet
    // ready. Orchestrators key rollout advance on readiness, so a fail-open
    // deadline would erase the only signal that prevents another donor. The
    // gate is one-shot: once open, fleet state never demotes readiness, and a
    // gated node still serves its owned cells through peers.
    let ready_gate_ms: u64 = crate::env_vars::with_default("CELLAR_READY_FLEET_GATE_MS", 120_000)?;
    match (&ready_ownership, ready_gate_ms) {
        (Some(ownership), gate_ms) if gate_ms > 0 => {
            let ownership = ownership.clone();
            let bucket = ownership.bucket_client();
            let gate_node = node.clone();
            let gate_flag = app.fleet_ready.clone();
            let max_restoring = max_activations as u64;
            crate::asyncrt::spawn(async move {
                let started = std::time::Instant::now();
                let mut deadline_reported = false;
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let (current, peers) = tokio::join!(
                        crate::drain_token::read(&bucket),
                        ownership.read_capacity_peers()
                    );
                    // Judge lease and token expiry after the I/O. A slow list
                    // must not make a record live at the time before the read.
                    let now_ms = crate::ownership_store::now_ms();
                    let status = match (&current, &peers) {
                        (Ok(current), Ok(peers)) => cellar_logic::drain::fleet_status(
                            current.as_ref().and_then(|current| current.token.as_ref()),
                            &gate_node,
                            now_ms,
                            peers,
                            max_restoring,
                        ),
                        // An unreadable store is not settled; the deadline
                        // bounds the wait.
                        _ => Err(cellar_logic::drain::FleetUnsettled::Unreadable),
                    };
                    let waited_ms = started.elapsed().as_millis() as u64;
                    if status.is_ok() {
                        let readiness_reason = if peers.as_ref().ok().is_some_and(|peers| {
                            cellar_logic::drain::joining_contributes_successor_capacity(
                                peers, &gate_node, now_ms,
                            )
                        }) {
                            "successor_capacity"
                        } else {
                            "fleet_settled"
                        };
                        gate_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                        tracing::info!(
                            event = "ready_gate_open",
                            readiness_reason,
                            waited_ms,
                            "the fleet is settled; serving readiness is open"
                        );
                        return;
                    }
                    if waited_ms >= gate_ms && !deadline_reported {
                        deadline_reported = true;
                        let holder = current
                            .as_ref()
                            .ok()
                            .and_then(|current| current.as_ref())
                            .and_then(|current| current.token.as_ref())
                            .map(|token| token.node.as_str())
                            .unwrap_or_default();
                        tracing::warn!(
                            event = "ready_gate_expired",
                            holder,
                            reason = ?status.err(),
                            waited_ms,
                            "the ready-gate observation deadline expired; readiness stays closed"
                        );
                    }
                }
            })
            .detach();
        }
        _ => app
            .fleet_ready
            .store(true, std::sync::atomic::Ordering::SeqCst),
    }

    // Ownership balancing. Each node reads the shared fleet sample and, when it
    // is the densest node and a peer has room, hands a bounded batch of idle
    // cells through the drain's release-and-adopt pipeline. Cell moves need
    // no fleet-wide lock: the per-cell ownership CAS is the authority, and the
    // densest-node rule elects one donor per consistent sample. A donor
    // plans again only from leases sampled after its last batch had one
    // load-sample period to reach them, so no plan mixes counts from before
    // a batch with counts from after it.
    //
    // The same lease sample carries the bucket-format gate
    // (cellar_logic::format): a node pages a large takeover only while every
    // live lease reads a paged epoch. The loop therefore runs even with
    // balancing off, at the balancing interval or FORMAT_GATE_SAMPLE_MS, and
    // the gate is judged before any balancing condition can skip the tick.
    // Off until the first sample, so a takeover in the first seconds of a
    // process clones, which is always safe.
    if let Some(ownership) = ready_ownership.clone() {
        let app = app.clone();
        let node = node.clone();
        let replication = explorer_replication.clone();
        let sample_ms = fleet_sample_ms;
        crate::asyncrt::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(sample_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut settled_after_ms = 0;
            loop {
                tick.tick().await;
                let now_ms = crate::ownership_store::now_ms();
                // The view is judged at the instant the sample was taken.
                // Its leases are copies that age while the real nodes renew,
                // and nodes started together renew in lock-step, so judged
                // at this node's clock a view a few seconds old shows every
                // lease dead at once: on the 2026-09-05 fleet that closed the
                // gate and skipped balancing on a third of the ticks.
                let (view_ms, peers) = match ownership.read_shared_capacity_peers(sample_ms).await {
                    Ok(view) => view,
                    Err(error) => {
                        // At info with the chain: a failed tick closes the
                        // format gate, and a fleet run has to say why.
                        tracing::info!(event = "fleet_sample_failed", error = format!("{error:#}"));
                        // A missing or in-flight refresh cannot keep granting
                        // format permission from an earlier fleet membership.
                        (now_ms, Vec::new())
                    }
                };
                if let Some(replication) = &replication {
                    let format = cellar_logic::format::BUCKET_FORMAT;
                    let open = cellar_logic::format::fleet_reads(&peers, view_ms, format);
                    if replication.set_paged_fleet(open) != open {
                        tracing::info!(
                            event = "paged_gate",
                            open,
                            format,
                            live_leases = peers.iter().filter(|p| p.expires_ms > view_ms).count(),
                            view_age_ms = now_ms.saturating_sub(view_ms),
                            "every live lease reads a paged epoch, or one does not"
                        );
                    }
                }
                if rebalance_interval_ms == 0 {
                    continue;
                }
                let paused = ownership
                    .live()
                    .rebalance_paused
                    .load(std::sync::atomic::Ordering::Relaxed);
                if !app.fleet_ready() || app.is_draining() || paused {
                    continue;
                }
                // One batch outstanding at a time, but one cell must not
                // hold the fleet: a cell whose proof cannot finish stays in
                // flight for a long time, and the densest node is the only
                // donor. Plan around it with the rest of the batch.
                let status = app.drain_status().await;
                let in_flight =
                    status.quiescing + status.evicting + status.releasing + status.adopting;
                if in_flight >= DEFAULT_REBALANCE_BATCH_CELLS {
                    continue;
                }
                let Some(cells) = cellar_logic::rebalance::surplus(
                    &peers,
                    &node,
                    view_ms,
                    ownership.lease_ttl_ms(),
                    settled_after_ms,
                    DEFAULT_REBALANCE_BATCH_CELLS - in_flight,
                ) else {
                    continue;
                };
                tracing::info!(
                    event = "rebalance_batch",
                    cells,
                    "handing idle cells to peers below their ownership target"
                );
                if app.tx.send(Message::Rebalance { cells }).is_err() {
                    return;
                }
                settled_after_ms = now_ms + LOAD_SAMPLE_PERIOD.as_millis() as u64;
            }
        })
        .detach();
    }

    if let Some((client, ownership)) = wake_scan {
        let authority_wait_seconds = if clean_reload_candidate { 60 } else { 10 };
        let deadline = Instant::now() + std::time::Duration::from_secs(authority_wait_seconds);
        while !app.healthy().await {
            if Instant::now() >= deadline {
                anyhow::bail!("node authority was not established before wake scan");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // The boot scan acts for the fleet: it is what finds the alarms this
        // node's previous incarnation left behind, and any orphan that came
        // due while nothing was watching. It probes like the elected waker
        // does, so a rolling restart does not send every node the whole
        // fleet's due set once each.

        // And again, on a timer, for the rest of the process's life.
        //
        // The boot scan alone only covers alarms that came due while nothing
        // was watching *before this node started*. A node that dies while
        // this one is already running leaves its cells with armed alarms and
        // no owner, and nothing would look at them again until this process
        // restarted -- an alarm silently not firing, which is the one thing a
        // Durable Object is not allowed to do.
        //
        // Every node lists the due entries on its tick, because a node's own
        // dormant cells can carry alarms it has never loaded (a cell that
        // arrived by an ownership-only handoff), and only that node can wake
        // them. But only the node that holds the advisory waker role acts for
        // the fleet: it reads each due cell's owner outside the activation
        // queue and hints the orphans, while every other node hints with
        // `Owned` scope, which the core answers from memory. Before that
        // split, every node pushed the whole fleet's due set through its own
        // activation queue, one permit and one owner read per entry, and its
        // own alarm wakes queued behind them.
        //
        // Every decision about a hinted entry still stays in the core: a
        // hint for a cell this node already serves is ignored, and one for a
        // cell with a live owner elsewhere resolves to that owner rather
        // than stealing it.
        let tick_ms = crate::env_vars::positive::<u64>("CELLAR_WAKER_TICK_MS")?.unwrap_or(60_000);
        wake_discovery::start(&app, client, ownership, &node, tick_ms)?;
    }

    // Arm the schedule, then watch the pointer. Adoption arms it again, so
    // a cron change travels with the deployment that carries it.
    spawn_cron_arm(app.clone());
    if let Some(bucket) = deploy_bucket.filter(|_| !raft_enabled) {
        start_pointer_watcher(
            app.clone(),
            DeploymentResources {
                bucket,
                assets: asset_cache,
            },
            node.clone(),
            settings.region.clone(),
            reload_rx,
            backend.clone(),
        );
    } else {
        // Without a pointer watcher, keeping the receiver alive makes a
        // reload enqueue successfully and then wait forever for its reply.
        drop(reload_rx);
    }
    if let Some(client) = deploy_agent {
        crate::control_plane::start_deploy_agent(client.clone(), reload_tx.clone());
        let presence_app = app.clone();
        crate::control_plane::start_presence_agent(crate::control_plane::PresenceRuntime {
            s3: client,
            replication: explorer_replication,
            node_session_id: node,
            advertise,
            listen,
            credential_version: adapter_credential_version
                .expect("managed adapters have a credential version"),
            snapshot: Arc::new(move || {
                let app = presence_app.clone();
                Box::pin(async move { app.presence().await })
            }),
            reload: reload_tx.clone(),
        });
    }

    crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
        "cellar listening on {} (ownership={ownership_name})",
        listener.local_addr()?
    ))?;
    crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
        "cellar internal listening on {} (advertise={})",
        internal_listener.local_addr()?,
        app.advertise
    ))?;
    #[cfg(not(cellar_internal_tests))]
    let stop_operations = fleet_bucket
        .clone()
        .map(|bucket| {
            operations::NodeOperations::install(
                bucket,
                clean_reload_node.clone(),
                process_generation.clone(),
            )
        })
        .transpose()?;
    let outcome = driver::run(
        driver::PreparedDriver {
            app,
            listener: Some(listener),
            external_ingress: None,
            internal_listener,
            channels,
            fence_rx,
            actor_exit_rx,
            ready_ownership,
            fleet_bucket,
            clean_reload_node,
            process_generation,
            data_dir,
            durability_owner,
            policy: driver::RunPolicy {
                shutdown: shutdown_policy,
                local_cache_max_bytes,
                local_cache_prune_period: LOCAL_CACHE_PRUNE_PERIOD,
            },
            #[cfg(all(test, cellar_internal_tests))]
            shutdown_accept_failure_test_active,
        },
        async move {
            let supplied_stop = async move {
                match stop {
                    Some(stop) => stop.await,
                    None => std::future::pending::<()>().await,
                }
            };
            crate::asyncrt::select! {
                _ = supplied_stop => ShutdownMode::Preserve,
                _ = sigterm.recv() => ShutdownMode::Handoff,
                _ = sigint.recv() => ShutdownMode::Handoff,
            }
        },
    )
    .await;
    #[cfg(not(cellar_internal_tests))]
    let outcome = {
        crate::asyncrt::shutdown_process_domain().await;
        // Joining a host thread belongs to the CLI boundary, after its domain
        // has dropped the tracked core root. Keep the directory lock until then.
        let core_join = tokio::task::spawn_blocking(move || core_thread.join()).await;
        let outcome = match core_join {
            Ok(Ok(())) => outcome,
            _ => Err(anyhow::anyhow!("core host thread failed while joining")),
        };
        if let Some(operations) = stop_operations {
            operations
                .joined(&outcome, tokio::runtime::Handle::current())
                .await;
        }
        outcome
    };
    #[cfg(cellar_internal_tests)]
    drop(core_thread);
    let outcome = outcome?;
    if let Some(observer) = observer {
        observer(outcome.clone());
    }
    if shutdown_report_json {
        crate::cli_output::Output::new(crate::cli_output::Format::Text)
            .line(format_args!("{}", outcome.cli_report()))?;
    }
    if return_to_caller {
        drop(LOG_GUARD.lock().unwrap().take());
        anyhow::ensure!(
            outcome.cli_exit_code() == 0,
            "host shutdown failed: {outcome:?}"
        );
        return Ok(());
    }
    // Process ownership belongs to this command-line adapter alone.
    exit_flushed(outcome.cli_exit_code());
}

use crate::machine::{
    lease_ttl_ms_from_environment, local_cache_max_bytes_from_environment, random_node_session_id,
    random_peer_key, DEFAULT_MAX_OUTBOUND_WEBSOCKETS, PEER_CONNECT_TIMEOUT,
};

/// Signals cancellation when the connection handling this request goes away.
struct HangUp(Option<oneshot::Sender<()>>);

impl Drop for HangUp {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Abandons a forwarded fetch on the owner when the peer connection carrying
/// it goes away. After the handler answers, a drop cancels only its response
/// body pin because the isolate has no unanswered request left to abort.
struct AbortPeerFetchOnHangUp {
    runtime: RuntimeManager,
    scope: String,
    request_id: Option<crate::engine_api::RequestId>,
    drain_pins: DrainPinRegistry,
    handler_active: bool,
}

impl AbortPeerFetchOnHangUp {
    fn disarm(&mut self) {
        self.request_id = None;
    }

    fn handler_answered(&mut self) {
        self.handler_active = false;
    }
}

impl Drop for AbortPeerFetchOnHangUp {
    fn drop(&mut self) {
        if let Some(request_id) = self.request_id {
            let _ = self
                .drain_pins
                .cancel_request(request_id, "peer_connection_closed");
            if self.handler_active {
                self.runtime.abort_fetch(&self.scope, request_id);
            }
        }
    }
}

fn forwarder_response_stream(
    stream: crate::engine_api::HttpChunkStream,
    mut abort: RemoteFetchAbortGuard,
) -> crate::engine_api::HttpChunkStream {
    abort.body_active();
    Box::pin(futures_util::stream::unfold(
        (stream, Some(abort)),
        |(mut stream, mut abort)| async move {
            match stream.next().await {
                Some(chunk) => {
                    // An upstream body error already ended the owner
                    // transport. A second connection carrying an abort adds
                    // no signal; only a downstream drop while the upstream is
                    // still live needs the explicit peer request.
                    if chunk.is_err() {
                        if let Some(mut abort) = abort.take() {
                            abort.disarm();
                        }
                    }
                    Some((chunk, (stream, abort)))
                }
                None => {
                    if let Some(mut abort) = abort.take() {
                        abort.disarm();
                    }
                    None
                }
            }
        },
    ))
}

#[cfg(all(test, cellar_internal_tests))]
mod durability_lifecycle_private {
    include!(env!("CELLAR_CONFORMANCE_MAIN_DURABILITY_TESTS"));
}
