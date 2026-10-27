// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Own and join the engine threads outside the node execution domain.
#![allow(clippy::disallowed_methods)]
use crate::{JavaScriptApplication, Source};
use anyhow::{ensure, Result};
use std::cell::Cell;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
    mpsc, Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc as channel, watch, OwnedSemaphorePermit, Semaphore};
#[cfg(feature = "embed")]
mod binary;
mod budgets;
#[cfg(feature = "embed")]
mod encoding;
#[cfg(feature = "embed")]
mod engine;
#[cfg(feature = "node")]
pub mod node;

/// Finite admission, value and execution limits. Heap exhaustion can still be
/// process-fatal in V8; production binaries require an external supervisor.
#[derive(Clone, Debug)]
pub struct Options {
    /// Maximum admitted invocations across this runtime, including pending I/O.
    pub max_invocations: usize,
    /// Maximum retained JavaScript cell objects across live native activations.
    pub max_objects: usize,
    /// Embedded V8 native text-decoder handles, reclaimed with their JS owners.
    /// Separate from cell-object admission. Node uses its own encoding globals.
    pub max_text_decoders: usize,
    /// Maximum outstanding SDK operations per invocation.
    pub max_pending_operations: usize,
    /// Total SDK operation budget for one invocation, including completed calls.
    pub max_operations: usize,
    /// Maximum encoded value size for an invocation, operation, reply, or result.
    pub max_value_bytes: usize,
    /// Buffered body, individual stream chunk and WebSocket frame byte limit.
    pub max_body_bytes: usize,
    /// Maximum storage batch operations or live transport capabilities per invocation.
    pub max_batch_operations: usize,
    /// Shared budget for retained native bridge payloads. Node framing, partial
    /// input and decoded attachments use this same pool. V8 reserves 128 bytes
    /// per admitted completion; startup requires max_value_bytes + 128 bytes.
    /// Node startup requires
    /// enough capacity for one maximum value and its overlapping framed copies.
    pub max_bridge_bytes: usize,
    /// V8 heap limit, or Node old-space limit. This does not cap process RSS.
    pub heap_bytes: usize,
    /// Startup compilation and module initialization budget.
    pub startup_timeout: Duration,
    /// Embedded V8 pump budget, including its batch of calls and microtasks.
    /// Node uses `NodeOptions::heartbeat_timeout` for event-loop liveness.
    pub turn_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_invocations: 64,
            max_objects: 4096,
            max_text_decoders: 4096,
            max_pending_operations: 16,
            max_operations: 1024,
            max_value_bytes: 2 * 1024 * 1024,
            max_body_bytes: 1024 * 1024,
            max_batch_operations: 128,
            max_bridge_bytes: 32 * 1024 * 1024,
            heap_bytes: 128 * 1024 * 1024,
            startup_timeout: Duration::from_secs(5),
            turn_timeout: Duration::from_millis(100),
        }
    }
}

/// Native bridge reservations observed at one point in time. Fields are read
/// independently; this is diagnostic data, not an atomic admission decision.
/// Engine heap, process RSS and cell-object counts are outside these gauges.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct BridgeUsage {
    /// Whether the engine has revoked admission and notified native waiters.
    pub stopped: bool,
    /// Native invocations still holding an admission permit.
    pub active_invocations: usize,
    /// Native invocation, operation and reply payload reservations, including
    /// Node's queued/partial frames, decoded payloads and V8 completion reserves.
    pub retained_bytes: usize,
}

/// Revocable authority shared by the native invocation and the engine thread.
pub(crate) type Life = Arc<AtomicBool>;

/// Admission message. The byte permit follows the payload until it is dropped.
pub(crate) struct Start {
    pub budget_source: Option<cellar::sdk::rpc::BudgetSource>,
    pub id: u64,
    pub payload: String,
    pub body: Option<Vec<u8>>,
    pub active: Life,
    pub owner: Life,
    pub sender: channel::Sender<Event>,
    pub _bytes: OwnedSemaphorePermit,
    // One engine thread takes this once when publishing the terminal result.
    // Cell moves with this owned message; no shared cross-thread access occurs.
    pub _completion: Cell<Option<OwnedSemaphorePermit>>,
}

/// Engine-to-host messages; SDK effects execute on the receiving native future.
pub(crate) enum Event {
    Operation {
        id: u64,
        name: String,
        payload: String,
        callback_scope: Option<u64>,
        blocking: bool,
        // Embedded synchronous calls have a private reply slot. Node routes
        // its replies through the existing authenticated transport.
        synchronous: Option<mpsc::SyncSender<Reply>>,
        body: Option<Vec<u8>>,
        signal: Option<cellar::sdk::web::AbortSignal>,
        budget: Option<cellar::sdk::rpc::CallBudget>,
        _bytes: OwnedSemaphorePermit,
    },
    Complete {
        payload: String,
        body: Option<Vec<u8>>,
        failed: bool,
        _bytes: OwnedSemaphorePermit,
    },
}

/// Encoded SDK outcome returned to one operation within one invocation.
pub(crate) struct Reply {
    pub invocation: u64,
    pub id: u64,
    pub payload: String,
    pub body: Option<Vec<u8>>,
    pub _bytes: OwnedSemaphorePermit,
}

#[derive(Clone, Copy)]
enum StopReason {
    Shutdown = 1,
    Failure = 2,
}

pub(crate) struct Runtime {
    pub options: Options,
    pub permits: Arc<Semaphore>,
    pub bytes: Arc<Semaphore>,
    pub closed: AtomicBool,
    // The first stop wins. A later host return cannot relabel a fatal timeout.
    stop_reason: AtomicU8,
    pub stopped: watch::Sender<bool>,
    // Observe the actual native attachment-copy boundary in saturation tests.
    #[cfg(all(test, feature = "embed"))]
    attachment_copied: AtomicU64,
    #[cfg(all(test, feature = "embed"))]
    completion_copied: AtomicU64,
    next: AtomicU64,
    starts: mpsc::SyncSender<Start>,
    replies: mpsc::SyncSender<Reply>,
    completion_bytes: usize,
}

impl Runtime {
    pub fn usage(&self) -> BridgeUsage {
        BridgeUsage {
            stopped: self.closed.load(Ordering::Acquire),
            active_invocations: self.options.max_invocations - self.permits.available_permits(),
            retained_bytes: self.options.max_bridge_bytes - self.bytes.available_permits(),
        }
    }

    pub fn id(&self) -> Result<u64> {
        // IDs cross JSON and JavaScript Number; never allocate an inexact integer.
        self.next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                (v < 9_007_199_254_740_991).then_some(v + 1)
            })
            .map_err(|_| anyhow::anyhow!("JavaScript identity space exhausted"))
    }

    pub fn reserve(&self, n: usize) -> Result<OwnedSemaphorePermit> {
        // Refuse overload immediately instead of queuing unbounded payloads.
        ensure!(
            n <= self.options.max_value_bytes,
            "JavaScript value exceeds limit"
        );
        Ok(self
            .bytes
            .clone()
            .try_acquire_many_owned(u32::try_from(n)?)?)
    }

    pub fn start(&self, start: Start) -> Result<()> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            "JavaScript runtime stopped"
        );
        if self.completion_bytes != 0 {
            start
                ._completion
                .set(Some(self.reserve(self.completion_bytes)?));
        }
        self.starts
            .try_send(start)
            .map_err(|_| anyhow::anyhow!("JavaScript admission exhausted"))
    }

    #[cfg(test)]
    pub fn reply(&self, invocation: u64, id: u64, payload: String) -> Result<()> {
        self.reply_parts(invocation, id, (payload, None))
    }

    pub fn respond(
        &self,
        invocation: u64,
        id: u64,
        payload: String,
        synchronous: Option<mpsc::SyncSender<Reply>>,
    ) -> Result<()> {
        self.respond_parts(invocation, id, (payload, None), synchronous)
    }

    #[cfg(test)]
    pub fn reply_parts(
        &self,
        invocation: u64,
        id: u64,
        parts: (String, Option<Vec<u8>>),
    ) -> Result<()> {
        self.respond_parts(invocation, id, parts, None)
    }

    pub fn respond_parts(
        &self,
        invocation: u64,
        id: u64,
        (payload, body): (String, Option<Vec<u8>>),
        synchronous: Option<mpsc::SyncSender<Reply>>,
    ) -> Result<()> {
        ensure!(
            synchronous.is_none() || body.is_none(),
            "synchronous replies cannot carry attachments"
        );
        let bytes = self.reserve(value_size(&payload, body.as_deref())?)?;
        synchronous
            .as_ref()
            .unwrap_or(&self.replies)
            .try_send(Reply {
                invocation,
                id,
                payload,
                body,
                _bytes: bytes,
            })
            .map_err(|_| anyhow::anyhow!("JavaScript reply capacity exhausted"))
    }

    fn record_stop(&self, reason: StopReason) {
        let _ =
            self.stop_reason
                .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire);
    }

    fn stop(&self, reason: StopReason) {
        self.record_stop(reason);
        self.closed.store(true, Ordering::Release);
        self.stopped.send_replace(true);
    }

    #[cfg(feature = "embed")]
    fn close(&self) {
        self.stop(StopReason::Failure);
    }

    #[cfg(feature = "embed")]
    fn shutdown_requested(&self) -> bool {
        self.stop_reason.load(Ordering::Acquire) == StopReason::Shutdown as u8
    }
}

/// Synchronous compatibility calls run on the original native invocation. Keep
/// this allowlist local and finite: waiting on network/container I/O would block
/// the language event loop and could deadlock application-to-application calls.
pub(crate) fn supports_synchronous(name: &str) -> bool {
    cellar_guest::workers::supports_synchronous(name)
        || matches!(
            name,
            "clock.now"
                | "storage.read"
                | "storage.apply"
                | "storage.sql"
                | "alarm.get"
                | "workers.callback.start_sync"
                | "workers.callback.finish_sync"
                | "workers.rpc.dup"
        )
}

// Completion may wake the next native transaction before its reply is observed
// by JS. Native callback guards/queues serialize effects; this control wait must
// not occupy the JS mutation slot. Failure classification still treats finish
// as a mutation because its commit acknowledgement can be lost. Workers routed
// calls also have independent native futures/output gates. They may overlap,
// without being classified as reads or taking the local storage mutation slot.
// Body output may await consumer backpressure while independent waitUntil work
// writes storage. Its native gate proves earlier writes; it does not own the
// cell's mutation slot. Storage sync is likewise an output barrier, not a
// SQLite mutation; parallel frame proofs must not refuse one another. Output
// failures retain uncertain effect provenance. Container WebSocket output is
// likewise independent: a received frame may trigger a storage write before
// its matching send acknowledgement arrives. Native socket output gates remain.
// Existing process I/O and resource retirement likewise use the provider's
// control queue and output proofs, independently of SQLite mutation admission.
pub(crate) fn requires_mutation_slot(name: &str) -> bool {
    !name.starts_with("workers.rpc.")
        && !matches!(
            name,
            "storage.sync"
                | "container.exec.write"
                | "container.exec.close_stdin"
                | "container.exec.kill"
                | "container.release"
                | "container.websocket.send_text"
                | "container.websocket.send_binary"
                | "container.websocket.close"
                | "websocket.send"
                | "websocket.close"
                | "response.write"
                | "response.finish"
                | "workers.callback.finish"
                | "workers.callback.finish_sync"
                | "workers.container.perform"
                | "workers.rpc"
                | "workers.fetch"
                | "workers.fetch_stream"
                | "workers.container.fetch"
                | "workers.container.fetch_stream"
        )
        && !cellar_guest::operations::is_read(name)
}

pub(crate) fn supports_operation_cancellation(name: &str) -> bool {
    matches!(
        name,
        "response.read"
            | "workers.rpc.invoke"
            | "response.closed"
            | "websocket.next"
            | "websocket.closed"
            | "cell.call"
            | "container.port.fetch"
            | "container.port.fetch_stream"
            | "container.response.read"
            | "container.exec.read_stdout"
            | "container.exec.read_stderr"
            | "container.exec.wait"
            | "container.port.connect_websocket"
            | "container.websocket.receive"
            | "workers.fetch"
            | "workers.fetch_stream"
            | "workers.container.fetch"
            | "workers.container.fetch_stream"
    )
}

/// Process bootstrap. Every call owns a joined engine and watchdog. V8's global
/// platform is initialized once and retained for process lifetime.
#[cfg(feature = "embed")]
pub struct JavaScriptRuntime;

#[cfg(feature = "embed")]
impl JavaScriptRuntime {
    /// Initialize V8 and validate registrations before calling `host`.
    /// Returning from `host` stops and joins the engine before this call returns.
    pub fn run<T>(
        options: Options,
        source: Source,
        host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
    ) -> Result<T> {
        run_with(options, source, EngineBackend::Embedded, host, engine::run)
    }
}

struct Close(Arc<Runtime>, StopReason);

// A finite native result allowance remains reserved while an embedded handler
// runs. Node owns its framed transport admission independently.
const COMPLETION_BYTES: usize = 128;

enum EngineBackend {
    #[cfg(feature = "embed")]
    Embedded,
    #[cfg(feature = "node")]
    Node,
}

impl EngineBackend {
    fn completion_bytes(self) -> usize {
        match self {
            #[cfg(feature = "embed")]
            Self::Embedded => COMPLETION_BYTES,
            #[cfg(feature = "node")]
            Self::Node => 0,
        }
    }
}

impl Drop for Close {
    fn drop(&mut self) {
        self.0.stop(self.1);
    }
}

fn run_with<T>(
    options: Options,
    source: Source,
    backend: EngineBackend,
    host: impl FnOnce(Arc<JavaScriptApplication>) -> Result<T>,
    engine: impl FnOnce(
            Arc<Runtime>,
            Source,
            mpsc::Receiver<Start>,
            mpsc::Receiver<Reply>,
            mpsc::SyncSender<Result<()>>,
        ) -> Result<()>
        + Send,
) -> Result<T> {
    let completion_bytes = backend.completion_bytes();
    source.validate()?;
    source.rpc.validate_bridge(options.max_value_bytes)?;
    ensure!(
        (1..=4096).contains(&options.max_invocations)
            && (1..=1_000_000).contains(&options.max_objects)
            && (1..=65_536).contains(&options.max_text_decoders),
        "invalid JavaScript admission limits"
    );
    ensure!(
        options.max_pending_operations > 0
            && options.max_pending_operations <= 1024
            && options.max_operations > 0,
        "invalid JavaScript operation limits"
    );
    ensure!(
        options.max_value_bytes >= COMPLETION_BYTES
            && options.max_value_bytes <= options.max_bridge_bytes
            && options.max_bridge_bytes <= u32::MAX as usize,
        "invalid JavaScript byte limits"
    );
    ensure!(
        options
            .max_value_bytes
            .checked_add(completion_bytes)
            .is_some_and(|bytes| bytes <= options.max_bridge_bytes),
        "JavaScript bridge byte budget cannot fit a maximum value and its completion"
    );
    ensure!(
        options.heap_bytes >= 8 * 1024 * 1024
            && !options.startup_timeout.is_zero()
            && options.startup_timeout <= Duration::from_secs(60)
            && !options.turn_timeout.is_zero()
            && options.turn_timeout <= Duration::from_secs(60),
        "invalid JavaScript execution limits"
    );
    let (starts, receive) = mpsc::sync_channel(options.max_invocations);
    // Separate capacity for replies: starts can never starve completions.
    let (replies, results) =
        mpsc::sync_channel(options.max_invocations * options.max_pending_operations);
    let runtime = Arc::new(Runtime {
        permits: Arc::new(Semaphore::new(options.max_invocations)),
        bytes: Arc::new(Semaphore::new(options.max_bridge_bytes)),
        options,
        closed: AtomicBool::new(false),
        stop_reason: AtomicU8::new(0),
        stopped: watch::channel(false).0,
        #[cfg(all(test, feature = "embed"))]
        attachment_copied: AtomicU64::new(0),
        #[cfg(all(test, feature = "embed"))]
        completion_copied: AtomicU64::new(0),
        next: AtomicU64::new(1),
        starts,
        replies,
        completion_bytes,
    });
    let (ready, startup) = mpsc::sync_channel(1);
    let worker_runtime = runtime.clone();
    let worker_source = source.clone();
    std::thread::scope(|threads| {
        let worker = threads.spawn(move || {
            let _close = Close(worker_runtime.clone(), StopReason::Failure);
            engine(
                worker_runtime.clone(),
                worker_source,
                receive,
                results,
                ready,
            )
        });

        let close = Close(runtime.clone(), StopReason::Shutdown);
        let result = match startup.recv() {
            Ok(Ok(())) => host(Arc::new(JavaScriptApplication::new(runtime, source))),
            _ => Err(anyhow::anyhow!(
                "JavaScript application initialization failed"
            )),
        };
        // Signal shutdown before joining, including when host startup failed.
        drop(close);
        let engine = worker
            .join()
            .map_err(|_| anyhow::anyhow!("JavaScript engine thread panicked"))?;
        // An explicit host stop may interrupt dispatch or a microtask checkpoint.
        // Preserve the host's result only for that typed cause; budget expiry,
        // ordinary engine errors and panics still fail bootstrap.
        #[cfg(feature = "embed")]
        let engine = match engine {
            Err(error) if error.is::<engine::RequestedShutdown>() => Ok(()),
            result => result,
        };
        match (result, engine) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(engine)) => {
                Err(error.context(format!("JavaScript runtime also failed: {engine}")))
            }
        }
    })
}

/// Charge control metadata and exact owned bytes, with checked arithmetic.
pub(crate) fn value_size(payload: &str, body: Option<&[u8]>) -> Result<usize> {
    payload
        .len()
        .checked_add(body.map_or(0, <[u8]>::len))
        .ok_or_else(|| anyhow::anyhow!("JavaScript value size overflow"))
}

#[cfg(all(test, feature = "embed"))]
#[path = "../runtime_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../synchronous_runtime_tests.rs"]
mod synchronous_tests;

#[cfg(test)]
mod attachment_tests;

#[cfg(test)]
mod cancellation_tests;
