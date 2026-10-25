// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Own and join the engine threads outside the node execution domain.
#![allow(clippy::disallowed_methods)]
use crate::{JavaScriptApplication, Source};
use anyhow::{ensure, Result};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc, Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc as channel, watch, OwnedSemaphorePermit, Semaphore};
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
    /// Shared budget for retained bridge payloads; also bounds Node transport queues.
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

/// Revocable authority shared by the native invocation and the engine thread.
pub(crate) type Life = Arc<AtomicBool>;

/// Admission message. The byte permit follows the payload until it is dropped.
pub(crate) struct Start {
    pub id: u64,
    pub payload: String,
    pub active: Life,
    pub owner: Life,
    pub sender: channel::Sender<Event>,
    pub _bytes: OwnedSemaphorePermit,
}

/// Engine-to-host messages; SDK effects execute on the receiving native future.
pub(crate) enum Event {
    Operation {
        id: u64,
        name: String,
        payload: String,
        callback_scope: Option<u64>,
        // Embedded synchronous calls have a private reply slot. Node routes
        // its replies through the existing authenticated transport.
        synchronous: Option<mpsc::SyncSender<Reply>>,
        _bytes: OwnedSemaphorePermit,
    },
    Complete {
        payload: String,
        failed: bool,
        _bytes: OwnedSemaphorePermit,
    },
}

/// Encoded SDK outcome returned to one operation within one invocation.
pub(crate) struct Reply {
    pub invocation: u64,
    pub id: u64,
    pub payload: String,
    pub _bytes: OwnedSemaphorePermit,
}

pub(crate) struct Runtime {
    pub options: Options,
    pub permits: Arc<Semaphore>,
    pub bytes: Arc<Semaphore>,
    pub closed: AtomicBool,
    pub stopped: watch::Sender<bool>,
    next: AtomicU64,
    starts: mpsc::SyncSender<Start>,
    replies: mpsc::SyncSender<Reply>,
}

impl Runtime {
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
        self.starts
            .try_send(start)
            .map_err(|_| anyhow::anyhow!("JavaScript admission exhausted"))
    }

    #[cfg(test)]
    pub fn reply(&self, invocation: u64, id: u64, payload: String) -> Result<()> {
        self.respond(invocation, id, payload, None)
    }

    pub fn respond(
        &self,
        invocation: u64,
        id: u64,
        payload: String,
        synchronous: Option<mpsc::SyncSender<Reply>>,
    ) -> Result<()> {
        let bytes = self.reserve(payload.len())?;
        synchronous
            .as_ref()
            .unwrap_or(&self.replies)
            .try_send(Reply {
                invocation,
                id,
                payload,
                _bytes: bytes,
            })
            .map_err(|_| anyhow::anyhow!("JavaScript reply capacity exhausted"))
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.stopped.send_replace(true);
    }
}

/// Synchronous compatibility calls run on the original native invocation. Keep
/// this allowlist local and finite: waiting on network/container I/O would block
/// the language event loop and could deadlock application-to-application calls.
pub(crate) fn supports_synchronous(name: &str) -> bool {
    cellar_guest::workers::supports_synchronous(name)
        || matches!(
            name,
            "clock.now" | "storage.read" | "storage.apply" | "storage.sql" | "alarm.get"
        )
}

// Completion may wake the next native transaction before its reply is observed
// by JS. Native callback guards/queues serialize effects; this control wait must
// not occupy the JS mutation slot. Failure classification still treats finish
// as a mutation because its commit acknowledgement can be lost.
pub(crate) fn requires_mutation_slot(name: &str) -> bool {
    name != "workers.callback.finish" && !cellar_guest::operations::is_read(name)
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
        run_with(options, source, host, engine::run)
    }
}

struct Close(Arc<Runtime>);

impl Drop for Close {
    fn drop(&mut self) {
        self.0.close();
    }
}

fn run_with<T>(
    options: Options,
    source: Source,
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
    source.validate()?;
    ensure!(
        (1..=4096).contains(&options.max_invocations)
            && (1..=1_000_000).contains(&options.max_objects),
        "invalid JavaScript admission limits"
    );
    ensure!(
        options.max_pending_operations > 0
            && options.max_pending_operations <= 1024
            && options.max_operations > 0,
        "invalid JavaScript operation limits"
    );
    ensure!(
        options.max_value_bytes >= 128
            && options.max_value_bytes <= options.max_bridge_bytes
            && options.max_bridge_bytes <= u32::MAX as usize,
        "invalid JavaScript byte limits"
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
        stopped: watch::channel(false).0,
        next: AtomicU64::new(1),
        starts,
        replies,
    });
    let (ready, startup) = mpsc::sync_channel(1);
    let worker_runtime = runtime.clone();
    let worker_source = source.clone();
    std::thread::scope(|threads| {
        let worker = threads.spawn(move || {
            let _close = Close(worker_runtime.clone());
            engine(
                worker_runtime.clone(),
                worker_source,
                receive,
                results,
                ready,
            )
        });

        let close = Close(runtime.clone());
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

#[cfg(all(test, feature = "embed"))]
#[path = "../runtime_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../synchronous_runtime_tests.rs"]
mod synchronous_tests;
