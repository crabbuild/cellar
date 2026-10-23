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
    pub max_invocations: usize,
    pub max_objects: usize,
    pub max_pending_operations: usize,
    pub max_operations: usize,
    pub max_value_bytes: usize,
    pub max_body_bytes: usize,
    pub max_batch_operations: usize,
    pub max_bridge_bytes: usize,
    pub heap_bytes: usize,
    /// Startup compilation and module initialization budget.
    pub startup_timeout: Duration,
    /// Maximum uninterrupted engine turn, including microtask checkpoints.
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

pub(crate) type Life = Arc<AtomicBool>;

pub(crate) struct Start {
    pub id: u64,
    pub payload: String,
    pub active: Life,
    pub owner: Life,
    pub sender: channel::Sender<Event>,
    pub _bytes: OwnedSemaphorePermit,
}

pub(crate) enum Event {
    Operation {
        id: u64,
        name: String,
        payload: String,
        _bytes: OwnedSemaphorePermit,
    },
    Complete {
        payload: String,
        failed: bool,
        _bytes: OwnedSemaphorePermit,
    },
}

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
        self.next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                (v < 9_007_199_254_740_991).then_some(v + 1)
            })
            .map_err(|_| anyhow::anyhow!("JavaScript identity space exhausted"))
    }

    pub fn reserve(&self, n: usize) -> Result<OwnedSemaphorePermit> {
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

    pub fn reply(&self, invocation: u64, id: u64, payload: String) -> Result<()> {
        let bytes = self.reserve(payload.len())?;
        self.replies
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

/// Process bootstrap. Every call owns a joined engine and watchdog. V8's global
/// platform is initialized once and retained for process lifetime.
#[cfg(feature = "embed")]
pub struct JavaScriptRuntime;

#[cfg(feature = "embed")]
impl JavaScriptRuntime {
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
