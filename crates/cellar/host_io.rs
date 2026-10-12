// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Invocation-local host state shared by Rust applications and engine adapters.

use crate::host_channels::CallChains;
use crate::http_streams::retain_first_http_cleanup_panic;
use crate::http_streams::{
    claim_http_stream, HttpStreamClaim, HttpStreamService, RequestBodyGuard,
};
use crate::WorkerInvocationLimits as ResourceLimits;
use crate::{asyncrt, storage};
use cellar_native::event_context::EventFrames;
use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

pub(crate) mod input_gate_lifecycle;
use input_gate_lifecycle::CrossEntryGateClaims;

pub(crate) type ArmGateRx = tokio::sync::oneshot::Receiver<Result<(), String>>;
pub(crate) type BackgroundRegistration = Box<dyn Any + Send>;

#[derive(Clone, Copy)]
pub(crate) struct ContextCleanup {
    pub retire_input_gates: fn(&IoContext),
    pub close_websockets: fn(&IoContext, Vec<u64>),
    pub close_tcp: fn(u64),
}

struct ContextRegistration {
    id: u64,
    cleanup: Option<Box<dyn FnOnce(u64) + Send + Sync>>,
}

impl Drop for ContextRegistration {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup(self.id);
        }
    }
}

/// Weak continuation lookup for one worker. A registry never owns an event.
#[derive(Default)]
pub(crate) struct ContextRegistry(Mutex<HashMap<u64, Weak<IoContext>>>);

impl ContextRegistry {
    pub(crate) fn register(self: &Arc<Self>, context: &mut Arc<IoContext>) -> u64 {
        let id = allocate_io_context_id();
        let registry = Arc::downgrade(self);
        Arc::get_mut(context)
            .expect("new context is uniquely owned")
            .set_registration(id, move |id| {
                if let Some(registry) = registry.upgrade() {
                    registry.0.lock().unwrap().remove(&id);
                }
            });
        self.0.lock().unwrap().insert(id, Arc::downgrade(context));
        id
    }

    pub(crate) fn lookup(&self, id: u64) -> Option<Arc<IoContext>> {
        let mut contexts = self.0.lock().unwrap();
        let context = contexts.get(&id).and_then(Weak::upgrade);
        if context.is_none() {
            contexts.remove(&id);
        }
        context
    }

    #[cfg(cellar_internal_tests)]
    pub(crate) fn contains(&self, id: u64) -> bool {
        self.0.lock().unwrap().contains_key(&id)
    }

    #[cfg(cellar_internal_tests)]
    pub(crate) fn is_empty(&self) -> bool {
        self.0.lock().unwrap().is_empty()
    }
}

#[cfg(unix)]
pub(crate) fn clock_nanos(clock: libc::clockid_t) -> Option<u64> {
    // SAFETY: `clock_gettime` initializes the complete `timespec` on success,
    // and the pointer remains valid for the call.
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(clock, time.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: The successful call above initialized `time`.
    let time = unsafe { time.assume_init() };
    let seconds = u64::try_from(time.tv_sec).ok()?;
    let nanos = u64::try_from(time.tv_nsec).ok()?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[cfg(unix)]
pub(crate) fn platform_thread_cpu_nanos() -> Option<u64> {
    clock_nanos(libc::CLOCK_THREAD_CPUTIME_ID)
}

#[cfg(not(unix))]
pub(crate) fn platform_thread_cpu_nanos() -> Option<u64> {
    None
}

struct CpuTurnStart {
    thread_nanos: Option<u64>,
    wall: Instant,
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TailLog {
    timestamp: i64,
    level: String,
    message: Vec<String>,
}

pub(crate) const TAIL_LOG_LIMIT_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct TailLogCapture {
    records: Vec<TailLog>,
    /// The serialized record bytes, including separators but excluding the
    /// two array delimiters. The records and this measure share one lock, so
    /// no caller can retain a record without charging it to the same budget.
    serialized_bytes: usize,
    /// A record that crosses the limit ends capture for this invocation.
    /// Continuing after that record would expose later context that Workerd
    /// omits, even when a smaller record could fit in the remaining budget.
    full: bool,
}

#[derive(Default)]
struct JsonByteCounter(usize);

impl std::io::Write for JsonByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialized_json_bytes(value: &impl serde::Serialize) -> usize {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, value).expect("TailLog serialization is infallible");
    counter.0
}

#[derive(Debug)]
pub(crate) struct RedirectSubrequestLimit(pub(crate) String);

impl std::fmt::Display for RedirectSubrequestLimit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RedirectSubrequestLimit {}

/// What one cell event's outbound effects gate against.
#[derive(Clone)]
pub(crate) struct EgressFrame {
    /// The scope whose connection records this event's writes: the cell for an
    /// ordinary event, and the facet for an event of an embedded facet.
    pub(crate) storage: String,
    /// The committed-write position of `storage` when the event started.
    pub(crate) before: u64,
    /// The root cell's gate, when `storage` names an embedded facet. Kept
    /// beside the scope it stores under, so an effect cannot take one without
    /// the other and charge a facet's egress to the facet.
    pub(crate) root: Option<storage::RootGate>,
}

// Cell gates are process-wide, so their owner ids must not repeat in another
// isolate. A per-isolate sequence can make one event appear to own another
// isolate's gate and keep the wrong event alive.
static NEXT_IO_CONTEXT_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) fn allocate_io_context_id() -> u64 {
    NEXT_IO_CONTEXT_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .unwrap_or_else(|_| panic!("the process exhausted its IoContext ids"))
        + 1
}

/// The writable directory subset of Workerd's per-request virtual filesystem.
///
/// The first compatibility slice stores directories only. Keeping the tree on
/// `IoContext` is the isolation mechanism: async continuations for one request
/// share it, while a concurrent or later request receives a different tree.
/// No path can escape into the host filesystem.
pub(crate) struct RequestVfs {
    nodes: HashMap<String, VfsNode>,
}

/// Keep the node kind in the tree, even while the first slice can only create
/// directories. Deno's VFS uses the same entry-typed shape; a path-only set
/// would make a later file node indistinguishable from its parent directory.
enum VfsNode {
    Directory { writable: bool },
}

pub(crate) enum VfsMkdirResult {
    Created(Option<String>),
    Error(&'static str),
}

impl Default for RequestVfs {
    fn default() -> Self {
        Self {
            nodes: [
                ("/", false),
                ("/bundle", false),
                ("/dev", false),
                ("/tmp", true),
            ]
            .into_iter()
            .map(|(path, writable)| (path.to_string(), VfsNode::Directory { writable }))
            .collect(),
        }
    }
}

pub(crate) struct InputGateClaims {
    /// Set before the turn owner releases its final `IoContext` owner. The
    /// watch wakes its pending gate acquisition even when the promise
    /// reaction that requested the gate belongs to another event.
    pub(crate) retired: bool,
    pub(crate) held: HashMap<cellar_logic::gate::EventId, String>,
    pub(crate) retirement: tokio::sync::watch::Sender<bool>,
}

impl Default for InputGateClaims {
    fn default() -> Self {
        let (retirement, _) = tokio::sync::watch::channel(false);
        Self {
            retired: false,
            held: HashMap::new(),
            retirement,
        }
    }
}

impl RequestVfs {
    pub(crate) fn stat(&self, path: &str) -> Option<bool> {
        self.nodes.get(path).map(|node| match node {
            VfsNode::Directory { writable } => *writable,
        })
    }

    pub(crate) fn mkdir(&mut self, path: &str, recursive: bool) -> VfsMkdirResult {
        if path != "/tmp" && !path.starts_with("/tmp/") {
            return VfsMkdirResult::Error("EPERM");
        }
        if self.stat(path).is_some() {
            return VfsMkdirResult::Created(None);
        }
        if recursive {
            let mut current = "/tmp".to_string();
            let mut first_created = None;
            for segment in path.trim_start_matches("/tmp/").split('/') {
                current.push('/');
                current.push_str(segment);
                if self
                    .nodes
                    .insert(current.clone(), VfsNode::Directory { writable: true })
                    .is_none()
                    && first_created.is_none()
                {
                    first_created = Some(current.clone());
                }
            }
            return VfsMkdirResult::Created(first_created);
        }
        let parent =
            path.rsplit_once('/').map_or(
                "/",
                |(parent, _)| {
                    if parent.is_empty() {
                        "/"
                    } else {
                        parent
                    }
                },
            );
        if self.stat(parent).is_none() {
            return VfsMkdirResult::Error("ENOENT");
        }
        self.nodes
            .insert(path.to_string(), VfsNode::Directory { writable: true });
        VfsMkdirResult::Created(None)
    }
}

/// Shared owner of an in-flight invocation's host resources.
///
/// Request bodies, gates, delivery chains, budgets and temporary directories
/// follow this allocation across host turns. Engines may attach opaque state
/// and cleanup callbacks; the host never interprets engine handles. A weak
/// registry registration attributes continuations without retaining the event.
pub struct IoContext {
    pub(crate) native_activation: Option<Arc<crate::application::Activation>>,
    registration: Option<ContextRegistration>,
    engine: OnceLock<Box<dyn Any + Send + Sync>>,
    cleanup: OnceLock<ContextCleanup>,
    cpu_clock: OnceLock<fn() -> Option<u64>>,
    /// This caller's delivery order for each cell it has called. See
    /// `CallOrder` — it is here rather than in a process-wide map because
    /// nothing outside this caller ever reads it.
    pub call_chains: CallChains,
    /// Event frames, innermost last. A frame collects the `waitUntil`
    /// promises registered while it is the innermost one.
    ///
    /// Nesting is per-request and strictly LIFO — a service-binding
    /// dispatch, an RPC entry, or a DO construction pushes its own frame so
    /// its `waitUntil` binds to that dispatch, matching workerd.
    events: Mutex<EventFrames<BackgroundRegistration, ArmGateRx>>,
    /// Workerd gives every request an empty, memory-backed `/tmp`. Directory
    /// state belongs here so it follows the request across promise turns and
    /// disappears with the request instead of leaking across isolate reuse.
    /// The tree stays unallocated when the application does not use `node:fs`.
    pub(crate) vfs: OnceLock<Mutex<RequestVfs>>,
    /// Isolate-polled sockets this request opened. Dropping the request's
    /// pending ops aborts everything the isolate is waiting on, but a socket
    /// is a host-side resource that abort cannot reach: its connector task
    /// exits only when the isolate stops reading. So the request closes its
    /// own sockets, and [`IoContext::drop`] is what makes that unforgettable.
    ///
    /// Flat rather than a stack. Only sockets that belong to no cell land
    /// here, and one `IoContext` is one request, so there is nothing to nest:
    /// a service-binding dispatch or a Durable Object call gets a context of
    /// its own rather than a frame inside this one.
    pub(crate) sockets: Mutex<Vec<u64>>,
    /// HTMLRewriter instances this request created. Each holds a parked
    /// parser thread; freeing them at retirement is what reclaims the
    /// thread when a transform is abandoned unread — nothing else can,
    /// because the pending event promise roots the output stream.
    pub(crate) rewriters: Mutex<Vec<u64>>,
    /// Outbound TCP sockets this event opened. A socket cannot outlive
    /// its event: retirement drops the registry entries, which closes
    /// the connections.
    pub(crate) tcp_sockets: Mutex<Vec<u64>>,
    /// Input-gate holds and pending acquisitions for this event.
    ///
    /// The event owns the cell and host id together, and retirement wakes a
    /// pending acquisition. A timeout or cancellation can therefore abandon
    /// only its own hold and cannot resume after its event has gone away.
    pub(crate) input_gates: Mutex<InputGateClaims>,
    /// Blocks created by this event but run during another event's turn.
    /// These claims keep this event's native operations and resources alive;
    /// their watch wakes the drive when the final block ends.
    pub(crate) cross_entry_gates: CrossEntryGateClaims,
    /// Host-backed request bodies this handler owns. A subrequest transfers
    /// an id out before its target takes ownership, so only one request can
    /// reclaim an unread tail.
    pub(crate) body_streams: Mutex<HashMap<u64, HttpStreamClaim>>,
    /// What each running event's outbound effects gate against, innermost
    /// last. An outbound effect raised during the event consults this: if the
    /// handler has advanced the position, the effect waits for the output gate
    /// before it leaves the process.
    ///
    /// Empty for stateless Worker code, which owns no cell and gates
    /// nothing.
    pub(crate) egress: Mutex<Vec<EgressFrame>>,
    /// Ops another event's turn enqueued for this event, waiting for this
    /// event's driver to take them. See `adopt`: a continuation that resumes
    /// inside a foreign turn still enqueues through that turn, and only the
    /// driver of the event it belongs to may poll and cancel its work.
    ///
    /// `None` once this event retired: the mailbox is closed, and a hand-off
    /// that arrives after that is refused rather than parked. A parked op no
    /// driver will ever take is a `Global<PromiseResolver>` held for the life
    /// of the isolate, which is the leak `InFlight::abandon` exists to
    /// prevent; refusing lets the handing turn drop the resolver instead,
    /// while it still owns the isolate.
    pub(crate) handed: Mutex<Option<Vec<HandedOp>>>,
    /// How many ops `handed` holds, readable without taking its lock.
    ///
    /// Every pass of a driver's wake loop asks whether a foreign turn left it
    /// an op, and for all but a few passes in a cell's life the answer is no.
    /// The lock is uncontended, so this saves only the lock itself, but it
    /// pays that back on a loop that turns once per operation the event
    /// completes.
    handed_len: AtomicUsize,
    /// Wakes this event's driver when `handed` grows or a pending entrypoint
    /// starts. Both changes can make the driver's current wait obsolete.
    handed_wake: tokio::sync::Notify,
    /// IDs of nested `WorkerEntrypoint` calls that have not settled. The
    /// driver consumes these IDs only when the request has no native work, so
    /// it can reject the calls instead of timing out the enclosing event.
    pending_events: Mutex<HashSet<u64>>,
    /// The invocation-local subrequest budget. The count lives with the
    /// request context, so two concurrent calls into one loaded isolate never
    /// spend one another's allowance.
    subrequest_limit: Option<u32>,
    subrequests: AtomicUsize,
    /// A request-scoped client counts redirects against this request. A weak
    /// capture avoids a client/context cycle, and one client retains the
    /// connection pool across all fetches in the invocation.
    redirect_client: OnceLock<reqwest::Client>,
    cpu_limit_nanos: Option<u64>,
    cpu_used_nanos: AtomicU64,
    cpu_turn: Mutex<Option<CpuTurnStart>>,
    /// Whether this event records console calls for a Dynamic Worker tail.
    tail_reporting: bool,
    tail_logs: Mutex<TailLogCapture>,
}

/// An op enqueued by one event's continuation during another event's turn: the
/// promise id, the work, and its complete lifetime classification.
pub(crate) type HandedOp = (u64, asyncrt::OpFuture, asyncrt::OpLifetime);

impl IoContext {
    pub(crate) fn install_cpu_clock(&self, clock: fn() -> Option<u64>) {
        let _ = self.cpu_clock.set(clock);
    }

    fn sample_cpu_clock(&self) -> Option<u64> {
        self.cpu_clock
            .get()
            .copied()
            .unwrap_or(platform_thread_cpu_nanos)()
    }

    pub(crate) fn engine_state<T: Any + Send + Sync + Default>(&self) -> &T {
        self.engine
            .get_or_init(|| Box::<T>::default())
            .downcast_ref()
            .expect("an invocation has one engine attachment type")
    }

    pub(crate) fn set_registration(
        &mut self,
        id: u64,
        cleanup: impl FnOnce(u64) + Send + Sync + 'static,
    ) {
        assert!(
            self.registration.is_none(),
            "an invocation was registered twice"
        );
        self.registration = Some(ContextRegistration {
            id,
            cleanup: Some(Box::new(cleanup)),
        });
    }

    pub(crate) fn record_tail_log(&self, level: &str, body: &str) {
        if !self.tail_reporting {
            return;
        }
        let mut capture = self.tail_logs.lock().unwrap();
        if capture.full {
            return;
        }
        let separator = usize::from(!capture.records.is_empty());
        // A serialized string cannot be shorter than its UTF-8 input. This
        // rejects an obviously oversized call without allocating another
        // buffer or scanning it for JSON escapes.
        let minimum_bytes = capture
            .serialized_bytes
            .saturating_add(separator)
            .saturating_add(body.len())
            .saturating_add(2);
        if minimum_bytes > TAIL_LOG_LIMIT_BYTES {
            capture.full = true;
            return;
        }
        let record = TailLog {
            timestamp: crate::telemetry::now_unix_us() / 1_000,
            level: level.to_string(),
            message: vec![body.to_string()],
        };
        let record_bytes = serialized_json_bytes(&record);
        let next_bytes = capture
            .serialized_bytes
            .saturating_add(separator)
            .saturating_add(record_bytes);
        if next_bytes.saturating_add(2) > TAIL_LOG_LIMIT_BYTES {
            capture.full = true;
            return;
        }
        capture.serialized_bytes = next_bytes;
        capture.records.push(record);
    }

    pub(crate) fn take_tail_logs(&self) -> Vec<TailLog> {
        std::mem::take(&mut *self.tail_logs.lock().unwrap()).records
    }

    /// The continuation id this context is registered under, which names
    /// the event to the cell's gate. `None` for a context with no captured event.
    pub(crate) fn continuation_id(&self) -> Option<u64> {
        self.registration
            .as_ref()
            .map(|registration| registration.id)
    }

    pub(crate) fn cpu_limit_ms(&self) -> Option<u32> {
        self.cpu_limit_nanos
            .and_then(|limit| u32::try_from(limit / 1_000_000).ok())
    }

    pub(crate) fn redirect_counting_client(self: &Arc<Self>) -> reqwest::Client {
        self.redirect_client
            .get_or_init(|| {
                let context = Arc::downgrade(self);
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                        if attempt.previous().len() >= 10 {
                            return attempt.error("too many redirects");
                        }
                        if let Some(context) = context.upgrade() {
                            if let Err(error) = context.charge_subrequest() {
                                return attempt.error(RedirectSubrequestLimit(error));
                            }
                        }
                        attempt.follow()
                    }))
                    .build()
                    .expect("build the redirect-counting HTTP client")
            })
            .clone()
    }

    pub(crate) fn has_subrequest_limit(&self) -> bool {
        self.subrequest_limit.is_some()
    }

    /// Spend one subrequest before it leaves this invocation. Failed attempts
    /// count too, so catching an exhausted budget cannot restore allowance.
    pub(crate) fn charge_subrequest(&self) -> Result<(), String> {
        let Some(limit) = self.subrequest_limit else {
            return Ok(());
        };
        let used = self.subrequests.fetch_add(1, Ordering::Relaxed) + 1;
        if used > limit as usize {
            return Err(format!("Worker exceeded subrequest limit of {limit}"));
        }
        Ok(())
    }

    // CPU accounting measures the executing thread; an invocation's logical
    // event clock cannot measure synchronous work or the wall-clock fallback.
    #[allow(clippy::disallowed_methods)]
    pub(crate) fn begin_cpu_turn(&self) {
        if self.cpu_limit_nanos.is_none() {
            return;
        }
        *self.cpu_turn.lock().unwrap() = Some(CpuTurnStart {
            thread_nanos: self.sample_cpu_clock(),
            wall: Instant::now(),
        });
    }

    pub(crate) fn remaining_cpu_budget(&self) -> Option<Duration> {
        self.cpu_limit_nanos.map(|limit| {
            Duration::from_nanos(limit.saturating_sub(self.cpu_used_nanos.load(Ordering::Relaxed)))
        })
    }

    pub(crate) fn finish_cpu_turn(&self) -> Result<(), String> {
        let Some(limit) = self.cpu_limit_nanos else {
            return Ok(());
        };
        let Some(started) = self.cpu_turn.lock().unwrap().take() else {
            return Ok(());
        };
        let elapsed = started
            .thread_nanos
            .zip(self.sample_cpu_clock())
            .map(|(started, finished)| finished.saturating_sub(started))
            .unwrap_or_else(|| started.wall.elapsed().as_nanos() as u64);
        let used = self.cpu_used_nanos.fetch_add(elapsed, Ordering::Relaxed) + elapsed;
        if used > limit {
            return Err(format!(
                "Worker exceeded CPU limit of {} ms",
                limit / 1_000_000
            ));
        }
        Ok(())
    }

    #[allow(clippy::new_ret_no_self, clippy::new_without_default)]
    #[doc(hidden)]
    pub fn new() -> Arc<Self> {
        Self::with_resource_limits(None)
    }

    pub(crate) fn with_resource_limits(limits: Option<ResourceLimits>) -> Arc<Self> {
        Self::with_options(limits, false)
    }

    #[cfg(all(test, cellar_internal_tests))]
    pub(crate) fn with_tail_reporting(tail_reporting: bool) -> Arc<Self> {
        Self::with_options(None, tail_reporting)
    }

    pub(crate) fn with_options(limits: Option<ResourceLimits>, tail_reporting: bool) -> Arc<Self> {
        Arc::new(Self {
            registration: None,
            engine: OnceLock::new(),
            cleanup: OnceLock::from(ContextCleanup {
                retire_input_gates: |context| {
                    let _ = crate::host_gates::abandon_context_input_gates(context);
                },
                close_websockets: crate::host_gates::ws_close_request_sockets,
                close_tcp: crate::host_tcp::free,
            }),
            cpu_clock: OnceLock::new(),
            call_chains: CallChains::default(),
            events: Mutex::new(EventFrames::default()),
            vfs: OnceLock::new(),
            sockets: Mutex::new(Vec::new()),
            rewriters: Mutex::new(Vec::new()),
            tcp_sockets: Mutex::new(Vec::new()),
            input_gates: Mutex::new(InputGateClaims::default()),
            cross_entry_gates: CrossEntryGateClaims::default(),
            body_streams: Mutex::new(HashMap::new()),
            egress: Mutex::new(Vec::new()),
            handed: Mutex::new(Some(Vec::new())),
            handed_len: AtomicUsize::new(0),
            handed_wake: tokio::sync::Notify::new(),
            pending_events: Mutex::new(HashSet::new()),
            subrequest_limit: limits.and_then(|limits| limits.sub_requests),
            subrequests: AtomicUsize::new(0),
            redirect_client: OnceLock::new(),
            cpu_limit_nanos: limits
                .and_then(|limits| limits.cpu_ms)
                .map(|milliseconds| u64::from(milliseconds).saturating_mul(1_000_000)),
            cpu_used_nanos: AtomicU64::new(0),
            cpu_turn: Mutex::new(None),
            tail_reporting,
            native_activation: None,
            tail_logs: Mutex::new(TailLogCapture::default()),
        })
    }

    /// Take an op that another event's turn enqueued for this event.
    ///
    /// Reports whether the mailbox accepted it. A closed mailbox refuses, and
    /// the caller must then drop the op's resolver: this event will never run
    /// the continuation waiting on it.
    #[must_use]
    pub(crate) fn hand_op(
        &self,
        id: u64,
        future: asyncrt::OpFuture,
        lifetime: asyncrt::OpLifetime,
    ) -> bool {
        let mut handed = self.handed.lock().unwrap();
        let Some(queue) = handed.as_mut() else {
            return false;
        };
        queue.push((id, future, lifetime));
        // Published under the lock, so a reader that sees this count can take
        // the lock and find the op there.
        self.handed_len.store(queue.len(), Ordering::Release);
        drop(handed);
        self.handed_wake.notify_one();
        true
    }

    /// Whether a foreign turn has left this event an op, without locking.
    ///
    /// A `false` answer is authoritative for the driver that asks it. Only
    /// that driver empties the queue, so nothing it must see can appear and
    /// vanish behind this read; a hand-off that lands immediately after it
    /// wakes the driver through `handed_ready` instead.
    pub(crate) fn has_handed_ops(&self) -> bool {
        self.handed_len.load(Ordering::Acquire) != 0
    }

    pub(crate) fn accepts_handed_ops(&self) -> bool {
        self.handed.lock().unwrap().is_some()
    }

    pub(crate) fn register_pending_event(&self, id: u64) {
        self.pending_events.lock().unwrap().insert(id);
        // The call can start during another event's microtask checkpoint,
        // after this context's driver has already parked. Wake that driver so
        // it can reject the call when no native operation can resume it.
        self.handed_wake.notify_one();
    }

    pub(crate) fn finish_pending_event(&self, id: u64) {
        self.pending_events.lock().unwrap().remove(&id);
    }

    pub(crate) fn has_pending_events(&self) -> bool {
        !self.pending_events.lock().unwrap().is_empty()
    }

    pub(crate) fn take_pending_events(&self) -> Vec<u64> {
        let mut pending = self.pending_events.lock().unwrap();
        let mut ids = pending.drain().collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Resolve once another turn has handed this event an op or started a
    /// pending entrypoint call.
    ///
    /// This can also resolve with nothing to take, and a caller must treat
    /// an empty queue as ordinary. `notify_one` stores a permit when no
    /// driver is parked, and the driver takes the op on its next pass
    /// through `has_handed_ops` before it waits again, so the permit
    /// outlives the op it announced and releases one later wait early. That
    /// wait costs one atomic read and parks again, which is why the permit
    /// is left alone: cancelling it needs a second piece of state, and that
    /// state has to stay consistent with the queue.
    pub(crate) async fn handed_ready(&self) {
        self.handed_wake.notified().await;
    }

    /// Hand this event's driver everything a foreign turn left for it.
    pub(crate) fn take_handed_ops(&self) -> Vec<HandedOp> {
        let mut handed = self.handed.lock().unwrap();
        self.handed_len.store(0, Ordering::Release);
        handed.as_mut().map(std::mem::take).unwrap_or_default()
    }

    /// Close the mailbox and return whatever no driver took.
    ///
    /// Called when the event retires. The caller drops the resolvers of the
    /// ops that come back, and every later hand-off is refused at the source,
    /// so no op can sit here waiting for a driver that will never run again.
    #[must_use]
    pub(crate) fn close_handed_ops(&self) -> Vec<HandedOp> {
        let mut handed = self.handed.lock().unwrap();
        self.handed_len.store(0, Ordering::Release);
        handed.take().unwrap_or_default()
    }

    pub(crate) fn begin_event(&self) {
        self.events.lock().unwrap().begin();
    }

    pub(crate) fn release_input_gate(
        &self,
        cell: &str,
        event: cellar_logic::gate::EventId,
    ) -> bool {
        let mut claims = self.input_gates.lock().unwrap();
        if !claims.held.get(&event).is_some_and(|owned| owned == cell) {
            return false;
        }
        claims.held.remove(&event);
        true
    }

    pub(crate) fn retire_input_gates(&self) -> Vec<(String, cellar_logic::gate::EventId)> {
        let mut claims = self.input_gates.lock().unwrap();
        claims.retired = true;
        let retirement = claims.retirement.clone();
        let mut held = claims
            .held
            .drain()
            .map(|(event, cell)| (cell, event))
            .collect::<Vec<_>>();
        held.sort_unstable();
        drop(claims);
        retirement.send_replace(true);
        held
    }

    pub(crate) fn end_event_registrations(&self) -> Option<Vec<BackgroundRegistration>> {
        let ended = self.events.lock().unwrap().end()?;
        drop(ended.discarded_gates);
        Some(ended.background)
    }

    pub(crate) fn register_background(&self, work: BackgroundRegistration) {
        let refused = self.events.lock().unwrap().register_background(work).err();
        drop(refused);
    }

    pub(crate) fn accepts_wait_until(&self) -> bool {
        self.events.lock().unwrap().accepts_background()
    }

    pub(crate) fn take_late_background(&self) -> Vec<BackgroundRegistration> {
        self.events.lock().unwrap().take_late_background()
    }

    pub(crate) fn seal_wait_until(&self) {
        let abandoned = self.events.lock().unwrap().seal_background();
        drop(abandoned);
    }

    pub(crate) fn register_arm_gate(&self, gate: ArmGateRx) -> Result<(), ArmGateRx> {
        self.events.lock().unwrap().register_gate(gate)
    }

    pub(crate) fn take_arm_gates(&self) -> Vec<ArmGateRx> {
        self.events.lock().unwrap().take_gates()
    }

    pub(crate) fn depth(&self) -> usize {
        self.events.lock().unwrap().depth()
    }

    /// Close every isolate-polled socket this request opened.
    ///
    /// Draining makes the operation idempotent. The drive loop calls it when
    /// the request retires, and `drop` covers every earlier failure path.
    pub(crate) fn close_sockets(&self) {
        let opened = self.sockets.lock().unwrap().drain(..).collect();
        if let Some(cleanup) = self.cleanup.get() {
            (cleanup.close_websockets)(self, opened);
        }
        let rewriters: Vec<u64> = self.rewriters.lock().unwrap().drain(..).collect();
        for id in rewriters {
            crate::html_rewriter::free(id);
        }
        let tcp: Vec<u64> = self.tcp_sockets.lock().unwrap().drain(..).collect();
        if let Some(cleanup) = self.cleanup.get() {
            for id in tcp {
                (cleanup.close_tcp)(id);
            }
        }
    }

    #[doc(hidden)]
    pub fn own_body_stream(&self, stream_id: u64) {
        let Some(claim) = claim_http_stream(stream_id) else {
            return;
        };
        let replaced = self.body_streams.lock().unwrap().insert(stream_id, claim);
        // Releasing a claim can drop an arbitrary source. Keep that work out
        // of the request-ownership lock.
        drop(replaced);
    }

    pub(crate) fn transfer_body_stream(&self, stream_id: u64) -> Option<RequestBodyGuard> {
        let claim = self.body_streams.lock().unwrap().remove(&stream_id)?;
        // Move the exact service claim into the guard. A later asynchronous
        // drop cannot resolve through another ambient Domain.
        Some(RequestBodyGuard::transferred(claim))
    }

    /// Replace one request-owned source with the two sources created by a
    /// native tee. Response streams are not request-owned, so they keep the
    /// registry's ordinary unowned lifecycle.
    pub(crate) fn replace_body_stream(
        &self,
        stream_id: u64,
        service: &Arc<HttpStreamService>,
        branches: (u64, u64),
    ) {
        let original = self.body_streams.lock().unwrap().remove(&stream_id);
        let Some(original) = original else {
            return;
        };
        debug_assert!(Arc::ptr_eq(&original.service, service));
        let mut branch_claims = Vec::new();
        for branch in [branches.0, branches.1] {
            if let Some(claim) = service.claim(branch) {
                branch_claims.push((branch, claim));
            }
        }
        let replaced = {
            let mut owned = self.body_streams.lock().unwrap();
            branch_claims
                .into_iter()
                .filter_map(|(branch, claim)| owned.insert(branch, claim))
                .collect::<Vec<_>>()
        };
        // The original entry was consumed by tee, so releasing this claim is
        // a no-op. Replaced claims still release through their bound service.
        drop(original);
        drop(replaced);
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn begin_event_for_test(&self) {
        self.begin_event();
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn end_event_for_test(&self) -> Option<()> {
        self.end_event_registrations().map(drop)
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn register_arm_gate_for_test(
        &self,
        gate: tokio::sync::oneshot::Receiver<Result<(), String>>,
    ) -> Result<(), tokio::sync::oneshot::Receiver<Result<(), String>>> {
        self.register_arm_gate(gate)
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn take_arm_gates_for_test(
        &self,
    ) -> Vec<tokio::sync::oneshot::Receiver<Result<(), String>>> {
        self.take_arm_gates()
    }
}

impl Drop for IoContext {
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        // The drive loop also abandons these while it still owns the isolate,
        // because that path resets the JavaScript actor state. Drop is the
        // final host-side invariant for an earlier exit: no request context
        // can leave a process-wide input gate active after it is gone.
        if let Some(cleanup) = self.cleanup.get() {
            (cleanup.retire_input_gates)(self);
        }
        self.close_sockets();
        let mut claims = self
            .body_streams
            .get_mut()
            .unwrap()
            .drain()
            .collect::<Vec<_>>();
        // A claim can drop an arbitrary source. Fix the first-panic order even
        // when the request map uses a different randomized hash seed.
        claims.sort_unstable_by_key(|(stream_id, _)| *stream_id);
        let mut first_panic = None;
        for (_, claim) in claims {
            retain_first_http_cleanup_panic(
                &mut first_panic,
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(claim))),
            );
        }
        drop(self.registration.take());
        if let Some(payload) = first_panic {
            if already_panicking {
                std::mem::forget(payload);
            } else {
                std::panic::resume_unwind(payload);
            }
        }
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<IoContext>>> = const { RefCell::new(None) };
}

/// Install an invocation for this host turn, restoring its predecessor on drop.
///
/// The guard is bound to its entering thread. Outgoing owners drop after the
/// TLS borrow ends so their cleanup may safely reenter the host context API.
///
/// ```compile_fail
/// use cellar::host_io::{CurrentGuard, IoContext};
/// fn requires_send<T: Send>(_: T) {}
/// requires_send(CurrentGuard::enter(IoContext::new()));
/// ```
pub struct CurrentGuard {
    previous: Option<Arc<IoContext>>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl CurrentGuard {
    pub fn enter(context: Arc<IoContext>) -> Self {
        Self {
            previous: CURRENT.with(|current| current.borrow_mut().replace(context)),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for CurrentGuard {
    fn drop(&mut self) {
        let outgoing = CURRENT
            .with(|current| std::mem::replace(&mut *current.borrow_mut(), self.previous.take()));
        drop(outgoing);
    }
}

pub(crate) fn current_or_init(create: impl FnOnce() -> Arc<IoContext>) -> Arc<IoContext> {
    if let Some(context) = installed_context() {
        return context;
    }
    let created = create();
    CURRENT.with(|current| {
        let mut current = current.borrow_mut();
        current.get_or_insert_with(|| created.clone()).clone()
    })
}

/// The invocation installed on this thread, if a host turn is running.
pub fn installed_context() -> Option<Arc<IoContext>> {
    CURRENT.with(|current| current.borrow().clone())
}

#[cfg(test)]
mod tests;
