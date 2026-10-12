// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Engine-independent request turn ownership, replies and operation lifetimes.

use crate::engine_api::{
    fail_in_turn_error, HttpResponse, QueueDispatchResult, RequestBody, RequestId, RpcData,
    RpcOutcome,
};
use crate::host_channels::CallOrder;
use crate::host_gates::*;
use crate::host_io::IoContext;
use crate::native_worker;
use crate::wake_entry::ArmGateRx;
use crate::ws_registry::{WsDispatch, WsIn};
use crate::{asyncrt, storage};
use anyhow::{anyhow, Result};
use std::pin::Pin;
#[cfg(cellar_internal_tests)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// An engine's private roots and operation resolvers. Compiled Rust turns
/// have no attachment; host lifetime and gate decisions remain shared.
pub(crate) trait TurnEngine: std::any::Any + Send + Sync {
    fn background_active(&self) -> bool;
    fn clear_background(&mut self);
    fn remove_operations(&self, ids: Vec<u64>);
    fn discard_response_capture(&self, context: &IoContext);
}

/// A host operation and the identity used by its owning turn.
pub type Op = (u64, asyncrt::OpFuture);

pub enum AlarmDispatch {
    /// Claim whatever is due now, and record the outcome.
    Due,
    #[cfg(cellar_internal_tests)]
    Armed,
    #[cfg(cellar_internal_tests)]
    Claimed(i64),
}

/// One event a cell receives.
///
/// Every variant is something the outside world did to the cell, and each
/// becomes an `InFlight` entry driven by its own tokio task. Lifecycle —
/// taking a cell in, giving it back, cancelling a request — is not here: it
/// is a direct call on the isolate, because it is not an event and has no
/// handler to run.
pub enum CellJob {
    Fetch {
        request_id: Option<RequestId>,
        scope: String,
        name: Option<String>,
        url: String,
        method: String,
        body: RequestBody,
        headers: Vec<(String, String)>,
        reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
        /// Where this call sits in its caller's order for this cell, if it
        /// came from a script rather than from a peer or the ingress.
        order: Option<CallOrder>,
    },
    Rpc {
        request_id: Option<RequestId>,
        scope: String,
        name: Option<String>,
        method: String,
        args: RpcData,
        reply: tokio::sync::oneshot::Sender<Result<RpcOutcome>>,
    },
    WsOpen {
        scope: String,
        ws_id: u64,
        /// As on `WsMessage`, for a socket that a facet dialed.
        facet_opening: Option<u64>,
        protocol: String,
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
    },
    WsMessage {
        scope: String,
        ws_id: u64,
        /// The opening of the facet that accepted the socket, for an event of
        /// a facet's socket. See `facet_socket_refusal`.
        facet_opening: Option<u64>,
        data: WsIn,
        /// Released after the first turn, so the socket can deliver the next
        /// message in order without waiting for this handler's async work.
        started: Option<tokio::sync::oneshot::Sender<()>>,
        reply: tokio::sync::oneshot::Sender<Result<WsDispatch>>,
    },
    WsClosed {
        scope: String,
        ws_id: u64,
        /// As on `WsMessage`.
        facet_opening: Option<u64>,
        code: u16,
        reason: String,
        was_clean: bool,
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
    },
    Alarm {
        /// The shell task identity used to interrupt a firing during drain.
        request_id: Option<RequestId>,
        scope: String,
        scheduled_ms: i64,
        /// Which alarm to run, and who owns the bookkeeping.
        claim: AlarmDispatch,
        /// Replies with (next alarm, the handler's write delta). The delta is
        /// sampled inside the turn — cell storage must never be reached from
        /// the shell — and the shell uses it to prove the consuming commit
        /// durable before the core settles. Every answer shape samples it the
        /// same way now, in `InFlight::answer_settled`.
        reply:
            tokio::sync::oneshot::Sender<Result<(cellar_logic::wake::AlarmSnapshot, Option<u64>)>>,
    },
    #[cfg(cellar_internal_tests)]
    SyncErrorForTest {
        scope: String,
        gate: ArmGateRx,
        socket_id: Option<u64>,
        terminate: bool,
        reply: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
    },
}

impl CellJob {
    /// The cell this event addresses, which is also the realm it runs in.
    pub fn scope(&self) -> &str {
        match self {
            CellJob::Fetch { scope, .. }
            | CellJob::Rpc { scope, .. }
            | CellJob::WsOpen { scope, .. }
            | CellJob::WsMessage { scope, .. }
            | CellJob::WsClosed { scope, .. }
            | CellJob::Alarm { scope, .. } => scope,
            #[cfg(cellar_internal_tests)]
            CellJob::SyncErrorForTest { scope, .. } => scope,
        }
    }

    /// The client request that can cancel a fetch or cell RPC event.
    pub(crate) fn request_id(&self) -> Option<RequestId> {
        match self {
            CellJob::Fetch { request_id, .. } | CellJob::Rpc { request_id, .. } => *request_id,
            _ => None,
        }
    }

    /// Queue sends use native RPC, so the method is the narrow seam where the
    /// owner can bound producer events without limiting an ordinary Durable
    /// Object RPC or a Queue alarm and settlement.
    pub(crate) fn is_queue_producer(&self) -> bool {
        matches!(self, CellJob::Rpc { scope, method, .. }
            if scope.split_once(':').is_some_and(|(class, _)|
                class == crate::deploy::QUEUE_CLASS) && method == "__queueSend")
    }

    /// This event's place in its caller's order, taken out so the drive can
    /// wait on it and then release the call behind it.
    pub fn take_order(&mut self) -> Option<CallOrder> {
        match self {
            CellJob::Fetch { order, .. } => order.take(),
            _ => None,
        }
    }

    /// Fail this event without running it.
    pub fn fail(self, error: anyhow::Error) {
        match self {
            CellJob::Fetch { reply, .. } => drop(reply.send(Err(error))),
            CellJob::Rpc { reply, .. } => drop(reply.send(Err(error))),
            CellJob::WsOpen { reply, .. } => drop(reply.send(Err(error))),
            CellJob::WsMessage { reply, .. } => drop(reply.send(Err(error))),
            CellJob::WsClosed { reply, .. } => drop(reply.send(Err(error))),
            CellJob::Alarm { reply, .. } => drop(reply.send(Err(error))),
            #[cfg(cellar_internal_tests)]
            CellJob::SyncErrorForTest { reply, .. } => drop(reply.send(Err(error))),
        }
    }
}

/// The storage authority installed when a cell enters an isolate.
///
/// The path and epoch form one value because opening either one without the
/// other would let later asynchronous work use the wrong ownership epoch.
#[doc(hidden)]
pub struct CellStorage<'a> {
    pub path: &'a str,
    pub epoch: u64,
    /// Fleet ownership supplies persistent writer epochs. Standalone ownership
    /// resets on restart and never publishes discovery objects.
    pub replicated_wake: bool,
    /// The activation's paged VFS, when its restore paged. The file at `path`
    /// is then sparse; opening it without this VFS reads holes as data.
    pub vfs: Option<&'a str>,
}

/// One in-flight request inside a shared isolate.
///
/// Owned by the request's own tokio task, which is why every field is
/// `Send`: the task suspends between turns and can resume on any worker,
/// then re-enters the isolate it is affiliated with.
/// Where a finished handler's result goes, and in what shape.
///
/// A fetch answers an `HttpResponse` and an entrypoint RPC answers bytes.
/// Everything between — the turn loop, the op region, cancellation, the
/// budget — is identical, so the difference lives here rather than in two
/// copies of `drive`.
pub enum Answer {
    Fetch(tokio::sync::oneshot::Sender<Result<HttpResponse>>),
    Rpc(tokio::sync::oneshot::Sender<Result<Vec<u8>>>),
    Queue(tokio::sync::oneshot::Sender<Result<QueueDispatchResult>>),
    /// A DO method call, which answers a value rather than a response.
    CellRpc(tokio::sync::oneshot::Sender<Result<RpcOutcome>>),
    /// A `webSocketMessage`, which answers the frames the output gate held
    /// and the write they are gated on.
    WsMessage(tokio::sync::oneshot::Sender<Result<WsDispatch>>),
    /// An event whose result is that it finished: `webSocketOpen`,
    /// `webSocketClose`. It answers the position its writes reached, so the
    /// shell can open the barrier they need.
    Ack(tokio::sync::oneshot::Sender<Result<Option<u64>>>),
    /// An alarm, which answers whatever alarm the handler left armed.
    Alarm(tokio::sync::oneshot::Sender<Result<(cellar_logic::wake::AlarmSnapshot, Option<u64>)>>),
}

/// The invocation data that becomes one Tail Worker event after all referenced
/// work finishes. The response sender stays independent, so tail delivery
/// cannot delay or replace the Dynamic Worker response.
pub(crate) struct TailReportState {
    pub(crate) reply: tokio::sync::oneshot::Sender<String>,
    pub(crate) script_name: String,
    pub(crate) event_timestamp: i64,
    pub(crate) url: String,
    pub(crate) method: String,
    pub(crate) headers: serde_json::Map<String, serde_json::Value>,
    pub(crate) response_status: Option<u16>,
    pub(crate) failure: Option<TailException>,
}

/// A failure and the instant it occurred. Tail delivery can wait for
/// `waitUntil` work, so sampling the clock when the report finishes would
/// move the exception forward by the duration of that work.
#[derive(serde::Serialize)]
pub(crate) struct TailException {
    timestamp: i64,
    name: &'static str,
    message: String,
}

impl TailException {
    pub(crate) fn new(message: String) -> Self {
        Self {
            timestamp: crate::telemetry::now_unix_us() / 1_000,
            name: "Error",
            message,
        }
    }
}

pub(crate) fn tail_request_headers(
    headers: &[(String, String)],
) -> serde_json::Map<String, serde_json::Value> {
    let mut normalized = serde_json::Map::new();
    for (name, value) in headers {
        match normalized.entry(name.to_ascii_lowercase()) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(serde_json::Value::String(value.clone()));
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                let serde_json::Value::String(joined) = entry.get_mut() else {
                    unreachable!("tail request headers contain only strings");
                };
                joined.push_str(", ");
                joined.push_str(value);
            }
        }
    }
    normalized
}

impl TailReportState {
    pub(crate) fn record_failure(&mut self, message: String) {
        self.failure = Some(TailException::new(message));
    }

    pub(crate) fn finish(self, context: &IoContext) {
        let outcome = if self.failure.is_some() {
            "exception"
        } else {
            "ok"
        };
        let exceptions: Vec<_> = self.failure.into_iter().collect();
        let event = serde_json::json!([{
            "scriptName": self.script_name,
            "event": {
                "request": {
                    "cf": {},
                    "headers": self.headers,
                    "method": self.method,
                    "url": self.url,
                },
                "response": self.response_status.map(|status| serde_json::json!({
                    "status": status,
                })),
            },
            "eventTimestamp": self.event_timestamp,
            "logs": context.take_tail_logs(),
            "exceptions": exceptions,
            "outcome": outcome,
        }]);
        let _ = self.reply.send(event.to_string());
    }
}

impl Answer {
    /// Send an error, whichever shape the caller is waiting for.
    pub(crate) fn fail(self, error: anyhow::Error) {
        let _ = self.fail_with_arm_gates(error, Vec::new());
    }

    pub(crate) fn fail_with_arm_gates(
        self,
        error: anyhow::Error,
        gates: Vec<ArmGateRx>,
    ) -> Option<GatedReplyRx> {
        match self {
            Answer::Fetch(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Rpc(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Queue(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::CellRpc(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::WsMessage(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Ack(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
            Answer::Alarm(reply) => send_answer_after_arm_gates(reply, Err(error), gates),
        }
    }
}

pub(crate) async fn await_arm_gates(gates: Vec<ArmGateRx>) -> Result<(), String> {
    let mut first_failure = None;
    for gate in gates {
        let failure = match gate.await {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(_) => Some("wake-entry gate task dropped".into()),
        };
        if first_failure.is_none() {
            first_failure = failure;
        }
    }
    match first_failure {
        Some(failure) => Err(failure),
        None => Ok(()),
    }
}

pub(crate) fn send_answer_after_arm_gates<T: Send + 'static>(
    mut reply: tokio::sync::oneshot::Sender<Result<T>>,
    value: Result<T>,
    gates: Vec<ArmGateRx>,
) -> Option<GatedReplyRx> {
    if gates.is_empty() {
        drop(reply.send(value));
        return None;
    }
    let (completed, completion) = tokio::sync::oneshot::channel();
    let (cancel, mut cancelled) = tokio::sync::oneshot::channel();
    #[cfg(cellar_internal_tests)]
    let drop_task = asyncrt::services()
        .wake_entry()
        .drop_next_gated_reply_task
        .swap(false, Ordering::AcqRel);
    asyncrt::spawn(async move {
        #[cfg(cellar_internal_tests)]
        if drop_task {
            return;
        }
        enum GateWait {
            Completed(Result<(), String>),
            CallerClosed,
            Cancelled,
            DriverDropped,
        }
        let gate_wait = {
            let mut waiting = Box::pin(await_arm_gates(gates));
            let result = asyncrt::select_biased! {
                "a completed gate aggregate wins a tie with caller cancellation";
                result = &mut waiting => GateWait::Completed(result),
                cancellation = async {
                    asyncrt::select_biased! {
                        "a closed caller wins a tie with an explicit cancellation signal";
                        _ = reply.closed() => GateWait::CallerClosed,
                        cancelled = &mut cancelled => match cancelled {
                            Ok(()) => GateWait::Cancelled,
                            Err(_) => GateWait::DriverDropped,
                        },
                    }
                } => cancellation,
            };
            // On either cancellation path this drops every unobserved gate
            // receiver before the completion becomes visible to the driver.
            drop(waiting);
            result
        };
        let value = match gate_wait {
            GateWait::Completed(gate_result) => match (value, gate_result) {
                (value, Ok(())) => value,
                (Ok(_), Err(error)) => Err(anyhow!("wake-entry gate: {error}")),
                (Err(primary), Err(error)) => {
                    tracing::error!(
                        event = "wake_entry_gate_failed_after_handler_error",
                        handler_error = %primary,
                        gate_error = %error,
                    );
                    Err(primary.context(format!("wake-entry gate also failed: {error}")))
                }
            },
            GateWait::CallerClosed => {
                let failure = value
                    .as_ref()
                    .err()
                    .map(|error| crate::telemetry::cap_error(format!("{error:#}")));
                drop(value);
                let _ = completed.send(GatedReplyCompletion::CallerClosed { failure });
                return;
            }
            // The driver observed a request cancellation after the handler
            // fixed its reply. Do not enter JavaScript again: the handler is
            // already over. A successful answer becomes a disconnect error,
            // while an existing handler error remains the primary failure.
            GateWait::Cancelled => match value {
                Ok(_) => Err(anyhow!("The client has disconnected")),
                Err(primary) => Err(primary),
            },
            // Dropping the driver drops both halves of its completion handle.
            // Release every detached resource, but do not let an abandoned
            // driver manufacture a reply that nothing owns any longer.
            GateWait::DriverDropped => {
                drop(value);
                return;
            }
        };
        let failure = value
            .as_ref()
            .err()
            .map(|error| crate::telemetry::cap_error(format!("{error:#}")));
        let completion = match reply.send(value) {
            Ok(()) => GatedReplyCompletion::Sent { failure },
            Err(value) => {
                drop(value);
                GatedReplyCompletion::CallerClosed { failure }
            }
        };
        let _ = completed.send(completion);
    })
    .detach();
    Some(GatedReplyRx {
        completion,
        cancel: Some(cancel),
    })
}

pub(crate) enum GatedReplyCompletion {
    /// The final reply value is now visible to the receiver.
    Sent { failure: Option<String> },
    /// The receiver disappeared, so no reply remains to deliver.
    CallerClosed { failure: Option<String> },
}

pub(crate) struct GatedReplyRx {
    completion: tokio::sync::oneshot::Receiver<GatedReplyCompletion>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

impl GatedReplyRx {
    /// Stop waiting on event gates after a request cancellation. The detached
    /// task owns the reply value and gate receivers, so it performs their
    /// ordered cleanup and reports completion back to the driver.
    pub(crate) fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

impl std::future::Future for GatedReplyRx {
    type Output = Result<GatedReplyCompletion, tokio::sync::oneshot::error::RecvError>;

    fn poll(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().completion).poll(context)
    }
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub async fn await_arm_gates_for_test(
    gates: Vec<tokio::sync::oneshot::Receiver<Result<(), String>>>,
) -> Result<(), String> {
    await_arm_gates(gates).await
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub enum GatedReplyCompletionForTest {
    Sent { failure: Option<String> },
    CallerClosed { failure: Option<String> },
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub struct GatedReplyForTest(GatedReplyRx);

#[cfg(cellar_internal_tests)]
impl std::future::Future for GatedReplyForTest {
    type Output = Result<GatedReplyCompletionForTest, tokio::sync::oneshot::error::RecvError>;

    fn poll(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context).map(|result| {
            result.map(|completion| match completion {
                GatedReplyCompletion::Sent { failure } => {
                    GatedReplyCompletionForTest::Sent { failure }
                }
                GatedReplyCompletion::CallerClosed { failure } => {
                    GatedReplyCompletionForTest::CallerClosed { failure }
                }
            })
        })
    }
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn send_answer_after_arm_gates_for_test<T: Send + 'static>(
    reply: tokio::sync::oneshot::Sender<Result<T>>,
    value: Result<T>,
    gates: Vec<tokio::sync::oneshot::Receiver<Result<(), String>>>,
) -> Option<GatedReplyForTest> {
    send_answer_after_arm_gates(reply, value, gates).map(GatedReplyForTest)
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn drop_next_gated_reply_task_for_test() {
    asyncrt::services()
        .wake_entry()
        .drop_next_gated_reply_task
        .store(true, Ordering::Release);
}

/// What a claimed alarm still owes its bookkeeping.
///
/// `finish_alarm_handler` must run exactly once however the event ends, and
/// an event can now end without running JS at all — a budget overrun, a
/// handler waiting on nothing — so the entry carries the claim rather than
/// the caller that made it.
pub(crate) struct AlarmClaim {
    /// The instant the dispatch was judged due against, so the outcome is
    /// recorded against the same one.
    pub(crate) now_ms: i64,
}
pub struct InFlight {
    /// The handler's promise. A `Global` because it outlives the turn that
    /// created it: Locals never cross a turn, exactly as workerd's
    /// `Worker::Lock` never does.
    pub(crate) engine: Option<Box<dyn TurnEngine>>,
    pub(crate) native: Option<Mutex<native_worker::NativeFlight>>,
    pub(crate) context: Arc<IoContext>,
    /// The cell this event belongs to, and `None` for stateless work.
    ///
    /// It is what the storage-shaped parts of settling — the durability
    /// position, a fatal SQL error — are read against.
    pub(crate) scope: Option<String>,
    /// The cell's committed-write position before the handler ran.
    ///
    /// The answer carries a write position only if the handler advanced it,
    /// so the output gate sees a write this event made and ignores cellar's
    /// own activation writes (the actor name, alarm bookkeeping).
    pub(crate) writes_before: Option<u64>,
    pub(crate) request_id: Option<RequestId>,
    #[allow(dead_code)]
    pub(crate) active_request_id: Option<RequestId>,
    pub(crate) reply: Option<Answer>,
    pub(crate) gated_reply: Option<GatedReplyRx>,
    /// `waitUntil` work still running after the response was sent. The
    /// request stays in flight until it settles, as the single-request loop
    /// keeps driving it.
    /// A successfully completed Durable Object event keeps every referenced
    /// native operation alive. Workerd does not require `waitUntil` for this
    /// I/O, so dropping these operations loses detached timers and subrequests.
    pub(crate) completed_cell_event: bool,
    /// Ops this request is waiting on, so a completion can be attributed to
    /// the request whose context must be current while its continuation runs.
    pub(crate) ops: std::collections::HashSet<u64>,
    /// The subset of `ops` whose resource owns this event's `IoContext`.
    /// These operations continue after the handler and `waitUntil` settle.
    pub(crate) io_context_ops: std::collections::HashSet<u64>,
    /// The subset of `ops` that can run while referenced work is live but
    /// cannot keep this event alive by themselves.
    pub(crate) unrefed_ops: std::collections::HashSet<u64>,
    /// The alarm bookkeeping this entry still owes, if it is one.
    pub(crate) alarm: Option<AlarmClaim>,
    pub(crate) started: Instant,
    /// The propagation context for this entry. A rejected foreign context
    /// stays here with `sampled` clear, so continuations can forward it
    /// without treating it as permission to record spans or logs.
    pub(crate) trace: Option<crate::telemetry::TraceContext>,
    /// Why the event failed, captured for the span's `error` so a query sees
    /// the reason, not only that it failed. This also records an output-gate
    /// failure after a successful handler.
    pub(crate) failure: Option<String>,
    /// The report owed to the loader isolate after this invocation retires.
    pub(crate) tail_report: Option<TailReportState>,
    /// The worker's weak continuation lookup for operation handoff.
    pub(crate) registry: Arc<crate::host_io::ContextRegistry>,
}

impl InFlight {
    /// Subscribe and recheck a foreign input-gate claim as one wait preparation.
    pub(crate) fn prepare_cross_entry_gate_wait(
        &self,
    ) -> crate::host_io::input_gate_lifecycle::PreparedWait {
        self.context
            .prepare_cross_entry_gate_wait(self.answered(), || self.finished())
    }

    pub(crate) fn engine_background_active(&self) -> bool {
        self.engine
            .as_ref()
            .is_some_and(|engine| engine.background_active())
    }
    pub(crate) fn clear_engine_background(&mut self) {
        if let Some(engine) = &mut self.engine {
            engine.clear_background();
        }
    }
    pub(crate) fn remove_engine_ops(&self, ids: Vec<u64>) {
        if let Some(engine) = &self.engine {
            engine.remove_operations(ids);
        }
    }

    /// The committed write position when the handler advanced its cell.
    pub(crate) fn write_delta(&self) -> Result<Option<u64>> {
        self.gate_positions().map(|(write, _)| write)
    }

    /// Sample both the committed-write and observed-position output barriers.
    /// Returning them together prevents either barrier from releasing output
    /// covered by the other.
    pub(crate) fn gate_positions(&self) -> Result<(Option<u64>, Option<u64>)> {
        match self.scope.as_deref() {
            Some(scope) => gate_positions(scope, self.writes_before),
            None => Ok((None, None)),
        }
    }

    /// Fail whichever shape is waiting, without knowing which.
    ///
    /// This must not touch cell storage: a budget overrun and a stuck
    /// handler are found on the driving task, between turns, where the
    /// storage thread-local is null — settling the alarm claim here
    /// panicked the node (denoland/celld#170). The claim stays put, and
    /// the drive loop's `owes_alarm` turn records it as not counting
    /// against the retry limit, because a failure here is not the
    /// handler's. A handler that threw is recorded by `settle`, which
    /// knows that it did, before it reaches this.
    ///
    /// For the same reason the error leaves here without the position the
    /// handler's commits reached: `fail_in_turn` is the half that can sample
    /// it. A write a handler made before it ran out of budget is therefore
    /// still an unproven commit with no barrier of its own.
    pub(crate) fn fail(&mut self, error: anyhow::Error) {
        if let Some(report) = &mut self.tail_report {
            report.record_failure(format!("{error:#}"));
        }
        if let Some(reply) = self.reply.take() {
            if matches!(&reply, Answer::WsMessage(_)) {
                // A failed message has no successful dispatch whose barrier
                // can authorize its output. Discard the still-active capture
                // here so rejection, cancellation, and turn failures cannot
                // leave frames for the turn-end flush to publish.
                if let Some(engine) = &self.engine {
                    engine.discard_response_capture(&self.context);
                }
            }
            if self.trace.is_some_and(|trace| trace.sampled) {
                self.failure = Some(crate::telemetry::cap_error(error.to_string()));
            }
            self.gated_reply = reply.fail_with_arm_gates(error, self.context.take_arm_gates());
        }
    }

    /// Fail the event from inside its isolate turn, where the cell's storage
    /// is at hand. The error carries the positions the turn sampled, so the
    /// shell gates it as it gates a success. A commit a handler made before it
    /// threw is as unproven as one it answered with, and without a ticket it
    /// opened no barrier, so a read-only request that followed revealed it
    /// while a crash could still lose it. A handler that only read carries the
    /// observed position for the same reason: the message it throws with can
    /// quote what it read, and the error answer reveals that state as a
    /// response body does. An alarm that rejected settled its claim before
    /// this, so the delta covers its retry record too; one that failed any
    /// other way still owes that record, and `turn_finish_alarm` samples again
    /// after writing it.
    pub(crate) fn fail_in_turn(&mut self, error: anyhow::Error) {
        let error = match self.gate_positions() {
            Ok(positions) => fail_in_turn_error(error, positions),
            Err(sample) => sample,
        };
        self.fail(error);
    }

    /// Fail the event of a client that has hung up. The write half of
    /// `fail_in_turn` applies — a commit the handler made still needs its
    /// barrier — but the read-only half does not: there is no client left to
    /// tell, so no message carries what the handler read, and a read-only
    /// ticket would only hold the request's pin, and a shutdown, for a
    /// durability round trip that proves nothing.
    pub(crate) fn fail_cancelled(&mut self, error: anyhow::Error) {
        let error = match self.write_delta() {
            Ok(write_position) => fail_in_turn_error(error, (write_position, None)),
            Err(sample) => sample,
        };
        self.fail(error);
    }

    /// Record how a claimed alarm ended. Runs once; later calls do nothing.
    pub(crate) fn settle_alarm(&mut self, ok: bool, counts_against_limit: bool) {
        let (Some(scope), Some(claim)) = (self.scope.as_deref(), self.alarm.take()) else {
            return;
        };
        if ok {
            storage::finish_alarm_handler(scope, true, claim.now_ms);
        } else {
            storage::finish_alarm_handler_with_retry_policy(
                scope,
                false,
                claim.now_ms,
                counts_against_limit,
            );
        }
    }

    /// Whether a claimed alarm's outcome is still unrecorded. True only
    /// where the event ended without ever entering the isolate again.
    pub fn owes_alarm(&self) -> bool {
        self.alarm.is_some()
    }

    /// Done when the response is sent and no referenced work remains.
    pub fn finished(&self) -> bool {
        self.retired() && !self.has_refed_ops()
    }

    /// Whether an operation can keep this event alive.
    pub(crate) fn has_refed_ops(&self) -> bool {
        self.ops != self.unrefed_ops
    }

    /// Borrow the reply gate and operation mailbox together. The wake loop
    /// must listen to both while the detached gate owns the response, and a
    /// split borrow avoids cloning the context on every operation pass.
    pub(crate) fn gated_reply_and_io_context(
        &mut self,
    ) -> (Option<&mut GatedReplyRx>, &Arc<IoContext>) {
        (self.gated_reply.as_mut(), &self.context)
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn has_gated_reply_for_test(&self) -> bool {
        self.gated_reply.is_some()
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub async fn finish_gated_reply_for_test(&mut self) -> bool {
        let Some(gated_reply) = self.gated_reply.as_mut() else {
            return false;
        };
        let completion = gated_reply.await;
        self.finish_gated_reply(completion);
        true
    }

    /// Ask the detached gate owner to release every event gate and finish the
    /// reply edge. The driver waits for its completion before it retires the
    /// event, so response resources cannot outlive that cleanup.
    pub(crate) fn cancel_gated_reply(&mut self) {
        if let Some(gated_reply) = &mut self.gated_reply {
            gated_reply.cancel();
        }
    }

    /// Retire a gate waiter after it sent the reply or observed caller-close.
    ///
    /// This method also closes request sockets when the completion makes the
    /// entry retire. `finish_turn` cannot do that earlier because the reply
    /// value can still own response resources while the gate is pending.
    pub(crate) fn finish_gated_reply(
        &mut self,
        completion: Result<GatedReplyCompletion, tokio::sync::oneshot::error::RecvError>,
    ) {
        self.gated_reply = None;
        if self.trace.is_some_and(|trace| trace.sampled) {
            match completion {
                Ok(GatedReplyCompletion::Sent {
                    failure: Some(failure),
                })
                | Ok(GatedReplyCompletion::CallerClosed {
                    failure: Some(failure),
                }) => self.failure = Some(failure),
                Ok(GatedReplyCompletion::Sent { failure: None }) => {}
                Ok(GatedReplyCompletion::CallerClosed { failure: None }) => {
                    if self.failure.is_none() {
                        self.failure = Some("The client has disconnected".to_string());
                    }
                }
                Err(_) => {
                    if self.failure.is_none() {
                        self.failure = Some("wake-entry reply gate task dropped".to_string());
                    }
                }
            }
        }
        if self.retired() {
            self.context.close_sockets();
        }
    }

    /// The request has answered and its `waitUntil` work has settled.
    ///
    /// An isolate-polled WebSocket can still own an op after this point, and
    /// that op must not defer retirement. Retiring closes the sockets the
    /// request opened, and the pump of an outbound socket ends only with a
    /// close, so a retirement that waited for the pump waited for itself and
    /// the request never left the drive loop. The op keeps the entry's ops
    /// alive instead, through `keeps_native_ops`, which is what a socket the
    /// response took over needs: that socket left the request's set at the
    /// handoff, so retiring does not close it, and its pump runs until the
    /// client closes it.
    pub(crate) fn retired(&self) -> bool {
        self.reply.is_none()
            && self.gated_reply.is_none()
            && !self.engine_background_active()
            && self.native.as_ref().is_none_or(|flight| !flight.lock().unwrap().has_work())
            // A block that holds the gate is still this event's work:
            // retiring would close the sockets its callback may still read.
            && !self.holds_gate()
            // A reaction from this event can run its block during another
            // event's turn. Retain this event until that block releases its
            // claim, or retirement closes resources the callback still uses.
            && self.context.retire_without_cross_entry_gate()
    }

    /// Why the event failed, when a sampled trace captured it.
    pub fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    pub(crate) fn finish_tail_report(&mut self) {
        if let Some(report) = self.tail_report.take() {
            report.finish(&self.context);
        }
    }

    /// How long this handler may still run, or `None` once it has answered.
    ///
    /// The budget bounds the *response*, not the request: `waitUntil` work
    /// continues after the client has been served and is not charged for the
    /// time the handler already spent.
    pub fn remaining(&self, budget: Duration) -> Option<Duration> {
        self.reply
            .is_some()
            .then(|| budget.saturating_sub(self.started.elapsed()))
    }

    /// Give up on a handler that will not settle.
    pub fn time_out(&mut self, budget: Duration) {
        if let Some(native) = &self.native {
            native.lock().unwrap().stop_handler();
        }
        self.fail(anyhow!("handler exceeded {budget:?} budget"));
    }

    /// Nothing this request awaits can move it, so it will never settle on
    /// its own. Reachable when a handler awaits a promise only some *other*
    /// request could resolve — which the pump concealed, because it settled
    /// every entry on every turn whoever the turn belonged to.
    pub fn stuck(&mut self) {
        self.native = None;
        self.fail(anyhow!("handler is waiting on nothing"));
        self.clear_engine_background();
        self.context.seal_wait_until();
    }

    /// Whether a nested `WorkerEntrypoint` call is still pending. When no
    /// native operation can resume JavaScript, the call itself must reject;
    /// failing the enclosing event would prevent its caller from catching the
    /// Workerd cancellation.
    pub(crate) fn has_pending_events(&self) -> bool {
        self.context.has_pending_events()
    }

    /// Has the client been answered? `waitUntil` work can still be running.
    pub fn answered(&self) -> bool {
        self.reply.is_none() && self.gated_reply.is_none()
    }

    /// Whether a native operation can still resume JavaScript for this event.
    ///
    /// A detached reply gate is host work. It cannot keep handler operations
    /// alive after the handler ends. A successfully completed Durable Object
    /// event keeps its pending I/O without `waitUntil`, as Workerd does. An
    /// operation that owns the event's `IoContext` and a block that holds or
    /// is queued to take a cell's input gate can also continue. The block
    /// belongs to the object, not to the client: it runs to completion as
    /// workerd's critical section does, and its `finally` opens the gate.
    /// Dropping its ops with the reply dropped the timer or subrequest it
    /// awaited, and the gate stayed shut for every later event (#733).
    pub(crate) fn keeps_native_ops(&self) -> bool {
        self.reply.is_some()
            || self
                .native
                .as_ref()
                .is_some_and(|flight| flight.lock().unwrap().has_work())
            || self.completed_cell_event
            || !self.io_context_ops.is_empty()
            || self.keeps_native_ops_after_disconnect()
    }

    /// Whether work that survives a normal client disconnect still needs
    /// this event's native operations.
    ///
    /// An origin claim covers operations started before a cross-entry block.
    /// Its callback can already hold their promises even though another
    /// event owns the block's turn and therefore owns the gate engagement.
    pub(crate) fn keeps_native_ops_after_disconnect(&self) -> bool {
        self.engine_background_active()
            || self.engages_gate()
            || self.context.has_cross_entry_gate_claim()
    }

    /// Complete the resource-retirement edge that a claim release woke.
    pub(crate) fn finish_cross_entry_gates(&self) {
        if self.retired() {
            self.context.close_sockets();
        }
    }

    /// Whether this event holds a cell's input gate now, or is queued to
    /// take one.
    pub(crate) fn engages_gate(&self) -> bool {
        gate_engagement(self.scope.as_deref(), &self.context) != GateEngagement::None
    }

    /// Whether this event holds a cell's input gate now. A queued block has
    /// not started, so only a held one keeps the event's sockets: a stale
    /// queue count from a future that was never adopted must not keep the
    /// event from retiring.
    pub(crate) fn holds_gate(&self) -> bool {
        gate_engagement(self.scope.as_deref(), &self.context) == GateEngagement::Holds
    }

    /// Whether a client can still disconnect from this request. A request
    /// with no id is internal and has no client to hang up.
    pub fn cancellable(&self) -> bool {
        self.reply.is_some() && self.request_id.is_some()
    }

    pub fn request_id(&self) -> Option<RequestId> {
        self.request_id
    }

    /// Close operation handoff and release this event's outstanding work.
    /// Engines also remove any resolver roots for abandoned operations.
    pub fn abandon(&mut self) {
        self.native = None;
        self.context.seal_wait_until();
        self.io_context_ops.clear();
        self.unrefed_ops.clear();
        // An op a foreign turn handed this event after its driver's last pass
        // never reached `ops`, so the drain below cannot reach its resolver.
        // Closing the mailbox both yields those ops and refuses every later
        // hand-off, which is what bounds this to one place instead of a race
        // the driver would have to keep re-checking on its way out.
        let handed = self.context.close_handed_ops();
        if self.ops.is_empty() && handed.is_empty() {
            return;
        }
        let mut ids = self.ops.drain().collect::<Vec<_>>();
        ids.extend(handed.iter().map(|(id, _, _)| *id));
        drop(handed);
        self.remove_engine_ops(ids);
    }

    /// The context whose mailbox carries the ops other turns enqueued for
    /// this event.
    ///
    /// Borrowed, not cloned. The driver reads this once per pass of a loop
    /// that turns for every operation the event completes, and it holds the
    /// entry alive for longer than it holds this.
    pub(crate) fn io_context(&self) -> &Arc<IoContext> {
        &self.context
    }

    /// Give an op to the event whose continuation enqueued it.
    ///
    /// The owner's driver polls it, so the owner's retirement decides when it
    /// is cancelled and this event's retirement cannot take it away. An owner
    /// that is already gone can never run the continuation again, so its
    /// resolver goes with the op, exactly as `abandon` drops this event's own.
    pub(crate) fn hand_op(
        &mut self,
        owner: u64,
        id: u64,
        future: asyncrt::OpFuture,
        lifetime: asyncrt::OpLifetime,
    ) {
        // One branch for both refusals — an owner that is already gone, and
        // one whose mailbox closed while this turn ran. Splitting them let the
        // second forget to drop the resolver, which is the whole defect.
        let delivered = self
            .registry
            .lookup(owner)
            .is_some_and(|context| context.hand_op(id, future, lifetime));
        if !delivered {
            self.remove_engine_ops(vec![id]);
        }
    }

    /// Take the ops another turn enqueued for this event, and record them as
    /// this event's own so that `deliver` and `abandon` treat them alike.
    pub(crate) fn take_handed_ops(&mut self) -> Vec<Op> {
        // The common answer is "none", and reaching it without the queue's
        // lock is why the count exists.
        if !self.context.has_handed_ops() {
            return Vec::new();
        }
        let handed = self.context.take_handed_ops();
        let mut ops = Vec::with_capacity(handed.len());
        for (id, future, lifetime) in handed {
            self.ops.insert(id);
            if lifetime == asyncrt::OpLifetime::IoContext {
                self.io_context_ops.insert(id);
            }
            if lifetime == asyncrt::OpLifetime::Unrefed {
                self.unrefed_ops.insert(id);
            }
            ops.push((id, future));
        }
        ops
    }

    #[allow(dead_code)]
    pub(crate) fn retire_background_for_shutdown(&mut self) {
        self.clear_engine_background();
        self.context.seal_wait_until();
        self.context.close_sockets();
    }
}

#[cfg(all(test, cellar_internal_tests))]
static UNIX_CLOCK_FOR_TEST: std::sync::OnceLock<fn() -> Option<i64>> = std::sync::OnceLock::new();
#[cfg(all(test, cellar_internal_tests))]
pub(crate) fn set_unix_clock_for_test(clock: fn() -> Option<i64>) {
    let _ = UNIX_CLOCK_FOR_TEST.set(clock);
}

// Alarm dispatch and tail timestamps preserve the real host wall clock;
// the compatibility adapter can inject its controlled test clock above.
#[allow(clippy::disallowed_methods)]
pub(crate) fn unix_now_ms() -> i64 {
    #[cfg(all(test, cellar_internal_tests))]
    if let Some(timestamp_ms) = UNIX_CLOCK_FOR_TEST.get().and_then(|clock| clock()) {
        return timestamp_ms;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
pub(crate) fn gate_positions(
    scope: &str,
    writes_before: Option<u64>,
) -> Result<(Option<u64>, Option<u64>)> {
    let sample = storage::write_position(scope)?;
    let write = write_delta(writes_before, sample);
    let observed = if write.is_none() {
        storage::observed_position(scope, sample)
    } else {
        None
    };
    Ok((write, observed))
}
pub(crate) fn write_delta(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    match (before, after) {
        (Some(before), Some(after)) if after > before => Some(after),
        _ => None,
    }
}
/// The check runs in the turn that would start the handler, so no abort or
/// give-back can close the facet between the check and the dispatch. An
/// event for a facet that is not open would otherwise reach `_instance`,
/// which constructs a missing instance: the class would run under the
/// facet's scope with no storage and outside its root's lifetime.
///
/// The check compares the opening, not the root epoch. A facet that its root
/// reopened is a new actor that never accepted the socket, and a generation
/// swap reopens a facet at the same epoch. The new isolate's owner counter
/// starts again, so the facet's scope can repeat too.
pub(crate) fn facet_socket_refusal(
    scope: &str,
    facet_opening: Option<u64>,
) -> Option<anyhow::Error> {
    let opening = facet_opening?;
    (storage::facet_opening(scope) != Some(opening))
        .then(|| anyhow!("the Durable Object facet that holds this WebSocket is no longer running"))
}
/// Take the ops this turn enqueued that belong to this event, and hand every
/// other event its own.
///
/// Drained after `settle`, not before: ending an event runs JS, and anything
/// that starts there belongs to this request too. The pump drained first and
/// so could attribute those to whichever entry it settled next.
///
/// A turn is not proof of ownership. An event can poll a foreign continuation during
/// this turn's microtask checkpoint — a cell event resumed by the promise this
/// event just settled — and an op that continuation enqueues arrives here
/// exactly like this event's own. Adopting it made the wrong driver poll it,
/// and retiring then cancelled it together with the resolver, so the event
/// really waiting never settled and every later event queued behind it. The op
/// layer names the enqueueing continuation,
/// so an op with a foreign name goes to that event's driver instead. Critical
/// sections carry an explicit driver alongside their resource context; the
/// op wrapper uses that driver so gate retirement and op cancellation agree.
pub(crate) fn adopt(entry: &mut InFlight) -> Vec<Op> {
    let spawns = asyncrt::drain_spawns();
    let mine = entry.context.continuation_id();
    let mut ops = Vec::with_capacity(spawns.len());
    for (id, future, lifetime, owner) in spawns {
        if let Some(owner) = owner.filter(|owner| Some(*owner) != mine) {
            entry.hand_op(owner, id, future, lifetime);
            continue;
        }
        entry.ops.insert(id);
        if lifetime == asyncrt::OpLifetime::IoContext {
            entry.io_context_ops.insert(id);
        }
        if lifetime == asyncrt::OpLifetime::Unrefed {
            entry.unrefed_ops.insert(id);
        }
        ops.push((id, future));
    }
    ops
}
