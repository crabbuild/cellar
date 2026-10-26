// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! The channels a cell runtime uses to hand work to the host: one request
//! type and one sender per host service for Rust application turns.
use crate::bucket::Bucket;
use crate::engine_api::QueueBatch;
use crate::engine_api::RequestBody;
use crate::engine_api::RpcData;
use crate::engine_api::{next_do_request_id, HttpResponse, RequestId};
use crate::http_streams::RequestBodyGuard;
#[doc(hidden)]
pub use crate::outbound_rpc::Reservation as RpcReservation;
use crate::ws_registry::OutboundWsReq;
use anyhow::Result;
pub use cellar_native::call_order::{CallChains, CallOrder, CallWait};

/// Reserve a caller-local delivery position and retain its original event.
#[doc(hidden)]
pub fn enter_call_order(
    caller: std::sync::Arc<crate::host_io::IoContext>,
    cell: &str,
) -> CallOrder {
    caller.call_chains.enter_with_owner(cell, caller.clone())
}
#[cfg(cellar_internal_tests)]
use std::cell::RefCell;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

/// Host routing and storage handles owned by one execution domain.
#[derive(Default)]
pub(crate) struct HostChannels {
    do_call: OnceLock<tokio::sync::mpsc::UnboundedSender<DoCallReq>>,
    gate: OnceLock<tokio::sync::mpsc::UnboundedSender<GateReq>>,
    facet: OnceLock<tokio::sync::mpsc::UnboundedSender<FacetReq>>,
    service_call: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcCallReq>>,
    service_rpc: OnceLock<tokio::sync::mpsc::UnboundedSender<SvcRpcReq>>,
    asset_call: OnceLock<tokio::sync::mpsc::UnboundedSender<AssetCallReq>>,
    queue_dispatch: OnceLock<tokio::sync::mpsc::UnboundedSender<QueueDispatchReq>>,
    rpc_call: OnceLock<tokio::sync::mpsc::UnboundedSender<RpcCallReq>>,
    outbound_ws: OnceLock<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>>,
    kv_blob: OnceLock<Bucket>,
    blob: OnceLock<Bucket>,
    cancels: Arc<Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>>>,
}

/// A selector into the current host's channels, never a process-wide sender.
/// Returns owned handles so waits cannot borrow another host's storage.
pub(crate) struct HostSlot<T> {
    select: fn(&HostChannels) -> &OnceLock<T>,
}

impl<T: Clone> HostSlot<T> {
    const fn new(select: fn(&HostChannels) -> &OnceLock<T>) -> Self {
        Self { select }
    }

    pub(crate) fn get(&self) -> Option<T> {
        let services = crate::asyncrt::services();
        if !services.accepts_work() {
            return None;
        }
        (self.select)(services.channels()).get().cloned()
    }

    fn set(&self, value: T) -> Result<(), T> {
        let services = crate::asyncrt::services();
        if !services.accepts_work() {
            return Err(value);
        }
        (self.select)(services.channels()).set(value)
    }
}

/// A cell fetch handed to the host's existing local/fleet routing policy.
/// Runtime adapters marshal the request; the host owns admission and transfer.
pub struct DoCallReq {
    pub request_id: Option<RequestId>,
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    /// Explicit request signals must reach the handler even during cold
    /// routing; ordinary transport cancellation may stop admission immediately.
    pub deliver_abort_to_handler: bool,
    pub scope: String,
    pub name: Option<String>,
    pub url: String,
    pub method: String,
    pub body: RequestBody,
    pub body_guard: RequestBodyGuard,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
    /// Caller-local delivery order. Remote HTTP retains its existing policy.
    pub order: Option<CallOrder>,
    pub parent: Option<crate::telemetry::TraceContext>,
}

pub(crate) static DO_CALL_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<DoCallReq>> =
    HostSlot::new(|channels| &channels.do_call);

pub fn set_do_call_tx(tx: tokio::sync::mpsc::UnboundedSender<DoCallReq>) {
    let _ = DO_CALL_TX.set(tx);
}

/// Ingress and application subrequests resolve ownership through one queue.
#[must_use]
pub fn submit_do_call(call: DoCallReq) -> bool {
    DO_CALL_TX.get().is_some_and(|tx| tx.send(call).is_ok())
}

fn call_cancels() -> Arc<Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>>> {
    crate::asyncrt::services().channels().cancels.clone()
}

/// A registration owns its exact process-wide request identity. Drop sends
/// cancellation; a completed reply disarms it and removes the registry entry.
pub struct CallCancelGuard {
    id: Option<RequestId>,
    cancels: Arc<Mutex<HashMap<RequestId, tokio::sync::oneshot::Sender<()>>>>,
}

pub fn begin_call_cancellation() -> (
    RequestId,
    tokio::sync::oneshot::Receiver<()>,
    CallCancelGuard,
) {
    let id = next_do_request_id();
    let (send, receive) = tokio::sync::oneshot::channel();
    let cancels = call_cancels();
    let previous = cancels.lock().unwrap().insert(id, send);
    assert!(
        previous.is_none(),
        "process-wide request identity was reused"
    );
    (
        id,
        receive,
        CallCancelGuard {
            id: Some(id),
            cancels,
        },
    )
}

/// Abort by the shared opaque token used by HTTP, service and fetch adapters.
/// Waking receivers and dropping their wakers never runs under the registry lock.
pub fn cancel_call(id: RequestId) -> bool {
    let sender = call_cancels().lock().unwrap().remove(&id);
    sender.is_some_and(|sender| sender.send(()).is_ok())
}

impl CallCancelGuard {
    pub fn disarm(&mut self) {
        if let Some(id) = self.id.take() {
            let sender = self.cancels.lock().unwrap().remove(&id);
            drop(sender);
        }
    }
}

impl Drop for CallCancelGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let sender = self.cancels.lock().unwrap().remove(&id);
            if let Some(sender) = sender {
                let _ = sender.send(());
            }
        }
    }
}

/// A ticket asking the actor whether an outbound effect may leave the process.
///
/// Every in-handler channel takes one: `fetch`, a service binding, a call to
/// another cell, and a frame on a socket the isolate opened. `position` is
/// present when the running event wrote through it, absent when the event only
/// read and the effect must trail whatever the cell already has outstanding.
pub struct GateReq {
    pub scope: String,
    /// The route the held effect would leave by, the position it must see
    /// proven, and the epoch that position was sampled at. The sample happens
    /// in the handler's turn, before `dispatch_gate` acquires the request that
    /// pins the cell; a reset in between discards the sampled write and the
    /// request would activate the next epoch, so the core refuses a ticket
    /// whose epoch is not the resident one. The core stores the route and
    /// hands it back, so the shell can route the release to the adapter
    /// holding the effect.
    pub ticket: crate::actor::GateTicket,
    pub reply: tokio::sync::oneshot::Sender<Result<(), cellar_logic::RequestError>>,
}

pub(crate) static GATE_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<GateReq>> =
    HostSlot::new(|channels| &channels.gate);

/// A facet's stream, asked for by the isolate that runs the facet: a facet
/// is a database and a replication stream of its own
/// (`crate::facet_streams`), which only the runtime can activate, delete, or
/// prove.
#[doc(hidden)]
#[derive(Clone)]
pub struct FacetOpenAuthority {
    pub(crate) validate: std::sync::Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl FacetOpenAuthority {
    pub(crate) fn check(&self) -> Result<()> {
        (self.validate)()
    }
}

pub enum FacetReq {
    /// Activate the stream of the facet at `names` below `root`, and answer
    /// its database file and whether replication restored it.
    Open {
        authority: Option<FacetOpenAuthority>,
        root: String,
        epoch: u64,
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<(std::path::PathBuf, bool)>>,
    },
    /// Delete the facet's stream and every stream below it.
    Delete {
        root: String,
        epoch: u64,
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
    /// Bounded subtree cleanup for a durably closed source.
    DeleteBatch {
        root: String,
        epoch: u64,
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<bool>>,
    },
    /// Prove every committed write of the stream durable.
    Prove {
        stream: String,
        epoch: u64,
        reply: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

pub(crate) static FACET_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<FacetReq>> =
    HostSlot::new(|channels| &channels.facet);

pub fn set_facet_tx(tx: tokio::sync::mpsc::UnboundedSender<FacetReq>) {
    let _ = FACET_TX.set(tx);
}

pub fn set_gate_tx(tx: tokio::sync::mpsc::UnboundedSender<GateReq>) {
    let _ = GATE_TX.set(tx);
}

/// A service-binding call: `env.NAME.fetch()`. Unlike a Durable Object call
/// there is no identity to resolve — any isolate running `script` will do — so
/// the runtime hands this straight to that script's stateless isolate pool.
pub struct SvcCallReq {
    /// Fires when the caller's request signal aborts, so the router stops
    /// waiting on the target instead of leaving the call outstanding.
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    /// The application generation of the calling isolate. The target is
    /// resolved in that generation's service graph, so a caller built for
    /// one deployment never reaches a target from another.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    /// A named Worker entrypoint and its props, or the target's default export
    /// when absent. Keeping both values together prevents a route from losing
    /// the props that select its authority boundary.
    pub entrypoint: Option<crate::WorkerFetchEntrypoint>,
    pub url: String,
    pub method: String,
    pub body: RequestBody,
    /// Owns a streamed body until the target installs its request context.
    pub body_guard: RequestBodyGuard,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}

pub(crate) static SVC_CALL_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<SvcCallReq>> =
    HostSlot::new(|channels| &channels.service_call);

/// A Worker assets-binding call. The script name selects the immutable asset
/// index loaded for that Worker; unlike ingress this never falls back into the
/// Worker and therefore cannot recurse.
pub struct AssetCallReq {
    /// The calling runtime's application generation; see `SvcCallReq`.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub reply: tokio::sync::oneshot::Sender<Result<HttpResponse>>,
}

pub(crate) static ASSET_CALL_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<AssetCallReq>> =
    HostSlot::new(|channels| &channels.asset_call);

/// An RPC operation on a named `WorkerEntrypoint` of another script. A call's
/// arguments and every result cross as structured-clone bytes.
pub struct SvcRpcReq {
    /// Dropping or signalling the caller cancels an unfinished invocation.
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    /// The calling isolate's application generation; see `SvcCallReq`.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub entrypoint: String,
    /// Structured-clone bytes for the target entrypoint's `ctx.props`.
    pub props: Vec<u8>,
    pub operation: crate::WorkerRpcOperation,
    pub reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
}

pub(crate) static SVC_RPC_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<SvcRpcReq>> =
    HostSlot::new(|channels| &channels.service_rpc);

/// The persisted identity a consumer settlement must match for one message.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct QueueLeaseRef {
    pub message_id: String,
    pub seq: i64,
    pub generation: u64,
}

/// A durable broker batch released through its output gate.
pub struct QueueDispatchReq {
    pub scope: String,
    /// The dispatching queue cell's application generation; see
    /// `SvcCallReq`.
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub lease_id: String,
    pub leases: Vec<QueueLeaseRef>,
    pub batch: QueueBatch,
}

pub(crate) static QUEUE_DISPATCH_TX: HostSlot<
    tokio::sync::mpsc::UnboundedSender<QueueDispatchReq>,
> = HostSlot::new(|channels| &channels.queue_dispatch);
/// The fleet bucket used by KV values that are too large for a namespace cell.
///
/// The landed Blob binding established the safe pattern: a cloneable bucket
/// handle can live beside the asynchronous ops, so independent requests do not
/// queue behind one task that awaits every object-store operation in order.
static KV_BLOB_STORE: HostSlot<Bucket> = HostSlot::new(|channels| &channels.kv_blob);

pub fn set_kv_blob_store(store: crate::bucket::Bucket) {
    let _ = KV_BLOB_STORE.set(store);
}

pub(crate) fn kv_blob_store() -> std::result::Result<crate::bucket::Bucket, String> {
    KV_BLOB_STORE
        .get()
        .ok_or_else(|| "KV large values need a fleet bucket".to_string())
}

pub fn set_svc_rpc_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcRpcReq>) {
    let _ = SVC_RPC_TX.set(tx);
}

pub fn set_svc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<SvcCallReq>) {
    let _ = SVC_CALL_TX.set(tx);
}

pub fn set_queue_dispatch_tx(tx: tokio::sync::mpsc::UnboundedSender<QueueDispatchReq>) {
    let _ = QUEUE_DISPATCH_TX.set(tx);
}

pub fn set_asset_call_tx(tx: tokio::sync::mpsc::UnboundedSender<AssetCallReq>) {
    let _ = ASSET_CALL_TX.set(tx);
}

/// A native Durable Object RPC call (`stub.someMethod(...args)`).
/// Routing/activation is identical to fetch calls.
pub struct RpcCallReq {
    /// SDK submission ownership. Trusted host control requests use `None`.
    #[doc(hidden)]
    pub reservation: Option<RpcReservation>,
    #[doc(hidden)]
    pub parent_cancellation: Option<std::sync::Arc<crate::runtime::InvocationCancellation>>,
    #[doc(hidden)]
    pub signal: Option<cellar_native::web::AbortSignal>,
    /// Host-local monotonic deadline. Peers must receive a remaining duration,
    /// rather than this timestamp. `None` retains ordinary/event RPC policy.
    #[doc(hidden)]
    pub deadline_ms: Option<u64>,
    #[doc(hidden)]
    pub authority: Option<crate::runtime::RpcAuthority>,
    /// Correlates local execution and the authenticated peer abort protocol.
    pub request_id: Option<RequestId>,
    /// The caller's owned wait signals cancellation before or during dispatch.
    pub cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    pub scope: String,
    pub name: Option<String>,
    pub method: String,
    pub args: RpcData,
    pub reply: tokio::sync::oneshot::Sender<Result<RpcData>>,
}

pub(crate) static RPC_CALL_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<RpcCallReq>> =
    HostSlot::new(|channels| &channels.rpc_call);

pub fn set_rpc_call_tx(tx: tokio::sync::mpsc::UnboundedSender<RpcCallReq>) {
    let _ = RPC_CALL_TX.set(tx);
}

static OUTBOUND_WS_TX: HostSlot<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>> =
    HostSlot::new(|channels| &channels.outbound_ws);

pub fn set_outbound_ws_tx(tx: tokio::sync::mpsc::UnboundedSender<OutboundWsReq>) {
    let _ = OUTBOUND_WS_TX.set(tx);
}

#[cfg(cellar_internal_tests)]
/// An outbound connector scoped to the current internal-test JS thread.
///
/// The production connector is process-wide. Installing a test sender there
/// captures requests from unrelated suites, and those suites have no receiver
/// that can answer them. The thread-local sender follows the cell execution
/// harnesses, while this handle owns its receiver and removes the sender when
/// the case ends.
#[doc(hidden)]
pub struct TestOutboundWsConnector {
    requests: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<OutboundWsReq>>,
    /// A scoped connector must drop on the thread where it installed its sender.
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(cellar_internal_tests)]
impl TestOutboundWsConnector {
    #[doc(hidden)]
    pub fn requests(
        &self,
    ) -> &tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<OutboundWsReq>> {
        &self.requests
    }
}

#[cfg(cellar_internal_tests)]
thread_local! {
    static TEST_OUTBOUND_WS_TX: RefCell<Option<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>>> =
        const { RefCell::new(None) };
}

#[cfg(cellar_internal_tests)]
impl Drop for TestOutboundWsConnector {
    fn drop(&mut self) {
        TEST_OUTBOUND_WS_TX.with(|slot| {
            assert!(
                slot.borrow_mut().take().is_some(),
                "test outbound WebSocket connector was not installed",
            );
        });
    }
}

#[cfg(cellar_internal_tests)]
/// Install the internal-test connector for the current JS thread.
#[doc(hidden)]
pub fn install_outbound_ws_connector_for_test() -> TestOutboundWsConnector {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    TEST_OUTBOUND_WS_TX.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(
            slot.is_none(),
            "test outbound WebSocket connector is already installed"
        );
        *slot = Some(tx);
    });
    TestOutboundWsConnector {
        requests: tokio::sync::Mutex::new(rx),
        _not_send: std::marker::PhantomData,
    }
}

/// Select the scoped internal-test connector before the production connector.
#[allow(dead_code)]
pub(crate) fn outbound_ws_tx() -> Option<tokio::sync::mpsc::UnboundedSender<OutboundWsReq>> {
    #[cfg(cellar_internal_tests)]
    if let Some(tx) = TEST_OUTBOUND_WS_TX.with(|slot| slot.borrow().clone()) {
        return Some(tx);
    }
    OUTBOUND_WS_TX.get()
}

/// The fleet bucket, once a node has opened one. Installed beside the
/// wake-entry gate at startup; absent for a bucketless run.
pub(crate) static BLOB_STORE: HostSlot<Bucket> = HostSlot::new(|channels| &channels.blob);

/// Give the Blob bindings the fleet bucket to live in.
pub fn set_blob_store(bucket: Bucket) {
    let _ = BLOB_STORE.set(bucket);
}

/// Why the output gate did not release a ticket.
pub(crate) enum GateRefusal {
    /// The process installed no gate channel. A write must still fail closed
    /// here: an acknowledgement nobody can prove is the loss this gate exists
    /// to prevent.
    NoChannel,
    /// The core answered, and the answer is that the write is not durable.
    Unproven(cellar_logic::RequestError),
    /// The shell dropped the ticket before the core answered.
    Dropped,
    /// A facet's write never reached a database a proof can cover.
    Unpersisted(String),
}

impl std::fmt::Display for GateRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateRefusal::NoChannel => f.write_str("no output-gate channel"),
            GateRefusal::Unproven(error @ cellar_logic::RequestError::DurabilityUnproven) => {
                write!(
                    f,
                    "the write this request follows is not durable ({error:?})"
                )
            }
            GateRefusal::Unproven(error) => {
                write!(f, "the output gate refused the ticket ({error:?})")
            }
            GateRefusal::Dropped => f.write_str("output gate dropped"),
            GateRefusal::Unpersisted(error) => {
                write!(f, "the facet's write did not reach its database ({error})")
            }
        }
    }
}

/// Ask the host to prove the caller's sampled position before an effect leaves.
pub(crate) async fn request_gate(
    channel: Option<&tokio::sync::mpsc::UnboundedSender<GateReq>>,
    scope: String,
    ticket: crate::actor::GateTicket,
) -> std::result::Result<(), GateRefusal> {
    let (tx, receive) = tokio::sync::oneshot::channel();
    let sent = channel
        .map(|gate| {
            gate.send(GateReq {
                scope,
                ticket,
                reply: tx,
            })
            .is_ok()
        })
        .unwrap_or(false);
    if !sent {
        return Err(GateRefusal::NoChannel);
    }
    match receive.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(GateRefusal::Unproven(error)),
        Err(_) => Err(GateRefusal::Dropped),
    }
}

/// Submit only after the caller's durability future releases the effect.
/// Taking the sender after the await preserves host-channel installation order.
pub(crate) async fn send_after_gate<T>(
    gate: impl std::future::Future<Output = std::result::Result<(), String>>,
    channel: impl FnOnce() -> Option<tokio::sync::mpsc::UnboundedSender<T>>,
    request: T,
    missing: &'static str,
) -> std::result::Result<(), String> {
    gate.await?;
    match channel() {
        Some(tx) if tx.send(request).is_ok() => Ok(()),
        _ => Err(missing.to_string()),
    }
}
