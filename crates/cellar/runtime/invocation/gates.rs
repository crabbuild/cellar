// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Host delivery, durability, cancellation and invocation resource gates.

use crate::engine_api::RequestId;
use crate::host_channels::{FacetReq, GateRefusal, FACET_TX, GATE_TX};
use crate::host_io::input_gate_lifecycle::CrossEntryGateClaim;
#[cfg(cellar_internal_tests)]
use crate::host_io::installed_context;
use crate::host_io::{EgressFrame, IoContext};
use crate::wake_entry::{launch_arm_gate, ArmGateRx};
use crate::ws_registry::*;
use crate::{asyncrt, storage};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Sample the source under its SQLite lock. A missing source cannot grant
/// publication or cleanup authority; the output gate fails closed for an arm.
pub(crate) fn observe_alarm(cell: &str, at_ms: Option<i64>) -> cellar_logic::wake::AlarmSnapshot {
    storage::alarm_snapshot(cell).unwrap_or_else(|error| {
        tracing::warn!(%cell, %error, "alarm observation has no committed wake source");
        cellar_logic::wake::AlarmSnapshot::without_wake(at_ms)
    })
}

/// A committed installation needs discovery coverage: launch the entry PUT
/// and register it against the current event's output gate. No-op when the
/// bound already covers it or no gate is configured.
pub(crate) fn spawn_arm_gate(cell: &str, at_ms: i64, context: Option<Arc<IoContext>>) {
    if let Some(rx) = launch_arm_gate(cell, observe_alarm(cell, Some(at_ms))) {
        register_arm_gate_with_current_event(rx, context);
    }
}

pub(crate) fn register_arm_gate_with_current_event(
    gate: ArmGateRx,
    context: Option<Arc<IoContext>>,
) {
    let Some(context) = context else {
        drop(gate);
        return;
    };
    if let Err(gate) = context.register_arm_gate(gate) {
        // The response boundary sealed this context. The PUT itself
        // continues, but its receiver cannot migrate into a later event's
        // reply batch.
        drop(gate);
    }
}

#[cfg(cellar_internal_tests)]
pub(crate) fn register_test_pending_arm_gate(cell: &str, gate: ArmGateRx) {
    let services = asyncrt::services();
    let mut pending = services.wake_entry().test_pending.lock().unwrap();
    let gates = pending.entry(cell.to_string()).or_default();
    gates.retain_mut(|gate| {
        matches!(
            gate.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        )
    });
    gates.push(gate);
}

#[cfg(all(test, cellar_internal_tests))]
pub(crate) fn spawn_observed_arm_gate_for_test(
    cell: &str,
    alarm: cellar_logic::wake::AlarmSnapshot,
) {
    let Some(gate) = launch_arm_gate(cell, alarm) else {
        return;
    };
    match installed_context() {
        Some(context) => register_arm_gate_with_current_event(gate, Some(context)),
        None => register_test_pending_arm_gate(cell, gate),
    }
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn pause_next_arm_gate_for_test() -> tokio::sync::oneshot::Sender<Result<(), String>> {
    let (resume, paused) = tokio::sync::oneshot::channel();
    asyncrt::services()
        .wake_entry()
        .scripted
        .lock()
        .unwrap()
        .push_back(paused);
    resume
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn pause_next_arm_gate_for_cell_for_test(
    cell: &str,
) -> tokio::sync::oneshot::Sender<Result<(), String>> {
    let (resume, paused) = tokio::sync::oneshot::channel();
    asyncrt::services()
        .wake_entry()
        .scripted_by_cell
        .lock()
        .unwrap()
        .entry(cell.to_string())
        .or_default()
        .push_back(paused);
    resume
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn next_arm_gate_is_paused_for_test() -> bool {
    !asyncrt::services()
        .wake_entry()
        .scripted
        .lock()
        .unwrap()
        .is_empty()
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn next_arm_gate_for_cell_is_paused_for_test(cell: &str) -> bool {
    asyncrt::services()
        .wake_entry()
        .scripted_by_cell
        .lock()
        .unwrap()
        .get(cell)
        .is_some_and(|gates| !gates.is_empty())
}

/// Drain the compatibility wake-entry PUT gates for a simulated S1 cell.
#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub async fn drain_arm_gates(cell: &str) -> Result<(), String> {
    let services = asyncrt::services();
    let gates = services
        .wake_entry()
        .test_pending
        .lock()
        .unwrap()
        .remove(cell);
    let Some(gates) = gates else {
        return Ok(());
    };
    await_arm_gates(gates).await
}

/// What an outbound effect must trail before it leaves the process.
///
/// The three cases are named rather than nested inside one another because
/// two of them once shared a representation and lost this distinction:
/// a reader that starts after another request committed samples the new
/// position as its own baseline and advances nothing, so a two-state gate read
/// it as code that owns no cell at all and let its side effect out ungated.
/// A variant per case makes the distinction one the compiler keeps.
pub(crate) enum EgressGate {
    /// No cell event is running. Stateless Worker code owns no cell state, so
    /// its egress reveals nothing that can still be lost and leaves directly.
    NoCell,
    /// This event wrote through the position. The effect waits for that
    /// write. The last field is the activation epoch the position belongs to.
    Wrote(String, cellar_logic::Channel, u64, Option<u64>),
    /// A read-only output. It reveals whatever the cell holds, so it carries
    /// the position it observed above the cell's published baseline, for the
    /// core to hold it behind the proof of a commit whose handler has not
    /// taken a ticket yet, and it trails the newest write barrier still open
    /// on the cell otherwise. Only the core knows which writes are
    /// outstanding, so this asks rather than guesses. The last field is the
    /// activation epoch the sample was taken at.
    ReadOnly(String, cellar_logic::Channel, Option<u64>, Option<u64>),
    /// A facet's storage failed. No proof can cover its write, so the effect
    /// fails closed. The field is the storage failure.
    Unpersisted(String),
    /// An effect of a facet that wrote: the facet's own stream proves the
    /// write before the root cell's gate verdicts the rest, as workerd's
    /// facet output gate holds the effect. Fields: the root cell's gate, the
    /// facet's stream, and the activation epoch.
    Facet(Box<EgressGate>, String, u64),
}

impl EgressGate {
    /// Whether the effect must consult the gate before it leaves. True for a
    /// read as well as a write: a read whose cell has no barrier is released
    /// at once, but only the core can say so.
    pub(crate) fn is_gated(&self) -> bool {
        !matches!(self, EgressGate::NoCell)
    }

    /// The cell whose event raised the effect. The host derives this from the
    /// active event, so JavaScript cannot claim another cell's authority.
    #[allow(dead_code)]
    pub(crate) fn cell_scope(&self) -> Option<&str> {
        match self {
            EgressGate::NoCell | EgressGate::Unpersisted(_) => None,
            EgressGate::Wrote(cell, ..) | EgressGate::ReadOnly(cell, ..) => Some(cell),
            EgressGate::Facet(root, ..) => root.cell_scope(),
        }
    }
}

/// The cell event the executing JavaScript belongs to.
///
/// The frame of the running event, if a cell event is running at all.
pub(crate) fn current_cell_event(context: &IoContext) -> Option<EgressFrame> {
    context.egress.lock().unwrap().last().cloned()
}

/// Return and clear the number of output-gate samples for one test cell.
#[cfg(all(test, cellar_internal_tests))]
pub(crate) fn take_egress_gate_samples_for_test(
    scope: &str,
    channel: cellar_logic::Channel,
) -> u64 {
    asyncrt::services()
        .wake_entry()
        .egress_gate_samples
        .lock()
        .unwrap()
        .remove(&(scope.to_string(), channel))
        .unwrap_or(0)
}

/// Sample what the running handler's outbound effects must trail. Answers for
/// every active cell event, including a read whose core lookup can settle
/// immediately. Every sample here happens in the calling turn; the ticket
/// carries them to the host loop, which must not reach cell storage itself.
pub(crate) fn egress_gate_request(
    context: &IoContext,
    channel: cellar_logic::Channel,
) -> EgressGate {
    if !context.host_services().accepts_work() {
        return EgressGate::Unpersisted("execution domain is closed".into());
    }
    let Some(frame) = current_cell_event(context) else {
        return EgressGate::NoCell;
    };
    #[cfg(all(test, cellar_internal_tests))]
    {
        let cell = frame
            .root
            .as_ref()
            .map(|root| root.cell.clone())
            .unwrap_or_else(|| frame.storage.clone());
        *asyncrt::services()
            .wake_entry()
            .egress_gate_samples
            .lock()
            .unwrap()
            .entry((cell, channel))
            .or_default() += 1;
    }
    let (cell, before) = (frame.storage, frame.before);
    let sample = match storage::write_position(&cell) {
        Ok(sample) => sample,
        Err(error) => return EgressGate::Unpersisted(error.to_string()),
    };
    if let Some(root) = frame.root {
        return facet_egress_gate(&cell, root, channel);
    }
    let epoch = storage::activation_epoch(&cell);
    match sample.filter(|position| *position > before) {
        Some(position) => EgressGate::Wrote(cell, channel, position, epoch),
        // A read-only output in a process that configured no output gate has
        // nothing to trail. Such a process cannot acknowledge a write either
        // -- `await_egress_gate` fails a write closed for want of the same
        // channel -- so no committed-but-unproven write exists for a read to
        // reveal. Answer as
        // if no cell event were running, which keeps the effect on the direct,
        // synchronous path it has always taken; a WebSocket frame in
        // particular must not join a deferred queue whose flush needs a gate
        // to release it. A write still takes a ticket and still fails closed.
        None if GATE_TX.get().is_none() => EgressGate::NoCell,
        None => {
            let observed = storage::observed_position(&cell, sample);
            EgressGate::ReadOnly(cell, channel, observed, epoch)
        }
    }
}

/// Sample an effect raised by an event of an embedded facet.
///
/// A facet is not a cell: no Worker exports its class under the id
/// `facet_scope` builds, so a ticket in the facet's name activates a cell
/// that cannot start. The effect can reveal what the root cell showed the
/// facet, so it trails the root cell's reader gate with the sample the parent
/// took for the call; a write of the facet's own is proved on its stream.
pub(crate) fn facet_egress_gate(
    facet: &str,
    root: storage::RootGate,
    channel: cellar_logic::Channel,
) -> EgressGate {
    if let Some(error) = storage::sql_critical_error(facet) {
        return EgressGate::Unpersisted(error);
    }
    // A process with no output gate has nothing to trail, and a WebSocket
    // frame must keep its direct path (see `egress_gate_request`).
    if GATE_TX.get().is_none() {
        return EgressGate::NoCell;
    }
    let gate = EgressGate::ReadOnly(root.cell, channel, root.observed, Some(root.epoch));
    // Any effect can reveal a facet write, this event's or an earlier one's,
    // so every effect waits for the facet's stream, as workerd's facet
    // output gate holds it.
    EgressGate::Facet(Box::new(gate), root.stream, root.epoch)
}

/// Prove a facet's stream through the host loop, which owns the replicator.
pub(crate) async fn prove_facet(
    stream: String,
    epoch: u64,
) -> std::result::Result<(), GateRefusal> {
    // No host loop means no replicator, so nothing can prove the write and
    // nothing can lose it to a later proof either.
    let Some(sender) = FACET_TX.get() else {
        return Ok(());
    };
    let (reply, receive) = tokio::sync::oneshot::channel();
    let request = FacetReq::Prove {
        stream,
        epoch,
        reply,
    };
    sender.send(request).map_err(|_| GateRefusal::Dropped)?;
    match receive.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(GateRefusal::Unpersisted(format!("{error:#}"))),
        Err(_) => Err(GateRefusal::Dropped),
    }
}

/// Take one ticket on the output gate and wait for the core's verdict.
///
/// This is the output gate applied to an in-handler effect rather than to the
/// response. It cannot deadlock the handler that is awaiting it: the ticket is
/// served by `dispatch_gate` on the host's own loop and resolved by the
/// replicator's independent task, neither of which needs the isolate's event
/// loop to run. A read-only ticket carries `None` and the core releases it at
/// once when the cell has no barrier open, so an ordinary read pays one actor
/// hop and no replica write.
pub(crate) async fn egress_gate_verdict(gate: EgressGate) -> std::result::Result<(), GateRefusal> {
    let services = asyncrt::services();
    if !services.accepts_work() {
        return Err(GateRefusal::Unpersisted(
            "execution domain is closed".into(),
        ));
    }
    let gate = match gate {
        EgressGate::Facet(root, stream, epoch) => {
            prove_facet(stream, epoch).await?;
            if !services.accepts_work() {
                return Err(GateRefusal::Unpersisted(
                    "execution domain is closed".into(),
                ));
            }
            *root
        }
        gate => gate,
    };
    let (cell, channel, position, observed, epoch) = match gate {
        EgressGate::Facet(..) => unreachable!("a facet gate nests no facet gate"),
        EgressGate::NoCell => return Ok(()),
        EgressGate::Unpersisted(error) => return Err(GateRefusal::Unpersisted(error)),
        EgressGate::Wrote(cell, channel, position, epoch) => {
            (cell, channel, Some(position), None, epoch)
        }
        EgressGate::ReadOnly(cell, channel, observed, epoch) => {
            (cell, channel, None, observed, epoch)
        }
    };
    crate::host_channels::request_gate(
        GATE_TX.get().as_ref(),
        cell,
        crate::actor::GateTicket {
            channel,
            position,
            observed,
            epoch,
        },
    )
    .await
}

/// Wait for the writes an outbound effect can reveal to be proven durable
/// before it leaves the process. See `egress_gate_verdict`.
pub(crate) async fn await_egress_gate(gate: EgressGate) -> std::result::Result<(), String> {
    egress_gate_verdict(gate)
        .await
        .map_err(|refusal| match refusal {
            GateRefusal::Unproven(_) | GateRefusal::Unpersisted(_) => {
                format!("refusing to send: {refusal}")
            }
            GateRefusal::NoChannel | GateRefusal::Dropped => refusal.to_string(),
        })
}

/// Hold an outbound request until the write it follows is durable, then hand
/// it to the host.
///
/// The send moves inside the future deliberately. These channels dispatch as
/// soon as `send` is called, so gating anywhere after it would let the effect
/// leave first -- the request would already be on its way while the caller
/// waited for a durability answer that no longer decides anything.
pub(crate) async fn gated_channel_send<T>(
    gate: EgressGate,
    channel: &'static crate::host_channels::HostSlot<tokio::sync::mpsc::UnboundedSender<T>>,
    request: T,
    missing: &'static str,
) -> std::result::Result<(), String> {
    crate::host_channels::send_after_gate(
        await_egress_gate(gate),
        || channel.get(),
        request,
        missing,
    )
    .await
}

/// Input gates and request cancellation belong to one execution domain.
/// Identical cell scopes in separate hosts must never share a holder or waiter.
pub(crate) struct GateService {
    pub(crate) cells: Mutex<HashMap<String, CellGate>>,
    waiters: Mutex<HashMap<String, Vec<GateWake>>>,
    pub(crate) engagements: Arc<AtomicUsize>,
    pub(crate) next_event: AtomicU64,
    cancelled: Mutex<HashSet<RequestId>>,
    pending: Mutex<HashSet<RequestId>>,
    shutdown: Mutex<HashSet<RequestId>>,
    pending_count: AtomicUsize,
}

impl Default for GateService {
    fn default() -> Self {
        Self {
            cells: Mutex::new(HashMap::new()),
            waiters: Mutex::new(HashMap::new()),
            engagements: Arc::new(AtomicUsize::new(0)),
            next_event: AtomicU64::new(1),
            cancelled: Mutex::new(HashSet::new()),
            pending: Mutex::new(HashSet::new()),
            shutdown: Mutex::new(HashSet::new()),
            pending_count: AtomicUsize::new(0),
        }
    }
}

/// One cell's input gate, with the event that holds it and the events queued
/// to take it, named by the continuation id of their `IoContext`.
///
/// The critical section binds this owner to its continuation's operation
/// driver before acquisition. An ordinary continuation belongs to its origin;
/// a critical section retains the driver that started it, even when a foreign
/// checkpoint resumes its callback. The origin claim preserves the separate
/// resource context. Recording only the ambient turn here lets that event
/// retire while another driver polls the acquisition, rejecting a live block.
pub(crate) struct CellGate {
    engagements: Arc<AtomicUsize>,
    pub(crate) gate: cellar_logic::gate::InputGate,
    pub(crate) holder: Option<u64>,
    /// A reaction can belong to an event other than the turn owner. The claim
    /// keeps that originating event and its resources active until the block
    /// ends. A same-event block needs no claim because its `InFlight` owns the
    /// gate itself.
    pub(crate) origin: Option<CrossEntryGateClaim>,
    /// Counted, because one event can start several blocks at once.
    pub(crate) queued: HashMap<u64, u32>,
}

impl CellGate {
    pub(crate) fn new(service: &GateService) -> Self {
        Self {
            engagements: service.engagements.clone(),
            gate: cellar_logic::gate::InputGate::default(),
            holder: None,
            origin: None,
            queued: HashMap::new(),
        }
    }

    pub(crate) fn take(
        &mut self,
        event: cellar_logic::gate::EventId,
        owner: Option<u64>,
        origin: &mut Option<CrossEntryGateClaim>,
    ) -> bool {
        let was_open = self.gate.is_open();
        let taken = self.gate.acquire(event);
        if taken && was_open {
            self.set_holder(owner, origin.take());
        }
        taken
    }

    pub(crate) fn set_holder(&mut self, owner: Option<u64>, origin: Option<CrossEntryGateClaim>) {
        if self.holder.is_none() && owner.is_some() {
            self.engagements.fetch_add(1, Ordering::Relaxed);
        }
        self.holder = owner;
        self.origin = origin;
    }

    pub(crate) fn clear_holder(&mut self) -> Option<CrossEntryGateClaim> {
        if self.holder.take().is_some() {
            self.engagements.fetch_sub(1, Ordering::Relaxed);
        }
        self.origin.take()
    }

    pub(crate) fn release(
        &mut self,
        event: cellar_logic::gate::EventId,
    ) -> (bool, Option<CrossEntryGateClaim>) {
        if !self.gate.release(event) {
            return (false, None);
        }
        (true, self.clear_holder())
    }

    pub(crate) fn abandon(
        &mut self,
        event: cellar_logic::gate::EventId,
    ) -> (bool, Option<CrossEntryGateClaim>) {
        if !self.gate.abandon(event) {
            return (false, None);
        }
        (true, self.clear_holder())
    }

    pub(crate) fn is_unused(&self) -> bool {
        self.gate.is_open() && self.queued.is_empty()
    }
}

/// How the event running under `context` is engaged with the cells' gates:
/// it holds one, it is queued to take one, or neither.
///
/// Every gate is searched, not only the gate of the event's own cell: a
/// reaction of cell A's event can run inside cell B's checkpoint, and the
/// block it starts then belongs to A's gate while its ops belong to B's
/// entry. B's entry is the one that must keep them, and it finds itself
/// through its own id whichever gate names it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum GateEngagement {
    Holds,
    Queued,
    None,
}

pub(crate) fn gate_engagement(cell: Option<&str>, context: &Arc<IoContext>) -> GateEngagement {
    let service = context.host_services().gates();
    if service.engagements.load(Ordering::Relaxed) == 0 {
        return GateEngagement::None;
    }
    let Some(id) = context.continuation_id() else {
        return GateEngagement::None;
    };

    fn of(gate: &CellGate, id: u64) -> GateEngagement {
        if !gate.gate.is_open() && gate.holder == Some(id) {
            GateEngagement::Holds
        } else if gate.queued.contains_key(&id) {
            GateEngagement::Queued
        } else {
            GateEngagement::None
        }
    }
    let gates = service.cells.lock().unwrap();
    // The event's own cell first: nearly every block is on it, and the walk
    // below is the price of the cross-cell case alone.
    if let Some(gate) = cell.and_then(|cell| gates.get(cell)) {
        let engagement = of(gate, id);
        if engagement != GateEngagement::None {
            return engagement;
        }
    }
    let mut engagement = GateEngagement::None;
    for gate in gates.values() {
        match of(gate, id) {
            GateEngagement::Holds => return GateEngagement::Holds,
            GateEngagement::Queued => engagement = GateEngagement::Queued,
            GateEngagement::None => {}
        }
    }
    engagement
}

/// What a waiting event is told when the gate finally opens: nothing, or
/// the reason the holder failed.
pub(crate) type GateWake = tokio::sync::oneshot::Sender<Result<(), String>>;

/// Take a ticket to be woken when `cell`'s gate opens, or `None` if it is
/// open now.
///
/// The gate's lock is held across the check and the enqueue, so a release
/// that lands between the two cannot leave an event waiting on a gate that
/// is already open.
pub fn cell_gate_wait(cell: &str) -> Option<tokio::sync::oneshot::Receiver<Result<(), String>>> {
    cell_gate_wait_in(&asyncrt::services().gates(), cell)
}

pub(crate) fn cell_gate_wait_in(
    service: &GateService,
    cell: &str,
) -> Option<tokio::sync::oneshot::Receiver<Result<(), String>>> {
    let gates = service.cells.lock().unwrap();
    if gates.get(cell).is_none_or(|gate| gate.gate.is_open()) {
        return None;
    }
    let (wake, waiter) = tokio::sync::oneshot::channel();
    service
        .waiters
        .lock()
        .unwrap()
        .entry(cell.to_string())
        .or_default()
        .push(wake);
    drop(gates);
    Some(waiter)
}

pub(crate) enum CellGateAcquisition {
    Acquired,
    Waiting {
        gate: tokio::sync::oneshot::Receiver<Result<(), String>>,
        retirement: tokio::sync::watch::Receiver<bool>,
    },
    Retired,
}

/// Acquire `cell` for `event`, or enqueue its next attempt before the gate can
/// change state.
///
/// A failed holder wakes its existing waiters with an error. Separating the
/// failed acquisition from waiter registration lets that failure land between
/// the two, so the event can miss the error and run against the reset actor.
/// The context lock also covers the successful host claim, so event retirement
/// cannot land between acquiring the process gate and recording its owner.
pub(crate) fn acquire_cell_gate(
    context: &IoContext,
    cell: &str,
    event: cellar_logic::gate::EventId,
    owner: Option<u64>,
    origin: &mut Option<CrossEntryGateClaim>,
) -> CellGateAcquisition {
    let mut claims = context.input_gates.lock().unwrap();
    if claims.retired || !context.host_services().accepts_work() {
        return CellGateAcquisition::Retired;
    }
    let service = context.host_services().gates();
    let mut gates = service.cells.lock().unwrap();
    if gates
        .entry(cell.to_string())
        .or_insert_with(|| CellGate::new(&service))
        .take(event, owner, origin)
    {
        let previous = claims.held.insert(event, cell.to_string());
        assert!(previous.is_none(), "an input-gate event id was reused");
        return CellGateAcquisition::Acquired;
    }
    let (wake, waiter) = tokio::sync::oneshot::channel();
    service
        .waiters
        .lock()
        .unwrap()
        .entry(cell.to_string())
        .or_default()
        .push(wake);
    let retirement = claims.retirement.subscribe();
    drop(gates);
    CellGateAcquisition::Waiting {
        gate: waiter,
        retirement,
    }
}

/// Wake everything waiting on `cell`'s gate. Each re-checks and re-queues if
/// another block took the gate first, so waking all of them is correct and
/// the order they resume in is theirs to lose, not this function's to keep.
fn wake_gate_waiters(service: &GateService, cell: &str, outcome: Result<(), String>) {
    let waiters = service.waiters.lock().unwrap().remove(cell);
    for wake in waiters.into_iter().flatten() {
        let _ = wake.send(outcome.clone());
    }
}

pub(crate) const ABANDONED_INPUT_GATE: &str = "the cell's critical section ended without releasing";
pub(crate) const RETIRED_INPUT_GATE: &str =
    "the cell event ended before it could acquire an input gate";

/// Release one completed block in the host that acquired it.
pub(crate) fn release_cell_gate(
    context: &IoContext,
    cell: &str,
    event: cellar_logic::gate::EventId,
    outcome: Result<(), String>,
) {
    let service = context.host_services().gates();
    let origin = {
        let mut gates = service.cells.lock().unwrap();
        let Some(gate) = gates.get_mut(cell) else {
            return;
        };
        let (opened, origin) = gate.release(event);
        // A stale release or a remaining nested hold cannot wake the waiters.
        // In particular, a stale error must not fail a replacement holder's
        // queued events.
        if !opened {
            return;
        }
        if gate.is_unused() {
            gates.remove(cell);
        }
        origin
    };
    // Releasing the origin claim can wake its drive and eventually run
    // `IoContext::drop`, which also locks the gate map. Keep that work outside
    // the map's lock.
    drop(origin);
    wake_gate_waiters(&service, cell, outcome);
}

/// Retire `context`'s waiters and abandon every input gate that it still owns.
///
/// The event id is part of each claim. A concurrent event can already be
/// running when this one dies, so a cell name alone cannot safely decide
/// which holder to release. Cleanup uses the context's captured host even when
/// its last owner is dropped in another execution domain.
pub(crate) fn abandon_context_input_gates(
    context: &IoContext,
) -> Vec<(String, cellar_logic::gate::EventId)> {
    let service = context.host_services().gates();
    let claims = context.retire_input_gates();
    for (cell, event) in &claims {
        let (abandoned, origin) = {
            let mut gates = service.cells.lock().unwrap();
            let (abandoned, origin) = gates
                .get_mut(cell)
                .map_or((false, None), |gate| gate.abandon(*event));
            if abandoned {
                gates.remove(cell);
            }
            (abandoned, origin)
        };
        // See `release_cell_gate`: releasing an origin can re-enter this map
        // from its eventual context destructor.
        drop(origin);
        if abandoned {
            wake_gate_waiters(&service, cell, Err(ABANDONED_INPUT_GATE.to_string()));
        }
    }
    claims
}

#[cfg(all(test, cellar_internal_tests))]
pub(crate) type AbortRequestPauseForTest = (
    RequestId,
    std::sync::mpsc::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[cfg(all(test, cellar_internal_tests))]
static ABORT_REQUEST_PAUSE_FOR_TEST: Mutex<Option<AbortRequestPauseForTest>> = Mutex::new(None);

#[cfg(all(test, cellar_internal_tests))]
pub(crate) struct AbortRequestPauseHandleForTest {
    pub(crate) entered: std::sync::mpsc::Receiver<()>,
    pub(crate) release: Option<std::sync::mpsc::Sender<()>>,
}

#[cfg(all(test, cellar_internal_tests))]
impl AbortRequestPauseHandleForTest {
    pub(crate) fn wait_until_entered(&self) {
        self.entered
            .recv()
            .expect("the abort publisher dropped before its publication seam");
    }

    pub(crate) fn release(mut self) {
        self.release
            .take()
            .expect("the abort publication pause was released twice")
            .send(())
            .expect("the abort publisher dropped while paused");
    }
}

#[cfg(all(test, cellar_internal_tests))]
pub(crate) fn pause_abort_request_for_test(
    request_id: RequestId,
) -> AbortRequestPauseHandleForTest {
    let (entered_tx, entered) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let previous = ABORT_REQUEST_PAUSE_FOR_TEST
        .lock()
        .unwrap()
        .replace((request_id, entered_tx, release_rx));
    assert!(
        previous.is_none(),
        "an abort publication pause is already armed"
    );
    AbortRequestPauseHandleForTest {
        entered,
        release: Some(release),
    }
}

#[cfg(all(test, cellar_internal_tests))]
pub(crate) fn pause_abort_request_if_armed_for_test(request_id: RequestId) {
    let pause = {
        let mut pause = ABORT_REQUEST_PAUSE_FOR_TEST.lock().unwrap();
        if pause
            .as_ref()
            .is_some_and(|(expected, _, _)| *expected == request_id)
        {
            pause.take()
        } else {
            None
        }
    };
    if let Some((_, entered, release)) = pause {
        entered
            .send(())
            .expect("the abort publication test dropped its observer");
        release
            .recv()
            .expect("the abort publication test dropped its release");
    }
}

#[cfg(all(test, cellar_internal_tests))]
pub(crate) fn request_cancellation_state_for_test(request_id: RequestId) -> (bool, bool) {
    let service = asyncrt::services().gates();
    (
        service.pending.lock().unwrap().contains(&request_id),
        service.cancelled.lock().unwrap().contains(&request_id),
    )
}

impl GateService {
    pub(crate) fn abort_request(&self, request_id: RequestId) {
        let mut pending = self.pending.lock().unwrap();
        if pending.insert(request_id) {
            self.pending_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn clear_request_cancellation(&self, request_id: RequestId) {
        let _ = self.take_pending_abort(request_id);
        self.cancelled.lock().unwrap().remove(&request_id);
        self.shutdown.lock().unwrap().remove(&request_id);
    }

    fn take_pending_abort(&self, request_id: RequestId) -> bool {
        if self.pending_count.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let mut pending = self.pending.lock().unwrap();
        if pending.remove(&request_id) {
            self.pending_count.fetch_sub(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub(crate) fn take_request_cancellation(&self, request_id: Option<RequestId>) -> bool {
        request_id.is_some_and(|request_id| {
            self.cancelled.lock().unwrap().remove(&request_id)
                || self.take_pending_abort(request_id)
        })
    }

    pub(crate) fn take_shutdown_cancellation(&self, request_id: Option<RequestId>) -> bool {
        request_id.is_some_and(|request_id| self.shutdown.lock().unwrap().remove(&request_id))
    }
}

/// Mark an in-flight request cancelled in the currently entered host domain.
/// Publishers on arbitrary application threads should retain the request's
/// `RequestCancellationLifetime`, which captures the owning host.
pub fn abort_request(request_id: RequestId) {
    #[cfg(all(test, cellar_internal_tests))]
    pause_abort_request_if_armed_for_test(request_id);
    asyncrt::services().gates().abort_request(request_id);
}

/// Force a cell event to retire for a lifecycle transition. Unlike a client
/// disconnect, shutdown cannot preserve `waitUntil` work because the runtime
/// and its ownership are about to leave this host.
pub fn abort_request_for_shutdown(request_id: RequestId) {
    let service = asyncrt::services().gates();
    service.shutdown.lock().unwrap().insert(request_id);
    service.abort_request(request_id);
}

#[cfg(cellar_internal_tests)]
pub(crate) fn clear_request_cancellation(request_id: RequestId) {
    asyncrt::services()
        .gates()
        .clear_request_cancellation(request_id);
}

pub fn take_shutdown_cancellation(request_id: Option<RequestId>) -> bool {
    asyncrt::services()
        .gates()
        .take_shutdown_cancellation(request_id)
}

#[cfg(cellar_internal_tests)]
#[doc(hidden)]
pub fn clear_request_cancellation_for_test(request_id: RequestId) {
    clear_request_cancellation(request_id);
}

pub fn take_request_cancellation(request_id: Option<RequestId>) -> bool {
    asyncrt::services()
        .gates()
        .take_request_cancellation(request_id)
}

/// Close every isolate-polled socket a finished request opened.
///
/// The close frame is what actually ends the connection: the connector task
/// writes it to the wire and stops pumping, and only then does it drop the
/// `WsPull` sender that this socket's `__ws_next` is waiting on. Unregistering
/// first would strand the task instead — it detects a dead isolate by the
/// send failing, and nothing else tells it to go.
pub(crate) fn ws_close_request_sockets(context: &IoContext, opened: Vec<u64>) {
    for id in opened {
        let registry = ws_registry();
        let handoff = registry.lock().unwrap().worker_handoffs.remove(&id);
        drop(handoff);
        // A socket whose remote already hung up has no output sender left,
        // and that is the ordinary case rather than an error. `ws_emit` logs
        // a dropped frame for an unknown id, so ask before sending.
        let open = ws_registry().lock().unwrap().outputs.contains_key(&id);
        if open {
            // Retirement can run after its turn guard restores a predecessor.
            // These sockets belong to this owner, including its output gate.
            ws_emit_uncaptured(context, id, WsOut::Close(1001, "request ended".into()));
        }
        ws_pull_unregister(id);
        ws_unregister(id);
    }
}

pub(crate) fn ws_emit_uncaptured(context: &IoContext, id: u64, out: WsOut) {
    // Uncaptured socket output cannot wait for the handler to
    // return, and this handler may be awaiting a reply to the very frame being
    // held. Waiting on the DURABILITY ticket instead has no such cycle -- it
    // is resolved by the replicator, which does not need this event loop -- so
    // the frame can be held without deadlocking the script that sent it.
    let gate = egress_gate_request(context, ws_channel(id));
    // Resolved here, on the thread the frame was sent from; the registry is
    // moved into the flush below. The flush runs on a runtime that has no
    // services of its own, so a lookup inside it would answer with whichever
    // instance that runtime reaches — the socket's frames and the socket's
    // registry could then come from two different instances.
    let flush = ws_flush_state();
    let registry = ws_registry();
    // Gated for a read-only frame as well, and deliberately: the frame reveals
    // what the cell holds, so it has to ask the core whether a barrier is open
    // rather than assume its own event opened one. A cell with nothing
    // outstanding answers at once and the queue flushes on the same tick.
    let Some(flushing) =
        flush.emit_or_defer(&registry, std::iter::once((id, out)), gate.is_gated())
    else {
        return;
    };
    // Detached onto the HOST runtime (`op_handle`) deliberately: this flush
    // exists precisely to outlive the dispatch that produced the frames — a
    // writing connect handler's ready frame, an alarm broadcast — and it
    // awaits a durability ticket that resolves with no isolate involvement.
    // Both region-owned homes silently kill it: `asyncrt::enqueue`'s future is
    // aborted when the dispatch region closes, and the isolate thread's local
    // request driver stops polling its operation future the moment the
    // dispatch returns. Either way the frames never left the process.
    asyncrt::spawn(async move {
        let held = await_egress_gate(gate).await;
        flushing.release(&registry, held);
    })
    .detach();
}

#[cfg(all(test, not(cellar_internal_tests)))]
mod library_ownership_tests {
    use super::*;
    use crate::asyncrt::ExecutionDomain;

    #[tokio::test]
    async fn context_cleanup_in_another_domain_only_abandons_its_own_gate() {
        let first = ExecutionDomain::new(tokio::runtime::Handle::current());
        let second = ExecutionDomain::new(tokio::runtime::Handle::current());
        let first_context = first.run(async { IoContext::new() }).await.unwrap();
        let second_context = second.run(async { IoContext::new() }).await.unwrap();
        // The event and cell identities deliberately collide in the two hosts.
        for context in [&first_context, &second_context] {
            assert!(matches!(
                acquire_cell_gate(context, "Account:same", 1, Some(1), &mut None),
                CellGateAcquisition::Acquired
            ));
        }
        let first_service = first.services().gates();
        let second_service = second.services().gates();
        let first_waiter = cell_gate_wait_in(&first_service, "Account:same").unwrap();
        let mut second_waiter = cell_gate_wait_in(&second_service, "Account:same").unwrap();
        second.run(async { drop(first_context) }).await.unwrap();
        assert_eq!(
            first_waiter.await.unwrap(),
            Err(ABANDONED_INPUT_GATE.into())
        );
        assert_eq!(first_service.engagements.load(Ordering::Relaxed), 0);
        assert_eq!(second_service.engagements.load(Ordering::Relaxed), 1);
        assert!(matches!(
            second_waiter.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        // A completed hold also releases its captured owner, regardless of
        // which host is currently entered.
        first
            .run(async {
                assert!(second_context.release_input_gate("Account:same", 1));
                release_cell_gate(&second_context, "Account:same", 1, Ok(()));
            })
            .await
            .unwrap();
        assert_eq!(second_waiter.await.unwrap(), Ok(()));
        assert_eq!(second_service.engagements.load(Ordering::Relaxed), 0);
        first.shutdown().await;
        second.shutdown().await;
    }

    #[tokio::test]
    async fn abort_publication_and_retirement_keep_the_captured_host() {
        let first = ExecutionDomain::new(tokio::runtime::Handle::current());
        let second = ExecutionDomain::new(tokio::runtime::Handle::current());
        let lifetime = first
            .run(async { crate::runtime::RequestCancellationLifetime::stateless() })
            .await
            .unwrap();
        let id = lifetime.request_id();
        second
            .run(async {
                lifetime.publish_abort();
                assert!(!take_request_cancellation(Some(id)));
            })
            .await
            .unwrap();
        assert!(first
            .run(async { take_request_cancellation(Some(id)) })
            .await
            .unwrap());
        // Same request id in two registries: finishing A from B clears only A.
        second
            .run(async {
                abort_request_for_shutdown(id);
                lifetime.publish_abort();
                lifetime.finish();
                assert!(take_request_cancellation(Some(id)));
                assert!(take_shutdown_cancellation(Some(id)));
            })
            .await
            .unwrap();
        assert!(!first
            .run(async { take_request_cancellation(Some(id)) })
            .await
            .unwrap());
        first.shutdown().await;
        lifetime.publish_abort();
        assert!(!first.services().gates().take_request_cancellation(Some(id)));
        second.shutdown().await;
    }

    #[tokio::test]
    async fn closed_domains_cannot_turn_missing_output_channels_into_ungated_effects() {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let context = domain.run(async { IoContext::new() }).await.unwrap();
        domain
            .run(async {
                domain.close();
                assert!(matches!(
                    egress_gate_request(&context, cellar_logic::Channel::Service),
                    EgressGate::Unpersisted(_)
                ));
                assert!(egress_gate_verdict(EgressGate::NoCell).await.is_err());
            })
            .await
            .unwrap();
        domain.shutdown().await;
    }
}
