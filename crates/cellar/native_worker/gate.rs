// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Native holds on the same process input gate used by cell delivery.

use super::*;
use futures_util::future::{BoxFuture, FutureExt, Shared};
use std::sync::atomic::AtomicBool;

pub(crate) type Ready = Shared<BoxFuture<'static, Result<(), String>>>;

pub(crate) struct InputGate {
    context: Arc<IoContext>,
    cell: String,
    event: cellar_logic::gate::EventId,
    held: Arc<AtomicBool>,
    failure: Mutex<Option<String>>,
    retirement: tokio::sync::watch::Sender<bool>,
    pub(crate) ready: Ready,
}

struct Queued {
    cell: String,
    owner: u64,
}

impl Queued {
    fn new(cell: String, owner: u64) -> Self {
        *cell_gates()
            .lock()
            .unwrap()
            .entry(cell.clone())
            .or_default()
            .queued
            .entry(owner)
            .or_default() += 1;
        GATE_ENGAGEMENTS.fetch_add(1, Ordering::Relaxed);
        Self { cell, owner }
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        let mut gates = cell_gates().lock().unwrap();
        let remove = gates.get_mut(&self.cell).is_some_and(|gate| {
            if let Some(count) = gate.queued.get_mut(&self.owner) {
                *count -= 1;
                if *count == 0 {
                    gate.queued.remove(&self.owner);
                }
            }
            gate.is_unused()
        });
        if remove {
            gates.remove(&self.cell);
        }
        GATE_ENGAGEMENTS.fetch_sub(1, Ordering::Relaxed);
    }
}

impl InputGate {
    pub(crate) fn acquire(context: Arc<IoContext>, cell: String) -> Result<Arc<Self>> {
        let owner = context
            .continuation_id()
            .ok_or_else(|| anyhow!(RETIRED_INPUT_GATE))?;
        let event = NEXT_GATE_EVENT.fetch_add(1, Ordering::Relaxed);
        let held = Arc::new(AtomicBool::new(false));
        let acquired = acquire_cell_gate(&context, &cell, event, Some(owner), &mut None);
        let ready: BoxFuture<'static, Result<(), String>> = match acquired {
            CellGateAcquisition::Acquired => {
                held.store(true, Ordering::Release);
                Box::pin(async { Ok(()) })
            }
            CellGateAcquisition::Retired => anyhow::bail!(RETIRED_INPUT_GATE),
            CellGateAcquisition::Waiting {
                mut gate,
                mut retirement,
            } => {
                let queued = Queued::new(cell.clone(), owner);
                let context = context.clone();
                let cell = cell.clone();
                let held = held.clone();
                Box::pin(async move {
                    let _queued = queued;
                    loop {
                        let outcome = crate::asyncrt::select_biased! {
                            "a failed gate remains visible when its queued event retires";
                            outcome = &mut gate => outcome.map_err(|_| "cell stopped while waiting for its input gate".to_string())?,
                            retired = retirement.wait_for(|retired| *retired) => {
                                let _ = retired;
                                return Err(RETIRED_INPUT_GATE.to_string());
                            }
                        };
                        outcome?;
                        match acquire_cell_gate(&context, &cell, event, Some(owner), &mut None) {
                            CellGateAcquisition::Acquired => {
                                held.store(true, Ordering::Release);
                                return Ok(());
                            }
                            CellGateAcquisition::Retired => {
                                return Err(RETIRED_INPUT_GATE.to_string())
                            }
                            CellGateAcquisition::Waiting {
                                gate: next,
                                retirement: next_retirement,
                            } => {
                                gate = next;
                                retirement = next_retirement;
                            }
                        }
                    }
                })
            }
        };
        let input_gate = Arc::new(Self {
            context,
            cell,
            event,
            held,
            failure: Mutex::new(None),
            retirement: tokio::sync::watch::channel(false).0,
            ready: ready.shared(),
        });
        if let Some(activation) = &input_gate.context.native_activation {
            let mut gates = activation.resources.input_gates.lock().unwrap();
            gates.retain(|gate| gate.strong_count() != 0);
            gates.push(Arc::downgrade(&input_gate));
        }
        Ok(input_gate)
    }

    pub(crate) fn retire(&self) {
        self.fail("native cell activation has retired".into());
        self.retirement.send_replace(true);
        let _ = abandon_context_input_gates(&self.context);
    }

    pub(crate) fn retirement(&self) -> tokio::sync::watch::Receiver<bool> {
        self.retirement.subscribe()
    }

    pub(crate) fn fail(&self, reason: String) {
        let mut failure = self.failure.lock().unwrap();
        if failure.is_none() {
            *failure = Some(reason.clone());
        }
        // An acquisition refused by the preceding holder never ran a block.
        // It must not reset a replacement cell.
        if self.held.load(Ordering::Acquire) {
            if let Some(activation) = &self.context.native_activation {
                let mut reset = activation.resources.failure.lock().unwrap();
                if reset.is_none() {
                    *reset = Some(reason);
                }
            }
            // A failed block resets the instance immediately. Pending sibling
            // work must not delay refusal of events behind the failed hold.
            if self.held.swap(false, Ordering::AcqRel)
                && self.context.release_input_gate(&self.cell, self.event)
            {
                release_cell_gate(
                    &self.cell,
                    self.event,
                    Err(failure.as_ref().unwrap().clone()),
                );
            }
        }
    }
}

impl Drop for InputGate {
    fn drop(&mut self) {
        if self.held.load(Ordering::Acquire)
            && self.context.release_input_gate(&self.cell, self.event)
        {
            let outcome = self.failure.get_mut().unwrap().take().map_or(Ok(()), Err);
            release_cell_gate(&self.cell, self.event, outcome);
        }
    }
}
