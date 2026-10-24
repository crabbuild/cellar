// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Native holds on the owning host's input gate used by cell delivery.

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
    service: Arc<GateService>,
    cell: String,
    owner: u64,
}

impl Queued {
    fn new(service: Arc<GateService>, cell: String, owner: u64) -> Self {
        *service
            .cells
            .lock()
            .unwrap()
            .entry(cell.clone())
            .or_insert_with(|| CellGate::new(&service))
            .queued
            .entry(owner)
            .or_default() += 1;
        service.engagements.fetch_add(1, Ordering::Relaxed);
        Self {
            service,
            cell,
            owner,
        }
    }
}

impl Drop for Queued {
    fn drop(&mut self) {
        let mut gates = self.service.cells.lock().unwrap();
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
        self.service.engagements.fetch_sub(1, Ordering::Relaxed);
    }
}

impl InputGate {
    pub(crate) fn acquire(context: Arc<IoContext>, cell: String) -> Result<Arc<Self>> {
        let owner = context
            .continuation_id()
            .ok_or_else(|| anyhow!(RETIRED_INPUT_GATE))?;
        let service = context.host_services().gates();
        let event = service.next_event.fetch_add(1, Ordering::Relaxed);
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
                let queued = Queued::new(service, cell.clone(), owner);
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
                    &self.context,
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
            release_cell_gate(&self.context, &self.cell, self.event, outcome);
        }
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
mod library_ownership_tests {
    use super::*;
    use crate::asyncrt::ExecutionDomain;

    #[tokio::test]
    async fn queued_cleanup_uses_its_owner_after_the_domain_closes() {
        let first = ExecutionDomain::new(tokio::runtime::Handle::current());
        let second = ExecutionDomain::new(tokio::runtime::Handle::current());
        let first_service = first.services().gates();
        let second_service = second.services().gates();
        let first_queued = Queued::new(first_service.clone(), "Account:same".into(), 1);
        let second_queued = Queued::new(second_service.clone(), "Account:same".into(), 1);
        first.shutdown().await;
        second.run(async { drop(first_queued) }).await.unwrap();
        assert!(first_service.cells.lock().unwrap().is_empty());
        assert_eq!(first_service.engagements.load(Ordering::Relaxed), 0);
        assert_eq!(second_service.engagements.load(Ordering::Relaxed), 1);
        assert_eq!(
            second_service.cells.lock().unwrap()["Account:same"].queued[&1],
            1
        );
        drop(second_queued);
        assert!(second_service.cells.lock().unwrap().is_empty());
        assert_eq!(second_service.engagements.load(Ordering::Relaxed), 0);
        second.shutdown().await;
    }
}
