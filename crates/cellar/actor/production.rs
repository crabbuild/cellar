// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! The production selector for the Actor execution domain.

use super::*;
use futures_util::StreamExt as _;

impl Actor {
    pub(super) fn ensure_owner(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.host_services.accepts_work()
                && Arc::ptr_eq(&self.host_services, &crate::asyncrt::services()),
            "actor belongs to another or closed execution domain"
        );
        Ok(())
    }

    /// Run on a caller-owned core executor while effects use the domain's
    /// host executor. The caller retains the task result and fencing receiver
    /// and decides how its application responds to either outcome.
    #[cfg(not(cellar_internal_tests))]
    pub fn spawn(
        self,
        domain: &crate::asyncrt::ExecutionDomain,
        core_executor: &tokio::runtime::Handle,
        rx: mpsc::UnboundedReceiver<Message>,
    ) -> anyhow::Result<crate::asyncrt::TaskHandle<anyhow::Result<()>>> {
        anyhow::ensure!(
            Arc::ptr_eq(&self.host_services, &domain.services()),
            "actor belongs to another execution domain"
        );
        Ok(domain.spawn_on(core_executor, self.run(rx))?)
    }

    /// Runs the production start-select-step loop.
    ///
    /// This selector stays unbiased because production permits every
    /// ready-input order. Direct callers can use [`Actor::start`] and
    /// [`Actor::step`] without this raw Tokio select.
    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Message>) -> anyhow::Result<()> {
        self.ensure_owner()?;
        let mut effects = FuturesUnordered::new();
        let mut delays = DelayQueue::new();
        let mut timer_slots = TimerSlots::<delay_queue::Key>::default();
        let mut out = StepOutput::default();
        self.start(&mut out);
        drain_step_output(&mut out, &mut effects, &mut delays, &mut timer_slots);
        loop {
            crate::asyncrt::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    self.step(ActorInput::Message(message), &mut out);
                }
                Some(completed) = effects.next(), if !effects.is_empty() => {
                    self.step(ActorInput::Completed(completed), &mut out);
                }
                Some(expired) = delays.next(), if !delays.is_empty() => {
                    let arm = expired.into_inner();
                    if timer_slots.fire(&arm.slot, arm.ordinal).is_some() {
                        self.step(ActorInput::TimerFired(arm.timer), &mut out);
                    }
                }
            }
            drain_step_output(&mut out, &mut effects, &mut delays, &mut timer_slots);
        }
        Ok(())
    }
}
