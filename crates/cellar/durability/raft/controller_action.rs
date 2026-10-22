//! Controller attempts call the same local APIs as explicit administration.
//! Cancellation ends observation, not a claim that an admitted change aborted.
use super::*;

pub(super) enum Planned {
    Begin(RepairPlan),
    Advance {
        id: ProposalId,
        step: RepairStep,
        revision: LogIndex,
    },
    Activate(client::LearnerActivation),
    Transfer(crate::raft::completion::LeadershipTransfer),
}

pub(super) async fn until_stopped<T>(
    mut stop: watch::Receiver<bool>,
    future: impl std::future::Future<Output = Result<T, CommandFailure>>,
) -> Result<T, CommandFailure> {
    let future = Box::pin(future);
    if *stop.borrow_and_update() {
        return Err(CommandFailure::Unavailable);
    }
    crate::asyncrt::select_biased! { "controller closure precedes a ready attempt result";
        _ = stop.changed() => Err(CommandFailure::Unavailable),
        result = future => result,
    }
}

async fn execute(
    local: RaftCellHandle,
    planned: Planned,
    options: RaftControllerOptions,
) -> Result<(), CommandFailure> {
    let timeout = std::time::Duration::from_millis(options.operation_timeout_ms);
    match planned {
        Planned::Begin(plan) => local.begin_repair(plan, timeout).await.map(|_| ()),
        Planned::Advance { id, step, revision } => local
            .advance_repair(id, step, revision, timeout)
            .await
            .map(|_| ()),
        Planned::Activate(activation) => local.activate_learner(activation).await.map(|_| ()),
        Planned::Transfer(transfer) => local.transfer_leadership(transfer, timeout).await,
    }
}

impl Driver {
    pub(super) fn start_controller_action(&mut self, index: usize, planned: Planned, now: u64) {
        let Some(controller) = self.controller.as_mut() else {
            return;
        };
        if !controller.status.enabled || controller.busy.is_some() {
            return;
        }
        let counter = match &planned {
            Planned::Begin(_) => Some(&mut controller.status.repair_requests),
            Planned::Activate(_) => Some(&mut controller.status.activation_requests),
            Planned::Transfer(_) => Some(&mut controller.status.transfer_attempts),
            Planned::Advance { .. } => None,
        };
        if let Some(counter) = counter {
            let Some(next) = counter.checked_add(1) else {
                controller.status.enabled = false;
                controller.stop.send_replace(true);
                return;
            };
            *counter = next;
        }
        if matches!(&planned, Planned::Transfer(_)) {
            controller.last_transfer = now;
        }
        let Some(sequence) = controller.next_sequence() else {
            return;
        };
        let epoch = controller.epoch;
        let stop = controller.stop.subscribe();
        let Some(history) = controller.histories.get(index) else {
            return;
        };
        let local = RaftCellHandle {
            handle: self.local.clone(),
            binding: history.binding.clone(),
        };
        let options = controller.options.clone();
        controller.busy = Some(sequence);
        self.jobs.push(Box::pin(async move {
            let result = until_stopped(stop, execute(local, planned, options)).await;
            Job::Controller(Box::new(Completed::Acted {
                sequence,
                epoch,
                history: index,
                result,
            }))
        }));
    }
}
