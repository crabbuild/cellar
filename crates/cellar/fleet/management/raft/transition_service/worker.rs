// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::collections::BTreeMap;

pub(super) struct Work {
    control: RaftFleetControl,
    pub(super) options: RaftFleetTransitionServiceOptions,
    lease: ControlServiceLease,
    stop: watch::Receiver<bool>,
    status: watch::Sender<RaftFleetTransitionServiceStatus>,
    finished: watch::Sender<bool>,
    pub(super) local: BTreeMap<GroupHistory, RaftFleetTransitionChildStep>,
    pub(super) initialized: Vec<GroupHistory>,
    cursor: usize,
    operation: Option<[u8; 16]>,
}

impl Work {
    pub(super) fn new(
        control: RaftFleetControl,
        options: RaftFleetTransitionServiceOptions,
        lease: ControlServiceLease,
        stop: watch::Receiver<bool>,
        status: watch::Sender<RaftFleetTransitionServiceStatus>,
        finished: watch::Sender<bool>,
    ) -> Self {
        Self {
            control,
            options,
            lease,
            stop,
            status,
            finished,
            local: BTreeMap::new(),
            initialized: Vec::new(),
            cursor: 0,
            operation: None,
        }
    }

    pub(super) async fn run(mut self) {
        let _done = ControlServiceCompletion::new(self.finished.clone());
        let mut requested_stop = self.stop.clone();
        let mut node_stop = self.lease.stop_receiver();
        self.status.send_modify(|s| s.running = true);
        loop {
            if *self.stop.borrow() || *node_stop.borrow() {
                break;
            }
            let control = self.control.clone();
            let result = crate::asyncrt::select! {
                _ = requested_stop.changed() => break,
                _ = node_stop.changed() => break,
                result = Box::pin(self.turn(&control)) => result,
            };
            self.status.send_modify(|s| {
                s.rounds = s.rounds.saturating_add(1);
                match result {
                    Ok(()) => {
                        s.last_failure = None;
                        s.consecutive_failures = 0;
                    }
                    Err(error) => {
                        s.needs_operator = matches!(
                            error.code,
                            RaftFleetFailureCode::InvalidInput
                                | RaftFleetFailureCode::PayloadConflict
                                | RaftFleetFailureCode::Forbidden
                                | RaftFleetFailureCode::Unsupported
                        );
                        s.last_failure = Some(error);
                        s.consecutive_failures = s.consecutive_failures.saturating_add(1);
                    }
                }
            });
            if self.status.borrow().needs_operator {
                break;
            }
            let failures = self.status.borrow().consecutive_failures;
            let delay = self
                .options
                .interval
                .saturating_mul(1u32 << failures.min(8))
                .min(self.options.retry_max_interval);
            let Ok(now) = self.control.domain.mono_ms() else {
                break;
            };
            let Ok(timer) = self
                .control
                .domain
                .sleep_until(now.saturating_add(delay.as_millis() as u64))
            else {
                break;
            };
            crate::asyncrt::select! {
                _ = requested_stop.changed() => break,
                _ = node_stop.changed() => break,
                _ = timer => (),
            }
        }
        self.status.send_modify(|s| s.running = false);
        // Registry admission outlives every callback future. Entered install
        // and pre-copy jobs keep their separate physical owners after this join.
        drop(self.lease);
    }

    async fn turn(&mut self, control: &RaftFleetControl) -> Result<()> {
        let turn = Turn::new(&control.domain, self.options.turn_timeout)?;
        let Some(consent) = control
            .catalog
            .active_transition_execution(turn.remaining()?)
            .await?
        else {
            self.select(None);
            return Ok(());
        };
        let Some(directory) = control
            .namespaces
            .iter()
            .find(|directory| directory.namespace().root_digest() == consent.namespace)
            .cloned()
        else {
            self.select(None);
            return Ok(());
        };
        self.select(Some(consent.identity.operation));
        let operation = ProposalId::new(consent.identity.operation)
            .map_err(|_| RaftFleetFailure::invalid("transition operation ID"))?;
        let transition = control
            .catalog
            .reopen_managed_transition(&directory, operation, turn.remaining()?)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        if transition.plan.digest() != consent.plan
            || transition.request.principal != consent.identity.principal
        {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        let phase = transition.journal.as_ref().map(|j| j.phase());
        self.status.send_modify(|s| s.phase = phase);
        let (parents, children) = directory
            .transition_routes(&transition.plan)
            .map_err(FleetCatalogError::from)?;
        if parents.is_empty() || parents.len() > 2 || children.is_empty() || children.len() > 2 {
            return Err(RaftFleetFailure::invalid(
                "transition service supports binary split/merge",
            ));
        }
        let context = actions::Context {
            control,
            directory: &directory,
            consent: &consent,
            transition: &transition,
            parents: &parents,
            children: &children,
        };
        let index = self.cursor;
        self.cursor = self.cursor.wrapping_add(1);
        let local = Box::pin(actions::local(&context, self, index, &turn)).await;
        let coordinated = if self.options.coordinate {
            Box::pin(actions::coordinate(&context, self, index, &turn)).await
        } else {
            Ok(())
        };
        coordinated.and(local)
    }

    fn select(&mut self, operation: Option<[u8; 16]>) {
        if self.operation == operation {
            return;
        }
        self.operation = operation;
        self.local.clear();
        self.initialized.clear();
        self.cursor = 0;
        self.status.send_modify(|s| {
            s.operation_id = operation.map(hex::encode);
            s.phase = None;
            s.last_action = None;
            s.last_failure = None;
            s.consecutive_failures = 0;
        });
    }

    pub(super) fn action(&self, action: &'static str) {
        self.status.send_modify(|s| s.last_action = Some(action));
    }
}
