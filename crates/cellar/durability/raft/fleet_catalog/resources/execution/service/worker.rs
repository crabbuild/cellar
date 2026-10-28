// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The node job set polls and joins this worker, including on physical closure.
use super::*;
use crate::raft::budget::Permit;
use crate::raft::fleet_catalog::deadline::Deadline;
use std::sync::atomic::Ordering;

pub(crate) struct MovementServiceWork {
    catalog: FleetCatalog,
    observer: RaftCells,
    evidence: Arc<dyn FleetMovementEvidence>,
    options: FleetMovementServiceOptions,
    stop: watch::Receiver<bool>,
    done: watch::Sender<bool>,
    status: watch::Sender<FleetMovementServiceStatus>,
    schedule: schedule::Schedule,
    admitted: bool,
    memory: Option<Permit>,
    node_stop: Option<watch::Receiver<bool>>,
    topology: Option<super::topology::TopologyService>,
    detection: Option<super::detection::Detection>,
    isolated: Option<super::isolated::IsolatedRepairService>,
}

impl MovementServiceWork {
    pub(super) fn new(
        catalog: FleetCatalog,
        observer: RaftCells,
        evidence: Arc<dyn FleetMovementEvidence>,
        options: FleetMovementServiceOptions,
        stop: watch::Receiver<bool>,
        done: watch::Sender<bool>,
        status: watch::Sender<FleetMovementServiceStatus>,
    ) -> Self {
        Self {
            catalog,
            observer,
            evidence,
            options,
            stop,
            done,
            status,
            schedule: schedule::Schedule::default(),
            admitted: false,
            memory: None,
            node_stop: None,
            topology: None,
            detection: None,
            isolated: None,
        }
    }

    pub(super) fn set_topology(
        &mut self,
        evidence: Option<(
            Arc<dyn FleetTopologyEvidence>,
            FleetTopologyCollectionOptions,
        )>,
    ) {
        self.topology = evidence
            .map(|(evidence, options)| super::topology::TopologyService::new(evidence, options));
    }

    pub(super) fn set_isolated_repair(
        &mut self,
        config: Option<(
            cellar_logic::consensus::ProposalId,
            Arc<dyn FleetIsolatedRepairEvidence>,
        )>,
    ) {
        self.isolated = config.map(|(operation, evidence)| {
            super::isolated::IsolatedRepairService::new(operation, evidence)
        });
    }

    pub(super) fn set_detection(&mut self, options: Option<FleetFailureDetectionOptions>) {
        self.detection = options.map(super::detection::Detection::new);
    }

    pub(crate) fn admit(&mut self, host: &FleetHostAuthorization) -> Result<()> {
        if host.catalog.birth_owner()? != self.catalog.birth_owner()? {
            return Err(FleetCatalogError::Invalid("movement service fleet"));
        }
        if self.observer.handle.closing.load(Ordering::Acquire)
            || self.observer.handle.admission.is_draining()
            || *self.stop.borrow()
        {
            return Err(FleetCatalogError::Closed);
        }
        let mut slot = self
            .observer
            .handle
            .fleet_movement_service
            .lock()
            .map_err(|_| FleetCatalogError::Closed)?;
        if slot.is_some() {
            return Err(FleetCatalogError::Overloaded);
        }
        // A retained service plus one bounded catalog turn. Transport/physical
        // work separately acquires its existing request, seed and image pools.
        let memory = self
            .observer
            .handle
            .control_budget
            .acquire(2 * ROOT_LIMIT)
            .ok_or(FleetCatalogError::Overloaded)?;
        // Use the enrolled node's installed client, never a caller's alternate
        // backend with the same descriptive fleet identity. Periodic I/O has a
        // private turn under the node's domain, so it cannot occupy foreground
        // catalog admission or outlive the node's joined job set.
        let source = &host.catalog.inner;
        self.catalog = FleetCatalog {
            inner: Arc::new(Inner {
                bucket: source.bucket.clone(),
                domain: self.observer.handle.domain.clone(),
                prefix: source.prefix.clone(),
                cluster: source.cluster,
                incarnation: source.incarnation,
                turn: Mutex::new(()),
            }),
        };
        let (stop, wait) = watch::channel(false);
        *slot = Some(stop);
        self.memory = Some(memory);
        if let Some(detection) = &mut self.detection {
            detection.set_origin(host.node());
        }
        self.admitted = true;
        self.status.send_modify(|status| status.running = true);
        // Keep public cancellation and node drain independent. The node-owned
        // receiver is selected for the worker's entire lifetime, including I/O.
        self.node_stop = Some(wait);
        Ok(())
    }

    pub(crate) async fn run(mut self) {
        let mut stop = self.stop.clone();
        let Some(mut node_stop) = self.node_stop.take() else {
            return;
        };
        if *stop.borrow_and_update() || *node_stop.borrow_and_update() {
            return;
        }
        crate::asyncrt::select_biased! { "service stop precedes another ready catalog or movement action";
            _ = stop.changed() => (),
            _ = node_stop.changed() => (),
            _ = self.poll() => (),
        }
    }

    async fn poll(&mut self) {
        loop {
            let now = match self.catalog.inner.domain.mono_ms() {
                Ok(now) => now,
                Err(_) => return,
            };
            if self.observer.handle.closing.load(Ordering::Acquire)
                || self.observer.handle.admission.is_draining()
            {
                return;
            }
            self.status
                .send_modify(|status| status.poll_rounds = status.poll_rounds.saturating_add(1));
            let timeout = self.options.turn_timeout;
            let result = crate::asyncrt::timeout(timeout, self.round())
                .await
                .unwrap_or(Err(FleetCatalogError::Unconfirmed));
            let delay = match result {
                Ok(()) => {
                    self.status.send_modify(|status| {
                        status.consecutive_round_failures = 0;
                        status.last_round_failure = None;
                    });
                    self.isolated
                        .as_ref()
                        .filter(|_| self.schedule.entries.is_empty())
                        .and_then(|r| r.pass_delay(&self.options))
                        .or_else(|| self.topology.as_ref().and_then(|t| t.collection_delay()))
                        .unwrap_or(self.options.interval)
                        .as_millis() as u64
                }
                Err(FleetCatalogError::Closed) => return,
                Err(error) => {
                    if let Some(detection) = &mut self.detection {
                        detection.invalidate();
                    }
                    self.publish_work();
                    let kind = schedule::classify(&error);
                    self.status.send_modify(|status| {
                        status.consecutive_round_failures =
                            status.consecutive_round_failures.saturating_add(1);
                        status.last_round_failure = Some(kind);
                    });
                    let failures = self.status.borrow().consecutive_round_failures;
                    schedule::retry_delay(
                        &self.options,
                        failures,
                        crate::asyncrt::rng("fleet-service-poll-backoff").next_u32(),
                    )
                }
            };
            let Ok(end) = self
                .catalog
                .inner
                .domain
                .mono_ms()
                .map(|at| at.saturating_add(delay))
            else {
                return;
            };
            let Ok(timer) = self.catalog.inner.domain.sleep_until(end.max(now)) else {
                return;
            };
            timer.await;
        }
    }

    async fn round(&mut self) -> Result<()> {
        let deadline = Deadline::new(&self.catalog, self.options.turn_timeout)?;
        let cut = self.catalog.read(deadline.remaining()?).await?;
        let pending = self
            .catalog
            .pending_movements(&cut, deadline.remaining()?)
            .await?;
        self.schedule.refresh(pending);
        if !self.schedule.entries.is_empty() {
            if let Some(topology) = &mut self.topology {
                topology.interrupt();
            }
        }
        self.publish_work();
        self.status
            .send_modify(|status| status.last_revision = Some(cut.revision()));
        let now = self
            .catalog
            .inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        if let Some(index) = self.schedule.select(now) {
            let movement = &self.schedule.entries[index].pending;
            self.status.send_modify(|status| {
                status.execution_attempts = status.execution_attempts.saturating_add(1)
            });
            let result = async {
                let request = self
                    .evidence
                    .refresh(movement, deadline.remaining()?)
                    .await?;
                if request.reservation != movement.reservation {
                    return Err(FleetCatalogError::Invalid("movement evidence reservation"));
                }
                self.catalog
                    .reconcile_movement(&cut, &request, &self.observer, deadline.remaining()?)
                    .await
            }
            .await;
            let now = self
                .catalog
                .inner
                .domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?;
            let entry = &mut self.schedule.entries[index];
            match result {
                Ok(progress) if progress.settled => {
                    self.schedule.entries.remove(index);
                    self.status.send_modify(|status| {
                        status.settled_movements = status.settled_movements.saturating_add(1)
                    });
                }
                Ok(progress) => {
                    entry.status.phase = Some(progress.phase);
                    entry.status.consecutive_failures = 0;
                    entry.status.last_failure = None;
                    entry.status.needs_operator = false;
                    entry.next_at = now.saturating_add(self.options.interval.as_millis() as u64);
                }
                Err(FleetCatalogError::Closed) => return Err(FleetCatalogError::Closed),
                Err(error) => {
                    entry.status.consecutive_failures =
                        entry.status.consecutive_failures.saturating_add(1);
                    let kind = schedule::classify(&error);
                    entry.status.last_failure = Some(kind);
                    entry.status.needs_operator = kind == FleetMovementServiceFailure::Rejected;
                    entry.next_at = now.saturating_add(schedule::retry_delay(
                        &self.options,
                        entry.status.consecutive_failures,
                        crate::asyncrt::rng("fleet-service-move-backoff").next_u32(),
                    ));
                }
            }
        } else {
            // Due admitted effects always run first. A retry/backoff window
            // may advance one independent repair while a local slot is free;
            // its fresh root admission still counts every durable debit.
            if let Some(isolated) = self
                .isolated
                .as_mut()
                .filter(|_| self.schedule.entries.len() < super::super::super::MAX_MOVES)
            {
                isolated
                    .round(
                        &self.catalog,
                        &self.observer,
                        &cut,
                        &self.options,
                        &deadline,
                        &self.status,
                    )
                    .await?;
            }
            if let Some(detection) = &mut self.detection {
                detection
                    .round(&self.catalog, &self.observer, &cut, &deadline)
                    .await?;
            }
            if let Some(topology) = self
                .topology
                .as_mut()
                .filter(|_| self.schedule.entries.is_empty())
            {
                topology
                    .round(
                        &self.catalog,
                        &self.observer,
                        &cut,
                        &self.options,
                        &deadline,
                        &self.status,
                    )
                    .await?;
            }
        }
        self.publish_work();
        Ok(())
    }

    fn publish_work(&self) {
        if let Some(detection) = &self.detection {
            if let Ok(now) = self.catalog.inner.domain.mono_ms() {
                self.status
                    .send_modify(|status| status.failure_detection = Some(detection.view(now)));
            }
        }
        let work = self
            .schedule
            .entries
            .iter()
            .map(|entry| entry.status.clone())
            .collect();
        self.status.send_modify(|status| status.work = work);
    }
}

impl Drop for MovementServiceWork {
    fn drop(&mut self) {
        if self.admitted {
            if let Ok(mut slot) = self.observer.handle.fleet_movement_service.lock() {
                slot.take();
            }
        }
        self.memory.take();
        self.status.send_modify(|status| status.running = false);
        self.done.send_replace(true);
    }
}
