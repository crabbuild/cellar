// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The node job set polls and joins this worker, including on physical closure.
use super::*;
use crate::application::consensus::group_client::{
    ControlServiceCompletion, ControlServiceKind, ControlServiceLease,
};
use crate::raft::fleet_catalog::deadline::Deadline;

pub(crate) struct MovementServiceWork {
    catalog: FleetCatalog,
    observer: RaftCells,
    evidence: Option<Arc<dyn FleetMovementEvidence>>,
    options: FleetMovementServiceOptions,
    stop: watch::Receiver<bool>,
    status: watch::Sender<FleetMovementServiceStatus>,
    schedule: schedule::Schedule,
    member_cleanup: bool,
    member_cleanup_only: bool,
    cleanup_cursor: usize,
    node_stop: Option<watch::Receiver<bool>>,
    topology: Option<super::topology::TopologyService>,
    detection: Option<super::detection::Detection>,
    isolated: Option<super::isolated::IsolatedRepairService>,
    recovery: Option<super::recovery::Recovery>,
    _registration: Option<ControlServiceLease>,
    _node_owned_stop: Option<watch::Sender<bool>>,
    _completion: ControlServiceCompletion,
}

impl MovementServiceWork {
    pub(super) fn new(
        catalog: FleetCatalog,
        observer: RaftCells,
        evidence: Option<Arc<dyn FleetMovementEvidence>>,
        options: FleetMovementServiceOptions,
        stop: watch::Receiver<bool>,
        done: watch::Sender<bool>,
        status: watch::Sender<FleetMovementServiceStatus>,
    ) -> Self {
        let member_cleanup = options.reclaim_removed_members;
        Self {
            catalog,
            observer: observer.internal_observer(),
            evidence,
            options,
            stop,
            status,
            schedule: schedule::Schedule::default(),
            member_cleanup,
            member_cleanup_only: false,
            cleanup_cursor: 0,
            node_stop: None,
            topology: None,
            detection: None,
            isolated: None,
            recovery: None,
            _registration: None,
            _node_owned_stop: None,
            _completion: ControlServiceCompletion::new(done),
        }
    }

    pub(super) fn set_member_cleanup(&mut self) {
        self.member_cleanup = true;
        self.member_cleanup_only = true;
    }

    pub(super) fn own_stop(&mut self, stop: watch::Sender<bool>) {
        self._node_owned_stop = Some(stop);
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

    pub(super) fn set_recovery(
        &mut self,
        platform: Arc<dyn FleetFailureRecoveryPlatform>,
        repair: Arc<dyn FleetIsolatedRepairEvidence>,
        options: FleetFailureRecoveryOptions,
    ) {
        self.recovery = Some(super::recovery::Recovery::new(platform, repair, options));
    }

    pub(crate) fn admit(&mut self, host: &FleetHostAuthorization) -> Result<()> {
        if host.catalog.birth_owner()? != self.catalog.birth_owner()? {
            return Err(FleetCatalogError::Invalid("movement service fleet"));
        }
        if self.observer.is_closed()
            || self.observer.handle.admission.is_draining()
            || *self.stop.borrow()
        {
            return Err(FleetCatalogError::Closed);
        }
        if let Some(recovery) = &mut self.recovery {
            recovery.admit_origin(host)?;
        }
        let registration = self.observer.register_control_service(
            ControlServiceKind::Movement,
            self.options.control_reservation_bytes(),
        )?;
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
        self.node_stop = Some(registration.stop_receiver());
        self._registration = Some(registration);
        if let Some(detection) = &mut self.detection {
            detection.set_origin(host.node());
        }
        self.status.send_modify(|status| status.running = true);
        // Keep public cancellation and node drain independent. The node-owned
        // receiver is selected for the worker's entire lifetime, including I/O.
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
            if self.observer.is_closed() || self.observer.handle.admission.is_draining() {
                return;
            }
            self.status
                .send_modify(|status| status.poll_rounds = status.poll_rounds.saturating_add(1));
            let timeout = self.options.turn_timeout;
            // One complete control turn belongs to this admitted worker. Keep
            // its mode-specific future state off the enclosing run/select
            // frames; the same timeout and stop owner retain it through await.
            let result = crate::asyncrt::timeout(timeout, Box::pin(self.round()))
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
                        .or_else(|| {
                            self.recovery
                                .as_ref()
                                .filter(|_| self.schedule.entries.is_empty())
                                .and_then(|r| r.pass_delay(&self.options))
                        })
                        .or_else(|| self.topology.as_ref().and_then(|t| t.collection_delay()))
                        .unwrap_or(self.options.interval)
                        .as_millis() as u64
                }
                Err(FleetCatalogError::Closed) => return,
                Err(error) => {
                    if let Some(detection) = &mut self.detection {
                        detection.invalidate();
                    }
                    if let Some(recovery) = &mut self.recovery {
                        recovery.detection.invalidate();
                    }
                    self.publish_work();
                    let kind = schedule::classify(&error);
                    tracing::debug!(failure = ?kind, error = %error, "fleet service round failed");
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
        let catalog = self.catalog.clone();
        let deadline = Deadline::new(&catalog, self.options.turn_timeout)?;
        let cut = self.catalog.read(deadline.remaining()?).await?;
        if self.member_cleanup_only {
            return self.cleanup_round(&cut, &deadline).await.map(|_| ());
        }
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
                    .as_ref()
                    .ok_or(FleetCatalogError::Invalid("missing movement evidence"))?
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
            if self.member_cleanup && self.cleanup_round(&cut, &deadline).await? {
                return Ok(());
            }
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
            // Detection and isolation may unblock already-admitted work even
            // when every movement slot is occupied. Repair admission checks
            // the durable resource book independently before adding any debt.
            if let Some(recovery) = self.recovery.as_mut() {
                recovery
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

    async fn cleanup_round(
        &mut self,
        cut: &FleetSnapshot,
        deadline: &Deadline<'_>,
    ) -> Result<bool> {
        let pending = self
            .catalog
            .run(
                deadline.remaining()?,
                self.catalog
                    .pending_member_cleanup(cut, self.observer.identity().node),
            )
            .await?;
        self.status.send_modify(|status| {
            status.last_revision = Some(cut.revision());
            status.pending_member_cleanups = pending.len() as u32;
        });
        if pending.is_empty() {
            return Ok(false);
        }
        let reservation = pending[self.cleanup_cursor % pending.len()];
        self.cleanup_cursor = self.cleanup_cursor.wrapping_add(1);
        let authority = self
            .catalog
            .run(
                deadline.remaining()?,
                self.catalog.host_from(cut, self.observer.identity().node),
            )
            .await?;
        self.status.send_modify(|status| {
            status.member_cleanup_attempts = status.member_cleanup_attempts.saturating_add(1)
        });
        match self
            .observer
            .reclaim_removed_member_owned(reservation, deadline.remaining()?, Some(authority))
            .await
        {
            Ok(complete) => {
                self.status.send_modify(|status| {
                    status.last_member_cleanup_failure = None;
                    if complete {
                        status.reclaimed_members = status.reclaimed_members.saturating_add(1);
                    }
                });
                Ok(true)
            }
            Err(error) => {
                use crate::raft::cell_groups::CellGroupControllerError;
                let error = match error {
                    CellGroupControllerError::Catalog(error) => match Arc::try_unwrap(error) {
                        Ok(error) => FleetCatalogError::from(error),
                        Err(error) => FleetCatalogError::Storage(anyhow::anyhow!("{error}")),
                    },
                    CellGroupControllerError::Closed => FleetCatalogError::Closed,
                    error => FleetCatalogError::Storage(error.into()),
                };
                self.status.send_modify(|status| {
                    status.last_member_cleanup_failure = Some(schedule::classify(&error))
                });
                Err(error)
            }
        }
    }

    fn publish_work(&self) {
        if let Some(detection) = self
            .detection
            .as_ref()
            .or_else(|| self.recovery.as_ref().map(|r| &r.detection))
        {
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
        self.status.send_modify(|status| status.running = false);
    }
}
