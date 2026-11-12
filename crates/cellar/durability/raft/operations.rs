// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned stop observations. These are not deployment-fencing certificates
//! or permission to delete active replica disks, even with complete S3 coverage.
use crate::application::consensus::{CommandFailure, RaftCells};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Copy, Debug)]
pub struct RaftCloseReportLimits {
    pub max_groups: usize,
    pub max_bytes: usize,
}

impl Default for RaftCloseReportLimits {
    fn default() -> Self {
        Self {
            max_groups: 128,
            max_bytes: 1024 * 1024,
        }
    }
}

impl RaftCloseReportLimits {
    pub(crate) fn validate(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=4096).contains(&self.max_groups)
                && (4096..=8 * 1024 * 1024).contains(&self.max_bytes),
            "invalid Raft close report limits"
        );
        Ok(())
    }
}

/// Requested Node shutdown records the actual cell-host close result before
/// joining the complete domain. Missing startup/close proof remains explicit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftNodeShutdownReport {
    pub result:
        Result<crate::application::consensus::group_client::RaftCloseReport, RaftNodeShutdownError>,
    pub startup_incomplete: bool,
    /// Best-effort handoff under the original Node shutdown deadline. Missing
    /// evidence is not a graceful drain, even when physical closure succeeded.
    pub drain: Option<Result<RaftDrainReport, CommandFailure>>,
}

/// Preserve the joined closure failure, including both controller and log
/// failures. Cloned waiters share one failure identity; admission errors remain
/// comparable by value. This error never resolves an individual mutation.
#[derive(Clone, Debug)]
pub enum RaftNodeShutdownError {
    Admission(CommandFailure),
    Closure(Arc<crate::application::consensus::group_client::RaftCloseError>),
}

impl From<CommandFailure> for RaftNodeShutdownError {
    fn from(error: CommandFailure) -> Self {
        Self::Admission(error)
    }
}

impl PartialEq for RaftNodeShutdownError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Admission(a), Self::Admission(b)) => a == b,
            (Self::Closure(a), Self::Closure(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for RaftNodeShutdownError {}

impl std::fmt::Display for RaftNodeShutdownError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => std::fmt::Display::fmt(error, f),
            Self::Closure(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for RaftNodeShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Admission(error) => error,
            Self::Closure(error) => error.as_ref(),
        })
    }
}

/// Each registered local replica receives at most one bounded handoff turn.
/// Waiting for admitted group work and inspecting a successor share that turn's
/// deadline; a busy group cannot renew its budget or delay every other group.
/// Acceptance is not an election result; release observes the old leader step
/// down. Neither observation fences a deployment or authorizes disk deletion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RaftDrainReport {
    pub groups_examined: usize,
    pub handoff_attempts: usize,
    pub handoffs_accepted: usize,
    pub leadership_released: usize,
    pub unavailable_groups: usize,
    pub deadline_elapsed: bool,
}

impl RaftNodeShutdownReport {
    pub(crate) fn cli_summary(&self) -> serde_json::Value {
        match &self.result {
            Ok(report) => {
                serde_json::json!({"owners_closed":true,"startup_incomplete":self.startup_incomplete,"groups":report.groups_total,"omitted_group_details":report.omitted_groups,"groups_with_archive_lag":report.groups_with_archive_lag,"groups_with_unknown_archive":report.groups_with_unknown_archive,"mutation_outcomes":report.mutation_outcomes,"retained_calls_at_shutdown":report.retained_calls_at_shutdown,"retain_replica_disks":true,"handoff":self.drain.as_ref().map(|result|match result {Ok(drain)=>serde_json::json!({"groups_examined":drain.groups_examined,"attempts":drain.handoff_attempts,"accepted":drain.handoffs_accepted,"leadership_released":drain.leadership_released,"unavailable_groups":drain.unavailable_groups,"deadline_elapsed":drain.deadline_elapsed}),Err(error)=>serde_json::json!({"error":error.to_string()})})})
            }
            Err(error) => {
                serde_json::json!({"owners_closed":false,"startup_incomplete":self.startup_incomplete,"error":error.to_string(),"retain_replica_disks":true})
            }
        }
    }
}

#[derive(Default)]
struct State {
    sealed: bool,
    started: bool,
    poisoned: bool,
    service: Option<RaftCells>,
    report: Option<RaftNodeShutdownReport>,
    deadline: Option<u64>,
    managed: bool,
    stop: Option<Arc<crate::raft::fleet_stop::Reporter>>,
}

/// One production cell host for the Node's exclusive raft directory. A complete
/// Node restart supplies a new lifecycle; cancelled startup cannot silently reuse
/// a slot before its owners join. Raw verification infrastructure remains caller
/// managed and supplies no cell-host semantic shutdown proof.
#[derive(Default)]
pub(crate) struct NodeLifecycle {
    state: Mutex<State>,
    shutdown: OnceLock<tokio::sync::mpsc::UnboundedSender<crate::actor::ShutdownMode>>,
    maintenance: Arc<crate::raft::fleet_stop::MaintenanceAdmission>,
    maintenance_dispatch: Arc<OnceLock<crate::raft::fleet_stop::MaintenanceSender>>,
}

impl NodeLifecycle {
    /// Both adapters route to this same Node's driver. Prefer the embedded
    /// owner's command channel when installed before driver readiness.
    pub(crate) fn configure_shutdown(
        &self,
        sender: tokio::sync::mpsc::UnboundedSender<crate::actor::ShutdownMode>,
    ) {
        let _ = self.shutdown.set(sender);
    }

    pub(crate) fn maintenance_receiver(
        &self,
    ) -> Option<crate::raft::fleet_stop::MaintenanceReceiver> {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        self.maintenance_dispatch.set(sender).ok()?;
        Some(receiver)
    }

    pub(crate) fn public_maintenance_closed(&self) -> bool {
        self.maintenance.public_closed()
    }

    /// Startup consumes a synced returned-boot gate before endpoint publication.
    /// No accepted application work exists yet; both reversible gates close now.
    pub(crate) fn install_startup_maintenance(
        &self,
        selector: &crate::raft::fleet_stop::StopSelector,
    ) -> Result<(), crate::raft::fleet_catalog::FleetCatalogError> {
        let state = self
            .state
            .lock()
            .map_err(|_| crate::raft::fleet_catalog::FleetCatalogError::Closed)?;
        if state.sealed || state.service.is_some() {
            return Err(crate::raft::fleet_catalog::FleetCatalogError::Conflict);
        }
        self.maintenance.begin(selector)?;
        self.maintenance.seal_raft(selector)
    }

    pub(crate) fn maintenance_matches(
        &self,
        selector: &crate::raft::fleet_stop::StopSelector,
    ) -> bool {
        self.maintenance.matches(selector)
    }

    pub(crate) fn maintenance_now(&self) -> Result<u64, CommandFailure> {
        self.sample_service()?
            .ok_or(CommandFailure::Unavailable)?
            .handle
            .domain
            .mono_ms()
            .map_err(|_| CommandFailure::Unavailable)
    }

    pub(crate) fn seal_maintenance_raft(
        &self,
        selector: &crate::raft::fleet_stop::StopSelector,
    ) -> Result<(), crate::raft::fleet_catalog::FleetCatalogError> {
        self.maintenance.seal_raft(selector)?;
        self.sample_service()
            .map_err(|_| crate::raft::fleet_catalog::FleetCatalogError::Closed)?
            .ok_or(crate::raft::fleet_catalog::FleetCatalogError::Closed)?
            .handle
            .admission
            .barrier()
            .map_err(|_| crate::raft::fleet_catalog::FleetCatalogError::Closed)
    }

    pub(crate) fn managed(&self) -> Result<bool, CommandFailure> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?
            .managed)
    }

    pub(crate) fn stop_reporter(
        &self,
    ) -> Result<Option<Arc<crate::raft::fleet_stop::Reporter>>, CommandFailure> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?
            .stop
            .clone())
    }

    /// After the domain joins, transfer the final reporter to its surrounding
    /// host so retained HostServices do not retain the closed engine caches.
    pub(crate) fn take_stop_reporter(
        &self,
    ) -> Result<Option<Arc<crate::raft::fleet_stop::Reporter>>, CommandFailure> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?
            .stop
            .take())
    }

    fn sample_service(
        &self,
    ) -> Result<Option<crate::application::consensus::RaftCells>, CommandFailure> {
        let state = self.state.lock().map_err(|_| CommandFailure::Unavailable)?;
        if state.poisoned || state.started && state.service.is_none() {
            return Err(CommandFailure::Unavailable);
        }
        Ok(state.service.clone())
    }

    /// Native ingress borrows this node's joined owner. Retaining a request
    /// must not keep the external SDK lifetime alive after node shutdown.
    pub(crate) fn application_service(&self) -> Result<RaftCells, CommandFailure> {
        let state = self.state.lock().map_err(|_| CommandFailure::Unavailable)?;
        if state.sealed || state.poisoned || self.maintenance.public_closed() {
            return Err(CommandFailure::Unavailable);
        }
        state
            .service
            .as_ref()
            .map(RaftCells::internal_observer)
            .ok_or(CommandFailure::Unavailable)
    }

    pub(crate) fn resource_usage(
        &self,
    ) -> Result<
        Option<crate::application::consensus::group_client::CellGroupResourceUsage>,
        CommandFailure,
    > {
        Ok(self
            .sample_service()?
            .map(|service| service.resource_usage()))
    }

    pub(crate) fn performance(
        &self,
    ) -> Result<Option<crate::application::consensus::RaftPerformanceSnapshot>, CommandFailure>
    {
        Ok(self.sample_service()?.map(|service| service.performance()))
    }

    pub(crate) fn begin(
        self: &Arc<Self>,
        managed: bool,
        returning: bool,
    ) -> Result<Starting, CommandFailure> {
        let mut state = self.state.lock().map_err(|_| CommandFailure::Unavailable)?;
        if state.sealed || state.started || state.poisoned {
            return Err(CommandFailure::Unavailable);
        }
        state.started = true;
        state.managed = managed;
        if returning {
            self.maintenance.hold_startup();
        }
        Ok(Starting {
            owner: self.clone(),
            armed: true,
        })
    }

    pub(crate) fn quiesce(&self, budget_ms: Option<u64>) {
        if let Ok(mut state) = self.state.lock() {
            state.sealed = true;
            if let Some(service) = state.service.clone() {
                if state.deadline.is_none() {
                    state.deadline = budget_ms.and_then(|budget| {
                        service.handle.domain.mono_ms().ok()?.checked_add(budget)
                    });
                }
                service.begin_node_drain();
            }
        }
    }

    pub(crate) async fn close(&self) -> Option<RaftNodeShutdownReport> {
        self.quiesce(None);
        let (service, deadline) = {
            let state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => {
                    return Some(RaftNodeShutdownReport {
                        result: Err(CommandFailure::Unavailable.into()),
                        startup_incomplete: true,
                        drain: None,
                    })
                }
            };
            if let Some(report) = &state.report {
                return Some(report.clone());
            }
            if !state.started {
                return None;
            }
            (state.service.clone(), state.deadline)
        };
        let report = match service {
            Some(service) => {
                let drain = match deadline {
                    Some(deadline) => Some(service.drain_node(deadline).await),
                    None => None,
                };
                RaftNodeShutdownReport {
                    result: service
                        .close()
                        .await
                        .map_err(RaftNodeShutdownError::Closure),
                    startup_incomplete: false,
                    drain,
                }
            }
            None => RaftNodeShutdownReport {
                result: Err(CommandFailure::Unavailable.into()),
                startup_incomplete: true,
                drain: None,
            },
        };
        if let Ok(mut state) = self.state.lock() {
            state.report = Some(report.clone());
            state.service.take();
        }
        Some(report)
    }
}

pub(crate) struct Starting {
    owner: Arc<NodeLifecycle>,
    armed: bool,
}

impl Starting {
    pub(crate) fn install(
        mut self,
        service: RaftCells,
        stop: Option<crate::raft::fleet_stop::Services>,
    ) -> Result<(), CommandFailure> {
        let mut state = self
            .owner
            .state
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?;
        if state.sealed {
            return Err(CommandFailure::Unavailable);
        }
        if let Some(stop) = stop {
            let shutdown = self
                .owner
                .shutdown
                .get()
                .cloned()
                .ok_or(CommandFailure::Unavailable)?;
            state.stop = Some(
                crate::raft::fleet_stop::Reporter::start(
                    stop,
                    shutdown,
                    self.owner.maintenance.clone(),
                    self.owner.maintenance_dispatch.clone(),
                    service.clone(),
                )
                .map_err(|_| CommandFailure::Unavailable)?,
            );
        }
        service
            .handle
            .admission
            .bind_maintenance(self.owner.maintenance.clone())?;
        state.service = Some(service);
        self.armed = false;
        Ok(())
    }

    /// The caller returned normally after joining failed infrastructure. A
    /// dropped/cancelled startup instead poisons the slot until Node restart.
    pub(crate) fn failed(mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            if !state.sealed {
                state.started = false;
                state.managed = false;
            }
            self.armed = false;
        }
    }
}

impl Drop for Starting {
    fn drop(&mut self) {
        if self.armed {
            if let Ok(mut state) = self.owner.state.lock() {
                state.poisoned = true;
            }
        }
    }
}
