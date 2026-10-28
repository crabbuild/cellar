// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node-owned worker resumes admitted work. Explicit topology mode also
//! collects and admits rooted desired placement. No timeout or local hint can
//! release a reservation or establish convergence.
mod detection;
mod isolated;
mod schedule;
mod topology;
mod worker;
use super::*;
pub use detection::{
    FleetFailureDetectionOptions, FleetFailureDetectionStatus, FleetNodeFailureStatus,
    FleetNodeHealth,
};
use std::pin::Pin;
use tokio::sync::watch;
pub(crate) use worker::MovementServiceWork;

/// Deployment integration supplies occupancy evidence for the exact move on
/// every attempt. This callback is deadline-bound and may be dropped on stop.
/// Retain prior evidence when needed to resolve a historical terminal receipt;
/// an unavailable measurement must return an error, never invented occupancy.
pub trait FleetMovementEvidence: Send + Sync {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetMovementExecutionRequest>> + Send + 'a>>;
}

/// Supply current conservative deployment bounds and complete namespace
/// handles for the exact rooted desired topology. Called at collection start
/// and before planning. The node owns and cancels/joins each callback future.
pub trait FleetTopologyEvidence: Send + Sync {
    fn refresh<'a>(
        &'a self,
        operation: &'a FleetTopologyOperation,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetTopologyServiceInputs>> + Send + 'a>>;
}

pub struct FleetTopologyServiceInputs {
    pub request: FleetTopologyRequest,
    pub namespaces: Vec<crate::raft::catalog::NamespaceCatalog>,
}

/// Local diagnostics, not a convergence, membership or physical-release proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetTopologyServiceStatus {
    pub operation: [u8; 16],
    pub phase: FleetTopologyPhase,
    pub observed_revision: u64,
    pub histories_checked: u64,
    pub planned_movements: Option<u64>,
    pub admitted_movements: u64,
    pub last_superseded_admission: Option<[u8; 16]>,
}

#[derive(Clone, Debug)]
pub struct FleetMovementServiceOptions {
    /// Minimum delay after a completed round. No missed-tick catch-up occurs.
    pub interval: Duration,
    pub retry_max_interval: Duration,
    /// One original budget covers discovery, evidence and the execution turn.
    pub turn_timeout: Duration,
}

impl Default for FleetMovementServiceOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            retry_max_interval: Duration::from_secs(10),
            turn_timeout: Duration::from_secs(5),
        }
    }
}

impl FleetMovementServiceOptions {
    fn validate(&self) -> Result<()> {
        if !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&self.interval)
            || !(self.interval..=Duration::from_secs(60)).contains(&self.retry_max_interval)
            || !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&self.turn_timeout)
        {
            return Err(FleetCatalogError::Invalid("movement service bounds"));
        }
        Ok(())
    }
}

/// Explicit bounds for active topology collection, independent of movement and
/// idle polling. The soft quantum is checked between complete history proofs;
/// every proof retains the movement turn's original remaining deadline.
#[derive(Clone, Debug)]
pub struct FleetTopologyCollectionOptions {
    pub interval: Duration,
    pub max_histories_per_round: usize,
    pub quantum: Duration,
}

impl Default for FleetTopologyCollectionOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(50),
            max_histories_per_round: 16,
            quantum: Duration::from_millis(100),
        }
    }
}

impl FleetTopologyCollectionOptions {
    fn validate(&self) -> Result<()> {
        if !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&self.interval)
            || !(1..=16).contains(&self.max_histories_per_round)
            || self.quantum < Duration::from_millis(1)
            || self.quantum > Duration::from_millis(250)
        {
            return Err(FleetCatalogError::Invalid("topology collection bounds"));
        }
        Ok(())
    }
}

/// Diagnostic outcome of the last attempt, never a membership or capacity proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FleetMovementServiceFailure {
    Conflict,
    Overloaded,
    Unconfirmed,
    Storage,
    Observation,
    Rejected,
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetMovementServiceEntry {
    pub reservation: [u8; 16],
    /// Last pre-action observation; it is not current quorum progress.
    pub phase: Option<FleetMovementPhase>,
    pub consecutive_failures: u32,
    pub last_failure: Option<FleetMovementServiceFailure>,
    pub needs_operator: bool,
}

/// Bounded local service counters, reset on start. Pending work is reconstructed
/// from storage; absence from a later scan is not counted as verified settlement.
/// Local scan diagnostics only. Exhaustion does not certify current replica
/// health, evacuation, node removal, or physical resource release.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetIsolatedRepairServiceStatus {
    pub operation: cellar_logic::consensus::ProposalId,
    pub failed_boot: crate::raft::fleet_stop::BootBinding,
    pub scan_revision: u64,
    pub histories_checked: u64,
    pub blocked_histories: u64,
    pub inventory_exhausted: bool,
    pub last_blocked_group: Option<GroupScope>,
    pub last_blocked_failure: Option<FleetMovementServiceFailure>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FleetMovementServiceStatus {
    pub failure_detection: Option<FleetFailureDetectionStatus>,
    pub isolated_repair: Option<FleetIsolatedRepairServiceStatus>,
    pub running: bool,
    pub poll_rounds: u64,
    pub execution_attempts: u64,
    pub settled_movements: u64,
    pub last_revision: Option<u64>,
    pub consecutive_round_failures: u32,
    pub last_round_failure: Option<FleetMovementServiceFailure>,
    /// At most two tracked reservations. Unconfirmed work retains its slot.
    pub work: Vec<FleetMovementServiceEntry>,
    pub topology: Option<FleetTopologyServiceStatus>,
}

/// Explicitly started, default-off service. The node job set owns its worker.
/// Dropping this handle requests stop; shutdown also waits for its worker and
/// evidence callback to exit. Node drain/closure stops and joins it. Already
/// submitted physical work retains its ordinary node owner and durable intent.
pub struct FleetMovementService {
    clock: crate::asyncrt::DomainToken,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    status: watch::Receiver<FleetMovementServiceStatus>,
}

impl FleetMovementService {
    pub fn status(&self) -> FleetMovementServiceStatus {
        let mut status = self.status.borrow().clone();
        if let Some(detection) = &mut status.failure_detection {
            detection.age(if status.running {
                self.clock.mono_ms().ok()
            } else {
                None
            });
        }
        status
    }

    pub fn stop(&self) {
        self.stop.send_replace(true);
    }

    pub async fn shutdown(mut self) {
        self.stop();
        while !*self.done.borrow_and_update() {
            if self.done.changed().await.is_err() {
                break;
            }
        }
    }
}

impl Drop for FleetMovementService {
    fn drop(&mut self) {
        self.stop();
    }
}

enum ServiceMode {
    Admitted,
    Topology(
        Arc<dyn FleetTopologyEvidence>,
        FleetTopologyCollectionOptions,
    ),
    Isolated(
        cellar_logic::consensus::ProposalId,
        Arc<dyn FleetIsolatedRepairEvidence>,
    ),
    Detection(FleetFailureDetectionOptions),
}

impl FleetCatalog {
    /// Start one paced worker on this enrolled origin. A second service on the
    /// same RaftCells is refused, including through a different catalog adapter.
    /// No new reservation, optional balance or failure-driven repair is admitted.
    pub async fn start_movement_service(
        &self,
        observer: RaftCells,
        evidence: Arc<dyn FleetMovementEvidence>,
        options: FleetMovementServiceOptions,
    ) -> Result<FleetMovementService> {
        self.start_fleet_service(observer, evidence, ServiceMode::Admitted, options)
            .await
    }

    /// Explicit default-off controller for rooted topology operations. Existing
    /// admitted work has priority. New collection, one move admission and
    /// terminal completion/cancellation are paced by the same owned worker. This
    /// entry point retains one history per round at `options.interval`; use
    /// `start_topology_service_with_collection` for explicit collection bounds.
    pub async fn start_topology_service(
        &self,
        observer: RaftCells,
        movement_evidence: Arc<dyn FleetMovementEvidence>,
        topology_evidence: Arc<dyn FleetTopologyEvidence>,
        options: FleetMovementServiceOptions,
    ) -> Result<FleetMovementService> {
        let collection = FleetTopologyCollectionOptions {
            interval: options.interval,
            max_histories_per_round: 1,
            quantum: Duration::from_millis(100),
        };
        self.start_topology_service_with_collection(
            observer,
            movement_evidence,
            topology_evidence,
            options,
            collection,
        )
        .await
    }

    /// Start topology execution with separately bounded active collection.
    /// Idle polling, admitted movement pacing and failure backoff still use
    /// `options`; collection never accelerates a failed or idle round.
    pub async fn start_topology_service_with_collection(
        &self,
        observer: RaftCells,
        movement_evidence: Arc<dyn FleetMovementEvidence>,
        topology_evidence: Arc<dyn FleetTopologyEvidence>,
        options: FleetMovementServiceOptions,
        collection: FleetTopologyCollectionOptions,
    ) -> Result<FleetMovementService> {
        collection.validate()?;
        self.start_fleet_service(
            observer,
            movement_evidence,
            ServiceMode::Topology(topology_evidence, collection),
            options,
        )
        .await
    }

    /// Generate and execute required replacements for the explicitly admitted
    /// evacuation operation after permanent exact-boot isolation. Reuses the
    /// node's sole owned movement worker; no failure is inferred from timeouts.
    /// Missing quorum remains blocked while other histories receive turns.
    pub async fn start_isolated_repair_service(
        &self,
        observer: RaftCells,
        movement_evidence: Arc<dyn FleetMovementEvidence>,
        repair_evidence: Arc<dyn FleetIsolatedRepairEvidence>,
        operation: cellar_logic::consensus::ProposalId,
        options: FleetMovementServiceOptions,
    ) -> Result<FleetMovementService> {
        self.start_fleet_service(
            observer,
            movement_evidence,
            ServiceMode::Isolated(operation, repair_evidence),
            options,
        )
        .await
    }

    /// Observe exact enrolled boots through bounded authenticated probes while
    /// continuing already-admitted movements. At most four probes run per turn;
    /// all observations expire after five seconds, including while other work
    /// takes priority. Unknown outcomes restart failure grace. This explicit,
    /// default-off mode does not isolate, evacuate or admit new replacements.
    pub async fn start_failure_detection_service(
        &self,
        observer: RaftCells,
        movement_evidence: Arc<dyn FleetMovementEvidence>,
        options: FleetMovementServiceOptions,
        detection: FleetFailureDetectionOptions,
    ) -> Result<FleetMovementService> {
        detection.validate()?;
        self.start_fleet_service(
            observer,
            movement_evidence,
            ServiceMode::Detection(detection),
            options,
        )
        .await
    }

    async fn start_fleet_service(
        &self,
        observer: RaftCells,
        evidence: Arc<dyn FleetMovementEvidence>,
        mode: ServiceMode,
        options: FleetMovementServiceOptions,
    ) -> Result<FleetMovementService> {
        options.validate()?;
        // A retained capability cannot start new work after its original owner
        // closes. The receiving node independently selects its installed fleet
        // client and binds the actual worker to its own execution domain.
        self.inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        let timeout = options.turn_timeout;
        let (stop, wait) = watch::channel(false);
        let (done, finished) = watch::channel(false);
        let (status, view) = watch::channel(FleetMovementServiceStatus::default());
        let service = FleetMovementService {
            clock: observer.handle.domain.clone(),
            stop,
            done: finished,
            status: view,
        };
        let mut work = MovementServiceWork::new(
            self.clone(),
            observer.clone(),
            evidence,
            options,
            wait,
            done,
            status,
        );
        match mode {
            ServiceMode::Admitted => (),
            ServiceMode::Topology(evidence, collection) => {
                work.set_topology(Some((evidence, collection)))
            }
            ServiceMode::Isolated(operation, evidence) => {
                work.set_isolated_repair(Some((operation, evidence)))
            }
            ServiceMode::Detection(options) => work.set_detection(Some(options)),
        }
        observer.start_movement_worker(work, timeout).await?;
        Ok(service)
    }
}

#[cfg(test)]
mod tests;
