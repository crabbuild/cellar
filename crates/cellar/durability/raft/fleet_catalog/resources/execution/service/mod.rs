// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node-owned worker resumes admitted work. It never creates placement or
//! releases a reservation from a timeout, missing inventory row or local hint.
mod schedule;
mod worker;
use super::*;
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FleetMovementServiceStatus {
    pub running: bool,
    pub poll_rounds: u64,
    pub execution_attempts: u64,
    pub settled_movements: u64,
    pub last_revision: Option<u64>,
    pub consecutive_round_failures: u32,
    pub last_round_failure: Option<FleetMovementServiceFailure>,
    /// At most two tracked reservations. Unconfirmed work retains its slot.
    pub work: Vec<FleetMovementServiceEntry>,
}

/// Explicitly started, default-off service. The node job set owns its worker.
/// Dropping this handle requests stop; shutdown also waits for its worker and
/// evidence callback to exit. Node drain/closure stops and joins it. Already
/// submitted physical work retains its ordinary node owner and durable intent.
pub struct FleetMovementService {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    status: watch::Receiver<FleetMovementServiceStatus>,
}

impl FleetMovementService {
    pub fn status(&self) -> FleetMovementServiceStatus {
        self.status.borrow().clone()
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
            stop,
            done: finished,
            status: view,
        };
        let work = MovementServiceWork::new(
            self.clone(),
            observer.clone(),
            evidence,
            options,
            wait,
            done,
            status,
        );
        observer.start_movement_worker(work, timeout).await?;
        Ok(service)
    }
}

#[cfg(test)]
mod tests;
