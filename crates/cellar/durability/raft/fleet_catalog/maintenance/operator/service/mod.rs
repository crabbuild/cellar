// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit Node-owned preparation. A service never supplies human stop consent.
mod worker;
use super::*;
use tokio::sync::watch;
pub(crate) use worker::RestartServiceWork;

#[derive(Clone, Debug)]
pub struct FleetRestartServiceOptions {
    pub interval: Duration,
    pub turn_timeout: Duration,
    /// Soft quantum between complete steps, sharing one turn deadline.
    pub active_quantum: Duration,
    pub max_steps_per_round: usize,
}

impl Default for FleetRestartServiceOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            turn_timeout: Duration::from_secs(5),
            active_quantum: Duration::from_millis(100),
            max_steps_per_round: 16,
        }
    }
}

impl FleetRestartServiceOptions {
    fn validate(&self) -> Result<()> {
        if !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&self.interval)
            || !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&self.turn_timeout)
            || !(Duration::from_millis(1)..=Duration::from_millis(250))
                .contains(&self.active_quantum)
            || !(1..=16).contains(&self.max_steps_per_round)
        {
            return Err(FleetCatalogError::Invalid("restart service bounds"));
        }
        Ok(())
    }
}

/// Local diagnostics only; no counter or phase is a live readiness proof.
#[derive(Clone, Debug, Default)]
pub struct FleetRestartServiceStatus {
    pub running: bool,
    pub rounds: u64,
    pub steps: u64,
    pub operation: Option<[u8; 16]>,
    pub phase: Option<FleetRestartOperatorPhase>,
    pub checked_histories: u64,
    pub consecutive_failures: u32,
    pub last_failure: Option<FleetMovementServiceFailure>,
}

/// Drop requests cancellation. Shutdown waits for the Node-owned worker and
/// its outstanding callback to exit; admitted physical work retains its owner.
pub struct FleetRestartService {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    status: watch::Receiver<FleetRestartServiceStatus>,
}

impl FleetRestartService {
    pub fn status(&self) -> FleetRestartServiceStatus {
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

impl Drop for FleetRestartService {
    fn drop(&mut self) {
        self.stop();
    }
}

impl FleetCatalog {
    /// Resume rooted preparation/cancellation on one enrolled observer. Starts
    /// no operation and grants no stop, return, resume or capacity authority.
    /// Independent adapters share the same per-Node singleton and budget.
    pub async fn start_restart_service(
        &self,
        observer: crate::application::consensus::RaftCells,
        namespaces: Vec<NamespaceCatalog>,
        evidence: Option<Arc<dyn FleetMovementEvidence>>,
        options: FleetRestartServiceOptions,
    ) -> Result<FleetRestartService> {
        options.validate()?;
        if namespaces.len() > 64 {
            return Err(FleetCatalogError::Limit);
        }
        self.inner
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        let (stop, wait) = watch::channel(false);
        let (done, finished) = watch::channel(false);
        let (status, view) = watch::channel(FleetRestartServiceStatus::default());
        let service = FleetRestartService {
            stop,
            done: finished,
            status: view,
        };
        let timeout = options.turn_timeout;
        let work = RestartServiceWork::new(
            FleetRestartOperatorDriver::new(self.clone(), namespaces, observer.clone(), evidence)?,
            options,
            wait,
            done,
            status,
        );
        observer.start_restart_worker(work, timeout).await?;
        Ok(service)
    }
}
