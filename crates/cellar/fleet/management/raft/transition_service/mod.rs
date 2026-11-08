// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned execution of explicitly consented, rooted split/merge work.
mod actions;
mod worker;

use super::*;
use crate::application::consensus::group_client::{
    ControlServiceCompletion, ControlServiceKind, ControlServiceLease,
};
use crate::raft::{cell_groups::TransitionPhase, fleet_catalog::ManagedTransitionExecution};
use cellar_logic::cell_group::GroupHistory;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionExecutionRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub expected_revision: u64,
    pub plan_digest: String,
}

/// Explicitly start one service per managed node. All voters perform their
/// own local steps; at least one coordinator performs archive and catalog work.
/// Redundant coordinators are safe: existing conditional journals serialize
/// decisions and existing physical pools retain entered work.
#[derive(Clone, Debug)]
pub struct RaftFleetTransitionServiceOptions {
    pub coordinate: bool,
    /// Drain retired local sources after all final members applied retirement.
    /// Disable only when an external owner is responsible for their shutdown.
    pub quiesce_retired_sources: bool,
    /// Keep rooted transition ownership through local collection and proved
    /// slot settlement. An external cleanup owner can disable this policy.
    pub reclaim_retired_sources: bool,
    pub interval: Duration,
    pub retry_max_interval: Duration,
    pub turn_timeout: Duration,
}

impl Default for RaftFleetTransitionServiceOptions {
    fn default() -> Self {
        Self {
            coordinate: true,
            quiesce_retired_sources: true,
            reclaim_retired_sources: true,
            interval: Duration::from_millis(500),
            retry_max_interval: Duration::from_secs(10),
            turn_timeout: Duration::from_secs(5),
        }
    }
}

impl RaftFleetTransitionServiceOptions {
    pub(super) fn validate(&self) -> Result<()> {
        if self.reclaim_retired_sources && !self.quiesce_retired_sources
            || !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&self.interval)
            || !(self.interval..=Duration::from_secs(60)).contains(&self.retry_max_interval)
            || !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&self.turn_timeout)
        {
            return Err(RaftFleetFailure::invalid("transition service bounds"));
        }
        Ok(())
    }
}

/// Bounded local diagnostics. No counter or sampled phase is a quorum,
/// activation, convergence or physical-release proof.
#[derive(Clone, Debug, Default)]
pub struct RaftFleetTransitionServiceStatus {
    pub running: bool,
    pub rounds: u64,
    pub operation_id: Option<String>,
    pub phase: Option<TransitionPhase>,
    pub last_action: Option<&'static str>,
    pub last_failure: Option<RaftFleetFailure>,
    pub consecutive_failures: u32,
    pub needs_operator: bool,
}

/// Drop requests stop; shutdown also joins the actual registry-owned worker.
/// Stopping never cancels its durable transition or releases physical debt.
pub struct RaftFleetTransitionService {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    status: watch::Receiver<RaftFleetTransitionServiceStatus>,
}

impl RaftFleetTransitionService {
    pub(super) fn completion(&self) -> watch::Receiver<bool> {
        self.done.clone()
    }

    pub fn status(&self) -> RaftFleetTransitionServiceStatus {
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

impl Drop for RaftFleetTransitionService {
    fn drop(&mut self) {
        self.stop();
    }
}

impl RaftFleetControl {
    /// Give durable consent to complete the exact admitted plan automatically,
    /// including its irreversible seal. Merely admitting a cancellable plan
    /// never grants this consent. Keep this request unchanged after uncertainty.
    pub async fn authorize_transition_execution(
        &self,
        principal: &str,
        request: &RaftFleetTransitionExecutionRequest,
        timeout: Duration,
    ) -> Result<()> {
        self.check_catalog(&request.catalog_id)?;
        records::catalog_id(&request.plan_digest)?;
        records::revision(request.expected_revision)?;
        let operation = ProposalId::new(records::id(&request.operation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("transition operation ID"))?;
        let directory = self.managed_namespace(&request.namespace_id)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let transition = self
            .authorize_transition_operator(
                directory,
                operation,
                principal,
                &request.plan_digest,
                turn.remaining()?,
            )
            .await?;
        self.catalog
            .consent_transition_execution(
                directory,
                &transition,
                &FleetRequestIdentity {
                    id: records::id(&request.request_id)?,
                    operation: operation.bytes(),
                    principal: principal.into(),
                    expected_revision: request.expected_revision,
                },
                turn.remaining()?,
            )
            .await?;
        Ok(())
    }

    /// Start automatic execution against this node's exact installed boot.
    /// Each round discovers one consented operation from the disruption slot,
    /// performs at most one local voter step and one coordination step, and
    /// shares one original turn budget. No per-cell task or inventory broadcast.
    pub async fn start_transition_service(
        &self,
        node: &RaftCells,
        options: RaftFleetTransitionServiceOptions,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionService> {
        options.validate()?;
        if self.transition_preparation.is_none() {
            return Err(RaftFleetFailure::unsupported());
        }
        let turn = Turn::new(&self.domain, timeout)?;
        let host = node.installed_host(turn.remaining()?).await?;
        if host.stop_catalog().catalog_id()? != self.catalog.catalog_id()? {
            return Err(FleetCatalogError::Conflict.into());
        }
        let lease = node.register_control_service(ControlServiceKind::Transition, 64 * 1024)?;
        let mut control = self.clone();
        control.node_observer = Some(node.internal_observer());
        let (stop, receiver) = watch::channel(false);
        let (finished, done) = watch::channel(false);
        let (status, observed) = watch::channel(RaftFleetTransitionServiceStatus::default());
        let work = worker::Work::new(control, options, lease, receiver, status, finished);
        node.handle
            .domain
            .spawn_cleanup(Box::pin(work.run()))
            .map_err(|_| FleetCatalogError::Closed)?
            .detach();
        Ok(RaftFleetTransitionService {
            stop,
            done,
            status: observed,
        })
    }
}
