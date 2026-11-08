// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned selection, admission and explicit execution policy for partitions.
mod worker;

use super::*;
use crate::application::consensus::group_client::{
    ControlServiceCompletion, ControlServiceKind, ControlServiceLease,
};
use cellar_logic::cell_group_placement::PartitionPolicy;
use futures_util::future::BoxFuture;
use tokio::sync::watch;

/// Fresh advisory selection and the catalog basis against which placement is
/// proposed. Admission revalidates both; this value grants no serving authority.
#[derive(Clone, Debug)]
pub struct RaftFleetPartitionPlanContext {
    pub recommendation: RaftFleetPartitionRecommendation,
    pub basis: RaftFleetTransitionBasis,
}

/// Trusted deployment placement policy. Pressure changes priority only. Plan
/// supplies fresh operation/child identities and RF3 placement for the selected
/// parents. Calls are bounded by one service turn and cancelled on node stop.
/// Restored work bypasses both callbacks and retains its original identities.
pub trait RaftFleetPartitionPlanner: Send + Sync {
    fn pressure(&self, timeout: Duration) -> BoxFuture<'_, Result<RaftFleetPartitionPressure>>;

    fn plan(
        &self,
        context: RaftFleetPartitionPlanContext,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<RaftFleetTransitionAdmissionRequest>>;
}

/// Complete explicitly consents to irreversible seal/execution of selected or
/// restored work for this service's principal and configured namespaces only.
/// AdmissionOnly leaves consent to the existing operator API.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RaftFleetPartitionExecutionPolicy {
    #[default]
    AdmissionOnly,
    Complete,
}

#[derive(Clone, Debug)]
pub struct RaftFleetPartitionServiceOptions {
    pub principal: String,
    pub max_groups: usize,
    pub execution: RaftFleetPartitionExecutionPolicy,
    pub interval: Duration,
    pub retry_max_interval: Duration,
    pub turn_timeout: Duration,
    /// The embedded transition worker performs this node's voter steps and,
    /// when configured, coordination. Start on every participating managed node.
    pub transition: RaftFleetTransitionServiceOptions,
}

impl RaftFleetPartitionServiceOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.principal.is_empty()
            || self.principal.len() > 128
            || self.principal.chars().any(char::is_control)
            || !(1..=128).contains(&self.max_groups)
            || !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&self.interval)
            || !(self.interval..=Duration::from_secs(60)).contains(&self.retry_max_interval)
            || !(Duration::from_millis(1)..=Duration::from_secs(60)).contains(&self.turn_timeout)
        {
            return Err(RaftFleetFailure::invalid("partition service bounds"));
        }
        self.transition.validate()
    }
}

/// Bounded diagnostics; counters and sampled phases are not durability proofs.
#[derive(Clone, Debug, Default)]
pub struct RaftFleetPartitionServiceStatus {
    pub running: bool,
    pub rounds: u64,
    /// Successful bounded reads from remote group leaders, not quorum proofs.
    pub remote_samples: u64,
    pub operation_id: Option<String>,
    pub phase: Option<crate::raft::cell_groups::TransitionPhase>,
    pub last_action: Option<&'static str>,
    pub last_failure: Option<RaftFleetFailure>,
    pub consecutive_failures: u32,
    pub needs_operator: bool,
    pub transition: RaftFleetTransitionServiceStatus,
}

/// Drop requests stop. Shutdown joins both the scheduler and its embedded
/// transition worker. Durable selected work and physical debt survive shutdown.
pub struct RaftFleetPartitionService {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    status: watch::Receiver<RaftFleetPartitionServiceStatus>,
}

impl RaftFleetPartitionService {
    pub fn status(&self) -> RaftFleetPartitionServiceStatus {
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

impl Drop for RaftFleetPartitionService {
    fn drop(&mut self) {
        self.stop();
    }
}

impl RaftFleetControl {
    /// Start one bounded automatic partition owner on this installed node. It
    /// restores the Fleet cursor before requesting new observations or placement.
    /// Complete execution requires explicit policy; no default seal consent.
    pub async fn start_partition_service(
        &self,
        node: &RaftCells,
        planner: Arc<dyn RaftFleetPartitionPlanner>,
        policy: PartitionPolicy,
        options: RaftFleetPartitionServiceOptions,
        timeout: Duration,
    ) -> Result<RaftFleetPartitionService> {
        options.validate()?;
        let turn = Turn::new(&self.domain, timeout)?;
        let host = node.installed_host(turn.remaining()?).await?;
        if host.stop_catalog().catalog_id()? != self.catalog.catalog_id()? {
            return Err(FleetCatalogError::Conflict.into());
        }
        let lease =
            node.register_control_service(ControlServiceKind::Partition, 2 * 1024 * 1024)?;
        let mut control = self.clone();
        control.domain = node.handle.domain.clone();
        control.catalog = host.stop_catalog().owned_client(control.domain.clone());
        control.node_observer = Some(node.internal_observer());
        let scheduler =
            RaftFleetPartitionScheduler::new(control.clone(), policy, options.max_groups)?;
        // The embedded worker needs its own bounded catalog turn, sharing the
        // installed backend rather than contending with this worker's client.
        let mut executor = control.clone();
        executor.catalog = host.stop_catalog().owned_client(control.domain.clone());
        let child = executor
            .start_transition_service(node, options.transition.clone(), turn.remaining()?)
            .await?;
        let mut child_done = child.completion();
        let (stop, receiver) = watch::channel(false);
        let (finished, done) = watch::channel(false);
        let (status, observed) = watch::channel(RaftFleetPartitionServiceStatus::default());
        let work = worker::Work {
            control,
            scheduler,
            planner,
            options,
            lease,
            stop: receiver,
            status,
            finished,
            child,
        };
        if let Ok(task) = node.handle.domain.spawn_cleanup(Box::pin(work.run())) {
            task.detach();
        } else {
            // Rejected spawn drops Work, requesting the already-started child
            // to stop. Join its actual lifetime before reporting startup failure.
            while !*child_done.borrow_and_update() {
                if child_done.changed().await.is_err() {
                    break;
                }
            }
            return Err(FleetCatalogError::Closed.into());
        }
        Ok(RaftFleetPartitionService {
            stop,
            done,
            status: observed,
        })
    }
}
