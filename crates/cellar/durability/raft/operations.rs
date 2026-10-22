//! Node-owned stop observations. These are not deployment-fencing certificates
//! or permission to delete active replica disks, even with complete S3 coverage.
use crate::application::consensus::{
    CommandFailure, CommandId, EngineInstanceId, GroupScope, LogIndex, LogPosition, MemberId,
    NodeIdentity, RaftCells,
};
use std::sync::{Arc, Mutex};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnfinishedCommand {
    pub scope: GroupScope,
    pub id: CommandId,
}

/// Final synced local log observations. Archive coverage is the last verified
/// coverage observed by this host; a remote publisher may have advanced farther.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftGroupCloseReport {
    pub scope: GroupScope,
    pub member: MemberId,
    pub committed: LogIndex,
    pub applied: LogPosition,
    pub archived: LogIndex,
    pub retired: bool,
    pub failed: bool,
}

/// Successful return follows local runtime/transport/SQL/log joins. Submitted
/// commands can remain ambiguous and durable intents can remain unsettled. The
/// complete group counts include omitted details. No field grants disk deletion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftCloseReport {
    pub node: NodeIdentity,
    pub storage_instance: EngineInstanceId,
    pub groups: Vec<RaftGroupCloseReport>,
    pub groups_total: usize,
    pub omitted_groups: usize,
    pub groups_with_archive_lag: usize,
    /// Recovered durable histories parked without a host registration. Their
    /// archive coverage is unknown, not zero lag. They count in groups_total
    /// and omitted_groups; the shared engine still closes their owners.
    pub groups_with_unknown_archive: usize,
    pub unfinished: Vec<UnfinishedCommand>,
}

/// Requested Node shutdown records the actual cell-host close result before
/// joining the complete domain. Missing startup/close proof remains explicit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftNodeShutdownReport {
    pub result: Result<RaftCloseReport, CommandFailure>,
    pub startup_incomplete: bool,
    /// Best-effort handoff under the original Node shutdown deadline. Missing
    /// evidence is not a graceful drain, even when physical closure succeeded.
    pub drain: Option<Result<RaftDrainReport, CommandFailure>>,
}

/// Each registered local replica receives at most one bounded handoff turn.
/// Temporary local work and candidate inspection share that turn's deadline.
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
                serde_json::json!({"owners_closed":true,"startup_incomplete":self.startup_incomplete,"groups":report.groups_total,"omitted_group_details":report.omitted_groups,"groups_with_archive_lag":report.groups_with_archive_lag,"groups_with_unknown_archive":report.groups_with_unknown_archive,"unfinished_commands":report.unfinished.len(),"retain_replica_disks":true,"handoff":self.drain.as_ref().map(|result|match result {Ok(drain)=>serde_json::json!({"groups_examined":drain.groups_examined,"attempts":drain.handoff_attempts,"accepted":drain.handoffs_accepted,"leadership_released":drain.leadership_released,"unavailable_groups":drain.unavailable_groups,"deadline_elapsed":drain.deadline_elapsed}),Err(error)=>serde_json::json!({"error":error.to_string()})})})
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
}

/// One production cell host for the Node's exclusive raft directory. A complete
/// Node restart supplies a new lifecycle; cancelled startup cannot silently reuse
/// a slot before its owners join. Raw verification infrastructure remains caller
/// managed and supplies no cell-host semantic shutdown proof.
#[derive(Default)]
pub(crate) struct NodeLifecycle {
    state: Mutex<State>,
}

impl NodeLifecycle {
    pub(crate) fn resource_usage(
        &self,
    ) -> Result<Option<crate::application::consensus::RaftResourceUsage>, CommandFailure> {
        let service = {
            let state = self.state.lock().map_err(|_| CommandFailure::Unavailable)?;
            if state.poisoned || state.started && state.service.is_none() {
                // Starting/cancelled/removed hosts have no live sampler. A
                // missing observation cannot become a healthy zero snapshot.
                return Err(CommandFailure::Unavailable);
            }
            state.service.clone()
        };
        Ok(service.map(|service| service.resource_usage()))
    }

    pub(crate) fn begin(self: &Arc<Self>) -> Result<Starting, CommandFailure> {
        let mut state = self.state.lock().map_err(|_| CommandFailure::Unavailable)?;
        if state.sealed || state.started || state.poisoned {
            return Err(CommandFailure::Unavailable);
        }
        state.started = true;
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
                        result: Err(CommandFailure::Unavailable),
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
                    result: service.close().await,
                    startup_incomplete: false,
                    drain,
                }
            }
            None => RaftNodeShutdownReport {
                result: Err(CommandFailure::Unavailable),
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
    pub(crate) fn install(mut self, service: RaftCells) -> Result<(), CommandFailure> {
        let mut state = self
            .owner
            .state
            .lock()
            .map_err(|_| CommandFailure::Unavailable)?;
        if state.sealed {
            return Err(CommandFailure::Unavailable);
        }
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
