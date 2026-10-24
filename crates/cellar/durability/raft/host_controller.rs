//! Owned reconciliation. Inspection supplies hints; ordinary revision-bound
//! host/runtime operations retain every quorum, seeding and progress check.
use super::*;
use crate::application::consensus::client::RaftCellHandle;
use crate::raft::inspection::{InspectionError, InspectionSnapshot};
use crate::raft::member_state::{RepairPhase, RepairPlan, RepairStep};
use cellar_logic::raft_leadership::LeaderPolicy;
use cellar_logic::raft_placement::PlacementPolicy;
use tokio::sync::watch;

#[path = "controller_action.rs"]
mod action;
#[path = "placement.rs"]
mod policy;
#[path = "controller_probe.rs"]
mod probe;

const PROBE_MEMORY: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RaftControllerOptions {
    pub placement: PlacementPolicy,
    pub leadership: Option<LeaderPolicy>,
    pub start_paused: bool,
    pub interval_ms: u64,
    pub probe_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub seed_timeout_ms: u64,
    pub unavailable_after_ms: u64,
    pub learner_timeout_ms: u64,
    /// Includes the local sample and all current voters/active learner.
    pub probes_per_group: usize,
    pub probe_concurrency: usize,
    /// Retained decoded observations, separate from transport peak memory.
    pub metadata_bytes: usize,
}

impl RaftControllerOptions {
    pub fn new(placement: PlacementPolicy) -> Self {
        Self {
            placement,
            leadership: None,
            start_paused: false,
            interval_ms: 100,
            probe_timeout_ms: 500,
            operation_timeout_ms: 5000,
            seed_timeout_ms: 30_000,
            unavailable_after_ms: 5000,
            learner_timeout_ms: 60_000,
            probes_per_group: 8,
            probe_concurrency: 4,
            metadata_bytes: 8 * PROBE_MEMORY,
        }
    }

    pub(super) fn validate(&self, host: &RaftHostOptions) -> anyhow::Result<()> {
        self.placement.validate()?;
        if let Some(policy) = self.leadership {
            policy.validate()?;
        }
        let profile = host
            .node_profile
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("controller requires a node profile"))?;
        anyhow::ensure!(host.node_directory.is_some() && host.enable_snapshots
            && profile.bytes_per_group == self.placement.required_bytes
            && host.max_control_requests >= 8,
            "controller requires directory, snapshots, matching reservations and reserved control capacity");
        anyhow::ensure!(
            (10..=60_000).contains(&self.interval_ms)
                && (1..=60_000).contains(&self.probe_timeout_ms)
                && (1..=60_000).contains(&self.operation_timeout_ms)
                && (1..=300_000).contains(&self.seed_timeout_ms)
                && (self.interval_ms..=3_600_000).contains(&self.unavailable_after_ms)
                && (self.seed_timeout_ms..=3_600_000).contains(&self.learner_timeout_ms)
                && (4..=32).contains(&self.probes_per_group)
                && (1..=4).contains(&self.probe_concurrency)
                && self.metadata_bytes >= self.probes_per_group * PROBE_MEMORY
                && self.metadata_bytes <= 128 * PROBE_MEMORY,
            "invalid controller bounds"
        );
        Ok(())
    }
}

/// Local service observations. Counters record attempts, never quorum proof or
/// a promise that an admitted transfer won an election. They reset on node boot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftControllerStatus {
    pub enabled: bool,
    pub recovering: bool,
    pub probe_rounds: u64,
    pub repair_requests: u64,
    pub activation_requests: u64,
    pub transfer_attempts: u64,
    pub completed_repairs: u64,
}

struct FailedProbe {
    peer: RaftPeerNode,
    since: u64,
    last: u64,
}

struct History {
    binding: Binding,
    candidate_cursor: Option<NodeId>,
    pull_cursor: Option<NodeId>,
    epoch: u64,
    failures: BTreeMap<NodeId, FailedProbe>,
    leader_term: Option<Term>,
    gained_at: u64,
    seeding: Option<(ProposalId, u64)>,
    last_completed: Option<ProposalId>,
}

pub(super) struct Controller {
    options: RaftControllerOptions,
    status: RaftControllerStatus,
    histories: Vec<History>,
    inventory: usize,
    active: BTreeSet<usize>,
    cursor: usize,
    active_cursor: Option<usize>,
    priority_turn: bool,
    intent: Option<(usize, RepairPlan)>,
    started: Option<u64>,
    last_transfer: u64,
    next_at: u64,
    sequence: u64,
    epoch: u64,
    busy: Option<u64>,
    stop: watch::Sender<bool>,
    memory: Arc<Budget>,
}

pub(super) enum Completed {
    Observed {
        sequence: u64,
        epoch: u64,
        history: usize,
        result: Result<Box<probe::Observed>, CommandFailure>,
    },
    Acted {
        sequence: u64,
        epoch: u64,
        history: usize,
        result: Result<(), CommandFailure>,
    },
}

impl Controller {
    pub(super) fn new(options: RaftControllerOptions, bindings: Vec<Binding>) -> Self {
        let (stop, _) = watch::channel(false);
        let histories = bindings
            .into_iter()
            .map(|binding| History {
                binding,
                candidate_cursor: None,
                pull_cursor: None,
                epoch: 0,
                failures: BTreeMap::new(),
                leader_term: None,
                gained_at: 0,
                seeding: None,
                last_completed: None,
            })
            .collect();
        Self {
            status: RaftControllerStatus {
                enabled: !options.start_paused,
                recovering: true,
                probe_rounds: 0,
                repair_requests: 0,
                activation_requests: 0,
                transfer_attempts: 0,
                completed_repairs: 0,
            },
            memory: Budget::new(options.metadata_bytes, 1),
            options,
            histories,
            inventory: 0,
            active: BTreeSet::new(),
            cursor: 0,
            active_cursor: None,
            priority_turn: false,
            intent: None,
            started: None,
            last_transfer: 0,
            next_at: 0,
            sequence: 0,
            epoch: 1,
            busy: None,
            stop,
        }
    }

    pub(super) fn stop(&self) {
        self.stop.send_replace(true);
    }

    fn next_sequence(&mut self) -> Option<u64> {
        match self.sequence.checked_add(1) {
            Some(sequence) => {
                self.sequence = sequence;
                Some(sequence)
            }
            None => {
                self.status.enabled = false;
                self.stop.send_replace(true);
                None
            }
        }
    }

    fn history(&mut self, index: usize, now: u64) -> Option<&mut History> {
        let history = self.histories.get_mut(index)?;
        if history.epoch != self.epoch {
            history.epoch = self.epoch;
            history.failures.clear();
            history.leader_term = None;
            history.gained_at = now;
            history.seeding = None;
        }
        Some(history)
    }

    fn next_history(&mut self) -> Option<usize> {
        if self.histories.is_empty() {
            return None;
        }
        self.priority_turn = !self.priority_turn;
        if self.priority_turn && !self.active.is_empty() {
            let after = self
                .active_cursor
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let index = self
                .active
                .range((after, std::ops::Bound::Unbounded))
                .next()
                .or_else(|| self.active.first())
                .copied()?;
            self.active_cursor = Some(index);
            return Some(index);
        }
        let index = self.cursor;
        self.cursor = (self.cursor + 1) % self.histories.len();
        Some(index)
    }
}

impl Driver {
    pub(super) fn publish_controller_resources(&self) {
        let sample = self.controller.as_ref().and_then(|controller| {
            Some((
                self.log.domain().mono_ms().ok()?,
                crate::application::consensus::RaftControllerResources {
                    enabled: controller.status.enabled,
                    recovering: controller.status.recovering,
                    busy: controller.busy.is_some(),
                    metadata: crate::application::consensus::RaftBudgetUsage::capture(
                        &controller.memory,
                    ),
                    probe_rounds: controller.status.probe_rounds,
                    repair_requests: controller.status.repair_requests,
                    activation_requests: controller.status.activation_requests,
                    transfer_attempts: controller.status.transfer_attempts,
                    completed_repairs: controller.status.completed_repairs,
                    sample_age_ms: 0,
                },
            ))
        });
        if let Ok(mut source) = self.local.controller.lock() {
            *source = sample;
        }
    }

    pub(super) fn controller_control(&mut self, mut request: NodeRequest, enabled: Option<bool>) {
        let result = (|| {
            let controller = self
                .controller
                .as_mut()
                .ok_or(CommandFailure::UnsupportedCapability)?;
            if let Some(enabled) = enabled {
                if enabled != controller.status.enabled {
                    let now = self
                        .log
                        .domain()
                        .mono_ms()
                        .map_err(|_| CommandFailure::Unavailable)?;
                    let epoch = controller
                        .epoch
                        .checked_add(1)
                        .ok_or(CommandFailure::Unavailable)?;
                    controller.stop.send_replace(true);
                    let (stop, _) = watch::channel(false);
                    controller.stop = stop;
                    controller.epoch = epoch;
                    controller.status.enabled = enabled;
                    controller.status.recovering = true;
                    controller.inventory = 0;
                    controller.active.clear();
                    controller.last_transfer = now;
                    controller.next_at = now.saturating_add(controller.options.interval_ms);
                }
            }
            Ok(Response::Controller(controller.status.clone()))
        })();
        reply_node(&mut request, result);
    }

    pub(super) fn pump_controller(&mut self) {
        if self.local.admission.is_draining() {
            if let Some(controller) = &self.controller {
                controller.stop();
            }
            return;
        }
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let Some(controller) = self.controller.as_mut() else {
            return;
        };
        if !controller.status.enabled || controller.busy.is_some() || now < controller.next_at {
            return;
        }
        if controller.started.is_none() {
            controller.started = Some(now);
            controller.last_transfer = now;
        }
        // Reconstruct existing durable plans in bounded batches before any new
        // automatic plan. Directory state cannot supply or clear this intent.
        if controller.status.recovering {
            let end = (controller.inventory + 4).min(controller.histories.len());
            for index in controller.inventory..end {
                let scope = &controller.histories[index].binding.scope;
                let group = match self.log.group(scope) {
                    Ok(group) => group,
                    Err(super::super::storage::LogError::NotFound) => continue,
                    Err(_) => {
                        controller.next_at = now.saturating_add(controller.options.interval_ms);
                        return;
                    }
                };
                let active = match group
                    .membership()
                    .and_then(|membership| membership.repair_status())
                {
                    Ok(status) => status.is_some_and(|status| status.phase.active()),
                    Err(_) => {
                        controller.next_at = now.saturating_add(controller.options.interval_ms);
                        return;
                    }
                };
                if active {
                    controller.active.insert(index);
                }
            }
            controller.inventory = end;
            if end != controller.histories.len() {
                return;
            }
            controller.status.recovering = false;
            if let Some((index, _)) = controller.intent.as_ref() {
                controller.active.insert(*index);
            }
        }
        let Some(index) = controller.next_history() else {
            return;
        };
        let options = controller.options.clone();
        let Some(memory) = controller
            .memory
            .acquire(options.probes_per_group * PROBE_MEMORY)
        else {
            return;
        };
        let Some(sequence) = controller.next_sequence() else {
            return;
        };
        let epoch = controller.epoch;
        let stop = controller.stop.subscribe();
        let Some(history) = controller.history(index, now) else {
            return;
        };
        let binding = history.binding.clone();
        let cursor = if binding.member().is_some() {
            &mut history.candidate_cursor
        } else {
            &mut history.pull_cursor
        };
        let setup = probe::targets(
            &binding.scope,
            &self.cells,
            &self.peers,
            self.log.limits().node,
            *cursor,
            options.probes_per_group,
        );
        let (targets, next) = match setup {
            Ok(setup) => setup,
            Err(_) => {
                controller.next_at = now.saturating_add(options.interval_ms);
                return;
            }
        };
        *cursor = next;
        controller.busy = Some(sequence);
        controller.next_at = now.saturating_add(options.interval_ms);
        let Some(count) = controller.status.probe_rounds.checked_add(1) else {
            controller.status.enabled = false;
            controller.stop.send_replace(true);
            return;
        };
        controller.status.probe_rounds = count;
        let work = probe::Work {
            local: RaftCellHandle {
                handle: self.local.clone(),
                binding,
            },
            transport: self.transport.clone(),
            runtime: self.runtime.clone(),
            domain: self.log.domain(),
            options,
            targets,
            memory,
        };
        let performance = self.local.performance.clone();
        self.jobs.push(Box::pin(async move {
            let timer = performance.timer(crate::raft::performance::Operation::ControllerProbe);
            let result = action::until_stopped(stop, work.run()).await;
            timer.finish(result.is_ok());
            Job::Controller(Box::new(Completed::Observed {
                sequence,
                epoch,
                history: index,
                result: result.map(Box::new),
            }))
        }));
    }

    pub(super) fn finish_controller(&mut self, completed: Completed) {
        let (sequence, epoch, index) = match &completed {
            Completed::Observed {
                sequence,
                epoch,
                history,
                ..
            }
            | Completed::Acted {
                sequence,
                epoch,
                history,
                ..
            } => (*sequence, *epoch, *history),
        };
        let Some(controller) = self.controller.as_mut() else {
            return;
        };
        if controller.busy != Some(sequence) {
            return;
        }
        controller.busy = None;
        if !controller.status.enabled
            || controller.epoch != epoch
            || self.closing.load(Ordering::Acquire)
            || self.local.admission.is_draining()
        {
            return;
        }
        let observed = match completed {
            Completed::Observed {
                result: Ok(observed),
                ..
            } => observed,
            Completed::Acted {
                result: Err(error), ..
            } => {
                tracing::debug!(%error, "automatic operation needs committed-state reconciliation");
                return;
            }
            _ => return,
        };
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let context = policy::Context {
            cells: &self.cells,
            peers: &self.peers,
            local: self.log.limits().node,
            rng: &mut self.rng,
        };
        let decision = policy::decide(controller, index, *observed, now, context);
        if let Ok(Some(action)) = decision {
            self.start_controller_action(index, action, now);
        }
    }
}
