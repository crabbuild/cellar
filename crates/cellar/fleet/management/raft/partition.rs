// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded adaptive sizing over live group observations and existing admission.
use super::*;
use crate::application::consensus::{CellGroupPartitionContext, CellGroupPartitionObservation};
use crate::raft::cell_groups::TransitionPhase;
use cellar_logic::{
    cell_group::{CellKey, GroupHistory},
    cell_group_placement::{
        PartitionBasis, PartitionDecision, PartitionPolicy, PartitionSample, PartitionTracker,
        SplitReason, WaitReason,
    },
};
use sha2::Digest as _;
use std::collections::BTreeMap;

/// Trusted deployment observations. These affect scheduling priority only;
/// capacity, failure-domain separation and serving authority are revalidated
/// by the Fleet, directory and Raft owners during admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftFleetPartitionPressure {
    pub placement_class: [u8; 32],
    pub leader_cpu_bps: u16,
    pub failure_domain_load_bps: u16,
}

/// Owned advisory decision. Keeping it does not retain quorum or a route lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RaftFleetPartitionRecommendation {
    Wait(WaitReason),
    Split {
        basis: Box<PartitionBasis>,
        pivot: CellKey,
        reason: SplitReason,
    },
    Merge {
        left: Box<PartitionBasis>,
        right: Box<PartitionBasis>,
    },
}

impl RaftFleetPartitionRecommendation {
    pub(super) fn from_decision(decision: PartitionDecision<'_>) -> Self {
        match decision {
            PartitionDecision::Wait(reason) => Self::Wait(reason),
            PartitionDecision::Split {
                basis,
                pivot,
                reason,
            } => Self::Split {
                basis: Box::new(basis.clone()),
                pivot: pivot.clone(),
                reason,
            },
            PartitionDecision::Merge { left, right } => Self::Merge {
                left: Box::new(left.clone()),
                right: Box::new(right.clone()),
            },
        }
    }

    fn decision(&self) -> PartitionDecision<'_> {
        match self {
            Self::Wait(reason) => PartitionDecision::Wait(*reason),
            Self::Split {
                basis,
                pivot,
                reason,
            } => PartitionDecision::Split {
                basis,
                pivot,
                reason: *reason,
            },
            Self::Merge { left, right } => PartitionDecision::Merge { left, right },
        }
    }
}

struct Tracked {
    previous: CellGroupPartitionObservation,
    directory_digest: [u8; 32],
    placement_class: [u8; 32],
    topology_at: u64,
    tracker: PartitionTracker,
}

struct Pending {
    principal: String,
    request: RaftFleetTransitionAdmissionRequest,
    recommendation: Option<RaftFleetPartitionRecommendation>,
}

/// One bounded local sampling turn. Recommendations are advisory; staging
/// journals the caller's exact operation/placement identities before admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftFleetPartitionPoll {
    pub sampled: Option<GroupHistory>,
    pub recommendation: Option<RaftFleetPartitionRecommendation>,
}

/// One explicitly driven node-local scheduling owner. Each observation samples
/// one selected local group; there is no fleet-wide inventory broadcast or
/// per-cell task. Call observation at the policy's interval and use the
/// existing managed transition steps after admission.
///
/// One scheduled operation is pending in the Fleet cursor. The exact request
/// and plan are selected in the Fleet root before child admission. Errors and lost
/// responses retain that work. A replacement discovers it with resume_pending;
/// it needs no caller-retained request IDs and cannot fall back to fresh work.
///
/// No background work is started. The caller owns polling and shutdown; all
/// physical work remains with the existing node and managed-transition owners.
pub struct RaftFleetPartitionScheduler {
    control: RaftFleetControl,
    policy: PartitionPolicy,
    max_groups: usize,
    groups: BTreeMap<GroupHistory, Tracked>,
    pending: Option<Pending>,
    inventory_cursor: Option<GroupHistory>,
    selection_cursor: Option<GroupHistory>,
    _memory: crate::raft::budget::Permit,
}

impl RaftFleetPartitionScheduler {
    pub fn new(
        control: RaftFleetControl,
        policy: PartitionPolicy,
        max_groups: usize,
    ) -> Result<Self> {
        if !(1..=128).contains(&max_groups) {
            return Err(RaftFleetFailure::invalid(
                "partition tracker capacity must be 1–128",
            ));
        }
        let observer = control
            .node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        // Previous observation and tracker each retain at most seven 1-KiB
        // cell IDs plus fixed metadata. Include inventory, selected candidate,
        // frozen request and transient clones in this existing control budget.
        let memory = observer
            .control_budget()
            .acquire(max_groups * 32 * 1024 + 64 * 1024)
            .ok_or(FleetCatalogError::Overloaded)?;
        Ok(Self {
            control,
            policy,
            max_groups,
            groups: BTreeMap::new(),
            pending: None,
            inventory_cursor: None,
            selection_cursor: None,
            _memory: memory,
        })
    }

    /// Discover active local groups in configured namespaces, sample one in
    /// round-robin order and select a fresh split or compatible adjacent merge.
    /// No cell enumeration, remote inventory broadcast or mutation occurs.
    /// The complete local cut must fit the explicit tracker capacity; overflow
    /// refuses rather than sampling only a favored prefix. Poll often enough
    /// for the configured group's rate windows and hysteresis to remain fresh.
    /// Pending work freezes discovery until its exact request is reconciled.
    pub async fn poll(
        &mut self,
        pressure: RaftFleetPartitionPressure,
        timeout: Duration,
    ) -> Result<RaftFleetPartitionPoll> {
        validate_pressure(pressure)?;
        let domain = self.control.domain.clone();
        let turn = Turn::new(&domain, timeout)?;
        self.restore_pending(turn.remaining()?).await?;
        if self.pending.is_some() {
            return Ok(RaftFleetPartitionPoll {
                sampled: None,
                recommendation: None,
            });
        }
        let scopes: Vec<_> = self
            .control
            .namespaces
            .iter()
            .map(|directory| {
                let config = directory.namespace_config();
                (config.cluster, config.namespace)
            })
            .collect();
        let groups = self
            .control
            .node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?
            .partition_groups(&scopes, self.max_groups)
            .map_err(|error| match error {
                crate::raft::completion::RuntimeError::Overloaded => {
                    FleetCatalogError::Limit.into()
                }
                other => observation_failure(other),
            })?;
        self.groups
            .retain(|history, _| groups.binary_search(history).is_ok());
        let selected = groups
            .iter()
            .copied()
            .find(|history| self.inventory_cursor.is_none_or(|cursor| *history > cursor))
            .or_else(|| groups.first().copied());
        if let Some(history) = selected {
            // Advance even on a failed sample, so one stale/failing owner
            // cannot monopolize later turns. Failed samples lose advisory state.
            self.inventory_cursor = Some(history);
            if let Err(error) = Box::pin(self.observe(history, pressure, turn.remaining()?)).await {
                self.groups.remove(&history);
                return Err(error);
            }
        }
        let recommendation = super::partition_selection::select(
            self.groups
                .values()
                .map(|tracked| (&tracked.previous.route, &tracked.tracker)),
            self.policy,
            self.now()?,
            self.selection_cursor,
        );
        turn.remaining()?;
        if let Some((history, _)) = &recommendation {
            self.selection_cursor = Some(*history);
        }
        Ok(RaftFleetPartitionPoll {
            sampled: selected,
            recommendation: recommendation.map(|(_, candidate)| candidate),
        })
    }

    pub fn tracked_groups(&self) -> usize {
        self.groups.len()
    }

    /// Original request retained even when an admission reply is lost.
    pub fn pending_request(&self) -> Option<&RaftFleetTransitionAdmissionRequest> {
        self.pending.as_ref().map(|pending| &pending.request)
    }

    /// Forget advisory samples only. An admitted or uncertain request retains
    /// its own frozen basis and is unaffected by sampling-cache eviction.
    pub fn forget(&mut self, history: GroupHistory) {
        self.groups.remove(&history);
    }

    /// Read one current directory selection and one live local group. The first
    /// observation (or a changed owner/term/membership/root) starts a fresh
    /// cooldown and rate window. Polling faster than the explicit sampling
    /// interval does not accumulate or destroy hysteresis.
    pub async fn observe(
        &mut self,
        history: GroupHistory,
        pressure: RaftFleetPartitionPressure,
        timeout: Duration,
    ) -> Result<Option<PartitionSample>> {
        validate_pressure(pressure)?;
        if !self.groups.contains_key(&history) && self.groups.len() == self.max_groups {
            return Err(FleetCatalogError::Limit.into());
        }
        let namespace = hex::encode(history.namespace.bytes());
        let directory = self.control.managed_namespace(&namespace)?;
        let observer = self
            .control
            .node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?;
        let turn = Turn::new(&self.control.domain, timeout)?;
        let (digest, route) = turn
            .run(async {
                let view = directory
                    .observe()
                    .await
                    .map_err(transition_admission::directory_failure)?;
                let birth = directory
                    .materialize_selected_history(history)
                    .await
                    .map_err(transition_admission::directory_failure)?;
                Ok::<_, RaftFleetFailure>((view.digest(), birth.route().clone()))
            })
            .await??;
        let group = observer.group(history).map_err(observation_failure)?;
        let current = group
            .observe_partition(turn.remaining()?)
            .await
            .map_err(observation_failure)?;
        if current.route != route {
            self.groups.remove(&history);
            return Err(FleetCatalogError::Conflict.into());
        }
        let now = current.workload.sampled_at_ms;
        let reset = || Tracked {
            previous: current.clone(),
            directory_digest: digest,
            placement_class: pressure.placement_class,
            topology_at: now,
            tracker: PartitionTracker::default(),
        };
        let Some(tracked) = self.groups.get_mut(&history) else {
            self.groups.insert(history, reset());
            return Ok(None);
        };
        if tracked.directory_digest != digest || tracked.placement_class != pressure.placement_class
        {
            *tracked = reset();
            return Ok(None);
        }
        if now == tracked.previous.workload.sampled_at_ms
            && current.same_workload_owner(&tracked.previous)
        {
            return Ok(None);
        }
        let context = CellGroupPartitionContext {
            directory_digest: digest,
            topology_changed_at_ms: tracked.topology_at,
            durability: directory.namespace_config().minimum_durability,
            placement_class: pressure.placement_class,
            leader_cpu_bps: pressure.leader_cpu_bps,
            failure_domain_load_bps: pressure.failure_domain_load_bps,
            active_and_stable: current.stable_leader,
        };
        let Some(sample) = current.sample_since(&tracked.previous, context) else {
            *tracked = reset();
            return Ok(None);
        };
        let elapsed = now.saturating_sub(tracked.previous.workload.sampled_at_ms);
        if elapsed < self.policy.limits().sample_interval_ms {
            return Ok(None);
        }
        if elapsed > self.policy.limits().max_sample_age_ms {
            *tracked = reset();
            return Ok(None);
        }
        tracked.tracker.observe(self.policy, sample.clone(), now);
        tracked.previous = current;
        Ok(Some(sample))
    }

    pub fn recommend_split(
        &self,
        history: GroupHistory,
    ) -> Result<RaftFleetPartitionRecommendation> {
        let Some(tracked) = self.groups.get(&history) else {
            return Ok(RaftFleetPartitionRecommendation::Wait(
                WaitReason::NoFreshSample,
            ));
        };
        Ok(RaftFleetPartitionRecommendation::from_decision(
            tracked.tracker.recommend_split(
                self.policy,
                self.now()?,
                u16::from(self.pending.is_some()),
            ),
        ))
    }

    pub fn recommend_merge(
        &self,
        left: GroupHistory,
        right: GroupHistory,
    ) -> Result<RaftFleetPartitionRecommendation> {
        let (Some(left), Some(right)) = (self.groups.get(&left), self.groups.get(&right)) else {
            return Ok(RaftFleetPartitionRecommendation::Wait(
                WaitReason::NoFreshSample,
            ));
        };
        Ok(RaftFleetPartitionRecommendation::from_decision(
            left.tracker.recommend_merge(
                &right.tracker,
                self.policy,
                self.now()?,
                u16::from(self.pending.is_some()),
            ),
        ))
    }

    /// Freeze the selected recommendation in the Fleet's rooted work slot before
    /// reserving children. This creates no execution or irreversible-seal consent.
    /// A lost reply must be reconciled with the same request or resume_pending.
    pub async fn stage(
        &mut self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<()> {
        let domain = self.control.domain.clone();
        let turn = Turn::new(&domain, timeout)?;
        self.restore_pending(turn.remaining()?).await?;
        if let Some(pending) = &self.pending {
            if pending.principal != principal || pending.request != *request {
                return Err(RaftFleetFailure {
                    code: RaftFleetFailureCode::Busy,
                    outcome: RaftFleetOutcome::NotSubmitted,
                    message: "Reconcile the original scheduled partition before admitting another."
                        .into(),
                });
            }
        } else {
            if principal.is_empty()
                || principal.len() > 128
                || principal.chars().any(char::is_control)
            {
                return Err(RaftFleetFailure::invalid("invalid operator principal"));
            }
            request.validate(&request.namespace_id, &request.operation_id)?;
            self.control.check_catalog(&request.catalog_id)?;
            let directory = self.control.managed_namespace(&request.namespace_id)?;
            let recommendation = match &request.partition {
                RaftFleetTransitionPartition::Split { parent, .. } => {
                    self.recommend_split(parent.history(directory)?)?
                }
                RaftFleetTransitionPartition::Merge { left, right } => {
                    self.recommend_merge(left.history(directory)?, right.history(directory)?)?
                }
            };
            if let RaftFleetPartitionRecommendation::Wait(reason) = recommendation {
                return Err(RaftFleetFailure {
                    code: RaftFleetFailureCode::Blocked,
                    outcome: RaftFleetOutcome::NotSubmitted,
                    message: format!(
                        "Partition sampling has not admitted a recommendation: {reason:?}."
                    ),
                });
            }
            if let (
                RaftFleetTransitionPartition::Split { pivot_cell, .. },
                RaftFleetPartitionRecommendation::Split { pivot, .. },
            ) = (&request.partition, &recommendation)
            {
                if pivot.cell().as_str() != pivot_cell {
                    return Err(RaftFleetFailure::invalid(
                        "partition recommendation mismatch",
                    ));
                }
            }
            self.pending = Some(Pending {
                principal: principal.into(),
                request: request.clone(),
                recommendation: Some(recommendation),
            });
        }
        let pending = self.pending.as_ref().ok_or(FleetCatalogError::Closed)?;
        Box::pin(
            self.control.stage_scheduled_partition(
                &pending.principal,
                &pending.request,
                pending
                    .recommendation
                    .as_ref()
                    .map(RaftFleetPartitionRecommendation::decision),
                turn.remaining()?,
            ),
        )
        .await
    }

    /// Select durable work, then use canonical admission to resume its exact
    /// reservations. The scheduler never allocates replacement identities after
    /// uncertainty and never treats staging as execution consent.
    pub async fn admit(
        &mut self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        let domain = self.control.domain.clone();
        let turn = Turn::new(&domain, timeout)?;
        self.stage(principal, request, turn.remaining()?).await?;
        Box::pin(
            self.control
                .resume_partition_admission(principal, request, turn.remaining()?),
        )
        .await
    }

    /// Discover and resume the Fleet's durable request with no caller-supplied
    /// operation or reservation IDs. A missing original plan refuses admission.
    pub async fn resume_pending(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<RaftFleetTransitionAdmission>> {
        let domain = self.control.domain.clone();
        let turn = Turn::new(&domain, timeout)?;
        self.restore_pending(turn.remaining()?).await?;
        let Some(pending) = &self.pending else {
            return Ok(None);
        };
        let principal = pending.principal.clone();
        let request = pending.request.clone();
        Box::pin(self.admit(&principal, &request, turn.remaining()?))
            .await
            .map(Some)
    }

    async fn restore_pending(&mut self, timeout: Duration) -> Result<()> {
        if self.pending.is_some() {
            return Ok(());
        }
        let turn = Turn::new(&self.control.domain, timeout)?;
        let Some(work) = self
            .control
            .catalog
            .pending_partition_work(turn.remaining()?)
            .await?
        else {
            return Ok(());
        };
        let (principal, request): (String, RaftFleetTransitionAdmissionRequest) =
            serde_json::from_str(&work.payload)
                .map_err(|_| FleetCatalogError::Corrupt("partition work payload"))?;
        request.validate(&request.namespace_id, &request.operation_id)?;
        self.control.check_catalog(&request.catalog_id)?;
        let directory = self.control.managed_namespace(&request.namespace_id)?;
        if directory.namespace().root_digest() != work.namespace
            || principal != work.principal
            || records::id(&request.operation_id)? != work.operation
            || serde_json::to_string(&(&principal, &request))
                .map_err(|_| FleetCatalogError::Corrupt("partition work payload"))?
                != work.payload
        {
            return Err(FleetCatalogError::Corrupt("partition work request binding").into());
        }
        let operation = ProposalId::new(work.operation)
            .map_err(|_| FleetCatalogError::Corrupt("partition work operation"))?;
        let plan = self
            .control
            .catalog
            .reopen_transition_operator_plan(directory, operation, work.request, turn.remaining()?)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        if plan.digest() != work.plan {
            return Err(FleetCatalogError::PayloadConflict.into());
        }
        self.pending = Some(Pending {
            principal,
            request,
            recommendation: None,
        });
        Ok(())
    }

    /// Resume an explicitly retained request. Use resume_pending for discovery.
    /// Only an already persisted operator plan may resume; absent or changed plans
    /// refuse without falling back to fresh unsampled admission.
    pub async fn resume(
        &mut self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        let turn = Turn::new(&self.control.domain, timeout)?;
        if self.pending.is_none() {
            request.validate(&request.namespace_id, &request.operation_id)?;
            self.control.check_catalog(&request.catalog_id)?;
            let directory = self.control.managed_namespace(&request.namespace_id)?;
            let operation = ProposalId::new(records::id(&request.operation_id)?)
                .map_err(|_| RaftFleetFailure::invalid("transition operation ID"))?;
            let digest = sha2::Sha256::digest(
                serde_json::to_vec(&(principal, request))
                    .map_err(|_| RaftFleetFailure::invalid("transition admission payload"))?,
            )
            .into();
            self.control
                .catalog
                .reopen_transition_operator_plan(directory, operation, digest, turn.remaining()?)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            self.pending = Some(Pending {
                principal: principal.into(),
                request: request.clone(),
                recommendation: None,
            });
        }
        Box::pin(self.admit(principal, request, turn.remaining()?)).await
    }

    /// Release the rooted cursor and local slot after observing its terminal
    /// phase. Absence, refusal, timeout and a lost reply all retain the slot.
    /// Physical reservations and archive holds are settled by their own owners.
    pub async fn settle(&mut self, timeout: Duration) -> Result<bool> {
        let domain = self.control.domain.clone();
        let turn = Turn::new(&domain, timeout)?;
        self.restore_pending(turn.remaining()?).await?;
        let Some(pending) = &self.pending else {
            return Ok(true);
        };
        let directory = self
            .control
            .managed_namespace(&pending.request.namespace_id)?;
        let digest = sha2::Sha256::digest(
            serde_json::to_vec(&(&pending.principal, &pending.request))
                .map_err(|_| RaftFleetFailure::invalid("transition admission payload"))?,
        )
        .into();
        let operation = ProposalId::new(records::id(&pending.request.operation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("transition operation ID"))?;
        let Some(plan) = self
            .control
            .catalog
            .reopen_transition_operator_plan(directory, operation, digest, turn.remaining()?)
            .await?
        else {
            return Ok(false);
        };
        let status = self
            .control
            .transition_status(
                &pending.request.namespace_id,
                &pending.request.operation_id,
                turn.remaining()?,
            )
            .await?;
        if status.plan_digest != hex::encode(plan.digest()) {
            return Err(FleetCatalogError::Conflict.into());
        }
        if status.fleet_slot_held
            || !matches!(
                status.phase,
                Some(TransitionPhase::Retired | TransitionPhase::Cancelled)
            )
        {
            return Ok(false);
        }
        if !self
            .control
            .catalog
            .finish_partition_work(directory, operation, digest, turn.remaining()?)
            .await?
        {
            return Ok(false);
        }
        self.pending = None;
        self.groups.clear();
        Ok(true)
    }

    fn now(&self) -> Result<u64> {
        self.control
            .node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?
            .handle
            .domain
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed.into())
    }
}

fn validate_pressure(pressure: RaftFleetPartitionPressure) -> Result<()> {
    if pressure.placement_class == [0; 32]
        || pressure.leader_cpu_bps > 10_000
        || pressure.failure_domain_load_bps > 10_000
    {
        return Err(RaftFleetFailure::invalid("partition pressure observation"));
    }
    Ok(())
}

fn observation_failure(error: crate::raft::completion::RuntimeError) -> RaftFleetFailure {
    match error {
        crate::raft::completion::RuntimeError::Overloaded => FleetCatalogError::Overloaded.into(),
        _ => FleetCatalogError::Unconfirmed.into(),
    }
}
