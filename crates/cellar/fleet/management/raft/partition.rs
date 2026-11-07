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
    fn from_decision(decision: PartitionDecision<'_>) -> Self {
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
    recommendation: RaftFleetPartitionRecommendation,
}

/// One explicitly driven node-local scheduling owner. Each observation samples
/// one selected local group; there is no fleet-wide inventory broadcast or
/// per-cell task. Call observation at the policy's interval and use the
/// existing managed transition steps after admission.
///
/// At most one operation can be pending here. It is frozen *before* dispatch,
/// including across cancellation of the admission future. Errors and timeouts
/// never release it or allocate new identities. Persist the exact request in
/// the operator's work journal before dispatch; after controller restart the
/// existing managed transition API resumes its rooted reservations. This
/// ephemeral sampler is not a replacement for that durable journal.
///
/// No background work is started. The caller owns polling and shutdown; all
/// physical work remains with the existing node and managed-transition owners.
pub struct RaftFleetPartitionScheduler {
    control: RaftFleetControl,
    policy: PartitionPolicy,
    max_groups: usize,
    groups: BTreeMap<GroupHistory, Tracked>,
    pending: Option<Pending>,
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
        if control.node_observer.is_none() {
            return Err(RaftFleetFailure::unsupported());
        }
        Ok(Self {
            control,
            policy,
            max_groups,
            groups: BTreeMap::new(),
            pending: None,
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
        if pressure.placement_class == [0; 32]
            || pressure.leader_cpu_bps > 10_000
            || pressure.failure_domain_load_bps > 10_000
        {
            return Err(RaftFleetFailure::invalid("partition pressure observation"));
        }
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

    /// Submit one recommendation through canonical operator admission. The
    /// caller supplies stable operation, child, creation and reservation IDs
    /// and proposed voters. Scheduling never invents capacity or membership.
    ///
    /// An exact retry uses the original frozen recommendation. If its plan was
    /// rooted, admission resumes that plan; otherwise live source checks still
    /// have to succeed. A changed payload cannot replace uncertain work.
    pub async fn admit(
        &mut self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        let turn = Turn::new(&self.control.domain, timeout)?;
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
                recommendation,
            });
        }
        let pending = self.pending.as_ref().ok_or(FleetCatalogError::Closed)?;
        Box::pin(self.control.admit_recommended_partition(
            &pending.principal,
            &pending.request,
            pending.recommendation.decision(),
            turn.remaining()?,
        ))
        .await
    }

    /// Release the local operation slot only after observing its rooted terminal
    /// phase. Absence, refusal, timeout and a lost reply all retain the slot.
    /// Physical reservations and archive holds are settled by their own owners.
    pub async fn settle(&mut self, timeout: Duration) -> Result<bool> {
        let Some(pending) = &self.pending else {
            return Ok(true);
        };
        let turn = Turn::new(&self.control.domain, timeout)?;
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

fn observation_failure(error: crate::raft::completion::RuntimeError) -> RaftFleetFailure {
    match error {
        crate::raft::completion::RuntimeError::Overloaded => FleetCatalogError::Overloaded.into(),
        _ => FleetCatalogError::Unconfirmed.into(),
    }
}
