// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reserve fresh children and admit one exact managed split or merge.

use super::*;
use crate::raft::{
    catalog::{CatalogError, InitialMember},
    cell_groups::{CellGroupBirthSpec, DirectoryMutation, TransitionPhase},
    fleet_catalog::TransitionOperatorStep,
};
use cellar_logic::{
    cell_group::{CellId, GroupHistory, GroupRoute, RouteEpoch},
    cell_group_placement::{PartitionBasis, PartitionDecision},
    consensus::{GroupId, IncarnationId, MemberId},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

enum Admission<'a> {
    Fresh(&'a [&'a PartitionBasis]),
    Rooted,
}

struct PreparedOperator {
    snapshot: crate::raft::fleet_catalog::FleetSnapshot,
    step: TransitionOperatorStep,
    plan: DirectoryMutation,
    proposals: Vec<CellGroupBirthSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionGroup {
    pub group_id: String,
    pub incarnation_id: String,
}

impl RaftFleetTransitionGroup {
    pub(super) fn history(&self, directory: &RangeDirectory) -> Result<GroupHistory> {
        let config = directory.namespace_config();
        Ok(GroupHistory {
            cluster: config.cluster,
            namespace: config.namespace,
            group: GroupId::new(records::id(&self.group_id)?)
                .map_err(|_| RaftFleetFailure::invalid("transition group ID"))?,
            incarnation: IncarnationId::new(records::id(&self.incarnation_id)?)
                .map_err(|_| RaftFleetFailure::invalid("transition incarnation ID"))?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum RaftFleetTransitionPartition {
    Split {
        parent: RaftFleetTransitionGroup,
        pivot_cell: String,
    },
    Merge {
        left: RaftFleetTransitionGroup,
        right: RaftFleetTransitionGroup,
    },
}

impl RaftFleetTransitionPartition {
    fn parents(&self) -> Vec<&RaftFleetTransitionGroup> {
        match self {
            Self::Split { parent, .. } => vec![parent],
            Self::Merge { left, right } => vec![left, right],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionChild {
    pub group: RaftFleetTransitionGroup,
    pub request_id: String,
    pub creation_id: String,
    pub voters: [RaftFleetFirstGroupVoter; 3],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionAdmissionRequest {
    pub catalog_id: String,
    pub namespace_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub expected_fleet_revision: u64,
    pub expected_directory_digest: String,
    pub class: String,
    pub partition: RaftFleetTransitionPartition,
    /// In key order for a split. Each child has its own exact reservation ID.
    pub children: Vec<RaftFleetTransitionChild>,
}

impl RaftFleetTransitionAdmissionRequest {
    pub fn validate(&self, namespace: &str, operation: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.expected_directory_digest)?;
        records::revision(self.expected_fleet_revision)?;
        records::id(namespace)?;
        records::id(operation)?;
        let children = match &self.partition {
            RaftFleetTransitionPartition::Split { pivot_cell, .. } => {
                CellId::new(pivot_cell)
                    .map_err(|_| RaftFleetFailure::invalid("split pivot cell"))?;
                2
            }
            RaftFleetTransitionPartition::Merge { .. } => 1,
        };
        if self.namespace_id != namespace
            || self.operation_id != operation
            || self.children.len() != children
            || self.class.is_empty()
            || self.class.len() > 128
        {
            return Err(RaftFleetFailure::invalid("transition admission selector"));
        }
        let mut groups = BTreeSet::new();
        for group in self
            .partition
            .parents()
            .into_iter()
            .chain(self.children.iter().map(|child| &child.group))
        {
            records::id(&group.incarnation_id)?;
            if !groups.insert(records::id(&group.group_id)?) {
                return Err(RaftFleetFailure::invalid(
                    "transition group IDs must be distinct",
                ));
            }
        }
        let mut requests = BTreeSet::from([records::id(&self.request_id)?]);
        for child in &self.children {
            records::id(&child.creation_id)?;
            if !requests.insert(records::id(&child.request_id)?) {
                return Err(RaftFleetFailure::invalid(
                    "transition request IDs must be distinct",
                ));
            }
            let nodes = child
                .voters
                .iter()
                .map(|voter| records::id(&voter.node_id))
                .collect::<Result<BTreeSet<_>>>()?;
            if nodes.len() != 3
                || child.voters[0].member_id == 0
                || child
                    .voters
                    .windows(2)
                    .any(|pair| pair[0].member_id >= pair[1].member_id)
            {
                return Err(RaftFleetFailure::invalid(
                    "transition voters must be distinct and ordered",
                ));
            }
        }
        Ok(())
    }

    fn child_proposals(
        &self,
        directory: &RangeDirectory,
        parents: &[GroupRoute],
    ) -> Result<Vec<CellGroupBirthSpec>> {
        let expected = self
            .partition
            .parents()
            .into_iter()
            .map(|parent| parent.history(directory))
            .collect::<Result<Vec<_>>>()?;
        if !parents.iter().map(|route| route.history).eq(expected) {
            return Err(FleetCatalogError::Conflict.into());
        }
        let ranges = match &self.partition {
            RaftFleetTransitionPartition::Split { pivot_cell, .. } => {
                let cell = CellId::new(pivot_cell)
                    .map_err(|_| RaftFleetFailure::invalid("split pivot cell"))?;
                let pivot = crate::application::consensus::cell_routing_key(
                    parents[0].history.namespace,
                    &cell,
                );
                let (left, right) = parents[0].range.split(pivot).map_err(|_| {
                    RaftFleetFailure::invalid("split pivot must be inside the parent range")
                })?;
                vec![left, right]
            }
            RaftFleetTransitionPartition::Merge { .. } => {
                vec![parents[0].range.merge(&parents[1].range).map_err(|_| {
                    RaftFleetFailure::invalid("merge parents must be adjacent and ordered")
                })?]
            }
        };
        let epoch = parents
            .iter()
            .map(|route| route.epoch.get())
            .max()
            .and_then(|epoch| epoch.checked_add(1))
            .and_then(|epoch| RouteEpoch::new(epoch).ok())
            .ok_or_else(|| RaftFleetFailure::invalid("transition route epoch"))?;
        self.children
            .iter()
            .zip(ranges)
            .map(|(child, range)| {
                let voters = child
                    .voters
                    .iter()
                    .map(|voter| {
                        Ok(InitialMember {
                            node: NodeId::new(records::id(&voter.node_id)?)
                                .map_err(|_| RaftFleetFailure::invalid("transition voter node"))?,
                            member: MemberId::new(voter.member_id).map_err(|_| {
                                RaftFleetFailure::invalid("transition voter member")
                            })?,
                        })
                    })
                    .collect::<Result<_>>()?;
                Ok(CellGroupBirthSpec {
                    history: child.group.history(directory)?,
                    range,
                    epoch,
                    creation: ProposalId::new(records::id(&child.creation_id)?)
                        .map_err(|_| RaftFleetFailure::invalid("transition child creation"))?,
                    voters,
                    learners: vec![],
                })
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionBasis {
    pub catalog_id: String,
    pub namespace_id: String,
    pub fleet_revision: u64,
    pub directory_revision: u64,
    pub directory_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetTransitionAdmission {
    pub request_id: String,
    pub operation_id: String,
    pub plan_digest: String,
    pub phase: TransitionPhase,
}

impl RaftFleetControl {
    /// Cancel selected work before managed transition admission. The durable
    /// fence wins before any birth cleanup. Original child identities and all
    /// reserved physical debt remain retained; an admitted or already live
    /// child refuses completion rather than erasing possible ownership.
    pub async fn cancel_staged_partition(
        &self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<()> {
        request.validate(&request.namespace_id, &request.operation_id)?;
        self.check_catalog(&request.catalog_id)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let directory = self.managed_namespace(&request.namespace_id)?;
        let operation = ProposalId::new(records::id(&request.operation_id)?)
            .map_err(|_| RaftFleetFailure::invalid("transition operation ID"))?;
        if self
            .catalog
            .pending_partition_work(turn.remaining()?)
            .await?
            .is_some_and(|work| work.operation == operation.bytes() && work.principal != principal)
        {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::Forbidden,
                outcome: RaftFleetOutcome::NotSubmitted,
                message: "Selected partition belongs to another operator principal.".into(),
            });
        }
        let digest = Sha256::digest(
            serde_json::to_vec(&(principal, request))
                .map_err(|_| RaftFleetFailure::invalid("transition admission payload"))?,
        )
        .into();
        let plan = self
            .catalog
            .reopen_transition_operator_plan(directory, operation, digest, turn.remaining()?)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        directory
            .transition_routes(&plan)
            .map_err(directory_failure)?;
        Box::pin(self.catalog.cancel_partition_work(
            directory,
            operation,
            digest,
            principal,
            turn.remaining()?,
        ))
        .await?;
        Ok(())
    }

    pub async fn transition_basis(
        &self,
        namespace: &str,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionBasis> {
        let directory = self.managed_namespace(namespace)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let snapshot = self.catalog.read(turn.remaining()?).await?;
        let view = self
            .domain
            .run(crate::asyncrt::timeout(
                turn.remaining()?,
                directory.observe(),
            ))
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(directory_failure)?;
        Ok(RaftFleetTransitionBasis {
            catalog_id: self.catalog_id.clone(),
            namespace_id: namespace.into(),
            fleet_revision: snapshot.revision(),
            directory_revision: view.revision(),
            directory_digest: hex::encode(view.digest()),
        })
    }

    /// Stage the exact plan, reconcile prior child reservations, then admit
    /// the replacement with the existing disruption fence.
    pub async fn admit_transition(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        Box::pin(self.admit_transition_inner(
            principal,
            namespace,
            operation,
            request,
            Admission::Fresh(&[]),
            timeout,
        ))
        .await
    }

    /// Admit the exact split or merge selected by a placement recommendation.
    /// Fresh work requires live source quorum checks. A rooted plan can resume
    /// its original reservations even after the observed leadership changes.
    pub async fn admit_recommended_partition(
        &self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        recommendation: PartitionDecision<'_>,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        let sources = recommended_sources(request, recommendation)?;
        Box::pin(self.admit_transition_inner(
            principal,
            &request.namespace_id,
            &request.operation_id,
            request,
            Admission::Fresh(&sources),
            timeout,
        ))
        .await
    }

    /// Resume a verified operator plan without a new sizing observation. The
    /// inner read must still find that plan; it may never fall back to Fresh.
    pub(super) async fn resume_partition_admission(
        &self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        Box::pin(self.admit_transition_inner(
            principal,
            &request.namespace_id,
            &request.operation_id,
            request,
            Admission::Rooted,
            timeout,
        ))
        .await
    }

    async fn admit_transition_inner(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        admission: Admission<'_>,
        timeout: Duration,
    ) -> Result<RaftFleetTransitionAdmission> {
        let turn = Turn::new(&self.domain, timeout)?;
        let PreparedOperator {
            mut snapshot,
            step,
            plan,
            proposals,
        } = self
            .prepare_transition_operator(
                principal,
                namespace,
                operation,
                request,
                admission,
                turn.remaining()?,
            )
            .await?;
        let directory = self.managed_namespace(namespace)?;
        let operation = step.operation;
        let digest = step.request_digest;

        let identity = if let Some(admitted) = self
            .catalog
            .reopen_managed_transition(directory, operation, turn.remaining()?)
            .await?
        {
            if admitted.request != step.identity(admitted.request.expected_revision)
                || admitted.plan.digest() != plan.digest()
            {
                return Err(FleetCatalogError::PayloadConflict.into());
            }
            admitted.request
        } else {
            let (_, children) = directory
                .transition_routes(&plan)
                .map_err(directory_failure)?;
            self.domain
                .run(crate::asyncrt::timeout(
                    turn.remaining()?,
                    directory.validate_managed_transition_input(&plan),
                ))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(|_| FleetCatalogError::Unconfirmed)?
                .map_err(directory_failure)?;
            for ((child, proposal), route) in request.children.iter().zip(proposals).zip(children) {
                let child_step = TransitionOperatorStep {
                    operation,
                    principal: principal.into(),
                    request_digest: digest,
                    step: records::id(&child.request_id)?,
                };
                if self
                    .catalog
                    .reopen_transition_operator_child(
                        &snapshot,
                        directory,
                        &route,
                        &child_step,
                        turn.remaining()?,
                    )
                    .await?
                {
                    continue;
                }
                let identity = child_step.identity(snapshot.revision());
                let reserved = self
                    .catalog
                    .reserve_birth(
                        &snapshot,
                        &identity,
                        directory,
                        proposal,
                        &request.class,
                        turn.remaining()?,
                    )
                    .await?;
                if reserved.authorization.route() != &route {
                    return Err(
                        FleetCatalogError::Corrupt("operator child reservation route").into(),
                    );
                }
                snapshot = reserved.commit.snapshot;
            }
            step.identity(snapshot.revision())
        };
        let journal = self
            .catalog
            .plan_managed_transition(&identity, directory, &plan, turn.remaining()?)
            .await?;
        Ok(RaftFleetTransitionAdmission {
            request_id: request.request_id.clone(),
            operation_id: request.operation_id.clone(),
            plan_digest: hex::encode(plan.digest()),
            phase: journal.phase(),
        })
    }

    async fn prepare_transition_operator(
        &self,
        principal: &str,
        namespace: &str,
        operation: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        admission: Admission<'_>,
        timeout: Duration,
    ) -> Result<PreparedOperator> {
        request.validate(namespace, operation)?;
        self.check_catalog(&request.catalog_id)?;
        let directory = self.managed_namespace(namespace)?;
        let operation = ProposalId::new(records::id(operation)?)
            .map_err(|_| RaftFleetFailure::invalid("transition operation ID"))?;
        let bytes = serde_json::to_vec(&(principal, request))
            .map_err(|_| RaftFleetFailure::invalid("transition admission payload"))?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let step = TransitionOperatorStep {
            operation,
            principal: principal.into(),
            request_digest: digest,
            step: records::id(&request.request_id)?,
        };
        let turn = Turn::new(&self.domain, timeout)?;
        let snapshot = self.catalog.read(turn.remaining()?).await?;
        let (plan, proposals) = if let Some(plan) = self
            .catalog
            .reopen_transition_operator_plan(directory, operation, digest, turn.remaining()?)
            .await?
        {
            let (parents, children) = directory
                .transition_routes(&plan)
                .map_err(directory_failure)?;
            let proposals = request.child_proposals(directory, &parents)?;
            if self
                .catalog
                .prepare_transition_child_routes(
                    &snapshot,
                    directory,
                    &request.class,
                    proposals.clone(),
                    turn.remaining()?,
                )
                .await?
                != children
            {
                return Err(FleetCatalogError::Corrupt("operator transition child binding").into());
            }
            (plan, proposals)
        } else {
            let sources = match admission {
                Admission::Fresh(sources) => sources,
                Admission::Rooted => return Err(FleetCatalogError::Conflict.into()),
            };
            if snapshot.revision() != request.expected_fleet_revision {
                return Err(FleetCatalogError::Conflict.into());
            }
            let (view, parents) = self
                .domain
                .run(Box::pin(crate::asyncrt::timeout(
                    turn.remaining()?,
                    Box::pin(async {
                        let view = directory.observe().await.map_err(directory_failure)?;
                        if hex::encode(view.digest()) != request.expected_directory_digest {
                            return Err(FleetCatalogError::Conflict.into());
                        }
                        let mut parents = Vec::new();
                        for parent in request.partition.parents() {
                            let birth = directory
                                .materialize_selected_history(parent.history(directory)?)
                                .await
                                .map_err(directory_failure)?;
                            parents.push(birth.route().clone());
                        }
                        Ok::<_, RaftFleetFailure>((view, parents))
                    }),
                )))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(|_| FleetCatalogError::Unconfirmed)??;
            if !sources.is_empty() {
                if sources.len() != parents.len()
                    || sources.iter().zip(&parents).any(|(source, route)| {
                        source.route != *route || source.directory_digest != view.digest()
                    })
                {
                    return Err(FleetCatalogError::Conflict.into());
                }
                let observer = self
                    .node_observer
                    .as_ref()
                    .ok_or_else(|| RaftFleetFailure {
                        code: RaftFleetFailureCode::Unsupported,
                        outcome: RaftFleetOutcome::NotSubmitted,
                        message: "Recommended admission requires a managed quorum observer.".into(),
                    })?;
                for source in sources {
                    Box::pin(self.catalog.require_partition_source(
                        &snapshot,
                        directory,
                        observer,
                        source,
                        turn.remaining()?,
                    ))
                    .await?;
                }
            }
            let proposals = request.child_proposals(directory, &parents)?;
            let children = self
                .catalog
                .prepare_transition_child_routes(
                    &snapshot,
                    directory,
                    &request.class,
                    proposals.clone(),
                    turn.remaining()?,
                )
                .await?;
            let plan = directory
                .prepare_replacement(&view, operation, &parents, &children)
                .map_err(directory_failure)?;
            self.catalog
                .stage_transition_operator_plan(
                    &snapshot,
                    directory,
                    &step.identity(snapshot.revision()),
                    digest,
                    &plan,
                    turn.remaining()?,
                )
                .await?;
            (plan, proposals)
        };

        Ok(PreparedOperator {
            snapshot,
            step,
            plan,
            proposals,
        })
    }

    /// Persist the exact scheduler request after live plan validation and before
    /// reserving either child. This record supplies no irreversible consent.
    pub(super) async fn stage_scheduled_partition(
        &self,
        principal: &str,
        request: &RaftFleetTransitionAdmissionRequest,
        recommendation: Option<PartitionDecision<'_>>,
        timeout: Duration,
    ) -> Result<()> {
        let turn = Turn::new(&self.domain, timeout)?;
        let sources = recommendation
            .map(|decision| recommended_sources(request, decision))
            .transpose()?;
        let admission = sources
            .as_ref()
            .map_or(Admission::Rooted, |sources| Admission::Fresh(sources));
        let prepared = self
            .prepare_transition_operator(
                principal,
                &request.namespace_id,
                &request.operation_id,
                request,
                admission,
                turn.remaining()?,
            )
            .await?;
        let node = self
            .node_observer
            .as_ref()
            .ok_or_else(RaftFleetFailure::unsupported)?
            .identity()
            .node;
        let payload = serde_json::to_string(&(principal, request))
            .map_err(|_| RaftFleetFailure::invalid("partition work request"))?;
        self.catalog
            .stage_partition_work(
                &prepared.snapshot,
                self.managed_namespace(&request.namespace_id)?,
                node,
                &prepared.step,
                payload,
                turn.remaining()?,
            )
            .await?;
        Ok(())
    }
}

pub(super) fn directory_failure(error: CatalogError) -> FleetCatalogError {
    match error {
        CatalogError::Unconfirmed => FleetCatalogError::Unconfirmed,
        CatalogError::Conflict(_) | CatalogError::Fenced => FleetCatalogError::Conflict,
        error => FleetCatalogError::from(error),
    }
}

fn recommended_sources<'a>(
    request: &RaftFleetTransitionAdmissionRequest,
    recommendation: PartitionDecision<'a>,
) -> Result<Vec<&'a PartitionBasis>> {
    crate::raft::cell_groups::partition::validate_recommendation(&recommendation)
        .map_err(directory_failure)?;
    let sources = match (&request.partition, recommendation) {
        (
            RaftFleetTransitionPartition::Split { pivot_cell, .. },
            PartitionDecision::Split { basis, pivot, .. },
        ) if pivot.cell().as_str() == pivot_cell => vec![basis],
        (RaftFleetTransitionPartition::Merge { .. }, PartitionDecision::Merge { left, right }) => {
            vec![left, right]
        }
        _ => {
            return Err(RaftFleetFailure::invalid(
                "partition recommendation mismatch",
            ))
        }
    };
    Ok(sources)
}
