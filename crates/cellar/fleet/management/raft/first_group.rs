// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated first-group requests use the configured managed directory.
use super::*;
use crate::raft::{
    catalog::InitialMember, cell_groups::CellGroupBirthSpec,
    fleet_catalog::RaftManagedFirstGroupRequest,
};
use cellar_logic::{
    cell_group::{CellRange, GroupHistory, RouteEpoch},
    consensus::{GroupId, IncarnationId, MemberId, NamespaceId},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetFirstGroupStandby {
    pub catalog_id: String,
    pub namespace_id: String,
    pub standby: String,
}

impl RaftFleetFirstGroupStandby {
    pub fn validate(&self) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::id(&self.namespace_id)?;
        records::catalog_id(&self.standby)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetFirstGroupVoter {
    pub node_id: String,
    pub member_id: u64,
}

/// Keep this complete plan for reserve and publish. Voters are ordered by
/// ascending member ID. No address or application class comes from the caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetFirstGroupPlan {
    pub catalog_id: String,
    pub namespace_id: String,
    pub standby: String,
    pub request_id: String,
    pub operation_id: String,
    pub expected_revision: u64,
    pub publication_id: String,
    pub group_id: String,
    pub incarnation_id: String,
    pub creation_id: String,
    pub voters: [RaftFleetFirstGroupVoter; 3],
}

impl RaftFleetFirstGroupPlan {
    pub fn validate(&self, namespace: &str) -> Result<()> {
        records::catalog_id(&self.catalog_id)?;
        records::catalog_id(&self.standby)?;
        records::id(namespace)?;
        if namespace != self.namespace_id {
            return Err(RaftFleetFailure::invalid("first group namespace binding"));
        }
        for id in [
            &self.request_id,
            &self.operation_id,
            &self.publication_id,
            &self.group_id,
            &self.incarnation_id,
            &self.creation_id,
        ] {
            records::id(id)?;
        }
        records::revision(self.expected_revision)?;
        if self
            .voters
            .windows(2)
            .any(|pair| pair[0].member_id >= pair[1].member_id)
            || self
                .voters
                .iter()
                .map(|voter| &voter.node_id)
                .collect::<BTreeSet<_>>()
                .len()
                != 3
        {
            return Err(RaftFleetFailure::invalid(
                "first group voters must be distinct and ordered",
            ));
        }
        for voter in &self.voters {
            records::id(&voter.node_id)?;
            MemberId::new(voter.member_id)
                .map_err(|_| RaftFleetFailure::invalid("first group member ID"))?;
        }
        Ok(())
    }

    fn setup(
        &self,
        principal: &str,
        directory: &RangeDirectory,
    ) -> Result<RaftManagedFirstGroupRequest> {
        let config = directory.namespace_config();
        let mut standby = [0; 32];
        hex::decode_to_slice(&self.standby, &mut standby)
            .map_err(|_| RaftFleetFailure::invalid("first group standby digest"))?;
        let voters = self
            .voters
            .iter()
            .map(|voter| {
                Ok(InitialMember {
                    node: NodeId::new(records::id(&voter.node_id)?)
                        .map_err(|_| RaftFleetFailure::invalid("first group node ID"))?,
                    member: MemberId::new(voter.member_id)
                        .map_err(|_| RaftFleetFailure::invalid("first group member ID"))?,
                })
            })
            .collect::<Result<_>>()?;
        Ok(RaftManagedFirstGroupRequest {
            reservation: self.identity(principal, &self.request_id, self.expected_revision)?,
            standby,
            publication: proposal(&self.publication_id)?,
            group: CellGroupBirthSpec {
                history: GroupHistory {
                    cluster: config.cluster,
                    namespace: config.namespace,
                    group: GroupId::new(records::id(&self.group_id)?)
                        .map_err(|_| RaftFleetFailure::invalid("first group ID"))?,
                    incarnation: IncarnationId::new(records::id(&self.incarnation_id)?)
                        .map_err(|_| RaftFleetFailure::invalid("first group incarnation"))?,
                },
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)
                    .map_err(|_| RaftFleetFailure::invalid("first group epoch"))?,
                creation: proposal(&self.creation_id)?,
                voters,
                learners: vec![],
            },
        })
    }

    fn identity(
        &self,
        principal: &str,
        request: &str,
        revision: u64,
    ) -> Result<FleetRequestIdentity> {
        Ok(FleetRequestIdentity {
            id: records::id(request)?,
            operation: records::id(&self.operation_id)?,
            principal: principal.into(),
            expected_revision: revision,
        })
    }
}

fn proposal(id: &str) -> Result<ProposalId> {
    ProposalId::new(records::id(id)?)
        .map_err(|_| RaftFleetFailure::invalid("first group proposal ID"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetFirstGroupMutation {
    pub plan: RaftFleetFirstGroupPlan,
    pub request_id: String,
    pub expected_revision: u64,
}

pub type RaftFleetFirstGroupPublish = RaftFleetFirstGroupMutation;

impl RaftFleetFirstGroupMutation {
    pub fn validate(&self, namespace: &str) -> Result<()> {
        self.plan.validate(namespace)?;
        records::id(&self.request_id)?;
        records::revision(self.expected_revision)?;
        if self.request_id == self.plan.request_id
            || self.expected_revision <= self.plan.expected_revision
        {
            return Err(RaftFleetFailure::invalid(
                "first group confirmation identity",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftFleetFirstGroupStage {
    Reserved,
    Published,
    ArchiveInitialized,
}

/// Metadata admission for one exact request. A voter still needs its own
/// activation and a fresh quorum before the application can serve.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaftFleetFirstGroupAcceptance {
    pub plan: RaftFleetFirstGroupPlan,
    pub request_id: String,
    pub expected_revision: u64,
    pub stage: RaftFleetFirstGroupStage,
}

impl RaftFleetFirstGroupAcceptance {
    pub(crate) fn from_plan(
        plan: &RaftFleetFirstGroupPlan,
        request_id: &str,
        expected_revision: u64,
        stage: RaftFleetFirstGroupStage,
    ) -> Self {
        Self {
            plan: plan.clone(),
            request_id: request_id.into(),
            expected_revision,
            stage,
        }
    }
}

impl RaftFleetControl {
    pub async fn first_group_standby(
        &self,
        namespace: &str,
        timeout: Duration,
    ) -> Result<RaftFleetFirstGroupStandby> {
        let directory = self.first_group_directory(namespace)?;
        let standby = self
            .catalog
            .observe_managed_standby_at(directory, timeout)
            .await?;
        Ok(RaftFleetFirstGroupStandby {
            catalog_id: self.catalog_id.clone(),
            namespace_id: namespace.into(),
            standby: hex::encode(standby),
        })
    }

    pub async fn reserve_first_group(
        &self,
        principal: &str,
        namespace: &str,
        plan: &RaftFleetFirstGroupPlan,
        timeout: Duration,
    ) -> Result<RaftFleetFirstGroupAcceptance> {
        plan.validate(namespace)?;
        self.check_catalog(&plan.catalog_id)?;
        let directory = self.first_group_directory(namespace)?;
        let setup = plan.setup(principal, directory)?;
        self.catalog
            .reserve_managed_first_group_at(directory, setup, timeout)
            .await?;
        Ok(RaftFleetFirstGroupAcceptance::from_plan(
            plan,
            &plan.request_id,
            plan.expected_revision,
            RaftFleetFirstGroupStage::Reserved,
        ))
    }

    pub async fn publish_first_group(
        &self,
        principal: &str,
        namespace: &str,
        request: &RaftFleetFirstGroupPublish,
        timeout: Duration,
    ) -> Result<RaftFleetFirstGroupAcceptance> {
        request.validate(namespace)?;
        let plan = &request.plan;
        self.check_catalog(&plan.catalog_id)?;
        let directory = self.first_group_directory(namespace)?;
        let setup = plan.setup(principal, directory)?;
        let confirmation =
            plan.identity(principal, &request.request_id, request.expected_revision)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let prepared = self
            .catalog
            .reopen_managed_first_group_at(directory, setup, turn.remaining()?)
            .await?;
        self.catalog
            .publish_managed_first_group(&prepared, &confirmation, turn.remaining()?)
            .await?;
        Ok(RaftFleetFirstGroupAcceptance::from_plan(
            plan,
            &request.request_id,
            request.expected_revision,
            RaftFleetFirstGroupStage::Published,
        ))
    }

    pub async fn initialize_first_group_archive(
        &self,
        principal: &str,
        namespace: &str,
        request: &RaftFleetFirstGroupMutation,
        timeout: Duration,
    ) -> Result<RaftFleetFirstGroupAcceptance> {
        request.validate(namespace)?;
        let plan = &request.plan;
        self.check_catalog(&plan.catalog_id)?;
        let directory = self.first_group_directory(namespace)?;
        let setup = plan.setup(principal, directory)?;
        let identity = plan.identity(principal, &request.request_id, request.expected_revision)?;
        let turn = Turn::new(&self.domain, timeout)?;
        let prepared = self
            .catalog
            .reopen_managed_first_group_at(directory, setup, turn.remaining()?)
            .await?;
        self.catalog
            .initialize_managed_first_group_archive(
                &prepared,
                &identity,
                &self.archive_pool,
                turn.remaining()?,
            )
            .await?;
        Ok(RaftFleetFirstGroupAcceptance::from_plan(
            plan,
            &request.request_id,
            request.expected_revision,
            RaftFleetFirstGroupStage::ArchiveInitialized,
        ))
    }

    fn first_group_directory(&self, namespace: &str) -> Result<&RangeDirectory> {
        let id = NamespaceId::new(records::id(namespace)?)
            .map_err(|_| RaftFleetFailure::invalid("first group namespace ID"))?;
        let mut found = self
            .namespaces
            .iter()
            .filter(|directory| directory.namespace_config().namespace == id);
        let directory = found.next().ok_or_else(|| RaftFleetFailure {
            code: RaftFleetFailureCode::NotFound,
            outcome: RaftFleetOutcome::NotSubmitted,
            message: "The namespace is outside configured fleet management.".into(),
        })?;
        if found.next().is_some() {
            return Err(RaftFleetFailure::invalid(
                "duplicate configured first group namespace",
            ));
        }
        Ok(directory)
    }
}
