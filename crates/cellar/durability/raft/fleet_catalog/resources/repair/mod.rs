// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Required repair after rooted exclusion and trusted exact-boot isolation.
//! Deployment supplies bounds; the adapter owns selection and fresh admission.
mod membership;
mod planning;
use super::*;
use crate::raft::catalog::NamespaceCatalog;
use cellar_logic::consensus::ProposalId;
pub(super) use planning::Planning;
use std::pin::Pin;

#[derive(Clone, Debug)]
pub struct FleetIsolatedRepairContext {
    pub operation: ProposalId,
    pub failed_boot: BootBinding,
    pub group: GroupScope,
}

/// Conservative deployment inputs for one history and at most 64 candidate or
/// surviving nodes. Missing nodes are ineligible; omitted surviving voters block
/// repair. Callbacks cannot choose a source, target, member ID or repair plan.
pub struct FleetIsolatedRepairInputs {
    pub history: FleetTopologyHistoryResources,
    pub nodes: Vec<FleetTopologyNodeResources>,
    pub namespaces: Vec<NamespaceCatalog>,
}

/// The owned service cancels/joins the callback future with its original round
/// deadline. Providers must own physical work and its resource reservations
/// through completion, including when this waiter is dropped.
pub trait FleetIsolatedRepairEvidence: Send + Sync {
    fn refresh<'a>(
        &'a self,
        context: &'a FleetIsolatedRepairContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<FleetIsolatedRepairInputs>> + Send + 'a>>;
}

pub(super) struct Group {
    pub history: super::super::histories::FleetHistory,
    pub voters: std::collections::BTreeMap<NodeId, cellar_logic::consensus::MemberId>,
    pub latest: Option<crate::raft::member_state::FleetMembershipPermit>,
}

pub(super) struct Selected {
    pub request: FleetMovementRequest,
    pub reserve: FleetResourceBounds,
}

impl FleetCatalog {
    pub(super) async fn reserve_isolated_repair(
        &self,
        cut: &FleetSnapshot,
        selected: &Selected,
        observer: &crate::application::consensus::RaftCells,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<FleetMovementAdmission> {
        let mut canonical = super::records::Request::new(self, &selected.request.resources)?;
        canonical.movement = Some(super::movement::Movement::new(&selected.request)?);
        canonical.validate(self)?;
        let resources = self
            .reserve_resource_request(
                cut,
                canonical,
                observer,
                namespaces,
                super::admission::AdmissionOptions {
                    topology: false,
                    reserve: selected.reserve,
                },
                timeout,
            )
            .await?;
        let permit = resources
            .record
            .permit
            .clone()
            .ok_or(FleetCatalogError::Corrupt("missing isolated repair permit"))?;
        let authorization = FleetPermitAuthorization::admitted(
            permit,
            resources.applied,
            Some(resources.record.request.attestation.boot.clone()),
        );
        Ok(FleetMovementAdmission {
            resources,
            authorization,
        })
    }
}
