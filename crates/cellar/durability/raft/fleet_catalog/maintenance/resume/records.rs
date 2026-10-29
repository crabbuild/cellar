// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rooted intent and terminal facts contain no clock or restored health lease.
use super::*;
use serde::Deserialize;

pub(super) const INTENT_KIND: u8 = 90;
pub(super) const COMPLETE_KIND: u8 = 93;
pub(super) const INTENT_PREFIX: &str = "restart-resume/";
pub(super) const COMPLETE_PREFIX: &str = "restart-resumed/";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IntentRequest {
    pub identity: FleetRequestIdentity,
    pub returning: FleetRequestIdentity,
    pub attestation: FleetCapacityAttestation,
    pub resume: [u8; 16],
}

impl IntentRequest {
    pub fn same_scope(&self, request: &FleetRestartResumeRequest) -> bool {
        self.identity.operation == request.identity.operation
            && self.returning == request.returning
            && self.attestation == request.attestation
            && self.resume == request.resume.bytes()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub format: u8,
    pub request: IntentRequest,
    pub input_root: [u8; 32],
    pub inputs: [u8; 32],
    pub candidate: [u8; 32],
    pub physical: [u8; 32],
    pub returned: [u8; 32],
    pub parent: [u8; 16],
}

impl Intent {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{INTENT_PREFIX}{}", hex::encode(operation))
    }

    pub fn check_scope(request: &FleetRestartResumeRequest, cut: &ReturnedCut) -> Result<()> {
        request.validate()?;
        if request.returning != cut.returning
            || request.attestation.boot != cut.quiescence.boot
            || request.identity.operation != cut.boot_request.operation
            || request.resume.bytes() == cut.quiescence.request
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(())
    }

    pub fn selector(
        request: &FleetRestartResumeRequest,
        cut: &ReturnedCut,
    ) -> Result<StopSelector> {
        Self::check_scope(request, cut)?;
        let mut selector = cut.quiescence.clone();
        selector.request = request.resume.bytes();
        Ok(selector)
    }

    pub fn new(
        request: &FleetRestartResumeRequest,
        cut: &ReturnedCut,
        book: &Book,
        input_root: [u8; 32],
        candidate: [u8; 32],
    ) -> Result<Self> {
        let record = Self {
            format: 1,
            request: IntentRequest {
                identity: request.identity.clone(),
                returning: request.returning.clone(),
                attestation: request.attestation.clone(),
                resume: request.resume.bytes(),
            },
            input_root,
            inputs: book.basis.digest()?,
            candidate,
            physical: book.physical,
            returned: cut.digest,
            parent: cut.boot_request.id,
        };
        record.validate(cut)?;
        Ok(record)
    }

    pub fn validate(&self, cut: &ReturnedCut) -> Result<()> {
        let request = FleetRestartResumeRequest {
            identity: self.request.identity.clone(),
            returning: self.request.returning.clone(),
            attestation: self.request.attestation.clone(),
            resume: ProposalId::new(self.request.resume)
                .map_err(|_| FleetCatalogError::Corrupt("resume child"))?,
        };
        Self::check_scope(&request, cut)?;
        if self.format != 1
            || [
                self.input_root,
                self.inputs,
                self.candidate,
                self.physical,
                self.returned,
            ]
            .contains(&[0; 32])
            || self.parent != cut.boot_request.id
            || self.returned != cut.digest
            || self.effect_selector()? != Self::selector(&request, cut)?
            || self.request.identity.expected_revision <= cut.boot_request.expected_revision
        {
            return Err(FleetCatalogError::Corrupt("resume intent"));
        }
        Ok(())
    }

    // The canonical request already contains this boot and every selector ID.
    // Derive the physical token instead of duplicating a boot in the Root row.
    pub fn effect_selector(&self) -> Result<StopSelector> {
        let selector = StopSelector {
            purpose: crate::raft::fleet_stop::LifecyclePurpose::Maintenance,
            boot: self.request.attestation.boot.clone(),
            epoch: self
                .request
                .attestation
                .boot
                .epoch
                .checked_add(1)
                .ok_or(FleetCatalogError::Limit)?,
            operation: self.request.identity.operation,
            finalization: self.request.returning.id,
            request: self.request.resume,
        };
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("resume selector"))?;
        Ok(selector)
    }

    pub fn digest(&self) -> Result<[u8; 32]> {
        Ok(hash(&encode(INTENT_KIND, self, 4096)?))
    }

    pub fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.identity.id,
            operation: self.request.identity.operation,
            principal: self.request.identity.principal.clone(),
            expected_revision: self.request.identity.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: Self::key(self.request.identity.operation),
                value: Some(encode(INTENT_KIND, self, 4096)?),
            }],
        })
    }
}

// A terminal record binds the separate canonical Node change by digest. It
// contains no duplicate Node or physical selector and stays inside 4 KiB.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completed {
    pub format: u8,
    pub request: FleetRestartResumeCompleteRequest,
    pub input_root: [u8; 32],
    pub intent: [u8; 32],
    pub returned: [u8; 32],
    pub candidate: [u8; 32],
    pub slot: [u8; 32],
    pub node: [u8; 32],
    pub attestation: FleetCapacityAttestation,
    pub retained: u64,
    pub replaced: u64,
}

#[derive(Clone)]
pub(super) struct Completion {
    pub fact: Completed,
    pub node: super::super::super::nodes::NodeRecord,
}

impl Completion {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{COMPLETE_PREFIX}{}", hex::encode(operation))
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        encode(COMPLETE_KIND, &self.fact, 4096)
    }

    pub fn capacity_request(&self) -> FleetCapacityRequest {
        FleetCapacityRequest {
            identity: self.fact.request.identity.clone(),
            attestation: self.fact.attestation.clone(),
            action: FleetCapacityAction::Activate,
        }
    }

    fn validate_structure(&self, catalog: &FleetCatalog) -> Result<()> {
        let fact = &self.fact;
        validate_request(&fact.request.identity)?;
        validate_request(&fact.request.resume)?;
        fact.attestation.validate()?;
        self.node.validate(&catalog.inner)?;
        if fact.format != 1
            || [
                fact.input_root,
                fact.intent,
                fact.returned,
                fact.candidate,
                fact.slot,
                fact.node,
            ]
            .contains(&[0; 32])
            || fact.node
                != hash(&encode(
                    super::super::super::nodes::NODE_KIND,
                    &self.node,
                    4096,
                )?)
            || fact.request.identity.operation != fact.request.resume.operation
            || fact.request.identity.expected_revision <= fact.request.resume.expected_revision
            || self.node.state != FleetNodeState::Active
            || self.node.operation().is_some()
            || self.node.boot_binding(catalog.birth_owner()?.catalog)? != fact.attestation.boot
            || self.node.boot.as_ref().and_then(|b| b.epoch.checked_add(2)) != Some(self.node.epoch)
            || fact.retained > HISTORY_LIMIT
            || fact.replaced > HISTORY_LIMIT
        {
            return Err(FleetCatalogError::Corrupt("resume completion"));
        }
        Ok(())
    }

    pub fn validate(
        &self,
        catalog: &FleetCatalog,
        intent: &Intent,
        cut: &ReturnedCut,
    ) -> Result<()> {
        self.validate_structure(catalog)?;
        intent.validate(cut)?;
        let mut node = cut.node.clone();
        node.resume_restart(cut.boot_request.operation, cut.quiescence.epoch)?;
        if self.fact.request.resume != intent.request.identity
            || self.fact.intent != intent.digest()?
            || self.fact.returned != cut.digest
            || self.fact.slot != cut.slot
            || self.node != node
            || self.fact.attestation != intent.request.attestation
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(())
    }

    pub fn mutation(&self, catalog: &FleetCatalog) -> Result<FleetMutation> {
        self.validate_structure(catalog)?;
        Ok(FleetMutation {
            id: self.fact.request.identity.id,
            operation: self.fact.request.identity.operation,
            principal: self.fact.request.identity.principal.clone(),
            expected_revision: self.fact.request.identity.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(
                        super::super::super::nodes::NODE_KIND,
                        &self.node,
                        4096,
                    )?),
                },
                catalog.restart_capacity_change(
                    &self.node,
                    &self.capacity_request(),
                    self.fact.input_root,
                )?,
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.fact.request.identity.operation),
                    value: Some(self.bytes()?),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: super::super::super::disruption::SLOT_KEY.into(),
                    value: None,
                },
            ],
        })
    }

    pub fn from_mutation(catalog: &FleetCatalog, mutation: &FleetMutation) -> Result<Self> {
        mutation.validate()?;
        if mutation.changes.len() != 4
            || mutation.changes[0].table != FleetTable::Nodes
            || mutation.changes[2].table != FleetTable::Operations
            || mutation.changes[2].key != Self::key(mutation.operation)
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let fact: Completed = decode(
            COMPLETE_KIND,
            mutation.changes[2]
                .value
                .as_deref()
                .ok_or(FleetCatalogError::PayloadConflict)?,
            4096,
        )?;
        let node = decode(
            super::super::super::nodes::NODE_KIND,
            mutation.changes[0]
                .value
                .as_deref()
                .ok_or(FleetCatalogError::PayloadConflict)?,
            4096,
        )?;
        let record = Self { fact, node };
        if record.mutation(catalog)? != *mutation {
            return Err(FleetCatalogError::PayloadConflict);
        }
        Ok(record)
    }
}
