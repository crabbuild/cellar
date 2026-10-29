// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A controller's immutable operator intent is separate from physical authority.
use super::*;
use serde::Deserialize;

pub(super) const KIND: u8 = 95;
pub(super) const POINTER_KIND: u8 = 96;
pub(super) const PREFIX: &str = "restart-controller/";
pub(super) const ACTIVE: &str = "restart-controller-active";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub format: u8,
    pub request: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub boot: crate::raft::fleet_stop::BootBinding,
    pub grace_seconds: u16,
    pub birth_resolution: FleetRestartBirthResolution,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pointer {
    format: u8,
    operation: [u8; 16],
    request: [u8; 16],
    digest: [u8; 32],
}

impl Record {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    pub fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.request)?;
        self.boot
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("operator restart boot"))?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.boot.catalog != catalog.birth_owner()?.catalog
            || self.boot.fleet != catalog.inner.incarnation
            || !(1..=300).contains(&self.grace_seconds)
        {
            return Err(FleetCatalogError::Corrupt("operator restart intent"));
        }
        Ok(())
    }

    pub fn matches(&self, request: &FleetRestartOperatorRequest) -> bool {
        self.request == request.identity
            && self.boot.node == request.node.bytes()
            && self.boot.process == request.expected_process.bytes()
            && self.grace_seconds == request.grace_seconds
            && self.birth_resolution == request.birth_resolution
    }

    pub fn bytes(&self) -> Result<Vec<u8>> {
        encode(KIND, self, 4096)
    }

    pub fn pointer(&self) -> Result<Pointer> {
        Ok(Pointer {
            format: 1,
            operation: self.request.operation,
            request: self.request.id,
            digest: hash(&self.bytes()?),
        })
    }

    pub fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.request.operation),
                    value: Some(self.bytes()?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: ACTIVE.into(),
                    value: Some(encode(POINTER_KIND, &self.pointer()?, 4096)?),
                },
            ],
        })
    }
}

impl Pointer {
    pub fn operation(&self) -> Result<ProposalId> {
        if self.format != 1 || self.digest == [0; 32] || self.request == [0; 16] {
            return Err(FleetCatalogError::Corrupt("operator restart pointer"));
        }
        ProposalId::new(self.operation)
            .map_err(|_| FleetCatalogError::Corrupt("operator restart operation"))
    }

    pub fn matches(&self, record: &Record) -> Result<bool> {
        Ok(*self == record.pointer()?)
    }
}
