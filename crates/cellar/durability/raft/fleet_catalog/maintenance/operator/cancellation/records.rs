// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A cancel command is intent; only a validated terminal fact releases its slot.
use super::*;
use serde::Deserialize;

pub(super) const COMMAND_KIND: u8 = 98;
pub(super) const TERMINAL_KIND: u8 = 99;
pub(in super::super) const COMMAND_PREFIX: &str = "restart-controller-cancel/";
pub(in super::super) const TERMINAL_PREFIX: &str = "restart-controller-cancelled/";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in super::super) struct Command {
    pub format: u8,
    pub request: FleetRequestIdentity,
    pub intent: [u8; 32],
    pub input_root: [u8; 32],
    pub fence: [u8; 16],
    #[serde(deserialize_with = "required_operation")]
    pub execution: Option<[u8; 16]>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Effect {
    Unadmitted,
    Preparation { request: FleetRequestIdentity },
    Gate { request: FleetRequestIdentity },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Terminal {
    pub format: u8,
    pub request: FleetRequestIdentity,
    pub input_root: [u8; 32],
    pub parent: [u8; 16],
    pub command: [u8; 32],
    pub effect: Effect,
}

impl Command {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{COMMAND_PREFIX}{}", hex::encode(operation))
    }

    pub fn validate(&self, intent: &Record) -> Result<()> {
        validate_request(&self.request)?;
        ProposalId::new(self.fence)
            .map_err(|_| FleetCatalogError::Corrupt("operator cancellation fence"))?;
        if let Some(op) = self.execution {
            ProposalId::new(op)
                .map_err(|_| FleetCatalogError::Corrupt("operator cancellation execution"))?;
        }
        if self.format != 1
            || self.intent != hash(&intent.bytes()?)
            || self.input_root == [0; 32]
            || self.request.operation != intent.request.operation
            || self.request.expected_revision <= intent.request.expected_revision
            || self.execution == Some(intent.request.operation)
        {
            return Err(FleetCatalogError::Corrupt("operator cancellation intent"));
        }
        Ok(())
    }

    pub fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: Self::key(self.request.operation),
                value: Some(encode(COMMAND_KIND, self, 4096)?),
            }],
        })
    }
}

impl Terminal {
    pub fn key(operation: [u8; 16]) -> String {
        format!("{TERMINAL_PREFIX}{}", hex::encode(operation))
    }

    pub fn validate(&self, command: &Command) -> Result<()> {
        validate_request(&self.request)?;
        if self.format != 1
            || self.input_root == [0; 32]
            || self.parent != command.request.id
            || self.command != hash(&encode(COMMAND_KIND, command, 4096)?)
            || self.request.operation != command.request.operation
            || self.request.expected_revision <= command.request.expected_revision
        {
            return Err(FleetCatalogError::Corrupt("operator cancellation terminal"));
        }
        if let Effect::Preparation { request } | Effect::Gate { request } = &self.effect {
            validate_request(request)?;
            if Some(request.operation) != command.execution
                || request.expected_revision <= command.request.expected_revision
                || request.expected_revision >= self.request.expected_revision
            {
                return Err(FleetCatalogError::Corrupt("operator cancellation effect"));
            }
        }
        Ok(())
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
                    value: Some(encode(TERMINAL_KIND, self, 4096)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: super::super::records::ACTIVE.into(),
                    value: None,
                },
            ],
        })
    }
}

fn required_operation<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<[u8; 16]>, D::Error> {
    Option::<[u8; 16]>::deserialize(decoder)
}
