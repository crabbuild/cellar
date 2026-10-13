//! Host-minted event RPC provenance. Application arguments cannot provide
//! these fields; private binding adapters mint them before owner routing.

use super::{Binding, Resource};
use crate::application::Context;
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Action {
    Resolve,
    Configure,
    Append,
    Read,
    Consume,
    Administer,
}

/// Carried by trusted host request adapters and authenticated peer tunnels.
/// Fields and constructors remain private to the framework.
#[doc(hidden)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub(crate) principal: String,
    pub(crate) grant: String,
    pub(crate) resource: Resource,
    pub(crate) source: Option<String>,
    pub(crate) action: Action,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) operator: Option<String>,
}

impl Authority {
    pub(crate) fn deployment(script: &str, definition: &super::Definition) -> Result<Self> {
        definition.validate()?;
        let grant = definition
            .grants
            .iter()
            .find(|grant| grant.principal == script && grant.administer)
            .context("deployment has no resource administration grant")?;
        let authority = Self {
            principal: script.into(),
            grant: grant.id.clone(),
            resource: definition.resource.clone(),
            source: None,
            action: Action::Configure,
            operator: None,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub(crate) fn resolution(binding: &Binding) -> Result<Self> {
        binding.validate()?;
        let authority = Self {
            principal: binding.principal.clone(),
            grant: binding.grant.clone(),
            resource: binding.resource.clone(),
            source: None,
            action: Action::Resolve,
            operator: None,
        };
        authority.validate()?;
        Ok(authority)
    }

    /// Framework dispatch uses the immutable principal in a persisted source
    /// lane. A later application generation may rename its script or remove
    /// the binding; neither action revokes the receiver's independent grant.
    pub(crate) fn delivery(context: &Context, persisted: &Binding) -> Result<Self> {
        context.check_turn()?;
        persisted.validate()?;
        anyhow::ensure!(persisted.publish, "stored lane does not grant publication");
        let source = context
            .identity()
            .context("event delivery requires its source cell")?
            .scope
            .clone();
        let authority = Self {
            principal: persisted.principal.clone(),
            grant: persisted.grant.clone(),
            resource: persisted.resource.clone(),
            source: Some(source),
            action: Action::Append,
            operator: None,
        };
        authority.validate()?;
        Ok(authority)
    }

    pub(crate) fn mint(context: &Context, binding: &Binding, action: Action) -> Result<Self> {
        context.check_turn()?;
        binding.validate()?;
        anyhow::ensure!(
            binding.principal == context.worker_config.script_name,
            "stream principal does not match its host application identity"
        );
        let permitted = match action {
            Action::Resolve => {
                binding.publish
                    || binding.read
                    || binding.administer
                    || !binding.subscriptions.is_empty()
            }
            Action::Configure | Action::Administer => binding.administer,
            Action::Append => binding.publish,
            Action::Read => binding.read,
            Action::Consume => !binding.subscriptions.is_empty(),
        };
        anyhow::ensure!(permitted, "stream binding does not grant this operation");
        let source = context.identity().map(|identity| identity.scope.clone());
        anyhow::ensure!(
            action != Action::Append || source.is_some(),
            "stream append requires a source cell"
        );
        Ok(Self {
            principal: context.worker_config.script_name.clone(),
            grant: binding.grant.clone(),
            resource: binding.resource.clone(),
            source,
            action,
            operator: None,
        })
    }

    pub(crate) fn operator(binding: &Binding, action: Action, operator: String) -> Result<Self> {
        binding.validate()?;
        anyhow::ensure!(
            matches!(action, Action::Read) && binding.read
                || matches!(action, Action::Administer) && binding.administer,
            "binding does not grant this operator action"
        );
        let authority = Self {
            principal: binding.principal.clone(),
            grant: binding.grant.clone(),
            resource: binding.resource.clone(),
            source: None,
            action,
            operator: Some(operator),
        };
        authority.validate()?;
        Ok(authority)
    }

    pub(crate) fn validate_operator_target(&self, scope: &str, method: &str) -> Result<()> {
        if self.operator.is_none() {
            return Ok(());
        }
        anyhow::ensure!(
            scope == super::cell_scope(&self.resource)?,
            "operator target is not its stream"
        );
        let permitted = match self.action {
            Action::Read => method == "__streamRead",
            Action::Administer => matches!(
                method,
                "__streamDescribe"
                    | "__streamSubscriptionStatus"
                    | "__streamSubscriptionChange"
                    | "__streamAudit"
                    | "__streamOperationStatus"
            ),
            _ => false,
        };
        anyhow::ensure!(
            permitted,
            "operator authority does not grant this RPC method"
        );
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        anyhow::ensure!(
            self.operator.as_ref().is_none_or(|operator| {
                !operator.is_empty()
                    && operator.len() <= 256
                    && self.source.is_none()
                    && matches!(self.action, Action::Read | Action::Administer)
            }),
            "invalid event operator authority"
        );
        anyhow::ensure!(
            !self.principal.is_empty()
                && self.principal.len() <= 256
                && !self.grant.is_empty()
                && self.grant.len() <= 256,
            "invalid event RPC authority"
        );
        anyhow::ensure!(
            self.source
                .as_ref()
                .is_none_or(|source| cellar_logic::cell::valid_cell_scope(source)),
            "invalid event RPC source scope"
        );
        anyhow::ensure!(
            self.action != Action::Append || self.source.is_some(),
            "event append has no source identity"
        );
        Ok(())
    }
}
