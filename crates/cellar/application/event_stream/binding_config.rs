//! Deployment selectors are resolved once; delivery bindings always carry UUIDs.

use super::{Binding, Resource};
use crate::application::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceConfig {
    pub tenant: String,
    pub application: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    pub family: String,
}

impl ResourceConfig {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("tenant", &self.tenant),
            ("application", &self.application),
            ("name", &self.name),
            ("family", &self.family),
        ] {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 128,
                "invalid stream resource {field}"
            );
        }
        if let Some(id) = &self.id {
            super::store::validate_id(id)?;
        }
        Ok(())
    }

    pub(crate) fn matches_name(&self, resource: &Resource) -> bool {
        self.tenant == resource.tenant
            && self.application == resource.application
            && self.name == resource.name
    }
}

impl From<Resource> for ResourceConfig {
    fn from(resource: Resource) -> Self {
        Self {
            tenant: resource.tenant,
            application: resource.application,
            id: Some(resource.id),
            name: resource.name,
            family: resource.family,
        }
    }
}

/// A deployment binding. Omitting `resource.id` selects a catalog name; the
/// declaration grants no runtime authority until owner activation completes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingConfig {
    pub environment: String,
    pub resource: ResourceConfig,
    pub principal: String,
    pub grant: String,
    #[serde(default)]
    pub publish: bool,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub administer: bool,
}

impl From<Binding> for BindingConfig {
    fn from(binding: Binding) -> Self {
        Self {
            environment: binding.environment,
            resource: binding.resource.into(),
            principal: binding.principal,
            grant: binding.grant,
            publish: binding.publish,
            read: binding.read,
            subscriptions: binding.subscriptions,
            administer: binding.administer,
        }
    }
}

impl BindingConfig {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        for (field, value) in [
            ("binding", &self.environment),
            ("principal", &self.principal),
            ("grant", &self.grant),
        ] {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 256,
                "invalid stream {field}"
            );
        }
        anyhow::ensure!(
            self.subscriptions.len() <= 128,
            "too many stream subscription grants"
        );
        anyhow::ensure!(
            self.subscriptions
                .iter()
                .all(|name| !name.is_empty() && name.len() <= 128),
            "invalid subscription grant"
        );
        Ok(())
    }

    pub(crate) fn resolve(&self, resource: Resource) -> Result<Binding> {
        self.validate()?;
        resource.validate()?;
        anyhow::ensure!(
            self.resource.matches_name(&resource)
                && self.resource.family == resource.family
                && self
                    .resource
                    .id
                    .as_ref()
                    .is_none_or(|id| *id == resource.id),
            "resolved binding differs from its deployment selector"
        );
        let binding = Binding {
            environment: self.environment.clone(),
            resource,
            principal: self.principal.clone(),
            grant: self.grant.clone(),
            publish: self.publish,
            read: self.read,
            subscriptions: self.subscriptions.clone(),
            administer: self.administer,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub(crate) fn explicit(&self) -> Result<Binding> {
        let id = self.resource.id.clone().ok_or_else(|| {
            super::error::error(
                super::ErrorCode::Unavailable,
                "named binding has not been resolved",
            )
        })?;
        self.resolve(Resource {
            tenant: self.resource.tenant.clone(),
            application: self.resource.application.clone(),
            id,
            name: self.resource.name.clone(),
            family: self.resource.family.clone(),
        })
    }
}
