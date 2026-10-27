// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-declared provider slots, separate from invocation-owned handles.
use crate::{Adapter, Capabilities, Limits, Provider, Session, Validation};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Capability discovery contains public slot names, never provider credentials.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingCapabilities {
    pub name: String,
    pub capabilities: Capabilities,
}

/// Additional host-selected providers. The default `container` slot is supplied
/// separately by the existing adapter. At most 63 additional slots are allowed.
#[derive(Clone, Default)]
pub struct Bindings(BTreeMap<String, Adapter>);

impl Bindings {
    /// A named provider must declare the same durable journal/slot identity.
    /// Refusals leave the registry unchanged; Native owns only the default slot.
    pub fn insert(&mut self, name: impl Into<String>, provider: Arc<dyn Provider>) -> Result<()> {
        let name = name.into();
        validate_name(&name)?;
        ensure!(
            name != "container",
            "use the default provider for container"
        );
        ensure!(self.0.len() < 63, "too many container bindings");
        ensure!(!self.0.contains_key(&name), "duplicate container binding");
        ensure!(
            provider.binding_name() == Some(name.as_str()),
            "provider binding identity must match its declared slot"
        );
        self.0.insert(name, Adapter::new(provider));
        Ok(())
    }

    /// Validate after all builder calls, including a later default override.
    pub fn validate(&self, default: &dyn Provider) -> Result<()> {
        ensure!(
            default
                .binding_name()
                .is_none_or(|name| !self.0.contains_key(name)),
            "default and named providers share a durable binding"
        );
        Ok(())
    }

    /// Configuration-only discovery; does not open sessions or contact providers.
    pub fn capabilities(&self, limits: &Limits) -> Vec<BindingCapabilities> {
        self.0
            .iter()
            .map(|(name, adapter)| BindingCapabilities {
                name: name.clone(),
                capabilities: adapter.capabilities(limits),
            })
            .collect()
    }

    /// Open once per original root-cell invocation. Provider `open` must not
    /// create external compute; effects remain inside checked session execution.
    pub fn sessions(&self) -> BindingSessions {
        BindingSessions(
            self.0
                .iter()
                .map(|(name, adapter)| (name.clone(), adapter.session()))
                .collect(),
        )
    }
}

/// Each slot owns separate resource handles and exact-event authority.
#[derive(Default)]
pub struct BindingSessions(BTreeMap<String, Session>);

impl BindingSessions {
    pub fn get(&self, name: &str) -> Result<&Session> {
        validate_name(name).map_err(|error| error.context(Validation))?;
        self.0.get(name).ok_or_else(|| anyhow::anyhow!(Validation))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

pub(crate) fn validate_name(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')),
        "invalid container binding name"
    );
    Ok(())
}

#[cfg(test)]
#[path = "bindings_tests.rs"]
mod tests;
