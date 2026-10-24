// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{Binding, Resource};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_DEFINITION_BYTES: usize = 256 << 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    Bounded,
    Required,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub id: String,
    pub principal: String,
    #[serde(default)]
    pub publish: bool,
    #[serde(default)]
    pub read: bool,
    #[serde(default)]
    pub subscriptions: Vec<String>,
    #[serde(default)]
    pub administer: bool,
}

impl Grant {
    pub fn from_binding(binding: &Binding) -> Self {
        Self {
            id: binding.grant.clone(),
            principal: binding.principal.clone(),
            publish: binding.publish,
            read: binding.read,
            subscriptions: binding.subscriptions.clone(),
            administer: binding.administer,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "offset")]
pub enum Start {
    Earliest,
    Latest,
    Offset(i64),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionDefinition {
    pub name: String,
    pub start: Start,
    pub required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub resource: Resource,
    pub revision: i64,
    pub retention: Retention,
    pub max_bytes: i64,
    pub max_records: i64,
    pub max_age_ms: i64,
    pub grants: Vec<Grant>,
    pub subscriptions: Vec<SubscriptionDefinition>,
}

impl Definition {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        anyhow::ensure!(
            self.revision > 0 && self.max_bytes > 0 && self.max_records > 0 && self.max_age_ms > 0,
            "stream revision and retention limits must be positive"
        );
        anyhow::ensure!(
            !self.grants.is_empty() && self.grants.len() <= 128 && self.subscriptions.len() <= 128,
            "invalid stream grant or subscription count"
        );
        let mut grants = BTreeSet::new();
        for grant in &self.grants {
            anyhow::ensure!(
                !grant.id.is_empty()
                    && grant.id.len() <= 256
                    && !grant.principal.is_empty()
                    && grant.principal.len() <= 256
                    && grants.insert((&grant.principal, &grant.id)),
                "invalid or duplicate stream grant"
            );
            anyhow::ensure!(
                grant.subscriptions.len() <= 128
                    && grant
                        .subscriptions
                        .iter()
                        .all(|name| !name.is_empty() && name.len() <= 128),
                "invalid stream subscription grant"
            );
        }
        let mut names = BTreeSet::new();
        for subscription in &self.subscriptions {
            anyhow::ensure!(
                !subscription.name.is_empty()
                    && subscription.name.len() <= 128
                    && names.insert(&subscription.name),
                "invalid or duplicate stream subscription"
            );
            if let Start::Offset(offset) = subscription.start {
                anyhow::ensure!(offset > 0, "invalid subscription start offset");
            }
        }
        anyhow::ensure!(
            self.retention != Retention::Required
                || self
                    .subscriptions
                    .iter()
                    .any(|subscription| subscription.required),
            "required retention needs at least one required subscription"
        );
        anyhow::ensure!(
            self.retention != Retention::Bounded
                || !self
                    .subscriptions
                    .iter()
                    .any(|subscription| subscription.required),
            "bounded history cannot promise required-consumer retention"
        );
        anyhow::ensure!(
            self.grants.iter().any(|grant| grant.administer),
            "stream configuration needs an administrative grant"
        );
        anyhow::ensure!(
            serde_json::to_vec(self)?.len() <= MAX_DEFINITION_BYTES,
            "stream definition is too large"
        );
        Ok(())
    }
}
