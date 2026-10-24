// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Validated deployment declarations. These describe desired resources; they
//! are not catalog activation receipts or application-supplied RPC authority.

use super::Manifest;
use crate::application::event_stream::{BindingConfig, Definition};
use anyhow::{Context, Result};
use std::collections::BTreeSet;

pub const MAX_DEPLOYMENT_STREAMS: usize = 128;
pub const MAX_DEPLOYMENT_STREAM_BINDINGS: usize = 128;

#[derive(Clone, Debug, Default)]
pub struct EventResources {
    pub definitions: Vec<Definition>,
    pub bindings: Vec<BindingConfig>,
}

pub fn validate_event_resources(
    script: &str,
    definitions: &[Definition],
    bindings: &[BindingConfig],
    occupied: &BTreeSet<String>,
    limits: &cellar_logic::event_stream::Limits,
) -> Result<()> {
    limits.validate()?;
    anyhow::ensure!(
        definitions.len() <= MAX_DEPLOYMENT_STREAMS
            && bindings.len() <= MAX_DEPLOYMENT_STREAM_BINDINGS,
        "deployment event resource count exceeds policy"
    );
    let mut identities = BTreeSet::new();
    let mut names = BTreeSet::new();
    for definition in definitions {
        definition.validate()?;
        let resource = &definition.resource;
        anyhow::ensure!(
            identities.insert(&resource.id)
                && names.insert((&resource.tenant, &resource.application, &resource.name)),
            "duplicate stream resource identity or live name"
        );
        anyhow::ensure!(
            definition.subscriptions.len() <= limits.max_subscriptions,
            "stream subscriptions exceed the serving host policy"
        );
        anyhow::ensure!(
            definition
                .grants
                .iter()
                .any(|grant| grant.principal == script && grant.administer),
            "declared stream has no deployment-owner administration grant"
        );
    }
    let mut environments = occupied.clone();
    for binding in bindings {
        binding.validate()?;
        crate::application::event_stream::encoding::json(binding, 64 << 10)?;
        anyhow::ensure!(
            super::valid_binding_name(&binding.environment)
                && environments.insert(binding.environment.clone()),
            "invalid or duplicate event stream binding environment"
        );
        anyhow::ensure!(
            binding.principal == script,
            "event binding cannot impersonate another application"
        );
        if let Some(definition) = definitions.iter().find(|definition| {
            binding
                .resource
                .id
                .as_ref()
                .is_some_and(|id| *id == definition.resource.id)
                || binding.resource.matches_name(&definition.resource)
        }) {
            binding.resolve(definition.resource.clone())?;
            let grant = definition
                .grants
                .iter()
                .find(|grant| grant.principal == script && grant.id == binding.grant)
                .context("event binding does not name a declared grant")?;
            anyhow::ensure!(
                (!binding.publish || grant.publish)
                    && (!binding.read || grant.read)
                    && (!binding.administer || grant.administer)
                    && binding
                        .subscriptions
                        .iter()
                        .all(|name| grant.subscriptions.contains(name)),
                "event binding exceeds the declared resource grant"
            );
            anyhow::ensure!(
                binding.subscriptions.iter().all(|name| definition
                    .subscriptions
                    .iter()
                    .any(|subscription| subscription.name == *name)),
                "event binding consumes an undeclared subscription"
            );
        }
    }
    Ok(())
}

/// Parse bounded metadata before any code is fetched or workers constructed.
/// Unmarked metadata is rejected rather than silently producing empty bindings.
pub fn event_resources_from_manifest(
    manifest: &Manifest,
    limits: &cellar_logic::event_stream::Limits,
) -> Result<EventResources> {
    fn entries<'a>(
        manifest: &'a Manifest,
        key: &str,
        maximum: usize,
    ) -> Result<&'a [serde_json::Value]> {
        match manifest.raw_metadata.get(key) {
            None => Ok(&[]),
            Some(serde_json::Value::Array(entries)) if entries.len() <= maximum => Ok(entries),
            Some(_) => anyhow::bail!("invalid or oversized deployment {key}"),
        }
    }
    let mut resources = EventResources::default();
    for value in entries(manifest, "streams", MAX_DEPLOYMENT_STREAMS)? {
        let encoded = crate::application::event_stream::encoding::json(
            value,
            crate::application::event_stream::MAX_DEFINITION_BYTES,
        )?;
        resources
            .definitions
            .push(serde_json::from_slice(&encoded)?);
    }
    for value in entries(manifest, "stream_bindings", MAX_DEPLOYMENT_STREAM_BINDINGS)? {
        let encoded = crate::application::event_stream::encoding::json(value, 64 << 10)?;
        resources.bindings.push(serde_json::from_slice(&encoded)?);
    }
    let configured = !resources.definitions.is_empty() || !resources.bindings.is_empty();
    let reserved = [
        crate::deploy::EVENT_STREAM_CLASS,
        crate::deploy::EVENT_CATALOG_CLASS,
    ];
    let gated = manifest
        .required_features
        .iter()
        .any(|feature| feature == super::FEATURE_EVENT_DELIVERY_V1);
    let classes = manifest
        .do_classes
        .iter()
        .any(|class| reserved.contains(&class.as_str()));
    if !configured && !gated && !classes {
        return Ok(resources);
    }
    anyhow::ensure!(
        gated && configured,
        "event resource configuration requires the event-delivery-v1 feature gate"
    );
    anyhow::ensure!(
        manifest.native.is_some(),
        "event delivery requires a native application deployment"
    );
    for class in reserved {
        anyhow::ensure!(
            manifest.do_classes.iter().any(|declared| declared == class)
                && manifest
                    .sqlite_classes
                    .iter()
                    .any(|declared| declared == class),
            "event deployment must install both reserved SQLite event classes"
        );
    }
    let raw_bindings = match manifest.raw_metadata.get("bindings") {
        None => &[][..],
        Some(serde_json::Value::Array(bindings)) => bindings.as_slice(),
        Some(_) => anyhow::bail!("invalid deployment binding metadata"),
    };
    let occupied = raw_bindings
        .iter()
        .filter_map(|binding| {
            binding
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    validate_event_resources(
        &manifest.script_name,
        &resources.definitions,
        &resources.bindings,
        &occupied,
        limits,
    )?;
    Ok(resources)
}
