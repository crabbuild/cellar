// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{BackendIdentity, MaintenanceBackend, MaintenancePolicy};
use crate::application::{
    Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub(crate) struct Registration {
    pub backend: Arc<dyn MaintenanceBackend>,
    pub policy: MaintenancePolicy,
}

/// Explicit host configuration. Original provider configurations may remain
/// registered for outstanding generations after the application changes.
#[derive(Clone, Default)]
pub struct Supervision {
    registrations: BTreeMap<BackendIdentity, Registration>,
}

impl Supervision {
    pub fn register(
        &mut self,
        backend: Arc<dyn MaintenanceBackend>,
        policy: MaintenancePolicy,
    ) -> Result<()> {
        let key = backend.identity();
        crate::storage::container_bindings::validate_name(&key.binding)?;
        crate::storage::container_bindings::validate_name(&key.provider)?;
        ensure!(
            key.configuration_digest.len() == 64
                && key
                    .configuration_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "invalid maintenance configuration digest"
        );
        let policy = policy.validate()?;
        ensure!(
            !self.registrations.contains_key(&key),
            "duplicate maintenance backend identity"
        );
        ensure!(
            self.registrations.len() < 256,
            "maintenance backend registration limit exceeded"
        );
        self.registrations
            .insert(key, Registration { backend, policy });
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: &Self) -> Result<()> {
        for (key, registration) in &other.registrations {
            if let Some(prior) = self.registrations.get(key) {
                ensure!(
                    Arc::ptr_eq(&prior.backend, &registration.backend)
                        && prior.policy == registration.policy,
                    "ambiguous cohosted maintenance backend"
                );
            } else {
                self.register(registration.backend.clone(), registration.policy)?;
            }
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    pub(crate) fn select(
        &self,
        declarations: &crate::application::container_bindings::ProviderDeclarations,
    ) -> Result<Self> {
        let mut selected = Self::default();
        for (name, declaration) in declarations {
            let key = BackendIdentity {
                binding: name.clone(),
                provider: declaration.provider.clone(),
                configuration_digest: declaration.configuration_digest.clone(),
            };
            if let Some(registration) = self.registrations.get(&key) {
                selected.registrations.insert(key, registration.clone());
            } else {
                ensure!(
                    !self.registrations.keys().any(|key| key.binding == *name),
                    "provider supervision does not match its declared configuration"
                );
            }
        }
        Ok(selected)
    }

    pub(crate) fn same_registrations(&self, other: &Self) -> bool {
        self.registrations.len() == other.registrations.len()
            && self.registrations.iter().all(|(key, registration)| {
                other.registrations.get(key).is_some_and(|other| {
                    registration.policy == other.policy
                        && Arc::ptr_eq(&registration.backend, &other.backend)
                })
            })
    }

    pub(crate) fn max_attempt_timeout_ms(&self) -> Option<u64> {
        self.registrations
            .values()
            .map(|r| r.policy.attempt_timeout_ms)
            .max()
    }

    pub(crate) fn contract_digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"cellar-provider-supervision-1");
        hash.update((self.registrations.len() as u16).to_be_bytes());
        for (key, registration) in &self.registrations {
            for text in [&key.binding, &key.provider, &key.configuration_digest] {
                hash.update((text.len() as u16).to_be_bytes());
                hash.update(text.as_bytes());
            }
            let policy = registration.policy;
            for value in [
                policy.inspect_interval_ms,
                policy.maximum_lifetime_ms,
                policy.attempt_timeout_ms,
            ] {
                hash.update(value.to_be_bytes());
            }
            hash.update([u8::from(policy.idle_timeout_ms.is_some())]);
            hash.update(policy.idle_timeout_ms.unwrap_or(0).to_be_bytes());
        }
        hash.finalize().into()
    }

    pub(crate) fn find(
        &self,
        binding: &str,
        record: &super::super::container_bindings::BindingRecord,
    ) -> Option<&Registration> {
        self.registrations.get(&BackendIdentity {
            binding: binding.into(),
            provider: record.provider.clone(),
            configuration_digest: record.configuration_digest.clone(),
        })
    }
}

/// Add host-owned provider maintenance independently of application alarms.
/// Shared-group classes must declare their provider journals. Matching backend
/// identities and retention policies become part of the namespace contract;
/// their work runs through the shared durable scheduler rather than Fleet gates.
pub fn supervise(
    application: Arc<dyn Application>,
    supervision: Supervision,
) -> Arc<dyn Application> {
    Arc::new(Supervised {
        application,
        supervision: Arc::new(supervision),
    })
}

struct Supervised {
    application: Arc<dyn Application>,
    supervision: Arc<Supervision>,
}

impl Application for Supervised {
    fn uses_raft_primitives(&self) -> bool {
        self.application.uses_raft_primitives()
    }

    fn raft_event_definitions(&self) -> &[crate::application::event_stream::Definition] {
        self.application.raft_event_definitions()
    }

    fn filesystem_policy(&self, class: &str) -> Option<crate::application::filesystem::Policy> {
        self.application.filesystem_policy(class)
    }

    fn sandbox_executor(
        &self,
        class: &str,
    ) -> Option<Arc<dyn crate::application::containers::SandboxExecutor>> {
        self.application.sandbox_executor(class)
    }

    fn container_supervision(&self) -> Option<Arc<Supervision>> {
        Some(self.supervision.clone())
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.application.clone().invoke(context, invocation)
    }

    fn validate_configuration(
        &self,
        options: &crate::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        self.application.validate_configuration(options)
    }

    fn validate_deployment(&self, deployment: &crate::fleet::LoadedDeployment) -> Result<()> {
        self.application.validate_deployment(deployment)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn rpc_catalog(&self) -> Option<&crate::application::rpc::Catalog> {
        self.application.rpc_catalog()
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.application.create_cell(context, identity)
    }

    fn resolve_raft_class(
        &self,
        class: &str,
    ) -> std::result::Result<
        Arc<crate::application::consensus::RaftClass>,
        crate::application::consensus::RegistrationError,
    > {
        self.application
            .resolve_raft_class(class)?
            .with_provider_supervision(&self.supervision)
    }

    fn supports_workflow(&self, class: &str) -> bool {
        self.application.supports_workflow(class)
    }

    fn run_workflow(
        self: Arc<Self>,
        class: String,
        context: Context,
        event: crate::application::workflow::Event,
        steps: crate::application::workflow::Step,
    ) -> HandlerFuture<cellar_native::clone::StructuredValue> {
        self.application
            .clone()
            .run_workflow(class, context, event, steps)
    }
}
