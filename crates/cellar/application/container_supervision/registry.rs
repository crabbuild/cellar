// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{BackendIdentity, MaintenanceBackend, MaintenancePolicy};
use crate::application::{
    Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use anyhow::{ensure, Result};
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

/// Add host-owned provider maintenance to any native or language application.
/// Backends execute within the native scheduler, independently of guest alarms.
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
        self.application.resolve_raft_class(class)
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
