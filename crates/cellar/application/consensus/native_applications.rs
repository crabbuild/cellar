// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable namespace resolution across cohosted native deployments.

use super::{NamespaceConfig, RaftClass, RegistrationError};
use crate::application::{
    Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use std::sync::Arc;

/// A primary application with a bounded catalogue of cohosted definitions.
/// Namespace schema digests select the exact deployment; class-name ordering
/// never supplies authority. Definitions must already have validated deployment
/// configuration. Removing required code refuses namespace startup.
pub struct RaftApplications {
    primary: Arc<dyn Application>,
    applications: Vec<Arc<dyn Application>>,
}

impl RaftApplications {
    pub fn new(
        primary: Arc<dyn Application>,
        cohosted: impl IntoIterator<Item = Arc<dyn Application>>,
    ) -> Result<Self, RegistrationError> {
        let mut applications = vec![primary.clone()];
        for application in cohosted {
            if applications.iter().any(|a| Arc::ptr_eq(a, &application)) {
                continue;
            }
            if applications.len() == 256 {
                return Err(RegistrationError::Limit);
            }
            applications.push(application);
        }
        Ok(Self {
            primary,
            applications,
        })
    }
}

impl Application for RaftApplications {
    fn supports_cell(&self, class: &str) -> bool {
        self.primary.supports_cell(class)
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.primary.clone().invoke(context, invocation)
    }

    fn create_cell(
        &self,
        context: Context,
        identity: &CellIdentity,
    ) -> anyhow::Result<Arc<dyn Cell>> {
        self.primary.create_cell(context, identity)
    }

    fn validate_configuration(
        &self,
        options: &crate::engine_api::WorkerConfigOptions,
    ) -> anyhow::Result<()> {
        self.primary.validate_configuration(options)
    }

    fn validate_deployment(
        &self,
        deployment: &crate::fleet::LoadedDeployment,
    ) -> anyhow::Result<()> {
        self.primary.validate_deployment(deployment)
    }

    fn resolve_raft_class(&self, class: &str) -> Result<Arc<RaftClass>, RegistrationError> {
        self.primary.resolve_raft_class(class)
    }

    fn resolve_raft_namespace(
        &self,
        namespace: &NamespaceConfig,
        class: &str,
    ) -> Result<Arc<RaftClass>, RegistrationError> {
        for application in &self.applications {
            match application.resolve_raft_class(class) {
                Ok(definition) if definition.schema_digest() == namespace.schema_digest => {
                    return Ok(definition)
                }
                Ok(_)
                | Err(
                    RegistrationError::UnknownClass(_) | RegistrationError::UnsupportedFactory(_),
                ) => (),
                Err(error) => return Err(error),
            }
        }
        Err(RegistrationError::ContractMismatch)
    }
}

#[cfg(test)]
#[path = "native_applications_tests.rs"]
mod tests;
