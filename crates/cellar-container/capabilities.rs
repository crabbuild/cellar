// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configuration-time capability metadata, shared by every language facade.
use crate::{Limits, Operation, Provider};
use serde::{Deserialize, Serialize};

/// An isolate cannot promise Linux processes, PTYs or a filesystem merely by
/// implementing a method with the same name as a Linux provider's method.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Environment {
    Unspecified,
    Linux,
    WorkerIsolate,
}

/// Ownership protection actually supplied by this configured provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fencing {
    Unspecified,
    NativeCell,
    LocalChecksAndExpiry,
    ProviderEnforced,
}

/// Requested ownership guarantee for a direct cloud provider. Local checks
/// cannot revoke requests already accepted by a remote service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ownership {
    RequireProviderFence,
    LocalChecksAndExpiry,
}

/// Only operations actually declared by the configured provider are advertised.
/// Optional future features are absent until a backend implements their contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub format: u8,
    pub provider: String,
    pub environment: Environment,
    pub fencing: Fencing,
    pub operations: Vec<String>,
    pub max_body_bytes: usize,
    pub max_resources: Option<usize>,
    pub synchronous_running: bool,
    pub queued_lifecycle: bool,
    pub exec_pty: bool,
}

impl Capabilities {
    pub(crate) fn new(provider: &dyn Provider, limits: &Limits) -> Self {
        let operations = Operation::ALL
            .iter()
            .filter(|op| **op == Operation::Capabilities || provider.operations().contains(op))
            .map(|op| op.name().to_owned())
            .collect();
        Self {
            format: 1,
            provider: provider.name().to_owned(),
            environment: provider.environment(),
            fencing: provider.fencing(),
            operations,
            max_body_bytes: limits.max_body_bytes,
            max_resources: provider.max_resources(),
            synchronous_running: provider.synchronous_running()
                && provider.operations().contains(&Operation::Running),
            queued_lifecycle: crate::queued::supported(provider),
            exec_pty: provider.exec_pty()
                && (provider.operations().contains(&Operation::Exec)
                    || provider.operations().contains(&Operation::ExecOutput)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Adapter, Native};
    use std::sync::Arc;

    #[test]
    fn native_capabilities_report_actual_operations_and_host_limit() {
        let capabilities =
            Adapter::new(Arc::new(Native)).capabilities(&Limits { max_body_bytes: 42 });
        assert_eq!(capabilities.provider, "native");
        assert_eq!(capabilities.environment, Environment::Linux);
        assert_eq!(capabilities.fencing, Fencing::NativeCell);
        assert_eq!(capabilities.max_body_bytes, 42);
        assert_eq!(capabilities.max_resources, Some(64));
        assert!(capabilities.synchronous_running);
        assert!(capabilities.queued_lifecycle);
        assert_eq!(capabilities.operations.len(), Operation::ALL.len() - 1);
        assert!(!capabilities
            .operations
            .contains(&Operation::InterceptOutboundHttp.name().into()));
        let extended = Adapter::new(Arc::new(Native.with_http_interception()))
            .capabilities(&Limits { max_body_bytes: 42 });
        assert_eq!(extended.operations.len(), Operation::ALL.len());
        assert!(extended
            .operations
            .contains(&Operation::InterceptOutboundHttp.name().into()));
        let mut same_metadata = extended;
        same_metadata.operations = capabilities.operations.clone();
        assert_eq!(same_metadata, capabilities);
        assert!(capabilities
            .operations
            .iter()
            .all(|name| Operation::parse(name).is_ok()));
        assert!(!capabilities
            .operations
            .iter()
            .any(|name| name.contains("snapshot")));
        let encoded = serde_json::to_value(&capabilities).unwrap();
        assert_eq!(
            serde_json::from_value::<Capabilities>(encoded).unwrap(),
            capabilities
        );
    }
}
