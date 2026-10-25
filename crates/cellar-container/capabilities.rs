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

/// Only operations actually declared by the configured provider are advertised.
/// Optional future features are absent until a backend implements their contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub format: u8,
    pub provider: String,
    pub environment: Environment,
    pub operations: Vec<String>,
    pub max_body_bytes: usize,
    pub max_resources: Option<usize>,
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
            operations,
            max_body_bytes: limits.max_body_bytes,
            max_resources: provider.max_resources(),
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
        assert_eq!(capabilities.max_body_bytes, 42);
        assert_eq!(capabilities.max_resources, Some(64));
        assert_eq!(capabilities.operations.len(), Operation::ALL.len());
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
