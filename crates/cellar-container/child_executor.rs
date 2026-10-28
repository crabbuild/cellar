// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded provider operations for authenticated, independently owned children.
use crate::{Adapter, Cancelled, Fencing, Limits, Operation, Provider, Validation};
use anyhow::{ensure, Result};
use cellar::sdk::{
    containers::{DeleteSafety, SandboxExecutor, SandboxFailure, SandboxFailureKind},
    Context, HandlerFuture,
};
use serde_json::Value;
use std::sync::Arc;

/// Registers a provider for a dedicated `Application::sandbox_executor` class.
/// Include account, image, policy and provider settings in the host-supplied
/// configuration digest. Never put credentials in the digest's source manifest.
/// Each operation gets a fresh invocation session; no process/socket handles
/// cross child calls. All binary bodies and combined output are bounded to 64 KiB.
#[derive(Clone)]
pub struct ChildExecutor {
    adapter: Adapter,
    configuration: String,
    limits: Limits,
}

impl ChildExecutor {
    pub fn new(provider: Arc<dyn Provider>, configuration_digest: String) -> Result<Self> {
        ensure!(
            configuration_digest.len() == 64
                && configuration_digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "invalid sandbox executor configuration digest"
        );
        for operation in [Operation::Start, Operation::Reconcile, Operation::Destroy] {
            ensure!(
                provider.operations().contains(&operation),
                "sandbox executor requires start, reconcile and destroy"
            );
        }
        ensure!(
            provider.fencing() == Fencing::NativeCell
                || provider.maintenance_backend().is_some_and(
                    |backend| backend.delete_safety() == DeleteSafety::RevalidateExactResource
                ),
            "sandbox executor requires repeat-safe exact-resource cleanup"
        );
        Ok(Self {
            adapter: Adapter::new(provider),
            configuration: configuration_digest,
            limits: Limits {
                max_body_bytes: 64 * 1024,
            },
        })
    }
}

fn bounded(operation: Operation) -> bool {
    matches!(
        operation,
        Operation::Capabilities
            | Operation::Running
            | Operation::Start
            | Operation::Reconcile
            | Operation::Destroy
            | Operation::ExecOutput
            | Operation::ReadFile
            | Operation::WriteFile
            | Operation::StatFile
            | Operation::ListFiles
            | Operation::WaitPort
            | Operation::Fetch
    )
}

impl SandboxExecutor for ChildExecutor {
    fn requires_native_capacity(&self) -> bool {
        self.adapter.provider().fencing() == Fencing::NativeCell
    }

    fn configuration_digest(&self) -> &str {
        &self.configuration
    }

    fn execute(
        self: Arc<Self>,
        context: Context,
        operation: String,
        input: Value,
    ) -> HandlerFuture<Value> {
        Box::pin(async move {
            let operation = Operation::parse(&operation).map_err(|_| SandboxFailure {
                kind: SandboxFailureKind::Refused,
            })?;
            ensure!(
                bounded(operation),
                SandboxFailure {
                    kind: SandboxFailureKind::Refused
                }
            );
            let result = self
                .adapter
                .session()
                .execute(&context, operation, input, &self.limits)
                .await;
            match result {
                Ok(value) if operation == Operation::Capabilities => {
                    let mut capabilities: crate::Capabilities = serde_json::from_value(value)?;
                    capabilities
                        .operations
                        .retain(|name| Operation::parse(name).is_ok_and(bounded));
                    capabilities.synchronous_running = false;
                    capabilities.queued_lifecycle = false;
                    capabilities.max_resources = Some(0);
                    Ok(serde_json::to_value(capabilities)?)
                }
                Ok(value) => Ok(value),
                Err(error) => {
                    let kind = if error.is::<cellar::container::CapacityExceeded>() {
                        SandboxFailureKind::Capacity
                    } else if error.is::<Validation>()
                        || error
                            .downcast_ref::<Cancelled>()
                            .is_some_and(Cancelled::before_dispatch)
                    {
                        SandboxFailureKind::Refused
                    } else {
                        SandboxFailureKind::Unknown
                    };
                    Err(SandboxFailure { kind }.into())
                }
            }
        })
    }
}
