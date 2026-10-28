// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host-selected child executors and bounded authenticated calls.
use super::{SandboxAllocation, SandboxAllocationState, SandboxCatalog, SandboxParentState};
use crate::application::{Context, HandlerFuture};
use crate::runtime::sandbox_execution::{Action, Authority, Identity, Reply};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub const MAX_SANDBOX_MESSAGE_BYTES: usize = 256 * 1024;

/// Host registration for a dedicated execution class. Ordinary application
/// dispatch to this class is refused. Every call gets fresh child event authority.
/// Destroy must prove exact-resource retirement and be safe to repeat after
/// revalidation. An unknown create or command must never be replayed internally.
pub trait SandboxExecutor: Send + Sync + 'static {
    /// Stable digest of provider/account/image/policy configuration, without secrets.
    fn configuration_digest(&self) -> &str;

    /// Native creation needs Engine placement capacity. Cleanup and observations
    /// always use ordinary routing, including on full hosts.
    fn requires_native_capacity(&self) -> bool {
        false
    }

    fn execute(
        self: Arc<Self>,
        context: Context,
        operation: String,
        input: Value,
    ) -> HandlerFuture<Value>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxFailureKind {
    Capacity,
    Refused,
    Unknown,
}

/// A returned refusal describes this operation. A lost transport reply remains
/// unknown, even if the destination may have refused before provider effects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxFailure {
    pub kind: SandboxFailureKind,
}

impl std::fmt::Display for SandboxFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self.kind {
            SandboxFailureKind::Capacity => "sandbox capacity refused before provider submission",
            SandboxFailureKind::Refused => "sandbox operation refused before provider submission",
            SandboxFailureKind::Unknown => "sandbox outcome requires reconciliation",
        })
    }
}

impl std::error::Error for SandboxFailure {}

pub(crate) fn digest(parameters: &Value) -> Result<String> {
    // Feature unification can enable serde_json's insertion-ordered maps.
    // Sort recursively so the identity never depends on input key order.
    let mut ordered = parameters.clone();
    ordered.sort_all_objects();
    let encoded = serde_json::to_vec(&ordered)?;
    ensure!(
        encoded.len() <= MAX_SANDBOX_MESSAGE_BYTES,
        "sandbox message exceeds byte limit"
    );
    Ok(hex::encode(Sha256::digest(encoded)))
}

impl SandboxCatalog {
    /// Reserve and start one named child in a host-configured execution class.
    /// A repeated call consults the child's durable creation intent; it cannot
    /// replay an entered create. Proven capacity refusal permits a later retry.
    pub fn start_child(
        &self,
        context: &Context,
        name: &str,
        binding: &str,
        parameters: Value,
    ) -> HandlerFuture<SandboxAllocation> {
        let catalog = self.clone();
        let context = context.clone();
        let name = name.to_string();
        let binding = binding.to_string();
        Box::pin(async move {
            let digest = digest(&parameters)?;
            let target = context
                .namespace(&binding)?
                .id_from_name("configuration")
                .scope();
            let class = target
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("sandbox namespace has no class"))?
                .0;
            let executor = context
                .worker_config
                .native_application
                .as_ref()
                .and_then(|app| app.sandbox_executor(class))
                .ok_or_else(|| anyhow::anyhow!("sandbox executor is not registered"))?;
            let reserved = catalog
                .reserve_registered(
                    &context,
                    &name,
                    &binding,
                    &digest,
                    Some(executor.configuration_digest()),
                )
                .await?
                .allocation;
            catalog
                .call_child(&context, &reserved, Action::Start, parameters)
                .await?;
            // Re-read: a concurrent parent retirement/deletion closes this
            // acknowledgement even if the child accepted the earlier start.
            let current = catalog
                .get(&context, &name)?
                .ok_or_else(|| anyhow::anyhow!("sandbox reservation disappeared"))?;
            ensure!(
                current.generation == reserved.generation,
                "sandbox generation changed during startup"
            );
            catalog.confirm_ready(&context, &current).await
        })
    }

    /// Observe the same child/provider identity without creating or resuming it.
    pub fn inspect_child(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
    ) -> HandlerFuture<Value> {
        self.call_child(context, expected, Action::Inspect, serde_json::json!({}))
    }

    /// Execute a bounded operation without exporting invocation-local handles.
    /// Commands are never retried automatically after a lost response.
    pub fn execute_child(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
        operation: &str,
        input: Value,
    ) -> HandlerFuture<Value> {
        self.call_child(
            context,
            expected,
            Action::Call {
                operation: operation.into(),
            },
            input,
        )
    }

    /// Persist cleanup intent, then delete the exact child. Failed or cancelled
    /// cleanup remains scheduled independently of the application's alarm.
    pub fn destroy_child(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
    ) -> HandlerFuture<SandboxAllocation> {
        let catalog = self.clone();
        let context = context.clone();
        let expected = expected.clone();
        Box::pin(async move {
            let result = async {
                let deleting = catalog.begin_delete(&context, &expected).await?;
                catalog
                    .call_child(&context, &deleting, Action::Destroy, serde_json::json!({}))
                    .await?;
                catalog.confirm_deleted(&context, &deleting).await
            }
            .await;
            if result.is_err() {
                // A concurrent cleanup can confirm this exact generation while
                // this call awaits RPC/durability. Its committed tombstone is
                // positive evidence; an error or an absent record is not.
                if let Some(current) = catalog.get(&context, &expected.name)? {
                    let mut retired = expected;
                    retired.state = SandboxAllocationState::Deleted;
                    retired.revision = current.revision;
                    if current == retired {
                        context.storage()?.sync().await?;
                        ensure!(
                            catalog.get(&context, &current.name)?.as_ref() == Some(&current),
                            "sandbox replacement raced deletion acknowledgement"
                        );
                        return Ok(current);
                    }
                }
            }
            result
        })
    }

    pub(crate) fn call_child(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
        action: Action,
        input: Value,
    ) -> HandlerFuture<Value> {
        let prepared: Result<_> = (|| {
            let scope = self.check(context)?;
            ensure!(
                self.get(context, &expected.name)?.as_ref() == Some(expected),
                "sandbox allocation checkpoint changed"
            );
            let parent = self
                .parent(context)?
                .ok_or_else(|| anyhow::anyhow!("sandbox parent missing"))?;
            let cleanup = matches!(action, Action::Destroy);
            ensure!(
                if cleanup {
                    expected.state == SandboxAllocationState::Deleting
                } else {
                    parent.state == SandboxParentState::Active
                        && matches!(
                            expected.state,
                            SandboxAllocationState::Reserved | SandboxAllocationState::Ready
                        )
                },
                "sandbox lifecycle refuses operation"
            );
            action.validate()?;
            digest(&input)?;
            let class = expected
                .execution_scope
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("sandbox target missing class"))?
                .0;
            let executor = context
                .worker_config
                .native_application
                .as_ref()
                .and_then(|app| app.sandbox_executor(class))
                .ok_or_else(|| anyhow::anyhow!("sandbox execution class is not registered"))?;
            let identity =
                Identity::new(scope, &parent, expected, executor.configuration_digest())?;
            crate::storage::sandbox_work::check_registration(
                scope,
                expected,
                executor.configuration_digest(),
            )?;
            let authority = Authority::new(identity, action, executor.requires_native_capacity())?;
            Ok((authority, context.storage()?.sync()))
        })();
        let catalog = self.clone();
        let context = context.clone();
        let expected = expected.clone();
        Box::pin(async move {
            let (authority, flush) = prepared?;
            flush.await?;
            catalog.check(&context)?;
            ensure!(
                catalog.get(&context, &expected.name)?.as_ref() == Some(&expected),
                "sandbox allocation changed before dispatch"
            );
            if !matches!(authority.action(), Action::Destroy) {
                ensure!(
                    catalog
                        .parent(&context)?
                        .is_some_and(|parent| parent.state == SandboxParentState::Active),
                    "sandbox parent admission closed"
                );
            }
            let cleanup = matches!(authority.action(), Action::Destroy);
            let result = crate::native_worker::sandbox_rpc(
                &context,
                authority,
                expected.execution_name.clone(),
                input,
            )
            .await?;
            catalog.check(&context)?;
            ensure!(
                catalog.get(&context, &expected.name)?.as_ref() == Some(&expected),
                "sandbox allocation changed while awaiting child reply"
            );
            if !cleanup {
                ensure!(
                    catalog
                        .parent(&context)?
                        .is_some_and(|parent| parent.state == SandboxParentState::Active),
                    "sandbox parent admission closed while awaiting child reply"
                );
            }
            let reply: Reply =
                serde_json::from_value(crate::application::decode_rpc(&result)?.to_json()?)?;
            match reply {
                Reply::Success(value) => Ok(value),
                Reply::Failure(failure) => Err(failure.into()),
            }
        })
    }
}
