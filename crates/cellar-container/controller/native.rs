// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary native SDK values over the existing fenced controller protocol.
use super::ControllerProvider;
use crate::{
    files, native, results, values, Limits, Operation, Reconciliation, ReconciliationState,
};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{containers::*, Context};
use serde_json::{json, Value};
use std::sync::Arc;

/// Host registration for Context.container on a shared-group class.
/// Interactive execution, port sessions and interception require their own
/// remote resource owners and remain refused by this bounded adapter.
pub struct NativeContainerAdapter {
    provider: Arc<ControllerProvider>,
    limits: Limits,
}

impl NativeContainerAdapter {
    pub fn new(provider: Arc<ControllerProvider>, limits: Limits) -> Result<Self> {
        let controller = provider.controller.limits();
        ensure!(
            limits.max_body_bytes > 0
                && limits.max_body_bytes <= controller.max_payload_bytes
                && limits.max_body_bytes <= controller.max_result_bytes,
            "native container bounds exceed the controller contract"
        );
        Ok(Self { provider, limits })
    }

    fn prepare_operation<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> Result<crate::PreparedProviderCall<'a>> {
        if operation != Operation::Start {
            return self
                .provider
                .prepare_call(context, operation, input, lifecycle);
        }
        let journal = BindingJournal::open(context, &self.provider.identity.binding)?;
        let previous = journal.load(context)?;
        let replacing = previous.as_ref().is_some_and(|previous| {
            previous.record.intent == BindingIntent::Running
                && previous.record.observation == BindingObservation::Confirmed
                && previous.record.resource_id.is_some()
        });
        if !replacing {
            // Pending Create/Delete keeps its original identity and recovery
            // contract. Only a confirmed resource selects native replacement.
            return self
                .provider
                .prepare_call(context, operation, input, lifecycle);
        }
        ensure!(
            lifecycle.is_some(),
            "native replacement requires lifecycle admission"
        );
        journal.check_retention(context)?;
        ensure!(
            previous
                .as_ref()
                .is_some_and(|previous| previous.record.generation < i64::MAX as u64),
            "binding generation exhausted"
        );
        let authority = context.container_provider_authority()?;
        let delete =
            self.provider
                .prepare_call(context, Operation::Destroy, json!({}), lifecycle)?;
        Ok(Box::new(move || {
            Box::pin(async move {
                // The framework proves the old generation's delete intent
                // before constructing this work. Unknown deletion returns here;
                // it never reserves or submits a new generation.
                delete().await?;
                // Like explicit Destroy.await followed by Start.await in one
                // callback, stage known retirement and the next intent in the
                // same private transaction. An uncertain Delete never gets here.
                let create =
                    self.provider
                        .prepare_call(context, Operation::Start, input, lifecycle)?;
                // Prove the resulting generation/intent/claim at the original
                // Quorum/S3 minimum before constructing any new provider work.
                // Recovery seals an unsubmitted Create with its original ID;
                // it never replays this multi-phase callback or renews expiry.
                authority.before_effect(context).await?;
                create().await
            })
        }))
    }
}

fn encode(request: ContainerRequest) -> Result<(Operation, Value)> {
    Ok(match request {
        ContainerRequest::Observe => (Operation::Reconcile, json!({})),
        ContainerRequest::Start(options) => (
            Operation::Start,
            json!({"options":{
                "entrypoint":options.entrypoint,"env":options.env,"enable_internet":options.enable_internet,"labels":options.labels
            }}),
        ),
        ContainerRequest::Destroy => (Operation::Destroy, json!({})),
        ContainerRequest::Monitor => (Operation::Monitor, json!({})),
        ContainerRequest::Signal(signal) => (Operation::Signal, json!({"signal":signal})),
        ContainerRequest::SetInactivityTimeout(timeout) => (
            Operation::SetInactivityTimeout,
            json!({"timeout_ms":u64::try_from(timeout.as_millis())?}),
        ),
        ContainerRequest::ReadFile { path, max_bytes } => (
            Operation::ReadFile,
            json!({"path":path.as_str(),"max_bytes":max_bytes}),
        ),
        ContainerRequest::WriteFile { path, data, mode } => (
            Operation::WriteFile,
            json!({"path":path.as_str(),"data":STANDARD.encode(data),"mode":mode}),
        ),
        ContainerRequest::StatFile(path) => (Operation::StatFile, json!({"path":path.as_str()})),
        ContainerRequest::ListFiles {
            path,
            max_bytes,
            max_entries,
        } => (
            Operation::ListFiles,
            json!({"path":path.as_str(),"max_bytes":max_bytes,"max_entries":max_entries}),
        ),
    })
}

fn decode(operation: Operation, value: Value) -> Result<ContainerReply> {
    Ok(match operation {
        Operation::Reconcile => {
            let result: Reconciliation = serde_json::from_value(value)?;
            ContainerReply::Running(match result.state {
                ReconciliationState::Running => true,
                ReconciliationState::Stopped
                | ReconciliationState::Deleted
                | ReconciliationState::Unbound => false,
                ReconciliationState::Deleting => {
                    anyhow::bail!("container lifecycle reconciliation remains pending")
                }
            })
        }
        Operation::Start => {
            let encoded = value.as_str().context("invalid container run")?;
            let run = encoded.parse::<u64>()?;
            ensure!(
                run != 0 && run.to_string() == encoded,
                "invalid container run"
            );
            ContainerReply::Started(run)
        }
        Operation::Monitor => {
            ContainerReply::Exited(value.as_i64().context("invalid container exit status")?)
        }
        Operation::Destroy
        | Operation::Signal
        | Operation::SetInactivityTimeout
        | Operation::WriteFile => ContainerReply::Done,
        Operation::ReadFile => ContainerReply::Bytes(
            STANDARD.decode(value.as_str().context("invalid container file bytes")?)?,
        ),
        Operation::StatFile => ContainerReply::Metadata(serde_json::from_value(value)?),
        Operation::ListFiles => ContainerReply::Entries(serde_json::from_value(value)?),
        _ => anyhow::bail!("invalid native container operation"),
    })
}

impl ContainerAdapter for NativeContainerAdapter {
    fn identity(&self) -> BackendIdentity {
        self.provider.identity()
    }

    fn max_body_bytes(&self) -> usize {
        self.limits.max_body_bytes
    }

    fn prepare<'a>(
        &'a self,
        context: &'a Context,
        request: ContainerRequest,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> Result<PreparedContainerCall<'a>> {
        if let ContainerRequest::WriteFile { data, .. } = &request {
            ensure!(
                data.len() <= self.limits.max_body_bytes,
                "file data exceeds host limit"
            );
        }
        let (operation, input) = encode(request)?;
        values::check_depth(&input)?;
        native::validate(operation.name(), &input, &self.limits)?;
        ensure!(
            serde_json::to_vec(&super::Request::new(operation, input.clone()))?.len()
                <= self.provider.controller.limits().max_payload_bytes,
            "native container request exceeds the controller payload bound"
        );
        let result_limit = match operation {
            Operation::ReadFile | Operation::ListFiles => input["max_bytes"]
                .as_u64()
                .context("file result limit missing")?
                as usize,
            _ => self.limits.max_body_bytes,
        };
        let entry_limit = if operation == Operation::ListFiles {
            Some(
                input["max_entries"]
                    .as_u64()
                    .context("file entry limit missing")? as usize,
            )
        } else {
            None
        };
        let call = self.prepare_operation(context, operation, input, lifecycle)?;
        Ok(Box::new(move || {
            Box::pin(async move {
                let result = call().await?;
                values::check_depth(&result)?;
                results::validate(operation, &result, result_limit)?;
                if let Some(entries) = entry_limit {
                    files::validate_listing(&result, result_limit, entries)?;
                }
                decode(operation, result)
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar::sdk::RaftClass;
    use std::time::Duration;

    struct Metadata(usize);

    impl ContainerAdapter for Metadata {
        fn identity(&self) -> BackendIdentity {
            BackendIdentity {
                binding: "TOOL".into(),
                provider: "test".into(),
                configuration_digest: "02".repeat(32),
            }
        }

        fn max_body_bytes(&self) -> usize {
            self.0
        }

        fn prepare<'a>(
            &'a self,
            _: &'a Context,
            _: ContainerRequest,
            _: Option<&'a BindingLifecycleGuard>,
        ) -> Result<PreparedContainerCall<'a>> {
            anyhow::bail!("metadata registration must not invoke provider preparation")
        }
    }

    #[test]
    fn native_container_registration_pins_capability_and_body_bounds() -> Result<()> {
        let bare = || RaftClass::new("Counter", 1, [1; 32]);
        let journal = bare()?.provider_binding("TOOL", "test", [2; 32])?;
        let adapter = bare()?.container_adapter(Arc::new(Metadata(4096)))?;
        let larger = bare()?.container_adapter(Arc::new(Metadata(8192)))?;
        ensure!(
            journal.schema_digest() != adapter.schema_digest()
                && adapter.schema_digest() != larger.schema_digest()
        );
        ensure!(adapter.container_adapter(Arc::new(Metadata(4096))).is_err());
        ensure!(bare()?.container_adapter(Arc::new(Metadata(0))).is_err());
        Ok(())
    }

    #[test]
    fn native_container_timeout_overflow_refuses_without_json_panic() {
        assert!(encode(ContainerRequest::SetInactivityTimeout(Duration::MAX)).is_err());
    }

    #[test]
    fn native_container_pending_reconciliation_cannot_report_stopped() {
        assert!(decode(
            Operation::Reconcile,
            json!({"state":"deleting","generation":"1"})
        )
        .is_err());
    }

    #[test]
    fn native_container_runs_require_canonical_nonzero_identity() {
        for value in ["0", "01", "-1", "18446744073709551616"] {
            assert!(decode(Operation::Start, json!(value)).is_err());
        }
        assert!(matches!(
            decode(Operation::Start, json!("1")),
            Ok(ContainerReply::Started(1))
        ));
    }
}
