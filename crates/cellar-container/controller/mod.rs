// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Normal provider sessions backed by independently durable execution ownership.
mod lifecycle;
mod protocol;
use crate::{Environment, Fencing, Limits, Operation, Provider, ProviderFuture, ProviderSession};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{containers::*, Context};
pub use lifecycle::OperationRefused;
pub use protocol::{Reply, Request};
use std::sync::Arc;

const OPERATIONS: &[Operation] = &[
    Operation::Start,
    Operation::Destroy,
    Operation::Reconcile,
    Operation::Running,
    Operation::Monitor,
    Operation::Signal,
    Operation::SetInactivityTimeout,
    Operation::ExecOutput,
    Operation::ReadFile,
    Operation::WriteFile,
    Operation::StatFile,
    Operation::ListFiles,
];

/// Host-selected Linux container bridge using the durable controller protocol.
/// The selected backend must implement Request/Reply and actual remote fencing;
/// the controller is not a Docker driver or an authenticated network transport.
/// Shared-group declarations must pin `identity()` in their provider binding.
#[derive(Clone)]
pub struct ControllerProvider {
    controller: Arc<ExecutionController>,
    identity: BackendIdentity,
}

impl ControllerProvider {
    pub fn new(controller: Arc<ExecutionController>) -> Result<Self> {
        ensure!(
            controller.options().limits.max_inflight >= 2,
            "container observations need reserved control capacity"
        );
        let identity = controller.identity();
        Ok(Self {
            controller,
            identity,
        })
    }

    pub fn identity(&self) -> BackendIdentity {
        self.identity.clone()
    }

    async fn fence(
        &self,
        context: &Context,
        journal: &BindingJournal,
        lifecycle: Option<&BindingLifecycleGuard>,
    ) -> Result<ExecutionGrant> {
        match lifecycle {
            Some(lifecycle) => {
                lifecycle
                    .fence_execution(context, self.controller.clone())
                    .await
            }
            None => {
                journal
                    .fence_execution(
                        context,
                        journal.lock_lifecycle(context).await?,
                        self.controller.clone(),
                    )
                    .await
            }
        }
    }

    async fn original(
        &self,
        context: &Context,
        grant: &ExecutionGrant,
        kind: ExecutionOperationKind,
    ) -> Result<Option<ExecutionPendingOperation>> {
        let controller = self.controller.clone();
        grant
            .dispatch(context, move |claim| async move {
                controller.lifecycle(claim, kind).await
            })
            .await
    }

    async fn reconcile_original(
        &self,
        context: &Context,
        grant: &ExecutionGrant,
        original: ExecutionPendingOperation,
    ) -> Result<ExecutionOutcome> {
        let controller = self.controller.clone();
        grant
            .dispatch(context, move |claim| async move {
                controller.reconcile(claim, original).await
            })
            .await
    }

    async fn reconcile_pending(
        controller: Arc<ExecutionController>,
        claim: ExecutionClaim,
    ) -> Result<()> {
        for original in controller.pending(claim.clone()).await? {
            match original.operation.kind {
                ExecutionOperationKind::Read | ExecutionOperationKind::Inspect => {}
                ExecutionOperationKind::Create
                | ExecutionOperationKind::Execute
                | ExecutionOperationKind::Write
                | ExecutionOperationKind::Delete
                | ExecutionOperationKind::Signal
                | ExecutionOperationKind::Configure => {
                    // Observe the original receipt only. Unknown remains debt;
                    // no recovery call may resubmit a mutation or renew its ID.
                    controller.reconcile(claim.clone(), original).await?;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_mutations(&self, context: &Context, grant: &ExecutionGrant) -> Result<()> {
        let controller = self.controller.clone();
        grant
            .dispatch(context, move |claim| {
                Self::reconcile_pending(controller, claim)
            })
            .await
    }

    async fn observe(&self, context: &Context, grant: &ExecutionGrant) -> Result<Reply> {
        let bytes = serde_json::to_vec(&Request::new(Operation::Running, serde_json::json!({})))?;
        let request = grant.next_operation(context, ExecutionOperationKind::Inspect, &bytes)?;
        let controller = self.controller.clone();
        let outcome = grant
            .dispatch(context, move |claim| async move {
                // These controller lookups share one admitted I/O closure. Each
                // still checks the same immutable claim, and none lends SQL
                // authority or replays provider work. Physical lifecycle/data
                // mutations retain their own intent and output proofs.
                Self::reconcile_pending(controller.clone(), claim.clone()).await?;
                controller.execute(claim, request, bytes).await
            })
            .await?;
        let reply = Self::reply(outcome, grant.claim(), Operation::Running)?;
        ensure!(
            reply.value.is_boolean(),
            ExecutionControllerFailure::UnknownOperation
        );
        Ok(reply)
    }

    fn reply(
        outcome: ExecutionOutcome,
        claim: &ExecutionClaim,
        operation: Operation,
    ) -> Result<Reply> {
        match outcome {
            ExecutionOutcome::Completed(bytes) => Reply::decode(&bytes, claim, operation)
                .context(ExecutionControllerFailure::UnknownOperation),
            ExecutionOutcome::Refused | ExecutionOutcome::ExpiredBeforeSubmission => {
                Err(OperationRefused.into())
            }
        }
    }

    async fn call(
        &self,
        context: &Context,
        grant: &ExecutionGrant,
        operation: Operation,
        input: serde_json::Value,
        stable_id: Option<[u8; 16]>,
    ) -> Result<Reply> {
        let bytes = serde_json::to_vec(&Request::new(operation, input))?;
        let mut request = grant.next_operation(context, protocol::kind(operation)?, &bytes)?;
        if let Some(id) = stable_id {
            request.id = id;
        }
        let controller = self.controller.clone();
        let result = grant
            .dispatch(context, move |claim| async move {
                controller.execute(claim, request, bytes).await
            })
            .await?;
        Self::reply(result, grant.claim(), operation)
    }

    async fn execute(
        &self,
        context: &Context,
        operation: Operation,
        input: serde_json::Value,
        lifecycle: Option<&BindingLifecycleGuard>,
    ) -> Result<serde_json::Value> {
        let journal = BindingJournal::open(context, &self.identity.binding)?;
        match operation {
            Operation::Start => {
                self.start(
                    context,
                    &journal,
                    lifecycle.context("controller Start requires lifecycle admission")?,
                    input,
                )
                .await
            }
            Operation::Destroy => {
                self.destroy(
                    context,
                    &journal,
                    lifecycle.context("controller Destroy requires lifecycle admission")?,
                )
                .await
            }
            Operation::Reconcile => {
                self.reconcile(
                    context,
                    &journal,
                    lifecycle.context("controller Reconcile requires lifecycle admission")?,
                )
                .await
            }
            _ => {
                journal.check_retention(context)?;
                let record = journal.load(context)?;
                if record
                    .as_ref()
                    .is_none_or(|record| record.record.is_retired())
                    && operation == Operation::Running
                {
                    return Ok(serde_json::json!(false));
                }
                let record = record.context("controller resource is unbound")?;
                ensure!(
                    record.record.intent == BindingIntent::Running
                        && record.record.observation == BindingObservation::Confirmed,
                    "controller resource requires lifecycle reconciliation"
                );
                let grant = self.fence(context, &journal, lifecycle).await?;
                let grant = if operation.is_read() {
                    grant.release_lifecycle(context)?
                } else {
                    grant
                };
                Ok(self
                    .call(context, &grant, operation, input, None)
                    .await?
                    .value)
            }
        }
    }
}

impl Provider for ControllerProvider {
    fn name(&self) -> &str {
        &self.identity.provider
    }

    fn binding_name(&self) -> Option<&str> {
        Some(&self.identity.binding)
    }

    fn environment(&self) -> Environment {
        Environment::Linux
    }

    fn fencing(&self) -> Fencing {
        Fencing::ProviderEnforced
    }

    fn operations(&self) -> &[Operation] {
        OPERATIONS
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(self.clone())
    }
}

impl ProviderSession for ControllerProvider {
    fn lifecycle_admission(&self, operation: Operation) -> bool {
        !operation.is_read()
    }

    fn execute<'a>(
        &'a self,
        _context: &'a Context,
        _operation: Operation,
        _input: serde_json::Value,
        _limits: &'a Limits,
    ) -> ProviderFuture<'a> {
        Box::pin(async { anyhow::bail!("controller provider requires checked session dispatch") })
    }

    fn execute_admitted<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: serde_json::Value,
        _limits: &'a Limits,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> ProviderFuture<'a> {
        Box::pin(self.execute(context, operation, input, lifecycle))
    }
}
