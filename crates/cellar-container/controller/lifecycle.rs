// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{protocol, ControllerProvider, Reply, Request};
use crate::{Operation, Reconciliation, ReconciliationState};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{containers::*, Context, HandlerFuture};
use rand::RngCore;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// The controller proved no provider effect for this operation. An uncertain
/// mutation never carries this error. Start also proves generation retirement
/// before returning it; other operations retain their current generation.
#[derive(Debug)]
pub struct OperationRefused;

impl std::fmt::Display for OperationRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("provider operation was refused before effects")
    }
}

impl std::error::Error for OperationRefused {}

fn request_id(record: &BindingRecord) -> Result<[u8; 16]> {
    let mut bytes = [0; 16];
    for (index, pair) in record
        .request_id
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .enumerate()
    {
        ensure!(index < 16, "invalid binding request identity");
        bytes[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    ensure!(
        record.request_id.len() == 32 && bytes != [0; 16],
        "invalid binding request identity"
    );
    Ok(bytes)
}

fn delete_id(record: &BindingRecord) -> Result<[u8; 16]> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-controller-binding-delete-1");
    hash.update(request_id(record)?);
    Ok(hash.finalize()[..16].try_into()?)
}

fn original_create(claim: &ExecutionClaim) -> Result<ExecutionOperation> {
    let record = &claim.checkpoint().record;
    ensure!(
        record.parameters_digest.len() == 64,
        "invalid create digest"
    );
    let mut digest = [0; 32];
    for (index, pair) in record
        .parameters_digest
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .enumerate()
    {
        digest[index] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(ExecutionOperation {
        id: request_id(record)?,
        kind: ExecutionOperationKind::Create,
        payload_digest: digest,
        expires_unix_ms: 1,
    })
}

impl ControllerProvider {
    async fn recover_create(
        &self,
        context: &Context,
        grant: &ExecutionGrant,
    ) -> Result<Option<Reply>> {
        let original = match self
            .original(context, grant, ExecutionOperationKind::Create)
            .await?
        {
            Some(original) => original,
            None => {
                // Absence alone is not a fence: an older request may still be
                // queued. Atomically install its original-ID refusal first.
                let request = original_create(grant.claim())?;
                let controller = self.controller.clone();
                grant
                    .dispatch(context, move |claim| async move {
                        controller.seal_unsubmitted(claim, request).await
                    })
                    .await?
            }
        };
        match self.reconcile_original(context, grant, original).await? {
            ExecutionOutcome::Completed(bytes) => Ok(Some(
                Reply::decode(&bytes, grant.claim(), Operation::Start)
                    .context(ExecutionControllerFailure::UnknownOperation)?,
            )),
            ExecutionOutcome::Refused | ExecutionOutcome::ExpiredBeforeSubmission => Ok(None),
        }
    }

    fn confirm_create(
        &self,
        context: &Context,
        journal: &BindingJournal,
        grant: &ExecutionGrant,
        reply: &Reply,
    ) -> Result<BindingCheckpoint> {
        let current = journal
            .load(context)?
            .context("binding create disappeared")?;
        ensure!(
            grant.claim().checkpoint() == &current,
            "binding changed during create recovery"
        );
        let mut record = current.record;
        record.resource_id = Some(reply.resource_id.clone());
        record.observation = if record.intent == BindingIntent::Running {
            BindingObservation::Confirmed
        } else {
            BindingObservation::Pending
        };
        journal.prepare_exchange(context, Some(current.revision), record)
    }

    async fn retire(
        &self,
        context: &Context,
        journal: &BindingJournal,
        checkpoint: BindingCheckpoint,
        durable: bool,
    ) -> Result<()> {
        let mut record = checkpoint.record;
        record.intent = BindingIntent::Deleted;
        record.observation = BindingObservation::Confirmed;
        if durable {
            journal
                .compare_exchange(context, Some(checkpoint.revision), record)
                .await?;
        } else {
            journal.prepare_exchange(context, Some(checkpoint.revision), record)?;
        }
        Ok(())
    }

    pub(super) async fn start(
        &self,
        context: &Context,
        journal: &BindingJournal,
        lifecycle: &BindingLifecycleGuard,
        input: Value,
    ) -> Result<Value> {
        let parameters = Request::new(Operation::Start, input.clone()).parameters()?;
        let previous = journal.load(context)?;
        if let Some(current) = previous
            .as_ref()
            .filter(|current| !current.record.is_retired())
        {
            journal.check_retention(context)?;
            ensure!(
                current.record.intent == BindingIntent::Running
                    && current.record.observation == BindingObservation::Pending,
                "controller resource already exists or deletion is unresolved"
            );
            ensure!(
                current.record.parameters_digest == parameters,
                "pending create has different parameters"
            );
            let grant = self.fence(context, journal, Some(lifecycle)).await?;
            let reply = self.recover_create(context, &grant).await?;
            return match reply {
                Some(reply) => {
                    self.confirm_create(context, journal, &grant, &reply)?;
                    Ok(reply.value)
                }
                None => {
                    self.retire(context, journal, current.clone(), true).await?;
                    Err(OperationRefused.into())
                }
            };
        }
        let generation = previous.as_ref().map_or(Ok(1), |previous| {
            previous
                .record
                .generation
                .checked_add(1)
                .context("binding generation exhausted")
        })?;
        let mut nonce = [0; 16];
        cellar::asyncrt::rng("controller_provider_create").fill_bytes(&mut nonce);
        ensure!(nonce != [0; 16], "empty provider request identity");
        let record = BindingRecord {
            provider: self.identity.provider.clone(),
            configuration_digest: self.identity.configuration_digest.clone(),
            parameters_digest: parameters,
            generation,
            request_id: protocol::hex_digest(&nonce),
            resource_id: None,
            intent: BindingIntent::Running,
            observation: BindingObservation::Pending,
        };
        let current = journal.prepare_exchange(
            context,
            previous.as_ref().map(|previous| previous.revision),
            record,
        )?;
        let grant = self.fence(context, journal, Some(lifecycle)).await?;
        journal.check_retention(context)?;
        let reply = match self
            .call(
                context,
                &grant,
                Operation::Start,
                input,
                Some(request_id(&current.record)?),
            )
            .await
        {
            Ok(reply) => reply,
            Err(error) if error.downcast_ref::<OperationRefused>().is_some() => {
                self.retire(context, journal, current, true).await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        self.confirm_create(context, journal, &grant, &reply)?;
        Ok(reply.value)
    }

    async fn delete_resource(
        &self,
        context: &Context,
        grant: &ExecutionGrant,
        stable_id: [u8; 16],
    ) -> Result<Reply> {
        let bytes = serde_json::to_vec(&Request::new(Operation::Destroy, json!({})))?;
        let mut request = grant.next_operation(context, ExecutionOperationKind::Delete, &bytes)?;
        let controller = self.controller.clone();
        let outcome = grant
            .dispatch(context, move |claim| async move {
                // The deleted intent and this exact claim are already proven.
                // Settle old debt before deciding whether submission is safe.
                // Every call rechecks the immutable claim independently.
                Self::reconcile_pending(controller.clone(), claim.clone()).await?;
                match controller
                    .lifecycle(claim.clone(), ExecutionOperationKind::Delete)
                    .await?
                {
                    Some(original) => match controller.reconcile(claim.clone(), original).await? {
                        outcome @ ExecutionOutcome::Completed(_) => return Ok(outcome),
                        ExecutionOutcome::Refused | ExecutionOutcome::ExpiredBeforeSubmission => {
                            // A fresh attempt is allowed only after authoritative
                            // no-effect proof; uncertainty never selects this path.
                        }
                    },
                    None => request.id = stable_id,
                }
                controller.execute(claim, request, bytes).await
            })
            .await?;
        Self::reply(outcome, grant.claim(), Operation::Destroy)
    }

    async fn delete(
        &self,
        context: &Context,
        journal: &BindingJournal,
        mut grant: ExecutionGrant,
        durable: bool,
    ) -> Result<()> {
        let mut current = journal
            .load(context)?
            .context("binding delete disappeared")?;
        if current.record.resource_id.is_none() {
            self.reconcile_mutations(context, &grant).await?;
            let reply = self.recover_create(context, &grant).await?;
            match reply {
                None => return self.retire(context, journal, current, durable).await,
                Some(reply) => current.record.resource_id = Some(reply.resource_id),
            }
        }
        if current.record.intent != BindingIntent::Deleted
            || journal.load(context)?.as_ref() != Some(&current)
        {
            current.record.intent = BindingIntent::Deleted;
            current.record.observation = BindingObservation::Pending;
            current = journal.prepare_exchange(context, Some(current.revision), current.record)?;
            grant = grant.refresh(context).await?;
        }
        let reply = self
            .delete_resource(context, &grant, delete_id(&current.record)?)
            .await?;
        ensure!(
            reply.value.is_null(),
            ExecutionControllerFailure::UnknownOperation
        );
        self.retire(context, journal, current, durable).await
    }

    pub(super) async fn destroy(
        &self,
        context: &Context,
        journal: &BindingJournal,
        lifecycle: &BindingLifecycleGuard,
    ) -> Result<Value> {
        if journal
            .load(context)?
            .is_none_or(|current| current.record.is_retired())
        {
            return Ok(Value::Null);
        }
        let grant = self.fence(context, journal, Some(lifecycle)).await?;
        self.delete(context, journal, grant, false).await?;
        Ok(Value::Null)
    }

    pub(super) async fn reconcile(
        &self,
        context: &Context,
        journal: &BindingJournal,
        lifecycle: &BindingLifecycleGuard,
    ) -> Result<Value> {
        let Some(mut current) = journal.load(context)? else {
            return Ok(Reconciliation::value(ReconciliationState::Unbound, None));
        };
        if current.record.is_retired() {
            return Ok(Reconciliation::value(
                ReconciliationState::Deleted,
                Some(current.record.generation),
            ));
        }
        let mut grant = self.fence(context, journal, Some(lifecycle)).await?;
        if current.record.intent == BindingIntent::Deleted {
            // A proven intent can precede the controller's first reservation.
            // Drive the same fenced, stable-ID protocol as Destroy; observing
            // no receipt alone cannot leave the desired deletion stranded.
            self.delete(context, journal, grant, false).await?;
            return Ok(Reconciliation::value(
                ReconciliationState::Deleted,
                Some(current.record.generation),
            ));
        }
        if current.record.resource_id.is_none() {
            self.reconcile_mutations(context, &grant).await?;
            match self.recover_create(context, &grant).await? {
                Some(reply) => {
                    current = self.confirm_create(context, journal, &grant, &reply)?;
                    grant = grant.refresh(context).await?;
                }
                None => {
                    self.retire(context, journal, current.clone(), false)
                        .await?;
                    return Ok(Reconciliation::value(
                        ReconciliationState::Deleted,
                        Some(current.record.generation),
                    ));
                }
            }
        }
        let reply = self.observe(context, &grant).await?;
        let running = reply
            .value
            .as_bool()
            .context(ExecutionControllerFailure::UnknownOperation)?;
        Ok(Reconciliation::value(
            if running {
                ReconciliationState::Running
            } else {
                ReconciliationState::Stopped
            },
            Some(current.record.generation),
        ))
    }
}

impl FencedMaintenanceBackend for ControllerProvider {
    fn identity(&self) -> BackendIdentity {
        self.identity.clone()
    }

    fn authority(&self) -> Arc<dyn ExecutionFenceAuthority> {
        self.controller.clone()
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
        grant: ExecutionGrant,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            let journal = BindingJournal::open(&context, &self.identity.binding)?;
            ensure!(
                grant.claim().checkpoint() == &expected
                    && journal.load(&context)?.as_ref() == Some(&expected),
                "maintenance generation differs"
            );
            if action == MaintenanceAction::Delete
                || expected.record.intent == BindingIntent::Deleted
            {
                // Inspect-after-submission is conservative for generic
                // providers. This controller first reconciles original debt
                // and only submits the stable Delete ID if no entry exists.
                return self.delete(&context, &journal, grant, true).await;
            }
            if expected.record.resource_id.is_none() {
                self.reconcile_mutations(&context, &grant).await?;
                match self.recover_create(&context, &grant).await? {
                    Some(reply) => {
                        self.confirm_create(&context, &journal, &grant, &reply)?;
                    }
                    None => {
                        self.retire(&context, &journal, expected, true).await?;
                    }
                }
            } else {
                self.observe(&context, &grant).await?;
            }
            Ok(())
        })
    }
}
