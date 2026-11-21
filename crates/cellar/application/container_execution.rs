// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable execution claims for an independently enforced provider fence.
use super::{
    containers::{BackendIdentity, BindingCheckpoint, BindingJournal, BindingLifecycleGuard},
    Context, HandlerFuture,
};
use anyhow::{ensure, Result};
use cellar_logic::cell_group::CellHistory;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// One proven ownership claim. This is metadata for a trusted controller, not
/// a bearer credential, distributed lease or proof that a process has stopped.
/// Authenticate the host/controller channel and enforce this claim on *every*
/// provider request, including retained processes, sockets and file handles.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(
    try_from = "super::container_execution_wire::Wire",
    into = "super::container_execution_wire::Wire"
)]
pub struct ExecutionClaim {
    pub(super) history: CellHistory,
    pub(super) binding: String,
    pub(super) key: [u8; 32],
    pub(super) checkpoint: BindingCheckpoint,
    pub(super) claim: crate::storage::container_execution::Claim,
}

impl ExecutionClaim {
    pub fn history(&self) -> &CellHistory {
        &self.history
    }

    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Stable across elections, movement and resource generations. Includes
    /// cluster, namespace, exact cell ID, cell incarnation and binding name.
    pub fn resource_key(&self) -> [u8; 32] {
        self.key
    }

    /// Strictly increasing across changes of group/term or binding checkpoint,
    /// carried with the cell image. Physical group terms are never compared.
    pub fn sequence(&self) -> u64 {
        self.claim.sequence
    }

    /// Immutable within an ownership claim; compare together with sequence.
    pub fn owner_digest(&self) -> &str {
        &self.claim.owner
    }

    /// Original resource generation, create identity and current binding
    /// observation. A new claim never permits replay of an unknown operation.
    pub fn checkpoint(&self) -> &BindingCheckpoint {
        &self.checkpoint
    }
}

/// Host-selected controller which independently enforces execution ownership.
/// Its configuration, including controller identity, must be covered by the
/// binding's configuration digest. There is deliberately no permissive default.
pub trait ExecutionFenceAuthority: Send + Sync + 'static {
    fn identity(&self) -> BackendIdentity;

    /// Authenticate the caller. Persist a high-water mark per resource_key;
    /// reject lower sequences and equal sequences with another owner/checkpoint.
    /// For a higher sequence, stop/revoke *all* older execution and prove it
    /// cannot resume before activating the new grant. Serialize this transition
    /// with provider submission and reject old grants on every provider request.
    /// Repeating the exact claim reconciles it without creating another grant.
    /// Lost controller state must refuse admission, never reset the high-water
    /// mark. Cancellation/lost replies leave the original claim unresolved.
    /// A fresh cell incarnation cannot bypass fencing: independently establish
    /// permanent revocation of the old history's provider execution before
    /// admitting the new history. A fresh ID or Raft receipt is insufficient.
    fn activate(self: Arc<Self>, claim: ExecutionClaim) -> HandlerFuture<()>;
}

/// Exact invocation authority plus a controller-acknowledged claim. Retaining
/// this handle does not extend the invocation deadline or provider grant.
pub struct ExecutionGrant {
    journal: BindingJournal,
    claim: ExecutionClaim,
    lifecycle: Option<Arc<BindingLifecycleGuard>>,
    authority: Arc<dyn ExecutionFenceAuthority>,
}

impl ExecutionGrant {
    pub fn claim(&self) -> &ExecutionClaim {
        &self.claim
    }

    /// Bind a finite controller operation to this exact command and ordinal.
    /// Reuse an ordinal only for the same operation; recovery keeps that ID.
    /// The controller retains the first accepted deadline even if a later
    /// receipt lookup carries a new request budget. This performs no I/O.
    pub fn operation(
        &self,
        context: &Context,
        ordinal: u64,
        kind: super::containers::ExecutionOperationKind,
        payload: &[u8],
    ) -> Result<super::containers::ExecutionOperation> {
        self.operation_in_domain(
            context,
            ordinal,
            kind,
            payload,
            b"cellar-controller-invocation-operation-1",
        )
    }

    /// Allocate a unique operation within this exact invocation. Sessions using
    /// the same Context share the counter; explicit caller ordinals use a
    /// separate identity domain. No invocation authority or deadline is renewed.
    pub fn next_operation(
        &self,
        context: &Context,
        kind: super::containers::ExecutionOperationKind,
        payload: &[u8],
    ) -> Result<super::containers::ExecutionOperation> {
        self.check(context)?;
        let ordinal = context
            .state
            .provider_operation
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |n| n.checked_add(1),
            )
            .map_err(|_| super::containers::ExecutionControllerFailure::Capacity)?;
        self.operation_in_domain(
            context,
            ordinal,
            kind,
            payload,
            b"cellar-controller-invocation-automatic-operation-1",
        )
    }

    fn operation_in_domain(
        &self,
        context: &Context,
        ordinal: u64,
        kind: super::containers::ExecutionOperationKind,
        payload: &[u8],
        domain: &[u8],
    ) -> Result<super::containers::ExecutionOperation> {
        self.check(context)?;
        ensure!(
            !self.claim.checkpoint.record.is_retired(),
            super::containers::ExecutionControllerFailure::StaleClaim
        );
        let source = crate::storage::capture_raft_dispatch()?
            .ok_or(super::consensus::CommandFailure::UnsupportedCapability)?
            .binding_source()?;
        ensure!(
            source.history == self.claim.history,
            "controller operation history differs"
        );
        let mut id = Sha256::new();
        id.update(domain);
        id.update(self.claim.resource_key());
        id.update(source.parent.client.bytes());
        id.update(source.parent.sequence().to_be_bytes());
        id.update(ordinal.to_be_bytes());
        let id: [u8; 32] = id.finalize().into();
        super::containers::ExecutionOperation::new(
            id[..16].try_into()?,
            kind,
            payload,
            source
                .deadline_unix_ms
                .ok_or(super::consensus::CommandFailure::Unavailable)?,
        )
    }

    /// Reconcile a fresh claim after checkpointing a binding change, retaining
    /// this invocation's existing lifecycle admission and selected controller.
    /// The old grant remains invalid for the changed checkpoint. No business
    /// operation is retried; the controller reconciles the exact new claim.
    pub fn refresh(&self, context: &Context) -> HandlerFuture<ExecutionGrant> {
        let valid = match &self.lifecycle {
            Some(lifecycle) => lifecycle.check(context, &self.journal),
            None => self.journal.check(context).map(|_| ()),
        };
        if let Err(error) = valid {
            return Box::pin(async move { Err(error) });
        }
        let journal = self.journal.clone();
        let context = context.clone();
        let lifecycle = self.lifecycle.clone();
        let authority = self.authority.clone();
        Box::pin(async move {
            let guard = match lifecycle {
                Some(guard) => guard.retained(),
                None => journal.lock_lifecycle(&context).await?,
            };
            journal.fence_execution(&context, guard, authority).await
        })
    }

    /// Release lifecycle serialization for data/stream observations. The exact
    /// invocation, checkpoint and execution claim remain bound to this grant;
    /// another Context cannot lend it a newer owner or generation.
    pub fn release_lifecycle(mut self, context: &Context) -> Result<Self> {
        self.check(context)?;
        self.lifecycle.take();
        Ok(self)
    }

    fn retained(&self) -> Self {
        Self {
            journal: self.journal.clone(),
            claim: self.claim.clone(),
            lifecycle: self.lifecycle.clone(),
            authority: self.authority.clone(),
        }
    }

    /// Run provider I/O under this original invocation after its required proof,
    /// parking the source writer rather than lending SQL authority to the I/O.
    /// The provider still enforces the supplied claim at its execution boundary.
    pub fn dispatch<T, F, Fut>(&self, context: &Context, action: F) -> HandlerFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(ExecutionClaim) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        let prepared = (|| -> Result<_> {
            self.check(context)?;
            ensure!(
                !self.claim.checkpoint.record.is_retired(),
                super::containers::ExecutionControllerFailure::StaleClaim
            );
            Ok(crate::storage::capture_raft_dispatch()?
                .ok_or(super::consensus::CommandFailure::UnsupportedCapability)?)
        })();
        let dispatch = match prepared {
            Ok(dispatch) => dispatch,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let grant = self.retained();
        let context = context.clone();
        Box::pin(async move {
            let claim = grant.claim.clone();
            let result = dispatch
                .schedule_effect(move |_| action(claim))
                .future
                .await??;
            grant.check(&context)?;
            Ok(result)
        })
    }

    /// Local publication check only. The provider must still reject stale
    /// claims at its own execution boundary, including already-open handles.
    pub fn check(&self, context: &Context) -> Result<()> {
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.check(context, &self.journal)?;
        }
        let scope = self.journal.check(context)?;
        ensure!(
            self.journal.load(context)?.as_ref() == Some(&self.claim.checkpoint),
            "provider binding changed during execution; outcome may be unknown"
        );
        crate::storage::container_execution::check(scope, &self.journal.name, &self.claim.claim)
    }

    /// Prove business changes at the original invocation's required durability
    /// before submitting work with this claim to the enforcing controller.
    pub fn before_effect(&self, context: &Context) -> HandlerFuture<()> {
        if let Err(error) = self.check(context) {
            return Box::pin(async move { Err(error) });
        }
        let gate = self.journal.authority.before_effect(context);
        let journal = self.journal.clone();
        let claim = self.claim.clone();
        let lifecycle = self.lifecycle.clone();
        let authority = self.authority.clone();
        let context = context.clone();
        Box::pin(async move {
            gate.await?;
            ExecutionGrant {
                journal,
                claim,
                lifecycle,
                authority,
            }
            .check(&context)
        })
    }
}

impl BindingJournal {
    /// Stage the exact execution claim in this invocation's private SQLite
    /// transaction without controller I/O or a durability acknowledgement.
    /// Lifecycle admission must remain held. This metadata is not permission
    /// to execute: await `fence_execution` before any controller/provider I/O.
    /// Its idempotent reservation then proves/reuses the original invocation's
    /// complete checkpoint, including business changes and binding intent.
    pub fn prepare_execution_claim(
        &self,
        context: &Context,
        lifecycle: &BindingLifecycleGuard,
        authority: &dyn ExecutionFenceAuthority,
    ) -> Result<ExecutionClaim> {
        lifecycle.check(context, self)?;
        let scope = self.check(context)?;
        ensure!(
            context.raft_storage.is_some(),
            super::consensus::CommandFailure::UnsupportedCapability
        );
        let dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(super::consensus::CommandFailure::UnsupportedCapability)?;
        let source = dispatch.binding_source()?;
        ensure!(
            source.term != 0 && source.group.same_namespace(&source.history),
            super::consensus::CommandFailure::Unavailable
        );
        let checkpoint = self
            .load(context)?
            .ok_or_else(|| anyhow::anyhow!("provider execution requires a reserved binding"))?;
        let expected = BackendIdentity {
            binding: self.name.to_string(),
            provider: checkpoint.record.provider.clone(),
            configuration_digest: checkpoint.record.configuration_digest.clone(),
        };
        ensure!(
            authority.identity() == expected,
            "execution fence authority configuration mismatch"
        );
        let history_digest = crate::raft::cell_groups::cell_image_history_digest(&source.history);
        let mut owner = Sha256::new();
        owner.update(b"cellar-provider-execution-owner-1");
        owner.update(history_digest);
        owner.update(source.group.group.bytes());
        owner.update(source.group.incarnation.bytes());
        owner.update(source.term.to_be_bytes());
        owner.update(source.configuration);
        let claim = crate::storage::container_execution::claim(
            scope,
            &self.name,
            &checkpoint,
            &hex::encode(owner.finalize()),
        )?;
        let mut key = Sha256::new();
        key.update(b"cellar-provider-execution-resource-1");
        key.update(history_digest);
        key.update((self.name.len() as u16).to_be_bytes());
        key.update(self.name.as_bytes());
        let claim = ExecutionClaim {
            history: source.history,
            binding: self.name.to_string(),
            key: key.finalize().into(),
            checkpoint,
            claim,
        };
        Ok(claim)
    }

    /// Reserve and prove a shared-group ownership claim, then ask the selected
    /// external authority to fence prior execution. No controller I/O occurs
    /// before the required quorum/S3 proof. Errors retain the claim for exact
    /// reconciliation; this function never retries the controller or business
    /// operation. Consumes the exact binding's lifecycle guard and retains it
    /// through the returned grant and its effect gates. Drop the grant before
    /// acquiring another lifecycle guard. Changing the binding requires a new
    /// grant; never change it concurrently with provider I/O.
    pub fn fence_execution(
        &self,
        context: &Context,
        lifecycle: BindingLifecycleGuard,
        authority: Arc<dyn ExecutionFenceAuthority>,
    ) -> HandlerFuture<ExecutionGrant> {
        let journal = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let claim =
                journal.prepare_execution_claim(&context, &lifecycle, authority.as_ref())?;
            let dispatch = crate::storage::capture_raft_dispatch()?
                .ok_or(super::consensus::CommandFailure::UnsupportedCapability)?;
            let request = claim.clone();
            let controller = authority.clone();
            // This owned I/O boundary proves the exact newly reserved claim and
            // parks its writer without lending SQLite authority to the controller.
            // An idempotent reservation can reuse only this invocation's
            // unchanged unfinished-receipt proof. A new claim or fresh
            // invocation still requires its own complete checkpoint.
            dispatch
                .schedule_effect(move |_| controller.activate(request))
                .future
                .await??;
            let grant = ExecutionGrant {
                journal,
                claim,
                lifecycle: Some(Arc::new(lifecycle)),
                authority,
            };
            grant.check(&context)?;
            Ok(grant)
        })
    }
}
