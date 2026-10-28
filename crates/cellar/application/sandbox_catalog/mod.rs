// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable named allocation metadata for host-owned sandbox controllers.
pub(crate) mod values;
use super::{containers::ProviderAuthority, Context, HandlerFuture};
use crate::storage::sandbox_catalog as store;
use anyhow::{ensure, Result};
use rand::RngCore;
use std::sync::Arc;
pub use values::{
    SandboxAllocation, SandboxAllocationPage, SandboxAllocationState, SandboxParent,
    SandboxParentState, SandboxReservation,
};

/// Host-only parent catalog, bound to one original root-cell event. Reopen it in
/// each event; records survive restart and business `delete_all`. This catalog
/// does not create compute, fence child requests or schedule cleanup by itself.
#[derive(Clone)]
pub struct SandboxCatalog {
    authority: Arc<ProviderAuthority>,
}

impl SandboxCatalog {
    pub fn open(context: &Context) -> Result<Self> {
        let catalog = Self {
            authority: Arc::new(context.container_provider_authority()?),
        };
        catalog.check(context)?;
        Ok(catalog)
    }

    fn check<'a>(&self, context: &'a Context) -> Result<&'a str> {
        self.authority.check(context)?;
        ensure!(
            context
                .storage()?
                .0
                .resources
                .transaction
                .lock()
                .unwrap()
                .is_none(),
            "sandbox catalogs cannot be used inside storage callbacks"
        );
        Ok(&context.identity().unwrap().scope)
    }

    pub fn parent(&self, context: &Context) -> Result<Option<SandboxParent>> {
        store::parent(self.check(context)?)
    }

    pub fn get(&self, context: &Context, name: &str) -> Result<Option<SandboxAllocation>> {
        values::name(name)?;
        store::load(self.check(context)?, name)
    }

    pub fn list(
        &self,
        context: &Context,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SandboxAllocationPage> {
        values::page(after, limit)?;
        store::list(self.check(context)?, after, limit, false)
    }

    /// While active, list explicitly deleting children. After parent retirement
    /// begins, list every child lacking confirmed deletion. No full scan is
    /// materialized; every page is bounded to at most 256 records.
    pub fn cleanup(
        &self,
        context: &Context,
        after: Option<&str>,
        limit: usize,
    ) -> Result<SandboxAllocationPage> {
        values::page(after, limit)?;
        store::list(self.check(context)?, after, limit, true)
    }

    /// Reserve a named execution cell in a configured namespace. Equal names
    /// reuse the same generation and reject changed binding/parameters. Only
    /// confirmed deletion permits a new generation, with a different child ID.
    /// Await this future before issuing any child/provider effect.
    pub fn reserve(
        &self,
        context: &Context,
        name: &str,
        binding: &str,
        parameters_digest: &str,
    ) -> HandlerFuture<SandboxReservation> {
        let prepared: Result<_> = (|| {
            let scope = self.check(context)?;
            values::name(name)?;
            values::name(binding)?;
            ensure!(
                values::hex(parameters_digest, 64),
                "invalid sandbox parameters digest"
            );
            let namespace = context.namespace(binding)?;
            let mut incarnation = [0; 16];
            crate::asyncrt::rng("sandbox_parent_incarnation").fill_bytes(&mut incarnation);
            let (parent, reservation) = store::reserve(
                scope,
                &hex::encode(incarnation),
                name,
                binding,
                parameters_digest,
                &|name| namespace.id_from_name(name).scope(),
            )?;
            Ok((parent, reservation, context.storage()?.sync()))
        })();
        let catalog = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let (parent, reservation, flush) = prepared?;
            flush.await?;
            catalog.current(&context, &parent, Some(&reservation.allocation))?;
            Ok(reservation)
        })
    }

    /// Checkpoint a verified child acknowledgement. The host controller must
    /// correlate the original execution scope/generation; an empty inventory,
    /// elapsed timeout or lost response cannot establish this observation.
    pub fn confirm_ready(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
    ) -> HandlerFuture<SandboxAllocation> {
        self.transition(context, expected, SandboxAllocationState::Ready)
    }

    /// Persist exact-child deletion intent before submitting cleanup.
    pub fn begin_delete(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
    ) -> HandlerFuture<SandboxAllocation> {
        self.transition(context, expected, SandboxAllocationState::Deleting)
    }

    /// Record proven retirement of the exact child, after deletion intent is
    /// durable. A provider DELETE acknowledgement alone may not prove absence.
    pub fn confirm_deleted(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
    ) -> HandlerFuture<SandboxAllocation> {
        self.transition(context, expected, SandboxAllocationState::Deleted)
    }

    fn transition(
        &self,
        context: &Context,
        expected: &SandboxAllocation,
        state: SandboxAllocationState,
    ) -> HandlerFuture<SandboxAllocation> {
        let prepared: Result<_> = (|| {
            let scope = self.check(context)?;
            expected.validate()?;
            let (parent, allocation) = store::transition(scope, expected, state)?;
            Ok((parent, allocation, context.storage()?.sync()))
        })();
        let catalog = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let (parent, allocation, flush) = prepared?;
            flush.await?;
            catalog.current(&context, &parent, Some(&allocation))?;
            Ok(allocation)
        })
    }

    /// Atomically close future reservations and expose all remaining children
    /// to cleanup. It does not await provider deletion or erase any identities.
    pub fn begin_retirement(&self, context: &Context) -> HandlerFuture<SandboxParent> {
        self.retirement(context, false)
    }

    /// Finish only after every child has confirmed deletion. The retained parent
    /// tombstone cannot be reopened; physical cell removal needs host integration.
    pub fn finish_retirement(&self, context: &Context) -> HandlerFuture<SandboxParent> {
        self.retirement(context, true)
    }

    fn retirement(&self, context: &Context, finish: bool) -> HandlerFuture<SandboxParent> {
        let prepared: Result<_> = (|| {
            let scope = self.check(context)?;
            let mut incarnation = [0; 16];
            crate::asyncrt::rng("sandbox_parent_incarnation").fill_bytes(&mut incarnation);
            let parent = store::retire(scope, &hex::encode(incarnation), finish)?;
            Ok((parent, context.storage()?.sync()))
        })();
        let catalog = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let (parent, flush) = prepared?;
            flush.await?;
            catalog.current(&context, &parent, None)?;
            Ok(parent)
        })
    }

    fn current(
        &self,
        context: &Context,
        parent: &SandboxParent,
        allocation: Option<&SandboxAllocation>,
    ) -> Result<()> {
        ensure!(
            self.parent(context)?.as_ref() == Some(parent),
            "sandbox parent changed while awaiting durability; reconcile before effects"
        );
        if let Some(allocation) = allocation {
            ensure!(
                self.get(context, &allocation.name)?.as_ref() == Some(allocation),
                "sandbox allocation changed while awaiting durability; reconcile before effects"
            );
        }
        Ok(())
    }
}
