// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact native invocation authority for adapter-owned resources.
use super::Context;
use anyhow::{ensure, Result};
use std::sync::Arc;

/// Bind adapter resources to an executing invocation, including its facet.
/// This is not activation authority: a later event of the same cell needs its
/// own resources. Checking it never replaces a service's remote fencing proof.
pub struct InvocationAuthority {
    context: Context,
}

impl Context {
    /// Capture authority after validating the current native or Raft invocation.
    pub fn invocation_authority(&self) -> Result<InvocationAuthority> {
        self.check_adapter_turn()?;
        Ok(InvocationAuthority {
            context: self.clone(),
        })
    }

    fn check_adapter_turn(&self) -> Result<()> {
        let Some(authority) = &self.raft_storage else {
            return self.check_turn();
        };
        // Constructor Storage may enter later commands. An adapter authority
        // must instead retain the exact invocation that admitted its work.
        ensure!(
            authority.invocation.is_some(),
            super::consensus::CommandFailure::UnsupportedCapability
        );
        ensure!(
            !self
                .state
                .retired
                .load(std::sync::atomic::Ordering::Acquire),
            "adapter invocation has retired"
        );
        crate::storage::require_raft_dispatch_storage(authority)?;
        // Dispatch retains the original domain, deadline and activation fence
        // across checkpoints and parked-I/O polls.
        crate::storage::capture_raft_dispatch()?
            .ok_or(super::consensus::CommandFailure::UnsupportedCapability)?
            .check_current()
    }
}

impl InvocationAuthority {
    /// Refuse another event, another facet, cancellation or expired ownership.
    pub fn check(&self, context: &Context) -> Result<()> {
        context.check_adapter_turn()?;
        self.context.check_adapter_turn()?;
        let same_facet = match (&self.context.facet, &context.facet) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        ensure!(
            Arc::ptr_eq(&self.context.io, &context.io) && same_facet,
            "adapter resource belongs to another invocation or facet"
        );
        Ok(())
    }
}
