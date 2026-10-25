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
    /// Capture authority after validating the current native turn.
    pub fn invocation_authority(&self) -> Result<InvocationAuthority> {
        self.check_turn()?;
        Ok(InvocationAuthority {
            context: self.clone(),
        })
    }
}

impl InvocationAuthority {
    /// Refuse another event, another facet, cancellation or expired ownership.
    pub fn check(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        self.context.check_turn()?;
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
