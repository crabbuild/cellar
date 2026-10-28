// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Installation is prepared without changing routing, then committed by the
//! owning network driver after its acknowledged update. Old requests retain
//! their handler; new requests consult the latest committed table.

use crate::{Destination, DnsFamily, Host, HttpTarget};
use anyhow::{ensure, Result};
use std::sync::Arc;

pub const MAX_INTERCEPT_ENTRIES: usize = 128;

struct Entry<T> {
    target: HttpTarget,
    handler: Arc<T>,
}

impl<T> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            handler: self.handler.clone(),
        }
    }
}

pub struct Rules<T> {
    identity: Arc<()>,
    revision: u64,
    exhausted: bool,
    entries: Vec<Entry<T>>,
}

impl<T> Default for Rules<T> {
    fn default() -> Self {
        Self {
            identity: Arc::new(()),
            revision: 0,
            exhausted: false,
            entries: Vec::new(),
        }
    }
}

/// An uncommitted routing change. Dropping it leaves the active rules intact.
pub struct Prepared<T> {
    identity: Arc<()>,
    previous: u64,
    revision: u64,
    entries: Vec<Entry<T>>,
}

impl<T> Rules<T> {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn entry_count(&self) -> usize {
        self.entries
            .iter()
            .map(|entry| entry.target.entries())
            .sum()
    }

    pub fn prepare(&self, target: HttpTarget, handler: T) -> Result<Prepared<T>> {
        ensure!(!self.exhausted, "intercept revision exhausted");
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("intercept revision exhausted"))?;
        let mut entries = self.entries.clone();
        let handler = Arc::new(handler);
        if let Some(current) = entries.iter_mut().find(|entry| entry.target == target) {
            current.handler = handler;
        } else {
            ensure!(
                self.entry_count() + target.entries() <= MAX_INTERCEPT_ENTRIES,
                "You can't configure more than 128 egress interceptors"
            );
            entries.push(Entry { target, handler });
        }
        Ok(Prepared {
            identity: self.identity.clone(),
            previous: self.revision,
            revision,
            entries,
        })
    }

    pub fn commit(&mut self, prepared: Prepared<T>) -> Result<()> {
        ensure!(
            Arc::ptr_eq(&self.identity, &prepared.identity),
            "intercept belongs to another registry"
        );
        ensure!(
            !self.exhausted && self.revision == prepared.previous,
            "stale intercept installation"
        );
        self.entries = prepared.entries;
        self.revision = prepared.revision;
        Ok(())
    }

    pub fn resolve(&self, destination: &Destination<'_>) -> Option<Arc<T>> {
        self.entries
            .iter()
            .find(|entry| entry.target.matches(destination))
            .map(|entry| entry.handler.clone())
    }

    pub fn resolves(&self, host: &Host, family: DnsFamily) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.target.resolves(host, family))
    }

    /// Retirement invalidates prepared updates as well as routing. Never reset
    /// revision to zero when reusing a cell's registry for another run.
    pub fn retire(&mut self) -> Result<()> {
        self.entries.clear();
        if let Some(next) = self.revision.checked_add(1) {
            self.revision = next;
        } else {
            self.exhausted = true;
            anyhow::bail!("intercept revision exhausted");
        }
        Ok(())
    }
}

impl<T> Prepared<T> {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn targets(&self) -> impl Iterator<Item = &HttpTarget> {
        self.entries.iter().map(|entry| &entry.target)
    }
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
