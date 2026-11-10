// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Scoped active-owner index. Pages are advisory and never membership proofs.
use super::*;
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{ClusterId, NamespaceId},
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

#[cfg(test)]
#[path = "registration_inventory_tests.rs"]
mod tests;

pub(crate) struct PartitionGroupPage {
    pub revision: [u8; 32],
    pub groups: Vec<GroupHistory>,
    pub next: Option<GroupHistory>,
}

pub(super) struct Inventory {
    scopes: BTreeMap<(ClusterId, NamespaceId), Scope>,
    revision: u64,
}

struct Scope {
    revision: u64,
    groups: BTreeSet<GroupHistory>,
}

impl Inventory {
    pub(super) fn new(groups: impl Iterator<Item = GroupHistory>) -> Self {
        let mut inventory = Self {
            scopes: BTreeMap::new(),
            revision: 1,
        };
        for group in groups {
            inventory
                .scopes
                .entry((group.cluster, group.namespace))
                .or_insert_with(|| Scope {
                    revision: 1,
                    groups: BTreeSet::new(),
                })
                .groups
                .insert(group);
        }
        inventory
    }

    pub(super) fn insert(&mut self, group: GroupHistory) {
        let scope = self
            .scopes
            .entry((group.cluster, group.namespace))
            .or_insert_with(|| Scope {
                revision: 0,
                groups: BTreeSet::new(),
            });
        if scope.groups.insert(group) {
            self.revision = next_revision(self.revision);
            scope.revision = self.revision;
        }
    }

    pub(super) fn remove(&mut self, group: &GroupHistory) {
        let scope = (group.cluster, group.namespace);
        if let Some(groups) = self.scopes.get_mut(&scope) {
            if groups.groups.remove(group) {
                self.revision = next_revision(self.revision);
                groups.revision = self.revision;
                if groups.groups.is_empty() {
                    self.scopes.remove(&scope);
                }
            }
        }
    }

    pub(super) fn revision(
        &self,
        scopes: &[(ClusterId, NamespaceId)],
    ) -> Result<[u8; 32], RuntimeError> {
        if self.revision == 0 || scopes.len() > 64 {
            return Err(RuntimeError::Overloaded);
        }
        let mut hash = Sha256::new();
        hash.update(b"cellar-local-roster-inventory-1");
        for scope in scopes.iter().copied().collect::<BTreeSet<_>>() {
            hash.update(scope.0.bytes());
            hash.update(scope.1.bytes());
            hash.update(
                self.scopes
                    .get(&scope)
                    .map_or(0, |scope| scope.revision)
                    .to_be_bytes(),
            );
        }
        Ok(hash.finalize().into())
    }

    pub(super) fn page(
        &self,
        scopes: &[(ClusterId, NamespaceId)],
        revision: Option<[u8; 32]>,
        after: Option<GroupHistory>,
        capacity: usize,
        limit: usize,
    ) -> Result<PartitionGroupPage, RuntimeError> {
        if !(1..=128).contains(&capacity)
            || !(1..=16).contains(&limit)
            || limit > capacity
            || scopes.len() > 64
        {
            return Err(RuntimeError::Overloaded);
        }
        let current = self.revision(scopes)?;
        if revision.is_some_and(|revision| revision != current)
            || (after.is_some() && revision.is_none())
        {
            return Err(RuntimeError::NotReady);
        }
        let scopes: BTreeSet<_> = scopes.iter().copied().collect();
        // Namespace counts are indexed: no walk over foreign groups, parked
        // engines, cells or manifests. The complete configured bound still
        // refuses overflow, including on continuation pages.
        let total: usize = scopes
            .iter()
            .map(|scope| self.scopes.get(scope).map_or(0, |scope| scope.groups.len()))
            .sum();
        if total > capacity {
            return Err(RuntimeError::Overloaded);
        }
        if let Some(after) = after {
            let scope = (after.cluster, after.namespace);
            if !scopes.contains(&scope)
                || !self
                    .scopes
                    .get(&scope)
                    .is_some_and(|scope| scope.groups.contains(&after))
            {
                return Err(RuntimeError::NotReady);
            }
        }
        let mut selected = Vec::with_capacity(limit);
        for scope in scopes {
            if after.is_some_and(|after| scope < (after.cluster, after.namespace)) {
                continue;
            }
            let Some(groups) = self.scopes.get(&scope) else {
                continue;
            };
            let lower = after
                .filter(|after| scope == (after.cluster, after.namespace))
                .map_or(Unbounded, Excluded);
            for group in groups.groups.range((lower, Unbounded)) {
                if selected.len() == limit {
                    return Ok(PartitionGroupPage {
                        revision: current,
                        next: selected.last().copied(),
                        groups: selected,
                    });
                }
                selected.push(*group);
            }
        }
        Ok(PartitionGroupPage {
            revision: current,
            groups: selected,
            next: None,
        })
    }
}

fn next_revision(revision: u64) -> u64 {
    // Exhaustion permanently refuses advisory inventory. Empty scope entries
    // may be removed: a reactivated scope receives a fresh global generation.
    if revision == 0 {
        0
    } else {
        revision.checked_add(1).unwrap_or(0)
    }
}
