// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pure transitions of one persistent remote-root registry. Conditional storage
//! serializes these transitions; this module supplies neither data verification
//! nor native installation authority.

use super::{
    intent::Operation, ArchiveError, RootAllocation, RootDescriptor, RootIntent, RootReference,
    RootReservation,
};
use crate::raft::manifest::Position;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(super) const MAX_REFERENCES: usize = 64;
pub(super) const MAX_RESERVATIONS: usize = 8;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pending {
    pub ticket: RootReservation,
    pub parent: Option<RootDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Selected {
    pub root: RootDescriptor,
    pub position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub revision: u64,
    pub pending: Vec<Pending>,
    pub references: Vec<RootReference>,
    pub selected: Option<Selected>,
}

impl State {
    pub fn empty() -> Self {
        Self {
            revision: 1,
            pending: Vec::new(),
            references: Vec::new(),
            selected: None,
        }
    }

    pub fn validate(&self, registry: [u8; 32]) -> Result<(), ArchiveError> {
        if self.revision == 0 || self.revision == u64::MAX {
            return Err(ArchiveError::Corrupt("root registry revision"));
        }
        if self.pending.len() > MAX_RESERVATIONS || self.references.len() > MAX_REFERENCES {
            return Err(ArchiveError::Limit);
        }
        let mut ids = BTreeMap::new();
        let mut roots = BTreeMap::new();
        let mut previous = 0;
        for pending in &self.pending {
            let ticket = pending.ticket;
            if ticket.id < 2
                || ticket.registry != registry
                || ticket.id <= previous
                || ticket.id > self.revision
                || ticket.image == [0; 32]
                || ticket.allocation == [0; 32]
                || ids.insert(ticket.id, ()).is_some()
            {
                return Err(ArchiveError::Corrupt("root reservation identity/order"));
            }
            previous = ticket.id;
            if let Some(parent) = pending.parent {
                if parent.generation >= ticket.id {
                    return Err(ArchiveError::Corrupt("root reservation ancestry"));
                }
                insert_root(&mut roots, parent, self.revision)?;
            }
        }
        previous = 0;
        for reference in &self.references {
            if reference.registry != registry
                || reference.id <= previous
                || reference.id > self.revision
                || reference.root.generation >= reference.id
                || reference.allocation == [0; 32]
                || ids.insert(reference.id, ()).is_some()
            {
                return Err(ArchiveError::Corrupt("root reference identity/order"));
            }
            previous = reference.id;
            insert_root(&mut roots, reference.root, self.revision)?;
        }
        if let Some(selected) = &self.selected {
            selected.position.decode()?;
            insert_root(&mut roots, selected.root, self.revision)?;
        }
        if self
            .pending
            .iter()
            .any(|entry| roots.contains_key(&entry.ticket.id))
        {
            return Err(ArchiveError::Corrupt("reserved generation already bound"));
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<u64, ArchiveError> {
        self.revision = self
            .revision
            .checked_add(1)
            .filter(|next| *next != u64::MAX)
            .ok_or(ArchiveError::Limit)?;
        Ok(self.revision)
    }

    pub fn referenced(&self, reference: RootReference) -> Result<RootDescriptor, ArchiveError> {
        self.references
            .iter()
            .find(|candidate| **candidate == reference)
            .map(|entry| entry.root)
            .ok_or(ArchiveError::ReferenceExpired)
    }

    pub fn reserved(&self, ticket: RootReservation) -> Result<&Pending, ArchiveError> {
        self.pending
            .iter()
            .find(|entry| entry.ticket == ticket)
            .ok_or(ArchiveError::ReferenceExpired)
    }

    pub fn allocate(
        &mut self,
        intent: &RootIntent,
        allocation: [u8; 32],
    ) -> Result<RootAllocation, ArchiveError> {
        // Exact live results reconcile retries even if subsequent unrelated
        // transitions have advanced the revision. Removed IDs never reappear.
        for pending in &self.pending {
            if pending.ticket.id == intent.revision && pending.ticket.allocation == allocation {
                return Ok(RootAllocation::Reservation(pending.ticket));
            }
        }
        for reference in &self.references {
            if reference.id == intent.revision && reference.allocation == allocation {
                return Ok(RootAllocation::Reference(*reference));
            }
        }
        if intent.revision <= self.revision {
            return Err(ArchiveError::ReferenceExpired);
        }
        if self.revision.checked_add(1) != Some(intent.revision) {
            return Err(ArchiveError::Conflict);
        }
        let registry = intent.registry.0;
        match intent.operation {
            Operation::Reserve { image, parent } => {
                if image == [0; 32] {
                    return Err(ArchiveError::Invalid("empty reserved image root"));
                }
                if self.pending.len() == MAX_RESERVATIONS {
                    return Err(ArchiveError::Limit);
                }
                let parent = parent.map(|parent| self.referenced(parent)).transpose()?;
                let ticket = RootReservation {
                    registry,
                    id: self.advance()?,
                    image,
                    allocation,
                };
                self.pending.push(Pending { ticket, parent });
                Ok(RootAllocation::Reservation(ticket))
            }
            Operation::Bind {
                ticket,
                root,
                select,
            } => {
                self.reserved(ticket)?;
                root.validate()?;
                if root.generation != ticket.id || root.image != ticket.image {
                    return Err(ArchiveError::Invalid("root differs from reservation"));
                }
                if self.references.len() == MAX_REFERENCES {
                    return Err(ArchiveError::Limit);
                }
                if let Some(position) = select {
                    self.can_select(root, position)?;
                }
                let reference = RootReference {
                    registry,
                    id: self.advance()?,
                    root,
                    allocation,
                };
                self.pending.retain(|entry| entry.ticket != ticket);
                self.references.push(reference);
                if let Some(position) = select {
                    self.selected = Some(Selected { root, position });
                }
                Ok(RootAllocation::Reference(reference))
            }
            Operation::Clone { reference } => {
                let root = self.referenced(reference)?;
                self.add_reference(registry, root, allocation)
            }
            Operation::Acquire { expected } => {
                if self
                    .selected
                    .as_ref()
                    .is_none_or(|selected| selected.root != expected)
                {
                    return Err(ArchiveError::ReferenceExpired);
                }
                self.add_reference(registry, expected, allocation)
            }
        }
    }

    fn add_reference(
        &mut self,
        registry: [u8; 32],
        root: RootDescriptor,
        allocation: [u8; 32],
    ) -> Result<RootAllocation, ArchiveError> {
        if self.references.len() == MAX_REFERENCES {
            return Err(ArchiveError::Limit);
        }
        let reference = RootReference {
            registry,
            id: self.advance()?,
            root,
            allocation,
        };
        self.references.push(reference);
        Ok(RootAllocation::Reference(reference))
    }

    pub fn release(&mut self, reference: RootReference) -> Result<bool, ArchiveError> {
        let Some(found) = self
            .references
            .iter()
            .find(|entry| entry.id == reference.id)
        else {
            return Ok(false);
        };
        if *found != reference {
            return Err(ArchiveError::Conflict);
        }
        self.advance()?;
        self.references.retain(|entry| *entry != reference);
        Ok(true)
    }

    pub fn abort(&mut self, ticket: RootReservation) -> Result<bool, ArchiveError> {
        let Some(found) = self
            .pending
            .iter()
            .find(|entry| entry.ticket.id == ticket.id)
        else {
            return Ok(false);
        };
        if found.ticket != ticket {
            return Err(ArchiveError::Conflict);
        }
        self.advance()?;
        self.pending.retain(|entry| entry.ticket != ticket);
        Ok(true)
    }

    pub fn select(
        &mut self,
        reference: RootReference,
        position: Position,
    ) -> Result<(), ArchiveError> {
        let root = self.referenced(reference)?;
        self.can_select(root, position)?;
        self.advance()?;
        self.selected = Some(Selected { root, position });
        Ok(())
    }

    fn can_select(&self, root: RootDescriptor, position: Position) -> Result<(), ArchiveError> {
        position.decode()?;
        if self.selected.as_ref().is_some_and(|before| {
            position.index < before.position.index
                || position.term < before.position.term
                || position.index == before.position.index
                    && (position != before.position || root != before.root)
        }) {
            return Err(ArchiveError::Conflict);
        }
        Ok(())
    }

    pub fn roots(&self) -> Vec<RootDescriptor> {
        self.references
            .iter()
            .map(|entry| entry.root)
            .chain(self.selected.iter().map(|entry| entry.root))
            .chain(self.pending.iter().filter_map(|entry| entry.parent))
            .map(|root| (root.generation, root))
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect()
    }
}

fn insert_root(
    roots: &mut BTreeMap<u64, RootDescriptor>,
    root: RootDescriptor,
    revision: u64,
) -> Result<(), ArchiveError> {
    root.validate()?;
    if root.generation > revision
        || roots
            .insert(root.generation, root)
            .is_some_and(|before| before != root)
    {
        return Err(ArchiveError::Corrupt("root generation identity"));
    }
    Ok(())
}
