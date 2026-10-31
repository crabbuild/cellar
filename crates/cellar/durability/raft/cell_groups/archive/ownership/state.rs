// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pure bounded transitions. All allocating mutations reconcile their exact
//! intent digest; retired revisions never reappear and generations never repeat.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    domain: String,
    identity: CellGroupArchiveCreation,
    pub revision: u64,
    pub pending: Vec<Pending>,
    pub holds: Vec<CellGroupArchiveHold>,
    pub selected: Option<Descriptor>,
}

impl State {
    pub fn empty(identity: CellGroupArchiveCreation) -> Self {
        Self {
            domain: STATE_DOMAIN.into(),
            identity,
            revision: 1,
            pending: Vec::new(),
            holds: Vec::new(),
            selected: None,
        }
    }

    pub fn check(&self, identity: &CellGroupArchiveCreation) -> Result<()> {
        if self.domain != STATE_DOMAIN
            || &self.identity != identity
            || self.revision == 0
            || self.revision == u64::MAX
        {
            return Err(CellGroupArchiveError::Invalid(
                "archive registry domain/revision",
            ));
        }
        if self.pending.len() > 8 || self.holds.len() > 64 {
            return Err(CellGroupArchiveError::Limit);
        }
        let mut ids = BTreeSet::new();
        let mut previous = 0;
        for pending in &self.pending {
            let t = pending.ticket;
            if t.registry != identity.id
                || t.generation < 2
                || t.generation <= previous
                || t.generation > self.revision
                || t.allocation == [0; 32]
                || !ids.insert(t.generation)
            {
                return Err(CellGroupArchiveError::Invalid("archive pending generation"));
            }
            previous = t.generation;
            if let Some(parent) = &pending.parent {
                parent.check(identity, t.generation)?;
            }
        }
        previous = 0;
        for hold in &self.holds {
            if hold.registry != identity.id
                || hold.id <= previous
                || hold.id > self.revision
                || hold.allocation == [0; 32]
                || !ids.insert(hold.id)
            {
                return Err(CellGroupArchiveError::Invalid(
                    "archive hold identity/order",
                ));
            }
            previous = hold.id;
            hold.root.check(identity, hold.id)?;
        }
        let mut generations = BTreeMap::new();
        for root in self.roots() {
            root.check(identity, self.revision)?;
            if ids.contains(&root.object.generation) {
                return Err(CellGroupArchiveError::Invalid(
                    "pending generation already bound",
                ));
            }
            if generations
                .insert(root.object.generation, root.clone())
                .is_some_and(|before| before != root)
            {
                return Err(CellGroupArchiveError::Invalid(
                    "archive generation descriptor conflict",
                ));
            }
            if self.selected.as_ref().is_none_or(|selected| {
                root.marker.position().index > selected.marker.position().index
            }) {
                return Err(CellGroupArchiveError::Invalid(
                    "archive retained root exceeds head",
                ));
            }
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<u64> {
        self.revision = self
            .revision
            .checked_add(1)
            .filter(|n| *n != u64::MAX)
            .ok_or(CellGroupArchiveError::Limit)?;
        Ok(self.revision)
    }

    pub fn reconcile(&self, intent: &CellGroupArchiveIntent) -> Result<Option<Allocation>> {
        let allocation = intent.digest()?;
        if intent.registry != self.identity.id {
            return Err(CellGroupArchiveError::Conflict);
        }
        if let Some(p) = self
            .pending
            .iter()
            .find(|p| p.ticket.generation == intent.revision && p.ticket.allocation == allocation)
        {
            return Ok(Some(Allocation::Upload(p.ticket)));
        }
        if let Some(h) = self
            .holds
            .iter()
            .find(|h| h.id == intent.revision && h.allocation == allocation)
        {
            return Ok(Some(Allocation::Hold(Box::new(h.clone()))));
        }
        Ok(None)
    }

    pub fn allocate(&mut self, intent: &CellGroupArchiveIntent) -> Result<Allocation> {
        if let Some(result) = self.reconcile(intent)? {
            return Ok(result);
        }
        if intent.revision <= self.revision {
            return Err(CellGroupArchiveError::Expired);
        }
        if self.revision.checked_add(1) != Some(intent.revision) {
            return Err(CellGroupArchiveError::Conflict);
        }
        let allocation = intent.digest()?;
        match &intent.action {
            Action::Reserve { parent } => {
                if parent != &self.selected {
                    return Err(CellGroupArchiveError::Conflict);
                }
                if self.pending.len() == 8 {
                    return Err(CellGroupArchiveError::Limit);
                }
                let ticket = CellGroupArchiveUploadTicket {
                    registry: self.identity.id,
                    generation: self.advance()?,
                    allocation,
                };
                self.pending.push(Pending {
                    ticket,
                    parent: parent.clone(),
                });
                Ok(Allocation::Upload(ticket))
            }
            Action::Acquire { root } => {
                if self.selected.as_ref() != Some(root) {
                    return Err(CellGroupArchiveError::Expired);
                }
                self.hold(root.clone(), allocation)
            }
            Action::Publish { ticket, root } => {
                if !self.pending.iter().any(|p| &p.ticket == ticket)
                    || root.object.generation != ticket.generation
                {
                    return Err(CellGroupArchiveError::Expired);
                }
                root.check(&self.identity, intent.revision)?;
                if let Some(selected) = &self.selected {
                    let before = selected.marker.position();
                    let after = root.marker.position();
                    if after.index < before.index
                        || after.term < before.term
                        || (after.index == before.index && root.marker != selected.marker)
                    {
                        return Err(CellGroupArchiveError::Conflict);
                    }
                }
                let hold = self.hold(root.clone(), allocation)?;
                self.pending.retain(|p| &p.ticket != ticket);
                self.selected = Some(root.clone());
                Ok(hold)
            }
        }
    }

    fn hold(&mut self, root: Descriptor, allocation: [u8; 32]) -> Result<Allocation> {
        if self.holds.len() == 64 {
            return Err(CellGroupArchiveError::Limit);
        }
        let hold = CellGroupArchiveHold {
            registry: self.identity.id,
            id: self.advance()?,
            allocation,
            root,
        };
        self.holds.push(hold.clone());
        Ok(Allocation::Hold(Box::new(hold)))
    }

    pub fn retire(
        &mut self,
        hold: Option<&CellGroupArchiveHold>,
        ticket: Option<&CellGroupArchiveUploadTicket>,
    ) -> Result<bool> {
        let id = match (hold, ticket) {
            (Some(h), None) => h.id,
            (None, Some(t)) => t.generation,
            _ => return Err(CellGroupArchiveError::Invalid("archive retirement purpose")),
        };
        if id < 2 || id > self.revision {
            return Err(CellGroupArchiveError::Conflict);
        }
        if hold.is_some_and(|h| self.holds.iter().any(|p| p.id == h.id && p != h))
            || ticket.is_some_and(|t| {
                self.pending
                    .iter()
                    .any(|p| p.ticket.generation == t.generation && &p.ticket != t)
            })
        {
            return Err(CellGroupArchiveError::Conflict);
        }
        let before = self.pending.len() + self.holds.len();
        if let Some(hold) = hold {
            self.holds.retain(|h| h != hold);
        }
        if let Some(ticket) = ticket {
            self.pending.retain(|p| &p.ticket != ticket);
        }
        if before == self.pending.len() + self.holds.len() {
            return Ok(false);
        }
        self.advance()?;
        Ok(true)
    }

    pub fn roots(&self) -> Vec<Descriptor> {
        let mut roots = Vec::new();
        for root in self
            .selected
            .iter()
            .chain(self.holds.iter().map(|h| &h.root))
            .chain(self.pending.iter().filter_map(|p| p.parent.as_ref()))
        {
            if !roots.contains(root) {
                roots.push(root.clone());
            }
        }
        roots
    }
}
