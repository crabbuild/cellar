//! The host publishes bounded metadata once. Retained handles share the same
//! one-way member slot; a name is never replaced with another history.
use super::*;
use std::sync::RwLock;

pub(crate) struct Bindings {
    entries: RwLock<BTreeMap<(NamespaceId, String), Binding>>,
    limit: usize,
}

impl Bindings {
    /// Startup has already validated the static registration count.
    pub(crate) fn new(entries: BTreeMap<(NamespaceId, String), Binding>, limit: usize) -> Self {
        Self {
            entries: RwLock::new(entries),
            limit,
        }
    }

    pub(crate) fn get(&self, key: &(NamespaceId, String)) -> Result<Binding, CommandFailure> {
        self.entries
            .read()
            .map_err(|_| CommandFailure::CorruptState)?
            .get(key)
            .cloned()
            .ok_or(CommandFailure::Unavailable)
    }

    pub(crate) fn contains_scope(&self, scope: &GroupScope) -> Result<bool, CommandFailure> {
        Ok(self
            .entries
            .read()
            .map_err(|_| CommandFailure::CorruptState)?
            .get(&(scope.namespace, scope.cell.clone()))
            .is_some_and(|b| b.scope == *scope))
    }

    pub(crate) fn len(&self) -> Result<usize, CommandFailure> {
        Ok(self
            .entries
            .read()
            .map_err(|_| CommandFailure::CorruptState)?
            .len())
    }

    /// Only the serialized host calls this. Duplicate names always refuse;
    /// idempotency is checked against the actor's history before publication.
    pub(crate) fn insert(&self, binding: Binding) -> Result<(), CommandFailure> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| CommandFailure::CorruptState)?;
        let key = (binding.scope.namespace, binding.scope.cell.clone());
        if entries.contains_key(&key) {
            return Err(CommandFailure::CommandConflict);
        }
        if entries.len() >= self.limit {
            return Err(CommandFailure::Overloaded);
        }
        entries.insert(key, binding);
        Ok(())
    }
}
