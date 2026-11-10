// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained history refuses new work; temporary reservations defer callbacks.
use crate::raft::{
    cache::{CachedGroup, GroupLog},
    storage::LogError,
};
use cellar_logic::consensus::LogIndex;
use std::sync::{Arc, RwLock};

#[cfg(test)]
#[path = "host_admission_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Usage {
    bytes: u64,
    entries: u64,
}

#[derive(Default)]
pub(super) struct Retained {
    observed: Option<Observation>,
}

struct Observation {
    cache: Arc<RwLock<CachedGroup>>,
    revision: u64,
    archived: LogIndex,
    usage: Usage,
}

impl Retained {
    fn observe(&mut self, group: &GroupLog, archived: LogIndex) -> Result<Usage, LogError> {
        let revision = group.cache_revision()?;
        if let Some(observed) = &self.observed {
            if Arc::ptr_eq(&observed.cache, &group.cache)
                && observed.revision == revision
                && observed.archived == archived
            {
                return Ok(observed.usage);
            }
        }
        let cache = group.read()?;
        let mut usage = Usage::default();
        if cache.applied.position.index > archived {
            if let Some(image) = &cache.applied.image {
                usage.bytes = image.length;
            }
        }
        for (_, entry) in cache
            .entries
            .range(archived.get().max(1).saturating_add(1)..)
        {
            usage.bytes = usage
                .bytes
                .saturating_add(u64::from(protobuf::Message::compute_size(entry)) + 128);
            if entry.get_entry_type() == ::raft::prelude::EntryType::EntryNormal
                && !entry.data.is_empty()
            {
                usage.entries = usage.entries.saturating_add(1);
            }
        }
        // A concurrent publication may give us a newer cache than `revision`.
        // Keeping the earlier revision is conservative: the next observation
        // recounts it. The strong cache identity also prevents address reuse
        // from matching an observation retained from a different group/open.
        self.observed = Some(Observation {
            cache: group.cache.clone(),
            revision,
            archived,
            usage,
        });
        Ok(usage)
    }
}
