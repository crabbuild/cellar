// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Only reads queued before initiation share a fresh correlated barrier.
use super::*;
use cellar_logic::consensus::LogIndex;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Identity {
    pub history: GroupHistory,
    pub term: Term,
    pub configuration: u64,
}

struct Cohort {
    identity: Identity,
    members: usize,
    initiated: bool,
    term: Option<Term>,
    barrier: Option<LogIndex>,
    retry_at: u64,
}

/// Every member already holds its original node/group read reservation. That
/// envelope covers the shared state and index; this adds no admission bank.
pub(super) struct Cohorts {
    entries: BTreeMap<Vec<u8>, Cohort>,
    open: BTreeMap<GroupHistory, Vec<u8>>,
    members: usize,
    maximum: usize,
}

impl Cohorts {
    pub fn new(maximum: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            open: BTreeMap::new(),
            members: 0,
            maximum,
        }
    }

    pub fn join(
        &mut self,
        identity: Identity,
        proposed: Vec<u8>,
        now: u64,
    ) -> Result<(Vec<u8>, bool), RuntimeError> {
        if self.members >= self.maximum {
            return Err(RuntimeError::Overloaded);
        }
        if let Some(context) = self.open.get(&identity.history) {
            let cohort = self.entries.get_mut(context).expect("open read cohort");
            if !cohort.initiated && cohort.identity == identity {
                cohort.members += 1;
                self.members += 1;
                return Ok((context.clone(), true));
            }
        }
        if self.entries.contains_key(&proposed) {
            return Err(RuntimeError::Protocol("reused read cohort context".into()));
        }
        self.open.insert(identity.history, proposed.clone());
        self.entries.insert(
            proposed.clone(),
            Cohort {
                identity,
                members: 1,
                initiated: false,
                term: None,
                barrier: None,
                retry_at: now.saturating_add(super::reads::FORWARDED_RETRY_MS),
            },
        );
        self.members += 1;
        Ok((proposed, false))
    }

    pub fn progress<T>(
        &mut self,
        context: &[u8],
        now: u64,
        select: impl FnOnce(Identity, &mut Option<Term>, &mut Option<LogIndex>, bool) -> T,
    ) -> (T, bool) {
        let cohort = self.entries.get_mut(context).expect("pending read cohort");
        // Seal before entering the owner, even if readiness postpones the
        // physical request. A later arrival can never join an initiated barrier.
        let sealed = !cohort.initiated;
        cohort.initiated = true;
        if self
            .open
            .get(&cohort.identity.history)
            .is_some_and(|open| open.as_slice() == context)
        {
            self.open.remove(&cohort.identity.history);
        }
        let before = (cohort.term, cohort.barrier);
        let retry = cohort.barrier.is_none() && now >= cohort.retry_at;
        if retry {
            cohort.retry_at = now.saturating_add(super::reads::FORWARDED_RETRY_MS);
        }
        let result = select(
            cohort.identity,
            &mut cohort.term,
            &mut cohort.barrier,
            retry,
        );
        (
            result,
            sealed || retry || before != (cohort.term, cohort.barrier),
        )
    }

    pub fn observe(
        &mut self,
        history: GroupHistory,
        term: Term,
        index: LogIndex,
        context: &[u8],
    ) -> bool {
        let Some(cohort) = self.entries.get_mut(context) else {
            return false;
        };
        if cohort.identity.history == history
            && cohort.identity.term == term
            && cohort.term == Some(term)
        {
            cohort.barrier = Some(cohort.barrier.map_or(index, |seen| seen.max(index)));
        }
        true
    }

    pub fn deadline(&self, context: &[u8], deadline: u64) -> u64 {
        let cohort = self.entries.get(context).expect("pending read cohort");
        if cohort.barrier.is_none() {
            deadline.min(cohort.retry_at)
        } else {
            deadline
        }
    }

    pub fn leave(&mut self, context: &[u8]) {
        let cohort = self.entries.get_mut(context).expect("pending read cohort");
        cohort.members -= 1;
        self.members -= 1;
        if cohort.members == 0 {
            if self
                .open
                .get(&cohort.identity.history)
                .is_some_and(|open| open.as_slice() == context)
            {
                self.open.remove(&cohort.identity.history);
            }
            self.entries.remove(context);
        }
    }
}

#[cfg(test)]
#[path = "read_cohort_tests.rs"]
mod tests;
