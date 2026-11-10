// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned earliest effect deadlines for parked voters.

use cellar_logic::cell_group::GroupHistory;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(super) struct ParkDeadlines {
    by_group: BTreeMap<GroupHistory, u64>,
    by_deadline: BTreeSet<(u64, GroupHistory)>,
}

impl ParkDeadlines {
    pub(super) fn set(&mut self, group: GroupHistory, deadline: Option<u64>) {
        self.remove(&group);
        if let Some(deadline) = deadline {
            self.by_group.insert(group, deadline);
            self.by_deadline.insert((deadline, group));
        }
    }

    pub(super) fn remove(&mut self, group: &GroupHistory) {
        if let Some(deadline) = self.by_group.remove(group) {
            self.by_deadline.remove(&(deadline, *group));
        }
    }

    pub(super) fn next(&self) -> Option<u64> {
        self.by_deadline.first().map(|(deadline, _)| *deadline)
    }

    pub(super) fn pop_due(&mut self, now: u64) -> Option<GroupHistory> {
        let &(deadline, group) = self.by_deadline.first()?;
        if deadline > now {
            return None;
        }
        self.remove(&group);
        Some(group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

    fn history(id: u8) -> GroupHistory {
        GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        }
    }

    #[test]
    fn park_deadlines_pop_only_due_voters_and_order_ties() {
        let mut deadlines = ParkDeadlines::default();
        deadlines.set(history(1), Some(12));
        deadlines.set(history(3), Some(9));
        deadlines.set(history(2), Some(9));
        assert_eq!(deadlines.next(), Some(9));
        assert_eq!(deadlines.pop_due(8), None);
        assert_eq!(deadlines.pop_due(9), Some(history(2)));
        assert_eq!(deadlines.pop_due(9), Some(history(3)));
        assert_eq!(deadlines.pop_due(9), None);
        assert_eq!(deadlines.pop_due(12), Some(history(1)));
        assert_eq!(deadlines.next(), None);
    }

    #[test]
    fn park_deadlines_replacement_and_early_wake_remove_old_entries() {
        let mut deadlines = ParkDeadlines::default();
        deadlines.set(history(1), Some(9));
        deadlines.set(history(1), Some(21));
        deadlines.set(history(2), Some(15));
        deadlines.remove(&history(2));
        deadlines.set(history(3), Some(19));
        deadlines.set(history(3), None);
        assert_eq!(deadlines.next(), Some(21));
        assert_eq!(deadlines.pop_due(20), None);
        assert_eq!(deadlines.pop_due(21), Some(history(1)));
        assert_eq!(deadlines.pop_due(u64::MAX), None);
    }
}
