// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded scheduling hints, never archive coverage or serving authority.

use crate::raft::completion::RuntimeError;
use cellar_logic::{cell_group::GroupHistory, consensus::LogIndex};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

struct Window {
    deadline: u64,
    users: usize,
    deferring: bool,
    progressed: bool,
}

struct Group {
    identity: Arc<()>,
    window: Option<Window>,
}

pub(in crate::raft::cell_groups::controller) struct Foregrounds {
    groups: Mutex<BTreeMap<GroupHistory, Group>>,
    max_groups: usize,
}

pub(crate) struct Invocation {
    owner: Weak<Foregrounds>,
    group: GroupHistory,
    entered: LogIndex,
    identity: Arc<()>,
}

impl Foregrounds {
    pub(in crate::raft::cell_groups::controller) fn new(max_groups: usize) -> Arc<Self> {
        Arc::new(Self {
            groups: Mutex::new(BTreeMap::new()),
            max_groups,
        })
    }

    pub(super) fn register(&self, group: GroupHistory) -> Result<(), RuntimeError> {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        if groups.contains_key(&group) {
            return Err(RuntimeError::Invalid(
                "duplicate S3 foreground group".into(),
            ));
        }
        if groups.len() >= self.max_groups {
            return Err(RuntimeError::Overloaded);
        }
        groups.insert(
            group,
            Group {
                identity: Arc::new(()),
                window: None,
            },
        );
        Ok(())
    }

    pub(super) fn admit(
        self: &Arc<Self>,
        group: GroupHistory,
        deadline: u64,
        now: u64,
        entered: LogIndex,
    ) -> Result<Invocation, RuntimeError> {
        if deadline <= now {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        let registered = groups.get_mut(&group).ok_or(RuntimeError::NotReady)?;
        let window = registered.window.get_or_insert(Window {
            deadline,
            users: 0,
            deferring: true,
            progressed: false,
        });
        let users = window
            .users
            .checked_add(1)
            .ok_or(RuntimeError::Overloaded)?;
        // A later arrival may shorten this cohort, never extend it.
        window.deadline = window.deadline.min(deadline);
        window.users = users;
        Ok(Invocation {
            owner: Arc::downgrade(self),
            group,
            entered,
            identity: registered.identity.clone(),
        })
    }

    pub(super) fn blocks(&self, group: GroupHistory, now: u64) -> bool {
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&group)
            .and_then(|g| g.window.as_ref())
            .is_some_and(|w| w.deferring && w.users != 0 && now < w.deadline)
    }

    pub(super) fn background_started(&self, group: GroupHistory) {
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(registered) = groups.get_mut(&group) {
            if let Some(window) = &mut registered.window {
                // Expiry opens the background lane for the rest of this cohort.
                // Overlapping/replaying callers cannot immediately close it.
                window.deferring = false;
                if window.users == 0 {
                    registered.window = None;
                }
            }
        }
    }

    pub(super) fn forget(&self, group: GroupHistory) {
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&group);
    }

    pub(super) fn clear(&self) {
        self.groups
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

impl Invocation {
    /// Only the normal final S3 output and authority gates call this. Replaying
    /// a receipt at/before admission grants no new group progress and cannot
    /// renew a deferral window under continuous retry traffic.
    pub(crate) fn completed(&self, index: LogIndex) {
        if index <= self.entered {
            return;
        }
        if let Some(owner) = self.owner.upgrade() {
            let mut groups = owner.groups.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(registered) = groups.get_mut(&self.group) {
                if Arc::ptr_eq(&registered.identity, &self.identity) {
                    if let Some(window) = &mut registered.window {
                        window.progressed = true;
                    }
                }
            }
        }
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let mut groups = owner.groups.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(registered) = groups.get_mut(&self.group) {
            // Retirement/return may install this same logical history again.
            // An obsolete guard cannot release or bless the new registration.
            if !Arc::ptr_eq(&registered.identity, &self.identity) {
                return;
            }
            if let Some(window) = &mut registered.window {
                window.users -= 1;
                if window.users == 0 && (window.progressed || !window.deferring) {
                    registered.window = None;
                }
                // Keep a failed empty cohort's original deadline until a
                // background attempt starts. New failures cannot extend it.
                // The existing shared controller timer observes this release.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

    fn group(n: u8) -> GroupHistory {
        GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([n; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        }
    }

    fn index(n: u64) -> LogIndex {
        LogIndex::new(n)
    }

    fn owner(capacity: usize, registered: &[u8]) -> Arc<Foregrounds> {
        let owner = Foregrounds::new(capacity);
        for n in registered {
            owner.register(group(*n)).unwrap();
        }
        owner
    }

    #[test]
    fn overlapping_calls_cannot_extend_the_first_deadline() {
        let owner = owner(2, &[3, 4]);
        let a = owner.admit(group(3), 50, 10, index(1)).unwrap();
        let b = owner.admit(group(3), 100, 20, index(1)).unwrap();
        assert!(owner.blocks(group(3), 49));
        assert!(!owner.blocks(group(3), 50));
        owner.background_started(group(3));
        let c = owner.admit(group(3), 200, 60, index(2)).unwrap();
        assert!(!owner.blocks(group(3), 61));
        drop((a, b, c));
        assert!(owner.groups.lock().unwrap()[&group(3)].window.is_none());
    }

    #[test]
    fn failed_calls_and_old_receipts_do_not_renew_a_window() {
        let owner = owner(1, &[3]);
        drop(owner.admit(group(3), 50, 10, index(5)).unwrap());
        let replay = owner.admit(group(3), 100, 20, index(5)).unwrap();
        replay.completed(index(5));
        drop(replay);
        let later = owner.admit(group(3), 200, 60, index(6)).unwrap();
        assert!(!owner.blocks(group(3), 61));
        drop(later);
        owner.background_started(group(3));
        assert!(owner.groups.lock().unwrap()[&group(3)].window.is_none());
    }

    #[test]
    fn fresh_progress_finishes_an_idle_cohort() {
        let owner = owner(1, &[3]);
        let a = owner.admit(group(3), 50, 10, index(5)).unwrap();
        a.completed(index(6));
        drop(a);
        let b = owner.admit(group(3), 100, 40, index(6)).unwrap();
        assert!(owner.blocks(group(3), 60));
        b.completed(index(7));
        drop(b);
        assert!(owner.groups.lock().unwrap()[&group(3)].window.is_none());
    }

    #[test]
    fn progress_does_not_hide_an_overlapping_deadline() {
        let owner = owner(1, &[3]);
        let a = owner.admit(group(3), 50, 10, index(5)).unwrap();
        let b = owner.admit(group(3), 100, 20, index(5)).unwrap();
        a.completed(index(6));
        drop(a);
        assert!(!owner.blocks(group(3), 50));
        drop(b);
    }

    #[test]
    fn groups_are_independent_and_registration_is_bounded() {
        let owner = owner(1, &[3]);
        let a = owner.admit(group(3), 50, 10, index(1)).unwrap();
        assert!(!owner.blocks(group(4), 10));
        assert!(matches!(
            owner.register(group(4)),
            Err(RuntimeError::Overloaded)
        ));
        owner.forget(group(3));
        owner.register(group(4)).unwrap();
        let b = owner.admit(group(4), 50, 10, index(1)).unwrap();
        drop(a);
        assert!(owner.blocks(group(4), 11));
        drop(b);
        owner.clear();
        assert!(owner.groups.lock().unwrap().is_empty());
    }

    #[test]
    fn expired_or_unregistered_calls_refuse_and_guards_do_not_own_the_host() {
        let owner = owner(1, &[3]);
        assert!(matches!(
            owner.admit(group(3), 10, 10, index(1)),
            Err(RuntimeError::DeadlineExceeded)
        ));
        assert!(matches!(
            owner.admit(group(4), 50, 10, index(1)),
            Err(RuntimeError::NotReady)
        ));
        assert!(owner.groups.lock().unwrap()[&group(3)].window.is_none());
        let hint = owner.admit(group(3), 20, 10, index(1)).unwrap();
        let weak = Arc::downgrade(&owner);
        drop(owner);
        assert!(weak.upgrade().is_none());
        drop(hint);
    }

    #[test]
    fn an_old_guard_cannot_modify_a_returned_groups_new_window() {
        let owner = owner(1, &[3]);
        let old = owner.admit(group(3), 50, 10, index(1)).unwrap();
        owner.forget(group(3));
        owner.register(group(3)).unwrap();
        let current = owner.admit(group(3), 100, 20, index(2)).unwrap();
        old.completed(index(99));
        drop(old);
        assert!(owner.blocks(group(3), 60));
        let groups = owner.groups.lock().unwrap();
        let window = groups[&group(3)].window.as_ref().unwrap();
        assert_eq!(window.users, 1);
        assert!(!window.progressed);
        drop(groups);
        current.completed(index(3));
        drop(current);
        assert!(owner.groups.lock().unwrap()[&group(3)].window.is_none());
    }
}
