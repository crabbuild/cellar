// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Foreground output progress without starving queued background publication.

use cellar_logic::cell_group::GroupHistory;
use std::collections::VecDeque;

const FOREGROUND_BURST: u8 = 2;

pub(super) enum Eligibility {
    Remove,
    Blocked,
    Background,
    Foreground,
}

#[derive(Default)]
pub(super) struct PublicationQueue {
    groups: VecDeque<GroupHistory>,
    foreground_turns: u8,
}

impl PublicationQueue {
    /// The manager owns the per-group queued flag and total group bound.
    pub(super) fn enqueue(&mut self, group: GroupHistory) {
        self.groups.push_back(group);
    }

    pub(super) fn remove(&mut self, group: GroupHistory) {
        self.groups.retain(|queued| *queued != group);
    }

    /// Keep FIFO order within each class. A blocked group consumes no turn;
    /// every launched foreground attempt counts, including failed attempts.
    /// Classification prunes expired/cancelled outputs before granting priority.
    pub(super) fn next(
        &mut self,
        mut classify: impl FnMut(GroupHistory) -> Eligibility,
    ) -> Option<GroupHistory> {
        let mut foreground = None;
        let mut background = None;
        self.groups.retain(|group| {
            match classify(*group) {
                Eligibility::Remove => return false,
                Eligibility::Blocked => (),
                Eligibility::Foreground => {
                    foreground.get_or_insert(*group);
                }
                Eligibility::Background => {
                    background.get_or_insert(*group);
                }
            }
            true
        });
        let selected = match (foreground, background) {
            (Some(_), Some(background)) if self.foreground_turns >= FOREGROUND_BURST => background,
            (Some(foreground), _) => foreground,
            (_, background) => background?,
        };
        let position = self
            .groups
            .iter()
            .position(|group| *group == selected)
            .expect("selected queued archive");
        let selected = self
            .groups
            .remove(position)
            .expect("selected queued archive");
        self.foreground_turns = if foreground == Some(selected) {
            self.foreground_turns
                .saturating_add(1)
                .min(FOREGROUND_BURST)
        } else {
            0
        };
        Some(selected)
    }
}

#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;
