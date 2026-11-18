// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::LogIndex, consensus::Term};

fn genesis() -> (HardState, ConfState, LogPosition) {
    (
        HardState {
            term: 1,
            commit: 1,
            ..Default::default()
        },
        ConfState {
            voters: vec![7, 3, 5],
            ..Default::default()
        },
        LogPosition {
            index: LogIndex::new(1),
            term: Term::new(1),
        },
    )
}

#[test]
fn genesis_selects_one_minimum_voter_independent_of_order() {
    let (hard, mut voters, applied) = genesis();
    for order in [vec![7, 3, 5], vec![3, 5, 7], vec![5, 7, 3]] {
        voters.voters = order;
        assert_eq!(
            candidate(&hard, &voters, applied),
            Some(MemberId::new(3).unwrap())
        );
    }
    voters.voters.clear();
    voters.learners = vec![3, 5, 7];
    assert_eq!(candidate(&hard, &voters, applied), None);
}

#[test]
fn unvoted_later_or_incomplete_cuts_cannot_select_a_startup_candidate() {
    let (hard, voters, applied) = genesis();
    for (term, commit, index, applied_term) in [
        (0, 1, 1, 1),
        (2, 1, 1, 1),
        (1, 0, 1, 1),
        (1, 2, 1, 1),
        (1, 1, 2, 1),
        (1, 1, 1, 2),
    ] {
        let mut changed = hard.clone();
        changed.term = term;
        changed.commit = commit;
        let changed_applied = LogPosition {
            index: LogIndex::new(index),
            term: Term::new(applied_term),
        };
        assert_eq!(candidate(&changed, &voters, changed_applied), None);
    }
    let mut joint = voters.clone();
    joint.voters_outgoing = vec![3, 5, 7];
    assert_eq!(candidate(&hard, &joint, applied), None);
}

#[test]
fn retained_vote_must_still_name_a_current_or_outgoing_voter() {
    let (mut hard, mut voters, _) = genesis();
    hard.term = 7;
    hard.commit = 44;
    hard.vote = 5;
    let applied = LogPosition {
        index: LogIndex::new(44),
        term: Term::new(7),
    };
    assert_eq!(
        candidate(&hard, &voters, applied),
        Some(MemberId::new(5).unwrap())
    );
    voters.voters = vec![3, 7];
    voters.voters_outgoing = vec![5];
    assert_eq!(
        candidate(&hard, &voters, applied),
        Some(MemberId::new(5).unwrap())
    );
    hard.vote = 11;
    assert_eq!(candidate(&hard, &voters, applied), None);
    hard.term = 0;
    hard.vote = 5;
    assert_eq!(candidate(&hard, &voters, applied), None);
}
