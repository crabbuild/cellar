// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn group(number: u8) -> GroupHistory {
    GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([1; 16]).unwrap(),
        group: GroupId::new([number; 16]).unwrap(),
        incarnation: IncarnationId::new([1; 16]).unwrap(),
    }
}

#[test]
fn foreground_precedes_older_background_without_reordering_its_peers() {
    let mut queue = PublicationQueue::default();
    for number in 1..=5 {
        queue.enqueue(group(number));
    }
    let classify = |selected| {
        if selected == group(3) || selected == group(5) {
            Eligibility::Foreground
        } else {
            Eligibility::Background
        }
    };
    for number in [3, 5, 1, 2, 4] {
        assert_eq!(queue.next(classify), Some(group(number)));
    }
    assert_eq!(queue.next(classify), None);
}

#[test]
fn continuous_foreground_load_preserves_bounded_background_progress() {
    let mut queue = PublicationQueue::default();
    for number in 1..=5 {
        queue.enqueue(group(number));
    }
    let mut foreground_run = 0;
    let mut backgrounds = Vec::new();
    for _ in 0..180 {
        let next = queue
            .next(|selected| {
                if selected == group(1) || selected == group(2) {
                    Eligibility::Background
                } else {
                    Eligibility::Foreground
                }
            })
            .unwrap();
        if next == group(1) || next == group(2) {
            backgrounds.push(next);
            foreground_run = 0;
        } else {
            foreground_run += 1;
            assert!(foreground_run <= 2, "ready background archive starved");
        }
        // Both classes remain continuously ready, including new foreground work.
        queue.enqueue(next);
    }
    assert_eq!(backgrounds.len(), 60);
    for (index, selected) in backgrounds.into_iter().enumerate() {
        assert_eq!(selected, group(1 + (index % 2) as u8));
    }
}

#[test]
fn blocked_and_removed_groups_neither_consume_turns_nor_reorder_ready_work() {
    let mut queue = PublicationQueue::default();
    for number in 1..=5 {
        queue.enqueue(group(number));
    }
    assert_eq!(
        queue.next(|selected| match selected {
            value if value == group(1) => Eligibility::Remove,
            value if value == group(2) => Eligibility::Blocked,
            value if value == group(4) => Eligibility::Foreground,
            _ => Eligibility::Background,
        }),
        Some(group(4))
    );
    queue.remove(group(5));
    assert_eq!(
        queue.next(|selected| if selected == group(2) {
            Eligibility::Blocked
        } else {
            Eligibility::Background
        }),
        Some(group(3))
    );
    assert_eq!(queue.next(|_| Eligibility::Blocked), None);
    assert_eq!(queue.next(|_| Eligibility::Foreground), Some(group(2)));
    assert_eq!(queue.next(|_| Eligibility::Foreground), None);
}
