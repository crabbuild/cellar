// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::Timer;

#[tokio::test]
async fn far_future_timer_is_bounded_without_firing_early_or_reviving_displaced_arms() {
    let mut delays = DelayQueue::new();
    let mut slots = TimerSlots::default();
    let timer = Timer::CellAlarm {
        cell: "future".into(),
        generation: 1,
    };
    let (arm, _) = TimerSlots::default().arm(timer.clone(), u64::MAX, ());
    assert_eq!(
        timer_delay(&arm, 0).as_millis(),
        u128::from(TIMER_CHECKPOINT_MS)
    );
    let key = delays.insert(arm.clone(), timer_delay(&arm, 0));
    slots.install(&arm, key);
    // Simulate a checkpoint being removed from DelayQueue's wheel.
    delays.remove(&key);
    assert!(ready_timer(arm.clone(), &mut delays, &mut slots, TIMER_CHECKPOINT_MS).is_none());
    assert_eq!(delays.len(), 1);
    let current = slots.fire(&arm.slot, arm.ordinal).unwrap();
    delays.remove(&current);
    assert!(ready_timer(arm.clone(), &mut delays, &mut slots, u64::MAX).is_none());
    assert!(delays.is_empty(), "an already-displaced arm was revived");
    let key = delays.insert(arm.clone(), timer_delay(&arm, u64::MAX));
    slots.install(&arm, key);
    delays.remove(&key);
    assert_eq!(
        ready_timer(arm, &mut delays, &mut slots, u64::MAX),
        Some(timer)
    );
    assert!(delays.is_empty());
}
