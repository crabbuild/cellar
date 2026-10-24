// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::call_deadline;
use std::time::Duration;

#[test]
fn nested_calls_keep_the_original_parent_deadline() {
    assert_eq!(call_deadline(120, 1_000, None, Some(150)), 150);
    assert_eq!(
        call_deadline(120, 1_000, Some(Duration::from_secs(60)), Some(150)),
        150
    );
    assert_eq!(
        call_deadline(120, 1_000, Some(Duration::from_millis(10)), Some(150)),
        130
    );
    assert_eq!(call_deadline(160, 1_000, None, Some(150)), 150);
}

#[test]
fn root_calls_cannot_exceed_host_policy_or_overflow() {
    assert_eq!(call_deadline(120, 1_000, None, None), 1_120);
    assert_eq!(call_deadline(120, 1_000, Some(Duration::MAX), None), 1_120);
    assert_eq!(call_deadline(120, 1_000, Some(Duration::ZERO), None), 120);
    assert_eq!(call_deadline(u64::MAX - 1, 1_000, None, None), u64::MAX);
}
