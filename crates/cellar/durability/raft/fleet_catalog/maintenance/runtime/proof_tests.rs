// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;

#[test]
fn restart_sample_budget_retains_peer_age_and_collection_time() {
    assert_eq!(sample_budget(10_000, 9_000, 6_000).unwrap(), 1000);
    assert_eq!(sample_budget(10_000, 6_000, 9_000).unwrap(), 1000);
    assert_eq!(sample_budget(10_000, 5_000, 9_000).unwrap(), 0);
}

#[test]
fn restart_sample_budget_refuses_expired_and_backwards_clock() {
    for (current, started, sampled) in [
        (10_001, 9_000, 5_000),
        (10_001, 5_000, 9_000),
        (9_000, 10_000, 10_000),
    ] {
        assert!(matches!(
            sample_budget(current, started, sampled),
            Err(FleetCatalogError::Unconfirmed)
        ));
    }
}
