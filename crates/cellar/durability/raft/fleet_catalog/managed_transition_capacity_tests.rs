// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Admission only; byte ownership supplies no physical inventory or authority.
use super::*;
use crate::raft::{budget::Budget, physical_inventory::RESPONSE_BYTES};

#[test]
fn inventory_collection_leaves_room_for_fresh_trust_at_every_target() -> anyhow::Result<()> {
    let bank = Budget::new(16 * RESPONSE_BYTES, 16);
    let directory = bank.acquire(FleetTrustReader::RESERVATION_BYTES).unwrap();
    let service = bank.acquire(4096).unwrap();
    let baseline = bank.usage();
    let collection = reserve_inventory_collection(&bank)?;
    let mut retained = Vec::new();
    for _ in 0..MAX_TARGETS {
        let response = bank
            .acquire(RESPONSE_BYTES)
            .ok_or_else(|| anyhow::anyhow!("physical response could not enter"))?;
        let refresh = bank
            .acquire(FleetTrustReader::RESERVATION_BYTES)
            .ok_or_else(|| anyhow::anyhow!("collection blocked required fresh trust"))?;
        drop(refresh);
        retained.push(response.retain_bytes(RESPONSE_BYTES).unwrap());
        // Completed immutable outputs keep bytes, not active work slots.
        assert_eq!(bank.usage().1, baseline.1 + 1);
    }
    drop(collection);
    assert_eq!(
        bank.usage(),
        (baseline.0 + MAX_TARGETS * RESPONSE_BYTES, baseline.1)
    );
    drop(retained);
    assert_eq!(bank.usage(), baseline);
    drop((directory, service));
    assert_eq!(bank.usage(), (0, 0));
    Ok(())
}

#[test]
fn inventory_collection_preserves_full_intrinsic_fit_refusal() {
    let bank = Budget::new(MAX_TARGETS * RESPONSE_BYTES, 16);
    assert!(matches!(
        reserve_inventory_collection(&bank),
        Err(FleetCatalogError::Overloaded)
    ));
    assert_eq!(bank.usage(), (0, 0));
}
