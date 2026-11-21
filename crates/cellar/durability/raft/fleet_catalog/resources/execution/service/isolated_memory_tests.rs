// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{fleet_catalog::FleetTrustReader, physical_inventory::RESPONSE_BYTES};

#[test]
fn isolated_repair_memory_leaves_room_for_both_quorum_phases() {
    let bank = Budget::new(16 * 1024 * 1024, 16);
    let service = bank.acquire(2 * ROOT_LIMIT).unwrap();
    let directory = bank.acquire(FleetTrustReader::RESERVATION_BYTES).unwrap();
    let other = bank.acquire(4096).unwrap();
    let baseline = bank.usage();
    let mut memory = Memory::new(&bank, 6).unwrap();
    assert_eq!(bank.usage(), (baseline.0 + RESPONSE_BYTES, baseline.1 + 1));
    let response = bank.acquire(RESPONSE_BYTES).unwrap();
    let routing = bank.acquire(FleetTrustReader::RESERVATION_BYTES).unwrap();
    drop(routing);
    drop(response);

    memory.planning().unwrap();
    assert_eq!(bank.usage(), (baseline.0 + 5 * ROOT_LIMIT, baseline.1 + 1));
    let physical = bank.acquire(RESPONSE_BYTES).unwrap();
    drop(physical);

    memory.admission().unwrap();
    assert_eq!(bank.usage(), (baseline.0 + 2 * ROOT_LIMIT, baseline.1 + 1));
    let response = bank.acquire(RESPONSE_BYTES).unwrap();
    let routing = bank.acquire(FleetTrustReader::RESERVATION_BYTES).unwrap();
    drop(routing);
    drop(response);
    drop(memory);
    assert_eq!(bank.usage(), baseline);
    drop((other, directory, service));
    assert_eq!(bank.usage(), (0, 0));
}

#[test]
fn isolated_repair_memory_failed_growth_keeps_the_proof_charged() {
    let bank = Budget::new(16 * 1024 * 1024, 16);
    let service = bank.acquire(2 * ROOT_LIMIT).unwrap();
    let mut memory = Memory::new(&bank, 6).unwrap();
    let pressure = bank.acquire(11 * 1024 * 1024).unwrap();
    let before = bank.usage();
    assert!(matches!(
        memory.planning(),
        Err(FleetCatalogError::Overloaded)
    ));
    assert_eq!(bank.usage(), before);
    drop(pressure);
    memory.planning().unwrap();
    drop(memory);
    assert_eq!(bank.usage(), (2 * ROOT_LIMIT, 1));
    drop(service);
    assert_eq!(bank.usage(), (0, 0));
}

#[test]
fn isolated_repair_memory_preserves_full_model_misfit_refusal() {
    let bank = Budget::new(16 * 1024 * 1024, 16);
    for nodes in [64, usize::MAX] {
        assert!(matches!(
            Memory::new(&bank, nodes),
            Err(FleetCatalogError::Limit)
        ));
        assert_eq!(bank.usage(), (0, 0));
    }
}
