// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn pool() -> CellGroupCheckpointPool {
    CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
        max_sources: 1,
        max_retained_bytes: 64 * 1024 * 1024,
        max_cells: 4,
        max_transfer_bytes: 4 * 1024 * 1024,
        max_chunk_bytes: 64 * 1024,
    })
    .unwrap()
}

#[test]
fn archive_inventory_admission_is_retained_before_storage_opens() {
    let pool = pool();
    let pending = pool.prepare_archive_receive(1024).unwrap();
    assert_eq!(pool.usage().sources, 1);
    let reserved = pool.usage().retained_bytes;
    assert!(reserved >= 1024 * 512 + 16384);
    assert!(matches!(
        pool.prepare_archive_receive(1024),
        Err(CellGroupCheckpointError::Busy)
    ));
    assert_eq!(pool.usage().retained_bytes, reserved);
    drop(pending);
    assert_eq!(
        pool.usage(),
        CellGroupCheckpointUsage {
            sources: 0,
            retained_bytes: 0
        }
    );
    assert!(pool.prepare_archive_receive(1024).is_ok());
}

#[test]
fn archive_inventory_cannot_bypass_existing_receiver_byte_bank() {
    let pool = pool();
    assert!(matches!(
        pool.prepare_archive_receive(1_000_000),
        Err(CellGroupCheckpointError::Busy)
    ));
    assert!(matches!(
        pool.prepare_archive_receive(usize::MAX),
        Err(CellGroupCheckpointError::Limit)
    ));
    assert_eq!(pool.usage().sources, 0);
    assert_eq!(pool.usage().retained_bytes, 0);
}
