// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn releases_only_proven_lengths_of_previously_charged_files() {
    let before = Charges {
        unattributed: 200,
        files: vec![
            File(0, 1, 10),
            File(0, 2, 20),
            File(0, 3, 30),
            File(1, 1, 40),
        ],
    };
    let removed = [File(0, 1, 15), File(0, 3, 25), File(1, 2, 999)];
    let sample = Charges {
        unattributed: 0,
        files: vec![File(0, 4, 7), File(1, 1, 39)],
    };
    let after = before.reconcile(&removed, sample).unwrap();
    assert_eq!(after.floor().unwrap(), 300 - 10 - 25 + 7);
    assert_eq!(after.unattributed, 200 + 20 + 5);
    assert_eq!(after.files[1].2, 40);
}

#[test]
fn a_later_receipt_cannot_spend_unattributed_debt() {
    let before = Charges {
        unattributed: 30,
        files: vec![File(0, 4, 7), File(1, 1, 40)],
    };
    let removed = [File(0, 2, 50), File(0, 4, 7)];
    let sample = Charges {
        unattributed: 0,
        files: vec![File(0, 5, 3), File(1, 1, 44)],
    };
    let after = before.reconcile(&removed, sample).unwrap();
    assert_eq!(after.floor().unwrap(), 77);
    assert_eq!(after.unattributed, 30);
}

#[test]
fn rejects_duplicate_and_still_present_deletions() {
    let before = Charges {
        unattributed: 0,
        files: vec![File(0, 1, 15)],
    };
    let sample = || Charges {
        unattributed: 0,
        files: vec![File(0, 1, 20)],
    };
    assert!(matches!(
        before.reconcile(&[File(0, 1, 15)], sample()),
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    assert!(matches!(
        before.reconcile(&[File(0, 2, 15), File(0, 2, 15)], sample()),
        Err(FleetCatalogError::Corrupt(_))
    ));
}

#[test]
fn retained_and_new_charges_check_overflow() {
    let before = Charges {
        unattributed: u64::MAX,
        files: Vec::new(),
    };
    let sample = Charges {
        unattributed: 0,
        files: vec![File(0, 1, 1)],
    };
    assert!(matches!(
        before.reconcile(&[], sample),
        Err(FleetCatalogError::Limit)
    ));
}
