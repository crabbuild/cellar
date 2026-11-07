// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Storage-shape tests; decoded receipt material cannot mint runtime proofs.
use super::*;
use crate::application::consensus::cell_routing_key;
use cellar_logic::{cell_group::*, consensus::*};

fn large_entry() -> Entry {
    let history = GroupHistory {
        cluster: ClusterId::new([240; 16]).unwrap(),
        namespace: NamespaceId::new([241; 16]).unwrap(),
        group: GroupId::new([242; 16]).unwrap(),
        incarnation: IncarnationId::new([243; 16]).unwrap(),
    };
    let route = GroupRoute {
        history,
        range: CellRange::new(
            CellBound::Key(cell_routing_key(
                history.namespace,
                &CellId::new("\\\"".repeat(500)).unwrap(),
            )),
            CellBound::PositiveInfinity,
        )
        .unwrap(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [244; 32],
        schema_digest: [245; 32],
    };
    let root = GroupManifest::genesis(route).unwrap();
    let hold:CellGroupArchiveHold=serde_json::from_value(serde_json::json!({"registry":vec![246;32],"id":3,"allocation":vec![247;32],"root":{"object":{"registry":vec![246;32],"generation":2,"digest":vec![248;32],"length":128},"marker":root}})).unwrap();
    hold.check_reference().unwrap();
    let survivors = std::array::from_fn(|i| Survivor {
        node: [i as u8 + 1; 16],
        disk: [i as u8 + 10; 16],
        engine: [i as u8 + 20; 16],
        process: [i as u8 + 30; 16],
        physical_host: [i as u8 + 40; 16],
        epoch: 1,
        member: i as u64 + 1,
        applied: root.position().into(),
        publication_digest: [249; 32],
    });
    Entry {
        history: [250; 32],
        record: [251; 32],
        evidence: Evidence::Live(Box::new(LiveEvidence {
            fixed: Publication {
                root,
                configuration_digest: [252; 32],
            },
            archive: Archive {
                hold,
                configuration_digest: [252; 32],
            },
            survivors,
        })),
    }
}

#[test]
fn complete_group_receipt_keeps_original_index_row_bound() {
    let entry = large_entry();
    assert!(encode(ENTRY_KIND, &entry, ENTRY_LIMIT).is_err());
    let (row, object) = entry.stored().unwrap();
    assert!(row.len() <= ENTRY_LIMIT);
    let (digest, payload) = object.unwrap();
    assert_eq!(digest, hash(&payload));
    assert!(payload.len() <= PAGE_LIMIT);
    let stored: StoredEntry = decode(ENTRY_KIND, &row, ENTRY_LIMIT).unwrap();
    assert!(matches!(stored.evidence,StoredEvidence::Live {object} if object==digest));
    let live: Box<LiveEvidence> = decode(LIVE_KIND, &payload, PAGE_LIMIT).unwrap();
    assert_eq!(Evidence::Live(live), entry.evidence);
    assert!(row.len() + payload.len() <= APPEND_LIMIT * ENTRY_LIMIT);
}

#[test]
fn cancelled_receipt_uses_no_external_live_payload() {
    let entry = Entry {
        history: [1; 32],
        record: [2; 32],
        evidence: Evidence::Cancelled { operation: [3; 16] },
    };
    let (row, object) = entry.stored().unwrap();
    assert!(object.is_none());
    let stored: StoredEntry = decode(ENTRY_KIND, &row, ENTRY_LIMIT).unwrap();
    assert!(matches!(stored.evidence,StoredEvidence::Cancelled {operation} if operation==[3;16]));
    assert!(decode::<StoredEntry>(
        ENTRY_KIND,
        &encode(ENTRY_KIND, &large_entry(), PAGE_LIMIT).unwrap(),
        ENTRY_LIMIT
    )
    .is_err());
}
