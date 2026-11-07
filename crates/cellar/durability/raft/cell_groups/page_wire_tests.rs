// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::EnrollmentRecord;

fn fixture() -> (GroupRoute, Page) {
    let history = CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new("cell").unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    };
    let route = GroupRoute {
        history: GroupHistory {
            cluster: history.cluster,
            namespace: history.namespace,
            group: GroupId::new([4; 16]).unwrap(),
            incarnation: IncarnationId::new([5; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [6; 32],
        schema_digest: [7; 32],
    };
    let initial = CellImage {
        epoch: 1,
        txid: 1,
        checksum: 0,
        commitment: CellImageCommitment::Materialized([8; 32]),
    };
    let enrollment = EnrollmentRecord::new(
        history.clone(),
        ProposalId::new([9; 16]).unwrap(),
        route.schema_digest,
        initial,
    )
    .unwrap();
    let witness = CommitWitness {
        group: route.history,
        position: LogPosition {
            index: LogIndex::new(2),
            term: Term::new(1),
        },
        proposal: enrollment.creation(),
    };
    (
        route,
        Page {
            leaf: CellManifestLeaf {
                enrollment,
                version: CellVersion {
                    history,
                    sequence: MutationSequence::new(1).unwrap(),
                    image: initial,
                },
                geometry: CellImageGeometry {
                    length: 512,
                    page_size: 512,
                },
                last_mutation: LogPosition {
                    index: LogIndex::new(2),
                    term: Term::new(1),
                },
                witness,
                effects: EffectSummary::default(),
            },
            left: None,
            right: None,
        },
    )
}

#[test]
fn creation_record_is_required_in_marker_one_pages() {
    let (route, page) = fixture();
    let limits = ManifestLimits::default();
    let encoded = encode(&page, &route, limits).unwrap();
    assert_eq!(decode(&encoded, &route, limits).unwrap().leaf, page.leaf);
    let original = String::from_utf8(encoded[MAGIC.len()..].to_vec()).unwrap();
    let record = String::from_utf8(page.leaf.enrollment.encode().unwrap()).unwrap();
    let replace = |field: &str, value: serde_json::Value| {
        let parsed: serde_json::Value = serde_json::from_str(&record).unwrap();
        record.replacen(
            &format!("\"{field}\":{}", parsed[field]),
            &format!("\"{field}\":{value}"),
            1,
        )
    };
    // Keep every unaffected field in canonical order. A reordered JSON map
    // must not be the reason these malformed identities refuse.
    for changed in [
        None,
        Some("null".to_owned()),
        Some(replace("cell", "another-cell".into())),
        Some(replace("schema", serde_json::json!(vec![0; 32]))),
        Some(replace("initial_txid", 2.into())),
        Some(replace("creation", serde_json::json!(vec![0; 16]))),
    ] {
        let changed = match changed {
            Some(value) => original.replacen(&record, &value, 1),
            None => original.replacen(&format!("\"enrollment\":{record},"), "", 1),
        };
        assert_ne!(changed, original);
        let mut bytes = MAGIC.to_vec();
        bytes.extend(changed.as_bytes());
        assert!(decode(&bytes, &route, limits).is_err());
    }
}

#[test]
fn creation_binding_survives_later_versions_and_bounds_all_cell_names() {
    let (route, mut page) = fixture();
    let original = page.leaf.enrollment.clone();
    page.leaf.version.sequence = MutationSequence::new(2).unwrap();
    assert!(encode(&page, &route, ManifestLimits::default()).is_err());
    page.leaf.version.image.txid = 2;
    page.leaf.version.image.commitment = CellImageCommitment::Materialized([10; 32]);
    page.leaf.last_mutation.index = LogIndex::new(3);
    page.leaf.witness.position = page.leaf.last_mutation;
    page.leaf.witness.proposal = ProposalId::new([10; 16]).unwrap();
    let bytes = encode(&page, &route, ManifestLimits::default()).unwrap();
    let restored = decode(&bytes, &route, ManifestLimits::default()).unwrap();
    assert_eq!(restored.leaf.enrollment, original);
    assert_eq!(restored.leaf.version, page.leaf.version);

    let id = CellId::new("\\".repeat(MAX_CELL_ID_BYTES)).unwrap();
    page.leaf.version.history.cell = id;
    page.leaf.enrollment = EnrollmentRecord::new(
        page.leaf.version.history.clone(),
        original.creation(),
        original.schema(),
        original.initial(),
    )
    .unwrap();
    let bytes = encode(&page, &route, ManifestLimits::default()).unwrap();
    assert!(bytes.len() <= MAX_PAGE_BYTES);
    assert_eq!(
        decode(&bytes, &route, ManifestLimits::default())
            .unwrap()
            .leaf,
        page.leaf
    );
}

#[test]
fn commit_witness_is_required_and_bounded_at_marker_one() {
    let (route, page) = fixture();
    let limits = ManifestLimits::default();
    let encoded = encode(&page, &route, limits).unwrap();
    let original = String::from_utf8(encoded[MAGIC.len()..].to_vec()).unwrap();
    let witness = serde_json::to_string(&WireWitness::from(page.leaf.witness)).unwrap();
    let replace = |field: &str, value: serde_json::Value| {
        let parsed: serde_json::Value = serde_json::from_str(&witness).unwrap();
        witness.replacen(
            &format!("\"{field}\":{}", parsed[field]),
            &format!("\"{field}\":{value}"),
            1,
        )
    };
    for changed in [
        None,
        Some("null".to_owned()),
        Some(replace("namespace", serde_json::json!(vec![80; 16]))),
        Some(replace("group", serde_json::json!(vec![0; 16]))),
        Some(replace("proposal", serde_json::json!(vec![0; 16]))),
        Some(replace("proposal", serde_json::json!(vec![80; 16]))),
        Some(replace("index", 0.into())),
        Some(replace("index", 3.into())),
        Some(replace("term", 2.into())),
    ] {
        let changed = match changed {
            Some(value) => original.replacen(&witness, &value, 1),
            None => original.replacen(&format!("\"witness\":{witness},"), "", 1),
        };
        assert_ne!(changed, original);
        let mut bytes = MAGIC.to_vec();
        bytes.extend(changed.as_bytes());
        assert!(decode(&bytes, &route, limits).is_err());
    }
}
