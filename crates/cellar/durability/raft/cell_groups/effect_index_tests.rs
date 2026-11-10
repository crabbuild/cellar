// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Metadata-index tests only. They do not qualify effect execution or durability.
use super::*;
use crate::raft::cell_groups::{CellImageGeometry, EffectSummary, EnrollmentRecord};
use cellar_logic::cell_group::*;
use cellar_logic::consensus::*;
use std::sync::Arc;

struct Fixture {
    _directory: tempfile::TempDir,
    store: ManifestPageStore,
    tree: ManifestTree,
    root: ManifestRoot,
    leaves: Vec<CellManifestLeaf>,
}

fn fixture(empty: bool) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let mut store = ManifestPageStore::open(
        directory.path(),
        Arc::new(cellar_ltx::DirectFileSystem),
        4096,
        32 * 1024 * 1024,
        16,
    )
    .unwrap();
    let group = GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([4; 16]).unwrap(),
        incarnation: IncarnationId::new([5; 16]).unwrap(),
    };
    let route = GroupRoute {
        history: group,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [6; 32],
        schema_digest: [7; 32],
    };
    let tree = ManifestTree::new(route, ManifestLimits::default()).unwrap();
    let image = CellImage {
        epoch: 1,
        txid: if empty { 0 } else { 1 },
        checksum: 0,
        commitment: CellImageCommitment::Materialized(if empty {
            use sha2::Digest as _;
            sha2::Sha256::digest([]).into()
        } else {
            [8; 32]
        }),
    };
    let mut root = ManifestRoot::default();
    let mut leaves = vec![];
    for n in 0..64 {
        let history = CellHistory {
            cluster: group.cluster,
            namespace: group.namespace,
            cell: CellId::new(format!("cell-{n}")).unwrap(),
            incarnation: IncarnationId::new([3; 16]).unwrap(),
        };
        let enrollment = EnrollmentRecord::new(
            history.clone(),
            ProposalId::new([9; 16]).unwrap(),
            tree.route().schema_digest,
            image,
        )
        .unwrap();
        let position = LogPosition {
            index: LogIndex::new(2),
            term: Term::new(1),
        };
        let effects = match n {
            3 => EffectSummary {
                next_alarm_unix_millis: Some(10),
                ..Default::default()
            },
            19 => EffectSummary {
                next_outbox_unix_millis: Some(10),
                pending_outbox: 1,
                ..Default::default()
            },
            49 => EffectSummary {
                next_alarm_unix_millis: Some(20),
                ..Default::default()
            },
            _ => EffectSummary {
                next_alarm_unix_millis: Some(1000 + n),
                ..Default::default()
            },
        };
        let leaf = CellManifestLeaf {
            enrollment: enrollment.clone(),
            version: CellVersion {
                history,
                sequence: MutationSequence::new(1).unwrap(),
                image,
            },
            geometry: CellImageGeometry {
                length: if empty { 0 } else { 512 },
                page_size: 512,
            },
            last_mutation: position,
            witness: CommitWitness {
                group,
                position,
                proposal: enrollment.creation(),
            },
            effects,
        };
        root = tree.update(&mut store, root, leaf.clone(), None).unwrap();
        leaves.push(leaf);
    }
    Fixture {
        _directory: directory,
        store,
        tree,
        root,
        leaves,
    }
}

#[test]
fn due_selection_matches_independent_order_and_wraps_fairly() {
    let f = fixture(false);
    assert_eq!(f.root.next_due_unix_millis(), Some(10));
    for now in [0, 9, 10, 19, 20, 1000, 9999] {
        let mut eligible: Vec<_> = f
            .leaves
            .iter()
            .filter(|leaf| {
                leaf.effects
                    .next_due_unix_millis()
                    .is_some_and(|at| at <= now)
            })
            .collect();
        eligible.sort_by_key(|leaf| {
            cell_routing_key(f.tree.route().history.namespace, &leaf.version.history.cell)
        });
        for cursor in f
            .leaves
            .iter()
            .map(|leaf| Some(&leaf.version.history.cell))
            .chain([None])
        {
            let key = cursor.map(|cell| cell_routing_key(f.tree.route().history.namespace, cell));
            let expected = eligible
                .iter()
                .copied()
                .find(|leaf| {
                    key.as_ref().is_none_or(|key| {
                        &cell_routing_key(
                            f.tree.route().history.namespace,
                            &leaf.version.history.cell,
                        ) > key
                    })
                })
                .or_else(|| eligible.first().copied());
            let actual = f.tree.next_due(&f.store, f.root, now, cursor).unwrap();
            assert_eq!(actual.as_ref(), expected);
        }
    }
    let mut cursor = None;
    let mut seen = std::collections::BTreeMap::new();
    for _ in 0..20 {
        let leaf = f
            .tree
            .next_due(&f.store, f.root, 10, cursor.as_ref())
            .unwrap()
            .unwrap();
        *seen.entry(leaf.version.history.cell.clone()).or_insert(0) += 1;
        cursor = Some(leaf.version.history.cell);
    }
    assert_eq!(seen.len(), 2);
    assert!(seen.values().all(|count| *count == 10));
}

#[test]
fn changing_effects_updates_new_root_and_preserves_earlier_root() {
    let mut f = fixture(false);
    let original = f.root;
    for n in [3, 19, 49] {
        let previous = f.leaves[n].clone();
        let mut next = previous.clone();
        next.effects = EffectSummary::default();
        next.version.sequence = next.version.sequence.checked_next().unwrap();
        next.version.image.txid += 1;
        next.version.image.commitment = CellImageCommitment::Materialized([10; 32]);
        next.last_mutation.index = LogIndex::new(100 + n as u64);
        next.witness.position = next.last_mutation;
        next.witness.proposal = ProposalId::new([10; 16]).unwrap();
        f.root = f
            .tree
            .update(&mut f.store, f.root, next.clone(), Some(&previous.version))
            .unwrap();
        f.leaves[n] = next;
    }
    assert_eq!(f.root.next_due_unix_millis(), Some(1000));
    assert!(f
        .tree
        .next_due(&f.store, f.root, 999, None)
        .unwrap()
        .is_none());
    assert!(f
        .tree
        .next_due(&f.store, original, 10, None)
        .unwrap()
        .is_some());
    f.tree
        .verify(
            &f.store,
            f.root,
            LogPosition {
                index: LogIndex::new(1000),
                term: Term::new(1),
            },
            |_| Ok(()),
        )
        .unwrap();
    let mut forged = f.root;
    forged.0.as_mut().unwrap().next_due_unix_millis = None;
    assert!(f
        .tree
        .verify(
            &f.store,
            forged,
            LogPosition {
                index: LogIndex::new(1000),
                term: Term::new(1)
            },
            |_| Ok(())
        )
        .is_err());
}

#[test]
fn marker_one_references_require_nullable_due_summary() {
    let f = fixture(false);
    let reference = PageRef {
        digest: [1; 32],
        height: 1,
        cells: 1,
        last_mutation_index: 1,
        last_mutation_term: 1,
        next_due_unix_millis: None,
    };
    let mut value = serde_json::to_value(reference).unwrap();
    assert!(value["next_due_unix_millis"].is_null());
    assert_eq!(
        serde_json::from_value::<PageRef>(value.clone()).unwrap(),
        reference
    );
    value
        .as_object_mut()
        .unwrap()
        .remove("next_due_unix_millis");
    assert!(serde_json::from_value::<PageRef>(value).is_err());
    for field in ["last_mutation_index", "last_mutation_term"] {
        let mut value = serde_json::to_value(reference).unwrap();
        value.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<PageRef>(value).is_err());
    }
    let mut invalid = f.leaves[3].clone();
    invalid.effects.next_outbox_unix_millis = Some(i64::MAX as u64 + 1);
    invalid.effects.pending_outbox = 1;
    assert!(invalid.validate(f.tree.route()).is_err());
}

#[test]
fn complete_manifest_authenticates_latest_mutation_across_other_cell_updates() {
    let mut f = fixture(false);
    let before = f.root;
    assert_eq!(
        before.last_mutation(),
        Some(LogPosition {
            index: LogIndex::new(2),
            term: Term::new(1)
        })
    );
    let mut changed = f.leaves[7].clone();
    let old = changed.version.clone();
    changed.version.sequence = MutationSequence::new(2).unwrap();
    changed.version.image.txid = 2;
    changed.version.image.commitment = CellImageCommitment::Materialized([11; 32]);
    changed.last_mutation = LogPosition {
        index: LogIndex::new(40),
        term: Term::new(2),
    };
    changed.witness = CommitWitness {
        group: f.tree.route().history,
        position: changed.last_mutation,
        proposal: ProposalId::new([12; 16]).unwrap(),
    };
    let after = f
        .tree
        .update(&mut f.store, before, changed.clone(), Some(&old))
        .unwrap();
    assert_eq!(after.cells(), 64);
    assert_eq!(after.last_mutation(), Some(changed.last_mutation));
    assert_eq!(before.last_mutation().unwrap().index.get(), 2);
    let applied = LogPosition {
        index: LogIndex::new(60),
        term: Term::new(3),
    };
    f.tree.verify(&f.store, after, applied, |_| Ok(())).unwrap();
    let mut forged = after;
    forged.0.as_mut().unwrap().last_mutation_index = 39;
    assert!(f
        .tree
        .verify(&f.store, forged, applied, |_| Ok(()))
        .is_err());
    let mut forged = after;
    forged.0.as_mut().unwrap().last_mutation_term = 1;
    assert!(f
        .tree
        .verify(&f.store, forged, applied, |_| Ok(()))
        .is_err());
}

#[test]
fn verified_pinned_index_matches_page_lookup_without_opening_images() {
    let mut f = fixture(true);
    let position = LogPosition {
        index: LogIndex::new(1000),
        term: Term::new(1),
    };
    f.tree
        .verify(&f.store, f.root, position, |_| Ok(()))
        .unwrap();
    let pins = crate::raft::cell_groups::ManifestPins::complete(
        &f.tree,
        &mut f.store,
        f.root,
        &mut |_| Ok(crate::raft::cell_groups::CellImagePin::Empty),
    )
    .unwrap();
    for now in [0, 10, 20, 1000, 9999] {
        for leaf in &f.leaves {
            let cell = &leaf.version.history.cell;
            let key = cell_routing_key(f.tree.route().history.namespace, cell);
            let expected = f.tree.next_due(&f.store, f.root, now, Some(cell)).unwrap();
            let actual = pins.next_due(now, Some(&key));
            assert_eq!(actual.as_ref().map(|(leaf, _)| *leaf), expected.as_ref());
            if let Some((leaf, image)) = actual {
                image.check(leaf, f.tree.route().schema_digest).unwrap();
            }
        }
    }
}
