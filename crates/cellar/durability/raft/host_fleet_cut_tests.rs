// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn position(index: u64, term: u64) -> LogPosition {
    LogPosition {
        index: LogIndex::new(index),
        term: Term::new(term),
    }
}

fn marker() -> CommittedImage {
    CommittedImage {
        name: "image-1".into(),
        commitment: [7; 32].into(),
        length: 4096,
        ltx_epoch: 1,
        txid: 1,
        checksum: 0,
        page_size: 4096,
    }
}

fn root(
    index: u64,
    term: u64,
    mutation: u64,
    digest: u8,
) -> crate::raft::cell_groups::GroupManifest {
    use cellar_logic::cell_group::*;
    let route = GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [8; 32],
        schema_digest: [9; 32],
    };
    let mut value =
        serde_json::to_value(crate::raft::cell_groups::GroupManifest::genesis(route).unwrap())
            .unwrap();
    value["index"] = serde_json::json!(index);
    value["term"] = serde_json::json!(term);
    if mutation > 0 {
        value["root"] = serde_json::json!({"digest":vec![digest;32],"height":2,"cells":2,"pending_calls":0,"last_mutation_index":mutation,"last_mutation_term":1,"next_due_unix_millis":null});
    }
    serde_json::from_value(value).unwrap()
}

fn cut(node: u8, index: u64, term: u64, mutation: u64, digest: u8) -> FleetPublishedCut {
    let root = root(index, term, mutation, digest);
    let history = root.route().history;
    FleetPublishedCut {
        binding: FleetGroupBinding::new(IncarnationId::new([5; 16]).unwrap(), &history, [6; 32])
            .unwrap(),
        replica: ReplicaIdentity {
            scope: history,
            member: MemberId::new(node as u64).unwrap(),
            node: NodeId::new([node; 16]).unwrap(),
            disk: DiskLineage::new([node + 10; 16]).unwrap(),
            process: ProcessGeneration::new([node + 20; 16]).unwrap(),
        },
        storage_instance: EngineInstanceId::new([node + 30; 16]).unwrap(),
        root,
        configuration_digest: [10; 32],
    }
}

#[test]
fn publication_requires_exact_applied_marker() {
    let durable = DurableApply {
        position: position(5, 2),
        image: Some(marker()),
    };
    let visible = marker();
    assert!(matches!(
        Publication::capture(&durable, Some((&visible, position(2, 1)))).unwrap(),
        Publication::Image(_)
    ));
    let mut different = visible.clone();
    different.commitment = [8; 32].into();
    assert_eq!(
        Publication::capture(&durable, Some((&different, position(2, 1)))).unwrap(),
        Publication::Pending
    );
    assert!(Publication::capture(&durable, Some((&different, position(5, 2)))).is_err());
    let mut equivalent_generation = visible.clone();
    equivalent_generation.name = "snapshot-image-2".into();
    assert_eq!(
        Publication::capture(&durable, Some((&equivalent_generation, position(5, 2)))).unwrap(),
        Publication::Pending
    );
    assert!(Publication::capture(&durable, Some((&visible, position(6, 2)))).is_err());
    assert_eq!(
        Publication::capture(&durable, None).unwrap(),
        Publication::Pending
    );
    let empty = DurableApply {
        position: position(5, 2),
        image: None,
    };
    assert_eq!(
        Publication::capture(&empty, None).unwrap(),
        Publication::Empty
    );
    assert!(Publication::capture(&empty, Some((&visible, position(2, 1)))).is_err());
}

#[test]
fn publication_preserves_extent_domain_and_refuses_materialized_substitution() {
    use crate::application::consensus::ImageCommitment;
    use cellar_ltx::internal::{ExtentRootReference, ImageRoot};
    let mut rooted = marker();
    rooted.commitment = ImageCommitment::Extents(
        ExtentRootReference::new(ImageRoot::from_bytes([7; 32]), 144).unwrap(),
    );
    rooted.name = format!("{}.root", hex::encode([7; 32]));
    let durable = DurableApply {
        position: position(5, 2),
        image: Some(rooted.clone()),
    };
    let Publication::Image(published) =
        Publication::capture(&durable, Some((&rooted, position(2, 1)))).unwrap()
    else {
        panic!("complete extent publication missing")
    };
    assert_eq!(published.commitment(), rooted.commitment);
    assert_eq!(
        published.digest(),
        Err(CommandFailure::UnsupportedCapability)
    );
    let rooted_cut = cut(1, 5, 2, 2, 7);
    let flat_cut = cut(2, 5, 2, 2, 8);
    assert_eq!(
        rooted_cut.covers(&flat_cut),
        Err(CommandFailure::CorruptState)
    );
    assert_eq!(
        flat_cut.covers(&rooted_cut),
        Err(CommandFailure::CorruptState)
    );
    assert_eq!(
        Publication::capture(&durable, Some((&marker(), position(2, 1)))),
        Err(CommandFailure::CorruptState)
    );
}

#[test]
fn fixed_publication_cut_covers_configuration_only_progress_and_later_writes() {
    let minimum = cut(1, 5, 2, 2, 7);
    let protocol = cut(2, 7, 3, 2, 7);
    assert!(protocol.covers(&minimum).unwrap());
    assert!(!minimum.covers(&protocol).unwrap());
    let mut later = cut(3, 9, 3, 8, 8);
    let mut value = serde_json::to_value(later.root()).unwrap();
    value["root"]["last_mutation_term"] = serde_json::json!(3);
    later.root = serde_json::from_value(value).unwrap();
    assert!(later.covers(&minimum).unwrap());
    assert_eq!(minimum.applied(), position(5, 2));
    assert_eq!(minimum.root().root().last_mutation(), Some(position(2, 1)));
    assert_eq!(minimum.root().root().cells(), 2);
}

#[test]
fn fixed_publication_cut_rejects_foreign_configuration_and_inconsistent_prefix() {
    let minimum = cut(1, 5, 2, 2, 7);
    let mut changed = cut(2, 7, 3, 2, 7);
    changed.configuration_digest = [99; 32];
    assert_eq!(
        changed.covers(&minimum),
        Err(CommandFailure::CommandConflict)
    );
    let mut changed = minimum.clone();
    let mut value = serde_json::to_value(&changed.root).unwrap();
    value["birth"] = serde_json::json!(vec![99; 32]);
    changed.root = serde_json::from_value(value).unwrap();
    assert_eq!(
        changed.covers(&minimum),
        Err(CommandFailure::CommandConflict)
    );
    changed = minimum.clone();
    changed.binding = FleetGroupBinding::new(
        IncarnationId::new([5; 16]).unwrap(),
        &changed.replica.scope,
        [99; 32],
    )
    .unwrap();
    assert_eq!(
        changed.covers(&minimum),
        Err(CommandFailure::CommandConflict)
    );
    for inconsistent in [cut(2, 5, 1, 2, 7), cut(2, 7, 3, 2, 99), cut(2, 7, 3, 4, 8)] {
        assert_eq!(
            inconsistent.covers(&minimum),
            Err(CommandFailure::CorruptState)
        );
    }
    let mut changed = minimum.clone();
    let mut value = serde_json::to_value(&changed.root).unwrap();
    value["root"]["cells"] = serde_json::json!(3);
    changed.root = serde_json::from_value(value).unwrap();
    assert_eq!(changed.covers(&minimum), Err(CommandFailure::CorruptState));
}

#[test]
fn canonical_empty_cut_is_distinct_from_missing_publication() {
    let empty = cut(1, 5, 2, 0, 0);
    assert_eq!(empty.root().root().cells(), 0);
    assert!(cut(2, 7, 3, 0, 0).covers(&empty).unwrap());
    let mut later = cut(2, 9, 3, 8, 7);
    let mut value = serde_json::to_value(&later.root).unwrap();
    value["root"]["last_mutation_term"] = serde_json::json!(3);
    later.root = serde_json::from_value(value).unwrap();
    assert!(later.covers(&empty).unwrap());
    assert_eq!(
        cut(2, 7, 3, 4, 7).covers(&empty),
        Err(CommandFailure::CorruptState)
    );
    assert!(!empty.covers(&later).unwrap());
    assert_eq!(
        Publication::capture(
            &DurableApply {
                position: position(5, 2),
                image: None
            },
            None
        )
        .unwrap(),
        Publication::Empty
    );
    assert_ne!(Publication::Empty, Publication::Pending);
}
