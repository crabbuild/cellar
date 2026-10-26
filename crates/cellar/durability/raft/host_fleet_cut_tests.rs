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

fn image(index: u64, term: u64, digest: u8) -> FleetPublishedImage {
    FleetPublishedImage::new(
        position(index, term),
        ImageState {
            epoch: 1,
            txid: TXID(index),
            checksum: 0,
            page_size: 4096,
            length: 4096,
            digest: [digest; 32],
        },
    )
    .unwrap()
}

fn cut(node: u8, index: u64, term: u64, image: Option<FleetPublishedImage>) -> FleetPublishedCut {
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:cut".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    FleetPublishedCut {
        binding: FleetGroupBinding::new(IncarnationId::new([5; 16]).unwrap(), &scope, [6; 32])
            .unwrap(),
        replica: ReplicaIdentity {
            scope,
            member: MemberId::new(node as u64).unwrap(),
            node: NodeId::new([node; 16]).unwrap(),
            disk: DiskLineage::new([node + 10; 16]).unwrap(),
            process: ProcessGeneration::new([node + 20; 16]).unwrap(),
        },
        storage_instance: EngineInstanceId::new([node + 30; 16]).unwrap(),
        birth: [8; 32],
        schema: [9; 32],
        revision: LogIndex::new(3),
        applied: position(index, term),
        image,
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
fn publication_refuses_extent_root_as_materialized_digest() {
    use crate::application::consensus::ImageCommitment;
    use cellar_ltx::internal::{ExtentRootReference, ImageRoot};

    let mut rooted = marker();
    rooted.commitment = ImageCommitment::Extents(
        ExtentRootReference::new(ImageRoot::from_bytes([7; 32]), 144).unwrap(),
    );
    let durable = DurableApply {
        position: position(5, 2),
        image: Some(rooted.clone()),
    };
    for visible in [&rooted, &marker()] {
        assert_eq!(
            Publication::capture(&durable, Some((visible, position(2, 1)))),
            Err(CommandFailure::UnsupportedCapability)
        );
    }
}

#[test]
fn fixed_publication_cut_covers_configuration_only_progress_and_later_writes() {
    let minimum = cut(1, 5, 2, Some(image(2, 1, 7)));
    let protocol_only = cut(2, 7, 3, Some(image(2, 1, 7)));
    assert!(protocol_only.covers(&minimum).unwrap());
    assert!(!minimum.covers(&protocol_only).unwrap());
    let later = cut(3, 9, 3, Some(image(8, 3, 8)));
    assert!(later.covers(&minimum).unwrap());
    assert_eq!(minimum.applied(), position(5, 2));
    assert_eq!(minimum.image().unwrap().position(), position(2, 1));
    let mut restored = minimum.clone();
    restored.image.as_mut().unwrap().position = restored.applied;
    assert!(protocol_only.covers(&restored).unwrap());
    assert!(restored.covers(&minimum).unwrap());
}

#[test]
fn fixed_publication_cut_rejects_foreign_configuration_and_inconsistent_prefix() {
    let minimum = cut(1, 5, 2, Some(image(2, 1, 7)));
    let mut changed = cut(2, 7, 3, Some(image(2, 1, 7)));
    changed.revision = LogIndex::new(6);
    assert!(matches!(
        changed.covers(&minimum),
        Err(CommandFailure::CommandConflict)
    ));
    changed.revision = minimum.revision;
    changed.birth = [99; 32];
    assert!(matches!(
        changed.covers(&minimum),
        Err(CommandFailure::CommandConflict)
    ));
    let mut foreign = minimum.clone();
    foreign.binding = FleetGroupBinding::new(
        IncarnationId::new([5; 16]).unwrap(),
        &foreign.replica.scope,
        [99; 32],
    )
    .unwrap();
    assert!(matches!(
        foreign.covers(&minimum),
        Err(CommandFailure::CommandConflict)
    ));
    for inconsistent in [
        cut(2, 5, 1, Some(image(2, 1, 7))),
        cut(2, 7, 3, Some(image(2, 1, 99))),
        cut(2, 7, 3, Some(image(4, 2, 8))),
    ] {
        assert!(matches!(
            inconsistent.covers(&minimum),
            Err(CommandFailure::CorruptState)
        ));
    }
}

#[test]
fn canonical_empty_cut_is_distinct_from_missing_publication() {
    let empty = cut(1, 5, 2, None);
    assert!(cut(2, 7, 3, None).covers(&empty).unwrap());
    assert!(cut(2, 9, 3, Some(image(8, 3, 7))).covers(&empty).unwrap());
    assert!(matches!(
        cut(2, 7, 3, Some(image(4, 2, 7))).covers(&empty),
        Err(CommandFailure::CorruptState)
    ));
    assert!(!empty.covers(&cut(2, 9, 3, Some(image(8, 3, 7)))).unwrap());
    assert_eq!(
        Publication::Empty,
        Publication::capture(
            &DurableApply {
                position: position(5, 2),
                image: None
            },
            None
        )
        .unwrap()
    );
    assert_ne!(Publication::Empty, Publication::Pending);
}
