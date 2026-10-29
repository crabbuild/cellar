// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn fixture() -> (CachedGroup, ImageView) {
    let mut conf = ::raft::prelude::ConfState::default();
    conf.set_voters(vec![1, 2, 3]);
    let mut cache = crate::raft::cache::genesis(conf);
    let image = ImageView {
        position: cache.applied.position,
        image: crate::raft::cache::CommittedImage {
            name: "verified.sqlite".into(),
            commitment: [9; 32].into(),
            length: 8192,
            ltx_epoch: 1,
            txid: 1,
            checksum: 0,
            page_size: 4096,
        },
    };
    cache.applied.image = Some(image.image.clone());
    (cache, image)
}

fn compact(cache: &mut CachedGroup) {
    cache.snapshot.mut_metadata().index = 3;
    cache.snapshot.mut_metadata().term = 2;
    cache.applied.position = LogPosition {
        index: LogIndex::new(3),
        term: Term::new(2),
    };
    cache.hard_state.commit = 3;
    cache.hard_state.term = 2;
    cache.validate(128).unwrap();
}

#[test]
fn inspection_preserves_original_publication_after_protocol_checkpoint() {
    let (mut cache, image) = fixture();
    assert!(valid_published_position(&cache, &image));
    compact(&mut cache);
    assert_eq!(cache.term(image.position.index.get()), None);
    assert!(valid_published_position(&cache, &image));
    assert_eq!(image.position.index.get(), 1);
}

#[test]
fn inspection_compacted_publication_requires_the_complete_durable_marker() {
    let (mut cache, image) = fixture();
    compact(&mut cache);
    for field in 0..8 {
        let mut changed = cache.clone();
        let marker = changed.applied.image.as_mut().unwrap();
        match field {
            0 => marker.name = "other.sqlite".into(),
            1 => marker.commitment = [8; 32].into(),
            2 => marker.length += 4096,
            3 => marker.ltx_epoch += 1,
            4 => marker.txid += 1,
            5 => marker.checksum = cellar_ltx::CHECKSUM_FLAG | 1,
            6 => marker.page_size = 8192,
            _ => changed.applied.image = None,
        }
        assert!(!valid_published_position(&changed, &image));
    }
}

#[test]
fn inspection_never_uses_marker_equality_to_override_a_retained_term() {
    let (mut cache, mut image) = fixture();
    image.position.term = Term::new(2);
    assert!(!valid_published_position(&cache, &image));
    compact(&mut cache);
    image.position = cache.applied.position;
    image.position.term = Term::new(1);
    assert!(!valid_published_position(&cache, &image));
    image.position.index = LogIndex::new(4);
    assert!(!valid_published_position(&cache, &image));
    image.position.index = LogIndex::new(1);
    image.position.term = Term::new(3);
    assert!(!valid_published_position(&cache, &image));
    image.position.term = Term::new(1);
    cache.hard_state.commit = 2;
    assert!(!valid_published_position(&cache, &image));
}
