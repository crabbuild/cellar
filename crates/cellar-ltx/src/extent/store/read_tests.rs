// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::{
    internal::{Geometry, ImageBinding},
    DirectFileSystem,
};

#[test]
fn a_cached_descriptor_pins_its_root_after_every_source_view_is_gone() {
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("store");
    let store = NodeExtentStore::open(
        directory.clone(),
        Arc::new(DirectFileSystem),
        Default::default(),
    )
    .unwrap();
    let bytes = vec![7; 512];
    let id = ExtentId::of(&bytes).unwrap();
    store
        .put_extent(id, |offset, count| {
            Ok(bytes[offset as usize..offset as usize + count].to_vec())
        })
        .unwrap();
    let manifest = ExtentManifest::new(
        ImageBinding {
            history: [1; 32],
            schema: [2; 32],
            epoch: 1,
            txid: 1,
            checksum: 0,
        },
        Geometry::new(512, 512).unwrap(),
        &[id],
    )
    .unwrap();
    let sealed = store.seal_root(manifest).unwrap();
    let scratch = ScratchPool::default();
    let file = sealed.view().cached_extent(id, &scratch).unwrap();
    let clone = file.clone();
    drop(sealed);
    store.retain_only(&[]).unwrap();
    assert_eq!(
        store.reclaim(10).unwrap(),
        0,
        "cached file lost its root pin"
    );
    drop(store);
    assert!(NodeExtentStore::open(
        directory.clone(),
        Arc::new(DirectFileSystem),
        Default::default()
    )
    .is_err());
    drop(file);
    assert_eq!(clone.chunk(0, 512).unwrap(), bytes);
    assert_eq!(scratch.usage().unwrap().reserved_files, 1);
    drop(clone);
    assert_eq!(scratch.usage().unwrap(), Default::default());
    let store =
        NodeExtentStore::open(directory, Arc::new(DirectFileSystem), Default::default()).unwrap();
    store.retain_only(&[]).unwrap();
    assert_eq!(store.reclaim(10).unwrap(), 2);
    assert_eq!(store.usage().unwrap().bytes, 0);
}
