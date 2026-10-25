// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Content-root protocol, immutable transitions and untrusted manifest bounds.

use cellar_ltx::internal::{
    ExtentId, ExtentManifest, Geometry, ImageBinding, ImageRoot, RootError, EXTENT_BYTES,
};
use sha2::{Digest, Sha256};

fn binding() -> ImageBinding {
    ImageBinding {
        history: [1; 32],
        schema: [2; 32],
        epoch: 3,
        txid: 4,
        checksum: 0,
    }
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn canonical_vector_matches_independent_length_prefixed_sha256_encoder() -> Result<(), RootError> {
    // These constants were produced by a separate Python hashlib/struct
    // implementation, including the twelve empty-tree sibling levels.
    let bytes = vec![7; 4096];
    let extent = ExtentId::of(&bytes)?;
    assert_eq!(
        hex(extent.bytes()),
        "e1f9ae3922d967b0429939ca63cb53b5f385393f5b3d2119758bcfa2b1ae7b69"
    );
    let manifest = ExtentManifest::new(binding(), Geometry::new(4096, 4096)?, &[extent])?;
    assert_eq!(
        hex(manifest.root().bytes()),
        "49829ecb25a273042c7177e262b69238ee2ca64e3dafb02db017e7bfc6733320"
    );
    assert_ne!(
        manifest.root().bytes(),
        <[u8; 32]>::from(Sha256::digest(&bytes))
    );
    let encoded = manifest.encode()?;
    assert_eq!(encoded.len(), 144);
    let decoded = ExtentManifest::decode(&encoded, manifest.root())?;
    assert_eq!(decoded.binding(), binding());
    assert_eq!(decoded.geometry(), manifest.geometry());
    assert_eq!(decoded.extent(0), Some(extent));
    assert_eq!(decoded.extent(1), None);
    assert_eq!(decoded.encode()?, encoded);
    Ok(())
}

#[test]
fn every_binding_field_and_geometry_changes_the_commitment() -> Result<(), RootError> {
    let id = ExtentId::of(&[7; 4096])?;
    let geometry = Geometry::new(4096, 4096)?;
    let base = ExtentManifest::new(binding(), geometry, &[id])?;
    for field in 0..5 {
        let mut changed = binding();
        match field {
            0 => changed.history[0] ^= 1,
            1 => changed.schema[0] ^= 1,
            2 => changed.epoch += 1,
            3 => changed.txid += 1,
            _ => changed.checksum = cellar_ltx::CHECKSUM_FLAG | 9,
        }
        let next = base.with_updates(changed, geometry, &[])?;
        assert_ne!(base.root(), next.root());
    }
    let changed_page_size = ExtentManifest::new(binding(), Geometry::new(512, 4096)?, &[id])?;
    assert_ne!(base.root(), changed_page_size.root());
    assert!(matches!(
        base.with_updates(binding(), Geometry::new(512, 4096)?, &[]),
        Err(RootError::Geometry)
    ));
    Ok(())
}

#[test]
fn every_manifest_byte_and_incomplete_or_extra_input_is_rejected() -> Result<(), RootError> {
    let id = ExtentId::of(&[7; 4096])?;
    let manifest = ExtentManifest::new(binding(), Geometry::new(4096, 4096)?, &[id])?;
    let encoded = manifest.encode()?;
    for length in 0..encoded.len() {
        assert!(ExtentManifest::decode(&encoded[..length], manifest.root()).is_err());
    }
    for index in 0..encoded.len() {
        let mut corrupt = encoded.clone();
        corrupt[index] ^= 1;
        assert!(
            ExtentManifest::decode(&corrupt, manifest.root()).is_err(),
            "accepted corrupted byte {index}"
        );
    }
    let mut extra = encoded.clone();
    extra.push(0);
    assert!(matches!(
        ExtentManifest::decode(&extra, manifest.root()),
        Err(RootError::Encoding)
    ));
    assert!(matches!(
        ExtentManifest::decode(&encoded, ImageRoot::from_bytes([0; 32])),
        Err(RootError::RootDigest)
    ));
    let mut count = encoded;
    count[108..112].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(matches!(
        ExtentManifest::decode(&count, manifest.root()),
        Err(RootError::Encoding)
    ));
    assert!(matches!(
        ExtentManifest::decode(&vec![0; 1024 * 1024], manifest.root()),
        Err(RootError::Encoding)
    ));
    Ok(())
}

#[test]
fn immutable_updates_match_fresh_roots_across_growth_and_truncation() -> Result<(), RootError> {
    let full = ExtentId::of(&vec![1; EXTENT_BYTES as usize])?;
    let alternate = ExtentId::of(&vec![2; EXTENT_BYTES as usize])?;
    let partial = ExtentId::of(&[3; 4096])?;
    let base_geometry = Geometry::new(4096, 3 * EXTENT_BYTES)?;
    let base = ExtentManifest::new(binding(), base_geometry, &[full, alternate, full])?;
    let original_root = base.root();
    let mut position = binding();
    position.txid += 1;
    for (length, updates, expected) in [
        (3 * EXTENT_BYTES, vec![(1, full)], vec![full, full, full]),
        (
            4 * EXTENT_BYTES,
            vec![(3, alternate)],
            vec![full, alternate, full, alternate],
        ),
        (
            3 * EXTENT_BYTES + 4096,
            vec![(3, partial)],
            vec![full, alternate, full, partial],
        ),
        (2 * EXTENT_BYTES, vec![], vec![full, alternate]),
        (EXTENT_BYTES + 4096, vec![(1, partial)], vec![full, partial]),
        (4096, vec![(0, partial)], vec![partial]),
    ] {
        let geometry = Geometry::new(4096, length)?;
        let next = base.with_updates(position, geometry, &updates)?;
        let rebuilt = ExtentManifest::new(position, geometry, &expected)?;
        assert_eq!(next.root(), rebuilt.root());
        assert_eq!(next.encode()?, rebuilt.encode()?);
        assert_eq!(base.root(), original_root, "a child mutated its parent");
        assert_eq!(base.extent(1), Some(alternate));
        assert_eq!(next.extent(geometry.extent_count()), None);
    }
    Ok(())
}

#[test]
fn changing_boundary_requires_exact_coverage_and_ordered_unique_updates() -> Result<(), RootError> {
    let full = ExtentId::of(&vec![1; EXTENT_BYTES as usize])?;
    let partial = ExtentId::of(&[3; 4096])?;
    let geometry = Geometry::new(4096, EXTENT_BYTES + 4096)?;
    let base = ExtentManifest::new(binding(), geometry, &[full, partial])?;
    for (geometry, updates, error) in [
        (
            Geometry::new(4096, 2 * EXTENT_BYTES)?,
            vec![],
            RootError::Coverage,
        ),
        (
            Geometry::new(4096, 3 * EXTENT_BYTES)?,
            vec![(2, full)],
            RootError::Coverage,
        ),
        (Geometry::new(4096, 4096)?, vec![], RootError::Coverage),
        (geometry, vec![(0, full), (0, partial)], RootError::Order),
        (geometry, vec![(1, partial), (0, full)], RootError::Order),
        (geometry, vec![(2, full)], RootError::Coverage),
    ] {
        assert!(base
            .with_updates(binding(), geometry, &updates)
            .is_err_and(|actual| actual == error));
    }
    assert!(ExtentManifest::new(binding(), geometry, &[full]).is_err());
    assert!(ExtentManifest::new(binding(), geometry, &[full, partial, full]).is_err());
    Ok(())
}

#[test]
fn decimal_quota_uses_a_small_manifest_and_correct_offsets_above_four_gib() -> Result<(), RootError>
{
    let geometry = Geometry::new(4096, 10_000_000_000 / 4096 * 4096)?;
    assert_eq!(geometry.extent_count(), 2385);
    assert_eq!(geometry.extent_length(1024), Some(EXTENT_BYTES as u32));
    let id = ExtentId::of(&vec![1; EXTENT_BYTES as usize])?;
    let last_length = geometry.extent_length(2384).unwrap();
    let tail = ExtentId::of(&vec![2; last_length as usize])?;
    let mut ids = vec![id; 2385];
    ids[2384] = tail;
    let manifest = ExtentManifest::new(binding(), geometry, &ids)?;
    let encoded = manifest.encode()?;
    assert_eq!(encoded.len(), 76_432);
    assert!(encoded.len() < 80 * 1024);
    let decoded = ExtentManifest::decode(&encoded, manifest.root())?;
    assert_eq!(decoded.extent(2384), Some(tail));
    let mut next_binding = binding();
    next_binding.txid += 1;
    let changed = ExtentId::of(&vec![2; EXTENT_BYTES as usize])?;
    ids[1024] = changed;
    let next = manifest.with_updates(next_binding, geometry, &[(1024, changed)])?;
    assert_eq!(
        next.root(),
        ExtentManifest::new(next_binding, geometry, &ids)?.root()
    );
    assert_eq!(manifest.extent(1024), Some(id));
    assert_eq!(manifest.extent(2384), Some(tail));
    assert!(matches!(
        Geometry::new(4096, geometry.length() + 4096),
        Err(RootError::Limit)
    ));
    Ok(())
}

#[test]
fn empty_images_and_sqlite_page_geometries_are_explicit() -> Result<(), RootError> {
    let mut empty_binding = binding();
    empty_binding.txid = 0;
    let empty = ExtentManifest::new(empty_binding, Geometry::new(0, 0)?, &[])?;
    assert_eq!(empty.encode()?.len(), 112);
    assert_eq!(
        ExtentManifest::decode(&empty.encode()?, empty.root())?.root(),
        empty.root()
    );
    let id = ExtentId::of(&[0; 4096])?;
    let grown = empty.with_updates(binding(), Geometry::new(4096, 4096)?, &[(0, id)])?;
    assert_eq!(
        grown.root(),
        ExtentManifest::new(binding(), Geometry::new(4096, 4096)?, &[id])?.root()
    );
    let shrunk = grown.with_updates(empty_binding, Geometry::new(0, 0)?, &[])?;
    assert_eq!(shrunk.root(), empty.root());
    for power in 9..=16 {
        let page_size = 1 << power;
        assert!(Geometry::new(page_size, u64::from(page_size)).is_ok());
    }
    for (page_size, length) in [
        (0, 4096),
        (4096, 0),
        (1000, 4000),
        (4096, 4097),
        (131072, 131072),
    ] {
        assert!(Geometry::new(page_size, length).is_err());
    }
    let mut invalid = binding();
    invalid.epoch = 0;
    assert!(matches!(
        ExtentManifest::new(invalid, grown.geometry(), &[id]),
        Err(RootError::Binding)
    ));
    invalid = binding();
    invalid.checksum = 1;
    assert!(matches!(
        ExtentManifest::new(invalid, grown.geometry(), &[id]),
        Err(RootError::Binding)
    ));
    assert!(matches!(
        ExtentManifest::new(empty_binding, grown.geometry(), &[id]),
        Err(RootError::Binding)
    ));
    Ok(())
}

#[test]
fn extent_identity_requires_exact_nonempty_bytes() -> Result<(), RootError> {
    let id = ExtentId::of(&[7; 4096])?;
    id.verify(&[7; 4096])?;
    assert_eq!(id.verify(&[8; 4096]), Err(RootError::ExtentDigest));
    assert_eq!(id.verify(&[7; 4095]), Err(RootError::ExtentDigest));
    assert_eq!(ExtentId::of(&[]), Err(RootError::Geometry));
    assert_eq!(
        ExtentId::of(&vec![0; EXTENT_BYTES as usize + 1]),
        Err(RootError::Geometry)
    );
    let full_geometry = Geometry::new(4096, EXTENT_BYTES)?;
    assert!(matches!(
        ExtentManifest::new(binding(), full_geometry, &[id]),
        Err(RootError::Geometry)
    ));
    let full = ExtentId::of(&vec![7; EXTENT_BYTES as usize])?;
    let manifest = ExtentManifest::new(binding(), full_geometry, &[full])?;
    assert!(matches!(
        manifest.with_updates(binding(), full_geometry, &[(0, id)]),
        Err(RootError::Geometry)
    ));
    Ok(())
}
