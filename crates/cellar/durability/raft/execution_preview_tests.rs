// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The reuse proof binds exact physical bytes and the open image-store owner.
#![allow(clippy::disallowed_methods)]
use super::*;

#[test]
fn rooted_preview_transfer_requires_submission_exact_payload_geometry_and_inventory(
) -> anyhow::Result<()> {
    use cellar_ltx::internal::{
        ExtentId, ExtentManifest, Geometry, ImageBinding, NodeExtentStore, EXTENT_BYTES,
    };
    use std::sync::Arc;
    let root = tempfile::tempdir()?;
    let source = root.path().join("source.sqlite");
    let database = rusqlite::Connection::open(&source)?;
    database.execute_batch(
        "PRAGMA page_size=512;CREATE TABLE counter(value);INSERT INTO counter VALUES(1)",
    )?;
    drop(database);
    let bytes = std::fs::read(source)?;
    let geometry = Geometry::new(512, bytes.len() as u64)?;
    let id = ExtentId::of(&bytes)?;
    let binding = ImageBinding {
        history: [7; 32],
        schema: [8; 32],
        epoch: 1,
        txid: 1,
        checksum: 0,
    };
    let manifest = ExtentManifest::new(binding, geometry, &[id])?;
    let store = NodeExtentStore::open(
        root.path().join("images"),
        Arc::new(cellar_ltx::DirectFileSystem),
        Default::default(),
    )?;
    let foreign = NodeExtentStore::open(
        root.path().join("foreign"),
        Arc::new(cellar_ltx::DirectFileSystem),
        Default::default(),
    )?;
    for store in [&store, &foreign] {
        store.put_extent_bytes(id, &bytes)?;
    }
    let image =
        Arc::new(store.seal_image_root(manifest.clone(), &mut vec![0; EXTENT_BYTES as usize])?);
    let foreign_image = foreign.seal_image_root(manifest, &mut vec![0; EXTENT_BYTES as usize])?;
    assert_eq!(image.reference(), foreign_image.reference());
    let empty = ExtentManifest::new(
        ImageBinding { txid: 0, ..binding },
        Geometry::new(0, 0)?,
        &[],
    )?;
    let empty_image = store.seal_image_root(empty, &mut vec![0; EXTENT_BYTES as usize])?;
    let base = images::RootState::of(&empty_image);
    let result = images::RootState::of(&image);
    let payload = b"exact encoded rooted CellUpdate";
    let mut pending = Pending {
        proposal: ProposalId::new([1; 16])?,
        preview: Some(Preview::Rooted(image.clone())),
        base: State::Rooted(base),
        result: State::Rooted(result),
        payload_digest: Sha256::digest(payload).into(),
        kind: Kind::Prune {
            client: ClientId::new([1; 16])?,
            through: 1,
        },
        submitted: false,
    };
    assert!(pending
        .take_rooted_preview(&store, payload, base, result)
        .is_none());
    pending.submitted = true;
    assert!(pending
        .take_rooted_preview(&foreign, payload, base, result)
        .is_none());
    assert!(pending
        .take_rooted_preview(&store, b"different LTX bytes", base, result)
        .is_none());
    let mut wrong_base = base;
    wrong_base.image.txid += 1;
    assert!(pending
        .take_rooted_preview(&store, payload, wrong_base, result)
        .is_none());
    let wrong_result = images::RootState {
        geometry: Geometry::new(512, bytes.len() as u64 + 512)?,
        ..result
    };
    assert!(pending
        .take_rooted_preview(&store, payload, base, wrong_result)
        .is_none());
    let transferred = pending
        .take_rooted_preview(&store, payload, base, result)
        .unwrap();
    assert!(Arc::ptr_eq(&image, &transferred));
    assert!(pending.preview.is_none());
    assert!(pending
        .take_rooted_preview(&store, payload, base, result)
        .is_none());
    drop(pending);
    store.retain_only(&[])?;
    assert_eq!(
        store.reclaim(32)?,
        0,
        "transferred image lost its physical pin"
    );
    Ok(())
}

#[test]
fn configured_images_can_exceed_the_default_64_mib() {
    let limits = ExecutionLimits {
        max_image_bytes: 128 * 1024 * 1024,
        ..Default::default()
    };
    assert!(limits.validate().is_ok());
    assert_eq!(ExecutionLimits::default().max_image_bytes, 64 * 1024 * 1024);
}

#[test]
fn preview_transfer_requires_submission_exact_bytes_states_and_store() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("source.sqlite");
    let database = rusqlite::Connection::open(&source)?;
    database.execute_batch(
        "PRAGMA page_size=512; CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)",
    )?;
    drop(database);
    let bytes = std::fs::read(source)?;
    let base = ImageState::empty(1)?;
    let result = ImageState {
        epoch: 1,
        txid: cellar_ltx::TXID(1),
        checksum: 0,
        page_size: 512,
        length: bytes.len() as u64,
        digest: Sha256::digest(&bytes).into(),
    };
    let id = ImageId::new([1; 16])?;
    let open = |name: &str| -> anyhow::Result<GenerationStore> {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory)?;
        std::fs::write(
            directory.join(format!("{}.sqlite", hex::encode(id.bytes()))),
            &bytes,
        )?;
        Ok(GenerationStore::open(
            directory,
            std::sync::Arc::new(cellar_ltx::DirectFileSystem),
            Default::default(),
        )?)
    };
    let store = open("images")?;
    let foreign = open("foreign")?;
    let pin = store.adopt(id, result)?;
    let foreign_pin = foreign.adopt(id, result)?;
    assert!(store.clone().owns(&pin));
    assert!(!store.owns(&foreign_pin));
    // This is the encoded payload fingerprint, not a proposal ID or just the
    // final digest. A different intermediate chain cannot reuse the proof.
    let payload = b"exact encoded CellUpdate bytes";
    let mut pending = Pending {
        proposal: ProposalId::new([1; 16])?,
        preview: Some(Preview::Materialized(pin)),
        base: State::Materialized(base),
        result: State::Materialized(result),
        payload_digest: Sha256::digest(payload).into(),
        kind: Kind::Prune {
            client: ClientId::new([1; 16])?,
            through: 1,
        },
        submitted: false,
    };
    assert!(pending
        .take_preview(&store, payload, base, result)
        .is_none());
    pending.submitted = true;
    assert!(pending
        .take_preview(&foreign, payload, base, result)
        .is_none());
    assert!(pending
        .take_preview(
            &store,
            b"different LTX bytes with the same proposal",
            base,
            result
        )
        .is_none());
    let mut wrong_base = base;
    wrong_base.epoch += 1;
    assert!(pending
        .take_preview(&store, payload, wrong_base, result)
        .is_none());
    let mut wrong_result = result;
    wrong_result.digest[0] ^= 1;
    assert!(pending
        .take_preview(&store, payload, base, wrong_result)
        .is_none());
    let mut wrong_result = result;
    wrong_result.length += wrong_result.page_size as u64;
    assert!(pending
        .take_preview(&store, payload, base, wrong_result)
        .is_none());
    let transferred = pending.take_preview(&store, payload, base, result).unwrap();
    assert_eq!(
        transferred.open_reader()?.read_exact_at(0, bytes.len())?,
        bytes
    );
    assert!(pending.preview.is_none());
    assert!(pending
        .take_preview(&store, payload, base, result)
        .is_none());
    // Dropping the old private owner cannot retire its transferred generation.
    drop(pending);
    drop(transferred);
    store.reclaim(16)?;
    assert_eq!(store.usage()?.generations, 1);
    Ok(())
}
