// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Preparation evidence never becomes authority or an idle image owner.
#![allow(clippy::disallowed_methods)]

use super::*;
use crate::raft::execution::CellPreparedCommand;
use cellar_logic::consensus::*;
use cellar_ltx::{
    apply::ImageState,
    generation::{GenerationStore, ImageId},
    TXID,
};

fn group() -> anyhow::Result<GroupHistory> {
    Ok(GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    })
}

fn image(root: &std::path::Path) -> anyhow::Result<(GenerationStore, Preview)> {
    let source = root.join("source.sqlite");
    let database = rusqlite::Connection::open(&source)?;
    database.execute_batch(
        "PRAGMA page_size=512; CREATE TABLE value(n); INSERT INTO value VALUES(7)",
    )?;
    drop(database);
    let bytes = std::fs::read(source)?;
    let id = ImageId::new([1; 16])?;
    let directory = root.join("images");
    std::fs::create_dir(&directory)?;
    std::fs::write(
        directory.join(format!("{}.sqlite", hex::encode(id.bytes()))),
        &bytes,
    )?;
    let store = GenerationStore::open(
        directory,
        Arc::new(cellar_ltx::DirectFileSystem),
        Default::default(),
    )?;
    let pin = store.adopt(
        id,
        ImageState {
            epoch: 1,
            txid: TXID(1),
            checksum: 0,
            page_size: 512,
            length: bytes.len() as u64,
            digest: Sha256::digest(&bytes).into(),
        },
    )?;
    Ok((store, Preview::Materialized(pin)))
}

#[test]
fn exact_preparation_requires_input_engine_group_and_validation_profile() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained preparation evidence disk: {}",
        root.path().display()
    );
    let (store, preview) = image(root.path())?;
    let engine = Arc::new(());
    let group = group()?;
    let bytes = b"exact encoded entry, including every LTX byte and declaration";
    let limits = store.apply_limits();
    let prepared = PreparedCellImage::new(engine.clone(), group, bytes, limits, &preview);
    assert!(prepared.matches(&engine, group, bytes));
    assert!(!prepared.matches(&Arc::new(()), group, bytes));
    let mut other = group;
    other.incarnation = IncarnationId::new([9; 16])?;
    assert!(!prepared.matches(&engine, other, bytes));
    assert!(!prepared.matches(
        &engine,
        group,
        b"different input with the same expected image"
    ));
    assert!(prepared.image(limits).is_some());
    let mut smaller = limits;
    smaller.max_total_decoded_bytes /= 2;
    assert!(prepared.image(smaller).is_none());
    let mut different_frame = limits;
    different_frame.decode.max_frame_block_bytes *= 4;
    assert!(prepared.image(different_frame).is_none());
    Ok(())
}

#[test]
fn submission_clones_do_not_retain_prepared_images_after_producer_closes() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained preparation custody disk: {}",
        root.path().display()
    );
    let (store, preview) = image(root.path())?;
    let engine = Arc::new(());
    let group = group()?;
    let bytes = b"exact encoded entry";
    let image =
        PreparedCellImage::new(engine.clone(), group, bytes, store.apply_limits(), &preview);
    let command = CellPreparedCommand::new(
        bytes.to_vec(),
        ProposalId::new([1; 16])?,
        Term::new(1),
        group,
        engine,
    )
    .with_image(&image);
    let clone = command.clone();
    let id = match &preview {
        Preview::Materialized(pin) => pin.id(),
        Preview::Rooted(_) => unreachable!(),
    };
    store.retire(id)?;
    drop(preview);
    store.reclaim(16)?;
    assert_eq!(store.usage()?.generations, 1);
    assert!(clone.prepared_image().unwrap().upgrade().is_some());
    drop(image);
    assert!(command.prepared_image().unwrap().upgrade().is_none());
    assert!(clone.prepared_image().unwrap().upgrade().is_none());
    store.reclaim(16)?;
    assert_eq!(store.usage()?.generations, 0);
    Ok(())
}
