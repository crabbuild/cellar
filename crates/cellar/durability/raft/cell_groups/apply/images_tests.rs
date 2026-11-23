// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A retained result name is not evidence for a different supplied delta.
#![allow(clippy::disallowed_methods)]

use super::*;
use cellar_logic::{cell_group::*, consensus::*};
use cellar_ltx::{ltx, DirectFileSystem};
use std::sync::Arc;

#[test]
fn cached_result_without_live_preparation_still_replays_exact_input() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained cached result replay disk: {}",
        root.path().display()
    );
    let source = root.path().join("source.sqlite");
    let database = rusqlite::Connection::open(&source)?;
    database.execute_batch(
        "PRAGMA page_size=512; CREATE TABLE value(n); INSERT INTO value VALUES(7)",
    )?;
    let bytes = std::fs::read(&source)?;
    let cell = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("cached-replay")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let result = ImageState {
        epoch: 1,
        txid: TXID(1),
        checksum: 0,
        page_size: 512,
        length: bytes.len() as u64,
        digest: Sha256::digest(&bytes).into(),
    };
    let store = GenerationStore::open(
        root.path().join("images"),
        Arc::new(DirectFileSystem),
        Default::default(),
    )?;
    let encode = |image: &[u8]| -> anyhow::Result<Vec<u8>> {
        let pages: Vec<_> = image
            .as_chunks::<512>()
            .0
            .iter()
            .enumerate()
            .map(|(index, page)| (index as u32 + 1, page.to_vec()))
            .collect();
        Ok(ltx::encode_file_v0_5_2(
            &ltx::Header {
                version: ltx::VERSION,
                flags: ltx::HEADER_FLAG_NO_CHECKSUM,
                page_size: 512,
                commit: pages.len() as u32,
                min_txid: TXID(1),
                max_txid: TXID(1),
                ..Default::default()
            },
            &pages,
            0,
        )?)
    };
    let encoded = encode(&bytes)?;
    let decoded = ltx::decode_file_bounded(&encoded, Default::default())?;
    let pin = store.prepare(
        image_id(&cell, 1, result)?,
        None,
        &ImageUpdate {
            base: ImageState::empty(1)?,
            result,
            segments: vec![LtxSegment {
                bytes: &encoded,
                min_txid: TXID(1),
                max_txid: TXID(1),
                page_count: decoded.pages().len(),
                decoded_bytes: decoded.decoded_bytes(),
            }],
        },
    )?;
    let images = CellGroupApplyImages::Materialized {
        images: store,
        services: ReadServices {
            now_unix_millis: Arc::new(|| 0),
            fill_random: Arc::new(|bytes| bytes.fill(17)),
        },
    };
    let apply = |encoded: &[u8]| -> Result<EnrollmentImage, ExecutionError> {
        let decoded = ltx::decode_file_bounded(encoded, Default::default()).map_err(storage)?;
        images.prepare_with_image(
            ImageApply {
                cell: &cell,
                sequence: 1,
                schema: [9; 32],
                base: None,
                image: LtxImage {
                    epoch: 1,
                    txid: 1,
                    checksum: 0,
                    commitment: ImageCommitment::Sha256(result.digest),
                },
                files: &[GroupLtx {
                    bytes: encoded,
                    min_txid: 1,
                    max_txid: 1,
                    page_count: decoded.pages().len() as u64,
                    decoded_bytes: decoded.decoded_bytes(),
                }],
                max_effect_rows: 100,
            },
            false,
            None,
        )
    };
    // A legitimate replay can use the name only after checking its input.
    drop(apply(&encoded)?);
    database.execute_batch("UPDATE value SET n=8")?;
    drop(database);
    let different = encode(&std::fs::read(source)?)?;
    assert_ne!(different, encoded);
    // Valid LTX with the same advertised result must not inherit the old seal.
    assert!(apply(&different).is_err());
    pin.verify_and_sync()?;
    Ok(())
}
