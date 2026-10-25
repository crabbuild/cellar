// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable private SQLite/WAL capture state. Never a query/recovery authority.

use super::apply::{image_position, PublishedImage};
use super::execution::PrivateOptions;
use super::receipts;
use crate::application::consensus::{CellUpdate, LtxUpdate};
use crate::storage::CommandDatabase;
use anyhow::{bail, ensure, Context as _, Result};
use cellar_logic::consensus::{GroupScope, ProposalId};
use cellar_ltx::apply::{ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationPin, ImageId};
use cellar_ltx::{ltx, Db, FileSystem, TXID};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(super) struct Working {
    pub database: CommandDatabase,
    capture: Db,
    frozen: Option<ImageState>,
}

impl Working {
    pub fn open(options: &PrivateOptions, base: Option<&PublishedImage>) -> Result<Self> {
        let filesystem = options.host.filesystem();
        let path = options.directory.join("working.sqlite");
        clean(filesystem.as_ref(), &options.directory, &path)?;
        if let Some(base) = base {
            let pin = base.pin();
            let state = pin.state();
            ensure!(
                state.length <= options.limits.max_image_bytes,
                "command base exceeds image bound"
            );
            pin.private_copy(&filesystem, &path)?;
            // Only the independently sealed base is a recovery authority.
            // This copy is discarded on every reopen, including after a crash.
        }
        let mut capture = Db::open_disposable_with_host(
            &path,
            options.host.clone(),
            options.sqlite_vfs.as_deref(),
        )?;
        let database = CommandDatabase::open(
            &path,
            options.sqlite_vfs.as_deref(),
            options.limits.max_image_bytes,
        )?;
        receipts::install(database.framework())?;
        if let Some(base) = base {
            let state = base.pin().state();
            // The zero-page baseline is local capture bookkeeping. Starting at
            // the WAL header includes every opening/control/schema write.
            capture
                .seed_continuation(state.txid, (state.length / state.page_size as u64) as u32)?;
        }
        Ok(Self {
            database,
            capture,
            frozen: None,
        })
    }

    pub fn begin_mutation(&mut self) {
        self.frozen = None;
    }

    pub fn matches(&self, base: &PublishedImage) -> bool {
        self.frozen == Some(base.pin().state())
    }

    pub fn trim(&self, options: &PrivateOptions, base: Option<&PublishedImage>) -> Result<()> {
        let txid = base.map(|b| b.pin().state().txid).unwrap_or(TXID(0));
        let directory = l0_directory(&self.capture);
        let entries = match options.host.filesystem().read_dir_iter(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let filesystem = options.host.filesystem();
        for (count, entry) in entries.enumerate() {
            ensure!(
                count <= options.limits.envelope.max_files,
                "private capture inventory exceeds bound"
            );
            let entry = entry?;
            ensure!(!entry.is_dir, "unexpected L0 directory");
            let name = entry.file_name.to_str().context("non-UTF8 L0 name")?;
            let (_, max) = ltx::parse_filename(name)?;
            ensure!(max <= txid, "unaccepted private capture tail");
            if max < txid {
                filesystem.remove_file(&directory.join(name))?;
            }
        }
        // Capture history is private bookkeeping; rebuilding deletes it again.
        Ok(())
    }

    pub fn capture(
        &mut self,
        options: &PrivateOptions,
        scope: &GroupScope,
        proposal: ProposalId,
        base: Option<&PublishedImage>,
    ) -> Result<CellUpdate> {
        let limits = options.limits.envelope;
        let captured = self.capture.capture_with_limits(
            limits.max_files,
            ltx::DecodeLimits {
                max_encoded_bytes: limits.max_payload_bytes,
                max_decoded_bytes: limits.max_decoded_bytes,
                max_pages: usize::try_from(limits.max_pages)?,
                max_image_bytes: options.limits.max_image_bytes,
                ..Default::default()
            },
        )?;
        let result_position = self.capture.pos()?;
        let base_state = base
            .map(|b| b.pin().state())
            .unwrap_or(ImageState::empty(1)?);
        ensure!(
            result_position.txid > base_state.txid,
            "command produced no complete LTX cut"
        );
        let mut files = BTreeMap::new();
        let mut encoded = 0_usize;
        let mut pages = 0_u64;
        let mut decoded = 0_u64;
        for captured in captured {
            let min = captured.txid;
            let max = captured.txid;
            ensure!(
                min > base_state.txid && max <= result_position.txid,
                "capture range crosses base/result"
            );
            let bytes = captured.bytes.into_vec();
            let length = bytes.len();
            encoded = encoded
                .checked_add(length)
                .context("encoded capture overflow")?;
            ensure!(
                encoded <= limits.max_payload_bytes,
                "encoded command delta exceeds bound"
            );
            let verified = ltx::decode_file_bounded(
                &bytes,
                ltx::DecodeLimits {
                    max_encoded_bytes: limits.max_payload_bytes,
                    max_decoded_bytes: limits.max_decoded_bytes,
                    max_pages: usize::try_from(limits.max_pages)?,
                    max_image_bytes: options.limits.max_image_bytes,
                    ..Default::default()
                },
            )?;
            ensure!(
                verified.header().min_txid == min && verified.header().max_txid == max,
                "captured L0 identity/header mismatch"
            );
            pages = pages
                .checked_add(verified.pages().len() as u64)
                .context("capture page overflow")?;
            decoded = decoded
                .checked_add(verified.decoded_bytes())
                .context("decoded capture overflow")?;
            ensure!(
                pages <= limits.max_pages && decoded <= limits.max_decoded_bytes,
                "decoded command delta exceeds bound"
            );
            let update = LtxUpdate {
                min_txid: min.0,
                max_txid: max.0,
                page_count: verified.pages().len() as u64,
                decoded_bytes: verified.decoded_bytes(),
                bytes,
            };
            ensure!(files.insert(min, update).is_none(), "duplicate L0 range");
        }
        let (length, page_size) = self.database.geometry(options.limits.max_image_bytes)?;
        // Read the actual committed SQLite main/WAL overlay, independently of
        // the LTX bytes whose application must produce this exact digest. Keep
        // WAL scratch/index admission separate from the logical image quota.
        let digest = self
            .capture
            .committed_image_digest(length, page_size, 512 * 1024 * 1024)?;
        let result = ImageState {
            epoch: base_state.epoch,
            txid: result_position.txid,
            checksum: result_position.post_apply_checksum,
            page_size,
            length,
            digest,
        };
        let update = CellUpdate {
            scope: scope.clone(),
            application_digest: options.application_digest,
            proposal,
            base: image_position(base_state),
            result: image_position(result),
            files: files.into_values().collect(),
        };
        self.frozen = Some(result);
        Ok(update)
    }

    pub fn close(self) -> Result<()> {
        drop(self.database);
        self.capture.close()?;
        Ok(())
    }
}

pub(super) fn verify_preview(
    options: &PrivateOptions,
    base: Option<&PublishedImage>,
    id: ImageId,
    update: &CellUpdate,
) -> Result<GenerationPin> {
    let header = ltx::Header::parse(&update.files.last().context("empty command cut")?.bytes)?;
    let result = ImageState {
        epoch: update.result.epoch,
        txid: TXID(update.result.txid),
        checksum: update.result.checksum,
        digest: update.result.image_digest,
        page_size: header.page_size,
        length: header.commit as u64 * header.page_size as u64,
    };
    let base_pin = base.map(PublishedImage::pin);
    let base_state = base_pin
        .as_ref()
        .map(GenerationPin::state)
        .unwrap_or(ImageState::empty(update.base.epoch)?);
    let segments = update
        .files
        .iter()
        .map(|file| {
            Ok(LtxSegment {
                bytes: &file.bytes,
                min_txid: TXID(file.min_txid),
                max_txid: TXID(file.max_txid),
                page_count: usize::try_from(file.page_count)?,
                decoded_bytes: file.decoded_bytes,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(options.images.prepare(
        id,
        base_pin.as_ref(),
        &ImageUpdate {
            base: base_state,
            result,
            segments,
        },
    )?)
}

fn l0_directory(capture: &Db) -> PathBuf {
    capture.meta_path().join("ltx/0")
}

fn clean(filesystem: &dyn FileSystem, directory: &Path, path: &Path) -> Result<()> {
    let meta = Db::meta_path_for_path(path);
    let names = [
        "working.lock",
        "working.sqlite",
        "working.sqlite-wal",
        "working.sqlite-shm",
        "working.sqlite-journal",
        ".working.sqlite-litestream",
    ];
    for (count, entry) in filesystem.read_dir_iter(directory)?.enumerate() {
        ensure!(
            count < names.len(),
            "private directory inventory exceeds bound"
        );
        let entry = entry?;
        ensure!(
            names.iter().any(|name| entry.file_name == *name),
            "unexpected private directory entry"
        );
    }
    for name in &names[1..5] {
        match filesystem.remove_file(&directory.join(name)) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
    }
    for _ in 0..32 {
        if filesystem.remove_dir_batch(&meta, 256)? {
            // Cleanup need not survive a crash: the next open repeats it before
            // admitting a callback or reading any private capture history.
            return Ok(());
        }
    }
    bail!("private metadata cleanup exceeds its bound")
}
