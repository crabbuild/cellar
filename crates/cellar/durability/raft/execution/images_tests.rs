// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical preview validation and disposable shared-cell close.
#![allow(clippy::disallowed_methods)]

use super::*;
use crate::raft::execution::ExecutionLimits;
use crate::raft::working::{verify_preview_pin, Working};
use crate::storage::{CommandInputs, ImmutableQueryOptions};
use anyhow::{ensure, Result};
use cellar_ltx::generation::{GenerationStore, ReadServices};
use std::io::Read as _;
use std::sync::Arc;

#[test]
fn rooted_preview_validates_configured_apply_profile_before_reuse() -> Result<()> {
    use cellar_ltx::{internal::NodeExtentLimits, stream_snapshot::ScratchLimits};

    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained rooted-profile disk: {}", root.path().display());
    let images = NodeExtentStore::open(
        root.path().join("images"),
        Arc::new(cellar_ltx::DirectFileSystem),
        NodeExtentLimits::default(),
    )?;
    let execution = ExecutionLimits {
        max_image_bytes: 16 * 1024 * 1024,
        ..ExecutionLimits::default()
    };
    let bounds = RootPrivateOptions::workspace_bounds(execution)?;
    let workspace = images.create_disposable_workspace([1; 32], bounds)?;
    let profile = cellar_ltx::apply::ApplyLimits::default();
    ensure!(profile.decode.max_image_bytes != execution.max_image_bytes);
    let options = Options::rooted(
        RootPrivateOptions {
            workspace,
            host: Default::default(),
            sqlite_vfs: None,
            images: images.clone(),
            query_services: ReadServices {
                now_unix_millis: Arc::new(|| 0),
                fill_random: Arc::new(|bytes| bytes.fill(7)),
            },
            application_digest: [2; 32],
            limits: execution,
            cache_directory: root.path().join("cache"),
            cache_pool: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * cellar_ltx::internal::EXTENT_BYTES,
                max_files: 2,
            })?,
            cache: CacheLimits {
                max_cached_bytes: cellar_ltx::internal::EXTENT_BYTES,
                max_cached_extents: 1,
            },
        },
        Some(profile),
    );
    ensure!(images.usage()?.workspace_bytes == bounds.reservation()?);
    ensure!(options.limits.max_image_bytes == execution.max_image_bytes);
    let mut working = WorkingImage::open(&options, None, [3; 32])?;
    working.database_mut().transaction(
        CommandInputs {
            now_unix_millis: 0,
            random: [7; 32],
        },
        |tx| {
            tx.execute("CREATE TABLE counter(value INTEGER)", [])?;
            tx.execute("INSERT INTO counter VALUES(7)", [])?;
            Ok(())
        },
    )?;
    let update = working.capture_initial(&options)?;
    let id = ImageId::new([4; 16])?;
    let mut strict = options.clone();
    strict.root_apply_limits = Some(cellar_ltx::apply::ApplyLimits {
        max_total_pages: 1,
        ..profile
    });
    ensure!(
        strict
            .images
            .preview_initial(&strict, id, &update, [3; 32])
            .is_err(),
        "preview must enforce the configured profile, not merely relabel its seal"
    );
    let preview = options
        .images
        .preview_initial(&options, id, &update, [3; 32])?;
    let prepared = super::super::CellPreparedImage::new(
        Arc::new(()),
        cellar_logic::cell_group::GroupHistory {
            cluster: cellar_logic::consensus::ClusterId::new([1; 16])?,
            namespace: cellar_logic::consensus::NamespaceId::new([2; 16])?,
            group: cellar_logic::consensus::GroupId::new([3; 16])?,
            incarnation: cellar_logic::consensus::IncarnationId::new([4; 16])?,
        },
        cellar_logic::consensus::Term::new(1),
        b"exact input",
        &preview,
        options.images.preparation_limits(&options)?,
    );
    ensure!(prepared.image(profile).is_some());
    ensure!(prepared.image(apply_limits(execution)?).is_none());
    let database = options.images.read_cell(&options, &preview.cell_pin())?;
    let value: i64 = database
        .connection()
        .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    ensure!(value == 7);
    drop(database);
    working.close_cell(&options)?;
    drop(strict);
    drop(options);
    ensure!(images.usage()?.workspace_bytes == 0);
    Ok(())
}

#[test]
fn disposable_close_preserves_main_and_independently_sealed_result() -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained disposable-close disk: {}", root.path().display());
    let images = GenerationStore::open(
        root.path().join("images"),
        Arc::new(cellar_ltx::DirectFileSystem),
        Default::default(),
    )?;
    let private_options = PrivateOptions {
        directory: root.path().join("private"),
        host: Default::default(),
        sqlite_vfs: None,
        images,
        query_services: ReadServices {
            now_unix_millis: Arc::new(|| 0),
            fill_random: Arc::new(|bytes| bytes.fill(7)),
        },
        application_digest: [1; 32],
        limits: ExecutionLimits::default(),
    };
    std::fs::create_dir(&private_options.directory)?;
    let options: Options = private_options.clone().into();
    let inputs = CommandInputs {
        now_unix_millis: 0,
        random: [7; 32],
    };
    let mut initial = Working::open_pin(&private_options, None)?;
    initial.database.transaction(inputs, |tx| {
        tx.execute("CREATE TABLE counter(value INTEGER)", [])?;
        tx.execute("INSERT INTO counter VALUES(0)", [])?;
        Ok(())
    })?;
    let first = initial.capture_pin(&private_options, None)?;
    let base = verify_preview_pin(
        &private_options,
        None,
        ImageId::new([1; 16])?,
        first.base,
        first.result,
        &first.files,
    )?;
    WorkingImage::Materialized(Box::new(initial)).close_cell(&options)?;

    let mut working = Working::open_pin(&private_options, Some(&base))?;
    working.begin_mutation();
    working.database.transaction(inputs, |tx| {
        tx.execute("UPDATE counter SET value=value+1", [])?;
        Ok(())
    })?;
    let update = working.capture_pin(&private_options, Some(&base))?;
    let sealed = verify_preview_pin(
        &private_options,
        Some(&base),
        ImageId::new([2; 16])?,
        update.base,
        update.result,
        &update.files,
    )?;
    let private = private_options.directory.join("working.sqlite");
    let before = std::fs::read(&private)?;
    // Keep the inode observable after real close_cell removes its pathname.
    // This ordinary read handle does not hold a SQLite reader/checkpoint lock.
    let mut discarded_main = std::fs::File::open(&private)?;
    let wal_bytes = std::fs::metadata(private_options.directory.join("working.sqlite-wal"))?.len();
    ensure!(wal_bytes > 32, "the command must have an actual WAL");
    let value: i64 =
        working
            .database
            .framework()
            .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    ensure!(value == 1);
    WorkingImage::Materialized(Box::new(working)).close_cell(&options)?;
    ensure!(
        !private.exists(),
        "closed shared-cell scratch must be discarded"
    );
    let mut after = Vec::new();
    discarded_main.read_to_end(&mut after)?;
    drop(discarded_main);

    // The independently replayed result remains complete after private close.
    sealed.verify_and_sync()?;
    let database = crate::storage::open_immutable_generation(
        sealed.clone(),
        ImmutableQueryOptions {
            services: private_options.query_services.clone(),
            sqlite_vec: false,
        },
    )?;
    let value: i64 = database
        .connection()
        .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    ensure!(value == 1);
    drop(database);
    let reopened = Working::open_pin(&private_options, Some(&sealed))?;
    let value: i64 =
        reopened
            .database
            .framework()
            .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    ensure!(value == 1, "rebuild must use the sealed result");
    WorkingImage::Materialized(Box::new(reopened)).close_cell(&options)?;
    sealed.verify_and_sync()?;
    let unchanged = before == after;
    eprintln!(
        "disposable close work: main_unchanged={unchanged} before_bytes={} after_bytes={} wal_bytes={wal_bytes}",
        before.len(),
        after.len()
    );
    ensure!(
        unchanged,
        "private close copied WAL pages into main that the next rebuild discards"
    );
    Ok(())
}
