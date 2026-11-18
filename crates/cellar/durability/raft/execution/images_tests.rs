// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Inspect the real shared-cell close after it unlinks disposable scratch.
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
