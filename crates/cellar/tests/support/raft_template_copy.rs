// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private writable input preparation. The supplied source retains its bytes
//! and permissions whether the filesystem supports COW cloning or not.

use anyhow::{ensure, Result};
use cellar_ltx::{DirectFileSystem, FileSystem};
use std::{io::Read as _, path::Path};

#[cfg(test)]
#[path = "raft_template_copy_tests.rs"]
mod tests;

pub(super) fn prepare(source: &Path, destination: &Path) -> Result<()> {
    match DirectFileSystem.clone_read_write(source, destination)? {
        Some(file) => drop(file),
        None => copy_fallback(source, destination)?,
    }
    Ok(())
}

fn copy_fallback(source: &Path, destination: &Path) -> Result<()> {
    let input = std::fs::File::open(source)?;
    let metadata = input.metadata()?;
    ensure!(metadata.is_file(), "prepared input must be a regular file");
    let length = metadata.len();
    let mut input = input.take(length);
    // Create our own writable file exclusively. fs::copy would inherit the
    // supplied source's read-only mode, preventing the private SQLite edits.
    let mut output = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination)?;
    let copied = std::io::copy(&mut input, &mut output)?;
    ensure!(
        copied == length
            && input.get_ref().metadata()?.len() == length
            && output.metadata()?.len() == length,
        "prepared input changed length during copying"
    );
    Ok(())
}
