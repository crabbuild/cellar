// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded reads of retained follower files through one pinned handle.
use anyhow::Context as _;
use std::path::Path;

pub(super) fn read(
    filesystem: &dyn cellar_ltx::FileSystem,
    path: &Path,
    limit: usize,
    what: &str,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut file = match filesystem.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("cannot open {what}")),
    };
    // Pin one inode across both length checks and the bounded read. A path
    // stat followed by whole-file read can race replacement and allocation.
    // Only absence at open means optional state; later errors remain errors.
    let length = file
        .file_len()
        .with_context(|| format!("cannot stat opened {what}"))?;
    anyhow::ensure!(length <= limit as u64, "{what} exceeds format bound");
    let bytes = file
        .read_exact_at(0, length as usize)
        .with_context(|| format!("cannot read opened {what}"))?;
    anyhow::ensure!(
        bytes.len() as u64 == length && file.file_len()? == length,
        "{what} changed during read"
    );
    Ok(Some(bytes))
}
