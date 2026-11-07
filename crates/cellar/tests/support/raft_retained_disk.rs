// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::path::Path;

// The stopped engine must remain byte-for-byte retained. Only hash the Raft
// subtree, never the surrounding application's credentials or live fleet data.
pub fn disk_digest(
    root: &Path,
) -> Result<std::collections::BTreeMap<std::path::PathBuf, [u8; 32]>> {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, [u8; 32]>,
    ) -> Result<()> {
        let mut entries = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let kind = entry.file_type()?;
            ensure!(
                !kind.is_symlink(),
                "unexpected link in retained voter engine"
            );
            if kind.is_dir() {
                visit(root, &path, files)?;
            } else {
                let bytes = std::fs::read(&path)?;
                files.insert(
                    path.strip_prefix(root)?.to_owned(),
                    Sha256::digest(bytes).into(),
                );
            }
        }
        Ok(())
    }
    let mut files = std::collections::BTreeMap::new();
    visit(root, root, &mut files)?;
    ensure!(!files.is_empty(), "missing retained engine files");
    Ok(files)
}
