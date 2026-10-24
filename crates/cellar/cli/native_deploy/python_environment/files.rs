// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::protocol::{PythonFile, PythonInventory, MAX_PYTHON_FILES};
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

/// Verify the complete provisioned file tree, including executable modes.
/// This is bootstrap/tool I/O; call before entering a node execution domain.
pub fn verify_inventory_files(inventory: &PythonInventory) -> Result<()> {
    ensure!(
        !inventory.roots.is_empty()
            && inventory.roots.len() <= 32
            && inventory.files.len() <= MAX_PYTHON_FILES,
        "Python inventory root/file limit"
    );
    let mut actual = BTreeMap::new();
    for root in &inventory.roots {
        ensure!(
            root.is_absolute() && root.canonicalize()? == *root && root.is_dir(),
            "Python inventory root must be canonical directory"
        );
        scan(root, &inventory.roots, &mut actual)?;
    }
    ensure!(
        actual == inventory.files,
        "Python provisioned files differ from the compiled inventory"
    );
    super::prerequisites::verify_graph_files(inventory)?;
    Ok(())
}

fn scan(path: &Path, roots: &[PathBuf], actual: &mut BTreeMap<PathBuf, PythonFile>) -> Result<()> {
    ensure!(
        actual.len() <= MAX_PYTHON_FILES,
        "Python inventory file limit"
    );
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        let target = std::fs::read_link(path)?;
        let resolved = path.canonicalize()?;
        ensure!(
            roots.iter().any(|root| resolved.starts_with(root)),
            "Python inventory symlink escapes closure"
        );
        actual.insert(path.into(), PythonFile::Symlink { target });
    } else if metadata.is_dir() {
        for child in std::fs::read_dir(path)? {
            scan(&child?.path(), roots, actual)?;
        }
    } else {
        ensure!(
            metadata.is_file(),
            "Python inventory contains a special file"
        );
        let mut file = std::fs::File::open(path)?;
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 65536];
        let mut bytes = 0u64;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            bytes += count as u64;
            digest.update(&buffer[..count]);
        }
        ensure!(
            bytes == metadata.len(),
            "Python inventory file changed while hashing"
        );
        #[cfg(unix)]
        let executable = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o111 != 0
        };
        #[cfg(not(unix))]
        let executable = false;
        actual.insert(
            path.into(),
            PythonFile::File {
                bytes,
                sha256: format!("{:x}", digest.finalize()),
                executable,
            },
        );
    }
    Ok(())
}
