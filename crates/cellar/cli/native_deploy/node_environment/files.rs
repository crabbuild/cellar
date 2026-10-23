use crate::protocol::{NodeFile, NodeInventory, MAX_NODE_EXPANDED_BYTES, MAX_NODE_FILES};
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

pub fn verify_inventory_files(inventory: &NodeInventory) -> Result<()> {
    inventory.validate(&inventory.prefix)?;
    ensure!(
        inventory.prefix.canonicalize()? == inventory.prefix,
        "Node prefix is not canonical"
    );
    ensure!(
        capture(&inventory.prefix)? == inventory.files,
        "Node provisioned files differ from compiled inventory"
    );
    Ok(())
}

pub(super) fn capture(root: &Path) -> Result<BTreeMap<PathBuf, NodeFile>> {
    let mut files = BTreeMap::new();
    let mut expanded = 0;
    scan(root, 0, &mut expanded, &mut files)?;
    Ok(files)
}

fn scan(
    path: &Path,
    depth: usize,
    expanded: &mut u64,
    files: &mut BTreeMap<PathBuf, NodeFile>,
) -> Result<()> {
    ensure!(
        depth <= 64 && files.len() < MAX_NODE_FILES,
        "Node inventory depth/file limit"
    );
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        !metadata.file_type().is_symlink(),
        "Node runtime closure must resolve symlinks before capture"
    );
    if metadata.is_dir() {
        for child in std::fs::read_dir(path)? {
            scan(&child?.path(), depth + 1, expanded, files)?;
        }
        return Ok(());
    }
    ensure!(metadata.is_file(), "Node runtime contains special file");
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes += n as u64;
        *expanded = expanded
            .checked_add(n as u64)
            .ok_or_else(|| anyhow::anyhow!("Node byte overflow"))?;
        ensure!(
            *expanded <= MAX_NODE_EXPANDED_BYTES,
            "Node expanded-byte limit"
        );
        hash.update(&buffer[..n]);
    }
    ensure!(bytes == metadata.len(), "Node file changed during capture");
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    };
    #[cfg(not(unix))]
    let executable = false;
    files.insert(
        path.into(),
        NodeFile {
            bytes,
            sha256: format!("{:x}", hash.finalize()),
            executable,
        },
    );
    Ok(())
}
