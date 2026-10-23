//! Checked content-addressed archive reader. Never invokes tar unpack.
use super::super::filesystem::{rename_new, sync_directories};
use super::files::verify_inventory_files;
use crate::protocol::{
    NodeEnvironmentRef, NodeInventory, MAX_NODE_ARCHIVE_BYTES, MAX_NODE_EXPANDED_BYTES,
    MAX_NODE_FILES, MAX_NODE_INVENTORY_BYTES,
};
use anyhow::{ensure, Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
    pub format: u32,
    pub prefix: PathBuf,
    pub inventory_sha256: String,
}

pub(super) struct Checked {
    pub header: Header,
    pub inventory: NodeInventory,
    pub inventory_bytes: Vec<u8>,
}

/// Inspect a bounded archive; optionally spool verified blobs for installation.
pub(super) fn inspect(path: &Path, blobs: Option<&Path>) -> Result<Checked> {
    ensure!(
        std::fs::metadata(path)?.len() <= MAX_NODE_ARCHIVE_BYTES,
        "Node archive exceeds byte limit"
    );
    let decoded = flate2::read::GzDecoder::new(std::fs::File::open(path)?);
    let maximum = MAX_NODE_EXPANDED_BYTES
        + (MAX_NODE_FILES as u64 * 1024)
        + MAX_NODE_INVENTORY_BYTES as u64
        + 65536;
    let mut archive = tar::Archive::new(decoded.take(maximum + 1));
    let mut entries = archive.entries()?.raw(true);
    let mut first = entries.next().context("Node archive has no header")??;
    ensure!(
        first.path()?.as_ref() == Path::new("environment.json")
            && first.header().entry_type().is_file(),
        "Node archive header must be first regular entry"
    );
    let header: Header = serde_json::from_slice(&bounded(&mut first, 65536)?)?;
    ensure!(header.format == 1, "unsupported Node archive format");
    let mut second = entries.next().context("Node archive has no inventory")??;
    ensure!(
        second.path()?.as_ref() == Path::new("inventory.json")
            && second.header().entry_type().is_file(),
        "Node inventory must be second regular entry"
    );
    let inventory_bytes = bounded(&mut second, MAX_NODE_INVENTORY_BYTES)?;
    ensure!(
        format!("{:x}", Sha256::digest(&inventory_bytes)) == header.inventory_sha256,
        "Node archive inventory digest mismatch"
    );
    let inventory: NodeInventory = serde_json::from_slice(&inventory_bytes)?;
    inventory.validate(&header.prefix)?;
    let mut required = BTreeMap::new();
    for entry in inventory.files.values() {
        if let Some(previous) = required.insert(entry.sha256.clone(), entry.bytes) {
            ensure!(previous == entry.bytes, "contradictory Node blob sizes");
        }
    }
    let mut seen = BTreeSet::new();
    for entry in entries {
        let mut entry = entry?;
        ensure!(
            entry.header().entry_type().is_file(),
            "Node archive accepts only regular content blobs"
        );
        let name = entry.path()?;
        let name = name.to_str().context("Node archive path is not UTF-8")?;
        let sha = name
            .strip_prefix("blobs/")
            .context("unexpected Node archive entry")?
            .to_string();
        let expected = *required.get(&sha).context("unlisted Node archive blob")?;
        ensure!(seen.insert(sha.clone()), "duplicate Node archive blob");
        ensure!(entry.size() == expected, "Node archive blob size mismatch");
        let mut output: Box<dyn Write> = match blobs {
            Some(root) => Box::new(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(root.join(&sha))?,
            ),
            None => Box::new(std::io::sink()),
        };
        let mut hash = Sha256::new();
        let mut bytes = 0u64;
        let mut buffer = [0u8; 65536];
        loop {
            let count = entry.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            bytes += count as u64;
            ensure!(bytes <= expected, "Node archive blob byte overflow");
            hash.update(&buffer[..count]);
            output.write_all(&buffer[..count])?;
        }
        ensure!(
            bytes == expected && format!("{:x}", hash.finalize()) == sha,
            "Node archive blob digest mismatch"
        );
    }
    ensure!(
        seen.len() == required.len(),
        "Node archive is missing content blobs"
    );
    Ok(Checked {
        header,
        inventory,
        inventory_bytes,
    })
}

fn bounded(reader: &mut impl Read, maximum: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= maximum,
        "Node archive metadata exceeds byte limit"
    );
    Ok(bytes)
}

pub(super) fn verify_reference(reference: &NodeEnvironmentRef, checked: &Checked) -> Result<()> {
    reference.validate()?;
    ensure!(
        checked.header.prefix == reference.prefix
            && checked.header.inventory_sha256 == reference.inventory_sha256
            && checked.inventory.target == reference.target,
        "Node archive declaration mismatch"
    );
    Ok(())
}

/// Restore under the exact prefix baked into the executable. Existing prefixes
/// are read-only verified and reused; different contents are never overwritten.
pub(super) fn install(
    reference: &NodeEnvironmentRef,
    archive: &Path,
    requested_prefix: &Path,
) -> Result<()> {
    reference.validate()?;
    ensure!(
        requested_prefix == reference.prefix,
        "Node install prefix differs from compiled inventory"
    );
    let parent = requested_prefix
        .parent()
        .context("Node prefix needs parent")?;
    ensure!(
        parent.canonicalize()? == parent,
        "Node install parent must be canonical"
    );
    let blobs = tempfile::tempdir_in(parent)?;
    let checked = inspect(archive, Some(blobs.path()))?;
    verify_reference(reference, &checked)?;
    if requested_prefix.exists() {
        verify_inventory_files(&checked.inventory)?;
        return Ok(());
    }
    let staging = tempfile::tempdir_in(parent)?;
    for (path, entry) in &checked.inventory.files {
        let relative = relative(path, requested_prefix)?;
        let output = staging.path().join(relative);
        std::fs::create_dir_all(output.parent().context("Node file needs parent")?)?;
        let mut input = std::fs::File::open(blobs.path().join(&entry.sha256))?;
        let mut destination = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)?;
        std::io::copy(&mut input, &mut destination)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            destination.set_permissions(std::fs::Permissions::from_mode(if entry.executable {
                0o555
            } else {
                0o444
            }))?;
        }
        destination.sync_all()?;
    }
    // Only checked regular files and directories created here are present.
    sync_directories(staging.path())?;
    // Atomically refuse even an empty competing directory. A separate check
    // followed by ordinary rename would silently replace that directory.
    if let Err(error) = rename_new(staging.path(), requested_prefix) {
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
        verify_inventory_files(&checked.inventory)?;
        return Ok(());
    }
    std::fs::File::open(parent)?.sync_all()?;
    verify_inventory_files(&checked.inventory)?;
    Ok(())
}

fn relative<'a>(path: &'a Path, prefix: &Path) -> Result<&'a Path> {
    let relative = path.strip_prefix(prefix)?;
    ensure!(
        !relative.as_os_str().is_empty()
            && relative
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "invalid Node install relative path"
    );
    Ok(relative)
}
