//! Checked content-addressed archive reader. Never invokes tar unpack.
use super::files::verify_inventory_files;
use crate::protocol::{
    PythonEnvironmentRef, PythonFile, PythonInventory, MAX_PYTHON_ARCHIVE_BYTES,
    MAX_PYTHON_EXPANDED_BYTES, MAX_PYTHON_FILES, MAX_PYTHON_INVENTORY_BYTES,
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
    pub inventory: PythonInventory,
    pub inventory_bytes: Vec<u8>,
}

/// Inspect a bounded archive; optionally spool verified blobs for installation.
pub(super) fn inspect(path: &Path, blobs: Option<&Path>) -> Result<Checked> {
    ensure!(
        std::fs::metadata(path)?.len() <= MAX_PYTHON_ARCHIVE_BYTES,
        "Python archive exceeds byte limit"
    );
    let decoded = flate2::read::GzDecoder::new(std::fs::File::open(path)?);
    let maximum = MAX_PYTHON_EXPANDED_BYTES
        + (MAX_PYTHON_FILES as u64 * 1024)
        + MAX_PYTHON_INVENTORY_BYTES as u64
        + 65536;
    let mut archive = tar::Archive::new(decoded.take(maximum + 1));
    let mut entries = archive.entries()?.raw(true);
    let mut first = entries.next().context("Python archive has no header")??;
    ensure!(
        first.path()?.as_ref() == Path::new("environment.json")
            && first.header().entry_type().is_file(),
        "Python archive header must be first regular entry"
    );
    let header: Header = serde_json::from_slice(&bounded(&mut first, 65536)?)?;
    ensure!(header.format == 1, "unsupported Python archive format");
    let mut second = entries
        .next()
        .context("Python archive has no inventory")??;
    ensure!(
        second.path()?.as_ref() == Path::new("inventory.json")
            && second.header().entry_type().is_file(),
        "Python inventory must be second regular entry"
    );
    let inventory_bytes = bounded(&mut second, MAX_PYTHON_INVENTORY_BYTES)?;
    ensure!(
        format!("{:x}", Sha256::digest(&inventory_bytes)) == header.inventory_sha256,
        "Python archive inventory digest mismatch"
    );
    let inventory: PythonInventory = serde_json::from_slice(&inventory_bytes)?;
    inventory.validate(&header.prefix)?;
    let mut required = BTreeMap::new();
    for entry in inventory.files.values() {
        if let PythonFile::File { bytes, sha256, .. } = entry {
            if let Some(previous) = required.insert(sha256.clone(), *bytes) {
                ensure!(previous == *bytes, "contradictory Python blob sizes");
            }
        }
    }
    let mut seen = BTreeSet::new();
    for entry in entries {
        let mut entry = entry?;
        ensure!(
            entry.header().entry_type().is_file(),
            "Python archive accepts only regular content blobs"
        );
        let name = entry.path()?;
        let name = name.to_str().context("Python archive path is not UTF-8")?;
        let sha = name
            .strip_prefix("blobs/")
            .context("unexpected Python archive entry")?
            .to_string();
        let expected = *required.get(&sha).context("unlisted Python archive blob")?;
        ensure!(seen.insert(sha.clone()), "duplicate Python archive blob");
        ensure!(
            entry.size() == expected,
            "Python archive blob size mismatch"
        );
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
            ensure!(bytes <= expected, "Python archive blob byte overflow");
            hash.update(&buffer[..count]);
            output.write_all(&buffer[..count])?;
        }
        ensure!(
            bytes == expected && format!("{:x}", hash.finalize()) == sha,
            "Python archive blob digest mismatch"
        );
    }
    ensure!(
        seen.len() == required.len(),
        "Python archive is missing content blobs"
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
        "Python archive metadata exceeds byte limit"
    );
    Ok(bytes)
}

pub(super) fn verify_reference(reference: &PythonEnvironmentRef, checked: &Checked) -> Result<()> {
    reference.validate()?;
    ensure!(
        checked.header.prefix == reference.prefix
            && checked.header.inventory_sha256 == reference.inventory_sha256
            && checked.inventory.target == reference.target,
        "Python archive declaration mismatch"
    );
    Ok(())
}

/// Restore under the exact prefix baked into the executable. Existing prefixes
/// are read-only verified and reused; different contents are never overwritten.
pub(super) fn install(
    reference: &PythonEnvironmentRef,
    archive: &Path,
    requested_prefix: &Path,
) -> Result<()> {
    reference.validate()?;
    ensure!(
        requested_prefix == reference.prefix,
        "Python install prefix differs from compiled inventory"
    );
    let parent = requested_prefix
        .parent()
        .context("Python prefix needs parent")?;
    ensure!(
        parent.canonicalize()? == parent,
        "Python install parent must be canonical"
    );
    let blobs = tempfile::tempdir_in(parent)?;
    let checked = inspect(archive, Some(blobs.path()))?;
    verify_reference(reference, &checked)?;
    super::prerequisites::verify_prerequisites(&checked.inventory)?;
    if requested_prefix.exists() {
        verify_inventory_files(&checked.inventory)?;
        return Ok(());
    }
    let staging = tempfile::tempdir_in(parent)?;
    for (path, entry) in &checked.inventory.files {
        let relative = relative(path, requested_prefix)?;
        let output = staging.path().join(relative);
        std::fs::create_dir_all(output.parent().context("Python file needs parent")?)?;
        if let PythonFile::File {
            sha256, executable, ..
        } = entry
        {
            let mut input = std::fs::File::open(blobs.path().join(sha256))?;
            let mut destination = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)?;
            std::io::copy(&mut input, &mut destination)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                destination.set_permissions(std::fs::Permissions::from_mode(if *executable {
                    0o555
                } else {
                    0o444
                }))?;
            }
            #[cfg(not(unix))]
            let _ = executable;
            destination.sync_all()?;
        }
    }
    for (path, entry) in &checked.inventory.files {
        if let PythonFile::Symlink { target } = entry {
            let output = staging.path().join(relative(path, requested_prefix)?);
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &output)?;
            #[cfg(not(unix))]
            {
                let _ = (target, output);
                anyhow::bail!("Python environment installation requires Unix");
            }
        }
    }
    // Only directories/files we created are present; symlink destinations were
    // checked against the inventory roots before any filesystem mutation.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // The installed runtime may be read by a distinct service account.
        std::fs::set_permissions(staging.path(), std::fs::Permissions::from_mode(0o755))?;
    }
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
        "invalid Python install relative path"
    );
    Ok(relative)
}

fn sync_directories(path: &Path) -> Result<()> {
    for child in std::fs::read_dir(path)? {
        let child = child?;
        if child.file_type()?.is_dir() {
            sync_directories(&child.path())?;
        }
    }
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn rename_new(source: &Path, destination: &Path) -> std::io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let source = std::ffi::CString::new(source.as_os_str().as_encoded_bytes())
            .map_err(std::io::Error::other)?;
        let destination = std::ffi::CString::new(destination.as_os_str().as_encoded_bytes())
            .map_err(std::io::Error::other)?;
        #[cfg(target_os = "linux")]
        // SAFETY: both names are owned, NUL-terminated paths valid for this
        // synchronous syscall. Kernel NOREPLACE performs one atomic publication.
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        #[cfg(target_os = "macos")]
        // SAFETY: owned path buffers remain alive for the syscall; RENAME_EXCL
        // atomically refuses any existing destination, including empty dirs.
        let result =
            unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (source, destination);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Python environment installation requires Linux or macOS",
        ))
    }
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
