//! Qualified OS/native providers. All I/O is bootstrap/tool work before a node
//! execution domain exists; no interpreter, package installer or tool is run.
#![allow(clippy::disallowed_methods)]
use crate::protocol::{
    PythonFile, PythonInventory, PythonNativeLibrary, MAX_PYTHON_EXPANDED_BYTES,
};
use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path};

mod loaded;
mod platform;

#[cfg(test)]
pub(super) fn current_platform() -> Result<crate::protocol::PythonPlatform> {
    platform::current()
}

/// Refuse a different qualified platform or missing/changed external library.
/// This does not require the bundled files to have been restored yet.
pub fn verify_prerequisites(inventory: &PythonInventory) -> Result<()> {
    let prerequisites = &inventory.prerequisites;
    prerequisites.validate(&inventory.target, &inventory.roots, &inventory.files)?;
    ensure!(
        prerequisites.platform == platform::current()?,
        "Python native prerequisite platform differs"
    );
    for (path, provider) in &prerequisites.libraries {
        match provider {
            PythonNativeLibrary::File { bytes, sha256 } => verify_file(path, *bytes, sha256)?,
            PythonNativeLibrary::MacosSharedCache => ensure!(
                platform::cached(path)?,
                "Python system-cache prerequisite is absent"
            ),
        }
    }
    Ok(())
}

/// Check the providers actually selected by the process loader. Call before
/// interpreter initialization and after imports, before admitting the host.
pub fn verify_loaded_libraries(inventory: &PythonInventory) -> Result<()> {
    verify_prerequisites(inventory)?;
    let executable = std::env::current_exe()?.canonicalize()?;
    for path in loaded::snapshot()? {
        // macOS shared-cache paths can intentionally have no inode. The
        // vendor OS build is one attested provider for its transitive cache.
        if crate::protocol::system_cache_path(&path) && platform::cached(&path)? {
            continue;
        }
        let path = path.canonicalize()?;
        if path == executable {
            continue;
        }
        match (
            inventory.files.get(&path),
            inventory.prerequisites.libraries.get(&path),
        ) {
            (Some(PythonFile::File { bytes, sha256, .. }), _)
            | (_, Some(PythonNativeLibrary::File { bytes, sha256 })) => {
                verify_file(&path, *bytes, sha256)?
            }
            _ => anyhow::bail!("loaded native library is outside the qualified Python environment"),
        }
    }
    Ok(())
}

/// Every inventoried native binary needs a captured dependency record, including
/// unused extensions. Missing external dependencies cannot hide until import.
pub(super) fn verify_graph_files(inventory: &PythonInventory) -> Result<()> {
    inventory
        .prerequisites
        .validate(&inventory.target, &inventory.roots, &inventory.files)?;
    for (path, entry) in &inventory.files {
        if matches!(entry, PythonFile::File { .. }) && native_file(path)? {
            ensure!(
                inventory.prerequisites.dependencies.contains_key(path),
                "Python native binary has no dependency record"
            );
        }
    }
    for (path, entry) in &inventory.prerequisites.libraries {
        if matches!(entry, PythonNativeLibrary::File { .. }) {
            ensure!(
                inventory.prerequisites.dependencies.contains_key(path),
                "external native library has no dependency record"
            );
        }
    }
    Ok(())
}

fn verify_file(path: &Path, expected_bytes: u64, expected_sha256: &str) -> Result<()> {
    ensure!(
        path.canonicalize()? == path && path.is_file(),
        "Python native prerequisite path differs"
    );
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() == expected_bytes && expected_bytes <= MAX_PYTHON_EXPANDED_BYTES,
        "Python native prerequisite size differs"
    );
    let mut reader = file.take(expected_bytes + 1);
    let mut digest = Sha256::new();
    let mut total = 0u64;
    let mut buffer = [0u8; 65536];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        total += count as u64;
        digest.update(&buffer[..count]);
    }
    ensure!(
        total == expected_bytes && format!("{:x}", digest.finalize()) == expected_sha256,
        "Python native prerequisite digest differs"
    );
    Ok(())
}

fn native_file(path: &Path) -> Result<bool> {
    let mut header = Vec::with_capacity(8);
    std::fs::File::open(path)?
        .take(8)
        .read_to_end(&mut header)?;
    let Some(magic) = header.get(..4) else {
        return Ok(false);
    };
    let architectures = header
        .get(4..8)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_be_bytes);
    let little_architectures = header
        .get(4..8)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_le_bytes);
    Ok(matches!(
        magic,
        b"\x7fELF"
            | b"\xce\xfa\xed\xfe"
            | b"\xcf\xfa\xed\xfe"
            | b"\xfe\xed\xfa\xce"
            | b"\xfe\xed\xfa\xcf"
    ) || (matches!(magic, b"\xca\xfe\xba\xbe" | b"\xca\xfe\xba\xbf")
        && architectures.is_some_and(|value| (1..=32).contains(&value)))
        || (matches!(magic, b"\xbe\xba\xfe\xca" | b"\xbf\xba\xfe\xca")
            && little_architectures.is_some_and(|value| (1..=32).contains(&value))))
}

#[cfg(all(
    test,
    any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))
))]
mod tests;
