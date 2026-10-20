//! Immutable provisioned filesystem closure, bound into the compiled host.
//! Verification is bootstrap I/O, before Python initialization or node admission.
#![allow(clippy::disallowed_methods)]
use super::{initialization::build_layout, Options};
use crate::Source;
use anyhow::{ensure, Context as _, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    format: u32,
    target: String,
    version: String,
    soabi: String,
    home: PathBuf,
    executable: PathBuf,
    standard_library_paths: Vec<PathBuf>,
    module_paths: Vec<PathBuf>,
    roots: Vec<PathBuf>,
    source: Application,
    files: BTreeMap<PathBuf, Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Application {
    sha256: String,
    filename: String,
    descriptor: serde_json::Value,
    distributions: Vec<(String, String)>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum Entry {
    File { bytes: u64, sha256: String },
    Symlink { target: PathBuf },
}

/// Return whether this executable has a build-bound filesystem inventory.
pub fn is_verified_build() -> bool {
    !build_layout::INVENTORY.is_empty()
}

pub(super) fn verify(options: &Options, source: &Source) -> Result<()> {
    if !is_verified_build() {
        return Ok(());
    }
    ensure!(
        build_layout::INVENTORY.len() <= 32 * 1024 * 1024,
        "Python inventory exceeds byte limit"
    );
    let inventory: Inventory = serde_json::from_str(build_layout::INVENTORY)?;
    ensure!(
        inventory.format == 1 && inventory.target == build_layout::TARGET,
        "Python inventory format/target mismatch"
    );
    ensure!(
        inventory.version.starts_with("3.14.") && inventory.soabi == options.interpreter.soabi,
        "Python inventory ABI mismatch"
    );
    ensure!(
        inventory.home == options.interpreter.home.canonicalize()?
            && inventory.executable == options.interpreter.executable.canonicalize()?
            && inventory.standard_library_paths
                == canonical_paths(&options.interpreter.standard_library_paths)?
            && inventory.module_paths == canonical_paths(&options.module_paths)?,
        "Python inventory layout mismatch"
    );
    ensure!(
        inventory.source.sha256 == format!("{:x}", Sha256::digest(source.code.as_bytes()))
            && inventory.source.filename == source.filename
            && inventory.source.descriptor == source.descriptor()
            && inventory.source.distributions == source.distributions,
        "Python source/registration inventory mismatch"
    );
    ensure!(
        !inventory.roots.is_empty() && inventory.files.len() <= 200_000,
        "Python inventory file limit"
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
    for path in inventory
        .standard_library_paths
        .iter()
        .chain(&inventory.module_paths)
        .chain([&inventory.executable])
    {
        let canonical = path.canonicalize()?;
        ensure!(
            inventory
                .roots
                .iter()
                .any(|root| canonical.starts_with(root)),
            "Python import root/executable outside inventory"
        );
    }
    // Identify the library actually bound to the process, not a guessed file
    // named libpython in the provisioned directory. The loader runs before main;
    // missing libraries therefore fail even before this bootstrap can start.
    let library = loaded_library()?;
    ensure!(
        matches!(inventory.files.get(&library), Some(Entry::File { .. })),
        "loaded libpython is outside the verified inventory"
    );
    // SAFETY: Py_GetVersion is documented to work before initialization and
    // returns a process-lifetime NUL-terminated CPython-owned string.
    let version = unsafe { std::ffi::CStr::from_ptr(pyo3::ffi::Py_GetVersion()) }.to_string_lossy();
    ensure!(
        version.split_whitespace().next() == Some(inventory.version.as_str()),
        "loaded CPython micro version differs from inventory"
    );
    Ok(())
}

fn canonical_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    paths
        .iter()
        .map(|path| path.canonicalize().map_err(Into::into))
        .collect()
}

fn loaded_library() -> Result<PathBuf> {
    let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
    // SAFETY: the static symbol name is NUL-terminated. dlsym only queries
    // the process loader; no Python API or resolved function is invoked here.
    // Resolve through the loader so dladdr never identifies an executable's
    // procedure-linkage trampoline instead of the actual shared library.
    let symbol = unsafe { libc::dlsym(libc::RTLD_DEFAULT, c"Py_GetVersion".as_ptr()) };
    ensure!(!symbol.is_null(), "cannot resolve loaded libpython");
    // SAFETY: symbol belongs to the loaded process and dladdr initializes info
    // on success; its address is inspected only, never dereferenced.
    let found = unsafe { libc::dladdr(symbol, info.as_mut_ptr()) };
    ensure!(found != 0, "cannot identify loaded libpython");
    // SAFETY: successful dladdr initialized info and owns dli_fname for the
    // lifetime of the loaded library, which remains linked throughout the host.
    let info = unsafe { info.assume_init() };
    ensure!(!info.dli_fname.is_null(), "loaded libpython has no path");
    // SAFETY: the successful loader query above returned a non-null,
    // NUL-terminated library pathname which remains owned by the loader.
    let path = unsafe { std::ffi::CStr::from_ptr(info.dli_fname) }.to_str()?;
    Path::new(path)
        .canonicalize()
        .context("locate loaded libpython")
}

fn scan(path: &Path, roots: &[PathBuf], actual: &mut BTreeMap<PathBuf, Entry>) -> Result<()> {
    ensure!(actual.len() <= 200_000, "Python inventory file limit");
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        let target = std::fs::read_link(path)?;
        let resolved = path.canonicalize()?;
        ensure!(
            roots.iter().any(|root| resolved.starts_with(root)),
            "Python inventory symlink escapes closure"
        );
        actual.insert(path.into(), Entry::Symlink { target });
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
        actual.insert(
            path.into(),
            Entry::File {
                bytes,
                sha256: format!("{:x}", digest.finalize()),
            },
        );
    }
    Ok(())
}
