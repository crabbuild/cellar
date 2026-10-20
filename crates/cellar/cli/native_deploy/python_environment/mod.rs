//! Build/install filesystem resources outside node execution domains.
#![allow(clippy::disallowed_methods)]
mod archive;
mod files;
pub use files::verify_inventory_files;

use crate::{
    bucket::Bucket,
    protocol::{
        Manifest, NativeArtifact, PythonEnvironmentRef, FEATURE_EMBEDDED_PYTHON_V1,
        MAX_PYTHON_ARCHIVE_BYTES,
    },
};
use anyhow::{ensure, Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

/// Explicit build inputs. Both artifacts are produced before invoking Cargo;
/// startup/request execution never installs or downloads Python packages.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonProject {
    pub inventory: PathBuf,
    pub archive: PathBuf,
}

pub(super) struct Prepared {
    pub inventory_path: PathBuf,
    pub archive_path: PathBuf,
    checked: archive::Checked,
    sha256: String,
    bytes: u64,
}

impl PythonProject {
    pub(super) fn prepare(&self, base: &Path) -> Result<Prepared> {
        let inventory_path = base.join(&self.inventory).canonicalize()?;
        let archive_path = base.join(&self.archive).canonicalize()?;
        let checked = archive::inspect(&archive_path, None)?;
        let mut expected = Vec::new();
        std::fs::File::open(&inventory_path)?
            .take(crate::protocol::MAX_PYTHON_INVENTORY_BYTES as u64 + 1)
            .read_to_end(&mut expected)?;
        ensure!(
            expected.len() <= crate::protocol::MAX_PYTHON_INVENTORY_BYTES,
            "Python inventory exceeds byte limit"
        );
        ensure!(
            expected == checked.inventory_bytes,
            "build inventory differs from environment archive"
        );
        // Capture happens before compilation. Refuse a changed provisioned
        // environment instead of rebuilding against contents outside the lock.
        verify_inventory_files(&checked.inventory)?;
        let (sha256, bytes) = identity(&archive_path)?;
        Ok(Prepared {
            inventory_path,
            archive_path,
            checked,
            sha256,
            bytes,
        })
    }
}

impl Prepared {
    pub(super) fn executable(&self) -> &Path {
        &self.checked.inventory.executable
    }

    pub(super) fn target(&self) -> &str {
        &self.checked.inventory.target
    }

    pub(super) fn linker_arguments(&self) -> Result<Vec<String>> {
        let mut directories = std::collections::BTreeSet::new();
        for (path, entry) in &self.checked.inventory.files {
            if !matches!(entry, crate::protocol::PythonFile::File { .. })
                || !path.starts_with(&self.checked.inventory.home)
            {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            if name == "Python"
                || (name.starts_with("libpython3.14")
                    && (name.ends_with(".dylib") || name.contains(".so")))
            {
                directories.insert(
                    path.parent()
                        .context("libpython requires directory")?
                        .to_path_buf(),
                );
            }
        }
        ensure!(
            !directories.is_empty() && directories.len() <= 32,
            "Python closure needs bounded libpython search directories"
        );
        let mut arguments = Vec::new();
        for directory in directories {
            // Separate -Xlinker arguments preserve spaces and commas in paths.
            // No shell is involved, and flags cannot be injected through a path.
            for value in [
                "-Xlinker",
                "-rpath",
                "-Xlinker",
                directory
                    .to_str()
                    .context("Python library path must be UTF-8")?,
            ] {
                arguments.extend(["-C".to_string(), format!("link-arg={value}")]);
            }
        }
        Ok(arguments)
    }

    pub(super) fn stage(
        &self,
        manifest: &mut Manifest,
        executable: &NativeArtifact,
        output: &Path,
    ) -> Result<()> {
        ensure!(
            self.checked.inventory.target == executable.target,
            "Python inventory target differs from native build"
        );
        let reference = PythonEnvironmentRef {
            format: 1,
            target: executable.target.clone(),
            executable_sha256: executable.sha256.clone(),
            inventory_sha256: self.checked.header.inventory_sha256.clone(),
            archive_sha256: self.sha256.clone(),
            archive_bytes: self.bytes,
            prefix: self.checked.header.prefix.clone(),
        };
        reference.validate()?;
        let directory = output.join("python");
        std::fs::create_dir_all(&directory)?;
        let destination = directory.join(&reference.archive_sha256);
        if destination.exists() {
            verify_file(&reference, &destination)?;
        } else {
            let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
            std::io::copy(
                &mut std::fs::File::open(&self.archive_path)?,
                temporary.as_file_mut(),
            )?;
            temporary.as_file().sync_all()?;
            verify_file(&reference, temporary.path())?;
            temporary
                .persist_noclobber(&destination)
                .map_err(|error| anyhow::anyhow!(error.error))?;
        }
        manifest
            .native
            .as_mut()
            .context("Python environment requires native executable")?
            .python
            .push(reference);
        if !manifest
            .required_features
            .iter()
            .any(|feature| feature == FEATURE_EMBEDDED_PYTHON_V1)
        {
            manifest
                .required_features
                .push(FEATURE_EMBEDDED_PYTHON_V1.into());
        }
        manifest.version = super::deployment_identity(manifest)?;
        crate::protocol::validate_execution_manifest(manifest)?;
        Ok(())
    }
}

fn identity(path: &Path) -> Result<(String, u64)> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && metadata.len() <= MAX_PYTHON_ARCHIVE_BYTES,
        "Python archive must be a bounded regular file"
    );
    let mut hash = Sha256::new();
    let mut file = std::fs::File::open(path)?;
    let mut buffer = [0u8; 65536];
    let mut bytes = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        bytes += count as u64;
        ensure!(
            bytes <= MAX_PYTHON_ARCHIVE_BYTES,
            "Python archive exceeds byte limit"
        );
        hash.update(&buffer[..count]);
    }
    ensure!(
        bytes == metadata.len(),
        "Python archive changed while reading"
    );
    Ok((format!("{:x}", hash.finalize()), bytes))
}

pub(super) fn verify_file(reference: &PythonEnvironmentRef, path: &Path) -> Result<()> {
    reference.validate()?;
    let (sha256, bytes) = identity(path)?;
    ensure!(
        sha256 == reference.archive_sha256 && bytes == reference.archive_bytes,
        "Python archive digest/size mismatch"
    );
    let checked = archive::inspect(path, None)?;
    archive::verify_reference(reference, &checked)
}

pub(super) fn load_staged(manifest: &Manifest, root: &Path) -> Result<BTreeMap<String, PathBuf>> {
    let mut result = BTreeMap::new();
    for reference in &manifest
        .native
        .as_ref()
        .context("Python environment requires native manifest")?
        .python
    {
        let path = root.join("python").join(&reference.archive_sha256);
        verify_file(reference, &path)?;
        result.insert(reference.archive_sha256.clone(), path);
    }
    Ok(result)
}

pub(super) async fn publish(
    bucket: &Bucket,
    reference: &PythonEnvironmentRef,
    path: &Path,
) -> Result<()> {
    verify_file(reference, path)?;
    let bytes = read_bounded(reference, path)?;
    reference.verify_archive(&bytes)?;
    let key = reference.key()?;
    if let Some((existing, _)) = bucket
        .get_bounded(&key, usize::try_from(reference.archive_bytes)?)
        .await?
    {
        reference.verify_archive(&existing)?;
    } else {
        bucket.put(&key, bytes).await?;
    }
    Ok(())
}

pub(super) async fn install(
    bucket: &Bucket,
    reference: &PythonEnvironmentRef,
    prefix: &Path,
) -> Result<()> {
    ensure!(
        prefix.is_absolute()
            && prefix.components().all(|component| matches!(
                component,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )),
        "--python-root must be an absolute path without traversal"
    );
    let requested = prefix
        .parent()
        .context("Python prefix needs parent")?
        .canonicalize()?
        .join(prefix.file_name().context("Python prefix needs name")?);
    ensure!(
        requested == reference.prefix,
        "--python-root must equal the compiled environment prefix"
    );
    let (bytes, _) = bucket
        .get_bounded(&reference.key()?, usize::try_from(reference.archive_bytes)?)
        .await?
        .context("Python environment archive is absent")?;
    reference.verify_archive(&bytes)?;
    let parent = prefix.parent().context("Python prefix needs parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    archive::install(reference, temporary.path(), &reference.prefix)
}

fn read_bounded(reference: &PythonEnvironmentRef, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(reference.archive_bytes + 1)
        .read_to_end(&mut bytes)?;
    reference.verify_archive(&bytes)?;
    Ok(bytes)
}
