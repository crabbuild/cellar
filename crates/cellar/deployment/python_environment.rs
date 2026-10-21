//! Data-only identities for immutable embedded-Python deployment resources.
//! No interpreter or Python API is linked into the deployment protocol.
use super::python_prerequisites::PythonPrerequisites;
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
};

pub const FEATURE_EMBEDDED_PYTHON_V1: &str = "embedded-python-v1";
pub const MAX_PYTHON_INVENTORY_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_PYTHON_FILES: usize = 200_000;
pub const MAX_PYTHON_ARCHIVE_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_PYTHON_EXPANDED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// An environment archive paired with one exact native executable artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonEnvironmentRef {
    pub format: u32,
    pub target: String,
    pub executable_sha256: String,
    pub inventory_sha256: String,
    pub archive_sha256: String,
    pub archive_bytes: u64,
    /// Immutable absolute provisioning path captured by the compiled host.
    pub prefix: PathBuf,
}

impl PythonEnvironmentRef {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.format == 1, "unsupported Python environment format");
        ensure!(
            !self.target.is_empty()
                && self
                    .target
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid Python target"
        );
        for digest in [
            &self.executable_sha256,
            &self.inventory_sha256,
            &self.archive_sha256,
        ] {
            ensure!(valid_digest(digest), "invalid Python artifact SHA-256");
        }
        ensure!(
            (1..=MAX_PYTHON_ARCHIVE_BYTES).contains(&self.archive_bytes),
            "Python archive exceeds byte limit"
        );
        ensure!(
            normal_absolute(&self.prefix) && self.prefix.parent().is_some(),
            "invalid Python environment prefix"
        );
        Ok(())
    }

    pub fn key(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "deploy-blobs/python/sha256/{}/{}",
            &self.archive_sha256[..2],
            self.archive_sha256
        ))
    }

    pub fn verify_archive(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        ensure!(
            bytes.len() as u64 == self.archive_bytes
                && format!("{:x}", Sha256::digest(bytes)) == self.archive_sha256,
            "Python environment archive digest/size mismatch"
        );
        Ok(())
    }
}

/// Exact provisioned files and application registrations bound into a host.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonInventory {
    pub format: u32,
    pub target: String,
    pub version: String,
    pub soabi: String,
    pub home: PathBuf,
    pub executable: PathBuf,
    pub standard_library_paths: Vec<PathBuf>,
    pub module_paths: Vec<PathBuf>,
    pub roots: Vec<PathBuf>,
    pub source: PythonSourceInventory,
    pub files: BTreeMap<PathBuf, PythonFile>,
    pub prerequisites: PythonPrerequisites,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PythonSourceInventory {
    pub sha256: String,
    pub filename: String,
    pub descriptor: serde_json::Value,
    pub distributions: Vec<(String, String)>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum PythonFile {
    File {
        bytes: u64,
        sha256: String,
        executable: bool,
    },
    Symlink {
        target: PathBuf,
    },
}

impl PythonInventory {
    /// Validate bounded paths and identities without consulting the filesystem.
    pub fn validate(&self, prefix: &Path) -> Result<()> {
        ensure!(
            self.format == 1 && !self.target.is_empty(),
            "invalid Python inventory format/target"
        );
        ensure!(
            self.version.starts_with("3.14.")
                && (self.soabi.starts_with("cpython-314-")
                    || self.soabi.starts_with("cpython-314t-"))
                && !self.soabi.contains('\0'),
            "inventory requires CPython 3.14 or free-threaded CPython 3.14t"
        );
        ensure!(
            normal_absolute(prefix) && prefix.parent().is_some(),
            "invalid Python prefix"
        );
        ensure!(
            !self.roots.is_empty()
                && self.roots.len() <= 32
                && !self.files.is_empty()
                && self.files.len() <= MAX_PYTHON_FILES,
            "Python inventory root/file limit"
        );
        ensure!(
            !self.standard_library_paths.is_empty()
                && self.standard_library_paths.len() <= 32
                && self.module_paths.len() <= 32,
            "Python inventory import-root limit"
        );
        for root in &self.roots {
            ensure!(
                normal_absolute(root) && root.starts_with(prefix),
                "Python inventory root escapes prefix"
            );
        }
        for path in std::iter::once(&self.home)
            .chain([&self.executable])
            .chain(&self.standard_library_paths)
            .chain(&self.module_paths)
        {
            ensure!(
                normal_absolute(path) && self.roots.iter().any(|root| path.starts_with(root)),
                "Python layout escapes inventory roots"
            );
        }
        ensure!(
            valid_digest(&self.source.sha256) && !self.source.filename.is_empty(),
            "invalid Python source inventory"
        );
        let mut expanded = 0u64;
        for (path, entry) in &self.files {
            ensure!(
                normal_absolute(path)
                    && self.roots.iter().any(|root| path.starts_with(root))
                    && self.roots.iter().all(|root| path != root),
                "Python file escapes inventory roots"
            );
            match entry {
                PythonFile::File { bytes, sha256, .. } => {
                    ensure!(valid_digest(sha256), "invalid Python file SHA-256");
                    expanded = expanded
                        .checked_add(*bytes)
                        .ok_or_else(|| anyhow::anyhow!("Python expanded-byte overflow"))?;
                    ensure!(
                        expanded <= MAX_PYTHON_EXPANDED_BYTES,
                        "Python expanded-byte limit"
                    );
                }
                PythonFile::Symlink { target } => {
                    ensure!(!target.as_os_str().is_empty(), "empty Python symlink");
                    let destination = if target.is_absolute() {
                        target.clone()
                    } else {
                        path.parent().unwrap().join(target)
                    };
                    let destination = normalize(&destination)?;
                    ensure!(
                        self.roots.iter().any(|root| destination.starts_with(root)),
                        "Python symlink escapes inventory roots"
                    );
                }
            }
        }
        self.prerequisites
            .validate(&self.target, &self.roots, &self.files)?;
        Ok(())
    }
}

pub(crate) fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub(crate) fn normal_absolute(path: &Path) -> bool {
    path.to_str().is_some_and(|value| !value.contains('\0'))
        && path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_)))
}

fn normalize(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_absolute(),
        "Python symlink needs absolute destination"
    );
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => result.push(component.as_os_str()),
            Component::Normal(value) => result.push(value),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(result.pop(), "Python symlink escapes root");
            }
            _ => anyhow::bail!("unsupported Python path prefix"),
        }
    }
    Ok(result)
}
