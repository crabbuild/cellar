//! Data-only identities for Node runtime resources. The native host links no engine.
use super::python_environment::{normal_absolute, valid_digest};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const FEATURE_NODE_V1: &str = "node-runtime-v1";
pub const MAX_NODE_INVENTORY_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_NODE_FILES: usize = 200_000;
pub const MAX_NODE_ARCHIVE_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_NODE_EXPANDED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeEnvironmentRef {
    pub format: u32,
    pub target: String,
    pub executable_sha256: String,
    pub inventory_sha256: String,
    pub archive_sha256: String,
    pub archive_bytes: u64,
    pub prefix: PathBuf,
}

impl NodeEnvironmentRef {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format == 1
                && !self.target.is_empty()
                && self
                    .target
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid Node environment format/target"
        );
        ensure!(
            [
                &self.executable_sha256,
                &self.inventory_sha256,
                &self.archive_sha256
            ]
            .into_iter()
            .all(|s| valid_digest(s)),
            "invalid Node environment SHA-256"
        );
        ensure!(
            (1..=MAX_NODE_ARCHIVE_BYTES).contains(&self.archive_bytes),
            "Node archive exceeds byte limit"
        );
        ensure!(
            normal_absolute(&self.prefix) && self.prefix.parent().is_some(),
            "invalid Node environment prefix"
        );
        Ok(())
    }

    pub fn key(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "deploy-blobs/node/sha256/{}/{}",
            &self.archive_sha256[..2],
            self.archive_sha256
        ))
    }

    pub fn verify_archive(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        ensure!(
            bytes.len() as u64 == self.archive_bytes
                && format!("{:x}", Sha256::digest(bytes)) == self.archive_sha256,
            "Node archive digest/size mismatch"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFile {
    pub bytes: u64,
    pub sha256: String,
    pub executable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeInventory {
    pub format: u32,
    pub target: String,
    pub version: String,
    pub platform: String,
    pub arch: String,
    pub prefix: PathBuf,
    pub executable: PathBuf,
    pub entry: PathBuf,
    pub working_directory: PathBuf,
    pub files: BTreeMap<PathBuf, NodeFile>,
}

impl NodeInventory {
    pub fn validate(&self, prefix: &Path) -> Result<()> {
        ensure!(
            self.format == 1
                && prefix == self.prefix
                && normal_absolute(prefix)
                && prefix.parent().is_some(),
            "invalid Node inventory prefix/format"
        );
        let version: Vec<_> = self.version.split('.').collect();
        ensure!(
            version.len() == 3
                && version
                    .iter()
                    .all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                && version[0].parse::<u32>()? >= 24,
            "Node inventory requires Node 24 or later"
        );
        let architecture = match self.arch.as_str() {
            "arm64" => "aarch64",
            "x64" => "x86_64",
            _ => anyhow::bail!("unsupported Node architecture"),
        };
        let suffix = match self.platform.as_str() {
            "darwin" => "apple-darwin",
            "linux" => "unknown-linux-gnu",
            _ => anyhow::bail!("unsupported Node platform"),
        };
        ensure!(
            self.target == format!("{architecture}-{suffix}"),
            "Node inventory target mismatch"
        );
        ensure!(
            !self.files.is_empty() && self.files.len() <= MAX_NODE_FILES,
            "Node inventory file limit"
        );
        for path in [&self.executable, &self.entry, &self.working_directory] {
            ensure!(
                normal_absolute(path) && path.starts_with(prefix),
                "Node layout escapes prefix"
            );
        }
        ensure!(
            self.files.contains_key(&self.entry)
                && self
                    .files
                    .get(&self.executable)
                    .is_some_and(|f| f.executable),
            "Node inventory needs executable runtime and entry module"
        );
        ensure!(
            !self.files.contains_key(&self.working_directory)
                && self
                    .files
                    .keys()
                    .any(|p| p.starts_with(&self.working_directory)),
            "Node working directory is not inventoried"
        );
        let mut expanded = 0u64;
        for (path, file) in &self.files {
            ensure!(
                normal_absolute(path)
                    && path.starts_with(prefix)
                    && path != prefix
                    // Match capture's depth bound before directory creation
                    // and recursive syncing of a downloaded archive.
                    && path.strip_prefix(prefix)?.components().count() <= 64
                    && valid_digest(&file.sha256),
                "invalid Node inventory file"
            );
            let mut parent = path.parent();
            while let Some(path) = parent {
                if path == prefix {
                    break;
                }
                ensure!(
                    !self.files.contains_key(path),
                    "Node file conflicts with parent directory"
                );
                parent = path.parent();
            }
            expanded = expanded
                .checked_add(file.bytes)
                .ok_or_else(|| anyhow::anyhow!("Node expanded-byte overflow"))?;
            ensure!(
                expanded <= MAX_NODE_EXPANDED_BYTES,
                "Node expanded-byte limit"
            );
        }
        Ok(())
    }
}
