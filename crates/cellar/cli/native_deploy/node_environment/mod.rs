// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Build/install filesystem resources outside node execution domains.
#![allow(clippy::disallowed_methods)]
mod archive;
mod files;
pub use files::verify_inventory_files;

use crate::{
    bucket::Bucket,
    protocol::{
        Manifest, NativeArtifact, NodeEnvironmentRef, FEATURE_NODE_V1, MAX_NODE_ARCHIVE_BYTES,
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

/// An immutable runtime directory prepared before Cargo. Paths are relative to
/// the project; entry, executable and working directory are relative to root.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeProject {
    pub root: PathBuf,
    pub executable: PathBuf,
    pub entry: PathBuf,
    pub working_directory: PathBuf,
}

pub(super) struct Prepared {
    pub inventory_path: PathBuf,
    archive_path: PathBuf,
    checked: archive::Checked,
    sha256: String,
    bytes: u64,
}

impl NodeProject {
    pub(super) fn prepare(&self, base: &Path, output: &Path, target: &str) -> Result<Prepared> {
        let prefix = base.join(&self.root).canonicalize()?;
        ensure!(prefix.is_dir(), "Node runtime root must be a directory");
        let inside = |path: &Path| -> Result<PathBuf> {
            ensure!(
                !path.as_os_str().is_empty()
                    && path
                        .components()
                        .all(|c| matches!(c, std::path::Component::Normal(_))),
                "Node project paths must be contained relative paths"
            );
            Ok(prefix.join(path))
        };
        let executable = inside(&self.executable)?;
        let probe = std::process::Command::new(&executable)
            .env_remove("NODE_OPTIONS")
            .env_remove("NODE_PATH")
            .args([
                "--eval",
                "process.stdout.write(JSON.stringify({version:process.versions.node,platform:process.platform,arch:process.arch}))",
            ])
            .output()?;
        ensure!(
            probe.status.success() && probe.stdout.len() <= 4096,
            "Node runtime probe failed"
        );
        let info: serde_json::Value = serde_json::from_slice(&probe.stdout)?;
        let field = |key: &str| {
            info[key]
                .as_str()
                .map(String::from)
                .ok_or_else(|| anyhow::anyhow!("Node runtime identity missing"))
        };
        let inventory = crate::protocol::NodeInventory {
            format: 1,
            target: target.into(),
            version: field("version")?,
            platform: field("platform")?,
            arch: field("arch")?,
            prefix: prefix.clone(),
            executable,
            entry: inside(&self.entry)?,
            working_directory: inside(&self.working_directory)?,
            files: files::capture(&prefix)?,
        };
        inventory.validate(&prefix)?;
        std::fs::create_dir_all(output)?;
        let output = output.canonicalize()?;
        ensure!(
            !output.starts_with(&prefix),
            "Node package output must be outside runtime prefix"
        );
        let directory = output.join("node");
        std::fs::create_dir_all(&directory)?;
        let raw = serde_json::to_vec(&inventory)?;
        ensure!(
            raw.len() <= crate::protocol::MAX_NODE_INVENTORY_BYTES,
            "Node inventory byte limit"
        );
        let inventory_sha256 = format!("{:x}", Sha256::digest(&raw));
        let inventory_path = directory.join(format!("{inventory_sha256}.json"));
        write_new(&inventory_path, &raw)?;
        let archive_path = capture_archive(&inventory, &raw, &inventory_sha256, &directory)?;
        let (sha256, bytes) = identity(&archive_path)?;
        let checked = archive::inspect(&archive_path, None)?;
        ensure!(
            checked.inventory_bytes == raw,
            "Node captured inventory differs from archive"
        );
        Ok(Prepared {
            inventory_path,
            archive_path,
            checked,
            sha256,
            bytes,
        })
    }
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        ensure!(
            std::fs::read(path)? == bytes,
            "runtime metadata changed at immutable path"
        );
        return Ok(());
    }
    use std::io::Write;
    let mut file =
        tempfile::NamedTempFile::new_in(path.parent().context("metadata parent missing")?)?;
    file.write_all(bytes)?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path)
        .map_err(|e| anyhow::anyhow!(e.error))?;
    Ok(())
}

fn capture_archive(
    inventory: &crate::protocol::NodeInventory,
    raw: &[u8],
    inventory_sha256: &str,
    directory: &Path,
) -> Result<PathBuf> {
    let temporary = tempfile::NamedTempFile::new_in(directory)?;
    let output = flate2::write::GzEncoder::new(temporary.reopen()?, flate2::Compression::fast());
    let mut writer = tar::Builder::new(output);

    fn append(
        writer: &mut tar::Builder<flate2::write::GzEncoder<std::fs::File>>,
        name: &str,
        bytes: u64,
        input: impl std::io::Read,
    ) -> Result<()> {
        let mut header = tar::Header::new_ustar();
        header.set_size(bytes);
        header.set_mode(0o444);
        header.set_cksum();
        writer.append_data(&mut header, name, input)?;
        Ok(())
    }
    let header = serde_json::to_vec(&archive::Header {
        format: 1,
        prefix: inventory.prefix.clone(),
        inventory_sha256: inventory_sha256.into(),
    })?;
    append(
        &mut writer,
        "environment.json",
        header.len() as u64,
        header.as_slice(),
    )?;
    append(&mut writer, "inventory.json", raw.len() as u64, raw)?;
    let mut seen = std::collections::BTreeSet::new();
    for (path, file) in &inventory.files {
        if seen.insert(&file.sha256) {
            append(
                &mut writer,
                &format!("blobs/{}", file.sha256),
                file.bytes,
                std::fs::File::open(path)?,
            )?;
        }
    }
    writer.into_inner()?.finish()?.sync_all()?;
    let (sha, _) = identity(temporary.path())?;
    let destination = directory.join(sha);
    if destination.exists() {
        ensure!(
            identity(&destination)? == identity(temporary.path())?,
            "Node archive immutable path changed"
        );
    } else {
        temporary
            .persist_noclobber(&destination)
            .map_err(|e| anyhow::anyhow!(e.error))?;
    }
    Ok(destination)
}

impl Prepared {
    pub(super) fn stage(
        &self,
        manifest: &mut Manifest,
        executable: &NativeArtifact,
        output: &Path,
    ) -> Result<()> {
        verify_inventory_files(&self.checked.inventory)?;
        ensure!(
            self.checked.inventory.target == executable.target,
            "Node inventory target differs from native build"
        );
        let reference = NodeEnvironmentRef {
            format: 1,
            target: executable.target.clone(),
            executable_sha256: executable.sha256.clone(),
            inventory_sha256: self.checked.header.inventory_sha256.clone(),
            archive_sha256: self.sha256.clone(),
            archive_bytes: self.bytes,
            prefix: self.checked.header.prefix.clone(),
        };
        reference.validate()?;
        let directory = output.join("node");
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
            .context("Node environment requires native executable")?
            .node
            .push(reference);
        if !manifest
            .required_features
            .iter()
            .any(|feature| feature == FEATURE_NODE_V1)
        {
            manifest.required_features.push(FEATURE_NODE_V1.into());
        }
        manifest.version = super::deployment_identity(manifest)?;
        crate::protocol::validate_execution_manifest(manifest)?;
        Ok(())
    }
}

fn identity(path: &Path) -> Result<(String, u64)> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file() && metadata.len() <= MAX_NODE_ARCHIVE_BYTES,
        "Node archive must be a bounded regular file"
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
            bytes <= MAX_NODE_ARCHIVE_BYTES,
            "Node archive exceeds byte limit"
        );
        hash.update(&buffer[..count]);
    }
    ensure!(
        bytes == metadata.len(),
        "Node archive changed while reading"
    );
    Ok((format!("{:x}", hash.finalize()), bytes))
}

pub(super) fn verify_file(reference: &NodeEnvironmentRef, path: &Path) -> Result<()> {
    reference.validate()?;
    let (sha256, bytes) = identity(path)?;
    ensure!(
        sha256 == reference.archive_sha256 && bytes == reference.archive_bytes,
        "Node archive digest/size mismatch"
    );
    let checked = archive::inspect(path, None)?;
    archive::verify_reference(reference, &checked)
}

pub(super) fn load_staged(manifest: &Manifest, root: &Path) -> Result<BTreeMap<String, PathBuf>> {
    let mut result = BTreeMap::new();
    for reference in &manifest
        .native
        .as_ref()
        .context("Node environment requires native manifest")?
        .node
    {
        let path = root.join("node").join(&reference.archive_sha256);
        verify_file(reference, &path)?;
        result.insert(reference.archive_sha256.clone(), path);
    }
    Ok(result)
}

pub(super) async fn publish(
    bucket: &Bucket,
    reference: &NodeEnvironmentRef,
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
    reference: &NodeEnvironmentRef,
    prefix: &Path,
) -> Result<()> {
    ensure!(
        prefix.is_absolute()
            && prefix.components().all(|component| matches!(
                component,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )),
        "--node-root must be an absolute path without traversal"
    );
    let requested = prefix
        .parent()
        .context("Node prefix needs parent")?
        .canonicalize()?
        .join(prefix.file_name().context("Node prefix needs name")?);
    ensure!(
        requested == reference.prefix,
        "--node-root must equal the compiled environment prefix"
    );
    let (bytes, _) = bucket
        .get_bounded(&reference.key()?, usize::try_from(reference.archive_bytes)?)
        .await?
        .context("Node environment archive is absent")?;
    reference.verify_archive(&bytes)?;
    let parent = prefix.parent().context("Node prefix needs parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    use std::io::Write;
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    archive::install(reference, temporary.path(), &reference.prefix)
}

fn read_bounded(reference: &NodeEnvironmentRef, path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(reference.archive_bytes + 1)
        .read_to_end(&mut bytes)?;
    reference.verify_archive(&bytes)?;
    Ok(bytes)
}

#[cfg(all(test, unix))]
#[path = "tests.rs"]
mod tests;
