// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Build images once, stage verified archives, publish before selectors.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::container::{ContainerSpec, ImageArchive};
use std::{
    io::Write,
    process::{Command, Stdio},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerProject {
    pub class_name: String,
    /// A Dockerfile relative to the native project, or an image reference.
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_instances: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
}

pub(super) fn validate_projects(config: &ApplicationConfig) -> Result<()> {
    let mut classes = std::collections::BTreeSet::new();
    for container in &config.containers {
        anyhow::ensure!(
            config.cells.contains(&container.class_name) && classes.insert(&container.class_name),
            "container class must name one declared cell class exactly once"
        );
        anyhow::ensure!(
            !container.image.is_empty(),
            "container image must not be empty"
        );
        anyhow::ensure!(
            container
                .instance_type
                .as_deref()
                .is_none_or(|kind| crate::container::instance_resources(kind).is_some()),
            "unknown container instance type"
        );
        anyhow::ensure!(
            container
                .runtime
                .as_deref()
                .is_none_or(|runtime| !runtime.is_empty()),
            "container runtime must not be empty"
        );
    }
    Ok(())
}

fn directory(root: &Path) -> Result<PathBuf> {
    let directory = root.join("images");
    std::fs::create_dir_all(&directory)?;
    anyhow::ensure!(
        std::fs::symlink_metadata(&directory)?.file_type().is_dir(),
        "native image directory must be a regular directory"
    );
    Ok(directory)
}

fn identity(image: &str, path: &Path) -> Result<ImageArchive> {
    let metadata = std::fs::symlink_metadata(path)?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "native image archive must be a regular file"
    );
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        bytes += count as u64;
    }
    anyhow::ensure!(
        bytes == metadata.len(),
        "native image archive changed while reading"
    );
    let archive = ImageArchive {
        image: image.into(),
        sha256: format!("{:x}", hash.finalize()),
        bytes,
    };
    archive.validate()?;
    Ok(archive)
}

pub(super) fn verify_file(archive: &ImageArchive, path: &Path) -> Result<()> {
    archive.validate()?;
    anyhow::ensure!(
        &identity(&archive.image, path)? == archive,
        "native container image archive checksum mismatch"
    );
    Ok(())
}

fn save(image: &str, root: &Path) -> Result<ImageArchive> {
    let directory = directory(root)?;
    let image_key = image
        .strip_prefix("cellar-image:")
        .context("invalid resolved container image")?;
    let index = directory.join(format!("{image_key}.json"));
    if index.try_exists()? {
        anyhow::ensure!(
            std::fs::symlink_metadata(&index)?.file_type().is_file(),
            "native image index must be a regular file"
        );
        let archive: ImageArchive = serde_json::from_slice(&std::fs::read(&index)?)?;
        anyhow::ensure!(
            archive.image == image,
            "native image index does not name the resolved image"
        );
        verify_file(&archive, &directory.join(format!("{}.tar", archive.sha256)))?;
        return Ok(archive);
    }
    let temporary = tempfile::NamedTempFile::new_in(&directory)?;
    let output = Command::new(crate::deploy::engine_cli())
        .arg("save")
        .arg("--output")
        .arg(temporary.path())
        .arg(image)
        .stdin(Stdio::null())
        .output()
        .context("save native container image")?;
    anyhow::ensure!(
        output.status.success(),
        "save native container image failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    temporary.as_file().sync_all()?;
    let archive = identity(image, temporary.path())?;
    let target = directory.join(format!("{}.tar", archive.sha256));
    if target.try_exists()? {
        verify_file(&archive, &target)?;
    } else {
        temporary
            .persist_noclobber(&target)
            .map_err(|error| anyhow::anyhow!(error.error))?;
    }
    let mut staged = tempfile::NamedTempFile::new_in(&directory)?;
    staged.write_all(&serde_json::to_vec(&archive)?)?;
    staged.as_file().sync_all()?;
    staged
        .persist_noclobber(index)
        .map_err(|error| anyhow::anyhow!(error.error))?;
    Ok(archive)
}

pub(super) fn stage(
    config: &ApplicationConfig,
    manifest: &mut Manifest,
    project_root: &Path,
    root: &Path,
    dev: bool,
) -> Result<()> {
    if config.containers.is_empty() {
        return Ok(());
    }
    let native = manifest.native.as_ref().unwrap();
    let architectures: std::collections::BTreeSet<_> = native
        .artifacts
        .iter()
        .map(|artifact| artifact.target.split('-').next().unwrap())
        .collect();
    anyhow::ensure!(
        architectures.len() == 1,
        "native container packages require one executable architecture"
    );
    let platform = if dev {
        None
    } else {
        Some(
            std::env::var("CELLAR_CONTAINER_PLATFORM").unwrap_or_else(|_| {
                if manifest.native.as_ref().unwrap().artifacts[0]
                    .target
                    .starts_with("aarch64-")
                {
                    "linux/arm64"
                } else {
                    "linux/amd64"
                }
                .into()
            }),
        )
    };
    let mut archives = BTreeMap::new();
    for container in &config.containers {
        let image =
            crate::deploy::resolve_image(project_root, &container.image, platform.as_deref())?;
        archives.insert(image.clone(), save(&image, root)?);
        manifest.containers.push(ContainerSpec {
            class_name: container.class_name.clone(),
            image,
            instance_type: container.instance_type.clone(),
            max_instances: container.max_instances,
            runtime: container.runtime.clone(),
        });
    }
    let fence = crate::deploy::resolve_fence_image(platform.as_deref())?;
    archives.insert(fence.clone(), save(&fence, root)?);
    manifest.fence_image = Some(fence);
    manifest.container_images = archives.into_values().collect();
    manifest.required_features.extend([
        crate::protocol::FEATURE_CONTAINERS_V1.into(),
        crate::protocol::FEATURE_CONTAINER_IMAGES_V1.into(),
    ]);
    manifest.required_features.sort();
    crate::protocol::validate_execution_manifest(manifest)?;
    Ok(())
}

pub(super) fn load_staged(manifest: &Manifest, root: &Path) -> Result<BTreeMap<String, PathBuf>> {
    let mut files = BTreeMap::new();
    if manifest.container_images.is_empty() {
        return Ok(files);
    }
    let directory = directory(root)?;
    for archive in &manifest.container_images {
        let path = directory.join(format!("{}.tar", archive.sha256));
        verify_file(archive, &path)?;
        files.insert(archive.sha256.clone(), path);
    }
    Ok(files)
}

pub(super) async fn publish(bucket: &Bucket, archive: &ImageArchive, path: &Path) -> Result<()> {
    use crate::bucket::{BlobAttributes, BlobConditions, BlobRange, BlobRead};
    use futures_util::StreamExt;
    let key = archive.key()?;
    match bucket
        .get_blob(&key, BlobRange::Whole, &BlobConditions::default())
        .await?
    {
        BlobRead::Hit(mut existing) => {
            anyhow::ensure!(
                existing.meta.size == archive.bytes,
                "stored native image archive length mismatch"
            );
            let mut hash = Sha256::new();
            while let Some(chunk) = existing.body.next().await {
                hash.update(chunk.map_err(anyhow::Error::msg)?);
            }
            anyhow::ensure!(
                format!("{:x}", hash.finalize()) == archive.sha256,
                "stored native image archive checksum mismatch"
            );
            return Ok(());
        }
        BlobRead::Missing => {}
        BlobRead::Unmet(_) => bail!("unexpected native image precondition result"),
    }
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut upload = crate::blob_store::UploadGuard::new(
        bucket
            .begin_multipart(&key, &BlobAttributes::default())
            .await?,
    );
    let mut hash = Sha256::new();
    let mut bytes = 0u64;
    loop {
        let mut buffer = vec![0u8; 8 * 1024 * 1024];
        let mut count = 0;
        while count < buffer.len() {
            let read = file.read(&mut buffer[count..]).await?;
            if read == 0 {
                break;
            }
            count += read;
        }
        if count == 0 {
            break;
        }
        buffer.truncate(count);
        bytes += count as u64;
        anyhow::ensure!(
            bytes <= archive.bytes,
            "native image archive grew during publication"
        );
        hash.update(&buffer);
        upload.put_part(buffer.into()).await?;
    }
    anyhow::ensure!(
        bytes == archive.bytes && format!("{:x}", hash.finalize()) == archive.sha256,
        "native image archive changed during publication"
    );
    upload.complete().await?;
    Ok(())
}
