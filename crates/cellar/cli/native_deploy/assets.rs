// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native delivery uses the retained asset builder, index and blob protocol.

use super::*;
use crate::deploy::BuiltAssets;
use crate::protocol::{asset_blob_key, AssetIndex, AssetManifestRef, RunWorkerFirst};
use std::io::Write;

// Leave room for 20,000 maximally escaped 1024-byte paths and MIME metadata.
const MAX_INDEX_BYTES: u64 = 256 * 1024 * 1024;

/// Paths are relative to the native project file. _headers and _redirects are
/// read from this directory with the same rules as source deployments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetProject {
    pub directory: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html_handling: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_found_handling: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_worker_first: Option<RunWorkerFirst>,
}

pub(super) fn validate_reference(reference: &AssetManifestRef) -> Result<()> {
    anyhow::ensure!(
        reference.index == "assets.json",
        "unsupported native asset index name"
    );
    anyhow::ensure!(
        asset_blob_key(&reference.sha256).is_some(),
        "invalid native asset index digest"
    );
    Ok(())
}

fn decode_index(bytes: &[u8], reference: &AssetManifestRef) -> Result<AssetIndex> {
    validate_reference(reference)?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_INDEX_BYTES,
        "native asset index exceeds 256 MiB"
    );
    anyhow::ensure!(
        format!("{:x}", Sha256::digest(bytes)) == reference.sha256,
        "native asset index checksum mismatch"
    );
    let index: AssetIndex = serde_json::from_slice(bytes).context("decode native asset index")?;
    crate::assets::validate_index(&index, reference, crate::env_vars::max_asset_file_bytes()?)?;
    crate::assets::validate_directives(&index.config)?;
    Ok(index)
}

fn read_payload(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect staged resource {}", path.display()))?;
    anyhow::ensure!(
        metadata.file_type().is_file(),
        "staged resource must be a regular file: {}",
        path.display()
    );
    anyhow::ensure!(
        metadata.len() <= limit,
        "staged resource exceeds its size limit: {}",
        path.display()
    );
    // A bounded read also covers growth after the metadata check.
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 == metadata.len() && bytes.len() as u64 <= limit,
        "staged resource changed while being read: {}",
        path.display()
    );
    Ok(bytes)
}

fn resource_directory(directory: &Path) -> Result<PathBuf> {
    let resources = directory.join("assets");
    anyhow::ensure!(
        std::fs::symlink_metadata(&resources)?.file_type().is_dir(),
        "native asset staging directory must be a regular directory"
    );
    Ok(resources)
}

fn stage_payload(directory: &Path, sha256: &str, bytes: &[u8]) -> Result<()> {
    anyhow::ensure!(
        asset_blob_key(sha256).is_some(),
        "invalid staged asset digest"
    );
    let path = directory.join(sha256);
    if path.try_exists()? {
        anyhow::ensure!(
            read_payload(&path, bytes.len() as u64)? == bytes,
            "existing staged asset checksum mismatch"
        );
        return Ok(());
    }
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| anyhow::anyhow!(error.error))?;
    Ok(())
}

impl ApplicationConfig {
    /// Build and stage declared assets, then hash their index into the native
    /// deployment identity. Executables are staged separately by Project::build.
    pub fn stage_manifest(
        &self,
        artifacts: Vec<NativeArtifact>,
        project_root: &Path,
        directory: &Path,
    ) -> Result<Manifest> {
        self.stage_manifest_mode(artifacts, project_root, directory, false)
    }

    pub(super) fn stage_manifest_mode(
        &self,
        artifacts: Vec<NativeArtifact>,
        project_root: &Path,
        directory: &Path,
        dev: bool,
    ) -> Result<Manifest> {
        let mut manifest = self.base_manifest(artifacts)?;
        images::stage(self, &mut manifest, project_root, directory, dev)?;
        let Some(project) = &self.assets else {
            manifest.version = deployment_identity(&manifest)?;
            return Ok(manifest);
        };
        let (built, raw_metadata) = crate::deploy::build_project_assets(
            &serde_json::to_value(project)?,
            project_root,
            manifest
                .raw_metadata
                .as_object()
                .context("native metadata is not an object")?,
        )?;
        let reference = AssetManifestRef {
            index: "assets.json".into(),
            sha256: format!("{:x}", Sha256::digest(&built.index)),
            file_count: built.file_count,
            total_bytes: built.total_bytes,
        };
        decode_index(&built.index, &reference)?;
        std::fs::create_dir_all(directory.join("assets"))?;
        let resources = resource_directory(directory)?;
        for (sha256, body) in &built.blobs {
            stage_payload(&resources, sha256, body)?;
        }
        stage_payload(&resources, &reference.sha256, &built.index)?;
        manifest.assets = Some(reference);
        manifest
            .raw_metadata
            .as_object_mut()
            .unwrap()
            .insert("assets".into(), raw_metadata);
        manifest
            .required_features
            .push(crate::protocol::FEATURE_ASSETS_V1.into());
        manifest.required_features.sort();
        manifest.version = deployment_identity(&manifest)?;
        Ok(manifest)
    }
}

pub(super) fn load_staged(manifest: &Manifest, directory: &Path) -> Result<Option<BuiltAssets>> {
    let Some(reference) = &manifest.assets else {
        return Ok(None);
    };
    validate_reference(reference)?;
    let resources = resource_directory(directory)?;
    let bytes = read_payload(&resources.join(&reference.sha256), MAX_INDEX_BYTES)?;
    let index = decode_index(&bytes, reference)?;
    let mut blobs: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    for (path, entry) in &index.entries {
        if let Some(body) = blobs.get(&entry.sha256) {
            anyhow::ensure!(
                body.len() as u64 == entry.bytes,
                "native asset length mismatch: {path}"
            );
            continue;
        }
        let body = read_payload(&resources.join(&entry.sha256), entry.bytes)?;
        anyhow::ensure!(
            body.len() as u64 == entry.bytes
                && format!("{:x}", Sha256::digest(&body)) == entry.sha256,
            "native asset checksum mismatch: {path}"
        );
        blobs.insert(entry.sha256.clone(), body);
    }
    Ok(Some(BuiltAssets {
        index: bytes,
        blobs,
        file_count: reference.file_count,
        total_bytes: reference.total_bytes,
    }))
}
