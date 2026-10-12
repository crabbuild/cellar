//! Rust executable packaging and publication over the retained bucket protocol.

// Build tools and executable inspection run outside cell execution domains.
#![allow(clippy::disallowed_methods)]

use crate::bucket::Bucket;
use crate::protocol::{
    Manifest, NativeArtifact, NativeDeployment, FEATURE_NATIVE_V1, NATIVE_HOST_PROTOCOL,
};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

mod build;
mod images;
pub use build::{verify_native_dependencies, Project};
pub use images::ContainerProject;
mod assets;
pub mod cli;
pub mod dev;
pub use assets::AssetProject;

pub fn current_target() -> &'static str {
    env!("CELLAR_NATIVE_TARGET")
}

/// Hash an executable without allocating its entire contents at node boot.
pub fn executable_identity(path: &Path, target: &str) -> Result<NativeArtifact> {
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("open native executable {}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut bytes = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        bytes += read as u64;
    }
    let artifact = NativeArtifact {
        target: target.into(),
        sha256: format!("{:x}", hash.finalize()),
        bytes,
    };
    artifact.key()?;
    anyhow::ensure!(bytes > 0, "native executable must not be empty");
    Ok(artifact)
}

pub(crate) fn current_executable_identity() -> Result<&'static NativeArtifact> {
    static IDENTITY: OnceLock<std::result::Result<NativeArtifact, String>> = OnceLock::new();
    IDENTITY
        .get_or_init(|| {
            std::env::current_exe()
                .context("locate running executable")
                .and_then(|path| executable_identity(&path, current_target()))
                .map_err(|error| format!("{error:#}"))
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))
}

/// Native application configuration. Binding records use the retained host
/// binding protocol; native packages share its services and namespace keys.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    pub script_name: String,
    pub application_version: String,
    #[serde(default)]
    pub cells: Vec<String>,
    #[serde(default)]
    pub bindings: Vec<serde_json::Value>,
    #[serde(default)]
    pub compatibility_date: Option<String>,
    #[serde(default)]
    pub compatibility_flags: Vec<String>,
    #[serde(default)]
    pub crons: Vec<String>,
    #[serde(default)]
    pub queue_consumers: Vec<crate::protocol::QueueConsumerConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets: Option<AssetProject>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub containers: Vec<images::ContainerProject>,
}

impl ApplicationConfig {
    pub fn manifest(&self, artifacts: Vec<NativeArtifact>) -> Result<Manifest> {
        anyhow::ensure!(
            self.assets.is_none() && self.containers.is_empty(),
            "asset/container configuration requires stage_manifest or a native project build"
        );
        self.base_manifest(artifacts)
    }

    fn base_manifest(&self, artifacts: Vec<NativeArtifact>) -> Result<Manifest> {
        images::validate_projects(self)?;
        anyhow::ensure!(
            !self.script_name.is_empty()
                && self
                    .script_name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
            "invalid native application script name"
        );
        let mut classes = self.cells.clone();
        let mut names = std::collections::BTreeSet::new();
        for binding in &self.bindings {
            let kind = binding
                .get("type")
                .and_then(serde_json::Value::as_str)
                .context("binding requires a type")?;
            let name = binding
                .get("name")
                .and_then(serde_json::Value::as_str)
                .context("binding requires a name")?;
            anyhow::ensure!(
                !name.is_empty() && names.insert(name),
                "empty or duplicate binding name {name}"
            );
            let required = match kind {
                "durable_object_namespace" => "class_name",
                "service" => "service",
                "blob_bucket" => "bucket_name",
                "sql" => {
                    classes.push(crate::deploy::SQL_CLASS.into());
                    "database_id"
                }
                "kv" => {
                    classes.push(crate::deploy::KV_CLASS.into());
                    "id"
                }
                "queue" => {
                    classes.push(crate::deploy::QUEUE_CLASS.into());
                    "queue"
                }
                "workflow" => {
                    classes.push(crate::deploy::workflow_class(&self.script_name));
                    anyhow::ensure!(
                        binding
                            .get("workflow_name")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|v| !v.is_empty()),
                        "binding {name} requires workflow_name"
                    );
                    "class_name"
                }
                "worker_loader" => continue,
                "plain_text" => "text",
                other => bail!("unsupported native binding type {other}"),
            };
            anyhow::ensure!(
                binding
                    .get(required)
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|v| required == "text" || !v.is_empty()),
                "binding {name} requires {required}"
            );
        }
        if !self.queue_consumers.is_empty() {
            classes.push(crate::deploy::QUEUE_CLASS.into());
        }
        classes.sort();
        classes.dedup();
        anyhow::ensure!(
            self.cells.iter().all(|c| !c.is_empty()),
            "native cell class must not be empty"
        );
        let mut bindings = self.bindings.clone();
        if let Some(name) = self
            .assets
            .as_ref()
            .and_then(|assets| assets.binding.as_ref())
        {
            anyhow::ensure!(
                names.insert(name.as_str()),
                "duplicate asset binding name {name}"
            );
            bindings.push(serde_json::json!({"type":"assets", "name":name}));
        }
        let mut metadata = serde_json::Map::new();
        metadata.insert("bindings".into(), serde_json::json!(bindings));
        if let Some(date) = &self.compatibility_date {
            metadata.insert("compatibility_date".into(), date.clone().into());
        }
        metadata.insert(
            "compatibility_flags".into(),
            serde_json::json!(self.compatibility_flags),
        );
        let mut artifacts = artifacts;
        artifacts.sort_by(|a, b| (&a.target, &a.sha256).cmp(&(&b.target, &b.sha256)));
        let native = NativeDeployment {
            host_protocol: NATIVE_HOST_PROTOCOL,
            application_version: self.application_version.clone(),
            artifacts,
        };
        let mut manifest = Manifest {
            schema_version: 3,
            version: String::new(),
            script_name: self.script_name.clone(),
            main_module: None,
            native: Some(native),
            do_classes: classes.clone(),
            sqlite_classes: classes,
            modules: Vec::new(),
            assets: None,
            crons: self.crons.clone(),
            queue_consumers: self.queue_consumers.clone(),
            containers: Vec::new(),
            fence_image: None,
            container_images: Vec::new(),
            required_features: vec![FEATURE_NATIVE_V1.into()],
            raw_metadata: metadata.into(),
        };
        for (kind, feature) in [
            ("sql", crate::protocol::FEATURE_SQL_V1),
            ("kv", crate::protocol::FEATURE_KV_V1),
            ("queue", crate::protocol::FEATURE_QUEUES_V1),
            ("workflow", crate::protocol::FEATURE_WORKFLOWS_V1),
            ("blob_bucket", crate::protocol::FEATURE_BLOB_V1),
        ] {
            if self
                .bindings
                .iter()
                .any(|binding| binding["type"].as_str() == Some(kind))
            {
                manifest.required_features.push(feature.into());
            }
        }
        if !self.queue_consumers.is_empty()
            && !manifest
                .required_features
                .iter()
                .any(|f| f == crate::protocol::FEATURE_QUEUES_V1)
        {
            manifest
                .required_features
                .push(crate::protocol::FEATURE_QUEUES_V1.into());
        }
        if !self.crons.is_empty() {
            manifest
                .required_features
                .push(crate::protocol::FEATURE_CRON_V1.into());
        }
        manifest.required_features.sort();
        crate::protocol::validate_execution_manifest(&manifest)?;
        crate::protocol::validate_queue_manifest(&manifest)?;
        // Native identity includes the complete configuration and executable
        // declarations. Registration uses application_version, avoiding a
        // self-hash embedded in the executable that would change its hash.
        manifest.version = deployment_identity(&manifest)?;
        Ok(manifest)
    }
}

fn deployment_identity(manifest: &Manifest) -> Result<String> {
    let mut unhashed = manifest.clone();
    unhashed.version.clear();
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&unhashed)?)
    ))
}

/// A package may contain several scripts linked into the same host executable.
/// Native delivery publishes dependencies before the application's selector.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Package {
    pub primary: Manifest,
    #[serde(default)]
    pub cohosted: Vec<Manifest>,
}

impl Package {
    pub fn validate(&self) -> Result<()> {
        let mut scripts = std::collections::BTreeSet::new();
        for manifest in std::iter::once(&self.primary).chain(&self.cohosted) {
            anyhow::ensure!(
                scripts.insert(&manifest.script_name),
                "duplicate native package script"
            );
            crate::protocol::validate_required_features(&manifest.required_features)?;
            crate::protocol::validate_execution_manifest(manifest)?;
            anyhow::ensure!(
                manifest.native.is_some(),
                "native package requires native manifests"
            );
            crate::protocol::validate_queue_manifest(manifest)?;
            if let Some(reference) = &manifest.assets {
                assets::validate_reference(reference)?;
                anyhow::ensure!(
                    manifest
                        .required_features
                        .iter()
                        .any(|feature| feature == crate::protocol::FEATURE_ASSETS_V1),
                    "native assets require the assets-v1 feature"
                );
            }
            anyhow::ensure!(
                manifest.version == deployment_identity(manifest)?,
                "native manifest deployment identity mismatch"
            );
        }
        Ok(())
    }

    /// Upload verified executable blobs before using the retained deployment
    /// publisher, including queue attachment and pointer CAS semantics.
    pub async fn publish(
        &self,
        bucket: &Bucket,
        executables: &BTreeMap<String, PathBuf>,
    ) -> Result<()> {
        self.publish_resources(bucket, executables, BTreeMap::new(), BTreeMap::new())
            .await
    }

    /// Publish an on-disk package produced by Project::build or stage_manifest.
    /// Validate all asset indexes and bodies before changing any selector.
    pub async fn publish_staged(&self, bucket: &Bucket, directory: &Path) -> Result<()> {
        self.validate()?;
        let mut resources = BTreeMap::new();
        let mut executables = BTreeMap::new();
        let mut images = BTreeMap::new();
        for manifest in std::iter::once(&self.primary).chain(&self.cohosted) {
            images.extend(images::load_staged(manifest, directory)?);
            if let Some(staged) = assets::load_staged(manifest, directory)? {
                resources.insert(manifest.script_name.clone(), staged);
            }
            for artifact in &manifest.native.as_ref().unwrap().artifacts {
                executables.insert(artifact.sha256.clone(), directory.join(&artifact.sha256));
            }
        }
        self.publish_resources(bucket, &executables, resources, images)
            .await
    }

    async fn publish_resources(
        &self,
        bucket: &Bucket,
        executables: &BTreeMap<String, PathBuf>,
        mut assets: BTreeMap<String, crate::deploy::BuiltAssets>,
        images: BTreeMap<String, PathBuf>,
    ) -> Result<()> {
        self.validate()?;
        for manifest in std::iter::once(&self.primary).chain(&self.cohosted) {
            for archive in &manifest.container_images {
                let path = images
                    .get(&archive.sha256)
                    .context("native container images require publish_staged")?;
                images::verify_file(archive, path)?;
            }
        }
        for manifest in std::iter::once(&self.primary).chain(&self.cohosted) {
            anyhow::ensure!(
                manifest.assets.is_some() == assets.contains_key(&manifest.script_name),
                "native assets require a staged package; use publish_staged"
            );
        }
        crate::wake_format::ensure_ready(bucket).await?;
        let mut artifacts = BTreeMap::new();
        for manifest in std::iter::once(&self.primary).chain(&self.cohosted) {
            for artifact in &manifest.native.as_ref().unwrap().artifacts {
                artifacts.insert(artifact.sha256.clone(), artifact);
            }
        }
        for (sha, artifact) in artifacts {
            let path = executables
                .get(&sha)
                .with_context(|| format!("no executable file supplied for {sha}"))?;
            let bytes = std::fs::read(path)?;
            artifact.verify(&bytes)?;
            let key = artifact.key()?;
            // Verify an existing blob too: presence alone is insufficient.
            if let Some((existing, _)) = bucket.get(&key).await? {
                artifact.verify(&existing)?;
            } else {
                bucket.put(&key, bytes).await?;
            }
        }
        for manifest in std::iter::once(&self.primary).chain(&self.cohosted) {
            for archive in &manifest.container_images {
                images::publish(bucket, archive, &images[&archive.sha256]).await?;
            }
        }
        for manifest in self.cohosted.iter().chain(std::iter::once(&self.primary)) {
            let built = crate::deploy::Built {
                script_name: manifest.script_name.clone(),
                version: manifest.version.clone(),
                prefix: format!("deploy/{}/{}", manifest.script_name, manifest.version),
                manifest: manifest.clone(),
                modules: Vec::new(),
                assets: assets.remove(&manifest.script_name),
                bundled_in: Duration::ZERO,
                images: Vec::new(),
                fence_image: None,
                upload_images: false,
            };
            if manifest.script_name == self.primary.script_name {
                crate::deploy::write(bucket, &built).await?;
            } else {
                crate::deploy::write_component(bucket, &built).await?;
            }
        }
        Ok(())
    }

    /// Download and verify a target executable into a versioned output path.
    /// Does not start or replace a running node. Activation uses the existing
    /// graceful shutdown and an application-owned process supervisor.
    pub async fn install(&self, bucket: &Bucket, target: &str, output: &Path) -> Result<()> {
        self.validate()?;
        let artifacts = &self.primary.native.as_ref().unwrap().artifacts;
        let matches: Vec<_> = artifacts.iter().filter(|a| a.target == target).collect();
        anyhow::ensure!(
            matches.len() == 1,
            "native install requires exactly one executable for target {target}"
        );
        let artifact = matches[0];
        let (bytes, _) = bucket
            .get(&artifact.key()?)
            .await?
            .context("native executable blob is absent")?;
        artifact.verify(&bytes)?;
        let parent = output
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
        use std::io::Write;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            temporary
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o755))?;
        }
        temporary
            .persist_noclobber(output)
            .map_err(|e| anyhow::anyhow!(e.error))?;
        #[cfg(unix)]
        std::fs::File::open(parent)?.sync_all()?;
        Ok(())
    }
}
