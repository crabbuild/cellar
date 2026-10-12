//! Delivery integrity uses a deterministic engine CLI; no real engine is needed.
use anyhow::Result;
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, executable_identity, ApplicationConfig, Package},
    protocol::validate_execution_manifest,
};
use serde_json::json;

#[test]
#[allow(clippy::disallowed_methods)]
fn native_packages_stage_verify_and_publish_application_and_fence_images() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
        let directory = tempfile::tempdir()?;
        let cli = directory.path().join("engine.py");
        std::fs::write(&cli, r#"#!/usr/bin/env python3
import sys, pathlib
a=sys.argv[1:]
if a[0]=='build':
 p=pathlib.Path(a[a.index('-f')+1])
 print('sha256:fence' if 'nftables' in p.read_text() else 'sha256:app')
elif a[:2]==['image','inspect']:
 if a[3]=='{{.Id}}': print('sha256:app')
 else: print('["sha256:layer"]'+('{"Cmd":["nft"]}' if a[-1]=='sha256:fence' else '{"Cmd":["server"]}'))
elif a[0]=='save':
 # More than one multipart chunk, with stable byte content.
 pathlib.Path(a[a.index('--output')+1]).write_bytes((a[-1]+'\n').encode()*130000)
elif a[0] not in ('tag','pull'): sys.exit(12)
"#)?;
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o755))?;
        let previous = std::env::var_os("CELLAR_DOCKER");
        std::env::set_var("CELLAR_DOCKER", &cli);
        struct Restore(Option<std::ffi::OsString>);
        impl Drop for Restore { fn drop(&mut self) { match self.0.take() { Some(value) => std::env::set_var("CELLAR_DOCKER", value), None => std::env::remove_var("CELLAR_DOCKER") } } }
        let _restore = Restore(previous);
        let executable = directory.path().join("application");
        std::fs::write(&executable, b"compiled native fixture")?;
        let artifact = executable_identity(&executable, current_target())?;
        let config: ApplicationConfig = serde_json::from_value(json!({
            "script_name":"sandbox", "application_version":"code-v1", "cells":["Sandbox"],
            "containers":[{"class_name":"Sandbox", "image":"registry.invalid/app:test", "instance_type":"basic", "max_instances":4, "runtime":"runsc"}],
        }))?;
        assert!(config.manifest(vec![artifact.clone()]).is_err());
        let staged = directory.path().join("package");
        std::fs::create_dir_all(&staged)?;
        std::fs::copy(&executable, staged.join(&artifact.sha256))?;
        let primary = config.stage_manifest(vec![artifact.clone()], directory.path(), &staged)?;
        assert_eq!(primary.container_images.len(), 2);
        assert_eq!(primary.containers[0].runtime.as_deref(), Some("runsc"));
        assert!(primary.container_images.iter().any(|image| Some(&image.image) == primary.fence_image.as_ref()));
        let repeat = config.stage_manifest(vec![artifact], directory.path(), &staged)?;
        assert_eq!(repeat.version, primary.version);
        let mut missing = primary.clone();
        missing.container_images.pop();
        assert!(validate_execution_manifest(&missing).is_err());
        let mut ungated = primary.clone();
        ungated.required_features.retain(|feature| feature != "container-images-v1");
        assert!(validate_execution_manifest(&ungated).is_err());
        let mut extra = primary.clone();
        extra.container_images.push(primary.container_images[0].clone());
        assert!(validate_execution_manifest(&extra).is_err());
        let mut mixed = primary.clone();
        let mut other = mixed.native.as_ref().unwrap().artifacts[0].clone();
        other.target = if other.target.starts_with("aarch64-") { "x86_64-unknown-linux-gnu" } else { "aarch64-unknown-linux-gnu" }.into();
        mixed.native.as_mut().unwrap().artifacts.push(other);
        assert!(validate_execution_manifest(&mixed).is_err());
        let package = Package { primary, cohosted: Vec::new() };
        package.validate()?;
        let archive = &package.primary.container_images[0];
        let path = staged.join("images").join(format!("{}.tar", archive.sha256));
        let original = std::fs::read(&path)?;
        std::fs::write(&path, b"corrupt archive")?;
        let bucket = Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
        assert!(package.publish_staged(&bucket, &staged).await.is_err());
        assert!(bucket.get("deploy/current.json").await?.is_none());
        assert!(bucket.get("wake/format.json").await?.is_none());
        std::fs::write(&path, original)?;
        package.publish_staged(&bucket, &staged).await?;
        for archive in &package.primary.container_images {
            let bytes = bucket.get(&archive.key()?).await?.unwrap().0;
            archive.verify(&bytes)?;
        }
        let loaded = cellar::fleet::load_named_worker(&bucket, "sandbox", "node".into()).await?;
        assert_eq!(loaded.container_images, package.primary.container_images);
        let pointer = bucket.get("deploy/current.json").await?.unwrap().0;
        package.publish_staged(&bucket, &staged).await?;
        assert_eq!(bucket.get("deploy/current.json").await?.unwrap().0, pointer);
        bucket.put(&archive.key()?, b"corrupt stored archive".to_vec()).await?;
        assert!(package.publish_staged(&bucket, &staged).await.is_err());
        assert_eq!(bucket.get("deploy/current.json").await?.unwrap().0, pointer);
        Ok(())
    })
}
