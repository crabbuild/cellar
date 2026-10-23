use super::*;
use crate::protocol::{NodeFile, NodeInventory};

struct Fixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    output: PathBuf,
    prepared: Prepared,
}

impl Fixture {
    fn new() -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir()?;
        let base = directory.path().canonicalize()?;
        let root = base.join("runtime");
        let output = base.join("package");
        std::fs::create_dir_all(root.join("bin"))?;
        std::fs::create_dir_all(root.join("app"))?;
        let platform = if cfg!(target_os = "macos") {
            "darwin"
        } else {
            "linux"
        };
        let arch = if cfg!(target_arch = "aarch64") {
            "arm64"
        } else {
            "x64"
        };
        let metadata = serde_json::json!({"version":"24.13.1","platform":platform,"arch":arch});
        std::fs::write(
            root.join("bin/node"),
            format!("#!/bin/sh\nprintf '%s' '{metadata}'\n"),
        )?;
        std::fs::set_permissions(
            root.join("bin/node"),
            std::fs::Permissions::from_mode(0o755),
        )?;
        std::fs::write(root.join("app/index.mjs"), b"export const fetch=()=>null")?;
        let project = NodeProject {
            root: root.clone(),
            executable: "bin/node".into(),
            entry: "app/index.mjs".into(),
            working_directory: "app".into(),
        };
        let prepared = project.prepare(&base, &output, crate::native_deploy::current_target())?;
        Ok(Self {
            _directory: directory,
            root,
            output,
            prepared,
        })
    }

    fn reference(&self) -> NodeEnvironmentRef {
        NodeEnvironmentRef {
            format: 1,
            target: self.prepared.checked.inventory.target.clone(),
            executable_sha256: "a".repeat(64),
            inventory_sha256: self.prepared.checked.header.inventory_sha256.clone(),
            archive_sha256: self.prepared.sha256.clone(),
            archive_bytes: self.prepared.bytes,
            prefix: self.root.clone(),
        }
    }
}

#[test]
fn node_inventory_capture_restore_and_tampering_refusal() -> Result<()> {
    let fixture = Fixture::new()?;
    let reference = fixture.reference();
    verify_file(&reference, &fixture.prepared.archive_path)?;
    let expected = fixture.prepared.checked.inventory.files.clone();
    std::fs::remove_dir_all(&fixture.root)?;
    archive::install(&reference, &fixture.prepared.archive_path, &fixture.root)?;
    assert_eq!(files::capture(&fixture.root)?, expected);
    archive::install(&reference, &fixture.prepared.archive_path, &fixture.root)?;
    assert!(archive::install(
        &reference,
        &fixture.prepared.archive_path,
        &fixture.root.with_extension("wrong")
    )
    .is_err());
    use std::os::unix::fs::PermissionsExt;
    let entry = fixture.root.join("app/index.mjs");
    std::fs::set_permissions(&entry, std::fs::Permissions::from_mode(0o644))?;
    std::fs::write(&entry, b"tampered")?;
    assert!(verify_inventory_files(&fixture.prepared.checked.inventory).is_err());
    assert!(archive::install(&reference, &fixture.prepared.archive_path, &fixture.root).is_err());
    assert_eq!(std::fs::read(&entry)?, b"tampered");
    Ok(())
}

#[test]
fn node_inventory_rejects_escape_symlink_mode_and_extra_files() -> Result<()> {
    let fixture = Fixture::new()?;
    let inventory = &fixture.prepared.checked.inventory;
    let mut invalid: NodeInventory = serde_json::from_slice(&serde_json::to_vec(inventory)?)?;
    invalid.entry = fixture.root.join("../escape");
    assert!(invalid.validate(&fixture.root).is_err());
    let mut invalid = inventory.clone();
    invalid.files.insert(
        fixture.root.join("app"),
        NodeFile {
            bytes: 0,
            sha256: "a".repeat(64),
            executable: false,
        },
    );
    assert!(invalid.validate(&fixture.root).is_err());
    let mut invalid = inventory.clone();
    invalid.target = "wrong-target".into();
    assert!(invalid.validate(&fixture.root).is_err());
    let mut invalid = inventory.clone();
    invalid.files.insert(
        fixture.root.join(vec!["nested"; 65].join("/")),
        NodeFile {
            bytes: 0,
            sha256: "a".repeat(64),
            executable: false,
        },
    );
    assert!(invalid.validate(&fixture.root).is_err());
    std::os::unix::fs::symlink("index.mjs", fixture.root.join("app/link"))?;
    assert!(verify_inventory_files(inventory).is_err());
    std::fs::remove_file(fixture.root.join("app/link"))?;
    std::fs::write(fixture.root.join("app/injected.mjs"), b"unexpected")?;
    assert!(verify_inventory_files(inventory).is_err());
    std::fs::remove_file(fixture.root.join("app/injected.mjs"))?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        &inventory.executable,
        std::fs::Permissions::from_mode(0o644),
    )?;
    assert!(verify_inventory_files(inventory).is_err());
    Ok(())
}

#[test]
fn node_archives_reject_missing_duplicate_unlisted_and_special_entries() -> Result<()> {
    let fixture = Fixture::new()?;
    for fault in ["missing", "duplicate", "unlisted", "symlink", "oversized"] {
        let path = fixture.output.join(format!("{fault}.tgz"));
        let file = flate2::write::GzEncoder::new(
            std::fs::File::create(&path)?,
            flate2::Compression::fast(),
        );
        let mut writer = tar::Builder::new(file);

        fn append(
            writer: &mut tar::Builder<flate2::write::GzEncoder<std::fs::File>>,
            name: &str,
            bytes: &[u8],
        ) -> Result<()> {
            let mut header = tar::Header::new_ustar();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o444);
            header.set_cksum();
            writer.append_data(&mut header, name, bytes)?;
            Ok(())
        }
        append(
            &mut writer,
            "environment.json",
            &serde_json::to_vec(&fixture.prepared.checked.header)?,
        )?;
        append(
            &mut writer,
            "inventory.json",
            &fixture.prepared.checked.inventory_bytes,
        )?;
        for (index, (path, file)) in fixture.prepared.checked.inventory.files.iter().enumerate() {
            if index == 0 && fault == "missing" {
                continue;
            }
            let name = format!("blobs/{}", file.sha256);
            let mut bytes = std::fs::read(path)?;
            if index == 0 && fault == "oversized" {
                bytes.push(0);
            }
            append(&mut writer, &name, &bytes)?;
            if index == 0 && fault == "duplicate" {
                append(&mut writer, &name, &bytes)?;
            }
        }
        if fault == "unlisted" {
            append(
                &mut writer,
                &format!("blobs/{}", "f".repeat(64)),
                b"unlisted",
            )?;
        }
        if fault == "symlink" {
            let mut header = tar::Header::new_ustar();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_link_name("../../escape")?;
            header.set_cksum();
            writer.append_data(&mut header, "blobs/link", [].as_slice())?;
        }
        writer.into_inner()?.finish()?;
        assert!(
            archive::inspect(&path, None).is_err(),
            "accepted {fault} archive"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_publish_and_install_require_verified_runtime_before_selector_or_executable(
) -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let fixture = Fixture::new()?;
            let host = fixture.output.join("host");
            std::fs::write(&host, b"native fixture")?;
            let artifact = crate::native_deploy::executable_identity(
                &host,
                crate::native_deploy::current_target(),
            )?;
            std::fs::copy(&host, fixture.output.join(&artifact.sha256))?;
            let config: crate::native_deploy::ApplicationConfig = serde_json::from_value(
                serde_json::json!({"script_name":"node-fixture","application_version":"v1"}),
            )?;
            let mut manifest = config.manifest(vec![artifact.clone()])?;
            fixture
                .prepared
                .stage(&mut manifest, &artifact, &fixture.output)?;
            let package = crate::native_deploy::Package {
                primary: manifest,
                cohosted: vec![],
            };
            package.validate()?;
            let bucket = Bucket::open_dev(&fixture.output.join("bucket.sqlite"))?;
            let archive = &fixture.prepared.archive_path;
            let saved = archive.with_extension("saved");
            std::fs::rename(archive, &saved)?;
            assert!(package
                .publish_staged(&bucket, &fixture.output)
                .await
                .is_err());
            assert!(bucket.get("deploy/current.json").await?.is_none());
            std::fs::rename(saved, archive)?;
            package.publish_staged(&bucket, &fixture.output).await?;
            let installed = fixture.output.join("installed");
            assert!(package
                .install(&bucket, crate::native_deploy::current_target(), &installed)
                .await
                .is_err());
            assert!(!installed.exists());
            std::fs::remove_dir_all(&fixture.root)?;
            package
                .install_with_runtimes(
                    &bucket,
                    crate::native_deploy::current_target(),
                    &installed,
                    None,
                    Some(&fixture.root),
                )
                .await?;
            assert_eq!(std::fs::read(installed)?, b"native fixture");
            verify_inventory_files(&fixture.prepared.checked.inventory)?;
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}
