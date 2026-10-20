use super::*;
use crate::protocol::{PythonSourceInventory, MAX_PYTHON_ARCHIVE_BYTES};
use std::io::Cursor;

struct Fixture {
    _directory: tempfile::TempDir,
    prefix: PathBuf,
    path: PathBuf,
    inventory: PythonInventory,
    blobs: BTreeMap<String, Vec<u8>>,
}

impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().canonicalize()?;
        let prefix = root.join("environment");
        let mut files = BTreeMap::new();
        let mut blobs = BTreeMap::new();
        for (name, data, executable) in [
            ("python/bin/python3.14", b"interpreter".as_slice(), true),
            (
                "python/lib/encodings/__init__.py",
                b"stdlib".as_slice(),
                false,
            ),
            ("site/business.py", b"business logic".as_slice(), false),
        ] {
            let sha256 = format!("{:x}", Sha256::digest(data));
            files.insert(
                prefix.join(name),
                PythonFile::File {
                    bytes: data.len() as u64,
                    sha256: sha256.clone(),
                    executable,
                },
            );
            blobs.insert(sha256, data.to_vec());
        }
        files.insert(
            prefix.join("python/bin/python"),
            PythonFile::Symlink {
                target: "python3.14".into(),
            },
        );
        let inventory = PythonInventory {
            format: 1,
            target: crate::native_deploy::current_target().into(),
            version: "3.14.5".into(),
            soabi: "cpython-314-test".into(),
            home: prefix.join("python"),
            executable: prefix.join("python/bin/python3.14"),
            standard_library_paths: vec![prefix.join("python/lib")],
            module_paths: vec![prefix.join("site")],
            roots: vec![prefix.join("python"), prefix.join("site")],
            source: PythonSourceInventory {
                sha256: "b".repeat(64),
                filename: "app.py".into(),
                descriptor: serde_json::json!({"events":["fetch"],"cells":{}}),
                distributions: vec![],
            },
            files,
        };
        Ok(Self {
            path: root.join("environment.tar.gz"),
            _directory: directory,
            prefix,
            inventory,
            blobs,
        })
    }

    fn write(&self, fault: &str) -> Result<PythonEnvironmentRef> {
        let raw = serde_json::to_vec(&self.inventory)?;
        let inventory_sha256 = format!("{:x}", Sha256::digest(&raw));
        let header = Header {
            format: 1,
            prefix: self.prefix.clone(),
            inventory_sha256: inventory_sha256.clone(),
        };
        let output = flate2::write::GzEncoder::new(
            std::fs::File::create(&self.path)?,
            flate2::Compression::default(),
        );
        let mut writer = tar::Builder::new(output);

        fn append(
            writer: &mut tar::Builder<flate2::write::GzEncoder<std::fs::File>>,
            name: &str,
            data: &[u8],
        ) -> Result<()> {
            let mut header = tar::Header::new_ustar();
            header.set_size(data.len() as u64);
            header.set_mode(0o444);
            header.set_cksum();
            writer.append_data(&mut header, name, Cursor::new(data))?;
            Ok(())
        }
        append(
            &mut writer,
            "environment.json",
            &serde_json::to_vec(&header)?,
        )?;
        append(&mut writer, "inventory.json", &raw)?;
        for (index, (sha, data)) in self.blobs.iter().enumerate() {
            if fault == "missing" && index == 0 {
                continue;
            }
            let data = if fault == "corrupt" && index == 0 {
                vec![0; data.len()]
            } else {
                data.clone()
            };
            append(&mut writer, &format!("blobs/{sha}"), &data)?;
            if fault == "duplicate" && index == 0 {
                append(&mut writer, &format!("blobs/{sha}"), &data)?;
            }
        }
        if fault == "unlisted" {
            append(
                &mut writer,
                &format!("blobs/{}", "c".repeat(64)),
                b"unlisted",
            )?;
        }
        if fault == "symlink-entry" {
            let mut header = tar::Header::new_ustar();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_link_name("../../escape")?;
            header.set_cksum();
            writer.append_data(&mut header, "blobs/unlisted", Cursor::new([]))?;
        }
        writer.into_inner()?.finish()?;
        let bytes = std::fs::read(&self.path)?;
        Ok(PythonEnvironmentRef {
            format: 1,
            target: self.inventory.target.clone(),
            executable_sha256: "a".repeat(64),
            inventory_sha256,
            archive_sha256: format!("{:x}", Sha256::digest(&bytes)),
            archive_bytes: bytes.len() as u64,
            prefix: self.prefix.clone(),
        })
    }
}

#[test]
fn both_threading_abis_round_trip_and_other_abis_are_refused() -> Result<()> {
    for soabi in ["cpython-314-test", "cpython-314t-test"] {
        let mut fixture = Fixture::new()?;
        fixture.inventory.soabi = soabi.into();
        let reference = fixture.write("")?;
        reference.verify_archive(&std::fs::read(&fixture.path)?)?;
        install(&reference, &fixture.path, &fixture.prefix)?;
        verify_inventory_files(&fixture.inventory)?;
    }
    let mut fixture = Fixture::new()?;
    for soabi in ["cpython-313t-test", "cpython-314tt-test", "pypy-314-test"] {
        fixture.inventory.soabi = soabi.into();
        assert!(fixture.inventory.validate(&fixture.prefix).is_err());
    }
    Ok(())
}

#[test]
fn verified_restore_preserves_files_modes_links_and_reuses_without_overwrite() -> Result<()> {
    let fixture = Fixture::new()?;
    let reference = fixture.write("")?;
    reference.verify_archive(&std::fs::read(&fixture.path)?)?;
    install(&reference, &fixture.path, &fixture.prefix)?;
    verify_inventory_files(&fixture.inventory)?;
    assert_eq!(
        std::fs::read(fixture.prefix.join("python/bin/python"))?,
        b"interpreter"
    );
    assert!(install(&reference, &fixture.path, &fixture.prefix).is_ok());
    std::fs::write(fixture.prefix.join("site/unlisted.py"), b"extra code")?;
    assert!(install(&reference, &fixture.path, &fixture.prefix).is_err());
    assert_eq!(
        std::fs::read(fixture.prefix.join("site/unlisted.py"))?,
        b"extra code"
    );
    Ok(())
}

#[test]
fn malformed_archives_and_escaping_inventory_never_create_prefix() -> Result<()> {
    for fault in [
        "missing",
        "corrupt",
        "duplicate",
        "unlisted",
        "symlink-entry",
    ] {
        let fixture = Fixture::new()?;
        let reference = fixture.write(fault)?;
        assert!(
            install(&reference, &fixture.path, &fixture.prefix).is_err(),
            "accepted {fault}"
        );
        assert!(
            !fixture.prefix.exists(),
            "{fault} created provisioning prefix"
        );
    }
    let mut fixture = Fixture::new()?;
    fixture.inventory.files.insert(
        fixture.prefix.join("site/outside"),
        PythonFile::Symlink {
            target: "../../escape".into(),
        },
    );
    let reference = fixture.write("")?;
    assert!(install(&reference, &fixture.path, &fixture.prefix).is_err());
    assert!(!fixture.prefix.exists());
    Ok(())
}

#[test]
fn identity_and_publication_refuse_wrong_prefix_target_and_existing_empty_directory() -> Result<()>
{
    let fixture = Fixture::new()?;
    let reference = fixture.write("")?;
    let checked = inspect(&fixture.path, None)?;
    let mut wrong = reference.clone();
    wrong.target = "wrong-target".into();
    assert!(verify_reference(&wrong, &checked).is_err());
    assert!(install(
        &reference,
        &fixture.path,
        &fixture.prefix.with_file_name("other")
    )
    .is_err());
    wrong.archive_bytes = MAX_PYTHON_ARCHIVE_BYTES + 1;
    assert!(wrong.validate().is_err());
    std::fs::create_dir(&fixture.prefix)?;
    assert!(install(&reference, &fixture.path, &fixture.prefix).is_err());
    assert_eq!(std::fs::read_dir(&fixture.prefix)?.count(), 0);
    let staging = tempfile::tempdir_in(fixture.prefix.parent().unwrap())?;
    assert_eq!(
        rename_new(staging.path(), &fixture.prefix)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert!(staging.path().exists());
    assert!(fixture.prefix.is_dir());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bucket_publication_and_install_restore_runtime_and_refuse_corruption_before_activation(
) -> Result<()> {
    use crate::{
        bucket::Bucket,
        native_deploy::{current_target, executable_identity, ApplicationConfig, Package},
        protocol::FEATURE_EMBEDDED_PYTHON_V1,
    };
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let fixture = Fixture::new()?;
            let mut reference = fixture.write("")?;
            let root = fixture.path.parent().unwrap();
            let staged = root.join("staged");
            std::fs::create_dir_all(staged.join("python"))?;
            let source = root.join("host-source");
            std::fs::write(&source, b"verified native host fixture")?;
            let artifact = executable_identity(&source, current_target())?;
            reference.executable_sha256 = artifact.sha256.clone();
            std::fs::copy(&source, staged.join(&artifact.sha256))?;
            let archive_path = staged.join("python").join(&reference.archive_sha256);
            std::fs::write(&archive_path, b"corrupt staged archive")?;
            let config: ApplicationConfig = serde_json::from_value(
                serde_json::json!({"script_name":"python-package-test","application_version":"v1"}),
            )?;
            let mut manifest = config.manifest(vec![artifact])?;
            manifest
                .native
                .as_mut()
                .unwrap()
                .python
                .push(reference.clone());
            manifest
                .required_features
                .push(FEATURE_EMBEDDED_PYTHON_V1.into());
            manifest.version = crate::native_deploy::deployment_identity(&manifest)?;
            let package = Package {
                primary: manifest,
                cohosted: vec![],
            };
            package.validate()?;
            let bucket = Bucket::open_dev(&root.join("bucket.sqlite"))?;
            assert!(package.publish_staged(&bucket, &staged).await.is_err());
            assert!(bucket.get("deploy/current.json").await?.is_none());
            std::fs::copy(&fixture.path, &archive_path)?;
            package.publish_staged(&bucket, &staged).await?;
            assert_eq!(
                crate::fleet::read_current_pointer(&bucket)
                    .await?
                    .script_name
                    .as_deref(),
                Some("python-package-test")
            );
            let installed = root.join("installed-host");
            assert!(package
                .install(&bucket, current_target(), &installed)
                .await
                .is_err());
            assert!(!installed.exists() && !fixture.prefix.exists());
            let bytes = std::fs::read(&fixture.path)?;
            bucket
                .put(&reference.key()?, b"corrupt remote archive".to_vec())
                .await?;
            assert!(package
                .install_with_python(&bucket, current_target(), &installed, Some(&fixture.prefix))
                .await
                .is_err());
            assert!(!installed.exists() && !fixture.prefix.exists());
            bucket.put(&reference.key()?, bytes).await?;
            package
                .install_with_python(&bucket, current_target(), &installed, Some(&fixture.prefix))
                .await?;
            assert_eq!(std::fs::read(&installed)?, std::fs::read(&source)?);
            verify_inventory_files(&fixture.inventory)?;
            let second = root.join("second-host");
            package
                .install_with_python(&bucket, current_target(), &second, Some(&fixture.prefix))
                .await?;
            assert_eq!(std::fs::read(&second)?, std::fs::read(&source)?);
            std::fs::write(fixture.prefix.join("site/unlisted.py"), b"unlisted code")?;
            let refused = root.join("refused-host");
            assert!(package
                .install_with_python(&bucket, current_target(), &refused, Some(&fixture.prefix))
                .await
                .is_err());
            assert!(!refused.exists());
            Ok(())
        })
        .await;
    domain.shutdown().await;
    result?
}
