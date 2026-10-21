use super::*;
use crate::protocol::{PythonPlatform, PythonPrerequisites, PythonSourceInventory};
use anyhow::Context as _;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt};

fn identity(path: &Path) -> Result<(u64, String)> {
    let bytes = std::fs::read(path)?;
    Ok((bytes.len() as u64, format!("{:x}", Sha256::digest(&bytes))))
}

fn fixture(directory: &Path) -> Result<PythonInventory> {
    let root = directory.canonicalize()?.join("environment");
    std::fs::create_dir(&root)?;
    let executable = root.join("python");
    std::fs::write(&executable, b"fixture")?;
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))?;
    let mut libraries = BTreeMap::new();
    let mut dependencies = BTreeMap::new();
    let current = std::env::current_exe()?.canonicalize()?;
    for path in loaded::snapshot()? {
        if platform::cached(&path)? {
            libraries.insert(path, PythonNativeLibrary::MacosSharedCache);
        } else {
            let path = path.canonicalize()?;
            if path == current {
                continue;
            }
            let (bytes, sha256) = identity(&path)?;
            libraries.insert(path.clone(), PythonNativeLibrary::File { bytes, sha256 });
            dependencies.insert(path, BTreeMap::new());
        }
    }
    let first = libraries
        .keys()
        .next()
        .context("fixture has no loader providers")?
        .clone();
    dependencies.insert(
        executable.clone(),
        BTreeMap::from([("system".into(), vec![first.clone()])]),
    );
    Ok(PythonInventory {
        format: 1,
        target: crate::native_deploy::current_target().into(),
        version: "3.14.5".into(),
        soabi: "cpython-314-test".into(),
        home: root.clone(),
        executable: executable.clone(),
        standard_library_paths: vec![root.clone()],
        module_paths: vec![],
        roots: vec![root],
        source: PythonSourceInventory {
            sha256: "a".repeat(64),
            filename: "app.py".into(),
            descriptor: serde_json::json!({"events":[],"cells":{}}),
            distributions: vec![],
        },
        files: BTreeMap::from([(
            executable,
            PythonFile::File {
                bytes: 7,
                sha256: format!("{:x}", Sha256::digest(b"fixture")),
                executable: true,
            },
        )]),
        prerequisites: PythonPrerequisites {
            platform: platform::current()?,
            libraries,
            dependencies,
            host_dependencies: BTreeMap::from([("system".into(), vec![first])]),
        },
    })
}

#[test]
fn cohort_and_external_file_loss_refuse_before_admission() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut inventory = fixture(directory.path())?;
    verify_prerequisites(&inventory)?;
    verify_loaded_libraries(&inventory)?;
    let original = inventory.prerequisites.platform.clone();
    match &mut inventory.prerequisites.platform {
        PythonPlatform::Macos { build, .. } => build.push_str("-different"),
        PythonPlatform::Linux { kernel_release, .. } => kernel_release.push_str("-different"),
    }
    assert!(verify_prerequisites(&inventory).is_err());
    inventory.prerequisites.platform = original;
    let library = directory
        .path()
        .canonicalize()?
        .join("PRIVATE-NATIVE-PREREQUISITE");
    std::fs::write(&library, b"qualified")?;
    let (bytes, sha256) = identity(&library)?;
    inventory
        .prerequisites
        .libraries
        .insert(library.clone(), PythonNativeLibrary::File { bytes, sha256 });
    inventory
        .prerequisites
        .dependencies
        .insert(library.clone(), BTreeMap::new());
    verify_prerequisites(&inventory)?;
    std::fs::write(&library, b"corrupted")?;
    assert!(verify_prerequisites(&inventory).is_err());
    std::fs::remove_file(&library)?;
    assert!(verify_prerequisites(&inventory).is_err());
    Ok(())
}

#[test]
fn native_files_cannot_omit_dependency_records() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut inventory = fixture(directory.path())?;
    super::super::files::verify_inventory_files(&inventory)?;
    let library = inventory.home.join("unrecorded-native.so");
    std::fs::write(&library, b"\x7fELFfixture")?;
    let (bytes, sha256) = identity(&library)?;
    inventory.files.insert(
        library.clone(),
        PythonFile::File {
            bytes,
            sha256,
            executable: false,
        },
    );
    assert!(super::super::files::verify_inventory_files(&inventory)
        .unwrap_err()
        .to_string()
        .contains("dependency record"));
    inventory
        .prerequisites
        .dependencies
        .insert(library, BTreeMap::new());
    super::super::files::verify_inventory_files(&inventory)?;
    Ok(())
}

#[test]
fn actual_loader_provider_is_refused_until_qualified_and_removed_on_close() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut inventory = fixture(directory.path())?;
    let source = directory.path().join("provider.c");
    std::fs::write(
        &source,
        "int cellar_prerequisite_fixture(void) { return 7; }\n",
    )?;
    let library = directory
        .path()
        .canonicalize()?
        .join(if cfg!(target_os = "macos") {
            "provider.dylib"
        } else {
            "provider.so"
        });
    let mode = if cfg!(target_os = "macos") {
        "-dynamiclib"
    } else {
        "-shared"
    };
    ensure!(
        std::process::Command::new("cc")
            .args([mode, "-fPIC"])
            .arg(&source)
            .arg("-o")
            .arg(&library)
            .status()?
            .success(),
        "compile native provider fixture"
    );

    struct Library(*mut libc::c_void);

    impl Drop for Library {
        fn drop(&mut self) {
            // SAFETY: this guard owns exactly one successful dlopen reference.
            unsafe {
                libc::dlclose(self.0);
            }
        }
    }
    let name = std::ffi::CString::new(library.to_str().context("fixture path must be UTF-8")?)?;
    // SAFETY: the private C fixture has no constructors, exported callbacks or
    // threads. The guard retains its loader reference until checks complete.
    let loaded = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
    ensure!(!loaded.is_null(), "load native provider fixture");
    let handle = Library(loaded);
    assert!(verify_loaded_libraries(&inventory).is_err());
    let (bytes, sha256) = identity(&library)?;
    inventory
        .prerequisites
        .libraries
        .insert(library.clone(), PythonNativeLibrary::File { bytes, sha256 });
    inventory
        .prerequisites
        .dependencies
        .insert(library.clone(), BTreeMap::new());
    verify_loaded_libraries(&inventory)?;
    drop(handle);
    assert!(!loaded::snapshot()?
        .iter()
        .any(|path| path.canonicalize().ok().as_ref() == Some(&library)));
    Ok(())
}
