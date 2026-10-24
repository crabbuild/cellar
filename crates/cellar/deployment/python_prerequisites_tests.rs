// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn fixture() -> (
    PythonPrerequisites,
    Vec<PathBuf>,
    BTreeMap<PathBuf, PythonFile>,
) {
    let library = PathBuf::from("/usr/lib/libSystem.B.dylib");
    let binary = PathBuf::from("/environment/python/libpython.dylib");
    let prerequisites = PythonPrerequisites {
        platform: PythonPlatform::Macos {
            machine: "arm64".into(),
            build: "25G1".into(),
            shared_cache_uuid: "a".repeat(32),
        },
        libraries: BTreeMap::from([(library.clone(), PythonNativeLibrary::MacosSharedCache)]),
        dependencies: BTreeMap::from([(
            binary.clone(),
            BTreeMap::from([("/usr/lib/libSystem.B.dylib".into(), vec![library.clone()])]),
        )]),
        host_dependencies: BTreeMap::from([("libSystem.B.dylib".into(), vec![library])]),
    };
    (
        prerequisites,
        vec!["/environment".into()],
        BTreeMap::from([(
            binary,
            PythonFile::File {
                bytes: 4,
                sha256: "a".repeat(64),
                executable: true,
            },
        )]),
    )
}

#[test]
fn prerequisite_graph_requires_qualified_providers_and_matching_platform() {
    let (value, roots, files) = fixture();
    let check =
        |value: &PythonPrerequisites| value.validate("aarch64-apple-darwin", &roots, &files);
    check(&value).unwrap();
    assert!(value
        .validate("x86_64-unknown-linux-gnu", &roots, &files)
        .is_err());
    let mut wrong = value.clone();
    wrong
        .host_dependencies
        .insert("unknown".into(), vec!["/unqualified/lib.so".into()]);
    assert!(check(&wrong).is_err());
    let mut wrong = value.clone();
    wrong.libraries.insert(
        "/environment/lib.so".into(),
        PythonNativeLibrary::File {
            bytes: 1,
            sha256: "a".repeat(64),
        },
    );
    assert!(check(&wrong).is_err());
    let mut wrong = value.clone();
    wrong.libraries.insert(
        "/foreign/lib.so".into(),
        PythonNativeLibrary::MacosSharedCache,
    );
    assert!(check(&wrong).is_err());
    let mut wrong = value;
    wrong
        .dependencies
        .insert("/not-in-the-inventory.so".into(), BTreeMap::new());
    assert!(check(&wrong).is_err());
}

#[test]
fn external_files_are_hash_locked_and_bounded() {
    let (mut value, roots, files) = fixture();
    let library = PathBuf::from("/usr/lib/libSystem.B.dylib");
    value.libraries.insert(
        library.clone(),
        PythonNativeLibrary::File {
            bytes: 4,
            sha256: "b".repeat(64),
        },
    );
    let check =
        |value: &PythonPrerequisites| value.validate("aarch64-apple-darwin", &roots, &files);
    check(&value).unwrap();
    for (bytes, sha256) in [
        (0, "b".repeat(64)),
        (1, "not-a-digest".into()),
        (MAX_PYTHON_EXPANDED_BYTES + 1, "b".repeat(64)),
    ] {
        value
            .libraries
            .insert(library.clone(), PythonNativeLibrary::File { bytes, sha256 });
        assert!(check(&value).is_err());
    }
    value.libraries.insert(
        library.clone(),
        PythonNativeLibrary::File {
            bytes: 4,
            sha256: "b".repeat(64),
        },
    );
    value.host_dependencies = (0..=MAX_PYTHON_NATIVE_EDGES)
        .map(|index| (format!("library{index}"), vec![library.clone()]))
        .collect();
    assert!(check(&value).is_err());
}

#[test]
fn linux_cannot_claim_an_apple_cache_and_inventory_has_no_legacy_default() {
    let (mut value, roots, files) = fixture();
    value.platform = PythonPlatform::Linux {
        machine: "x86_64".into(),
        kernel_release: "6.14.0-test".into(),
        os_release_sha256: "c".repeat(64),
    };
    assert!(value
        .validate("x86_64-unknown-linux-gnu", &roots, &files)
        .is_err());
    value.libraries.values_mut().for_each(|library| {
        *library = PythonNativeLibrary::File {
            bytes: 4,
            sha256: "b".repeat(64),
        }
    });
    value
        .validate("x86_64-unknown-linux-gnu", &roots, &files)
        .unwrap();
    let encoded = serde_json::to_value(&value).unwrap();
    for field in ["platform", "libraries", "dependencies", "host_dependencies"] {
        let mut missing = encoded.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<PythonPrerequisites>(missing).is_err());
    }
    let mut unknown = encoded;
    unknown["accept_unknown"] = serde_json::json!(true);
    assert!(serde_json::from_value::<PythonPrerequisites>(unknown).is_err());
}

#[test]
fn provider_alternatives_are_explicit_sorted_and_qualified() {
    let (mut value, roots, files) = fixture();
    let other = PathBuf::from("/usr/lib/libobjc.A.dylib");
    value
        .libraries
        .insert(other.clone(), PythonNativeLibrary::MacosSharedCache);
    value.host_dependencies.insert(
        "contextual".into(),
        vec!["/usr/lib/libSystem.B.dylib".into(), other.clone()],
    );
    value
        .validate("aarch64-apple-darwin", &roots, &files)
        .unwrap();
    value
        .host_dependencies
        .get_mut("contextual")
        .unwrap()
        .push(other);
    assert!(value
        .validate("aarch64-apple-darwin", &roots, &files)
        .is_err());
}
