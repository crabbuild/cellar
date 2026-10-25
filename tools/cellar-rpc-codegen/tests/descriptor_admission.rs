// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the build tool's filesystem/subprocess boundary with no compilers
//! on PATH. Invalid contracts must fail before touching generated output.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto,
};
use std::{
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "cellar-rpc-codegen-admission-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        // Atomic creation refuses a stale directory instead of reusing it.
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn bounded_run(input: &Path, output: &Path) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_cellar-rpc-codegen"))
        .arg("--descriptor")
        .arg(input)
        .arg("--out")
        .arg(output)
        .env("PATH", "")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let result = child.wait_with_output().unwrap();
            panic!("generator waited for invalid descriptor input: {result:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn oversized_descriptor_files_are_refused_before_generation() {
    let scratch = Scratch::new();
    let input = scratch.0.join("large.binpb");
    let output = scratch.0.join("generated");
    let size = 4 * 1024 * 1024 + 1;
    std::fs::File::create(&input)
        .unwrap()
        .set_len(size)
        .unwrap();
    std::fs::create_dir(&output).unwrap();
    std::fs::write(output.join("user-file.txt"), b"preserve this file").unwrap();
    let result = bounded_run(&input, &output);
    assert_eq!(result.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&result.stderr).contains("RPC descriptor file exceeds"));
    assert_eq!(std::fs::metadata(&input).unwrap().len(), size);
    assert_eq!(std::fs::read_dir(&output).unwrap().count(), 1);
    assert_eq!(
        std::fs::read(output.join("user-file.txt")).unwrap(),
        b"preserve this file"
    );
}

#[cfg(unix)]
#[test]
fn fifo_descriptors_are_refused_without_waiting_for_a_writer() {
    let scratch = Scratch::new();
    let input = scratch.0.join("waiting.binpb");
    let output = scratch.0.join("generated");
    // Only fixture creation needs coreutils; the generator itself has no tools.
    assert!(Command::new("mkfifo")
        .arg(&input)
        .status()
        .unwrap()
        .success());
    let result = bounded_run(&input, &output);
    assert_eq!(result.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&result.stderr).contains("regular file"));
    assert!(!output.exists());
}

fn descriptor() -> FileDescriptorSet {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("service.proto".into()),
            package: Some("test".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Value".into()),
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("Counter".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Read".into()),
                    input_type: Some(".test.Value".into()),
                    output_type: Some(".test.Value".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn invalid_contracts_never_create_or_overwrite_generated_output() {
    let mut collision = descriptor();
    collision.file[0].enum_type.push(EnumDescriptorProto {
        name: Some("State".into()),
        value: vec![EnumValueDescriptorProto {
            name: Some("Value".into()),
            number: Some(0),
            ..Default::default()
        }],
        ..Default::default()
    });
    let mut invisible = descriptor();
    invisible.file[0].message_type[0]
        .field
        .push(FieldDescriptorProto {
            name: Some("other".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::Message as i32),
            type_name: Some(".other.Value".into()),
            ..Default::default()
        });
    invisible.file.push(FileDescriptorProto {
        name: Some("other.proto".into()),
        package: Some("other".into()),
        syntax: Some("proto3".into()),
        message_type: vec![DescriptorProto {
            name: Some("Value".into()),
            ..Default::default()
        }],
        ..Default::default()
    });

    for (descriptor, expected) in [
        (collision, "duplicate RPC descriptor symbol: test.Value"),
        (
            invisible,
            "RPC type requires a visible descriptor import: other.Value",
        ),
    ] {
        let scratch = Scratch::new();
        let input = scratch.0.join("input.binpb");
        let output = scratch.0.join("generated");
        std::fs::write(&input, descriptor.encode_to_vec()).unwrap();
        for exists in [false, true] {
            if exists {
                std::fs::create_dir(&output).unwrap();
                for name in ["contracts.binpb", "rpc.json", "user-file.txt"] {
                    std::fs::write(output.join(name), b"preserve this file").unwrap();
                }
            }
            let result = Command::new(env!("CARGO_BIN_EXE_cellar-rpc-codegen"))
                .args(["--descriptor"])
                .arg(&input)
                .args(["--out"])
                .arg(&output)
                .args(["--languages", "rust,python,typescript,javascript"])
                .env("PATH", scratch.0.join("no-compilers"))
                .output()
                .unwrap();
            assert_eq!(result.status.code(), Some(1));
            let error = String::from_utf8_lossy(&result.stderr);
            assert!(
                error.contains(expected),
                "unexpected generator failure: {error}"
            );
            assert_eq!(output.exists(), exists);
            if exists {
                assert_eq!(std::fs::read_dir(&output).unwrap().count(), 3);
                for name in ["contracts.binpb", "rpc.json", "user-file.txt"] {
                    assert_eq!(
                        std::fs::read(output.join(name)).unwrap(),
                        b"preserve this file"
                    );
                }
            }
        }
    }
}
