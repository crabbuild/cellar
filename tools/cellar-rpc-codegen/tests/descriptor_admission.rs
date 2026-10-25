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
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
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
