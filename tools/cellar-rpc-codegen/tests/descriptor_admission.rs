// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the build tool's filesystem/subprocess boundary with no compilers
//! on PATH. Invalid contracts must fail before touching generated output.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MethodDescriptorProto, ServiceDescriptorProto,
    UninterpretedOption,
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

    let mut message_set = descriptor();
    message_set.file[0].message_type[0]
        .options
        .get_or_insert_default()
        .message_set_wire_format = Some(true);
    let mut lazy = descriptor();
    lazy.file[0].message_type[0]
        .field
        .push(FieldDescriptorProto {
            name: Some("text".into()),
            number: Some(1),
            label: Some(Label::Optional as i32),
            r#type: Some(Type::String as i32),
            options: Some(prost_types::FieldOptions {
                lazy: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        });
    let mut unresolved = descriptor();
    unresolved.file[0]
        .options
        .get_or_insert_default()
        .uninterpreted_option
        .push(UninterpretedOption::default());
    let mut deep = descriptor();
    let mut nested = &mut deep.file[0].message_type[0];
    for _ in 1..32 {
        nested.nested_type.push(DescriptorProto {
            name: Some("Child".into()),
            ..Default::default()
        });
        nested = &mut nested.nested_type[0];
    }

    let mut inputs = vec![
        (collision, "duplicate RPC descriptor symbol: test.Value"),
        (
            invisible,
            "RPC type requires a visible descriptor import: other.Value",
        ),
        (message_set, "RPC MessageSet"),
        (lazy, "lazy RPC option"),
        (unresolved, "unresolved RPC descriptor options"),
        (deep, "RPC descriptor nesting exceeds limit"),
    ]
    .into_iter()
    .map(|(set, error)| (set.encode_to_vec(), error))
    .collect::<Vec<_>>();
    let mut future_file = descriptor().file[0].encode_to_vec();
    prost::encoding::encode_key(14, prost::encoding::WireType::Varint, &mut future_file);
    prost::encoding::encode_varint(1000, &mut future_file);
    let mut future = Vec::new();
    prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, &mut future);
    prost::encoding::encode_varint(future_file.len() as u64, &mut future);
    future.extend(future_file);
    inputs.push((future, "unsupported RPC descriptor field"));

    for (fields, expected) in [
        (
            vec![("foo_bar", None), ("fooBar", None)],
            "duplicate default RPC JSON field name",
        ),
        (
            vec![("first", Some("same")), ("second", Some("same"))],
            "duplicate effective RPC JSON field name",
        ),
        (vec![("first", Some("a\0b"))], "invalid RPC JSON field name"),
        (
            vec![("first", Some("[test.field]"))],
            "invalid RPC JSON field name",
        ),
    ] {
        let mut set = descriptor();
        set.file[0].message_type[0].field = fields
            .into_iter()
            .enumerate()
            .map(|(index, (name, json_name))| FieldDescriptorProto {
                name: Some(name.into()),
                number: Some(i32::try_from(index + 1).unwrap()),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                json_name: json_name.map(str::to_owned),
                ..Default::default()
            })
            .collect();
        inputs.push((set.encode_to_vec(), expected));
    }

    for (descriptor, expected) in inputs {
        let scratch = Scratch::new();
        let input = scratch.0.join("input.binpb");
        let output = scratch.0.join("generated");
        std::fs::write(&input, descriptor).unwrap();
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
