// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
// Copyright 2019 PingCAP, Inc. Licensed under Apache-2.0.
// Modified for Cellar: use a pinned pure Rust parser instead of system protoc.
// Strict enum decoding retains protobuf-build 0.14.1's upstream rewrite.

use std::path::PathBuf;

fn main() {
    assert!(std::env::var_os("CARGO_FEATURE_PROTOBUF_CODEC").is_some(),
        "Cellar's raft-proto integration supports protobuf-codec only");
    assert!(std::env::var_os("CARGO_FEATURE_PROST_CODEC").is_none(),
        "prost-codec is not supported by Cellar's selected engine integration");
    let base = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("protos");
    std::fs::create_dir_all(&out).unwrap();
    protobuf_codegen_pure::Codegen::new()
        .out_dir(&out)
        .inputs(&[base.join("proto/eraftpb.proto")])
        .includes(&[base.join("include"), base.join("proto")])
        .run().expect("generate pinned Raft protobuf definitions");
    let generated = out.join("eraftpb.rs");
    let source = std::fs::read_to_string(&generated).unwrap();
    let enums = regex::Regex::new(
        r"::protobuf::rt::read_proto3_enum_with_unknown_fields_into\(([^,]+), ([^,]+), &mut ([^,]+), [^\)]+\)\?").unwrap();
    let source = enums.replace_all(&source,
        "if $1 == ::protobuf::wire_format::WireTypeVarint { $3 = $2.read_enum()?; } else { return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type)); }");
    std::fs::write(&generated, source.as_bytes()).unwrap();
    std::fs::write(out.join("mod.rs"), "pub mod eraftpb;\n").unwrap();
    println!("cargo:rerun-if-changed=proto/eraftpb.proto");
    println!("cargo:rerun-if-changed=include/rustproto.proto");
}
