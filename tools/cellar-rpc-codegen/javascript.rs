// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invoke pinned message/service plugins from the same descriptor artifact.
use anyhow::{ensure, Context, Result};
use cellar::sdk::rpc::Contract;
use prost_types::FileDescriptorSet;
use std::{path::Path, process::Command};

pub fn generate(
    output: &Path,
    descriptor: &FileDescriptorSet,
    contract: &Contract,
    tools: &Path,
    language: &str,
) -> Result<()> {
    let directory = output.join(language);
    std::fs::create_dir_all(&directory)?;
    let es = tools
        .join("node_modules/.bin/protoc-gen-es")
        .canonicalize()
        .context("install the pinned JavaScript tools with npm ci --prefix tools/rpc")?;
    let cellar = tools
        .join("protoc-gen-cellar.mjs")
        .canonicalize()
        .context("Cellar JavaScript service plugin is missing")?;
    let target = if language == "typescript" {
        "ts"
    } else {
        "js+dts"
    };
    let mut command = Command::new("protoc");
    command
        .arg(format!(
            "--descriptor_set_in={}",
            output.join("contracts.binpb").display()
        ))
        .arg(format!("--plugin=protoc-gen-es={}", es.display()))
        .arg(format!("--plugin=protoc-gen-cellar={}", cellar.display()))
        .arg(format!("--es_out={}", directory.display()))
        .arg(format!("--cellar_out={}", directory.display()))
        .arg(format!("--es_opt=target={target},import_extension=js"))
        .arg(format!(
            "--cellar_opt=target={target},import_extension=js,digest={}",
            contract.digest()
        ));
    for file in &descriptor.file {
        let name = file
            .name
            .as_deref()
            .context("descriptor filename is required")?;
        if !name.starts_with("google/protobuf/") {
            command.arg(name);
        }
    }
    let result = command
        .output()
        .context("JavaScript generation requires protoc and Node.js on PATH")?;
    ensure!(
        result.status.success(),
        "JavaScript generation failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    super::format::generated_javascript(&directory)
}
