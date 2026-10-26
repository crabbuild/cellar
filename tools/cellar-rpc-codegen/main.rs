// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Build-time generation for Cellar's explicitly registered business services.
// Like tools/xtask, this process owns local build files and compiler subprocesses
// outside a node execution domain. The runtime crates retain the injected-I/O
// and output policies; this exception applies only to this build tool.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

mod artifact;
mod format;
mod graph;
mod javascript;
mod names;
mod python;
// Generated fixtures follow the same spacing policy as handwritten Rust. Keep
// one implementation of that policy in the repository's owning formatter.
mod rust;
#[path = "../xtask/src/spacing.rs"]
mod spacing;

use anyhow::{bail, Context, Result};
use cellar::sdk::rpc::{Contract, Limits};
use prost::Message;
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut descriptor = None;
    let mut output = None;
    let mut languages = String::from("rust");
    let mut javascript_tools = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../rpc");
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--descriptor" => {
                descriptor = Some(PathBuf::from(
                    args.next().context("missing descriptor path")?,
                ))
            }
            "--out" => output = Some(PathBuf::from(args.next().context("missing output path")?)),
            "--languages" => languages = args.next().context("missing language list")?,
            "--javascript-tools" => {
                javascript_tools =
                    PathBuf::from(args.next().context("missing JavaScript tools directory")?)
            }
            "--help" | "-h" => {
                println!("cellar-rpc-codegen --descriptor contracts.binpb --out generated [--languages rust,python,typescript,javascript] [--javascript-tools tools/rpc]");
                return Ok(());
            }
            _ => bail!("unknown code generator argument: {argument}"),
        }
    }
    let bytes = artifact::read_descriptor(&descriptor.context("--descriptor is required")?)?;
    let contract = Contract::from_descriptor(&bytes, Limits::default())?;
    let descriptor = prost_types::FileDescriptorSet::decode(contract.descriptor())?;
    names::validate(&descriptor)?;
    let languages: Vec<_> = languages.split(',').collect();
    anyhow::ensure!(
        !languages.is_empty()
            && languages.iter().all(|language| matches!(
                *language,
                "rust" | "python" | "typescript" | "javascript"
            )),
        "unsupported generation language"
    );
    let output = output.context("--out is required")?;
    std::fs::create_dir_all(&output)?;
    std::fs::write(output.join("contracts.binpb"), contract.descriptor())?;
    std::fs::write(
        output.join("rpc.json"),
        format!("{}\n", serde_json::to_string_pretty(&contract.inventory())?),
    )?;
    if languages.contains(&"rust") {
        let rust_dir = output.join("rust");
        std::fs::create_dir_all(&rust_dir)?;
        rust::generate(&rust_dir, &descriptor, contract.descriptor())?;
    }
    if languages.contains(&"python") {
        python::generate(&output, &descriptor, &contract)?;
    }
    for language in ["typescript", "javascript"] {
        if languages.contains(&language) {
            javascript::generate(&output, &descriptor, &contract, &javascript_tools, language)?;
        }
    }
    Ok(())
}
