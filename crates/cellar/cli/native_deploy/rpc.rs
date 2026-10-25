// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Offline admission for generated RPC artifacts and native registration tables.
use crate::cli_output::{Format, Output};
use crate::sdk::rpc::{Catalog, Contract, Limits};
use anyhow::{bail, ensure, Context as _, Result};
use serde_json::Value;
use std::{collections::BTreeSet, io::Read, path::PathBuf, sync::Arc};

// Generated JSON can exceed its compact descriptor's size. Bound the build
// tool's reads independently; this is not a runtime codec or RSS limit.
const MAX_JSON_BYTES: usize = 64 * 1024 * 1024;
const HELP: &str = "cellar rpc check --descriptor FILE --inventory FILE [--catalog FILE]\n\
    Validate the portable descriptor and exact generated rpc.json offline.\n\
    --catalog also checks a canonical Catalog::inventory() registration table.\n\
    Inputs are read only; no compiler, guest runtime or live node is invoked.";

pub(super) fn run(arguments: Vec<String>) -> Result<()> {
    let mut arguments = arguments.into_iter();
    match arguments.next().as_deref() {
        None | Some("help" | "--help" | "-h") => {
            Output::new(Format::Text).line(format_args!("{HELP}"))?;
            return Ok(());
        }
        Some("check") => (),
        Some(_) => bail!("unknown RPC command; use `cellar rpc --help`"),
    }
    let mut descriptor = None;
    let mut inventory = None;
    let mut catalog = None;
    while let Some(flag) = arguments.next() {
        if matches!(flag.as_str(), "--help" | "-h") {
            Output::new(Format::Text).line(format_args!("{HELP}"))?;
            return Ok(());
        }
        let destination = match flag.as_str() {
            "--descriptor" => &mut descriptor,
            "--inventory" => &mut inventory,
            "--catalog" => &mut catalog,
            _ => bail!("unknown RPC check option {flag}"),
        };
        ensure!(destination.is_none(), "duplicate RPC check option {flag}");
        *destination = Some(PathBuf::from(
            arguments
                .next()
                .with_context(|| format!("{flag} requires a path"))?,
        ));
    }
    let descriptor = descriptor.context("RPC check requires --descriptor")?;
    let inventory = inventory.context("RPC check requires --inventory")?;
    let bytes = read(&descriptor, Contract::MAX_DESCRIPTOR_BYTES)?;
    let contract = Arc::new(Contract::from_descriptor(&bytes, Limits::default())?);
    let inventory = read(&inventory, MAX_JSON_BYTES)?;
    ensure!(
        inventory == canonical(&contract.inventory())?,
        "RPC inventory differs from the descriptor's generated digest, methods, limits or types; regenerate rpc.json"
    );
    let (classes, registrations) = match catalog {
        Some(path) => check_catalog(&read(&path, MAX_JSON_BYTES)?, contract.clone())?,
        None => (None, None),
    };
    // Nothing is printed as successful until every requested check completes.
    Output::new(Format::Json).line(format_args!(
        "{}",
        serde_json::json!({"format":1,"valid":true,"digest":contract.digest(),
            "methods":contract.methods().count(),"registered_classes":classes,
            "registered_methods":registrations})
    ))?;
    Ok(())
}

fn read(path: &std::path::Path, maximum: usize) -> Result<Vec<u8>> {
    ensure!(
        path.metadata()?.is_file(),
        "RPC input must be a regular file"
    );
    let file = std::fs::File::open(path)
        .with_context(|| format!("open RPC artifact {}", path.display()))?;
    let metadata = file.metadata()?;
    ensure!(metadata.is_file(), "RPC input must be a regular file");
    ensure!(
        metadata.len() <= maximum as u64,
        "RPC artifact exceeds its {maximum}-byte limit"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= maximum,
        "RPC artifact grew beyond its {maximum}-byte limit"
    );
    Ok(bytes)
}

fn canonical(value: &Value) -> Result<Vec<u8>> {
    Ok(format!("{}\n", serde_json::to_string_pretty(value)?).into_bytes())
}

fn check_catalog(bytes: &[u8], contract: Arc<Contract>) -> Result<(Option<usize>, Option<usize>)> {
    let inventory: Value = serde_json::from_slice(bytes).context("invalid RPC catalog JSON")?;
    let classes = inventory
        .as_object()
        .context("RPC catalog must be an object")?;
    let mut catalog = Catalog::new();
    let mut registrations = 0usize;
    for (class, entries) in classes {
        let entries = entries
            .as_object()
            .context("RPC catalog class must contain methods")?;
        let mut services = BTreeSet::new();
        for name in entries.keys() {
            let method = contract
                .method(name)
                .context("RPC catalog contains a method outside this descriptor")?;
            services.insert(
                method
                    .name
                    .split_once('/')
                    .context("RPC method has no service")?
                    .0,
            );
        }
        // The native catalog owns class/service admission and requires complete
        // service registration. Reuse it instead of inventing a CLI policy.
        for service in services {
            catalog.register(class, service, contract.clone())?;
        }
        registrations = registrations
            .checked_add(entries.len())
            .context("RPC catalog count overflow")?;
    }
    // Exact canonical bytes also reject duplicate JSON keys, altered codec
    // metadata and partial services; a last-key-wins parser cannot hide drift.
    ensure!(
        bytes == canonical(&catalog.inventory())?,
        "RPC catalog differs from complete native registrations; regenerate Catalog::inventory()"
    );
    Ok((Some(classes.len()), Some(registrations)))
}
