// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The shipped CLI checks real generated bundles without compilers or a node.
// These fixtures own disposable build files outside node execution domains.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::rpc::{Catalog, Contract, Limits};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    process::{Command, Output},
    sync::Arc,
};

struct Fixture {
    root: tempfile::TempDir,
    descriptor: PathBuf,
    inventory: PathBuf,
}

impl Fixture {
    fn new(bundle: &str) -> Result<Self> {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tools/rpc/fixtures")
            .join(bundle);
        let root = tempfile::tempdir()?;
        let descriptor = root.path().join("contracts.binpb");
        let inventory = root.path().join("rpc.json");
        std::fs::copy(source.join("contracts.binpb"), &descriptor)?;
        std::fs::copy(source.join("rpc.json"), &inventory)?;
        std::fs::write(root.path().join("user.txt"), "preserve unrelated work")?;
        Ok(Self {
            root,
            descriptor,
            inventory,
        })
    }

    fn check(&self, extra: &[&str]) -> Result<Output> {
        let before_descriptor = std::fs::read(&self.descriptor)?;
        let before_inventory = std::fs::read(&self.inventory)?;
        let catalog = extra.get(1).filter(|_| extra.first() == Some(&"--catalog"));
        let before_catalog = catalog.map(std::fs::read).transpose()?;
        let output = Command::new(env!("CARGO_BIN_EXE_cellar"))
            .current_dir(self.root.path())
            .env("PATH", "")
            .args(["rpc", "check", "--descriptor"])
            .arg(&self.descriptor)
            .arg("--inventory")
            .arg(&self.inventory)
            .args(extra)
            .output()?;
        ensure!(std::fs::read(&self.descriptor)? == before_descriptor);
        ensure!(std::fs::read(&self.inventory)? == before_inventory);
        if let (Some(path), Some(before)) = (catalog, before_catalog) {
            ensure!(std::fs::read(path)? == before);
        }
        ensure!(
            std::fs::read_to_string(self.root.path().join("user.txt"))?
                == "preserve unrelated work"
        );
        ensure!(
            std::fs::read_dir(self.root.path())?.count()
                == if extra.first() == Some(&"--catalog") {
                    4
                } else {
                    3
                }
        );
        Ok(output)
    }
}

fn canonical(value: &Value) -> Result<Vec<u8>> {
    Ok(format!("{}\n", serde_json::to_string_pretty(value)?).into_bytes())
}

fn refused(output: Output, category: &str) -> Result<()> {
    ensure!(!output.status.success(), "accepted invalid artifact");
    ensure!(
        output.stdout.is_empty(),
        "published success before validation"
    );
    ensure!(
        String::from_utf8(output.stderr)?.contains(category),
        "missing refusal category {category}"
    );
    Ok(())
}

#[test]
fn checks_real_counter_and_portable_profile_without_build_tools() -> Result<()> {
    for bundle in ["generated", "profile-generated"] {
        let fixture = Fixture::new(bundle)?;
        let output = fixture.check(&[])?;
        ensure!(output.status.success(), "RPC check failed: {output:?}");
        let report: Value = serde_json::from_slice(&output.stdout)?;
        let inventory: Value = serde_json::from_slice(&std::fs::read(&fixture.inventory)?)?;
        ensure!(report["format"] == 1 && report["valid"] == true);
        ensure!(report["digest"] == inventory["digest"]);
        ensure!(
            report["methods"].as_u64()
                == Some(inventory["methods"].as_array().unwrap().len() as u64)
        );
        ensure!(report["registered_classes"].is_null() && report["registered_methods"].is_null());
    }
    Ok(())
}

#[test]
fn refuses_inventory_drift_ambiguous_json_and_invalid_descriptors_without_writes() -> Result<()> {
    for field in [
        "digest",
        "methods",
        "limits",
        "types",
        "extra",
        "duplicate",
        "formatting",
    ] {
        let fixture = Fixture::new("generated")?;
        let original = std::fs::read(&fixture.inventory)?;
        let mut inventory: Value = serde_json::from_slice(&original)?;
        match field {
            "digest" => inventory["digest"] = json!("0".repeat(64)),
            "methods" => inventory["methods"][0]["output"] = json!("example.counter.v1.Count"),
            "limits" => inventory["limits"]["max_depth"] = json!(32),
            "types" => inventory["types"] = json!({}),
            "extra" => inventory["unregistered"] = json!(true),
            "duplicate" | "formatting" => (),
            _ => unreachable!(),
        }
        let bytes = match field {
            "duplicate" => format!(
                "{{\"digest\":\"wrong\",{}",
                std::str::from_utf8(&original)?[1..].to_owned()
            )
            .into_bytes(),
            "formatting" => serde_json::to_vec(&inventory)?,
            _ => canonical(&inventory)?,
        };
        std::fs::write(&fixture.inventory, bytes)?;
        refused(fixture.check(&[])?, "RPC inventory differs")?;
    }
    let fixture = Fixture::new("generated")?;
    std::fs::write(&fixture.descriptor, b"not a descriptor")?;
    refused(fixture.check(&[])?, "invalid RPC descriptor")?;
    Ok(())
}

#[test]
fn checks_complete_native_catalogs_and_refuses_stale_or_partial_registrations() -> Result<()> {
    let fixture = Fixture::new("generated")?;
    let contract = Arc::new(Contract::from_descriptor(
        &std::fs::read(&fixture.descriptor)?,
        Limits::default(),
    )?);
    let mut catalog = Catalog::new();
    catalog.register("Counter", "example.counter.v1.Counter", contract)?;
    let valid = catalog.inventory();
    let path = fixture.root.path().join("catalog.json");
    let extra = ["--catalog", path.to_str().unwrap()];
    std::fs::write(&path, canonical(&valid)?)?;
    let output = fixture.check(&extra)?;
    ensure!(output.status.success(), "catalog check failed: {output:?}");
    let report: Value = serde_json::from_slice(&output.stdout)?;
    ensure!(report["registered_classes"] == 1 && report["registered_methods"] == 2);
    for field in [
        "digest",
        "limits",
        "partial",
        "unknown",
        "class",
        "duplicate",
    ] {
        let mut inventory = valid.clone();
        match field {
            "digest" => {
                inventory["Counter"]["example.counter.v1.Counter/GetCount"]["digest"] =
                    json!("0".repeat(64))
            }
            "limits" => {
                inventory["Counter"]["example.counter.v1.Counter/GetCount"]["limits"]["max_depth"] =
                    json!(32)
            }
            "partial" => {
                inventory["Counter"]
                    .as_object_mut()
                    .unwrap()
                    .remove("example.counter.v1.Counter/Increment");
            }
            "unknown" => {
                inventory["Counter"]["example.counter.v1.Counter/PrivateHelper"] = json!({})
            }
            "class" => {
                let methods = inventory
                    .as_object_mut()
                    .unwrap()
                    .remove("Counter")
                    .unwrap();
                inventory["Bad:scope"] = methods;
            }
            "duplicate" => (),
            _ => unreachable!(),
        }
        let bytes = if field == "duplicate" {
            format!(
                "{{\"Counter\":{{}},{}",
                std::str::from_utf8(&canonical(&valid)?)?[1..].to_owned()
            )
            .into_bytes()
        } else {
            canonical(&inventory)?
        };
        std::fs::write(&path, bytes)?;
        refused(
            fixture.check(&extra)?,
            if field == "class" {
                "invalid RPC class"
            } else if field == "unknown" {
                "outside this descriptor"
            } else {
                "RPC catalog differs"
            },
        )?;
    }
    Ok(())
}

#[test]
fn bounds_input_reads_and_refuses_duplicate_options() -> Result<()> {
    for (artifact, maximum) in [
        ("contracts.binpb", 4 * 1024 * 1024),
        ("rpc.json", 64 * 1024 * 1024),
    ] {
        let fixture = Fixture::new("generated")?;
        let path = fixture.root.path().join(artifact);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)?
            .set_len(maximum + 1)?;
        let output = Command::new(env!("CARGO_BIN_EXE_cellar"))
            .env("PATH", "")
            .args(["rpc", "check", "--descriptor"])
            .arg(&fixture.descriptor)
            .arg("--inventory")
            .arg(&fixture.inventory)
            .output()?;
        refused(output, "byte limit")?;
        ensure!(std::fs::metadata(&path)?.len() == maximum + 1);
    }
    let fixture = Fixture::new("generated")?;
    refused(
        fixture.check(&["--descriptor", "missing.binpb"])?,
        "duplicate RPC check option",
    )?;
    refused(
        fixture.check(&["--out", "must-not-exist"])?,
        "unknown RPC check option",
    )?;
    ensure!(!fixture.root.path().join("must-not-exist").exists());
    Ok(())
}
