//! Embed only explicitly selected immutable Node inventory metadata.
// Build scripts run outside the Cellar execution domain.
#![allow(clippy::disallowed_methods)]
use std::{
    io::{Read, Write},
    path::PathBuf,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = std::io::stdout().lock();
    writeln!(output, "cargo:rerun-if-env-changed=CELLAR_NODE_INVENTORY")?;
    let mut bytes = Vec::new();
    if std::env::var_os("CARGO_FEATURE_NODE").is_some() {
        if let Some(path) = std::env::var_os("CELLAR_NODE_INVENTORY") {
            writeln!(
                output,
                "cargo:rerun-if-changed={}",
                PathBuf::from(&path).display()
            )?;
            std::fs::File::open(path)?
                .take(32 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 32 * 1024 * 1024 {
                return Err("Node inventory exceeds byte limit".into());
            }
        }
    }
    std::fs::write(
        PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR missing")?)
            .join("node_inventory.json"),
        bytes,
    )?;
    Ok(())
}
