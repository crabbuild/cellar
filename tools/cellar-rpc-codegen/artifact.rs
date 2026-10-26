// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bound build-time input before native descriptor parsing or output creation.
use anyhow::{ensure, Context, Result};
use cellar::sdk::rpc::Contract;
use std::{fs::File, io::Read, path::Path};

pub(super) fn read_descriptor(path: &Path) -> Result<Vec<u8>> {
    // Check before open as well: opening a FIFO for reading can wait for a
    // writer forever. The opened handle is checked again before allocation.
    ensure!(
        path.metadata()
            .with_context(|| format!("inspect RPC descriptor {}", path.display()))?
            .is_file(),
        "RPC descriptor input must be a regular file"
    );
    let file =
        File::open(path).with_context(|| format!("open RPC descriptor {}", path.display()))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file(),
        "RPC descriptor input must be a regular file"
    );
    let maximum = Contract::MAX_DESCRIPTOR_BYTES;
    ensure!(
        metadata.len() <= maximum as u64,
        "RPC descriptor file exceeds its {maximum}-byte limit"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= maximum,
        "RPC descriptor file grew beyond its {maximum}-byte limit"
    );
    Ok(bytes)
}
