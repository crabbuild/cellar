// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cross-file descriptor semantics shared by startup and every generator.

mod imports;
mod symbols;

use super::contract::valid_identifier;
use anyhow::{ensure, Result};
use prost_types::FileDescriptorSet;

// These bound expanded names, not just their compact descriptor spelling. A
// long package repeated across many short declarations must not amplify into
// unbounded inventory allocations. Keep aggregate accounting before allocation.
const MAX_NAME_BYTES: usize = 1024;
const MAX_SYMBOL_BYTES: usize = 16 * 1024 * 1024;
const MAX_SYMBOLS: usize = 100_000;

pub(super) fn validate(set: &FileDescriptorSet) -> Result<()> {
    let imports = imports::Imports::new(&set.file)?;
    let mut symbols = symbols::Symbols::default();
    // Declare all packages first so a package/type conflict cannot depend on
    // descriptor order, including a package that has no declarations of its own.
    for file in &set.file {
        ensure!(
            file.syntax.as_deref() == Some("proto3"),
            "RPC contracts require proto3"
        );
        ensure!(file.extension.is_empty(), "RPC extensions are unsupported");
        let package = file.package.as_deref().unwrap_or_default();
        ensure!(
            package.len() <= MAX_NAME_BYTES && valid_identifier(package, true),
            "invalid or oversized RPC package"
        );
        symbols.package(package)?;
    }
    for (index, file) in set.file.iter().enumerate() {
        symbols.file(index, file)?;
    }
    symbols.validate_references(&imports)
}

#[cfg(test)]
mod tests;
