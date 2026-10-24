// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Context, Result};
use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

const COPYRIGHT: &str = "Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.";

fn notice(source: &str, comment: &str) -> String {
    let header = format!("{comment} {COPYRIGHT}\n\n");
    if source.starts_with(&header) {
        source.to_owned()
    } else {
        format!("{header}{source}")
    }
}

/// Keep plugin output reproducible with exactly one final newline.
pub fn generated_javascript(directory: &Path) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            generated_javascript(&path)?;
        } else if path
            .extension()
            .is_some_and(|suffix| suffix == "js" || suffix == "ts")
        {
            let source = std::fs::read_to_string(&path)?;
            std::fs::write(path, notice(&format!("{}\n", source.trim_end()), "//"))?;
        }
    }
    Ok(())
}

pub fn generated_rust(directory: &Path) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_none_or(|suffix| suffix != "rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path)?;
        let mut child = Command::new("rustfmt")
            .args(["--edition", "2021", "--emit", "stdout"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("RPC generation requires the rustfmt component")?;
        child
            .stdin
            .take()
            .context("rustfmt stdin unavailable")?
            .write_all(source.as_bytes())?;
        let output = child.wait_with_output()?;
        ensure!(
            output.status.success(),
            "generated RPC source did not format"
        );
        let source = String::from_utf8(output.stdout)?;
        std::fs::write(
            path,
            notice(&super::spacing::separate_items(&source)?, "//"),
        )?;
    }
    Ok(())
}

pub fn generated_python(directory: &Path) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            generated_python(&path)?;
        } else if path
            .extension()
            .is_some_and(|suffix| suffix == "py" || suffix == "pyi")
        {
            let source = std::fs::read_to_string(&path)?;
            std::fs::write(path, notice(&source, "#"))?;
        }
    }
    Ok(())
}
