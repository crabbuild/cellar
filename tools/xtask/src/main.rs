//! Repository tooling owns process and filesystem operations outside node execution.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]

mod spacing;

use anyhow::{bail, Context, Result};
use std::{
    env, fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

fn main() -> Result<()> {
    let arguments: Vec<_> = env::args().skip(1).collect();
    let check = match arguments
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["fmt"] => false,
        ["fmt", "--check"] => true,
        ["fmt", "--help"] | ["--help"] | [] => {
            println!("cargo format          Apply rustfmt and Cellar declaration spacing\ncargo format --check  Check formatting without changing source files");
            return Ok(());
        }
        _ => bail!("usage: cargo format [--check]"),
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .context("formatter must live under tools/xtask")?;
    if !format(root, check)? {
        bail!("formatting differs; run `cargo format` from the repository");
    }
    Ok(())
}

fn format(root: &Path, check: bool) -> Result<bool> {
    let inventory = Command::new("git")
        .args([
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            "*.rs",
        ])
        .current_dir(root)
        .output()
        .context("list Rust sources with Git")?;
    if !inventory.status.success() {
        bail!(
            "could not list Rust sources: {}",
            String::from_utf8_lossy(&inventory.stderr)
        );
    }
    let inventory =
        String::from_utf8(inventory.stdout).context("Rust source paths must be UTF-8")?;
    let mut paths: Vec<_> = inventory
        .split('\0')
        .filter(|path| !path.is_empty())
        .collect();
    paths.sort_unstable();
    paths.dedup();
    // A tracked source can be deleted locally before it is removed from the index.
    paths.retain(|path| root.join(path).is_file());

    let mut clean = true;
    // Format in memory and parse every file before writing. Standard rustfmt
    // reads stdin without traversing modules, so ignored files stay untouched.
    let mut updates = Vec::new();
    for relative in &paths {
        let path = root.join(relative);
        let source = fs::read_to_string(&path).with_context(|| format!("read {relative}"))?;
        let standard = rustfmt(root, &source).with_context(|| format!("rustfmt {relative}"))?;
        let spaced =
            spacing::separate_items(&standard).with_context(|| format!("parse {relative}"))?;
        if spaced != source {
            if check {
                eprintln!("{relative}: formatting differs; run cargo format");
                clean = false;
            } else {
                updates.push((path, spaced));
            }
        }
    }
    for (path, source) in updates {
        fs::write(&path, source).with_context(|| format!("write {}", path.display()))?;
    }
    if clean {
        println!(
            "{} {} Rust files",
            if check { "Checked" } else { "Formatted" },
            paths.len()
        );
    }
    Ok(clean)
}

fn rustfmt(root: &Path, source: &str) -> Result<String> {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2021", "--emit", "stdout"])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("run rustfmt; install the rustfmt toolchain component")?;
    child
        .stdin
        .take()
        .context("rustfmt stdin unavailable")?
        .write_all(source.as_bytes())?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        bail!("{}", String::from_utf8_lossy(&output.stderr));
    }
    String::from_utf8(output.stdout).context("rustfmt output must be UTF-8")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository() -> Result<tempfile::TempDir> {
        let root = tempfile::tempdir()?;
        let status = Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(root.path())
            .status()?;
        assert!(status.success());
        Ok(root)
    }

    #[test]
    fn check_is_read_only_and_apply_is_idempotent() -> Result<()> {
        let root = repository()?;
        let path = root.path().join("source with spaces.rs");
        let original = "use std::sync::Arc;\npub struct Supervisor;\nimpl Supervisor {\nfn first( ){}\nfn second(){}\n}\n";
        fs::write(&path, original)?;
        assert!(!format(root.path(), true)?);
        assert_eq!(fs::read_to_string(&path)?, original);
        assert!(format(root.path(), false)?);
        let formatted = fs::read_to_string(&path)?;
        assert!(formatted.contains("Arc;\n\npub struct Supervisor;\n\nimpl Supervisor"));
        assert!(formatted.contains("fn first() {}\n\n    fn second() {}"));
        assert!(format(root.path(), true)?);
        assert!(format(root.path(), false)?);
        assert_eq!(fs::read_to_string(&path)?, formatted);
        Ok(())
    }

    #[test]
    fn ignored_sources_and_deleted_tracked_files_are_excluded() -> Result<()> {
        let root = repository()?;
        fs::write(root.path().join(".gitignore"), "ignored.rs\n")?;
        fs::write(
            root.path().join("ignored.rs"),
            "invalid Rust preserved verbatim",
        )?;
        fs::write(root.path().join("removed.rs"), "fn removed() {}\n")?;
        assert!(Command::new("git")
            .args(["add", "removed.rs"])
            .current_dir(root.path())
            .status()?
            .success());
        fs::remove_file(root.path().join("removed.rs"))?;
        fs::write(
            root.path().join("new.rs"),
            "fn first() {}\nfn second() {}\n",
        )?;
        assert!(format(root.path(), false)?);
        assert_eq!(
            fs::read_to_string(root.path().join("ignored.rs"))?,
            "invalid Rust preserved verbatim"
        );
        assert_eq!(
            fs::read_to_string(root.path().join("new.rs"))?,
            "fn first() {}\n\nfn second() {}\n"
        );
        Ok(())
    }
}
