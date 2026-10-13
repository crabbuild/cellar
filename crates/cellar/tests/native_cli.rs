//! The shipped CLI exposes the Rust delivery workflow.
use anyhow::{ensure, Result};
use std::process::Command;

#[test]
fn default_cli_builds_rust_and_retains_fleet_operator_commands() -> Result<()> {
    let executable = env!("CARGO_BIN_EXE_cellar");
    let help = Command::new(executable).arg("--help").output()?;
    ensure!(help.status.success(), "help failed: {:?}", help);
    let help = String::from_utf8(help.stdout)?;
    ensure!(help.contains("cellar build PROJECT.json"));
    ensure!(help.contains("Run a compiled application executable"));
    let version = Command::new(executable).arg("--version").output()?;
    ensure!(version.status.success());
    ensure!(
        String::from_utf8(version.stdout)?.trim() == concat!("cellar ", env!("CARGO_PKG_VERSION"))
    );
    let sql = Command::new(executable).args(["sql", "--help"]).output()?;
    ensure!(sql.status.success(), "SQL operator help failed: {:?}", sql);
    ensure!(String::from_utf8(sql.stdout)?.contains("cellar sql"));
    let dev = Command::new(executable).args(["dev", "--help"]).output()?;
    ensure!(dev.status.success(), "dev help failed: {:?}", dev);
    ensure!(String::from_utf8(dev.stdout)?.starts_with("cellar dev PROJECT.json"));
    Ok(())
}
