//! The shipped CLI exposes the Rust delivery workflow.
use anyhow::{ensure, Result};
use std::process::Command;

#[test]
fn default_cli_builds_rust_and_retains_fleet_operator_commands() -> Result<()> {
    let executable = env!("CARGO_BIN_EXE_cellar");
    {
        let help = Command::new(executable).arg("--help").output()?;
        ensure!(help.status.success(), "help failed: {:?}", help);
        let help = String::from_utf8(help.stdout)?;
        ensure!(help.contains("cellar build PROJECT.json"));
        ensure!(help.contains("Run a compiled application executable"));
        let version = Command::new(executable).arg("--version").output()?;
        ensure!(version.status.success());
        ensure!(
            String::from_utf8(version.stdout)?.trim()
                == concat!("cellar ", env!("CARGO_PKG_VERSION"))
        );
        let sql = Command::new(executable).args(["sql", "--help"]).output()?;
        ensure!(sql.status.success(), "SQL operator help failed: {:?}", sql);
        ensure!(String::from_utf8(sql.stdout)?.contains("cellar sql"));
        let fleet = Command::new(executable)
            .args(["fleet", "upgrade", "--help"])
            .output()?;
        ensure!(
            fleet.status.success(),
            "fleet upgrade help failed: {fleet:?}"
        );
        let help = String::from_utf8(fleet.stdout)?;
        ensure!(help.contains("--confirm-stopped-writers") && help.contains("protocol 3"));
        let unconfirmed = Command::new(executable)
            .args([
                "fleet",
                "upgrade",
                "--operation",
                "00000000000000000000000000000099",
                "--reason",
                "test incomplete coordination",
            ])
            .output()?;
        ensure!(!unconfirmed.status.success());
        ensure!(String::from_utf8(unconfirmed.stderr)?
            .contains("all old writers and recovery processes are stopped"));
    }
    Ok(())
}
