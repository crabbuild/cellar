//! Explicit operator entry for the coordinated event-delivery cutover.

use anyhow::{bail, Context, Result};

const HELP: &str =
    "cellar fleet upgrade --operation UUID --reason TEXT --confirm-stopped-writers [FLEET OPTIONS]

Stop every old writer and recovery process, then wait for all node leases to
expire before upgrading. Existing cell, log and wake objects are preserved.
Rebuild application executables for native host protocol 3 before restarting.
The operation UUID is 32 lowercase hexadecimal characters. Repeat the exact
operation and reason after an ambiguous response. Downgrade is unsupported.";

pub(super) async fn run(arguments: Vec<String>) -> Result<()> {
    let mut arguments = arguments.into_iter();
    let command = arguments.next().unwrap_or_else(|| "--help".into());
    if matches!(command.as_str(), "--help" | "help" | "-h") {
        return help();
    }
    if command != "upgrade" {
        bail!("unknown fleet command {command}");
    }
    let mut fleet = crate::cli_options::FleetFlags::default();
    let mut operation = None;
    let mut reason = None;
    let mut confirmed = false;
    let mut json = false;
    while let Some(argument) = arguments.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            return help();
        }
        if argument == "--confirm-stopped-writers" {
            confirmed = true;
            continue;
        }
        if argument == "--json" {
            json = true;
            continue;
        }
        let mut value = |flag: &str| {
            arguments
                .next()
                .with_context(|| format!("{flag} requires a value"))
        };
        if fleet.consume(&argument, &mut value)? {
            continue;
        }
        match argument.as_str() {
            "--operation" => operation = Some(value("--operation")?),
            "--reason" => reason = Some(value("--reason")?),
            _ => bail!("unknown fleet upgrade option {argument}"),
        }
    }
    let request = crate::wake_format::Upgrade {
        operation: operation.context("fleet upgrade requires --operation")?,
        reason: reason.context("fleet upgrade requires --reason")?,
        confirmed_stopped_writers: confirmed,
    };
    request.validate()?;
    let bucket = fleet.resolve("cellar fleet upgrade")?.open().await?;
    let receipt = crate::wake_format::upgrade(&bucket, &request).await?;
    let output = crate::cli_output::Output::new(if json {
        crate::cli_output::Format::Json
    } else {
        crate::cli_output::Format::Text
    });
    if json {
        output.line(format_args!(
            "{}",
            serde_json::json!({"format":crate::wake_format::FORMAT,"upgrade":receipt})
        ))?;
    } else {
        output.line(format_args!(
            "fleet format {} ready; operation {}",
            crate::wake_format::FORMAT,
            receipt.as_ref().map_or("fresh storage", |receipt| receipt
                .request
                .operation
                .as_str())
        ))?;
    }
    Ok(())
}

fn help() -> Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).line(format_args!(
        "{HELP}\n\n{}\n  --json              Print the durable upgrade receipt as JSON",
        crate::cli_options::FLEET_HELP
    ))?;
    Ok(())
}
