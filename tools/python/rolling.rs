// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Two immutable Python versions in distinct installed processes. Development
//! bucket semantics are real; this does not emulate quorum fault qualification.
#![allow(clippy::disallowed_methods)]
use super::{installed_host::InstalledHost, Process};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    native_deploy::{current_target, Package},
};
use serde_json::{json, Value};
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub(super) async fn qualify(old: &Path, new: &Path, scratch: &Path) -> Result<()> {
    let old_data: Package = serde_json::from_slice(&std::fs::read(old.join("package.json"))?)?;
    let new_data: Package = serde_json::from_slice(&std::fs::read(new.join("package.json"))?)?;
    old_data.validate()?;
    new_data.validate()?;
    let before = old_data
        .primary
        .native
        .as_ref()
        .context("old native descriptor missing")?;
    let after = new_data
        .primary
        .native
        .as_ref()
        .context("new native descriptor missing")?;
    ensure!(
        old_data.primary.script_name == "python-example"
            && new_data.primary.script_name == "python-example",
        "rolling probe requires the Python example in both packages"
    );
    ensure!(
        before.application_version != after.application_version,
        "rolling qualification requires different compiled application versions"
    );
    let old_runtime = before
        .python
        .iter()
        .find(|entry| entry.target == current_target())
        .context("old current-target runtime missing")?;
    let new_runtime = after
        .python
        .iter()
        .find(|entry| entry.target == current_target())
        .context("new current-target runtime missing")?;
    ensure!(
        old_runtime.executable_sha256 != new_runtime.executable_sha256,
        "rolling qualification requires different executable bytes"
    );
    ensure!(
        old_runtime.inventory_sha256 != new_runtime.inventory_sha256,
        "rolling qualification requires a changed source/environment inventory"
    );
    for reference in [old_runtime, new_runtime] {
        ensure!(
            reference.prefix.starts_with(scratch) && reference.prefix != scratch,
            "rolling runtime must live inside explicit scratch prefix"
        );
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    for signal in ["-TERM", "-INT"] {
        cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
            "Qualifying Python process handoff with {signal}"
        ))?;
        let work = tempfile::tempdir_in(scratch)?;
        ensure!(
            !work.path().starts_with(&old_runtime.prefix)
                && !work.path().starts_with(&new_runtime.prefix),
            "rolling work cannot live inside runtime prefix"
        );
        let store = work.path().join("fleet.sqlite");
        let bucket = Bucket::open_dev(&store)?;
        let old_host = work.path().join("application-before");
        let new_host = work.path().join("application-after");
        old_data.publish_staged(&bucket, old).await?;
        old_data
            .install_with_python(
                &bucket,
                current_target(),
                &old_host,
                Some(&old_runtime.prefix),
            )
            .await?;
        let mut owner =
            InstalledHost::start(&old_host, &store, work.path(), "python-before").await?;
        let sealed = increment(&client, &owner.origin, "before-upgrade", 3).await?;
        ensure!(sealed.value()?["count"] == 3);
        new_data.publish_staged(&bucket, new).await?;
        new_data
            .install_with_python(
                &bucket,
                current_target(),
                &new_host,
                Some(&new_runtime.prefix),
            )
            .await?;
        owner
            .observe_refused_replacement(&new_data.primary.version)
            .await?;
        check_package(&old_host, old, true).await?;
        check_package(&old_host, new, false).await?;
        check_package(&new_host, new, true).await?;
        let mut replacement =
            InstalledHost::start(&new_host, &store, work.path(), "python-after").await?;
        owner.ensure_live()?;
        ensure!(
            increment(&client, &replacement.origin, "before-upgrade", 3).await? == sealed,
            "new ingress changed a proven receipt held by the old owner"
        );
        owner.signal(signal).await?;
        replacement.ensure_live()?;
        let recovered = client
            .get(format!("{}/counters/rolling", replacement.origin))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            recovered
                .headers()
                .get("x-cellar-code-revision")
                .is_some_and(|value| value == "rolling-v2"),
            "replacement did not execute the changed Python cell handler"
        );
        let state: Value = recovered.json().await?;
        ensure!(
            state["count"] == 3,
            "handoff lost acknowledged state: {state}"
        );
        ensure!(
            increment(&client, &replacement.origin, "after-upgrade", 2)
                .await?
                .value()?["count"]
                == 5,
            "replacement could not mutate recovered state"
        );
        ensure!(
            increment(&client, &replacement.origin, "before-upgrade", 3).await? == sealed,
            "replacement failed to replay the exact pre-upgrade response"
        );
        replacement.stop(&client, true).await?;
    }
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
        "PASS Python rolling processes: different executable/application versions, package mismatch refused, TERM/INT handoff, recovered state/receipts and complete reports"))?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Receipt {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Receipt {
    pub(super) fn value(&self) -> Result<Value> {
        Ok(serde_json::from_slice(&self.body)?)
    }
}

pub(super) async fn increment(
    client: &reqwest::Client,
    origin: &str,
    key: &str,
    amount: u64,
) -> Result<Receipt> {
    let response = client
        .post(format!("{origin}/counters/rolling"))
        .header("idempotency-key", key)
        .json(&json!({"amount":amount}))
        .send()
        .await?
        .error_for_status()?;
    // Only these application fields exist in the base/changed-source fixture.
    // Exclude generated transport metadata, but catch a revision header added
    // to a replay rather than returned from the retained response.
    let headers = response
        .headers()
        .iter()
        .filter(|(name, _)| matches!(name.as_str(), "content-type" | "x-cellar-code-revision"))
        .map(|(name, value)| Ok((name.to_string(), value.to_str()?.to_string())))
        .collect::<Result<Vec<_>>>()?;
    Ok(Receipt {
        status: response.status().as_u16(),
        headers,
        body: response.bytes().await?.to_vec(),
    })
}

async fn check_package(host: &Path, package: &Path, accepted: bool) -> Result<()> {
    let output = tempfile::NamedTempFile::new()?;
    let mut child = Process(
        Command::new(host)
            .env_clear()
            .arg("--check-native-package")
            .arg(package.join("package.json"))
            .stdout(Stdio::from(output.as_file().try_clone()?))
            .stderr(Stdio::from(output.as_file().try_clone()?))
            .spawn()?,
    );
    let started = Instant::now();
    let deadline = started + Duration::from_secs(15);
    loop {
        if let Some(status) = child.0.try_wait()? {
            if status.success() != accepted {
                let (_, path) = output
                    .keep()
                    .map_err(|error| anyhow::anyhow!(error.error))?;
                anyhow::bail!("package check acceptance {accepted} contradicted by {status}; host={}, package={}, diagnostics={}",
                    host.display(), package.display(), path.display());
            }
            cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(
                format_args!(
                    "PASS Python package checker: host={}, accepted={accepted}, elapsed={:?}",
                    host.file_name()
                        .context("checker host filename")?
                        .to_string_lossy(),
                    started.elapsed(),
                ),
            )?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            let _ = child.0.kill();
            let _ = child.0.wait();
            let (_, path) = output
                .keep()
                .map_err(|error| anyhow::anyhow!(error.error))?;
            anyhow::bail!("rolling package checker timed out; host={}, package={}, accepted={accepted}, elapsed={:?}, diagnostics={}",
                host.display(), package.display(), started.elapsed(), path.display());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
