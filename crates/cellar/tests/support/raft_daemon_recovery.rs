// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real hard-killed voter, immutable command retries and retained-disk resume.
//! This small dev-store fixture is not a 10 GB or object-store RTO campaign.

use super::*;
use std::{fs::OpenOptions, io::Write};

pub(super) struct Ledger {
    file: File,
    started: Instant,
}

impl Ledger {
    pub(super) fn new(root: &Path, backend: &str) -> Result<Self> {
        let path = std::env::var_os("CELLAR_RAFT_PROCESS_EVIDENCE")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join("process-recovery-ledger.jsonl"));
        ensure!(path.is_absolute(), "process evidence path must be absolute");
        let parent = path.parent().context("process evidence parent absent")?;
        let mut ledger = Self {
            file: OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?,
            started: Instant::now(),
        };
        ledger.record(serde_json::json!({
            "format": 1, "phase": "started", "scope": "retained_disk_hard_kill",
            "store": backend, "operator": "cli_admin", "archive_setup": "cli_admin",
            "voter_startup": "owned_fresh_quorum", "qualified_10gb": false,
        }))?;
        cellar_ltx::FileSystem::sync_all(&cellar_ltx::DirectFileSystem, parent)?;
        Ok(ledger)
    }

    pub(super) fn record(&mut self, mut event: Value) -> Result<()> {
        event
            .as_object_mut()
            .context("ledger event must be an object")?
            .insert(
                "controller_elapsed_us".into(),
                u64::try_from(self.started.elapsed().as_micros())?.into(),
            );
        serde_json::to_writer(&mut self.file, &event)?;
        self.file.write_all(b"\n")?;
        // The parent survives the injected worker kill. Persist request identity
        // before sending so a lost response remains an explicit ambiguity.
        self.file.sync_all()?;
        Ok(())
    }
}

pub(super) struct Acknowledged {
    pub(super) cell: &'static str,
    pub(super) ingress: Ingress,
    pub(super) leader: usize,
    pub(super) command: String,
    pub(super) retry_until: String,
    pub(super) receipt: Value,
}

struct Request<'a> {
    cell: &'a str,
    ingress: Ingress,
    command: &'a str,
    retry_until: &'a str,
    delta: i64,
}

impl Request<'_> {
    async fn send(
        &self,
        client: &reqwest::Client,
        addresses: &[ManagedIngress],
        excluded: Option<usize>,
        ledger: &mut Ledger,
    ) -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for (index, address) in addresses.iter().enumerate() {
                let public = self.ingress.endpoint(address);
                if Some(index) == excluded {
                    continue;
                }
                ensure!(
                    Instant::now() < deadline,
                    "command reconciliation deadline expired"
                );
                ledger.record(serde_json::json!({
                    "phase": "send", "cell": self.cell, "command": self.command,
                    "retry_until": self.retry_until, "delta": self.delta, "member": index + 1,
                    "ingress": self.ingress.label(),
                }))?;
                let response = client
                    .post(format!(
                        "{public}/counters/{}?delta={}",
                        self.cell, self.delta
                    ))
                    .bearer_auth(SECRET)
                    .header("x-command-id", self.command)
                    .header("x-retry-until-ms", self.retry_until)
                    .send()
                    .await;
                let (status, body) = match response {
                    Ok(response) => {
                        let status = response.status();
                        match response.text().await {
                            Ok(body) => (status, body),
                            Err(error) => {
                                ledger.record(serde_json::json!({"phase":"ambiguous", "cell":self.cell, "command":self.command, "error":error.to_string()}))?;
                                continue;
                            }
                        }
                    }
                    Err(error) => {
                        ledger.record(serde_json::json!({"phase":"ambiguous", "cell":self.cell, "command":self.command, "error":error.to_string()}))?;
                        continue;
                    }
                };
                if status.is_success() {
                    let receipt: Value = serde_json::from_str(&body)?;
                    ledger.record(serde_json::json!({"phase":"acknowledged", "cell":self.cell, "command":self.command, "receipt":receipt, "ingress":self.ingress.label()}))?;
                    return Ok(receipt);
                }
                ledger.record(serde_json::json!({"phase":"refused", "cell":self.cell, "command":self.command, "status":status.as_u16(), "body":body}))?;
                ensure!(
                    status == 503,
                    "unexpected reconciliation failure {status}: {body}"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn matching_receipt(actual: &Value, expected: &Value) -> Result<()> {
    for field in ["value", "group", "group_incarnation", "cell", "incarnation"] {
        ensure!(
            actual[field] == expected[field],
            "replayed receipt differs at {field}: {actual}"
        );
    }
    // The SDK returns the exact cached result with a version covering the
    // immutable image containing that receipt, not necessarily its first image.
    let index = actual["index"].as_u64().context("receipt index absent")?;
    let term = actual["term"].as_u64().context("receipt term absent")?;
    let minimum = expected["index"]
        .as_u64()
        .context("original index absent")?;
    let minimum_term = expected["term"].as_u64().context("original term absent")?;
    ensure!(
        index >= minimum && term >= minimum_term && (index != minimum || term == minimum_term),
        "replayed receipt has a regressed or different history: {actual}"
    );
    Ok(())
}

pub(super) async fn hard_kill_and_resume(
    root: &Path,
    client: &reqwest::Client,
    processes: &mut [Process],
    addresses: &mut [ManagedIngress],
    acknowledged: &[Acknowledged],
    ledger: &mut Ledger,
) -> Result<()> {
    ensure!(
        acknowledged.len() == 2,
        "both cells need acknowledged commands"
    );
    let killed = acknowledged[0].leader;
    let old_pid = processes[killed].pid()?;
    ledger.record(
        serde_json::json!({"phase":"before_hard_kill", "pid":old_pid, "member":killed + 1}),
    )?;
    let process = &mut processes[killed];
    if let Some(voter) = &process.voter {
        voter.kill_if_running()?;
    } else {
        process.child.kill()?;
    }
    let status = process.child.wait()?;
    if let Some(voter) = &process.voter {
        voter.killed()?;
    } else {
        ensure!(!status.success(), "injected hard kill exited successfully");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt as _;
            ensure!(
                status.signal() == Some(9),
                "worker was not killed by SIGKILL: {status}"
            );
        }
    }
    ledger.record(
        serde_json::json!({"phase":"hard_kill_joined", "pid":old_pid, "status":status.to_string()}),
    )?;
    let mut current = Vec::new();
    for old in acknowledged {
        let duplicate = Request {
            cell: old.cell,
            ingress: old.ingress,
            command: &old.command,
            retry_until: &old.retry_until,
            delta: 11,
        };
        matching_receipt(
            &duplicate
                .send(client, addresses, Some(killed), ledger)
                .await?,
            &old.receipt,
        )?;
        let command = format!("{}:2", hex::encode([90; 16]));
        let retry = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            + 60_000)
            .to_string();
        let next = Request {
            cell: old.cell,
            ingress: old.ingress,
            command: &command,
            retry_until: &retry,
            delta: 7,
        }
        .send(client, addresses, Some(killed), ledger)
        .await?;
        ensure!(
            next["value"] == 18
                && next["group"] == old.receipt["group"]
                && next["incarnation"] == old.receipt["incarnation"],
            "post-kill command lost history: {next}"
        );
        // Reconcile the old receipt after a newer command as well: a duplicate
        // cannot execute again or accidentally return the current value.
        matching_receipt(
            &duplicate
                .send(client, addresses, Some(killed), ledger)
                .await?,
            &old.receipt,
        )?;
        current.push((old, next));
    }
    let disk = docker::disk(root, killed);
    processes[killed] = Process::spawn(
        root,
        &format!("cluster-resume-{killed}"),
        &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
        false,
    )?;
    addresses[killed] = processes[killed].managed_addresses().await?;
    let new_pid = processes[killed].pid()?;
    ensure!(new_pid != old_pid, "resume reused the killed PID");
    ledger.record(serde_json::json!({"phase":"restarted", "old_pid":old_pid, "new_pid":new_pid, "member":killed + 1}))?;
    for (old, receipt) in current {
        local_read(
            client,
            (old.ingress, &addresses[killed]),
            killed,
            old.cell,
            &receipt,
            18,
            ledger,
        )
        .await?;
        let command = format!("{}:3", hex::encode([90; 16]));
        let retry = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis()
            + 60_000)
            .to_string();
        let next = Request {
            cell: old.cell,
            ingress: old.ingress,
            command: &command,
            retry_until: &retry,
            delta: 5,
        }
        .send(client, addresses, None, ledger)
        .await?;
        ensure!(
            next["value"] == 23
                && next["group"] == receipt["group"]
                && next["incarnation"] == receipt["incarnation"],
            "post-resume write lost history: {next}"
        );
        let duplicate = Request {
            cell: old.cell,
            ingress: old.ingress,
            command: &old.command,
            retry_until: &old.retry_until,
            delta: 11,
        };
        matching_receipt(
            &duplicate.send(client, addresses, None, ledger).await?,
            &old.receipt,
        )?;
        for ingress in Ingress::ALL {
            local_read(
                client,
                (ingress, &addresses[killed]),
                killed,
                old.cell,
                &next,
                23,
                ledger,
            )
            .await?;
        }
        ledger.record(serde_json::json!({
            "phase":"post_resume_write_verified", "cell":old.cell,
            "receipt":next, "ingress":old.ingress.label(),
        }))?;
    }
    ledger.record(serde_json::json!({
        "phase":"retained_disk_hard_kill_passed", "physical_groups":1,
        "logical_cells":acknowledged.len(), "qualified_10gb":false,
    }))?;
    Ok(())
}

async fn local_read(
    client: &reqwest::Client,
    (ingress, address): (Ingress, &ManagedIngress),
    member_index: usize,
    cell: &str,
    receipt: &Value,
    expected_value: i64,
    ledger: &mut Ledger,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let public = ingress.endpoint(address);
    loop {
        let response = client
            .get(format!("{public}/counters/{cell}"))
            .bearer_auth(SECRET)
            .header("x-read-consistency", "at-least")
            .header(
                "x-min-version",
                receipt["version_token"]
                    .as_str()
                    .context("version absent")?,
            )
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if status.is_success() {
            let value: Value = serde_json::from_str(&body)?;
            ensure!(
                value["value"] == expected_value
                    && value["served_by"] == member_index as u64 + 1
                    && value["group"] == receipt["group"]
                    && value["group_incarnation"] == receipt["group_incarnation"]
                    && value["incarnation"] == receipt["incarnation"],
                "restarted voter served the wrong state: {value}"
            );
            ledger.record(serde_json::json!({"phase":"resumed_local_read_verified", "cell":cell, "receipt":value,
                "ingress":ingress.label()}))?;
            return Ok(());
        }
        ensure!(
            status == 503 && Instant::now() < deadline,
            "restarted voter failed {status}: {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn verify_linearizable_before_loss(
    client: &reqwest::Client,
    address: &ManagedIngress,
    ledger: &mut Ledger,
) -> Result<()> {
    for ingress in Ingress::ALL {
        let public = ingress.endpoint(address);
        let reply = client
            .get(format!("{public}/counters/one"))
            .bearer_auth(SECRET)
            .header("x-read-consistency", "linearizable")
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        ensure!(reply["value"] == 23, "wrong pre-loss read: {reply}");
        ledger.record(serde_json::json!({
            "phase":"pre_loss_linearizable", "reply":reply, "ingress":ingress.label(),
        }))?;
    }
    Ok(())
}

pub(super) async fn verify_quorum_loss(
    client: &reqwest::Client,
    address: &ManagedIngress,
    survivor: usize,
    ledger: &mut Ledger,
) -> Result<()> {
    let client_id = ClientId::new([90; 16])?;
    let id = CommandId::new(client_id, 4)?;
    let command = format!("{}:4", hex::encode([90; 16]));
    let retry = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        + 60_000)
        .to_string();
    let ambiguity = format!("Ambiguous {{ command: {id:?} }}; cause=");
    for ingress in Ingress::ALL {
        let public = ingress.endpoint(address);
        let url = format!("{public}/counters/one");
        let response = client
            .get(&url)
            .bearer_auth(SECRET)
            .header("x-read-consistency", "linearizable")
            .send()
            .await?;
        let status = response.status();
        let cause = response.text().await?;
        ledger.record(serde_json::json!({
            "phase":"no_quorum_linearizable", "member":survivor + 1,
            "status":status.as_u16(), "cause":cause, "ingress":ingress.label(),
        }))?;
        ensure!(
            status == 503 && quorum_refusal(&cause),
            "linearizable read without quorum returned {status}: {cause}"
        );

        ledger.record(serde_json::json!({
            "phase":"no_quorum_write_send", "member":survivor + 1,
            "cell":"one", "command":command, "retry_until":retry, "delta":9,
            "ingress":ingress.label(),
        }))?;
        let response = client
            .post(format!("{url}?delta=9"))
            .bearer_auth(SECRET)
            .header("x-command-id", &command)
            .header("x-retry-until-ms", &retry)
            .header("x-write-durability", "quorum")
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        let cause = body.strip_prefix(&ambiguity).unwrap_or(&body);
        ledger.record(serde_json::json!({
            "phase":"no_quorum_write", "member":survivor + 1,
            "command":command, "status":status.as_u16(), "cause":cause, "body":body,
            "ingress":ingress.label(),
        }))?;
        ensure!(
            status == 503 && quorum_refusal(cause),
            "quorum write without a majority returned {status}: {body}"
        );

        // The same surviving voter still offers an explicit historical read.
        let reply = client
            .get(&url)
            .bearer_auth(SECRET)
            .header("x-read-consistency", "stale")
            .header("x-read-policy", "disconnected-historical")
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        ensure!(
            reply["value"] == 23 && reply["served_by"] == survivor + 1,
            "surviving voter returned the wrong historical image: {reply}"
        );
        ledger.record(serde_json::json!({
            "phase":"no_quorum_historical", "member":survivor + 1,
            "reply":reply, "ingress":ingress.label(),
        }))?;
    }
    Ok(())
}

fn quorum_refusal(cause: &str) -> bool {
    matches!(
        cause,
        "Unavailable" | "NotLeader { leader: None }" | "VersionUnavailable"
    )
}
