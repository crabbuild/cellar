// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent voter processes; all mutations enter through ordinary SDK bindings.
use super::*;
use serde_json::json;

struct Address {
    public: String,
    internal: String,
}

async fn startup_lease(
    leases: &cellar::ownership_store::BucketOwnership,
    root: &Path,
    previous: &str,
    process: &Process,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut first = None;
    loop {
        ensure!(!process.logs()?.contains("cellar listening on "), "held startup opened ingress");
        if root.join("primitive-startup-held").is_file() {
            if let Some(lease) = leases.read_node_lease("raft-cli-cluster-0").await? {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis();
                if lease.generation != previous && u128::from(lease.expires_ms) > now {
                    match &first {
                        None => first = Some((lease.generation.clone(), lease.expires_ms)),
                        Some((generation, expires)) if generation == &lease.generation && lease.expires_ms > *expires => {
                            ledger.record(json!({"phase":"primitive_startup_lease_verified",
                                "old_generation":previous,"generation":generation,
                                "first_expires_ms":expires,"renewed_expires_ms":lease.expires_ms,
                                "public_admission":false}))?;
                            persist(root, "primitive-startup-release", b"1")?;
                            return Ok(());
                        }
                        Some((generation, _)) => ensure!(generation == &lease.generation, "held startup changed lease generation"),
                    }
                }
            }
        }
        ensure!(Instant::now() < deadline, "node lease did not renew while Raft startup held ingress closed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn request(
    client: &reqwest::Client,
    address: &Address,
    route: &str,
    input: Option<Value>,
    ledger: &mut recovery::Ledger,
) -> Result<(u16, Value)> {
    ledger.record(json!({"phase":"primitive_send","route":route,"input":input}))?;
    let started = Instant::now();
    let url = format!("{}{route}", address.public);
    let call = match input {
        Some(input) => client.post(url).json(&input),
        None => client.get(url),
    };
    let response = call.bearer_auth(SECRET).send().await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    ledger.record(json!({"phase":"primitive_reply","route":route,"status":status,"body":body,"elapsed_ms":u64::try_from(started.elapsed().as_millis())?}))?;
    Ok((status, serde_json::from_str(&body)?))
}

async fn admitted(
    client: &reqwest::Client,
    address: &Address,
    route: &str,
    input: Option<Value>,
    expected: Value,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    let (status, body) = request(client, address, route, input, ledger).await?;
    ensure!(
        status == 200 && body == expected,
        "primitive request failed: {route}: {status} {body}"
    );
    Ok(())
}

async fn inspect(client: &reqwest::Client, address: &Address) -> Result<Value> {
    Ok(client
        .get(format!("{}/inspect", address.public))
        .bearer_auth(SECRET)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn observe(
    client: &reqwest::Client,
    addresses: &[Address],
    excluded: Option<usize>,
    minimum_cells: u64,
    ledger: &mut recovery::Ledger,
) -> Result<Vec<Value>> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut voters = Vec::new();
        for (index, address) in addresses
            .iter()
            .enumerate()
            .filter(|(index, _)| Some(*index) != excluded)
        {
            let observed = inspect(client, address).await?;
            ensure!(
                observed["hosted_groups"] == 2,
                "primitive fallback or missing shared group: {observed}"
            );
            let groups = observed["groups"]
                .as_array()
                .context("primitive observation groups")?;
            ensure!(groups.len() == 2, "primitive group observation count");
            for (namespace, group) in groups.iter().enumerate() {
                let expected = super::group(namespace)?;
                ensure!(
                    group["namespace"] == hex::encode(expected.namespace.bytes())
                        && group["group"] == hex::encode(expected.group.bytes())
                        && group["incarnation"] == hex::encode(expected.incarnation.bytes())
                        && group["member"] == index + 1,
                    "primitive history or member differs: {observed}"
                );
            }
            voters.push(observed);
        }
        let ready = (0..2).all(|namespace| {
            let groups = voters
                .iter()
                .map(|v| &v["groups"][namespace])
                .collect::<Vec<_>>();
            let leaders = groups
                .iter()
                .filter(|group| group["is_leader"] == true)
                .collect::<Vec<_>>();
            leaders.len() == 1
                && leaders[0]["ready"] == true
                && groups.iter().all(|group| {
                    group["ready"] == group["is_leader"]
                        && (group["is_leader"] == true) == (group["member"] == leaders[0]["member"])
                        && group["cells"].as_u64().is_some_and(|n| n == minimum_cells)
                        && group["leader"] == leaders[0]["member"]
                        && group["term"] == leaders[0]["term"]
                })
        });
        if ready {
            ledger.record(json!({"phase":"primitive_observed","voters":voters}))?;
            return Ok(voters);
        }
        ensure!(
            Instant::now() < deadline,
            "primitive voters did not converge: {voters:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn start(root: &Path, index: usize, mode: &str) -> Result<Process> {
    let disk = docker::disk(root, index);
    let mut process = Process::spawn(
        root,
        mode,
        &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()],
        false,
    )?;
    let marker = root.join(format!("primitives-boot-{mode}"));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        ensure!(
            process.child.try_wait()?.is_none() && Instant::now() < deadline,
            "primitive boot failed: {}",
            process.logs()?
        );
        match std::fs::read_to_string(&marker) {
            Ok(pid) if pid.parse::<u64>()? == process.pid()? => return Ok(process),
            Ok(_) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn addresses(processes: &mut [Process]) -> Result<Vec<Address>> {
    let mut addresses = Vec::new();
    for process in processes {
        let (public, internal) = process.addresses().await?;
        addresses.push(Address { public, internal });
    }
    Ok(addresses)
}

async fn stop(
    client: &reqwest::Client,
    process: &mut Process,
    address: &Address,
    index: usize,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    client
        .post(format!("{}/shutdown", address.internal))
        .send()
        .await?
        .error_for_status()?;
    let logs = process.exited(true).await?;
    let report = logs
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|report| report["event"] == "daemon_node_stopped")
        .context("primitive joined shutdown report")?;
    ensure!(
        report["raft"]["owners_closed"] == true
            && report["raft"]["startup_incomplete"] == false
            && report["raft"]["groups"] == 2
            && report["raft"]["retain_replica_disks"] == true,
        "primitive owners did not join: {report}"
    );
    ledger.record(json!({"phase":"primitive_shutdown_joined","member":index+1,"report":report}))?;
    Ok(())
}

async fn reads(
    client: &reqwest::Client,
    addresses: &[Address],
    excluded: Option<usize>,
    stage: &str,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    for (index, address) in addresses
        .iter()
        .enumerate()
        .filter(|(index, _)| Some(*index) != excluded)
    {
        for adapter in ["native", "axum"] {
            for name in ["one", "two"] {
                for kind in ["sql", "kv"] {
                    let expected = if kind == "sql" {
                        json!([[1, format!("{stage}-{name}")]])
                    } else {
                        json!(format!("{stage}-{name}"))
                    };
                    admitted(
                        client,
                        address,
                        &format!("/{adapter}/{kind}/{name}/read"),
                        None,
                        expected,
                        ledger,
                    )
                    .await?;
                    ledger.record(json!({"phase":"primitive_read_verified","stage":stage,"member":index+1,"adapter":adapter,"family":kind,"cell":name}))?;
                }
            }
        }
    }
    Ok(())
}

async fn writes(
    client: &reqwest::Client,
    address: &Address,
    stage: &str,
    ledger: &mut recovery::Ledger,
) -> Result<()> {
    for (name, adapter) in [("one", "native"), ("two", "axum")] {
        for kind in ["sql", "kv"] {
            admitted(
                client,
                address,
                &format!("/{adapter}/{kind}/{name}/write"),
                Some(json!({"id":1,"value":format!("{stage}-{name}")})),
                json!(true),
                ledger,
            )
            .await?;
            ledger.record(json!({"phase":"primitive_write_verified","stage":stage,"adapter":adapter,"family":kind,"cell":name}))?;
        }
    }
    Ok(())
}

pub(crate) async fn qualify() -> Result<()> {
    ensure!(
        std::env::var_os(RUSTFS_ENDPOINT).is_some(),
        "primitive daemon qualification requires real RustFS"
    );
    ensure!(std::env::var(ENABLED)? == "1", "invalid primitive gate");
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    cellar::note!(
        "primitive daemon retained artifacts: {}",
        root.path().display()
    );
    let mut ledger = recovery::Ledger::new(root.path(), "rustfs_catalog_archives_and_node_leases")?;
    ledger.record(json!({"phase":"primitive_scope","families":["sql","kv"],"groups":2,"cells_per_group":2,
        "minimum":std::env::var(MINIMUM)?,"image":std::env::var("CELLAR_RAFT_PROCESS_IMAGE_MODE")?,
        "hosting":"native_daemon","application_adapters":["native","axum"],"external_axum_hosting":false,
        "same_id_transport_retry":false,"retained_root":root.path()}))?;
    startup::initialize(root.path()).await?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(15))
        .build()?;
    let mut processes = Vec::new();
    for index in 0..3 {
        processes.push(start(root.path(), index, &format!("cluster-{index}")).await?);
    }
    let mut entrances = addresses(&mut processes).await?;
    verify_deployment_leases(root.path(), 3).await?;
    observe(&client, &entrances, None, 0, &mut ledger).await?;
    for (name, adapter) in [("one", "native"), ("two", "axum")] {
        admitted(
            &client,
            &entrances[0],
            &format!("/{adapter}/sql/{name}/setup"),
            Some(json!({})),
            json!(true),
            &mut ledger,
        )
        .await?;
    }
    writes(&client, &entrances[0], "initial", &mut ledger).await?;
    for (name, adapter) in [("one", "native"), ("two", "axum")] {
        admitted(
            &client,
            &entrances[1],
            &format!("/{adapter}/sql/{name}/rollback"),
            Some(json!({})),
            json!(true),
            &mut ledger,
        )
        .await?;
    }
    reads(&client, &entrances, None, "initial", &mut ledger).await?;
    observe(&client, &entrances, None, 2, &mut ledger).await?;
    let bucket = open_deployment_store(root.path())?;
    let leases = cellar::ownership_store::BucketOwnership::new(bucket.clone(), bucket, String::new(), String::new());
    let previous = leases.read_node_lease("raft-cli-cluster-0").await?.context("initial node lease")?.generation;
    let old_pids = processes
        .iter()
        .map(Process::pid)
        .collect::<Result<Vec<_>>>()?;
    for index in 0..3 {
        stop(
            &client,
            &mut processes[index],
            &entrances[index],
            index,
            &mut ledger,
        )
        .await?;
    }
    processes.clear();
    for (index, old_pid) in old_pids.iter().enumerate() {
        let process = start(root.path(), index, &format!("cluster-resume-{index}")).await?;
        ensure!(
            process.pid()? != *old_pid,
            "retained voter did not change process"
        );
        processes.push(process);
    }
    startup_lease(&leases, root.path(), &previous, &processes[0], &mut ledger).await?;
    entrances = addresses(&mut processes).await?;
    reads(&client, &entrances, None, "initial", &mut ledger).await?;
    let voters = observe(&client, &entrances, None, 2, &mut ledger).await?;
    ledger.record(json!({"phase":"primitive_retained_restart_verified","old_pids":old_pids}))?;
    let killed = voters
        .iter()
        .position(|v| v["groups"][0]["is_leader"] == true)
        .context("SQL leader absent")?;
    let old_pid = processes[killed].pid()?;
    let old_term = voters[killed]["groups"][0]["term"]
        .as_u64()
        .context("leader term")?;
    ledger.record(json!({"phase":"primitive_before_leader_kill","member":killed+1,"pid":old_pid,"term":old_term,"voters":voters}))?;
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
        ensure!(!status.success(), "SIGKILL succeeded");
    }
    let survivors = observe(&client, &entrances, Some(killed), 2, &mut ledger).await?;
    ensure!(
        survivors.iter().all(|v| v["groups"][0]["term"]
            .as_u64()
            .is_some_and(|term| term > old_term)),
        "SQL term did not advance after leader kill"
    );
    let active = (killed + 1) % 3;
    writes(&client, &entrances[active], "after-kill", &mut ledger).await?;
    reads(&client, &entrances, Some(killed), "after-kill", &mut ledger).await?;
    ledger.record(json!({"phase":"primitive_leader_loss_verified","killed_member":killed+1,"old_pid":old_pid,"survivors":survivors}))?;
    let mut replacement = start(root.path(), killed, &format!("cluster-rejoin-{killed}")).await?;
    let (public, internal) = replacement.addresses().await?;
    ensure!(
        replacement.pid()? != old_pid,
        "killed voter did not change process"
    );
    processes[killed] = replacement;
    entrances[killed] = Address { public, internal };
    observe(&client, &entrances, None, 2, &mut ledger).await?;
    reads(&client, &entrances, None, "after-kill", &mut ledger).await?;
    let block = root.path().join("block-archive-writes");
    File::options().write(true).create_new(true).open(&block)?;
    ledger.record(json!({"phase":"primitive_archive_blocked"}))?;
    let outage=async {
        for adapter in ["native","axum"] {
            for kind in ["sql","kv"] {
                let route=format!("/{adapter}/{kind}/one/write");
                let (status,body)=request(&client,&entrances[active],&route,Some(json!({"id":1,"value":"outage-one"})),&mut ledger).await?;
                ensure!(status==if minimum()?==Durability::Quorum {200}else{503},"wrong archive-outage acknowledgement: {route}: {status} {body}");
                ledger.record(json!({"phase":"primitive_archive_boundary","adapter":adapter,"family":kind,"status":status}))?;
            }
        }
        let deadline=Instant::now()+TIMEOUT;
        loop {
            let denied=(0..3).filter(|voter|root.path().join(format!("archive-write-denied-{voter}")).is_file()).map(|voter|voter+1).collect::<Vec<_>>();
            if !denied.is_empty() {
                ledger.record(json!({"phase":"primitive_archive_denied","members":denied}))?;
                break;
            }
            ensure!(Instant::now()<deadline,"archive outage did not intercept a real write");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<_,anyhow::Error>(())
    }.await;
    std::fs::remove_file(block)?;
    outage?;
    ledger.record(json!({"phase":"primitive_archive_restored"}))?;
    // These are fresh stateless requests with semantically idempotent values.
    // They do not prove same-command replay of an uncertain public response.
    writes(&client, &entrances[active], "restored", &mut ledger).await?;
    reads(&client, &entrances, None, "restored", &mut ledger).await?;
    for index in 0..2 {
        stop(
            &client,
            &mut processes[index],
            &entrances[index],
            index,
            &mut ledger,
        )
        .await?;
    }
    for adapter in ["native", "axum"] {
        for kind in ["sql", "kv"] {
            for operation in ["read", "write"] {
                let input = (operation == "write").then(|| json!({"id":1,"value":"must-not-ack"}));
                let (status, body) = request(
                    &client,
                    &entrances[2],
                    &format!("/{adapter}/{kind}/one/{operation}"),
                    input,
                    &mut ledger,
                )
                .await?;
                ensure!(
                    status == 503,
                    "minority served protected primitive {operation}: {status} {body}"
                );
                ledger.record(json!({"phase":"primitive_quorum_refused","adapter":adapter,"family":kind,"operation":operation,"status":status}))?;
            }
        }
    }
    stop(&client, &mut processes[2], &entrances[2], 2, &mut ledger).await?;
    ledger
        .record(json!({"phase":"primitive_daemon_passed","physical_groups":2,"logical_cells":4}))?;
    cellar::note!("public primitive daemon qualification passed");
    Ok(())
}
