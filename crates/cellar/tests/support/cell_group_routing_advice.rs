// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated bad advice remains guidance and cannot admit a producer.
use super::*;
use serde_json::json;
use sha2::Digest as _;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

#[path = "cell_group_routing_advice_wire.rs"]
mod wire;

#[derive(Clone, Copy, Debug)]
pub(super) enum Fault {
    Control,
    ForeignCluster,
    ForeignNamespace,
    ForeignGroup,
    ForeignIncarnation,
    NonMember,
    Configuration,
    LowerTerm,
    ConflictingTerm,
    WrongProcess,
    WrongExporter,
}

const FAULTS: [Fault; 10] = [
    Fault::ForeignCluster,
    Fault::ForeignNamespace,
    Fault::ForeignGroup,
    Fault::ForeignIncarnation,
    Fault::NonMember,
    Fault::Configuration,
    Fault::LowerTerm,
    Fault::ConflictingTerm,
    Fault::WrongProcess,
    Fault::WrongExporter,
];

#[derive(Clone, Copy, Debug)]
enum Caller {
    Sdk,
    Native,
    Axum,
}

#[derive(Clone)]
struct Advice {
    history: GroupHistory,
    revision: u64,
    configuration: [u8; 32],
    term: u64,
    issuer: u64,
    leader: u64,
}

impl Advice {
    fn encode(&self, fault: Fault, seed: bool) -> Vec<u8> {
        let mut fields = [
            self.history.cluster.bytes(),
            self.history.namespace.bytes(),
            self.history.group.bytes(),
            self.history.incarnation.bytes(),
        ];
        match fault {
            Fault::ForeignCluster => fields[0] = [251; 16],
            Fault::ForeignNamespace => fields[1] = [251; 16],
            Fault::ForeignGroup => fields[2] = [251; 16],
            Fault::ForeignIncarnation => fields[3] = [251; 16],
            _ => (),
        }
        let mut bytes = vec![1];
        for field in fields {
            bytes.extend_from_slice(&field);
        }
        bytes.extend_from_slice(&self.revision.to_be_bytes());
        bytes.extend_from_slice(&if matches!(fault, Fault::Configuration) {
            [251; 32]
        } else {
            self.configuration
        });
        let term = match fault {
            Fault::LowerTerm => self.term - 1,
            Fault::ConflictingTerm => self.term,
            _ if seed => self.term,
            _ => self.term + 1,
        };
        bytes.extend_from_slice(&term.to_be_bytes());
        let leader = if seed {
            self.issuer
        } else if matches!(fault, Fault::NonMember) {
            99
        } else {
            self.leader
        };
        bytes.extend_from_slice(&leader.to_be_bytes());
        bytes
    }
}

struct State {
    advice: Advice,
    receipt: CommandReceipt,
    fault: Fault,
    commands: Vec<serde_json::Value>,
}

async fn serve_connection(
    tcp: tokio::net::TcpStream,
    acceptor: tokio_rustls::TlsAcceptor,
    state: Arc<Mutex<State>>,
) -> Result<()> {
    let stream = acceptor.accept(tcp).await?;
    let mut connection = wire::Wire::new(stream)?;
    while let Some((request, input, binding)) = connection.receive().await? {
        let (body, fault) = {
            let mut state = state
                .lock()
                .map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
            match request.route.purpose.as_str() {
                "Query" => {
                    ensure!(input.starts_with(b"CLGCQQ01"));
                    let mut body = b"CLGCQS01".to_vec();
                    body.push(0);
                    body.extend(state.advice.encode(Fault::Control, true));
                    let token = state.receipt.version_token.as_str().as_bytes();
                    body.extend_from_slice(&u16::try_from(token.len())?.to_be_bytes());
                    body.extend_from_slice(token);
                    body.extend_from_slice(
                        &u32::try_from(state.receipt.result.len())?.to_be_bytes(),
                    );
                    body.extend_from_slice(&state.receipt.result);
                    (body, Fault::Control)
                }
                "Command" => {
                    ensure!(input.starts_with(b"CLGCCQ01") && input.len() >= 32);
                    let fault = state.fault;
                    let mut body = b"CLGCCS01".to_vec();
                    body.push(1); // The responder has not entered a producer.
                    body.extend(state.advice.encode(fault, false));
                    body.push(12);
                    body.extend_from_slice(&state.advice.leader.to_be_bytes());
                    ensure!(state.commands.len() < 1024, "finite command attempt bank");
                    state.commands.push(json!({
                        "fault": format!("{fault:?}"), "route": request.route,
                        "client": hex::encode(&input[8..24]),
                        "sequence": u64::from_be_bytes(input[24..32].try_into()?),
                        "input_sha256": hex::encode(<[u8; 32]>::from(sha2::Sha256::digest(&input))),
                        "request": hex::encode(&input),
                        "response": hex::encode(&body),
                    }));
                    (body, fault)
                }
                "CommandProbe" => {
                    ensure!(input.starts_with(b"CLGCPQ01"));
                    (b"CLGCPS01\0\0".to_vec(), Fault::Control)
                }
                purpose => anyhow::bail!("unexpected owned wire request {purpose}"),
            }
        };
        connection.reply(request, binding, body, fault).await?;
    }
    Ok::<_, anyhow::Error>(())
}

struct Server {
    state: Arc<Mutex<State>>,
    stop: Option<oneshot::Sender<()>>,
    joined: Option<JoinHandle<Result<()>>>,
}

impl Server {
    async fn start(
        address: std::net::SocketAddr,
        advice: Advice,
        receipt: CommandReceipt,
    ) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let acceptor = wire::acceptor()?;
        let state = Arc::new(Mutex::new(State {
            advice,
            receipt,
            fault: Fault::Control,
            commands: vec![],
        }));
        let observed = state.clone();
        let (stop, mut stopping) = oneshot::channel();
        let joined = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                cellar::asyncrt::select! {
                    _ = &mut stopping => break,
                    result = connections.join_next(), if !connections.is_empty() => { result.context("wire connection")???; }
                    accepted = listener.accept() => {
                        let (tcp, _) = accepted?;
                        let acceptor = acceptor.clone();
                        let state = observed.clone();
                        ensure!(connections.len() < 8, "bounded owned wire connections");
                        connections.spawn(serve_connection(tcp, acceptor, state));
                    }
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                match result {
                    Ok(result) => result?,
                    Err(error) if error.is_cancelled() => (),
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(())
        });
        Ok(Self {
            state,
            stop: Some(stop),
            joined: Some(joined),
        })
    }

    async fn close(mut self) -> Result<()> {
        self.stop
            .take()
            .context("wire stop owner")?
            .send(())
            .map_err(|_| anyhow::anyhow!("wire server already stopped"))?;
        self.joined.take().context("wire join owner")?.await?
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

fn options(sequence: u64) -> Result<CommandOptions> {
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?;
    Ok(CommandOptions {
        durability: Durability::S3,
        timeout: TIMEOUT,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([249; 16])?, sequence)?,
            now + 60_000,
        )
    })
}

async fn call(
    cluster: &nodes::Cluster,
    caller: Caller,
    cell: &RaftCellHandle,
    command: &CommandOptions,
    expected: Option<i64>,
) -> Result<serde_json::Value> {
    if matches!(caller, Caller::Sdk) {
        let result = cell
            .command(
                &cell.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                command.clone(),
            )
            .await;
        if let Some(expected) = expected {
            let receipt = result?;
            ensure!(
                receipt.id == command.id
                    && receipt.durability == Durability::S3
                    && receipt.result == expected.to_be_bytes()
            );
            return Ok(
                json!({"version": format!("{:?}", receipt.version), "witness": format!("{:?}", receipt.witness)}),
            );
        }
        let failure = result.err().context("bad advice request must refuse")?;
        ensure!(failure.command == command.id);
        ensure!(failure.cause.refusal().is_none_or(|reason| matches!(
            reason,
            CommandFailure::Unavailable | CommandFailure::NotLeader { .. }
        )));
        return Ok(json!({"ambiguous": failure.ambiguous, "failure": format!("{failure:?}")}));
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let response = client
        .post(format!(
            "{}?delta=1",
            cluster
                .web()?
                .url(INGRESS, matches!(caller, Caller::Axum), "one")
        ))
        .bearer_auth(http::secret())
        .header(
            "x-command-id",
            format!(
                "{}:{}",
                hex::encode(command.id.client.bytes()),
                command.id.sequence()
            ),
        )
        .header("x-retry-until-ms", command.retry_until_ms)
        .header("x-write-durability", "s3")
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    if let Some(expected) = expected {
        ensure!(
            status == 200,
            "wire advice reconciliation HTTP {status}: {body}"
        );
        let result: serde_json::Value = serde_json::from_str(&body)?;
        http::value(&result, "one", expected)?;
        ensure!(result["durability"] == "s3");
        return Ok(result);
    }
    ensure!(status == 503, "bad advice HTTP {status}: {body}");
    // Preserve the public counter's typed cause and original ambiguous ID.
    let ambiguous = format!(
        "{:?}; cause=",
        CommandFailure::Ambiguous {
            command: command.id
        }
    );
    let cause = body.strip_prefix(&ambiguous).unwrap_or(&body);
    ensure!(
        cause == "Unavailable" || cause.starts_with("NotLeader { leader: "),
        "bad advice failure: {body}"
    );
    Ok(json!({"ambiguous": body.starts_with(&ambiguous), "failure": body}))
}

fn counter(host: &RaftCells, name: &str) -> Result<u64> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    sample
        .counters
        .into_iter()
        .find(|c| c.name == name)
        .map(|c| c.value)
        .context("wire advice counter")
}

async fn seed(cell: &RaftCellHandle, server: &Server, ordinal: usize) -> Result<()> {
    let (member, expected) = {
        let mut state = server
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
        state.advice.term += 4;
        (MemberId::new(state.advice.issuer)?, state.receipt.clone())
    };
    let result = cell
        .query(
            ProposalId::new((12000_u128 + ordinal as u128).to_be_bytes())?,
            &cell.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                timeout: TIMEOUT,
                max_attempts: 1,
                routing: QueryRouting::Replicas {
                    members: vec![member],
                    attempt_timeout: TIMEOUT,
                    leader_fallback: false,
                },
            },
        )
        .await?;
    ensure!(
        result.served_by == member
            && result.result == expected.result
            && result.version == expected.version
            && result.version_token == expected.version_token
    );
    Ok(())
}

fn peer_record(peer: &RaftPeerNode) -> serde_json::Value {
    json!({
        "node": hex::encode(peer.node.bytes()), "disk": hex::encode(peer.disk.bytes()),
        "process": hex::encode(peer.process.bytes()), "label": peer.label,
        "query_address": peer.query_address.to_string(), "server_name": peer.server_name,
    })
}

async fn exercise(
    cluster: &mut nodes::Cluster,
    callbacks: &AtomicUsize,
    rooted: bool,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    let group = cluster.managed.history;
    let issuer = cluster.leader().await?;
    let cell = cluster.managed.hosts[INGRESS].cell(counter::history(
        group.cluster,
        group.namespace,
        "one",
    )?)?;
    cluster.managed.hosts[issuer]
        .cell(cell.history().clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([249; 16])?,
                durability: Durability::Quorum,
            },
            TIMEOUT,
        )
        .await?;
    let original = cell
        .command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            options(1)?,
        )
        .await?;
    ensure!(callbacks.load(Ordering::Acquire) == 1);
    let catalog = &cluster.managed.fixtures[0].catalog;
    let cut = catalog.read(TIMEOUT).await?;
    let authorization = catalog.authorize_group(&cut, &group, TIMEOUT).await?;
    let membership = cluster.managed.hosts[INGRESS]
        .observe_peer_fleet_membership(
            cluster.managed.peers[issuer].node,
            authorization,
            cut.revision(),
            TIMEOUT,
        )
        .await?;
    let prior = cluster.managed.hosts[issuer]
        .group(group)?
        .replication_status(TIMEOUT)
        .await?;
    let leader = (issuer + 1) % INGRESS;
    cluster.managed.hosts[issuer]
        .group(group)?
        .transfer_leadership(
            LeadershipTransfer {
                target: MemberId::new(leader as u64 + 1)?,
                expected_term: prior.term,
                expected_revision: prior.membership_revision,
            },
            TIMEOUT,
        )
        .await?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if cluster.leader().await? == leader {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await??;
    let stable = cluster.managed.hosts[leader]
        .group(group)?
        .observe(TIMEOUT)
        .await?;
    let address = cluster.managed.peers[issuer].query_address;
    cluster.stop(issuer).await?;
    let server = Server::start(
        address,
        Advice {
            history: group,
            revision: membership.membership().membership_revision().get(),
            configuration: membership.membership().configuration_digest(),
            term: stable.status.term.get() + 100,
            issuer: issuer as u64 + 1,
            leader: leader as u64 + 1,
        },
        original,
    )
    .await?;
    let work = async {
        let ingress = &cluster.managed.hosts[INGRESS];
        let origin = cluster.managed.peers[INGRESS].clone();
        let mut ordinal = 0;
        for caller in [Caller::Sdk, Caller::Native, Caller::Axum] {
            for fault in FAULTS {
                seed(&cell, &server, ordinal).await?;
                let revision = 2 + u64::try_from(ordinal)? * 2;
                for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
                    if index != issuer { host.revoke_command_origin(group.cluster, group.namespace, origin.node, revision)?; }
                }
                let command = options(u64::try_from(ordinal)? + 2)?;
                let before = sample(cluster)?;
                let stages = authority::sql_stages(cluster)?;
                let entered = callbacks.load(Ordering::Acquire);
                let rejected = counter(ingress, "cell_command_pre_admission_refusals")?;
                let start = {
                    let mut state = server.state.lock().map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
                    state.fault = fault;
                    state.commands.len()
                };
                let failure = call(cluster, caller, &cell, &command, None).await?;
                let after = sample(cluster)?;
                let records = {
                    let state = server.state.lock().map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
                    state.commands[start..].to_vec()
                };
                ensure!(!records.is_empty(), "fault must cross the actual command TLS lane");
                ensure!(records.iter().all(|record| record["client"] == hex::encode(command.id.client.bytes()) && record["sequence"] == command.id.sequence()));
                ensure!(after[0] > before[0] && after[4] > before[4]);
                let callbacks_refused = callbacks.load(Ordering::Acquire);
                let stages_refused = authority::sql_stages(cluster)?;
                let rejected_refused = counter(ingress, "cell_command_pre_admission_refusals")?;
                ensure!(callbacks_refused == entered && stages_refused == stages);
                let authenticated = !matches!(fault, Fault::ForeignCluster | Fault::ForeignNamespace | Fault::ForeignGroup | Fault::ForeignIncarnation | Fault::WrongProcess | Fault::WrongExporter);
                if authenticated {
                    ensure!(rejected_refused > rejected);
                    if matches!(fault, Fault::Configuration | Fault::NonMember) { ensure!(after[3] > before[3]); }
                    if matches!(fault, Fault::LowerTerm | Fault::ConflictingTerm) { ensure!(after[1] > before[1]); }
                } else {
                    ensure!(rejected_refused == rejected && failure["ambiguous"] == true);
                }
                for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
                    if index != issuer {
                        host.grant_command_origin(NamespaceCommandOrigin { cluster: group.cluster, namespace: group.namespace, revision: revision + 1, peer: origin.clone() })?;
                    }
                }
                {
                    let mut state = server.state.lock().map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
                    state.fault = Fault::Control;
                }
                let expected = i64::try_from(ordinal)? + 2;
                let recovered = call(cluster, caller, &cell, &command, Some(expected)).await?;
                let replay = cell.command(&cell.contract("Increment")?, 1_i64.to_be_bytes().to_vec(), command.clone()).await?;
                ensure!(replay.id == command.id && replay.durability == Durability::S3 && replay.result == expected.to_be_bytes());
                ensure!(callbacks.load(Ordering::Acquire) == entered + 1);
                let current = cluster.managed.hosts[leader].group(group)?.observe(TIMEOUT).await?;
                ensure!(current.status.term == stable.status.term && current.status.leader == stable.status.leader && current.membership_revision == stable.membership_revision);
                cellar::note!("WIRE_ADVICE_EVIDENCE {}", json!({
                    "schema": 1, "case": "wire_advice_preserves_admission", "extents": rooted, "ingress": format!("{caller:?}"), "fault": format!("{fault:?}"),
                    "history": format!("{group:?}"), "issuer": peer_record(&cluster.managed.peers[issuer]), "origin": peer_record(&origin),
                    "command": {"client": hex::encode(command.id.client.bytes()), "sequence": command.id.sequence()},
                    "request_timeout_ms": TIMEOUT.as_millis(), "retry_until_ms": command.retry_until_ms,
                    "failure": failure, "wire_requests": records,
                    "routing_before": before, "routing_after_refusal": after,
                    "callbacks_before": entered, "callbacks_after_refusal": callbacks_refused, "callbacks_after_replay": callbacks.load(Ordering::Acquire),
                    "sql_stages_before": stages, "sql_stages_after_refusal": stages_refused, "sql_stages_after_replay": authority::sql_stages(cluster)?,
                    "proven_rejections_before": rejected, "proven_rejections_after_refusal": rejected_refused,
                    "required_durability": "S3", "achieved_durability": format!("{:?}", replay.durability),
                    "result_be": hex::encode(&replay.result), "membership_revision": current.membership_revision.get(),
                    "configuration_digest": hex::encode(membership.membership().configuration_digest()), "term_unchanged": current.status.term == stable.status.term,
                    "receipt": {"version": format!("{:?}", replay.version), "witness": format!("{:?}", replay.witness), "token": replay.version_token.as_str()},
                    "recovered": recovered,
                }));
                server.state.lock().map_err(|_| anyhow::anyhow!("wire state poisoned"))?.receipt = replay;
                ordinal += 1;
            }
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    let joined = server.close().await;
    work?;
    joined
}

pub(super) async fn run(make: Option<RoutingStoreFactory>) -> Result<()> {
    for rooted in [false, true] {
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = callbacks.clone();
        let store = Arc::new(match make {
            Some(make) => fault::Store::around(make("wire-advice", rooted)?),
            None => fault::Store::default(),
        });
        let mut cluster = Box::pin(nodes::Cluster::start_with_callback(
            rooted,
            store,
            Arc::new(move || {
                observed.fetch_add(1, Ordering::AcqRel);
            }),
        ))
        .await?;
        let result = Box::pin(exercise(&mut cluster, &callbacks, rooted)).await;
        let joined = cluster.close().await;
        if result.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.managed.roots) {
                eprintln!("retained wire advice disk: {}", root.keep().display());
            }
        }
        cluster.managed.fixtures[0].domain.shutdown().await;
        result?;
        joined?;
    }
    Ok(())
}
