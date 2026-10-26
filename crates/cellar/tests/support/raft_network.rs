// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real TLS/engine/RawNode fixtures. Application images below are opaque test
//! bytes, not SQLite/LTX apply qualification. Faults affect emitted peer traffic.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, ensure, Context, Result};
use cellar::application::consensus::verification::*;
use cellar::peer_auth::PeerAuth;
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_ltx::DirectFileSystem;
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use raft::prelude::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

pub const IDS: [u8; 2] = [10, 11];

pub fn scope(id: u8) -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: format!("Counter:{id}"),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

pub fn identity(scope: &GroupScope, member: usize) -> ReplicaIdentity {
    ReplicaIdentity {
        scope: scope.clone(),
        member: MemberId::new(member as u64).unwrap(),
        node: NodeId::new([member as u8; 16]).unwrap(),
        disk: DiskLineage::new([100 + member as u8; 16]).unwrap(),
        process: ProcessGeneration::new([9; 16]).unwrap(),
    }
}

pub fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}

pub fn server_tls() -> Result<tokio_rustls::TlsAcceptor> {
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_single_cert(
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

pub fn client_tls() -> Result<tokio_rustls::TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(
        include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into(),
    )?;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

pub fn route(scope: &GroupScope, addresses: &[(SocketAddr, SocketAddr, SocketAddr)]) -> GroupRoute {
    GroupRoute {
        scope: scope.clone(),
        schema_digest: [4; 32],
        revision: 1,
        peers: addresses
            .iter()
            .enumerate()
            .map(|(i, (control, data, query))| RaftPeer {
                replica: identity(scope, i + 1),
                label: format!("node-{}", i + 1),
                server_name: "localhost".into(),
                control_address: *control,
                data_address: *data,
                query_address: *query,
            })
            .collect(),
    }
}

pub struct Host {
    pub domain: ExecutionDomain,
    pub log: ReplicaLog,
    pub runtime: RaftRuntime,
    pub transport: RaftTransport,
    pub events: Option<RuntimeEvents>,
    pub path: PathBuf,
}

impl Host {
    pub async fn new(
        path: &Path,
        member: usize,
        scopes: &[GroupScope],
        options: TransportOptions,
    ) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let replica = identity(&scope(10), member);
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    path.join("log"),
                    replica.node,
                    replica.disk,
                    replica.process,
                    LogOpenMode::Create,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        for scope in scopes {
            log.create_group(GroupBootstrap {
                scope: scope.clone(),
                member: replica.member,
                birth_digest: [4; 32],
                voters: (1..=3).map(|id| MemberId::new(id).unwrap()).collect(),
                learners: Vec::new(),
            })
            .await?;
        }
        let (runtime, events) = RaftRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([member as u8; 32]),
                ..Default::default()
            },
        )
        .await?;
        let transport = RaftTransport::start(
            runtime.clone(),
            Arc::new(PeerAuth::new([17; 32], format!("node-{member}"))?),
            tls()?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            options,
        )
        .await?;
        Ok(Self {
            domain,
            log,
            runtime,
            transport,
            events: Some(events),
            path: path.into(),
        })
    }

    pub fn addresses(&self) -> (SocketAddr, SocketAddr, SocketAddr) {
        (
            self.transport.control_address(),
            self.transport.data_address(),
            self.transport.query_address(),
        )
    }

    pub async fn close(self) -> Result<()> {
        self.transport.close().await;
        self.runtime.close().await?;
        self.log.close().await?;
        self.domain.shutdown().await;
        ensure!(self.transport.usage().tasks == 0);
        ensure!(
            self.transport.usage().control_bytes == 0 && self.transport.usage().data_bytes == 0
        );
        Ok(())
    }

    fn dispatcher(&mut self, faults: Arc<Mutex<Faults>>) -> Result<()> {
        let mut events = self.events.take().context("dispatcher already started")?;
        let runtime = self.runtime.clone();
        let transport = self.transport.clone();
        let path = self.path.clone();
        self.domain.spawn(async move {
            let mut deliveries = FuturesUnordered::new();
            loop {
                cellar::asyncrt::select! {
                    result = deliveries.next(), if !deliveries.is_empty() => { let _ = result; },
                    event = events.recv(), if deliveries.len() < 256 => {
                        let Some(event) = event else { return Ok::<(), anyhow::Error>(()); };
                        match event.kind {
                            RuntimeEventKind::Message { replica, message } => {
                                let duplicate = {
                                    let mut faults = faults.lock().unwrap();
                                    if faults.blocked.contains(&message.to) { continue; }
                                    if faults.hold {
                                        ensure!(faults.held.len() < 128, "fixture hold budget");
                                        faults.held.push((replica.scope, message)); continue;
                                    }
                                    faults.duplicate
                                };
                                for _ in 0..if duplicate {2} else {1} {
                                    match transport.submit(&replica.scope, message.clone()) {
                                        Ok(delivery) => deliveries.push(delivery.accepted().boxed()),
                                        Err(_) => { let _ = runtime.report_unreachable(&replica.scope,message.to).await; }
                                    }
                                }
                            }
                            RuntimeEventKind::Apply { ticket, .. } => {
                                let image = fixture_image(&path,&ticket)?;
                                runtime.complete_apply(ApplyCompletion { ticket, image }).await?;
                            }
                            RuntimeEventKind::Failed { error, .. } => bail!("runtime failed: {error}"),
                            RuntimeEventKind::Snapshot { .. } => bail!("unexpected snapshot in retained-log fixture"),
                            _ => (),
                        }
                    }
                }
            }
        })?.detach();
        Ok(())
    }
}

fn fixture_image(path: &Path, ticket: &ApplyTicket) -> Result<CommittedImage> {
    let name = format!(
        "{}-{}.fixture",
        hex::encode(ticket.replica().scope.group.bytes()),
        ticket.position().index.get()
    );
    let bytes = format!(
        "fixture:{}:{}",
        ticket.position().term.get(),
        ticket.position().index.get()
    )
    .into_bytes();
    std::fs::write(path.join(&name), &bytes)?;
    std::fs::File::open(path.join(&name))?.sync_all()?;
    std::fs::File::open(path)?.sync_all()?;
    Ok(CommittedImage {
        name,
        commitment: <[u8; 32]>::from(Sha256::digest(&bytes)).into(),
        length: bytes.len() as u64,
        ltx_epoch: 1,
        txid: ticket.position().index.get(),
        page_size: 512,
        checksum: 0,
    })
}

#[derive(Default)]
struct Faults {
    blocked: BTreeSet<u64>,
    hold: bool,
    held: Vec<(GroupScope, Message)>,
    duplicate: bool,
}

pub async fn process_main() -> Result<()> {
    let config: Value = serde_json::from_str(&std::env::var("CELLAR_RAFT_NETWORK_FIXTURE")?)?;
    let member = config["member"].as_u64().context("member")? as usize;
    let path = PathBuf::from(config["path"].as_str().context("path")?);
    let mut host = Host::new(
        &path,
        member,
        &IDS.map(scope),
        TransportOptions {
            io_timeout_ms: 1000,
            ..Default::default()
        },
    )
    .await?;
    let faults = Arc::new(Mutex::new(Faults::default()));
    host.dispatcher(faults.clone())?;
    reply(
        &json!({"ready":[host.addresses().0.to_string(),host.addresses().1.to_string(),host.addresses().2.to_string()]}),
    )?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let command: Value = serde_json::from_str(&line)?;
        let id = command["group"].as_u64().unwrap_or(10) as u8;
        let response: Result<Value> = async {
            Ok(match command["op"].as_str().context("op")? {
                "install" => {
                    let addresses: Vec<(SocketAddr,SocketAddr,SocketAddr)> = command["addresses"].as_array().context("addresses")?.iter().map(|p| Ok((p[0].as_str().context("control")?.parse()?,p[1].as_str().context("data")?.parse()?,p[2].as_str().context("query")?.parse()?))).collect::<Result<_>>()?;
                    for id in IDS { host.transport.install_group(route(&scope(id), &addresses))?; }
                    json!({"ok":true})
                }
                "campaign" => { host.runtime.campaign(&scope(id)).await?; json!({"ok":true}) }
                "tick" => { host.runtime.tick().await?; json!({"ok":true}) }
                "status" => {
                    let s = host.runtime.status(&scope(id)).await?;
                    let u = host.transport.usage();
                    json!({"leader":s.is_leader,"ready":s.protocol_ready,"term":s.term.get(),"committed":s.committed.get(),"applied":s.applied.index.get(),"failed":u.failed,"groups":u.groups,"peers":u.peers})
                }
                "propose" => {
                    let p = host.runtime.propose(&scope(id),vec![],command["data"].as_str().unwrap_or("command").as_bytes().to_vec()).await?;
                    json!({"index":p.position.index.get(),"term":p.position.term.get()})
                }
                "fault" => {
                    let mut f = faults.lock().unwrap();
                    f.blocked = command["blocked"].as_array().map(|v| v.iter().filter_map(Value::as_u64).collect()).unwrap_or_default();
                    f.hold = command["hold"].as_bool().unwrap_or(false);
                    f.duplicate = command["duplicate"].as_bool().unwrap_or(false);
                    json!({"ok":true})
                }
                "release" => {
                    let mut held = {
                        let mut f = faults.lock().unwrap(); f.hold = false; std::mem::take(&mut f.held)
                    };
                    let count = held.len();
                    let responses = held.iter().filter(|(_,m)| matches!(m.get_msg_type(), raft::prelude::MessageType::MsgAppendResponse | raft::prelude::MessageType::MsgHeartbeatResponse)).count();
                    held.reverse();
                    for (scope,message) in held {
                        let _ = host.transport.submit(&scope,message)?.accepted().await;
                    }
                    json!({"released":count,"responses":responses})
                }
                "shutdown" => json!({"ok":true}),
                op => bail!("unknown fixture command {op}"),
            })
        }.await;
        match response {
            Ok(value) => reply(&value)?,
            Err(e) => reply(&json!({"error":format!("{e:#}")}))?,
        }
        if command["op"] == "shutdown" {
            break;
        }
    }
    host.close().await
}

fn reply(value: &Value) -> Result<()> {
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text)
        .line(format_args!("RAFT:{value}"))?;
    Ok(())
}

pub struct Process {
    pub child: Child,
    stdin: ChildStdin,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    pub addresses: (SocketAddr, SocketAddr, SocketAddr),
}

impl Process {
    pub async fn start(path: &Path, member: usize) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "raft_transport_process",
                "--nocapture",
            ])
            .env(
                "CELLAR_RAFT_NETWORK_FIXTURE",
                json!({"path":path,"member":member}).to_string(),
            )
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let stdin = child.stdin.take().context("stdin")?;
        let stdout = BufReader::new(child.stdout.take().context("stdout")?).lines();
        let mut process = Self {
            child,
            stdin,
            stdout,
            addresses: (
                "127.0.0.1:1".parse()?,
                "127.0.0.1:2".parse()?,
                "127.0.0.1:3".parse()?,
            ),
        };
        let ready = process.read().await?;
        process.addresses = (
            ready["ready"][0]
                .as_str()
                .context("ready control")?
                .parse()?,
            ready["ready"][1].as_str().context("ready data")?.parse()?,
            ready["ready"][2].as_str().context("ready query")?.parse()?,
        );
        Ok(process)
    }

    async fn read(&mut self) -> Result<Value> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let line = self
                    .stdout
                    .next_line()
                    .await?
                    .context("child exited before reply")?;
                if let Some((_, reply)) = line.split_once("RAFT:") {
                    return Ok::<_, anyhow::Error>(serde_json::from_str(reply)?);
                }
            }
        })
        .await?
    }

    pub async fn rpc(&mut self, command: Value) -> Result<Value> {
        self.stdin
            .write_all(format!("{command}\n").as_bytes())
            .await?;
        self.stdin.flush().await?;
        let response = self.read().await?;
        ensure!(
            response.get("error").is_none(),
            "child command failed: {response}"
        );
        Ok(response)
    }

    pub async fn status(&mut self, id: u8) -> Result<Value> {
        self.rpc(json!({"op":"status","group":id})).await
    }

    pub async fn ready(&mut self, id: u8) -> Result<Value> {
        for _ in 0..300 {
            let status = self.status(id).await?;
            if status["ready"] == true {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        bail!("leader did not become ready")
    }

    pub async fn committed(&mut self, id: u8, index: u64) -> Result<()> {
        for _ in 0..300 {
            let status = self.status(id).await?;
            if status["committed"].as_u64().unwrap_or(0) >= index
                && status["applied"].as_u64().unwrap_or(0) >= index
            {
                return Ok(());
            }
            self.rpc(json!({"op":"tick"})).await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        bail!("node did not apply committed index {index}")
    }

    pub async fn close(mut self) -> Result<()> {
        self.rpc(json!({"op":"shutdown"})).await?;
        ensure!(
            tokio::time::timeout(Duration::from_secs(10), self.child.wait())
                .await??
                .success(),
            "child failed shutdown"
        );
        Ok(())
    }
}
