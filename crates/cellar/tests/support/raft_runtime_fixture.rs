//! Real engines and RawNodes; the small application stub is not SQLite/LTX qualification.
#![allow(clippy::disallowed_methods)]

use super::fault::{FaultFileSystem, Gate};
use anyhow::{bail, ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use protobuf::Message as _;
use raft::prelude::{Message, MessageType, Snapshot};
use raft::Storage;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub fn scope(id: u8) -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: format!("Counter:{id}"),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

pub fn storage_options(
    path: &Path,
    member: usize,
    process: u8,
    mode: LogOpenMode,
) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        path.into(),
        NodeId::new([member as u8; 16]).unwrap(),
        DiskLineage::new([100 + member as u8; 16]).unwrap(),
        ProcessGeneration::new([process; 16]).unwrap(),
        mode,
    )
}

pub fn runtime_options(member: usize) -> RaftRuntimeOptions {
    RaftRuntimeOptions {
        tick_interval_ms: None,
        election_seed: Some([10 + member as u8; 32]),
        ..Default::default()
    }
}

pub async fn seed_group(log: &ReplicaLog, id: u8, member: usize, voters: usize) -> Result<()> {
    log.create_group(GroupBootstrap {
        scope: scope(id),
        member: MemberId::new(member as u64)?,
        birth_digest: [id; 32],
        voters: (1..=voters)
            .map(|id| MemberId::new(id as u64).unwrap())
            .collect(),
        learners: Vec::new(),
    })
    .await?;
    Ok(())
}

pub struct Host {
    pub domain: ExecutionDomain,
    pub log: ReplicaLog,
    pub runtime: RaftRuntime,
    pub events: RuntimeEvents,
    pub gate: Arc<Gate>,
    pub path: PathBuf,
}

pub struct Cluster {
    pub hosts: Vec<Host>,
    pub hold_apply: BTreeSet<(usize, GroupId)>,
    pub held_apply: Vec<(usize, ApplyTicket)>,
    pub hold_append_responses: BTreeSet<GroupId>,
    pub hold_read_responses: bool,
    pub held_messages: VecDeque<(GroupScope, Message)>,
    pub light_commits: Vec<(usize, GroupId, u64)>,
    pub persisted: Vec<(usize, GroupId, u64)>,
    pub applied: Vec<(usize, ApplyTicket)>,
    pub allow_snapshots: bool,
    pub held_snapshots: Vec<(usize, ApplyTicket, Snapshot)>,
    pub installed_snapshots: Vec<(usize, ApplyTicket)>,
    network: VecDeque<(GroupScope, Message)>,
    pending: FuturesUnordered<futures_util::future::BoxFuture<'static, Result<(), RuntimeError>>>,
    _directory: Option<tempfile::TempDir>,
}

impl Cluster {
    pub async fn new(count: usize, ids: &[u8]) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let mut cluster = Self::at(directory.path(), count, ids).await?;
        cluster._directory = Some(directory);
        Ok(cluster)
    }

    pub async fn at(root: &Path, count: usize, ids: &[u8]) -> Result<Self> {
        let mut hosts = Vec::new();
        for member in 1..=count {
            let path = root.join(format!("node-{member}"));
            let gate = Gate::new(None);
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let log = domain
                .run(ReplicaLog::open(
                    storage_options(&path, member, member as u8, LogOpenMode::Create),
                    Arc::new(FaultFileSystem { gate: gate.clone() }),
                ))
                .await??;
            for &id in ids {
                seed_group(&log, id, member, count).await?;
            }
            let (runtime, events) =
                RaftRuntime::start(log.clone(), runtime_options(member)).await?;
            hosts.push(Host {
                domain,
                log,
                runtime,
                events,
                gate,
                path,
            });
        }
        Ok(Self {
            hosts,
            hold_apply: BTreeSet::new(),
            held_apply: Vec::new(),
            hold_append_responses: BTreeSet::new(),
            hold_read_responses: false,
            held_messages: VecDeque::new(),
            light_commits: Vec::new(),
            persisted: Vec::new(),
            applied: Vec::new(),
            allow_snapshots: false,
            held_snapshots: Vec::new(),
            installed_snapshots: Vec::new(),
            network: VecDeque::new(),
            pending: FuturesUnordered::new(),
            _directory: None,
        })
    }

    pub async fn pump(&mut self) -> Result<()> {
        for index in 0..self.hosts.len() {
            while let Ok(event) = self.hosts[index].events.try_recv() {
                match event.kind {
                    RuntimeEventKind::Message { replica, message } => {
                        self.network.push_back((replica.scope, message))
                    }
                    RuntimeEventKind::Apply { ticket, entry } => {
                        ensure!(
                            entry_digest(&entry) == ticket.entry_digest(),
                            "apply ticket digest"
                        );
                        let log = self.hosts[index].log.group(&ticket.replica().scope)?;
                        ensure!(
                            log.initial_state()?.hard_state.commit >= ticket.position().index.get(),
                            "apply before durable commit"
                        );
                        if self
                            .hold_apply
                            .contains(&(index, ticket.replica().scope.group))
                        {
                            self.held_apply.push((index, ticket));
                        } else {
                            self.finish_apply(index, ticket).await?;
                        }
                    }
                    RuntimeEventKind::Applied { ticket, applied } => {
                        let group = self.hosts[index].log.group(&ticket.replica().scope)?;
                        ensure!(
                            group.identity() == ticket.replica()
                                && group.applied()?.position.index >= applied.position.index,
                            "publication before marker sync"
                        );
                        self.applied.push((index, ticket));
                    }
                    RuntimeEventKind::Persisted {
                        replica,
                        sequence,
                        committed,
                        light_commit,
                        ..
                    } => {
                        self.persisted.push((index, replica.scope.group, sequence));
                        if light_commit {
                            self.light_commits
                                .push((index, replica.scope.group, committed.get()));
                        }
                    }
                    RuntimeEventKind::Snapshot {
                        ticket, snapshot, ..
                    } => {
                        ensure!(
                            self.allow_snapshots,
                            "unexpected snapshot in retained-log fixture"
                        );
                        self.held_snapshots.push((index, ticket, snapshot));
                    }
                    RuntimeEventKind::SnapshotInstalled { ticket, applied } => {
                        ensure!(self.allow_snapshots);
                        ensure!(
                            self.hosts[index]
                                .log
                                .group(&ticket.replica().scope)?
                                .applied()?
                                .position
                                .index
                                >= applied.position.index
                        );
                        self.installed_snapshots.push((index, ticket));
                    }
                    RuntimeEventKind::Failed { error, .. } => {
                        bail!("runtime group failed: {error}")
                    }
                    RuntimeEventKind::SnapshotAborted { .. } => {
                        bail!("snapshot cancelled without a fixture request")
                    }
                    RuntimeEventKind::ReadIndex { .. } | RuntimeEventKind::Checkpointed { .. } => {}
                }
            }
        }
        ensure!(
            self.network.len() + self.held_messages.len() + self.pending.len() < 1024,
            "bounded fixture network"
        );
        while self.pending.len() < 64 {
            let Some((scope, message)) = self.network.pop_front() else {
                break;
            };
            if (self.hold_append_responses.contains(&scope.group)
                && message.get_msg_type() == MessageType::MsgAppendResponse)
                || (self.hold_read_responses
                    && message.get_msg_type() == MessageType::MsgHeartbeatResponse
                    && !message.context.is_empty())
            {
                self.held_messages.push_back((scope, message));
                continue;
            }
            let target = message.to as usize;
            if target == 0 || target > self.hosts.len() {
                continue;
            }
            let runtime = self.hosts[target - 1].runtime.clone();
            self.pending
                .push(async move { runtime.step(&scope, message).await }.boxed());
        }
        while let Some(Some(result)) = self.pending.next().now_or_never() {
            result?;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(())
    }

    pub async fn ready(&mut self, id: u8, leader: usize) -> Result<()> {
        for _ in 0..3000 {
            self.pump().await?;
            if self.hosts[leader]
                .runtime
                .status(&scope(id))
                .await?
                .protocol_ready
            {
                return Ok(());
            }
        }
        bail!("leader {leader} for group {id} did not pass its current-term barrier")
    }

    pub async fn settled(&mut self, ids: &[u8]) -> Result<()> {
        let mut stable = 0;
        for _ in 0..3000 {
            self.pump().await?;
            let mut quiet = self.network.is_empty() && self.pending.is_empty();
            for host in &self.hosts {
                for &id in ids {
                    let state = host.runtime.status(&scope(id)).await?;
                    quiet &= !state.awaiting_ready
                        && !state.awaiting_apply
                        && state.applied.index.get() >= 2;
                }
            }
            stable = if quiet { stable + 1 } else { 0 };
            if stable >= 3 {
                return Ok(());
            }
        }
        bail!("fixture did not reach a protocol boundary")
    }

    pub async fn elect(&mut self, id: u8, leader: usize) -> Result<()> {
        self.hosts[leader].runtime.campaign(&scope(id)).await?;
        self.ready(id, leader).await
    }

    pub async fn applied_to(&mut self, id: u8, node: usize, index: u64) -> Result<()> {
        for _ in 0..3000 {
            self.pump().await?;
            if self.hosts[node]
                .runtime
                .status(&scope(id))
                .await?
                .applied
                .index
                .get()
                >= index
            {
                return Ok(());
            }
        }
        bail!("group {id} node {node} never applied {index}")
    }

    pub async fn finish_apply(&self, index: usize, ticket: ApplyTicket) -> Result<()> {
        let image = write_image(&self.hosts[index].path, &ticket)?;
        self.hosts[index]
            .runtime
            .complete_apply(ApplyCompletion { ticket, image })
            .await?;
        Ok(())
    }

    pub fn release_messages(&mut self) {
        self.hold_append_responses.clear();
        self.hold_read_responses = false;
        self.network.append(&mut self.held_messages);
    }

    pub async fn close(mut self) -> Result<()> {
        for host in &self.hosts {
            host.runtime.close().await?;
            host.log.close().await?;
            host.domain.shutdown().await;
        }
        self.pending.clear();
        Ok(())
    }
}

pub fn entry_digest(entry: &raft::prelude::Entry) -> [u8; 32] {
    Sha256::digest(entry.write_to_bytes().unwrap()).into()
}

pub fn write_image(path: &Path, ticket: &ApplyTicket) -> Result<CommittedImage> {
    let name = format!(
        "{}-{}.fixture",
        hex::encode(ticket.replica().scope.group.bytes()),
        ticket.position().index.get()
    );
    let bytes = format!(
        "application fixture at {}:{}",
        ticket.position().term.get(),
        ticket.position().index.get()
    )
    .into_bytes();
    let image = path.join(&name);
    std::fs::write(&image, &bytes)?;
    std::fs::File::open(&image)?.sync_all()?;
    std::fs::File::open(path)?.sync_all()?;
    Ok(CommittedImage {
        name,
        digest: Sha256::digest(&bytes).into(),
        length: bytes.len() as u64,
        ltx_epoch: 1,
        txid: ticket.position().index.get(),
        page_size: 512,
        checksum: ticket.position().index.get(),
    })
}
