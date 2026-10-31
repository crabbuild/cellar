// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real node workers share the existing SQLite/image fixture and engine owners.

use super::*;
use std::collections::BTreeMap;

#[path = "cell_group_checkpoint_local.rs"]
mod checkpoint_local;
#[path = "cell_group_network_probes.rs"]
mod network_probes;
#[path = "cell_group_seed_probes.rs"]
mod seed_probes;
pub(super) use checkpoint_local::{
    local_publication, local_shutdown, tcp_cancelled_delivery, tcp_protocol, tcp_publication,
    tcp_retry,
};

#[path = "cell_group_snapshot_owner.rs"]
mod snapshot_owner;
pub(super) use snapshot_owner::{
    checkpoint_bounds, checkpoint_matching_log, checkpoint_owner, checkpoint_shutdown,
};

#[path = "cell_group_checkpoint_pressure.rs"]
mod checkpoint_pressure;
pub(super) use checkpoint_pressure::transfer_pressure;

#[path = "cell_group_checkpoint_receive.rs"]
mod checkpoint_receive;
pub(super) use checkpoint_receive::{install_checkpoint, refuse_checkpoint};

#[path = "cell_group_tcp.rs"]
mod tcp;
pub(super) use tcp::physical_transport;

struct RuntimeCluster {
    snapshot_sources: BTreeMap<(usize, GroupHistory), PreparedGroupManifest>,
    snapshot_transfers: usize,
    drop_snapshot_delivery: bool,
    retry_snapshot: bool,
    failed_snapshot: bool,
    retry_checkpoint: Option<CellGroupCheckpointRegistration>,
    transports: Vec<RaftTransport>,
    appliers: BTreeMap<(usize, GroupHistory), (CellGroupSqlPool, CellGroupApplier)>,
    cluster: Cluster,
    runtimes: Vec<CellGroupRuntime>,
    events: Vec<CellGroupRuntimeEvents>,
    applied: BTreeMap<GroupHistory, Vec<LogIndex>>,
    extra: BTreeMap<GroupHistory, Vec<Voter>>,
    network: VecDeque<(GroupHistory, Message)>,
}

impl RuntimeCluster {
    async fn new() -> Result<Self> {
        Self::with_limits(256, 256, 1_000_000).await
    }

    async fn with_limits(
        max_events: usize,
        max_requests: usize,
        max_cells_per_group: u32,
    ) -> Result<Self> {
        Box::pin(Self::from_cluster(
            Cluster::new().await?,
            max_events,
            max_requests,
            max_cells_per_group,
        ))
        .await
    }

    async fn from_cluster(
        mut cluster: Cluster,
        max_events: usize,
        max_requests: usize,
        max_cells_per_group: u32,
    ) -> Result<Self> {
        let mut runtimes = Vec::new();
        let mut events = Vec::new();
        let mut applied = Vec::new();
        for n in 0..3 {
            let owner = cluster.voters[n].owner.take().context("old direct owner")?;
            let root = owner.manifest().clone();
            drop(owner);
            applied.push(root.marker().position().index);
            let (runtime, receiver) = CellGroupRuntime::start(
                cluster.voters[n].log.clone(),
                RaftRuntimeOptions {
                    tick_interval_ms: None,
                    election_seed: Some([71; 32]),
                    max_events,
                    max_requests,
                    max_cells_per_group,
                    ..Default::default()
                },
            )
            .await?;
            runtime.attach(cluster.permit(n).await?, root).await?;
            runtimes.push(runtime);
            events.push(receiver);
        }
        let applied = BTreeMap::from([(cluster.birth.route().history, applied)]);
        Ok(Self {
            snapshot_sources: BTreeMap::new(),
            snapshot_transfers: 0,
            drop_snapshot_delivery: false,
            retry_snapshot: false,
            failed_snapshot: false,
            retry_checkpoint: None,
            transports: Vec::new(),
            appliers: BTreeMap::new(),
            cluster,
            runtimes,
            events,
            applied,
            extra: BTreeMap::new(),
            network: VecDeque::new(),
        })
    }

    async fn pump(
        &mut self,
        hold: &[GroupHistory],
        held: &mut BTreeMap<GroupHistory, CellGroupApply>,
    ) -> Result<()> {
        let primary = self.cluster.birth.route().history;
        for n in 0..3 {
            for _ in 0..64 {
                let Ok(event) = self.events[n].try_recv() else {
                    break;
                };
                let scope = event.group();
                match event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))? {
                    CellGroupEffect::Message(message) => {
                        self.network.push_back((scope, message.clone()))
                    }
                    CellGroupEffect::Apply(_) => {
                        let work = event.retain_apply().context("retained apply")?;
                        if n == 0 && hold.contains(&scope) {
                            assert!(held.insert(scope, work).is_none());
                        } else {
                            if let Some((pool, applier)) = self.appliers.get(&(n, scope)) {
                                // Keep the physical apply future out of every
                                // caller's inline transport/recovery state.
                                Box::pin(async {
                                    pool.prepare_apply(applier, work)
                                        .await?
                                        .complete(&self.runtimes[n])
                                        .await?;
                                    Ok::<_, anyhow::Error>(())
                                })
                                .await?;
                                continue;
                            }
                            let voter = if scope == primary {
                                &mut self.cluster.voters[n]
                            } else {
                                &mut self.extra.get_mut(&scope).context("extra voter")?[n]
                            };
                            let next = prepare(voter, &work)?;
                            self.runtimes[n].complete_apply(scope, work, next).await?;
                        }
                    }
                    CellGroupEffect::Applied { manifest, .. }
                    | CellGroupEffect::CheckpointInstalled { manifest }
                    | CellGroupEffect::CheckpointPublished { manifest } => {
                        self.applied.get_mut(&scope).context("applied")?[n] =
                            manifest.position().index
                    }
                    CellGroupEffect::ReadIndex { .. } => (),
                    CellGroupEffect::Persist(_) => anyhow::bail!("node worker leaked persistence"),
                }
            }
        }
        let messages = std::mem::take(&mut self.network);
        if !self.transports.is_empty() {
            for (history, message) in messages {
                let snapshot = message.get_msg_type() == MessageType::MsgSnapshot;
                let transport = &self.transports[message.from as usize - 1];
                let delivery = match transport.submit_physical(&history, message.clone()) {
                    Ok(delivery) => delivery,
                    Err(TransportError::Overloaded) if self.retry_snapshot && snapshot => {
                        self.network.push_back((history, message));
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                if snapshot && self.drop_snapshot_delivery {
                    drop(delivery);
                } else {
                    let result = delivery.accepted().await;
                    if snapshot && self.retry_snapshot && !self.failed_snapshot {
                        assert!(
                            result.is_err(),
                            "revoked receiver must refuse the first actual Raft snapshot"
                        );
                        self.failed_snapshot = true;
                        let to = message.to as usize - 1;
                        self.retry_checkpoint =
                            Some(self.transports[to].register_group_checkpoint(
                                self.appliers[&(to, history)].1.clone(),
                            )?);
                        // Do not requeue this failed snapshot. Raft must create
                        // another after its delivery failure is consumed.
                        continue;
                    }
                    result?;
                }
                if snapshot {
                    self.snapshot_transfers += 1;
                }
            }
            if self.retry_snapshot && self.snapshot_transfers == 0 {
                self.runtimes[0].tick().await?;
            }
            return Ok(());
        }
        let mut ordinary = VecDeque::new();
        for (scope, message) in messages {
            if message.get_msg_type() == MessageType::MsgSnapshot {
                self.transfer_checkpoint(scope, message).await?;
            } else {
                ordinary.push_back((scope, message));
            }
        }
        let runtimes = &self.runtimes;
        for (scope, message, result) in futures_util::future::join_all(ordinary.into_iter().map(
            |(scope, message)| async move {
                let result = runtimes[message.to as usize - 1]
                    .admit_step(scope, message.clone())
                    .await;
                (scope, message, result)
            },
        ))
        .await
        {
            match result {
                Ok(()) => (),
                // Preserve the peer frame until bounded transport admission is
                // available. Test deadlines still fail a permanently stuck peer.
                Err(RuntimeError::Overloaded | RuntimeError::NotReady) => {
                    self.network.push_back((scope, message))
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    async fn settle(&mut self, histories: &[GroupHistory]) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                self.pump(&[], &mut BTreeMap::new()).await?;
                let mut done = true;
                for &scope in histories {
                    let status = self.runtimes[0].status(scope).await?;
                    done &= status.ready
                        && self.applied[&scope]
                            .iter()
                            .all(|index| *index >= status.applied.index);
                }
                if done {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await?
    }

    async fn drive(&mut self, target: Option<LogIndex>) -> Result<()> {
        let history = self.cluster.birth.route().history;
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                self.pump(&[], &mut BTreeMap::new()).await?;
                let status = match self.runtimes[0].status(history).await {
                    Ok(status) => status,
                    Err(RuntimeError::Overloaded) => {
                        // With one ordinary slot, an admitted peer frame can
                        // still own it. Keep pumping under the existing deadline.
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                let target = target.unwrap_or(status.applied.index);
                if status.ready && self.applied[&history].iter().all(|index| *index >= target) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await?
    }

    async fn close(self) -> Result<()> {
        for transport in &self.transports {
            transport.close().await;
        }
        drop(self.transports);
        for (pool, _) in self.appliers.values() {
            pool.close().await?;
        }
        drop(self.appliers);
        for runtime in &self.runtimes {
            runtime.close().await?;
        }
        drop(self.events);
        drop(self.runtimes);
        drop(self.extra);
        drop(self.snapshot_sources);
        drop(self.retry_checkpoint);
        self.cluster.close().await
    }
}

pub(super) async fn replicated_cells() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    for name in ["worker-a", "worker-b", "worker-c"] {
        let cell = history(&route, name)?;
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let accepted = fixture.runtimes[0]
            .propose(route.history, request(&route, cell, 1, &cut)?)
            .await?;
        fixture.drive(Some(accepted.position.index)).await?;
    }
    for voter in &fixture.cluster.voters {
        let group = voter.log.cell_group(&route.history)?;
        assert_eq!(
            group
                .group_manifest()?
                .context("published group")?
                .root()
                .cells(),
            3
        );
    }
    fixture.close().await
}

pub(super) async fn cancelled_close_joins_sync() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    fixture.runtimes[0]
        .propose(
            route.history,
            request(&route, history(&route, "closing")?, 1, &cut)?,
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let runtime = fixture.runtimes[0].clone();
    let closing = runtime.close();
    assert!(tokio::time::timeout(Duration::from_millis(30), closing)
        .await
        .is_err());
    assert!(CellGroupRuntime::start(
        fixture.cluster.voters[0].log.clone(),
        RaftRuntimeOptions::default()
    )
    .await
    .is_err());
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), runtime.close()).await??;
    let (replacement, events) = CellGroupRuntime::start(
        fixture.cluster.voters[0].log.clone(),
        RaftRuntimeOptions::default(),
    )
    .await?;
    replacement.close().await?;
    drop(events);
    fixture.close().await
}

impl RuntimeCluster {
    async fn add_group(&mut self, number: u8) -> Result<GroupHistory> {
        let scope = GroupHistory {
            cluster: self.cluster.birth.route().history.cluster,
            namespace: NamespaceId::new([number; 16])?,
            group: GroupId::new([number; 16])?,
            incarnation: IncarnationId::new([number; 16])?,
        };
        let domain = &self.cluster.domain;
        let catalog = Catalog::new(
            Arc::new(object_store::memory::InMemory::new()),
            CatalogOptions::default(),
        )?;
        let namespace = domain
            .run(catalog.open_namespace(NamespaceConfig::new(
                scope.cluster,
                scope.namespace,
                cellar_logic::NamespaceMode::Raft,
                [6; 32],
            )))
            .await??;
        let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
        let request = directory.prepare_group_birth(CellGroupBirthSpec {
            history: scope,
            range: CellRange::all(),
            epoch: RouteEpoch::new(1)?,
            creation: ProposalId::new([number; 16])?,
            voters: (1..=3)
                .map(|n| InitialMember {
                    member: MemberId::new(n).unwrap(),
                    node: NodeId::new([n as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        })?;
        domain
            .run(directory.reserve_group_birth(&request))
            .await??;
        let initialization = directory.prepare_initialization(
            ProposalId::new([number + 1; 16])?,
            &[request.route().clone()],
        )?;
        domain.run(directory.publish(&initialization)).await??;
        let birth = domain
            .run(directory.materialize_group_birth(&request))
            .await??;
        let mut voters = Vec::new();
        let mut applied = Vec::new();
        for n in 0..3 {
            let log = self.cluster.voters[n].log.clone();
            let permit = domain
                .run(directory.bootstrap_group_member(&birth, &log))
                .await??;
            let base = self
                .cluster
                ._root
                .path()
                .join(format!("extra-{number}-node-{n}"));
            let tree = ManifestTree::new(birth.route().clone(), ManifestLimits::default())?;
            let mut pages = ManifestPageStore::open(
                &base.join("pages"),
                Arc::new(DirectFileSystem),
                1024,
                16 * 1024 * 1024,
                16,
            )?;
            let images = GenerationStore::open(
                base.join("images"),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?;
            let marker = permit.group().group_manifest()?.context("genesis")?;
            applied.push(marker.position().index);
            let root = tree.prepare_complete(
                permit.group(),
                &mut pages,
                marker.root(),
                marker.position(),
                marker.membership_revision(),
                |_| panic!("empty genesis"),
            )?;
            self.runtimes[n].attach(permit, root).await?;
            voters.push(Voter {
                log,
                owner: None,
                pages: Some(pages),
                images,
                extents: None,
                tree,
                gate: self.cluster.voters[n].gate.clone(),
                paused: false,
                held: None,
            });
        }
        self.applied.insert(scope, applied);
        self.extra.insert(scope, voters);
        Ok(scope)
    }
}

pub(super) async fn shared_batches() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let second = fixture.add_group(31).await?;
    let third = fixture.add_group(41).await?;
    let histories = [fixture.cluster.birth.route().history, second, third];
    let before = fixture.cluster.voters[0].log.performance_snapshot();
    for result in futures_util::future::join_all(
        histories
            .iter()
            .map(|&history| fixture.runtimes[0].campaign(history)),
    )
    .await
    {
        result?;
    }
    fixture.settle(&histories).await?;
    let after = fixture.cluster.voters[0].log.performance_snapshot();
    assert!(before.available && after.available && !before.saturated && !after.saturated);
    let groups = |s: &RaftPerformanceSnapshot| {
        s.counters
            .iter()
            .find(|c| c.name == "engine_persist_groups")
            .unwrap()
            .value
    };
    let batches = |s: &RaftPerformanceSnapshot| {
        s.operations
            .iter()
            .find(|o| o.name == "engine_persist")
            .unwrap()
            .succeeded
    };
    let group_delta = groups(&after) - groups(&before);
    let batch_delta = batches(&after) - batches(&before);
    assert!(batch_delta > 0 && group_delta > batch_delta, "actual engine batches must include multiple groups: {group_delta} groups / {batch_delta} batches");
    fixture.close().await
}

pub(super) async fn shared_proposal_budget() -> Result<()> {
    let mut fixture = RuntimeCluster::with_limits(256, 8, 1_000_000).await?;
    let first = fixture.cluster.birth.route().history;
    let second = fixture.add_group(61).await?;
    for scope in [first, second] {
        fixture.runtimes[0].campaign(scope).await?;
        fixture.settle(&[scope]).await?;
    }
    let route = fixture.cluster.birth.route().clone();
    let other_route = fixture.extra[&second][0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("other group")?
        .route()
        .clone();
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let mut first_request = None;
    let mut target = LogIndex::new(0);
    for n in 0..8 {
        let data = request(&route, history(&route, &format!("budget-{n}"))?, 1, &cut)?;
        let accepted = fixture.runtimes[0].propose(first, data.clone()).await?;
        if n == 0 {
            first_request = Some((data, accepted.clone()));
        }
        target = accepted.position.index;
    }
    let other = request(&other_route, history(&other_route, "other-group")?, 1, &cut)?;
    assert!(matches!(
        fixture.runtimes[0].propose(second, other.clone()).await,
        Err(RuntimeError::Overloaded)
    ));
    let (data, accepted) = first_request.context("first acceptance")?;
    let retry = fixture.runtimes[0].propose(first, data).await?;
    assert_eq!(accepted.position, retry.position);
    assert_eq!(accepted.digest, retry.digest);
    tokio::time::timeout(Duration::from_secs(20), async {
        while fixture.applied[&first].iter().any(|index| *index < target) {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let accepted = fixture.runtimes[0].propose(second, other).await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while fixture.applied[&second]
            .iter()
            .any(|index| *index < accepted.position.index)
        {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    fixture.close().await
}

pub(super) async fn bounded_apply_completion() -> Result<()> {
    let mut fixture = RuntimeCluster::with_limits(2, 256, 1_000_000).await?;
    let first = fixture.cluster.birth.route().history;
    let second = fixture.add_group(51).await?;
    let histories = [first, second];
    for result in futures_util::future::join_all(
        histories
            .iter()
            .map(|&scope| fixture.runtimes[0].campaign(scope)),
    )
    .await
    {
        result?;
    }
    fixture
        .settle(&histories)
        .await
        .context("settling bounded-output groups")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (7);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let mut targets = BTreeMap::new();
    for scope in histories {
        let route = if scope == first {
            fixture.cluster.birth.route().clone()
        } else {
            fixture.extra[&scope][0]
                .log
                .cell_group(&scope)?
                .group_manifest()?
                .context("route")?
                .route()
                .clone()
        };
        let accepted = fixture.runtimes[0]
            .propose(scope, request(&route, history(&route, "quota")?, 1, &cut)?)
            .await?;
        targets.insert(scope, accepted.position.index);
    }
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        while held.len() != 2 {
            fixture.pump(&histories, &mut held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| {
        format!(
            "bounded apply progress: held={}, applied={:?}",
            held.len(),
            fixture.applied
        )
    })??;
    // Both output slots are retained by physical work. Completing one must
    // release its reservation before admitting its distinct Applied event.
    let work = held.remove(&first).context("first physical job")?;
    let next = prepare(&mut fixture.cluster.voters[0], &work)?;
    fixture.runtimes[0]
        .complete_apply(first, work, next)
        .await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while fixture.applied[&first][0] < targets[&first] {
            fixture.pump(&[second], &mut held).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| {
        format!(
            "bounded apply progress: held={}, applied={:?}",
            held.len(),
            fixture.applied
        )
    })??;
    assert_eq!(held.len(), 1, "another group's physical job remains held");
    let work = held.remove(&second).context("second physical job")?;
    let next = prepare(
        &mut fixture.extra.get_mut(&second).context("second voter")?[0],
        &work,
    )?;
    fixture.runtimes[0]
        .complete_apply(second, work, next)
        .await?;
    fixture
        .settle(&histories)
        .await
        .context("settling bounded-output groups")?;
    for scope in histories {
        assert!(fixture.applied[&scope]
            .iter()
            .all(|index| *index >= targets[&scope]));
    }
    fixture.close().await
}

// A pending peer/protocol request retains ordinary admission through a physical
// sync. Attachment must refuse before attempting to claim an existing owner.
pub(super) async fn attachment_reserves_before_owner() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    let scope = cluster.birth.route().history;
    let owner = cluster.voters[0].owner.take().context("direct owner")?;
    let root = owner.manifest().clone();
    drop(owner);
    let (runtime, events) = CellGroupRuntime::start(
        cluster.voters[0].log.clone(),
        RaftRuntimeOptions {
            max_requests: 1,
            tick_interval_ms: None,
            election_seed: Some([71; 32]),
            ..Default::default()
        },
    )
    .await?;
    runtime
        .attach(cluster.permit(0).await?, root.clone())
        .await?;
    let duplicate = cluster.permit(0).await?;
    let gate = cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let mut vote = Message::default();
    vote.set_msg_type(MessageType::MsgRequestVote);
    vote.from = 2;
    vote.to = 1;
    vote.term = root.marker().position().term.get() + 1;
    vote.index = root.marker().position().index.get();
    vote.log_term = root.marker().position().term.get();
    runtime.step(scope, vote.clone()).await?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let mut pending = Box::pin(runtime.step(scope, vote));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pending.as_mut())
            .await
            .is_err()
    );
    assert!(matches!(
        runtime.attach(duplicate, root).await,
        Err(RuntimeError::Overloaded)
    ));
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), pending).await??;
    runtime.close().await?;
    drop(events);
    drop(runtime);
    cluster.close().await
}

#[path = "cell_group_sql.rs"]
mod sql;
pub(super) use sql::{
    controller_apply_faults, controller_close, controller_owners, controller_replication,
};
pub(super) use sql::{owned_command_failure, owned_commands};

pub(super) async fn cell_receipts(rooted: bool) -> Result<()> {
    sql::cell_receipts(rooted).await
}

pub(super) async fn sql_pool_admission(rooted: bool) -> Result<()> {
    sql::pool_admission(rooted).await
}

pub(super) async fn reconcile_sql_producers(rooted: bool) -> Result<()> {
    sql::reconcile_producers(rooted).await
}

pub(super) async fn reconcile_sql_refusals(rooted: bool) -> Result<()> {
    sql::reconcile_refusals(rooted).await
}

pub(super) async fn initialize_cells(rooted: bool) -> Result<()> {
    sql::initialize_cells(rooted).await
}

pub(super) async fn recover_initialization(rooted: bool) -> Result<()> {
    sql::recover_initialization(rooted).await
}

pub(super) async fn submission_rejections(rooted: bool) -> Result<()> {
    sql::submission_rejections(rooted).await
}

pub(super) use sql::{coherent_reads, query_lifecycle};

pub(super) use sql::apply_images;

pub(super) use sql::{apply_cancellation, apply_failure};

pub(super) use sql::recover_apply;

pub(super) use sql::registered_commands;

pub(super) use sql::registered_queries;

pub(super) use sql::routed_queries;

pub(super) use sql::routed_commands;

pub(super) async fn commands_over_tcp(rooted: bool) -> Result<()> {
    Box::pin(sql::commands_over_tcp(rooted)).await
}

pub(super) use sql::{
    enrollment_requires_registration, owned_enrollment, owned_enrollment_failure,
};

pub(super) use sql::{remote_queries, remote_query_lifecycle};

pub(super) use sql::{nonmember_queries, nonmember_query_revocation};

pub(super) use sql::cached_dispatch_execution;

pub(super) use sql::{remote_command_lifecycle, remote_commands};

pub(super) use sql::{remote_enrollment, remote_enrollment_lifecycle};

pub(super) use sql::{receipt_lookup_cancelled, receipt_lookup_ingress, receipt_lookup_pending};

pub(super) use sql::{
    registration_close, registration_live, registration_recovery, registration_refusals,
};

pub(super) use sql::{
    group_quarantine_retains_producer_and_serves_healthy_group,
    shared_engine_sync_failure_still_stops_all_groups,
};

pub(super) use sql::{
    provisioning_coalesces_and_serves, provisioning_joins_cancelled_close,
    provisioning_recovers_selected_roots, provisioning_refusals_preserve_healthy_group,
    provisioning_retries_retained_owner,
};

pub(super) use sql::{automatic_checkpoints, controller_checkpoint_shutdown};

pub(super) use sql::{membership_lifecycle, membership_sync};

pub(super) use sql::{seed_minority, seed_source};

pub(super) use sql::seed_install;

pub(super) use sql::seed_receiver_faults;

pub(super) use sql::{seed_persistence, seed_transfer_revocation};

pub(super) use sql::{seed_crash, seed_process_fixture};

pub(super) use sql::{membership_crash, membership_process_fixture};

pub(super) use sql::{seed_retry, shared_inventory};
