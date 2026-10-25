// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Two independent three-voter native clusters; real receiver transactions and
//! TLS Raft transports, with deliberately ambiguous source-side completion.
#![allow(clippy::disallowed_methods)]
use super::{faults, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    consensus::*, Application, Node, NodeRaftConfig, NodeRaftListeners, Output, Registry,
    ShutdownMode,
};
use futures_util::future::BoxFuture;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

const BINDING: [u8; 32] = [71; 32];

fn scope(base: u8, class: &str) -> Result<GroupScope> {
    Ok(GroupScope {
        cluster: ClusterId::new([base; 16])?,
        namespace: NamespaceId::new([base + 1; 16])?,
        cell: format!("{class}:one"),
        group: GroupId::new([base + 50; 16])?,
        incarnation: IncarnationId::new([base + 51; 16])?,
    })
}

struct Definition {
    application: Arc<dyn Application>,
    schema: [u8; 32],
    definition: [u8; 32],
    prime: OperationContract,
    get: OperationContract,
    write: OperationContract,
}

impl Definition {
    fn new(class: RaftClass, write: &str) -> Result<Self> {
        let definition = class.definition_digest();
        let (schema, prime, get, write) = (
            class.schema_digest(),
            class.contract("Prime").context("Prime")?,
            class.contract("Get").context("Get")?,
            class.contract(write).context("write")?,
        );
        Ok(Self {
            application: Arc::new(
                Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
            ),
            schema,
            definition,
            prime,
            get,
            write,
        })
    }
}

struct Cluster {
    nodes: Vec<Option<Node>>,
    services: Vec<Option<RaftCells>>,
    definitions: Vec<Definition>,
    registration: RaftCellRegistration,
    _roots: Vec<tempfile::TempDir>,
    store: Arc<faults::Store>,
}

impl Cluster {
    async fn start(
        base: u8,
        name: &str,
        definitions: Vec<Definition>,
        store: Arc<faults::Store>,
    ) -> Result<Self> {
        ensure!(
            definitions.len() == 3
                && definitions
                    .iter()
                    .all(|d| d.schema == definitions[0].schema)
        );
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace: NamespaceConfig::new(
                ClusterId::new([base; 16])?,
                NamespaceId::new([base + 1; 16])?,
                NamespaceMode::Raft,
                definitions[0].schema,
            ),
            class: name.into(),
            birth: BirthProposal {
                scope: scope(base, name)?,
                voters: (1..=3)
                    .map(|member| {
                        Ok(InitialMember {
                            member: MemberId::new(member)?,
                            node: NodeId::new([base + member as u8 - 1; 16])?,
                        })
                    })
                    .collect::<Result<_>>()?,
                learners: vec![],
            },
        };
        let mut cluster = Self {
            nodes: vec![],
            services: vec![],
            definitions,
            registration,
            _roots: vec![],
            store,
        };
        let mut peers = vec![];
        let mut listeners = vec![];
        for index in 0..3 {
            let root = tempfile::tempdir()?;
            let label = format!("effect-native-{}", base + index as u8);
            let node = public_node::node_named(
                root.path(),
                cluster.definitions[index].application.clone(),
                &label,
            )
            .await?;
            let bound = NodeRaftListeners {
                control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            };
            peers.push(RaftPeerNode {
                node: NodeId::new([base + index as u8; 16])?,
                disk: DiskLineage::new([base + index as u8 + 20; 16])?,
                process: ProcessGeneration::new([base + index as u8 + 30; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: bound.control.local_addr()?,
                data_address: bound.data.local_addr()?,
                query_address: bound.query.local_addr()?,
            });
            cluster.nodes.push(Some(node));
            cluster._roots.push(root);
            listeners.push(bound);
        }
        for (index, listeners) in listeners.into_iter().enumerate() {
            let node = cluster.nodes[index].as_ref().context("node")?;
            let peer = &peers[index];
            let service = node
                .start_raft_cells(
                    RaftCellsConfig {
                        infrastructure: NodeRaftConfig {
                            log: ReplicaLogOptions::new(
                                node.info().data_dir.join("raft"),
                                peer.node,
                                peer.disk,
                                peer.process,
                                LogOpenMode::Create,
                            ),
                            consensus: RaftRuntimeOptions {
                                tick_interval_ms: Some(20),
                                ..Default::default()
                            },
                            transport: TransportOptions::default(),
                        },
                        catalog: CatalogOptions {
                            prefix: "effects/native".into(),
                            ..Default::default()
                        },
                        cells: vec![cluster.registration.clone()],
                        peers: peers.clone(),
                        query_origins: vec![],
                        host: RaftHostOptions {
                            effects: Some(RaftEffectOptions {
                                interval_ms: 20,
                                operation_timeout_ms: 3000,
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    },
                    RaftCellsServices {
                        listeners,
                        tls: public_node::tls()?,
                        store: cluster.store.clone(),
                        application: cluster.definitions[index].application.clone(),
                        versions: public_node::version_keys()?,
                    },
                )
                .await?;
            cluster.services.push(Some(service));
        }
        let leader = cluster.leader().await?;
        cluster
            .write(
                leader,
                cluster.definitions[leader].prime.clone(),
                vec![],
                1,
                Durability::S3,
            )
            .await?;
        Ok(cluster)
    }

    fn cell(&self, index: usize) -> Result<RaftCellHandle> {
        Ok(self.services[index]
            .as_ref()
            .context("stopped service")?
            .cell(
                self.registration.namespace.namespace,
                &self.registration.birth.scope.cell,
            )?)
    }

    async fn leader(&self) -> Result<usize> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for index in 0..3 {
                    if self.services[index].is_some()
                        && self
                            .cell(index)?
                            .status()
                            .await
                            .is_ok_and(|s| s.command_ready)
                    {
                        return Ok::<_, anyhow::Error>(index);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?
    }

    async fn write(
        &self,
        index: usize,
        contract: OperationContract,
        input: Vec<u8>,
        sequence: u64,
        durability: Durability,
    ) -> Result<CommandReceipt> {
        let mut options = public_node::options(sequence)?;
        options.durability = durability;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = self
                .cell(index)?
                .command(contract.clone(), input.clone(), options.clone())
                .await;
            if !matches!(result, Err(CommandFailure::Overloaded))
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(result?);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn read(&self, index: usize, minimum: Option<VersionToken>) -> Result<QueryResult> {
        Ok(self
            .cell(index)?
            .query(
                self.definitions[index].get.clone(),
                vec![],
                QueryOptions {
                    consistency: minimum.map_or(QueryConsistency::Stale, QueryConsistency::AtLeast),
                    timeout: Duration::from_millis(1000),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn settled(&self, sequence: u64) -> Result<QueryResult> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for index in 0..3 {
                    if self.services[index].is_some() {
                        if let Ok(read) = self.read(index, None).await {
                            if read.result.len() == 24
                                && u64::from_be_bytes(read.result[16..24].try_into()?) == sequence
                            {
                                return Ok::<_, anyhow::Error>(read);
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?
    }

    async fn stop(&mut self, index: usize) -> Result<RaftCloseReport> {
        let service = self.services[index]
            .take()
            .context("service already stopped")?;
        let report = service.close().await?;
        if let Some(node) = self.nodes[index].take() {
            node.shutdown(ShutdownMode::Preserve).await?;
        }
        Ok(report)
    }

    async fn close(mut self) -> Result<()> {
        self.store
            .archive_unavailable
            .store(false, Ordering::SeqCst);
        self.store.archive_hang.store(false, Ordering::SeqCst);
        self.store.archive_resume.notify_waiters();
        let leader = self.leader().await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = self.cell(leader)?.status().await?;
                if status.command_ready && status.archived >= status.applied.index {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        for index in 0..3 {
            if self.services[index].is_none() {
                continue;
            }
            let report = self.stop(index).await?;
            ensure!(
                report.unfinished.is_empty(),
                "history {:?}, member {index}, unfinished {:?}",
                self.registration.birth.scope,
                report.unfinished
            );
        }
        Ok(())
    }
}

struct ObservedSink {
    native: Arc<RaftNativeEffectSink>,
    requests: Mutex<Vec<RaftDeliveryRequest>>,
    attempts: AtomicUsize,
    block_source_archive: Arc<AtomicBool>,
    source_store: Arc<faults::Store>,
}

impl RaftEffectSink for ObservedSink {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        {
            let mut requests = self.requests.lock().unwrap_or_else(|e| e.into_inner());
            if requests.len() < 32 {
                requests.push(request.clone());
            }
        }
        let call = self.native.deliver(request);
        let store = self.source_store.clone();
        let block = self.block_source_archive.clone();
        // The source host owns this complete future; no task is detached.
        Box::pin(async move {
            let proof = call.await?;
            if block.swap(false, Ordering::SeqCst) {
                store.archive_unavailable.store(true, Ordering::SeqCst);
            }
            Ok(proof)
        })
    }
}

struct Fixture {
    source: Cluster,
    destination: Cluster,
    sink: Arc<ObservedSink>,
    source_calls: Vec<Arc<AtomicUsize>>,
    destination_calls: Vec<Arc<AtomicUsize>>,
    source_gate: Arc<public_node::Gate>,
    source_entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    destination_gate: Arc<public_node::Gate>,
    _destination_entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    _release_source: public_node::Release,
    _release_destination: public_node::Release,
}

impl Fixture {
    async fn start(hold_source: bool, hold_destination: bool) -> Result<Self> {
        let source_key = Arc::new(RaftEffectSigningKey::new([72; 32])?);
        let receiver_key = Arc::new(RaftEffectSigningKey::new([73; 32])?);
        let (destination_gate, destination_entered) = public_node::Gate::new();
        let destination_release = public_node::Release(destination_gate.clone());
        let (source_gate, source_entered) = public_node::Gate::new();
        let source_release = public_node::Release(source_gate.clone());
        if !hold_source {
            source_gate.release();
        }
        if !hold_destination {
            destination_gate.release();
        }
        let source_calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let destination_calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions=destination_calls.iter().map(|calls| {
            let calls=calls.clone();let gate=destination_gate.clone();
            let origin=scope(1,"Producer")?;
            let inbox=RaftInbox::new(vec![RaftEffectSourceGrant::new(origin.cluster,origin.namespace,"Producer","Events",BINDING,source_key.verification_key())?],receiver_key.clone(),Durability::S3,16)?;
            Definition::new(RaftClass::new("Receiver",1,[74;32])?
                .inbox("Receive",[75;32],inbox,move |context,payload| {
                    calls.fetch_add(1,Ordering::SeqCst);gate.wait()?;
                    let amount=i64::from_be_bytes(payload.try_into()?);
                    context.sql().execute("UPDATE receiver SET value=value+?1",[amount])?;
                    Ok(amount.to_be_bytes().to_vec())
                })?
                .command("Prime",[76;32],|context,_|{context.sql().execute_batch("CREATE TABLE IF NOT EXISTS receiver(value INTEGER);INSERT INTO receiver SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM receiver)")?;Ok(vec![])})?
                .query("Get",[77;32],|context,_|{let value:i64=context.query_row("SELECT value FROM receiver",[],|row|row.get(0))?;Ok(value.to_be_bytes().to_vec())})?,"Receive")
        }).collect::<Result<_>>()?;
        let destination = Cluster::start(
            11,
            "Receiver",
            definitions,
            Arc::new(faults::Store::default()),
        )
        .await?;
        let target = RaftEffectTarget::new(
            destination.registration.birth.scope.clone(),
            destination.definitions[0].definition,
            destination.definitions[0].write.clone(),
            receiver_key.verification_key(),
            Durability::S3,
        )?;
        let source_store = Arc::new(faults::Store::default());
        let sink = Arc::new(ObservedSink {
            native: RaftNativeEffectSink::new(
                (0..3).map(|i| destination.cell(i)).collect::<Result<_>>()?,
                Duration::from_millis(1000),
                60_000,
            )?,
            requests: Mutex::new(vec![]),
            attempts: AtomicUsize::new(0),
            block_source_archive: Arc::new(AtomicBool::new(false)),
            source_store: source_store.clone(),
        });
        let outbox = RaftOutbox::new(
            source_key,
            RaftOutboxLimits {
                retry_delay_ms: 250,
                ..Default::default()
            },
        )?
        .binding("Events", BINDING, target, sink.clone())?;
        let definitions=source_calls.iter().map(|calls|{
            let calls=calls.clone();let gate=source_gate.clone();
            Definition::new(RaftClass::new("Producer",1,[78;32])?.outbox(outbox.clone())?
                .alarm("Nudge",[82;32],|context,_|{context.sql().execute("UPDATE producer SET value=value+1",[])?;Ok(())})?
                .command("Prime",[79;32],|context,_|{context.sql().execute_batch("CREATE TABLE IF NOT EXISTS producer(value INTEGER);INSERT INTO producer SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM producer)")?;Ok(vec![])})?
                .command("Emit",[80;32],move |context,input|{
                    ensure!(input.len()==9);calls.fetch_add(1,Ordering::SeqCst);
                    let amount=i64::from_be_bytes(input[..8].try_into()?);
                    context.sql().execute("UPDATE producer SET value=value+?1",[amount])?;
                    let sequence=context.emit("Events",&input[..8])?;
                    if input[8]==1 {gate.wait()?;}else if input[8]==2 {anyhow::bail!("rollback private emission");}else if input[8]==3 {context.set_alarm("Nudge",context.inputs().now_unix_millis,&[])?;}
                    Ok(sequence.to_be_bytes().to_vec())
                })?
                .query("Get",[81;32],|context,_|{
                    let value:i64=context.query_row("SELECT value FROM producer",[],|row|row.get(0))?;
                    let progress=context.outbox_progress("Events")?;
                    let mut result=value.to_be_bytes().to_vec();result.extend(progress.emitted.to_be_bytes());result.extend(progress.settled.to_be_bytes());Ok(result)
                })?,"Emit")
        }).collect::<Result<_>>()?;
        let source = Cluster::start(1, "Producer", definitions, source_store).await?;
        Ok(Self {
            source,
            destination,
            sink,
            source_calls,
            destination_calls,
            source_gate,
            source_entered,
            destination_gate,
            _destination_entered: destination_entered,
            _release_source: source_release,
            _release_destination: destination_release,
        })
    }

    fn input(amount: i64, hold: u8) -> Vec<u8> {
        let mut input = amount.to_be_bytes().to_vec();
        input.push(hold);
        input
    }

    fn destination_executions(&self) -> usize {
        self.destination_calls
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .sum()
    }

    fn source_executions(&self) -> usize {
        self.source_calls
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .sum()
    }

    async fn assert_result(&self, sequence: u64, value: i64) -> Result<()> {
        let source = self.source.settled(sequence).await?;
        ensure!(i64::from_be_bytes(source.result[..8].try_into()?) == value);
        ensure!(u64::from_be_bytes(source.result[8..16].try_into()?) == sequence);
        for index in 0..3 {
            if self.source.services[index].is_some() {
                ensure!(
                    self.source
                        .read(index, Some(source.version_token.clone()))
                        .await?
                        .result
                        == source.result
                );
            }
        }
        let leader = self.destination.leader().await?;
        let destination = self.destination.read(leader, None).await?;
        ensure!(i64::from_be_bytes(destination.result.as_slice().try_into()?) == value);
        for index in 0..3 {
            ensure!(
                self.destination
                    .read(index, Some(destination.version_token.clone()))
                    .await?
                    .result
                    == destination.result
            );
        }
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.source_gate.release();
        self.destination_gate.release();
        self.source
            .close()
            .await
            .context("outbox source shutdown")?;
        self.destination
            .close()
            .await
            .context("outbox destination shutdown")
    }
}

pub async fn never_speculative() -> Result<()> {
    let mut f = Fixture::start(true, false)
        .await
        .context("start outbox qualification fixtures")?;
    let leader = f
        .source
        .leader()
        .await
        .context("source readiness before private emission")?;
    let mut options = public_node::options(2)?;
    options.durability = Durability::S3;
    let mut call = Box::pin(f.source.cell(leader)?.command(
        f.source.definitions[leader].write.clone(),
        Fixture::input(7, 1),
        options,
    ));
    // Admission starts before the returned future is polled. Observe an early
    // refusal alongside the gate so it cannot masquerade as a missing callback.
    tokio::time::timeout(Duration::from_secs(5), async {
        cellar::asyncrt::select! {
            entered = f.source_entered.recv() => entered.context("private emission channel closed"),
            result = &mut call => anyhow::bail!("source command completed before private emission: {result:?}"),
        }
    })
    .await
    .context("observe private outbox emission")??;
    tokio::time::sleep(Duration::from_millis(750)).await;
    ensure!(f.sink.attempts.load(Ordering::SeqCst) == 0 && f.destination_executions() == 0);
    f.source_gate.release();
    let receipt = call.await.context("commit released outbox command")?;
    ensure!(receipt.durability == Durability::S3);
    f.assert_result(1, 7)
        .await
        .context("settle committed outbox command")?;
    ensure!(f.destination_executions() == 1 && f.source_executions() == 1);
    let rollback = f
        .source
        .write(
            leader,
            f.source.definitions[leader].write.clone(),
            Fixture::input(3, 2),
            3,
            Durability::S3,
        )
        .await;
    ensure!(rollback.is_err());
    tokio::time::sleep(Duration::from_millis(500)).await;
    f.assert_result(1, 7)
        .await
        .context("outbox state after private rollback")?;
    ensure!(f.destination_executions() == 1);
    f.close().await
}

pub async fn leadership_race() -> Result<()> {
    let mut f = Fixture::start(false, false).await?;
    let target = f.destination.leader().await?;
    f.destination
        .store
        .archive_hang
        .store(true, Ordering::SeqCst);
    let leader = f.source.leader().await?;
    f.source
        .write(
            leader,
            f.source.definitions[leader].write.clone(),
            Fixture::input(9, 0),
            2,
            Durability::S3,
        )
        .await?;
    // The old request has committed business state, but its S3 output is held.
    // Dropping the old source's waiter cannot undo that receiver transaction.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let read = f.destination.read(target, None).await?;
            if i64::from_be_bytes(read.result.as_slice().try_into()?) == 9 {
                let status = f.destination.cell(target)?.status().await?;
                if status.archived < read.version.position.index {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    ensure!(f.destination_executions() == 1);
    let old_attempts = f.sink.attempts.load(Ordering::SeqCst);
    f.source.stop(leader).await?;
    let replacement = f.source.leader().await?;
    ensure!(replacement != leader);
    tokio::time::timeout(Duration::from_secs(10), async {
        while f.sink.attempts.load(Ordering::SeqCst) <= old_attempts {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    {
        let requests = f.sink.requests.lock().unwrap_or_else(|e| e.into_inner());
        ensure!(
            requests.len() >= 2
                && requests.iter().all(|r| r.encoded() == requests[0].encoded()
                    && r.command_id() == requests[0].command_id())
        );
    }
    f.destination
        .store
        .archive_hang
        .store(false, Ordering::SeqCst);
    f.destination.store.archive_resume.notify_waiters();
    f.assert_result(1, 9).await?;
    ensure!(f.destination_executions() == 1 && f.source_executions() == 1);
    f.close().await
}

pub async fn replicated_progress() -> Result<()> {
    let mut f = Fixture::start(false, false).await?;
    f.sink.block_source_archive.store(true, Ordering::SeqCst);
    let leader = f.source.leader().await?;
    f.source
        .write(
            leader,
            f.source.definitions[leader].write.clone(),
            Fixture::input(11, 0),
            2,
            Durability::S3,
        )
        .await?;
    let settled = f.source.settled(1).await?;
    ensure!(f.source.store.archive_unavailable.load(Ordering::SeqCst));
    f.source
        .read(leader, Some(settled.version_token.clone()))
        .await?;
    let status = f.source.cell(leader)?.status().await?;
    ensure!(
        status.applied.index >= settled.version.position.index
            && status.archived < settled.version.position.index,
        "settled cut {}, source applied {}, archived {}",
        settled.version.position.index.get(),
        status.applied.index.get(),
        status.archived.get()
    );
    // Source cleanup committed while its promised S3 completion response is
    // unavailable. Stop the producer and recover progress on another voter.
    let report = f.source.stop(leader).await?;
    ensure!(
        !report.unfinished.is_empty(),
        "missing ambiguous S3 completion in close report"
    );
    let attempts = f.sink.attempts.load(Ordering::SeqCst);
    f.source
        .store
        .archive_unavailable
        .store(false, Ordering::SeqCst);
    f.source.leader().await?;
    f.assert_result(1, 11).await?;
    tokio::time::sleep(Duration::from_millis(750)).await;
    ensure!(
        f.sink.attempts.load(Ordering::SeqCst) == attempts
            && f.destination_executions() == 1
            && f.source_executions() == 1
    );
    f.close().await
}

pub async fn authentication_and_replay() -> Result<()> {
    let f = Fixture::start(false, false).await?;
    let leader = f.source.leader().await?;
    f.source
        .write(
            leader,
            f.source.definitions[leader].write.clone(),
            Fixture::input(7, 0),
            2,
            Durability::S3,
        )
        .await?;
    f.assert_result(1, 7).await?;
    let request = f
        .sink
        .requests
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .first()
        .context("first delivery")?
        .clone();
    let target = f.destination.leader().await?;
    let receiver = f.destination.cell(target)?;
    let contract = f.destination.definitions[target].write.clone();
    let mut options = public_node::options(1)?;
    options.id = request.command_id();
    options.durability = Durability::S3;
    let replay = receiver
        .command(
            contract.clone(),
            request.encoded().to_vec(),
            options.clone(),
        )
        .await?;
    ensure!(replay.durability == Durability::S3);
    request.verify_receipt(&replay.result)?;
    let mut wrong = request.encoded().to_vec();
    let last = wrong.last_mut().context("request bytes")?;
    *last ^= 1;
    ensure!(receiver
        .command(contract.clone(), wrong, options.clone())
        .await
        .is_err());
    ensure!(matches!(
        receiver
            .command(
                f.destination.definitions[target].prime.clone(),
                vec![],
                options.clone()
            )
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let mut insufficient = options.clone();
    insufficient.durability = Durability::Quorum;
    ensure!(matches!(
        receiver
            .command(contract.clone(), request.encoded().to_vec(), insufficient)
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let again = receiver
        .command(
            contract.clone(),
            request.encoded().to_vec(),
            options.clone(),
        )
        .await?;
    ensure!(again.result == replay.result && f.destination_executions() == 1);
    f.source
        .write(
            leader,
            f.source.definitions[leader].write.clone(),
            Fixture::input(5, 0),
            3,
            Durability::S3,
        )
        .await?;
    f.assert_result(2, 12).await?;
    {
        let requests = f.sink.requests.lock().unwrap_or_else(|e| e.into_inner());
        ensure!(requests
            .iter()
            .all(|r| r.command_id().client == request.command_id().client));
        ensure!(requests.iter().any(|r| r.command_id().sequence() == 2));
    }
    let old = receiver
        .command(contract, request.encoded().to_vec(), options)
        .await?;
    ensure!(
        old.result == replay.result
            && f.destination_executions() == 2
            && f.source_executions() == 2
    );
    f.close().await
}

pub async fn shared_alarm_client() -> Result<()> {
    let f = Fixture::start(false, false).await?;
    let leader = f.source.leader().await?;
    f.source
        .write(
            leader,
            f.source.definitions[leader].write.clone(),
            Fixture::input(7, 3),
            2,
            Durability::S3,
        )
        .await?;
    let source = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let read = f.source.settled(1).await?;
            if i64::from_be_bytes(read.result[..8].try_into()?) == 8 {
                return Ok::<_, anyhow::Error>(read);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    for index in 0..3 {
        ensure!(
            f.source
                .read(index, Some(source.version_token.clone()))
                .await?
                .result
                == source.result
        );
    }
    let destination = f.destination.leader().await?;
    let read = f.destination.read(destination, None).await?;
    ensure!(
        i64::from_be_bytes(read.result.as_slice().try_into()?) == 7
            && f.source_executions() == 1
            && f.destination_executions() == 1
    );
    f.close().await
}
