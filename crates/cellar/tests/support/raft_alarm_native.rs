//! Three actual voters and an owned alarm timer; no cell factory or Fleet wake.
#![allow(clippy::disallowed_methods)]
use super::public_node;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    consensus::*, Application, Node, NodeRaftConfig, NodeRaftListeners, Output, Registry,
    ShutdownMode,
};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

struct Definition {
    application: Arc<dyn Application>,
    schema: [u8; 32],
    prime: OperationContract,
    arm: OperationContract,
    cancel: OperationContract,
    get: OperationContract,
}

fn definition(calls: Arc<AtomicUsize>, gate: Arc<public_node::Gate>) -> Result<Definition> {
    let class=RaftClass::new("Timer",1,[61;32])?
        .alarm("Tick",[62;32],move |context,input| {
            calls.fetch_add(1,Ordering::SeqCst);
            let amount=i64::from_be_bytes(input.try_into()?);
            context.sql().execute("UPDATE timer SET value=value+?1",[amount])?;
            Ok(())
        })?
        .command("Prime",[63;32],|context,_| {
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS timer(value INTEGER); INSERT INTO timer SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM timer)")?;
            Ok(vec![])
        })?
        .command("Arm",[64;32],move |context,input| {
            ensure!(input.len()==17);
            let delay=u64::from_be_bytes(input[..8].try_into()?);
            let due=context.inputs().now_unix_millis.checked_add(delay).context("alarm due overflow")?;
            context.set_alarm("Tick",due,&input[8..16])?;
            if input[16]==1 {gate.wait()?;}
            Ok(vec![])
        })?
        .command("Cancel",[65;32],|context,_| {context.cancel_alarm("Tick")?;Ok(vec![])})?
        .query("Get",[66;32],|context,_| {
            let value:i64=context.query_row("SELECT value FROM timer",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    ensure!(class.contract("__sys_raft_alarm_Tick").is_none());
    let prime = class.contract("Prime").context("prime")?;
    let arm = class.contract("Arm").context("arm")?;
    let cancel = class.contract("Cancel").context("cancel")?;
    let get = class.contract("Get").context("get")?;
    let schema = class.schema_digest();
    let application =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    Ok(Definition {
        application,
        schema,
        prime,
        arm,
        cancel,
        get,
    })
}

struct Fixture {
    nodes: Vec<Option<Node>>,
    services: Vec<Option<RaftCells>>,
    definitions: Vec<Definition>,
    calls: Vec<Arc<AtomicUsize>>,
    registration: RaftCellRegistration,
    gate: Arc<public_node::Gate>,
    entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    _roots: Vec<tempfile::TempDir>,
}

impl Fixture {
    async fn start(hold: bool) -> Result<Self> {
        let (gate, entered) = public_node::Gate::new();
        if !hold {
            gate.release();
        }
        let calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|calls| definition(calls.clone(), gate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            definitions[0].schema,
        );
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Timer".into(),
            birth: BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Timer:one".into(),
                    group: GroupId::new([50; 16])?,
                    incarnation: IncarnationId::new([51; 16])?,
                },
                voters: (1..=3)
                    .map(|id| {
                        Ok(InitialMember {
                            member: MemberId::new(id)?,
                            node: NodeId::new([id as u8; 16])?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                learners: vec![],
            },
        };
        let mut fixture = Self {
            nodes: vec![],
            services: vec![],
            definitions,
            calls,
            registration,
            gate,
            entered,
            _roots: vec![],
        };
        let mut peers = vec![];
        let mut listeners = vec![];
        for index in 0..3 {
            let root = tempfile::tempdir()?;
            let label = format!("alarm-native-{}", index + 1);
            let node = public_node::node_named(
                root.path(),
                fixture.definitions[index].application.clone(),
                &label,
            )
            .await?;
            let bound = NodeRaftListeners {
                control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            };
            peers.push(RaftPeerNode {
                node: NodeId::new([index as u8 + 1; 16])?,
                disk: DiskLineage::new([index as u8 + 20; 16])?,
                process: ProcessGeneration::new([index as u8 + 30; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: bound.control.local_addr()?,
                data_address: bound.data.local_addr()?,
                query_address: bound.query.local_addr()?,
            });
            fixture.nodes.push(Some(node));
            fixture._roots.push(root);
            listeners.push(bound);
        }
        let store = Arc::new(object_store::memory::InMemory::new());
        for (index, listeners) in listeners.into_iter().enumerate() {
            let node = fixture.nodes[index].as_ref().context("node")?;
            let peer = &peers[index];
            let host = RaftHostOptions {
                alarms: Some(RaftAlarmOptions {
                    interval_ms: 20,
                    ..Default::default()
                }),
                ..Default::default()
            };
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
                            prefix: "alarms/native".into(),
                            ..Default::default()
                        },
                        cells: vec![fixture.registration.clone()],
                        peers: peers.clone(),
                        query_origins: vec![],
                        host,
                    },
                    RaftCellsServices {
                        listeners,
                        tls: public_node::tls()?,
                        store: store.clone(),
                        application: fixture.definitions[index].application.clone(),
                        versions: public_node::version_keys()?,
                    },
                )
                .await?;
            fixture.services.push(Some(service));
        }
        let leader = fixture.leader().await?;
        fixture
            .write(leader, fixture.definitions[leader].prime.clone(), vec![], 1)
            .await?;
        Ok(fixture)
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
        tokio::time::timeout(Duration::from_secs(10), async {
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
    ) -> Result<CommandReceipt> {
        let mut options = public_node::options(sequence)?;
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

    fn input(delay: u64, amount: i64, hold: bool) -> Vec<u8> {
        let mut input = delay.to_be_bytes().to_vec();
        input.extend(amount.to_be_bytes());
        input.push(u8::from(hold));
        input
    }

    async fn value(&self, index: usize, minimum: VersionToken) -> Result<i64> {
        let output = self
            .cell(index)?
            .query(
                self.definitions[index].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(minimum),
                    ..Default::default()
                },
            )
            .await?;
        Ok(i64::from_be_bytes(output.result.as_slice().try_into()?))
    }

    fn fired(&self) -> usize {
        self.calls
            .iter()
            .map(|calls| calls.load(Ordering::SeqCst))
            .sum()
    }

    async fn stop(&mut self, index: usize) -> Result<()> {
        if let Some(service) = self.services[index].take() {
            ensure!(service.close().await?.unfinished.is_empty());
        }
        if let Some(node) = self.nodes[index].take() {
            node.shutdown(ShutdownMode::Preserve).await?;
        }
        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        self.gate.release();
        for index in 0..3 {
            self.stop(index).await?;
        }
        Ok(())
    }
}

pub async fn no_traffic_recovery() -> Result<()> {
    let mut fixture = Fixture::start(false).await?;
    let result = async {
        let primary = fixture.leader().await?;
        let arm = fixture
            .write(
                primary,
                fixture.definitions[primary].arm.clone(),
                Fixture::input(5000, 7, false),
                2,
            )
            .await?;
        for index in 0..3 {
            ensure!(fixture.value(index, arm.version_token.clone()).await? == 0);
        }
        fixture.stop(primary).await?;
        // No business/status/management calls until the first result audit.
        tokio::time::sleep(Duration::from_secs(8)).await;
        ensure!(
            fixture.fired() == 1,
            "alarm callback count {}",
            fixture.fired()
        );
        ensure!(
            fixture.calls[primary].load(Ordering::SeqCst) == 0,
            "stopped leader fired"
        );
        for index in 0..3 {
            if fixture.services[index].is_some() {
                ensure!(fixture.value(index, arm.version_token.clone()).await? == 7);
            }
        }
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

pub async fn private_intent() -> Result<()> {
    let mut fixture = Fixture::start(true).await?;
    let result = async {
        let primary = fixture.leader().await?;
        let future = fixture.cell(primary)?.command(
            fixture.definitions[primary].arm.clone(),
            Fixture::input(0, 9, true),
            public_node::options(2)?,
        );
        tokio::time::timeout(Duration::from_secs(5), fixture.entered.recv())
            .await?
            .context("private gate")?;
        tokio::time::sleep(Duration::from_millis(750)).await;
        ensure!(fixture.fired() == 0, "private alarm fired");
        fixture.gate.release();
        let arm = future.await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        ensure!(fixture.fired() == 1);
        for index in 0..3 {
            ensure!(fixture.value(index, arm.version_token.clone()).await? == 9);
        }
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

pub async fn rearm_cancel_and_system_refusal() -> Result<()> {
    let fixture = Fixture::start(false).await?;
    let result = async {
        let primary = fixture.leader().await?;
        fixture
            .write(
                primary,
                fixture.definitions[primary].arm.clone(),
                Fixture::input(1000, 1, false),
                2,
            )
            .await?;
        fixture
            .write(
                primary,
                fixture.definitions[primary].arm.clone(),
                Fixture::input(1000, 9, false),
                3,
            )
            .await?;
        fixture
            .write(
                primary,
                fixture.definitions[primary].cancel.clone(),
                vec![],
                4,
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        ensure!(fixture.fired() == 0);
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-alarm-callback-1");
        hash.update([62; 32]);
        let forbidden = OperationContract {
            operation: "__sys_raft_alarm_Tick".into(),
            kind: OperationKind::Command,
            digest: hash.finalize().into(),
        };
        ensure!(matches!(
            fixture
                .cell(primary)?
                .command(forbidden, vec![], public_node::options(5)?)
                .await,
            Err(CommandFailure::UnsupportedCapability)
        ));
        let arm = fixture
            .write(
                primary,
                fixture.definitions[primary].arm.clone(),
                Fixture::input(0, 5, false),
                5,
            )
            .await?;
        tokio::time::sleep(Duration::from_millis(1500)).await;
        ensure!(fixture.fired() == 1);
        for index in 0..3 {
            ensure!(fixture.value(index, arm.version_token.clone()).await? == 5);
        }
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

pub async fn disabled_scheduler_preflight() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = definition(calls.clone(), gate)?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        definition.schema,
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace,
        class: "Timer".into(),
        birth: BirthProposal {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                cell: "Timer:disabled".into(),
                group: GroupId::new([50; 16])?,
                incarnation: IncarnationId::new([51; 16])?,
            },
            voters: vec![InitialMember {
                member: MemberId::new(1)?,
                node: NodeId::new([1; 16])?,
            }],
            learners: vec![],
        },
    };
    let store = Arc::new(object_store::memory::InMemory::new());
    let mut errors = vec![];
    for variant in 0..4 {
        let mut host = RaftHostOptions::default();
        if variant != 0 {
            host.alarms = Some(RaftAlarmOptions::default());
        }
        if variant == 1 {
            host.max_input_bytes = 1;
        }
        if variant == 2 {
            host.execution.max_client_identities = 1;
        }
        if variant == 3 {
            host.execution.max_retained_receipts = 1;
        }
        let outcome = public_node::cells_with_host(
            &node,
            definition.application.clone(),
            store.clone(),
            registration.clone(),
            false,
            host,
        )
        .await;
        errors.push(match outcome {
            Ok(service) => {
                service.close().await?;
                None
            }
            Err(error) => Some(error),
        });
    }
    let allocated = node.info().data_dir.join("raft").exists();
    node.shutdown(ShutdownMode::Preserve).await?;
    for error in errors {
        let error = error.context("alarm class activated without required scheduler capacity")?;
        ensure!(format!("{error:#}").contains("alarm capability requires"));
    }
    ensure!(!allocated && store.list(None).try_collect::<Vec<_>>().await?.is_empty());
    ensure!(calls.load(Ordering::SeqCst) == 0);
    Ok(())
}

pub async fn node_shutdown_joins_timer() -> Result<()> {
    let mut fixture = Fixture::start(false).await?;
    let result = async {
        let primary = fixture.leader().await?;
        fixture
            .write(
                primary,
                fixture.definitions[primary].arm.clone(),
                Fixture::input(3_600_000, 1, false),
                2,
            )
            .await?;
        let node = fixture.nodes[primary].take().context("node")?;
        let service = fixture.services[primary].take().context("service")?;
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            node.shutdown(ShutdownMode::Preserve),
        )
        .await??;
        ensure!(matches!(outcome, cellar::sdk::NodeOutcome::Stopped(_)));
        ensure!(service.is_closed());
        // Node joins domain cancellation. Unified graceful Raft receipt and
        // archive-obligation reporting remain MR-13's separate contract.
        ensure!(matches!(
            service.close().await,
            Err(CommandFailure::Unavailable)
        ));
        ensure!(fixture.fired() == 0);
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}
