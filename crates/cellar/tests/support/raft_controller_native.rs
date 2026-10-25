//! Six actual nodes. All sources use identical advertised physical endpoints;
//! data cuts leave direct control/inspection connections live.
#![allow(clippy::disallowed_methods)]
use super::{cluster, faults as archive_faults, log_fault, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    consensus::*, Node, NodeOutcome, NodeRaftConfig, NodeRaftListeners, ShutdownMode,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

struct Fixture {
    nodes: Vec<Option<Node>>,
    services: Vec<Option<RaftCells>>,
    peers: Vec<RaftPeerNode>,
    data: Vec<cluster::DataLink>,
    definitions: Vec<public_node::Counter>,
    registrations: Vec<RaftCellRegistration>,
    calls: Vec<Arc<AtomicUsize>>,
    store: Arc<dyn object_store::ObjectStore>,
    _roots: Vec<tempfile::TempDir>,
    _entered: tokio::sync::mpsc::UnboundedReceiver<()>,
}

struct CaptureFixture {
    gates: Vec<Arc<log_fault::Gate>>,
    store: Arc<dyn object_store::ObjectStore>,
}

struct SqlHold {
    store: Arc<archive_faults::Store>,
    reads: Vec<Arc<super::log_fault::Gate>>,
}

struct ArchiveRelease(Arc<archive_faults::Store>);

impl Drop for ArchiveRelease {
    fn drop(&mut self) {
        self.0.archive_hang.store(false, Ordering::Release);
        self.0.archive_resume.notify_waiters();
    }
}

impl Fixture {
    async fn start(groups: usize, balance: bool, draining: Option<usize>) -> Result<Self> {
        Self::start_with_report_limits(
            groups,
            balance,
            draining,
            RaftCloseReportLimits::default(),
            None,
        )
        .await
    }

    async fn start_with_report_limits(
        groups: usize,
        balance: bool,
        draining: Option<usize>,
        report_limits: RaftCloseReportLimits,
        capture: Option<&CaptureFixture>,
    ) -> Result<Self> {
        Self::start_configured(groups, balance, draining, report_limits, None, capture).await
    }

    async fn start_configured(
        groups: usize,
        balance: bool,
        draining: Option<usize>,
        report_limits: RaftCloseReportLimits,
        hold: Option<SqlHold>,
        capture: Option<&CaptureFixture>,
    ) -> Result<Self> {
        ensure!(hold.is_none() || capture.is_none());
        ensure!((1..=16).contains(&groups));
        let (gate, entered) = public_node::Gate::new();
        gate.release();
        let calls: Vec<_> = (0..6).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|calls| {
                public_node::counter_with_blobs(
                    calls.clone(),
                    gate.clone(),
                    hold.is_some() || capture.is_some(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            definitions[0].schema,
        );
        let registrations = (0..groups)
            .map(|group| {
                Ok(RaftCellRegistration {
                    placement: RaftCellPlacement::Replica,
                    namespace: namespace.clone(),
                    class: "Counter".into(),
                    birth: BirthProposal {
                        scope: GroupScope {
                            cluster: namespace.cluster,
                            namespace: namespace.namespace,
                            cell: format!("Counter:automatic-{group}"),
                            group: GroupId::new([70 + group as u8; 16])?,
                            incarnation: IncarnationId::new([3; 16])?,
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
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let store: Arc<dyn object_store::ObjectStore> = match (&hold, capture) {
            (Some(hold), _) => hold.store.clone(),
            (None, Some(capture)) => capture.store.clone(),
            (None, None) => Arc::new(object_store::memory::InMemory::new()),
        };
        let mut fixture = Self {
            nodes: vec![],
            services: vec![],
            peers: vec![],
            data: vec![],
            definitions,
            registrations,
            calls,
            store: store.clone(),
            _roots: vec![],
            _entered: entered,
        };
        let mut listeners = vec![];
        for index in 0..6 {
            let root = tempfile::tempdir()?;
            let label = format!("automatic-native-{}", index + 1);
            let filesystem: Arc<dyn cellar_ltx::FileSystem> = match (&hold, capture) {
                (Some(hold), _) => Arc::new(log_fault::FaultFileSystem {
                    gate: hold.reads[index].clone(),
                }),
                (None, Some(capture)) => Arc::new(log_fault::FaultFileSystem {
                    gate: capture.gates[index].clone(),
                }),
                (None, None) => Arc::new(cellar_ltx::DirectFileSystem),
            };
            let node = public_node::node_declared_with_filesystem(
                root.path(),
                fixture.definitions[index].application.clone(),
                &label,
                vec![],
                filesystem,
            )
            .await?;
            let bound = NodeRaftListeners {
                control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            };
            let data = cluster::DataLink::start(bound.data.local_addr()?).await?;
            fixture.peers.push(RaftPeerNode {
                node: NodeId::new([index as u8 + 1; 16])?,
                disk: DiskLineage::new([index as u8 + 20; 16])?,
                process: ProcessGeneration::new([index as u8 + 30; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: bound.control.local_addr()?,
                data_address: data.address(),
                query_address: bound.query.local_addr()?,
            });
            fixture._roots.push(root);
            fixture.nodes.push(Some(node));
            fixture.data.push(data);
            listeners.push(bound);
        }
        for (index, listeners) in listeners.into_iter().enumerate() {
            let node = fixture.nodes[index].as_ref().context("node")?;
            let peer = &fixture.peers[index];
            let mut host = RaftHostOptions {
                close_report: report_limits,
                ..Default::default()
            };
            if hold.is_some() {
                host.blocking_workers = 1;
                host.archive.max_batch_groups = 1;
            }
            let reservation = host.images.max_bytes;
            host.node_profile = Some(RaftNodeProfile {
                pool: "native".into(),
                region: "local".into(),
                zone: format!("zone-{}", index % 3),
                group_capacity: groups as u32,
                byte_capacity: reservation * groups as u64,
                bytes_per_group: reservation,
                leader_capacity: groups as u32,
                draining: draining == Some(index),
            });
            host.node_directory = Some(RaftNodeDirectoryOptions {
                poll_ms: 20,
                timeout_ms: 5000,
                batch_nodes: 6,
            });
            host.inspection_scan = RaftInspectionScanOptions {
                interval_ms: 20,
                groups_per_batch: 2,
                ..Default::default()
            };
            host.inspection_origins = fixture
                .peers
                .iter()
                .enumerate()
                .filter(|(source, _)| *source != index)
                .map(|(_, peer)| NamespaceInspectionOrigin {
                    cluster: namespace.cluster,
                    namespace: namespace.namespace,
                    revision: 1,
                    peer: peer.clone(),
                })
                .collect();
            let mut controller = RaftControllerOptions::new(PlacementPolicy {
                revision: 1,
                pool: "native".into(),
                distinct_zones: true,
                distinct_regions: false,
                max_sample_age_ms: 5000,
                required_bytes: reservation,
            });
            controller.start_paused = true;
            controller.unavailable_after_ms = 500;
            if balance {
                controller.leadership = Some(LeaderPolicy {
                    max_sample_age_ms: 5000,
                    min_transfer_interval_ms: 500,
                    deadband_basis_points: 500,
                });
            }
            host.controller = Some(controller);
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
                            prefix: "automatic/native".into(),
                            ..Default::default()
                        },
                        cells: fixture
                            .registrations
                            .iter()
                            .cloned()
                            .map(|mut registration| {
                                if index >= 3 {
                                    registration.placement = RaftCellPlacement::Standby;
                                }
                                registration
                            })
                            .collect(),
                        peers: fixture.peers.clone(),
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
        Ok(fixture)
    }

    fn service(&self, node: usize) -> Result<&RaftCells> {
        self.services[node].as_ref().context("stopped service")
    }

    fn cell(&self, node: usize, group: usize) -> Result<RaftCellHandle> {
        let registration = &self.registrations[group];
        Ok(self.service(node)?.cell(
            registration.namespace.namespace,
            &registration.birth.scope.cell,
        )?)
    }

    async fn leader(&self, group: usize) -> Result<usize> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for index in 0..6 {
                    if let Ok(cell) = self.cell(index, group) {
                        if cell.status().await.is_ok_and(|status| status.command_ready) {
                            return Ok(index);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?
    }

    async fn enable(&self, enabled: bool) -> Result<()> {
        for service in self.services.iter().flatten() {
            assert_eq!(
                service.set_controller_enabled(enabled).await?.enabled,
                enabled
            );
        }
        Ok(())
    }

    async fn write(&self, group: usize, id: u64, value: i64) -> Result<CommandReceipt> {
        let primary = self.leader(group).await?;
        Ok(self
            .cell(primary, group)?
            .command(
                self.definitions[primary].increment.clone(),
                value.to_be_bytes().to_vec(),
                public_node::options(id)?,
            )
            .await?)
    }

    async fn stop(&mut self, index: usize) -> Result<()> {
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            self.nodes[index]
                .take()
                .context("node")?
                .shutdown(ShutdownMode::Preserve),
        )
        .await
        .context("node shutdown did not join")??;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("node did not stop gracefully: {outcome:?}");
        };
        ensure!(
            report.ingress_drained
                && report.application_work_drained
                && report.cells_drained
                && report.log_quiesced
                && report.local_durability_closed
                && !report.drain_timed_out
                && !report.deadline_elapsed,
            "incomplete node drain: {report:?}"
        );
        let raft = report.raft.context("missing Raft stop report")?;
        ensure!(!raft.startup_incomplete);
        let closed = raft.result?;
        let service = self.services[index].take().context("service")?;
        ensure!(service.is_closed());
        ensure!(service.close().await? == closed);
        Ok(())
    }

    async fn close(mut self) -> Result<()> {
        for index in 0..6 {
            if self.nodes[index].is_some() {
                self.stop(index).await?;
            }
        }
        for link in &mut self.data {
            link.close().await?;
        }
        Ok(())
    }
}

pub async fn no_traffic_repair() -> Result<()> {
    let mut fixture = Fixture::start(1, false, None).await?;
    let primary = fixture.leader(0).await?;
    fixture.enable(true).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture
            .service(primary)?
            .controller_status()
            .await?
            .probe_rounds
            == 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    fixture.enable(false).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(usage) = fixture.service(primary)?.resource_usage().controller {
                if !usage.enabled && usage.probe_rounds > 0 {
                    ensure!(
                        usage.metadata.max_bytes > 0
                            && usage.metadata.bytes <= usage.metadata.max_bytes
                    );
                    let text = fixture.service(primary)?.resource_usage().prometheus();
                    ensure!(
                        text.contains("cellar_raft_controller_sample_available 1\n")
                            && text.contains("cellar_raft_controller_enabled 0\n")
                    );
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let before = fixture
        .service(primary)?
        .controller_status()
        .await?
        .probe_rounds;
    let receipt = fixture.write(0, 1, 7).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        fixture
            .service(primary)?
            .controller_status()
            .await?
            .probe_rounds,
        before
    );
    fixture.enable(true).await?;
    let victim = (primary + 1) % 3;
    fixture.stop(victim).await?;
    // No client business, status, placement or repair calls occur in this window.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let completed = fixture
        .cell(primary, 0)?
        .repair_status()
        .await?
        .context("timer created no repair plan")?;
    assert_eq!(
        completed.phase,
        RepairPhase::Complete,
        "first administrative observation must already see completion"
    );
    assert_eq!(completed.plan.obsolete, MemberId::new(victim as u64 + 1)?);
    assert_eq!(completed.plan.replacement.member, MemberId::new(4)?);
    let replacement = victim + 3;
    assert_eq!(
        completed.plan.replacement.node,
        fixture.peers[replacement].node
    );
    assert_eq!(
        fixture
            .cell(replacement, 0)?
            .query(
                fixture.definitions[replacement].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token),
                    ..Default::default()
                }
            )
            .await?
            .result,
        7_i64.to_be_bytes()
    );
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        1
    );
    let performance = fixture.service(primary)?.performance();
    ensure!(performance.available && !performance.saturated);
    for name in ["controller_probe", "controller_action"] {
        let operation = performance
            .operations
            .iter()
            .find(|operation| operation.name == name)
            .context("controller measurements")?;
        ensure!(operation.started > 0 && operation.succeeded > 0 && operation.count > 0);
    }
    fixture.close().await
}

pub async fn shutdown_handoff() -> Result<()> {
    shutdown_handoff_scenario(None, false).await
}

// The membership target shares this fixture but does not run shutdown cases.
#[allow(dead_code)]
pub async fn shutdown_handoff_during_archive_capture(
    store: Arc<dyn object_store::ObjectStore>,
    exhaust_group_budget: bool,
) -> Result<()> {
    let capture = CaptureFixture {
        gates: (0..6).map(|_| log_fault::Gate::new(None)).collect(),
        store,
    };
    shutdown_handoff_scenario(Some(capture), exhaust_group_budget).await
}

async fn shutdown_handoff_scenario(
    capture: Option<CaptureFixture>,
    exhaust_group_budget: bool,
) -> Result<()> {
    let mut fixture = Fixture::start_with_report_limits(
        1,
        false,
        None,
        RaftCloseReportLimits::default(),
        capture.as_ref(),
    )
    .await?;
    let mut held = None;
    let mut shutdown = None;
    let result = async {
        fixture.enable(false).await?;
        let receipt = fixture.write(0, 1, 7).await?;
        let primary = fixture.leader(0).await?;
        for index in 0..3 {
            let read = fixture
                .cell(index, 0)?
                .query(
                    fixture.definitions[index].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(read.result == 7i64.to_be_bytes());
        }
        if let Some(capture) = &capture {
            // All application calls have completed. An unavailable archive
            // keeps retrying this cut; the Blob-enabled class opens the actual
            // immutable SQL image while capturing its dependency closure.
            held = Some(capture.gates[primary].arm(log_fault::HOLD_IMAGE_READ));
            tokio::time::timeout(
                Duration::from_secs(5),
                capture.gates[primary].entered.notified(),
            )
            .await
            .context("archive capture did not enter the physical reader")?;
        }
        let node = fixture.nodes[primary].take().context("node")?;
        shutdown = Some(tokio::spawn(async move {
            node.shutdown(ShutdownMode::Preserve).await
        }));
        if held.is_some() {
            let service = fixture.service(primary)?;
            tokio::time::timeout(Duration::from_secs(3), async {
                while !service.is_closed() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .context("shutdown did not seal admission")?;
            if exhaust_group_budget {
                tokio::time::timeout(Duration::from_secs(3), async {
                    while !service
                        .resource_usage()
                        .sql
                        .is_some_and(|sql| sql.queue_closed)
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .context("busy group extended the bounded handoff turn")?;
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                ensure!(
                    service
                        .resource_usage()
                        .sql
                        .is_some_and(|sql| !sql.queue_closed),
                    "drain skipped a temporary archive capture"
                );
            }
            ensure!(
                !shutdown.as_ref().context("shutdown")?.is_finished(),
                "shutdown did not join its physical reader"
            );
            drop(held.take());
        }
        let observed = tokio::time::timeout(
            Duration::from_secs(10),
            shutdown.as_mut().context("shutdown")?,
        )
        .await?;
        shutdown.take();
        let outcome = observed??;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected stop: {outcome:?}")
        };
        let raft = report.raft.context("missing Raft report")?;
        let handoff = raft.drain.context("missing handoff result")??;
        if exhaust_group_budget {
            ensure!(
                handoff.groups_examined == 1
                    && handoff.handoff_attempts == 0
                    && handoff.handoffs_accepted == 0
                    && handoff.leadership_released == 0
                    && handoff.unavailable_groups == 1
                    && !handoff.deadline_elapsed,
                "busy handoff did not keep its original group budget: {handoff:?}"
            );
        } else {
            ensure!(
                handoff.groups_examined == 1
                    && handoff.handoff_attempts == 1
                    && handoff.handoffs_accepted == 1
                    && handoff.leadership_released == 1
                    && handoff.unavailable_groups == 0
                    && !handoff.deadline_elapsed,
                "handoff did not complete: {handoff:?}"
            );
        }
        let closed = raft.result?;
        ensure!(closed.unfinished.is_empty());
        ensure!(
            fixture.services[primary]
                .take()
                .context("service")?
                .close()
                .await?
                == closed
        );
        let next = fixture.leader(0).await?;
        ensure!(next != primary);
        let next = fixture.write(0, 2, 3).await?;
        ensure!(next.result == 10i64.to_be_bytes());
        Ok(())
    }
    .await;
    drop(held);
    if let Some(shutdown) = shutdown {
        shutdown.await??;
    }
    fixture.close().await?;
    result
}

/// Hold the published image read inside the real SQL archive-capture worker.
/// No accepted application command or query can delay Node's application drain.
#[allow(dead_code)] // Also compiled by the membership/controller fixture target.
pub async fn shutdown_handoff_with_busy_sql(expire: bool) -> Result<()> {
    let reads: Vec<_> = (0..6).map(|_| super::log_fault::Gate::new(None)).collect();
    let mut release_read = None;
    let store = Arc::new(archive_faults::Store::default());
    store.archive_hang.store(true, Ordering::Release);
    let mut release_archive = Some(ArchiveRelease(store.clone()));
    let mut fixture = Fixture::start_configured(
        1,
        false,
        None,
        RaftCloseReportLimits::default(),
        Some(SqlHold {
            store: store.clone(),
            reads: reads.clone(),
        }),
        None,
    )
    .await?;
    let result = async {
        fixture.enable(false).await?;
        let primary = fixture.leader(0).await?;
        fixture.write(0, 1, 4).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.archive_attempts.load(Ordering::Acquire) < 3 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("all voter archive producers entered object I/O")?;
        let scope = &fixture.registrations[0].birth.scope;
        let images = fixture.nodes[primary]
            .as_ref()
            .context("node")?
            .info()
            .data_dir
            .join("raft-cells")
            .join(hex::encode(scope.namespace.bytes()))
            .join(hex::encode(scope.group.bytes()))
            .join(hex::encode(scope.incarnation.bytes()))
            .join("images");
        let image_paths = || -> Result<std::collections::BTreeSet<std::path::PathBuf>> {
            Ok(std::fs::read_dir(&images)?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<std::io::Result<Vec<_>>>()?
                .into_iter()
                .filter(|path| path.extension().is_some_and(|ext| ext == "sqlite"))
                .collect())
        };
        let before = image_paths()?;
        let receipt = fixture.write(0, 2, 3).await?;
        for index in 0..3 {
            let read = fixture
                .cell(index, 0)?
                .query(
                    fixture.definitions[index].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(read.result == 7i64.to_be_bytes());
        }
        let after = image_paths()?;
        let added: Vec<_> = after.difference(&before).cloned().collect();
        // Committed apply reuses the exactly matched sealed leader preview.
        // It did not belong to the first cut, which must finish before capture.
        ensure!(added.len() == 1, "expected one reused committed preview");
        *reads[primary].image_read_paths.lock().unwrap() = added;
        release_read = Some(reads[primary].arm(super::log_fault::HOLD_IMAGE_READ));
        drop(release_archive.take());
        tokio::time::timeout(Duration::from_secs(5), reads[primary].entered.notified())
            .await
            .context("archive capture did not enter physical image read")?;
        let service = fixture.service(primary)?.clone();
        let performance = service.performance();
        let capture = performance
            .operations
            .iter()
            .find(|op| op.name == "sql_archive_capture")
            .context("archive capture observation")?;
        ensure!(
            capture.started > capture.succeeded + capture.failed + capture.abandoned,
            "held read is not owned by the SQL archive capture"
        );
        let started = tokio::time::Instant::now();
        let stop = tokio::spawn(
            fixture.nodes[primary]
                .take()
                .context("node")?
                .shutdown(ShutdownMode::Preserve),
        );
        let observed_close = if expire {
            tokio::time::timeout(Duration::from_secs(3), async {
                while !service
                    .resource_usage()
                    .sql
                    .is_some_and(|sql| sql.queue_closed)
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .is_ok()
        } else {
            tokio::time::sleep(Duration::from_millis(600)).await;
            service
                .resource_usage()
                .sql
                .is_some_and(|sql| sql.queue_closed)
        };
        let elapsed = started.elapsed();
        // Release the physical read and join shutdown before asserting, including
        // the pre-fix early-close regression. Waiter cancellation is not a join.
        drop(release_read.take());
        let outcome = tokio::time::timeout(Duration::from_secs(15), stop).await???;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected stop: {outcome:?}")
        };
        let raft = report.raft.context("missing Raft report")?;
        let drain = raft.drain.context("missing handoff result")??;
        ensure!(
            drain.groups_examined == 1 && !drain.deadline_elapsed,
            "unexpected drain: {drain:?}"
        );
        if expire {
            ensure!(
                observed_close && elapsed >= Duration::from_millis(900),
                "SQL wait did not retain its bounded turn: {elapsed:?}, {drain:?}"
            );
            ensure!(
                drain.handoff_attempts == 0
                    && drain.handoffs_accepted == 0
                    && drain.leadership_released == 0
                    && drain.unavailable_groups == 1,
                "stalled SQL invented a handoff: {drain:?}"
            );
        } else {
            ensure!(!observed_close, "busy SQL caused early close: {drain:?}");
            ensure!(
                drain.handoff_attempts == 1
                    && drain.handoffs_accepted == 1
                    && drain.leadership_released == 1
                    && drain.unavailable_groups == 0,
                "handoff after physical SQL release failed: {drain:?}"
            );
        }
        let closed = raft.result?;
        ensure!(closed.unfinished.is_empty() && service.close().await? == closed);
        fixture.services[primary].take();
        let successor = fixture.leader(0).await?;
        ensure!(successor != primary);
        ensure!(fixture.write(0, 3, 3).await?.result == 10i64.to_be_bytes());
        Ok(())
    }
    .await;
    drop(release_archive);
    drop(release_read);
    fixture.close().await?;
    result
}

pub async fn bounded_shutdown_report() -> Result<()> {
    // Keep all four cold histories genuinely behind their archive cut. Empty
    // configurations now archive normally without a SQL callback.
    let store = Arc::new(archive_faults::Store::default());
    store.archive_unavailable.store(true, Ordering::Release);
    let capture = CaptureFixture {
        gates: (0..6).map(|_| log_fault::Gate::new(None)).collect(),
        store,
    };
    let mut fixture = Fixture::start_with_report_limits(
        4,
        false,
        None,
        RaftCloseReportLimits {
            max_groups: 1,
            max_bytes: 4096,
        },
        Some(&capture),
    )
    .await?;
    let result = async {
        for group in 0..4 {
            fixture.leader(group).await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if fixture.cell(0, group)?.status().await?.applied.index.get() > 0 {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
        }
        let outcome = fixture.nodes[0]
            .take()
            .context("node")?
            .shutdown(ShutdownMode::Preserve)
            .await?;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected stop: {outcome:?}")
        };
        let closed = report.raft.context("missing Raft report")?.result?;
        ensure!(closed.groups_total == 4 && closed.groups.len() == 1 && closed.omitted_groups == 3);
        // The refused archive leaves durable protocol progress uncovered.
        // Count every omitted history while retaining zero SQL callbacks.
        ensure!(closed.groups_with_archive_lag == 4 && closed.unfinished.is_empty());
        ensure!(fixture
            .calls
            .iter()
            .all(|calls| calls.load(Ordering::Acquire) == 0));
        ensure!(
            fixture.services[0]
                .take()
                .context("service")?
                .close()
                .await?
                == closed
        );
        Ok(())
    }
    .await;
    fixture.close().await?;
    result
}

pub async fn empty_cell_repair() -> Result<()> {
    let mut fixture = Fixture::start(1, false, None).await?;
    let primary = fixture.leader(0).await?;
    fixture.enable(true).await?;
    let victim = (primary + 1) % 3;
    fixture.stop(victim).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let completed = fixture
        .cell(primary, 0)?
        .repair_status()
        .await?
        .context("empty history was not repaired")?;
    assert_eq!(completed.phase, RepairPhase::Complete);
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        0
    );
    let receipt = fixture.write(0, 1, 5).await?;
    let replacement = victim + 3;
    assert_eq!(
        fixture
            .cell(replacement, 0)?
            .query(
                fixture.definitions[replacement].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token),
                    ..Default::default()
                }
            )
            .await?
            .result,
        5_i64.to_be_bytes()
    );
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        1
    );
    fixture.close().await
}

async fn force_leader(fixture: &Fixture, group: usize, destination: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let primary = fixture.leader(group).await?;
        if primary == destination {
            return Ok(());
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "leadership preparation deadline"
        );
        let status = fixture.cell(primary, group)?.status().await?;
        let result = fixture
            .cell(primary, group)?
            .transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(destination as u64 + 1)?,
                    expected_term: status.term,
                    expected_revision: status.membership_revision,
                },
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await;
        match result {
            Ok(())
            | Err(
                CommandFailure::Overloaded
                | CommandFailure::NotLeader { .. }
                | CommandFailure::CommandConflict,
            ) => (),
            Err(error) => return Err(error.into()),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn balance_with_lag() -> Result<()> {
    let fixture = Fixture::start(8, true, None).await?;
    for group in 0..8 {
        let receipt = fixture.write(group, 1, 1).await?;
        // Establish the old published image before isolating the lagging voter.
        for reader in 0..3 {
            assert_eq!(
                fixture
                    .cell(reader, group)?
                    .query(
                        fixture.definitions[reader].get.clone(),
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                            ..Default::default()
                        }
                    )
                    .await?
                    .result,
                1_i64.to_be_bytes()
            );
        }
        force_leader(&fixture, group, 0).await?;
    }
    fixture.data[2].block(true).await?;
    let mut receipts = vec![];
    for group in 0..8 {
        assert_eq!(fixture.leader(group).await?, 0);
        let receipt = fixture.write(group, 2, 1).await?;
        assert_eq!(
            fixture
                .cell(1, group)?
                .query(
                    fixture.definitions[1].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        ..Default::default()
                    }
                )
                .await?
                .result,
            2_i64.to_be_bytes()
        );
        let lagging = fixture
            .cell(0, group)?
            .inspect_peer(fixture.peers[2].node, Duration::from_secs(1))
            .await?;
        ensure!(
            lagging
                .member
                .as_ref()
                .context("lagging voter metadata")?
                .published
                .is_some_and(|cut| cut.index < receipt.version.position.index),
            "data hold failed to keep the voter behind while control inspection remained live"
        );
        receipts.push(receipt);
    }
    let began = tokio::time::Instant::now();
    fixture.enable(true).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut leaders = [0; 6];
    for group in 0..8 {
        leaders[fixture.leader(group).await?] += 1;
    }
    ensure!(
        leaders[1] > 0,
        "automatic balancing did not select the caught-up voter"
    );
    assert_eq!(leaders[2], 0, "a data-lagging voter became a leader");
    for service in fixture.services.iter().flatten() {
        let status = service.controller_status().await?;
        let maximum = began.elapsed().as_millis() / 500 + 1;
        ensure!(
            u128::from(status.transfer_attempts) <= maximum,
            "transfer attempts exceeded the node-wide cooldown"
        );
    }
    fixture.data[2].block(false).await?;
    fixture.enable(false).await?;
    for (group, receipt) in receipts.into_iter().enumerate() {
        for reader in 0..3 {
            assert_eq!(
                fixture
                    .cell(reader, group)?
                    .query(
                        fixture.definitions[reader].get.clone(),
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                            ..Default::default()
                        }
                    )
                    .await?
                    .result,
                2_i64.to_be_bytes()
            );
        }
    }
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        16
    );
    fixture.close().await
}

async fn claim_count(fixture: &Fixture) -> Result<usize> {
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStore as _;
    Ok(fixture
        .store
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .iter()
        .filter(|object| object.location.as_ref().contains("/claims/"))
        .count())
}

pub async fn minority_refused() -> Result<()> {
    let mut fixture = Fixture::start(1, false, None).await?;
    let primary = fixture.leader(0).await?;
    fixture.write(0, 1, 3).await?;
    let claims = claim_count(&fixture).await?;
    ensure!(claims > 0, "fixture created no durable member claims");
    // Establish the minority while controllers are paused, before allowing
    // any automatic proposal or a fresh seed authorization attempt.
    for index in 0..3 {
        if index != primary {
            fixture.stop(index).await?;
        }
    }
    fixture.enable(true).await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        claim_count(&fixture).await?,
        claims,
        "reachable object storage authorized a new member without quorum"
    );
    assert!(fixture.cell(primary, 0)?.repair_status().await?.is_none());
    for index in 3..6 {
        let view = fixture.cell(index, 0)?.inspect().await?;
        assert_eq!(view.occupancy, ReplicaOccupancy::Standby);
        assert_eq!(view.node.groups, 0);
    }
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        1
    );
    fixture.close().await
}

pub async fn plan_recovery() -> Result<()> {
    let mut fixture = Fixture::start(1, false, Some(2)).await?;
    let receipt = fixture
        .write(0, 1, 9)
        .await
        .context("repair recovery setup command")?;
    assert_eq!(
        fixture
            .cell(0, 0)?
            .query(
                fixture.definitions[0].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    ..Default::default()
                }
            )
            .await?
            .result,
        9_i64.to_be_bytes()
    );
    force_leader(&fixture, 0, 0)
        .await
        .context("repair recovery leader placement")?;
    fixture.data[5].block(true).await?;
    fixture.enable(true).await?;
    let initial = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(plan) = fixture.cell(0, 0)?.repair_status().await? {
                if plan.phase == RepairPhase::Seeding {
                    return Ok::<_, anyhow::Error>(plan);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("repair recovery initial seeding deadline")??;
    assert_eq!(initial.plan.obsolete, MemberId::new(3)?);
    assert_eq!(initial.plan.replacement.member, MemberId::new(4)?);
    assert_eq!(initial.plan.replacement.node, fixture.peers[5].node);
    fixture.service(0)?.set_controller_enabled(false).await?;
    fixture.service(0)?.set_controller_enabled(true).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let retried = fixture
        .cell(0, 0)?
        .repair_status()
        .await?
        .context("controller lost the committed plan")?;
    assert_eq!(retried.plan, initial.plan);
    assert_eq!(retried.phase, RepairPhase::Seeding);
    fixture
        .stop(0)
        .await
        .context("repair recovery old leader shutdown")?;
    let successor = fixture
        .leader(0)
        .await
        .context("repair recovery successor election")?;
    let recovered = fixture
        .cell(successor, 0)?
        .repair_status()
        .await?
        .context("new leader lost the committed plan")?;
    assert_eq!(recovered.plan, initial.plan);
    assert_eq!(
        recovered.phase,
        RepairPhase::Seeding,
        "uncaught learner was promoted during leadership loss"
    );
    fixture.data[5].block(false).await?;
    // The next repair may legitimately replace the now-dead old leader too.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let primary = fixture
        .leader(0)
        .await
        .context("repair recovery completed-plan leader")?;
    let final_plan = fixture
        .cell(primary, 0)?
        .repair_status()
        .await?
        .context("missing completed repair")?;
    assert_eq!(final_plan.phase, RepairPhase::Complete);
    assert_eq!(
        fixture.cell(primary, 0)?.status().await?.member_high_water,
        5
    );
    let progress = fixture.cell(primary, 0)?.replication_status().await?;
    let voters = progress
        .members
        .iter()
        .filter(|member| member.voter)
        .map(|member| member.member.get())
        .collect::<Vec<_>>();
    assert_eq!(voters, vec![2, 4, 5]);
    for reader in [1, 3, 5] {
        assert_eq!(
            fixture
                .cell(reader, 0)?
                .query(
                    fixture.definitions[reader].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        ..Default::default()
                    }
                )
                .await?
                .result,
            9_i64.to_be_bytes()
        );
    }
    assert_eq!(
        fixture
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        1
    );
    fixture.close().await
}
