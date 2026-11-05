// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The Node-owned failure service drives shared-group repair and terminal S3
//! verification. Platform/occupancy assertions are fixtures, not Docker fences.
use super::*;
use cellar::application::consensus::verification::cell_groups::RangeDirectory;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex,
    },
};

#[path = "cell_group_managed_recovery_archive.rs"]
mod archive;

type CatalogResult<T> = std::result::Result<T, FleetCatalogError>;

struct Callback(Arc<AtomicUsize>);

impl Drop for Callback {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Platform {
    catalog: FleetCatalog,
    directory: RangeDirectory,
    boot: FleetBootBinding,
    stopped: RaftCells,
    hold: AtomicBool,
    active: Arc<AtomicUsize>,
    entered: tokio::sync::Notify,
    contexts: Mutex<Vec<FleetFailureRecoveryContext>>,
}

impl FleetFailureRecoveryPlatform for Platform {
    fn namespaces<'a>(
        &'a self,
        _: Duration,
    ) -> Pin<Box<dyn Future<Output = CatalogResult<Vec<RangeDirectory>>> + Send + 'a>> {
        Box::pin(async move { Ok(vec![self.directory.clone()]) })
    }

    fn isolate<'a>(
        &'a self,
        context: &'a FleetFailureRecoveryContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = CatalogResult<FleetIsolationAttestation>> + Send + 'a>> {
        Box::pin(async move {
            if context.boot() != &self.boot || !self.stopped.is_closed() {
                return Err(FleetCatalogError::Invalid(
                    "fixture isolation boot not joined",
                ));
            }
            let cut = self.catalog.read(timeout).await?;
            let admission = self
                .catalog
                .evacuation_admission(&cut, context.operation(), timeout)
                .await?;
            if admission.node().bytes() != self.boot.node {
                return Err(FleetCatalogError::Invalid(
                    "fixture isolation before exclusion",
                ));
            }
            {
                let mut contexts = self.contexts.lock().unwrap();
                if contexts.last().is_some_and(|old| old != context) {
                    return Err(FleetCatalogError::Conflict);
                }
                contexts.push(context.clone());
            }
            self.active.fetch_add(1, Ordering::AcqRel);
            let _callback = Callback(self.active.clone());
            self.entered.notify_one();
            while self.hold.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok(FleetIsolationAttestation {
                boot: self.boot.clone(),
                evidence_digest: [92; 32],
                evidence_reference: "fixture/complete-node-joined-and-restart-disabled".into(),
            })
        })
    }
}

struct Inputs {
    hosts: Vec<RaftCells>,
    directory: RangeDirectory,
    history: GroupHistory,
    failed: FleetBootBinding,
    calls: AtomicUsize,
}

fn occupancy(view: RaftHostObservation) -> CatalogResult<FleetResourceAttestation> {
    Ok(FleetResourceAttestation {
        boot: view.fleet_boot.ok_or(FleetCatalogError::Unconfirmed)?,
        profile_revision: 1,
        used: FleetResourceBounds {
            engine_slots: view.node.groups,
            durable_bytes: 1024 * 1024,
            memory_bytes: 64 * 1024 * 1024,
        },
        evidence_digest: [73; 32],
        evidence_reference: "fixture-only/automatic-group-repair-occupancy".into(),
    })
}

impl FleetIsolatedRepairEvidence for Inputs {
    fn refresh<'a>(
        &'a self,
        context: &'a FleetIsolatedRepairContext,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = CatalogResult<FleetIsolatedRepairInputs>> + Send + 'a>> {
        Box::pin(async move {
            if context.group != self.history || context.failed_boot != self.failed {
                return Err(FleetCatalogError::Conflict);
            }
            self.calls.fetch_add(1, Ordering::AcqRel);
            let mut nodes = Vec::new();
            let mut bytes = 0;
            for host in &self.hosts {
                let view = host
                    .observe_node(timeout)
                    .await
                    .map_err(|_| FleetCatalogError::Unconfirmed)?;
                bytes = bytes.max(view.node.profile.bytes_per_group);
                nodes.push(FleetTopologyNodeResources {
                    attestation: occupancy(view)?,
                    reserve: FleetResourceBounds {
                        engine_slots: 0,
                        durable_bytes: 0,
                        memory_bytes: 8 * 1024 * 1024,
                    },
                });
            }
            let bounds = FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: bytes,
                memory_bytes: 8 * 1024 * 1024,
            };
            Ok(FleetIsolatedRepairInputs {
                history: FleetTopologyHistoryResources {
                    group: self.history,
                    resident: bounds,
                    peak: bounds,
                },
                nodes,
                namespaces: vec![self.directory.clone()],
            })
        })
    }
}

impl FleetMovementEvidence for Inputs {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = CatalogResult<FleetMovementExecutionRequest>> + Send + 'a>>
    {
        Box::pin(async move {
            let host = self
                .hosts
                .iter()
                .find(|host| host.identity().node == movement.plan.replacement.node)
                .ok_or(FleetCatalogError::InventoryIncomplete)?;
            Ok(FleetMovementExecutionRequest {
                reservation: movement.reservation,
                attestation: occupancy(
                    host.observe_node(timeout)
                        .await
                        .map_err(|_| FleetCatalogError::Unconfirmed)?,
                )?,
                from_s3: true,
            })
        })
    }
}

fn options() -> FleetMovementServiceOptions {
    FleetMovementServiceOptions {
        interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(500),
        turn_timeout: TIMEOUT,
    }
}

async fn start(
    catalog: &FleetCatalog,
    observer: &RaftCells,
    inputs: &Arc<Inputs>,
    platform: &Arc<Platform>,
    verifier: &FleetGroupArchiveVerifier,
) -> Result<FleetMovementService> {
    Ok(catalog
        .start_failure_recovery_service(
            observer.clone(),
            inputs.clone(),
            inputs.clone(),
            platform.clone(),
            options(),
            FleetFailureRecoveryOptions {
                detection: FleetFailureDetectionOptions::default(),
                archive: verifier.clone(),
            },
        )
        .await?)
}

async fn wait(
    service: &FleetMovementService,
    ready: impl Fn(&FleetMovementServiceStatus) -> bool,
) -> Result<()> {
    // Keep the existing automatic fleet fixture's sixteen five-second round
    // envelope. Individual calls, failure grace and service turns stay bounded.
    tokio::time::timeout(TIMEOUT * 16, async {
        loop {
            let status = service.status();
            if ready(&status) {
                return Ok::<_, anyhow::Error>(());
            }
            ensure!(status.running, "failure recovery stopped: {status:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("automatic group recovery deadline: {:?}", service.status()))??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_failure_recovery_restarts_and_verifies_s3_before_removal() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::fleet_catalog::resources::execution::service::worker=debug")
        .with_test_writer()
        .try_init();
    for rooted in [false, true] {
        Box::pin(recover(rooted, Arc::new(fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_failure_recovery_restarts_and_verifies_s3_before_removal() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-failure-recovery",
            rooted,
        )?));
        Box::pin(recover(rooted, store)).await?;
    }
    Ok(())
}

async fn recover(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let mut stopped_index = None;
    let mut service = None;
    let replay_root = tempfile::tempdir()?;
    let work = async {
        let seed = seed_cells(&cluster.hosts, cluster.history).await?;
        discover(&cluster).await?;
        let index = leader(&cluster.hosts[..3], cluster.history).await?;
        let obsolete = (index + 1) % 3;
        let observer = &cluster.hosts[index];
        // Establish an explicit complete S3 cut before failing a voter; do not
        // execute new business mutations to drive the subsequent repair.
        for (i, original) in seed.receipts.iter().enumerate() {
            let handle = observer.cell(seed.cells[i].clone())?;
            let mut options = CommandOptions::new(original.id, seed.retry);
            options.durability = Durability::S3;
            let receipt = handle
                .command(
                    &handle.contract("Add")?,
                    [7i64, 11][i].to_be_bytes().to_vec(),
                    options,
                )
                .await?;
            ensure!(
                receipt.result == original.result
                    && receipt.version == original.version
                    && receipt.durability == Durability::S3
            );
        }
        let boot = cluster.hosts[obsolete]
            .observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("failed boot")?;
        let node = cluster.nodes.remove(obsolete);
        stopped_index = Some(obsolete);
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        let closure = cluster.hosts[obsolete].close().await?;
        ensure!(
            closure.groups_total == 1
                && closure.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
        let disk = cluster.roots[obsolete].path().join("raft");
        let retained = isolation::disk_digest(&disk)?;
        let f = &cluster.fixtures[0];
        let independent = Arc::new(fault::Store::around(f.store.clone()));
        let namespace = f
            .domain
            .run(
                Catalog::new(
                    independent.clone(),
                    CatalogOptions {
                        prefix: "managed/".into(),
                        ..Default::default()
                    },
                )?
                .existing_namespace(f.declaration.namespace.clone()),
            )
            .await??;
        let verifier_directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
        let verifier = archive::verifier(f, replay_root.path(), rooted).await?;
        independent
            .deny_group_payload_reads
            .store(true, Ordering::Release);
        let platform = Arc::new(Platform {
            catalog: f.catalog.clone(),
            directory: verifier_directory,
            boot: boot.clone(),
            stopped: cluster.hosts[obsolete].clone(),
            hold: AtomicBool::new(true),
            active: Arc::new(AtomicUsize::new(0)),
            entered: tokio::sync::Notify::new(),
            contexts: Mutex::new(Vec::new()),
        });
        let inputs = Arc::new(Inputs {
            hosts: cluster
                .hosts
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != obsolete)
                .map(|(_, host)| host.clone())
                .collect(),
            directory: cluster.directory.clone(),
            history: cluster.history,
            failed: boot.clone(),
            calls: AtomicUsize::new(0),
        });
        let baseline = observer.resource_usage().control;
        service = Some(start(&f.catalog, observer, &inputs, &platform, &verifier).await?);
        tokio::time::timeout(TIMEOUT * 4, platform.entered.notified())
            .await
            .context("failure detector did not enter rooted platform isolation")?;
        let context = platform
            .contexts
            .lock()
            .unwrap()
            .last()
            .cloned()
            .context("rooted failure context")?;
        ensure!(context.boot() == &boot && platform.active.load(Ordering::Acquire) == 1);
        let charged = observer.resource_usage().control;
        ensure!(
            charged.items == baseline.items + 1
                && charged.bytes > baseline.bytes
                && charged.bytes <= charged.max_bytes
                && charged.max_items == baseline.max_items
                && charged.max_bytes == baseline.max_bytes
        );
        let duplicate = start(&f.catalog, observer, &inputs, &platform, &verifier).await;
        ensure!(matches!(
            duplicate
                .err()
                .and_then(|e| e.downcast::<FleetCatalogError>().ok()),
            Some(FleetCatalogError::Overloaded)
        ));
        ensure!(observer.resource_usage().control == charged);
        ensure!(
            inputs.calls.load(Ordering::Acquire) == 0,
            "repair ran before platform isolation completed"
        );
        service.take().unwrap().shutdown().await;
        ensure!(observer.resource_usage().control == baseline);
        ensure!(
            platform.active.load(Ordering::Acquire) == 0,
            "shutdown did not join the isolation callback"
        );
        let cut = f.catalog.read(TIMEOUT).await?;
        ensure!(
            f.catalog
                .evacuation_admission(&cut, context.operation(), TIMEOUT)
                .await?
                .node()
                .bytes()
                == boot.node
        );
        ensure!(f.catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
        platform.hold.store(false, Ordering::Release);
        service = Some(start(&f.catalog, observer, &inputs, &platform, &verifier).await?);
        wait(service.as_ref().unwrap(), |status| {
            status
                .failure_recovery
                .as_ref()
                .is_some_and(|r| r.phase == FleetFailureRecoveryPhase::VerifyingFinalization)
                && independent
                    .denied_group_payload_reads
                    .load(Ordering::Acquire)
                    > 0
        })
        .await?;
        let cut = f.catalog.read(TIMEOUT).await?;
        ensure!(f
            .catalog
            .finalization_receipt(&cut, context.operation(), TIMEOUT)
            .await?
            .is_none());
        ensure!(f
            .catalog
            .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
            .await?
            .is_none());
        ensure!(
            f.catalog
                .pending_reservations(&cut, TIMEOUT)
                .await?
                .is_empty(),
            "completed repair retained the active debit"
        );
        ensure!(cluster.hosts[3].resource_usage().retained_manifest_cells == Some(2));
        ensure!(isolation::disk_digest(&disk)? == retained);
        service.take().unwrap().shutdown().await;
        ensure!(observer.resource_usage().control == baseline);
        let payload_reads = independent.group_payload_reads.load(Ordering::Acquire);
        let isolation_calls = platform.contexts.lock().unwrap().len();
        ensure!(isolation_calls >= 2 && inputs.calls.load(Ordering::Acquire) > 0);
        independent
            .deny_group_payload_reads
            .store(false, Ordering::Release);
        service = Some(start(&f.catalog, observer, &inputs, &platform, &verifier).await?);
        wait(service.as_ref().unwrap(), |status| {
            status.failure_recovery.as_ref().is_some_and(|r| {
                r.phase == FleetFailureRecoveryPhase::Removed && r.removed_revision.is_some()
            })
        })
        .await?;
        ensure!(
            platform.contexts.lock().unwrap().len() == isolation_calls,
            "restart repeated a committed platform isolation"
        );
        let status = service
            .as_ref()
            .unwrap()
            .status()
            .failure_recovery
            .context("terminal failure recovery status")?;
        ensure!(status.context == context);
        service.take().unwrap().shutdown().await;
        ensure!(observer.resource_usage().control == baseline);
        let cut = f.catalog.read(TIMEOUT).await?;
        let removed = f
            .catalog
            .isolated_removal_receipt(&cut, context.operation(), TIMEOUT)
            .await?
            .context("journal-confirmed isolated removal")?;
        ensure!(
            removed.node() == boot.node
                && Some(removed.applied_revision()) == status.removed_revision
        );
        ensure!(removed.isolation_digest() != [0; 32] && removed.candidate_digest() != [0; 32]);
        ensure!(f
            .catalog
            .authorize_host(&cut, cluster.fixtures[obsolete].spec.node, TIMEOUT)
            .await
            .is_err());
        ensure!(
            f.catalog
                .pending_reservations(&cut, TIMEOUT)
                .await?
                .is_empty()
                && f.catalog.pending_movements(&cut, TIMEOUT).await?.is_empty()
        );
        ensure!(
            isolation::disk_digest(&disk)? == retained,
            "terminal removal reclaimed the old engine"
        );
        ensure!(
            independent.group_payload_reads.load(Ordering::Acquire) > payload_reads,
            "terminal proof borrowed serving state instead of S3 replay"
        );
        verify_cells(observer, &seed, 2).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Some(service) = service {
        service.shutdown().await;
    }
    if let Some(index) = stopped_index {
        cluster.hosts.remove(index);
    }
    let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained failed automatic recovery disk: {}",
                root.keep().display()
            );
        }
        eprintln!(
            "retained independent recovery verifier: {}",
            replay_root.keep().display()
        );
    }
    joined?;
    work
}
