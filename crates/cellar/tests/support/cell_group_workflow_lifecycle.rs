// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public Workflow journals and streams across managed voter replacement and wake.
use super::*;
use cellar::sdk::fleet_operations::{
    RaftFleetControl, RaftFleetParkPhase, RaftFleetParkRequest, RaftFleetTransitionGroup,
};
use managed_fixture::movement;

const STREAM_BYTES: usize = 128 * 1024 + 17;

pub(super) async fn workflow(
    context: Context,
    steps: Step,
    signals: Arc<contract::Signals>,
) -> Result<StructuredValue> {
    let prepared = signals.clone();
    steps
        .run("prepare", move |_| async move {
            prepared.first.fetch_add(1, Ordering::SeqCst);
            StructuredValue::from_json(json!({"prepared":true}))
        })
        .await?;
    let captured = signals.clone();
    steps
        .run_stream("bytes", move |_| async move {
            captured.stream_runs.fetch_add(1, Ordering::SeqCst);
            let pipe = cellar::sdk::streams::IdentityTransformStream::new();
            let writer = pipe.writable.get_writer()?;
            context.wait_until_byte_stream(&pipe.readable, async move {
                writer.write(vec![7; STREAM_BYTES]).await?;
                writer.close().await?;
                Ok(())
            })?;
            Ok(pipe.readable)
        })
        .await?;
    steps
        .wait_for_event(
            "approval",
            WaitOptions::new("approve").timeout("10 minutes"),
        )
        .await?;
    steps
        .run("finish", move |_| async move {
            signals.last.fetch_add(1, Ordering::SeqCst);
            StructuredValue::from_json(json!({"bytes":STREAM_BYTES,"complete":true}))
        })
        .await
}

async fn local_version(
    cluster: &ManagedCluster,
    node: usize,
    history: &CellHistory,
    id: u8,
) -> Result<CellVersion> {
    let cell = cluster.hosts[node].cell(history.clone())?;
    let observed = cell
        .query(
            ProposalId::new([id; 16])?,
            &cell.contract("__sqlIngressSequence")?,
            0_u64.to_be_bytes().to_vec(),
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        observed.served_by == MemberId::new(u64::try_from(node)? + 1)?,
        "replacement read escaped its local voter"
    );
    ensure!(observed.served_by_node == cluster.hosts[node].identity());
    Ok(observed.version)
}

async fn replace(
    cluster: &ManagedCluster,
    history: &CellHistory,
    minimum: &CellVersion,
) -> Result<()> {
    let source = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let obsolete = (source + 1) % 3;
    movement::discover(cluster).await?;
    let (_, execution) = movement::admit_replacement(
        cluster,
        movement::ReplacementTarget {
            source,
            obsolete,
            spare: 3,
        },
        [186, 187],
        true,
    )
    .await?;
    let catalog = &cluster.fixtures[0].catalog;
    let mut settled = false;
    for _ in 0..8 {
        movement::discover(cluster).await?;
        let cut = catalog.read(TIMEOUT).await?;
        match catalog
            .reconcile_movement(&cut, &execution, &cluster.hosts[source], TIMEOUT)
            .await
        {
            Ok(progress) if progress.settled => {
                settled = true;
                break;
            }
            Ok(_) => (),
            Err(cellar::sdk::consensus::verification::FleetCatalogError::Unconfirmed) => {
                let cut = catalog.read(TIMEOUT).await?;
                let pending = catalog.pending_reservations(&cut, TIMEOUT).await?;
                ensure!(
                    pending.is_empty()
                        || pending.len() == 1 && pending[0].reservation == execution.reservation,
                    "Workflow replacement lost its durable reservation"
                );
            }
            Err(error) => return Err(error).context("Workflow S3 voter replacement"),
        }
    }
    ensure!(settled, "Workflow replacement did not settle");
    ensure!(
        cluster.fixtures[3]
            .store
            .group_payload_reads
            .load(Ordering::Acquire)
            > 0,
        "replacement never fetched an S3 archive payload"
    );
    let membership = cluster.hosts[source]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    ensure!(
        membership.configuration().voters.len() == 3
            && membership.configuration().voters.contains(&4)
            && !membership
                .configuration()
                .voters
                .contains(&(obsolete as u64 + 1)),
        "Workflow replacement did not install RF3 membership"
    );
    drop(membership);
    movement::discover(cluster).await?;
    let restored = local_version(cluster, 3, history, 188).await?;
    ensure!(
        matches!(restored.covers(minimum), Ok(true)),
        "S3 replacement lost the committed Workflow image"
    );
    ensure!(call_at(cluster, 3, "read-stream", "lifecycle").await? == json!(STREAM_BYTES));
    Ok(())
}

async fn park_and_wake(cluster: &ManagedCluster) -> Result<()> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let membership = cluster.hosts[leader]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let voters = membership.configuration().voters.clone();
    ensure!(voters.len() == 3 && voters.contains(&4));
    drop(membership);
    let fixture = &cluster.fixtures[0];
    let revision = fixture.catalog.read(TIMEOUT).await?.revision();
    let control = RaftFleetControl::new(
        &fixture.domain,
        fixture.catalog.clone(),
        vec![cluster.directory.clone()],
    )?
    .with_node_observer(cluster.hosts[leader].clone());
    let request = RaftFleetParkRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(cluster.history.namespace.bytes()),
        group: RaftFleetTransitionGroup {
            group_id: hex::encode(cluster.history.group.bytes()),
            incarnation_id: hex::encode(cluster.history.incarnation.bytes()),
        },
        operation_id: hex::encode([189; 16]),
        request_id: hex::encode([190; 16]),
        settlement_request_id: hex::encode([191; 16]),
        expected_revision: revision,
    };
    let parked = control
        .park_group(
            &managed_fixture::request(190, revision).principal,
            &request,
            TIMEOUT,
        )
        .await?;
    ensure!(parked.phase == RaftFleetParkPhase::Delivered);
    for member in voters {
        let node = usize::try_from(member.checked_sub(1).context("zero Workflow voter")?)?;
        ensure!(
            !cluster.hosts[node]
                .group(cluster.history)?
                .status()
                .await?
                .ready,
            "Workflow voter was not fenced in park"
        );
    }
    // The public resource read, rather than an operator wake, must restore service.
    ensure!(call_at(cluster, 3, "read-stream", "lifecycle").await? == json!(STREAM_BYTES));
    Ok(())
}

pub async fn qualify(real: bool, external: bool) -> Result<()> {
    for extents in [false, true] {
        Box::pin(case(real, external, extents))
            .await
            .with_context(|| {
                format!("Workflow lifecycle RustFS={real} Axum={external} extents={extents}")
            })?;
    }
    Ok(())
}

async fn case(real: bool, external: bool, extents: bool) -> Result<()> {
    let backend = real
        .then(|| {
            rustfs::StoreConfig::load(
                if external {
                    "workflow-lifecycle-axum"
                } else {
                    "workflow-lifecycle"
                },
                extents,
            )
        })
        .transpose()?;
    let store = Arc::new(match &backend {
        Some(backend) => catalog_fault::Store::around(backend.open(None)?),
        None => catalog_fault::Store::default(),
    });
    let signals = Arc::new(contract::Signals::default());
    let (application, configuration) = application_with_sink(signals.clone(), false, false)?;
    let class = cellar::deploy::workflow_class(contract::SCRIPT);
    let mut fixture = Fixture::with_application(extents, &class, application, store).await?;
    fixture.configuration = Some(configuration);
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 30_000,
        ..Default::default()
    });
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
    let domain = fixture.domain.clone();
    let started = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[7],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await;
    let mut cluster = match started {
        Ok(cluster) => cluster,
        Err(error) => {
            if let Some(http) = http {
                http.close().await?;
            }
            domain.shutdown().await;
            return Err(error);
        }
    };
    let work = domain
        .run(async {
            named_service::grant(&cluster).await?;
            // Establish the movement fixture's initial serving quorum, as the
            // other Workflow fixture does for its separately activated group.
            // The instance is still absent: its enrollment and create must
            // complete inside the unmodified public request deadline.
            let readiness_started = cellar::asyncrt::mono_ms();
            managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            eprintln!(
                "Workflow lifecycle initial quorum ready after {}ms",
                cellar::asyncrt::mono_ms().saturating_sub(readiness_started)
            );
            call(&cluster, "create", "lifecycle").await?;
            state(&cluster, "lifecycle", "waiting").await?;
            ensure!(
                signals.first.load(Ordering::SeqCst) == 1
                    && signals.stream_runs.load(Ordering::SeqCst) == 1
            );
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let history = cluster.hosts[leader]
                .resolve_cell_history(
                    cluster.history.cluster,
                    cluster.history.namespace,
                    CellId::new(cellar::engine_api::workflow_cell_scope(
                        contract::SCRIPT,
                        "orders",
                        "lifecycle",
                    ))?,
                    QueryOptions::default(),
                )
                .await?
                .context("Workflow lifecycle cell history")?;
            let minimum = local_version(&cluster, leader, &history, 193).await?;
            replace(&cluster, &history, &minimum)
                .await
                .context("Workflow replacement phase")?;
            park_and_wake(&cluster)
                .await
                .context("Workflow park/wake phase")?;
            named_service::restart(&mut cluster)
                .await
                .context("Workflow retained replacement restart")?;
            state(&cluster, "lifecycle", "waiting").await?;
            let resumed = local_version(&cluster, 3, &history, 192).await?;
            ensure!(
                matches!(resumed.covers(&minimum), Ok(true)),
                "retained restart lost the committed Workflow image"
            );
            ensure!(
                signals.first.load(Ordering::SeqCst) == 1
                    && signals.stream_runs.load(Ordering::SeqCst) == 1,
                "completed Workflow steps replayed during replacement, wake or restart"
            );
            call_at(&cluster, 3, "event", "lifecycle").await?;
            let complete = state(&cluster, "lifecycle", "complete").await?;
            ensure!(complete["output"] == json!({"bytes":STREAM_BYTES,"complete":true}));
            ensure!(signals.last.load(Ordering::SeqCst) == 1);
            ensure!(call(&cluster, "read-stream", "lifecycle").await? == json!(STREAM_BYTES));
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    if work.is_err() {
        for (node, host) in cluster.hosts.iter().enumerate() {
            eprintln!(
                "Workflow lifecycle failure resources: {}",
                json!({"node":node,"resources":host.resource_usage(),"performance":host.performance()})
            );
        }
    }
    let closed = managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &cluster.hosts,
        1,
    )
    .await;
    if let Err(error) = &closed {
        eprintln!("Workflow lifecycle joined close failed: {error:#}");
    }
    if work.is_err() || closed.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained Workflow lifecycle disk: {}",
                root.keep().display()
            );
        }
    }
    if let Some(http) = http {
        http.close().await?;
    }
    domain.shutdown().await;
    work?;
    closed?;
    eprintln!("Workflow lifecycle RustFS={real} Axum={external} extents={extents}: PASS");
    Ok(())
}
