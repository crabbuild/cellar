// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{DiskLineage, GroupId, IncarnationId, MemberId, ProcessGeneration};

fn route() -> anyhow::Result<(
    Route,
    ReplicaIdentity<GroupHistory>,
    ReplicaIdentity<GroupHistory>,
)> {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let replica = |id| -> anyhow::Result<_> {
        Ok(ReplicaIdentity {
            scope: history,
            member: MemberId::new(u64::from(id))?,
            node: NodeId::new([id; 16])?,
            disk: DiskLineage::new([id + 3; 16])?,
            process: ProcessGeneration::new([id + 6; 16])?,
        })
    };
    let leader = replica(1)?;
    let voter = replica(2)?;
    let route = Route::new_group(&leader, "leader", &voter, "voter", [5; 32])?;
    Ok((route, leader, voter))
}

#[test]
fn park_reply_is_bound_to_tls_request_cut_and_process() -> anyhow::Result<()> {
    let auth = PeerAuth::new([8; 32], "leader")?;
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[9; 32],
        options: &options,
        max_body: 0,
    };
    let (route, leader, voter) = route()?;
    let cut = serde_json::from_str(r#"{"generation":3,"term":2,"membership_revision":1}"#)?;
    let request = Request {
        route,
        cut,
        timeout_ms: 100,
        certificate_digest: None,
    };
    assert_eq!(request.deadline(10, 109)?, 110);
    assert!(matches!(
        request.deadline(10, 110),
        Err(TransportError::Timeout)
    ));
    let head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])?;
    let reply = wire.encode(
        RESPONSE_MAGIC,
        RESPONSE_PURPOSE,
        &head.tag,
        &Reply::Acknowledged { sequence: 6 },
        &[],
    )?;

    let mut rebooted = voter.clone();
    rebooted.process = ProcessGeneration::new([19; 16])?;
    let other_route = Route::new_group(&leader, "leader", &rebooted, "voter", [5; 32])?;
    let other = Request {
        route: other_route,
        cut: request.cut.clone(),
        timeout_ms: request.timeout_ms,
        certificate_digest: None,
    };
    let other_head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &other, &[])?;
    assert!(matches!(
        wire.decode::<Reply>(
            RESPONSE_MAGIC,
            RESPONSE_PURPOSE,
            &reply,
            Some(&other_head.tag)
        ),
        Err(TransportError::Authentication)
    ));
    let other_cut = Request {
        route: request.route.clone(),
        cut: serde_json::from_str(r#"{"generation":4,"term":2,"membership_revision":1}"#)?,
        timeout_ms: request.timeout_ms,
        certificate_digest: None,
    };
    let other_head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &other_cut, &[])?;
    assert!(matches!(
        wire.decode::<Reply>(
            RESPONSE_MAGIC,
            RESPONSE_PURPOSE,
            &reply,
            Some(&other_head.tag)
        ),
        Err(TransportError::Authentication)
    ));
    let install = Request {
        route: request.route.clone(),
        cut: request.cut.clone(),
        timeout_ms: request.timeout_ms,
        certificate_digest: Some(Sha256::digest(b"certificate").into()),
    };
    let install_head = wire.encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &install, b"")?;
    assert!(matches!(
        wire.decode::<Reply>(
            RESPONSE_MAGIC,
            RESPONSE_PURPOSE,
            &reply,
            Some(&install_head.tag)
        ),
        Err(TransportError::Authentication)
    ));
    assert!(matches!(
        install.certificate(b"modified certificate"),
        Err(TransportError::Authentication)
    ));
    let other_wire = Wire {
        exporter: &[10; 32],
        ..wire
    };
    assert!(matches!(
        other_wire.decode::<Reply>(RESPONSE_MAGIC, RESPONSE_PURPOSE, &reply, Some(&head.tag)),
        Err(TransportError::Authentication)
    ));

    let decoded: Reply = wire.decode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &reply, Some(&head.tag))?;
    let expected_cut = request.cut.clone();
    let receipt = decoded.acknowledgement(request)?;
    assert_eq!(receipt.voter, voter);
    assert_eq!(receipt.leader, leader.member);
    assert_eq!(receipt.cut, expected_cut);
    assert_eq!(receipt.sync_sequence, 6);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn park_certificate_delivery_and_wake_over_three_tls_voters() -> anyhow::Result<()> {
    use crate::{
        asyncrt::ExecutionDomain,
        raft::{
            catalog::{Catalog, CatalogOptions, InitialMember, NamespaceConfig},
            cell_groups::{
                CellGroupApplier, CellGroupApplyImages, CellGroupBirthSpec, CellGroupEffect,
                CellGroupRuntime, DirectoryLimits, ManifestPageStore, ManifestTree,
                ParkPrepareRequest, RangeDirectory,
            },
            options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
            storage::ReplicaLog,
        },
    };
    use anyhow::Context as _;
    use cellar_logic::{
        cell_group::{CellRange, MembershipRevision, ParkGeneration, RouteEpoch},
        consensus::ProposalId,
    };
    use cellar_ltx::{
        generation::{GenerationStore, ReadServices},
        DirectFileSystem,
    };

    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (_, leader, _) = route()?;
    let history = leader.scope;
    let catalog = Catalog::new(
        Arc::new(object_store::memory::InMemory::new()),
        CatalogOptions::default(),
    )?;
    let namespace = domain
        .run(catalog.open_namespace(NamespaceConfig::new(
            history.cluster,
            history.namespace,
            cellar_logic::NamespaceMode::Raft,
            [5; 32],
        )))
        .await??;
    let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let voters = (1..=3)
        .map(|id| -> anyhow::Result<_> {
            Ok(InitialMember {
                member: MemberId::new(u64::from(id))?,
                node: NodeId::new([id; 16])?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let birth_request = directory.prepare_group_birth(CellGroupBirthSpec {
        history,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1)?,
        creation: ProposalId::new([18; 16])?,
        voters: voters.clone(),
        learners: vec![],
    })?;
    domain
        .run(directory.reserve_group_birth(&birth_request))
        .await??;
    let initialize = directory
        .prepare_initialization(ProposalId::new([19; 16])?, &[birth_request.route().clone()])?;
    domain.run(directory.publish(&initialize)).await??;
    let birth = domain
        .run(directory.materialize_group_birth(&birth_request))
        .await??;

    let mut peers = Vec::new();
    let mut logs = Vec::new();
    let mut runtimes = Vec::new();
    let mut transports = Vec::new();
    let mut pumps = Vec::new();
    for (index, voter) in voters.iter().enumerate() {
        let base = root.path().join(format!("voter-{index}"));
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    base.join("log"),
                    voter.node,
                    DiskLineage::new([index as u8 + 11; 16])?,
                    ProcessGeneration::new([index as u8 + 21; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let permit = domain
            .run(directory.bootstrap_group_member(&birth, &log))
            .await??;
        let group = permit.group().clone();
        let identity = group.identity().clone();
        let (root, applier) = domain
            .blocking(move || -> anyhow::Result<_> {
                let mut pages = ManifestPageStore::open(
                    &base.join("pages"),
                    Arc::new(DirectFileSystem),
                    128,
                    1024 * 1024,
                    16,
                )?;
                let marker = group.group_manifest()?.context("genesis")?;
                let tree = ManifestTree::new(marker.route().clone(), Default::default())?;
                let root = tree.prepare_complete(
                    &group,
                    &mut pages,
                    marker.root(),
                    marker.position(),
                    marker.membership_revision(),
                    |_| unreachable!("empty genesis"),
                )?;
                let images = CellGroupApplyImages::Materialized {
                    images: GenerationStore::open(
                        base.join("images"),
                        Arc::new(DirectFileSystem),
                        Default::default(),
                    )?,
                    services: ReadServices {
                        now_unix_millis: Arc::new(crate::asyncrt::wall_ms),
                        fill_random: Arc::new(|bytes| bytes.fill(17)),
                    },
                };
                let applier = CellGroupApplier::new(
                    group,
                    pages,
                    images,
                    Default::default(),
                    Default::default(),
                    32,
                )?;
                Ok((root, applier))
            })?
            .await??;
        let (runtime, mut events) = CellGroupRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                election_seed: Some([index as u8 + 31; 32]),
                ..Default::default()
            },
        )
        .await?;
        let tls = RaftTls::new(
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
        )?;
        let label = format!("park-voter-{index}");
        let transport = RaftTransport::start_shared(
            runtime.clone(),
            Arc::new(PeerAuth::new([8; 32], &label)?),
            tls,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TransportOptions::default(),
        )
        .await?;
        runtime.attach(permit, root).await?;
        let pump = domain.spawn({
            let runtime = runtime.clone();
            let transport = transport.clone();
            async move {
                while let Some(event) = events.recv().await {
                    match event.effect().map_err(Clone::clone)? {
                        CellGroupEffect::Message(_) => {
                            // The transport owns queued delivery independent of the waiter.
                            let _delivery = transport.submit_cell_group_event(&event)?;
                        }
                        CellGroupEffect::Apply(_) => {
                            let work = event.retain_apply().context("physical apply")?;
                            drop(event);
                            let prepare = work.clone();
                            let applier = applier.clone();
                            let next = crate::asyncrt::blocking(move || applier.prepare(&prepare))
                                .await??;
                            runtime.complete_apply(history, work, next).await?;
                        }
                        _ => (),
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
        })?;
        peers.push(RaftPeer {
            replica: identity,
            label,
            server_name: "localhost".into(),
            control_address: transport.control_address(),
            data_address: transport.data_address(),
            query_address: transport.query_address(),
        });
        logs.push(log);
        runtimes.push(runtime);
        transports.push(transport);
        pumps.push(pump);
    }
    for transport in &transports {
        transport.install_physical_group(GroupRoute {
            scope: history,
            schema_digest: birth.route().schema_digest,
            revision: 1,
            peers: peers.clone(),
        })?;
    }
    let runtime = &runtimes[0];
    let transport = &transports[0];
    let log = &logs[0];
    runtime.campaign(history).await?;
    let status = wait_park_test(&domain, || async {
        let status = runtime.status(history).await?;
        Ok(status.ready.then_some(status))
    })
    .await?;
    let abandoned = ParkGeneration::new(status.applied.index.get() + 1)?;
    let deadline = log.domain().mono_ms()? + MAX_PARK_NEGOTIATION_MS;
    let status = wait_park_test(&domain, || async {
        match runtime
            .fence_managed_park(history, abandoned, status.term, deadline)
            .await
        {
            Ok(()) => Ok(Some(runtime.status(history).await?)),
            Err(RuntimeError::NotReady) => Ok(None),
            Err(error) => Err(error.into()),
        }
    })
    .await?;
    assert!(status.applied.index.get() > abandoned.get());
    assert!(log
        .cell_group(&history)?
        .group_manifest()?
        .context("root")?
        .park_cut()
        .is_none());
    let deadline = log.domain().mono_ms()? + MAX_PARK_NEGOTIATION_MS;
    let request = ParkPrepareRequest {
        proposal: ProposalId::new([20; 16])?,
        term: status.term,
        membership: MembershipRevision::new(1)?,
        expected_applied: status.applied,
        deadline_ms: deadline,
    };
    let prepared = wait_park_test(&domain, || async {
        match runtime.prepare_park(history, request).await {
            Ok(prepared) => Ok(Some(prepared)),
            Err(RuntimeError::NotReady) => Ok(None),
            Err(error) => Err(error.into()),
        }
    })
    .await?;
    let cut = wait_park_test(&domain, || async {
        Ok(log
            .cell_group(&history)?
            .group_manifest()?
            .and_then(|marker| marker.park_cut().cloned())
            .filter(|cut| cut.position() == prepared.proposal.position))
    })
    .await?;
    let certificate = transport
        .collect_park_certificate(history, cut.clone(), deadline)
        .await?;
    transport
        .deliver_park_certificate(history, &certificate, deadline)
        .await?;
    for log in &logs {
        let group = log.cell_group(&history)?;
        let cache = group.read()?;
        let parked = cache.local_park.as_ref().context("durable park")?;
        assert!(parked.parked());
        assert_eq!(parked.certificate(), Some(certificate.certificate()));
    }
    assert!(!runtime.status(history).await?.ready);
    let deadline = log.domain().mono_ms()? + MAX_PARK_NEGOTIATION_MS;
    assert!(matches!(
        runtime.park_admission(history, deadline).await,
        Err(RuntimeError::NotReady)
    ));
    assert!(matches!(
        runtime
            .wake_park(
                history,
                ParkGeneration::new(cut.generation() - 1)?,
                deadline
            )
            .await,
        Err(RuntimeError::StaleCompletion)
    ));
    assert!(matches!(
        runtime
            .prepare_park(
                history,
                ParkPrepareRequest {
                    deadline_ms: deadline,
                    ..request
                }
            )
            .await,
        Err(RuntimeError::NotReady)
    ));
    let retry = transport
        .collect_park_certificate(history, cut.clone(), deadline)
        .await?;
    assert_eq!(retry.certificate(), certificate.certificate());
    transport
        .deliver_park_certificate(history, &retry, deadline)
        .await?;
    let observed = runtime
        .wake_park(history, ParkGeneration::new(cut.generation())?, deadline)
        .await?;
    assert!(observed.root().position().index > cut.position().index);
    drop(observed);
    let resumed = wait_park_test(&domain, || async {
        let status = runtime.status(history).await?;
        Ok((status.ready && status.applied.index > cut.position().index).then_some(status))
    })
    .await?;
    assert!(resumed.term > cut.position().term);
    assert!(matches!(
        runtime
            .enter_park(history, certificate.certificate().clone())
            .await,
        Err(RuntimeError::NotReady)
    ));

    for runtime in &runtimes {
        runtime.close().await?;
    }
    for pump in pumps {
        pump.await??;
    }
    for transport in &transports {
        transport.close().await;
    }
    for log in &logs {
        log.close().await?;
    }
    domain.shutdown().await;
    Ok(())
}

async fn wait_park_test<T, F: Future<Output = anyhow::Result<Option<T>>>>(
    domain: &crate::asyncrt::ExecutionDomain,
    mut sample: impl FnMut() -> F,
) -> anyhow::Result<T> {
    domain
        .run(crate::asyncrt::timeout(
            std::time::Duration::from_secs(10),
            async {
                loop {
                    if let Some(value) = sample().await? {
                        return Ok(value);
                    }
                    crate::asyncrt::sleep(std::time::Duration::from_millis(10)).await;
                }
            },
        ))
        .await??
}
