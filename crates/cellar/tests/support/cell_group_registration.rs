// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Runtime registration uses real engines, catalog reads, TLS and shared SQL.
use super::remote_queries::{keys, wait_for};
use super::*;
use std::sync::atomic::Ordering;
#[path = "raft_catalog_fault.rs"]
#[allow(dead_code)]
mod catalog_fault;

pub(super) struct Prepared {
    directory: RangeDirectory,
    pub(super) birth: CellGroupBirth,
    store: Arc<catalog_fault::Store>,
    pub(super) inputs: Vec<CellGroupRegistration>,
    number: u8,
}

fn class() -> Result<Arc<RaftClass>> {
    Ok(Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .initializer([83; 32], |_, tx| {
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                Ok(())
            })?
            .command("Add", [82; 32], |ctx, input| {
                ctx.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(input.to_vec())
            })?,
    ))
}

pub(super) async fn prepare(f: &RuntimeCluster, number: u8, a: &CellHistory) -> Result<Prepared> {
    let store = Arc::new(catalog_fault::Store::default());
    let old = f.cluster.birth.route();
    let scope = GroupHistory {
        cluster: old.history.cluster,
        namespace: NamespaceId::new([number; 16])?,
        group: GroupId::new([number; 16])?,
        incarnation: IncarnationId::new([number; 16])?,
    };
    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let namespace = f
        .cluster
        .domain
        .run(catalog.open_namespace(NamespaceConfig::new(
            scope.cluster,
            scope.namespace,
            cellar_logic::NamespaceMode::Raft,
            old.schema_digest,
        )))
        .await??;
    let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let birth = directory.prepare_group_birth(CellGroupBirthSpec {
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
    f.cluster
        .domain
        .run(directory.reserve_group_birth(&birth))
        .await??;
    let initialize = directory
        .prepare_initialization(ProposalId::new([number + 1; 16])?, &[birth.route().clone()])?;
    f.cluster
        .domain
        .run(directory.publish(&initialize))
        .await??;
    let birth = f
        .cluster
        .domain
        .run(directory.materialize_group_birth(&birth))
        .await??;
    let mut prepared = Prepared {
        directory,
        birth,
        store,
        inputs: vec![],
        number,
    };
    prepared.inputs = recover(f, &prepared, a).await?;
    Ok(prepared)
}

async fn recover(
    f: &RuntimeCluster,
    prepared: &Prepared,
    a: &CellHistory,
) -> Result<Vec<CellGroupRegistration>> {
    let mut parts = vec![];
    for n in 0..3 {
        let permit = f
            .cluster
            .domain
            .run(
                prepared
                    .directory
                    .bootstrap_group_member(&prepared.birth, &f.cluster.voters[n].log),
            )
            .await??;
        let pages = ManifestPageStore::open(
            &f.cluster
                ._root
                .path()
                .join(format!("registered-{}-{n}", prepared.number)),
            Arc::new(DirectFileSystem),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let images = match f.cell_options_at(a, n).await? {
            CellSqlOptions::Materialized(o) => CellGroupApplyImages::Materialized {
                images: o.images,
                services: o.query_services,
            },
            CellSqlOptions::Extents(o) => CellGroupApplyImages::Extents {
                images: o.images,
                services: o.query_services,
                limits: Default::default(),
                cache_directory: o.cache_directory,
                cache_pool: o.cache_pool,
                cache: o.cache,
            },
        };
        let (applier, current) = CellGroupApplier::recover(
            permit.group().clone(),
            pages,
            images,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        parts.push((permit, current, applier));
    }
    let route = f.tcp_route(prepared.birth.route().history)?;
    Ok(parts
        .into_iter()
        .map(|(permit, current, applier)| CellGroupRegistration {
            permit,
            current,
            applier,
            transport: route.clone(),
        })
        .collect())
}

async fn duplicate(
    f: &RuntimeCluster,
    prepared: &Prepared,
    input: &CellGroupRegistration,
    n: usize,
) -> Result<CellGroupRegistration> {
    Ok(CellGroupRegistration {
        permit: f
            .cluster
            .domain
            .run(
                prepared
                    .directory
                    .bootstrap_group_member(&prepared.birth, &f.cluster.voters[n].log),
            )
            .await??,
        current: input.current.clone(),
        applier: input.applier.clone(),
        transport: input.transport.clone(),
    })
}

pub(super) fn dispatcher(
    f: &RuntimeCluster,
    prepared: &Prepared,
    class: Arc<RaftClass>,
) -> Result<CellGroupDispatcher> {
    Ok(CellGroupDispatcher::new(
        &f.cluster.domain,
        prepared.directory.clone(),
        class,
        keys(190)?,
        CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?)
}

pub(super) async fn enroll(
    f: &RuntimeCluster,
    controller: &CellGroupController,
    dispatch: &CellGroupDispatcher,
    cell: &CellHistory,
) -> Result<()> {
    let invocation = dispatch
        .enroll(
            cell.clone(),
            CellEnrollmentRequest {
                creation: ProposalId::new([219; 16])?,
                durability: Durability::Quorum,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(10),
                max_attempts: 2,
            },
            None,
        )
        .await?;
    let enrolled = controller
        .enroll(invocation, f.cell_options(cell).await?)?
        .response()
        .await?;
    assert_eq!(enrolled.record().cell(), cell);
    drop(enrolled);
    Ok(())
}

async fn replicate(
    f: &RuntimeCluster,
    route: &GroupRoute,
    cell: &CellHistory,
    rooted: bool,
) -> Result<()> {
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(23);")?;
    let accepted = f.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(route, cell, &bytes, rooted)?,
        )
        .await?;
    wait_for(async || {
        for runtime in &f.runtimes {
            if runtime.status(route.history).await?.applied.index < accepted.position.index {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
}

pub(crate) async fn registration_live() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let mut prepared = prepare(&f, 31, &a).await?;
        let duplicate = duplicate(&f, &prepared, &prepared.inputs[0], 0).await?;
        let mut excess = prepare(&f, 41, &a).await?;
        prepared
            .store
            .hold_group_head_reads
            .store(true, Ordering::Release);
        let calls = prepared
            .inputs
            .drain(..)
            .enumerate()
            .map(|(n, input)| controllers[n].register(input))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        wait_for(async || {
            Ok(prepared
                .store
                .active_group_head_reads
                .load(Ordering::Acquire)
                == 3)
        })
        .await?;
        assert_eq!(controllers[0].usage().groups, 1);
        assert_eq!(controllers[0].usage().pending_registrations, 1);
        let rejected = controllers[0]
            .register(duplicate)
            .err()
            .context("duplicate")?;
        assert!(matches!(
            rejected.error,
            CellGroupControllerError::Runtime(RuntimeError::AlreadyAttached)
        ));
        drop(rejected);
        let rejected = controllers[0]
            .register(excess.inputs.remove(0))
            .err()
            .context("pending capacity")?;
        assert!(matches!(
            rejected.error,
            CellGroupControllerError::Runtime(RuntimeError::Overloaded)
        ));
        replicate(
            &f,
            &route,
            &history(&route, "live-during-register")?,
            rooted,
        )
        .await?;
        // Lost waiters cannot abandon the attachment or free its reserved slot.
        drop(calls);
        prepared
            .store
            .hold_group_head_reads
            .store(false, Ordering::Release);
        prepared.store.group_head_reads_resume.notify_waiters();
        wait_for(async || {
            Ok(controllers.iter().all(|c| {
                c.usage().groups == 2
                    && c.usage().pending_registrations == 0
                    && c.usage().retained_commands == 0
            }))
        })
        .await?;
        assert!(matches!(
            controllers[0]
                .register(*rejected.registration)
                .err()
                .context("active capacity")?
                .error,
            CellGroupControllerError::Runtime(RuntimeError::Overloaded)
        ));
        let group = prepared.birth.route().history;
        f.runtimes[0].campaign(group).await?;
        wait_for(async || Ok(f.runtimes[0].status(group).await?.ready)).await?;
        let cell = history(prepared.birth.route(), "dynamic-enrollment")?;
        let dispatch = dispatcher(&f, &prepared, class.clone())?;
        enroll(&f, &controllers[0], &dispatch, &cell).await?;
        enroll(&f, &controllers[0], &dispatch, &cell).await?;
        for runtime in &f.runtimes {
            wait_for(async || {
                Ok(runtime
                    .reconciliation_authority(group, cell.clone())
                    .await
                    .is_ok())
            })
            .await?;
            assert_eq!(
                runtime
                    .reconciliation_authority(group, cell.clone())
                    .await?
                    .cell()
                    .version
                    .sequence
                    .get(),
                1
            );
        }
        for transport in &f.transports {
            assert_eq!(transport.usage().peers, 2);
        }
        for c in controllers {
            c.close().await?;
        }
        f.close().await?;
    }
    Ok(())
}

pub(crate) async fn registration_refusals() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let mut prepared = prepare(&f, 31, &a).await?;
        let mut wrong = duplicate(&f, &prepared, &prepared.inputs[0], 0).await?;
        wrong.applier = prepared.inputs[1].applier.clone();
        assert!(controllers[0].register(wrong).is_err());
        let mut wrong = duplicate(&f, &prepared, &prepared.inputs[0], 0).await?;
        wrong.current = prepared.inputs[1].current.clone();
        assert!(controllers[0].register(wrong).is_err());
        let mut wrong = duplicate(&f, &prepared, &prepared.inputs[0], 0).await?;
        let pages = ManifestPageStore::open(
            &f.cluster._root.path().join("foreign-page-owner"),
            Arc::new(DirectFileSystem),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let config = match f.cell_options(&a).await? {
            CellSqlOptions::Materialized(o) => CellGroupApplyImages::Materialized {
                images: o.images,
                services: o.query_services,
            },
            CellSqlOptions::Extents(o) => CellGroupApplyImages::Extents {
                images: o.images,
                services: o.query_services,
                limits: Default::default(),
                cache_directory: o.cache_directory,
                cache_pool: o.cache_pool,
                cache: o.cache,
            },
        };
        wrong.applier = CellGroupApplier::new(
            wrong.permit.group().clone(),
            pages,
            config,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        assert!(matches!(
            controllers[0]
                .register(wrong)
                .err()
                .context("foreign pages")?
                .error,
            CellGroupControllerError::Apply(_)
        ));
        // An already installed external route cannot be adopted or removed.
        f.transports[0].install_physical_group(prepared.inputs[0].transport.clone())?;
        let input = duplicate(&f, &prepared, &prepared.inputs[0], 0).await?;
        assert!(
            matches!(controllers[0].register(input)?.response().await, Err(e) if matches!(e.as_ref(),CellGroupControllerError::Transport(TransportError::Identity)))
        );
        f.transports[0].remove_physical_group(&prepared.birth.route().history)?;
        // Bootstrap revalidation fails after real local genesis; healthy groups
        // remain usable and the failed registration's slot/route is released.
        let input = duplicate(&f, &prepared, &prepared.inputs[0], 0).await?;
        prepared
            .store
            .fail_directory_reads
            .store(true, Ordering::Release);
        assert!(controllers[0].register(input)?.response().await.is_err());
        prepared
            .store
            .fail_directory_reads
            .store(false, Ordering::Release);
        assert!(!controllers[0].usage().closing);
        assert_eq!(controllers[0].usage().groups, 1);
        assert_eq!(controllers[0].usage().pending_registrations, 0);
        replicate(
            &f,
            &route,
            &history(&route, "after-registration-failure")?,
            rooted,
        )
        .await?;
        for (n, input) in prepared.inputs.drain(..).enumerate() {
            controllers[n].register(input)?.response().await?;
        }
        f.runtimes[0]
            .campaign(prepared.birth.route().history)
            .await?;
        wait_for(async || {
            Ok(f.runtimes[0]
                .status(prepared.birth.route().history)
                .await?
                .ready)
        })
        .await?;
        let dispatch = dispatcher(&f, &prepared, class)?;
        enroll(
            &f,
            &controllers[0],
            &dispatch,
            &history(prepared.birth.route(), "after-retry")?,
        )
        .await?;
        for c in controllers {
            c.close().await?;
        }
        f.close().await?;
    }
    Ok(())
}

pub(crate) async fn registration_close() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let mut prepared = prepare(&f, 31, &a).await?;
        prepared
            .store
            .hold_group_head_reads
            .store(true, Ordering::Release);
        let call = controllers[0].register(prepared.inputs.remove(0))?;
        wait_for(async || {
            Ok(prepared
                .store
                .active_group_head_reads
                .load(Ordering::Acquire)
                == 1)
        })
        .await?;
        drop(call);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
                .await
                .is_err()
        );
        assert!(controllers[0].usage().closing);
        assert_eq!(controllers[0].usage().pending_registrations, 1);
        assert!(CellGroupRuntime::start(
            f.cluster.voters[0].log.clone(),
            RaftRuntimeOptions::default()
        )
        .await
        .is_err());
        prepared
            .store
            .hold_group_head_reads
            .store(false, Ordering::Release);
        prepared.store.group_head_reads_resume.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), controllers[0].wait()).await??;
        assert_eq!(controllers[0].usage().pending_registrations, 0);
        let (replacement, events) = CellGroupRuntime::start(
            f.cluster.voters[0].log.clone(),
            RaftRuntimeOptions::default(),
        )
        .await?;
        replacement.close().await?;
        drop(events);
        for c in controllers {
            c.close().await?;
        }
        f.close().await?;
    }
    Ok(())
}

pub(super) async fn mutate(
    f: &RuntimeCluster,
    controller: &CellGroupController,
    dispatch: &CellGroupDispatcher,
    class: &RaftClass,
    cell: &CellHistory,
    retry: u64,
) -> Result<()> {
    let invocation = dispatch
        .command(
            cell.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([223; 16])?, 1)?,
                contract: class.contract("Add").context("Add")?,
                input: b"persisted".to_vec(),
                durability: Durability::Quorum,
                retry_until_ms: retry,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(10),
                max_attempts: 2,
            },
            None,
        )
        .await?;
    let response = controller
        .command(invocation, f.cell_options(cell).await?)?
        .response()
        .await?;
    assert_eq!(response.receipt().receipt.result(), b"persisted");
    assert_eq!(response.receipt().version.sequence.get(), 2);
    drop(response);
    Ok(())
}

pub(crate) async fn registration_recovery() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let mut prepared = prepare(&f, 31, &a).await?;
        for (n, input) in prepared.inputs.drain(..).enumerate() {
            controllers[n].register(input)?.response().await?;
        }
        let group = prepared.birth.route().history;
        f.runtimes[0].campaign(group).await?;
        wait_for(async || Ok(f.runtimes[0].status(group).await?.ready)).await?;
        let cell = history(prepared.birth.route(), "recovered-registration")?;
        let dispatch = dispatcher(&f, &prepared, class.clone())?;
        enroll(&f, &controllers[0], &dispatch, &cell).await?;
        let retry = f
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        mutate(&f, &controllers[0], &dispatch, &class, &cell, retry).await?;
        wait_for(async || {
            for runtime in &f.runtimes {
                if runtime
                    .reconciliation_authority(group, cell.clone())
                    .await?
                    .cell()
                    .version
                    .sequence
                    .get()
                    != 2
                {
                    return Ok(false);
                }
            }
            Ok(true)
        })
        .await?;
        for c in &controllers {
            c.close().await?;
        }
        drop(controllers);
        f.appliers.clear();
        f.transports.clear();
        f.runtimes.clear();
        for n in 0..3 {
            f.cluster.voters[n].log.close().await?;
            let number = n as u8 + 1;
            let log = f
                .cluster
                .domain
                .run(ReplicaLog::open(
                    ReplicaLogOptions::new(
                        f.cluster._root.path().join(format!("node-{number}/log")),
                        NodeId::new([number; 16])?,
                        DiskLineage::new([number + 10; 16])?,
                        ProcessGeneration::new([21; 16])?,
                        LogOpenMode::Resume,
                    ),
                    Arc::new(fault::FaultFileSystem {
                        gate: f.cluster.voters[n].gate.clone(),
                    }),
                ))
                .await??;
            f.cluster.voters[n].log = log;
        }

        // Reopen only the engine-selected complete closure, then register with
        // empty controllers; registration alone still grants no quorum authority.
        for n in 0..3 {
            let (runtime, events) = CellGroupRuntime::start(
                f.cluster.voters[n].log.clone(),
                RaftRuntimeOptions {
                    tick_interval_ms: None,
                    election_seed: Some([71; 32]),
                    ..Default::default()
                },
            )
            .await?;
            f.transports
                .push(super::super::tcp::start(&runtime, n).await?);
            f.runtimes.push(runtime);
            f.events.push(events);
        }
        prepared.inputs = recover(&f, &prepared, &a).await?;
        let mut controllers = vec![];
        for (n, events) in f.events.drain(..).enumerate() {
            let pool = CellGroupSqlPool::start(
                &f.cluster.voters[n].log,
                CellSqlLimits {
                    workers: 1,
                    queue_per_worker: 1,
                    max_pending_cells: 1,
                    group_pending_cells: 1,
                    node_bytes: 1024 * 1024 * 1024,
                    group_bytes: 512 * 1024 * 1024,
                },
            )?;
            f.appliers.insert(
                (n, group),
                (pool.clone(), prepared.inputs[n].applier.clone()),
            );
            let c = CellGroupController::start(
                f.runtimes[n].clone(),
                events,
                f.transports[n].clone(),
                pool,
                vec![],
                controller::limits(),
            )
            .await?;
            assert_eq!(c.usage().groups, 0);
            controllers.push(c);
        }
        for (n, input) in prepared.inputs.drain(..).enumerate() {
            controllers[n].register(input)?.response().await?;
        }
        assert!(matches!(
            f.runtimes[0].execution_authority(group, cell.clone()).await,
            Err(RuntimeError::NotLeader)
        ));
        f.runtimes[0].campaign(group).await?;
        wait_for(async || Ok(f.runtimes[0].status(group).await?.ready)).await?;
        mutate(&f, &controllers[0], &dispatch, &class, &cell, retry).await?;
        let deadline = f
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 5_000 })
            .await?;
        let authority = f.runtimes[0]
            .read_authority(
                group,
                cell.clone(),
                CellReadConsistency::Linearizable,
                deadline,
            )
            .await?;
        let output = f.appliers[&(0, group)]
            .0
            .query(f.cell_options(&cell).await?, authority, |db, version| {
                assert_eq!(version.sequence.get(), 2);
                let n: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                Ok(n.to_le_bytes().to_vec())
            })
            .await?;
        assert_eq!(output.result(), &8_i64.to_le_bytes());
        drop(output);
        for c in controllers {
            c.close().await?;
        }
        f.close().await?;
    }
    Ok(())
}
