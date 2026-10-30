// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit assigned-range provisioning over real engines, files and TLS.
use super::remote_queries::{keys, wait_for};
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
#[path = "raft_catalog_fault.rs"]
#[allow(dead_code)]
mod catalog_fault;
use super::apply_faults::physical;

async fn assignment(
    f: &RuntimeCluster,
    number: u8,
) -> Result<(RangeDirectory, GroupRoute, Arc<catalog_fault::Store>)> {
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
    // No materialized birth, bootstrap permit, page store or selected root has
    // been prepared by the fixture. Production provisioning owns all of them.
    for voter in &f.cluster.voters {
        assert!(matches!(
            voter.log.cell_group(&scope),
            Err(LogError::NotFound)
        ));
    }
    Ok((directory, birth.route().clone(), store))
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

async fn settings(
    f: &RuntimeCluster,
    a: &CellHistory,
    node: usize,
) -> Result<CellGroupProvisioning> {
    let images = match f.cell_options_at(a, node).await? {
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
    let peers = f
        .tcp_route(f.cluster.birth.route().history)?
        .peers
        .iter()
        .map(RaftPeerNode::from)
        .collect();
    Ok(CellGroupProvisioning {
        directory: f.cluster._root.path().join(format!("provision-{node}")),
        filesystem: Arc::new(DirectFileSystem),
        images,
        manifest: ManifestLimits {
            max_cells: 32,
            max_depth: 16,
        },
        entries: GroupEntryLimits::default(),
        max_effect_rows: 100,
        max_pages: 1024,
        max_page_bytes: 16 * 1024 * 1024,
        max_retained_roots: 16,
        peers,
        peer_revision: 1,
        max_preparations: 1,
        max_preparation_bytes: 512 * 1024 * 1024,
        max_parallel_preparations: 1,
        max_waiters: 2,
    })
}

fn pages_path(base: &std::path::Path, h: GroupHistory) -> std::path::PathBuf {
    base.join(format!(
        "{}-{}-{}-{}",
        hex::encode(h.cluster.bytes()),
        hex::encode(h.namespace.bytes()),
        hex::encode(h.group.bytes()),
        hex::encode(h.incarnation.bytes())
    ))
}

pub(crate) async fn provisioning_coalesces_and_serves() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let mut limits = controller::limits();
        limits.max_commands = 1;
        let controllers = controller::start_with_limits(&mut f, limits).await?;
        let (directory, route, store) = assignment(&f, 31).await?;
        let (foreign, _, _) = assignment(&f, 31).await?;
        let (other, other_route, _) = assignment(&f, 41).await?;
        for (node, c) in controllers.iter().enumerate() {
            c.configure_provisioning(settings(&f, &a, node).await?)?;
        }
        assert!(controllers[0]
            .configure_provisioning(settings(&f, &a, 0).await?)
            .is_err());
        store.hold_group_head_reads.store(true, Ordering::Release);
        let first = controllers[0].provision(directory.clone(), route.clone())?;
        wait_for(async || Ok(store.active_group_head_reads.load(Ordering::Acquire) == 1)).await?;
        let second = controllers[0].provision(directory.clone(), route.clone())?;
        wait_for(async || Ok(controllers[0].usage().provision_waiters == 2)).await?;
        assert!(matches!(
            controllers[0].provision(directory.clone(), route.clone()),
            Err(CellGroupControllerError::Runtime(RuntimeError::Overloaded))
        ));
        assert_eq!(controllers[0].usage().pending_registrations, 1);
        assert_eq!(controllers[0].usage().retained_preparations, 1);
        assert_eq!(store.active_group_head_reads.load(Ordering::Acquire), 1);
        let mut second = Box::pin(second.response());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), second.as_mut())
                .await
                .is_err()
        );
        drop(first);
        // Dropping waiters cannot release a still-running physical preparation.
        assert_eq!(controllers[0].usage().retained_preparations, 1);
        store.hold_group_head_reads.store(false, Ordering::Release);
        store.group_head_reads_resume.notify_waiters();
        second.await?;
        wait_for(async || {
            Ok(controllers[0].usage().groups == 2 && controllers[0].usage().provision_waiters == 0)
        })
        .await?;
        assert_eq!(controllers[0].usage().preparation_bytes, 0);
        let rejected = controllers[0]
            .provision(other, other_route)?
            .response()
            .await
            .unwrap_err();
        assert!(matches!(
            rejected.as_ref(),
            CellGroupControllerError::Runtime(RuntimeError::Overloaded)
        ));
        assert!(controllers[0]
            .provision(foreign, route.clone())?
            .response()
            .await
            .is_err());
        let mut wrong = route.clone();
        wrong.schema_digest = [99; 32];
        assert!(controllers[0]
            .provision(directory.clone(), wrong)?
            .response()
            .await
            .is_err());
        for c in &controllers[1..] {
            c.provision(directory.clone(), route.clone())?
                .response()
                .await?;
        }
        controllers[0]
            .provision(directory.clone(), route.clone())?
            .response()
            .await?;
        f.runtimes[0].campaign(route.history).await?;
        wait_for(async || Ok(f.runtimes[0].status(route.history).await?.ready)).await?;
        let dispatch = CellGroupDispatcher::new(
            &f.cluster.domain,
            directory.clone(),
            class.clone(),
            keys(190)?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        let cell = history(&route, "provisioned-cell")?;
        registration::enroll(&f, &controllers[0], &dispatch, &cell).await?;
        wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
        let retry = f
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        registration::mutate(&f, &controllers[0], &dispatch, &class, &cell, retry).await?;
        wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
        registration::mutate(&f, &controllers[0], &dispatch, &class, &cell, retry).await?;
        wait_for(async || Ok(controllers[0].usage().retained_commands == 0)).await?;
        for runtime in &f.runtimes {
            wait_for(async || {
                Ok(runtime
                    .reconciliation_authority(route.history, cell.clone())
                    .await
                    .is_ok_and(|a| a.cell().version.sequence.get() == 2))
            })
            .await?;
        }
        for c in &controllers {
            assert_eq!(c.usage().groups, 2);
            c.close().await?;
            assert_eq!(c.usage().provision_waiters, 0);
        }
        drop(dispatch);
        drop(controllers);
        f.close().await?;
    }
    Ok(())
}

pub(crate) async fn provisioning_retries_retained_owner() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let (directory, route, store) = assignment(&f, 31).await?;
        let mut options = settings(&f, &a, 0).await?;
        let path = pages_path(&options.directory, route.history);
        let faults = physical::Faults::new(path.clone(), None);
        options.filesystem = Arc::new(physical::FaultFileSystem {
            faults: faults.clone(),
        });
        controllers[0].configure_provisioning(options)?;
        store.fail_read.store(true, Ordering::Release);
        assert!(controllers[0]
            .provision(directory.clone(), route.clone())?
            .response()
            .await
            .is_err());
        assert_eq!(controllers[0].usage().pending_registrations, 0);
        assert_eq!(controllers[0].usage().preparation_bytes, 0);
        let fault_store = store.clone();
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            fault_store.fail_read.store(true, Ordering::Release);
        }));
        // Catalog refusal occurs only after physical recovery. The selected page
        // owner remains held and must not be reopened on the next attempt.
        assert!(controllers[0]
            .provision(directory.clone(), route.clone())?
            .response()
            .await
            .is_err());
        assert_eq!(controllers[0].usage().retained_preparations, 1);
        assert_eq!(controllers[0].usage().pending_registrations, 1);
        assert!(cellar_ltx::FileSystem::lock_file(&DirectFileSystem, &path.join("LOCK")).is_err());
        let syncs = faults.synced_paths.lock().unwrap().len();
        controllers[0]
            .provision(directory.clone(), route.clone())?
            .response()
            .await?;
        assert_eq!(
            faults.synced_paths.lock().unwrap().len(),
            syncs,
            "retry must reuse recovered page owner"
        );
        assert_eq!(controllers[0].usage().retained_preparations, 0);
        for c in controllers {
            c.close().await?;
        }
        f.close().await?;
    }
    Ok(())
}

struct Release(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

pub(crate) async fn provisioning_joins_cancelled_close() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let (directory, route, _) = assignment(&f, 31).await?;
        let mut options = settings(&f, &a, 0).await?;
        let faults = physical::Faults::new(pages_path(&options.directory, route.history), None);
        let entered = Arc::new(AtomicBool::new(false));
        let reached = entered.clone();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let release = Release(Some(send));
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            reached.store(true, Ordering::Release);
            receive
                .recv_timeout(Duration::from_secs(15))
                .expect("physical join release");
        }));
        options.filesystem = Arc::new(physical::FaultFileSystem { faults });
        controllers[0].configure_provisioning(options)?;
        let response = controllers[0].provision(directory, route)?;
        wait_for(async || Ok(entered.load(Ordering::Acquire))).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(30), controllers[0].wait())
                .await
                .is_err()
        );
        assert_eq!(controllers[0].usage().retained_preparations, 1);
        assert!(matches!(
            CellGroupRuntime::start(
                f.cluster.voters[0].log.clone(),
                RaftRuntimeOptions::default()
            )
            .await,
            Err(RuntimeError::Storage(LogError::AlreadyExists))
        ));
        drop(response);
        drop(release);
        controllers[0].close().await?;
        let (replacement, events) = CellGroupRuntime::start(
            f.cluster.voters[0].log.clone(),
            RaftRuntimeOptions::default(),
        )
        .await?;
        replacement.close().await?;
        drop(events);
        for c in controllers {
            c.close().await?;
            assert_eq!(c.usage().retained_preparations, 0);
        }
        f.close().await?;
    }
    Ok(())
}

pub(crate) async fn provisioning_recovers_selected_roots() -> Result<()> {
    for rooted in [false, true] {
        let class = class()?;
        let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
        let (directory, route, _) = assignment(&f, 31).await?;
        for (n, c) in controllers.iter().enumerate() {
            c.configure_provisioning(settings(&f, &a, n).await?)?;
            c.provision(directory.clone(), route.clone())?
                .response()
                .await?;
        }
        f.runtimes[0].campaign(route.history).await?;
        wait_for(async || Ok(f.runtimes[0].status(route.history).await?.ready)).await?;
        let dispatch = CellGroupDispatcher::new(
            &f.cluster.domain,
            directory.clone(),
            class.clone(),
            keys(190)?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        let cell = history(&route, "recovered-provision")?;
        registration::enroll(&f, &controllers[0], &dispatch, &cell).await?;
        let retry = f
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        registration::mutate(&f, &controllers[0], &dispatch, &class, &cell, retry).await?;
        for runtime in &f.runtimes {
            wait_for(async || {
                Ok(runtime
                    .reconciliation_authority(route.history, cell.clone())
                    .await
                    .is_ok_and(|a| a.cell().version.sequence.get() == 2))
            })
            .await?;
        }
        for c in controllers {
            c.close().await?;
        }
        reopen_and_verify(&mut f, directory, &route, &cell, &dispatch, &class, retry).await?;
        drop(dispatch);
        f.close().await?;
    }
    Ok(())
}

// Image-store handles remain in this process; the engine/page owners and runtime
// are fresh. This is deliberately not independent-process/power-loss evidence.
async fn reopen_and_verify(
    f: &mut RuntimeCluster,
    directory: RangeDirectory,
    route: &GroupRoute,
    cell: &CellHistory,
    dispatch: &CellGroupDispatcher,
    class: &RaftClass,
    retry: u64,
) -> Result<()> {
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
    let mut controllers = vec![];
    let mut pools = vec![];
    let events = std::mem::take(&mut f.events);
    for (n, events) in events.into_iter().enumerate() {
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
        let c = CellGroupController::start(
            f.runtimes[n].clone(),
            events,
            f.transports[n].clone(),
            pool.clone(),
            vec![],
            controller::limits(),
        )
        .await?;
        assert_eq!(c.usage().groups, 0);
        let options = settings(f, cell, n).await?;
        let marker = f.cluster.voters[n]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("selected marker")?;
        let page = pages_path(&options.directory, route.history).join(format!(
            "{}.page",
            hex::encode(marker.root().digest().context("nonempty root")?)
        ));
        let saved = if n == 0 {
            Some(std::fs::read(&page)?)
        } else {
            None
        };
        c.configure_provisioning(options)?;
        if let Some(bytes) = saved {
            std::fs::remove_file(&page)?;
            assert!(c
                .provision(directory.clone(), route.clone())?
                .response()
                .await
                .is_err());
            assert_eq!(c.usage().groups, 0);
            assert_eq!(
                f.cluster.voters[n]
                    .log
                    .cell_group(&route.history)?
                    .group_manifest()?
                    .as_ref(),
                Some(&marker)
            );
            std::fs::write(&page, bytes)?;
        }
        c.provision(directory.clone(), route.clone())?
            .response()
            .await?;
        controllers.push(c);
        pools.push(pool);
    }
    assert!(matches!(
        f.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await,
        Err(RuntimeError::NotLeader)
    ));
    f.runtimes[0].campaign(route.history).await?;
    wait_for(async || Ok(f.runtimes[0].status(route.history).await?.ready)).await?;
    registration::mutate(f, &controllers[0], dispatch, class, cell, retry).await?;
    let deadline = f
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() + 5_000 })
        .await?;
    let authority = f.runtimes[0]
        .read_authority(
            route.history,
            cell.clone(),
            CellReadConsistency::Linearizable,
            deadline,
        )
        .await?;
    let output = pools[0]
        .query(f.cell_options(cell).await?, authority, |db, version| {
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
    Ok(())
}

pub(crate) async fn provisioning_refusals_preserve_healthy_group() -> Result<()> {
    for rooted in [false, true] {
        for missing_member in [false, true] {
            let class = class()?;
            let (mut f, _, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
            let controllers = controller::start_with_limits(&mut f, controller::limits()).await?;
            let (directory, route, _) = assignment(&f, 31).await?;
            let mut options = settings(&f, &a, 0).await?;
            let incorrect_endpoint = std::net::TcpListener::bind("127.0.0.1:0")?;
            if missing_member {
                options.peers.pop();
            } else {
                options.peers[0].control_address = incorrect_endpoint.local_addr()?;
            }
            controllers[0].configure_provisioning(options)?;
            let error = controllers[0]
                .provision(directory, route.clone())?
                .response()
                .await
                .unwrap_err();
            if missing_member {
                assert!(matches!(
                    error.as_ref(),
                    CellGroupControllerError::Invalid("unconfigured birth member")
                ));
                assert!(matches!(
                    f.cluster.voters[0].log.cell_group(&route.history),
                    Err(LogError::NotFound)
                ));
                assert_eq!(controllers[0].usage().retained_preparations, 0);
            } else {
                assert!(matches!(
                    error.as_ref(),
                    CellGroupControllerError::Transport(TransportError::Identity)
                ));
                assert_eq!(controllers[0].usage().retained_preparations, 1);
            }
            assert!(!controllers[0].usage().closing);
            let dispatch = CellGroupDispatcher::new(
                &f.cluster.domain,
                f.cluster.directory.clone(),
                class.clone(),
                keys(190)?,
                CellDispatchLimits {
                    max_cached_routes: 32,
                    route_cache_bytes: 256 * 1024,
                    max_invocations: 8,
                    retained_bytes: 1024 * 1024,
                    max_input_bytes: 1024,
                },
            )?;
            let retry = f
                .cluster
                .domain
                .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
                .await?;
            registration::mutate(&f, &controllers[0], &dispatch, &class, &a, retry).await?;
            for c in controllers {
                c.close().await?;
            }
            drop(dispatch);
            f.close().await?;
        }
    }
    Ok(())
}
