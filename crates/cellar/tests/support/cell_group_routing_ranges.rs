// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Live split publications preserve unrelated cached routes and logical cells.
use super::*;
use cellar::application::consensus::{
    verification::cell_groups::{cell_routing_key, GroupRoute, TransitionPhase},
    CellGroupRecoveryPool, NamespaceQueryOrigin,
};
use cellar::sdk::fleet_operations::*;

const NAMES: [&str; 6] = ["one", "two", "three", "four", "five", "six"];

fn definition(callbacks: Arc<AtomicUsize>) -> Result<RaftClass> {
    counter::definition_with_callback(move || {
        callbacks.fetch_add(1, Ordering::AcqRel);
    })
}

fn history(group: GroupHistory, name: &str) -> Result<CellHistory> {
    ensure!(NAMES.contains(&name), "counter cell is not authorized");
    Ok(CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: CellId::new(format!("Counter:{name}"))?,
        incarnation: IncarnationId::new([4; 16])?,
    })
}

#[path = "cell_group_routing_old_lookup.rs"]
mod old_lookup;

#[derive(Clone, Copy, Debug)]
enum Case {
    Overlap,
    OldLookup(old_lookup::Caller),
}

pub(super) async fn split_repairs_only_overlap() -> Result<()> {
    run(Case::Overlap).await
}

pub(super) async fn old_lookup_cannot_resurrect_parent() -> Result<()> {
    for caller in [
        old_lookup::Caller::Sdk,
        old_lookup::Caller::Native,
        old_lookup::Caller::Axum,
    ] {
        run(Case::OldLookup(caller)).await?;
    }
    Ok(())
}

async fn run(case: Case) -> Result<()> {
    for rooted in [false, true] {
        let callbacks = Arc::new(AtomicUsize::new(0));
        let first = Fixture::with_store(
            rooted,
            definition(callbacks.clone())?,
            Arc::new(fault::Store::default()),
        )
        .await?;
        let group = GroupHistory {
            cluster: first.declaration.namespace.cluster,
            namespace: first.declaration.namespace.namespace,
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([7; 16])?,
        };
        let mut apis = Vec::new();
        let mut applications = Vec::new();
        for _ in 0..7 {
            let class = definition(callbacks.clone())?;
            let api =
                counter::CounterApi::with_history_resolver(http::secret(), &class, move |name| {
                    history(group, name)
                })?;
            applications.push(counter::application(api.clone(), class)?);
            apis.push(api);
        }
        // Six data nodes provide overlap reservations without changing the
        // original two-history or byte limits. Node seven is only an ingress.
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            first,
            &[7, 8, 9, 10],
            ArchiveSetup::Initialized,
            true,
            Some(applications),
            FirstGroupSetup::Operator,
        ))
        .await?;
        let nodes: Vec<_> = std::mem::take(&mut cluster.nodes)
            .into_iter()
            .map(Some)
            .collect();
        let mut workers = Vec::new();
        let mut roots = Vec::new();
        let mut web = None;
        let work = async {
            movement::discover(&cluster).await?;
            for (index, host) in cluster.hosts.iter().enumerate() {
                *apis[index].cells.lock().unwrap() = Some(host.clone());
                for peer in &cluster.peers {
                    if peer.node != host.identity().node {
                        host.grant_command_origin(NamespaceCommandOrigin {
                            cluster: group.cluster,
                            namespace: group.namespace,
                            revision: 1,
                            peer: peer.clone(),
                        })?;
                    }
                }
                if index != INGRESS {
                    host.grant_query_origin(NamespaceQueryOrigin {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        revision: 1,
                        peer: cluster.peers[INGRESS].clone(),
                    })?;
                }
            }
            web = Some(http::Web::start(&nodes, &apis).await?);
            match case {
                Case::Overlap => {
                    Box::pin(exercise(
                        &cluster,
                        web.as_ref().context("live range HTTP owners")?,
                        &callbacks,
                        rooted,
                        &mut workers,
                        &mut roots,
                    ))
                    .await
                }
                Case::OldLookup(caller) => {
                    Box::pin(old_lookup::exercise(
                        &cluster,
                        web.as_ref().context("live range HTTP owners")?,
                        &callbacks,
                        rooted,
                        &mut workers,
                        &mut roots,
                        caller,
                    ))
                    .await
                }
            }
        }
        .await;
        if work.is_err() {
            crate::managed_fixture::diagnostics::record(&cluster, "live-range-routing").await;
            for (index, worker) in workers.iter().enumerate() {
                eprintln!("range worker {index}: {:?}", worker.status());
            }
        }
        for worker in workers {
            worker.shutdown().await;
        }
        let http_joined = match web {
            Some(web) => web.close().await,
            None => Ok(()),
        };
        let joined = movement::close(nodes.into_iter().flatten().collect(), &cluster.hosts).await;
        for api in apis {
            *api.cells.lock().unwrap() = None;
        }
        if work.is_err() || joined.is_err() || http_joined.is_err() {
            roots.append(&mut cluster.roots);
            for root in roots {
                eprintln!("retained range-publication disk: {}", root.keep().display());
            }
        }
        cluster.fixtures[0].domain.shutdown().await;
        work.with_context(|| {
            format!("live split routing case={case:?}, extents={rooted}; close={joined:?}; HTTP={http_joined:?}")
        })?;
        http_joined?;
        joined?;
    }
    Ok(())
}

async fn control(
    cluster: &ManagedCluster,
    rooted: bool,
    observer: usize,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<RaftFleetControl> {
    let root = tempfile::tempdir()?;
    let f = &cluster.fixtures[0];
    let verifier =
        crate::managed_fixture::verifier::open(f, root.path(), rooted, Default::default()).await?;
    roots.push(root);
    Ok(RaftFleetControl::new(
        &f.domain,
        Fixture::open_catalog(&f.domain, f.store.clone(), cluster.history.cluster)?,
        vec![cluster.directory.clone()],
    )?
    .with_transition_preparer(verifier, CellGroupRecoveryPool::new(Default::default())?)
    .with_node_observer(cluster.hosts[observer].clone()))
}

fn group(history: GroupHistory) -> RaftFleetTransitionGroup {
    RaftFleetTransitionGroup {
        group_id: hex::encode(history.group.bytes()),
        incarnation_id: hex::encode(history.incarnation.bytes()),
    }
}

async fn split(
    cluster: &ManagedCluster,
    coordinator: &RaftFleetControl,
    workers: &[RaftFleetTransitionService],
    parent: GroupHistory,
    pivot: &CellHistory,
    placements: [[usize; 3]; 2],
    ordinal: u8,
) -> Result<[GroupRoute; 2]> {
    let namespace = hex::encode(parent.namespace.bytes());
    let basis = coordinator.transition_basis(&namespace, TIMEOUT).await?;
    let first_id = 130 + ordinal * 8;
    let request = RaftFleetTransitionAdmissionRequest {
        catalog_id: basis.catalog_id,
        namespace_id: namespace.clone(),
        operation_id: hex::encode([first_id; 16]),
        request_id: hex::encode([first_id + 1; 16]),
        expected_fleet_revision: basis.fleet_revision,
        expected_directory_digest: basis.directory_digest,
        class: "Counter".into(),
        partition: RaftFleetTransitionPartition::Split {
            parent: group(parent),
            pivot_cell: pivot.cell.as_str().into(),
        },
        children: placements
            .iter()
            .enumerate()
            .map(|(child, voters)| {
                let id = first_id + 2 + child as u8;
                RaftFleetTransitionChild {
                    group: RaftFleetTransitionGroup {
                        group_id: hex::encode([id; 16]),
                        incarnation_id: hex::encode([id; 16]),
                    },
                    request_id: hex::encode([id; 16]),
                    creation_id: hex::encode([id; 16]),
                    voters: std::array::from_fn(|i| RaftFleetFirstGroupVoter {
                        node_id: hex::encode(cluster.fixtures[voters[i]].spec.node.bytes()),
                        member_id: i as u64 + 1,
                    }),
                }
            })
            .collect(),
    };
    let admitted = coordinator
        .admit_transition(
            "range-routing-test",
            &namespace,
            &request.operation_id,
            &request,
            TIMEOUT,
        )
        .await?;
    ensure!(admitted.phase == TransitionPhase::Planned);
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    coordinator
        .authorize_transition_execution(
            "range-routing-test",
            &RaftFleetTransitionExecutionRequest {
                catalog_id: coordinator.catalog_id().into(),
                namespace_id: namespace.clone(),
                operation_id: request.operation_id.clone(),
                request_id: hex::encode([first_id + 4; 16]),
                expected_revision: cut.revision(),
                plan_digest: admitted.plan_digest,
            },
            TIMEOUT,
        )
        .await?;
    // Same preparation bound as the existing automatic-transition fixture;
    // every individual SDK turn retains the original five-second limit.
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let state = coordinator
                .transition_status(&namespace, &request.operation_id, TIMEOUT)
                .await?;
            if state.phase == Some(TransitionPhase::Retired) && !state.fleet_slot_held {
                return Ok::<_, anyhow::Error>(());
            }
            for worker in workers {
                ensure!(!worker.status().needs_operator, "{:?}", worker.status());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("split {ordinal} publication and rooted source settlement"))??;
    let mut routes = Vec::new();
    for child in 0..2 {
        let id = first_id + 2 + child;
        let history = GroupHistory {
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([id; 16])?,
            ..parent
        };
        let birth = cluster.fixtures[0]
            .domain
            .run(cluster.directory.materialize_selected_history(history))
            .await??;
        routes.push(birth.route().clone());
    }
    Ok(routes.try_into().unwrap())
}

struct Cell {
    name: &'static str,
    history: CellHistory,
    original: CommandReceipt,
    latest: CommandReceipt,
    value: i64,
    sequence: u64,
}

fn options(index: usize, sequence: u64, retry: u64) -> Result<CommandOptions> {
    Ok(CommandOptions {
        durability: Durability::S3,
        timeout: TIMEOUT,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([200 + index as u8; 16])?, sequence)?,
            retry,
        )
    })
}

fn directory_lookups(cluster: &ManagedCluster) -> Result<u64> {
    let mut total = 0;
    for host in &cluster.hosts {
        let sample = host.performance();
        ensure!(sample.available && !sample.saturated);
        total += sample
            .counters
            .iter()
            .find(|counter| counter.name == "cell_command_directory_lookups")
            .context("command directory work counter")?
            .value;
    }
    Ok(total)
}

fn route<'a>(routes: &'a [GroupRoute], cell: &CellHistory) -> Result<&'a GroupRoute> {
    let key = cell_routing_key(cell.namespace, &cell.cell);
    routes
        .iter()
        .find(|route| route.range.contains(&key))
        .context("current cell range")
}

struct Ingress<'a> {
    cluster: &'a ManagedCluster,
    web: &'a http::Web,
    client: &'a reqwest::Client,
    retry: u64,
}

impl Ingress<'_> {
    async fn write(
        &self,
        cell: &mut Cell,
        expected: &GroupRoute,
        index: usize,
        adapter: Option<bool>,
    ) -> Result<()> {
        let handle = self.cluster.hosts[INGRESS].cell(cell.history.clone())?;
        let selected = options(index, cell.sequence, self.retry)?;
        if let Some(axum) = adapter {
            let reply = http::Web::send(
                self.client
                    .post(format!(
                        "{}?delta=1",
                        self.web.url(INGRESS, axum, cell.name)
                    ))
                    .header(
                        "x-command-id",
                        format!(
                            "{}:{}",
                            hex::encode(selected.id.client.bytes()),
                            selected.id.sequence()
                        ),
                    )
                    .header("x-retry-until-ms", self.retry)
                    .header("x-write-durability", "s3"),
                format!("split write {} axum={axum}", cell.name),
            )
            .await?
            .success()?;
            http::value(&reply, cell.name, cell.value + 1)?;
            ensure!(
                reply["group"] == hex::encode(expected.history.group.bytes())
                    && reply["durability"] == "s3"
            );
        }
        // For HTTP this is an exact-ID replay, not a replacement mutation. The SDK
        // must recover the same result and independently retain its S3 proof.
        let receipt = handle
            .command(
                &handle.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                selected.clone(),
            )
            .await?;
        ensure!(receipt.id == selected.id && receipt.result == (cell.value + 1).to_be_bytes());
        ensure!(
            receipt.version.history == cell.history
                && receipt.witness.group == expected.history
                && receipt.durability == Durability::S3
        );
        cell.latest = receipt;
        cell.value += 1;
        cell.sequence += 1;
        Ok(())
    }
}

struct Prepared {
    cells: Vec<Cell>,
    order: Vec<usize>,
    coordinator: RaftFleetControl,
    first: [GroupRoute; 2],
    client: reqwest::Client,
    retry: u64,
}

async fn prepare(
    cluster: &ManagedCluster,
    rooted: bool,
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<Prepared> {
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let selected = leader(&cluster.hosts[..3], cluster.history).await?;
    let mut cells = Vec::new();
    for (index, name) in NAMES.into_iter().enumerate() {
        let history = history(cluster.history, name)?;
        let handle = cluster.hosts[selected].cell(history.clone())?;
        handle
            .enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([180 + index as u8; 16])?,
                    durability: Durability::S3,
                },
                TIMEOUT,
            )
            .await?;
        let original = handle
            .command(
                &handle.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                options(index, 1, retry)?,
            )
            .await?;
        ensure!(original.result == 1_i64.to_be_bytes());
        cells.push(Cell {
            name,
            history,
            latest: original.clone(),
            original,
            value: 1,
            sequence: 2,
        });
    }
    // CellKey ordering, rather than caller names, defines each whole-cell cut.
    let mut order: Vec<_> = (0..cells.len()).collect();
    order.sort_by_key(|i| cell_routing_key(cells[*i].history.namespace, &cells[*i].history.cell));
    let coordinator = Box::pin(control(cluster, rooted, 0, roots)).await?;
    for index in [0, 1, 2, 4, 5, 6] {
        let endpoint = Box::pin(control(cluster, rooted, index, roots)).await?;
        workers.push(
            endpoint
                .start_transition_service(
                    &cluster.hosts[index],
                    RaftFleetTransitionServiceOptions {
                        coordinate: index == 4,
                        quiesce_retired_sources: true,
                        reclaim_retired_sources: true,
                        interval: Duration::from_millis(50),
                        retry_max_interval: Duration::from_millis(250),
                        turn_timeout: TIMEOUT,
                    },
                    TIMEOUT,
                )
                .await?,
        );
    }
    let first = Box::pin(split(
        cluster,
        &coordinator,
        workers,
        cluster.history,
        &cells[order[3]].history,
        [[0, 4, 5], [1, 2, 4]],
        0,
    ))
    .await?;
    ensure!(first[0].range.merge(&first[1].range)? == CellRange::all());
    Ok(Prepared {
        cells,
        order,
        coordinator,
        first,
        client,
        retry,
    })
}

async fn exercise(
    cluster: &ManagedCluster,
    web: &http::Web,
    callbacks: &AtomicUsize,
    rooted: bool,
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let Prepared {
        mut cells,
        order,
        coordinator,
        first,
        client,
        retry,
    } = prepare(cluster, rooted, workers, roots).await?;
    let ingress = Ingress {
        cluster,
        web,
        client: &client,
        retry,
    };
    for (index, cell) in cells.iter_mut().enumerate() {
        let expected = route(&first, &cell.history)?;
        ingress.write(cell, expected, index, None).await?;
    }
    let untouched = order[5];
    let second = Box::pin(split(
        cluster,
        &coordinator,
        workers,
        first[0].history,
        &cells[order[1]].history,
        [[0, 1, 6], [2, 5, 6]],
        1,
    ))
    .await?;
    ensure!(second[0].range.merge(&second[1].range)? == first[0].range);
    let current = [second[0].clone(), second[1].clone(), first[1].clone()];
    let before = directory_lookups(cluster)?;
    ingress
        .write(&mut cells[untouched], &first[1], untouched, Some(false))
        .await?;
    ensure!(
        directory_lookups(cluster)? == before,
        "unrelated cached range missed before overlapping repair"
    );
    let repair_before = directory_lookups(cluster)?;
    for index in order[..3].iter().copied() {
        let expected = route(&current, &cells[index].history)?;
        for adapter in [None, Some(false), Some(true)] {
            ingress
                .write(&mut cells[index], expected, index, adapter)
                .await?;
        }
    }
    ensure!(
        directory_lookups(cluster)? > repair_before,
        "changed cached range never repaired"
    );
    let before = directory_lookups(cluster)?;
    ingress
        .write(&mut cells[untouched], &first[1], untouched, Some(true))
        .await?;
    ensure!(
        directory_lookups(cluster)? == before,
        "interval repair evicted an unrelated cached range"
    );
    verify(cluster, web, &client, &cells, &current, retry, callbacks).await?;
    eprintln!("public split routing extents={rooted}: two live RF3 splits; unrelated range retained before and after overlap repair; exact original IDs, S3 receipts and old AtLeast tokens; unchanged two-history/512-MiB profiles");
    Ok(())
}

async fn verify(
    cluster: &ManagedCluster,
    web: &http::Web,
    client: &reqwest::Client,
    cells: &[Cell],
    current: &[GroupRoute; 3],
    retry: u64,
    callbacks: &AtomicUsize,
) -> Result<()> {
    for (index, cell) in cells.iter().enumerate() {
        let expected = route(current, &cell.history)?;
        let handle = cluster.hosts[INGRESS].cell(cell.history.clone())?;
        let replay = handle
            .command(
                &handle.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                options(index, 1, retry)?,
            )
            .await?;
        ensure!(
            replay.id == cell.original.id
                && replay.result == cell.original.result
                && replay.version.history == cell.history
                && replay.durability == Durability::S3
        );
        let read = handle
            .query(
                ProposalId::new([220 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(cell.original.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            read.result == cell.value.to_be_bytes()
                && read.version.history == cell.history
                && read.witness == cell.latest.witness
        );
        let placement = current
            .iter()
            .position(|route| route.history == expected.history)
            .context("selected physical placement")?;
        for (member, voter) in [[0, 1, 6], [2, 5, 6], [1, 2, 4]][placement]
            .into_iter()
            .enumerate()
        {
            let observed = cluster.hosts[voter]
                .group(expected.history)?
                .observe(TIMEOUT)
                .await?;
            ensure!(
                observed.replica.scope == expected.history
                    && observed.replica.member == MemberId::new(member as u64 + 1)?
            );
            let local = cluster.hosts[voter].cell(cell.history.clone())?;
            let read = local
                .query(
                    ProposalId::new([230 + index as u8; 16])?,
                    &local.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(cell.latest.version_token.clone()),
                        routing: QueryRouting::Local,
                        timeout: TIMEOUT,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                read.result == cell.value.to_be_bytes()
                    && read.version.history == cell.history
                    && read.witness == cell.latest.witness
                    && read.served_by == observed.replica.member
                    && read.served_by_node == cluster.hosts[voter].identity()
            );
        }
        for axum in [false, true] {
            let reply = web
                .query(
                    client,
                    (INGRESS, axum),
                    cell.name,
                    "at-least",
                    &serde_json::json!({"version_token": cell.original.version_token.as_str()}),
                    None,
                )
                .await?
                .success()?;
            http::value(&reply, cell.name, cell.value)?;
            ensure!(
                reply["group"] == hex::encode(cell.latest.witness.group.group.bytes())
                    && reply["group_incarnation"]
                        == hex::encode(cell.latest.witness.group.incarnation.bytes())
                    && reply["index"].as_u64() == Some(cell.latest.witness.position.index.get())
                    && reply["term"].as_u64() == Some(cell.latest.witness.position.term.get())
            );
        }
    }
    ensure!(
        i64::try_from(callbacks.load(Ordering::Acquire))?
            == cells.iter().map(|cell| cell.value).sum::<i64>()
    );
    for (index, host) in cluster.hosts.iter().enumerate() {
        let observed = host.observe_node(TIMEOUT).await?;
        ensure!(
            observed.node.profile.group_capacity == 2
                && observed.node.profile.byte_capacity == 512 * 1024 * 1024
                && observed.node.profile.bytes_per_group == 256 * 1024 * 1024
        );
        ensure!(observed.node.groups <= 2);
        if index == INGRESS {
            ensure!(observed.node.groups == 0);
        }
    }
    Ok(())
}
