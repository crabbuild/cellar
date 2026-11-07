// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real receiver commands and signatures; only the source-side reply is lost.
use super::*;
use cellar::application::consensus::{
    RaftDeliveryRequest, RaftEffectOptions, RaftEffectSigningKey, RaftEffectSink,
    RaftEffectSourceGrant, RaftEffectTarget, RaftInbox, RaftOutbox, RaftOutboxLimits,
};
use futures_util::future::BoxFuture;
use std::future::Future;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex, Weak,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    LostReply,
    LeaderChange,
    Shutdown,
}

struct State {
    mode: Mode,
    arrivals: AtomicUsize,
    first: Mutex<Option<(CommandId, Vec<u8>)>>,
    first_proof: Mutex<Option<Vec<u8>>>,
    changed: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    dropped: AtomicBool,
    polls: AtomicUsize,
}

struct Endpoint {
    controllers: Vec<CellGroupController>,
    dispatcher: CellGroupDispatcher,
    options: Vec<CellSqlOptions>,
    leader: AtomicUsize,
    runtimes: Vec<CellGroupRuntime>,
    group: GroupHistory,
    source: CellHistory,
}

struct Routing {
    endpoint: Mutex<Weak<Endpoint>>,
    state: Arc<State>,
}

struct PendingDelivery(Arc<State>);

impl Drop for PendingDelivery {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
    }
}

impl RaftEffectSink for Routing {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, Result<Vec<u8>, CommandFailure>> {
        let endpoint = self.endpoint.lock().unwrap().upgrade();
        let state = self.state.clone();
        Box::pin(async move {
            let endpoint = endpoint.ok_or(CommandFailure::Unavailable)?;
            let attempt = state.arrivals.fetch_add(1, Ordering::SeqCst);
            {
                let mut first = state.first.lock().unwrap();
                if let Some((id, bytes)) = first.as_ref() {
                    if *id != request.command_id() || bytes != request.encoded() {
                        state.changed.store(true, Ordering::SeqCst);
                        return Err(CommandFailure::CommandConflict);
                    }
                } else {
                    *first = Some((request.command_id(), request.encoded().to_vec()));
                }
            }
            let node = endpoint.leader.load(Ordering::SeqCst);
            if request.target().durability() == Durability::S3 {
                let selected = endpoint.runtimes[node]
                    .reconciliation_authority(endpoint.group, endpoint.source.clone())
                    .await
                    .map_err(|_| CommandFailure::Unavailable)?;
                let published = endpoint.controllers[node]
                    .archive_status(endpoint.group)
                    .map_err(|_| CommandFailure::Unavailable)?
                    .and_then(|status| status.published)
                    .ok_or(CommandFailure::Unavailable)?;
                assert_eq!(selected.cell().effects.pending_outbox, 1);
                assert!(
                    published.position().index >= selected.cell().last_mutation.index,
                    "source claim must have published S3 coverage before send"
                );
            }
            let dispatcher = endpoint
                .dispatcher
                .clone()
                .with_s3(&endpoint.controllers[node])
                .map_err(|_| CommandFailure::Unavailable)?;
            let invocation = dispatcher
                .command(
                    request.target().scope().clone(),
                    CellCommandRequest {
                        id: request.command_id(),
                        contract: request.target().contract().clone(),
                        input: request.encoded().to_vec(),
                        durability: request.target().durability(),
                        retry_until_ms: cellar::asyncrt::wall_ms().max(0) as u64 + 60_000,
                    },
                    CellRoutingLimits {
                        timeout: Duration::from_secs(5),
                        max_attempts: 4,
                    },
                    None,
                )
                .await
                .map_err(|_| CommandFailure::Unavailable)?;
            let receipt = endpoint.controllers[node]
                .command(invocation, endpoint.options[node].clone())
                .map_err(|_| CommandFailure::Unavailable)?
                .response()
                .await
                .map_err(|_| CommandFailure::Unavailable)?;
            assert_eq!(receipt.durability(), request.target().durability());
            let proof = receipt
                .local_receipt()
                .expect("local command receipt")
                .receipt
                .result()
                .to_vec();
            // The adapter has consumed the typed receiver result into bounded
            // reply bytes. A simulated lost network reply must not retain the
            // receiver's SQL operation slot while another cell runs.
            drop(receipt);
            request
                .verify_receipt(&proof)
                .map_err(|_| CommandFailure::CommandConflict)?;
            if attempt == 0 {
                *state.first_proof.lock().unwrap() = Some(proof.clone());
                state.entered.notify_one();
                if state.mode == Mode::LostReply {
                    return Err(CommandFailure::Unavailable);
                }
                let _pending = PendingDelivery(state.clone());
                let released = state.release.notified();
                tokio::pin!(released);
                std::future::poll_fn(|cx| {
                    state.polls.fetch_add(1, Ordering::SeqCst);
                    released.as_mut().poll(cx)
                })
                .await;
            }
            Ok(proof)
        })
    }
}

pub(crate) async fn lost_reply() -> Result<()> {
    run(Mode::LostReply).await
}

pub(crate) async fn leader_change() -> Result<()> {
    run(Mode::LeaderChange).await
}

pub(crate) async fn shutdown() -> Result<()> {
    run(Mode::Shutdown).await
}

#[derive(Debug, PartialEq, Eq)]
struct DeliveryObservation {
    command: CommandId,
    bytes: Vec<u8>,
    proof: Vec<u8>,
}

pub(crate) async fn identity_across_bindings() -> Result<()> {
    // Independently boot both physical bindings through real Raft and SQL.
    // Each loses one receiver reply, then proves a single receiver mutation.
    // A live transfer's seal/publication protocol belongs to CG-09.
    for rooted in [false, true] {
        let before = Box::pin(run_image(
            Mode::LostReply,
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            Durability::Quorum,
            GroupId::new([3; 16])?,
        ))
        .await?;
        let after = Box::pin(run_image(
            Mode::LostReply,
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            Durability::Quorum,
            GroupId::new([31; 16])?,
        ))
        .await?;
        assert_eq!(
            before, after,
            "group binding changed logical delivery identity or proof"
        );
    }
    Ok(())
}

async fn run(mode: Mode) -> Result<()> {
    for rooted in [false, true] {
        Box::pin(run_image(
            mode,
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
            Durability::Quorum,
            GroupId::new([3; 16])?,
        ))
        .await?;
    }
    Ok(())
}

pub(crate) async fn s3_lost_reply(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    Box::pin(run_image(
        Mode::LostReply,
        rooted,
        store,
        Durability::S3,
        GroupId::new([3; 16])?,
    ))
    .await?;
    Ok(())
}

async fn run_image(
    mode: Mode,
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
    minimum: Durability,
    group: GroupId,
) -> Result<DeliveryObservation> {
    let state = Arc::new(State {
        mode,
        arrivals: AtomicUsize::new(0),
        first: Mutex::new(None),
        first_proof: Mutex::new(None),
        changed: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        dropped: AtomicBool::new(false),
        polls: AtomicUsize::new(0),
    });
    let routing = Arc::new(Routing {
        endpoint: Mutex::new(Weak::new()),
        state: state.clone(),
    });
    let sender = Arc::new(RaftEffectSigningKey::new([211; 32])?);
    let receiver = Arc::new(RaftEffectSigningKey::new([212; 32])?);
    let received = Arc::new(AtomicUsize::new(0));
    let count = received.clone();
    let base = RaftClass::new("Counter", 1, [213; 32])?;
    let definition = base.definition_digest();
    let target_cell = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:registered-b")?,
        incarnation: IncarnationId::new([8; 16])?,
    };
    let receive = OperationContract {
        operation: "Receive".into(),
        kind: OperationKind::Command,
        digest: [214; 32],
    };
    let target = RaftEffectTarget::new(
        target_cell.clone(),
        definition,
        receive.clone(),
        receiver.verification_key(),
        minimum,
    )?;
    let inbox = RaftInbox::new(
        vec![RaftEffectSourceGrant::new(
            target_cell.cluster,
            target_cell.namespace,
            "Counter",
            "Events",
            [215; 32],
            sender.verification_key(),
        )?],
        receiver,
        minimum,
        4,
    )?;
    let outbox = RaftOutbox::new(
        sender,
        RaftOutboxLimits {
            retry_delay_ms: 100,
            ..Default::default()
        },
    )?
    .binding("Events", [215; 32], target, routing.clone())?;
    let class = Arc::new(
        base.inbox("Receive", receive.digest, inbox, move |context, input| {
            count.fetch_add(1, Ordering::SeqCst);
            let amount = i64::from_be_bytes(input.try_into()?);
            context.sql().execute("UPDATE value SET n=n+?1", [amount])?;
            Ok(amount.to_be_bytes().to_vec())
        })?
        .outbox(outbox)?
        .command("Emit", [216; 32], |context, input| {
            Ok(context.emit("Events", input)?.to_be_bytes().to_vec())
        })?
        .command("Touch", [217; 32], |context, _| {
            context.sql().execute("UPDATE value SET n=n+100", [])?;
            Ok(Vec::new())
        })?,
    );
    assert_eq!(class.definition_digest(), definition);
    let filesystem = Arc::new(DirectFileSystem);
    let cluster = Cluster::with_store_birth(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        filesystem.clone(),
        class.schema_digest(),
        store,
        ClusterBirthOptions {
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            group,
            minimum,
            log: ClusterLogLimits::default(),
        },
    )
    .await?;
    let (mut fixture, route, source, destination) = Box::pin(registered::setup_cluster_named(
        rooted,
        cluster,
        filesystem,
        ["Counter:registered-a", "Counter:registered-b"],
    ))
    .await?;
    assert_eq!(destination, target_cell);
    let keys = VersionKeys::new(
        VersionSigningKey::new([218; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(30),
    )?;
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        keys,
        CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 64 * 1024,
        },
    )?;
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    let archive = if minimum == Durability::S3 {
        let (archive, _) =
            archive_controller::registration_minimum(&fixture, &route, rooted, minimum).await?;
        controllers[0].archive(archive.clone())?.response().await?;
        archive_controller::published(
            &controllers[0],
            route.history,
            fixture.runtimes[0]
                .status(route.history)
                .await?
                .applied
                .index,
        )
        .await?;
        Some(archive)
    } else {
        None
    };
    let dispatcher = if archive.is_some() {
        dispatcher.with_s3(&controllers[0])?
    } else {
        dispatcher
    };
    let mut destination_options = Vec::new();
    for (node, controller) in controllers.iter().enumerate() {
        let mut resources = BTreeMap::new();
        for cell in [&source, &destination] {
            resources.insert(cell.clone(), fixture.cell_options_at(cell, node).await?);
        }
        destination_options.push(resources[&destination].clone());
        controller.configure_effects(vec![CellGroupEffectRegistration::new(
            dispatcher.clone(),
            RaftEffectOptions::default(),
            move |cell| {
                resources
                    .get(cell)
                    .cloned()
                    .ok_or(ExecutionError::Invalid("effect resources"))
            },
        )?])?;
    }
    let endpoint = Arc::new(Endpoint {
        controllers: controllers.clone(),
        dispatcher: dispatcher.clone(),
        options: destination_options,
        leader: AtomicUsize::new(0),
        runtimes: fixture.runtimes.clone(),
        group: route.history,
        source: source.clone(),
    });
    *routing.endpoint.lock().unwrap() = Arc::downgrade(&endpoint);
    let retry_until = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms().max(0) as u64 + 60_000 })
        .await?;
    let invocation = dispatcher
        .command(
            source.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([219; 16])?, 1)?,
                contract: class.contract("Emit").context("Emit")?,
                input: 7_i64.to_be_bytes().to_vec(),
                durability: minimum,
                retry_until_ms: retry_until,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(5),
                max_attempts: 4,
            },
            None,
        )
        .await?;
    let receipt = controllers[0]
        .command(invocation, fixture.cell_options(&source).await?)?
        .response()
        .await?;
    assert_eq!(
        receipt
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        1_u64.to_be_bytes()
    );
    assert_eq!(receipt.durability(), minimum);
    drop(receipt);
    tokio::time::timeout(Duration::from_secs(5), state.entered.notified()).await?;
    if mode == Mode::LeaderChange {
        let previous_polls = state.polls.load(Ordering::SeqCst);
        let touch = dispatcher
            .command(
                source.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([220; 16])?, 1)?,
                    contract: class.contract("Touch").context("Touch")?,
                    input: Vec::new(),
                    durability: minimum,
                    retry_until_ms: retry_until,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(5),
                    max_attempts: 4,
                },
                None,
            )
            .await?;
        drop(
            controllers[0]
                .command(touch, fixture.cell_options(&source).await?)?
                .response()
                .await?,
        );
        controller::wait_for(async || {
            Ok(state.polls.load(Ordering::SeqCst) >= previous_polls + 2
                || state.dropped.load(Ordering::SeqCst))
        })
        .await?;
        assert!(
            !state.dropped.load(Ordering::SeqCst),
            "a later cell mutation must not revoke a committed dispatch"
        );
        let old_term = fixture.runtimes[0].status(route.history).await?.term;
        endpoint.leader.store(1, Ordering::SeqCst);
        let mut transferred = false;
        controller::wait_for(async || {
            // The healthy leader's lease suppresses ordinary pre-votes.
            // Use the actual TimeoutNow handoff, including its caught-up
            // target check, within this same progress deadline.
            if !transferred {
                match fixture.runtimes[0].transfer_leader(route.history, 2).await {
                    Ok(()) => transferred = true,
                    Err(RuntimeError::NotReady) => return Ok(false),
                    Err(error) => return Err(error.into()),
                }
            }
            let status = fixture.runtimes[1].status(route.history).await?;
            Ok(status.ready && status.term > old_term && state.dropped.load(Ordering::SeqCst))
        })
        .await?;
    } else if mode == Mode::Shutdown {
        controllers[0].close().await?;
        assert!(state.dropped.load(Ordering::SeqCst));
        assert_eq!(received.load(Ordering::SeqCst), 1);
        controller::wait_for(async || {
            for runtime in fixture.runtimes.iter().skip(1) {
                let source_view = runtime
                    .reconciliation_authority(route.history, source.clone())
                    .await?;
                let destination_view = runtime
                    .reconciliation_authority(route.history, destination.clone())
                    .await?;
                if source_view.cell().version.sequence.get() != 3
                    || destination_view.cell().version.sequence.get() != 2
                {
                    return Ok(false);
                }
            }
            Ok(true)
        })
        .await?;
    }
    if mode != Mode::Shutdown {
        controller::wait_for(async || {
            for runtime in &fixture.runtimes {
                let selected = runtime
                    .reconciliation_authority(route.history, source.clone())
                    .await?;
                if selected.cell().effects.pending_outbox != 0 {
                    return Ok(false);
                }
            }
            for controller in &controllers {
                let usage = controller.usage();
                if usage.active_effects != 0 || usage.retained_commands != 0 {
                    return Ok(false);
                }
            }
            for (pool, _) in fixture.appliers.values() {
                let usage = pool.usage()?;
                if usage.active_cells != 0 || usage.retained_operations != 0 {
                    return Ok(false);
                }
            }
            Ok(true)
        })
        .await?;
        assert_eq!(state.arrivals.load(Ordering::SeqCst), 2);
    }
    assert!(!state.changed.load(Ordering::SeqCst));
    assert_eq!(received.load(Ordering::SeqCst), 1);
    for (node, runtime) in fixture
        .runtimes
        .iter()
        .enumerate()
        .skip(usize::from(mode == Mode::Shutdown))
    {
        let source_view = runtime
            .reconciliation_authority(route.history, source.clone())
            .await?;
        assert_eq!(
            source_view.cell().version.sequence.get(),
            match mode {
                Mode::LostReply => 5,
                Mode::LeaderChange => 6,
                Mode::Shutdown => 3,
            }
        );
        assert_eq!(
            source_view.cell().effects.pending_outbox,
            u64::from(mode == Mode::Shutdown)
        );
        let deadline = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 5000 })
            .await?;
        let authority = runtime
            .read_authority(
                route.history,
                destination.clone(),
                CellReadConsistency::Stale,
                deadline,
            )
            .await?;
        let result = fixture.appliers[&(node, route.history)]
            .0
            .query(
                fixture.cell_options_at(&destination, node).await?,
                authority,
                |database, version| {
                    assert_eq!(version.sequence.get(), 2);
                    let value: i64 =
                        database
                            .connection()
                            .query_row("SELECT n FROM value", [], |row| row.get(0))?;
                    Ok(value.to_be_bytes().to_vec())
                },
            )
            .await?;
        assert_eq!(result.result(), 7_i64.to_be_bytes());
    }
    if let Some(archive) = &archive {
        let current = fixture.runtimes[0]
            .status(route.history)
            .await?
            .applied
            .index;
        archive_controller::published(&controllers[0], route.history, current).await?;
        let coverage = archive_controller::verified_current(
            &fixture,
            archive,
            rooted,
            "effect-recovery-audit",
        )
        .await?;
        assert_eq!(coverage.recovery().manifest().pinned_images(), 2);
        for cell in [&source, &destination] {
            let selected = fixture.runtimes[0]
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            let (restored, _) = coverage
                .recovery()
                .manifest()
                .pin_cell(&cell.cell)
                .context("S3 effect cell")?;
            assert_eq!(restored, selected.cell());
        }
    }
    for controller in &controllers {
        controller.close().await?;
        assert_eq!(controller.usage().active_effects, 0);
    }
    for (pool, _) in fixture.appliers.values() {
        assert_eq!(pool.usage()?.active_cells, 0);
        assert_eq!(pool.usage()?.retained_bytes, 0);
    }
    drop(endpoint);
    drop(controllers);
    fixture.close().await?;
    let (command, bytes) = state
        .first
        .lock()
        .unwrap()
        .take()
        .context("first delivery")?;
    let proof = state
        .first_proof
        .lock()
        .unwrap()
        .take()
        .context("receiver proof")?;
    Ok(DeliveryObservation {
        command,
        bytes,
        proof,
    })
}
