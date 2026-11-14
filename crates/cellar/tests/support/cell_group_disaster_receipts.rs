// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered receipts and non-reusable client floors across a cold group seed.
use super::*;
#[path = "cell_group_disaster_public.rs"]
mod public;
use cellar::storage::CommandInputs;
use std::collections::BTreeMap;

pub(in super::super) struct ReceiptWorkload {
    pub(in super::super) class: Arc<RaftClass>,
    calls: Arc<AtomicUsize>,
    now: u64,
    old: BTreeMap<String, CellVersionObservation>,
    results: BTreeMap<(String, u64), Vec<u8>>,
}

impl ReceiptWorkload {
    fn new() -> Result<Self> {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("RecoveryCounter", 1, [84; 32])?.command(
            "Add",
            [85; 32],
            move |context, input| {
                count.fetch_add(1, Ordering::AcqRel);
                context.sql().execute("UPDATE value SET n=n+1", [])?;
                // Include the incarnation in the result: re-execution under the
                // fresh identity could not produce the archived result.
                let mut result = context.identity().cell.as_str().as_bytes().to_vec();
                result.extend_from_slice(&context.identity().incarnation.bytes());
                result.extend_from_slice(input);
                Ok(result)
            },
        )?);
        Ok(Self {
            class,
            calls,
            now: 0,
            old: BTreeMap::new(),
            results: BTreeMap::new(),
        })
    }

    fn request(&self, sequence: u64, input: &[u8]) -> Result<CellCommandRequest> {
        Ok(CellCommandRequest {
            id: CommandId::new(ClientId::new([188; 16])?, sequence)?,
            contract: self.class.contract("Add").context("Add")?,
            input: input.to_vec(),
            durability: Durability::Quorum,
            retry_until_ms: self.now + 300_000,
        })
    }

    pub(in super::super) async fn populate(
        &mut self,
        cluster: &mut Cluster,
        expected: &mut BTreeMap<String, Vec<u8>>,
    ) -> Result<()> {
        self.now = cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
            .await?;
        for (name, sequence) in [("alpha", 1), ("alpha", 2), ("alpha", 3), ("beta", 1)] {
            let cell = history(cluster.birth.route(), name)?;
            let authority = cluster.voters[0]
                .owner
                .as_ref()
                .context("source owner")?
                .execution_authority(&cell)?;
            let voter = &cluster.voters[0];
            let services = ReadServices {
                now_unix_millis: Arc::new(|| 10),
                fill_random: Arc::new(|bytes| bytes.fill(17)),
            };
            let images = match &voter.extents {
                Some(images) => CellGroupApplyImages::Extents {
                    images: images.clone(),
                    services,
                    limits: Default::default(),
                    cache_directory: cluster._root.path().join("node-1/source-query-cache"),
                    cache_pool: ScratchPool::new(ScratchLimits {
                        max_bytes: 2 * EXTENT_BYTES,
                        max_files: 2,
                    })?,
                    cache: CacheLimits {
                        max_cached_bytes: EXTENT_BYTES,
                        max_cached_extents: 1,
                    },
                },
                None => CellGroupApplyImages::Materialized {
                    images: voter.images.clone(),
                    services,
                },
            };
            let mut options = sql_options(
                images,
                cluster
                    ._root
                    .path()
                    .join(format!("node-1/source-sql-{name}")),
                self.class.schema_digest(),
            )?;
            match &mut options {
                CellSqlOptions::Materialized(o) => o.limits.max_retained_receipts = 2,
                CellSqlOptions::Extents(o) => o.limits.max_retained_receipts = 2,
            }
            let input = vec![sequence as u8];
            let mut request = self.request(sequence, &input)?;
            request.durability = if name == "alpha" && sequence >= 2 {
                Durability::S3
            } else {
                Durability::Quorum
            };
            if name == "alpha" && sequence == 1 {
                request.retry_until_ms = self.now - 1000;
            }
            let attempt = CommandAttempt {
                request: CommandRequest {
                    id: request.id,
                    operation: "Add".into(),
                    contract_digest: [85; 32],
                    request_digest: command_request_digest(&input),
                    durability: request.durability,
                    retry_until_ms: request.retry_until_ms,
                },
                proposal: ProposalId::new([100 + self.calls.load(Ordering::Acquire) as u8; 16])?,
                preview: ImageId::new([100 + self.calls.load(Ordering::Acquire) as u8; 16])?,
                inputs: CommandInputs {
                    now_unix_millis: if sequence <= 2 && name == "alpha" {
                        self.now - 2000
                    } else {
                        self.now
                    },
                    random: [17; 32],
                },
            };
            let class = self.class.clone();
            let (executor, prepared) = cluster
                .domain
                .blocking(move || -> Result<_> {
                    let mut executor = open_private(options, authority)?;
                    let CellStageOutcome::Prepared(prepared) =
                        executor.stage_registered(attempt, &class, &input, true)?
                    else {
                        anyhow::bail!("source command must execute once")
                    };
                    executor.mark_submitted(prepared.proposal())?;
                    Ok((executor, prepared))
                })?
                .await??;
            let CellSubmissionOutcome::Accepted(accepted) = cluster.voters[0]
                .owner
                .as_mut()
                .context("source owner")?
                .submit(prepared)?
            else {
                anyhow::bail!("source command refused")
            };
            cluster.drive(false).await?;
            let authority = cluster.voters[0]
                .owner
                .as_ref()
                .context("source owner")?
                .execution_authority(&cell)?;
            assert!(authority.cell().last_mutation.index >= accepted.position.index);
            let receipt = cluster
                .domain
                .blocking(move || -> Result<_> {
                    let mut executor = executor;
                    let receipt = executor.finish(authority)?;
                    executor.close()?;
                    Ok(receipt)
                })?
                .await??;
            self.results
                .insert((name.into(), sequence), receipt.receipt.result().to_vec());
            self.old.insert(name.into(), receipt.observation());
            expected.insert(
                name.into(),
                image_bytes(
                    cluster,
                    cluster.voters[0]
                        .owner
                        .as_ref()
                        .context("source owner")?
                        .manifest(),
                    name,
                )
                .await?,
            );
        }
        assert_eq!(self.calls.load(Ordering::Acquire), 4);
        Ok(())
    }
}

fn sql_options(
    images: CellGroupApplyImages,
    directory: std::path::PathBuf,
    schema: [u8; 32],
) -> Result<CellSqlOptions> {
    let host = cellar_ltx::LtxHost::new(
        || 10,
        |_| Ok(Duration::ZERO),
        |path| async move { std::fs::read(path) },
        |job| async move {
            job();
            Ok(())
        },
    )
    .with_filesystem(Arc::new(DirectFileSystem));
    let mut limits = ExecutionLimits::default();
    // This small workload has a 1 MiB capture contract. The default 16 MiB
    // payload contract alone reserves four buffers, exceeding the fixture's
    // unchanged 64 MiB group SQL budget before receipt lookup can run.
    limits.envelope.max_payload_bytes = 1024 * 1024;
    Ok(match images {
        CellGroupApplyImages::Materialized { images, services } => {
            CellSqlOptions::Materialized(PrivateOptions {
                directory,
                host,
                sqlite_vfs: None,
                images,
                query_services: services,
                application_digest: schema,
                limits,
            })
        }
        CellGroupApplyImages::Extents {
            images,
            services,
            cache,
            ..
        } => CellSqlOptions::Extents(CellSqlRootOptions {
            host,
            sqlite_vfs: None,
            images,
            query_services: services,
            application_digest: schema,
            limits,
            cache_directory: directory.with_extension("read-cache"),
            // The writer and immutable receipt reader may coexist. Neither may
            // borrow the applier's independently admitted single-file cache.
            cache_pool: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })?,
            cache,
        }),
    })
}

fn open_private(
    options: CellSqlOptions,
    authority: CellExecutionAuthority,
) -> Result<CellPrivateExecutor> {
    Ok(match options {
        CellSqlOptions::Materialized(o) => CellPrivateExecutor::open(o, authority)?,
        CellSqlOptions::Extents(o) => {
            let workspace = o.images.create_workspace(
                cell_image_history_digest(&authority.cell().version.history),
                RootPrivateOptions::workspace_bounds(o.limits)?,
            )?;
            CellPrivateExecutor::open_rooted(
                RootPrivateOptions {
                    workspace,
                    host: o.host,
                    sqlite_vfs: o.sqlite_vfs,
                    images: o.images,
                    query_services: o.query_services,
                    application_digest: o.application_digest,
                    limits: o.limits,
                    cache_directory: o.cache_directory,
                    cache_pool: o.cache_pool,
                    cache: o.cache,
                },
                authority,
            )?
        }
    })
}

async fn image_bytes(
    cluster: &Cluster,
    root: &PreparedGroupManifest,
    name: &str,
) -> Result<Vec<u8>> {
    let (_, pin) = root
        .pin_cell(&CellId::new(name)?)
        .context("selected image")?;
    pin_bytes(cluster, pin).await
}

async fn pin_bytes(cluster: &Cluster, pin: CellImagePin) -> Result<Vec<u8>> {
    cluster
        .domain
        .blocking(move || -> Result<_> {
            Ok(match pin {
                CellImagePin::Empty => vec![],
                CellImagePin::Materialized(pin) => pin
                    .open_reader()?
                    .read_exact_at(0, pin.state().length as usize)?,
                CellImagePin::Extents(pin) => {
                    let mut bytes = Vec::new();
                    for ordinal in 0..pin.manifest().geometry().extent_count() {
                        bytes.extend(
                            pin.view()
                                .read_extent(pin.manifest().extent(ordinal).context("extent")?)?,
                        );
                    }
                    bytes
                }
            })
        })?
        .await?
}

fn keys() -> Result<VersionKeys> {
    Ok(VersionKeys::new(
        VersionSigningKey::new([89; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(600),
    )?)
}

async fn controllers_applied(
    voters: &[FreshVoter],
    history: GroupHistory,
    index: LogIndex,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut done = true;
            for voter in voters {
                done &= voter.runtime.status(history).await?.applied.index >= index;
            }
            if done {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?
}

struct Serving {
    controllers: Vec<CellGroupController>,
    transports: Vec<RaftTransport>,
    dispatcher: CellGroupDispatcher,
}

impl Serving {
    async fn start(
        s: &Scenario,
        quorum: &mut FreshQuorum,
        workload: &ReceiptWorkload,
        rooted: bool,
        ordinal: u8,
    ) -> Result<Self> {
        let mut transports = Vec::new();
        for (node, voter) in quorum.voters.iter().enumerate() {
            transports.push(runtime_fixture::tcp::start(&voter.runtime, node).await?);
        }
        let route = cellar::application::consensus::verification::GroupRoute {
            scope: quorum.route.history,
            schema_digest: quorum.route.schema_digest,
            revision: 1,
            peers: quorum
                .voters
                .iter()
                .zip(&transports)
                .enumerate()
                .map(|(node, (voter, transport))| -> Result<_> {
                    Ok(RaftPeer {
                        replica: voter
                            .log
                            .cell_group(&quorum.route.history)?
                            .identity()
                            .clone(),
                        label: format!("node-{}", node + 1),
                        server_name: "localhost".into(),
                        control_address: transport.control_address(),
                        data_address: transport.data_address(),
                        query_address: transport.query_address(),
                    })
                })
                .collect::<Result<_>>()?,
        };
        for transport in &transports {
            transport.install_physical_group(route.clone())?;
        }
        let mut controllers = Vec::new();
        for (voter, transport) in quorum.voters.iter_mut().zip(&transports) {
            controllers.push(
                CellGroupController::start(
                    voter.runtime.clone(),
                    voter.events.take().context("exclusive recovered events")?,
                    transport.clone(),
                    voter.sql.clone(),
                    vec![voter.applier.clone()],
                    CellGroupControllerLimits {
                        max_groups: 1,
                        max_parallel_apply: 1,
                        retry_interval_ms: 5,
                        max_commands: 2,
                        max_command_bytes: 64 * 1024,
                        checkpoints: None,
                    },
                )
                .await?,
            );
        }
        let io = CellGroupArchivePool::new(Default::default())?;
        let birth = s
            .cluster
            .domain
            .run(
                s.cluster
                    .directory
                    .materialize_selected_group(&quorum.route),
            )
            .await??;
        if ordinal == 0 {
            s.cluster
                .directory
                .prepare_group_archive_binding(&s.cluster.domain, &io, &birth)
                .await?
                .commit(&s.cluster.domain)
                .await?;
        } else {
            s.cluster
                .directory
                .open_group_archive(&s.cluster.domain, &io, &birth)
                .await?;
        }
        let verifier = destination_at(
            &s.cluster,
            rooted,
            Arc::new(DirectFileSystem),
            s.cluster
                ._root
                .path()
                .join(format!("fresh-verifier-{ordinal}")),
            quorum.route.clone(),
        )
        .await?;
        controllers[0]
            .archive(CellGroupArchiveRegistration {
                publisher: CellGroupArchivePublisher::LocalReplica,
                directory: s.cluster.directory.clone(),
                birth,
                io,
                owners: CellGroupArchiveUploadPool::new(Default::default())?,
                tails: CellGroupArchiveTailPool::new(Default::default())?,
                checkpoints: CellGroupCheckpointPool::new(checkpoint_fixture::limits())?,
                store: verifier.store,
                scratch: ScratchPool::new(ScratchLimits {
                    max_bytes: 32 * 1024 * 1024,
                    max_files: 1,
                })?,
                scratch_directory: s.cluster._root.path().into(),
            })?
            .response()
            .await?;
        let dispatcher = CellGroupDispatcher::new(
            &s.cluster.domain,
            s.cluster.directory.clone(),
            workload.class.clone(),
            keys()?,
            CellDispatchLimits {
                max_cached_routes: 4,
                route_cache_bytes: 256 * 1024,
                max_invocations: 4,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?
        .with_s3(&controllers[0])?;
        Ok(Self {
            controllers,
            transports,
            dispatcher,
        })
    }

    async fn command(
        &self,
        s: &Scenario,
        quorum: &FreshQuorum,
        w: &ReceiptWorkload,
        name: &str,
        sequence: u64,
        input: &[u8],
    ) -> Result<CellCommandResponse> {
        let cell = fresh_cell(s, quorum, name)?;
        let call = self
            .dispatcher
            .command(
                cell.clone(),
                w.request(sequence, input)?,
                CellRoutingLimits {
                    timeout: Duration::from_secs(15),
                    max_attempts: 4,
                },
                None,
            )
            .await?;
        Ok(self.controllers[0]
            .command(
                call,
                sql_options(
                    quorum.voters[0].images.clone(),
                    quorum.voters[0].base.join(format!("sql-{name}")),
                    quorum.route.schema_digest,
                )?,
            )?
            .response()
            .await?)
    }

    async fn close(self) -> Result<()> {
        for controller in self.controllers {
            controller.close().await?;
        }
        for transport in self.transports {
            transport.close().await;
        }
        Ok(())
    }
}

fn fresh_cell(_s: &Scenario, quorum: &FreshQuorum, name: &str) -> Result<CellHistory> {
    Ok(CellHistory {
        cell: CellId::new(name)?,
        ..quorum.cell.clone()
    })
}

pub(crate) async fn registered_receipts(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    let mut workload = ReceiptWorkload::new()?;
    let mut s =
        Box::new(Box::pin(Scenario::with_workload(rooted, store, Some(&mut workload))).await?);
    let seeds = seed_voters(&mut s, rooted).await?;
    let mut quorum = publish_voters(&mut s, seeds).await?;
    activate_voters(&s, &mut quorum).await?;
    serving_phase(&mut s, &mut quorum, &workload, rooted, 0).await?;
    restart_voters(&s, &mut quorum, rooted).await?;
    serving_phase(&mut s, &mut quorum, &workload, rooted, 1).await?;
    for voter in quorum.voters {
        voter.runtime.close().await?;
        voter.sql.close().await?;
        voter.log.close().await?;
    }
    s.cluster.close().await
}

fn serving_phase<'a>(
    s: &'a mut Scenario,
    quorum: &'a mut FreshQuorum,
    w: &'a ReceiptWorkload,
    rooted: bool,
    ordinal: u8,
) -> futures_util::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let serving = Serving::start(s, quorum, w, rooted, ordinal).await?;
        let now = s
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
            .await?;
        for name in ["alpha", "beta"] {
            let cell = fresh_cell(s, quorum, name)?;
            let old = &w.old[name];
            let old_token = keys()?.issue_cell(old, quorum.route.schema_digest, w.now)?;
            // Prove rejection is due to the history, while the old token remains
            // valid for its original identity at the same real wall time.
            assert_eq!(
                keys()?.verify_cell(
                    &old_token,
                    &old.version.history,
                    quorum.route.schema_digest,
                    now
                )?,
                *old
            );
            assert_eq!(
                keys()?.verify_cell(&old_token, &cell, quorum.route.schema_digest, now),
                Err(VersionError::WrongHistory)
            );
            assert_ne!(old.version.history, cell);
            assert_ne!(old.witness.group, quorum.route.history);
            let sequence = if name == "alpha" { 2 } else { 1 };
            let response = serving
                .command(s, quorum, w, name, sequence, &[sequence as u8])
                .await?;
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .receipt
                    .result(),
                &w.results[&(name.into(), sequence)]
            );
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .version
                    .history,
                cell
            );
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .witness
                    .group,
                quorum.route.history
            );
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .version
                    .sequence
                    .get(),
                if ordinal == 1 && name == "alpha" {
                    2
                } else {
                    1
                }
            );
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .receipt
                    .minimum_durability(),
                if name == "alpha" {
                    Durability::S3
                } else {
                    Durability::Quorum
                }
            );
            assert_eq!(
                response.durability(),
                if name == "alpha" {
                    Durability::S3
                } else {
                    Durability::Quorum
                }
            );
            let response_time = s
                .cluster
                .domain
                .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
                .await?;
            assert_eq!(
                keys()?.verify_cell(
                    response.token(),
                    &cell,
                    quorum.route.schema_digest,
                    response_time
                )?,
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .observation()
            );
        }
        assert_eq!(
            w.calls.load(Ordering::Acquire),
            if ordinal == 0 { 4 } else { 5 }
        );
        let floor = serving
            .command(s, quorum, w, "alpha", 1, &[1])
            .await
            .err()
            .context("restored expired floor")?;
        assert!(matches!(
            floor
                .downcast_ref::<Arc<CellCommandError>>()
                .map(|e| e.cause.as_ref()),
            Some(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::CommandExpired
            )))
        ));
        let conflict = serving
            .command(s, quorum, w, "alpha", 2, b"foreign input")
            .await
            .err()
            .context("restored receipt identity")?;
        assert!(matches!(
            conflict
                .downcast_ref::<Arc<CellCommandError>>()
                .map(|e| e.cause.as_ref()),
            Some(CellDispatchError::Execution(ExecutionError::Refused(
                CommandFailure::CommandConflict
            )))
        ));
        if ordinal == 0 {
            let response = serving.command(s, quorum, w, "alpha", 4, &[4]).await?;
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .version
                    .sequence
                    .get(),
                2
            );
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .version
                    .history,
                quorum.cell
            );
            let index = response
                .local_receipt()
                .expect("local command receipt")
                .position
                .index;
            drop(response);
            controllers_applied(&quorum.voters, quorum.route.history, index).await?;
            let root = quorum.voters[0].applier.history();
            // Read from the actual fresh engine-selected pin; retained restart
            // below must preserve this new image as well as both old receipts.
            let authority = quorum.voters[0]
                .runtime
                .reconciliation_authority(root, quorum.cell.clone())
                .await?;
            s.expected.insert(
                "alpha".into(),
                pin_bytes(&s.cluster, authority.image().clone()).await?,
            );
        } else {
            let response = serving.command(s, quorum, w, "alpha", 4, &[4]).await?;
            assert_eq!(
                response
                    .local_receipt()
                    .expect("local command receipt")
                    .version
                    .sequence
                    .get(),
                2
            );
        }
        assert_eq!(w.calls.load(Ordering::Acquire), 5);
        serving.close().await
    })
}
