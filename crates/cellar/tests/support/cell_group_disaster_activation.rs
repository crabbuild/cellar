// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real fresh voters, scheduler, physical apply and topology CAS after all old
//! voter disks are gone. The external fence is still a contract fixture.
use super::*;

struct FreshVoter {
    log: ReplicaLog,
    applier: CellGroupApplier,
    member: u64,
    base: std::path::PathBuf,
    options: ReplicaLogOptions,
    runtime: CellGroupRuntime,
    events: CellGroupRuntimeEvents,
    sql: CellGroupSqlPool,
}

async fn pump(
    voters: &mut [FreshVoter],
    history: GroupHistory,
    messages: &mut VecDeque<Message>,
) -> Result<()> {
    for (ordinal, voter) in voters.iter_mut().enumerate() {
        for _ in 0..64 {
            let Ok(event) = voter.events.try_recv() else {
                break;
            };
            match event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))? {
                CellGroupEffect::Message(message) => messages.push_back(message.clone()),
                CellGroupEffect::Apply(_) => {
                    let work = event.retain_apply().context("recovered apply")?;
                    Box::pin(voter.sql.prepare_apply(&voter.applier, work))
                        .await?
                        .complete(&voter.runtime)
                        .await?;
                }
                CellGroupEffect::Applied { .. } | CellGroupEffect::ReadIndex { .. } => (),
                _ => anyhow::bail!("unexpected recovered event on {ordinal}"),
            }
        }
    }
    for _ in 0..messages.len() {
        let message = messages.pop_front().context("queued fresh message")?;
        let target = voters
            .iter()
            .position(|v| v.member == message.to)
            .context("fresh target member")?;
        match voters[target].runtime.step(history, message.clone()).await {
            Ok(()) => (),
            Err(RuntimeError::NotReady) => messages.push_back(message),
            Err(error) => return Err(error.into()),
        }
    }
    tokio::task::yield_now().await;
    Ok(())
}

async fn applied(voters: &mut [FreshVoter], history: GroupHistory, index: LogIndex) -> Result<()> {
    let mut messages = VecDeque::new();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            pump(voters, history, &mut messages).await?;
            let mut done = true;
            for voter in voters.iter() {
                done &= voter.runtime.status(history).await?.applied.index >= index;
            }
            if done {
                return Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await?
}

// Each cold control phase owns its future. A single inline state machine for
// the entire disk-loss drill otherwise consumes the default test-thread stack
// before nested archive verification can run.
type SeededVoters = (
    Vec<SeededCellGroupRecovery>,
    Vec<ReplicaLog>,
    Vec<(std::path::PathBuf, ReplicaLogOptions)>,
);

struct FreshQuorum {
    voters: Vec<FreshVoter>,
    route: GroupRoute,
    cell: CellHistory,
    authority: Arc<Authority>,
    publication: DirectoryPublication,
}

pub(crate) async fn activate_quorum(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    let mut s = Box::new(Scenario::new(rooted, store).await?);
    let seeds = seed_voters(&mut s, rooted).await?;
    let mut quorum = publish_voters(&mut s, seeds).await?;
    activate_voters(&s, &mut quorum).await?;
    mutate_voters(&mut s, &mut quorum, rooted).await?;
    restart_voters(&s, &mut quorum, rooted).await?;
    for voter in quorum.voters {
        voter.runtime.close().await?;
        voter.sql.close().await?;
        voter.log.close().await?;
    }
    s.cluster.close().await
}

fn seed_voters(
    s: &mut Scenario,
    rooted: bool,
) -> futures_util::future::BoxFuture<'_, Result<SeededVoters>> {
    Box::pin(async move {
        seed::erase_voters(s, rooted).await?;
        let proof = s.fence(&s.cluster.directory, &s.request).await?;
        let route = s.request.replacement().route().clone();
        let pool = CellGroupRecoveryPool::new(Default::default())?;
        let scratch = ScratchPool::new(ScratchLimits {
            max_bytes: 32 * 1024 * 1024,
            max_files: 1,
        })?;
        let mut seeds = Vec::new();
        let mut logs = Vec::new();
        let mut reopen = Vec::new();
        for (ordinal, member) in s.request.replacement().voters().iter().enumerate() {
            assert!(s
                .cluster
                .domain
                .run(
                    s.cluster
                        .directory
                        .publish_group_recovery(&s.request, &s.authority)
                )
                .await?
                .is_err());
            let base = s.cluster._root.path().join(format!("fresh-{ordinal}"));
            let destination = destination_at(
                &s.cluster,
                rooted,
                Arc::new(DirectFileSystem),
                base.join("data"),
                route.clone(),
            )
            .await?;
            let prepared = pool
                .prepare(
                    &s.cluster.domain,
                    proof.clone(),
                    destination.store.clone(),
                    scratch.clone(),
                    s.cluster._root.path().into(),
                )
                .await?;
            let options = ReplicaLogOptions::new(
                base.join("log"),
                member.node,
                DiskLineage::new([210 + ordinal as u8; 16])?,
                ProcessGeneration::new([220; 16])?,
                LogOpenMode::Create,
            );
            let log = s
                .cluster
                .domain
                .run(ReplicaLog::open(
                    options.clone(),
                    Arc::new(DirectFileSystem),
                ))
                .await??;
            reopen.push((base, options));
            let seed = s
                .cluster
                .directory
                .seed_group_recovery(&s.cluster.domain, prepared, &log, &s.authority)
                .await?;
            assert!(s
                .cluster
                .domain
                .run(s.cluster.directory.bootstrap_recovered_group_member(
                    &s.request,
                    &log,
                    Arc::new(Authority::default())
                ))
                .await?
                .is_err());
            if let Some(first) = seeds.first() {
                let first: &SeededCellGroupRecovery = first;
                assert_eq!(seed.manifest().marker(), first.manifest().marker());
                assert_eq!(seed.binding(), first.binding());
            }
            seeds.push(seed);
            logs.push(log);
        }
        assert_eq!(pool.usage().operations, 0);
        drop(proof);
        assert_eq!(s.receivers.usage().sources, 0);
        Ok((seeds, logs, reopen))
    })
}

fn publish_voters(
    s: &mut Scenario,
    seeded: SeededVoters,
) -> futures_util::future::BoxFuture<'_, Result<FreshQuorum>> {
    Box::pin(async move {
        let (seeds, logs, reopen) = seeded;
        let route = s.request.replacement().route().clone();
        s.authority.reject.store(true, Ordering::Release);
        assert!(s
            .cluster
            .domain
            .run(
                s.cluster
                    .directory
                    .publish_group_recovery(&s.request, &s.authority)
            )
            .await?
            .is_err());
        s.authority.reject.store(false, Ordering::Release);
        // Lose the actual head CAS response and all immediate reads. The replacement
        // is selected; retry must recover that exact publication without rebasing.
        s.remote
            .lose_directory_head_and_reads
            .store(true, Ordering::Release);
        assert!(matches!(
            s.cluster
                .domain
                .run(
                    s.cluster
                        .directory
                        .publish_group_recovery(&s.request, &s.authority)
                )
                .await?,
            Err(CatalogError::Unconfirmed)
        ));
        s.remote.outage.store(false, Ordering::Release);
        let publication = s
            .cluster
            .domain
            .run(
                s.cluster
                    .directory
                    .publish_group_recovery(&s.request, &s.authority),
            )
            .await??;
        assert_eq!(
            publication.journal_digest(),
            s.cluster
                .domain
                .run(
                    s.cluster
                        .directory
                        .publish_group_recovery(&s.request, &s.authority)
                )
                .await??
                .journal_digest()
        );
        let birth = s
            .cluster
            .domain
            .run(
                s.cluster
                    .directory
                    .materialize_group_birth(s.request.replacement()),
            )
            .await??;
        assert!(matches!(
            s.cluster
                .domain
                .run(s.cluster.directory.bootstrap_group_member(&birth, &logs[0]))
                .await?,
            Err(CatalogError::Fenced)
        ));
        // The same head still fences ordinary topology while activation is pending.
        assert!(matches!(
            s.cluster
                .domain
                .run(s.cluster.directory.publish(&s.competing))
                .await?,
            Err(CatalogError::Fenced)
        ));
        let cell = seeds[0]
            .manifest()
            .pin_cell(&CellId::new("alpha")?)
            .context("restored alpha")?
            .0
            .version
            .history
            .clone();
        let authority = Arc::new(std::mem::take(&mut s.authority));
        let binding_key = s
            .remote
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .find(|meta| meta.location.as_ref().contains("/recovery-bindings/"))
            .context("recovery binding key")?
            .location;
        let binding_bytes = s.remote.get(&binding_key).await?.bytes().await?;
        s.remote.delete(&binding_key).await?;
        assert!(s
            .cluster
            .domain
            .run(s.cluster.directory.bootstrap_recovered_group_member(
                &s.request,
                &logs[0],
                authority.clone()
            ))
            .await?
            .is_err());
        assert!(matches!(
            s.cluster
                .domain
                .run(s.cluster.directory.bootstrap_group_member(&birth, &logs[0]))
                .await?,
            Err(CatalogError::Fenced)
        ));
        s.remote.put(&binding_key, binding_bytes.into()).await?;
        let mut voters = Vec::new();
        for ((seed, log), (base, options)) in seeds.into_iter().zip(logs).zip(reopen) {
            let permit = s
                .cluster
                .domain
                .run(s.cluster.directory.bootstrap_recovered_group_member(
                    &s.request,
                    &log,
                    authority.clone(),
                ))
                .await??;
            let (runtime, events) = CellGroupRuntime::start(
                log.clone(),
                RaftRuntimeOptions {
                    tick_interval_ms: None,
                    election_seed: Some([231; 32]),
                    ..Default::default()
                },
            )
            .await?;
            runtime.attach(permit, seed.manifest().clone()).await?;
            let sql = CellGroupSqlPool::start(
                &log,
                CellSqlLimits {
                    workers: 1,
                    queue_per_worker: 4,
                    max_pending_cells: 8,
                    group_pending_cells: 8,
                    node_bytes: 128 * 1024 * 1024,
                    group_bytes: 64 * 1024 * 1024,
                },
            )?;
            voters.push(FreshVoter {
                member: seed.group().identity().member.get(),
                applier: seed.applier().clone(),
                base,
                options,
                log,
                runtime,
                events,
                sql,
            });
        }
        Ok(FreshQuorum {
            voters,
            route,
            cell,
            authority,
            publication,
        })
    })
}

fn activate_voters<'a>(
    s: &'a Scenario,
    quorum: &'a mut FreshQuorum,
) -> futures_util::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let FreshQuorum {
            voters,
            route,
            cell,
            authority,
            publication,
        } = quorum;
        let history = route.history;
        let grant = s
            .cluster
            .domain
            .run(s.cluster.directory.prepare_group_recovery_activation(
                &s.request,
                &voters[0].log,
                authority.as_ref(),
            ))
            .await??;
        assert!(matches!(
            voters[0].runtime.activate_recovery(grant).await,
            Err(RuntimeError::NotLeader)
        ));
        voters[0].runtime.campaign(history).await?;
        applied(voters, history, LogIndex::new(2)).await?;
        let status = voters[0].runtime.status(history).await?;
        assert_eq!(status.leader, Some(11));
        assert!(status.term.get() > 1 && !status.ready);
        assert!(matches!(
            voters[0]
                .runtime
                .execution_authority(history, cell.clone())
                .await,
            Err(RuntimeError::NotReady)
        ));
        let deadline = s
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 5000 })
            .await?;
        let pending_read = voters[0]
            .runtime
            .read_authority(history, cell.clone(), CellReadConsistency::Stale, deadline)
            .await;
        assert!(
            matches!(&pending_read, Err(RuntimeError::NotReady)),
            "pending recovery read refusal: {:?}",
            pending_read.as_ref().err()
        );
        let grant = s
            .cluster
            .domain
            .run(s.cluster.directory.prepare_group_recovery_activation(
                &s.request,
                &voters[0].log,
                authority.as_ref(),
            ))
            .await??;
        assert!(matches!(
            voters[1].runtime.activate_recovery(grant).await,
            Err(RuntimeError::StaleCompletion)
        ));
        let grant = s
            .cluster
            .domain
            .run(s.cluster.directory.prepare_group_recovery_activation(
                &s.request,
                &voters[0].log,
                authority.as_ref(),
            ))
            .await??;
        let CellGroupRecoveryActivation::Accepted(proposal) =
            voters[0].runtime.activate_recovery(grant).await?
        else {
            anyhow::bail!("activation skipped replication")
        };
        let grant = s
            .cluster
            .domain
            .run(s.cluster.directory.prepare_group_recovery_activation(
                &s.request,
                &voters[0].log,
                authority.as_ref(),
            ))
            .await??;
        let CellGroupRecoveryActivation::Accepted(retry) =
            voters[0].runtime.activate_recovery(grant).await?
        else {
            anyhow::bail!("activation committed without network")
        };
        assert_eq!(proposal.position, retry.position);
        assert_eq!(proposal.digest, retry.digest);
        assert!(!voters[0].runtime.status(history).await?.ready);
        applied(voters, history, proposal.position.index).await?;
        assert!(voters[0].runtime.status(history).await?.ready);
        voters[0]
            .runtime
            .execution_authority(history, cell.clone())
            .await?;
        let grant = s
            .cluster
            .domain
            .run(s.cluster.directory.prepare_group_recovery_activation(
                &s.request,
                &voters[0].log,
                authority.as_ref(),
            ))
            .await??;
        let CellGroupRecoveryActivation::Applied(acknowledgement) =
            voters[0].runtime.activate_recovery(grant).await?
        else {
            anyhow::bail!("activation not selected")
        };
        s.remote
            .lose_directory_head_and_reads
            .store(true, Ordering::Release);
        assert!(matches!(
            s.cluster
                .domain
                .run(s.cluster.directory.complete_group_recovery(
                    &s.request,
                    &acknowledgement,
                    authority.as_ref()
                ))
                .await?,
            Err(CatalogError::Unconfirmed)
        ));
        s.remote.outage.store(false, Ordering::Release);
        assert_eq!(
            s.cluster
                .domain
                .run(s.cluster.directory.complete_group_recovery(
                    &s.request,
                    &acknowledgement,
                    authority.as_ref()
                ))
                .await??
                .journal_digest(),
            publication.journal_digest()
        );
        assert_eq!(
            s.cluster
                .domain
                .run(s.cluster.directory.complete_group_recovery(
                    &s.request,
                    &acknowledgement,
                    authority.as_ref()
                ))
                .await??
                .journal_digest(),
            publication.journal_digest()
        );
        drop(acknowledgement);
        Ok(())
    })
}

fn mutate_voters<'a>(
    s: &'a mut Scenario,
    quorum: &'a mut FreshQuorum,
    rooted: bool,
) -> futures_util::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let FreshQuorum {
            voters,
            route,
            cell,
            authority,
            ..
        } = quorum;
        let history = route.history;
        // Exercise a real cell-addressed LTX mutation on the restored group, through
        // the bounded production physical applier, in both image representations.
        let before = &s.expected["alpha"];
        let after = images::image(Some(before), "UPDATE value SET n=n+1")?;
        let cut = images::Cut::new(before, &after, 1, true, false);
        let bytes = request(route, cell.clone(), 2, &cut)?;
        let mut entry = CellGroupEntry::decode(&bytes, route, GroupEntryLimits::default())?;
        if rooted {
            let GroupCommand::Mutation(mutation) = &mut entry.command else {
                anyhow::bail!("mutation")
            };
            mutation.base.commitment =
                checkpoint_fixture::extent_commitment(route, cell, cut.base, before)?;
            mutation.result.commitment =
                checkpoint_fixture::extent_commitment(route, cell, cut.result, &after)?;
        }
        let proposal = voters[0]
            .runtime
            .propose(history, entry.encode(route, GroupEntryLimits::default())?)
            .await?;
        applied(voters, history, proposal.position.index).await?;
        s.expected.insert("alpha".into(), after);
        for voter in voters.iter() {
            let applier = voter.applier.clone();
            let selected = s
                .cluster
                .domain
                .blocking(move || applier.recover_selected())?
                .await??;
            assert!(selected.marker().is_active());
            assert_eq!(
                selected
                    .pin_cell(&cell.cell)
                    .context("mutated cell")?
                    .0
                    .version
                    .sequence
                    .get(),
                2
            );
            tail_fixture::check_images(&s.cluster, &selected, &s.expected).await?;
            s.cluster
                .domain
                .run(s.cluster.directory.bootstrap_recovered_group_member(
                    &s.request,
                    &voter.log,
                    authority.clone(),
                ))
                .await??;
        }
        Ok(())
    })
}

fn restart_voters<'a>(
    s: &'a Scenario,
    quorum: &'a mut FreshQuorum,
    rooted: bool,
) -> futures_util::future::BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        let FreshQuorum {
            voters,
            route,
            cell,
            authority,
            ..
        } = quorum;
        let history = route.history;
        let mut reopen = Vec::new();
        for voter in voters.drain(..) {
            voter.runtime.close().await?;
            voter.sql.close().await?;
            voter.log.close().await?;
            let mut options = voter.options.clone();
            options.mode = LogOpenMode::Resume;
            options.process = ProcessGeneration::new([240; 16])?;
            reopen.push((voter.base.clone(), options));
            drop(voter);
        }
        for (base, options) in reopen {
            let log = s
                .cluster
                .domain
                .run(ReplicaLog::open(
                    options.clone(),
                    Arc::new(DirectFileSystem),
                ))
                .await??;
            let group = log.cell_group(&history)?;
            let target = base.join("data");
            let (applier, root) = s
                .cluster
                .domain
                .blocking(move || reopen_applier(group, target, rooted))?
                .await??;
            assert!(root.marker().is_active());
            tail_fixture::check_images(&s.cluster, &root, &s.expected).await?;
            let permit = s
                .cluster
                .domain
                .run(s.cluster.directory.bootstrap_recovered_group_member(
                    &s.request,
                    &log,
                    authority.clone(),
                ))
                .await??;
            let member = permit.group().identity().member.get();
            let (runtime, events) = CellGroupRuntime::start(
                log.clone(),
                RaftRuntimeOptions {
                    tick_interval_ms: None,
                    election_seed: Some([241; 32]),
                    ..Default::default()
                },
            )
            .await?;
            runtime.attach(permit, root).await?;
            assert!(!runtime.status(history).await?.ready);
            let sql = CellGroupSqlPool::start(
                &log,
                CellSqlLimits {
                    workers: 1,
                    queue_per_worker: 4,
                    max_pending_cells: 8,
                    group_pending_cells: 8,
                    node_bytes: 128 * 1024 * 1024,
                    group_bytes: 64 * 1024 * 1024,
                },
            )?;
            voters.push(FreshVoter {
                log,
                applier,
                member,
                runtime,
                events,
                sql,
                base,
                options,
            });
        }
        let old_term = voters[0].runtime.status(history).await?.term;
        let before = voters[0].runtime.status(history).await?.applied.index;
        voters[0].runtime.campaign(history).await?;
        applied(voters, history, LogIndex::new(before.get() + 1)).await?;
        assert!(voters[0].runtime.status(history).await?.ready);
        assert!(voters[0].runtime.status(history).await?.term > old_term);
        voters[0]
            .runtime
            .execution_authority(history, cell.clone())
            .await?;
        Ok(())
    })
}

fn reopen_applier(
    group: cellar::application::consensus::verification::GroupLog<GroupHistory>,
    base: std::path::PathBuf,
    rooted: bool,
) -> Result<(CellGroupApplier, PreparedGroupManifest)> {
    let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(DirectFileSystem);
    let pages = ManifestPageStore::open(
        &base.join("pages"),
        filesystem.clone(),
        1024,
        16 * 1024 * 1024,
        16,
    )?;
    let services = ReadServices {
        now_unix_millis: Arc::new(|| 10),
        fill_random: Arc::new(|bytes| bytes.fill(17)),
    };
    let images = if rooted {
        CellGroupApplyImages::Extents {
            images: cellar_ltx::internal::NodeExtentStore::open(
                base.join("extents"),
                filesystem,
                Default::default(),
            )?,
            services,
            limits: Default::default(),
            cache_directory: base.join("cache"),
            cache_pool: ScratchPool::new(ScratchLimits {
                max_bytes: 32 * 1024 * 1024,
                max_files: 1,
            })?,
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        }
    } else {
        CellGroupApplyImages::Materialized {
            images: GenerationStore::open(base.join("images"), filesystem, Default::default())?,
            services,
        }
    };
    Ok(CellGroupApplier::recover(
        group,
        pages,
        images,
        ManifestLimits::default(),
        GroupEntryLimits::default(),
        100,
    )?)
}
