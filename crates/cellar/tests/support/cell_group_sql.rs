// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The actual cell SQL producer on domain workers with three persisted voters.

use super::*;
use cellar_ltx::internal::{
    CacheLimits, ExtentId, ExtentManifest, ExtentRootReference, Geometry, ImageBinding, ImageRoot,
    NodeExtentStore, EXTENT_BYTES,
};
use cellar_ltx::stream_snapshot::{ScratchLimits, ScratchPool};

fn cell_attempt(tag: u8) -> Result<CommandAttempt> {
    Ok(CommandAttempt {
        request: CommandRequest {
            id: CommandId::new(ClientId::new([81; 16])?, 1)?,
            operation: "cell.increment".into(),
            contract_digest: [6; 32],
            request_digest: [9; 32],
            durability: Durability::Quorum,
            retry_until_ms: 1000,
        },
        proposal: ProposalId::new([tag; 16])?,
        preview: ImageId::new([tag; 16])?,
        inputs: cellar::storage::CommandInputs {
            now_unix_millis: 10,
            random: [17; 32],
        },
    })
}

impl RuntimeCluster {
    async fn private_cell(&self, cell: &CellHistory) -> Result<CellPrivateExecutor> {
        let authority = self.runtimes[0]
            .execution_authority(self.cluster.birth.route().history, cell.clone())
            .await?;
        let options = self.cell_options(cell).await?;
        self.cluster
            .domain
            .blocking(move || match options {
                CellSqlOptions::Materialized(o) => CellPrivateExecutor::open(o, authority),
                CellSqlOptions::Extents(o) => {
                    let workspace = o
                        .images
                        .create_workspace(
                            cell_image_history_digest(&authority.cell().version.history),
                            RootPrivateOptions::workspace_bounds(o.limits)?,
                        )
                        .map_err(|e| ExecutionError::Storage(e.into()))?;
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
                    )
                }
            })?
            .await?
            .map_err(Into::into)
    }

    async fn cell_options(&self, cell: &CellHistory) -> Result<CellSqlOptions> {
        self.cell_options_at(cell, 0).await
    }

    async fn cell_options_at(&self, cell: &CellHistory, node: usize) -> Result<CellSqlOptions> {
        let options = PrivateOptions {
            directory: self
                .cluster
                ._root
                .path()
                .join(format!("private-{node}-{}", cell.cell.as_str())),
            host: cellar_ltx::LtxHost::new(
                || 10,
                |_| Ok(Duration::ZERO),
                |path| async move { std::fs::read(path) },
                |job| async move {
                    job();
                    Ok(())
                },
            )
            .with_filesystem(Arc::new(DirectFileSystem)),
            sqlite_vfs: None,
            images: self.cluster.voters[node].images.clone(),
            query_services: cellar_ltx::generation::ReadServices {
                now_unix_millis: Arc::new(|| 10),
                fill_random: Arc::new(|bytes| bytes.fill(17)),
            },
            application_digest: [6; 32],
            limits: ExecutionLimits::default(),
        };
        let extents = self.cluster.voters[node].extents.clone();
        self.cluster
            .domain
            .blocking(move || -> Result<_> {
                if let Some(store) = extents {
                    let rooted = CellSqlRootOptions {
                        host: options.host,
                        sqlite_vfs: None,
                        images: store,
                        query_services: options.query_services,
                        application_digest: options.application_digest,
                        limits: options.limits,
                        cache_directory: options.directory.with_extension("cache"),
                        cache_pool: ScratchPool::new(ScratchLimits {
                            max_bytes: 2 * EXTENT_BYTES,
                            max_files: 2,
                        })?,
                        cache: CacheLimits {
                            max_cached_bytes: EXTENT_BYTES,
                            max_cached_extents: 1,
                        },
                    };
                    Ok(CellSqlOptions::Extents(rooted))
                } else {
                    Ok(CellSqlOptions::Materialized(options))
                }
            })?
            .await?
    }
}

async fn enrolled_sql_cells(
    rooted: bool,
) -> Result<(RuntimeCluster, GroupRoute, CellHistory, CellHistory)> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    if rooted {
        for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                fixture.cluster._root.path().join(format!("rooted-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }

    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let a = history(&route, "sql-a")?;
    let b = history(&route, "sql-b")?;
    let c = history(&route, "sql-c")?;
    for cell in [&a, &b, &c] {
        let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (0);")?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let bytes = if rooted {
            let manifest = ExtentManifest::new(
                ImageBinding {
                    history: cell_image_history_digest(cell),
                    schema: route.schema_digest,
                    epoch: cut.result.epoch,
                    txid: cut.result.txid.0,
                    checksum: cut.result.checksum,
                },
                Geometry::new(cut.result.page_size, cut.result.length)?,
                &bytes
                    .chunks(EXTENT_BYTES as usize)
                    .map(ExtentId::of)
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            )?;
            let encoded = request(&route, cell.clone(), 1, &cut)?;
            let mut entry = CellGroupEntry::decode(&encoded, &route, GroupEntryLimits::default())?;
            let GroupCommand::Enroll { image, .. } = &mut entry.command else {
                anyhow::bail!("enrollment")
            };
            image.commitment = ImageCommitment::Extents(ExtentRootReference::new(
                manifest.root(),
                manifest.encode()?.len().try_into()?,
            )?);
            entry.encode(&route, GroupEntryLimits::default())?
        } else {
            request(&route, cell.clone(), 1, &cut)?
        };
        let proposed = fixture.runtimes[0].propose(route.history, bytes).await?;
        fixture.drive(Some(proposed.position.index)).await?;
    }
    Ok((fixture, route, a, b))
}

pub(super) async fn cell_receipts(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    assert!(matches!(
        fixture.runtimes[1]
            .execution_authority(route.history, a.clone())
            .await,
        Err(RuntimeError::NotReady)
    ));
    let mut wrong = a.clone();
    wrong.incarnation = IncarnationId::new([99; 16])?;
    assert!(fixture.runtimes[0]
        .execution_authority(route.history, wrong)
        .await
        .is_err());
    let mut ea = fixture.private_cell(&a).await?;
    let mut eb = fixture.private_cell(&b).await?;
    let (ea_out, pa) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(prepared) = ea.stage(cell_attempt(82)?, true, |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"a-result".to_vec())
            })?
            else {
                anyhow::bail!("new A receipt")
            };
            ea.mark_submitted(prepared.proposal())?;
            Ok((ea, prepared))
        })?
        .await??;
    ea = ea_out;
    let (eb_out, pb) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(prepared) = eb.stage(cell_attempt(83)?, true, |tx| {
                tx.execute("UPDATE value SET n=n+7", [])?;
                Ok(b"b-result".to_vec())
            })?
            else {
                anyhow::bail!("new B receipt")
            };
            eb.mark_submitted(prepared.proposal())?;
            Ok((eb, prepared))
        })?
        .await??;
    eb = eb_out;
    // Both use the same client/sequence, but B's committed image cannot settle A.
    let proposed = fixture.runtimes[0]
        .propose(route.history, pb.bytes().to_vec())
        .await?;
    fixture.drive(Some(proposed.position.index)).await?;
    let cb = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let (ea_out, eb_out) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            assert!(ea.finish(cb).is_err());
            assert!(matches!(
                ea.stage(cell_attempt(84)?, true, |_| panic!("unresolved A callback")),
                Err(ExecutionError::Busy)
            ));
            Ok((ea, eb))
        })?
        .await??;
    ea = ea_out;
    eb = eb_out;
    // A prepared before B committed remains valid because its own base is unchanged.
    let proposed = fixture.runtimes[0]
        .propose(route.history, pa.bytes().to_vec())
        .await?;
    fixture.drive(Some(proposed.position.index)).await?;
    let ca = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let cb = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            let ra = ea.finish(ca)?;
            let rb = eb.finish(cb)?;
            assert_eq!(ra.receipt.result(), b"a-result");
            assert_eq!(rb.receipt.result(), b"b-result");
            assert_eq!(ra.version.sequence.get(), 2);
            assert_eq!(rb.version.sequence.get(), 2);
            assert_ne!(ra.version.history, rb.version.history);
            ea.close()?;
            eb.close()?;
            Ok(())
        })?
        .await??;
    for (cell, expected, value) in [
        (&a, b"a-result".as_slice(), 1),
        (&b, b"b-result".as_slice(), 7),
    ] {
        let mut executor = fixture.private_cell(cell).await?;
        let expected = expected.to_vec();
        fixture
            .cluster
            .domain
            .blocking(move || -> Result<()> {
                let CellStageOutcome::Duplicate(receipt) =
                    executor.stage(cell_attempt(85)?, false, |_| {
                        panic!("receipt retry must not execute callback")
                    })?
                else {
                    anyhow::bail!("missing duplicate")
                };
                assert_eq!(receipt.receipt.result(), expected);
                executor.close()?;
                Ok(())
            })?
            .await??;
        for voter in &fixture.cluster.voters {
            let marker = voter
                .log
                .cell_group(&route.history)?
                .group_manifest()?
                .context("selected root")?;
            let leaf = voter
                .tree
                .get(&voter.pages, marker.root(), &cell.cell)?
                .context("selected cell")?;
            let query = match leaf.version.image.commitment {
                CellImageCommitment::Materialized(digest) => {
                    let pin = voter.images.adopt(
                        image_id(cell, 2)?,
                        ImageState {
                            epoch: leaf.version.image.epoch,
                            txid: TXID(leaf.version.image.txid),
                            checksum: leaf.version.image.checksum,
                            digest,
                            length: leaf.geometry.length,
                            page_size: leaf.geometry.page_size,
                        },
                    )?;
                    cellar_ltx::generation::open_immutable(
                        pin,
                        cellar_ltx::generation::ReadServices {
                            now_unix_millis: Arc::new(|| 10),
                            fill_random: Arc::new(|b| b.fill(17)),
                        },
                    )?
                }
                CellImageCommitment::Extents {
                    root,
                    manifest_bytes,
                } => {
                    let store = voter.extents.as_ref().context("rooted store")?;
                    let view = store.open_root(ExtentRootReference::new(
                        ImageRoot::from_bytes(root),
                        manifest_bytes,
                    )?)?;
                    let cache = fixture
                        .cluster
                        ._root
                        .path()
                        .join(format!("verify-{}", cell.cell.as_str()));
                    std::fs::create_dir_all(&cache)?;
                    cellar_ltx::internal::open_extent_image(
                        view.image(
                            cache,
                            ScratchPool::new(ScratchLimits {
                                max_bytes: 2 * EXTENT_BYTES,
                                max_files: 2,
                            })?,
                            CacheLimits {
                                max_cached_bytes: EXTENT_BYTES,
                                max_cached_extents: 1,
                            },
                        )?,
                        cellar_ltx::generation::ReadServices {
                            now_unix_millis: Arc::new(|| 10),
                            fill_random: Arc::new(|b| b.fill(17)),
                        },
                    )?
                }
            };
            let observed: i64 = query
                .connection()
                .query_row("SELECT n FROM value", [], |r| r.get(0))?;
            assert_eq!(observed, value);
        }
    }
    let stale = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let mut executor = fixture.private_cell(&a).await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let physical = fixture.cluster.domain.blocking(move || -> Result<()> {
        let mut attempt = cell_attempt(86)?;
        attempt.request.id = CommandId::new(ClientId::new([81; 16])?, 2)?;
        let result = executor.stage(attempt, true, |tx| {
            tx.execute("UPDATE value SET n=n+100", [])?;
            let _ = entered.send(());
            wait.recv()?;
            Ok(b"must-not-escape".to_vec())
        });
        assert!(matches!(
            result,
            Err(ExecutionError::Refused(
                cellar_logic::consensus::CommandFailure::Unavailable
            ))
        ));
        executor.close()?;
        Ok(())
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    fixture.runtimes[0].close().await?;
    assert!(stale.check().is_err());
    release.send(())?;
    physical.await??;
    let marker = fixture.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("selected root")?;
    let leaf = fixture.cluster.voters[0]
        .tree
        .get(&fixture.cluster.voters[0].pages, marker.root(), &a.cell)?
        .context("selected cell")?;
    assert_eq!(leaf.version, stale.cell().version);
    drop(stale);
    fixture.close().await
}

#[path = "cell_group_sql_pool.rs"]
mod pool;

pub(super) async fn pool_admission(rooted: bool) -> Result<()> {
    pool::admission(rooted).await
}

#[path = "cell_group_sql_reconcile.rs"]
mod reconciliation;

pub(super) async fn reconcile_producers(rooted: bool) -> Result<()> {
    reconciliation::reconcile(rooted).await
}

pub(super) async fn reconcile_refusals(rooted: bool) -> Result<()> {
    reconciliation::refusals(rooted).await
}
