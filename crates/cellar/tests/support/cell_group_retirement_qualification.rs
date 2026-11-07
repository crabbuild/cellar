// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Parent cleanup must leave independently recoverable child baselines.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupArchiveCollectionLimits, CellGroupArchiveTailPool, CellGroupArchiveUploadPool,
    CellImagePin,
};
use object_store::path::Path as ObjectPath;

pub(super) struct ArchiveCheck {
    source: CellGroupArchiveRegistry,
    preparation: CellGroupArchiveHold,
    children: Vec<CellGroupArchiveHold>,
    cells: Vec<CellHistory>,
    archived: CommandReceipt,
}

impl ArchiveCheck {
    pub(super) async fn new(
        cluster: &ManagedCluster,
        directory: &RangeDirectory,
        transition: &ReopenedManagedCellGroupTransition,
        source: &CellGroupArchiveRegistry,
        preparation: &CellGroupArchiveHold,
        cells: &[CellHistory],
        archived: CommandReceipt,
    ) -> Result<Self> {
        ensure!(archived.durability == Durability::S3);
        let children = cluster.fixtures[0]
            .catalog
            .reopen_managed_transfer_child_holds(transition, directory, TIMEOUT)
            .await?
            .context("selected child archive baselines")?;
        ensure!(children.len() == 2);
        Ok(Self {
            source: source.clone(),
            preparation: preparation.clone(),
            children,
            cells: cells.to_vec(),
            archived,
        })
    }

    pub(super) async fn before_release(
        &self,
        cluster: &ManagedCluster,
        directory: &RangeDirectory,
        transition: &ReopenedManagedCellGroupTransition,
    ) -> Result<()> {
        let f = &cluster.fixtures[0];
        let pool = CellGroupArchivePool::new(Default::default())?;
        for child in &self.children {
            let fleet = Fixture::open_catalog(&f.domain, f.store.clone(), cluster.history.cluster)?;
            let adapter = Catalog::new(
                f.store.clone(),
                CatalogOptions {
                    prefix: "managed/".into(),
                    ..Default::default()
                },
            )?;
            let namespace = f
                .domain
                .run(adapter.existing_namespace(f.declaration.namespace.clone()))
                .await??;
            let fresh = RangeDirectory::new(namespace, DirectoryLimits::default())?;
            let retained = fleet
                .reopen_managed_transition(&fresh, transition.plan.operation(), TIMEOUT)
                .await?
                .context("restart during parent cleanup")?;
            ensure!(
                retained.plan.encode() == transition.plan.encode()
                    && retained.request == transition.request
            );
            let journal = retained.journal.as_ref().context("retained retirement")?;
            ensure!(
                journal.phase() == TransitionPhase::Retired && !journal.parent_holds_released()
            );
            let birth = f
                .domain
                .run(fresh.materialize_selected_history(child.marker().route().history))
                .await??;
            let (opened, reads) =
                fault::count_reads(fresh.open_group_archive(f.domain.as_ref(), &pool, &birth))
                    .await;
            opened?;
            let key = reads
                .keys()
                .find(|path| path.ends_with("/binding") && path.contains("/archives/"))
                .context("child archive binding was actually read")?
                .clone();
            let (refused, attempted) = fault::count_reads(fault::missing_read(
                key.clone(),
                fleet.release_managed_transfer_parent_holds(
                    &retained,
                    &fresh,
                    &pool,
                    &cluster.hosts[3],
                    TIMEOUT,
                ),
            ))
            .await;
            ensure!(
                attempted.get(&key).is_some_and(|count| *count > 0),
                "child archive refusal was not reached"
            );
            ensure!(
                matches!(
                    refused,
                    Err(FleetCatalogError::Corrupt("missing group archive binding"))
                ),
                "missing immutable child binding must fail closed: {refused:?}"
            );
            let denied = f.store.denied_prefix_reads.load(Ordering::Acquire);
            *f.store.denied_prefix.lock().unwrap() = Some(ObjectPath::from(key));
            let unavailable = fleet
                .release_managed_transfer_parent_holds(
                    &retained,
                    &fresh,
                    &pool,
                    &cluster.hosts[3],
                    TIMEOUT,
                )
                .await;
            *f.store.denied_prefix.lock().unwrap() = None;
            ensure!(
                f.store.denied_prefix_reads.load(Ordering::Acquire) > denied,
                "child archive outage must be exercised"
            );
            ensure!(
                matches!(unavailable, Err(FleetCatalogError::Storage(_))),
                "child archive outage must retain parent pins: {unavailable:?}"
            );
            ensure!(!f
                .domain
                .run(directory.active_transition())
                .await??
                .context("failed cleanup keeps topology owned")?
                .1
                .parent_holds_released());
            let mut parents = f
                .domain
                .run(directory.sealed_source_holds(&transition.plan))
                .await??;
            parents.push(self.preparation.clone());
            for parent in parents {
                self.recover(cluster, &self.source, &parent, false).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn after_release(
        &self,
        cluster: &ManagedCluster,
        directory: &RangeDirectory,
    ) -> Result<()> {
        let f = &cluster.fixtures[0];
        let uploads = CellGroupArchiveUploadPool::new(Default::default())?;
        let tails = CellGroupArchiveTailPool::new(Default::default())?;
        let collection = self
            .source
            .collect(
                f.domain.as_ref(),
                &uploads,
                &tails,
                CellGroupArchiveCollectionLimits::default(),
            )
            .await?;
        ensure!(collection.scanned_objects > 0 && collection.protected_objects > 0);
        let (intent, paths) =
            fault::count_reads(self.source.prepare_acquire(f.domain.as_ref())).await;
        intent?; // Preparing a read-only intent acquires no hold.
        let key = paths
            .keys()
            .find(|key| key.ends_with("/ownership/registry"))
            .context("source registry read")?;
        let prefix = key
            .strip_suffix("/ownership/registry")
            .context("source archive prefix")?;
        *f.store.denied_prefix.lock().unwrap() = Some(ObjectPath::from(prefix));
        let result = async {
            let before = f.store.denied_prefix_reads.load(Ordering::Acquire);
            ensure!(self
                .source
                .prepare_acquire(f.domain.as_ref())
                .await
                .is_err());
            ensure!(
                f.store.denied_prefix_reads.load(Ordering::Acquire) > before,
                "source archive refusal must be exercised"
            );
            let refused = f.store.denied_prefix_reads.load(Ordering::Acquire);
            let pool = CellGroupArchivePool::new(Default::default())?;
            let mut restored = 0;
            for child in &self.children {
                let birth = f
                    .domain
                    .run(directory.materialize_selected_history(child.marker().route().history))
                    .await??;
                let registry = directory
                    .open_group_archive(f.domain.as_ref(), &pool, &birth)
                    .await?;
                restored += self.recover(cluster, &registry, child, true).await?;
            }
            ensure!(restored == self.cells.len() as u32);
            ensure!(
                f.store.denied_prefix_reads.load(Ordering::Acquire) == refused,
                "child recovery depended on a retired source archive"
            );
            Ok(())
        }
        .await;
        *f.store.denied_prefix.lock().unwrap() = None;
        result
    }

    async fn recover(
        &self,
        cluster: &ManagedCluster,
        registry: &CellGroupArchiveRegistry,
        hold: &CellGroupArchiveHold,
        check_receipt: bool,
    ) -> Result<u32> {
        let f = &cluster.fixtures[0];
        // Keep failed/cancelled replay inventories until the owning domain joins.
        // Entered physical work must never lose files when its waiter is dropped.
        let scratch = tempfile::Builder::new()
            .prefix("retirement-replay-")
            .tempdir()?
            .keep();
        let base = scratch.join("receiver");
        let route = hold.marker().route().clone();
        let rooted = f.declaration.namespace.image_storage == RaftImageStorage::Extents;
        let receiver = f
            .domain
            .blocking(move || preparation::store(base, route, rooted))?
            .await??;
        let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
            receiver.store,
            receiver.scratch,
            receiver.directory,
        )?;
        let tails = CellGroupArchiveTailPool::new(Default::default())?;
        let coverage = registry
            .verify_hold(f.domain.as_ref(), hold, &tails, receiver)
            .await?;
        let mut checked = 0;
        if check_receipt {
            for (index, cell) in self.cells.iter().enumerate() {
                if let Some((leaf, pin)) = coverage.recovery().manifest().pin_cell(&cell.cell) {
                    ensure!(leaf.version.history == *cell);
                    checked += 1;
                    let receipt = (index == 0).then(|| self.archived.clone());
                    let path = scratch.join(format!("query-{index}"));
                    f.domain
                        .blocking(move || check_image(pin, path, [11i64, 12][index], receipt))?
                        .await??;
                }
            }
        }
        let cells = coverage.marker().root().cells();
        ensure!(
            !check_receipt || checked == cells,
            "child recovery included an unexpected cell"
        );
        drop(coverage);
        Ok(cells)
    }
}

fn check_image(
    pin: CellImagePin,
    cache: std::path::PathBuf,
    expected: i64,
    receipt: Option<CommandReceipt>,
) -> Result<()> {
    use cellar_ltx::{
        generation::ReadServices,
        internal::{CacheLimits, EXTENT_BYTES},
        stream_snapshot::{ScratchLimits, ScratchPool},
    };
    let options = cellar::storage::ImmutableQueryOptions {
        sqlite_vec: false,
        services: ReadServices {
            now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
            fill_random: Arc::new(|bytes| bytes.fill(17)),
        },
    };
    let db = match pin {
        CellImagePin::Materialized(pin) => {
            cellar::storage::open_immutable_generation(pin, options)?
        }
        CellImagePin::Extents(pin) => cellar::storage::open_immutable_extent_image(
            pin.view().image(
                cache,
                ScratchPool::new(ScratchLimits {
                    max_bytes: 32 * 1024 * 1024,
                    max_files: 2,
                })?,
                CacheLimits {
                    max_cached_bytes: EXTENT_BYTES,
                    max_cached_extents: 1,
                },
            )?,
            options,
        )?,
        CellImagePin::Empty => anyhow::bail!("retained business cell is empty"),
    };
    let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    ensure!(value == expected);
    if let Some(receipt) = receipt {
        let (result, minimum): (Vec<u8>, i64) = db.connection().query_row(
            "SELECT result, minimum_durability FROM __sys_raft_receipts WHERE client_id=?1 AND sequence=?2",
            rusqlite::params![receipt.id.client.bytes().as_slice(), receipt.id.sequence().to_be_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        ensure!(
            result == receipt.result && minimum == 1,
            "original S3 receipt was lost or weakened"
        );
    }
    Ok(())
}
