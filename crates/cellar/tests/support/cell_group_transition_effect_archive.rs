// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently restore the child outbox completion and receiver dedupe rows.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupArchivePool, CellGroupArchiveTailPool, CellGroupCheckpointPool, CellImagePin,
};
use cellar_ltx::{
    generation::ReadServices,
    internal::{CacheLimits, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
};

pub(super) enum Layout {
    Split,
    Merged,
}

pub(super) async fn verify(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
    received: &CommandReceipt,
    completed: &CommandReceipt,
    layout: Layout,
) -> Result<()> {
    let f = &cluster.fixtures[0];
    let view = f.domain.run(directory.observe()).await??;
    let (groups, cells) = match layout {
        Layout::Split => (2, 1),
        Layout::Merged => (1, 2),
    };
    ensure!(view.groups() == groups);
    let pool = CellGroupArchivePool::new(Default::default())?;
    for index in 0..2 {
        let cell = cell(index)?;
        let route = f.domain.run(directory.lookup(&view, &cell.cell)).await??;
        let birth = f
            .domain
            .run(directory.materialize_selected_history(route.history))
            .await??;
        let registry = directory
            .open_group_archive(f.domain.as_ref(), &pool, &birth)
            .await?;
        let hold = preparation::acquire_hold(f.domain.as_ref(), &registry).await?;
        if index == 0 {
            ensure!(
                hold.marker().route().history == completed.witness.group
                    && hold.marker().position().index >= completed.witness.position.index,
                "child archive does not cover its S3 completion fence"
            );
        }
        // Keep entered replay inventories until the owning domain has joined.
        let scratch = tempfile::Builder::new()
            .prefix("effect-transfer-replay-")
            .tempdir()?
            .keep();
        let base = scratch.join("receiver");
        let target = route.clone();
        let rooted = f.declaration.namespace.image_storage == RaftImageStorage::Extents;
        let target = f
            .domain
            .blocking(move || preparation::store(base, target, rooted))?
            .await??;
        let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
            target.store,
            target.scratch,
            target.directory,
        )?;
        let tails = CellGroupArchiveTailPool::new(Default::default())?;
        let coverage = registry
            .verify_hold(f.domain.as_ref(), &hold, &tails, receiver)
            .await?;
        let (leaf, image) = coverage
            .recovery()
            .manifest()
            .pin_cell(&cell.cell)
            .context("missing effect cell after archive replay")?;
        ensure!(
            leaf.version.history == cell && coverage.marker().root().cells() == cells,
            "effect archive partition shape changed"
        );
        let proof = received.result.clone();
        f.domain
            .blocking(move || check_image(image, scratch.join("query"), index, proof, None))?
            .await??;
        drop(coverage);
        registry.release(f.domain.as_ref(), &hold).await?;
    }
    Ok(())
}

fn check_image(
    pin: CellImagePin,
    cache: std::path::PathBuf,
    index: u8,
    proof: Vec<u8>,
    baseline: Option<(Vec<CommandReceipt>, Vec<u8>)>,
) -> Result<()> {
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
        CellImagePin::Empty => anyhow::bail!("archived effect cell is empty"),
    };
    if index == 0 {
        let pending: i64 =
            db.connection()
                .query_row("SELECT count(*) FROM __sys_raft_outbox", [], |row| {
                    row.get(0)
                })?;
        let (emitted, settled): (Vec<u8>, Vec<u8>) = db.connection().query_row(
            "SELECT emitted,settled FROM __sys_raft_outbox_lanes WHERE lane='Events'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let expected_pending = i64::from(baseline.is_some());
        let expected_settled = u64::from(baseline.is_none());
        ensure!(
            pending == expected_pending
                && emitted == 1u64.to_be_bytes()
                && settled == expected_settled.to_be_bytes(),
            "S3 child recovery lost source effect state"
        );
        if let Some((_, request)) = &baseline {
            let (sequence, actual, minimum): (Vec<u8>, Vec<u8>, i64) = db.connection().query_row(
                "SELECT sequence,request,minimum FROM __sys_raft_outbox WHERE lane='Events'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            ensure!(
                sequence == 1u64.to_be_bytes() && &actual == request && minimum == 1,
                "published closure lost or changed the pending S3 envelope"
            );
        }
    } else {
        let (count, total): (i64, i64) =
            db.query_row("SELECT count,total FROM effect_deliveries", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?;
        ensure!(count == 1 && total == 17);
        let (sequence, receipt): (Vec<u8>, Vec<u8>) = db.connection().query_row(
            "SELECT sequence,receipt FROM __sys_raft_inbox",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let rows: i64 =
            db.connection()
                .query_row("SELECT count(*) FROM __sys_raft_inbox", [], |row| {
                    row.get(0)
                })?;
        ensure!(
            rows == 1 && sequence == 1u64.to_be_bytes() && receipt == proof,
            "S3 child recovery lost the exact receiver dedupe proof"
        );
    }
    if let Some((receipts, _)) = baseline {
        let value: i64 = db.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
        ensure!(value == [10i64, 11][usize::from(index)]);
        for receipt in receipts {
            ensure!(receipt.durability == Durability::S3);
            let (result, minimum): (Vec<u8>, i64) = db.connection().query_row(
                "SELECT result,minimum_durability FROM __sys_raft_receipts WHERE client_id=?1 AND sequence=?2",
                rusqlite::params![receipt.id.client.bytes().as_slice(), receipt.id.sequence().to_be_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)))?;
            ensure!(
                result == receipt.result && minimum == 1,
                "published closure lost an S3 receipt"
            );
        }
    }
    Ok(())
}

pub(super) async fn verify_publication_baseline(
    domain: &ExecutionDomain,
    restored: &PreparedCellGroupTransfer,
    root: &std::path::Path,
    receipts: Vec<CommandReceipt>,
    proof: Vec<u8>,
    request: Vec<u8>,
) -> Result<()> {
    ensure!(restored.children().len() == 2 && receipts.len() == 3);
    let mut checked = 0;
    for index in 0..2 {
        let history = cell(index)?;
        let mut matches = restored
            .children()
            .iter()
            .filter_map(|child| child.pin_cell(&history.cell).map(|pin| (child, pin)));
        let (child, (leaf, image)) = matches.next().context("missing published cell")?;
        ensure!(
            matches.next().is_none()
                && child.root().cells() == 1
                && leaf.version.history == history
        );
        ensure!(child
            .genesis()
            .is_some_and(|genesis| !genesis.marker().is_active()));
        let acknowledgements = receipts
            .iter()
            .filter(|r| r.version.history == history)
            .cloned()
            .collect::<Vec<_>>();
        ensure!(
            !acknowledgements.is_empty()
                && acknowledgements
                    .iter()
                    .all(|r| leaf.version.sequence >= r.version.sequence)
        );
        checked += acknowledgements.len();
        let cache = root.join(format!("query-{index}"));
        let proof = proof.clone();
        let request = request.clone();
        domain
            .blocking(move || {
                check_image(
                    image,
                    cache,
                    index,
                    proof,
                    Some((acknowledgements, request)),
                )
            })?
            .await??;
    }
    ensure!(checked == receipts.len());
    Ok(())
}
