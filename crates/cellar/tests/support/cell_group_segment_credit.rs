// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent disk and Fleet observations after real checkpoint reclamation.
use super::*;
use cellar::application::consensus::verification::{FleetTable, RaftSegmentId};
use cellar::sdk::RaftClass;
use raft_engine::FileNameExt as _;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

const WRITES: u64 = 12;
const INPUT_BYTES: usize = 12 * 1024;

pub(super) async fn qualify(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let cluster = Box::pin(start(rooted, store, false)).await?;
    let result = Box::pin(exercise(&cluster)).await;
    if result.is_err() {
        managed_fixture::diagnostics::record(&cluster, "segment-credit").await;
    }
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    if result.is_err() || joined.is_err() {
        for root in cluster.roots {
            eprintln!("retained segment-credit disk at {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| format!("physical segment credit, extents={rooted}"))?;
    joined
}

async fn start(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    periodic: bool,
) -> Result<ManagedCluster> {
    let class = RaftClass::new("Counter", 1, [81; 32])?
        .initializer([82; 32], |_, sql| {
            sql.execute_batch(
                "CREATE TABLE counter(value INTEGER, payload BLOB); INSERT INTO counter VALUES(0, X'');",
            )?;
            Ok(())
        })?
        .command("Add", [83; 32], |context, input| {
            let delta = i64::from_be_bytes(input.get(..8).context("counter delta")?.try_into()?);
            context.sql().execute(
                "UPDATE counter SET value=value+?1, payload=?2",
                rusqlite::params![delta, &input[8..]],
            )?;
            let value: i64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [84; 32], |context, _| {
            let value: i64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    let mut first = managed_fixture::Fixture::with_store(rooted, class, store).await?;
    first.max_input_bytes = INPUT_BYTES;
    first.small_segments = true;
    first.settle_periodic_purges = periodic;
    Box::pin(ManagedCluster::start_with_applications(
        first,
        &[],
        managed_fixture::ArchiveSetup::Initialized,
        false,
        None,
        managed_fixture::FirstGroupSetup::Normal,
    ))
    .await
}

async fn exercise(cluster: &ManagedCluster) -> Result<()> {
    let seeded = seed_cells(&cluster.hosts, cluster.history).await?;
    let selected = leader(&cluster.hosts, cluster.history).await?;
    let host = &cluster.hosts[selected];
    let fixture = &cluster.fixtures[selected];
    let group = host.group(cluster.history)?;
    let cell = host.cell(seeded.cells[0].clone())?;
    let add = cell.contract("Add")?;
    let disk = cluster.nodes[selected].info().data_dir.join("raft/log");
    let client = ClientId::new([212; 16])?;
    let mut number = 1;

    for cycle in 0..2 {
        let denied = archive_denials(cluster);
        for fixture in &cluster.fixtures {
            fixture
                .store
                .deny_group_archive_writes
                .store(true, Ordering::Release);
        }
        // Charge a live segment basis before archive coverage permits local
        // compaction. Compare the next settlement to this rooted value.
        let charged = async {
            let mut last = None;
            for entry in 1..=WRITES {
                let sequence = cycle * WRITES + entry;
                let payload = payload(sequence);
                let options = CommandOptions::new(CommandId::new(client, sequence)?, seeded.retry);
                // Response delivery can precede the producer's final admission
                // release. Two maximum-input reservations exceed this fixture's
                // byte bank, so serialize the workload through physical exit.
                wait_for_command_exit(host).await?;
                let receipt = cell.command(&add, payload.clone(), options.clone()).await?;
                ensure!(receipt.result == (7 + sequence as i64).to_be_bytes());
                last = Some((receipt, payload, options));
            }
            let (original, payload, options) = last.context("last write")?;
            tokio::time::timeout(TIMEOUT, async {
                while archive_denials(cluster) == denied {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .context("waiting for the archive fault to deny a real write")?;
            ensure!(
                group.observe(TIMEOUT).await?.checkpoint.index < original.witness.position.index,
                "archive gate must leave new writes in the local log"
            );
            let floor = host.reclaim_raft_segments(number, TIMEOUT).await?;
            let before = files(&fixture.domain, disk.clone()).await?;
            ensure!(before.len() > 4, "fixture must rotate real log segments");
            Ok::<_, anyhow::Error>((original, payload, options, floor, before))
        }
        .await;
        for fixture in &cluster.fixtures {
            fixture
                .store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
        }
        let (original, payload, options, charged_floor, before) = charged?;
        number += 1;
        let index = original.witness.position.index;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if group
                    .archive_status()?
                    .and_then(|status| status.published)
                    .is_some_and(|manifest| manifest.position().index >= index)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("waiting for archive publication before checkpoint selection")??;
        // Checkpoints capture the preceding applied cut. A successor on the
        // other cell makes the archived final write eligible while preserving
        // the exact query and replay value on this cell.
        let successor = host.cell(seeded.cells[1].clone())?;
        successor
            .command(
                &successor.contract("Add")?,
                0_i64.to_be_bytes().to_vec(),
                CommandOptions::new(
                    CommandId::new(ClientId::new([214 + cycle as u8; 16])?, 1)?,
                    seeded.retry,
                ),
            )
            .await?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if group.observe(TIMEOUT).await?.checkpoint.index >= index {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("waiting for an installed checkpoint covering the writes")??;
        let mut credit = None;
        for _ in 0..8 {
            let floor = host.reclaim_raft_segments(number, TIMEOUT).await?;
            let after = files(&fixture.domain, disk.clone()).await?;
            let surviving = before
                .iter()
                .filter_map(|(id, bytes)| after.get(id).map(|size| (*bytes).min(*size)))
                .sum::<u64>();
            let removed = before.keys().filter(|id| !after.contains_key(id)).count();
            ensure!(floor >= surviving, "floor omitted surviving segment bytes");
            if removed > 0 && floor < charged_floor {
                credit = Some((number, floor));
                break;
            }
            number += 1;
        }
        let (replayed_number, credited_floor) = credit.context("real file-byte credit")?;
        let cut = fixture.catalog.read(TIMEOUT).await?;
        let floor = floor_at(&fixture.catalog, &cut, host.identity().node).await?;
        ensure!(floor == (replayed_number, credited_floor));
        ensure!(host.reclaim_raft_segments(replayed_number, TIMEOUT).await? == credited_floor);
        ensure!(fixture.catalog.read(TIMEOUT).await?.revision() == cut.revision());
        number = replayed_number + 1;
        let replayed = cell.command(&add, payload, options).await?;
        ensure!(replayed.id == original.id && replayed.result == original.result);
        let live = cell
            .query(
                ProposalId::new([213 + cycle as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(original.version_token),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            live.result == original.result,
            "purge or replay changed live SQL"
        );
    }
    Ok(())
}

pub(super) async fn periodic(rooted: bool) -> Result<()> {
    let store = Arc::new(catalog_fault::Store::default());
    let cluster = Box::pin(start(rooted, store.clone(), true)).await?;
    let result = Box::pin(exercise_periodic(&cluster, &store)).await;
    if result.is_err() {
        managed_fixture::diagnostics::record(&cluster, "periodic-segment-credit").await;
    }
    store.fleet_cas_hang.store(false, Ordering::Release);
    store.fleet_cas_resume.notify_waiters();
    let joined = movement::close(cluster.nodes, &cluster.hosts).await;
    if result.is_err() || joined.is_err() {
        for root in cluster.roots {
            eprintln!(
                "retained periodic segment-credit disk: {}",
                root.keep().display()
            );
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| format!("periodic segment purge retry, extents={rooted}"))?;
    joined
}

async fn exercise_periodic(cluster: &ManagedCluster, store: &catalog_fault::Store) -> Result<()> {
    let seeded = seed_cells(&cluster.hosts, cluster.history).await?;
    let observer = &cluster.hosts[0];
    let catalog = &cluster.fixtures[0].catalog;
    let node = observer.identity().node;
    let first = observer.reclaim_raft_segments(1, TIMEOUT).await?;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(floor_at(catalog, &cut, node).await? == (1, first));

    let selected = leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[selected].cell(seeded.cells[0].clone())?;
    let contract = cell.contract("Add")?;
    let client = ClientId::new([219; 16])?;
    store.fail_fleet_before.store(true, Ordering::Release);
    store.fleet_cas_hang.store(true, Ordering::Release);
    store.hold_next_fleet_cas.store(true, Ordering::Release);
    let ((), ()) = tokio::try_join!(
        async {
            for sequence in 1..=2 * WRITES {
                // Maximum-input reservations must wait for actual producer
                // exit, including when its receipt has already been delivered.
                wait_for_command_exit(&cluster.hosts[selected]).await?;
                let receipt = cell
                    .command(
                        &contract,
                        payload(sequence),
                        CommandOptions::new(CommandId::new(client, sequence)?, seeded.retry),
                    )
                    .await?;
                ensure!(receipt.result == (7 + sequence as i64).to_be_bytes());
            }
            Ok::<_, anyhow::Error>(())
        },
        async {
            tokio::time::timeout(2 * TIMEOUT, async {
                while !store.fleet_cas_waiting.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("periodic purge did not reach the Fleet CAS")?;
            let held = catalog.read(TIMEOUT).await?;
            ensure!(held.revision() == cut.revision());
            ensure!(floor_at(catalog, &held, node).await? == (1, first));
            store.fleet_cas_hang.store(false, Ordering::Release);
            store.fleet_cas_resume.notify_waiters();
            tokio::time::timeout(TIMEOUT, async {
                while store.fail_fleet_before.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("periodic settlement did not consume the CAS fault")?;
            Ok::<_, anyhow::Error>(())
        },
    )?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let current = catalog.read(TIMEOUT).await?;
            let (number, bytes) = floor_at(catalog, &current, node).await?;
            if number > 1 {
                ensure!(number == current.revision() - cut.revision() + 1);
                ensure!(bytes > 0);
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("periodic worker did not settle its retry")??;
    Ok(())
}

async fn wait_for_command_exit(host: &RaftCells) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        while host.resource_usage().host.controller.ordinary.items != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("previous command owner did not release its admission")
}

async fn floor_at(
    catalog: &FleetCatalog,
    cut: &cellar::application::consensus::verification::FleetSnapshot,
    node: NodeId,
) -> Result<(u64, u64)> {
    let key = format!("segment-floor/{}", hex::encode(node.bytes()));
    let floor = catalog
        .get(cut, FleetTable::Operations, &key, TIMEOUT)
        .await?
        .context("missing rooted segment floor")?;
    ensure!(floor.len() >= 45);
    let len = u32::from_be_bytes(floor[9..13].try_into()?) as usize;
    ensure!(floor.len() == 45 + len && &floor[..8] == b"CLRCAT01" && floor[8] == 241);
    use sha2::Digest as _;
    ensure!(sha2::Sha256::digest(&floor[..13 + len]).as_slice() == &floor[13 + len..]);
    let row: serde_json::Value = serde_json::from_slice(&floor[13..13 + len])?;
    Ok((
        row["number"].as_u64().context("purge number")?,
        row["bytes"].as_u64().context("purge bytes")?,
    ))
}

fn payload(sequence: u64) -> Vec<u8> {
    let mut payload = vec![0; INPUT_BYTES];
    payload[..8].copy_from_slice(&1_i64.to_be_bytes());
    // Deterministic non-repeating data exercises real file rotation.
    let mut state = sequence;
    for byte in &mut payload[8..] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    payload
}

fn archive_denials(cluster: &ManagedCluster) -> usize {
    cluster
        .fixtures
        .iter()
        .map(|fixture| {
            fixture
                .store
                .denied_group_archive_writes
                .load(Ordering::Acquire)
        })
        .sum()
}

async fn files(
    domain: &cellar::asyncrt::ExecutionDomain,
    path: PathBuf,
) -> Result<BTreeMap<RaftSegmentId, u64>> {
    domain
        .blocking(move || {
            let mut files = BTreeMap::new();
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let name = entry.file_name();
                if let Some(id) = name.to_str().and_then(RaftSegmentId::parse_file_name) {
                    files.insert(id, entry.metadata()?.len());
                }
            }
            Ok(files)
        })?
        .await?
}
