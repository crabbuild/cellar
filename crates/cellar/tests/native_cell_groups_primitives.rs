// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared Storage over private commands and pinned committed queries.

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, RaftClass, Storage};
use managed_fixture::{ArchiveSetup, FirstGroupSetup, Fixture, ManagedCluster, TIMEOUT};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
// The host fixture controls wall clocks, test processes and on-disk faults.
#[path = "support/cell_group_managed_fixture.rs"]
#[allow(clippy::disallowed_methods)]
pub mod managed_fixture;
#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

fn update(storage: &Storage, delta: i64) -> Result<i64> {
    storage.tx(|tx| {
        let (_, rows, _) = tx.sql("SELECT value FROM counter", &[])?;
        let value = rows[0][0].as_i64().context("integer counter")?;
        ensure!(tx.get_json("value")? == Some(json!(value)));
        let next = value.checked_add(delta).context("counter overflow")?;
        tx.sql("UPDATE counter SET value=?1", &[json!(next)])?;
        tx.put_json("value", &json!(next))?;
        tx.tx(|nested| {
            nested.sql("UPDATE counter SET value=999", &[])?;
            nested.put_json("temporary", &json!(true))?;
            nested.rollback()
        })?;
        ensure!(tx.get_json("temporary")?.is_none());
        let (_, rows, _) = tx.sql("SELECT value FROM counter", &[])?;
        ensure!(rows == vec![vec![json!(next)]]);
        ensure!(tx.sql("COMMIT", &[]).is_err());
        ensure!(tx.sql("SELECT v FROM __sys_cell_kv", &[]).is_err());
        Ok(next)
    })
}

async fn run(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let rejected_after_savepoint = Arc::new(AtomicUsize::new(0));
    let escaped = Arc::new(Mutex::new(None::<Storage>));
    let escaped_query = Arc::new(Mutex::new(None::<Storage>));
    let callback_calls = calls.clone();
    let callback_rejected = rejected_after_savepoint.clone();
    let callback_escaped = escaped.clone();
    let command_query_view = escaped_query.clone();
    let query_view = escaped_query.clone();
    let query_command_view = escaped.clone();
    let class = RaftClass::new("Counter", 1, [211; 32])?
        .initializer([212; 32], |identity, transaction| {
            if identity.cell.as_str() == "Counter:storage-empty" {
                return transaction.execute_batch(
                    "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
                );
            }
            let storage = transaction.storage()?;
            storage.sql(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
                &[],
            )?;
            storage.put_json("value", &json!(0))
        })?
        .command("Add", [213; 32], move |context, input| {
            callback_calls.fetch_add(1, Ordering::AcqRel);
            let storage = context.storage()?;
            let mut previous = callback_escaped.lock().unwrap();
            if let Some(old) = previous.replace(storage.clone()) {
                ensure!(old.get_json("value").is_err(), "escaped view was reused");
            }
            drop(previous);
            if let Some(old) = command_query_view.lock().unwrap().as_ref() {
                ensure!(
                    old.get_json("value").is_err(),
                    "query view entered a command"
                );
            }
            let delta = i64::from_be_bytes(input.try_into()?);
            let result = update(&storage, delta)?;
            if delta < 0 {
                callback_rejected.fetch_add(1, Ordering::AcqRel);
                anyhow::bail!("application rejected the staged update");
            }
            Ok(result.to_be_bytes().to_vec())
        })?
        .query("Get", [214; 32], move |context, _| {
            let storage = context.storage();
            if let Some(old) = query_view.lock().unwrap().replace(storage.clone()) {
                ensure!(
                    old.get_json("value").is_err(),
                    "escaped query view was reused"
                );
            }
            if let Some(old) = query_command_view.lock().unwrap().as_ref() {
                ensure!(
                    old.get_json("value").is_err(),
                    "command view entered a query"
                );
            }
            let (columns, rows, written) = storage.sql("SELECT value FROM counter", &[])?;
            ensure!(columns == ["value"] && written == 0);
            let value = rows[0][0].as_i64().context("integer counter")?;
            let expected =
                (context.identity().cell.as_str() != "Counter:storage-empty").then(|| json!(value));
            ensure!(storage.get_json("value")? == expected);
            // Enter through raw application SQL before invoking the fixed KV read.
            let nested = context.query_row("SELECT 1", [], |_| Ok(storage.get_json("value")))??;
            ensure!(nested == expected);
            let rows = storage.get_many(&["value".into(), "missing".into(), "value".into()])?;
            let keys = rows.into_iter().map(|(key, _)| key).collect::<Vec<_>>();
            let listed = storage.list(&cellar::sdk::ListOptions {
                prefix: Some("val".into()),
                ..Default::default()
            })?;
            ensure!(keys.len() == usize::from(expected.is_some()));
            ensure!(keys.into_iter().eq(listed.into_iter().map(|(key, _)| key)));
            ensure!(storage.get_json("temporary")?.is_none());
            ensure!(storage.sql_database_size()? > 0);
            ensure!(storage.sql("UPDATE counter SET value=999", &[]).is_err());
            ensure!(storage.sql("CREATE TEMP TABLE forbidden(v)", &[]).is_err());
            ensure!(storage.sql("SELECT v FROM __sys_cell_kv", &[]).is_err());
            ensure!(storage.put_json("value", &json!(999)).is_err());
            ensure!(storage.delete_many(&[]).is_err());
            ensure!(storage
                .tx::<()>(|_| panic!("read-only transaction entered"))
                .is_err());
            Ok(value.to_be_bytes().to_vec())
        })?;
    let fixture = Box::pin(Fixture::with_class(rooted, class)).await?;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let work = async {
        let history = cluster.history;
        let leader = managed_fixture::leader(&cluster.hosts, history).await?;
        let now = cluster.fixtures[0]
            .domain
            .run(async { cellar::asyncrt::wall_ms() })
            .await?;
        let retry = u64::try_from(now)?
            .checked_add(60_000)
            .context("retry horizon")?;
        let mut cells = Vec::new();
        for (name, creation) in [
            ("Counter:storage-one", 215),
            ("Counter:storage-two", 216),
            ("Counter:storage-empty", 221),
        ] {
            let cell = cluster.hosts[leader].cell(CellHistory {
                cluster: history.cluster,
                namespace: history.namespace,
                cell: CellId::new(name)?,
                incarnation: IncarnationId::new([217; 16])?,
            })?;
            cell.enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([creation; 16])?,
                    durability: Durability::Quorum,
                },
                TIMEOUT,
            )
            .await?;
            cells.push(cell);
        }
        let first = &cells[0];
        let second = &cells[1];
        let add = first.contract("Add")?;
        let id = CommandId::new(ClientId::new([218; 16])?, 1)?;
        let options = CommandOptions::new(id, retry);
        let receipt = first
            .command(&add, 7i64.to_be_bytes().to_vec(), options.clone())
            .await?;
        ensure!(receipt.result == 7i64.to_be_bytes() && receipt.witness.group == history);
        ensure!(receipt.durability == Durability::Quorum);
        let old = escaped
            .lock()
            .unwrap()
            .clone()
            .context("captured Storage")?;
        ensure!(old.get_json("value").is_err());
        let replay = first
            .command(&add, 7i64.to_be_bytes().to_vec(), options.clone())
            .await?;
        ensure!(replay.result == receipt.result && replay.version == receipt.version);
        ensure!(calls.load(Ordering::Acquire) == 1);

        let failure = CommandOptions::new(CommandId::new(ClientId::new([219; 16])?, 1)?, retry);
        ensure!(first
            .command(&add, (-3i64).to_be_bytes().to_vec(), failure)
            .await
            .is_err());
        ensure!(rejected_after_savepoint.load(Ordering::Acquire) == 1);
        let read = first
            .query(
                ProposalId::new([220; 16])?,
                &first.contract("Get")?,
                Vec::new(),
                QueryOptions::default(),
            )
            .await?;
        ensure!(read.result == 7i64.to_be_bytes() && read.version == receipt.version);
        ensure!(escaped_query
            .lock()
            .unwrap()
            .as_ref()
            .context("captured query Storage")?
            .get_json("value")
            .is_err());

        let archived = second
            .command(
                &second.contract("Add")?,
                11i64.to_be_bytes().to_vec(),
                CommandOptions {
                    durability: Durability::S3,
                    ..options
                },
            )
            .await?;
        ensure!(archived.result == 11i64.to_be_bytes() && archived.durability == Durability::S3);
        ensure!(archived.id == receipt.id && archived.witness.group == history);
        ensure!(archived.version.history != receipt.version.history);

        let checked = second
            .query(
                ProposalId::new([222; 16])?,
                &second.contract("Get")?,
                Vec::new(),
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(archived.version_token),
                    ..QueryOptions::default()
                },
            )
            .await?;
        ensure!(checked.result == 11i64.to_be_bytes() && checked.version == archived.version);
        let empty = &cells[2];
        let checked = empty
            .query(
                ProposalId::new([223; 16])?,
                &empty.contract("Get")?,
                Vec::new(),
                QueryOptions::default(),
            )
            .await?;
        ensure!(checked.result == 0i64.to_be_bytes() && calls.load(Ordering::Acquire) == 3);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained Storage bridge disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_reads_and_writes_exact_cell_versions() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(run(rooted))
            .await
            .with_context(|| format!("Storage bridge extents={rooted}"))?;
    }
    Ok(())
}
