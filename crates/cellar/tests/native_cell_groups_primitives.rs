// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared Storage over private commands and pinned committed queries.

use anyhow::{ensure, Context as _, Result};
use cellar::engine_api::RpcData;
use cellar::sdk::consensus::verification::ExecutionError;
use cellar::sdk::{
    consensus::*, Application, Cell, Context, HandlerFuture, Invocation, Output, RaftClass,
    Registry, Storage,
};
use futures_util::FutureExt;
use managed_fixture::{ArchiveSetup, FirstGroupSetup, Fixture, ManagedCluster, TIMEOUT};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Notify;

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;
// The host fixture controls wall clocks, test processes and on-disk faults.
#[path = "support/cell_group_activations.rs"]
mod activations;
#[path = "support/cell_group_configuration.rs"]
mod configuration;
#[path = "support/cell_group_dispatch.rs"]
mod dispatch;
#[path = "support/cell_group_interleaving.rs"]
mod interleaving;
#[path = "support/cell_group_managed_fixture.rs"]
#[allow(clippy::disallowed_methods)]
pub mod managed_fixture;
#[path = "support/cell_group_rustfs.rs"]
pub mod rustfs;
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
            let refused = storage.sync().now_or_never().context("sync refusal")?;
            ensure!(
                refused
                    .expect_err("synchronous commands cannot schedule RPC sync")
                    .downcast_ref::<CommandFailure>()
                    == Some(&CommandFailure::UnsupportedCapability)
            );
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
            ensure!(matches!(storage.sync().now_or_never(), Some(Err(_))));
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

struct NativeCounter {
    storage: Storage,
    rejected: Arc<AtomicUsize>,
    gates: Arc<[RpcGate; 7]>,
    invocation_count: AtomicUsize,
    previous_context: Mutex<Option<Context>>,
}

#[derive(Default)]
struct RpcGate {
    entered: Notify,
    release: Notify,
    dropped: Notify,
    resumed: AtomicUsize,
}

struct Waiting<'a>(&'a Notify);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl Cell for NativeCounter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            ensure!(context.var("COUNT_STEP") == Some("1"));
            ensure!(context.var("LABEL") == Some("shared \"configuration\"\n日本語"));
            let Invocation::CellRpc {
                method,
                args: RpcData::Json(input),
            } = invocation
            else {
                anyhow::bail!("expected JSON cell RPC");
            };
            let input: serde_json::Value = serde_json::from_str(&input)?;
            let invocation = self.invocation_count.fetch_add(1, Ordering::AcqRel) + 1;
            if let Some(expected) = input["invocation"].as_u64() {
                ensure!(
                    invocation as u64 == expected,
                    "activation-local invocation count changed"
                );
            }
            if let Some(previous) = self
                .previous_context
                .lock()
                .unwrap()
                .replace(context.clone())
            {
                ensure!(
                    previous.storage().is_err(),
                    "an old RPC Context entered a later invocation"
                );
            }
            if method == "Transactions" {
                exercise_raft_transactions(self.storage.clone(), context.clone()).await?;
                if input["reject"] == json!(true) {
                    self.rejected.fetch_add(1, Ordering::AcqRel);
                    anyhow::bail!("reject the outer RPC after savepoints");
                }
                return Ok(Output::Rpc(RpcData::Json("true".into())));
            }
            if method == "Sequence" {
                let delta = input["delta"].as_i64().context("first delta")?;
                let first = self
                    .storage
                    .transaction(&context, move |tx| async move { update(&tx, delta) })
                    .await?;
                let gate =
                    &self.gates[usize::try_from(input["gate"].as_u64().context("sequence gate")?)?];
                let _waiting = Waiting(&gate.dropped);
                gate.entered.notify_one();
                gate.release.notified().await;
                gate.resumed.fetch_add(1, Ordering::AcqRel);
                ensure!(context.storage()?.get_json("value")? == Some(json!(first)));
                let delta = input["after"].as_i64().context("second delta")?;
                let second = self
                    .storage
                    .transaction(&context, move |tx| async move { update(&tx, delta) })
                    .await?;
                return Ok(Output::Rpc(RpcData::Json(json!(second).to_string())));
            }
            if method == "Sync" {
                let delta = input["delta"].as_i64().context("sync delta")?;
                let first = self
                    .storage
                    .get_json("value")?
                    .and_then(|value| value.as_i64())
                    .context("value before sync")?
                    .checked_add(delta)
                    .context("sync value overflow")?;
                if input["ordered"] == json!(true) {
                    drop(self.storage.transaction(&context, move |tx| async move {
                        yield_transaction().await;
                        update(&tx, delta)
                    }));
                } else {
                    ensure!(update(&self.storage, delta)? == first);
                }
                let synced = self.storage.sync();
                if input["detached"] == json!(true) {
                    drop(synced);
                } else {
                    synced.await?;
                }
                if let Some(index) = input["gate"].as_u64() {
                    let gate = self
                        .gates
                        .get(usize::try_from(index)?)
                        .context("sync gate")?;
                    let _waiting = Waiting(&gate.dropped);
                    gate.entered.notify_one();
                    gate.release.notified().await;
                    gate.resumed.fetch_add(1, Ordering::AcqRel);
                }
                ensure!(context.storage()?.get_json("value")? == Some(json!(first)));
                let second = update(
                    &self.storage,
                    input["after"].as_i64().context("after sync delta")?,
                )?;
                if input["reject"] == json!(true) {
                    anyhow::bail!("reject the RPC after explicit storage sync");
                }
                return Ok(Output::Rpc(RpcData::Json(json!(second).to_string())));
            }
            ensure!(method == "Update");
            ensure!(context.wait_until(async { Ok(()) }).is_err());
            let delta = input["delta"].as_i64().context("delta")?;
            let original = self.clone();
            let scope = context.clone();
            self.storage
                .transaction(&context, move |storage| async move {
                    let mut next = update(&storage, delta)?;
                    ensure!(scope.storage()?.get_json("value")? == Some(json!(next)));
                    if let Some(index) = input["gate"].as_u64() {
                        let gate = original
                            .gates
                            .get(usize::try_from(index)?)
                            .context("RPC gate")?;
                        let _waiting = Waiting(&gate.dropped);
                        gate.entered.notify_one();
                        gate.release.notified().await;
                        gate.resumed.fetch_add(1, Ordering::AcqRel);
                        let resumed = scope.storage()?;
                        ensure!(resumed.get_json("value")? == Some(json!(next)));
                        next = update(&resumed, input["after"].as_i64().context("resumed delta")?)?;
                    }
                    if input["malformed_reply"] == json!(true) {
                        original.rejected.fetch_add(1, Ordering::AcqRel);
                        return Ok(Output::Rpc(RpcData::Json("malformed".into())));
                    }
                    Ok(Output::Rpc(RpcData::Json(json!(next).to_string())))
                })
                .await
        })
    }
}

async fn yield_transaction() {
    let mut yielded = false;
    futures_util::future::poll_fn(|cx| {
        if std::mem::replace(&mut yielded, true) {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

fn transaction_row(storage: &Storage, name: &str, value: i64) -> Result<()> {
    storage.sql(
        "INSERT INTO transaction_items(name,value) VALUES(?1,?2)",
        &[json!(name), json!(value)],
    )?;
    storage.put_json(name, &json!(value))
}

async fn exercise_raft_transactions(storage: Storage, context: Context) -> Result<()> {
    storage.sql(
        "CREATE TABLE IF NOT EXISTS transaction_items(name TEXT PRIMARY KEY, value INTEGER);
         DELETE FROM transaction_items;",
        &[],
    )?;
    let scope = context.clone();
    let escaped = storage
        .transaction(&context, move |outer| async move {
            transaction_row(&outer, "outer", 1)?;
            let refused = outer
                .sync()
                .await
                .expect_err("sync cannot split an async transaction");
            ensure!(refused.to_string().contains("open transaction"));
            yield_transaction().await;
            ensure!(scope.storage()?.get_json("outer")? == Some(json!(1)));
            let rolled = outer
                .transaction(&scope, |child| async move {
                    transaction_row(&child, "rolled", 9)?;
                    yield_transaction().await;
                    child.rollback()?;
                    Ok(child)
                })
                .await?;
            ensure!(rolled.get_json("rolled").is_err());
            ensure!(outer.get_json("rolled")?.is_none());
            let failed: Result<()> = outer
                .transaction(&scope, |child| async move {
                    transaction_row(&child, "failed", 9)?;
                    yield_transaction().await;
                    anyhow::bail!("expected child rollback")
                })
                .await;
            ensure!(failed.is_err() && outer.get_json("failed")?.is_none());
            let panicked: Result<()> = outer
                .transaction(&scope, |child| async move {
                    transaction_row(&child, "panicked", 9)?;
                    yield_transaction().await;
                    panic!("expected async transaction panic");
                })
                .await;
            ensure!(panicked.is_err() && outer.get_json("panicked")?.is_none());
            let constructor: Result<()> = outer
                .transaction(&scope, |_| -> HandlerFuture<()> {
                    panic!("expected transaction constructor panic");
                })
                .await;
            ensure!(constructor.is_err());

            let first = outer.transaction(&scope, |child| async move {
                yield_transaction().await;
                transaction_row(&child, "child", 2)?;
                Ok(child)
            });
            let second = outer.transaction(&scope, |child| async move {
                ensure!(child.get_json("child")? == Some(json!(2)));
                Ok(child)
            });
            drop(first);
            let closed = second.await?;
            ensure!(closed.get_json("child").is_err());

            let detached = outer.transaction(&scope, |child| async move {
                yield_transaction().await;
                transaction_row(&child, "detached", 3)
            });
            drop(detached);
            Ok(outer)
        })
        .await?;
    ensure!(escaped.get_json("outer").is_err());
    ensure!(storage.get_json("detached")? == Some(json!(3)));
    storage.tx(|sync| {
        let refused = sync
            .sync()
            .now_or_never()
            .context("sync transaction refusal")?;
        ensure!(refused
            .expect_err("sync cannot split a savepoint")
            .to_string()
            .contains("open transaction"));
        let refused = sync.transaction(&context, |_| -> HandlerFuture<()> {
            panic!("async work entered a synchronous transaction");
        });
        ensure!(matches!(refused.now_or_never(), Some(Err(_))));
        Ok(())
    })?;
    transaction_row(&storage, "quota", 0)?;
    for _ in 0..4 {
        drop(storage.transaction(&context, |tx| async move {
            yield_transaction().await;
            tx.sql(
                "UPDATE transaction_items SET value=value+1 WHERE name='quota'",
                &[],
            )?;
            Ok(())
        }));
    }
    let overflow = storage
        .transaction(&context, |_| -> HandlerFuture<()> {
            panic!("overloaded callback must never run");
        })
        .await;
    let error = overflow.expect_err("task budget must refuse the fifth callback");
    ensure!(error.downcast_ref::<CommandFailure>() == Some(&CommandFailure::Overloaded));
    let error = storage
        .sync()
        .await
        .expect_err("sync shares the pending-work budget");
    ensure!(error.downcast_ref::<CommandFailure>() == Some(&CommandFailure::Overloaded));
    Ok(())
}

async fn run_native_cell_rpc(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let saved_context = Arc::new(Mutex::new(None::<Context>));
    let gates: Arc<[RpcGate; 7]> = Arc::new(std::array::from_fn(|_| RpcGate::default()));
    let factory_calls = calls.clone();
    let factory_rejected = rejected.clone();
    let factory_context = saved_context.clone();
    let factory_gates = gates.clone();
    let class = RaftClass::new("Counter", 1, [231; 32])?
        .initializer([232; 32], |_, tx| {
            let storage = tx.storage()?;
            storage.sql(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
                &[],
            )?;
            storage.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Update", [233; 32])?
        .json_cell_rpc("Transactions", [250; 32])?
        .json_cell_rpc("Sequence", [225; 32])?
        .json_cell_rpc("Sync", [210; 32])?
        .query("TransactionRows", [251; 32], |context, _| {
            let (_, rows, _) = context.storage().sql(
                "SELECT name, value FROM transaction_items ORDER BY name",
                &[],
            )?;
            Ok(serde_json::to_vec(&rows)?)
        })?
        .query("Get", [234; 32], |context, _| {
            let storage = context.storage();
            let (_, rows, _) = storage.sql("SELECT value FROM counter", &[])?;
            let value = rows[0][0].as_i64().context("integer counter")?;
            ensure!(storage.get_json("value")? == Some(json!(value)));
            let factories = storage.get_json("factories")?;
            Ok(serde_json::to_vec(&json!([value, factories]))?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, identity| {
                ensure!(context.var("LABEL") == Some("shared \"configuration\"\n日本語"));
                let step: i64 = context
                    .var("COUNT_STEP")
                    .context("declared COUNT_STEP")?
                    .parse()?;
                factory_calls.fetch_add(1, Ordering::AcqRel);
                ensure!(identity == context.identity().context("Raft cell identity")?);
                ensure!(identity.class == "Counter" && identity.epoch != 0);
                let prior = factory_context.lock().unwrap().replace(context.clone());
                if let Some(prior) = prior {
                    ensure!(prior.storage().is_err(), "retired Context acquired Storage");
                }
                let storage = context.storage()?;
                let count = storage
                    .get_json("factories")?
                    .map_or(0, |value| value.as_i64().unwrap_or(-1));
                ensure!(count >= 0);
                storage.put_json("factories", &json!(count + step))?;
                Ok(Arc::new(NativeCounter {
                    storage,
                    rejected: factory_rejected.clone(),
                    gates: factory_gates.clone(),
                    invocation_count: AtomicUsize::new(0),
                    previous_context: Mutex::new(None),
                }))
            },
        )?,
    );
    let mut options = configuration::options("counter-service");
    options.vars = vec![
        ("LABEL".into(), "shared \"configuration\"\n日本語".into()),
        ("COUNT_STEP".into(), "1".into()),
    ];
    application.validate_deployment(&configuration::deployment(options.clone()))?;
    let declared = application.resolve_raft_class("Counter")?;
    options.vars.reverse();
    options.node = "another-voter".into();
    application.validate_configuration(&options)?;
    ensure!(Arc::ptr_eq(
        &declared,
        &application.resolve_raft_class("Counter")?
    ));
    let mut changed = options.clone();
    changed.vars[0].1 = "2".into();
    ensure!(application.validate_configuration(&changed).is_err());
    ensure!(
        calls.load(Ordering::Acquire) == 0,
        "configuration constructed a cell"
    );
    let mut fixture = Box::pin(Fixture::with_application(
        rooted,
        "Counter",
        application,
        store,
    ))
    .await?;
    fixture.configuration = Some(options);
    fixture.sql_workers = 1;
    fixture.max_pending_transactions = 4;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let work = cluster.fixtures[0]
        .domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let mut cells = Vec::new();
            for (name, proposal) in [("Counter:ordinary", 236), ("Counter:unrelated", 244)] {
                let cell = cluster.hosts[leader].cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(name)?,
                    incarnation: IncarnationId::new([235; 16])?,
                })?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([proposal; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
                cells.push(cell);
            }
            let cell = &cells[0];
            ensure!(calls.load(Ordering::Acquire) == 0);
            let now = cluster.fixtures[0]
                .domain
                .run(async { cellar::asyncrt::wall_ms() })
                .await?;
            let retry = u64::try_from(now)?
                .checked_add(60_000)
                .context("retry horizon")?;
            let options = |client| -> Result<CommandOptions> {
                Ok(CommandOptions::new(
                    CommandId::new(ClientId::new([client; 16])?, 1)?,
                    retry,
                ))
            };
            // Five independently applied roots plus the final receipt are a
            // multi-stage conformance request. Choose its budget before admission;
            // basic requests keep the SDK default and cancellation stays at 1s.
            let multiple = |client| -> Result<CommandOptions> {
                Ok(CommandOptions { timeout: Duration::from_secs(20), ..options(client)? })
            };
            let update = cell.contract("Update")?;
            let input = serde_json::to_vec(&json!({"delta": 7, "invocation": 1}))?;
            let written = cell.command(&update, input.clone(), options(237)?).await?;
            ensure!(written.result == b"7" && written.durability == Durability::Quorum);
            ensure!(written.witness.group == group);
            let replay = cell.command(&update, input, options(237)?).await?;
            ensure!(replay.result == written.result && replay.version == written.version);
            ensure!(calls.load(Ordering::Acquire) == 1);
            let usage = cluster.hosts[leader].resource_usage().sql.context("activation usage")?;
            ensure!(usage.idle_activations == 1 && usage.retained_activations == 1);
            ensure!(usage.activation_capacity == usage.limits.max_pending_cells);
            ensure!(saved_context
                .lock()
                .unwrap()
                .as_ref()
                .context("saved Context")?
                .storage()
                .is_err());

            ensure!(cell
                .command(
                    &update,
                    serde_json::to_vec(&json!({"delta": 20, "malformed_reply": true, "invocation": 2}))?,
                    options(240)?,
                )
                .await
                .is_err());
            ensure!(calls.load(Ordering::Acquire) == 1);
            ensure!(cluster.hosts[leader].resource_usage().sql.context("retired activation usage")?.retained_activations == 0);
            ensure!(rejected.load(Ordering::Acquire) == 1);
            // The explicit root transaction committed before reply validation.
            // Its original pending invocation must never rerun the factory.
            let partial = cell
                .query(
                    ProposalId::new([238; 16])?,
                    &cell.contract("Get")?,
                    Vec::new(),
                    QueryOptions::default(),
                )
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&partial.result)? == json!([27, 1])
            );
            ensure!(partial.version.sequence.get() == written.version.sequence.get() + 1);
            ensure!(cell
                .command(
                    &update,
                    serde_json::to_vec(&json!({"delta": 20, "malformed_reply": true, "invocation": 2}))?,
                    options(240)?
                )
                .await
                .is_err());
            ensure!(calls.load(Ordering::Acquire) == 1);
            check_rpc_read(cell, 227, &partial.version, json!([27, 1])).await?;

            let input = serde_json::to_vec(&json!({"delta": 3, "gate": 0, "after": 5, "invocation": 1}))?;
            let (resumed, interleaved) =
                tokio::join!(cell.command(&update, input.clone(), options(239)?), async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[0].entered.notified()).await?;
                    ensure!(saved_context
                        .lock()
                        .unwrap()
                        .as_ref()
                        .context("suspended Context")?
                        .storage()
                        .is_err());
                    check_rpc_read(cell, 241, &partial.version, json!([27, 1])).await?;
                    let unrelated = cells[1]
                        .command(
                            &update,
                            serde_json::to_vec(&json!({"delta": 4}))?,
                            options(242)?,
                        )
                        .await?;
                    ensure!(unrelated.result == b"4" && unrelated.witness.group == group);
                    gates[0].release.notify_one();
                    Ok::<_, anyhow::Error>(())
                },);
            interleaved?;
            let resumed = resumed?;
            ensure!(resumed.result == b"35" && resumed.durability == Durability::Quorum);
            ensure!(gates[0].resumed.load(Ordering::Acquire) == 1);
            check_rpc_read(cell, 245, &resumed.version, json!([35, 2])).await?;
            let retry = cell.command(&update, input, options(239)?).await?;
            ensure!(retry.result == resumed.result && retry.version == resumed.version);
            ensure!(calls.load(Ordering::Acquire) == 3);
            let usage = cluster.hosts[leader].resource_usage().sql.context("shared activation usage")?;
            ensure!(usage.idle_activations == 2 && usage.retained_activations == 2);

            let short = CommandOptions {
                timeout: Duration::from_secs(1),
                ..options(246)?
            };
            let (expired, cleaned) = tokio::join!(
                cell.command(
                    &update,
                    serde_json::to_vec(&json!({"delta": 100, "gate": 1, "after": 1, "invocation": 2}))?,
                    short,
                ),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[1].entered.notified()).await?;
                    cellar::asyncrt::timeout(TIMEOUT, gates[1].dropped.notified()).await?;
                    Ok::<_, anyhow::Error>(())
                },
            );
            cleaned?;
            ensure!(expired.is_err() && gates[1].resumed.load(Ordering::Acquire) == 0);
            check_rpc_read(cell, 247, &resumed.version, json!([35, 2])).await?;

            let long = CommandOptions {
                timeout: Duration::from_secs(20),
                ..options(248)?
            };
            let (revoked, transferred) = tokio::join!(
                cell.command(
                    &update,
                    serde_json::to_vec(&json!({"delta": 200, "gate": 2, "after": 1, "invocation": 1}))?,
                    long,
                ),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[2].entered.notified()).await?;
                    let group = cluster.hosts[leader].group(group)?;
                    let status = group.replication_status(TIMEOUT).await?;
                    let target = (leader + 1) % 3;
                    group
                        .transfer_leadership(
                            LeadershipTransfer {
                                target: MemberId::new(target as u64 + 1)?,
                                expected_term: status.term,
                                expected_revision: status.membership_revision,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    cellar::asyncrt::timeout(TIMEOUT, gates[2].dropped.notified()).await?;
                    gates[2].release.notify_one();
                    Ok::<_, anyhow::Error>(target)
                },
            );
            let target = transferred?;
            let error = revoked.expect_err("stale RPC must not commit");
            ensure!(error.cause.refusal() == Some(&CommandFailure::Unavailable));
            ensure!(gates[2].resumed.load(Ordering::Acquire) == 0);
            let current = cluster.hosts[target].cell(cell.history().clone())?;
            check_rpc_read(&current, 249, &resumed.version, json!([35, 2])).await?;
            ensure!(calls.load(Ordering::Acquire) == 4);
            let transaction_contract = current.contract("Transactions")?;
            let transaction_receipt = current
                .command(&transaction_contract, b"{}".to_vec(), multiple(252)?)
                .await?;
            ensure!(transaction_receipt.result == b"true");
            let rows = current
                .query(
                    ProposalId::new([253; 16])?,
                    &current.contract("TransactionRows")?,
                    Vec::new(),
                    QueryOptions::default(),
                )
                .await?;
            ensure!(rows.version == transaction_receipt.version);
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&rows.result)?
                    == json!([["child", 2], ["detached", 3], ["outer", 1], ["quota", 4]])
            );
            ensure!(current
                .command(
                    &transaction_contract,
                    br#"{"reject":true}"#.to_vec(),
                    multiple(254)?
                )
                .await
                .is_err());
            let unchanged = current
                .query(
                    ProposalId::new([255; 16])?,
                    &current.contract("TransactionRows")?,
                    Vec::new(),
                    QueryOptions::default(),
                )
                .await?;
            ensure!(rejected.load(Ordering::Acquire) == 2);
            ensure!(unchanged.version.sequence.get() == rows.version.sequence.get() + 1);
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&unchanged.result)?
                    == json!([["child", 2], ["detached", 3], ["outer", 1]])
            );

            let sequence = current.contract("Sequence")?;
            let sequence_input = serde_json::to_vec(&json!({"delta": 10, "gate": 3, "after": 20}))?;
            let strong = CommandOptions {
                durability: Durability::S3,
                ..multiple(226)?
            };
            let (sequence_result, observed) = tokio::join!(
                current.command(&sequence, sequence_input.clone(), strong.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[3].entered.notified()).await?;
                    let first = current
                        .query(
                            ProposalId::new([224; 16])?,
                            &current.contract("Get")?,
                            Vec::new(),
                            QueryOptions::default(),
                        )
                        .await?;
                    ensure!(
                        serde_json::from_slice::<serde_json::Value>(&first.result)?
                            == json!([45, 4])
                    );
                    ensure!(first.version.sequence.get() == unchanged.version.sequence.get() + 1);
                    gates[3].release.notify_one();
                    Ok::<_, anyhow::Error>(first.version)
                },
            );
            let observed = observed?;
            let sequence_result = sequence_result?;
            ensure!(
                sequence_result.result == b"65" && sequence_result.durability == Durability::S3
            );
            ensure!(sequence_result.version.sequence.get() == observed.sequence.get() + 2);
            let factories = calls.load(Ordering::Acquire);
            let replay = current.command(&sequence, sequence_input, strong).await?;
            ensure!(
                replay.result == sequence_result.result
                    && replay.version == sequence_result.version
            );
            ensure!(calls.load(Ordering::Acquire) == factories);

            // Lose leadership only after the first transaction has applied.
            // The recovered pending identity blocks whole-handler reexecution.
            let interrupted_input =
                serde_json::to_vec(&json!({"delta": 100, "gate": 4, "after": 1000}))?;
            let (interrupted, moved) = tokio::join!(
                current.command(
                    &sequence,
                    interrupted_input.clone(),
                    CommandOptions {
                        timeout: Duration::from_secs(20),
                        ..options(223)?
                    }
                ),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[4].entered.notified()).await?;
                    let first = current
                        .query(
                            ProposalId::new([222; 16])?,
                            &current.contract("Get")?,
                            Vec::new(),
                            QueryOptions::default(),
                        )
                        .await?;
                    ensure!(
                        serde_json::from_slice::<serde_json::Value>(&first.result)?
                            == json!([165, 4])
                    );
                    let pending = current.receipt(CellReceiptRequest {
                        id: options(223)?.id, contract: sequence.clone(), input: interrupted_input.clone(),
                        durability: Durability::Quorum,
                    }, TIMEOUT).await;
                    ensure!(matches!(pending, Err(error) if matches!(&*error, CellDispatchError::Execution(ExecutionError::Uncertain))), "unfinished RPC must not become an absent or completed receipt");
                    let handle = cluster.hosts[target].group(group)?;
                    let status = handle.replication_status(TIMEOUT).await?;
                    let destination = (target + 1) % 3;
                    handle
                        .transfer_leadership(
                            LeadershipTransfer {
                                target: MemberId::new(destination as u64 + 1)?,
                                expected_term: status.term,
                                expected_revision: status.membership_revision,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    cellar::asyncrt::timeout(TIMEOUT, gates[4].dropped.notified()).await?;
                    gates[4].release.notify_one();
                    Ok::<_, anyhow::Error>((destination, first.version))
                },
            );
            ensure!(interrupted.is_err() && gates[4].resumed.load(Ordering::Acquire) == 0);
            let (destination, preserved) = moved?;
            let recovered = cluster.hosts[destination].cell(current.history().clone())?;
            check_rpc_read(&recovered, 220, &preserved, json!([165, 4])).await?;
            let factories = calls.load(Ordering::Acquire);
            ensure!(recovered
                .command(&sequence, interrupted_input, options(223)?)
                .await
                .is_err());
            ensure!(calls.load(Ordering::Acquire) == factories);
            check_rpc_read(&recovered, 219, &preserved, json!([165, 4])).await?;
            let next = recovered.command(&update, br#"{"delta":0}"#.to_vec(), CommandOptions {
                id: CommandId::new(ClientId::new([223; 16])?, 2)?, ..options(223)?
            }).await?;
            ensure!(next.result == b"165");
            check_rpc_read(&recovered, 217, &next.version, json!([165, 5])).await?;
            let factories = calls.load(Ordering::Acquire);
            ensure!(recovered.command(&sequence, serde_json::to_vec(&json!({"delta":100,"gate":4,"after":1000}))?, options(223)?).await.is_err());
            ensure!(calls.load(Ordering::Acquire) == factories);
            check_rpc_read(&recovered, 216, &next.version, json!([165, 5])).await?;

            // Admission is owned before this response future is returned.
            // Dropping it cannot cancel the node's root or final receipt.
            let lost_options = options(218)?;
            let lost_input = br#"{"delta":1}"#.to_vec();
            drop(recovered.command(&update, lost_input.clone(), lost_options.clone()));
            let request = CellReceiptRequest {
                id: lost_options.id, contract: update.clone(), input: lost_input.clone(), durability: Durability::Quorum,
            };
            let completed = cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    match recovered.receipt(request.clone(), TIMEOUT).await {
                        Ok(proof) if proof.receipt().is_some() => break Ok::<_, anyhow::Error>(proof),
                        Ok(_) => (),
                        Err(error) if matches!(&*error, CellDispatchError::Execution(ExecutionError::Uncertain)
                            | CellDispatchError::Runtime(RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Overloaded)) => (),
                        Err(error) => return Err(error.into()),
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            }).await??;
            ensure!(completed.receipt().context("unpolled RPC receipt")?.result() == b"166");
            ensure!(calls.load(Ordering::Acquire) == factories);
            let replay = cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    match recovered.command(&update, lost_input.clone(), lost_options.clone()).await {
                        Ok(receipt) => break Ok::<_, anyhow::Error>(receipt),
                        Err(error) if error.cause.refusal() == Some(&CommandFailure::Overloaded) => (),
                        Err(error) => return Err(error.into()),
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            }).await??;
            ensure!(replay.result == b"166" && replay.version == completed.observation().version);
            ensure!(calls.load(Ordering::Acquire) == factories);
            check_rpc_read(&recovered, 215, &replay.version, json!([166, 5])).await?;

            // Explicit sync publishes preceding SQL/KV before the response.
            // S3 additionally requires the same exact checkpoint archive proof.
            let sync = recovered.contract("Sync")?;
            let sync_input = serde_json::to_vec(&json!({"delta":10,"gate":5,"after":20,"ordered":true}))?;
            let strong = CommandOptions { durability: Durability::S3, ..multiple(209)? };
            let (synced, first) = tokio::join!(
                recovered.command(&sync, sync_input.clone(), strong.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[5].entered.notified()).await?;
                    let observed = recovered.query(
                        ProposalId::new([208; 16])?, &recovered.contract("Get")?, Vec::new(),
                        QueryOptions::default(),
                    ).await?;
                    ensure!(serde_json::from_slice::<serde_json::Value>(&observed.result)? == json!([176,5]));
                    ensure!(observed.version.sequence.get() == replay.version.sequence.get() + 2);
                    gates[5].release.notify_one();
                    Ok::<_, anyhow::Error>(observed.version)
                },
            );
            let first = first?;
            let synced = synced?;
            ensure!(synced.result == b"196" && synced.durability == Durability::S3);
            ensure!(synced.version.sequence.get() == first.sequence.get() + 1);
            let factories = calls.load(Ordering::Acquire);
            let same = recovered.command(&sync, sync_input, strong).await?;
            ensure!(same.result == synced.result && same.version == synced.version);
            ensure!(calls.load(Ordering::Acquire) == factories);

            let rejected_input = serde_json::to_vec(&json!({"delta":100,"after":1000,"reject":true}))?;
            ensure!(recovered.command(&sync, rejected_input.clone(), multiple(207)?).await.is_err());
            let preserved = recovered.query(
                ProposalId::new([206; 16])?, &recovered.contract("Get")?, Vec::new(), QueryOptions::default(),
            ).await?;
            ensure!(serde_json::from_slice::<serde_json::Value>(&preserved.result)? == json!([296,5]));
            ensure!(preserved.version.sequence.get() == synced.version.sequence.get() + 1);
            let factories = calls.load(Ordering::Acquire);
            ensure!(recovered.command(&sync, rejected_input, multiple(207)?).await.is_err());
            ensure!(calls.load(Ordering::Acquire) == factories);

            // A dropped, never-polled sync future still owns its queued commit.
            // Observe that commit, then revoke leadership before the handler resumes.
            let lost_input = serde_json::to_vec(&json!({"delta":100,"gate":6,"after":1000,"detached":true}))?;
            let (lost, moved) = tokio::join!(
                recovered.command(&sync, lost_input.clone(), multiple(205)?),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[6].entered.notified()).await?;
                    let committed = cellar::asyncrt::timeout(TIMEOUT, async {
                        loop {
                            let observed = recovered.query(
                                ProposalId::new([204; 16])?, &recovered.contract("Get")?, Vec::new(), QueryOptions::default(),
                            ).await?;
                            if serde_json::from_slice::<serde_json::Value>(&observed.result)? == json!([396,6]) {
                                break Ok::<_, anyhow::Error>(observed.version);
                            }
                            cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                        }
                    }).await??;
                    ensure!(committed.sequence.get() == preserved.version.sequence.get() + 1);
                    let pending = recovered.receipt(CellReceiptRequest {
                        id: multiple(205)?.id, contract: sync.clone(), input: lost_input.clone(), durability: Durability::Quorum,
                    }, TIMEOUT).await;
                    ensure!(matches!(pending, Err(error) if matches!(&*error, CellDispatchError::Execution(ExecutionError::Uncertain))));
                    let handle = cluster.hosts[destination].group(group)?;
                    let status = handle.replication_status(TIMEOUT).await?;
                    let successor = (destination + 1) % 3;
                    handle.transfer_leadership(LeadershipTransfer {
                        target: MemberId::new(successor as u64 + 1)?, expected_term: status.term,
                        expected_revision: status.membership_revision,
                    }, TIMEOUT).await?;
                    cellar::asyncrt::timeout(TIMEOUT, gates[6].dropped.notified()).await?;
                    gates[6].release.notify_one();
                    Ok::<_, anyhow::Error>((successor, committed))
                },
            );
            ensure!(lost.is_err() && gates[6].resumed.load(Ordering::Acquire) == 0);
            let (successor, committed) = moved?;
            let final_cell = cluster.hosts[successor].cell(recovered.history().clone())?;
            check_rpc_read(&final_cell, 203, &committed, json!([396,6])).await?;
            let factories = calls.load(Ordering::Acquire);
            ensure!(final_cell.command(&sync, lost_input, multiple(205)?).await.is_err());
            ensure!(calls.load(Ordering::Acquire) == factories);
            check_rpc_read(&final_cell, 202, &committed, json!([396,6])).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("Raft RPC execution domain")
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained ordinary cell RPC disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

async fn check_rpc_read(
    cell: &RaftCellHandle,
    proposal: u8,
    version: &CellVersion,
    expected: serde_json::Value,
) -> Result<()> {
    let observed = cell
        .query(
            ProposalId::new([proposal; 16])?,
            &cell.contract("Get")?,
            Vec::new(),
            QueryOptions::default(),
        )
        .await?;
    ensure!(&observed.version == version);
    ensure!(serde_json::from_slice::<serde_json::Value>(&observed.result)? == expected);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_json_rpc_uses_raft_owned_context() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(run_native_cell_rpc(
            rooted,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await
        .with_context(|| format!("ordinary Raft RPC extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_ordinary_json_rpc_uses_raft_owned_context() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "ordinary-primitives",
            rooted,
        )?));
        Box::pin(run_native_cell_rpc(rooted, store))
            .await
            .with_context(|| format!("ordinary Raft RPC RustFS extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_cell_activation_cache_is_bounded_and_fenced() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(activations::run(
            rooted,
            Arc::new(catalog_fault::Store::default()),
        ))
        .await
        .with_context(|| format!("activation cache extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_ordinary_cell_activation_cache_is_bounded_and_fenced() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(catalog_fault::Store::around(rustfs::store(
            "ordinary-activations",
            rooted,
        )?));
        Box::pin(activations::run(rooted, store))
            .await
            .with_context(|| format!("activation cache RustFS extents={rooted}"))?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_cell_rpcs_interleave_between_proven_roots() -> Result<()> {
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            Box::pin(interleaving::run(
                rooted,
                durability,
                Arc::new(catalog_fault::Store::default()),
            ))
            .await
            .with_context(|| format!("interleaving extents={rooted} durability={durability:?}"))?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_ordinary_cell_rpcs_interleave_between_proven_roots() -> Result<()> {
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            let store = Arc::new(catalog_fault::Store::around(rustfs::store(
                if durability == Durability::S3 {
                    "ordinary-interleaving-s3"
                } else {
                    "ordinary-interleaving-quorum"
                },
                rooted,
            )?));
            Box::pin(interleaving::run(rooted, durability, store))
                .await
                .with_context(|| {
                    format!("RustFS interleaving extents={rooted} durability={durability:?}")
                })?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_cell_sdk_dispatch_requires_source_proof() -> Result<()> {
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            Box::pin(dispatch::run(
                rooted,
                durability,
                Arc::new(catalog_fault::Store::default()),
            ))
            .await
            .with_context(|| format!("SDK dispatch extents={rooted} durability={durability:?}"))?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_ordinary_cell_sdk_dispatch_requires_source_proof() -> Result<()> {
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            let store = Arc::new(catalog_fault::Store::around(rustfs::store(
                if durability == Durability::S3 {
                    "ordinary-dispatch-s3"
                } else {
                    "ordinary-dispatch-quorum"
                },
                rooted,
            )?));
            Box::pin(dispatch::run(rooted, durability, store))
                .await
                .with_context(|| {
                    format!("RustFS SDK dispatch extents={rooted} durability={durability:?}")
                })?;
        }
    }
    Ok(())
}
