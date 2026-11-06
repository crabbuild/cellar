// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQL consistency and authority at the public shared-group SDK boundary.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_shared_read_cluster.rs"]
mod cluster;
#[path = "support/cell_group_disaster_public_node.rs"]
mod public_node;

use anyhow::{Context as _, Result};
use cellar::application::consensus::verification::cell_groups::{
    CellRoutingError, CellRoutingFailure,
};
use cellar::sdk::{consensus::*, Application, Output, Registry};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const TIMEOUT: Duration = Duration::from_secs(5);
type Query = dyn for<'a> Fn(&QueryContext<'a>, &[u8]) -> Result<Vec<u8>> + Send + Sync;

struct Definition {
    schema: [u8; 32],
    increment: OperationContract,
    get: OperationContract,
    application: Arc<dyn Application>,
}

impl Definition {
    fn new(query: Arc<Query>) -> Result<Self> {
        let class = RaftClass::new("Counter", 1, [21; 32])?
            .initializer([24; 32], |_, sql| {
                sql.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)",
                )?;
                Ok(())
            })?
            .command("Increment", [22; 32], |context, input| {
                let delta = i64::from_be_bytes(input.try_into()?);
                context
                    .sql()
                    .execute("UPDATE counter SET value=value+?1", [delta])?;
                let value: i64 =
                    context
                        .sql()
                        .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })?
            .query("Get", [23; 32], move |context, input| query(context, input))?;
        Ok(Self {
            schema: class.schema_digest(),
            increment: class.contract("Increment").context("increment contract")?,
            get: class.contract("Get").context("get contract")?,
            application: Arc::new(
                Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
            ),
        })
    }
}

fn get(context: &QueryContext<'_>, _: &[u8]) -> Result<Vec<u8>> {
    let value: i64 = context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    Ok(value.to_be_bytes().to_vec())
}

fn query_id() -> ProposalId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    ProposalId::new(u128::from(sequence).to_be_bytes()).expect("nonzero query ID")
}

fn assert_unavailable(result: Result<QueryResult, Arc<CellDispatchError>>) {
    let error = result.expect_err("read must refuse without its required consistency proof");
    assert!(
        error.refusal() == Some(&CommandFailure::VersionUnavailable)
            || matches!(
                error.as_ref(),
                CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                    | CellDispatchError::Routing(CellRoutingError {
                        reason: CellRoutingFailure::Deadline,
                        ..
                    })
            ),
        "unexpected read failure: {error:?}"
    );
}

fn at_least(receipt: &CommandReceipt) -> QueryOptions {
    QueryOptions {
        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
        routing: QueryRouting::Local,
        ..Default::default()
    }
}

async fn increment(
    cell: &RaftCellHandle,
    definition: &Definition,
    sequence: u64,
    delta: i64,
) -> Result<CommandReceipt> {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let options = CommandOptions::new(
        CommandId::new(ClientId::new([7; 16])?, sequence)?,
        now + 60_000,
    );
    Ok(cell
        .command(&definition.increment, delta.to_be_bytes().to_vec(), options)
        .await?)
}

struct Fixture {
    cluster: cluster::Cluster,
    handles: Vec<RaftCellHandle>,
    definition: Definition,
    initial: CommandReceipt,
}

impl Fixture {
    async fn start(query: Arc<Query>) -> Result<Self> {
        let definition = Definition::new(query)?;
        let cluster = cluster::Cluster::start(&definition).await?;
        let cell = CellHistory {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            cell: CellId::new("Counter:one")?,
            incarnation: IncarnationId::new([7; 16])?,
        };
        let handles = cluster
            .services
            .iter()
            .map(|service| service.cell(cell.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let primary = cluster.leader(None).await?;
        handles[primary]
            .enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([8; 16])?,
                    durability: Durability::Quorum,
                },
                TIMEOUT,
            )
            .await?;
        // Verify every voter can read the committed enrollment locally before
        // the first write. All fault cases start from this complete RF3 cut.
        for (index, handle) in handles.iter().enumerate() {
            let ready = handle
                .query(
                    query_id(),
                    &definition.get,
                    vec![],
                    QueryOptions {
                        routing: QueryRouting::Local,
                        ..Default::default()
                    },
                )
                .await
                .with_context(|| format!("verify enrolled cell on voter {index}"))?;
            assert_eq!(ready.served_by, MemberId::new(index as u64 + 1)?);
            assert_eq!(ready.result, 0_i64.to_be_bytes());
        }
        let primary = cluster.leader(None).await?;
        let initial = increment(&handles[primary], &definition, 1, 1)
            .await
            .context("commit the baseline write")?;
        for handle in &handles {
            let result = handle
                .query(query_id(), &definition.get, vec![], at_least(&initial))
                .await?;
            assert_eq!(result.result, 1_i64.to_be_bytes());
        }
        Ok(Self {
            cluster,
            handles,
            definition,
            initial,
        })
    }

    async fn write(&self, leader: usize, sequence: u64, delta: i64) -> Result<CommandReceipt> {
        increment(&self.handles[leader], &self.definition, sequence, delta).await
    }

    fn group(&self, replica: usize) -> Result<RaftGroupHandle> {
        Ok(self.cluster.services[replica].group(self.cluster.history)?)
    }

    async fn close(self) -> Result<()> {
        self.cluster.close().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_your_writes_follower() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = fixture.cluster.leader(None).await?;
    let follower = (primary + 1) % 3;
    fixture.cluster.isolate(follower).await?;
    let written = fixture.write(primary, 2, 4).await?;
    assert_eq!(written.result, 5_i64.to_be_bytes());
    let pending = fixture.handles[follower].query(
        query_id(),
        &fixture.definition.get,
        vec![],
        at_least(&written),
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    let historical = fixture.handles[follower]
        .query(
            query_id(),
            &fixture.definition.get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                routing: QueryRouting::Local,
                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(historical.result, 1_i64.to_be_bytes());
    fixture.cluster.heal();
    let read = pending.await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert!(read.version.covers(&written.version)?);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remote_query_wait_cannot_block_its_ltx_catchup() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = fixture.cluster.leader(None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    fixture.cluster.isolate_consensus(target).await?;
    let newer = fixture.write(primary, 2, 4).await?;
    let options = QueryOptions {
        routing: QueryRouting::Replicas {
            members: vec![MemberId::new(target as u64 + 1)?],
            attempt_timeout: TIMEOUT,
            leader_fallback: false,
        },
        timeout: TIMEOUT,
        ..at_least(&newer)
    };
    let query = fixture.handles[source].query(query_id(), &fixture.definition.get, vec![], options);
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.cluster.services[target]
            .resource_usage()
            .runtime
            .read_pins
            .items
            == 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    fixture.cluster.heal();
    let read = query.await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(target as u64 + 1)?);
    assert!(read.version.covers(&newer.version)?);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lost_read_index_response_retries_without_leader_change() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = fixture.cluster.leader(None).await?;
    let follower = (primary + 1) % 3;
    fixture.handles[follower]
        .query(
            query_id(),
            &fixture.definition.get,
            vec![],
            at_least(&fixture.initial),
        )
        .await?;
    let before = fixture.group(follower)?.status().await?;
    fixture.cluster.block_control(primary, follower).await?;
    let pending = fixture.handles[follower].query(
        query_id(),
        &fixture.definition.get,
        vec![],
        QueryOptions {
            consistency: QueryConsistency::Linearizable,
            routing: QueryRouting::Local,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
    );
    let membership = fixture
        .group(follower)?
        .quorum_membership(Duration::from_secs(2));
    // Both requests can reach the leader, and the third voter supplies its safe
    // ReadIndex majority. Only the reply direction is unavailable. Heal before
    // the follower's minimum election timeout, keeping the term/leader fixed.
    let pending = async { tokio::join!(pending, membership) };
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(
        fixture.cluster.services[follower]
            .resource_usage()
            .runtime
            .read_pins
            .items,
        2,
        "both ReadIndex requests must be in flight before healing"
    );
    fixture.cluster.heal();
    let (result, membership) = pending.await;
    let membership = membership.context("retry forwarded membership ReadIndex")?;
    assert_eq!(membership.barrier().term, before.term);
    assert_eq!(
        membership.replica().member,
        MemberId::new(follower as u64 + 1)?
    );
    assert_eq!(membership.configuration().voters.len(), 3);
    drop(membership);
    let after = fixture.group(follower)?.status().await?;
    assert_eq!(after.term, before.term);
    assert_eq!(after.leader, before.leader);
    fixture.close().await?;
    assert_eq!(
        result
            .context("a lost ReadIndex response must be retried under the original invocation")?
            .result,
        1i64.to_be_bytes()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn linearizable_read_barrier() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let old = fixture.cluster.leader(None).await?;
    let written = fixture.write(old, 2, 4).await?;
    let options = QueryOptions {
        consistency: QueryConsistency::Linearizable,
        ..Default::default()
    };
    for (i, handle) in fixture.handles.iter().enumerate() {
        let read = handle
            .query(query_id(), &fixture.definition.get, vec![], options.clone())
            .await
            .with_context(|| format!("initial linearizable read on replica {i}"))?;
        assert_eq!(read.result, 5_i64.to_be_bytes());
        assert!(read.version.covers(&written.version)?);
    }
    fixture.cluster.isolate(old).await?;
    let next = fixture.cluster.leader(Some(old)).await?;
    // The new term commits a physical barrier without mutating this cell.
    let read = fixture.handles[next]
        .query(query_id(), &fixture.definition.get, vec![], options.clone())
        .await
        .context("new leader protocol-only cut")?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.version, written.version);
    assert_eq!(read.witness, written.witness);
    let barrier = fixture.group(next)?.status().await?.applied;
    assert!(barrier.index > written.witness.position.index);
    assert!(barrier.term > written.witness.position.term);
    let newer = fixture.write(next, 3, 7).await?;
    assert_eq!(newer.result, 12_i64.to_be_bytes());
    for (i, handle) in fixture
        .handles
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != old)
    {
        let read = handle
            .query(query_id(), &fixture.definition.get, vec![], options.clone())
            .await
            .with_context(|| format!("post-election linearizable read on replica {i}"))?;
        assert_eq!(read.result, 12_i64.to_be_bytes(), "replica {i}");
        assert!(read.version.covers(&newer.version)?);
    }
    fixture.cluster.heal();
    let read = fixture.handles[old]
        .query(query_id(), &fixture.definition.get, vec![], options)
        .await
        .context("healed former leader linearizable read")?;
    assert_eq!(read.result, 12_i64.to_be_bytes());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn minority_strong_read_refused() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = fixture.cluster.leader(None).await?;
    let follower = (primary + 1) % 3;
    fixture.cluster.isolate(follower).await?;
    let newer = fixture.write(primary, 2, 4).await?;
    let strong = QueryOptions {
        consistency: QueryConsistency::Linearizable,
        timeout: Duration::from_millis(250),
        ..Default::default()
    };
    assert_unavailable(
        fixture.handles[follower]
            .query(query_id(), &fixture.definition.get, vec![], strong)
            .await,
    );
    let mut minimum = at_least(&newer);
    minimum.timeout = Duration::from_millis(250);
    assert_unavailable(
        fixture.handles[follower]
            .query(query_id(), &fixture.definition.get, vec![], minimum)
            .await,
    );
    // A minimum-version read can legitimately use an already-covered prefix
    // without quorum; it must not be confused with a fresh linearizable read.
    assert_eq!(
        fixture.handles[follower]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                at_least(&fixture.initial)
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    assert_unavailable(
        fixture.handles[follower]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                QueryOptions::default(),
            )
            .await,
    );
    assert_eq!(
        fixture.handles[follower]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Stale,
                    routing: QueryRouting::Local,
                    stale_policy: StaleReadPolicy::DisconnectedHistorical,
                    ..Default::default()
                }
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    fixture.cluster.heal();
    assert_eq!(
        fixture.handles[follower]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                at_least(&newer)
            )
            .await?
            .result,
        5_i64.to_be_bytes()
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn query_write_capability_denied() -> Result<()> {
    let query = Arc::new(|context: &QueryContext<'_>, input: &[u8]| {
        if input == b"attack" {
            for sql in [
                "UPDATE counter SET value=999 RETURNING value",
                "CREATE TABLE injected(value INTEGER)",
                "PRAGMA journal_mode=WAL",
                "ATTACH ':memory:' AS injected",
                "DELETE FROM counter RETURNING value",
            ] {
                let error = context
                    .query_row(sql, [], |row| row.get::<_, i64>(0))
                    .unwrap_err();
                let sqlite = error
                    .downcast_ref::<rusqlite::Error>()
                    .ok_or_else(|| anyhow::anyhow!("missing SQLite refusal: {error}"))?;
                assert!(
                    matches!(sqlite, rusqlite::Error::SqliteFailure(error, _) if error.code == rusqlite::ErrorCode::AuthorizationForStatementDenied)
                );
            }
        }
        get(context, input)
    });
    let fixture = Fixture::start(query).await?;
    for handle in &fixture.handles {
        let result = handle
            .query(
                query_id(),
                &fixture.definition.get,
                b"attack".to_vec(),
                at_least(&fixture.initial),
            )
            .await?;
        assert_eq!(result.result, 1_i64.to_be_bytes());
    }
    let primary = fixture.cluster.leader(None).await?;
    assert_eq!(
        fixture.write(primary, 2, 4).await?.result,
        5_i64.to_be_bytes()
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn unpolled_query_deadline_interrupts_sql_and_joins_work() -> Result<()> {
    let (entered, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let query = Arc::new(move |context: &QueryContext<'_>, input: &[u8]| {
        if input == b"slow" {
            entered.send(())?;
            let _: i64 = context.query_row("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n", [], |row| row.get(0))?;
        }
        get(context, input)
    });
    let fixture = Fixture::start(query).await?;
    let primary = fixture.cluster.leader(None).await?;
    let pending = fixture.handles[primary].query(
        query_id(),
        &fixture.definition.get,
        b"slow".to_vec(),
        QueryOptions {
            timeout: Duration::from_millis(150),
            ..at_least(&fixture.initial)
        },
    );
    tokio::time::timeout(Duration::from_secs(5), receive.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("query missing"))?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while !fixture.cluster.services[primary]
            .resource_usage()
            .sql
            .is_some_and(|usage| usage.queued_or_running == 0)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_unavailable(pending.await);
    assert_eq!(
        fixture.handles[primary]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                at_least(&fixture.initial)
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    fixture.close().await
}
