// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQL consistency and authority at the public shared-group SDK boundary.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_shared_read_cluster.rs"]
mod cluster;
#[path = "support/cell_group_disaster_public_node.rs"]
mod public_node;
#[path = "support/raft_read_gate.rs"]
mod read_gate;

use anyhow::{Context as _, Result};
use cellar::application::consensus::verification::cell_groups::{
    CellRoutingError, CellRoutingFailure,
};
use cellar::application::consensus::verification::TransportError;
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

#[derive(Clone)]
struct Definition {
    schema: [u8; 32],
    serves_queries: bool,
    increment: OperationContract,
    get: OperationContract,
    application: Arc<dyn Application>,
}

impl Definition {
    fn new(query: Option<Arc<Query>>) -> Result<Self> {
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
            })?;
        let serves_queries = query.is_some();
        let class = if let Some(query) = query {
            class.query("Get", [23; 32], move |context, input| query(context, input))?
        } else {
            class
        };
        Ok(Self {
            schema: class.schema_digest(),
            serves_queries,
            increment: class.contract("Increment").context("increment contract")?,
            get: OperationContract {
                operation: "Get".into(),
                kind: OperationKind::Query,
                digest: [23; 32],
            },
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

fn read_deadline(error: &CellDispatchError) -> bool {
    error.refusal() == Some(&CommandFailure::VersionUnavailable)
        || matches!(
            error,
            CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                | CellDispatchError::Transport(TransportError::Timeout)
                | CellDispatchError::Routing(CellRoutingError {
                    reason: CellRoutingFailure::Deadline,
                    ..
                })
        )
}

fn assert_unavailable(result: Result<QueryResult, Arc<CellDispatchError>>) {
    let error = result.expect_err("read must refuse without its required consistency proof");
    assert!(read_deadline(&error), "unexpected read failure: {error:?}");
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
        Self::start_with_limits(query, public_node::limits(), RaftHostOptions::default()).await
    }

    async fn start_with_limits(
        query: Arc<Query>,
        shared: CellGroupNodeLimits,
        host: RaftHostOptions,
    ) -> Result<Self> {
        let definition = Definition::new(Some(query))?;
        Self::start_definitions(std::array::from_fn(|_| definition.clone()), shared, host).await
    }

    async fn start_definitions(
        definitions: [Definition; 3],
        shared: CellGroupNodeLimits,
        host: RaftHostOptions,
    ) -> Result<Self> {
        let definition = definitions[0].clone();
        let cluster = cluster::Cluster::start(&definitions, shared, host).await?;
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
        let enrollment = cluster.services[primary]
            .group(cluster.history)?
            .status()
            .await?
            .applied
            .index;
        // Every voter starts from the committed enrollment. Voters with query
        // code also serve the initial SQL image through their local SDK.
        for (index, handle) in handles.iter().enumerate() {
            if !definitions[index].serves_queries {
                cluster.services[index]
                    .group(cluster.history)?
                    .quorum_membership_after(enrollment, TIMEOUT)
                    .await?;
                continue;
            }
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
        for (index, handle) in handles.iter().enumerate() {
            if !definitions[index].serves_queries {
                cluster.services[index]
                    .group(cluster.history)?
                    .quorum_membership_after(initial.witness.position.index, TIMEOUT)
                    .await?;
                continue;
            }
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

    fn sql_idle(&self, replica: usize) -> bool {
        self.cluster.services[replica]
            .resource_usage()
            .sql
            .is_some_and(|usage| usage.queued_or_running == 0)
    }

    async fn join_reads(&self, replicas: &[usize], timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            while replicas.iter().any(|replica| {
                let usage = self.cluster.services[*replica].resource_usage();
                usage.host.controller.retained_commands != 0
                    || usage.runtime.read_pins.items != 0
                    || !usage.sql.is_some_and(|sql| sql.queued_or_running == 0)
            }) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("join read work")
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
    let former_leader = fixture.group(old)?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = former_leader.status().await?;
            if status.term == barrier.term
                && status.leader == Some(next as u64 + 1)
                && status.applied.index >= newer.witness.position.index
            {
                return Ok::<_, RuntimeError>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("former leader did not catch up after healing")??;
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

fn routed(receipt: &CommandReceipt, member: MemberId, fallback: bool) -> QueryOptions {
    QueryOptions {
        routing: QueryRouting::Replicas {
            members: vec![member],
            attempt_timeout: Duration::from_millis(180),
            leader_fallback: fallback,
        },
        timeout: Duration::from_secs(2),
        ..at_least(receipt)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fallback_preserves_contract() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = fixture.cluster.leader(None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    let target_member = MemberId::new(target as u64 + 1)?;
    let primary_member = MemberId::new(primary as u64 + 1)?;
    fixture.cluster.isolate_consensus(target).await?;
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());

    assert_unavailable(
        fixture.handles[source]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                routed(&newer, target_member, false),
            )
            .await,
    );
    for consistency in [
        QueryConsistency::AtLeast(newer.version_token.clone()),
        QueryConsistency::Linearizable,
    ] {
        let read = fixture.handles[source]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                QueryOptions {
                    consistency,
                    ..routed(&newer, target_member, true)
                },
            )
            .await?;
        assert_eq!(read.served_by, primary_member);
        assert_eq!(read.result, 5_i64.to_be_bytes());
        assert!(read.version.covers(&newer.version)?);
    }

    let timeout = Duration::from_millis(100);
    let short = |attempt_timeout| QueryOptions {
        timeout,
        routing: QueryRouting::Replicas {
            members: vec![target_member],
            attempt_timeout,
            leader_fallback: true,
        },
        ..at_least(&newer)
    };
    let error = fixture.handles[source]
        .query(
            query_id(),
            &fixture.definition.get,
            vec![],
            short(Duration::from_secs(1)),
        )
        .await
        .expect_err("attempt longer than the invocation must refuse admission");
    assert!(matches!(
        error.as_ref(),
        CellDispatchError::Invalid("query replica routing bounds")
    ));
    assert_unavailable(
        tokio::time::timeout(
            Duration::from_millis(500),
            fixture.handles[source].query(
                query_id(),
                &fixture.definition.get,
                vec![],
                short(timeout),
            ),
        )
        .await?,
    );
    fixture
        .join_reads(&[source, target], Duration::from_millis(500))
        .await?;
    fixture.cluster.heal();
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delayed_tcp_query_reply_cannot_settle_a_later_read() -> Result<()> {
    let (gate, mut entered) = read_gate::Gate::new();
    let release = read_gate::Release(gate.clone());
    let fixture = Fixture::start(Arc::new(move |context, input| {
        if input == b"hold" {
            gate.wait()?;
        }
        get(context, input)
    }))
    .await?;
    let primary = fixture.cluster.leader(None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    let target_member = MemberId::new(target as u64 + 1)?;
    let timeout = Duration::from_secs(1);
    let options = QueryOptions {
        timeout,
        routing: QueryRouting::Replicas {
            members: vec![target_member],
            attempt_timeout: timeout,
            leader_fallback: false,
        },
        ..at_least(&fixture.initial)
    };
    let warm = fixture.handles[source]
        .query(query_id(), &fixture.definition.get, vec![], options.clone())
        .await?;
    assert_eq!(warm.served_by, target_member);

    let delayed = fixture.handles[source].query(
        query_id(),
        &fixture.definition.get,
        b"hold".to_vec(),
        options.clone(),
    );
    tokio::time::timeout(Duration::from_secs(2), entered.recv())
        .await?
        .context("remote query did not enter")?;
    let held = fixture.cluster.hold_query_responses(source, target)?;
    release.0.release();
    tokio::time::timeout(Duration::from_millis(750), async {
        while fixture.cluster.held_query_response_bytes(source, target)? == 0
            || !fixture.sql_idle(target)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    assert_unavailable(tokio::time::timeout(Duration::from_secs(2), delayed).await?);

    drop(held);
    let read = fixture.handles[source]
        .query(
            query_id(),
            &fixture.definition.get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(newer.version_token.clone()),
                ..options
            },
        )
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, target_member);
    assert!(read.version.covers(&newer.version)?);
    fixture
        .join_reads(&[0, 1, 2], Duration::from_secs(2))
        .await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn follower_drains_committed_batch_after_held_reader() -> Result<()> {
    let (gate, mut entered) = read_gate::Gate::new();
    let release = read_gate::Release(gate.clone());
    let fixture = Fixture::start(Arc::new(move |context, input| {
        if input == b"hold" {
            gate.wait()?;
        }
        get(context, input)
    }))
    .await?;
    let result = async {
        let primary = fixture.cluster.leader(None).await?;
        let follower = (primary + 1) % 3;
        let pending = fixture.handles[follower].query(
            query_id(),
            &fixture.definition.get,
            b"hold".to_vec(),
            QueryOptions {
                timeout: Duration::from_secs(30),
                ..at_least(&fixture.initial)
            },
        );
        tokio::time::timeout(TIMEOUT, entered.recv())
            .await?
            .context("follower reader did not enter")?;
        let mut newest = fixture.initial.clone();
        // The pinned follower read must coexist with quorum writes through the
        // other voters. Its original SQLite image must remain coherent.
        for sequence in 2..=33 {
            newest = fixture
                .write(primary, sequence, 1)
                .await
                .with_context(|| format!("write backlog sequence {sequence}"))?;
            assert_eq!(newest.result, (sequence as i64).to_be_bytes());
        }
        release.0.release();
        assert_eq!(pending.await?.result, 1_i64.to_be_bytes());
        for (index, handle) in fixture.handles.iter().enumerate() {
            let result = handle
                .query(
                    query_id(),
                    &fixture.definition.get,
                    vec![],
                    QueryOptions {
                        timeout: Duration::from_secs(10),
                        ..at_least(&newest)
                    },
                )
                .await;
            if let Err(error) = &result {
                let mut statuses = Vec::new();
                for replica in 0..fixture.handles.len() {
                    statuses.push(fixture.group(replica)?.status().await);
                }
                anyhow::bail!(
                    "replica {index} did not drain writes: {error:?}; groups={statuses:?}"
                );
            }
            let read = result?;
            assert_eq!(read.result, 33_i64.to_be_bytes());
            assert!(read.version.covers(&newest.version)?);
        }
        let final_write = fixture.write(primary, 34, 1).await?;
        let read = fixture.handles[follower]
            .query(
                query_id(),
                &fixture.definition.get,
                vec![],
                at_least(&final_write),
            )
            .await?;
        assert_eq!(read.result, 34_i64.to_be_bytes());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    release.0.release();
    let cleanup = fixture.close().await;
    result.and(cleanup)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stale_is_committed_prefix() -> Result<()> {
    let (gate, mut entered) = read_gate::Gate::new();
    let release = read_gate::Release(gate.clone());
    let fixture = Fixture::start(Arc::new(move |context, input| {
        let mut value = get(context, input)?;
        if input == b"hold" {
            let QueryVersion::Cell(observed) = context.version() else {
                anyhow::bail!("missing logical cell observation");
            };
            let index = observed.witness.position.index.get();
            gate.wait()?;
            assert_eq!(get(context, input)?, value);
            value.extend_from_slice(&index.to_be_bytes());
        }
        Ok(value)
    }))
    .await?;
    let primary = fixture.cluster.leader(None).await?;
    let follower = (primary + 1) % 3;
    let pending = fixture.handles[follower].query(
        query_id(),
        &fixture.definition.get,
        b"hold".to_vec(),
        QueryOptions {
            consistency: QueryConsistency::Stale,
            routing: QueryRouting::Local,
            stale_policy: StaleReadPolicy::DisconnectedHistorical,
            ..Default::default()
        },
    );
    tokio::time::timeout(TIMEOUT, entered.recv())
        .await?
        .context("query did not pin")?;
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    release.0.release();

    let read = pending.await?;
    assert_eq!(&read.result[..8], &1_i64.to_be_bytes());
    assert_eq!(
        u64::from_be_bytes(read.result[8..].try_into()?),
        read.witness.position.index.get()
    );
    assert_eq!(read.witness, fixture.initial.witness);
    assert_eq!(read.version, fixture.initial.version);
    assert!(read.witness.position.index < newer.witness.position.index);
    let latest = fixture.handles[follower]
        .query(
            query_id(),
            &fixture.definition.get,
            vec![],
            at_least(&newer),
        )
        .await?;
    assert_eq!(latest.result, 5_i64.to_be_bytes());
    assert!(latest.version.covers(&newer.version)?);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn completed_unpolled_responses_keep_their_byte_budget() -> Result<()> {
    const PAYLOAD: usize = 60_000;
    const REPLIES: usize = 16;
    let mut shared = public_node::limits();
    shared.controller.max_commands = 32;
    shared.controller.max_command_bytes = 1024 * 1024;
    shared.dispatch.max_invocations = 32;
    shared.dispatch.max_input_bytes = 4096;
    shared.sql.max_pending_cells = 32;
    shared.sql.group_pending_cells = 32;
    shared.sql.group_bytes = 1024 * 1024;
    // A small command envelope fits the same per-group SQL byte budget.
    let mut host = RaftHostOptions::default();
    host.execution.envelope.max_payload_bytes = 128 * 1024;
    host.archive.limits.envelope = host.execution.envelope;
    let fixture = Fixture::start_with_limits(
        Arc::new(|context, input| {
            if input == b"large" {
                Ok(vec![17; PAYLOAD])
            } else {
                get(context, input)
            }
        }),
        shared,
        host,
    )
    .await?;
    let primary = fixture.cluster.leader(None).await?;
    let query = || {
        fixture.handles[primary].query(
            query_id(),
            &fixture.definition.get,
            b"large".to_vec(),
            at_least(&fixture.initial),
        )
    };
    let mut responses = Vec::new();
    for count in 1..=REPLIES {
        responses.push(query());
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let usage = fixture.cluster.services[primary].resource_usage();
                if usage.sql.is_some_and(|sql| {
                    sql.queued_or_running == 0 && sql.retained_bytes == count * (PAYLOAD + 4096)
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("complete and retain unpolled response {count}"))?;
    }
    let error = query()
        .await
        .expect_err("held replies exhaust the SQL byte budget");
    assert_eq!(error.refusal(), Some(&CommandFailure::Overloaded));

    assert_eq!(responses.remove(0).await?.result, vec![17; PAYLOAD]);
    assert_eq!(query().await?.result, vec![17; PAYLOAD]);
    drop(responses);
    fixture.join_reads(&[primary], TIMEOUT).await?;
    assert_eq!(
        fixture.cluster.services[primary]
            .resource_usage()
            .sql
            .context("SQL budget observation")?
            .retained_bytes,
        0
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn saturated_query_peer_refuses_and_expires_queued_work() -> Result<()> {
    use std::sync::atomic::AtomicUsize;
    let (gate, mut entered) = read_gate::Gate::new();
    let release = read_gate::Release(gate.clone());
    let unexpected = Arc::new(AtomicUsize::new(0));
    let executed = unexpected.clone();
    let mut shared = public_node::limits();
    shared.controller.max_commands = 96;
    shared.controller.max_command_bytes = 4 * 1024 * 1024;
    shared.dispatch.max_invocations = 96;
    shared.dispatch.retained_bytes = 4 * 1024 * 1024;
    let fixture = Fixture::start_with_limits(
        Arc::new(move |context, input| {
            if input == b"queued" {
                executed.fetch_add(1, Ordering::SeqCst);
            }
            if input == b"hold" {
                gate.wait()?;
            }
            get(context, input)
        }),
        shared,
        RaftHostOptions::default(),
    )
    .await?;
    let primary = fixture.cluster.leader(None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    let target_member = MemberId::new(target as u64 + 1)?;
    let options = |timeout| QueryOptions {
        timeout,
        routing: QueryRouting::Replicas {
            members: vec![target_member],
            attempt_timeout: timeout,
            leader_fallback: false,
        },
        ..at_least(&fixture.initial)
    };
    // Hold the real peer connection in its destination callback. Later calls
    // must consume finite peer bytes while queued behind this invocation.
    let first = fixture.handles[source].query(
        query_id(),
        &fixture.definition.get,
        b"hold".to_vec(),
        options(Duration::from_secs(5)),
    );
    tokio::time::timeout(Duration::from_secs(2), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("remote callback did not enter"))?;
    let source_usage = fixture.cluster.services[source]
        .resource_usage()
        .transport
        .context("source transport observer")?;
    let target_usage = fixture.cluster.services[target]
        .resource_usage()
        .transport
        .context("target transport observer")?;
    assert!(source_usage.outgoing.query.items > 0 && source_usage.outgoing.query.bytes > 0);
    assert!(target_usage.incoming.query.items > 0 && target_usage.incoming.query.bytes > 0);
    assert!(source_usage.outgoing_connections.query > 0);
    assert!(target_usage.incoming_connections.query > 0);
    assert!(
        target_usage.incoming_connections.query <= target_usage.incoming_connection_limits.query
    );
    assert!(source_usage.outgoing.query.bytes <= source_usage.outgoing.query.max_bytes);
    assert!(target_usage.incoming.query.bytes <= target_usage.incoming.query.max_bytes);
    let queued: Vec<_> = (0..64)
        .map(|_| {
            fixture.handles[source].query(
                query_id(),
                &fixture.definition.get,
                b"queued".to_vec(),
                options(Duration::from_secs(1)),
            )
        })
        .collect();
    let mut overloaded = 0;
    let mut expired = 0;
    // Poll all responses while queued work waits. An already refused
    // response still expires when the caller waits past its deadline.
    let replies = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::future::join_all(queued),
    )
    .await?;
    for response in replies {
        match response {
            Err(error)
                if matches!(
                    error.as_ref(),
                    CellDispatchError::Transport(TransportError::Overloaded)
                ) =>
            {
                overloaded += 1
            }
            Err(error) if read_deadline(&error) => expired += 1,
            result => anyhow::bail!("unexpected queued response: {result:?}"),
        }
    }
    assert!(overloaded > 0, "peer saturation was not observed");
    assert!(
        expired > 0,
        "no admitted request expired behind the held RPC"
    );
    assert_eq!(unexpected.load(Ordering::SeqCst), 0);
    // Consensus and LTX use separate connections/capacity. They must publish a
    // new command while the first query and expired peer queue are still held.
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    release.0.release();
    let first = first.await?;
    assert_eq!(first.result, 1_i64.to_be_bytes());
    assert_eq!(first.served_by, MemberId::new(target as u64 + 1)?);
    assert!(first.witness.position.index < newer.witness.position.index);
    let mut fresh = options(Duration::from_secs(2));
    fresh.consistency = QueryConsistency::AtLeast(newer.version_token.clone());
    let read = fixture.handles[source]
        .query(query_id(), &fixture.definition.get, vec![], fresh)
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert!(read.version.covers(&newer.version)?);
    assert_eq!(
        unexpected.load(Ordering::SeqCst),
        0,
        "expired queued request executed after the peer resumed"
    );
    fixture
        .join_reads(&[0, 1, 2], Duration::from_secs(2))
        .await?;
    let performance: Vec<_> = fixture
        .cluster
        .services
        .iter()
        .map(RaftCells::performance)
        .collect();
    assert!(performance
        .iter()
        .all(|sample| sample.available && !sample.saturated));
    for lane in ["control", "data", "query"] {
        for direction in ["read", "write"] {
            let name = format!("transport_{lane}_{direction}_bytes");
            let bytes: u64 = performance
                .iter()
                .map(|sample| {
                    sample
                        .counters
                        .iter()
                        .find(|counter| counter.name == name)
                        .unwrap()
                        .value
                })
                .sum();
            assert!(bytes > 0, "real TLS traffic missing from {name}");
        }
    }
    for lane in ["control", "data"] {
        for phase in ["queue", "exchange", "admit"] {
            let name = format!("transport_{lane}_{phase}");
            let successes: u64 = performance
                .iter()
                .map(|sample| {
                    sample
                        .operations
                        .iter()
                        .find(|operation| operation.name == name)
                        .unwrap()
                        .succeeded
                })
                .sum();
            assert!(successes > 0, "real TLS traffic missing from {name}");
        }
    }
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn voter_without_query_code_can_route_to_a_compatible_replica() -> Result<()> {
    let queryless = Definition::new(None)?;
    let registered = Definition::new(Some(Arc::new(get)))?;
    assert_eq!(queryless.schema, registered.schema);
    let fixture = Fixture::start_definitions(
        [queryless, registered.clone(), registered],
        public_node::limits(),
        RaftHostOptions::default(),
    )
    .await?;
    let queryless = &fixture.handles[0];
    assert!(matches!(
        queryless.contract("Get"),
        Err(RegistrationError::UnknownOperation(_))
    ));
    let local = queryless
        .query(
            query_id(),
            &fixture.definition.get,
            vec![],
            at_least(&fixture.initial),
        )
        .await
        .expect_err("queryless voter must refuse local execution");
    assert!(matches!(
        local.as_ref(),
        CellDispatchError::Registration(RegistrationError::UnknownOperation(_))
    ));

    let remote = QueryOptions {
        routing: QueryRouting::Replicas {
            members: vec![MemberId::new(1)?, MemberId::new(2)?],
            attempt_timeout: Duration::from_millis(500),
            leader_fallback: false,
        },
        ..at_least(&fixture.initial)
    };
    let read = queryless
        .query(query_id(), &fixture.definition.get, vec![], remote.clone())
        .await?;
    assert_eq!(read.result, 1_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(2)?);
    assert!(read.version.covers(&fixture.initial.version)?);

    let wrong = OperationContract {
        digest: [99; 32],
        ..fixture.definition.get.clone()
    };
    let refused = queryless
        .query(query_id(), &wrong, vec![], remote.clone())
        .await
        .expect_err("remote replica must check the exact query contract");
    assert_eq!(
        refused.refusal(),
        Some(&CommandFailure::UnsupportedCapability)
    );
    let refused = fixture.handles[1]
        .query(query_id(), &wrong, vec![], remote)
        .await
        .expect_err("origin must refuse a wrong digest for a known query");
    assert!(matches!(
        refused.as_ref(),
        CellDispatchError::Registration(RegistrationError::ContractMismatch)
    ));
    fixture.close().await
}
