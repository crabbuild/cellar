// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SDK/native/Axum reads use actual admission, Raft persistence and SQL callbacks.
use super::*;
use futures_util::{future::BoxFuture, FutureExt};
use std::sync::atomic::AtomicU64;

#[path = "cell_group_routing_read_gate.rs"]
mod gate;
use crate::storage_fault;

#[derive(Clone, Copy, Debug)]
enum Ingress {
    Sdk,
    Native,
    Axum,
}

#[derive(Debug)]
enum Read {
    Selected(serde_json::Value),
    TermRefused,
}

#[derive(Default)]
struct Waiters([Option<tokio::task::JoinHandle<Result<Read>>>; 2]);

impl Drop for Waiters {
    fn drop(&mut self) {
        for task in self.0.iter_mut().filter_map(Option::take) {
            task.abort();
        }
    }
}

impl Waiters {
    fn start(&mut self, index: usize, read: BoxFuture<'static, Result<Read>>) {
        assert!(self.0[index].is_none());
        self.0[index] = Some(tokio::spawn(read));
    }

    async fn finish(&mut self, index: usize) -> Result<Read> {
        self.0[index].take().context("public query waiter")?.await?
    }

    async fn cancel(&mut self, index: usize) -> Result<()> {
        let task = self.0[index].take().context("entered query waiter")?;
        task.abort();
        ensure!(task.await.unwrap_err().is_cancelled());
        Ok(())
    }
}

struct Drill<'a> {
    cluster: &'a nodes::Cluster,
    callbacks: &'a Arc<gate::Callbacks>,
    filesystem: &'a Arc<storage_fault::Gate>,
    client: reqwest::Client,
    queries: AtomicU64,
    retry: u64,
}

fn unix_ms() -> Result<u64> {
    Ok(u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

impl Drill<'_> {
    fn host(&self) -> &RaftCells {
        &self.cluster.managed.hosts[0]
    }

    fn history(&self, name: &str) -> Result<CellHistory> {
        let group = self.cluster.managed.history;
        counter::history(group.cluster, group.namespace, name)
    }

    async fn wait(&self, label: &str, mut ready: impl FnMut() -> Result<bool>) -> Result<()> {
        tokio::time::timeout(TIMEOUT, async {
            while !ready()? {
                self.callbacks.clean()?;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .with_context(|| label.to_owned())??;
        self.callbacks.clean()
    }

    fn sample(&self) -> Result<[u64; 2]> {
        let snapshot = self.host().performance();
        ensure!(snapshot.available && !snapshot.saturated);
        let mut values = [0; 2];
        for (n, name) in [
            "cell_linearizable_read_barriers",
            "cell_linearizable_read_cohort_joins",
        ]
        .into_iter()
        .enumerate()
        {
            values[n] = snapshot
                .counters
                .iter()
                .find(|c| c.name == name)
                .context("read observation")?
                .value;
        }
        Ok(values)
    }

    async fn drained(&self) -> Result<()> {
        self.wait("join physical query owners", || {
            let usage = self.host().resource_usage();
            Ok(usage.runtime.read_pins.items == 0 && usage.host.controller.ordinary.items == 0)
        })
        .await
    }

    fn read(&self, ingress: Ingress, name: &'static str) -> BoxFuture<'static, Result<Read>> {
        let ordinal = self.queries.fetch_add(1, Ordering::AcqRel) + 1;
        let cell = self
            .history(name)
            .and_then(|history| self.host().cell(history).map_err(Into::into));
        let url = self
            .cluster
            .web()
            .map(|web| web.url(0, matches!(ingress, Ingress::Axum), name));
        let client = self.client.clone();
        async move {
            eprintln!("public cohort request ingress={ingress:?} cell={name} ordinal={ordinal}");
            let invocation = ProposalId::new((1000_u128 + u128::from(ordinal)).to_be_bytes())?;
            if matches!(ingress, Ingress::Sdk) {
                let cell = cell?;
                eprintln!("public SDK cohort invocation={invocation:?}");
                let result = cell
                    .query(
                        invocation,
                        &cell.contract("Get")?,
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::Linearizable,
                            routing: QueryRouting::Local,
                            max_attempts: 1,
                            timeout: TIMEOUT,
                            ..Default::default()
                        },
                    )
                    .await;
                return match result {
                    Ok(reply) => Ok(Read::Selected(serde_json::json!({
                        "cell": reply.version.history.cell.as_str(),
                        "value": i64::from_be_bytes(reply.result.as_slice().try_into()?),
                        "sequence": reply.version.sequence.get(),
                        "version_token": reply.version_token.as_str(),
                        "served_by": reply.served_by.get(),
                    }))),
                    Err(error)
                        if matches!(
                            error.as_ref(),
                            CellDispatchError::Runtime(RuntimeError::NotLeader)
                        ) =>
                    {
                        Ok(Read::TermRefused)
                    }
                    Err(error) => Err(anyhow::anyhow!("SDK query refused: {error}")),
                };
            }
            let reply = http::Web::send(
                client
                    .get(url?)
                    .header("x-read-consistency", "linearizable")
                    .header("x-query-members", "1")
                    .header("x-query-attempt-ms", "4000"),
                format!("public cohort {ingress:?} {name} ordinal={ordinal}"),
            )
            .await?;
            if reply.status == 503 {
                reply.refused(503, "NotLeader { leader: None }")?;
                Ok(Read::TermRefused)
            } else {
                Ok(Read::Selected(reply.success()?))
            }
        }
        .boxed()
    }

    fn verify(&self, reply: Read, expected: &CommandReceipt) -> Result<()> {
        let Read::Selected(reply) = reply else {
            anyhow::bail!("unexpected term refusal");
        };
        ensure!(reply["cell"].as_str() == Some(expected.version.history.cell.as_str()));
        ensure!(
            reply["value"].as_i64()
                == Some(i64::from_be_bytes(expected.result.as_slice().try_into()?))
        );
        ensure!(reply["sequence"].as_u64() == Some(expected.version.sequence.get()));
        ensure!(reply["served_by"].as_u64() == Some(1));
        let token = CellVersionToken::from_encoded(
            reply["version_token"]
                .as_str()
                .context("query version token")?,
        )?;
        let observed = Fixture::version_keys()?.verify_cell(
            &token,
            &expected.version.history,
            self.cluster.managed.fixtures[0]
                .declaration
                .namespace
                .schema_digest,
            unix_ms()?,
        )?;
        ensure!(
            observed.version == expected.version
                && observed.witness.group == expected.witness.group
        );
        ensure!(observed.witness.position.index >= expected.witness.position.index);
        Ok(())
    }

    fn options(&self, ordinal: u8) -> CommandOptions {
        CommandOptions {
            durability: Durability::S3,
            timeout: TIMEOUT,
            ..CommandOptions::new(
                CommandId::new(ClientId::new([210 + ordinal; 16]).unwrap(), 1).unwrap(),
                self.retry,
            )
        }
    }

    async fn write(
        &self,
        leader: usize,
        name: &str,
        delta: i64,
        ordinal: u8,
    ) -> Result<CommandReceipt> {
        let cell = self.cluster.managed.hosts[leader].cell(self.history(name)?)?;
        let receipt = cell
            .command(
                &cell.contract("Increment")?,
                delta.to_be_bytes().to_vec(),
                self.options(ordinal),
            )
            .await?;
        ensure!(receipt.durability == Durability::S3);
        eprintln!(
            "public cohort gating command {:?}: S3 at {:?}",
            receipt.id, receipt.witness.position
        );
        Ok(receipt)
    }

    async fn leader(&self, target: usize) -> Result<()> {
        let old = self
            .cluster
            .leader()
            .await
            .context("public cohort find current leader")?;
        if old != target {
            self.cluster.managed.hosts[old]
                .group(self.cluster.managed.history)
                .with_context(|| format!("public cohort source group {old}"))?
                .transfer_leader(MemberId::new(target as u64 + 1)?)
                .await
                .with_context(|| format!("public cohort handoff {old} -> {target}"))?;
        }
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let status = self.cluster.managed.hosts[target]
                    .group(self.cluster.managed.history)?
                    .status()
                    .await?;
                if status.ready && status.leader == Some(target as u64 + 1) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await??;
        Ok(())
    }

    async fn caught_up(&self, leader: usize) -> Result<()> {
        let floor = self.cluster.managed.hosts[leader]
            .group(self.cluster.managed.history)?
            .status()
            .await?;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let local = self
                    .host()
                    .group(self.cluster.managed.history)?
                    .status()
                    .await?;
                if local.term == floor.term
                    && local.leader == floor.leader
                    && local.applied.index >= floor.applied.index
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await??;
        Ok(())
    }

    async fn prequeued(&self, ingress: Ingress, expected: &[CommandReceipt; 2]) -> Result<()> {
        self.leader(1).await?;
        self.caught_up(1).await?;
        let old_term = self
            .host()
            .group(self.cluster.managed.history)?
            .status()
            .await?
            .term;
        let before = self.sample()?;
        let ordinary = self.host().resource_usage().host.controller.ordinary;
        ensure!(ordinary.items == 0 && ordinary.max_items == 2);
        let sync = self.filesystem.arm(storage_fault::HOLD_SYNC);
        self.cluster.managed.hosts[1]
            .group(self.cluster.managed.history)?
            .transfer_leader(MemberId::new(1)?)
            .await?;
        tokio::time::timeout(TIMEOUT, self.filesystem.entered.notified()).await?;
        let held_status = self
            .host()
            .group(self.cluster.managed.history)?
            .status()
            .await?;
        ensure!(
            held_status.term > old_term,
            "the held sync must belong to the actual handoff"
        );
        let callbacks = self.callbacks.arm();
        let mut reads = Waiters::default();
        reads.start(0, self.read(ingress, "one"));
        reads.start(1, self.read(ingress, "two"));
        self.wait(
            "two public reads admitted behind actual Raft persistence",
            || {
                let usage = self.host().resource_usage();
                Ok(usage.runtime.read_pins.items == 2 && usage.host.controller.ordinary.items == 2)
            },
        )
        .await?;
        self.host()
            .group(self.cluster.managed.history)?
            .status()
            .await?;
        ensure!(self.sample()? == before);
        drop(sync);
        self.wait("cohort selected separate image pins", || {
            Ok(self.callbacks.entered().is_some()
                && self.host().resource_usage().runtime.read_pins.items == 2)
        })
        .await?;
        ensure!(self.sample()? == [before[0] + 1, before[1] + 1]);
        let cancelled = self
            .callbacks
            .entered()
            .context("entered physical callback")?;
        reads.cancel(cancelled).await?;
        let retained = self.host().resource_usage();
        ensure!(
            retained.runtime.read_pins.items == 2 && retained.host.controller.ordinary.items == 2
        );
        ensure!(
            retained.host.controller.ordinary.max_items == ordinary.max_items
                && retained.host.controller.ordinary.max_bytes == ordinary.max_bytes
        );
        drop(callbacks);
        self.verify(reads.finish(1 - cancelled).await?, &expected[1 - cancelled])?;
        self.drained().await?;
        eprintln!("public cohort {ingress:?}: two queued reads, one barrier, separate pins, cancelled entered caller retained");
        Ok(())
    }

    async fn late(&self, ingress: Ingress, expected: &[CommandReceipt; 2]) -> Result<()> {
        self.leader(0).await?;
        let before = self.sample()?;
        let callbacks = self.callbacks.arm();
        let mut reads = Waiters::default();
        reads.start(0, self.read(ingress, "one"));
        self.wait("first query entered its immutable image", || {
            Ok(self.callbacks.entered() == Some(0))
        })
        .await?;
        ensure!(self.sample()? == [before[0] + 1, before[1]]);
        reads.start(1, self.read(ingress, "two"));
        self.wait(
            "later arrival initiates a fresh barrier and its own pin",
            || {
                Ok(self.sample()?[0] == before[0] + 2
                    && self.host().resource_usage().runtime.read_pins.items == 2)
            },
        )
        .await?;
        ensure!(self.sample()? == [before[0] + 2, before[1]]);
        drop(callbacks);
        self.verify(reads.finish(0).await?, &expected[0])?;
        self.verify(reads.finish(1).await?, &expected[1])?;
        self.drained().await?;
        eprintln!("public cohort {ingress:?}: later arrival used a second barrier with original two-slot admission");
        Ok(())
    }

    async fn term(&self, ingress: Ingress, ordinal: u8) -> Result<CommandReceipt> {
        self.leader(1).await?;
        self.caught_up(1).await?;
        let old = self
            .host()
            .group(self.cluster.managed.history)?
            .status()
            .await?
            .term;
        let sync = self.filesystem.arm(storage_fault::HOLD_SYNC);
        let (entered, receipt) = tokio::join!(
            tokio::time::timeout(TIMEOUT, self.filesystem.entered.notified()),
            self.write(1, "one", 1, ordinal)
        );
        entered?;
        let receipt = receipt?;
        let callbacks = self.callbacks.queries.load(Ordering::Acquire);
        let mut reads = Waiters::default();
        reads.start(0, self.read(ingress, "one"));
        reads.start(1, self.read(ingress, "two"));
        self.wait("public reads queued before term cut", || {
            let usage = self.host().resource_usage();
            Ok(usage.runtime.read_pins.items == 2 && usage.host.controller.ordinary.items == 2)
        })
        .await?;
        self.host()
            .group(self.cluster.managed.history)?
            .status()
            .await?;
        self.leader(2).await?;
        ensure!(
            self.host()
                .group(self.cluster.managed.history)?
                .status()
                .await?
                .term
                == old
        );
        drop(sync);
        ensure!(
            matches!(reads.finish(0).await?, Read::TermRefused)
                && matches!(reads.finish(1).await?, Read::TermRefused)
        );
        ensure!(self.callbacks.queries.load(Ordering::Acquire) == callbacks);
        ensure!(
            self.host()
                .group(self.cluster.managed.history)?
                .status()
                .await?
                .term
                > old
        );
        let replayed = self.write(2, "one", 1, ordinal).await?;
        ensure!(
            receipt.id == replayed.id
                && receipt.result == replayed.result
                && receipt.version == replayed.version
        );
        self.drained().await?;
        for (index, host) in self.cluster.managed.hosts[..3].iter().enumerate() {
            ensure!(!host.is_closed(), "handoff closed voter {index}");
            host.group(self.cluster.managed.history)?.status().await?;
        }
        eprintln!("public cohort {ingress:?}: actual higher term refused queued reads without callback; S3 command replay unchanged");
        Ok(receipt)
    }
}

pub(super) async fn run() -> Result<()> {
    for rooted in [false, true] {
        let callbacks = Arc::new(gate::Callbacks::default());
        let query_callback = callbacks.clone();
        let command_callback = callbacks.clone();
        let filesystem = storage_fault::Gate::new(None);
        let mut cluster = Box::pin(nodes::Cluster::start_with_read_control(
            rooted,
            Arc::new(fault::Store::default()),
            Arc::new(storage_fault::FaultFileSystem {
                gate: filesystem.clone(),
            }),
            Arc::new(move || {
                command_callback.commands.fetch_add(1, Ordering::AcqRel);
            }),
            Arc::new(move |cell| query_callback.query(cell)),
        ))
        .await?;
        let work = async {
            cluster.authorize_commands()?;
            cluster.bind()?;
            let drill = Drill {
                cluster: &cluster,
                callbacks: &callbacks,
                filesystem: &filesystem,
                client: reqwest::Client::builder()
                    .no_proxy()
                    .timeout(TIMEOUT + Duration::from_secs(1))
                    .build()?,
                queries: AtomicU64::new(0),
                retry: unix_ms()? + 60_000,
            };
            drill.leader(1).await?;
            for (name, tag) in [("one", 181), ("two", 182)] {
                cluster.managed.hosts[1]
                    .cell(drill.history(name)?)?
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([tag; 16])?,
                            durability: Durability::Quorum,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            let mut expected = [
                drill.write(1, "one", 1, 1).await?,
                drill.write(1, "two", 2, 2).await?,
            ];
            for (ordinal, ingress) in [Ingress::Sdk, Ingress::Native, Ingress::Axum]
                .into_iter()
                .enumerate()
            {
                Box::pin(drill.prequeued(ingress, &expected))
                    .await
                    .with_context(|| format!("queued ingress={ingress:?}"))?;
                Box::pin(drill.late(ingress, &expected))
                    .await
                    .with_context(|| format!("late ingress={ingress:?}"))?;
                expected[0] = Box::pin(drill.term(ingress, 3 + ordinal as u8)).await?;
                ensure!(callbacks.commands.load(Ordering::Acquire) == 3 + ordinal);
                ensure!(callbacks.queries.load(Ordering::Acquire) == 4 * (ordinal + 1));
            }
            callbacks.clean()?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        callbacks.resume();
        let joined = cluster.close().await;
        if work.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.managed.roots) {
                eprintln!(
                    "retained public read cohort disk: {}",
                    root.keep().display()
                );
            }
        }
        cluster.managed.fixtures[0].domain.shutdown().await;
        work.with_context(|| format!("public read cohorts extents={rooted}"))?;
        joined?;
    }
    Ok(())
}
