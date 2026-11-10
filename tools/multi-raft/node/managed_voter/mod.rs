// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent managed voters. Descriptive RPCs rederive SDK proofs locally;
//! the controller owns declarations, capacity, fencing and publication.
mod actions;
mod configuration;
mod startup;

use crate::*;
use anyhow::Context;
use cellar::sdk::{consensus::*, fleet_operations::*, *};
use serde::Deserialize;
use std::sync::Mutex;
use tokio::net::TcpListener;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct Recovery {
    selection: RaftFleetTransitionRecoverySelection,
    fenced: Arc<FleetFencedTransitionRecovery>,
    plan: Arc<FleetTransitionRecoveryReplacement>,
}

struct Seed {
    history: GroupHistory,
    reservation: Arc<FleetRecoveryRangeReservation>,
    prepared: RaftFleetPreparedRecoveryRange,
    synchronized: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SeedRelease {
    plan: [u8; 32],
    commitment: [u8; 32],
}

struct State {
    config: configuration::Configuration,
    domain: Arc<ExecutionDomain>,
    cells: RaftCells,
    catalog: FleetCatalog,
    control: RaftFleetControl,
    recovery_pool: CellGroupRecoveryPool,
    recovery: Mutex<Option<Recovery>>,
    seed: Mutex<Option<Seed>>,
    // Exact local release receipts only; live history owners remain in the SDK.
    released_seeds: Mutex<std::collections::BTreeMap<GroupHistory, SeedRelease>>,
    lane: tokio::sync::Mutex<()>,
    stop: tokio::sync::watch::Sender<bool>,
    add: OperationContract,
    get: OperationContract,
}

pub(super) async fn run(domain: Arc<ExecutionDomain>, options: &Options) -> Result<()> {
    let class = counter()?;
    let config = configuration::Configuration::load(&options.config, &class)?;
    let bucket = config.bucket()?;
    let catalog = FleetCatalog::new(
        &domain,
        bucket.clone(),
        ClusterId::new(id(&config.connection.cluster_id)?)?,
        IncarnationId::new(id(&config.connection.incarnation_id)?)?,
        &config.connection.coordination_prefix,
    )?;
    // Open existing roots before creating a node or touching its engine.
    let recovery_pool = configuration::recovery_pool()?;
    let control =
        RaftFleetControl::connect(&domain, bucket.clone(), config.connection.clone(), TIMEOUT)
            .await?
            .with_transition_preparer(
                crate::managed_operator::verifier(
                    &domain,
                    options.data.join("verification"),
                    config.rooted(),
                )
                .await?,
                recovery_pool.clone(),
            );
    let add = class.contract("Add").context("compiled Add contract")?;
    let get = class.contract("Get").context("compiled Get contract")?;
    let application: Arc<dyn Application> = Arc::new(
        Registry::new(|_, _| {
            Box::pin(async {
                anyhow::bail!(
                    "qualification application requests use the authenticated SDK endpoint"
                )
            })
        })
        .raft_class(class)?,
    );
    let path = options.data.join("node");
    ensure!(
        config.retained == path.join("raft").exists(),
        "explicit fresh/retained engine mode required"
    );
    let node = startup::node(&config, path, application.clone()).await?;
    let started = async {
        let cells = startup::cells(
            &node,
            &config,
            &bucket,
            &catalog,
            application,
            config.retained,
        )
        .await?;
        let listener = TcpListener::bind((config.bind, config.api_port)).await?;
        Ok::<_, anyhow::Error>((cells, listener))
    }
    .await;
    let (cells, listener) = match started {
        Ok(started) => started,
        Err(error) => {
            let joined = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("managed voter startup cleanup: {joined:?}")));
        }
    };
    let (stop, mut stopping) = tokio::sync::watch::channel(false);
    let state = Arc::new(State {
        control: control.with_node_observer(cells.clone()),
        config,
        domain,
        cells,
        catalog,
        recovery_pool,
        recovery: Mutex::new(None),
        seed: Mutex::new(None),
        released_seeds: Mutex::new(Default::default()),
        lane: tokio::sync::Mutex::new(()),
        stop,
        add,
        get,
    });
    let server = actions::router(state.clone());
    let serving = async {
        output(
            json!({"managed_voter":"control_ready", "node":hex::encode(state.config.node),
        "process":hex::encode(state.config.process), "address":listener.local_addr()?.to_string()}),
        )?;
        axum::serve(listener, server)
            .with_graceful_shutdown(async move {
                cellar::asyncrt::select! {
                    _ = stopping.changed() => {},
                    _ = terminate() => {},
                }
            })
            .await
            .map_err(anyhow::Error::from)
    }
    .await;
    // These caches own idle copies. Entered SDK producers retain their own
    // clones until the owning node/domain joins them, including after a lost RPC.
    state.seed.lock().unwrap().take();
    state.recovery.lock().unwrap().take();
    let outcome = node.shutdown(ShutdownMode::Preserve).await;
    let closed = state.cells.close().await;
    serving?;
    let outcome = outcome?;
    let closed = closed?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(_))
            && closed.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
            && closed.omitted_groups == 0
            && closed.groups.len() == closed.groups_total
            && closed.groups.iter().all(|group| !group.failed),
        "managed voter did not prove joined shutdown: {outcome:?}, {closed:?}"
    );
    // A retained manifest keeps its committed activation bit. It is not a live
    // worker flag; Stopped and NoUnresolvedLocalProducers prove joined ownership.
    output(json!({"managed_voter":"stopped", "joined":true, "groups":closed.groups_total}))
}

async fn terminate() {
    #[cfg(unix)]
    if let Ok(mut signal) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        signal.recv().await;
        return;
    }
    let _ = tokio::signal::ctrl_c().await;
}

pub(super) fn counter() -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [81; 32])?
        .initializer([82; 32], |_, sql| {
            sql.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
            )?;
            Ok(())
        })?
        .command("Add", [83; 32], |context, input| {
            let delta = i64::from_be_bytes(input.try_into()?);
            let current: i64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            let value = current.checked_add(delta).context("counter overflow")?;
            context
                .sql()
                .execute("UPDATE counter SET value=?1", [value])?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [84; 32], |context, _| {
            let value: i64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?)
}

fn id<const N: usize>(value: &str) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    ensure!(
        value.len() == 2 * N
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "canonical hexadecimal identity required"
    );
    hex::decode_to_slice(value, &mut bytes)?;
    ensure!(bytes != [0; N], "nonzero identity required");
    Ok(bytes)
}

fn digest(value: &str) -> Result<[u8; 32]> {
    id(value)
}

impl State {
    fn history(&self, group: [u8; 16], incarnation: [u8; 16]) -> Result<GroupHistory> {
        Ok(GroupHistory {
            cluster: ClusterId::new(id(&self.config.connection.cluster_id)?)?,
            namespace: NamespaceId::new(id(&self.config.namespace().namespace_id)?)?,
            group: GroupId::new(group)?,
            incarnation: IncarnationId::new(incarnation)?,
        })
    }

    fn cell(&self, cell: String, incarnation: [u8; 16]) -> Result<RaftCellHandle> {
        ensure!(
            cell.starts_with("Counter:"),
            "Counter cell identity required"
        );
        Ok(self.cells.cell(CellHistory {
            cluster: ClusterId::new(id(&self.config.connection.cluster_id)?)?,
            namespace: NamespaceId::new(id(&self.config.namespace().namespace_id)?)?,
            cell: CellId::new(cell)?,
            incarnation: IncarnationId::new(incarnation)?,
        })?)
    }

    fn selected_recovery(&self) -> Result<Recovery> {
        self.recovery
            .lock()
            .unwrap()
            .clone()
            .context("reopen the fenced recovery in this process first")
    }
}
