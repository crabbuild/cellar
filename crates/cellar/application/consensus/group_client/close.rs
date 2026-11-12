// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Joined closure records complete physical group counts. A sampled empty queue
//! is accepted only behind sealed admission and idle physical SQL ownership.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Local producer quiescence does not prove that callers received outcomes,
/// archive coverage is complete, or old deployments can be restarted/deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationOutcomes {
    Unobserved,
    NoUnresolvedLocalProducers,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftGroupCloseReport {
    pub history: GroupHistory,
    pub member: MemberId,
    pub committed: cellar_logic::consensus::LogIndex,
    pub applied: cellar_logic::consensus::LogPosition,
    /// None is unavailable coverage, never a measured zero-lag archive.
    pub archived: Option<cellar_logic::consensus::LogPosition>,
    /// Activation metadata in the final synced manifest, not a live quorum or
    /// serving permission after shutdown.
    pub active: bool,
    pub failed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftCloseReport {
    pub node: NodeIdentity,
    pub storage_instance: EngineInstanceId,
    pub groups: Vec<RaftGroupCloseReport>,
    pub groups_total: usize,
    pub omitted_groups: usize,
    pub groups_with_archive_lag: usize,
    pub groups_with_unknown_archive: usize,
    /// Consumer admission still retained after physical joins. A nonzero count
    /// withholds the local producer-quiescence proof.
    pub retained_calls_at_shutdown: usize,
    pub mutation_outcomes: MutationOutcomes,
}

#[derive(Clone, Debug)]
pub struct RaftCloseError {
    pub controller: Option<Arc<CellGroupControllerError>>,
    pub log: Option<Arc<LogError>>,
    pub inventory: Option<Arc<LogError>>,
}

impl std::fmt::Display for RaftCloseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shared Raft closure: {self:?}")
    }
}

impl std::error::Error for RaftCloseError {}

type Closed = Result<RaftCloseReport, Arc<RaftCloseError>>;

pub(super) struct Close {
    pub closing: AtomicBool,
    requested: Notify,
    terminal: Mutex<Option<Closed>>,
    finished: Notify,
    admission: Arc<crate::application::consensus::admission::Admission>,
}

impl Close {
    pub fn new(admission: Arc<crate::application::consensus::admission::Admission>) -> Arc<Self> {
        Arc::new(Self {
            closing: AtomicBool::new(false),
            requested: Notify::new(),
            terminal: Mutex::new(None),
            finished: Notify::new(),
            admission,
        })
    }

    pub fn request(&self) {
        self.closing.store(true, Ordering::Release);
        let _ = self.admission.seal();
        self.requested.notify_one();
    }

    pub async fn wait(&self) -> Closed {
        loop {
            let notified = self.finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self
                .terminal
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }
}

pub(super) fn start(
    core: Arc<Core>,
    close: Arc<Close>,
    domain: crate::asyncrt::DomainToken,
) -> Result<(), CellGroupControllerError> {
    domain
        .spawn_cleanup(async move {
            let requested = close.requested.notified();
            tokio::pin!(requested);
            requested.as_mut().enable();
            if !close.closing.load(Ordering::Acquire) {
                crate::asyncrt::select! { _ = requested => (), _ = core.controller.wait() => () }
            }
            close.request();
            core.services.close().await;
            core.gateways
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clear();
            let controller = core.host.close().await.err().map(Arc::new);
            // Admission is sealed and every physical owner has joined. Sampling
            // before this join confuses work that cleanup is about to release
            // with a producer that remains unresolved after closure. Held replies,
            // poisoned admission and failed groups still withhold the proof.
            let quiesced = controller.is_none()
                && !close.admission.is_poisoned()
                && core.controller.local_producers_quiesced()
                && core.resources.sql.usage().is_ok_and(|sql| {
                    sql.active_cells == 0
                        && sql.retained_operations == 0
                        && sql.queued_or_running == 0
                });
            // Read after persistence/SQL/runtime joins, before closing the engine.
            // Include every durable group even if its detail row exceeds the cap.
            let retained = core.controller.usage().retained_commands;
            let report = capture(&core, retained, quiesced && retained == 0);
            let log = core.log.close().await.err().map(Arc::new);
            let result = match (controller, log, report) {
                (None, None, Ok(report)) => Ok(report),
                (controller, log, report) => Err(Arc::new(RaftCloseError {
                    controller,
                    log,
                    inventory: report.err().map(Arc::new),
                })),
            };
            *close.terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            close.finished.notify_waiters();
        })
        .map_err(|_| CellGroupControllerError::Closed)?
        .detach();
    Ok(())
}

fn capture(
    core: &Core,
    retained_calls: usize,
    quiesced: bool,
) -> Result<RaftCloseReport, LogError> {
    let groups = core.log.cell_groups()?;
    let mut report = RaftCloseReport {
        node: core.node,
        storage_instance: core.engine,
        groups: Vec::new(),
        groups_total: groups.len(),
        omitted_groups: 0,
        groups_with_archive_lag: 0,
        groups_with_unknown_archive: 0,
        retained_calls_at_shutdown: retained_calls,
        mutation_outcomes: if quiesced {
            MutationOutcomes::NoUnresolvedLocalProducers
        } else {
            MutationOutcomes::Unobserved
        },
    };
    // Fixed-size domain values only. Charge generously for vector/allocation
    // overhead; counts remain complete even when all detail rows are omitted.
    let details = core
        .close_limits
        .max_groups
        .min(core.close_limits.max_bytes / 512);
    for group in groups {
        let history = group.identity().scope;
        let published = core
            .controller
            .archive_status(history)
            .ok()
            .flatten()
            .and_then(|status| status.published);
        let failed = core
            .controller
            .group_failure(history)
            .map_or(true, |failure| failure.is_some());
        if failed {
            report.mutation_outcomes = MutationOutcomes::Unobserved;
        }
        let cache = group.read()?;
        let marker = cache
            .group_manifest
            .as_ref()
            .ok_or(LogError::Corrupt("closed group manifest missing".into()))?;
        let archived = published
            .filter(|published| published.route() == marker.route())
            .map(|published| published.position());
        match archived {
            None => report.groups_with_unknown_archive += 1,
            Some(position) if position.index.get() < cache.hard_state.commit => {
                report.groups_with_archive_lag += 1
            }
            Some(_) => (),
        }
        if report.groups.len() < details {
            report.groups.push(RaftGroupCloseReport {
                history,
                member: group.identity().member,
                committed: cellar_logic::consensus::LogIndex::new(cache.hard_state.commit),
                applied: marker.position(),
                archived,
                active: marker.is_active(),
                failed,
            });
        } else {
            report.omitted_groups += 1;
        }
    }
    Ok(report)
}
